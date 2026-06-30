//! Nœud **Live (Dublin)** — récepteur, exécution réelle uniquement.
//!
//! **Zéro code paper** : pas de `PaperEngine`, pas de simulation VWAP, pas d'écriture du journal
//! paper dans la hot-loop. Le nœud est *toujours live* (`live_enabled = true` au démarrage) ; le
//! Start/Stop du dashboard ne fait que basculer `live_paused`. Les ordres passent par l'`OrderEngine`
//! (acteur mpsc) — la hot loop 50 ms n'attend jamais un POST CLOB. Bankroll via `watch::channel`.

use std::sync::{Arc, Mutex};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot, watch};

use crate::concurrency::bus::Side;
use crate::config::Config;
use crate::dashboard;
use crate::net::udp;
use crate::net::wire::WireSignal;
use crate::polymarket::live_executor::{self, LiveCredentials, OrderKind};
use crate::polymarket::order_engine::{self, OrderCmd, OrderResult};
use crate::polymarket::pm_poller::{spawn_pm_poller, PmShared};
use crate::polymarket::relayer::{Market, PolyBook};
use crate::polymarket::pm_user_ws;
use crate::polymarket::pm_websocket;
use crate::state::RuntimeControls;
use crate::strategy::bankroll::{self, KellyParams};
use crate::strategy::live_position::LivePositionManager;

/// Secondes restantes sous lesquelles une position est **flattenée d'office** (on ne peut pas tenir
/// au-delà de la résolution de la fenêtre 5 min). En miroir : on n'OUVRE plus et on annule tout BUY
/// GTC qui dort sous ce seuil — sinon un maker rempli à ~20s de la fin était instantanément dumpé à
/// perte (le bug "max_hold à la même milliseconde que le fill").
const FORCE_EXIT_REMAINING_S: i64 = 30;

/// Contexte d'un BUY en attente de confirmation par l'OrderEngine.
struct PendingOpen {
    rx: oneshot::Receiver<OrderResult>,
    side: Side,
    token_id: String,
    neg_risk: bool,
    order_price: f64,
    size: f64,
    tick: f64,
    now_ms: u64,
}

pub async fn run(cfg: Config, listen_port: u16) -> anyhow::Result<()> {
    // Mode d'exécution : taker (FAK, chemin actuel préservé) ou maker (GTC resting — en construction).
    let maker_mode = cfg.exec_mode.eq_ignore_ascii_case("maker");
    if maker_mode {
        tracing::warn!("📐 EXEC_MODE=maker — entrée GTC au bid ACTIVE (sorties FAK ; TP-maker à venir). Plus de FAK no-match à l'entrée.");
    }
    tracing::info!(listen_port, exec_mode = %cfg.exec_mode, "🎯 LIVE (Dublin) démarré — exécution réelle");

    // Nœud toujours-live : live activé d'office ; le Start/Stop ne touche que `live_paused`.
    // Par sécurité, on démarre EN PAUSE (l'opérateur presse Start pour armer l'exécution).
    let controls = Arc::new(RuntimeControls::new());
    controls.live_enabled.store(true, Ordering::Relaxed);

    let live_creds = LiveCredentials::from_env();
    if let Some(ref c) = live_creds {
        if let Err(e) = live_executor::startup_poly(c).await {
            tracing::error!(error = %e, "🛑 startup Polymarket échoué — arrêt");
            return Err(e);
        }
    }
    if cfg.live_armed {
        tracing::warn!(creds = live_creds.is_some(), "⚠️  LIVE_ARMED=true — envoi réel possible");
    }
    // Filet anti-orpheline au démarrage : un crash/kill pendant un BUY GTC resting peut laisser
    // un ordre dormant dans le carnet (non suivi par l'état rechargé). En maker armé, on balaie
    // TOUT au boot pour repartir d'un carnet propre — aucune position n'existe encore à ce stade.
    if maker_mode && cfg.live_armed {
        if let Some(ref c) = live_creds {
            match live_executor::cancel_all_orders(c).await {
                Ok(()) => tracing::warn!("🧹 balayage démarrage maker — carnet vidé (orphelins éventuels annulés)"),
                Err(e) => tracing::error!(error = %e, "⚠️  balayage démarrage maker échoué (orphelins possibles)"),
            }
        }
    }
    if cfg.live_force_min_size {
        tracing::warn!("⚠️  LIVE_FORCE_MIN_SIZE=true — taille minimale forcée");
    }

    // Bankroll via watch::channel — zéro lock dans la hot loop.
    let (bk_tx, bk_rx) = watch::channel(None::<f64>);
    if let Some(creds) = live_creds.clone() {
        let tx = bk_tx.clone();
        tokio::spawn(async move {
            let mut poll = tokio::time::interval(Duration::from_secs(cfg.bankroll_poll_secs));
            loop {
                poll.tick().await;
                match live_executor::get_collateral_balance(&creds).await {
                    Ok(usdc) => { let _ = tx.send(Some(usdc));
                        tracing::info!(usdc = format!("{usdc:.2}"), "💰 bankroll réelle CLOB"); }
                    Err(e) => tracing::warn!(error = %e, "lecture bankroll CLOB échouée"),
                }
            }
        });
    }

    // OrderEngine : acteur mpsc — POST CLOB hors hot loop.
    let engine_tx = live_creds.as_ref()
        .map(|c| order_engine::spawn_order_engine(c.clone(), cfg.live_armed, cfg.order_engine_queue));

    // User WS : une seule task ; on lui envoie le condition_id au rollover.
    let (user_ws_cond_tx, mut user_ws_fill_rx) = live_creds.as_ref()
        .map(|c| pm_user_ws::init_user_ws(c.clone()))
        .map(|(tx, rx)| (Some(tx), Some(rx)))
        .unwrap_or((None, None));

    let dash = dashboard::shared(cfg.dry_run, "live");
    // Historique de prix pour le chart du dashboard (courbe du token + lignes Entry/TP/SL).
    let dash_hist = dashboard::history();
    {
        let (port, st, ct, hist) = (cfg.dashboard_port, dash.clone(), controls.clone(), dash_hist.clone());
        tokio::spawn(async move { let _ = dashboard::serve(port, st, ct, Some(hist)).await; });
    }

    let pm = Arc::new(Mutex::new(PmShared::default()));
    let ws_market_tx = pm_websocket::init_market_ws(pm.clone());
    spawn_pm_poller(pm.clone(), false, Some(ws_market_tx), live_creds.clone(), cfg.pm_ws_stale_threshold_ms);

    let lat = crate::latency::shared();
    {
        let l = lat.clone();
        tokio::spawn(async move { crate::latency::run(l, crate::latency::Probes::PmOnly).await; });
    }

    let kelly = KellyParams {
        kelly_fraction: cfg.kelly_fraction, max_size_pct: cfg.max_kelly_size_pct,
        tp_cents: cfg.take_profit_cents, sl_cents: cfg.stop_loss_cents, max_hold_secs: cfg.max_hold_secs,
    };
    let mut live_mgr = LivePositionManager::load_or_init(
        kelly,
        std::env::var("LIVE_STATE_PATH").unwrap_or_else(|_| "data/live_state.json".into()),
        std::env::var("LIVE_TRADES_PATH").unwrap_or_else(|_| "data/live_trades.jsonl".into()),
    );

    let mut rx = udp::listen(listen_port).await?;
    let mut last_fire_ms: u64 = 0;
    let mut last_fair: f64 = 0.5;
    let mut tick_interval = tokio::time::interval(Duration::from_millis(50));
    let mut log_throttle: u32 = 0;
    let mut live_dd = bankroll::LiveDrawdown::default();
    let mut live_pnl = bankroll::LivePnl::default();
    let mut was_active = false;
    let mut live_shots: u64 = 0;
    let mut pending_opens: Vec<PendingOpen> = Vec::new();
    let mut pending_close: Option<(oneshot::Receiver<OrderResult>, &'static str)> = None;
    let mut user_ws_condition_id: String = String::new();
    // Latence pipeline (mise à jour au dernier ordre soumis).
    let mut last_transport_ms: Option<u64> = None; // radar→live (NTP)
    let mut last_decide_ms: Option<u64> = None;     // recv UDP → try_send (mono-horloge)
    let mut last_pos_bid: Option<f64> = None;       // dernier bid observé du token de la position (résolution au rollover)

    // Snapshot hoissé pour traitement immédiat du signal UDP (Bloc E).
    let mut now_ms: u64 = 0;
    let mut live_bankroll_val: Option<f64> = None;
    let mut market: Option<Market> = None;
    let mut real_up: f64 = 0.5;
    let mut up_book: Arc<PolyBook> = Arc::new(PolyBook::default());
    let mut down_book: Arc<PolyBook> = Arc::new(PolyBook::default());
    let mut remaining_s: i64 = 0;
    let mut last_sweep_ms: u64 = 0; // dernier balayage anti-orpheline (maker + Idle)
    let mut last_status_poll_ms: u64 = 0; // dernier poll de statut d'ordre (PendingBuy)
    let mut last_sell_attempt_ms: u64 = 0; // throttle des re-tentatives de SELL (anti-spam 50 ms)
    let mut last_hist_ms: u64 = 0;         // dernier point poussé dans l'historique du chart (~1/s)
    // Token dont le cache d'allowance CONDITIONAL a déjà été rafraîchi pour la position courante.
    // Sans ce refresh, le CLOB voit 0 token outcome après le BUY → tout SELL est rejeté « balance 0 »
    // (on rentre mais on ne sort pas). On le fait UNE fois par position, dès l'adoption (hors hot-loop).
    let mut conditional_synced_token: Option<String> = None;
    // Le BUY suivi est totalement MATCHED → plus aucun fill à venir : on ARRÊTE de le poller (sinon
    // un GET CLOB inutile toutes les 1,5 s pendant toute la détention). Remis à false à chaque BUY.
    let mut buy_poll_done = false;
    // Réconciliation par PULL : un poll serveur (get_order_status) renvoie ici (order_id, size, price,
    // terminal) dès qu'un ordre suivi a réellement rempli — indépendant du WebSocket (anti-orpheline).
    // `terminal` = l'ordre est entièrement MATCHED (plus de croissance possible → on coupe le poll).
    let (recon_tx, mut recon_rx) = mpsc::unbounded_channel::<(String, f64, f64, bool)>();

    tracing::info!("🔄 boucle live démarrée — tick 50 ms actif");
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("SIGINT reçu — arrêt propre (live)");
                break Ok(());
            }
            Some(sig) = rx.recv() => {
                match sig {
                    WireSignal::Kill { .. } => tracing::warn!("⚡ KILL reçu — abstention"),
                    WireSignal::Attack { side, price, sent_ms, .. } => {
                        // Latence transport radar→live (requiert NTP sync) + chrono décision.
                        let recv_ms = chrono::Utc::now().timestamp_millis() as u64;
                        let recv_instant = Instant::now();
                        let transport_ms = recv_ms.saturating_sub(sent_ms);
                        let fair = price as f64;
                        last_fair = fair;
                        let gap = match side { Side::Up => fair - real_up, Side::Down => real_up - fair };
                        // Gap requis dynamique : plus le binaire est décidé (|real_up−0.5| grand) ET
                        // plus on est tard dans la fenêtre (temps écoulé), plus l'edge exigé est grand.
                        let decisiveness = (real_up - 0.5).abs();
                        let time_factor = ((300.0 - remaining_s as f64) / 300.0).clamp(0.0, 1.0);
                        // Sortie FAK = fee taker Polymarket (1 jambe ; entrée maker GTC = 0 fee). Maximale
                        // à real≈0.5 → exige plus d'edge au mid (là où la fee mange le TP), permissif aux
                        // extrêmes. fee/share = coeff·p·(1−p), p ≈ prix du token (real_up(1−real_up) symétrique).
                        let fee_term = taker_exit_fee(cfg.taker_fee_coeff, real_up);
                        let required_gap = cfg.gap_min + cfg.gap_dynamic_k * decisiveness * time_factor + fee_term;
                        let reject = if controls.is_breaker_tripped() { Some("breaker déclenché") }
                            else if !controls.live_active() { Some("live en pause") }
                            else if market.is_none() { Some("pas de marché") }
                            // Bloque l'entrée dès qu'on est dans la zone de flatten forcé : ouvrir là
                            // = sortie forcée immédiate à perte. Cohérent avec le forced-exit (§2).
                            else if remaining_s <= cfg.end_window_block_secs.max(FORCE_EXIT_REMAINING_S) { Some("fin de fenêtre (flatten imminent)") }
                            else if real_up < cfg.price_min || real_up > cfg.price_max { Some("hors bande de prix (binaire trop décidé)") }
                            else if now_ms.saturating_sub(last_fire_ms) < cfg.cooldown_ms { Some("cooldown") }
                            else if gap < required_gap { Some("gap insuffisant") }
                            else { None };
                        if let Some(reason) = reject {
                            tracing::info!(reason, side = side.as_str(), fair = format!("{fair:.3}"),
                                real = format!("{real_up:.3}"), gap = format!("{gap:+.3}"),
                                req = format!("{required_gap:.3}"),
                                gap_min = cfg.gap_min, "✗ signal rejeté (live)");
                        } else if let Some(m) = &market {
                            let (book, token) = if side == Side::Up {
                                (&*up_book, &m.up_token_id)
                            } else {
                                (&*down_book, &m.down_token_id)
                            };
                            let edge = gap;
                            if !live_mgr.is_idle() || !pending_opens.is_empty() {
                                tracing::info!(reason = "position/ordre live déjà en cours", "✗ ordre live ignoré");
                            } else {
                                match (live_bankroll_val, engine_tx.as_ref()) {
                                    (None, _) => tracing::warn!("bankroll pas encore lue — tir ignoré"),
                                    (_, None) => tracing::warn!("OrderEngine absent — tir ignoré"),
                                    (Some(bk), Some(engine)) => {
                                        // TAKER (défaut, EXEC_MODE=taker) : kind=Fak → `place_order` envoie
                                        // un VRAI ordre de marché (le SDK lit le carnet serveur, sweep des
                                        // asks). `order_price` ci-dessous n'est qu'une ESTIMATION (best_ask)
                                        // pour le sizing/bankroll — il est IGNORÉ par le market buy. Symétrie
                                        // entrée/sortie : achat ET vente au marché.
                                        // MAKER (EXEC_MODE=maker, legacy) : GTC dans le spread, prix réel.
                                        let price_kind = if maker_mode {
                                            match (book.best_bid(), book.best_ask()) {
                                                (Some(bid), Some(ask)) => Some((
                                                    maker_buy_price(bid, ask, m.tick_size,
                                                        cfg.maker_price_k_spread, cfg.maker_price_eps_ticks),
                                                    OrderKind::Gtc,
                                                )),
                                                _ => None,
                                            }
                                        } else {
                                            Some(((book.best_ask().unwrap_or(real_up) + cfg.entry_buffer).min(0.99), OrderKind::Fak))
                                        };
                                        let Some((order_price, kind)) = price_kind else {
                                            tracing::info!(side = side.as_str(),
                                                "✗ tir maker ignoré — carnet incomplet (pas de bid/ask fiable)");
                                            continue;
                                        };
                                        let sized = if cfg.fixed_order_usd > 0.0 {
                                            // Notionnel fixe ($) — Kelly ignoré ; plancher = min d'échange.
                                            Some((cfg.fixed_order_usd / order_price).floor().max(m.min_order_size))
                                        } else if cfg.live_force_min_size {
                                            Some(m.min_order_size)
                                        } else {
                                            bankroll::adjust_size_to_min(
                                                kelly.kelly_size_for(edge, order_price, bk),
                                                m.min_order_size,
                                            )
                                        };
                                        // Plancher NOTIONNEL : un BUY market sous 1$ est rejeté
                                        // (« invalid amount, min size: 1 »). On vise notional_target_usd
                                        // (>1$) car le fill réel est un peu SOUS l'estimation order_price
                                        // (best_ask+buffer) → la marge évite de retomber sous 1$.
                                        let sized = sized.map(|s|
                                            s.max((cfg.notional_target_usd / order_price).ceil()).max(m.min_order_size));
                                        match sized {
                                            None => tracing::info!(min = m.min_order_size, "✗ taille sous le minimum"),
                                            Some(size) if size * order_price > bk => tracing::warn!(
                                                cost = format!("{:.2}", size * order_price),
                                                bankroll = format!("{bk:.2}"),
                                                "✗ bankroll insuffisante"),
                                            Some(size) => {
                                                if cfg.live_force_min_size {
                                                    tracing::warn!(size, "⚠️ taille FORCÉE au minimum");
                                                }
                                                let (tx, rx_r) = oneshot::channel();
                                                let cmd = OrderCmd::Open {
                                                    side, token_id: token.clone(), neg_risk: m.neg_risk,
                                                    price: order_price, size, tick: m.tick_size,
                                                    min_order_size: m.min_order_size, kind, reply: tx,
                                                };
                                                if engine.try_send(cmd).is_ok() {
                                                    pending_opens.push(PendingOpen {
                                                        rx: rx_r, side, token_id: token.clone(),
                                                        neg_risk: m.neg_risk, order_price, size,
                                                        tick: m.tick_size, now_ms,
                                                    });
                                                    buy_poll_done = false; // nouveau BUY → re-arme le poll de statut
                                                    last_fire_ms = now_ms;
                                                    last_transport_ms = Some(transport_ms);
                                                    last_decide_ms = Some(recv_instant.elapsed().as_millis() as u64);
                                                    tracing::info!(mode = if maker_mode {"MAKER GTC"} else {"TAKER FAK"},
                                                        side = side.as_str(), price = order_price, size,
                                                        transport_ms, "⚡ BUY soumis à OrderEngine");
                                                } else {
                                                    tracing::warn!("OrderEngine plein — tir ignoré");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                continue;
            }
            _ = tick_interval.tick() => {}
        }

        // ── Tick 50ms ────────────────────────────────────────────────────────────────
        now_ms = chrono::Utc::now().timestamp_millis() as u64;
        live_bankroll_val = *bk_rx.borrow();

        {
            let g = pm.lock().unwrap();
            market = g.market.clone();
            real_up = g.real_up;
            up_book = g.up_book.clone();
            down_book = g.down_book.clone();
            remaining_s = g.remaining_s;
        }

        // ── 0. Rollover user WS — notifie la task du nouveau condition_id ──────────────
        if let Some(ref m) = market {
            if m.condition_id != user_ws_condition_id && !m.condition_id.is_empty() {
                user_ws_condition_id = m.condition_id.clone();
                if let Some(ref tx) = user_ws_cond_tx {
                    let _ = tx.send(Some(m.condition_id.clone()));
                }
                // Pré-chauffe l'allowance CONDITIONAL des 2 tokens du nouveau marché (hors hot-path).
                // Ainsi, à la 1re vente d'une position, le cache d'allowance est déjà chaud → on
                // n'attend QUE le settlement du solde, pas l'approbation. (Le solde lui-même est
                // rafraîchi à l'adoption / sur rejet — il dépend du settlement on-chain.)
                if cfg.live_armed {
                    if let Some(creds) = live_creds.clone() {
                        let (up_tok, down_tok) = (m.up_token_id.clone(), m.down_token_id.clone());
                        tokio::spawn(async move {
                            for tok in [up_tok, down_tok] {
                                if let Err(e) = live_executor::sync_conditional_allowance(&creds, &tok).await {
                                    tracing::warn!(error = %e, token_id = %tok, "⚠️ pré-chauffe allowance CONDITIONAL (rollover) échouée");
                                }
                            }
                        });
                    }
                }
            }
        }

        // ── 0b. Drain fills user WS ────────────────────────────────────────────────────
        // Draine TOUS les fills WS de ce tick (mpsc, pas watch : aucun fill coalescé/perdu).
        // RÉCONCILIATION CORRECTE : on matche sur NOTRE order_id (taker OU maker — quand notre
        // ordre resting se fait remplir, Polymarket met notre id dans `maker_order_id` et le `side`
        // de l'event est celui de la contrepartie). On décide achat/vente par NOTRE phase, jamais
        // par le côté du taker. Sans ça, un fill maker était attribué à la contrepartie → ignoré
        // → position orpheline (le bug observé en prod).
        if let Some(ref mut fill_rx) = user_ws_fill_rx {
            while let Ok(fill) = fill_rx.try_recv() {
                if let Some(buy_id) = live_mgr.tracked_buy_order_id() {
                    if fill.involves(&buy_id) {
                        tracing::info!(order_id = %buy_id, filled = fill.filled_size, price = fill.avg_price,
                            taker_sell = fill.taker_side_is_sell, "✅ fill BUY confirmé via user WS (maker/taker)");
                        live_mgr.on_fill_confirmed_buy(&buy_id, fill.filled_size, fill.avg_price, now_ms);
                        continue;
                    }
                }
                // Pas notre BUY suivi. Nos VENTES sont des FAK taker → leur fill revient dans la
                // réponse HTTP (on_sell_result), pas besoin du WS ici. On logge pour diagnostic.
                tracing::debug!(taker = ?fill.taker_order_id, maker = ?fill.maker_order_id,
                    filled = fill.filled_size, "fill WS sans BUY suivi correspondant — ignoré (vente gérée via HTTP)");
            }
        }

        // Réconciliation par PULL : le poll serveur a confirmé qu'un ordre suivi a rempli (le WS a
        // pu rater le fill). On adopte la position → Open, même sans event WS. C'est LE filet qui
        // garantit qu'on sait qu'on est rentré (et donc qu'on revend) tant qu'on tient l'order_id.
        while let Ok((oid, server_filled, price, terminal)) = recon_rx.try_recv() {
            live_mgr.reconcile_buy_to_server(&oid, server_filled, price, now_ms);
            // Ordre entièrement MATCHED et c'est bien le BUY qu'on suit → inutile de re-poller.
            if terminal && live_mgr.tracked_buy_order_id().as_deref() == Some(oid.as_str()) {
                buy_poll_done = true;
            }
        }

        // ── 1. Drain résultats OrderEngine ────────────────────────────────────────────
        pending_opens.retain_mut(|p| {
            match p.rx.try_recv() {
                Ok(res) => {
                    live_mgr.on_buy_result(res, p.side, &p.token_id, p.neg_risk,
                        p.order_price, p.size, p.tick, p.now_ms);
                    false
                }
                Err(oneshot::error::TryRecvError::Empty) => true,
                Err(_) => false,
            }
        });
        // Taker (entrée au marché) : un BUY FAK market remplit ou échoue tout de suite. S'il revient
        // sans fill (carnet vide → fill 0), il créerait un PendingBuy que SEUL le cycle de vie maker
        // nettoie → orphelin en taker. On repasse Idle immédiatement (rien ne dort dans le carnet).
        if !maker_mode && live_mgr.pending_buy_info().is_some() {
            live_mgr.cancel_pending_buy();
        }
        if let Some((r, reason)) = pending_close.as_mut() {
            match r.try_recv() {
                Ok(res) => {
                    let min_os = market.as_ref().map(|m| m.min_order_size).unwrap_or(5.0);
                    let needs_cond_refresh = live_mgr.on_sell_result(res, reason, now_ms, min_os);
                    // SELL rejeté « balance 0 » → cache CONDITIONAL probablement périmé : on le
                    // rafraîchit (hors hot-loop) pour débloquer la prochaine tentative de revente.
                    // SELL_SKIP_BALANCE_REFRESH (expérimental) : on SAUTE le refresh → la re-tentative
                    // re-tire la vente brute (teste si le moteur off-chain l'accepte au MATCHED).
                    if needs_cond_refresh && cfg.live_armed && !cfg.sell_skip_balance_refresh {
                        if let (Some(creds), Some(tok)) =
                            (live_creds.clone(), live_mgr.position().map(|p| p.token_id.clone()))
                        {
                            tokio::spawn(async move {
                                if let Err(e) = live_executor::sync_conditional_allowance(&creds, &tok).await {
                                    tracing::warn!(error = %e, "⚠️ refresh cache CONDITIONAL (post-rejet SELL) échoué");
                                }
                            });
                        }
                    }
                    pending_close = None;
                }
                Err(oneshot::error::TryRecvError::Empty) => {}
                Err(_) => { pending_close = None; }
            }
        }

        // ── Refresh CONDITIONAL à l'adoption d'une position (débloque le SELL) ────────────────
        // Dès qu'on détient un token (Open), on rafraîchit UNE fois le cache d'allowance CONDITIONAL
        // du CLOB pour ce token : sinon le moteur de matching voit 0 et rejette toute revente.
        match live_mgr.position().map(|p| p.token_id.clone()) {
            Some(tok) => {
                if conditional_synced_token.as_deref() != Some(tok.as_str()) {
                    conditional_synced_token = Some(tok.clone());
                    // Nouvelle position → reset du throttle de sortie : le 1er exit (TP surtout) doit
                    // pouvoir partir SANS délai (le throttle exit_retry_ms ne concerne que les re-essais).
                    last_sell_attempt_ms = 0;
                    if cfg.live_armed {
                        if let Some(creds) = live_creds.clone() {
                            tokio::spawn(async move {
                                match live_executor::sync_conditional_allowance(&creds, &tok).await {
                                    Ok(()) => tracing::info!(token_id = %tok, "🔄 cache CONDITIONAL rafraîchi à l'adoption — SELL armé"),
                                    Err(e) => tracing::warn!(error = %e, token_id = %tok, "⚠️ refresh cache CONDITIONAL à l'adoption échoué (SELL pourrait être rejeté)"),
                                }
                            });
                        }
                    }
                }
            }
            None => conditional_synced_token = None,
        }

        // ── Maker : cycle de vie SÛR d'un BUY GTC resting (anti-orpheline) ───────────────
        // RÈGLE : un fill GAGNE TOUJOURS. On n'envoie l'annulation qu'au timeout, et on ne passe
        // Idle qu'APRÈS une fenêtre de grâce SANS fill (le fill, même tardif, repasse Open via WS).
        // PULL anti-orpheline (source de vérité SERVEUR, indépendante du WS) : tant qu'on suit un
        // ordre d'achat (PendingBuy en attente OU Open déjà adopté), on DEMANDE toutes les ~1,5 s
        // « combien a réellement rempli ? ». Le résultat (cumulatif) revient par recon_rx et
        // `reconcile_buy_to_server` cale la taille suivie sur la vérité serveur (idempotent). Ainsi,
        // même si le WS rate TOUS les events, on sait qu'on est rentré → on gère/revend la position.
        if maker_mode && cfg.live_armed && !buy_poll_done {
            if let Some(track_id) = live_mgr.tracked_buy_order_id() {
                if now_ms.saturating_sub(last_status_poll_ms) >= 1500 {
                    last_status_poll_ms = now_ms;
                    if let Some(creds) = live_creds.clone() {
                        let tx = recon_tx.clone();
                        tokio::spawn(async move {
                            match live_executor::get_order_status(&creds, &track_id).await {
                                Ok(st) if st.size_matched > 0.0 => {
                                    // "MATCHED" = ordre entièrement rempli → plus rien à poller.
                                    let terminal = st.status.eq_ignore_ascii_case("MATCHED");
                                    let _ = tx.send((track_id, st.size_matched, st.price, terminal));
                                }
                                Ok(_) => {}
                                Err(e) => tracing::warn!(order_id = %track_id, error = %e,
                                    "⚠️ poll statut ordre échoué — si ça se répète, l'endpoint /data/order est à corriger"),
                            }
                        });
                    }
                }
            }
        }

        if maker_mode {
            if let Some((buy_id, since_ms, cancel_since)) = live_mgr.pending_buy_info() {
                match cancel_since {
                    None => {
                        // Annulation si timeout ATTEINT, OU si on entre dans la zone de flatten forcé :
                        // un GTC rempli sous FORCE_EXIT_REMAINING_S serait dumpé instantanément à perte.
                        let near_window_close = remaining_s <= FORCE_EXIT_REMAINING_S;
                        // Timeout adaptatif : frac du temps restant, borné [floor (≥ latence POST observée
                        // ~1,3 s), buy_timeout_ms]. Tôt en fenêtre → grâce pleine ; proche de l'expiration →
                        // grâce courte (mais jamais < floor, sinon on annule avant que l'ordre repose).
                        let eff_timeout = ((remaining_s.max(0) as f64) * 1000.0 * cfg.grace_frac_of_remaining)
                            .clamp(cfg.buy_grace_floor_ms as f64, cfg.buy_timeout_ms as f64) as u64;
                        if now_ms.saturating_sub(since_ms) >= eff_timeout || near_window_close {
                            if let Some(engine) = engine_tx.as_ref() {
                                let (tx, _rx) = oneshot::channel();
                                let _ = engine.try_send(OrderCmd::Cancel { order_id: buy_id.clone(), reply: tx });
                            }
                            let cause = if near_window_close { "fin de fenêtre (flatten imminent)" } else { "non rempli (timeout)" };
                            tracing::info!(order_id = %buy_id, cause, "🗑 BUY GTC annulé — fenêtre de grâce");
                            live_mgr.mark_buy_cancelling(now_ms);
                        }
                    }
                    Some(cancel_t) => {
                        if now_ms.saturating_sub(cancel_t) >= cfg.cancel_grace_ms {
                            tracing::info!(order_id = %buy_id, "✓ BUY GTC annulé (aucun fill pendant la grâce) — Idle");
                            live_mgr.cancel_pending_buy();
                        }
                    }
                }
            }
            // Backstop périodique : si une annulation a silencieusement échoué (erreur API,
            // try_send abandonné) alors que la FSM est repassée Idle après la grâce, un ordre
            // peut rester orphelin dans le carnet. Quand on est Idle (aucune position, aucun BUY
            // suivi, aucun ordre en vol), on balaie le carnet ~toutes les 60 s. Money-safe : au
            // pire on annule un BUY resting légitime AVANT son fill (= trade manqué, pas de perte).
            if cfg.live_armed
                && live_mgr.is_idle()
                && pending_opens.is_empty()
                && pending_close.is_none()
                && now_ms.saturating_sub(last_sweep_ms) >= 60_000
            {
                last_sweep_ms = now_ms;
                if let Some(creds) = live_creds.clone() {
                    tokio::spawn(async move {
                        if let Err(e) = live_executor::cancel_all_orders(&creds).await {
                            tracing::error!(error = %e, "⚠️  balayage périodique maker échoué");
                        }
                    });
                }
            }
        }

        // Anti-position-coincée : un PendingSell sans confirmation WS au-delà du timeout repasse
        // Open → la section 2 ci-dessous re-tente la vente (jamais bloqué à détenir une position).
        if pending_close.is_none() {
            live_mgr.revert_stuck_pending_sell(now_ms, cfg.sell_timeout_ms);
        }

        // ── 2. Live manage → OrderEngine SELL (non-bloquant) ─────────────────────────
        if pending_close.is_none() {
            if let (Some(pos), Some(engine)) = (live_mgr.position().cloned(), engine_tx.as_ref()) {
                if let Some(m) = &market {
                    // ── Rollover : la fenêtre 5 min a changé → le token de la position n'est plus
                    // l'actif tradable. On NE trade PAS le carnet du nouveau marché (sinon faux "TP"
                    // sur le mauvais token) : la position est réglée on-chain (gagnée/perdue).
                    let current_token = if pos.side == Side::Up { &m.up_token_id } else { &m.down_token_id };
                    if pos.token_id != *current_token {
                        tracing::warn!(pos_token = %pos.token_id, slug = %m.slug,
                            "🪙 fenêtre tournée — position réglée à l'expiration (pas de TP sur le marché suivant)");
                        live_mgr.resolve_expired(last_pos_bid.unwrap_or(0.0));
                        last_pos_bid = None;
                    } else {
                    let book = if pos.side == Side::Up { &*up_book } else { &*down_book };
                    if let Some(bid) = book.best_bid() {
                        last_pos_bid = Some(bid);
                        let held_ms = now_ms.saturating_sub(pos.opened_ms);
                        let held_s = (held_ms / 1000) as i64;
                        // TP : immédiat (on encaisse un move favorable dès qu'il arrive).
                        // SL : armé seulement après MIN_HOLD_SL_MS (évite le SL instantané sur le
                        //      spread d'entrée). max_hold/fin de fenêtre = sortie forcée.
                        let reason = if bid >= pos.tp_price { Some("take_profit") }
                            else if held_ms >= cfg.min_hold_sl_ms && bid <= pos.sl_price { Some("stop_loss") }
                            // Distinct de max_hold : la fenêtre se ferme, flatten forcé (≠ "détenu trop
                            // longtemps"). Évite l'étiquette trompeuse "max_hold" sur un fill instantané.
                            else if remaining_s <= FORCE_EXIT_REMAINING_S { Some("window_close") }
                            else if held_s >= kelly.max_hold_secs { Some("max_hold") }
                            else { None };
                        // Throttle : un SELL rejeté (ex. settlement on-chain pas fini) revient en
                        // erreur immédiate → sans garde on re-poste toutes les 50 ms. On limite à
                        // exit_retry_ms (défaut 150 ms) : assez court pour rattraper le bid courant
                        // sur carnet mince (anti dérive TP→SL), assez long pour ne pas spammer.
                        if let Some(r) = reason {
                            if now_ms.saturating_sub(last_sell_attempt_ms) >= cfg.exit_retry_ms {
                                last_sell_attempt_ms = now_ms;
                                // VENTE AU MARCHÉ : le TP/SL n'est qu'un DÉCLENCHEUR (faut-il sortir ?),
                                // pas un prix. En live (POLY_1271) `place_order` envoie un VRAI ordre de
                                // marché (le SDK lit le book SERVEUR en direct, sweep des bids, FAK) →
                                // `exit` ci-dessous est IGNORÉ pour la vente. On le calcule seulement
                                // comme prix de repli pour les chemins non sig_type=3 (EIP-712).
                                let exit = (bid - cfg.exit_buffer).max(0.01);
                                let (tx, rx_r) = oneshot::channel();
                                let cmd = OrderCmd::Close {
                                    token_id: pos.token_id.clone(), side: pos.side, neg_risk: pos.neg_risk,
                                    price: exit, size: pos.size, tick: m.tick_size, kind: OrderKind::Fak, reply: tx,
                                };
                                if engine.try_send(cmd).is_ok() {
                                    pending_close = Some((rx_r, r));
                                    tracing::info!(reason = r, size = pos.size, "⚡ SELL soumis à OrderEngine");
                                }
                            }
                        }
                    }
                    } // fin else (token de la position toujours actif)
                }
            }
        }

        // ── 3. Circuit breaker (drawdown sur bankroll réelle) ─────────────────────────
        let breaker_hit = live_bankroll_val.map_or(false, |real| live_dd.breached(real, cfg.max_drawdown));
        if !controls.is_breaker_tripped() && breaker_hit && controls.trip_breaker() {
            tracing::error!(max_dd = cfg.max_drawdown, "🛑 CIRCUIT BREAKER live — drawdown atteint");
        }

        let active = controls.live_active();
        if active && !was_active { live_pnl.reset(); live_shots = 0; }
        was_active = active;
        let live_pnl_val = if active { live_bankroll_val.map(|bk| live_pnl.update(bk)) } else { None };

        // ── 4. Dashboard (champs live uniquement) ─────────────────────────────────────
        let lat_snap = lat.lock().unwrap().clone();
        let pm_ws_stale_ms = {
            let last = pm.lock().unwrap().last_ws_ts_ms;
            if last > 0 { Some(now_ms.saturating_sub(last)) } else { None }
        };
        {
            let mut d = dash.write().await;
            d.market_slug = market.as_ref().map(|m| m.slug.clone()).unwrap_or_default();
            d.remaining_s = remaining_s;
            d.fair_up = last_fair; d.real_up = real_up; d.gap = last_fair - real_up;
            if let Some(p) = live_mgr.position() {
                d.in_position = true; d.pos_side = p.side.as_str().into();
                d.pos_entry = p.entry_price; d.pos_tp = p.tp_price; d.pos_sl = p.sl_price;
            } else {
                d.in_position = false;
            }
            d.mode = if controls.is_breaker_tripped() { "BREAKER" }
                else if controls.live_active() { "LIVE" } else { "PAUSE" }.into();
            d.live_enabled = controls.is_live_enabled();
            d.live_paused = controls.is_live_paused();
            d.live_armed = cfg.live_armed;
            d.breaker_tripped = controls.is_breaker_tripped();
            d.max_drawdown = cfg.max_drawdown;
            d.lat_polymarket_ms = lat_snap.polymarket_ms;
            d.live_bankroll = live_bankroll_val;
            d.live_pnl = if live_mgr.state.shots > 0 { Some(live_mgr.state.realized_pnl) } else { live_pnl_val };
            d.live_shots = live_mgr.state.shots.max(live_shots);
            d.live_force_min = cfg.live_force_min_size;
            d.fixed_order_usd = cfg.fixed_order_usd;
            d.maker = maker_mode; // expose le mode d'exécution (maker GTC | taker FAK) au dashboard
            d.lat_last_buy_ms = live_mgr.last_buy_ms;
            d.lat_last_sell_ms = live_mgr.last_sell_ms;
            d.pm_ws_stale_ms = pm_ws_stale_ms;
            // Latence totale signal→ordre = transport (radar→live) + décision + POST CLOB.
            d.lat_transport_ms = last_transport_ms;
            d.lat_decide_ms = last_decide_ms;
            d.lat_post_ms = live_mgr.last_buy_ms; // BUY FAK : début POST → réponse CLOB
            d.lat_total_ms = match (last_transport_ms, last_decide_ms, live_mgr.last_buy_ms) {
                (Some(t), Some(d2), Some(p)) => Some(t + d2 + p),
                _ => None,
            };
        }

        // ── Historique de prix pour le chart (~1/s) ───────────────────────────────────
        // `p` = mark du TOKEN DE LA POSITION (Up = real_up, Down = 1−real_up) pendant une position,
        // sinon real_up. Les niveaux entry/tp/sl sont dans le même espace → lignes alignées.
        if now_ms.saturating_sub(last_hist_ms) >= 1000 {
            last_hist_ms = now_ms;
            let (p, entry, tp, sl) = match live_mgr.position() {
                Some(pos) => {
                    let mark = if pos.side == Side::Up { real_up } else { 1.0 - real_up };
                    (mark, Some(pos.entry_price), Some(pos.tp_price), Some(pos.sl_price))
                }
                None => (real_up, None, None, None),
            };
            dashboard::push_history(&dash_hist, dashboard::PricePoint { t: now_ms / 1000, p, entry, tp, sl });
        }

        log_throttle += 1;
        if log_throttle % 100 == 0 {
            tracing::info!(real = format!("{:.3}", real_up), live_shots = live_mgr.state.shots,
                bankroll = format!("{:?}", live_bankroll_val), "live");
        }
    }
}

/// Arrondit au tick le plus proche, clampé dans [0.01, 0.99] (identique à `order_engine`/`live_position`).
fn round_tick(p: f64, tick: f64) -> f64 {
    if tick <= 0.0 { return p; }
    ((p / tick).round() * tick).clamp(0.01, 0.99)
}

/// Prix d'un BUY maker posé DANS le spread (`mid − k·spread`), borné sous l'ask (`eps` ticks) pour
/// rester STRICTEMENT maker. `k=0.5` ⇒ best bid (ancien comportement), `0.25` ⇒ entre mid et bid
/// (meilleur taux de fill), `0` ⇒ mid. Le plafond `ask − eps·tick` garantit qu'on ne croise jamais.
fn maker_buy_price(bid: f64, ask: f64, tick: f64, k_spread: f64, eps_ticks: f64) -> f64 {
    let spread = (ask - bid).max(0.0);
    let mid = (bid + ask) / 2.0;
    let ceiling = (ask - eps_ticks * tick).max(0.01); // garde-fou maker (sous l'ask)
    let lo = bid.min(ceiling);
    round_tick((mid - k_spread * spread).clamp(lo, ceiling), tick)
}

/// Fee de SORTIE taker (1 jambe FAK) en prix/share : `coeff·p·(1−p)`. Maximale à `p=0.5`, ~0 aux
/// extrêmes (doc Polymarket : `fee = shares·coeff·p·(1−p)`, crypto `coeff=0.07`). L'entrée maker GTC
/// ne paie rien → un seul terme.
fn taker_exit_fee(coeff: f64, p: f64) -> f64 { coeff * p * (1.0 - p) }

#[cfg(test)]
mod tests {
    use super::{maker_buy_price, round_tick, taker_exit_fee};

    #[test]
    fn maker_price_below_ask_above_bid_for_k_quarter() {
        // Spread large : bid 0.40 / ask 0.50, tick 0.01, k=0.25, eps=1.
        let p = maker_buy_price(0.40, 0.50, 0.01, 0.25, 1.0);
        assert!(p < 0.50, "doit rester maker (< ask), got {p}");
        assert!(p <= 0.49 + 1e-9, "plafonné à ask − 1 tick, got {p}");
        assert!(p > 0.40, "au-dessus du best bid pour k<0.5 (meilleur fill), got {p}");
        assert!((p - 0.425).abs() <= 0.01, "≈ mid − k·spread (0.425 au tick), got {p}");
    }

    #[test]
    fn maker_price_k_half_equals_best_bid() {
        // k=0.5 ⇒ mid − 0.5·spread = bid (ancien comportement préservé).
        let p = maker_buy_price(0.40, 0.50, 0.01, 0.5, 1.0);
        assert!((p - 0.40).abs() < 1e-9, "k=0.5 ⇒ best bid, got {p}");
    }

    #[test]
    fn maker_price_one_tick_market_clamps_under_ask() {
        // Spread 1 tick : bid 0.49 / ask 0.50 → le plafond (ask − 1 tick = 0.49) lie, jamais ≥ ask.
        let p = maker_buy_price(0.49, 0.50, 0.01, 0.25, 1.0);
        assert!((p - 0.49).abs() < 1e-9, "clamp au plafond 0.49, got {p}");
        assert!(p < 0.50, "jamais ≥ ask");
    }

    #[test]
    fn taker_exit_fee_max_at_mid_zero_at_extremes() {
        let mid = taker_exit_fee(0.07, 0.50);
        assert!((mid - 0.0175).abs() < 1e-9, "0.07·0.25 = 0.0175 au mid, got {mid}");
        assert!(taker_exit_fee(0.07, 0.05) < mid / 4.0, "fee minuscule aux extrêmes");
        assert!(taker_exit_fee(0.07, 0.95) < mid / 4.0, "symétrique : faible à 0.95 aussi");
    }

    #[test]
    fn round_tick_rounds_and_clamps() {
        assert!((round_tick(0.4267, 0.01) - 0.43).abs() < 1e-9);
        assert_eq!(round_tick(0.004, 0.01), 0.01); // clamp bas
    }
}
