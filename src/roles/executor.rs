//! Nœud **Exécuteur (Dublin)** — récepteur.
//!
//! Possède le carnet Polymarket, la bankroll et le moteur paper. Une tâche UDP dédiée décode les
//! paquets 6 octets du radar et les pousse dans un `mpsc` ; la boucle timer (50 ms) :
//!   1. gère la position ouverte (TP/SL/max-hold) à chaque tick ;
//!   2. draine les signaux reçus → calcule le **gap = fair(paquet) − real(local)**, applique le
//!      filtre `gap_min` + fin-de-fenêtre + cooldown, **dimensionne via Kelly** (autoritaire),
//!      puis `paper.fire` (DRY_RUN : fill simulé, aucun ordre réel).
//!
//! Sonde de latence côté Dublin : Polymarket uniquement (`Probes::PmOnly`).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::concurrency::bus::Side;
use crate::config::Config;
use crate::dashboard;
use crate::net::udp;
use crate::net::wire::WireSignal;
use crate::polymarket::live_executor::{self, LiveCredentials};
use crate::polymarket::pm_poller::{spawn_pm_poller, PmShared};
use crate::state::RuntimeControls;
use crate::strategy::bankroll::{self, KellyParams, PaperEngine};
use crate::strategy::live_position::LivePositionManager;

pub async fn run(cfg: Config, listen_port: u16) -> anyhow::Result<()> {
    tracing::info!(listen_port, dry_run = cfg.dry_run, "🎯 EXÉCUTEUR (Dublin) démarré");

    let controls = Arc::new(RuntimeControls::new());
    let live_creds = LiveCredentials::from_env();
    if let Some(ref c) = live_creds {
        if let Err(e) = live_executor::startup_poly(c).await {
            tracing::error!(error = %e, "🛑 startup Polymarket échoué — arrêt");
            return Err(e);
        }
    }
    if cfg.live_armed {
        tracing::warn!(creds = live_creds.is_some(), "⚠️  LIVE_ARMED=true — envoi réel possible (si signature vérifiée)");
    }
    if cfg.live_force_min_size {
        tracing::warn!("⚠️  LIVE_FORCE_MIN_SIZE=true — taille minimale forcée (Kelly ignoré, agressif)");
    }

    // Vraie collatéral USDC (CLOB) — lue toutes les 30 s ; sert de bankroll pour le sizing LIVE.
    let live_bankroll = Arc::new(Mutex::new(None::<f64>));
    if let Some(creds) = live_creds.clone() {
        let bk = live_bankroll.clone();
        tokio::spawn(async move {
            let mut poll = tokio::time::interval(Duration::from_secs(30));
            loop {
                poll.tick().await;
                match live_executor::get_collateral_balance(&creds).await {
                    Ok(usdc) => { *bk.lock().unwrap() = Some(usdc);
                        tracing::info!(usdc = format!("{usdc:.2}"), "💰 bankroll réelle CLOB"); }
                    Err(e) => tracing::warn!(error = %e, "lecture bankroll CLOB échouée"),
                }
            }
        });
    }

    let dash = dashboard::shared(cfg.dry_run);
    {
        let (port, st, ct) = (cfg.dashboard_port, dash.clone(), controls.clone());
        tokio::spawn(async move { let _ = dashboard::serve(port, st, ct).await; });
    }

    // `false` = l'exécuteur n'a pas besoin du strike (le fair arrive dans le paquet radar).
    let pm = Arc::new(Mutex::new(PmShared::default()));
    spawn_pm_poller(pm.clone(), false);

    let lat = crate::latency::shared();
    {
        let l = lat.clone();
        tokio::spawn(async move { crate::latency::run(l, crate::latency::Probes::PmOnly).await; });
    }

    let kelly = KellyParams {
        kelly_fraction: cfg.kelly_fraction, max_size_pct: cfg.max_kelly_size_pct,
        tp_cents: cfg.take_profit_cents, sl_cents: cfg.stop_loss_cents, max_hold_secs: cfg.max_hold_secs,
    };
    let mut paper = PaperEngine::load_or_init(
        cfg.start_cash, kelly,
        std::env::var("STATE_PATH").unwrap_or_else(|_| "data/sniper_state.json".into()),
        std::env::var("TRADES_PATH").unwrap_or_else(|_| "data/sniper_trades.jsonl".into()),
    );
    // Manager LIVE — symétrique au PaperEngine, mais touche le CLOB. Persistance séparée.
    let mut live_mgr = LivePositionManager::load_or_init(
        kelly,
        std::env::var("LIVE_STATE_PATH").unwrap_or_else(|_| "data/live_state.json".into()),
        std::env::var("LIVE_TRADES_PATH").unwrap_or_else(|_| "data/live_trades.jsonl".into()),
    );

    let mut rx = udp::listen(listen_port).await?;

    let mut last_fire_ms: u64 = 0;
    let mut last_fair: f64 = 0.5; // dernier fair reçu (affichage gap)
    let mut tick = tokio::time::interval(Duration::from_millis(50));
    let mut log_throttle: u32 = 0;
    let mut live_dd = bankroll::LiveDrawdown::default(); // drawdown sur la bankroll réelle (live)
    let mut live_pnl = bankroll::LivePnl::default();     // PnL réalisé live (Δ bankroll)
    let mut was_live = false;                            // détection de transition paper→live
    let mut live_shots: u64 = 0;                         // ordres live acceptés cette session
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                // Arrêt propre : l'état paper est déjà persisté à chaque clôture de position.
                tracing::info!("SIGINT reçu — arrêt propre (exécuteur)");
                break Ok(());
            }
            _ = tick.tick() => {}
        }
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;

        let (market, real_up, up_book, down_book, remaining_s) = {
            let g = pm.lock().unwrap();
            (g.market.clone(), g.real_up, g.up_book.clone(), g.down_book.clone(), g.remaining_s)
        };

        // 1. Gestion de la position ouverte (TP/SL/max-hold) — paper.
        let mark_bid = if let Some(p) = &paper.position {
            let bk = if p.side == Side::Up { &up_book } else { &down_book };
            bk.best_bid()
        } else { None };
        paper.manage(mark_bid, now_ms, remaining_s);

        // 1b. Gestion de la position LIVE (symétrique). Le manager poste les SELL FAK lui-même
        //     si TP/SL/max-hold/fin-de-fenêtre est atteint ; reste idempotent sinon.
        if let (Some(p), Some(creds), Some(m)) =
            (live_mgr.position.as_ref(), live_creds.as_ref(), market.as_ref())
        {
            let live_book = if p.side == Side::Up { &up_book } else { &down_book };
            let live_mark = live_book.best_bid();
            live_mgr.manage(
                creds, cfg.live_armed, live_mark, live_book,
                m.min_order_size, m.tick_size, now_ms, remaining_s,
            ).await;
        }

        // Circuit breaker (drawdown) — LIVE : vraie bankroll CLOB ; PAPER : equity fictive.
        let breaker_hit = if controls.live_active() {
            match *live_bankroll.lock().unwrap() {
                Some(real) => live_dd.breached(real, cfg.max_drawdown),
                None => false, // bankroll réelle pas encore lue
            }
        } else {
            bankroll::check_drawdown_breaker(paper.equity(mark_bid), cfg.start_cash, cfg.max_drawdown)
        };
        if !controls.is_breaker_tripped() && breaker_hit && controls.trip_breaker() {
            tracing::error!(mode = controls.mode_label(), max_dd = cfg.max_drawdown,
                "🛑 CIRCUIT BREAKER — drawdown atteint, exécution coupée");
        }

        // PnL live = Δ bankroll réelle depuis l'activation du live ; référence reposée à la bascule.
        let is_live = controls.live_active();
        if is_live && !was_live { live_pnl.reset(); live_shots = 0; }
        was_live = is_live;
        let live_pnl_val = if is_live {
            live_bankroll.lock().unwrap().map(|bk| live_pnl.update(bk))
        } else { None };

        // 2. Drain des signaux UDP reçus du radar.
        while let Ok(sig) = rx.try_recv() {
            match sig {
                WireSignal::Kill => tracing::warn!("⚡ KILL reçu — abstention"),
                WireSignal::Attack { side, price, .. } => {
                    let fair = price as f64;
                    last_fair = fair;
                    // gap = edge orienté selon le sens (toujours « fair en faveur du token visé »).
                    let gap = match side {
                        Side::Up => fair - real_up,
                        Side::Down => real_up - fair,
                    };
                    // Raison de rejet unique (loggée) — sinon `None` = on tente l'exécution.
                    let reject = if controls.is_breaker_tripped() {
                        Some("breaker déclenché")
                    } else if market.is_none() {
                        Some("pas de marché")
                    } else if remaining_s <= cfg.end_window_block_secs {
                        Some("fin de fenêtre")
                    } else if now_ms.saturating_sub(last_fire_ms) < cfg.cooldown_ms {
                        Some("cooldown")
                    } else if gap < cfg.gap_min {
                        Some("gap insuffisant")
                    } else {
                        None
                    };
                    if let Some(reason) = reject {
                        tracing::info!(reason, side = side.as_str(), fair = format!("{fair:.3}"),
                            real = format!("{real_up:.3}"), gap = format!("{gap:+.3}"),
                            gap_min = cfg.gap_min, "✗ signal rejeté");
                        continue;
                    }
                    if let Some(m) = &market {
                        let (book, token) = if side == Side::Up {
                            (&up_book, &m.up_token_id)
                        } else {
                            (&down_book, &m.down_token_id)
                        };
                        let edge = gap; // gap ≥ gap_min > 0 ici
                        // Aiguillage live → paper.
                        if is_live {
                            // En LIVE : on n'ouvre une position que si on a la vraie bankroll lue
                            // ET qu'on n'a pas déjà une position live ouverte (LivePositionManager
                            // l'enforce aussi, mais on évite un tir inutile).
                            if live_mgr.position.is_some() {
                                tracing::info!(reason = "position live déjà ouverte",
                                    "✗ ordre live ignoré");
                            } else {
                                match (*live_bankroll.lock().unwrap(), live_creds.as_ref()) {
                                    (None, _) => tracing::warn!("LIVE actif mais bankroll réelle pas encore lue — tir ignoré"),
                                    (_, None) => tracing::warn!("LIVE actif mais POLY_* credentials absents — tir ignoré"),
                                    (Some(bk), Some(creds)) => {
                                        let order_price = book.best_ask().unwrap_or(real_up);
                                        // Sizing : Kelly normal, OU taille minimale forcée (micro-test).
                                        // LivePositionManager rajoutera la garde notionnel ≥ $1.
                                        let sized = if cfg.live_force_min_size {
                                            Some(m.min_order_size)
                                        } else {
                                            bankroll::adjust_size_to_min(
                                                paper.kelly_size_for(edge, order_price, bk),
                                                m.min_order_size,
                                            )
                                        };
                                        match sized {
                                            None => tracing::info!(reason = "taille sous le minimum",
                                                min = m.min_order_size, "✗ ordre live ignoré"),
                                            Some(size) if size * order_price > bk => tracing::warn!(
                                                cost = format!("{:.2}", size * order_price), bankroll = format!("{bk:.2}"),
                                                "✗ ordre live ignoré — bankroll insuffisante"),
                                            Some(size) => {
                                                if cfg.live_force_min_size {
                                                    tracing::warn!(size, "⚠️ taille FORCÉE au minimum (LIVE_FORCE_MIN_SIZE)");
                                                }
                                                if live_mgr.try_open(
                                                    creds, cfg.live_armed, side, token, m.neg_risk,
                                                    order_price, size, m.tick_size, m.min_order_size, now_ms,
                                                ).await {
                                                    last_fire_ms = now_ms;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        } else if !controls.is_paper_paused()
                            && paper.fire(side, token, edge, book, m.tick_size, m.min_order_size, now_ms)
                        {
                            last_fire_ms = now_ms;
                        }
                    }
                }
            }
        }

        // Dashboard exécuteur (PM/position/PnL ; OBI laissé à 0).
        let lat_snap = lat.lock().unwrap().clone();
        {
            let mut d = dash.write().await;
            d.market_slug = market.as_ref().map(|m| m.slug.clone()).unwrap_or_default();
            d.remaining_s = remaining_s;
            d.fair_up = last_fair; d.real_up = real_up; d.gap = last_fair - real_up;
            // Position affichée : la live prend la priorité si elle existe, sinon la paper.
            if let Some(p) = &live_mgr.position {
                d.in_position = true;
                d.pos_side = p.side.as_str().into();
                d.pos_entry = p.entry_price; d.pos_tp = p.tp_price; d.pos_sl = p.sl_price;
            } else if let Some(p) = &paper.position {
                d.in_position = true;
                d.pos_side = p.side.as_str().into();
                d.pos_entry = p.entry_price; d.pos_tp = p.tp_price; d.pos_sl = p.sl_price;
            } else {
                d.in_position = false;
            }
            d.cash = paper.state.cash; d.equity = paper.equity(mark_bid);
            d.realized_pnl = paper.state.realized_pnl; d.drawdown = paper.drawdown();
            d.shots = paper.state.shots; d.wins = paper.state.wins; d.losses = paper.state.losses;
            d.hit_rate = paper.hit_rate();
            d.mode = controls.mode_label().into();
            d.paper_paused = controls.is_paper_paused();
            d.live_enabled = controls.is_live_enabled();
            d.live_paused = controls.is_live_paused();
            d.live_armed = cfg.live_armed;
            d.breaker_tripped = controls.is_breaker_tripped();
            d.initial_capital = cfg.start_cash;
            d.max_drawdown = cfg.max_drawdown;
            d.lat_polymarket_ms = lat_snap.polymarket_ms;
            d.live_bankroll = *live_bankroll.lock().unwrap();
            // PnL live : on PRIVILÉGIE le PnL réalisé par le manager (somme des clôtures réelles).
            // Sinon fallback sur le Δ bankroll (live_pnl_val) — utile avant la 1re clôture.
            d.live_pnl = if controls.live_active() {
                if live_mgr.state.shots > 0 { Some(live_mgr.state.realized_pnl) } else { live_pnl_val }
            } else { None };
            d.live_shots = live_mgr.state.shots.max(live_shots);
            d.live_force_min = cfg.live_force_min_size;
        }

        log_throttle += 1;
        if log_throttle % 100 == 0 {
            tracing::info!(real = format!("{:.3}", real_up), shots = paper.state.shots,
                cash = format!("{:.2}", paper.state.cash), "executor");
        }
    }
}
