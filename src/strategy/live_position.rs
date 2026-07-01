//! Gestion **symétrique au `PaperEngine`** pour les ordres LIVE Polymarket.
//!
//! Bloc C : machine d'états `LivePhase` — plus de fallback optimiste sur `filled_size`.
//!
//! Transitions :
//!   BUY POST → filled_size Some(n>0) → Open
//!   BUY POST → filled_size None      → PendingBuy (en attente WS ou timeout)
//!   PendingBuy + FillEvent WS         → Open
//!   PendingBuy + timeout              → Reconciling{Buy}
//!   Open → SELL POST → filled_size Some → Idle
//!   Open → SELL POST → filled_size None → PendingSell
//!   PendingSell + FillEvent WS SELL   → Idle
//!   PendingSell + timeout             → Reconciling{Sell}
//!
//! Invariants :
//!   - Une seule position à la fois (Idle → PendingBuy → Open → PendingSell → Idle).
//!   - Jamais de second SELL si phase != Open.
//!   - Aucun POST si LIVE_ARMED=false.
//!   - Notionnel ≥ $1 enforced sur BUY + SELL.

use std::fs;
use std::io::Write as _;

use serde::{Deserialize, Serialize};

use crate::concurrency::bus::Side;
use crate::polymarket::live_executor::{self, LiveCredentials, OrderArgs, OrderKind, PlaceResult};
use crate::polymarket::order_engine::OrderResult;
use crate::polymarket::relayer::PolyBook;
use crate::strategy::bankroll::KellyParams;

/// Au-delà de ce délai après l'ouverture, un `balance: 0` n'est plus du settlement on-chain
/// (BUY pas encore réglé) mais une position réellement disparue → abandon autorisé.
const SETTLE_GRACE_MS: u64 = 5000;

/// État cumulé du trading live (compteurs + PnL réalisé). Persisté sur disque.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LiveState {
    pub realized_pnl: f64,
    pub shots: u64,
    pub wins: u64,
    pub losses: u64,
    pub failed_closes: u64,
}

/// Position live ouverte. `size` = fill réel du BUY (jamais la taille demandée).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LivePosition {
    pub side: Side,
    pub token_id: String,
    pub entry_price: f64,
    pub size: f64,
    pub tp_price: f64,
    pub sl_price: f64,
    pub opened_ms: u64,
    pub neg_risk: bool,
    pub buy_order_id: String,
    /// Pas de prix du marché — mémorisé pour re-rounder tp/sl si la position grossit (fill GTC
    /// multi-trades). `serde(default)` = 0.0 pour les états persistés avant ce champ.
    #[serde(default)]
    pub tick: f64,
    /// Cumul d'achat réel (source = poll serveur du BUY), **monotone croissant**. La réconciliation
    /// d'achat se cale dessus — JAMAIS sur `size` — sinon une vente partielle (qui baisse `size`)
    /// serait « ressuscitée » par le poll au cumul d'achat. `size = bought - sold` (net vendable).
    #[serde(default)]
    pub bought: f64,
    /// Cumul de vente réalisé sur cette position. `size = bought - sold`.
    #[serde(default)]
    pub sold: f64,
    /// Stratégie 2 « favorite fin de fenêtre » : on TIENT jusqu'à la résolution (le token gagnant
    /// paie $1). La gestion ignore TP/SL/max_hold/force-exit ; seule la catastrophe vend.
    #[serde(default)]
    pub hold_to_resolution: bool,
    /// Catastrophe armée (retournement détecté) : une fois `true`, reste `true` → re-vente forcée au
    /// marché à chaque tick jusqu'au fill, même si le gap contraire reflue (pas de retour en hold).
    #[serde(default)]
    pub catastrophe_armed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReconcileKind { Buy, Sell }

/// Machine d'états du cycle de vie d'une position live.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum LivePhase {
    #[default]
    Idle,
    /// POST BUY accepté mais filled_size absent → l'ordre (GTC) dort, on attend fill WS ou timeout.
    /// `cancel_since_ms` : None = en attente ; Some(t) = annulation envoyée à t, on attend la fenêtre
    /// de grâce AVANT de déclarer Idle (un fill peut encore arriver et GAGNE toujours).
    PendingBuy {
        order_id: String,
        side: Side,
        token_id: String,
        neg_risk: bool,
        requested_size: f64,
        tick: f64,
        since_ms: u64,
        cancel_since_ms: Option<u64>,
    },
    Open(LivePosition),
    /// POST SELL accepté mais filled_size absent → attend fill WS ou timeout.
    PendingSell {
        position: LivePosition,
        reason: String,
        since_ms: u64,
    },
    /// Timeout sans fill : nécessite réconciliation manuelle.
    Reconciling {
        order_id: String,
        since_ms: u64,
        kind: ReconcileKind,
    },
}

/// Manager symétrique au `PaperEngine` mais qui touche le CLOB réel.
pub struct LivePositionManager {
    pub phase: LivePhase,
    pub state: LiveState,
    pub last_buy_ms: Option<u64>,
    pub last_sell_ms: Option<u64>,
    /// Échecs de clôture consécutifs (runtime, non persisté) — anti-boucle de SELL.
    consec_close_fails: u32,
    /// order_id d'un BUY déjà comptabilisé via la réponse HTTP (fill taker immédiat) — on ignore
    /// alors un éventuel fill WS du MÊME ordre (sinon double comptage). None = aucun (cas maker
    /// resting : le fill ne vient QUE du WS, à compter normalement).
    http_filled_buy_id: Option<String>,
    params: KellyParams,
    state_path: String,
    trades_path: String,
}

/// Compat — `position` retourne `Some` uniquement si la phase est `Open`.
impl LivePositionManager {
    pub fn position(&self) -> Option<&LivePosition> {
        if let LivePhase::Open(ref p) = self.phase { Some(p) } else { None }
    }

    /// Aucune position ni ordre en cours → on peut tirer un nouveau signal.
    pub fn is_idle(&self) -> bool { matches!(self.phase, LivePhase::Idle) }

    /// Stratégie favorite : marque la position `Open` courante comme « tenue jusqu'à résolution »
    /// (exemptée TP/SL/max_hold/force-exit ; seule la catastrophe la vend).
    pub fn mark_hold_to_resolution(&mut self) {
        if let LivePhase::Open(ref mut p) = self.phase {
            p.hold_to_resolution = true;
            self.persist();
        }
    }

    /// Arme la catastrophe (retournement) : la position re-vendra au marché à chaque tick jusqu'au
    /// fill, sans retour en hold. Idempotent.
    pub fn arm_catastrophe(&mut self) {
        if let LivePhase::Open(ref mut p) = self.phase {
            if !p.catastrophe_armed { p.catastrophe_armed = true; self.persist(); }
        }
    }

    /// order_id du BUY qu'on suit (PendingBuy en attente OU Open déjà rempli) — pour réconcilier un
    /// fill WS quel que soit le champ (taker/maker) où Polymarket place notre id.
    pub fn tracked_buy_order_id(&self) -> Option<String> {
        match &self.phase {
            LivePhase::PendingBuy { order_id, .. } => Some(order_id.clone()),
            LivePhase::Open(p) => Some(p.buy_order_id.clone()),
            _ => None,
        }
    }

    /// Anti-position-coincée : un `PendingSell` sans confirmation WS après `timeout_ms` est
    /// REPASSÉ `Open` pour que la hot loop re-tente la vente. Si la vente d'origine avait en fait
    /// rempli (WS lent), la re-vente échouera proprement (`no balance`) et la logique d'abandon
    /// réglée prendra le relais — on ne reste JAMAIS bloqué à détenir une position invendue.
    /// Retourne `true` si une re-tentative a été armée.
    pub fn revert_stuck_pending_sell(&mut self, now_ms: u64, timeout_ms: u64) -> bool {
        if let LivePhase::PendingSell { position, since_ms, .. } = &self.phase {
            if now_ms.saturating_sub(*since_ms) >= timeout_ms {
                let pos = position.clone();
                tracing::warn!(token_id = %pos.token_id, held = pos.size,
                    "⏱ PendingSell sans confirmation WS — re-tentative de vente (anti-blocage)");
                self.phase = LivePhase::Open(pos);
                self.persist();
                return true;
            }
        }
        false
    }

    /// Si un BUY GTC dort (PendingBuy) : (order_id, since_ms, cancel_since_ms) — pour le timeout maker.
    pub fn pending_buy_info(&self) -> Option<(String, u64, Option<u64>)> {
        if let LivePhase::PendingBuy { ref order_id, since_ms, cancel_since_ms, .. } = self.phase {
            Some((order_id.clone(), since_ms, cancel_since_ms))
        } else { None }
    }

    /// Marque qu'une annulation a été envoyée (timeout) — on NE passe PAS Idle tout de suite :
    /// un fill peut encore arriver pendant la fenêtre de grâce et il GAGNE (→ Open via WS).
    pub fn mark_buy_cancelling(&mut self, now_ms: u64) {
        if let LivePhase::PendingBuy { ref mut cancel_since_ms, .. } = self.phase {
            if cancel_since_ms.is_none() { *cancel_since_ms = Some(now_ms); self.persist(); }
        }
    }

    /// Confirme l'abandon d'un BUY GTC (après la fenêtre de grâce, sans fill) → Idle.
    pub fn cancel_pending_buy(&mut self) {
        if matches!(self.phase, LivePhase::PendingBuy { .. }) {
            self.phase = LivePhase::Idle;
            self.persist();
        }
    }
}

#[derive(Serialize)]
struct LiveTradeRec<'a> {
    ts: String,
    kind: &'a str,
    side: &'a str,
    price: f64,
    size: f64,
    pnl: f64,
    order_id: &'a str,
    realized_pnl_after: f64,
}

impl LivePositionManager {
    pub fn load_or_init(params: KellyParams, state_path: String, trades_path: String) -> Self {
        let state = fs::read_to_string(&state_path).ok()
            .and_then(|s| serde_json::from_str::<LiveState>(&s).ok())
            .unwrap_or_default();
        tracing::info!(realized_pnl = state.realized_pnl, shots = state.shots,
            wins = state.wins, losses = state.losses, "État LIVE chargé");
        Self { phase: LivePhase::Idle, state, last_buy_ms: None, last_sell_ms: None,
            consec_close_fails: 0, http_filled_buy_id: None, params, state_path, trades_path }
    }

    /// Tente d'ouvrir une position : POST BUY FAK.
    /// Retourne `true` si un POST a été envoyé (et n'est pas en phase non-Idle).
    #[allow(clippy::too_many_arguments)]
    pub async fn try_open(
        &mut self,
        creds: &LiveCredentials,
        live_armed: bool,
        side: Side,
        token_id: &str,
        neg_risk: bool,
        order_price: f64,
        size: f64,
        tick: f64,
        min_order_size: f64,
        now_ms: u64,
    ) -> bool {
        if !matches!(self.phase, LivePhase::Idle) { return false; }
        let size_min_cost = clob_min_size_for(min_order_size, order_price);
        let size_final = size.max(size_min_cost);
        let args = OrderArgs { side, price: order_price, size: size_final, is_sell: false };
        let t0 = tokio::time::Instant::now();
        let result = live_executor::place_order(live_armed, Some(creds), token_id, neg_risk, args, OrderKind::Fak).await;
        let buy_ms = t0.elapsed().as_millis() as u64;
        tracing::info!(buy_ms, side = side.as_str(), token_id, "⏱ latence BUY FAK");
        match result {
            Ok(PlaceResult::Placed { order_id, filled_size, avg_price, post_ms: buy_post_ms }) => {
                self.last_buy_ms = Some(buy_post_ms);
                match filled_size {
                    Some(n) if n > 0.0 => {
                        let entry = avg_price.unwrap_or(order_price);
                        self.open_position(side, token_id, neg_risk, entry, n, tick, &order_id, now_ms);
                    }
                    _ => {
                        // filled_size absent ou nul → PendingBuy, attend confirmation WS.
                        tracing::warn!(order_id = %order_id, "BUY accepté sans fill_size — PendingBuy");
                        self.phase = LivePhase::PendingBuy {
                            order_id, side, token_id: token_id.to_string(), neg_risk,
                            requested_size: size_final, tick, since_ms: now_ms, cancel_since_ms: None,
                        };
                    }
                }
                true
            }
            Ok(PlaceResult::DryRun) => false,
            Err(e) => {
                tracing::error!(error = %e, side = side.as_str(), token_id, "❌ BUY live échoué");
                false
            }
        }
    }

    /// Gère la position ouverte. Retourne `true` si la position est fermée.
    pub async fn manage(
        &mut self,
        creds: &LiveCredentials,
        live_armed: bool,
        mark_bid: Option<f64>,
        book: &PolyBook,
        min_order_size: f64,
        tick: f64,
        now_ms: u64,
        remaining_s: i64,
    ) -> bool {
        let LivePhase::Open(ref p) = self.phase else { return false };
        let Some(bid) = mark_bid else { return false };
        let (tp_price, sl_price, opened_ms, max_hold) = (p.tp_price, p.sl_price, p.opened_ms, self.params.max_hold_secs);
        let held_s = (now_ms.saturating_sub(opened_ms) / 1000) as i64;

        let reason = if bid >= tp_price { Some("take_profit") }
        else if bid <= sl_price { Some("stop_loss") }
        else if held_s >= max_hold || remaining_s <= 30 { Some("max_hold") }
        else { None };

        let Some(reason) = reason else { return false };
        let _ = book;
        let exit_target = match reason {
            "take_profit" => tp_price,
            "stop_loss"   => sl_price,
            _             => bid,
        };
        self.try_close(creds, live_armed, exit_target, min_order_size, tick, reason).await
    }

    async fn try_close(
        &mut self,
        creds: &LiveCredentials,
        live_armed: bool,
        exit_price: f64,
        min_order_size: f64,
        tick: f64,
        reason: &str,
    ) -> bool {
        let LivePhase::Open(ref p) = self.phase else { return false };
        let (side, token_id, size, entry, neg_risk) =
            (p.side, p.token_id.clone(), p.size, p.entry_price, p.neg_risk);

        let sell_price = round_tick(exit_price.clamp(0.01, 0.99), tick);
        if size * sell_price < 1.0 {
            self.state.failed_closes += 1;
            tracing::error!(reason, token_id = %token_id, size, sell_price,
                "❌ SELL impossible — notionnel < $1, position conservée");
            self.persist();
            return false;
        }
        let _ = min_order_size;
        let args = OrderArgs { side, price: sell_price, size, is_sell: true };
        let t0 = tokio::time::Instant::now();
        let result = live_executor::place_order(live_armed, Some(creds), &token_id, neg_risk, args, OrderKind::Fak).await;
        let sell_ms = t0.elapsed().as_millis() as u64;
        tracing::info!(sell_ms, reason, token_id = %token_id, "⏱ latence SELL FAK");
        match result {
            Ok(PlaceResult::Placed { order_id, filled_size, avg_price, post_ms: sell_post_ms }) => {
                self.last_sell_ms = Some(sell_post_ms);
                match filled_size {
                    Some(n) if n > 0.0 => {
                        let got = avg_price.expect("avg_price doit accompagner filled_size");
                        self.record_close(order_id, side.as_str(), n, got, entry, reason);
                        true
                    }
                    _ => {
                        // filled_size absent → PendingSell, attend confirmation WS.
                        tracing::warn!(order_id = %order_id, reason, "SELL accepté sans fill_size — PendingSell");
                        let pos = if let LivePhase::Open(p) = std::mem::replace(&mut self.phase, LivePhase::Idle) {
                            p
                        } else { unreachable!() };
                        self.phase = LivePhase::PendingSell {
                            position: pos, reason: reason.to_string(), since_ms: sell_ms,
                        };
                        false
                    }
                }
            }
            Ok(PlaceResult::DryRun) => false,
            Err(e) => {
                self.state.failed_closes += 1;
                tracing::error!(error = %e, reason, token_id = %token_id,
                    "❌ SELL live échoué — ré-essai au prochain tick");
                self.persist();
                false
            }
        }
    }

    /// Callback BUY depuis OrderEngine (hot loop non-bloquante).
    #[allow(clippy::too_many_arguments)]
    pub fn on_buy_result(
        &mut self,
        res: OrderResult,
        side: Side,
        token_id: &str,
        neg_risk: bool,
        order_price: f64,
        _size: f64,
        tick: f64,
        now_ms: u64,
    ) {
        match res {
            OrderResult::Placed { order_id, filled_size, avg_price, post_ms, .. } => {
                self.last_buy_ms = Some(post_ms);
                match filled_size {
                    Some(n) if n > 0.0 => {
                        // Fill TAKER immédiat compté ici (HTTP). On le mémorise pour ignorer un
                        // éventuel doublon WS du même ordre (anti-double-comptage).
                        self.http_filled_buy_id = Some(order_id.clone());
                        let entry = avg_price.unwrap_or(order_price);
                        self.open_position(side, token_id, neg_risk, entry, n, tick, &order_id, now_ms);
                    }
                    _ => {
                        tracing::warn!(order_id = %order_id, "BUY (Engine) sans fill_size — PendingBuy");
                        self.phase = LivePhase::PendingBuy {
                            order_id, side, token_id: token_id.to_string(), neg_risk,
                            requested_size: _size, tick, since_ms: now_ms, cancel_since_ms: None,
                        };
                    }
                }
            }
            OrderResult::DryRun => {}
            OrderResult::Cancelled { .. } => {} // géré côté hot loop, pas ici
            OrderResult::Failed { error, .. } => {
                tracing::error!(error = %error, "❌ BUY live échoué (OrderEngine)");
            }
        }
    }

    /// Callback SELL depuis OrderEngine. `now_ms` sert à distinguer un `balance: 0` de
    /// settlement (BUY pas encore réglé on-chain → ré-essai) d'une position vraiment disparue
    /// (fermée à la main / réglée à l'expiration → abandon).
    /// Retourne `true` si l'appelant doit rafraîchir le cache **CONDITIONAL** du token (le SELL a
    /// été rejeté « balance 0 », souvent un cache d'allowance périmé après le BUY — pas une position
    /// disparue). L'appelant lance alors `sync_conditional_allowance(token_id)` hors hot-loop.
    #[must_use]
    pub fn on_sell_result(&mut self, res: OrderResult, reason: &str, now_ms: u64, min_order_size: f64) -> bool {
        match res {
            OrderResult::Placed { order_id, filled_size, avg_price, post_ms, .. } => {
                self.last_sell_ms = Some(post_ms);
                match filled_size {
                    Some(n) if n > 0.0 => {
                        let got = avg_price.expect("avg_price accompagne filled_size");
                        let pos_info = if let LivePhase::Open(p) = &self.phase {
                            Some((p.entry_price, p.side.as_str().to_string(), p.size))
                        } else { None };
                        match pos_info {
                            // Fill PARTIEL re-vendable : encaisse le PnL des n vendus, réduit la
                            // position et reste Open → la manage re-vend le reste au prochain tick.
                            Some((entry, side, pos_size)) if pos_size - n >= min_order_size => {
                                let pnl = (got - entry) * n;
                                self.state.realized_pnl += pnl;
                                self.append("close_partial", &side, got, n, pnl, &order_id);
                                if let LivePhase::Open(ref mut p) = self.phase {
                                    p.sold += n; p.size = (p.bought - p.sold).max(0.0);
                                }
                                self.consec_close_fails = 0;
                                self.persist();
                                tracing::warn!(sold = n, remaining = pos_size - n,
                                    pnl = format!("{pnl:.2}"), "↩ SELL partiel — on re-vend le reste");
                            }
                            // Fill complet (ou reste < min d'échange = dust invendable) → clôture.
                            Some((entry, side, pos_size)) => {
                                self.record_close(order_id, &side, n, got, entry, reason);
                                let remaining = pos_size - n;
                                if remaining > 0.01 {
                                    tracing::warn!(dust = remaining,
                                        "⚠ reliquat invendable (< min d'échange) — se règle à l'expiration");
                                }
                            }
                            // Phase != Open (déjà clôturée via WS ?) — best effort.
                            None => self.record_close(order_id, "?", n, got, 0.0, reason),
                        }
                    }
                    _ => {
                        tracing::warn!(order_id = %order_id, reason, "SELL (Engine) sans fill_size — PendingSell");
                        if let LivePhase::Open(pos) = std::mem::replace(&mut self.phase, LivePhase::Idle) {
                            self.phase = LivePhase::PendingSell {
                                position: pos, reason: reason.to_string(), since_ms: now_ms,
                            };
                        }
                    }
                }
            }
            OrderResult::DryRun => {}
            OrderResult::Cancelled { .. } => {} // géré côté hot loop, pas ici
            OrderResult::Failed { error, .. } => {
                self.state.failed_closes += 1;
                self.consec_close_fails += 1;
                let no_balance = error.to_lowercase().contains("balance");
                // Le serveur indique souvent le solde RÉEL détenu dans l'erreur ("balance: N" en base
                // units). Si N > 0, on détient encore des tokens → ON NE DOIT PAS ABANDONNER : on
                // recale la taille sur ce solde (anti « on vend plus qu'on a ») et on re-tentera.
                let held_tokens = parse_balance_base_units(&error);
                if let Some(held) = held_tokens {
                    if held > 0.0 {
                        if let LivePhase::Open(ref mut p) = self.phase {
                            if p.size > held + 1e-9 {
                                tracing::warn!(suivi = p.size, detenu = held,
                                    "⚠️ taille suivie > solde réel — recalée sur le solde serveur (anti-oversell)");
                                // Garde l'invariant size = bought - sold (le serveur fait foi sur le net).
                                p.sold = (p.bought - held).max(0.0);
                                p.size = held;
                            }
                        }
                        tracing::warn!(reason, detenu = held, consec = self.consec_close_fails,
                            "❌ SELL rejeté (solde détenu > 0) — taille recalée, ré-essai (JAMAIS d'abandon tant qu'on détient)");
                        self.persist();
                        return false; // le cache voit nos tokens → inutile de le rafraîchir
                    }
                }
                let held_ms = match &self.phase {
                    LivePhase::Open(p) => now_ms.saturating_sub(p.opened_ms),
                    _ => u64::MAX,
                };
                let settled = held_ms >= SETTLE_GRACE_MS;
                // `balance: 0` (ou absent) n'est PAS une preuve de position disparue : le plus souvent
                // c'est le cache d'allowance CONDITIONAL périmé (pas rafraîchi après le BUY) OU le BUY
                // pas encore réglé on-chain. On demande un REFRESH du cache (retour `true`) et on
                // ré-essaie. On n'ABANDONNE qu'après settlement ET plusieurs échecs (cache déjà
                // rafraîchi, vrai 0) — sinon on jetait une position qu'on détient (« on perd l'ordre »).
                if no_balance && held_tokens.map_or(true, |h| h <= 0.0) {
                    if !settled || self.consec_close_fails < 3 {
                        tracing::warn!(reason, held_ms, consec = self.consec_close_fails,
                            "⏳ SELL « balance 0 » — refresh cache CONDITIONAL + ré-essai (cache périmé ou settlement)");
                        self.persist();
                        return true;
                    }
                    tracing::error!(error = %error, reason, held_ms,
                        "🛑 position introuvable on-chain après refresh+settlement (balance=0) — phase → Idle, voir bankroll");
                    self.phase = LivePhase::Idle;
                    self.state.losses += 1;
                    self.consec_close_fails = 0;
                    self.http_filled_buy_id = None;
                    self.persist();
                    return false;
                }
                // Erreur non-`balance` : on garde la position et on ré-essaie. Au-delà de 5 échecs
                // d'affilée on alerte fort (carnet vide ? ordre malformé ?) sans JAMAIS abandonner.
                if self.consec_close_fails >= 5 {
                    tracing::error!(error = %error, reason, consec = self.consec_close_fails, held_ms,
                        "🚨 SELL échoue en boucle (position TOUJOURS détenue) — ré-essai jusqu'au rollover, vérifier le carnet");
                } else {
                    tracing::error!(error = %error, reason, consec = self.consec_close_fails,
                        "❌ SELL live échoué (OrderEngine) — ré-essai au prochain tick");
                }
                self.persist();
                return false;
            }
        }
        // Placé/DryRun/Cancelled : aucun rejet « balance », pas de refresh à demander.
        false
    }

    /// Confirmation WS d'un fill SELL — clôture proprement. Conservé (tests + réconciliation
    /// future) : aujourd'hui les ventes FAK taker sont vues via la réponse HTTP (`on_sell_result`).
    #[allow(dead_code)]
    pub fn apply_close(
        &mut self,
        order_id: String,
        filled_size: Option<f64>,
        avg_price: Option<f64>,
        reason: &str,
        min_order_size: f64,
    ) {
        let (entry, side_str, pos_size) = match &self.phase {
            LivePhase::Open(p) => (p.entry_price, p.side.as_str().to_string(), p.size),
            LivePhase::PendingSell { position: p, .. } => (p.entry_price, p.side.as_str().to_string(), p.size),
            _ => {
                tracing::warn!(reason, "apply_close ignoré — phase != Open/PendingSell");
                return;
            }
        };
        let Some(n) = filled_size.filter(|&n| n > 0.0) else {
            tracing::error!(reason, order_id = %order_id, "apply_close : filled_size nul — PendingSell conservé");
            self.state.failed_closes += 1;
            self.persist();
            return;
        };
        let got = avg_price.unwrap_or(0.0);
        // Fill PARTIEL re-vendable : encaisse le PnL des n vendus, réduit la position et REPASSE
        // Open → la hot loop re-vend le reste. Sinon le reliquat serait orphelin (bot Idle → rachète).
        if pos_size - n >= min_order_size {
            let pnl = (got - entry) * n;
            self.state.realized_pnl += pnl;
            self.append("close_partial", &side_str, got, n, pnl, &order_id);
            let mut pos = match std::mem::replace(&mut self.phase, LivePhase::Idle) {
                LivePhase::Open(p) => p,
                LivePhase::PendingSell { position, .. } => position,
                _ => unreachable!("pos_size venait d'Open/PendingSell"),
            };
            pos.sold += n;
            pos.size = (pos.bought - pos.sold).max(0.0);
            tracing::warn!(sold = n, remaining = pos.size, pnl = format!("{pnl:.2}"),
                "↩ SELL partiel (WS) — on re-vend le reste");
            self.phase = LivePhase::Open(pos);
            self.consec_close_fails = 0;
            self.persist();
            return;
        }
        self.record_close(order_id, &side_str, n, got, entry, reason);
    }

    /// La fenêtre 5 min a expiré (rollover) : le token de la position n'est plus tradable, la
    /// position est réglée on-chain. On la clôture au dernier prix observé (`mark_price`, estimation ;
    /// la bankroll réelle fait foi). Évite le faux "TP" sur le token du marché suivant.
    pub fn resolve_expired(&mut self, mark_price: f64) {
        if let LivePhase::Open(p) = self.phase.clone() {
            let mark = mark_price.clamp(0.0, 1.0);
            self.record_close(p.buy_order_id.clone(), p.side.as_str(), p.size, mark, p.entry_price, "expired");
        }
    }

    /// Confirmation WS d'un fill BUY → réconcilie `PendingBuy → Open` (le POST n'avait pas
    /// renvoyé de `filled_size`). Utilise le `neg_risk` mémorisé dans `PendingBuy`.
    pub fn on_fill_confirmed_buy(&mut self, order_id: &str, filled_size: f64, avg_price: f64, now_ms: u64) {
        if filled_size <= 0.0 { return; }
        // Anti-double-comptage : ce fill a déjà été pris en compte via la réponse HTTP (taker-cross).
        if self.http_filled_buy_id.as_deref() == Some(order_id) {
            tracing::debug!(order_id, "fill BUY WS ignoré — déjà comptabilisé via HTTP");
            return;
        }
        match self.phase.clone() {
            LivePhase::PendingBuy { order_id: pend_id, side, token_id, neg_risk, tick, .. } => {
                // Ne réconcilier que si le fill correspond au BUY en attente (sécurité multi-ordres).
                if !pend_id.is_empty() && order_id != pend_id {
                    tracing::warn!(fill = %order_id, pending = %pend_id, "fill BUY WS d'un autre ordre — ignoré");
                    return;
                }
                tracing::info!(order_id, filled_size, avg_price, "✅ PendingBuy réconcilié via user WS → Open");
                self.open_position(side, &token_id, neg_risk, avg_price, filled_size, tick, order_id, now_ms);
            }
            // Déjà Open : le WS ne dimensionne PAS la position (il enverrait des incréments, le poll
            // serveur envoie le cumulatif → mélanger double-compterait). La taille est calée par
            // `reconcile_buy_to_server` (poll). Ici on ne fait rien (le poll grossira si besoin).
            LivePhase::Open(_) if order_id == self.tracked_buy_order_id().as_deref().unwrap_or("") => {
                tracing::debug!(order_id, "fill BUY WS sur position déjà Open — taille gérée par le poll serveur");
            }
            _ => {
                tracing::warn!(fill = %order_id, phase = ?std::mem::discriminant(&self.phase),
                    "fill BUY WS sans PendingBuy/Open correspondant — ignoré");
            }
        }
    }

    /// Réconciliation par PULL serveur (source de vérité, idempotente). `server_filled` = quantité
    /// CUMULÉE réellement remplie selon le CLOB. On cale la taille suivie dessus :
    /// - `PendingBuy` → on ADOPTE la position (le WS a pu rater le fill) ;
    /// - `Open` (même ordre) → on GROSSIT si le serveur en voit plus (jamais on ne réduit) ;
    /// - sinon → rien.
    /// Idempotent : un même cumulatif re-livré ne change rien (anti-double-comptage).
    pub fn reconcile_buy_to_server(&mut self, order_id: &str, server_filled: f64, price: f64, now_ms: u64) {
        if server_filled <= 0.0 { return; }
        match self.phase.clone() {
            LivePhase::PendingBuy { order_id: pend_id, side, token_id, neg_risk, tick, .. } => {
                if !pend_id.is_empty() && order_id != pend_id { return; }
                tracing::warn!(order_id, server_filled, price,
                    "🛟 fill confirmé par POLL serveur (hors WS) — adoption de la position");
                self.open_position(side, &token_id, neg_risk, price, server_filled, tick, order_id, now_ms);
            }
            LivePhase::Open(mut p) if order_id == p.buy_order_id => {
                // On compare au cumul d'ACHAT (`bought`), pas à `size` : sinon une vente partielle
                // (qui a baissé `size`) serait ressuscitée jusqu'au cumul d'achat → on re-détiendrait
                // ce qu'on vient de vendre. On ne grossit que si le serveur voit STRICTEMENT plus
                // d'ACHAT qu'enregistré (tolérance dust) → idempotent.
                if server_filled > p.bought + 1e-6 {
                    let added = server_filled - p.bought;
                    let tick = if p.tick > 0.0 { p.tick } else { 0.01 };
                    p.entry_price = (p.entry_price * p.bought + price * added) / server_filled;
                    p.bought = server_filled;
                    p.size = (p.bought - p.sold).max(0.0); // net vendable (ne ressuscite pas les ventes)
                    p.tp_price = round_tick((p.entry_price + self.params.tp_cents / 100.0).min(0.99), tick);
                    p.sl_price = round_tick((p.entry_price - self.params.sl_cents / 100.0).max(0.01), tick);
                    tracing::warn!(order_id, added, bought = server_filled, size = p.size,
                        entry = format!("{:.3}", p.entry_price),
                        "➕ poll serveur : achat calé sur le cumulatif réel (net vendable préservé)");
                    self.append("reconcile_grow", p.side.as_str(), price, added, 0.0, order_id);
                    self.phase = LivePhase::Open(p);
                    self.persist();
                }
            }
            _ => {}
        }
    }

    fn open_position(
        &mut self,
        side: Side,
        token_id: &str,
        neg_risk: bool,
        entry: f64,
        filled: f64,
        tick: f64,
        order_id: &str,
        now_ms: u64,
    ) {
        let tp = round_tick((entry + self.params.tp_cents / 100.0).min(0.99), tick);
        let sl = round_tick((entry - self.params.sl_cents / 100.0).max(0.01), tick);
        self.state.shots += 1;
        let pos = LivePosition {
            side, token_id: token_id.to_string(), entry_price: entry, size: filled,
            tp_price: tp, sl_price: sl, opened_ms: now_ms, neg_risk,
            buy_order_id: order_id.to_string(), tick,
            bought: filled, sold: 0.0,
            hold_to_resolution: false, catastrophe_armed: false,
        };
        self.append("open", side.as_str(), entry, filled, 0.0, order_id);
        tracing::warn!(side = side.as_str(), token_id, entry = format!("{entry:.3}"),
            size = filled, tp = format!("{tp:.2}"), sl = format!("{sl:.2}"),
            order_id = %order_id, "🎯 SNIPE LIVE");
        self.phase = LivePhase::Open(pos);
        self.consec_close_fails = 0;
        self.persist();
    }

    fn record_close(&mut self, order_id: String, side: &str, sold: f64, got_price: f64, entry: f64, reason: &str) {
        let pnl = (got_price - entry) * sold;
        self.state.realized_pnl += pnl;
        if pnl >= 0.0 { self.state.wins += 1 } else { self.state.losses += 1 }
        let kind = match reason {
            "take_profit" => "close_tp", "stop_loss" => "close_sl",
            "expired" => "close_expired", "window_close" => "close_window",
            _ => "close_max_hold",
        };
        self.append(kind, side, got_price, sold, pnl, &order_id);
        tracing::warn!(reason, exit = format!("{got_price:.3}"), pnl = format!("{pnl:.2}"),
            realized_pnl = format!("{:.2}", self.state.realized_pnl), order_id = %order_id, "✖ clôture LIVE");
        self.phase = LivePhase::Idle;
        self.consec_close_fails = 0;
        self.http_filled_buy_id = None;
        self.persist();
    }

    #[allow(dead_code)]
    pub fn hit_rate(&self) -> f64 {
        let n = self.state.wins + self.state.losses;
        if n == 0 { 0.0 } else { self.state.wins as f64 / n as f64 }
    }

    fn persist(&self) {
        #[derive(Serialize)]
        struct Snapshot<'a> { state: &'a LiveState, phase: &'a LivePhase }
        let snap = Snapshot { state: &self.state, phase: &self.phase };
        let tmp = format!("{}.tmp", self.state_path);
        if let Ok(j) = serde_json::to_string_pretty(&snap) {
            if fs::write(&tmp, j).is_ok() { let _ = fs::rename(&tmp, &self.state_path); }
        }
    }

    fn append(&self, kind: &str, side: &str, price: f64, size: f64, pnl: f64, order_id: &str) {
        let rec = LiveTradeRec {
            ts: chrono::Utc::now().to_rfc3339(), kind, side, price, size, pnl, order_id,
            realized_pnl_after: self.state.realized_pnl,
        };
        if let Ok(line) = serde_json::to_string(&rec) {
            if let Ok(mut f) = fs::OpenOptions::new().create(true).append(true).open(&self.trades_path) {
                let _ = writeln!(f, "{line}");
            }
        }
    }
}

fn clob_min_size_for(min_order_size_tokens: f64, price: f64) -> f64 {
    let by_notional = if price > 0.0 { (1.0 / price).ceil() } else { min_order_size_tokens };
    min_order_size_tokens.max(by_notional)
}

fn round_tick(p: f64, tick: f64) -> f64 {
    if tick <= 0.0 { return p; }
    ((p / tick).round() * tick).clamp(0.01, 0.99)
}

/// Extrait le solde RÉEL détenu d'une erreur CLOB du type
/// `"...the balance is not enough -> balance: 4995787, order amount: 5000000"`.
/// Renvoie le solde en TOKENS (base units / 1e6), ou `None` si absent. Sert à recaler la taille de
/// vente sur ce qu'on possède vraiment et à ne JAMAIS abandonner une position encore détenue.
fn parse_balance_base_units(error: &str) -> Option<f64> {
    let low = error.to_lowercase();
    let idx = low.find("balance:")?;
    let after = &error[idx + "balance:".len()..];
    let digits: String = after.trim_start().chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() { return None; }
    digits.parse::<f64>().ok().map(|base| base / 1_000_000.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mgr() -> LivePositionManager {
        LivePositionManager::load_or_init(
            KellyParams { kelly_fraction: 0.5, max_size_pct: 0.10, tp_cents: 4.0, sl_cents: 3.0, max_hold_secs: 60 },
            "/tmp/live_state_test_phase_c.json".into(),
            "/tmp/live_trades_test_phase_c.jsonl".into(),
        )
    }

    // Manager sur fichier d'état dédié (évite la collision /tmp entre tests parallèles).
    fn mgr_named(tag: &str) -> LivePositionManager {
        let p = format!("/tmp/live_state_test_{tag}.json");
        let _ = fs::remove_file(&p);
        LivePositionManager::load_or_init(
            KellyParams { kelly_fraction: 0.5, max_size_pct: 0.10, tp_cents: 4.0, sl_cents: 3.0, max_hold_secs: 60 },
            p.into(),
            format!("/tmp/live_trades_test_{tag}.jsonl").into(),
        )
    }

    #[test]
    fn fresh_manager_has_no_position_no_state() {
        let m = mgr();
        assert!(m.position().is_none());
        assert!(matches!(m.phase, LivePhase::Idle));
        assert_eq!(m.state.shots, 0);
    }

    #[test]
    fn buy_result_with_fill_opens_position() {
        let mut m = mgr();
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        assert!(matches!(m.phase, LivePhase::Open(_)));
        assert_eq!(m.state.shots, 1);
        let _ = fs::remove_file("/tmp/live_state_test_phase_c.json");
    }

    #[test]
    fn buy_result_without_fill_goes_pending_buy() {
        let mut m = mgr();
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: None, avg_price: None,
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        // Pas de position ouverte sans fill confirmé.
        assert!(m.position().is_none());
        assert!(matches!(m.phase, LivePhase::PendingBuy { .. }), "phase doit être PendingBuy, got {:?}", m.phase);
        assert_eq!(m.state.shots, 0, "shots ne doit pas s'incrémenter avant fill");
    }

    #[test]
    fn fill_wins_the_cancel_race() {
        // Anti-orpheline : un BUY GTC resting passe en annulation (timeout), MAIS le fill arrive
        // pendant la fenêtre de grâce → la position DOIT s'ouvrir (le fill gagne toujours).
        let mut m = mgr_named("fill_wins_race");
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: None, avg_price: None,
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        assert!(matches!(m.phase, LivePhase::PendingBuy { .. }));
        // Timeout → on marque l'annulation (mais on NE passe PAS Idle).
        m.mark_buy_cancelling(2000);
        match m.pending_buy_info() {
            Some((id, _, cancel_since)) => {
                assert_eq!(id, "o1");
                assert_eq!(cancel_since, Some(2000), "cancel_since doit être posé");
            }
            None => panic!("doit rester PendingBuy après mark_buy_cancelling"),
        }
        // Le fill arrive (tardif) → la position s'ouvre malgré l'annulation en cours.
        m.on_fill_confirmed_buy("o1", 10.0, 0.50, 2500);
        assert!(matches!(m.phase, LivePhase::Open(_)), "le fill doit ouvrir la position, got {:?}", m.phase);
        assert!(m.position().is_some());
    }

    #[test]
    fn http_filled_buy_ignores_duplicate_ws() {
        // Taker-cross : le BUY remplit via la réponse HTTP (filled_size présent) → position ouverte.
        // Un éventuel fill WS du MÊME ordre NE DOIT PAS re-gonfler la position (anti-double-comptage).
        let mut m = mgr_named("http_dedup");
        m.on_buy_result(
            OrderResult::Placed { order_id: "x1".into(), filled_size: Some(5.0), avg_price: Some(0.24),
                post_ms: 50 },
            Side::Up, "tok", false, 0.30, 5.0, 0.01, 1000,
        );
        assert!(matches!(m.phase, LivePhase::Open(_)));
        // Doublon WS du même order_id → ignoré.
        m.on_fill_confirmed_buy("x1", 5.0, 0.24, 1100);
        match &m.phase {
            LivePhase::Open(p) => assert!((p.size - 5.0).abs() < 1e-9, "taille inchangée (pas de double), got {}", p.size),
            other => panic!("doit rester Open, got {other:?}"),
        }
    }

    #[test]
    fn poll_reconcile_adopts_then_grows_idempotent() {
        // PULL serveur = source de vérité. Un GTC resting : le poll renvoie le CUMULATIF rempli.
        // PendingBuy → adoption ; cumulatif plus grand → on grossit ; même cumulatif re-livré →
        // AUCUN changement (idempotent, anti-double-comptage). Sans WS du tout.
        let mut m = mgr_named("poll_reconcile");
        m.on_buy_result(
            OrderResult::Placed { order_id: "g1".into(), filled_size: None, avg_price: None,
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        assert!(matches!(m.phase, LivePhase::PendingBuy { .. }));
        // Poll : 6 remplis → adoption (Open, taille 6).
        m.reconcile_buy_to_server("g1", 6.0, 0.50, 1100);
        match &m.phase {
            LivePhase::Open(p) => assert!((p.size - 6.0).abs() < 1e-9, "adoption à 6, got {}", p.size),
            other => panic!("doit être Open, got {other:?}"),
        }
        // Poll suivant : MÊME cumulatif 6 → idempotent, pas de double.
        m.reconcile_buy_to_server("g1", 6.0, 0.50, 1200);
        if let LivePhase::Open(p) = &m.phase { assert!((p.size - 6.0).abs() < 1e-9, "idempotent, got {}", p.size); }
        // Poll suivant : cumulatif 10 (le reste a rempli) → on grossit à 10 (anti-orpheline).
        m.reconcile_buy_to_server("g1", 10.0, 0.50, 1300);
        if let LivePhase::Open(p) = &m.phase {
            assert!((p.size - 10.0).abs() < 1e-9, "grossi à 10, got {}", p.size);
        } else { panic!("doit rester Open"); }
        // Poll d'un AUTRE ordre → ignoré.
        m.reconcile_buy_to_server("autre", 5.0, 0.40, 1400);
        if let LivePhase::Open(p) = &m.phase { assert!((p.size - 10.0).abs() < 1e-9, "autre ordre ignoré"); }
    }

    #[test]
    fn cancel_after_grace_without_fill_goes_idle() {
        // Si AUCUN fill n'arrive pendant la grâce, l'annulation aboutit → Idle (pas d'orpheline).
        let mut m = mgr_named("cancel_grace_idle");
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: None, avg_price: None,
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        m.mark_buy_cancelling(2000);
        m.cancel_pending_buy();
        assert!(m.is_idle(), "sans fill, l'annulation aboutie repasse Idle, got {:?}", m.phase);
        assert!(m.position().is_none());
    }

    #[test]
    fn sell_result_without_fill_goes_pending_sell() {
        let mut m = mgr();
        // Ouvre d'abord une position.
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        assert!(matches!(m.phase, LivePhase::Open(_)));
        // SELL sans fill.
        let _ = m.on_sell_result(
            OrderResult::Placed { order_id: "o2".into(), filled_size: None, avg_price: None,
                post_ms: 30 },
            "take_profit", 1000, 5.0,
        );
        assert!(matches!(m.phase, LivePhase::PendingSell { .. }), "doit être PendingSell, got {:?}", m.phase);
        let _ = fs::remove_file("/tmp/live_state_test_phase_c.json");
    }

    #[test]
    fn apply_close_with_fill_clears_position() {
        let mut m = mgr();
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        m.apply_close("o2".into(), Some(10.0), Some(0.54), "take_profit", 5.0);
        assert!(matches!(m.phase, LivePhase::Idle));
        assert_eq!(m.state.wins, 1);
        assert!((m.state.realized_pnl - 0.40).abs() < 1e-6, "pnl = (0.54-0.50)*10 = 0.40");
        let _ = fs::remove_file("/tmp/live_state_test_phase_c.json");
    }

    #[test]
    fn apply_close_without_fill_keeps_pending_sell() {
        let mut m = mgr();
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        // Force PendingSell.
        let _ = m.on_sell_result(
            OrderResult::Placed { order_id: "o2".into(), filled_size: None, avg_price: None,
                post_ms: 30 },
            "stop_loss", 1000, 5.0,
        );
        // apply_close sans fill : ne doit pas clôturer.
        m.apply_close("o2".into(), None, None, "stop_loss", 5.0);
        assert!(!matches!(m.phase, LivePhase::Idle), "Idle sans fill = dangereux");
        let _ = fs::remove_file("/tmp/live_state_test_phase_c.json");
    }

    #[test]
    fn parse_balance_from_clob_error() {
        let e = "not enough balance / allowance: the balance is not enough -> balance: 4995787, order amount: 5000000";
        let held = parse_balance_base_units(e).expect("doit parser le solde");
        assert!((held - 4.995787).abs() < 1e-9, "solde = 4.995787 tokens, got {held}");
        assert!(parse_balance_base_units("réseau timeout").is_none());
        assert_eq!(parse_balance_base_units("the balance is not enough -> balance: 0, order amount: 5000000"), Some(0.0));
    }

    #[test]
    fn oversell_resizes_position_never_abandons() {
        // Bug prod : taille suivie (5.0) > détenu réel (4.995787) → POST rejeté "not enough balance".
        // On DOIT recaler la position sur le solde serveur et NE PAS abandonner (sinon orpheline).
        let mut m = mgr_named("oversell_resize");
        m.on_buy_result(
            OrderResult::Placed { order_id: "b1".into(), filled_size: Some(5.0), avg_price: Some(0.24),
                post_ms: 50 },
            Side::Down, "tok", false, 0.24, 5.0, 0.01, 1000,
        );
        assert!(matches!(m.phase, LivePhase::Open(_)));
        // SELL rejeté : le serveur dit qu'on détient 4.995787 (held_ms grand = settled).
        let err = "the balance is not enough -> balance: 4995787, order amount: 5000000".to_string();
        let _ = m.on_sell_result(OrderResult::Failed { error: err }, "stop_loss", 60_000, 5.0);
        match &m.phase {
            LivePhase::Open(p) => assert!((p.size - 4.995787).abs() < 1e-6,
                "taille recalée sur le solde réel, got {}", p.size),
            other => panic!("doit RESTER Open (jamais abandon tant qu'on détient), got {other:?}"),
        }
    }

    #[test]
    fn sell_balance_zero_asks_refresh_and_keeps_position() {
        // « balance 0 » au SELL = cache CONDITIONAL périmé (pas une position perdue). on_sell_result
        // doit DEMANDER un refresh (true) et GARDER la position, jamais l'abandonner d'emblée.
        let mut m = mgr_named("sell_balance_zero_refresh");
        m.on_buy_result(
            OrderResult::Placed { order_id: "b1".into(), filled_size: Some(5.0), avg_price: Some(0.30), post_ms: 50 },
            Side::Up, "tok", false, 0.30, 5.0, 0.01, 1000,
        );
        let err = "the balance is not enough -> balance: 0, order amount: 5000000".to_string();
        // Même APRÈS settlement, le 1er échec demande un refresh et conserve la position.
        let need = m.on_sell_result(OrderResult::Failed { error: err.clone() }, "stop_loss", 60_000, 5.0);
        assert!(need, "doit demander un refresh CONDITIONAL");
        assert!(matches!(m.phase, LivePhase::Open(_)), "ne PAS abandonner au 1er balance:0, got {:?}", m.phase);
        // Échecs répétés post-settlement (cache rafraîchi, vrai 0) → abandon contrôlé.
        let _ = m.on_sell_result(OrderResult::Failed { error: err.clone() }, "stop_loss", 60_000, 5.0);
        let _ = m.on_sell_result(OrderResult::Failed { error: err }, "stop_loss", 60_000, 5.0);
        assert!(m.is_idle(), "après 3 échecs settled (vrai 0), abandon contrôlé, got {:?}", m.phase);
    }

    #[test]
    fn favorite_flags_default_false_and_settable() {
        // Stratégie 2 : une position normale n'est ni hold_to_resolution ni catastrophe_armed.
        // mark_hold_to_resolution / arm_catastrophe posent les flags sur la position Open courante.
        let mut m = mgr_named("favorite_flags");
        m.on_buy_result(
            OrderResult::Placed { order_id: "f1".into(), filled_size: Some(5.0), avg_price: Some(0.85), post_ms: 50 },
            Side::Up, "tok", false, 0.85, 5.0, 0.01, 1000,
        );
        match &m.phase {
            LivePhase::Open(p) => { assert!(!p.hold_to_resolution); assert!(!p.catastrophe_armed); }
            o => panic!("doit être Open, got {o:?}"),
        }
        m.mark_hold_to_resolution();
        m.arm_catastrophe();
        match &m.phase {
            LivePhase::Open(p) => { assert!(p.hold_to_resolution, "hold posé"); assert!(p.catastrophe_armed, "catastrophe armée"); }
            o => panic!("doit rester Open, got {o:?}"),
        }
    }

    #[test]
    fn buy_poll_does_not_resurrect_after_partial_sell() {
        // CŒUR DU FIX anti-emmêlage : achat 10, vente partielle 6 → net 4. Un poll d'achat re-livrant
        // le cumul d'achat (10) ne DOIT PAS regonfler la position (sinon on re-détient le vendu).
        let mut m = mgr_named("no_resurrect_after_partial");
        m.on_buy_result(
            OrderResult::Placed { order_id: "g1".into(), filled_size: Some(10.0), avg_price: Some(0.50), post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        // Vente partielle de 6 (min_order_size 1 → reste 4 ≥ 1 = re-vendable).
        let _ = m.on_sell_result(
            OrderResult::Placed { order_id: "s1".into(), filled_size: Some(6.0), avg_price: Some(0.55), post_ms: 30 },
            "take_profit", 1100, 1.0,
        );
        match &m.phase {
            LivePhase::Open(p) => assert!((p.size - 4.0).abs() < 1e-9, "net 4 après vente, got {}", p.size),
            o => panic!("doit rester Open, got {o:?}"),
        }
        // Poll d'achat : cumul d'achat 10 re-livré → net INCHANGÉ (4), surtout pas 10.
        m.reconcile_buy_to_server("g1", 10.0, 0.50, 1200);
        match &m.phase {
            LivePhase::Open(p) => {
                assert!((p.size - 4.0).abs() < 1e-9, "le poll ne ressuscite pas la vente, got size {}", p.size);
                assert!((p.bought - 10.0).abs() < 1e-9, "bought reste le cumul d'achat 10, got {}", p.bought);
            }
            o => panic!("doit rester Open, got {o:?}"),
        }
    }

    #[test]
    fn stuck_pending_sell_reverts_to_open_for_retry() {
        // ANTI-COINCÉE : un PendingSell sans confirmation WS au-delà du timeout doit REPASSER Open
        // (re-vente au tick suivant), jamais rester bloqué à détenir une position invendue.
        let mut m = mgr_named("stuck_pending_sell");
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        let _ = m.on_sell_result(
            OrderResult::Placed { order_id: "o2".into(), filled_size: None, avg_price: None, post_ms: 30 },
            "stop_loss", 5_000, 5.0,
        );
        assert!(matches!(m.phase, LivePhase::PendingSell { .. }));
        // Avant le timeout : on reste PendingSell.
        assert!(!m.revert_stuck_pending_sell(6_000, 3_000), "1s < timeout → pas de revert");
        assert!(matches!(m.phase, LivePhase::PendingSell { .. }));
        // Après le timeout (8s - 5s = 3s ≥ 3000) : repasse Open, position intacte (re-vendable).
        assert!(m.revert_stuck_pending_sell(8_000, 3_000), "timeout atteint → revert");
        match &m.phase {
            LivePhase::Open(p) => assert!((p.size - 10.0).abs() < 1e-9, "position intacte"),
            other => panic!("doit repasser Open, got {other:?}"),
        }
    }

    #[test]
    fn partial_ws_close_keeps_remainder_open() {
        // GAP B : un fill SELL PARTIEL confirmé via WS doit réduire la position et rester Open
        // (re-vente du reste), pas passer Idle avec un reliquat orphelin.
        let mut m = mgr_named("partial_ws_close");
        m.on_buy_result(
            OrderResult::Placed { order_id: "o1".into(), filled_size: Some(10.0), avg_price: Some(0.50),
                post_ms: 50 },
            Side::Up, "tok", false, 0.50, 10.0, 0.01, 1000,
        );
        // Vend 6 sur 10 via WS → reste 4 (≥ min 5 ? non : 4 < 5 → dust). Prends 7 pour rester ≥ min.
        m.apply_close("o2".into(), Some(7.0), Some(0.54), "take_profit", 1.0);
        match &m.phase {
            LivePhase::Open(p) => assert!((p.size - 3.0).abs() < 1e-9, "reste 3 à re-vendre, got {}", p.size),
            other => panic!("doit rester Open (reliquat re-vendable), got {other:?}"),
        }
    }

    #[test]
    fn clob_min_size_respects_dollar_notional() {
        assert_eq!(clob_min_size_for(5.0, 0.50), 5.0);
        assert_eq!(clob_min_size_for(5.0, 0.10), 10.0);
        assert_eq!(clob_min_size_for(5.0, 0.20), 5.0);
        assert_eq!(clob_min_size_for(5.0, 0.19), 6.0);
    }

    #[test]
    fn round_tick_clamps_to_valid_range() {
        assert_eq!(round_tick(0.5234, 0.01), 0.52);
        assert_eq!(round_tick(0.005, 0.01), 0.01);
        assert_eq!(round_tick(0.999, 0.01), 0.99);
    }
}
