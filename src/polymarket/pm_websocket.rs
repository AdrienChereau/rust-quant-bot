//! WebSocket Polymarket — carnets temps réel (Phase 2).
//!
//! Se connecte à `wss://ws-subscriptions-clob.polymarket.com/ws/market`, souscrit aux tokens
//! Up/Down du marché actif, met à jour `PmShared.up_book` / `down_book` + champs top-of-book
//! dénormalisés. Reconnexion exponentielle avec jitter.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::relayer::{Level, PolyBook, PmShared};

const PM_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";
const PING_INTERVAL_S: u64 = 10;
const MAX_BACKOFF_S: u64 = 60;

/// Lance la tâche WS carnets en arrière-plan.
/// Remplace progressivement le REST polling : quand le WS est actif, `last_ws_ts_ms` est mis
/// à jour et `pm_poller` peut arrêter d'appeler `get_book()`.
pub fn spawn_market_ws(pm: Arc<Mutex<PmShared>>) {
    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            // Récupère les tokens depuis le marché courant.
            let tokens = {
                let g = pm.lock().unwrap();
                g.market.as_ref().map(|m| vec![m.up_token_id.clone(), m.down_token_id.clone()])
            };
            let Some(tokens) = tokens else {
                // Pas encore de marché — attend 1 s puis réessaie.
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            };

            match run_ws_session(&pm, &tokens).await {
                Ok(()) => {
                    tracing::info!("pm_ws: session terminée proprement, reconnexion dans {backoff}s");
                }
                Err(e) => {
                    tracing::warn!(error = %e, backoff, "pm_ws: erreur, reconnexion dans {backoff}s");
                }
            }
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_S);
        }
    });
}

async fn run_ws_session(pm: &Arc<Mutex<PmShared>>, tokens: &[String]) -> anyhow::Result<()> {
    let (ws, _) = connect_async(PM_WS_URL).await?;
    let (mut sink, mut stream) = ws.split();

    // Subscribe au canal market pour les tokens Up/Down.
    let sub = serde_json::json!({
        "assets_ids": tokens,
        "type": "market",
        "custom_feature_enabled": true
    });
    sink.send(Message::Text(sub.to_string())).await?;
    tracing::info!(tokens = ?tokens, "pm_ws: souscription marché envoyée");

    let mut ping_interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_S));
    ping_interval.tick().await; // consomme le tick immédiat

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(Message::Text(txt))) => {
                        if let Err(e) = process_message(&txt, pm) {
                            tracing::debug!(error = %e, "pm_ws: message ignoré");
                        }
                    }
                    Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(_)) => {}
                }
            }
            _ = ping_interval.tick() => {
                sink.send(Message::Ping(vec![])).await?;
            }
        }
    }
}

fn process_message(txt: &str, pm: &Arc<Mutex<PmShared>>) -> anyhow::Result<()> {
    // Le CLOB WS envoie un tableau d'events.
    let events: Vec<WsEvent> = serde_json::from_str(txt)?;
    let now_ms = chrono::Utc::now().timestamp_millis() as u64;

    let mut g = pm.lock().unwrap();
    for ev in events {
        let up_tok = g.market.as_ref().map(|m| m.up_token_id.clone());
        let dn_tok = g.market.as_ref().map(|m| m.down_token_id.clone());
        match ev.event_type.as_deref() {
            Some("book") => {
                // Snapshot complet du carnet.
                let book = build_book(&ev);
                let mid = book.mid().unwrap_or(0.0);
                if up_tok.as_deref() == Some(ev.asset_id.as_deref().unwrap_or("")) {
                    g.real_up = mid;
                    g.up_best_bid = book.best_bid().unwrap_or(0.0);
                    g.up_best_ask = book.best_ask().unwrap_or(1.0);
                    g.up_book = Arc::new(book);
                } else if dn_tok.as_deref() == Some(ev.asset_id.as_deref().unwrap_or("")) {
                    g.down_best_bid = book.best_bid().unwrap_or(0.0);
                    g.down_best_ask = book.best_ask().unwrap_or(1.0);
                    g.down_book = Arc::new(book);
                }
                g.last_ws_ts_ms = now_ms;
            }
            Some("price_change") => {
                // Delta niveaux.
                apply_price_change(&ev, &up_tok, &dn_tok, &mut g, now_ms);
            }
            Some("tick_size_change") => {
                // Invalide les metas de token.
                #[cfg(feature = "live")]
                if let Some(asset) = &ev.asset_id {
                    crate::polymarket::poly1271::invalidate_token_meta(asset);
                }
            }
            _ => {}
        }
    }
    Ok(())
}

fn build_book(ev: &WsEvent) -> PolyBook {
    PolyBook {
        bids: ev.bids.iter().filter_map(|l| parse_level(l)).collect(),
        asks: ev.asks.iter().filter_map(|l| parse_level(l)).collect(),
    }
}

fn apply_price_change(
    ev: &WsEvent,
    up_tok: &Option<String>,
    dn_tok: &Option<String>,
    g: &mut std::sync::MutexGuard<PmShared>,
    now_ms: u64,
) {
    let asset = ev.asset_id.as_deref().unwrap_or("");
    let is_up = up_tok.as_deref() == Some(asset);
    let is_dn = !is_up && dn_tok.as_deref() == Some(asset);
    if !is_up && !is_dn { return; }

    // Clone le book actuel pour le modifier.
    let book = if is_up {
        Arc::make_mut(&mut g.up_book)
    } else {
        Arc::make_mut(&mut g.down_book)
    };

    for change in &ev.changes {
        let Some(price) = parse_f64(&change.price) else { continue };
        let Some(size) = parse_f64(&change.size) else { continue };
        let levels = if change.side.as_deref() == Some("BUY") { &mut book.bids } else { &mut book.asks };
        if size == 0.0 {
            levels.retain(|l| (l.price - price).abs() > 1e-9);
        } else if let Some(l) = levels.iter_mut().find(|l| (l.price - price).abs() < 1e-9) {
            l.size = size;
        } else {
            levels.push(Level { price, size });
        }
    }

    if is_up {
        g.real_up = g.up_book.mid().unwrap_or(g.real_up);
        g.up_best_bid = g.up_book.best_bid().unwrap_or(0.0);
        g.up_best_ask = g.up_book.best_ask().unwrap_or(1.0);
    } else {
        g.down_best_bid = g.down_book.best_bid().unwrap_or(0.0);
        g.down_best_ask = g.down_book.best_ask().unwrap_or(1.0);
    }
    g.last_ws_ts_ms = now_ms;
}

fn parse_level(l: &RawLevel) -> Option<Level> {
    Some(Level { price: parse_f64(&l.price)?, size: parse_f64(&l.size)? })
}

fn parse_f64(s: &str) -> Option<f64> {
    s.parse().ok()
}

#[derive(Deserialize)]
struct WsEvent {
    #[serde(rename = "type")]
    event_type: Option<String>,
    asset_id: Option<String>,
    #[serde(default)] bids: Vec<RawLevel>,
    #[serde(default)] asks: Vec<RawLevel>,
    #[serde(default)] changes: Vec<PriceChange>,
}

#[derive(Deserialize)]
struct RawLevel {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct PriceChange {
    price: String,
    size: String,
    side: Option<String>,
}
