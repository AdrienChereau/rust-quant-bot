//! WebSocket Polymarket user — confirmation des fills (Phase 4).
//!
//! Endpoint : `wss://ws-subscriptions-clob.polymarket.com/ws/user`
//! Auth : `{"auth": {"apiKey": "...", "secret": "...", "passphrase": "..."}, ...}`
//! Subscribe : `{"markets": ["<condition_id>"], "type": "user", "auth": {...}}`
//!
//! Events reçus :
//! - `event_type = "trade"` : fill confirmé (taker_order_id, maker_order_id, size, price)
//! - `event_type = "order"` UPDATE : size_matched, price
//!
//! Lifecycle : une seule task lancée au boot via `init_user_ws()` ; le rollover marché
//! envoie le nouveau `condition_id` dans le `watch::Sender<Option<String>>` → resouscrit
//! in-session sans reconnecter.

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use super::live_executor::LiveCredentials;
use super::pm_websocket::parse_events;

const PM_USER_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/user";
const PING_INTERVAL_S: u64 = 10;
const MAX_BACKOFF_S: u64 = 60;

/// Événement de fill confirmé publié vers l'executor.
///
/// On porte les DEUX order_id : quand NOTRE ordre resting (maker) se fait remplir, Polymarket met
/// notre id dans `maker_order_id` et celui de la contrepartie dans `taker_order_id` — et le `side`
/// de l'event est celui du TAKER (l'opposé du nôtre). L'executor matche donc sur SON order_id (peu
/// importe le champ) et décide acheteur/vendeur par SA phase, pas par `taker_side_is_sell`.
#[derive(Debug, Clone)]
pub struct FillEvent {
    pub taker_order_id: Option<String>,
    pub maker_order_id: Option<String>,
    pub filled_size: f64,
    pub avg_price: f64,
    pub taker_side_is_sell: bool, // côté du TAKER (brut, pour debug/log uniquement)
}

impl FillEvent {
    /// Vrai si `id` est l'un des deux order_id du trade (on est partie au trade, taker OU maker).
    pub fn involves(&self, id: &str) -> bool {
        self.taker_order_id.as_deref() == Some(id) || self.maker_order_id.as_deref() == Some(id)
    }
}

/// Lance la task WS user **une fois** au boot.
/// Retourne :
/// - `watch::Sender<Option<String>>` : l'executor y envoie le `condition_id` du marché courant
/// - `mpsc::UnboundedReceiver<FillEvent>` : l'executor draine TOUS les fills confirmés. mpsc (pas
///   watch) car un `watch` coalesce : deux fills dans le même tick → le 1er écrasé → orphelin.
pub fn init_user_ws(
    creds: LiveCredentials,
) -> (watch::Sender<Option<String>>, mpsc::UnboundedReceiver<FillEvent>) {
    let (cond_tx, mut cond_rx) = watch::channel(None::<String>);
    let (fill_tx, fill_rx) = mpsc::unbounded_channel::<FillEvent>();

    tokio::spawn(async move {
        let mut backoff = 1u64;
        loop {
            // Attend qu'il y ait un condition_id (clone immédiat pour libérer le guard avant await).
            let condition_id = loop {
                let maybe = cond_rx.borrow().clone();
                if let Some(id) = maybe { break id; }
                if cond_rx.changed().await.is_err() { return; }
            };

            match run_ws_session(&creds, &condition_id, &mut cond_rx, &fill_tx).await {
                Ok(()) => tracing::info!("pm_user_ws: session terminée, reconnexion dans {backoff}s"),
                Err(e) => tracing::warn!(error = %e, backoff, "pm_user_ws: erreur, reconnexion dans {backoff}s"),
            }
            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff * 2).min(MAX_BACKOFF_S);
        }
    });

    (cond_tx, fill_rx)
}

async fn run_ws_session(
    creds: &LiveCredentials,
    initial_condition_id: &str,
    cond_rx: &mut watch::Receiver<Option<String>>,
    fill_tx: &mpsc::UnboundedSender<FillEvent>,
) -> anyhow::Result<()> {
    tracing::info!("pm_user_ws: connexion WS user…");
    let (ws, _) = tokio::time::timeout(
        Duration::from_secs(15),
        connect_async(PM_USER_WS_URL),
    ).await
    .map_err(|_| anyhow::anyhow!("pm_user_ws: timeout connexion (15 s)"))??;
    tracing::info!("pm_user_ws: WS user connecté ✓");
    let (mut sink, mut stream) = ws.split();

    subscribe(&mut sink, creds, initial_condition_id).await?;

    let mut ping_interval = tokio::time::interval(Duration::from_secs(PING_INTERVAL_S));
    ping_interval.tick().await;

    loop {
        tokio::select! {
            msg = stream.next() => {
                match msg {
                    None => return Ok(()),
                    Some(Err(e)) => return Err(e.into()),
                    Some(Ok(Message::Text(txt))) => {
                        process_message(&txt, fill_tx);
                    }
                    Some(Ok(Message::Close(_))) => return Ok(()),
                    Some(Ok(_)) => {}
                }
            }
            _ = ping_interval.tick() => {
                sink.send(Message::Ping(vec![])).await?;
            }
            // Rollover : nouveau condition_id → resouscrit in-session.
            Ok(()) = cond_rx.changed() => {
                let new_id = cond_rx.borrow().clone();
                if let Some(id) = new_id {
                    tracing::info!(condition_id = %id, "pm_user_ws: resouscription rollover");
                    subscribe(&mut sink, creds, &id).await?;
                }
            }
        }
    }
}

async fn subscribe(
    sink: &mut (impl SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin),
    creds: &LiveCredentials,
    condition_id: &str,
) -> anyhow::Result<()> {
    // Auth format officiel : {apiKey, secret, passphrase} — PAS HMAC L2.
    let sub = serde_json::json!({
        "auth": {
            "apiKey": creds.api_key,
            "secret": creds.api_secret,
            "passphrase": creds.passphrase,
        },
        "markets": [condition_id],
        "type": "user",
    });
    tracing::info!(condition_id, "pm_user_ws: souscription user envoyée");
    sink.send(Message::Text(sub.to_string())).await
        .map_err(|e| anyhow::anyhow!("pm_user_ws send: {e}"))
}

fn process_message(txt: &str, fill_tx: &mpsc::UnboundedSender<FillEvent>) {
    let events = parse_events::<UserEvent>(txt);
    for ev in events {
        match ev.event_type.as_deref() {
            Some("trade") => {
                if let (Some(size_s), Some(price_s)) = (ev.size.as_ref(), ev.price.as_ref()) {
                    let filled_size: f64 = size_s.parse().unwrap_or(0.0);
                    let avg_price: f64 = price_s.parse().unwrap_or(0.0);
                    if filled_size > 0.0 && avg_price > 0.0 {
                        let taker_side_is_sell = ev.side.as_deref() == Some("SELL");
                        // Log BRUT : on garde les deux id + le side tel quel pour vérifier sur un vrai
                        // run où Polymarket place NOTRE id (taker vs maker) selon qu'on cross ou qu'on
                        // se fait remplir en resting.
                        tracing::info!(taker_order_id = ?ev.taker_order_id, maker_order_id = ?ev.maker_order_id,
                            filled_size, avg_price, side = ?ev.side, "pm_user_ws: trade (brut)");
                        let _ = fill_tx.send(FillEvent {
                            taker_order_id: ev.taker_order_id.clone(),
                            maker_order_id: ev.maker_order_id.clone(),
                            filled_size, avg_price, taker_side_is_sell,
                        });
                    }
                }
            }
            Some("order") => {
                // UPDATE ordre : size_matched disponible, utile pour réconciliation partielle.
                if let (Some(id), Some(matched)) = (&ev.id, &ev.size_matched) {
                    let n: f64 = matched.parse().unwrap_or(0.0);
                    if n > 0.0 {
                        tracing::debug!(order_id = %id, size_matched = n, "pm_user_ws: order update");
                    }
                }
            }
            _ => {}
        }
    }
}

#[derive(Deserialize)]
struct UserEvent {
    #[serde(rename = "type")]
    event_type: Option<String>,
    id: Option<String>,
    taker_order_id: Option<String>,
    maker_order_id: Option<String>,
    size: Option<String>,
    size_matched: Option<String>,
    price: Option<String>,
    side: Option<String>,    // "BUY" / "SELL"
    #[allow(dead_code)]
    status: Option<String>,  // MATCHED / CONFIRMED
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_chan() -> (mpsc::UnboundedSender<FillEvent>, mpsc::UnboundedReceiver<FillEvent>) {
        mpsc::unbounded_channel::<FillEvent>()
    }

    #[test]
    fn trade_event_carries_both_ids() {
        let (tx, mut rx) = make_chan();
        let txt = serde_json::json!([{
            "type": "trade",
            "taker_order_id": "order-abc",
            "maker_order_id": "order-xyz",
            "size": "10.5",
            "price": "0.52",
            "side": "BUY",
            "status": "MATCHED"
        }]).to_string();
        process_message(&txt, &tx);
        let fill = rx.try_recv().expect("fill doit être publié");
        assert!(fill.involves("order-abc"), "doit matcher le taker id");
        assert!(fill.involves("order-xyz"), "doit matcher le maker id");
        assert!(!fill.involves("autre"));
        assert!((fill.filled_size - 10.5).abs() < 1e-9);
        assert!((fill.avg_price - 0.52).abs() < 1e-9);
    }

    #[test]
    fn maker_fill_is_matchable_by_our_maker_id() {
        // CŒUR DU FIX : notre ordre resting (maker) se fait remplir → notre id est dans
        // `maker_order_id` et le side de l'event est SELL (côté taker). On DOIT pouvoir matcher
        // notre id malgré tout — sinon on ignore notre propre achat (orpheline).
        let (tx, mut rx) = make_chan();
        let txt = serde_json::json!([{
            "type": "trade",
            "taker_order_id": "contrepartie",
            "maker_order_id": "le-notre",
            "size": "5.0",
            "price": "0.16",
            "side": "SELL",
        }]).to_string();
        process_message(&txt, &tx);
        let fill = rx.try_recv().unwrap();
        assert!(fill.involves("le-notre"), "notre id (maker) doit matcher");
        assert!(fill.taker_side_is_sell, "side brut = SELL (le taker a vendu)");
    }

    #[test]
    fn order_update_does_not_produce_fill_event() {
        let (tx, mut rx) = make_chan();
        let txt = serde_json::json!([{
            "type": "order",
            "id": "order-abc",
            "size_matched": "3.0",
            "price": "0.50"
        }]).to_string();
        process_message(&txt, &tx);
        assert!(rx.try_recv().is_err(), "order UPDATE ne doit pas publier de FillEvent");
    }

    #[test]
    fn trade_with_zero_size_ignored() {
        let (tx, mut rx) = make_chan();
        let txt = serde_json::json!([{
            "type": "trade",
            "taker_order_id": "order-abc",
            "size": "0",
            "price": "0.50",
        }]).to_string();
        process_message(&txt, &tx);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn two_fills_same_batch_both_delivered() {
        // ANTI-ORPHELINE : deux trades dans le même message (même tick) doivent TOUS deux être
        // livrés. Un canal `watch` en aurait coalescé un → reliquat orphelin. mpsc les garde.
        let (tx, mut rx) = make_chan();
        let txt = serde_json::json!([
            { "type": "trade", "taker_order_id": "g1", "size": "6.0", "price": "0.50", "side": "BUY" },
            { "type": "trade", "taker_order_id": "g1", "size": "4.0", "price": "0.52", "side": "BUY" }
        ]).to_string();
        process_message(&txt, &tx);
        let f1 = rx.try_recv().expect("1er fill");
        let f2 = rx.try_recv().expect("2e fill ne doit PAS être perdu");
        assert!((f1.filled_size - 6.0).abs() < 1e-9);
        assert!((f2.filled_size - 4.0).abs() < 1e-9);
        assert!(rx.try_recv().is_err(), "exactement 2 fills");
    }
}
