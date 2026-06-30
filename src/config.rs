//! Configuration du sniper, chargée depuis l'environnement (`.env`).

use std::env;

fn env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    env::var(key).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

#[derive(Debug, Clone)]
pub struct Config {
    pub dry_run: bool,
    pub dashboard_port: u16,

    // Flux marché
    pub binance_ws_url: String,
    pub okx_ws_url: String,

    // OBI (P2/P3)
    pub obi_band_pct: f64,           // bande % autour du mid (0.0005 = 0.05 %)
    pub obi_top_n: usize,            // top-N niveaux BBO (0 = mode bande)
    pub obi_fire_threshold: f64,     // seuil de la magnitude consolidée
    pub obi_floor_per_exchange: f64, // chaque exchange doit dépasser ce floor
    pub weight_binance: f64,         // 0.65
    pub weight_okx: f64,             // 0.35

    // FSM sniper (P4)
    pub obi_dwell_ms: u64,           // persistance avant tir
    pub cooldown_ms: u64,
    pub gap_min: f64,                // |fair − real| minimal (plancher du gap requis)
    pub gap_dynamic_k: f64,          // GAP_DYNAMIC_K : gap requis += k·|real_up−0.5|·(temps_écoulé/fenêtre)
                                     // → exige un edge d'autant plus grand que le binaire est décidé ET tardif.
    pub velocity_confirm: f64,       // |ΔP_1s| minimal (0 = désactivé)
    pub taker_fee_coeff: f64,        // TAKER_FEE_COEFF : coeff de la fee taker Polymarket (crypto = 0.07).
                                     // fee/share = coeff·p·(1−p). En stratégie entrée maker (GTC, 0 fee)
                                     // / sortie FAK (taker), on ne la paie qu'à la SORTIE → le gap requis
                                     // intègre 1·coeff·p·(1−p) (0 = désactivé).

    // Garde-fou binaire (anti deep-ITM/OTM)
    pub price_min: f64,              // PRICE_MIN : real_up min pour trader (sinon binaire trop décidé)
    pub price_max: f64,              // PRICE_MAX : real_up max pour trader
    // Volatilité (fair_up B&S, côté radar)
    pub vol_window_ms: u64,          // VOL_WINDOW_MS : fenêtre de calcul σ (longue = robuste aux impulsions)
    pub vol_sigma_cap: f64,          // VOL_SIGMA_CAP : plafond de σ annualisé (anti-pic sur impulsion)

    // Défensif (P4)
    pub vacuum_velocity: f64,        // ΔP_1s ≤ seuil → vide de liquidité
    pub vacuum_obi: f64,
    pub end_window_block_secs: i64,

    // Bankroll / Kelly (P5)
    pub start_cash: f64,
    pub kelly_fraction: f64,
    pub max_kelly_size_pct: f64,     // plafond taille / equity
    pub take_profit_cents: f64,
    pub stop_loss_cents: f64,
    pub max_hold_secs: i64,

    // Live testing (passage paper → réel)
    pub exec_mode: String,     // EXEC_MODE : "taker" (FAK, chemin actuel) | "maker" (GTC resting).
                               // Défaut "taker" → le live actuel est préservé, le maker est opt-in.
    pub maker_price_k_spread: f64, // MAKER_PRICE_K_SPREAD : prix maker = mid − k·spread. 0.5 = best bid
                                   // (ancien comportement), 0.25 = entre mid et bid (plus de fill), 0 = mid.
    pub maker_price_eps_ticks: f64, // MAKER_PRICE_EPS_TICKS : plafonner le prix maker à ask − eps·tick
                                    // (reste STRICTEMENT maker, jamais de cross accidentel).
    pub grace_frac_of_remaining: f64, // GRACE_FRAC_OF_REMAINING : timeout BUY GTC = frac·temps_restant,
                                      // borné [buy_grace_floor_ms, buy_timeout_ms].
    pub buy_grace_floor_ms: u64,  // BUY_GRACE_FLOOR_MS : plancher du timeout adaptatif (≥ latence POST).
    pub buy_timeout_ms: u64,    // BUY_TIMEOUT_MS : plafond du timeout BUY GTC (grâce pleine tôt en fenêtre).
    pub sell_timeout_ms: u64,   // SELL_TIMEOUT_MS : PendingSell sans confirmation WS après ce délai →
                                // on REPASSE Open pour re-tenter la vente (anti-position-coincée).
    pub cancel_grace_ms: u64,   // CANCEL_GRACE_MS : après une annulation, fenêtre où un fill GAGNE encore
                                // (→ Open) avant de déclarer Idle. Anti-orpheline (course annulation/fill).
    pub max_drawdown: f64,     // circuit breaker sur l'equity (en $)
    pub live_armed: bool,      // LIVE_ARMED : verrou matériel pour l'envoi RÉEL d'ordres
    pub live_force_min_size: bool, // LIVE_FORCE_MIN_SIZE : ignore Kelly, force la taille minimale
                                   // (agressif — micro-test plomberie sur petite bankroll)
    pub fixed_order_usd: f64,      // FIXED_ORDER_USD > 0 : ignore Kelly, force un notionnel fixe ($)
                                   // à chaque tir (plancher = minimum d'échange). Tests/comparaison.
    pub maker_mode: bool,          // MAKER_MODE (paper) : simule une entrée maker (fill au bid) au lieu
                                   // de taker (VWAP des asks) → capte le spread. Optimiste (fill garanti).
    pub exit_buffer: f64,          // EXIT_BUFFER : marge sous le bid pour les sorties (garantit le
                                   // fill de la vente ; la FAK price-improve).
    pub entry_buffer: f64,         // ENTRY_BUFFER : marge AU-DESSUS de l'ask pour l'achat (garantit
                                   // le fill du BUY malgré le mouvement du prix pendant le round-trip).
    pub min_hold_sl_ms: u64,       // MIN_HOLD_SL_MS : délai avant que le SL puisse se déclencher
                                   // (évite le SL instantané sur le spread d'entrée). TP non concerné.

    // Infrastructure live (Bloc D)
    pub pm_ws_stale_threshold_ms: u64, // skip REST book si WS < ce seuil (ms)
    pub bankroll_poll_secs: u64,       // fréquence refresh bankroll CLOB
    pub order_engine_queue: usize,     // capacité mpsc OrderEngine
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            dry_run: env_or("DRY_RUN", true),
            dashboard_port: env_or("PORT", 8768),

            binance_ws_url: env::var("BINANCE_WS_URL")
                .unwrap_or_else(|_| "wss://stream.binance.com:9443/ws/btcusdt@depth".into()),
            okx_ws_url: env::var("OKX_WS_URL")
                .unwrap_or_else(|_| "wss://ws.okx.com:8443/ws/v5/public".into()),

            obi_band_pct: env_or("OBI_BAND_PCT", 0.0005),
            obi_top_n: env_or("OBI_TOP_N", 10usize),
            obi_fire_threshold: env_or("OBI_FIRE_THRESHOLD", 0.20),
            obi_floor_per_exchange: env_or("OBI_FLOOR_PER_EXCHANGE", 0.20),
            weight_binance: env_or("WEIGHT_BINANCE", 0.65),
            weight_okx: env_or("WEIGHT_OKX", 0.35),

            obi_dwell_ms: env_or("OBI_DWELL_MS", 0),
            cooldown_ms: env_or("COOLDOWN_MS", 3000),
            gap_min: env_or("GAP_MIN", 0.02),
            gap_dynamic_k: env_or("GAP_DYNAMIC_K", 0.5),
            velocity_confirm: env_or("VELOCITY_CONFIRM", 0.0),
            taker_fee_coeff: env_or("TAKER_FEE_COEFF", 0.07),

            price_min: env_or("PRICE_MIN", 0.02),
            price_max: env_or("PRICE_MAX", 0.98),
            vol_window_ms: env_or("VOL_WINDOW_MS", 30000u64),
            vol_sigma_cap: env_or("VOL_SIGMA_CAP", 2.0),

            vacuum_velocity: env_or("VACUUM_VELOCITY", -0.0010),
            vacuum_obi: env_or("VACUUM_OBI", -0.40),
            // On peut trader en fin de fenêtre (le gap dynamique exige l'edge) — petit garde minimal.
            end_window_block_secs: env_or("END_WINDOW_BLOCK_SECS", 5),

            start_cash: env_or("START_CASH", 200.0),
            kelly_fraction: env_or("KELLY_FRACTION", 0.5),
            max_kelly_size_pct: env_or("MAX_KELLY_SIZE_PCT", 0.02),
            take_profit_cents: env_or("TAKE_PROFIT_CENTS", 4.0),
            stop_loss_cents: env_or("STOP_LOSS_CENTS", 3.0),
            max_hold_secs: env_or("MAX_HOLD_SECS", 60),

            exec_mode: env::var("EXEC_MODE").unwrap_or_else(|_| "taker".into()),
            maker_price_k_spread: env_or("MAKER_PRICE_K_SPREAD", 0.25),
            maker_price_eps_ticks: env_or("MAKER_PRICE_EPS_TICKS", 1.0),
            grace_frac_of_remaining: env_or("GRACE_FRAC_OF_REMAINING", 0.15),
            buy_grace_floor_ms: env_or("BUY_GRACE_FLOOR_MS", 1500u64),
            buy_timeout_ms: env_or("BUY_TIMEOUT_MS", 8000u64),
            sell_timeout_ms: env_or("SELL_TIMEOUT_MS", 3000u64),
            cancel_grace_ms: env_or("CANCEL_GRACE_MS", 3000u64),
            max_drawdown: env_or("MAX_DRAWDOWN", 20.0),
            live_armed: env_or("LIVE_ARMED", false),
            live_force_min_size: env_or("LIVE_FORCE_MIN_SIZE", false),
            fixed_order_usd: env_or("FIXED_ORDER_USD", 0.0),
            maker_mode: env_or("MAKER_MODE", false),
            exit_buffer: env_or("EXIT_BUFFER", 0.02),
            entry_buffer: env_or("ENTRY_BUFFER", 0.02),
            min_hold_sl_ms: env_or("MIN_HOLD_SL_MS", 500u64),

            pm_ws_stale_threshold_ms: env_or("PM_WS_STALE_THRESHOLD_MS", 2000u64),
            bankroll_poll_secs: env_or("BANKROLL_POLL_SECS", 10u64),
            order_engine_queue: env_or("ORDER_ENGINE_QUEUE", 8usize),
        }
    }
}
