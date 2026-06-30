# rust-quant-bot — point d'entrée (où on en est)

Sniper front-running **Binance+OKX → Polymarket** (fenêtres BTC 5 min). Le radar (Tokyo) calcule un
signal OBI consolidé et tire en UDP vers les nœuds d'exécution. **Architecture à 3 nœuds isolés**
depuis la v3 (split paper/live).

> Langue : répondre en **français**. Mot d'ordre du projet : **la rapidité du chemin live**.

## Topologie (v3.0.0-split)

| Nœud | Rôle CLI | Build | Dashboard | Fichiers d'état | Machine |
|------|----------|-------|-----------|-----------------|---------|
| **Radar** | `radar` | défaut | `:8768` | — | Tokyo |
| **Live**  | `live`  | `--features live` | `:8769` | `data/live_state.json` + `.jsonl` | Dublin (eu-west-1) |
| **Paper** | `paper` | défaut | `:8768` | `data/sniper_state.json` + `.jsonl` | **machine séparée** |

- Le radar **tire aux deux, LIVE d'abord** (UDP), paper ensuite (`TARGET_PAPER_IP` optionnel).
- Le nœud **live ne contient AUCUN code paper** et inversement → le paper ne peut jamais voler le
  CPU/les locks du live (process + machines séparés). C'est le cœur du refactor v3.
- `mono` (radar+exécuteur in-process) reste pour le dev local. `executor` = alias legacy de `live`.

## Build / run

```bash
cargo build --release                 # paper + radar (n'importe quel rustc)
cargo build --release --features live  # live (rustc >= 1.91 — AWS only, pas en local 1.86)
cargo test                             # 59 tests
# local 3-process : radar TARGET_LIVE=127.0.0.1:8080 TARGET_PAPER=127.0.0.1:8081
#                   live --listen-port 8080 (PORT=8769) ; paper --listen-port 8081 (PORT=8768)
```

## Start/Stop = pause logicielle

Bouton dashboard → `POST /start` | `/stop` → bascule un AtomicBool (`live_paused` ou `paper_paused`).
Le process et les WebSockets restent chauds. `LIVE_ARMED=true` (env) reste requis pour l'envoi réel.

## ⚠️ Pièges live maker — câblage Polymarket (NE PAS reproduire)

Diagnostiqués en prod (paper gagnait, live « rentrait mais ne sortait pas »). Coûteux à re-trouver.

1. **VENDRE exige de rafraîchir le cache allowance `CONDITIONAL`** (≠ `COLLATERAL`). La doc impose
   d'approuver/rafraîchir DEUX assets : USDC (acheter) **et** les conditional tokens (vendre). Après
   un BUY, le CLOB voit **0** token outcome → tout SELL rejeté `not enough balance`. On rafraîchit
   `sync_conditional_allowance(token_id)` (asset_type=CONDITIONAL) **dès l'adoption** d'une position,
   hors hot-loop (`roles/live.rs`). Sans ça : on détient mais on ne peut pas revendre.
2. **Un `balance: 0` au SELL n'est PAS une preuve de position perdue** — souvent juste le cache
   CONDITIONAL périmé, ou le BUY pas encore réglé (`MATCHED→MINED→CONFIRMED`). Ne JAMAIS abandonner
   d'emblée : refresh + ré-essai, abandon seulement après settlement ET plusieurs échecs
   (`on_sell_result` renvoie un `bool` « needs refresh »).
3. **Le poll d'achat ne doit JAMAIS recaler `size` après une vente.** La position porte `bought`
   (cumul achat, monotone) et `sold` ; `size = bought − sold`. `reconcile_buy_to_server` se cale sur
   `bought`, sinon une vente partielle est « ressuscitée » au cumul d'achat (on re-détient le vendu).
4. **Taille de vente tronquée à 2 décimales vers le bas** (`decimal_from_f64(.., floor=true)` dans
   `poly1271.rs`) — jamais arrondie au-dessus, sinon `not enough balance`. Reliquat < dust = se règle
   à l'expiration.
5. **post-only = GTC/GTD UNIQUEMENT.** `postOnly=true` avec FAK/FOK → ordre rejeté. Entrée maker =
   GTC (post-only possible pour garantir le rebate), sortie taker = FAK (jamais post-only). Gérer
   les statuts `delayed`/`unmatched` et erreurs `ORDER_DELAYED`/`MARKET_NOT_READY` (marchés à délai).
6. **Fees Polymarket** : maker = **0** ; taker **crypto = 7 bps**, `fee = shares × 0.07 × p × (1−p)`
   (max ~1,75¢/share à p≈0,5, ~0,3¢ aux extrêmes). La **clôture taker (FAK)** paie cette fee → à
   intégrer dans l'edge requis / le TP (sinon un TP de 4¢ à mi-prix est mangé par la fee).
7. **`max_hold` vs fin de fenêtre** : un GTC qui remplit < 30 s avant le rollover déclenche la sortie
   forcée `remaining_s <= 30` (loguée `max_hold`) → entrée+sortie à la même ms à perte. `opened_ms`
   est bien l'heure du FILL (pas `created_at`) ; le vrai correctif = ne pas laisser un BUY resting
   filler dans la zone de mort (l'annuler avant), pas toucher au timer.

> Source de vérité des fills : poll serveur `GET /data/order` (achat) + réponse HTTP FAK (vente).
> Le user-WS est un filet, jamais l'unique source (il rate des events). Symétriser la vente via
> `GET /data/trades` reste un durcissement ouvert.

## Docs

- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — topologie, hot-path, locks, isolation.
- [docs/MATH.md](docs/MATH.md) — OBI, Black-Scholes, Kelly, TP/SL, breaker (avec refs fichier:ligne).
- [docs/RUNBOOK.md](docs/RUNBOOK.md) — déploiement (`deploy/*.sh`), env, NTP, rollback.
- [docs/TUTORIAL-AWS-DEBUTANT.md](docs/TUTORIAL-AWS-DEBUTANT.md) — pas-à-pas AWS+terminal pour débutant.
- [docs/CHANGELOG.md](docs/CHANGELOG.md) — historique aligné sur les tags git.

## Rollback

Tags git : `v2.0.0-mono-toggle` (avant split, paper+live couplés) · `v3.0.0-split` (après).
`deploy/rollback.sh <live|paper|radar> <tag>`.
