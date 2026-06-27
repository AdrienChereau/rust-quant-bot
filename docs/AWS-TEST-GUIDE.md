# Guide de test AWS — POLY_1271 (sig_type=3)

Guide pas-à-pas pour valider bankroll, auth CLOB et ordres sur le **serveur exécuteur (Dublin)**.
**Aucun Python requis** — tout passe par le binaire Rust.

---

## Architecture rappel

| Nœud | Rôle | Tests concernés |
|------|------|-----------------|
| **Dublin** (executor) | Polymarket + bankroll live | Toutes les étapes ci-dessous |
| **Tokyo** (radar) | Signaux UDP | Optionnel pour test end-to-end sniper |

Ce guide se concentre sur **Dublin** (là où vivent les `POLY_*` et le polling bankroll).

---

## Prérequis

### Sur ta machine locale (une seule fois)

- `.env` ou `.env.local` avec au minimum :
  - `POLY_PRIVATE_KEY`
  - `POLY_FUNDER_ADDRESS` (deposit wallet)
  - `POLY_SIGNER_ADDRESS` (MetaMask EOA owner)
  - `POLY_SIG_TYPE=3`
- Rust ≥ 1.91 (`rustup update stable`)

### Sur AWS Dublin

- Repo cloné dans `/home/ubuntu/rust-quant-bot` (ou équivalent)
- Fichier `.env` à la racine du repo (**jamais commité**)
- Port dashboard `8768` protégé (localhost / Tailscale uniquement) avant tout live

---

## Étape 0 — Préparer les credentials (local → AWS)

Les clés API L2 se dérivent **en local** (flow L1 avec ta clé privée). Ne jamais committer la private key sur le serveur via git — copie manuelle du `.env` uniquement.

```bash
# Sur ta machine locale, à la racine du repo
cd ~/rust-quant-bot
cargo build --release --features live

cargo run --release --features live -- poly derive-creds
```

Sortie attendue :

```
POLY_API_KEY=...
POLY_API_SECRET=...
POLY_PASSPHRASE=...
```

Copie ces 3 lignes dans le `.env` **sur AWS**, avec le reste :

```env
POLY_SIG_TYPE=3
POLY_FUNDER_ADDRESS=0x...    # deposit wallet (polymarket.com/settings)
POLY_SIGNER_ADDRESS=0x...    # MetaMask EOA (≠ deposit wallet)
POLY_PRIVATE_KEY=0x...       # clé MetaMask owner
POLY_API_KEY=...
POLY_API_SECRET=...
POLY_PASSPHRASE=...

LIVE_ARMED=false
RUST_LOG=info
MAX_DRAWDOWN=20
START_CASH=200
```

Vérifie les permissions :

```bash
chmod 600 ~/rust-quant-bot/.env
```

---

## Étape 1 — Build sur AWS

```bash
ssh ubuntu@<IP_DUBLIN>
cd ~/rust-quant-bot
git pull   # récupérer le code avec POLY_1271

source "$HOME/.cargo/env"
rustup update stable

# Build release avec signing + SDK Polymarket
RUSTFLAGS="-C target-cpu=native" cargo build --release --features live
```

Vérifier que le binaire existe :

```bash
ls -lh target/release/rust-quant-bot
```

---

## Étape 2 — Preflight auth + balance (`poly verify`)

**Test le plus important** — même auth L2 que le bot, sans lancer le sniper.

```bash
cd ~/rust-quant-bot
cargo run --release --features live -- poly verify
```

| Résultat | Signification |
|----------|---------------|
| `OK — balance CLOB : 18.44 USDC` | Auth OK, bankroll lisible |
| `401 Unauthorized/Invalid api key` | Voir § Dépannage |
| `credentials POLY_* incomplètes` | `.env` manquant ou mal rempli |

Ce que fait `poly verify` en interne :
1. `log_config_check()` — WARN si EOA ≠ clé privée
2. `sync_balance_allowance` si `sig_type=3` (cache CLOB deposit wallet)
3. `GET /balance-allowance` (HMAC L2)

---

## Étape 3 — Mettre à jour systemd

Le service par défaut compile sans `--features live`. Après rebuild live, redémarre l'exécuteur.

```bash
# Éditer si besoin (User, chemins)
sudo nano /etc/systemd/system/rust-quant-bot-executor.service
sudo systemctl daemon-reload
sudo systemctl restart rust-quant-bot-executor
sudo systemctl status rust-quant-bot-executor
```

**Important** : le unit file embarque `Environment=DRY_RUN=true`. Les variables systemd **écrasent** le `.env` au démarrage. Pour le paper dashboard, c'est OK. Pour le live, tu devras retirer ou adapter cette ligne dans le service.

Logs :

```bash
tail -f ~/rust-quant-bot/data/executor.log
```

Succès au boot :

```
credentials POLY chargées  sig_type=3  funder=0x...  signer=0x...
cache balance-allowance CLOB synchronisé   # sig_type=3 uniquement
```

Puis toutes les ~30 s :

```
💰 bankroll réelle CLOB  usdc=18.44
```

Échec typique :

```
lecture bankroll CLOB échouée  error=CLOB /balance-allowance 401 Unauthorized: ...
```

Filtre pratique :

```bash
tail -f ~/rust-quant-bot/data/executor.log | grep -iE "bankroll|balance|credentials|401|POLY"
```

---

## Étape 4 — Dry-run ordre (`poly dry-order`)

Teste la signature sans envoyer (tant que `LIVE_ARMED=false`).

Récupère un `token_id` liquide (marché BTC 5 min actif) :

```bash
# Option A — depuis les logs bot après quelques secondes
grep "nouveau marché" ~/rust-quant-bot/data/executor.log | tail -1

# Option B — API Gamma (slug fenêtre courante)
curl -s "https://gamma-api.polymarket.com/events/slug/btc-updown-5m-$(($(date +%s)/300*300))" \
  | jq -r '.markets[0].clobTokenIds' 
```

Puis :

```bash
cd ~/rust-quant-bot
cargo run --release --features live -- poly dry-order \
  --token-id <TOKEN_ID_UP_OU_DOWN> \
  --price 0.01 \
  --size 1
```

| sig_type | Log attendu |
|----------|-------------|
| **3** | `LIVE order POLY_1271 signé` + `signature_len` ~317 bytes |
| 0/2 | `LIVE order signé` + JSON avec `"orderType":"FAK"` |

Sortie CLI : `Dry-run OK — ordre signé, non POSTé`

---

## Étape 5 — Dry-run via dashboard (bot running)

1. Ouvre le dashboard (via SSH tunnel ou Tailscale) : `http://127.0.0.1:8768`
2. Vérifie **live bankroll** ≈ ton solde Polymarket
3. Active **Live ON** + ▶ **Live** (mode runtime, pas `LIVE_ARMED`)
4. Attends un signal sniper ou force les conditions
5. Dans les logs : ordre signé, **aucun** `✅ ordre LIVE accepté`

```bash
tail -f ~/rust-quant-bot/data/executor.log | grep -iE "LIVE order|DryRun|accepté"
```

---

## Étape 6 — Micro-trade réel (optionnel, dernière étape)

> Ne faire qu'**un seul** ordre minimal, après étapes 2–5 OK.

1. Protège le dashboard (`8768` non exposé publiquement)
2. Édite `.env` :

```env
LIVE_ARMED=true
```

3. Redémarre :

```bash
sudo systemctl restart rust-quant-bot-executor
```

4. Confirme dans les logs :

```
⚠️  LIVE_ARMED=true — envoi réel possible
✅ ordre LIVE POLY_1271 accepté  order_id=...
```

5. **Remets immédiatement** `LIVE_ARMED=false` après validation.

---

## Checklist complète

| # | Action | Commande / critère | OK |
|---|--------|-------------------|-----|
| 1 | Creds dérivés en local | `poly derive-creds` | ☐ |
| 2 | `.env` AWS complet (7× `POLY_*`) | `chmod 600 .env` | ☐ |
| 3 | Build live | `cargo build --release --features live` | ☐ |
| 4 | Preflight balance | `poly verify` → `OK — balance CLOB` | ☐ |
| 5 | Boot executor | `credentials POLY chargées` sans WARN 401 | ☐ |
| 6 | Poll bankroll | `💰 bankroll réelle CLOB` toutes les 30 s | ☐ |
| 7 | Dry-run CLI | `poly dry-order` → `Dry-run OK` | ☐ |
| 8 | Dry-run dashboard | Live ON, pas de POST réel | ☐ |
| 9 | Micro-trade | `LIVE_ARMED=true`, 1 ordre, puis repasser à `false` | ☐ |

---

## Dépannage

| Symptôme | Cause probable | Action |
|----------|----------------|--------|
| `401 Unauthorized/Invalid api key` | HMAC ou mauvais EOA | `POLY_SIGNER_ADDRESS` = EOA lié à l'API key ; re-run `poly derive-creds` en local |
| `401` + signer WARN au boot | `POLY_SIGNER_ADDRESS` ≠ clé privée | Aligner adresse MetaMask et `POLY_PRIVATE_KEY` |
| Balance 0 alors que UI montre des fonds | Cache CLOB pas sync | `poly verify` (sync auto sig_type=3) ; vérifier `POLY_FUNDER_ADDRESS` = deposit wallet |
| `sig_type=3 requiert --features live` | Binaire sans feature | Rebuild `--features live` |
| `INVALID_SIGNATURE` au POST | Proxy deposit wallet non déployé | 1 trade minimal via UI Polymarket (déploie le contrat on-chain) |
| `signer address has to be the address of the API KEY` | Issue SDK / proxy non déployé | Idem + vérifier deposit wallet dans settings |
| Bankroll OK mais tir ignoré | Poll pas encore passé | Attendre 30 s ou vérifier log `bankroll réelle pas encore lue` |
| Dashboard live ne répond pas | Port fermé / mauvais bind | SSH tunnel : `ssh -L 8768:127.0.0.1:8768 ubuntu@<IP>` |

### Commandes diagnostic rapide

```bash
# Variables chargées (sans afficher les secrets)
grep -E '^POLY_(SIG_TYPE|FUNDER|SIGNER)=' ~/rust-quant-bot/.env

# Dernières erreurs auth
grep -iE "401|Unauthorized|bankroll.*échou" ~/rust-quant-bot/data/executor.log | tail -20

# Neg-risk marché sniper (info)
grep "neg_risk" ~/rust-quant-bot/data/executor.log | tail -5
```

---

## Commandes utiles (référence)

```bash
# Preflight complet
cargo run --release --features live -- poly verify

# Dry-run ordre
cargo run --release --features live -- poly dry-order --token-id <ID> --price 0.01 --size 1

# Rebuild + restart
RUSTFLAGS="-C target-cpu=native" cargo build --release --features live \
  && sudo systemctl restart rust-quant-bot-executor

# Logs bankroll en direct
tail -f ~/rust-quant-bot/data/executor.log | grep -iE "bankroll|balance|LIVE|401"
```

---

## Voir aussi

- [`.env.example`](../.env.example) — schéma variables
- [`docs/METAMASK-SETUP.md`](METAMASK-SETUP.md) — setup compte + sig_type
- [`deploy/README-hft-deploy.md`](../deploy/README-hft-deploy.md) — infra multi-nœuds
