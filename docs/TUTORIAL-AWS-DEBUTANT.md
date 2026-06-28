# Tutoriel AWS + terminal pour débutant — déployer le bot de A à Z

Ce guide part de zéro. Objectif : faire tourner les **3 nœuds** du bot sur AWS.
Suis les parties dans l'ordre. Chaque commande est expliquée. Prends ton temps.

> Convention : tout ce qui est dans un bloc `comme ça` est une commande à **copier-coller**.
> Quand tu vois `XXX_A_REMPLACER`, remplace par ta vraie valeur (sans les `<>`).

---

## Partie 0 — C'est quoi qu'on construit ?

3 ordinateurs Linux dans le cloud (« instances EC2 ») :

| Nom | Région AWS | Rôle | Pourquoi là |
|-----|-----------|------|-------------|
| **radar** | Tokyo (`ap-northeast-1`) | écoute Binance/OKX, envoie les signaux | proche des exchanges |
| **live** | Dublin (`eu-west-1`) | passe les **vrais ordres** sur Polymarket | proche de Polymarket = rapide |
| **paper** | Tokyo (même box que le radar) | **simulation**, faux argent | doit être séparé de Dublin |

Donc tu n'as besoin que de **2 instances** : une à Tokyo (radar **+** paper cohabitent), une à
Dublin (live tout seul, pour la vitesse).

**Schéma :**
```
   Tokyo : [radar]  --signal-->  Dublin : [live]   --> vrais ordres Polymarket
            [paper] <--signal--/         (rapide, isolé)
       (simulation, même machine que le radar)
```

---

## Partie 1 — Le terminal de ton Mac (les bases)

Ouvre l'app **Terminal** (Cmd+Espace, tape « Terminal », Entrée). C'est une fenêtre où tu tapes
des commandes. Les 5 commandes que tu utiliseras :

| Commande | Ce que ça fait |
|----------|----------------|
| `pwd` | affiche le dossier où tu es |
| `ls` | liste les fichiers du dossier |
| `cd nom_du_dossier` | entre dans un dossier (`cd ..` = remonter) |
| `nano fichier` | éditeur de texte simple (Ctrl+O = sauver, Ctrl+X = quitter) |
| `ssh` | se connecter à une machine distante (on verra) |

**Va dans le dossier du bot** (à faire à chaque fois avant les commandes `deploy/`) :
```bash
cd ~/Documents/ia/Poly/rust-quant-bot
```
Vérifie que tu es au bon endroit :
```bash
ls
```
Tu dois voir `Cargo.toml`, `src`, `deploy`, `docs`… Si oui, tu es prêt.

---

## Partie 2 — Créer les instances AWS

### 2.1 Compte et console
1. Va sur https://aws.amazon.com → **Créer un compte** (carte bancaire requise, facturation à l'usage).
2. Une fois connecté, tu arrives sur la **Console AWS**. En haut à droite il y a un **sélecteur de
   région** (ex. « Ireland » / « Tokyo »). **La région est cruciale** : une instance créée à Tokyo
   n'apparaît pas quand tu es en région Dublin.

### 2.2 Créer la paire de clés (ton « mot de passe » SSH)
Tu te connecteras aux machines avec un fichier `.pem` (pas un mot de passe).
1. Console → cherche **EC2** → menu gauche **Réseau et sécurité → Paires de clés → Créer**.
2. Nom : `poly-key`. Type : **RSA**, format **.pem**. Crée.
3. Le fichier `poly-key.pem` se télécharge. **Range-le et protège-le** :
```bash
mkdir -p ~/.ssh
mv ~/Downloads/poly-key.pem ~/.ssh/
chmod 400 ~/.ssh/poly-key.pem
```
`chmod 400` = « lisible par moi seul » (SSH refuse une clé trop ouverte).
> ⚠️ Refais l'étape 2.1-2.2 **dans chaque région** OU coche la même clé : en pratique une paire de
> clés est **par région**. Crée donc `poly-key` à Tokyo **et** à Dublin (même nom, c'est plus simple).

### 2.3 Lancer l'instance **Dublin (live)**
1. Passe la région en haut à droite sur **Europe (Ireland) eu-west-1**.
2. EC2 → **Instances → Lancer une instance**.
3. **Nom** : `live-dublin`.
4. **Image (AMI)** : **Ubuntu Server 24.04 LTS** (gratuit éligible).
5. **Type d'instance** : `t3.small` (2 vCPU — nécessaire car le live épingle 2 cœurs). Suffisant pour
   démarrer ; tu pourras monter en gamme plus tard.
6. **Paire de clés** : choisis `poly-key`.
7. **Paramètres réseau → Modifier** : crée un **groupe de sécurité** `sg-live` (on règle les ports en
   Partie 3). Pour l'instant laisse SSH (22) ouvert depuis « Mon IP ».
8. **Stockage** : 20 Gio suffisent.
9. **Lancer l'instance**.

### 2.4 Lancer l'instance **Tokyo (radar + paper)**
Refais 2.3 mais :
- Région : **Asia Pacific (Tokyo) ap-northeast-1**.
- Nom : `radar-tokyo`.
- Groupe de sécurité : `sg-tokyo`.
- Type : `t3.small` convient (radar + paper sont légers).

### 2.5 Donner une IP fixe (Elastic IP) — important
Par défaut, l'IP publique change à chaque redémarrage. Or le radar doit connaître l'IP du live. On
fige donc les IP :
1. Région **Dublin** → EC2 → **Réseau et sécurité → IP Elastic → Allouer** → puis **Associer** à
   `live-dublin`. Note l'IP : ce sera ton `IP_LIVE`.
2. Région **Tokyo** → pareil → associer à `radar-tokyo`. Note `IP_TOKYO`.

---

## Partie 3 — Les ports (groupes de sécurité)

Un « groupe de sécurité » = un pare-feu. Par défaut **tout le sortant est autorisé** (le radar peut
donc joindre Binance/OKX, le live peut joindre Polymarket). On ne règle que **l'entrant**.

> Trouve ton IP perso (pour n'ouvrir les dashboards qu'à toi) : tape « mon ip » dans Google, ou
> `curl ifconfig.me` dans le terminal. Note-la : `MON_IP`.

### 3.1 Groupe `sg-live` (Dublin)
EC2 (région Dublin) → Groupes de sécurité → `sg-live` → **Modifier les règles entrantes** :
| Type | Protocole | Port | Source | Pourquoi |
|------|-----------|------|--------|----------|
| SSH | TCP | 22 | `MON_IP/32` | te connecter |
| Custom UDP | UDP | 8080 | `IP_TOKYO/32` | recevoir les signaux du radar |
| Custom TCP | TCP | 8769 | `MON_IP/32` | dashboard live (optionnel, on préfèrera un tunnel) |

### 3.2 Groupe `sg-tokyo` (radar + paper)
| Type | Protocole | Port | Source | Pourquoi |
|------|-----------|------|--------|----------|
| SSH | TCP | 22 | `MON_IP/32` | te connecter |
| Custom UDP | UDP | 8081 | `IP_TOKYO/32` | le paper reçoit les signaux (le radar tire en local) |
| Custom TCP | TCP | 8768 | `MON_IP/32` | dashboards radar/paper (optionnel) |

> Tu peux laisser les ports 8768/8769 **fermés** et utiliser un **tunnel SSH** (Partie 9, plus sûr).

---

## Partie 4 — Se connecter aux machines (SSH)

Format : `ssh -i CLÉ ubuntu@IP`. `ubuntu` est l'utilisateur par défaut des images Ubuntu.

**Live (Dublin)** :
```bash
ssh -i ~/.ssh/poly-key.pem ubuntu@IP_LIVE
```
La première fois, tape `yes` pour accepter l'empreinte. Tu es maintenant **dans** la machine Dublin
(le prompt change, genre `ubuntu@ip-172-…`). Pour ressortir : `exit`.

**Tokyo** (dans une autre fenêtre de terminal) :
```bash
ssh -i ~/.ssh/poly-key.pem ubuntu@IP_TOKYO
```

---

## Partie 5 — Préparer chaque machine (Rust + outils)

À faire **sur les 2 machines** (connecte-toi en SSH puis colle ce bloc). C'est l'étape la plus
longue (téléchargements), ~5–10 min par machine.

```bash
# Met à jour le système + outils de compilation (openssl requis par le bot)
sudo apt update && sudo apt install -y build-essential pkg-config libssl-dev curl git

# Installe Rust (toolchain récent — requis par le live)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"
rustc --version      # doit afficher 1.91 ou plus récent
```
Si `rustc --version` affiche < 1.91 : `rustup update stable`.

**Synchronise l'horloge** (indispensable pour la mesure de latence transport) — sur les 2 machines :
```bash
sudo timedatectl set-ntp true
timedatectl | grep -i ntp     # doit dire "active: yes"
```

Quand c'est fait sur une machine, tape `exit` et fais l'autre.

---

## Partie 6 — Les secrets Polymarket (sur Dublin uniquement)

Le live a besoin de tes clés Polymarket. Elles ne vont **que** sur Dublin et **jamais** dans git.

Connecte-toi à Dublin (`ssh -i ~/.ssh/poly-key.pem ubuntu@IP_LIVE`) puis crée le fichier `.env` :
```bash
mkdir -p ~/rust-quant-bot
nano ~/rust-quant-bot/.env
```
Colle (remplace par TES valeurs — cf. ta procédure `poly derive-creds`) :
```
POLY_PRIVATE_KEY=0x...
POLY_FUNDER_ADDRESS=0x...
POLY_SIGNER_ADDRESS=0x...
POLY_API_KEY=...
POLY_API_SECRET=...
POLY_PASSPHRASE=...
POLY_SIG_TYPE=3
LIVE_ARMED=false
```
`LIVE_ARMED=false` = sécurité : aucun ordre réel tant que tu n'as pas tout vérifié.
Sauve : **Ctrl+O**, Entrée, puis **Ctrl+X**. Protège le fichier :
```bash
chmod 600 ~/rust-quant-bot/.env
```
`exit` pour ressortir.

---

## Partie 7 — Configurer le déploiement (sur ton Mac)

Retour dans **ton terminal Mac** (pas en SSH) :
```bash
cd ~/Documents/ia/Poly/rust-quant-bot
cp deploy/hosts.env.example deploy/hosts.env
nano deploy/hosts.env
```
Renseigne (option A : paper sur la même box que le radar → même IP que le radar) :
```
LIVE_HOST=ubuntu@IP_LIVE
LIVE_DASH_PORT=8769

PAPER_HOST=ubuntu@IP_TOKYO
PAPER_DASH_PORT=8768

RADAR_HOST=ubuntu@IP_TOKYO
RADAR_DASH_PORT=8768

REMOTE_DIR=/home/ubuntu/rust-quant-bot
```
Sauve (Ctrl+O, Ctrl+X). Ce fichier est **gitignoré** (ne part jamais sur GitHub).

> Pour que `deploy.sh` se connecte sans taper `-i poly-key.pem` à chaque fois, dis à SSH quelle clé
> utiliser. Crée/édite `~/.ssh/config` :
> ```bash
> nano ~/.ssh/config
> ```
> et colle :
> ```
> Host IP_LIVE IP_TOKYO
>   User ubuntu
>   IdentityFile ~/.ssh/poly-key.pem
> ```
> (remplace par tes vraies IP). Sauve.

---

## Partie 8 — Déployer

Toujours sur ton Mac, dans le dossier du bot. **D'abord un essai à blanc** (ne modifie rien) :
```bash
deploy/deploy.sh live --dry-run
```
Ça liste les fichiers qui seraient envoyés. Si pas d'erreur, lance pour de vrai (l'ordre conseillé) :
```bash
deploy/deploy.sh radar    # envoie le code à Tokyo, compile, démarre le radar
deploy/deploy.sh paper    # démarre le paper (sur Tokyo aussi)
deploy/deploy.sh live     # compile avec --features live sur Dublin, démarre le live
```
Chaque commande : copie les sources, compile **sur la machine distante** (le premier build prend
quelques minutes), installe le service systemd, démarre. À la fin tu vois `status … active (running)`.

> systemd = le gestionnaire qui garde le bot allumé et le relance s'il plante (`Restart=on-failure`).

---

## Partie 9 — Voir les dashboards (tunnel SSH, sûr)

Plutôt qu'ouvrir les ports au public, on crée un **tunnel** : « branche le port distant sur mon Mac ».

**Dashboard live** — dans un terminal Mac :
```bash
ssh -i ~/.ssh/poly-key.pem -L 8769:localhost:8769 ubuntu@IP_LIVE
```
Laisse cette fenêtre ouverte, puis dans ton navigateur : **http://localhost:8769**

**Dashboard paper** — autre terminal :
```bash
ssh -i ~/.ssh/poly-key.pem -L 8768:localhost:8768 ubuntu@IP_TOKYO
```
Navigateur : **http://localhost:8768**

Tu dois voir le terminal LIVE (bankroll, PnL, latence totale) et le terminal PAPER (PnL paper).

---

## Partie 10 — Vérifier puis passer en réel

1. **Preflight** (vérifie tes clés + la balance, sans rien risquer), sur ton Mac :
```bash
deploy/preflight.sh
```
Doit afficher `OK — balance CLOB : … USDC`. Sinon, tes clés `.env` sont à corriger.

2. **Vérifie les signaux** : sur le dashboard live et paper, regarde que les données bougent. Côté
   radar, chaque tir loggue `🚀 signal UDP envoyé` **deux fois** (live puis paper) :
```bash
deploy/ctl.sh radar logs
```

3. **Armer le réel** (quand tu es sûr) — connecte-toi à Dublin et passe `LIVE_ARMED=true` :
```bash
ssh -i ~/.ssh/poly-key.pem ubuntu@IP_LIVE
nano ~/rust-quant-bot/.env      # mets LIVE_ARMED=true, sauve
exit
```
Puis sur ton Mac :
```bash
deploy/ctl.sh live restart
```

4. **Démarrer l'exécution** (le live démarre toujours **en pause** par sécurité) :
```bash
deploy/ctl.sh live run        # = bouton START (pause logicielle relâchée)
```
Pour mettre en pause sans tout couper :
```bash
deploy/ctl.sh live pause      # = bouton STOP (les connexions restent chaudes)
```

---

## Partie 11 — Au quotidien & dépannage

**Commandes utiles** (depuis ton Mac) :
```bash
deploy/ctl.sh live status     # le service tourne-t-il ?
deploy/ctl.sh live logs       # 80 dernières lignes de log
deploy/ctl.sh paper logs
deploy/ctl.sh live restart    # redémarrage dur (recharge l'état automatiquement)
```

**Mettre à jour le code après une modif** : refais simplement `deploy/deploy.sh <role>`.

**Revenir à une version précédente** (rollback) :
```bash
deploy/rollback.sh live v2.0.0-mono-toggle   # ancienne archi (paper+live couplés)
deploy/rollback.sh live v3.0.0-split         # archi actuelle
```

**Si ça ne marche pas :**
| Symptôme | Piste |
|----------|-------|
| `Permission denied (publickey)` au SSH | mauvaise clé/IP, ou `chmod 400` oublié sur le `.pem` |
| `deploy.sh` bloque à la compilation | Rust pas installé sur la machine (refais Partie 5) |
| dashboard vide / pas de signal | vérifie les ports UDP du groupe de sécurité (Partie 3) |
| preflight `401`/erreur balance | `.env` incomplet ou `POLY_SIG_TYPE` ≠ 3 |
| latence transport farfelue | NTP pas activé sur une machine (Partie 5) |

**Règle d'or** : ne supprime/n'écrase **jamais** les fichiers `data/*.json` pour « remettre à zéro » —
un simple `restart` recharge l'état. C'est ta mémoire de PnL/positions.

**Coûts** : 2× `t3.small` tournent ~24/24. Pense à **arrêter** (pas supprimer) les instances quand tu
ne t'en sers pas longtemps : EC2 → Instances → clic droit → « Arrêter l'instance ». (Une instance
arrêtée garde son disque mais perd son IP publique non-Elastic — d'où l'Elastic IP en Partie 2.5.)
```

---

## Récap ultra-court (une fois tout installé)

```bash
cd ~/Documents/ia/Poly/rust-quant-bot
deploy/deploy.sh radar && deploy/deploy.sh paper && deploy/deploy.sh live
deploy/preflight.sh
deploy/ctl.sh live run
# dashboards : ssh -L 8769:localhost:8769 ubuntu@IP_LIVE  → http://localhost:8769
```
