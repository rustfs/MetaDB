# Déploiement & Montée en Charge en Ligne (Français)

* **Retour à l'index**: [Présentation Français](./README.md) | [Portail](../../README.md)

Le binaire actuel ne lance un déploiement persistant qu'en `all-in-one`. Les tests d'intégration couvrent Raft multi-processus dans la même cellule. Les processus découpés par rôle et la topologie multi-cellules sont un objectif de conception, pas un déploiement que ce dépôt exécute. La fourchette 100 milliards – 10 000 milliards et plus est un modèle de conception, pas un résultat mesuré ici. Les chiffres de l'ordre de 10^12 enregistrements publiés dans `arXiv:2610.32247v1` sont des données de production Baidu.

> **État d'implémentation** : le LSM persistant, la réplication Raft, le découpage local des shards et les opérations gRPC/CLI all-in-one ont une couverture de régression. Seul `all-in-one` démarre un démon. `be`, `proxy`, `cell-manager`, `global-manager` et `RegisterCell` à distance sont rejetés. Voir les [notes de correction](../../designs/correctness_hardening.md).

---

## Étape 1 : Déploiement Mononœud Autonome (`metadbd --role all-in-one`) `[Implémenté]`

Tous les rôles (`GlobalManager`, `CellLocalManager`, `MetaCache`, `Proxy` et stockage `BE` en **OpenRaft `1-Voter`**) tournent dans un seul binaire. Le WAL local du moteur KV est désactivé : seul le journal segmenté `.raftlog` (protégé par CRC32C) assure la durabilité séquentielle.

```toml
[node]
node_id = 1
role = "all-in-one"
data_dir = "/var/lib/metadb"
grpc_listen_addr = "127.0.0.1:7800"
metrics_listen_addr = "127.0.0.1:9100"

[clock]
max_clock_drift_ms = 500

[storage]
memtable_size_bytes = 33554432          # 32 Mio
block_cache_bytes = 536870912           # 512 Mio (cache LRU à 16 fragments)
mvcc_retention_ms = 5000                # Fenêtre MVCC en mémoire de 5 s
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296 # 4 Gio
```

Rust 1.100.0 (`rust-toolchain.toml`). Seul le rôle `all-in-one` démarre. Ce processus ne termine pas TLS. Sans `[security].shared_secret`, il n'y a pas d'authentification, et seules les liaisons en boucle locale sont acceptées. Le défaut est `127.0.0.1`. Un joker ou une autre adresse avec un secret vide refuse de démarrer. Un `shared_secret` non vide exige `authorization: Bearer <secret>` pour les métadonnées, les transactions, le plan de contrôle, Raft sur le même port, et `GET /metrics`.

```bash
metadbd --config /etc/metadb/standalone.toml
```

---

## Étape 2 : Découpage Dynamique sur Frontière `PID` `[Implémenté]`

Lorsqu'un shard dépasse `shard_split_threshold_bytes`, `PolicyShardSplitter` choisit une frontière stricte `group_key = PID` (`[0x01][pid: 8B BE]`) afin que `(PID, "_attr_")` et tous ses enfants restent sur le même shard.

---

## Étape 3 : Topologie Multi-Cell Envisagée

L'étape 3 est un objectif de conception. Le binaire actuel ne la déploie pas.

`metadbd` n'exécute que `--role all-in-one`. `PersistentMetaDbCluster::open_and_serve` dans `crates/metadb-server/src/lib.rs` refuse `be`, `proxy`, `cell-manager` et `global-manager` avant d'écouter. `RegisterCell` dans `crates/metadb-server/src/grpc_services.rs` renvoie une erreur applicative et n'enregistre pas une seconde cellule. Cet arbre n'a pas de déploiement multi-processus et multi-cellules en service.

Un second `metadbd --role all-in-one` peut rejoindre la **même** cellule lorsque `--join` ou `--cell-manager` désigne le leader courant et que le répertoire de données est vierge. Ce chemin est couvert par `crates/metadb-server/tests/join_existing_group.rs`. Le processus qui rejoint reste all-in-one. Ce n'est pas un backend découpé par rôle, et la jonction ne crée pas une autre cellule. Voir [Exploitation](../../fr/operations.md).

### 3.1 Votants dans une seule cellule

Ne lancez pas `metadbd --role be`. Ce rôle se termine dans `open_and_serve` ; ces commandes ne sont pas un mode opératoire. `metadb-ctl raft scale-replicas` appelle `ScaleShardReplicas` et exige que le nombre de votants demandé soit déjà enregistré dans la cellule. Une cellule all-in-one neuve n'a qu'un nœud, donc `--target-voters 3` échoue tant que ces nœuds n'existent pas. Les contraintes sont dans [Exploitation](../../fr/operations.md).

### 3.2 Modèle de conception d'une topologie désagrégée (`100B – 10T+` enregistrements)

`100B – 10T+` (100 Md – 10 000 Md et plus) est une **plage de modèle de conception** : une arithmétique pour dimensionner une future topologie multi-cellules. Ce n'est pas un nombre d'enregistrements stockés par ce dépôt, ni un chiffre de production Baidu présenté comme un résultat de `rustfs/MetaDB`. Les tailles publiées dans `arXiv:2610.32247v1`, dont les 1 848 milliards d'enregistrements (`1,848 × 10^12`) du cluster BOS2, sont des données de production Baidu.

Le tableau est le découpage de rôles envisagé. Ce binaire n'exécute pas ces rôles.

| Rôle | `--role` de conception | Responsabilité envisagée |
| :--- | :--- | :--- |
| **Gestionnaire global** | `global-manager` | Associerait des intervalles grossiers `[start_key, end_key)` à un `CellId`, allouerait des blocs de `65 536` identifiants et coordonnerait les baux de renommage de répertoire entre cellules. |
| **Gestionnaire de cellule** | `cell-manager` | Traiterait les battements de cœur BE (`20K–50K` shards par cellule) et orchestrerait les découpages `PID`, l'isolation jumbo et les changements d'appartenance. |
| **Stockage backend (`BE`)** | `be` | Hébergerait les groupes Multi-Raft et le moteur LSM sur des hôtes NVMe. |
| **Proxy sans état** | `proxy` | Embarquerait `MetaCache`, compilerait les 20 modèles SQL et coordonnerait le 2PC inter-shards. |

`metadb-ctl cell register` appelle `RegisterCell` et se termine avec un statut non nul. Il n'enregistre pas de cellule. Ne l'utilisez pas comme étape de fédération.
