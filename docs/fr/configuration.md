# Configuration

[Présentation](./README.md) · [English](../en/configuration.md) · [Deutsch](../de/configuration.md) · [日本語](../ja/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` et `ServerTomlConfig::from_toml_file` / `from_toml_str` lisent le TOML (`crates/metadb-server/src/config.rs`). Chaque section connue est facultative. Les champs omis prennent les valeurs par défaut ci-dessous.

On current `main`, a key inside `[security]` other than `shared_secret` fails parsing. TLS-like keys (`tls`, `ssl`, `cert`) and secret-like keys (`secret`, `password`, `token`, `credential`, `auth`) fail anywhere, including nested tables. Unknown keys that do not match those names are still ignored. Unmerged pull request #35 also rejects `anonymous` and `insecure` anywhere and accepts `[security].allow_unauthenticated` beside `shared_secret`. The empty-secret rules later in this file describe #35, not current `main`. Until #35 merges, `main` still accepts an empty secret on loopback and does not implement `allow_unauthenticated`.

## Valeurs par défaut

| Clé | Défaut | Appliquée par `open_and_serve` |
| :--- | :--- | :--- |
| `node.node_id` | `1` | Oui. Identifiant de nœud Raft et pair d'amorçage |
| `node.cell_id` | `1` | Oui. Nom de répertoire `cell_<id>` et seule cellule enregistrée dans ce processus |
| `node.role` | `all-in-one` | Oui. Toute autre valeur renvoie une erreur |
| `node.data_dir` | `./metadb-data` | Oui |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | Oui. Adresse de liaison |
| `node.metrics_listen_addr` | non défini | Applied only when set. Separate TCP listener. An empty secret does not listen unless `allow_unauthenticated = true`, and that flag is loopback-only |
| `clock.max_clock_drift_ms` | `500` | Oui. `HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432` (32 Mio) | Oui. Scelle et vide le memtable actif dès que ses octets comptabilisés atteignent cette taille. Les valeurs inférieures à 1 sont traitées comme 1 |
| `storage.max_imm_memtables` | `4` | Oui. Les écritures attendent au lieu de mettre en file plus de memtables immuables que ce plafond |
| `storage.block_cache_bytes` | `67108864` (64 Mio) | Oui. Budget total du cache de blocs à 16 voies |
| `storage.mvcc_retention_ms` | `5000` | Oui |
| `storage.bloom_bits_per_key` | `10` | Oui |
| `storage.locality_tombstone_threshold` | `128` | Oui. Mappé vers `LsmEngineConfig.tombstone_density_threshold` |
| `storage.locality_tombstone_ratio` | `0.50` | Oui. Multiplié par 1000, borné à 1..=1000, stocké dans `tombstone_min_ratio_permille` |
| `sharding.bootstrap_table_shards` | `true` | Oui. `true` crée les shards 1, 2 et 3. `false` crée le shard 1 sur `[0x01, 0x04)` |
| `sharding.shard_split_threshold_bytes` | `4294967296` (4 Gio) | Oui. Le contrôle de découpage utilise la taille approximative du shard |
| `security.shared_secret` | `""` | Empty does not start the process. Loopback without credentials requires `allow_unauthenticated = true`. A non-empty secret requires the same bearer on gRPC and metrics. A whitespace-only secret is rejected |
| `security.allow_unauthenticated` | `false` | Explicit opt-in for an empty secret on loopback only. Rejected together with a usable secret. Rejected on a wildcard or any other non-loopback bind. A whitespace-only secret is rejected even with this flag |

Il n'y a pas de table `[raft]`. `ServerTomlConfig.raft_options` est ignoré par serde et reste à `RaftNodeOptions::default()` (battement 150 ms, élection 450–900 ms, conservation du journal d'instantané 0, segment 64 Mio).

That skip is current `main`. Unmerged pull request #38 proposes loading a `[raft]` table. If #38 merges, this sentence has to be rewritten. Loading `[raft]` is not current behavior.

`to_lsm_config` fixe toujours `auto_locality_compaction: true`.

## Exemple

Ce fichier correspond aux valeurs par défaut, sauf un répertoire de données explicite, un port de métriques et un secret. `memtable_size_bytes` est le seuil de vidage du memtable actif. `max_imm_memtables` est le plafond de contre-pression des tables immuables.

```toml
[node]
node_id = 1
cell_id = 1
role = "all-in-one"
data_dir = "/var/lib/metadb"
grpc_listen_addr = "127.0.0.1:7800"
metrics_listen_addr = "127.0.0.1:9100"

[clock]
max_clock_drift_ms = 500

[storage]
memtable_size_bytes = 33554432
max_imm_memtables = 4
block_cache_bytes = 67108864
mvcc_retention_ms = 5000
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296

[security]
shared_secret = "replace-with-a-long-random-string"
```

Les chaînes de rôle sont en kebab-case : `all-in-one`, `be`, `proxy`, `cell-manager`, `global-manager`.

Les options CLI remplacent le fichier après son analyse : `--node-id`, `--cell-id`, `--role`, `--listen`, `--data-dir`. `--cell-manager` n'est pas un champ TOML. C'est seulement une option de `metadbd`.

## Sécurité

Ce processus ne termine pas TLS. Aucune configuration de certificat n'active un TLS natif. Une clé TLS, SSL ou de certificat dans le fichier fait échouer l'analyse, et le processus ne sert pas en clair. Terminer TLS dans un proxy externe, et ne pas laisser de clé TLS dans ce fichier.

Lorsque `shared_secret` n'est pas vide, `MetadataService`, `TransactionService`, `ControlPlaneService` et `RaftTransportService` exigent que `authorization` soit exactement `Bearer <shared_secret>`. `GET /metrics` sur `metrics_listen_addr` exige le même en-tête `Authorization`. Un en-tête absent, un bearer vide, un préfixe ou un jeton plus long est rejeté. Le corps d'échec des métriques n'est pas du texte Prometheus. Les pairs Raft envoient le même bearer. `metadb-ctl --auth-token` (variable d'environnement `METADB_AUTH_TOKEN`) et `metadbd --join` / `--cell-manager` l'envoient. `metadb-ctl cluster status` appelle `GetRoutes` et passe par le même intercepteur.

An empty secret does not start the process, including when every listener is loopback (`127.0.0.0/8`, `::1`, or the name `localhost`). Loopback development without credentials requires `allow_unauthenticated = true`. That flag still refuses a wildcard (`0.0.0.0`, `::`) or any other non-loopback address. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. Listeners stay plaintext. Keep an opted-in listener on a trusted network, or put every service behind a proxy that authenticates the client and block direct access to this process.

## Ce que l'on ne peut pas configurer

- Le découpage des sous-systèmes par rôle. Le champ de rôle est un interrupteur autoriser/refuser, pas un ensemble de fonctions.
- Les listes de pairs, les ensembles de votants ou un jeton de jonction. L'appartenance commence par ce seul processus. Des pairs supplémentaires n'apparaissent qu'après `RegisterNode` plus `ScaleShardReplicas`, et ces pairs doivent déjà être des membres Raft qui parlent `RaftTransportService` pour les mêmes journaux de shard. Un second processus all-in-one ne le fait pas. Voir [Exploitation](./operations.md).
- L'intervalle de clés d'une seconde cellule. `RegisterCell` renvoie une erreur et ne stocke rien. Un second all-in-one au répertoire vide rejoint le leader courant avec `[cluster] join` ; c'est toujours la même cellule.
- Les temporisations Raft, ou l'activation de la compaction par localité.
- Les certificats TLS natifs. Une clé TLS dans ce fichier est rejetée. La terminaison appartient à un proxy externe.
