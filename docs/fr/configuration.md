# Configuration

[Présentation](./README.md) · [English](../en/configuration.md) · [Deutsch](../de/configuration.md) · [日本語](../ja/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` et `ServerTomlConfig::from_toml_file` / `from_toml_str` lisent le TOML (`crates/metadb-server/src/config.rs`). Les clés inconnues qui ne nomment ni un secret ni TLS sont ignorées. Un `shared_secret` mal orthographié, une clé de jeton ou de mot de passe, ou une clé TLS ou de certificat sont refusés, et le processus ne démarre pas. Chaque section connue est facultative. Les champs omis prennent les valeurs par défaut ci-dessous.

## Valeurs par défaut

| Clé | Défaut | Appliquée par `open_and_serve` |
| :--- | :--- | :--- |
| `node.node_id` | `1` | Oui. Identifiant de nœud Raft et pair d'amorçage |
| `node.cell_id` | `1` | Oui. Nom de répertoire `cell_<id>` et seule cellule enregistrée dans ce processus |
| `node.role` | `all-in-one` | Oui. Toute autre valeur renvoie une erreur |
| `node.data_dir` | `./metadb-data` | Oui |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | Oui. Adresse de liaison |
| `node.metrics_listen_addr` | non défini | Oui, seulement s'il est défini. Écouteur TCP séparé. Une adresse non locale exige un secret non vide |
| `clock.max_clock_drift_ms` | `500` | Oui. `HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432` (32 Mio) | **Non.** Le champ est stocké et jamais lu |
| `storage.block_cache_bytes` | `67108864` (64 Mio) | Oui. Budget total du cache de blocs à 16 voies |
| `storage.mvcc_retention_ms` | `5000` | Oui |
| `storage.bloom_bits_per_key` | `10` | Oui |
| `storage.locality_tombstone_threshold` | `128` | Oui. Mappé vers `LsmEngineConfig.tombstone_density_threshold` |
| `storage.locality_tombstone_ratio` | `0.50` | Oui. Multiplié par 1000, borné à 1..=1000, stocké dans `tombstone_min_ratio_permille` |
| `sharding.bootstrap_table_shards` | `true` | Oui. `true` crée les shards 1, 2 et 3. `false` crée le shard 1 sur `[0x01, 0x04)` |
| `sharding.shard_split_threshold_bytes` | `4294967296` (4 Gio) | Oui. Le contrôle de découpage utilise la taille approximative du shard |
| `security.shared_secret` | `""` | Oui. Vide n'est accepté que si chaque écouteur est en boucle locale. Non vide exige le même bearer pour le gRPC et les métriques |

Il n'y a pas de table `[raft]`. `ServerTomlConfig.raft_options` est ignoré par serde et reste à `RaftNodeOptions::default()` (battement 150 ms, élection 450–900 ms, conservation du journal d'instantané 0, segment 64 Mio).

`to_lsm_config` fixe toujours `auto_locality_compaction: true`.

## Exemple

Ce fichier correspond aux valeurs par défaut, sauf un répertoire de données explicite, un port de métriques et un secret. `memtable_size_bytes` est montré pour qu'une configuration copiée ne surprenne pas ; le modifier ne change pas le comportement de vidage.

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

Un secret vide ou composé seulement d'espaces n'est accepté que si `grpc_listen_addr` et `metrics_listen_addr` (s'il est défini) sont en boucle locale (`127.0.0.0/8`, `::1`, ou le nom `localhost`). Un joker (`0.0.0.0`, `::`) ou toute autre adresse refuse d'écouter. Un secret vide en boucle locale produit un avertissement au démarrage. Les écouteurs restent en clair. Les garder sur un réseau de confiance, ou placer chaque service derrière un proxy qui authentifie le client, et bloquer l'accès direct à ce processus.

## Ce que l'on ne peut pas configurer

- Le découpage des sous-systèmes par rôle. Le champ de rôle est un interrupteur autoriser/refuser, pas un ensemble de fonctions.
- Les listes de pairs, les ensembles de votants ou un jeton de jonction. L'appartenance commence par ce seul processus. Des pairs supplémentaires n'apparaissent qu'après `RegisterNode` plus `ScaleShardReplicas`, et ces pairs doivent déjà être des membres Raft qui parlent `RaftTransportService` pour les mêmes journaux de shard. Un second processus all-in-one ne le fait pas. Voir [Exploitation](./operations.md).
- L'intervalle de clés d'une seconde cellule. `RegisterCell` renvoie une erreur et ne stocke rien. Un second all-in-one au répertoire vide rejoint le leader courant avec `[cluster] join` ; c'est toujours la même cellule.
- La limite en octets du memtable, les temporisations Raft, ou l'activation de la compaction par localité.
- Les certificats TLS natifs. Une clé TLS dans ce fichier est rejetée. La terminaison appartient à un proxy externe.
