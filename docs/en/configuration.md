# Configuration

[Overview](./README.md) · [Français](../fr/configuration.md) · [Deutsch](../de/configuration.md) · [日本語](../ja/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` and `ServerTomlConfig::from_toml_file` / `from_toml_str` read TOML (`crates/metadb-server/src/config.rs`). Unknown keys are ignored. The structs use `#[serde(default)]` and do not set `deny_unknown_fields`, and `from_toml_str` calls `toml::from_str` directly. A misspelled key keeps its default, including an empty `security.shared_secret`, and startup still succeeds. Every section is optional. Omitted fields take the defaults below.

## Defaults

| Key | Default | Applied by `open_and_serve` |
| :--- | :--- | :--- |
| `node.node_id` | `1` | Yes. Raft node id and the bootstrap peer |
| `node.cell_id` | `1` | Yes. Directory name `cell_<id>` and the only cell registered in this process |
| `node.role` | `all-in-one` | Yes. Any other value returns an error |
| `node.data_dir` | `./metadb-data` | Yes |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | Yes. Bind address |
| `node.metrics_listen_addr` | unset | Yes, only when set. `metadbd` binds a separate TCP listener |
| `clock.max_clock_drift_ms` | `500` | Yes. `HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432` (32 MiB) | **No.** The field is stored and never read |
| `storage.block_cache_bytes` | `67108864` (64 MiB) | Yes. Total budget of the 16-way block cache |
| `storage.mvcc_retention_ms` | `5000` | Yes |
| `storage.bloom_bits_per_key` | `10` | Yes |
| `storage.locality_tombstone_threshold` | `128` | Yes. Mapped to `LsmEngineConfig.tombstone_density_threshold` |
| `storage.locality_tombstone_ratio` | `0.50` | Yes. Multiplied by 1000, clamped to 1..=1000, stored as `tombstone_min_ratio_permille` |
| `sharding.bootstrap_table_shards` | `true` | Yes. `true` creates shards 1, 2, and 3. `false` creates shard 1 over `[0x01, 0x04)` |
| `sharding.shard_split_threshold_bytes` | `4294967296` (4 GiB) | Yes. Split check uses approximate shard size |
| `security.shared_secret` | `""` | Yes. Empty means metadata, transaction, and control-plane RPCs are unauthenticated |

There is no `[raft]` table. `ServerTomlConfig.raft_options` is skipped by serde and stays at `RaftNodeOptions::default()` (heartbeat 150 ms, election 450–900 ms, snapshot log keep 0, segment 64 MiB).

`to_lsm_config` always sets `auto_locality_compaction: true`.

## Example

This file matches the defaults except for an explicit data directory, a metrics port, and a secret. `memtable_size_bytes` is shown so a copied config is not surprising; changing it does not change flush behavior.

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

Role strings are kebab-case: `all-in-one`, `be`, `proxy`, `cell-manager`, `global-manager`.

CLI flags override the file after it is parsed: `--node-id`, `--cell-id`, `--role`, `--listen`, `--data-dir`. `--cell-manager` is not a TOML field. It is only a `metadbd` flag.

## Security

The process does not terminate TLS.

When `shared_secret` is non-empty, `MetadataService`, `TransactionService`, and `ControlPlaneService` require `authorization` to equal `Bearer <shared_secret>` exactly. Clients use a separate interceptor that attaches that header. `metadb-ctl --auth-token` (environment variable `METADB_AUTH_TOKEN`) and `metadbd --join` / `--cell-manager` send it. A prefix or a longer token is rejected. An empty secret still means those three services are unauthenticated.

`RaftTransportService` is registered without the interceptor. A bearer token does not authenticate Raft RPCs on that port.

An empty secret allows metadata, transaction, and control-plane RPCs. The code warns about this at startup. A bearer secret does not cover `RaftTransportService`, and a TLS terminator in front of the listener does not add that check: after termination, Raft RPCs are still unauthenticated. Before an untrusted client can reach the port, keep the bind on a trusted network such as `127.0.0.1`, or put every service behind a proxy that authenticates the client (for example mutual TLS) and block direct access to this process.

## What you cannot configure

- Per-role subsystem trimming. The role field is an allow/deny switch, not a feature set.
- A voter list in TOML. The first process bootstraps alone. A later all-in-one process joins with `[cluster] join = "host:port"` (or `--join` / `--cell-manager`) pointing at the current leader, and only when its data directory does not already own a cell. The leader must be reached directly; a follower does not forward the membership change. That join is the same cell, not a second cell. See [Operations](./operations.md).
- A second cell's key range. `RegisterCell` returns an error and stores nothing.
- Memtable byte limit, Raft timers, or locality-compaction on/off.
- TLS certificates.
