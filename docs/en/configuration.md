# Configuration

[Overview](./README.md) · [Français](../fr/configuration.md) · [Deutsch](../de/configuration.md) · [日本語](../ja/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` and `ServerTomlConfig::from_toml_file` / `from_toml_str` read TOML (`crates/metadb-server/src/config.rs`). Unknown keys that do not name a secret or TLS setting are ignored, except inside `[transport]`, where unknown keys are rejected. A misspelled `shared_secret`, a token or password key, or any TLS or certificate key is rejected and startup does not proceed, including those keys nested in tables or arrays. `[security].shared_secret` is accepted only as a direct key of the root `[security]` table. Every known section is optional. Omitted fields take the defaults below.

## Defaults

| Key | Default | Applied by `open_and_serve` |
| :--- | :--- | :--- |
| `node.node_id` | `1` | Yes. Raft node id and the bootstrap peer |
| `node.cell_id` | `1` | Yes. Directory name `cell_<id>` and the only cell registered in this process |
| `node.role` | `all-in-one` | Yes. Any other value returns an error |
| `node.data_dir` | `./metadb-data` | Yes |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | Yes. Bind address |
| `node.metrics_listen_addr` | unset | Yes, only when set. Separate TCP listener. A non-loopback address requires a non-empty secret |
| `clock.max_clock_drift_ms` | `500` | Yes. `HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432` (32 MiB) | Yes. Seals and flushes the active memtable once its accounted bytes reach this size. Values below 1 are treated as 1 |
| `storage.max_imm_memtables` | `4` | Yes. Writers wait instead of queueing more immutable memtables than this cap |
| `storage.block_cache_bytes` | `67108864` (64 MiB) | Yes. Total budget of the 16-way block cache |
| `storage.mvcc_retention_ms` | `5000` | Yes |
| `storage.bloom_bits_per_key` | `10` | Yes |
| `storage.locality_tombstone_threshold` | `128` | Yes. Mapped to `LsmEngineConfig.tombstone_density_threshold` |
| `storage.locality_tombstone_ratio` | `0.50` | Yes. Multiplied by 1000, clamped to 1..=1000, stored as `tombstone_min_ratio_permille` |
| `sharding.bootstrap_table_shards` | `true` | Yes. `true` creates shards 1, 2, and 3. `false` creates shard 1 over `[0x01, 0x04)` |
| `sharding.shard_split_threshold_bytes` | `4294967296` (4 GiB) | Yes. Split check uses approximate shard size |
| `security.shared_secret` | `""` | Yes. Empty is accepted only for loopback listeners. Non-empty requires the same bearer on gRPC and metrics |
| `transport.mode` | `tcp-grpc` | Yes. `tcp-grpc` keeps tonic gRPC over TCP. `prefer-rdma` also serves TCP/gRPC and records that RDMA was rejected. `rdma` (and `roce`, `rocev2`, `infiniband`, `ib`, `ibverbs`, `verbs`) refuses to start and does not bind. `tcp` and `grpc` are not aliases |
| `transport.negotiation_timeout_ms` | `1000` | Yes, as the in-process negotiation budget. Allowed range is `1..=60000`. There is no network handshake |
| `transport.max_inflight_negotiations` | `64` | Yes, as a ceiling. Allowed range is `1..=4096`. Zero is not unlimited |
| `transport.max_queue_pairs` | `16` | Stored as a future ceiling only. Allowed range is `1..=1024`. No queue pair is created |
| `transport.max_registered_bytes` | `16777216` (16 MiB) | Stored as a future ceiling only. Allowed range is `1..=67108864`. No memory is registered |
| `transport.max_message_bytes` | `1048576` (1 MiB) | Stored as a future ceiling only. Allowed range is `1..=8388608`. No RDMA message is posted |

There is no `[raft]` table. `ServerTomlConfig.raft_options` is skipped by serde and stays at `RaftNodeOptions::default()` (heartbeat 150 ms, election 450–900 ms, snapshot log keep 0, segment 64 MiB).

`to_lsm_config` always sets `auto_locality_compaction: true`.

## Example

This file matches the defaults except for an explicit data directory, a metrics port, and a secret. `memtable_size_bytes` is the active memtable flush threshold. `max_imm_memtables` is the immutable-table backpressure cap.

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

Role strings are kebab-case: `all-in-one`, `be`, `proxy`, `cell-manager`, `global-manager`.

CLI flags override the file after it is parsed: `--node-id`, `--cell-id`, `--role`, `--listen`, `--data-dir`. `--cell-manager` is not a TOML field. It is only a `metadbd` flag.

## Security

This process does not terminate TLS. There is no certificate configuration that turns on native TLS. If the file contains a TLS, SSL, or certificate key, parsing fails and the process does not serve plaintext. Terminate TLS in an external proxy, and do not leave a TLS key in this file.

When `shared_secret` is non-empty, `MetadataService`, `TransactionService`, `ControlPlaneService`, and `RaftTransportService` require `authorization` to equal `Bearer <shared_secret>` exactly. `GET /metrics` on `metrics_listen_addr` requires the same `Authorization` header. A missing header, an empty bearer, a prefix, or a longer token is rejected. The metrics failure body is not Prometheus text. Raft peers send the same bearer. `metadb-ctl --auth-token` (environment variable `METADB_AUTH_TOKEN`) and `metadbd --join` / `--cell-manager` send it. `metadb-ctl cluster status` calls `GetRoutes` and is covered by that interceptor.

An empty or whitespace-only secret is accepted only when `grpc_listen_addr` and `metrics_listen_addr` (if set) are loopback (`127.0.0.0/8`, `::1`, or the name `localhost`). A wildcard (`0.0.0.0`, `::`) or any other address refuses to listen. The code warns about an empty loopback secret at startup. The listeners are still plaintext. Keep them on a trusted network, or put every service behind a proxy that authenticates the client and block direct access to this process.

`empty_secret_on_loopback_serves_without_credentials` locks the current gap: `127.0.0.1` and an empty secret serve metadata, transactions, the control plane, and `GET /metrics` with no credentials. A `Basic` header is not consulted. A configured secret on that same loopback bind does not fail open: a missing header, `Authorization: Basic ...`, a lowercase `bearer` scheme, and a wrong bearer are rejected, and the rejected create is not visible afterwards. A whitespace secret still starts on loopback, and an anonymous call is rejected rather than treated as no secret.

Unknown keys that do not name a secret or TLS setting remain a fail-open hole. `unknown_toml_keys_are_ignored_and_loopback_still_serves` parses `grpc_listen_addrr`, `memtable_size_bytez`, and `joinn`, keeps the defaults, and serves. A `token_file` or `tls_mode` key still fails parsing. Closing that hole means rejecting those unknown keys, not teaching the server a new spelling.

## Transport

The carried transport is tonic gRPC over TCP. That path is unchanged when `[transport]` is omitted. This process does not implement RDMA. `RDMA_BACKEND_IMPLEMENTED` is false. No queue pair, completion queue, or registered memory is created, and the process does not open `/dev/infiniband` or load Soft-RoCE.

`mode = "rdma"` fails configuration parsing and `open_and_serve` before a data directory is created or a socket is bound. It does not fall back to TCP. `mode = "prefer-rdma"` starts the existing gRPC listener, logs that RDMA was rejected (`backend-not-implemented`), and records the selection as TCP/gRPC. The fallback is not an RDMA session.

Unknown keys inside `[transport]` are rejected. That is narrower than the rest of the file, where unknown non-secret keys are still ignored. An ignored `device`, `libibverbs`, or `insecure` key would look like a configured fast path. A key that asks for an unauthenticated or unverified transport is refused. RDMA is not a second authorization domain and does not bypass `[security].shared_secret`.

Resource keys are ceilings for a backend this repository does not have. Setting them does not allocate those resources. The design note is [RDMA transport policy](../designs/rdma_transport.md). Locale copies of this page do not describe `[transport]` yet; they must not be read as enabling RDMA.

## What you cannot configure

- Per-role subsystem trimming. The role field is an allow/deny switch, not a feature set.
- A voter list in TOML. The first process bootstraps alone. A later all-in-one process joins with `[cluster] join = "host:port"` (or `--join` / `--cell-manager`) pointing at the current leader, and only when its data directory does not already own a cell. The leader must be reached directly; a follower does not forward the membership change. That join is the same cell, not a second cell. See [Operations](./operations.md).
- A second cell's key range. `RegisterCell` returns an error and stores nothing.
- Raft timers or locality-compaction on/off.
- Native TLS certificates. A TLS key in this file is rejected. Termination belongs in an external proxy.
- A working RDMA device, provider library, or unauthenticated bypass listener. `mode = "rdma"` refuses to start. `prefer-rdma` keeps TCP/gRPC after an explicit rejection.
