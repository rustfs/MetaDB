# Configuration

[Overview](./README.md) · [Français](../fr/configuration.md) · [Deutsch](../de/configuration.md) · [日本語](../ja/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` and `ServerTomlConfig::from_toml_file` / `from_toml_str` read TOML (`crates/metadb-server/src/config.rs`). Unknown keys that do not name a secret, authentication, or TLS setting are ignored, except inside `[raft]` and `[transport]`: an unknown key in either table fails parsing. A misspelled `shared_secret`, a token, password, anonymous, or insecure key, or any TLS or certificate key is rejected and startup does not proceed, including those keys nested in tables or arrays. The only accepted keys directly under the root `[security]` table are `shared_secret` and `allow_unauthenticated`. Every known section is optional. Omitted fields take the defaults below.

## Defaults

| Key | Default | Applied by `open_and_serve` |
| :--- | :--- | :--- |
| `node.node_id` | `1` | Yes. Raft node id and the bootstrap peer |
| `node.cell_id` | `1` | Yes. Directory name `cell_<id>` for a process that hosts a cell. `global-manager` does not create that directory |
| `node.role` | `all-in-one` | Yes. `all-in-one`, `cell-manager`, and `global-manager` start. `be` and `proxy` return an error before listen |
| `cluster.global_manager` | empty | Yes, only for `cell-manager`. `host:port` of a `global-manager`. Empty does not register. `all-in-one` refuses to start when this is set |
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
| `sharding.jumbo_dedicated_root` | empty | Yes. A real directory outside the data directory. Empty does not move files. A symlink, or a path inside the data directory, refuses startup. The copy stays on this machine |
| `security.shared_secret` | `""` | Yes. Empty does not start a listener. A value with a non-whitespace character requires the same bearer on gRPC and the metrics listener. Whitespace-only is rejected |
| `security.allow_unauthenticated` | `false` | Yes. Explicit loopback-only development opt-in. It does not allow a non-loopback bind, and it cannot be combined with a usable secret |
| `raft.heartbeat_interval_ms` | `150` | Yes, when each local Raft group starts. Not reloaded while the process runs |
| `raft.election_timeout_min_ms` | `450` | Yes. Must be greater than the heartbeat and less than the maximum |
| `raft.election_timeout_max_ms` | `900` | Yes. Must be greater than the minimum |
| `raft.max_in_snapshot_log_to_keep` | `0` | Yes. Passed to OpenRaft |
| `raft.max_segment_bytes` | `67108864` (64 MiB) | Yes, for new segment rotation. `0` is rejected. Existing segment files are not rewritten |
| `transport.mode` | `tcp-grpc` | Yes. `tcp-grpc` keeps tonic gRPC over TCP. `prefer-rdma` also serves TCP/gRPC and records that RDMA was rejected. `rdma` (and `roce`, `rocev2`, `infiniband`, `ib`, `ibverbs`, `verbs`) refuses to start and does not bind. `tcp` and `grpc` are not aliases |
| `transport.negotiation_timeout_ms` | `1000` | Yes, as the in-process negotiation budget. Allowed range is `1..=60000`. There is no network handshake |
| `transport.max_inflight_negotiations` | `64` | Yes, as a ceiling. Allowed range is `1..=4096`. Zero is not unlimited |
| `transport.max_queue_pairs` | `16` | Stored as a future ceiling only. Allowed range is `1..=1024`. No queue pair is created |
| `transport.max_registered_bytes` | `16777216` (16 MiB) | Stored as a future ceiling only. Allowed range is `1..=67108864`. No memory is registered |
| `transport.max_message_bytes` | `1048576` (1 MiB) | Stored as a future ceiling only. Allowed range is `1..=8388608`. No RDMA message is posted |

`[raft]` is loaded. A missing table keeps `RaftNodeOptions::default()`. Unknown keys inside `[raft]` fail parsing. The inequalities are the same ones OpenRaft checks at group start. See [Operability](./operability.md).

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

[raft]
heartbeat_interval_ms = 150
election_timeout_min_ms = 450
election_timeout_max_ms = 900
max_in_snapshot_log_to_keep = 0
max_segment_bytes = 67108864
```

Role strings are kebab-case: `all-in-one`, `be`, `proxy`, `cell-manager`, `global-manager`. `global-manager` loads `GLOBAL_CELL_DIRECTORY` and does not open `be/` or a cell Raft group. `cell-manager` opens one local cell and its Raft groups. When `cluster.global_manager` is set, it calls `RegisterCell` on that address after the listener accepts and shuts down if the call fails. `be` and `proxy` still return before listen. The directory file is not a Raft group.

CLI flags override the file after it is parsed: `--node-id`, `--cell-id`, `--role`, `--listen`, `--data-dir`. `--cell-manager` is not a TOML field. It is only a `metadbd` flag.

## Security

This process does not terminate TLS. There is no certificate configuration that turns on native TLS. If the file contains a TLS, SSL, or certificate key, parsing fails and the process does not serve plaintext. Terminate TLS in an external proxy, and do not leave a TLS key in this file.

When `shared_secret` contains a non-whitespace character, `MetadataService`, `TransactionService`, `ControlPlaneService`, and `RaftTransportService` require `authorization` to equal `Bearer <shared_secret>` exactly. Every request on `metrics_listen_addr`, including `GET /metrics`, `GET /livez`, `GET /readyz`, and `GET /healthz`, requires the same `Authorization` header. A missing header, an empty bearer, a prefix, or a longer token is rejected. The failure body is `unauthorized` and is not Prometheus text. It does not echo the token. A whitespace-only secret does not take that exact-match path: metrics and probes return 401 even when `Authorization` is `Bearer` plus those same spaces. Raft peers send the same bearer when the secret is usable. `metadb-ctl --auth-token` (environment variable `METADB_AUTH_TOKEN`) and `metadbd --join` / `--cell-manager` send it. `metadb-ctl cluster status` calls `GetRoutes` and is covered by that interceptor. The client interceptor rejects a whitespace-only token instead of sending it.

An empty `shared_secret` does not start the process, including when every address is loopback. That is a compatibility change from the previous loopback default. Local development without credentials must set `allow_unauthenticated = true` and keep `grpc_listen_addr` and `metrics_listen_addr` (if set) on a literal loopback IP (`127.0.0.0/8` or `::1`). The hostname `localhost` is not accepted for this opt-in and is not resolved, including through a hosts file. A usable secret may still bind a hostname. Compatibility: an opted-in config that used `localhost` no longer starts. A wildcard (`0.0.0.0`, `::`) or any other address still refuses to listen. A whitespace-only secret is rejected even with that flag. Setting both a usable secret and `allow_unauthenticated` is rejected. The listeners are still plaintext. Keep an opted-in listener on a trusted network, or put every service behind a proxy that authenticates the client and block direct access to this process.

`empty_secret_on_loopback_does_not_listen` and `whitespace_secret_on_loopback_does_not_listen` cover those refusals. `explicit_unauthenticated_loopback_serves_without_credentials` covers the opt-in. A configured secret on loopback still rejects a missing header, `Authorization: Basic ...`, a lowercase `bearer` scheme, and a wrong bearer, and the rejected create is not visible afterwards.

A key fails parsing when its normalized form (lowercase, hyphens become underscores) contains one of these substrings, including inside nested tables and arrays: `secret`, `password`, `passwd`, `token`, `credential`, `auth`, `anonymous`, `anon`, `insecure`, `api_key`, `apikey`, `bearer`, `jwt`, `private_key`, `privatekey`, `encryption`, `encrypt`, `tls`, `ssl`, `cert`, `x509`, `mtls`, `cipher`, `keystore`, `https`, `ca_file`, `cafile`, or it starts with `secur`. The only exceptions are the direct keys `[security].shared_secret` and `[security].allow_unauthenticated`. This is not a rejection of every unknown key. `bloom_bits_per_key` and a misspelled `grpc_listen_addr` or storage key are still ignored. Unknown keys inside `[raft]` and `[transport]` fail parsing even when they do not match those substrings. `unknown_toml_keys_are_ignored_and_loopback_still_serves` parses `grpc_listen_addrr`, `memtable_size_bytez`, and `joinn`, keeps those defaults, and serves only because that fixture also sets `allow_unauthenticated`. Closing the remaining unknown-key hole means rejecting those keys, not teaching the server a new spelling.

## Transport

The carried transport is tonic gRPC over TCP. That path is unchanged when `[transport]` is omitted. This process does not implement RDMA. `RDMA_BACKEND_IMPLEMENTED` is false. No queue pair, completion queue, or registered memory is created, and the process does not open `/dev/infiniband` or load Soft-RoCE.

`mode = "rdma"` fails configuration parsing and `open_and_serve` before a data directory is created or a socket is bound. It does not fall back to TCP. `mode = "prefer-rdma"` starts the existing gRPC listener, logs that RDMA was rejected (`backend-not-implemented`), and records the selection as TCP/gRPC. The fallback is not an RDMA session.

Unknown keys inside `[transport]` are rejected. A `device` or `libibverbs` key is rejected because no RDMA backend is implemented. `insecure` is already rejected by the security-key walk. RDMA is not a second authorization domain and does not bypass `[security].shared_secret` or `allow_unauthenticated`.

Resource keys are ceilings for a backend this repository does not have. Setting them does not allocate those resources. The design note is [RDMA transport policy](../designs/rdma_transport.md). Locale copies of this page do not describe `[transport]` yet; they must not be read as enabling RDMA.

## What you cannot configure

- A remote `be` or a forwarding `proxy`. Those roles are rejected before listen. `cell-manager` still colocates its Raft groups. `global-manager` does not allocate ids, mint rename leases, or forward keys.
- A voter list in TOML. The first process bootstraps alone. A later all-in-one process joins with `[cluster] join = "host:port"` (or `--join` / `--cell-manager`) pointing at the current leader, and only when its data directory does not already own a cell. The leader must be reached directly; a follower does not forward the membership change. That join is the same cell, not a second cell. See [Operations](./operations.md).
- An arbitrary key slice as a cell range. `[cluster] cell_start_key` and `cell_end_key` are even-length hex and must be a contiguous union of the meta (`01`..`02`), quota (`02`..`03`), and secondary-index (`03`..`04`) tables. Empty means the whole user key space, which is the historical default. Join cannot be combined with those keys. The other process is not started by this file.
- Locality-compaction on/off. `to_lsm_config` always enables it. Raft timers are the `[raft]` table; see [Operability](./operability.md).
- Native TLS certificates. A TLS key in this file is rejected. Termination belongs in an external proxy.
- A working RDMA device, provider library, or unauthenticated bypass listener. `mode = "rdma"` refuses to start. `prefer-rdma` keeps TCP/gRPC after an explicit rejection.
