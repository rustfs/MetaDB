# Operability

[Overview](./README.md) · [Configuration](./configuration.md) · [Operations](./operations.md)

This page is the exact contract for health probes, readiness, the low-cardinality metrics added with them, and the `[raft]` table. It describes the process that results from this change. It is not a first-ship acceptance test and it is not a trillion-scale operations guide.

`[raft]` is loaded. `ServerTomlConfig.raft_options` is not `#[serde(skip)]`. The authentication rules in the next paragraph are unchanged by this `[raft]` loading.

`allow_unauthenticated` accepts only a literal loopback IP (`127.0.0.0/8` or `::1`). The hostname `localhost` is not accepted and is not resolved. After the gRPC socket and the metrics socket bind, the bound address is checked again. A whitespace-only secret is rejected by `classify_secret` before any bearer compare.

Locale configuration and architecture pages describe the same `[raft]` loading. Unknown keys in that table fail parsing. Values apply when a group starts and are not reloaded. `max_segment_bytes` does not rewrite segments already on disk. `/readyz` does not wait for the 250 ms 2PC recovery pass.

## Where the probes live

`node.metrics_listen_addr` is the only HTTP listener. When it is unset, this process has no `/livez`, `/readyz`, `/healthz`, or `/metrics`. There is no gRPC health RPC. `GRPC_ADDR` is written after bootstrap or join returns. It means the gRPC port is accepting. It does not mean `/readyz` is 200.

The metrics listener uses `classify_secret`, the same classification as gRPC. The body of a rejection is `unauthorized`. It does not contain the presented token or the configured secret, and it is not Prometheus text.

- `Absent`: the secret string is empty. The request is not challenged. `open_and_serve` reaches that state only when `[security].allow_unauthenticated = true` and every listener is loopback. A default config leaves the secret empty and the flag false, so startup refuses it. That default does not become this probe state.
- `WhitespaceOnly`: the secret is spaces or other whitespace and contains no other character. `/metrics`, `/livez`, `/readyz`, and `/healthz` return HTTP 401. That includes a request whose `Authorization` value is exactly `Bearer` plus the same whitespace. Startup rejects this secret even when `allow_unauthenticated` is set. The probe does not compare it as a configured bearer.
- `Configured`: the secret contains a non-whitespace character. `Authorization` must equal `Bearer <shared_secret>` exactly. A missing, empty, or different value is HTTP 401.

`allow_unauthenticated` still refuses a wildcard or any other non-loopback address. This listener does not terminate TLS.

Only `GET` is accepted. Any other method is HTTP 405. The route is the URI path, so a query string does not select another check and cannot ask for a dump. `GET /readyz?verbose=1` is the same probe as `GET /readyz`. There is no POST, no reload, and no shutdown URL. The probes are not a management API.

`/metrics` returns HTTP 200 with Prometheus text even when the process is not ready, so a scrape can see `metadb_ready 0`. Do not use `/metrics` as a traffic gate.

## Phase

`metadb_process_phase` is one unlabeled gauge:

| Value | Name | Meaning |
| :--- | :--- | :--- |
| 0 | `starting` | `open_and_serve` has not finished bootstrap, join, or the root-directory attempt |
| 1 | `serving` | That startup sequence returned. Shard conditions below still decide readiness |
| 2 | `shutting_down` | `shutdown` or `shutdown_without_flush` has started, and the process had not already failed |
| 3 | `failed` | Metrics bind or join failed. Shutdown does not overwrite this with `shutting_down` |

The metrics listener is bound before join, while the phase is still `starting`. A probe during join gets a response instead of "connection refused means not ready".

## Liveness and readiness

| Path | HTTP 200 | HTTP 503 |
| :--- | :--- | :--- |
| `GET /livez` | The phase is not `failed` and the handler can still answer. This includes `starting` and `shutting_down` | Phase is `failed`, or the cluster handle is already gone (`not live: unavailable`) |
| `GET /readyz` | Startup finished, shutdown has not started, and every local Raft group passes the shard checks | Anything else. The body is `not ready: <reason>` |
| `GET /healthz` | Same status as `/readyz` | Same status as `/readyz` |

`/healthz` is not a second readiness rule. Its status matches `/readyz`, so a caller that treats HTTP 200 as "send traffic" does not get a 200 merely because the socket is open. The body is one line:

```text
live=<0|1> ready=<0|1> phase=<name> reason=<token>
```

`/livez` is the restart probe. Do not point a restart probe at `/readyz` or `/healthz`. A leader election sets `leader_unknown` and those two paths return 503 on every replica that does not currently know a leader. Restarting the process for that would drop voters during the election.

`/livez` stays 200 during graceful shutdown so a restart probe does not kill the process while it flushes. The listener then closes, and later connections fail. A failed startup is `failed`: `/livez` is 503, and the process exits with the startup error.

gRPC is not closed when `/readyz` is 503. A starting or recovering voter must still accept `RaftTransportService`. The probe tells clients not to send metadata traffic. It does not fence the replica.

## When `/readyz` is 200

All of the following are required. The listener is not one of them.

1. Phase is `serving`.
2. At least one local Raft group is registered.
3. Every local group has all of:
   - storage is not marked unrecoverable
   - OpenRaft `running_state` is not a fatal error
   - the group has not entered `shutdown`
   - startup replay is not pending
   - this node is an applied voter
   - `last_applied` is set
   - `current_leader` is set

The first failing condition is the `reason` token, in that order: `no_local_shards`, `storage_unrecoverable`, `raft_fatal`, `raft_stopped`, `raft_recovery_pending`, `not_voter`, `raft_log_not_applied`, `leader_unknown`. Phase failures use `failed`, `shutting_down`, or `starting` and do not look at shards. A healthy shard list does not override those phases.

`current_leader` may be this node or a peer. Ready means this process can apply a client request or forward it to a known leader. It does not mean this process is the leader. A follower that does not know a leader is not ready, matching the RPC behavior that returns `NotLeader` in that case.

Startup replay is the committed or purged Raft index captured when the group opens. `ShardRaftGroup::start` waits up to 5 seconds for OpenRaft recovery. Success records the shard caught up. A timeout does not fail the process open and does not make the shard ready. The shard stays `raft_recovery_pending` until `last_applied` reaches that index. A fresh log with no committed or purged index does not wait and does not block readiness. An uncommitted tail alone does not block readiness.

`open_or_bootstrap` still runs incomplete-split recovery before it returns. A failure there aborts startup. That path is a startup error, not a 503 from a live process.

## What readiness does not cover

These stay out of the probe on purpose. Treating them as ready would be a broader claim than the code makes.

- The 250 ms `TxnRecoveryWorker` pass. Expired 2PC intents are not required to be drained before `/readyz` returns 200. The first pass runs after the phase flips to `serving`, and a failed pass is ignored by that loop.
- Backup holds, online backup copies, and restore. Those are a separate operator path.
- A second cell, Jumbo file movement, RDMA, and heartbeats. `StoreHeartbeat` still has no sender in `metadbd`.
- Disk space, clock health, and "this node can reach a quorum right now" beyond the leader id OpenRaft has published. `leader_unknown` is the check this process can make without starting a proposal.
- Per-shard series. Gauges below are counts. A shard id is not a label.

The shard gauges count flags independently. One group can increment both `metadb_shards_recovery_pending` and `metadb_shards_leader_unknown`. They are not a partition of `metadb_shards_total`. `metadb_shards_total` still counts shard handles registered on the cell. The `metadb_shards_*` operability gauges count local Raft groups. During shutdown those groups are removed from the host map, so the counts can drop to 0 while `metadb_process_phase` is `2` or `3`.

## Metrics

Existing series are unchanged: `metadb_cells_total`, `metadb_shards_total`, `metadb_in_memory_mvcc_writes_total`, `metadb_pruned_mvcc_versions_total`, `metadb_persistent_kv_writes_total`, `metadb_active_physical_tombstones`, `metadb_tombstones_traversed_on_scans_total`, `metadb_locality_compactions_total`, `metadb_min_raft_safe_purge_index`.

Unlabeled gauges added for operators:

| Name | Meaning |
| :--- | :--- |
| `metadb_ready` | 1 only when `/readyz` would be 200 |
| `metadb_live` | 1 when `/livez` would be 200 |
| `metadb_process_phase` | The table above |
| `metadb_shards_leader_known` | Local groups whose OpenRaft metrics have `current_leader` |
| `metadb_shards_leader_unknown` | Local groups that do not |
| `metadb_shards_recovery_pending` | Local groups still replaying the startup commit |
| `metadb_shards_log_not_applied` | Local groups with no `last_applied` |
| `metadb_shards_not_voter` | Local groups where this node is not an applied voter |
| `metadb_shards_storage_unrecoverable` | Local groups whose snapshot cannot be rebuilt |
| `metadb_shards_raft_stopped` | Local groups whose `shutdown` has started |
| `metadb_shards_raft_fatal` | Local groups whose OpenRaft `running_state` is an error |
| `metadb_raft_heartbeat_interval_ms` | Configured heartbeat. Not a measured RTT |
| `metadb_raft_election_timeout_min_ms` | Configured minimum election timeout |
| `metadb_raft_election_timeout_max_ms` | Configured maximum election timeout |
| `metadb_raft_max_in_snapshot_log_to_keep` | Configured OpenRaft snapshot-log retention |
| `metadb_raft_max_segment_bytes` | Configured segment size for new segment rotation |

No series carries a secret, a peer address, a data-directory path, or a user key. The storage-unrecoverable detail string stays in the process log and is not copied into the probe body.

## `[raft]`

```toml
[raft]
heartbeat_interval_ms = 150
election_timeout_min_ms = 450
election_timeout_max_ms = 900
max_in_snapshot_log_to_keep = 0
max_segment_bytes = 67108864
```

Omitted fields use those defaults. An omitted table uses all of them. Unknown keys inside `[raft]` fail parsing. The same OpenRaft check used at group start rejects the table when `election_timeout_min_ms` is not greater than `heartbeat_interval_ms`, or when it is not less than `election_timeout_max_ms`. `max_segment_bytes = 0` is rejected because rotation would open a file on every append. `max_in_snapshot_log_to_keep = 0` remains valid.

The values are applied when each `ShardRaftGroup` starts. They are not reloaded from a changed file. A later edit requires a process restart. `max_segment_bytes` affects rotation of the active segment. It does not rewrite segments already on disk. The five numbers are logged once at info when startup finishes (`applied raft options`) and are the gauges above. The log line does not include the shared secret.

`join_timeout` is still not a TOML field.

Other unknown keys outside `[raft]` and outside the security-key rejection are still ignored. That fail-open hole is unchanged. `[security]` parsing is unchanged: a TLS key, a misspelled `shared_secret`, and a secret outside `[security]` still refuse to start.
