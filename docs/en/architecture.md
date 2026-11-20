# Architecture as implemented

[Overview](./README.md) · [Français](../fr/architecture.md) · [Deutsch](../de/architecture.md) · [日本語](../ja/architecture.md) · [简体中文](../zh/architecture.md)

This page follows the code in `crates/`. `global-manager`, `cell-manager`, `be`, and `proxy` can run as separate processes. A bootstrap leader copies `GLOBAL_CELL_DIRECTORY` and a `CELL_MANAGER_STATE` snapshot into the lowest local shard's existing Raft log. `global-manager` still loads only a local file and does not open that group. The proxy chooses a registered cell for a contained metadata call, `ExecuteSql`, or user-key transaction, and fails closed otherwise. See [Operations](./operations.md#known-limitations).

## Process shape

`PersistentMetaDbCluster::open_and_serve` (`crates/metadb-server/src/lib.rs`):

1. `be` hosts local Raft replicas and refuses `cluster.global_manager` or `cluster.upstream`. `proxy` requires one `cluster.upstream`, refuses join, a cell range, a jumbo root, and https, and does not open Raft. `global-manager` refuses a join address, a cell range, a jumbo root, an upstream, or a data directory that already has `be/` or `CELL_MANAGER_STATE`. `all-in-one` refuses `cluster.global_manager` and `cluster.upstream`. Invalid combinations return before the process binds.
2. An empty `shared_secret` listens only when `allow_unauthenticated` is explicitly set, and only when every listener address is a literal loopback IP (`127.0.0.0/8` or `::1`). The hostname `localhost` is not accepted and is not resolved. A whitespace-only secret is rejected even when that flag is set. The flag does not permit a wildcard or any other address. The process then binds that same literal address (`grpc_listen_addr` defaults to `127.0.0.1:7800`; port `0` asks the OS for a port). After the socket is bound, the bound IP is checked again.
3. `global-manager` stops after loading `GLOBAL_CELL_DIRECTORY` and opening `shared_id_allocator/SHARED_ID_ALLOCATOR`. It does not open `ShardGroupHost`, does not bootstrap a cell, and does not run the 250 ms maintenance loop. `AllocateIdBlock` grants from that shared file. Rename-lease RPCs still return an application error. A metadata or transaction request contained by one published cell is forwarded to that cell's endpoint, with the process bearer. A key this process does not own is not applied on a local shard. Readiness is `directory_only` when the process is serving and has no local shards. An empty shard list is still `no_local_shards` for every other role. This role has no shard group, so its directory file is not proposed onto a shard log. A bootstrap leader that does host a shard still proposes `GLOBAL_CELL_DIRECTORY` and a `CELL_MANAGER_STATE` snapshot into that shard's existing log.
4. `all-in-one` and `cell-manager` open `DurableIdAllocator` at `<data_dir>/global_allocator`, `ShardGroupHost` at `<data_dir>/be`, and `DurableCellManager` at `<data_dir>/cell_<cell_id>`.
5. Those two roles register that one local cell on their own `DurableGlobalManager`. `cell-manager` does not accept a remote `RegisterCell`. When `cluster.global_manager` is set, it publishes its route union to that process after the listener accepts, using the same `TransportSecurity` dial as every other outbound client, and startup fails if that call fails.
6. Ensures root directory inode `1` (`ROOT_INODE_ID`) has an attribute record on a process that hosts the meta shard.
7. Serves four gRPC services on the same listener. All four take the same bearer interceptor. When `metrics_listen_addr` is set, `GET /metrics`, `GET /livez`, `GET /readyz`, and `GET /healthz` use `classify_secret`. A secret that contains a non-whitespace character rejects a missing or wrong bearer. A whitespace-only secret is 401 on those paths and does not match `Bearer` plus the same spaces. An empty secret listens only when `allow_unauthenticated` is set, and only for a literal loopback IP. The bound metrics address is checked again after that socket binds. This process does not terminate TLS. `[security.tls]` mode `tls` or `mtls` is refused before a data directory or listener is created. Per-method authorization is installed when `[security.rbac]` is present, and an unauthorized method is refused. When that table is absent, the layer is not installed. The shared bearer is not a role.
8. `all-in-one` and `cell-manager` spawn a maintenance task on a 250 ms interval. `global-manager` does not.

`metadbd` is a thin wrapper: it loads TOML, applies CLI overrides, calls `open_and_serve`, optionally calls `RegisterNode` on `--cell-manager`, and on Ctrl-C calls `shutdown`. Metrics, when configured, are served inside `open_and_serve`.

## Keys and tables

User-table prefixes (`crates/metadb-core/src/schema.rs`):

| Prefix | Table | Bootstrap range when `bootstrap_table_shards = true` |
| :--- | :--- | :--- |
| `0x01` | `MetaTable` | shard 1, `[0x01, 0x02)` |
| `0x02` | `QuotaTable` | shard 2, `[0x02, 0x03)` |
| `0x03` | `SecondaryIndexTable` | shard 3, `[0x03, 0x04)` |

`MetaTable` keys are `[0x01][pid: 8-byte big-endian][name UTF-8]`. The directory's own attributes are the reserved child `(pid, "_attr_")`, stored next to `(pid, child_name)`. User names that are empty, contain NUL, or equal `_attr_` are rejected (`ReservedEntryName`).

Directory inode ids have bit 63 clear. File inode ids have bit 63 set (`FILE_INODE_TAG_BIT`). Root is inode `1`. The durable allocator starts new directory ids at `2`, file ids at `1`, and dynamic shard ids at `4` (1–3 are the bootstrap shards). `LocalIdBuffer` grants directory ids in blocks of 256 and file inode ids and file ids in blocks of 4,096, after the grant is written to `ID_ALLOCATOR_STATE`.

Shard-local system prefixes:

| Prefix | Record |
| :--- | :--- |
| `0x04` | `DirShardDelta` for a non-anchor dual-mode sub-shard |
| `0x05` | 2PC `WriteIntent` |
| `0x06` | 2PC `TxnRecord` |
| `0x07` | spilled auxiliary outbox entries |

When `bootstrap_table_shards = false`, bootstrap creates one shard, id `1`, covering `[0x01, 0x04)`.

## Request path

A metadata call enters either the embedded `DistributedTxRouter` (`cluster.tx_router()`) or gRPC `MetadataService` / `TransactionService`. `StatelessProxyRouter` compiles one of the 20 SQL templates and calls the same router. There is no general SQL engine. The parser accepts only the statements below (`crates/metadb-proxy/src/sql_templates.rs`). `?` placeholders bind `u64`, `i64`, or string parameters. A literal `name = '_attr_'` is directory stat. A bound parameter equal to `_attr_` is rejected.

| # | Statement | Effect |
| :--- | :--- | :--- |
| 1 | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | create file |
| 2 | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | mkdir |
| 3 | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | unlink |
| 4 | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | rmdir |
| 5 | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | rename, no overwrite |
| 6 | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | rename, overwrite |
| 7 | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | dirstat |
| 8 | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | lookup |
| 9 | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | listdir, hard-coded limit 10,000 |
| 10 | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | listdir page |
| 11 | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | prefix listdir (`LIKE` is this prefix form only) |
| 12 | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | setattr mode |
| 13 | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | setattr uid |
| 14 | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | setattr size and fid |
| 15 | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | setattr modify time |
| 16 | `SELECT * FROM QuotaTable WHERE uid = ?` | get quota |
| 17 | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | set quota limit (the `ON CONFLICT` value is stored) |
| 18 | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | list quotas |
| 19 | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | get secondary index |
| 20 | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | scan secondary index |

`metadb-ctl sql` sends `ExecuteSql` with an empty parameter list, so the statement must use literals.

Routing uses `CompositeMetaCache`. On a miss or an epoch mismatch the cache is refreshed from the local cell and the call is retried against the local shard group. `DurableGlobalManager::route_user_key` looks up a **local** shard handle and refuses a key owned by a registered remote cell. The gRPC metadata and transaction handlers forward that request to the stored endpoint before calling the local router. The receiver sees `x-metadb-cell-forwarded` and does not forward again. An unreachable cell fails the call. On `proxy`, `execute_sql_via_registered_cell` dials that cell with `x-metadb-cell-forwarded` when one registered cell contains the statement, and otherwise returns a placement error. Metadata calls and user-key transactions use the same rule. A missing directory, a request that is not contained by one cell, keys split across two cells, or a range that only overlaps a cell fails closed. None of those calls is sent to `[cluster] upstream` or applied locally. `CommitPrimary2pc`, `ApplyAuxBatch`, `ForwardRouter`, and `GetRoutes` may still go to `[cluster] upstream`. `AllocateIdBlock` is refused. This is not a production MetaCache of every shard.

## Storage

Each shard's state machine opens a WAL-less LSM at `<data_dir>/be/shards/<shard_id 20 digits>/lsm`. SSTables use 4 KiB prefix-compressed data blocks (restart interval 16), a Bloom filter, and a `MANIFEST` with a `CURRENT` pointer. The block cache is a 16-way LRU. Its total budget is `storage.block_cache_bytes` (default 64 MiB). Each of the 16 cache shards gets at least 16 KiB.

MVCC versions live in memory for `storage.mvcc_retention_ms` (default 5,000). A coordinated flush seals the memtable, runs the attribute-aggregation hook, and writes an L0 SSTable. There is no separate user KV WAL. Crash recovery is the Raft log plus the LSM manifest. `LocalityCompactor` tracks tombstones per 9-byte prefix bucket and compacts a hot deletion range when both `locality_tombstone_threshold` and `locality_tombstone_ratio` are crossed. `auto_locality_compaction` is always set to `true` by `ServerTomlConfig::to_lsm_config`; the TOML file cannot turn it off.

`storage.memtable_size_bytes` is the active memtable flush threshold. `storage.max_imm_memtables` (default 4) is the immutable-table backpressure cap: a writer waits until an immutable table flushes instead of queueing another one.

## Raft

Each local shard is an `openraft` 0.10 group. The log is a segmented CRC32C file under `.../shards/<id>/raft`. Defaults, loaded from `[raft]` when that table or a field is omitted:

| Field | Default |
| :--- | :--- |
| heartbeat | 150 ms |
| election timeout | 450–900 ms |
| `max_in_snapshot_log_to_keep` | 0 |
| log segment size | 64 MiB |

Log purge is gated by `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)` and by the newest complete snapshot directory. The segmented log is the write-ahead log until that purge. After purge, the LSM is the only remaining copy. Startup and the 250 ms maintenance tick rewrite a missing snapshot from the LSM. If the flushed LSM cannot cover the purged prefix, open fails closed and cluster status does not report a live voter. There is no fourth copy of the command log. The 250 ms maintenance loop calls `flush_snapshot_and_purge` when a shard still retains log entries, unless a snapshot watermark is already at least `state_flushed_index` and fewer than 64 entries remain. An operator backup hold clamps that purge, and snapshot-directory deletion, until the hold is released or its wall-clock expiry passes. See [Operations](./operations.md#backup-and-restore).

`ConfChangeOrchestrator::add_learners_and_promote_voters` adds learners, waits until replication lag is at most 64 entries (default timeout 10 s, the value `scale_out_replicas` passes), then calls `change_membership`. The in-process test `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` does this with three tonic listeners and a basic KV state machine inside one OS process. It does not start `metadbd`.

## Transactions and auxiliary work

Single-directory create, unlink, setattr, lookup, dirstat, listdir, and same-shard mkdir/rmdir are proposed to the shard leader. Parent-directory counters are not locked on the foreground path: mutations record deltas, `ReplicatedAttrAggregator` folds them at the flush barrier, and dirstat reconciles unflushed deltas on the read path.

Quota and `fid -> (pid, name)` updates go through a per-shard auxiliary outbox. `AuxDispatcher` applies them on the maintenance tick. If the outbox cannot be applied, entries can spill under prefix `0x07` so Raft compaction is not pinned forever by that queue. `TxnRecoveryWorker` scans `TxnRecord` / `WriteIntent` on the same tick.

Cross-shard rename (and mkdir/rmdir that touch two shards) uses primary-anchored 2PC (`WriteIntent` + `TxnRecord`). The initial lease is `DEFAULT_2PC_TTL_MS` (3 seconds), stored as `expire_hlc` on the primary `TxnRecord` and on every participant `WriteIntent`, including each `DirExistsFence` holder. While the coordinator that allocated `txn_id` is still preparing or committing, it replicates `Extend2pcLease` before that deadline and pushes the same `expire_hlc` forward. A renewal after expiry or after abort is rejected and does not revive the transaction. If the coordinator stops renewing, `TxnRecoveryWorker` aborts a still-`Pending` transaction once the clock passes `expire_hlc` and clears its intents. A transaction that already reached `CommitPrimary` is not aborted for expiry; recovery rolls the secondary shards forward.

Renewal cannot move `expire_hlc` past `start_hlc + MAX_2PC_LEASE_LIFETIME_MS` (60 seconds). That cap is two 30-second routing budgets, so a Prepare that is still retrying inside the routing budget can keep its lease. It is not a longer fixed TTL. The in-memory deadline moves forward only after every already-prepared participant has applied that same `expire_hlc`. A transient renew failure retries and does not mark the lease failed while the durable deadline is still in the future.

The default MVCC retention stays 5 seconds. When a lease is installed, each prepared key on that shard is captured at the transaction's `start_hlc` when that value is still provable. A later renew does not replace the capture. The capture is written in the same L0 flush under prefix `0x0D` (`[txn_id: 16B BE][user_key]`, version 1 plus CRC32C) and loaded again after restart. Collapse-flush still stores only the latest value of the user key. A point read behind `disk_flushed_max_hlc` returns that capture, or `TransactionTimeout` when no capture is provable. `Ok(None)` means the absence was proved, including a captured tombstone. It does not return a newer collapsed value. A range read behind that watermark returns `TransactionTimeout` for the whole range. An unversioned SST cannot prove which keys were absent or deleted, and one leased key does not make the rest of the range provable. That timeout is not an empty directory. The hold ends on commit or abort of that transaction only. Another holder of the same directory fence keeps its own capture. After restart, a hold is rebuilt only from a stored per-holder `start_hlc` (or from the intent record's own `start_hlc` when that holder is the record owner). A holder without a stored start is not given `expire - 60s` and its expire cannot move later. A renew that asks for a longer deadline returns a non-fatal abort of that renew only, so the original deadline stays usable. Rebuild also requires a wall-clock reading that fits the HLC, is not the 1 ms fallback, and is not behind the last applied HLC by more than the 500 ms drift bound. A failed or rolled-back clock installs no hold and does not extend the durable deadline. Recovery can still abort a pending lease when the last replicated HLC is already past `expire_hlc`, and it still rolls a committed primary forward. Orphan and undecodable `0x0D` rows are deleted by `SweepOrphanLeasePreimages` without waiting for tombstone compaction. A non-rename `CommitPrimary` still uses the proposal timestamp and has no extra wall-clock veto. A `SnapshotGuard` scan reads the SST generation pinned with the cut and refuses that scan when the pinned checkpoint HLC is newer than the cut. That is not a per-key HLC proof inside the SST. `expire_hlc` must sit within 60 seconds of both `start_hlc` and the observation; overflow fails closed. `WriteIntentRecord` and `TxnStatusRecord` stay codec version 1. An in-flight 2PC does not add a fourth copy of the command log.

A directory rename also holds one `DirRenameLease` in the ID allocator. Renewal of that fence uses the same token, refuses to extend an expired lease, and stops at the same 60-second deadline measured from the grant. A requested TTL above that cap is rejected and does not grant a lease. Participant `expire_hlc` is not moved past the fence. If the fence cannot be renewed, the coordinator aborts the rename. A second directory rename is rejected until the lease is released or that deadline passes. The granted TTL is at least 100 ms.

## Sharding inside the process

`maybe_split_or_isolate_shard` runs when approximate shard size reaches `sharding.shard_split_threshold_bytes` (default 4 GiB), or when `TriggerShardSplit` forces a check.

`RaftShardSplitWorker::discover_split_key`:

- Needs at least two live keys.
- Quota and secondary-index shards split at a byte midpoint.
- `MetaTable` sums bytes per parent id. If one parent owns more than 85% of those bytes (`dominant_bytes * 100 > total_bytes * 85`), or the shard has a single parent, the candidate is `Jumbo`.
- Otherwise it picks a parent-id boundary near half the bytes. The split key is a PID prefix, so `_attr_` and the direct children of one parent stay together.
- If the route is already `DualModeFineGrained` for that parent, the split key is a `(pid, name)` midpoint, excluding `_attr_`.

A jumbo candidate writes `ShardHealthState::JumboIsolated { dedicated_node, dominant_pid }` onto the route. That flag alone does not move files. When `jumbo_dedicated_root` is set and the dedicated node is this process, maintenance CRC-checks the copy, fsyncs `MOVE_LIVE`, then opens Raft on that directory. A restart after `MOVE_LIVE` does not open the old directory. Maintenance does not copy that directory onto another machine. `relocate_jumbo_shard_to_remote` can place the bytes on a `127.0.0.1` listener. The listener does not serve a placed-file read until `MOVE_LIVE` has been fsynced, does not open Raft, and does not call `transfer_leader`. A visible rename whose directory fsync failed is not a completed cut, and a retry does not delete the source. Before that handoff removes the shard directory, both the committed `GLOBAL_CELL_DIRECTORY` and the committed `CELL_MANAGER_STATE` on that shard must be proposed onto another still-open local shard and be readable there. A non-leader target, a propose that did not land, a matching directory with a different manager body, or a closed source that cannot move both snapshots leaves the source directory in place. A restart reads both. An empty engine is not published as a new directory. `metadbd` does not start that listener. `JumboIsolationPlanner` writes the route flag. The chosen node is the registered node with the fewest jumbo routes, then the fewest other routes. With only the bootstrap node registered, that node is this process. That flag is detection. `reconcile_jumbo_placement` then writes `CELL_CONTROL_PLANE`. On one voter it records `LeaderOnDedicated` or `Blocked` and does not change the voter set. Automatic `transfer_leader` is not called: OpenRaft disables the leader lease before the peer answers, and this process cannot prove that older peers implement `TransferLeader`. Maintenance evaluates `consider_automatic_leadership_transfer` on each loop. That gate can request a transfer only when this process is already the leader, the live voter set is the intended set, and a transfer is allowed for the group. It refuses an untrusted clock, a visible `MOVE_STAGED` or `MOVE_LIVE`, a handoff in progress, `JUMBO_REMOTE_HANDOFF`, a closed source group, and a target that is not a different live voter. A `HintLive` sample is not an input. `automatic_leadership_transfer_allowed` is false, so the loop does not request a transfer and does not call `trigger().transfer_leader`. A refusal does not change membership, delete a source directory, or invent a leader. This is not a cross-host Jumbo copy, not a 3-VM exercise, and not production acceptance. A same-size voter replacement runs only when a node that is already a voter of some local shard can take the seat without lowering Raft fault tolerance. An already-correct leader is not recorded as this process's move. `DedicatedNodeNotShared` means that membership condition holds. It does not mean shard files moved to another machine. `metadb-ctl cluster status` prints the local placement row. `GET /metrics` counts those rows by phase and does not label them with a shard id. Neither surface reports `MOVE_STAGED` or `MOVE_LIVE`. See [Scale control plane](./scale-control-plane-status.md).

`enable_dual_mode_split` persists `split_mode = DualModeFineGrained` and `dual_mode_pid`. It does not itself copy keys. A later split uses the `(pid, name)` rule. The in-memory simulator (`CellLocalManager` in `control_plane.rs`) is a different type: its jumbo path returns `UnsplittableJumboShard` and its "scale to 3 voters" only edits a `BTreeMap`. Tests `T1`–`T6` exercise that simulator.

Split plans are stored in `CELL_MANAGER_STATE`. Crash points covered by `split_crash_recovery_tests` resume or roll back a plan on the next `maybe_split_or_isolate_shard`. Child shard directories are created under the same `be/shards` tree.

## Control-plane RPCs that do less than the name suggests

| RPC | Behavior in this tree |
| :--- | :--- |
| `RegisterNode` | Inserts `(node_id, grpc_addr)` into this cell's `CELL_MANAGER_STATE` |
| `StoreHeartbeat` | For a registered node, fsyncs liveness into `CELL_CONTROL_PLANE` when the wall clock is trusted. The historical 1 ms fallback and a clock that cannot be read do not write a new row. The sender's heartbeat-file generation is required. A receiver with no row for that node accepts the current sequence, including values above 64. The same generation rejects a jump above 64, a stale sequence, and a conflicting body. A different generation is a new baseline, which is how a recreated sender file or a new leader recovers. A new sequence sooner than 1 second after the previous observation is rejected. Accepting the RPC records a monotonic receive. A successful ack records a round trip on the sender. `decide_liveness` can return `HintLive` from that measurement and returns `Refuse` when the sample is missing, expired, or the clock is untrusted. The hint is not applied: it does not call `transfer_leader`, move a Jumbo shard, change membership, or replicate the cell directory. Shard descriptors are annotations, not routes. A bound node instance must be repeated; an address-only registration has no second check. When `[security.rbac]` is absent, the shared bearer remains the only credential and can overwrite the observation. When the table is present, `StoreHeartbeat` requires an exact `metadb.v1.ControlPlaneService/StoreHeartbeat` grant and a principal that is not the shared bearer, so holding only the bearer cannot overwrite it. The maintenance loop sends this process's own record, without shard descriptors, about every 3 seconds of monotonic time |
| `GetRoutes` | `cell_id = 0` lists every cached route and this process's local placement rows. A remote cell contributes routes and no placement rows. `automatic_leadership_transfer_enabled` is this process and is false. An unknown non-zero `cell_id` returns an error instead of an empty shard list |
| `AllocateIdBlock` | `global-manager` grants from `SHARED_ID_ALLOCATOR`. A `cell-manager` with `cluster.global_manager` forwards the RPC there and does not mint locally when that process is down. Other roles that may mint still use the local durable allocator. `be` and `proxy` refuse the RPC |
| `AllocSplitPlan` / `ActivateSplit` | Forwards to the local cell manager |
| `TriggerShardSplit` | Forces `maybe_split_or_isolate_shard` on a local shard. A jumbo candidate stores a placement row and returns no child, so `split_executed` stays false. The response can still carry that row |
| `EnableDualModeSplit` | Sets the local route flag. `split_name` in the request is ignored |
| `ScaleShardReplicas` | Calls `scale_out_replicas` if this cell exists and enough nodes are registered |
| `RegisterCell` | Stores a remote cell after its routes cover the requested range. A later metadata or transaction request contained by that cell is forwarded to its endpoint. A bootstrap leader also proposes that directory and a `CELL_MANAGER_STATE` snapshot into the lowest local shard log. A `global-manager` with no shard group keeps only the local file |
