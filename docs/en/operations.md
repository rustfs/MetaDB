# Operations

[Overview](./README.md) · [Français](../fr/operations.md) · [Deutsch](../de/operations.md) · [日本語](../ja/operations.md) · [简体中文](../zh/operations.md)

## Data directory

After a bootstrap with `bootstrap_table_shards = true`:

```text
<data_dir>/
  global_allocator/ID_ALLOCATOR_STATE
  cell_<cell_id>/CELL_MANAGER_STATE
  be/NODE_SHARD_REGISTRY
  be/shards/00000000000000000001/lsm/     # MetaTable
  be/shards/00000000000000000001/raft/
  be/shards/00000000000000000002/...      # QuotaTable
  be/shards/00000000000000000003/...      # SecondaryIndexTable
```

Shard ids are decimal, zero-padded to 20 digits. Child shards created by a split appear as further directories under `be/shards/`. `CELL_MANAGER_STATE` and `ID_ALLOCATOR_STATE` are CRC32C frames. Do not delete `NODE_SHARD_REGISTRY` while shard directories remain; the host uses it to recover split children.

Stop the process with Ctrl-C (or `PersistentMetaDbCluster::shutdown`) before copying the directory. A live process holds an LSM `LOCK` file per shard.

## `metadb-ctl`

Global flags:

| Flag | Default | Meaning |
| :--- | :--- | :--- |
| `--endpoint` | `127.0.0.1:7800` | `host:port`, or a URL that already starts with `http://` or `https://` |
| `--auth-token` | empty | Bearer token. Also read from `METADB_AUTH_TOKEN` |

The client opens a new gRPC channel per command. It does not speak `RaftTransportService`.

### `cluster status`

Calls `GetRoutes` with `cell_id = 0`. For each shard it prints `shard`, `cell`, `leader`, voter count, `epoch=(conf_ver, ver)`, `dual_mode`, and `jumbo`. It does not print disk free space, QPS, or Raft watermarks.

### `shard list --cell-id <u64>`

Calls `GetRoutes` for that cell (use `0` for every cached route). Prints `shard`, `cell`, raw start/end key bytes, and epoch `(conf_ver, version)`.

### `shard split --shard-id <u64>`

Calls `TriggerShardSplit`, which forces a split check even when the shard is under `shard_split_threshold_bytes`. Prints `split_executed` and `child_shard_id`. `split_executed=false` means the worker found no boundary (for example fewer than two keys, or a jumbo candidate that only flips the route flag).

### `shard enable-dual-mode --shard-id <u64> --pid <u64>`

Calls `EnableDualModeSplit`. The CLI sends an empty `split_name`. The server stores `dual_mode_pid` and does not split immediately.

### `raft scale-replicas --cell-id <u64> --target-voters <u32>`

Calls `ScaleShardReplicas`. The server requires `target_voters` entries already present in that cell's node map. A fresh all-in-one cell has one node, so `--target-voters 3` returns a consensus error of the form `need 3 registered nodes in cell <id>, have 1`. On success it prints how many local shards were passed to the membership change.

### `cell register --cell-id <u64> --endpoints <a,b,c>`

Calls `RegisterCell`. The server returns an application error and does not add a cell. `metadb-ctl` exits non-zero and does not print `Registered Cell`. Do not use this as a federation step.

### `sql <statement>`

Calls `ExecuteSql` with no bind parameters. The statement must be one of the 20 templates in [Architecture](./architecture.md), with literals instead of `?`.

```bash
metadb-ctl --endpoint 127.0.0.1:7800 sql \
  "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
```

### `bench`

See [Tests and benches](./testing.md#metadb-ctl-bench). This is a client against a running server. It is not a Cargo benchmark target. The repository does not check in a throughput number for it.

The separate prefix-load harness (100 prefixes × 10,000 creates, then 100,000 `SetAttr` overwrites, then 100,000 deletes, concurrency 64) is documented in [Tests and benches](./testing.md#prefix-load). GitHub Actions runs only the small profile. It does not run the 1,000,000-create profile.

## Metrics

Set `node.metrics_listen_addr` (for example `127.0.0.1:9100`). `open_and_serve` accepts TCP connections on that address before join finishes, so a probe can observe `starting`. `GET /metrics` returns Prometheus text even when the process is not ready. `GET /livez` is liveness. `GET /readyz` and `GET /healthz` are readiness: HTTP 200 only when startup has finished, shutdown has not started, and every local shard has recovered its committed prefix, applied a log entry, is a voter, and knows a leader. A bound socket is not enough. Any other path returns 404, and any other method returns 405. When `[security].shared_secret` contains a non-whitespace character, every request on this listener must send `Authorization: Bearer <shared_secret>`. A missing, empty, or wrong bearer returns HTTP 401 and a body that is not the metric text, including a request for `/status` or `/readyz`. A whitespace-only secret is rejected before that compare, so a bearer that repeats the spaces is still 401. An empty secret refuses to start on every address, including loopback, unless `allow_unauthenticated` is set. That flag still refuses a non-loopback metrics address. A whitespace-only secret refuses to start even when the flag is set. This listener does not terminate TLS. The exact reason tokens and the checks that are intentionally omitted are in [Operability](./operability.md).

Gauges and counters from `render_prometheus_metrics`:

| Name | Kind |
| :--- | :--- |
| `metadb_cells_total` | gauge |
| `metadb_shards_total` | gauge |
| `metadb_in_memory_mvcc_writes_total` | counter |
| `metadb_pruned_mvcc_versions_total` | counter |
| `metadb_persistent_kv_writes_total` | counter |
| `metadb_active_physical_tombstones` | gauge |
| `metadb_tombstones_traversed_on_scans_total` | counter |
| `metadb_locality_compactions_total` | counter |
| `metadb_min_raft_safe_purge_index` | gauge |
| `metadb_ready` | gauge, 1 only when `/readyz` is 200 |
| `metadb_live` | gauge, 1 when `/livez` is 200 |
| `metadb_process_phase` | gauge, 0 starting, 1 serving, 2 shutting down, 3 failed |
| `metadb_shards_leader_known` | gauge, local groups with a known leader |
| `metadb_shards_leader_unknown` | gauge |
| `metadb_shards_recovery_pending` | gauge |
| `metadb_shards_log_not_applied` | gauge |
| `metadb_shards_not_voter` | gauge |
| `metadb_shards_storage_unrecoverable` | gauge |
| `metadb_shards_raft_stopped` | gauge |
| `metadb_shards_raft_fatal` | gauge |
| `metadb_raft_heartbeat_interval_ms` | gauge, configured value |
| `metadb_raft_election_timeout_min_ms` | gauge, configured value |
| `metadb_raft_election_timeout_max_ms` | gauge, configured value |
| `metadb_raft_max_in_snapshot_log_to_keep` | gauge, configured value |
| `metadb_raft_max_segment_bytes` | gauge, configured value |

There is no `metadb_raft_watermark_lag_entries` series and no per-shard label. `metadb_cells_total` counts cells registered inside this process, which `open_and_serve` sets to one. `metadb_process_phase 1` does not by itself mean `metadb_ready 1`.

## Scale-out

What works inside one process:

- More shards. Growth past `shard_split_threshold_bytes`, or `metadb-ctl shard split`, can add shard groups under the same `be/shards` directory. The parent keeps a left key range and the child boots as another 1-voter group on this node. Restart reopens both.
- Dual-mode flag. `shard enable-dual-mode` marks one parent id. A later split of that shard may cut on `(pid, name)`.
- Jumbo flag. When one parent is more than 85% of `MetaTable` bytes on the shard, or the shard has a single parent, the route becomes `JumboIsolated`. The dedicated node id is chosen from nodes already registered in this cell. With one node, the flag names this process. No files are moved to another host. A placement record in `CELL_CONTROL_PLANE` says whether that node is already a voter and whether peer shards still list it. See [Scale control plane](./scale-control-plane-status.md).
- In-process voters. `DurableCellManager::scale_out_replicas` can add learners and promote them when those node ids are registered **and** each address already serves `RaftTransportService` for that shard's group. `crates/metadb-raft/tests/multi_raft_failover_tests.rs` does this with three listeners in one test process and a basic KV state machine, not the metadata state machine, and not three `metadbd` processes.

What does not work as an operator procedure:

1. `metadbd --role be`, `--role proxy`, `--role cell-manager`, and `--role global-manager` exit in `open_and_serve`. The subsystems are not trimmed by role, and the process refuses to start them under those names.
2. A second `metadbd --role all-in-one` joins an existing group when `--join` or `--cell-manager` names the current leader and the data directory is fresh. The leader appends a blank entry, snapshots, and promotes the joiner from learner to voter on the shards it already hosts. `--join` must reach that leader. A directory that already has Raft state reopens it and ignores a new join address. A fresh allocator for node ids 2 through 32767 starts in that node's inode, file-id, and shard-id namespace, so it does not reissue ids node 1 has already granted. An allocator file that has already granted low ids is not relocated. `Lookup`, directory listing, directory stat, `GetDirPartitionStat`, quota reads, and secondary-index reads that reach a follower with a known leader are forwarded to that leader. A follower that does not know a leader still returns `NotLeader`. While membership is joint, `GetRoutes` reports every applied voter, the current leader, and the applied epoch. After the uniform config commits, those voters and `conf_ver` match Raft. A join that cannot finish retries transient errors for 45 seconds, then shuts the listeners and local Raft groups down and returns the error. `metadbd` ends because startup returned that error. The first failed attempt does not abort the process by itself.
3. `metadb-ctl cell register` does not create a second cell. `RegisterCell` fails closed. `open_and_serve` registers only the local cell on the global manager.
4. The in-memory `MetaDbEngine::new_single_node` path used by tests `T1`–`T6` can bump an in-memory voter count to 3. That object has no disk and no Raft. It is not a scale-out of `metadbd`.

`topology_distributed` still uses `DurableCellManager::scale_out_replicas` with localhost voters inside one OS process. `join_existing_group` starts real `metadbd` processes and joins them. Disaggregated roles and a second Cell are still not live deployments. Jumbo dedicated-node isolation remains a route flag, not a move of shards onto another machine.

## Errors you can hit

Variants are defined in `crates/metadb-core/src/error.rs`. gRPC methods usually return the variant inside the response `error` field rather than as a non-OK status.

| Variant | When the code raises it |
| :--- | :--- |
| `ReservedEntryName` | Empty name, NUL, or `_attr_` used as a user entry |
| `AlreadyExists` / `NotFound` | Create or lookup of `(pid, name)` |
| `DirectoryNotEmpty` | `rmdir` while children remain, including before attribute flush |
| `InvalidEntryType` | `rmdir` on a file, or `unlink` on a directory |
| `TransactionTimeout` | Snapshot HLC older than the in-memory MVCC window |
| `ClockSkewExceeded` | Remote HLC ahead of local wall time by more than `max_clock_drift_ms` |
| `EpochMismatch` / `NotLeader` | Stale route epoch, or this replica is not the leader. Follower reads with a known leader are forwarded. `EpochMismatch` on a multi-key prepare aborts that round and regroups; it is not proposed again as the same batch |
| `RetryableConsensus` | A write's log entry was discarded on a leader change, or a quorum did not answer. Same-shard writes and 2PC retry an enveloped proposal inside the existing routing budget, reusing one proposal id. If that id already committed, the shard returns the original result and does not execute the command again. A receipt is removed once a later committed HLC is more than 10 minutes ahead of the receipt, which is longer than the 30-second write retry budget. Each command cleans a bounded batch of expired receipts, not the whole set. A receipt written before it carried a timestamp is stamped on a later commit and then ages out on the same window. A payload without that envelope is applied at most once: a discarded-log retry of those bytes, including one returned by a forwarded leader, returns `ConsensusError` and is not attempted again. A read that misses a quorum is retried only until the 2-second election budget, then returned. Storage failure and a stopped Raft node stay `ConsensusError` and are not retried |
| `UnsplittableJumboShard` | In-memory splitter (`shard.rs`), not the durable jumbo path, which sets `JumboIsolated` and returns success with no child shard |
| `WriteIntentConflict` / `TransactionAborted` | 2PC conflict, or a directory-rename lease already held |
| `InvalidRenameCycle` | Rename of a directory into itself or a descendant |
| `ConsensusError` | Not enough registered nodes, learner catch-up timeout (10 s), a membership change that cannot be planned yet, or a Raft storage or stopped-node failure |
| `SqlParseError` | Statement is not one of the 20 templates |

`CodecError` is also the type used for the unimplemented-role rejection and for bad TOML.

## Known limitations

- Role `all-in-one` only. `be`, `proxy`, `cell-manager`, and `global-manager` are rejected before listen.
- A second all-in-one process can join the same cell with `[cluster] join`. It does not create a second cell. `RegisterCell` returns an error.
- `GetRoutes` fails closed for an unknown non-zero `cell_id`. `StoreHeartbeat` does not replace cached routes. A registered node's heartbeat is fsynced in `CELL_CONTROL_PLANE`. Shard ids inside that RPC stay annotations.
- User names `.`, `..`, `/`, `\`, `_attr_`, empty, and NUL are rejected. A negative file size is rejected. A directory rename into its own descendant fails closed, including chains deeper than 1024.
- The HLC saturates at the 48-bit physical ceiling instead of wrapping the logical counter. `Prepare2pc` follows `header.shard_id`, not the primary-key shard.
- Quota cursor rows are written after the outbox lock is released, so a flush that already holds the memtable lock can still take the outbox lock.
- Jumbo isolation is still not a move of shard files onto a dedicated disk. The route flag names a node. Automatic leadership transfer is disabled. `CELL_CONTROL_PLANE` records a same-size voter replacement only after this process observes the planned voter set. An already-correct leader is not listed as this process's move. A one-voter cell does not change its voter set.
- Heartbeats are a local liveness observation, not a shard epoch and not a split or membership input. `metadbd` persists its own about every 3 seconds and sends it to the current leader when this process is not the leader. The sender does not include shard descriptors. Within one observation generation a remote sequence may advance by at most 64. Any accepted remote heartbeat that changes the stored row, including a same-sequence generation replacement, waits 1 second. An identical replay does not write. A receiver with no row, or a new observation generation after the sender's control file is recreated, accepts the sender's current sequence. A delayed older generation is not remembered; after the rate window it is just another replacement. The shared bearer can still overwrite that observation. That record still does not schedule placement. The shared bearer remains the only credential.
- A secret that contains a non-whitespace character authenticates Raft, the other three gRPC services, and the metrics listener, including `/livez`, `/readyz`, and `/healthz`, with the same bearer. An empty secret does not start the process. `allow_unauthenticated = true` is the explicit exception for literal loopback IPs (`127.0.0.0/8` or `::1`) and still refuses a wildcard bind. The hostname `localhost` is not accepted for that exception. The bound socket is checked again after listen. A whitespace-only secret is rejected and is not compared as a bearer. The process does not terminate TLS.
- `memtable_size_bytes` seals and flushes the active memtable. `max_imm_memtables` caps queued immutable tables. `[raft]` timers and `max_segment_bytes` apply when a group starts. They are not reloaded from a changed file, and the segment size does not rewrite segments already on disk.
- `metadb-ctl bench` for `hotspot-create` and `bulk-ttl-delete` prints JSON lines: a header with `bench_pid`, then phase reports with attempted, succeeded, failed, and latency percentiles. The process exits non-zero unless every requested operation succeeds. `--workload mixed` or `--embedded` runs the in-process stress harness. `sql` prints the response as JSON and exits non-zero on an application error.

## Backup and restore

This is a single-node operator path for one data directory. It is not a multi-Cell or Jumbo backup, and it is not a production backup system. There is no independent Raft WAL. If the segmented log, the complete snapshot, and the flushed LSM image are all missing, committed keys are not reconstructed. A passing test run does not make this path a first-ship pass.

The restorable image is the newest flushed LSM checkpoint, or the complete snapshot the hold protects when that checkpoint does not cover the purged log prefix, plus the segmented Raft log that is still on disk. The cell directory's `CLUSTER_IDENTITY` is copied with that image so the restored process keeps the same node instance id. Keys that are only in an unflushed memtable are restored by replaying that log. The copy does not rename or truncate files in the live data directory.

### Hold lifetime

`BACKUP_HOLD` in the data directory is the hold. `metadb-ctl backup hold` writes it as pending, which freezes purge and snapshot deletion, then latches a floor per shard:

- `max_purge_index` is the log prefix already purged. Purge cannot move past it.
- `retain_snapshots_through` is the newest complete snapshot index. Snapshot directories at or below that index are not replaced or deleted. A pending hold protects every snapshot index until the floor is latched.

Calling hold again while one is active does not move those floors or extend the deadline. `--ttl-secs` is a wall-clock lifetime from 1 second through 86400 seconds (the default is 900). The next purge or snapshot publication treats `now >= expires_unix_ms` as no hold and deletes the file. `metadb-ctl backup release` deletes the file immediately. A crash leaves the file in place until that deadline; it does not pin purge for the life of the process. Expiry follows the system clock. A clock step can shorten or lengthen the remaining time, but the 24 hour cap is the longest hold the command will install.

A purge that has already chosen its cutoff before the hold file is visible can finish. `metadb-ctl` waits until the running process has latched floors before it returns, and an in-process hold is pending before it reads those floors. After the command prints `phase=active`, later purge checks honor the floor.

### Commands

Stop is not required. If a shard `lsm/LOCK` is held, the command asks the running `metadbd` (its 250 ms maintenance loop) to latch the hold and to copy the log under the log lock. `--output` is resolved to an absolute path in the client before that request is written, so the server does not interpret it against its own working directory. If the process is stopped, the same commands read the directory directly. Do not start `metadbd` while a stopped-directory command is running. The command refuses an existing staging directory instead of deleting it. Releasing the hold lets the normal 250 ms loop purge again even when it does not build a new snapshot.

```bash
metadb-ctl backup hold --data-dir /var/lib/metadb --ttl-secs 900
metadb-ctl backup create --data-dir /var/lib/metadb --output /var/backups/metadb-2026-11-19
metadb-ctl backup release --data-dir /var/lib/metadb

# The destination path must not exist. An incomplete artifact is rejected and is not opened.
metadb-ctl backup restore --backup /var/backups/metadb-2026-11-19 --data-dir /var/lib/metadb-restored
metadbd --config restored.toml
```

Publication holds `be/SHARD_SET.lock` from the final shard-directory and registry check through the artifact rename. Shard directory creation, split publication, registry updates, and cell-state writes take that same lock, so a shard that becomes visible in that window cannot be omitted from a backup that still reports success. A directory or registry entry that appears before the locked check fails the backup instead. Offline backup reads each source `MANIFEST` without opening it for append. A crash scratch directory named exactly `.backup-ckpt-` plus a 20-digit shard id is removed before the next online checkpoint on that shard. The cleanup, checkpoint, byte copy, and removal share `.backup-ckpt-<shard>.lock`, so one backup cannot delete another backup's live scratch. A symlink or any other name at that scratch path is left in place.

Backup and restore anchor at the data directory, the artifact directory, or the staging directory the command created. `be`, `shards`, the shard directory, `lsm`, `raft`, and the files under them are one relative component each. That name is checked before the open. The open does not follow a symlink: `openat` with `O_NOFOLLOW` on Unix, and `CreateFile` relative to the directory handle with `FILE_FLAG_OPEN_REPARSE_POINT` on Windows (`cap-primitives` 4.0.3, `FollowSymlinks::No`). A Windows reparse point that is not reported as a symlink is rejected from its file attributes before its bytes are read. `CURRENT` must name one file in that directory. An absolute path, `..`, or a name containing `:` is rejected before that file is opened. Publish checks the staged registry, Raft log, LSM, and each `cell_*/CELL_MANAGER_STATE` through the same directory handle. The shard-reference check sees those bytes only. It does not open the staging path again. The live log copy uses that anchor while the log lock is held and after the active segment is synced. A symlink destination, a symlink temporary manifest, or a symlink scratch directory fails the command and is not truncated. Scratch cleanup deletes children through the opened directory. The final removal is still by name, so a different empty directory that replaces the scratch path after the identity check can be removed. Ancestors of the path the operator passed are still resolved. A concurrent writer can still replace a regular file. A hard link already inside the anchor is copied as that file's bytes.

On Unix, directory listing uses the opened directory descriptor. On Windows, `cap-primitives` 4.0.3 turns that handle back into a path and lists it with `std::fs::read_dir`. Child opens still use the handle and still reject reparse points, so a swapped listing cannot by itself supply the bytes that are copied. This repository was compiled on Linux only. macOS and Windows were not built here, so this is not a verified port of those targets.

`backup create` fails when no active hold is installed, when a shard is not listed on that hold, when the log is purged past the hold floor, or when neither the flushed LSM nor a protected complete snapshot covers the purged prefix. The artifact contains `BACKUP_MANIFEST` and `tree/`. Restore checks the manifest version, every file length and CRC32C, the Raft log (a torn tail is rejected rather than repaired), and that each shard LSM checkpoint matches the manifest and covers the purged index. It does not invent keys.

After restore, open the new directory with the same node id and cell id. Look up and list the keys that had been committed. Keys that were never flushed and were not in the copied log are not recovered.

### What this path does not do

- It does not add a Raft WAL beside the segmented log.
- It does not rebuild a missing LSM from nothing when the log and snapshot are gone.
- It does not take a consistent cut across Cells, Jumbo moves, or a multi-process replica set. A peer `install_snapshot` during the copy is outside this procedure.
- It does not stream large SSTables; each file is read into memory while it is copied.
- It does not claim to be race-free against a concurrent writer on the same machine. Below the anchored directory, symlink traversal is rejected. A regular file can still change after it is listed and before its bytes are read. A hard link that already lives inside the anchored directory is copied as that file's bytes. Hold-floor calculation and ordinary snapshot publication still open raft paths directly; those paths are outside the backup copy walk.
- It does not claim a verified macOS or Windows build. The nofollow open is compiled for those targets. They were not built in this change. Windows directory listing in `cap-primitives` 4.0.3 is path-based.
- It does not atomically `rmdir` the scratch directory by inode. After the opened directory is emptied, a different empty directory swapped onto that name can still be removed.
- It does not keep purge frozen after the deadline if the operator never returns. Release or wait for expiry before expecting log cleanup to resume.

## Documentation gaps

These items are missing from the product, so this guide cannot describe a procedure for them:

1. A multi-process layout for `be`, `proxy`, `cell-manager`, and `global-manager`.
2. A second Cell. `RegisterCell` stays an error. Join reaches only the current leader; a follower does not forward it.
3. Placing a jumbo shard on its own machine and moving other shard files off that disk. Automatic leadership transfer is disabled because OpenRaft drops the leader lease before peers answer, and older peers do not implement `TransferLeader`. A same-size voter replacement keeps fault tolerance and only adds a node that is already a voter. A node that has not joined a shard is not added.
4. Native TLS. This process does not terminate TLS. Config keys that ask for TLS or a certificate are rejected instead of serving plaintext. Put termination in an external proxy. Raft and metrics use the same bearer as the other services when a secret is set; that bearer is not mutual TLS. The 3-second heartbeat records liveness. It does not drive splits, cell federation, or disk drain.
5. A gRPC health RPC, probes when `metrics_listen_addr` is unset, and per-shard Prometheus series. `/readyz` does not wait for the 250 ms 2PC recovery pass. A leader election makes `/readyz` and `/healthz` return 503 on replicas that do not know a leader. `[raft]` is loaded and is not `#[serde(skip)]`. Its values apply when a group starts and are not reloaded. See [Operability](./operability.md).
6. A checked-in service-level result for `metadb-ctl bench`. The command prints numbers for the machine you run it on. This repository does not record one. See [Tests and benches](./testing.md).
