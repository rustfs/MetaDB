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

## Metrics

Set `node.metrics_listen_addr` (for example `127.0.0.1:9100`). `open_and_serve` accepts TCP connections on that address. `GET /metrics` returns Prometheus text. Any other path returns 404, and any other method returns 405. When `[security].shared_secret` is non-empty, every request on this listener must send `Authorization: Bearer <shared_secret>`. A missing, empty, or wrong bearer returns HTTP 401 and a body that is not the metric text, including a request for `/status`. A non-loopback metrics address with an empty secret refuses to start. This listener does not terminate TLS.

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

There is no `metadb_raft_watermark_lag_entries` series. `metadb_cells_total` counts cells registered inside this process, which `open_and_serve` sets to one.

## Scale-out

What works inside one process:

- More shards. Growth past `shard_split_threshold_bytes`, or `metadb-ctl shard split`, can add shard groups under the same `be/shards` directory. The parent keeps a left key range and the child boots as another 1-voter group on this node. Restart reopens both.
- Dual-mode flag. `shard enable-dual-mode` marks one parent id. A later split of that shard may cut on `(pid, name)`.
- Jumbo flag. When one parent is more than 85% of `MetaTable` bytes on the shard, or the shard has a single parent, the route becomes `JumboIsolated`. The dedicated node id is chosen from nodes already registered in this cell. With one node, the flag names this process. No files are moved to another host.
- In-process voters. `DurableCellManager::scale_out_replicas` can add learners and promote them when those node ids are registered **and** each address already serves `RaftTransportService` for that shard's group. `crates/metadb-raft/tests/multi_raft_failover_tests.rs` does this with three listeners in one test process and a basic KV state machine, not the metadata state machine, and not three `metadbd` processes.

What does not work as an operator procedure:

1. `metadbd --role be`, `--role proxy`, `--role cell-manager`, and `--role global-manager` exit in `open_and_serve`. The subsystems are not trimmed by role, and the process refuses to start them under those names.
2. A second `metadbd --role all-in-one` joins an existing group when `--join` or `--cell-manager` names the current leader and the data directory is fresh. The leader appends a blank entry, snapshots, and promotes the joiner from learner to voter on the shards it already hosts. `--join` must reach that leader. A directory that already has Raft state reopens it and ignores a new join address. Each process still has its own id allocator, so automatic inode and file ids issued on the joiner can collide with ids the first process already issued. `Lookup` on a follower returns `NotLeader` and is not forwarded.
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
| `EpochMismatch` / `NotLeader` | Stale route epoch, or a propose sent to a non-leader |
| `UnsplittableJumboShard` | In-memory splitter (`shard.rs`), not the durable jumbo path, which sets `JumboIsolated` and returns success with no child shard |
| `WriteIntentConflict` / `TransactionAborted` | 2PC conflict, or a directory-rename lease already held |
| `InvalidRenameCycle` | Rename of a directory into itself or a descendant |
| `ConsensusError` | Not enough registered nodes, learner catch-up timeout (10 s), or OpenRaft membership error |
| `SqlParseError` | Statement is not one of the 20 templates |

`CodecError` is also the type used for the unimplemented-role rejection and for bad TOML.

## Known limitations

- Role `all-in-one` only. `be`, `proxy`, `cell-manager`, and `global-manager` are rejected before listen.
- A second all-in-one process can join the same cell with `[cluster] join`. It does not create a second cell. `RegisterCell` returns an error.
- `GetRoutes` fails closed for an unknown non-zero `cell_id`. `StoreHeartbeat` does not replace cached routes.
- User names `.`, `..`, `/`, `\`, `_attr_`, empty, and NUL are rejected. A negative file size is rejected. A directory rename into its own descendant fails closed, including chains deeper than 1024.
- The HLC saturates at the 48-bit physical ceiling instead of wrapping the logical counter. `Prepare2pc` follows `header.shard_id`, not the primary-key shard.
- Quota cursor rows are written after the outbox lock is released, so a flush that already holds the memtable lock can still take the outbox lock.
- Jumbo isolation is a route flag plus a node id chosen from the local node map. It does not evict other shards from a disk.
- Heartbeats are an RPC with no sender in `metadbd`. `StoreHeartbeat` does not persist.
- A non-empty shared secret authenticates Raft, the other three gRPC services, and the metrics listener with the same bearer. An empty secret is accepted only for loopback binds and leaves those listeners unauthenticated. A wildcard bind with an empty secret refuses to start. The process does not terminate TLS.
- `memtable_size_bytes` seals and flushes the active memtable. `max_imm_memtables` caps queued immutable tables. Raft timer fields do not change runtime behavior through TOML.
- `metadb-ctl bench` for `hotspot-create` and `bulk-ttl-delete` prints JSON lines: a header with `bench_pid`, then phase reports with attempted, succeeded, failed, and latency percentiles. The process exits non-zero unless every requested operation succeeds. `--workload mixed` or `--embedded` runs the in-process stress harness. `sql` prints the response as JSON and exits non-zero on an application error.

## Documentation gaps

These items are missing from the product, so this guide cannot describe a procedure for them:

1. A multi-process layout for `be`, `proxy`, `cell-manager`, and `global-manager`.
2. A second Cell. `RegisterCell` stays an error. Join reaches only the current leader; a follower does not forward it.
3. Placing a jumbo shard on its own machine and moving other shards off that machine.
4. Native TLS. This process does not terminate TLS. Config keys that ask for TLS or a certificate are rejected instead of serving plaintext. Put termination in an external proxy. Raft and metrics use the same bearer as the other services when a secret is set; that bearer is not mutual TLS. A heartbeat-driven control loop is not implemented.
5. A memtable size or Raft timer that operators can set and observe.
6. A checked-in service-level result for `metadb-ctl bench`. The command prints numbers for the machine you run it on. This repository does not record one. See [Tests and benches](./testing.md).
