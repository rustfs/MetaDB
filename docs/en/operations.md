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

Calls `RegisterCell`. The server drops the body and returns success. The command prints `Registered Cell <id>` even though no cell was stored. Do not use this as a federation step.

### `sql <statement>`

Calls `ExecuteSql` with no bind parameters. The statement must be one of the 20 templates in [Architecture](./architecture.md), with literals instead of `?`.

```bash
metadb-ctl --endpoint 127.0.0.1:7800 sql \
  "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
```

### `bench`

See [Tests and benches](./testing.md#metadb-ctl-bench). This is a client against a running server. It is not a Cargo benchmark target. The repository does not check in a throughput number for it.

## Metrics

Set `node.metrics_listen_addr` (for example `127.0.0.1:9100`). `metadbd` accepts TCP connections and writes a single HTTP `200` whose body is Prometheus text. It does not route on the URL path. Any request receives the same body.

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
2. A second `metadbd --role all-in-one` with `--cell-manager <first>` still bootstraps its own cell and its own shards 1–3, then calls `RegisterNode`. Registration records an address. It does not copy the first process's log or make the second process a learner. `ScaleShardReplicas` will try `add_learner` against that address and time out or fail unless that process is already hosting the same shard groups, which the all-in-one bootstrap does not do.
3. `metadb-ctl cell register` does not create a second cell. `open_and_serve` registers only the local cell on the global manager.
4. The in-memory `MetaDbEngine::new_single_node` path used by tests `T1`–`T6` can bump an in-memory voter count to 3. That object has no disk and no Raft. It is not a scale-out of `metadbd`.

[Open pull request #2](https://github.com/rustfs/MetaDB/pull/2) (not merged) states the same boundary: its distributed test calls `DurableCellManager::scale_out_replicas` with three localhost Raft voters inside one OS process. It does not launch three `metadbd` processes. Disaggregated roles and a second Cell are not tested as live deployments there either. Jumbo dedicated-node isolation in that pull request remains the in-memory simulator behavior.

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

- One OS process, role `all-in-one` only.
- No join protocol. A second data directory is a second cluster.
- `RegisterCell` is a no-op success.
- Jumbo isolation is a route flag plus a node id chosen from the local node map. It does not evict other shards from a disk.
- Heartbeats are an RPC with no sender in `metadbd`. `StoreHeartbeat` does not persist.
- Raft on the public gRPC port is unauthenticated. The process does not terminate TLS.
- `memtable_size_bytes` and all Raft timer fields do not change runtime behavior through TOML.
- `metadb-ctl bench` reports throughput only. It does not check listing, dirstat, or quota after the run, and it does not print latency percentiles.

## Documentation gaps

These items are missing from the product, so this guide cannot describe a procedure for them:

1. A multi-process layout for `be`, `proxy`, `cell-manager`, and `global-manager`.
2. Adding a second and third `metadbd` OS process to an existing Raft group.
3. Creating and routing to a second Cell.
4. Placing a jumbo shard on its own machine and moving other shards off that machine.
5. TLS material, Raft authentication, and a heartbeat-driven control loop.
6. A memtable size or Raft timer that operators can set and observe.
7. A checked-in service-level result for `metadb-ctl bench`. The command prints numbers for the machine you run it on. This repository does not record one. The unmerged pull request #2 reports a separate embedded harness; those commands are not in this tree. See [Tests and benches](./testing.md).
