# Operations, `metadb-ctl` CLI & Performance Tuning (English)

* **Back to Guide Index**: [English Overview](./README.md) | [Hub](../../README.md)

---

## 1. `metadb-ctl` Administration CLI `[Implemented]`

`[Implemented]` means the `metadb-ctl` binary exists and talks to an all-in-one process. `cell register` does not create a cell. `metadbd --role be` is not a supported deployment. Output from a bench command is for the machine that ran it, not a checked-in result.

`metadb-ctl` connects to gRPC services to inspect local cluster state, manage shards, execute SQL templates, and run synthetic benchmarks. SQL results are complete JSON responses; command failures exit nonzero. Benchmark JSONL reports an allocated `bench_pid`, exact attempts, successes, failures, and latency statistics. See [upgrade and compatibility notes](../../designs/correctness_hardening.md).

```bash
# Inspect Cells, BE node health, shard counts, and 3-watermark Raft log lag
metadb-ctl --endpoint 127.0.0.1:7800 cluster status

# List shards with key ranges, ShardEpoch, Raft roles, and Jumbo isolation state
metadb-ctl --endpoint 127.0.0.1:7800 shard list --cell-id 1

# Manually trigger a PID-boundary split check on shard 12
metadb-ctl --endpoint 127.0.0.1:7800 shard split --shard-id 12

# Enable DualModeFineGrained (PID, name) splitting on an extreme Jumbo directory
metadb-ctl --endpoint 127.0.0.1:7800 shard enable-dual-mode --shard-id 42 --pid 900100

# Scale shard replication factor online (1-Voter -> Learner -> 3-Voter)
metadb-ctl --endpoint 127.0.0.1:7800 raft scale-replicas --cell-id 1 --target-voters 3

# Execute a parameterized metadata SQL template from the command line
metadb-ctl --endpoint 127.0.0.1:7800 sql "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"

# Run single-directory hotspot write & bulk-TTL-delete listdir benchmarks
metadb-ctl --endpoint 127.0.0.1:7800 bench --workload hotspot-create --concurrency 64 --ops 100000

# In-process mixed workload (create, lookup, dirstat, listdir, rename, unlink).
# Prints ops, error counts, latency percentiles, and checks directory listings plus quota.
# `smoke` is 5s and ignores METADB_STRESS_*. `local` defaults to 30s and reads:
#   METADB_STRESS_DURATION_SECS, METADB_STRESS_CONCURRENCY, METADB_STRESS_KEYSPACE, METADB_STRESS_SEED
metadb-ctl bench --embedded --profile smoke
metadb-ctl bench --embedded --profile local --duration-secs 120 --concurrency 16 --keyspace 32
```

The same harness is `metadb_server::stress`. `cargo test --workspace` runs the smoke profile. The longer local profile is:

```bash
cargo test -p metadb-server --test stress_harness stress_local -- --ignored --nocapture

# Unflushed restart overlapping in-flight creates, then a rename/unlink tail.
cargo test -p metadb-server --test stress_crash_restart -- --nocapture

# 3-voter leader stop under creates, then voter restart while creates continue.
cargo test -p metadb-server --test topology_distributed three_voter_leader_loss_under_load -- --nocapture
```

---

## 2. Managing Skewed Workloads: Jumbo Shards & Dual-Mode Splitting

1. **Automatic Dedicated-Node Isolation (`JumboIsolated`)**:
   * When a single directory (`PID`) exceeds `shard_split_threshold_bytes` (`4 GiB` default) and represents `>80%` of shard bytes, `PolicyShardSplitter` raises `UnsplittableJumboShard`.
   * `CellLocalManager` records `JumboIsolated` scheduling metadata. Physical migration to another daemon is not implemented.
2. **Dual-Mode `(PID, name)` Splitting (`DualModeFineGrained`)**:
   * Only if a single directory grows beyond a dedicated host's capacity (e.g., `>500M` files / `>100 GiB`), enable `DualModeFineGrained` via `metadb-ctl shard enable-dual-mode`.
   * Non-anchor sub-shards maintain shard-local `[0x04] DirShardDelta` counters: `create_file` and `unlink` remain `1-RTT` single-shard operations, while `dirstat` switches automatically to shared-barrier scatter-gather (currently issued serially).

---

## 3. Storage & Compaction Tuning (`metadb-storage`)

| Parameter | Default | Tuning Guidance |
| :--- | :--- | :--- |
| `mvcc_retention_ms` | `5000` (`5s`) | Controls how long in-memory MVCC versions are retained for active `SnapshotGuard`s and write coalescing. Keep at `3000–5000ms`; never run multi-second scans in a single snapshot—use cursor-paginated `ListDir` (SQL Template #10). |
| `memtable_size_bytes` | `32 MiB` | Active memtable budget. Crossing it seals and flushes. `max_imm_memtables` (default 4) makes writers wait instead of queueing more immutable tables. |
| `block_cache_bytes` | `64 MiB` | Set to `25%–40%` of host RAM on dedicated BE nodes so SSTable `IndexBlock`, `FilterBlock`, and hot directory `DataBlock`s stay memory-resident across the 16 LRU cache shards. |
| `locality_tombstone_threshold` | `128` | Number of physical LSM tombstones in a single 9-byte `[table_prefix][PID]` bucket (combined with `locality_tombstone_ratio >= 0.50`) that triggers targeted bottommost range compaction. Lower to `64` for latency-sensitive `listdir` workloads under heavy TTL deletion. |

---

## 4. Troubleshooting Common Errors

* **`MetaDbError::TransactionTimeout`**:
  * *Cause*: A read `SnapshotGuard` was held longer than `mvcc_retention_ms` (`5,000ms`) or requested a `read_hlc` older than `disk_flushed_max_hlc`.
  * *Fix*: Replace unbounded directory scans with paginated `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT 1000`, acquiring a fresh snapshot per page.
* **`MetaDbError::ClockSkewExceeded`**:
  * *Cause*: Wall-clock drift between Proxy/BE nodes exceeded `max_clock_drift_ms` (`500ms`).
  * *Fix*: Verify `chrony` / NTP synchronization across all cluster hosts (`chronyc tracking`).
* **`MetaDbError::EpochMismatch` / `MetaDbError::NotLeader`**:
  * *Cause*: Transient shard split (`version` bump) or Raft leader failover (`conf_ver` / term change).
  * *Fix*: Handled automatically by `metadb-proxy` and the Rust SDK via `MetaCache` refresh + retry. Persistent occurrences indicate an unreachable `CellLocalManager`.
* **Growing `.raftlog` Disk Usage**:
  * *Cause*: `safe_purge_index = min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)` is pinned by stalled L0 disk I/O or a blocked snapshot transfer. (Note: unreachable remote `QuotaTable` shards do **not** pin `.raftlog` indefinitely because overdue `AuxOutbox` tasks spill to local `[0x07]` SSTables on L0 flush.)
  * *Fix*: Inspect NVMe write latency and the exposed `metadb_min_raft_safe_purge_index` metric (`:9100/metrics`). Compare per-shard logs during diagnosis; no per-shard watermark-lag metric is currently exported.
