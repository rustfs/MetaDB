# Operations, `metadb-ctl` CLI & Performance Tuning (English)

* **Back to Guide Index**: [English Overview](./README.md) | [Hub](../../README.md)

---

## 1. `metadb-ctl` Administration CLI `[Implemented]`

`metadb-ctl` connects to `ControlPlaneService` and `MetadataService` over gRPC to inspect cluster topology, manage shards, execute SQL templates, and run synthetic benchmarks:

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
```

---

## 2. Managing Skewed Workloads: Jumbo Shards & Dual-Mode Splitting

1. **Automatic Dedicated-Node Isolation (`JumboIsolated`)**:
   * When a single directory (`PID`) exceeds `shard_split_threshold_bytes` (`4 GiB` default) and represents `>80%` of shard bytes, `PolicyShardSplitter` raises `UnsplittableJumboShard`.
   * `CellLocalManager` marks the shard `JumboIsolated` and migrates peer shard leaders/replicas off that BE host so the jumbo directory gets uncontended NVMe and CPU resources.
2. **Dual-Mode `(PID, name)` Splitting (`DualModeFineGrained`)**:
   * Only if a single directory grows beyond a dedicated host's capacity (e.g., `>500M` files / `>100 GiB`), enable `DualModeFineGrained` via `metadb-ctl shard enable-dual-mode`.
   * Non-anchor sub-shards maintain shard-local `[0x04] DirShardDelta` counters: `create_file` and `unlink` remain `1-RTT` single-shard operations, while `dirstat` switches automatically to `1-hop` parallel scatter-gather.

---

## 3. Storage & Compaction Tuning (`metadb-storage`)

| Parameter | Default | Tuning Guidance |
| :--- | :--- | :--- |
| `mvcc_retention_ms` | `5000` (`5s`) | Controls how long in-memory MVCC versions are retained for active `SnapshotGuard`s and write coalescing. Keep at `3000–5000ms`; never run multi-second scans in a single snapshot—use cursor-paginated `ListDir` (SQL Template #10). |
| `memtable_size_bytes` | `32 MiB` | Increase to `64 MiB` on write-heavy AI checkpoint clusters with `>= 64 GiB` RAM per BE node to maximize in-memory MVCC version pruning before L0 flush. |
| `block_cache_bytes` | `512 MiB` | Set to `25%–40%` of host RAM on dedicated BE nodes so SSTable `IndexBlock`, `FilterBlock`, and hot directory `DataBlock`s stay memory-resident across the 16 LRU cache shards. |
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
  * *Fix*: Inspect NVMe write latency and `metadb_raft_watermark_lag_entries` in Prometheus metrics (`:9100/metrics`).
