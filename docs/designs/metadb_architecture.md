# EGM Specification: MetaDB Distributed Metadata Engine

* **Status**: Approved
* **Methodology**: [Google Elephant-Goldfish Model (EGM)](https://github.com/google/elephant-goldfish-model)
* **Primary Reference**: Biao Cao et al., *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1) (Baidu & Peking University, 2026)
* **Semantic Boundaries**:
  * `FeatureRange`: Sections 1 – 6
  * `DetailedRange`: Sections 7 – 9

---

## 1. Problem Statement (`FeatureRange`)

Distributed storage systems at exabyte scale (flat object stores such as BOS/S3, hierarchical object stores such as Mantle, and POSIX file systems such as CFS) require a unified metadata layer capable of storing **100 billion to 10+ trillion metadata records** while delivering sub-millisecond point/range latency and millions of QPS.

As demonstrated in Baidu's MetaDB paper (`arXiv:2610.32247v1`), general-purpose distributed databases fail on three fronts:
1. **Data Model Mismatch**: Flat range/hash partitioning splits entries of the same directory across shards, turning `>96%` localized single-directory operations into multi-shard 2PC transactions and incurring multiple network round-trips.
2. **Transaction Model Mismatch**: Synchronous row locking on parent directory attributes (`num_children`, `modify_time`) causes a `10x–36x` throughput collapse under concurrent single-directory file creation, while synchronous ACID updates to `QuotaTable` and `SecondaryIndexTable` force unnecessary cross-shard 2PC.
3. **Storage Implementation Mismatch**: Persisting MVCC versions and tombstones to disk for transient (`<10ms`) metadata transactions creates a double-tombstone penalty (MVCC tombstones + LSM tombstones) that spikes `listdir` latency from `<100ms` to `~6s` during consecutive bulk TTL deletions.

Furthermore, adopters need an engine that can run in **Single-Node All-in-One Mode** on day one and scale out online into a **Multi-Cell Distributed Cluster** without rewriting storage or recovery logic.

---

## 2. Goals & Non-Goals (`FeatureRange`)

### Goals
* **G1 (Topology-Aware Sharding & Pushdown)**: Colocate directory attributes `(PID, "_attr_")` with child entries `(PID, name)` using `group_key = PID` boundary splitting, and execute single-directory Read-Modify-Write operations in `1 RTT` on the shard leader.
* **G2 (Semantic-Aware Off-Critical-Path Execution)**: Aggregate hotspot parent directory attributes (`num_children`, `size_bytes`, `modify_time`) asynchronously in memory with **Read-Path Reconciliation**, and decouple `QuotaTable` and `SecondaryIndexTable` updates into an HLC-idempotent asynchronous sink.
* **G3 (Lifecycle-Aware Storage Engine)**: Retain MVCC versions strictly in memory (`In-Memory MVCC`) for the `5s` transaction window—flushing only the latest committed state or a single physical tombstone—and trigger **Locality-Aware Compaction** on key ranges with high tombstone density.
* **G4 (Single-Node to Trillion-Scale Evolution)**: Support `1-Voter` Raft in single-node mode, dynamic `PID`-boundary shard splitting, online `ConfChange` (`1 -> 3` replicas), and a Two-Tier Control Plane (`GlobalManager` + `CellLocalManager` + `MetaCache` + `HLC`) scaling to 100B–10T+ records.
* **G5 (Lightweight SQL Template Interface)**: Compile the `~20` fixed metadata SQL patterns directly into storage execution plans.

### Non-Goals
* **NG1**: Arbitrary ad-hoc SQL joins, subqueries, or long-running analytical transactions (`>5s`).
* **NG2**: Raw object data-block storage (handled by the RustFS data plane).

---

## 3. Proposed Architecture (`FeatureRange`)

### 3.1 Three Core Tables (`metadb-core`)
1. `MetaTable`: Keyed by `MetaKey { pid: InodeId, name: String }` encoded in lexicographical big-endian byte order (`[table_prefix=0x01][pid: 8B BE][name_bytes]`).
   * Special child name `"_attr_"` stores the directory's own attributes (`InodeAttr`) colocated under `pid`.
   * Sharding `group_key` is `pid`.
2. `QuotaTable`: Keyed by `QuotaKey { uid: UserId }` (`[table_prefix=0x02][uid: 8B BE]`), storing `QuotaRecord { uid, used_bytes, used_inodes, limit_bytes, last_applied_hlc }`.
3. `SecondaryIndexTable`: Keyed by `SecondaryIndexKey { fid: FileId }` (`[table_prefix=0x03][fid: 8B BE]`), storing `SecondaryIndexRecord { fid, pid, name, create_time, last_applied_hlc }`.

### 3.2 Hybrid Logical Clock (`HLC`)
* Every node runs an independent `HybridLogicalClock` packing a 48-bit physical millisecond wall clock and a 16-bit logical counter into a monotonic `HlcTimestamp(u64)`.
* Eliminates central Timestamp Oracle (TSO) bottlenecks.

### 3.3 Storage Layer (`metadb-storage`)
* **WAL-less Persistent KV + Raft Log**: All mutations are first appended to `RaftLog` (which assigns a monotonically increasing `LogSequenceNumber (LSN)`). The underlying persistent KV engine (`PersistentKvStore`) disables its own WAL.
* **Safe Raft Log Truncation Watermark**:
  $$\text{safe\_truncate\_lsn} = \min(\text{storage\_flushed\_lsn},\ \text{attr\_aggregated\_lsn},\ \text{aux\_sink\_acked\_lsn})$$
* **In-Memory MVCC (`MemMvccStore`)**: Stores version chains `VecDeque<MvccVersion>` per key in memory. Snapshots older than `retention_window_ms` (`5,000ms`) are rejected with `MetaDbError::TransactionTimeout`. On `flush_to_persistent()`, only the newest version per key is written (`Put` for live value, single physical `Delete` tombstone for `MvccTombstone`), and all older versions/MVCC tombstones are discarded.
* **Locality-Aware Compaction (`LocalityCompactor`)**: Tracks deletion counts per key prefix bucket (`pid` or key-range slice). When `tombstones_in_bucket >= density_threshold`, `compact_range(start_key, end_key)` purges physical tombstones solely within that bucket.

### 3.4 Transaction & Off-Critical-Path Layer (`metadb-tx`)
* **Single-Shard Pushdown (`PushdownExecutor`)**: Executes `CreateFile`, `Mkdir`, `Unlink`, `Rmdir`, `DirStat`, `Lookup`, and `ListDir` atomically on a single shard.
* **2PC Cross-Shard Coordinator**: Executes atomic cross-directory `Rename` when `src_pid` and `dst_pid` reside on different shards.
* **Async Attribute Aggregator (`AsyncAttrAggregator`)**: Accumulates `AttrDelta { pid, delta_children, delta_bytes, latest_modify_time, lsn }` in memory without locking `(pid, "_attr_")`. Persisted `InodeAttr` tracks `applied_lsn`; `reconcile_attr(pid, persisted_attr)` folds only deltas with `delta.lsn > persisted_attr.applied_lsn` to prevent undercounting or double-counting during concurrent flushes. `rmdir` verifies emptiness via an authoritative live-child scan (excluding the reserved `"_attr_"` entry, which is rejected for user-created files).
* **Async Auxiliary Sink (`AsyncAuxiliarySink`)**: Queues `AuxUpdateTask` entries (`QuotaUpdate` and `SecondaryIndexUpdate`), batches them by target, and applies them idempotently using per-origin-shard LSN watermarks (`applied_lsn_by_shard`) for additive `QuotaTable` deltas and monotonic `HlcTimestamp` checks for `SecondaryIndexTable` mutations.

### 3.5 Topology-Induced Sharding & Two-Tier Control Plane (`metadb-cluster`)
* **Shard Splitting on `group_key = PID`**: Splits shards only between distinct `PID` values unless the shard is explicitly placed in `ShardSplitMode::DualModeFineGrained`.
* **Jumbo Shard Detection & Isolation**: Shards holding a single massive `PID` above `jumbo_shard_threshold_bytes` are marked `JumboIsolated` and scheduled onto dedicated BE nodes.
* **Two-Tier Control Plane**: `GlobalManager` maps coarse key ranges to `CellId`; each `CellLocalManager` manages per-shard Raft groups, `PID`-boundary splits, and replica membership (`SingleNode` 1-Voter $\rightarrow$ `ThreeWayReplica` 3-Voter via `ConfChange`).

---

## 4. Alternatives Considered (`FeatureRange`)

1. **General-Purpose Distributed SQL (TiDB / CockroachDB)**: Rejected due to 100% multi-shard transaction ratio when secondary/quota indexes are updated in-band, 36.2x lower throughput under single-directory concurrent writes, and multi-second `listdir` latency spikes under bulk deletions (`arXiv:2610.32247v1`, §7 & Appendix A).
2. **Decoupled External Directory Index Service**: Rejected because maintaining directory trees outside the transactional database requires cross-system 2PC and duplicate replication/recovery machinery (`arXiv:2610.32247v1`, §8).
3. **Separate Non-Raft Storage Path for Single-Node Mode**: Rejected because dual durability paths (RocksDB WAL vs. Raft Log) break state-machine symmetry for `In-Memory MVCC` flush watermarks and asynchronous queue recovery during online cluster expansion.

---

## 5. Risks & Mitigations (`FeatureRange`)

1. **Risk: Single-Directory Skew Exceeding Single-Node Disk/CPU (Jumbo Shard)**:
   * *Mitigation*: Two-level defense: (a) `CellLocalManager` automatically isolates jumbo shards onto dedicated BE nodes (`JumboIsolated`); (b) operator/policy switch to `DualModeFineGrained` allows splitting a single `PID` across shards by `(PID, name)` sub-ranges, automatically routing operations on that `PID` through the 2PC path.
2. **Risk: Unbounded Raft Log Growth if Remote Auxiliary Shard is Unreachable**:
   * *Mitigation*: When `AsyncAuxiliarySink` retry lag exceeds `max_pending_aux_tasks`, pending tasks are spilled into a local persistent retry queue within the shard, advancing `aux_sink_acked_lsn` so Raft log truncation proceeds unimpeded.
3. **Risk: Stale Parent Directory Attributes on Immediate Read**:
   * *Mitigation*: `DirStat` and `GetAttr` always invoke `AsyncAttrAggregator::reconcile_attr()`, folding in-memory pending deltas into the persisted `_attr_` record before returning to the caller.

---

## 6. Capacity & Scale Verification (`FeatureRange`)

* **100B Records (`10^11`)**: `~18 TB` logical (`~54 TB` 3-replica), `~4,500` shards (`4 GiB` each), served by a single Cell of `16–24` BE nodes.
* **1T–10T Records (`10^12 – 10^13`)**: `180 TB – 1.8 PB` logical (`540 TB – 5.4 PB` 3-replica), `45,000 – 450,000` shards partitioned across `4–20` Cells under `GlobalManager`.

---

## 7. Target Scope & File Boundary (`DetailedRange`)

 In compliance with the EGM Implementation Readiness Standard, Phase 5 implementation is strictly scoped to the following repository-relative files:

* `Cargo.toml`
* `README.md`
* `Architecture.md`
* `_agents/skills/metadb_elephant/SKILL.md`
* `docs/designs/SITEMAP.md`
* `docs/designs/concepts_brief.md`
* `docs/designs/metadb_architecture.md`
* `docs/designs/review_report.md`
* `crates/metadb-core/Cargo.toml`
* `crates/metadb-core/src/lib.rs`
* `crates/metadb-core/src/hlc.rs`
* `crates/metadb-core/src/schema.rs`
* `crates/metadb-core/src/error.rs`
* `crates/metadb-storage/Cargo.toml`
* `crates/metadb-storage/src/lib.rs`
* `crates/metadb-storage/src/raft_log.rs`
* `crates/metadb-storage/src/mvcc.rs`
* `crates/metadb-storage/src/compaction.rs`
* `crates/metadb-storage/src/engine.rs`
* `crates/metadb-tx/Cargo.toml`
* `crates/metadb-tx/src/lib.rs`
* `crates/metadb-tx/src/attr_aggregator.rs`
* `crates/metadb-tx/src/aux_sink.rs`
* `crates/metadb-tx/src/pushdown.rs`
* `crates/metadb-cluster/Cargo.toml`
* `crates/metadb-cluster/src/lib.rs`
* `crates/metadb-cluster/src/shard.rs`
* `crates/metadb-cluster/src/control_plane.rs`
* `crates/metadb-proxy/Cargo.toml`
* `crates/metadb-proxy/src/lib.rs`
* `crates/metadb-server/Cargo.toml`
* `crates/metadb-server/src/lib.rs`
* `crates/metadb-server/tests/integration_tests.rs`

---

## 8. Actionable Implementation Steps (`DetailedRange`)

### Phase 5.1: `metadb-core`
1. Implement `HlcTimestamp` and thread-safe `HybridLogicalClock` (`now()`, `update(remote)`).
2. Implement big-endian lexicographical key encoding for `MetaKey (pid, name)` with `ATTR_ENTRY_NAME = "_attr_"`, `QuotaKey (uid)`, and `SecondaryIndexKey (fid)`.
3. Guarantee that `MetaKey::group_key()` returns `pid.to_be_bytes()` so all entries sharing a parent directory share an identical 8-byte prefix after the table byte.

### Phase 5.2: `metadb-storage`
1. Implement `RaftLog` supporting append, checkpoint tracking (`storage_flushed_lsn`, `attr_aggregated_lsn`, `aux_sink_acked_lsn`), safe truncation (`min` of the three watermarks), and crash recovery replay.
2. Implement `MemMvccStore` with in-memory version chains (`MvccValue::Value(Vec<u8>)` and `MvccValue::Tombstone`), enforcing `tx_timeout_ms` (`5,000ms`) and pruning MVCC tombstones during `flush_to_persistent()` so only a single physical LSM tombstone is written per deleted key.
3. Implement `LocalityCompactor` tracking range tombstone density per prefix bucket and triggering `compact_range()` on hot deletion ranges.
4. Combine them in `LifecycleStorageEngine`.

### Phase 5.3: `metadb-tx`
1. Implement `AsyncAttrAggregator` with lock-free/fine-grained per-PID delta folding (`delta_children`, `delta_bytes`, `max(modify_time)`), `reconcile_attr()` for linearizable reads, and `flush_to_engine()` for batched background persistence.
2. Implement `AsyncAuxiliarySink` with destination batching, HLC-based idempotency (`last_applied_hlc`), and local overflow spill so `aux_sink_acked_lsn` never blocks Raft log truncation indefinitely.
3. Implement `PushdownExecutor` for `1-RTT` single-shard directory operations (`mkdir`, `create_file`, `unlink`, `rmdir`, `dirstat`, `lookup`, `listdir`) and a `TwoPhaseCommitCoordinator` for cross-directory `rename`.

### Phase 5.4: `metadb-cluster`, `metadb-proxy`, and `metadb-server`
1. Implement `ShardMetadata` and `PolicyShardSplitter` that splits shards strictly on `group_key = PID` boundaries, detects unsplittable Jumbo Shards (`JumboIsolated`), and supports `DualModeFineGrained` fallback.
2. Implement Raft membership transitions (`ConfChange::AddLearner`, `ConfChange::PromoteVoter`, `ConfChange::RemoveNode`) enabling zero-downtime evolution from `1-Voter` Single-Node mode to `3-Voter` Distributed mode.
3. Implement `CellLocalManager`, `GlobalManager`, and `MetaCache` for two-tier control plane routing across millions of shards.
4. Implement `SqlTemplateParser` in `metadb-proxy` supporting the canonical metadata SQL templates (`INSERT INTO MetaTable ...`, `SELECT * FROM MetaTable WHERE pid = ? ...`, `DELETE FROM MetaTable ...`, etc.).
5. Assemble `MetaDbClusterEngine` in `metadb-server` supporting single-node initialization, automatic `PID`-boundary shard splitting, and online scale-out to multi-node/multi-cell clusters.

---

## 9. Test Plan (`DetailedRange`)

1. **T1 (`group_key = PID` Colocation & Split Test)**: Insert entries across multiple directories until shard size exceeds threshold; verify that shard split points occur strictly between distinct `PID`s and never separate `(PID, "_attr_")` from `(PID, child_name)`.
2. **T2 (Jumbo Shard Isolation & Dual-Mode Fallback Test)**: Populate a single directory beyond the split threshold; verify `CellLocalManager` marks it `JumboIsolated` and assigns a dedicated node, and verify `DualModeFineGrained` fallback when explicitly enabled.
3. **T3 (Hotspot Directory Async Attribute Aggregation & Read Reconciliation Test)**: Concurrently create 1,000 files in the same directory; verify zero parent-lock serialization, verify immediate `dirstat` returns exact `num_children = 1000` and latest `modify_time` before background flush, and verify persisted state after flush.
4. **T4 (Decoupled Async Quota & Secondary Index Idempotency Test)**: Execute file creations and replay duplicate `AuxUpdateTask` batches with identical or older HLC timestamps (simulating Raft leader failover); verify `QuotaTable` and `SecondaryIndexTable` apply each update exactly once.
5. **T5 (In-Memory MVCC Double-Tombstone Elimination & Locality Compaction Test)**: Perform bulk creation and consecutive TTL deletions; verify `MemMvccStore` prunes MVCC tombstones on flush (reducing persistent writes) and `LocalityCompactor` purges only the hot deletion range, leaving zero tombstones traversed during `listdir`.
6. **T6 (Single-Node to Distributed Online Evolution Test)**: Start `MetaDbClusterEngine` in Stage 1 (`1 Node, 1-Voter Raft`), grow into Stage 2 (`Multi-Shard Split`), and evolve online into Stage 3 (`3-Voter Multi-Raft` across multiple nodes and multiple Cells) while verifying data integrity and SQL template execution.
