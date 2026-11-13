# EGM Specification: MetaDB Distributed Metadata Engine

* **Status**: Phase 0 Approved Umbrella Specification
* **Methodology**: [Google Elephant-Goldfish Model (EGM)](https://github.com/google/elephant-goldfish-model)
* **Primary Reference**: Biao Cao et al., *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1) (Baidu & Peking University, 2026)
* **Modular Subsystem Contracts**:
  * [`storage_lsm_mvcc.md`](./storage_lsm_mvcc.md) — Pure-Rust WAL-less LSM, `SnapshotGuard` MVCC & `LocalityCompactor` (`metadb-storage`)
  * [`consensus_and_transactions.md`](./consensus_and_transactions.md) — `openraft` Multi-Raft, 3-Watermark Log, `ReadIndex`, `PushdownExecutor`, `AsyncAttrAggregator`, `AuxOutbox` & 2PC (`metadb-raft`, `metadb-tx`)
  * [`sharding_and_control_plane.md`](./sharding_and_control_plane.md) — `ShardEpoch` Topology Sharding, Jumbo `DualModeFineGrained`, Two-Tier Control Plane, 20 SQL Templates & Protobuf v1 (`metadb-proto`, `metadb-cluster`, `metadb-proxy`)
  * [`../DEVELOPMENT_PLAN.md`](../DEVELOPMENT_PLAN.md) — Phased Pure-Rust Implementation Plan (Phases 1–6)
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

### 3.1 Seven Disjoint Binary Keyspaces (`metadb-core`)
1. `MetaTable` (`0x01`): Keyed by `MetaKey { pid: InodeId, name: String }` encoded in big-endian byte order (`[0x01][pid: 8B BE][name_utf8]`).
   * Reserved child name `"_attr_"` stores the directory's own `InodeAttr` (`88B` fixed binary payload with `parent_pid`, `applied_raft_index`, and `CRC32C`) colocated under `pid` (`group_key = pid`).
2. `QuotaTable` (`0x02`): Keyed by `QuotaKey { uid: UserId }` (`[0x02][uid: 8B BE]`), storing `QuotaRecord` with `applied_index_by_shard: BTreeMap<ShardId, u64>`.
3. `SecondaryIndexTable` (`0x03`): Keyed by `SecondaryIndexKey { fid: FileId }` (`[0x03][fid: 8B BE]`), storing `SecondaryIndexRecord` with `(last_applied_hlc, tombstone_hlc, is_deleted)`.
4. Shard-Local Replicated System Prefixes (`0x04..0x07`):
   * `0x04 DirShardDelta`: Per-sub-shard partial directory stats & existence lease for `DualModeFineGrained` jumbo directories.
   * `0x05 WriteIntent`: Durable Raft-replicated 2PC participant locks (`Put`, `Delete`, `DeletingDir`, `DirExistsFence`).
   * `0x06 TxnRecord`: Primary-Anchored 2PC commit/abort source of truth (`ttl_ms = 3,000ms`).
   * `0x07 SpilledAuxOutbox`: Local persistent overflow queue preventing remote shard partitions from blocking Raft log truncation.

### 3.2 Hybrid Logical Clock (`HLC`) with Drift & Overflow Guard
* Every node runs a decentralized `HybridLogicalClock` packing a 48-bit physical millisecond wall clock and a 16-bit logical counter into `HlcTimestamp(u64)`, enforcing `max_clock_drift_ms` (`500ms`) on incoming RPC timestamps and advancing the 48-bit physical component (never wrapping `u16` to `0`) if logical ticks reach `u16::MAX`.

### 3.3 Pure-Rust WAL-less LSM & `SnapshotGuard` MVCC (`metadb-storage` & `metadb-raft`)
* **WAL-less `LsmStorageBackend` + Segmented `openraft` Log**: Local KV WAL files are disabled; pre-flush durability relies on `metadb-raft`'s CRC32C-framed `.raftlog` segments and durable `HARD_STATE.last_committed`.
* **Safe Raft Log Purge Gate**:
  $$\text{safe\_purge\_index} = \min\big(\text{state\_flushed\_index},\ \text{aux\_outbox\_durable\_index},\ \text{last\_snapshot\_index}\big)$$
* **`SnapshotGuard` & `ReadCut` In-Memory MVCC (`MemMvccStore`)**: Active readers pin an immutable `ReadCut { max_raft_index, max_hlc }` + `Arc<Version>` + `Vec<Arc<ImmMemTable>>` (`<= 5,000ms` timeout). Sealed `ImmMemTable`s collapse-flush unversioned latest values or single physical tombstones to `flush_generation`-ordered 4 KiB CRC32C SSTables **strictly when** `min_active_snapshot_hlc >= imm.max_hlc`.
* **Bottommost-Safe Locality-Aware Compaction (`LocalityCompactor`)**: Tracks tombstone count and ratio per 9-byte `[table_prefix][group_key: 8B]` bucket, rewrites all keys of overlapping input SSTables, and drops physical tombstones strictly when proven bottommost.

### 3.4 Deterministic Replicated Transactions & Off-Critical-Path Queues (`metadb-tx`)
* **Single-Shard Pushdown (`PushdownExecutor`)**: Executes `CreateFile`, `Unlink`, `SetAttr`, `Lookup`, `DirStat`, `ListDir`, and shard-colocated `Mkdir`/`Rmdir` in `1 RTT` (fenced by `ShardEpoch` and batched `ReadIndex`).
* **Crash-Recoverable Primary-Anchored 2PC (`TwoPhaseCommitCoordinator`)**: Executes cross-shard `Rename` (exclusive and POSIX overwrite), cross-shard `Mkdir`, and cross-shard `Rmdir` with cycle detection (`DirRenameGuard` + `parent_pid` walk) and `TxnRecoveryWorker` roll-forward/abort.
* **`AsyncAttrAggregator` + Coordinated Flush Barrier**: Folds deltas `<= M` directly into `(PID, "_attr_")` inside the sealing `ImmMemTable` batch before `attr_flushed_index` advances, while `reconcile_attr` folds strictly `delta.raft_index > persisted.applied_raft_index && delta.hlc <= read_hlc`.
* **Exact-Once FIFO `AuxOutbox`**: Dispatches `QuotaTable` and `SecondaryIndexTable` batches in strict ascending `origin_raft_index` order per `(origin_shard_id, target_shard_id)`, spilling overdue tasks to `[0x07]` on L0 flush.

### 3.5 Topology Sharding, Two-Tier Control Plane & Wire Contracts (`metadb-proto`, `metadb-cluster`, `metadb-proxy`, `metadb-server`)
* **Unified `[0x01, 0x04)` Sharding**: `PolicyShardSplitter` splits `MetaTable` shards on `group_key = PID` boundaries via `RaftCmd::ExecuteSplit`, isolates single-`PID` Jumbo Shards (`JumboIsolated`), and supports `DualModeFineGrained` `(PID, name)` splits (`1-RTT` writes + `1-hop` parallel scatter-gather `dirstat`).
* **Two-Tier Control Plane**: `GlobalManager` + per-Cell `CellLocalManager` + `MetaCache` + bitspace-partitioned `IdRangeAllocator` (File bit-63 = `1`, Directory bit-63 = `0` with shard-locality leases).
* **Interfaces**: 20 parameterized/escaped SQL templates in `metadb-proxy`, `tonic` gRPC services in `metadb-proto`, embedded `MetaDbEngine` SDK, `metadbd` daemon, and `metadb-ctl` CLI in `metadb-server`.

---

## 4. Alternatives Considered (`FeatureRange`)

1. **General-Purpose Distributed SQL (TiDB / CockroachDB)**: Rejected due to 100% multi-shard transaction ratio when secondary/quota indexes are updated in-band, 36.2x lower throughput under single-directory concurrent writes, and multi-second `listdir` latency spikes under bulk deletions (`arXiv:2610.32247v1`, §7 & Appendix A).
2. **External C/C++ Storage Engines (`librocksdb-sys`)**: Rejected to maintain a 100% memory-safe pure-Rust stack with native `SnapshotGuard`-gated MemTable collapse and 9-byte prefix tombstone-density compaction hooks.
3. **Decoupled External Directory Index Service**: Rejected because maintaining directory trees outside the transactional database requires cross-system 2PC and duplicate replication/recovery machinery (`arXiv:2610.32247v1`, §8).
4. **Separate Non-Raft Storage Path for Single-Node Mode**: Rejected because dual durability paths break state-machine symmetry for `MemMvccStore` watermarks and asynchronous queue recovery during online `1-Voter -> 3-Voter` cluster expansion.

---

## 5. Risks & Mitigations (`FeatureRange`)

1. **Single-Directory Skew (Jumbo Shard)**: Mitigated by (a) `CellLocalManager` `JumboIsolated` dedicated-node placement, and (b) `DualModeFineGrained` `(PID, name)` splitting using shard-local `[0x04] DirShardDelta` records so `create`/`unlink` remain `1-RTT` lock-free while `dirstat` uses `1-hop` scatter-gather.
2. **Unbounded Raft Log Growth During Remote Sink Partitions**: Mitigated by spilling overdue `AuxOutbox` entries into `[0x07] SpilledAuxOutbox` inside the local L0 SSTable flush, advancing `aux_outbox_durable_index` immediately.
3. **Future-Read Anomaly & Stale Parent Attributes**: Mitigated by `ActiveSnapshotRegistry` gating unversioned L0 flushes until `min_active_snapshot_hlc >= imm.max_hlc` and `AsyncAttrAggregator` folding deltas via the Coordinated Flush Barrier + Read-Path Reconciliation.

---

## 6. Capacity & Scale Verification (`FeatureRange`)

* **Single-Node Starter (`10^8` records)**: `~18 GB` (`1-Voter`), `5–16` shards, `< 2 GiB` RAM on 1 `metadbd` node.
* **100B Records (`10^11`)**: `~18 TB` logical (`~54 TB` 3-replica), `~4,500` shards (`4 GiB` each), served by 1 Cell (`~20` BE nodes).
* **1T–10T Records (`10^12 – 10^13`)**: `180 TB – 1.8 PB` logical (`540 TB – 5.4 PB` 3-replica), `45,000 – 450,000` shards partitioned across `4–20` Cells under `GlobalManager`.

---

## 7. Target Workspace Scope & File Boundaries (`DetailedRange`)

Phases 1–6 target the 8-crate Cargo workspace defined in [`../DEVELOPMENT_PLAN.md`](../DEVELOPMENT_PLAN.md):
* `crates/metadb-core`: `src/{lib.rs, error.rs, hlc.rs, schema.rs, epoch.rs, system_keys.rs, checksum.rs}`
* `crates/metadb-proto`: `proto/metadb/v1/{common,metadata,transaction,raft,control_plane}.proto`, `src/{lib.rs, convert.rs}`
* `crates/metadb-storage`: `src/{lib.rs, backend.rs, block.rs, bloom.rs, cache.rs, sstable.rs, manifest.rs, version_set.rs, mvcc.rs, compaction.rs, engine.rs}` (specified in [`storage_lsm_mvcc.md`](./storage_lsm_mvcc.md))
* `crates/metadb-raft`: `src/{lib.rs, types.rs, log_store.rs, state_machine.rs, network.rs, read_index.rs, group_host.rs, conf_change.rs}` (specified in [`consensus_and_transactions.md`](./consensus_and_transactions.md))
* `crates/metadb-tx`: `src/{lib.rs, state_machine_impl.rs, pushdown.rs, attr_aggregator.rs, aux_outbox.rs, two_pc.rs, recovery_worker.rs}` (specified in [`consensus_and_transactions.md`](./consensus_and_transactions.md))
* `crates/metadb-cluster`: `src/{lib.rs, shard.rs, split_worker.rs, jumbo.rs, id_allocator.rs, meta_cache.rs, cell_manager.rs, global_manager.rs}` (specified in [`sharding_and_control_plane.md`](./sharding_and_control_plane.md))
* `crates/metadb-proxy`: `src/{lib.rs, sql_lexer.rs, sql_templates.rs, router.rs}` (specified in [`sharding_and_control_plane.md`](./sharding_and_control_plane.md))
* `crates/metadb-server`: `src/{lib.rs, config.rs, grpc_services.rs, metrics.rs}`, `src/bin/{metadbd.rs, metadb-ctl.rs}`

---

## 8. Phased Implementation Sequence (`DetailedRange`)

Full step-by-step execution rules, invariants, and verification commands are specified in [`../DEVELOPMENT_PLAN.md`](../DEVELOPMENT_PLAN.md):
1. **Phase 1 (`metadb-core` & `metadb-proto`)**: `0x01..0x07` CRC32C binary codecs, HLC drift guard, `ShardEpoch`, and Protobuf v1 / `tonic` gRPC contracts.
2. **Phase 2 (`metadb-storage`)**: Pure-Rust WAL-less SSTable LSM, `MANIFEST` recovery, 16-shard LRU `BlockCache`, Bloom filters, `SnapshotGuard` MVCC, and bottommost-safe `LocalityCompactor`.
3. **Phase 3 (`metadb-raft`)**: `openraft` Multi-Raft, segmented CRC32C `.raftlog` store, 3-watermark purge gate, batched `ReadIndex`, and online `1-Voter -> 3-Voter` `ConfChange`.
4. **Phase 4 (`metadb-tx`)**: Deterministic `RaftStateMachine`, `PushdownExecutor`, Coordinated Flush Barrier `AsyncAttrAggregator`, FIFO `AuxOutbox` + `[0x07]` spill, and Primary-Anchored 2PC + `TxnRecoveryWorker`.
5. **Phase 5 (`metadb-cluster` & `metadb-proxy`)**: Epoch-fenced `ExecuteSplit`, Jumbo isolation & `DualModeFineGrained` scatter-gather, Two-Tier Control Plane, `IdRangeAllocator`, and 20 parameterized SQL templates.
6. **Phase 6 (`metadb-server`)**: Unified `MetaDbEngine` SDK, `metadbd` multi-role server, `metadb-ctl` CLI, and end-to-end Stage 1 -> Stage 2 -> Stage 3 evolution suite.

---

## 9. System-Wide Acceptance Test Matrix (`DetailedRange`)

Subsystem fault-injection tests (`T-LSM-*`, `T-MVCC-*`, `T-COMP-*`, `T-RAFT-*`, `T-TX-*`, `T-2PC-*`, `T-SHARD-*`, `T-ID-*`, `T-SQL-*`) are defined in the three modular specifications and culminate in the end-to-end cluster evolution suite (`e2e_cluster_evolution_tests.rs`) verifying:
1. Persistent `1-Voter` crash recovery across torn `.raftlog` and `MANIFEST` tails.
2. Live `group_key = PID` shard splitting under concurrent load with zero mid-directory splits and transparent `EpochMismatch` retry.
3. Jumbo Shard dedicated-node isolation and `DualModeFineGrained` `(PID, name)` scatter-gather `dirstat` + paginated `listdir` + 2PC `rmdir`.
4. Hotspot single-directory concurrent writes with linearizable Read-Path Reconciliation across L0 Coordinated Flush Barriers.
5. Exact-once `QuotaTable` and `SecondaryIndexTable` convergence across network partitions (`[0x07]` local spill) and leader failovers.
6. Crash-recoverable cross-shard 2PC `rename` (exclusive + overwrite + cycle rejection) and online `1-Voter -> Learner -> 3-Voter` Multi-Cell expansion.
