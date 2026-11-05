# MetaDB

**MetaDB** is a high-performance, file-system structure-aware distributed transactional metadata database engine written in Rust for [RustFS](https://github.com/rustfs).

> **Academic & Architectural Attribution**:
> The architecture of `rustfs/MetaDB` is directly inspired by and adapted from Baidu's trillion-scale production metadata center paper:
> **Biao Cao, Kang Chen, Zhihui Yin, Qiushi Chen, Jielong Jian, Jiwei Xiong, Xinxing Wang, Yan Xing, Liguo Duan, Ran Zheng, Xianjun Meng, and Zhenyu Hou**, *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1) (Baidu & Peking University, 2026).
>
> **Spec-Driven Development Methodology**:
> Designed, stress-tested, and verified using Google's [Elephant-Goldfish Model (EGM)](https://github.com/google/elephant-goldfish-model). See [`Architecture.md`](./Architecture.md) and [`docs/designs/SITEMAP.md`](./docs/designs/SITEMAP.md) for the complete specification and zero-context Goldfish review reports.

---

## Why MetaDB?

General-purpose distributed databases (such as TiDB, CockroachDB, and FoundationDB) treat file-system and object-storage metadata as generic flat key-value or relational rows. At exabyte scale, this "black-box" approach creates three severe bottlenecks:

1. **Data Model Mismatch**: Flat hash or size-based sharding fractures entries of the same directory across multiple shards, turning `>96%` of localized single-directory operations (`create`, `mkdir`, `unlink`, `listdir`) into multi-shard Two-Phase Commit (2PC) transactions and requiring multiple coordinator-to-storage network round-trips.
2. **Transaction Model Mismatch**: Synchronously locking the shared parent-directory attribute record (`num_children`, `modify_time`) collapses concurrent single-directory write throughput by `10x–36x`, while synchronous updates to `QuotaTable` and `SecondaryIndexTable` force unnecessary cross-shard 2PC on every write.
3. **Storage Implementation Mismatch**: Persisting multi-version concurrency control (MVCC) tombstones to disk for transient (`<10ms`) metadata transactions creates a **double-tombstone penalty** (MVCC tombstones + LSM-tree tombstones). Under consecutive bulk TTL deletions, accumulated tombstones degrade `listdir` range-scan latency from `<100ms` to `~6 seconds`.

---

## Three Core Architectural Pillars

### 1. Tree-Topology-Induced Sharding and Execution (`metadb-cluster` & `metadb-tx`)
* **Colocated Schema (`(PID, "_attr_")`)**: `MetaTable` is keyed by `(PID, name)` with `group_key = PID`. A directory's own attributes are stored in a reserved child entry `(PID, "_attr_")` physically adjacent to all of its direct children `(PID, child_name)`.
* **Policy/Size-Based Sharding**: Shards split strictly along `group_key = PID` boundaries so entries of the same directory remain in a single shard, paired with **Jumbo-Shard Isolation Scheduling** and **Dual-Mode Sharding Fallback** for extreme single-directory workloads.
* **Single-Shard Transaction Logic Pushdown**: Executes single-directory Read-Modify-Write (`create`, `mkdir`, `unlink`, `rmdir`, `dirstat`, `listdir`) directly on the shard leader in `1 RTT`, reserving 2PC strictly for cross-directory `rename` (`<3%` of operations).

### 2. Semantic-Aware Off-Critical-Path Processing (`metadb-tx`)
* **Conflict-Free Asynchronous Attribute Aggregation**: Removes parent directory attribute locks from the write critical path. Child mutations append deltas (`delta_children`, `delta_bytes`, `modify_time`) to the Raft log and an in-memory aggregator, while **Read-Path Reconciliation** atomically merges persisted `_attr_` state with un-flushed deltas (`delta.lsn > persisted.applied_lsn`) so `dirstat` is always linearizable.
* **Decoupled Asynchronous Auxiliary Updates**: Updates `QuotaTable` and `SecondaryIndexTable` (`FID -> (PID, name)`) out-of-band via batched asynchronous queues backed by Raft log persistence and per-shard LSN / HLC idempotency.

### 3. Lifecycle-Aware Storage Engine (`metadb-storage`)
* **In-Memory MVCC (`MemMvccStore`)**: Retains multi-version states and MVCC tombstones strictly in memory for the `5-second` transaction window. On flush, stale versions and MVCC tombstones are pruned in memory, writing only the latest value or a single physical LSM tombstone (cutting persistent KV writes by `~60%`).
* **Locality-Aware Compaction (`LocalityCompactor`)**: Tracks tombstone density per spatial prefix bucket and triggers targeted range compaction on hot bulk-deletion ranges to keep `listdir` latency flat.

---

## Single-Node to Trillion-Scale Evolution

MetaDB runs the **exact same storage engine and Raft state machine** from a single-binary embedded node to a 1,000+ node cluster:

* **Stage 1: Single-Node All-in-One (`1-Voter` Raft)**: Zero network coordination; uses the Raft log as the sole durability log (with underlying KV WAL disabled).
* **Stage 2: Dynamic Multi-Shard Split**: Automatically splits shards along `group_key = PID` boundaries as directory metadata grows.
* **Stage 3: Multi-Cell Distributed Cluster (`100B – 10T+` Records)**: Zero-downtime Raft `ConfChange` (`1-Voter -> Learner -> 3-Voter`), decentralized **Hybrid Logical Clocks (HLC)**, stateless **MetaCache**, and a **Two-Tier Control Plane (`GlobalManager` + `CellLocalManager`)** partitioning millions of shards across independent Cells.

---

## Workspace Crates

| Crate | Responsibility |
| :--- | :--- |
| [`crates/metadb-core`](./crates/metadb-core) | `MetaTable`, `QuotaTable`, `SecondaryIndexTable` binary codecs, `(PID, "_attr_")` colocation keys, and decentralized `HybridLogicalClock` (`HLC`). |
| [`crates/metadb-storage`](./crates/metadb-storage) | WAL-less `RaftLog` with three-watermark truncation safety, `MemMvccStore` (In-Memory MVCC), and `LocalityCompactor` (Locality-Aware Compaction). |
| [`crates/metadb-tx`](./crates/metadb-tx) | `PushdownExecutor` (`1-RTT` RMW), `TwoPhaseCommitCoordinator` (cross-shard `rename`), `AsyncAttrAggregator`, and `AsyncAuxiliarySink`. |
| [`crates/metadb-cluster`](./crates/metadb-cluster) | `PolicyShardSplitter` (`group_key = PID`), Jumbo Shard isolation, Raft `ConfChange` (`1-Voter -> 3-Voter`), `GlobalManager`, `CellLocalManager`, and `MetaCache`. |
| [`crates/metadb-proxy`](./crates/metadb-proxy) | `SqlTemplateParser` compiling fixed metadata SQL patterns directly into storage execution commands. |
| [`crates/metadb-server`](./crates/metadb-server) | Unified `MetaDbEngine` orchestrating single-node and distributed multi-Cell deployments. |

---

## Build & Test

```bash
cargo test --workspace
```

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
