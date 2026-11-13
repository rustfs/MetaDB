# MetaDB

**MetaDB** is a high-performance, 100% pure-Rust, file-system structure-aware distributed transactional metadata database engine built for [RustFS](https://github.com/rustfs).

* **User & Operator Guides**: [Documentation Hub](./docs/README.md) — [English](./docs/guides/en/README.md) | [日本語](./docs/guides/ja/README.md) | [Français](./docs/guides/fr/README.md) | [简体中文](./docs/guides/zh/README.md) | [Deutsch](./docs/guides/de/README.md)
* **Architecture & Engineering**: [`Architecture.md`](./Architecture.md) | [`docs/DEVELOPMENT_PLAN.md`](./docs/DEVELOPMENT_PLAN.md) | [`docs/designs/SITEMAP.md`](./docs/designs/SITEMAP.md) | [`docs/designs/review_report.md`](./docs/designs/review_report.md)

> **Academic & Architectural Attribution**:
> The architecture of `rustfs/MetaDB` is inspired by and adapted from Baidu's trillion-scale production metadata center paper:
> **Biao Cao, Kang Chen, Zhihui Yin, Qiushi Chen, Jielong Jian, Jiwei Xiong, Xinxing Wang, Yan Xing, Liguo Duan, Ran Zheng, Xianjun Meng, and Zhenyu Hou**, *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1) (Baidu & Peking University, 2026).
> *(Note: Trillion-record production figures cited from `arXiv:2610.32247v1` reflect Baidu's production deployment; see [Current Implementation Status](#current-implementation-status) below for `rustfs/MetaDB` repository milestones.)*
>
> **Spec-Driven Development Methodology**:
> Designed, stress-tested, and verified using Google's [Elephant-Goldfish Model (EGM)](https://github.com/google/elephant-goldfish-model).

---

## Why MetaDB?

General-purpose distributed databases (such as TiDB, CockroachDB, and FoundationDB) treat file-system and object-storage metadata as generic flat key-value or relational rows. At exabyte scale, this "black-box" approach creates three severe bottlenecks:

1. **Data Model Mismatch**: Flat hash or byte-midpoint sharding fractures entries of the same directory across multiple shards, turning `>96%` of localized single-directory operations (`create`, `mkdir`, `unlink`, `listdir`) into multi-shard Two-Phase Commit (2PC) transactions and incurring extra network round-trips.
2. **Transaction Model Mismatch**: Synchronously locking the shared parent-directory attribute record (`num_children`, `size_bytes`, `modify_time`) collapses concurrent single-directory write throughput by `10x–36x`, while synchronous updates to `QuotaTable` and `SecondaryIndexTable` force unnecessary cross-shard 2PC on every write.
3. **Storage Implementation Mismatch**: Persisting multi-version concurrency control (MVCC) tombstones to disk for transient (`<10ms`) metadata transactions creates a **double-tombstone penalty** (MVCC tombstones + LSM-tree tombstones). Under consecutive bulk TTL deletions, accumulated tombstones degrade `listdir` range-scan latency from `<100ms` to `~6 seconds`.

---

## Three Core Architectural Pillars

### 1. Tree-Topology-Induced Sharding and Execution (`metadb-cluster` & `metadb-tx`)
* **Colocated Schema (`(PID, "_attr_")`)**: `MetaTable` is keyed by `(PID, name)` with `group_key = PID`. A directory's own attributes are stored in a reserved child entry `(PID, "_attr_")` physically adjacent to its direct children `(PID, child_name)`.
* **Epoch-Fenced `PID`-Boundary Sharding & Jumbo Defense**: Shards split strictly along `group_key = PID` boundaries under `ShardEpoch` fencing, paired with **Jumbo-Shard Dedicated-Node Isolation** and **`DualModeFineGrained` `(PID, name)` Splitting** (using shard-local `DirShardDelta` records for `1-RTT` writes and `1-hop` scatter-gather `dirstat`).
* **Single-Shard Pushdown + Crash-Recoverable 2PC**: Executes single-directory Read-Modify-Write (`create`, `unlink`, `setattr`, `lookup`, `dirstat`, `listdir`, and colocated `mkdir`/`rmdir`) in `1 RTT` on the shard leader, reserving Primary-Anchored 2PC (`WriteIntent` + `TxnRecord`) for cross-shard `rename` (`<3%` of workload) and cross-shard `mkdir`/`rmdir`.

### 2. Semantic-Aware Off-Critical-Path Processing (`metadb-tx`)
* **Conflict-Free Asynchronous Attribute Aggregation**: Removes parent directory attribute locks from the foreground write path. Child mutations append deterministic deltas to `AsyncAttrAggregator`, folded into `(PID, "_attr_")` at the L0 SSTable **Coordinated Flush Barrier** while **Read-Path Reconciliation** guarantees linearizable `dirstat` reads.
* **Exact-Once Asynchronous `AuxOutbox`**: Propagates `QuotaTable` and `SecondaryIndexTable` (`FID -> (PID, name)`) updates in strict FIFO per-shard LSN batches with `[0x07]` local overflow spill so Raft log compaction is never blocked by remote partitions.

### 3. Lifecycle-Aware Pure-Rust Storage Engine (`metadb-storage` & `metadb-raft`)
* **WAL-less LSM + 3-Watermark OpenRaft**: Disables local KV WAL files in favor of segmented CRC32C Raft logs (`openraft`), clamping log purge to `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)`.
* **`SnapshotGuard` In-Memory MVCC**: Retains multi-version chains in memory for the `5s` transaction window, gating unversioned L0 SSTable collapse-flushes on `ActiveSnapshotRegistry` (`min_active_snapshot_hlc >= imm.max_hlc`) to eliminate persisted MVCC tombstones (`~60%` fewer KV writes) with zero future-read anomalies.
* **Bottommost-Safe Locality-Aware Compaction**: Tracks tombstone density per 9-byte prefix bucket and compacts hot deletion ranges down to the deepest overlapping SSTable level to keep `listdir` latency flat.

---

## Single-Node to Multi-Cell Evolution

MetaDB runs the **exact same `StorageBackend` and `openraft` state machine** from an embedded single-binary node to a multi-Cell cluster:

* **Stage 1 — Single-Node All-in-One (`1-Voter` OpenRaft)**: Zero network coordination; sequential Raft log persistence + embedded Rust SDK or `metadbd --role all-in-one`.
* **Stage 2 — Dynamic Multi-Shard Split**: Automatically splits shards along `group_key = PID` boundaries on a single host.
* **Stage 3 — Multi-Cell Distributed Cluster (`3-Voter` Multi-Raft)**: Online `AddLearner` -> SSTable snapshot catch-up -> joint-consensus `Voter` promotion, decentralized `HLC` clocks, stateless `MetaCache`, and a **Two-Tier Control Plane (`GlobalManager` + `CellLocalManager`)**.

---

## 8-Crate Pure-Rust Workspace Architecture

| Crate | Status | Responsibility |
| :--- | :--- | :--- |
| `crates/metadb-core` | Prototype (`v0.1.0`) -> Phase 1 | `0x01..0x07` binary codecs, `(PID, "_attr_")` keys, `ShardEpoch`, CRC32C, `HLC` (+ drift guard). |
| `crates/metadb-proto` | Planned (Phase 1) | Protobuf v1 schemas & `tonic`/`prost` gRPC contracts (`MetadataService`, `TransactionService`, `RaftTransportService`, `ControlPlaneService`). |
| `crates/metadb-storage` | Prototype (`v0.1.0`) -> Phase 2 | `StorageBackend` trait, pure-Rust WAL-less SSTable LSM, `MANIFEST`, `BlockCache`, Bloom filter, `SnapshotGuard` MVCC, `LocalityCompactor`. |
| `crates/metadb-raft` | Planned (Phase 3) | `openraft` Multi-Raft, segmented CRC32C log store, 3-watermark purge gate, batched `ReadIndex`, `1->3` Voter `ConfChange`. |
| `crates/metadb-tx` | Prototype (`v0.1.0`) -> Phase 4 | Deterministic `RaftStateMachine`, `1-RTT` `PushdownExecutor`, `AsyncAttrAggregator`, FIFO `AuxOutbox` + `[0x07]` spill, crash-recoverable 2PC. |
| `crates/metadb-cluster` | Prototype (`v0.1.0`) -> Phase 5 | `PID` & `DualModeFineGrained` splitter, `ShardEpoch` fencing, Jumbo isolation, `GlobalManager`, `CellLocalManager`, `MetaCache`, `IdRangeAllocator`. |
| `crates/metadb-proxy` | Prototype (`v0.1.0`) -> Phase 5 | Tokenizing compiler for all 20 parameterized metadata SQL templates + Epoch-aware router. |
| `crates/metadb-server` | Prototype (`v0.1.0`) -> Phase 6 | Embedded `MetaDbEngine` SDK + `metadbd` daemon + `metadb-ctl` operator CLI. |

---

## Current Implementation Status

* **Completed**:
  * **Phase 0**: Complete English Architecture & Zero-Decision EGM Subsystem Specifications, Adversarial Goldfish Design Review (`docs/designs/review_report.md`), Phased Rust Development Plan (`docs/DEVELOPMENT_PLAN.md`), and Multilingual Usage Guides (`en`, `ja`, `fr`, `zh`, `de`).
  * **Baseline Prototype (`v0.1.0-prototype`)**: 6-crate synchronous in-memory verification baseline passing `cargo test --workspace`.
* **Next Up (Phases 1–6)**: Step-by-step pure-Rust implementation of `metadb-core`/`metadb-proto` (Phase 1), WAL-less disk LSM + `SnapshotGuard` MVCC (Phase 2), `openraft` Multi-Raft (Phase 3), replicated transactions & 2PC recovery (Phase 4), distributed sharding/control plane & 20 SQL templates (Phase 5), and `metadbd` + `metadb-ctl` binaries (Phase 6).

---

## Build & Test

```bash
cargo test --workspace
```

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
