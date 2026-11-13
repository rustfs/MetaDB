---
name: metadb_elephant
description: Project context loader for MetaDB following the Google Elephant-Goldfish Model (EGM).
---

# MetaDB Elephant Context Skill

This skill loads the architectural history, foundational literature, subsystem contracts, and crate boundaries for **MetaDB** (`rustfs/MetaDB`), a 100% pure-Rust, file-system structure-aware distributed metadata database engine built for [RustFS](https://github.com/rustfs).

## Core References

1. **Primary Architecture Specification**: [`Architecture.md`](../../../Architecture.md)
2. **Phased Rust Development Plan (Phases 1–6)**: [`docs/DEVELOPMENT_PLAN.md`](../../../docs/DEVELOPMENT_PLAN.md)
3. **EGM Design Index**: [`docs/designs/SITEMAP.md`](../../../docs/designs/SITEMAP.md)
4. **EGM Concept Brief**: [`docs/designs/concepts_brief.md`](../../../docs/designs/concepts_brief.md)
5. **EGM Umbrella & Modular Subsystem Specs**:
   * [`docs/designs/metadb_architecture.md`](../../../docs/designs/metadb_architecture.md)
   * [`docs/designs/storage_lsm_mvcc.md`](../../../docs/designs/storage_lsm_mvcc.md)
   * [`docs/designs/consensus_and_transactions.md`](../../../docs/designs/consensus_and_transactions.md)
   * [`docs/designs/sharding_and_control_plane.md`](../../../docs/designs/sharding_and_control_plane.md)
6. **EGM Goldfish Review Report**: [`docs/designs/review_report.md`](../../../docs/designs/review_report.md)
7. **Multilingual Usage Documentation (`en`, `ja`, `fr`, `zh`, `de`)**: [`docs/README.md`](../../../docs/README.md)
8. **Foundational Paper**: Biao Cao et al. (Baidu & Peking University), *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, arXiv:2610.32247v1 (2026).

## Target 8-Crate Pure-Rust Workspace

* `metadb-core`: `HLC` (+ drift guard), `0x01..0x07` binary codecs, `CompositeKeyBound`, `ShardEpoch`, CRC32C, errors.
* `metadb-proto`: Protobuf v1 schemas and `tonic`/`prost` gRPC contracts (`MetadataService`, `TransactionService`, `RaftTransportService`, `ControlPlaneService`).
* `metadb-storage`: `StorageBackend` trait, pure-Rust WAL-less SSTable LSM (4 KiB CRC32C blocks, Bloom filters including physical tombstones, 16-shard LRU `BlockCache`, atomic `MANIFEST`), `SnapshotGuard`-gated `MemMvccStore`, and bottommost-safe `LocalityCompactor`.
* `metadb-raft`: `openraft` Multi-Raft (`1-Voter` single-node to `3-Voter` distributed), segmented CRC32C log store, 3-watermark purge gate, `ReadIndex` batcher, and online `ConfChange`.
* `metadb-tx`: Deterministic `RaftStateMachine`, `1-RTT` `PushdownExecutor`, `AsyncAttrAggregator` (Coordinated Flush Barrier + Read-Path Reconciliation), FIFO `AuxOutbox` + `[0x07]` local spill, and Primary-Anchored crash-recoverable 2PC (`Rename`, cross-shard `Mkdir`/`Rmdir`).
* `metadb-cluster`: Unified `[0x01, 0x04)` sharding, `ShardEpoch` fencing, `PID`-boundary & `DualModeFineGrained` (`[0x04] DirShardDelta`) splitting, Jumbo dedicated-node isolation, `GlobalManager`, `CellLocalManager`, `MetaCache`, and `IdRangeAllocator`.
* `metadb-proxy`: Tokenizing compiler for all 20 parameterized metadata SQL templates + Epoch-aware stateless router.
* `metadb-server`: Embedded `MetaDbEngine` SDK + `metadbd` multi-role daemon + `metadb-ctl` operator CLI.

## Implementation Status Policy

* **Current Baseline**: Phase 0 Approved Specification Suite + `v0.1.0-prototype` (initial 6-crate synchronous in-memory prototype).
* **Phases 1–6 Execution**: Always consult `docs/DEVELOPMENT_PLAN.md` and keep implementation status badges synchronized across `Architecture.md`, `README.md`, and `docs/guides/{en,ja,fr,zh,de}/` as each phase completes.

## Non-Negotiable Architectural Invariants

1. **Data Model Invariant**: `MetaTable` (`0x01`) is keyed by `(PID, name)` with `group_key = PID` and colocated `(PID, "_attr_")` (`88B` CRC32C binary layout including `parent_pid` and `applied_raft_index`). User entry names equal to `"_attr_"`, empty, or containing `'\0'` are rejected.
2. **Consensus & Durability Invariant**: Local KV WAL is disabled. Durability before SSTable flush relies solely on `metadb-raft`'s segmented CRC32C log + durable `HARD_STATE.last_committed`. Physical log purge is clamped to `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)`.
3. **Coordinated Off-Critical-Path Invariant**: `AsyncAttrAggregator` folds deltas in `(baseline.applied_raft_index, M]` via `ViewAtRaftIndex(M)` inside the L0 SSTable seal barrier before `state_flushed_index` advances to `M`. `AuxOutbox` keys tasks uniquely by `(origin_shard_id, origin_raft_index, task_ordinal)`, advances watermarks strictly along contiguous coverage frontiers, spills overdue tasks to `[0x07]`, and cleans acknowledged `[0x07]` keys via `RaftCmd::AckAuxTasks`.
4. **`SnapshotGuard` Storage Invariant**: `ImmMemTable`s collapse-flush unversioned entries/tombstones to L0 SSTables strictly when `min_active_snapshot_hlc >= imm.max_hlc` (`<= 5,000ms` hard timeout), and `LocalityCompactor` drops physical tombstones strictly at the bottommost overlapping SSTable level for the compacted prefix bucket.
