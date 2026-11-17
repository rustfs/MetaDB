# MetaDB EGM Design Sitemap

This directory contains the living design specifications and adversarial zero-context review artifacts for **MetaDB** (`rustfs/MetaDB`), authored in English following Google's [Elephant-Goldfish Model (EGM)](https://github.com/google/elephant-goldfish-model) for Spec-Driven Development (SDD).

## Specification & Planning Index

| Artifact | Role / EGM Stage | Scope | Status |
| :--- | :--- | :--- | :--- |
| [`../../Architecture.md`](../../Architecture.md) | Master Architecture | End-to-end 8-crate Rust architecture, three mismatches from `arXiv:2610.32247v1`, capacity math (`100M–10T+`), and implementation status matrix. | Phase 0 Approved |
| [`../DEVELOPMENT_PLAN.md`](../DEVELOPMENT_PLAN.md) | Engineering Roadmap | Phased pure-Rust implementation plan (Phases 1–6), crate dependency DAG, file boundaries, verification commands, and acceptance gates. | Phase 0 Approved |
| [`concepts_brief.md`](./concepts_brief.md) | Upstream Ideation (`Brainstorm`) | Core hypothesis and 6-lens adversarial Goldfish shootdown summary. | Phase 0 Approved |
| [`metadb_architecture.md`](./metadb_architecture.md) | Umbrella EGM Spec (`Draft`) | System-level `FeatureRange` + `DetailedRange` specification tying together the 8 workspace crates. | Phase 0 Approved |
| [`storage_lsm_mvcc.md`](./storage_lsm_mvcc.md) | Subsystem Spec 1 (`Draft`) | `metadb-storage`: `StorageBackend` trait, 4 KiB CRC32C SSTables, Bloom filter, sharded LRU `BlockCache`, `MANIFEST` recovery, `SnapshotGuard` In-Memory MVCC, and bottommost-safe `LocalityCompactor`. | Phase 0 Approved |
| [`consensus_and_transactions.md`](./consensus_and_transactions.md) | Subsystem Spec 2 (`Draft`) | `metadb-raft` & `metadb-tx`: `openraft` Multi-Raft, segmented CRC32C log store, 3-watermark purge gate, `ReadIndex`, `1-RTT` `PushdownExecutor`, `AsyncAttrAggregator` Coordinated Flush Barrier, FIFO `AuxOutbox` + `[0x07]` spill, and crash-recoverable 2PC. | Phase 0 Approved |
| [`sharding_and_control_plane.md`](./sharding_and_control_plane.md) | Subsystem Spec 3 (`Draft`) | `metadb-proto`, `metadb-cluster` & `metadb-proxy`: Unified `[0x01, 0x04)` composite sharding, `ShardEpoch` fencing, Jumbo `DualModeFineGrained` scatter-gather, `GlobalManager`/`CellLocalManager`/`MetaCache`, `IdRangeAllocator`, 20 SQL templates, and Protobuf v1 contracts. | Phase 0 Approved |
| [`review_report.md`](./review_report.md) | Adversarial Audit (`Review & Resolve`) | Independent zero-context Goldfish subagent review findings and specification resolutions. | Phase 0 Approved & Goldfish-Verified |
| [`../README.md`](../README.md) | Multilingual Usage Hub | User and operator documentation index across English (`en`), Japanese (`ja`), French (`fr`), Simplified Chinese (`zh`), and German (`de`). | Phase 0 Active |

The [codec dependency analysis](codec_dependencies.md) records CRC and serialization dependency selection, storage-format compatibility, and review coverage.
