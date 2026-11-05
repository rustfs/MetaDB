---
name: metadb_elephant
description: Project context loader for MetaDB following the Google Elephant-Goldfish Model (EGM).
---

# MetaDB Elephant Context Skill

This skill loads the architectural history, foundational literature, and crate boundaries for **MetaDB** (`rustfs/MetaDB`), a trillion-scale, file-system topology-aware distributed metadata database engine written in Rust.

## Core References

1. **Primary Architecture Document**: [`Architecture.md`](../../../Architecture.md)
2. **EGM Design Index**: [`docs/designs/SITEMAP.md`](../../../docs/designs/SITEMAP.md)
3. **EGM Concept Brief**: [`docs/designs/concepts_brief.md`](../../../docs/designs/concepts_brief.md)
4. **EGM Formal Specification**: [`docs/designs/metadb_architecture.md`](../../../docs/designs/metadb_architecture.md)
5. **EGM Goldfish Review Report**: [`docs/designs/review_report.md`](../../../docs/designs/review_report.md)
6. **Foundational Paper**: Biao Cao et al. (Baidu & Peking University), *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, arXiv:2610.32247v1 (2026).

## Architectural Invariants

1. **Data Model Invariant**: `MetaTable` is keyed by `(PID, name)` with `group_key = PID`. Directory attributes are stored in a reserved child entry `(PID, "_attr_")` so parent attributes and direct children remain physically colocated in the same shard.
2. **Consensus & Durability Invariant**: RocksDB/LSM native WAL is disabled (`disable_wal = true`). Durability relies exclusively on the Raft log (`1-Voter` Raft in single-node mode; `3-Voter` Multi-Raft in distributed mode). Raft log truncation is strictly bounded by the minimum of the storage flush watermark, the async attribute aggregation watermark, and the async auxiliary sink watermark.
3. **Off-Critical-Path Invariant**: Hotspot directory attributes (`num_children`, `modify_time`, `size_bytes`) are aggregated asynchronously in memory and reconciled on the read path (`persisted + pending`). Auxiliary tables (`QuotaTable`, `SecondaryIndexTable`) are updated asynchronously out-of-band with HLC-based idempotency.
4. **Lifecycle-Aware Storage Invariant**: Multi-version states and MVCC tombstones reside strictly in memory (`In-Memory MVCC`) for the short transaction timeout window (`5s`), flushing only the latest committed state or a single physical LSM tombstone, paired with density-driven `Locality-Aware Compaction`.
