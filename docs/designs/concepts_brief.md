# MetaDB Concept Brief (`/egm-brainstorm`)

* **Project**: `rustfs/MetaDB`
* **Methodology**: [Google Elephant-Goldfish Model (EGM)](https://github.com/google/elephant-goldfish-model)
* **Theoretical Foundation**: Biao Cao et al. (Baidu & Peking University), *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1)

---

## 1. Core Hypothesis

General-purpose distributed transactional databases (such as TiDB, CockroachDB, and FoundationDB) suffer from severe performance degradation when treated as a "black box" for file-system and object-storage metadata workloads due to three architectural mismatches identified in Baidu's production deployment of MetaDB:

1. **Data Model Mismatch**: Flat range/hash sharding fractures entries of the same directory across multiple shards, escalating `>96%` of single-directory metadata operations (`create`, `mkdir`, `unlink`, `listdir`) into expensive multi-shard 2PC transactions and requiring multiple coordinator-to-storage network round-trips.
2. **Transaction Model Mismatch**: Rigid, uniform ACID transactions synchronously lock parent-directory attribute rows (`num_children`, `modify_time`)—causing a `10x` throughput collapse under concurrent single-directory writes—and force synchronous two-phase commit (2PC) for auxiliary metadata (`QuotaTable` and `SecondaryIndexTable`) that can safely tolerate relaxed propagation latency.
3. **Storage Implementation Mismatch**: Persisting MVCC versions and tombstones to disk for short-lived metadata transactions (`<10ms`) creates a "double-tombstone" problem (both MVCC tombstones and LSM-tree tombstones). During TTL-driven consecutive bulk deletions, accumulated tombstones degrade `listdir` scan latency from `<100ms` to `~6s`.

**Hypothesis**: By co-designing a Rust metadata engine around file-system tree topology, semantic-aware off-critical-path processing, and a lifecycle-aware storage engine—while unifying single-node (`1-Voter Raft`) and distributed (`3-Voter Multi-Raft` with a two-tier `Global/Cell Manager`) execution—MetaDB can start as a lightweight single-node embedded/standalone engine and seamlessly scale online to **100 billion – 10+ trillion metadata records** across 1,000+ nodes.

---

## 2. Six-Lens Goldfish Shootdown Summary

During Phase 1 ideation, the concept was challenged across six adversarial lenses:

| Lens | Challenge Raised | Architectural Resolution |
| :--- | :--- | :--- |
| **1. Scale & Bottleneck Critic** | Anchoring a directory (`group_key = PID`) to a single shard creates unsplittable "Jumbo Shards" when a single directory holds hundreds of millions of files. | Implement **Jumbo-Shard Isolation Scheduling** (dedicated node placement by `CellLocalManager`) plus a **Dual-Mode Sharding Fallback** that allows operator/policy-triggered sub-directory hash/range splitting when a single `PID` exceeds node limits. |
| **2. Durability & Crash Recovery Critic** | Disabling the underlying KV WAL while holding MVCC versions and async attribute/quota queues in memory risks data loss or duplicate quota accounting on crash. | Persist all primary writes, attribute deltas, and auxiliary update intents in the **Raft Log** before enqueuing in memory. Hold Raft log truncation to `min(storage_flush_lsn, attr_agg_lsn, aux_sink_lsn)` and deduplicate auxiliary updates using monotonic **Hybrid Logical Clock (HLC)** timestamps. |
| **3. Consistency & Read-Your-Writes Critic** | If parent directory attributes (`num_children`, `modify_time`) are updated asynchronously, `dirstat` immediately after `create` will return stale counts. | Introduce **Read-Path Reconciliation**: `dirstat` merges the persisted `(PID, "_attr_")` record with un-flushed in-memory delta accumulators on the shard leader in `<1µs`, guaranteeing linearizable read-your-writes for directory attributes. |
| **4. Single-Node to Distributed Evolution Critic** | Maintaining separate code paths for standalone single-node WAL vs. distributed Raft causes state-machine divergence during online scale-out. | Unify single-node and distributed modes via **Single-Voter Raft (`ReplicationFactor = 1`)** in Phase 1. Scale-out to 3 nodes uses standard Raft `ConfChange` (`AddLearner` $\rightarrow$ `PromoteVoter`) with zero code-path switching. |
| **5. Control Plane Scalability Critic** | A single central manager tracking heartbeats and routing for millions of shards (required at trillion-record scale) becomes a single point of failure and CPU bottleneck. | Adopt Baidu MetaDB's **Two-Tier Control Plane** (`GlobalManager` managing coarse key-range-to-Cell mappings + `CellLocalManager` handling per-shard heartbeats/splits within each Cell) fronted by a stateless **MetaCache** routing tier and decentralized **HLC** clocks. |
| **6. Storage Compaction Critic** | Time-based TTL compaction rewrites cold SSTables unnecessarily, causing high write amplification. | Implement **Locality-Aware Compaction** that tracks deletion density per key-range bucket in memory and triggers targeted range compaction strictly on narrow key ranges whose tombstone density exceeds a threshold. |

---

## 3. Approved Scope for Phase 2 (`/egm-draft`)

1. Author a comprehensive root `Architecture.md` and EGM specification `docs/designs/metadb_architecture.md` with explicit mathematical capacity modeling for **100 Billion to 10+ Trillion records**.
2. Subject the specification to zero-context Goldfish subagent reviews (`/egm-review`) and document resolutions in `docs/designs/review_report.md`.
3. Implement the modular 6-crate Rust workspace (`metadb-core`, `metadb-storage`, `metadb-tx`, `metadb-cluster`, `metadb-proxy`, `metadb-server`) and verify all invariants via automated test suites.
