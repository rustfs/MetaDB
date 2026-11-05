# MetaDB Architecture Specification

* **Project**: `rustfs/MetaDB`
* **Status**: Approved & Active
* **Design Methodology**: [Google Elephant-Goldfish Model (EGM)](https://github.com/google/elephant-goldfish-model) for Spec-Driven Development (SDD)
* **Primary Theoretical Reference**: This architecture is directly inspired by and adapted from Baidu's trillion-scale production metadata database paper:
  > **Biao Cao, Kang Chen, Zhihui Yin, Qiushi Chen, Jielong Jian, Jiwei Xiong, Xinxing Wang, Yan Xing, Liguo Duan, Ran Zheng, Xianjun Meng, and Zhenyu Hou.**
  > *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, arXiv:2610.32247v1 (Baidu & Peking University, 2026). [https://arxiv.org/html/2610.32247v1](https://arxiv.org/html/2610.32247v1)

---

## 1. Executive Summary & Foundational Attribution

Modern exabyte-scale storage systems—including flat object storage (S3/BOS), hierarchical object namespaces (HDFS-compatible Mantle), and POSIX distributed file systems (CFS)—increasingly delegate metadata management to distributed transactional databases. However, Baidu's five-year production experience operating clusters across **1,000+ metadata machines**, **tens of trillions of metadata records**, and **exabytes of storage** demonstrates a fundamental lesson: **treating a general-purpose distributed database (such as TiDB or CockroachDB) as a "black box" for file-system metadata leaves 2.2x–4.1x throughput untapped and wastes ~50% of cluster hardware.**

**MetaDB** (`rustfs/MetaDB`) is a Rust implementation of a file-system structure-aware, semantic-aware, and lifecycle-aware transactional metadata engine. It is engineered from day one to support **seamless evolution from an embedded/standalone single-node deployment (`1-Voter Raft`) to a multi-cell distributed cluster (`3-Voter Multi-Raft`) capable of managing 100 billion to 10+ trillion metadata records.**

---

## 2. Problem Statement: Three Architectural Mismatches

Following the empirical findings of the Baidu MetaDB paper (`arXiv:2610.32247v1`, §2.3), general-purpose distributed databases suffer from three structural mismatches when serving file-system and object-storage metadata:

### 2.1 Data Model Mismatch (Flat Sharding vs. Tree Topology)
* **Observation**: In production file-system workloads, **>96%–97% of directory-tree-modifying operations** (`create`, `mkdir`, `unlink`, `rmdir`, `listdir`) are single-directory operations that touch only a target child entry and its immediate parent directory's attributes. Cross-directory `rename` accounts for `<3%`.
* **Bottleneck**: General-purpose databases partition tables by arbitrary size thresholds or flat hash boundaries. This fractures entries belonging to the same parent directory (`PID`) across multiple shards, escalating localized single-directory operations into expensive multi-shard Two-Phase Commit (2PC) transactions (`100%` multi-shard transaction ratio in unoptimized deployments) and requiring multiple network round-trips (RTTs) between stateless SQL proxies and storage nodes.

### 2.2 Transaction Model Mismatch (Rigid ACID vs. Differentiated Semantics)
* **Observation**: Modern AI/ML training pipelines (e.g., Kubeflow checkpoints) concurrently create tens of thousands of files inside the same directory.
* **Bottleneck 1 (Parent Attribute Contention)**: Enforcing synchronous row locks on the shared parent-directory record to update `num_children`, `size_bytes`, and `modify_time` causes throughput to collapse by **10x** (from `~60K QPS` to `~6K QPS` in Baidu's Generation-2 database, and a `36.2x` gap vs. TiDB).
* **Bottleneck 2 (Over-Strict Auxiliary Atomicity)**: Updating user quotas (`QuotaTable`) and reverse data-plane indexes (`SecondaryIndexTable`) inside the same ACID transaction as primary metadata forces cross-shard 2PC on every write, even though billing, quota monitoring, and garbage-collection indexes safely tolerate milliseconds-to-minutes of asynchronous propagation delay.

### 2.3 Storage Implementation Mismatch (Long-Lived MVCC vs. Transient Metadata Lifecycle)
* **Observation**: Metadata transactions are overwhelmingly short-lived (`<10ms`), whereas object storage buckets routinely undergo massive consecutive range deletions driven by Time-To-Live (TTL) expirations on creation-time-prefixed keys.
* **Bottleneck**: Traditional LSM-tree + MVCC engines persist both logical MVCC tombstones and physical LSM-tree tombstones to disk ("double-tombstone penalty") to serve hypothetical long-running analytical queries. Passive level-saturation compaction fails to purge localized deletion ranges promptly, causing foreground `listdir` scans (1,000 items) to traverse millions of dead tombstones and spiking latency from `<100ms` to **~6 seconds**.

---

## 3. Goals & Non-Goals

### 3.1 Goals
1. **Topology-Aligned Single-Shard Execution**: Keep `>96%` of directory modifications strictly within a single shard via `group_key = PID` colocation and execute the entire Read-Modify-Write (RMW) sequence in a single storage-node hop (`1 RTT`).
2. **Conflict-Free High-Concurrency Directory Writes**: Eliminate lock contention on hotspot parent directory attributes via in-memory delta/timestamp aggregation paired with linearizable **Read-Path Reconciliation**.
3. **Zero Double-Tombstone Storage & Sub-Millisecond `listdir`**: Prevent short-lived MVCC versions/tombstones from flushing to disk (`In-Memory MVCC`, cutting KV writes by `~60%`) and proactively compact dense tombstone key ranges (`Locality-Aware Compaction`).
4. **Seamless Single-Node to Trillion-Scale Evolution**: Run identically as a single-binary `1-Voter Raft` standalone engine and scale out online—without data-model migration—to a two-tier `GlobalManager + CellLocalManager` cluster supporting **100B–10T+ records**.
5. **Lightweight SQL Template Compatibility**: Support the `~20` fixed metadata SQL patterns through direct pattern compilation without the CPU overhead of a general-purpose cost-based SQL optimizer.

### 3.2 Non-Goals
1. **General-Purpose OLAP / Long-Running Analytical Queries**: Transactions exceeding the `5-second` in-memory MVCC retention window are rejected by design; offline analytics must run against exported snapshots.
2. **Arbitrary Multi-Table SQL Joins**: Only the three canonical metadata tables (`MetaTable`, `QuotaTable`, `SecondaryIndexTable`) and their fixed access patterns are supported.
3. **Data-Plane Block/Chunk Storage**: MetaDB manages namespace, attributes, quotas, and `FID -> (PID, name)` secondary indexes; raw object payload bytes reside in the RustFS data plane.

---

## 4. Core Data Model & Schema Design

Following Section 3.1 of the Baidu MetaDB paper, MetaDB organizes all state into three range-partitioned tables ordered lexicographically by binary primary key:

```text
+--------------------------------------------------------------------------------------------------+
| 1. MetaTable (Primary Metadata)                                                                  |
|    Primary Key : (PID: u64 [big-endian], name: String)                                           |
|    Sharding Key: group_key = PID (anchors parent attributes and all direct children together)    |
|    Value Fields: id (InodeId: u64), entry_type (File | Directory | Attr), link_count (u32),      |
|                  num_children (i64), size_bytes (i64), modify_time (HLC), create_time (HLC),     |
|                  uid (u64), mode (u32), fid (Option<FileId>)                                     |
|    Special Row : (PID, "_attr_") stores the attributes of directory `PID` colocated with its     |
|                  children `(PID, child_name)`.                                                   |
+--------------------------------------------------------------------------------------------------+
| 2. QuotaTable (Auxiliary Metadata)                                                               |
|    Primary Key : (UID: u64 [big-endian])                                                         |
|    Value Fields: used_bytes (i64), used_inodes (i64), quota_limit_bytes (u64),                   |
|                  last_applied_hlc (HLC) [for at-most-once asynchronous deduplication]            |
+--------------------------------------------------------------------------------------------------+
| 3. SecondaryIndexTable (Auxiliary Metadata)                                                      |
|    Primary Key : (FID: u64 [big-endian])                                                         |
|    Value Fields: pid (u64), name (String), create_time (HLC), last_applied_hlc (HLC)             |
+--------------------------------------------------------------------------------------------------+
```

### Why `(PID, "_attr_")` Colocation Matters
Because ASCII `"_attr_"` sorts deterministically alongside `(PID, child_name)` under the same `PID` prefix, a directory's own attribute record `(PID, "_attr_")` and all of its direct children `(PID, "file_a")`, `(PID, "file_b")` reside in the **exact same physical shard** whenever shards split along `group_key = PID` boundaries.

---

## 5. System Architecture & The Three Core Pillars

```mermaid
flowchart TB
    Client["Storage Client (CFS / BOS / Mantle / RustFS)"]
    Proxy["Stateless Proxy Layer\n(~20 Fixed SQL Templates -> Direct RPC Plan)"]
    MetaCache["MetaCache Cluster\n(High-Speed Shard Routing Cache)"]
    GM["Global Manager\n(Key-Range -> Cell Mapping & Cross-Cell Rebalance)"]
    CLM["Cell Local Manager\n(Per-Shard Heartbeats, PID Boundary Split, Jumbo Isolation)"]

    subgraph BE["Backend Storage Node (BE) - Multi-Raft Leader"]
        Raft["Raft Consensus Log (Single-Voter or 3-Voter)\n[Sole Durability Log; RocksDB/LSM WAL Disabled]"]
        Pushdown["Single-Shard Pushdown Executor (1-RTT RMW)"]
        AttrAgg["Conflict-Free Async Attribute Aggregator\n(In-Memory Delta/HLC Queue + Read Reconciliation)"]
        AuxSink["Decoupled Async Auxiliary Sink\n(Batched Quota & Secondary Index Dispatcher)"]
        MemMVCC["In-Memory MVCC Layer\n(5s Transient Version Window; Prunes MVCC Tombstones on Flush)"]
        LocComp["Locality-Aware Compaction Engine\n(Range Tombstone Density Tracker + Targeted Compaction)"]
        LSM["Persistent Sorted KV / LSM Store (Single Instance per Disk)"]
    end

    Client --> Proxy
    Proxy <--> MetaCache
    MetaCache -. "Cache Miss" .-> CLM
    CLM <--> GM
    Proxy == "1-RTT Pushdown / 2PC Rename" ==> Pushdown
    Pushdown --> Raft
    Raft --> MemMVCC
    Raft --> AttrAgg
    Raft --> AuxSink
    AttrAgg -- "Batch Flush Deltas" --> MemMVCC
    Pushdown -. "Read-Path Reconciliation" .-> AttrAgg
    MemMVCC -- "Latest Committed Value / Single LSM Tombstone" --> LSM
    LocComp -- "Targeted Range Compaction" --> LSM
    AuxSink == "Idempotent Batch RPC (HLC Dedup)" ==> Proxy
```

### Pillar ①: Tree-Topology-Induced Sharding and Execution (`metadb-cluster` & `metadb-tx`)
1. **Policy/Size-Based Sharding on `group_key = PID`**:
   * When a shard's size exceeds `shard_split_threshold_bytes` (default `4 GiB` in production, configurable down to `64 KiB` in tests), the splitter scans for a clean `PID` boundary near the midpoint rather than slicing mid-directory.
   * **Jumbo Shard Handling**: If a single directory (`PID`) grows into an unsplittable Jumbo Shard (e.g., 100M–900M entries, up to `180 GiB`):
     1. *Jumbo-Shard Isolation*: `CellLocalManager` flags the shard as `JumboIsolated`, migrating peer replicas off the host so the jumbo directory exclusively utilizes a dedicated node's CPU and NVMe I/O.
     2. *Dual-Mode Split Fallback*: If a jumbo directory exceeds a single node's physical capacity, operators or policy can switch that specific `PID` into `DualModeSplit`, allowing splits on `(PID, name)` boundaries and automatically falling back to 2PC for operations touching that directory.
2. **Single-Shard Transaction Logic Pushdown**:
   * Instead of a proxy-coordinated read phase followed by a write phase (`>=2 RTTs`), single-directory operations (`create`, `mkdir`, `unlink`, `rmdir`, `dirstat`, `listdir`) are dispatched as a single self-contained `PushdownOp` to the shard leader (`1 RTT`).
   * Only cross-directory `rename(src_pid, src_name, dst_pid, dst_name)` when `shard(src_pid) != shard(dst_pid)` invokes the built-in HLC-ordered Two-Phase Commit (`2PC`) coordinator.

### Pillar ②: Semantic-Aware Off-Critical-Path Processing (`metadb-tx`)
1. **Conflict-Free Asynchronous Attribute Aggregation**:
   * Creating or deleting a child entry under `PID` **does not lock** `(PID, "_attr_")`.
   * Instead, the mutation appends an `AttrDelta { pid, delta_children: +1/-1, delta_bytes: +size/-size, modify_time: hlc }` to the local Raft log and enqueues it in the in-memory `AsyncAttrAggregator`.
   * Background workers periodically fold all queued deltas for each `PID` (`sum(delta_children)`, `sum(delta_bytes)`, `max(modify_time)`) and apply a single batched update to `(PID, "_attr_")`.
   * **Read-Path Reconciliation**: Every `InodeAttr` (`_attr_`) stores an `applied_lsn` watermark, and every queued `AttrDelta` records its originating Raft `lsn`. Any `dirstat(PID)` or `get_attr(PID)` call reads the persisted `(PID, "_attr_")` from `MemMvccStore` and folds strictly those un-flushed deltas in `AsyncAttrAggregator` with `delta.lsn > persisted_attr.applied_lsn`. Furthermore, `rmdir` performs an authoritative prefix scan over live children `(target_dir_id, *)` (excluding the reserved `"_attr_"` key) rather than relying on asynchronous `num_children`. Clients always observe strictly up-to-date, linearizable directory attributes without write-path lock contention.
2. **Decoupled Asynchronous Auxiliary Updates**:
   * Primary metadata mutations emit `AuxUpdateTask` records (`QuotaDelta` for `UID` and `IndexUpsert`/`IndexDelete` for `FID`) into the local Raft log and push them to `AsyncAuxiliarySink`.
   * The sink drains tasks in destination-grouped batches. Each task carries its originating `(origin_shard_id, raft_lsn, hlc)`: `QuotaTable` tracks per-shard LSN watermarks (`applied_lsn_by_shard`) so additive quota deltas are applied **exactly once**, while `SecondaryIndexTable` enforces monotonic `HLC` generation checks so out-of-order `rename` index updates never overwrite newer locations.

### Pillar ③: Lifecycle-Aware Storage Engine (`metadb-storage`)
1. **In-Memory MVCC (`MemMvccStore`)**:
   * Every active key maintains a short version chain in memory indexed by `HLC`.
   * Because metadata transactions complete in `<10ms` (with a hard timeout of `5,000ms`), MemTables are retained in memory for at least `tx_timeout_ms` (`5s`) before flushing.
   * **Flush Pruning Rule**: When flushing an immutable MemTable to the persistent LSM store:
     * Stale historical versions are discarded in memory.
     * If the latest version is `Value(v)`, only `Put(key, v)` is written to persistent storage.
     * If the latest version is `MvccTombstone`, the MVCC tombstone is **not** written as a versioned value; instead, a single physical LSM `Delete(key)` tombstone is emitted. This eliminates the "double-tombstone" problem and cuts persistent KV writes by `~60%`.
2. **Locality-Aware Compaction (`LocalityCompactor`)**:
   * The storage engine partitions the keyspace into lightweight prefix/range buckets and tracks the ratio of physical LSM tombstones to live keys per range.
   * When consecutive bulk deletions (such as TTL expiration of time-prefixed objects) push a range's tombstone count above `tombstone_density_threshold`, `LocalityCompactor` immediately compacts **only that narrow `[start_key, end_key)` range**, reclaiming dead tombstones in milliseconds and keeping `listdir` scan latency flat (`<1ms–4ms`).

---

## 6. Scaling Analysis: Can MetaDB Reach 100 Billion to 10+ Trillion Scale?

**Yes.** Both Baidu's 5-year production deployment (`arXiv:2610.32247v1`, Table 1) and our architectural capacity model prove that this design scales smoothly from **1 node to 1,000+ nodes** and from **thousands of records to 10+ trillion (`10^13`) records**.

### 6.1 Empirical Production Proof (Baidu Paper Table 1)
In Baidu's production environment, single MetaDB clusters already operate at trillion-record scale:
* **BOS2 Cluster**: **575 BE nodes**, managing **1.848 trillion (`1.848 × 10^12`) metadata records** and **909 PB** of storage, sustaining **2.063 Million read QPS** and **747K write QPS**.
* **BOS1 & BOS3 Clusters**: **414 and 406 BE nodes**, managing **1.343 trillion** and **1.356 trillion records** respectively.
* **Fleet Capacity**: Scales to **>1,000 metadata nodes** and **tens of trillions of records** in a single cluster while cutting machine count by `50%` compared to TiDB/CockroachDB-style general-purpose databases.

### 6.2 Quantitative Capacity Model for 100B – 10T Records

| Scale Tier | Total Records | Avg Record Size (Compressed) | Raw Data Volume | 3-Replica Physical Storage | Shards (@ 4 GiB / Shard) | Cluster Size (@ 2×4TB NVMe / Node) | Control Plane Topology |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Single-Node Starter** | `100 Million` (`10^8`) | `~180 B` | `~18 GB` | `~18 GB` (1-Voter) | `5–16 Shards` | **1 Node** | Embedded `CellLocalManager` |
| **100 Billion (`10^11`)** | `100 Billion` | `~180 B` | `~18 TB` | `~54 TB` (3-Voter) | `~4,500 Shards` | **12–24 Nodes** | 1 Cell + `MetaCache` |
| **1 Trillion (`10^12`)** | `1 Trillion` | `~180 B` | `~180 TB` | `~540 TB` (3-Voter) | `~45,000 Shards` | **120–200 Nodes** | `GlobalManager` + 4 Cells |
| **10 Trillion (`10^13`)** | `10 Trillion` | `~180 B` | `~1.8 PB` | `~5.4 PB` (3-Voter) | `~450,000 Shards` | **800–1,200 Nodes** | `GlobalManager` + 20 Cells |

### 6.3 Why the Architecture Does Not Hit a Scaling Wall at Trillion Scale
1. **Data Plane Scale-Out (Zero Cross-Shard Coordination for 96%+ Ops)**: Because `group_key = PID` colocation + asynchronous auxiliary updates reduce multi-shard transactions from `100%` to `0.79%–4.37%`, throughput scales **linearly** with the number of BE nodes without 2PC lock-table amplification.
2. **Control Plane Scale-Out (Two-Tier `GlobalManager` + `CellLocalManager`)**: A single Raft manager struggles above ~100K shards due to heartbeat storms. By partitioning the cluster into independent **Cells** (each managing `20K–50K` shards via its own `CellLocalManager`), the `GlobalManager` only maintains a tiny table of contiguous key-range-to-Cell mappings (`<1,000` entries), completely eliminating control-plane bottlenecks at 1M+ shards.
3. **Timestamp Scale-Out (Decentralized HLC vs. Centralized TSO)**: Replacing a central Timestamp Oracle (TSO) with **Hybrid Logical Clocks (HLC)** removes the network-wide timestamp allocation bottleneck and single point of write unavailability.
4. **Routing Scale-Out (Stateless Proxy + `MetaCache`)**: Clients and proxies cache `(PID -> ShardId -> LeaderNode)` mappings in `MetaCache`, achieving `>99.9%` single-hop routing accuracy and shielding managers from restart thundering herds.

---

## 7. Seamless Three-Stage Evolution: Single-Node to Distributed Cluster

MetaDB uses the **exact same engine, storage format, and Raft state machine** across all three deployment stages:

### Stage 1: Single-Node All-in-One (`ReplicationFactor = 1`)
* `Proxy`, `MetaCache`, `CellLocalManager`, and `BE` run in a single process with in-memory channel/direct calls.
* Uses a **`1-Voter` Raft Group** (`SingleNodeRaft`) rather than a separate non-Raft code path. Why? Because RocksDB/LSM WAL is disabled; using the same Raft log for persistence ensures that crash recovery, `In-Memory MVCC` watermarks, and `AsyncAttrAggregator` / `AsyncAuxiliarySink` log replay behave identically in single-node and distributed modes.

### Stage 2: Single-Node Multi-Shard Split
* Starts with a single root shard covering `PID` range `[0, u64::MAX]`.
* As directory entries grow past `shard_split_threshold`, `CellLocalManager` dynamically splits shards along `PID` boundaries on the same node.

### Stage 3: Online Scale-Out to Multi-Node & Multi-Cell (`1 -> 3 -> N` Nodes)
* **Step 3a (HA Upgrade `1 -> 3` Nodes)**: New BE nodes join as Raft `Learner` replicas, stream the snapshot and Raft log, and are promoted to `Voter` via `ConfChange` with zero downtime.
* **Step 3b (Horizontal Shard Rebalancing `3 -> N` Nodes)**: `CellLocalManager` transfers shard replicas and leadership across nodes; `JumboIsolated` shards are automatically given dedicated nodes.
* **Step 3c (Multi-Cell Federation `N -> 1,000+` Nodes)**: `GlobalManager` partitions contiguous key ranges into independent `Cells`, coordinating cross-Cell range migrations when cluster capacity expands.

---

## 8. Alternatives Considered (EGM Mandatory Section)

1. **Alternative 1: Using Unmodified TiDB / CockroachDB as a Black-Box Metadata Store**
   * *Reason for Rejection*: Baidu's Generation-2 deployment and Appendix A benchmarks prove that general-purpose databases incur `100%` multi-shard transactions (when updating secondary/quota indexes), suffer a `36.2x` throughput drop under single-directory concurrent writes due to row-level lock contention on parent attributes, and experience `2.5s–6s` `listdir` latency spikes during bulk TTL deletions due to persisted MVCC + LSM double tombstones.
2. **Alternative 2: Building a Custom Ad-Hoc Replication Protocol (CephFS / Custom MDS Style)**
   * *Reason for Rejection*: Separate directory-indexing and KV layers require cross-system 2PC and bespoke replication/recovery logic (which cost Baidu 6 months of a 10-engineer team before being abandoned, §8). Using Multi-Raft + structured tables (`MetaTable`, `QuotaTable`, `SecondaryIndexTable`) unifies both flat object namespaces (BOS) and hierarchical file trees (CFS/Mantle) under one transactional engine.
3. **Alternative 3: Bypassing Raft in Single-Node Mode via Native LSM WAL**
   * *Reason for Rejection*: Maintaining two durability mechanisms (LSM WAL for single-node vs. Raft log for distributed) bifurcates crash recovery for `In-Memory MVCC` and asynchronous queues (`AsyncAttrAggregator` / `AsyncAuxiliarySink`). A `1-Voter` Raft log in single-node mode has the same sequential disk I/O cost as a WAL while guaranteeing 100% state-machine parity during online scale-out.
4. **Alternative 4: Time-Bounded Eager LSM Compaction (e.g., Lethe / Acheron)**
   * *Reason for Rejection*: Enforcing rigid time-based tombstone retention rewrites cold SSTable levels indiscriminately, causing severe write amplification. MetaDB's **Locality-Aware Compaction** triggers strictly on key ranges whose observed tombstone density exceeds a threshold, cleaning up bulk deletions only where range scans are actually impacted.

---

## 9. Risks, Limitations & Mitigations (EGM Mandatory Section)

| Risk / Limitation | Impact | Mitigation in MetaDB |
| :--- | :--- | :--- |
| **Extreme Single-Directory Skew (Jumbo Shards)** | A single directory (`PID`) with `>500M` files cannot be split by `group_key = PID` alone, potentially saturating one node's NVMe I/O or Raft log bandwidth. | 1. **Automatic Jumbo Shard Isolation**: `CellLocalManager` evicts peer shards so the Jumbo Shard monopolizes the machine.<br>2. **Dual-Mode Split Fallback**: Allows fine-grained `(PID, name)` range splitting for flagged jumbo `PID`s, trading single-shard pushdown for horizontal capacity on that directory. |
| **Eventual Consistency of Quotas & Secondary Indexes** | `QuotaTable` and `SecondaryIndexTable` lag behind `MetaTable` by a few milliseconds (or longer during remote shard network partitions). | 1. Explicitly document that quotas are soft/near-real-time accounting; provide a configurable per-shard local credit lease for strict quota enforcement if needed.<br>2. Bound `AsyncAuxiliarySink` queue depth with backpressure and persist un-drained tasks in the Raft log with `HLC` idempotency. |
| **Raft Log Retention During Sink Backpressure or Unflushed MemTables** | Because RocksDB WAL is disabled, Raft logs cannot be truncated past `min(flush_lsn, attr_agg_lsn, aux_sink_lsn)`. | Enforce a bounded memory budget on `MemMvccStore` and `AsyncAttrAggregator` (force-flushing when watermark is reached) and spill overdue `AuxUpdateTask` records to a dedicated local retry table so Raft log truncation is never indefinitely blocked. |
| **Rejection of Long-Running Transactions (`>5s`)** | `In-Memory MVCC` prunes historical versions older than `tx_timeout_ms` (`5s`) during flush. | Metadata operations execute in `<10ms`. Background namespace crawlers or fsck scanners use cursor-based paginated `listdir(pid, after_name, limit)` across short transactions rather than a single long-lived MVCC snapshot. |

---

## 10. Crate Architecture & Implementation Map

The repository is structured as a modular Cargo workspace with six crates:

```text
rustfs/MetaDB
├── Architecture.md                        # Master architectural specification (this document)
├── README.md                              # Project overview, quickstart, and scale benchmarks
├── Cargo.toml                             # Workspace manifest
├── _agents/skills/metadb_elephant/        # EGM Elephant project context skill
├── docs/designs/                          # EGM Spec-Driven Development artifacts
│   ├── SITEMAP.md
│   ├── concepts_brief.md
│   ├── metadb_architecture.md
│   └── review_report.md
└── crates/
    ├── metadb-core/                       # Core schema (MetaTable, QuotaTable, SecondaryIndexTable),
    │                                      # (PID, "_attr_") key encoding, HLC clock, and errors
    ├── metadb-storage/                    # Lifecycle-Aware Storage Engine: RaftLog (WAL-less durability),
    │                                      # In-Memory MVCC (MemMvccStore), Persistent KV, LocalityCompactor
    ├── metadb-tx/                         # Single-Shard Pushdown Executor, 2PC cross-shard Rename,
    │                                      # AsyncAttrAggregator (+ Read Reconciliation), AsyncAuxiliarySink
    ├── metadb-cluster/                    # Policy/Size Sharding (group_key = PID), Jumbo Shard Isolation,
    │                                      # Dual-Mode Split, Two-Tier Control Plane (Global + Cell Managers),
    │                                      # MetaCache, and 1-Node -> Multi-Node Raft ConfChange
    ├── metadb-proxy/                      # Lightweight SQL Template Parser (~20 fixed metadata SQL patterns)
    │                                      # and stateless request router
    └── metadb-server/                     # Unified MetaDbEngine supporting Stage 1 (Single-Node),
                                           # Stage 2 (Multi-Shard), and Stage 3 (Distributed Multi-Cell)
```
