# MetaDB User Guide (English)

* **Languages**: **English** | [日本語](../ja/README.md) | [Français](../fr/README.md) | [简体中文](../zh/README.md) | [Deutsch](../de/README.md)
* **Documentation Hub**: [`docs/README.md`](../../README.md)

**MetaDB** (`rustfs/MetaDB`) is a 100% pure-Rust, file-system structure-aware distributed transactional metadata engine built for [RustFS](https://github.com/rustfs), inspired by Baidu's trillion-scale MetaDB architecture ([`arXiv:2610.32247v1`](https://arxiv.org/html/2610.32247v1)).

---

## Guide Navigation

1. **Overview & Embedded Quickstart** *(this page)*
2. [**Deployment & Online Cluster Scale-Out**](./deployment.md) — Single-Node `metadbd`, `1-Voter -> 3-Voter` Multi-Raft promotion, and Multi-Cell `GlobalManager` / `CellLocalManager` / `MetaCache` topology.
3. [**Client APIs, gRPC & 20 SQL Templates**](./client-apis-and-sql.md) — Embedded Rust SDK, `tonic` gRPC services, and the complete reference of 20 metadata SQL templates.
4. [**Operations, `metadb-ctl` & Performance Tuning**](./operations-and-tuning.md) — CLI administration, Jumbo Shard isolation & Dual-Mode splitting, `SnapshotGuard` MVCC & `LocalityCompactor` tuning, and troubleshooting.

---

## Feature Readiness Matrix (Phases 1–6 Verified)

MetaDB's 8-crate pure-Rust implementation is verified across all core subsystems:

| Feature Area | Repository Status | Implementation Crate / Binary |
| :--- | :--- | :--- |
| Embedded Rust SDKs (`PersistentMetaDbCluster` & `MetaDbEngine`) | **Implemented & Tested** (async persistent + sync in-memory) | `metadb-server` |
| `(PID, "_attr_")` Colocation, `0x01..0x07` Codecs & `HLC` | **Implemented & Tested** (88B CRC32C + 500ms drift guard) | `metadb-core` |
| Disk Persistence (Pure-Rust WAL-less SSTable LSM) | **Implemented & Tested** (`LsmStorageBackend`, `MANIFEST`, Bloom, LRU Cache) | `metadb-storage` |
| `SnapshotGuard` `ReadCut` MVCC & `LocalityCompactor` | **Implemented & Tested** (`ActiveSnapshotRegistry` + bottommost compaction) | `metadb-storage` |
| Multi-Raft Consensus (`1-Voter -> 3-Voter` + `ReadIndex`) | **Implemented & Tested** (`openraft` 0.10 + `.raftlog` + `tonic`) | `metadb-raft` |
| `1-RTT` Pushdown, `ReplicatedAttrAggregator` & `AuxOutbox` | **Implemented & Tested** (Coordinated Flush Barrier + `[0x07]` local spill) | `metadb-tx` |
| Cross-Shard 2PC (`Rename`, `Mkdir`, `Rmdir`, `SetAttr`) | **Implemented & Tested** (Primary-Anchored `TxnRecord` + `WriteIntent` recovery) | `metadb-tx` |
| `PID`-Boundary Split, Jumbo Isolation & `DualMode` | **Implemented & Tested** (`ShardEpoch` fencing + `DirShardDelta` scatter-gather) | `metadb-cluster` |
| 20 Parameterized SQL Templates | **Implemented & Tested** (all 20 `?` & escaped SQL templates) | `metadb-proxy` |
| `metadbd` Server Daemon & `metadb-ctl` CLI | **Implemented & Tested** (`crates/metadb-server/src/bin/`) | `metadb-server` |

---

## Quickstart: Embedded Rust SDK (`PersistentMetaDbCluster` & `MetaDbEngine`)

Add `metadb-server` and `metadb-core` from the workspace to embed MetaDB directly inside a Rust application or test harness:

```toml
[dependencies]
metadb-core = { path = "crates/metadb-core" }
metadb-server = { path = "crates/metadb-server" }
```

```rust
use metadb_core::ROOT_INODE_ID;
use metadb_server::{MetaDbConfig, MetaDbEngine};

fn main() -> metadb_core::Result<()> {
    // 1. Boot a Stage 1 single-node engine (pre-initializes root directory `/` with PID = 1).
    let engine = MetaDbEngine::new_single_node(MetaDbConfig::default());

    // 2. Create a subdirectory `/checkpoints` (dir_id = 100) and a file inside it (1-RTT pushdown).
    engine.mkdir(ROOT_INODE_ID, "checkpoints", 100, /*uid=*/ 1000, 0o755)?;
    engine.create_file(
        /*pid=*/ 100,
        "step_0001.pt",
        /*inode_id=*/ 2001,
        /*fid=*/ 9001,
        /*uid=*/ 1000,
        /*mode=*/ 0o644,
        /*size_bytes=*/ 104_857_600,
    )?;

    // 3. Linearizable directory stat via Read-Path Reconciliation (even before background flush!).
    let dir_stat = engine.dirstat(100)?;
    assert_eq!(dir_stat.num_children, 1);
    assert_eq!(dir_stat.size_bytes, 104_857_600);

    // 4. Execute a lightweight metadata SQL template directly.
    let entries = engine.listdir(100)?;
    println!("Live entries in /checkpoints: {}", entries.len());

    // 5. Flush async attribute deltas, drain Quota/SecondaryIndex queues, and flush MVCC.
    engine.run_background_maintenance()?;
    Ok(())
}
```

### Async Persistent SDK (`PersistentMetaDbCluster`)

`PersistentMetaDbCluster::open_and_serve(config).await?` persists shards under `<data_dir>/be/shards/<shard_id>/` backed by `LsmStorageBackend` and OpenRaft with automatic background maintenance, auxiliary outbox dispatch, and gRPC endpoints:

```rust
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};

let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(
    ServerTomlConfig::from_toml_file("/etc/metadb/standalone.toml")?,
).await?;
let dir = cluster.mkdir_auto_id(1, "checkpoints", 1000, 0o755).await?;
let file = cluster.create_file_auto_id(dir.attr.inode_id, "step_0002.pt", 1000, 0o644, 4096).await?;
cluster.shutdown().await?;
```
