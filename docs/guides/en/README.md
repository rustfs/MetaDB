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

## Feature Readiness Matrix (Current Prototype vs. Phases 1–6)

MetaDB tracks implementation readiness transparently across every milestone:

| Feature Area | Current Status (`v0.1.0-prototype`) | Target Production Milestone |
| :--- | :--- | :--- |
| Embedded Rust SDK (`MetaDbEngine`) | **Implemented** (synchronous in-memory prototype) | **Phase 6** (async `tokio` + persistent LSM/OpenRaft) |
| `(PID, "_attr_")` Colocation & `HLC` Clock | **Implemented** (`metadb-core`) | **Phase 1** (88B CRC32C framing + 500ms drift guard) |
| Disk Persistence (Pure-Rust WAL-less SSTable LSM) | Simulated in RAM (`BTreeMap`) | **Phase 2** (`LsmStorageBackend`, `MANIFEST`, Bloom, LRU Cache) |
| `SnapshotGuard` In-Memory MVCC & `LocalityCompactor` | In-RAM prototype | **Phase 2** (`ActiveSnapshotRegistry` + bottommost range compaction) |
| Multi-Raft Consensus (`1-Voter -> 3-Voter` + `ReadIndex`) | Simulated in RAM (`VecDeque`) | **Phase 3** (`openraft` + segmented CRC32C `.raftlog` + `tonic`) |
| `1-RTT` Pushdown, `AsyncAttrAggregator` & `AuxOutbox` | **Implemented** (single-process RAM) | **Phase 4** (Coordinated Flush Barrier + `[0x07]` local spill) |
| Cross-Shard 2PC (`Rename`, `Mkdir`, `Rmdir`) | In-RAM `Rename` prototype | **Phase 4** (Primary-Anchored `TxnRecord` + `WriteIntent` recovery) |
| `PID`-Boundary Split, Jumbo Isolation & `DualMode` | **Implemented** (in-RAM split & isolation) | **Phase 5** (`ShardEpoch` fencing + `DirShardDelta` scatter-gather) |
| SQL Template Interface | **Implemented** (8 literal SQL patterns) | **Phase 5** (20 parameterized `?` & escaped SQL templates) |
| `metadbd` Server Daemon & `metadb-ctl` CLI | Planned | **Phase 6** (`crates/metadb-server/src/bin/`) |

---

## Quickstart: Embedded Rust SDK (`v0.1.0-prototype` — Available Today)

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

### Target Async Persistent SDK (`Phase 6`)

Once Phases 2–6 land, `MetaDbEngine::open(config).await?` persists shards to `<data_dir>/shards/<shard_id>/` backed by `LsmStorageBackend` and `1-Voter` OpenRaft with automatic background flush, compaction, and auxiliary outbox workers:

```rust
// Target API (Phase 6)
let engine = MetaDbEngine::open(MetaDbConfig::from_toml_file("/etc/metadb/standalone.toml")?).await?;
let file = engine.create_file_auto_id(100, "step_0002.pt", /*uid=*/ 1000, 0o644, 4096).await?;
engine.shutdown().await?;
```
