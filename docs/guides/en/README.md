# MetaDB User Guide (English)

* **Languages**: **English** | [日本語](../ja/README.md) | [Français](../fr/README.md) | [简体中文](../zh/README.md) | [Deutsch](../de/README.md)
* **Documentation Hub**: [`docs/README.md`](../../README.md)

**MetaDB** (`rustfs/MetaDB`) is a 100% pure-Rust, file-system structure-aware distributed transactional metadata engine built for [RustFS](https://github.com/rustfs), inspired by Baidu's trillion-scale MetaDB architecture ([`arXiv:2610.32247v1`](https://arxiv.org/html/2610.32247v1)).

---

## Guide Navigation

1. **Overview & Embedded Quickstart** *(this page)*
2. [**Deployment**](./deployment.md) — Single-node `metadbd`. Stage 3 role-split topology is a design note, not a deployment this binary performs.
3. [**Client APIs, gRPC & 20 SQL Templates**](./client-apis-and-sql.md) — Embedded Rust SDK, `tonic` gRPC services, and the complete reference of 20 metadata SQL templates.
4. [**Operations, `metadb-ctl` & Performance Tuning**](./operations-and-tuning.md) — CLI administration, Jumbo Shard isolation & Dual-Mode splitting, `SnapshotGuard` MVCC & `LocalityCompactor` tuning, the embedded stress harness, and troubleshooting.

---

## Feature readiness

The Baidu paper [`arXiv:2610.32247v1`](https://arxiv.org/html/2610.32247v1) reports Baidu production figures, including 575+ nodes per cluster and trillion-scale metadata. Those figures are not results of this repository. This matrix is not a first-launch pass and not a scale-gate result.

Each status cell uses one label: **design claim**, **crate-tested**, or **multi-process unverified**. In-tree tests are crate or in-process coverage unless the cell says otherwise. They are not a multi-process 3-voter operator rehearsal and not a three-VM result. Operator detail is in [`docs/en/operations.md`](../../en/operations.md) and [`docs/en/architecture.md`](../../en/architecture.md). Applied memtable settings, in-flight 2PC lease renewal, and the seeded simulation catalog ([`docs/en/simulation-testing.md`](../../en/simulation-testing.md)) are on current `main`. They are not Baidu production measurements.

| Feature Area | Repository Status | Implementation Crate / Binary |
| :--- | :--- | :--- |
| Embedded Rust SDKs (`PersistentMetaDbCluster` & `MetaDbEngine`) | **crate-tested**. `PersistentMetaDbCluster` is covered by single-process tests. `MetaDbEngine::new_single_node` is an in-memory simulator with no disk and no Raft. | `metadb-server` |
| `(PID, "_attr_")` Colocation, `0x01..0x07` Codecs & `HLC` | **crate-tested**. `metadb-core` unit tests cover the 88B CRC32C `InodeAttr` and the 500ms drift guard. | `metadb-core` |
| Disk Persistence (Pure-Rust WAL-less SSTable LSM) | **crate-tested**. `LsmStorageBackend`, `MANIFEST`, Bloom, and the LRU block cache are covered by storage crate tests. | `metadb-storage` |
| `SnapshotGuard` `ReadCut` MVCC & `LocalityCompactor` | **crate-tested**. `ActiveSnapshotRegistry` and bottommost compaction are covered by storage crate tests. A flat `listdir` P99 under `<4ms` is a paper design target, not a measurement of this repository. | `metadb-storage` |
| Multi-Raft Consensus (`1-Voter -> 3-Voter` + `ReadIndex`) | **multi-process unverified**. The checked-in 1-voter to 3-voter test runs three tonic listeners and a basic KV state machine inside one OS process. It does not start `metadbd`. `join_existing_group` starts a second `metadbd` and joins one group. A three-`metadbd` voter rehearsal is outside these tests. | `metadb-raft` |
| `1-RTT` Pushdown, `ReplicatedAttrAggregator` & `AuxOutbox` | **crate-tested**. Coordinated flush-barrier aggregation and `[0x07]` local spill are covered by `metadb-tx` tests inside one process. | `metadb-tx` |
| Cross-Shard 2PC (`Rename`, `Mkdir`, `Rmdir`, `SetAttr`) | **crate-tested**. Primary-anchored `TxnRecord` and `WriteIntent` recovery are covered by `two_pc_crash_tests`. A live coordinator renews the 3-second lease up to 60 seconds (`two_pc_lease_keepalive_tests`). | `metadb-tx` |
| `PID`-boundary split & `DualMode` route flag | **crate-tested**. Epoch-fenced PID split is covered in-process. `enable_dual_mode_split` persists a route flag and does not itself copy keys. | `metadb-cluster` |
| Jumbo isolation (dedicated host) | **design claim**. Jumbo isolation in this tree is a route flag plus a node id chosen from the local node map. With one node, the flag names this process. Files stay on this host. Peer shards stay on their current host. | `metadb-cluster` |
| 20 Parameterized SQL Templates | **crate-tested**. `sql_templates_tests` covers all 20 templates. | `metadb-proxy` |
| `metadbd` Server Daemon & `metadb-ctl` CLI | **crate-tested**. The all-in-one binaries live in `crates/metadb-server/src/bin/`. `open_and_serve` accepts only `all-in-one`. | `metadb-server` |

---

## Quickstart: Embedded Rust SDK (`PersistentMetaDbCluster` & `MetaDbEngine`)

The crash-safe engine is the async `MetaDbEngine::open` / `PersistentMetaDbCluster` API (OpenRaft + WAL-less LSM on a real `data_dir`). `MetaDbEngine::new_single_node` is an **in-memory simulator** only: it does not open a data directory and must not be treated as the verified storage engine.

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
    // In-memory simulator only — not durable. Use MetaDbEngine::open for the real engine.
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

### Async Persistent SDK (`MetaDbEngine::open` / `PersistentMetaDbCluster`)

`MetaDbEngine::open(config).await?` (and `PersistentMetaDbCluster::open_and_serve(config).await?` when also retrieving the bound gRPC address) persists shards under `<data_dir>/be/shards/<shard_id>/` backed by `LsmStorageBackend` and OpenRaft with automatic background maintenance, auxiliary outbox dispatch, and gRPC endpoints:

```rust
use metadb_server::{MetaDbConfig, MetaDbEngine};

let engine = MetaDbEngine::open(
    MetaDbConfig::from_toml_file("/etc/metadb/standalone.toml")?,
).await?;
let dir = engine.mkdir_auto_id(1, "checkpoints", 1000, 0o755).await?;
let file = engine.create_file_auto_id(dir.attr.inode_id, "step_0002.pt", 1000, 0o644, 4096).await?;
engine.shutdown().await?;
```
