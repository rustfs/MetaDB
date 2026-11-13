# MetaDB 使用指南（简体中文）

* **多语言切换**: [English](../en/README.md) | [日本語](../ja/README.md) | [Français](../fr/README.md) | **简体中文** | [Deutsch](../de/README.md)
* **文档中心**: [`docs/README.md`](../../README.md)

**MetaDB** (`rustfs/MetaDB`) 是专为 [RustFS](https://github.com/rustfs) 打造的 100% 纯 Rust 分布式事务型元数据数据库引擎，架构设计参考并吸收了百度万亿级生产元数据中心论文（[*MetaDB: Putting File-System Structure Back into Distributed Databases*, `arXiv:2610.32247v1`](https://arxiv.org/html/2610.32247v1)）。

---

## 指南导航

1. **概览与嵌入式快速开始** *（本页）*
2. [**单机与分布式多单元（Multi-Cell）集群部署**](./deployment.md) — `metadbd` 单节点部署、`1-Voter -> 3-Voter` Multi-Raft 在线无损扩缩容，以及 `GlobalManager` / `CellLocalManager` / `MetaCache` 两层控制面拓扑。
3. [**Rust SDK、gRPC 接口与 20 条 SQL 模板参考**](./client-apis-and-sql.md) — 嵌入式 Rust SDK、`tonic` gRPC 服务与全部 20 条轻量级元数据 SQL 模板。
4. [**运维管理、`metadb-ctl` CLI 与性能调优排障**](./operations-and-tuning.md) — 运维命令行工具、超大目录分片（Jumbo Shard）隔离与双模分裂、`SnapshotGuard` 内存 MVCC 与局部性感知压缩（Locality-Aware Compaction）调优及常见故障排查。

---

## 功能实现状态矩阵（Phase 1–6 已全部实现并验证）

以下为当前仓库 8 个纯 Rust Crate 的已实现与测试验证状态：

| 功能模块 | 当前仓库状态 | 所属 Crate / 二进制 |
| :--- | :--- | :--- |
| 嵌入式 Rust SDK (`PersistentMetaDbCluster` & `MetaDbEngine`) | **已实现并验证**（异步持久化版 + 同步内存版） | `metadb-server` |
| `(PID, "_attr_")` 同分片聚簇、`0x01..0x07` 编码与 `HLC` | **已实现并验证**（88B CRC32C 编码 + 500ms 时钟偏斜保护） | `metadb-core` |
| 磁盘持久化（纯 Rust 无 WAL SSTable LSM） | **已实现并验证** (`LsmStorageBackend`、`MANIFEST`、Bloom、LRU 缓存) | `metadb-storage` |
| `SnapshotGuard` `ReadCut` 内存 MVCC 与局部性压缩 | **已实现并验证** (`ActiveSnapshotRegistry` + 最底层安全范围压缩) | `metadb-storage` |
| Multi-Raft 共识 (`1-Voter -> 3-Voter` + `ReadIndex`) | **已实现并验证** (`openraft` 0.10 + 分段 `.raftlog` + `tonic`) | `metadb-raft` |
| `1-RTT` 下推、`ReplicatedAttrAggregator` 与 `AuxOutbox` | **已实现并验证**（协同刷盘屏障 + `[0x07]` 本地溢出落盘与清理） | `metadb-tx` |
| 跨分片 2PC 事务 (`Rename`、`Mkdir`、`Rmdir`、`SetAttr`) | **已实现并验证**（主锁锚定 `TxnRecord` + `WriteIntent` 崩溃恢复） | `metadb-tx` |
| `PID` 边界分片分裂、Jumbo 隔离与双模分裂 (`DualMode`) | **已实现并验证** (`ShardEpoch` 围栏 + `DirShardDelta` 扇出聚合) | `metadb-cluster` |
| 轻量级 SQL 模板编译执行 | **已实现并验证**（全部 20 条参数化 `?` 与转义 SQL 模板） | `metadb-proxy` |
| `metadbd` 服务端守护进程与 `metadb-ctl` 运维工具 | **已实现并验证** (`crates/metadb-server/src/bin/`) | `metadb-server` |

---

## 快速开始：嵌入式 Rust SDK（`PersistentMetaDbCluster` 与 `MetaDbEngine`）

在 Cargo 工程中引入工作区内的 `metadb-core` 与 `metadb-server`，即可在进程内直接嵌入运行 MetaDB：

```toml
[dependencies]
metadb-core = { path = "crates/metadb-core" }
metadb-server = { path = "crates/metadb-server" }
```

```rust
use metadb_core::ROOT_INODE_ID;
use metadb_server::{MetaDbConfig, MetaDbEngine};

fn main() -> metadb_core::Result<()> {
    // 1. 启动 Stage 1 单节点引擎（自动初始化根目录 `/`，PID = 1）
    let engine = MetaDbEngine::new_single_node(MetaDbConfig::default());

    // 2. 创建子目录 `/checkpoints` (dir_id = 100) 并通过 1-RTT 下推写入文件元数据
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

    // 3. 读取路径对账（Read-Path Reconciliation）：即使后台尚未刷盘，dirstat 仍保证线性一致！
    let dir_stat = engine.dirstat(100)?;
    assert_eq!(dir_stat.num_children, 1);
    assert_eq!(dir_stat.size_bytes, 104_857_600);

    // 4. 列出目录下所有存活子项（自动过滤内部保留项 `_attr_`）
    let entries = engine.listdir(100)?;
    println!("Live entries in /checkpoints: {}", entries.len());

    // 5. 触发后台维护：折叠目录属性增量、消费配额/二级索引异步队列并刷写内存 MVCC
    engine.run_background_maintenance()?;
    Ok(())
}
```

### 异步持久化 SDK（`MetaDbEngine::open` / `PersistentMetaDbCluster`）

`MetaDbEngine::open(config).await?`（以及同时返回监听地址的 `PersistentMetaDbCluster::open_and_serve(config).await?`）基于 `LsmStorageBackend` 与 OpenRaft 实现磁盘级崩溃安全持久化、自动后台维护与 gRPC 服务监听：

```rust
use metadb_server::{MetaDbConfig, MetaDbEngine};

let engine = MetaDbEngine::open(
    MetaDbConfig::from_toml_file("/etc/metadb/standalone.toml")?,
).await?;
let dir = engine.mkdir_auto_id(1, "checkpoints", 1000, 0o755).await?;
let file = engine.create_file_auto_id(dir.attr.inode_id, "step_0002.pt", 1000, 0o644, 4096).await?;
engine.shutdown().await?;
```
