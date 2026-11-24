# MetaDB 实现说明

[English](../en/README.md) · [Français](../fr/README.md) · [Deutsch](../de/README.md) · [日本語](../ja/README.md)

这些页面按代码的实际实现描述 `rustfs/MetaDB`。它们是本仓库的运维与开发说明。描述更大拓扑的设计笔记在 [`Architecture.md`](../../Architecture.md)、[`docs/designs/`](../designs/SITEMAP.md) 以及 [`docs/guides/`](../README.md) 下较早的指南中。那些文档与代码不一致时，以这一套为准。

## 这个仓库是什么

MetaDB 是为 [RustFS](https://github.com/rustfs) 编写的纯 Rust 元数据引擎，面向文件与对象命名空间。数据模型跟随目录拓扑：一个目录和它的直接子项共享键前缀，因此单个目录上的创建、查找、列举或删除留在一个分片上。设计改编自百度与北京大学的论文 *MetaDB: Putting File-System Structure Back into Distributed Databases*（[arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1)）。论文中的生产规模数字描述的是百度的部署。本仓库没有发布对应的测量结果。

工作区版本是 `0.1.0`（Apache-2.0）。Rust 工具链在 `rust-toolchain.toml` 中固定为 **1.100.0**。

## 一个进程实际运行什么

The default `metadbd` process is **all-in-one**. `be`, `proxy`, `cell-manager`, and `global-manager` also start when their configuration is valid. An all-in-one process has:

- 一个数据目录
- 一个单元（`node.cell_id`，默认 `1`）
- 每个分片一个 OpenRaft 投票者（就是这个进程）
- 一个 gRPC 监听器，提供 `MetadataService`、`TransactionService`、`ControlPlaneService`、`RaftTransportService` 与标准 `grpc.health.v1.Health`（`Check` / `Watch`）
- 每 250 ms 一次的后台循环，排空辅助出站队列（含跨 Cell `GrpcRemoteAuxSink` `ApplyAuxBatch` 投递）、执行 2PC 恢复、检查分裂策略，并对 Raft 日志做快照（启动时在 `/readyz` 返回 200 前会先完成首轮本地 Leader 分片的 2PC 恢复扫描）

`ServerRole` 支持 `all-in-one`、`be`、`proxy`、`cell-manager` 和 `global-manager`。单副本与 3 副本在线 `PID` 边界分裂、双模式 `(PID, name)` 分裂、基于 `ProbeTransferLeader` 预检的安全领导权转移、3 个真实 `metadbd` 操作系统进程的高可用分裂与故障切换演练（`three_process_ha_rehearsal`），以及 4 角色存算/控制分离跨 Cell 端到端测试（`disaggregated_four_role_e2e`）均已在仓库测试中覆盖。见 [运维](./operations.md)。

## 文档地图

| 页面 | 内容 |
| :--- | :--- |
| [架构](./architecture.md) | 包、键、存储、Raft、事务、分片，以及两种引擎 |
| [构建与运行](./build-and-run.md) | 工具链、二进制、嵌入库、停止 |
| [配置](./configuration.md) | TOML 字段、默认值，以及进程不应用的字段 |
| [运维](./operations.md) | `metadb-ctl`、指标、扩容、分裂、限制、文档缺口 |
| [测试与压测](./testing.md) | `cargo test`、每个套件实际覆盖什么、`metadb-ctl bench` |

## 工作区包

| 包 | 代码做什么 |
| :--- | :--- |
| `metadb-core` | 键编码 `0x01`–`0x07`、`ShardEpoch`、混合逻辑时钟（默认漂移上限 500 ms）、88 字节的 `InodeAttr` |
| `metadb-proto` | [架构](./architecture.md) 中列出的 Protobuf 与 tonic 服务 |
| `metadb-storage` | 无 WAL 的 SSTable LSM、`MANIFEST`、16 路 LRU 块缓存、内存 MVCC、局部性压缩 |
| `metadb-raft` | `openraft` 0.10 组、分段 CRC32C `.raftlog`、快照、learner 到 voter 的成员变更 |
| `metadb-tx` | 下推变更、属性增量、辅助出站队列、以主分片为锚的 2PC |
| `metadb-cluster` | 持久单元管理器与全局管理器、ID 分配器、路由缓存、分裂工作者、巨型分片规划 |
| `metadb-proxy` | 20 条 SQL 模板的词法与语法分析，通向事务层的无状态路由器 |
| `metadb-server` | `PersistentMetaDbCluster`、内存中的 `MetaDbEngine::new_single_node`、`metadbd`、`metadb-ctl`、Prometheus 文本 |

## 调用引擎的两种方式

`MetaDbEngine::open` 和 `PersistentMetaDbCluster::open_and_serve` 打开数据目录、LSM 和 OpenRaft，并绑定 gRPC。要运行的是这一套。

`MetaDbEngine::new_single_node` 构造内存模拟器。它不打开数据目录、LSM 或 OpenRaft 组。`crates/metadb-server/tests/integration_tests.rs` 中的测试 `T1`–`T6` 使用它，包括 `T6` 里的“3 投票者”步骤，那一步只改内存中的成员表。
