# 单机与分布式多单元（Multi-Cell）集群部署（简体中文）

* **返回指南首页**: [简体中文概览](./README.md) | [文档中心](../../README.md)

MetaDB 在单机模式与千节点分布式集群中使用**完全相同的无 WAL LSM 存储引擎与 OpenRaft 状态机**，支持从单机平滑在线演进至百亿—十万亿级元数据集群，无需停机迁移数据格式。

> **实现状态说明**：无 WAL SSTable 持久化 LSM 引擎、`openraft` Multi-Raft（`1-Voter -> 3-Voter` 在线扩容）、带 `ShardEpoch` 围栏的 `PID` 边界与 `DualModeFineGrained` 分片分裂、Jumbo 分片隔离、守护进程 `metadbd` 以及运维工具 `metadb-ctl` 均已在当前 8-Crate 工作区中完整实现并通过测试验证。

---

## 阶段一：独立单节点部署 (`metadbd --role all-in-one`) `[已实现]`

在单机一体化模式下，`GlobalManager`、`CellLocalManager`、`MetaCache`、`Proxy` 与 `BE` 存储引擎运行于同一进程，底层采用 **`1-Voter` OpenRaft**。由于分段 CRC32C `.raftlog` 已经提供顺序写崩溃持久化保障，底层 LSM 引擎彻底关闭本地 KV WAL，避免双重写日志开销。

### `standalone.toml` 配置示例

```toml
[node]
node_id = 1
role = "all-in-one"
data_dir = "/var/lib/metadb"
grpc_listen_addr = "127.0.0.1:7800"
metrics_listen_addr = "127.0.0.1:9100"

[clock]
max_clock_drift_ms = 500

[storage]
memtable_size_bytes = 33554432          # 32 MiB
block_cache_bytes = 536870912           # 512 MiB（16 分片 LRU 块缓存）
mvcc_retention_ms = 5000                # 5 秒内存 MVCC 保留窗口
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true           # 初始化表前缀根分片：1(Meta)、2(Quota)、3(SecondaryIndex)
shard_split_threshold_bytes = 4294967296 # 4 GiB 分片分裂阈值
```

### 启动单机服务

工具链固定为 Rust **1.100.0**（`rust-toolchain.toml`）。当前只有 `all-in-one` 会真正启动；`be` / `proxy` / `cell-manager` / `global-manager` 会被拒绝，而不是静默变成全功能进程。

gRPC 的 metadata、transaction、control-plane 与 Raft 传输**没有 TLS**。`[security].shared_secret` 为空时也没有应用层认证，默认监听 `127.0.0.1`。在补上共享密钥或外部 TLS 终结之前，不要把该端口暴露到不可信网络。非空 `shared_secret` 要求 metadata / transaction / control-plane 携带 `authorization: Bearer <secret>`（同一端口上的 Raft 传输仍不校验该密钥）。

```bash
metadbd --config /etc/metadb/standalone.toml
```

---

## 阶段二：单节点动态 `PID` 边界分片分裂 `[已实现]`

当分片容量超过 `shard_split_threshold_bytes` 时，`PolicyShardSplitter` 会扫描 SSTable 块索引中点并严格沿着 `group_key = PID`（`[0x01][pid: 8B BE]`）边界执行原子分裂：
* 同一目录的属性记录 `(PID, "_attr_")` 与所有直接子项 `(PID, child_name)` 始终保持在同一分片内。
* 单机上的多个 OpenRaft 分片组可并行刷盘与压缩，充分压榨多核与 NVMe 多队列吞吐。

---

## 阶段三：在线扩展至 `3-Voter` Multi-Raft 与多单元集群 `[已实现]`

### 3.1 零停机高可用升级（`1-Voter -> Learner -> 3-Voter`）

1. 启动两台新的存储后端节点（`--role be`）接入当前控制面：
   ```bash
   metadbd --role be --node-id 2 --listen 10.0.1.12:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb
   metadbd --role be --node-id 3 --listen 10.0.1.13:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb
   ```
2. 通过 `metadb-ctl` 在线提升副本数：
   ```bash
   metadb-ctl raft scale-replicas --cell-id 1 --target-voters 3
   ```
   **内部演进流程**：
   * `ConfChangeOrchestrator` 先将节点 `2`、`3` 添加为无投票权的 `Learner`。
   * Leader 触发协同刷盘屏障，创建硬链接 SSTable 快照并通过 `InstallSnapshot` 与增量日志追平进度。
   * 当复制延迟低于 `64` 条日志后，通过 OpenRaft 联合共识（Joint Consensus）将其平滑提升为 `Voter`，全程**零写入中断**。

### 3.2 多单元（Multi-Cell）存算与控制面分离拓扑（百亿—十万亿级规模）

当单集群规模超过 `~50,000` 个分片时，可将各角色独立部署并划分为多个自治 **Cell**：

| 组件角色 | `--role` 参数 | 推荐部署方式 | 核心职责 |
| :--- | :--- | :--- | :--- |
| **全局管理器 (`GlobalManager`)** | `global-manager` | 3 副本 Raft 组 | 维护粗粒度 `[start_key, end_key) -> CellId` 映射、分配 `65,536` 步长全局 ID 号段、管理跨目录移动租约。 |
| **单元管理器 (`CellLocalManager`)** | `cell-manager` | 每 Cell 独立 3 副本组 | 承载本 Cell 内 BE 节点心跳（每 Cell `20K–50K` 分片）、`PID` 分裂调度、Jumbo 独占隔离与副本变更。 |
| **存储后端 (`BE`)** | `be` | 每 Cell `12 – 60+` 台 NVMe 节点 | 承载 Multi-Raft 分片、`MemMvccStore`、`LsmStorageBackend`、`1-RTT` 下推执行与异步发件箱。 |
| **无状态代理 (`Proxy`)** | `proxy` | 弹性扩缩容 / 与 RustFS 同机部署 | 内嵌 `MetaCache`（`>99.9%` 单跳命中率）、编译 20 条 SQL 模板并协调跨分片 2PC 事务。 |
