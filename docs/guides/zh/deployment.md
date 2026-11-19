# 单机与分布式多单元（Multi-Cell）集群部署（简体中文）

* **返回指南首页**: [简体中文概览](./README.md) | [文档中心](../../README.md)

当前二进制只以 `all-in-one` 运行持久化部署。同一单元内的多节点 Raft 由集成测试覆盖。按角色拆开的进程和多单元拓扑是设计目标，不是本仓库已经跑起来的部署。千亿到十万亿以上的记录数是设计模型，不是本仓库的实测结果；`arXiv:2610.32247v1` 里的万亿规模是百度生产数据。

> **实现状态**：持久化 LSM、Raft 复制、本机分片分裂，以及 all-in-one 的 gRPC/CLI 有回归覆盖。只有 `all-in-one` 会启动守护进程。`be`、`proxy`、`cell-manager`、`global-manager` 和远程 `RegisterCell` 会被拒绝。见 [正确性说明](../../designs/correctness_hardening.md)。

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

gRPC 的 metadata、transaction、control-plane、Raft 传输以及指标监听器**都不终结 TLS**。`[security].shared_secret` 为空时也没有应用层认证，并且只接受环回绑定。通配或其他非环回地址如果没有非空密钥，会拒绝启动。默认 `grpc_listen_addr` 是 `127.0.0.1:7800`。文件里的 TLS 或证书键会被拒绝，而不是当作明文服务。非空 `shared_secret` 要求 metadata、transaction、control-plane、同一端口上的 Raft 传输以及 `GET /metrics` 都携带 `authorization: Bearer <secret>`。对等 Raft 客户端发送同一个令牌。

```bash
metadbd --config /etc/metadb/standalone.toml
```

---

## 阶段二：单节点动态 `PID` 边界分片分裂 `[已实现]`

当分片容量超过 `shard_split_threshold_bytes` 时，`PolicyShardSplitter` 会扫描 SSTable 块索引中点并严格沿着 `group_key = PID`（`[0x01][pid: 8B BE]`）边界执行原子分裂：
* 同一目录的属性记录 `(PID, "_attr_")` 与所有直接子项 `(PID, child_name)` 始终保持在同一分片内。
* 单机上的多个 OpenRaft 分片组可并行刷盘与压缩，充分压榨多核与 NVMe 多队列吞吐。

---

## 阶段三：预期中的多单元拓扑

阶段三是设计目标。当前二进制不会按角色拆开进程来部署它。

`metadbd` 只运行 `--role all-in-one`。`crates/metadb-server/src/lib.rs` 中的 `PersistentMetaDbCluster::open_and_serve` 在监听之前拒绝 `be`、`proxy`、`cell-manager` 和 `global-manager`。`crates/metadb-server/src/grpc_services.rs` 中的 `RegisterCell` 返回应用错误，并且不保存第二个单元。本仓库没有在线的多进程、多单元部署。

第二个 `metadbd --role all-in-one` 可以在 `--join` 或 `--cell-manager` 指向当前 leader、且数据目录是新的时候加入**同一个**单元。该路径由 `crates/metadb-server/tests/join_existing_group.rs` 覆盖。加入者仍是 all-in-one 进程，不是裁剪后的存储后端，也不会创建另一个单元。见 [运维](../../zh/operations.md)。

### 3.1 同一单元内的投票者

不要启动 `metadbd --role be`。该角色在 `open_and_serve` 中退出，因此那些命令不是操作步骤。`metadb-ctl raft scale-replicas` 调用 `ScaleShardReplicas`，并要求目标投票者数量已经登记在该单元中。新建的 all-in-one 单元只有一个节点，所以在这些节点存在之前，`--target-voters 3` 会失败。约束见 [运维](../../zh/operations.md)。

### 3.2 存算分离拓扑的设计模型（`100B – 10T+` 条记录）

`100B – 10T+`（约千亿到十万亿以上）是**设计模型**中的规模区间，用来估算未来多单元拓扑可能需要的容量。它不是本仓库存储过的记录数，也不是把百度生产数据写成 `rustfs/MetaDB` 的结果。`arXiv:2610.32247v1` 发表的集群规模，包括 BOS2 的 1.848 万亿条记录，属于百度的生产数据。

下表是预期的角色划分。当前二进制不运行这些角色。

| 角色 | 设计中的 `--role` | 预期职责 |
| :--- | :--- | :--- |
| **全局管理器** | `global-manager` | 设计中把粗粒度 `[start_key, end_key)` 映射到 `CellId`，按 `65,536` 一段分配 ID，并协调跨单元目录改名租约。 |
| **单元管理器** | `cell-manager` | 设计中处理本单元 BE 心跳（每单元 `20K–50K` 分片），并编排 `PID` 分裂、Jumbo 隔离和成员变更。 |
| **存储后端 (`BE`)** | `be` | 设计中在 NVMe 主机上承载 Multi-Raft 分片组和 LSM 引擎。 |
| **无状态代理** | `proxy` | 设计中内嵌 `MetaCache`，编译 20 条 SQL 模板，并协调跨分片 2PC。 |

`metadb-ctl cell register` 调用 `RegisterCell` 并以非零状态退出。它不会注册新单元。不要把它当作联邦步骤。
