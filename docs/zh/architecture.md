# 按实现描述的架构

[概览](./README.md) · [English](../en/architecture.md) · [Français](../fr/architecture.md) · [Deutsch](../de/architecture.md) · [日本語](../ja/architecture.md)

本页跟随 `crates/` 里的代码。它不描述多进程单元部署。那种部署没有实现。见 [运维](./operations.md#已知限制)。

## 进程形态

`PersistentMetaDbCluster::open_and_serve`（`crates/metadb-server/src/lib.rs`）：

1. 拒绝除 `all-in-one` 以外的任何 `node.role`。
2. 绑定 `node.grpc_listen_addr`（默认 `127.0.0.1:7800`；`127.0.0.1:0` 让操作系统分配端口）。
3. 在 `<data_dir>/global_allocator` 打开 `DurableIdAllocator`。
4. 在 `<data_dir>/be` 打开 `ShardGroupHost`。
5. 在 `<data_dir>/cell_<cell_id>` 打开或引导 `DurableCellManager`。
6. 把这一个单元以键范围 `[0x01, 0x04)` 注册到 `DurableGlobalManager`。
7. 保证根 inode `1`（`ROOT_INODE_ID`）有属性记录。
8. 在同一监听器上提供四个 gRPC 服务。四个服务使用同一个可选的持有者拦截器。密钥为空时，所有调用都放行。
9. 启动间隔 250 ms 的维护任务。

`metadbd` 是薄封装：加载 TOML，应用命令行覆盖，调用 `open_and_serve`，可选地对 `--cell-manager` 调用 `RegisterNode`，可选地提供 Prometheus 文本，并在 Ctrl-C 时调用 `shutdown`。

## 键与表

用户表前缀（`crates/metadb-core/src/schema.rs`）：

| 前缀 | 表 | `bootstrap_table_shards = true` 时的引导范围 |
| :--- | :--- | :--- |
| `0x01` | `MetaTable` | 分片 1，`[0x01, 0x02)` |
| `0x02` | `QuotaTable` | 分片 2，`[0x02, 0x03)` |
| `0x03` | `SecondaryIndexTable` | 分片 3，`[0x03, 0x04)` |

`MetaTable` 的键是 `[0x01][pid: 8 字节大端][名称 UTF-8]`。目录自身的属性是保留子项 `(pid, "_attr_")`，紧挨着 `(pid, child_name)` 存放。空名称、含 NUL 的名称，或等于 `_attr_` 的用户名会被拒绝（`ReservedEntryName`）。

目录 inode 标识的第 63 位为 0。文件 inode 标识的第 63 位为 1（`FILE_INODE_TAG_BIT`）。根是 inode `1`。持久分配器把新目录标识从 `2` 开始，文件标识从 `1` 开始，动态分片标识从 `4` 开始（1–3 是引导分片）。`LocalIdBuffer` 在把授予写入 `ID_ALLOCATOR_STATE` 之后，按 256 个一批发放目录标识，按 4,096 个一批发放文件 inode 标识和文件标识。

分片本地的系统前缀：

| 前缀 | 记录 |
| :--- | :--- |
| `0x04` | 双模式非锚点子分片上的 `DirShardDelta` |
| `0x05` | 2PC 的 `WriteIntent` |
| `0x06` | 2PC 的 `TxnRecord` |
| `0x07` | 溢出的辅助出站条目 |

`bootstrap_table_shards = false` 时，引导创建一个分片，标识为 `1`，覆盖 `[0x01, 0x04)`。

## 请求路径

元数据调用从嵌入的 `DistributedTxRouter`（`cluster.tx_router()`）进入，或从 gRPC `MetadataService` / `TransactionService` 进入。`StatelessProxyRouter` 编译 20 条 SQL 模板之一并调用同一路由器。没有通用 SQL 引擎。解析器只接受下面的语句（`crates/metadb-proxy/src/sql_templates.rs`）。`?` 占位符绑定 `u64`、`i64` 或字符串参数。字面量 `name = '_attr_'` 是目录统计。绑定参数等于 `_attr_` 会被拒绝。

| # | 语句 | 效果 |
| :--- | :--- | :--- |
| 1 | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | 创建文件 |
| 2 | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | mkdir |
| 3 | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | unlink |
| 4 | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | rmdir |
| 5 | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | 不覆盖的重命名 |
| 6 | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | 覆盖的重命名 |
| 7 | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | dirstat |
| 8 | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | lookup |
| 9 | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | listdir，硬编码上限 10,000 |
| 10 | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | listdir 分页 |
| 11 | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | 前缀 listdir（`LIKE` 只接受这种前缀形式） |
| 12 | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | setattr mode |
| 13 | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | setattr uid |
| 14 | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | setattr 大小与 fid |
| 15 | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | setattr 修改时间 |
| 16 | `SELECT * FROM QuotaTable WHERE uid = ?` | 读取配额 |
| 17 | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | 设置配额上限（保存的是 `ON CONFLICT` 一侧的值） |
| 18 | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | 列举配额 |
| 19 | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | 读取二级索引 |
| 20 | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | 扫描二级索引 |

`metadb-ctl sql` 发送参数列表为空的 `ExecuteSql`，因此语句必须使用字面量而不是 `?`。

路由使用 `CompositeMetaCache`。未命中或纪元不匹配时，从本地单元刷新缓存，并对本地分片组重试。`DurableGlobalManager::route_user_key` 查找 **本地** 分片句柄。它不把用户请求转发到另一个进程。

## 存储

每个分片的状态机在 `<data_dir>/be/shards/<20 位 shard_id>/lsm` 打开无 WAL 的 LSM。SSTable 使用 4 KiB 前缀压缩数据块（重启间隔 16）、布隆过滤器和带 `CURRENT` 指针的 `MANIFEST`。块缓存是 16 路 LRU。总预算是 `storage.block_cache_bytes`（默认 64 MiB）。16 个缓存分片各自至少得到 16 KiB。

MVCC 版本在内存中保留 `storage.mvcc_retention_ms`（默认 5,000）。协调刷盘封印 memtable，运行属性聚合钩子，并写出 L0 SSTable。没有单独的用户 KV WAL。崩溃恢复靠 Raft 日志加上 LSM 清单。`LocalityCompactor` 按 9 字节前缀桶跟踪墓碑，并在 `locality_tombstone_threshold` 与 `locality_tombstone_ratio` 同时越过时压缩热点删除范围。`ServerTomlConfig::to_lsm_config` 总是把 `auto_locality_compaction` 设为 `true`；TOML 文件不能关掉它。

`storage.memtable_size_bytes` 会被反序列化，但从不读取。刷盘大小不取自该字段。

## Raft

每个本地分片是一个 `openraft` 0.10 组。日志是 `.../shards/<id>/raft` 下的分段 CRC32C 文件。下列默认值 **不** 从 TOML 加载（`raft_options` 为 `#[serde(skip)]`）：

| 字段 | 默认 |
| :--- | :--- |
| 心跳 | 150 ms |
| 选举超时 | 450–900 ms |
| `max_in_snapshot_log_to_keep` | 0 |
| 日志段大小 | 64 MiB |

日志清除受 `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)` 约束。250 ms 维护循环在分片仍保留日志条目时调用 `flush_snapshot_and_purge`，除非快照水位已经至少达到 `state_flushed_index` 且剩余条目少于 64。

`ConfChangeOrchestrator::add_learners_and_promote_voters` 添加 learner，等到复制滞后最多 64 条（默认超时 10 秒，即 `scale_out_replicas` 传入的值），然后调用 `change_membership`。进程内测试 `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` 用三个 tonic 监听器和一个基本 KV 状态机在一个操作系统进程里做这件事。它不启动 `metadbd`。

## 事务与辅助工作

单目录的创建、unlink、setattr、lookup、dirstat、listdir，以及同一分片上的 mkdir/rmdir，会提议给分片领导者。父目录计数器不在前台路径上加锁：变更记录增量，`ReplicatedAttrAggregator` 在刷盘屏障处折叠它们，dirstat 在读路径上核对尚未刷出的增量。

配额和 `fid -> (pid, name)` 的更新走每个分片的辅助出站队列。`AuxDispatcher` 在维护节拍上应用它们。若队列无法应用，条目可以溢出到前缀 `0x07` 之下，使 Raft 压缩不会被该队列永久钉住。`TxnRecoveryWorker` 在同一节拍上扫描 `TxnRecord` / `WriteIntent`。

跨分片重命名（以及触及两个分片的 mkdir/rmdir）使用以主分片为锚的 2PC（`WriteIntent` + `TxnRecord`）。目录重命名从标识分配器取得一个当前有效的 `DirRenameLease`。在第一个租约释放或其 HLC 到期之前，第二个租约会被拒绝。租约 TTL 至少为 100 ms。

## 进程内分片

`maybe_split_or_isolate_shard` 在近似分片大小达到 `sharding.shard_split_threshold_bytes`（默认 4 GiB）时运行，或在 `TriggerShardSplit` 强制检查时运行。

`RaftShardSplitWorker::discover_split_key`：

- 至少需要两个存活键。
- 配额和二级索引分片在字节中点分裂。
- `MetaTable` 按父标识累加字节。若一个父项拥有这些字节的 85% 以上（`dominant_bytes * 100 > total_bytes * 85`），或分片只有一个父项，候选就是 `Jumbo`。
- 否则它在接近一半字节处选择父标识边界。分裂键是 PID 前缀，因此 `_attr_` 与一个父项的直接子项留在一起。
- 若该父项的路由已经是 `DualModeFineGrained`，分裂键是排除 `_attr_` 之后的 `(pid, name)` 中点。

巨型候选 **不会** 移动分片目录。`JumboIsolationPlanner` 把 `ShardHealthState::JumboIsolated { dedicated_node, dominant_pid }` 写到路由上。选中的节点是已注册节点中巨型路由最少、其次其他路由最少的节点。只注册了引导节点时，该节点就是本进程。

`enable_dual_mode_split` 持久化 `split_mode = DualModeFineGrained` 和 `dual_mode_pid`。它本身不复制键。随后的分裂使用 `(pid, name)` 规则。内存模拟器（`control_plane.rs` 中的 `CellLocalManager`）是另一种类型：它的巨型路径返回 `UnsplittableJumboShard`，它的“扩到 3 个投票者”只改一个 `BTreeMap`。测试 `T1`–`T6` 行使的是这个模拟器。

分裂计划存在 `CELL_MANAGER_STATE` 中。`split_crash_recovery_tests` 覆盖的崩溃点会在下一次 `maybe_split_or_isolate_shard` 时继续或回滚计划。子分片目录创建在同一棵 `be/shards` 树下。

## 名不副实的控制面 RPC

| RPC | 本树中的行为 |
| :--- | :--- |
| `RegisterNode` | 把 `(node_id, grpc_addr)` 插入本单元的 `CELL_MANAGER_STATE` |
| `StoreHeartbeat` | 不把调用方提供的分片描述写入路由缓存。`metadbd` 从不发送此 RPC |
| `GetRoutes` | `cell_id = 0` 列出全部缓存路由。未知且非零的 `cell_id` 返回错误，而不是空分片列表 |
| `AllocateIdBlock` | 从本地持久分配器分配 |
| `AllocSplitPlan` / `ActivateSplit` | 转发给本地单元管理器 |
| `TriggerShardSplit` | 在本地分片上强制 `maybe_split_or_isolate_shard` |
| `EnableDualModeSplit` | 设置本地路由标志。请求中的 `split_name` 被忽略 |
| `ScaleShardReplicas` | 若本单元存在且已注册节点足够，则调用 `scale_out_replicas` |
| `RegisterCell` | 返回应用错误。本进程仍然只托管一个单元，不保存请求中的单元 |
