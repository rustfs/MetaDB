# 运维管理、`metadb-ctl` CLI 与性能调优排障（简体中文）

* **返回指南首页**: [简体中文概览](./README.md) | [文档中心](../../README.md)

---

## 1. `metadb-ctl` 运维命令行工具 `[已实现]`

```bash
# 查看各 Cell、BE 节点健康状态、分片分布与 Raft 三水位延迟
metadb-ctl --endpoint 127.0.0.1:7800 cluster status

# 列出指定 Cell 内的全部分片范围、ShardEpoch、副本角色与 Jumbo 隔离状态
metadb-ctl --endpoint 127.0.0.1:7800 shard list --cell-id 1

# 手动对指定分片触发 PID 边界分裂评估
metadb-ctl --endpoint 127.0.0.1:7800 shard split --shard-id 12

# 针对超大单目录开启 DualModeFineGrained (PID, name) 细粒度双模分裂
metadb-ctl --endpoint 127.0.0.1:7800 shard enable-dual-mode --shard-id 42 --pid 900100

# 在线将分片从单副本提升至 3 副本高可用模式 (1-Voter -> Learner -> 3-Voter)
metadb-ctl --endpoint 127.0.0.1:7800 raft scale-replicas --cell-id 1 --target-voters 3

# 在命令行执行元数据 SQL 模板
metadb-ctl --endpoint 127.0.0.1:7800 sql "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"

# 运行单目录高并发创建或批量 TTL 删除 listdir 基准测试
metadb-ctl --endpoint 127.0.0.1:7800 bench --workload hotspot-create --concurrency 64 --ops 100000
```

---

## 2. 超大目录（Jumbo Shard）治理策略

1. **第一级防护：自动节点独占隔离 (`JumboIsolated`)**：
   * `JumboIsolated` is a route flag. When one parent owns more than 85% of the shard's `MetaTable` bytes, or the shard has a single parent, `JumboIsolationPlanner` writes `dedicated_node` from nodes already registered in this cell. It does not migrate co-hosted peer shards onto another host. With one node, the flag names this process.
2. **第二级防护：双模细粒度分裂 (`DualModeFineGrained`)**：
   * 当单个目录规模突破单机物理上限（如数亿文件）时，通过 `metadb-ctl shard enable-dual-mode` 开启按 `(PID, name)` 子区间分裂。
   * 各非锚点子分片通过本地 `[0x04] DirShardDelta` 记录分片内局部统计信息：`create_file` / `unlink` 依然保持 **单分片 `1-RTT` 无锁写入**，而 `dirstat` 自动切换为 **`1-hop` 并行扇出聚合**。

---

## 3. 存储与压缩核心调优参数 (`metadb-storage`)

| 参数名 | 默认值 | 调优建议 |
| :--- | :--- | :--- |
| `mvcc_retention_ms` | `5000` (`5s`) | 内存 MVCC 活跃快照保留时间。切勿在单个快照中执行超过 5 秒的全量遍历；大目录请始终使用带 `name > ? LIMIT ?` 的游标分页查询（模板 #10）。 |
| `memtable_size_bytes` | `32 MiB` | 在内存充裕（`>= 64 GiB`）且高并发训练 Checkpoint 写入场景下可调大至 `64 MiB`，进一步提升内存 MVCC 版本合并裁剪率。 |
| `block_cache_bytes` | `512 MiB` | 生产独立 BE 节点建议设为物理内存的 `25%–40%`，让 SSTable 索引块、Bloom 过滤器及热点目录数据块常驻 16 分片 LRU 缓存。 |
| `locality_tombstone_threshold` | `128` | 单个 9 字节前缀桶 `[table_prefix][PID]` 内物理墓碑数阈值（同时满足墓碑占比 `>= 0.50` 时触发目标范围最底层压缩）。在密集 TTL 批量过期场景下可下调至 `64` 以确保 `listdir` 毫秒级延迟。 |

---

## 4. 常见错误与排障指南

* **`MetaDbError::TransactionTimeout`**：读取快照持有超过 `5,000ms` 或请求的 `read_hlc` 早于已落盘水位。请改用游标分页 `ListDir`（每页独立获取最新快照）。
* **`MetaDbError::ClockSkewExceeded`**：节点间物理时钟偏差超过 `max_clock_drift_ms` (`500ms`)。请检查集群 `chrony` / NTP 同步状态。
* **`MetaDbError::EpochMismatch` / `NotLeader`**：分片正在分裂或发生 Raft 切主，客户端 SDK 与 `metadb-proxy` 会自动刷新 `MetaCache` 并透明重试；若持续报错请检查 `CellLocalManager` 连通性。
* **`.raftlog` 磁盘占用增长**：Raft 日志清理受 `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)` 保护（远端配额分片不可达不会无限阻塞清理，因为超限 `AuxOutbox` 任务会在 L0 刷盘时自动溢出写入本地 `[0x07]` 表）。请重点检查本地 NVMe 刷盘延迟与快照传输状态。
