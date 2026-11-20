# Rust SDK、gRPC 接口与 20 条 SQL 模板参考（简体中文）

* **返回指南首页**: [简体中文概览](./README.md) | [文档中心](../../README.md)

MetaDB 提供三类客户端接入方式：
1. **嵌入式 Rust SDK (`metadb-server`)** — 同进程零序列化调用（提供 `PersistentMetaDbCluster` 异步持久化版与 `MetaDbEngine` 同步内存版）。
2. **强类型 `tonic` gRPC 接口 (`metadb-proto` & `metadb-server`)** — 面向分布式 RustFS 数据节点与代理的高吞吐多路复用 RPC。
3. **20 条轻量级元数据 SQL 模板 (`metadb-proxy`)** — `sql_templates_tests` covers all 20 templates. A compile time under `5µs` is not a measurement of this repository.

---

## 1. gRPC 服务契约概览 (`metadb.v1`) `[已实现]`

`[已实现]` means the process that hosts the shard serves these RPCs. A `global-manager` does not allocate ids or forward a key. `RegisterCell` persists a remote cell on the directory process after `GetRoutes` shows exact coverage. It does not forward later metadata calls. `be` and `proxy` are rejected before listen.

* **`MetadataService` (`metadata.proto`)**：
  * 单分片 `1-RTT` 写操作：`CreateFile`、`Mkdir`、`Unlink`、`Rmdir`、`SetAttr`（`chmod`、`chown`、`truncate`、`utimes`）。
  * 线性一致读操作（`ReadIndex`）：`Lookup`、`DirStat`（含读取路径实时对账）、`ListDir`（支持 `after_name` + `limit` 游标分页）、`ExecuteSql`。
  * 配额与反向二级索引查询：`GetQuota`、`SetQuotaLimit`、`ListQuotas`、`GetSecondaryIndex` (`FID -> (PID, name)`)、`ScanSecondaryIndex`。
* **`TransactionService` (`transaction.proto`)**：
  * `Rename(src_pid, src_name, dst_pid, dst_name, overwrite)`（同分片自动走 `1-RTT`，跨分片自动走主锁锚定 2PC），以及内部 2PC / 异步辅助批次接口 `Prepare2pc`、`CommitPrimary2pc`、`CommitSecondary2pc`、`Abort2pc`、`CheckTxnStatus`、`ApplyAuxBatch`。
* **`ControlPlaneService` (`control_plane.proto`)**：
  * `GetRoutes`、`AllocateIdBlock`、`StoreHeartbeat`、`TriggerShardSplit`、`EnableDualModeSplit`、`ScaleShardReplicas`、`RegisterCell`。

---

## 2. 全部 20 条元数据 SQL 模板完整参考 (`metadb-proxy`)

> **保留文件名规则**：用户创建、普通查找或重命名文件/目录时禁止使用保留名 `'_attr_'` 或包含空字符 `'\0'`（返回 `MetaDbError::ReservedEntryName`）。`name = '_attr_'` 仅专用于模板 #7 查询目录自身属性。

### A. 命名空间写入与重命名模板 (`MetaTable`)

| # | SQL 模板 | 语义与执行路径 | 状态 |
| :--- | :--- | :--- | :--- |
| **1** | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | 创建文件（`1-RTT` 下推 + 异步目录属性与配额/二级索引入队） | 已实现 |
| **2** | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | 创建子目录（同分片 `1-RTT` 或跨分片 `2PC`） | 已实现 |
| **3** | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | 删除普通文件（`1-RTT` 下推） | 已实现 |
| **4** | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | 删除空目录（权威存活子项扫描校验） | 已实现 |
| **5** | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | 排他重命名（目标已存在时返回 `AlreadyExists`） | 已实现 |
| **6** | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | POSIX 覆盖重命名（自动扣减被覆盖文件的配额与二级索引） | 已实现 |

### B. 命名空间点查与分页扫描模板 (`MetaTable`)

| # | SQL 模板 | 语义与执行路径 | 状态 |
| :--- | :--- | :--- | :--- |
| **7** | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | 线性一致 `DirStat`（`ReadIndex` + 读取路径内存增量对账） | 已实现 |
| **8** | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | 线性一致子项点查 `Lookup` | 已实现 |
| **9** | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | 目录扫描（受默认单页上限保护） | 已实现 |
| **10** | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | 游标分页 `ListDir`（大目录推荐用法） | 已实现 |
| **11** | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | 前缀过滤分页列目录（支持 `'prefix%'`） | 已实现 |

### C. Inode 属性原地修改模板 (`MetaTable`)

| # | SQL 模板 | 语义与执行路径 | 状态 |
| :--- | :--- | :--- | :--- |
| **12** | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | 修改权限位 `chmod`（`1-RTT` 下推） | 已实现 |
| **13** | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | 修改属主 `chown` + 异步配额双向变更 | 已实现 |
| **14** | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | 提交文件大小与数据面 `FID` 映射 | 已实现 |
| **15** | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | 更新修改时间戳 `utimes`（`1-RTT` 下推） | 已实现 |

### D. 用户配额表与二级反向索引表模板 (`QuotaTable` & `SecondaryIndexTable`)

| # | SQL 模板 | 语义与执行路径 | 状态 |
| :--- | :--- | :--- | :--- |
| **16** | `SELECT * FROM QuotaTable WHERE uid = ?` | 查询指定用户的已用字节数、已用 Inode 数及配额上限 | 已实现 |
| **17** | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | 设置或更新用户存储配额上限 | 已实现 |
| **18** | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | 分页扫描用户配额账本 | 已实现 |
| **19** | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | 根据数据块 `FID` 反查 `(PID, name, create_time)` | 已实现 |
| **20** | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | 数据面垃圾回收（GC）/ 巡检游标分页扫描 | 已实现 |
