# 运维

[概览](./README.md) · [English](../en/operations.md) · [Français](../fr/operations.md) · [Deutsch](../de/operations.md) · [日本語](../ja/operations.md)

## 数据目录

`bootstrap_table_shards = true` 引导之后：

```text
<data_dir>/
  global_allocator/ID_ALLOCATOR_STATE
  cell_<cell_id>/CELL_MANAGER_STATE
  be/NODE_SHARD_REGISTRY
  be/shards/00000000000000000001/lsm/     # MetaTable
  be/shards/00000000000000000001/raft/
  be/shards/00000000000000000002/...      # QuotaTable
  be/shards/00000000000000000003/...      # SecondaryIndexTable
```

分片标识是十进制，左侧补零到 20 位。分裂产生的子分片是 `be/shards/` 下的更多目录。`CELL_MANAGER_STATE` 和 `ID_ALLOCATOR_STATE` 是 CRC32C 帧。分片目录还在时不要删除 `NODE_SHARD_REGISTRY`；宿主用它恢复分裂子分片。

复制目录之前用 Ctrl-C（或 `PersistentMetaDbCluster::shutdown`）停止进程。活着的进程为每个分片持有一个 LSM `LOCK` 文件。

## `metadb-ctl`

全局标志：

| 标志 | 默认 | 含义 |
| :--- | :--- | :--- |
| `--endpoint` | `127.0.0.1:7800` | `主机:端口`，或已经以 `http://` 或 `https://` 开头的 URL |
| `--auth-token` | 空 | 持有者令牌。也可从 `METADB_AUTH_TOKEN` 读取 |

客户端为每条命令打开一条新的 gRPC 通道。它不说 `RaftTransportService`。

### `cluster status`

以 `cell_id = 0` 调用 `GetRoutes`。对每个分片打印 `shard`、`cell`、`leader`、投票者数量、`epoch=(conf_ver, ver)`、`dual_mode` 和 `jumbo`。It then prints local `CELL_CONTROL_PLANE` placement rows (`phase`, `phase_code`, `dedicated_node`, `dominant_pid`, `moves`, attempt timestamps, `reason`) and `placement_automatic_transfer_enabled=false`. Those rows are not routes. A remote cell contributes no placement rows. It does not print disk free space, QPS, Raft watermarks, or `MOVE_STAGED` / `MOVE_LIVE`. The reconciler does not publish phase `recorded`. It does not stamp `transfer_attempt_unix_ms`. A new row stores 0. A later reconcile keeps a non-zero value already in the file.

### `shard list --cell-id <u64>`

为该单元调用 `GetRoutes`（`0` 表示每条缓存路由）。打印 `shard`、`cell`、原始起止键字节，以及纪元 `(conf_ver, version)`，然后打印同一进程上该单元的 placement 行。A remote cell prints `Placements: 0`.

### `shard split --shard-id <u64>`

调用 `TriggerShardSplit`，即使分片低于 `shard_split_threshold_bytes` 也强制做分裂检查。On success it prints `split_executed`, `child_shard_id`, and the placement fields. When no placement row exists, including an ordinary split, the line is `placement_phase=` and `placement_phase_code=0`. Code 0 is only that no-record case. Stored rows use codes 1–4. `split_executed=false` 表示没有创建子分片（存活键少于两个，或巨型候选写入 placement 行后仍保持 `split_executed=false`）。

### `shard enable-dual-mode --shard-id <u64> --pid <u64>`

调用 `EnableDualModeSplit`。命令行发送空的 `split_name`。服务器保存 `dual_mode_pid`，并不立刻分裂。

### `raft scale-replicas --cell-id <u64> --target-voters <u32>`

调用 `ScaleShardReplicas`。服务器要求该单元的节点表里已经有 `target_voters` 条记录。新的 all-in-one 单元只有一个节点，因此 `--target-voters 3` 返回形如 `need 3 registered nodes in cell <id>, have 1` 的共识错误。成功时打印有多少本地分片被交给成员变更。

### `cell register --cell-id <u64> --endpoints <a,b,c>`

调用 `RegisterCell`。只有端点的路由恰好覆盖请求范围时，服务器才对 `GLOBAL_CELL_DIRECTORY` 做 fsync。省略键会发送 `[01, 04)`，它与默认单元重叠，`metadb-ctl` 以非零状态退出。该单元包含的后续请求会转发到已保存的端点。

### `sql <语句>`

调用不带绑定参数的 `ExecuteSql`。语句必须是 [架构](./architecture.md) 中 20 条模板之一，用字面量代替 `?`。

```bash
metadb-ctl --endpoint 127.0.0.1:7800 sql \
  "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
```

### `bench`

See [Tests and benches](./testing.md#metadb-ctl-bench). Remote `hotspot-create` and `bulk-ttl-delete` are a client against a running server. They print JSON with `attempted`, `succeeded`, and `failed`, and the process exits non-zero unless `succeeded` equals `--ops`. `--embedded` and `--workload mixed` run the [in-process stress harness](./testing.md#in-process-stress-harness) instead of dialing the endpoint. That path passes when `errors` and `correctness_errors` are both zero, and it prints latency percentiles in microseconds. Neither path is a Cargo benchmark target. The repository does not check in a throughput number for either path. A printed line is not a service-level result and is not the official 1,000,000-create profile.

The separate prefix-load harness (100 prefixes × 10,000 creates, then 100,000 `SetAttr` overwrites, then 100,000 deletes, concurrency 64) is documented in [Tests and benches](../en/testing.md#prefix-load). GitHub Actions runs only the small profile. It does not run the 1,000,000-create profile.

## 指标

设置 `node.metrics_listen_addr`（例如 `127.0.0.1:9100`）。`open_and_serve` 在该地址接受 TCP 连接。`GET /metrics` 返回 Prometheus 文本。其他路径返回 404，其他方法返回 405。`[security].shared_secret` 非空时，该监听器上的每个请求都必须发送 `Authorization: Bearer <shared_secret>`。缺失、空或错误的令牌返回 HTTP 401，正文不是指标文本，包括对 `/status` 的请求。An empty secret does not start the process on any address, including loopback, unless `allow_unauthenticated = true`. That flag is loopback-only and still refuses a wildcard or other non-loopback address. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.

`render_prometheus_metrics` 的计量与计数：

| 名称 | 种类 |
| :--- | :--- |
| `metadb_cells_total` | gauge |
| `metadb_shards_total` | gauge |
| `metadb_in_memory_mvcc_writes_total` | counter |
| `metadb_pruned_mvcc_versions_total` | counter |
| `metadb_persistent_kv_writes_total` | counter |
| `metadb_active_physical_tombstones` | gauge |
| `metadb_tombstones_traversed_on_scans_total` | counter |
| `metadb_locality_compactions_total` | counter |
| `metadb_min_raft_safe_purge_index` | gauge |
| `metadb_placement_records` | gauge, local placement rows |
| `metadb_placement_phase_recorded` | gauge; the reconciler does not publish this phase |
| `metadb_placement_phase_leader_on_dedicated` | gauge |
| `metadb_placement_phase_dedicated_not_shared` | gauge |
| `metadb_placement_phase_blocked` | gauge |
| `metadb_placement_moves_leadership` | gauge |
| `metadb_placement_moves_voter_replaced` | gauge |
| `metadb_placement_rows_with_transfer_attempt` | gauge; the reconciler does not stamp a new value and keeps one already in the file |
| `metadb_placement_rows_with_replace_attempt` | gauge |
| `metadb_placement_automatic_transfer_enabled` | gauge, 0 in this build |

There is no `metadb_raft_watermark_lag_entries` series and no per-shard placement label. There is no `MOVE_STAGED` or `MOVE_LIVE` series. `metadb_cells_total` counts rows in this process's `GLOBAL_CELL_DIRECTORY`, including a remote cell after `RegisterCell`. `metadb_shards_total` counts only local shards. A fresh process has one cell. The English operability page lists the readiness gauges that this table does not repeat.

## 扩容

在一个进程内有效的做法：

- 更多分片。增长超过 `shard_split_threshold_bytes`，或 `metadb-ctl shard split`，可以在同一 `be/shards` 目录下增加分片组。父分片保留左侧键范围，子分片在本节点上作为另一个 1 投票者组启动。重启会重新打开两者。
- 双模式标志。`shard enable-dual-mode` 标记一个父标识。该分片之后的分裂可以按 `(pid, name)` 切开。
- 巨型标志。当一个父项超过该分片上 `MetaTable` 字节的 85%，或分片只有一个父项时，路由变为 `JumboIsolated`。专用节点标识从本单元已注册的节点中选出。只有一个节点时，标志指向本进程。标志本身不会把文件搬到另一台主机。回环上的 `MOVE_STAGED` / `MOVE_LIVE` 可以把目录放到 `127.0.0.1` 监听器。`metadbd` 不启动该监听器，不打开 Raft，不调用 `transfer_leader`，也不是跨主机的生产拷贝。见 `docs/en/scale-control-plane-status.md`。
- 进程内投票者。当这些节点标识已经注册，**并且** 每个地址已经为该分片的组提供 `RaftTransportService` 时，`DurableCellManager::scale_out_replicas` 可以添加 learner 并提升它们。`crates/metadb-raft/tests/multi_raft_failover_tests.rs` 用一个测试进程里的三个监听器和基本 KV 状态机做这件事，不是元数据状态机，也不是三个 `metadbd` 进程。

这些进程会做的事：

1. `metadbd --role be` 托管本地 Raft 副本并提供它们的元数据。它不发布单元，不分配标识块，也不发放重命名租约。第二个 `be` 可以用 `[cluster] join` 加入第一个。`metadbd --role proxy` 需要 `[cluster] upstream`，并且不打开 Raft 组。元数据调用、`ExecuteSql` 或带用户键的事务会拨到包含它的那一个已注册单元，并带上 `x-metadb-cell-forwarded`。目录缺失、请求不被一个单元包含、键分属两个单元，或范围只与单元重叠时，返回放置错误，不会发给 upstream。`CommitPrimary2pc`、`ApplyAuxBatch`、`ForwardRouter` 和 `GetRoutes` 仍可以走该 upstream。`AllocateIdBlock` 会被拒绝。`--role global-manager` 提供 `GLOBAL_CELL_DIRECTORY`，打开 `shared_id_allocator/SHARED_ID_ALLOCATOR`，并从该文件授予 `AllocateIdBlock`。被一个已注册单元完整包含的请求也会转发到该单元。它不打开分片组，因此该进程的目录文件不会复制到 Raft 日志上，本身也不是 Raft 组。`--role cell-manager` 托管一个本地单元，并可以发布它。目录文件仍然不是自己的 Raft 组。引导领导者会把目录和 `CELL_MANAGER_STATE` 快照复制到编号最低的本地分片的现有 Raft 日志中。
2. 第二个 `metadbd --role all-in-one` 在数据目录为空、且 `--join` 或 `--cell-manager` 指向当前 leader 时，会加入已有 Raft 组。leader 先追加一条空日志并做快照，再把加入者从 learner 提升为 voter。`--join` 必须打到该 leader。已经有 Raft 状态的目录会重新打开本地状态，并忽略新的 join 地址。节点号 2 到 32767 的全新分配器从该节点自己的 inode、file id 和 shard id 命名空间起步，因此不会重发节点 1 已经发出的 id。已经发过低位 id 的分配器文件不会被搬迁。已知 leader 的 follower 上，`Lookup`、列目录、目录统计、`GetDirPartitionStat`、配额读和二级索引读会转发到该 leader。不知道 leader 时仍返回 `NotLeader`。成员变更处于 joint 时，`GetRoutes` 报告已经应用的全部投票者、当前 leader 和应用后的 epoch。统一配置提交后，这些投票者和 `conf_ver` 与 Raft 一致。加入在 45 秒内重试暂时性错误，然后关闭监听和本地 Raft 组并返回错误。`metadbd` 结束是因为启动返回了这个错误。第一次失败本身不会中止进程。
3. `metadb-ctl cell register` 只在该单元的路由覆盖请求范围之后记录第二个单元。此后，被该单元包含的请求会被转发。配置有效时，`open_and_serve` 会启动 `all-in-one`、`be`、`proxy`、`cell-manager` 和 `global-manager`。
4. 测试 `T1`–`T6` 使用的内存路径 `MetaDbEngine::new_single_node` 可以把内存中的投票者计数加到 3。那个对象没有磁盘，也没有 Raft。它不是 `metadbd` 的扩容。

`topology_distributed` still uses `DurableCellManager::scale_out_replicas` with localhost voters inside one OS process. `join_existing_group` starts real `metadbd` processes and joins them. The shared-secret case in that file opens two clusters inside the test process; it does not start two `metadbd` OS processes. 配置有效时，`be`、`proxy`、`cell-manager` 和 `global-manager` 都会启动。这不是拆分后的生产部署。第二个单元可以记在本进程的目录里；这不是生产多单元部署。 This process can copy its own jumbo shard directory onto `jumbo_dedicated_root`. A loopback `MOVE_STAGED` / `MOVE_LIVE` placement is specified in `docs/en/scale-control-plane-status.md`. It does not call `transfer_leader`, does not replicate the cell directory, and is not a production move.

## 会遇到的错误

变体定义在 `crates/metadb-core/src/error.rs`。gRPC 方法通常把变体放在响应的 `error` 字段里，而不是非 OK 状态。

| 变体 | 代码何时抛出 |
| :--- | :--- |
| `ReservedEntryName` | 空名称、NUL，或把 `_attr_` 用作用户条目 |
| `AlreadyExists` / `NotFound` | 创建或查找 `(pid, name)` |
| `DirectoryNotEmpty` | 仍有子项时 `rmdir`，包括属性尚未刷出之前 |
| `InvalidEntryType` | 对文件 `rmdir`，或对目录 `unlink` |
| `TransactionTimeout` | 快照 HLC 早于内存 MVCC 窗口 |
| `ClockSkewExceeded` | 远端 HLC 领先本地墙钟超过 `max_clock_drift_ms` |
| `EpochMismatch` / `NotLeader` | 过期的路由纪元，或本副本不是 leader。已知 leader 的 follower 读会转发。多键 Prepare 上的 `EpochMismatch` 会放弃这一轮并重新分组，不会把同一批再提议一次 |
| `RetryableConsensus` | 写日志在换主时被丢弃，或法定人数没有应答。同分片写和 2PC 会在现有路由预算内重试。每次逻辑写使用同一个提案 id。若该 id 已经提交，分片返回原来的结果，不再执行这条命令。存储失败和已停止的 Raft 节点仍是 `ConsensusError`，不会重试 |
| `UnsplittableJumboShard` | 内存分裂器（`shard.rs`），不是持久巨型路径。持久路径设置 `JumboIsolated` 并在没有子分片时返回成功 |
| `WriteIntentConflict` / `TransactionAborted` | 2PC 冲突，或目录重命名租约已被持有 |
| `InvalidRenameCycle` | 把目录重命名到自身或后代 |
| `ConsensusError` | 已注册节点不足、learner 追赶超时（10 秒），或 OpenRaft 成员错误 |
| `SqlParseError` | 语句不是 20 条模板之一 |

非法的角色组合和非法 TOML 使用 `CodecError`。`be`、`proxy`、`cell-manager` 和 `global-manager` 不是这种拒绝：配置有效时它们会启动。

## 已知限制

- 配置有效时，`all-in-one`、`cell-manager`、`global-manager`、`be` 和 `proxy` 都会启动。`be` 托管本地副本，不分配标识。`proxy` 把被包含的元数据调用、`ExecuteSql` 或带用户键的事务拨到已注册单元，否则失败关闭。`CommitPrimary2pc`、`ApplyAuxBatch`、`ForwardRouter` 和 `GetRoutes` 仍可以使用 `[cluster] upstream`。`AllocateIdBlock` 会被拒绝。proxy 不托管 Raft。`global-manager` 没有本地分片。它会转发被已注册单元包含的请求，并从 `SHARED_ID_ALLOCATOR` 授予共享标识块。因为它不打开分片组，目录文件不会复制到分片日志上。`cell-manager` 仍把 Raft 组放在同一进程里。
- 第二个 all-in-one 进程可以用 `[cluster] join` 加入同一单元。这次加入不会创建第二个单元。路由覆盖请求范围时，`RegisterCell` 会记录另一个单元。该进程会转发此后被这个单元包含的请求。
- 未知且非零的 `cell_id` 上，`GetRoutes` 失败关闭。`StoreHeartbeat` 不会替换缓存路由。
- 巨型隔离是路由标志加上从本地节点表选出的节点标识。它不会把其他分片从磁盘上赶走。
- Registered-node liveness is fsynced in `CELL_CONTROL_PLANE`. `metadbd` persists its own record about every 3 seconds and sends it to the current leader when this process is not the leader. Descriptors on that RPC are annotations, not routes. The observation does not schedule placement. See [Scale control plane](../en/scale-control-plane-status.md).
- A secret that contains a non-whitespace character authenticates Raft, the other three gRPC services, and the metrics listener, including `/livez`, `/readyz`, and `/healthz`, with the same bearer. An empty secret does not start the process. `allow_unauthenticated = true` is the explicit exception for literal loopback IPs (`127.0.0.0/8` or `::1`) and still refuses a wildcard bind. The hostname `localhost` is not accepted for that exception. The bound socket is checked again after listen. A whitespace-only secret is rejected and is not compared as a bearer. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.
- `memtable_size_bytes` 会封印并刷出活动 memtable。`max_imm_memtables` 限制已排队的不可变表数量。`[raft]` timers and `max_segment_bytes` apply when a group starts. They are not reloaded from a changed file, and the segment size does not rewrite segments already on disk.
- `hotspot-create` 和 `bulk-ttl-delete` 的 `metadb-ctl bench` 打印 JSON 行：先是带 `bench_pid` 的头，然后是包含 attempted、succeeded、failed 和延迟分位数的阶段报告。只要有一次请求的操作失败，进程就以非零状态退出。`--workload mixed` 或 `--embedded` 运行进程内的压力工具。`sql` 把响应打印成 JSON，遇到应用错误时以非零状态退出。

## 文档缺口

下列能力产品里没有，因此本指南无法为它们写出步骤：

1. 不是覆盖全部分片的生产 MetaCache，也不是拆分后的生产部署。配置有效时，`be`、`proxy`、`cell-manager` 和 `global-manager` 都会启动。
2. `RegisterCell` 不再保持为错误。路由覆盖范围时它会写入 `GLOBAL_CELL_DIRECTORY`，此后被该单元包含的请求会被转发。join 仍必须到达当前 leader；follower 不会转发成员变更。
3. 把巨型分片放到独立机器上，并把其他分片从那台机器移走。回环监听器可以用 `MOVE_STAGED` / `MOVE_LIVE` 接收一个巨型目录。它不是 `metadbd`，没有鉴权，不打开 Raft，不调用 `transfer_leader`，也不是搬到另一台主机的生产拷贝。
4. 本进程内的 TLS。进程不终结 TLS。要求 TLS 或证书的配置键会被拒绝，而不是以明文提供服务。请在外部代理终结 TLS。密钥非空时，Raft 和指标与其他服务使用同一个持有者令牌；这不是双向 TLS。由心跳驱动的控制循环没有实现。
5. A gRPC health RPC, probes when `metrics_listen_addr` is unset, and per-shard Prometheus series. `/readyz` does not wait for the 250 ms 2PC recovery pass. A leader election makes `/readyz` and `/healthz` return 503 on replicas that do not know a leader. `[raft]` is loaded and is not `#[serde(skip)]`. Its values apply when a group starts and are not reloaded. See [Operability](../en/operability.md).
6. A checked-in service-level result for `metadb-ctl bench`. The command prints numbers for the machine you run it on. This repository does not record one. See [Tests and benches](./testing.md).
