# 单机与分布式多单元（Multi-Cell）集群部署（简体中文）

* **返回指南首页**: [简体中文概览](./README.md) | [文档中心](../../README.md)

The current binary runs a persistent deployment only as `all-in-one`. A second `all-in-one` process can join the same cell. Role-split processes and the multi-cell topology are a design target, not a deployment this repository runs. The `100B`–`10T+` range is a design model, not a measurement of this repository. Trillion-scale figures in `arXiv:2610.32247v1` are Baidu production metrics, not results of this repository.

> **What this tree runs**: persistent LSM, Raft replication, same-process shard splits, and all-in-one gRPC/CLI have tests in this workspace. Only `all-in-one` starts a daemon. `be`, `proxy`, `cell-manager`, `global-manager`, and remote `RegisterCell` are rejected. Jumbo isolation is a route flag and does not move peer shards. See [correctness notes](../../designs/correctness_hardening.md).

---

## 阶段一：独立单节点部署 (`metadbd --role all-in-one`) `[已实现]`

`[已实现]` means one `metadbd --role all-in-one` process in current source. It is not a multi-cell deployment. `MetaDbEngine::new_single_node` is an in-memory simulator with no disk and no Raft, not this process.

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

[security]
# Loopback development only. Literal 127.0.0.1. Remove this flag when you set shared_secret.
allow_unauthenticated = true
```

### 启动单机服务

工具链固定为 Rust **1.100.0**（`rust-toolchain.toml`）。当前只有 `all-in-one` 会真正启动；`be` / `proxy` / `cell-manager` / `global-manager` 会被拒绝，而不是静默变成全功能进程。

The `[security]` block above is the explicit loopback development opt-in. It is not a credential. `grpc_listen_addr` and `metrics_listen_addr` stay on the literal IP `127.0.0.1`. The hostname `localhost` is not accepted for `allow_unauthenticated`. An empty secret without that flag does not start the process.

```bash
metadbd --config /etc/metadb/standalone.toml
```

To require `authorization: Bearer <secret>` on metadata, transaction, control-plane, Raft transport, and `GET /metrics`, replace that `[security]` block. Do not leave `allow_unauthenticated` in the file. Both together are rejected. A whitespace-only `shared_secret` is rejected even if the flag remains. Peer Raft clients send the same token. `metadb-ctl --auth-token` (or `METADB_AUTH_TOKEN`) and `metadbd --join` / `--cell-manager` send that header.

```toml
[security]
shared_secret = "replace-with-a-long-random-string"
```

---

## 阶段二：单节点动态 `PID` 边界分片分裂 `[已实现]`

`[已实现]` means in-process PID-boundary splits inside that same all-in-one process. It is not a multi-host topology. Jumbo isolation is a route flag and does not move peer shards.

当分片容量超过 `shard_split_threshold_bytes` 时，`PolicyShardSplitter` 会扫描 SSTable 块索引中点并严格沿着 `group_key = PID`（`[0x01][pid: 8B BE]`）边界执行原子分裂：
* 同一目录的属性记录 `(PID, "_attr_")` 与所有直接子项 `(PID, child_name)` 始终保持在同一分片内。
* 单机上的多个 OpenRaft 分片组可并行刷盘与压缩，充分压榨多核与 NVMe 多队列吞吐。

---

## Stage 3: Intended multi-cell topology (not deployable today)

Stage 3 is a design target. The current binary does not deploy it.

`metadbd` only runs `--role all-in-one`. `PersistentMetaDbCluster::open_and_serve` in `crates/metadb-server/src/lib.rs` rejects `be`, `proxy`, `cell-manager`, and `global-manager` before it listens. `RegisterCell` stores a second all-in-one cell in the caller's `GLOBAL_CELL_DIRECTORY` after that cell's routes cover the requested range. The caller does not forward keys. This is not a disaggregated deployment.

A second `metadbd --role all-in-one` can join the **same** cell when `--join` or `--cell-manager` names the current leader and the data directory is fresh. That path is covered by `crates/metadb-server/tests/join_existing_group.rs`. The joiner is still an all-in-one process. It is not a role-trimmed backend, and the join does not create another cell. See [Operations](../../zh/operations.md).

### Step 3.1: Voters inside one cell

Do not start `metadbd --role be`. That role exits in `open_and_serve`, so those commands are not a runbook. `metadb-ctl raft scale-replicas` calls `ScaleShardReplicas` and requires the requested voter count to already be registered in the cell. A fresh all-in-one cell has one node, so `--target-voters 3` fails until those nodes exist. The constraints are in [Operations](../../zh/operations.md).

### Step 3.2: Design model for a disaggregated topology (`100B – 10T+` records)

`100B – 10T+` is a design-model range: arithmetic for how a future multi-cell topology might be sized. It is not a record count this repository has stored. It is also not a Baidu production figure claimed as a result of `rustfs/MetaDB`. Baidu's published sizes, including the BOS2 figure of 1.848 trillion records in `arXiv:2610.32247v1`, are Baidu production metrics, not results of this repository.

The table is the intended role split. This binary does not run those roles. Jumbo isolation in this tree is a route flag. It does not move peer shards onto a dedicated host.

| Role | Design `--role` | Intended responsibility |
| :--- | :--- | :--- |
| **Global Manager** | `global-manager` | Would map coarse `[start_key, end_key)` intervals to `CellId`, allocate `65,536`-ID blocks, and coordinate cross-Cell directory rename leases. |
| **Cell Local Manager** | `cell-manager` | Would process BE heartbeats (`20K–50K` shards/Cell) and orchestrate `PID` splits and membership changes. |
| **Backend Storage (`BE`)** | `be` | Would host Multi-Raft shard groups and the LSM engine on NVMe hosts. |
| **Stateless Proxy** | `proxy` | Would embed `MetaCache`, compile the 20 SQL templates, and coordinate cross-shard 2PC. |

`metadb-ctl cell register` calls `RegisterCell`. Without `--start-key` and `--end-key` it sends `[01, 04)`, which overlaps a default cell and exits non-zero. With those keys it persists the cell only when the endpoint's routes cover that range. It does not forward keys.
