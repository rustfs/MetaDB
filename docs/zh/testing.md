# 测试与压测

[概览](./README.md) · [English](../en/testing.md) · [Français](../fr/testing.md) · [Deutsch](../de/testing.md) · [日本語](../ja/testing.md)

在仓库根目录用固定工具链（Rust 1.100.0）运行。

## 检查

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` 会构建库、集成测试和两个二进制。没有 `[[bench]]` 目标，也没有 `benches/` 目录。本树自带的负载发生器是下面的 `metadb-ctl bench`。

较窄的常用命令：

```bash
cargo test -p metadb-server --test integration_tests
cargo test -p metadb-server --test e2e_cluster_evolution_tests
cargo test -p metadb-raft --test multi_raft_failover_tests
cargo test -p metadb-raft --test single_voter_recovery_tests
cargo test -p metadb-cluster --test epoch_split_tests
cargo test -p metadb-cluster --test dual_mode_jumbo_tests
cargo test -p metadb-cluster --test split_crash_recovery_tests
cargo test -p metadb-tx --test two_pc_crash_tests
cargo test -p metadb-tx --test aux_idempotency_tests
cargo test -p metadb-tx --test attr_reconcile_race_tests
cargo test -p metadb-proxy --test sql_templates_tests
cargo test -p metadb-storage --test lsm_recovery_tests
cargo test -p metadb-storage --test mvcc_snapshot_tests
cargo test -p metadb-storage --test locality_compaction_tests
```

## 每个套件实际覆盖什么

### 内存模拟器（`MetaDbEngine::new_single_node`）

`crates/metadb-server/tests/integration_tests.rs` 中的测试 `T1`–`T6` 不打开数据目录。

| 测试 | 检查什么 |
| :--- | :--- |
| `test_t1_pid_boundary_sharding_preserves_directory_colocation` | 按父标识分裂时，一个目录的子项留在一起 |
| `test_t2_jumbo_shard_isolation_and_dual_mode_fallback` | 内存中的巨型错误与双模式回退 |
| `test_t3_conflict_free_async_attr_aggregation_and_read_reconciliation` | 刷盘之前 dirstat 能看到属性增量 |
| `test_t4_decoupled_async_auxiliary_idempotency_and_cross_shard_rename` | 配额与索引队列，以及跨分片重命名 |
| `test_t5_in_memory_mvcc_and_locality_aware_compaction_on_bulk_deletes` | 内存后端上的 MVCC 与局部性压缩 |
| `test_t6_single_node_to_distributed_multi_cell_evolution_and_sql_proxy` | 保留名、SQL、非空 `rmdir`，然后在 **内存** 成员表上做 `register_node` + `scale_out_raft_replicas(3)` |

`T6` 不启动 Raft，也不启动第二个单元进程。这个名字描述的是模拟器的成员对象。

### 持久的单进程

| 测试 | 检查什么 |
| :--- | :--- |
| `test_persistent_restart_lookup_dirstat_quota` | 在临时目录上 `open_and_serve`，关闭，重新打开，lookup / dirstat / 配额 |
| `test_maintenance_tick_purges_raft_log` | 24 次创建之后，一次维护节拍丢弃仍保留的 Raft 条目 |
| `test_e2e_persistent_grpc_cluster_restart_split_2pc_and_locality_compaction` | 一个持久进程、真实 gRPC、重启、强制分裂、重命名 2PC、批量删除、配额、二级索引、Prometheus 文本 |

端到端测试把 `shard_split_threshold_bytes = 2048`、`locality_tombstone_threshold = 20`，以便小夹具越过这些门槛。这些值是测试配置，不是部署默认值。

### Raft，仍然是一个操作系统进程

| 测试 | 检查什么 |
| :--- | :--- |
| `test_single_voter_walless_crash_recovery_and_three_watermark_pinning` | 一个投票者、无 WAL 恢复、清除水位 |
| `test_suffix_only_truncate_final_crc_rejection_and_snapshot_verification` | 截断的尾部和校验和拒绝 |
| `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` | 进程内三个 tonic 监听器、基本 KV 应用处理器、`AddLearner` 到 3 个投票者、停止原领导者、故障转移后的读取 |

故障转移测试在测试进程内绑定 `127.0.0.1:0` 三次。这不是三个操作系统进程，也不提供 `MetadataService`。

### 集群、事务、SQL、存储

| 测试 | 检查什么 |
| :--- | :--- |
| `test_epoch_fenced_live_pid_split_and_durable_id_allocator` | 纪元围栏下的 PID 分裂和持久标识分配器 |
| `test_jumbo_isolation_and_three_way_dual_mode_scatter_gather_and_rmdir` | 持久巨型标志、双模式分散收集、rmdir |
| `split_crash_recovery_tests` | 分裂期间的持久化失败与崩溃点，然后重新打开 |
| `test_two_pc_crash_recovery_posix_overwrite_and_cycle_fencing` | 2PC 恢复、覆盖重命名、环路拒绝 |
| `test_aux_outbox_spill_contiguous_frontiers_gap_rejection_and_cleanup` | `0x07` 溢出、间隙、清理 |
| `test_concurrent_attr_aggregation_read_reconciliation_and_l0_flush_barriers` | 并发属性聚合 |
| `test_all_twenty_metadata_sql_templates_and_attr_isolation` | 全部 20 条模板、字面量与 `?`、`_attr_` 隔离 |
| LSM / MVCC / 局部性测试 | SSTable 与 `MANIFEST` 恢复、`SnapshotGuard`、最底层压缩 |

单元测试也在 `metadb-core`（`hlc`、`schema`）和 `metadb-proto`（`convert`）中。

## `metadb-ctl bench`

先启动 `metadbd`。然后：

```bash
cargo run -p metadb-server --bin metadb-ctl -- \
  --endpoint 127.0.0.1:7800 \
  bench --workload hotspot-create --concurrency 16 --ops 1000

cargo run -p metadb-server --bin metadb-ctl -- \
  --endpoint 127.0.0.1:7800 \
  bench --workload bulk-ttl-delete --concurrency 16 --ops 1000
```

标志：`--workload` 为 `hotspot-create`（默认）、`bulk-ttl-delete` 或 `mixed`。省略 `--concurrency` 时为 16。`--ops` 默认 1000。`--embedded` 或 `--workload mixed` 会启动一个临时的 all-in-one 节点和进程内压力工具，而不是拨号 `--endpoint`。

来自 `crates/metadb-server/src/bin/metadb-ctl.rs` 的远程负载行为：

- 客户端分配一个目录 id，再分配长度为 `--ops` 的文件 inode 和文件 id 块。数量或结尾溢出的授予会被拒绝。
- 它打印一个包含 `bench_pid`、`workload` 和 `requested_ops` 的 JSON 对象，然后在新目录下创建同样数量的 4096 字节文件。
- 每个阶段打印包含 `attempted`、`succeeded`、`failed`、`transport_failed`、`application_failed`、`errors_by_kind` 和延迟分位数的 JSON。若 `succeeded` 不等于 `--ops`，进程以非零状态退出。
- `bulk-ttl-delete` 删除已创建的名字，再用 `limit = 1` 列举该目录。
- 再跑一次会分配新的 `bench_pid`。被拒绝的创建不算成功。

不要把这条命令打出的数字当作仓库结果。它取决于机器、构建配置和你指向的服务器。本树没有记录样本。

## 未合并的拉取请求 2

[拉取请求 2](https://github.com/rustfs/MetaDB/pull/2) 处于打开状态，不属于本分支。它增加持久拓扑测试和一套嵌入式压力工具。它自己的说明写道：

- 单节点测试在临时目录上调用 `PersistentMetaDbCluster::open_and_serve`。
- 扩容测试在同一进程里强制一次 PID 边界分裂。
- 分布式测试是一个进程、三个 localhost Raft 投票者。它调用 `DurableCellManager::scale_out_replicas`。它不启动三个 `metadbd` 操作系统进程。
- `open_and_serve` 仍然拒绝除 `all-in-one` 以外的每个角色。拆开的角色和第二个单元仍未实现。

该拉取请求的报告，在作者的虚拟机上、Rust 1.100.0（未写明硬件），包含本分支没有的那套工具的一行冒烟输出：

```text
metadb stress profile=smoke ops=12219 errors=0 correctness_errors=0 elapsed_ms=5221 ops_per_s=2340.3 latency_us p50=1318 p99=5207 max=12449
```

这一行是一次未合并测试的单次运行。它不是服务目标，本分支上的 `metadb-ctl bench` 也不会复现它。`cargo test -p metadb-server --test topology_distributed` 和 `metadb-ctl bench --embedded` 这类命令只存在于该拉取请求的分支上。
