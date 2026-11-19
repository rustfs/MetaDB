# 测试与压测

[概览](./README.md) · [English](../en/testing.md) · [Français](../fr/testing.md) · [Deutsch](../de/testing.md) · [日本語](../ja/testing.md)

在仓库根目录用固定工具链（Rust 1.100.0）运行。

## 检查

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` 会构建库、集成测试和两个二进制。没有 `[[bench]]` 目标，也没有 `benches/` 目录。本树自带的负载发生器是下面的 `metadb-ctl bench` 和 `cargo test -p metadb-server --test stress_harness`。

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
cargo test -p metadb-server --test stress_harness
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

标志：`--workload` 为 `hotspot-create`（默认）、`bulk-ttl-delete` 或 `mixed`。对远程负载，省略 `--concurrency` 时为 16，`--ops` 默认为 1000。只有走嵌入路径时才读取 `--profile`。`--embedded` 或 `--workload mixed` 不拨号 `--endpoint`，也不使用 `--ops`。它会启动一个临时的 all-in-one 节点并运行进程内压力工具。见 [进程内压力工具](#进程内压力工具)。

来自 `crates/metadb-server/src/bin/metadb-ctl.rs` 的远程负载行为：

- 客户端分配一个目录 id，再分配长度为 `--ops` 的文件 inode 和文件 id 块，并拒绝数量或结尾溢出的授予。
- 它打印一个包含 `bench_pid`、`workload` 和 `requested_ops` 的 JSON 对象，然后在新目录下创建同样数量的 4096 字节文件。
- 每个阶段打印包含 `attempted`、`succeeded`、`failed`、`transport_failed`、`application_failed`、`errors_by_kind` 以及延迟分位数（`p50_ms`、`p99_ms`、`p999_ms`）的 JSON。若 `succeeded` 不等于 `--ops`，进程以非零状态退出。
- `bulk-ttl-delete` 删除已创建的名字，再用 `limit = 1` 列举该目录，并打印这一页是否为空。
- 再跑一次会分配新的 `bench_pid`。它不复用固定的 inode，也不把被拒绝的创建记为成功。

不要把这条命令打出的数字当作仓库结果。远程运行取决于机器、构建配置和你指向的服务器。嵌入运行取决于机器和构建配置。本树没有记录其中任何一份样本。

## 进程内压力工具

`crates/metadb-server/tests/stress_harness.rs` 和 `metadb-ctl bench --embedded` 都调用 `metadb_server::stress`（`crates/metadb-server/src/stress.rs`）。没有名为 `mixed` 的第三种配置。`--workload mixed` 选择的就是这套工具。两种配置跑同一套混合负载：在一个临时数据目录上的持久 all-in-one 节点里做 create、lookup、dirstat、listdir、rename 和 unlink。计时循环之后，工具运行维护节拍，再按成功的操作核对目录列举、dirstat 和配额。维护节拍失败会给 `errors` 加一，把 `correctness_errors` 设为 1，并跳过这次核对。这次运行是一个操作系统进程和一个数据目录。它不是三个 `metadbd` 进程，也不是 [官方前缀负载](../en/testing.md#prefix-load)。

| 配置 | 测试 | 时长 | 并发 | 目录数 | 何时运行 |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `smoke` | `stress_smoke_mixed_workload` | 5 秒 | 4 | 4 | `cargo test -p metadb-server --test stress_harness`，包括 `cargo test --workspace`。忽略 `METADB_STRESS_*`。 |
| `local` | `stress_local_mixed_workload` | 除非覆盖，否则 30 秒 | 除非覆盖，否则 8 | 除非覆盖，否则 16 | `#[ignore]`。不属于 `cargo test --workspace`。读取 `METADB_STRESS_DURATION_SECS`、`METADB_STRESS_CONCURRENCY`、`METADB_STRESS_KEYSPACE` 和 `METADB_STRESS_SEED`。 |

```bash
cargo test -p metadb-server --test stress_harness stress_smoke -- --nocapture

METADB_STRESS_DURATION_SECS=120 \
METADB_STRESS_CONCURRENCY=16 \
METADB_STRESS_KEYSPACE=32 \
METADB_STRESS_SEED=7 \
  cargo test -p metadb-server --test stress_harness stress_local -- --ignored --nocapture

cargo run -p metadb-server --bin metadb-ctl -- bench --embedded --profile smoke
cargo run -p metadb-server --bin metadb-ctl -- bench --embedded --profile local
```

`METADB_STRESS_*` 适用于 local 配置，包括 `metadb-ctl bench --embedded --profile local`。在 `metadb-ctl bench --embedded` 上，`--duration-secs`、`--concurrency`、`--keyspace` 和 `--seed` 会在读完环境变量之后覆盖所选的任一配置。`cargo test` 的 local 配置没有这些标志覆盖，只读上述变量。时长、并发或目录数小于 1 时会被抬到 1。`--profile` 默认为 `smoke`。`--profile ci` 与 smoke 配置相同。其他 `--profile` 值会以错误退出。smoke 配置不读 `METADB_STRESS_*`。远程的 `hotspot-create` 和 `bulk-ttl-delete` 忽略 `--profile`。

测试打印一行，并要求 `errors = 0`、`correctness_errors = 0` 且 `ops > 0`。`metadb-ctl bench --embedded` 会打印 `embedded metadbd listening on <addr>` 和同一行，并在 `errors` 或 `correctness_errors` 大于 0 时以非零状态退出。这一行的字段是：

```text
metadb stress profile=<name> ops=<count> errors=<count> correctness_errors=<count> elapsed_ms=<ms> ops_per_s=<rate> latency_us p50=<us> p99=<us> max=<us>
```

`ops` 只统计混合循环里成功的操作。`ops_per_s` 用该计数除以经过的时间。`errors` 统计混合循环里没有成功的操作。维护节拍失败也会加进 `errors`，并把 `correctness_errors` 设为 1，而不再跑核对。核对真正跑起来时，`correctness_errors` 既统计目录列举、dirstat 和配额的不一致，也统计失败的核对调用（`dirstat`、`listdir` 和 `get_quota`）。`latency_us` 的 `p50`、`p99` 和 `max` 是每一次混合循环尝试的延迟分位数，包括失败的尝试，单位是微秒。它们不是 `ops` 的分位数。它们会被打印。本树不强制延迟门槛。

任一命令打出的一行，都是打出它的那台机器上的结果。本仓库不把这一行记入仓库。它不是服务级结果，不是首次交付通过，也不是官方的 1,000,000 次创建前缀负载。

## 持久拓扑与加入

这些命令在本树中。CI 用 `cargo test --workspace --locked` 运行它们。

- `cargo test -p metadb-server --test topology_single_node` 在临时目录上打开 `PersistentMetaDbCluster`。
- `cargo test -p metadb-server --test topology_scale_out` 在该进程里强制一次 PID 边界分裂。
- `cargo test -p metadb-server --test topology_distributed` 使用一个进程，通过 `DurableCellManager::scale_out_replicas` 建立三个 localhost 投票者，包括创建负载下的领导者丢失。
- `cargo test -p metadb-server --test stress_crash_restart` 在一次创建还在 `propose` 之内时不刷 memtable 就停止，然后重新打开同一个 `data_dir`。
- `cargo test -p metadb-server --test join_existing_group` 覆盖两种不同的加入。`metadbd_processes_join_existing_group_and_fail_over` 启动真正的 `metadbd` 进程，加入第一个进程的 Raft 组，并做故障转移。`join_sends_the_shared_secret` 在测试进程里打开两个 `PersistentMetaDbCluster`，并检查这次加入发送了共享密钥。这一例不启动两个 `metadbd` 操作系统进程。
- `open_and_serve` 仍然拒绝除 `all-in-one` 以外的每个角色。第二个单元仍未实现。`RegisterCell` 返回错误。

冒烟配置和被忽略的 local 配置见 [进程内压力工具](#进程内压力工具)。`topology_distributed` 仍是一个操作系统进程里的三个 localhost Raft 投票者，不是三个 `metadbd` 进程。拆开的角色和第二个单元仍不是真实部署。
