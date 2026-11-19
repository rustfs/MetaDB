# 配置

[概览](./README.md) · [English](../en/configuration.md) · [Français](../fr/configuration.md) · [Deutsch](../de/configuration.md) · [日本語](../ja/configuration.md)

`metadbd --config` 以及 `ServerTomlConfig::from_toml_file` / `from_toml_str` 读取 TOML（`crates/metadb-server/src/config.rs`）。每个已知节都可以省略。省略的字段取下面的默认值。

A key inside `[security]` other than `shared_secret` and `allow_unauthenticated` fails parsing. TLS-like keys (`tls`, `ssl`, `cert`) and keys named like a secret or an authentication switch (`secret`, `password`, `token`, `credential`, `auth`, `anonymous`, `insecure`) fail anywhere, including nested tables. Unknown keys that do not match those names are still ignored. An empty `shared_secret` does not start the process. Development without credentials requires `allow_unauthenticated = true` and a literal loopback bind (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that flag. A wildcard or any other non-loopback address cannot use it. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected.

## 默认值

| 键 | 默认 | `open_and_serve` 是否应用 |
| :--- | :--- | :--- |
| `node.node_id` | `1` | 是。Raft 节点标识和引导对等体 |
| `node.cell_id` | `1` | 是。目录名 `cell_<id>`，以及本进程注册的唯一单元 |
| `node.role` | `all-in-one` | 是。任何其他值都返回错误 |
| `node.data_dir` | `./metadb-data` | 是 |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | 是。绑定地址 |
| `node.metrics_listen_addr` | 未设置 | Applied only when set. Separate TCP listener. An empty secret does not listen unless `allow_unauthenticated = true`, and that flag is loopback-only |
| `clock.max_clock_drift_ms` | `500` | 是。`HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432`（32 MiB） | 是。计入的字节达到该大小时，封印并刷出活动 memtable。小于 1 的值按 1 处理 |
| `storage.max_imm_memtables` | `4` | 是。写入方会等待，而不是把超过该上限的不可变 memtable 继续排队 |
| `storage.block_cache_bytes` | `67108864`（64 MiB） | 是。16 路块缓存的总预算 |
| `storage.mvcc_retention_ms` | `5000` | 是 |
| `storage.bloom_bits_per_key` | `10` | 是 |
| `storage.locality_tombstone_threshold` | `128` | 是。映射到 `LsmEngineConfig.tombstone_density_threshold` |
| `storage.locality_tombstone_ratio` | `0.50` | 是。乘以 1000，钳制到 1..=1000，存为 `tombstone_min_ratio_permille` |
| `sharding.bootstrap_table_shards` | `true` | 是。`true` 创建分片 1、2、3。`false` 创建覆盖 `[0x01, 0x04)` 的分片 1 |
| `sharding.shard_split_threshold_bytes` | `4294967296`（4 GiB） | 是。分裂检查使用分片的近似大小 |
| `security.shared_secret` | `""` | Empty does not start the process. Loopback without credentials requires `allow_unauthenticated = true`. A non-empty secret requires the same bearer on gRPC and metrics. A whitespace-only secret is rejected |
| `security.allow_unauthenticated` | `false` | Explicit opt-in for an empty secret on a literal loopback IP (`127.0.0.1` or `::1`) only. The hostname `localhost` is not accepted. Rejected together with a usable secret. Rejected on a wildcard or any other non-loopback bind. A whitespace-only secret is rejected even with this flag |

没有 `[raft]` 表。`ServerTomlConfig.raft_options` 被 serde 跳过，保持 `RaftNodeOptions::default()`（心跳 150 ms，选举 450–900 ms，快照日志保留 0，段大小 64 MiB）。


`to_lsm_config` 总是把 `auto_locality_compaction` 设为 `true`。

## 示例

除了明确的数据目录、指标端口和密钥，这个文件与默认值一致。`memtable_size_bytes` 是活动 memtable 的刷盘阈值。`max_imm_memtables` 是不可变表的反压上限。

```toml
[node]
node_id = 1
cell_id = 1
role = "all-in-one"
data_dir = "/var/lib/metadb"
grpc_listen_addr = "127.0.0.1:7800"
metrics_listen_addr = "127.0.0.1:9100"

[clock]
max_clock_drift_ms = 500

[storage]
memtable_size_bytes = 33554432
max_imm_memtables = 4
block_cache_bytes = 67108864
mvcc_retention_ms = 5000
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296

[security]
shared_secret = "replace-with-a-long-random-string"
```

角色字符串是 kebab-case：`all-in-one`、`be`、`proxy`、`cell-manager`、`global-manager`。

命令行标志在解析文件之后覆盖：`--node-id`、`--cell-id`、`--role`、`--listen`、`--data-dir`。`--cell-manager` 不是 TOML 字段。它只是 `metadbd` 的标志。

## 安全

本进程不终结 TLS。没有能打开本机 TLS 的证书配置。文件里如果出现 TLS、SSL 或证书键，解析失败，进程不会以明文提供服务。请在外部代理终结 TLS，并且不要把 TLS 键留在这个文件里。

`shared_secret` 非空时，`MetadataService`、`TransactionService`、`ControlPlaneService` 和 `RaftTransportService` 要求 `authorization` 精确等于 `Bearer <shared_secret>`。`metrics_listen_addr` 上的 `GET /metrics` 要求同一个 `Authorization` 头。缺失的头、空令牌、前缀或更长的令牌都会被拒绝。指标失败响应的正文不是 Prometheus 文本。Raft 对等体发送同一个令牌。`metadb-ctl --auth-token`（环境变量 `METADB_AUTH_TOKEN`）以及 `metadbd --join` / `--cell-manager` 也会发送它。`metadb-ctl cluster status` 调用 `GetRoutes`，走同一个拦截器。

An empty secret does not start the process. Loopback is not an implicit exception. Development without credentials requires `allow_unauthenticated = true` and every listener on a literal loopback IP (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that flag. A wildcard (`0.0.0.0`, `::`) or any other non-loopback address cannot use it. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. Listeners stay plaintext. Keep an opted-in listener on a trusted network, or put every service behind a proxy that authenticates the client and block direct access to this process.

## 不能配置的内容

- 按角色裁剪子系统。角色字段是允许或拒绝的开关，不是功能集合。
- 对等体列表、投票者集合或加入令牌。成员关系从这一个进程开始。额外对等体只在 `RegisterNode` 加上 `ScaleShardReplicas` 之后出现，而且这些对等体必须已经是同一分片日志上会说 `RaftTransportService` 的 Raft 成员。第二个 all-in-one 进程不会这样做。见 [运维](./operations.md)。
- 第二个单元的键范围。`RegisterCell` 返回错误并且什么都不保存。空数据目录上的第二个 all-in-one 进程用 `[cluster] join` 加入当前 leader，这仍是同一个单元。
- Raft 计时器，或局部性压缩的开关。
- 本进程内的 TLS 证书。配置中的 TLS 键会被拒绝。终结应放在外部代理。
