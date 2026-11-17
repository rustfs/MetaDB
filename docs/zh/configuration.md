# 配置

[概览](./README.md) · [English](../en/configuration.md) · [Français](../fr/configuration.md) · [Deutsch](../de/configuration.md) · [日本語](../ja/configuration.md)

`metadbd --config` 以及 `ServerTomlConfig::from_toml_file` / `from_toml_str` 读取 TOML（`crates/metadb-server/src/config.rs`）。未知键会被忽略。这些结构体使用 `#[serde(default)]`，没有 `deny_unknown_fields`，`from_toml_str` 直接调用 `toml::from_str`。拼错的键保持默认值，包括空的 `security.shared_secret`，进程仍然会启动。每个节都可以省略。省略的字段取下面的默认值。

## 默认值

| 键 | 默认 | `open_and_serve` 是否应用 |
| :--- | :--- | :--- |
| `node.node_id` | `1` | 是。Raft 节点标识和引导对等体 |
| `node.cell_id` | `1` | 是。目录名 `cell_<id>`，以及本进程注册的唯一单元 |
| `node.role` | `all-in-one` | 是。任何其他值都返回错误 |
| `node.data_dir` | `./metadb-data` | 是 |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | 是。绑定地址 |
| `node.metrics_listen_addr` | 未设置 | 仅在设置时应用。`metadbd` 绑定单独的 TCP 监听器 |
| `clock.max_clock_drift_ms` | `500` | 是。`HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432`（32 MiB） | **否。** 字段被保存，从不读取 |
| `storage.block_cache_bytes` | `67108864`（64 MiB） | 是。16 路块缓存的总预算 |
| `storage.mvcc_retention_ms` | `5000` | 是 |
| `storage.bloom_bits_per_key` | `10` | 是 |
| `storage.locality_tombstone_threshold` | `128` | 是。映射到 `LsmEngineConfig.tombstone_density_threshold` |
| `storage.locality_tombstone_ratio` | `0.50` | 是。乘以 1000，钳制到 1..=1000，存为 `tombstone_min_ratio_permille` |
| `sharding.bootstrap_table_shards` | `true` | 是。`true` 创建分片 1、2、3。`false` 创建覆盖 `[0x01, 0x04)` 的分片 1 |
| `sharding.shard_split_threshold_bytes` | `4294967296`（4 GiB） | 是。分裂检查使用分片的近似大小 |
| `security.shared_secret` | `""` | 是。空表示元数据、事务和控制面 RPC 不认证 |

没有 `[raft]` 表。`ServerTomlConfig.raft_options` 被 serde 跳过，保持 `RaftNodeOptions::default()`（心跳 150 ms，选举 450–900 ms，快照日志保留 0，段大小 64 MiB）。

`to_lsm_config` 总是把 `auto_locality_compaction` 设为 `true`。

## 示例

除了明确的数据目录、指标端口和密钥，这个文件与默认值一致。写出 `memtable_size_bytes` 是为了避免抄来的配置让人意外；改它不会改变刷盘行为。

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

进程不终结 TLS。

`shared_secret` 非空时，`MetadataService`、`TransactionService` 和 `ControlPlaneService` 要求元数据 `authorization` 等于 `Bearer <shared_secret>`。`metadb-ctl --auth-token`（环境变量 `METADB_AUTH_TOKEN`）发送这个头。`metadbd --cell-manager` 在调用 `RegisterNode` 时发送同一密钥。

`RaftTransportService` 注册时没有拦截器。持有者令牌不能认证该端口上的 Raft RPC。

空密钥允许元数据、事务和控制面 RPC。代码在启动时对此发出警告。持有者密钥不覆盖 `RaftTransportService`，在监听器前面终结 TLS 也不会补上这项检查：终结之后，Raft RPC 仍然不认证。在不可信客户端能够到达该端口之前，把绑定留在 `127.0.0.1` 这类可信网络上，或把每个服务放到会认证客户端的代理后面（例如双向 TLS），并阻断对本进程的直接访问。

## 不能配置的内容

- 按角色裁剪子系统。角色字段是允许或拒绝的开关，不是功能集合。
- 对等体列表、投票者集合或加入令牌。成员关系从这一个进程开始。额外对等体只在 `RegisterNode` 加上 `ScaleShardReplicas` 之后出现，而且这些对等体必须已经是同一分片日志上会说 `RaftTransportService` 的 Raft 成员。第二个 all-in-one 进程不会这样做。见 [运维](./operations.md)。
- 第二个单元的键范围。`RegisterCell` 返回错误并且什么都不保存。空数据目录上的第二个 all-in-one 进程用 `[cluster] join` 加入当前 leader，这仍是同一个单元。
- memtable 字节上限、Raft 计时器，或局部性压缩的开关。
- TLS 证书。
