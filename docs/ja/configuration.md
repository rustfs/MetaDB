# 設定

[概要](./README.md) · [English](../en/configuration.md) · [Français](../fr/configuration.md) · [Deutsch](../de/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` と `ServerTomlConfig::from_toml_file` / `from_toml_str` は TOML を読みます（`crates/metadb-server/src/config.rs`）。既知のセクションは省略できます。省略した項目は次の既定値になります。

A key fails parsing when its normalized form (lowercase, hyphens become underscores) contains one of these substrings, including inside nested tables and arrays: `secret`, `password`, `passwd`, `token`, `credential`, `auth`, `anonymous`, `anon`, `insecure`, `api_key`, `apikey`, `bearer`, `jwt`, `private_key`, `privatekey`, `encryption`, `encrypt`, `tls`, `ssl`, `cert`, `x509`, `mtls`, `cipher`, `keystore`, `https`, `ca_file`, `cafile`, or the normalized key starts with `secur`. The only exceptions are the direct keys `[security].shared_secret` and `[security].allow_unauthenticated`. Any other key directly under `[security]` also fails parsing. This is the classifier, not a sample list: a key is not ignored merely because its name is absent from a shorter summary. Keys that do not match this classifier are still ignored. An empty `shared_secret` does not start the process. Development without credentials requires `allow_unauthenticated = true` and a literal loopback bind (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that flag. A wildcard or any other non-loopback address cannot use it. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected.

## 既定値

| キー | 既定 | `open_and_serve` が適用するか |
| :--- | :--- | :--- |
| `node.node_id` | `1` | する。Raft ノード ID とブートストラップのピア |
| `node.cell_id` | `1` | する。ディレクトリ名 `cell_<id>` と、このプロセスが登録する唯一のセル |
| `node.role` | `all-in-one` | Yes. `all-in-one`, `be`, `proxy`, `cell-manager`, and `global-manager` start when that role's configuration is valid. An illegal combination fails before listen. An unknown `--role` returns `unsupported --role` from `metadbd`, not an unimplemented-role `CodecError` |
| `node.data_dir` | `./metadb-data` | する |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | する。バインドアドレス |
| `node.metrics_listen_addr` | 未設定 | Applied only when set. Separate TCP listener. An empty secret does not listen unless `allow_unauthenticated = true`, and that flag is loopback-only |
| `clock.max_clock_drift_ms` | `500` | する。`HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432`（32 MiB） | する。計上バイトがこの大きさに達したとき、アクティブな memtable を封印してフラッシュする。1 未満は 1 として扱う |
| `storage.max_imm_memtables` | `4` | する。書き手はこの上限を超える不変 memtable をキューに積まず、待つ |
| `storage.block_cache_bytes` | `67108864`（64 MiB） | する。16 分割ブロックキャッシュの合計予算 |
| `storage.mvcc_retention_ms` | `5000` | する |
| `storage.bloom_bits_per_key` | `10` | する |
| `storage.locality_tombstone_threshold` | `128` | する。`LsmEngineConfig.tombstone_density_threshold` へ写す |
| `storage.locality_tombstone_ratio` | `0.50` | する。1000 倍し、1..=1000 にクランプし、`tombstone_min_ratio_permille` として保存する |
| `sharding.bootstrap_table_shards` | `true` | する。`true` はシャード 1、2、3 を作る。`false` は `[0x01, 0x04)` を覆うシャード 1 を作る |
| `sharding.shard_split_threshold_bytes` | `4294967296`（4 GiB） | する。分割判定はシャードの概算サイズを使う |
| `security.shared_secret` | `""` | Empty does not start the process. Loopback without credentials requires `allow_unauthenticated = true`. A non-empty secret requires the same bearer on gRPC and metrics. A whitespace-only secret is rejected |
| `security.allow_unauthenticated` | `false` | Explicit opt-in for an empty secret on a literal loopback IP (`127.0.0.1` or `::1`) only. The hostname `localhost` is not accepted. Rejected together with a usable secret. Rejected on a wildcard or any other non-loopback bind. A whitespace-only secret is rejected even with this flag |

`[raft]` is loaded. `ServerTomlConfig.raft_options` is not `#[serde(skip)]`. Omitted fields use heartbeat 150 ms, election timeout 450–900 ms, `max_in_snapshot_log_to_keep` 0, and segment size 64 MiB (`67108864`). Unknown keys inside `[raft]` fail parsing. `election_timeout_min_ms` must be greater than `heartbeat_interval_ms` and less than `election_timeout_max_ms`. `max_segment_bytes = 0` is rejected. The values apply when each local Raft group starts and are not reloaded. `max_segment_bytes` does not rewrite segments already on disk. `join_timeout` is not a TOML field. There is no gRPC health RPC. `/livez`, `/readyz`, and `/healthz` exist only when `metrics_listen_addr` is set. `/readyz` does not wait for the 250 ms 2PC recovery pass. See [Operability](../en/operability.md).


`to_lsm_config` は常に `auto_locality_compaction: true` を設定します。

## 例

このファイルは、明示的なデータディレクトリ、メトリクスポート、シークレット以外は既定値です。`memtable_size_bytes` はアクティブな memtable のフラッシュしきい値です。`max_imm_memtables` は不変テーブルのバックプレッシャー上限です。

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

ロール文字列はケバブケースです。`all-in-one`、`be`、`proxy`、`cell-manager`、`global-manager`。

CLI フラグはファイルを解析したあとで上書きします。`--node-id`、`--cell-id`、`--role`、`--listen`、`--data-dir`。`--cell-manager` は TOML の項目ではありません。`metadbd` のフラグだけです。

## セキュリティ

このプロセスは TLS を終端しません。ネイティブ TLS を有効にする証明書設定はありません。ファイルに TLS、SSL、または証明書のキーがあると解析は失敗し、プロセスは平文では提供しません。TLS は外部プロキシで終端し、このファイルに TLS キーを残さないでください。

`shared_secret` が空でないとき、`MetadataService`、`TransactionService`、`ControlPlaneService`、`RaftTransportService` は `authorization` が `Bearer <shared_secret>` と完全に一致することを要求します。`metrics_listen_addr` の `GET /metrics` も同じ `Authorization` ヘッダを要求します。ヘッダの欠落、空のベアラ、接頭辞、より長いトークンは拒否されます。失敗したメトリクス応答の本体は Prometheus テキストではありません。Raft ピアは同じベアラを送ります。`metadb-ctl --auth-token`（環境変数 `METADB_AUTH_TOKEN`）と `metadbd --join` / `--cell-manager` も送ります。`metadb-ctl cluster status` は `GetRoutes` を呼び、同じインターセプタの対象です。

An empty secret does not start the process. Loopback is not an implicit exception. Development without credentials requires `allow_unauthenticated = true` and every listener on a literal loopback IP (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that flag. A wildcard (`0.0.0.0`, `::`) or any other non-loopback address cannot use it. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. Listeners stay plaintext. Keep an opted-in listener on a trusted network, or put every service behind a proxy that authenticates the client and block direct access to this process.

## 設定できないもの

- Trimming every role except `all-in-one` out of the process. `be`, `proxy`, `cell-manager`, and `global-manager` start when that role's configuration is valid. An illegal combination fails before listen. This is not a disaggregated production deployment and not a production MetaCache of every shard.
- ピア一覧、投票者集合、参加トークン。メンバーシップはこのプロセス 1 つから始まります。追加のピアは `RegisterNode` と `ScaleShardReplicas` のあとでのみ現れ、そのピアは同じシャードログに対してすでに `RaftTransportService` を話す Raft メンバーでなければなりません。2 つ目の all-in-one プロセスはそれをしません。 [運用](./operations.md) を参照してください。
- An arbitrary key slice as a cell range. `cell_start_key` and `cell_end_key` must be a contiguous union of whole tables. `RegisterCell` stores a remote cell only after its routes cover that range. Join still joins the same cell and does not create a second one.
- Locality-compaction on/off. `to_lsm_config` always enables it. Raft timers are the `[raft]` table; see [Operability](../en/operability.md).
- このプロセス内の TLS 証明書。設定中の TLS キーは拒否される。終端は外部プロキシで行う。
