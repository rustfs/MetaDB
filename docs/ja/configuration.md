# 設定

[概要](./README.md) · [English](../en/configuration.md) · [Français](../fr/configuration.md) · [Deutsch](../de/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` と `ServerTomlConfig::from_toml_file` / `from_toml_str` は TOML を読みます（`crates/metadb-server/src/config.rs`）。シークレットや TLS を示さない未知のキーは無視されます。綴りの違う `shared_secret`、トークンやパスワードのキー、TLS や証明書のキーは拒否され、プロセスは起動しません。既知のセクションは省略できます。省略した項目は次の既定値になります。

## 既定値

| キー | 既定 | `open_and_serve` が適用するか |
| :--- | :--- | :--- |
| `node.node_id` | `1` | する。Raft ノード ID とブートストラップのピア |
| `node.cell_id` | `1` | する。ディレクトリ名 `cell_<id>` と、このプロセスが登録する唯一のセル |
| `node.role` | `all-in-one` | する。それ以外の値はエラー |
| `node.data_dir` | `./metadb-data` | する |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | する。バインドアドレス |
| `node.metrics_listen_addr` | 未設定 | 設定されたときだけする。別の TCP リスナー。ループバック以外は空でないシークレットが必要 |
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
| `security.shared_secret` | `""` | する。空は全リスナーがループバックのときだけ受け付ける。空でない値は gRPC とメトリクスに同じベアラを要求する |

`[raft]` テーブルはありません。`ServerTomlConfig.raft_options` は serde がスキップし、`RaftNodeOptions::default()` のままです（ハートビート 150 ms、選挙 450–900 ms、スナップショットログ保持 0、セグメント 64 MiB）。

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

空、または空白だけのシークレットは、`grpc_listen_addr` と設定済みの `metrics_listen_addr` がどちらもループバック（`127.0.0.0/8`、`::1`、または名前 `localhost`）のときだけ受け付けます。ワイルドカード（`0.0.0.0`、`::`）やそれ以外のアドレスは待受を拒否します。空のループバックシークレットは起動時に警告されます。リスナーは引き続き平文です。信頼できるネットワークに留めるか、すべてのサービスをクライアントを認証するプロキシの後ろに置き、このプロセスへの直接アクセスを塞いでください。

## 設定できないもの

- ロールごとのサブシステム切り出し。ロール項目は許可と拒否のスイッチであり、機能セットではありません。
- ピア一覧、投票者集合、参加トークン。メンバーシップはこのプロセス 1 つから始まります。追加のピアは `RegisterNode` と `ScaleShardReplicas` のあとでのみ現れ、そのピアは同じシャードログに対してすでに `RaftTransportService` を話す Raft メンバーでなければなりません。2 つ目の all-in-one プロセスはそれをしません。 [運用](./operations.md) を参照してください。
- 2 つ目のセルのキー範囲。`RegisterCell` はエラーを返し、何も保存しません。空ディレクトリの 2 つ目の all-in-one は `[cluster] join` で現在のリーダーに参加します。これは同じセルです。
- Raft のタイマ、または局所性コンパクションのオンとオフ。
- このプロセス内の TLS 証明書。設定中の TLS キーは拒否される。終端は外部プロキシで行う。
