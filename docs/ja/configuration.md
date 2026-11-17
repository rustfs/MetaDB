# 設定

[概要](./README.md) · [English](../en/configuration.md) · [Français](../fr/configuration.md) · [Deutsch](../de/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` と `ServerTomlConfig::from_toml_file` / `from_toml_str` は TOML を読みます（`crates/metadb-server/src/config.rs`）。未知のキーは無視されます。構造体は `#[serde(default)]` を使い、`deny_unknown_fields` は付けず、`from_toml_str` は `toml::from_str` を直接呼びます。綴りの違うキーは既定値のままです。空の `security.shared_secret` もその例で、起動は成功します。すべてのセクションは省略できます。省略した項目は次の既定値になります。

## 既定値

| キー | 既定 | `open_and_serve` が適用するか |
| :--- | :--- | :--- |
| `node.node_id` | `1` | する。Raft ノード ID とブートストラップのピア |
| `node.cell_id` | `1` | する。ディレクトリ名 `cell_<id>` と、このプロセスが登録する唯一のセル |
| `node.role` | `all-in-one` | する。それ以外の値はエラー |
| `node.data_dir` | `./metadb-data` | する |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | する。バインドアドレス |
| `node.metrics_listen_addr` | 未設定 | 設定されたときだけする。`metadbd` が別の TCP リスナーをバインドする |
| `clock.max_clock_drift_ms` | `500` | する。`HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432`（32 MiB） | **しない。** フィールドは保持されるが読まれない |
| `storage.block_cache_bytes` | `67108864`（64 MiB） | する。16 分割ブロックキャッシュの合計予算 |
| `storage.mvcc_retention_ms` | `5000` | する |
| `storage.bloom_bits_per_key` | `10` | する |
| `storage.locality_tombstone_threshold` | `128` | する。`LsmEngineConfig.tombstone_density_threshold` へ写す |
| `storage.locality_tombstone_ratio` | `0.50` | する。1000 倍し、1..=1000 にクランプし、`tombstone_min_ratio_permille` として保存する |
| `sharding.bootstrap_table_shards` | `true` | する。`true` はシャード 1、2、3 を作る。`false` は `[0x01, 0x04)` を覆うシャード 1 を作る |
| `sharding.shard_split_threshold_bytes` | `4294967296`（4 GiB） | する。分割判定はシャードの概算サイズを使う |
| `security.shared_secret` | `""` | する。空は、メタデータ、トランザクション、コントロールプレーンの RPC が無認証であることを意味する |

`[raft]` テーブルはありません。`ServerTomlConfig.raft_options` は serde がスキップし、`RaftNodeOptions::default()` のままです（ハートビート 150 ms、選挙 450–900 ms、スナップショットログ保持 0、セグメント 64 MiB）。

`to_lsm_config` は常に `auto_locality_compaction: true` を設定します。

## 例

このファイルは、明示的なデータディレクトリ、メトリクスポート、シークレット以外は既定値です。`memtable_size_bytes` は、コピーした設定が驚かないように書いてあります。変えてもフラッシュの挙動は変わりません。

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

ロール文字列はケバブケースです。`all-in-one`、`be`、`proxy`、`cell-manager`、`global-manager`。

CLI フラグはファイルを解析したあとで上書きします。`--node-id`、`--cell-id`、`--role`、`--listen`、`--data-dir`。`--cell-manager` は TOML の項目ではありません。`metadbd` のフラグだけです。

## セキュリティ

このプロセスは TLS を終端しません。

`shared_secret` が空でないとき、`MetadataService`、`TransactionService`、`ControlPlaneService` は、メタデータ `authorization` が `Bearer <shared_secret>` と等しいことを要求します。`metadb-ctl --auth-token`（環境変数 `METADB_AUTH_TOKEN`）がそのヘッダを送ります。`metadbd --cell-manager` は `RegisterNode` を呼ぶとき同じシークレットを送ります。

`RaftTransportService` も同じインターセプタを使います。シークレットが空でないとき、このポートの Raft RPC も `Bearer <shared_secret>` を要求します。

シークレットが空なら、このリスナーの 4 つのサービスはすべて通ります。コードは起動時にこれを警告します。プロセスは引き続き TLS を終端しません。信頼できないクライアントがポートへ届く前に、バインドを `127.0.0.1` のような信頼できるネットワークに留めるか、すべてのサービスをクライアントを認証するプロキシ（たとえば相互 TLS）の後ろに置き、このプロセスへの直接アクセスを塞いでください。

## 設定できないもの

- ロールごとのサブシステム切り出し。ロール項目は許可と拒否のスイッチであり、機能セットではありません。
- ピア一覧、投票者集合、参加トークン。メンバーシップはこのプロセス 1 つから始まります。追加のピアは `RegisterNode` と `ScaleShardReplicas` のあとでのみ現れ、そのピアは同じシャードログに対してすでに `RaftTransportService` を話す Raft メンバーでなければなりません。2 つ目の all-in-one プロセスはそれをしません。 [運用](./operations.md) を参照してください。
- 2 つ目のセルのキー範囲。`RegisterCell` はエラーを返し、何も保存しません。空ディレクトリの 2 つ目の all-in-one は `[cluster] join` で現在のリーダーに参加します。これは同じセルです。
- memtable のバイト上限、Raft のタイマ、局所性コンパクションのオンとオフ。
- TLS 証明書。
