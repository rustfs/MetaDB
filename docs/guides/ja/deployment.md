# デプロイとオンラインクラスター拡張（日本語）

* **ガイド目次へ戻る**: [日本語概要](./README.md) | [ハブ](../../README.md)

現在のバイナリが永続化して起動するのは `all-in-one` だけです。同一セル内の複数プロセス Raft は統合テストが扱います。ロールごとに分けたプロセスと Multi-Cell 構成は設計目標であり、このリポジトリが稼働させている配置ではありません。1,000 億〜10 兆以上のレコード数は設計モデルであり、このリポジトリの測定結果ではありません。`arXiv:2610.32247v1` の兆規模の数値は Baidu の本番データです。

> **実装状態**: 永続 LSM、Raft 複製、単一ホスト上のシャード分割、および all-in-one の gRPC/CLI には回帰テストがあります。デーモンを起動できるロールは `all-in-one` だけです。`be`、`proxy`、`cell-manager`、`global-manager` と遠隔の `RegisterCell` は拒否されます。[正確性メモ](../../designs/correctness_hardening.md) を参照してください。

---

## Stage 1：スタンドアロン・シングルノード構成 (`metadbd --role all-in-one`) `[実装済]`

単一プロセス内で `GlobalManager`、`CellLocalManager`、`MetaCache`、`Proxy`、および **`1-Voter` OpenRaft** バックエンドを実行します。ローカル KV の WAL は無効化され、CRC32C チェックサム付き `.raftlog` セグメントへの順次追記のみで耐久性を保証します。

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
block_cache_bytes = 536870912           # 512 MiB (16-shard LRU)
mvcc_retention_ms = 5000                # 5 秒メモリ内 MVCC 保持ウィンドウ
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296 # 4 GiB
```

Rust 1.100.0（`rust-toolchain.toml`）。実装されているロールは `all-in-one` のみ。このプロセスは TLS を終端しない。空の `[security].shared_secret` では認証もなく、待受はループバックだけを受け付ける。既定は `127.0.0.1`。ワイルドカードやそれ以外のアドレスに空のシークレットがあると起動を拒否する。空でない `shared_secret` は metadata、transaction、control-plane、同じポートの Raft 転送、および `GET /metrics` に `authorization: Bearer <secret>` を要求する。

```bash
metadbd --config /etc/metadb/standalone.toml
```

---

## Stage 2：単一ホスト上の自動 `PID` 境界シャード分割 `[実装済]`

シャードサイズが `shard_split_threshold_bytes` を超えると、`PolicyShardSplitter` が `group_key = PID`（`[0x01][pid: 8B BE]`）境界でシャードを分割し、親ディレクトリ属性 `(PID, "_attr_")` と全子エントリを同一シャードに維持します。

---

## Stage 3：想定している Multi-Cell 構成

Stage 3 は設計目標です。現在のバイナリはこの構成をデプロイしません。

`metadbd` が実行するのは `--role all-in-one` だけです。`crates/metadb-server/src/lib.rs` の `PersistentMetaDbCluster::open_and_serve` は、待受の前に `be`、`proxy`、`cell-manager`、`global-manager` を拒否します。`crates/metadb-server/src/grpc_services.rs` の `RegisterCell` はアプリケーションエラーを返し、2 つ目のセルを保存しません。このツリーに、複数プロセスの Multi-Cell を実際に動かす配置はありません。

2 つ目の `metadbd --role all-in-one` は、`--join` または `--cell-manager` が現在のリーダーを指し、データディレクトリが空であるときに**同じ**セルへ参加できます。この経路は `crates/metadb-server/tests/join_existing_group.rs` が扱います。参加者は依然として all-in-one であり、ロールを削ったバックエンドではなく、別のセルも作りません。[運用](../../ja/operations.md) を参照してください。

### 3.1 同一セル内の投票者

`metadbd --role be` は起動しないでください。そのロールは `open_and_serve` で終了するため、手順にはなりません。`metadb-ctl raft scale-replicas` は `ScaleShardReplicas` を呼び、要求した投票者数がすでにそのセルへ登録されている必要があります。新しい all-in-one セルのノードは 1 つなので、それらのノードが存在するまで `--target-voters 3` は失敗します。制約は [運用](../../ja/operations.md) にあります。

### 3.2 分離構成の設計モデル（`100B – 10T+` レコード）

`100B – 10T+`（1,000 億〜10 兆以上）は**設計モデル**の範囲です。将来の Multi-Cell 構成がどの程度の容量になるかの算術であり、このリポジトリが保存したレコード数ではありません。Baidu の本番数値を `rustfs/MetaDB` の結果として書いたものでもありません。`arXiv:2610.32247v1` が公表するクラスター規模（BOS2 の 1.848 兆レコードを含む）は Baidu の本番データです。

表は想定しているロール分割です。このバイナリはこれらのロールを実行しません。

| ロール | 設計上の `--role` | 想定する責務 |
| :--- | :--- | :--- |
| **Global Manager** | `global-manager` | 粗い `[start_key, end_key)` を `CellId` へ対応させ、`65,536` 個単位の ID を割り当て、セルをまたぐディレクトリ改名リースを調整する、という設計です。 |
| **Cell Local Manager** | `cell-manager` | セル内の BE ハートビート（セルあたり `20K–50K` シャード）を処理し、`PID` 分割、Jumbo 隔離、メンバー変更を編成する、という設計です。 |
| **Backend Storage (`BE`)** | `be` | NVMe ホスト上で Multi-Raft シャードグループと LSM エンジンを持つ、という設計です。 |
| **Stateless Proxy** | `proxy` | `MetaCache` を内蔵し、20 種の SQL テンプレートをコンパイルし、シャードをまたぐ 2PC を調整する、という設計です。 |

`metadb-ctl cell register` は `RegisterCell` を呼び、非ゼロで終了します。セルは登録しません。連合の手順として使わないでください。
