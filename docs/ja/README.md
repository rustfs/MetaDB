# MetaDB 実装ガイド

[English](../en/README.md) · [Français](../fr/README.md) · [Deutsch](../de/README.md) · [简体中文](../zh/README.md)

この一連のページは、`rustfs/MetaDB` のツリーを実装どおりに説明します。このリポジトリの運用者と開発者向けの案内です。より大きいトポロジを描いた設計メモは [`Architecture.md`](../../Architecture.md)、[`docs/designs/`](../designs/SITEMAP.md)、および [`docs/guides/`](../README.md) 以下の以前のガイドにあります。それらの文書とコードが食い違うときは、この一式に従ってください。

## このリポジトリが何か

MetaDB は、ファイルとオブジェクトの名前空間向けの純粋な Rust のメタデータエンジンで、[RustFS](https://github.com/rustfs) のために書かれています。データモデルはディレクトリのトポロジに従います。ディレクトリとその直接の子はキー接頭辞を共有するため、単一ディレクトリの作成、検索、一覧、削除は 1 つのシャードに留まります。設計は Baidu と北京大学の論文 *MetaDB: Putting File-System Structure Back into Distributed Databases*（[arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1)）を踏まえています。論文中の本番規模の数値は Baidu の配備を述べています。このリポジトリは対応する測定値を公開していません。

ワークスペースのバージョンは `0.1.0`（Apache-2.0）です。Rust ツールチェーンは `rust-toolchain.toml` で **1.100.0** に固定されています。

## 1 つのプロセスが実際に動かすもの

`metadbd` と `PersistentMetaDbCluster::open_and_serve` は、単一の **all-in-one** プロセスを起動します。

- データディレクトリは 1 つ
- セルは 1 つ（`node.cell_id`、既定は `1`）
- シャードごとの OpenRaft 投票者は 1 つ（このプロセス）
- `MetadataService`、`TransactionService`、`ControlPlaneService`、`RaftTransportService` を提供する gRPC リスナーは 1 つ
- 250 ms ごとのバックグラウンドループが、補助アウトボックスの排出、2PC 回復、分割方針の確認、Raft ログのスナップショットを行う

`ServerRole` は `be`、`proxy`、`cell-manager`、`global-manager` も解析します。`open_and_serve` は `all-in-one` 以外のロールを拒否します。空のデータディレクトリを持つ 2 つ目の `metadbd` が `[cluster] join`（または `--join` / `--cell-manager`）を現在のリーダーに向けると、そのセルの Raft グループに参加します。リーダーは learner として加え、その後 voter に昇格します。すでにセル状態があるディレクトリはローカル Raft を開き直し、再参加しません。`RegisterCell` はアプリケーションエラーを返し、セルを追加しません。`metadb-ctl cell register` は非ゼロで終了します。未知で非ゼロの `cell_id` に対する `GetRoutes` はエラーを返します。`cell_id = 0` はキャッシュ済みルートをすべて列挙します。

シャード分割、デュアルモードの `(PID, name)` 分割、ジャンボのルートフラグは、その 1 プロセスの中で動きます。投票者を 3 つへ昇格する処理は `DurableCellManager::scale_out_replicas` に実装され、プロセス内の Raft テストで覆われています。これは `metadbd` を 3 プロセス起動する配備ではありません。詳細は [運用](./operations.md) を参照してください。

## 文書の構成

| ページ | 内容 |
| :--- | :--- |
| [アーキテクチャ](./architecture.md) | クレート、キー、ストレージ、Raft、トランザクション、シャーディング、2 つのエンジン |
| [ビルドと実行](./build-and-run.md) | ツールチェーン、バイナリ、組み込みライブラリ、停止 |
| [設定](./configuration.md) | TOML の項目、既定値、プロセスが適用しない項目 |
| [運用](./operations.md) | `metadb-ctl`、メトリクス、スケールアウト、分割、制限、文書化できない欠落 |
| [テストとベンチ](./testing.md) | `cargo test`、各スイートが実際に行使するもの、`metadb-ctl bench` |

## ワークスペースのクレート

| クレート | コードが行うこと |
| :--- | :--- |
| `metadb-core` | キーコーデック `0x01`–`0x07`、`ShardEpoch`、ハイブリッド論理時計（既定のドリフト上限 500 ms）、88 バイトの `InodeAttr` |
| `metadb-proto` | [アーキテクチャ](./architecture.md) に列挙した Protobuf と tonic サービス |
| `metadb-storage` | WAL なし SSTable LSM、`MANIFEST`、16 分割 LRU ブロックキャッシュ、メモリ内 MVCC、局所性コンパクション |
| `metadb-raft` | `openraft` 0.10 グループ、分割された CRC32C `.raftlog`、スナップショット、learner から voter へのメンバー変更 |
| `metadb-tx` | プッシュダウン変異、属性デルタ、補助アウトボックス、プライマリ固定の 2PC |
| `metadb-cluster` | 永続セルマネージャとグローバルマネージャ、ID 割り当て、ルートキャッシュ、分割ワーカー、ジャンボ計画 |
| `metadb-proxy` | 20 個の SQL テンプレートの字句解析と構文解析、トランザクション層へのステートレスルータ |
| `metadb-server` | `PersistentMetaDbCluster`、メモリ内の `MetaDbEngine::new_single_node`、`metadbd`、`metadb-ctl`、Prometheus テキスト |

## エンジンの呼び方は 2 つ

`MetaDbEngine::open` と `PersistentMetaDbCluster::open_and_serve` はデータディレクトリ、LSM、OpenRaft を開き、gRPC をバインドします。動かすのはこちらです。

`MetaDbEngine::new_single_node` はメモリ内シミュレータを作ります。データディレクトリも LSM も OpenRaft グループも開きません。`crates/metadb-server/tests/integration_tests.rs` の `T1`–`T6` がこれを使い、`T6` の「3 投票者」の手順もメモリ上のメンバー表を書き換えるだけです。
