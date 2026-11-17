# ビルドと実行

[概要](./README.md) · [English](../en/build-and-run.md) · [Français](../fr/build-and-run.md) · [Deutsch](../de/build-and-run.md) · [简体中文](../zh/build-and-run.md)

## ツールチェーン

`rust-toolchain.toml` はチャンネル `1.100.0` とコンポーネント `rustfmt`、`clippy` を固定します。クレートのエディションは `2021` です。Rust のソースは C または C++ のライブラリをリンクしません。ただし `metadb-proto` のコンパイルには `protoc` バイナリが必要です（`prost-build` がそれを呼び出します）。Debian または Ubuntu のパッケージは `protobuf-compiler` です。別の場所に `protoc` があるときは、環境変数 `PROTOC` にそのパスを設定します。チェックアウトしたディレクトリで次を実行します。

```bash
rustc --version
cargo --version
```

ダウンロードが許されていれば、`rustup` は最初の Cargo 呼び出しで固定されたツールチェーンを入れます。

## ビルド

```bash
cargo build --release -p metadb-server --bins
```

生成されるもの:

- `target/release/metadbd`
- `target/release/metadb-ctl`

デバッグビルドは `cargo build -p metadb-server --bins` で、同じ名前を `target/debug/` に書きます。

`cargo fmt --all -- --check` と `cargo clippy --workspace --all-targets -- -D warnings` が、このリポジトリの整形とリントのコマンドです。テストは [テストとベンチ](./testing.md) にあります。

## `metadbd` を起動する

`metadbd` のフラグ（`crates/metadb-server/src/bin/metadbd.rs`）:

| フラグ | 効果 |
| :--- | :--- |
| `--config <パス>` | TOML を読む。無いときは `ServerTomlConfig::default()` を使う |
| `--node-id <u64>` | `node.node_id` を上書きする |
| `--cell-id <u64>` | `node.cell_id` を上書きする |
| `--role <名前>` | `all-in-one`、`be`、`proxy`、`cell-manager`、`global-manager`。先へ進むのは `all-in-one` だけ |
| `--listen <ホスト:ポート>` | `node.grpc_listen_addr` を上書きする |
| `--data-dir <パス>` | `node.data_dir` を上書きする |
| `--cell-manager <ホスト:ポート>` | 起動後、そのエンドポイントへ `RegisterNode` を呼ぶ |

`all-in-one` 以外のロールはリスナーをバインドする前に失敗し、`server role ... is not implemented; only all-in-one runs a process` を返します。

例（ループバックのみ）:

```bash
mkdir -p /tmp/metadb-data
cargo run -p metadb-server --bin metadbd -- \
  --data-dir /tmp/metadb-data \
  --listen 127.0.0.1:7800 \
  --node-id 1 \
  --cell-id 1 \
  --role all-in-one
```

ファイルを使う場合は [設定](./configuration.md) を参照します。

```bash
cargo run -p metadb-server --bin metadbd -- --config ./standalone.toml
```

ログは `tracing-subscriber` の `EnvFilter` を使います。既定のフィルタは `info` です。標準の環境変数 `RUST_LOG` で上書きします。例: `RUST_LOG=debug`。

`security.shared_secret` が空のとき、起動時に gRPC には認証も TLS も無いという警告が出ます。既定のバインドは `127.0.0.1` です。信頼できないネットワークへこのソケットを出さないでください。空でない `[security].shared_secret` は、メタデータ、トランザクション、コントロールプレーン、および Raft 転送の RPC に同じベアラを要求します。プロセスは引き続き TLS を終端しません。リスナーを信頼できるネットワークに留めるか、すべてのサービスを、クライアントを認証するプロキシの後ろに置き、このプロセスへの直接アクセスを塞いでください。

Ctrl-C はシャード状態をフラッシュし、OpenRaft グループを止めます（`cluster.shutdown()`）。

## ライブラリを埋め込む

このワークスペースの外のクレートからのパス依存です。

```toml
[dependencies]
metadb-core = { path = "path/to/MetaDB/crates/metadb-core" }
metadb-server = { path = "path/to/MetaDB/crates/metadb-server" }
tokio = { version = "1", features = ["full"] }
```

永続オープンです。`MetaDbEngine::open` は `Arc<PersistentMetaDbCluster>` を返し、gRPC もバインドします。

```rust
use metadb_core::ROOT_INODE_ID;
use metadb_server::{MetaDbEngine, ServerTomlConfig};

#[tokio::main]
async fn main() -> metadb_core::Result<()> {
    let cfg = ServerTomlConfig::from_toml_file("standalone.toml")?;
    let cluster = MetaDbEngine::open(cfg).await?;

    let dir = cluster
        .mkdir_auto_id(ROOT_INODE_ID, "checkpoints", 1000, 0o755)
        .await?;
    let file = cluster
        .create_file_auto_id(dir.attr.inode_id, "step_0002.pt", 1000, 0o644, 4096)
        .await?;
    let stat = cluster.tx_router().dirstat(dir.attr.inode_id).await?;
    let _ = (file, stat);

    cluster.shutdown().await?;
    Ok(())
}
```

`mkdir_auto_id` と `create_file_auto_id` は `LocalIdBuffer` から ID を割り当てます。すでに ID を持っている呼び出し側は `tx_router()` を使います。

- `create_file`、`mkdir`、`unlink`、`rmdir`、`setattr`、`rename`
- `lookup`、`dirstat`、`listdir_paginated`
- `set_quota_limit`、`get_quota`、`list_quotas`
- `get_secondary_index`、`scan_secondary_index`
- `init_dir_attr`（ルートのブートストラップで使用）

`PersistentMetaDbCluster::open_and_serve` は同じオープン経路で、バインドされたアドレス文字列も返します。

`MetaDbEngine::new_single_node` は同期でメモリ内です。テストがすでに使っている場所だけで使ってください。クラッシュセーフではなく、`metadbd` が動かすものでもありません。

## 運用 CLI

`metadb-ctl` はクライアントです。データディレクトリは開きません。起動中の `metadbd` を指します。

```bash
cargo run -p metadb-server --bin metadb-ctl -- --endpoint 127.0.0.1:7800 cluster status
```

コマンド、認証、ベンチクライアントは [運用](./operations.md) と [テストとベンチ](./testing.md) にあります。
