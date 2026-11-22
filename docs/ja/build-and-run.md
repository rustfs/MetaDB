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
| `--role <名前>` | `all-in-one`, `be`, `proxy`, `cell-manager`, or `global-manager`. A valid config starts that role. An unknown name returns `unsupported --role` from `metadbd` |
| `--listen <ホスト:ポート>` | `node.grpc_listen_addr` を上書きする |
| `--data-dir <パス>` | `node.data_dir` を上書きする |
| `--cell-manager <ホスト:ポート>` | 起動後、そのエンドポイントへ `RegisterNode` を呼ぶ |

`open_and_serve` starts `all-in-one`, `be`, `proxy`, `cell-manager`, and `global-manager` when that role's configuration is valid. `validate_role_topology` rejects an illegal combination before `TcpListener::bind`. An unknown `--role` returns `unsupported --role` from `metadbd` (`crates/metadb-server/src/bin/metadbd.rs`) and does not reach `open_and_serve`. It is not an unimplemented-role `CodecError`. This is not a disaggregated production deployment.

`metadbd` with no config file uses an empty `shared_secret` and `allow_unauthenticated = false`, so that process does not start. The file below is the explicit loopback development opt-in. It is not a credential. Bind only a literal loopback IP (`127.0.0.1`). The hostname `localhost` is not accepted for this flag. Do not use it on a non-loopback address.

```bash
mkdir -p /tmp/metadb-data
cat > /tmp/metadb-loopback.toml <<'EOF'
[node]
data_dir = "/tmp/metadb-data"
grpc_listen_addr = "127.0.0.1:7800"

[security]
allow_unauthenticated = true
EOF
cargo run -p metadb-server --bin metadbd -- --config /tmp/metadb-loopback.toml
```

A usable secret is a different file. Replace the `[security]` block. Do not set `allow_unauthenticated` in that same file. Both together are rejected. A whitespace-only secret is rejected.

```bash
mkdir -p /tmp/metadb-data
cat > /tmp/metadb-auth.toml <<'EOF'
[node]
data_dir = "/tmp/metadb-data"
grpc_listen_addr = "127.0.0.1:7800"

[security]
shared_secret = "replace-with-a-long-random-string"
EOF
cargo run -p metadb-server --bin metadbd -- --config /tmp/metadb-auth.toml
```

Pass that same secret to the client as `metadb-ctl --auth-token` or `METADB_AUTH_TOKEN`.

ログは `tracing-subscriber` の `EnvFilter` を使います。既定のフィルタは `info` です。標準の環境変数 `RUST_LOG` で上書きします。例: `RUST_LOG=debug`。

An empty `security.shared_secret` does not start the process. Loopback is not an implicit exception. Development without credentials sets `allow_unauthenticated = true` and keeps every listener on a literal loopback IP (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that opt-in. A wildcard or other non-loopback address refuses to listen. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. A non-empty `[security].shared_secret` requires the same bearer for metadata, transaction, control-plane, and Raft transport RPCs, and for `GET /metrics`. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`. A TLS, certificate, anonymous, or insecure key in the configuration fails startup instead of serving plaintext. Keep an opted-in listener on a trusted network, or put every service behind a proxy that authenticates the client and block direct access to this process.

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
