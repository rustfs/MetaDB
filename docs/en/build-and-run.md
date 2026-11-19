# Build and run

[Overview](./README.md) · [Français](../fr/build-and-run.md) · [Deutsch](../de/build-and-run.md) · [日本語](../ja/build-and-run.md) · [简体中文](../zh/build-and-run.md)

## Toolchain

`rust-toolchain.toml` pins channel `1.100.0` with components `rustfmt` and `clippy`. Crate edition is `2021`. The Rust sources do not link a C or C++ library. Compiling `metadb-proto` does require the `protoc` binary (`prost-build` shells out to it). On Debian or Ubuntu that package is `protobuf-compiler`. If `protoc` is installed elsewhere, set `PROTOC` to its path. From a checkout:

```bash
rustc --version
cargo --version
```

`rustup` installs the pinned toolchain on the first Cargo invocation when it is allowed to download it.

## Build

```bash
cargo build --release -p metadb-server --bins
```

That produces:

- `target/release/metadbd`
- `target/release/metadb-ctl`

Debug builds use `cargo build -p metadb-server --bins` and write the same names under `target/debug/`.

`cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` are the formatting and lint commands used by this repository. Tests are listed in [Tests and benches](./testing.md).

## Run `metadbd`

`metadbd` flags (`crates/metadb-server/src/bin/metadbd.rs`):

| Flag | Effect |
| :--- | :--- |
| `--config <path>` | Load TOML. Without it, `ServerTomlConfig::default()` is used |
| `--node-id <u64>` | Overrides `node.node_id` |
| `--cell-id <u64>` | Overrides `node.cell_id` |
| `--role <name>` | `all-in-one`, `be`, `proxy`, `cell-manager`, or `global-manager`. Only `all-in-one` proceeds |
| `--listen <host:port>` | Overrides `node.grpc_listen_addr` |
| `--data-dir <path>` | Overrides `node.data_dir` |
| `--cell-manager <host:port>` | After the process is up, calls `RegisterNode` on that endpoint |

Any role other than `all-in-one` fails before the listener is bound, with `server role ... is not implemented; only all-in-one runs a process`.

Example, loopback only. `metadbd` with no config file uses an empty `shared_secret` and `allow_unauthenticated = false`, so that process does not start. The file below is the explicit loopback opt-in. It is not a credential. Do not use it on a non-loopback address.

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

A usable secret is a different configuration. Put a secret you choose in `shared_secret` and do not set `allow_unauthenticated` in that same file. Both together are rejected. See [Configuration](./configuration.md).

```bash
cargo run -p metadb-server --bin metadbd -- --config ./standalone.toml
```

Logs use `tracing-subscriber`'s `EnvFilter`. The default filter is `info`. Override it with the standard `RUST_LOG` variable, for example `RUST_LOG=debug`.

An empty `security.shared_secret` does not start the process. The default bind is `127.0.0.1`, and loopback is not an implicit exception. Local development without credentials sets `allow_unauthenticated = true` and keeps every listener on a literal loopback IP (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that opt-in. A wildcard or other non-loopback address still refuses to listen. A whitespace-only secret is rejected. A non-empty `[security].shared_secret` requires the same bearer on metadata, transaction, control-plane, Raft transport, and `GET /metrics`. The process does not terminate TLS. A TLS, certificate, anonymous, or insecure key in the config fails startup instead of serving plaintext. Keep an opted-in listener on a trusted network, or put every service behind a proxy that authenticates clients and block direct access to this process.

Ctrl-C flushes shard state and stops the OpenRaft groups (`cluster.shutdown()`).

## Embed the library

Path dependencies from a crate outside this workspace:

```toml
[dependencies]
metadb-core = { path = "path/to/MetaDB/crates/metadb-core" }
metadb-server = { path = "path/to/MetaDB/crates/metadb-server" }
tokio = { version = "1", features = ["full"] }
```

Persistent open. `MetaDbEngine::open` returns `Arc<PersistentMetaDbCluster>` and also binds gRPC.

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

`mkdir_auto_id` and `create_file_auto_id` allocate ids from `LocalIdBuffer`. Callers that already have ids use `tx_router()`:

- `create_file`, `mkdir`, `unlink`, `rmdir`, `setattr`, `rename`
- `lookup`, `dirstat`, `listdir_paginated`
- `set_quota_limit`, `get_quota`, `list_quotas`
- `get_secondary_index`, `scan_secondary_index`
- `init_dir_attr` (used at bootstrap for the root)

`PersistentMetaDbCluster::open_and_serve` is the same open path and also returns the bound address string.

`MetaDbEngine::new_single_node` is synchronous and in-memory. Use it only where a test already uses it. It is not crash-safe and it is not what `metadbd` runs.

## Operator CLI

`metadb-ctl` is a client. It does not open a data directory. Point it at a running `metadbd`:

```bash
cargo run -p metadb-server --bin metadb-ctl -- --endpoint 127.0.0.1:7800 cluster status
```

Commands, authentication, and the bench client are in [Operations](./operations.md) and [Tests and benches](./testing.md).
