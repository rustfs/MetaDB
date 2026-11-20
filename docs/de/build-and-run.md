# Bauen und starten

[Überblick](./README.md) · [English](../en/build-and-run.md) · [Français](../fr/build-and-run.md) · [日本語](../ja/build-and-run.md) · [简体中文](../zh/build-and-run.md)

## Werkzeugkette

`rust-toolchain.toml` setzt den Kanal `1.100.0` mit den Komponenten `rustfmt` und `clippy` fest. Die Crate-Edition ist `2021`. Die Rust-Quellen linken keine C- oder C++-Bibliothek. Das Übersetzen von `metadb-proto` braucht aber das Programm `protoc` (`prost-build` ruft es auf). Auf Debian oder Ubuntu ist das Paket `protobuf-compiler`. Liegt `protoc` woanders, `PROTOC` auf seinen Pfad setzen. Aus einem Checkout:

```bash
rustc --version
cargo --version
```

`rustup` installiert die festgesetzte Werkzeugkette beim ersten Cargo-Aufruf, wenn es herunterladen darf.

## Bauen

```bash
cargo build --release -p metadb-server --bins
```

Das erzeugt:

- `target/release/metadbd`
- `target/release/metadb-ctl`

Debug-Builds verwenden `cargo build -p metadb-server --bins` und schreiben dieselben Namen unter `target/debug/`.

`cargo fmt --all -- --check` und `cargo clippy --workspace --all-targets -- -D warnings` sind die Format- und Lint-Befehle dieses Repositorys. Tests stehen in [Tests und Benches](./testing.md).

## `metadbd` starten

Flags von `metadbd` (`crates/metadb-server/src/bin/metadbd.rs`):

| Flag | Wirkung |
| :--- | :--- |
| `--config <Pfad>` | Lädt TOML. Ohne Flag wird `ServerTomlConfig::default()` verwendet |
| `--node-id <u64>` | Überschreibt `node.node_id` |
| `--cell-id <u64>` | Überschreibt `node.cell_id` |
| `--role <Name>` | `all-in-one`, `be`, `proxy`, `cell-manager` oder `global-manager`. Nur `all-in-one` läuft weiter |
| `--listen <Host:Port>` | Überschreibt `node.grpc_listen_addr` |
| `--data-dir <Pfad>` | Überschreibt `node.data_dir` |
| `--cell-manager <Host:Port>` | Ruft nach dem Start `RegisterNode` an diesem Endpunkt auf |

Jede Rolle außer `all-in-one` scheitert, bevor der Listener gebunden wird, mit `server role ... is not implemented; only all-in-one runs a process`.

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

Logs verwenden den `EnvFilter` von `tracing-subscriber`. Der Standardfilter ist `info`. Überschreiben mit der üblichen Variable `RUST_LOG`, zum Beispiel `RUST_LOG=debug`.

An empty `security.shared_secret` does not start the process. Loopback is not an implicit exception. Development without credentials sets `allow_unauthenticated = true` and keeps every listener on a literal loopback IP (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that opt-in. A wildcard or other non-loopback address refuses to listen. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. A non-empty `[security].shared_secret` requires the same bearer for metadata, transaction, control-plane, and Raft transport RPCs, and for `GET /metrics`. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` terminate TLS 1.3 in this process. See `docs/designs/transport_security.md`. A TLS, certificate, anonymous, or insecure key in the configuration fails startup instead of serving plaintext. Keep an opted-in listener on a trusted network, or put every service behind a proxy that authenticates the client and block direct access to this process.

Strg-C schreibt den Shard-Zustand und stoppt die OpenRaft-Gruppen (`cluster.shutdown()`).

## Die Bibliothek einbetten

Pfadabhängigkeiten aus einem Crate außerhalb dieses Workspace:

```toml
[dependencies]
metadb-core = { path = "path/to/MetaDB/crates/metadb-core" }
metadb-server = { path = "path/to/MetaDB/crates/metadb-server" }
tokio = { version = "1", features = ["full"] }
```

Dauerhaftes Öffnen. `MetaDbEngine::open` liefert `Arc<PersistentMetaDbCluster>` und bindet ebenfalls gRPC.

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

`mkdir_auto_id` und `create_file_auto_id` vergeben Kennungen aus `LocalIdBuffer`. Aufrufer, die Kennungen schon haben, nutzen `tx_router()`:

- `create_file`, `mkdir`, `unlink`, `rmdir`, `setattr`, `rename`
- `lookup`, `dirstat`, `listdir_paginated`
- `set_quota_limit`, `get_quota`, `list_quotas`
- `get_secondary_index`, `scan_secondary_index`
- `init_dir_attr` (beim Bootstrap für die Wurzel)

`PersistentMetaDbCluster::open_and_serve` ist derselbe Öffnungspfad und liefert zusätzlich die gebundene Adresse als Zeichenkette.

`MetaDbEngine::new_single_node` ist synchron und im Speicher. Nur dort verwenden, wo ein Test es schon verwendet. Es ist nicht absturzsicher und nicht das, was `metadbd` ausführt.

## Betriebs-CLI

`metadb-ctl` ist ein Client. Es öffnet kein Datenverzeichnis. Es auf einen laufenden `metadbd` richten:

```bash
cargo run -p metadb-server --bin metadb-ctl -- --endpoint 127.0.0.1:7800 cluster status
```

Befehle, Authentifizierung und der Bench-Client stehen in [Betrieb](./operations.md) und [Tests und Benches](./testing.md).
