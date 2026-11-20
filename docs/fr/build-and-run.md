# Compilation et exécution

[Présentation](./README.md) · [English](../en/build-and-run.md) · [Deutsch](../de/build-and-run.md) · [日本語](../ja/build-and-run.md) · [简体中文](../zh/build-and-run.md)

## Chaîne d'outils

`rust-toolchain.toml` épingle le canal `1.100.0` avec les composants `rustfmt` et `clippy`. L'édition des crates est `2021`. Les sources Rust ne lient pas de bibliothèque C ou C++. Compiler `metadb-proto` exige toutefois le binaire `protoc` (`prost-build` l'invoque). Sur Debian ou Ubuntu, le paquet est `protobuf-compiler`. Si `protoc` est installé ailleurs, définir `PROTOC` vers son chemin. Depuis une copie du dépôt :

```bash
rustc --version
cargo --version
```

`rustup` installe la chaîne épinglée au premier appel Cargo lorsqu'il est autorisé à la télécharger.

## Compilation

```bash
cargo build --release -p metadb-server --bins
```

Cela produit :

- `target/release/metadbd`
- `target/release/metadb-ctl`

Les builds de débogage utilisent `cargo build -p metadb-server --bins` et écrivent les mêmes noms sous `target/debug/`.

`cargo fmt --all -- --check` et `cargo clippy --workspace --all-targets -- -D warnings` sont les commandes de formatage et de lint utilisées par ce dépôt. Les tests sont listés dans [Tests et bancs](./testing.md).

## Lancer `metadbd`

Options de `metadbd` (`crates/metadb-server/src/bin/metadbd.rs`) :

| Option | Effet |
| :--- | :--- |
| `--config <chemin>` | Charge le TOML. Sans elle, `ServerTomlConfig::default()` est utilisé |
| `--node-id <u64>` | Remplace `node.node_id` |
| `--cell-id <u64>` | Remplace `node.cell_id` |
| `--role <nom>` | `all-in-one`, `be`, `proxy`, `cell-manager` ou `global-manager`. Seul `all-in-one` continue |
| `--listen <hôte:port>` | Remplace `node.grpc_listen_addr` |
| `--data-dir <chemin>` | Remplace `node.data_dir` |
| `--cell-manager <hôte:port>` | Une fois le processus démarré, appelle `RegisterNode` sur ce point de terminaison |

Tout rôle autre que `all-in-one` échoue avant la liaison de l'écouteur, avec `server role ... is not implemented; only all-in-one runs a process`.

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

Les journaux utilisent l'`EnvFilter` de `tracing-subscriber`. Le filtre par défaut est `info`. On le remplace avec la variable standard `RUST_LOG`, par exemple `RUST_LOG=debug`.

An empty `security.shared_secret` does not start the process. Loopback is not an implicit exception. Development without credentials sets `allow_unauthenticated = true` and keeps every listener on a literal loopback IP (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that opt-in. A wildcard or other non-loopback address refuses to listen. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. A non-empty `[security].shared_secret` requires the same bearer for metadata, transaction, control-plane, and Raft transport RPCs, and for `GET /metrics`. The process does not terminate TLS. A TLS, certificate, anonymous, or insecure key in the configuration fails startup instead of serving plaintext. Keep an opted-in listener on a trusted network, or put every service behind a proxy that authenticates the client and block direct access to this process.

Ctrl-C vide l'état des shards et arrête les groupes OpenRaft (`cluster.shutdown()`).

## Embarquer la bibliothèque

Dépendances de chemin depuis un crate extérieur à cet espace de travail :

```toml
[dependencies]
metadb-core = { path = "path/to/MetaDB/crates/metadb-core" }
metadb-server = { path = "path/to/MetaDB/crates/metadb-server" }
tokio = { version = "1", features = ["full"] }
```

Ouverture persistante. `MetaDbEngine::open` renvoie `Arc<PersistentMetaDbCluster>` et lie aussi gRPC.

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

`mkdir_auto_id` et `create_file_auto_id` allouent les identifiants depuis `LocalIdBuffer`. Les appelants qui ont déjà des identifiants utilisent `tx_router()` :

- `create_file`, `mkdir`, `unlink`, `rmdir`, `setattr`, `rename`
- `lookup`, `dirstat`, `listdir_paginated`
- `set_quota_limit`, `get_quota`, `list_quotas`
- `get_secondary_index`, `scan_secondary_index`
- `init_dir_attr` (utilisé à l'amorçage pour la racine)

`PersistentMetaDbCluster::open_and_serve` est le même chemin d'ouverture et renvoie en plus la chaîne d'adresse liée.

`MetaDbEngine::new_single_node` est synchrone et en mémoire. Ne l'utiliser que là où un test l'utilise déjà. Il n'est pas résistant aux plantages et ce n'est pas ce que `metadbd` exécute.

## CLI d'exploitation

`metadb-ctl` est un client. Il n'ouvre pas de répertoire de données. Le pointer vers un `metadbd` en cours d'exécution :

```bash
cargo run -p metadb-server --bin metadb-ctl -- --endpoint 127.0.0.1:7800 cluster status
```

Les commandes, l'authentification et le client de banc sont dans [Exploitation](./operations.md) et [Tests et bancs](./testing.md).
