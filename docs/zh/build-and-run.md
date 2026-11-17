# 构建与运行

[概览](./README.md) · [English](../en/build-and-run.md) · [Français](../fr/build-and-run.md) · [Deutsch](../de/build-and-run.md) · [日本語](../ja/build-and-run.md)

## 工具链

`rust-toolchain.toml` 把通道固定为 `1.100.0`，组件为 `rustfmt` 和 `clippy`。包的 edition 是 `2021`。Rust 源码不链接 C 或 C++ 库。编译 `metadb-proto` 仍然需要 `protoc` 可执行文件（`prost-build` 会调用它）。在 Debian 或 Ubuntu 上，对应的包是 `protobuf-compiler`。若 `protoc` 装在别处，把环境变量 `PROTOC` 设为它的路径。在检出目录中：

```bash
rustc --version
cargo --version
```

若允许下载，`rustup` 会在第一次 Cargo 调用时安装这个固定工具链。

## 构建

```bash
cargo build --release -p metadb-server --bins
```

产物：

- `target/release/metadbd`
- `target/release/metadb-ctl`

调试构建使用 `cargo build -p metadb-server --bins`，同样的名字写在 `target/debug/` 下。

`cargo fmt --all -- --check` 和 `cargo clippy --workspace --all-targets -- -D warnings` 是本仓库使用的格式化与静态检查命令。测试列在 [测试与压测](./testing.md)。

## 运行 `metadbd`

`metadbd` 的标志（`crates/metadb-server/src/bin/metadbd.rs`）：

| 标志 | 作用 |
| :--- | :--- |
| `--config <路径>` | 读取 TOML。没有它时使用 `ServerTomlConfig::default()` |
| `--node-id <u64>` | 覆盖 `node.node_id` |
| `--cell-id <u64>` | 覆盖 `node.cell_id` |
| `--role <名称>` | `all-in-one`、`be`、`proxy`、`cell-manager` 或 `global-manager`。只有 `all-in-one` 会继续 |
| `--listen <主机:端口>` | 覆盖 `node.grpc_listen_addr` |
| `--data-dir <路径>` | 覆盖 `node.data_dir` |
| `--cell-manager <主机:端口>` | 进程起来之后，对该端点调用 `RegisterNode` |

除 `all-in-one` 以外的角色在绑定监听器之前失败，错误为 `server role ... is not implemented; only all-in-one runs a process`。

示例，仅本机回环：

```bash
mkdir -p /tmp/metadb-data
cargo run -p metadb-server --bin metadbd -- \
  --data-dir /tmp/metadb-data \
  --listen 127.0.0.1:7800 \
  --node-id 1 \
  --cell-id 1 \
  --role all-in-one
```

使用文件时见 [配置](./configuration.md)：

```bash
cargo run -p metadb-server --bin metadbd -- --config ./standalone.toml
```

日志使用 `tracing-subscriber` 的 `EnvFilter`。默认过滤器是 `info`。用标准环境变量 `RUST_LOG` 覆盖，例如 `RUST_LOG=debug`。

`security.shared_secret` 为空时，进程在启动时记录一条警告：gRPC 没有认证，也没有 TLS。默认绑定是 `127.0.0.1`。不要把这个套接字公布到不可信网络。非空的 `[security].shared_secret` 对元数据、事务、控制面和 Raft 传输 RPC 要求同一个持有者令牌。进程仍然不终结 TLS。把监听器留在可信网络上，或把每个服务放到会认证客户端的代理后面，并阻断对本进程的直接访问。

Ctrl-C 会刷出分片状态并停止 OpenRaft 组（`cluster.shutdown()`）。

## 嵌入库

工作区之外的包使用路径依赖：

```toml
[dependencies]
metadb-core = { path = "path/to/MetaDB/crates/metadb-core" }
metadb-server = { path = "path/to/MetaDB/crates/metadb-server" }
tokio = { version = "1", features = ["full"] }
```

持久打开。`MetaDbEngine::open` 返回 `Arc<PersistentMetaDbCluster>`，同时绑定 gRPC。

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

`mkdir_auto_id` 和 `create_file_auto_id` 从 `LocalIdBuffer` 分配标识。调用方已经持有标识时使用 `tx_router()`：

- `create_file`、`mkdir`、`unlink`、`rmdir`、`setattr`、`rename`
- `lookup`、`dirstat`、`listdir_paginated`
- `set_quota_limit`、`get_quota`、`list_quotas`
- `get_secondary_index`、`scan_secondary_index`
- `init_dir_attr`（引导根目录时使用）

`PersistentMetaDbCluster::open_and_serve` 是同一条打开路径，并额外返回已绑定的地址字符串。

`MetaDbEngine::new_single_node` 是同步的、内存中的。只在已有测试使用它的地方使用。它不能在崩溃后恢复，也不是 `metadbd` 运行的引擎。

## 运维命令行

`metadb-ctl` 是客户端。它不打开数据目录。把它指向正在运行的 `metadbd`：

```bash
cargo run -p metadb-server --bin metadb-ctl -- --endpoint 127.0.0.1:7800 cluster status
```

命令、认证和压测客户端在 [运维](./operations.md) 与 [测试与压测](./testing.md)。
