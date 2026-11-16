# MetaDB implementation guide

[Français](../fr/README.md) · [Deutsch](../de/README.md) · [日本語](../ja/README.md) · [简体中文](../zh/README.md)

These pages describe the `rustfs/MetaDB` tree as it is implemented. They are the operator and developer guide for this repository. Design notes that describe a larger topology live in [`Architecture.md`](../../Architecture.md), [`docs/designs/`](../designs/SITEMAP.md), and the earlier guides under [`docs/guides/`](../README.md). Where those documents and the code disagree, follow this set.

## What this repository is

MetaDB is a pure-Rust metadata engine for file and object namespaces, written for [RustFS](https://github.com/rustfs). The data model follows directory topology: a directory and its direct children share a key prefix, so a single-directory create, lookup, list, or unlink stays on one shard. The design is adapted from Baidu and Peking University, *MetaDB: Putting File-System Structure Back into Distributed Databases*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1). Production figures in that paper describe Baidu's deployment. This repository does not publish a matching measurement.

The workspace version is `0.1.0` (Apache-2.0). The Rust toolchain is pinned to **1.100.0** in `rust-toolchain.toml`.

## What one process actually runs

`metadbd` and `PersistentMetaDbCluster::open_and_serve` start a single **all-in-one** process:

- one data directory
- one cell (`node.cell_id`, default `1`)
- one OpenRaft voter per shard (this process)
- one gRPC listener that serves `MetadataService`, `TransactionService`, `ControlPlaneService`, and `RaftTransportService`
- a background loop every 250 ms that drains auxiliary outboxes, runs 2PC recovery, checks split policy, and snapshots Raft logs

`ServerRole` also parses `be`, `proxy`, `cell-manager`, and `global-manager`. `open_and_serve` rejects every role except `all-in-one`. A second `metadbd` process with its own data directory bootstraps its own cell. It does not join the first process's Raft groups. `ControlPlaneService.RegisterCell` returns success and ignores the request, so a second Cell is not created by `metadb-ctl cell register`.

Shard splits, dual-mode `(PID, name)` splits, and jumbo route flags do run inside that one process. Replica promotion to three voters is implemented on `DurableCellManager::scale_out_replicas` and is covered by in-process Raft tests. It is not a three-`metadbd`-process deployment. See [Operations](./operations.md).

## Document map

| Page | Contents |
| :--- | :--- |
| [Architecture](./architecture.md) | Crates, keys, storage, Raft, transactions, sharding, and the two engines |
| [Build and run](./build-and-run.md) | Toolchain, binaries, embedded library, shutdown |
| [Configuration](./configuration.md) | TOML fields, defaults, and fields the process does not apply |
| [Operations](./operations.md) | `metadb-ctl`, metrics, scale-out, splits, limitations, documentation gaps |
| [Tests and benches](./testing.md) | `cargo test`, what each suite actually exercises, `metadb-ctl bench` |

## Workspace crates

| Crate | What the code does |
| :--- | :--- |
| `metadb-core` | Key codecs `0x01`–`0x07`, `ShardEpoch`, hybrid logical clock (500 ms default drift guard), 88-byte `InodeAttr` |
| `metadb-proto` | Protobuf and tonic services listed in [Architecture](./architecture.md) |
| `metadb-storage` | WAL-less SSTable LSM, `MANIFEST`, 16-way LRU block cache, in-memory MVCC, locality compaction |
| `metadb-raft` | `openraft` 0.10 groups, segmented CRC32C `.raftlog`, snapshots, learner-to-voter membership change |
| `metadb-tx` | Pushdown mutations, attribute deltas, auxiliary outbox, primary-anchored 2PC |
| `metadb-cluster` | Durable cell and global managers, ID allocator, route cache, split worker, jumbo planner |
| `metadb-proxy` | Lexer and parser for the 20 SQL templates, stateless router onto the transaction layer |
| `metadb-server` | `PersistentMetaDbCluster`, in-memory `MetaDbEngine::new_single_node`, `metadbd`, `metadb-ctl`, Prometheus text |

## Two ways to call the engine

`MetaDbEngine::open` and `PersistentMetaDbCluster::open_and_serve` open a data directory, LSM, and OpenRaft, and bind gRPC. That is the engine to run.

`MetaDbEngine::new_single_node` builds an in-memory simulator. It does not open a data directory, LSM, or OpenRaft group. Tests `T1`–`T6` in `crates/metadb-server/tests/integration_tests.rs` use it, including the "3-voter" step in `T6`, which only edits an in-memory membership map.
