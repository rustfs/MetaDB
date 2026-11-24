# MetaDB implementation guide

[Français](../fr/README.md) · [Deutsch](../de/README.md) · [日本語](../ja/README.md) · [简体中文](../zh/README.md)

These pages describe the `rustfs/MetaDB` tree as it is implemented. They are the operator and developer guide for this repository. Design notes that describe a larger topology live in [`Architecture.md`](../../Architecture.md), [`docs/designs/`](../designs/SITEMAP.md), and the earlier guides under [`docs/guides/`](../README.md). Where those documents and the code disagree, follow this set.

## What this repository is

MetaDB is a pure-Rust metadata engine for file and object namespaces, written for [RustFS](https://github.com/rustfs). The data model follows directory topology: a directory and its direct children share a key prefix, so a single-directory create, lookup, list, or unlink stays on one shard. The design is adapted from Baidu and Peking University, *MetaDB: Putting File-System Structure Back into Distributed Databases*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1). Production figures in that paper describe Baidu's deployment. This repository does not publish a matching measurement.

The workspace version is `0.1.0` (Apache-2.0). The Rust toolchain is pinned to **1.100.0** in `rust-toolchain.toml`.

## What one process actually runs

The default `metadbd` process is **all-in-one**. `be`, `proxy`, `cell-manager`, and `global-manager` also start when their configuration is valid. An all-in-one process has:

- one data directory
- one cell (`node.cell_id`, default `1`)
- one OpenRaft voter per shard (this process)
- one gRPC listener that serves `MetadataService`, `TransactionService`, `ControlPlaneService`, `RaftTransportService`, and `grpc.health.v1.Health` (`Check` and `Watch`)
- a background loop every 250 ms that drains auxiliary outboxes (including cross-cell `ApplyAuxBatch` via `GrpcRemoteAuxSink`), runs 2PC recovery, checks split policy, and snapshots Raft logs. Startup also runs an initial 2PC recovery pass on locally led shards before `/readyz` flips to 200. About every 3 seconds of monotonic time that loop persists a control-plane heartbeat and, when this process is not the Raft leader, sends that liveness record to the leader.

`ServerRole` parses `all-in-one`, `be`, `proxy`, `cell-manager`, and `global-manager`. `open_and_serve` starts those roles when that role's configuration is valid; an invalid combination fails before creating `data_dir` or binding a listener. Shard splits (1-voter and 3-voter), dual-mode `(PID, name)` splits, `ProbeTransferLeader`-guarded leadership transfer (`metadb-ctl raft transfer-leader`), 3-`metadbd` voter rehearsal (`three_process_ha_rehearsal`), and 4-role disaggregated cross-cell propagation (`disaggregated_four_role_e2e`) are covered in this tree. See [Operations](./operations.md).

## Document map

| Page | Contents |
| :--- | :--- |
| [Architecture](./architecture.md) | Crates, keys, storage, Raft, transactions, sharding, and the two engines |
| [Build and run](./build-and-run.md) | Toolchain, binaries, embedded library, shutdown |
| [Configuration](./configuration.md) | TOML fields, defaults, and fields the process does not apply |
| [Operations](./operations.md) | `metadb-ctl`, metrics, scale-out, splits, limitations, documentation gaps |
| [Operability](./operability.md) | `/livez`, `/readyz`, `/healthz`, phase codes, and the `[raft]` table |
| [Scale control plane](./scale-control-plane-status.md) | What multi-cell routing, roles, jumbo placement, heartbeats, and membership safety do in this tree, and what is still missing |
| [Tests and benches](./testing.md) | `cargo test`, what each suite actually exercises, `metadb-ctl bench` |
| [Simulation testing](./simulation-testing.md) | Seeded workload catalog, and how it relates to FoundationDB's testing design |

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
