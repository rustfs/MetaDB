# MetaDB

**MetaDB** is a high-performance, 100% pure-Rust, file-system structure-aware distributed transactional metadata database engine built for [RustFS](https://github.com/rustfs).

* **Implementation guides** (what this tree runs): [English](./docs/en/README.md) | [Français](./docs/fr/README.md) | [Deutsch](./docs/de/README.md) | [日本語](./docs/ja/README.md) | [简体中文](./docs/zh/README.md)
* **Earlier operator guides**: [Documentation Hub](./docs/README.md) — [English](./docs/guides/en/README.md) | [日本語](./docs/guides/ja/README.md) | [Français](./docs/guides/fr/README.md) | [简体中文](./docs/guides/zh/README.md) | [Deutsch](./docs/guides/de/README.md)
* **Architecture & Engineering**: [`Architecture.md`](./Architecture.md) | [`docs/DEVELOPMENT_PLAN.md`](./docs/DEVELOPMENT_PLAN.md) | [`docs/designs/SITEMAP.md`](./docs/designs/SITEMAP.md) | [`docs/designs/review_report.md`](./docs/designs/review_report.md)

> **Academic & Architectural Attribution**:
> The architecture of `rustfs/MetaDB` is inspired by and adapted from Baidu's trillion-scale production metadata center paper:
> **Biao Cao, Kang Chen, Zhihui Yin, Qiushi Chen, Jielong Jian, Jiwei Xiong, Xinxing Wang, Yan Xing, Liguo Duan, Ran Zheng, Xianjun Meng, and Zhenyu Hou**, *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1) (Baidu & Peking University, 2026).
> *(Note: Trillion-record production figures cited from `arXiv:2610.32247v1` reflect Baidu's production deployment; see [Current Implementation Status](#current-implementation-status) below for `rustfs/MetaDB` repository milestones.)*
>
> **Spec-Driven Development Methodology**:
> Designed, stress-tested, and verified using Google's [Elephant-Goldfish Model (EGM)](https://github.com/google/elephant-goldfish-model).

---

## Why MetaDB?

General-purpose distributed databases (such as TiDB, CockroachDB, and FoundationDB) treat file-system and object-storage metadata as generic flat key-value or relational rows. At exabyte scale, this "black-box" approach creates three severe bottlenecks:

1. **Data Model Mismatch**: Flat hash or byte-midpoint sharding fractures entries of the same directory across multiple shards, turning `>96%` of localized single-directory operations (`create`, `mkdir`, `unlink`, `listdir`) into multi-shard Two-Phase Commit (2PC) transactions and incurring extra network round-trips.
2. **Transaction Model Mismatch**: Synchronously locking the shared parent-directory attribute record (`num_children`, `size_bytes`, `modify_time`) collapses concurrent single-directory write throughput by `10x–36x`, while synchronous updates to `QuotaTable` and `SecondaryIndexTable` force unnecessary cross-shard 2PC on every write.
3. **Storage Implementation Mismatch**: Persisting multi-version concurrency control (MVCC) tombstones to disk for transient (`<10ms`) metadata transactions creates a **double-tombstone penalty** (MVCC tombstones + LSM-tree tombstones). Under consecutive bulk TTL deletions, accumulated tombstones degrade `listdir` range-scan latency from `<100ms` to `~6 seconds`.

---

## Three Core Architectural Pillars

### 1. Tree-Topology-Induced Sharding and Execution (`metadb-cluster` & `metadb-tx`)
* **Colocated Schema (`(PID, "_attr_")`)**: `MetaTable` is keyed by `(PID, name)` with `group_key = PID`. A directory's own attributes are stored in a reserved child entry `(PID, "_attr_")` physically adjacent to its direct children `(PID, child_name)`.
* **Epoch-Fenced `PID`-Boundary Sharding & Jumbo Defense**: Shards split strictly along `group_key = PID` boundaries under `ShardEpoch` fencing, paired with **Jumbo-Shard Dedicated-Node Isolation** and **`DualModeFineGrained` `(PID, name)` Splitting** (using shard-local `DirShardDelta` records for `1-RTT` writes and `1-hop` scatter-gather `dirstat`).
* **Single-Shard Pushdown + Crash-Recoverable 2PC**: Executes single-directory Read-Modify-Write (`create`, `unlink`, `setattr`, `lookup`, `dirstat`, `listdir`, and colocated `mkdir`/`rmdir`) in `1 RTT` on the shard leader, reserving Primary-Anchored 2PC (`WriteIntent` + `TxnRecord`) for cross-shard `rename` (`<3%` of workload) and cross-shard `mkdir`/`rmdir`.

### 2. Semantic-Aware Off-Critical-Path Processing (`metadb-tx`)
* **Conflict-Free Asynchronous Attribute Aggregation**: Removes parent directory attribute locks from the foreground write path. Child mutations append deterministic deltas to `AsyncAttrAggregator`, folded into `(PID, "_attr_")` at the L0 SSTable **Coordinated Flush Barrier** while **Read-Path Reconciliation** guarantees linearizable `dirstat` reads.
* **Exact-Once Asynchronous `AuxOutbox`**: Propagates `QuotaTable` and `SecondaryIndexTable` (`FID -> (PID, name)`) updates in strict FIFO per-shard LSN batches with `[0x07]` local overflow spill so Raft log compaction is never blocked by remote partitions.

### 3. Lifecycle-Aware Pure-Rust Storage Engine (`metadb-storage` & `metadb-raft`)
* **WAL-less LSM + 3-Watermark OpenRaft**: Disables local KV WAL files in favor of segmented CRC32C Raft logs (`openraft`), clamping log purge to `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)`.
* **`SnapshotGuard` In-Memory MVCC**: Retains multi-version chains in memory for the `5s` transaction window, gating unversioned L0 SSTable collapse-flushes on `ActiveSnapshotRegistry` (`min_active_snapshot_hlc >= imm.max_hlc`) to eliminate persisted MVCC tombstones (`~60%` fewer KV writes) with zero future-read anomalies.
* **Bottommost-Safe Locality-Aware Compaction**: Tracks tombstone density per 9-byte prefix bucket and compacts hot deletion ranges down to the deepest overlapping SSTable level to keep `listdir` latency flat.

---

## Single-Node to Multi-Cell Evolution

MetaDB runs the **exact same `StorageBackend` and `openraft` state machine** from an embedded single-binary node to a multi-Cell cluster:

* **Stage 1 — Single-Node All-in-One (`1-Voter` OpenRaft)**: Zero network coordination; sequential Raft log persistence + embedded Rust SDK or `metadbd --role all-in-one`.
* **Stage 2 — Dynamic Multi-Shard Split**: Automatically splits shards along `group_key = PID` boundaries on a single host.
* **Stage 3 — Multi-Cell Distributed Cluster (`3-Voter` Multi-Raft)**: Online `AddLearner` -> SSTable snapshot catch-up -> joint-consensus `Voter` promotion, decentralized `HLC` clocks, stateless `MetaCache`, and a **Two-Tier Control Plane (`GlobalManager` + `CellLocalManager`)**.

`metadbd` today runs Stage 1, plus in-process PID-boundary splits (Stage 2) on that same process. `open_and_serve` rejects every role except `all-in-one`. A second process bootstraps its own cell. `RegisterCell` does not store a second Cell. Learner promotion exists in-process; a three-`metadbd` deployment does not. See [docs/en/operations.md](./docs/en/operations.md).

---

## 8-Crate Pure-Rust Workspace Architecture

| Crate | Status | Responsibility |
| :--- | :--- | :--- |
| [`crates/metadb-core`](./crates/metadb-core) | Implemented (Phase 1) | `0x01..0x07` CRC32C binary codecs, `(PID, "_attr_")` keys, `ShardEpoch`, `CompositeKeyRange`, `HLC` (+ drift/overflow guards). |
| [`crates/metadb-proto`](./crates/metadb-proto) | Implemented (Phase 1) | Protobuf v1 schemas & `tonic`/`prost` gRPC contracts (`MetadataService`, `TransactionService`, `RaftTransportService`, `ControlPlaneService`). |
| [`crates/metadb-storage`](./crates/metadb-storage) | Implemented (Phase 2) | `StorageBackend` trait, pure-Rust WAL-less SSTable LSM, `MANIFEST`, 16-shard LRU `BlockCache`, Bloom filter, `SnapshotGuard` `ReadCut` MVCC, `LocalityCompactor`. |
| [`crates/metadb-raft`](./crates/metadb-raft) | Implemented (Phase 3) | `openraft` 0.10 Multi-Raft, segmented CRC32C `.raftlog`, 3-watermark purge gate, batched `ReadIndex`, SSTable snapshots, `1->3` Voter `ConfChange`. |
| [`crates/metadb-tx`](./crates/metadb-tx) | Implemented (Phase 4) | Deterministic `TxStateMachineHandler`, `1-RTT` pushdown, `ReplicatedAttrAggregator`, contiguous `ShardAuxOutbox` + `[0x07]` spill, crash-recoverable 2PC. |
| [`crates/metadb-cluster`](./crates/metadb-cluster) | Implemented (Phase 5) | 3-stage `ExecuteSplit` (`PID` & `DualModeFineGrained`), `ShardEpoch` fencing, Jumbo isolation, `DurableGlobalManager`, `DurableCellManager`, `CompositeMetaCache`, `DurableIdAllocator`. |
| [`crates/metadb-proxy`](./crates/metadb-proxy) | Implemented (Phase 5) | Tokenizing `SqlLexer` & compiler for all 20 parameterized metadata SQL templates + `StatelessProxyRouter`. |
| [`crates/metadb-server`](./crates/metadb-server) | Implemented (Phase 6) | `PersistentMetaDbCluster` & `MetaDbEngine` SDKs + `metadbd` daemon + `metadb-ctl` operator CLI + Prometheus metrics. |

---

## Current Implementation Status

Phase completion in the list below means the crates and the tests in [docs/en/testing.md](./docs/en/testing.md) are in this tree. It does not mean a multi-process Cell deployment is supported.

* **Phases 0–6 Completed & Verified**:
  * **Phase 0**: Master `Architecture.md`, EGM subsystem specs, zero-context Goldfish review & resolutions (`B1`–`B18`, `N1`–`N5`), `DEVELOPMENT_PLAN.md`, and 5-language usage guides (`en`, `ja`, `fr`, `zh`, `de`).
  * **Phases 1–6**: Full 8-crate 100% pure-Rust implementation (`zero C/C++ FFI`) passing `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace` (including fault-injection suites for SSTable/MANIFEST crash recovery, `SnapshotGuard` `ReadCut` isolation, `LocalityCompactor`, `1-Voter -> 3-Voter` Multi-Raft snapshot promotion & leader failover, `[0x07]` outbox spill/cleanup, 2PC crash recovery, 3-way `DualModeFineGrained` scatter-gather, all 20 SQL templates, and end-to-end gRPC cluster evolution).

---

## Build & Test

The toolchain is pinned to Rust **1.100.0** in `rust-toolchain.toml` (`rustfmt`, `clippy`). Crate edition stays `2021`.

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

Operator and developer guides in English, French, German, Japanese, and Simplified Chinese are under [docs/en/](./docs/en/README.md) (and `docs/fr`, `docs/de`, `docs/ja`, `docs/zh`).

`cargo test --workspace` includes the persistent topology suites and the stress smoke. T1–T6 still exercise the in-memory `MetaDbEngine::new_single_node` simulator. The persistent suites open a real `data_dir` through `PersistentMetaDbCluster` / `DurableCellManager`.

CI runs those three commands from a clean tree with `--locked` (tests also pass `--no-fail-fast`). See [CI and release packages](#ci-and-release-packages).

| Suite | Command |
| :--- | :--- |
| Single-node metadata, shutdown, and reopen | `cargo test -p metadb-server --test topology_single_node` |
| Single-node shard split and keyspace coverage | `cargo test -p metadb-server --test topology_scale_out` |
| 3-voter membership, metadata, leader change, and voter restart | `cargo test -p metadb-server --test topology_distributed` |
| Unflushed restart overlapping in-flight creates | `cargo test -p metadb-server --test stress_crash_restart -- --nocapture` |
| 3-voter leader loss and restart under create load | `cargo test -p metadb-server --test topology_distributed three_voter_leader_loss_under_load -- --nocapture` |
| Stress smoke (5s mixed workload, correctness, latency) | `cargo test -p metadb-server --test stress_harness stress_smoke -- --nocapture` |

### Stress profiles

The reusable harness is `metadb_server::stress`. The smoke profile is fixed (5 seconds, 4 clients, 4 directories) and does not read the environment, so CI stays bounded. The local profile is ignored by `cargo test --workspace`:

```bash
# Defaults: 30s, concurrency 8, 16 directories.
METADB_STRESS_DURATION_SECS=120 \
METADB_STRESS_CONCURRENCY=16 \
METADB_STRESS_KEYSPACE=32 \
METADB_STRESS_SEED=7 \
  cargo test -p metadb-server --test stress_harness stress_local -- --ignored --nocapture

# Same harness inside a temporary all-in-one process:
cargo run -p metadb-server --bin metadb-ctl -- bench --embedded --profile smoke
cargo run -p metadb-server --bin metadb-ctl -- bench --embedded --profile local
```

`METADB_STRESS_DURATION_SECS`, `METADB_STRESS_CONCURRENCY`, `METADB_STRESS_KEYSPACE`, and `METADB_STRESS_SEED` apply to the local profile and to `metadb-ctl bench --embedded --profile local`. Explicit `--duration-secs`, `--concurrency`, `--keyspace`, and `--seed` override the profile.

`stress_crash_restart` stops a single-node process with `PersistentMetaDbCluster::shutdown_without_flush` while creates are still inside Raft propose, reopens the same `data_dir`, and checks listings, `dirstat`, quota, and a secondary index. A second stop covers a joined rename, unlink, and create tail. `three_voter_leader_loss_under_load` runs creates on a 3-voter cell, stops the leader without flushing, checks the new leader, and restarts the stopped voter while creates continue.

`metadb-ctl bench --workload hotspot-create` and `--workload bulk-ttl-delete` still drive a remote gRPC endpoint and report throughput only. `--workload mixed` is the embedded harness.

`metadbd` only runs `--role all-in-one`. `topology_distributed` still builds a 3-voter cell in-process. A second OS process can join that cell's Raft groups: `cargo test -p metadb-server --test join_existing_group`. `RegisterCell` does not create a second Cell. CI runs `cargo fmt`, `cargo clippy --locked`, and `cargo test --locked`, times the job out at 45 minutes, cancels a superseded run, and rejects a bare `#[ignore]` attribute.

## CI and release packages

[`.github/workflows/ci.yml`](./.github/workflows/ci.yml) is the only workflow.

| Job | When it runs | What it does |
| :--- | :--- | :--- |
| Format, Clippy & Test | Every push, every pull request to `main`, and `v*` tags | `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked --no-fail-fast` on `ubuntu-24.04`. A bare `#[ignore]` with no reason string fails the job. |
| Package `x86_64-unknown-linux-gnu` | Pull requests, pushes to `main`, and `v*` tags | Native `ubuntu-24.04` release build of `metadbd` and `metadb-ctl`. |
| Package `aarch64-unknown-linux-gnu` | Same as the x86_64 package job | Native `ubuntu-24.04-arm` release build. No cross or musl toolchain. |
| Publish GitHub Release | Push of a `v*` tag, after tests and both packages succeed | Uploads the archives and checksums. `contents: write` is granted only on this job. |

A new push to the same ref cancels the in-progress run. Each job stops after 45 minutes (15 minutes for the release job).

Package artifacts (also attached to the GitHub Release for a `v*` tag):

- `metadb-<version>-x86_64-unknown-linux-gnu.tar.gz`
- `metadb-<version>-aarch64-unknown-linux-gnu.tar.gz`
- a `.sha256` sidecar for each archive

`<version>` is `[workspace.package].version` in `Cargo.toml` (currently `0.1.0`). `metadbd --version` reports that same string. The Actions artifact name is the tarball basename, `metadb-<version>-<target>`. The tarball contains `metadbd`, `metadb-ctl`, `LICENSE`, and `SHA256SUMS` for those files. These are glibc builds. The workflow does not publish to crates.io. On a tag, the release also includes one `SHA256SUMS` covering both archives. A tag with a hyphen (for example `v0.1.0-rc.1`) is marked as a prerelease. The tag text is not required to equal the crate version; asset names follow `Cargo.toml`.

`stress_local_mixed_workload` stays `#[ignore]` and is not part of this CI job. The 5-second `stress_smoke_mixed_workload` still runs.

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
