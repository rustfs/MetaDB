# MetaDB

**MetaDB** is a high-performance, 100% pure-Rust, file-system structure-aware distributed transactional metadata database engine built for [RustFS](https://github.com/rustfs).

* **Implementation guides** (what this tree runs): [English](./docs/en/README.md) | [Français](./docs/fr/README.md) | [Deutsch](./docs/de/README.md) | [日本語](./docs/ja/README.md) | [简体中文](./docs/zh/README.md)
* **Earlier operator guides**: [Documentation Hub](./docs/README.md) — [English](./docs/guides/en/README.md) | [日本語](./docs/guides/ja/README.md) | [Français](./docs/guides/fr/README.md) | [简体中文](./docs/guides/zh/README.md) | [Deutsch](./docs/guides/de/README.md)
* **Architecture & Engineering**: [`Architecture.md`](./Architecture.md) | [`docs/DEVELOPMENT_PLAN.md`](./docs/DEVELOPMENT_PLAN.md) | [`docs/designs/SITEMAP.md`](./docs/designs/SITEMAP.md) | [`docs/designs/review_report.md`](./docs/designs/review_report.md)

> **Academic & Architectural Attribution**:
> The architecture of `rustfs/MetaDB` is inspired by and adapted from Baidu's trillion-scale production metadata center paper:
> **Zebin Ren ([@ZebinRen](https://github.com/ZebinRen)), Biao Cao, Kang Chen ([@kangchenPKU](https://github.com/kangchenPKU)), Zhihui Yin, Qiushi Chen, Jielong Jian, Jiwei Xiong, Xinxing Wang, Yan Xing, Liguo Duan, Ran Zheng, Xianjun Meng, and Zhenyu Hou**, *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1) (Vrije Universiteit Amsterdam, Baidu & Peking University, 2026).
> *(Note: Trillion-record production figures cited from `arXiv:2610.32247v1` are Baidu production metrics, not results of this repository. [Current Implementation Status](#current-implementation-status) does not record a first-launch or trillion-scale pass.)*
>
> **Spec-Driven Development Methodology**:
> Designed and stress-tested as a specification with Google's [Elephant-Goldfish Model (EGM)](https://github.com/google/elephant-goldfish-model). That review is not a first-launch or scale-gate pass.

---

## Why MetaDB?

General-purpose distributed databases (such as TiDB, CockroachDB, and FoundationDB) treat file-system and object-storage metadata as generic flat key-value or relational rows. The three bottlenecks below are Baidu production observations reported in `arXiv:2610.32247v1`. None of the ratios or latencies is a measurement of this repository.

1. **Data Model Mismatch**: The paper reports that flat hash or byte-midpoint sharding turns `>96%` of localized single-directory operations (`create`, `mkdir`, `unlink`, `listdir`) into multi-shard Two-Phase Commit (2PC). That percentage is Baidu's workload figure, not a ratio counted in this tree.
2. **Transaction Model Mismatch**: The paper reports that synchronously locking the shared parent-directory attribute record collapses concurrent single-directory write throughput by `10x–36x` (including a `36.2x` gap versus TiDB in that study). Synchronous `QuotaTable` and `SecondaryIndexTable` updates are the paper's description of extra cross-shard 2PC, not a benchmark of this repository.
3. **Storage Implementation Mismatch**: The paper reports a double-tombstone penalty for short-lived (`<10ms`) metadata transactions, and `listdir` latency moving from `<100ms` to `~6 seconds` under bulk TTL deletion (`arXiv:2610.32247v1`, §2.3.3 and §7.4). Those latencies are not measurements of this repository.

---

## Three Core Architectural Pillars

### 1. Tree-Topology-Induced Sharding and Execution (`metadb-cluster` & `metadb-tx`)
* **Colocated Schema (`(PID, "_attr_")`)**: `MetaTable` is keyed by `(PID, name)` with `group_key = PID`. A directory's own attributes are stored in a reserved child entry `(PID, "_attr_")` physically adjacent to its direct children `(PID, child_name)`.
* **Epoch-Fenced `PID`-Boundary Sharding & Jumbo Defense**: Shards split strictly along `group_key = PID` boundaries under `ShardEpoch` fencing. In this tree, jumbo isolation is a `JumboIsolated` route flag. It does not migrate peer shards to a dedicated host. **`DualModeFineGrained` `(PID, name)` splitting** uses shard-local `DirShardDelta` records for `1-RTT` writes and `1-hop` scatter-gather `dirstat`.
* **Single-Shard Pushdown + Crash-Recoverable 2PC**: Executes single-directory Read-Modify-Write (`create`, `unlink`, `setattr`, `lookup`, `dirstat`, `listdir`, and colocated `mkdir`/`rmdir`) in `1 RTT` on the shard leader, reserving Primary-Anchored 2PC (`WriteIntent` + `TxnRecord`) for cross-shard `rename` and cross-shard `mkdir`/`rmdir`. The `<3%` share of cross-directory `rename` is a Baidu paper workload figure (`arXiv:2610.32247v1`, Table 2), not a benchmark of this repository.

### 2. Semantic-Aware Off-Critical-Path Processing (`metadb-tx`)
* **Conflict-Free Asynchronous Attribute Aggregation**: Removes parent directory attribute locks from the foreground write path. Child mutations append deterministic deltas to `AsyncAttrAggregator`, folded into `(PID, "_attr_")` at the L0 SSTable **Coordinated Flush Barrier** while **Read-Path Reconciliation** guarantees linearizable `dirstat` reads.
* **Exact-Once Asynchronous `AuxOutbox`**: Propagates `QuotaTable` and `SecondaryIndexTable` (`FID -> (PID, name)`) updates in strict FIFO per-shard LSN batches with `[0x07]` local overflow spill so Raft log compaction is never blocked by remote partitions.

### 3. Lifecycle-Aware Pure-Rust Storage Engine (`metadb-storage` & `metadb-raft`)
* **WAL-less LSM + 3-Watermark OpenRaft**: Disables local KV WAL files in favor of segmented CRC32C Raft logs (`openraft`), clamping log purge to `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)`.
* **`SnapshotGuard` In-Memory MVCC**: Retains multi-version chains in memory for the `5s` transaction window and gates an unversioned L0 collapse-flush on `ActiveSnapshotRegistry` (`min_active_snapshot_hlc >= imm.max_hlc`). That flush may keep only the latest value of a key. It does not pin every historical version, and it does not stop an unversioned SSTable from dropping older versions of other keys. A range read at `start_hlc` can therefore include later values of keys an open 2PC lease does not cover. Point reads of the keys that lease records stay at the pre-image until commit. The `~60%` fewer KV writes figure is a paper/model claim from `arXiv:2610.32247v1`, not a measurement of this repository. The same range-read limit is in [docs/en/architecture.md](./docs/en/architecture.md).
* **Bottommost-Safe Locality-Aware Compaction**: Tracks tombstone density per 9-byte prefix bucket and compacts hot deletion ranges down to the deepest overlapping SSTable level. A flat `listdir` P99 under `<4ms` is a design target in `arXiv:2610.32247v1`, not a measurement of this repository.

---

## Single-Node to Multi-Cell Evolution

The design keeps one `StorageBackend` and one `openraft` state machine from an embedded single-binary node toward a multi-Cell cluster. Stage 3 is a design target. This binary does not deploy it.

* **Stage 1 — Single-Node All-in-One (`1-Voter` OpenRaft)**: Zero network coordination; sequential Raft log persistence with `metadbd --role all-in-one` or the persistent API `MetaDbEngine::open` / `PersistentMetaDbCluster`. `MetaDbEngine::new_single_node` is a separate in-memory simulator: no data directory, no OpenRaft group, and no WAL-less LSM.
* **Stage 2 — Dynamic Multi-Shard Split**: Splits shards along `group_key = PID` boundaries on a single host.
* **Stage 3 — Multi-Cell Distributed Cluster (design only)**: The design describes `AddLearner`, SSTable snapshot catch-up, joint-consensus voter promotion, decentralized `HLC` clocks, stateless `MetaCache`, and a two-tier control plane. Role-trimmed `be`, `proxy`, `cell-manager`, and `global-manager` processes are not a deployment this tree runs.

`metadbd` today runs Stage 1, plus in-process PID-boundary splits (Stage 2) on that same process. `open_and_serve` rejects every role except `all-in-one`. A second `metadbd --role all-in-one` joins the same cell when `--join` or `--cell-manager` names the current leader and the data directory is fresh (`crates/metadb-server/tests/join_existing_group.rs`). Without that join, a new data directory bootstraps its own cell. `RegisterCell` does not store a second Cell. A three-`metadbd` voter rehearsal is not in this tree. See [docs/en/operations.md](./docs/en/operations.md).

---

## 8-Crate Pure-Rust Workspace Architecture

| Crate | In this tree | Responsibility |
| :--- | :--- | :--- |
| [`crates/metadb-core`](./crates/metadb-core) | Present (phase label 1) | `0x01..0x07` CRC32C binary codecs, `(PID, "_attr_")` keys, `ShardEpoch`, `CompositeKeyRange`, `HLC` (+ drift/overflow guards). |
| [`crates/metadb-proto`](./crates/metadb-proto) | Present (phase label 1) | Protobuf v1 schemas & `tonic`/`prost` gRPC contracts (`MetadataService`, `TransactionService`, `RaftTransportService`, `ControlPlaneService`). |
| [`crates/metadb-storage`](./crates/metadb-storage) | Present (phase label 2) | `StorageBackend` trait, pure-Rust WAL-less SSTable LSM, `MANIFEST`, 16-shard LRU `BlockCache`, Bloom filter, `SnapshotGuard` `ReadCut` MVCC, `LocalityCompactor`. |
| [`crates/metadb-raft`](./crates/metadb-raft) | Present (phase label 3) | `openraft` 0.10 Multi-Raft, segmented CRC32C `.raftlog`, 3-watermark purge gate, batched `ReadIndex`, SSTable snapshots. In-process `1->3` voter coverage is not a three-VM rehearsal. |
| [`crates/metadb-tx`](./crates/metadb-tx) | Present (phase label 4) | Deterministic `TxStateMachineHandler`, `1-RTT` pushdown, `ReplicatedAttrAggregator`, contiguous `ShardAuxOutbox` + `[0x07]` spill, crash-recoverable 2PC with in-flight lease renewal. |
| [`crates/metadb-cluster`](./crates/metadb-cluster) | Present (phase label 5) | 3-stage `ExecuteSplit` (`PID` & `DualModeFineGrained`), `ShardEpoch` fencing, a `JumboIsolated` route flag, `DurableGlobalManager`, `DurableCellManager`, `CompositeMetaCache`, `DurableIdAllocator`. |
| [`crates/metadb-proxy`](./crates/metadb-proxy) | Present (phase label 5) | Tokenizing `SqlLexer` & compiler for all 20 parameterized metadata SQL templates + `StatelessProxyRouter`. |
| [`crates/metadb-server`](./crates/metadb-server) | Present (phase label 6) | `PersistentMetaDbCluster` & `MetaDbEngine` SDKs + `metadbd` daemon + `metadb-ctl` operator CLI + Prometheus metrics. `metadbd` serves `all-in-one` only. |

---

## Current Implementation Status

Phase labels below mean the crates and the tests described in [docs/en/testing.md](./docs/en/testing.md) are in this tree. They do not mean a multi-process Cell deployment is supported, and they do not mean a first-launch or scale-gate pass. Trillion-scale figures in `arXiv:2610.32247v1` are Baidu production metrics, not results of this repository. The `listdir` P99 `<4ms` figure is a design target from that paper, not a measurement of this tree. Applied memtable settings, in-flight 2PC lease renewal, and the seeded simulation catalog are on this tree. They are not Baidu production measurements.

* **Phases 0–6 are in this tree; they are not a first-launch or scale-gate verification**:
  * **Phase 0**: Master `Architecture.md`, EGM subsystem specs, zero-context Goldfish review & resolutions (`B1`–`B18`, `N1`–`N5`), `DEVELOPMENT_PLAN.md`, and 5-language usage guides (`en`, `ja`, `fr`, `zh`, `de`). The Goldfish review checked the specification. It is not a runtime acceptance test.
  * **Phases 1–6**: The 8 crates are a pure-Rust implementation (`zero C/C++ FFI`). The commands checked into the workspace are `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets -- -D warnings`, and `cargo test --workspace`. Those suites include fault-injection coverage for SSTable/`MANIFEST` crash recovery, `SnapshotGuard` `ReadCut` isolation, `LocalityCompactor`, in-process `1-Voter -> 3-Voter` snapshot promotion and leader failover, `[0x07]` outbox spill/cleanup, 2PC crash recovery (including lease renewal), 3-way `DualModeFineGrained` scatter-gather, all 20 SQL templates, and the gRPC tests listed in [docs/en/testing.md](./docs/en/testing.md). `crates/metadb-storage/tests/first_ship_storage_regressions.rs` covers live-row `listdir` limit pushdown and density rebuild on open. That file is not a launch pass. Process-restart `listdir` and multi-process 3-voter failover `listdir` with `entries_examined` assertions are not in this tree.

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

`.github/workflows/ci.yml` lists those three commands with `--locked` (tests also pass `--no-fail-fast`). This README does not say that workflow is enabled. See [CI and release packages](#ci-and-release-packages).

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

`metadb-ctl bench --workload hotspot-create` and `--workload bulk-ttl-delete` dial a remote gRPC endpoint and print JSON: a header, then phase reports with `attempted`, `succeeded`, `failed`, and latency percentiles. The process exits non-zero unless every requested operation succeeds. `--workload mixed` and `--embedded` run the in-process stress harness. Numbers from that command are for the machine that ran it; this repository does not record them as a result.

`metadbd` only runs `--role all-in-one`. `topology_distributed` still builds a 3-voter cell in-process. A second OS process can join that cell's Raft groups: `cargo test -p metadb-server --test join_existing_group`. `RegisterCell` does not create a second Cell. The checked-in `ci.yml` lists `cargo fmt`, `cargo clippy --locked`, and `cargo test --locked`. Job `rust-check-lint-test` has `timeout-minutes: 90`. Job `package` has `timeout-minutes: 45`. The file sets `cancel-in-progress` for a newer push to the same ref and rejects a bare `#[ignore]` attribute. This README does not say that workflow is enabled.

## CI and release packages

[`.github/workflows/ci.yml`](./.github/workflows/ci.yml) is the test and package workflow file. The same directory also contains `dependency-audit.yml` and `security-adversarial-regressions.yml`. This section records timeouts checked into `ci.yml`. It does not say any workflow is enabled.

| Job | When it runs | What it does |
| :--- | :--- | :--- |
| Format, Clippy & Test | Every push, every pull request to `main`, and `v*` tags | `cargo fmt --all -- --check`, `cargo clippy --workspace --all-targets --locked -- -D warnings`, `cargo test --workspace --locked --no-fail-fast` on `ubuntu-24.04`. A bare `#[ignore]` with no reason string fails the job. |
| Package `x86_64-unknown-linux-gnu` | Pull requests, pushes to `main`, and `v*` tags | Native `ubuntu-24.04` release build of `metadbd` and `metadb-ctl`. |
| Package `aarch64-unknown-linux-gnu` | Same as the x86_64 package job | Native `ubuntu-24.04-arm` release build. No cross or musl toolchain. |
| Publish GitHub Release | Push of a `v*` tag, after tests and both packages succeed | Uploads the archives and checksums. `contents: write` is granted only on this job. |

The file sets `cancel-in-progress` for a newer push to the same ref. Checked-in `timeout-minutes` values are 90 for Format, Clippy & Test (`rust-check-lint-test`), 20 for the CI-scale prefix load (`prefix-load-ci`), 45 for each package job, and 15 for the release job.

Package artifacts (also attached to the GitHub Release for a `v*` tag):

- `metadb-<version>-x86_64-unknown-linux-gnu.tar.gz`
- `metadb-<version>-aarch64-unknown-linux-gnu.tar.gz`
- a `.sha256` sidecar for each archive

`<version>` is `[workspace.package].version` in `Cargo.toml` (currently `0.1.0`). `metadbd --version` reports that same string. The Actions artifact name is the tarball basename, `metadb-<version>-<target>`. The tarball contains `metadbd`, `metadb-ctl`, `LICENSE`, and `SHA256SUMS` for those files. These are glibc builds. The workflow does not publish to crates.io. On a tag, the release also includes one `SHA256SUMS` covering both archives. A tag with a hyphen (for example `v0.1.0-rc.1`) is marked as a prerelease. The tag text is not required to equal the crate version; asset names follow `Cargo.toml`.

`stress_local_mixed_workload` stays `#[ignore]` and is not part of this CI job. The 5-second `stress_smoke_mixed_workload` still runs.

## License

Licensed under the [Apache License, Version 2.0](./LICENSE).
