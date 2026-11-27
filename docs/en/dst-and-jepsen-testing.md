# DST and Jepsen Testing for MetaDB

[Tests and benches](./testing.md) · [Simulation testing](./simulation-testing.md)

MetaDB ships two dedicated, complementary correctness verification suites in addition to unit, integration, and `fdb_chaos` ensembles:

1. **DST (Deterministic Simulation Testing)** — a true single-threaded discrete-event virtual-clock cluster simulator (`crates/metadb-server/src/dst.rs`, `crates/metadb-server/tests/dst_tests.rs`, `metadb-ctl dst`).
2. **Jepsen (`https://jepsen.io/`)** — formal black-box concurrency checkers (Wing-Gong-Lowe linearizability, Elle Adya DSG cycle detection, `set-full`, `bank`, and standard Clojure EDN history export) executed against **real multi-process `metadbd` OS clusters** and **3-voter `Swizzle-Clog` network-partitioned clusters** (`crates/metadb-server/src/jepsen.rs`, `crates/metadb-server/tests/jepsen_tests.rs`, `metadb-ctl jepsen`), plus an **official Clojure Jepsen + 5-node Docker Linux harness** (`tests/jepsen/`).

---

## 1. Deterministic Simulation Testing (`DST`)

### Architecture

Unlike wall-clock chaos tests, `metadb_server::dst` executes an entire multi-node (`N = 3` or `N = 5`), multi-shard MetaDB cluster **inside a single thread** with zero wall-clock reads, zero real sockets, and zero OS sleep timers:

| Component | Implementation | What It Tests |
| :--- | :--- | :--- |
| **Virtual Clock & Event Heap** | `SimWorld` (`BinaryHeap<Reverse<ScheduledEvent>>`) | Strictly ordered `(deliver_at_ms, seq)` execution; years of election/lease timeouts simulate in milliseconds. |
| **Per-Node Skewed Clocks** | `SimNodeClock` -> real `metadb_core::HybridLogicalClock` | Per-node offset, drift ppm, forward/backward step jumps, and `DEFAULT_MAX_CLOCK_DRIFT_MS` (`500 ms`) skew rejection. |
| **Fault-Injecting Virtual Disk** | `SimDisk` | Volatile write-back page cache vs `fsync`-durable frames, CRC32C-verified frames, power-loss cache drops, suffix torn-tail frame recovery, and injected `fsync` I/O rollback. |
| **Fault-Injecting Virtual Network** | `SimNetwork` | Directed mesh with `Swizzle-Clog` asymmetric partitions, majority/minority splits, bridge partitions, jitter/packet reordering, packet duplication, and post-apply reply drops. |
| **Real MetaDB State Machines** | Real `TxStateMachineHandler`, `LifecycleStorageEngine`, `ShardWatermarkTracker`, `ThreadBuggifyGuard`, `CodeProbeSnapshot` | Production `TxRaftCmd` execution (`CreateFile`, `SetAttr`, `Prepare2pcChecked`, `CommitPrimary2pc`, `CommitSecondary2pc`, `Abort2pc`, `CheckAndAbortExpiredTxn`, `ApplyAuxBatch`, `AckAuxTasks`, `FenceShardRangeForSplit`, `UpdateShardRangeOnSplit`, `RestoreShardRange`). |
| **Continuous & Final Oracles** | Raft safety + 6-Invariant Oracle + WGL + Elle | Continuously checks Election Safety, Log Matching, State Machine Safety, and Committed Durability on every event; audits replica convergence, zero leaked `0x05/0x06/0x07/0x0D` keys, `DirStat`/`QuotaTable`/`SecondaryIndexTable`, WGL linearizability, and Elle strict serializability. |

### Scenarios & Commands

Every `(DstScenario, seed)` is **100% bit-for-bit reproducible** (`events_processed`, `final_virtual_time_ms`, `trace_hash`, `state_fingerprint`, `history_digest`, and `buggify_firings` repeat identically across runs):

- `raft-linearizability`: 3-node & 5-node Multi-Raft + `ReadIndex` + stale-leader reads + partitions + packet reorder/duplication + reply drops + power loss + WGL Linearizability.
- `two-pc-crash-skew`: Cross-shard Primary-Anchored 2PC `Rename` + coordinator crashes before/after `CommitPrimary` + lease expiry recovery + HLC clock-skew rejections + Elle Strict Serializability.
- `split-aux-convergence`: Online PID-boundary shard split (`Fence` -> `Restore` rollback + live split to Shard 5) + `0x07` outbox spill + `QuotaTable` / `SecondaryIndexTable` / `DirStat` conservation + `InstallSnapshot`.
- `disk-fault-recovery`: Aggressive `SimDisk` power loss, volatile cache loss, CRC32C torn-tail frame recovery, injected `fsync` error rollback, and snapshot compaction.

```bash
# Run the CI DST integration test suite (170+ multi-node seeds + bit-for-bit replay proof):
cargo test -p metadb-server --test dst_tests --locked -- --nocapture

# Run custom multi-thousand-seed DST campaigns via metadb-ctl:
cargo run --release -p metadb-server --bin metadb-ctl -- \
  dst --scenario all --seed 0x1 --seeds 250 --ops 40 --concurrency 8
```

---

## 2. Jepsen (`jepsen.io`) Testing

### Formal Checkers (`metadb_server::jepsen`)

1. **Wing-Gong-Lowe (WGL / Knossos / Porcupine) Linearizability Checker**: Per-key partitioned exact backtracking DFS with `(DynBitSet, state_hash)` memoization supporting `:invoke`, `:ok`, `:fail`, and indeterminate `:info` operations.
2. **Elle Transactional Dependency-Graph Checker**: Constructs the Adya Direct Serialization Graph (`ww`, `wr`, `rw`, `rt`) and uses Tarjan SCC + shortest-cycle BFS to detect **G0**, **G1a**, **G1b**, **G1c**, **G-single**, **G2-item**, and **Strict-Serializability (`rt`)** violations.
3. **Jepsen `set-full` Checker**: Verifies zero lost acknowledged writes (`lost_ok == 0`) and zero phantom/aborted writes (`unexpected == 0`).
4. **Jepsen `bank` Checker**: Verifies exact byte/balance conservation and non-negativity across concurrent cross-shard moves (`QuotaTable` and `DirStat`).
5. **Jepsen Clojure EDN Export (`history.edn`)**: Writes standard EDN event logs directly loadable by official `jepsen`, `knossos`, and `elle`.

### Live Cluster Harnesses

- **Real 3-Process `metadbd` OS Cluster (`run_os_cluster_jepsen`)**: Spawns three independent `metadbd` OS processes over real TCP/gRPC with Bearer authentication and attacks them with real OS **`SIGKILL` + restart**, **`SIGSTOP` / `SIGCONT` leader-freeze (stale-leader attack)**, online **`TriggerShardSplit`**, and **`TransferShardLeader`** nemeses while concurrent gRPC workers run `register`, `set`, `rename-elle`, and `bank` workloads.
- **Real 3-Voter `Swizzle-Clog` Partition Cluster (`run_partition_jepsen`)**: Attacks a live 3-voter Multi-Raft TCP/gRPC cluster with `SwizzleClogController` asymmetric link clogs, node isolation, jitter, and post-apply reply drops.
- **Official Clojure Jepsen + 5-Node Docker Linux Suite (`tests/jepsen/`)**: Complete Leiningen project (`jepsen 0.3.8`, `elle 0.2.3`, `knossos 0.3.12`) and 5-node Docker Compose cluster (`iptables` / `tc` / `SIGKILL` / `SIGSTOP` / clock skew); see [tests/jepsen/README.md](../../tests/jepsen/README.md).

```bash
# Run the full Rust Jepsen test suite (negative fault-detector proofs + 3-process OS cluster + Swizzle-Clog partitions):
cargo test -p metadb-server --test jepsen_tests --locked -- --nocapture

# Run via metadb-ctl and export history.edn:
cargo run --release -p metadb-server --bin metadb-ctl -- \
  jepsen --workload register --nemesis pause --clients 4 --duration-secs 10 \
  --edn-out /tmp/metadb-jepsen.edn
```
