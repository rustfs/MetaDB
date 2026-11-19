# Simulation testing

[Tests and benches](./testing.md) · [CI and release gates](./ci-and-release-gates.md)

This page is the MetaDB testing standard for a seeded simulation catalog. It records what FoundationDB's published testing design does, what this repository already covers, and the MetaDB-native adaptation that lands with `metadb_server::simulation`.

The design is informed by FoundationDB's public documentation (Apache License 2.0). No FoundationDB source file was copied into this tree. Flow, `fdbserver`, Joshua, TestHarness2, bindingtester, the C API tester, and Circus are not ported.

- [Simulation and Testing](https://apple.github.io/foundationdb/testing.html), FoundationDB 8.0.1
- [Client Testing](https://apple.github.io/foundationdb/client-testing.html) (buggify, and workloads that run either in simulation or on a real cluster)
- [Internal Dev Tools](https://apple.github.io/foundationdb/internal-dev-tools.html) (code probes, TestHarness2, Joshua ensembles)
- [Test catalog](https://github.com/apple/foundationdb/tree/main/tests) (`tests/` descriptors, including `fast/`, `slow/`, `rare/`, `restarting/`, `noSim/`, `negative/`, and `python_tests/`)

Trillion-scale figures in [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1) are Baidu production data. They are not results of this repository and they are not results of this catalog.

## Three execution layers

Every new workload names one of these layers. A result from one layer is evidence for that layer.

| Layer | What the code actually does | What a passing run is |
| :--- | :--- | :--- |
| In-memory / same-process simulation | `MetaDbEngine::new_single_node`. No data directory, no WAL-less LSM, no OpenRaft group. | The scripted directory model matches list, lookup, and `dirstat` for that seed. |
| Single-process persistence | `PersistentMetaDbCluster::open_and_serve` on one data directory. A restart is another `open_and_serve` in the **same OS process** after `shutdown_without_flush` or after `flush_all` plus `shutdown`. `flush_all` runs one maintenance tick, which snapshots and purges a non-empty Raft log, and then flushes memtables. | Acknowledged names, child counts, and byte totals still match after that reopen. |
| Real multi-process / multi-node | More than one OS process or machine, with a real network between them. | Not produced by this catalog. Official three-replica coverage/delete and concurrent `add_learner` stay deferred until the Aliyun 1-to-5 rehearsal. This harness does not start those machines. |

In-process shrink, an in-process 3-voter listener set, and a single-process restart are the second layer or an in-process Raft test. They are not the third layer and they do not replace three-VM evidence.

## What is deterministic here

FoundationDB Simulation runs a whole cluster in one process on the Flow runtime. The simulated clock is shared, the same seed repeats the run, and the simulator can drop connections, stop machines, and fault a datacenter. MetaDB has no Flow runtime, so this catalog does not claim that property.

What this catalog does:

- [`plan_schedule`](../../crates/metadb-server/src/simulation.rs) is a pure function of the workload id and a `u64` seed. The same arguments produce the same steps and the same fault index. The mixer is SplitMix64, written in this file. It is not FoundationDB's simulated random source.
- On the in-memory engine the runner is single-threaded. Two calls with the same seed produce the same directory fingerprint (names, child counts, byte totals) and the same trace hash. Hybrid-logical-clock values still come from the wall clock and are not part of that fingerprint.
- On the persistent engine the same function chooses the script and the fault index. Re-running that seed reissues the same calls and the same restart point. OpenRaft and Tokio timers are wall-clock timers. A matching fingerprint means the acknowledged directory model survived. It does not mean the Raft log bytes were identical.

The report line from `SimReport` carries the workload id, the layer, the seed, the fault label, the violation count, and the trace hash. A failure prints that line. Reproduce it by calling `run_in_memory` or `run_persistent` with the same workload and seed. The CI tests do not read an environment variable, so a developer shell cannot stretch them.

## Comparison catalog

| FoundationDB capability | MetaDB today | Adaptation in this change | Layer |
| :--- | :--- | :--- | :--- |
| Deterministic whole-cluster simulation (`fdbserver -r simulation` on Flow, one process, shared simulated clock) | No Flow runtime. `new_single_node` is an in-memory API simulator. `PersistentMetaDbCluster` is one real process whose Raft and Tokio timers use the wall clock. | Seeded script plus an in-memory replay of the directory model. Persistent runs replay the script and the fault index. A simulated network clock is not claimed. | In-memory for the replay. Persistent restart is its own layer. A whole-cluster network simulation would be a new runtime and is not in this change. |
| Seed printed on failure, same seed repeats the run | `METADB_STRESS_SEED` selects the op mix of the ignored local stress profile. The CI smoke profile is wall-clock bounded and concurrent, so its op trace is not a single-threaded replay. | `SimReport` includes the seed, the fault label, and a trace hash. The in-memory test runs the same seed twice and compares the hash and the fingerprint. | In-memory replay is seed-identical for the directory model. Persistent scripts are rerunnable. Raft timing is not claimed to be seed-identical. |
| Buggify (a client randomly returns errors a production cluster can surface) | No buggify sections. Auth and input tests are fixed cases. | Not ported. The in-memory workload omits one script step before the call is made. The model does not include that step. That is a client that never sent the request. | In-memory. It is not a server fault and not a network partition. |
| Network, machine, and datacenter faults, including the swizzle-clog pattern (clog a random subset of connections, then unclog them in a random order) | No simulated network. In-process tests can stop a voter inside one OS process. `shutdown_without_flush` is a single-process crash. `join_existing_group` starts a second `metadbd`. | First catalog: unflushed restart, and flush-plus-purge then restart, both on one data directory. Swizzle-clog, disk-full, and datacenter loss are listed in `deferred_capabilities` and are not run. | Restart is single-process persistence. A real partition is multi-process. |
| Workload catalog under `tests/` (`fast`, `slow`, `rare`, `restarting`, `noSim`, `negative`, `python_tests`, and other top-level descriptors) | Rust integration tests with fixed scenarios. `#[ignore]` holds `official_prefix_load` and `stress_local_mixed_workload`. There is no descriptor tree. | Three workloads in `ci_workloads()`: `metadata_crud`, `metadata_crud_unflushed_restart`, `metadata_crud_flush_purge_restart`. The directory tree was not copied. | Each workload's `ExecutionLayer` field. |
| Cycle-style invariant workloads (a check the script can evaluate after the run) | The stress harness compares listings, `dirstat`, and quota with its op log. T1–T6 check named properties. Split, LSM, and 2PC tests inject their own crash points. | This catalog's invariant is the directory model: root and each script directory match on listing names, `dirstat` child count, and `dirstat` byte total. Lookup of a present name checks its size. Lookup of `missing.dat` expects absence. Quota and secondary-index checks stay in the existing stress and topology tests. | In-memory and single-process persistence. |
| TestHarness2 and Joshua (`python3 -m test_harness.app -s $JOSHUA_SEED`, ensemble statistics, JSON or XML reports, code-probe coverage across a nightly ensemble) | `cargo test --workspace --locked` is the CI runner. There is no ensemble orchestrator and no code-probe coverage gate. | Not ported. `SimReport`'s one-line display is the report. The existing workspace test step runs the new tests. No workflow file is added or edited. | Tooling. A green `cargo test` remains the current gate, and that gate is not a first-ship pass. |
| CTest entries in both native and simulation mode, `UNIT_TEST_SEED` | `cargo test` runs each suite once, on the normal runtime. | The in-memory workload and the persistent workloads are separate tests, not one binary with a `--simulation` flag. | Both of the layers this catalog runs. |
| bindingtester and the C API tester | The Rust engine and the gRPC services have in-process tests. There is no multi-language binding tester. | Not in this change. | A future external client would be real multi-process. |
| Circus performance runs (a described data set, a workload mix, a comparison across days) | `metadb-ctl bench` and `prefix_load` exist. CI runs a bounded prefix fixture. The official profile is 100 prefixes × 10,000 creates plus 100,000 overwrite/delete operations at concurrency 64, on three VMs, and it is ignored under `GITHUB_ACTIONS`. This tree does not check in a sample line from that command. | Not ported. This catalog does not record throughput and does not treat latency as a pass/fail gate. | The official load is real multi-process (three VMs). The CI fixture is a smaller in-process run and is not that load. |
| Restarting tests, including runs that need the previous binaries | `stress_crash_restart`, split crash points, LSM and manifest recovery, and topology reopen already exist. They are fixed scenarios, not a seed catalog. | The catalog adds one seeded unflushed restart and one seeded flush-plus-purge restart. Old-binary upgrade pairs are not generated. | Single-process persistence. |
| `noSim` (cases the simulator cannot host) | Behaviors this tree cannot treat as simulated: a second Cell (`RegisterCell` returns an error), any role other than `all-in-one`, three-replica coverage/delete, and concurrent `add_learner`. | Those ids live in `deferred_capabilities()` with layer `real-multi-process`. A unit test asserts they are absent from `ci_workloads()`. | Real multi-process. |
| Negative tests (a run that is expected to fail) | Fixed tests already expect specific errors (reserved names, auth rejection, torn Raft tails). | No new test is expected to fail. Known deferred gaps, including multi-voter PID-split quorum and the official 1-to-5 rehearsal, are not encoded as must-fail cases. | Not applicable. A gap stays listed as deferred. |

## Automated testing standard

This is the standard for simulation-style tests added to this tree. It does not promote the current CI gate to a production gate or a first-ship gate. [CI and release gates](./ci-and-release-gates.md) still describes the gates that exist today. A green workspace test, a green audit job, and a green run of this catalog are each a report of what that command checked.

1. Name the execution layer in the workload spec and in the test comment. Use the three names in the table above.
2. A simulation workload's script and fault index are a pure function of the workload id and the seed. `plan_schedule` must not open an engine.
3. The in-memory layer may claim that the directory fingerprint repeats for a seed. The persistent layer may claim that the same script and the same fault index were used. It may not claim a simulated clock or a bit-identical Raft trace.
4. The pass condition is the invariant. A mismatch in listing, lookup, or `dirstat` fails the test. The checker itself has a unit test that a wrong child count produces a violation, so an empty checker does not pass silently.
5. A client omission is recorded in the model as a call that was not sent. It is not labeled a network partition. A restart is labeled with the shutdown it actually performed (`shutdown_without_flush`, or `flush_all` then `shutdown`).
6. Do not add a passing test, or a test that is expected to fail, for a gap that is still deferred. Multi-voter PID-split quorum, official 1-to-5 coverage/delete, and concurrent `add_learner` stay in the deferred list until a real multi-process run exists. This standard does not start the Aliyun machines and does not assume they are reachable.
7. Do not describe the result as a first-ship pass, as the official million-operation load, as a 3-VM rehearsal, or as a figure from arXiv:2610.32247v1.
8. CI picks the catalog up because `cargo test --workspace --locked` compiles `crates/metadb-server/tests/simulation_catalog.rs`. Do not add a workflow, retarget a workflow, or re-enable a disabled workflow for this catalog.
9. Keep the script inside a small step budget so the workspace test stays a normal CI run. Longer local stress remains the ignored `stress_local_mixed_workload` profile and is not part of this catalog.
10. A fault that the product cannot represent in-process stays in `deferred_capabilities()` with layer `real-multi-process` and a reason a reviewer can check.

## Workloads that run

`cargo test -p metadb-server --test simulation_catalog`

| Workload | Seed in the CI test | Fault | Invariant |
| :--- | :--- | :--- | :--- |
| `metadata_crud` | 1, 7, 99, and `0xC0FFEE`. Seed 1 is also run twice and the trace hashes are compared. The report prints every seed in hexadecimal (`0x1`, `0x7`, `0x63`, `0xc0ffee`). | One client omission. The step index comes from the seed. | Directory model on `new_single_node`. |
| `metadata_crud_unflushed_restart` | 11 | `shutdown_without_flush`, then reopen, after a seeded count of mutation-kind steps. | Directory model on one data directory. |
| `metadata_crud_flush_purge_restart` | 13 | `flush_all`, then `shutdown`, then reopen, after a seeded count of mutation-kind steps. | Directory model on one data directory. |

The planner keeps at least four mutation-kind steps (create, unlink, or rename) so a restart has work on both sides of the fault. Unlink or rename on an empty directory becomes a `dirstat` and does not change the model. The restart clock still counts the scripted step, so the fault index does not depend on how many files happened to exist.

Library tests in `simulation::tests` check that the schedule is pure, that the omission and restart indexes land inside the script, that the deferred ids are absent from the CI catalog, and that the in-memory runner refuses a persistent workload instead of pretending to run it.

## Deferred, and not started here

`deferred_capabilities()` in `crates/metadb-server/src/simulation.rs` lists:

- network partition
- machine or datacenter fault (the FoundationDB swizzle-clog pattern is named there as their pattern, and it is not implemented)
- three-replica coverage/delete
- concurrent `add_learner`
- the official million-operation load

Those entries exist so a reviewer can see the boundary. They are not tests, they do not fail the suite, and they do not pass the suite.
