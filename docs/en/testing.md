# Tests and benches

[Overview](./README.md) · [Français](../fr/testing.md) · [Deutsch](../de/testing.md) · [日本語](../ja/testing.md) · [简体中文](../zh/testing.md)

Run these from the repository root with the pinned toolchain (Rust 1.100.0).

## Checks

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` builds the library, the integration tests, and both binaries. There is no `[[bench]]` target and no `benches/` directory. The load generators that ship in this tree are `metadb-ctl bench` and `cargo test -p metadb-server --test stress_harness`, below.

Useful narrower commands:

```bash
cargo test -p metadb-server --test integration_tests
cargo test -p metadb-server --test e2e_cluster_evolution_tests
cargo test -p metadb-raft --test multi_raft_failover_tests
cargo test -p metadb-raft --test single_voter_recovery_tests
cargo test -p metadb-cluster --test epoch_split_tests
cargo test -p metadb-cluster --test dual_mode_jumbo_tests
cargo test -p metadb-cluster --test split_crash_recovery_tests
cargo test -p metadb-tx --test two_pc_crash_tests
cargo test -p metadb-tx --test aux_idempotency_tests
cargo test -p metadb-tx --test attr_reconcile_race_tests
cargo test -p metadb-proxy --test sql_templates_tests
cargo test -p metadb-storage --test lsm_recovery_tests
cargo test -p metadb-storage --test mvcc_snapshot_tests
cargo test -p metadb-storage --test locality_compaction_tests
cargo test -p metadb-server --test auth_fail_closed
cargo test -p metadb-server --test prefix_load
cargo test -p metadb-server --test stress_harness
```

## What each suite actually exercises

### In-memory simulator (`MetaDbEngine::new_single_node`)

`crates/metadb-server/tests/integration_tests.rs` tests `T1`–`T6` do not open a data directory.

| Test | What it checks |
| :--- | :--- |
| `test_t1_pid_boundary_sharding_preserves_directory_colocation` | Parent-id split keeps a directory's children together |
| `test_t2_jumbo_shard_isolation_and_dual_mode_fallback` | In-memory jumbo error and dual-mode fallback |
| `test_t3_conflict_free_async_attr_aggregation_and_read_reconciliation` | Attribute deltas visible to dirstat before flush |
| `test_t4_decoupled_async_auxiliary_idempotency_and_cross_shard_rename` | Quota/index queues and cross-shard rename |
| `test_t5_in_memory_mvcc_and_locality_aware_compaction_on_bulk_deletes` | MVCC and locality compaction on the in-memory backend |
| `test_t6_single_node_to_distributed_multi_cell_evolution_and_sql_proxy` | Reserved name, SQL, non-empty `rmdir`, then `register_node` + `scale_out_raft_replicas(3)` on the **in-memory** membership map |

`T6` does not start Raft and does not start a second cell process. The name describes the simulator's membership object.

### Seeded simulation catalog

`cargo test -p metadb-server --test simulation_catalog` runs the seeded catalog. The comparison with FoundationDB's testing design, and the standard for that catalog, are in [Simulation testing](./simulation-testing.md). It names three layers and runs only the first two: in-memory `MetaDbEngine::new_single_node`, and a single-process `PersistentMetaDbCluster` restart (unflushed, or flush plus purge, then reopen in the same OS process). Real multi-process workloads are not in the catalog. Official three-replica coverage/delete and concurrent `add_learner` stay deferred until the Aliyun 1-to-5 rehearsal. A passing run is the scripted directory invariant for the seeds in the test. It is not a first-ship pass, not the official million-operation load, and not three-VM evidence. Trillion-scale figures in arXiv:2610.32247v1 are Baidu production data, not results of this run.

### Persistent single process

| Test | What it checks |
| :--- | :--- |
| `test_persistent_restart_lookup_dirstat_quota` | `open_and_serve` on a temp directory, shutdown, reopen, lookup / dirstat / quota |
| `test_maintenance_tick_purges_raft_log` | After 24 creates, one maintenance tick drops retained Raft entries |
| `test_e2e_persistent_grpc_cluster_restart_split_2pc_and_locality_compaction` | One persistent process, real gRPC, restart, forced split, rename 2PC, bulk delete, quota, secondary index, Prometheus text |

The end-to-end test sets `shard_split_threshold_bytes = 2048` and `locality_tombstone_threshold = 20` so a small fixture crosses those gates. Those values are test configuration, not deployment defaults.

### Raft, still one OS process

| Test | What it checks |
| :--- | :--- |
| `test_single_voter_walless_crash_recovery_and_three_watermark_pinning` | One voter, WAL-less recovery, purge watermarks |
| `test_suffix_only_truncate_final_crc_rejection_and_snapshot_verification` | Torn tail and checksum rejection |
| `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` | Three in-process tonic listeners, basic KV apply handler, `AddLearner` to 3 voters, stop the original leader, read after failover |

The failover test binds `127.0.0.1:0` three times inside the test process. It is not three operating-system processes and it does not serve `MetadataService`.

### Cluster, transactions, SQL, storage

| Test | What it checks |
| :--- | :--- |
| `test_epoch_fenced_live_pid_split_and_durable_id_allocator` | Epoch-fenced PID split and the durable id allocator |
| `test_jumbo_isolation_and_three_way_dual_mode_scatter_gather_and_rmdir` | Durable jumbo flag, dual-mode scatter-gather, rmdir |
| `split_crash_recovery_tests` | Persist failure and crash points during split, then reopen |
| `test_two_pc_crash_recovery_posix_overwrite_and_cycle_fencing` | 2PC recovery, overwrite rename, cycle rejection |
| `test_aux_outbox_spill_contiguous_frontiers_gap_rejection_and_cleanup` | `0x07` spill, gaps, cleanup |
| `test_concurrent_attr_aggregation_read_reconciliation_and_l0_flush_barriers` | Concurrent attribute aggregation |
| `test_all_twenty_metadata_sql_templates_and_attr_isolation` | All 20 templates, literals and `?`, `_attr_` isolation |
| LSM / MVCC / locality tests | SSTable and `MANIFEST` recovery, `SnapshotGuard`, bottommost compaction |

Unit tests also live in `metadb-core` (`hlc`, `schema`) and `metadb-proto` (`convert`).

## `metadb-ctl bench`

Start `metadbd` first. Then:

```bash
cargo run -p metadb-server --bin metadb-ctl -- \
  --endpoint 127.0.0.1:7800 \
  bench --workload hotspot-create --concurrency 16 --ops 1000

cargo run -p metadb-server --bin metadb-ctl -- \
  --endpoint 127.0.0.1:7800 \
  bench --workload bulk-ttl-delete --concurrency 16 --ops 1000
```

Flags: `--workload` is `hotspot-create` (the default), `bulk-ttl-delete`, or `mixed`. For the remote workloads, omitted `--concurrency` is 16 and `--ops` defaults to 1000. `--profile` is read only when the command takes the embedded path. `--embedded` or `--workload mixed` does not dial `--endpoint` and does not use `--ops`. It starts a temporary all-in-one node and runs the in-process stress harness. See [In-process stress harness](#in-process-stress-harness).

Behavior from `crates/metadb-server/src/bin/metadb-ctl.rs` for the remote workloads:

- The client allocates one directory id, then file-inode and file-id blocks of length `--ops`, and refuses a grant whose count or end overflows.
- It prints one JSON object with `bench_pid`, `workload`, and `requested_ops`, then creates that many 4096-byte files under a new directory.
- Each phase prints JSON with `attempted`, `succeeded`, `failed`, `transport_failed`, `application_failed`, `errors_by_kind`, and latency percentiles (`p50_ms`, `p99_ms`, `p999_ms`). The process exits non-zero unless `succeeded` equals `--ops`.
- `bulk-ttl-delete` unlinks the names that were created, then lists the directory with `limit = 1` and prints whether that page is empty.
- A second run allocates a new `bench_pid`. It does not reuse a fixed inode, and it does not report rejected creates as successes.

Do not treat a number from this command as a repository result. A remote run depends on the machine, the build profile, and the server you pointed at. An embedded run depends on the machine and the build profile. This tree does not record a sample of either.

## In-process stress harness

`crates/metadb-server/tests/stress_harness.rs` and `metadb-ctl bench --embedded` both call `metadb_server::stress` (`crates/metadb-server/src/stress.rs`). There is no third profile named `mixed`. `--workload mixed` selects this harness. Both profiles run the same mixed workload: create, lookup, dirstat, listdir, rename, and unlink against one persistent all-in-one node on a temporary data directory. After the timed loop the harness runs maintenance ticks, then checks directory listings, dirstat, and quota against the operations that succeeded. A failed maintenance tick adds one to `errors`, sets `correctness_errors` to 1, and skips that check. The run is one OS process and one data directory. It is not three `metadbd` processes, and it is not the [official prefix load](#prefix-load).

| Profile | Test | Duration | Concurrency | Directories | When it runs |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `smoke` | `stress_smoke_mixed_workload` | 5 seconds | 4 | 4 | `cargo test -p metadb-server --test stress_harness`, including `cargo test --workspace`. Ignores `METADB_STRESS_*`. |
| `local` | `stress_local_mixed_workload` | 30 seconds unless overridden | 8 unless overridden | 16 unless overridden | `#[ignore]`. Not part of `cargo test --workspace`. Reads `METADB_STRESS_DURATION_SECS`, `METADB_STRESS_CONCURRENCY`, `METADB_STRESS_KEYSPACE`, and `METADB_STRESS_SEED`. |

```bash
cargo test -p metadb-server --test stress_harness stress_smoke -- --nocapture

METADB_STRESS_DURATION_SECS=120 \
METADB_STRESS_CONCURRENCY=16 \
METADB_STRESS_KEYSPACE=32 \
METADB_STRESS_SEED=7 \
  cargo test -p metadb-server --test stress_harness stress_local -- --ignored --nocapture

cargo run -p metadb-server --bin metadb-ctl -- bench --embedded --profile smoke
cargo run -p metadb-server --bin metadb-ctl -- bench --embedded --profile local
```

`METADB_STRESS_*` applies to the local profile, including `metadb-ctl bench --embedded --profile local`. On `metadb-ctl bench --embedded`, `--duration-secs`, `--concurrency`, `--keyspace`, and `--seed` override whichever profile was selected, after the environment is read. The `cargo test` local profile has no flag overrides; it reads only those variables. A duration, concurrency, or directory count below 1 is raised to 1. `--profile` defaults to `smoke`. `--profile ci` is the same smoke profile. Any other `--profile` value exits with an error. The smoke profile does not read `METADB_STRESS_*`. Remote `hotspot-create` and `bulk-ttl-delete` ignore `--profile`.

The test prints one line and requires `errors = 0`, `correctness_errors = 0`, and `ops > 0`. `metadb-ctl bench --embedded` prints `embedded metadbd listening on <addr>` and the same line, and exits non-zero when `errors` or `correctness_errors` is greater than zero. The line fields are:

```text
metadb stress profile=<name> ops=<count> errors=<count> correctness_errors=<count> elapsed_ms=<ms> ops_per_s=<rate> latency_us p50=<us> p99=<us> max=<us>
```

`ops` counts only mixed-loop operations that succeeded. `ops_per_s` divides that count by the elapsed time. `errors` counts mixed-loop operations that did not succeed. A failed maintenance tick is also added to `errors`, and it sets `correctness_errors` to 1 instead of running the check. When the check runs, `correctness_errors` counts mismatched directory listings, dirstat, and quota, and it also counts failed verification calls (`dirstat`, `listdir`, and `get_quota`). `latency_us` `p50`, `p99`, and `max` are percentiles of every mixed-loop attempt, including failed attempts, in microseconds. They are not percentiles of `ops`. They are printed. This tree does not enforce a latency threshold.

A line from either command is a result for the machine that printed it. This repository does not record that line. It is not a service-level result, not a first-ship pass, and not the official 1,000,000-create prefix load.

## Fail-closed auth leftovers

`cargo test -p metadb-server --test auth_fail_closed` covers the listener checks from the fail-closed startup work. `persisted_non_loopback_advertise_without_secret_fails_closed` and `default_empty_secret_is_rejected_before_any_listener` are in `security_adversarial_regressions.rs`.

| Test | What it locks |
| :--- | :--- |
| `wildcard_without_a_secret_does_not_listen` | Wildcard gRPC or metrics bind with an empty or whitespace secret does not listen |
| `persisted_non_loopback_advertise_without_secret_fails_closed` | After a `0.0.0.0` process with a secret shuts down, restarting that directory with an empty secret and `allow_unauthenticated = true` on `127.0.0.1:0` still refuses the saved non-loopback address |
| `default_empty_secret_is_rejected_before_any_listener` | Default empty secret on `127.0.0.1` fails with the implicit-exception error and does not mention a recovered listener |
| `misspelled_secret_and_tls_options_do_not_start` | A misspelled `shared_secret`, or a TLS key, fails parsing |
| `configured_secret_rejects_missing_and_wrong_credentials` | Missing, empty, and wrong bearers are rejected on cluster status, lookup, Raft, and metrics |
| `empty_secret_on_loopback_does_not_listen` | `127.0.0.1` with an empty secret and `allow_unauthenticated = false` does not listen |
| `explicit_unauthenticated_loopback_serves_without_credentials` | The same loopback bind with `allow_unauthenticated = true` serves `GetRoutes`, `CreateFile`, `TransactionService.Rename`, and `GET /metrics` with no `Authorization` header. A `Basic` header is not consulted. This is the explicit local-development opt-in |
| `configured_secret_rejects_basic_auth_without_applying_the_write` | On loopback, with a secret set, a missing header, `Authorization: Basic ...`, a lowercase `bearer` scheme, the raw secret, and a wrong bearer return `Unauthenticated`. The rejected `CreateFile` is not visible to an authenticated lookup. Metrics with `Basic` returns HTTP 401 and no metric text |
| `whitespace_secret_on_loopback_does_not_listen` | A whitespace-only secret does not start, including when `allow_unauthenticated` is set |
| `unknown_toml_keys_are_ignored_and_loopback_still_serves` | `grpc_listen_addrr`, `memtable_size_bytez`, and `joinn` are ignored. The process starts and serves only because that fixture sets `allow_unauthenticated = true` with an empty secret and then binds `127.0.0.1`. Those unknown keys are not what opens the listener. `token_file` and `tls_mode` still fail parsing |

An empty secret on loopback no longer serves. The opt-in above is the only unauthenticated path, and it accepts only literal loopback IPs, not the hostname `localhost`. Unknown keys that do not name a secret, authentication, or TLS setting are still ignored: a typo in a storage or join field does not fail startup. This suite does not add TLS, mutual TLS, RBAC, a second metrics credential, or an audit log. Listeners stay plaintext.

## Prefix load

`crates/metadb-server/tests/prefix_load.rs` is a client harness. It allocates directory, file-inode, and file-id blocks, creates one directory per prefix, then runs three phases: create, `SetAttr` size overwrite, and delete. It fails if any RPC fails or if the sample lookups disagree with those phases. It does not change memtable, cache, or Raft settings. A line it prints is a result for that process and those endpoints. This repository does not check that line in, and it is not a figure from arXiv:2610.32247v1.

| Profile | Creates | SetAttr | Deletes | Concurrency | When it runs |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `ci` | 2 prefixes × 8 | 8 | 8 | 4 | `cargo test -p metadb-server --test prefix_load`, including GitHub Actions |
| `official` | 100 prefixes × 10,000 (1,000,000) | 100,000 | 100,000 | 64 | Ignored. Refused when `GITHUB_ACTIONS=true` |

The CI test starts one in-process node on `127.0.0.1:0` and ignores `METADB_LOAD_*`. Do not point the CI job at the official counts.

### Official load on three VMs

Run `metadbd` on three virtual machines in one cell. Bind each process to an address the other machines can dial. `0.0.0.0` is stored as the advertise address and is not a usable peer address. A non-loopback bind requires the same non-empty `[security].shared_secret` on every node. Use a fresh data directory on the joining nodes. `--join` must reach the current leader. A follower does not forward that membership change.

```toml
# /etc/metadb/node1.toml
[node]
node_id = 1
cell_id = 1
role = "all-in-one"
data_dir = "/var/lib/metadb"
grpc_listen_addr = "10.0.0.11:7800"

[security]
shared_secret = "replace-with-a-long-random-string"
```

```bash
# VM 1
metadbd --config /etc/metadb/node1.toml

# VM 2, after VM 1 is serving. Same cell_id and shared_secret. Fresh data_dir.
metadbd --config /etc/metadb/node2.toml --node-id 2 --listen 10.0.0.12:7800 --join 10.0.0.11:7800

# VM 3, still joining the leader (VM 1, unless leadership has moved).
metadbd --config /etc/metadb/node3.toml --node-id 3 --listen 10.0.0.13:7800 --join 10.0.0.11:7800
```

`node2.toml` and `node3.toml` are the same shape as node 1 with their own `node_id`, `data_dir`, and `grpc_listen_addr`. Confirm three voters before the load:

```bash
metadb-ctl --endpoint 10.0.0.11:7800 --auth-token "$SECRET" cluster status
```

From a machine that can reach all three listeners, unset any `METADB_LOAD_PREFIXES`, `METADB_LOAD_CREATES_PER_PREFIX`, `METADB_LOAD_SETATTR`, `METADB_LOAD_DELETES`, or `METADB_LOAD_CONCURRENCY` left over from a dry run. A variable that is set to an empty string or a non-integer fails the run before any endpoint is dialed. The first endpoint grants the id blocks. Each create, `SetAttr`, delete, and prefix `mkdir` starts on `index % endpoint_count` and rotates to the next endpoint after `NotLeader` or an unavailable response. An application error fails the run. Prefix directory names are `loadp<allocated-dir-id>`, so a dry run can leave its directories in place and a later run still creates new ones.

```bash
METADB_LOAD_ENDPOINTS=http://10.0.0.11:7800,http://10.0.0.12:7800,http://10.0.0.13:7800 \
METADB_LOAD_TOKEN="$SECRET" \
cargo test -p metadb-server --test prefix_load -- --ignored --nocapture --test-threads=1 official_prefix_load
```

A dry run against the same voters uses the override variables and prints `profile=official-override`. It is not the official load:

```bash
METADB_LOAD_ENDPOINTS=http://10.0.0.11:7800,http://10.0.0.12:7800,http://10.0.0.13:7800 \
METADB_LOAD_TOKEN="$SECRET" \
METADB_LOAD_PREFIXES=2 \
METADB_LOAD_CREATES_PER_PREFIX=10 \
METADB_LOAD_SETATTR=5 \
METADB_LOAD_DELETES=5 \
METADB_LOAD_CONCURRENCY=2 \
cargo test -p metadb-server --test prefix_load -- --ignored --nocapture --test-threads=1 official_prefix_load
```

The harness does not start the three servers. Joining two voters is the procedure above. It is not a second cell, and `metadb-ctl cell register` still does not create one.

## Persistent topology and join

These commands are in this tree. CI runs them with `cargo test --workspace --locked`.

- `cargo test -p metadb-server --test topology_single_node` opens `PersistentMetaDbCluster` on a temporary directory.
- `cargo test -p metadb-server --test topology_scale_out` forces a PID-boundary split in that process.
- `cargo test -p metadb-server --test topology_distributed` uses one process and `DurableCellManager::scale_out_replicas` for three localhost voters, including leader loss under create load.
- `cargo test -p metadb-server --test stress_crash_restart` stops without flushing the memtable while a create is inside `propose`, then reopens the same `data_dir`.
- `cargo test -p metadb-server --test join_existing_group` covers two different joins. `metadbd_processes_join_existing_group_and_fail_over` starts real `metadbd` processes, joins the first process's Raft group, and fails over. `join_sends_the_shared_secret` opens two `PersistentMetaDbCluster` servers inside the test process and checks that the shared secret is sent on that join. That case does not start two `metadbd` OS processes.
- `cargo test -p metadb-server --test placement_status_tests` checks `metadb-ctl` placement lines and the unlabeled `metadb_placement_*` gauges on one process. It does not move jumbo files and does not boot three voters. `split_executed` stays false in that test.
- `open_and_serve` starts `all-in-one`, `global-manager`, `cell-manager`, `be`, and `proxy` when that role's config is valid. `be` hosts local Raft replicas and does not mint ids. `proxy` requires `[cluster] upstream` and forwards to that one shard host. An invalid combination fails before listen. A second cell is registered only when its routes cover the requested range. A later request contained by that cell is forwarded. The directory file is still process-local.

The smoke profile and the ignored local profile are the [in-process stress harness](#in-process-stress-harness). `topology_distributed` is still one OS process with three localhost Raft voters, not three `metadbd` processes. `be` and `proxy` start on a valid config. The proxy is not a multi-cell router, and a second cell is still not a live multi-cell deployment.
