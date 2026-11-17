# Tests and benches

[Overview](./README.md) · [Français](../fr/testing.md) · [Deutsch](../de/testing.md) · [日本語](../ja/testing.md) · [简体中文](../zh/testing.md)

Run these from the repository root with the pinned toolchain (Rust 1.100.0).

## Checks

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` builds the library, the integration tests, and both binaries. There is no `[[bench]]` target and no `benches/` directory. The load generator that ships in this tree is `metadb-ctl bench`, below.

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

Flags: `--workload` is `hotspot-create` (the default), `bulk-ttl-delete`, or `mixed`. `--concurrency` defaults to 16 when omitted. `--ops` defaults to 1000. `--embedded` or `--workload mixed` starts a temporary all-in-one node and runs the in-process stress harness instead of dialing `--endpoint`.

Behavior from `crates/metadb-server/src/bin/metadb-ctl.rs` for the remote workloads:

- The client allocates one directory id, then file-inode and file-id blocks of length `--ops`, and refuses a grant whose count or end overflows.
- It prints one JSON object with `bench_pid`, `workload`, and `requested_ops`, then creates that many 4096-byte files under a new directory.
- Each phase prints JSON with `attempted`, `succeeded`, `failed`, `transport_failed`, `application_failed`, `errors_by_kind`, and latency percentiles (`p50_ms`, `p99_ms`, `p999_ms`). The process exits non-zero unless `succeeded` equals `--ops`.
- `bulk-ttl-delete` unlinks the names that were created, then lists the directory with `limit = 1` and prints whether that page is empty.
- A second run allocates a new `bench_pid`. It does not reuse a fixed inode, and it does not report rejected creates as successes.

Do not treat a number from this command as a repository result. It depends on the machine, the build profile, and the server you pointed at. This tree does not record a sample.

## Persistent topology and join

These commands are in this tree. CI runs them with `cargo test --workspace --locked`.

- `cargo test -p metadb-server --test topology_single_node` opens `PersistentMetaDbCluster` on a temporary directory.
- `cargo test -p metadb-server --test topology_scale_out` forces a PID-boundary split in that process.
- `cargo test -p metadb-server --test topology_distributed` uses one process and `DurableCellManager::scale_out_replicas` for three localhost voters, including leader loss under create load.
- `cargo test -p metadb-server --test stress_crash_restart` stops without flushing the memtable while a create is inside `propose`, then reopens the same `data_dir`.
- `cargo test -p metadb-server --test join_existing_group` starts a second `metadbd` and joins the first process's Raft group, including a shared-secret case.
- `open_and_serve` still rejects every role except `all-in-one`. A second Cell stays unimplemented. `RegisterCell` returns an error.

`stress_smoke_mixed_workload` runs in that same workspace test. It prints one line for the machine that ran it. This repository does not check in a service-level target. Remote `metadb-ctl bench` records attempt counts and latency percentiles in JSON and fails the process when an application or transport error occurs.
