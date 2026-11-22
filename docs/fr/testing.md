# Tests et bancs

[Présentation](./README.md) · [English](../en/testing.md) · [Deutsch](../de/testing.md) · [日本語](../ja/testing.md) · [简体中文](../zh/testing.md)

Les exécuter depuis la racine du dépôt avec la chaîne épinglée (Rust 1.100.0).

## Contrôles

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` builds the library, the integration tests, and both binaries. There is no `[[bench]]` target and no `benches/` directory. The load generators that ship in this tree are `metadb-ctl bench` and `cargo test -p metadb-server --test stress_harness`, below.

Commandes plus étroites utiles :

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
cargo test -p metadb-server --test stress_harness
```

## Ce que chaque suite exerce réellement

### Simulateur en mémoire (`MetaDbEngine::new_single_node`)

Les tests `T1`–`T6` de `crates/metadb-server/tests/integration_tests.rs` n'ouvrent pas de répertoire de données.

| Test | Ce qu'il vérifie |
| :--- | :--- |
| `test_t1_pid_boundary_sharding_preserves_directory_colocation` | Un découpage par identifiant de parent garde ensemble les enfants d'un répertoire |
| `test_t2_jumbo_shard_isolation_and_dual_mode_fallback` | Erreur jumbo en mémoire et repli en mode double |
| `test_t3_conflict_free_async_attr_aggregation_and_read_reconciliation` | Deltas d'attributs visibles pour dirstat avant le vidage |
| `test_t4_decoupled_async_auxiliary_idempotency_and_cross_shard_rename` | Files quota/index et renommage inter-shards |
| `test_t5_in_memory_mvcc_and_locality_aware_compaction_on_bulk_deletes` | MVCC et compaction par localité sur le backend en mémoire |
| `test_t6_single_node_to_distributed_multi_cell_evolution_and_sql_proxy` | Nom réservé, SQL, `rmdir` non vide, puis `register_node` + `scale_out_raft_replicas(3)` sur la table d'appartenance **en mémoire** |

`T6` ne démarre pas Raft et ne démarre pas un second processus de cellule. Le nom décrit l'objet d'appartenance du simulateur.

### Processus persistant unique

| Test | Ce qu'il vérifie |
| :--- | :--- |
| `test_persistent_restart_lookup_dirstat_quota` | `open_and_serve` sur un répertoire temporaire, arrêt, réouverture, lookup / dirstat / quota |
| `test_maintenance_tick_purges_raft_log` | Après 24 créations, un tic de maintenance abandonne des entrées Raft conservées |
| `test_e2e_persistent_grpc_cluster_restart_split_2pc_and_locality_compaction` | Un processus persistant, gRPC réel, redémarrage, découpage forcé, 2PC de renommage, suppression en masse, quota, index secondaire, texte Prometheus |

Le test de bout en bout fixe `shard_split_threshold_bytes = 2048` et `locality_tombstone_threshold = 20` pour qu'un petit jeu de données franchisse ces seuils. Ces valeurs sont une configuration de test, pas les défauts de déploiement.

### Raft, toujours un seul processus du système

| Test | Ce qu'il vérifie |
| :--- | :--- |
| `test_single_voter_walless_crash_recovery_and_three_watermark_pinning` | Un votant, reprise sans WAL, filigranes de purge |
| `test_suffix_only_truncate_final_crc_rejection_and_snapshot_verification` | Queue tronquée et rejet de somme de contrôle |
| `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` | Trois écouteurs tonic dans le processus, gestionnaire d'application KV simple, `AddLearner` jusqu'à 3 votants, arrêt du leader d'origine, lecture après bascule |

Le test de bascule lie `127.0.0.1:0` trois fois dans le processus de test. Ce ne sont pas trois processus du système d'exploitation et il ne sert pas `MetadataService`.

### Cluster, transactions, SQL, stockage

| Test | Ce qu'il vérifie |
| :--- | :--- |
| `test_epoch_fenced_live_pid_split_and_durable_id_allocator` | Découpage PID clôturé par époque et allocateur d'identifiants durable |
| `test_jumbo_isolation_and_three_way_dual_mode_scatter_gather_and_rmdir` | Indicateur jumbo durable, scatter-gather en mode double, rmdir |
| `split_crash_recovery_tests` | Échec de persistance et points de plantage pendant le découpage, puis réouverture |
| `test_two_pc_crash_recovery_posix_overwrite_and_cycle_fencing` | Reprise 2PC, renommage avec écrasement, rejet de cycle |
| `test_aux_outbox_spill_contiguous_frontiers_gap_rejection_and_cleanup` | Déversement `0x07`, trous, nettoyage |
| `test_concurrent_attr_aggregation_read_reconciliation_and_l0_flush_barriers` | Agrégation d'attributs concurrente |
| `test_all_twenty_metadata_sql_templates_and_attr_isolation` | Les 20 modèles, littéraux et `?`, isolation de `_attr_` |
| Tests LSM / MVCC / localité | Reprise SSTable et `MANIFEST`, `SnapshotGuard`, compaction la plus profonde |

Des tests unitaires vivent aussi dans `metadb-core` (`hlc`, `schema`) et `metadb-proto` (`convert`).

## `metadb-ctl bench`

Démarrer `metadbd` d'abord. Puis :

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

`crates/metadb-server/tests/stress_harness.rs` and `metadb-ctl bench --embedded` both call `metadb_server::stress` (`crates/metadb-server/src/stress.rs`). There is no third profile named `mixed`. `--workload mixed` selects this harness. Both profiles run the same mixed workload: create, lookup, dirstat, listdir, rename, and unlink against one persistent all-in-one node on a temporary data directory. After the timed loop the harness runs maintenance ticks, then checks directory listings, dirstat, and quota against the operations that succeeded. A failed maintenance tick adds one to `errors`, sets `correctness_errors` to 1, and skips that check. The run is one OS process and one data directory. It is not three `metadbd` processes, and it is not the [official prefix load](../en/testing.md#prefix-load).

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

## Persistent topology and join

These commands are in this tree. CI runs them with `cargo test --workspace --locked`.

- `cargo test -p metadb-server --test topology_single_node` opens `PersistentMetaDbCluster` on a temporary directory.
- `cargo test -p metadb-server --test topology_scale_out` forces a PID-boundary split in that process.
- `cargo test -p metadb-server --test topology_distributed` uses one process and `DurableCellManager::scale_out_replicas` for three localhost voters, including leader loss under create load.
- `cargo test -p metadb-server --test stress_crash_restart` stops without flushing the memtable while a create is inside `propose`, then reopens the same `data_dir`.
- `cargo test -p metadb-server --test join_existing_group` covers two different joins. `metadbd_processes_join_existing_group_and_fail_over` starts real `metadbd` processes, joins the first process's Raft group, and fails over. `join_sends_the_shared_secret` opens two `PersistentMetaDbCluster` servers inside the test process and checks that the shared secret is sent on that join. That case does not start two `metadbd` OS processes.
- `open_and_serve` starts `all-in-one`, `be`, `proxy`, `cell-manager`, and `global-manager` when that role's configuration is valid. An illegal combination fails before listen. An unknown `--role` returns `unsupported --role` from `metadbd`, not an unimplemented-role `CodecError`. A second cell is registered only when its routes cover the requested range. A later request contained by that cell is forwarded. Join does not create that second cell. This is not a production MetaCache of every shard and not a disaggregated production deployment.

The smoke profile and the ignored local profile are the [in-process stress harness](#in-process-stress-harness). `topology_distributed` is still one OS process with three localhost Raft voters, not three `metadbd` processes. `be` and `proxy` start on a valid config. A second cell recorded by `RegisterCell` is not a live multi-cell production deployment and not a production MetaCache of every shard.
