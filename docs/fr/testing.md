# Tests et bancs

[Présentation](./README.md) · [English](../en/testing.md) · [Deutsch](../de/testing.md) · [日本語](../ja/testing.md) · [简体中文](../zh/testing.md)

Les exécuter depuis la racine du dépôt avec la chaîne épinglée (Rust 1.100.0).

## Contrôles

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` construit la bibliothèque, les tests d'intégration et les deux binaires. Il n'y a pas de cible `[[bench]]` ni de répertoire `benches/`. Les générateurs de charge livrés dans cet arbre sont `metadb-ctl bench` et `cargo test -p metadb-server --test stress_harness`, ci-dessous.

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

Options : `--workload` vaut `hotspot-create` (le défaut), `bulk-ttl-delete` ou `mixed`. Pour les charges distantes, `--concurrency` vaut 16 s'il est omis et `--ops` vaut 1000 par défaut. `--profile` n'est lu que sur le chemin embarqué. `--embedded` ou `--workload mixed` n'appelle pas `--endpoint` et n'utilise pas `--ops`. La commande démarre un nœud all-in-one temporaire et exécute le harnais de stress dans le processus. Voir [Harnais de stress dans le processus](#harnais-de-stress-dans-le-processus).

Comportement issu de `crates/metadb-server/src/bin/metadb-ctl.rs` pour les charges distantes :

- Le client alloue un identifiant de répertoire, puis des blocs d'inodes et d'identifiants de fichier de longueur `--ops`, et refuse une attribution dont le compte ou la fin déborde.
- Il imprime un objet JSON avec `bench_pid`, `workload` et `requested_ops`, puis crée autant de fichiers de 4096 octets dans un nouveau répertoire.
- Chaque phase imprime du JSON avec `attempted`, `succeeded`, `failed`, `transport_failed`, `application_failed`, `errors_by_kind` et des percentiles de latence (`p50_ms`, `p99_ms`, `p999_ms`). Le processus se termine en erreur si `succeeded` n'est pas égal à `--ops`.
- `bulk-ttl-delete` supprime les noms créés, puis liste le répertoire avec `limit = 1` et imprime si cette page est vide.
- Une seconde exécution alloue un nouveau `bench_pid`. Elle ne réutilise pas un inode fixe, et elle ne compte pas les créations rejetées comme des succès.

Ne pas traiter un nombre issu de cette commande comme un résultat du dépôt. Une exécution distante dépend de la machine, du profil de compilation et du serveur visé. Une exécution embarquée dépend de la machine et du profil de compilation. Cet arbre n'enregistre un échantillon ni de l'une ni de l'autre.

## Harnais de stress dans le processus

`crates/metadb-server/tests/stress_harness.rs` et `metadb-ctl bench --embedded` appellent tous deux `metadb_server::stress` (`crates/metadb-server/src/stress.rs`). Il n'y a pas de troisième profil nommé `mixed`. `--workload mixed` sélectionne ce harnais. Les deux profils exécutent la même charge mixte : create, lookup, dirstat, listdir, rename et unlink contre un nœud all-in-one persistant, sur un répertoire de données temporaire. Après la boucle minutée, le harnais exécute des ticks de maintenance, puis vérifie les listes de répertoire, dirstat et le quota par rapport aux opérations qui ont réussi. Un tick de maintenance en échec fait échouer le rapport. L'exécution est un processus du système d'exploitation et un répertoire de données. Ce n'est pas trois processus `metadbd`, et ce n'est pas la [charge officielle par préfixe](../en/testing.md#prefix-load).

| Profil | Test | Durée | Concurrence | Répertoires | Quand il s'exécute |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `smoke` | `stress_smoke_mixed_workload` | 5 secondes | 4 | 4 | `cargo test -p metadb-server --test stress_harness`, y compris `cargo test --workspace`. Ignore `METADB_STRESS_*`. |
| `local` | `stress_local_mixed_workload` | 30 secondes sauf remplacement | 8 sauf remplacement | 16 sauf remplacement | `#[ignore]`. Absent de `cargo test --workspace`. Lit `METADB_STRESS_DURATION_SECS`, `METADB_STRESS_CONCURRENCY`, `METADB_STRESS_KEYSPACE` et `METADB_STRESS_SEED`. |

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

`METADB_STRESS_*` s'applique au profil local, y compris `metadb-ctl bench --embedded --profile local`. Sur `metadb-ctl bench --embedded`, `--duration-secs`, `--concurrency`, `--keyspace` et `--seed` remplacent le profil choisi, après la lecture de l'environnement. Le profil local de `cargo test` n'a pas ces remplacements par option ; il ne lit que ces variables. Une durée, une concurrence ou un nombre de répertoires inférieur à 1 est relevé à 1. `--profile` vaut `smoke` par défaut. `--profile ci` est le même profil smoke. Toute autre valeur de `--profile` termine la commande en erreur. Le profil smoke ne lit pas `METADB_STRESS_*`. Les charges distantes `hotspot-create` et `bulk-ttl-delete` ignorent `--profile`.

Le test imprime une ligne et exige `errors = 0`, `correctness_errors = 0` et `ops > 0`. `metadb-ctl bench --embedded` imprime `embedded metadbd listening on <addr>` et la même ligne, et se termine en erreur lorsque `errors` ou `correctness_errors` est supérieur à zéro. Les champs de la ligne sont :

```text
metadb stress profile=<name> ops=<count> errors=<count> correctness_errors=<count> elapsed_ms=<ms> ops_per_s=<rate> latency_us p50=<us> p99=<us> max=<us>
```

`errors` compte les opérations en échec dans la boucle mixte. `correctness_errors` compte les désaccords trouvés ensuite. `latency_us` `p50`, `p99` et `max` sont des percentiles de la latence enregistrée, en microsecondes. Ils sont imprimés. Cet arbre n'impose pas de seuil de latence.

Une ligne issue de l'une ou l'autre commande est un résultat pour la machine qui l'a imprimée. Ce dépôt n'en enregistre pas. Ce n'est pas un résultat de niveau service, pas une validation de première livraison, et pas la charge officielle de 1 000 000 créations.

## Topologie persistante et jointure

Ces commandes sont dans cet arbre. La CI les exécute avec `cargo test --workspace --locked`.

- `cargo test -p metadb-server --test topology_single_node` ouvre `PersistentMetaDbCluster` sur un répertoire temporaire.
- `cargo test -p metadb-server --test topology_scale_out` force un découpage à frontière de PID dans ce processus.
- `cargo test -p metadb-server --test topology_distributed` utilise un processus et `DurableCellManager::scale_out_replicas` pour trois votants localhost, y compris la perte du leader sous une charge de créations.
- `cargo test -p metadb-server --test stress_crash_restart` s'arrête sans vider le memtable pendant qu'une création est dans `propose`, puis rouvre le même `data_dir`.
- `cargo test -p metadb-server --test join_existing_group` démarre un second `metadbd` et le joint au groupe Raft du premier processus, y compris un cas avec secret partagé.
- `open_and_serve` rejette toujours tout rôle autre que `all-in-one`. Une seconde cellule reste non implémentée. `RegisterCell` renvoie une erreur.

Le profil smoke et le profil local ignoré sont le [harnais de stress dans le processus](#harnais-de-stress-dans-le-processus). `topology_distributed` reste un processus du système d'exploitation avec trois votants Raft sur localhost, et non trois processus `metadbd`. Les rôles désagrégés et une seconde cellule ne sont toujours pas des déploiements vivants.
