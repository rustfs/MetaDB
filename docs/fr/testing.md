# Tests et bancs

[Présentation](./README.md) · [English](../en/testing.md) · [Deutsch](../de/testing.md) · [日本語](../ja/testing.md) · [简体中文](../zh/testing.md)

Les exécuter depuis la racine du dépôt avec la chaîne épinglée (Rust 1.100.0).

## Contrôles

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` construit la bibliothèque, les tests d'intégration et les deux binaires. Il n'y a pas de cible `[[bench]]` ni de répertoire `benches/`. Le générateur de charge livré dans cet arbre est `metadb-ctl bench`, ci-dessous.

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

Options : `--workload` vaut `hotspot-create` (le défaut) ou `bulk-ttl-delete`. `--concurrency` vaut 16 par défaut. `--ops` vaut 1000 par défaut. Toute autre chaîne de charge est une erreur.

Comportement issu de `crates/metadb-server/src/bin/metadb-ctl.rs` :

- Chaque tâche exécute `ops / concurrency` itérations, et au moins une. Le reste de la division est abandonné, donc le nombre imprimé peut être inférieur à `--ops`. Si `--ops` est inférieur à `--concurrency`, le nombre peut être supérieur à `--ops` car chaque tâche s'exécute encore une fois. Le nombre imprimé est celui des tentatives, pas celui des mutations acceptées.
- `hotspot-create` fait un mkdir de l'inode `999000` nommé `bench_hotspot_create` sous l'inode parent `1`, puis appelle `CreateFile` pour des fichiers de 4096 octets. Le `Result` du mkdir est abandonné. `unwrap` porte seulement sur le `Result` tonic, donc un échec de transport ou de statut interrompt le processus. `CreateFile` renvoie les échecs d'application tels que `AlreadyExists` dans un `CreateFileResponse.error` OK, et le client ignore ce champ.
- `bulk-ttl-delete` fait les mêmes créations sous l'inode `999001` (`bench_bulk_ttl_delete`), puis appelle `Unlink` de la même façon (`unwrap` de transport, `error` d'application ignorée), puis un `ListDir` avec `limit = 1000`.
- `hotspot-create` imprime `Completed <n> single-directory hotspot creates in <s>s (<ops/s> ops/s)` après les tentatives, y compris celles dont l'erreur d'application a été ignorée.
- `bulk-ttl-delete` imprime le nombre de tentatives d'unlink, les secondes, les ops/s, le nombre d'entrées vivantes renvoyé par cette liste, et la durée de la liste en millisecondes. L'appel de liste est vérifié ; les unlink ne le sont pas.
- Le client ne calcule pas de percentiles de latence et ne vérifie ni dirstat ni quota. Une seconde exécution contre le même serveur peut encore imprimer `Completed` et un débit lorsque chaque création est rejetée avec `AlreadyExists`.

Ne pas traiter un nombre issu de cette commande comme un résultat du dépôt. Il dépend de la machine, du profil de compilation et du serveur visé. Cet arbre n'enregistre pas d'échantillon.

## Pull request ouverte n° 2

La [pull request n° 2](https://github.com/rustfs/MetaDB/pull/2) est ouverte et ne fait pas partie de cette branche. Elle ajoute des tests de topologie persistants et un harnais de stress embarqué. Sa propre description dit :

- Le test mono-nœud appelle `PersistentMetaDbCluster::open_and_serve` sur un répertoire temporaire.
- Le test de montée en charge force un découpage à frontière de PID dans ce même processus.
- Le test distribué est un processus avec trois votants Raft sur localhost. Il appelle `DurableCellManager::scale_out_replicas`. Il ne lance pas trois processus `metadbd` du système d'exploitation.
- `open_and_serve` rejette toujours tout rôle autre que `all-in-one`. Les rôles désagrégés et une seconde cellule restent non implémentés.

Le rapport de cette pull request, sur la machine virtuelle de l'auteur avec Rust 1.100.0 (le matériel n'est pas nommé), contient une ligne smoke du harnais que cette branche ne contient pas :

```text
metadb stress profile=smoke ops=12219 errors=0 correctness_errors=0 elapsed_ms=5221 ops_per_s=2340.3 latency_us p50=1318 p99=5207 max=12449
```

Cette ligne est une seule exécution d'un test non fusionné. Ce n'est pas une cible de service et elle n'est pas reproduite par `metadb-ctl bench` sur cette branche. Des commandes telles que `cargo test -p metadb-server --test topology_distributed` et `metadb-ctl bench --embedded` n'existent que sur la branche de cette pull request.
