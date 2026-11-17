# Tests und Benches

[Überblick](./README.md) · [English](../en/testing.md) · [Français](../fr/testing.md) · [日本語](../ja/testing.md) · [简体中文](../zh/testing.md)

Diese Befehle im Repository-Wurzelverzeichnis mit der festgesetzten Werkzeugkette ausführen (Rust 1.100.0).

## Prüfungen

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` baut die Bibliothek, die Integrationstests und beide Binärdateien. Es gibt kein `[[bench]]`-Ziel und kein Verzeichnis `benches/`. Der Lastgenerator, der in diesem Baum mitgeliefert wird, ist `metadb-ctl bench`, unten.

Nützliche engere Befehle:

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

## Was jede Suite wirklich ausführt

### Simulator im Speicher (`MetaDbEngine::new_single_node`)

Die Tests `T1`–`T6` in `crates/metadb-server/tests/integration_tests.rs` öffnen kein Datenverzeichnis.

| Test | Was er prüft |
| :--- | :--- |
| `test_t1_pid_boundary_sharding_preserves_directory_colocation` | Ein Split an der Elternkennung hält die Kinder eines Verzeichnisses zusammen |
| `test_t2_jumbo_shard_isolation_and_dual_mode_fallback` | Jumbo-Fehler im Speicher und Dual-Modus-Rückfall |
| `test_t3_conflict_free_async_attr_aggregation_and_read_reconciliation` | Attributdeltas sind für dirstat vor dem Flush sichtbar |
| `test_t4_decoupled_async_auxiliary_idempotency_and_cross_shard_rename` | Quota-/Index-Warteschlangen und shard-übergreifendes Umbenennen |
| `test_t5_in_memory_mvcc_and_locality_aware_compaction_on_bulk_deletes` | MVCC und Locality-Compaction auf dem Backend im Speicher |
| `test_t6_single_node_to_distributed_multi_cell_evolution_and_sql_proxy` | Reservierter Name, SQL, nicht-leeres `rmdir`, dann `register_node` + `scale_out_raft_replicas(3)` auf der Membership-Map **im Speicher** |

`T6` startet weder Raft noch einen zweiten Zellenprozess. Der Name beschreibt das Membership-Objekt des Simulators.

### Ein dauerhafter Prozess

| Test | Was er prüft |
| :--- | :--- |
| `test_persistent_restart_lookup_dirstat_quota` | `open_and_serve` auf einem temporären Verzeichnis, Herunterfahren, Wiederöffnen, lookup / dirstat / Quota |
| `test_maintenance_tick_purges_raft_log` | Nach 24 Anlegevorgängen wirft ein Wartungstakt behaltene Raft-Einträge weg |
| `test_e2e_persistent_grpc_cluster_restart_split_2pc_and_locality_compaction` | Ein dauerhafter Prozess, echtes gRPC, Neustart, erzwungener Split, Rename-2PC, Massenlöschen, Quota, Sekundärindex, Prometheus-Text |

Der Ende-zu-Ende-Test setzt `shard_split_threshold_bytes = 2048` und `locality_tombstone_threshold = 20`, damit eine kleine Fixture diese Schwellen überschreitet. Diese Werte sind Testkonfiguration, nicht Einsatzvorgaben.

### Raft, immer noch ein Betriebssystemprozess

| Test | Was er prüft |
| :--- | :--- |
| `test_single_voter_walless_crash_recovery_and_three_watermark_pinning` | Ein Wähler, WAL-lose Recovery, Bereinigungs-Wasserstände |
| `test_suffix_only_truncate_final_crc_rejection_and_snapshot_verification` | Abgerissenes Ende und Prüfsummenablehnung |
| `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` | Drei prozessinterne tonic-Listener, einfacher KV-Apply-Handler, `AddLearner` auf 3 Wähler, ursprünglichen Leader stoppen, Lesen nach Failover |

Der Failover-Test bindet `127.0.0.1:0` dreimal im Testprozess. Das sind keine drei Betriebssystemprozesse, und er bedient nicht `MetadataService`.

### Cluster, Transaktionen, SQL, Speicher

| Test | Was er prüft |
| :--- | :--- |
| `test_epoch_fenced_live_pid_split_and_durable_id_allocator` | Epoch-geschützter PID-Split und der dauerhafte ID-Zuteiler |
| `test_jumbo_isolation_and_three_way_dual_mode_scatter_gather_and_rmdir` | Dauerhaftes Jumbo-Flag, Dual-Modus-Scatter-Gather, rmdir |
| `split_crash_recovery_tests` | Persistenzfehler und Absturzpunkte während des Splits, dann Wiederöffnen |
| `test_two_pc_crash_recovery_posix_overwrite_and_cycle_fencing` | 2PC-Recovery, Überschreib-Umbenennen, Zyklusablehnung |
| `test_aux_outbox_spill_contiguous_frontiers_gap_rejection_and_cleanup` | Auslagerung `0x07`, Lücken, Aufräumen |
| `test_concurrent_attr_aggregation_read_reconciliation_and_l0_flush_barriers` | Nebenläufige Attributaggregation |
| `test_all_twenty_metadata_sql_templates_and_attr_isolation` | Alle 20 Vorlagen, Literale und `?`, Isolation von `_attr_` |
| LSM- / MVCC- / Locality-Tests | SSTable- und `MANIFEST`-Recovery, `SnapshotGuard`, unterste Compaction |

Unit-Tests liegen auch in `metadb-core` (`hlc`, `schema`) und `metadb-proto` (`convert`).

## `metadb-ctl bench`

Zuerst `metadbd` starten. Dann:

```bash
cargo run -p metadb-server --bin metadb-ctl -- \
  --endpoint 127.0.0.1:7800 \
  bench --workload hotspot-create --concurrency 16 --ops 1000

cargo run -p metadb-server --bin metadb-ctl -- \
  --endpoint 127.0.0.1:7800 \
  bench --workload bulk-ttl-delete --concurrency 16 --ops 1000
```

Flags: `--workload` ist `hotspot-create` (die Vorgabe) oder `bulk-ttl-delete`. `--concurrency` ist Vorgabe 16. `--ops` ist Vorgabe 1000. Jede andere Workload-Zeichenkette ist ein Fehler.

Verhalten aus `crates/metadb-server/src/bin/metadb-ctl.rs`:

- Jede Aufgabe läuft `ops / concurrency` Iterationen, mindestens eine. Der Divisionsrest entfällt, daher kann die gedruckte Zahl kleiner als `--ops` sein. Ist `--ops` kleiner als `--concurrency`, kann die Zahl größer als `--ops` sein, weil jede Aufgabe trotzdem einmal läuft. Die gedruckte Zahl ist die Zahl der Versuche, nicht die Zahl angenommener Mutationen.
- `hotspot-create` legt per mkdir Inode `999000` namens `bench_hotspot_create` unter Eltern-Inode `1` an und ruft dann `CreateFile` für Dateien von 4096 Bytes auf. Das mkdir-`Result` wird verworfen. `unwrap` gilt nur für das tonic-`Result`, daher beendet ein Transport- oder Statusfehler den Prozess. `CreateFile` liefert Anwendungsfehler wie `AlreadyExists` in einem OK-`CreateFileResponse.error`, und der Client verwirft dieses Feld.
- `bulk-ttl-delete` tut dieselben Anlegevorgänge unter Inode `999001` (`bench_bulk_ttl_delete`), ruft `Unlink` auf dieselbe Weise auf (Transport-`unwrap`, Anwendungs-`error` ignoriert) und dann ein `ListDir` mit `limit = 1000`.
- `hotspot-create` druckt `Completed <n> single-directory hotspot creates in <s>s (<ops/s> ops/s)` nach den Versuchen, auch nach Versuchen, deren Anwendungsfehler ignoriert wurde.
- `bulk-ttl-delete` druckt die Zahl der Unlink-Versuche, Sekunden, ops/s, die von dieser Liste gelieferte Zahl lebender Einträge und die Listendauer in Millisekunden. Der Listenaufruf wird geprüft; die Unlinks nicht.
- Der Client berechnet keine Latenzperzentile und prüft weder dirstat noch Quota. Ein zweiter Lauf gegen denselben Server kann weiter `Completed` und einen Durchsatz drucken, wenn jede Anlage mit `AlreadyExists` abgewiesen wird.

Eine Zahl aus diesem Befehl nicht als Repository-Ergebnis behandeln. Sie hängt von der Maschine, dem Build-Profil und dem angesprochenen Server ab. Dieser Baum zeichnet keine Stichprobe auf.

## Offener Pull Request 2

[Pull Request 2](https://github.com/rustfs/MetaDB/pull/2) ist offen und nicht Teil dieses Zweigs. Er fügt dauerhafte Topologietests und einen eingebetteten Stress-Harness hinzu. Seine eigene Beschreibung sagt:

- Der Einzelknoten-Test ruft `PersistentMetaDbCluster::open_and_serve` auf einem temporären Verzeichnis auf.
- Der Skalierungstest erzwingt einen PID-Grenz-Split in demselben Prozess.
- Der verteilte Test ist ein Prozess mit drei localhost-Raft-Wählern. Er ruft `DurableCellManager::scale_out_replicas` auf. Er startet keine drei `metadbd`-Betriebssystemprozesse.
- `open_and_serve` weist weiterhin jede Rolle außer `all-in-one` zurück. Disaggregierte Rollen und eine zweite Zelle bleiben unimplementiert.

Der Bericht dieses Pull Requests, auf der VM des Autors mit Rust 1.100.0 (die Hardware ist nicht genannt), enthält eine Smoke-Zeile aus dem Harness, den dieser Zweig nicht enthält:

```text
metadb stress profile=smoke ops=12219 errors=0 correctness_errors=0 elapsed_ms=5221 ops_per_s=2340.3 latency_us p50=1318 p99=5207 max=12449
```

Diese Zeile ist ein einzelner Lauf eines nicht zusammengeführten Tests. Sie ist kein Dienstziel und wird von `metadb-ctl bench` auf diesem Zweig nicht nachgestellt. Befehle wie `cargo test -p metadb-server --test topology_distributed` und `metadb-ctl bench --embedded` gibt es nur auf dem Zweig dieses Pull Requests.
