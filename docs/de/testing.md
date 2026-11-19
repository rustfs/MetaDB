# Tests und Benches

[Überblick](./README.md) · [English](../en/testing.md) · [Français](../fr/testing.md) · [日本語](../ja/testing.md) · [简体中文](../zh/testing.md)

Diese Befehle im Repository-Wurzelverzeichnis mit der festgesetzten Werkzeugkette ausführen (Rust 1.100.0).

## Prüfungen

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` baut die Bibliothek, die Integrationstests und beide Binärdateien. Es gibt kein `[[bench]]`-Ziel und kein Verzeichnis `benches/`. Die Lastgeneratoren, die in diesem Baum mitgeliefert werden, sind `metadb-ctl bench` und `cargo test -p metadb-server --test stress_harness`, unten.

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
cargo test -p metadb-server --test stress_harness
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

Flags: `--workload` ist `hotspot-create` (die Vorgabe), `bulk-ttl-delete` oder `mixed`. Für die entfernten Workloads ist ausgelassenes `--concurrency` 16, und `--ops` hat die Vorgabe 1000. `--profile` wird nur auf dem eingebetteten Pfad gelesen. `--embedded` oder `--workload mixed` wählt `--endpoint` nicht an und benutzt `--ops` nicht. Der Befehl startet einen temporären All-in-one-Knoten und führt den prozessinternen Stress-Harness aus. Siehe [Prozessinterner Stress-Harness](#prozessinterner-stress-harness).

Verhalten aus `crates/metadb-server/src/bin/metadb-ctl.rs` für die entfernten Workloads:

- Der Client belegt eine Verzeichnis-ID und dann Datei-Inode- und Datei-ID-Blöcke der Länge `--ops` und lehnt eine Zuteilung ab, deren Anzahl oder Ende überläuft.
- Er druckt ein JSON-Objekt mit `bench_pid`, `workload` und `requested_ops` und legt danach so viele 4096-Byte-Dateien in einem neuen Verzeichnis an.
- Jede Phase druckt JSON mit `attempted`, `succeeded`, `failed`, `transport_failed`, `application_failed`, `errors_by_kind` und Latenzperzentilen (`p50_ms`, `p99_ms`, `p999_ms`). Der Prozess endet mit einem Fehler, wenn `succeeded` nicht `--ops` entspricht.
- `bulk-ttl-delete` entfernt die angelegten Namen, listet das Verzeichnis mit `limit = 1` und druckt, ob diese Seite leer ist.
- Ein zweiter Lauf belegt eine neue `bench_pid`. Er verwendet keine feste Inode wieder und zählt abgewiesene Anlegen nicht als Erfolg.

Eine Zahl aus diesem Befehl nicht als Repository-Ergebnis behandeln. Ein entfernter Lauf hängt von der Maschine, dem Build-Profil und dem angesprochenen Server ab. Ein eingebetteter Lauf hängt von der Maschine und dem Build-Profil ab. Dieser Baum zeichnet von keinem der beiden eine Stichprobe auf.

## Prozessinterner Stress-Harness

`crates/metadb-server/tests/stress_harness.rs` und `metadb-ctl bench --embedded` rufen beide `metadb_server::stress` auf (`crates/metadb-server/src/stress.rs`). Es gibt kein drittes Profil namens `mixed`. `--workload mixed` wählt diesen Harness. Beide Profile führen dieselbe gemischte Last aus: create, lookup, dirstat, listdir, rename und unlink gegen einen dauerhaften All-in-one-Knoten auf einem temporären Datenverzeichnis. Nach der zeitlich begrenzten Schleife führt der Harness Wartungsticks aus und prüft dann Verzeichnislisten, dirstat und Quota gegen die erfolgreichen Operationen. Ein fehlgeschlagener Wartungstick lässt den Bericht fehlschlagen. Der Lauf ist ein Betriebssystemprozess und ein Datenverzeichnis. Er ist nicht drei `metadbd`-Prozesse und nicht die [offizielle Präfix-Last](../en/testing.md#prefix-load).

| Profil | Test | Dauer | Nebenläufigkeit | Verzeichnisse | Wann er läuft |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `smoke` | `stress_smoke_mixed_workload` | 5 Sekunden | 4 | 4 | `cargo test -p metadb-server --test stress_harness`, einschließlich `cargo test --workspace`. Ignoriert `METADB_STRESS_*`. |
| `local` | `stress_local_mixed_workload` | 30 Sekunden, sofern nicht überschrieben | 8, sofern nicht überschrieben | 16, sofern nicht überschrieben | `#[ignore]`. Nicht Teil von `cargo test --workspace`. Liest `METADB_STRESS_DURATION_SECS`, `METADB_STRESS_CONCURRENCY`, `METADB_STRESS_KEYSPACE` und `METADB_STRESS_SEED`. |

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

`METADB_STRESS_*` gilt für das Local-Profil, einschließlich `metadb-ctl bench --embedded --profile local`. Bei `metadb-ctl bench --embedded` überschreiben `--duration-secs`, `--concurrency`, `--keyspace` und `--seed` das gewählte Profil, nachdem die Umgebung gelesen wurde. Das Local-Profil von `cargo test` hat diese Flag-Überschreibungen nicht; es liest nur diese Variablen. Eine Dauer, eine Nebenläufigkeit oder eine Verzeichniszahl unter 1 wird auf 1 angehoben. `--profile` ist Vorgabe `smoke`. `--profile ci` ist dasselbe Smoke-Profil. Jeder andere `--profile`-Wert beendet den Befehl mit einem Fehler. Das Smoke-Profil liest `METADB_STRESS_*` nicht. Entfernte `hotspot-create` und `bulk-ttl-delete` ignorieren `--profile`.

Der Test druckt eine Zeile und verlangt `errors = 0`, `correctness_errors = 0` und `ops > 0`. `metadb-ctl bench --embedded` druckt `embedded metadbd listening on <addr>` und dieselbe Zeile und endet mit einem Fehler, wenn `errors` oder `correctness_errors` größer als null ist. Die Felder der Zeile sind:

```text
metadb stress profile=<name> ops=<count> errors=<count> correctness_errors=<count> elapsed_ms=<ms> ops_per_s=<rate> latency_us p50=<us> p99=<us> max=<us>
```

`errors` zählt fehlgeschlagene Operationen in der gemischten Schleife. `correctness_errors` zählt danach gefundene Abweichungen. `latency_us` `p50`, `p99` und `max` sind Perzentile der aufgezeichneten Operationslatenz, in Mikrosekunden. Sie werden gedruckt. Dieser Baum erzwingt keine Latenzschwelle.

Eine Zeile aus einem der beiden Befehle ist ein Ergebnis für die Maschine, die sie gedruckt hat. Dieses Repository zeichnet keine auf. Sie ist kein Ergebnis auf Dienstebene, kein First-Ship-Bestehen und nicht die offizielle Präfix-Last von 1.000.000 Anlegen.

## Dauerhafte Topologie und Beitritt

Diese Befehle sind in diesem Baum. Die CI führt sie mit `cargo test --workspace --locked` aus.

- `cargo test -p metadb-server --test topology_single_node` öffnet `PersistentMetaDbCluster` auf einem temporären Verzeichnis.
- `cargo test -p metadb-server --test topology_scale_out` erzwingt einen PID-Grenz-Split in diesem Prozess.
- `cargo test -p metadb-server --test topology_distributed` nutzt einen Prozess und `DurableCellManager::scale_out_replicas` für drei localhost-Wähler, einschließlich Leader-Verlust unter Anlege-Last.
- `cargo test -p metadb-server --test stress_crash_restart` stoppt ohne Memtable-Flush, während ein Anlegen in `propose` ist, und öffnet dasselbe `data_dir` wieder.
- `cargo test -p metadb-server --test join_existing_group` startet ein zweites `metadbd` und lässt es der Raft-Gruppe des ersten Prozesses beitreten, einschließlich eines Falls mit gemeinsamem Geheimnis.
- `open_and_serve` weist weiterhin jede Rolle außer `all-in-one` zurück. Eine zweite Zelle bleibt unimplementiert. `RegisterCell` liefert einen Fehler.

Das Smoke-Profil und das ignorierte Local-Profil sind der [prozessinterne Stress-Harness](#prozessinterner-stress-harness). `topology_distributed` bleibt ein Betriebssystemprozess mit drei localhost-Raft-Wählern, nicht drei `metadbd`-Prozesse. Disaggregierte Rollen und eine zweite Zelle sind weiterhin keine lebenden Einsätze.
