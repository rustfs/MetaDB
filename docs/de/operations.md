# Betrieb

[Überblick](./README.md) · [English](../en/operations.md) · [Français](../fr/operations.md) · [日本語](../ja/operations.md) · [简体中文](../zh/operations.md)

## Datenverzeichnis

Nach einem Bootstrap mit `bootstrap_table_shards = true`:

```text
<data_dir>/
  global_allocator/ID_ALLOCATOR_STATE
  cell_<cell_id>/CELL_MANAGER_STATE
  be/NODE_SHARD_REGISTRY
  be/shards/00000000000000000001/lsm/     # MetaTable
  be/shards/00000000000000000001/raft/
  be/shards/00000000000000000002/...      # QuotaTable
  be/shards/00000000000000000003/...      # SecondaryIndexTable
```

Shard-Kennungen sind dezimal, auf 20 Stellen mit Nullen aufgefüllt. Kind-Shards aus einem Split erscheinen als weitere Verzeichnisse unter `be/shards/`. `CELL_MANAGER_STATE` und `ID_ALLOCATOR_STATE` sind CRC32C-Rahmen. `NODE_SHARD_REGISTRY` nicht löschen, solange Shard-Verzeichnisse bleiben; der Host nutzt sie, um Split-Kinder wiederherzustellen.

Den Prozess mit Strg-C (oder `PersistentMetaDbCluster::shutdown`) beenden, bevor das Verzeichnis kopiert wird. Ein lebender Prozess hält je Shard eine LSM-Datei `LOCK`.

## `metadb-ctl`

Globale Flags:

| Flag | Vorgabe | Bedeutung |
| :--- | :--- | :--- |
| `--endpoint` | `127.0.0.1:7800` | `Host:Port`, oder eine URL, die schon mit `http://` oder `https://` beginnt |
| `--auth-token` | leer | Bearer-Token. Wird auch aus `METADB_AUTH_TOKEN` gelesen |

Der Client öffnet je Befehl einen neuen gRPC-Kanal. Er spricht nicht `RaftTransportService`.

### `cluster status`

Ruft `GetRoutes` mit `cell_id = 0` auf. Je Shard druckt es `shard`, `cell`, `leader`, Wählerzahl, `epoch=(conf_ver, ver)`, `dual_mode` und `jumbo`. It then prints local `CELL_CONTROL_PLANE` placement rows (`phase`, `phase_code`, `dedicated_node`, `dominant_pid`, `moves`, attempt timestamps, `reason`) and `placement_automatic_transfer_enabled=false`. Those rows are not routes. A remote cell contributes no placement rows. It does not print disk free space, QPS, Raft watermarks, or `MOVE_STAGED` / `MOVE_LIVE`. The reconciler does not publish phase `recorded`. It does not stamp `transfer_attempt_unix_ms`. A new row stores 0. A later reconcile keeps a non-zero value already in the file.

### `shard list --cell-id <u64>`

Ruft `GetRoutes` für diese Zelle auf (`0` für jede gecachte Route). Druckt `shard`, `cell`, rohe Start-/Endschlüsselbytes und Epoch `(conf_ver, version)`, then the same local placement lines. A remote cell prints `Placements: 0`.

### `shard split --shard-id <u64>`

Ruft `TriggerShardSplit` auf, das eine Split-Prüfung erzwingt, auch wenn der Shard unter `shard_split_threshold_bytes` liegt. On success it prints `split_executed`, `child_shard_id`, and the placement fields. When no placement row exists, including an ordinary split, the line is `placement_phase=` and `placement_phase_code=0`. Code 0 is only that no-record case. Stored rows use codes 1–4. `split_executed=false` heißt, der Worker hat kein Kind erzeugt (zum Beispiel weniger als zwei Schlüssel, oder ein Jumbo-Kandidat, der eine Placement-Zeile schreibt und trotzdem `split_executed=false` lässt).

### `shard enable-dual-mode --shard-id <u64> --pid <u64>`

Ruft `EnableDualModeSplit` auf. Die CLI sendet ein leeres `split_name`. Der Server speichert `dual_mode_pid` und splittet nicht sofort.

### `raft scale-replicas --cell-id <u64> --target-voters <u32>`

Ruft `ScaleShardReplicas` auf. Der Server verlangt, dass schon `target_voters` Einträge in der Knotentabelle dieser Zelle stehen. Eine frische all-in-one-Zelle hat einen Knoten, daher liefert `--target-voters 3` einen Consensus-Fehler der Form `need 3 registered nodes in cell <id>, have 1`. Bei Erfolg druckt es, wie viele lokale Shards an den Membership-Wechsel übergeben wurden.

### `cell register --cell-id <u64> --endpoints <a,b,c>`

Calls `RegisterCell`. The server fsyncs `GLOBAL_CELL_DIRECTORY` only when the endpoint's routes cover the requested range exactly. Omitting the keys sends `[01, 04)`, which overlaps a default cell, and `metadb-ctl` exits non-zero. The caller does not forward keys.

### `sql <Anweisung>`

Ruft `ExecuteSql` ohne Bind-Parameter auf. Die Anweisung muss eine der 20 Vorlagen in [Architektur](./architecture.md) sein, mit Literalen statt `?`.

```bash
metadb-ctl --endpoint 127.0.0.1:7800 sql \
  "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
```

### `bench`

See [Tests and benches](./testing.md#metadb-ctl-bench). Remote `hotspot-create` and `bulk-ttl-delete` are a client against a running server. They print JSON with `attempted`, `succeeded`, and `failed`, and the process exits non-zero unless `succeeded` equals `--ops`. `--embedded` and `--workload mixed` run the [in-process stress harness](./testing.md#in-process-stress-harness) instead of dialing the endpoint. That path passes when `errors` and `correctness_errors` are both zero, and it prints latency percentiles in microseconds. Neither path is a Cargo benchmark target. The repository does not check in a throughput number for either path. A printed line is not a service-level result and is not the official 1,000,000-create profile.

The separate prefix-load harness (100 prefixes × 10,000 creates, then 100,000 `SetAttr` overwrites, then 100,000 deletes, concurrency 64) is documented in [Tests and benches](../en/testing.md#prefix-load). GitHub Actions runs only the small profile. It does not run the 1,000,000-create profile.

## Metriken

`node.metrics_listen_addr` setzen (zum Beispiel `127.0.0.1:9100`). `open_and_serve` nimmt auf dieser Adresse TCP-Verbindungen an. `GET /metrics` liefert Prometheus-Text. Jeder andere Pfad liefert 404, jede andere Methode 405. Wenn `[security].shared_secret` nicht leer ist, muss jede Anfrage auf diesem Listener `Authorization: Bearer <shared_secret>` senden. Ein fehlender, leerer oder falscher Bearer liefert HTTP 401 und einen Rumpf, der nicht der Metriktext ist, auch für `/status`. An empty secret does not start the process on any address, including loopback, unless `allow_unauthenticated = true`. That flag is loopback-only and still refuses a wildcard or other non-loopback address. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.

Gauges und Counter aus `render_prometheus_metrics`:

| Name | Art |
| :--- | :--- |
| `metadb_cells_total` | gauge |
| `metadb_shards_total` | gauge |
| `metadb_in_memory_mvcc_writes_total` | counter |
| `metadb_pruned_mvcc_versions_total` | counter |
| `metadb_persistent_kv_writes_total` | counter |
| `metadb_active_physical_tombstones` | gauge |
| `metadb_tombstones_traversed_on_scans_total` | counter |
| `metadb_locality_compactions_total` | counter |
| `metadb_min_raft_safe_purge_index` | gauge |
| `metadb_placement_records` | gauge, local placement rows |
| `metadb_placement_phase_recorded` | gauge; the reconciler does not publish this phase |
| `metadb_placement_phase_leader_on_dedicated` | gauge |
| `metadb_placement_phase_dedicated_not_shared` | gauge |
| `metadb_placement_phase_blocked` | gauge |
| `metadb_placement_moves_leadership` | gauge |
| `metadb_placement_moves_voter_replaced` | gauge |
| `metadb_placement_rows_with_transfer_attempt` | gauge; the reconciler does not stamp a new value and keeps one already in the file |
| `metadb_placement_rows_with_replace_attempt` | gauge |
| `metadb_placement_automatic_transfer_enabled` | gauge, 0 in this build |

There is no `metadb_raft_watermark_lag_entries` series and no per-shard placement label. There is no `MOVE_STAGED` or `MOVE_LIVE` series. `metadb_cells_total` counts rows in this process's `GLOBAL_CELL_DIRECTORY`, including a remote cell after `RegisterCell`. `metadb_shards_total` counts only local shards. A fresh process has one cell. The English operability page lists the readiness gauges that this table does not repeat.

## Skalierung

Was in einem Prozess funktioniert:

- Mehr Shards. Wachstum über `shard_split_threshold_bytes` hinaus, oder `metadb-ctl shard split`, kann Shard-Gruppen unter demselben Verzeichnis `be/shards` hinzufügen. Der Elternteil behält einen linken Schlüsselbereich, das Kind startet als weitere 1-Wähler-Gruppe auf diesem Knoten. Ein Neustart öffnet beide wieder.
- Dual-Modus-Flag. `shard enable-dual-mode` markiert eine Elternkennung. Ein späterer Split dieses Shards kann auf `(pid, name)` schneiden.
- Jumbo-Flag. Wenn ein Elternteil mehr als 85 % der `MetaTable`-Bytes auf dem Shard besitzt, oder der Shard nur ein Elternteil hat, wird die Route `JumboIsolated`. Die Kennung des gewidmeten Knotens wird aus den schon in dieser Zelle registrierten Knoten gewählt. Bei einem Knoten nennt das Flag diesen Prozess. Das Flag selbst verschiebt keine Dateien. Ein Loopback-`MOVE_STAGED`/`MOVE_LIVE` kann das Verzeichnis auf einen `127.0.0.1`-Listener legen. Der Listener wird nicht von `metadbd` gestartet, öffnet kein Raft und ruft `transfer_leader` nicht auf. Das ist keine Produktionskopie. Siehe `docs/en/scale-control-plane-status.md`.
- Prozessinterne Wähler. `DurableCellManager::scale_out_replicas` kann Learner hinzufügen und hochstufen, wenn diese Knotenkennungen registriert sind **und** jede Adresse schon `RaftTransportService` für die Gruppe dieses Shards bedient. `crates/metadb-raft/tests/multi_raft_failover_tests.rs` tut das mit drei Listenern in einem Testprozess und einer einfachen KV-Zustandsmaschine, nicht der Metadaten-Zustandsmaschine und nicht drei `metadbd`-Prozessen.

Was als Betriebsverfahren nicht funktioniert:

1. `metadbd --role be` hosts local Raft replicas and serves their metadata. It does not publish a cell or mint ids. `metadbd --role proxy` forwards metadata and transactions to one `[cluster] upstream` and does not host Raft. `--role global-manager` serves `GLOBAL_CELL_DIRECTORY` only and does not open Raft groups. `--role cell-manager` hosts one local cell and its Raft groups, and publishes that cell when `[cluster] global_manager` is set. The directory file is not a Raft group, and the global process does not forward keys.
2. Ein zweiter `metadbd --role all-in-one` tritt einer bestehenden Gruppe bei, wenn `--join` oder `--cell-manager` den aktuellen Leader nennt und das Datenverzeichnis leer ist. Der Leader hängt einen leeren Eintrag an, schreibt einen Snapshot und befördert den Beitretenden vom Learner zum Voter. `--join` muss diesen Leader erreichen. Ein Verzeichnis mit vorhandenem Raft-Zustand öffnet ihn erneut und ignoriert eine neue Join-Adresse. Ein frischer Allocator für Knoten-IDs 2 bis 32767 beginnt im Inode-, File-ID- und Shard-ID-Namensraum dieses Knotens und gibt daher keine IDs erneut aus, die Knoten 1 schon vergeben hat. Eine Allocator-Datei, die bereits niedrige IDs vergeben hat, wird nicht umgebucht. `Lookup`, Verzeichnisauflistung, Verzeichnisstatistik, `GetDirPartitionStat`, Quota- und Sekundärindex-Lesungen, die einen Follower mit bekanntem Leader erreichen, werden an diesen Leader weitergeleitet. Ein Follower ohne bekannten Leader liefert weiterhin `NotLeader`. Während die Mitgliedschaft joint ist, meldet `GetRoutes` jeden angewendeten Wähler, den aktuellen Leader und die angewendete Epoche. Nach dem Commit der uniformen Konfiguration stimmen diese Wähler und `conf_ver` mit Raft überein. Ein Join, der nicht fertig wird, wiederholt vorübergehende Fehler 45 Sekunden lang, fährt dann Listener und lokale Raft-Gruppen herunter und gibt den Fehler zurück. `metadbd` endet, weil der Start diesen Fehler zurückgegeben hat. Der erste Fehlversuch bricht den Prozess nicht von selbst ab.
3. `metadb-ctl cell register` records a second cell only after that cell's routes cover the requested range. This process does not forward those keys. `open_and_serve` still starts one local cell.
4. Der Speicherpfad `MetaDbEngine::new_single_node`, den die Tests `T1`–`T6` nutzen, kann eine Wählerzahl im Speicher auf 3 setzen. Dieses Objekt hat weder Platte noch Raft. Es ist keine Skalierung von `metadbd`.

`topology_distributed` still uses `DurableCellManager::scale_out_replicas` with localhost voters inside one OS process. `join_existing_group` starts real `metadbd` processes and joins them. The shared-secret case in that file opens two clusters inside the test process; it does not start two `metadbd` OS processes. Disaggregated roles are still not a live deployment. A second cell can be recorded in this process's directory; that is not a production multi-cell deployment. This process can copy its own jumbo shard directory onto `jumbo_dedicated_root`. A loopback `MOVE_STAGED` / `MOVE_LIVE` placement is specified in `docs/en/scale-control-plane-status.md`. It does not call `transfer_leader`, does not replicate the cell directory, and is not a production move.

## Fehler, die auftreten können

Die Varianten sind in `crates/metadb-core/src/error.rs` definiert. gRPC-Methoden liefern die Variante gewöhnlich im Antwortfeld `error` und nicht als Nicht-OK-Status.

| Variante | Wann der Code sie auslöst |
| :--- | :--- |
| `ReservedEntryName` | Leerer Name, NUL, oder `_attr_` als Nutzereintrag |
| `AlreadyExists` / `NotFound` | Anlegen oder Nachschlagen von `(pid, name)` |
| `DirectoryNotEmpty` | `rmdir`, solange Kinder bleiben, auch vor dem Attribut-Flush |
| `InvalidEntryType` | `rmdir` auf einer Datei, oder `unlink` auf einem Verzeichnis |
| `TransactionTimeout` | Snapshot-HLC älter als das MVCC-Fenster im Speicher |
| `ClockSkewExceeded` | Entferntes HLC der lokalen Wanduhr um mehr als `max_clock_drift_ms` voraus |
| `EpochMismatch` / `NotLeader` | Veraltete Routen-Epoche, oder dieses Replikat ist nicht der Leader. Lesezugriffe eines Followers mit bekanntem Leader werden weitergeleitet. `EpochMismatch` bei einem Mehrschlüssel-Prepare bricht diese Runde ab und gruppiert neu; derselbe Stapel wird nicht erneut vorgeschlagen |
| `RetryableConsensus` | Der Log-Eintrag eines Schreibens wurde beim Leaderwechsel verworfen, oder ein Quorum hat nicht geantwortet. Schreibzugriffe desselben Shards und 2PC wiederholen das innerhalb des bestehenden Routing-Budgets. Ein logisches Schreiben behält eine Proposal-ID. Ist diese schon committed, liefert der Shard das ursprüngliche Ergebnis und führt den Befehl nicht erneut aus. Speicherfehler und ein gestoppter Raft-Knoten bleiben `ConsensusError` und werden nicht wiederholt |
| `UnsplittableJumboShard` | Splitter im Speicher (`shard.rs`), nicht der dauerhafte Jumbo-Pfad, der `JumboIsolated` setzt und Erfolg ohne Kind-Shard liefert |
| `WriteIntentConflict` / `TransactionAborted` | 2PC-Konflikt, oder ein Verzeichnis-Umbenennungs-Lease ist schon gehalten |
| `InvalidRenameCycle` | Umbenennen eines Verzeichnisses in sich selbst oder einen Nachkommen |
| `ConsensusError` | Nicht genug registrierte Knoten, Learner-Aufhol-Timeout (10 s), oder OpenRaft-Membership-Fehler |
| `SqlParseError` | Die Anweisung ist keine der 20 Vorlagen |

`CodecError` ist auch der Typ für die Ablehnung einer nicht implementierten Rolle und für ungültiges TOML.

## Bekannte Grenzen

- `all-in-one`, `cell-manager`, `global-manager`, `be`, and `proxy` start on a valid config. `be` hosts local replicas and does not mint ids. `proxy` forwards to one upstream and does not host Raft. `global-manager` has no local shards and does not forward. `cell-manager` still colocates its Raft groups.
- A second all-in-one process can join the same cell with `[cluster] join`. That does not create a second cell. `RegisterCell` records a different cell only after its routes cover the requested range, and this process does not forward keys.
- `GetRoutes` scheitert geschlossen bei einer unbekannten `cell_id` ungleich null. `StoreHeartbeat` ersetzt zwischengespeicherte Routen nicht.
- Jumbo-Isolation ist ein Routenflag plus eine Knotenkennung aus der lokalen Knotentabelle. Sie verdrängt andere Shards nicht von einer Platte.
- Registered-node liveness is fsynced in `CELL_CONTROL_PLANE`. `metadbd` persists its own record about every 3 seconds and sends it to the current leader when this process is not the leader. Descriptors on that RPC are annotations, not routes. The observation does not schedule placement. See [Scale control plane](../en/scale-control-plane-status.md).
- A secret that contains a non-whitespace character authenticates Raft, the other three gRPC services, and the metrics listener, including `/livez`, `/readyz`, and `/healthz`, with the same bearer. An empty secret does not start the process. `allow_unauthenticated = true` is the explicit exception for literal loopback IPs (`127.0.0.0/8` or `::1`) and still refuses a wildcard bind. The hostname `localhost` is not accepted for that exception. The bound socket is checked again after listen. A whitespace-only secret is rejected and is not compared as a bearer. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.
- `memtable_size_bytes` versiegelt die aktive Memtable und schreibt sie aus. `max_imm_memtables` begrenzt die eingereihten unveränderlichen Tabellen. `[raft]` timers and `max_segment_bytes` apply when a group starts. They are not reloaded from a changed file, and the segment size does not rewrite segments already on disk.
- `metadb-ctl bench` für `hotspot-create` und `bulk-ttl-delete` druckt JSON-Zeilen: einen Kopf mit `bench_pid`, dann Phasenberichte mit attempted, succeeded, failed und Latenzperzentilen. Der Prozess endet mit einem Fehler, wenn nicht jede angeforderte Operation gelingt. `--workload mixed` oder `--embedded` startet den prozessinternen Stress-Harness. `sql` druckt die Antwort als JSON und endet bei einem Anwendungsfehler mit einem Fehlerstatus.

## Dokumentationslücken

Diese Punkte fehlen im Produkt, daher kann dieser Leitfaden kein Verfahren dafür beschreiben:

1. Eine Mehrprozess-Anordnung für `be`, `proxy`, `cell-manager` und `global-manager`.
2. A second Cell. `RegisterCell` stays an error. Join reaches only the current leader; a follower does not forward it.
3. Einen Jumbo-Shard auf eine eigene Maschine legen und andere Shards von dieser Maschine wegziehen. Ein Loopback-Listener kann ein Jumbo-Verzeichnis mit `MOVE_STAGED` / `MOVE_LIVE` aufnehmen. Das ist nicht `metadbd`, nicht authentifiziert, öffnet kein Raft, ruft `transfer_leader` nicht auf und ist keine produktionsreife Kopie auf einen anderen Host.
4. Natives TLS. Dieser Prozess beendet TLS nicht. Konfigurationsschlüssel, die TLS oder ein Zertifikat verlangen, werden abgelehnt, statt Klartext zu dienen. Die Terminierung gehört in einen externen Proxy. Raft und Metriken nutzen denselben Bearer wie die anderen Dienste, wenn ein Geheimnis gesetzt ist; das ist kein gegenseitiges TLS. Eine heartbeat-getriebene Steuerschleife ist nicht implementiert.
5. A gRPC health RPC, probes when `metrics_listen_addr` is unset, and per-shard Prometheus series. `/readyz` does not wait for the 250 ms 2PC recovery pass. A leader election makes `/readyz` and `/healthz` return 503 on replicas that do not know a leader. `[raft]` is loaded and is not `#[serde(skip)]`. Its values apply when a group starts and are not reloaded. See [Operability](../en/operability.md).
6. A checked-in service-level result for `metadb-ctl bench`. The command prints numbers for the machine you run it on. This repository does not record one. See [Tests and benches](./testing.md).
