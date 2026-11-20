# Architektur wie implementiert

[Überblick](./README.md) · [English](../en/architecture.md) · [Français](../fr/architecture.md) · [日本語](../ja/architecture.md) · [简体中文](../zh/architecture.md)

Diese Seite folgt dem Code in `crates/`. Sie beschreibt keinen Mehrprozess-Zelleneinsatz. Dieser Einsatz ist nicht implementiert. Siehe [Betrieb](./operations.md#bekannte-grenzen).

## Prozessform

`PersistentMetaDbCluster::open_and_serve` (`crates/metadb-server/src/lib.rs`):

1. Weist jedes `node.role` außer `all-in-one` zurück.
2. An empty `shared_secret` does not start the process unless `allow_unauthenticated = true`, and that flag is loopback-only. A whitespace-only secret is rejected even with the flag. A usable secret plus the flag is rejected. A wildcard or other non-loopback address cannot use the opt-in. It then binds `node.grpc_listen_addr` (default `127.0.0.1:7800`; `127.0.0.1:0` asks the OS for a port).
3. Öffnet `DurableIdAllocator` unter `<data_dir>/global_allocator`.
4. Öffnet `ShardGroupHost` unter `<data_dir>/be`.
5. Öffnet oder bootstrapt `DurableCellManager` unter `<data_dir>/cell_<cell_id>`.
6. Registriert diese eine Zelle bei `DurableGlobalManager` für den Schlüsselbereich `[0x01, 0x04)`.
7. Stellt sicher, dass Wurzel-Inode `1` (`ROOT_INODE_ID`) einen Attributsatz hat.
8. Serves four gRPC services on one listener. All four use the same bearer interceptor. When `metrics_listen_addr` is set, `GET /metrics` on that address requires the same secret. A non-empty secret rejects a missing or wrong bearer. An empty secret does not listen unless `allow_unauthenticated = true`, and that flag is loopback-only. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.
9. Startet eine Wartungsaufgabe im Abstand von 250 ms.

`metadbd` ist eine dünne Hülle: Es lädt TOML, wendet CLI-Überschreibungen an, ruft `open_and_serve` auf, ruft optional `RegisterNode` an `--cell-manager` auf und ruft bei Strg-C `shutdown` auf. Metriken werden, wenn konfiguriert, in `open_and_serve` bedient.

## Schlüssel und Tabellen

Präfixe der Nutzertabellen (`crates/metadb-core/src/schema.rs`):

| Präfix | Tabelle | Bootstrap-Bereich, wenn `bootstrap_table_shards = true` |
| :--- | :--- | :--- |
| `0x01` | `MetaTable` | Shard 1, `[0x01, 0x02)` |
| `0x02` | `QuotaTable` | Shard 2, `[0x02, 0x03)` |
| `0x03` | `SecondaryIndexTable` | Shard 3, `[0x03, 0x04)` |

`MetaTable`-Schlüssel sind `[0x01][pid: 8 Byte Big-Endian][Name UTF-8]`. Die eigenen Attribute des Verzeichnisses sind das reservierte Kind `(pid, "_attr_")`, neben `(pid, child_name)` gespeichert. Nutzernamen, die leer sind, NUL enthalten oder gleich `_attr_` sind, werden abgewiesen (`ReservedEntryName`).

Verzeichnis-Inode-Kennungen haben Bit 63 gelöscht. Datei-Inode-Kennungen haben Bit 63 gesetzt (`FILE_INODE_TAG_BIT`). Die Wurzel ist Inode `1`. Der dauerhafte Zuteiler beginnt neue Verzeichniskennungen bei `2`, Dateikennungen bei `1` und dynamische Shard-Kennungen bei `4` (1–3 sind die Bootstrap-Shards). `LocalIdBuffer` vergibt Verzeichniskennungen in Blöcken von 256 und Datei-Inode- sowie Dateikennungen in Blöcken von 4.096, nachdem die Zuteilung nach `ID_ALLOCATOR_STATE` geschrieben wurde.

Shard-lokale Systempräfixe:

| Präfix | Satz |
| :--- | :--- |
| `0x04` | `DirShardDelta` für einen Nicht-Anker-Subshard im Dual-Modus |
| `0x05` | 2PC-`WriteIntent` |
| `0x06` | 2PC-`TxnRecord` |
| `0x07` | ausgelagerte Hilfsausgangseinträge |

Wenn `bootstrap_table_shards = false`, erzeugt der Bootstrap einen Shard, Kennung `1`, über `[0x01, 0x04)`.

## Anfrageweg

Ein Metadatenaufruf kommt entweder über den eingebetteten `DistributedTxRouter` (`cluster.tx_router()`) oder über gRPC `MetadataService` / `TransactionService`. `StatelessProxyRouter` übersetzt eine der 20 SQL-Vorlagen und ruft denselben Router auf. Es gibt keine allgemeine SQL-Engine. Der Parser akzeptiert nur die Anweisungen unten (`crates/metadb-proxy/src/sql_templates.rs`). Platzhalter `?` binden Parameter `u64`, `i64` oder Zeichenkette. Ein Literal `name = '_attr_'` ist die Verzeichnisstatistik. Ein gebundener Parameter gleich `_attr_` wird abgewiesen.

| # | Anweisung | Wirkung |
| :--- | :--- | :--- |
| 1 | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | Datei anlegen |
| 2 | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | mkdir |
| 3 | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | unlink |
| 4 | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | rmdir |
| 5 | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | Umbenennen ohne Überschreiben |
| 6 | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | Umbenennen mit Überschreiben |
| 7 | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | dirstat |
| 8 | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | lookup |
| 9 | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | listdir, festes Limit 10.000 |
| 10 | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | listdir-Seite |
| 11 | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | Präfix-listdir (`LIKE` nur in dieser Präfixform) |
| 12 | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | setattr mode |
| 13 | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | setattr uid |
| 14 | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | setattr Größe und fid |
| 15 | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | setattr Änderungszeit |
| 16 | `SELECT * FROM QuotaTable WHERE uid = ?` | Quota lesen |
| 17 | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | Quotalimit setzen (der `ON CONFLICT`-Wert wird gespeichert) |
| 18 | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | Quotas auflisten |
| 19 | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | Sekundärindex lesen |
| 20 | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | Sekundärindex abtasten |

`metadb-ctl sql` sendet `ExecuteSql` mit leerer Parameterliste, die Anweisung muss also Literale statt `?` verwenden.

Das Routing nutzt `CompositeMetaCache`. Bei einem Fehltreffer oder einer Epoch-Abweichung wird der Cache aus der lokalen Zelle aufgefrischt und der Aufruf gegen die lokale Shard-Gruppe wiederholt. `DurableGlobalManager::route_user_key` sucht einen **lokalen** Shard-Handle. Es leitet die Nutzeranfrage nicht an einen anderen Prozess weiter.

## Speicher

Die Zustandsmaschine jedes Shards öffnet ein WAL-loses LSM unter `<data_dir>/be/shards/<shard_id mit 20 Ziffern>/lsm`. SSTables verwenden 4-KiB-präfixkomprimierte Datenblöcke (Restart-Intervall 16), einen Bloom-Filter und ein `MANIFEST` mit einem `CURRENT`-Zeiger. Der Blockcache ist ein 16-facher LRU. Sein Gesamtbudget ist `storage.block_cache_bytes` (Vorgabe 64 MiB). Jeder der 16 Cache-Shards erhält mindestens 16 KiB.

MVCC-Versionen leben `storage.mvcc_retention_ms` (Vorgabe 5.000) lang im Speicher. Ein koordinierter Flush versiegelt die Memtable, führt den Attribut-Aggregationshaken aus und schreibt eine L0-SSTable. Es gibt kein separates Nutzer-KV-WAL. Absturzrecovery ist das Raft-Log plus das LSM-Manifest. `LocalityCompactor` zählt Tombstones je 9-Byte-Präfix-Eimer und kompaktiert einen heißen Löschbereich, wenn sowohl `locality_tombstone_threshold` als auch `locality_tombstone_ratio` überschritten sind. `auto_locality_compaction` setzt `ServerTomlConfig::to_lsm_config` immer auf `true`; die TOML-Datei kann es nicht abschalten.

`storage.memtable_size_bytes` ist die Flush-Schwelle der aktiven Memtable. `storage.max_imm_memtables` (Vorgabe 4) ist die Gegendruck-Obergrenze für unveränderliche Tabellen: ein Schreiber wartet, bis eine unveränderliche Tabelle ausgeschrieben ist, statt eine weitere einzureihen.

## Raft

Jeder lokale Shard ist eine `openraft`-0.10-Gruppe. Das Log ist eine segmentierte CRC32C-Datei unter `.../shards/<id>/raft`. Defaults are loaded from `[raft]` when that table or a field is omitted. `raft_options` is not `#[serde(skip)]`. Unknown keys in `[raft]` fail parsing. Values apply at group start and are not reloaded. `max_segment_bytes` does not rewrite segments already on disk. `/readyz` is 200 only after startup has finished, shutdown has not started, and every local Raft group has finished startup replay, applied a log entry, is a voter, and has a known leader. A bound socket is not enough. There is no gRPC health RPC. See [Operability](../en/operability.md).


| Feld | Vorgabe |
| :--- | :--- |
| Heartbeat | 150 ms |
| Wahl-Timeout | 450–900 ms |
| `max_in_snapshot_log_to_keep` | 0 |
| Log-Segmentgröße | 64 MiB |

Die Log-Bereinigung ist durch `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)` begrenzt. Die 250-ms-Wartungsschleife ruft `flush_snapshot_and_purge` auf, wenn ein Shard noch Log-Einträge behält, es sei denn, eine Snapshot-Wasserstandsmarke liegt bereits bei mindestens `state_flushed_index` und es bleiben weniger als 64 Einträge.

`ConfChangeOrchestrator::add_learners_and_promote_voters` fügt Learner hinzu, wartet, bis der Replikationsrückstand höchstens 64 Einträge beträgt (Standard-Timeout 10 s, der Wert, den `scale_out_replicas` übergibt), und ruft dann `change_membership` auf. Der prozessinterne Test `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` tut das mit drei tonic-Listenern und einer einfachen KV-Zustandsmaschine in einem Betriebssystemprozess. Er startet `metadbd` nicht.

## Transaktionen und Hilfsarbeit

Erzeugen, unlink, setattr, lookup, dirstat, listdir in einem Verzeichnis sowie mkdir/rmdir auf demselben Shard werden dem Shard-Leader vorgeschlagen. Zähler des Elternverzeichnisses werden auf dem Vordergrundpfad nicht gesperrt: Mutationen zeichnen Deltas auf, `ReplicatedAttrAggregator` faltet sie an der Flush-Schranke, und dirstat gleicht ungeschriebene Deltas auf dem Lesepfad ab.

Quota- und `fid -> (pid, name)`-Aktualisierungen laufen über einen Hilfsausgang je Shard. `AuxDispatcher` wendet sie im Wartungstakt an. Wenn der Ausgang nicht angewendet werden kann, können Einträge unter Präfix `0x07` ausgelagert werden, damit die Raft-Compaction nicht dauerhaft von dieser Warteschlange festgehalten wird. `TxnRecoveryWorker` tastet `TxnRecord` / `WriteIntent` im selben Takt ab.

Shard-übergreifendes Umbenennen (und mkdir/rmdir, die zwei Shards berühren) nutzt am Primäranker gebundene 2PC (`WriteIntent` + `TxnRecord`). Verzeichnisumbenennungen nehmen ein einzelnes aktives `DirRenameLease` vom ID-Zuteiler. Ein zweites Lease wird abgewiesen, bis das erste freigegeben ist oder seine HLC-Ablaufzeit vorbei ist. Die Lease-TTL beträgt mindestens 100 ms.

## Sharding innerhalb des Prozesses

`maybe_split_or_isolate_shard` läuft, wenn die ungefähre Shard-Größe `sharding.shard_split_threshold_bytes` erreicht (Vorgabe 4 GiB), oder wenn `TriggerShardSplit` eine Prüfung erzwingt.

`RaftShardSplitWorker::discover_split_key`:

- Braucht mindestens zwei lebende Schlüssel.
- Quota- und Sekundärindex-Shards teilen an einem Byte-Mittelpunkt.
- `MetaTable` summiert Bytes je Elternkennung. Wenn ein Elternteil mehr als 85 % dieser Bytes besitzt (`dominant_bytes * 100 > total_bytes * 85`), oder der Shard nur ein Elternteil hat, ist der Kandidat `Jumbo`.
- Sonst wählt er eine Elternkennungsgrenze nahe der halben Bytezahl. Der Split-Schlüssel ist ein PID-Präfix, sodass `_attr_` und die direkten Kinder eines Elternteils zusammenbleiben.
- Ist die Route für dieses Elternteil schon `DualModeFineGrained`, ist der Split-Schlüssel ein `(pid, name)`-Mittelpunkt, ohne `_attr_`.

Ein Jumbo-Kandidat **verschiebt** das Shard-Verzeichnis nicht. `JumboIsolationPlanner` schreibt `ShardHealthState::JumboIsolated { dedicated_node, dominant_pid }` auf die Route. Der gewählte Knoten ist der registrierte Knoten mit den wenigsten Jumbo-Routen, dann mit den wenigsten übrigen Routen. Ist nur der Bootstrap-Knoten registriert, ist dieser Knoten dieser Prozess.

`enable_dual_mode_split` speichert `split_mode = DualModeFineGrained` und `dual_mode_pid`. Es kopiert selbst keine Schlüssel. Ein späterer Split nutzt die Regel `(pid, name)`. Der Simulator im Speicher (`CellLocalManager` in `control_plane.rs`) ist ein anderer Typ: Sein Jumbo-Pfad liefert `UnsplittableJumboShard`, und sein „auf 3 Wähler skalieren“ ändert nur eine `BTreeMap`. Die Tests `T1`–`T6` üben diesen Simulator.

Split-Pläne liegen in `CELL_MANAGER_STATE`. Absturzpunkte, die `split_crash_recovery_tests` abdeckt, setzen einen Plan beim nächsten `maybe_split_or_isolate_shard` fort oder nehmen ihn zurück. Kind-Shard-Verzeichnisse entstehen unter demselben Baum `be/shards`.

## Control-Plane-RPCs, die weniger tun als der Name sagt

| RPC | Verhalten in diesem Baum |
| :--- | :--- |
| `RegisterNode` | Fügt `(node_id, grpc_addr)` in das `CELL_MANAGER_STATE` dieser Zelle ein |
| `StoreHeartbeat` | Schreibt keine vom Aufrufer gelieferten Shard-Deskriptoren in den Routen-Cache. `metadbd` sendet diesen RPC nie |
| `GetRoutes` | `cell_id = 0` listet jede zwischengespeicherte Route. Eine unbekannte `cell_id` ungleich null liefert einen Fehler statt einer leeren Liste |
| `AllocateIdBlock` | Zuteilen aus dem lokalen dauerhaften Zuteiler |
| `AllocSplitPlan` / `ActivateSplit` | Leitet an den lokalen Zellmanager weiter |
| `TriggerShardSplit` | Erzwingt `maybe_split_or_isolate_shard` auf einem lokalen Shard |
| `EnableDualModeSplit` | Setzt das lokale Routenflag. `split_name` in der Anfrage wird ignoriert |
| `ScaleShardReplicas` | Ruft `scale_out_replicas` auf, wenn diese Zelle existiert und genug Knoten registriert sind |
| `RegisterCell` | Stores a remote cell after its routes cover the requested range. This process does not forward those keys |
