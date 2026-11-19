# Betrieb, `metadb-ctl` CLI & Leistungsoptimierung (Deutsch)

* **Zurück zur Übersicht**: [Deutsch Übersicht](./README.md) | [Hub](../../README.md)

---

## 1. Administrations-CLI `metadb-ctl` `[Implementiert]`

```bash
# Cluster-, Cell-, BE-Knoten- und Raft-Watermark-Status prüfen
metadb-ctl --endpoint 127.0.0.1:7800 cluster status

# Shards, Schlüsselbereiche, ShardEpoch und Jumbo-Isolierung anzeigen
metadb-ctl --endpoint 127.0.0.1:7800 shard list --cell-id 1

# Manuelle Prüfung eines PID-Grenz-Splits auslösen
metadb-ctl --endpoint 127.0.0.1:7800 shard split --shard-id 12

# DualModeFineGrained (PID, name)-Splitting für ein extremes Jumbo-Verzeichnis aktivieren
metadb-ctl --endpoint 127.0.0.1:7800 shard enable-dual-mode --shard-id 42 --pid 900100

# Replikationsfaktor im laufenden Betrieb auf 3-Voter erhöhen
metadb-ctl --endpoint 127.0.0.1:7800 raft scale-replicas --cell-id 1 --target-voters 3

# SQL-Vorlage ausführen oder Hotspot-Benchmark starten
metadb-ctl --endpoint 127.0.0.1:7800 sql "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
metadb-ctl --endpoint 127.0.0.1:7800 bench --workload hotspot-create --concurrency 64 --ops 100000
```

---

## 2. Umgang mit Jumbo Shards & Dual-Mode-Splitting

1. **`JumboIsolated` route flag**: When one parent owns more than 85% of the shard's `MetaTable` bytes, or the shard has a single parent, `JumboIsolationPlanner` writes `dedicated_node` from nodes already registered in this cell. It does not migrate co-hosted peer shards onto another host. With one node, the flag names this process.
2. **Feingranulares `DualModeFineGrained`-Splitting `(PID, name)`**: Überschreitet ein Verzeichnis selbst einen dedizierten Host, teilt `DualModeFineGrained` den Bereich entlang `(PID, name)` auf. Dank lokaler `[0x04] DirShardDelta`-Einträge bleiben `create_file` und `unlink` sperrfreie `1-RTT`-Einzel-Shard-Operationen, während `dirstat` per `1-Hop`-Scatter-Gather exakt zusammengeführt wird.

---

## 3. Wichtige Speicher- und Kompaktierungsparameter (`metadb-storage`)

* **`mvcc_retention_ms` (Standard `5000` ms)**: Haltedauer für aktive `SnapshotGuard`-Leser und Schreib-Koaleszierung. Nutzen Sie für große Verzeichnisse stets die Cursor-Paginierung (SQL-Vorlage #10) statt langlaufender Einzel-Scans.
* **`memtable_size_bytes` (Standard `32 MiB`)**: Bei schreibintensiven KI-Checkpoint-Workloads auf `64 MiB` erhöhen, um noch mehr kurzlebige MVCC-Versionen im RAM zu verwerfen.
* **`block_cache_bytes` (Standard `512 MiB`)**: Auf dedizierten BE-Knoten `25 %–40 %` des Host-RAMs zuweisen.
* **`locality_tombstone_threshold` (Standard `128`)**: Schwellenwert physischer LSM-Tombstones pro 9-Byte-Präfix `[table_prefix][PID]` (kombiniert mit Quote `>= 0.50`) für gezielte Bottommost-Kompaktierung; bei massiven TTL-Löschwellen auf `64` senken.

---

## 4. Fehlerbehebung (Troubleshooting)

* **`MetaDbError::TransactionTimeout`**: Ein `SnapshotGuard` wurde länger als `5.000 ms` gehalten. Stellen Sie Verzeichnis-Scans auf Cursor-Paginierung (`WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT 1000`) um.
* **`MetaDbError::ClockSkewExceeded`**: Zeitabweichung zwischen Knoten `> 500 ms`; `chrony` / NTP-Synchronisation prüfen.
* **`MetaDbError::EpochMismatch` / `NotLeader`**: Normale Übergangsmeldung bei Shard-Splits oder Leader-Wechseln; SDK und `metadb-proxy` aktualisieren `MetaCache` und wiederholen den Aufruf automatisch.
* **Wachsende `.raftlog`-Belegung**: Prüfen Sie die lokale NVMe-Flush-Latenz (`state_flushed_index`). Nicht erreichbare entfernte `QuotaTable`-Shards blockieren die Log-Bereinigung **nicht**, da überfällige `AuxOutbox`-Einträge beim L0-Flush lokal nach `[0x07] SpilledAuxOutbox` ausgelagert werden.
