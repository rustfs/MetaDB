# Client-APIs, gRPC-Dienste & 20 SQL-Vorlagen (Deutsch)

* **Zurück zur Übersicht**: [Deutsch Übersicht](./README.md) | [Hub](../../README.md)

---

## 1. Client-Schnittstellen im Überblick

1. **Eingebettetes Rust-SDK (`metadb-server`)**: Direkte In-Process-Aufrufe ohne Serialisierung (`PersistentMetaDbCluster` asynchrone persistente API und `MetaDbEngine` synchrone API).
2. **Typisierte `tonic` gRPC-Dienste (`metadb.v1` — Implementiert)**:
   * `MetadataService`: `CreateFile`, `Mkdir`, `Unlink`, `Rmdir`, `SetAttr`, `Lookup`, `DirStat`, `ListDir`, `ExecuteSql`, `GetQuota`, `SetQuotaLimit`, `ListQuotas`, `GetSecondaryIndex`, `ScanSecondaryIndex`.
   * `TransactionService`: `Rename` (automatisch `1-RTT` lokal oder primärverankertes `2PC` über Shards hinweg) sowie `Prepare2pc`, `CommitPrimary2pc`, `CommitSecondary2pc`, `Abort2pc`, `CheckTxnStatus`, `ApplyAuxBatch`.
   * `ControlPlaneService`: `GetRoutes`, `AllocateIdBlock`, `StoreHeartbeat`, `TriggerShardSplit`, `EnableDualModeSplit`, `ScaleShardReplicas`, `RegisterCell`.
3. **20 Leichtgewichtige SQL-Vorlagen (`metadb-proxy`)**: Direkte Kompilierung in `<5 µs` ohne allgemeinen SQL-Optimizer (alle 20 literalen, `?`-parametrisierten und escapteten SQL-Vorlagen sind implementiert und getestet).

> **Reservierter Eintragsname**: Der Name `'_attr_'` (sowie Namen mit `'\0'`) ist für reguläre Dateien/Verzeichnisse gesperrt (`MetaDbError::ReservedEntryName`) und ausschließlich für Vorlage #7 (`DirStat`) reserviert.

---

## 2. Vollständige Referenz der 20 Metadaten-SQL-Vorlagen

| # | SQL-Vorlage | Semantik & Ausführungspfad | Status |
| :--- | :--- | :--- | :--- |
| **1** | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | Datei anlegen (`1-RTT`-Pushdown + asynchrone Deltas) | Implementiert |
| **2** | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | Verzeichnis anlegen (`1-RTT` kolokiert oder `2PC`) | Implementiert |
| **3** | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | Datei löschen `Unlink` (`1-RTT`) | Implementiert |
| **4** | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | Leeres Verzeichnis löschen (autoritativer Kinder-Scan) | Implementiert |
| **5** | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | Exklusives Umbenennen (`AlreadyExists`, falls Ziel existiert) | Implementiert |
| **6** | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | Atomares POSIX-Überschreib-Rename | Implementiert |
| **7** | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | Linearisierbares `DirStat` (`ReadIndex` + Reconciliation) | Implementiert |
| **8** | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | Linearisierbare Punktabfrage `Lookup` | Implementiert |
| **9** | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | Verzeichnis-Scan (mit Standard-Seitenlimit) | Implementiert |
| **10** | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | Cursor-paginiertes `ListDir` (für große Verzeichnisse empfohlen) | Implementiert |
| **11** | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | Präfix-gefiltertes `ListDir` (`'prefix%'`) | Implementiert |
| **12** | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | Berechtigungen ändern `chmod` (`1-RTT`) | Implementiert |
| **13** | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | Eigentümer ändern `chown` + asynchroner Quota-Transfer | Implementiert |
| **14** | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | Objektgröße und `FID` aktualisieren | Implementiert |
| **15** | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | Änderungszeitstempel setzen `utimes` (`1-RTT`) | Implementiert |
| **16** | `SELECT * FROM QuotaTable WHERE uid = ?` | Benutzer-Quota (`used_bytes`, `used_inodes`, Limit) abfragen | Implementiert |
| **17** | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | Quota-Limit setzen oder aktualisieren | Implementiert |
| **18** | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | Paginierter Quota-Audit-Scan | Implementiert |
| **19** | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | Rückwärtsindex `FID -> (PID, name, create_time)` | Implementiert |
| **20** | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | Cursor-paginierter GC-/Scrubber-Scan | Implementiert |
