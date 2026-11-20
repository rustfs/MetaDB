# Client APIs, gRPC Services & 20 Metadata SQL Templates (English)

* **Back to Guide Index**: [English Overview](./README.md) | [Hub](../../README.md)

MetaDB exposes three client access surfaces tailored to different integration needs:
1. **Embedded Rust SDK (`metadb-server`)** — Zero-serialization direct calls for co-located Rust services (`PersistentMetaDbCluster` async persistent API and `MetaDbEngine` synchronous API).
2. **Typed `tonic` gRPC APIs (`metadb-proto` & `metadb-server`)** — High-throughput multiplexed RPCs for networked RustFS data nodes and proxies (`MetadataService`, `TransactionService`, `ControlPlaneService`, `RaftTransportService`).
3. **20 Lightweight SQL Templates (`metadb-proxy`)** — `sql_templates_tests` covers all 20 literal, `?`-parameterized, and escaped templates. A compile time under `5µs` is not a measurement of this repository.

---

## 1. gRPC Service Surface (`metadb.v1`) `[Implemented]`

`[Implemented]` means the process that hosts the shard serves these RPCs. A `global-manager` does not allocate ids or forward a key. `RegisterCell` persists a remote cell on the directory process after `GetRoutes` shows that cell covering the requested range exactly. It does not forward later metadata calls. `be` serves the replicas it hosts. `proxy` forwards these calls to one upstream and does not host Raft.

### `MetadataService` (`proto/metadb/v1/metadata.proto`)
* **Namespace Mutations (`1-RTT` Pushdown)**: `CreateFile`, `Mkdir`, `Unlink`, `Rmdir`, `SetAttr` (`chmod`, `chown`, `truncate`, `utimes`).
* **Linearizable Namespace Reads (`ReadIndex`)**: `Lookup`, `DirStat` (with Read-Path Reconciliation), `ListDir` (cursor-paginated via `after_name` + `limit`), `ExecuteSql`.
* **Quota & Reverse Index Queries**: `GetQuota`, `SetQuotaLimit`, `ListQuotas`, `GetSecondaryIndex` (`FID -> (PID, name)`), `ScanSecondaryIndex`.

### `TransactionService` (`proto/metadb/v1/transaction.proto`)
* **Client Entry**: `Rename(src_pid, src_name, dst_pid, dst_name, overwrite)` — automatically executes in `1 RTT` within a single shard or via Primary-Anchored 2PC across shards.
* **Internal Shard-to-Shard RPCs**: `Prepare2pc`, `CommitPrimary2pc`, `CommitSecondary2pc`, `Abort2pc`, `CheckTxnStatus`, `ApplyAuxBatch`.

### `ControlPlaneService` (`proto/metadb/v1/control_plane.proto`)
* `GetRoutes` (populates `MetaCache`), `AllocateIdBlock`, `StoreHeartbeat`, `TriggerShardSplit`, `EnableDualModeSplit`, `ScaleShardReplicas`, `RegisterCell`.

---

## 2. Complete Reference: 20 Metadata SQL Templates (`metadb-proxy`)

MetaDB intentionally rejects arbitrary multi-table `JOIN`s and unbounded analytical scans (`>5s`), compiling the 20 canonical metadata SQL patterns directly into storage execution plans. Positional `?` parameters and SQL standard single-quote escaping (`'it''s.txt'`) are supported across all templates.

> **Reserved Name Rule**: Creating, looking up as a regular child, or renaming a user entry to the literal name `'_attr_'` (or containing `'\0'`) returns `MetaDbError::ReservedEntryName`. Use Template #7 (`name = '_attr_'`) exclusively for `DirStat`.

### A. Namespace Write & Rename Templates (`MetaTable`)

| # | SQL Template | Semantics & Execution Path | Status |
| :--- | :--- | :--- | :--- |
| **1** | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | Create file (`1-RTT` Pushdown + async `AttrDelta` & `AuxOutbox`) | Implemented |
| **2** | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | Create directory (`1-RTT` colocated or `2PC`) | Implemented |
| **3** | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | Unlink regular file (`1-RTT` Pushdown) | Implemented |
| **4** | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | Remove empty directory (authoritative live-child scan) | Implemented |
| **5** | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | Exclusive rename (`AlreadyExists` if destination exists) | Implemented |
| **6** | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | POSIX atomic overwrite rename (reclaims overwritten file quota & FID) | Implemented |

### B. Namespace Point Read & Paginated Scan Templates (`MetaTable`)

| # | SQL Template | Semantics & Execution Path | Status |
| :--- | :--- | :--- | :--- |
| **7** | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | Linearizable `DirStat` (`ReadIndex` + Read-Path Reconciliation) | Implemented |
| **8** | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | Linearizable child `Lookup` (`ReadIndex`) | Implemented |
| **9** | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | Full directory scan (bounded by default page limit) | Implemented |
| **10** | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | Cursor-paginated `ListDir` (recommended for large directories) | Implemented |
| **11** | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | Prefix-filtered `ListDir` (trailing `'prefix%'` pattern) | Implemented |

### C. Inode Attribute Update Templates (`MetaTable`)

| # | SQL Template | Semantics & Execution Path | Status |
| :--- | :--- | :--- | :--- |
| **12** | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | `chmod` permission update (`1-RTT` Pushdown) | Implemented |
| **13** | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | `chown` ownership transfer + async `QuotaDelta` (`-old_uid`, `+new_uid`) | Implemented |
| **14** | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | Commit object size / backing `FID` + parent/quota/index deltas | Implemented |
| **15** | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | `utimes` explicit timestamp update (`1-RTT` Pushdown) | Implemented |

### D. Quota & Reverse Secondary Index Templates (`QuotaTable` & `SecondaryIndexTable`)

| # | SQL Template | Semantics & Execution Path | Status |
| :--- | :--- | :--- | :--- |
| **16** | `SELECT * FROM QuotaTable WHERE uid = ?` | Read per-user `used_bytes`, `used_inodes`, and `quota_limit_bytes` | Implemented |
| **17** | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | Configure or update user quota limit | Implemented |
| **18** | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | Paginated user quota audit scan | Implemented |
| **19** | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | Reverse lookup `FID -> (PID, name, create_time)` | Implemented |
| **20** | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | Cursor-paginated data-plane GC / scrubber scan | Implemented |
