# MetaDB-Benutzerhandbuch (Deutsch)

* **Sprachen**: [English](../en/README.md) | [日本語](../ja/README.md) | [Français](../fr/README.md) | [简体中文](../zh/README.md) | **Deutsch**
* **Dokumentations-Hub**: [`docs/README.md`](../../README.md)

**MetaDB** (`rustfs/MetaDB`) ist eine zu **100 % in reinem Rust** geschriebene, dateisystemstruktur-bewusste verteilte transaktionale Metadaten-Datenbank für [RustFS](https://github.com/rustfs), basierend auf den Erkenntnissen aus Baidus Produktionsarchitektur für Billionen von Metadatensätzen ([*MetaDB: Putting File-System Structure Back into Distributed Databases*, `arXiv:2610.32247v1`](https://arxiv.org/html/2610.32247v1)).

---

## Inhaltsverzeichnis

1. **Übersicht & Eingebetteter Schnellstart** *(diese Seite)*
2. [**Bereitstellung & Online-Skalierung**](./deployment.md) — Einzelknoten-Betrieb (`metadbd`), unterbrechungsfreie `1-Voter -> 3-Voter` Multi-Raft-Erweiterung und Multi-Cell-Topologie (`GlobalManager`, `CellLocalManager`, `MetaCache`).
3. [**Client-APIs, gRPC-Dienste & 20 SQL-Vorlagen**](./client-apis-and-sql.md) — Eingebettetes Rust-SDK, `tonic` gRPC-Schnittstellen und Referenz aller 20 Metadaten-SQL-Vorlagen.
4. [**Betrieb, `metadb-ctl` CLI & Leistungsoptimierung**](./operations-and-tuning.md) — CLI-Verwaltung, Jumbo-Shard-Isolierung & Dual-Mode-Splitting, `SnapshotGuard`-MVCC- & `LocalityCompactor`-Tuning sowie Fehlerbehebung.

---

## Funktions- und Reifegradmatrix (Phasen 1–6 Implementiert & Verifiziert)

| Funktionsbereich | Repository-Status | Crate / Binärdatei |
| :--- | :--- | :--- |
| Eingebettete Rust-SDKs (`PersistentMetaDbCluster` & `MetaDbEngine`) | **Implementiert & Getestet** (asynchron persistent + synchron RAM) | `metadb-server` |
| `(PID, "_attr_")`-Kolokation, `0x01..0x07` & `HLC`-Uhr | **Implementiert & Getestet** (88-Byte-CRC32C + 500-ms-Driftschutz) | `metadb-core` |
| Persistenter Speicher (WAL-freies Rust-SSTable-LSM) | **Implementiert & Getestet** (`LsmStorageBackend`, `MANIFEST`, Bloom, LRU-Cache) | `metadb-storage` |
| In-Memory-MVCC (`SnapshotGuard` `ReadCut`) & `LocalityCompactor` | **Implementiert & Getestet** (`ActiveSnapshotRegistry` + Bottommost-Kompaktierung) | `metadb-storage` |
| Multi-Raft-Konsens (`1-Voter -> 3-Voter` + `ReadIndex`) | **Implementiert & Getestet** (`openraft` 0.10 + `.raftlog`-Segmente + `tonic`) | `metadb-raft` |
| `1-RTT`-Pushdown, `ReplicatedAttrAggregator` & `AuxOutbox` | **Implementiert & Getestet** (koordinierte Flush-Barriere + `[0x07]`-Überlauf) | `metadb-tx` |
| Shard-übergreifendes 2PC (`Rename`, `Mkdir`, `Rmdir`, `SetAttr`) | **Implementiert & Getestet** (primärverankertes `TxnRecord` + `WriteIntent`) | `metadb-tx` |
| `PID`-Grenz-Split, Jumbo-Isolierung & `DualMode` | **Implementiert & Getestet** (`ShardEpoch`-Fencing + `DirShardDelta`) | `metadb-cluster` |
| Leichtgewichtiger 20-SQL-Vorlagen-Compiler | **Implementiert & Getestet** (alle 20 parametrisierten `?`-SQL-Vorlagen) | `metadb-proxy` |
| Server-Daemon `metadbd` & `metadb-ctl` CLI | **Implementiert & Getestet** (`crates/metadb-server/src/bin/`) | `metadb-server` |

---

## Schnellstart: Eingebettetes Rust-SDK (`PersistentMetaDbCluster` & `MetaDbEngine`)

```toml
[dependencies]
metadb-core = { path = "crates/metadb-core" }
metadb-server = { path = "crates/metadb-server" }
```

```rust
use metadb_core::ROOT_INODE_ID;
use metadb_server::{MetaDbConfig, MetaDbEngine};

fn main() -> metadb_core::Result<()> {
    // 1. Stage-1-Einzelknoten starten (initialisiert Wurzelverzeichnis `/` mit PID = 1)
    let engine = MetaDbEngine::new_single_node(MetaDbConfig::default());

    // 2. Unterverzeichnis `/checkpoints` und Datei per 1-RTT-Pushdown anlegen
    engine.mkdir(ROOT_INODE_ID, "checkpoints", 100, /*uid=*/ 1000, 0o755)?;
    engine.create_file(100, "step_0001.pt", 2001, 9001, 1000, 0o644, 104_857_600)?;

    // 3. Linearisierbares dirstat dank Read-Path-Reconciliation (auch vor dem Hintergrund-Flush)
    let dir_stat = engine.dirstat(100)?;
    assert_eq!(dir_stat.num_children, 1);

    // 4. Aktive Einträge auflisten (interner Schlüssel `_attr_` wird automatisch ausgeblendet)
    let entries = engine.listdir(100)?;
    println!("Einträge in /checkpoints: {}", entries.len());

    // 5. Hintergrundwartung ausführen (Attribut-Deltas, Quotas/Indizes & MVCC flushen)
    engine.run_background_maintenance()?;
    Ok(())
}
```

### Asynchrones Persistentes & gRPC-Cluster-SDK (`PersistentMetaDbCluster`)

`PersistentMetaDbCluster::open_and_serve(config).await?` startet den absturzsicheren Speicher auf Basis von `LsmStorageBackend` und OpenRaft inklusive Hintergrundwartung und gRPC-Endpunkt:

```rust
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};

let (cluster, grpc_addr) = PersistentMetaDbCluster::open_and_serve(
    ServerTomlConfig::from_toml_file("/etc/metadb/standalone.toml")?,
).await?;
let dir = cluster.mkdir_auto_id(1, "checkpoints", 1000, 0o755).await?;
let file = cluster.create_file_auto_id(dir.attr.inode_id, "step_0002.pt", 1000, 0o644, 4096).await?;
cluster.shutdown().await?;
```
