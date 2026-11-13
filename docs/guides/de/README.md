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

## Funktions- und Reifegradmatrix (Aktueller Prototyp vs. Phasen 1–6)

| Funktionsbereich | Aktueller Stand (`v0.1.0-prototype`) | Ziel-Meilenstein (Produktion) |
| :--- | :--- | :--- |
| Eingebettetes Rust-SDK (`MetaDbEngine`) | **Implementiert** (synchroner RAM-Prototyp) | **Phase 6** (asynchrones `tokio` + persistentes LSM/OpenRaft) |
| `(PID, "_attr_")`-Kolokation & `HLC`-Uhr | **Implementiert** (`metadb-core`) | **Phase 1** (88-Byte-CRC32C-Format + 500-ms-Driftschutz) |
| Persistenter Speicher (WAL-freies Rust-SSTable-LSM) | Im RAM simuliert (`BTreeMap`) | **Phase 2** (`LsmStorageBackend`, `MANIFEST`, Bloom, LRU-Cache) |
| In-Memory-MVCC (`SnapshotGuard`) & `LocalityCompactor` | RAM-Prototyp | **Phase 2** (`ActiveSnapshotRegistry` + Bottommost-Kompaktierung) |
| Multi-Raft-Konsens (`1-Voter -> 3-Voter` + `ReadIndex`) | Im RAM simuliert (`VecDeque`) | **Phase 3** (`openraft` + `.raftlog`-Segmente + `tonic`) |
| `1-RTT`-Pushdown, `AsyncAttrAggregator` & `AuxOutbox` | **Implementiert** (Einzelprozess-RAM) | **Phase 4** (koordinierte Flush-Barriere + lokaler `[0x07]`-Überlauf) |
| Shard-übergreifendes 2PC (`Rename`, `Mkdir`, `Rmdir`) | RAM-Prototyp für `Rename` | **Phase 4** (primärverankertes `TxnRecord` + `WriteIntent`-Recovery) |
| `PID`-Grenz-Split, Jumbo-Isolierung & `DualMode` | **Implementiert** (RAM-Split & Isolierung) | **Phase 5** (`ShardEpoch`-Fencing + `DirShardDelta`-Scatter-Gather) |
| Leichtgewichtiger SQL-Vorlagen-Compiler | **Implementiert** (8 literale SQL-Muster) | **Phase 5** (alle 20 parametrisierten `?`-SQL-Vorlagen) |
| Server-Daemon `metadbd` & `metadb-ctl` CLI | Geplant | **Phase 6** (`crates/metadb-server/src/bin/`) |

---

## Schnellstart: Eingebettetes Rust-SDK (`v0.1.0-prototype` sofort nutzbar)

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
