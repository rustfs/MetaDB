# Guide Utilisateur MetaDB (Français)

* **Langues**: [English](../en/README.md) | [日本語](../ja/README.md) | **Français** | [简体中文](../zh/README.md) | [Deutsch](../de/README.md)
* **Portail documentaire**: [`docs/README.md`](../../README.md)

**MetaDB** (`rustfs/MetaDB`) est un moteur de base de données transactionnelle distribuée de métadonnées écrit **100 % en Rust pur**, conçu autour de la topologie des systèmes de fichiers pour [RustFS](https://github.com/rustfs) et inspiré de l'architecture à l'échelle du millier de milliards d'entrées de Baidu ([*MetaDB: Putting File-System Structure Back into Distributed Databases*, `arXiv:2610.32247v1`](https://arxiv.org/html/2610.32247v1)).

---

## Sommaire du Guide

1. **Présentation & Démarrage rapide embarqué** *(cette page)*
2. [**Déploiement & Montée en charge en ligne**](./deployment.md) — Nœud autonome `metadbd`, promotion Multi-Raft sans interruption (`1-Voter -> 3-Voter`), et topologie Multi-Cell (`GlobalManager`, `CellLocalManager`, `MetaCache`).
3. [**APIs Clientes, Services gRPC & 20 Modèles SQL**](./client-apis-and-sql.md) — SDK Rust embarqué, services gRPC `tonic`, et catalogue complet des 20 modèles SQL de métadonnées.
4. [**Exploitation, CLI `metadb-ctl` & Optimisation**](./operations-and-tuning.md) — Administration CLI, isolation des Jumbo Shards & découpage Dual-Mode, réglage `SnapshotGuard` MVCC / `LocalityCompactor`, et dépannage.

---

## Matrice de Maturité (Phases 1–6 Implémentées & Vérifiées)

| Domaine Fonctionnel | État du Dépôt | Crate / Binaire |
| :--- | :--- | :--- |
| SDKs Rust embarqués (`PersistentMetaDbCluster` & `MetaDbEngine`) | **Implémenté & Testé** (persistant asynchrone + RAM synchrone) | `metadb-server` |
| Colocalisation `(PID, "_attr_")`, codecs `0x01..0x07` & `HLC` | **Implémenté & Testé** (88 octets CRC32C + garde de dérive 500 ms) | `metadb-core` |
| Stockage persistant (LSM SSTable pur Rust sans WAL) | **Implémenté & Testé** (`LsmStorageBackend`, `MANIFEST`, Bloom, cache LRU) | `metadb-storage` |
| MVCC en mémoire (`SnapshotGuard` `ReadCut`) & `LocalityCompactor` | **Implémenté & Testé** (`ActiveSnapshotRegistry` + compaction ciblée) | `metadb-storage` |
| Consensus Multi-Raft (`1-Voter -> 3-Voter` + `ReadIndex`) | **Implémenté & Testé** (`openraft` 0.10 + `.raftlog` CRC32C + `tonic`) | `metadb-raft` |
| Pushdown `1-RTT`, `ReplicatedAttrAggregator` & `AuxOutbox` | **Implémenté & Testé** (barrière coordonnée + débordement `[0x07]`) | `metadb-tx` |
| Transactions 2PC inter-shards (`Rename`, `Mkdir`, `Rmdir`, `SetAttr`) | **Implémenté & Testé** (`TxnRecord` primaire + `WriteIntent`) | `metadb-tx` |
| Découpage sur frontière `PID`, isolation Jumbo & `DualMode` | **Implémenté & Testé** (clôture `ShardEpoch` + `DirShardDelta`) | `metadb-cluster` |
| Compilateur léger de 20 modèles SQL | **Implémenté & Testé** (20 modèles SQL paramétrés `?` avec échappement) | `metadb-proxy` |
| Démon serveur `metadbd` & CLI `metadb-ctl` | **Implémenté & Testé** (`crates/metadb-server/src/bin/`) | `metadb-server` |

---

## Démarrage rapide : SDK Rust embarqué (`PersistentMetaDbCluster` & `MetaDbEngine`)

```toml
[dependencies]
metadb-core = { path = "crates/metadb-core" }
metadb-server = { path = "crates/metadb-server" }
```

```rust
use metadb_core::ROOT_INODE_ID;
use metadb_server::{MetaDbConfig, MetaDbEngine};

fn main() -> metadb_core::Result<()> {
    // Simulateur en mémoire uniquement. Le moteur durable est MetaDbEngine::open.
    let engine = MetaDbEngine::new_single_node(MetaDbConfig::default());

    // 2. Créer un répertoire `/checkpoints` et un fichier en 1 RTT (Pushdown)
    engine.mkdir(ROOT_INODE_ID, "checkpoints", 100, /*uid=*/ 1000, 0o755)?;
    engine.create_file(100, "step_0001.pt", 2001, 9001, 1000, 0o644, 104_857_600)?;

    // 3. Lecture linéarisable via réconciliation en lecture avant même le flush asynchrone
    let dir_stat = engine.dirstat(100)?;
    assert_eq!(dir_stat.num_children, 1);

    // 4. Lister les entrées actives du répertoire (`_attr_` est filtré automatiquement)
    let entries = engine.listdir(100)?;
    println!("Entrées dans /checkpoints : {}", entries.len());

    // 5. Purger les deltas d'attributs, la file auxiliaire et la couche MVCC
    engine.run_background_maintenance()?;
    Ok(())
}
```

### SDK Asynchrone Persistant & gRPC (`MetaDbEngine::open` / `PersistentMetaDbCluster`)

`MetaDbEngine::open(config).await?` (ou `PersistentMetaDbCluster::open_and_serve(config).await?` pour obtenir également l'adresse gRPC liée) démarre le moteur persistant basé sur `LsmStorageBackend` et OpenRaft avec maintenance asynchrone et écoute gRPC :

```rust
use metadb_server::{MetaDbConfig, MetaDbEngine};

let engine = MetaDbEngine::open(
    MetaDbConfig::from_toml_file("/etc/metadb/standalone.toml")?,
).await?;
let dir = engine.mkdir_auto_id(1, "checkpoints", 1000, 0o755).await?;
let file = engine.create_file_auto_id(dir.attr.inode_id, "step_0002.pt", 1000, 0o644, 4096).await?;
engine.shutdown().await?;
```
