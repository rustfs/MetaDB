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

## Matrice de Maturité (Prototype actuel vs. Phases 1–6)

| Domaine Fonctionnel | État Actuel (`v0.1.0-prototype`) | Jalon de Production Cible |
| :--- | :--- | :--- |
| SDK Rust embarqué (`MetaDbEngine`) | **Implémenté** (prototype synchrone en RAM) | **Phase 6** (`tokio` asynchrone + LSM / OpenRaft persistant) |
| Colocalisation `(PID, "_attr_")` & horloge `HLC` | **Implémenté** (`metadb-core`) | **Phase 1** (encodage 88 octets CRC32C + garde de dérive 500 ms) |
| Stockage persistant (LSM SSTable pur Rust sans WAL) | Simulé en mémoire (`BTreeMap`) | **Phase 2** (`LsmStorageBackend`, `MANIFEST`, Bloom, cache LRU) |
| MVCC en mémoire (`SnapshotGuard`) & `LocalityCompactor` | Prototype en RAM | **Phase 2** (`ActiveSnapshotRegistry` + compaction ciblée) |
| Consensus Multi-Raft (`1-Voter -> 3-Voter` + `ReadIndex`) | Simulé en mémoire (`VecDeque`) | **Phase 3** (`openraft` + segments `.raftlog` CRC32C + `tonic`) |
| Pushdown `1-RTT`, `AsyncAttrAggregator` & `AuxOutbox` | **Implémenté** (processus unique RAM) | **Phase 4** (barrière de flush coordonnée + débordement `[0x07]`) |
| Transactions 2PC inter-shards (`Rename`, `Mkdir`, `Rmdir`) | Prototype `Rename` en RAM | **Phase 4** (`TxnRecord` ancré sur le shard primaire + `WriteIntent`) |
| Découpage sur frontière `PID`, isolation Jumbo & `DualMode` | **Implémenté** (découpage & isolation RAM) | **Phase 5** (clôture `ShardEpoch` + agrégation `DirShardDelta`) |
| Compilateur léger de modèles SQL | **Implémenté** (8 modèles SQL littéraux) | **Phase 5** (20 modèles SQL paramétrés `?` avec échappement) |
| Démon serveur `metadbd` & CLI `metadb-ctl` | Planifié | **Phase 6** (`crates/metadb-server/src/bin/`) |

---

## Démarrage rapide : SDK Rust embarqué (`v0.1.0-prototype` disponible)

```toml
[dependencies]
metadb-core = { path = "crates/metadb-core" }
metadb-server = { path = "crates/metadb-server" }
```

```rust
use metadb_core::ROOT_INODE_ID;
use metadb_server::{MetaDbConfig, MetaDbEngine};

fn main() -> metadb_core::Result<()> {
    // 1. Démarrer un nœud Stage 1 (initialise la racine `/` avec PID = 1)
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
