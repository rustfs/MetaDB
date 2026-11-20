# Guide d'implémentation MetaDB

[English](../en/README.md) · [Deutsch](../de/README.md) · [日本語](../ja/README.md) · [简体中文](../zh/README.md)

Ces pages décrivent l'arbre `rustfs/MetaDB` tel qu'il est implémenté. Elles constituent le guide d'exploitation et de développement de ce dépôt. Les notes de conception qui décrivent une topologie plus large se trouvent dans [`Architecture.md`](../../Architecture.md), [`docs/designs/`](../designs/SITEMAP.md) et les guides antérieurs sous [`docs/guides/`](../README.md). En cas de désaccord entre ces documents et le code, suivre cet ensemble.

## Ce qu'est ce dépôt

MetaDB est un moteur de métadonnées entièrement écrit en Rust pour les espaces de noms de fichiers et d'objets, destiné à [RustFS](https://github.com/rustfs). Le modèle de données suit la topologie des répertoires : un répertoire et ses enfants directs partagent un préfixe de clé, de sorte qu'une création, une recherche, un listage ou une suppression dans un seul répertoire reste sur un seul shard. La conception est adaptée de Baidu et de l'Université de Pékin, *MetaDB: Putting File-System Structure Back into Distributed Databases*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1). Les chiffres de production de cet article décrivent le déploiement de Baidu. Ce dépôt ne publie pas de mesure correspondante.

La version de l'espace de travail est `0.1.0` (Apache-2.0). La chaîne d'outils Rust est épinglée à **1.100.0** dans `rust-toolchain.toml`.

## Ce qu'un processus exécute réellement

`metadbd` et `PersistentMetaDbCluster::open_and_serve` démarrent un seul processus **all-in-one** :

- un répertoire de données
- une cellule (`node.cell_id`, valeur par défaut `1`)
- un votant OpenRaft par shard (ce processus)
- un écouteur gRPC qui sert `MetadataService`, `TransactionService`, `ControlPlaneService` et `RaftTransportService`
- une boucle d'arrière-plan toutes les 250 ms qui vide les files auxiliaires, reprend les transactions 2PC, vérifie la politique de découpage et prend des instantanés des journaux Raft

`ServerRole` accepte aussi l'analyse de `be`, `proxy`, `cell-manager` et `global-manager`. `open_and_serve` rejette tout rôle autre que `all-in-one`. Un second `metadbd` au répertoire vide, avec `[cluster] join` (ou `--join` / `--cell-manager`) pointé sur le leader courant, rejoint les groupes Raft de cette cellule : le leader l'ajoute comme learner puis le promeut voter. Un répertoire qui a déjà un état de cellule rouvre le Raft local et ne rejoint pas à nouveau. `RegisterCell` stores a remote cell after its routes cover the requested range and does not forward keys. `metadb-ctl cell register` without a range still sends `[01, 04)` and exits non-zero against a default cell. `GetRoutes` pour un `cell_id` non nul inconnu renvoie une erreur. `cell_id = 0` liste encore toutes les routes en cache.

Les découpages de shards, les découpages `(PID, name)` en mode double et les indicateurs de route jumbo s'exécutent bien dans ce processus unique. La promotion de répliques à trois votants est implémentée sur `DurableCellManager::scale_out_replicas` et couverte par des tests Raft dans un seul processus. Ce n'est pas un déploiement de trois processus `metadbd`. Voir [Exploitation](./operations.md).

## Plan du document

| Page | Contenu |
| :--- | :--- |
| [Architecture](./architecture.md) | Crates, clés, stockage, Raft, transactions, découpage, et les deux moteurs |
| [Compilation et exécution](./build-and-run.md) | Chaîne d'outils, binaires, bibliothèque embarquée, arrêt |
| [Configuration](./configuration.md) | Champs TOML, valeurs par défaut, et champs que le processus n'applique pas |
| [Exploitation](./operations.md) | `metadb-ctl`, métriques, montée en charge, découpages, limites, lacunes de documentation |
| [Tests et bancs](./testing.md) | `cargo test`, ce que chaque suite exerce réellement, `metadb-ctl bench` |

## Crates de l'espace de travail

| Crate | Ce que fait le code |
| :--- | :--- |
| `metadb-core` | Codecs de clés `0x01`–`0x07`, `ShardEpoch`, horloge logique hybride (garde de dérive par défaut 500 ms), `InodeAttr` de 88 octets |
| `metadb-proto` | Protobuf et services tonic listés dans [Architecture](./architecture.md) |
| `metadb-storage` | LSM SSTable sans WAL, `MANIFEST`, cache de blocs LRU à 16 voies, MVCC en mémoire, compaction par localité |
| `metadb-raft` | Groupes `openraft` 0.10, `.raftlog` segmenté en CRC32C, instantanés, changement d'appartenance learner vers voter |
| `metadb-tx` | Mutations en pushdown, deltas d'attributs, file auxiliaire, 2PC ancré sur le primaire |
| `metadb-cluster` | Gestionnaires durables de cellule et global, allocateur d'identifiants, cache de routes, worker de découpage, planificateur jumbo |
| `metadb-proxy` | Analyseur lexical et syntaxique des 20 modèles SQL, routeur sans état vers la couche transaction |
| `metadb-server` | `PersistentMetaDbCluster`, `MetaDbEngine::new_single_node` en mémoire, `metadbd`, `metadb-ctl`, texte Prometheus |

## Deux façons d'appeler le moteur

`MetaDbEngine::open` et `PersistentMetaDbCluster::open_and_serve` ouvrent un répertoire de données, le LSM et OpenRaft, et lient gRPC. C'est le moteur à exécuter.

`MetaDbEngine::new_single_node` construit un simulateur en mémoire. Il n'ouvre ni répertoire de données, ni LSM, ni groupe OpenRaft. Les tests `T1`–`T6` de `crates/metadb-server/tests/integration_tests.rs` l'utilisent, y compris l'étape « 3 votants » de `T6`, qui ne modifie qu'une table d'appartenance en mémoire.
