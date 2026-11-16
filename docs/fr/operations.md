# Exploitation

[Présentation](./README.md) · [English](../en/operations.md) · [Deutsch](../de/operations.md) · [日本語](../ja/operations.md) · [简体中文](../zh/operations.md)

## Répertoire de données

Après un amorçage avec `bootstrap_table_shards = true` :

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

Les identifiants de shard sont décimaux, complétés de zéros sur 20 chiffres. Les shards enfants créés par un découpage apparaissent comme d'autres répertoires sous `be/shards/`. `CELL_MANAGER_STATE` et `ID_ALLOCATOR_STATE` sont des trames CRC32C. Ne pas supprimer `NODE_SHARD_REGISTRY` tant que des répertoires de shards restent : l'hôte s'en sert pour reprendre les enfants de découpage.

Arrêter le processus avec Ctrl-C (ou `PersistentMetaDbCluster::shutdown`) avant de copier le répertoire. Un processus vivant tient un fichier `LOCK` LSM par shard.

## `metadb-ctl`

Options globales :

| Option | Défaut | Sens |
| :--- | :--- | :--- |
| `--endpoint` | `127.0.0.1:7800` | `hôte:port`, ou une URL qui commence déjà par `http://` ou `https://` |
| `--auth-token` | vide | Jeton bearer. Lu aussi depuis `METADB_AUTH_TOKEN` |

Le client ouvre un nouveau canal gRPC par commande. Il ne parle pas `RaftTransportService`.

### `cluster status`

Appelle `GetRoutes` avec `cell_id = 0`. Pour chaque shard il imprime `shard`, `cell`, `leader`, le nombre de votants, `epoch=(conf_ver, ver)`, `dual_mode` et `jumbo`. Il n'imprime ni l'espace disque libre, ni le QPS, ni les filigranes Raft.

### `shard list --cell-id <u64>`

Appelle `GetRoutes` pour cette cellule (utiliser `0` pour chaque route en cache). Imprime `shard`, `cell`, les octets bruts de début et de fin de clé, et l'époque `(conf_ver, version)`.

### `shard split --shard-id <u64>`

Appelle `TriggerShardSplit`, qui force un contrôle de découpage même si le shard est sous `shard_split_threshold_bytes`. Imprime `split_executed` et `child_shard_id`. `split_executed=false` signifie que le worker n'a pas trouvé de frontière (par exemple moins de deux clés, ou un candidat jumbo qui ne fait que basculer l'indicateur de route).

### `shard enable-dual-mode --shard-id <u64> --pid <u64>`

Appelle `EnableDualModeSplit`. Le CLI envoie un `split_name` vide. Le serveur stocke `dual_mode_pid` et ne découpe pas immédiatement.

### `raft scale-replicas --cell-id <u64> --target-voters <u32>`

Appelle `ScaleShardReplicas`. Le serveur exige que `target_voters` entrées soient déjà présentes dans la table des nœuds de cette cellule. Une cellule all-in-one fraîche a un nœud, donc `--target-voters 3` renvoie une erreur de consensus de la forme `need 3 registered nodes in cell <id>, have 1`. En cas de succès il imprime combien de shards locaux ont été passés au changement d'appartenance.

### `cell register --cell-id <u64> --endpoints <a,b,c>`

Appelle `RegisterCell`. Le serveur abandonne le corps et renvoie un succès. La commande imprime `Registered Cell <id>` alors qu'aucune cellule n'a été stockée. Ne pas l'utiliser comme étape de fédération.

### `sql <énoncé>`

Appelle `ExecuteSql` sans paramètres liés. L'énoncé doit être l'un des 20 modèles de [Architecture](./architecture.md), avec des littéraux à la place de `?`.

```bash
metadb-ctl --endpoint 127.0.0.1:7800 sql \
  "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
```

### `bench`

Voir [Tests et bancs](./testing.md#metadb-ctl-bench). C'est un client contre un serveur en cours d'exécution. Ce n'est pas une cible de banc Cargo. Le dépôt n'enregistre pas de nombre de débit pour cette commande.

## Métriques

Définir `node.metrics_listen_addr` (par exemple `127.0.0.1:9100`). `metadbd` accepte les connexions TCP et écrit un unique HTTP `200` dont le corps est du texte Prometheus. Il ne route pas sur le chemin d'URL. Toute requête reçoit le même corps.

Jauges et compteurs de `render_prometheus_metrics` :

| Nom | Genre |
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

Il n'y a pas de série `metadb_raft_watermark_lag_entries`. `metadb_cells_total` compte les cellules enregistrées dans ce processus, ce que `open_and_serve` fixe à un.

## Montée en charge

Ce qui fonctionne dans un seul processus :

- Davantage de shards. Le dépassement de `shard_split_threshold_bytes`, ou `metadb-ctl shard split`, peut ajouter des groupes de shards sous le même répertoire `be/shards`. Le parent conserve un intervalle de clés à gauche et l'enfant démarre comme un autre groupe à 1 votant sur ce nœud. Un redémarrage rouvre les deux.
- Indicateur de mode double. `shard enable-dual-mode` marque un identifiant de parent. Un découpage ultérieur de ce shard peut couper sur `(pid, name)`.
- Indicateur jumbo. Lorsqu'un parent représente plus de 85 % des octets `MetaTable` du shard, ou que le shard n'a qu'un parent, la route devient `JumboIsolated`. L'identifiant de nœud dédié est choisi parmi les nœuds déjà enregistrés dans cette cellule. Avec un seul nœud, l'indicateur nomme ce processus. Aucun fichier n'est déplacé vers un autre hôte.
- Votants dans le processus. `DurableCellManager::scale_out_replicas` peut ajouter des learners et les promouvoir lorsque ces identifiants de nœud sont enregistrés **et** que chaque adresse sert déjà `RaftTransportService` pour le groupe de ce shard. `crates/metadb-raft/tests/multi_raft_failover_tests.rs` le fait avec trois écouteurs dans un seul processus de test et une machine d'état KV simple, pas la machine d'état de métadonnées, et pas trois processus `metadbd`.

Ce qui ne fonctionne pas comme procédure d'exploitation :

1. `metadbd --role be`, `--role proxy`, `--role cell-manager` et `--role global-manager` sortent dans `open_and_serve`. Les sous-systèmes ne sont pas découpés par rôle, et le processus refuse de les démarrer sous ces noms.
2. Un second `metadbd --role all-in-one` avec `--cell-manager <premier>` amorce quand même sa propre cellule et ses propres shards 1–3, puis appelle `RegisterNode`. L'enregistrement note une adresse. Il ne copie pas le journal du premier processus et ne fait pas du second un learner. `ScaleShardReplicas` tentera `add_learner` vers cette adresse et expirera ou échouera, sauf si ce processus héberge déjà les mêmes groupes de shards, ce que l'amorçage all-in-one ne fait pas.
3. `metadb-ctl cell register` ne crée pas une seconde cellule. `open_and_serve` n'enregistre que la cellule locale sur le gestionnaire global.
4. Le chemin en mémoire `MetaDbEngine::new_single_node` utilisé par les tests `T1`–`T6` peut porter un compteur de votants en mémoire à 3. Cet objet n'a ni disque ni Raft. Ce n'est pas une montée en charge de `metadbd`.

La [pull request ouverte n° 2](https://github.com/rustfs/MetaDB/pull/2) (non fusionnée) énonce la même frontière : son test distribué appelle `DurableCellManager::scale_out_replicas` avec trois votants Raft sur localhost dans un seul processus du système d'exploitation. Il ne lance pas trois processus `metadbd`. Les rôles désagrégés et une seconde cellule n'y sont pas non plus testés comme déploiements vivants. L'isolation jumbo sur nœud dédié dans cette pull request reste le comportement du simulateur en mémoire.

## Erreurs que l'on peut rencontrer

Les variantes sont définies dans `crates/metadb-core/src/error.rs`. Les méthodes gRPC renvoient en général la variante dans le champ `error` de la réponse plutôt que comme un statut non OK.

| Variante | Quand le code la lève |
| :--- | :--- |
| `ReservedEntryName` | Nom vide, NUL, ou `_attr_` utilisé comme entrée utilisateur |
| `AlreadyExists` / `NotFound` | Création ou recherche de `(pid, name)` |
| `DirectoryNotEmpty` | `rmdir` alors que des enfants restent, y compris avant le vidage des attributs |
| `InvalidEntryType` | `rmdir` sur un fichier, ou `unlink` sur un répertoire |
| `TransactionTimeout` | HLC d'instantané plus ancien que la fenêtre MVCC en mémoire |
| `ClockSkewExceeded` | HLC distant en avance sur l'horloge murale locale de plus de `max_clock_drift_ms` |
| `EpochMismatch` / `NotLeader` | Époque de route périmée, ou propose envoyé à un non-leader |
| `UnsplittableJumboShard` | Découpeur en mémoire (`shard.rs`), pas le chemin jumbo durable, qui pose `JumboIsolated` et renvoie un succès sans shard enfant |
| `WriteIntentConflict` / `TransactionAborted` | Conflit 2PC, ou bail de renommage de répertoire déjà détenu |
| `InvalidRenameCycle` | Renommage d'un répertoire dans lui-même ou un descendant |
| `ConsensusError` | Pas assez de nœuds enregistrés, délai de rattrapage des learners (10 s), ou erreur d'appartenance OpenRaft |
| `SqlParseError` | L'énoncé n'est pas l'un des 20 modèles |

`CodecError` est aussi le type utilisé pour le rejet d'un rôle non implémenté et pour un TOML invalide.

## Limites connues

- Un seul processus du système d'exploitation, rôle `all-in-one` seulement.
- Pas de protocole de jonction. Un second répertoire de données est un second cluster.
- `RegisterCell` est un succès qui ne fait rien.
- L'isolation jumbo est un indicateur de route plus un identifiant de nœud choisi dans la table locale. Elle n'évince pas d'autres shards d'un disque.
- Les battements de cœur sont un RPC sans émetteur dans `metadbd`. `StoreHeartbeat` ne persiste pas.
- Raft sur le port gRPC public n'est pas authentifié. Le processus ne termine pas TLS.
- `memtable_size_bytes` et tous les champs de temporisation Raft ne changent pas le comportement d'exécution via le TOML.
- `metadb-ctl bench` rapporte un débit sur des tentatives. `unwrap` ne prend que les échecs de transport ou de statut. Les erreurs d'application telles que `AlreadyExists` reviennent dans une réponse OK et sont abandonnées, y compris pour unlink, donc une seconde exécution peut encore imprimer un débit. Il ne vérifie ni le listage, ni dirstat, ni le quota après l'exécution, et il n'imprime pas de percentiles de latence.

## Lacunes de documentation

Ces éléments manquent au produit, donc ce guide ne peut pas décrire de procédure pour eux :

1. Une disposition multi-processus pour `be`, `proxy`, `cell-manager` et `global-manager`.
2. L'ajout d'un second et d'un troisième processus `metadbd` du système d'exploitation à un groupe Raft existant.
3. La création et le routage vers une seconde cellule.
4. Le placement d'un shard jumbo sur sa propre machine et le déplacement des autres shards hors de cette machine.
5. La matière TLS, l'authentification Raft, et une boucle de contrôle pilotée par les battements de cœur.
6. Une taille de memtable ou une temporisation Raft que l'exploitant peut régler et observer.
7. Un résultat de niveau service enregistré pour `metadb-ctl bench`. La commande imprime des nombres pour la machine sur laquelle on l'exécute. Ce dépôt n'en enregistre pas. La pull request non fusionnée n° 2 rapporte un harnais embarqué distinct ; ces commandes ne sont pas dans cet arbre. Voir [Tests et bancs](./testing.md).
