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

Appelle `RegisterCell`. Le serveur renvoie une erreur applicative et n'ajoute pas de cellule. `metadb-ctl` se termine avec un code non nul et n'imprime pas `Registered Cell`. Ne pas l'utiliser comme étape de fédération.

### `sql <énoncé>`

Appelle `ExecuteSql` sans paramètres liés. L'énoncé doit être l'un des 20 modèles de [Architecture](./architecture.md), avec des littéraux à la place de `?`.

```bash
metadb-ctl --endpoint 127.0.0.1:7800 sql \
  "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
```

### `bench`

Voir [Tests et bancs](./testing.md#metadb-ctl-bench). C'est un client contre un serveur en cours d'exécution. Ce n'est pas une cible de banc Cargo. Le dépôt n'enregistre pas de nombre de débit pour cette commande.

## Métriques

Définir `node.metrics_listen_addr` (par exemple `127.0.0.1:9100`). `open_and_serve` accepte les connexions TCP sur cette adresse. `GET /metrics` renvoie du texte Prometheus. Tout autre chemin renvoie 404, et toute autre méthode renvoie 405. Lorsque `[security].shared_secret` n'est pas vide, chaque requête sur cet écouteur doit envoyer `Authorization: Bearer <shared_secret>`. Un bearer absent, vide ou incorrect renvoie HTTP 401 et un corps qui n'est pas le texte des métriques, y compris une requête vers `/status`. Une adresse de métriques non locale avec un secret vide refuse de démarrer. Cet écouteur ne termine pas TLS.

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
2. Un second `metadbd --role all-in-one` rejoint un groupe existant lorsque `--join` ou `--cell-manager` nomme le leader courant et que le répertoire de données est vide. Le leader ajoute une entrée vide, prend un instantané, puis promeut le nouveau membre de learner à voter. `--join` doit atteindre ce leader. Un répertoire qui a déjà un état Raft le rouvre et ignore une nouvelle adresse de join. Un allocateur neuf pour les identifiants de nœud 2 à 32767 démarre dans l'espace de noms inode, file id et shard id de ce nœud, et ne réémet donc pas les identifiants déjà accordés par le nœud 1. Un fichier d'allocateur qui a déjà accordé de petits identifiants n'est pas déplacé. `Lookup`, le listage, la statistique de répertoire, `GetDirPartitionStat`, les lectures de quota et d'index secondaire qui atteignent un follower dont le leader est connu sont transmis à ce leader. Un follower qui ne connaît pas de leader renvoie encore `NotLeader`. Pendant une configuration jointe, `GetRoutes` rapporte chaque votant appliqué, le leader courant et l'époque appliquée. Après le commit de la configuration uniforme, ces votants et `conf_ver` correspondent à Raft. Un join qui n'aboutit pas réessaie les erreurs transitoires pendant 45 secondes, puis arrête les listeners et les groupes Raft locaux et renvoie l'erreur. `metadbd` se termine parce que le démarrage a renvoyé cette erreur. Le premier échec n'interrompt pas le processus à lui seul.
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
| `EpochMismatch` / `NotLeader` | Époque de route périmée, ou ce réplica n'est pas le leader. Les lectures d'un follower dont le leader est connu sont transmises. Un `EpochMismatch` sur un Prepare multi-clés abandonne ce tour et regroupe à nouveau ; le même lot n'est pas reproposé |
| `RetryableConsensus` | L'entrée de journal d'une écriture a été écartée lors d'un changement de leader, ou un quorum n'a pas répondu. Les écritures du même shard et le 2PC les réessaient dans le budget de routage existant. Une écriture logique conserve un seul identifiant de proposition. S'il est déjà validé, le shard renvoie le résultat d'origine et n'exécute pas la commande une seconde fois. Une panne de stockage et un nœud Raft arrêté restent `ConsensusError` et ne sont pas réessayés |
| `UnsplittableJumboShard` | Découpeur en mémoire (`shard.rs`), pas le chemin jumbo durable, qui pose `JumboIsolated` et renvoie un succès sans shard enfant |
| `WriteIntentConflict` / `TransactionAborted` | Conflit 2PC, ou bail de renommage de répertoire déjà détenu |
| `InvalidRenameCycle` | Renommage d'un répertoire dans lui-même ou un descendant |
| `ConsensusError` | Pas assez de nœuds enregistrés, délai de rattrapage des learners (10 s), ou erreur d'appartenance OpenRaft |
| `SqlParseError` | L'énoncé n'est pas l'un des 20 modèles |

`CodecError` est aussi le type utilisé pour le rejet d'un rôle non implémenté et pour un TOML invalide.

## Limites connues

- Un seul processus du système d'exploitation, rôle `all-in-one` seulement.
- Un second processus all-in-one peut rejoindre la même cellule avec `[cluster] join`. Cela ne crée pas une seconde cellule. `RegisterCell` renvoie une erreur.
- `GetRoutes` échoue fermé pour un `cell_id` non nul inconnu. `StoreHeartbeat` ne remplace pas les routes en cache.
- L'isolation jumbo est un indicateur de route plus un identifiant de nœud choisi dans la table locale. Elle n'évince pas d'autres shards d'un disque.
- Les battements de cœur sont un RPC sans émetteur dans `metadbd`. `StoreHeartbeat` ne persiste pas.
- Un secret partagé non vide authentifie Raft, les trois autres services gRPC et l'écouteur de métriques avec le même bearer. Un secret vide n'est accepté que pour les liaisons en boucle locale et laisse ces écouteurs non authentifiés. Une liaison joker avec un secret vide refuse de démarrer. Le processus ne termine pas TLS.
- `memtable_size_bytes` et tous les champs de temporisation Raft ne changent pas le comportement d'exécution via le TOML.
- `metadb-ctl bench` pour `hotspot-create` et `bulk-ttl-delete` imprime des lignes JSON : un en-tête avec `bench_pid`, puis des rapports de phase avec attempted, succeeded, failed et des percentiles de latence. Le processus se termine en erreur si une opération demandée échoue. `--workload mixed` ou `--embedded` lance le harnais de stress dans le processus. `sql` imprime la réponse en JSON et se termine en erreur sur une erreur d'application.

## Lacunes de documentation

Ces éléments manquent au produit, donc ce guide ne peut pas décrire de procédure pour eux :

1. Une disposition multi-processus pour `be`, `proxy`, `cell-manager` et `global-manager`.
2. L'ajout d'un second et d'un troisième processus `metadbd` du système d'exploitation à un groupe Raft existant.
3. La création et le routage vers une seconde cellule.
4. Le placement d'un shard jumbo sur sa propre machine et le déplacement des autres shards hors de cette machine.
5. Le TLS natif. Ce processus ne termine pas TLS. Les clés de configuration qui demandent TLS ou un certificat sont rejetées au lieu de servir en clair. Placer la terminaison dans un proxy externe. Raft et les métriques utilisent le même bearer que les autres services lorsqu'un secret est défini ; ce n'est pas du TLS mutuel. Une boucle de contrôle pilotée par les battements de cœur n'est pas implémentée.
6. Une taille de memtable ou une temporisation Raft que l'exploitant peut régler et observer.
7. Un résultat de niveau service enregistré pour `metadb-ctl bench`. La commande imprime des nombres pour la machine sur laquelle on l'exécute. Ce dépôt n'en enregistre pas. La pull request non fusionnée n° 2 rapporte un harnais embarqué distinct ; ces commandes ne sont pas dans cet arbre. Voir [Tests et bancs](./testing.md).
