# Architecture telle qu'implémentée

[Présentation](./README.md) · [English](../en/architecture.md) · [Deutsch](../de/architecture.md) · [日本語](../ja/architecture.md) · [简体中文](../zh/architecture.md)

Cette page suit le code de `crates/`. Elle ne décrit pas un déploiement de cellule multi-processus. Ce déploiement n'est pas implémenté. Voir [Exploitation](./operations.md#limites-connues).

## Forme du processus

`PersistentMetaDbCluster::open_and_serve` (`crates/metadb-server/src/lib.rs`) :

1. Rejette tout `node.role` autre que `all-in-one`.
2. Lie `node.grpc_listen_addr` (défaut `127.0.0.1:7800` ; `127.0.0.1:0` demande un port au système).
3. Ouvre `DurableIdAllocator` dans `<data_dir>/global_allocator`.
4. Ouvre `ShardGroupHost` dans `<data_dir>/be`.
5. Ouvre ou amorce `DurableCellManager` dans `<data_dir>/cell_<cell_id>`.
6. Enregistre cette unique cellule sur `DurableGlobalManager` pour l'intervalle de clés `[0x01, 0x04)`.
7. Garantit que l'inode racine `1` (`ROOT_INODE_ID`) possède un enregistrement d'attributs.
8. Sert quatre services gRPC sur le même écouteur. Les services de métadonnées, de transaction et de plan de contrôle prennent un intercepteur bearer facultatif. `RaftTransportService` n'en prend pas.
9. Lance une tâche de maintenance toutes les 250 ms.

`metadbd` est une fine enveloppe : il charge le TOML, applique les remplacements CLI, appelle `open_and_serve`, appelle éventuellement `RegisterNode` sur `--cell-manager`, sert éventuellement le texte Prometheus, et sur Ctrl-C appelle `shutdown`.

## Clés et tables

Préfixes des tables utilisateur (`crates/metadb-core/src/schema.rs`) :

| Préfixe | Table | Intervalle d'amorçage si `bootstrap_table_shards = true` |
| :--- | :--- | :--- |
| `0x01` | `MetaTable` | shard 1, `[0x01, 0x02)` |
| `0x02` | `QuotaTable` | shard 2, `[0x02, 0x03)` |
| `0x03` | `SecondaryIndexTable` | shard 3, `[0x03, 0x04)` |

Les clés de `MetaTable` sont `[0x01][pid : 8 octets gros-boutistes][nom UTF-8]`. Les attributs du répertoire lui-même sont l'enfant réservé `(pid, "_attr_")`, stocké à côté de `(pid, child_name)`. Les noms utilisateur vides, contenant NUL, ou égaux à `_attr_` sont rejetés (`ReservedEntryName`).

Les identifiants d'inode de répertoire ont le bit 63 à zéro. Les identifiants d'inode de fichier ont le bit 63 à un (`FILE_INODE_TAG_BIT`). La racine est l'inode `1`. L'allocateur durable commence les nouveaux identifiants de répertoire à `2`, les identifiants de fichier à `1`, et les identifiants de shard dynamiques à `4` (1–3 sont les shards d'amorçage). `LocalIdBuffer` accorde les identifiants de répertoire par blocs de 256, et les identifiants d'inode de fichier ainsi que les identifiants de fichier par blocs de 4 096, après écriture de l'octroi dans `ID_ALLOCATOR_STATE`.

Préfixes système locaux au shard :

| Préfixe | Enregistrement |
| :--- | :--- |
| `0x04` | `DirShardDelta` pour un sous-shard non ancre en mode double |
| `0x05` | `WriteIntent` de 2PC |
| `0x06` | `TxnRecord` de 2PC |
| `0x07` | entrées de file auxiliaire déversées |

Lorsque `bootstrap_table_shards = false`, l'amorçage crée un seul shard, id `1`, couvrant `[0x01, 0x04)`.

## Chemin d'une requête

Un appel de métadonnées entre soit par le `DistributedTxRouter` embarqué (`cluster.tx_router()`), soit par gRPC `MetadataService` / `TransactionService`. `StatelessProxyRouter` compile l'un des 20 modèles SQL et appelle le même routeur. Il n'y a pas de moteur SQL général. L'analyseur n'accepte que les énoncés ci-dessous (`crates/metadb-proxy/src/sql_templates.rs`). Les emplacements `?` lient des paramètres `u64`, `i64` ou chaîne. Un littéral `name = '_attr_'` est une statistique de répertoire. Un paramètre lié égal à `_attr_` est rejeté.

| # | Énoncé | Effet |
| :--- | :--- | :--- |
| 1 | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | créer un fichier |
| 2 | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | mkdir |
| 3 | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | unlink |
| 4 | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | rmdir |
| 5 | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | renommage sans écrasement |
| 6 | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | renommage avec écrasement |
| 7 | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | dirstat |
| 8 | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | lookup |
| 9 | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | listdir, limite figée à 10 000 |
| 10 | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | page de listdir |
| 11 | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | listdir par préfixe (`LIKE` n'accepte que cette forme de préfixe) |
| 12 | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | setattr du mode |
| 13 | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | setattr de l'uid |
| 14 | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | setattr de la taille et du fid |
| 15 | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | setattr de l'heure de modification |
| 16 | `SELECT * FROM QuotaTable WHERE uid = ?` | lire le quota |
| 17 | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | fixer la limite de quota (la valeur `ON CONFLICT` est stockée) |
| 18 | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | lister les quotas |
| 19 | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | lire l'index secondaire |
| 20 | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | balayer l'index secondaire |

`metadb-ctl sql` envoie `ExecuteSql` avec une liste de paramètres vide : l'énoncé doit donc utiliser des littéraux.

Le routage utilise `CompositeMetaCache`. En cas d'absence ou de décalage d'époque, le cache est rafraîchi depuis la cellule locale et l'appel est réessayé sur le groupe de shard local. `DurableGlobalManager::route_user_key` cherche un descripteur de shard **local**. Il ne transmet pas la requête utilisateur à un autre processus.

## Stockage

La machine d'état de chaque shard ouvre un LSM sans WAL dans `<data_dir>/be/shards/<shard_id sur 20 chiffres>/lsm`. Les SSTables utilisent des blocs de données de 4 Kio compressés par préfixe (intervalle de redémarrage 16), un filtre de Bloom et un `MANIFEST` avec un pointeur `CURRENT`. Le cache de blocs est un LRU à 16 voies. Son budget total est `storage.block_cache_bytes` (défaut 64 Mio). Chacune des 16 voies reçoit au moins 16 Kio.

Les versions MVCC vivent en mémoire pendant `storage.mvcc_retention_ms` (défaut 5 000). Un vidage coordonné scelle le memtable, exécute le crochet d'agrégation d'attributs et écrit une SSTable L0. Il n'y a pas de WAL KV utilisateur séparé. La reprise après plantage est le journal Raft plus le manifeste LSM. `LocalityCompactor` suit les tombstones par seau de préfixe de 9 octets et compacte un intervalle de suppression chaud lorsque `locality_tombstone_threshold` et `locality_tombstone_ratio` sont tous deux franchis. `auto_locality_compaction` est toujours mis à `true` par `ServerTomlConfig::to_lsm_config` ; le fichier TOML ne peut pas le désactiver.

`storage.memtable_size_bytes` est désérialisé et jamais lu. La taille de vidage ne vient pas de ce champ.

## Raft

Chaque shard local est un groupe `openraft` 0.10. Le journal est un fichier segmenté CRC32C sous `.../shards/<id>/raft`. Options par défaut, qui ne sont **pas** chargées depuis le TOML (`raft_options` est `#[serde(skip)]`) :

| Champ | Défaut |
| :--- | :--- |
| battement | 150 ms |
| délai d'élection | 450–900 ms |
| `max_in_snapshot_log_to_keep` | 0 |
| taille de segment de journal | 64 Mio |

La purge du journal est bornée par `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)`. La boucle de maintenance de 250 ms appelle `flush_snapshot_and_purge` lorsqu'un shard conserve encore des entrées de journal, sauf si un filigrane d'instantané est déjà au moins égal à `state_flushed_index` et qu'il reste moins de 64 entrées.

`ConfChangeOrchestrator::add_learners_and_promote_voters` ajoute des learners, attend que le retard de réplication soit d'au plus 64 entrées (délai par défaut 10 s, valeur passée par `scale_out_replicas`), puis appelle `change_membership`. Le test dans le processus `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` le fait avec trois écouteurs tonic et une machine d'état KV simple, dans un seul processus du système d'exploitation. Il ne démarre pas `metadbd`.

## Transactions et travail auxiliaire

Les créations, unlink, setattr, lookup, dirstat, listdir d'un seul répertoire, ainsi que mkdir/rmdir sur le même shard, sont proposés au leader du shard. Les compteurs du répertoire parent ne sont pas verrouillés sur le chemin de premier plan : les mutations enregistrent des deltas, `ReplicatedAttrAggregator` les replie à la barrière de vidage, et dirstat réconcilie les deltas non vidés sur le chemin de lecture.

Les mises à jour de quota et de `fid -> (pid, name)` passent par une file auxiliaire par shard. `AuxDispatcher` les applique au tic de maintenance. Si la file ne peut pas être appliquée, les entrées peuvent se déverser sous le préfixe `0x07` afin que la compaction Raft ne reste pas bloquée indéfiniment par cette file. `TxnRecoveryWorker` parcourt `TxnRecord` / `WriteIntent` au même tic.

Le renommage inter-shards (et mkdir/rmdir qui touchent deux shards) utilise un 2PC ancré sur le primaire (`WriteIntent` + `TxnRecord`). Les renommages de répertoire prennent un seul `DirRenameLease` actif auprès de l'allocateur d'identifiants. Un second bail est rejeté tant que le premier n'est pas libéré ou que son expiration HLC n'est pas passée. Le TTL du bail est d'au moins 100 ms.

## Découpage à l'intérieur du processus

`maybe_split_or_isolate_shard` s'exécute lorsque la taille approximative du shard atteint `sharding.shard_split_threshold_bytes` (défaut 4 Gio), ou lorsque `TriggerShardSplit` force un contrôle.

`RaftShardSplitWorker::discover_split_key` :

- Exige au moins deux clés vivantes.
- Les shards de quota et d'index secondaire se découpent à un milieu en octets.
- `MetaTable` somme les octets par identifiant de parent. Si un parent possède plus de 85 % de ces octets (`dominant_bytes * 100 > total_bytes * 85`), ou si le shard n'a qu'un parent, le candidat est `Jumbo`.
- Sinon il choisit une frontière d'identifiant de parent près de la moitié des octets. La clé de découpage est un préfixe de PID, de sorte que `_attr_` et les enfants directs d'un parent restent ensemble.
- Si la route est déjà `DualModeFineGrained` pour ce parent, la clé de découpage est un milieu `(pid, name)`, en excluant `_attr_`.

Un candidat jumbo ne **déplace pas** le répertoire du shard. `JumboIsolationPlanner` écrit `ShardHealthState::JumboIsolated { dedicated_node, dominant_pid }` sur la route. Le nœud choisi est le nœud enregistré qui porte le moins de routes jumbo, puis le moins d'autres routes. Avec seul le nœud d'amorçage enregistré, ce nœud est ce processus.

`enable_dual_mode_split` persiste `split_mode = DualModeFineGrained` et `dual_mode_pid`. Il ne copie pas lui-même les clés. Un découpage ultérieur utilise la règle `(pid, name)`. Le simulateur en mémoire (`CellLocalManager` dans `control_plane.rs`) est un type différent : son chemin jumbo renvoie `UnsplittableJumboShard` et son « passage à 3 votants » ne modifie qu'un `BTreeMap`. Les tests `T1`–`T6` exercent ce simulateur.

Les plans de découpage sont stockés dans `CELL_MANAGER_STATE`. Les points de plantage couverts par `split_crash_recovery_tests` reprennent ou annulent un plan au prochain `maybe_split_or_isolate_shard`. Les répertoires de shards enfants sont créés sous le même arbre `be/shards`.

## RPC du plan de contrôle qui font moins que leur nom

| RPC | Comportement dans cet arbre |
| :--- | :--- |
| `RegisterNode` | Insère `(node_id, grpc_addr)` dans le `CELL_MANAGER_STATE` de cette cellule |
| `StoreHeartbeat` | Insère les protos de route dans le cache en mémoire. Ignore les champs disque et QPS. `metadbd` n'envoie jamais ce RPC |
| `GetRoutes` | Liste les routes du cache (`cell_id = 0`) ou les routes d'une cellule |
| `AllocateIdBlock` | Alloue depuis l'allocateur durable local |
| `AllocSplitPlan` / `ActivateSplit` | Transmet au gestionnaire de cellule local |
| `TriggerShardSplit` | Force `maybe_split_or_isolate_shard` sur un shard local |
| `EnableDualModeSplit` | Pose l'indicateur de route local. `split_name` de la requête est ignoré |
| `ScaleShardReplicas` | Appelle `scale_out_replicas` si cette cellule existe et si assez de nœuds sont enregistrés |
| `RegisterCell` | Renvoie `error: None` et ne stocke ni `cell_id`, ni l'intervalle de clés, ni les points de terminaison |
