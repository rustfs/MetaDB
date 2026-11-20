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

Appelle `GetRoutes` avec `cell_id = 0`. Pour chaque shard il imprime `shard`, `cell`, `leader`, le nombre de votants, `epoch=(conf_ver, ver)`, `dual_mode` et `jumbo`. It then prints local `CELL_CONTROL_PLANE` placement rows (`phase`, `phase_code`, `dedicated_node`, `dominant_pid`, `moves`, attempt timestamps, `reason`) and `placement_automatic_transfer_enabled=false`. Those rows are not routes. A remote cell contributes no placement rows. It does not print disk free space, QPS, Raft watermarks, or `MOVE_STAGED` / `MOVE_LIVE`. The reconciler does not publish phase `recorded`. It does not stamp `transfer_attempt_unix_ms`. A new row stores 0. A later reconcile keeps a non-zero value already in the file.

### `shard list --cell-id <u64>`

Appelle `GetRoutes` pour cette cellule (utiliser `0` pour chaque route en cache). Imprime `shard`, `cell`, les octets bruts de début et de fin de clé, et l'époque `(conf_ver, version)`, puis les mêmes lignes de placement locales. A remote cell prints `Placements: 0`.

### `shard split --shard-id <u64>`

Appelle `TriggerShardSplit`, qui force un contrôle de découpage même si le shard est sous `shard_split_threshold_bytes`. On success it prints `split_executed`, `child_shard_id`, and the placement fields. When no placement row exists, including an ordinary split, the line is `placement_phase=` and `placement_phase_code=0`. Code 0 is only that no-record case. Stored rows use codes 1–4. `split_executed=false` signifie qu'aucun shard enfant n'a été créé (moins de deux clés, ou un candidat jumbo qui écrit une ligne de placement et laisse `split_executed=false`).

### `shard enable-dual-mode --shard-id <u64> --pid <u64>`

Appelle `EnableDualModeSplit`. Le CLI envoie un `split_name` vide. Le serveur stocke `dual_mode_pid` et ne découpe pas immédiatement.

### `raft scale-replicas --cell-id <u64> --target-voters <u32>`

Appelle `ScaleShardReplicas`. Le serveur exige que `target_voters` entrées soient déjà présentes dans la table des nœuds de cette cellule. Une cellule all-in-one fraîche a un nœud, donc `--target-voters 3` renvoie une erreur de consensus de la forme `need 3 registered nodes in cell <id>, have 1`. En cas de succès il imprime combien de shards locaux ont été passés au changement d'appartenance.

### `cell register --cell-id <u64> --endpoints <a,b,c>`

Calls `RegisterCell`. The server fsyncs `GLOBAL_CELL_DIRECTORY` only when the endpoint's routes cover the requested range exactly. Omitting the keys sends `[01, 04)`, which overlaps a default cell, and `metadb-ctl` exits non-zero. The caller does not forward keys.

### `sql <énoncé>`

Appelle `ExecuteSql` sans paramètres liés. L'énoncé doit être l'un des 20 modèles de [Architecture](./architecture.md), avec des littéraux à la place de `?`.

```bash
metadb-ctl --endpoint 127.0.0.1:7800 sql \
  "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
```

### `bench`

See [Tests and benches](./testing.md#metadb-ctl-bench). Remote `hotspot-create` and `bulk-ttl-delete` are a client against a running server. They print JSON with `attempted`, `succeeded`, and `failed`, and the process exits non-zero unless `succeeded` equals `--ops`. `--embedded` and `--workload mixed` run the [in-process stress harness](./testing.md#in-process-stress-harness) instead of dialing the endpoint. That path passes when `errors` and `correctness_errors` are both zero, and it prints latency percentiles in microseconds. Neither path is a Cargo benchmark target. The repository does not check in a throughput number for either path. A printed line is not a service-level result and is not the official 1,000,000-create profile.

The separate prefix-load harness (100 prefixes × 10,000 creates, then 100,000 `SetAttr` overwrites, then 100,000 deletes, concurrency 64) is documented in [Tests and benches](../en/testing.md#prefix-load). GitHub Actions runs only the small profile. It does not run the 1,000,000-create profile.

## Métriques

Définir `node.metrics_listen_addr` (par exemple `127.0.0.1:9100`). `open_and_serve` accepte les connexions TCP sur cette adresse. `GET /metrics` renvoie du texte Prometheus. Tout autre chemin renvoie 404, et toute autre méthode renvoie 405. Lorsque `[security].shared_secret` n'est pas vide, chaque requête sur cet écouteur doit envoyer `Authorization: Bearer <shared_secret>`. Un bearer absent, vide ou incorrect renvoie HTTP 401 et un corps qui n'est pas le texte des métriques, y compris une requête vers `/status`. An empty secret does not start the process on any address, including loopback, unless `allow_unauthenticated = true`. That flag is loopback-only and still refuses a wildcard or other non-loopback address. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.

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
| `metadb_placement_records` | gauge, local placement rows |
| `metadb_placement_phase_recorded` | gauge; the reconciler does not publish this phase |
| `metadb_placement_phase_leader_on_dedicated` | gauge |
| `metadb_placement_phase_dedicated_not_shared` | gauge |
| `metadb_placement_phase_blocked` | gauge |
| `metadb_placement_moves_leadership` | gauge |
| `metadb_placement_moves_voter_replaced` | gauge |
| `metadb_placement_rows_with_transfer_attempt` | gauge; the reconciler does not stamp a new value and keeps one already in the file |
| `metadb_placement_rows_with_replace_attempt` | gauge |
| `metadb_placement_automatic_transfer_enabled` | gauge, 0 in this build |

There is no `metadb_raft_watermark_lag_entries` series and no per-shard placement label. There is no `MOVE_STAGED` or `MOVE_LIVE` series. `metadb_cells_total` counts rows in this process's `GLOBAL_CELL_DIRECTORY`, including a remote cell after `RegisterCell`. `metadb_shards_total` counts only local shards. A fresh process has one cell. The English operability page lists the readiness gauges that this table does not repeat.

## Montée en charge

Ce qui fonctionne dans un seul processus :

- Davantage de shards. Le dépassement de `shard_split_threshold_bytes`, ou `metadb-ctl shard split`, peut ajouter des groupes de shards sous le même répertoire `be/shards`. Le parent conserve un intervalle de clés à gauche et l'enfant démarre comme un autre groupe à 1 votant sur ce nœud. Un redémarrage rouvre les deux.
- Indicateur de mode double. `shard enable-dual-mode` marque un identifiant de parent. Un découpage ultérieur de ce shard peut couper sur `(pid, name)`.
- Indicateur jumbo. Lorsqu'un parent représente plus de 85 % des octets `MetaTable` du shard, ou que le shard n'a qu'un parent, la route devient `JumboIsolated`. L'identifiant de nœud dédié est choisi parmi les nœuds déjà enregistrés dans cette cellule. Avec un seul nœud, l'indicateur nomme ce processus. L'indicateur lui-même ne déplace pas de fichiers. Un placement loopback `MOVE_STAGED` / `MOVE_LIVE` peut envoyer le répertoire à un listener `127.0.0.1`. `metadbd` ne démarre pas ce listener. Il n'ouvre pas Raft, n'appelle pas `transfer_leader` et n'est pas une copie de production. Voir `docs/en/scale-control-plane-status.md`.
- Votants dans le processus. `DurableCellManager::scale_out_replicas` peut ajouter des learners et les promouvoir lorsque ces identifiants de nœud sont enregistrés **et** que chaque adresse sert déjà `RaftTransportService` pour le groupe de ce shard. `crates/metadb-raft/tests/multi_raft_failover_tests.rs` le fait avec trois écouteurs dans un seul processus de test et une machine d'état KV simple, pas la machine d'état de métadonnées, et pas trois processus `metadbd`.

Ce qui ne fonctionne pas comme procédure d'exploitation :

1. `metadbd --role be` hosts local Raft replicas and serves their metadata. It does not publish a cell or mint ids. `metadbd --role proxy` dials one registered cell for a contained metadata call, `ExecuteSql`, or user-key transaction and fails closed otherwise. `CommitPrimary2pc`, `ApplyAuxBatch`, `ForwardRouter`, and `GetRoutes` may still use `[cluster] upstream`. `AllocateIdBlock` is refused. The proxy does not host Raft. `--role global-manager` serves `GLOBAL_CELL_DIRECTORY` only and does not open Raft groups. `--role cell-manager` hosts one local cell and its Raft groups, and publishes that cell when `[cluster] global_manager` is set. The directory file is not a Raft group, and the global process does not forward keys.
2. Un second `metadbd --role all-in-one` rejoint un groupe existant lorsque `--join` ou `--cell-manager` nomme le leader courant et que le répertoire de données est vide. Le leader ajoute une entrée vide, prend un instantané, puis promeut le nouveau membre de learner à voter. `--join` doit atteindre ce leader. Un répertoire qui a déjà un état Raft le rouvre et ignore une nouvelle adresse de join. Un allocateur neuf pour les identifiants de nœud 2 à 32767 démarre dans l'espace de noms inode, file id et shard id de ce nœud, et ne réémet donc pas les identifiants déjà accordés par le nœud 1. Un fichier d'allocateur qui a déjà accordé de petits identifiants n'est pas déplacé. `Lookup`, le listage, la statistique de répertoire, `GetDirPartitionStat`, les lectures de quota et d'index secondaire qui atteignent un follower dont le leader est connu sont transmis à ce leader. Un follower qui ne connaît pas de leader renvoie encore `NotLeader`. Pendant une configuration jointe, `GetRoutes` rapporte chaque votant appliqué, le leader courant et l'époque appliquée. Après le commit de la configuration uniforme, ces votants et `conf_ver` correspondent à Raft. Un join qui n'aboutit pas réessaie les erreurs transitoires pendant 45 secondes, puis arrête les listeners et les groupes Raft locaux et renvoie l'erreur. `metadbd` se termine parce que le démarrage a renvoyé cette erreur. Le premier échec n'interrompt pas le processus à lui seul.
3. `metadb-ctl cell register` records a second cell only after that cell's routes cover the requested range. This process does not forward those keys. `open_and_serve` still starts one local cell.
4. Le chemin en mémoire `MetaDbEngine::new_single_node` utilisé par les tests `T1`–`T6` peut porter un compteur de votants en mémoire à 3. Cet objet n'a ni disque ni Raft. Ce n'est pas une montée en charge de `metadbd`.

`topology_distributed` still uses `DurableCellManager::scale_out_replicas` with localhost voters inside one OS process. `join_existing_group` starts real `metadbd` processes and joins them. The shared-secret case in that file opens two clusters inside the test process; it does not start two `metadbd` OS processes. Disaggregated roles are still not a live deployment. A second cell can be recorded in this process's directory; that is not a production multi-cell deployment. This process can copy its own jumbo shard directory onto `jumbo_dedicated_root`. A loopback `MOVE_STAGED` / `MOVE_LIVE` placement is specified in `docs/en/scale-control-plane-status.md`. It does not call `transfer_leader`, does not replicate the cell directory, and is not a production move.

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

- `all-in-one`, `cell-manager`, `global-manager`, `be`, and `proxy` start on a valid config. `be` hosts local replicas and does not mint ids. `proxy` dials one registered cell for a contained metadata call, `ExecuteSql`, or user-key transaction and fails closed otherwise. It does not host Raft. `global-manager` has no local shards and does not forward. `cell-manager` still colocates its Raft groups.
- A second all-in-one process can join the same cell with `[cluster] join`. That does not create a second cell. `RegisterCell` records a different cell only after its routes cover the requested range, and this process does not forward keys.
- `GetRoutes` échoue fermé pour un `cell_id` non nul inconnu. `StoreHeartbeat` ne remplace pas les routes en cache.
- L'isolation jumbo est un indicateur de route plus un identifiant de nœud choisi dans la table locale. Elle n'évince pas d'autres shards d'un disque.
- Registered-node liveness is fsynced in `CELL_CONTROL_PLANE`. `metadbd` persists its own record about every 3 seconds and sends it to the current leader when this process is not the leader. Descriptors on that RPC are annotations, not routes. The observation does not schedule placement. See [Scale control plane](../en/scale-control-plane-status.md).
- A secret that contains a non-whitespace character authenticates Raft, the other three gRPC services, and the metrics listener, including `/livez`, `/readyz`, and `/healthz`, with the same bearer. An empty secret does not start the process. `allow_unauthenticated = true` is the explicit exception for literal loopback IPs (`127.0.0.0/8` or `::1`) and still refuses a wildcard bind. The hostname `localhost` is not accepted for that exception. The bound socket is checked again after listen. A whitespace-only secret is rejected and is not compared as a bearer. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.
- `memtable_size_bytes` scelle et vide le memtable actif. `max_imm_memtables` limite les tables immuables en file. `[raft]` timers and `max_segment_bytes` apply when a group starts. They are not reloaded from a changed file, and the segment size does not rewrite segments already on disk.
- `metadb-ctl bench` pour `hotspot-create` et `bulk-ttl-delete` imprime des lignes JSON : un en-tête avec `bench_pid`, puis des rapports de phase avec attempted, succeeded, failed et des percentiles de latence. Le processus se termine en erreur si une opération demandée échoue. `--workload mixed` ou `--embedded` lance le harnais de stress dans le processus. `sql` imprime la réponse en JSON et se termine en erreur sur une erreur d'application.

## Lacunes de documentation

Ces éléments manquent au produit, donc ce guide ne peut pas décrire de procédure pour eux :

1. Une disposition multi-processus pour `be`, `proxy`, `cell-manager` et `global-manager`.
2. A second Cell. `RegisterCell` stays an error. Join reaches only the current leader; a follower does not forward it.
3. Le placement d'un shard jumbo sur sa propre machine et le déplacement des autres shards hors de cette machine. Un listener loopback peut recevoir un répertoire jumbo avec `MOVE_STAGED` / `MOVE_LIVE`. Ce n'est pas `metadbd`, ce n'est pas authentifié, cela n'ouvre pas Raft, n'appelle pas `transfer_leader` et n'est pas une copie de production vers un autre hôte.
4. Le TLS natif. Ce processus ne termine pas TLS. Les clés de configuration qui demandent TLS ou un certificat sont rejetées au lieu de servir en clair. Placer la terminaison dans un proxy externe. Raft et les métriques utilisent le même bearer que les autres services lorsqu'un secret est défini ; ce n'est pas du TLS mutuel. Une boucle de contrôle pilotée par les battements de cœur n'est pas implémentée.
5. A gRPC health RPC, probes when `metrics_listen_addr` is unset, and per-shard Prometheus series. `/readyz` does not wait for the 250 ms 2PC recovery pass. A leader election makes `/readyz` and `/healthz` return 503 on replicas that do not know a leader. `[raft]` is loaded and is not `#[serde(skip)]`. Its values apply when a group starts and are not reloaded. See [Operability](../en/operability.md).
6. A checked-in service-level result for `metadb-ctl bench`. The command prints numbers for the machine you run it on. This repository does not record one. See [Tests and benches](./testing.md).
