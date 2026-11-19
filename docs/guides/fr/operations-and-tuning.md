# Exploitation, CLI `metadb-ctl` & Optimisation (Français)

* **Retour à l'index**: [Présentation Français](./README.md) | [Portail](../../README.md)

---

## 1. Utilitaire d'Administration `metadb-ctl` `[Implémenté]`

`[Implémenté]` means the `metadb-ctl` binary exists and talks to an all-in-one process. `cell register` does not create a cell. `metadbd --role be` is not a supported deployment. Output from a bench command is for the machine that ran it, not a checked-in result.

```bash
# État global des cellules, nœuds BE et filigranes Raft
metadb-ctl --endpoint 127.0.0.1:7800 cluster status

# Liste des shards, plages de clés, ShardEpoch et statut JumboIsolated
metadb-ctl --endpoint 127.0.0.1:7800 shard list --cell-id 1

# Déclencher un découpage sur frontière PID
metadb-ctl --endpoint 127.0.0.1:7800 shard split --shard-id 12

# Activer le mode DualModeFineGrained (PID, name) sur un répertoire géant
metadb-ctl --endpoint 127.0.0.1:7800 shard enable-dual-mode --shard-id 42 --pid 900100

# Promouvoir les shards de 1-Voter vers 3-Voter en ligne
metadb-ctl --endpoint 127.0.0.1:7800 raft scale-replicas --cell-id 1 --target-voters 3

# Exécuter une requête SQL modèle ou un test de charge
metadb-ctl --endpoint 127.0.0.1:7800 sql "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
metadb-ctl --endpoint 127.0.0.1:7800 bench --workload hotspot-create --concurrency 64 --ops 100000
```

---

## 2. Gestion des Répertoires Géants (Jumbo Shards & Dual-Mode)

1. **`JumboIsolated` route flag**: When one parent owns more than 85% of the shard's `MetaTable` bytes, or the shard has a single parent, `JumboIsolationPlanner` writes `dedicated_node` from nodes already registered in this cell. It does not migrate co-hosted peer shards onto another host. With one node, the flag names this process.
2. **Découpage fin `DualModeFineGrained` `(PID, name)`** : Si ce répertoire dépasse la capacité d'un serveur dédié, l'activation du mode `DualModeFineGrained` découpe la plage `(PID, name)` tout en maintenant un compteur local `[0x04] DirShardDelta` sur chaque sous-shard : `create_file` et `unlink` restent en `1 RTT` sans verrouillage inter-shards, tandis que `dirstat` effectue une collecte parallèle en `1` saut réseau.

---

## 3. Réglages Clés du Moteur de Stockage (`metadb-storage`)

* **`mvcc_retention_ms` (`5000` ms par défaut)** : Durée maximale de rétention des versions MVCC en mémoire. Utilisez systématiquement la pagination par curseur (`LIMIT`, modèle #10) au lieu de scans longs.
* **`memtable_size_bytes` (`32 Mio` par défaut)** : Augmentez à `64 Mio` pour les charges d'entraînement IA à forte concurrence afin d'élaguer davantage de versions transitoires avant l'écriture L0.
* **`block_cache_bytes` (`512 Mio` par défaut)** : Allouez `25 %–40 %` de la RAM sur les nœuds BE dédiés.
* **`locality_tombstone_threshold` (`128` par défaut)** : Seuil de pierres tombales physiques par préfixe de 9 octets `[table_prefix][PID]` déclenchant une compaction ciblée jusqu'au niveau le plus bas (réduire à `64` en cas d'expirations TTL massives).

---

## 4. Dépannage

* **`MetaDbError::TransactionTimeout`** : Un `SnapshotGuard` a dépassé la fenêtre de `5 000 ms`. Découpez vos parcours `ListDir` avec `WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT 1000`.
* **`MetaDbError::ClockSkewExceeded`** : Dérive d'horloge `> 500 ms` entre nœuds ; vérifiez la synchronisation NTP / `chrony`.
* **`MetaDbError::EpochMismatch` / `NotLeader`** : Rafraîchi et réessayé automatiquement via `MetaCache` lors d'un split ou d'une élection Raft.
* **Croissance des fichiers `.raftlog`** : Vérifiez la latence disque NVMe sur `state_flushed_index` (les pannes de shards `QuotaTable` distants ne bloquent pas la purge Raft grâce au débordement local dans `[0x07] SpilledAuxOutbox`).
