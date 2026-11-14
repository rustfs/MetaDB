# Déploiement & Montée en Charge en Ligne (Français)

* **Retour à l'index**: [Présentation Français](./README.md) | [Portail](../../README.md)

MetaDB exécute **exactement le même moteur LSM sans WAL et la même machine à états OpenRaft** du nœud unique jusqu'au cluster multi-cellules de plus de 1 000 serveurs, sans migration hors ligne.

---

## Étape 1 : Déploiement Mononœud Autonome (`metadbd --role all-in-one`) `[Implémenté]`

Tous les rôles (`GlobalManager`, `CellLocalManager`, `MetaCache`, `Proxy` et stockage `BE` en **OpenRaft `1-Voter`**) tournent dans un seul binaire. Le WAL local du moteur KV est désactivé : seul le journal segmenté `.raftlog` (protégé par CRC32C) assure la durabilité séquentielle.

```toml
[node]
node_id = 1
role = "all-in-one"
data_dir = "/var/lib/metadb"
grpc_listen_addr = "127.0.0.1:7800"
metrics_listen_addr = "127.0.0.1:9100"

[clock]
max_clock_drift_ms = 500

[storage]
memtable_size_bytes = 33554432          # 32 Mio
block_cache_bytes = 536870912           # 512 Mio (cache LRU à 16 fragments)
mvcc_retention_ms = 5000                # Fenêtre MVCC en mémoire de 5 s
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296 # 4 Gio
```

Rust 1.100.0 (`rust-toolchain.toml`). Seul le rôle `all-in-one` démarre. Sans `[security].shared_secret`, le gRPC n'a ni authentification ni TLS ; l'écoute par défaut est `127.0.0.1`. Ne pas exposer ce port à un réseau non fiable.

```bash
metadbd --config /etc/metadb/standalone.toml
```

---

## Étape 2 : Découpage Dynamique sur Frontière `PID` `[Implémenté]`

Lorsqu'un shard dépasse `shard_split_threshold_bytes`, `PolicyShardSplitter` choisit une frontière stricte `group_key = PID` (`[0x01][pid: 8B BE]`) afin que `(PID, "_attr_")` et tous ses enfants restent sur le même shard.

---

## Étape 3 : Extension sans Interruption vers Multi-Raft `3-Voter` & Multi-Cell `[Implémenté]`

### 3.1 Promotion Haute Disponibilité `1-Voter -> Learner -> 3-Voter`

```bash
metadbd --role be --node-id 2 --listen 10.0.1.12:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb
metadbd --role be --node-id 3 --listen 10.0.1.13:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb

metadb-ctl raft scale-replicas --cell-id 1 --target-voters 3
```

Les nouveaux nœuds rejoignent le groupe en tant que `Learner`, reçoivent un instantané SSTable par liens physiques (`InstallSnapshot`) suivi des segments `.raftlog`, puis passent `Voter` par consensus conjoint sans arrêt des écritures.

### 3.2 Architecture Multi-Cell à Deux Niveaux (`100 Md – 10 000 Md+` d'entrées)

* **`global-manager`** : Table grossière `[start_key, end_key) -> CellId` (`<1 000` entrées) et allocation de blocs de `65 536` identifiants.
* **`cell-manager`** : Battements de cœur par shard (`20K–50K` shards/Cell), découpage `PID` et isolation dédiée des Jumbo Shards.
* **`be`** : Groupes Multi-Raft, `MemMvccStore`, `LsmStorageBackend` et exécution `1-RTT`.
* **`proxy`** : Routage `MetaCache` en un saut, compilation des 20 modèles SQL et coordination 2PC.
