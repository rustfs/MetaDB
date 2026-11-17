# Bereitstellung & Online-Skalierung (Deutsch)

* **Zurück zur Übersicht**: [Deutsch Übersicht](./README.md) | [Hub](../../README.md)

MetaDB nutzt über alle Ausbaustufen hinweg **exakt dieselbe WAL-freie LSM-Speicher-Engine und dieselbe OpenRaft-Zustandsmaschine**, sodass ein Einzelknoten im laufenden Betrieb ohne Datenmigration zu einem Multi-Cell-Cluster skaliert werden kann.

---

## Stufe 1: Eigenständiger Einzelknoten (`metadbd --role all-in-one`) `[Implementiert]`

Im Einzelknotenmodus laufen `GlobalManager`, `CellLocalManager`, `MetaCache`, `Proxy` und das **`1-Voter`-OpenRaft**-Speicher-Backend in einem einzigen Prozess. Auf ein separates KV-WAL wird verzichtet, da das CRC32C-gesicherte `.raftlog` bereits sequenzielle Absturzsicherheit garantiert.

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
memtable_size_bytes = 33554432          # 32 MiB
block_cache_bytes = 536870912           # 512 MiB (16-Shard-LRU-Cache)
mvcc_retention_ms = 5000                # 5 s In-Memory-MVCC-Zeitfenster
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296 # 4 GiB
```

Rust 1.100.0 (`rust-toolchain.toml`). Nur die Rolle `all-in-one` startet. Dieser Prozess beendet TLS nicht. Ohne `[security].shared_secret` gibt es keine Authentifizierung, und nur Loopback-Binds werden akzeptiert. Der Default ist `127.0.0.1`. Eine Wildcard oder eine andere Adresse mit leerem Geheimnis verweigert den Start. Ein nicht leeres `shared_secret` verlangt `authorization: Bearer <secret>` für Metadaten, Transaktionen, die Control Plane, Raft auf demselben Port und `GET /metrics`.

```bash
metadbd --config /etc/metadb/standalone.toml
```

---

## Stufe 2: Dynamisches Shard-Splitting an `PID`-Grenzen `[Implementiert]`

Überschreitet ein Shard `shard_split_threshold_bytes`, teilt `PolicyShardSplitter` ihn strikt entlang einer `group_key = PID`-Grenze (`[0x01][pid: 8B BE]`), sodass `(PID, "_attr_")` und sämtliche Kind-Einträge gemeinsam auf demselben Shard verbleiben.

---

## Stufe 3: Unterbrechungsfreie Skalierung auf `3-Voter` Multi-Raft & Multi-Cell `[Implementiert]`

### 3.1 Online-Hochstufung von `1-Voter` auf `3-Voter` HA

```bash
metadbd --role be --node-id 2 --listen 10.0.1.12:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb
metadbd --role be --node-id 3 --listen 10.0.1.13:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb

metadb-ctl raft scale-replicas --cell-id 1 --target-voters 3
```

Neue Knoten treten zunächst als `Learner` bei, übernehmen per Hardlink erzeugte SSTable-Snapshots (`InstallSnapshot`) sowie `.raftlog`-Einträge und werden anschließend per Joint Consensus ohne Schreibunterbrechung zu `Voter`-Replikaten hochgestuft.

### 3.2 Zweistufige Multi-Cell-Steuerungsebene (`100 Mrd. – 10 Bio.+` Einträge)

* **`global-manager`**: Grobe Schlüsselbereichs-zu-`CellId`-Zuordnung (`<1.000` Einträge) sowie globale `65.536`-ID-Blockvergabe.
* **`cell-manager`**: Verarbeitung der 3s-BE-Heartbeats (`20K–50K` Shards pro Cell), `PID`-Splits und dedizierte Jumbo-Shard-Isolierung.
* **`be`**: Multi-Raft-Gruppen, `MemMvccStore`, `LsmStorageBackend` und `1-RTT`-Pushdown.
* **`proxy`**: Ein-Hop-Routing über `MetaCache`, Kompilierung der 20 SQL-Vorlagen und 2PC-Koordination.
