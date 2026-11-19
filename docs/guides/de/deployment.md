# Bereitstellung & Online-Skalierung (Deutsch)

* **Zurück zur Übersicht**: [Deutsch Übersicht](./README.md) | [Hub](../../README.md)

Das aktuelle Binary startet eine persistente Bereitstellung nur als `all-in-one`. Integrationstests decken Raft mit mehreren Prozessen in derselben Zelle ab. Nach Rollen getrennte Prozesse und die Multi-Cell-Topologie sind ein Entwurfsziel, keine Bereitstellung, die dieses Repository betreibt. Der Bereich 100 Milliarden bis 10 Billionen und mehr ist ein Entwurfsmodell, kein hier gemessenes Ergebnis. Die Billionenwerte in `arXiv:2610.32247v1` sind Produktionsdaten von Baidu.

> **Implementierungsstand**: Persistentes LSM, Raft-Replikation, lokale Shard-Splits und all-in-one gRPC/CLI haben Regressionstests. Nur `all-in-one` startet einen Daemon. `be`, `proxy`, `cell-manager`, `global-manager` und entferntes `RegisterCell` werden abgelehnt. Siehe die [Korrektheitshinweise](../../designs/correctness_hardening.md).

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

## Stufe 3: Vorgesehene Multi-Cell-Topologie

Stufe 3 ist ein Entwurfsziel. Das aktuelle Binary stellt sie nicht bereit.

`metadbd` führt nur `--role all-in-one` aus. `PersistentMetaDbCluster::open_and_serve` in `crates/metadb-server/src/lib.rs` lehnt `be`, `proxy`, `cell-manager` und `global-manager` ab, bevor es zuhört. `RegisterCell` in `crates/metadb-server/src/grpc_services.rs` liefert einen Anwendungsfehler und speichert keine zweite Zelle. Dieser Baum hat keine laufende Mehrprozess- und Multi-Cell-Bereitstellung.

Ein zweiter `metadbd --role all-in-one` kann derselben Zelle beitreten, wenn `--join` oder `--cell-manager` den aktuellen Leader nennt und das Datenverzeichnis frisch ist. Diesen Pfad deckt `crates/metadb-server/tests/join_existing_group.rs` ab. Der Beitretende bleibt ein all-in-one-Prozess. Er ist kein nach Rolle beschnittenes Backend, und der Beitritt erzeugt keine weitere Zelle. Siehe [Betrieb](../../de/operations.md).

### 3.1 Wähler innerhalb einer Zelle

Starten Sie `metadbd --role be` nicht. Diese Rolle beendet sich in `open_and_serve`; die Befehle sind keine Betriebsanleitung. `metadb-ctl raft scale-replicas` ruft `ScaleShardReplicas` auf und verlangt, dass die gewünschte Wählerzahl bereits in der Zelle registriert ist. Eine frische all-in-one-Zelle hat einen Knoten, daher scheitert `--target-voters 3`, bis diese Knoten vorhanden sind. Die Grenzen stehen in [Betrieb](../../de/operations.md).

### 3.2 Entwurfsmodell einer getrennten Topologie (`100B – 10T+` Einträge)

`100B – 10T+` (100 Mrd. – 10 Bio. und mehr) ist eine **Spanne des Entwurfsmodells**: eine Rechnung dafür, wie eine künftige Multi-Cell-Topologie dimensioniert werden könnte. Es ist keine Eintragszahl, die dieses Repository gespeichert hat, und keine Baidu-Produktionszahl, die als Ergebnis von `rustfs/MetaDB` ausgegeben wird. Die in `arXiv:2610.32247v1` veröffentlichten Clustergrößen, einschließlich 1,848 Billionen Einträge (`1,848 × 10^12`) von BOS2, sind Produktionsdaten von Baidu.

Die Tabelle ist die vorgesehene Rollenaufteilung. Dieses Binary führt diese Rollen nicht aus.

| Rolle | `--role` im Entwurf | Vorgesehene Aufgabe |
| :--- | :--- | :--- |
| **Global Manager** | `global-manager` | Würde grobe Intervalle `[start_key, end_key)` auf eine `CellId` abbilden, ID-Blöcke zu `65.536` vergeben und zellübergreifende Verzeichnis-Umbenennungsleases koordinieren. |
| **Cell Local Manager** | `cell-manager` | Würde BE-Heartbeats (`20K–50K` Shards je Zelle) verarbeiten und `PID`-Splits, Jumbo-Isolierung und Mitgliedschaftsänderungen steuern. |
| **Backend-Speicher (`BE`)** | `be` | Würde Multi-Raft-Shard-Gruppen und die LSM-Engine auf NVMe-Hosts tragen. |
| **Zustandsloser Proxy** | `proxy` | Würde `MetaCache` einbetten, die 20 SQL-Vorlagen übersetzen und shardübergreifendes 2PC koordinieren. |

`metadb-ctl cell register` ruft `RegisterCell` auf und endet mit einem Status ungleich null. Es registriert keine Zelle. Verwenden Sie es nicht als Föderationsschritt.
