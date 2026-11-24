# MetaDB-Implementierungshandbuch

[English](../en/README.md) · [Français](../fr/README.md) · [日本語](../ja/README.md) · [简体中文](../zh/README.md)

Diese Seiten beschreiben den Baum `rustfs/MetaDB` so, wie er implementiert ist. Sie sind das Betriebs- und Entwicklungshandbuch für dieses Repository. Entwurfsnotizen zu einer größeren Topologie stehen in [`Architecture.md`](../../Architecture.md), [`docs/designs/`](../designs/SITEMAP.md) und den älteren Anleitungen unter [`docs/guides/`](../README.md). Wo diese Dokumente und der Code voneinander abweichen, gilt dieser Satz.

## Was dieses Repository ist

MetaDB ist eine reine Rust-Metadaten-Engine für Datei- und Objektnamensräume, geschrieben für [RustFS](https://github.com/rustfs). Das Datenmodell folgt der Verzeichnis-Topologie: Ein Verzeichnis und seine direkten Kinder teilen sich ein Schlüsselpräfix, sodass Erzeugen, Nachschlagen, Auflisten oder Löschen in einem einzelnen Verzeichnis auf einem Shard bleibt. Der Entwurf ist angepasst von Baidu und der Peking-Universität, *MetaDB: Putting File-System Structure Back into Distributed Databases*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1). Produktionszahlen in diesem Papier beschreiben Baidus Einsatz. Dieses Repository veröffentlicht keine entsprechende Messung.

Die Workspace-Version ist `0.1.0` (Apache-2.0). Die Rust-Werkzeugkette ist in `rust-toolchain.toml` auf **1.100.0** festgesetzt.

## Was ein Prozess tatsächlich ausführt

The default `metadbd` process is **all-in-one**. `be`, `proxy`, `cell-manager`, and `global-manager` also start when their configuration is valid. An all-in-one process has:

- ein Datenverzeichnis
- eine Zelle (`node.cell_id`, Vorgabe `1`)
- ein OpenRaft-Wähler je Shard (dieser Prozess)
- ein gRPC-Listener, der `MetadataService`, `TransactionService`, `ControlPlaneService`, `RaftTransportService` und `grpc.health.v1.Health` bedient
- eine Hintergrundschleife alle 250 ms, die Hilfsausgänge leert (einschließlich zellenübergreifendem `ApplyAuxBatch` über `GrpcRemoteAuxSink`), 2PC-Wiederherstellung ausführt, die Split-Regel prüft und Raft-Logs sichert

Shard-Splits (1 Wähler und 3 Wähler), `(PID, name)`-Splits im Dual-Modus, sicherer Leadership-Transfer (`ProbeTransferLeader`), die 3-Prozess-`metadbd`-Übung (`three_process_ha_rehearsal`) und der 4-Rollen-E2E-Test (`disaggregated_four_role_e2e`) werden in diesem Baum getestet. Siehe [Betrieb](./operations.md).

## Seitenübersicht

| Seite | Inhalt |
| :--- | :--- |
| [Architektur](./architecture.md) | Crates, Schlüssel, Speicher, Raft, Transaktionen, Sharding und die zwei Engines |
| [Bauen und starten](./build-and-run.md) | Werkzeugkette, Binärdateien, eingebettete Bibliothek, Beenden |
| [Konfiguration](./configuration.md) | TOML-Felder, Vorgaben und Felder, die der Prozess nicht anwendet |
| [Betrieb](./operations.md) | `metadb-ctl`, Metriken, Skalierung, Splits, Grenzen, Dokumentationslücken |
| [Tests und Benches](./testing.md) | `cargo test`, was jede Suite wirklich ausführt, `metadb-ctl bench` |

## Workspace-Crates

| Crate | Was der Code tut |
| :--- | :--- |
| `metadb-core` | Schlüssel-Codecs `0x01`–`0x07`, `ShardEpoch`, hybride logische Uhr (Standard-Driftgrenze 500 ms), 88-Byte-`InodeAttr` |
| `metadb-proto` | Protobuf- und tonic-Dienste, aufgeführt in [Architektur](./architecture.md) |
| `metadb-storage` | WAL-loses SSTable-LSM, `MANIFEST`, 16-facher LRU-Blockcache, MVCC im Speicher, Locality-Compaction |
| `metadb-raft` | `openraft`-0.10-Gruppen, segmentiertes CRC32C-`.raftlog`, Snapshots, Membership-Wechsel von Learner zu Voter |
| `metadb-tx` | Pushdown-Mutationen, Attributdeltas, Hilfsausgang, am Primäranker gebundene 2PC |
| `metadb-cluster` | Dauerhafte Zell- und Global-Manager, ID-Zuteiler, Routen-Cache, Split-Worker, Jumbo-Planer |
| `metadb-proxy` | Lexer und Parser für die 20 SQL-Vorlagen, zustandsloser Router auf die Transaktionsschicht |
| `metadb-server` | `PersistentMetaDbCluster`, `MetaDbEngine::new_single_node` im Speicher, `metadbd`, `metadb-ctl`, Prometheus-Text |

## Zwei Arten, die Engine aufzurufen

`MetaDbEngine::open` und `PersistentMetaDbCluster::open_and_serve` öffnen ein Datenverzeichnis, LSM und OpenRaft und binden gRPC. Das ist die Engine, die man betreibt.

`MetaDbEngine::new_single_node` baut einen Simulator im Speicher. Er öffnet kein Datenverzeichnis, kein LSM und keine OpenRaft-Gruppe. Die Tests `T1`–`T6` in `crates/metadb-server/tests/integration_tests.rs` verwenden ihn, einschließlich des Schritts „3 Wähler“ in `T6`, der nur eine Membership-Map im Speicher ändert.
