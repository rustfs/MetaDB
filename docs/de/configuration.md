# Konfiguration

[Überblick](./README.md) · [English](../en/configuration.md) · [Français](../fr/configuration.md) · [日本語](../ja/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` und `ServerTomlConfig::from_toml_file` / `from_toml_str` lesen TOML (`crates/metadb-server/src/config.rs`). Unbekannte Schlüssel werden ignoriert. Die Strukturen verwenden `#[serde(default)]` und setzen `deny_unknown_fields` nicht, und `from_toml_str` ruft `toml::from_str` direkt auf. Ein falsch geschriebener Schlüssel behält seine Vorgabe, auch ein leeres `security.shared_secret`, und der Start gelingt trotzdem. Jeder Abschnitt ist optional. Ausgelassene Felder nehmen die Vorgaben unten an.

## Vorgaben

| Schlüssel | Vorgabe | Von `open_and_serve` angewendet |
| :--- | :--- | :--- |
| `node.node_id` | `1` | Ja. Raft-Knotenkennung und Bootstrap-Peer |
| `node.cell_id` | `1` | Ja. Verzeichnisname `cell_<id>` und die einzige in diesem Prozess registrierte Zelle |
| `node.role` | `all-in-one` | Ja. Jeder andere Wert liefert einen Fehler |
| `node.data_dir` | `./metadb-data` | Ja |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | Ja. Bind-Adresse |
| `node.metrics_listen_addr` | nicht gesetzt | Ja, nur wenn gesetzt. `metadbd` bindet einen eigenen TCP-Listener |
| `clock.max_clock_drift_ms` | `500` | Ja. `HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432` (32 MiB) | **Nein.** Das Feld wird gespeichert und nie gelesen |
| `storage.block_cache_bytes` | `67108864` (64 MiB) | Ja. Gesamtbudget des 16-fachen Blockcaches |
| `storage.mvcc_retention_ms` | `5000` | Ja |
| `storage.bloom_bits_per_key` | `10` | Ja |
| `storage.locality_tombstone_threshold` | `128` | Ja. Abgebildet auf `LsmEngineConfig.tombstone_density_threshold` |
| `storage.locality_tombstone_ratio` | `0.50` | Ja. Mal 1000, auf 1..=1000 begrenzt, gespeichert als `tombstone_min_ratio_permille` |
| `sharding.bootstrap_table_shards` | `true` | Ja. `true` erzeugt die Shards 1, 2 und 3. `false` erzeugt Shard 1 über `[0x01, 0x04)` |
| `sharding.shard_split_threshold_bytes` | `4294967296` (4 GiB) | Ja. Die Split-Prüfung nutzt die ungefähre Shard-Größe |
| `security.shared_secret` | `""` | Ja. Leer heißt: Metadaten-, Transaktions- und Control-Plane-RPCs sind unauthentifiziert |

Es gibt keine Tabelle `[raft]`. `ServerTomlConfig.raft_options` wird von serde übersprungen und bleibt bei `RaftNodeOptions::default()` (Heartbeat 150 ms, Wahl 450–900 ms, Snapshot-Log-Behalten 0, Segment 64 MiB).

`to_lsm_config` setzt immer `auto_locality_compaction: true`.

## Beispiel

Diese Datei entspricht den Vorgaben, außer einem ausdrücklichen Datenverzeichnis, einem Metrik-Port und einem Geheimnis. `memtable_size_bytes` steht hier, damit eine kopierte Konfiguration nicht überrascht; eine Änderung ändert das Flush-Verhalten nicht.

```toml
[node]
node_id = 1
cell_id = 1
role = "all-in-one"
data_dir = "/var/lib/metadb"
grpc_listen_addr = "127.0.0.1:7800"
metrics_listen_addr = "127.0.0.1:9100"

[clock]
max_clock_drift_ms = 500

[storage]
memtable_size_bytes = 33554432
block_cache_bytes = 67108864
mvcc_retention_ms = 5000
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296

[security]
shared_secret = "replace-with-a-long-random-string"
```

Rollenzeichenketten sind kebab-case: `all-in-one`, `be`, `proxy`, `cell-manager`, `global-manager`.

CLI-Flags überschreiben die Datei nach dem Einlesen: `--node-id`, `--cell-id`, `--role`, `--listen`, `--data-dir`. `--cell-manager` ist kein TOML-Feld. Es ist nur ein Flag von `metadbd`.

## Sicherheit

Der Prozess beendet TLS nicht.

Wenn `shared_secret` nicht leer ist, verlangen `MetadataService`, `TransactionService` und `ControlPlaneService`, dass der Metadatenwert `authorization` gleich `Bearer <shared_secret>` ist. `metadb-ctl --auth-token` (Umgebungsvariable `METADB_AUTH_TOKEN`) sendet diesen Header. `metadbd --cell-manager` sendet dasselbe Geheimnis, wenn es `RegisterNode` aufruft.

`RaftTransportService` wird ohne den Interceptor registriert. Ein Bearer-Token authentifiziert Raft-RPCs auf diesem Port nicht.

Ein leeres Geheimnis lässt Metadaten-, Transaktions- und Control-Plane-RPCs zu. Der Code warnt beim Start davor. Ein Bearer-Geheimnis deckt `RaftTransportService` nicht ab, und ein TLS-Terminator vor dem Listener fügt diese Prüfung nicht hinzu: nach der Terminierung bleiben Raft-RPCs unauthentifiziert. Bevor ein nicht vertrauenswürdiger Client den Port erreichen kann, die Bindung auf einem vertrauenswürdigen Netz wie `127.0.0.1` lassen, oder jeden Dienst hinter einen Proxy setzen, der den Client authentifiziert (zum Beispiel gegenseitiges TLS), und den direkten Zugriff auf diesen Prozess sperren.

## Was sich nicht konfigurieren lässt

- Zuschneiden der Teilsysteme nach Rolle. Das Rollenfeld ist ein Zulassen/Ablehnen-Schalter, keine Funktionsmenge.
- Peer-Listen, Wählermengen oder ein Join-Token. Die Membership beginnt als dieser eine Prozess. Weitere Peers erscheinen erst nach `RegisterNode` plus `ScaleShardReplicas`, und diese Peers müssen bereits Raft-Mitglieder sein, die `RaftTransportService` für dieselben Shard-Logs sprechen. Ein zweiter all-in-one-Prozess tut das nicht. Siehe [Betrieb](./operations.md).
- Den Schlüsselbereich einer zweiten Zelle. `RegisterCell` liefert einen Fehler und speichert nichts. Ein zweiter all-in-one-Prozess mit leerem Verzeichnis tritt dem aktuellen Leader mit `[cluster] join` bei; das bleibt dieselbe Zelle.
- Memtable-Bytegrenze, Raft-Zeitgeber oder Locality-Compaction an/aus.
- TLS-Zertifikate.
