# Konfiguration

[Überblick](./README.md) · [English](../en/configuration.md) · [Français](../fr/configuration.md) · [日本語](../ja/configuration.md) · [简体中文](../zh/configuration.md)

`metadbd --config` und `ServerTomlConfig::from_toml_file` / `from_toml_str` lesen TOML (`crates/metadb-server/src/config.rs`). Jeder bekannte Abschnitt ist optional. Ausgelassene Felder nehmen die Vorgaben unten an.

A key fails parsing when its normalized form (lowercase, hyphens become underscores) contains one of these substrings, including inside nested tables and arrays: `secret`, `password`, `passwd`, `token`, `credential`, `auth`, `anonymous`, `anon`, `insecure`, `api_key`, `apikey`, `bearer`, `jwt`, `private_key`, `privatekey`, `encryption`, `encrypt`, `tls`, `ssl`, `cert`, `x509`, `mtls`, `cipher`, `keystore`, `https`, `ca_file`, `cafile`, or the normalized key starts with `secur`. The only exceptions are the direct keys `[security].shared_secret` and `[security].allow_unauthenticated`. Any other key directly under `[security]` also fails parsing. This is the classifier, not a sample list: a key is not ignored merely because its name is absent from a shorter summary. Keys that do not match this classifier are still ignored. An empty `shared_secret` does not start the process. Development without credentials requires `allow_unauthenticated = true` and a literal loopback bind (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that flag. A wildcard or any other non-loopback address cannot use it. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected.

## Vorgaben

| Schlüssel | Vorgabe | Von `open_and_serve` angewendet |
| :--- | :--- | :--- |
| `node.node_id` | `1` | Ja. Raft-Knotenkennung und Bootstrap-Peer |
| `node.cell_id` | `1` | Ja. Verzeichnisname `cell_<id>` und die einzige in diesem Prozess registrierte Zelle |
| `node.role` | `all-in-one` | Ja. Jeder andere Wert liefert einen Fehler |
| `node.data_dir` | `./metadb-data` | Ja |
| `node.grpc_listen_addr` | `127.0.0.1:7800` | Ja. Bind-Adresse |
| `node.metrics_listen_addr` | nicht gesetzt | Applied only when set. Separate TCP listener. An empty secret does not listen unless `allow_unauthenticated = true`, and that flag is loopback-only |
| `clock.max_clock_drift_ms` | `500` | Ja. `HybridLogicalClock::with_max_drift_ms` |
| `storage.memtable_size_bytes` | `33554432` (32 MiB) | Ja. Versiegelt und schreibt die aktive Memtable aus, sobald ihre angerechneten Bytes diese Größe erreichen. Werte unter 1 gelten als 1 |
| `storage.max_imm_memtables` | `4` | Ja. Schreiber warten, statt mehr unveränderliche Memtables als diese Obergrenze einzureihen |
| `storage.block_cache_bytes` | `67108864` (64 MiB) | Ja. Gesamtbudget des 16-fachen Blockcaches |
| `storage.mvcc_retention_ms` | `5000` | Ja |
| `storage.bloom_bits_per_key` | `10` | Ja |
| `storage.locality_tombstone_threshold` | `128` | Ja. Abgebildet auf `LsmEngineConfig.tombstone_density_threshold` |
| `storage.locality_tombstone_ratio` | `0.50` | Ja. Mal 1000, auf 1..=1000 begrenzt, gespeichert als `tombstone_min_ratio_permille` |
| `sharding.bootstrap_table_shards` | `true` | Ja. `true` erzeugt die Shards 1, 2 und 3. `false` erzeugt Shard 1 über `[0x01, 0x04)` |
| `sharding.shard_split_threshold_bytes` | `4294967296` (4 GiB) | Ja. Die Split-Prüfung nutzt die ungefähre Shard-Größe |
| `security.shared_secret` | `""` | Empty does not start the process. Loopback without credentials requires `allow_unauthenticated = true`. A non-empty secret requires the same bearer on gRPC and metrics. A whitespace-only secret is rejected |
| `security.allow_unauthenticated` | `false` | Explicit opt-in for an empty secret on a literal loopback IP (`127.0.0.1` or `::1`) only. The hostname `localhost` is not accepted. Rejected together with a usable secret. Rejected on a wildcard or any other non-loopback bind. A whitespace-only secret is rejected even with this flag |

`[raft]` is loaded. `ServerTomlConfig.raft_options` is not `#[serde(skip)]`. Omitted fields use heartbeat 150 ms, election timeout 450–900 ms, `max_in_snapshot_log_to_keep` 0, and segment size 64 MiB (`67108864`). Unknown keys inside `[raft]` fail parsing. `election_timeout_min_ms` must be greater than `heartbeat_interval_ms` and less than `election_timeout_max_ms`. `max_segment_bytes = 0` is rejected. The values apply when each local Raft group starts and are not reloaded. `max_segment_bytes` does not rewrite segments already on disk. `join_timeout` is not a TOML field. There is no gRPC health RPC. `/livez`, `/readyz`, and `/healthz` exist only when `metrics_listen_addr` is set. `/readyz` does not wait for the 250 ms 2PC recovery pass. See [Operability](../en/operability.md).


`to_lsm_config` setzt immer `auto_locality_compaction: true`.

## Beispiel

Diese Datei entspricht den Vorgaben, außer einem ausdrücklichen Datenverzeichnis, einem Metrik-Port und einem Geheimnis. `memtable_size_bytes` ist die Flush-Schwelle der aktiven Memtable. `max_imm_memtables` ist die Gegendruck-Obergrenze für unveränderliche Tabellen.

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
max_imm_memtables = 4
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

Dieser Prozess beendet TLS nicht. Es gibt keine Zertifikatskonfiguration, die natives TLS einschaltet. Ein TLS-, SSL- oder Zertifikatsschlüssel in der Datei lässt das Parsen fehlschlagen, und der Prozess dient nicht im Klartext. TLS in einem externen Proxy beenden und keinen TLS-Schlüssel in dieser Datei lassen.

Wenn `shared_secret` nicht leer ist, verlangen `MetadataService`, `TransactionService`, `ControlPlaneService` und `RaftTransportService`, dass `authorization` genau `Bearer <shared_secret>` ist. `GET /metrics` auf `metrics_listen_addr` verlangt denselben `Authorization`-Header. Ein fehlender Header, ein leerer Bearer, ein Präfix oder ein längerer Token wird abgelehnt. Der Fehlerkörper der Metriken ist kein Prometheus-Text. Raft-Peers senden denselben Bearer. `metadb-ctl --auth-token` (Umgebungsvariable `METADB_AUTH_TOKEN`) und `metadbd --join` / `--cell-manager` senden ihn. `metadb-ctl cluster status` ruft `GetRoutes` auf und liegt unter demselben Interceptor.

An empty secret does not start the process. Loopback is not an implicit exception. Development without credentials requires `allow_unauthenticated = true` and every listener on a literal loopback IP (`127.0.0.1` or `::1`). The hostname `localhost` is not accepted for that flag. A wildcard (`0.0.0.0`, `::`) or any other non-loopback address cannot use it. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. Listeners stay plaintext. Keep an opted-in listener on a trusted network, or put every service behind a proxy that authenticates the client and block direct access to this process.

## Was sich nicht konfigurieren lässt

- Zuschneiden der Teilsysteme nach Rolle. Das Rollenfeld ist ein Zulassen/Ablehnen-Schalter, keine Funktionsmenge.
- Peer-Listen, Wählermengen oder ein Join-Token. Die Membership beginnt als dieser eine Prozess. Weitere Peers erscheinen erst nach `RegisterNode` plus `ScaleShardReplicas`, und diese Peers müssen bereits Raft-Mitglieder sein, die `RaftTransportService` für dieselben Shard-Logs sprechen. Ein zweiter all-in-one-Prozess tut das nicht. Siehe [Betrieb](./operations.md).
- Den Schlüsselbereich einer zweiten Zelle. `RegisterCell` liefert einen Fehler und speichert nichts. Ein zweiter all-in-one-Prozess mit leerem Verzeichnis tritt dem aktuellen Leader mit `[cluster] join` bei; das bleibt dieselbe Zelle.
- Locality-compaction on/off. `to_lsm_config` always enables it. Raft timers are the `[raft]` table; see [Operability](../en/operability.md).
- Native TLS-Zertifikate. Ein TLS-Schlüssel in dieser Datei wird abgelehnt. Die Terminierung gehört in einen externen Proxy.
