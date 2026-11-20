# Déploiement & Montée en Charge en Ligne (Français)

* **Retour à l'index**: [Présentation Français](./README.md) | [Portail](../../README.md)

The current binary runs a persistent deployment only as `all-in-one`. A second `all-in-one` process can join the same cell. Role-split processes and the multi-cell topology are a design target, not a deployment this repository runs. The `100B`–`10T+` range is a design model, not a measurement of this repository. Trillion-scale figures in `arXiv:2610.32247v1` are Baidu production metrics, not results of this repository.

---

## Étape 1 : Déploiement Mononœud Autonome (`metadbd --role all-in-one`) `[Implémenté]`

`[Implémenté]` means one `metadbd --role all-in-one` process in current source. It is not a multi-cell deployment. `MetaDbEngine::new_single_node` is an in-memory simulator with no disk and no Raft, not this process.

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

[security]
# Loopback development only. Literal 127.0.0.1. Remove this flag when you set shared_secret.
allow_unauthenticated = true
```

Rust 1.100.0 (`rust-toolchain.toml`). Only `all-in-one` starts. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.

The `[security]` block above is the explicit loopback development opt-in. It is not a credential. `grpc_listen_addr` and `metrics_listen_addr` stay on the literal IP `127.0.0.1`. The hostname `localhost` is not accepted for `allow_unauthenticated`. An empty secret without that flag does not start the process.

```bash
metadbd --config /etc/metadb/standalone.toml
```

To require `authorization: Bearer <secret>` on metadata, transaction, control-plane, Raft transport, and `GET /metrics`, replace that `[security]` block. Do not leave `allow_unauthenticated` in the file. Both together are rejected. A whitespace-only secret is rejected even if the flag remains. Peer Raft clients send the same token. `metadb-ctl --auth-token` (or `METADB_AUTH_TOKEN`) and `metadbd --join` / `--cell-manager` send that header.

```toml
[security]
shared_secret = "replace-with-a-long-random-string"
```

---

## Étape 2 : Découpage Dynamique sur Frontière `PID` `[Implémenté]`

`[Implémenté]` means in-process PID-boundary splits inside that same all-in-one process. It is not a multi-host topology. Jumbo isolation is a route flag and does not move peer shards.

Lorsqu'un shard dépasse `shard_split_threshold_bytes`, `PolicyShardSplitter` choisit une frontière stricte `group_key = PID` (`[0x01][pid: 8B BE]`) afin que `(PID, "_attr_")` et tous ses enfants restent sur le même shard.

---

## Stage 3: Intended multi-cell topology (not deployable today)

Stage 3 is a design target. The current binary does not deploy it.

`metadbd` only runs `--role all-in-one`. `PersistentMetaDbCluster::open_and_serve` in `crates/metadb-server/src/lib.rs` rejects `be`, `proxy`, `cell-manager`, and `global-manager` before it listens. `RegisterCell` stores a second all-in-one cell in the caller's `GLOBAL_CELL_DIRECTORY` after that cell's routes cover the requested range. The caller does not forward keys. This is not a disaggregated deployment.

A second `metadbd --role all-in-one` can join the **same** cell when `--join` or `--cell-manager` names the current leader and the data directory is fresh. That path is covered by `crates/metadb-server/tests/join_existing_group.rs`. The joiner is still an all-in-one process. It is not a role-trimmed backend, and the join does not create another cell. See [Operations](../../fr/operations.md).

### Step 3.1: Voters inside one cell

Do not start `metadbd --role be`. That role exits in `open_and_serve`, so those commands are not a runbook. `metadb-ctl raft scale-replicas` calls `ScaleShardReplicas` and requires the requested voter count to already be registered in the cell. A fresh all-in-one cell has one node, so `--target-voters 3` fails until those nodes exist. The constraints are in [Operations](../../fr/operations.md).

### Step 3.2: Design model for a disaggregated topology (`100B – 10T+` records)

`100B – 10T+` is a design-model range: arithmetic for how a future multi-cell topology might be sized. It is not a record count this repository has stored. It is also not a Baidu production figure claimed as a result of `rustfs/MetaDB`. Baidu's published sizes, including the BOS2 figure of 1.848 trillion records in `arXiv:2610.32247v1`, are Baidu production metrics, not results of this repository.

The table is the intended role split. This binary does not run those roles. Jumbo isolation in this tree is a route flag. It does not move peer shards onto a dedicated host.

| Role | Design `--role` | Intended responsibility |
| :--- | :--- | :--- |
| **Global Manager** | `global-manager` | Would map coarse `[start_key, end_key)` intervals to `CellId`, allocate `65,536`-ID blocks, and coordinate cross-Cell directory rename leases. |
| **Cell Local Manager** | `cell-manager` | Would process BE heartbeats (`20K–50K` shards/Cell) and orchestrate `PID` splits and membership changes. |
| **Backend Storage (`BE`)** | `be` | Would host Multi-Raft shard groups and the LSM engine on NVMe hosts. |
| **Stateless Proxy** | `proxy` | Would embed `MetaCache`, compile the 20 SQL templates, and coordinate cross-shard 2PC. |

`metadb-ctl cell register` calls `RegisterCell`. Without `--start-key` and `--end-key` it sends `[01, 04)`, which overlaps a default cell and exits non-zero. With those keys it persists the cell only when the endpoint's routes cover that range. It does not forward keys.
