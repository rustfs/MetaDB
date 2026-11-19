# Deployment & Online Cluster Scale-Out (English)

* **Back to Guide Index**: [English Overview](./README.md) | [Hub](../../README.md)

MetaDB uses a WAL-less LSM storage engine and OpenRaft state machine for persistent all-in-one deployments. Multi-node Raft is exercised by integration tests; the disaggregated and multi-Cell daemon topology remains a design target.

> **Implementation Status**: Persistent LSM, Raft replication, local shard splitting, and all-in-one gRPC/CLI operations have regression coverage. Only `all-in-one` starts a daemon. Other roles and remote Cell registration are rejected. See [correctness and compatibility notes](../../designs/correctness_hardening.md) before upgrading.

---

## Stage 1: Standalone Single-Node Deployment (`metadbd --role all-in-one`) `[Implemented]`

In Stage 1, `GlobalManager`, `CellLocalManager`, `MetaCache`, `Proxy`, and the `BE` storage engine run inside a single process backed by **`1-Voter` OpenRaft**. Local KV WAL files remain disabled because the segmented CRC32C `.raftlog` provides full crash durability with a single sequential `fdatasync`.

### `standalone.toml` Configuration

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
memtable_size_bytes = 33554432          # Flush the active memtable at 32 MiB
max_imm_memtables = 4                   # Immutable-table backpressure cap
block_cache_bytes = 536870912           # 512 MiB (16-shard LRU)
mvcc_retention_ms = 5000                # 5s In-Memory MVCC window
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true           # Shards 1 (Meta), 2 (Quota), 3 (SecondaryIndex)
shard_split_threshold_bytes = 4294967296 # 4 GiB
```

### Launching Standalone Node

Rust **1.100.0** is pinned in `rust-toolchain.toml`. Only `--role all-in-one` starts a process. `be`, `proxy`, `cell-manager`, and `global-manager` are parsed but rejected: this binary does not trim subsystems by role.

```bash
metadbd --config /etc/metadb/standalone.toml
```

### Network exposure

`MetadataService`, `TransactionService`, `ControlPlaneService`, and `RaftTransportService` do **not** terminate TLS. Neither does the metrics listener. With an empty `[security].shared_secret` (the default) they perform no application authentication, and only loopback binds are accepted. A wildcard or other non-loopback address without a non-empty secret refuses to start. The default `grpc_listen_addr` is `127.0.0.1:7800`. A TLS or certificate key in this file is rejected rather than served as plaintext.

Set a shared secret to require `authorization: Bearer <secret>` on metadata, transaction, control-plane, Raft transport, and `GET /metrics`. Peer Raft clients propagate the same token. `metadb-ctl --auth-token` (or `METADB_AUTH_TOKEN`) and `metadbd --cell-manager` send that header:

```toml
[security]
shared_secret = "replace-with-a-long-random-string"
```

---

## Stage 2: Single-Node Automatic `PID`-Boundary Shard Splitting `[Implemented]`

As metadata grows past `shard_split_threshold_bytes`, `CellLocalManager` and `PolicyShardSplitter` automatically split `MetaTable` shards along `group_key = PID` boundaries (`[0x01][pid: 8B BE]`):
* `(PID, "_attr_")` and all direct children `(PID, child_name)` stay colocated on the same shard.
* Multiple local OpenRaft shard groups parallelize MemTable flushes and compactions across NVMe queues.

---

## Stage 3: Intended Multi-Cell Topology

Stage 3 is a design target. The current binary does not deploy it.

`metadbd` only runs `--role all-in-one`. `PersistentMetaDbCluster::open_and_serve` in `crates/metadb-server/src/lib.rs` rejects `be`, `proxy`, `cell-manager`, and `global-manager` before it listens. `RegisterCell` in `crates/metadb-server/src/grpc_services.rs` returns an application error and does not store a second cell. This tree has no live multi-process multi-Cell deployment.

A second `metadbd --role all-in-one` can join the **same** cell when `--join` or `--cell-manager` names the current leader and the data directory is fresh. That path is covered by `crates/metadb-server/tests/join_existing_group.rs`. The joiner is still an all-in-one process. It is not a role-trimmed backend, and the join does not create another cell. See [Operations](../../en/operations.md#scale-out).

### Step 3.1: Voters Inside One Cell

Do not start `metadbd --role be`. That role exits in `open_and_serve`, so those commands are not a runbook. `metadb-ctl raft scale-replicas` calls `ScaleShardReplicas` and requires the requested voter count to already be registered in the cell. A fresh all-in-one cell has one node, so `--target-voters 3` fails until those nodes exist. The constraints are in [Operations](../../en/operations.md).

### Step 3.2: Design Model for a Disaggregated Topology (`100B – 10T+` Records)

`100B – 10T+` is a design-model range: arithmetic for how a future multi-cell topology might be sized. It is not a record count this repository has stored. It is also not a Baidu production figure claimed as a result of `rustfs/MetaDB`. Baidu's published sizes, including the BOS2 figure of 1.848 trillion records in `arXiv:2610.32247v1`, are Baidu production data.

The table is the intended role split. This binary does not run those roles.

| Role | Design `--role` | Intended Responsibility |
| :--- | :--- | :--- |
| **Global Manager** | `global-manager` | Would map coarse `[start_key, end_key)` intervals to `CellId`, allocate `65,536`-ID blocks, and coordinate cross-Cell directory rename leases. |
| **Cell Local Manager** | `cell-manager` | Would process BE heartbeats (`20K–50K` shards/Cell) and orchestrate `PID` splits, jumbo isolation, and membership changes. |
| **Backend Storage (`BE`)** | `be` | Would host Multi-Raft shard groups and the LSM engine on NVMe hosts. |
| **Stateless Proxy** | `proxy` | Would embed `MetaCache`, compile the 20 SQL templates, and coordinate cross-shard 2PC. |

`metadb-ctl cell register` calls `RegisterCell` and exits non-zero. It does not register a cell. Do not use it as a federation step.
