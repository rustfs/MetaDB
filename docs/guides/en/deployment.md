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
memtable_size_bytes = 33554432          # Reserved; not an active flush threshold
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

## Stage 3: Multi-Node Raft Tests and Intended Multi-Cell Topology

### Step 3.1: Zero-Downtime Upgrade from `1-Voter` to `3-Voter` HA

> **Current binary**: role-trimmed `be` / `proxy` / `cell-manager` / `global-manager` processes are not implemented. `metadbd` only runs `all-in-one`. The steps below describe the intended topology; launching those roles fails fast instead of silently starting every subsystem.

1. Start two additional Backend (`be`) nodes pointing to the existing control plane:
   ```bash
   metadbd --role be --node-id 2 --listen 10.0.1.12:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb
   metadbd --role be --node-id 3 --listen 10.0.1.13:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb
   ```
2. Trigger online replica promotion via `metadb-ctl`:
   ```bash
   metadb-ctl raft scale-replicas --cell-id 1 --target-voters 3
   ```
   **Internal Execution**:
   * `ConfChangeOrchestrator` adds Nodes `2` and `3` as non-voting `Learner`s (`AddLearner`).
   * Leader triggers a Coordinated Flush Barrier, hard-links an SSTable checkpoint, and streams `InstallSnapshot` + tail `.raftlog` entries.
   * Once replication lag drops below `64` entries, OpenRaft executes joint-consensus `ChangeMembership` promoting Nodes `2` and `3` to `Voter`s with **zero write downtime**.

### Step 3.2: Disaggregated Multi-Cell Topology (`100B – 10T+` Records)

The following table describes an intended topology, not runnable role-trimmed daemon deployments:

| Role | `--role` Flag | Recommended Deployment | Responsibility |
| :--- | :--- | :--- | :--- |
| **Global Manager** | `global-manager` | 3-node Raft group | Maps coarse `[start_key, end_key)` intervals to `CellId`; allocates `65,536`-ID blocks; coordinates cross-Cell directory rename leases. |
| **Cell Local Manager** | `cell-manager` | 3-node Raft group per Cell | Processes 3s BE heartbeats (`20K–50K` shards/Cell), orchestrates `PID` splits, Jumbo node isolation, and `ConfChange`. |
| **Backend Storage (`BE`)** | `be` | `12 – 60+` NVMe hosts per Cell | Hosts Multi-Raft shard groups, `MemMvccStore`, `LsmStorageBackend`, `PushdownExecutor`, and `AuxDispatcher`. |
| **Stateless Proxy** | `proxy` | Autoscaled / co-deployed with RustFS | Embeds `MetaCache` (`>99.9%` single-hop hit rate), compiles 20 SQL templates, and coordinates cross-shard 2PC. |

```bash
# Register a new Cell in GlobalManager for horizontal expansion
metadb-ctl cell register --cell-id 2 --endpoints 10.0.2.10:7801,10.0.2.11:7801,10.0.2.12:7801
```
