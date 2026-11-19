# Scale control plane: this increment and what is still missing

[Overview](./README.md) · [Architecture](./architecture.md) · [Operations](./operations.md)

This page is the evidence record for multi-cell routing, role separation, Jumbo shard placement, heartbeats, and membership safety. It describes the tree after the control-plane increment that added `CELL_CONTROL_PLANE`. It is not a first-ship pass and it is not a scale gate. Figures in arXiv:2610.32247 (including Baidu's 575-node BOS2 cluster and 1.848 trillion records) are Baidu production metrics. They are not results of this repository, and this change does not produce them.

`Architecture.md` section 12 still says several of these subsystems are "Implemented & Tested". Where that matrix and this page disagree, follow this page and the code. Locale guides under `docs/{fr,de,ja,zh}` were not rewritten here.

Root `Architecture.md` was left unchanged on purpose. Consolidating it with the locale guides is a separate documentation workstream.

## What this increment does

1. **Heartbeats are durable and are sent.** `StoreHeartbeat` for a registered node fsyncs `CELL_CONTROL_PLANE` (CRC32C frame, version 1). `monotonic_seq` must be non-zero. A lower sequence is rejected and the stored body stays. The same sequence and the same disk-free value and shard-id annotation is a replay and does not rewrite the timestamp. The same sequence with a different body is a conflict and does not overwrite. Shard descriptors in the RPC are stored only as `reported_shard_ids`. They are not copied into `CELL_MANAGER_STATE` or `CompositeMetaCache`. An unregistered node is rejected. A failed fsync leaves the previous record and returns an error.
2. **The process sends its own heartbeat.** The 250 ms maintenance loop persists a local heartbeat about every 3 seconds (`HEARTBEAT_INTERVAL_MS`). Disk free space is unknown on that record; a zero is not stored as a measurement. When this process is not the Raft leader, it sends that same sequence to the leader's control-plane address and retries the same sequence once. The sender does not attach shard descriptors. A lost response does not roll the local sequence back. The next interval uses a new sequence.
3. **Jumbo placement is a record plus a narrow Raft action.** `JumboIsolated` remains the detection flag from `JumboIsolationPlanner`. `reconcile_jumbo_placement` writes a placement record from live Raft voters:
   - `Blocked` when the dedicated node is not already a voter, the shard has no leader, or a move was refused. No voter is added for a node that has only a registered address.
   - `LeaderOnDedicated` when that node is the current leader and peer shards still list it, or when this process hosts no other shard. The second case is not evidence that a co-located shard was moved.
   - `Recorded` when leadership transfer or a peer replacement is still outstanding.
   - `DedicatedNodeNotShared` only when at least one other local shard exists and none of those shards currently has the dedicated node as a voter.
   - `committed_moves` grows only after this process's own Raft call reaches that postcondition. A restart does not invent a move it cannot prove it committed.
4. **Membership changes from this path keep a majority.** `jumbo_membership_change_allowed` refuses an empty set, a drop in fault tolerance (`(n-1)/2`), and a new set whose overlap is not a majority of the old set. The peer replacement keeps the same voter count and uses a node that is already a voter of some local shard. Another jumbo shard's dedicated node is not used as that spare. Operator `scale_out_replicas` / `remove_voter` is unchanged and can still shrink on purpose.
5. **Leadership transfer uses a real Raft RPC.** `RaftTransportService.TransferLeader` carries OpenRaft's `TransferLeaderRequest` and the server calls `handle_transfer_leader`. The placement path calls `trigger().transfer_leader` only when this process is the leader and the target is already a voter. OpenRaft disables the leader lease when that call is submitted. If the target is not leader within 3 seconds, the move is not recorded as committed and the next attempt waits 30 seconds. A one-voter cell never submits it, because the dedicated node is either already the leader or not a voter.

`RegisterCell` still returns an error. `open_and_serve` still rejects every role except `all-in-one`.

## Implemented versus missing

| Area | Evidence in this tree | Status |
| :--- | :--- | :--- |
| Multi-cell routing | `DurableGlobalManager::register_cell` is an in-process map. `open_and_serve` registers one cell over `[0x01, 0x04)`. `RegisterCell` returns "not implemented" and stores nothing (`grpc_services.rs`). `GetRoutes` for an unknown non-zero `cell_id` fails closed. | **Missing.** No durable cell directory, no cross-process range map, no forwarding between cells. |
| Role separation | `ServerRole` parses `be`, `proxy`, `cell-manager`, and `global-manager`. `open_and_serve` returns before listen for every role except `all-in-one` (`lib.rs`). | **Missing.** One process still hosts the global manager, the cell manager, the proxy, and the Raft groups. |
| Jumbo detection flag | `JumboIsolationPlanner` and `maybe_split_or_isolate_shard` persist `ShardHealthState::JumboIsolated` in `CELL_MANAGER_STATE`. | **Present, and it is only a flag.** It does not move files. |
| Jumbo leadership placement | `reconcile_jumbo_placement` plus `RaftTransportService.TransferLeader`. | **Code is present.** It runs only when the dedicated node is already a voter and this process is the leader. It was not executed against three voters in this change. |
| Jumbo peer drain / file migration | Same-size replacement is planned and calls the existing learner promotion or `change_membership` only when a proven spare keeps fault tolerance. | **Partial.** Uniform voter sets (the join path adds a node to every shard) have no spare, so the plan stays blocked. Shard directories are not copied to another host. |
| Heartbeat persistence | `CELL_CONTROL_PLANE`, `accept_remote_heartbeat`, `publish_local_heartbeat`. | **Present** for node liveness, sequence, optional reported disk free, and shard-id annotations. |
| Heartbeat sending | `PersistentMetaDbCluster::publish_control_heartbeat` from the maintenance loop. | **Present** as liveness to the current leader. It does not carry shard stats and it does not schedule splits. |
| Heartbeat-driven scheduling | Design text in `docs/designs/sharding_and_control_plane.md` §6.1. | **Missing.** Splits still run from the local maintenance tick. |
| Membership safety for operator scale-in | `plan_membership_voters`, `remove_voter`, joint-config handling in `uniform_voter_addrs`. | **Present, and it can shrink.** Scale-in to 2 or 1 voters is an explicit operator path. It is not a three-VM result. |
| Membership safety for jumbo moves | `jumbo_membership_change_allowed`. | **Present** for the jumbo path only. It does not change operator scale-in. |
| Raft majority on the data path | OpenRaft joint consensus via `ConfChangeOrchestrator`. | **Unchanged** by this increment. This page does not re-certify it. |
| 2PC lease / storage formats | `a48d038` ("Pin 2PC lease pre-images and reject an over-cap rename TTL") is not an ancestor of this branch. | **Not imported.** Lease and storage formats were not changed. |

## Remaining backlog and dependencies

These items block a scale gate. None of them is delivered by this change.

1. **Second cell.** A durable global range table, a real `RegisterCell` that does not succeed until the new cell's manager is reachable, and routing that fails closed when the range is only partly published. Depends on role separation: today every process is its own global manager, so two processes do not share one cell map.
2. **Role separation.** `be`, `proxy`, `cell-manager`, and `global-manager` must each start only their own listeners and state. That work has to preserve the auth fail-closed rules owned by the auth/config workstream. This change does not trim subsystems.
3. **Jumbo file placement.** Moving a shard directory onto a dedicated disk, streaming its snapshot to a node that has not joined, and draining peer shards off that disk. The safe replacement in this tree cannot fire on the uniform voter sets `scale_out_replicas` creates, because there is no spare voter. Adding a voter that has not joined is intentionally refused here.
4. **Leadership transfer at three voters.** The RPC and the caller exist. A regression that boots three metadata voters, detects a jumbo shard, and observes the leader change was not run. Until that run exists, do not treat `committed_moves` as a demonstrated three-node result. Mixed versions are unsafe: OpenRaft disables the leader lease when `transfer_leader` is submitted, and a peer that does not implement `TransferLeader` makes that broadcast fail.
5. **Heartbeat as a scheduler.** Per-shard bytes, QPS, and disk free measured by this process, sent on a trusted channel, and used only as hints. The scheduler must keep using local Raft and LSM state for splits. Client descriptors must stay non-authoritative. A 3-second cell-wide heartbeat bus for tens of thousands of shards is not built.
6. **Global and cell managers on Raft.** `CELL_MANAGER_STATE` and `CELL_CONTROL_PLANE` are local files. They are not a replicated control-plane group. A follower's placement file is not the leader's file.
7. **2PC lease pre-images.** Commit `a48d038` is unreviewed and was not imported. Later lease work has to land on its own review. This increment does not depend on it and does not change the 3-second lease, the 60-second cap, or the intent/txn record layout.
8. **Operator status for the placement record.** There is no `metadb-ctl` field and no metrics series for `PlacementPhase`. `TriggerShardSplit` still reports `split_executed = false` for a jumbo shard even when a later leadership transfer is only recorded in `CELL_CONTROL_PLANE`. Health, metrics, and operator Raft timer configuration belong to another workstream and were not edited.
9. **Docs outside this English set.** `docs/{fr,de,ja,zh}` still describe the previous heartbeat and jumbo behavior in places. Root `Architecture.md` section 12 still marks the control plane and jumbo isolation as implemented. Those updates belong to documentation consolidation.
10. **Gates this page does not move.** No-ship stays in force. A first-ship gate is a separate bar from a scale gate (multi-cell, disaggregated roles, jumbo disk isolation, and a heartbeat bus that can carry a cell of 20k–30k shards). This increment is neither gate. CI workflows were not edited. The commands below were not run.

## Validation not run

Do not read this file as a test report. The following were not executed while the change was written:

- `cargo test -p metadb-cluster --lib jumbo_placement`
- `cargo test -p metadb-cluster --lib control_plane_state`
- `cargo test -p metadb-cluster --test scale_control_plane_tests`
- `cargo test -p metadb-server --test control_plane_claim_tests`
- `cargo test --workspace --locked`
- Any benchmark, three-voter jumbo transfer, or multi-cell drill

`crates/metadb-cluster/tests/scale_control_plane_tests.rs` and the heartbeat cases in `crates/metadb-server/tests/control_plane_claim_tests.rs` are the regression source. They cover sequence replay, conflict, a failed fsync, reload, a corrupt `CELL_CONTROL_PLANE` file, and a one-voter placement that must not change voter sets. They do not boot three voters.
