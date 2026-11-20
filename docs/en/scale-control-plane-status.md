# Scale control plane: this increment and what is still missing

[Overview](./README.md) · [Architecture](./architecture.md) · [Operations](./operations.md)

This page is the evidence record for multi-cell routing, role separation, Jumbo shard placement, heartbeats, and membership safety. It describes the tree after the control-plane increment that added `CELL_CONTROL_PLANE`. It is not a first-ship pass and it is not a scale gate. Figures in arXiv:2610.32247 (including Baidu's 575-node BOS2 cluster and 1.848 trillion records) are Baidu production metrics. They are not results of this repository, and this change does not produce them.

`Architecture.md` section 12 still says several of these subsystems are "Implemented & Tested". Where that matrix and this page disagree, follow this page and the code. Locale guides under `docs/{fr,de,ja,zh}` were not rewritten here.

Root `Architecture.md` was left unchanged on purpose. Consolidating it with the locale guides is a separate documentation workstream.

## What this increment does

1. **Heartbeats are durable and are sent.** `StoreHeartbeat` for a registered node fsyncs `CELL_CONTROL_PLANE` (CRC32C frame, version 1). Each sender file has an `observation_generation`. A receiver with no row accepts that generation's current sequence, even when it is above 64, so a leader change after many local publications is not a permanent rejection. The same generation rejects a forward jump above 64, a lower sequence, and a different body at the same sequence. A different generation, minted when the sender file is created again, is a new baseline at whatever sequence the sender now has. Every state-changing remote accept, including that replacement, waits 1 second. An identical replay does not write. This process does not keep a set of retired generations: after the rate window a delayed older generation replaces the row again. That is still not a scheduling input. The shared bearer can overwrite the observation and cannot make a later honest generation unreadable. These records are not copied into routes and do not schedule placement. An unregistered node is rejected. The shared bearer is still the only credential; a bound node instance must be repeated, and an address-only registration does not gain a second check. A failed fsync before rename leaves the previous record. If rename succeeds and the directory sync fails, memory keeps the new file so a later write cannot put an older sequence back. The sender does not roll back a sequence after a failed send.
2. **The process sends its own heartbeat.** The 250 ms maintenance loop persists a local heartbeat about every 3 seconds (`HEARTBEAT_INTERVAL_MS`). Disk free space is unknown on that record; a zero is not stored as a measurement. When this process is not the Raft leader, it sends that same sequence to the leader's control-plane address and retries the same sequence once. The sender does not attach shard descriptors. A lost response does not roll the local sequence back. The next interval uses a new sequence.
3. **Jumbo placement is a record plus a narrow Raft action.** `JumboIsolated` remains the detection flag from `JumboIsolationPlanner`. `reconcile_jumbo_placement` writes a placement record from live Raft voters:
   - `Blocked` when the dedicated node is not already a voter, the shard has no leader, leadership is elsewhere (automatic transfer is off), or a voter change was refused. No voter is added for a node that has only a registered address.
   - `LeaderOnDedicated` when that node is already the current leader and peer shards still list it, or when this process hosts no other shard. That observation is not a `LeadershipTransferred` move.
   - `DedicatedNodeNotShared` only when at least one other local shard exists and none of those shards currently has the dedicated node as a voter.
   - `committed_moves` gains `VoterReplaced` only after this process observes the planned uniform voter set. A route-file persist failure does not drop that entry or label the set as uncommitted. A restart does not invent a move.
4. **Membership changes from this path keep a majority.** `jumbo_membership_change_allowed` refuses an empty set, a drop in fault tolerance (`(n-1)/2`), and a new set whose overlap is not a majority of the old set. The peer replacement keeps the same voter count and uses a node that is already a voter of some local shard. Another jumbo shard's dedicated node is not used as that spare. Operator `scale_out_replicas` / `remove_voter` is unchanged and can still shrink on purpose.
5. **Automatic leadership transfer is disabled.** OpenRaft 0.10 `trigger().transfer_leader` marks the transfer and disables the leader lease before any peer answers. Older processes do not implement `RaftTransportService.TransferLeader`. This process cannot prove that capability without that lease-affecting call, and node registration is not proof. `reconcile_jumbo_placement` therefore does not call `trigger().transfer_leader`. The RPC handler remains so a peer that does send one is answered on the same bearer interceptor as `Vote`. A 3 second poll is not used. Pull request #38 (`f55cc6cf`) can set `election_timeout_max_ms` above 3 seconds; a poll that short would return while OpenRaft could still be rejecting writes. `transfer_poll_covers_election` records that comparison and does not enable the call. `committed_moves` does not gain `LeadershipTransferred` when the leader is already the dedicated node.

`open_and_serve` still rejects every role except `all-in-one`. A second cell is a separate all-in-one process. `RegisterCell` fsyncs it into `GLOBAL_CELL_DIRECTORY` only after `GetRoutes` shows exact coverage. That file is not replicated, keys are not forwarded, and heartbeats do not schedule the cell. Automatic `transfer_leader` stays off. Linux is the supported platform.

## Implemented versus missing

| Area | Evidence in this tree | Status |
| :--- | :--- | :--- |
| Multi-cell routing | `GLOBAL_CELL_DIRECTORY` records the local cell and a remote cell after a live `GetRoutes` covers the requested table range exactly. A partial cover, an overlap, or an unreachable first probe writes nothing. `GetRoutes` for an unknown non-zero `cell_id` fails closed. A key owned by a remote cell returns an error on this process. | **Partial.** One process serves one cell. The directory is not a Raft group and is not copied by backup. There is no forwarding and no shared inode allocator across cells. |
| Role separation | `ServerRole` parses `be`, `proxy`, `cell-manager`, and `global-manager`. `open_and_serve` returns before listen for every role except `all-in-one` (`lib.rs`). | **Missing.** One process still hosts the global manager, the cell manager, the proxy, and the Raft groups. |
| Jumbo detection flag | `JumboIsolationPlanner` and `maybe_split_or_isolate_shard` persist `ShardHealthState::JumboIsolated` in `CELL_MANAGER_STATE`. | **Present, and it is only a flag.** It does not move files. |
| Jumbo leadership placement | `automatic_leadership_transfer_allowed` is false. The handler exists; maintenance does not call `trigger().transfer_leader`. | **Disabled.** Mixed-version capability is unknown. Registration is not a capability proof. |
| Jumbo peer drain / file migration | Same-size replacement is planned and calls the existing learner promotion or `change_membership` only when a proven spare keeps fault tolerance. | **Partial.** Uniform voter sets (the join path adds a node to every shard) have no spare, so the plan stays blocked. Shard directories are not copied to another host. |
| Heartbeat persistence | `CELL_CONTROL_PLANE`, `accept_remote_heartbeat`, `publish_local_heartbeat`. | **Present** as an unreplicated local observation. Sequence advance is at most 64. A new remote sequence inside 1 second is rejected. It is not a shard epoch and does not schedule placement. |
| Heartbeat sending | `PersistentMetaDbCluster::publish_control_heartbeat` from the maintenance loop. | **Present** as liveness to the current leader. It does not carry shard stats and it does not schedule splits. |
| Heartbeat-driven scheduling | Design text in `docs/designs/sharding_and_control_plane.md` §6.1. | **Missing.** Splits still run from the local maintenance tick. |
| Membership safety for operator scale-in | `plan_membership_voters`, `remove_voter`, joint-config handling in `uniform_voter_addrs`. | **Present, and it can shrink.** Scale-in to 2 or 1 voters is an explicit operator path. It is not a three-VM result. |
| Membership safety for jumbo moves | `jumbo_membership_change_allowed`. | **Present** for the jumbo path only. It does not change operator scale-in. |
| Raft majority on the data path | OpenRaft joint consensus via `ConfChangeOrchestrator`. | **Unchanged** by this increment. This page does not re-certify it. |
| 2PC lease / storage formats | `a48d038` ("Pin 2PC lease pre-images and reject an over-cap rename TTL") is not an ancestor of this branch. | **Not imported.** Lease and storage formats were not changed. |

## Remaining backlog and dependencies

These items block a scale gate. None of them is delivered by this change.

1. **Shared cell map.** Each process has its own `GLOBAL_CELL_DIRECTORY`. Registering on process A does not make process B list A's cell. Clients must ask the process that stored the row. There is still no replicated global manager and no cross-cell forwarding. A second cell's node id must be disjoint from nodes already in the directory. Uncovered tables are not served.
2. **Role separation.** `be`, `proxy`, `cell-manager`, and `global-manager` must each start only their own listeners and state. That work has to preserve the auth fail-closed rules owned by the auth/config workstream. This change does not trim subsystems.
3. **Jumbo file placement.** Moving a shard directory onto a dedicated disk, streaming its snapshot to a node that has not joined, and draining peer shards off that disk. The safe replacement in this tree cannot fire on the uniform voter sets `scale_out_replicas` creates, because there is no spare voter. Adding a voter that has not joined is intentionally refused here.
4. **Safe automatic leadership transfer.** Still missing. It needs a capability check that does not disable the leader lease, then a wait of at least the configured `election_timeout_max_ms` (#38, head `f55cc6cf`, not merged here). Until that exists, maintenance must keep the action disabled.
5. **Heartbeat as a scheduler.** Per-shard bytes, QPS, and disk free measured by this process, sent on a trusted channel, and used only as hints. The scheduler must keep using local Raft and LSM state for splits. Client descriptors must stay non-authoritative. A 3-second cell-wide heartbeat bus for tens of thousands of shards is not built.
6. **Global and cell managers on Raft.** `CELL_MANAGER_STATE` and `CELL_CONTROL_PLANE` are local files. They are not a replicated control-plane group. A follower's placement file is not the leader's file.
7. **2PC lease pre-images.** Commit `a48d038` is unreviewed and was not imported. Later lease work has to land on its own review. This increment does not depend on it and does not change the 3-second lease, the 60-second cap, or the intent/txn record layout.
8. **Operator status for the placement record.** There is no `metadb-ctl` field and no metrics series for `PlacementPhase`. `TriggerShardSplit` still reports `split_executed = false` for a jumbo shard even when a later leadership transfer is only recorded in `CELL_CONTROL_PLANE`. Health, metrics, and operator Raft timer configuration belong to another workstream and were not edited.
9. **Docs outside this English set.** `docs/{fr,de,ja,zh}` still describe the previous heartbeat and jumbo behavior in places. Root `Architecture.md` section 12 still marks the control plane and jumbo isolation as implemented. Those updates belong to documentation consolidation.
10. **Gates this page does not move.** No-ship stays in force. A first-ship gate is a separate bar from a scale gate (multi-cell, disaggregated roles, jumbo disk isolation, and a heartbeat bus that can carry a cell of 20k–30k shards). This increment is neither gate. CI workflows were not edited. The commands below were not run.

## Main merge

Current `main` is `f9425f39fa241f44b1938909c35bc653b36de36f`, which contains #35 (explicit auth opt-in) and #38 (readiness probes and `[raft]` options). This branch merges that commit. It does not rebase. Empty secrets still refuse to serve unless `allow_unauthenticated` is set for a literal loopback IP. `/livez`, `/readyz`, and `/healthz` keep `classify_secret`. `[raft]` timers apply at group start. `TransferLeader` stays on the same bearer interceptor as `Vote`. Automatic leadership transfer stays disabled, so a configured election timeout is not raced by a 3 second transfer poll. `transfer_poll_covers_election` remains the check a future attempt would have to pass.

## Validation not run

Do not read this file as a test report. The following were not executed while the change was written:

- `cargo test -p metadb-cluster --lib jumbo_placement`
- `cargo test -p metadb-cluster --lib control_plane_state`
- `cargo test -p metadb-cluster --test scale_control_plane_tests`
- `cargo test -p metadb-server --test control_plane_claim_tests`
- `cargo test --workspace --locked`
- Any benchmark, three-voter jumbo transfer, or multi-cell drill

`crates/metadb-cluster/tests/scale_control_plane_tests.rs` and the heartbeat cases in `crates/metadb-server/tests/control_plane_claim_tests.rs` are the regression source. They cover sequence replay, conflict, a failed fsync, reload, a corrupt `CELL_CONTROL_PLANE` file, and a one-voter placement that must not change voter sets. They do not boot three voters.
