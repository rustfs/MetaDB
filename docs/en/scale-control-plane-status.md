# Scale control plane: this increment and what is still missing

[Overview](./README.md) · [Architecture](./architecture.md) · [Operations](./operations.md)

This page is the evidence record for multi-cell routing, role separation, Jumbo shard placement, heartbeats, and membership safety. It describes the tree after the control-plane increment that added `CELL_CONTROL_PLANE`, the later operator-status increment that prints local placement rows and counts them, and the measured-heartbeat increment. A receive or a completed send/ack round trip can produce a liveness hint. A missing sample, an expired sample, or an untrusted clock refuses the hint. The hint is not applied. It is not a first-ship pass and it is not a scale gate. Figures in arXiv:2610.32247 (including Baidu's 575-node BOS2 cluster and 1.848 trillion records) are Baidu production metrics. They are not results of this repository, and this change does not produce them.

`Architecture.md` section 12 still says several of these subsystems are "Implemented & Tested". Where that matrix and this page disagree, follow this page and the code. Locale guides under `docs/{fr,de,ja,zh}` were not rewritten here.

Root `Architecture.md` section 7 and the control-plane row in section 12 now mention the shard-log snapshot. The rest of that matrix was not rewritten. Consolidating it with the locale guides is a separate documentation workstream.

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

`global-manager` loads `GLOBAL_CELL_DIRECTORY` and does not open `be/` or a cell Raft group. `cell-manager` hosts one local cell and, when `cluster.global_manager` is set, publishes that cell after listen. `be` hosts local Raft replicas and does not mint ids. `proxy` forwards metadata to one `[cluster] upstream` and does not host Raft. `RegisterCell` fsyncs a row only after `GetRoutes` shows exact coverage. On a process that bootstrapped the cell and leads its lowest local shard, that directory and a `CELL_MANAGER_STATE` snapshot are proposed into that shard's existing Raft log (`0x0F` in the shard LSM). A second voter of the same shard reads the same cells and membership after apply. A joined replica does not publish. A `global-manager` with no shard group still has only the local file. The directory file is not its own Raft group. Keys are not forwarded. A heartbeat hint does not schedule the cell and does not publish that snapshot. Automatic `transfer_leader` stays off. This is not a shared id allocator and not a production multi-cell deployment. Linux is the supported platform.

## Measured liveness hint

`HeartbeatScheduler` keeps measured samples in memory. They are not reloaded from `CELL_CONTROL_PLANE`. A durable `observed_unix_ms` value, by itself, is not a sample. Publishing this process's own row does not make this process `HintLive`.

A sample is recorded in two cases:

- The handler accepts a remote heartbeat, including an identical replay. The receive time is this process's monotonic clock at accept, not the caller's wall timestamp.
- A `StoreHeartbeat` ack returns. The sender records `recv_mono_ns - sent_mono_ns` for its own node. A gap of zero, a backwards gap, or a gap above 2 seconds is not recorded.

`decide_liveness` returns `HintLive` only when the decision-time wall clock is trusted, the sample wall clock is trusted, the monotonic age and the wall age agree within 500 ms, and that age is under 9 seconds (`HEARTBEAT_FRESHNESS_MS`, three publish intervals). Otherwise it returns `Refuse`:

- `UntrustedClock` when the wall clock is missing, is `0`, is the historical 1 ms fallback from `HybridLogicalClock::wall_clock_ms`, moves backwards past the 500 ms drift, or no longer agrees with the monotonic age. A stalled wall clock that still looks recent is this case, not a healthy node.
- `MissingHeartbeat` when no measured sample is stored.
- `ExpiredHeartbeat` when both clocks agree that the sample is at least 9 seconds old.
- `UntrustedMeasurement` when the sample has no receive time, a zero round trip, or a round trip above 2 seconds.

An untrusted wall clock also refuses `publish_local_heartbeat` and `accept_remote_heartbeat` before a new row is written. The previous file is left as it was. The 3 second publish interval is monotonic. A backwards monotonic step is due again. It is not treated as a still-fresh local tick.

Maintenance calls `observe_liveness_hints` and does not apply the result. `liveness_effects` is false for `transfer_leader`, a cross-machine Jumbo move, a membership change, and cell-directory replication on every decision, including `HintLive`. Splits still use local Raft and LSM state. Client shard descriptors stay annotations. The shared bearer is unchanged. The directory snapshot above is a separate publication by the bootstrap leader. The hint does not trigger it. This is not a bus for 20k–30k shards per cell, not a separate control-plane Raft group, and not production acceptance.

## Implemented versus missing

| Area | Evidence in this tree | Status |
| :--- | :--- | :--- |
| Multi-cell routing | `GLOBAL_CELL_DIRECTORY` records the local cell and a remote cell after a live `GetRoutes` covers the requested table range exactly. A partial cover, an overlap, or an unreachable first probe writes nothing. `GetRoutes` for an unknown non-zero `cell_id` fails closed. A key owned by a remote cell returns an error on this process. A bootstrap leader also stores that directory and a `CELL_MANAGER_STATE` snapshot in the lowest local shard's Raft log. A loaded directory file that omits a replicated cell refuses to open. A publication that drops a cell id is rejected. | **Partial.** One process serves one cell. The directory file is not its own Raft group. Voters of that shard share the snapshot; a directory-only process does not. There is no forwarding and no shared inode allocator across cells. |
| Role separation | `global-manager` serves the directory file only. `cell-manager` serves one local cell and can publish it. `be` serves local Raft replicas and does not mint ids. `proxy` forwards to one upstream and does not host Raft (`lib.rs`). | **Partial.** The cell and BE processes still colocate the replicas they serve. The directory is not a Raft group. The proxy is not a multi-cell router. |
| Jumbo detection flag | `JumboIsolationPlanner` and `maybe_split_or_isolate_shard` persist `ShardHealthState::JumboIsolated` in `CELL_MANAGER_STATE`. | **Present, and it is only a flag.** It does not move files. |
| Jumbo leadership placement | `automatic_leadership_transfer_allowed` is false. The handler exists; maintenance does not call `trigger().transfer_leader`. | **Disabled.** Mixed-version capability is unknown. Registration is not a capability proof. |
| Jumbo peer drain / file migration | Same-size replacement is planned and calls the existing learner promotion or `change_membership` only when a proven spare keeps fault tolerance. A configured `jumbo_dedicated_root` CRC-checks this process's own jumbo shard directory, renames `MOVE_LIVE` into place, then opens Raft there. A directory fsync failure after that rename keeps the dedicated copy and does not reopen the old directory. Restart after that record does not open the old directory and does not delete the dedicated copy. | **Partial.** The copy does not leave this machine. Peer replicas stay. Automatic transfer stays off. Uniform voter sets still have no spare, so membership drain stays blocked. |
| Heartbeat persistence | `CELL_CONTROL_PLANE`, `accept_remote_heartbeat`, `publish_local_heartbeat`. | **Present** as an unreplicated local observation. Sequence advance is at most 64. A new remote sequence inside 1 second is rejected. An untrusted wall clock does not write a new row. The row is not a shard epoch and does not schedule placement. |
| Heartbeat sending | `PersistentMetaDbCluster::publish_control_heartbeat` from the maintenance loop. | **Present.** The due check is monotonic. A successful ack records a round trip on the sender. The RPC still carries no shard stats. |
| Heartbeat-driven scheduling | `HeartbeatScheduler::decide_liveness`. | **Partial hint.** A measured receive or round trip can return `HintLive`. Missing, expired, and untrusted inputs return `Refuse`. Maintenance does not apply the hint. Splits still run from local Raft and LSM state. |
| Membership safety for operator scale-in | `plan_membership_voters`, `remove_voter`, joint-config handling in `uniform_voter_addrs`. | **Present, and it can shrink.** Scale-in to 2 or 1 voters is an explicit operator path. It is not a three-VM result. |
| Membership safety for jumbo moves | `jumbo_membership_change_allowed`. | **Present** for the jumbo path only. It does not change operator scale-in. |
| Operator placement status | `GetRoutes.placements`, `TriggerShardSplitResponse.placement`, `metadb-ctl` placement lines, unlabeled `metadb_placement_*` gauges. | **Partial.** Local rows only. `split_executed` stays false when the worker did not create a child. File-move phase is not on this surface. |
| Raft majority on the data path | OpenRaft joint consensus via `ConfChangeOrchestrator`. | **Unchanged** by this increment. This page does not re-certify it. |
| 2PC lease / storage formats | Current `main` includes `da547f4`, which reclaims orphan 2PC pre-images. | **Not changed here.** The placement-status commit does not edit lease recovery. Commit `a48d038` was not imported as its own commit. |

## Remaining backlog and dependencies

These items block a scale gate. Item 8 is a partial operator view of a local placement row. The others are not delivered by that view.

1. **Shared cell map.** A bootstrap leader proposes `GLOBAL_CELL_DIRECTORY` and a `CELL_MANAGER_STATE` snapshot into the lowest local shard's existing Raft log. A second voter of that shard sees the same cells and membership after apply. A restart reads that snapshot; a corrupt directory or manager file refuses to open, and a directory file that drops a replicated cell refuses to open. A joined replica does not publish. A `global-manager` with no shard group still has only its local file, so registering there does not by itself update another process. There is no separate global-manager Raft group and no cross-cell forwarding. A second cell's node id must be disjoint from nodes already in the directory. Uncovered tables are not served.
2. **Remote placement and multi-cell proxy routing.** `be` can host and join local replicas. `proxy` forwards to one configured shard host. A cell manager still cannot place a shard on a BE it does not already run, and the proxy does not read the cell directory to choose a target. Auth fail-closed rules are unchanged.
3. **Jumbo file placement onto another machine.** This process can move its own jumbo directory onto `jumbo_dedicated_root`. Streaming a snapshot to a node that has not joined, and draining peer shards off a disk, are still missing. The safe replacement in this tree cannot fire on the uniform voter sets `scale_out_replicas` creates, because there is no spare voter. Adding a voter that has not joined is intentionally refused.
4. **Safe automatic leadership transfer.** Still missing. It needs a capability check that does not disable the leader lease, then a wait of at least the configured `election_timeout_max_ms` (#38, head `f55cc6cf`, not merged here). Until that exists, maintenance must keep the action disabled.
5. **Heartbeat as a scheduler.** A measured receive or round trip can be a liveness hint, and an untrusted clock refuses that hint. Still missing: per-shard bytes, QPS, and disk free measured by this process; a channel stronger than the shared bearer; using the hint as an input to splits; and a 3-second cell-wide heartbeat bus for tens of thousands of shards. The hint is not applied. It does not call `transfer_leader`, move a Jumbo shard, change membership, or publish the cell directory. Client descriptors stay non-authoritative. Splits still use local Raft and LSM state.
6. **Global and cell managers on Raft.** The directory and a `CELL_MANAGER_STATE` snapshot now ride the existing shard Raft log. There is still no separate control-plane Raft group. `CELL_CONTROL_PLANE` remains a local file. A follower's placement file is not the leader's file. A scale-in whose caller leaves the voter set can leave the snapshot behind the applied membership until a later publication from the leader. The heartbeat hint does not publish that snapshot.
7. **2PC lease pre-images.** Commit `a48d038` was not imported as its own commit. Current `main` includes `da547f4` (reclaim orphan pre-images). The placement-status change does not edit that recovery, the 3-second lease, the 60-second cap, or the intent/txn record layout.
8. **Operator status for the placement record.** `metadb-ctl cluster status` and `shard list` print each local placement row: phase name and code (`recorded` 1, `leader_on_dedicated` 2, `dedicated_not_shared` 3, `blocked` 4), dedicated node, dominant pid, committed moves, both attempt timestamps, update time, and blocked reason. `placement_automatic_transfer_enabled` is this process and is false. `shard split` prints the phase fields on success. When no placement row exists, including an ordinary split, the line is `placement_phase=` and `placement_phase_code=0`. Code 0 is that no-record case. Stored rows use 1–4. `split_executed` stays false when no child was created. The reconciler does not stamp `transfer_attempt_unix_ms` and does not call `transfer_leader`. It keeps a non-zero transfer timestamp already in the file. A new row stores 0. Rows are insert-only; this process does not delete one when the shard is no longer jumbo. Unlabeled gauges count local rows by phase, stored moves, and rows with a non-zero attempt timestamp. They are not readiness inputs. Still missing from this surface: `MOVE_STAGED` / `MOVE_LIVE`, placement rows for a remote cell, publication of `PlacementPhase::Recorded`, per-shard labels, and any series whose value is a node id, a path, or bytes copied onto another machine. Health probes and `[raft]` loading belong to the operability workstream and were not redefined here.
9. **Docs outside this English set.** Locale operations pages name the placement fields and gauges. Other sections of `docs/{fr,de,ja,zh}` still describe the previous heartbeat and jumbo behavior. Root `Architecture.md` section 12 still marks the control plane and jumbo isolation as implemented. Those updates belong to documentation consolidation.
10. **Gates this page does not move.** No-ship stays in force. A first-ship gate is a separate bar from a scale gate (multi-cell, disaggregated roles, jumbo disk isolation, and a heartbeat bus that can carry a cell of 20k–30k shards). This increment is neither gate. CI workflows were not edited. The commands below were not run.

## Base

This increment starts from `main` `1d77bb06069bdaf97aa924581373dbf422484349` (the local jumbo directory move). It does not rebase. Empty secrets still refuse to serve unless `allow_unauthenticated` is set for a literal loopback IP. Automatic leadership transfer stays disabled.

## Validation for the role split

These commands were executed on this increment. They are not a workspace run and they are not a CI result:

- `cargo check -p metadb-server --tests --locked`
- `cargo clippy -p metadb-server --all-targets --locked --no-deps -- -D warnings -A clippy::assertions_on_constants` (the landed `assert!(!metadb_transport::RDMA_BACKEND_IMPLEMENTED)` still fails that lint without the allow)
- `cargo test -p metadb-server --test role_separation_tests --locked`
- `cargo test -p metadb-server --test control_plane_claim_tests --locked -- disaggregated_roles_are_rejected_before_listen`
- `cargo test -p metadb-server --test second_cell_tests --locked`

`cargo test --workspace --locked` was not run. GitHub Actions was not enabled and was not waited on.

## Validation not run

Do not read the list below as a test report for this increment. The following were not executed:

- `cargo test -p metadb-cluster --lib jumbo_placement`
- `cargo test -p metadb-cluster --lib control_plane_state`
- `cargo test -p metadb-cluster --test scale_control_plane_tests`
- `cargo test -p metadb-server --test control_plane_claim_tests`
- `cargo test --workspace --locked`
- Any benchmark, three-voter jumbo transfer, or multi-cell drill

`crates/metadb-cluster/tests/scale_control_plane_tests.rs` and the heartbeat cases in `crates/metadb-server/tests/control_plane_claim_tests.rs` are the regression source. They cover sequence replay, conflict, a failed fsync, reload, a corrupt `CELL_CONTROL_PLANE` file, and a one-voter placement that must not change voter sets. They do not boot three voters.

## Placement status check

These commands were executed again after merging `main` `3e7ac6ddfd653c105950e200fc7d052547698be8`. They are not a workspace run and they are not a CI result:

- `cargo test -p metadb-cluster --lib jumbo_placement::tests::operator_phase_codes_do_not_use_zero --locked`
- `cargo test -p metadb-server --lib metrics::tests --locked`
- `cargo test -p metadb-server --test placement_status_tests --locked`
- `cargo clippy -p metadb-proto -p metadb-cluster -p metadb-server --all-targets --locked --no-deps -- -D warnings -A clippy::assertions_on_constants`
- `cargo fmt --all -- --check`

They do not boot three voters, do not copy a jumbo directory, and do not enable automatic leadership transfer. `cargo test --workspace --locked` was not run. GitHub Actions was not enabled.

## Measured heartbeat check

These commands were executed for the measured-heartbeat increment. They are not a workspace run and they are not a CI result:

- `cargo fmt --all -- --check`
- `cargo test -p metadb-cluster --lib heartbeat_scheduler --locked`
- `cargo test -p metadb-cluster --lib control_plane_state --locked`
- `cargo test -p metadb-cluster --test scale_control_plane_tests --locked`
- `cargo test -p metadb-server --test control_plane_claim_tests --locked -- control_plane_does_not_pretend_missing_topology_ops_succeeded`
- `cargo clippy -p metadb-cluster --all-targets --locked --no-deps -- -D warnings -A clippy::assertions_on_constants`

`cargo clippy -p metadb-server --all-targets --locked --no-deps -- -D warnings -A clippy::assertions_on_constants` was not clean. It stops on `clippy::derivable_impls` for `RbacSection` in `crates/metadb-server/src/config.rs` and, with that lint allowed, on `clippy::needless_borrow` in `crates/metadb-server/src/bin/metadb-ctl.rs`. This increment does not edit those files.

They do not boot three voters, do not call `transfer_leader`, do not copy a Jumbo directory onto another machine, and do not replicate `GLOBAL_CELL_DIRECTORY`. `cargo test --workspace --locked` was not run. GitHub Actions was not enabled.
