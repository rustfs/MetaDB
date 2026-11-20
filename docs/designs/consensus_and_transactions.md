# EGM Subsystem Specification 2: OpenRaft Multi-Raft Consensus, Off-Critical-Path Queues & Crash-Recoverable 2PC (`metadb-raft` & `metadb-tx`)

* **Status**: Phase 0 design note. Not a first-launch acceptance of the running tree.
* **Target Crates**: `crates/metadb-raft` and `crates/metadb-tx`
* **Primary Reference**: Zebin Ren ([@ZebinRen](https://github.com/ZebinRen)), Biao Cao, Kang Chen ([@kangchenPKU](https://github.com/kangchenPKU)), Zhihui Yin, Qiushi Chen, Jielong Jian, Jiwei Xiong, Xinxing Wang, Yan Xing, Liguo Duan, Ran Zheng, Xianjun Meng, and Zhenyu Hou, *"MetaDB: Putting File-System Structure Back into Distributed Databases"*, [arXiv:2610.32247v1](https://arxiv.org/html/2610.32247v1) (Vrije Universiteit Amsterdam, Baidu & Peking University, 2026), §3.2, §4.2, §5.1, §5.2
* **Semantic Boundaries**:
  * `FeatureRange`: Sections 1 – 4
  * `DetailedRange` (Zero-Decision Implementation Contract): Sections 5 – 10

---

## 1. Problem Statement (`FeatureRange`)

General-purpose distributed databases penalize file-system metadata workloads through three consensus and transaction overheads:
1. **Double Write-Ahead Logging**: Writing every mutation first to a Raft consensus log and again to a local LSM/RocksDB WAL doubles sequential NVMe I/O bandwidth.
2. **Hotspot Parent Directory Serialization & In-Band Auxiliary 2PC**: Locking `(PID, "_attr_")` synchronously on every child creation collapses concurrent single-directory throughput by `10x–36x`, while updating `QuotaTable` (`UID`) and `SecondaryIndexTable` (`FID -> (PID, name)`) inside the primary transaction forces multi-shard 2PC on `100%` of file creates/deletes.
3. **State-Machine Divergence Across Deployment Modes**: Systems that bypass Raft in single-node mode cannot seamlessly scale out online to `3-Voter` HA clusters without data migration or divergent crash-recovery paths.

`metadb-raft` and `metadb-tx` solve these challenges by pairing **WAL-less Multi-Raft (`openraft`)**—used identically in `1-Voter` single-node mode and `3-Voter` distributed mode—with **1-RTT Single-Shard Pushdown**, **Deterministic Off-Critical-Path Attribute/Auxiliary Queues**, and **Crash-Recoverable Primary-Anchored 2PC**.

---

## 2. Goals & Non-Goals (`FeatureRange`)

### Goals
* **G1 (Unified `1-Voter` to `3-Voter` Multi-Raft)**: Host arbitrary shard Raft groups per BE node via `openraft` over segmented CRC32C disk logs and multiplexed `tonic` gRPC channels, supporting zero-downtime `1-Voter -> Learner -> 3-Voter` promotion.
* **G2 (3-Watermark Truncation & Snapshot Completeness)**: Enforce `safe_purge_index = min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)` and embed all folded `_attr_` and un-acked `AuxOutbox` items (`[0x07]`) inside Raft SSTable snapshots.
* **G3 (Linearizable `ReadIndex` + Race-Free `AsyncAttrAggregator`)**: Fence linearizable reads via batched `ReadIndex`, reconcile in-memory `AttrDelta` entries (`raft_index > persisted.applied_index && hlc <= read_hlc`), and persist folded `_attr_` records deterministically via the Coordinated Flush Barrier.
* **G4 (Exact-Once FIFO `AuxOutbox` with Local Spill)**: Guarantee exact-once asynchronous propagation to `QuotaTable` and `SecondaryIndexTable` across retries, splits, and leader failovers, spilling overdue tasks to `[0x07] SpilledAuxOutbox` so Raft logs never grow unbounded.
* **G5 (Crash-Recoverable Primary-Anchored 2PC)**: Provide atomic, deadlock-free cross-shard `Rename` (exclusive and POSIX overwrite), cross-shard `Mkdir`, and cross-shard `Rmdir` with cycle prevention and automatic orphan transaction recovery (`TxnRecoveryWorker`).

### Non-Goals
* **NG1**: Multi-statement interactive client-held SQL transactions (`BEGIN ... COMMIT` spanning arbitrary client think time). All transactions execute server-side within `<= 3,000ms` TTL (`< 5,000ms` MVCC window).

---

## 3. High-Level Consensus & Transaction Flow (`FeatureRange`)

```mermaid
sequenceDiagram
    participant C as Proxy / SDK Caller
    participant L as Shard Leader (OpenRaft)
    participant F as Shard Followers (0 in Stage 1; 2 in Stage 3)
    participant SM as Deterministic RaftStateMachine
    participant Aux as Target Quota/Index Shard

    C->>L: PushdownOp (expected_epoch, op, caller_hlc)
    L->>L: Pre-check ShardEpoch & assign commit_hlc = clock.update_checked(caller_hlc)
    par Persist & Replicate Raft Log Entry
        L->>L: Append to CRC32C .raftlog + fdatasync
        L->>F: AppendEntries RPC (tonic gRPC)
    end
    F-->>L: Quorum Ack (immediate in 1-Voter mode)
    L->>SM: apply(raft_index, commit_hlc, PushdownOp)
    Note over SM: 1. Re-verify ShardEpoch == expected_epoch<br/>2. Check/validate no conflicting [0x05] WriteIntent<br/>3. Mutate MemMvccStore (child entry)<br/>4. Append deterministic AttrDelta (raft_index, commit_hlc)<br/>5. Enqueue AuxOutbox task (origin_shard_id, raft_index)
    SM-->>L: ApplyResponse
    L-->>C: 1-RTT Success (MetaEntry / InodeAttr)
    L--)Aux: Async FIFO AuxBatch RPC (exact-once idempotency)
    Aux-->>L: Ack(max_origin_raft_index) -> RaftCmd::AckAuxWatermark
```

---

## 4. Alternatives Considered & Risk Matrix (`FeatureRange`)

* **Alternatives Rejected**:
  * *Two-Phase Coordinator Log on Stateless Proxies*: Rejected because crashed stateless proxies would strand participant locks indefinitely. Storing `TxnRecord` (`[0x06]`) inside the Primary Shard's Raft group makes transaction recovery self-contained and highly available.
  * *Wall-Clock Timers for Replica-Local `_attr_` Flushes*: Rejected because independent wall clocks across Raft replicas would stamp divergent `modify_time` values in SSTables. Deriving `_attr_.modify_time` strictly from `max(delta.hlc)` inside the Coordinated Flush Barrier keeps every replica's state machine deterministic.
* **Key Risks & Mitigations**:
  * *Concurrent Cross-Directory `rmdir` vs. `create_file`*: Mitigated by 2PC `Rmdir` placing `WriteIntent::DeletingDir` on `(target_dir_id, "_attr_")` during Prepare after scanning `[target_dir_id, target_dir_id+1)`, causing concurrent child creates on `Shard(target_dir_id)` to abort cleanly.
  * *Concurrent Directory `rename(/a, /b/a)` vs. `rename(/b, /a/b)` Cycle Race*: Mitigated by storing `parent_pid` on every `(dir_id, "_attr_")` record and acquiring a lease-bounded `DirRenameGuard` on `CellLocalManager`/`GlobalManager` exclusively when moving **directories** across distinct parents (`<0.01%` of ops), walking ancestors up to `ROOT_INODE_ID = 1` before committing.

---

## 5. OpenRaft Log Storage, Watermarks & Snapshots (`DetailedRange`)

### 5.1 Segmented Raft Log Wire Format (`crates/metadb-raft/src/log_store.rs`)
Per-shard Raft directory: `<data_dir>/shards/<shard_id:020>/raft/`:
* `HARD_STATE`: `[term: u64 LE][has_vote: u8][voted_for_node: u64 LE][committed_vote: u8][has_committed: u8][committed_term: u64 LE][committed_leader_node: u64 LE][committed_index: u64 LE][purged_index: u64 LE][purged_term: u64 LE][crc32c: u32 LE]`, updated via `HARD_STATE.tmp` -> `fdatasync` -> atomic `rename`.
  * **Durable `last_committed` Contract**: Implements `RaftLogStorage::save_committed` and `read_committed` durably in `HARD_STATE` so crash recovery replays strictly committed entries `(flushed_raft_index ..= last_committed.index]` and never applies uncommitted tail entries to the WAL-less LSM engine.
* `segment_<first_index:020>.raftlog` (default target `64 MiB` per segment):
  * `8-byte Segment Header`: `magic = 0x4D524146544C4F31` (`"MRAFTLO1"`).
  * Sequence of variable-length frames:
    `[frame_len: u32 LE][frame_crc32c: u32 LE][log_index: u64 LE][term: u64 LE][entry_kind: u8 (0=Blank, 1=Normal, 2=Membership)][payload_bytes]`.
  * **Strict `(hlc, raft_index)` Monotonicity**: On proposal, the leader stamps `entry_hlc = clock.now_strictly_after(last_proposed_hlc)`; inside `RaftStateMachine::apply`, every normal entry verifies `(entry_hlc, log_index) > (last_applied_hlc, last_applied_index)`.
  * **Startup Recovery**: Scan segment files in ascending `first_index`. Verify `frame_crc32c` on every frame; if the final segment ends with a partial/torn frame (power loss prior to `fdatasync` completion), truncate the segment file to the last valid frame offset.
  * **Uncommitted Suffix Truncation (`truncate(log_id)`)**: Truncates uncommitted tail entries `>= log_id.index` when OpenRaft resolves log conflicts after leader election (asserting `log_id.index > last_committed_index`).

### 5.2 Three-Watermark Safe Prefix Purge & WAL-less Crash Recovery (`purge(log_id)`)
OpenRaft invokes `purge(upto_log_id)` according to snapshot/compaction policy, but `SegmentedRaftLogStore` clamps physical segment deletion strictly to three subsystem watermarks:
$$\text{safe\_purge\_index} = \min\big(\text{upto\_log\_id.index},\ \text{state\_flushed\_index},\ \text{aux\_outbox\_durable\_index},\ \text{last\_snapshot\_index}\big)$$
where:
* `state_flushed_index` (unified `MemMvccStore` + `AsyncAttrAggregator` watermark): advances inside `PreFlushHook::on_flush_durable(M)` strictly after the L0 SSTable (containing both collapsed `MemMvccStore` keys and folded `(PID, "_attr_")` updates up to `M`) and `MANIFEST` (`DurableShardCheckpoint`) are `fdatasync`'d.
* `aux_outbox_durable_index = max(contiguous_aux_acked_index, spilled_aux_outbox_flushed_index)`: advances **strictly along contiguous `origin_raft_index` coverage frontiers**—every `(origin_raft_index, task_ordinal)` with `origin_raft_index <= aux_outbox_durable_index` is guaranteed to be either durably ACKed via `RaftCmd::AckAuxTasks` OR durably written to `[0x07] SpilledAuxOutbox`.
* `last_snapshot_index`: OpenRaft's installed/created snapshot index floor.
* **Exact WAL-less Startup Recovery Protocol**:
  1. Recover `LsmStorageBackend` from `MANIFEST` -> yields `DurableShardCheckpoint` with `F = flushed_raft_log_id.index` (`0` on an empty shard) and `contiguous_aux_acked_index`.
  2. Rehydrate spilled outbox state by scanning `[0x07][origin_raft_index: 8B BE][task_ordinal: u16 BE]` keys in `LsmStorageBackend` with `origin_raft_index > contiguous_aux_acked_index`; initialize `AsyncAttrAggregator::pending_by_pid` empty (because all deltas `<= F` are already folded inside `_attr_` at `MANIFEST` checkpoint `F`).
  3. **`RaftStateMachine::applied_state()` reports `flushed_raft_log_id` (`F`) and `last_membership` to OpenRaft on startup.**
  4. OpenRaft deterministically replays committed `.raftlog` entries strictly in `(F ..= last_committed.index]` through `apply()`, reconstructing `MemMvccStore`, `AsyncAttrAggregator` (`index > F`), and un-spilled `AuxOutbox` tasks (`index > F`) with zero lost or double-counted updates.

### 5.3 Deterministic Snapshot Creation, Atomic `install_snapshot` & Learner Promotion
1. **Snapshot Build (`build_snapshot`)**:
   * Trigger a `MemMvccStore` seal + Coordinated Flush Barrier (spilling un-acked `AuxOutbox` tasks to `[0x07]`) and **await completion of the L0 SSTable + `MANIFEST` `fdatasync`**.
   * Hard-link `.sst` files + `MANIFEST` of the pinned `Version` into `snapshots/snap_<S>.tmp/` where **`S = checkpoint_version.flushed_raft_index` strictly**.
   * Write `SNAPSHOT_META` (`last_log_id = S`, `DurableShardCheckpoint`, per-file sizes & CRC32C), `fdatasync(SNAPSHOT_META)`, atomically `rename("snap_<S>.tmp", "snap_<S>")`, `fsync(snapshots_dir)`, and only then advance `last_snapshot_index = S`.
2. **Atomic Snapshot Installation (`install_snapshot` on Learner/Follower)**:
   * Ignore idempotently if `snapshot_meta.last_log_id.index <= current_applied_index`.
   * Stream files into `shards/<shard_id>/lsm_installing_<S>/`, verify every file's size and CRC32C against `SNAPSHOT_META`, `fdatasync` all files, and `fsync(lsm_installing_<S>)`.
   * Acquire exclusive shard state-machine lock (marking any active follower `SnapshotGuard`s expired): atomically swap directories (`rename("lsm", "lsm_old_<S>")` -> `rename("lsm_installing_<S>", "lsm")` -> `fsync(shard_dir)`), reopen `LsmStorageBackend` at `S`, clear `MemMvccStore` and `AsyncAttrAggregator`, rehydrate `AuxOutbox` from `[0x07]` (`index > snapshot.contiguous_aux_acked_index`), update `ShardDescriptor`, persist `HARD_STATE.committed = snapshot_meta.last_log_id`, and remove `lsm_old_<S>`.
3. **Online `ConfChangeOrchestrator` (`1-Voter -> 3-Voter`)**:
   * Step 1: `raft.add_learner(node_id, node_addr, blocking = false)`. OpenRaft streams `InstallSnapshot` followed by tail `AppendEntries` strictly for `index > S`.
   * Step 2: Poll `raft.metrics()` until `replication[node_id].matched >= last_log_index - learner_catchup_threshold` (default `64` entries).
   * Step 3: `raft.change_membership(BTreeSet::from([n1, n2, n3]), retain = false)`. OpenRaft executes two-step joint consensus $(C_{\text{old}} \cup C_{\text{new}} \rightarrow C_{\text{new}})$ with zero write pause.

### 5.4 Linearizable `ReadIndex` Batching & Post-Wait Epoch Revalidation (`ReadIndexBatcher`)
* Every linearizable read pre-checks strict `expected_epoch == local_epoch` (`conf_ver` AND `version`), joins `ReadIndexBatcher` (`200µs` micro-batch `raft.ensure_linearizable()`), and awaits `applied_index >= read_index`.
* **Post-Wait Epoch Revalidation & Atomic `ReadCut` Acquisition**: Once `applied_index >= read_index`, under the state-machine read-cut lock:
  1. Re-verify `current_state_machine_epoch == expected_epoch` (returning `EpochMismatch` if any `ExecuteSplit` or `ConfChange` applied in `(pre_check_index ..= read_index]`).
  2. Acquire `SnapshotGuard` via `acquire_linearizable_snapshot(read_index)` pinning `ReadCut { max_raft_index: current_applied_index, max_hlc: current_applied_hlc }` (or `acquire_fixed_snapshot(barrier_hlc)` after advancing `clock.update_checked(barrier_hlc)` + `ReadIndex` for multi-shard `DualModeFineGrained` `dirstat`).
* In `1-Voter` mode, `ensure_linearizable()` completes synchronously with `0` network hops.

---

## 6. Operation Routing & Execution Matrix (`DetailedRange`)

| Operation | Single-Shard `1-RTT` Condition (`PushdownExecutor`) | Cross-Shard `2PC` Condition (`TwoPhaseCommitCoordinator`) |
| :--- | :--- | :--- |
| `CreateFile(pid, name, ...)` | **Always (`100%`)** — routed to `Shard(MetaKey::child(pid, name))`. Checks `(pid, "_attr_")` (or `[0x04] DirShardDelta` in `DualModeFineGrained`) has no `DeletingDir` intent. | Never required. |
| `Unlink(pid, name)` | **Always (`100%`)** — routed to `Shard(MetaKey::child(pid, name))`. | Never required. |
| `SetAttr(pid, name, patch)` | **File (`>99.9%`)**: Always `1-RTT` on `Shard(pid, name)`.<br>**Directory**: `1-RTT` when `dir_id` is `DirectoryColocated` AND `Shard(pid, name) == Shard(dir_id, "_attr_")` (atomically updates both dentry and authoritative `(dir_id, "_attr_")`). | **Directory** when `Shard(pid, name) != Shard(dir_id, "_attr_")` OR `dir_id` is `DualModeFineGrained` (2PC updates `(pid, name)`, authoritative `(dir_id, "_attr_")`, and all `[0x04][dir_id]` sub-shards). |
| `Lookup(pid, name)` | **File**: `ReadIndex` + point read on `Shard(pid, name)`.<br>**Directory**: returns dentry `(inode_id, Directory)` for path traversal; full `stat` delegates to `DirStat(dir_id)` where `(dir_id, "_attr_")` is authoritative. | Never required. |
| `DirStat(pid)` | `DirectoryColocated` (`>99.99%`): `ReadIndex` + `reconcile_attr` on authoritative `(pid, "_attr_")`. | `DualModeFineGrained`: parallel `ReadIndex` + shared `barrier_hlc` scatter-gather across sub-shards intersecting `[pid, pid+1)`. |
| `ListDir(pid, after, limit)` | Single shard if `[ (pid, after), (pid+1, "") )` fits in one shard or `limit` satisfied locally (keyset continuation). | Sequential cursor continuation across adjacent `DualModeFineGrained` sub-shards if needed. |
| `Mkdir(pid, name, new_dir_id)` | When `Shard(MetaKey::child(pid, name)) == Shard(MetaKey::dir_attr(new_dir_id))` (single atomic Raft entry). | When `Shard(pid, name) != Shard(new_dir_id, "_attr_")`. |
| `Rmdir(pid, name)` | When `target_dir_id` is `DirectoryColocated` AND `Shard(pid, name) == Shard(target_dir_id, "_attr_")`. | When `Shard(pid, name) != Shard(target_dir_id, "_attr_")` OR `target_dir_id` is `DualModeFineGrained`. |
| `Rename(src_pid, src, dst_pid, dst, overwrite)` | Same-shard when `Shard(src_pid, src) == Shard(dst_pid, dst)` (and for directories, `dir_id` is `DirectoryColocated` on that same shard). | Whenever `src`, `dst`, or moved directory `(dir_id, "_attr_")` / `[0x04][dir_id]` span `>= 2` shards. |

---

## 7. `AsyncAttrAggregator` & Exact-Once `AuxOutbox` Contracts (`DetailedRange`)

### 7.1 `InodeAttr` Binary Codec (`88` Bytes Deterministic Big-Endian + CRC32C)
To support directory-rename cycle detection and deterministic Raft reconciliation, `InodeAttr` encodes into `88` fixed bytes:
* `[0]`: `codec_version: u8 (= 1)`
* `[1]`: `entry_type: u8` (`1 = File`, `2 = Directory`, `3 = DirectoryAttr`)
* `[2..10]`: `inode_id: u64 BE`
* `[10..18]`: `parent_pid: u64 BE` (`1` for root `/`)
* `[18..26]`: `uid: u64 BE`
* `[26..30]`: `mode: u32 BE`
* `[30..34]`: `link_count: u32 BE`
* `[34..42]`: `num_children: i64 BE`
* `[42..50]`: `size_bytes: i64 BE`
* `[50..58]`: `create_time: u64 BE` (`HlcTimestamp`)
* `[58..66]`: `modify_time: u64 BE` (`HlcTimestamp`)
* `[66]`: `has_fid: u8` (`0` or `1`)
* `[67..75]`: `fid: u64 BE`
* `[75..83]`: `applied_raft_index: u64 BE`
* `[83]`: `reserved_flags: u8` (`0`)
* `[84..88]`: `crc32c: u32 BE` (computed over bytes `[0..84]`)

### 7.2 `AsyncAttrAggregator` State Machine
* **On `RaftStateMachine::apply(raft_index, hlc, op)`**:
  Push `AttrDeltaItem { pid, delta_children, delta_bytes, hlc, raft_index }` to `pending_by_pid[pid]`.
* **On `dirstat(pid, &snapshot_guard)`**:
  Read `(pid, "_attr_")` (or `[0x04][pid]` on non-anchor `DualModeFineGrained` sub-shards) via `snapshot_guard`. Fold every `item` in `pending_by_pid[pid]` satisfying:
  `item.raft_index > persisted.applied_raft_index && item.hlc <= snapshot_guard.read_hlc()`.
* **On `PreFlushHook::on_seal_memtable(M, max_hlc, view_at_m)`**:
  `view_at_m` reads across `SealingMemTable (<= M) -> queued ImmMemTables (newest->oldest, <= M) -> VersionSet` (never `VersionSet` alone!). For each `pid` having deltas with `raft_index <= M`, read baseline `_attr_` (or `[0x04][pid]`) from `view_at_m`, fold strictly deltas in `(baseline.applied_raft_index, M]`, set `attr.applied_raft_index = max_folded_index`, and emit `(attr_key, attr.modify_time, MvccValue::Value(attr.encode()))` into the sealing `ImmMemTable`. (If `pid` was deleted by `rmdir` at `<= M`, skip emitting `_attr_` and mark deltas `<= M` obsolete).
* **On `PreFlushHook::on_flush_durable(M)`**:
  Remove all deltas with `item.raft_index <= M` from `pending_by_pid` and advance `attr_flushed_index = M`.

### 7.3 `AuxOutbox` & Contiguous-Frontier Destination Idempotency State Machine
* **Unique Multi-Task Identity `(origin_shard_id, origin_raft_index, task_ordinal)`**:
  * A single Raft entry (e.g., `CreateFile`, `SetAttr(chown)`, or POSIX overwrite `Rename`) may emit multiple `AuxUpdateTask`s (`task_ordinal: u16 = 0, 1, 2, ...`).
  * `SpilledAuxOutbox` (`0x07`) is keyed as **`[0x07][origin_raft_index: u64 BE][task_ordinal: u16 BE]`** (`11` bytes), preventing same-entry tasks from overwriting one another when spilled to SSTables.
  * `OriginShard` tracks per-task completion across targets; `contiguous_aux_acked_index` advances to `R` **if and only if every `(idx, ord)` with `idx <= R` has been durably ACKed by its destination shard**.
  * **Replicated `[0x07]` Cleanup (`RaftCmd::AckAuxTasks`)**: As destination ACKs arrive, the leader coalesces `RaftCmd::AckAuxTasks { acked_task_ids: Vec<(u64, u16)>, new_contiguous_acked_index: u64 }` (every `100ms` or `512` tasks). Inside `apply`, every replica deletes the exact acknowledged `[0x07][idx][ord]` spill keys and updates `contiguous_aux_acked_index` (persisted in `DurableShardCheckpoint` on flush; `LocalityCompactor` purges `[0x07]` tombstones), bounding `[0x07]` size strictly.
  * On the shard leader, `AuxDispatcher` reads un-acked tasks (`spilled [0x07]` in ascending `(origin_raft_index, task_ordinal)` followed by in-memory `AuxOutbox`), partitions by target shard via `MetaCache`, and sends **at most one in-flight `ApplyAuxBatchRpc` per `(origin_shard_id, target_shard_id)`** sorted strictly by `(origin_raft_index, task_ordinal)`.
* **`QuotaTable` Contiguous Frontier, Exact-Once & Retirement Rule**:
  `QuotaRecord` maintains `applied_by_shard: BTreeMap<ShardId, (u64, u16)>` (`(last_raft_index, last_task_ordinal)`):
  * Each `QuotaDelta` carries `prev_uid_cursor: (u64, u16)` (the exact previous `(origin_raft_index, task_ordinal)` emitted by `origin_shard_id` for this `uid`, or `(0, 0)` initially).
  * Let `prev = record.applied_by_shard.get(&origin_shard_id).copied().unwrap_or((0, 0))`.
  * If `(origin_raft_index, task_ordinal) <= prev`: ignore as duplicate replay (`ACK` immediately).
  * If `prev_uid_cursor > prev`: **reject batch with `MetaDbError::AuxOutOfOrderGap`** so `OriginShard` resends from `prev` without skipping unapplied deltas.
  * Otherwise (`prev_uid_cursor == prev`): apply `used_bytes = (used_bytes + delta_bytes).max(0)`, `used_inodes = (used_inodes + delta_inodes).max(0)`, `last_applied_hlc = max(last_applied_hlc, hlc)`, and set `applied_by_shard[origin_shard_id] = (origin_raft_index, task_ordinal)`.
  * *Watermark Retirement*: If any shard is ever retired/merged and `GlobalManager` marks `retired_shard_fully_acked(shard_id)`, `QuotaRecord` writers/compactions prune retired `shard_id` keys so `applied_by_shard` remains bounded.
* **`SecondaryIndexTable` Total Order `(hlc, origin_shard_id, origin_raft_index, task_ordinal)` Rule**:
  `SecondaryIndexRecord` stores `{ fid, pid, name, create_time, last_ver: (HlcTimestamp, ShardId, u64, u16), tombstone_ver: (HlcTimestamp, ShardId, u64, u16), is_deleted: bool }`:
  * Comparisons use the **strictly total tuple `(hlc, origin_shard_id, origin_raft_index, task_ordinal)`** so cross-shard HLC ties are broken deterministically: `IndexUpsert` applies iff `incoming_ver > max(last_ver, tombstone_ver)`; `IndexDelete` applies iff `incoming_ver > max(last_ver, tombstone_ver)`.

---

## 8. Crash-Recoverable Primary-Anchored 2PC Protocol (`DetailedRange`)

### 8.1 Keys & Records
* `txn_id`: `u128` (`[coord_node_id: u64][monotonic_seq_or_hlc: u64]`).
* `ttl_ms`: `3,000ms`. The first `expire_hlc` is `start_hlc.physical_ms() + ttl_ms`. A coordinator that still holds `txn_id` and is still in `Pending` replicates `Extend2pcLease` before that deadline, updating the primary `TxnRecord`, every participant `WriteIntent`, and any `DirExistsFence` holder. Renewal stops at `start_hlc + 60,000ms` (two routing budgets). A renewal after expiry or abort is rejected and does not change a `Committed` record. `Committed` is not aborted because `expire_hlc` passed. Default `mvcc_retention_ms` stays `5,000ms`. The value of each prepared key at `start_hlc` is captured when the lease is installed, if that version is still provable, and a renew does not replace it. The capture is stored in the same L0 flush under prefix `0x0D`, keyed by transaction id and user key, as version 1 plus CRC32C. A point read behind the flush watermark returns that capture, or `TransactionTimeout` when none was proved. `Ok(None)` is proven absence. It does not return a newer collapsed value. Collapse-flush still keeps only the latest user-key value. A range read behind the flush watermark returns `TransactionTimeout` for the whole range, because an unversioned SST cannot prove absent or deleted keys. That timeout is not an empty range. The hold is released on commit or abort. After restart it is rebuilt from `TxnRecord` and live `WriteIntent`s only when a wall reading still tracks monotonic time and is inside `expire_hlc`. An unreadable clock, the 1 ms fallback, or a reading that has stepped away from monotonic time does not rebuild the hold and does not extend the lease. Prepare, the pre-commit check, and commit timestamp selection use that monotonic reading (`lease_wall_ms`), not `HybridLogicalClock::now`. A missing reading does not abort shards that are already prepared and does not publish a later `expire_hlc`. The durable lock stays until the replicated HLC passes `expire_hlc`, the stored window exceeds 60 seconds, or a later trusted reading does. `WriteIntentRecord` and `TxnStatusRecord` stay codec version 1. The capture stays on prefix `0x0D`. A directory-rename TTL above the 60-second cap is rejected and does not grant a fence. A rename-lease observation behind the published window returns `UntrustedClock` and does not move `expire_hlc`. A previous rename token cannot extend after a newer token exists.
* **Primary Key Assignment & Split-Resilient Routing**:
  * Cross-shard `Rename`: `primary_key = MetaKey::child(src_pid, src_name).encode()`.
  * Cross-shard `Mkdir` / `Rmdir`: `primary_key = MetaKey::child(parent_pid, name).encode()`.
  * Note: Participant `WriteIntentRecord`s route `CheckTxnStatus` via `MetaCache::route_key(&intent.primary_key)` (using `primary_shard_hint` only as a cache hint), so even if `PrimaryShard` subsequently splits, `[0x06][txn_id]` (partitioned by `txn.primary_key` affinity during `ExecuteSplit`) is always located deterministically!
* **`[0x05][target_user_key]` -> `WriteIntentRecord`**:
  `{ txn_id, primary_shard_hint, primary_key, start_hlc, expire_hlc, rename_fence_token: Option<u64>, intent_op }` where `intent_op` is:
  * `Put { value: Vec<u8>, attr_delta: Option<AttrDeltaSpec>, aux_tasks: Vec<AuxTaskSpec> }`
  * `Delete { attr_delta: Option<AttrDeltaSpec>, aux_tasks: Vec<AuxTaskSpec> }`
  * `DeletingDir { dir_id: InodeId }` (placed on `(dir_id, "_attr_")` during `Rmdir` Prepare)
  * `DirExistsFence { dir_id: InodeId }` (multi-txn refset `[0x05][dir_attr_key][txn_id]` conflicting **only** with `DeletingDir`).
* **`[0x06][txn_id: 16B BE]` -> `TxnStatusRecord`** (stored on the shard owning `primary_key`):
  `{ txn_id, primary_key, participant_keys: Vec<Vec<u8>>, pending_ack_keys: BTreeSet<Vec<u8>>, start_hlc, expire_hlc, rename_fence_token: Option<u64>, state: Pending | Committed { commit_hlc } | Aborted }`.
  * **Durable Completion Marker**: `[0x06][txn_id]` is **retained on disk/MemMvccStore until `pending_ack_keys` becomes empty** (i.e., every secondary participant has durably applied `CommitSecondary2pc` or `Abort2pc` and returned ACK; if an intent on a secondary was already applied and deleted on an earlier retry, `CommitSecondary2pc` returns idempotent `Ok` immediately!).

### 8.2 Phase 1 (Prepare), Monotonic `DirRenameGuard` Fencing & POSIX Overwrite Semantics
1. **Cross-Parent Directory Rename Fencing**: If moving a **Directory** across parents (`src_pid != dst_pid`), Coordinator acquires `DirRenameLease { rename_fence_token: u64, expire_hlc }` (`ttl = 3s`) from `GlobalManager` (which persists and monotonically increments `rename_fence_token`, never granting a new lease until the active lease is released or past `expire_hlc` + safety margin), and walks `dst_pid -> parent_pid -> ... -> ROOT_INODE_ID` verifying `moved_dir_id` never appears in `dst_pid`'s ancestor chain (`Err(MetaDbError::InvalidRenameCycle)` on match). Both `Prepare2pc` and `CommitPrimary2pc` verify `now_hlc <= lease.expire_hlc` and record `rename_fence_token` (rejecting stale coordinators if the lease expired!).
2. **Deterministic POSIX Overwrite `Rename` (`overwrite == true`)**:
   * Inside `Prepare2pc` on `Shard(dst_pid, dst_name)`:
     * If `(dst_pid, dst_name)` does not exist: `Put` intent encodes `dst_pid` `AttrDelta(+1 child, +source.size_bytes)` and `AuxTask::IndexUpsert(source.fid -> dst_pid, dst_name)`.
     * If `(dst_pid, dst_name)` exists as a regular `File` (`victim`, when `source` is also a `File`): `Put` intent deterministically encodes net `dst_pid` `AttrDelta(0 children, source.size_bytes - victim.size_bytes)` (or if `src_pid == dst_pid`, combined net `AttrDelta(-1 child, -victim.size_bytes)`), `QuotaDelta(victim.uid, -victim.size_bytes, -1 inode)`, `AuxTask::IndexDelete(victim.fid)`, and `AuxTask::IndexUpsert(source.fid -> dst_pid, dst_name)`.
3. Coordinator dispatches `Prepare2pc` in parallel to all participant shards with `expected_epoch`.
4. Inside `RaftStateMachine::apply(Prepare2pc)`:
   * Verify `expected_epoch`.
   * Check `[0x05][key]`: incompatible exclusive `WriteIntent`s return `MetaDbError::WriteIntentConflict` immediately without blocking inside `apply`.
   * Write `[0x05]` `WriteIntentRecord`(s); on `PrimaryShard`, atomically write `[0x06][txn_id] = TxnStatusRecord::Pending` (with `pending_ack_keys` initialized to all secondary participant keys) in the **same Raft entry**. Return `prepared_hlc = shard_clock.now()`.

### 8.3 Phase 2 (Primary Atomic Commit & Secondary Roll-Forward)
1. If **any** participant fails Prepare: Coordinator transitions `[0x06][txn_id] -> Aborted`, drives `Abort2pc(txn_id)` on all prepared secondaries until `pending_ack_keys` is empty, deletes `[0x06][txn_id]`, and releases `DirRenameLease` if held.
2. If **all** participants succeed Prepare:
   * Coordinator computes `commit_hlc = max(clock.now(), max(participant_prepared_hlcs))`.
   * Coordinator sends `CommitPrimary2pc { txn_id, commit_hlc, rename_fence_token }` to `PrimaryShard`.
   * Inside `PrimaryShard` Raft `apply`:
     * Look up `[0x06][txn_id]`. If `Aborted` or if `rename_fence_token` lease has expired (`commit_hlc > expire_hlc`), transition to `Aborted` and return `Err(MetaDbError::TransactionAborted)`. If already `Committed { c }`, return `Ok(c)` idempotently.
     * If `Pending`: atomically set `[0x06][txn_id].state = Committed { commit_hlc }`, materialize local `[0x05]` intents into `MemMvccStore` at `commit_hlc`, emit local `AttrDelta` and `AuxOutbox` entries at `commit_hlc`, and delete local `[0x05]` intents.
     * **Linearizability Point**: The transaction is now durably committed.
   * Coordinator sends `CommitSecondary2pc { txn_id, commit_hlc }` in parallel to Secondary Shard(s):
     * **Exact-Once Secondary Apply Proof**: Inside Secondary Shard Raft `apply`, if `[0x05][target_key]` with matching `txn_id` is present, it atomically materializes the `Put`/`Delete` mutation at `commit_hlc`, emits local `AttrDelta`/`AuxOutbox` entries, and deletes `[0x05][target_key]`; if `[0x05][target_key]` for `txn_id` is already absent (meaning an earlier `CommitSecondary2pc` retry already applied, since `CommitPrimary2pc` is unreachable unless `Prepare2pc` committed `[0x05]` and no abort is permitted after primary commit), it returns `Ok(AlreadyApplied)` as a pure no-op without duplicating deltas or auxiliary tasks.
     * As each secondary ACKs, `MarkParticipantAcked { txn_id, participant_keys }` removes ACKed keys from `pending_ack_keys` and deletes `[0x06][txn_id]` **only once `pending_ack_keys.is_empty()`**.

### 8.4 Read-Path Intent Resolution & `TxnRecoveryWorker`
* **Reader Encountering `[0x05]` Intent**: Any linearizable `get`/`scan` at `read_hlc` checks `[0x05]` in the target range:
  * If `intent.start_hlc > read_hlc`: ignore the intent (transaction could only commit at `commit_hlc > start_hlc > read_hlc`, invisible to this snapshot!).
  * If `intent.start_hlc <= read_hlc`: query `PrimaryShard::CheckTxnStatus(txn_id, caller_hlc = read_hlc)`:
    * `Committed { commit_hlc }` -> roll forward secondary intent locally via `CommitSecondary2pc` and evaluate visibility (`commit_hlc <= read_hlc`).
    * `Aborted` -> clean up secondary intent locally and continue read.
    * `Pending` -> abort only when expiry is proved by the replicated HLC, by a monotonic-tracking wall reading, or because the stored window exceeds 60 seconds. `CheckTxnStatus` does not propose when that proof is missing, so a raw wall jump is not committed as the abort HLC. The caller's `caller_hlc` is not that proof. A committed or aborted record is rolled forward or cleaned up on this read. Otherwise the lock stays. Directory-rename acquire and extend use the same monotonic reading. A jumped process wall does not replace a lease that is still inside its published `expire_hlc` and does not report that jump as expiry.
* **Write conflict**: a `WriteIntentConflict` finalizes a committed or aborted transaction immediately, and aborts a pending transaction only when expiry is proved. It does not wait for the next maintenance tick, and it does not treat an untrusted clock as "still inside the lease".
* **Background `TxnRecoveryWorker`**: The 250 ms maintenance tick still scans `[0x06]` and `[0x05]`. `Committed` or `Aborted` records with participant keys still unacked are driven on that tick and also on the read or conflict path above. A pending lease is aborted when the replicated HLC is past `expire_hlc`, a monotonic-tracking wall reading is past it, or the stored window exceeds 60 seconds. A missing or skewed wall clock does not abort it and does not extend it. A restarted coordinator may renew a still-`Pending` lease only while that wall reading is inside the published window and within the cap. `Extend2pcLease` for a transaction that is no longer a holder of the targeted intent returns `NotHolder` and writes nothing.

### 8.5 What this does not prove

* This is not a full 2PC production pass, not a three-voter exercise, and not a scale acceptance.
* A wall clock that is already wrong when the process takes its monotonic anchor, and that then moves with monotonic time, is not detected. The durable lock can outlive real time when no replicated HLC passes `expire_hlc` and the wall reading stays untrusted.
* SSTables still have no per-key HLC. The pinned-generation test shows one Linux process can compact a generation a snapshot already holds and still read that snapshot's bytes through the open file, and that an open `0x0D` capture is not replaced by a newer collapsed value. It does not prove a reader that reopens the unlinked path, and it does not prove every compaction schedule.
* Orphan `0x0D` rows are removed when a recovery pass sees them. There is no separate continuous pre-image compactor.
* Rejecting a previous holder at apply time is not a lease-transfer protocol. Automatic `transfer_leader`, Jumbo placement, and RDMA are out of scope.

---

## 9. Target File Boundaries (`DetailedRange`)

* `crates/metadb-raft/Cargo.toml`
* `crates/metadb-raft/src/{lib.rs, types.rs, log_store.rs, state_machine.rs, network.rs, read_index.rs, group_host.rs, conf_change.rs}`
* `crates/metadb-tx/Cargo.toml`
* `crates/metadb-tx/src/{lib.rs, state_machine_impl.rs, pushdown.rs, attr_aggregator.rs, aux_outbox.rs, two_pc.rs, recovery_worker.rs}`

---

## 10. Verification & Fault-Injection Test Plan (`DetailedRange`)

1. **T-RAFT-1 (WAL-less Torn-Log Crash & 3-Watermark Pinning)**: Write 500 files without flushing `MemMvccStore`, append a corrupted partial frame to `.raftlog`, restart node, verify torn frame is truncated and all 500 files + `AsyncAttrAggregator` counts + `AuxOutbox` tasks recover identically; verify `.raftlog` segments are pinned until all 3 watermarks advance.
2. **T-RAFT-2 (Online `1-Voter -> Learner -> 3-Voter` Under Live Load)**: Run continuous `create_file` traffic on a `1-Voter` shard, attach 2 new nodes as `Learner`s, verify SSTable snapshot + log catch-up, promote to `3-Voter`, kill original leader, and verify zero acknowledged writes lost and `ReadIndex` rejects reads on partitioned old leader.
3. **T-TX-1 (Coordinated Flush Barrier & Read Reconciliation Race)**: Spawn 16 writer tasks (`create_file`/`unlink`), 8 `dirstat` reader tasks, and continuous L0 MemTable flushes; verify `dirstat.num_children` never dips below committed truth or double-counts across flush boundaries.
4. **T-TX-2 (`AuxOutbox` Partition Spill & Failover Exact-Once)**: Block network to `QuotaTable` shard until `AuxOutbox` spills to `[0x07]`, verify Raft log purges normally, kill `OriginShard` leader, unblock network, and verify new leader drains `[0x07]` + duplicate replays leave `QuotaRecord.used_bytes` and `SecondaryIndexTable` 100% exact (including unlink tombstones blocking stale pre-unlink upserts).
5. **T-2PC-1 (Coordinator Crash Matrix & Concurrent Rename/Rmdir Races)**:
   * Crash coordinator (a) after Secondary Prepare before Primary Prepare, (b) after all Prepares before Primary Commit, and (c) after Primary Commit before Secondary Commit; verify `TxnRecoveryWorker` and concurrent readers deterministically abort (a)/(b) and roll forward (c).
   * Execute concurrent `rename(A->B)` + `rename(B->A)` (verify zero deadlock), concurrent cross-shard `rmdir(d)` + `create_file(d, "x")` (verify strict serialization via `DeletingDir` vs `DirExistsFence`), and `rename(/a/b, /a/b/c/d)` (verify `InvalidRenameCycle` rejection).
