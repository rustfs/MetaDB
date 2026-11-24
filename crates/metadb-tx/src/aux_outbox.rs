//! Per-shard Raft-replicated `ReplicatedAttrAggregator` and contiguous-frontier `ShardAuxOutbox`
//! with `[0x07][origin_raft_index: 8B BE][task_ordinal: 2B BE]` overflow spill and `AckAuxTasks` cleanup.

use std::collections::{BTreeMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};

use metadb_core::{
    decode_origin_quota_cursor_pair, encode_origin_quota_cursor_pair, saturating_add_i64,
    AuxIntentSpec, AuxOutboxEntry, AuxTaskPayload, DirShardDeltaKey, DirShardDeltaRecord,
    HlcTimestamp, InodeAttr, InodeId, MetaKey, OriginQuotaCursorKey, Result, ShardId,
    SpilledAuxOutboxKey, UserId,
};
use metadb_raft::ShardWatermarkTracker;
use metadb_storage::{LifecycleStorageEngine, MvccValue, SnapshotGuard, ViewAtRaftIndex};

/// Default maximum un-acknowledged tasks held in RAM before spilling to `[0x07]` on L0 flush.
pub const DEFAULT_OUTBOX_SPILL_THRESHOLD: usize = 256;

/// Folded `i128` directory deltas are stored in `i64` attributes.
/// Values outside `i64` saturate; the caller then clamps a negative total to 0.
fn clamp_delta_to_i64(value: i128) -> i64 {
    if value > i64::MAX as i128 {
        i64::MAX
    } else if value < i64::MIN as i128 {
        i64::MIN
    } else {
        value as i64
    }
}

#[derive(Debug, Clone)]
pub struct ReplicatedAttrDelta {
    pub raft_index: u64,
    pub hlc: HlcTimestamp,
    pub delta_children: i128,
    pub delta_bytes: i128,
}

/// Deterministic per-shard attribute delta aggregator integrated with `PreFlushHook` (`consensus_and_transactions.md` §7.2).
pub struct ReplicatedAttrAggregator {
    pending_by_pid: RwLock<BTreeMap<InodeId, VecDeque<ReplicatedAttrDelta>>>,
}

impl Default for ReplicatedAttrAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicatedAttrAggregator {
    pub fn new() -> Self {
        Self {
            pending_by_pid: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn record_delta(
        &self,
        pid: InodeId,
        raft_index: u64,
        hlc: HlcTimestamp,
        delta_children: i64,
        delta_bytes: i64,
    ) {
        let mut guard = self.pending_by_pid.write().unwrap();
        let queue = guard.entry(pid).or_default();
        if let Some(previous) = queue
            .back_mut()
            .filter(|delta| delta.raft_index == raft_index)
        {
            // A 2PC command may modify several keys in one directory. Fold its
            // deltas as one atomic step, avoiding transient counter overflow and
            // ensuring an applied-index baseline includes the whole command.
            previous.delta_children += i128::from(delta_children);
            previous.delta_bytes += i128::from(delta_bytes);
            previous.hlc = previous.hlc.max(hlc);
        } else {
            queue.push_back(ReplicatedAttrDelta {
                raft_index,
                hlc,
                delta_children: i128::from(delta_children),
                delta_bytes: i128::from(delta_bytes),
            });
        }
    }

    /// Reconciles an authoritative `(pid, "_attr_")` record at `snap.cut()` (`Resolution B1`).
    pub fn reconcile_inode_attr(
        &self,
        pid: InodeId,
        mut base: InodeAttr,
        snap: &SnapshotGuard,
    ) -> InodeAttr {
        let cut = snap.cut();
        let guard = self.pending_by_pid.read().unwrap();
        let baseline = base.applied_lsn;
        if let Some(q) = guard.get(&pid) {
            for d in q {
                if d.raft_index > baseline
                    && d.raft_index <= cut.max_raft_index
                    && d.hlc <= cut.max_hlc
                {
                    base.num_children =
                        saturating_add_i64(base.num_children, clamp_delta_to_i64(d.delta_children))
                            .max(0);
                    base.size_bytes =
                        saturating_add_i64(base.size_bytes, clamp_delta_to_i64(d.delta_bytes))
                            .max(0);
                    if d.hlc > base.modify_time {
                        base.modify_time = d.hlc;
                    }
                    base.applied_lsn = d.raft_index;
                }
            }
        }
        base
    }

    /// Reconciles a non-anchor `DualModeFineGrained` `[0x04][pid]` record at `snap.cut()`.
    pub fn reconcile_dir_shard_delta(
        &self,
        pid: InodeId,
        mut base: DirShardDeltaRecord,
        snap: &SnapshotGuard,
    ) -> DirShardDeltaRecord {
        let cut = snap.cut();
        let guard = self.pending_by_pid.read().unwrap();
        let baseline = base.applied_raft_index;
        if let Some(q) = guard.get(&pid) {
            for d in q {
                if d.raft_index > baseline
                    && d.raft_index <= cut.max_raft_index
                    && d.hlc <= cut.max_hlc
                {
                    base.num_children =
                        saturating_add_i64(base.num_children, clamp_delta_to_i64(d.delta_children))
                            .max(0);
                    base.size_bytes =
                        saturating_add_i64(base.size_bytes, clamp_delta_to_i64(d.delta_bytes))
                            .max(0);
                    if d.hlc > base.modify_time {
                        base.modify_time = d.hlc;
                    }
                    base.applied_raft_index = d.raft_index;
                }
            }
        }
        base
    }

    pub(crate) fn pending_totals_since(&self, pid: InodeId, baseline: u64) -> (i128, i128) {
        let guard = self.pending_by_pid.read().unwrap();
        guard
            .get(&pid)
            .into_iter()
            .flat_map(|q| q.iter())
            .filter(|delta| delta.raft_index > baseline)
            .fold((0, 0), |(children, bytes), delta| {
                (children + delta.delta_children, bytes + delta.delta_bytes)
            })
    }

    /// Last child mutation represented by pending deltas at a serialized apply boundary.
    pub(crate) fn latest_delta_hlc(&self, pid: InodeId, max_raft_index: u64) -> HlcTimestamp {
        self.pending_by_pid
            .read()
            .unwrap()
            .get(&pid)
            .into_iter()
            .flat_map(|queue| queue.iter())
            .filter(|delta| delta.raft_index <= max_raft_index)
            .map(|delta| delta.hlc)
            .max()
            .unwrap_or(HlcTimestamp::ZERO)
    }

    /// Folds deltas in `(baseline.applied_index, sealed_raft_index]` reading `view_at_m`
    /// across `SealingMemTable + ImmMemTables + VersionSet` (`Resolution B5` / `G-S3`).
    pub fn fold_into_seal_batch(
        &self,
        sealed_raft_index: u64,
        view_at_m: &ViewAtRaftIndex<'_>,
    ) -> Result<Vec<(Vec<u8>, HlcTimestamp, MvccValue)>> {
        let guard = self.pending_by_pid.read().unwrap();
        let mut out = Vec::new();

        for (&pid, queue) in guard.iter() {
            let has_eligible = queue.iter().any(|d| d.raft_index <= sealed_raft_index);
            if !has_eligible {
                continue;
            }

            let attr_key = MetaKey::dir_attr(pid).encode();
            if let Some(raw_attr) = view_at_m.get(&attr_key)? {
                let mut attr = InodeAttr::decode(&raw_attr)?;
                let mut changed = false;
                let mut max_folded_hlc = HlcTimestamp::ZERO;
                let baseline = attr.applied_lsn;
                for d in queue {
                    if d.raft_index > baseline && d.raft_index <= sealed_raft_index {
                        attr.num_children = saturating_add_i64(
                            attr.num_children,
                            clamp_delta_to_i64(d.delta_children),
                        )
                        .max(0);
                        attr.size_bytes =
                            saturating_add_i64(attr.size_bytes, clamp_delta_to_i64(d.delta_bytes))
                                .max(0);
                        if d.hlc > attr.modify_time {
                            attr.modify_time = d.hlc;
                        }
                        max_folded_hlc = max_folded_hlc.max(d.hlc);
                        attr.applied_lsn = d.raft_index;
                        changed = true;
                    }
                }
                if changed {
                    out.push((attr_key, max_folded_hlc, MvccValue::Value(attr.encode())));
                }
                continue;
            }

            // Fallback: check if `pid` is a non-anchor `DualModeFineGrained` sub-shard (`[0x04][pid]`).
            let shard_delta_key = DirShardDeltaKey { pid }.encode().to_vec();
            if let Some(raw_sub) = view_at_m.get(&shard_delta_key)? {
                let mut sub = DirShardDeltaRecord::decode(&raw_sub)?;
                let mut changed = false;
                let mut max_folded_hlc = HlcTimestamp::ZERO;
                let baseline = sub.applied_raft_index;
                for d in queue {
                    if d.raft_index > baseline && d.raft_index <= sealed_raft_index {
                        sub.num_children = saturating_add_i64(
                            sub.num_children,
                            clamp_delta_to_i64(d.delta_children),
                        )
                        .max(0);
                        sub.size_bytes =
                            saturating_add_i64(sub.size_bytes, clamp_delta_to_i64(d.delta_bytes))
                                .max(0);
                        if d.hlc > sub.modify_time {
                            sub.modify_time = d.hlc;
                        }
                        max_folded_hlc = max_folded_hlc.max(d.hlc);
                        sub.applied_raft_index = d.raft_index;
                        changed = true;
                    }
                }
                if changed {
                    out.push((
                        shard_delta_key,
                        max_folded_hlc,
                        MvccValue::Value(sub.encode()),
                    ));
                }
            }
        }
        Ok(out)
    }

    /// Prunes folded deltas `<= durable_raft_index` strictly AFTER `VersionSet` publication.
    pub fn on_flush_durable(&self, durable_raft_index: u64) {
        let mut guard = self.pending_by_pid.write().unwrap();
        for queue in guard.values_mut() {
            while let Some(front) = queue.front() {
                if front.raft_index <= durable_raft_index {
                    queue.pop_front();
                } else {
                    break;
                }
            }
        }
        guard.retain(|_, q| !q.is_empty());
    }

    pub fn clear_all(&self) {
        self.pending_by_pid.write().unwrap().clear();
    }

    pub fn pending_delta_count(&self) -> usize {
        self.pending_by_pid
            .read()
            .unwrap()
            .values()
            .map(|q| q.len())
            .sum()
    }
}

#[derive(Debug, Clone)]
struct TrackedOutboxTask {
    entry: AuxOutboxEntry,
    sealed_at_index: Option<u64>,
    durable_spilled: bool,
    reconstructable_from_log: bool,
}

struct OutboxInner {
    tasks: BTreeMap<(u64, u16), TrackedOutboxTask>,
    last_uid_cursor: BTreeMap<UserId, (u64, u16)>,
    acked_uid_cursor: BTreeMap<UserId, (u64, u16)>,
    contiguous_acked_index: u64,
    max_observed_raft_index: u64,
    last_durable_seal_index: u64,
}

impl OutboxInner {
    fn recompute_contiguous_acked(&mut self) {
        if let Some((&(first_unacked_idx, _), _)) = self.tasks.first_key_value() {
            let candidate = first_unacked_idx.saturating_sub(1);
            if candidate > self.contiguous_acked_index {
                self.contiguous_acked_index = candidate;
            }
        } else if self.max_observed_raft_index > self.contiguous_acked_index {
            self.contiguous_acked_index = self.max_observed_raft_index;
        }
    }

    /// Computes the exact contiguous durable coverage frontier (`Alignment #2` & `Resolution B8`):
    /// Highest `R` such that EVERY task with `origin_raft_index <= R` is either durably ACKed
    /// OR durably persisted in `[0x07] SpilledAuxOutbox`.
    fn contiguous_durable_frontier(&self) -> u64 {
        for (&(idx, _), item) in &self.tasks {
            if !item.durable_spilled {
                return idx.saturating_sub(1).max(self.contiguous_acked_index);
            }
        }
        self.contiguous_acked_index
            .max(self.last_durable_seal_index)
            .max(
                self.tasks
                    .last_key_value()
                    .map(|(&(idx, _), _)| idx)
                    .unwrap_or(0),
            )
    }
}

/// Per-shard replicated auxiliary outbox with `[0x07][origin_raft_index][task_ordinal]` overflow spill.
pub struct ShardAuxOutbox {
    origin_shard_id: ShardId,
    spill_threshold: usize,
    force_spill_next_seal: AtomicBool,
    watermarks: Arc<ShardWatermarkTracker>,
    inner: RwLock<OutboxInner>,
}

impl ShardAuxOutbox {
    pub fn new(
        origin_shard_id: ShardId,
        spill_threshold: usize,
        watermarks: Arc<ShardWatermarkTracker>,
    ) -> Self {
        Self {
            origin_shard_id,
            spill_threshold: spill_threshold.max(1),
            force_spill_next_seal: AtomicBool::new(false),
            watermarks,
            inner: RwLock::new(OutboxInner {
                tasks: BTreeMap::new(),
                last_uid_cursor: BTreeMap::new(),
                acked_uid_cursor: BTreeMap::new(),
                contiguous_acked_index: 0,
                max_observed_raft_index: 0,
                last_durable_seal_index: 0,
            }),
        }
    }

    pub fn origin_shard_id(&self) -> ShardId {
        self.origin_shard_id
    }

    pub fn set_force_spill_next_seal(&self) {
        self.force_spill_next_seal.store(true, Ordering::Release);
    }

    /// Records all auxiliary specs emitted by a single Raft entry (`task_ordinal = 0, 1, ...`).
    /// If `specs` is empty, advances `contiguous_acked_index` if all prior entries were already ACKed.
    pub fn record_entry_specs(
        &self,
        raft_index: u64,
        hlc: HlcTimestamp,
        specs: Vec<AuxIntentSpec>,
    ) {
        self.record_entry_specs_with_reconstructability(raft_index, hlc, specs, true);
    }

    /// Records auxiliary specs with an explicit `reconstructable_from_log` flag.
    /// Tasks whose pre-image attributes are overwritten/deleted on L0 seal (`reconstructable_from_log = false`)
    /// automatically spill into `[0x07]` when a memtable seal occurs before acknowledgment.
    pub fn record_entry_specs_with_reconstructability(
        &self,
        raft_index: u64,
        hlc: HlcTimestamp,
        specs: Vec<AuxIntentSpec>,
        reconstructable_from_log: bool,
    ) {
        let mut guard = self.inner.write().unwrap();
        guard.max_observed_raft_index = guard.max_observed_raft_index.max(raft_index);

        if specs.is_empty() {
            if guard.tasks.is_empty() {
                guard.contiguous_acked_index = raft_index;
                self.watermarks.advance_contiguous_aux_acked(raft_index);
                self.watermarks.advance_aux_outbox_durable(raft_index);
            }
            return;
        }

        for (ord_usize, spec) in specs.into_iter().enumerate() {
            let task_ordinal = ord_usize as u16;
            let payload = match spec {
                AuxIntentSpec::QuotaDelta {
                    uid,
                    delta_bytes,
                    delta_inodes,
                } => {
                    let prev_uid_cursor = guard
                        .last_uid_cursor
                        .insert(uid, (raft_index, task_ordinal))
                        .unwrap_or((0, 0));
                    AuxTaskPayload::QuotaDelta {
                        uid,
                        delta_bytes,
                        delta_inodes,
                        prev_uid_cursor,
                    }
                }
                AuxIntentSpec::IndexUpsert {
                    fid,
                    pid,
                    name,
                    create_time,
                } => AuxTaskPayload::IndexUpsert {
                    fid,
                    pid,
                    name,
                    create_time,
                },
                AuxIntentSpec::IndexDelete { fid } => AuxTaskPayload::IndexDelete { fid },
            };

            let entry = AuxOutboxEntry {
                origin_shard_id: self.origin_shard_id,
                origin_raft_index: raft_index,
                task_ordinal,
                hlc,
                payload,
            };
            guard.tasks.insert(
                (raft_index, task_ordinal),
                TrackedOutboxTask {
                    entry,
                    sealed_at_index: None,
                    durable_spilled: false,
                    reconstructable_from_log,
                },
            );
        }
    }

    /// Writes each quota cursor emitted by `raft_index` to `[0x08][uid]`.
    ///
    /// Acknowledged `[0x07]` tasks are deleted, so rehydrate cannot recover the predecessor
    /// from the spill alone. Persists `(latest_cursor, acked_cursor)` (`V2`) so unspilled
    /// `.raftlog` reconstruction preserves the durable acknowledged baseline after L0 folding.
    pub fn persist_quota_cursors(
        &self,
        engine: &LifecycleStorageEngine,
        hlc: HlcTimestamp,
        raft_index: u64,
    ) {
        // Copy under the outbox lock, then write the engine after releasing it.
        // `seal_active_memtable` holds the MVCC write lock and then takes this
        // outbox lock to spill. Writing the memtable while this read lock is held
        // deadlocks that flush against the apply that is persisting the cursor.
        let writes: Vec<(Vec<u8>, Vec<u8>)> = {
            let guard = self.inner.read().unwrap();
            guard
                .last_uid_cursor
                .iter()
                .filter(|(_, (idx, _))| *idx == raft_index)
                .map(|(&uid, &(idx, ord))| {
                    let acked = guard.acked_uid_cursor.get(&uid).copied().unwrap_or((0, 0));
                    (
                        OriginQuotaCursorKey { uid }.encode().to_vec(),
                        encode_origin_quota_cursor_pair((idx, ord), acked),
                    )
                })
                .collect()
        };
        for (key, value) in writes {
            engine.apply_committed_put(key, value, hlc, raft_index);
        }
    }

    /// Spills un-acked tasks `<= sealed_raft_index` into `[0x07][origin_raft_index][task_ordinal]`
    /// whenever `force_spill_next_seal` is set, any non-reconstructable task is pending,
    /// OR unspilled task count `>= spill_threshold`.
    pub fn spill_into_seal_batch(
        &self,
        sealed_raft_index: u64,
        sealed_max_hlc: HlcTimestamp,
    ) -> Vec<(Vec<u8>, HlcTimestamp, MvccValue)> {
        let force = self.force_spill_next_seal.swap(false, Ordering::AcqRel);
        let mut guard = self.inner.write().unwrap();

        let mut unspilled_count = 0usize;
        let mut has_non_reconstructable = false;
        for (&(idx, _), t) in &guard.tasks {
            if idx <= sealed_raft_index && !t.durable_spilled {
                unspilled_count += 1;
                if !t.reconstructable_from_log {
                    has_non_reconstructable = true;
                }
            }
        }

        if !force && !has_non_reconstructable && unspilled_count < self.spill_threshold {
            return Vec::new();
        }

        let mut out = Vec::with_capacity(unspilled_count);
        for (&(idx, ord), item) in guard.tasks.iter_mut() {
            if idx <= sealed_raft_index && !item.durable_spilled && item.sealed_at_index.is_none() {
                let key = SpilledAuxOutboxKey::new(idx, ord).encode().to_vec();
                let val = item.entry.encode();
                item.sealed_at_index = Some(sealed_raft_index);
                out.push((key, sealed_max_hlc, MvccValue::Value(val)));
            }
        }
        out
    }

    /// Invoked strictly after L0 SSTable + `MANIFEST` `fdatasync` completes at `durable_raft_index`.
    /// Returns the exact contiguous `aux_outbox_durable_index` frontier (`Alignment #2`).
    pub fn on_flush_durable(&self, durable_raft_index: u64) -> u64 {
        let mut guard = self.inner.write().unwrap();
        let mut all_up_to_m_covered = true;
        for (&(idx, _), item) in guard.tasks.iter_mut() {
            if idx <= durable_raft_index {
                if let Some(seal_idx) = item.sealed_at_index {
                    if seal_idx <= durable_raft_index {
                        item.durable_spilled = true;
                    }
                }
                if !item.durable_spilled {
                    all_up_to_m_covered = false;
                }
            }
        }
        if all_up_to_m_covered {
            guard.last_durable_seal_index = guard.last_durable_seal_index.max(durable_raft_index);
        }
        let frontier = guard.contiguous_durable_frontier();
        self.watermarks.advance_aux_outbox_durable(frontier);
        frontier
    }

    /// Applies `RaftCmd::AckAuxTasks`, deleting spilled `[0x07]` keys and advancing contiguous watermarks.
    pub fn apply_acked_tasks(
        &self,
        raft_index: u64,
        hlc: HlcTimestamp,
        acked_keys: &[(u64, u16)],
        engine: &LifecycleStorageEngine,
    ) -> u64 {
        // Collect deletions and acked cursor updates under the outbox lock, then write them after
        // releasing it. A size-triggered flush takes this same lock from `spill_into_seal_batch`.
        let (deletes, cursor_updates, acked) = {
            let mut guard = self.inner.write().unwrap();
            guard.max_observed_raft_index = guard.max_observed_raft_index.max(raft_index);
            let mut deletes = Vec::new();
            let mut touched_uids = std::collections::BTreeSet::new();
            for &(idx, ord) in acked_keys {
                let delete_spill = if let Some(removed) = guard.tasks.remove(&(idx, ord)) {
                    if let AuxTaskPayload::QuotaDelta { uid, .. } = &removed.entry.payload {
                        bump_uid_cursor(&mut guard.acked_uid_cursor, *uid, (idx, ord));
                        touched_uids.insert(*uid);
                    }
                    removed.durable_spilled || removed.sealed_at_index.is_some()
                } else {
                    // Idempotent delete in case `[0x07]` existed from prior snapshot/recovery.
                    true
                };
                if delete_spill {
                    deletes.push(SpilledAuxOutboxKey::new(idx, ord).encode().to_vec());
                }
            }
            let cursor_updates: Vec<(Vec<u8>, Vec<u8>)> = touched_uids
                .into_iter()
                .map(|uid| {
                    let acked_cur = guard.acked_uid_cursor.get(&uid).copied().unwrap_or((0, 0));
                    let latest_cur = guard
                        .last_uid_cursor
                        .get(&uid)
                        .copied()
                        .unwrap_or(acked_cur);
                    (
                        OriginQuotaCursorKey { uid }.encode().to_vec(),
                        encode_origin_quota_cursor_pair(latest_cur, acked_cur),
                    )
                })
                .collect();
            guard.recompute_contiguous_acked();
            self.watermarks
                .advance_contiguous_aux_acked(guard.contiguous_acked_index);
            let frontier = guard.contiguous_durable_frontier();
            self.watermarks.advance_aux_outbox_durable(frontier);
            (deletes, cursor_updates, guard.contiguous_acked_index)
        };
        for key in deletes {
            engine.apply_committed_delete(key, hlc, raft_index);
        }
        for (key, val) in cursor_updates {
            engine.apply_committed_put(key, val, hlc, raft_index);
        }
        acked
    }

    /// Rehydrates spilled `[0x07]` tasks from `engine` on startup or after `install_snapshot` (`Resolution B6`).
    pub fn rehydrate_from_storage(
        &self,
        contiguous_acked_index: u64,
        flushed_raft_index: u64,
        engine: &LifecycleStorageEngine,
    ) -> Result<()> {
        let start = SpilledAuxOutboxKey::prefix_start(contiguous_acked_index);
        let end = SpilledAuxOutboxKey::prefix_end();
        let snap = engine.acquire_linearizable_snapshot(flushed_raft_index)?;
        let rows = engine.scan_with_snapshot(&start, &end, &snap)?;

        let mut guard = self.inner.write().unwrap();
        guard.tasks.clear();
        guard.last_uid_cursor.clear();
        guard.acked_uid_cursor.clear();
        guard.contiguous_acked_index = contiguous_acked_index;
        guard.max_observed_raft_index = flushed_raft_index.max(contiguous_acked_index);
        guard.last_durable_seal_index = flushed_raft_index;

        for (k_bytes, v_bytes) in rows {
            let sk = SpilledAuxOutboxKey::decode(&k_bytes)?;
            let entry = AuxOutboxEntry::decode(&v_bytes)?;
            if let AuxTaskPayload::QuotaDelta { uid, .. } = &entry.payload {
                bump_uid_cursor(
                    &mut guard.last_uid_cursor,
                    *uid,
                    (sk.origin_raft_index, sk.task_ordinal),
                );
            }
            guard.tasks.insert(
                (sk.origin_raft_index, sk.task_ordinal),
                TrackedOutboxTask {
                    entry,
                    sealed_at_index: Some(sk.origin_raft_index),
                    durable_spilled: true,
                    reconstructable_from_log: true,
                },
            );
        }

        let cursor_rows = engine.scan_with_snapshot(
            &OriginQuotaCursorKey::prefix_start(),
            &OriginQuotaCursorKey::prefix_end(),
            &snap,
        )?;
        for (k_bytes, v_bytes) in cursor_rows {
            let key = OriginQuotaCursorKey::decode(&k_bytes)?;
            let (latest, acked) = decode_origin_quota_cursor_pair(&v_bytes)?;
            let effective_acked = if latest.0 <= contiguous_acked_index {
                latest
            } else {
                acked
            };
            if effective_acked != (0, 0) {
                bump_uid_cursor(&mut guard.acked_uid_cursor, key.uid, effective_acked);
                bump_uid_cursor(&mut guard.last_uid_cursor, key.uid, effective_acked);
            }
        }

        let frontier = guard.contiguous_durable_frontier();
        self.watermarks
            .set_contiguous_aux_acked(guard.contiguous_acked_index);
        self.watermarks.set_aux_outbox_durable(frontier);
        Ok(())
    }

    /// Reconstructs unspilled tasks in `(contiguous_acked_index ..= flushed_raft_index]` from `.raftlog`
    /// after a crash following a size-triggered L0 flush below `spill_threshold`.
    pub fn recover_unspilled_entry_specs(
        &self,
        raft_index: u64,
        hlc: HlcTimestamp,
        specs: Vec<AuxIntentSpec>,
    ) {
        if specs.is_empty() {
            return;
        }
        let mut guard = self.inner.write().unwrap();
        if raft_index <= guard.contiguous_acked_index {
            return;
        }
        if guard
            .tasks
            .range((raft_index, 0)..=(raft_index, u16::MAX))
            .next()
            .is_some()
        {
            return;
        }
        guard.max_observed_raft_index = guard.max_observed_raft_index.max(raft_index);
        for (ord_usize, spec) in specs.into_iter().enumerate() {
            let task_ordinal = ord_usize as u16;
            let payload = match spec {
                AuxIntentSpec::QuotaDelta {
                    uid,
                    delta_bytes,
                    delta_inodes,
                } => {
                    let prev_uid_cursor = guard
                        .last_uid_cursor
                        .insert(uid, (raft_index, task_ordinal))
                        .unwrap_or((0, 0));
                    AuxTaskPayload::QuotaDelta {
                        uid,
                        delta_bytes,
                        delta_inodes,
                        prev_uid_cursor,
                    }
                }
                AuxIntentSpec::IndexUpsert {
                    fid,
                    pid,
                    name,
                    create_time,
                } => AuxTaskPayload::IndexUpsert {
                    fid,
                    pid,
                    name,
                    create_time,
                },
                AuxIntentSpec::IndexDelete { fid } => AuxTaskPayload::IndexDelete { fid },
            };
            let entry = AuxOutboxEntry {
                origin_shard_id: self.origin_shard_id,
                origin_raft_index: raft_index,
                task_ordinal,
                hlc,
                payload,
            };
            guard.tasks.insert(
                (raft_index, task_ordinal),
                TrackedOutboxTask {
                    entry,
                    sealed_at_index: None,
                    durable_spilled: false,
                    reconstructable_from_log: true,
                },
            );
        }
        guard.last_durable_seal_index = guard.contiguous_acked_index;
        let frontier = guard.contiguous_durable_frontier();
        self.watermarks.set_aux_outbox_durable(frontier);
    }

    /// Returns all currently un-acknowledged entries sorted strictly by `(origin_raft_index, task_ordinal)`.
    pub fn pending_entries(&self) -> Vec<AuxOutboxEntry> {
        let guard = self.inner.read().unwrap();
        guard.tasks.values().map(|t| t.entry.clone()).collect()
    }

    pub fn pending_count(&self) -> usize {
        self.inner.read().unwrap().tasks.len()
    }

    pub fn contiguous_acked_index(&self) -> u64 {
        self.inner.read().unwrap().contiguous_acked_index
    }

    pub fn contiguous_durable_frontier(&self) -> u64 {
        self.inner.read().unwrap().contiguous_durable_frontier()
    }
}

fn bump_uid_cursor(map: &mut BTreeMap<UserId, (u64, u16)>, uid: UserId, cursor: (u64, u16)) {
    map.entry(uid)
        .and_modify(|cur| {
            if cursor > *cur {
                *cur = cursor;
            }
        })
        .or_insert(cursor);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use metadb_core::{AuxIntentSpec, AuxTaskPayload, HlcTimestamp};
    use metadb_raft::ShardWatermarkTracker;
    use metadb_storage::LifecycleStorageEngine;

    use super::*;

    #[test]
    fn rehydrate_keeps_acked_quota_cursor() {
        let engine = LifecycleStorageEngine::new(1, 5_000, 128, false);
        let hlc = HlcTimestamp::new(1_000, 0);
        let wm = Arc::new(ShardWatermarkTracker::default());
        let outbox = ShardAuxOutbox::new(1, 64, wm.clone());
        outbox.record_entry_specs(
            3,
            hlc,
            vec![AuxIntentSpec::QuotaDelta {
                uid: 7,
                delta_bytes: 10,
                delta_inodes: 1,
            }],
        );
        outbox.persist_quota_cursors(&engine, hlc, 3);
        outbox.apply_acked_tasks(4, hlc, &[(3, 0)], &engine);
        assert_eq!(outbox.pending_count(), 0);

        let restarted = ShardAuxOutbox::new(1, 64, wm);
        restarted.rehydrate_from_storage(3, 3, &engine).unwrap();
        restarted.record_entry_specs(
            8,
            hlc,
            vec![AuxIntentSpec::QuotaDelta {
                uid: 7,
                delta_bytes: 1,
                delta_inodes: 0,
            }],
        );
        let pending = restarted.pending_entries();
        assert_eq!(pending.len(), 1);
        match &pending[0].payload {
            AuxTaskPayload::QuotaDelta {
                prev_uid_cursor, ..
            } => {
                assert_eq!(*prev_uid_cursor, (3, 0));
            }
            other => panic!("expected quota delta, got {other:?}"),
        }
    }
}
