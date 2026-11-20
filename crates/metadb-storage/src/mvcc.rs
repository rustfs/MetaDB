//! In-Memory MVCC (`MemMvccStore`) with `SnapshotGuard` `ReadCut` isolation,
//! `ActiveSnapshotRegistry` Safe-Flush Gate, and `PreFlushHook` Coordinated Flush Barrier.

use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, RwLock};
use std::time::{Duration, Instant};

use metadb_core::{
    encode_lease_preimage, lease_preimage_key, HlcTimestamp, LeasePreimageValue, MetaDbError,
    Result,
};

use crate::backend::{PersistentEntry, StorageBackend};
use crate::manifest::DurableShardCheckpoint;
use crate::raft_log::LogSequenceNumber;
use crate::version_set::Version;

/// Default transaction timeout and in-memory MVCC retention window (`5,000 ms`, §6.1).
pub const DEFAULT_MVCC_RETENTION_MS: u64 = 5_000;

/// Total immutable read cut `(max_raft_index, max_hlc)` pinned by a `SnapshotGuard` (`Resolution B1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadCut {
    pub max_raft_index: LogSequenceNumber,
    pub max_hlc: HlcTimestamp,
}

/// Value state of an in-memory MVCC version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MvccValue {
    /// Live byte value.
    Value(Vec<u8>),
    /// Logical MVCC tombstone (kept strictly in memory; collapsed to a single physical LSM tombstone on flush).
    Tombstone,
}

/// A single timestamped and Raft-indexed version in memory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MvccVersion {
    pub hlc: HlcTimestamp,
    pub lsn: LogSequenceNumber,
    pub value: MvccValue,
}

/// Sealed immutable MemTable queued for collapse-flush once `min_active_snapshot_hlc >= max_hlc`.
#[derive(Debug, Clone)]
pub struct ImmMemTable {
    pub seal_id: u64,
    pub max_raft_index: LogSequenceNumber,
    pub max_hlc: HlcTimestamp,
    pub checkpoint: DurableShardCheckpoint,
    pub entries: BTreeMap<Vec<u8>, Vec<MvccVersion>>,
}

impl ImmMemTable {
    pub fn get_at_cut(&self, key: &[u8], cut: ReadCut) -> Option<MvccValue> {
        let chain = self.entries.get(key)?;
        for v in chain.iter().rev() {
            if v.lsn <= cut.max_raft_index && v.hlc <= cut.max_hlc {
                return Some(v.value.clone());
            }
        }
        None
    }
}

/// Read-only view spanning `SealingMemTable (<= M) -> queued ImmMemTables (<= M) -> VersionSet`
/// passed into `PreFlushHook::on_seal_memtable(M)` (`Resolution B5` / `G-S3`).
pub struct ViewAtRaftIndex<'a> {
    sealed_index: LogSequenceNumber,
    sealing_entries: &'a BTreeMap<Vec<u8>, VecDeque<MvccVersion>>,
    imm_queue: &'a VecDeque<Arc<ImmMemTable>>,
    backend: &'a dyn StorageBackend,
    version: Arc<Version>,
}

impl<'a> ViewAtRaftIndex<'a> {
    /// Reads the latest value of `key` committed at `raft_index <= M` across memory and disk.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(chain) = self.sealing_entries.get(key) {
            for v in chain.iter().rev() {
                if v.lsn <= self.sealed_index {
                    return Ok(match &v.value {
                        MvccValue::Value(b) => Some(b.clone()),
                        MvccValue::Tombstone => None,
                    });
                }
            }
        }
        for imm in self.imm_queue.iter().rev() {
            if let Some(chain) = imm.entries.get(key) {
                for v in chain.iter().rev() {
                    if v.lsn <= self.sealed_index {
                        return Ok(match &v.value {
                            MvccValue::Value(b) => Some(b.clone()),
                            MvccValue::Tombstone => None,
                        });
                    }
                }
            }
        }
        match self.backend.get_in_version(&self.version, key)? {
            Some(PersistentEntry::Value(b)) => Ok(Some(b)),
            Some(PersistentEntry::PhysicalTombstone) | None => Ok(None),
        }
    }
}

/// Hook for atomic Coordinated Flush Barriers (`AsyncAttrAggregator` folding + `AuxOutbox` `[0x07]` spill).
pub trait PreFlushHook: Send + Sync {
    fn on_seal_memtable(
        &self,
        sealed_raft_index: LogSequenceNumber,
        sealed_max_hlc: HlcTimestamp,
        view_at_m: &ViewAtRaftIndex<'_>,
    ) -> Result<Vec<(Vec<u8>, HlcTimestamp, MvccValue)>>;

    /// Invoked strictly after the L0 SSTable and `MANIFEST` are `fdatasync`'d and published.
    /// Returns the contiguous `aux_outbox_durable_index` coverage frontier.
    fn on_flush_durable(
        &self,
        durable_raft_index: LogSequenceNumber,
        durable_max_hlc: HlcTimestamp,
    ) -> LogSequenceNumber;
}

/// Collapsed key output emitted when flushing an `ImmMemTable` to an `L0` SSTable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushedKeyState {
    pub key: Vec<u8>,
    pub lsn: LogSequenceNumber,
    pub latest_value: Option<Vec<u8>>,
    pub pruned_versions: usize,
}

/// Shared active MemTable generation (`Arc<ActiveMemTable>`), pinned inside `PinnedSnapshotData`
/// and cleared on 5s forced expiration (`Resolution B1`).
pub type ActiveMemTable = RwLock<BTreeMap<Vec<u8>, VecDeque<MvccVersion>>>;

/// Point-in-time pinned `(ActiveMemTable, ImmMemTables, Version)` held inside `SnapshotSlot.state`
/// so forced 5s expiration drops all `Arc`s immediately even if a stalled caller retains `SnapshotGuard`.
pub struct PinnedSnapshotData {
    pub active: Arc<ActiveMemTable>,
    pub imms: Vec<Arc<ImmMemTable>>,
    pub version: Arc<Version>,
}

pub struct SnapshotSlot {
    pub id: u64,
    pub cut: ReadCut,
    pub pinned_flushed_lsn: LogSequenceNumber,
    pub created_at: Instant,
    pub expired: AtomicBool,
    /// Step-level read/write lock protecting `Option<PinnedSnapshotData>`, held ONLY during bounded
    /// `get` / 256-key `scan_page` steps (`<100µs`) so forced expiration at `5,000ms` clears `None`
    /// and reclaims RAM/SSTables without indefinite blocking.
    pub state: RwLock<Option<PinnedSnapshotData>>,
}

impl SnapshotSlot {
    /// Returns `true` if `PinnedSnapshotData` is still resident (becomes `false` immediately on forced expiration or drop).
    pub fn has_pinned_data(&self) -> bool {
        self.state.read().unwrap().is_some()
    }
}

/// Registry of active point-in-time `SnapshotGuard`s gating unversioned L0 SSTable collapse-flushes.
struct LeaseHold {
    /// Oldest HLC a reader of this transaction may still need.
    retain_not_before: HlcTimestamp,
    /// Wall-clock end of the hold. Renewals push this forward with `expire_hlc`.
    until: Instant,
    /// Participant user keys whose pre-image must stay readable.
    keys: Vec<Vec<u8>>,
}

/// Value proven visible at this transaction's `start_hlc`.
/// A later renew must not replace it with a value written after that HLC.
struct PinnedPreimage {
    hlc: HlcTimestamp,
    value: MvccValue,
    persisted: bool,
}

pub struct ActiveSnapshotRegistry {
    next_id: AtomicU64,
    retention: Duration,
    slots: Mutex<BTreeMap<u64, Arc<SnapshotSlot>>>,
    /// In-flight 2PC leases. A point read of a named key can return the value
    /// captured at that lease's start. Flush still persists the user key's latest value.
    lease_holds: Mutex<BTreeMap<u128, LeaseHold>>,
    /// `(txn_id, user_key)`. One transaction does not keep another's capture.
    preimages: Mutex<BTreeMap<(u128, Vec<u8>), PinnedPreimage>>,
    flush_mu: Mutex<()>,
    flush_cv: Condvar,
}

impl ActiveSnapshotRegistry {
    pub fn new(retention_ms: u64) -> Self {
        Self {
            next_id: AtomicU64::new(1),
            retention: Duration::from_millis(retention_ms.max(1)),
            slots: Mutex::new(BTreeMap::new()),
            lease_holds: Mutex::new(BTreeMap::new()),
            preimages: Mutex::new(BTreeMap::new()),
            flush_mu: Mutex::new(()),
            flush_cv: Condvar::new(),
        }
    }

    /// Keeps this transaction's capture of `keys` readable back to `retain_not_before`
    /// until `until`. A later renewal may only move the floor earlier and the deadline later.
    /// The first capture for a `(txn_id, key)` is kept. Flush still writes the latest
    /// user-key value; the capture is a separate `0x0D` row in that same batch.
    pub fn renew_lease_hold(
        &self,
        txn_id: u128,
        retain_not_before: HlcTimestamp,
        until: Instant,
        keys: &[Vec<u8>],
        pins: &[(Vec<u8>, HlcTimestamp, MvccValue)],
    ) {
        let mut holds = self.lease_holds.lock().unwrap();
        holds
            .entry(txn_id)
            .and_modify(|hold| {
                if retain_not_before < hold.retain_not_before {
                    hold.retain_not_before = retain_not_before;
                }
                if until > hold.until {
                    hold.until = until;
                }
                for key in keys {
                    if !hold.keys.iter().any(|existing| existing == key) {
                        hold.keys.push(key.clone());
                    }
                }
            })
            .or_insert(LeaseHold {
                retain_not_before,
                until,
                keys: keys.to_vec(),
            });
        drop(holds);
        let mut preimages = self.preimages.lock().unwrap();
        for (key, hlc, value) in pins {
            preimages
                .entry((txn_id, key.clone()))
                .or_insert(PinnedPreimage {
                    hlc: *hlc,
                    value: value.clone(),
                    persisted: false,
                });
        }
    }

    /// Pre-image captured for a live hold of `key` whose start is at or before `read_hlc`.
    /// The capture with the greatest HLC still visible at `read_hlc` wins.
    pub fn preimage_at(&self, key: &[u8], read_hlc: HlcTimestamp) -> Option<MvccValue> {
        let now = Instant::now();
        let holds = self.lease_holds.lock().unwrap();
        let preimages = self.preimages.lock().unwrap();
        let mut best_hlc: Option<HlcTimestamp> = None;
        let mut best_value: Option<MvccValue> = None;
        for (txn_id, hold) in holds.iter() {
            if now >= hold.until || hold.retain_not_before > read_hlc {
                continue;
            }
            if !hold.keys.iter().any(|held| held == key) {
                continue;
            }
            let Some(pin) = preimages.get(&(*txn_id, key.to_vec())) else {
                continue;
            };
            if pin.hlc > read_hlc {
                continue;
            }
            if best_hlc.map(|hlc| pin.hlc > hlc).unwrap_or(true) {
                best_hlc = Some(pin.hlc);
                best_value = Some(pin.value.clone());
            }
        }
        best_value
    }

    /// Pre-images that still belong to a live hold and have not been written yet.
    pub fn unpersisted_preimages(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let now = Instant::now();
        let holds = self.lease_holds.lock().unwrap();
        let preimages = self.preimages.lock().unwrap();
        let mut out = Vec::new();
        for ((txn_id, user_key), pin) in preimages.iter() {
            if pin.persisted {
                continue;
            }
            let Some(hold) = holds.get(txn_id) else {
                continue;
            };
            if now >= hold.until || !hold.keys.iter().any(|held| held == user_key) {
                continue;
            }
            let stored = match &pin.value {
                MvccValue::Tombstone => LeasePreimageValue::Tombstone,
                MvccValue::Value(bytes) => LeasePreimageValue::Value(bytes.clone()),
            };
            out.push((
                lease_preimage_key(*txn_id, user_key),
                encode_lease_preimage(pin.hlc, &stored),
            ));
        }
        out
    }

    pub fn mark_preimages_persisted(&self, storage_keys: &[Vec<u8>]) {
        let mut preimages = self.preimages.lock().unwrap();
        for storage_key in storage_keys {
            if storage_key.len() < 17 {
                continue;
            }
            let mut id = [0u8; 16];
            id.copy_from_slice(&storage_key[1..17]);
            let txn_id = u128::from_be_bytes(id);
            let user_key = storage_key[17..].to_vec();
            if let Some(pin) = preimages.get_mut(&(txn_id, user_key)) {
                pin.persisted = true;
            }
        }
    }

    /// Loads a pre-image that a previous flush already wrote. Does not replace a capture
    /// this process already proved.
    pub fn ingest_persisted_preimage(
        &self,
        txn_id: u128,
        user_key: Vec<u8>,
        hlc: HlcTimestamp,
        value: MvccValue,
    ) {
        self.preimages
            .lock()
            .unwrap()
            .entry((txn_id, user_key))
            .or_insert(PinnedPreimage {
                hlc,
                value,
                persisted: true,
            });
    }

    /// Drops captures whose transaction no longer has a hold.
    pub fn drop_pins_without_hold(&self) {
        let holds = self.lease_holds.lock().unwrap();
        let live: Vec<u128> = holds.keys().copied().collect();
        drop(holds);
        self.preimages
            .lock()
            .unwrap()
            .retain(|(txn_id, _), _| live.contains(txn_id));
    }

    /// Drops the hold and its captures after commit or abort.
    pub fn release_lease_hold(&self, txn_id: u128) {
        self.lease_holds.lock().unwrap().remove(&txn_id);
        self.preimages
            .lock()
            .unwrap()
            .retain(|(id, _), _| *id != txn_id);
        self.notify_flush_waiters();
    }

    /// Earliest retain-not-before among holds whose deadline has not passed.
    pub fn lease_retain_floor(&self) -> Option<HlcTimestamp> {
        let now = Instant::now();
        let mut holds = self.lease_holds.lock().unwrap();
        holds.retain(|_, hold| now < hold.until);
        holds.values().map(|hold| hold.retain_not_before).min()
    }

    /// `read_hlc` is inside an open lease that names `key`.
    pub fn lease_covers_key(&self, key: &[u8], read_hlc: HlcTimestamp) -> bool {
        let now = Instant::now();
        let holds = self.lease_holds.lock().unwrap();
        holds.values().any(|hold| {
            now < hold.until
                && read_hlc.physical_ms() >= hold.retain_not_before.physical_ms()
                && hold.keys.iter().any(|held| held == key)
        })
    }

    pub fn register(
        &self,
        cut: ReadCut,
        pinned_flushed_lsn: LogSequenceNumber,
        pinned: PinnedSnapshotData,
    ) -> Arc<SnapshotSlot> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let slot = Arc::new(SnapshotSlot {
            id,
            cut,
            pinned_flushed_lsn,
            created_at: Instant::now(),
            expired: AtomicBool::new(false),
            state: RwLock::new(Some(pinned)),
        });
        self.slots.lock().unwrap().insert(id, slot.clone());
        slot
    }

    /// Wakes writers blocked on the immutable-memtable cap.
    pub fn notify_flush_waiters(&self) {
        let _guard = self.flush_mu.lock().unwrap();
        self.flush_cv.notify_all();
    }

    /// Parks until a snapshot is dropped or `timeout` elapses.
    pub fn wait_for_flush_progress(&self, timeout: Duration) {
        let guard = self.flush_mu.lock().unwrap();
        let _ = self.flush_cv.wait_timeout(guard, timeout);
    }

    pub fn unregister(&self, id: u64) {
        let removed = self.slots.lock().unwrap().remove(&id);
        if let Some(slot) = removed {
            *slot.state.write().unwrap() = None;
            self.notify_flush_waiters();
        }
    }

    /// Marks any guard older than `retention` as `expired = true` under `slot.state.write()`,
    /// clears `Option<PinnedSnapshotData>` to free RAM/SSTables immediately, removes expired slots,
    /// and returns `(min_active_hlc, min_pinned_flushed_lsn)`.
    pub fn expire_stale_and_min_bounds(&self) -> Option<(HlcTimestamp, LogSequenceNumber)> {
        let mut guard = self.slots.lock().unwrap();
        let mut expired_ids = Vec::new();
        let mut min_hlc: Option<HlcTimestamp> = None;
        let mut min_flushed: Option<LogSequenceNumber> = None;

        for (&id, slot) in guard.iter() {
            if slot.expired.load(Ordering::Acquire) || slot.created_at.elapsed() > self.retention {
                let mut state_w = slot.state.write().unwrap();
                slot.expired.store(true, Ordering::Release);
                *state_w = None;
                expired_ids.push(id);
            } else {
                min_hlc = Some(match min_hlc {
                    Some(cur) => cur.min(slot.cut.max_hlc),
                    None => slot.cut.max_hlc,
                });
                min_flushed = Some(match min_flushed {
                    Some(cur) => cur.min(slot.pinned_flushed_lsn),
                    None => slot.pinned_flushed_lsn,
                });
            }
        }
        let expired_any = !expired_ids.is_empty();
        for id in expired_ids {
            guard.remove(&id);
        }
        drop(guard);
        if expired_any {
            self.notify_flush_waiters();
        }
        min_hlc.zip(min_flushed)
    }

    pub fn active_count(&self) -> usize {
        self.slots.lock().unwrap().len()
    }
}

/// RAII snapshot handle pinning `(ReadCut, PinnedSnapshotData)` (`Resolution B1`).
pub struct SnapshotGuard {
    slot: Arc<SnapshotSlot>,
    retention: Duration,
    oldest_retained_hlc: HlcTimestamp,
    registry: Arc<ActiveSnapshotRegistry>,
}

impl SnapshotGuard {
    #[inline]
    pub fn cut(&self) -> ReadCut {
        self.slot.cut
    }

    #[inline]
    pub fn read_hlc(&self) -> HlcTimestamp {
        self.slot.cut.max_hlc
    }

    #[inline]
    pub fn has_pinned_data(&self) -> bool {
        self.slot.has_pinned_data()
    }

    /// Acquires the short-lived per-step read lease, verifies the snapshot has not timed out,
    /// and passes `&PinnedSnapshotData` to `f`.
    pub fn with_step_lease<T>(
        &self,
        f: impl FnOnce(&PinnedSnapshotData) -> Result<T>,
    ) -> Result<T> {
        {
            let r = self.slot.state.read().unwrap();
            if !self.slot.expired.load(Ordering::Acquire)
                && self.slot.created_at.elapsed() <= self.retention
            {
                if let Some(pinned) = r.as_ref() {
                    return f(pinned);
                }
            }
        }
        let mut w = self.slot.state.write().unwrap();
        self.slot.expired.store(true, Ordering::Release);
        *w = None;
        Err(MetaDbError::TransactionTimeout {
            snapshot_hlc: self.slot.cut.max_hlc.as_u64(),
            oldest_retained_hlc: self.oldest_retained_hlc.as_u64(),
        })
    }

    /// Explicitly marks this guard expired and releases its `PinnedSnapshotData` immediately.
    pub fn force_expire_for_test(&self) {
        let mut w = self.slot.state.write().unwrap();
        self.slot.expired.store(true, Ordering::Release);
        *w = None;
    }
}

impl Drop for SnapshotGuard {
    fn drop(&mut self) {
        self.registry.unregister(self.slot.id);
    }
}

struct MemMvccInner {
    active_versions: Arc<ActiveMemTable>,
    hlc_to_lsn: VecDeque<(HlcTimestamp, LogSequenceNumber)>,
    imm_queue: VecDeque<Arc<ImmMemTable>>,
    next_seal_id: u64,
    latest_hlc: HlcTimestamp,
    latest_lsn: LogSequenceNumber,
    disk_flushed_max_hlc: HlcTimestamp,
    last_flushed_lsn: LogSequenceNumber,
    total_mvcc_writes: u64,
    total_pruned_versions: u64,
    active_bytes: usize,
}

/// Result of looking for the value of a key at `read_hlc` without trusting a
/// collapsed SSTable that may be newer than that timestamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisibleAt {
    /// A memory version with `hlc <= read_hlc` is still present.
    Memory(MvccValue),
    /// No memory version is visible, and the flush watermark is not ahead of `read_hlc`.
    DiskSafe,
    /// No memory version is visible, and the flush watermark is ahead of `read_hlc`.
    Unprovable,
}

fn memory_value_leq(guard: &MemMvccInner, key: &[u8], read_hlc: HlcTimestamp) -> Option<MvccValue> {
    let mut best: Option<(HlcTimestamp, MvccValue)> = None;
    let consider = |chain: &[MvccVersion], best: &mut Option<(HlcTimestamp, MvccValue)>| {
        for version in chain {
            if version.hlc <= read_hlc
                && best
                    .as_ref()
                    .map(|(hlc, _)| version.hlc > *hlc)
                    .unwrap_or(true)
            {
                *best = Some((version.hlc, version.value.clone()));
            }
        }
    };
    {
        let active = guard.active_versions.read().unwrap();
        if let Some(chain) = active.get(key) {
            let owned: Vec<MvccVersion> = chain.iter().cloned().collect();
            consider(&owned, &mut best);
        }
    }
    for imm in &guard.imm_queue {
        if let Some(chain) = imm.entries.get(key) {
            consider(chain, &mut best);
        }
    }
    best.map(|(_, value)| value)
}

/// In-Memory MVCC store coordinating active version chains, `ImmMemTable` queue,
/// and `ActiveSnapshotRegistry`.
pub struct MemMvccStore {
    retention_ms: u64,
    registry: Arc<ActiveSnapshotRegistry>,
    inner: RwLock<MemMvccInner>,
    flush_mutex: std::sync::Mutex<()>,
}

impl MemMvccStore {
    pub fn new(retention_ms: u64) -> Self {
        Self::with_recovered_watermarks(retention_ms, 0, HlcTimestamp::ZERO)
    }

    pub fn with_recovered_watermarks(
        retention_ms: u64,
        flushed_lsn: LogSequenceNumber,
        flushed_max_hlc: HlcTimestamp,
    ) -> Self {
        Self {
            retention_ms,
            registry: Arc::new(ActiveSnapshotRegistry::new(retention_ms)),
            inner: RwLock::new(MemMvccInner {
                active_versions: Arc::new(RwLock::new(BTreeMap::new())),
                hlc_to_lsn: VecDeque::new(),
                imm_queue: VecDeque::new(),
                next_seal_id: 1,
                latest_hlc: flushed_max_hlc,
                latest_lsn: flushed_lsn,
                disk_flushed_max_hlc: flushed_max_hlc,
                last_flushed_lsn: flushed_lsn,
                total_mvcc_writes: 0,
                total_pruned_versions: 0,
                active_bytes: 0,
            }),
            flush_mutex: std::sync::Mutex::new(()),
        }
    }

    pub fn snapshot_registry(&self) -> &Arc<ActiveSnapshotRegistry> {
        &self.registry
    }

    pub fn put(&self, key: Vec<u8>, value: Vec<u8>, hlc: HlcTimestamp, lsn: LogSequenceNumber) {
        let accounted = key.len().saturating_add(value.len()).max(1);
        let mut guard = self.inner.write().unwrap();
        guard.latest_hlc = guard.latest_hlc.max(hlc);
        guard.latest_lsn = guard.latest_lsn.max(lsn);
        guard.total_mvcc_writes += 1;
        guard.active_bytes = guard.active_bytes.saturating_add(accounted);
        guard.hlc_to_lsn.push_back((hlc, lsn));
        guard
            .active_versions
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push_back(MvccVersion {
                hlc,
                lsn,
                value: MvccValue::Value(value),
            });
    }

    pub fn delete(&self, key: Vec<u8>, hlc: HlcTimestamp, lsn: LogSequenceNumber) {
        let accounted = key.len().max(1);
        let mut guard = self.inner.write().unwrap();
        guard.latest_hlc = guard.latest_hlc.max(hlc);
        guard.latest_lsn = guard.latest_lsn.max(lsn);
        guard.total_mvcc_writes += 1;
        guard.active_bytes = guard.active_bytes.saturating_add(accounted);
        guard.hlc_to_lsn.push_back((hlc, lsn));
        guard
            .active_versions
            .write()
            .unwrap()
            .entry(key)
            .or_default()
            .push_back(MvccVersion {
                hlc,
                lsn,
                value: MvccValue::Tombstone,
            });
    }

    pub fn active_memtable_bytes(&self) -> usize {
        self.inner.read().unwrap().active_bytes
    }

    pub fn retention_ms(&self) -> u64 {
        self.retention_ms
    }

    pub fn acquire_linearizable_snapshot(
        &self,
        min_read_index: LogSequenceNumber,
        backend: &dyn StorageBackend,
    ) -> Result<SnapshotGuard> {
        let guard = self.inner.read().unwrap();
        let pinned_version = backend.current_version();
        let pinned_flushed_lsn = pinned_version.checkpoint.flushed_raft_index;
        let cut = ReadCut {
            max_raft_index: guard.latest_lsn.max(min_read_index),
            max_hlc: guard.latest_hlc,
        };
        let pinned = PinnedSnapshotData {
            active: guard.active_versions.clone(),
            imms: guard.imm_queue.iter().cloned().collect(),
            version: pinned_version,
        };
        let slot = self.registry.register(cut, pinned_flushed_lsn, pinned);
        Ok(SnapshotGuard {
            slot,
            retention: Duration::from_millis(self.retention_ms),
            oldest_retained_hlc: guard.disk_flushed_max_hlc,
            registry: self.registry.clone(),
        })
    }

    pub fn acquire_fixed_snapshot(
        &self,
        barrier_hlc: HlcTimestamp,
        backend: &dyn StorageBackend,
    ) -> Result<SnapshotGuard> {
        let guard = self.inner.read().unwrap();
        let pinned_version = backend.current_version();
        let pinned_flushed_lsn = pinned_version.checkpoint.flushed_raft_index;
        self.check_retention_window(&guard, barrier_hlc)?;
        if barrier_hlc < guard.disk_flushed_max_hlc {
            return Err(MetaDbError::TransactionTimeout {
                snapshot_hlc: barrier_hlc.as_u64(),
                oldest_retained_hlc: guard.disk_flushed_max_hlc.as_u64(),
            });
        }

        let mut max_raft_index = guard.last_flushed_lsn;
        for &(h, lsn) in &guard.hlc_to_lsn {
            if h <= barrier_hlc && lsn > max_raft_index {
                max_raft_index = lsn;
            }
        }
        let cut = ReadCut {
            max_raft_index,
            max_hlc: barrier_hlc,
        };
        let pinned = PinnedSnapshotData {
            active: guard.active_versions.clone(),
            imms: guard.imm_queue.iter().cloned().collect(),
            version: pinned_version,
        };
        let slot = self.registry.register(cut, pinned_flushed_lsn, pinned);
        Ok(SnapshotGuard {
            slot,
            retention: Duration::from_millis(self.retention_ms),
            oldest_retained_hlc: guard.disk_flushed_max_hlc,
            registry: self.registry.clone(),
        })
    }

    pub fn get_with_pinned(
        &self,
        key: &[u8],
        cut: ReadCut,
        pinned: &PinnedSnapshotData,
    ) -> Option<MvccValue> {
        {
            let guard = self.inner.read().unwrap();
            let active_map = guard.active_versions.read().unwrap();
            if let Some(chain) = active_map.get(key) {
                for v in chain.iter().rev() {
                    if v.lsn <= cut.max_raft_index && v.hlc <= cut.max_hlc {
                        return Some(v.value.clone());
                    }
                }
            }
            for imm in guard.imm_queue.iter().rev() {
                if let Some(val) = imm.get_at_cut(key, cut) {
                    return Some(val);
                }
            }
        }
        {
            let pinned_active = pinned.active.read().unwrap();
            if let Some(chain) = pinned_active.get(key) {
                for v in chain.iter().rev() {
                    if v.lsn <= cut.max_raft_index && v.hlc <= cut.max_hlc {
                        return Some(v.value.clone());
                    }
                }
            }
        }
        for imm in pinned.imms.iter().rev() {
            if let Some(val) = imm.get_at_cut(key, cut) {
                return Some(val);
            }
        }
        None
    }

    pub fn range_page_with_pinned(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        cut: ReadCut,
        pinned: &PinnedSnapshotData,
        limit: usize,
    ) -> BTreeMap<Vec<u8>, MvccValue> {
        if limit == 0 || start_key >= end_key {
            return BTreeMap::new();
        }
        let mut out: BTreeMap<Vec<u8>, MvccValue> = BTreeMap::new();

        let collect_dq_map = |map: &BTreeMap<Vec<u8>, VecDeque<MvccVersion>>,
                              out: &mut BTreeMap<Vec<u8>, MvccValue>| {
            let mut taken = 0usize;
            for (k, chain) in
                map.range::<[u8], _>((Bound::Included(start_key), Bound::Excluded(end_key)))
            {
                if out.contains_key(k) {
                    continue;
                }
                for v in chain.iter().rev() {
                    if v.lsn <= cut.max_raft_index && v.hlc <= cut.max_hlc {
                        out.insert(k.clone(), v.value.clone());
                        taken += 1;
                        break;
                    }
                }
                if taken >= limit {
                    break;
                }
            }
        };

        let collect_imm = |imm: &ImmMemTable, out: &mut BTreeMap<Vec<u8>, MvccValue>| {
            let mut taken = 0usize;
            for (k, chain) in imm
                .entries
                .range::<[u8], _>((Bound::Included(start_key), Bound::Excluded(end_key)))
            {
                if out.contains_key(k) {
                    continue;
                }
                for v in chain.iter().rev() {
                    if v.lsn <= cut.max_raft_index && v.hlc <= cut.max_hlc {
                        out.insert(k.clone(), v.value.clone());
                        taken += 1;
                        break;
                    }
                }
                if taken >= limit {
                    break;
                }
            }
        };

        {
            let guard = self.inner.read().unwrap();
            let active_map = guard.active_versions.read().unwrap();
            collect_dq_map(&active_map, &mut out);
            for imm in guard.imm_queue.iter().rev() {
                collect_imm(imm, &mut out);
            }
        }
        {
            let pinned_active = pinned.active.read().unwrap();
            collect_dq_map(&pinned_active, &mut out);
        }
        for imm in pinned.imms.iter().rev() {
            collect_imm(imm, &mut out);
        }

        while out.len() > limit {
            out.pop_last();
        }
        out
    }

    pub fn get_at(&self, key: &[u8], read_hlc: HlcTimestamp) -> Result<Option<MvccValue>> {
        let (memory, behind, disk_hlc) = {
            let guard = self.inner.read().unwrap();
            self.check_retention_window(&guard, read_hlc)?;
            let behind = read_hlc < guard.disk_flushed_max_hlc;
            let memory = memory_value_leq(&guard, key, read_hlc);
            (memory, behind, guard.disk_flushed_max_hlc)
        };
        if let Some(value) = memory {
            return Ok(Some(value));
        }
        if behind {
            // The collapsed disk value may be a write newer than `read_hlc`.
            // Only a pre-image captured at a live lease's start is safe to return.
            if let Some(value) = self.registry.preimage_at(key, read_hlc) {
                return Ok(Some(value));
            }
            return Err(MetaDbError::TransactionTimeout {
                snapshot_hlc: read_hlc.as_u64(),
                oldest_retained_hlc: disk_hlc.as_u64(),
            });
        }
        Ok(None)
    }

    /// Whether the value visible at `read_hlc` is still in memory, must be read from
    /// disk because every flushed version is `<= read_hlc`, or cannot be proved.
    pub fn classify_visible(&self, key: &[u8], read_hlc: HlcTimestamp) -> VisibleAt {
        let guard = self.inner.read().unwrap();
        if let Some(value) = memory_value_leq(&guard, key, read_hlc) {
            return VisibleAt::Memory(value);
        }
        if read_hlc < guard.disk_flushed_max_hlc {
            VisibleAt::Unprovable
        } else {
            VisibleAt::DiskSafe
        }
    }

    pub fn range_at(
        &self,
        start_key: &[u8],
        end_key: &[u8],
        read_hlc: HlcTimestamp,
    ) -> Result<BTreeMap<Vec<u8>, MvccValue>> {
        let guard = self.inner.read().unwrap();
        self.check_retention_window(&guard, read_hlc)?;
        // An unversioned SST cannot prove which keys in the range were absent,
        // deleted, or written after `read_hlc`. A lease pin for one key does not
        // make the rest of the range provable, so the scan fails closed.
        if read_hlc < guard.disk_flushed_max_hlc {
            return Err(MetaDbError::TransactionTimeout {
                snapshot_hlc: read_hlc.as_u64(),
                oldest_retained_hlc: guard.disk_flushed_max_hlc.as_u64(),
            });
        }

        let mut out = BTreeMap::new();
        let active_map = guard.active_versions.read().unwrap();
        for (k, chain) in
            active_map.range::<[u8], _>((Bound::Included(start_key), Bound::Excluded(end_key)))
        {
            for v in chain.iter().rev() {
                if v.hlc <= read_hlc {
                    out.insert(k.clone(), v.value.clone());
                    break;
                }
            }
        }
        for imm in guard.imm_queue.iter().rev() {
            for (k, chain) in imm
                .entries
                .range::<[u8], _>((Bound::Included(start_key), Bound::Excluded(end_key)))
            {
                if out.contains_key(k) {
                    continue;
                }
                for v in chain.iter().rev() {
                    if v.hlc <= read_hlc {
                        out.insert(k.clone(), v.value.clone());
                        break;
                    }
                }
            }
        }
        Ok(out)
    }

    pub fn seal_active_memtable(
        &self,
        backend: &dyn StorageBackend,
        mut checkpoint: DurableShardCheckpoint,
        hook: Option<&dyn PreFlushHook>,
    ) -> Result<Option<Arc<ImmMemTable>>> {
        let mut guard = self.inner.write().unwrap();
        let sealed_index = checkpoint.flushed_raft_index.max(guard.latest_lsn);
        let sealed_hlc = checkpoint.flushed_max_hlc.max(guard.latest_hlc);
        guard.latest_lsn = sealed_index;
        guard.latest_hlc = sealed_hlc;
        checkpoint.flushed_raft_index = sealed_index;
        checkpoint.flushed_max_hlc = sealed_hlc;

        let injected = if let Some(h) = hook {
            let active_map = guard.active_versions.read().unwrap();
            let view = ViewAtRaftIndex {
                sealed_index,
                sealing_entries: &active_map,
                imm_queue: &guard.imm_queue,
                backend,
                version: backend.current_version(),
            };
            h.on_seal_memtable(sealed_index, sealed_hlc, &view)?
        } else {
            Vec::new()
        };

        {
            let active_arc = guard.active_versions.clone();
            let mut active_map = active_arc.write().unwrap();
            for (k, hlc, val) in injected {
                guard.latest_hlc = guard.latest_hlc.max(hlc);
                guard.total_mvcc_writes += 1;
                active_map.entry(k).or_default().push_back(MvccVersion {
                    hlc,
                    lsn: sealed_index,
                    value: val,
                });
            }
        }

        if guard.active_versions.read().unwrap().is_empty() {
            guard.active_bytes = 0;
            if sealed_index == guard.last_flushed_lsn {
                return Ok(None);
            }
        }

        let seal_id = guard.next_seal_id;
        guard.next_seal_id += 1;
        let old_active = std::mem::replace(
            &mut guard.active_versions,
            Arc::new(RwLock::new(BTreeMap::new())),
        );
        guard.active_bytes = 0;
        let entries: BTreeMap<Vec<u8>, Vec<MvccVersion>> = old_active
            .read()
            .unwrap()
            .iter()
            .map(|(k, dq)| (k.clone(), dq.iter().cloned().collect()))
            .collect();

        let imm = Arc::new(ImmMemTable {
            seal_id,
            max_raft_index: sealed_index,
            max_hlc: guard.latest_hlc.max(sealed_hlc),
            checkpoint: DurableShardCheckpoint {
                flushed_max_hlc: guard.latest_hlc.max(sealed_hlc),
                ..checkpoint
            },
            entries,
        });
        guard.imm_queue.push_back(imm.clone());
        Ok(Some(imm))
    }

    pub fn flush_eligible_imms(
        &self,
        backend: &dyn StorageBackend,
        hook: Option<&dyn PreFlushHook>,
    ) -> Result<(Vec<FlushedKeyState>, LogSequenceNumber, LogSequenceNumber)> {
        let _flush_guard = self.flush_mutex.lock().unwrap();
        let mut all_flushed = Vec::new();
        let mut max_state_flushed_lsn = self.inner.read().unwrap().last_flushed_lsn;
        let mut aux_durable_frontier = 0u64;

        loop {
            let active_bounds = self.registry.expire_stale_and_min_bounds();
            let candidate = {
                let guard = self.inner.read().unwrap();
                let Some(front) = guard.imm_queue.front() else {
                    break;
                };
                if let Some((min_hlc, min_pinned_flushed_lsn)) = active_bounds {
                    if min_hlc < front.max_hlc || min_pinned_flushed_lsn < front.max_raft_index {
                        break;
                    }
                }
                front.clone()
            };

            let mut batch_to_persist: Vec<(Vec<u8>, PersistentEntry)> =
                Vec::with_capacity(candidate.entries.len());
            let mut batch_flushed_states: Vec<FlushedKeyState> =
                Vec::with_capacity(candidate.entries.len());
            let mut pruned_total = 0u64;

            for (k, chain) in &candidate.entries {
                let Some(latest) = chain.last() else {
                    continue;
                };
                let mut pruned_for_key = chain.len().saturating_sub(1);
                let latest_value = match &latest.value {
                    MvccValue::Value(v) => {
                        batch_to_persist.push((k.clone(), PersistentEntry::Value(v.clone())));
                        Some(v.clone())
                    }
                    MvccValue::Tombstone => {
                        pruned_for_key += 1;
                        batch_to_persist.push((k.clone(), PersistentEntry::PhysicalTombstone));
                        None
                    }
                };
                pruned_total += pruned_for_key as u64;
                batch_flushed_states.push(FlushedKeyState {
                    key: k.clone(),
                    lsn: latest.lsn,
                    latest_value,
                    pruned_versions: pruned_for_key,
                });
            }

            let preimage_rows = self.registry.unpersisted_preimages();
            let mut merged: BTreeMap<Vec<u8>, PersistentEntry> =
                batch_to_persist.into_iter().collect();
            let mut persisted_preimage_keys = Vec::new();
            for (key, bytes) in preimage_rows {
                if let std::collections::btree_map::Entry::Vacant(slot) = merged.entry(key.clone())
                {
                    slot.insert(PersistentEntry::Value(bytes));
                    persisted_preimage_keys.push(key);
                }
            }
            let batch_to_persist: Vec<(Vec<u8>, PersistentEntry)> = merged.into_iter().collect();
            backend.flush_l0_batch(&batch_to_persist, candidate.checkpoint.clone())?;
            self.registry
                .mark_preimages_persisted(&persisted_preimage_keys);

            {
                let mut guard = self.inner.write().unwrap();
                if guard
                    .imm_queue
                    .front()
                    .map(|f| f.seal_id == candidate.seal_id)
                    .unwrap_or(false)
                {
                    guard.imm_queue.pop_front();
                    self.registry.notify_flush_waiters();
                }
                guard.disk_flushed_max_hlc = guard.disk_flushed_max_hlc.max(candidate.max_hlc);
                guard.last_flushed_lsn = guard.last_flushed_lsn.max(candidate.max_raft_index);
                guard.latest_hlc = guard.latest_hlc.max(guard.disk_flushed_max_hlc);
                guard.latest_lsn = guard.latest_lsn.max(guard.last_flushed_lsn);
                guard.total_pruned_versions += pruned_total;
                while let Some(&(_, lsn)) = guard.hlc_to_lsn.front() {
                    if lsn <= guard.last_flushed_lsn {
                        guard.hlc_to_lsn.pop_front();
                    } else {
                        break;
                    }
                }
                max_state_flushed_lsn = guard.last_flushed_lsn;
            }

            if let Some(h) = hook {
                aux_durable_frontier =
                    h.on_flush_durable(candidate.max_raft_index, candidate.max_hlc);
            }

            all_flushed.extend(batch_flushed_states);
        }

        Ok((all_flushed, max_state_flushed_lsn, aux_durable_frontier))
    }

    pub fn imm_queue_len(&self) -> usize {
        self.inner.read().unwrap().imm_queue.len()
    }

    pub fn latest_watermarks(
        &self,
    ) -> (
        LogSequenceNumber,
        HlcTimestamp,
        LogSequenceNumber,
        HlcTimestamp,
    ) {
        let g = self.inner.read().unwrap();
        (
            g.latest_lsn,
            g.latest_hlc,
            g.last_flushed_lsn,
            g.disk_flushed_max_hlc,
        )
    }

    pub fn reset_after_snapshot_install(
        &self,
        snapshot_lsn: LogSequenceNumber,
        snapshot_hlc: HlcTimestamp,
    ) {
        let mut g = self.inner.write().unwrap();
        g.active_versions = Arc::new(RwLock::new(BTreeMap::new()));
        g.hlc_to_lsn.clear();
        g.imm_queue.clear();
        g.active_bytes = 0;
        g.latest_lsn = snapshot_lsn;
        g.latest_hlc = snapshot_hlc;
        g.last_flushed_lsn = snapshot_lsn;
        g.disk_flushed_max_hlc = snapshot_hlc;
    }

    pub fn stats(&self) -> (u64, u64) {
        let guard = self.inner.read().unwrap();
        (guard.total_mvcc_writes, guard.total_pruned_versions)
    }

    fn check_retention_window(&self, guard: &MemMvccInner, read_hlc: HlcTimestamp) -> Result<()> {
        let latest_phys = guard.latest_hlc.physical_ms();
        let mut oldest_phys = latest_phys.saturating_sub(self.retention_ms);
        // An open 2PC lease pushes the floor back to the transaction start so a
        // read or rollback at that HLC still works after the default 5s window.
        if let Some(floor) = self.registry.lease_retain_floor() {
            oldest_phys = oldest_phys.min(floor.physical_ms());
        }
        if latest_phys > oldest_phys && read_hlc.physical_ms() < oldest_phys {
            return Err(MetaDbError::TransactionTimeout {
                snapshot_hlc: read_hlc.as_u64(),
                oldest_retained_hlc: HlcTimestamp::new(oldest_phys, 0).as_u64(),
            });
        }
        Ok(())
    }
}
