//! True Single-Threaded Discrete-Event Deterministic Simulation Testing (DST) Engine for MetaDB.
//!
//! Inspired by FoundationDB's Flow simulator and TigerBeetle's VOPR, this runtime executes a
//! complete multi-node (`N = 3` or `N = 5`), multi-shard MetaDB cluster **inside a single thread**
//! driven exclusively by:
//! - **Virtual Simulated Clock (`SimClock`)**: zero wall-clock reads, zero `tokio::time::sleep`,
//!   zero real sockets, zero OS threads. Time advances strictly via a deterministic min-heap
//!   event queue (`BinaryHeap<Reverse<ScheduledEvent>>`) ordered by `(deliver_at_ms, seq_id)`.
//! - **Seeded Deterministic PRNG (`SimRng`, SplitMix64)**: every RPC latency, packet drop,
//!   reordering, duplication, asymmetric partition (`Swizzle-Clog`), node power loss, volatile
//!   page-cache loss, torn tail frame, `fsync` I/O failure, clock skew/jump, and `Buggify` trigger
//!   is a pure function of a single `u64` seed.
//! - **Real MetaDB Production State Machines**: every simulated replica hosts MetaDB's real
//!   [`TxStateMachineHandler`] (`ShardApplyHandler::apply_command`), real [`LifecycleStorageEngine`]
//!   (`MemMvccStore` + `SnapshotGuard` + `PreFlushHook`), real [`HybridLogicalClock`]
//!   (`now_with_wall_ms` / `update_checked_with_wall_ms`), real [`ShardWatermarkTracker`], real
//!   `TxRaftCmd` payloads (`CreateFile`, `SetAttr`, `Unlink`, `Prepare2pcChecked`,
//!   `CommitPrimary2pc`, `CommitSecondary2pc`, `Abort2pc`, `CheckAndAbortExpiredTxn`,
//!   `ApplyAuxBatch`, `AckAuxTasks`, `FenceShardRangeForSplit`, `UpdateShardRangeOnSplit`,
//!   `RestoreShardRange`), and real [`ThreadBuggifyGuard`] / [`CodeProbeSnapshot`].
//! - **Continuous Consensus + Linearizability + Elle + 6-Invariant Oracle**: continuously checks
//!   Raft Election Safety, Log Matching, State Machine Safety, and Committed Durability on every
//!   simulated event, then runs full-stack MetaDB invariants + Jepsen WGL Linearizability + Elle
//!   Strict-Serializability verification at the end of every seed.
//! - **100% Bit-For-Bit Reproducibility**: re-running any `(DstScenario, seed)` produces identical
//!   `(trace_hash, state_fingerprint, history_digest, final_virtual_time_ms, events_processed)`.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::fmt;
use std::sync::Arc;

use metadb_core::buggify::splitmix64;
use metadb_core::codec;
use metadb_core::{
    append_crc32c_le, buggify, code_probe, make_file_inode_id, verify_crc32c_le, BuggifyConfig,
    CodeProbeSnapshot, CompositeKeyRange, FileId, HlcTimestamp, HybridLogicalClock, InodeAttr,
    InodeId, IntentOp, MetaDbError, MetaKey, NodeId, QuotaKey, QuotaRecord, SecondaryIndexKey,
    SecondaryIndexRecord, ShardEpoch, ShardId, SpilledAuxOutboxKey, ThreadBuggifyGuard, TxnId,
    TxnRecordKey, TxnState, TxnStatusRecord, UserId, DEFAULT_MAX_CLOCK_DRIFT_MS, ROOT_INODE_ID,
    SYSTEM_PREFIX_LEASE_PREIMAGE, SYSTEM_PREFIX_SPILLED_AUX_OUTBOX, SYSTEM_PREFIX_TXN_RECORD,
    SYSTEM_PREFIX_WRITE_INTENT, TABLE_PREFIX_META, TABLE_PREFIX_QUOTA,
    TABLE_PREFIX_SECONDARY_INDEX,
};
use metadb_raft::{MetaDbRaftRequest, ShardApplyHandler, ShardWatermarkTracker};
use metadb_storage::{DurableShardCheckpoint, LifecycleStorageEngine, PersistentEntry};
use metadb_tx::{encode_proposed_command, SetAttrPatch, TxRaftCmd, TxStateMachineHandler};

use crate::jepsen::{
    check_elle_rename_history, check_register_linearizability, ElleCheckResult, HistoryRecorder,
    JepsenOpKind, JepsenOpValue, JepsenProcess, LinearizabilityResult,
};

/// Fixed deterministic virtual epoch (`2023-11-14T22:13:20Z` in milliseconds).
const VIRTUAL_EPOCH_MS: u64 = 1_700_000_000_000;
const HEARTBEAT_INTERVAL_MS: u64 = 50;
const ELECTION_TIMEOUT_MIN_MS: u64 = 180;
const ELECTION_TIMEOUT_MAX_MS: u64 = 340;
const TWO_PC_TTL_MS: u64 = 600;
const UID_POOL: [UserId; 3] = [11, 22, 33];

const SHARD_META_LEFT: ShardId = 1;
const SHARD_META_RIGHT: ShardId = 2;
const SHARD_QUOTA: ShardId = 3;
const SHARD_INDEX: ShardId = 4;
const SHARD_META_SPLIT_CHILD: ShardId = 5;

const DIR_LEFT_A: InodeId = 100;
const DIR_LEFT_B: InodeId = 300;
const DIR_RIGHT_A: InodeId = 600;

/// Deterministic simulation scenario selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DstScenario {
    /// 3-node / 5-node Multi-Raft + `ReadIndex` + `Swizzle-Clog` partitions + packet
    /// reorder/duplication + reply drops + leader crash + WGL Linearizability verification.
    RaftLinearizability,
    /// Cross-shard Primary-Anchored 2PC `Rename` + coordinator crashes mid-2PC + lease expiry
    /// recovery + per-node clock skew / forward+backward clock jumps + Elle Strict-Serializability.
    TwoPcCrashAndClockSkew,
    /// Online PID-boundary shard split (`Fence` -> child bootstrap -> `Update` / `Restore`) +
    /// `0x07` `ShardAuxOutbox` spill + `QuotaTable`/`SecondaryIndexTable` convergence + `InstallSnapshot`.
    SplitAndAuxConvergence,
    /// Aggressive `SimDisk` volatile page-cache loss + CRC32C torn tail frame corruption +
    /// injected `fsync` I/O failure rollback + snapshot compaction/log purge recovery.
    DiskFaultAndTornRecovery,
}

impl DstScenario {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::RaftLinearizability => "raft_linearizability",
            Self::TwoPcCrashAndClockSkew => "two_pc_crash_and_clock_skew",
            Self::SplitAndAuxConvergence => "split_and_aux_convergence",
            Self::DiskFaultAndTornRecovery => "disk_fault_and_torn_recovery",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "raft-linearizability" | "raft_linearizability" | "raft" => {
                Some(Self::RaftLinearizability)
            }
            "two-pc-crash-skew" | "two_pc_crash_and_clock_skew" | "2pc" => {
                Some(Self::TwoPcCrashAndClockSkew)
            }
            "split-aux-convergence" | "split_and_aux_convergence" | "split" => {
                Some(Self::SplitAndAuxConvergence)
            }
            "disk-fault-recovery" | "disk_fault_and_torn_recovery" | "disk" => {
                Some(Self::DiskFaultAndTornRecovery)
            }
            _ => None,
        }
    }

    pub fn all() -> [Self; 4] {
        [
            Self::RaftLinearizability,
            Self::TwoPcCrashAndClockSkew,
            Self::SplitAndAuxConvergence,
            Self::DiskFaultAndTornRecovery,
        ]
    }
}

/// Configuration for a single DST seed run.
#[derive(Debug, Clone)]
pub struct DstConfig {
    pub scenario: DstScenario,
    pub seed: u64,
    pub node_count: usize,
    pub workload_ops: u32,
}

impl DstConfig {
    pub fn for_scenario(scenario: DstScenario, seed: u64) -> Self {
        let node_count = if scenario == DstScenario::RaftLinearizability && seed % 2 == 1 {
            5
        } else {
            3
        };
        Self {
            scenario,
            seed,
            node_count,
            workload_ops: 36,
        }
    }
}

/// Detailed deterministic report returned by [`run_dst_seed`].
#[derive(Debug, Clone)]
pub struct DstReport {
    pub scenario: DstScenario,
    pub seed: u64,
    pub node_count: usize,
    pub events_processed: u64,
    pub final_virtual_time_ms: u64,
    pub trace_hash: u64,
    pub state_fingerprint: u64,
    pub history_digest: u64,
    pub buggify_firings: u64,
    pub raft_elections: u64,
    pub snapshots_installed: u64,
    pub packets_dropped: u64,
    pub packets_duplicated: u64,
    pub packets_reordered: u64,
    pub node_crashes: u64,
    pub disk_torn_tails_recovered: u64,
    pub fsync_errors_rolled_back: u64,
    pub clock_skew_rejections: u64,
    pub two_pc_committed: u64,
    pub two_pc_aborted: u64,
    pub splits_completed: u64,
    pub linearizability: Option<LinearizabilityResult>,
    pub elle: Option<ElleCheckResult>,
    pub probes_hit: BTreeMap<String, u64>,
    pub violations: Vec<String>,
}

impl DstReport {
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
            && self.linearizability.as_ref().is_none_or(|r| r.linearizable)
            && self.elle.as_ref().is_none_or(|r| r.valid)
    }
}

impl fmt::Display for DstReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "metadb dst scenario={} seed=0x{:x} nodes={} ok={} events={} vtime_ms={} trace=0x{:016x} state=0x{:016x} hist=0x{:016x} buggify={} elections={} snaps={} drop={} dup={} crashes={} torn={} fsync_err={} skew_rej={} 2pc_ok={} 2pc_abort={} splits={}",
            self.scenario.as_str(),
            self.seed,
            self.node_count,
            self.is_ok(),
            self.events_processed,
            self.final_virtual_time_ms,
            self.trace_hash,
            self.state_fingerprint,
            self.history_digest,
            self.buggify_firings,
            self.raft_elections,
            self.snapshots_installed,
            self.packets_dropped,
            self.packets_duplicated,
            self.node_crashes,
            self.disk_torn_tails_recovered,
            self.fsync_errors_rolled_back,
            self.clock_skew_rejections,
            self.two_pc_committed,
            self.two_pc_aborted,
            self.splits_completed
        )?;
        if let Some(lin) = &self.linearizability {
            write!(f, " wgl={}", lin.linearizable)?;
        }
        if let Some(elle) = &self.elle {
            write!(f, " elle={}", elle.valid)?;
        }
        if !self.violations.is_empty() {
            write!(f, " violations={:?}", self.violations)?;
        }
        Ok(())
    }
}

/// Ensemble report across many consecutive DST seeds.
#[derive(Debug, Clone)]
pub struct DstEnsembleReport {
    pub scenario: DstScenario,
    pub start_seed: u64,
    pub total_seeds: u32,
    pub passed_seeds: u32,
    pub failed_seeds: u32,
    pub total_events: u64,
    pub total_buggify_firings: u64,
    pub total_elections: u64,
    pub total_crashes: u64,
    pub total_snapshots: u64,
    pub total_torn_recovered: u64,
    pub total_fsync_rollbacks: u64,
    pub total_skew_rejections: u64,
    pub first_failure: Option<String>,
}

impl DstEnsembleReport {
    pub fn is_ok(&self) -> bool {
        self.failed_seeds == 0
    }
}

impl fmt::Display for DstEnsembleReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "metadb dst-ensemble scenario={} start_seed=0x{:x} seeds={} passed={} failed={} events={} buggify={} elections={} crashes={} snaps={} torn={} fsync_err={} skew_rej={}",
            self.scenario.as_str(),
            self.start_seed,
            self.total_seeds,
            self.passed_seeds,
            self.failed_seeds,
            self.total_events,
            self.total_buggify_firings,
            self.total_elections,
            self.total_crashes,
            self.total_snapshots,
            self.total_torn_recovered,
            self.total_fsync_rollbacks,
            self.total_skew_rejections
        )?;
        if let Some(fail) = &self.first_failure {
            write!(f, " first_failure=[{fail}]")?;
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Deterministic PRNG, Running Trace Hasher, and Per-Node Skewed Virtual Clock
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct SimRng {
    state: u64,
}

impl SimRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0xD57A_C0DE_1234_5678,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        splitmix64(self.state)
    }

    fn next_u128_nonzero(&mut self) -> u128 {
        let hi = self.next_u64() as u128;
        let lo = self.next_u64() as u128;
        let v = (hi << 64) | lo;
        if v == 0 {
            1
        } else {
            v
        }
    }

    fn range_u64(&mut self, lo: u64, hi_inclusive: u64) -> u64 {
        if hi_inclusive <= lo {
            return lo;
        }
        lo + (self.next_u64() % (hi_inclusive - lo + 1))
    }

    fn chance_permille(&mut self, permille: u64) -> bool {
        if permille == 0 {
            return false;
        }
        if permille >= 1000 {
            return true;
        }
        (self.next_u64() % 1000) < permille
    }
}

#[derive(Debug, Clone)]
struct TraceHasher {
    hash: u64,
}

impl TraceHasher {
    fn new(seed: u64) -> Self {
        Self {
            hash: 0xcbf2_9ce4_8422_2325 ^ splitmix64(seed),
        }
    }

    fn mix_u64(&mut self, val: u64) {
        for b in val.to_le_bytes() {
            self.hash ^= u64::from(b);
            self.hash = self.hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }

    fn mix_bytes(&mut self, bytes: &[u8]) {
        for &b in bytes {
            self.hash ^= u64::from(b);
            self.hash = self.hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
}

/// Per-node simulated physical clock with offset, drift, and step-jump injection,
/// driving MetaDB's real [`HybridLogicalClock`].
struct SimNodeClock {
    offset_ms: i64,
    drift_ppm: i64,
    hlc: Arc<HybridLogicalClock>,
}

impl SimNodeClock {
    fn new(offset_ms: i64, drift_ppm: i64) -> Self {
        Self {
            offset_ms,
            drift_ppm,
            hlc: Arc::new(HybridLogicalClock::with_max_drift_ms(
                DEFAULT_MAX_CLOCK_DRIFT_MS,
            )),
        }
    }

    fn physical_ms(&self, virtual_elapsed_ms: u64) -> u64 {
        let drift_delta =
            ((virtual_elapsed_ms as i128 * self.drift_ppm as i128) / 1_000_000) as i64;
        let signed = (VIRTUAL_EPOCH_MS as i128)
            + (virtual_elapsed_ms as i128)
            + (self.offset_ms as i128)
            + (drift_delta as i128);
        signed.max(10_000) as u64
    }

    fn now(&self, virtual_elapsed_ms: u64) -> HlcTimestamp {
        self.hlc
            .now_with_wall_ms(self.physical_ms(virtual_elapsed_ms))
    }

    fn update_checked(
        &self,
        remote: HlcTimestamp,
        virtual_elapsed_ms: u64,
    ) -> metadb_core::Result<HlcTimestamp> {
        self.hlc
            .update_checked_with_wall_ms(remote, self.physical_ms(virtual_elapsed_ms))
    }

    /// Deterministically advances the local HLC strictly past `floor` (matching production
    /// `metadb-raft`'s monotonic Raft HLC barrier across leader failover & snapshot install).
    fn advance_past(&self, floor: HlcTimestamp, virtual_elapsed_ms: u64) -> HlcTimestamp {
        if floor == HlcTimestamp::ZERO {
            return self.now(virtual_elapsed_ms);
        }
        let phys = self
            .physical_ms(virtual_elapsed_ms)
            .max(floor.physical_ms());
        self.hlc
            .update_checked_with_wall_ms(floor, phys)
            .unwrap_or_else(|_| floor.next_strict())
    }
}

// ---------------------------------------------------------------------------
// Deterministic Fault-Injecting Virtual Disk (`SimDisk`)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SimLogEntry {
    index: u64,
    term: u64,
    is_noop: bool,
    request: MetaDbRaftRequest,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SimHardState {
    current_term: u64,
    voted_for: Option<NodeId>,
    snapshot_index: u64,
    snapshot_term: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct SimSnapshotPayload {
    snapshot_index: u64,
    snapshot_term: u64,
    epoch: ShardEpoch,
    range: CompositeKeyRange,
    max_hlc: HlcTimestamp,
    contiguous_aux_acked: u64,
    /// All live persisted KV entries at `snapshot_index`.
    kvs: Vec<(Vec<u8>, Vec<u8>)>,
}

/// Fault-injecting virtual disk per `(node, shard)` modeling volatile write-back page cache vs
/// `fsync`-durable storage, CRC32C-framed log segments, torn tails, and injected `fsync` errors!
#[derive(Debug, Clone)]
struct SimDisk {
    /// CRC32C-framed durable `HARD_STATE`.
    durable_hard_state: Vec<u8>,
    /// Unflushed `HARD_STATE` waiting for `fsync`.
    unflushed_hard_state: Option<Vec<u8>>,
    /// Durable CRC32C-framed log entries (`index -> frame_with_crc32c`).
    durable_log_frames: BTreeMap<u64, Vec<u8>>,
    /// Unflushed log frames in volatile write-back cache (lost or torn on power loss!).
    unflushed_log_frames: BTreeMap<u64, Vec<u8>>,
    /// Durable CRC32C-framed snapshot bundle.
    durable_snapshot: Option<Vec<u8>>,
    /// Injected next `fsync` error counter (for testing clean Raft append rollback).
    fail_next_fsyncs: u32,
}

impl SimDisk {
    fn new(initial_epoch: ShardEpoch, initial_range: CompositeKeyRange) -> Self {
        let hs = SimHardState {
            current_term: 0,
            voted_for: None,
            snapshot_index: 0,
            snapshot_term: 0,
        };
        let mut raw = codec::serialize_serde(&hs).expect("serialize initial hard state");
        append_crc32c_le(&mut raw);
        let init_snap = SimSnapshotPayload {
            snapshot_index: 0,
            snapshot_term: 0,
            epoch: initial_epoch,
            range: initial_range,
            max_hlc: HlcTimestamp::ZERO,
            contiguous_aux_acked: 0,
            kvs: Vec::new(),
        };
        let mut snap_raw = codec::serialize_serde(&init_snap).expect("serialize initial snap");
        append_crc32c_le(&mut snap_raw);
        Self {
            durable_hard_state: raw,
            unflushed_hard_state: None,
            durable_log_frames: BTreeMap::new(),
            unflushed_log_frames: BTreeMap::new(),
            durable_snapshot: Some(snap_raw),
            fail_next_fsyncs: 0,
        }
    }

    fn write_hard_state(&mut self, hs: &SimHardState, do_fsync: bool) -> Result<(), String> {
        let mut raw = codec::serialize_serde(hs).map_err(|e| e.to_string())?;
        append_crc32c_le(&mut raw);
        self.unflushed_hard_state = Some(raw);
        if do_fsync {
            self.fsync_hard_state()?;
        }
        Ok(())
    }

    fn fsync_hard_state(&mut self) -> Result<(), String> {
        if self.fail_next_fsyncs > 0 {
            self.fail_next_fsyncs -= 1;
            self.unflushed_hard_state = None;
            return Err("SimDisk injected fsync error on HARD_STATE".to_string());
        }
        if let Some(raw) = self.unflushed_hard_state.take() {
            self.durable_hard_state = raw;
        }
        Ok(())
    }

    fn append_entries_and_sync(&mut self, entries: &[SimLogEntry]) -> Result<(), String> {
        let mut staged = Vec::with_capacity(entries.len());
        for e in entries {
            let mut raw = codec::serialize_serde(e).map_err(|err| err.to_string())?;
            append_crc32c_le(&mut raw);
            staged.push((e.index, raw));
        }
        // Stage in volatile cache first.
        for (idx, raw) in &staged {
            self.unflushed_log_frames.insert(*idx, raw.clone());
        }
        if self.fail_next_fsyncs > 0 {
            self.fail_next_fsyncs -= 1;
            // Roll back volatile cache just like `SegmentedRaftLogStore::rollback_append`!
            for (idx, _) in staged {
                self.unflushed_log_frames.remove(&idx);
            }
            return Err("SimDisk injected fsync failure on raftlog append".to_string());
        }
        // Commit volatile frames to durable disk!
        for (idx, raw) in staged {
            self.unflushed_log_frames.remove(&idx);
            self.durable_log_frames.insert(idx, raw);
        }
        Ok(())
    }

    fn truncate_suffix_from(&mut self, first_remove_index: u64) {
        self.durable_log_frames
            .retain(|&idx, _| idx < first_remove_index);
        self.unflushed_log_frames
            .retain(|&idx, _| idx < first_remove_index);
    }

    fn persist_snapshot_and_purge(
        &mut self,
        snap: &SimSnapshotPayload,
        hs: &SimHardState,
    ) -> Result<(), String> {
        let mut snap_raw = codec::serialize_serde(snap).map_err(|e| e.to_string())?;
        append_crc32c_le(&mut snap_raw);
        let mut hs_raw = codec::serialize_serde(hs).map_err(|e| e.to_string())?;
        append_crc32c_le(&mut hs_raw);
        self.durable_snapshot = Some(snap_raw);
        self.durable_hard_state = hs_raw;
        self.unflushed_hard_state = None;
        self.durable_log_frames
            .retain(|&idx, _| idx > snap.snapshot_index);
        self.unflushed_log_frames
            .retain(|&idx, _| idx > snap.snapshot_index);
        Ok(())
    }

    /// Simulates sudden power loss: drops volatile cache and optionally appends a torn/corrupted
    /// tail frame (simulating mid-write power loss on an unflushed trailing write).
    fn simulate_power_loss(&mut self, rng: &mut SimRng, inject_torn_tail: bool) {
        self.unflushed_hard_state = None;
        self.unflushed_log_frames.clear();
        if inject_torn_tail {
            let next_idx = self
                .durable_log_frames
                .keys()
                .next_back()
                .copied()
                .unwrap_or(0)
                + 1;
            let mut torn = vec![
                0xDE,
                0xAD,
                0xBE,
                0xEF,
                (rng.next_u64() & 0xFF) as u8,
                0x11,
                0x22,
            ];
            if rng.chance_permille(500) {
                // Corrupt CRC32C bitflip on a full-length synthetic tail frame.
                torn.extend_from_slice(&[0x33; 16]);
            }
            self.durable_log_frames.insert(next_idx, torn);
        }
    }

    /// Recovers `(SimHardState, SimSnapshotPayload, Vec<SimLogEntry>, torn_tails_recovered)` from
    /// durable disk frames, verifying CRC32C on every frame and cleanly discarding torn tail frames!
    fn recover(
        &mut self,
    ) -> Result<(SimHardState, SimSnapshotPayload, Vec<SimLogEntry>, u64), String> {
        let hs_body = verify_crc32c_le(&self.durable_hard_state, "SimDisk HARD_STATE")
            .map_err(|e| e.to_string())?;
        let hs: SimHardState = codec::deserialize_serde(hs_body).map_err(|e| e.to_string())?;

        let snap_raw = self
            .durable_snapshot
            .as_ref()
            .ok_or_else(|| "SimDisk missing durable snapshot".to_string())?;
        let snap_body =
            verify_crc32c_le(snap_raw, "SimDisk snapshot").map_err(|e| e.to_string())?;
        let snap: SimSnapshotPayload =
            codec::deserialize_serde(snap_body).map_err(|e| e.to_string())?;

        let mut entries = Vec::new();
        let mut torn_tails = 0u64;
        let keys: Vec<u64> = self.durable_log_frames.keys().copied().collect();
        let total = keys.len();
        let mut drop_from: Option<u64> = None;
        for (pos, idx) in keys.into_iter().enumerate() {
            let raw = &self.durable_log_frames[&idx];
            match verify_crc32c_le(raw, "SimDisk raft frame")
                .ok()
                .and_then(|body| codec::deserialize_serde::<SimLogEntry>(body).ok())
            {
                Some(entry) if entry.index == idx => {
                    if entry.index > snap.snapshot_index {
                        entries.push(entry);
                    }
                }
                _ => {
                    if pos + 1 == total {
                        // Suffix-only torn tail frame: cleanly discard & count recovery!
                        torn_tails += 1;
                        drop_from = Some(idx);
                        code_probe("probe.dst.disk_torn_tail_recovered");
                    } else {
                        return Err(format!(
                            "interior raft log corruption at index {idx} (not tail!)"
                        ));
                    }
                }
            }
        }
        if let Some(idx) = drop_from {
            self.durable_log_frames.remove(&idx);
        }
        Ok((hs, snap, entries, torn_tails))
    }
}

// ---------------------------------------------------------------------------
// Deterministic Multi-Raft Replica Hosting REAL MetaDB `TxStateMachineHandler`
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

#[derive(Debug, Clone)]
struct PendingClientWrite {
    log_index: u64,
    term: u64,
    client_op_id: u64,
}

#[derive(Debug, Clone)]
struct PendingReadIndex {
    read_id: u64,
    term: u64,
    read_index: u64,
    acks: BTreeSet<NodeId>,
    pid: InodeId,
    name: String,
    client: JepsenProcess,
    invoke_time_ns: u64,
    kind: JepsenOpKind,
}

struct SimShardReplica {
    shard_id: ShardId,
    peers: Vec<NodeId>,
    role: RaftRole,
    current_term: u64,
    voted_for: Option<NodeId>,
    votes_received: BTreeSet<NodeId>,
    leader_hint: Option<NodeId>,
    snapshot_index: u64,
    snapshot_term: u64,
    log: BTreeMap<u64, SimLogEntry>,
    commit_index: u64,
    last_applied: u64,
    /// Raft §8 no-op barrier index appended when this replica won its current term's election.
    leader_noop_index: u64,
    next_index: BTreeMap<NodeId, u64>,
    match_index: BTreeMap<NodeId, u64>,
    election_deadline_ms: u64,
    pending_writes: Vec<PendingClientWrite>,
    pending_reads: Vec<PendingReadIndex>,
    disk: SimDisk,
    watermarks: Arc<ShardWatermarkTracker>,
    engine: Arc<LifecycleStorageEngine>,
    handler: Arc<TxStateMachineHandler>,
}

impl SimShardReplica {
    fn new(
        _node_id: NodeId,
        shard_id: ShardId,
        peers: Vec<NodeId>,
        initial_epoch: ShardEpoch,
        initial_range: CompositeKeyRange,
        election_deadline_ms: u64,
    ) -> Self {
        let watermarks = Arc::new(ShardWatermarkTracker::default());
        let engine = Arc::new(LifecycleStorageEngine::new(shard_id, 60_000, 64, true));
        let handler = Arc::new(TxStateMachineHandler::with_spill_threshold(
            shard_id,
            initial_epoch,
            initial_range.clone(),
            4,
            watermarks.clone(),
        ));
        let disk = SimDisk::new(initial_epoch, initial_range);
        Self {
            shard_id,
            peers,
            role: RaftRole::Follower,
            current_term: 0,
            voted_for: None,
            votes_received: BTreeSet::new(),
            leader_hint: None,
            snapshot_index: 0,
            snapshot_term: 0,
            log: BTreeMap::new(),
            commit_index: 0,
            last_applied: 0,
            leader_noop_index: 0,
            next_index: BTreeMap::new(),
            match_index: BTreeMap::new(),
            election_deadline_ms,
            pending_writes: Vec::new(),
            pending_reads: Vec::new(),
            disk,
            watermarks,
            engine,
            handler,
        }
    }

    fn last_log_index(&self) -> u64 {
        self.log
            .keys()
            .next_back()
            .copied()
            .unwrap_or(self.snapshot_index)
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        if index == 0 {
            Some(0)
        } else if index == self.snapshot_index {
            Some(self.snapshot_term)
        } else {
            self.log.get(&index).map(|e| e.term)
        }
    }

    fn last_log_term(&self) -> u64 {
        let idx = self.last_log_index();
        self.term_at(idx).unwrap_or(0)
    }

    fn quorum_size(&self) -> usize {
        (self.peers.len() / 2) + 1
    }

    fn hard_state(&self) -> SimHardState {
        SimHardState {
            current_term: self.current_term,
            voted_for: self.voted_for,
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
        }
    }

    fn capture_snapshot_payload(&self) -> SimSnapshotPayload {
        // Flush `ReplicatedAttrAggregator` & spill pending aux tasks into engine before snapshot!
        self.handler.prepare_for_snapshot(self.last_applied);
        let snap = self
            .engine
            .acquire_linearizable_snapshot(self.last_applied)
            .expect("acquire dst snapshot");
        let mut kvs = Vec::new();
        let all = self
            .engine
            .scan_with_snapshot(&[0x00], &[0xFF], &snap)
            .expect("scan dst snapshot");
        let mut existing_spilled: BTreeSet<Vec<u8>> = BTreeSet::new();
        for (k, v) in all {
            if k.first() == Some(&SYSTEM_PREFIX_SPILLED_AUX_OUTBOX) {
                existing_spilled.insert(k.clone());
            }
            // Reconcile directory `_attr_` records so folded dirstat deltas are persisted!
            if k.first() == Some(&TABLE_PREFIX_META) {
                if let Ok(mk) = MetaKey::decode(&k) {
                    if mk.is_attr_key() {
                        if let Ok(base) = InodeAttr::decode(&v) {
                            let rec = self
                                .handler
                                .attr_aggregator()
                                .reconcile_inode_attr(mk.pid, base, &snap);
                            kvs.push((k, rec.encode()));
                            continue;
                        }
                    }
                }
            }
            kvs.push((k, v));
        }
        // Spill all unacked in-RAM `ShardAuxOutbox` entries into `[0x07]` inside the snapshot so
        // log compaction + crash/InstallSnapshot NEVER loses an unacked aux outbox entry!
        for entry in self.handler.outbox().pending_entries() {
            let sk = SpilledAuxOutboxKey {
                origin_raft_index: entry.origin_raft_index,
                task_ordinal: entry.task_ordinal,
            }
            .encode()
            .to_vec();
            if existing_spilled.insert(sk.clone()) {
                kvs.push((sk, entry.encode()));
            }
        }
        let max_hlc = self.engine.mvcc().latest_watermarks().1;
        SimSnapshotPayload {
            snapshot_index: self.last_applied,
            snapshot_term: self
                .term_at(self.last_applied)
                .unwrap_or(self.snapshot_term),
            epoch: self.handler.current_epoch(),
            range: self.handler.key_range(),
            max_hlc,
            contiguous_aux_acked: self.watermarks.contiguous_aux_acked_index(),
            kvs,
        }
    }

    fn rebuild_state_machine_from_snapshot(&mut self, snap: &SimSnapshotPayload) {
        let watermarks = Arc::new(ShardWatermarkTracker::new(
            snap.snapshot_index,
            snap.contiguous_aux_acked,
            snap.snapshot_index,
        ));
        let engine = Arc::new(LifecycleStorageEngine::new(self.shard_id, 60_000, 64, true));
        let handler = Arc::new(TxStateMachineHandler::with_spill_threshold(
            self.shard_id,
            snap.epoch,
            snap.range.clone(),
            4,
            watermarks.clone(),
        ));
        for (k, v) in &snap.kvs {
            engine.apply_committed_put(k.clone(), v.clone(), snap.max_hlc, snap.snapshot_index);
        }
        let cp = DurableShardCheckpoint {
            flushed_raft_index: snap.snapshot_index,
            flushed_max_hlc: snap.max_hlc,
            flushed_raft_term: snap.snapshot_term,
            flushed_raft_leader: 0,
            membership_bytes: Vec::new(),
            descriptor_bytes: handler.encode_descriptor_bytes(),
            contiguous_aux_acked_index: snap.contiguous_aux_acked,
        };
        handler.on_snapshot_installed(&cp, &engine);
        self.watermarks = watermarks;
        self.engine = engine;
        self.handler = handler;
        self.snapshot_index = snap.snapshot_index;
        self.snapshot_term = snap.snapshot_term;
        self.last_applied = snap.snapshot_index;
        self.commit_index = self.commit_index.max(snap.snapshot_index);
    }

    fn apply_entry_to_state_machine(
        &mut self,
        entry: &SimLogEntry,
    ) -> metadb_core::Result<Vec<u8>> {
        if entry.is_noop {
            self.last_applied = entry.index;
            return Ok(Vec::new());
        }
        self.engine.begin_command();
        let res = match self.handler.previously_committed_proposal(
            &entry.request,
            &self.engine,
            entry.request.hlc,
        ) {
            Ok(Some(prev)) => Ok(prev),
            Ok(None) => self.handler.apply_command(
                entry.index,
                entry.request.hlc,
                &entry.request,
                &self.engine,
            ),
            Err(e) => Err(e),
        };
        let _ = self.engine.end_command(res.is_ok());
        self.last_applied = entry.index;
        res
    }

    fn take_snapshot_and_compact(&mut self) -> Result<(), String> {
        if self.last_applied <= self.snapshot_index {
            return Ok(());
        }
        let snap = self.capture_snapshot_payload();
        self.snapshot_index = snap.snapshot_index;
        self.snapshot_term = snap.snapshot_term;
        let hs = self.hard_state();
        self.disk.persist_snapshot_and_purge(&snap, &hs)?;
        self.log.retain(|&idx, _| idx > self.snapshot_index);
        code_probe("probe.dst.snapshot_compacted");
        Ok(())
    }

    fn reboot_from_disk(&mut self, next_election_deadline_ms: u64) -> Result<u64, String> {
        let (hs, snap, entries, torn_recovered) = self.disk.recover()?;
        self.role = RaftRole::Follower;
        self.current_term = hs.current_term;
        self.voted_for = hs.voted_for;
        self.votes_received.clear();
        self.leader_hint = None;
        self.leader_noop_index = 0;
        self.pending_writes.clear();
        self.pending_reads.clear();
        self.next_index.clear();
        self.match_index.clear();
        self.election_deadline_ms = next_election_deadline_ms;
        self.commit_index = snap.snapshot_index;
        self.rebuild_state_machine_from_snapshot(&snap);
        self.log.clear();
        for e in entries {
            self.log.insert(e.index, e);
        }
        Ok(torn_recovered)
    }
}

// ---------------------------------------------------------------------------
// Discrete-Event Messages & Scheduler
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
enum RpcPayload {
    RequestVote {
        term: u64,
        candidate_id: NodeId,
        last_log_index: u64,
        last_log_term: u64,
    },
    RequestVoteResp {
        term: u64,
        vote_granted: bool,
    },
    AppendEntries {
        term: u64,
        leader_id: NodeId,
        prev_log_index: u64,
        prev_log_term: u64,
        entries: Vec<SimLogEntry>,
        leader_commit: u64,
        read_probe_id: Option<u64>,
    },
    AppendEntriesResp {
        term: u64,
        success: bool,
        match_index: u64,
        read_probe_id: Option<u64>,
    },
    InstallSnapshot {
        term: u64,
        leader_id: NodeId,
        snapshot_bytes_with_crc: Vec<u8>,
    },
    InstallSnapshotResp {
        term: u64,
        applied_index: u64,
    },
}

#[derive(Debug, Clone)]
enum EventPayload {
    RaftRpc {
        shard_id: ShardId,
        from: NodeId,
        to: NodeId,
        rpc: RpcPayload,
    },
    TickElection {
        node_id: NodeId,
        shard_id: ShardId,
    },
    TickHeartbeat {
        node_id: NodeId,
        shard_id: ShardId,
    },
    DriveWorkload {
        step: u32,
    },
    DriveNemesis {
        round: u32,
    },
    NodeRestart {
        node_id: NodeId,
    },
    DrainAuxOutbox,
}

#[derive(Debug, Clone)]
struct ScheduledEvent {
    deliver_at_ms: u64,
    seq: u64,
    payload: EventPayload,
}

impl PartialEq for ScheduledEvent {
    fn eq(&self, other: &Self) -> bool {
        self.deliver_at_ms == other.deliver_at_ms && self.seq == other.seq
    }
}

impl Eq for ScheduledEvent {}

impl PartialOrd for ScheduledEvent {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ScheduledEvent {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.deliver_at_ms
            .cmp(&other.deliver_at_ms)
            .then_with(|| self.seq.cmp(&other.seq))
    }
}

/// Deterministic virtual network mesh with directed link clogs, jitter, reordering & duplication.
struct SimNetwork {
    clogged_links: BTreeSet<(NodeId, NodeId)>,
    min_delay_ms: u64,
    max_delay_ms: u64,
    drop_permille: u64,
    dup_permille: u64,
    reply_drop_permille: u64,
    packets_dropped: u64,
    packets_duplicated: u64,
    packets_reordered: u64,
    last_deliver_by_link: BTreeMap<(NodeId, NodeId), u64>,
}

impl SimNetwork {
    fn new() -> Self {
        Self {
            clogged_links: BTreeSet::new(),
            min_delay_ms: 2,
            max_delay_ms: 18,
            drop_permille: 0,
            dup_permille: 0,
            reply_drop_permille: 0,
            packets_dropped: 0,
            packets_duplicated: 0,
            packets_reordered: 0,
            last_deliver_by_link: BTreeMap::new(),
        }
    }

    fn isolate_node(&mut self, victim: NodeId, all_nodes: &[NodeId]) {
        for &other in all_nodes {
            if other != victim {
                self.clogged_links.insert((victim, other));
                self.clogged_links.insert((other, victim));
            }
        }
    }

    fn partition_groups(&mut self, group_a: &[NodeId], group_b: &[NodeId]) {
        for &a in group_a {
            for &b in group_b {
                self.clogged_links.insert((a, b));
                self.clogged_links.insert((b, a));
            }
        }
    }

    fn clog_link(&mut self, from: NodeId, to: NodeId) {
        self.clogged_links.insert((from, to));
    }

    fn heal_all(&mut self) {
        self.clogged_links.clear();
    }
}

/// Client operation metadata tracked across asynchronous Raft commit & reply drops.
#[derive(Debug, Clone)]
struct ClientOpRecord {
    client: JepsenProcess,
    kind: JepsenOpKind,
    value_on_ok: JepsenOpValue,
    resolved: bool,
}

/// Authoritative model tracking committed files/dirs for end-of-seed 6-invariant verification.
#[derive(Debug, Clone, Default)]
struct DstReferenceModel {
    /// `(pid, name) -> (inode_id, fid, uid, size_bytes)`
    files: BTreeMap<(InodeId, String), (InodeId, FileId, UserId, i64)>,
    /// `dir_id -> uid`
    dirs: BTreeMap<InodeId, UserId>,
}

impl DstReferenceModel {
    fn expected_dirstat(&self, pid: InodeId) -> (i64, i64) {
        let mut count = 0i64;
        let mut bytes = 0i64;
        for ((p, _), (_, _, _, sz)) in &self.files {
            if *p == pid {
                count += 1;
                bytes += *sz;
            }
        }
        (count, bytes)
    }

    fn expected_quota(&self, uid: UserId) -> (i64, i64) {
        let mut inodes = 0i64;
        let mut bytes = 0i64;
        for &d_uid in self.dirs.values() {
            if d_uid == uid {
                inodes += 1;
            }
        }
        for (_, _, f_uid, sz) in self.files.values() {
            if *f_uid == uid {
                inodes += 1;
                bytes += *sz;
            }
        }
        (inodes, bytes)
    }
}

type PendingModelFile = (InodeId, String, InodeId, FileId, UserId, i64);

/// Single-threaded discrete-event Deterministic Simulation World.
struct SimWorld {
    cfg: DstConfig,
    now_ms: u64,
    next_event_seq: u64,
    next_op_id: u64,
    next_read_id: u64,
    next_txn_seq: u64,
    next_file_seq: u64,
    rng: SimRng,
    hasher: TraceHasher,
    queue: BinaryHeap<Reverse<ScheduledEvent>>,
    network: SimNetwork,
    node_ids: Vec<NodeId>,
    crashed_nodes: BTreeSet<NodeId>,
    clocks: BTreeMap<NodeId, SimNodeClock>,
    replicas: BTreeMap<(NodeId, ShardId), SimShardReplica>,
    active_shards: BTreeSet<ShardId>,
    split_published: bool,
    /// Safety oracle maps:
    /// `(shard_id, term) -> elected_leader` (Election Safety!)
    leaders_by_term: BTreeMap<(ShardId, u64), NodeId>,
    /// `(shard_id, raft_index) -> payload_hash` (State Machine Safety!)
    applied_payloads: BTreeMap<(ShardId, u64), u64>,
    /// `(shard_id, raft_index) -> term` of committed entries (Committed Durability!)
    committed_entries: BTreeMap<(ShardId, u64), u64>,
    /// `(shard_id, raft_index) -> Ok(()) | Err(msg)` from `TxStateMachineHandler::apply_command`.
    apply_results: BTreeMap<(ShardId, u64), Result<(), String>>,
    /// Client operation bookkeeping for Jepsen linearizability & Elle history recording.
    history: HistoryRecorder,
    client_ops: BTreeMap<u64, ClientOpRecord>,
    /// Active cross-shard 2PC pending transactions (`txn_id -> (primary_key, src_pid, dst_pid, version, expire_hlc, primary_committed, participant_keys)`).
    in_flight_2pc: Vec<ActiveTwoPc>,
    /// Prevents re-entrant `DriveWorkload` steps from firing while a synchronous multi-phase
    /// protocol driver (`execute_cross_shard_2pc_rename`, `execute_online_shard_split`,
    /// `recover_all_2pc`) is advancing virtual time via `wait_committed`.
    sync_workload_active: bool,
    /// Current token slot for Elle rename workload (`true` = `DIR_LEFT_A`, `false` = `DIR_RIGHT_A`).
    token_on_left: bool,
    token_version: u64,
    /// Strictly monotonic unique version generator for attempted 2PC renames (never reused after
    /// aborts so Elle `G1a` checks have exact unique-value semantics).
    next_token_version: u64,
    reg_initials: BTreeMap<u64, i64>,
    model: DstReferenceModel,
    /// Pending `CreateFile` model entries keyed by `(shard_id, log_index, term)`; applied to
    /// `self.model.files` inside `apply_committed_on_replica` iff that exact `(idx, term)` commits
    /// and returns `Ok` (immune to timeouts or deposed-leader overwrites).
    pending_model_files: BTreeMap<(ShardId, u64, u64), PendingModelFile>,
    applied_model_file_indices: BTreeSet<(ShardId, u64)>,
    events_processed: u64,
    raft_elections: u64,
    snapshots_installed: u64,
    node_crashes: u64,
    disk_torn_tails_recovered: u64,
    fsync_errors_rolled_back: u64,
    clock_skew_rejections: u64,
    two_pc_committed: u64,
    two_pc_aborted: u64,
    splits_completed: u64,
    violations: Vec<String>,
}

#[derive(Debug, Clone)]
struct ActiveTwoPc {
    txn_id: TxnId,
    primary_shard: ShardId,
    secondary_shard: ShardId,
    primary_key: Vec<u8>,
    secondary_key: Vec<u8>,
    expire_hlc: HlcTimestamp,
    primary_committed: bool,
    commit_hlc: Option<HlcTimestamp>,
    version: u64,
    moving_left_to_right: bool,
    client_op_id: u64,
}

impl SimWorld {
    fn new(cfg: DstConfig) -> Self {
        let mut rng = SimRng::new(cfg.seed);
        let hasher = TraceHasher::new(cfg.seed);
        let node_ids: Vec<NodeId> = (1..=(cfg.node_count as u64)).collect();
        let mut clocks = BTreeMap::new();
        for &nid in &node_ids {
            // Subtle deterministic initial skew per node (`-30..+30ms`), expanded by nemesis in 2PC scenario.
            let offset = (rng.range_u64(0, 60) as i64) - 30;
            let drift = (rng.range_u64(0, 200) as i64) - 100;
            clocks.insert(nid, SimNodeClock::new(offset, drift));
        }

        let shard_specs = [
            (
                SHARD_META_LEFT,
                CompositeKeyRange::meta_pid_range(0, 500).unwrap(),
            ),
            (
                SHARD_META_RIGHT,
                CompositeKeyRange::meta_pid_range(500, u64::MAX).unwrap(),
            ),
            (SHARD_QUOTA, CompositeKeyRange::quota_table_root()),
            (SHARD_INDEX, CompositeKeyRange::secondary_index_table_root()),
        ];

        let mut replicas = BTreeMap::new();
        let mut active_shards = BTreeSet::new();
        for (sid, range) in shard_specs {
            active_shards.insert(sid);
            for &nid in &node_ids {
                // Node 1 has slightly earlier initial deadline so initial bootstrap converges quickly.
                let dl = if nid == 1 {
                    rng.range_u64(40, 80)
                } else {
                    rng.range_u64(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS)
                };
                replicas.insert(
                    (nid, sid),
                    SimShardReplica::new(
                        nid,
                        sid,
                        node_ids.clone(),
                        ShardEpoch::initial(),
                        range.clone(),
                        dl,
                    ),
                );
            }
        }

        let mut network = SimNetwork::new();
        match cfg.scenario {
            DstScenario::RaftLinearizability => {
                network.drop_permille = 40;
                network.dup_permille = 35;
                network.reply_drop_permille = 120;
                network.max_delay_ms = 24;
            }
            DstScenario::TwoPcCrashAndClockSkew => {
                network.drop_permille = 20;
                network.dup_permille = 20;
                network.reply_drop_permille = 60;
                network.max_delay_ms = 18;
            }
            DstScenario::SplitAndAuxConvergence => {
                network.drop_permille = 25;
                network.dup_permille = 25;
                network.reply_drop_permille = 50;
                network.max_delay_ms = 20;
            }
            DstScenario::DiskFaultAndTornRecovery => {
                network.drop_permille = 30;
                network.dup_permille = 20;
                network.reply_drop_permille = 60;
                network.max_delay_ms = 16;
            }
        }

        Self {
            cfg,
            now_ms: 0,
            next_event_seq: 1,
            next_op_id: 1,
            next_read_id: 1,
            next_txn_seq: 1,
            next_file_seq: 1,
            rng,
            hasher,
            queue: BinaryHeap::new(),
            network,
            node_ids,
            crashed_nodes: BTreeSet::new(),
            clocks,
            replicas,
            active_shards,
            split_published: false,
            leaders_by_term: BTreeMap::new(),
            applied_payloads: BTreeMap::new(),
            committed_entries: BTreeMap::new(),
            apply_results: BTreeMap::new(),
            history: HistoryRecorder::new(),
            client_ops: BTreeMap::new(),
            in_flight_2pc: Vec::new(),
            sync_workload_active: false,
            token_on_left: true,
            token_version: 1,
            next_token_version: 2,
            reg_initials: BTreeMap::new(),
            model: DstReferenceModel::default(),
            pending_model_files: BTreeMap::new(),
            applied_model_file_indices: BTreeSet::new(),
            events_processed: 0,
            raft_elections: 0,
            snapshots_installed: 0,
            node_crashes: 0,
            disk_torn_tails_recovered: 0,
            fsync_errors_rolled_back: 0,
            clock_skew_rejections: 0,
            two_pc_committed: 0,
            two_pc_aborted: 0,
            splits_completed: 0,
            violations: Vec::new(),
        }
    }

    fn schedule(&mut self, deliver_at_ms: u64, payload: EventPayload) {
        let seq = self.next_event_seq;
        self.next_event_seq += 1;
        self.queue.push(Reverse(ScheduledEvent {
            deliver_at_ms: deliver_at_ms.max(self.now_ms),
            seq,
            payload,
        }));
    }

    fn send_rpc(&mut self, shard_id: ShardId, from: NodeId, to: NodeId, rpc: RpcPayload) {
        if self.crashed_nodes.contains(&from) || self.crashed_nodes.contains(&to) {
            return;
        }
        if self.network.clogged_links.contains(&(from, to)) {
            self.network.packets_dropped += 1;
            code_probe("probe.dst.network_link_clogged");
            return;
        }
        if self.rng.chance_permille(self.network.drop_permille) {
            self.network.packets_dropped += 1;
            return;
        }
        let delay = self
            .rng
            .range_u64(self.network.min_delay_ms, self.network.max_delay_ms);
        let deliver_at = self.now_ms + delay;
        if let Some(&prev_deliver) = self.network.last_deliver_by_link.get(&(from, to)) {
            if deliver_at < prev_deliver {
                self.network.packets_reordered += 1;
                code_probe("probe.dst.network_packet_reordered");
            }
        }
        self.network
            .last_deliver_by_link
            .insert((from, to), deliver_at);

        self.schedule(
            deliver_at,
            EventPayload::RaftRpc {
                shard_id,
                from,
                to,
                rpc: rpc.clone(),
            },
        );
        if self.rng.chance_permille(self.network.dup_permille) {
            self.network.packets_duplicated += 1;
            let dup_delay = delay + self.rng.range_u64(1, 20);
            self.schedule(
                self.now_ms + dup_delay,
                EventPayload::RaftRpc {
                    shard_id,
                    from,
                    to,
                    rpc,
                },
            );
            code_probe("probe.dst.network_packet_duplicated");
        }
    }

    fn route_pid_to_shard(&self, pid: InodeId) -> ShardId {
        if pid >= 500 {
            SHARD_META_RIGHT
        } else if self.split_published && pid >= 250 {
            SHARD_META_SPLIT_CHILD
        } else {
            SHARD_META_LEFT
        }
    }

    fn find_leader(&self, shard_id: ShardId) -> Option<NodeId> {
        let mut best: Option<(u64, NodeId)> = None;
        for &nid in &self.node_ids {
            if self.crashed_nodes.contains(&nid) {
                continue;
            }
            if let Some(r) = self.replicas.get(&(nid, shard_id)) {
                if r.role == RaftRole::Leader && best.is_none_or(|(t, _)| r.current_term > t) {
                    best = Some((r.current_term, nid));
                }
            }
        }
        best.map(|(_, id)| id)
    }

    /// Returns `Some(leader_id)` ONLY when the shard's current leader is alive, holds bidirectional
    /// network connectivity with a quorum of alive replicas, and has committed & applied its
    /// current-term Raft §8 no-op barrier (`last_applied >= leader_noop_index`).
    fn quorum_caught_up_leader(&self, shard_id: ShardId) -> Option<NodeId> {
        let lid = self.find_leader(shard_id)?;
        let r = self.replicas.get(&(lid, shard_id))?;
        if r.role != RaftRole::Leader
            || r.last_applied < r.leader_noop_index
            || r.last_applied < r.commit_index
        {
            return None;
        }
        let mut reachable = 0usize;
        for &p in &r.peers {
            if self.crashed_nodes.contains(&p) {
                continue;
            }
            if p == lid
                || (!self.network.clogged_links.contains(&(lid, p))
                    && !self.network.clogged_links.contains(&(p, lid)))
            {
                reachable += 1;
            }
        }
        (reachable >= r.quorum_size()).then_some(lid)
    }

    /// Performs a synchronous quorum-confirmed linearizable read of `MetaKey::child(pid, "token")`
    /// on `shard_id` at the current virtual instant (no re-entrant event stepping).
    /// Returns `None` when the shard is currently unavailable (mid-election or partitioned),
    /// and `Some(val_opt)` when a caught-up quorum leader actually observed `val_opt`.
    fn read_token_slot_linearizable(&self, shard_id: ShardId, pid: InodeId) -> Option<Option<u64>> {
        let lid = self.quorum_caught_up_leader(shard_id)?;
        let r = self.replicas.get(&(lid, shard_id))?;
        let snap = r
            .engine
            .acquire_linearizable_snapshot(r.last_applied)
            .ok()?;
        let raw = r
            .engine
            .get_with_snapshot(&MetaKey::child(pid, "token").encode(), &snap)
            .ok()?;
        Some(raw.and_then(|b| InodeAttr::decode(&b).ok().map(|a| a.size_bytes as u64)))
    }

    fn run_until(&mut self, stop_at_ms: u64, max_events: u64) {
        let mut processed_here = 0u64;
        while let Some(Reverse(ev)) = self.queue.peek() {
            if ev.deliver_at_ms > stop_at_ms || processed_here >= max_events {
                break;
            }
            let Reverse(ev) = self.queue.pop().unwrap();
            self.now_ms = ev.deliver_at_ms;
            self.events_processed += 1;
            processed_here += 1;
            self.hasher.mix_u64(self.now_ms);
            self.hasher.mix_u64(ev.seq);
            self.handle_event(ev.payload);
            if self.violations.len() >= 16 {
                break;
            }
        }
        if self.now_ms < stop_at_ms {
            self.now_ms = stop_at_ms;
        }
    }

    fn handle_event(&mut self, payload: EventPayload) {
        match payload {
            EventPayload::TickElection { node_id, shard_id } => {
                self.on_tick_election(node_id, shard_id);
            }
            EventPayload::TickHeartbeat { node_id, shard_id } => {
                self.on_tick_heartbeat(node_id, shard_id);
            }
            EventPayload::RaftRpc {
                shard_id,
                from,
                to,
                rpc,
            } => {
                self.on_raft_rpc(shard_id, from, to, rpc);
            }
            EventPayload::DriveWorkload { step } => {
                self.on_workload_step(step);
            }
            EventPayload::DriveNemesis { round } => {
                self.on_nemesis_round(round);
            }
            EventPayload::NodeRestart { node_id } => {
                self.on_node_restart(node_id);
            }
            EventPayload::DrainAuxOutbox => {
                self.drain_all_aux_outboxes();
            }
        }
    }

    fn on_tick_election(&mut self, node_id: NodeId, shard_id: ShardId) {
        if self.crashed_nodes.contains(&node_id) {
            return;
        }
        let Some(r) = self.replicas.get_mut(&(node_id, shard_id)) else {
            return;
        };
        if r.role == RaftRole::Leader {
            return;
        }
        if self.now_ms < r.election_deadline_ms {
            let dl = r.election_deadline_ms;
            self.schedule(dl, EventPayload::TickElection { node_id, shard_id });
            return;
        }
        // Start election!
        r.role = RaftRole::Candidate;
        r.current_term += 1;
        r.voted_for = Some(node_id);
        r.votes_received.clear();
        r.votes_received.insert(node_id);
        r.leader_hint = None;
        let term = r.current_term;
        let last_log_index = r.last_log_index();
        let last_log_term = r.last_log_term();
        let peers = r.peers.clone();
        let hs = r.hard_state();
        let _ = r.disk.write_hard_state(&hs, true);
        let next_dl = self.now_ms
            + self
                .rng
                .range_u64(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
        r.election_deadline_ms = next_dl;
        self.hasher.mix_u64(0xE1EC);
        self.hasher.mix_u64(node_id);
        self.hasher.mix_u64(shard_id);
        self.hasher.mix_u64(term);

        for peer in peers {
            if peer != node_id {
                self.send_rpc(
                    shard_id,
                    node_id,
                    peer,
                    RpcPayload::RequestVote {
                        term,
                        candidate_id: node_id,
                        last_log_index,
                        last_log_term,
                    },
                );
            }
        }
        self.schedule(next_dl, EventPayload::TickElection { node_id, shard_id });
    }

    fn on_tick_heartbeat(&mut self, node_id: NodeId, shard_id: ShardId) {
        if self.crashed_nodes.contains(&node_id) {
            return;
        }
        let Some(r) = self.replicas.get(&(node_id, shard_id)) else {
            return;
        };
        if r.role != RaftRole::Leader {
            return;
        }
        self.broadcast_append_entries(node_id, shard_id, None);
        self.schedule(
            self.now_ms + HEARTBEAT_INTERVAL_MS,
            EventPayload::TickHeartbeat { node_id, shard_id },
        );
    }

    fn broadcast_append_entries(
        &mut self,
        leader_id: NodeId,
        shard_id: ShardId,
        read_probe_id: Option<u64>,
    ) {
        let Some(r) = self.replicas.get(&(leader_id, shard_id)) else {
            return;
        };
        if r.role != RaftRole::Leader {
            return;
        }
        let term = r.current_term;
        let commit_index = r.commit_index;
        let snapshot_index = r.snapshot_index;
        let peers = r.peers.clone();
        let mut outbound: Vec<(NodeId, RpcPayload)> = Vec::new();
        for peer in peers {
            if peer == leader_id {
                continue;
            }
            let next_idx = r
                .next_index
                .get(&peer)
                .copied()
                .unwrap_or(r.last_log_index() + 1);
            if next_idx <= snapshot_index && snapshot_index > 0 {
                // Follower fell behind compacted log prefix -> send real CRC32C-verified `InstallSnapshot`!
                if let Some(snap_raw) = r.disk.durable_snapshot.clone() {
                    outbound.push((
                        peer,
                        RpcPayload::InstallSnapshot {
                            term,
                            leader_id,
                            snapshot_bytes_with_crc: snap_raw,
                        },
                    ));
                    continue;
                }
            }
            let prev_log_index = next_idx.saturating_sub(1);
            let prev_log_term = r.term_at(prev_log_index).unwrap_or(0);
            let entries: Vec<SimLogEntry> = r
                .log
                .range(next_idx..)
                .take(16)
                .map(|(_, e)| e.clone())
                .collect();
            outbound.push((
                peer,
                RpcPayload::AppendEntries {
                    term,
                    leader_id,
                    prev_log_index,
                    prev_log_term,
                    entries,
                    leader_commit: commit_index,
                    read_probe_id,
                },
            ));
        }
        for (peer, rpc) in outbound {
            self.send_rpc(shard_id, leader_id, peer, rpc);
        }
    }

    fn on_raft_rpc(&mut self, shard_id: ShardId, from: NodeId, to: NodeId, rpc: RpcPayload) {
        if self.crashed_nodes.contains(&to) {
            return;
        }
        if !self.replicas.contains_key(&(to, shard_id)) {
            return;
        }

        match rpc {
            RpcPayload::RequestVote {
                term,
                candidate_id,
                last_log_index,
                last_log_term,
            } => {
                let (reply_term, granted, next_dl) = {
                    let r = self.replicas.get_mut(&(to, shard_id)).unwrap();
                    if term > r.current_term {
                        r.current_term = term;
                        r.role = RaftRole::Follower;
                        r.voted_for = None;
                        r.leader_hint = None;
                    }
                    let up_to_date = last_log_term > r.last_log_term()
                        || (last_log_term == r.last_log_term()
                            && last_log_index >= r.last_log_index());
                    let can_vote = term == r.current_term
                        && (r.voted_for.is_none() || r.voted_for == Some(candidate_id))
                        && up_to_date;
                    if can_vote {
                        r.voted_for = Some(candidate_id);
                        let hs = r.hard_state();
                        let _ = r.disk.write_hard_state(&hs, true);
                        let dl = self.now_ms
                            + self
                                .rng
                                .range_u64(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
                        r.election_deadline_ms = dl;
                        (r.current_term, true, Some(dl))
                    } else {
                        (r.current_term, false, None)
                    }
                };
                if let Some(dl) = next_dl {
                    self.schedule(
                        dl,
                        EventPayload::TickElection {
                            node_id: to,
                            shard_id,
                        },
                    );
                }
                self.send_rpc(
                    shard_id,
                    to,
                    from,
                    RpcPayload::RequestVoteResp {
                        term: reply_term,
                        vote_granted: granted,
                    },
                );
            }
            RpcPayload::RequestVoteResp { term, vote_granted } => {
                let mut became_leader = false;
                let mut elected_term = 0u64;
                {
                    let r = self.replicas.get_mut(&(to, shard_id)).unwrap();
                    if term > r.current_term {
                        r.current_term = term;
                        r.role = RaftRole::Follower;
                        r.voted_for = None;
                        let hs = r.hard_state();
                        let _ = r.disk.write_hard_state(&hs, true);
                        return;
                    }
                    if r.role == RaftRole::Candidate && term == r.current_term && vote_granted {
                        r.votes_received.insert(from);
                        if r.votes_received.len() >= r.quorum_size() {
                            // Append Raft §8 leader no-op barrier in `current_term` so linearizable
                            // reads & 2PC coordinators never observe stale pre-election state!
                            let noop_idx = r.last_log_index() + 1;
                            let noop_entry = SimLogEntry {
                                index: noop_idx,
                                term: r.current_term,
                                is_noop: true,
                                request: MetaDbRaftRequest {
                                    expected_epoch: r.handler.current_epoch(),
                                    hlc: HlcTimestamp::ZERO,
                                    payload: Vec::new(),
                                },
                            };
                            if r.disk
                                .append_entries_and_sync(std::slice::from_ref(&noop_entry))
                                .is_ok()
                            {
                                r.log.insert(noop_idx, noop_entry);
                                r.role = RaftRole::Leader;
                                r.leader_hint = Some(to);
                                r.leader_noop_index = noop_idx;
                                let peers = r.peers.clone();
                                for p in peers {
                                    r.next_index.insert(p, noop_idx + 1);
                                    r.match_index.insert(p, 0);
                                }
                                r.match_index.insert(to, noop_idx);
                                became_leader = true;
                                elected_term = r.current_term;
                            } else {
                                // `fsync` error while persisting leader barrier -> step down cleanly.
                                r.role = RaftRole::Follower;
                                r.votes_received.clear();
                                self.fsync_errors_rolled_back += 1;
                                code_probe("probe.dst.fsync_error_rolled_back");
                            }
                        }
                    }
                }
                if became_leader {
                    self.raft_elections += 1;
                    // CONTINUOUS INVARIANT 1: Election Safety!
                    if let Some(&prev) = self.leaders_by_term.get(&(shard_id, elected_term)) {
                        if prev != to {
                            self.violations.push(format!(
                                "ELECTION SAFETY VIOLATION on shard={shard_id} term={elected_term}: leaders {prev} and {to}"
                            ));
                        }
                    } else {
                        self.leaders_by_term.insert((shard_id, elected_term), to);
                    }
                    code_probe("probe.dst.leader_elected");
                    self.broadcast_append_entries(to, shard_id, None);
                    self.schedule(
                        self.now_ms + HEARTBEAT_INTERVAL_MS,
                        EventPayload::TickHeartbeat {
                            node_id: to,
                            shard_id,
                        },
                    );
                }
            }
            RpcPayload::AppendEntries {
                term,
                leader_id,
                prev_log_index,
                prev_log_term,
                entries,
                leader_commit,
                read_probe_id,
            } => {
                let mut reply_success = false;
                let mut reply_match = 0u64;
                let reply_term;
                let mut new_dl = None;
                let mut fsync_failed = false;

                {
                    let r = self.replicas.get_mut(&(to, shard_id)).unwrap();
                    if term < r.current_term {
                        reply_term = r.current_term;
                    } else {
                        if term > r.current_term || r.role != RaftRole::Follower {
                            r.current_term = term;
                            r.role = RaftRole::Follower;
                            r.voted_for = None;
                            r.pending_reads.clear();
                            let hs = r.hard_state();
                            let _ = r.disk.write_hard_state(&hs, true);
                        }
                        r.leader_hint = Some(leader_id);
                        let dl = self.now_ms
                            + self
                                .rng
                                .range_u64(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
                        r.election_deadline_ms = dl;
                        new_dl = Some(dl);
                        reply_term = r.current_term;

                        // Check log matching at `prev_log_index`.
                        let prev_ok = if prev_log_index < r.snapshot_index {
                            true
                        } else {
                            r.term_at(prev_log_index) == Some(prev_log_term)
                        };

                        if prev_ok {
                            // Handle conflicting suffix truncation & new entry append via `SimDisk`.
                            let mut to_append = Vec::new();
                            let mut truncate_at: Option<u64> = None;
                            for e in &entries {
                                if e.index <= r.snapshot_index {
                                    continue;
                                }
                                if let Some(existing) = r.log.get(&e.index) {
                                    if existing.term != e.term {
                                        truncate_at = Some(e.index);
                                        to_append.push(e.clone());
                                    }
                                } else {
                                    to_append.push(e.clone());
                                }
                            }
                            if let Some(cut) = truncate_at {
                                // CONTINUOUS INVARIANT: Never truncate already-committed entries!
                                if cut <= r.commit_index {
                                    self.violations.push(format!(
                                        "LOG MATCHING VIOLATION on node={to} shard={shard_id}: attempted truncate at {cut} <= commit {}",
                                        r.commit_index
                                    ));
                                }
                                r.log.retain(|&idx, _| idx < cut);
                                r.disk.truncate_suffix_from(cut);
                            }
                            if !to_append.is_empty() {
                                if r.disk.append_entries_and_sync(&to_append).is_err() {
                                    fsync_failed = true;
                                } else {
                                    for e in to_append {
                                        r.log.insert(e.index, e);
                                    }
                                }
                            }
                            if !fsync_failed {
                                reply_success = true;
                                reply_match = entries
                                    .last()
                                    .map(|e| e.index)
                                    .unwrap_or(prev_log_index)
                                    .min(r.last_log_index());
                                if leader_commit > r.commit_index {
                                    r.commit_index = leader_commit.min(r.last_log_index());
                                }
                            }
                        }
                    }
                }
                if fsync_failed {
                    self.fsync_errors_rolled_back += 1;
                    code_probe("probe.dst.fsync_error_rolled_back");
                }
                if let Some(dl) = new_dl {
                    self.schedule(
                        dl,
                        EventPayload::TickElection {
                            node_id: to,
                            shard_id,
                        },
                    );
                }
                if reply_success {
                    self.apply_committed_on_replica(to, shard_id);
                }
                self.send_rpc(
                    shard_id,
                    to,
                    from,
                    RpcPayload::AppendEntriesResp {
                        term: reply_term,
                        success: reply_success,
                        match_index: reply_match,
                        read_probe_id,
                    },
                );
            }
            RpcPayload::AppendEntriesResp {
                term,
                success,
                match_index,
                read_probe_id,
            } => {
                let mut committed_advanced = false;
                let mut completed_reads: Vec<PendingReadIndex> = Vec::new();
                {
                    let r = self.replicas.get_mut(&(to, shard_id)).unwrap();
                    if term > r.current_term {
                        r.current_term = term;
                        r.role = RaftRole::Follower;
                        r.voted_for = None;
                        r.pending_reads.clear();
                        let hs = r.hard_state();
                        let _ = r.disk.write_hard_state(&hs, true);
                        return;
                    }
                    if r.role != RaftRole::Leader || term != r.current_term {
                        return;
                    }
                    if success {
                        let cur_match = r.match_index.get(&from).copied().unwrap_or(0);
                        let new_match = cur_match.max(match_index);
                        r.match_index.insert(from, new_match);
                        r.next_index.insert(from, new_match + 1);

                        // Advance commit_index for entries from `current_term` replicated on quorum!
                        let mut matches: Vec<u64> = r.match_index.values().copied().collect();
                        matches.sort_unstable_by(|a, b| b.cmp(a));
                        let quorum_idx = matches
                            .get(r.quorum_size().saturating_sub(1))
                            .copied()
                            .unwrap_or(0);
                        if quorum_idx > r.commit_index
                            && r.term_at(quorum_idx) == Some(r.current_term)
                        {
                            r.commit_index = quorum_idx;
                            committed_advanced = true;
                        }

                        // Credit `ReadIndex` quorum heartbeat ack!
                        if let Some(pid) = read_probe_id {
                            let quorum = r.quorum_size();
                            let last_applied = r.last_applied;
                            let mut still_waiting = Vec::new();
                            for mut pr in std::mem::take(&mut r.pending_reads) {
                                if pr.read_id == pid && pr.term == r.current_term {
                                    pr.acks.insert(from);
                                }
                                if pr.acks.len() >= quorum && last_applied >= pr.read_index {
                                    completed_reads.push(pr);
                                } else {
                                    still_waiting.push(pr);
                                }
                            }
                            r.pending_reads = still_waiting;
                        }
                    } else {
                        let next = r.next_index.get(&from).copied().unwrap_or(1);
                        r.next_index.insert(from, next.saturating_sub(1).max(1));
                    }
                }
                if committed_advanced {
                    self.record_committed_durability(to, shard_id);
                    self.apply_committed_on_replica(to, shard_id);
                    // Also check any ReadIndex waiting on `last_applied >= read_index`!
                    let extra = self.drain_ready_read_indices(to, shard_id);
                    completed_reads.extend(extra);
                }
                for pr in completed_reads {
                    self.complete_linearizable_read(to, shard_id, pr);
                }
            }
            RpcPayload::InstallSnapshot {
                term,
                leader_id,
                snapshot_bytes_with_crc,
            } => {
                let reply_term;
                let applied_idx;
                let mut installed = false;
                let mut installed_max_hlc = HlcTimestamp::ZERO;
                {
                    let r = self.replicas.get_mut(&(to, shard_id)).unwrap();
                    if term < r.current_term {
                        reply_term = r.current_term;
                        applied_idx = r.snapshot_index;
                    } else {
                        if term > r.current_term || r.role != RaftRole::Follower {
                            r.current_term = term;
                            r.role = RaftRole::Follower;
                            r.voted_for = None;
                        }
                        r.leader_hint = Some(leader_id);
                        reply_term = r.current_term;
                        if let Ok(body) =
                            verify_crc32c_le(&snapshot_bytes_with_crc, "InstallSnapshot RPC")
                        {
                            if let Ok(snap) = codec::deserialize_serde::<SimSnapshotPayload>(body) {
                                // Raft §7: NEVER install a stale/duplicated snapshot at or below
                                // `last_applied` / `commit_index` (would rewind the state machine!).
                                if snap.snapshot_index > r.last_applied
                                    && snap.snapshot_index > r.snapshot_index
                                {
                                    installed_max_hlc = snap.max_hlc;
                                    r.rebuild_state_machine_from_snapshot(&snap);
                                    r.log.retain(|&idx, _| idx > snap.snapshot_index);
                                    let hs = r.hard_state();
                                    let _ = r.disk.persist_snapshot_and_purge(&snap, &hs);
                                    installed = true;
                                }
                            }
                        }
                        applied_idx = r.last_applied;
                    }
                }
                if installed {
                    if let Some(clk) = self.clocks.get(&to) {
                        let _ = clk.advance_past(installed_max_hlc, self.now_ms);
                    }
                    self.snapshots_installed += 1;
                    code_probe("probe.dst.snapshot_installed");
                }
                self.send_rpc(
                    shard_id,
                    to,
                    from,
                    RpcPayload::InstallSnapshotResp {
                        term: reply_term,
                        applied_index: applied_idx,
                    },
                );
            }
            RpcPayload::InstallSnapshotResp {
                term,
                applied_index,
            } => {
                let r = self.replicas.get_mut(&(to, shard_id)).unwrap();
                if term > r.current_term {
                    r.current_term = term;
                    r.role = RaftRole::Follower;
                    r.voted_for = None;
                    let hs = r.hard_state();
                    let _ = r.disk.write_hard_state(&hs, true);
                    return;
                }
                if r.role == RaftRole::Leader && term == r.current_term {
                    let cur = r.match_index.get(&from).copied().unwrap_or(0);
                    let m = cur.max(applied_index);
                    r.match_index.insert(from, m);
                    r.next_index.insert(from, m + 1);
                }
            }
        }
    }

    fn record_committed_durability(&mut self, leader_id: NodeId, shard_id: ShardId) {
        let Some(r) = self.replicas.get(&(leader_id, shard_id)) else {
            return;
        };
        let commit_idx = r.commit_index;
        for (&idx, entry) in r.log.range(..=commit_idx) {
            if let Some(&prev_term) = self.committed_entries.get(&(shard_id, idx)) {
                if prev_term != entry.term {
                    self.violations.push(format!(
                        "COMMITTED DURABILITY VIOLATION on shard={shard_id} idx={idx}: term {prev_term} overwritten by {}",
                        entry.term
                    ));
                }
            } else {
                self.committed_entries.insert((shard_id, idx), entry.term);
            }
        }
    }

    fn apply_committed_on_replica(&mut self, node_id: NodeId, shard_id: ShardId) {
        type AppliedEntryRecord = (
            u64,
            u64,
            HlcTimestamp,
            metadb_core::Result<Vec<u8>>,
            Vec<u8>,
        );
        let mut newly_applied: Vec<AppliedEntryRecord> = Vec::new();
        let mut acked_client_ops: Vec<(u64, metadb_core::Result<Vec<u8>>)> = Vec::new();
        let should_compact;

        {
            let Some(r) = self.replicas.get_mut(&(node_id, shard_id)) else {
                return;
            };
            while r.last_applied < r.commit_index {
                let next_idx = r.last_applied + 1;
                let Some(entry) = r.log.get(&next_idx).cloned() else {
                    break;
                };
                let res = r.apply_entry_to_state_machine(&entry);
                newly_applied.push((
                    entry.index,
                    entry.term,
                    entry.request.hlc,
                    res.clone(),
                    entry.request.payload.clone(),
                ));

                // Resolve matching client write waiters on leader!
                let mut remaining = Vec::new();
                for pw in std::mem::take(&mut r.pending_writes) {
                    if pw.log_index == entry.index && pw.term == entry.term {
                        acked_client_ops.push((pw.client_op_id, res.clone()));
                    } else if pw.log_index > entry.index {
                        remaining.push(pw);
                    }
                }
                r.pending_writes = remaining;
            }
            should_compact = (r.last_applied.saturating_sub(r.snapshot_index) >= 12)
                || (buggify("storage.early_memtable_seal")
                    && r.last_applied.saturating_sub(r.snapshot_index) >= 4);
        }

        // CONTINUOUS INVARIANT 2: State Machine Safety!
        for (idx, entry_term, entry_hlc, res, payload) in newly_applied {
            if entry_hlc != HlcTimestamp::ZERO {
                if let Some(clk) = self.clocks.get(&node_id) {
                    let _ = clk.advance_past(entry_hlc, self.now_ms);
                }
            }
            let outcome = res.as_ref().map(|_| ()).map_err(|e| e.to_string());
            self.apply_results.entry((shard_id, idx)).or_insert(outcome);
            if res.is_ok() && self.applied_model_file_indices.insert((shard_id, idx)) {
                if let Some((pid, name, ino, fid, uid, sz)) = self
                    .pending_model_files
                    .remove(&(shard_id, idx, entry_term))
                {
                    self.model.files.insert((pid, name), (ino, fid, uid, sz));
                }
            }
            let mut h = TraceHasher::new(shard_id);
            h.mix_bytes(&payload);
            let digest = h.hash;
            if let Some(&expected) = self.applied_payloads.get(&(shard_id, idx)) {
                if expected != digest {
                    self.violations.push(format!(
                        "STATE MACHINE SAFETY VIOLATION on shard={shard_id} idx={idx}: replica node={node_id} applied divergent payload"
                    ));
                }
            } else {
                self.applied_payloads.insert((shard_id, idx), digest);
            }
            self.hasher.mix_u64(idx);
            self.hasher.mix_u64(digest);
        }

        // Complete client operations (potentially injecting post-apply reply drops -> `:info`!).
        for (op_id, res) in acked_client_ops {
            let drop_reply = self.rng.chance_permille(self.network.reply_drop_permille);
            if let Some(op) = self.client_ops.get_mut(&op_id) {
                if op.resolved {
                    continue;
                }
                op.resolved = true;
                let time_ns = self.now_ms * 1_000_000;
                match res {
                    Ok(_) if drop_reply => {
                        self.history.complete_info_at(
                            time_ns,
                            op.client,
                            op.kind.clone(),
                            "simulated post-apply reply drop",
                        );
                        code_probe("probe.dst.post_apply_reply_dropped");
                    }
                    Ok(_) => {
                        self.history.complete_ok_at(
                            time_ns,
                            op.client,
                            op.kind.clone(),
                            op.value_on_ok.clone(),
                        );
                    }
                    Err(e) => {
                        self.history.complete_fail_at(
                            time_ns,
                            op.client,
                            op.kind.clone(),
                            e.to_string(),
                        );
                    }
                }
            }
        }

        if should_compact {
            if let Some(r) = self.replicas.get_mut(&(node_id, shard_id)) {
                let _ = r.take_snapshot_and_compact();
            }
        }
    }

    fn drain_ready_read_indices(
        &mut self,
        node_id: NodeId,
        shard_id: ShardId,
    ) -> Vec<PendingReadIndex> {
        let Some(r) = self.replicas.get_mut(&(node_id, shard_id)) else {
            return Vec::new();
        };
        let quorum = r.quorum_size();
        let last_applied = r.last_applied;
        let mut ready = Vec::new();
        let mut wait = Vec::new();
        for pr in std::mem::take(&mut r.pending_reads) {
            if pr.acks.len() >= quorum && last_applied >= pr.read_index {
                ready.push(pr);
            } else {
                wait.push(pr);
            }
        }
        r.pending_reads = wait;
        ready
    }

    fn complete_linearizable_read(
        &mut self,
        node_id: NodeId,
        shard_id: ShardId,
        pr: PendingReadIndex,
    ) {
        let Some(r) = self.replicas.get(&(node_id, shard_id)) else {
            return;
        };
        let key_bytes = MetaKey::child(pr.pid, &pr.name).encode();
        let val = r
            .engine
            .acquire_linearizable_snapshot(r.last_applied)
            .ok()
            .and_then(|snap| r.engine.get_with_snapshot(&key_bytes, &snap).ok().flatten())
            .and_then(|raw| InodeAttr::decode(&raw).ok())
            .map(|a| a.size_bytes);
        let time_ns = (self.now_ms * 1_000_000).max(pr.invoke_time_ns);
        self.history
            .complete_ok_at(time_ns, pr.client, pr.kind, JepsenOpValue::OptInt(val));
    }

    /// Proposes a real `TxRaftCmd` on `shard_id`'s current leader (or specified node),
    /// persisting via `SimDisk` and replicating over `SimNetwork`.
    fn propose_tx_cmd(
        &mut self,
        shard_id: ShardId,
        cmd: &TxRaftCmd,
        caller_hlc: Option<HlcTimestamp>,
        client_op: Option<(JepsenProcess, JepsenOpKind, JepsenOpValue)>,
    ) -> Result<(NodeId, u64, u64, HlcTimestamp), String> {
        let Some(leader_id) = self.find_leader(shard_id) else {
            if let Some((proc_id, kind, _)) = client_op {
                let ts = self.now_ms * 1_000_000;
                self.history.invoke_at(ts, proc_id, kind.clone());
                self.history
                    .complete_fail_at(ts, proc_id, kind, "no leader");
            }
            return Err(format!("no leader for shard {shard_id}"));
        };

        // Drive MetaDB's REAL `HybridLogicalClock` drift protection + shard-monotonic HLC floor!
        let shard_floor_hlc = {
            let r = self.replicas.get(&(leader_id, shard_id)).unwrap();
            let mvcc_max = r.engine.mvcc().latest_watermarks().1;
            let log_max = r
                .log
                .values()
                .map(|e| e.request.hlc)
                .max()
                .unwrap_or(HlcTimestamp::ZERO);
            mvcc_max.max(log_max)
        };
        let clock = self.clocks.get(&leader_id).unwrap();
        let mut commit_hlc = match caller_hlc {
            Some(remote) => match clock.update_checked(remote, self.now_ms) {
                Ok(h) => h,
                Err(MetaDbError::ClockSkewExceeded { .. }) => {
                    self.clock_skew_rejections += 1;
                    code_probe("probe.dst.clock_skew_rejected");
                    if let Some((proc_id, kind, _)) = client_op {
                        let ts = self.now_ms * 1_000_000;
                        self.history.invoke_at(ts, proc_id, kind.clone());
                        self.history
                            .complete_fail_at(ts, proc_id, kind, "ClockSkewExceeded");
                    }
                    return Err("ClockSkewExceeded".to_string());
                }
                Err(e) => return Err(e.to_string()),
            },
            None => clock.now(self.now_ms),
        };
        if commit_hlc <= shard_floor_hlc {
            commit_hlc = clock.advance_past(shard_floor_hlc, self.now_ms);
        }

        let proposal_id = self.rng.next_u128_nonzero();
        let payload = encode_proposed_command(proposal_id, cmd).map_err(|e| e.to_string())?;
        let (epoch, next_idx, term, entry) = {
            let r = self.replicas.get(&(leader_id, shard_id)).unwrap();
            let epoch = r.handler.current_epoch();
            let next_idx = r.last_log_index() + 1;
            let term = r.current_term;
            let entry = SimLogEntry {
                index: next_idx,
                term,
                is_noop: false,
                request: MetaDbRaftRequest {
                    expected_epoch: epoch,
                    hlc: commit_hlc,
                    payload,
                },
            };
            (epoch, next_idx, term, entry)
        };
        let _ = epoch;

        // Persist on leader's `SimDisk` (subject to injected `fsync` I/O errors!).
        let append_res = {
            let r = self.replicas.get_mut(&(leader_id, shard_id)).unwrap();
            r.disk.append_entries_and_sync(std::slice::from_ref(&entry))
        };
        if let Err(e) = append_res {
            self.fsync_errors_rolled_back += 1;
            code_probe("probe.dst.fsync_error_rolled_back");
            if let Some((proc_id, kind, _)) = client_op {
                let ts = self.now_ms * 1_000_000;
                self.history.invoke_at(ts, proc_id, kind.clone());
                self.history.complete_fail_at(ts, proc_id, kind, e.clone());
            }
            return Err(e);
        }

        let op_id = if let Some((proc_id, kind, value_on_ok)) = client_op {
            let id = self.next_op_id;
            self.next_op_id += 1;
            let ts = self.now_ms * 1_000_000;
            self.history.invoke_at(ts, proc_id, kind.clone());
            self.client_ops.insert(
                id,
                ClientOpRecord {
                    client: proc_id,
                    kind,
                    value_on_ok,
                    resolved: false,
                },
            );
            Some(id)
        } else {
            None
        };

        {
            let r = self.replicas.get_mut(&(leader_id, shard_id)).unwrap();
            r.log.insert(next_idx, entry);
            r.match_index.insert(leader_id, next_idx);
            if let Some(client_op_id) = op_id {
                r.pending_writes.push(PendingClientWrite {
                    log_index: next_idx,
                    term,
                    client_op_id,
                });
            }
        }
        self.broadcast_append_entries(leader_id, shard_id, None);
        Ok((leader_id, next_idx, term, commit_hlc))
    }

    /// Initiates a real Raft `ReadIndex` linearizable read on `target_node` (which may even be a
    /// stale partitioned leader — proving a partitioned leader NEVER serves stale reads!).
    fn issue_linearizable_read(
        &mut self,
        target_node: NodeId,
        shard_id: ShardId,
        key: u64,
        pid: InodeId,
        name: String,
        client: JepsenProcess,
    ) {
        let kind = JepsenOpKind::Read { key };
        let invoke_ns = self.now_ms * 1_000_000;
        self.history.invoke_at(invoke_ns, client, kind.clone());

        if self.crashed_nodes.contains(&target_node) {
            self.history
                .complete_fail_at(invoke_ns, client, kind, "node crashed");
            return;
        }
        let Some(r) = self.replicas.get_mut(&(target_node, shard_id)) else {
            self.history
                .complete_fail_at(invoke_ns, client, kind, "unknown shard");
            return;
        };
        if r.role != RaftRole::Leader {
            self.history
                .complete_fail_at(invoke_ns, client, kind, "NotLeader");
            return;
        }
        let read_id = self.next_read_id;
        self.next_read_id += 1;
        let mut acks = BTreeSet::new();
        acks.insert(target_node);
        r.pending_reads.push(PendingReadIndex {
            read_id,
            term: r.current_term,
            read_index: r.commit_index.max(r.leader_noop_index),
            acks,
            pid,
            name,
            client,
            invoke_time_ns: invoke_ns,
            kind,
        });
        // Send quorum heartbeat probe over `SimNetwork`!
        self.broadcast_append_entries(target_node, shard_id, Some(read_id));
    }

    /// Synchronously advances virtual time until `shard_id` commits `target_index` or `budget_ms` elapses.
    fn wait_committed(&mut self, shard_id: ShardId, target_index: u64, budget_ms: u64) -> bool {
        let deadline = self.now_ms + budget_ms;
        while self.now_ms < deadline {
            if let Some(lid) = self.find_leader(shard_id) {
                if let Some(r) = self.replicas.get(&(lid, shard_id)) {
                    if r.last_applied >= target_index {
                        return true;
                    }
                }
            }
            self.run_until((self.now_ms + 20).min(deadline), 200);
        }
        false
    }

    /// Like `wait_committed`, additionally verifying that `(shard_id, target_index)` committed
    /// with the exact `expected_term` (not overwritten by a new leader's entry / NoOp!) AND that
    /// `TxStateMachineHandler::apply_command` returned `Ok`:
    /// - `Some(true)`: committed with `expected_term` and applied `Ok`
    /// - `Some(false)`: overwritten by a different term OR `apply_command` returned `Err`
    /// - `None`: timed out before `target_index` was applied on the shard leader
    fn wait_committed_ok(
        &mut self,
        shard_id: ShardId,
        target_index: u64,
        expected_term: u64,
        budget_ms: u64,
    ) -> Option<bool> {
        if !self.wait_committed(shard_id, target_index, budget_ms) {
            return None;
        }
        if let Some(&committed_term) = self.committed_entries.get(&(shard_id, target_index)) {
            if committed_term != expected_term {
                return Some(false);
            }
        }
        Some(
            self.apply_results
                .get(&(shard_id, target_index))
                .is_none_or(|r| r.is_ok()),
        )
    }

    /// Synchronously drains all spilled & in-RAM `ShardAuxOutbox` batches from meta shards to
    /// `SHARD_QUOTA` and `SHARD_INDEX` and replicates `AckAuxTasks` back!
    fn drain_all_aux_outboxes(&mut self) {
        let meta_shards: Vec<ShardId> = self
            .active_shards
            .iter()
            .copied()
            .filter(|&s| {
                s == SHARD_META_LEFT || s == SHARD_META_RIGHT || s == SHARD_META_SPLIT_CHILD
            })
            .collect();

        for sid in meta_shards {
            let Some(lid) = self.find_leader(sid) else {
                continue;
            };
            let entries = {
                let Some(r) = self.replicas.get(&(lid, sid)) else {
                    continue;
                };
                // Collect both in-memory and `[0x07]` spilled outbox entries!
                let mut pending = r.handler.outbox().pending_entries();
                if let Ok(snap) = r.engine.acquire_linearizable_snapshot(r.last_applied) {
                    if let Ok(rows) = r.engine.scan_with_snapshot(
                        &[SYSTEM_PREFIX_SPILLED_AUX_OUTBOX],
                        &SpilledAuxOutboxKey::prefix_end(),
                        &snap,
                    ) {
                        for (_, raw) in rows {
                            if let Ok(entry) = metadb_core::AuxOutboxEntry::decode(&raw) {
                                pending.push(entry);
                            }
                        }
                    }
                }
                pending
            };
            if entries.is_empty() {
                continue;
            }
            let mut quota_batch = Vec::new();
            let mut index_batch = Vec::new();
            for e in &entries {
                match e.payload {
                    metadb_core::AuxTaskPayload::QuotaDelta { .. } => quota_batch.push(e.clone()),
                    metadb_core::AuxTaskPayload::IndexUpsert { .. }
                    | metadb_core::AuxTaskPayload::IndexDelete { .. } => {
                        index_batch.push(e.clone())
                    }
                }
            }
            let mut acked_keys = Vec::new();
            if !quota_batch.is_empty() {
                let cmd = TxRaftCmd::ApplyAuxBatch {
                    entries: quota_batch.clone(),
                };
                if let Ok((_, idx, term, _)) = self.propose_tx_cmd(SHARD_QUOTA, &cmd, None, None) {
                    if self.wait_committed_ok(SHARD_QUOTA, idx, term, 600) == Some(true) {
                        for e in &quota_batch {
                            acked_keys.push((e.origin_raft_index, e.task_ordinal));
                        }
                    }
                }
            }
            if !index_batch.is_empty() {
                let cmd = TxRaftCmd::ApplyAuxBatch {
                    entries: index_batch.clone(),
                };
                if let Ok((_, idx, term, _)) = self.propose_tx_cmd(SHARD_INDEX, &cmd, None, None) {
                    if self.wait_committed_ok(SHARD_INDEX, idx, term, 600) == Some(true) {
                        for e in &index_batch {
                            acked_keys.push((e.origin_raft_index, e.task_ordinal));
                        }
                    }
                }
            }
            if !acked_keys.is_empty() {
                let ack_cmd = TxRaftCmd::AckAuxTasks { acked_keys };
                if let Ok((_, idx, term, _)) = self.propose_tx_cmd(sid, &ack_cmd, None, None) {
                    let _ = self.wait_committed_ok(sid, idx, term, 600);
                }
            }
        }
    }

    /// Executes an online PID-boundary shard split of `SHARD_META_LEFT` (`[0x01, 0..500)`) into
    /// `SHARD_META_LEFT` (`[0x01, 0..250)`) + `SHARD_META_SPLIT_CHILD` (`[0x01, 250..500)`).
    fn execute_online_shard_split(&mut self, abort_after_fence: bool) {
        if self.split_published {
            return;
        }
        let Some(lid) = self.find_leader(SHARD_META_LEFT) else {
            return;
        };
        let (orig_epoch, orig_range) = {
            let r = self.replicas.get(&(lid, SHARD_META_LEFT)).unwrap();
            (r.handler.current_epoch(), r.handler.key_range())
        };
        let left_range = CompositeKeyRange::meta_pid_range(0, 250).unwrap();
        let right_range = CompositeKeyRange::meta_pid_range(250, 500).unwrap();
        let fenced_epoch = orig_epoch.bump_version();

        // Step 1: Fence parent shard!
        let fence_cmd = TxRaftCmd::FenceShardRangeForSplit {
            new_range: left_range.clone(),
            new_epoch: fenced_epoch,
        };
        let Ok((_, fence_idx, fence_term, _)) =
            self.propose_tx_cmd(SHARD_META_LEFT, &fence_cmd, None, None)
        else {
            return;
        };
        if self.wait_committed_ok(SHARD_META_LEFT, fence_idx, fence_term, 800) != Some(true) {
            return;
        }

        if abort_after_fence {
            // Exercise `RestoreShardRange` rollback path!
            let restore_cmd = TxRaftCmd::RestoreShardRange {
                restored_range: orig_range,
                new_epoch: fenced_epoch.bump_version(),
            };
            if let Ok((_, ridx, rterm, _)) =
                self.propose_tx_cmd(SHARD_META_LEFT, &restore_cmd, None, None)
            {
                if self.wait_committed_ok(SHARD_META_LEFT, ridx, rterm, 800) == Some(true) {
                    code_probe("probe.dst.split_fence_restored");
                }
            }
            return;
        }

        // Step 2: Capture right-half keys `[250, 500)` from fenced parent & seed `SHARD_META_SPLIT_CHILD`!
        let (right_kvs, max_hlc) = {
            let Some(cur_lid) = self.find_leader(SHARD_META_LEFT) else {
                return;
            };
            let r = self.replicas.get(&(cur_lid, SHARD_META_LEFT)).unwrap();
            let snap = r
                .engine
                .acquire_linearizable_snapshot(r.last_applied)
                .expect("split snapshot");
            let raw = r
                .engine
                .scan_with_snapshot(&right_range.start_key, &right_range.end_key, &snap)
                .unwrap_or_default();
            let mut migrated = Vec::new();
            for (k, v) in raw {
                if k.first() == Some(&TABLE_PREFIX_META) {
                    if let Ok(mk) = MetaKey::decode(&k) {
                        if mk.is_attr_key() {
                            if let Ok(base) = InodeAttr::decode(&v) {
                                let mut rec = r
                                    .handler
                                    .attr_aggregator()
                                    .reconcile_inode_attr(mk.pid, base, &snap);
                                // Match `split_worker.rs`: reset `applied_lsn` to 0 on migrated
                                // directory `_attr_` records so the child shard's fresh Raft log
                                // (starting at index 1) folds all post-split deltas!
                                rec.applied_lsn = 0;
                                migrated.push((k, rec.encode()));
                                continue;
                            }
                        }
                    }
                }
                migrated.push((k, v));
            }
            (migrated, r.engine.mvcc().latest_watermarks().1)
        };

        let child_epoch = ShardEpoch::initial();
        let child_snap = SimSnapshotPayload {
            snapshot_index: 1,
            snapshot_term: 1,
            epoch: child_epoch,
            range: right_range.clone(),
            max_hlc,
            contiguous_aux_acked: 1,
            kvs: right_kvs.clone(),
        };
        self.active_shards.insert(SHARD_META_SPLIT_CHILD);
        let nids = self.node_ids.clone();
        for nid in nids {
            let dl = self.now_ms
                + if nid == 1 {
                    30
                } else {
                    self.rng
                        .range_u64(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS)
                };
            let mut rep = SimShardReplica::new(
                nid,
                SHARD_META_SPLIT_CHILD,
                self.node_ids.clone(),
                child_epoch,
                right_range.clone(),
                dl,
            );
            rep.rebuild_state_machine_from_snapshot(&child_snap);
            let hs = rep.hard_state();
            let _ = rep.disk.persist_snapshot_and_purge(&child_snap, &hs);
            self.replicas.insert((nid, SHARD_META_SPLIT_CHILD), rep);
            self.schedule(
                dl,
                EventPayload::TickElection {
                    node_id: nid,
                    shard_id: SHARD_META_SPLIT_CHILD,
                },
            );
        }

        // Wait for child shard leader election!
        for _ in 0..40 {
            if self.find_leader(SHARD_META_SPLIT_CHILD).is_some() {
                break;
            }
            self.run_until(self.now_ms + 40, 200);
        }

        // Step 3: Commit `UpdateShardRangeOnSplit` on parent shard, deleting migrated right-half keys!
        let keys_to_delete: Vec<Vec<u8>> = right_kvs.into_iter().map(|(k, _)| k).collect();
        let update_cmd = TxRaftCmd::UpdateShardRangeOnSplit {
            new_range: left_range,
            new_epoch: fenced_epoch.bump_version(),
            keys_to_delete,
            updated_anchor_attr: None,
            updated_shard_delta: None,
        };
        if let Ok((_, uidx, uterm, _)) =
            self.propose_tx_cmd(SHARD_META_LEFT, &update_cmd, None, None)
        {
            if self.wait_committed_ok(SHARD_META_LEFT, uidx, uterm, 800) == Some(true) {
                self.split_published = true;
                self.splits_completed += 1;
                code_probe("probe.dst.online_split_completed");
            }
        }
    }

    /// Executes a real Primary-Anchored Cross-Shard 2PC Rename moving `token` between
    /// `DIR_LEFT_A` (`SHARD_META_LEFT`) and `DIR_RIGHT_A` (`SHARD_META_RIGHT`), with optional
    /// simulated coordinator crashes mid-2PC!
    fn execute_cross_shard_2pc_rename(&mut self, crash_stage: u8) {
        let (src_pid, dst_pid, src_shard, dst_shard, moving_left_to_right) = if self.token_on_left {
            (
                DIR_LEFT_A,
                DIR_RIGHT_A,
                SHARD_META_LEFT,
                SHARD_META_RIGHT,
                true,
            )
        } else {
            (
                DIR_RIGHT_A,
                DIR_LEFT_A,
                SHARD_META_RIGHT,
                SHARD_META_LEFT,
                false,
            )
        };
        let src_key = MetaKey::child(src_pid, "token").encode();
        let dst_key = MetaKey::child(dst_pid, "token").encode();

        // Read current token entry on source shard via a quorum-confirmed caught-up leader.
        let Some(src_lid) = self.quorum_caught_up_leader(src_shard) else {
            return;
        };
        let Some(src_raw) = self.replicas.get(&(src_lid, src_shard)).and_then(|r| {
            let snap = r
                .engine
                .acquire_linearizable_snapshot(r.last_applied)
                .ok()?;
            r.engine.get_with_snapshot(&src_key, &snap).ok().flatten()
        }) else {
            return;
        };
        let Ok(mut attr) = InodeAttr::decode(&src_raw) else {
            return;
        };

        let next_ver = self.next_token_version;
        self.next_token_version += 1;
        let txn_seq = self.next_txn_seq;
        self.next_txn_seq += 1;
        let txn_id: TxnId = (1u128 << 64) | (txn_seq as u128);
        let client_proc = JepsenProcess::Client(txn_seq as u32);
        let op_kind = JepsenOpKind::TxnRename {
            txn_id: txn_seq,
            src_slot: src_pid,
            dst_slot: dst_pid,
            version: next_ver,
        };
        let op_id = self.next_op_id;
        self.next_op_id += 1;
        let invoke_ns = self.now_ms * 1_000_000;
        self.history
            .invoke_at(invoke_ns, client_proc, op_kind.clone());

        let old_size = attr.size_bytes;
        attr.parent_pid = dst_pid;
        attr.size_bytes = next_ver as i64;
        let dst_bytes = attr.encode();
        let start_hlc = self.clocks[&src_lid].now(self.now_ms);
        let expire_hlc = HlcTimestamp::new(start_hlc.physical_ms() + TWO_PC_TTL_MS, 0);

        let all_keys = vec![src_key.clone(), dst_key.clone()];
        let src_intent = IntentOp::Delete {
            attr_deltas: vec![metadb_core::AttrDeltaSpec {
                pid: src_pid,
                delta_children: -1,
                delta_bytes: -old_size,
            }],
            aux_specs: vec![],
        };
        let dst_intent = IntentOp::Put {
            value: dst_bytes,
            attr_deltas: vec![metadb_core::AttrDeltaSpec {
                pid: dst_pid,
                delta_children: 1,
                delta_bytes: next_ver as i64,
            }],
            aux_specs: vec![
                metadb_core::AuxIntentSpec::QuotaDelta {
                    uid: attr.uid,
                    delta_inodes: 0,
                    delta_bytes: (next_ver as i64) - old_size,
                },
                metadb_core::AuxIntentSpec::IndexUpsert {
                    fid: attr.fid.unwrap_or(20_001),
                    pid: dst_pid,
                    name: "token".to_string(),
                    create_time: attr.create_time,
                },
            ],
        };

        // Phase 1a: Prepare Secondary (`dst_shard`).
        let prep_sec = TxRaftCmd::Prepare2pcChecked {
            txn_id,
            is_primary: false,
            primary_key: src_key.clone(),
            participant_keys: all_keys.clone(),
            start_hlc,
            expire_hlc,
            rename_fence_token: None,
            intents: vec![(dst_key.clone(), dst_intent)],
            expected_values: vec![(dst_key.clone(), None)],
        };
        let Ok((_, sec_idx, sec_term, _)) =
            self.propose_tx_cmd(dst_shard, &prep_sec, Some(start_hlc), None)
        else {
            self.history.complete_fail_at(
                self.now_ms * 1_000_000,
                client_proc,
                op_kind,
                "prepare secondary failed",
            );
            return;
        };
        match self.wait_committed_ok(dst_shard, sec_idx, sec_term, 500) {
            Some(true) => {}
            Some(false) => {
                self.history.complete_fail_at(
                    self.now_ms * 1_000_000,
                    client_proc,
                    op_kind,
                    "prepare secondary rejected by state machine",
                );
                return;
            }
            None => {
                self.in_flight_2pc.push(ActiveTwoPc {
                    txn_id,
                    primary_shard: src_shard,
                    secondary_shard: dst_shard,
                    primary_key: src_key,
                    secondary_key: dst_key,
                    expire_hlc,
                    primary_committed: false,
                    commit_hlc: None,
                    version: next_ver,
                    moving_left_to_right,
                    client_op_id: op_id,
                });
                self.history.complete_info_at(
                    self.now_ms * 1_000_000,
                    client_proc,
                    op_kind,
                    "prepare secondary timeout",
                );
                return;
            }
        }

        // Phase 1b: Prepare Primary (`src_shard`).
        let prep_pri = TxRaftCmd::Prepare2pcChecked {
            txn_id,
            is_primary: true,
            primary_key: src_key.clone(),
            participant_keys: all_keys,
            start_hlc,
            expire_hlc,
            rename_fence_token: None,
            intents: vec![(src_key.clone(), src_intent)],
            expected_values: vec![(src_key.clone(), Some(src_raw))],
        };
        let Ok((_, pri_idx, pri_term, pri_hlc)) =
            self.propose_tx_cmd(src_shard, &prep_pri, Some(start_hlc), None)
        else {
            // Abort secondary (or defer to recovery if dst_shard is partitioned)!
            let abort_sec = TxRaftCmd::Abort2pc {
                txn_id,
                target_user_keys: vec![dst_key.clone()],
            };
            let cleaned = self
                .propose_tx_cmd(dst_shard, &abort_sec, None, None)
                .is_ok_and(|(_, idx, t, _)| {
                    self.wait_committed_ok(dst_shard, idx, t, 500) == Some(true)
                });
            if !cleaned {
                self.in_flight_2pc.push(ActiveTwoPc {
                    txn_id,
                    primary_shard: src_shard,
                    secondary_shard: dst_shard,
                    primary_key: src_key,
                    secondary_key: dst_key,
                    expire_hlc,
                    primary_committed: false,
                    commit_hlc: None,
                    version: next_ver,
                    moving_left_to_right,
                    client_op_id: op_id,
                });
            }
            self.history.complete_fail_at(
                self.now_ms * 1_000_000,
                client_proc,
                op_kind,
                "prepare primary failed",
            );
            return;
        };
        match self.wait_committed_ok(src_shard, pri_idx, pri_term, 500) {
            Some(true) => {}
            Some(false) => {
                let abort_sec = TxRaftCmd::Abort2pc {
                    txn_id,
                    target_user_keys: vec![dst_key.clone()],
                };
                let cleaned = self
                    .propose_tx_cmd(dst_shard, &abort_sec, None, None)
                    .is_ok_and(|(_, idx, t, _)| {
                        self.wait_committed_ok(dst_shard, idx, t, 500) == Some(true)
                    });
                if !cleaned {
                    self.in_flight_2pc.push(ActiveTwoPc {
                        txn_id,
                        primary_shard: src_shard,
                        secondary_shard: dst_shard,
                        primary_key: src_key,
                        secondary_key: dst_key,
                        expire_hlc,
                        primary_committed: false,
                        commit_hlc: None,
                        version: next_ver,
                        moving_left_to_right,
                        client_op_id: op_id,
                    });
                }
                self.history.complete_fail_at(
                    self.now_ms * 1_000_000,
                    client_proc,
                    op_kind,
                    "prepare primary rejected by state machine",
                );
                return;
            }
            None => {
                self.in_flight_2pc.push(ActiveTwoPc {
                    txn_id,
                    primary_shard: src_shard,
                    secondary_shard: dst_shard,
                    primary_key: src_key,
                    secondary_key: dst_key,
                    expire_hlc,
                    primary_committed: false,
                    commit_hlc: None,
                    version: next_ver,
                    moving_left_to_right,
                    client_op_id: op_id,
                });
                self.history.complete_info_at(
                    self.now_ms * 1_000_000,
                    client_proc,
                    op_kind,
                    "prepare primary timeout",
                );
                return;
            }
        }

        if crash_stage == 1 {
            // Simulated Coordinator Crash AFTER Prepare BEFORE CommitPrimary!
            // Leave intents in place for recovery worker to abort after `expire_hlc`!
            code_probe("probe.dst.two_pc_crash_before_commit");
            self.in_flight_2pc.push(ActiveTwoPc {
                txn_id,
                primary_shard: src_shard,
                secondary_shard: dst_shard,
                primary_key: src_key,
                secondary_key: dst_key,
                expire_hlc,
                primary_committed: false,
                commit_hlc: None,
                version: next_ver,
                moving_left_to_right,
                client_op_id: op_id,
            });
            self.history.complete_info_at(
                self.now_ms * 1_000_000,
                client_proc,
                op_kind,
                "coordinator crashed after prepare",
            );
            return;
        }

        // Phase 2a: CommitPrimary2pc!
        let commit_hlc = pri_hlc.next_strict();
        let commit_pri = TxRaftCmd::CommitPrimary2pc {
            txn_id,
            commit_hlc,
            rename_fence_token: None,
        };
        let Ok((_, cidx, cterm, _)) =
            self.propose_tx_cmd(src_shard, &commit_pri, Some(commit_hlc), None)
        else {
            self.in_flight_2pc.push(ActiveTwoPc {
                txn_id,
                primary_shard: src_shard,
                secondary_shard: dst_shard,
                primary_key: src_key,
                secondary_key: dst_key,
                expire_hlc,
                primary_committed: false,
                commit_hlc: None,
                version: next_ver,
                moving_left_to_right,
                client_op_id: op_id,
            });
            self.history.complete_info_at(
                self.now_ms * 1_000_000,
                client_proc,
                op_kind,
                "commit primary failed",
            );
            return;
        };
        match self.wait_committed_ok(src_shard, cidx, cterm, 500) {
            Some(true) => {}
            Some(false) => {
                // Primary's Phase 1b `0x05` / `0x06` records are still present: hand off to
                // recovery worker so both shards abort & GC cleanly once expired!
                self.in_flight_2pc.push(ActiveTwoPc {
                    txn_id,
                    primary_shard: src_shard,
                    secondary_shard: dst_shard,
                    primary_key: src_key,
                    secondary_key: dst_key,
                    expire_hlc,
                    primary_committed: false,
                    commit_hlc: None,
                    version: next_ver,
                    moving_left_to_right,
                    client_op_id: op_id,
                });
                self.history.complete_fail_at(
                    self.now_ms * 1_000_000,
                    client_proc,
                    op_kind,
                    "commit primary rejected or deposed",
                );
                return;
            }
            None => {
                self.in_flight_2pc.push(ActiveTwoPc {
                    txn_id,
                    primary_shard: src_shard,
                    secondary_shard: dst_shard,
                    primary_key: src_key,
                    secondary_key: dst_key,
                    expire_hlc,
                    primary_committed: false,
                    commit_hlc: None,
                    version: next_ver,
                    moving_left_to_right,
                    client_op_id: op_id,
                });
                self.history.complete_info_at(
                    self.now_ms * 1_000_000,
                    client_proc,
                    op_kind,
                    "commit primary timeout",
                );
                return;
            }
        }

        if crash_stage == 2 {
            // Simulated Coordinator Crash AFTER CommitPrimary BEFORE CommitSecondary!
            // Recovery worker MUST roll forward the secondary shard!
            code_probe("probe.dst.two_pc_crash_after_primary_commit");
            self.token_on_left = !moving_left_to_right;
            self.token_version = next_ver;
            self.update_model_for_token_rename(src_pid, dst_pid, next_ver);
            self.in_flight_2pc.push(ActiveTwoPc {
                txn_id,
                primary_shard: src_shard,
                secondary_shard: dst_shard,
                primary_key: src_key,
                secondary_key: dst_key,
                expire_hlc,
                primary_committed: true,
                commit_hlc: Some(commit_hlc),
                version: next_ver,
                moving_left_to_right,
                client_op_id: op_id,
            });
            self.history.complete_info_at(
                self.now_ms * 1_000_000,
                client_proc,
                op_kind,
                "coordinator crashed after primary commit",
            );
            return;
        }

        // Phase 2b: CommitSecondary2pc + MarkParticipantAcked!
        let commit_sec = TxRaftCmd::CommitSecondary2pc {
            txn_id,
            commit_hlc,
            target_user_keys: vec![dst_key.clone()],
        };
        if let Ok((_, sidx, sterm, _)) = self.propose_tx_cmd(dst_shard, &commit_sec, None, None) {
            if self.wait_committed_ok(dst_shard, sidx, sterm, 500) == Some(true) {
                let ack = TxRaftCmd::MarkParticipantAcked {
                    txn_id,
                    acked_user_keys: vec![src_key.clone(), dst_key.clone()],
                };
                let ack_ok = self.propose_tx_cmd(src_shard, &ack, None, None).is_ok_and(
                    |(_, aidx, aterm, _)| {
                        self.wait_committed_ok(src_shard, aidx, aterm, 500) == Some(true)
                    },
                );
                self.token_on_left = !moving_left_to_right;
                self.token_version = next_ver;
                self.update_model_for_token_rename(src_pid, dst_pid, next_ver);
                if ack_ok {
                    self.two_pc_committed += 1;
                    self.history.complete_ok_at(
                        self.now_ms * 1_000_000,
                        client_proc,
                        op_kind,
                        JepsenOpValue::RenameCommitted { version: next_ver },
                    );
                    return;
                }
                // Secondary materialized, but primary `0x06` ack timed out / deposed: let recovery
                // finish `MarkParticipantAcked` cleanly!
                self.in_flight_2pc.push(ActiveTwoPc {
                    txn_id,
                    primary_shard: src_shard,
                    secondary_shard: dst_shard,
                    primary_key: src_key,
                    secondary_key: dst_key,
                    expire_hlc,
                    primary_committed: true,
                    commit_hlc: Some(commit_hlc),
                    version: next_ver,
                    moving_left_to_right,
                    client_op_id: op_id,
                });
                self.history.complete_ok_at(
                    self.now_ms * 1_000_000,
                    client_proc,
                    op_kind,
                    JepsenOpValue::RenameCommitted { version: next_ver },
                );
                return;
            }
        }
        // Fallback: hand off to recovery worker.
        self.token_on_left = !moving_left_to_right;
        self.token_version = next_ver;
        self.update_model_for_token_rename(src_pid, dst_pid, next_ver);
        self.in_flight_2pc.push(ActiveTwoPc {
            txn_id,
            primary_shard: src_shard,
            secondary_shard: dst_shard,
            primary_key: src_key,
            secondary_key: dst_key,
            expire_hlc,
            primary_committed: true,
            commit_hlc: Some(commit_hlc),
            version: next_ver,
            moving_left_to_right,
            client_op_id: op_id,
        });
        self.history.complete_info_at(
            self.now_ms * 1_000_000,
            client_proc,
            op_kind,
            "commit secondary deferred to recovery",
        );
    }

    fn update_model_for_token_rename(&mut self, src_pid: InodeId, dst_pid: InodeId, new_ver: u64) {
        if let Some((ino, fid, uid, _)) = self.model.files.remove(&(src_pid, "token".to_string())) {
            self.model.files.insert(
                (dst_pid, "token".to_string()),
                (ino, fid, uid, new_ver as i64),
            );
        }
    }

    /// Runs MetaDB's 2PC recovery protocol on all in-flight / crashed transactions!
    fn recover_all_2pc(&mut self) {
        let pending = std::mem::take(&mut self.in_flight_2pc);
        let mut still_pending = Vec::new();
        for tx in pending {
            let _ = tx.client_op_id;
            let _ = tx.version;
            let _ = tx.moving_left_to_right;
            let _ = tx.primary_key;
            // Read authoritative `TxnStatusRecord` from a quorum-caught-up primary leader!
            // If the primary shard is mid-election or partitioned, keep `tx` in `still_pending`
            // instead of mistaking shard unavailability for an absent `TxnStatusRecord`.
            let Some(primary_lid) = self.quorum_caught_up_leader(tx.primary_shard) else {
                still_pending.push(tx);
                continue;
            };
            let Some(r) = self.replicas.get(&(primary_lid, tx.primary_shard)) else {
                still_pending.push(tx);
                continue;
            };
            let Ok(snap) = r.engine.acquire_linearizable_snapshot(r.last_applied) else {
                still_pending.push(tx);
                continue;
            };
            let Ok(raw_opt) = r
                .engine
                .get_with_snapshot(&TxnRecordKey { txn_id: tx.txn_id }.encode(), &snap)
            else {
                still_pending.push(tx);
                continue;
            };
            let status_opt = raw_opt.and_then(|raw| TxnStatusRecord::decode(&raw).ok());

            match status_opt.map(|rec| rec.state) {
                Some(TxnState::Committed { commit_hlc }) => {
                    // Roll forward secondary & mark acked!
                    let csec = TxRaftCmd::CommitSecondary2pc {
                        txn_id: tx.txn_id,
                        commit_hlc,
                        target_user_keys: vec![tx.secondary_key.clone()],
                    };
                    let sec_ok = self
                        .propose_tx_cmd(tx.secondary_shard, &csec, None, None)
                        .is_ok_and(|(_, idx, t, _)| {
                            self.wait_committed_ok(tx.secondary_shard, idx, t, 800) == Some(true)
                        });
                    let mut ack_ok = false;
                    if sec_ok {
                        let ack = TxRaftCmd::MarkParticipantAcked {
                            txn_id: tx.txn_id,
                            acked_user_keys: vec![tx.primary_key.clone(), tx.secondary_key.clone()],
                        };
                        ack_ok = self
                            .propose_tx_cmd(tx.primary_shard, &ack, None, None)
                            .is_ok_and(|(_, aidx, at, _)| {
                                self.wait_committed_ok(tx.primary_shard, aidx, at, 800)
                                    == Some(true)
                            });
                    }
                    if sec_ok && ack_ok {
                        if !tx.primary_committed {
                            let (src_pid, dst_pid) = if tx.moving_left_to_right {
                                (DIR_LEFT_A, DIR_RIGHT_A)
                            } else {
                                (DIR_RIGHT_A, DIR_LEFT_A)
                            };
                            self.token_on_left = !tx.moving_left_to_right;
                            self.token_version = tx.version;
                            self.update_model_for_token_rename(src_pid, dst_pid, tx.version);
                        }
                        self.two_pc_committed += 1;
                        code_probe("probe.dst.two_pc_rolled_forward");
                    } else {
                        still_pending.push(tx);
                    }
                }
                Some(TxnState::Pending) => {
                    // Advance virtual time past `expire_hlc` if needed during recovery, then execute
                    // REAL `CheckAndAbortExpiredTxn` + `Abort2pc` + `MarkParticipantAcked`!
                    let now_hlc = self
                        .find_leader(tx.primary_shard)
                        .map(|lid| self.clocks[&lid].now(self.now_ms))
                        .unwrap_or(HlcTimestamp::new(VIRTUAL_EPOCH_MS + self.now_ms, 0));
                    if now_hlc <= tx.expire_hlc {
                        still_pending.push(tx);
                        continue;
                    }
                    let chk = TxRaftCmd::CheckAndAbortExpiredTxn {
                        txn_id: tx.txn_id,
                        caller_hlc: now_hlc,
                    };
                    let chk_ok = self
                        .propose_tx_cmd(tx.primary_shard, &chk, None, None)
                        .is_ok_and(|(_, idx, t, _)| {
                            self.wait_committed_ok(tx.primary_shard, idx, t, 800) == Some(true)
                        });
                    let ab_sec = TxRaftCmd::Abort2pc {
                        txn_id: tx.txn_id,
                        target_user_keys: vec![tx.secondary_key.clone()],
                    };
                    let sec_ok = self
                        .propose_tx_cmd(tx.secondary_shard, &ab_sec, None, None)
                        .is_ok_and(|(_, sidx, st, _)| {
                            self.wait_committed_ok(tx.secondary_shard, sidx, st, 800) == Some(true)
                        });
                    let mut ack_ok = false;
                    if chk_ok && sec_ok {
                        let ack = TxRaftCmd::MarkParticipantAcked {
                            txn_id: tx.txn_id,
                            acked_user_keys: vec![tx.primary_key.clone(), tx.secondary_key.clone()],
                        };
                        ack_ok = self
                            .propose_tx_cmd(tx.primary_shard, &ack, None, None)
                            .is_ok_and(|(_, aidx, at, _)| {
                                self.wait_committed_ok(tx.primary_shard, aidx, at, 800)
                                    == Some(true)
                            });
                    }
                    if chk_ok && sec_ok && ack_ok {
                        self.two_pc_aborted += 1;
                        code_probe("probe.dst.two_pc_expired_aborted");
                    } else {
                        still_pending.push(tx);
                    }
                }
                Some(TxnState::Aborted) => {
                    // Clean up any lingering secondary write intent & GC the primary `0x06` record!
                    let ab_sec = TxRaftCmd::Abort2pc {
                        txn_id: tx.txn_id,
                        target_user_keys: vec![tx.secondary_key.clone()],
                    };
                    let sec_ok = self
                        .propose_tx_cmd(tx.secondary_shard, &ab_sec, None, None)
                        .is_ok_and(|(_, sidx, st, _)| {
                            self.wait_committed_ok(tx.secondary_shard, sidx, st, 800) == Some(true)
                        });
                    let mut ack_ok = false;
                    if sec_ok {
                        let ack = TxRaftCmd::MarkParticipantAcked {
                            txn_id: tx.txn_id,
                            acked_user_keys: vec![tx.primary_key.clone(), tx.secondary_key.clone()],
                        };
                        ack_ok = self
                            .propose_tx_cmd(tx.primary_shard, &ack, None, None)
                            .is_ok_and(|(_, aidx, at, _)| {
                                self.wait_committed_ok(tx.primary_shard, aidx, at, 800)
                                    == Some(true)
                            });
                    }
                    if sec_ok && ack_ok {
                        self.two_pc_aborted += 1;
                    } else {
                        still_pending.push(tx);
                    }
                }
                None => {
                    // Primary record absent (either never prepared on primary, or already GC'd).
                    if tx.primary_committed {
                        if let Some(chlc) = tx.commit_hlc {
                            let csec = TxRaftCmd::CommitSecondary2pc {
                                txn_id: tx.txn_id,
                                commit_hlc: chlc,
                                target_user_keys: vec![tx.secondary_key.clone()],
                            };
                            let ok = self
                                .propose_tx_cmd(tx.secondary_shard, &csec, None, None)
                                .is_ok_and(|(_, idx, t, _)| {
                                    self.wait_committed_ok(tx.secondary_shard, idx, t, 800)
                                        == Some(true)
                                });
                            if !ok {
                                still_pending.push(tx);
                            }
                        }
                    } else {
                        let ab_sec = TxRaftCmd::Abort2pc {
                            txn_id: tx.txn_id,
                            target_user_keys: vec![tx.secondary_key.clone()],
                        };
                        let sec_ok = self
                            .propose_tx_cmd(tx.secondary_shard, &ab_sec, None, None)
                            .is_ok_and(|(_, sidx, st, _)| {
                                self.wait_committed_ok(tx.secondary_shard, sidx, st, 800)
                                    == Some(true)
                            });
                        if sec_ok {
                            self.two_pc_aborted += 1;
                        } else {
                            still_pending.push(tx);
                        }
                    }
                }
            }
        }
        self.in_flight_2pc = still_pending;
    }

    fn on_workload_step(&mut self, step: u32) {
        if self.sync_workload_active {
            self.schedule(self.now_ms + 30, EventPayload::DriveWorkload { step });
            return;
        }
        // Assign a distinct Jepsen client process per dispatched operation so async in-flight
        // writes/reads never collide on the same sequential process slot in `HistoryRecorder`!
        let client = JepsenProcess::Client(step.saturating_add(1));

        match self.cfg.scenario {
            DstScenario::RaftLinearizability | DstScenario::DiskFaultAndTornRecovery => {
                // Mix linearizable Register Writes (`SetAttr(size_bytes)`), `ReadIndex` Reads
                // (including reads directed at potentially partitioned/stale nodes!), and file CRUD!
                let key = self.rng.range_u64(0, 2);
                let action = self.rng.range_u64(0, 99);
                if action < 45 {
                    let val = (self.next_file_seq as i64) * 10 + 1;
                    self.next_file_seq += 1;
                    let cmd = TxRaftCmd::SetAttrSingleShard {
                        pid: DIR_LEFT_A,
                        name: format!("reg_{key}"),
                        patch: SetAttrPatch {
                            size_bytes: Some(val),
                            ..Default::default()
                        },
                    };
                    let kind = JepsenOpKind::Write { key, value: val };
                    let _ = self.propose_tx_cmd(
                        SHARD_META_LEFT,
                        &cmd,
                        None,
                        Some((client, kind, JepsenOpValue::Int(val))),
                    );
                } else if action < 85 {
                    // Pick either current leader OR a random node (testing stale-leader rejection!).
                    let target = if self.rng.chance_permille(750) {
                        self.find_leader(SHARD_META_LEFT).unwrap_or(1)
                    } else {
                        self.node_ids[(self.rng.next_u64() as usize) % self.node_ids.len()]
                    };
                    self.issue_linearizable_read(
                        target,
                        SHARD_META_LEFT,
                        key,
                        DIR_LEFT_A,
                        format!("reg_{key}"),
                        client,
                    );
                } else {
                    // Create a unique file in `DIR_RIGHT_A` (exercises multi-shard + quota/index outbox!).
                    let seq = self.next_file_seq;
                    self.next_file_seq += 1;
                    let uid = UID_POOL[(seq as usize) % UID_POOL.len()];
                    let sz = self.rng.range_u64(16, 512) as i64;
                    let ino = make_file_inode_id(50_000 + seq);
                    let fid = 80_000 + seq;
                    let name = format!("f_{seq:04}.dat");
                    let cmd = TxRaftCmd::CreateFile {
                        pid: DIR_RIGHT_A,
                        name: name.clone(),
                        inode_id: ino,
                        fid,
                        uid,
                        mode: 0o644,
                        size_bytes: sz,
                    };
                    if let Ok((_, idx, term, _)) =
                        self.propose_tx_cmd(SHARD_META_RIGHT, &cmd, None, None)
                    {
                        self.pending_model_files.insert(
                            (SHARD_META_RIGHT, idx, term),
                            (DIR_RIGHT_A, name, ino, fid, uid, sz),
                        );
                        let _ = self.wait_committed_ok(SHARD_META_RIGHT, idx, term, 350);
                    }
                }
            }
            DstScenario::TwoPcCrashAndClockSkew => {
                // Alternate cross-shard 2PC rename (including coordinator crashes!), clock-skewed
                // proposals, and atomic pair reads!
                if step.is_multiple_of(3) {
                    self.sync_workload_active = true;
                    // Recover any prior crashed 2PC before starting next rename.
                    self.recover_all_2pc();
                    let crash_stage = match self.rng.range_u64(0, 99) {
                        0..=24 => 1,  // Crash after Prepare before CommitPrimary
                        25..=49 => 2, // Crash after CommitPrimary before CommitSecondary
                        _ => 0,       // Normal 2PC commit
                    };
                    self.execute_cross_shard_2pc_rename(crash_stage);
                    self.sync_workload_active = false;
                } else if step % 3 == 1 {
                    // Issue a proposal carrying a severely skewed remote HLC (`+900ms > 500ms`)
                    // to verify `HybridLogicalClock::update_checked_with_wall_ms` rejects it cleanly!
                    let lid = self.find_leader(SHARD_META_LEFT).unwrap_or(1);
                    let local_phys = self.clocks[&lid].physical_ms(self.now_ms);
                    let future_hlc =
                        HlcTimestamp::new(local_phys + DEFAULT_MAX_CLOCK_DRIFT_MS + 400, 0);
                    let cmd = TxRaftCmd::CreateFile {
                        pid: DIR_LEFT_A,
                        name: format!("skew_rejected_{step}.dat"),
                        inode_id: make_file_inode_id(90_000 + u64::from(step)),
                        fid: 99_000 + u64::from(step),
                        uid: UID_POOL[0],
                        mode: 0o644,
                        size_bytes: 32,
                    };
                    let res = self.propose_tx_cmd(SHARD_META_LEFT, &cmd, Some(future_hlc), None);
                    if res.is_ok() {
                        self.violations.push(format!(
                            "CLOCK SKEW GUARD FAILED at step={step}: accepted +900ms remote HLC"
                        ));
                    }
                } else {
                    // Perform quorum-confirmed linearizable pair read of `token` across
                    // (`DIR_LEFT_A`, `DIR_RIGHT_A`) when no crash-pending 2PC is mid-flight,
                    // verifying atomic visibility!
                    if self.in_flight_2pc.is_empty() {
                        if let (Some(va), Some(vb)) = (
                            self.read_token_slot_linearizable(SHARD_META_LEFT, DIR_LEFT_A),
                            self.read_token_slot_linearizable(SHARD_META_RIGHT, DIR_RIGHT_A),
                        ) {
                            let tid = self.next_txn_seq;
                            self.next_txn_seq += 1;
                            let kind = JepsenOpKind::TxnReadPair {
                                txn_id: tid,
                                slot_a: DIR_LEFT_A,
                                slot_b: DIR_RIGHT_A,
                            };
                            let ts = self.now_ms * 1_000_000;
                            self.history.invoke_at(ts, client, kind.clone());
                            self.history.complete_ok_at(
                                ts,
                                client,
                                kind,
                                JepsenOpValue::PairRead {
                                    val_a: va,
                                    val_b: vb,
                                },
                            );
                        }
                    }
                }
            }
            DstScenario::SplitAndAuxConvergence => {
                self.sync_workload_active = true;
                // Trigger aborted split rollback at step 6, then live split at step 12!
                if step == 6 {
                    self.execute_online_shard_split(true);
                } else if step == 12 {
                    self.execute_online_shard_split(false);
                }
                let seq = self.next_file_seq;
                self.next_file_seq += 1;
                let pid = match seq % 3 {
                    0 => DIR_LEFT_A,  // Stays on Shard 1 (`< 250`)
                    1 => DIR_LEFT_B,  // Migrates to Split Child Shard 5 (`250..500`)!
                    _ => DIR_RIGHT_A, // Lives on Shard 2 (`>= 500`)
                };
                let sid = self.route_pid_to_shard(pid);
                let uid = UID_POOL[(seq as usize) % UID_POOL.len()];
                let ino = make_file_inode_id(40_000 + seq);
                let fid = 70_000 + seq;
                let sz = self.rng.range_u64(32, 1024) as i64;
                let name = format!("sp_{seq:04}.dat");
                let cmd = TxRaftCmd::CreateFile {
                    pid,
                    name: name.clone(),
                    inode_id: ino,
                    fid,
                    uid,
                    mode: 0o644,
                    size_bytes: sz,
                };
                if let Ok((_, idx, term, _)) = self.propose_tx_cmd(sid, &cmd, None, None) {
                    self.pending_model_files
                        .insert((sid, idx, term), (pid, name, ino, fid, uid, sz));
                    let _ = self.wait_committed_ok(sid, idx, term, 400);
                }
                if step.is_multiple_of(4) {
                    self.drain_all_aux_outboxes();
                }
                self.sync_workload_active = false;
            }
        }
    }

    fn on_nemesis_round(&mut self, round: u32) {
        let all = self.node_ids.clone();
        match self.cfg.scenario {
            DstScenario::RaftLinearizability => match round % 4 {
                0 => {
                    // Isolate current Shard 1 leader!
                    let victim = self.find_leader(SHARD_META_LEFT).unwrap_or(1);
                    self.network.heal_all();
                    self.network.isolate_node(victim, &all);
                    code_probe("probe.dst.nemesis_isolate_leader");
                }
                1 => {
                    // Asymmetric bridge partition: node 1 <-> node 2 & node 2 <-> node 3, 1 !-> 3.
                    self.network.heal_all();
                    if all.len() >= 3 {
                        self.network.clog_link(all[0], all[2]);
                        self.network.clog_link(all[2], all[0]);
                    }
                    code_probe("probe.dst.nemesis_asymmetric_partition");
                }
                2 => {
                    // Crash & restart a single node (keeping majority alive)!
                    self.network.heal_all();
                    let victim = all[(self.rng.next_u64() as usize) % all.len()];
                    self.crash_node(victim, true, 240);
                }
                _ => {
                    self.network.heal_all();
                }
            },
            DstScenario::TwoPcCrashAndClockSkew => {
                // Inject realistic NTP-bounded physical clock steps (forward & backward within
                // `[-180ms, +180ms]` so pairwise skew stays `< DEFAULT_MAX_CLOCK_DRIFT_MS = 500ms`
                // while `step % 3 == 1` separately probes `+900ms` drift-rejection fail-closed).
                let victim = all[(self.rng.next_u64() as usize) % all.len()];
                if let Some(clk) = self.clocks.get_mut(&victim) {
                    let jump = if round.is_multiple_of(2) { 120 } else { -120 };
                    clk.offset_ms = (clk.offset_ms + jump).clamp(-180, 180);
                    code_probe("probe.dst.nemesis_clock_step");
                }
                if round % 3 == 1 {
                    let target = all[((self.rng.next_u64() as usize) + 1) % all.len()];
                    self.network.heal_all();
                    self.network.isolate_node(target, &all);
                } else {
                    self.network.heal_all();
                }
            }
            DstScenario::SplitAndAuxConvergence => match round % 3 {
                0 => {
                    let victim = all[(self.rng.next_u64() as usize) % all.len()];
                    self.crash_node(victim, false, 260);
                }
                1 => {
                    self.network.heal_all();
                    let half = all.len() / 2;
                    self.network.partition_groups(&all[..half], &all[half..]);
                }
                _ => {
                    self.network.heal_all();
                }
            },
            DstScenario::DiskFaultAndTornRecovery => {
                // Inject `fsync` I/O failures + sudden power loss with torn tail frames!
                self.network.heal_all();
                let victim = all[(self.rng.next_u64() as usize) % all.len()];
                if round.is_multiple_of(2) {
                    for &sid in &self.active_shards {
                        if let Some(r) = self.replicas.get_mut(&(victim, sid)) {
                            r.disk.fail_next_fsyncs = 1;
                        }
                    }
                } else {
                    self.crash_node(victim, true, 200);
                }
            }
        }
    }

    fn crash_node(&mut self, node_id: NodeId, inject_torn_tail: bool, restart_after_ms: u64) {
        if self.crashed_nodes.contains(&node_id) {
            return;
        }
        // Keep majority alive so cluster preserves availability across single-node crash!
        let max_crashed = (self.node_ids.len() - 1) / 2;
        if self.crashed_nodes.len() >= max_crashed {
            return;
        }
        self.crashed_nodes.insert(node_id);
        self.node_crashes += 1;
        let shards: Vec<ShardId> = self.active_shards.iter().copied().collect();
        for sid in shards {
            if let Some(r) = self.replicas.get_mut(&(node_id, sid)) {
                r.disk.simulate_power_loss(&mut self.rng, inject_torn_tail);
                r.pending_reads.clear();
            }
        }
        code_probe("probe.dst.node_power_loss");
        self.schedule(
            self.now_ms + restart_after_ms,
            EventPayload::NodeRestart { node_id },
        );
    }

    fn on_node_restart(&mut self, node_id: NodeId) {
        if !self.crashed_nodes.remove(&node_id) {
            return;
        }
        let shards: Vec<ShardId> = self.active_shards.iter().copied().collect();
        for sid in shards {
            let dl = self.now_ms
                + self
                    .rng
                    .range_u64(ELECTION_TIMEOUT_MIN_MS, ELECTION_TIMEOUT_MAX_MS);
            if let Some(r) = self.replicas.get_mut(&(node_id, sid)) {
                match r.reboot_from_disk(dl) {
                    Ok(torn) => {
                        self.disk_torn_tails_recovered += torn;
                    }
                    Err(e) => {
                        self.violations.push(format!(
                            "DISK RECOVERY FAILURE on node={node_id} shard={sid}: {e}"
                        ));
                    }
                }
            }
            self.schedule(
                dl,
                EventPayload::TickElection {
                    node_id,
                    shard_id: sid,
                },
            );
        }
        code_probe("probe.dst.node_restarted");
    }

    /// Runs the full simulation lifecycle: bootstrap -> seed dirs/files -> fault+workload loop ->
    /// heal & recovery -> full-stack 6-invariant + replica convergence + Jepsen WGL/Elle audit!
    fn execute(mut self) -> DstReport {
        let probe_snap = CodeProbeSnapshot::capture();
        let buggify_guard = ThreadBuggifyGuard::install(self.cfg.seed, BuggifyConfig::default());

        // 1. Schedule initial election timers on all `(node, shard)` replicas.
        let init_pairs: Vec<(NodeId, ShardId, u64)> = self
            .replicas
            .iter()
            .map(|(&(nid, sid), r)| (nid, sid, r.election_deadline_ms))
            .collect();
        for (nid, sid, dl) in init_pairs {
            self.schedule(
                dl,
                EventPayload::TickElection {
                    node_id: nid,
                    shard_id: sid,
                },
            );
        }

        // Wait for initial leaders on all 4 bootstrap shards.
        for _ in 0..40 {
            if self
                .active_shards
                .iter()
                .all(|&s| self.find_leader(s).is_some())
            {
                break;
            }
            self.run_until(self.now_ms + 30, 500);
        }

        // 2. Seed initial directory hierarchy & workload keys!
        for (sid, dir_id, uid) in [
            (SHARD_META_LEFT, DIR_LEFT_A, UID_POOL[0]),
            (SHARD_META_LEFT, DIR_LEFT_B, UID_POOL[1]),
            (SHARD_META_RIGHT, DIR_RIGHT_A, UID_POOL[2]),
        ] {
            let cmd = TxRaftCmd::InitDirAttr {
                dir_id,
                parent_pid: ROOT_INODE_ID,
                uid,
                mode: 0o755,
            };
            if let Ok((_, idx, term, _)) = self.propose_tx_cmd(sid, &cmd, None, None) {
                let _ = self.wait_committed_ok(sid, idx, term, 600);
                self.model.dirs.insert(dir_id, uid);
            }
        }

        match self.cfg.scenario {
            DstScenario::RaftLinearizability | DstScenario::DiskFaultAndTornRecovery => {
                for key in 0..3u64 {
                    let cmd = TxRaftCmd::CreateFile {
                        pid: DIR_LEFT_A,
                        name: format!("reg_{key}"),
                        inode_id: make_file_inode_id(1_000 + key),
                        fid: 2_000 + key,
                        uid: UID_POOL[0],
                        mode: 0o644,
                        size_bytes: 0,
                    };
                    if let Ok((_, idx, term, _)) =
                        self.propose_tx_cmd(SHARD_META_LEFT, &cmd, None, None)
                    {
                        let _ = self.wait_committed_ok(SHARD_META_LEFT, idx, term, 600);
                    }
                    self.reg_initials.insert(key, 0);
                }
            }
            DstScenario::TwoPcCrashAndClockSkew => {
                let cmd = TxRaftCmd::CreateFile {
                    pid: DIR_LEFT_A,
                    name: "token".to_string(),
                    inode_id: make_file_inode_id(10_001),
                    fid: 20_001,
                    uid: UID_POOL[0],
                    mode: 0o644,
                    size_bytes: 1,
                };
                if let Ok((_, idx, term, _)) =
                    self.propose_tx_cmd(SHARD_META_LEFT, &cmd, None, None)
                {
                    if self.wait_committed_ok(SHARD_META_LEFT, idx, term, 600) == Some(true) {
                        self.model.files.insert(
                            (DIR_LEFT_A, "token".to_string()),
                            (make_file_inode_id(10_001), 20_001, UID_POOL[0], 1),
                        );
                    }
                }
            }
            DstScenario::SplitAndAuxConvergence => {}
        }

        // 3. Schedule interleaved workload steps, nemesis rounds, and aux drains in virtual time!
        let base_ms = self.now_ms + 20;
        for step in 0..self.cfg.workload_ops {
            self.schedule(
                base_ms + u64::from(step) * 35,
                EventPayload::DriveWorkload { step },
            );
        }
        let nemesis_rounds = (self.cfg.workload_ops / 4).max(4);
        for round in 0..nemesis_rounds {
            self.schedule(
                base_ms + 70 + u64::from(round) * 140,
                EventPayload::DriveNemesis { round },
            );
        }
        for tick in 0..4u64 {
            self.schedule(base_ms + (tick + 1) * 300, EventPayload::DrainAuxOutbox);
        }

        let workload_end_ms = base_ms + u64::from(self.cfg.workload_ops) * 35 + 400;
        self.run_until(workload_end_ms, 200_000);

        // 4. Heal network, reboot any crashed nodes, advance virtual time past 2PC lease TTL,
        //    resolve all crashed 2PC transactions, recover any interrupted split fence, drain all
        //    aux outboxes, and converge replicas!
        self.network.heal_all();
        self.network.drop_permille = 0;
        self.network.dup_permille = 0;
        self.network.reply_drop_permille = 0;
        let crashed: Vec<NodeId> = self.crashed_nodes.iter().copied().collect();
        for nid in crashed {
            self.on_node_restart(nid);
        }
        // Leave clocks skewed inside valid NTP bounds so recovery works under persistent skew.
        self.run_until(self.now_ms + TWO_PC_TTL_MS + 400, 100_000);
        if self.cfg.scenario == DstScenario::SplitAndAuxConvergence && !self.split_published {
            // Match `SplitWorker` post-crash recovery: if an earlier `FenceShardRangeForSplit`
            // committed while `RestoreShardRange` or `UpdateShardRangeOnSplit` was interrupted
            // by a crash/partition, complete rollback or roll-forward after heal!
            if let Some(lid) = self.quorum_caught_up_leader(SHARD_META_LEFT) {
                let orig_range = CompositeKeyRange::meta_pid_range(0, 500).unwrap();
                let (cur_epoch, cur_range) = {
                    let r = &self.replicas[&(lid, SHARD_META_LEFT)];
                    (r.handler.current_epoch(), r.handler.key_range())
                };
                if cur_range != orig_range {
                    if self.active_shards.contains(&SHARD_META_SPLIT_CHILD) {
                        // Child shard already bootstrapped -> roll forward `UpdateShardRangeOnSplit`.
                        let right_range = CompositeKeyRange::meta_pid_range(250, 500).unwrap();
                        let keys_to_delete: Vec<Vec<u8>> = {
                            let r = &self.replicas[&(lid, SHARD_META_LEFT)];
                            r.engine
                                .acquire_linearizable_snapshot(r.last_applied)
                                .ok()
                                .and_then(|snap| {
                                    r.engine
                                        .scan_with_snapshot(
                                            &right_range.start_key,
                                            &right_range.end_key,
                                            &snap,
                                        )
                                        .ok()
                                })
                                .unwrap_or_default()
                                .into_iter()
                                .map(|(k, _)| k)
                                .collect()
                        };
                        let update_cmd = TxRaftCmd::UpdateShardRangeOnSplit {
                            new_range: CompositeKeyRange::meta_pid_range(0, 250).unwrap(),
                            new_epoch: cur_epoch.bump_version(),
                            keys_to_delete,
                            updated_anchor_attr: None,
                            updated_shard_delta: None,
                        };
                        if let Ok((_, uidx, uterm, _)) =
                            self.propose_tx_cmd(SHARD_META_LEFT, &update_cmd, None, None)
                        {
                            if self.wait_committed_ok(SHARD_META_LEFT, uidx, uterm, 800)
                                == Some(true)
                            {
                                self.split_published = true;
                                self.splits_completed += 1;
                                code_probe("probe.dst.online_split_completed");
                            }
                        }
                    } else {
                        // Interrupted fence without child shard -> roll back `RestoreShardRange`.
                        let restore_cmd = TxRaftCmd::RestoreShardRange {
                            restored_range: orig_range,
                            new_epoch: cur_epoch.bump_version(),
                        };
                        if let Ok((_, ridx, rterm, _)) =
                            self.propose_tx_cmd(SHARD_META_LEFT, &restore_cmd, None, None)
                        {
                            if self.wait_committed_ok(SHARD_META_LEFT, ridx, rterm, 800)
                                == Some(true)
                            {
                                code_probe("probe.dst.split_fence_restored");
                            }
                        }
                    }
                }
            }
        }
        for _ in 0..4 {
            self.recover_all_2pc();
            self.run_until(self.now_ms + 250, 50_000);
        }
        for _ in 0..3 {
            self.drain_all_aux_outboxes();
            self.run_until(self.now_ms + 250, 50_000);
        }

        // Wait for all replicas of every active shard to converge to identical `last_applied`!
        let converge_deadline = self.now_ms + 1_200;
        while self.now_ms < converge_deadline {
            let all_converged = self.active_shards.iter().all(|&sid| {
                let vals: Vec<u64> = self
                    .node_ids
                    .iter()
                    .filter_map(|&nid| self.replicas.get(&(nid, sid)).map(|r| r.last_applied))
                    .collect();
                vals.len() == self.node_ids.len() && vals.iter().min() == vals.iter().max()
            });
            if all_converged {
                break;
            }
            self.run_until(self.now_ms + 60, 20_000);
        }

        // 5. Final Full-Stack Audit:
        //    (a) Byte-for-byte multi-replica state convergence + zero leaked system keys!
        //    (b) DirStat, QuotaTable, SecondaryIndexTable verification!
        //    (c) Jepsen WGL Linearizability / Elle Strict-Serializability verification!
        let state_fingerprint = self.audit_replicas_and_invariants();

        // Mark any still-unresolved client writes as `:info` before checking linearizability.
        let end_ns = self.now_ms * 1_000_000;
        let unresolved: Vec<ClientOpRecord> = self
            .client_ops
            .values()
            .filter(|op| !op.resolved)
            .cloned()
            .collect();
        for op in unresolved {
            self.history.complete_info_at(
                end_ns,
                op.client,
                op.kind,
                "unacknowledged at simulation end",
            );
        }

        let hist = self.history.snapshot();
        let history_digest = hist.digest(true);

        let linearizability = match self.cfg.scenario {
            DstScenario::RaftLinearizability | DstScenario::DiskFaultAndTornRecovery => {
                let res = check_register_linearizability(&hist, &self.reg_initials);
                if !res.linearizable {
                    self.violations.push(format!(
                        "WGL LINEARIZABILITY VIOLATION: {:?}",
                        res.violation
                    ));
                }
                Some(res)
            }
            _ => None,
        };

        let elle = if self.cfg.scenario == DstScenario::TwoPcCrashAndClockSkew {
            // Also record final post-recovery atomic pair read!
            let read_slot = |this: &Self, shard: ShardId, pid: InodeId| -> Option<u64> {
                let lid = this.find_leader(shard)?;
                let r = this.replicas.get(&(lid, shard))?;
                let snap = r
                    .engine
                    .acquire_linearizable_snapshot(r.last_applied)
                    .ok()?;
                let raw = r
                    .engine
                    .get_with_snapshot(&MetaKey::child(pid, "token").encode(), &snap)
                    .ok()??;
                InodeAttr::decode(&raw).ok().map(|a| a.size_bytes as u64)
            };
            let va = read_slot(&self, SHARD_META_LEFT, DIR_LEFT_A);
            let vb = read_slot(&self, SHARD_META_RIGHT, DIR_RIGHT_A);
            let tid = self.next_txn_seq + 1;
            let kind = JepsenOpKind::TxnReadPair {
                txn_id: tid,
                slot_a: DIR_LEFT_A,
                slot_b: DIR_RIGHT_A,
            };
            let ts = self.now_ms * 1_000_000;
            self.history
                .invoke_at(ts, JepsenProcess::Client(99), kind.clone());
            self.history.complete_ok_at(
                ts,
                JepsenProcess::Client(99),
                kind,
                JepsenOpValue::PairRead {
                    val_a: va,
                    val_b: vb,
                },
            );
            let final_hist = self.history.snapshot();
            let res = check_elle_rename_history(&final_hist, 1);
            if !res.valid {
                for a in &res.anomalies {
                    self.violations
                        .push(format!("ELLE ANOMALY {}: {}", a.kind, a.description));
                }
            }
            Some(res)
        } else {
            None
        };

        let buggify_firings = buggify_guard.firings();
        drop(buggify_guard);
        let probes_hit: BTreeMap<String, u64> = CodeProbeSnapshot::capture()
            .delta_since(&probe_snap)
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        DstReport {
            scenario: self.cfg.scenario,
            seed: self.cfg.seed,
            node_count: self.cfg.node_count,
            events_processed: self.events_processed,
            final_virtual_time_ms: self.now_ms,
            trace_hash: self.hasher.hash,
            state_fingerprint,
            history_digest,
            buggify_firings,
            raft_elections: self.raft_elections,
            snapshots_installed: self.snapshots_installed,
            packets_dropped: self.network.packets_dropped,
            packets_duplicated: self.network.packets_duplicated,
            packets_reordered: self.network.packets_reordered,
            node_crashes: self.node_crashes,
            disk_torn_tails_recovered: self.disk_torn_tails_recovered,
            fsync_errors_rolled_back: self.fsync_errors_rolled_back,
            clock_skew_rejections: self.clock_skew_rejections,
            two_pc_committed: self.two_pc_committed,
            two_pc_aborted: self.two_pc_aborted,
            splits_completed: self.splits_completed,
            linearizability,
            elle,
            probes_hit,
            violations: self.violations,
        }
    }

    fn audit_replicas_and_invariants(&mut self) -> u64 {
        let mut fp = TraceHasher::new(0x57A7_E000);
        let active: Vec<ShardId> = self.active_shards.iter().copied().collect();

        type ReplicaKvDump = Vec<(Vec<u8>, Vec<u8>)>;
        // 1. Multi-replica byte-for-byte state convergence & zero leaked system keys!
        for sid in &active {
            let mut reference: Option<(NodeId, ReplicaKvDump)> = None;
            for &nid in &self.node_ids {
                let Some(r) = self.replicas.get(&(nid, *sid)) else {
                    self.violations
                        .push(format!("missing replica node={nid} shard={sid}"));
                    continue;
                };
                let Ok(snap) = r.engine.acquire_linearizable_snapshot(r.last_applied) else {
                    self.violations.push(format!(
                        "failed snapshot on node={nid} shard={sid} idx={}",
                        r.last_applied
                    ));
                    continue;
                };
                // Zero leaked system records (`0x05`, `0x06`, `0x07`, `0x0D`)!
                for (label, start, end) in [
                    (
                        "0x05 WriteIntent",
                        vec![SYSTEM_PREFIX_WRITE_INTENT],
                        vec![SYSTEM_PREFIX_WRITE_INTENT + 1],
                    ),
                    (
                        "0x06 TxnRecord",
                        vec![SYSTEM_PREFIX_TXN_RECORD],
                        vec![SYSTEM_PREFIX_TXN_RECORD + 1],
                    ),
                    (
                        "0x07 SpilledAuxOutbox",
                        vec![SYSTEM_PREFIX_SPILLED_AUX_OUTBOX],
                        SpilledAuxOutboxKey::prefix_end().to_vec(),
                    ),
                    (
                        "0x0D LeasePreimage",
                        vec![SYSTEM_PREFIX_LEASE_PREIMAGE],
                        vec![SYSTEM_PREFIX_LEASE_PREIMAGE + 1],
                    ),
                ] {
                    if let Ok(leaked) = r.engine.scan_with_snapshot(&start, &end, &snap) {
                        if !leaked.is_empty() {
                            self.violations.push(format!(
                                "node={nid} shard={sid} (applied={} commit={} snap={}) leaked {} keys in {label}",
                                r.last_applied,
                                r.commit_index,
                                r.snapshot_index,
                                leaked.len()
                            ));
                        }
                    }
                }
                let raw_kvs = r
                    .engine
                    .scan_with_snapshot(
                        &[TABLE_PREFIX_META],
                        &[TABLE_PREFIX_SECONDARY_INDEX + 1],
                        &snap,
                    )
                    .unwrap_or_default();
                let kvs: Vec<(Vec<u8>, Vec<u8>)> = raw_kvs
                    .into_iter()
                    .map(|(k, v)| {
                        if k.first() == Some(&TABLE_PREFIX_META) {
                            if let Ok(mk) = MetaKey::decode(&k) {
                                if mk.is_attr_key() {
                                    if let Ok(base) = InodeAttr::decode(&v) {
                                        let mut rec = r
                                            .handler
                                            .attr_aggregator()
                                            .reconcile_inode_attr(mk.pid, base, &snap);
                                        rec.applied_lsn = 0;
                                        return (k, rec.encode());
                                    }
                                }
                            }
                        }
                        (k, v)
                    })
                    .collect();

                match &reference {
                    None => {
                        for (k, v) in &kvs {
                            fp.mix_bytes(k);
                            fp.mix_bytes(v);
                        }
                        reference = Some((nid, kvs));
                    }
                    Some((ref_nid, ref_kvs)) => {
                        if &kvs != ref_kvs {
                            let detail = kvs
                                .iter()
                                .zip(ref_kvs.iter())
                                .find(|(a, b)| a != b)
                                .map(|((k1, v1), (k2, v2))| {
                                    format!(
                                        " first_diff=(k1={k1:?} a1={:?} vs k2={k2:?} a2={:?})",
                                        InodeAttr::decode(v1).ok(),
                                        InodeAttr::decode(v2).ok()
                                    )
                                })
                                .unwrap_or_default();
                            self.violations.push(format!(
                                "REPLICA DIVERGENCE shard={sid}: node={nid} ({} keys) != node={ref_nid} ({} keys){detail}",
                                kvs.len(),
                                ref_kvs.len()
                            ));
                        }
                    }
                }
            }
        }

        // 2. For `SplitAndAuxConvergence` and `TwoPcCrashAndClockSkew`, audit exact `DirStat`,
        //    `QuotaTable`, and `SecondaryIndexTable` conservation against `self.model`!
        if matches!(
            self.cfg.scenario,
            DstScenario::SplitAndAuxConvergence | DstScenario::TwoPcCrashAndClockSkew
        ) {
            for &pid in self.model.dirs.keys() {
                let sid = self.route_pid_to_shard(pid);
                if let Some(lid) = self.find_leader(sid) {
                    let r = &self.replicas[&(lid, sid)];
                    if let Ok(snap) = r.engine.acquire_linearizable_snapshot(r.last_applied) {
                        if let Ok(Some(raw)) = r
                            .engine
                            .get_with_snapshot(&MetaKey::dir_attr(pid).encode(), &snap)
                        {
                            if let Ok(base) = InodeAttr::decode(&raw) {
                                let rec = r
                                    .handler
                                    .attr_aggregator()
                                    .reconcile_inode_attr(pid, base, &snap);
                                let (exp_children, exp_bytes) = self.model.expected_dirstat(pid);
                                if rec.num_children != exp_children || rec.size_bytes != exp_bytes {
                                    self.violations.push(format!(
                                        "DIRSTAT MISMATCH pid={pid} shard={sid}: got (children={}, bytes={}) != expected ({exp_children}, {exp_bytes})",
                                        rec.num_children, rec.size_bytes
                                    ));
                                }
                            }
                        }
                    }
                }
            }

            // Audit QuotaTable (`SHARD_QUOTA`) exactness for `SplitAndAuxConvergence`!
            if self.cfg.scenario == DstScenario::SplitAndAuxConvergence {
                if let Some(qlid) = self.find_leader(SHARD_QUOTA) {
                    let r = &self.replicas[&(qlid, SHARD_QUOTA)];
                    if let Ok(snap) = r.engine.acquire_linearizable_snapshot(r.last_applied) {
                        for &uid in &UID_POOL {
                            let (exp_ino, exp_bytes) = self.model.expected_quota(uid);
                            // InitDirAttr does not emit a quota aux delta; only created files do!
                            let file_inodes = exp_ino
                                - self.model.dirs.values().filter(|&&u| u == uid).count() as i64;
                            let got = r
                                .engine
                                .get_with_snapshot(&QuotaKey { uid }.encode(), &snap)
                                .ok()
                                .flatten()
                                .and_then(|b| QuotaRecord::decode(&b).ok())
                                .unwrap_or_else(|| QuotaRecord::new(uid, u64::MAX));
                            if got.used_inodes != file_inodes || got.used_bytes != exp_bytes {
                                self.violations.push(format!(
                                    "QUOTA MISMATCH uid={uid}: got (inodes={}, bytes={}) != expected ({file_inodes}, {exp_bytes})",
                                    got.used_inodes, got.used_bytes
                                ));
                            }
                        }
                    }
                }

                // Audit SecondaryIndexTable (`SHARD_INDEX`) bijection!
                if let Some(ilid) = self.find_leader(SHARD_INDEX) {
                    let r = &self.replicas[&(ilid, SHARD_INDEX)];
                    if let Ok(snap) = r.engine.acquire_linearizable_snapshot(r.last_applied) {
                        for ((pid, name), (_, fid, _, _)) in &self.model.files {
                            let got = r
                                .engine
                                .get_with_snapshot(&SecondaryIndexKey { fid: *fid }.encode(), &snap)
                                .ok()
                                .flatten()
                                .and_then(|b| SecondaryIndexRecord::decode(&b).ok());
                            match got {
                                Some(rec) if rec.pid == *pid && &rec.name == name => {}
                                other => {
                                    self.violations.push(format!(
                                        "SECONDARY INDEX MISMATCH fid={fid}: got {other:?}, expected ({pid}, {name})"
                                    ));
                                }
                            }
                        }
                        let total_rows = r
                            .engine
                            .scan_with_snapshot(
                                &[TABLE_PREFIX_SECONDARY_INDEX],
                                &[TABLE_PREFIX_SECONDARY_INDEX + 1],
                                &snap,
                            )
                            .map(|v| v.len())
                            .unwrap_or(0);
                        if total_rows != self.model.files.len() {
                            self.violations.push(format!(
                                "SECONDARY INDEX CARDINALITY MISMATCH: got {total_rows}, expected {}",
                                self.model.files.len()
                            ));
                        }
                    }
                }
            }
        }

        // Verify route contiguity `[0x01, 0x04)` across active shards!
        let mut ranges: Vec<(ShardId, CompositeKeyRange)> = Vec::new();
        for sid in &active {
            if let Some(lid) = self.find_leader(*sid) {
                ranges.push((*sid, self.replicas[&(lid, *sid)].handler.key_range()));
            }
        }
        ranges.sort_by(|a, b| a.1.start_key.cmp(&b.1.start_key));
        for pair in ranges.windows(2) {
            if pair[0].1.end_key != pair[1].1.start_key {
                self.violations.push(format!(
                    "ROUTE GAP/OVERLAP between shard {} and shard {}",
                    pair[0].0, pair[1].0
                ));
            }
        }
        let _ = TABLE_PREFIX_QUOTA;
        let _ = PersistentEntry::PhysicalTombstone;

        fp.hash
    }
}

/// Runs a single deterministic DST seed inside a single thread (100% bit-for-bit reproducible).
pub fn run_dst_seed(cfg: &DstConfig) -> DstReport {
    SimWorld::new(cfg.clone()).execute()
}

/// Runs a parallel ensemble of consecutive DST seeds across `concurrency` threads (each seed is
/// completely thread-isolated with its own `SimWorld`, `SimClock`, `SimRng`, and `ThreadBuggifyGuard`).
pub fn run_dst_ensemble(
    scenario: DstScenario,
    start_seed: u64,
    seed_count: u32,
    workload_ops: u32,
    concurrency: usize,
) -> DstEnsembleReport {
    let workers = concurrency.max(1).min(seed_count.max(1) as usize);
    let next_offset = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let mut handles = Vec::with_capacity(workers);

    for _ in 0..workers {
        let next = next_offset.clone();
        handles.push(std::thread::spawn(move || {
            let mut local = Vec::new();
            loop {
                let off = next.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if off >= seed_count {
                    break;
                }
                let seed = start_seed.wrapping_add(u64::from(off));
                let mut cfg = DstConfig::for_scenario(scenario, seed);
                cfg.workload_ops = workload_ops.max(12);
                local.push(run_dst_seed(&cfg));
            }
            local
        }));
    }

    let mut passed = 0u32;
    let mut failed = 0u32;
    let mut total_events = 0u64;
    let mut total_buggify = 0u64;
    let mut total_elections = 0u64;
    let mut total_crashes = 0u64;
    let mut total_snapshots = 0u64;
    let mut total_torn = 0u64;
    let mut total_fsync = 0u64;
    let mut total_skew = 0u64;
    let mut first_failure = None;

    for h in handles {
        let reports = h.join().expect("dst worker thread");
        for r in reports {
            total_events += r.events_processed;
            total_buggify += r.buggify_firings;
            total_elections += r.raft_elections;
            total_crashes += r.node_crashes;
            total_snapshots += r.snapshots_installed;
            total_torn += r.disk_torn_tails_recovered;
            total_fsync += r.fsync_errors_rolled_back;
            total_skew += r.clock_skew_rejections;
            if r.is_ok() {
                passed += 1;
            } else {
                failed += 1;
                if first_failure.is_none() {
                    first_failure = Some(r.to_string());
                }
            }
        }
    }

    DstEnsembleReport {
        scenario,
        start_seed,
        total_seeds: seed_count,
        passed_seeds: passed,
        failed_seeds: failed,
        total_events,
        total_buggify_firings: total_buggify,
        total_elections,
        total_crashes,
        total_snapshots,
        total_torn_recovered: total_torn,
        total_fsync_rollbacks: total_fsync,
        total_skew_rejections: total_skew,
        first_failure,
    }
}

#[cfg(test)]
mod dst_unit_tests {
    use super::*;

    #[test]
    fn dst_seed_is_bit_for_bit_reproducible() {
        for scenario in DstScenario::all() {
            let cfg = DstConfig::for_scenario(scenario, 0xD57_0001);
            let r1 = run_dst_seed(&cfg);
            let r2 = run_dst_seed(&cfg);
            assert!(r1.is_ok(), "r1 failed: {r1}");
            assert!(r2.is_ok(), "r2 failed: {r2}");
            assert_eq!(r1.events_processed, r2.events_processed);
            assert_eq!(r1.final_virtual_time_ms, r2.final_virtual_time_ms);
            assert_eq!(r1.trace_hash, r2.trace_hash);
            assert_eq!(r1.state_fingerprint, r2.state_fingerprint);
            assert_eq!(r1.history_digest, r2.history_digest);
            assert_eq!(r1.buggify_firings, r2.buggify_firings);
        }
    }
}
