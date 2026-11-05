use std::collections::VecDeque;
use std::sync::RwLock;

use metadb_core::{FileId, HlcTimestamp, InodeId, ShardId, UserId};

/// Monotonically increasing Raft Log Sequence Number (LSN).
pub type LogSequenceNumber = u64;

/// Payload recorded inside a Raft log entry for unified durability of primary metadata,
/// asynchronous parent-directory attribute deltas, and asynchronous auxiliary updates.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RaftLogPayload {
    /// Primary key-value write in `MetaTable`.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Primary key deletion in `MetaTable`.
    Delete { key: Vec<u8> },
    /// Asynchronous parent-directory attribute delta (§5.2).
    AttrDelta {
        pid: InodeId,
        delta_children: i64,
        delta_bytes: i64,
        modify_time: HlcTimestamp,
    },
    /// Asynchronous quota update request (§5.1).
    AuxQuotaDelta {
        origin_shard_id: ShardId,
        uid: UserId,
        delta_bytes: i64,
        delta_inodes: i64,
    },
    /// Asynchronous secondary index upsert (`FID -> (PID, name)`).
    AuxIndexUpsert {
        fid: FileId,
        pid: InodeId,
        name: String,
        create_time: HlcTimestamp,
    },
    /// Asynchronous secondary index deletion.
    AuxIndexDelete { fid: FileId },
}

/// A committed entry in a shard's Raft log.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RaftLogEntry {
    pub lsn: LogSequenceNumber,
    pub term: u64,
    pub hlc: HlcTimestamp,
    pub payload: RaftLogPayload,
}

/// Three-component watermark governing safe Raft log truncation (§3.2, §5.1, §6.1).
///
/// Because MetaDB disables the local storage engine's native WAL to avoid double-logging,
/// Raft log entries may only be truncated up to:
/// `min(storage_flushed_lsn, attr_aggregated_lsn, aux_sink_acked_lsn)`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TruncationWatermarks {
    pub storage_flushed_lsn: LogSequenceNumber,
    pub attr_aggregated_lsn: LogSequenceNumber,
    pub aux_sink_acked_lsn: LogSequenceNumber,
}

impl TruncationWatermarks {
    /// Computes the highest LSN that can be safely truncated from the Raft log
    /// without risking data loss on node crash or leader failover.
    pub fn safe_truncate_lsn(&self) -> LogSequenceNumber {
        self.storage_flushed_lsn
            .min(self.attr_aggregated_lsn)
            .min(self.aux_sink_acked_lsn)
    }
}

struct RaftLogInner {
    term: u64,
    next_lsn: LogSequenceNumber,
    entries: VecDeque<RaftLogEntry>,
    watermarks: TruncationWatermarks,
}

/// Replicated consensus log for a shard (works identically in `1-Voter` Single-Node
/// mode and `3-Voter` Multi-Raft distributed mode).
pub struct RaftLog {
    shard_id: ShardId,
    inner: RwLock<RaftLogInner>,
}

impl RaftLog {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            shard_id,
            inner: RwLock::new(RaftLogInner {
                term: 1,
                next_lsn: 1,
                entries: VecDeque::new(),
                watermarks: TruncationWatermarks::default(),
            }),
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    /// Appends a payload to the Raft log and returns the assigned `LogSequenceNumber`.
    pub fn append(&self, hlc: HlcTimestamp, payload: RaftLogPayload) -> LogSequenceNumber {
        let mut guard = self.inner.write().unwrap();
        let lsn = guard.next_lsn;
        guard.next_lsn += 1;
        let term = guard.term;
        guard.entries.push_back(RaftLogEntry {
            lsn,
            term,
            hlc,
            payload,
        });
        lsn
    }

    /// Returns the most recently assigned LSN (or 0 if empty).
    pub fn last_lsn(&self) -> LogSequenceNumber {
        let guard = self.inner.read().unwrap();
        guard.next_lsn.saturating_sub(1)
    }

    /// Updates the storage flush watermark and truncates prefix entries that are now durable
    /// across all three subsystems.
    pub fn advance_storage_flushed_lsn(&self, lsn: LogSequenceNumber) -> usize {
        let mut guard = self.inner.write().unwrap();
        guard.watermarks.storage_flushed_lsn = guard.watermarks.storage_flushed_lsn.max(lsn);
        Self::truncate_locked(&mut guard)
    }

    /// Updates the async attribute aggregation watermark and truncates eligible prefix entries.
    pub fn advance_attr_aggregated_lsn(&self, lsn: LogSequenceNumber) -> usize {
        let mut guard = self.inner.write().unwrap();
        guard.watermarks.attr_aggregated_lsn = guard.watermarks.attr_aggregated_lsn.max(lsn);
        Self::truncate_locked(&mut guard)
    }

    /// Updates the async auxiliary sink acknowledgment watermark and truncates eligible prefix entries.
    pub fn advance_aux_sink_acked_lsn(&self, lsn: LogSequenceNumber) -> usize {
        let mut guard = self.inner.write().unwrap();
        guard.watermarks.aux_sink_acked_lsn = guard.watermarks.aux_sink_acked_lsn.max(lsn);
        Self::truncate_locked(&mut guard)
    }

    /// Returns current watermarks.
    pub fn watermarks(&self) -> TruncationWatermarks {
        self.inner.read().unwrap().watermarks
    }

    /// Returns all retained log entries with `lsn > after_lsn` for crash recovery or learner catch-up.
    pub fn entries_after(&self, after_lsn: LogSequenceNumber) -> Vec<RaftLogEntry> {
        let guard = self.inner.read().unwrap();
        guard
            .entries
            .iter()
            .filter(|e| e.lsn > after_lsn)
            .cloned()
            .collect()
    }

    /// Returns the count of currently retained (un-truncated) Raft log entries.
    pub fn retained_len(&self) -> usize {
        self.inner.read().unwrap().entries.len()
    }

    fn truncate_locked(inner: &mut RaftLogInner) -> usize {
        let safe_lsn = inner.watermarks.safe_truncate_lsn();
        let mut removed = 0;
        while let Some(front) = inner.entries.front() {
            if front.lsn <= safe_lsn {
                inner.entries.pop_front();
                removed += 1;
            } else {
                break;
            }
        }
        removed
    }
}
