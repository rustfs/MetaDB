//! OpenRaft `RaftTypeConfig` (`MetaDbTypeConfig`), 3-watermark tracker, and snapshot payloads.

use std::sync::atomic::{AtomicU64, Ordering};

use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use metadb_core::{HlcTimestamp, MetaDbError, ShardEpoch};
use metadb_storage::DurableShardCheckpoint;

/// Application request replicated through a shard's OpenRaft group.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct MetaDbRaftRequest {
    pub expected_epoch: ShardEpoch,
    pub hlc: HlcTimestamp,
    pub payload: Vec<u8>,
}

impl std::fmt::Display for MetaDbRaftRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MetaDbRaftRequest(epoch={:?}, hlc={}, len={})",
            self.expected_epoch,
            self.hlc.0,
            self.payload.len()
        )
    }
}

/// Response returned after `RaftStateMachine::apply` commits a `MetaDbRaftRequest`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct MetaDbRaftResponse {
    pub applied_raft_index: u64,
    pub applied_hlc: HlcTimestamp,
    pub current_epoch: ShardEpoch,
    pub result: std::result::Result<Vec<u8>, MetaDbError>,
}

impl std::fmt::Display for MetaDbRaftResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "MetaDbRaftResponse(idx={}, hlc={})",
            self.applied_raft_index, self.applied_hlc.0
        )
    }
}

/// Built-in KV payload used by standalone `metadb-raft` tests (`BasicKvApplyHandler`).
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub enum BasicKvCommand {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
    Noop,
}

/// Verified per-file payload inside a `SnapshotArchive` with explicit `(expected_len, crc32c)` metadata.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct SnapshotFileEntry {
    pub name: String,
    pub expected_len: u64,
    pub crc32c: u32,
    // SST files routinely exceed the default preallocation cap. Native byte
    // vectors validate the complete input slice before allocating their buffer.
    #[wincode(with = "wincode::containers::Vec<u8, wincode::len::UseIntLen<u64, { usize::MAX }>>")]
    pub bytes: Vec<u8>,
}

/// Portable snapshot bundle containing verified hard-linked `.sst` and `MANIFEST` files
/// pinned strictly at `checkpoint.flushed_raft_index` (`Resolution B6` / `G-S5`).
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct SnapshotArchive {
    pub checkpoint: DurableShardCheckpoint,
    /// Verified files inside the shard `lsm/` directory.
    pub files: Vec<SnapshotFileEntry>,
}

openraft::declare_raft_types!(
    pub MetaDbTypeConfig:
        D = MetaDbRaftRequest,
        R = MetaDbRaftResponse,
        NodeId = u64,
        Node = BasicNode,
);

/// Current applied state broadcast to `ReadIndexBatcher` waiters via `tokio::sync::watch`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppliedFrontier {
    pub raft_index: u64,
    pub hlc: HlcTimestamp,
    pub epoch: ShardEpoch,
}

impl Default for AppliedFrontier {
    fn default() -> Self {
        Self {
            raft_index: 0,
            hlc: HlcTimestamp::ZERO,
            epoch: ShardEpoch::initial(),
        }
    }
}

/// Atomic three-watermark gate clamping physical `.raftlog` prefix purges (`consensus_and_transactions.md` §5.2):
/// `safe_purge_index = min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)`.
/// Also tracks `contiguous_aux_acked_index` separately from `aux_outbox_durable_index` so that
/// `[0x07]` spills advance `.raftlog` purge without skipping un-acked `[0x07]` tasks on `MANIFEST` recovery.
#[derive(Debug)]
pub struct ShardWatermarkTracker {
    state_flushed_index: AtomicU64,
    aux_outbox_durable_index: AtomicU64,
    contiguous_aux_acked_index: AtomicU64,
    last_snapshot_index: AtomicU64,
}

impl Default for ShardWatermarkTracker {
    fn default() -> Self {
        Self::new(0, 0, 0)
    }
}

impl ShardWatermarkTracker {
    pub fn new(state_flushed: u64, aux_durable: u64, last_snapshot: u64) -> Self {
        Self {
            state_flushed_index: AtomicU64::new(state_flushed),
            aux_outbox_durable_index: AtomicU64::new(aux_durable),
            contiguous_aux_acked_index: AtomicU64::new(aux_durable),
            last_snapshot_index: AtomicU64::new(last_snapshot),
        }
    }

    pub fn advance_state_flushed(&self, idx: u64) {
        self.state_flushed_index.fetch_max(idx, Ordering::AcqRel);
    }

    pub fn advance_aux_outbox_durable(&self, idx: u64) {
        self.aux_outbox_durable_index
            .fetch_max(idx, Ordering::AcqRel);
    }

    pub fn set_aux_outbox_durable(&self, idx: u64) {
        self.aux_outbox_durable_index.store(idx, Ordering::Release);
    }

    pub fn advance_contiguous_aux_acked(&self, idx: u64) {
        self.contiguous_aux_acked_index
            .fetch_max(idx, Ordering::AcqRel);
    }

    pub fn set_contiguous_aux_acked(&self, idx: u64) {
        self.contiguous_aux_acked_index
            .store(idx, Ordering::Release);
    }

    pub fn contiguous_aux_acked_index(&self) -> u64 {
        self.contiguous_aux_acked_index.load(Ordering::Acquire)
    }

    /// Advances `aux_outbox_durable_index` and `contiguous_aux_acked_index` to `idx` if and only if
    /// all entries `< idx` are already contiguously durable/acked (used for `Blank` / `Membership` /
    /// rejected entries that emit 0 aux tasks).
    pub fn advance_aux_for_non_aux_entry(&self, idx: u64) {
        let prev = idx.saturating_sub(1);
        let _ = self.aux_outbox_durable_index.compare_exchange(
            prev,
            idx,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
        let _ = self.contiguous_aux_acked_index.compare_exchange(
            prev,
            idx,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn advance_last_snapshot(&self, idx: u64) {
        self.last_snapshot_index.fetch_max(idx, Ordering::AcqRel);
    }

    pub fn snapshot_values(&self) -> (u64, u64, u64) {
        (
            self.state_flushed_index.load(Ordering::Acquire),
            self.aux_outbox_durable_index.load(Ordering::Acquire),
            self.last_snapshot_index.load(Ordering::Acquire),
        )
    }

    #[inline]
    pub fn safe_purge_index(&self) -> u64 {
        let (s, a, snap) = self.snapshot_values();
        s.min(a).min(snap)
    }
}

#[cfg(test)]
mod codec_tests {
    use super::*;
    use metadb_core::{checksum_crc32c, codec};

    #[test]
    fn large_snapshot_file_uses_native_decoder_and_preserves_wire_bytes() {
        let payload = vec![42u8; (4 << 20) + 1];
        let archive = SnapshotArchive {
            checkpoint: DurableShardCheckpoint::default(),
            files: vec![SnapshotFileEntry {
                name: "000001.sst".into(),
                expected_len: payload.len() as u64,
                crc32c: checksum_crc32c(&payload),
                bytes: payload,
            }],
        };
        let mut bytes = codec::serialize(&archive).unwrap();
        assert_eq!(bytes, codec::serialize_serde(&archive).unwrap());
        let decoded: SnapshotArchive =
            wincode::deserialize(&bytes).expect("large snapshot file must use native decoding");
        assert_eq!(decoded, archive);
        assert_eq!(
            codec::deserialize::<SnapshotArchive>(&bytes).unwrap(),
            archive
        );

        let length_offset = bytes.len() - archive.files[0].bytes.len() - 8;
        bytes.truncate(length_offset + 8);
        bytes[length_offset..].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(wincode::deserialize::<SnapshotArchive>(&bytes).is_err());
        assert!(codec::deserialize::<SnapshotArchive>(&bytes).is_err());
    }
}
