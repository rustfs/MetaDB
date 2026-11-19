//! Durable cell heartbeats and Jumbo placement records (`CELL_CONTROL_PLANE`).
//!
//! This file is separate from `CELL_MANAGER_STATE`. A heartbeat must not rewrite
//! shard routes, and a placement record must not be confused with a route epoch.
//! The codec is the same CRC32C frame used by the cell state file. Version 1 is
//! the only layout this process reads; a different version fails the open.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use metadb_core::{InodeId, NodeId, ShardId};

use crate::jumbo_placement::{CommittedMove, PlacementPhase};

/// How often a process records and, when it is not the leader, sends its own heartbeat.
pub const HEARTBEAT_INTERVAL_MS: u64 = 3_000;

/// Minimum gap between two leadership-transfer or peer-replacement attempts for one shard.
pub const PLACEMENT_RETRY_MS: u64 = 30_000;

pub const CONTROL_PLANE_VERSION: u32 = 1;
pub const CONTROL_PLANE_FILE: &str = "CELL_CONTROL_PLANE";

/// Upper bound on shard ids stored from one heartbeat. The ids are annotations.
pub const MAX_REPORTED_SHARD_IDS: usize = 1_024;

/// Largest forward jump accepted from the stored sequence. A first record must
/// also sit inside `1..=MAX_HEARTBEAT_SEQ_ADVANCE`. `u64::MAX` cannot wedge the node.
pub const MAX_HEARTBEAT_SEQ_ADVANCE: u64 = 64;

/// Minimum gap between two remote heartbeats that carry a new sequence.
pub const MIN_REMOTE_HEARTBEAT_INTERVAL_MS: u64 = 1_000;

/// Heartbeats are an unreplicated local observation. They are not a shard epoch
/// and must not schedule splits, membership changes, or leadership transfer.
pub fn heartbeat_is_scheduling_truth() -> bool {
    false
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct PersistedControlPlane {
    pub version: u32,
    pub heartbeats: BTreeMap<NodeId, StoredHeartbeat>,
    /// Next sequence this process will assign to its own heartbeat. Remote sequences
    /// do not consume it. Zero is never assigned.
    pub next_local_seq: u64,
    pub placements: BTreeMap<ShardId, StoredPlacement>,
}

impl Default for PersistedControlPlane {
    fn default() -> Self {
        Self {
            version: CONTROL_PLANE_VERSION,
            heartbeats: BTreeMap::new(),
            next_local_seq: 1,
            placements: BTreeMap::new(),
        }
    }
}

/// One accepted heartbeat. `reported_shard_ids` came from a client RPC and are not routes.
/// `local_hosted_shard_ids` is filled only when this process built the record from its own route table.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct StoredHeartbeat {
    pub node_id: NodeId,
    pub seq: u64,
    pub observed_unix_ms: u64,
    pub disk_free_bytes: Option<u64>,
    pub reported_shard_ids: Vec<ShardId>,
    pub local_hosted_shard_ids: Vec<ShardId>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct StoredPlacement {
    pub shard_id: ShardId,
    pub dominant_pid: InodeId,
    pub dedicated_node: NodeId,
    pub phase: PlacementPhase,
    pub blocked_reason: String,
    /// Moves this process observed after its own Raft call returned. A restart does not
    /// invent entries for a move it cannot prove it committed.
    pub committed_moves: Vec<CommittedMove>,
    pub last_transfer_attempt_unix_ms: u64,
    pub last_replace_attempt_unix_ms: u64,
    pub updated_unix_ms: u64,
}

impl StoredPlacement {
    pub fn same_outcome(&self, other: &Self) -> bool {
        self.shard_id == other.shard_id
            && self.dominant_pid == other.dominant_pid
            && self.dedicated_node == other.dedicated_node
            && self.phase == other.phase
            && self.blocked_reason == other.blocked_reason
            && self.committed_moves == other.committed_moves
            && self.last_transfer_attempt_unix_ms == other.last_transfer_attempt_unix_ms
            && self.last_replace_attempt_unix_ms == other.last_replace_attempt_unix_ms
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeartbeatWrite {
    pub node_id: NodeId,
    pub seq: u64,
    pub observed_unix_ms: u64,
    pub disk_free_bytes: Option<u64>,
    pub reported_shard_ids: Vec<ShardId>,
    pub local_hosted_shard_ids: Vec<ShardId>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeartbeatDecision {
    /// Persist `write` in place of any older record for this node.
    Store(StoredHeartbeat),
    /// The same sequence and body are already durable. Do not rewrite the timestamp.
    Replay(StoredHeartbeat),
}

pub fn unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

pub fn normalize_ids(ids: impl IntoIterator<Item = ShardId>) -> Result<Vec<ShardId>, String> {
    let mut out: Vec<ShardId> = ids.into_iter().filter(|id| *id != 0).collect();
    out.sort_unstable();
    out.dedup();
    if out.len() > MAX_REPORTED_SHARD_IDS {
        return Err(format!(
            "heartbeat lists {} shard ids; the stored annotation limit is {MAX_REPORTED_SHARD_IDS}. Client shard descriptors are not applied to routes",
            out.len()
        ));
    }
    Ok(out)
}

/// Decide whether `write` may update `existing`.
///
/// A lower sequence is rejected and the stored record stays. An equal sequence with the
/// same body is a replay. An equal sequence with a different body is a conflict and must
/// not overwrite. A higher sequence replaces the record.
pub fn decide_heartbeat(
    existing: Option<&StoredHeartbeat>,
    write: &HeartbeatWrite,
) -> Result<HeartbeatDecision, String> {
    if write.seq == 0 {
        return Err(
            "monotonic_seq must be non-zero; client shard descriptors are not applied to routes"
                .to_string(),
        );
    }
    if let Some(previous) = existing {
        if write.seq > previous.seq && write.seq - previous.seq > MAX_HEARTBEAT_SEQ_ADVANCE {
            return Err(format!(
                "heartbeat seq {} jumps {} past {}; the stored record was kept. A heartbeat is not a scheduling epoch",
                write.seq,
                write.seq - previous.seq,
                previous.seq
            ));
        }
    } else if write.seq > MAX_HEARTBEAT_SEQ_ADVANCE {
        return Err(format!(
            "initial heartbeat seq {} exceeds {MAX_HEARTBEAT_SEQ_ADVANCE}; the stored record was kept. A heartbeat is not a scheduling epoch",
            write.seq
        ));
    }
    let stored = StoredHeartbeat {
        node_id: write.node_id,
        seq: write.seq,
        observed_unix_ms: write.observed_unix_ms,
        disk_free_bytes: write.disk_free_bytes,
        reported_shard_ids: write.reported_shard_ids.clone(),
        local_hosted_shard_ids: write.local_hosted_shard_ids.clone(),
    };
    let Some(previous) = existing else {
        return Ok(HeartbeatDecision::Store(stored));
    };
    if previous.node_id != write.node_id {
        return Err(format!(
            "heartbeat map key {} does not match body node {}",
            previous.node_id, write.node_id
        ));
    }
    if write.seq < previous.seq {
        return Err(format!(
            "stale heartbeat seq {} is older than {}; the stored record was kept. Client shard descriptors are not applied to routes",
            write.seq, previous.seq
        ));
    }
    if write.seq == previous.seq {
        if same_body(previous, write) {
            return Ok(HeartbeatDecision::Replay(previous.clone()));
        }
        return Err(format!(
            "heartbeat seq {} conflicts with the stored body; the stored record was kept. Client shard descriptors are not applied to routes",
            write.seq
        ));
    }
    Ok(HeartbeatDecision::Store(stored))
}

/// A new remote sequence inside the interval is rejected. An equal sequence is a
/// replay or a conflict and is not rate-limited here.
pub fn remote_heartbeat_is_too_soon(
    previous_observed_unix_ms: u64,
    now_ms: u64,
    incoming_seq: u64,
    stored_seq: u64,
) -> bool {
    incoming_seq != stored_seq
        && now_ms.saturating_sub(previous_observed_unix_ms) < MIN_REMOTE_HEARTBEAT_INTERVAL_MS
}

fn same_body(stored: &StoredHeartbeat, write: &HeartbeatWrite) -> bool {
    stored.disk_free_bytes == write.disk_free_bytes
        && stored.reported_shard_ids == write.reported_shard_ids
        && stored.local_hosted_shard_ids == write.local_hosted_shard_ids
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(seq: u64, disk: Option<u64>, reported: &[u64]) -> HeartbeatWrite {
        HeartbeatWrite {
            node_id: 7,
            seq,
            observed_unix_ms: 10,
            disk_free_bytes: disk,
            reported_shard_ids: reported.to_vec(),
            local_hosted_shard_ids: Vec::new(),
        }
    }

    #[test]
    fn sequence_rules_keep_the_stored_body() {
        assert!(decide_heartbeat(None, &write(0, None, &[])).is_err());
        let stored = match decide_heartbeat(None, &write(1, Some(5), &[9])).unwrap() {
            HeartbeatDecision::Store(heartbeat) => heartbeat,
            HeartbeatDecision::Replay(_) => panic!("first write stores"),
        };
        assert!(matches!(
            decide_heartbeat(Some(&stored), &write(1, Some(5), &[9])).unwrap(),
            HeartbeatDecision::Replay(_)
        ));
        assert!(decide_heartbeat(Some(&stored), &write(1, Some(6), &[9])).is_err());
        assert!(decide_heartbeat(Some(&stored), &write(1, Some(5), &[8])).is_err());
        let zero_seq = decide_heartbeat(Some(&stored), &write(0, Some(5), &[9])).unwrap_err();
        assert!(zero_seq.contains("non-zero"), "{zero_seq}");
        let conflict = decide_heartbeat(
            Some(&stored),
            &HeartbeatWrite {
                disk_free_bytes: Some(9),
                ..write(1, Some(5), &[9])
            },
        )
        .unwrap_err();
        assert!(conflict.contains("conflicts"), "{conflict}");
        let next = match decide_heartbeat(Some(&stored), &write(2, None, &[4])).unwrap() {
            HeartbeatDecision::Store(heartbeat) => heartbeat,
            HeartbeatDecision::Replay(_) => panic!("higher seq stores"),
        };
        assert_eq!(next.seq, 2);
        assert!(decide_heartbeat(Some(&next), &write(1, None, &[4]))
            .unwrap_err()
            .contains("stale"));
        let poisoned = decide_heartbeat(Some(&next), &write(u64::MAX, None, &[4])).unwrap_err();
        assert!(poisoned.contains("jumps"), "{poisoned}");
        let initial = decide_heartbeat(None, &write(u64::MAX, None, &[])).unwrap_err();
        assert!(initial.contains("exceeds"), "{initial}");
        assert!(remote_heartbeat_is_too_soon(1_000, 1_500, 2, 1));
        assert!(!remote_heartbeat_is_too_soon(1_000, 1_500, 1, 1));
        assert!(!remote_heartbeat_is_too_soon(1_000, 3_000, 2, 1));
        assert!(!heartbeat_is_scheduling_truth());
    }

    #[test]
    fn too_many_shard_annotations_are_rejected() {
        let ids = 0..(MAX_REPORTED_SHARD_IDS as u64 + 2);
        let err = normalize_ids(ids).unwrap_err();
        assert!(err.contains("not applied"));
    }
}
