//! Durable cell heartbeats and Jumbo placement records (`CELL_CONTROL_PLANE`).
//!
//! This file is separate from `CELL_MANAGER_STATE`. A heartbeat must not rewrite
//! shard routes, and a placement record must not be confused with a route epoch.
//! The codec is the same CRC32C frame used by the cell state file. Version 1 is
//! the only layout this process reads; a different version fails the open.

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use metadb_core::{InodeId, MetaDbError, NodeId, ShardId};

use crate::jumbo_placement::{CommittedMove, PlacementPhase};

/// How often a process records and, when it is not the leader, sends its own heartbeat.
pub const HEARTBEAT_INTERVAL_MS: u64 = 3_000;

/// Minimum gap between two leadership-transfer or peer-replacement attempts for one shard.
pub const PLACEMENT_RETRY_MS: u64 = 30_000;

pub const CONTROL_PLANE_VERSION: u32 = 1;
pub const CONTROL_PLANE_FILE: &str = "CELL_CONTROL_PLANE";

/// Upper bound on shard ids stored from one heartbeat. The ids are annotations.
pub const MAX_REPORTED_SHARD_IDS: usize = 1_024;

/// Largest forward jump accepted inside one observation generation.
/// A receiver with no row, and a new generation, accept the sender's current
/// sequence even when it is larger. `u64::MAX` still cannot jump an established
/// generation. Heartbeats are not a scheduling epoch.
pub const MAX_HEARTBEAT_SEQ_ADVANCE: u64 = 64;

/// Minimum gap between two remote heartbeats that carry a new sequence.
pub const MIN_REMOTE_HEARTBEAT_INTERVAL_MS: u64 = 1_000;

/// Durable heartbeat rows are not a shard epoch. A measured liveness hint is not
/// one either: it must not schedule splits, membership changes, or leadership transfer.
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
    /// Identity of this process's heartbeat file. Zero means not minted yet.
    /// Losing the file mints a new value. It is not a Raft epoch.
    pub observation_generation: u128,
}

impl Default for PersistedControlPlane {
    fn default() -> Self {
        Self {
            version: CONTROL_PLANE_VERSION,
            heartbeats: BTreeMap::new(),
            next_local_seq: 1,
            placements: BTreeMap::new(),
            observation_generation: 0,
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
    /// Sender heartbeat-file generation. Not a node instance id and not a shard epoch.
    pub observation_generation: u128,
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
    pub observation_generation: u128,
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

pub fn normalize_ids(
    ids: impl IntoIterator<Item = ShardId>,
) -> std::result::Result<Vec<ShardId>, String> {
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
/// No stored row, or a different `observation_generation`, is first contact: any
/// non-zero sequence is the new baseline. That covers a leader that has never
/// stored this sender, a receiver that lost its file, and a sender that lost its
/// file and minted a new generation. The same generation still rejects a sequence
/// older than the stored one, a conflicting body, and a forward jump above
/// [`MAX_HEARTBEAT_SEQ_ADVANCE`]. None of these records schedule placement.
pub fn decide_heartbeat(
    existing: Option<&StoredHeartbeat>,
    write: &HeartbeatWrite,
) -> std::result::Result<HeartbeatDecision, String> {
    if write.seq == 0 {
        return Err(
            "monotonic_seq must be non-zero; client shard descriptors are not applied to routes"
                .to_string(),
        );
    }
    if write.observation_generation == 0 {
        return Err(
            "observation generation must be non-zero; a heartbeat is not a scheduling epoch"
                .to_string(),
        );
    }
    let stored = stored_from_write(write);
    let Some(previous) = existing else {
        return Ok(HeartbeatDecision::Store(stored));
    };
    if previous.node_id != write.node_id {
        return Err(format!(
            "heartbeat map key {} does not match body node {}",
            previous.node_id, write.node_id
        ));
    }
    if previous.observation_generation != write.observation_generation {
        return Ok(HeartbeatDecision::Store(stored));
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
    if write.seq - previous.seq > MAX_HEARTBEAT_SEQ_ADVANCE {
        return Err(format!(
            "heartbeat seq {} jumps {} past {}; the stored record was kept. A heartbeat is not a scheduling epoch",
            write.seq,
            write.seq - previous.seq,
            previous.seq
        ));
    }
    Ok(HeartbeatDecision::Store(stored))
}

fn stored_from_write(write: &HeartbeatWrite) -> StoredHeartbeat {
    StoredHeartbeat {
        node_id: write.node_id,
        seq: write.seq,
        observed_unix_ms: write.observed_unix_ms,
        disk_free_bytes: write.disk_free_bytes,
        reported_shard_ids: write.reported_shard_ids.clone(),
        local_hosted_shard_ids: write.local_hosted_shard_ids.clone(),
        observation_generation: write.observation_generation,
    }
}

/// Every accepted remote heartbeat that changes the stored row is limited,
/// including a same-sequence generation replacement. An identical replay does
/// not call this.
pub fn remote_state_change_is_too_soon(previous_observed_unix_ms: u64, now_ms: u64) -> bool {
    now_ms.saturating_sub(previous_observed_unix_ms) < MIN_REMOTE_HEARTBEAT_INTERVAL_MS
}

/// Reads a `CELL_CONTROL_PLANE` body after the CRC32C trailer has been checked.
///
/// The file frame is `body || crc32c_le`. This function sees only `body` and
/// requires one layout to consume every byte:
///
/// - Current version 1 appends `observation_generation: u128` on the plane and
///   on each stored heartbeat.
/// - The previous version 1 body omits those two fields. It still decodes.
///   Missing generations stay 0 until a sender presents a non-zero generation.
///
/// If both layouts consume the same bytes, the file is rejected. A heartbeat
/// is not a scheduling epoch.
pub fn decode_control_plane(
    body: &[u8],
) -> std::result::Result<PersistedControlPlane, MetaDbError> {
    let current = decode_exact::<PersistedControlPlane>(body);
    let legacy = decode_exact::<LegacyPersistedControlPlane>(body);
    match (current, legacy) {
        (Ok(state), Err(_)) => {
            if state.version != CONTROL_PLANE_VERSION {
                return Err(MetaDbError::CodecError(format!(
                    "{CONTROL_PLANE_FILE} version {} is not supported",
                    state.version
                )));
            }
            Ok(state)
        }
        (Err(_), Ok(legacy)) => {
            if legacy.version != CONTROL_PLANE_VERSION {
                return Err(MetaDbError::CodecError(format!(
                    "{CONTROL_PLANE_FILE} version {} is not supported",
                    legacy.version
                )));
            }
            Ok(upgrade_legacy(legacy))
        }
        (Ok(_), Ok(_)) => Err(MetaDbError::CodecError(format!(
            "{CONTROL_PLANE_FILE} matches both the current and the pre-generation layout"
        ))),
        (Err(error), Err(_)) => Err(MetaDbError::CodecError(format!(
            "{CONTROL_PLANE_FILE}: {error}"
        ))),
    }
}

fn decode_exact<T>(body: &[u8]) -> wincode::ReadResult<T>
where
    T: for<'de> wincode::SchemaRead<'de, wincode::config::DefaultConfig, Dst = T>,
{
    wincode::config::deserialize_exact(body, wincode::config::DefaultConfig::default())
}

fn upgrade_legacy(legacy: LegacyPersistedControlPlane) -> PersistedControlPlane {
    PersistedControlPlane {
        version: CONTROL_PLANE_VERSION,
        heartbeats: legacy
            .heartbeats
            .into_iter()
            .map(|(node_id, heartbeat)| {
                (
                    node_id,
                    StoredHeartbeat {
                        node_id: heartbeat.node_id,
                        seq: heartbeat.seq,
                        observed_unix_ms: heartbeat.observed_unix_ms,
                        disk_free_bytes: heartbeat.disk_free_bytes,
                        reported_shard_ids: heartbeat.reported_shard_ids,
                        local_hosted_shard_ids: heartbeat.local_hosted_shard_ids,
                        observation_generation: 0,
                    },
                )
            })
            .collect(),
        next_local_seq: legacy.next_local_seq,
        placements: legacy.placements,
        observation_generation: 0,
    }
}

#[derive(Debug, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct LegacyStoredHeartbeat {
    node_id: NodeId,
    seq: u64,
    observed_unix_ms: u64,
    disk_free_bytes: Option<u64>,
    reported_shard_ids: Vec<ShardId>,
    local_hosted_shard_ids: Vec<ShardId>,
}

#[derive(Debug, Deserialize, wincode::SchemaRead, wincode::SchemaWrite)]
struct LegacyPersistedControlPlane {
    version: u32,
    heartbeats: BTreeMap<NodeId, LegacyStoredHeartbeat>,
    next_local_seq: u64,
    placements: BTreeMap<ShardId, StoredPlacement>,
}

fn same_body(stored: &StoredHeartbeat, write: &HeartbeatWrite) -> bool {
    stored.disk_free_bytes == write.disk_free_bytes
        && stored.reported_shard_ids == write.reported_shard_ids
        && stored.local_hosted_shard_ids == write.local_hosted_shard_ids
}

#[cfg(test)]
mod tests {
    use super::*;
    use metadb_core::codec;

    fn write(seq: u64, disk: Option<u64>, reported: &[u64]) -> HeartbeatWrite {
        HeartbeatWrite {
            node_id: 7,
            seq,
            observed_unix_ms: 10,
            disk_free_bytes: disk,
            reported_shard_ids: reported.to_vec(),
            local_hosted_shard_ids: Vec::new(),
            observation_generation: 11,
        }
    }

    #[test]
    fn sequence_rules_keep_the_stored_body() {
        assert!(decide_heartbeat(None, &write(0, None, &[])).is_err());
        let missing_generation = HeartbeatWrite {
            observation_generation: 0,
            ..write(1, None, &[])
        };
        assert!(decide_heartbeat(None, &missing_generation)
            .unwrap_err()
            .contains("generation"));
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
        let first_high = match decide_heartbeat(None, &write(65, None, &[])).unwrap() {
            HeartbeatDecision::Store(heartbeat) => heartbeat,
            HeartbeatDecision::Replay(_) => panic!("empty receiver accepts seq 65"),
        };
        assert_eq!(first_high.seq, 65);
        let restarted = HeartbeatWrite {
            observation_generation: 22,
            seq: 1,
            ..write(1, None, &[])
        };
        let recovered = match decide_heartbeat(Some(&first_high), &restarted).unwrap() {
            HeartbeatDecision::Store(heartbeat) => heartbeat,
            HeartbeatDecision::Replay(_) => panic!("new generation is a new baseline"),
        };
        assert_eq!(recovered.seq, 1);
        assert_eq!(recovered.observation_generation, 22);
        let same_again = decide_heartbeat(Some(&recovered), &restarted).unwrap();
        assert!(matches!(same_again, HeartbeatDecision::Replay(_)));
        let conflict = HeartbeatWrite {
            disk_free_bytes: Some(3),
            ..restarted
        };
        assert!(decide_heartbeat(Some(&recovered), &conflict)
            .unwrap_err()
            .contains("conflicts"));
        assert!(remote_state_change_is_too_soon(1_000, 1_500));
        assert!(!remote_state_change_is_too_soon(1_000, 3_000));
        assert!(!heartbeat_is_scheduling_truth());
    }

    #[test]
    fn control_plane_layouts_consume_exactly_one_body() {
        let mut heartbeats = BTreeMap::new();
        heartbeats.insert(
            1,
            StoredHeartbeat {
                node_id: 1,
                seq: 80,
                observed_unix_ms: 9,
                disk_free_bytes: None,
                reported_shard_ids: Vec::new(),
                local_hosted_shard_ids: vec![1],
                observation_generation: 7,
            },
        );
        let current = PersistedControlPlane {
            version: CONTROL_PLANE_VERSION,
            heartbeats,
            next_local_seq: 81,
            placements: BTreeMap::new(),
            observation_generation: 7,
        };
        let bytes = codec::serialize(&current).unwrap();
        assert_eq!(decode_control_plane(&bytes).unwrap(), current);
        assert!(decode_exact::<LegacyPersistedControlPlane>(&bytes).is_err());

        let legacy = LegacyPersistedControlPlane {
            version: CONTROL_PLANE_VERSION,
            heartbeats: BTreeMap::from([(
                1,
                LegacyStoredHeartbeat {
                    node_id: 1,
                    seq: 4,
                    observed_unix_ms: 3,
                    disk_free_bytes: Some(8),
                    reported_shard_ids: Vec::new(),
                    local_hosted_shard_ids: Vec::new(),
                },
            )]),
            next_local_seq: 5,
            placements: BTreeMap::new(),
        };
        let legacy_bytes = codec::serialize(&legacy).unwrap();
        let upgraded = decode_control_plane(&legacy_bytes).unwrap();
        assert_eq!(upgraded.observation_generation, 0);
        assert_eq!(upgraded.heartbeats[&1].seq, 4);
        assert_eq!(upgraded.heartbeats[&1].observation_generation, 0);
        assert!(decode_exact::<PersistedControlPlane>(&legacy_bytes).is_err());

        let mut extra = bytes.clone();
        extra.push(0xAA);
        assert!(decode_control_plane(&extra).is_err());
    }

    #[test]
    fn too_many_shard_annotations_are_rejected() {
        let ids = 0..(MAX_REPORTED_SHARD_IDS as u64 + 2);
        let err = normalize_ids(ids).unwrap_err();
        assert!(err.contains("not applied"));
    }
}
