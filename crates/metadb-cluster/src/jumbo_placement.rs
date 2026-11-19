//! Membership-safe Jumbo placement decisions.
//!
//! `JumboIsolated` on a route is a detection flag. It does not by itself move a replica.
//! This module plans a same-size voter replacement that keeps Raft fault tolerance
//! and an old-majority overlap. Automatic leadership transfer is not planned:
//! OpenRaft disables the leader lease before peers answer `TransferLeader`.
//!
//! A plan that cannot do either is `Blocked` or `LeaderOnDedicated` with a reason.
//! Neither of those is a completed eviction.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use metadb_core::{InodeId, NodeId, ShardId};

/// Current Raft view of one shard. Built from live metrics, not from a client heartbeat.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ShardVoters {
    pub shard_id: ShardId,
    pub voters: BTreeSet<NodeId>,
    pub leader: Option<NodeId>,
}

/// Inputs to [`plan_jumbo_placement`]. Pure data; no I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementObservation {
    pub jumbo_shard_id: ShardId,
    pub dominant_pid: InodeId,
    pub dedicated_node: NodeId,
    pub local_node: NodeId,
    pub shards: Vec<ShardVoters>,
    /// Nodes that are already voters of at least one shard observed here.
    pub proven_voters: BTreeSet<NodeId>,
    /// Dedicated nodes of other jumbo shards. They are not used as drain spares.
    pub reserved_dedicated_nodes: BTreeSet<NodeId>,
}

/// OpenRaft 0.10 `trigger().transfer_leader` marks the transfer and disables the
/// leader lease before any peer answers `TransferLeader`. Older peers do not
/// implement that RPC. Registration in the node map is not proof that they do.
/// Automatic transfer stays off until a check that does not touch the lease can
/// show every current voter implements it.
pub const AUTOMATIC_LEADERSHIP_TRANSFER_ENABLED: bool = false;

pub const AUTOMATIC_TRANSFER_DISABLED_REASON: &str = "automatic leadership transfer is disabled; peer TransferLeader capability is unknown, and OpenRaft disables the leader lease before the RPC is known to succeed. Node registration is not that capability";

pub fn automatic_leadership_transfer_allowed() -> bool {
    AUTOMATIC_LEADERSHIP_TRANSFER_ENABLED
}

/// A transfer poll shorter than the configured election timeout can return while
/// OpenRaft is still rejecting writes. Pull request #38 (`f55cc6cf`) loads
/// `election_timeout_max_ms`. This process does not start that poll.
pub fn transfer_poll_covers_election(poll_ms: u64, election_timeout_max_ms: u64) -> bool {
    election_timeout_max_ms > 0 && poll_ms >= election_timeout_max_ms
}

/// `committed_moves` records only a transfer this process initiated and then
/// observed on the target. An already-correct leader is observation, not our move.
pub fn leadership_move_recorded(initiated_transfer: bool, target_became_leader: bool) -> bool {
    initiated_transfer && target_became_leader
}

/// Drops leadership transfer unless [`automatic_leadership_transfer_allowed`] is true.
pub fn executable_placement_actions(actions: &[PlacementAction]) -> Vec<PlacementAction> {
    actions
        .iter()
        .filter(|action| match action {
            PlacementAction::TransferLeadership { .. } => automatic_leadership_transfer_allowed(),
            PlacementAction::ReplaceDedicatedOnPeer { .. } => true,
        })
        .cloned()
        .collect()
}

/// One Raft action the executor may attempt. The planner never emits an action that
/// drops fault tolerance or addresses a node that has not already joined a shard.
/// `TransferLeadership` is not executable while automatic transfer is disabled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlacementAction {
    TransferLeadership {
        shard_id: ShardId,
        to: NodeId,
    },
    ReplaceDedicatedOnPeer {
        shard_id: ShardId,
        remove: NodeId,
        add: NodeId,
    },
}

/// A move this process recorded only after its own Raft call reached the postcondition.
/// Variant order is part of the `CELL_CONTROL_PLANE` layout.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub enum CommittedMove {
    LeadershipTransferred {
        shard_id: ShardId,
        to: NodeId,
    },
    VoterReplaced {
        shard_id: ShardId,
        removed: NodeId,
        added: NodeId,
    },
}

/// Durable phase. This describes the live membership condition, not a claim that
/// this process moved bytes. Variant order is part of the `CELL_CONTROL_PLANE` layout.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub enum PlacementPhase {
    /// The dedicated node is a voter, and leadership or a peer replacement is still outstanding.
    Recorded,
    /// The jumbo shard's current leader is the dedicated node. Peer shards may still list it.
    LeaderOnDedicated,
    /// At least one other local shard exists, and none of them currently has the dedicated node as a voter.
    DedicatedNodeNotShared,
    /// A required move was refused. No success is implied.
    Blocked,
}

/// What the executor should attempt, and the phase to publish if it attempts nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementDecision {
    pub phase: PlacementPhase,
    pub blocked_reason: String,
    pub actions: Vec<PlacementAction>,
}

/// Why a jumbo-driven voter-set change is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MembershipRefusal {
    EmptyTarget,
    /// The overlap is not a majority of the old voter set, so joint consensus can stall
    /// even when the old majority is up.
    LostOldMajorityOverlap {
        overlap: usize,
        old_majority: usize,
    },
    FaultToleranceDropped {
        previous: usize,
        next: usize,
    },
}

/// Voters that can fail and still leave a strict majority. `0` for one or two voters.
pub fn fault_tolerance(voter_count: usize) -> usize {
    if voter_count == 0 {
        0
    } else {
        voter_count - (voter_count / 2 + 1)
    }
}

/// Jumbo placement may change a voter set only when the new set is non-empty, fault
/// tolerance does not fall, and the overlap contains a majority of the old set.
pub fn jumbo_membership_change_allowed(
    old: &BTreeSet<NodeId>,
    new: &BTreeSet<NodeId>,
) -> Result<(), MembershipRefusal> {
    if new.is_empty() {
        return Err(MembershipRefusal::EmptyTarget);
    }
    let previous = fault_tolerance(old.len());
    let next = fault_tolerance(new.len());
    if next < previous {
        return Err(MembershipRefusal::FaultToleranceDropped { previous, next });
    }
    let old_majority = if old.is_empty() { 1 } else { old.len() / 2 + 1 };
    let overlap = old.intersection(new).count();
    if overlap < old_majority {
        return Err(MembershipRefusal::LostOldMajorityOverlap {
            overlap,
            old_majority,
        });
    }
    Ok(())
}

pub fn plan_jumbo_placement(obs: &PlacementObservation) -> PlacementDecision {
    let Some(jumbo) = obs
        .shards
        .iter()
        .find(|shard| shard.shard_id == obs.jumbo_shard_id)
    else {
        return blocked(
            "jumbo shard is not hosted on this process; no membership change was proposed",
        );
    };
    if !jumbo.voters.contains(&obs.dedicated_node) {
        return blocked(format!(
            "dedicated node {} is not a current voter of shard {}; this increment does not add a voter that has not already joined a shard",
            obs.dedicated_node, obs.jumbo_shard_id
        ));
    }

    let mut actions = Vec::new();
    let leader_here = jumbo.leader == Some(obs.dedicated_node);
    if !leader_here && jumbo.leader.is_none() {
        return blocked("jumbo shard has no current leader; transfer_leader was not called");
    }

    let mut reasons = Vec::new();
    let mut other_shards = 0usize;
    for shard in &obs.shards {
        if shard.shard_id == obs.jumbo_shard_id {
            continue;
        }
        other_shards += 1;
        if !shard.voters.contains(&obs.dedicated_node) {
            continue;
        }
        match replacement_spare(shard, obs) {
            Ok(add) => actions.push(PlacementAction::ReplaceDedicatedOnPeer {
                shard_id: shard.shard_id,
                remove: obs.dedicated_node,
                add,
            }),
            Err(reason) => reasons.push(reason),
        }
    }

    if other_shards == 0 {
        reasons.push(
            "no other local shard exists; this record is not evidence that a co-located shard was moved"
                .to_string(),
        );
    }
    if !leader_here {
        reasons.push(AUTOMATIC_TRANSFER_DISABLED_REASON.to_string());
    }

    let replace_pending = actions
        .iter()
        .any(|action| matches!(action, PlacementAction::ReplaceDedicatedOnPeer { .. }));
    let phase = if !leader_here {
        PlacementPhase::Blocked
    } else if reasons.is_empty() && other_shards > 0 && !replace_pending {
        PlacementPhase::DedicatedNodeNotShared
    } else if leader_here {
        PlacementPhase::LeaderOnDedicated
    } else {
        PlacementPhase::Blocked
    };

    PlacementDecision {
        phase,
        blocked_reason: reasons.join("; "),
        actions,
    }
}

fn replacement_spare(shard: &ShardVoters, obs: &PlacementObservation) -> Result<NodeId, String> {
    let mut candidates: Vec<NodeId> = obs
        .proven_voters
        .iter()
        .copied()
        .filter(|node| {
            *node != obs.dedicated_node
                && !shard.voters.contains(node)
                && !obs.reserved_dedicated_nodes.contains(node)
        })
        .collect();
    candidates.sort_unstable();
    let mut last_refusal = None;
    for add in candidates {
        let mut proposed = shard.voters.clone();
        proposed.remove(&obs.dedicated_node);
        proposed.insert(add);
        match jumbo_membership_change_allowed(&shard.voters, &proposed) {
            Ok(()) => return Ok(add),
            Err(refusal) => last_refusal = Some(refusal),
        }
    }
    let detail = match last_refusal {
        Some(MembershipRefusal::FaultToleranceDropped { previous, next }) => {
            format!("last candidate would drop fault tolerance from {previous} to {next}")
        }
        Some(MembershipRefusal::LostOldMajorityOverlap {
            overlap,
            old_majority,
        }) => format!("last candidate overlap {overlap} is below the old majority {old_majority}"),
        Some(MembershipRefusal::EmptyTarget) => "candidate produced an empty voter set".to_string(),
        None => "no proven voter is outside this shard's voter set".to_string(),
    };
    Err(format!(
        "shard {} cannot move dedicated node {} off its voter set without dropping Raft fault tolerance or losing an old majority ({detail})",
        shard.shard_id, obs.dedicated_node
    ))
}

fn blocked(reason: impl Into<String>) -> PlacementDecision {
    PlacementDecision {
        phase: PlacementPhase::Blocked,
        blocked_reason: reason.into(),
        actions: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn voters(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    fn shard(id: u64, ids: &[u64], leader: Option<u64>) -> ShardVoters {
        ShardVoters {
            shard_id: id,
            voters: voters(ids),
            leader,
        }
    }

    fn obs(dedicated: u64, shards: Vec<ShardVoters>, proven: &[u64]) -> PlacementObservation {
        PlacementObservation {
            jumbo_shard_id: 1,
            dominant_pid: 800,
            dedicated_node: dedicated,
            local_node: 1,
            shards,
            proven_voters: voters(proven),
            reserved_dedicated_nodes: BTreeSet::new(),
        }
    }

    #[test]
    fn single_voter_cell_does_not_plan_a_membership_change() {
        let decision = plan_jumbo_placement(&obs(
            1,
            vec![
                shard(1, &[1], Some(1)),
                shard(2, &[1], Some(1)),
                shard(3, &[1], Some(1)),
            ],
            &[1],
        ));
        assert_eq!(decision.phase, PlacementPhase::LeaderOnDedicated);
        assert!(decision.actions.is_empty(), "{decision:?}");
        assert!(
            decision.blocked_reason.contains("fault tolerance")
                || decision.blocked_reason.contains("proven voter"),
            "{}",
            decision.blocked_reason
        );
    }

    #[test]
    fn a_registered_non_voter_is_not_added() {
        let decision = plan_jumbo_placement(&obs(
            3,
            vec![shard(1, &[1], Some(1)), shard(2, &[1], Some(1))],
            &[1],
        ));
        assert_eq!(decision.phase, PlacementPhase::Blocked);
        assert!(decision.actions.is_empty(), "{decision:?}");
        assert!(
            decision.blocked_reason.contains("not a current voter"),
            "{}",
            decision.blocked_reason
        );
    }

    #[test]
    fn uniform_three_voters_do_not_transfer_or_shrink() {
        let decision = plan_jumbo_placement(&obs(
            2,
            vec![shard(1, &[1, 2, 3], Some(1)), shard(2, &[1, 2, 3], Some(1))],
            &[1, 2, 3],
        ));
        assert_eq!(decision.phase, PlacementPhase::Blocked);
        assert!(
            decision
                .actions
                .iter()
                .all(|action| !matches!(action, PlacementAction::TransferLeadership { .. })),
            "{decision:?}"
        );
        assert!(executable_placement_actions(&decision.actions).is_empty());
        assert!(
            decision
                .blocked_reason
                .contains("automatic leadership transfer is disabled"),
            "{}",
            decision.blocked_reason
        );
        assert!(!automatic_leadership_transfer_allowed());
        assert!(!leadership_move_recorded(false, true));
        assert!(leadership_move_recorded(true, true));
        // #38 can set election_timeout_max_ms above a 3s poll. Covering the
        // timeout is not enough to enable transfer.
        assert!(!transfer_poll_covers_election(3_000, 5_000));
        assert!(transfer_poll_covers_election(5_000, 5_000));
        let forced = vec![PlacementAction::TransferLeadership { shard_id: 1, to: 2 }];
        assert!(executable_placement_actions(&forced).is_empty());
    }

    #[test]
    fn a_proven_spare_keeps_the_same_fault_tolerance() {
        let decision = plan_jumbo_placement(&obs(
            2,
            vec![shard(1, &[1, 2, 3], Some(2)), shard(2, &[1, 2, 3], Some(1))],
            &[1, 2, 3, 4],
        ));
        assert_eq!(decision.phase, PlacementPhase::LeaderOnDedicated);
        assert_eq!(
            decision.actions,
            vec![PlacementAction::ReplaceDedicatedOnPeer {
                shard_id: 2,
                remove: 2,
                add: 4
            }]
        );
        let mut proposed = voters(&[1, 2, 3]);
        proposed.remove(&2);
        proposed.insert(4);
        assert!(jumbo_membership_change_allowed(&voters(&[1, 2, 3]), &proposed).is_ok());
    }

    #[test]
    fn shrinking_and_disjoint_replacement_are_refused() {
        let shrink = jumbo_membership_change_allowed(&voters(&[1, 2, 3]), &voters(&[1, 2]));
        assert!(matches!(
            shrink,
            Err(MembershipRefusal::FaultToleranceDropped {
                previous: 1,
                next: 0
            })
        ));
        let disjoint = jumbo_membership_change_allowed(&voters(&[1]), &voters(&[2]));
        assert!(matches!(
            disjoint,
            Err(MembershipRefusal::LostOldMajorityOverlap { .. })
        ));
        assert!(jumbo_membership_change_allowed(&voters(&[]), &BTreeSet::new()).is_err());
    }

    #[test]
    fn one_shard_is_not_reported_as_unshared() {
        let decision = plan_jumbo_placement(&obs(1, vec![shard(1, &[1], Some(1))], &[1]));
        assert_eq!(decision.phase, PlacementPhase::LeaderOnDedicated);
        assert_ne!(decision.phase, PlacementPhase::DedicatedNodeNotShared);
        assert!(decision.blocked_reason.contains("no other local shard"));
    }

    #[test]
    fn already_unshared_membership_has_no_actions() {
        let decision = plan_jumbo_placement(&obs(
            2,
            vec![shard(1, &[2, 3, 4], Some(2)), shard(2, &[1, 3, 4], Some(1))],
            &[1, 2, 3, 4],
        ));
        assert_eq!(decision.phase, PlacementPhase::DedicatedNodeNotShared);
        assert!(decision.actions.is_empty(), "{decision:?}");
        assert!(
            decision.blocked_reason.is_empty(),
            "{}",
            decision.blocked_reason
        );
    }

    #[test]
    fn another_jumbo_node_is_not_used_as_the_spare() {
        let mut observation = obs(
            2,
            vec![shard(1, &[1, 2, 3], Some(2)), shard(2, &[1, 2, 3], Some(1))],
            &[1, 2, 3, 4],
        );
        observation.reserved_dedicated_nodes.insert(4);
        let decision = plan_jumbo_placement(&observation);
        assert!(decision.actions.is_empty(), "{decision:?}");
        assert_eq!(decision.phase, PlacementPhase::LeaderOnDedicated);
    }
}
