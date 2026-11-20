//! Fail-closed gate for an automatic `transfer_leader` request.
//!
//! The maintenance loop may enter this path only when this process is already
//! the leader of that group, the live voter set is the set the caller intends
//! to transfer within, and a transfer is allowed for the group. Every other
//! input refuses. A refusal does not change membership, delete a source
//! directory, or invent a leader.
//!
//! A [`crate::LivenessDecision::HintLive`] sample is not an input. OpenRaft
//! `trigger().transfer_leader` disables the leader lease before any peer
//! answers, and [`crate::automatic_leadership_transfer_allowed`] stays false
//! until a check that does not touch the lease can prove every current voter
//! implements `TransferLeader`. This gate therefore does not call that method.
//!
//! This is not a cross-host Jumbo copy, not a 3-VM exercise, and not production
//! acceptance. Linux is the only supported platform. The transport stays TCP.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use metadb_core::{NodeId, ShardId};

use crate::jumbo_placement::{leadership_move_recorded, CommittedMove};

/// Why [`run_automatic_leadership_transfer`] did not request a transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferRefusal {
    /// The caller has a liveness hint and no independent transfer authority.
    HintLiveOnly,
    UntrustedClock,
    /// `MOVE_STAGED`, `MOVE_LIVE`, or a handoff is in progress for this shard.
    JumboMoveInProgress,
    /// `JUMBO_REMOTE_HANDOFF` is recorded for this shard.
    RemoteHandoff,
    /// The source Raft group is closed, missing, or its voter set cannot be read.
    ClosedSourceGroup,
    /// This process is not the current leader of the live voter set.
    NotLeader,
    /// The live voter set is not the set the caller intends to transfer within.
    VoterSetMismatch,
    /// The group is not allowed to transfer. The production flag is false.
    TransferNotAllowed,
    /// The target is not a member of the live voter set.
    TargetNotLiveVoter,
    /// The target is already this process. There is no other voter to transfer to.
    TargetIsCurrentLeader,
}

/// One shard the cell may evaluate. Maintenance leaves both observations unset
/// and reads Raft. Tests set them to describe a voter set this process is not
/// running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipTransferAttempt {
    pub shard_id: ShardId,
    pub target: NodeId,
    pub intended_voters: BTreeSet<NodeId>,
    pub observed_leader: Option<Option<NodeId>>,
    pub observed_live_voters: Option<BTreeSet<NodeId>>,
    pub group_transfer_allowed: bool,
    pub hint_live_only: bool,
    pub source_directory: PathBuf,
}

/// Facts the gate reads. The caller fills them. This function does no I/O.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(clippy::struct_excessive_bools)]
pub struct LeadershipTransferInput {
    pub shard_id: ShardId,
    pub local_node: NodeId,
    pub current_leader: Option<NodeId>,
    pub live_voters: BTreeSet<NodeId>,
    pub intended_voters: BTreeSet<NodeId>,
    pub target: NodeId,
    /// Production passes [`crate::automatic_leadership_transfer_allowed`].
    pub group_transfer_allowed: bool,
    pub clock_trusted: bool,
    /// Visible `MOVE_STAGED` / `MOVE_LIVE`, or a handoff that has not finished.
    pub jumbo_move_in_progress: bool,
    /// A `JUMBO_REMOTE_HANDOFF` record names this shard.
    pub remote_handoff: bool,
    pub source_group_open: bool,
    /// True when the only reason the caller asked is a `HintLive` sample.
    pub hint_live_only: bool,
}

/// What the transfer callback observed. A refusal never builds this.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferObservation {
    /// `trigger().transfer_leader` was entered. This build keeps it false.
    pub openraft_called: bool,
    /// The target was observed as leader after that call. Not a value we wrote.
    pub target_became_leader: bool,
}

/// Result of one gate evaluation. Refusal and an incomplete request share the
/// same side-effect flags.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeadershipTransferOutcome {
    pub transfer_requested: bool,
    pub openraft_called: bool,
    pub membership_changed: bool,
    pub source_directory_deleted: bool,
    /// Raft reported a different leader although this call did not observe one.
    pub invented_leader: bool,
    /// Leader to report. A refusal or an incomplete request keeps `current_leader`.
    pub leader: Option<NodeId>,
    pub refusal: Option<TransferRefusal>,
    /// Set only when a transfer was initiated and the target was then observed.
    pub committed: Option<CommittedMove>,
}

/// `Some` when a transfer must not be requested.
pub fn leadership_transfer_refusal(input: &LeadershipTransferInput) -> Option<TransferRefusal> {
    if input.hint_live_only {
        return Some(TransferRefusal::HintLiveOnly);
    }
    if !input.clock_trusted {
        return Some(TransferRefusal::UntrustedClock);
    }
    if input.jumbo_move_in_progress {
        return Some(TransferRefusal::JumboMoveInProgress);
    }
    if input.remote_handoff {
        return Some(TransferRefusal::RemoteHandoff);
    }
    if !input.source_group_open {
        return Some(TransferRefusal::ClosedSourceGroup);
    }
    if input.current_leader != Some(input.local_node)
        || !input.live_voters.contains(&input.local_node)
    {
        return Some(TransferRefusal::NotLeader);
    }
    if input.intended_voters.is_empty() || input.live_voters != input.intended_voters {
        return Some(TransferRefusal::VoterSetMismatch);
    }
    if !input.group_transfer_allowed {
        return Some(TransferRefusal::TransferNotAllowed);
    }
    if !input.live_voters.contains(&input.target) {
        return Some(TransferRefusal::TargetNotLiveVoter);
    }
    if input.target == input.local_node {
        return Some(TransferRefusal::TargetIsCurrentLeader);
    }
    None
}

/// Evaluate one transfer. `invoke` runs only when every gate passes.
///
/// `source_directory` is not removed. Membership is not changed here. The
/// reported leader stays `current_leader` unless `invoke` reports that OpenRaft
/// was entered and the target was then observed as leader.
pub fn run_automatic_leadership_transfer(
    input: &LeadershipTransferInput,
    source_directory: &Path,
    invoke: impl FnOnce(NodeId) -> TransferObservation,
) -> LeadershipTransferOutcome {
    let source_existed = source_directory.exists();
    if let Some(refusal) = leadership_transfer_refusal(input) {
        return refused(input, refusal);
    }
    let observation = invoke(input.target);
    let recorded = leadership_move_recorded(
        observation.openraft_called,
        observation.target_became_leader,
    );
    let leader = if recorded {
        Some(input.target)
    } else {
        input.current_leader
    };
    let committed = recorded.then_some(CommittedMove::LeadershipTransferred {
        shard_id: input.shard_id,
        to: input.target,
    });
    LeadershipTransferOutcome {
        transfer_requested: true,
        openraft_called: observation.openraft_called,
        membership_changed: false,
        source_directory_deleted: source_existed && !source_directory.exists(),
        invented_leader: false,
        leader,
        refusal: None,
        committed,
    }
}

fn refused(input: &LeadershipTransferInput, refusal: TransferRefusal) -> LeadershipTransferOutcome {
    LeadershipTransferOutcome {
        transfer_requested: false,
        openraft_called: false,
        membership_changed: false,
        source_directory_deleted: false,
        invented_leader: false,
        leader: input.current_leader,
        refusal: Some(refusal),
        committed: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::heartbeat_scheduler::{liveness_effects, LivenessDecision};
    use crate::jumbo_placement::automatic_leadership_transfer_allowed;

    fn voters(ids: &[u64]) -> BTreeSet<u64> {
        ids.iter().copied().collect()
    }

    fn permitted() -> LeadershipTransferInput {
        LeadershipTransferInput {
            shard_id: 7,
            local_node: 1,
            current_leader: Some(1),
            live_voters: voters(&[1, 2, 3]),
            intended_voters: voters(&[1, 2, 3]),
            target: 2,
            group_transfer_allowed: true,
            clock_trusted: true,
            jumbo_move_in_progress: false,
            remote_handoff: false,
            source_group_open: true,
            hint_live_only: false,
        }
    }

    fn run(
        input: &LeadershipTransferInput,
        observation: TransferObservation,
    ) -> (LeadershipTransferOutcome, u32) {
        let dir = tempfile::tempdir().unwrap();
        let marker = dir.path().join("source-marker");
        std::fs::write(&marker, b"keep").unwrap();
        let mut calls = 0u32;
        let outcome = run_automatic_leadership_transfer(input, dir.path(), |_| {
            calls += 1;
            observation
        });
        assert_eq!(std::fs::read(&marker).unwrap(), b"keep");
        assert!(dir.path().exists());
        assert!(!outcome.membership_changed, "{outcome:?}");
        assert!(!outcome.source_directory_deleted, "{outcome:?}");
        assert!(!outcome.invented_leader, "{outcome:?}");
        (outcome, calls)
    }

    #[test]
    fn transfer_does_not_run_when_this_process_is_not_leader() {
        let mut input = permitted();
        input.current_leader = Some(2);
        let (outcome, calls) = run(
            &input,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert!(!outcome.transfer_requested);
        assert!(!outcome.openraft_called);
        assert_eq!(outcome.refusal, Some(TransferRefusal::NotLeader));
        assert_eq!(outcome.leader, Some(2));
        assert!(outcome.committed.is_none());

        input.current_leader = None;
        let (missing, calls) = run(
            &input,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert_eq!(missing.refusal, Some(TransferRefusal::NotLeader));
        assert_eq!(missing.leader, None);
    }

    #[test]
    fn transfer_does_not_run_while_a_jumbo_handoff_is_in_progress() {
        let mut staged = permitted();
        staged.jumbo_move_in_progress = true;
        let (outcome, calls) = run(
            &staged,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert_eq!(outcome.refusal, Some(TransferRefusal::JumboMoveInProgress));
        assert_eq!(outcome.leader, Some(1));
        assert!(outcome.committed.is_none());

        let mut handed = permitted();
        handed.remote_handoff = true;
        let (remote, calls) = run(
            &handed,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert_eq!(remote.refusal, Some(TransferRefusal::RemoteHandoff));

        let mut closed = permitted();
        closed.source_group_open = false;
        let (shut, calls) = run(
            &closed,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert_eq!(shut.refusal, Some(TransferRefusal::ClosedSourceGroup));
        assert!(!shut.transfer_requested);
    }

    #[test]
    fn transfer_does_not_run_while_the_clock_is_untrusted() {
        let mut input = permitted();
        input.clock_trusted = false;
        let (outcome, calls) = run(
            &input,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert!(!outcome.transfer_requested);
        assert!(!outcome.openraft_called);
        assert_eq!(outcome.refusal, Some(TransferRefusal::UntrustedClock));
        assert_eq!(outcome.leader, Some(1));
        assert!(outcome.committed.is_none());
        assert!(!outcome.membership_changed);
        assert!(!outcome.source_directory_deleted);
    }

    #[test]
    fn a_hint_live_sample_does_not_authorize_transfer() {
        let mut input = permitted();
        input.hint_live_only = true;
        let (outcome, calls) = run(
            &input,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert_eq!(outcome.refusal, Some(TransferRefusal::HintLiveOnly));
        let hint = LivenessDecision::HintLive {
            node_id: 2,
            seq: 1,
            rtt_ns: Some(10),
            age_ms: 1,
        };
        assert!(!liveness_effects(&hint).transfer_leader);
        assert!(!automatic_leadership_transfer_allowed());
    }

    #[test]
    fn the_live_set_and_the_target_must_match_the_request() {
        let mut mismatch = permitted();
        mismatch.intended_voters = voters(&[1, 2, 4]);
        let (outcome, calls) = run(
            &mismatch,
            TransferObservation {
                openraft_called: false,
                target_became_leader: false,
            },
        );
        assert_eq!(calls, 0);
        assert_eq!(outcome.refusal, Some(TransferRefusal::VoterSetMismatch));

        let mut stranger = permitted();
        stranger.target = 9;
        let (missing, calls) = run(
            &stranger,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert_eq!(missing.refusal, Some(TransferRefusal::TargetNotLiveVoter));

        let mut itself = permitted();
        itself.target = 1;
        let (self_target, calls) = run(
            &itself,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert_eq!(
            self_target.refusal,
            Some(TransferRefusal::TargetIsCurrentLeader)
        );

        let mut disabled = permitted();
        disabled.group_transfer_allowed = false;
        let (off, calls) = run(
            &disabled,
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(calls, 0);
        assert_eq!(off.refusal, Some(TransferRefusal::TransferNotAllowed));
        assert!(!automatic_leadership_transfer_allowed());
    }

    #[test]
    fn a_permitted_request_does_not_invent_a_leader_or_a_membership_change() {
        let (outcome, calls) = run(
            &permitted(),
            TransferObservation {
                openraft_called: false,
                target_became_leader: false,
            },
        );
        assert_eq!(calls, 1);
        assert!(outcome.transfer_requested);
        assert!(!outcome.openraft_called);
        assert_eq!(outcome.leader, Some(1));
        assert!(outcome.committed.is_none());
        assert!(outcome.refusal.is_none());

        let (incomplete, calls) = run(
            &permitted(),
            TransferObservation {
                openraft_called: true,
                target_became_leader: false,
            },
        );
        assert_eq!(calls, 1);
        assert!(incomplete.openraft_called);
        assert_eq!(
            incomplete.leader,
            Some(1),
            "an incomplete transfer keeps the leader"
        );
        assert!(incomplete.committed.is_none());
        assert!(!incomplete.membership_changed);

        let (unobserved, _) = run(
            &permitted(),
            TransferObservation {
                openraft_called: false,
                target_became_leader: true,
            },
        );
        assert_eq!(unobserved.leader, Some(1));
        assert!(unobserved.committed.is_none());

        let (observed, _) = run(
            &permitted(),
            TransferObservation {
                openraft_called: true,
                target_became_leader: true,
            },
        );
        assert_eq!(observed.leader, Some(2));
        assert_eq!(
            observed.committed,
            Some(CommittedMove::LeadershipTransferred { shard_id: 7, to: 2 })
        );
        assert!(!observed.membership_changed);
        assert!(!observed.source_directory_deleted);
        assert!(!observed.invented_leader);
    }
}
