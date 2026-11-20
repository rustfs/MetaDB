//! Fail-closed leadership gate on one local Raft group.
//!
//! These tests run in one OS process. They do not boot three voters, do not
//! call OpenRaft `trigger().transfer_leader`, do not copy a Jumbo directory
//! onto another machine, and do not start a cloud VM. They are not a 3-VM
//! exercise and they are not production acceptance. Linux is the only
//! supported platform.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use metadb_cluster::jumbo_files::{shard_move_dir, write_move_phase, MOVE_LIVE, MOVE_STAGED};
use metadb_cluster::jumbo_remote::append_handoff;
use metadb_cluster::{
    automatic_leadership_transfer_allowed, capture_mono_ns, liveness_effects, CompositeMetaCache,
    DurableCellManager, DurableIdAllocator, LeadershipTransferAttempt, LivenessDecision,
    RemoteHandoffRecord, TransferRefusal,
};
use metadb_core::HybridLogicalClock;
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use tempfile::tempdir;

async fn open_cell(dir: &Path) -> (Arc<ShardGroupHost>, Arc<DurableCellManager>) {
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let host = Arc::new(ShardGroupHost::open(1, dir.join("be"), cache, clock).unwrap());
    let alloc = Arc::new(DurableIdAllocator::open(dir.join("alloc")).unwrap());
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        dir.join("cell_1"),
        host.clone(),
        "127.0.0.1:1".to_string(),
        true,
        4 * 1024 * 1024 * 1024,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        alloc,
        Arc::new(CompositeMetaCache::new()),
    )
    .await
    .unwrap();
    (host, cell)
}

fn voters(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

fn marker(dir: &Path) -> std::path::PathBuf {
    let path = dir.join("source-shard");
    fs::create_dir_all(&path).unwrap();
    fs::write(path.join("keep"), b"source-bytes").unwrap();
    path
}

/// Observations that would pass the gate if the one fact under test is healthy.
fn open_attempt(source: &Path) -> LeadershipTransferAttempt {
    LeadershipTransferAttempt {
        shard_id: 1,
        target: 2,
        intended_voters: voters(&[1, 2]),
        observed_leader: Some(Some(1)),
        observed_live_voters: Some(voters(&[1, 2])),
        group_transfer_allowed: true,
        hint_live_only: false,
        source_directory: source.to_path_buf(),
    }
}

fn assert_source_unchanged(cell: &DurableCellManager, source: &Path, requests_before: u64) {
    assert_eq!(cell.leadership_transfer_requests(), requests_before);
    assert_eq!(fs::read(source.join("keep")).unwrap(), b"source-bytes");
    assert!(cell.host().shard_dir(1).exists());
    assert!(cell.stored_placements().is_empty());
    assert!(!automatic_leadership_transfer_allowed());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_does_not_run_when_this_process_is_not_leader() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let source = marker(dir.path());
    let voters_before = cell.raft_voter_ids(1).unwrap();
    let leader_before = cell.host().get_group(1).unwrap().current_leader();
    assert_eq!(leader_before, Some(1));
    let requests = cell.leadership_transfer_requests();

    let mut attempt = open_attempt(&source);
    attempt.observed_leader = Some(Some(2));
    attempt.observed_live_voters = Some(voters(&[1, 2, 3]));
    attempt.intended_voters = voters(&[1, 2, 3]);
    attempt.target = 3;
    let outcome = cell.testing_attempt_leadership_transfer(&attempt);

    assert_eq!(outcome.refusal, Some(TransferRefusal::NotLeader));
    assert!(!outcome.transfer_requested);
    assert!(!outcome.openraft_called);
    assert!(!outcome.membership_changed);
    assert!(!outcome.source_directory_deleted);
    assert!(!outcome.invented_leader);
    assert!(outcome.committed.is_none());
    assert_eq!(outcome.leader, Some(2));
    assert_eq!(cell.raft_voter_ids(1).unwrap(), voters_before);
    assert_eq!(
        cell.host().get_group(1).unwrap().current_leader(),
        leader_before
    );
    assert_source_unchanged(&cell, &source, requests);
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_does_not_run_during_jumbo_move_or_remote_handoff() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let source = marker(dir.path());
    let voters_before = cell.raft_voter_ids(1).unwrap();
    let leader_before = cell.host().get_group(1).unwrap().current_leader();
    let root = dir.path().join("dedicated");
    let dest = shard_move_dir(&root, 1);
    fs::create_dir_all(&dest).unwrap();
    write_move_phase(&dest, 1, MOVE_STAGED, &[]).unwrap();
    cell.testing_set_jumbo_dedicated_root(Some(root.clone()));
    let requests = cell.leadership_transfer_requests();
    let staged = cell.testing_attempt_leadership_transfer(&open_attempt(&source));
    assert_eq!(staged.refusal, Some(TransferRefusal::JumboMoveInProgress));
    assert!(!staged.transfer_requested);
    assert!(!staged.openraft_called);
    assert!(!staged.membership_changed);
    assert!(!staged.invented_leader);
    assert_eq!(cell.raft_voter_ids(1).unwrap(), voters_before);

    write_move_phase(&dest, 1, MOVE_LIVE, &[]).unwrap();
    let live = cell.testing_attempt_leadership_transfer(&open_attempt(&source));
    assert_eq!(live.refusal, Some(TransferRefusal::JumboMoveInProgress));
    assert!(!live.transfer_requested);
    assert_eq!(cell.leadership_transfer_requests(), requests);

    cell.testing_set_jumbo_dedicated_root(None);
    append_handoff(
        &dir.path().join("cell_1"),
        RemoteHandoffRecord {
            shard_id: 1,
            dest_node: 2,
            destination_raft_opened: false,
            transfer_leader_called: false,
        },
    )
    .unwrap();
    let handed = cell.testing_attempt_leadership_transfer(&open_attempt(&source));
    assert_eq!(handed.refusal, Some(TransferRefusal::RemoteHandoff));
    assert!(!handed.transfer_requested);
    assert!(!handed.openraft_called);
    assert!(!handed.membership_changed);
    assert!(!handed.source_directory_deleted);
    assert!(!handed.invented_leader);
    assert!(handed.committed.is_none());
    assert_eq!(cell.raft_voter_ids(1).unwrap(), voters_before);
    assert_eq!(
        cell.host().get_group(1).unwrap().current_leader(),
        leader_before
    );
    assert!(source.exists());
    assert_source_unchanged(&cell, &source, requests);
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transfer_does_not_run_while_the_clock_is_untrusted() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let source = marker(dir.path());
    let voters_before = cell.raft_voter_ids(1).unwrap();
    let leader_before = cell.host().get_group(1).unwrap().current_leader();
    cell.testing_set_heartbeat_wall(Some(1));
    let requests = cell.leadership_transfer_requests();
    let outcome = cell.testing_attempt_leadership_transfer(&open_attempt(&source));
    assert_eq!(outcome.refusal, Some(TransferRefusal::UntrustedClock));
    assert!(!outcome.transfer_requested);
    assert!(!outcome.openraft_called);
    assert!(!outcome.membership_changed);
    assert!(!outcome.source_directory_deleted);
    assert!(!outcome.invented_leader);
    assert!(outcome.committed.is_none());
    assert_eq!(outcome.leader, Some(1));
    assert_eq!(cell.raft_voter_ids(1).unwrap(), voters_before);
    assert_eq!(
        cell.host().get_group(1).unwrap().current_leader(),
        leader_before
    );
    cell.testing_set_heartbeat_wall(None);
    let still = cell.testing_attempt_leadership_transfer(&open_attempt(&source));
    assert_eq!(still.refusal, Some(TransferRefusal::UntrustedClock));
    assert_source_unchanged(&cell, &source, requests);
    host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hint_live_and_a_closed_group_do_not_transfer() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let source = marker(dir.path());
    let published = cell.publish_local_heartbeat().unwrap();
    let recv = capture_mono_ns();
    let sent = recv.saturating_sub(1_000_000);
    cell.record_acknowledged_round_trip(1, published.seq, sent, recv);
    let hint = cell.liveness_decision(1);
    assert!(
        matches!(hint, LivenessDecision::HintLive { .. }),
        "{hint:?}"
    );
    assert!(!liveness_effects(&hint).transfer_leader);
    let requests = cell.leadership_transfer_requests();
    cell.consider_automatic_leadership_transfer();
    assert_eq!(cell.leadership_transfer_requests(), requests);

    let mut hinted = open_attempt(&source);
    hinted.hint_live_only = true;
    let outcome = cell.testing_attempt_leadership_transfer(&hinted);
    assert_eq!(outcome.refusal, Some(TransferRefusal::HintLiveOnly));
    assert!(!outcome.transfer_requested);
    assert_eq!(cell.leadership_transfer_requests(), requests);

    let voters_before = cell.raft_voter_ids(1).unwrap();
    host.shutdown_all().await.unwrap();
    let closed = cell.testing_attempt_leadership_transfer(&open_attempt(&source));
    assert_eq!(closed.refusal, Some(TransferRefusal::ClosedSourceGroup));
    assert!(!closed.transfer_requested);
    assert!(!closed.openraft_called);
    assert!(!closed.membership_changed);
    assert!(!closed.invented_leader);
    assert!(cell.host().get_group(1).is_none());
    assert!(cell.host().shard_dir(1).exists());
    assert_eq!(fs::read(source.join("keep")).unwrap(), b"source-bytes");
    assert_eq!(cell.leadership_transfer_requests(), requests);
    assert_eq!(voters_before.into_iter().collect::<Vec<_>>(), vec![1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_permitted_request_does_not_change_membership_or_the_leader() {
    let dir = tempdir().unwrap();
    let (host, cell) = open_cell(dir.path()).await;
    let source = marker(dir.path());
    let voters_before = cell.raft_voter_ids(1).unwrap();
    let leader_before = cell.host().get_group(1).unwrap().current_leader();
    assert_eq!(leader_before, Some(1));
    let requests = cell.leadership_transfer_requests();
    cell.consider_automatic_leadership_transfer();
    assert_eq!(cell.leadership_transfer_requests(), requests);

    let outcome = cell.testing_attempt_leadership_transfer(&open_attempt(&source));
    assert!(outcome.transfer_requested, "{outcome:?}");
    assert!(outcome.refusal.is_none(), "{outcome:?}");
    assert!(!outcome.openraft_called);
    assert!(!outcome.membership_changed);
    assert!(!outcome.source_directory_deleted);
    assert!(!outcome.invented_leader);
    assert!(outcome.committed.is_none());
    assert_eq!(outcome.leader, Some(1));
    assert_eq!(cell.leadership_transfer_requests(), requests + 1);
    assert_eq!(cell.raft_voter_ids(1).unwrap(), voters_before);
    assert_eq!(
        cell.host().get_group(1).unwrap().current_leader(),
        leader_before
    );
    assert!(cell.stored_placements().is_empty());
    assert_eq!(fs::read(source.join("keep")).unwrap(), b"source-bytes");

    let mut mismatch = open_attempt(&source);
    mismatch.intended_voters = voters(&[1, 9]);
    let refused = cell.testing_attempt_leadership_transfer(&mismatch);
    assert_eq!(refused.refusal, Some(TransferRefusal::VoterSetMismatch));
    assert!(!refused.transfer_requested);
    assert_eq!(cell.leadership_transfer_requests(), requests + 1);
    assert_eq!(cell.raft_voter_ids(1).unwrap(), voters_before);
    host.shutdown_all().await.unwrap();
}
