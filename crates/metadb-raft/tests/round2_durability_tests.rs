//! Round 2 audit regressions. These assertions describe the required recovery
//! contract under missing or inconsistent durable state.

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use metadb_core::{HlcTimestamp, HybridLogicalClock, ShardEpoch};
use metadb_raft::{
    BasicKvApplyHandler, BasicKvCommand, ChannelPool, MetaDbRaftRequest, MetaDbTypeConfig,
    MultiRaftRouter, RaftNodeOptions, SegmentedRaftLogStore, ShardRaftGroup, ShardStateMachine,
    ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use openraft::storage::{
    IOFlushed, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
};
use openraft::type_config::alias::{CommittedLeaderIdOf, EntryOf, LogIdOf, VoteOf};
use openraft::vote::RaftLeaderId;
use openraft::EntryPayload;

fn log_id(index: u64) -> LogIdOf<MetaDbTypeConfig> {
    LogIdOf::<MetaDbTypeConfig>::new(CommittedLeaderIdOf::<MetaDbTypeConfig>::new(1, 1), index)
}

fn put_entry(index: u64, key: &[u8]) -> EntryOf<MetaDbTypeConfig> {
    EntryOf::<MetaDbTypeConfig> {
        log_id: log_id(index),
        payload: EntryPayload::Normal(MetaDbRaftRequest {
            expected_epoch: ShardEpoch::initial(),
            hlc: HlcTimestamp(index),
            payload: bincode::serialize(&BasicKvCommand::Put {
                key: key.to_vec(),
                value: b"present".to_vec(),
            })
            .unwrap(),
        }),
    }
}

async fn committed_history(dir: &std::path::Path, segment_size: u64) {
    let mut store = SegmentedRaftLogStore::open_with_segment_size(
        dir,
        segment_size,
        Arc::new(ShardWatermarkTracker::default()),
    )
    .unwrap();
    store
        .append(
            (0..=2).map(|index| put_entry(index, &[index as u8])),
            IOFlushed::noop(),
        )
        .await
        .unwrap();
    store.save_committed(Some(log_id(2))).await.unwrap();
}

#[tokio::test]
async fn missing_first_committed_segment_preserves_recovery_evidence() {
    let dir = tempfile::tempdir().unwrap();
    committed_history(dir.path(), 1).await;
    fs::remove_file(dir.path().join("segment_00000000000000000000.raftlog")).unwrap();
    let evidence = dir.path().join("HARD_STATE.tmp");
    fs::write(&evidence, b"interrupted atomic metadata write").unwrap();
    assert!(
        SegmentedRaftLogStore::open(dir.path(), Arc::new(ShardWatermarkTracker::default()))
            .is_err()
    );
    assert_eq!(
        fs::read(evidence).unwrap(),
        b"interrupted atomic metadata write"
    );
}

#[tokio::test]
async fn committed_frame_gaps_duplicates_and_identity_mismatches_are_fatal() {
    for corruption in ["missing", "duplicate", "reordered", "identity"] {
        let dir = tempfile::tempdir().unwrap();
        committed_history(dir.path(), 1024 * 1024).await;
        let path = dir.path().join("segment_00000000000000000000.raftlog");
        let original = fs::read(&path).unwrap();
        let mut offset = 8;
        let mut frames = Vec::new();
        while offset < original.len() {
            let len = u32::from_le_bytes(original[offset..offset + 4].try_into().unwrap()) as usize;
            frames.push(original[offset..offset + 8 + len].to_vec());
            offset += 8 + len;
        }
        match corruption {
            "missing" => {
                frames.remove(1);
            }
            "duplicate" => {
                frames.insert(1, frames[0].clone());
            }
            "reordered" => frames.swap(1, 2),
            "identity" => {
                // Preserve the framing index and CRC while changing the serialized LogId.
                let mut entry: EntryOf<MetaDbTypeConfig> =
                    bincode::deserialize(&frames[1][16..]).unwrap();
                entry.log_id = log_id(9);
                frames[1].splice(16.., bincode::serialize(&entry).unwrap());
                let crc = metadb_core::checksum_crc32c(&frames[1][8..]);
                frames[1][4..8].copy_from_slice(&crc.to_le_bytes());
            }
            _ => unreachable!(),
        }
        let mut corrupt = original[..8].to_vec();
        corrupt.extend(frames.into_iter().flatten());
        fs::write(&path, &corrupt).unwrap();
        assert!(
            SegmentedRaftLogStore::open(dir.path(), Arc::new(ShardWatermarkTracker::default()))
                .is_err(),
            "accepted {corruption}"
        );
        assert_eq!(
            fs::read(path).unwrap(),
            corrupt,
            "modified {corruption} evidence"
        );
    }
}

#[tokio::test]
async fn every_legal_prefix_purge_recovers_its_remaining_committed_entries() {
    for segment_size in [1, 1024 * 1024] {
        for purged in 1..=4 {
            let dir = tempfile::tempdir().unwrap();
            let watermarks = Arc::new(ShardWatermarkTracker::new(purged, purged, purged));
            let mut store = SegmentedRaftLogStore::open_with_segment_size(
                dir.path(),
                segment_size,
                watermarks.clone(),
            )
            .unwrap();
            store
                .append((0..=4).map(|i| put_entry(i, &[i as u8])), IOFlushed::noop())
                .await
                .unwrap();
            store.save_committed(Some(log_id(4))).await.unwrap();
            store.purge(log_id(purged)).await.unwrap();
            drop(store);
            let mut reopened = SegmentedRaftLogStore::open(dir.path(), watermarks).unwrap();
            let indexes: Vec<_> = reopened
                .try_get_log_entries(..)
                .await
                .unwrap()
                .into_iter()
                .map(|e| e.log_id.index)
                .collect();
            assert_eq!(indexes, (purged + 1..=4).collect::<Vec<_>>());
            assert_eq!(
                reopened.get_log_state().await.unwrap().last_purged_log_id,
                Some(log_id(purged))
            );
        }
    }
}

#[test]
fn new_store_persists_initial_metadata_and_refuses_its_subsequent_loss() {
    let dir = tempfile::tempdir().unwrap();
    let watermarks = Arc::new(ShardWatermarkTracker::default());
    drop(SegmentedRaftLogStore::open(dir.path(), watermarks.clone()).unwrap());
    assert!(dir.path().join("HARD_STATE").is_file());
    assert!(SegmentedRaftLogStore::open(dir.path(), watermarks.clone()).is_ok());
    fs::remove_file(dir.path().join("HARD_STATE")).unwrap();
    assert!(SegmentedRaftLogStore::open(dir.path(), watermarks).is_err());
}

#[tokio::test]
async fn existing_hard_state_without_a_marker_keeps_its_durable_vote() {
    let dir = tempfile::tempdir().unwrap();
    let watermarks = Arc::new(ShardWatermarkTracker::default());
    let vote = VoteOf::<MetaDbTypeConfig>::new_committed(7, 1);
    let mut store = SegmentedRaftLogStore::open(dir.path(), watermarks.clone()).unwrap();
    store.save_vote(&vote).await.unwrap();
    drop(store);
    // Existing installations predate the initialization marker.
    fs::remove_file(dir.path().join("INITIALIZED")).unwrap();
    let mut reopened = SegmentedRaftLogStore::open(dir.path(), watermarks).unwrap();
    assert_eq!(reopened.read_vote().await.unwrap(), Some(vote));
    assert!(dir.path().join("INITIALIZED").is_file());
}

#[tokio::test]
async fn missing_hard_state_after_purge_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let watermarks = Arc::new(ShardWatermarkTracker::new(2, 2, 2));
    let mut store =
        SegmentedRaftLogStore::open_with_segment_size(dir.path(), 1, watermarks.clone()).unwrap();
    store
        .append((0..=2).map(|i| put_entry(i, &[i as u8])), IOFlushed::noop())
        .await
        .unwrap();
    store.save_committed(Some(log_id(2))).await.unwrap();
    store.purge(log_id(2)).await.unwrap();
    assert_eq!(store.segment_file_count(), 0);
    drop(store);
    fs::remove_file(dir.path().join("HARD_STATE")).unwrap();
    assert!(SegmentedRaftLogStore::open(dir.path(), watermarks).is_err());
}

#[tokio::test]
async fn missing_interior_committed_segment_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let watermarks = Arc::new(ShardWatermarkTracker::default());
    let mut store =
        SegmentedRaftLogStore::open_with_segment_size(dir.path(), 1, watermarks.clone()).unwrap();
    store
        .save_vote(&VoteOf::<MetaDbTypeConfig>::new_committed(1, 1))
        .await
        .unwrap();
    store
        .append(
            (0..=2).map(|index| put_entry(index, &[index as u8])),
            IOFlushed::noop(),
        )
        .await
        .unwrap();
    store.save_committed(Some(log_id(2))).await.unwrap();
    assert_eq!(store.segment_file_count(), 3);
    drop(store);

    fs::remove_file(dir.path().join("segment_00000000000000000001.raftlog")).unwrap();
    let reopened = SegmentedRaftLogStore::open(dir.path(), watermarks);
    if let Ok(mut store) = reopened {
        let retained: Vec<_> = store
            .try_get_log_entries(0..=2)
            .await
            .unwrap()
            .into_iter()
            .map(|entry| entry.log_id.index)
            .collect();
        panic!("recovery silently accepted missing committed index 1; retained={retained:?}; committed={:?}", store.read_committed().await.unwrap());
    }
}

async fn start_group(dir: &std::path::Path) -> metadb_core::Result<Arc<ShardRaftGroup>> {
    let watermarks = Arc::new(ShardWatermarkTracker::default());
    ShardRaftGroup::start(
        1,
        1,
        dir,
        Arc::new(ShardedBlockCache::new(1024 * 1024)),
        LsmEngineConfig::default(),
        RaftNodeOptions {
            max_segment_bytes: 1,
            ..Default::default()
        },
        Arc::new(BasicKvApplyHandler::new(
            ShardEpoch::initial(),
            watermarks.clone(),
        )),
        watermarks,
        Arc::new(HybridLogicalClock::new()),
        &MultiRaftRouter::new(),
        ChannelPool::default(),
    )
    .await
}

#[tokio::test]
async fn recovery_never_reports_missing_interior_write_as_applied() {
    let dir = tempfile::tempdir().unwrap();
    let group = start_group(dir.path()).await.unwrap();
    group
        .initialize_if_needed(BTreeMap::from([(1, "127.0.0.1:0".into())]))
        .await
        .unwrap();
    let mut lost_index = 0;
    for index in 0..3 {
        let response = group
            .propose(
                group.current_epoch(),
                HlcTimestamp::ZERO,
                bincode::serialize(&BasicKvCommand::Put {
                    key: vec![index],
                    value: b"acknowledged".to_vec(),
                })
                .unwrap(),
            )
            .await
            .unwrap();
        if index == 1 {
            lost_index = response.applied_raft_index;
        }
    }
    assert_eq!(group.engine().durable_checkpoint().flushed_raft_index, 0);
    group.shutdown().await.unwrap();
    drop(group);
    fs::remove_file(
        dir.path()
            .join("raft")
            .join(format!("segment_{lost_index:020}.raftlog")),
    )
    .unwrap();
    let recovered = match start_group(dir.path()).await {
        Ok(group) => group,
        Err(_) => return, // Refusing recovery is required when the committed prefix is incomplete.
    };
    let (_, snapshot) = recovered
        .linearizable_read(recovered.current_epoch())
        .await
        .unwrap();
    let lost_value = recovered
        .engine()
        .get_with_snapshot(&[1], &snapshot)
        .unwrap();
    let applied = recovered.read_batcher().current_frontier().raft_index;
    drop(snapshot);
    recovered.shutdown().await.unwrap();
    assert_eq!(lost_value, Some(b"acknowledged".to_vec()), "restart served linearizable reads after skipping committed write index {lost_index}; applied={applied}");
}

#[tokio::test]
async fn missing_hard_state_with_existing_logs_is_fatal() {
    let dir = tempfile::tempdir().unwrap();
    let watermarks = Arc::new(ShardWatermarkTracker::default());
    let mut store = SegmentedRaftLogStore::open(dir.path(), watermarks.clone()).unwrap();
    store
        .save_vote(&VoteOf::<MetaDbTypeConfig>::new_committed(1, 1))
        .await
        .unwrap();
    store
        .append([put_entry(0, b"committed")], IOFlushed::noop())
        .await
        .unwrap();
    store.save_committed(Some(log_id(0))).await.unwrap();
    drop(store);

    fs::remove_file(dir.path().join("HARD_STATE")).unwrap();
    let reopened = SegmentedRaftLogStore::open(dir.path(), watermarks);
    if let Ok(mut store) = reopened {
        panic!("existing raft history reopened without its durable vote/commit: vote={:?}, committed={:?}, retained={}", store.read_vote().await.unwrap(), store.read_committed().await.unwrap(), store.retained_entry_count());
    }
}

fn state_machine(dir: &std::path::Path) -> ShardStateMachine {
    let watermarks = Arc::new(ShardWatermarkTracker::default());
    ShardStateMachine::open(
        1,
        dir,
        Arc::new(ShardedBlockCache::new(1024 * 1024)),
        LsmEngineConfig::default(),
        Arc::new(BasicKvApplyHandler::new(
            ShardEpoch::initial(),
            watermarks.clone(),
        )),
        watermarks,
    )
    .unwrap()
    .0
}

#[tokio::test]
async fn snapshot_without_manifest_never_replaces_live_data() {
    let source_dir = tempfile::tempdir().unwrap();
    let mut source = state_machine(source_dir.path());
    source
        .apply(tokio_stream::iter([Ok((put_entry(2, b"source"), None))]))
        .await
        .unwrap();
    let mut builder = source.get_snapshot_builder().await;
    let mut snapshot = builder.build_snapshot().await.unwrap();
    assert_eq!(snapshot.snapshot.checkpoint.flushed_raft_index, 2);
    // Every supplied file has a valid CRC/length, but the required file set is absent.
    snapshot.snapshot.files.clear();

    let target_dir = tempfile::tempdir().unwrap();
    let mut target = state_machine(target_dir.path());
    target
        .apply(tokio_stream::iter([Ok((
            put_entry(1, b"target-sentinel"),
            None,
        ))]))
        .await
        .unwrap();
    target.flush_to_lsm().unwrap();
    let result = target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await;
    let engine = target.engine();
    let guard = engine.acquire_linearizable_snapshot(2).unwrap();
    let sentinel = engine
        .get_with_snapshot(b"target-sentinel", &guard)
        .unwrap();
    let checkpoint = engine.durable_checkpoint();
    assert!(result.is_err() && sentinel == Some(b"present".to_vec()), "incomplete archive install result={result:?}; old live sentinel={sentinel:?}; actual MANIFEST index={}; applied index={:?}", checkpoint.flushed_raft_index, target.applied_state().await.unwrap().0);
}

#[tokio::test]
async fn inconsistent_snapshot_bundles_preserve_live_data_and_applied_state() {
    let source_dir = tempfile::tempdir().unwrap();
    let mut source = state_machine(source_dir.path());
    source
        .apply(tokio_stream::iter([Ok((put_entry(2, b"older"), None))]))
        .await
        .unwrap();
    let mut builder = source.get_snapshot_builder().await;
    let older = builder.build_snapshot().await.unwrap();
    source
        .apply(tokio_stream::iter([Ok((put_entry(3, b"newer"), None))]))
        .await
        .unwrap();
    let current = builder.build_snapshot().await.unwrap();

    for corruption in [
        "current",
        "manifest",
        "sst",
        "duplicate",
        "unexpected",
        "stale-manifest",
        "term",
        "leader",
        "membership",
        "descriptor",
        "data-crc",
        "manifest-tail",
    ] {
        let mut archive = current.snapshot.clone();
        let mut meta = current.meta.clone();
        match corruption {
            "current" => archive.files.retain(|f| f.name != "CURRENT"),
            "manifest" => archive.files.retain(|f| !f.name.starts_with("MANIFEST-")),
            "sst" => archive.files.retain(|f| !f.name.ends_with(".sst")),
            "duplicate" => archive.files.push(archive.files[0].clone()),
            "unexpected" => {
                let mut extra = archive.files[0].clone();
                extra.name = "unexpected".into();
                archive.files.push(extra);
            }
            "stale-manifest" => archive.files = older.snapshot.files.clone(),
            "term" => {
                meta.last_log_id = Some(LogIdOf::<MetaDbTypeConfig>::new(
                    CommittedLeaderIdOf::<MetaDbTypeConfig>::new(2, 1),
                    3,
                ))
            }
            "leader" => {
                meta.last_log_id = Some(LogIdOf::<MetaDbTypeConfig>::new(
                    CommittedLeaderIdOf::<MetaDbTypeConfig>::new(1, 2),
                    3,
                ))
            }
            "membership" => archive.checkpoint.membership_bytes = vec![0xff],
            "descriptor" => {
                archive.checkpoint.descriptor_bytes =
                    ShardEpoch::initial().bump_conf_ver().encode().to_vec()
            }
            "data-crc" => {
                let sst = archive
                    .files
                    .iter_mut()
                    .find(|f| f.name.ends_with(".sst"))
                    .unwrap();
                sst.bytes[8] ^= 1;
                // The archive checksum is valid, but an SST data block is corrupt.
                sst.crc32c = metadb_core::checksum_crc32c(&sst.bytes);
            }
            "manifest-tail" => {
                let manifest = archive
                    .files
                    .iter_mut()
                    .find(|f| f.name.starts_with("MANIFEST-"))
                    .unwrap();
                manifest.bytes.push(0xff);
                manifest.expected_len += 1;
                manifest.crc32c = metadb_core::checksum_crc32c(&manifest.bytes);
            }
            _ => unreachable!(),
        }

        let target_dir = tempfile::tempdir().unwrap();
        let mut target = state_machine(target_dir.path());
        target
            .apply(tokio_stream::iter([Ok((put_entry(1, b"sentinel"), None))]))
            .await
            .unwrap();
        target.flush_to_lsm().unwrap();
        let before = target.engine().durable_checkpoint();
        assert!(
            target.install_snapshot(&meta, archive).await.is_err(),
            "accepted {corruption}"
        );
        assert_eq!(
            target.applied_state().await.unwrap().0,
            Some(log_id(1)),
            "published {corruption}"
        );
        let engine = target.engine();
        assert_eq!(
            engine.durable_checkpoint(),
            before,
            "replaced checkpoint for {corruption}"
        );
        let guard = engine.acquire_linearizable_snapshot(1).unwrap();
        assert_eq!(
            engine.get_with_snapshot(b"sentinel", &guard).unwrap(),
            Some(b"present".to_vec()),
            "lost sentinel for {corruption}"
        );
        drop(guard);
        drop(engine);
        // A rejected bundle must leave the receiver able to install a complete bundle.
        target
            .install_snapshot(&current.meta, current.snapshot.clone())
            .await
            .unwrap();
        assert_eq!(
            target.engine().durable_checkpoint(),
            current.snapshot.checkpoint
        );
    }
}

#[tokio::test]
async fn valid_older_snapshot_never_rolls_back_a_newer_applied_state() {
    let source_dir = tempfile::tempdir().unwrap();
    let mut source = state_machine(source_dir.path());
    source
        .apply(tokio_stream::iter([Ok((put_entry(2, b"source"), None))]))
        .await
        .unwrap();
    let mut builder = source.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();
    let target_dir = tempfile::tempdir().unwrap();
    let mut target = state_machine(target_dir.path());
    target
        .apply(tokio_stream::iter([Ok((put_entry(3, b"sentinel"), None))]))
        .await
        .unwrap();
    target.flush_to_lsm().unwrap();
    let before = target.engine().durable_checkpoint();
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(target.applied_state().await.unwrap().0, Some(log_id(3)));
    assert_eq!(target.engine().durable_checkpoint(), before);
}

#[tokio::test]
async fn entry_zero_snapshot_retains_its_complete_log_identity() {
    let source_dir = tempfile::tempdir().unwrap();
    let mut source = state_machine(source_dir.path());
    source
        .apply(tokio_stream::iter([Ok((put_entry(0, b"source"), None))]))
        .await
        .unwrap();
    let mut builder = source.get_snapshot_builder().await;
    let snapshot = builder.build_snapshot().await.unwrap();
    assert_eq!(snapshot.meta.last_log_id, Some(log_id(0)));
    let target_dir = tempfile::tempdir().unwrap();
    let mut target = state_machine(target_dir.path());
    target
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(target.applied_state().await.unwrap().0, Some(log_id(0)));
    drop(target);
    let mut recovered = state_machine(target_dir.path());
    assert_eq!(recovered.applied_state().await.unwrap().0, Some(log_id(0)));
    let engine = recovered.engine();
    let snapshot = engine.acquire_linearizable_snapshot(0).unwrap();
    assert_eq!(
        engine.get_with_snapshot(b"source", &snapshot).unwrap(),
        Some(b"present".to_vec())
    );
}
