use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::sync::Arc;

use metadb_core::{HlcTimestamp, HybridLogicalClock, MetaDbError, MetaKey, ShardEpoch};
use metadb_raft::{
    BasicKvApplyHandler, BasicKvCommand, ChannelPool, MultiRaftRouter, RaftNodeOptions,
    ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use tempfile::tempdir;

/// T-RAFT-1: Verifies WAL-less `1-Voter` crash recovery (un-flushed MemMvccStore entries replayed
/// from segmented `.raftlog` via durable `save_committed`/`read_committed`), torn `.raftlog` tail
/// truncation, and 3-watermark log purge pinning (`safe_purge_index`).
#[tokio::test]
async fn test_single_voter_walless_crash_recovery_and_three_watermark_pinning() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let epoch = ShardEpoch::initial();

    // 1. Boot Stage 1 single-voter group (`max_segment_bytes = 2048` to exercise multi-segment rotation)
    //    and write 40 keys WITHOUT flushing `MemMvccStore` to SSTables!
    {
        let watermarks = Arc::new(ShardWatermarkTracker::new(0, 0, 0));
        let handler = Arc::new(BasicKvApplyHandler::new(epoch, watermarks.clone()));
        let router = MultiRaftRouter::new();
        let opts = RaftNodeOptions {
            max_segment_bytes: 2048,
            ..Default::default()
        };

        let group = ShardRaftGroup::start(
            1,
            1,
            &shard_dir,
            cache.clone(),
            LsmEngineConfig::default(),
            opts,
            handler,
            watermarks.clone(),
            clock.clone(),
            &router,
            ChannelPool::default(),
        )
        .await
        .unwrap();

        group
            .initialize_if_needed(BTreeMap::from([(1u64, "127.0.0.1:0".to_string())]))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        let epoch = group.current_epoch();

        for i in 0..40u64 {
            let cmd = BasicKvCommand::Put {
                key: MetaKey::child(1, format!("unflushed_{:03}.dat", i)).encode(),
                value: format!("payload_{}", i).into_bytes(),
            };
            group
                .propose(epoch, HlcTimestamp::ZERO, bincode::serialize(&cmd).unwrap())
                .await
                .unwrap();
        }

        assert!(
            group.log_store().segment_file_count() >= 2,
            "expected multi-segment rotation"
        );

        // Verify 3-watermark pinning: state_flushed_index == 0 -> purge MUST retain all log entries!
        let _ = group.log_store().try_purge_to_safe_watermark().unwrap();
        assert!(group.log_store().retained_entry_count() >= 40);

        // Simulate abrupt power crash (drop without flushing MemMvccStore!).
        let _ = group.shutdown().await;
    }

    // 2. Inject a torn/partial frame at the tail of the latest `.raftlog` segment.
    let raft_dir = shard_dir.join("raft");
    let mut segs: Vec<_> = fs::read_dir(&raft_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_str()
                .map(|s| s.ends_with(".raftlog"))
                .unwrap_or(false)
        })
        .map(|e| e.path())
        .collect();
    segs.sort();
    {
        let mut f = OpenOptions::new()
            .append(true)
            .open(segs.last().unwrap())
            .unwrap();
        f.write_all(&999u32.to_le_bytes()).unwrap();
        f.write_all(&0xCAFE_BABEu32.to_le_bytes()).unwrap();
        f.write_all(b"torn_frame_tail").unwrap();
        f.sync_all().unwrap();
    }

    // 3. Restart node from disk: OpenRaft must truncate the torn tail, read `HARD_STATE.committed`,
    //    observe `MANIFEST` `applied_state == None`, and replay all 40 committed entries into RAM!
    {
        let watermarks = Arc::new(ShardWatermarkTracker::new(0, 0, 0));
        let handler = Arc::new(BasicKvApplyHandler::new(epoch, watermarks.clone()));
        let router = MultiRaftRouter::new();
        let opts = RaftNodeOptions {
            max_segment_bytes: 2048,
            ..Default::default()
        };

        let recovered = ShardRaftGroup::start(
            1,
            1,
            &shard_dir,
            cache.clone(),
            LsmEngineConfig::default(),
            opts,
            handler,
            watermarks.clone(),
            clock.clone(),
            &router,
            ChannelPool::default(),
        )
        .await
        .unwrap();

        // Note: Membership entry on initialize bumped conf_ver to 2!
        let active_epoch = recovered.current_epoch();
        let (_, snap) = recovered.linearizable_read(active_epoch).await.unwrap();
        for i in 0..40u64 {
            let k = MetaKey::child(1, format!("unflushed_{:03}.dat", i)).encode();
            assert_eq!(
                recovered.engine().get_with_snapshot(&k, &snap).unwrap(),
                Some(format!("payload_{}", i).into_bytes()),
                "entry {} must be recovered from .raftlog into WAL-less MemMvccStore",
                i
            );
        }
        drop(snap);

        // Verify stale epoch is rejected by `propose` and `linearizable_read` (`Resolution B16`).
        let stale_epoch = ShardEpoch {
            conf_ver: 99,
            version: 1,
        };
        assert!(matches!(
            recovered.linearizable_read(stale_epoch).await,
            Err(MetaDbError::EpochMismatch { .. })
        ));

        // Now flush SSTable + build Raft snapshot + advance all 3 watermarks -> `.raftlog` purges!
        let purged_to = recovered.flush_snapshot_and_purge().await.unwrap();
        assert!(purged_to >= 40);
        assert_eq!(recovered.log_store().retained_entry_count(), 0);

        recovered.shutdown().await.unwrap();
    }
}

/// T-RAFT-1B: Verifies crash-safe suffix-only `truncate_after` (preserving untouched prefix segment files),
/// strict `InvalidData` error on complete-frame CRC32C corruption in the final segment, and
/// `install_snapshot` index + per-file `(expected_len, crc32c)` validation.
#[tokio::test]
async fn test_suffix_only_truncate_final_crc_rejection_and_snapshot_verification() {
    use openraft::storage::{RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine};

    let dir = tempdir().unwrap();
    let wm = Arc::new(ShardWatermarkTracker::new(0, 0, 0));

    // 1. Use a single-voter group (`max_segment_bytes = 512`) to populate multi-segment `.raftlog` frames.
    let group_dir = dir.path().join("group_1");
    let cache = Arc::new(ShardedBlockCache::new(2 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let handler = Arc::new(BasicKvApplyHandler::new(ShardEpoch::initial(), wm.clone()));
    let router = MultiRaftRouter::new();
    let grp = ShardRaftGroup::start(
        10,
        1,
        &group_dir,
        cache.clone(),
        LsmEngineConfig::default(),
        RaftNodeOptions {
            max_segment_bytes: 512,
            ..Default::default()
        },
        handler.clone(),
        wm.clone(),
        clock,
        &router,
        ChannelPool::default(),
    )
    .await
    .unwrap();
    grp.initialize_if_needed(BTreeMap::from([(1u64, "127.0.0.1:0".to_string())]))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    let ep = grp.current_epoch();
    for i in 1..=15u64 {
        let cmd = BasicKvCommand::Put {
            key: MetaKey::child(1, format!("k_{:02}", i)).encode(),
            value: vec![i as u8; 64],
        };
        grp.propose(ep, HlcTimestamp::ZERO, bincode::serialize(&cmd).unwrap())
            .await
            .unwrap();
    }

    // Build a valid snapshot from `grp` for snapshot-verification checks below!
    let mut sm_clone = grp.state_machine().clone();
    let mut builder = sm_clone.get_snapshot_builder().await;
    let valid_snap = builder.build_snapshot().await.unwrap();
    grp.shutdown().await.unwrap();

    // Reopen `SegmentedRaftLogStore` on `group_dir/raft`, lower `committed` to index 5 so indices 6..=16 are uncommitted,
    // record first segment bytes, and verify `truncate_after(index 8)` leaves segment 0 untouched while truncating suffix!
    let grp_raft_dir = group_dir.join("raft");
    let mut raw_store =
        metadb_raft::SegmentedRaftLogStore::open_with_segment_size(&grp_raft_dir, 512, wm.clone())
            .unwrap();
    assert!(raw_store.segment_file_count() >= 2);
    let seg0_path = grp_raft_dir.join("segment_00000000000000000000.raftlog");
    let seg0_before = fs::read(&seg0_path).unwrap();

    let entry3 = raw_store
        .try_get_log_entries(3..=3)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let entry5 = raw_store
        .try_get_log_entries(5..=5)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let entry8 = raw_store
        .try_get_log_entries(8..=8)
        .await
        .unwrap()
        .pop()
        .unwrap();
    raw_store.save_committed(Some(entry5.log_id)).await.unwrap();

    // Truncating below committed index 5 must be rejected!
    assert!(raw_store.truncate_after(Some(entry3.log_id)).await.is_err());
    assert!(raw_store.truncate_after(None).await.is_err());

    // Suffix-only truncate_after(8) must keep 0..=8 and preserve `seg0_path` byte-for-byte!
    raw_store.truncate_after(Some(entry8.log_id)).await.unwrap();
    assert_eq!(fs::read(&seg0_path).unwrap(), seg0_before);
    drop(raw_store);

    let reopened_store =
        metadb_raft::SegmentedRaftLogStore::open_with_segment_size(&grp_raft_dir, 512, wm.clone())
            .unwrap();
    let mut reader = reopened_store.clone();
    let all_after = reader.try_get_log_entries(0..=100).await.unwrap();
    assert_eq!(all_after.last().unwrap().log_id.index, 8);
    drop(reader);
    drop(reopened_store);

    // 2. A bad CRC that is NOT the last frame of the last segment must refuse to open.
    //    A bad CRC on the last frame of the last segment is a torn tail and is truncated.
    let mut seg_files: Vec<_> = fs::read_dir(&grp_raft_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.extension()
                .and_then(|s| s.to_str())
                .map(|s| s == "raftlog")
                .unwrap_or(false)
        })
        .collect();
    seg_files.sort();
    assert!(
        seg_files.len() >= 2,
        "need a non-final segment to prove mid-log CRC is fatal"
    );
    let non_final = seg_files.first().unwrap().clone();
    let non_final_orig = fs::read(&non_final).unwrap();
    let mut non_final_bad = non_final_orig.clone();
    assert!(non_final_bad.len() > 16);
    non_final_bad[16] ^= 0xFF;
    fs::write(&non_final, &non_final_bad).unwrap();
    let corrupt_err = match metadb_raft::SegmentedRaftLogStore::open(&grp_raft_dir, wm.clone()) {
        Ok(_) => panic!("expected InvalidData on CRC mismatch before the last frame"),
        Err(e) => e,
    };
    assert_eq!(corrupt_err.kind(), std::io::ErrorKind::InvalidData);
    fs::write(&non_final, &non_final_orig).unwrap();

    let last_seg = seg_files.last().unwrap();
    let last_orig = fs::read(last_seg).unwrap();
    let mut last_bad = last_orig.clone();
    let frames = raft_frame_ends(&last_bad);
    assert!(
        !frames.is_empty(),
        "final segment must contain at least one frame"
    );
    let (frame_start, frame_end) = frames[frames.len() - 1];
    let flip_at = frame_start + 12;
    assert!(flip_at < frame_end);
    last_bad[flip_at] ^= 0xFF;
    fs::write(last_seg, &last_bad).unwrap();
    let truncated = metadb_raft::SegmentedRaftLogStore::open(&grp_raft_dir, wm.clone())
        .expect("last-frame CRC of the last segment must be truncated, not fatal");
    let mut truncated_reader = truncated.clone();
    let after_torn = truncated_reader.try_get_log_entries(0..=100).await.unwrap();
    let torn_last = after_torn.last().map(|e| e.log_id.index).unwrap_or(0);
    assert!(
        torn_last < 8,
        "torn final frame must be dropped, last index was {torn_last}"
    );
    drop(truncated_reader);
    drop(truncated);

    // 3. Verify `ShardStateMachine::install_snapshot` rejects checkpoint index mismatch and tampered file CRC32C!
    let target_sm_dir = dir.path().join("sm_install_target");
    let mut target_sm = metadb_raft::ShardStateMachine::open(
        99,
        &target_sm_dir,
        cache,
        LsmEngineConfig::default(),
        Arc::new(BasicKvApplyHandler::new(ShardEpoch::initial(), wm)),
        Arc::new(ShardWatermarkTracker::default()),
    )
    .unwrap()
    .0;

    let mut bad_idx_snap = valid_snap.snapshot.clone();
    bad_idx_snap.checkpoint.flushed_raft_index += 7;
    assert_eq!(
        target_sm
            .install_snapshot(&valid_snap.meta, bad_idx_snap)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidData
    );

    let mut bad_crc_snap = valid_snap.snapshot.clone();
    bad_crc_snap.files[0].bytes[0] ^= 0xFF;
    assert_eq!(
        target_sm
            .install_snapshot(&valid_snap.meta, bad_crc_snap)
            .await
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidData
    );

    // Valid snapshot installs cleanly, and re-installing at `<= current_idx` is a fast no-op!
    target_sm
        .install_snapshot(&valid_snap.meta, valid_snap.snapshot.clone())
        .await
        .unwrap();
    target_sm
        .install_snapshot(&valid_snap.meta, valid_snap.snapshot)
        .await
        .unwrap();
}

/// Returns `(frame_start, frame_end)` offsets for every complete frame after the 8-byte magic.
fn raft_frame_ends(raw: &[u8]) -> Vec<(usize, usize)> {
    let mut offset = 8usize;
    let mut out = Vec::new();
    while offset + 8 <= raw.len() {
        let frame_len = u32::from_le_bytes(raw[offset..offset + 4].try_into().unwrap()) as usize;
        let frame_end = offset + 8 + frame_len;
        if frame_end > raw.len() || frame_len < 8 {
            break;
        }
        out.push((offset, frame_end));
        offset = frame_end;
    }
    out
}
