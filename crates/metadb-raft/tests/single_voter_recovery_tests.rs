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
