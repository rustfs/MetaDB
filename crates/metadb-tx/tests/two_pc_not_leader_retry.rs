//! Prepare and Commit retry when the primary replica returns NotLeader with a known hint.

use std::collections::BTreeMap;
use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use metadb_core::codec;
use metadb_core::{
    CompositeKeyRange, HlcTimestamp, HybridLogicalClock, InodeId, MetaDbError, Result, ShardEpoch,
    ROOT_INODE_ID,
};
use metadb_raft::{
    ChannelPool, MultiRaftRouter, RaftNodeOptions, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{
    DirRenameLease, DistributedTxRouter, ShardDirectory, TxRaftCmd, TxStateMachineHandler,
};
use tempfile::tempdir;

struct TwoShard {
    shards: Vec<(
        CompositeKeyRange,
        Arc<ShardRaftGroup>,
        Arc<TxStateMachineHandler>,
    )>,
    clock: Arc<HybridLogicalClock>,
}

#[tonic::async_trait]
impl ShardDirectory for TwoShard {
    fn route_user_key(
        &self,
        user_key: &[u8],
    ) -> Result<(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)> {
        for (range, group, handler) in &self.shards {
            if range.contains_key(user_key) {
                return Ok((group.current_epoch(), group.clone(), handler.clone()));
            }
        }
        Err(MetaDbError::ShardRoutingError("unrouted key".to_string()))
    }

    fn allocate_txn_id(&self, coord_node_id: u64) -> Result<metadb_core::TxnId> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Ok(((coord_node_id as u128) << 64)
            | NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u128)
    }

    fn route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> Result<Vec<(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)>> {
        Ok(self
            .shards
            .iter()
            .filter(|(range, _, _)| range.overlaps(start_user_key, end_user_key))
            .map(|(_, group, handler)| (group.current_epoch(), group.clone(), handler.clone()))
            .collect())
    }

    async fn acquire_dir_rename_lease(
        &self,
        _moved_dir_id: InodeId,
        ttl_ms: u64,
    ) -> Result<DirRenameLease> {
        Ok(DirRenameLease {
            rename_fence_token: 1,
            expire_hlc: metadb_core::HlcTimestamp::new(self.clock.now().physical_ms() + ttl_ms, 0),
        })
    }

    async fn release_dir_rename_lease(&self, _token: u64) {}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_shard_prepare_and_commit_retry_known_not_leader() {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let router = MultiRaftRouter::new();
    let boot = |sid: u64, range: CompositeKeyRange| {
        let base = dir.path().join(format!("shard_{sid}"));
        let cache = cache.clone();
        let clock = clock.clone();
        let router = router.clone();
        async move {
            let watermarks = Arc::new(ShardWatermarkTracker::default());
            let handler = Arc::new(TxStateMachineHandler::new(
                sid,
                ShardEpoch::initial(),
                range.clone(),
                watermarks.clone(),
            ));
            let group = ShardRaftGroup::start(
                sid,
                1,
                base,
                cache,
                LsmEngineConfig::default(),
                RaftNodeOptions::default(),
                handler.clone(),
                watermarks,
                clock,
                &router,
                ChannelPool::default(),
            )
            .await
            .unwrap();
            group
                .initialize_if_needed(BTreeMap::from([(1, "127.0.0.1:1".to_string())]))
                .await
                .unwrap();
            (range, group, handler)
        }
    };
    let left = boot(1, CompositeKeyRange::meta_pid_range(0, 500).unwrap()).await;
    let right = boot(2, CompositeKeyRange::meta_pid_range(500, u64::MAX).unwrap()).await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let tx = DistributedTxRouter::new(
        1,
        clock,
        Arc::new(TwoShard {
            shards: vec![left, right],
            clock: Arc::new(HybridLogicalClock::new()),
        }),
    );
    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "src", 100, 7, 0o755).await.unwrap();

    let directory = tx.directory();
    let left_key = metadb_core::MetaKey::child(ROOT_INODE_ID, "dst").encode();
    let (_, left_group, _) = directory.route_user_key(&left_key).unwrap();
    let right_key = metadb_core::MetaKey::dir_attr(600).encode();
    let (_, right_group, _) = directory.route_user_key(&right_key).unwrap();
    left_group.testing_fail_next_proposals(1, Some(1));
    right_group.testing_fail_next_proposals(1, Some(1));

    tx.mkdir(ROOT_INODE_ID, "dst", 600, 7, 0o755)
        .await
        .expect("Prepare and Commit must retry NotLeader and finish the cross-shard mkdir");
    left_group.testing_fail_next_epoch_mismatch(1);
    right_group.testing_fail_next_epoch_mismatch(1);
    tx.mkdir(ROOT_INODE_ID, "other", 700, 7, 0o755)
        .await
        .expect("a split-style epoch mismatch must regroup and commit");
    left_group.testing_fail_next_log_discarded(1);
    right_group.testing_fail_next_log_discarded(1);
    tx.mkdir(ROOT_INODE_ID, "third", 800, 7, 0o755)
        .await
        .expect("Prepare and Commit must retry a discarded log entry");
    tx.create_file(ROOT_INODE_ID, "same_shard.dat", 42, 42, 7, 0o644, 8)
        .await
        .unwrap();
    left_group.testing_fail_next_log_discarded(1);
    tx.create_file(100, "again.dat", 43, 43, 7, 0o644, 8)
        .await
        .expect("a same-shard write must retry a discarded log entry");
    left_group.testing_fail_next_read_quorum(1);
    let found = tx
        .lookup(ROOT_INODE_ID, "dst")
        .await
        .expect("a single missing read quorum is retried inside the election budget");
    assert!(found.is_some());

    let epoch = left_group.current_epoch();
    let raw = codec::serialize(&TxRaftCmd::CreateFile {
        pid: ROOT_INODE_ID,
        name: "legacy.dat".to_string(),
        inode_id: 44,
        fid: 44,
        uid: 7,
        mode: 0o644,
        size_bytes: 1,
    })
    .unwrap();
    left_group.testing_fail_next_log_discarded(1);
    let rejected = left_group
        .propose(epoch, HlcTimestamp::ZERO, raw.clone())
        .await
        .unwrap_err();
    assert!(!rejected.is_retryable_write_consensus(), "{rejected}");
    assert!(
        rejected.to_string().contains("legacy proposal"),
        "{rejected}"
    );
    assert!(tx
        .lookup(ROOT_INODE_ID, "legacy.dat")
        .await
        .unwrap()
        .is_none());
    left_group
        .propose(epoch, HlcTimestamp::ZERO, raw.clone())
        .await
        .unwrap();
    assert!(tx
        .lookup(ROOT_INODE_ID, "legacy.dat")
        .await
        .unwrap()
        .is_some());
    left_group.testing_fail_next_log_discarded(1);
    let second = left_group
        .propose(epoch, HlcTimestamp::ZERO, raw)
        .await
        .unwrap_err();
    assert!(!second.is_retryable_write_consensus(), "{second}");
    assert!(tx
        .lookup(ROOT_INODE_ID, "legacy.dat")
        .await
        .unwrap()
        .is_some());
}
