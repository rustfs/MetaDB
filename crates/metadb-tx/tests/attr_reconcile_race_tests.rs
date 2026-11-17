use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use metadb_core::{
    CompositeKeyRange, HlcTimestamp, HybridLogicalClock, InodeId, MetaDbError, Result, ShardEpoch,
    ROOT_INODE_ID,
};
use metadb_raft::{
    ChannelPool, MultiRaftRouter, RaftNodeOptions, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{DirRenameLease, DistributedTxRouter, ShardDirectory, TxStateMachineHandler};
use tempfile::tempdir;

struct StaticTestDirectory {
    shards: Vec<(
        CompositeKeyRange,
        Arc<ShardRaftGroup>,
        Arc<TxStateMachineHandler>,
    )>,
    fence_seq: AtomicU64,
    clock: Arc<HybridLogicalClock>,
}

#[tonic::async_trait]
impl ShardDirectory for StaticTestDirectory {
    fn route_user_key(
        &self,
        user_key: &[u8],
    ) -> Result<(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)> {
        for (r, g, h) in &self.shards {
            if r.contains_key(user_key) {
                return Ok((g.current_epoch(), g.clone(), h.clone()));
            }
        }
        Err(MetaDbError::ShardRoutingError(
            "no shard for key".to_string(),
        ))
    }

    fn allocate_txn_id(&self, coord_node_id: u64) -> Result<metadb_core::TxnId> {
        static NEXT_TXN: AtomicU64 = AtomicU64::new(1);
        Ok(((coord_node_id as u128) << 64) | NEXT_TXN.fetch_add(1, Ordering::Relaxed) as u128)
    }

    fn route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> Result<Vec<(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)>> {
        let mut out = Vec::new();
        for (r, g, h) in &self.shards {
            if r.overlaps(start_user_key, end_user_key) {
                out.push((g.current_epoch(), g.clone(), h.clone()));
            }
        }
        Ok(out)
    }

    async fn acquire_dir_rename_lease(
        &self,
        _moved_dir_id: InodeId,
        ttl_ms: u64,
    ) -> Result<DirRenameLease> {
        let tok = self.fence_seq.fetch_add(1, Ordering::AcqRel);
        let now = self.clock.now();
        Ok(DirRenameLease {
            rename_fence_token: tok,
            expire_hlc: HlcTimestamp::new(now.physical_ms() + ttl_ms, 0),
        })
    }

    async fn release_dir_rename_lease(&self, _token: u64) {}
}

/// T-TX-1: Spawns concurrent writer tasks (`create_file`), concurrent `dirstat` readers, and
/// interleaved L0 Coordinated Flush Barriers on a persistent OpenRaft + LSM shard, verifying
/// `dirstat` never undercounts or double-counts across MemTable seals and SSTable flushes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_attr_aggregation_read_reconciliation_and_l0_flush_barriers() {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let router = MultiRaftRouter::new();

    let range = CompositeKeyRange::all_user_tables();
    let wm = Arc::new(ShardWatermarkTracker::default());
    let handler = Arc::new(TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        range.clone(),
        wm.clone(),
    ));

    let group = ShardRaftGroup::start(
        1,
        1,
        dir.path().join("shard_1"),
        cache,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        handler.clone(),
        wm,
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
    tokio::time::sleep(Duration::from_millis(80)).await;

    let dir_svc = Arc::new(StaticTestDirectory {
        shards: vec![(range, group.clone(), handler.clone())],
        fence_seq: AtomicU64::new(1),
        clock: clock.clone(),
    });
    let tx = Arc::new(DistributedTxRouter::new(1, clock, dir_svc));

    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "hot_dir", 500, 7, 0o755)
        .await
        .unwrap();

    // Spawn 8 writer tasks creating 15 files each (120 files total), 2 reader tasks checking monotonic
    // `num_children`, and 1 flusher task repeatedly sealing/flushing L0 SSTables mid-flight!
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut writers = Vec::new();
    for w in 0..8u64 {
        let tx_c = tx.clone();
        writers.push(tokio::spawn(async move {
            for i in 0..15u64 {
                let seq = w * 100 + i;
                tx_c.create_file(
                    500,
                    &format!("ckpt_{}_{}.pt", w, i),
                    10_000 + seq,
                    20_000 + seq,
                    7,
                    0o644,
                    1024,
                )
                .await
                .unwrap();
            }
        }));
    }

    let mut readers = Vec::new();
    for _ in 0..2 {
        let tx_r = tx.clone();
        let done_r = done.clone();
        readers.push(tokio::spawn(async move {
            let mut prev_count = 0i64;
            while !done_r.load(Ordering::Acquire) {
                let st = tx_r.dirstat(500).await.unwrap();
                assert!(
                    st.num_children >= prev_count,
                    "dirstat num_children regressed from {} to {} during concurrent L0 flush!",
                    prev_count,
                    st.num_children
                );
                assert_eq!(st.size_bytes, st.num_children * 1024);
                prev_count = st.num_children;
                tokio::task::yield_now().await;
            }
        }));
    }

    let grp_f = group.clone();
    let done_f = done.clone();
    let flusher = tokio::spawn(async move {
        while !done_f.load(Ordering::Acquire) {
            let _ = grp_f.state_machine().flush_to_lsm().unwrap();
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });

    for w in writers {
        w.await.unwrap();
    }
    done.store(true, Ordering::Release);
    for r in readers {
        r.await.unwrap();
    }
    flusher.await.unwrap();

    // Final flush & verification: exact 120 files, 120 * 1024 bytes, and 0 pending deltas in RAM!
    group.state_machine().flush_to_lsm().unwrap();
    assert_eq!(handler.attr_aggregator().pending_delta_count(), 0);

    let final_stat = tx.dirstat(500).await.unwrap();
    assert_eq!(final_stat.num_children, 120);
    assert_eq!(final_stat.size_bytes, 120 * 1024);

    group.shutdown().await.unwrap();
}
