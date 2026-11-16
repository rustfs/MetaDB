use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::Duration;

use metadb_core::{
    CompositeKeyRange, HybridLogicalClock, InodeId, MetaDbError, MetaKey, Result, ShardEpoch,
    TxnId, TxnStatusRecord, ROOT_INODE_ID, SYSTEM_PREFIX_TXN_RECORD, SYSTEM_PREFIX_WRITE_INTENT,
};
use metadb_raft::{
    ChannelPool, MultiRaftRouter, RaftNodeOptions, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{
    AuxDispatcher, DirRenameLease, DistributedTxRouter, RoutedShardHandle, SetAttrPatch,
    ShardDirectory, TwoPcCrashStage, TxStateMachineHandler, TxnRecoveryWorker,
};

struct TestDirectory {
    shards: Vec<(CompositeKeyRange, RoutedShardHandle)>,
}

#[tonic::async_trait]
impl ShardDirectory for TestDirectory {
    fn route_user_key(&self, key: &[u8]) -> Result<RoutedShardHandle> {
        self.shards
            .iter()
            .find(|(range, _)| range.contains_key(key))
            .map(|(_, handle)| handle.clone())
            .ok_or_else(|| MetaDbError::ShardRoutingError("unrouted test key".into()))
    }

    fn allocate_txn_id(&self, node: u64) -> Result<TxnId> {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Ok(((node as u128) << 64) | NEXT.fetch_add(1, Ordering::Relaxed) as u128)
    }

    fn route_user_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<RoutedShardHandle>> {
        Ok(self
            .shards
            .iter()
            .filter(|(range, _)| range.overlaps(start, end))
            .map(|(_, handle)| handle.clone())
            .collect())
    }

    async fn acquire_dir_rename_lease(&self, _: InodeId, _: u64) -> Result<DirRenameLease> {
        unreachable!("file rename needs no directory lease")
    }

    async fn release_dir_rename_lease(&self, _: u64) {}
}

/// Pause immediately after both rename reads, before the first Prepare, without
/// depending on task scheduling or arbitrary sleeps to create a stale read.
struct PausedDirectory {
    base: Arc<TestDirectory>,
    source_key: Vec<u8>,
    visits: AtomicU64,
    reached: mpsc::Sender<()>,
    released: Arc<(Mutex<bool>, Condvar)>,
}

#[tonic::async_trait]
impl ShardDirectory for PausedDirectory {
    fn route_user_key(&self, key: &[u8]) -> Result<RoutedShardHandle> {
        if key == self.source_key && self.visits.fetch_add(1, Ordering::Relaxed) == 2 {
            self.reached.send(()).unwrap();
            let (lock, cv) = &*self.released;
            let ready = lock.lock().unwrap();
            let (ready, timeout) = cv
                .wait_timeout_while(ready, Duration::from_secs(10), |r| !*r)
                .unwrap();
            assert!(
                *ready && !timeout.timed_out(),
                "concurrent edit never released rename"
            );
        }
        self.base.route_user_key(key)
    }

    fn allocate_txn_id(&self, node: u64) -> Result<TxnId> {
        self.base.allocate_txn_id(node)
    }

    fn route_user_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<RoutedShardHandle>> {
        self.base.route_user_range(start, end)
    }

    async fn acquire_dir_rename_lease(&self, id: InodeId, ttl: u64) -> Result<DirRenameLease> {
        self.base.acquire_dir_rename_lease(id, ttl).await
    }

    async fn release_dir_rename_lease(&self, token: u64) {
        self.base.release_dir_rename_lease(token).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_rename_source_destination_and_victim_never_commit() {
    let temp = tempfile::tempdir().unwrap();
    let raft_router = MultiRaftRouter::new();
    let clock = Arc::new(HybridLogicalClock::new());
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let ranges = [
        CompositeKeyRange::meta_pid_range(0, 500).unwrap(),
        CompositeKeyRange::meta_pid_range(500, u64::MAX).unwrap(),
        CompositeKeyRange::quota_table_root(),
        CompositeKeyRange::secondary_index_table_root(),
    ];
    let mut shards = Vec::new();
    for (index, range) in ranges.into_iter().enumerate() {
        let sid = index as u64 + 1;
        let wm = Arc::new(ShardWatermarkTracker::default());
        let handler = Arc::new(TxStateMachineHandler::new(
            sid,
            ShardEpoch::initial(),
            range.clone(),
            wm.clone(),
        ));
        let group = ShardRaftGroup::start(
            sid,
            1,
            temp.path().join(format!("shard_{sid}")),
            cache.clone(),
            LsmEngineConfig::default(),
            RaftNodeOptions::default(),
            handler.clone(),
            wm,
            clock.clone(),
            &raft_router,
            ChannelPool::default(),
        )
        .await
        .unwrap();
        group
            .initialize_if_needed(BTreeMap::from([(1, "127.0.0.1:0".into())]))
            .await
            .unwrap();
        shards.push((range, (ShardEpoch::initial(), group, handler)));
    }
    let directory = Arc::new(TestDirectory { shards });
    let tx = DistributedTxRouter::new(1, clock.clone(), directory.clone());
    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "src", 100, 10, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "dst", 600, 10, 0o755)
        .await
        .unwrap();

    for (case, overwrite) in [("source", false), ("absent", false), ("victim", true)] {
        tx.create_file(100, case, 1000, 8000, 10, 0o644, 10)
            .await
            .unwrap();
        if overwrite {
            tx.create_file(600, case, 2000, 9000, 20, 0o644, 20)
                .await
                .unwrap();
        }
        let (send, recv) = mpsc::channel();
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        let paused = Arc::new(PausedDirectory {
            base: directory.clone(),
            source_key: MetaKey::child(100, case).encode(),
            visits: AtomicU64::new(0),
            reached: send,
            released: released.clone(),
        });
        let stale_router = DistributedTxRouter::new(1, clock.clone(), paused);
        let rename =
            tokio::spawn(async move { stale_router.rename(100, case, 600, case, overwrite).await });
        // Run the blocking receive off the runtime so all Raft workers stay schedulable.
        tokio::task::spawn_blocking(move || recv.recv_timeout(Duration::from_secs(10)).unwrap())
            .await
            .unwrap();
        match case {
            "source" => {
                tx.setattr(
                    100,
                    case,
                    SetAttrPatch {
                        size_bytes: Some(80),
                        uid: Some(30),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            }
            "absent" => {
                tx.create_file(600, case, 3000, 10000, 40, 0o600, 30)
                    .await
                    .unwrap();
            }
            "victim" => {
                tx.setattr(
                    600,
                    case,
                    SetAttrPatch {
                        size_bytes: Some(90),
                        uid: Some(50),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            }
            _ => unreachable!(),
        }
        *released.0.lock().unwrap() = true;
        released.1.notify_all();
        let error = rename.await.unwrap().unwrap_err();
        assert!(
            matches!(error, MetaDbError::TransactionAborted(_)),
            "{case}: {error:?}"
        );
        let source = tx.lookup(100, case).await.unwrap().unwrap();
        assert_eq!(
            source.attr.size_bytes,
            if case == "source" { 80 } else { 10 }
        );
        let destination = tx.lookup(600, case).await.unwrap();
        match case {
            "source" => assert!(destination.is_none()),
            "absent" => assert_eq!(destination.as_ref().unwrap().attr.inode_id, 3000),
            "victim" => assert_eq!(destination.as_ref().unwrap().attr.size_bytes, 90),
            _ => unreachable!(),
        }
        for (_, (_, group, _)) in &directory.shards[..2] {
            let (_, snapshot) = group
                .linearizable_read(group.current_epoch())
                .await
                .unwrap();
            let intents = group
                .engine()
                .scan_with_snapshot(
                    &[SYSTEM_PREFIX_WRITE_INTENT],
                    &[SYSTEM_PREFIX_WRITE_INTENT + 1],
                    &snapshot,
                )
                .unwrap();
            assert!(intents.is_empty(), "rejected {case} prepare leaked intents");
        }
        tx.unlink(100, case).await.unwrap();
        if destination.is_some() {
            tx.unlink(600, case).await.unwrap();
        }
    }

    // Recreating a coordinator on the same node must not reuse the first
    // prepared transaction's identity or overwrite its retained primary record.
    for (name, fid) in [("restart_a", 11000), ("restart_b", 11001)] {
        tx.create_file(100, name, fid, fid, 60, 0o644, 1)
            .await
            .unwrap();
        let recreated =
            DistributedTxRouter::new(1, clock.clone(), directory.clone()).with_ttl_ms(1000);
        let error = recreated
            .rename_with_fault_injection(
                100,
                name,
                600,
                name,
                false,
                TwoPcCrashStage::StopAfterPrepareBeforePrimaryCommit,
            )
            .await
            .unwrap_err();
        assert!(matches!(error, MetaDbError::RpcError(_)));
    }
    let primary = &directory.shards[0].1 .1;
    let (_, snapshot) = primary
        .linearizable_read(primary.current_epoch())
        .await
        .unwrap();
    let records = primary
        .engine()
        .scan_with_snapshot(
            &[SYSTEM_PREFIX_TXN_RECORD],
            &[SYSTEM_PREFIX_TXN_RECORD + 1],
            &snapshot,
        )
        .unwrap();
    assert_eq!(
        records.len(),
        2,
        "recreated coordinator aliased an orphan transaction"
    );
    let ids: std::collections::BTreeSet<_> = records
        .iter()
        .map(|(_, raw)| TxnStatusRecord::decode(raw).unwrap().txn_id)
        .collect();
    assert_eq!(ids.len(), 2);
    drop(snapshot);
    tokio::time::sleep(Duration::from_millis(1100)).await;
    for (_, (_, group, handler)) in &directory.shards[..2] {
        TxnRecoveryWorker::run_recovery_pass_on_shard(group, handler, directory.as_ref(), &clock)
            .await
            .unwrap();
    }
    for name in ["restart_a", "restart_b"] {
        assert!(tx.lookup(100, name).await.unwrap().is_some());
        assert!(tx.lookup(600, name).await.unwrap().is_none());
        tx.unlink(100, name).await.unwrap();
    }

    for (_, (_, group, handler)) in &directory.shards[..2] {
        AuxDispatcher::drain_shard_outbox(group, handler, directory.as_ref(), &clock)
            .await
            .unwrap();
    }
    for uid in [20, 30, 40, 50, 60] {
        let quota = tx.get_quota(uid).await.unwrap().unwrap();
        assert_eq!((quota.used_bytes, quota.used_inodes), (0, 0), "uid={uid}");
    }
    for fid in [8000, 9000, 10000, 11000, 11001] {
        assert!(
            tx.get_secondary_index(fid).await.unwrap().is_none(),
            "fid={fid}"
        );
    }
    for pid in [100, 600] {
        let attr = tx.dirstat(pid).await.unwrap();
        assert_eq!((attr.num_children, attr.size_bytes), (0, 0));
    }
    for (_, (_, group, _)) in &directory.shards {
        group.shutdown().await.unwrap();
    }
}
