//! Lease renewal for primary-anchored cross-shard 2PC.
//!
//! These tests use one Raft group per shard inside this process. They are not
//! three-node evidence, and they are not a first-ship pass.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metadb_core::{
    CompositeKeyRange, HlcTimestamp, HybridLogicalClock, InodeId, IntentOp, MetaDbError, MetaKey,
    Result, ShardEpoch, TxnState, TxnStatusRecord, WriteIntentRecord, ROOT_INODE_ID,
    SYSTEM_PREFIX_TXN_RECORD, SYSTEM_PREFIX_WRITE_INTENT,
};
use metadb_raft::{
    ChannelPool, MultiRaftRouter, RaftNodeOptions, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{
    encode_proposed_command, fresh_proposal_id, DirRenameLease, DistributedTxRouter,
    ShardDirectory, TwoPcCrashStage, TwoPcPrepareGate, TxRaftCmd, TxStateMachineHandler,
    TxnRecoveryWorker, DEFAULT_2PC_TTL_MS, MAX_2PC_LEASE_LIFETIME_MS,
};
use tempfile::tempdir;

struct RenameLease {
    token: u64,
    expire_ms: u64,
    deadline_ms: u64,
}

struct MultiShardTestCluster {
    shards: Vec<(
        CompositeKeyRange,
        Arc<ShardRaftGroup>,
        Arc<TxStateMachineHandler>,
    )>,
    clock: Arc<HybridLogicalClock>,
    next_token: AtomicU64,
    rename_lease: Mutex<Option<RenameLease>>,
}

#[tonic::async_trait]
impl ShardDirectory for MultiShardTestCluster {
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
        static NEXT_TXN: AtomicU64 = AtomicU64::new(1);
        Ok(((coord_node_id as u128) << 64) | NEXT_TXN.fetch_add(1, Ordering::Relaxed) as u128)
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
        let now = self.clock.now().physical_ms();
        let ttl = ttl_ms.max(100);
        let expire_ms = now.saturating_add(ttl);
        let deadline_ms = now.saturating_add(MAX_2PC_LEASE_LIFETIME_MS.max(ttl));
        let token = self.next_token.fetch_add(1, Ordering::AcqRel);
        *self.rename_lease.lock().unwrap() = Some(RenameLease {
            token,
            expire_ms,
            deadline_ms,
        });
        Ok(DirRenameLease {
            rename_fence_token: token,
            expire_hlc: HlcTimestamp::new(expire_ms, 0),
        })
    }

    async fn extend_dir_rename_lease(
        &self,
        token: u64,
        ttl_ms: u64,
    ) -> Result<Option<HlcTimestamp>> {
        let now = self.clock.now();
        let mut guard = self.rename_lease.lock().unwrap();
        let Some(lease) = guard.as_mut() else {
            return Err(MetaDbError::TransactionAborted(
                "directory rename lease is not active".into(),
            ));
        };
        if lease.token != token {
            return Err(MetaDbError::TransactionAborted(
                "directory rename lease fence token mismatch".into(),
            ));
        }
        if now.physical_ms() > lease.expire_ms {
            return Err(MetaDbError::TransactionAborted(
                "directory rename lease expired".into(),
            ));
        }
        if now.physical_ms() >= lease.deadline_ms {
            return Err(MetaDbError::TransactionAborted(
                "directory rename lease exceeds lifetime cap".into(),
            ));
        }
        let expire_ms = now
            .physical_ms()
            .saturating_add(ttl_ms.max(100))
            .min(lease.deadline_ms);
        if expire_ms > lease.expire_ms {
            lease.expire_ms = expire_ms;
        }
        Ok(Some(HlcTimestamp::new(lease.expire_ms, 0)))
    }

    async fn release_dir_rename_lease(&self, token: u64) {
        let mut guard = self.rename_lease.lock().unwrap();
        if guard.as_ref().is_some_and(|lease| lease.token == token) {
            *guard = None;
        }
    }
}

impl MultiShardTestCluster {
    async fn shutdown_groups(&self) {
        for (_, group, _) in &self.shards {
            group.shutdown().await.expect("shard shutdown");
        }
    }
}

struct Cluster {
    directory: Arc<MultiShardTestCluster>,
    shard1: Arc<ShardRaftGroup>,
    shard2: Arc<ShardRaftGroup>,
    handler1: Arc<TxStateMachineHandler>,
    handler2: Arc<TxStateMachineHandler>,
    clock: Arc<HybridLogicalClock>,
}

async fn boot_group(
    dir: &std::path::Path,
    sid: u64,
    range: CompositeKeyRange,
    cache: Arc<ShardedBlockCache>,
    clock: Arc<HybridLogicalClock>,
    router: MultiRaftRouter,
) -> (
    CompositeKeyRange,
    Arc<ShardRaftGroup>,
    Arc<TxStateMachineHandler>,
) {
    let base = dir.join(format!("shard_{sid}"));
    let watermarks = Arc::new(ShardWatermarkTracker::default());
    let handler = Arc::new(TxStateMachineHandler::new(
        sid,
        ShardEpoch::initial(),
        range.clone(),
        watermarks.clone(),
    ));
    let mut group = None;
    for attempt in 0..20 {
        match ShardRaftGroup::start(
            sid,
            1,
            &base,
            cache.clone(),
            LsmEngineConfig::default(),
            RaftNodeOptions::default(),
            handler.clone(),
            watermarks.clone(),
            clock.clone(),
            &router,
            ChannelPool::default(),
        )
        .await
        {
            Ok(started) => {
                group = Some(started);
                break;
            }
            Err(error) if error.to_string().contains("would block") && attempt < 19 => {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
            Err(error) => panic!("{error}"),
        }
    }
    let group = group.expect("shard lock released after shutdown");
    group
        .initialize_if_needed(BTreeMap::from([(1u64, "127.0.0.1:0".to_string())]))
        .await
        .unwrap();
    (range, group, handler)
}

async fn boot_cluster(dir: &std::path::Path) -> Cluster {
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let router = MultiRaftRouter::new();
    let s1 = boot_group(
        dir,
        1,
        CompositeKeyRange::meta_pid_range(0, 500).unwrap(),
        cache.clone(),
        clock.clone(),
        router.clone(),
    )
    .await;
    let s2 = boot_group(
        dir,
        2,
        CompositeKeyRange::meta_pid_range(500, u64::MAX).unwrap(),
        cache.clone(),
        clock.clone(),
        router.clone(),
    )
    .await;
    let s3 = boot_group(
        dir,
        3,
        CompositeKeyRange::quota_table_root(),
        cache.clone(),
        clock.clone(),
        router.clone(),
    )
    .await;
    let s4 = boot_group(
        dir,
        4,
        CompositeKeyRange::secondary_index_table_root(),
        cache,
        clock.clone(),
        router,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let shard1 = s1.1.clone();
    let handler1 = s1.2.clone();
    let shard2 = s2.1.clone();
    let handler2 = s2.2.clone();
    Cluster {
        directory: Arc::new(MultiShardTestCluster {
            shards: vec![s1, s2, s3, s4],
            clock: clock.clone(),
            next_token: AtomicU64::new(1),
            rename_lease: Mutex::new(None),
        }),
        shard1,
        shard2,
        handler1,
        handler2,
        clock,
    }
}

async fn seed_dirs(tx: &DistributedTxRouter) {
    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "src", 100, 10, 0o755)
        .await
        .unwrap();
    tx.mkdir(ROOT_INODE_ID, "dst", 600, 10, 0o755)
        .await
        .unwrap();
}

async fn txn_records(group: &ShardRaftGroup) -> Vec<TxnStatusRecord> {
    let (_, snap) = group
        .linearizable_read(group.current_epoch())
        .await
        .unwrap();
    group
        .engine()
        .scan_with_snapshot(
            &[SYSTEM_PREFIX_TXN_RECORD],
            &[SYSTEM_PREFIX_TXN_RECORD + 1],
            &snap,
        )
        .unwrap()
        .into_iter()
        .map(|(_, raw)| TxnStatusRecord::decode(&raw).unwrap())
        .collect()
}

async fn intent_records(group: &ShardRaftGroup) -> Vec<WriteIntentRecord> {
    let (_, snap) = group
        .linearizable_read(group.current_epoch())
        .await
        .unwrap();
    group
        .engine()
        .scan_with_snapshot(
            &[SYSTEM_PREFIX_WRITE_INTENT],
            &[SYSTEM_PREFIX_WRITE_INTENT + 1],
            &snap,
        )
        .unwrap()
        .into_iter()
        .map(|(_, raw)| WriteIntentRecord::decode(&raw).unwrap())
        .collect()
}

async fn recover(cluster: &Cluster) {
    TxnRecoveryWorker::run_recovery_pass_on_shard(
        &cluster.shard1,
        &cluster.handler1,
        cluster.directory.as_ref(),
        &cluster.clock,
    )
    .await
    .unwrap();
    TxnRecoveryWorker::run_recovery_pass_on_shard(
        &cluster.shard2,
        &cluster.handler2,
        cluster.directory.as_ref(),
        &cluster.clock,
    )
    .await
    .unwrap();
}

async fn propose_extend(
    group: &ShardRaftGroup,
    clock: &HybridLogicalClock,
    txn_id: u128,
    coord_node_id: u64,
    start_hlc: HlcTimestamp,
    expire_hlc: HlcTimestamp,
    rename_fence_token: Option<u64>,
    target_user_keys: Vec<Vec<u8>>,
) -> Result<()> {
    let cmd = TxRaftCmd::Extend2pcLease {
        txn_id,
        coord_node_id,
        expire_hlc,
        start_hlc,
        rename_fence_token,
        target_user_keys,
    };
    let payload = encode_proposed_command(fresh_proposal_id(), &cmd).unwrap();
    let response = group
        .propose(group.current_epoch(), clock.now(), payload)
        .await?;
    response.result?;
    Ok(())
}

fn keys_on(handler: &TxStateMachineHandler, keys: &[Vec<u8>]) -> Vec<Vec<u8>> {
    let range = handler.key_range();
    keys.iter()
        .filter(|key| range.contains_key(key))
        .cloned()
        .collect()
}

fn holder_deadlines(records: &[WriteIntentRecord], txn_id: u128) -> Vec<u64> {
    let mut deadlines = Vec::new();
    for record in records {
        match &record.op {
            IntentOp::DirExistsFenceSet { holders } => {
                if let Some((_, expire)) = holders.get(&txn_id) {
                    deadlines.push(expire.physical_ms());
                }
            }
            _ if record.txn_id == txn_id => deadlines.push(record.expire_hlc.physical_ms()),
            _ => {}
        }
    }
    deadlines
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn progressing_rename_commits_past_the_initial_three_second_ttl() {
    assert_eq!(DEFAULT_2PC_TTL_MS, 3_000);
    assert!(MAX_2PC_LEASE_LIFETIME_MS >= 30_000);

    let dir = tempdir().unwrap();
    let cluster = boot_cluster(dir.path()).await;
    let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone());
    seed_dirs(&tx).await;
    tx.create_file(100, "live.txt", 1001, 8001, 10, 0o644, 64)
        .await
        .unwrap();
    let gate = Arc::new(TwoPcPrepareGate::new());
    let rename = {
        let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone())
            .with_post_prepare_gate(gate.clone());
        tokio::spawn(async move { tx.rename(100, "live.txt", 600, "moved.txt", false).await })
    };
    gate.wait_prepared().await;
    tokio::time::sleep(Duration::from_millis(3_200)).await;

    let records = txn_records(&cluster.shard1).await;
    assert_eq!(records.len(), 1);
    let pending = &records[0];
    assert!(matches!(pending.state, TxnState::Pending));
    let now = cluster.clock.now();
    assert!(
        pending.expire_hlc > now,
        "renewal must push the primary deadline past now; expire {:?} now {:?}",
        pending.expire_hlc,
        now
    );
    assert!(pending.expire_hlc.physical_ms() > pending.start_hlc.physical_ms() + 3_000);
    let intents = intent_records(&cluster.shard2).await;
    let deadlines = holder_deadlines(&intents, pending.txn_id);
    assert!(
        !deadlines.is_empty(),
        "participant intents must still exist"
    );
    assert!(deadlines
        .iter()
        .all(|deadline| *deadline > now.physical_ms()));

    let wrong = propose_extend(
        &cluster.shard1,
        &cluster.clock,
        pending.txn_id,
        99,
        pending.start_hlc,
        HlcTimestamp::new(pending.expire_hlc.physical_ms().saturating_add(1_000), 0),
        pending.rename_fence_token,
        pending.participant_keys.clone(),
    )
    .await;
    assert!(wrong.unwrap_err().to_string().contains("not held"));

    recover(&cluster).await;
    assert!(matches!(
        txn_records(&cluster.shard1).await[0].state,
        TxnState::Pending
    ));

    gate.release();
    rename.await.unwrap().unwrap();
    assert!(tx.lookup(100, "live.txt").await.unwrap().is_none());
    assert!(tx.lookup(600, "moved.txt").await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn abandoned_prepare_expires_and_renew_after_expiry_is_rejected() {
    let dir = tempdir().unwrap();
    let cluster = boot_cluster(dir.path()).await;
    let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone());
    seed_dirs(&tx).await;
    tx.create_file(100, "orphan.txt", 1002, 8002, 10, 0o644, 32)
        .await
        .unwrap();
    let error = tx
        .rename_with_fault_injection(
            100,
            "orphan.txt",
            600,
            "should_not.txt",
            false,
            TwoPcCrashStage::StopAfterPrepareBeforePrimaryCommit,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, MetaDbError::RpcError(_)));

    let before = txn_records(&cluster.shard1).await;
    assert_eq!(before.len(), 1);
    let original_expire = before[0].expire_hlc;
    tokio::time::sleep(Duration::from_millis(DEFAULT_2PC_TTL_MS + 500)).await;

    let extend = propose_extend(
        &cluster.shard1,
        &cluster.clock,
        before[0].txn_id,
        1,
        before[0].start_hlc,
        HlcTimestamp::new(cluster.clock.now().physical_ms().saturating_add(5_000), 0),
        before[0].rename_fence_token,
        keys_on(&cluster.handler1, &before[0].participant_keys),
    )
    .await
    .unwrap_err();
    assert!(
        extend.to_string().contains("rejected after expiry"),
        "{extend}"
    );
    let after = txn_records(&cluster.shard1).await;
    assert!(matches!(after[0].state, TxnState::Pending));
    assert_eq!(after[0].expire_hlc, original_expire);

    recover(&cluster).await;
    let revived = propose_extend(
        &cluster.shard1,
        &cluster.clock,
        before[0].txn_id,
        1,
        before[0].start_hlc,
        HlcTimestamp::new(cluster.clock.now().physical_ms().saturating_add(5_000), 0),
        before[0].rename_fence_token,
        vec![MetaKey::child(100, "orphan.txt").encode()],
    )
    .await
    .unwrap_err();
    assert!(
        revived.to_string().contains("cannot revive")
            || revived.to_string().contains("no live transaction")
            || revived.to_string().contains("rejected after expiry"),
        "{revived}"
    );
    assert!(intent_records(&cluster.shard1).await.is_empty());
    assert!(intent_records(&cluster.shard2).await.is_empty());
    assert!(tx.lookup(100, "orphan.txt").await.unwrap().is_some());
    assert!(tx.lookup(600, "should_not.txt").await.unwrap().is_none());
    assert!(txn_records(&cluster.shard1)
        .await
        .iter()
        .all(|record| !matches!(record.state, TxnState::Committed { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn committed_primary_rolls_forward_after_expire() {
    let dir = tempdir().unwrap();
    let cluster = boot_cluster(dir.path()).await;
    let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone())
        .with_ttl_ms(200);
    seed_dirs(&tx).await;
    tx.create_file(100, "committed.txt", 1003, 8003, 10, 0o644, 8)
        .await
        .unwrap();
    let error = tx
        .rename_with_fault_injection(
            100,
            "committed.txt",
            600,
            "landed.txt",
            false,
            TwoPcCrashStage::StopAfterPrimaryCommitBeforeSecondary,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, MetaDbError::RpcError(_)));
    let records = txn_records(&cluster.shard1).await;
    assert!(matches!(records[0].state, TxnState::Committed { .. }));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(cluster.clock.now() > records[0].expire_hlc);

    let landed = tx.lookup(600, "landed.txt").await.unwrap().unwrap();
    assert_eq!(landed.attr.inode_id, 1003);
    assert!(tx.lookup(100, "committed.txt").await.unwrap().is_none());
    recover(&cluster).await;
    assert!(tx.lookup(600, "landed.txt").await.unwrap().is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_prepare_delayed_past_three_seconds_still_commits() {
    let dir = tempdir().unwrap();
    let cluster = boot_cluster(dir.path()).await;
    let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone());
    seed_dirs(&tx).await;
    tx.create_file(100, "slow.txt", 1004, 8004, 10, 0o644, 4)
        .await
        .unwrap();
    let delayed = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone())
        .with_pre_prepare_hold(Duration::from_millis(3_200));
    delayed
        .rename(100, "slow.txt", 600, "slow-moved.txt", false)
        .await
        .unwrap();
    assert!(tx.lookup(600, "slow-moved.txt").await.unwrap().is_some());
    assert!(tx.lookup(100, "slow.txt").await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn work_past_the_hard_cap_is_aborted_and_recovered() {
    let dir = tempdir().unwrap();
    let cluster = boot_cluster(dir.path()).await;
    let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone());
    seed_dirs(&tx).await;
    tx.create_file(100, "capped.txt", 1005, 8005, 10, 0o644, 4)
        .await
        .unwrap();
    let gate = Arc::new(TwoPcPrepareGate::new());
    let rename = {
        let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone())
            .with_ttl_ms(200)
            .with_max_lease_lifetime_ms(700)
            .with_post_prepare_gate(gate.clone());
        tokio::spawn(async move {
            tx.rename(100, "capped.txt", 600, "capped-dst.txt", false)
                .await
        })
    };
    gate.wait_prepared().await;
    tokio::time::sleep(Duration::from_millis(350)).await;
    recover(&cluster).await;
    let mid = txn_records(&cluster.shard1).await;
    assert!(
        matches!(mid[0].state, TxnState::Pending),
        "keepalive must still be inside the cap"
    );
    assert!(mid[0].expire_hlc > cluster.clock.now());

    tokio::time::sleep(Duration::from_millis(700)).await;
    recover(&cluster).await;
    gate.release();
    let error = rename.await.unwrap().unwrap_err();
    assert!(
        matches!(error, MetaDbError::TransactionAborted(_)),
        "{error}"
    );
    assert!(tx.lookup(100, "capped.txt").await.unwrap().is_some());
    assert!(tx.lookup(600, "capped-dst.txt").await.unwrap().is_none());
    assert!(intent_records(&cluster.shard1).await.is_empty());
    assert!(intent_records(&cluster.shard2).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lookup_and_rollback_remain_correct_past_default_mvcc_retention() {
    let dir = tempdir().unwrap();
    let cluster = boot_cluster(dir.path()).await;
    let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone());
    seed_dirs(&tx).await;
    tx.create_file(100, "kept.txt", 1006, 8006, 10, 0o644, 16)
        .await
        .unwrap();
    let gate = Arc::new(TwoPcPrepareGate::new());
    let source_key = MetaKey::child(100, "kept.txt").encode();
    let rename = {
        let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone())
            .with_post_prepare_gate(gate.clone())
            .with_abandon_after_prepare_gate(true);
        tokio::spawn(async move { tx.rename(100, "kept.txt", 600, "lost.txt", false).await })
    };
    gate.wait_prepared().await;
    tokio::time::sleep(Duration::from_millis(5_200)).await;

    let records = txn_records(&cluster.shard1).await;
    let start_hlc = records[0].start_hlc;
    assert!(records[0].expire_hlc > cluster.clock.now());
    let historical = cluster
        .shard1
        .engine()
        .get(&source_key, start_hlc)
        .expect("lease must extend MVCC retention for the prepared key");
    assert!(historical.is_some(), "pre-image must survive the 5s window");
    let listed = cluster
        .shard1
        .engine()
        .scan_range(
            &MetaKey::encode_pid_prefix_start(100),
            &MetaKey::encode_pid_prefix_end(100),
            start_hlc,
        )
        .unwrap();
    assert!(listed.iter().any(|(key, _)| key == &source_key));

    gate.release();
    let error = rename.await.unwrap().unwrap_err();
    assert!(
        matches!(error, MetaDbError::TransactionAborted(_)),
        "{error}"
    );
    recover(&cluster).await;
    let kept = tx.lookup(100, "kept.txt").await.unwrap().unwrap();
    assert_eq!(kept.attr.inode_id, 1006);
    assert!(tx.lookup(600, "lost.txt").await.unwrap().is_none());
    assert!(intent_records(&cluster.shard1).await.is_empty());
    assert!(intent_records(&cluster.shard2).await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn directory_rename_extends_lease_and_fence_holder_together() {
    let dir = tempdir().unwrap();
    let cluster = boot_cluster(dir.path()).await;
    let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone());
    seed_dirs(&tx).await;
    tx.mkdir(100, "child", 150, 10, 0o755).await.unwrap();
    let gate = Arc::new(TwoPcPrepareGate::new());
    let rename = {
        let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone())
            .with_post_prepare_gate(gate.clone());
        tokio::spawn(async move { tx.rename(100, "child", 600, "child", false).await })
    };
    gate.wait_prepared().await;
    tokio::time::sleep(Duration::from_millis(3_200)).await;

    let records = txn_records(&cluster.shard1).await;
    let pending = &records[0];
    let now = cluster.clock.now().physical_ms();
    assert!(pending.expire_hlc.physical_ms() > pending.start_hlc.physical_ms() + 3_000);
    assert!(pending.expire_hlc.physical_ms() > now);
    let lease_expire = cluster
        .directory
        .rename_lease
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .expire_ms;
    assert!(lease_expire > pending.start_hlc.physical_ms() + 3_000);
    assert!(lease_expire > now);
    let mut saw_holder = false;
    for group in [&cluster.shard1, &cluster.shard2] {
        for deadline in holder_deadlines(&intent_records(group).await, pending.txn_id) {
            saw_holder = true;
            assert!(deadline > now);
            assert!(deadline > pending.start_hlc.physical_ms() + 3_000);
        }
    }
    assert!(saw_holder, "DirExistsFence holder must be renewed");
    recover(&cluster).await;
    assert!(matches!(
        txn_records(&cluster.shard1).await[0].state,
        TxnState::Pending
    ));
    gate.release();
    rename.await.unwrap().unwrap();
    assert!(tx.lookup(600, "child").await.unwrap().is_some());
    assert!(tx.lookup(100, "child").await.unwrap().is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extended_expire_hlc_survives_restart_after_flush_and_purge() {
    let dir = tempdir().unwrap();
    let cluster = boot_cluster(dir.path()).await;
    let tx = DistributedTxRouter::new(1, cluster.clock.clone(), cluster.directory.clone())
        .with_ttl_ms(300);
    seed_dirs(&tx).await;
    tx.create_file(100, "durable.txt", 1007, 8007, 10, 0o644, 4)
        .await
        .unwrap();
    let error = tx
        .rename_with_fault_injection(
            100,
            "durable.txt",
            600,
            "durable-dst.txt",
            false,
            TwoPcCrashStage::StopAfterPrepareBeforePrimaryCommit,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, MetaDbError::RpcError(_)));
    let pending = txn_records(&cluster.shard1).await.remove(0);
    let extended = HlcTimestamp::new(cluster.clock.now().physical_ms().saturating_add(8_000), 0);
    for (group, handler) in [
        (&cluster.shard1, &cluster.handler1),
        (&cluster.shard2, &cluster.handler2),
    ] {
        let keys = keys_on(handler, &pending.participant_keys);
        if keys.is_empty() {
            continue;
        }
        propose_extend(
            group,
            &cluster.clock,
            pending.txn_id,
            1,
            pending.start_hlc,
            extended,
            pending.rename_fence_token,
            keys,
        )
        .await
        .unwrap();
    }
    let renewed = txn_records(&cluster.shard1).await;
    assert_eq!(renewed[0].expire_hlc.physical_ms(), extended.physical_ms());

    let purged1 = cluster.shard1.flush_snapshot_and_purge().await.unwrap();
    let purged2 = cluster.shard2.flush_snapshot_and_purge().await.unwrap();
    assert!(
        purged1 > 0 && purged2 > 0,
        "flush+purge must drop both shard logs (purged {purged1}, {purged2})"
    );
    cluster.directory.shutdown_groups().await;
    drop(tx);
    drop(cluster);

    let clock = Arc::new(HybridLogicalClock::new());
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let router = MultiRaftRouter::new();
    let s1 = boot_group(
        dir.path(),
        1,
        CompositeKeyRange::meta_pid_range(0, 500).unwrap(),
        cache.clone(),
        clock.clone(),
        router.clone(),
    )
    .await;
    let s2 = boot_group(
        dir.path(),
        2,
        CompositeKeyRange::meta_pid_range(500, u64::MAX).unwrap(),
        cache.clone(),
        clock.clone(),
        router.clone(),
    )
    .await;
    let s3 = boot_group(
        dir.path(),
        3,
        CompositeKeyRange::quota_table_root(),
        cache.clone(),
        clock.clone(),
        router.clone(),
    )
    .await;
    let s4 = boot_group(
        dir.path(),
        4,
        CompositeKeyRange::secondary_index_table_root(),
        cache,
        clock.clone(),
        router,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    let restarted = Cluster {
        shard1: s1.1.clone(),
        handler1: s1.2.clone(),
        shard2: s2.1.clone(),
        handler2: s2.2.clone(),
        directory: Arc::new(MultiShardTestCluster {
            shards: vec![s1, s2, s3, s4],
            clock: clock.clone(),
            next_token: AtomicU64::new(1),
            rename_lease: Mutex::new(None),
        }),
        clock: clock.clone(),
    };
    let restored = txn_records(&restarted.shard1).await;
    assert_eq!(restored.len(), 1, "TxnRecord must survive purge");
    assert_eq!(restored[0].expire_hlc.physical_ms(), extended.physical_ms());
    assert!(matches!(restored[0].state, TxnState::Pending));
    let resumed = DistributedTxRouter::new(1, clock.clone(), restarted.directory.clone())
        .resume_pending_lease(&restored[0].primary_key, restored[0].txn_id)
        .await
        .unwrap();
    assert!(resumed.physical_ms() >= extended.physical_ms());
    let again = txn_records(&restarted.shard1).await;
    assert_eq!(again[0].expire_hlc.physical_ms(), resumed.physical_ms());
    assert!(!intent_records(&restarted.shard2).await.is_empty());
}
