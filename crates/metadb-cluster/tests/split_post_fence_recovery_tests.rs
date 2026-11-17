use std::path::Path;
use std::sync::Arc;

use metadb_cluster::{
    CompositeMetaCache, DurableCellManager, DurableGlobalManager, DurableIdAllocator,
    RaftShardSplitWorker, SplitCrashPoint,
};
use metadb_core::codec;
use metadb_core::{
    AttrDeltaSpec, CompositeKeyRange, HlcTimestamp, HybridLogicalClock, IntentOp, MetaDbError,
    MetaKey, TxnRecordKey, TxnState, TxnStatusRecord, WriteIntentKey, WriteIntentRecord,
    ROOT_INODE_ID,
};
use metadb_raft::{RaftNodeOptions, ShardGroupHost};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{DistributedTxRouter, TxRaftCmd};

struct Fixture {
    host: Arc<ShardGroupHost>,
    cell: Arc<DurableCellManager>,
    clock: Arc<HybridLogicalClock>,
    tx: DistributedTxRouter,
}

async fn try_open(dir: &Path) -> metadb_core::Result<Fixture> {
    let clock = Arc::new(HybridLogicalClock::new());
    let allocator = Arc::new(DurableIdAllocator::open(dir.join("alloc"))?);
    let cache = Arc::new(CompositeMetaCache::new());
    let host = Arc::new(ShardGroupHost::open(
        1,
        dir.join("be"),
        Arc::new(ShardedBlockCache::new(8 * 1024 * 1024)),
        clock.clone(),
    )?);
    let cell = DurableCellManager::open_or_bootstrap(
        1,
        dir.join("cell"),
        host.clone(),
        "127.0.0.1:0".into(),
        true,
        usize::MAX,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        allocator.clone(),
        cache.clone(),
    )
    .await?;
    let directory = Arc::new(DurableGlobalManager::new(clock.clone(), allocator, cache));
    directory.register_cell(CompositeKeyRange::all_user_tables(), cell.clone());
    let tx = DistributedTxRouter::new(1, clock.clone(), directory);
    Ok(Fixture {
        host,
        cell,
        clock,
        tx,
    })
}

async fn open(dir: &Path) -> Fixture {
    try_open(dir)
        .await
        .expect("startup must complete a fenced split despite a new retained-range intent")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fenced_split_finishes_on_restart_with_new_retained_range_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let split_key = MetaKey::encode_pid_prefix_start(500);
    let left_key = MetaKey::child(10, "left").encode();
    let right_key = MetaKey::child(900, "right").encode();
    let intent_key = WriteIntentKey::new(left_key.clone()).encode();
    let txn_id = 0xF3CE_u128;
    let txn_key = TxnRecordKey { txn_id }.encode();

    let child_id = {
        let fixture = open(dir.path()).await;
        fixture
            .tx
            .init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 7, 0o755)
            .await
            .unwrap();
        fixture
            .tx
            .mkdir(ROOT_INODE_ID, "left_dir", 10, 7, 0o755)
            .await
            .unwrap();
        fixture
            .tx
            .mkdir(ROOT_INODE_ID, "right_dir", 900, 7, 0o755)
            .await
            .unwrap();
        fixture
            .tx
            .create_file(10, "left", (1 << 63) + 10, 10, 7, 0o644, 17)
            .await
            .unwrap();
        fixture
            .tx
            .create_file(900, "right", (1 << 63) + 900, 900, 7, 0o644, 29)
            .await
            .unwrap();

        let route = fixture
            .cell
            .list_routes()
            .into_iter()
            .find(|route| route.shard_id == 1)
            .unwrap();
        let (_, parent, handler) = fixture.cell.lookup_local_handle(1).unwrap();
        let child_id = fixture
            .cell
            .alloc_split_plan(0xF3CE, 1, split_key.clone(), 2, None)
            .unwrap();
        let paused = RaftShardSplitWorker::execute_local_split(
            &fixture.host,
            &route,
            &parent,
            &handler,
            child_id,
            split_key.clone(),
            None,
            LsmEngineConfig::default(),
            RaftNodeOptions::default(),
            2,
            Some(SplitCrashPoint::AfterFence),
        )
        .await
        .err()
        .expect("the injected crash must interrupt the split");
        assert!(paused.to_string().contains("injected split crash"));
        assert_eq!(handler.key_range().end_key, split_key);
        assert!(!fixture.host.shard_dir(child_id).exists());

        // A successful fence drained the old transaction state. The parent can
        // subsequently accept an unrelated transaction wholly in its left range.
        let start_hlc = fixture.clock.now();
        let expire_hlc = HlcTimestamp::new(start_hlc.physical_ms() + 60_000, 0);
        let prepare = TxRaftCmd::Prepare2pc {
            txn_id,
            is_primary: true,
            primary_key: left_key.clone(),
            participant_keys: vec![left_key.clone()],
            start_hlc,
            expire_hlc,
            rename_fence_token: None,
            intents: vec![(
                left_key.clone(),
                IntentOp::Delete {
                    attr_deltas: vec![AttrDeltaSpec {
                        pid: 10,
                        delta_children: -1,
                        delta_bytes: -17,
                    }],
                    aux_specs: vec![],
                },
            )],
        };
        parent
            .propose(
                parent.current_epoch(),
                fixture.clock.now(),
                codec::serialize(&prepare).unwrap(),
            )
            .await
            .unwrap();
        fixture.host.shutdown_all_no_flush().await.unwrap();
        child_id
    };

    let fixture = open(dir.path()).await;
    let (_, parent, handler) = fixture.cell.lookup_local_handle(1).unwrap();
    let (_, child, child_handler) = fixture.cell.lookup_local_handle(child_id).unwrap();
    assert_eq!(handler.key_range().end_key, split_key);
    assert!(child_handler.key_range().contains_key(&right_key));
    assert!(!child_handler.key_range().contains_key(&left_key));
    let (_, parent_snapshot) = parent
        .linearizable_read(parent.current_epoch())
        .await
        .unwrap();
    let raw_intent = parent
        .engine()
        .get_with_snapshot(&intent_key, &parent_snapshot)
        .unwrap()
        .expect("new retained-range intent must stay on the parent");
    assert_eq!(
        WriteIntentRecord::decode(&raw_intent).unwrap().txn_id,
        txn_id
    );
    let raw_txn = parent
        .engine()
        .get_with_snapshot(&txn_key, &parent_snapshot)
        .unwrap()
        .expect("new retained-range primary record must stay on the parent");
    assert_eq!(
        TxnStatusRecord::decode(&raw_txn).unwrap().state,
        TxnState::Pending
    );
    assert!(parent
        .engine()
        .get_with_snapshot(&right_key, &parent_snapshot)
        .unwrap()
        .is_none());
    drop(parent_snapshot);

    let (_, child_snapshot) = child
        .linearizable_read(child.current_epoch())
        .await
        .unwrap();
    for forbidden_key in [&intent_key[..], &txn_key[..], &left_key[..]] {
        assert!(child
            .engine()
            .get_with_snapshot(forbidden_key, &child_snapshot)
            .unwrap()
            .is_none());
    }
    assert!(child
        .engine()
        .get_with_snapshot(&right_key, &child_snapshot)
        .unwrap()
        .is_some());
    drop(child_snapshot);

    let abort = TxRaftCmd::Abort2pc {
        txn_id,
        target_user_keys: vec![left_key],
    };
    parent
        .propose(
            parent.current_epoch(),
            fixture.clock.now(),
            codec::serialize(&abort).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        fixture
            .tx
            .lookup(10, "left")
            .await
            .unwrap()
            .unwrap()
            .attr
            .fid,
        Some(10)
    );
    assert_eq!(
        fixture
            .tx
            .lookup(900, "right")
            .await
            .unwrap()
            .unwrap()
            .attr
            .fid,
        Some(900)
    );
    fixture.host.shutdown_all().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_transaction_record_in_pending_split_still_aborts_startup() {
    let dir = tempfile::tempdir().unwrap();
    {
        let fixture = open(dir.path()).await;
        fixture
            .cell
            .alloc_split_plan(0xBAD, 1, MetaKey::encode_pid_prefix_start(500), 2, None)
            .unwrap();
        let (_, parent, _) = fixture.cell.lookup_local_handle(1).unwrap();
        // Persist an invalid application record inside a valid SST/checkpoint.
        // A nonempty transaction table alone must not classify it as retryable.
        parent.engine().apply_committed_put(
            TxnRecordKey { txn_id: 0xBAD }.encode().to_vec(),
            vec![0],
            fixture.clock.now(),
            0,
        );
        parent.state_machine().flush_to_lsm().unwrap();
        fixture.host.shutdown_all_no_flush().await.unwrap();
    }
    let error = try_open(dir.path())
        .await
        .err()
        .expect("malformed transaction state must fail closed instead of deferring startup");
    assert!(
        matches!(
            error,
            MetaDbError::CodecError(_) | MetaDbError::ChecksumMismatch { .. }
        ),
        "unexpected startup error: {error}"
    );
}
