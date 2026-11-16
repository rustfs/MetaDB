use std::sync::Arc;

use metadb_core::{
    CompositeKeyRange, DirShardDeltaKey, DirShardDeltaRecord, HlcTimestamp, InodeAttr, MetaKey,
    ShardEpoch,
};
use metadb_raft::{MetaDbRaftRequest, ShardApplyHandler, ShardWatermarkTracker};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache,
};
use metadb_tx::{TxRaftCmd, TxStateMachineHandler};

fn apply(
    handler: &TxStateMachineHandler,
    engine: &LifecycleStorageEngine,
    index: u64,
    cmd: TxRaftCmd,
) {
    let hlc = HlcTimestamp::new(1, index as u16);
    handler
        .apply_command(
            index,
            hlc,
            &MetaDbRaftRequest {
                expected_epoch: handler.current_epoch(),
                hlc,
                payload: bincode::serialize(&cmd).unwrap(),
            },
            engine,
        )
        .unwrap();
}

fn flush(handler: &TxStateMachineHandler, engine: &LifecycleStorageEngine, index: u64) {
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: index,
                flushed_max_hlc: HlcTimestamp::new(1, index as u16),
                contiguous_aux_acked_index: 0,
                ..Default::default()
            },
            Some(handler),
        )
        .unwrap();
}

fn run_stale_split_case(anchor: bool, flush_intervening_write: bool) {
    let temp = tempfile::tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(1024 * 1024));
    let engine =
        LifecycleStorageEngine::open_lsm(1, temp.path(), cache.clone(), LsmEngineConfig::default())
            .unwrap();
    let start = if anchor {
        MetaKey::encode_pid_prefix_start(100)
    } else {
        MetaKey::child(100, "a").encode()
    };
    let range = CompositeKeyRange::new(start.clone(), MetaKey::encode_pid_prefix_end(100)).unwrap();
    let handler = TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        range,
        Arc::new(ShardWatermarkTracker::default()),
    );
    if anchor {
        apply(
            &handler,
            &engine,
            1,
            TxRaftCmd::InitDirAttr {
                dir_id: 100,
                parent_pid: 1,
                uid: 10,
                mode: 0o755,
            },
        );
    } else {
        apply(
            &handler,
            &engine,
            1,
            TxRaftCmd::InitDirShardDelta {
                record: DirShardDeltaRecord {
                    inode_id: 100,
                    parent_pid: 1,
                    uid: 10,
                    mode: 0o755,
                    create_time: HlcTimestamp::new(1, 1),
                    num_children: 0,
                    size_bytes: 0,
                    modify_time: HlcTimestamp::new(1, 1),
                    applied_raft_index: 1,
                    deleting_fence: false,
                },
            },
        );
    }
    for (index, name, size) in [(2, "a", 10), (3, "z", 20)] {
        apply(
            &handler,
            &engine,
            index,
            TxRaftCmd::CreateFile {
                pid: 100,
                name: name.into(),
                inode_id: index,
                fid: index,
                uid: 10,
                mode: 0o644,
                size_bytes: size,
            },
        );
    }
    flush(&handler, &engine, 3);
    let attr_key = MetaKey::dir_attr(100).encode();
    let delta_key = DirShardDeltaKey { pid: 100 }.encode().to_vec();
    // The worker captured only one left child before a later left-side write.
    let stale_anchor = if anchor {
        let mut attr = InodeAttr::decode(
            &engine
                .get(&attr_key, HlcTimestamp::new(1, 3))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        attr.num_children = 1;
        attr.size_bytes = 10;
        Some((100, attr))
    } else {
        None
    };
    let stale_delta = if !anchor {
        let mut sub = DirShardDeltaRecord::decode(
            &engine
                .get(&delta_key, HlcTimestamp::new(1, 3))
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        sub.num_children = 1;
        sub.size_bytes = 10;
        Some(sub)
    } else {
        None
    };
    let left = CompositeKeyRange::new(start, MetaKey::child(100, "m").encode()).unwrap();
    let epoch = ShardEpoch::initial().bump_version();
    apply(
        &handler,
        &engine,
        4,
        TxRaftCmd::FenceShardRangeForSplit {
            new_range: left.clone(),
            new_epoch: epoch,
        },
    );
    apply(
        &handler,
        &engine,
        5,
        TxRaftCmd::CreateFile {
            pid: 100,
            name: "b".into(),
            inode_id: 5,
            fid: 5,
            uid: 10,
            mode: 0o644,
            size_bytes: 30,
        },
    );
    if flush_intervening_write {
        flush(&handler, &engine, 5);
    }
    apply(
        &handler,
        &engine,
        6,
        TxRaftCmd::UpdateShardRangeOnSplit {
            new_range: left,
            new_epoch: epoch,
            keys_to_delete: vec![MetaKey::child(100, "z").encode()],
            updated_anchor_attr: stale_anchor,
            updated_shard_delta: stale_delta,
        },
    );
    let snapshot = engine.acquire_linearizable_snapshot(6).unwrap();
    let (count, bytes, mtime) = if anchor {
        let raw = engine
            .get_with_snapshot(&attr_key, &snapshot)
            .unwrap()
            .unwrap();
        let attr = handler.attr_aggregator().reconcile_inode_attr(
            100,
            InodeAttr::decode(&raw).unwrap(),
            &snapshot,
        );
        (attr.num_children, attr.size_bytes, attr.modify_time)
    } else {
        let raw = engine
            .get_with_snapshot(&delta_key, &snapshot)
            .unwrap()
            .unwrap();
        let sub = handler.attr_aggregator().reconcile_dir_shard_delta(
            100,
            DirShardDeltaRecord::decode(&raw).unwrap(),
            &snapshot,
        );
        (sub.num_children, sub.size_bytes, sub.modify_time)
    };
    assert_eq!(
        (count, bytes),
        (2, 40),
        "anchor={anchor}, flushed={flush_intervening_write}"
    );
    assert!(mtime >= HlcTimestamp::new(1, 5));
    drop(snapshot);
    flush(&handler, &engine, 6);
    drop(engine);
    let reopened =
        LifecycleStorageEngine::open_lsm(1, temp.path(), cache, LsmEngineConfig::default())
            .unwrap();
    let raw = reopened
        .get(
            if anchor { &attr_key } else { &delta_key },
            HlcTimestamp::new(1, 6),
        )
        .unwrap()
        .unwrap();
    let stats = if anchor {
        let attr = InodeAttr::decode(&raw).unwrap();
        (attr.num_children, attr.size_bytes)
    } else {
        let sub = DirShardDeltaRecord::decode(&raw).unwrap();
        (sub.num_children, sub.size_bytes)
    };
    assert_eq!(stats, (2, 40), "split accounting changed after reopen");
}

#[test]
fn split_update_keeps_pending_anchor_writes() {
    run_stale_split_case(true, false);
}
#[test]
fn split_update_keeps_flushed_anchor_writes() {
    run_stale_split_case(true, true);
}
#[test]
fn split_update_keeps_pending_non_anchor_writes() {
    run_stale_split_case(false, false);
}
#[test]
fn split_update_keeps_flushed_non_anchor_writes() {
    run_stale_split_case(false, true);
}

fn run_anchor_metadata_case(flush_after_prepare: bool) {
    let temp = tempfile::tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(1024 * 1024));
    let engine =
        LifecycleStorageEngine::open_lsm(1, temp.path(), cache.clone(), LsmEngineConfig::default())
            .unwrap();
    let handler = TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        CompositeKeyRange::meta_table_root(),
        Arc::new(ShardWatermarkTracker::default()),
    );
    apply(
        &handler,
        &engine,
        1,
        TxRaftCmd::InitDirAttr {
            dir_id: 100,
            parent_pid: 1,
            uid: 10,
            mode: 0o755,
        },
    );
    apply(
        &handler,
        &engine,
        2,
        TxRaftCmd::CreateFile {
            pid: 100,
            name: "child".into(),
            inode_id: 2,
            fid: 2,
            uid: 10,
            mode: 0o644,
            size_bytes: 10,
        },
    );
    let key = MetaKey::dir_attr(100).encode();
    let raw = engine.get(&key, HlcTimestamp::new(1, 2)).unwrap().unwrap();
    let mut attr = InodeAttr::decode(&raw).unwrap();
    // Model the coordinator's reconciled DirStat snapshot, followed by
    // another committed child before it acquires the metadata intent.
    attr.num_children = 1;
    attr.size_bytes = 10;
    attr.applied_lsn = 2;
    attr.mode = 0o700;
    apply(
        &handler,
        &engine,
        3,
        TxRaftCmd::CreateFile {
            pid: 100,
            name: "later".into(),
            inode_id: 3,
            fid: 3,
            uid: 10,
            mode: 0o644,
            size_bytes: 20,
        },
    );
    apply(
        &handler,
        &engine,
        4,
        TxRaftCmd::Prepare2pcChecked {
            txn_id: 1,
            is_primary: true,
            primary_key: key.clone(),
            participant_keys: vec![key.clone()],
            start_hlc: HlcTimestamp::new(1, 4),
            expire_hlc: HlcTimestamp::new(1000, 0),
            rename_fence_token: None,
            intents: vec![(
                key.clone(),
                metadb_core::IntentOp::Put {
                    value: attr.encode(),
                    attr_deltas: vec![],
                    aux_specs: vec![],
                },
            )],
            expected_values: vec![(key.clone(), Some(raw))],
        },
    );
    if flush_after_prepare {
        flush(&handler, &engine, 4);
    }
    apply(
        &handler,
        &engine,
        5,
        TxRaftCmd::CommitPrimary2pc {
            txn_id: 1,
            commit_hlc: HlcTimestamp::new(1, 5),
            rename_fence_token: None,
        },
    );
    let snapshot = engine.acquire_linearizable_snapshot(5).unwrap();
    let raw = engine.get_with_snapshot(&key, &snapshot).unwrap().unwrap();
    let attr = handler.attr_aggregator().reconcile_inode_attr(
        100,
        InodeAttr::decode(&raw).unwrap(),
        &snapshot,
    );
    assert_eq!(
        (attr.num_children, attr.size_bytes, attr.mode),
        (2, 30, 0o700)
    );
    drop(snapshot);
    flush(&handler, &engine, 5);
    drop(engine);
    let reopened =
        LifecycleStorageEngine::open_lsm(1, temp.path(), cache, LsmEngineConfig::default())
            .unwrap();
    let attr = InodeAttr::decode(
        &reopened
            .get(&key, HlcTimestamp::new(1, 5))
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        (attr.num_children, attr.size_bytes, attr.mode),
        (2, 30, 0o700)
    );
}

#[test]
fn metadata_two_pc_preserves_pending_child_accounting() {
    run_anchor_metadata_case(false);
}
#[test]
fn metadata_two_pc_preserves_child_accounting_flushed_after_prepare() {
    run_anchor_metadata_case(true);
}
