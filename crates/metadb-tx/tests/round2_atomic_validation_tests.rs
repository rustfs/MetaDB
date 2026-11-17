use std::sync::Arc;

use metadb_core::codec;
use metadb_core::{
    AttrDeltaSpec, AuxOutboxEntry, AuxTaskPayload, CompositeKeyRange, HlcTimestamp, InodeAttr,
    IntentOp, MetaDbError, MetaKey, QuotaKey, QuotaRecord, ShardEpoch, WriteIntentKey,
};
use metadb_raft::{MetaDbRaftRequest, ShardApplyHandler, ShardWatermarkTracker};
use metadb_storage::{LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{SetAttrPatch, TxRaftCmd, TxStateMachineHandler};

fn apply(
    handler: &TxStateMachineHandler,
    engine: &LifecycleStorageEngine,
    index: u64,
    cmd: TxRaftCmd,
) -> metadb_core::Result<Vec<u8>> {
    let hlc = HlcTimestamp::new(1, index as u16);
    handler.apply_command(
        index,
        hlc,
        &MetaDbRaftRequest {
            expected_epoch: handler.current_epoch(),
            hlc,
            payload: codec::serialize(&cmd).unwrap(),
        },
        engine,
    )
}

fn fixture() -> (
    tempfile::TempDir,
    LifecycleStorageEngine,
    TxStateMachineHandler,
) {
    let dir = tempfile::tempdir().unwrap();
    let engine = LifecycleStorageEngine::open_lsm(
        1,
        dir.path(),
        Arc::new(ShardedBlockCache::new(1 << 20)),
        LsmEngineConfig::default(),
    )
    .unwrap();
    let handler = TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        CompositeKeyRange::all_user_tables(),
        Arc::new(ShardWatermarkTracker::default()),
    );
    apply(
        &handler,
        &engine,
        1,
        TxRaftCmd::InitDirAttr {
            dir_id: 1,
            parent_pid: 1,
            uid: 7,
            mode: 0o755,
        },
    )
    .unwrap();
    (dir, engine, handler)
}

fn create(name: &str, size_bytes: i64) -> TxRaftCmd {
    TxRaftCmd::CreateFile {
        pid: 1,
        name: name.into(),
        inode_id: (1 << 63) + 10,
        fid: 10,
        uid: 7,
        mode: 0o644,
        size_bytes,
    }
}

fn read(engine: &LifecycleStorageEngine, key: &[u8]) -> Option<Vec<u8>> {
    engine.get(key, HlcTimestamp::new(2, 0)).unwrap()
}

#[test]
fn invalid_sizes_and_directory_capacity_fail_before_any_side_effect() {
    let (_dir, engine, handler) = fixture();
    for (index, size) in [(2, -1), (3, i64::MIN)] {
        assert!(apply(&handler, &engine, index, create("negative", size)).is_err());
        assert!(read(&engine, &MetaKey::child(1, "negative").encode()).is_none());
        assert_eq!(handler.attr_aggregator().pending_delta_count(), 0);
    }
    apply(&handler, &engine, 4, create("full", i64::MAX)).unwrap();
    assert!(apply(&handler, &engine, 5, create("overflow", 1)).is_err());
    assert!(read(&engine, &MetaKey::child(1, "overflow").encode()).is_none());
    assert_eq!(handler.attr_aggregator().pending_delta_count(), 1);
    let before = read(&engine, &MetaKey::child(1, "full").encode());
    assert!(apply(
        &handler,
        &engine,
        6,
        TxRaftCmd::SetAttrSingleShard {
            pid: 1,
            name: "full".into(),
            patch: SetAttrPatch {
                size_bytes: Some(i64::MIN),
                ..Default::default()
            }
        }
    )
    .is_err());
    assert_eq!(read(&engine, &MetaKey::child(1, "full").encode()), before);
    apply(
        &handler,
        &engine,
        7,
        TxRaftCmd::Unlink {
            pid: 1,
            name: "full".into(),
        },
    )
    .unwrap();
    apply(&handler, &engine, 8, create("fits", 1)).unwrap();
}

#[test]
fn malformed_historical_size_is_rejected_before_unlink_deletes_the_key() {
    let (_dir, engine, handler) = fixture();
    apply(&handler, &engine, 2, create("victim", 1)).unwrap();
    let key = MetaKey::child(1, "victim").encode();
    let mut attr = InodeAttr::decode(&read(&engine, &key).unwrap()).unwrap();
    attr.size_bytes = i64::MIN;
    engine.apply_committed_put(key.clone(), attr.encode(), HlcTimestamp::new(1, 3), 3);
    assert!(apply(
        &handler,
        &engine,
        4,
        TxRaftCmd::Unlink {
            pid: 1,
            name: "victim".into()
        }
    )
    .is_err());
    assert_eq!(read(&engine, &key), Some(attr.encode()));
}

fn quota_task(
    origin: u64,
    index: u64,
    uid: u64,
    bytes: i64,
    inodes: i64,
    previous: (u64, u16),
) -> AuxOutboxEntry {
    AuxOutboxEntry {
        origin_shard_id: origin,
        origin_raft_index: index,
        task_ordinal: 0,
        hlc: HlcTimestamp::new(1, index as u16),
        payload: AuxTaskPayload::QuotaDelta {
            uid,
            delta_bytes: bytes,
            delta_inodes: inodes,
            prev_uid_cursor: previous,
        },
    }
}

#[test]
fn quota_overflow_rejects_the_whole_batch_and_does_not_advance_frontiers() {
    let (_dir, engine, handler) = fixture();
    apply(
        &handler,
        &engine,
        2,
        TxRaftCmd::ApplyAuxBatch {
            entries: vec![quota_task(1, 1, 7, i64::MAX, 1, (0, 0))],
        },
    )
    .unwrap();
    let key = QuotaKey { uid: 7 }.encode();
    let before = read(&engine, &key).unwrap();
    assert!(apply(
        &handler,
        &engine,
        3,
        TxRaftCmd::ApplyAuxBatch {
            entries: vec![
                quota_task(2, 1, 8, 10, 1, (0, 0)),
                quota_task(1, 2, 7, 1, 0, (1, 0)),
            ]
        }
    )
    .is_err());
    assert!(read(&engine, &QuotaKey { uid: 8 }.encode()).is_none());
    assert_eq!(read(&engine, &key), Some(before));
    apply(
        &handler,
        &engine,
        4,
        TxRaftCmd::ApplyAuxBatch {
            entries: vec![quota_task(2, 1, 7, -i64::MAX, -1, (0, 0))],
        },
    )
    .unwrap();
    let quota = QuotaRecord::decode(&read(&engine, &key).unwrap()).unwrap();
    assert_eq!((quota.used_bytes, quota.used_inodes), (0, 0));
}

#[test]
fn invalid_directory_ids_fail_on_single_shard_and_prepare_paths() {
    let (_dir, engine, handler) = fixture();
    for id in [0, 1, 1 << 63, u64::MAX] {
        assert!(apply(
            &handler,
            &engine,
            2,
            TxRaftCmd::MkdirSingleShard {
                pid: 1,
                name: "alias".into(),
                new_dir_id: id,
                uid: 7,
                mode: 0o755
            }
        )
        .is_err());
        assert!(read(&engine, &MetaKey::child(1, "alias").encode()).is_none());
        let attr = InodeAttr {
            inode_id: id,
            parent_pid: 1,
            entry_type: metadb_core::EntryType::Directory,
            uid: 7,
            mode: 0o755,
            link_count: 1,
            num_children: 0,
            size_bytes: 0,
            create_time: HlcTimestamp::new(1, 1),
            modify_time: HlcTimestamp::new(1, 1),
            fid: None,
            applied_lsn: 0,
        };
        let key = MetaKey::child(1, "alias").encode();
        let op = IntentOp::Put {
            value: attr.encode(),
            attr_deltas: vec![],
            aux_specs: vec![],
        };
        assert!(apply(&handler, &engine, 3, prepare(10, key.clone(), op)).is_err());
        assert!(read(&engine, &WriteIntentKey::new(key).encode()).is_none());
    }
}

fn prepare(txn_id: u128, key: Vec<u8>, op: IntentOp) -> TxRaftCmd {
    TxRaftCmd::Prepare2pc {
        txn_id,
        is_primary: false,
        primary_key: key.clone(),
        participant_keys: vec![key.clone()],
        start_hlc: HlcTimestamp::new(1, 1),
        expire_hlc: HlcTimestamp::new(5, 0),
        rename_fence_token: None,
        intents: vec![(key, op)],
    }
}

#[test]
fn prepared_positive_delta_reserves_capacity_until_commit_or_abort() {
    let (_dir, engine, handler) = fixture();
    apply(&handler, &engine, 2, create("template", 0)).unwrap();
    let mut attr =
        InodeAttr::decode(&read(&engine, &MetaKey::child(1, "template").encode()).unwrap())
            .unwrap();
    attr.size_bytes = i64::MAX;
    let key = MetaKey::child(1, "reserved").encode();
    let op = IntentOp::Put {
        value: attr.encode(),
        attr_deltas: vec![AttrDeltaSpec {
            pid: 1,
            delta_children: 1,
            delta_bytes: i64::MAX,
        }],
        aux_specs: vec![],
    };
    apply(&handler, &engine, 3, prepare(10, key.clone(), op)).unwrap();
    assert!(apply(&handler, &engine, 4, create("overflow", 1)).is_err());
    apply(
        &handler,
        &engine,
        5,
        TxRaftCmd::CommitSecondary2pc {
            txn_id: 10,
            commit_hlc: HlcTimestamp::new(1, 5),
            target_user_keys: vec![key.clone()],
        },
    )
    .unwrap();
    assert_eq!(
        InodeAttr::decode(&read(&engine, &key).unwrap())
            .unwrap()
            .size_bytes,
        i64::MAX
    );
    assert!(read(&engine, &WriteIntentKey::new(key).encode()).is_none());
}

#[test]
fn same_command_put_and_delete_preserve_maximum_directory_balance() {
    let (_dir, engine, handler) = fixture();
    apply(&handler, &engine, 2, create("source", i64::MAX)).unwrap();
    let source = MetaKey::child(1, "source").encode();
    let target = MetaKey::child(1, "target").encode();
    let attr = read(&engine, &source).unwrap();
    let intents = vec![
        (
            target.clone(),
            IntentOp::Put {
                value: attr,
                attr_deltas: vec![AttrDeltaSpec {
                    pid: 1,
                    delta_children: 1,
                    delta_bytes: i64::MAX,
                }],
                aux_specs: vec![],
            },
        ),
        (
            source.clone(),
            IntentOp::Delete {
                attr_deltas: vec![AttrDeltaSpec {
                    pid: 1,
                    delta_children: -1,
                    delta_bytes: -i64::MAX,
                }],
                aux_specs: vec![],
            },
        ),
    ];
    apply(
        &handler,
        &engine,
        3,
        TxRaftCmd::Prepare2pc {
            txn_id: 20,
            is_primary: false,
            primary_key: source.clone(),
            participant_keys: vec![target.clone(), source.clone()],
            start_hlc: HlcTimestamp::new(1, 3),
            expire_hlc: HlcTimestamp::new(5, 0),
            rename_fence_token: None,
            intents,
        },
    )
    .unwrap();
    // Put is materialized first, so its intermediate delta exceeds the i64
    // balance. Only the completed command is visible to readers and flushes.
    apply(
        &handler,
        &engine,
        4,
        TxRaftCmd::CommitSecondary2pc {
            txn_id: 20,
            commit_hlc: HlcTimestamp::new(1, 4),
            target_user_keys: vec![target.clone(), source.clone()],
        },
    )
    .unwrap();
    let snap = engine.acquire_linearizable_snapshot(4).unwrap();
    let base = InodeAttr::decode(
        &engine
            .get_with_snapshot(&MetaKey::dir_attr(1).encode(), &snap)
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    let result = handler
        .attr_aggregator()
        .reconcile_inode_attr(1, base, &snap);
    assert_eq!((result.num_children, result.size_bytes), (1, i64::MAX));
    assert!(read(&engine, &source).is_none());
    assert!(read(&engine, &target).is_some());
}

#[test]
fn fence_rechecks_intents_at_the_serialized_apply_boundary() {
    let (_dir, engine, handler) = fixture();
    apply(&handler, &engine, 2, create("template", 0)).unwrap();
    let key = MetaKey::child(1, "prepared").encode();
    let op = IntentOp::Put {
        value: read(&engine, &MetaKey::child(1, "template").encode()).unwrap(),
        attr_deltas: vec![AttrDeltaSpec {
            pid: 1,
            delta_children: 1,
            delta_bytes: 0,
        }],
        aux_specs: vec![],
    };
    apply(&handler, &engine, 3, prepare(30, key.clone(), op)).unwrap();
    let before = handler.key_range();
    let fence = TxRaftCmd::FenceShardRangeForSplit {
        new_range: CompositeKeyRange::new(
            before.start_key.clone(),
            MetaKey::encode_pid_prefix_start(100),
        )
        .unwrap(),
        new_epoch: ShardEpoch {
            conf_ver: 1,
            version: 2,
        },
    };
    // Simulate the lease expiring between the worker's scan and Raft apply.
    let hlc = HlcTimestamp::new(6, 0);
    let deferred = handler.apply_command(
        4,
        hlc,
        &MetaDbRaftRequest {
            expected_epoch: handler.current_epoch(),
            hlc,
            payload: codec::serialize(&fence).unwrap(),
        },
        &engine,
    );
    assert!(matches!(deferred, Err(MetaDbError::SplitDeferred(_))));
    assert_eq!(handler.key_range(), before);
    assert!(read(&engine, &WriteIntentKey::new(key).encode()).is_some());
}
