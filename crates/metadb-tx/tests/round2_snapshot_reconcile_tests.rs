//! Deterministic interleaving of a pinned read cut and the attr-delta flush hook.
use std::sync::Arc;

use metadb_core::codec;
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
                payload: codec::serialize(&cmd).unwrap(),
            },
            engine,
        )
        .unwrap();
}

fn pinned_read_must_retain_its_unfolded_deltas(anchor: bool) {
    let temp = tempfile::tempdir().unwrap();
    let engine = LifecycleStorageEngine::open_lsm(
        1,
        temp.path(),
        Arc::new(ShardedBlockCache::new(1024 * 1024)),
        LsmEngineConfig::default(),
    )
    .unwrap();
    let handler = TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        CompositeKeyRange::meta_table_root(),
        Arc::new(ShardWatermarkTracker::default()),
    );
    let init = if anchor {
        TxRaftCmd::InitDirAttr {
            dir_id: 100,
            parent_pid: 1,
            uid: 10,
            mode: 0o755,
        }
    } else {
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
        }
    };
    apply(&handler, &engine, 1, init);
    apply(
        &handler,
        &engine,
        2,
        TxRaftCmd::CreateFile {
            pid: 100,
            name: "child".into(),
            inode_id: 101,
            fid: 201,
            uid: 10,
            mode: 0o644,
            size_bytes: 17,
        },
    );

    // DirStat has acquired its read cut. A flush can run immediately after this
    // lock-protected cut, before DirStat reads/reconciles the pinned baseline.
    let snap = engine.acquire_linearizable_snapshot(2).unwrap();
    let key = if anchor {
        MetaKey::dir_attr(100).encode()
    } else {
        DirShardDeltaKey { pid: 100 }.encode().to_vec()
    };
    // DirStat obtains owned baseline bytes before entering reconcile's queue lock.
    let raw = engine.get_with_snapshot(&key, &snap).unwrap().unwrap();
    let read = || {
        if anchor {
            let base = InodeAttr::decode(&raw).unwrap();
            let attr = handler
                .attr_aggregator()
                .reconcile_inode_attr(100, base, &snap);
            (attr.num_children, attr.size_bytes)
        } else {
            let base = DirShardDeltaRecord::decode(&raw).unwrap();
            let attr = handler
                .attr_aggregator()
                .reconcile_dir_shard_delta(100, base, &snap);
            (attr.num_children, attr.size_bytes)
        }
    };
    assert_eq!(read(), (1, 17));
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: 2,
                flushed_max_hlc: HlcTimestamp::new(1, 2),
                ..Default::default()
            },
            Some(&handler),
        )
        .unwrap();
    let after = read();
    eprintln!("anchor={anchor}: same pinned cut before flush=(1, 17), after flush={after:?}");
    assert_eq!(
        after,
        (1, 17),
        "flush must preserve the already pinned read's directory totals"
    );
}

#[test]
fn anchor_snapshot_keeps_deltas_until_reconciliation() {
    pinned_read_must_retain_its_unfolded_deltas(true);
}

#[test]
fn non_anchor_snapshot_keeps_deltas_until_reconciliation() {
    pinned_read_must_retain_its_unfolded_deltas(false);
}
