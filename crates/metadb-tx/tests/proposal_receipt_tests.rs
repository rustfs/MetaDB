//! A discarded-log retry must not execute a command that already committed.

use std::sync::Arc;

use metadb_core::{CompositeKeyRange, HlcTimestamp, InodeAttr, MetaKey, ShardEpoch};
use metadb_raft::{MetaDbRaftRequest, ShardApplyHandler, ShardWatermarkTracker};
use metadb_storage::{LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{
    encode_proposed_command, fresh_proposal_id, proposal_receipt_key, SetAttrPatch, TxRaftCmd,
    TxStateMachineHandler, PROPOSAL_RECEIPT_RETENTION_MS,
};

fn apply(
    handler: &TxStateMachineHandler,
    engine: &LifecycleStorageEngine,
    index: u64,
    payload: Vec<u8>,
) -> Vec<u8> {
    let hlc = HlcTimestamp::new(1_000, index as u16);
    handler
        .apply_command(
            index,
            hlc,
            &MetaDbRaftRequest {
                expected_epoch: handler.current_epoch(),
                hlc,
                payload,
            },
            engine,
        )
        .unwrap()
}

fn mode_of(engine: &LifecycleStorageEngine, pid: u64, name: &str) -> u32 {
    let key = MetaKey::child(pid, name).encode();
    let raw = engine
        .get(&key, HlcTimestamp::new(9_000, 0))
        .unwrap()
        .expect("file attribute");
    InodeAttr::decode(&raw).unwrap().mode
}

#[test]
fn repeated_proposal_does_not_apply_over_a_later_write() {
    let temp = tempfile::tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(1024 * 1024));
    let engine =
        LifecycleStorageEngine::open_lsm(1, temp.path(), cache, LsmEngineConfig::default())
            .unwrap();
    let range = CompositeKeyRange::meta_pid_range(0, 1_000).unwrap();
    let handler = TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        range,
        Arc::new(ShardWatermarkTracker::default()),
    );
    apply(
        &handler,
        &engine,
        1,
        metadb_core::codec::serialize(&TxRaftCmd::InitDirAttr {
            dir_id: 10,
            parent_pid: 1,
            uid: 7,
            mode: 0o755,
        })
        .unwrap(),
    );
    apply(
        &handler,
        &engine,
        2,
        metadb_core::codec::serialize(&TxRaftCmd::CreateFile {
            pid: 10,
            name: "note".to_string(),
            inode_id: 100,
            fid: 100,
            uid: 7,
            mode: 0o644,
            size_bytes: 4,
        })
        .unwrap(),
    );

    let original_id = fresh_proposal_id();
    let original = encode_proposed_command(
        original_id,
        &TxRaftCmd::SetAttrSingleShard {
            pid: 10,
            name: "note".to_string(),
            patch: SetAttrPatch {
                mode: Some(0o755),
                ..SetAttrPatch::default()
            },
        },
    )
    .unwrap();
    let first_result = apply(&handler, &engine, 3, original.clone());
    assert_eq!(mode_of(&engine, 10, "note"), 0o755);

    let later = encode_proposed_command(
        fresh_proposal_id(),
        &TxRaftCmd::SetAttrSingleShard {
            pid: 10,
            name: "note".to_string(),
            patch: SetAttrPatch {
                mode: Some(0o700),
                ..SetAttrPatch::default()
            },
        },
    )
    .unwrap();
    apply(&handler, &engine, 4, later);
    assert_eq!(mode_of(&engine, 10, "note"), 0o700);

    let replay = apply(&handler, &engine, 5, original);
    assert_eq!(
        replay, first_result,
        "a committed proposal must return its original result"
    );
    assert_eq!(
        mode_of(&engine, 10, "note"),
        0o700,
        "replaying a discarded proposal must not apply it over a later write"
    );
}

#[test]
fn proposal_receipt_expires_after_the_retention_window() {
    let temp = tempfile::tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(1024 * 1024));
    let engine =
        LifecycleStorageEngine::open_lsm(1, temp.path(), cache, LsmEngineConfig::default())
            .unwrap();
    let range = CompositeKeyRange::meta_pid_range(0, 1_000).unwrap();
    let handler = TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        range,
        Arc::new(ShardWatermarkTracker::default()),
    );
    let early = HlcTimestamp::new(5_000, 0);
    handler
        .apply_command(
            1,
            early,
            &MetaDbRaftRequest {
                expected_epoch: handler.current_epoch(),
                hlc: early,
                payload: metadb_core::codec::serialize(&TxRaftCmd::InitDirAttr {
                    dir_id: 10,
                    parent_pid: 1,
                    uid: 7,
                    mode: 0o755,
                })
                .unwrap(),
            },
            &engine,
        )
        .unwrap();
    let proposal_id = fresh_proposal_id();
    let payload = encode_proposed_command(
        proposal_id,
        &TxRaftCmd::CreateFile {
            pid: 10,
            name: "kept".to_string(),
            inode_id: 11,
            fid: 11,
            uid: 7,
            mode: 0o644,
            size_bytes: 1,
        },
    )
    .unwrap();
    handler
        .apply_command(
            2,
            early,
            &MetaDbRaftRequest {
                expected_epoch: handler.current_epoch(),
                hlc: early,
                payload: payload.clone(),
            },
            &engine,
        )
        .unwrap();
    let late = HlcTimestamp::new(5_000 + PROPOSAL_RECEIPT_RETENTION_MS + 1, 0);
    handler
        .apply_command(
            3,
            late,
            &MetaDbRaftRequest {
                expected_epoch: handler.current_epoch(),
                hlc: late,
                payload: metadb_core::codec::serialize(&TxRaftCmd::CreateFile {
                    pid: 10,
                    name: "other".to_string(),
                    inode_id: 12,
                    fid: 12,
                    uid: 7,
                    mode: 0o644,
                    size_bytes: 1,
                })
                .unwrap(),
            },
            &engine,
        )
        .unwrap();
    assert!(
        engine
            .get(&proposal_receipt_key(proposal_id), late)
            .unwrap()
            .is_none(),
        "a receipt older than the retention window is deleted by a later commit"
    );
    handler
        .apply_command(
            4,
            late,
            &MetaDbRaftRequest {
                expected_epoch: handler.current_epoch(),
                hlc: late,
                payload,
            },
            &engine,
        )
        .unwrap_err();
}

#[test]
fn legacy_receipt_without_timestamp_is_stamped_then_expires() {
    let temp = tempfile::tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(1024 * 1024));
    let engine =
        LifecycleStorageEngine::open_lsm(1, temp.path(), cache, LsmEngineConfig::default())
            .unwrap();
    let range = CompositeKeyRange::meta_pid_range(0, 1_000).unwrap();
    let handler = TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        range,
        Arc::new(ShardWatermarkTracker::default()),
    );
    let proposal_id = fresh_proposal_id();
    let key = proposal_receipt_key(proposal_id);
    let stamped_at = HlcTimestamp::new(8_000, 0);
    engine.apply_committed_put(key.clone(), b"prior-result".to_vec(), stamped_at, 1);
    handler
        .apply_command(
            2,
            stamped_at,
            &MetaDbRaftRequest {
                expected_epoch: handler.current_epoch(),
                hlc: stamped_at,
                payload: metadb_core::codec::serialize(&TxRaftCmd::InitDirAttr {
                    dir_id: 10,
                    parent_pid: 1,
                    uid: 7,
                    mode: 0o755,
                })
                .unwrap(),
            },
            &engine,
        )
        .unwrap();
    let stamped = engine
        .get(&key, HlcTimestamp::new(9_000, 0))
        .unwrap()
        .expect("legacy receipt is stamped");
    assert!(stamped.starts_with(b"MDBR0001"));
    let late = HlcTimestamp::new(8_000 + PROPOSAL_RECEIPT_RETENTION_MS + 1, 0);
    handler
        .apply_command(
            3,
            late,
            &MetaDbRaftRequest {
                expected_epoch: handler.current_epoch(),
                hlc: late,
                payload: metadb_core::codec::serialize(&TxRaftCmd::CreateFile {
                    pid: 10,
                    name: "after".to_string(),
                    inode_id: 15,
                    fid: 15,
                    uid: 7,
                    mode: 0o644,
                    size_bytes: 1,
                })
                .unwrap(),
            },
            &engine,
        )
        .unwrap();
    assert!(
        engine.get(&key, late).unwrap().is_none(),
        "a stamped legacy receipt expires after the retention window"
    );
}
