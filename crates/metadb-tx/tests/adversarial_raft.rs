//! Replicated-state-machine checks that a proposer can bypass the router pre-checks:
//! negative sizes, directory-inode reuse, forked quota cursors, and prefix pagination.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use metadb_core::{
    AuxOutboxEntry, AuxTaskPayload, CompositeKeyRange, HlcTimestamp, HybridLogicalClock, InodeId,
    MetaDbError, Result, ShardEpoch, ROOT_INODE_ID,
};
use metadb_raft::{
    ChannelPool, MultiRaftRouter, RaftNodeOptions, ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use metadb_tx::{
    DirRenameLease, DistributedTxRouter, SetAttrPatch, ShardDirectory, TxRaftCmd,
    TxStateMachineHandler,
};
use tempfile::tempdir;

struct OneShard {
    range: CompositeKeyRange,
    group: Arc<ShardRaftGroup>,
    handler: Arc<TxStateMachineHandler>,
    fence_seq: AtomicU64,
    clock: Arc<HybridLogicalClock>,
}

#[tonic::async_trait]
impl ShardDirectory for OneShard {
    fn route_user_key(
        &self,
        user_key: &[u8],
    ) -> Result<(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)> {
        if self.range.contains_key(user_key) {
            Ok((
                self.group.current_epoch(),
                self.group.clone(),
                self.handler.clone(),
            ))
        } else {
            Err(MetaDbError::ShardRoutingError("unrouted key".to_string()))
        }
    }

    fn route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> Result<Vec<(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)>> {
        if self.range.overlaps(start_user_key, end_user_key) {
            Ok(vec![(
                self.group.current_epoch(),
                self.group.clone(),
                self.handler.clone(),
            )])
        } else {
            Ok(Vec::new())
        }
    }

    async fn acquire_dir_rename_lease(
        &self,
        _moved_dir_id: InodeId,
        ttl_ms: u64,
    ) -> Result<DirRenameLease> {
        Ok(DirRenameLease {
            rename_fence_token: self.fence_seq.fetch_add(1, Ordering::AcqRel),
            expire_hlc: HlcTimestamp::new(self.clock.now().physical_ms() + ttl_ms, 0),
        })
    }

    async fn release_dir_rename_lease(&self, _token: u64) {}
}

fn quota_delta(index: u64, prev: (u64, u16), bytes: i64, hlc: HlcTimestamp) -> AuxOutboxEntry {
    AuxOutboxEntry {
        origin_shard_id: 1,
        origin_raft_index: index,
        task_ordinal: 0,
        hlc,
        payload: AuxTaskPayload::QuotaDelta {
            uid: 77,
            delta_bytes: bytes,
            delta_inodes: 1,
            prev_uid_cursor: prev,
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn replicated_apply_rejects_forged_commands_and_keeps_prefix_pages() {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let router = MultiRaftRouter::new();
    let wm = Arc::new(ShardWatermarkTracker::default());
    let range = CompositeKeyRange::all_user_tables();
    let handler = Arc::new(TxStateMachineHandler::new(
        1,
        ShardEpoch::initial(),
        range.clone(),
        wm.clone(),
    ));
    let group = ShardRaftGroup::start(
        1,
        1,
        dir.path().join("shard"),
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
        .initialize_if_needed(std::collections::BTreeMap::from([(
            1u64,
            "127.0.0.1:0".to_string(),
        )]))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(80)).await;

    let directory = Arc::new(OneShard {
        range,
        group: group.clone(),
        handler,
        fence_seq: AtomicU64::new(1),
        clock: clock.clone(),
    });
    let tx = DistributedTxRouter::new(1, clock.clone(), directory);

    tx.init_dir_attr(ROOT_INODE_ID, ROOT_INODE_ID, 0, 0o755)
        .await
        .unwrap();

    let neg = TxRaftCmd::CreateFile {
        pid: ROOT_INODE_ID,
        name: "neg.bin".to_string(),
        inode_id: 9,
        fid: 9,
        uid: 1,
        mode: 0o644,
        size_bytes: -8,
    };
    let neg_err = group
        .propose(
            group.current_epoch(),
            clock.now(),
            bincode::serialize(&neg).unwrap(),
        )
        .await
        .unwrap_err();
    assert_eq!(neg_err, MetaDbError::InvalidFileSize(-8));
    assert!(tx.lookup(ROOT_INODE_ID, "neg.bin").await.unwrap().is_none());

    tx.create_file(ROOT_INODE_ID, "ok.bin", 10, 10, 1, 0o644, 4)
        .await
        .unwrap();
    let set_err = tx
        .setattr(
            ROOT_INODE_ID,
            "ok.bin",
            SetAttrPatch {
                size_bytes: Some(-1),
                ..SetAttrPatch::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(set_err, MetaDbError::InvalidFileSize(-1));
    assert_eq!(
        tx.lookup(ROOT_INODE_ID, "ok.bin")
            .await
            .unwrap()
            .unwrap()
            .attr
            .size_bytes,
        4
    );

    tx.mkdir(ROOT_INODE_ID, "secret", 50, 0, 0o700)
        .await
        .unwrap();
    let clobber = TxRaftCmd::MkdirSingleShard {
        pid: ROOT_INODE_ID,
        name: "alias".to_string(),
        new_dir_id: 50,
        uid: 7,
        mode: 0o777,
    };
    let clobber_err = group
        .propose(
            group.current_epoch(),
            clock.now(),
            bincode::serialize(&clobber).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        clobber_err,
        MetaDbError::AlreadyExists { pid: 50, .. }
    ));
    assert_eq!(tx.dirstat(50).await.unwrap().uid, 0);
    assert!(tx.lookup(ROOT_INODE_ID, "alias").await.unwrap().is_none());

    tx.create_file(ROOT_INODE_ID, "apple", 81, 81, 1, 0o644, 1)
        .await
        .unwrap();
    tx.create_file(ROOT_INODE_ID, "avocado", 82, 82, 1, 0o644, 1)
        .await
        .unwrap();
    tx.create_file(ROOT_INODE_ID, "banana", 83, 83, 1, 0o644, 1)
        .await
        .unwrap();
    tx.create_file(ROOT_INODE_ID, "berry", 84, 84, 1, 0o644, 1)
        .await
        .unwrap();
    let (page, _) = tx
        .listdir_paginated(ROOT_INODE_ID, Some("apple"), Some("b"), 10)
        .await
        .unwrap();
    let listed: Vec<_> = page.into_iter().map(|e| e.key.name).collect();
    assert_eq!(
        listed,
        vec!["banana".to_string(), "berry".to_string()],
        "a cursor before the prefix must not end the page on the first non-match"
    );

    let hlc = clock.now();
    let first = TxRaftCmd::ApplyAuxBatch {
        entries: vec![quota_delta(1, (0, 0), 10, hlc)],
    };
    group
        .propose(
            group.current_epoch(),
            clock.now(),
            bincode::serialize(&first).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tx.get_quota(77).await.unwrap().unwrap().used_bytes, 10);

    let fork = TxRaftCmd::ApplyAuxBatch {
        entries: vec![quota_delta(4, (0, 0), 1_000, hlc)],
    };
    let fork_err = group
        .propose(
            group.current_epoch(),
            clock.now(),
            bincode::serialize(&fork).unwrap(),
        )
        .await
        .unwrap_err();
    assert!(matches!(fork_err, MetaDbError::AuxOutOfOrderGap { .. }));
    assert_eq!(tx.get_quota(77).await.unwrap().unwrap().used_bytes, 10);

    let next = TxRaftCmd::ApplyAuxBatch {
        entries: vec![quota_delta(2, (1, 0), 5, hlc)],
    };
    group
        .propose(
            group.current_epoch(),
            clock.now(),
            bincode::serialize(&next).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(tx.get_quota(77).await.unwrap().unwrap().used_bytes, 15);

    group.shutdown().await.unwrap();
}
