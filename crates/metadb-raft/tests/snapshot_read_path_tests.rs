//! A purged or missing Raft snapshot file must not stop the read path.

use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;

use metadb_core::codec;
use metadb_core::{HlcTimestamp, HybridLogicalClock, MetaKey, ShardEpoch};
use metadb_raft::{
    BasicKvApplyHandler, BasicKvCommand, ChannelPool, MultiRaftRouter, RaftNodeOptions,
    ShardRaftGroup, ShardWatermarkTracker,
};
use metadb_storage::{LsmEngineConfig, ShardedBlockCache};
use openraft::storage::RaftStateMachine;
use tempfile::tempdir;

async fn boot(
    shard_dir: &std::path::Path,
    cache: Arc<ShardedBlockCache>,
    clock: Arc<HybridLogicalClock>,
) -> Arc<ShardRaftGroup> {
    let watermarks = Arc::new(ShardWatermarkTracker::new(0, 0, 0));
    let handler = Arc::new(BasicKvApplyHandler::new(
        ShardEpoch::initial(),
        watermarks.clone(),
    ));
    let router = MultiRaftRouter::new();
    let group = ShardRaftGroup::start(
        1,
        1,
        shard_dir,
        cache,
        LsmEngineConfig::default(),
        RaftNodeOptions::default(),
        handler,
        watermarks,
        clock,
        &router,
        ChannelPool::default(),
    )
    .await
    .unwrap();
    group
        .initialize_if_needed(BTreeMap::from([(1u64, "127.0.0.1:0".to_string())]))
        .await
        .unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(80)).await;
    group
}

async fn assert_keys(group: &ShardRaftGroup) {
    let epoch = group.current_epoch();
    let (_, snap) = group
        .linearizable_read(epoch)
        .await
        .unwrap_or_else(|error| {
            panic!("read path died after snapshot loss: {error}");
        });
    let start = MetaKey::child(1, "alpha").encode();
    let end = MetaKey::child(1, "zzz").encode();
    let listed = group
        .engine()
        .scan_with_snapshot(&start, &end, &snap)
        .unwrap_or_else(|error| panic!("list died after snapshot loss: {error}"));
    assert_eq!(listed.len(), 3, "list missed committed keys: {listed:?}");
    for name in ["alpha", "beta", "gamma"] {
        let key = MetaKey::child(1, name).encode();
        assert_eq!(
            group.engine().get_with_snapshot(&key, &snap).unwrap(),
            Some(name.as_bytes().to_vec()),
            "{name}"
        );
    }
}

#[tokio::test]
async fn missing_snapshot_after_flush_and_purge_does_not_kill_reads() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());

    {
        let group = boot(&shard_dir, cache.clone(), clock.clone()).await;
        let epoch = group.current_epoch();
        for name in ["alpha", "beta", "gamma"] {
            let cmd = BasicKvCommand::Put {
                key: MetaKey::child(1, name).encode(),
                value: name.as_bytes().to_vec(),
            };
            group
                .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
                .await
                .unwrap();
        }
        let purged = group.flush_snapshot_and_purge().await.unwrap();
        assert!(purged > 0);
        assert_eq!(group.log_store().retained_entry_count(), 0);
        assert_keys(&group).await;

        let snaps = shard_dir.join("raft/snapshots");
        let mut removed = false;
        for entry in fs::read_dir(&snaps).unwrap() {
            let entry = entry.unwrap();
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            if name.starts_with("snap_") && !name.contains('.') {
                fs::remove_file(entry.path().join("SNAPSHOT_META")).unwrap();
                removed = true;
            }
        }
        assert!(removed, "snapshot directory had no SNAPSHOT_META to remove");

        let mut state_machine = group.state_machine().clone();
        let loaded = state_machine.get_current_snapshot().await.unwrap();
        assert!(
            loaded.is_some(),
            "a live process must rewrite the missing snapshot from the LSM"
        );
        assert!(
            group.storage_unrecoverable().is_none(),
            "a rebuilt snapshot must still be reported as a live voter"
        );
        assert!(
            shard_dir
                .join("raft/snapshots")
                .read_dir()
                .unwrap()
                .any(|entry| {
                    entry
                        .unwrap()
                        .file_name()
                        .to_str()
                        .unwrap_or("")
                        .starts_with("snap_")
                }),
            "rewrite must put a snapshot directory back on disk"
        );
        assert_keys(&group).await;
        group.shutdown().await.unwrap();
    }

    let group = boot(&shard_dir, cache, clock).await;
    let mut state_machine = group.state_machine().clone();
    let rebuilt = state_machine.get_current_snapshot().await.unwrap();
    assert!(
        rebuilt.is_some(),
        "restart must rebuild a readable snapshot from the flushed LSM"
    );
    assert_keys(&group).await;
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn unpurged_raft_log_reconstructs_keys_without_a_snapshot() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    {
        let group = boot(&shard_dir, cache.clone(), clock.clone()).await;
        let epoch = group.current_epoch();
        for name in ["alpha", "beta", "gamma"] {
            let cmd = BasicKvCommand::Put {
                key: MetaKey::child(1, name).encode(),
                value: name.as_bytes().to_vec(),
            };
            group
                .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
                .await
                .unwrap();
        }
        assert!(
            group.log_store().retained_entry_count() > 0,
            "the segmented log is the WAL until purge"
        );
        let _ = fs::remove_dir_all(shard_dir.join("raft/snapshots"));
        group.shutdown().await.unwrap();
    }
    let group = boot(&shard_dir, cache, clock).await;
    assert!(
        group.storage_unrecoverable().is_none(),
        "an unpurged log must not be reported as an unrecoverable voter"
    );
    assert_keys(&group).await;
    group.shutdown().await.unwrap();
}

#[tokio::test]
async fn missing_lsm_and_snapshot_after_log_purge_is_not_a_live_voter() {
    let dir = tempdir().unwrap();
    let shard_dir = dir.path().join("shards/00000000000000000001");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let clock = Arc::new(HybridLogicalClock::new());
    let group = boot(&shard_dir, cache, clock).await;
    let epoch = group.current_epoch();
    for name in ["alpha", "beta", "gamma"] {
        let cmd = BasicKvCommand::Put {
            key: MetaKey::child(1, name).encode(),
            value: name.as_bytes().to_vec(),
        };
        group
            .propose(epoch, HlcTimestamp::ZERO, codec::serialize(&cmd).unwrap())
            .await
            .unwrap();
    }
    let purged = group.flush_snapshot_and_purge().await.unwrap();
    assert!(purged > 0);
    assert_eq!(group.log_store().retained_entry_count(), 0);

    let mut removed_sst = 0usize;
    for entry in fs::read_dir(shard_dir.join("lsm")).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if name.ends_with(".sst") {
            fs::remove_file(entry.path()).unwrap();
            removed_sst += 1;
        }
    }
    assert!(removed_sst > 0, "flush did not create an SSTable to remove");
    let _ = fs::remove_dir_all(shard_dir.join("raft/snapshots"));

    let mut state_machine = group.state_machine().clone();
    let error = state_machine
        .get_current_snapshot()
        .await
        .expect_err("missing LSM and snapshot must not look like a healthy snapshot");
    let message = error.to_string();
    assert!(
        message.contains("snapshot unrecoverable"),
        "missing durable copies must be an explicit error, not ENOENT: {message}"
    );
    assert!(
        message.contains("not reconstructed"),
        "the error must say committed data is not reconstructed: {message}"
    );
    let reason = group
        .storage_unrecoverable()
        .expect("cluster status must be able to see that this process is not a live voter");
    assert!(reason.contains("not reconstructed"), "{reason}");
    let read_error = match group.linearizable_read(group.current_epoch()).await {
        Ok(_) => panic!("reads must not succeed once storage is unrecoverable"),
        Err(error) => error,
    };
    assert!(
        read_error.to_string().contains("snapshot unrecoverable"),
        "{read_error}"
    );
    group.shutdown().await.unwrap();
}
