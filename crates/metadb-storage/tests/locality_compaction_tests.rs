use std::sync::Arc;

use metadb_core::{HlcTimestamp, MetaKey};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache,
};
use tempfile::tempdir;

/// T-COMP-1: Verifies bottommost-safe `LocalityCompactor` across overlapping L0/L1 SSTables
/// straddling outside `[start_key, end_key)` (`Resolution B4`):
/// - Purges 100% of physical tombstones inside the hot deletion bucket (`PID = 10`).
/// - Preserves live keys outside `[PID=10, PID=11)` in straddling L0 SSTables without data loss.
/// - Never resurrects older shadowed values from deeper levels.
#[test]
fn test_bottommost_locality_compactor_preserves_straddling_keys_and_purges_tombstones() {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_4/lsm");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let cfg = LsmEngineConfig {
        tombstone_density_threshold: 20,
        ..Default::default()
    };

    let engine = LifecycleStorageEngine::open_lsm(4, &lsm_dir, cache, cfg).unwrap();

    // 1. Populate PID=10 (30 keys) and PID=20 (10 keys) and compact into L1.
    let mut lsn = 0u64;
    for i in 0..30u64 {
        lsn += 1;
        let k = MetaKey::child(10, format!("obj_{:04}", i)).encode();
        engine.apply_committed_put(
            k,
            b"old_payload".to_vec(),
            HlcTimestamp::new(100, i as u16),
            lsn,
        );
    }
    for i in 0..10u64 {
        lsn += 1;
        let k = MetaKey::child(20, format!("keep_{:04}", i)).encode();
        engine.apply_committed_put(
            k,
            b"keep_v1".to_vec(),
            HlcTimestamp::new(101, i as u16),
            lsn,
        );
    }
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: lsn,
                flushed_max_hlc: HlcTimestamp::new(102, 0),
                ..Default::default()
            },
            None,
        )
        .unwrap();
    engine.compact_range(&[0x00], &[0xFF]);

    // 2. In a single L0 flush straddling BOTH PID=10 and PID=20:
    //    - Delete 25 keys in PID=10 (>= threshold 20 and >= 50% ratio -> triggers LocalityCompactor on PID=10!)
    //    - Update `keep_0005` in PID=20 to `keep_v2` (outside `[PID=10, PID=11)`!).
    for i in 0..25u64 {
        lsn += 1;
        let k = MetaKey::child(10, format!("obj_{:04}", i)).encode();
        engine.apply_committed_delete(k, HlcTimestamp::new(200, i as u16), lsn);
    }
    lsn += 1;
    let k_outer = MetaKey::child(20, "keep_0005").encode();
    engine.apply_committed_put(
        k_outer.clone(),
        b"keep_v2".to_vec(),
        HlcTimestamp::new(201, 0),
        lsn,
    );

    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: lsn,
                flushed_max_hlc: HlcTimestamp::new(201, 0),
                ..Default::default()
            },
            None,
        )
        .unwrap();

    // Verify LocalityCompactor triggered automatically, purged all 25 tombstones without
    // resurrecting old L1 values, and preserved `keep_v2` in straddling PID=20!
    let metrics = engine.metrics();
    assert!(metrics.compaction_stats.localized_compactions_triggered >= 1);
    assert_eq!(metrics.active_physical_tombstones, 0);

    let snap = engine.acquire_linearizable_snapshot(lsn).unwrap();
    let pid10_rows = engine
        .scan_with_snapshot(
            &MetaKey::encode_pid_prefix_start(10),
            &MetaKey::encode_pid_prefix_end(10),
            &snap,
        )
        .unwrap();
    assert_eq!(pid10_rows.len(), 5);
    assert_eq!(engine.metrics().tombstones_traversed_on_scans, 0);

    assert_eq!(
        engine.get_with_snapshot(&k_outer, &snap).unwrap(),
        Some(b"keep_v2".to_vec())
    );
}
