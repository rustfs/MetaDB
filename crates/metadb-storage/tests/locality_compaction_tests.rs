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

/// Verifies fixed-point compaction expansion across `0..=target_level` (`L0 + L1 + L2`) AND
/// `DataBlockBuilder` / `ShardedBlockCache` oversized guards.
#[test]
fn test_multilevel_fixed_point_compaction_and_oversized_block_guards() {
    use metadb_core::MetaDbError;
    use metadb_storage::block::{BlockEntryValue, DataBlock, DataBlockBuilder};
    use metadb_storage::cache::BlockCacheKey;
    use metadb_storage::{LsmStorageBackend, PersistentEntry, SstableBuilder, StorageBackend};
    use std::fs;

    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_fixed_point/lsm");
    let cache = Arc::new(ShardedBlockCache::new(16 * 16 * 1024)); // 16 KiB per shard

    // 1. Verify DataBlockBuilder rejects unshared key suffix > u16::MAX.
    let mut bb = DataBlockBuilder::new();
    let huge_key = vec![b'k'; 70_000];
    assert!(matches!(
        bb.add(&huge_key, &BlockEntryValue::Value(b"v".to_vec())),
        Err(MetaDbError::CodecError(_))
    ));

    // 2. Verify oversized DataBlock (> 16 KiB shard capacity) does not evict existing blocks!
    let mut small_bb = DataBlockBuilder::new();
    small_bb
        .add(b"small", &BlockEntryValue::Value(b"ok".to_vec()))
        .unwrap();
    let small_block = Arc::new(DataBlock::from_raw_checked(&small_bb.finish()).unwrap());
    let key_small = BlockCacheKey {
        shard_id: 1,
        sst_file_id: 1,
        block_offset: 0,
    };
    cache.insert(key_small, small_block);
    assert!(cache.get(&key_small).is_some());

    let mut big_bb = DataBlockBuilder::new();
    big_bb
        .add(b"big", &BlockEntryValue::Value(vec![0xAB; 64 * 1024]))
        .unwrap();
    let big_block = Arc::new(DataBlock::from_raw_checked(&big_bb.finish()).unwrap());
    // Insert oversized block into every shard index -> `key_small` MUST remain cached!
    for off in 1..=64 {
        cache.insert(
            BlockCacheKey {
                shard_id: 1,
                sst_file_id: off,
                block_offset: off * 4096,
            },
            big_block.clone(),
        );
    }
    assert!(cache.get(&key_small).is_some());

    // 3. Multi-level fixed-point expansion test:
    //    - Place `k_target` in `L2` (`file_id = 10`, level = 2)
    //    - Place stale `k_side = "stale_l1"` in `L1` (`file_id = 11`, level = 1) outside `[start, end)`
    //    - Flush wide `L0` SSTable containing BOTH `(k_target = "v_new")` and `(k_side = "fresh_l0")`!
    //    Compacting ONLY `[k_target, k_target + 1)` into `target_level = 2` MUST expand to pull in the
    //    overlapping `L1` SSTable so `fresh_l0` is never written to `L2` underneath `stale_l1` in `L1`!
    let k_target = MetaKey::child(10, "a.bin").encode();
    let k_side = MetaKey::child(90, "z.bin").encode();

    fs::create_dir_all(&lsm_dir).unwrap();
    {
        let (mut mf, _) = metadb_storage::ManifestLog::open_or_create(&lsm_dir).unwrap();
        let m_l2 = SstableBuilder::new(2, 10, 0, 1, HlcTimestamp::new(10, 0))
            .write_to_dir(
                &lsm_dir,
                &[(k_target.clone(), BlockEntryValue::Value(b"old_l2".to_vec()))],
            )
            .unwrap();
        let m_l1 = SstableBuilder::new(1, 11, 0, 2, HlcTimestamp::new(20, 0))
            .write_to_dir(
                &lsm_dir,
                &[(k_side.clone(), BlockEntryValue::Value(b"stale_l1".to_vec()))],
            )
            .unwrap();
        mf.append(&metadb_storage::VersionEdit {
            next_file_id: Some(20),
            next_flush_generation: Some(1),
            checkpoint: Some(DurableShardCheckpoint::default()),
            deleted_files: vec![],
            added_files: vec![m_l2, m_l1],
        })
        .unwrap();
    }

    let backend = LsmStorageBackend::open(1, &lsm_dir, cache, 10).unwrap();
    backend
        .flush_l0_batch(
            &[
                (
                    k_target.clone(),
                    PersistentEntry::Value(b"new_target".to_vec()),
                ),
                (k_side.clone(), PersistentEntry::Value(b"fresh_l0".to_vec())),
            ],
            DurableShardCheckpoint {
                flushed_raft_index: 3,
                flushed_max_hlc: HlcTimestamp::new(30, 0),
                ..Default::default()
            },
        )
        .unwrap();

    let mut k_target_end = k_target.clone();
    k_target_end.push(0x00);
    backend.compact_range(&k_target, &k_target_end).unwrap();

    let v = backend.current_version();
    assert_eq!(
        backend.get_in_version(&v, &k_target).unwrap(),
        Some(PersistentEntry::Value(b"new_target".to_vec()))
    );
    assert_eq!(
        backend.get_in_version(&v, &k_side).unwrap(),
        Some(PersistentEntry::Value(b"fresh_l0".to_vec())),
        "fixed-point expansion across 0..=target_level must prevent stale L1 from shadowing fresh L0 moved to L2"
    );
}
