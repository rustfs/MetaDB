use std::fs::{self, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use metadb_core::{HlcTimestamp, MetaDbError, MetaKey};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache,
    SSTABLE_FOOTER_LEN,
};
use tempfile::tempdir;

/// T-LSM-1: Verifies 64-byte SSTable footer, L0 `flush_generation` tombstone shadowing over L1,
/// Bloom filter tombstone indexing, and CRC32C corruption detection.
#[test]
fn test_lsm_sstable_bloom_tombstone_shadowing_and_crc32c() {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_1/lsm");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let cfg = LsmEngineConfig {
        auto_locality_compaction: false,
        ..Default::default()
    };

    let engine = LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache.clone(), cfg.clone()).unwrap();
    // Verify exclusive `lsm/LOCK` prevents concurrent opening of the same directory!
    assert!(matches!(
        LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache.clone(), cfg.clone()),
        Err(MetaDbError::StorageIoError(_))
    ));
    let k1 = MetaKey::child(10, "alpha.txt").encode();
    let k2 = MetaKey::child(10, "beta.txt").encode();

    // Flush 1: live values -> compact into L1 so K1 lives in L1.
    engine.apply_committed_put(k1.clone(), b"v1_old".to_vec(), HlcTimestamp::new(100, 1), 1);
    engine.apply_committed_put(
        k2.clone(),
        b"v2_live".to_vec(),
        HlcTimestamp::new(100, 2),
        2,
    );
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: 2,
                flushed_max_hlc: HlcTimestamp::new(100, 2),
                ..Default::default()
            },
            None,
        )
        .unwrap();
    engine.compact_range(&[0x00], &[0xFF]);

    // Flush 2: delete K1 into L0 (PhysicalTombstone in L0 must shadow V1 in L1 via Bloom filter!).
    engine.apply_committed_delete(k1.clone(), HlcTimestamp::new(200, 1), 3);
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: 3,
                flushed_max_hlc: HlcTimestamp::new(200, 1),
                ..Default::default()
            },
            None,
        )
        .unwrap();

    let snap = engine.acquire_linearizable_snapshot(3).unwrap();
    assert_eq!(
        engine.get_with_snapshot(&k1, &snap).unwrap(),
        None,
        "L0 PhysicalTombstone must shadow L1 live value without Bloom false-negative"
    );
    assert_eq!(
        engine.get_with_snapshot(&k2, &snap).unwrap(),
        Some(b"v2_live".to_vec())
    );
    drop(snap);
    drop(engine);

    // Verify SSTable file size >= 64-byte footer and corrupt a byte in the footer to verify CRC32C rejection.
    let sst_files: Vec<_> = fs::read_dir(&lsm_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().and_then(|s| s.to_str()) == Some("sst"))
        .collect();
    assert!(!sst_files.is_empty());
    let target_sst = sst_files[0].path();
    let len = fs::metadata(&target_sst).unwrap().len();
    assert!(len > SSTABLE_FOOTER_LEN as u64);

    let mut f = OpenOptions::new().write(true).open(&target_sst).unwrap();
    f.seek(SeekFrom::End(-20)).unwrap();
    f.write_all(&[0xFF]).unwrap();
    f.sync_all().unwrap();
    drop(f);

    let reopen_err = match LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache, cfg) {
        Ok(_) => panic!("expected ChecksumMismatch on corrupted SSTable footer"),
        Err(e) => e,
    };
    assert!(matches!(reopen_err, MetaDbError::ChecksumMismatch { .. }));
}

/// T-LSM-2: Verifies MANIFEST torn final EOF record truncation, orphan `.sst.tmp` cleanup,
/// hard-linked checkpoint creation, and strict non-final corruption rejection (`Resolution B3`).
#[test]
fn test_manifest_torn_eof_recovery_and_non_final_corruption_rejection() {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_2/lsm");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let cfg = LsmEngineConfig::default();

    {
        let engine =
            LifecycleStorageEngine::open_lsm(2, &lsm_dir, cache.clone(), cfg.clone()).unwrap();
        for idx in 1..=5u64 {
            let key = MetaKey::child(20, format!("file_{}.bin", idx)).encode();
            engine.apply_committed_put(
                key,
                vec![idx as u8; 64],
                HlcTimestamp::new(500, idx as u16),
                idx,
            );
        }
        engine
            .seal_and_flush_with_hook(
                DurableShardCheckpoint {
                    flushed_raft_index: 5,
                    flushed_max_hlc: HlcTimestamp::new(500, 5),
                    ..Default::default()
                },
                None,
            )
            .unwrap();

        // Create hard-linked checkpoint for Raft snapshot verification.
        let ckpt_dir = dir.path().join("checkpoint_snap_5");
        let ckpt = engine.create_checkpoint(&ckpt_dir).unwrap();
        assert_eq!(ckpt.flushed_raft_index, 5);
    }

    // Leave an orphan `.sst.tmp` file and append a torn partial record at EOF of MANIFEST-000001.
    let orphan_tmp = lsm_dir.join("00000000000000000999.sst.tmp");
    fs::write(&orphan_tmp, b"partial_sstable").unwrap();

    let manifest_path = lsm_dir.join("MANIFEST-000001");
    {
        let mut mf = OpenOptions::new()
            .append(true)
            .open(&manifest_path)
            .unwrap();
        // Write header claiming 100 bytes payload, followed by only 5 bytes (torn tail!).
        mf.write_all(&100u32.to_le_bytes()).unwrap();
        mf.write_all(&0xDEAD_BEEFu32.to_le_bytes()).unwrap();
        mf.write_all(b"12345").unwrap();
        mf.sync_all().unwrap();
    }

    // Reopen: torn EOF record must be truncated, `.sst.tmp` cleaned up, and checkpoint=5 recovered!
    {
        let recovered =
            LifecycleStorageEngine::open_lsm(2, &lsm_dir, cache.clone(), cfg.clone()).unwrap();
        assert!(!orphan_tmp.exists());
        assert_eq!(recovered.durable_checkpoint().flushed_raft_index, 5);
        let snap = recovered.acquire_linearizable_snapshot(5).unwrap();
        let k3 = MetaKey::child(20, "file_3.bin").encode();
        assert_eq!(
            recovered.get_with_snapshot(&k3, &snap).unwrap(),
            Some(vec![3u8; 64])
        );
    }

    // A complete but CRC-invalid record at EOF is a torn tail and must be truncated.
    {
        let mut mf = OpenOptions::new()
            .append(true)
            .open(&manifest_path)
            .unwrap();
        let payload = b"bad!";
        mf.write_all(&(payload.len() as u32).to_le_bytes()).unwrap();
        mf.write_all(&0u32.to_le_bytes()).unwrap();
        mf.write_all(payload).unwrap();
        mf.sync_all().unwrap();
    }
    {
        let recovered =
            LifecycleStorageEngine::open_lsm(2, &lsm_dir, cache.clone(), cfg.clone()).unwrap();
        assert_eq!(recovered.durable_checkpoint().flushed_raft_index, 5);
    }

    // Now corrupt a NON-FINAL record inside MANIFEST-000001 and verify strict `ManifestCorrupted` error!
    {
        let mut mf = OpenOptions::new().write(true).open(&manifest_path).unwrap();
        mf.seek(SeekFrom::Start(9)).unwrap(); // Inside first init record!
        mf.write_all(&[0xAA]).unwrap();
        mf.sync_all().unwrap();
    }
    let err = match LifecycleStorageEngine::open_lsm(2, &lsm_dir, cache, cfg) {
        Ok(_) => panic!("expected ManifestCorrupted on non-final record corruption"),
        Err(e) => e,
    };
    assert!(matches!(err, MetaDbError::ManifestCorrupted(_)));
}
