//! Tests for `LeveledCompactor` (`L0 -> L1 -> L2`), `MANIFEST` rotation (`>= 64` edits),
//! extended-length (`0x0D` max user-name) SSTable entries, block CRC corruption detection,
//! and `scan_live_until_with_stats` (`entries_examined` & `tombstones_traversed`).

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use metadb_core::{lease_preimage_key, HlcTimestamp, MetaDbError, MetaKey, MAX_USER_NAME_BYTES};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, LsmStorageBackend,
    ScanControl, ShardedBlockCache, StorageBackend,
};
use tempfile::tempdir;

#[test]
fn leveled_compaction_pushes_sstables_down_levels_and_preserves_live_state() {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_1/lsm");
    let cache = Arc::new(ShardedBlockCache::new(16 * 1024 * 1024));
    let cfg = LsmEngineConfig {
        auto_locality_compaction: false,
        l1_max_bytes: 1024,
        l2_max_bytes: 2048,
        ..Default::default()
    };
    let engine = LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache.clone(), cfg.clone()).unwrap();

    // Flush 6 SSTables (L0 trigger is 4) with small `l1_max_bytes` so L0 -> L1 -> L2 triggers.
    let mut raft_idx = 0u64;
    for batch_idx in 0..6u64 {
        let hlc = HlcTimestamp::new(1_000 + batch_idx * 10, 0);
        for k in 0..24u64 {
            raft_idx += 1;
            let key = MetaKey::child(10 + (k % 3), format!("file_{k:03}.dat")).encode();
            let val = format!("gen_{batch_idx}_val_{k:03}_padding_bytes_for_sstable").into_bytes();
            engine.apply_committed_put(key, val, hlc, raft_idx);
        }
        if batch_idx == 5 {
            for k in 0..6u64 {
                raft_idx += 1;
                let key = MetaKey::child(10 + (k % 3), format!("file_{k:03}.dat")).encode();
                engine.apply_committed_delete(key, hlc, raft_idx);
            }
        }
        engine
            .seal_and_flush_with_hook(
                DurableShardCheckpoint {
                    flushed_raft_index: raft_idx,
                    flushed_max_hlc: hlc,
                    ..Default::default()
                },
                None,
            )
            .unwrap();
    }

    drop(engine);
    let backend = LsmStorageBackend::open(1, &lsm_dir, cache.clone(), 10).unwrap();
    let ver = backend.current_version();
    assert!(
        !ver.levels[1].is_empty() || !ver.levels[2].is_empty() || !ver.levels[3].is_empty(),
        "expected SSTables promoted into L1/L2/L3, got levels: {:?}",
        ver.levels.iter().map(Vec::len).collect::<Vec<_>>()
    );
    drop(backend);

    let reopened = LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache, cfg).unwrap();
    let read_hlc = HlcTimestamp::new(10_000, 0);
    for k in 0..24u64 {
        let key = MetaKey::child(10 + (k % 3), format!("file_{k:03}.dat")).encode();
        let got = reopened.get(&key, read_hlc).unwrap();
        if k < 6 {
            assert!(
                got.is_none(),
                "deleted key {k} resurrected after leveled compaction"
            );
        } else {
            let expected = format!("gen_5_val_{k:03}_padding_bytes_for_sstable").into_bytes();
            assert_eq!(got.as_deref(), Some(expected.as_slice()));
        }
    }
}

#[test]
fn manifest_rotation_across_sixty_four_flushes_recovers_cleanly() {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_1/lsm");
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let cfg = LsmEngineConfig {
        auto_locality_compaction: false,
        ..Default::default()
    };

    {
        let engine =
            LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache.clone(), cfg.clone()).unwrap();
        for i in 1..=70u64 {
            let key = MetaKey::child(1, format!("k_{i:03}")).encode();
            let hlc = HlcTimestamp::new(5_000 + i, 0);
            engine.apply_committed_put(key, vec![b'v'; 32], hlc, i);
            engine
                .seal_and_flush_with_hook(
                    DurableShardCheckpoint {
                        flushed_raft_index: i,
                        flushed_max_hlc: hlc,
                        ..Default::default()
                    },
                    None,
                )
                .unwrap();
        }
    }

    let current = std::fs::read_to_string(lsm_dir.join("CURRENT")).unwrap();
    assert_ne!(
        current.trim(),
        "MANIFEST-000001",
        "expected MANIFEST rotation after >= 64 edits, CURRENT={current}"
    );

    let reopened = LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache, cfg).unwrap();
    assert_eq!(reopened.durable_checkpoint().flushed_raft_index, 70);
    for i in [1u64, 35, 64, 65, 70] {
        let key = MetaKey::child(1, format!("k_{i:03}")).encode();
        assert!(reopened
            .get(&key, HlcTimestamp::new(99_999, 0))
            .unwrap()
            .is_some());
    }
}

#[test]
fn max_name_lease_preimage_flushes_and_oversized_key_is_rejected() {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_1/lsm");
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let cfg = LsmEngineConfig::default();
    let engine = LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache, cfg).unwrap();

    let max_name = "x".repeat(MAX_USER_NAME_BYTES);
    let user_key = MetaKey::child(7, &max_name).encode();
    let preimage_key = lease_preimage_key(0xfeed_face, &user_key);
    assert!(preimage_key.len() > u16::MAX as usize);

    let hlc = HlcTimestamp::new(2_000, 0);
    engine.apply_committed_put(preimage_key.clone(), b"captured_preimage".to_vec(), hlc, 1);
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: 1,
                flushed_max_hlc: hlc,
                ..Default::default()
            },
            None,
        )
        .unwrap();

    assert_eq!(
        engine
            .get(&preimage_key, HlcTimestamp::new(2_001, 0))
            .unwrap()
            .as_deref(),
        Some(b"captured_preimage".as_slice())
    );

    // Keys larger than MAX_BLOCK_KEY_BYTES (70,000 bytes) must fail closed with CodecError.
    let too_large = vec![0x01; 70_000];
    let hlc2 = HlcTimestamp::new(2_002, 0);
    engine.apply_committed_put(too_large, b"nope".to_vec(), hlc2, 2);
    let err = engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: 2,
                flushed_max_hlc: hlc2,
                ..Default::default()
            },
            None,
        )
        .unwrap_err();
    assert!(matches!(err, MetaDbError::CodecError(_)), "got {err:?}");
}

#[test]
fn scan_stats_report_tombstones_and_corrupted_sstable_block_fails_crc() {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_1/lsm");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let cfg = LsmEngineConfig {
        auto_locality_compaction: false,
        tombstone_density_threshold: 4,
        tombstone_min_ratio_permille: 200,
        ..Default::default()
    };
    let engine = LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache, cfg.clone()).unwrap();

    let h1 = HlcTimestamp::new(1_000, 0);
    let mut lsn = 0u64;
    for i in 0..10u64 {
        lsn += 1;
        engine.apply_committed_put(
            MetaKey::child(55, format!("item_{i:02}")).encode(),
            vec![b'a'; 40],
            h1,
            lsn,
        );
    }
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: lsn,
                flushed_max_hlc: h1,
                ..Default::default()
            },
            None,
        )
        .unwrap();

    let h2 = HlcTimestamp::new(2_000, 0);
    for i in 0..6u64 {
        lsn += 1;
        engine.apply_committed_delete(MetaKey::child(55, format!("item_{i:02}")).encode(), h2, lsn);
    }
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: lsn,
                flushed_max_hlc: h2,
                ..Default::default()
            },
            None,
        )
        .unwrap();

    let start = MetaKey::encode_pid_prefix_start(55);
    let end = MetaKey::encode_pid_prefix_end(55);
    let snap = engine
        .acquire_fixed_snapshot(HlcTimestamp::new(3_000, 0))
        .unwrap();
    let mut live = 0usize;
    let stats = engine
        .scan_live_until_with_stats(&start, &end, &snap, |_k, _v| {
            live += 1;
            Ok(ScanControl::Continue)
        })
        .unwrap();
    drop(snap);
    assert_eq!(live, 4);
    assert!(
        stats.tombstones_traversed >= 6,
        "expected >= 6 tombstones traversed before compaction, got {stats:?}"
    );
    assert!(stats.entries_examined >= 10, "got {stats:?}");

    drop(engine);

    // Flip 1 byte in an SSTable data block on disk and verify uncached read rejects it with ChecksumMismatch.
    let sst_path = std::fs::read_dir(&lsm_dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .find(|p| p.extension().and_then(|s| s.to_str()) == Some("sst"))
        .expect("at least one sst file");
    {
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&sst_path)
            .unwrap();
        f.seek(SeekFrom::Start(4)).unwrap();
        f.write_all(&[0xff]).unwrap();
        f.sync_all().unwrap();
    }

    let fresh_cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let reopen_res = LifecycleStorageEngine::open_lsm(1, &lsm_dir, fresh_cache, cfg);
    assert!(
        matches!(reopen_res, Err(MetaDbError::ChecksumMismatch { .. })),
        "expected ChecksumMismatch on corrupted SSTable DataBlock during open/read"
    );
}

#[test]
fn leveled_promotion_fixed_point_expansion_preserves_non_overlapping_runs() {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_1/lsm");
    let cache = Arc::new(ShardedBlockCache::new(16 * 1024 * 1024));
    let cfg = LsmEngineConfig {
        auto_locality_compaction: false,
        l1_max_bytes: 512,
        l2_max_bytes: 1024,
        ..Default::default()
    };
    let engine = LifecycleStorageEngine::open_lsm(1, &lsm_dir, cache.clone(), cfg.clone()).unwrap();

    // Interleave wide and narrow key spans across 12 flushes so L0->L1 and L1->L2->L3 promotions
    // expand transitively across adjacent runs without ever creating overlapping SSTables in L1..L3.
    let mut lsn = 0u64;
    for round in 0..12u64 {
        let hlc = HlcTimestamp::new(10_000 + round * 10, 0);
        for k in (round % 3)..30u64 {
            lsn += 1;
            let key = MetaKey::child(2 + (k / 10), format!("entry_{k:03}.bin")).encode();
            if round % 4 == 3 && k % 5 == 0 {
                engine.apply_committed_delete(key, hlc, lsn);
            } else {
                let val = format!("r{round:02}_k{k:03}_{}", "x".repeat(48)).into_bytes();
                engine.apply_committed_put(key, val, hlc, lsn);
            }
        }
        engine
            .seal_and_flush_with_hook(
                DurableShardCheckpoint {
                    flushed_raft_index: lsn,
                    flushed_max_hlc: hlc,
                    ..Default::default()
                },
                None,
            )
            .unwrap();
    }

    let ver = engine.backend().current_version();
    for (lvl_idx, lvl) in ver.levels.iter().enumerate().skip(1) {
        for pair in lvl.windows(2) {
            assert!(
                pair[0].meta().max_key < pair[1].meta().min_key,
                "level L{lvl_idx} has overlapping SSTables: {:?} vs {:?}",
                pair[0].meta().max_key,
                pair[1].meta().min_key
            );
        }
    }

    let read_hlc = HlcTimestamp::new(50_000, 0);
    for k in 0..30u64 {
        let key = MetaKey::child(2 + (k / 10), format!("entry_{k:03}.bin")).encode();
        let got = engine.get(&key, read_hlc).unwrap();
        // Round 11 (`11 % 3 == 2`, `11 % 4 == 3`) touched `k >= 2` and deleted `k % 5 == 0` (`5,10,15,20,25`).
        // `k == 0` was last touched in round 9 (`put`); `k == 1` was last touched in round 10 (`put`).
        if k >= 2 && k % 5 == 0 {
            assert!(
                got.is_none(),
                "expected k={k} deleted at round 11, got {got:?}"
            );
        } else {
            let expected_round = if k == 0 {
                9
            } else if k == 1 {
                10
            } else {
                11
            };
            let expected = format!("r{expected_round:02}_k{k:03}_{}", "x".repeat(48)).into_bytes();
            assert_eq!(
                got.as_deref(),
                Some(expected.as_slice()),
                "mismatch on k={k}"
            );
        }
    }
}
