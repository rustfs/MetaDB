//! Regressions for the first-ship storage and listdir failures on main `f325f6a`.

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use metadb_core::{HlcTimestamp, MetaKey};
use metadb_storage::sstable::{
    data_blocks_touched, full_range_materializations, reset_scan_probes,
};
use metadb_storage::{LifecycleStorageEngine, LsmEngineConfig, ScanControl, ShardedBlockCache};
use tempfile::tempdir;

fn engine(dir: &std::path::Path, config: LsmEngineConfig) -> LifecycleStorageEngine {
    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    LifecycleStorageEngine::open_lsm(1, dir, cache, config).unwrap()
}

#[test]
fn listdir_limit_does_not_walk_the_remaining_tombstone_range() {
    let dir = tempdir().unwrap();
    let lsm = dir.path().join("lsm");
    let engine = engine(
        &lsm,
        LsmEngineConfig {
            auto_locality_compaction: false,
            tombstone_density_threshold: 100_000,
            memtable_size_bytes: 32 * 1024 * 1024,
            ..LsmEngineConfig::default()
        },
    );

    let live = MetaKey::child(7, "a").encode();
    engine.apply_committed_put(
        live.clone(),
        b"live".to_vec(),
        HlcTimestamp::new(1_000, 1),
        1,
    );
    for index in 0..2048u64 {
        let key = MetaKey::child(7, format!("t{index:04}")).encode();
        engine.apply_committed_delete(key, HlcTimestamp::new(1_000, 2), index + 2);
    }
    engine.flush_mvcc_to_persistent();

    let version = engine.backend().current_version();
    let blocks: usize = version
        .levels
        .iter()
        .flat_map(|level| level.iter())
        .map(|sst| sst.index_entries().len())
        .sum();
    assert!(
        blocks > 4,
        "the fixture must span multiple data blocks, got {blocks}"
    );

    reset_scan_probes();
    let before = engine.metrics();
    let snap = engine.acquire_linearizable_snapshot(0).unwrap();
    let mut rows = Vec::new();
    engine
        .scan_live_until(
            &MetaKey::encode_pid_prefix_start(7),
            &MetaKey::encode_pid_prefix_end(7),
            &snap,
            |key, value| {
                rows.push((key.to_vec(), value.to_vec()));
                Ok(ScanControl::Stop)
            },
        )
        .unwrap();
    drop(snap);

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, live);
    let after = engine.metrics();
    assert_eq!(
        after.tombstones_traversed_on_scans - before.tombstones_traversed_on_scans,
        0,
        "limit=1 must not count the 2048 tombstones that sort after the live row"
    );
    assert_eq!(
        after.entries_examined_on_scans - before.entries_examined_on_scans,
        1
    );
    let touched = data_blocks_touched();
    assert!(
        touched < blocks as u64,
        "limit=1 touched {touched} data blocks of {blocks}"
    );
}

#[test]
fn locality_density_survives_reopen() {
    let dir = tempdir().unwrap();
    let lsm = dir.path().join("lsm");
    let config = LsmEngineConfig {
        auto_locality_compaction: true,
        tombstone_density_threshold: 10_000,
        ..LsmEngineConfig::default()
    };
    let sample = MetaKey::child(7, "a").encode();
    {
        let engine = engine(&lsm, config.clone());
        for index in 0..100u64 {
            let key = MetaKey::child(7, format!("n{index:03}")).encode();
            engine.apply_committed_put(
                key,
                b"v".to_vec(),
                HlcTimestamp::new(2_000, index as u16),
                index + 1,
            );
        }
        engine.flush_mvcc_to_persistent();
        for index in 0..20u64 {
            let key = MetaKey::child(7, format!("n{index:03}")).encode();
            engine.apply_committed_delete(key, HlcTimestamp::new(3_000, index as u16), 200 + index);
        }
        engine.flush_mvcc_to_persistent();
        assert_eq!(engine.compactor().snapshot_bucket_counts(&sample), (20, 80));
    }

    let cache = Arc::new(ShardedBlockCache::new(8 * 1024 * 1024));
    let reopened = LifecycleStorageEngine::open_lsm_existing(1, &lsm, cache, config).unwrap();
    assert_eq!(
        reopened.compactor().snapshot_bucket_counts(&sample),
        (20, 80),
        "density must be rebuilt from SSTables on open"
    );
}

#[test]
fn compact_range_does_not_materialize_whole_ssts() {
    let dir = tempdir().unwrap();
    let lsm = dir.path().join("lsm");
    let engine = engine(
        &lsm,
        LsmEngineConfig {
            auto_locality_compaction: false,
            ..LsmEngineConfig::default()
        },
    );
    let mut live = Vec::new();
    for index in 0..400u64 {
        let key = MetaKey::child(9, format!("k{index:04}")).encode();
        engine.apply_committed_put(
            key.clone(),
            vec![index as u8],
            HlcTimestamp::new(4_000, index as u16),
            index + 1,
        );
        if index % 2 == 0 {
            live.push(key);
        }
    }
    engine.flush_mvcc_to_persistent();
    for index in 0..400u64 {
        if index % 2 == 1 {
            let key = MetaKey::child(9, format!("k{index:04}")).encode();
            engine.apply_committed_delete(
                key,
                HlcTimestamp::new(5_000, index as u16),
                1_000 + index,
            );
        }
    }
    engine.flush_mvcc_to_persistent();

    reset_scan_probes();
    let purged = engine.compact_range(
        &MetaKey::encode_pid_prefix_start(9),
        &MetaKey::encode_pid_prefix_end(9),
    );
    assert!(purged >= 200, "purged {purged}");
    assert_eq!(
        full_range_materializations(),
        0,
        "compact_range must not call read_all_entries"
    );

    let snap = engine.acquire_linearizable_snapshot(0).unwrap();
    for key in &live {
        assert!(engine.get_with_snapshot(key, &snap).unwrap().is_some());
    }
    let odd = MetaKey::child(9, "k0001").encode();
    assert!(engine.get_with_snapshot(&odd, &snap).unwrap().is_none());
}

#[test]
fn tiny_memtable_flushes_and_immutable_tables_respect_the_cap() {
    let dir = tempdir().unwrap();
    let lsm = dir.path().join("lsm");
    let engine = Arc::new(engine(
        &lsm,
        LsmEngineConfig {
            auto_locality_compaction: false,
            memtable_size_bytes: 1,
            max_imm_memtables: 4,
            ..LsmEngineConfig::default()
        },
    ));

    engine.apply_committed_put(
        b"first".to_vec(),
        b"v".to_vec(),
        HlcTimestamp::new(6_000, 1),
        1,
    );
    assert!(
        engine.durable_checkpoint().flushed_raft_index >= 1,
        "a 1-byte memtable threshold must flush"
    );
    assert_eq!(engine.mvcc().imm_queue_len(), 0);

    let snap = engine.acquire_linearizable_snapshot(1).unwrap();
    for index in 0..4u64 {
        engine.apply_committed_put(
            format!("held-{index}").into_bytes(),
            b"x".to_vec(),
            HlcTimestamp::new(7_000, index as u16),
            10 + index,
        );
        assert!(engine.mvcc().imm_queue_len() <= 4);
    }
    assert_eq!(engine.mvcc().imm_queue_len(), 4);

    let blocked = Arc::clone(&engine);
    let (started_tx, started_rx) = mpsc::channel();
    let handle = thread::spawn(move || {
        started_tx.send(()).unwrap();
        blocked.apply_committed_put(
            b"fifth".to_vec(),
            b"y".to_vec(),
            HlcTimestamp::new(8_000, 1),
            20,
        );
    });
    started_rx.recv().unwrap();
    thread::sleep(Duration::from_millis(150));
    assert!(
        !handle.is_finished(),
        "the fifth immutable memtable must wait for backpressure"
    );
    assert_eq!(engine.mvcc().imm_queue_len(), 4);
    drop(snap);
    handle.join().unwrap();
    assert!(engine.mvcc().imm_queue_len() <= 4);

    drop(engine);
    let cache = Arc::new(ShardedBlockCache::new(1024 * 1024));
    let reopened = LifecycleStorageEngine::open_lsm(
        1,
        &lsm,
        cache,
        LsmEngineConfig {
            memtable_size_bytes: 32 * 1024 * 1024,
            max_imm_memtables: 4,
            auto_locality_compaction: false,
            ..LsmEngineConfig::default()
        },
    )
    .unwrap();
    let snap = reopened.acquire_linearizable_snapshot(0).unwrap();
    assert_eq!(
        reopened.get_with_snapshot(b"first", &snap).unwrap(),
        Some(b"v".to_vec())
    );
    assert_eq!(
        reopened.get_with_snapshot(b"fifth", &snap).unwrap(),
        Some(b"y".to_vec())
    );
}
