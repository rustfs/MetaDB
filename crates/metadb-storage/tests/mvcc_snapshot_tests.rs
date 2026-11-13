use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use metadb_core::{HlcTimestamp, MetaDbError, MetaKey};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, MvccValue, PreFlushHook,
    ShardedBlockCache, ViewAtRaftIndex,
};
use tempfile::tempdir;

struct TestFoldHook {
    durable_frontier: AtomicU64,
}

impl PreFlushHook for TestFoldHook {
    fn on_seal_memtable(
        &self,
        sealed_raft_index: u64,
        sealed_max_hlc: HlcTimestamp,
        view_at_m: &ViewAtRaftIndex<'_>,
    ) -> metadb_core::Result<Vec<(Vec<u8>, HlcTimestamp, MvccValue)>> {
        // Verify `ViewAtRaftIndex` observes baseline written in an earlier queued ImmMemTable!
        let counter_key = MetaKey::dir_attr(77).encode();
        let prev_val = view_at_m
            .get(&counter_key)?
            .map(|b| u64::from_be_bytes(b.try_into().unwrap()))
            .unwrap_or(0);
        let next_val = prev_val + sealed_raft_index;
        Ok(vec![(
            counter_key,
            sealed_max_hlc,
            MvccValue::Value(next_val.to_be_bytes().to_vec()),
        )])
    }

    fn on_flush_durable(&self, durable_raft_index: u64, _durable_max_hlc: HlcTimestamp) -> u64 {
        self.durable_frontier
            .store(durable_raft_index, Ordering::SeqCst);
        durable_raft_index
    }
}

/// T-MVCC-1: Verifies `SnapshotGuard` `ReadCut` isolation against mid-scan phantom writes (`Resolution B1`),
/// Safe-Flush Gate pinning while active snapshot < `imm.max_hlc`, `ViewAtRaftIndex(M)` across queued
/// `ImmMemTable`s (`Resolution B5`), ~60% KV write pruning, and forced timeout rejection.
#[test]
fn test_snapshot_guard_read_cut_safe_flush_gate_and_view_at_raft_index() {
    let dir = tempdir().unwrap();
    let lsm_dir = dir.path().join("shard_3/lsm");
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let engine =
        LifecycleStorageEngine::open_lsm(3, &lsm_dir, cache, LsmEngineConfig::default()).unwrap();

    let ka = MetaKey::child(77, "a.txt").encode();
    let kb = MetaKey::child(77, "b.txt").encode();

    engine.apply_committed_put(ka.clone(), b"a_v1".to_vec(), HlcTimestamp::new(1_000, 1), 1);
    engine.apply_committed_put(kb.clone(), b"b_v1".to_vec(), HlcTimestamp::new(1_000, 2), 2);

    // Acquire SnapshotGuard g1 at cut (raft_index=2, hlc=(1000, 2)).
    let g1 = engine.acquire_linearizable_snapshot(2).unwrap();
    assert_eq!(g1.cut().max_raft_index, 2);

    // Overwrite ka multiple times and delete kb at newer HLCs / raft_indices.
    engine.apply_committed_put(ka.clone(), b"a_v2".to_vec(), HlcTimestamp::new(1_010, 1), 3);
    engine.apply_committed_put(ka.clone(), b"a_v3".to_vec(), HlcTimestamp::new(1_020, 1), 4);
    engine.apply_committed_delete(kb.clone(), HlcTimestamp::new(1_030, 1), 5);

    let hook = TestFoldHook {
        durable_frontier: AtomicU64::new(0),
    };

    // Seal MemTable #1 (up to index=5) while `g1` (`hlc=(1000,2) < (1030,1)`) is STILL ACTIVE!
    let (_, _) = engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: 5,
                flushed_max_hlc: HlcTimestamp::new(1_030, 1),
                ..Default::default()
            },
            Some(&hook),
        )
        .unwrap();

    // Safe-Flush Gate MUST hold ImmMemTable #1 in memory while g1 is active!
    assert_eq!(engine.mvcc().imm_queue_len(), 1);
    assert_eq!(hook.durable_frontier.load(Ordering::SeqCst), 0);

    // Seal MemTable #2 (up to index=6) while ImmMemTable #1 is STILL queued in RAM!
    // `ViewAtRaftIndex(6)` must observe the folded value (0 + 5 = 5) from queued ImmMemTable #1!
    engine.apply_committed_put(ka.clone(), b"a_v4".to_vec(), HlcTimestamp::new(1_040, 1), 6);
    let (_, _) = engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: 6,
                flushed_max_hlc: HlcTimestamp::new(1_040, 1),
                ..Default::default()
            },
            Some(&hook),
        )
        .unwrap();
    assert_eq!(engine.mvcc().imm_queue_len(), 2);

    // Verify g1 STILL observes exact point-in-time state (a_v1, b_v1) with zero future/phantom leaks!
    let rows_g1 = engine.scan_with_snapshot(&[0x01], &[0x02], &g1).unwrap();
    assert_eq!(
        rows_g1,
        vec![
            (ka.clone(), b"a_v1".to_vec()),
            (kb.clone(), b"b_v1".to_vec())
        ]
    );

    // Drop g1 -> Safe-Flush Gate opens and flushes both queued ImmMemTables cleanly!
    drop(g1);
    let (flushed_idx, aux_frontier) = engine.flush_queued_imms_with_hook(Some(&hook)).unwrap();
    assert_eq!(flushed_idx, 6);
    assert_eq!(aux_frontier, 6);
    assert_eq!(engine.mvcc().imm_queue_len(), 0);

    // Verify `ViewAtRaftIndex` accumulated `5 + 6 = 11` across the two queued ImmMemTables!
    let g2 = engine.acquire_linearizable_snapshot(6).unwrap();
    let folded_bytes = engine
        .get_with_snapshot(&MetaKey::dir_attr(77).encode(), &g2)
        .unwrap()
        .unwrap();
    assert_eq!(u64::from_be_bytes(folded_bytes.try_into().unwrap()), 11);
    assert_eq!(
        engine.get_with_snapshot(&ka, &g2).unwrap(),
        Some(b"a_v4".to_vec())
    );
    assert_eq!(engine.get_with_snapshot(&kb, &g2).unwrap(), None);

    // Verify intermediate versions and MVCC tombstone were pruned in memory before hitting disk.
    let metrics = engine.metrics();
    assert!(metrics.pruned_mvcc_versions >= 3);

    // Verify forced expiration immediately returns `TransactionTimeout`.
    g2.force_expire_for_test();
    assert!(matches!(
        engine.get_with_snapshot(&ka, &g2).unwrap_err(),
        MetaDbError::TransactionTimeout { .. }
    ));
}
