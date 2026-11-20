//! Point reads of a leased key must keep the value captured at `start_hlc`
//! after a newer version of that key is collapse-flushed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use metadb_core::{
    decode_lease_preimage, encode_lease_preimage, lease_preimage_key, HlcTimestamp,
    LeasePreimageValue, MetaDbError, MetaKey, SYSTEM_PREFIX_LEASE_PREIMAGE,
};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, MvccValue, ShardedBlockCache,
};

fn open(dir: &std::path::Path) -> LifecycleStorageEngine {
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    LifecycleStorageEngine::open_lsm(9, dir.join("lsm"), cache, LsmEngineConfig::default()).unwrap()
}

fn flush(engine: &LifecycleStorageEngine, index: u64, hlc: HlcTimestamp) {
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: index,
                flushed_max_hlc: hlc,
                ..Default::default()
            },
            None,
        )
        .unwrap();
}

#[test]
fn leased_point_read_keeps_the_first_capture_after_a_newer_collapse() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let key = MetaKey::child(7, "kept.txt").encode();
    let start = HlcTimestamp::new(1_000, 0);
    let later = HlcTimestamp::new(2_000, 0);
    engine.apply_committed_put(key.clone(), b"v0".to_vec(), start, 1);
    engine.mvcc().snapshot_registry().renew_lease_hold(
        42,
        start,
        Instant::now() + Duration::from_secs(30),
        &[key.clone()],
        &[(key.clone(), start, MvccValue::Value(b"v0".to_vec()))],
    );
    engine.apply_committed_put(key.clone(), b"v1".to_vec(), later, 2);
    flush(&engine, 2, later);

    assert_eq!(
        engine.get(&key, start).unwrap().as_deref(),
        Some(b"v0".as_slice()),
        "a leased point read must not return the collapsed newer value"
    );
    let newest = HlcTimestamp::new(u64::MAX, 0);
    assert_eq!(
        engine.get(&key, newest).unwrap().as_deref(),
        Some(b"v1".as_slice())
    );

    engine.mvcc().snapshot_registry().renew_lease_hold(
        42,
        start,
        Instant::now() + Duration::from_secs(30),
        &[key.clone()],
        &[(key.clone(), start, MvccValue::Value(b"v1".to_vec()))],
    );
    assert_eq!(
        engine.get(&key, start).unwrap().as_deref(),
        Some(b"v0".as_slice()),
        "a renew must not replace the capture"
    );

    let storage_key = lease_preimage_key(42, &key);
    let raw = engine
        .get(&storage_key, newest)
        .unwrap()
        .expect("the capture is in the same L0 flush");
    let (txn_id, user_key, hlc, value) = decode_lease_preimage(&storage_key, &raw).unwrap();
    assert_eq!(txn_id, 42);
    assert_eq!(user_key, key);
    assert_eq!(hlc, start);
    assert_eq!(value, LeasePreimageValue::Value(b"v0".to_vec()));

    drop(engine);
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let reopened = LifecycleStorageEngine::open_lsm_existing(
        9,
        dir.path().join("lsm"),
        cache,
        LsmEngineConfig::default(),
    )
    .unwrap();
    let raw = reopened.get(&storage_key, newest).unwrap().unwrap();
    assert_eq!(
        decode_lease_preimage(&storage_key, &raw).unwrap().3,
        LeasePreimageValue::Value(b"v0".to_vec()),
        "reopen must still see the durable capture"
    );
    assert!(
        matches!(
            reopened.get(&key, start).unwrap_err(),
            MetaDbError::TransactionTimeout { .. }
        ),
        "a reopened engine has no live hold, so it must not serve the newer collapsed value"
    );
}

#[test]
fn a_historical_point_read_without_a_capture_does_not_return_a_newer_collapsed_value() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let key = MetaKey::child(7, "plain.txt").encode();
    engine.apply_committed_put(key.clone(), b"v0".to_vec(), HlcTimestamp::new(1_000, 0), 1);
    engine.apply_committed_put(key.clone(), b"v1".to_vec(), HlcTimestamp::new(2_000, 0), 2);
    flush(&engine, 2, HlcTimestamp::new(2_000, 0));
    assert!(
        matches!(
            engine.get(&key, HlcTimestamp::new(1_000, 0)).unwrap_err(),
            MetaDbError::TransactionTimeout { .. }
        ),
        "missing capture must time out rather than return v1"
    );
    assert_eq!(
        engine
            .get(&key, HlcTimestamp::new(u64::MAX, 0))
            .unwrap()
            .as_deref(),
        Some(b"v1".as_slice())
    );
    let _ = SYSTEM_PREFIX_LEASE_PREIMAGE;
}

#[test]
fn range_scan_behind_the_flush_watermark_does_not_invent_absent_or_newer_keys() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let kept = MetaKey::child(7, "kept.txt").encode();
    let added = MetaKey::child(7, "added.txt").encode();
    let removed = MetaKey::child(7, "removed.txt").encode();
    let start = HlcTimestamp::new(1_000, 0);
    let later = HlcTimestamp::new(3_000, 0);
    engine.apply_committed_put(kept.clone(), b"kept".to_vec(), start, 1);
    engine.mvcc().snapshot_registry().renew_lease_hold(
        1,
        start,
        std::time::Instant::now() + std::time::Duration::from_secs(30),
        &[kept.clone(), removed.clone()],
        &[
            (kept.clone(), start, MvccValue::Value(b"kept".to_vec())),
            (removed.clone(), start, MvccValue::Tombstone),
        ],
    );
    engine.apply_committed_put(removed.clone(), b"later-body".to_vec(), later, 2);
    engine.apply_committed_put(added.clone(), b"new".to_vec(), later, 3);
    flush(&engine, 3, later);

    let scanned = engine.scan_range(
        &MetaKey::encode_pid_prefix_start(7),
        &MetaKey::encode_pid_prefix_end(7),
        start,
    );
    assert!(
        matches!(scanned, Err(MetaDbError::TransactionTimeout { .. })),
        "the range must fail closed instead of showing added.txt or hiding removed.txt: {scanned:?}"
    );
    assert_eq!(
        engine.get(&kept, start).unwrap().as_deref(),
        Some(b"kept".as_slice())
    );
    assert_eq!(engine.get(&removed, start).unwrap(), None);
    assert_eq!(
        engine
            .get(&added, HlcTimestamp::new(u64::MAX, 0))
            .unwrap()
            .as_deref(),
        Some(b"new".as_slice())
    );
    assert_eq!(
        engine
            .get(&removed, HlcTimestamp::new(u64::MAX, 0))
            .unwrap()
            .as_deref(),
        Some(b"later-body".as_slice()),
        "a current read must not stay on the captured absence"
    );
    let latest = engine
        .scan_range(
            &MetaKey::encode_pid_prefix_start(7),
            &MetaKey::encode_pid_prefix_end(7),
            HlcTimestamp::new(u64::MAX, 0),
        )
        .unwrap();
    assert!(latest.iter().any(|(key, _)| key == &added));
    assert!(latest.iter().any(|(key, _)| key == &removed));
}

#[test]
fn releasing_one_holder_keeps_the_other_capture_and_checkpoint_keeps_live_rows() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let key = MetaKey::child(7, "shared.txt").encode();
    let start_a = HlcTimestamp::new(1_000, 0);
    let start_b = HlcTimestamp::new(2_000, 0);
    engine.apply_committed_put(key.clone(), b"a".to_vec(), start_a, 1);
    engine.apply_committed_put(key.clone(), b"b".to_vec(), start_b, 2);
    let until = std::time::Instant::now() + std::time::Duration::from_secs(30);
    engine.mvcc().snapshot_registry().renew_lease_hold(
        1,
        start_a,
        until,
        &[key.clone()],
        &[(key.clone(), start_a, MvccValue::Value(b"a".to_vec()))],
    );
    engine.mvcc().snapshot_registry().renew_lease_hold(
        2,
        start_b,
        until,
        &[key.clone()],
        &[(key.clone(), start_b, MvccValue::Value(b"b".to_vec()))],
    );
    engine.apply_committed_put(key.clone(), b"c".to_vec(), HlcTimestamp::new(3_000, 0), 3);
    flush(&engine, 3, HlcTimestamp::new(3_000, 0));
    engine.mvcc().snapshot_registry().release_lease_hold(1);
    assert!(
        matches!(
            engine.get(&key, start_a).unwrap_err(),
            MetaDbError::TransactionTimeout { .. }
        ),
        "releasing A must not keep serving A's capture, and must not substitute C"
    );
    assert_eq!(
        engine.get(&key, start_b).unwrap().as_deref(),
        Some(b"b".as_slice()),
        "holder B must still be served after holder A is released"
    );

    let pin_b = lease_preimage_key(2, &key);
    let checkpoint = dir.path().join("checkpoint");
    engine.create_checkpoint(&checkpoint).unwrap();
    let cache = Arc::new(ShardedBlockCache::new(4 * 1024 * 1024));
    let restored = LifecycleStorageEngine::open_lsm_existing(
        9,
        &checkpoint,
        cache,
        LsmEngineConfig::default(),
    )
    .unwrap();
    let raw = restored
        .get(&pin_b, HlcTimestamp::new(u64::MAX, 0))
        .unwrap()
        .expect("checkpoint copies live 0x0D rows");
    assert_eq!(
        decode_lease_preimage(&pin_b, &raw).unwrap().3,
        LeasePreimageValue::Value(b"b".to_vec())
    );
}

#[test]
fn pinned_snapshot_scan_keeps_the_cut_after_later_writes_are_flushed() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let kept = MetaKey::child(4, "kept.txt").encode();
    let overwritten = MetaKey::child(4, "old.txt").encode();
    let deleted = MetaKey::child(4, "gone.txt").encode();
    let created = MetaKey::child(4, "new.txt").encode();
    let start = HlcTimestamp::new(1_000, 0);
    engine.apply_committed_put(kept.clone(), b"kept".to_vec(), start, 1);
    engine.apply_committed_put(overwritten.clone(), b"old".to_vec(), start, 2);
    engine.apply_committed_put(deleted.clone(), b"gone".to_vec(), start, 3);
    let snap = engine.acquire_linearizable_snapshot(3).unwrap();
    engine.apply_committed_put(
        overwritten.clone(),
        b"new".to_vec(),
        HlcTimestamp::new(2_000, 0),
        4,
    );
    engine.apply_committed_delete(deleted.clone(), HlcTimestamp::new(2_000, 0), 5);
    engine.apply_committed_put(
        created.clone(),
        b"created".to_vec(),
        HlcTimestamp::new(2_000, 0),
        6,
    );
    flush(&engine, 6, HlcTimestamp::new(2_000, 0));

    let listed = engine
        .scan_with_snapshot(
            &MetaKey::encode_pid_prefix_start(4),
            &MetaKey::encode_pid_prefix_end(4),
            &snap,
        )
        .unwrap();
    let value = |want: &[u8]| {
        listed
            .iter()
            .find(|(key, _)| key.as_slice() == want)
            .map(|(_, raw)| raw.clone())
    };
    assert_eq!(value(&kept).as_deref(), Some(b"kept".as_slice()));
    assert_eq!(value(&overwritten).as_deref(), Some(b"old".as_slice()));
    assert_eq!(value(&deleted).as_deref(), Some(b"gone".as_slice()));
    assert!(value(&created).is_none());
    drop(snap);
    drop(listed);
    flush(&engine, 6, HlcTimestamp::new(2_000, 0));
    let historical = engine.scan_range(
        &MetaKey::encode_pid_prefix_start(4),
        &MetaKey::encode_pid_prefix_end(4),
        start,
    );
    assert!(matches!(
        historical,
        Err(MetaDbError::TransactionTimeout { .. })
    ));
}

#[test]
fn releasing_the_hold_stops_serving_the_capture() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let key = MetaKey::child(7, "gone.txt").encode();
    let start = HlcTimestamp::new(1_000, 0);
    engine.apply_committed_put(key.clone(), b"v0".to_vec(), start, 1);
    engine.mvcc().snapshot_registry().renew_lease_hold(
        3,
        start,
        Instant::now() + Duration::from_secs(30),
        &[key.clone()],
        &[(key.clone(), start, MvccValue::Value(b"v0".to_vec()))],
    );
    engine.apply_committed_put(key.clone(), b"v1".to_vec(), HlcTimestamp::new(2_000, 0), 2);
    flush(&engine, 2, HlcTimestamp::new(2_000, 0));
    engine.mvcc().snapshot_registry().release_lease_hold(3);
    assert!(matches!(
        engine.get(&key, start).unwrap_err(),
        MetaDbError::TransactionTimeout { .. }
    ));
}

#[test]
fn flush_keeps_the_start_capture_when_the_same_prefix_has_a_newer_body() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let key = MetaKey::child(7, "capture.txt").encode();
    let start = HlcTimestamp::new(1_000, 0);
    let later = HlcTimestamp::new(3_000, 0);
    engine.apply_committed_put(key.clone(), b"v0".to_vec(), start, 1);
    engine.mvcc().snapshot_registry().renew_lease_hold(
        11,
        start,
        Instant::now() + Duration::from_secs(30),
        &[key.clone()],
        &[(key.clone(), start, MvccValue::Value(b"v0".to_vec()))],
    );
    let storage_key = lease_preimage_key(11, &key);
    engine.apply_committed_put(
        storage_key.clone(),
        encode_lease_preimage(later, &LeasePreimageValue::Value(b"newer".to_vec())),
        later,
        2,
    );
    engine.apply_committed_put(key.clone(), b"v1".to_vec(), later, 3);
    flush(&engine, 3, later);
    let raw = engine
        .get(&storage_key, HlcTimestamp::new(u64::MAX, 0))
        .unwrap()
        .expect("0x0D capture");
    assert_eq!(
        decode_lease_preimage(&storage_key, &raw).unwrap().3,
        LeasePreimageValue::Value(b"v0".to_vec()),
        "collapse must not replace the start capture with a newer 0x0D body"
    );
    assert_eq!(
        engine.get(&key, start).unwrap().as_deref(),
        Some(b"v0".as_slice())
    );
    assert_eq!(
        engine
            .get(&key, HlcTimestamp::new(u64::MAX, 0))
            .unwrap()
            .as_deref(),
        Some(b"v1".as_slice())
    );
}

#[test]
fn pinned_generation_compaction_keeps_the_cut_and_the_open_lease_capture() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path());
    let key = MetaKey::child(4, "gen.txt").encode();
    let start = HlcTimestamp::new(1_000, 0);
    let flushed = HlcTimestamp::new(2_000, 0);
    engine.apply_committed_put(key.clone(), b"v0".to_vec(), start, 1);
    engine.mvcc().snapshot_registry().renew_lease_hold(
        12,
        start,
        Instant::now() + Duration::from_secs(30),
        &[key.clone()],
        &[(key.clone(), start, MvccValue::Value(b"v0".to_vec()))],
    );
    flush(&engine, 1, flushed);
    let snap = engine.acquire_linearizable_snapshot(1).unwrap();
    let pinned_ids: Vec<u64> = engine
        .backend()
        .current_version()
        .levels
        .iter()
        .flatten()
        .map(|sst| sst.meta().file_id)
        .collect();
    assert!(
        !pinned_ids.is_empty(),
        "the first flush must publish a generation"
    );
    engine.apply_committed_put(key.clone(), b"v1".to_vec(), HlcTimestamp::new(3_000, 0), 2);
    flush(&engine, 2, HlcTimestamp::new(3_000, 0));
    let version = engine.backend().current_version();
    let mut min_key = None;
    let mut max_key = None;
    for level in &version.levels {
        for sst in level {
            let meta = sst.meta();
            min_key = Some(match min_key {
                Some(current) => std::cmp::min(current, meta.min_key.clone()),
                None => meta.min_key.clone(),
            });
            max_key = Some(match max_key {
                Some(current) => std::cmp::max(current, meta.max_key.clone()),
                None => meta.max_key.clone(),
            });
        }
    }
    let mut end = max_key.expect("flushed files");
    end.push(0);
    engine.compact_range(&min_key.expect("flushed files"), &end);
    let live_ids: Vec<u64> = engine
        .backend()
        .current_version()
        .levels
        .iter()
        .flatten()
        .map(|sst| sst.meta().file_id)
        .collect();
    assert!(
        pinned_ids.iter().any(|id| !live_ids.contains(id)),
        "compaction must replace the pinned generation; pinned={pinned_ids:?} live={live_ids:?}"
    );
    let listed = engine
        .scan_with_snapshot(
            &MetaKey::encode_pid_prefix_start(4),
            &MetaKey::encode_pid_prefix_end(4),
            &snap,
        )
        .unwrap();
    let pinned_value = listed
        .iter()
        .find(|(found, _)| found == &key)
        .map(|(_, raw)| raw.clone());
    assert_eq!(pinned_value.as_deref(), Some(b"v0".as_slice()));
    assert_eq!(
        engine.get(&key, start).unwrap().as_deref(),
        Some(b"v0".as_slice()),
        "an open lease must not observe the compacted newer value"
    );
    let storage_key = lease_preimage_key(12, &key);
    let raw = engine
        .get(&storage_key, HlcTimestamp::new(u64::MAX, 0))
        .unwrap()
        .expect("0x0D row survives compaction");
    assert_eq!(
        decode_lease_preimage(&storage_key, &raw).unwrap().3,
        LeasePreimageValue::Value(b"v0".to_vec())
    );
    assert_eq!(
        engine
            .get(&key, HlcTimestamp::new(u64::MAX, 0))
            .unwrap()
            .as_deref(),
        Some(b"v1".as_slice())
    );
}
