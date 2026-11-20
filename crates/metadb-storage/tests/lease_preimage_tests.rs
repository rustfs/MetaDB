//! Point reads of a leased key must keep the value captured at `start_hlc`
//! after a newer version of that key is collapse-flushed.

use std::sync::Arc;
use std::time::{Duration, Instant};

use metadb_core::{
    decode_lease_preimage, lease_preimage_key, HlcTimestamp, LeasePreimageValue, MetaDbError,
    MetaKey, SYSTEM_PREFIX_LEASE_PREIMAGE,
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
