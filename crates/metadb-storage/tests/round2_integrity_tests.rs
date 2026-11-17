//! Round 2: the manifest is the durable authority for a live SST's identity.

use std::fs;
use std::sync::Arc;

use metadb_core::HlcTimestamp;
use metadb_storage::sstable::sst_file_name;
use metadb_storage::{
    LifecycleStorageEngine, LsmEngineConfig, ManifestLog, ShardedBlockCache, VersionEdit,
};

#[test]
fn live_sst_metadata_must_match_its_manifest_record() {
    let expected = tempfile::tempdir().unwrap();
    let unrelated = tempfile::tempdir().unwrap();
    for (dir, value, index) in [
        (expected.path(), b"expected".as_slice(), 1),
        (unrelated.path(), b"unrelated".as_slice(), 9),
    ] {
        let engine = LifecycleStorageEngine::open_lsm(
            1,
            dir,
            Arc::new(ShardedBlockCache::new(1024 * 1024)),
            LsmEngineConfig::default(),
        )
        .unwrap();
        engine.apply_committed_put(b"key".to_vec(), value.to_vec(), HlcTimestamp(index), index);
        assert_eq!(engine.flush_mvcc_to_persistent(), index);
    }

    // Simulate a misdirected but internally checksummed file. The manifest
    // still names index 1; the substituted SST explicitly declares index 9.
    fs::copy(
        unrelated.path().join(sst_file_name(1)),
        expected.path().join(sst_file_name(1)),
    )
    .unwrap();
    let reopened = LifecycleStorageEngine::open_lsm(
        1,
        expected.path(),
        Arc::new(ShardedBlockCache::new(1024 * 1024)),
        LsmEngineConfig::default(),
    );
    if let Ok(engine) = reopened {
        let checkpoint = engine.durable_checkpoint();
        let version = engine.backend().current_version();
        let declared_sst_index = version.levels[0][0].meta().max_raft_index;
        let snapshot = engine.acquire_linearizable_snapshot(1).unwrap();
        let observed = engine.get_with_snapshot(b"key", &snapshot).unwrap();
        panic!("accepted SST inconsistent with manifest: manifest checkpoint={}, SST index={declared_sst_index}, returned={observed:?}", checkpoint.flushed_raft_index);
    }
}

#[test]
fn every_sst_identity_field_is_checked_against_the_manifest() {
    for mismatch in [
        "level",
        "generation",
        "size",
        "min-key",
        "max-key",
        "entries",
        "tombstones",
        "raft-index",
        "hlc",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let engine = LifecycleStorageEngine::open_lsm(
            1,
            dir.path(),
            Arc::new(ShardedBlockCache::new(1024 * 1024)),
            LsmEngineConfig::default(),
        )
        .unwrap();
        engine.apply_committed_put(b"key".to_vec(), b"value".to_vec(), HlcTimestamp(1), 1);
        assert_eq!(engine.flush_mvcc_to_persistent(), 1);
        let mut metadata = engine.backend().current_version().levels[0][0]
            .meta()
            .clone();
        drop(engine);
        match mismatch {
            "level" => metadata.level += 1,
            "generation" => metadata.flush_generation += 1,
            "size" => metadata.file_size += 1,
            "min-key" => metadata.min_key = b"jey".to_vec(),
            "max-key" => metadata.max_key = b"ley".to_vec(),
            "entries" => metadata.num_entries += 1,
            "tombstones" => metadata.num_tombstones += 1,
            "raft-index" => metadata.max_raft_index += 1,
            "hlc" => metadata.max_hlc = HlcTimestamp(2),
            _ => unreachable!(),
        }
        let (mut manifest, _) = ManifestLog::open_or_create(dir.path()).unwrap();
        manifest
            .append(&VersionEdit {
                added_files: vec![metadata],
                ..Default::default()
            })
            .unwrap();
        drop(manifest);
        assert!(
            LifecycleStorageEngine::open_lsm(
                1,
                dir.path(),
                Arc::new(ShardedBlockCache::new(1024 * 1024)),
                LsmEngineConfig::default()
            )
            .is_err(),
            "accepted SST {mismatch} mismatch"
        );
    }
}

#[test]
fn verify_existing_never_initializes_an_empty_directory() {
    let dir = tempfile::tempdir().unwrap();
    assert!(LifecycleStorageEngine::open_lsm_existing(
        1,
        dir.path(),
        Arc::new(ShardedBlockCache::new(1024 * 1024)),
        LsmEngineConfig::default()
    )
    .is_err());
    assert!(!dir.path().join("CURRENT").exists());
    assert!(!dir.path().join("MANIFEST-000001").exists());
}
