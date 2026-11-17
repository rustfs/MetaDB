//! Regression tests for fail-closed recovery. Every rejected reopen must preserve the
//! original files so an operator can restore/replay rather than lose forensic data.
use metadb_core::{HlcTimestamp, MetaDbError};
use metadb_storage::{
    DurableShardCheckpoint, LifecycleStorageEngine, LsmEngineConfig, ShardedBlockCache,
};
use std::collections::BTreeMap;
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

fn seed(path: &std::path::Path, compact: bool) {
    let engine = reopen(path).unwrap();
    engine.apply_committed_put(
        b"key".to_vec(),
        b"durable".to_vec(),
        HlcTimestamp::new(100, 0),
        1,
    );
    engine
        .seal_and_flush_with_hook(
            DurableShardCheckpoint {
                flushed_raft_index: 1,
                flushed_max_hlc: HlcTimestamp::new(100, 0),
                ..Default::default()
            },
            None,
        )
        .unwrap();
    if compact {
        engine.try_compact_range(b"a", b"z").unwrap();
    }
}
fn reopen(path: &std::path::Path) -> metadb_core::Result<LifecycleStorageEngine> {
    LifecycleStorageEngine::open_lsm(
        1,
        path,
        Arc::new(ShardedBlockCache::new(1024 * 1024)),
        LsmEngineConfig::default(),
    )
}
fn reject_without_deleting(path: &std::path::Path) {
    let before: BTreeMap<_, _> = fs::read_dir(path)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.file_name().unwrap() != "LOCK")
        .map(|p| {
            let raw = fs::read(&p).unwrap();
            (p, raw)
        })
        .collect();
    assert!(before
        .keys()
        .any(|p| p.extension().and_then(|x| x.to_str()) == Some("sst")));
    let result = reopen(path);
    assert!(
        matches!(result, Err(MetaDbError::ManifestCorrupted(_))),
        "corrupt manifest must fail closed instead of accepting a partial database"
    );
    for (file, raw) in before {
        assert_eq!(
            fs::read(&file).unwrap(),
            raw,
            "rejected reopen modified {}",
            file.display()
        );
    }
}

#[test]
fn incomplete_baseline_header_preserves_database() {
    for kept in 0..8 {
        let dir = tempdir().unwrap();
        seed(dir.path(), false);
        let mf = dir.path().join("MANIFEST-000001");
        let raw = fs::read(&mf).unwrap();
        fs::write(&mf, &raw[..kept]).unwrap();
        reject_without_deleting(dir.path());
    }
}

#[test]
fn impossible_manifest_counts_are_rejected_before_allocation() {
    use metadb_storage::VersionEdit;
    for count_offset in [4, 8] {
        let mut payload = VersionEdit::default().encode_payload();
        payload[count_offset..count_offset + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            VersionEdit::decode_payload(&payload),
            Err(MetaDbError::ManifestCorrupted(_))
        ));
    }
}
#[test]
fn corrupt_baseline_length_must_not_erase_ssts() {
    let dir = tempdir().unwrap();
    seed(dir.path(), false);
    let mf = dir.path().join("MANIFEST-000001");
    let mut raw = fs::read(&mf).unwrap();
    raw[3] ^= 0x80;
    fs::write(&mf, raw).unwrap();
    reject_without_deleting(dir.path());
}
#[test]
fn complete_crc_bad_tail_after_compaction_must_not_roll_back_manifest() {
    let dir = tempdir().unwrap();
    seed(dir.path(), true);
    let mf = dir.path().join("MANIFEST-000001");
    let mut raw = fs::read(&mf).unwrap();
    let last = raw.len() - 1;
    raw[last] ^= 1;
    fs::write(&mf, raw).unwrap();
    reject_without_deleting(dir.path());
}
#[test]
fn missing_current_must_not_initialize_over_existing_database() {
    let dir = tempdir().unwrap();
    seed(dir.path(), false);
    fs::remove_file(dir.path().join("CURRENT")).unwrap();
    reject_without_deleting(dir.path());
}
#[test]
fn sole_baseline_truncation_is_corruption_and_preserves_ssts() {
    // A standalone checkpoint is exactly one MANIFEST baseline referencing all SSTs.
    for removed in 1..=64 {
        let dir = tempdir().unwrap();
        let source = dir.path().join("source");
        seed(&source, false);
        let engine = reopen(&source).unwrap();
        let checkpoint = dir.path().join("checkpoint");
        engine.create_checkpoint(&checkpoint).unwrap();
        let mf = checkpoint.join("MANIFEST-000001");
        let raw = fs::read(&mf).unwrap();
        fs::write(&mf, &raw[..raw.len() - removed]).unwrap();
        reject_without_deleting(&checkpoint);
    }
}
#[test]
fn incomplete_new_tail_recovers_last_durable_state() {
    // Tail recovery remains allowed after a complete baseline/edits, including
    // every truncation of an append's header and payload.
    use metadb_storage::VersionEdit;
    let edit = VersionEdit {
        next_file_id: Some(999),
        ..Default::default()
    };
    let payload = edit.encode_payload();
    let mut frame = Vec::new();
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&metadb_core::checksum_crc32c(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    for kept in 1..frame.len() {
        let dir = tempdir().unwrap();
        seed(dir.path(), false);
        let mf = dir.path().join("MANIFEST-000001");
        let mut raw = fs::read(&mf).unwrap();
        raw.extend_from_slice(&frame[..kept]);
        fs::write(&mf, raw).unwrap();
        let engine = reopen(dir.path()).unwrap();
        assert_eq!(engine.durable_checkpoint().flushed_raft_index, 1);
        let snap = engine.acquire_linearizable_snapshot(1).unwrap();
        assert_eq!(
            engine.get_with_snapshot(b"key", &snap).unwrap(),
            Some(b"durable".to_vec())
        );
    }
}
#[test]
fn corrupted_nonfinal_length_is_not_a_torn_append() {
    let dir = tempdir().unwrap();
    seed(dir.path(), false);
    let engine = reopen(dir.path()).unwrap();
    engine.apply_committed_put(
        b"other".to_vec(),
        b"value".to_vec(),
        HlcTimestamp::new(101, 0),
        2,
    );
    engine.flush_mvcc_to_persistent();
    drop(engine);
    let mf = dir.path().join("MANIFEST-000001");
    let mut raw = fs::read(&mf).unwrap();
    let second = 8 + u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
    raw[second + 3] ^= 0x80;
    fs::write(&mf, raw).unwrap();
    reject_without_deleting(dir.path());
}
#[test]
fn corrupted_complete_final_length_is_not_a_torn_append() {
    let dir = tempdir().unwrap();
    seed(dir.path(), false);
    let mf = dir.path().join("MANIFEST-000001");
    let mut raw = fs::read(&mf).unwrap();
    let second = 8 + u32::from_le_bytes(raw[..4].try_into().unwrap()) as usize;
    raw[second + 3] ^= 0x80;
    fs::write(&mf, raw).unwrap();
    reject_without_deleting(dir.path());
}
#[test]
fn empty_manifest_is_corruption_and_preserves_ssts() {
    let dir = tempdir().unwrap();
    seed(dir.path(), false);
    fs::write(dir.path().join("MANIFEST-000001"), []).unwrap();
    reject_without_deleting(dir.path());
}
#[test]
fn replacing_lsm_files_does_not_reuse_cached_blocks() {
    let dir = tempdir().unwrap();
    let cache = Arc::new(ShardedBlockCache::new(1024 * 1024));
    let old = LifecycleStorageEngine::open_lsm(
        1,
        dir.path().join("old"),
        cache.clone(),
        LsmEngineConfig::default(),
    )
    .unwrap();
    old.apply_committed_put(
        b"key".to_vec(),
        b"old".to_vec(),
        HlcTimestamp::new(100, 0),
        1,
    );
    old.flush_mvcc_to_persistent();
    assert_eq!(
        old.get(b"key", HlcTimestamp::new(100, 0)).unwrap(),
        Some(b"old".to_vec())
    );
    // A snapshot installed from another history can assign file 1 to different bytes.
    let new = LifecycleStorageEngine::open_lsm(
        1,
        dir.path().join("new"),
        cache,
        LsmEngineConfig::default(),
    )
    .unwrap();
    new.apply_committed_put(
        b"key".to_vec(),
        b"new".to_vec(),
        HlcTimestamp::new(200, 0),
        2,
    );
    new.flush_mvcc_to_persistent();
    for _ in 0..3 {
        assert_eq!(
            new.get(b"key", HlcTimestamp::new(200, 0)).unwrap(),
            Some(b"new".to_vec()),
            "new file incarnation must not read old cached bytes"
        );
        assert_eq!(
            old.get(b"key", HlcTimestamp::new(100, 0)).unwrap(),
            Some(b"old".to_vec()),
            "old pinned reader must not populate the new reader's cache namespace"
        );
    }
}
#[test]
fn checksummed_sst_footer_with_impossible_length_returns_error_without_panicking() {
    use metadb_storage::block::BlockEntryValue;
    use metadb_storage::{SstableBuilder, SstableReader};
    let dir = tempdir().unwrap();
    SstableBuilder::new(0, 1, 1, 1, HlcTimestamp::new(100, 0))
        .write_to_dir(
            dir.path(),
            &[(b"key".to_vec(), BlockEntryValue::Value(b"value".to_vec()))],
        )
        .unwrap();
    let path = dir.path().join("00000000000000000001.sst");
    let mut raw = fs::read(&path).unwrap();
    let off = raw.len() - 64;
    raw[off + 8..off + 16].copy_from_slice(&u64::MAX.to_le_bytes());
    let mut input = raw[off..off + 52].to_vec();
    input.extend_from_slice(&raw[off + 56..]);
    raw[off + 52..off + 56].copy_from_slice(&metadb_core::checksum_crc32c(&input).to_le_bytes());
    fs::write(&path, raw).unwrap();
    let result = std::panic::catch_unwind(|| {
        SstableReader::open(1, path, Arc::new(ShardedBlockCache::new(1024 * 1024)))
    });
    assert!(
        result.is_ok(),
        "malformed SST footer caused allocation panic"
    );
    assert!(
        result.unwrap().is_err(),
        "out-of-file section must fail validation"
    );
}

#[test]
fn checkpoints_remain_readable_while_compaction_unlinks_inputs() {
    use std::sync::Barrier;
    let dir = tempdir().unwrap();
    let source = dir.path().join("source");
    seed(&source, false);
    let engine = Arc::new(reopen(&source).unwrap());
    let start = Arc::new(Barrier::new(2));
    let compact_engine = engine.clone();
    let compact_start = start.clone();
    let compactor = std::thread::spawn(move || {
        compact_start.wait();
        for _ in 0..100 {
            compact_engine.try_compact_range(b"a", b"z").unwrap();
        }
    });
    start.wait();
    for index in 0..40 {
        let checkpoint = dir.path().join(format!("checkpoint_{index}"));
        engine.create_checkpoint(&checkpoint).unwrap();
        let recovered = reopen(&checkpoint).unwrap();
        assert_eq!(recovered.durable_checkpoint().flushed_raft_index, 1);
        let snap = recovered.acquire_linearizable_snapshot(1).unwrap();
        assert_eq!(
            recovered.get_with_snapshot(b"key", &snap).unwrap(),
            Some(b"durable".to_vec())
        );
    }
    compactor.join().unwrap();
}
