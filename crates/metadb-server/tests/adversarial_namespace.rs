//! Adversarial checks against the in-memory engine: path-component names, non-negative
//! sizes, quota saturation, directory-inode reuse, and deep rename cycles.

use std::sync::Arc;
use std::thread;

use metadb_core::{MetaDbError, ROOT_INODE_ID};
use metadb_server::{MetaDbConfig, MetaDbEngine};

fn engine() -> MetaDbEngine {
    MetaDbEngine::new_single_node(MetaDbConfig {
        initial_node_id: 1,
        shard_split_threshold_bytes: 4 * 1024 * 1024 * 1024,
        mvcc_retention_ms: 5_000,
        tombstone_density_threshold: 128,
    })
}

#[test]
fn path_and_reserved_names_are_rejected() {
    let db = engine();
    for bad in ["", "_attr_", ".", "..", "a/b", "a\\b", "foo/../x", "a\0b"] {
        let err = db
            .create_file(ROOT_INODE_ID, bad, 100, 100, 1, 0o644, 1)
            .unwrap_err();
        assert!(
            matches!(err, MetaDbError::ReservedEntryName(_)),
            "name {:?} produced {:?}",
            bad,
            err
        );
    }
    db.create_file(ROOT_INODE_ID, ".hidden", 101, 101, 1, 0o644, 1)
        .unwrap();
    db.create_file(ROOT_INODE_ID, "foo..bar", 102, 102, 1, 0o644, 1)
        .unwrap();
    let names: Vec<_> = db
        .listdir(ROOT_INODE_ID)
        .unwrap()
        .into_iter()
        .map(|e| e.key.name)
        .collect();
    assert_eq!(names, vec![".hidden".to_string(), "foo..bar".to_string()]);
}

#[test]
fn negative_size_is_rejected_and_huge_sizes_do_not_wrap_quota() {
    let db = engine();
    let err = db
        .create_file(ROOT_INODE_ID, "neg.bin", 200, 200, 7, 0o644, -4_096)
        .unwrap_err();
    assert_eq!(err, MetaDbError::InvalidFileSize(-4_096));
    assert!(db.lookup(ROOT_INODE_ID, "neg.bin").unwrap().is_none());

    db.create_file(ROOT_INODE_ID, "max.bin", 201, 201, 7, 0o644, i64::MAX)
        .unwrap();
    db.create_file(ROOT_INODE_ID, "one.bin", 202, 202, 7, 0o644, 1)
        .unwrap();
    let stat = db.dirstat(ROOT_INODE_ID).unwrap();
    assert_eq!(stat.size_bytes, i64::MAX);
    assert_eq!(stat.num_children, 2);

    db.run_background_maintenance().unwrap();
    let quota = db.get_user_quota(7).expect("quota");
    assert_eq!(quota.used_bytes, i64::MAX);
    assert_eq!(quota.used_inodes, 2);
}

#[test]
fn reused_directory_inode_does_not_clobber_attr() {
    let db = engine();
    db.mkdir(ROOT_INODE_ID, "secret", 50, 0, 0o700).unwrap();
    db.create_file(50, "data", 500, 500, 0, 0o600, 8).unwrap();

    let err = db.mkdir(ROOT_INODE_ID, "alias", 50, 7, 0o777).unwrap_err();
    assert!(
        matches!(err, MetaDbError::AlreadyExists { pid: 50, .. }),
        "got {:?}",
        err
    );
    let stat = db.dirstat(50).unwrap();
    assert_eq!(stat.uid, 0);
    assert_eq!(stat.mode, 0o700);
    assert_eq!(stat.num_children, 1);
    assert!(db.lookup(ROOT_INODE_ID, "alias").unwrap().is_none());
    assert!(db.lookup(ROOT_INODE_ID, "secret").unwrap().is_some());

    let root_err = db
        .mkdir(ROOT_INODE_ID, "evil", ROOT_INODE_ID, 7, 0o777)
        .unwrap_err();
    assert!(matches!(
        root_err,
        MetaDbError::AlreadyExists {
            pid: ROOT_INODE_ID,
            ..
        }
    ));
    assert_eq!(db.dirstat(ROOT_INODE_ID).unwrap().num_children, 1);
}

#[test]
fn concurrent_mkdir_claims_one_inode() {
    let db = Arc::new(engine());
    let mut handles = Vec::new();
    for i in 0..8u64 {
        let db = Arc::clone(&db);
        handles.push(thread::spawn(move || {
            db.mkdir(ROOT_INODE_ID, &format!("n{i}"), 50, i, 0o755)
        }));
    }
    let winners: Vec<_> = handles
        .into_iter()
        .map(|h| h.join().unwrap())
        .filter(|r| r.is_ok())
        .collect();
    assert_eq!(winners.len(), 1);
    let names: Vec<_> = db
        .listdir(ROOT_INODE_ID)
        .unwrap()
        .into_iter()
        .map(|e| e.key.name)
        .collect();
    assert_eq!(names.len(), 1);
    assert_eq!(db.dirstat(50).unwrap().num_children, 0);
}

#[test]
fn directory_rename_rejects_cycles_including_deep_chains() {
    let db = engine();
    db.mkdir(ROOT_INODE_ID, "a", 50, 1, 0o755).unwrap();
    db.mkdir(ROOT_INODE_ID, "b", 51, 1, 0o755).unwrap();
    db.rename(ROOT_INODE_ID, "a", 51, "a2").unwrap();
    assert!(db.lookup(ROOT_INODE_ID, "a").unwrap().is_none());
    assert!(db.lookup(51, "a2").unwrap().is_some());

    db.mkdir(ROOT_INODE_ID, "top", 1_000, 1, 0o755).unwrap();
    let mut parent = 1_000u64;
    for i in 1..=1_100 {
        let id = 1_000 + i;
        db.mkdir(parent, "n", id, 1, 0o755).unwrap();
        parent = id;
    }
    let deepest = 1_000 + 1_100;
    let err = db
        .rename(ROOT_INODE_ID, "top", deepest, "cycle")
        .unwrap_err();
    assert_eq!(
        err,
        MetaDbError::InvalidRenameCycle {
            dir_id: 1_000,
            dst_pid: deepest
        }
    );
    assert!(db.lookup(ROOT_INODE_ID, "top").unwrap().is_some());
    assert!(db.lookup(deepest, "cycle").unwrap().is_none());
}
