use metadb_cluster::{AllocIdKind, DurableIdAllocator, LocalIdBuffer};
use metadb_core::HlcTimestamp;
use std::sync::Arc;

#[test]
fn local_id_buffer_never_reuses_grants_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let alloc = Arc::new(DurableIdAllocator::open(dir.path()).unwrap());
    let buffer = LocalIdBuffer::new(alloc, 7);
    let old = (
        buffer.next_file_inode_id().unwrap(),
        buffer.next_dir_inode_id().unwrap(),
        buffer.next_file_id().unwrap(),
    );
    drop(buffer);
    let alloc = Arc::new(DurableIdAllocator::open(dir.path()).unwrap());
    let restarted = LocalIdBuffer::new(alloc, 7);
    let new = (
        restarted.next_file_inode_id().unwrap(),
        restarted.next_dir_inode_id().unwrap(),
        restarted.next_file_id().unwrap(),
    );
    assert_ne!(old.0, new.0, "file inode reused after restart");
    assert_ne!(old.1, new.1, "directory inode reused after restart");
    assert_ne!(old.2, new.2, "file ID reused after restart");
}

#[test]
fn file_inode_grant_rejects_crossing_the_tag_bit() {
    let dir = tempfile::tempdir().unwrap();
    let alloc = DurableIdAllocator::open(dir.path()).unwrap();
    assert!(alloc
        .allocate_block(1, AllocIdKind::FileInode, 1u64 << 63)
        .is_err());
}

#[test]
fn rename_lease_ttl_overflow_returns_error_without_poisoning_allocator() {
    let dir = tempfile::tempdir().unwrap();
    let alloc = DurableIdAllocator::open(dir.path()).unwrap();
    assert!(alloc
        .acquire_dir_rename_lease(HlcTimestamp::new(1000, 0), u64::MAX, 60_000)
        .is_err());
    assert!(alloc.allocate_block(2, AllocIdKind::FileId, 1).is_ok());
}

#[test]
fn transaction_ids_remain_unique_after_coordinator_restart() {
    use metadb_cluster::{CompositeMetaCache, DurableGlobalManager};
    use metadb_core::HybridLogicalClock;
    use metadb_tx::ShardDirectory;
    let dir = tempfile::tempdir().unwrap();
    let new_manager = || {
        DurableGlobalManager::new(
            Arc::new(HybridLogicalClock::new()),
            Arc::new(DurableIdAllocator::open(dir.path()).unwrap()),
            Arc::new(CompositeMetaCache::new()),
        )
    };
    let manager = new_manager();
    let old = manager.allocate_txn_id(7).unwrap();
    drop(manager);
    let manager = new_manager();
    let new = manager.allocate_txn_id(7).unwrap();
    assert_ne!(old, new);
    assert_eq!(old >> 64, 7);
    assert_eq!(new >> 64, 7);
}
