use metadb_cluster::{AllocIdKind, DurableIdAllocator};
use metadb_core::codec;
use metadb_core::{HlcTimestamp, LeaseRenewReject, MetaDbError};

#[test]
fn independently_opened_allocators_must_not_grant_overlapping_ids() {
    let dir = tempfile::tempdir().unwrap();
    let first = DurableIdAllocator::open(dir.path()).unwrap();
    let second = match DurableIdAllocator::open(dir.path()) {
        Ok(second) => second,
        Err(_) => return, // Exclusive ownership would also satisfy the invariant.
    };
    let a = first.allocate_block(101, AllocIdKind::FileId, 8).unwrap();
    let b = second.allocate_block(102, AllocIdKind::FileId, 8).unwrap();
    assert!(
        a.0 + a.1 <= b.0 || b.0 + b.1 <= a.0,
        "independent handles issued overlapping grants: {a:?}, {b:?}"
    );
}

#[test]
fn independently_opened_allocators_must_share_rename_lease_exclusion() {
    let dir = tempfile::tempdir().unwrap();
    let first = DurableIdAllocator::open(dir.path()).unwrap();
    let second = match DurableIdAllocator::open(dir.path()) {
        Ok(second) => second,
        Err(_) => return,
    };
    let lease = first
        .acquire_dir_rename_lease(HlcTimestamp::new(1000, 0), 3000, 60_000)
        .unwrap();
    let conflicting = second.acquire_dir_rename_lease(HlcTimestamp::new(1000, 0), 3000, 60_000);
    assert!(
        conflicting.is_err(),
        "two authorities granted overlapping leases: {lease:?}, {conflicting:?}"
    );
}

#[test]
fn uuid_retry_with_different_id_kind_must_be_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    let file = allocator
        .allocate_block(123, AllocIdKind::FileInode, 8)
        .unwrap();
    let directory = allocator.allocate_block(123, AllocIdKind::DirInode, 8);
    assert!(
        directory.is_err(),
        "UUID replay returned a tagged file grant as directory IDs: {file:?}, {directory:?}"
    );
}

#[test]
fn allocator_lock_is_released_when_owner_drops() {
    let dir = tempfile::tempdir().unwrap();
    let first = DurableIdAllocator::open(dir.path()).unwrap();
    assert!(DurableIdAllocator::open(dir.path()).is_err());
    drop(first);
    assert!(DurableIdAllocator::open(dir.path()).is_ok());
}

#[test]
fn request_fingerprint_survives_reopen_and_rejects_changed_count() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    let grant = allocator
        .allocate_block(123, AllocIdKind::DirInode, 8)
        .unwrap();
    assert!(allocator
        .allocate_block(123, AllocIdKind::DirInode, 9)
        .is_err());
    drop(allocator);
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    assert_eq!(
        allocator
            .allocate_block(123, AllocIdKind::DirInode, 8)
            .unwrap(),
        grant
    );
    assert!(allocator
        .allocate_block(123, AllocIdKind::FileId, 8)
        .is_err());
}

fn write_legacy_state(dir: &std::path::Path, file_sequence: u64) {
    let state = (
        file_sequence,
        2u64,
        9u64,
        4u64,
        0u64,
        None::<(u64, HlcTimestamp)>,
        std::collections::BTreeMap::from([(123u128, (1u64, 8u64))]),
        std::collections::VecDeque::from([123u128]),
    );
    let mut raw = codec::serialize(&state).unwrap();
    metadb_core::append_crc32c_le(&mut raw);
    std::fs::write(dir.join("ID_ALLOCATOR_STATE"), raw).unwrap();
}

#[test]
fn legacy_allocator_migrates_without_reissuing_reserved_ids() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_state(dir.path(), 1);
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    assert!(allocator
        .allocate_block(123, AllocIdKind::FileId, 8)
        .is_err());
    let grant = allocator
        .allocate_block(456, AllocIdKind::FileId, 2)
        .unwrap();
    assert_eq!(grant, (9, 2));
    drop(allocator);
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    assert_eq!(
        allocator
            .allocate_block(456, AllocIdKind::FileId, 2)
            .unwrap(),
        grant
    );
}

#[test]
fn last_file_inode_block_does_not_overflow_local_cursor() {
    let dir = tempfile::tempdir().unwrap();
    write_legacy_state(
        dir.path(),
        metadb_core::FILE_INODE_TAG_BIT - metadb_cluster::CELL_FILE_ID_BLOCK_SIZE,
    );
    let allocator = std::sync::Arc::new(DurableIdAllocator::open(dir.path()).unwrap());
    let buffer = metadb_cluster::LocalIdBuffer::new(allocator, 1);
    let mut last = 0;
    for _ in 0..metadb_cluster::CELL_FILE_ID_BLOCK_SIZE {
        last = buffer.next_file_inode_id().unwrap();
    }
    assert_eq!(last, u64::MAX);
    assert!(buffer.next_file_inode_id().is_err());
}

#[test]
fn missing_allocator_state_never_resets_previously_granted_ids() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    allocator
        .allocate_block(123, AllocIdKind::FileId, 8)
        .unwrap();
    drop(allocator);
    let state = dir.path().join("ID_ALLOCATOR_STATE");
    std::fs::remove_file(&state).unwrap();
    assert!(DurableIdAllocator::open(dir.path()).is_err());
    assert!(
        !state.exists(),
        "refusal must preserve the incomplete directory for repair"
    );
}

#[test]
fn version_two_allocator_cannot_be_misread_as_legacy_counters() {
    type LegacyState = (
        u64,
        u64,
        u64,
        u64,
        u64,
        Option<(u64, HlcTimestamp)>,
        std::collections::BTreeMap<u128, (u64, u64)>,
        std::collections::VecDeque<u128>,
    );
    let dir = tempfile::tempdir().unwrap();
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    allocator
        .allocate_block(0, AllocIdKind::FileId, 128)
        .unwrap();
    drop(allocator);
    let raw = std::fs::read(dir.path().join("ID_ALLOCATOR_STATE")).unwrap();
    let body = metadb_core::verify_crc32c_le(&raw, "allocator").unwrap();
    assert!(
        codec::deserialize::<LegacyState>(body).is_err(),
        "an old decoder could reinterpret the versioned counters and reissue IDs"
    );
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    assert_eq!(
        allocator.allocate_block(0, AllocIdKind::FileId, 1).unwrap(),
        (129, 1)
    );
}

#[test]
fn rename_lease_ttl_above_the_cap_is_rejected_and_does_not_grant() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    let now = HlcTimestamp::new(1_000, 0);
    let rejected = allocator.acquire_dir_rename_lease(now, 60_001, 60_000);
    assert!(
        matches!(
            rejected,
            Err(MetaDbError::LeaseRenewRejected(
                LeaseRenewReject::RenameTtlExceedsCap
            ))
        ),
        "{rejected:?}"
    );
    let overflow =
        allocator.acquire_dir_rename_lease(HlcTimestamp::new(u64::MAX, 0), u64::MAX, u64::MAX);
    assert!(overflow.is_err(), "physical time plus TTL must not wrap");
    let (token, expire) = allocator
        .acquire_dir_rename_lease(now, 60_000, 60_000)
        .unwrap();
    assert_eq!(expire, HlcTimestamp::new(61_000, 0));
    assert_eq!(token, 1, "rejected grants must not consume the fence token");
    let minimum = {
        let dir = tempfile::tempdir().unwrap();
        let allocator = DurableIdAllocator::open(dir.path()).unwrap();
        allocator
            .acquire_dir_rename_lease(HlcTimestamp::new(5_000, 0), 1, 60_000)
            .unwrap()
    };
    assert_eq!(minimum.1, HlcTimestamp::new(5_100, 0));
}

#[test]
fn rename_lease_renewal_is_capped_and_survives_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let allocator = DurableIdAllocator::open(dir.path()).unwrap();
    let (token, expire) = allocator
        .acquire_dir_rename_lease(HlcTimestamp::new(10_000, 0), 1_000, 5_000)
        .unwrap();
    assert_eq!(expire, HlcTimestamp::new(11_000, 0));
    let extended = allocator
        .extend_dir_rename_lease(token, HlcTimestamp::new(10_500, 0), 1_000)
        .unwrap();
    assert_eq!(extended, HlcTimestamp::new(11_500, 0));
    assert!(allocator
        .acquire_dir_rename_lease(HlcTimestamp::new(10_500, 0), 1_000, 5_000)
        .is_err());
    assert!(allocator
        .extend_dir_rename_lease(token + 1, HlcTimestamp::new(10_500, 0), 1_000)
        .is_err());
    let mut expire = extended;
    while expire.physical_ms() < 15_000 {
        let now = expire.physical_ms() - 100;
        expire = allocator
            .extend_dir_rename_lease(token, HlcTimestamp::new(now, 0), 1_000)
            .unwrap();
    }
    assert_eq!(expire, HlcTimestamp::new(15_000, 0));
    assert!(allocator
        .extend_dir_rename_lease(token, HlcTimestamp::new(15_001, 0), 1_000)
        .is_err());
    drop(allocator);
    let reopened = DurableIdAllocator::open(dir.path()).unwrap();
    let still = reopened
        .extend_dir_rename_lease(token, HlcTimestamp::new(14_900, 0), 1_000)
        .unwrap();
    assert_eq!(still, HlcTimestamp::new(15_000, 0));
    assert!(reopened
        .acquire_dir_rename_lease(HlcTimestamp::new(14_900, 0), 1_000, 5_000)
        .is_err());
    assert!(reopened
        .extend_dir_rename_lease(token, HlcTimestamp::new(15_001, 0), 1_000)
        .is_err());
}
