//! Namespace isolation for [`DurableIdAllocator::open_for_node`](metadb_cluster::DurableIdAllocator::open_for_node).
//!
//! A fresh directory for `node_id > 1` starts in that node's inode, file-id, and shard-id
//! window. An allocator that has already granted ids is reopened from the stored counters.
//! Reopening it under a different node id must not silently relocate those counters.

use metadb_cluster::{AllocIdKind, DurableIdAllocator};
use metadb_core::{
    is_dir_inode_id, is_file_inode_id, make_file_inode_id, MetaDbError, FILE_INODE_TAG_BIT,
};

const MAX_NODE_ID: u64 = 32767;

fn file_seq_window(node_id: u64) -> (u64, u64) {
    let start = if node_id == 1 { 1 } else { (node_id << 48) + 1 };
    let end = if node_id == MAX_NODE_ID {
        FILE_INODE_TAG_BIT
    } else {
        ((node_id + 1) << 48) + 1
    };
    (start, end)
}

fn dir_inode_window(node_id: u64) -> (u64, u64) {
    let start = if node_id == 1 { 2 } else { (node_id << 48) + 2 };
    let end = if node_id == MAX_NODE_ID {
        FILE_INODE_TAG_BIT
    } else {
        ((node_id + 1) << 48) + 2
    };
    (start, end)
}

fn file_id_window(node_id: u64) -> (u64, u64) {
    file_seq_window(node_id)
}

fn shard_id_window(node_id: u64) -> (u64, u64) {
    let start = if node_id == 1 { 4 } else { (node_id << 32) + 4 };
    let end = if node_id == MAX_NODE_ID {
        u64::MAX
    } else {
        ((node_id + 1) << 32) + 4
    };
    (start, end)
}

fn assert_inside(kind: &str, node_id: u64, start: u64, count: u64, window: (u64, u64)) {
    let end = start.checked_add(count).expect("grant end");
    assert!(
        start >= window.0 && end <= window.1,
        "{kind} grant [{start}, {end}) from node {node_id} escaped {window:?}"
    );
}

fn grant(alloc: &DurableIdAllocator, uuid: u128, kind: AllocIdKind, count: u64) -> (u64, u64) {
    alloc
        .allocate_block(uuid, kind, count)
        .unwrap_or_else(|err| panic!("allocate {kind:?} on uuid {uuid:#x}: {err}"))
}

#[test]
fn fresh_node_above_one_starts_in_its_namespace() {
    let dir = tempfile::tempdir().unwrap();
    let node_id = 6u64;
    let alloc = DurableIdAllocator::open_for_node(dir.path(), node_id).unwrap();
    let (file_seq_start, _) = file_seq_window(node_id);
    let (dir_start, _) = dir_inode_window(node_id);
    let (file_id_start, _) = file_id_window(node_id);
    let (shard_start, _) = shard_id_window(node_id);

    let (file_inode, file_count) = grant(&alloc, 0x11, AllocIdKind::FileInode, 4);
    let (dir_inode, dir_count) = grant(&alloc, 0x12, AllocIdKind::DirInode, 4);
    let (file_id, file_id_count) = grant(&alloc, 0x13, AllocIdKind::FileId, 4);
    let (shard_id, shard_count) = grant(&alloc, 0x14, AllocIdKind::ShardId, 4);

    assert_eq!(file_inode, make_file_inode_id(file_seq_start));
    assert!(is_file_inode_id(file_inode));
    assert_eq!(dir_inode, dir_start);
    assert!(is_dir_inode_id(dir_inode));
    assert_eq!(file_id, file_id_start);
    assert_eq!(shard_id, shard_start);
    assert_inside(
        "file inode seq",
        node_id,
        file_seq_start,
        file_count,
        file_seq_window(node_id),
    );
    assert_inside(
        "dir inode",
        node_id,
        dir_inode,
        dir_count,
        dir_inode_window(node_id),
    );
    assert_inside(
        "file id",
        node_id,
        file_id,
        file_id_count,
        file_id_window(node_id),
    );
    assert_inside(
        "shard id",
        node_id,
        shard_id,
        shard_count,
        shard_id_window(node_id),
    );
}

#[test]
fn dirty_allocator_reopens_as_stored_and_is_not_relocated() {
    let dir = tempfile::tempdir().unwrap();
    let stored = {
        let alloc = DurableIdAllocator::open_for_node(dir.path(), 4).unwrap();
        let (file_inode, _) = grant(&alloc, 0x21, AllocIdKind::FileInode, 3);
        let (dir_inode, _) = grant(&alloc, 0x22, AllocIdKind::DirInode, 5);
        let (file_id, _) = grant(&alloc, 0x23, AllocIdKind::FileId, 7);
        let (shard_id, _) = grant(&alloc, 0x24, AllocIdKind::ShardId, 9);
        (file_inode, dir_inode, file_id, shard_id)
    };

    // The directory already granted ids. Opening it as node 32767 must continue the
    // stored watermarks from node 4, not jump to node 32767's fresh namespace.
    let alloc = DurableIdAllocator::open_for_node(dir.path(), MAX_NODE_ID).unwrap();
    let (file_inode, _) = grant(&alloc, 0x31, AllocIdKind::FileInode, 1);
    let (dir_inode, _) = grant(&alloc, 0x32, AllocIdKind::DirInode, 1);
    let (file_id, _) = grant(&alloc, 0x33, AllocIdKind::FileId, 1);
    let (shard_id, _) = grant(&alloc, 0x34, AllocIdKind::ShardId, 1);

    let (stored_file, stored_dir, stored_file_id, stored_shard) = stored;
    assert_eq!(file_inode, stored_file + 3);
    assert_eq!(dir_inode, stored_dir + 5);
    assert_eq!(file_id, stored_file_id + 7);
    assert_eq!(shard_id, stored_shard + 9);

    let (relocated_file_seq, _) = file_seq_window(MAX_NODE_ID);
    let (relocated_dir, _) = dir_inode_window(MAX_NODE_ID);
    let (relocated_file_id, _) = file_id_window(MAX_NODE_ID);
    let (relocated_shard, _) = shard_id_window(MAX_NODE_ID);
    assert_ne!(file_inode, make_file_inode_id(relocated_file_seq));
    assert_ne!(dir_inode, relocated_dir);
    assert_ne!(file_id, relocated_file_id);
    assert_ne!(shard_id, relocated_shard);
}

#[test]
fn reopened_same_node_continues_the_stored_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let first_file_id = {
        let alloc = DurableIdAllocator::open_for_node(dir.path(), 2).unwrap();
        let (file_id, count) = grant(&alloc, 0x41, AllocIdKind::FileId, 8);
        assert_eq!(file_id, (2u64 << 48) + 1);
        assert_eq!(count, 8);
        file_id
    };
    let alloc = DurableIdAllocator::open_for_node(dir.path(), 2).unwrap();
    let (next_file_id, _) = grant(&alloc, 0x42, AllocIdKind::FileId, 1);
    assert_eq!(next_file_id, first_file_id + 8);
    assert_inside("file id", 2, next_file_id, 1, file_id_window(2));
}

#[test]
fn independent_allocators_for_node_ids_1_through_32767_do_not_overlap() {
    // Fresh `open_for_node` counters, confirmed against real allocators below:
    // node 1 starts at the historical defaults, and every later node owns a
    // disjoint `[node_id << 48, next)` inode/file window and `[node_id << 32, next)` shard window.
    let sample = [1u64, 2, 3, 17, 255, 1024, 4096, 16384, 32766, MAX_NODE_ID];
    let mut issued_file_seqs = Vec::new();
    let mut issued_dir_inodes = Vec::new();
    let mut issued_file_ids = Vec::new();
    let mut issued_shard_ids = Vec::new();

    for node_id in sample {
        let dir = tempfile::tempdir().unwrap();
        let alloc = DurableIdAllocator::open_for_node(dir.path(), node_id).unwrap();
        let (file_inode, file_count) = grant(&alloc, 0x51, AllocIdKind::FileInode, 2);
        let (dir_inode, dir_count) = grant(&alloc, 0x52, AllocIdKind::DirInode, 2);
        let (file_id, file_id_count) = grant(&alloc, 0x53, AllocIdKind::FileId, 2);
        let (shard_id, shard_count) = grant(&alloc, 0x54, AllocIdKind::ShardId, 2);

        let (file_seq_start, _) = file_seq_window(node_id);
        assert_eq!(
            file_inode,
            make_file_inode_id(file_seq_start),
            "node {node_id}"
        );
        assert_eq!(dir_inode, dir_inode_window(node_id).0, "node {node_id}");
        assert_eq!(file_id, file_id_window(node_id).0, "node {node_id}");
        assert_eq!(shard_id, shard_id_window(node_id).0, "node {node_id}");
        assert!(is_file_inode_id(file_inode), "node {node_id}");
        assert!(is_dir_inode_id(dir_inode), "node {node_id}");

        issued_file_seqs.push((file_seq_start, file_seq_start + file_count));
        issued_dir_inodes.push((dir_inode, dir_inode + dir_count));
        issued_file_ids.push((file_id, file_id + file_id_count));
        issued_shard_ids.push((shard_id, shard_id + shard_count));
    }
    assert_ranges_disjoint("file inode seq", &issued_file_seqs);
    assert_ranges_disjoint("dir inode", &issued_dir_inodes);
    assert_ranges_disjoint("file id", &issued_file_ids);
    assert_ranges_disjoint("shard id", &issued_shard_ids);

    // The same windows cover every legal node id. Adjacent windows meet and do not overlap,
    // so two fresh allocators in `1..=32767` cannot hand out the same inode, file id, or shard id.
    let mut previous_file = None;
    let mut previous_dir = None;
    let mut previous_file_id = None;
    let mut previous_shard = None;
    for node_id in 1..=MAX_NODE_ID {
        previous_file = Some(assert_window_follows(
            "file inode seq",
            node_id,
            file_seq_window(node_id),
            previous_file,
        ));
        previous_dir = Some(assert_window_follows(
            "dir inode",
            node_id,
            dir_inode_window(node_id),
            previous_dir,
        ));
        previous_file_id = Some(assert_window_follows(
            "file id",
            node_id,
            file_id_window(node_id),
            previous_file_id,
        ));
        previous_shard = Some(assert_window_follows(
            "shard id",
            node_id,
            shard_id_window(node_id),
            previous_shard,
        ));
    }

    let rejected = DurableIdAllocator::open_for_node(tempfile::tempdir().unwrap().path(), 0);
    assert!(matches!(rejected, Err(MetaDbError::IdentityRejected(_))));
    let above =
        DurableIdAllocator::open_for_node(tempfile::tempdir().unwrap().path(), MAX_NODE_ID + 1);
    assert!(matches!(above, Err(MetaDbError::IdentityRejected(_))));
}

fn assert_window_follows(
    kind: &str,
    node_id: u64,
    window: (u64, u64),
    previous_end: Option<u64>,
) -> u64 {
    assert!(
        window.0 < window.1,
        "{kind} window for node {node_id} is empty: {window:?}"
    );
    if let Some(previous_end) = previous_end {
        assert!(
            window.0 >= previous_end,
            "{kind} window for node {node_id} overlaps the previous node at {window:?}, previous end {previous_end}"
        );
    }
    window.1
}

fn assert_ranges_disjoint(kind: &str, ranges: &[(u64, u64)]) {
    let mut ordered = ranges.to_vec();
    ordered.sort_unstable();
    for pair in ordered.windows(2) {
        assert!(
            pair[0].1 <= pair[1].0,
            "{kind} ranges overlap: {:?} and {:?}",
            pair[0],
            pair[1]
        );
    }
}
