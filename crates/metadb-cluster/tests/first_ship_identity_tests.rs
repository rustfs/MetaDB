use std::collections::BTreeSet;

use metadb_cluster::{plan_membership_voters, ClusterIdentity, DurableIdAllocator, LocalIdBuffer};
use metadb_core::{is_dir_inode_id, is_file_inode_id, MetaDbError};

#[test]
fn bootstrap_cluster_ids_do_not_collide_and_reopen_is_stable() {
    let first_dir = tempfile::tempdir().unwrap();
    let second_dir = tempfile::tempdir().unwrap();
    let first = ClusterIdentity::load_or_create(first_dir.path(), 1, 1, None).unwrap();
    let second = ClusterIdentity::load_or_create(second_dir.path(), 1, 1, None).unwrap();
    assert_ne!(first.cluster_id, second.cluster_id);
    assert_ne!(first.node_instance_id, second.node_instance_id);
    assert_ne!(first.cluster_id, 0);
    let reopened = ClusterIdentity::load_or_create(first_dir.path(), 1, 1, None).unwrap();
    assert_eq!(reopened, first);
    let rejected = ClusterIdentity::load_or_create(first_dir.path(), 1, 1, Some(second.cluster_id))
        .unwrap_err();
    assert!(
        matches!(rejected, MetaDbError::IdentityRejected(_)),
        "{rejected}"
    );
    let wrong_node = ClusterIdentity::load_or_create(first_dir.path(), 2, 1, None).unwrap_err();
    assert!(matches!(wrong_node, MetaDbError::IdentityRejected(_)));
}

#[test]
fn fresh_node_namespaces_do_not_overlap() {
    let node1_dir = tempfile::tempdir().unwrap();
    let node2_dir = tempfile::tempdir().unwrap();
    let node1 =
        std::sync::Arc::new(DurableIdAllocator::open_for_node(node1_dir.path(), 1).unwrap());
    let node2 =
        std::sync::Arc::new(DurableIdAllocator::open_for_node(node2_dir.path(), 2).unwrap());
    let buffer1 = LocalIdBuffer::new(node1, 1);
    let buffer2 = LocalIdBuffer::new(node2, 2);
    let file1 = buffer1.next_file_inode_id().unwrap();
    let file2 = buffer2.next_file_inode_id().unwrap();
    let dir1 = buffer1.next_dir_inode_id().unwrap();
    let dir2 = buffer2.next_dir_inode_id().unwrap();
    let fid1 = buffer1.next_file_id().unwrap();
    let fid2 = buffer2.next_file_id().unwrap();
    assert!(is_file_inode_id(file1) && is_file_inode_id(file2));
    assert!(is_dir_inode_id(dir1) && is_dir_inode_id(dir2));
    assert_ne!(file1, file2);
    assert_ne!(dir1, dir2);
    assert_ne!(fid1, fid2);
    assert!(file2 > file1);
    assert!(dir2 > dir1);
    let zero = DurableIdAllocator::open_for_node(tempfile::tempdir().unwrap().path(), 0);
    assert!(matches!(zero, Err(MetaDbError::IdentityRejected(_))));
}

#[test]
fn target_voter_count_does_not_drop_an_explicit_member() {
    let current = BTreeSet::from([1, 2]);
    let registered = BTreeSet::from([1, 2, 3]);
    let planned = plan_membership_voters(&current, &registered, 2, Some(3)).unwrap();
    assert_eq!(planned, BTreeSet::from([1, 2, 3]));

    let first = plan_membership_voters(&BTreeSet::from([1]), &registered, 2, Some(2)).unwrap();
    assert_eq!(first, BTreeSet::from([1, 2]));
    let second = plan_membership_voters(&first, &registered, 2, Some(3)).unwrap();
    assert_eq!(second, BTreeSet::from([1, 2, 3]));

    let shrunk = plan_membership_voters(&second, &registered, 2, None).unwrap();
    assert_eq!(shrunk, BTreeSet::from([1, 2]));
}
