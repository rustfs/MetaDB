//! A failed append fsync must not leave the rejected entry in memory or on disk.

use std::sync::Arc;

use openraft::entry::RaftEntry;
use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::alias::{EntryOf, LogIdOf};
use openraft::vote::RaftLeaderId;
use tempfile::tempdir;

use metadb_raft::{MetaDbTypeConfig, SegmentedRaftLogStore, ShardWatermarkTracker};

fn blank(index: u64) -> EntryOf<MetaDbTypeConfig> {
    let leader_id = openraft::impls::leader_id_adv::LeaderId::<u64, u64>::new(1, 1);
    EntryOf::<MetaDbTypeConfig>::new_blank(LogIdOf::<MetaDbTypeConfig>::new(leader_id, index))
}

#[tokio::test]
async fn failed_append_fsync_rolls_memory_and_segment_back() {
    let dir = tempdir().unwrap();
    let raft_dir = dir.path().join("raft");
    let watermarks = Arc::new(ShardWatermarkTracker::default());
    let mut store = SegmentedRaftLogStore::open(&raft_dir, watermarks).expect("open raft log");

    store
        .append([blank(1)], IOFlushed::Noop)
        .await
        .expect("first append");
    assert_eq!(store.retained_entry_count(), 1);

    store.testing_fail_next_syncs(1);
    let err = store
        .append([blank(2)], IOFlushed::Noop)
        .await
        .expect_err("injected fsync failure");
    assert!(
        err.to_string()
            .contains("injected raft append sync failure"),
        "{err}"
    );
    assert_eq!(store.retained_entry_count(), 1);

    drop(store);
    let reopened =
        SegmentedRaftLogStore::open(&raft_dir, Arc::new(ShardWatermarkTracker::default()))
            .expect("reopen raft log");
    assert_eq!(
        reopened.retained_entry_count(),
        1,
        "the unsynced entry must not be recovered"
    );
}
