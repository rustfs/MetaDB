//! Single-node scale-out: PID-boundary shard split on a persistent all-in-one process.
//!
//! After the split, meta/quota/secondary-index key ranges must still partition `[0x01, 0x04)`
//! with no gap, and directory operations must succeed on both sides of the cut. The same
//! checks run again after shutdown and reopen.

use std::collections::{BTreeSet, HashSet};

use metadb_cluster::CompositeShardRoute;
use metadb_core::{MetaKey, ROOT_INODE_ID};
use metadb_server::stress::persistent_config;
use metadb_server::PersistentMetaDbCluster;
use tempfile::tempdir;

fn assert_span_covered(routes: &[CompositeShardRoute], start: &[u8], end: &[u8]) {
    let mut clipped = Vec::new();
    for route in routes {
        let lo = if route.range.start_key.as_slice() > start {
            route.range.start_key.as_slice()
        } else {
            start
        };
        let hi = if route.range.end_key.as_slice() < end {
            route.range.end_key.as_slice()
        } else {
            end
        };
        if lo < hi {
            clipped.push((lo.to_vec(), hi.to_vec()));
        }
    }
    clipped.sort();
    assert!(
        !clipped.is_empty(),
        "no shard covers {start:?}..{end:?} among {} routes",
        routes.len()
    );
    assert_eq!(
        clipped[0].0.as_slice(),
        start,
        "keyspace does not start at the published root"
    );
    let mut cursor = clipped[0].1.clone();
    for (lo, hi) in clipped.iter().skip(1) {
        assert_eq!(
            lo.as_slice(),
            cursor.as_slice(),
            "gap or overlap at {cursor:?} next {lo:?}"
        );
        cursor = hi.clone();
    }
    assert_eq!(cursor.as_slice(), end, "keyspace ends early at {cursor:?}");
}

fn shard_for(cluster: &PersistentMetaDbCluster, pid: u64) -> u64 {
    let key = MetaKey::dir_attr(pid).encode();
    cluster
        .global_manager()
        .meta_cache()
        .lookup_key(&key)
        .unwrap_or_else(|| panic!("no route for pid {pid}"))
        .shard_id
}

async fn expect_dir(cluster: &PersistentMetaDbCluster, pid: u64, children: i64, bytes: i64) {
    let stat = cluster.tx_router().dirstat(pid).await.unwrap();
    assert_eq!(stat.num_children, children, "pid {pid}");
    assert_eq!(stat.size_bytes, bytes, "pid {pid}");
    let (entries, _) = cluster
        .tx_router()
        .listdir_paginated(pid, None, None, 100)
        .await
        .unwrap();
    assert_eq!(entries.len() as i64, children, "pid {pid}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_scale_out_preserves_keyspace_across_restart() {
    let dir = tempdir().unwrap();
    // Large threshold: growth happens through an explicit split, not a surprise maintenance split.
    let cfg = persistent_config(dir.path(), 4 * 1024 * 1024 * 1024);

    let (dirs, src_pid, dst_pid, shard_ids) = {
        let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
            .await
            .unwrap();
        let mut dirs = Vec::new();
        for i in 0..6 {
            let entry = cluster
                .mkdir_auto_id(ROOT_INODE_ID, &format!("bucket_{i}"), 3, 0o755)
                .await
                .unwrap();
            let pid = entry.attr.inode_id;
            for f in 0..6 {
                cluster
                    .create_file_auto_id(pid, &format!("f{f}.dat"), 3, 0o644, 256)
                    .await
                    .unwrap();
            }
            dirs.push(pid);
        }
        cluster
            .create_file_auto_id(ROOT_INODE_ID, "root_marker.txt", 3, 0o644, 64)
            .await
            .unwrap();

        let before = cluster.cell_manager().list_routes();
        assert_eq!(
            before.iter().filter(|route| route.shard_id == 1).count(),
            1,
            "meta table starts as one shard"
        );
        assert_span_covered(&before, &[0x01], &[0x04]);

        let child = cluster
            .cell_manager()
            .maybe_split_or_isolate_shard(1, true)
            .await
            .unwrap()
            .expect("meta shard must split on a PID boundary");
        assert!(child >= 4, "dynamic shard ids start at 4, got {child}");
        cluster.global_manager().refresh_meta_cache();

        let after = cluster.cell_manager().list_routes();
        assert_span_covered(&after, &[0x01], &[0x04]);
        let shard_ids: BTreeSet<u64> = after.iter().map(|route| route.shard_id).collect();
        assert!(shard_ids.contains(&child));
        assert!(shard_ids.len() >= 4);

        let mut owners = HashSet::new();
        for pid in &dirs {
            owners.insert(shard_for(&cluster, *pid));
            expect_dir(&cluster, *pid, 6, 6 * 256).await;
            cluster
                .create_file_auto_id(*pid, "after_split.dat", 3, 0o644, 128)
                .await
                .unwrap();
        }
        assert!(
            owners.len() >= 2,
            "directories must occupy both sides of the split, owners={owners:?}"
        );

        let dst_pid = dirs[0];
        let dst_shard = shard_for(&cluster, dst_pid);
        let src_pid = *dirs
            .iter()
            .find(|pid| shard_for(&cluster, **pid) != dst_shard)
            .expect("a directory on the other side of the split");
        assert_ne!(shard_for(&cluster, src_pid), dst_shard);

        cluster
            .tx_router()
            .rename(src_pid, "f0.dat", dst_pid, "from_other_shard.dat", false)
            .await
            .unwrap();
        assert!(cluster
            .tx_router()
            .lookup(src_pid, "f0.dat")
            .await
            .unwrap()
            .is_none());
        assert!(cluster
            .tx_router()
            .lookup(dst_pid, "from_other_shard.dat")
            .await
            .unwrap()
            .is_some());
        assert!(cluster
            .tx_router()
            .lookup(ROOT_INODE_ID, "root_marker.txt")
            .await
            .unwrap()
            .is_some());
        assert_span_covered(&cluster.cell_manager().list_routes(), &[0x01], &[0x04]);

        for pid in &dirs {
            let moved_away = if *pid == src_pid { 1 } else { 0 };
            let moved_here = if *pid == dst_pid { 1 } else { 0 };
            let children = 6 + 1 - moved_away + moved_here;
            let bytes = 6 * 256 + 128 - moved_away * 256 + moved_here * 256;
            expect_dir(&cluster, *pid, children, bytes).await;
            assert!(cluster
                .tx_router()
                .lookup(*pid, "after_split.dat")
                .await
                .unwrap()
                .is_some());
        }

        cluster.shutdown().await.unwrap();
        (dirs, src_pid, dst_pid, shard_ids)
    };

    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let reopened = cluster.cell_manager().list_routes();
    assert_span_covered(&reopened, &[0x01], &[0x04]);
    let reopened_ids: BTreeSet<u64> = reopened.iter().map(|route| route.shard_id).collect();
    assert_eq!(reopened_ids, shard_ids);

    for pid in &dirs {
        let moved_away = if *pid == src_pid { 1 } else { 0 };
        let moved_here = if *pid == dst_pid { 1 } else { 0 };
        let children = 6 + 1 - moved_away + moved_here;
        let bytes = 6 * 256 + 128 - moved_away * 256 + moved_here * 256;
        expect_dir(&cluster, *pid, children, bytes).await;
        assert!(cluster
            .tx_router()
            .lookup(*pid, "f1.dat")
            .await
            .unwrap()
            .is_some());
        assert!(cluster
            .tx_router()
            .lookup(*pid, "after_split.dat")
            .await
            .unwrap()
            .is_some());
    }
    assert!(cluster
        .tx_router()
        .lookup(src_pid, "f0.dat")
        .await
        .unwrap()
        .is_none());
    assert!(cluster
        .tx_router()
        .lookup(dst_pid, "from_other_shard.dat")
        .await
        .unwrap()
        .is_some());
    assert!(cluster
        .tx_router()
        .lookup(ROOT_INODE_ID, "root_marker.txt")
        .await
        .unwrap()
        .is_some());
    cluster.shutdown().await.unwrap();
}
