//! Single-node coverage against a real persistent MetaDB process.
//!
//! Opens `PersistentMetaDbCluster` (`MetaDbEngine::open` / `open_and_serve`) on a temporary
//! `data_dir`. This is not `MetaDbEngine::new_single_node`, which is an in-memory simulator.

use std::sync::Arc;

use metadb_core::{MetaDbError, ROOT_INODE_ID};
use metadb_proxy::ProxySqlResult;
use metadb_server::stress::persistent_config;
use metadb_server::PersistentMetaDbCluster;
use metadb_tx::SetAttrPatch;
use tempfile::tempdir;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_single_node_metadata_ops_survive_restart() {
    let dir = tempdir().unwrap();
    let cfg = persistent_config(dir.path(), 4 * 1024 * 1024 * 1024);

    let (work_id, archive_id, kept_fid, moved_fid) = {
        let (cluster, addr) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
            .await
            .unwrap();
        assert!(addr.starts_with("127.0.0.1:"), "bound {addr}");

        let tx = cluster.tx_router();
        let limit = tx.set_quota_limit(7, 10_000_000).await.unwrap();
        assert_eq!(limit.quota_limit_bytes, 10_000_000);

        let work = cluster
            .mkdir_auto_id(ROOT_INODE_ID, "work", 7, 0o755)
            .await
            .unwrap();
        let archive = cluster
            .mkdir_auto_id(ROOT_INODE_ID, "archive", 7, 0o755)
            .await
            .unwrap();
        let work_id = work.attr.inode_id;
        let archive_id = archive.attr.inode_id;

        let kept = cluster
            .create_file_auto_id(work_id, "kept.bin", 7, 0o644, 4096)
            .await
            .unwrap();
        let moved = cluster
            .create_file_auto_id(work_id, "temp.bin", 7, 0o644, 1024)
            .await
            .unwrap();
        let kept_fid = kept.attr.fid.expect("file id");
        let moved_fid = moved.attr.fid.expect("file id");

        let found = tx.lookup(work_id, "kept.bin").await.unwrap().unwrap();
        assert_eq!(found.attr.inode_id, kept.attr.inode_id);
        assert!(tx.lookup(work_id, "missing.bin").await.unwrap().is_none());

        let (page, _) = tx.listdir_paginated(work_id, None, None, 10).await.unwrap();
        let names: Vec<_> = page.iter().map(|entry| entry.key.name.as_str()).collect();
        assert_eq!(names, vec!["kept.bin", "temp.bin"]);

        let stat = tx.dirstat(work_id).await.unwrap();
        assert_eq!(stat.num_children, 2);
        assert_eq!(stat.size_bytes, 4096 + 1024);

        let patched = tx
            .setattr(
                work_id,
                "kept.bin",
                SetAttrPatch {
                    mode: Some(0o640),
                    ..SetAttrPatch::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(patched.attr.mode, 0o640);

        tx.rename(work_id, "temp.bin", archive_id, "stored.bin", false)
            .await
            .unwrap();
        assert!(tx.lookup(work_id, "temp.bin").await.unwrap().is_none());
        assert!(tx.lookup(archive_id, "stored.bin").await.unwrap().is_some());
        assert_eq!(tx.dirstat(work_id).await.unwrap().num_children, 1);
        assert_eq!(tx.dirstat(archive_id).await.unwrap().num_children, 1);
        assert_eq!(tx.dirstat(work_id).await.unwrap().size_bytes, 4096);
        assert_eq!(tx.dirstat(archive_id).await.unwrap().size_bytes, 1024);

        let busy = tx.rmdir(ROOT_INODE_ID, "work").await.unwrap_err();
        assert!(matches!(
            busy,
            MetaDbError::DirectoryNotEmpty { dir_id, .. } if dir_id == work_id
        ));

        let reserved = tx
            .create_file(ROOT_INODE_ID, "_attr_", 99, 99, 7, 0o644, 1)
            .await
            .unwrap_err();
        assert!(matches!(reserved, MetaDbError::ReservedEntryName(_)));

        let sql = format!("SELECT * FROM MetaTable WHERE pid = {work_id} AND name = 'kept.bin'");
        let params: &[metadb_proxy::SqlParam] = &[];
        match cluster
            .proxy_router()
            .execute_sql(&sql, params)
            .await
            .unwrap()
        {
            ProxySqlResult::OptionalEntry(Some(entry)) => assert_eq!(entry.attr.mode, 0o640),
            other => panic!("unexpected sql result: {other:?}"),
        }

        cluster.run_maintenance_tick().await.unwrap();
        let quota = tx.get_quota(7).await.unwrap().expect("quota");
        assert_eq!(quota.used_bytes, 4096 + 1024);
        // Two directories plus two files.
        assert_eq!(quota.used_inodes, 4);
        let kept_idx = tx.get_secondary_index(kept_fid).await.unwrap().unwrap();
        assert_eq!(kept_idx.pid, work_id);
        assert_eq!(kept_idx.name, "kept.bin");
        let moved_idx = tx.get_secondary_index(moved_fid).await.unwrap().unwrap();
        assert_eq!(moved_idx.pid, archive_id);
        assert_eq!(moved_idx.name, "stored.bin");
        let metrics = cluster.metrics_snapshot();
        assert!(metrics.cells_total >= 1);
        assert!(
            metrics.shards_total >= 3,
            "bootstrap opens meta, quota, and secondary-index shards, got {}",
            metrics.shards_total
        );

        cluster.shutdown().await.unwrap();
        (work_id, archive_id, kept_fid, moved_fid)
    };

    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let tx = cluster.tx_router();
    let stat = tx.dirstat(work_id).await.unwrap();
    assert_eq!(stat.num_children, 1);
    assert_eq!(stat.size_bytes, 4096);
    let kept = tx.lookup(work_id, "kept.bin").await.unwrap().unwrap();
    assert_eq!(kept.attr.mode, 0o640);
    assert_eq!(kept.attr.fid, Some(kept_fid));
    assert!(tx.lookup(work_id, "temp.bin").await.unwrap().is_none());
    let stored = tx.lookup(archive_id, "stored.bin").await.unwrap().unwrap();
    assert_eq!(stored.attr.size_bytes, 1024);
    assert_eq!(tx.dirstat(archive_id).await.unwrap().num_children, 1);

    let root_children = tx
        .listdir_paginated(ROOT_INODE_ID, None, None, 10)
        .await
        .unwrap()
        .0;
    let root_names: Vec<_> = root_children
        .iter()
        .map(|entry| entry.key.name.as_str())
        .collect();
    assert!(root_names.contains(&"work"));
    assert!(root_names.contains(&"archive"));

    cluster.run_maintenance_tick().await.unwrap();
    let quota = tx.get_quota(7).await.unwrap().expect("quota after restart");
    assert_eq!(quota.used_bytes, 4096 + 1024);
    assert_eq!(quota.used_inodes, 4);
    assert_eq!(quota.quota_limit_bytes, 10_000_000);
    let kept_idx = tx.get_secondary_index(kept_fid).await.unwrap().unwrap();
    assert_eq!(kept_idx.pid, work_id);
    let moved_idx = tx.get_secondary_index(moved_fid).await.unwrap().unwrap();
    assert_eq!(moved_idx.pid, archive_id);
    assert_eq!(moved_idx.name, "stored.bin");

    tx.unlink(archive_id, "stored.bin").await.unwrap();
    cluster.run_maintenance_tick().await.unwrap();
    assert!(tx.lookup(archive_id, "stored.bin").await.unwrap().is_none());
    assert_eq!(tx.dirstat(archive_id).await.unwrap().num_children, 0);
    tx.rmdir(ROOT_INODE_ID, "archive").await.unwrap();
    assert!(tx.lookup(ROOT_INODE_ID, "archive").await.unwrap().is_none());
    cluster.run_maintenance_tick().await.unwrap();
    let quota = tx.get_quota(7).await.unwrap().unwrap();
    // `work` + `kept.bin` remain. `archive` and `stored.bin` are gone.
    assert_eq!(quota.used_bytes, 4096);
    assert_eq!(quota.used_inodes, 2);

    cluster.shutdown().await.unwrap();
    drop(cluster);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_single_node_reopen_sees_mkdir_and_listdir() {
    let dir = tempdir().unwrap();
    let cfg = persistent_config(dir.path(), 4 * 1024 * 1024 * 1024);
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
        .await
        .unwrap();
    let nested = cluster
        .mkdir_auto_id(ROOT_INODE_ID, "nested", 4, 0o755)
        .await
        .unwrap();
    let nested_id = nested.attr.inode_id;
    cluster
        .create_file_auto_id(nested_id, "a.dat", 4, 0o644, 8)
        .await
        .unwrap();
    cluster
        .create_file_auto_id(nested_id, "b.dat", 4, 0o644, 8)
        .await
        .unwrap();
    cluster.shutdown().await.unwrap();
    drop(cluster);

    let cluster = Arc::new(
        PersistentMetaDbCluster::open_and_serve(cfg)
            .await
            .unwrap()
            .0,
    );
    let (entries, cursor) = cluster
        .tx_router()
        .listdir_paginated(nested_id, None, None, 1)
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].key.name, "a.dat");
    let next = cursor.expect("first page is full");
    let (rest, _) = cluster
        .tx_router()
        .listdir_paginated(nested_id, Some(&next), None, 10)
        .await
        .unwrap();
    assert_eq!(rest.len(), 1);
    assert_eq!(rest[0].key.name, "b.dat");
    cluster.shutdown().await.unwrap();
}
