//! Operator backup hold, backup, and restore.
//!
//! These tests rehearse the single-node path: committed keys survive a hold that
//! blocks purge, a backup taken while maintenance would otherwise snapshot and
//! purge, and a restore into an empty directory. A green run is not a first-ship pass.

use std::path::Path;
use std::time::{Duration, Instant};

use metadb_core::{MetaKey, ROOT_INODE_ID};
use metadb_raft::backup::{self, BackupHoldRecord};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};
use metadb_tx::ShardDirectory;
use tempfile::tempdir;

fn persistent_cfg(dir: &Path) -> ServerTomlConfig {
    let toml_text = format!(
        r#"
[node]
node_id = 1
cell_id = 1
role = "all-in-one"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296
"#,
        dir.display()
    );
    ServerTomlConfig::from_toml_str(&toml_text).unwrap()
}

fn floor_for(hold: &BackupHoldRecord, shard_id: u64) -> &backup::ShardHoldFloor {
    hold.floors
        .iter()
        .find(|floor| floor.shard_id == shard_id)
        .unwrap_or_else(|| panic!("hold missing shard {shard_id}"))
}

fn snapshot_dir(data_dir: &Path, shard_id: u64, index: u64) -> std::path::PathBuf {
    data_dir
        .join("be/shards")
        .join(format!("{shard_id:020}"))
        .join("raft/snapshots")
        .join(format!("snap_{index:020}"))
}

async fn purged_index(cluster: &PersistentMetaDbCluster) -> (u64, u64) {
    let (_, group, _) = cluster
        .global_manager()
        .route_user_key(&MetaKey::dir_attr(ROOT_INODE_ID).encode())
        .unwrap();
    (group.shard_id(), group.log_store().last_purged_index())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hold_blocks_purge_until_release() {
    let dir = tempdir().unwrap();
    let cfg = persistent_cfg(dir.path());
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let hold = cluster
        .acquire_backup_hold(Duration::from_secs(120))
        .unwrap();
    for i in 0..24 {
        cluster
            .create_file_auto_id(ROOT_INODE_ID, &format!("held-{i:02}.dat"), 3, 0o644, 64)
            .await
            .unwrap();
    }
    let (shard_id, _) = purged_index(&cluster).await;
    let floor = floor_for(&hold, shard_id).max_purge_index;
    cluster.run_maintenance_tick().await.unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        let (_, purged) = purged_index(&cluster).await;
        assert!(
            purged <= floor,
            "hold floor {floor} did not block purge to {purged}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cluster.release_backup_hold().unwrap();
    cluster.run_background_maintenance_tick().await.unwrap();
    let purged = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (_, purged) = purged_index(&cluster).await;
            if purged > floor {
                break purged;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("release did not allow purge to move past the hold floor");
    assert!(purged > floor);
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_hold_allows_purge_after_restart() {
    let dir = tempdir().unwrap();
    let cfg = persistent_cfg(dir.path());
    let floor = {
        let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
            .await
            .unwrap();
        let hold = cluster
            .acquire_backup_hold(Duration::from_secs(120))
            .unwrap();
        for i in 0..16 {
            cluster
                .create_file_auto_id(ROOT_INODE_ID, &format!("exp-{i:02}.dat"), 3, 0o644, 32)
                .await
                .unwrap();
        }
        let (shard_id, _) = purged_index(&cluster).await;
        let floor = floor_for(&hold, shard_id).max_purge_index;
        cluster.shutdown_without_flush().await.unwrap();
        floor
    };
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    backup::testing_force_expire(dir.path()).unwrap();
    cluster.run_maintenance_tick().await.unwrap();
    let purged = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let (_, purged) = purged_index(&cluster).await;
            if purged > floor {
                break purged;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("expired hold still blocked purge");
    assert!(purged > floor);
    cluster.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn backup_restore_reads_committed_keys_and_rejects_a_truncated_artifact() {
    let dir = tempdir().unwrap();
    let backup_dir = tempdir().unwrap();
    let artifact = backup_dir.path().join("artifact");
    let cfg = persistent_cfg(dir.path());
    let parent = {
        let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
        let parent = cluster
            .mkdir_auto_id(ROOT_INODE_ID, "bucket", 9, 0o755)
            .await
            .unwrap();
        for name in ["a.dat", "b.dat", "c.dat"] {
            cluster
                .create_file_auto_id(parent.attr.inode_id, name, 9, 0o644, 128)
                .await
                .unwrap();
        }
        cluster.run_maintenance_tick().await.unwrap();
        let hold = cluster
            .acquire_backup_hold(Duration::from_secs(180))
            .unwrap();
        assert!(
            hold.floors
                .iter()
                .any(|floor| floor.retain_snapshots_through > 0),
            "maintenance should have published a snapshot before the hold: {hold:?}"
        );
        for name in ["d.dat", "e.dat"] {
            cluster
                .create_file_auto_id(parent.attr.inode_id, name, 9, 0o644, 256)
                .await
                .unwrap();
        }
        cluster.run_maintenance_tick().await.unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        while Instant::now() < deadline {
            let groups = cluster
                .global_manager()
                .all_cells()
                .into_iter()
                .flat_map(|cell| cell.local_shard_handles())
                .collect::<Vec<_>>();
            for floor in &hold.floors {
                let purged = groups
                    .iter()
                    .find(|(_, group, _)| group.shard_id() == floor.shard_id)
                    .map(|(_, group, _)| group.log_store().last_purged_index())
                    .expect("shard missing");
                assert!(
                    purged <= floor.max_purge_index,
                    "shard {} purged to {purged} past hold {}",
                    floor.shard_id,
                    floor.max_purge_index
                );
                if floor.retain_snapshots_through > 0 {
                    assert!(
                        snapshot_dir(dir.path(), floor.shard_id, floor.retain_snapshots_through)
                            .is_dir(),
                        "hold deleted snapshot {} on shard {}",
                        floor.retain_snapshots_through,
                        floor.shard_id
                    );
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        cluster
            .create_file_auto_id(parent.attr.inode_id, "tail.dat", 9, 0o644, 64)
            .await
            .unwrap();
        cluster.create_backup(&artifact).unwrap();
        cluster.shutdown().await.unwrap();
        parent.attr.inode_id
    };

    let broken = backup_dir.path().join("broken");
    copy_tree(&artifact, &broken);
    let mut removed = false;
    for entry in walk_files(&broken.join("tree")) {
        if entry
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with("segment_") && name.ends_with(".raftlog"))
        {
            std::fs::remove_file(&entry).unwrap();
            removed = true;
            break;
        }
    }
    assert!(removed, "backup artifact had no raft segment to delete");
    let rejected = tempdir().unwrap();
    let rejected_data = rejected.path().join("data");
    let error = backup::restore_backup(&broken, &rejected_data, &metadb_server::check_backup_tree)
        .unwrap_err();
    assert!(
        error.to_string().contains("backup restore rejected"),
        "incomplete restore error was: {error}"
    );
    assert!(!rejected_data.exists());

    let restored = tempdir().unwrap();
    let data = restored.path().join("data");
    set_files_readonly(&artifact, true);
    let restore_result =
        backup::restore_backup(&artifact, &data, &metadb_server::check_backup_tree);
    set_files_readonly(&artifact, false);
    restore_result
        .expect("read-only artifact must restore without opening its MANIFEST for append");
    let cfg = persistent_cfg(&data);
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    for name in ["a.dat", "b.dat", "c.dat", "d.dat", "e.dat", "tail.dat"] {
        let found = cluster
            .tx_router()
            .lookup(parent, name)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{name} missing after restore"));
        assert!(found.attr.size_bytes > 0);
    }
    let (entries, _) = cluster
        .tx_router()
        .listdir_paginated(parent, None, None, 20)
        .await
        .unwrap();
    let names: Vec<_> = entries.iter().map(|entry| entry.key.name.clone()).collect();
    for name in ["a.dat", "b.dat", "c.dat", "d.dat", "e.dat", "tail.dat"] {
        assert!(
            names.iter().any(|got| got == name),
            "list missing {name}: {names:?}"
        );
    }
    cluster.shutdown().await.unwrap();
}

fn set_files_readonly(root: &Path, readonly: bool) {
    let mut stack = vec![root.to_path_buf()];
    while let Some(current) = stack.pop() {
        if current.is_dir() {
            for entry in std::fs::read_dir(&current).unwrap() {
                stack.push(entry.unwrap().path());
            }
            continue;
        }
        let mut permissions = std::fs::metadata(&current).unwrap().permissions();
        permissions.set_readonly(readonly);
        std::fs::set_permissions(&current, permissions).unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn released_hold_purges_a_short_tail_without_a_new_snapshot() {
    let dir = tempdir().unwrap();
    let cfg = persistent_cfg(dir.path());
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    for i in 0..3 {
        cluster
            .create_file_auto_id(ROOT_INODE_ID, &format!("warm-{i}.dat"), 3, 0o644, 16)
            .await
            .unwrap();
    }
    cluster.run_maintenance_tick().await.unwrap();
    let hold = cluster
        .acquire_backup_hold(Duration::from_secs(120))
        .unwrap();
    cluster
        .create_file_auto_id(ROOT_INODE_ID, "after-hold.dat", 3, 0o644, 16)
        .await
        .unwrap();
    cluster.run_maintenance_tick().await.unwrap();
    let (shard_id, purged_while_held) = purged_index(&cluster).await;
    let floor = floor_for(&hold, shard_id).max_purge_index;
    assert!(purged_while_held <= floor);
    let (_, group, _) = cluster
        .global_manager()
        .route_user_key(&MetaKey::dir_attr(ROOT_INODE_ID).encode())
        .unwrap();
    let retained = group.log_store().retained_entry_count();
    assert!(retained > 0 && retained < 64, "retained {retained}");
    cluster.release_backup_hold().unwrap();
    cluster.run_background_maintenance_tick().await.unwrap();
    let (_, purged) = purged_index(&cluster).await;
    assert!(
        purged > floor,
        "idle tick left purge at {purged}, hold floor was {floor}"
    );
    cluster.shutdown().await.unwrap();
}

fn copy_tree(source: &Path, dest: &Path) {
    let status = std::process::Command::new("cp")
        .arg("-a")
        .arg(source)
        .arg(dest)
        .status()
        .unwrap();
    assert!(status.success());
}

fn walk_files(root: &Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
            } else {
                out.push(path);
            }
        }
    }
    out
}
