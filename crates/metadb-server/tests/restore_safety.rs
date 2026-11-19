//! Restore checks that use the public backup API.
//!
//! A real artifact comes from `PersistentMetaDbCluster::create_backup`. Unsafe
//! manifest paths are written by editing that artifact's path field. `backup.rs`
//! stays unchanged, including the visibility of its manifest helpers. These tests
//! only check that restore refuses the case and does not write the destination.

use std::fs;
use std::path::{Path, PathBuf};

use metadb_core::{append_crc32c_le, verify_crc32c_le, ROOT_INODE_ID};
use metadb_raft::backup::{self, BACKUP_MANIFEST_FILE, BACKUP_TREE_DIR};
use metadb_server::{PersistentMetaDbCluster, ServerTomlConfig};

async fn sample_artifact(root: &Path) -> PathBuf {
    let data = root.join("data");
    let artifact = root.join("artifact");
    let mut cfg = ServerTomlConfig::default();
    cfg.node.data_dir = data;
    cfg.node.grpc_listen_addr = "127.0.0.1:0".into();
    cfg.security.shared_secret = "test-secret".into();
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    cluster
        .create_file_auto_id(ROOT_INODE_ID, "kept.dat", 1, 0o644, 8)
        .await
        .unwrap();
    cluster
        .acquire_backup_hold(std::time::Duration::from_secs(120))
        .unwrap();
    cluster.create_backup(&artifact).unwrap();
    cluster.shutdown().await.unwrap();
    artifact
}

fn copy_dir(source: &Path, dest: &Path) {
    fs::create_dir_all(dest).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = dest.join(entry.file_name());
        if entry.path().is_dir() {
            copy_dir(&entry.path(), &to);
        } else {
            fs::copy(entry.path(), &to).unwrap();
        }
    }
}

fn tree_paths(artifact: &Path) -> Vec<String> {
    let tree = artifact.join(BACKUP_TREE_DIR);
    let mut out = Vec::new();
    let mut stack = vec![tree.clone()];
    while let Some(dir) = stack.pop() {
        for entry in fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let relative = path.strip_prefix(&tree).unwrap();
            let mut text = String::new();
            for (index, component) in relative.components().enumerate() {
                if index > 0 {
                    text.push('/');
                }
                text.push_str(component.as_os_str().to_str().expect("utf-8 backup path"));
            }
            out.push(text);
        }
    }
    out.sort();
    out
}

fn path_field_start(payload: &[u8], path: &str) -> usize {
    let bytes = path.as_bytes();
    let len_prefix = (bytes.len() as u64).to_le_bytes();
    let mut from = 0;
    while from + bytes.len() <= payload.len() {
        let Some(rel) = payload[from..]
            .windows(bytes.len())
            .position(|window| window == bytes)
        else {
            break;
        };
        let start = from + rel;
        if start >= 8 && payload[start - 8..start] == len_prefix {
            return start;
        }
        from = start + 1;
    }
    panic!("manifest has no length-prefixed path {path}");
}

fn replace_manifest_path(artifact: &Path, from: &str, to: &str) {
    let manifest = artifact.join(BACKUP_MANIFEST_FILE);
    let bytes = fs::read(&manifest).unwrap();
    let mut payload = verify_crc32c_le(&bytes, "backup record").unwrap().to_vec();
    let start = path_field_start(&payload, from);
    let mut replacement = Vec::with_capacity(8 + to.len());
    replacement.extend_from_slice(&(to.len() as u64).to_le_bytes());
    replacement.extend_from_slice(to.as_bytes());
    payload.splice(start - 8..start + from.len(), replacement);
    append_crc32c_le(&mut payload);
    fs::write(manifest, payload).unwrap();
}

fn assert_directory_empty(dir: &Path) {
    let names: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(names.is_empty(), "{} was written: {names:?}", dir.display());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_rejects_non_empty_destination() {
    let root = tempfile::tempdir().unwrap();
    let artifact = sample_artifact(root.path()).await;
    let dest = root.path().join("dest");
    fs::create_dir_all(&dest).unwrap();
    let existing = dest.join("KEEP");
    fs::write(&existing, b"live-data").unwrap();

    let error = backup::restore_backup(&artifact, &dest, &metadb_server::check_backup_tree)
        .expect_err("restore into a non-empty destination must fail");
    assert!(
        error.to_string().contains("is not empty"),
        "non-empty destination error was: {error}"
    );
    assert_eq!(fs::read(&existing).unwrap(), b"live-data");
    let mut names: Vec<_> = fs::read_dir(&dest)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    names.sort();
    assert_eq!(names, vec![std::ffi::OsString::from("KEEP")]);
    assert!(!root.path().join(".dest.partial").exists());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restore_rejects_unsafe_manifest_paths() {
    let root = tempfile::tempdir().unwrap();
    let artifact = sample_artifact(root.path()).await;
    let source_path = tree_paths(&artifact)
        .into_iter()
        .find(|path| path.starts_with("be/shards/"))
        .expect("backup artifact contains a shard path");
    let absolute = root.path().join("absolute-probe");
    let absolute = absolute.to_str().expect("temp path is utf-8");
    let cases = [
        ("dotdot", "../outside"),
        ("absolute", absolute),
        ("backslash", r"windows\style"),
        ("dot-component", "ok/./nested"),
        ("dotdot-component", "ok/../up"),
    ];

    for (name, unsafe_path) in cases {
        let case_artifact = root.path().join(format!("artifact-{name}"));
        copy_dir(&artifact, &case_artifact);
        replace_manifest_path(&case_artifact, &source_path, unsafe_path);
        let scratch = root.path().join(format!("scratch-{name}"));
        fs::create_dir_all(&scratch).unwrap();
        let dest = scratch.join("dest");
        let error =
            backup::restore_backup(&case_artifact, &dest, &metadb_server::check_backup_tree)
                .expect_err("unsafe manifest path must be rejected");
        let message = error.to_string();
        assert!(
            message.contains("is not a relative backup path"),
            "unsafe path {unsafe_path} was not rejected: {message}"
        );
        assert!(
            !dest.exists(),
            "rejected restore created {}",
            dest.display()
        );
        assert_directory_empty(&scratch);
        if Path::new(unsafe_path).is_absolute() {
            assert!(
                !Path::new(unsafe_path).exists(),
                "rejected restore wrote {}",
                unsafe_path
            );
        }
    }
}
