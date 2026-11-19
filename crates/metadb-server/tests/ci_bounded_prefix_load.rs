//! Reduced CI prefix-load fixture.
//!
//! Shape: 4 prefixes × 25 creates, then 50 overwrite/delete operations, concurrency 4.
//! `errors` and `correctness_errors` must both be 0.
//!
//! This is a reduced CI fixture only. The official 100 prefixes × 10,000 creates +
//! 100,000 overwrite/delete at concurrency 64 belongs on three VMs, not in CI. A green
//! run of this file is not that load and is not a production benchmark.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use metadb_core::ROOT_INODE_ID;
use metadb_server::stress::persistent_config;
use metadb_server::PersistentMetaDbCluster;
use metadb_tx::SetAttrPatch;
use tempfile::tempdir;

const PREFIXES: usize = 4;
const CREATES_PER_PREFIX: usize = 25;
const MUTATIONS: usize = 50;
const CONCURRENCY: usize = 4;
const CREATE_BYTES: i64 = 64;
const OVERWRITE_BYTES: i64 = 256;
const OPEN_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Copy)]
enum Mutation {
    Overwrite,
    Delete,
}

struct ExpectedFile {
    size_bytes: i64,
    mode: u32,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ci_bounded_prefix_load_has_no_errors() {
    assert_eq!(CONCURRENCY, PREFIXES);
    let dir = tempdir().unwrap();
    // Large split threshold: this fixture must not depend on a surprise shard split.
    let cfg = persistent_config(dir.path(), 4 * 1024 * 1024 * 1024);
    let (cluster, _) =
        tokio::time::timeout(OPEN_TIMEOUT, PersistentMetaDbCluster::open_and_serve(cfg))
            .await
            .expect("open_and_serve timed out")
            .expect("open_and_serve failed");
    let cluster = Arc::new(cluster);

    let mut prefixes = Vec::with_capacity(PREFIXES);
    for index in 0..PREFIXES {
        let entry = cluster
            .mkdir_auto_id(ROOT_INODE_ID, &format!("prefix_{index}"), 7, 0o755)
            .await
            .unwrap_or_else(|err| panic!("mkdir prefix_{index}: {err}"));
        prefixes.push(entry.attr.inode_id);
    }

    let errors = Arc::new(AtomicU64::new(0));
    let error_log = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let mut create_workers = Vec::with_capacity(CONCURRENCY);
    for pid in prefixes.iter().copied() {
        let cluster = Arc::clone(&cluster);
        let errors = Arc::clone(&errors);
        let error_log = Arc::clone(&error_log);
        create_workers.push(tokio::spawn(async move {
            for seq in 0..CREATES_PER_PREFIX {
                let name = file_name(seq);
                if let Err(err) = cluster
                    .create_file_auto_id(pid, &name, 7, 0o644, CREATE_BYTES)
                    .await
                {
                    note_error(
                        &errors,
                        &error_log,
                        format!("create {name} in {pid}: {err}"),
                    );
                }
            }
        }));
    }
    for worker in create_workers {
        worker.await.expect("create worker panicked");
    }

    // 50 mutations spread across the four prefixes. Each file is owned by one worker.
    let plan = mutation_plan();
    assert_eq!(plan.len(), MUTATIONS);
    let mut mutation_workers = Vec::with_capacity(CONCURRENCY);
    for (worker, pid) in prefixes.iter().copied().enumerate() {
        let cluster = Arc::clone(&cluster);
        let errors = Arc::clone(&errors);
        let error_log = Arc::clone(&error_log);
        let owned: Vec<(usize, Mutation)> = plan
            .iter()
            .copied()
            .filter(|(prefix, _, _)| *prefix == worker)
            .map(|(_, seq, kind)| (seq, kind))
            .collect();
        mutation_workers.push(tokio::spawn(async move {
            for (seq, kind) in owned {
                let name = file_name(seq);
                let result = match kind {
                    Mutation::Overwrite => cluster
                        .tx_router()
                        .setattr(
                            pid,
                            &name,
                            SetAttrPatch {
                                mode: Some(0o640),
                                uid: None,
                                size_bytes: Some(OVERWRITE_BYTES),
                                fid: None,
                                modify_time: None,
                            },
                        )
                        .await
                        .map(|_| ()),
                    Mutation::Delete => cluster.tx_router().unlink(pid, &name).await.map(|_| ()),
                };
                if let Err(err) = result {
                    note_error(
                        &errors,
                        &error_log,
                        format!("{:?} {name} in {pid}: {err}", kind_name(kind)),
                    );
                }
            }
        }));
    }
    for worker in mutation_workers {
        worker.await.expect("mutation worker panicked");
    }

    let mut expected: HashMap<(usize, usize), Option<ExpectedFile>> = HashMap::new();
    for prefix in 0..PREFIXES {
        for seq in 0..CREATES_PER_PREFIX {
            expected.insert(
                (prefix, seq),
                Some(ExpectedFile {
                    size_bytes: CREATE_BYTES,
                    mode: 0o644,
                }),
            );
        }
    }
    for (prefix, seq, kind) in plan {
        match kind {
            Mutation::Overwrite => {
                expected.insert(
                    (prefix, seq),
                    Some(ExpectedFile {
                        size_bytes: OVERWRITE_BYTES,
                        mode: 0o640,
                    }),
                );
            }
            Mutation::Delete => {
                expected.insert((prefix, seq), None);
            }
        }
    }

    let mut correctness_errors = 0u64;
    let mut details = Vec::new();
    for (prefix, pid) in prefixes.iter().copied().enumerate() {
        let mut children = 0i64;
        let mut bytes = 0i64;
        for seq in 0..CREATES_PER_PREFIX {
            let name = file_name(seq);
            let want = expected.get(&(prefix, seq)).and_then(|file| file.as_ref());
            match cluster.tx_router().lookup(pid, &name).await {
                Ok(Some(entry)) => match want {
                    Some(file) => {
                        children += 1;
                        bytes += file.size_bytes;
                        if entry.attr.size_bytes != file.size_bytes || entry.attr.mode != file.mode
                        {
                            correctness_errors += 1;
                            details.push(format!(
                                "prefix {prefix} {name} size {} mode {:o}, want size {} mode {:o}",
                                entry.attr.size_bytes, entry.attr.mode, file.size_bytes, file.mode
                            ));
                        }
                    }
                    None => {
                        correctness_errors += 1;
                        details.push(format!("prefix {prefix} {name} still exists after delete"));
                    }
                },
                Ok(None) => {
                    if want.is_some() {
                        correctness_errors += 1;
                        details.push(format!("prefix {prefix} {name} missing"));
                    }
                }
                Err(err) => {
                    correctness_errors += 1;
                    details.push(format!("lookup prefix {prefix} {name}: {err}"));
                }
            }
        }
        match cluster.tx_router().dirstat(pid).await {
            Ok(stat) => {
                if stat.num_children != children || stat.size_bytes != bytes {
                    correctness_errors += 1;
                    details.push(format!(
                        "prefix {prefix} dirstat children {} bytes {}, want children {children} bytes {bytes}",
                        stat.num_children, stat.size_bytes
                    ));
                }
            }
            Err(err) => {
                correctness_errors += 1;
                details.push(format!("dirstat prefix {prefix}: {err}"));
            }
        }
    }

    let error_count = errors.load(Ordering::Relaxed);
    let logged = error_log.lock().unwrap().clone();
    cluster.shutdown().await.expect("shutdown");
    assert_eq!(
        error_count, 0,
        "errors={error_count} correctness_errors={correctness_errors} log={logged:?} details={details:?}"
    );
    assert_eq!(
        correctness_errors, 0,
        "errors={error_count} correctness_errors={correctness_errors} details={details:?}"
    );
}

fn file_name(seq: usize) -> String {
    format!("f{seq:02}.dat")
}

fn kind_name(kind: Mutation) -> &'static str {
    match kind {
        Mutation::Overwrite => "overwrite",
        Mutation::Delete => "delete",
    }
}

fn note_error(errors: &AtomicU64, log: &std::sync::Mutex<Vec<String>>, message: String) {
    errors.fetch_add(1, Ordering::Relaxed);
    log.lock().unwrap().push(message);
}

/// 13 + 13 + 12 + 12 = 50 overwrite/delete operations, one file each.
fn mutation_plan() -> Vec<(usize, usize, Mutation)> {
    let mut plan = Vec::with_capacity(MUTATIONS);
    for prefix in 0..PREFIXES {
        let overwrite = if prefix < 2 { 7 } else { 6 };
        let delete = 6;
        for seq in 0..overwrite {
            plan.push((prefix, seq, Mutation::Overwrite));
        }
        for seq in overwrite..(overwrite + delete) {
            plan.push((prefix, seq, Mutation::Delete));
        }
    }
    assert_eq!(plan.len(), MUTATIONS);
    plan
}
