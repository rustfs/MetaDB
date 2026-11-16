//! Unflushed restart of a real `PersistentMetaDbCluster` while creates are in flight.
//!
//! `shutdown_without_flush` stops Raft without checkpointing the memtable, so committed
//! entries that have not reached an LSM snapshot must be replayed from `.raftlog`.
//! Acknowledged creates have to be present after reopen. A create that was inside propose
//! when Raft stopped may or may not have committed; the listing must stay inside the set
//! of names clients attempted, and `dirstat` must match that listing.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use metadb_core::{FileId, InodeId, UserId, ROOT_INODE_ID};
use metadb_server::stress::persistent_config;
use metadb_server::PersistentMetaDbCluster;
use metadb_tx::DistributedTxRouter;
use tempfile::tempdir;

const FILE_BYTES: i64 = 128;
const FILE_UID: UserId = 42;
const FILE_MODE: u32 = 0o644;
const DIR_MODE: u32 = 0o755;
const WORKERS: usize = 4;
const MIN_ACKED: usize = 16;

#[derive(Clone, Copy)]
struct LiveFile {
    dir: InodeId,
    inode: InodeId,
    fid: FileId,
}

struct BurstReport {
    acked: HashMap<String, LiveFile>,
    attempted: HashSet<String>,
    crash_window_errors: u64,
    latencies_us: Vec<u64>,
}

struct LiveBurst {
    acked: Arc<Mutex<HashMap<String, LiveFile>>>,
    attempted: Arc<Mutex<HashSet<String>>>,
    in_flight: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    uncertain: Arc<AtomicBool>,
    pre_crash_error: Arc<Mutex<Option<String>>>,
    crash_errors: Arc<AtomicU64>,
    latencies: Arc<Mutex<Vec<u64>>>,
    joins: Vec<tokio::task::JoinHandle<()>>,
}

impl LiveBurst {
    fn start(cluster: Arc<PersistentMetaDbCluster>, pid: InodeId) -> Self {
        let acked = Arc::new(Mutex::new(HashMap::new()));
        let attempted = Arc::new(Mutex::new(HashSet::new()));
        let in_flight = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let uncertain = Arc::new(AtomicBool::new(false));
        let pre_crash_error = Arc::new(Mutex::new(None));
        let crash_errors = Arc::new(AtomicU64::new(0));
        let latencies = Arc::new(Mutex::new(Vec::new()));
        let mut joins = Vec::with_capacity(WORKERS);
        for tid in 0..WORKERS {
            let cluster = cluster.clone();
            let acked = acked.clone();
            let attempted = attempted.clone();
            let in_flight = in_flight.clone();
            let stop = stop.clone();
            let uncertain = uncertain.clone();
            let pre_crash_error = pre_crash_error.clone();
            let crash_errors = crash_errors.clone();
            let latencies = latencies.clone();
            joins.push(tokio::spawn(async move {
                let mut seq = 0u64;
                while seq < 24 {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    seq += 1;
                    let name = format!("c{tid:02}_{seq:04}.dat");
                    attempted.lock().unwrap().insert(name.clone());
                    in_flight.fetch_add(1, Ordering::Relaxed);
                    let started = Instant::now();
                    let result = tokio::time::timeout(
                        Duration::from_secs(5),
                        cluster.create_file_auto_id(pid, &name, FILE_UID, FILE_MODE, FILE_BYTES),
                    )
                    .await;
                    in_flight.fetch_sub(1, Ordering::Relaxed);
                    latencies
                        .lock()
                        .unwrap()
                        .push(started.elapsed().as_micros() as u64);
                    match result {
                        Ok(Ok(entry)) => {
                            let fid = entry.attr.fid.expect("file create returns a file id");
                            acked.lock().unwrap().insert(
                                name,
                                LiveFile {
                                    dir: pid,
                                    inode: entry.attr.inode_id,
                                    fid,
                                },
                            );
                        }
                        Ok(Err(err)) => note_failure(
                            &uncertain,
                            &pre_crash_error,
                            &crash_errors,
                            err.to_string(),
                        ),
                        Err(_) => note_failure(
                            &uncertain,
                            &pre_crash_error,
                            &crash_errors,
                            format!("create {name} timed out"),
                        ),
                    }
                }
            }));
        }
        Self {
            acked,
            attempted,
            in_flight,
            stop,
            uncertain,
            pre_crash_error,
            crash_errors,
            latencies,
            joins,
        }
    }

    async fn wait_until_overlapping(&self) {
        let started = Instant::now();
        loop {
            let n = self.acked.lock().unwrap().len();
            let busy = self.in_flight.load(Ordering::Relaxed);
            if n >= MIN_ACKED && busy >= 1 {
                return;
            }
            if started.elapsed() > Duration::from_secs(20) {
                panic!("create load did not overlap a propose (acked={n} in_flight={busy})");
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }

    fn begin_shutdown(&self) {
        self.uncertain.store(true, Ordering::Relaxed);
        self.stop.store(true, Ordering::Relaxed);
    }

    async fn finish(self) -> BurstReport {
        for join in self.joins {
            join.await.expect("create worker panicked");
        }
        if let Some(err) = self.pre_crash_error.lock().unwrap().clone() {
            panic!("create failed before the injected stop: {err}");
        }
        BurstReport {
            acked: self.acked.lock().unwrap().clone(),
            attempted: self.attempted.lock().unwrap().clone(),
            crash_window_errors: self.crash_errors.load(Ordering::Relaxed),
            latencies_us: self.latencies.lock().unwrap().clone(),
        }
    }
}

fn note_failure(
    uncertain: &AtomicBool,
    pre_crash_error: &Mutex<Option<String>>,
    crash_errors: &AtomicU64,
    message: String,
) {
    if uncertain.load(Ordering::Relaxed) {
        crash_errors.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let mut guard = pre_crash_error.lock().unwrap();
    if guard.is_none() {
        *guard = Some(message);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn unflushed_restart_under_create_load() {
    let dir = tempdir().unwrap();
    let cfg = persistent_config(dir.path(), 4 * 1024 * 1024 * 1024);
    let started = Instant::now();

    let recovered = {
        let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
            .await
            .unwrap();
        let hot = cluster
            .mkdir_auto_id(ROOT_INODE_ID, "hot", FILE_UID, DIR_MODE)
            .await
            .unwrap();
        let archive = cluster
            .mkdir_auto_id(ROOT_INODE_ID, "archive", FILE_UID, DIR_MODE)
            .await
            .unwrap();
        let hot_id = hot.attr.inode_id;
        let archive_id = archive.attr.inode_id;
        let burst = LiveBurst::start(cluster.clone(), hot_id);
        burst.wait_until_overlapping().await;
        let in_flight = burst.in_flight();
        burst.begin_shutdown();
        tokio::time::timeout(Duration::from_secs(15), cluster.shutdown_without_flush())
            .await
            .expect("shutdown_without_flush timed out while creates were in flight")
            .unwrap();
        // Join workers before reopen so their `ShardRaftGroup` clones drop the LSM lock.
        let report = burst.finish().await;
        drop(cluster);
        let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg.clone())
            .await
            .unwrap();
        let listing = assert_acked_subset(&cluster, hot_id, &report.acked, &report.attempted).await;
        let archive = list_names(cluster.tx_router(), archive_id).await;
        assert!(
            archive.is_empty(),
            "archive must stay empty across the create crash"
        );
        drain_aux(&cluster).await;
        assert_quota(&cluster, 2 + listing as i64, listing as i64 * FILE_BYTES).await;
        let (sample_name, sample) = report.acked.iter().next().expect("acked create");
        assert_index(&cluster, sample.fid, hot_id, sample_name).await;
        emit(
            "single-node-inflight-create",
            started.elapsed(),
            report.acked.len(),
            report.attempted.len(),
            listing,
            report.crash_window_errors,
            in_flight,
            &report.latencies_us,
        );
        (cluster, hot_id, archive_id)
    };

    let (cluster, hot_id, archive_id) = recovered;
    let tail_started = Instant::now();
    let mut model = rebuild_model(&cluster, hot_id).await;
    assert!(
        model.len() >= MIN_ACKED,
        "reopen lost acknowledged creates, listing={}",
        model.len()
    );
    let phase1_inodes: HashSet<InodeId> = model.values().map(|file| file.inode).collect();
    let unlinked = apply_joined_tail(
        cluster.clone(),
        hot_id,
        archive_id,
        &mut model,
        &phase1_inodes,
    )
    .await;
    let tail_ops = model.len() + unlinked.len();
    tokio::time::timeout(Duration::from_secs(15), cluster.shutdown_without_flush())
        .await
        .expect("second shutdown_without_flush timed out")
        .unwrap();
    drop(cluster);

    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let hot_listing = expect_dir(&cluster, hot_id, &model).await;
    let archive_listing = expect_dir(&cluster, archive_id, &model).await;
    drain_aux(&cluster).await;
    let files = model.len() as i64;
    assert_quota(&cluster, 2 + files, files * FILE_BYTES).await;
    for fid in &unlinked {
        let found = cluster.tx_router().get_secondary_index(*fid).await.unwrap();
        assert!(
            found.is_none(),
            "unlinked fid {fid} still has a secondary index"
        );
    }
    let renamed = model
        .iter()
        .find(|(_, file)| file.dir == archive_id)
        .expect("renamed file");
    assert_index(&cluster, renamed.1.fid, archive_id, renamed.0).await;
    let kept = model
        .iter()
        .find(|(_, file)| file.dir == hot_id)
        .expect("file kept in hot");
    assert_index(&cluster, kept.1.fid, hot_id, kept.0).await;
    emit(
        "single-node-rename-unlink-tail",
        tail_started.elapsed(),
        model.len(),
        tail_ops,
        hot_listing + archive_listing,
        0,
        0,
        &[],
    );
    cluster.shutdown().await.unwrap();
}

enum TailEvent {
    Unlink { name: String, fid: FileId },
    Rename { from: String, to: String },
    Creates(Vec<(String, InodeId, FileId)>),
}

async fn apply_joined_tail(
    cluster: Arc<PersistentMetaDbCluster>,
    hot_id: InodeId,
    archive_id: InodeId,
    model: &mut HashMap<String, LiveFile>,
    phase1_inodes: &HashSet<InodeId>,
) -> Vec<FileId> {
    let mut names: Vec<String> = model.keys().cloned().collect();
    names.sort();
    assert!(
        names.len() >= 4,
        "need at least 4 recovered files to unlink and rename, got {}",
        names.len()
    );
    let unlink_names = names[..2].to_vec();
    let rename_names = names[2..4].to_vec();
    let mut joins = Vec::new();
    for name in unlink_names {
        let cluster = cluster.clone();
        joins.push(tokio::spawn(async move {
            let removed = cluster
                .tx_router()
                .unlink(hot_id, &name)
                .await
                .unwrap_or_else(|err| panic!("unlink {name} failed: {err}"));
            TailEvent::Unlink {
                name,
                fid: removed.fid.expect("unlinked file id"),
            }
        }));
    }
    for name in rename_names {
        let cluster = cluster.clone();
        let dest = format!("{name}.moved");
        joins.push(tokio::spawn(async move {
            cluster
                .tx_router()
                .rename(hot_id, &name, archive_id, &dest, false)
                .await
                .unwrap_or_else(|err| panic!("rename {name} failed: {err}"));
            TailEvent::Rename {
                from: name,
                to: dest,
            }
        }));
    }
    for tid in 0..WORKERS {
        let cluster = cluster.clone();
        joins.push(tokio::spawn(async move {
            let mut created = Vec::new();
            for seq in 0..2 {
                let name = format!("n{tid:02}_{seq:02}.dat");
                let entry = cluster
                    .create_file_auto_id(hot_id, &name, FILE_UID, FILE_MODE, FILE_BYTES)
                    .await
                    .unwrap_or_else(|err| panic!("post-recovery create {name} failed: {err}"));
                created.push((name, entry.attr.inode_id, entry.attr.fid.expect("file id")));
            }
            TailEvent::Creates(created)
        }));
    }

    let mut unlinked = Vec::new();
    for join in joins {
        match join.await.expect("tail worker panicked") {
            TailEvent::Unlink { name, fid } => {
                let file = model.remove(&name).expect("unlinked name was recovered");
                assert_eq!(file.fid, fid, "unlink {name} returned a different file id");
                unlinked.push(fid);
            }
            TailEvent::Rename { from, to } => {
                let mut file = model.remove(&from).expect("renamed name was recovered");
                file.dir = archive_id;
                model.insert(to, file);
            }
            TailEvent::Creates(created) => {
                for (name, inode, fid) in created {
                    assert!(
                        !phase1_inodes.contains(&inode),
                        "inode {inode} reused after restart"
                    );
                    assert!(
                        model.values().all(|file| file.inode != inode),
                        "inode {inode} reused inside the post-recovery tail"
                    );
                    model.insert(
                        name,
                        LiveFile {
                            dir: hot_id,
                            inode,
                            fid,
                        },
                    );
                }
            }
        }
    }
    unlinked
}

async fn rebuild_model(
    cluster: &PersistentMetaDbCluster,
    hot_id: InodeId,
) -> HashMap<String, LiveFile> {
    let names = list_names(cluster.tx_router(), hot_id).await;
    let mut model = HashMap::new();
    for name in names {
        let entry = cluster
            .tx_router()
            .lookup(hot_id, &name)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("listed {name} did not look up"));
        model.insert(
            name,
            LiveFile {
                dir: hot_id,
                inode: entry.attr.inode_id,
                fid: entry.attr.fid.expect("file id"),
            },
        );
    }
    model
}

async fn assert_acked_subset(
    cluster: &PersistentMetaDbCluster,
    pid: InodeId,
    acked: &HashMap<String, LiveFile>,
    attempted: &HashSet<String>,
) -> usize {
    let tx = cluster.tx_router();
    let listing = list_names(tx, pid).await;
    let missing: Vec<_> = acked
        .keys()
        .filter(|name| !listing.contains(*name))
        .take(3)
        .cloned()
        .collect();
    assert!(
        missing.is_empty(),
        "acknowledged creates missing after restart: {missing:?} listing={} acked={}",
        listing.len(),
        acked.len()
    );
    let unexpected: Vec<_> = listing.difference(attempted).take(3).cloned().collect();
    assert!(
        unexpected.is_empty(),
        "listing contains names no client attempted: {unexpected:?}"
    );
    let stat = tx.dirstat(pid).await.unwrap();
    assert_eq!(stat.num_children, listing.len() as i64, "dirstat children");
    assert_eq!(
        stat.size_bytes,
        listing.len() as i64 * FILE_BYTES,
        "dirstat bytes"
    );
    for (name, file) in acked {
        let got = tx
            .lookup(pid, name)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("missing {name}"));
        assert_eq!(got.attr.inode_id, file.inode, "{name}");
        assert_eq!(got.attr.fid, Some(file.fid), "{name}");
        assert_eq!(got.attr.size_bytes, FILE_BYTES, "{name}");
    }
    listing.len()
}

async fn expect_dir(
    cluster: &PersistentMetaDbCluster,
    pid: InodeId,
    model: &HashMap<String, LiveFile>,
) -> usize {
    let tx = cluster.tx_router();
    let listing = list_names(tx, pid).await;
    let want: HashSet<String> = model
        .iter()
        .filter(|(_, file)| file.dir == pid)
        .map(|(name, _)| name.clone())
        .collect();
    assert_eq!(
        listing, want,
        "directory {pid} listing drifted across the second stop"
    );
    let stat = tx.dirstat(pid).await.unwrap();
    assert_eq!(stat.num_children, want.len() as i64);
    assert_eq!(stat.size_bytes, want.len() as i64 * FILE_BYTES);
    for name in &want {
        let file = &model[name];
        let got = tx.lookup(pid, name).await.unwrap().unwrap();
        assert_eq!(got.attr.inode_id, file.inode, "{name}");
        assert_eq!(got.attr.fid, Some(file.fid), "{name}");
        assert_eq!(got.attr.size_bytes, FILE_BYTES, "{name}");
    }
    listing.len()
}

async fn assert_quota(cluster: &PersistentMetaDbCluster, inodes: i64, bytes: i64) {
    let quota = cluster
        .tx_router()
        .get_quota(FILE_UID)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("quota for uid {FILE_UID} missing"));
    assert_eq!(quota.used_inodes, inodes, "quota inodes");
    assert_eq!(quota.used_bytes, bytes, "quota bytes");
}

async fn assert_index(cluster: &PersistentMetaDbCluster, fid: FileId, pid: InodeId, name: &str) {
    let idx = cluster
        .tx_router()
        .get_secondary_index(fid)
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("secondary index for fid {fid} missing"));
    assert_eq!(idx.pid, pid);
    assert_eq!(idx.name, name);
}

async fn drain_aux(cluster: &PersistentMetaDbCluster) {
    for _ in 0..8 {
        cluster.run_maintenance_tick().await.unwrap();
    }
}

async fn list_names(tx: &DistributedTxRouter, pid: InodeId) -> HashSet<String> {
    let mut names = HashSet::new();
    let mut cursor = None;
    loop {
        let (page, next) = tx
            .listdir_paginated(pid, cursor.as_deref(), None, 256)
            .await
            .unwrap();
        if page.is_empty() {
            break;
        }
        for entry in page {
            names.insert(entry.key.name);
        }
        match next {
            Some(next) => cursor = Some(next),
            None => break,
        }
    }
    names
}

#[allow(clippy::too_many_arguments)]
fn emit(
    profile: &str,
    elapsed: Duration,
    acked: usize,
    attempted: usize,
    listing: usize,
    crash_window_errors: u64,
    in_flight: usize,
    samples: &[u64],
) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    println!(
        "metadb failure profile={profile} acked={acked} attempted={attempted} listing={listing} crash_window_errors={crash_window_errors} in_flight_at_stop={in_flight} elapsed_ms={} latency_us p50={} p99={} max={}",
        elapsed.as_millis(),
        percentile(&sorted, 50),
        percentile(&sorted, 99),
        sorted.last().copied().unwrap_or(0),
    );
}

fn percentile(sorted: &[u64], pct: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() - 1) as u64 * pct / 100) as usize;
    sorted[index]
}
