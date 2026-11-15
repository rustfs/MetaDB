//! In-process mixed metadata stress harness.
//!
//! Drives a real [`PersistentMetaDbCluster`](crate::PersistentMetaDbCluster) (OpenRaft +
//! WAL-less LSM on a `data_dir`). The smoke profile is bounded for CI. The local profile
//! reads environment variables so a longer run does not require code changes.
//!
//! | Variable | Local default | Meaning |
//! | :--- | :--- | :--- |
//! | `METADB_STRESS_DURATION_SECS` | 30 | Wall-clock duration of the mixed loop |
//! | `METADB_STRESS_CONCURRENCY` | 8 | Concurrent async clients |
//! | `METADB_STRESS_KEYSPACE` | 16 | Number of directories the clients share |
//! | `METADB_STRESS_SEED` | 1 | Per-client op-mix seed |
//!
//! The smoke profile does not read these variables. `cargo test --workspace` stays bounded
//! even if a developer has exported a long local duration.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use metadb_core::{InodeId, UserId, ROOT_INODE_ID};

use crate::{PersistentMetaDbCluster, ServerTomlConfig};

const FILE_BYTES: i64 = 128;
const FILE_UID: UserId = 42;
const FILE_MODE: u32 = 0o644;
const DIR_MODE: u32 = 0o755;

/// Knobs for one harness run.
#[derive(Debug, Clone)]
pub struct StressProfile {
    pub name: String,
    pub duration: Duration,
    pub concurrency: usize,
    pub keyspace_dirs: usize,
    pub seed: u64,
}

impl StressProfile {
    /// CI / default profile. Ignores `METADB_STRESS_*` so the workspace suite cannot hang.
    pub fn smoke() -> Self {
        Self {
            name: "smoke".to_string(),
            duration: Duration::from_secs(5),
            concurrency: 4,
            keyspace_dirs: 4,
            seed: 0x5EED,
        }
    }

    /// Longer local profile. Environment variables override the defaults below.
    pub fn from_env() -> Self {
        let mut profile = Self {
            name: "local".to_string(),
            duration: Duration::from_secs(30),
            concurrency: 8,
            keyspace_dirs: 16,
            seed: 1,
        };
        if let Some(secs) = env_u64("METADB_STRESS_DURATION_SECS") {
            profile.duration = Duration::from_secs(secs.max(1));
        }
        if let Some(n) = env_u64("METADB_STRESS_CONCURRENCY") {
            profile.concurrency = (n as usize).max(1);
        }
        if let Some(n) = env_u64("METADB_STRESS_KEYSPACE") {
            profile.keyspace_dirs = (n as usize).max(1);
        }
        if let Some(n) = env_u64("METADB_STRESS_SEED") {
            profile.seed = n;
        }
        profile
    }
}

fn env_u64(key: &str) -> Option<u64> {
    std::env::var(key).ok().and_then(|raw| raw.parse().ok())
}

/// Printed metrics plus the correctness verdict.
#[derive(Debug, Clone)]
pub struct StressReport {
    pub profile: String,
    pub ops: u64,
    pub errors: u64,
    pub correctness_errors: u64,
    pub elapsed: Duration,
    pub p50_us: u64,
    pub p99_us: u64,
    pub max_us: u64,
    pub details: Vec<String>,
}

impl std::fmt::Display for StressReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let secs = self.elapsed.as_secs_f64().max(0.001);
        write!(
            f,
            "metadb stress profile={} ops={} errors={} correctness_errors={} elapsed_ms={} ops_per_s={:.1} latency_us p50={} p99={} max={}",
            self.profile,
            self.ops,
            self.errors,
            self.correctness_errors,
            self.elapsed.as_millis(),
            self.ops as f64 / secs,
            self.p50_us,
            self.p99_us,
            self.max_us,
        )?;
        for line in &self.details {
            write!(f, "\n  {line}")?;
        }
        Ok(())
    }
}

/// TOML config for a hermetic all-in-one node. `grpc_listen_addr` is `127.0.0.1:0`.
pub fn persistent_config(data_dir: &Path, shard_split_threshold_bytes: usize) -> ServerTomlConfig {
    let toml_text = format!(
        r#"
[node]
node_id = 1
cell_id = 1
role = "all-in-one"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[storage]
memtable_size_bytes = 4194304
block_cache_bytes = 8388608
mvcc_retention_ms = 5000

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = {shard_split_threshold_bytes}
"#,
        data_dir.display()
    );
    ServerTomlConfig::from_toml_str(&toml_text).expect("stress persistent config is valid TOML")
}

/// Runs create / lookup / dirstat / listdir / rename / unlink against `cluster`, then checks
/// directory listings, `dirstat` totals, and quota against the successful op log.
pub async fn run_mixed(
    cluster: Arc<PersistentMetaDbCluster>,
    profile: &StressProfile,
) -> StressReport {
    let started = Instant::now();
    let mut dirs = Vec::with_capacity(profile.keyspace_dirs);
    for i in 0..profile.keyspace_dirs {
        let name = format!("stress_dir_{i:04}");
        let entry = cluster
            .mkdir_auto_id(ROOT_INODE_ID, &name, FILE_UID, DIR_MODE)
            .await
            .unwrap_or_else(|err| panic!("stress setup mkdir {name} failed: {err}"));
        dirs.push(entry.attr.inode_id);
    }

    let live: Arc<Mutex<HashMap<InodeId, HashSet<String>>>> = Arc::new(Mutex::new(
        dirs.iter().map(|pid| (*pid, HashSet::new())).collect(),
    ));
    let latencies = Arc::new(Mutex::new(Vec::<u64>::new()));
    let ops = Arc::new(AtomicU64::new(0));
    let errors = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + profile.duration;

    let mut workers = Vec::with_capacity(profile.concurrency);
    for tid in 0..profile.concurrency {
        let cluster = cluster.clone();
        let live = live.clone();
        let latencies = latencies.clone();
        let ops = ops.clone();
        let errors = errors.clone();
        let dirs = dirs.clone();
        let seed = profile.seed ^ (tid as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
        workers.push(tokio::spawn(async move {
            client_loop(
                cluster, live, latencies, ops, errors, dirs, tid, seed, deadline,
            )
            .await;
        }));
    }
    for worker in workers {
        worker
            .await
            .unwrap_or_else(|err| panic!("stress client panicked: {err}"));
    }

    for _ in 0..8 {
        if let Err(err) = cluster.run_maintenance_tick().await {
            return failed_report(
                profile,
                started.elapsed(),
                ops.load(Ordering::Relaxed),
                errors.load(Ordering::Relaxed) + 1,
                vec![format!("maintenance tick failed: {err}")],
                &latencies.lock().unwrap(),
            );
        }
    }

    let (correctness_errors, details) = check_consistency(&cluster, &dirs, &live).await;
    let samples = latencies.lock().unwrap().clone();
    finish_report(
        profile,
        started.elapsed(),
        ops.load(Ordering::Relaxed),
        errors.load(Ordering::Relaxed),
        correctness_errors,
        details,
        &samples,
    )
}

#[allow(clippy::too_many_arguments)]
async fn client_loop(
    cluster: Arc<PersistentMetaDbCluster>,
    live: Arc<Mutex<HashMap<InodeId, HashSet<String>>>>,
    latencies: Arc<Mutex<Vec<u64>>>,
    ops: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
    dirs: Vec<InodeId>,
    tid: usize,
    mut state: u64,
    deadline: Instant,
) {
    let tx = cluster.tx_router().clone();
    let mut seq = 0u64;
    let mut mine: HashMap<InodeId, Vec<String>> =
        dirs.iter().map(|pid| (*pid, Vec::new())).collect();

    while Instant::now() < deadline {
        state = mix(state);
        let dir_index = (state as usize) % dirs.len();
        let pid = dirs[dir_index];
        let kind = (state >> 8) % 20;
        let op_started = Instant::now();
        let ok = if kind < 10 {
            seq += 1;
            let name = format!("c{tid:02}_{seq:06}.dat");
            match cluster
                .create_file_auto_id(pid, &name, FILE_UID, FILE_MODE, FILE_BYTES)
                .await
            {
                Ok(_) => {
                    live.lock()
                        .unwrap()
                        .get_mut(&pid)
                        .unwrap()
                        .insert(name.clone());
                    mine.get_mut(&pid).unwrap().push(name);
                    true
                }
                Err(_) => false,
            }
        } else if kind < 14 {
            let target = mine.get(&pid).and_then(|names| names.last().cloned());
            match target {
                Some(name) => tx.lookup(pid, &name).await.map(|found| found.is_some()) == Ok(true),
                None => {
                    tx.lookup(pid, "missing.dat")
                        .await
                        .map(|found| found.is_none())
                        == Ok(true)
                }
            }
        } else if kind < 17 {
            tx.dirstat(pid).await.is_ok()
        } else if kind < 19 {
            tx.listdir_paginated(pid, None, None, 32).await.is_ok()
        } else if let Some(src_name) = mine.get_mut(&pid).and_then(|names| names.pop()) {
            let dst_pid = dirs[(dir_index + 1) % dirs.len()];
            seq += 1;
            let dst_name = format!("c{tid:02}_{seq:06}.renamed");
            match tx.rename(pid, &src_name, dst_pid, &dst_name, false).await {
                Ok(_) => {
                    let mut guard = live.lock().unwrap();
                    guard.get_mut(&pid).unwrap().remove(&src_name);
                    guard.get_mut(&dst_pid).unwrap().insert(dst_name.clone());
                    drop(guard);
                    mine.get_mut(&dst_pid).unwrap().push(dst_name);
                    true
                }
                Err(_) => {
                    mine.get_mut(&pid).unwrap().push(src_name);
                    false
                }
            }
        } else {
            tx.dirstat(pid).await.is_ok()
        };

        latencies
            .lock()
            .unwrap()
            .push(op_started.elapsed().as_micros() as u64);
        if ok {
            ops.fetch_add(1, Ordering::Relaxed);
        } else {
            errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn check_consistency(
    cluster: &PersistentMetaDbCluster,
    dirs: &[InodeId],
    live: &Mutex<HashMap<InodeId, HashSet<String>>>,
) -> (u64, Vec<String>) {
    let expected = live.lock().unwrap().clone();
    let tx = cluster.tx_router();
    let mut errors = 0u64;
    let mut details = Vec::new();
    let mut live_files = 0i64;

    for pid in dirs {
        let want = expected.get(pid).cloned().unwrap_or_default();
        live_files += want.len() as i64;
        let stat = match tx.dirstat(*pid).await {
            Ok(stat) => stat,
            Err(err) => {
                errors += 1;
                push_detail(&mut details, format!("dirstat pid={pid} failed: {err}"));
                continue;
            }
        };
        if stat.num_children != want.len() as i64
            || stat.size_bytes != want.len() as i64 * FILE_BYTES
        {
            errors += 1;
            push_detail(
                &mut details,
                format!(
                    "dirstat pid={pid} children={} size={} want_children={} want_size={}",
                    stat.num_children,
                    stat.size_bytes,
                    want.len(),
                    want.len() as i64 * FILE_BYTES
                ),
            );
        }
        match list_names(tx.as_ref(), *pid).await {
            Ok(got) => {
                if got != want {
                    errors += 1;
                    push_detail(
                        &mut details,
                        format!(
                            "listdir pid={pid} got={} want={} sample_missing={:?}",
                            got.len(),
                            want.len(),
                            want.difference(&got).take(3).collect::<Vec<_>>()
                        ),
                    );
                }
            }
            Err(err) => {
                errors += 1;
                push_detail(&mut details, format!("listdir pid={pid} failed: {err}"));
            }
        }
    }

    let want_inodes = dirs.len() as i64 + live_files;
    let want_bytes = live_files * FILE_BYTES;
    match tx.get_quota(FILE_UID).await {
        Ok(Some(quota)) => {
            if quota.used_inodes != want_inodes || quota.used_bytes != want_bytes {
                errors += 1;
                push_detail(
                    &mut details,
                    format!(
                        "quota uid={FILE_UID} inodes={} bytes={} want_inodes={want_inodes} want_bytes={want_bytes}",
                        quota.used_inodes, quota.used_bytes
                    ),
                );
            }
        }
        Ok(None) => {
            if want_inodes != 0 {
                errors += 1;
                push_detail(
                    &mut details,
                    format!("quota uid={FILE_UID} missing, want_inodes={want_inodes}"),
                );
            }
        }
        Err(err) => {
            errors += 1;
            push_detail(&mut details, format!("get_quota failed: {err}"));
        }
    }

    (errors, details)
}

async fn list_names(
    tx: &metadb_tx::DistributedTxRouter,
    pid: InodeId,
) -> metadb_core::Result<HashSet<String>> {
    let mut names = HashSet::new();
    let mut cursor: Option<String> = None;
    loop {
        let after = cursor.as_deref();
        let (page, next) = tx.listdir_paginated(pid, after, None, 256).await?;
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
    Ok(names)
}

fn finish_report(
    profile: &StressProfile,
    elapsed: Duration,
    ops: u64,
    errors: u64,
    correctness_errors: u64,
    details: Vec<String>,
    samples: &[u64],
) -> StressReport {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    StressReport {
        profile: profile.name.clone(),
        ops,
        errors,
        correctness_errors,
        elapsed,
        p50_us: percentile(&sorted, 50),
        p99_us: percentile(&sorted, 99),
        max_us: sorted.last().copied().unwrap_or(0),
        details,
    }
}

fn failed_report(
    profile: &StressProfile,
    elapsed: Duration,
    ops: u64,
    errors: u64,
    details: Vec<String>,
    samples: &[u64],
) -> StressReport {
    finish_report(profile, elapsed, ops, errors, 1, details, samples)
}

fn percentile(sorted: &[u64], pct: u64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() - 1) as u64 * pct / 100) as usize;
    sorted[idx]
}

fn push_detail(details: &mut Vec<String>, line: String) {
    if details.len() < 8 {
        details.push(line);
    }
}

fn mix(mut state: u64) -> u64 {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
}
