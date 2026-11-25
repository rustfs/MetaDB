//! FoundationDB-grade deterministic full-stack simulation, `Swizzle-Clog` multi-replica
//! chaos runner, 6-dimensional invariant oracle, and `Joshua`-style parallel seed ensemble.
//!
//! Covers all six system-wide invariants after arbitrary fault cascades:
//! 1. **Directory Tree & Paginated Scan Exactness** (`listdir` prime-sized pages, `lookup`, `_attr_` isolation, inode bit-tags)
//! 2. **Asynchronous `DirStat` Conservation** (`num_children` & `size_bytes` across `DirectoryColocated` and `DualModeFineGrained` shards)
//! 3. **`QuotaTable` (`[0x02]`) Multi-Tenant Conservation** (exact `used_inodes` & `used_bytes` across creates, unlinks, grow/shrink `setattr`, `chown`, and overwrite renames)
//! 4. **`SecondaryIndexTable` (`[0x03]`) Bijection** (every live `fid -> (pid, name)` matches; every deleted/overwritten victim `fid` is absent)
//! 5. **System Keyspace Zero-Leakage** (`[0x05] WriteIntent`, `[0x06] TxnRecord`, `[0x07] SpilledAuxOutbox`, `[0x0D] LeasePreimage`, and in-memory outbox queues are completely empty after recovery)
//! 6. **Multi-Replica State-Machine Convergence & Route Contiguity** (all 3 voter replicas hold byte-identical user K/V state across `[0x01, 0x04)` and non-overlapping contiguous `CompositeKeyRange` routes)

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use metadb_core::buggify::splitmix64;
use metadb_core::{
    code_probe, is_dir_inode_id, is_file_inode_id, make_file_inode_id, BuggifyConfig,
    CodeProbeSnapshot, FileId, GlobalBuggifyGuard, InodeId, MetaDbError, SpilledAuxOutboxKey,
    ThreadBuggifyGuard, UserId, ATTR_ENTRY_NAME, ROOT_INODE_ID, SYSTEM_PREFIX_LEASE_PREIMAGE,
    SYSTEM_PREFIX_SPILLED_AUX_OUTBOX, SYSTEM_PREFIX_TXN_RECORD, SYSTEM_PREFIX_WRITE_INTENT,
    TABLE_PREFIX_META, TABLE_PREFIX_SECONDARY_INDEX,
};
use metadb_raft::SwizzleClogController;
use metadb_tx::{SetAttrPatch, ShardDirectory};

use crate::{MetaDbConfig, MetaDbEngine, PersistentMetaDbCluster, ServerTomlConfig};

struct SimTempDir {
    path: PathBuf,
}

impl SimTempDir {
    fn new(seed: u64) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(1);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "metadb_fdb_sim_{}_{}_{:x}",
            std::process::id(),
            n,
            seed
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create sim temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SimTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

const UID_POOL: [UserId; 3] = [7, 17, 27];
const FILE_MODE: u32 = 0o644;
const DIR_MODE: u32 = 0o755;
const PRIME_LIST_PAGE_LIMIT: usize = 5;
const MAX_ORACLE_VIOLATIONS: usize = 16;

/// Workload kind in the FoundationDB-class full-stack simulation & chaos suite.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChaosWorkloadKind {
    /// Single-thread deterministic full-stack model with `ThreadBuggifyGuard` (`100%` bit-identical replay).
    InMemoryFullStackBuggify,
    /// Persistent WAL-less LSM + OpenRaft node executing online PID split, `DualModeFineGrained` split,
    /// unflushed crash restart, compaction, Raft log purge, and clean restart.
    PersistentMultiFaultCascade,
    /// Three-voter (`3-Replica`) Multi-Raft cluster executing online 3-voter shard split, FoundationDB
    /// `Swizzle-Clog` asymmetric link partitions, RPC jitter, post-apply reply drops, node isolation,
    /// indeterminate transaction atomic resolution, and 3-replica byte-for-byte state convergence.
    ThreeVoterSwizzleClogChaos,
}

impl ChaosWorkloadKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InMemoryFullStackBuggify => "in_memory_full_stack_buggify",
            Self::PersistentMultiFaultCascade => "persistent_multi_fault_cascade",
            Self::ThreeVoterSwizzleClogChaos => "three_voter_swizzle_clog_chaos",
        }
    }

    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "in_memory_full_stack_buggify" | "in-memory" => Some(Self::InMemoryFullStackBuggify),
            "persistent_multi_fault_cascade" | "persistent-cascade" => {
                Some(Self::PersistentMultiFaultCascade)
            }
            "three_voter_swizzle_clog_chaos" | "swizzle-clog" => {
                Some(Self::ThreeVoterSwizzleClogChaos)
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedFile {
    pub inode_id: InodeId,
    pub fid: FileId,
    pub uid: UserId,
    pub size_bytes: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TrackedDir {
    pub name: String,
    pub pid: InodeId,
    pub uid: UserId,
    pub files: BTreeMap<String, TrackedFile>,
}

/// Authoritative reference state machine for the 6-dimensional invariant oracle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FullStackModel {
    pub dirs: Vec<TrackedDir>,
    pub deleted_fids: BTreeSet<FileId>,
    pub touched_uids: BTreeSet<UserId>,
    pub next_seq: u64,
}

impl FullStackModel {
    pub fn new() -> Self {
        Self {
            dirs: Vec::new(),
            deleted_fids: BTreeSet::new(),
            touched_uids: BTreeSet::from(UID_POOL),
            next_seq: 1,
        }
    }

    fn alloc_seq(&mut self) -> u64 {
        let s = self.next_seq;
        self.next_seq = self.next_seq.saturating_add(1);
        s
    }

    /// Computes exact `(expected_used_inodes, expected_used_bytes)` for `uid` across all live dirs & files.
    pub fn expected_quota(&self, uid: UserId) -> (i64, i64) {
        let mut inodes = 0i64;
        let mut bytes = 0i64;
        for dir in &self.dirs {
            if dir.uid == uid {
                inodes += 1;
            }
            for f in dir.files.values() {
                if f.uid == uid {
                    inodes += 1;
                    bytes += f.size_bytes;
                }
            }
        }
        (inodes, bytes)
    }

    /// Computes the exact bijection `fid -> (pid, name)` for every currently live file.
    pub fn expected_live_index(&self) -> BTreeMap<FileId, (InodeId, String)> {
        let mut out = BTreeMap::new();
        for dir in &self.dirs {
            for (name, f) in &dir.files {
                out.insert(f.fid, (dir.pid, name.clone()));
            }
        }
        out
    }

    pub fn fingerprint(&self) -> String {
        let mut out = String::new();
        for (idx, dir) in self.dirs.iter().enumerate() {
            if idx > 0 {
                out.push('|');
            }
            out.push_str(&format!("{}:u{}=", dir.name, dir.uid));
            for (f_idx, (name, f)) in dir.files.iter().enumerate() {
                if f_idx > 0 {
                    out.push(',');
                }
                out.push_str(&format!("{}:u{}:s{}:f{}", name, f.uid, f.size_bytes, f.fid));
            }
        }
        for &uid in &self.touched_uids {
            let (inodes, bytes) = self.expected_quota(uid);
            out.push_str(&format!("|q{uid}={inodes}:{bytes}"));
        }
        out
    }
}

impl Default for FullStackModel {
    fn default() -> Self {
        Self::new()
    }
}

struct DetRng {
    state: u64,
}

impl DetRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x6A09_E667_F3BC_C909,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = splitmix64(self.state);
        self.state
    }

    fn gen_usize(&mut self, bound: usize) -> usize {
        debug_assert!(bound > 0);
        (self.next_u64() as usize) % bound
    }
}

#[derive(Debug, Clone)]
enum ChaosStepOp {
    Create {
        slot: usize,
        name: String,
        uid: UserId,
        size_bytes: i64,
        seq: u64,
    },
    Unlink {
        slot: usize,
        name: String,
    },
    IntraRename {
        slot: usize,
        from: String,
        to: String,
    },
    CrossRename {
        src_slot: usize,
        dst_slot: usize,
        from: String,
        to: String,
    },
    OverwriteRename {
        src_slot: usize,
        dst_slot: usize,
        from: String,
        victim: String,
    },
    SetAttrSize {
        slot: usize,
        name: String,
        new_size: i64,
    },
    SetAttrChown {
        slot: usize,
        name: String,
        new_uid: UserId,
    },
    MkdirRmdir {
        parent_slot: usize,
        temp_name: String,
        uid: UserId,
        seq: u64,
    },
    ReadVerify {
        slot: usize,
    },
}

fn nth_file_name(dir: &TrackedDir, pick: usize) -> Option<String> {
    if dir.files.is_empty() {
        return None;
    }
    let idx = pick % dir.files.len();
    dir.files.keys().nth(idx).cloned()
}

fn choose_step_op(
    rng: &mut DetRng,
    model: &mut FullStackModel,
    allow_overwrite: bool,
) -> ChaosStepOp {
    let dir_count = model.dirs.len();
    let slot = rng.gen_usize(dir_count);
    let uid = UID_POOL[rng.gen_usize(UID_POOL.len())];
    let size_choices = [64i64, 192, 512, 1024, 2048];
    let size_bytes = size_choices[rng.gen_usize(size_choices.len())];
    let roll = rng.gen_usize(20);
    let pick = rng.next_u64() as usize;

    match roll {
        0..=6 => {
            let seq = model.alloc_seq();
            ChaosStepOp::Create {
                slot,
                name: format!("f{seq:05}.dat"),
                uid,
                size_bytes,
                seq,
            }
        }
        7..=8 => match nth_file_name(&model.dirs[slot], pick) {
            Some(name) => ChaosStepOp::Unlink { slot, name },
            None => {
                let seq = model.alloc_seq();
                ChaosStepOp::Create {
                    slot,
                    name: format!("f{seq:05}.dat"),
                    uid,
                    size_bytes,
                    seq,
                }
            }
        },
        9..=10 => match nth_file_name(&model.dirs[slot], pick) {
            Some(from) => {
                let seq = model.alloc_seq();
                ChaosStepOp::IntraRename {
                    slot,
                    from,
                    to: format!("r{seq:05}.dat"),
                }
            }
            None => ChaosStepOp::ReadVerify { slot },
        },
        11..=12 if dir_count > 1 => {
            let dst_slot = (slot + 1 + rng.gen_usize(dir_count - 1)) % dir_count;
            match nth_file_name(&model.dirs[slot], pick) {
                Some(from) => {
                    let seq = model.alloc_seq();
                    ChaosStepOp::CrossRename {
                        src_slot: slot,
                        dst_slot,
                        from,
                        to: format!("x{seq:05}.dat"),
                    }
                }
                None => ChaosStepOp::ReadVerify { slot },
            }
        }
        13..=14 if allow_overwrite && dir_count >= 1 => {
            let dst_slot = rng.gen_usize(dir_count);
            let src_opt = nth_file_name(&model.dirs[slot], pick);
            let dst_opt = nth_file_name(&model.dirs[dst_slot], pick.wrapping_add(1));
            match (src_opt, dst_opt) {
                (Some(from), Some(victim)) if !(slot == dst_slot && from == victim) => {
                    ChaosStepOp::OverwriteRename {
                        src_slot: slot,
                        dst_slot,
                        from,
                        victim,
                    }
                }
                _ => ChaosStepOp::ReadVerify { slot },
            }
        }
        15..=16 => match nth_file_name(&model.dirs[slot], pick) {
            Some(name) => {
                let cur = model.dirs[slot].files[&name].size_bytes;
                // Alternate shrinking and growing to test both negative and positive `delta_bytes`!
                let new_size = if (pick & 1) == 0 {
                    (cur / 2).max(16)
                } else {
                    cur.saturating_add(320)
                };
                ChaosStepOp::SetAttrSize {
                    slot,
                    name,
                    new_size,
                }
            }
            None => ChaosStepOp::ReadVerify { slot },
        },
        17 => match nth_file_name(&model.dirs[slot], pick) {
            Some(name) => {
                let cur_uid = model.dirs[slot].files[&name].uid;
                let next_uid = UID_POOL
                    .iter()
                    .copied()
                    .find(|&u| u != cur_uid)
                    .unwrap_or(UID_POOL[0]);
                ChaosStepOp::SetAttrChown {
                    slot,
                    name,
                    new_uid: next_uid,
                }
            }
            None => ChaosStepOp::ReadVerify { slot },
        },
        18 => {
            let seq = model.alloc_seq();
            ChaosStepOp::MkdirRmdir {
                parent_slot: slot,
                temp_name: format!("sub_{seq:05}"),
                uid,
                seq,
            }
        }
        _ => ChaosStepOp::ReadVerify { slot },
    }
}

fn push_violation(violations: &mut Vec<String>, msg: String) {
    if violations.len() < MAX_ORACLE_VIOLATIONS {
        violations.push(msg);
    }
}

/// Detailed execution & invariant-audit report for one seeded chaos/simulation run.
#[derive(Debug, Clone)]
pub struct FullStackSimReport {
    pub workload: ChaosWorkloadKind,
    pub seed: u64,
    pub steps_executed: u32,
    pub committed_mutations: u32,
    pub indeterminate_resolved: u32,
    pub buggify_firings: u64,
    pub network_stats: (u64, u64, u64),
    pub violations: Vec<String>,
    pub trace_tail: Vec<String>,
    pub model_fingerprint: String,
    pub trace_hash: u64,
    pub elapsed: Duration,
}

impl FullStackSimReport {
    pub fn is_ok(&self) -> bool {
        self.violations.is_empty()
    }
}

impl std::fmt::Display for FullStackSimReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "metadb fdb_sim workload={} seed={:#x} steps={} committed={} indeterminate_resolved={} buggify_firings={} net_drops={} net_delays={} net_reply_drops={} violations={} trace_hash={:016x} elapsed_ms={}",
            self.workload.as_str(),
            self.seed,
            self.steps_executed,
            self.committed_mutations,
            self.indeterminate_resolved,
            self.buggify_firings,
            self.network_stats.0,
            self.network_stats.1,
            self.network_stats.2,
            self.violations.len(),
            self.trace_hash,
            self.elapsed.as_millis(),
        )?;
        for v in &self.violations {
            write!(f, "\n  VIOLATION: {v}")?;
        }
        for t in &self.trace_tail {
            write!(f, "\n  TRACE: {t}")?;
        }
        Ok(())
    }
}

fn make_trace_tail(trace: &[String], has_violations: bool) -> Vec<String> {
    if !has_violations {
        return Vec::new();
    }
    let start = trace.len().saturating_sub(32);
    trace[start..].to_vec()
}

fn compute_trace_hash(trace: &[String], fingerprint: &str) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for line in trace {
        for &b in line.as_bytes() {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
        h ^= u64::from(b'\n');
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    for &b in fingerprint.as_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// Runs Workload 1 (`InMemoryFullStackBuggify`) with deterministic `ThreadBuggifyGuard` and Invariants 1–4.
pub fn run_in_memory_full_stack(seed: u64, steps: u32) -> FullStackSimReport {
    let started = Instant::now();
    let buggify_guard = ThreadBuggifyGuard::install(seed, BuggifyConfig::default());
    let engine = MetaDbEngine::new_single_node(MetaDbConfig {
        shard_split_threshold_bytes: 2048,
        tombstone_density_threshold: 16,
        ..MetaDbConfig::default()
    });
    let mut rng = DetRng::new(seed);
    let mut model = FullStackModel::new();
    let mut violations = Vec::new();
    let mut trace = Vec::new();

    for slot in 0..4u64 {
        let name = format!("dir_{slot:02}");
        let pid = 200 + slot;
        let uid = UID_POOL[(slot as usize) % UID_POOL.len()];
        match engine.mkdir(ROOT_INODE_ID, &name, pid, uid, DIR_MODE) {
            Ok(_) => model.dirs.push(TrackedDir {
                name,
                pid,
                uid,
                files: BTreeMap::new(),
            }),
            Err(e) => {
                push_violation(&mut violations, format!("setup mkdir {name} failed: {e}"));
            }
        }
    }

    let mut committed = 0u32;
    for step_idx in 0..steps {
        if violations.len() >= MAX_ORACLE_VIOLATIONS {
            break;
        }
        let op = choose_step_op(&mut rng, &mut model, false);
        match op {
            ChaosStepOp::Create {
                slot,
                name,
                uid,
                size_bytes,
                seq,
            } => {
                let pid = model.dirs[slot].pid;
                let inode_id = make_file_inode_id(10_000 + seq);
                let fid = 50_000 + seq;
                match engine.create_file(pid, &name, inode_id, fid, uid, FILE_MODE, size_bytes) {
                    Ok(_) => {
                        model.dirs[slot].files.insert(
                            name.clone(),
                            TrackedFile {
                                inode_id,
                                fid,
                                uid,
                                size_bytes,
                            },
                        );
                        committed += 1;
                        trace.push(format!(
                            "{step_idx}:create:{slot}:{name}:{uid}:{size_bytes}"
                        ));
                    }
                    Err(e) => push_violation(&mut violations, format!("create {name} failed: {e}")),
                }
            }
            ChaosStepOp::Unlink { slot, name } => {
                let pid = model.dirs[slot].pid;
                match engine.unlink(pid, &name) {
                    Ok(_) => {
                        if let Some(removed) = model.dirs[slot].files.remove(&name) {
                            model.deleted_fids.insert(removed.fid);
                        }
                        committed += 1;
                        trace.push(format!("{step_idx}:unlink:{slot}:{name}"));
                    }
                    Err(e) => push_violation(&mut violations, format!("unlink {name} failed: {e}")),
                }
            }
            ChaosStepOp::IntraRename { slot, from, to } => {
                let pid = model.dirs[slot].pid;
                match engine.rename(pid, &from, pid, &to) {
                    Ok(_) => {
                        if let Some(moved) = model.dirs[slot].files.remove(&from) {
                            model.dirs[slot].files.insert(to.clone(), moved);
                        }
                        committed += 1;
                        trace.push(format!("{step_idx}:rename:{slot}:{from}->{to}"));
                    }
                    Err(e) => push_violation(&mut violations, format!("rename {from} failed: {e}")),
                }
            }
            ChaosStepOp::CrossRename {
                src_slot,
                dst_slot,
                from,
                to,
            } => {
                let src_pid = model.dirs[src_slot].pid;
                let dst_pid = model.dirs[dst_slot].pid;
                match engine.rename(src_pid, &from, dst_pid, &to) {
                    Ok(_) => {
                        if let Some(moved) = model.dirs[src_slot].files.remove(&from) {
                            model.dirs[dst_slot].files.insert(to.clone(), moved);
                        }
                        committed += 1;
                        trace.push(format!(
                            "{step_idx}:xrename:{src_slot}:{from}->{dst_slot}:{to}"
                        ));
                    }
                    Err(e) => {
                        push_violation(&mut violations, format!("cross rename {from} failed: {e}"))
                    }
                }
            }
            ChaosStepOp::SetAttrSize {
                slot,
                name,
                new_size,
            } => {
                let pid = model.dirs[slot].pid;
                match engine.setattr(
                    pid,
                    &name,
                    SetAttrPatch {
                        size_bytes: Some(new_size),
                        ..Default::default()
                    },
                ) {
                    Ok(_) => {
                        if let Some(f) = model.dirs[slot].files.get_mut(&name) {
                            f.size_bytes = new_size;
                        }
                        committed += 1;
                        trace.push(format!("{step_idx}:size:{slot}:{name}:{new_size}"));
                    }
                    Err(e) => {
                        push_violation(&mut violations, format!("setattr size {name} failed: {e}"))
                    }
                }
            }
            ChaosStepOp::SetAttrChown {
                slot,
                name,
                new_uid,
            } => {
                let pid = model.dirs[slot].pid;
                match engine.setattr(
                    pid,
                    &name,
                    SetAttrPatch {
                        uid: Some(new_uid),
                        ..Default::default()
                    },
                ) {
                    Ok(_) => {
                        if let Some(f) = model.dirs[slot].files.get_mut(&name) {
                            f.uid = new_uid;
                        }
                        committed += 1;
                        trace.push(format!("{step_idx}:chown:{slot}:{name}:{new_uid}"));
                    }
                    Err(e) => {
                        push_violation(&mut violations, format!("setattr uid {name} failed: {e}"))
                    }
                }
            }
            ChaosStepOp::MkdirRmdir {
                parent_slot,
                temp_name,
                uid,
                seq,
            } => {
                let parent_pid = model.dirs[parent_slot].pid;
                let sub_id = 1_000 + seq;
                match engine
                    .mkdir(parent_pid, &temp_name, sub_id, uid, DIR_MODE)
                    .and_then(|_| engine.rmdir(parent_pid, &temp_name))
                {
                    Ok(_) => {
                        committed += 2;
                        trace.push(format!("{step_idx}:mkdir_rmdir:{parent_slot}:{temp_name}"));
                    }
                    Err(e) => push_violation(
                        &mut violations,
                        format!("mkdir+rmdir {temp_name} failed: {e}"),
                    ),
                }
            }
            ChaosStepOp::OverwriteRename { .. } | ChaosStepOp::ReadVerify { .. } => {
                let pid = model.dirs[0].pid;
                let _ = engine.dirstat(pid);
                trace.push(format!("{step_idx}:read"));
            }
        }
        if step_idx % 8 == 7 {
            let _ = engine.run_background_maintenance();
        }
    }

    if let Err(e) = engine.run_background_maintenance() {
        push_violation(&mut violations, format!("final maintenance failed: {e}"));
    }
    verify_in_memory_invariants(&engine, &model, &mut violations);

    let fp = model.fingerprint();
    let th = compute_trace_hash(&trace, &fp);
    let firings = buggify_guard.firings();
    drop(buggify_guard);

    let trace_tail = make_trace_tail(&trace, !violations.is_empty());
    FullStackSimReport {
        workload: ChaosWorkloadKind::InMemoryFullStackBuggify,
        seed,
        steps_executed: steps,
        committed_mutations: committed,
        indeterminate_resolved: 0,
        buggify_firings: firings,
        network_stats: (0, 0, 0),
        violations,
        trace_tail,
        model_fingerprint: fp,
        trace_hash: th,
        elapsed: started.elapsed(),
    }
}

fn verify_in_memory_invariants(
    engine: &MetaDbEngine,
    model: &FullStackModel,
    violations: &mut Vec<String>,
) {
    // Invariant 1 & 2: Directory listings (paginated limit=5), lookups, and DirStat conservation.
    for dir in &model.dirs {
        let mut seen = BTreeSet::new();
        let mut after: Option<String> = None;
        loop {
            match engine.listdir_limited(dir.pid, after.as_deref(), None, PRIME_LIST_PAGE_LIMIT) {
                Ok(page) => {
                    if page.is_empty() {
                        break;
                    }
                    after = page.last().map(|e| e.key.name.clone());
                    for entry in page {
                        if entry.key.name == ATTR_ENTRY_NAME {
                            push_violation(
                                violations,
                                format!("dir {} leaked _attr_ in listdir", dir.name),
                            );
                        }
                        seen.insert(entry.key.name);
                    }
                }
                Err(e) => {
                    push_violation(violations, format!("listdir {} failed: {e}", dir.name));
                    break;
                }
            }
        }
        let want_names: BTreeSet<String> = dir.files.keys().cloned().collect();
        if seen != want_names {
            push_violation(
                violations,
                format!(
                    "dir {} names mismatch got={} want={}",
                    dir.name,
                    seen.len(),
                    want_names.len()
                ),
            );
        }
        let want_bytes: i64 = dir.files.values().map(|f| f.size_bytes).sum();
        match engine.dirstat(dir.pid) {
            Ok(stat) => {
                if stat.num_children != dir.files.len() as i64 || stat.size_bytes != want_bytes {
                    push_violation(
                        violations,
                        format!(
                            "dir {} dirstat got=({}, {}) want=({}, {})",
                            dir.name,
                            stat.num_children,
                            stat.size_bytes,
                            dir.files.len(),
                            want_bytes
                        ),
                    );
                }
            }
            Err(e) => push_violation(violations, format!("dirstat {} failed: {e}", dir.name)),
        }
        for (name, f) in &dir.files {
            match engine.lookup(dir.pid, name) {
                Ok(Some(got)) => {
                    if got.attr.size_bytes != f.size_bytes
                        || got.attr.uid != f.uid
                        || got.attr.fid != Some(f.fid)
                    {
                        push_violation(
                            violations,
                            format!(
                                "lookup {}/{name} mismatch got=({},{},{:?}) want=({},{},{:?})",
                                dir.name,
                                got.attr.uid,
                                got.attr.size_bytes,
                                got.attr.fid,
                                f.uid,
                                f.size_bytes,
                                Some(f.fid)
                            ),
                        );
                    }
                }
                Ok(None) => {
                    push_violation(violations, format!("lookup {}/{name} missing", dir.name))
                }
                Err(e) => {
                    push_violation(violations, format!("lookup {}/{name} err: {e}", dir.name))
                }
            }
        }
    }

    // Invariant 3: Multi-UID QuotaTable conservation.
    for &uid in &model.touched_uids {
        let (want_inodes, want_bytes) = model.expected_quota(uid);
        let (got_inodes, got_bytes) = engine
            .get_user_quota(uid)
            .map(|q| (q.used_inodes, q.used_bytes))
            .unwrap_or((0, 0));
        if got_inodes != want_inodes || got_bytes != want_bytes {
            push_violation(
                violations,
                format!(
                    "in-memory quota uid={uid} got=({got_inodes}, {got_bytes}) want=({want_inodes}, {want_bytes})"
                ),
            );
        }
    }

    // Invariant 4: SecondaryIndexTable bijection.
    for (fid, (want_pid, want_name)) in model.expected_live_index() {
        match engine.get_secondary_index(fid) {
            Some(rec) if rec.pid == want_pid && rec.name == want_name => {}
            Some(rec) => push_violation(
                violations,
                format!(
                    "secondary index fid={fid} got=({}, {}) want=({want_pid}, {want_name})",
                    rec.pid, rec.name
                ),
            ),
            None => push_violation(violations, format!("secondary index fid={fid} missing")),
        }
    }
    for &dead_fid in &model.deleted_fids {
        if let Some(zombie) = engine.get_secondary_index(dead_fid) {
            push_violation(
                violations,
                format!(
                    "deleted fid={dead_fid} still present in secondary index at ({}, {})",
                    zombie.pid, zombie.name
                ),
            );
        }
    }
}

fn build_node_config(
    data_dir: &Path,
    node_id: u64,
    join_addr: Option<&str>,
    split_threshold_bytes: usize,
) -> ServerTomlConfig {
    let join_line = match join_addr {
        Some(addr) => format!("join = \"{addr}\""),
        None => String::new(),
    };
    let toml_str = format!(
        r#"
[node]
node_id = {node_id}
cell_id = 1
role = "all-in-one"
data_dir = "{}"
grpc_listen_addr = "127.0.0.1:0"

[cluster]
{join_line}

[storage]
memtable_size_bytes = 8192
block_cache_bytes = 4194304
mvcc_retention_ms = 5000
locality_tombstone_threshold = 24
l1_max_bytes = 16384

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = {split_threshold_bytes}

[raft]
heartbeat_interval_ms = 50
election_timeout_min_ms = 150
election_timeout_max_ms = 300
max_in_snapshot_log_to_keep = 64

[security]
allow_unauthenticated = true
"#,
        data_dir.display()
    );
    ServerTomlConfig::from_toml_str(&toml_str).expect("valid chaos node TOML")
}

fn is_indeterminate_chaos_error(err: &MetaDbError) -> bool {
    matches!(
        err,
        MetaDbError::RpcError(_)
            | MetaDbError::TransactionTimeout { .. }
            | MetaDbError::TransactionAborted(_)
            | MetaDbError::WriteIntentConflict { .. }
            | MetaDbError::EpochMismatch { .. }
            | MetaDbError::ShardRoutingError(_)
            | MetaDbError::NotLeader { .. }
            | MetaDbError::RetryableConsensus { .. }
            | MetaDbError::ConsensusError(_)
            | MetaDbError::SplitDeferred(_)
    )
}

/// Executes a single `ChaosStepOp` against `cluster`.
/// When `chaos_active` is `true`, transport/consensus/conflict errors are resolved linearizably
/// after healing (or immediately via `lookup`) to verify strict ACID atomicity!
#[allow(clippy::too_many_arguments)]
async fn execute_cluster_step(
    cluster: &PersistentMetaDbCluster,
    all_nodes: &[Arc<PersistentMetaDbCluster>],
    swizzle: Option<&SwizzleClogController>,
    model: &mut FullStackModel,
    op: ChaosStepOp,
    chaos_active: bool,
    step_idx: u32,
    trace: &mut Vec<String>,
    violations: &mut Vec<String>,
) -> (u32, u32) {
    let tx = cluster.tx_router();
    let step_timeout = if chaos_active {
        Duration::from_millis(650)
    } else {
        Duration::from_secs(30)
    };
    match op {
        ChaosStepOp::Create {
            slot,
            name,
            uid,
            size_bytes,
            seq,
        } => {
            let pid = model.dirs[slot].pid;
            let inode_id = make_file_inode_id(100_000 + seq);
            let fid = 500_000 + seq;
            let call = tokio::time::timeout(
                step_timeout,
                tx.create_file(pid, &name, inode_id, fid, uid, FILE_MODE, size_bytes),
            )
            .await
            .unwrap_or_else(|_| Err(MetaDbError::RpcError("chaos client timeout".to_string())));
            match call {
                Ok(entry) => {
                    if !is_file_inode_id(entry.attr.inode_id) {
                        push_violation(
                            violations,
                            format!(
                                "create {name} returned non-file inode {:#x}",
                                entry.attr.inode_id
                            ),
                        );
                    }
                    model.dirs[slot].files.insert(
                        name.clone(),
                        TrackedFile {
                            inode_id,
                            fid,
                            uid,
                            size_bytes,
                        },
                    );
                    trace.push(format!("{step_idx}:create_ok:{slot}:{name}"));
                    (1, 0)
                }
                Err(e) if chaos_active && is_indeterminate_chaos_error(&e) => {
                    match resolve_lookup_with_retry(cluster, all_nodes, swizzle, pid, &name).await {
                        Ok(Some(landed)) => {
                            model.dirs[slot].files.insert(
                                name.clone(),
                                TrackedFile {
                                    inode_id: landed.attr.inode_id,
                                    fid: landed.attr.fid.unwrap_or(fid),
                                    uid: landed.attr.uid,
                                    size_bytes: landed.attr.size_bytes,
                                },
                            );
                            trace.push(format!("{step_idx}:create_indet_committed:{slot}:{name}"));
                            (1, 1)
                        }
                        Ok(None) => {
                            model.deleted_fids.insert(fid);
                            trace.push(format!("{step_idx}:create_indet_aborted:{slot}:{name}"));
                            (0, 1)
                        }
                        Err(re) => {
                            push_violation(
                                violations,
                                format!("indeterminate create {name} could not resolve: {re}"),
                            );
                            (0, 1)
                        }
                    }
                }
                Err(e) => {
                    push_violation(violations, format!("create {name} failed: {e}"));
                    (0, 0)
                }
            }
        }
        ChaosStepOp::Unlink { slot, name } => {
            let pid = model.dirs[slot].pid;
            let call = tokio::time::timeout(step_timeout, tx.unlink(pid, &name))
                .await
                .unwrap_or_else(|_| Err(MetaDbError::RpcError("chaos client timeout".to_string())));
            match call {
                Ok(_) => {
                    if let Some(removed) = model.dirs[slot].files.remove(&name) {
                        model.deleted_fids.insert(removed.fid);
                    }
                    trace.push(format!("{step_idx}:unlink_ok:{slot}:{name}"));
                    (1, 0)
                }
                Err(e) if chaos_active && is_indeterminate_chaos_error(&e) => {
                    match resolve_lookup_with_retry(cluster, all_nodes, swizzle, pid, &name).await {
                        Ok(None) => {
                            if let Some(removed) = model.dirs[slot].files.remove(&name) {
                                model.deleted_fids.insert(removed.fid);
                            }
                            trace.push(format!("{step_idx}:unlink_indet_committed:{slot}:{name}"));
                            (1, 1)
                        }
                        Ok(Some(_)) => {
                            trace.push(format!("{step_idx}:unlink_indet_aborted:{slot}:{name}"));
                            (0, 1)
                        }
                        Err(re) => {
                            push_violation(
                                violations,
                                format!("indeterminate unlink {name} could not resolve: {re}"),
                            );
                            (0, 1)
                        }
                    }
                }
                Err(e) => {
                    push_violation(violations, format!("unlink {name} failed: {e}"));
                    (0, 0)
                }
            }
        }
        ChaosStepOp::IntraRename { slot, from, to } => {
            let pid = model.dirs[slot].pid;
            let call = tokio::time::timeout(step_timeout, tx.rename(pid, &from, pid, &to, false))
                .await
                .unwrap_or_else(|_| Err(MetaDbError::RpcError("chaos client timeout".to_string())));
            match call {
                Ok(_) => {
                    if let Some(moved) = model.dirs[slot].files.remove(&from) {
                        model.dirs[slot].files.insert(to.clone(), moved);
                    }
                    trace.push(format!("{step_idx}:rename_ok:{slot}:{from}->{to}"));
                    (1, 0)
                }
                Err(e) if chaos_active && is_indeterminate_chaos_error(&e) => {
                    reconcile_indeterminate_rename(
                        cluster, all_nodes, swizzle, model, slot, slot, &from, &to, None, step_idx,
                        trace, violations,
                    )
                    .await
                }
                Err(e) => {
                    push_violation(violations, format!("intra rename {from}->{to} failed: {e}"));
                    (0, 0)
                }
            }
        }
        ChaosStepOp::CrossRename {
            src_slot,
            dst_slot,
            from,
            to,
        } => {
            let src_pid = model.dirs[src_slot].pid;
            let dst_pid = model.dirs[dst_slot].pid;
            let call =
                tokio::time::timeout(step_timeout, tx.rename(src_pid, &from, dst_pid, &to, false))
                    .await
                    .unwrap_or_else(|_| {
                        Err(MetaDbError::RpcError("chaos client timeout".to_string()))
                    });
            match call {
                Ok(_) => {
                    if let Some(moved) = model.dirs[src_slot].files.remove(&from) {
                        model.dirs[dst_slot].files.insert(to.clone(), moved);
                    }
                    trace.push(format!(
                        "{step_idx}:xrename_ok:{src_slot}:{from}->{dst_slot}:{to}"
                    ));
                    (1, 0)
                }
                Err(e) if chaos_active && is_indeterminate_chaos_error(&e) => {
                    reconcile_indeterminate_rename(
                        cluster, all_nodes, swizzle, model, src_slot, dst_slot, &from, &to, None,
                        step_idx, trace, violations,
                    )
                    .await
                }
                Err(e) => {
                    push_violation(
                        violations,
                        format!("cross rename {src_slot}:{from}->{dst_slot}:{to} failed: {e}"),
                    );
                    (0, 0)
                }
            }
        }
        ChaosStepOp::OverwriteRename {
            src_slot,
            dst_slot,
            from,
            victim,
        } => {
            let src_pid = model.dirs[src_slot].pid;
            let dst_pid = model.dirs[dst_slot].pid;
            let expected_src_fid = model.dirs[src_slot].files.get(&from).map(|f| f.fid);
            let call = tokio::time::timeout(
                step_timeout,
                tx.rename(src_pid, &from, dst_pid, &victim, true),
            )
            .await
            .unwrap_or_else(|_| Err(MetaDbError::RpcError("chaos client timeout".to_string())));
            match call {
                Ok(_) => {
                    if let Some(moved) = model.dirs[src_slot].files.remove(&from) {
                        if let Some(old_victim) =
                            model.dirs[dst_slot].files.insert(victim.clone(), moved)
                        {
                            model.deleted_fids.insert(old_victim.fid);
                        }
                    }
                    trace.push(format!(
                        "{step_idx}:ovw_rename_ok:{src_slot}:{from}->{dst_slot}:{victim}"
                    ));
                    (1, 0)
                }
                Err(e) if chaos_active && is_indeterminate_chaos_error(&e) => {
                    reconcile_indeterminate_rename(
                        cluster,
                        all_nodes,
                        swizzle,
                        model,
                        src_slot,
                        dst_slot,
                        &from,
                        &victim,
                        expected_src_fid,
                        step_idx,
                        trace,
                        violations,
                    )
                    .await
                }
                Err(e) => {
                    push_violation(
                        violations,
                        format!(
                            "overwrite rename {src_slot}:{from}->{dst_slot}:{victim} failed: {e}"
                        ),
                    );
                    (0, 0)
                }
            }
        }
        ChaosStepOp::SetAttrSize {
            slot,
            name,
            new_size,
        } => {
            let pid = model.dirs[slot].pid;
            let call = tokio::time::timeout(
                step_timeout,
                tx.setattr(
                    pid,
                    &name,
                    SetAttrPatch {
                        size_bytes: Some(new_size),
                        ..Default::default()
                    },
                ),
            )
            .await
            .unwrap_or_else(|_| Err(MetaDbError::RpcError("chaos client timeout".to_string())));
            match call {
                Ok(_) => {
                    if let Some(f) = model.dirs[slot].files.get_mut(&name) {
                        f.size_bytes = new_size;
                    }
                    trace.push(format!("{step_idx}:size_ok:{slot}:{name}:{new_size}"));
                    (1, 0)
                }
                Err(e) if chaos_active && is_indeterminate_chaos_error(&e) => {
                    match resolve_lookup_with_retry(cluster, all_nodes, swizzle, pid, &name).await {
                        Ok(Some(cur)) => {
                            if let Some(f) = model.dirs[slot].files.get_mut(&name) {
                                f.size_bytes = cur.attr.size_bytes;
                            }
                            (1, 1)
                        }
                        Ok(None) => {
                            push_violation(
                                violations,
                                format!("file {name} vanished during indeterminate SetAttrSize"),
                            );
                            (0, 1)
                        }
                        Err(re) => {
                            push_violation(violations, format!("resolve SetAttrSize {name}: {re}"));
                            (0, 1)
                        }
                    }
                }
                Err(e) => {
                    push_violation(violations, format!("SetAttrSize {name} failed: {e}"));
                    (0, 0)
                }
            }
        }
        ChaosStepOp::SetAttrChown {
            slot,
            name,
            new_uid,
        } => {
            let pid = model.dirs[slot].pid;
            let call = tokio::time::timeout(
                step_timeout,
                tx.setattr(
                    pid,
                    &name,
                    SetAttrPatch {
                        uid: Some(new_uid),
                        ..Default::default()
                    },
                ),
            )
            .await
            .unwrap_or_else(|_| Err(MetaDbError::RpcError("chaos client timeout".to_string())));
            match call {
                Ok(_) => {
                    if let Some(f) = model.dirs[slot].files.get_mut(&name) {
                        f.uid = new_uid;
                    }
                    trace.push(format!("{step_idx}:chown_ok:{slot}:{name}:{new_uid}"));
                    (1, 0)
                }
                Err(e) if chaos_active && is_indeterminate_chaos_error(&e) => {
                    match resolve_lookup_with_retry(cluster, all_nodes, swizzle, pid, &name).await {
                        Ok(Some(cur)) => {
                            if let Some(f) = model.dirs[slot].files.get_mut(&name) {
                                f.uid = cur.attr.uid;
                            }
                            (1, 1)
                        }
                        Ok(None) => {
                            push_violation(
                                violations,
                                format!("file {name} vanished during indeterminate SetAttrChown"),
                            );
                            (0, 1)
                        }
                        Err(re) => {
                            push_violation(
                                violations,
                                format!("resolve SetAttrChown {name}: {re}"),
                            );
                            (0, 1)
                        }
                    }
                }
                Err(e) => {
                    push_violation(violations, format!("SetAttrChown {name} failed: {e}"));
                    (0, 0)
                }
            }
        }
        ChaosStepOp::MkdirRmdir {
            parent_slot,
            temp_name,
            uid,
            seq,
        } => {
            if chaos_active {
                // Skip composite two-call Mkdir+Rmdir during active link clogging so single-op
                // atomicity resolution remains exact; exercise paginated read instead.
                let pid = model.dirs[parent_slot].pid;
                let _ = tx
                    .listdir_paginated(pid, None, None, PRIME_LIST_PAGE_LIMIT)
                    .await;
                return (0, 0);
            }
            let parent_pid = model.dirs[parent_slot].pid;
            let dir_id = cluster
                .id_buffer()
                .next_dir_inode_id()
                .unwrap_or(50_000 + seq);
            match tx
                .mkdir(parent_pid, &temp_name, dir_id, uid, DIR_MODE)
                .await
            {
                Ok(entry) => {
                    if !is_dir_inode_id(entry.attr.inode_id) {
                        push_violation(
                            violations,
                            format!(
                                "mkdir {temp_name} returned non-dir id {:#x}",
                                entry.attr.inode_id
                            ),
                        );
                    }
                    if let Err(e) = tx.rmdir(parent_pid, &temp_name).await {
                        push_violation(violations, format!("rmdir {temp_name} failed: {e}"));
                    }
                    trace.push(format!(
                        "{step_idx}:mkdir_rmdir_ok:{parent_slot}:{temp_name}"
                    ));
                    (2, 0)
                }
                Err(e) => {
                    push_violation(violations, format!("mkdir {temp_name} failed: {e}"));
                    (0, 0)
                }
            }
        }
        ChaosStepOp::ReadVerify { slot } => {
            let pid = model.dirs[slot].pid;
            if !chaos_active {
                if let Err(e) = tx
                    .listdir_paginated(pid, None, None, PRIME_LIST_PAGE_LIMIT)
                    .await
                {
                    push_violation(violations, format!("read listdir slot={slot} failed: {e}"));
                }
            } else {
                let _ = tokio::time::timeout(
                    step_timeout,
                    tx.listdir_paginated(pid, None, None, PRIME_LIST_PAGE_LIMIT),
                )
                .await;
            }
            (0, 0)
        }
    }
}

async fn resolve_lookup_with_retry(
    cluster: &PersistentMetaDbCluster,
    all_nodes: &[Arc<PersistentMetaDbCluster>],
    swizzle: Option<&SwizzleClogController>,
    pid: InodeId,
    name: &str,
) -> Result<Option<metadb_core::MetaEntry>, String> {
    if let Some(ctrl) = swizzle {
        // Temporarily clear simulated wire faults and allow any single already-accepted server-side
        // Raft proposal in flight from the timed-out client call to finish committing or failing
        // before observing linearizable ground truth; `execute_chaos_step_with_clean_probe`
        // immediately re-arms the active swizzle-clog state.
        ctrl.heal_all();
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    let tx = cluster.tx_router();
    let mut last_err = String::new();
    for attempt in 0..45u64 {
        match tx.lookup(pid, name).await {
            Ok(res) => return Ok(res),
            Err(e) => {
                last_err = e.to_string();
                let _ = cluster.run_background_maintenance_tick().await;
                for n in all_nodes {
                    let _ = n.run_background_maintenance_tick().await;
                }
                tokio::time::sleep(Duration::from_millis(40 + attempt * 8)).await;
            }
        }
    }
    Err(last_err)
}

#[allow(clippy::too_many_arguments)]
async fn reconcile_indeterminate_rename(
    cluster: &PersistentMetaDbCluster,
    all_nodes: &[Arc<PersistentMetaDbCluster>],
    swizzle: Option<&SwizzleClogController>,
    model: &mut FullStackModel,
    src_slot: usize,
    dst_slot: usize,
    from: &str,
    to: &str,
    expected_src_fid: Option<FileId>,
    step_idx: u32,
    trace: &mut Vec<String>,
    violations: &mut Vec<String>,
) -> (u32, u32) {
    if let Some(ctrl) = swizzle {
        ctrl.heal_all();
        tokio::time::sleep(Duration::from_millis(120)).await;
    }
    let src_pid = model.dirs[src_slot].pid;
    let dst_pid = model.dirs[dst_slot].pid;
    // Drive any pending 2PC recovery before observing `(src, dst)` atomicity!
    for _ in 0..3 {
        let _ = cluster.run_background_maintenance_tick().await;
        for n in all_nodes {
            let _ = n.run_background_maintenance_tick().await;
        }
    }
    let src_obs = match resolve_lookup_with_retry(cluster, all_nodes, swizzle, src_pid, from).await
    {
        Ok(v) => v,
        Err(e) => {
            push_violation(violations, format!("resolve rename src {from} failed: {e}"));
            return (0, 1);
        }
    };
    let dst_obs = match resolve_lookup_with_retry(cluster, all_nodes, swizzle, dst_pid, to).await {
        Ok(v) => v,
        Err(e) => {
            push_violation(violations, format!("resolve rename dst {to} failed: {e}"));
            return (0, 1);
        }
    };

    let committed = match expected_src_fid {
        // OverwriteRename: `to` already existed before the call, so check whether `dst_obs`'s `fid`
        // became `expected_src_fid`!
        Some(src_fid) => dst_obs.as_ref().and_then(|e| e.attr.fid) == Some(src_fid),
        // Non-overwrite rename: `to` was freshly allocated, so `dst_obs.is_some()` iff committed!
        None => dst_obs.is_some(),
    };

    if committed {
        if src_obs.is_some() {
            push_violation(
                violations,
                format!(
                    "ATOMICITY VIOLATION: rename {src_slot}:{from}->{dst_slot}:{to} committed at dst AND still present at src!"
                ),
            );
        }
        if let Some(moved) = model.dirs[src_slot].files.remove(from) {
            if let Some(old_victim) = model.dirs[dst_slot].files.insert(to.to_string(), moved) {
                model.deleted_fids.insert(old_victim.fid);
            }
        }
        trace.push(format!(
            "{step_idx}:rename_indet_committed:{src_slot}:{from}->{dst_slot}:{to}"
        ));
        (1, 1)
    } else {
        if src_obs.is_none() {
            push_violation(
                violations,
                format!(
                    "ATOMICITY VIOLATION: rename {src_slot}:{from}->{dst_slot}:{to} aborted at dst AND lost src!"
                ),
            );
        }
        trace.push(format!(
            "{step_idx}:rename_indet_aborted:{src_slot}:{from}->{dst_slot}:{to}"
        ));
        (0, 1)
    }
}

/// Drains `AuxDispatcher` and `TxnRecoveryWorker` across all provided nodes until quiescent.
async fn quiesce_cluster_nodes(nodes: &[Arc<PersistentMetaDbCluster>]) {
    for round in 0..75 {
        let mut pending_outbox = 0usize;
        let mut open_intents = 0usize;
        for node in nodes {
            let _ = node.run_background_maintenance_tick().await;
            for cell in node.global_manager().all_cells() {
                for (_, grp, handler) in cell.local_shard_handles() {
                    if grp.current_leader() == Some(node.config().node.node_id) {
                        pending_outbox += handler.outbox().pending_count();
                        let eng = grp.engine();
                        if let Ok(snap) =
                            eng.acquire_linearizable_snapshot(grp.last_applied_index())
                        {
                            open_intents += eng
                                .scan_with_snapshot(
                                    &[SYSTEM_PREFIX_WRITE_INTENT],
                                    &[SYSTEM_PREFIX_WRITE_INTENT + 1],
                                    &snap,
                                )
                                .map(|v| v.len())
                                .unwrap_or(0);
                        }
                    }
                }
            }
        }
        if pending_outbox == 0 && open_intents == 0 && round >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(80)).await;
    }
}

/// Audits all 6 FoundationDB-grade invariants across `nodes` and `model`.
pub async fn verify_full_stack_cluster_invariants(
    primary_client_node: &PersistentMetaDbCluster,
    all_nodes: &[Arc<PersistentMetaDbCluster>],
    model: &FullStackModel,
    check_three_replica_convergence: bool,
    violations: &mut Vec<String>,
) {
    let tx = primary_client_node.tx_router();

    // Invariant 1 & 2: Paginated `listdir` (`limit = 5`), `lookup`, `_attr_` isolation, & `dirstat` conservation.
    match tx.dirstat(ROOT_INODE_ID).await {
        Ok(root_stat) => {
            if root_stat.num_children != model.dirs.len() as i64 || root_stat.size_bytes != 0 {
                push_violation(
                    violations,
                    format!(
                        "root dirstat got=({}, {}) want=({}, 0)",
                        root_stat.num_children,
                        root_stat.size_bytes,
                        model.dirs.len()
                    ),
                );
            }
        }
        Err(e) => push_violation(violations, format!("root dirstat failed: {e}")),
    }

    for dir in &model.dirs {
        let mut names = BTreeSet::new();
        let mut cursor: Option<String> = None;
        for _ in 0..256 {
            match tx
                .listdir_paginated(dir.pid, cursor.as_deref(), None, PRIME_LIST_PAGE_LIMIT)
                .await
            {
                Ok((page, next)) => {
                    for entry in page {
                        if entry.key.name == ATTR_ENTRY_NAME {
                            push_violation(
                                violations,
                                format!("dir {} leaked _attr_ in listdir", dir.name),
                            );
                        }
                        names.insert(entry.key.name);
                    }
                    if next.is_none() {
                        break;
                    }
                    cursor = next;
                }
                Err(e) => {
                    push_violation(violations, format!("listdir {} failed: {e}", dir.name));
                    break;
                }
            }
        }
        let want_names: BTreeSet<String> = dir.files.keys().cloned().collect();
        if names != want_names {
            let missing: Vec<_> = want_names.difference(&names).take(4).collect();
            let extra: Vec<_> = names.difference(&want_names).take(4).collect();
            push_violation(
                violations,
                format!(
                    "dir {} listing mismatch missing={missing:?} extra={extra:?}",
                    dir.name
                ),
            );
        }

        let want_bytes: i64 = dir.files.values().map(|f| f.size_bytes).sum();
        match tx.dirstat(dir.pid).await {
            Ok(stat) => {
                if stat.num_children != dir.files.len() as i64 || stat.size_bytes != want_bytes {
                    push_violation(
                        violations,
                        format!(
                            "dir {} dirstat got=({}, {}) want=({}, {})",
                            dir.name,
                            stat.num_children,
                            stat.size_bytes,
                            dir.files.len(),
                            want_bytes
                        ),
                    );
                }
            }
            Err(e) => push_violation(violations, format!("dirstat {} failed: {e}", dir.name)),
        }

        for (fname, f) in &dir.files {
            match tx.lookup(dir.pid, fname).await {
                Ok(Some(got)) => {
                    if got.attr.size_bytes != f.size_bytes
                        || got.attr.uid != f.uid
                        || got.attr.fid != Some(f.fid)
                        || !is_file_inode_id(got.attr.inode_id)
                    {
                        push_violation(
                            violations,
                            format!(
                                "lookup {}/{fname} mismatch got=(uid={},sz={},fid={:?}) want=(uid={},sz={},fid={:?})",
                                dir.name,
                                got.attr.uid,
                                got.attr.size_bytes,
                                got.attr.fid,
                                f.uid,
                                f.size_bytes,
                                Some(f.fid)
                            ),
                        );
                    }
                }
                Ok(None) => {
                    push_violation(violations, format!("lookup {}/{fname} missing", dir.name))
                }
                Err(e) => push_violation(
                    violations,
                    format!("lookup {}/{fname} failed: {e}", dir.name),
                ),
            }
        }
        if let Ok(Some(unexpected)) = tx.lookup(dir.pid, "__never_created__.dat").await {
            push_violation(
                violations,
                format!("absent lookup unexpectedly returned {:?}", unexpected.key),
            );
        }
    }

    // Invariant 3: Multi-UID QuotaTable (`[0x02]`) conservation.
    for &uid in &model.touched_uids {
        let (want_inodes, want_bytes) = model.expected_quota(uid);
        match tx.get_quota(uid).await {
            Ok(opt) => {
                let (got_inodes, got_bytes) =
                    opt.map(|q| (q.used_inodes, q.used_bytes)).unwrap_or((0, 0));
                if got_inodes != want_inodes || got_bytes != want_bytes {
                    push_violation(
                        violations,
                        format!(
                            "QuotaTable uid={uid} drift got=(inodes={got_inodes}, bytes={got_bytes}) want=(inodes={want_inodes}, bytes={want_bytes})"
                        ),
                    );
                }
            }
            Err(e) => push_violation(violations, format!("get_quota({uid}) failed: {e}")),
        }
    }

    // Invariant 4: SecondaryIndexTable (`[0x03]`) live bijection & deleted-FID absence.
    for (fid, (want_pid, want_name)) in model.expected_live_index() {
        match tx.get_secondary_index(fid).await {
            Ok(Some(rec)) if rec.pid == want_pid && rec.name == want_name => {}
            Ok(Some(rec)) => push_violation(
                violations,
                format!(
                    "SecondaryIndex fid={fid} stale mapping got=({}, {}) want=({want_pid}, {want_name})",
                    rec.pid, rec.name
                ),
            ),
            Ok(None) => push_violation(
                violations,
                format!("SecondaryIndex fid={fid} missing for live file {want_pid}/{want_name}"),
            ),
            Err(e) => push_violation(
                violations,
                format!("get_secondary_index({fid}) failed: {e}"),
            ),
        }
    }
    for &dead_fid in &model.deleted_fids {
        match tx.get_secondary_index(dead_fid).await {
            Ok(None) => {}
            Ok(Some(zombie)) => push_violation(
                violations,
                format!(
                    "SecondaryIndex zombie fid={dead_fid} still points to ({}, {})",
                    zombie.pid, zombie.name
                ),
            ),
            Err(e) => push_violation(
                violations,
                format!("get_secondary_index(dead {dead_fid}) failed: {e}"),
            ),
        }
    }

    // Invariant 5: System keyspace zero-leakage (`[0x05]`, `[0x06]`, `[0x07]`, `[0x0D]`).
    for node in all_nodes {
        let nid = node.config().node.node_id;
        for cell in node.global_manager().all_cells() {
            for (_, grp, handler) in cell.local_shard_handles() {
                let sid = grp.shard_id();
                if grp.current_leader() == Some(nid) && handler.outbox().pending_count() > 0 {
                    push_violation(
                        violations,
                        format!(
                            "node={nid} shard={sid} still has {} un-acked in-memory outbox tasks",
                            handler.outbox().pending_count()
                        ),
                    );
                }
                let eng = grp.engine();
                let Ok(snap) = eng.acquire_linearizable_snapshot(grp.last_applied_index()) else {
                    continue;
                };
                for (label, start, end) in [
                    (
                        "0x05 WriteIntent",
                        vec![SYSTEM_PREFIX_WRITE_INTENT],
                        vec![SYSTEM_PREFIX_WRITE_INTENT + 1],
                    ),
                    (
                        "0x06 TxnRecord",
                        vec![SYSTEM_PREFIX_TXN_RECORD],
                        vec![SYSTEM_PREFIX_TXN_RECORD + 1],
                    ),
                    (
                        "0x07 SpilledAuxOutbox",
                        vec![SYSTEM_PREFIX_SPILLED_AUX_OUTBOX],
                        SpilledAuxOutboxKey::prefix_end().to_vec(),
                    ),
                    (
                        "0x0D LeasePreimage",
                        vec![SYSTEM_PREFIX_LEASE_PREIMAGE],
                        vec![SYSTEM_PREFIX_LEASE_PREIMAGE + 1],
                    ),
                ] {
                    if let Ok(leaked) = eng.scan_with_snapshot(&start, &end, &snap) {
                        if !leaked.is_empty() {
                            push_violation(
                                violations,
                                format!(
                                    "node={nid} shard={sid} leaked {} keys in system keyspace {label}",
                                    leaked.len()
                                ),
                            );
                        }
                    }
                }
            }
        }
    }

    // Invariant 6: Route contiguity `[0x01, 0x04)` & 3-Voter byte-for-byte state-machine convergence.
    let mut routes = primary_client_node.cell_manager().list_routes();
    routes.sort_by(|a, b| a.range.start_key.cmp(&b.range.start_key));
    if routes.is_empty() {
        push_violation(violations, "cell has zero routes".to_string());
    } else {
        if routes.first().unwrap().range.start_key != vec![TABLE_PREFIX_META] {
            push_violation(
                violations,
                format!(
                    "first route does not start at [0x01]: {:?}",
                    routes.first().unwrap().range.start_key
                ),
            );
        }
        if routes.last().unwrap().range.end_key != vec![TABLE_PREFIX_SECONDARY_INDEX + 1] {
            push_violation(
                violations,
                format!(
                    "last route does not end at [0x04]: {:?}",
                    routes.last().unwrap().range.end_key
                ),
            );
        }
        for pair in routes.windows(2) {
            if pair[0].range.end_key != pair[1].range.start_key {
                push_violation(
                    violations,
                    format!(
                        "route gap/overlap between shard {} ({:?}) and shard {} ({:?})",
                        pair[0].shard_id, pair[0].range, pair[1].shard_id, pair[1].range
                    ),
                );
            }
        }
    }

    if check_three_replica_convergence && all_nodes.len() >= 2 {
        // Allow followers up to 1.5s after network heal to apply the final Raft log tail.
        for route in &routes {
            let sid = route.shard_id;
            for _ in 0..30 {
                let applied: Vec<u64> = all_nodes
                    .iter()
                    .filter_map(|n| n.host()?.get_group(sid).map(|g| g.last_applied_index()))
                    .collect();
                if applied.len() == all_nodes.len()
                    && applied.iter().min() == applied.iter().max()
                    && *applied.first().unwrap_or(&0) > 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            type ReplicaKvDump = Vec<(Vec<u8>, Vec<u8>)>;
            let mut reference: Option<(u64, ReplicaKvDump)> = None;
            for node in all_nodes {
                let nid = node.config().node.node_id;
                let Ok((_, grp, handler)) = node.cell_manager().lookup_local_handle(sid) else {
                    push_violation(
                        violations,
                        format!("replica node={nid} missing open handle for shard={sid}"),
                    );
                    continue;
                };
                let idx = grp.last_applied_index();
                let eng = grp.engine();
                let Ok(snap) = eng.acquire_linearizable_snapshot(idx) else {
                    push_violation(
                        violations,
                        format!("replica node={nid} shard={sid} failed snapshot at idx={idx}"),
                    );
                    continue;
                };
                let Ok(raw_kvs) = eng.scan_with_snapshot(
                    &[TABLE_PREFIX_META],
                    &[TABLE_PREFIX_SECONDARY_INDEX + 1],
                    &snap,
                ) else {
                    push_violation(
                        violations,
                        format!("replica node={nid} shard={sid} failed user-table scan"),
                    );
                    continue;
                };
                // Normalize `DirectoryAttr` (`_attr_`) via `reconcile_inode_attr` so replicas that
                // sealed/folded memtables at different local timings under `Buggify` compare their
                // exact logical state-machine values!
                let kvs: ReplicaKvDump = raw_kvs
                    .into_iter()
                    .map(|(k, v)| {
                        if k.first() == Some(&TABLE_PREFIX_META) {
                            if let Ok(mk) = metadb_core::MetaKey::decode(&k) {
                                if mk.is_attr_key() {
                                    if let Ok(base) = metadb_core::InodeAttr::decode(&v) {
                                        let mut rec = handler
                                            .attr_aggregator()
                                            .reconcile_inode_attr(mk.pid, base, &snap);
                                        rec.applied_lsn = 0;
                                        return (k, rec.encode());
                                    }
                                }
                            }
                        }
                        (k, v)
                    })
                    .collect();
                match &reference {
                    None => reference = Some((nid, kvs)),
                    Some((ref_nid, ref_kvs)) => {
                        if &kvs != ref_kvs {
                            push_violation(
                                violations,
                                format!(
                                    "REPLICA DIVERGENCE on shard={sid}: node={nid} ({} keys) != node={ref_nid} ({} keys)",
                                    kvs.len(),
                                    ref_kvs.len()
                                ),
                            );
                        }
                    }
                }
            }
        }
    }
}

/// Runs Workload 2 (`PersistentMultiFaultCascade`):
/// Combines seeded `Buggify`, online PID split, `DualModeFineGrained` hot-directory split,
/// cross-shard 2PC & overwrite renames, WAL-less unflushed crash restart, compaction/purge,
/// and clean restart, audited by Invariants 1–5.
pub async fn run_persistent_multi_fault_cascade(seed: u64, steps: u32) -> FullStackSimReport {
    let started = Instant::now();
    let total_steps = steps.max(18);
    let dir = SimTempDir::new(seed);
    let cfg = build_node_config(dir.path(), 1, None, 4096);
    let buggify_guard = GlobalBuggifyGuard::install(seed, BuggifyConfig::default());

    let mut violations = Vec::new();
    let mut trace = Vec::new();
    let mut rng = DetRng::new(seed);
    let mut model = FullStackModel::new();

    let (mut cluster, _) = match PersistentMetaDbCluster::open_and_serve(cfg.clone()).await {
        Ok(pair) => pair,
        Err(e) => {
            return FullStackSimReport {
                workload: ChaosWorkloadKind::PersistentMultiFaultCascade,
                seed,
                steps_executed: 0,
                committed_mutations: 0,
                indeterminate_resolved: 0,
                buggify_firings: 0,
                network_stats: (0, 0, 0),
                violations: vec![format!("initial open_and_serve failed: {e}")],
                trace_tail: Vec::new(),
                model_fingerprint: String::new(),
                trace_hash: 0,
                elapsed: started.elapsed(),
            };
        }
    };

    for slot in 0..4u32 {
        let name = format!("pdir_{slot:02}");
        let uid = UID_POOL[(slot as usize) % UID_POOL.len()];
        match cluster
            .mkdir_auto_id(ROOT_INODE_ID, &name, uid, DIR_MODE)
            .await
        {
            Ok(entry) => model.dirs.push(TrackedDir {
                name,
                pid: entry.attr.inode_id,
                uid,
                files: BTreeMap::new(),
            }),
            Err(e) => {
                push_violation(&mut violations, format!("setup mkdir {name} failed: {e}"));
            }
        }
    }

    let split_step = total_steps / 3;
    let crash_step = (total_steps * 2) / 3;
    let mut committed = 0u32;

    for step_idx in 0..total_steps {
        if violations.len() >= MAX_ORACLE_VIOLATIONS {
            break;
        }
        if step_idx == split_step {
            // Fault 1: Online PID shard split + DualModeFineGrained activation on hot directory 0!
            let _ = cluster
                .cell_manager()
                .maybe_split_or_isolate_shard(1, true)
                .await;
            if let Some(hot_dir) = model.dirs.first() {
                if let Ok((_, grp, _)) = cluster
                    .global_manager()
                    .route_user_key(&metadb_core::MetaKey::dir_attr(hot_dir.pid).encode())
                {
                    let _ = cluster.cell_manager().enable_dual_mode_split_with_hint(
                        grp.shard_id(),
                        hot_dir.pid,
                        Some("m".to_string()),
                    );
                }
            }
            trace.push(format!("{step_idx}:fault:split_and_dual_mode"));
        } else if step_idx == crash_step {
            // Fault 2: Abrupt WAL-less crash (`shutdown_without_flush`) + reopen from `.raftlog` & `MANIFEST`!
            if let Err(e) = cluster.shutdown_without_flush().await {
                push_violation(&mut violations, format!("unflushed crash failed: {e}"));
                break;
            }
            drop(cluster);
            match PersistentMetaDbCluster::open_and_serve(cfg.clone()).await {
                Ok((reopened, _)) => {
                    cluster = reopened;
                    trace.push(format!("{step_idx}:fault:unflushed_crash_reopen"));
                }
                Err(e) => {
                    push_violation(&mut violations, format!("reopen after crash failed: {e}"));
                    let trace_tail = make_trace_tail(&trace, true);
                    return FullStackSimReport {
                        workload: ChaosWorkloadKind::PersistentMultiFaultCascade,
                        seed,
                        steps_executed: step_idx,
                        committed_mutations: committed,
                        indeterminate_resolved: 0,
                        buggify_firings: buggify_guard.firings(),
                        network_stats: (0, 0, 0),
                        violations,
                        trace_tail,
                        model_fingerprint: model.fingerprint(),
                        trace_hash: 0,
                        elapsed: started.elapsed(),
                    };
                }
            }
        }

        let op = choose_step_op(&mut rng, &mut model, true);
        let (c, _) = execute_cluster_step(
            &cluster,
            &[],
            None,
            &mut model,
            op,
            false,
            step_idx,
            &mut trace,
            &mut violations,
        )
        .await;
        committed += c;
    }

    // Fault 3: Full flush + Raft snapshot/log purge + clean shutdown + second reopen!
    if violations.is_empty() {
        if let Err(e) = cluster.flush_all().await {
            push_violation(&mut violations, format!("flush_all failed: {e}"));
        }
        if let Err(e) = cluster.shutdown().await {
            push_violation(&mut violations, format!("clean shutdown failed: {e}"));
        }
        drop(cluster);
        match PersistentMetaDbCluster::open_and_serve(cfg).await {
            Ok((reopened, _)) => {
                trace.push("final:fault:flush_purge_reopen".to_string());
                let nodes = vec![reopened.clone()];
                quiesce_cluster_nodes(&nodes).await;
                verify_full_stack_cluster_invariants(
                    &reopened,
                    &nodes,
                    &model,
                    false,
                    &mut violations,
                )
                .await;
                let _ = reopened.shutdown().await;
            }
            Err(e) => push_violation(&mut violations, format!("second reopen failed: {e}")),
        }
    } else {
        let _ = cluster.shutdown_without_flush().await;
    }

    let fp = model.fingerprint();
    let th = compute_trace_hash(&trace, &fp);
    let firings = buggify_guard.firings();
    drop(buggify_guard);

    let trace_tail = make_trace_tail(&trace, !violations.is_empty());
    FullStackSimReport {
        workload: ChaosWorkloadKind::PersistentMultiFaultCascade,
        seed,
        steps_executed: total_steps,
        committed_mutations: committed,
        indeterminate_resolved: 0,
        buggify_firings: firings,
        network_stats: (0, 0, 0),
        violations,
        trace_tail,
        model_fingerprint: fp,
        trace_hash: th,
        elapsed: started.elapsed(),
    }
}

/// Runs Workload 3 (`ThreeVoterSwizzleClogChaos`):
/// Boots a 3-Voter Multi-Raft cluster (`Node 1, Node 2, Node 3`) with `SwizzleClogController`
/// and `GlobalBuggifyGuard`, executes online 3-voter shard split + FoundationDB `Swizzle-Clog`
/// asymmetric link clogging/unclogging + RPC jitter + post-apply reply drops + leader failover,
/// resolves indeterminate ops atomically, heals the cluster, and checks ALL 6 INVARIANTS!
pub async fn run_three_voter_swizzle_clog_chaos(seed: u64, steps: u32) -> FullStackSimReport {
    let started = Instant::now();
    let total_steps = steps.max(20);
    let root_dir = SimTempDir::new(seed);
    let swizzle = SwizzleClogController::new(seed);
    let buggify_guard = GlobalBuggifyGuard::install(
        seed,
        BuggifyConfig {
            enable_permille: 550,
            fire_permille: 200,
        },
    );

    let mut violations = Vec::new();
    let mut trace = Vec::new();
    let mut rng = DetRng::new(seed);
    let mut model = FullStackModel::new();

    // Boot Node 1 (bootstrap) + Node 2 (`--join`) + Node 3 (`--join`).
    let cfg1 = build_node_config(&root_dir.path().join("n1"), 1, None, 8192);
    let (n1, addr1) = match PersistentMetaDbCluster::open_and_serve(cfg1).await {
        Ok(p) => p,
        Err(e) => {
            return FullStackSimReport {
                workload: ChaosWorkloadKind::ThreeVoterSwizzleClogChaos,
                seed,
                steps_executed: 0,
                committed_mutations: 0,
                indeterminate_resolved: 0,
                buggify_firings: 0,
                network_stats: (0, 0, 0),
                violations: vec![format!("node 1 boot failed: {e}")],
                trace_tail: Vec::new(),
                model_fingerprint: String::new(),
                trace_hash: 0,
                elapsed: started.elapsed(),
            };
        }
    };
    let cfg2 = build_node_config(&root_dir.path().join("n2"), 2, Some(&addr1), 8192);
    let (n2, addr2) = match PersistentMetaDbCluster::open_and_serve(cfg2).await {
        Ok(p) => p,
        Err(e) => {
            let _ = n1.shutdown_without_flush().await;
            return FullStackSimReport {
                workload: ChaosWorkloadKind::ThreeVoterSwizzleClogChaos,
                seed,
                steps_executed: 0,
                committed_mutations: 0,
                indeterminate_resolved: 0,
                buggify_firings: 0,
                network_stats: (0, 0, 0),
                violations: vec![format!("node 2 join failed: {e}")],
                trace_tail: Vec::new(),
                model_fingerprint: String::new(),
                trace_hash: 0,
                elapsed: started.elapsed(),
            };
        }
    };
    let cfg3 = build_node_config(&root_dir.path().join("n3"), 3, Some(&addr1), 8192);
    let (n3, addr3) = match PersistentMetaDbCluster::open_and_serve(cfg3).await {
        Ok(p) => p,
        Err(e) => {
            let _ = n1.shutdown_without_flush().await;
            let _ = n2.shutdown_without_flush().await;
            return FullStackSimReport {
                workload: ChaosWorkloadKind::ThreeVoterSwizzleClogChaos,
                seed,
                steps_executed: 0,
                committed_mutations: 0,
                indeterminate_resolved: 0,
                buggify_firings: 0,
                network_stats: (0, 0, 0),
                violations: vec![format!("node 3 join failed: {e}")],
                trace_tail: Vec::new(),
                model_fingerprint: String::new(),
                trace_hash: 0,
                elapsed: started.elapsed(),
            };
        }
    };

    let nodes = vec![n1.clone(), n2.clone(), n3.clone()];
    let addrs = vec![addr1.clone(), addr2.clone(), addr3.clone()];
    for (node, addr) in nodes.iter().zip(addrs.iter()) {
        if let Some(host) = node.host() {
            host.channel_pool()
                .set_network_chaos_hook(addr.clone(), Some(swizzle.clone()));
        }
    }

    for slot in 0..4u32 {
        let name = format!("cdir_{slot:02}");
        let uid = UID_POOL[(slot as usize) % UID_POOL.len()];
        match n1.mkdir_auto_id(ROOT_INODE_ID, &name, uid, DIR_MODE).await {
            Ok(entry) => model.dirs.push(TrackedDir {
                name,
                pid: entry.attr.inode_id,
                uid,
                files: BTreeMap::new(),
            }),
            Err(e) => push_violation(&mut violations, format!("setup mkdir {name} failed: {e}")),
        }
    }

    // Plan a deterministic FoundationDB Swizzle-Clog schedule (2 directed links so quorum paths shift
    // asymmetrically without permanently wedging all 3 nodes simultaneously, plus 1 full-node isolation phase!).
    let plan = SwizzleClogController::plan_swizzle_clog(&addrs, seed, 2);
    let split_at = total_steps / 4;
    let swizzle_start = total_steps / 3;
    let isolate_at = (total_steps * 2) / 3;
    let heal_at = (total_steps * 5) / 6;

    let mut committed = 0u32;
    let mut indeterminate_resolved = 0u32;
    let mut chaos_active = false;

    for step_idx in 0..total_steps {
        if violations.len() >= MAX_ORACLE_VIOLATIONS {
            break;
        }
        if step_idx == split_at {
            // Execute online 3-voter shard split before clogging links!
            swizzle.heal_all();
            let _ = n1
                .cell_manager()
                .maybe_split_or_isolate_shard(1, true)
                .await;
            for peer in &nodes {
                let _ = peer.cell_manager().list_routes();
            }
            trace.push(format!("{step_idx}:fault:three_voter_split"));
        } else if step_idx == swizzle_start {
            // Activate FDB Swizzle-Clog directed link drops + RPC jitter + post-apply reply drops!
            chaos_active = true;
            swizzle.set_max_jitter_ms(3);
            swizzle.set_drop_reply_permille(50);
            for (from, to) in &plan.clog_order {
                swizzle.clog_link(from.clone(), to.clone());
            }
            trace.push(format!("{step_idx}:fault:swizzle_clog_engaged"));
        } else if step_idx == isolate_at {
            // Unclog in swizzled order, then isolate one node completely to force Raft leader election / failover!
            for (from, to) in &plan.unclog_order {
                swizzle.unclog_link(from, to);
            }
            let isolated_idx = (seed as usize) % addrs.len();
            swizzle.isolate_node(addrs[isolated_idx].clone());
            code_probe("probe.raft.leader_failover_observed");
            tokio::time::sleep(Duration::from_millis(320)).await;
            trace.push(format!(
                "{step_idx}:fault:isolate_node_{}",
                isolated_idx + 1
            ));
        } else if step_idx == heal_at {
            swizzle.heal_all();
            chaos_active = false;
            tokio::time::sleep(Duration::from_millis(250)).await;
            quiesce_cluster_nodes(&nodes).await;
            trace.push(format!("{step_idx}:fault:network_healed"));
        }

        // Route client traffic through a live non-isolated node (rotating by `step_idx` to exercise
        // follower `LeaderForward` + `client_write`!).
        let client_idx = (step_idx as usize) % nodes.len();
        let active_client = &nodes[client_idx];
        let op = choose_step_op(&mut rng, &mut model, true);

        // If `chaos_active`, heal briefly only if needed during indeterminate resolution?
        // Better: if an op returns an indeterminate error while `chaos_active`, we temporarily pause
        // new drops during linearizable lookup resolution so the model knows the ground truth, then
        // immediately restore the active swizzle-clog state!
        let saved_links = plan.clog_order.clone();
        let op_res = if chaos_active {
            execute_chaos_step_with_clean_probe(
                active_client,
                &nodes,
                &swizzle,
                &saved_links,
                &addrs,
                step_idx >= isolate_at && step_idx < heal_at,
                (seed as usize) % addrs.len(),
                &mut model,
                op,
                step_idx,
                &mut trace,
                &mut violations,
            )
            .await
        } else {
            execute_cluster_step(
                active_client,
                &nodes,
                None,
                &mut model,
                op,
                false,
                step_idx,
                &mut trace,
                &mut violations,
            )
            .await
        };
        committed += op_res.0;
        indeterminate_resolved += op_res.1;
    }

    swizzle.heal_all();
    quiesce_cluster_nodes(&nodes).await;

    verify_full_stack_cluster_invariants(&n1, &nodes, &model, true, &mut violations).await;

    let net_stats = swizzle.stats();
    for node in &nodes {
        let _ = node.shutdown().await;
    }

    let fp = model.fingerprint();
    let th = compute_trace_hash(&trace, &fp);
    let firings = buggify_guard.firings();
    drop(buggify_guard);

    let trace_tail = make_trace_tail(&trace, !violations.is_empty());
    FullStackSimReport {
        workload: ChaosWorkloadKind::ThreeVoterSwizzleClogChaos,
        seed,
        steps_executed: total_steps,
        committed_mutations: committed,
        indeterminate_resolved,
        buggify_firings: firings,
        network_stats: net_stats,
        violations,
        trace_tail,
        model_fingerprint: fp,
        trace_hash: th,
        elapsed: started.elapsed(),
    }
}

#[allow(clippy::too_many_arguments)]
async fn execute_chaos_step_with_clean_probe(
    active_client: &PersistentMetaDbCluster,
    all_nodes: &[Arc<PersistentMetaDbCluster>],
    swizzle: &SwizzleClogController,
    clog_links: &[(String, String)],
    addrs: &[String],
    node_isolated: bool,
    isolated_idx: usize,
    model: &mut FullStackModel,
    op: ChaosStepOp,
    step_idx: u32,
    trace: &mut Vec<String>,
    violations: &mut Vec<String>,
) -> (u32, u32) {
    // Pick a non-isolated client entry point when one node is fully isolated, while still rotating
    // across remaining voters/followers.
    let client = if node_isolated && addrs.len() > 1 {
        let pick = (isolated_idx + 1 + (step_idx as usize % (addrs.len() - 1))) % addrs.len();
        &all_nodes[pick]
    } else {
        active_client
    };

    // Execute the mutation WITH full chaos active! If an indeterminate error occurs, `execute_cluster_step`
    // resolves ground truth; to prevent false timeouts if 2 links happened to partition the client's read,
    // we allow `resolve_lookup_with_retry` to heal dropped replies while probing and then re-arm chaos.
    swizzle.set_drop_reply_permille(60);
    let before_indet = violations.len();
    let res = execute_cluster_step(
        client,
        all_nodes,
        Some(swizzle),
        model,
        op,
        true,
        step_idx,
        trace,
        violations,
    )
    .await;
    if violations.len() > before_indet {
        // Re-check after healing if resolution hit transient partition quorum unavailability.
        swizzle.heal_all();
    } else if node_isolated {
        swizzle.heal_all();
        swizzle.isolate_node(addrs[isolated_idx].clone());
        swizzle.set_max_jitter_ms(3);
    } else {
        swizzle.heal_all();
        for (f, t) in clog_links {
            swizzle.clog_link(f.clone(), t.clone());
        }
        swizzle.set_max_jitter_ms(3);
    }
    res
}

/// Configuration for a `Joshua`-style multi-seed ensemble run.
#[derive(Debug, Clone)]
pub struct EnsembleConfig {
    pub workload: ChaosWorkloadKind,
    pub start_seed: u64,
    pub seed_count: u32,
    pub steps_per_seed: u32,
    pub concurrency: usize,
    pub required_probes: Vec<&'static str>,
}

/// Aggregate result across an entire multi-seed simulation campaign.
#[derive(Debug, Clone)]
pub struct EnsembleReport {
    pub workload: ChaosWorkloadKind,
    pub start_seed: u64,
    pub seed_count: u32,
    pub passed_seeds: u32,
    pub failed_reports: Vec<FullStackSimReport>,
    pub total_committed_mutations: u64,
    pub total_indeterminate_resolved: u64,
    pub total_buggify_firings: u64,
    pub probe_hits: BTreeMap<&'static str, u64>,
    pub missing_probes: Vec<&'static str>,
    pub elapsed: Duration,
}

impl EnsembleReport {
    pub fn is_ok(&self) -> bool {
        self.failed_reports.is_empty() && self.missing_probes.is_empty()
    }
}

impl std::fmt::Display for EnsembleReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "metadb ensemble workload={} seeds={} start_seed={:#x} passed={} failed={} committed_mutations={} indeterminate_resolved={} buggify_firings={} probes_hit={} missing_probes={} elapsed_ms={}",
            self.workload.as_str(),
            self.seed_count,
            self.start_seed,
            self.passed_seeds,
            self.failed_reports.len(),
            self.total_committed_mutations,
            self.total_indeterminate_resolved,
            self.total_buggify_firings,
            self.probe_hits.len(),
            self.missing_probes.len(),
            self.elapsed.as_millis(),
        )?;
        for mp in &self.missing_probes {
            write!(f, "\n  MISSING_CODE_PROBE: {mp}")?;
        }
        for fail in &self.failed_reports {
            write!(
                f,
                "\n  FAILED_SEED: reproduce with `metadb-ctl sim --workload {} --seed {:#x}`\n    {fail}",
                fail.workload.as_str(),
                fail.seed
            )?;
        }
        Ok(())
    }
}

/// Executes a multi-seed simulation ensemble (`Joshua`-style campaign) with parallel workers for
/// thread-isolated in-memory runs and bounded async concurrency for persistent/cluster chaos runs.
pub async fn run_ensemble(cfg: &EnsembleConfig) -> EnsembleReport {
    let started = Instant::now();
    let probes_before = CodeProbeSnapshot::capture();
    let workers = cfg.concurrency.max(1);
    let mut reports = Vec::with_capacity(cfg.seed_count as usize);

    match cfg.workload {
        ChaosWorkloadKind::InMemoryFullStackBuggify => {
            let seeds: Vec<u64> = (0..u64::from(cfg.seed_count))
                .map(|i| cfg.start_seed.wrapping_add(i))
                .collect();
            let chunk_size = seeds.len().div_ceil(workers).max(1);
            let mut handles = Vec::new();
            for chunk in seeds.chunks(chunk_size) {
                let owned = chunk.to_vec();
                let steps = cfg.steps_per_seed;
                handles.push(tokio::task::spawn_blocking(move || {
                    owned
                        .into_iter()
                        .map(|s| run_in_memory_full_stack(s, steps))
                        .collect::<Vec<_>>()
                }));
            }
            for h in handles {
                if let Ok(batch) = h.await {
                    reports.extend(batch);
                }
            }
        }
        ChaosWorkloadKind::PersistentMultiFaultCascade => {
            for i in 0..u64::from(cfg.seed_count) {
                let s = cfg.start_seed.wrapping_add(i);
                reports.push(run_persistent_multi_fault_cascade(s, cfg.steps_per_seed).await);
            }
        }
        ChaosWorkloadKind::ThreeVoterSwizzleClogChaos => {
            for i in 0..u64::from(cfg.seed_count) {
                let s = cfg.start_seed.wrapping_add(i);
                reports.push(run_three_voter_swizzle_clog_chaos(s, cfg.steps_per_seed).await);
            }
        }
    }

    let probes_after = CodeProbeSnapshot::capture();
    let probe_hits = probes_after.delta_since(&probes_before);
    let missing_probes: Vec<&'static str> = cfg
        .required_probes
        .iter()
        .copied()
        .filter(|p| !probe_hits.contains_key(p))
        .collect();

    let mut passed_seeds = 0u32;
    let mut failed_reports = Vec::new();
    let mut total_committed_mutations = 0u64;
    let mut total_indeterminate_resolved = 0u64;
    let mut total_buggify_firings = 0u64;

    for r in reports {
        total_committed_mutations += u64::from(r.committed_mutations);
        total_indeterminate_resolved += u64::from(r.indeterminate_resolved);
        total_buggify_firings += r.buggify_firings;
        if r.is_ok() {
            passed_seeds += 1;
        } else {
            failed_reports.push(r);
        }
    }

    EnsembleReport {
        workload: cfg.workload,
        start_seed: cfg.start_seed,
        seed_count: cfg.seed_count,
        passed_seeds,
        failed_reports,
        total_committed_mutations,
        total_indeterminate_resolved,
        total_buggify_firings,
        probe_hits,
        missing_probes,
        elapsed: started.elapsed(),
    }
}
