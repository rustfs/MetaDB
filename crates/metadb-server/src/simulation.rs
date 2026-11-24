//! Seeded workload catalog for MetaDB.
//!
//! This is a MetaDB-native harness. It borrows the *shape* of FoundationDB's
//! published testing design (a deterministic script, a fault, a workload name,
//! and a report that includes the seed) and implements that shape on the engines
//! this repository already has. It is not Flow, `fdbserver -r simulation`, Joshua,
//! TestHarness2, bindingtester, or Circus, and it does not copy FoundationDB source.
//!
//! Three execution layers are distinct:
//!
//! 1. **In-memory / same-process simulation.** [`crate::MetaDbEngine::new_single_node`]
//!    keeps the directory model in process memory. It does not open a data directory,
//!    a WAL-less LSM, or an OpenRaft group. The script and the resulting names,
//!    child counts, and byte totals are a pure function of `(workload id, seed)`.
//!    Hybrid-logical-clock values still come from the wall clock and are not part
//!    of the reproduced fingerprint.
//! 2. **Single-process persistence.** [`crate::PersistentMetaDbCluster`] on one
//!    data directory, including `flush_all` (one maintenance tick, which snapshots
//!    and purges a non-empty Raft log, then a memtable flush) and
//!    `shutdown_without_flush`, followed by another `open_and_serve` in the same
//!    OS process. The script and the fault index are the same function of the seed.
//!    OpenRaft and Tokio use wall-clock timers, so this layer does not claim a
//!    simulated clock or a bit-identical Raft trace.
//! 3. **Real multi-process / multi-node.** Not executed here. Network, machine,
//!    and datacenter faults need real peers. Official three-replica coverage/delete
//!    and concurrent `add_learner` stay deferred until the Aliyun 1-to-5 rehearsal.
//!    This module does not start those machines.
//!
//! A report with zero invariant violations is a scripted directory check. It is
//! not a first-ship pass, not the official million-operation load, and not
//! three-VM evidence. Trillion-scale figures in arXiv:2610.32247v1 are Baidu
//! production data, not results of this harness.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use metadb_core::{
    is_dir_inode_id, is_file_inode_id, make_file_inode_id, InodeId, UserId, ATTR_ENTRY_NAME,
    ROOT_INODE_ID,
};

use crate::{MetaDbConfig, MetaDbEngine, PersistentMetaDbCluster, ServerTomlConfig};

const FILE_BYTES: i64 = 128;
const FILE_UID: UserId = 7;
const FILE_MODE: u32 = 0o644;
const DIR_MODE: u32 = 0o755;
const MISSING_NAME: &str = "missing.dat";
const MIN_MUTATION_STEPS: usize = 4;
const MAX_VIOLATIONS: usize = 8;
const MAX_LIST_PAGES: usize = 16;

/// Where a workload is allowed to run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExecutionLayer {
    /// `MetaDbEngine::new_single_node`. No data directory, LSM, or OpenRaft.
    InMemorySameProcess,
    /// One `PersistentMetaDbCluster` process, including flush, purge, and reopen.
    SingleProcessPersistent,
    /// More than one OS process or machine. This harness does not run that layer.
    RealMultiProcess,
}

impl ExecutionLayer {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InMemorySameProcess => "in-memory-same-process",
            Self::SingleProcessPersistent => "single-process-persistent",
            Self::RealMultiProcess => "real-multi-process",
        }
    }
}

impl std::fmt::Display for ExecutionLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How the planner turns a seed into a fault. The concrete index is stored on [`Schedule`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultTemplate {
    /// No scripted fault.
    None,
    /// Skip one script step before it is sent. The engine never sees that call.
    /// This is a client that did not send the request. It is not a network partition
    /// and it is not FoundationDB buggify.
    ClientOmission,
    /// After a seeded number of mutation-kind steps, `shutdown_without_flush` and reopen.
    UnflushedRestart,
    /// After a seeded number of mutation-kind steps, flush, purge, shut down, and reopen.
    FlushPurgeRestart,
    /// After a seeded number of mutation-kind steps, trigger a forced PID split, flush, shut down, and reopen.
    SplitFlushRestart,
}

/// One CI workload. The step list is produced by [`plan_schedule`], not stored here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkloadSpec {
    pub id: &'static str,
    pub layer: ExecutionLayer,
    pub summary: &'static str,
    pub dir_count: u32,
    pub steps: u32,
    pub fault: FaultTemplate,
}

/// A multi-process capability this harness deliberately does not run.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeferredCapability {
    pub id: &'static str,
    pub layer: ExecutionLayer,
    pub reason: &'static str,
}

/// Workloads `cargo test` runs. None of them are real multi-process.
pub fn ci_workloads() -> &'static [WorkloadSpec] {
    &[
        WorkloadSpec {
            id: "metadata_crud",
            layer: ExecutionLayer::InMemorySameProcess,
            summary: "Seeded create, unlink, rename, lookup, list, and dirstat on the in-memory engine, with one client omission.",
            dir_count: 3,
            steps: 40,
            fault: FaultTemplate::ClientOmission,
        },
        WorkloadSpec {
            id: "metadata_crud_unflushed_restart",
            layer: ExecutionLayer::SingleProcessPersistent,
            summary: "The same script shape on one persistent process, then shutdown without a memtable flush and reopen of that data directory.",
            dir_count: 2,
            steps: 12,
            fault: FaultTemplate::UnflushedRestart,
        },
        WorkloadSpec {
            id: "metadata_crud_flush_purge_restart",
            layer: ExecutionLayer::SingleProcessPersistent,
            summary: "The same script shape, then flush_all (maintenance tick plus memtable flush), shutdown, and reopen of that data directory.",
            dir_count: 2,
            steps: 12,
            fault: FaultTemplate::FlushPurgeRestart,
        },
        WorkloadSpec {
            id: "metadata_setattr_cross_rename_split_restart",
            layer: ExecutionLayer::SingleProcessPersistent,
            summary: "Seeded create, setattr size update, cross-directory rename, online PID-boundary split, flush, and persistent reopen.",
            dir_count: 3,
            steps: 18,
            fault: FaultTemplate::SplitFlushRestart,
        },
    ]
}

pub fn ci_workload(id: &str) -> Option<&'static WorkloadSpec> {
    ci_workloads().iter().find(|spec| spec.id == id)
}

/// Capabilities that need a real multi-process run. They are not CI workloads and
/// they are not expected-to-fail tests. The Aliyun rehearsal is not started here.
pub fn deferred_capabilities() -> &'static [DeferredCapability] {
    &[
        DeferredCapability {
            id: "network_partition",
            layer: ExecutionLayer::RealMultiProcess,
            reason: "This process has no simulated network. A connection fault needs real peers.",
        },
        DeferredCapability {
            id: "machine_or_datacenter_fault",
            layer: ExecutionLayer::RealMultiProcess,
            reason: "Machine and datacenter faults need more than one process. Swizzle-clogging is a FoundationDB simulation pattern and is not implemented here.",
        },
        DeferredCapability {
            id: "three_replica_coverage_delete",
            layer: ExecutionLayer::RealMultiProcess,
            reason: "Deferred until the Aliyun 1-to-5 rehearsal. This harness does not start those machines.",
        },
        DeferredCapability {
            id: "concurrent_add_learner",
            layer: ExecutionLayer::RealMultiProcess,
            reason: "Deferred until the Aliyun 1-to-5 rehearsal. This harness does not start those machines.",
        },
        DeferredCapability {
            id: "official_million_op_load",
            layer: ExecutionLayer::RealMultiProcess,
            reason: "The official load is three VMs. A passing script in this catalog is not that load.",
        },
    ]
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepKind {
    Create,
    Unlink,
    Rename,
    SetAttrSize,
    CrossDirRename,
    Lookup,
    List,
    Stat,
}

impl StepKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Create => "create",
            Self::Unlink => "unlink",
            Self::Rename => "rename",
            Self::SetAttrSize => "setattr-size",
            Self::CrossDirRename => "cross-dir-rename",
            Self::Lookup => "lookup",
            Self::List => "list",
            Self::Stat => "stat",
        }
    }

    /// Create, unlink, and rename count toward the restart clock even when the
    /// directory is empty and the runner degrades unlink or rename into a stat.
    /// The clock is a property of the script, so the fault index does not depend
    /// on the engine.
    pub fn is_mutation(self) -> bool {
        matches!(
            self,
            Self::Create | Self::Unlink | Self::Rename | Self::SetAttrSize | Self::CrossDirRename
        )
    }

    fn from_roll(roll: u32) -> Self {
        match roll % 8 {
            0..=2 => Self::Create,
            3 => Self::Unlink,
            4 => Self::Rename,
            5 => Self::Lookup,
            6 => Self::List,
            _ => Self::Stat,
        }
    }

    fn from_extended_roll(roll: u32) -> Self {
        match roll % 10 {
            0..=2 => Self::Create,
            3 => Self::SetAttrSize,
            4 => Self::CrossDirRename,
            5 => Self::Unlink,
            6 => Self::Rename,
            7 => Self::Lookup,
            8 => Self::List,
            _ => Self::Stat,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedStep {
    pub index: u32,
    pub kind: StepKind,
    pub dir_slot: u32,
    pub omit: bool,
}

/// Concrete fault chosen from the seed. `after_mutation_steps` counts
/// [`StepKind::is_mutation`] steps already executed, not successful writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScriptedFault {
    None,
    ClientOmission { step_index: u32 },
    UnflushedRestart { after_mutation_steps: u32 },
    FlushPurgeRestart { after_mutation_steps: u32 },
    SplitFlushRestart { after_mutation_steps: u32 },
}

impl ScriptedFault {
    pub fn as_label(&self) -> String {
        match self {
            Self::None => "none".to_string(),
            Self::ClientOmission { step_index } => format!("client-omission step={step_index}"),
            Self::UnflushedRestart {
                after_mutation_steps,
            } => format!("unflushed-restart after_mutation_steps={after_mutation_steps}"),
            Self::FlushPurgeRestart {
                after_mutation_steps,
            } => format!("flush-purge-restart after_mutation_steps={after_mutation_steps}"),
            Self::SplitFlushRestart {
                after_mutation_steps,
            } => format!("split-flush-restart after_mutation_steps={after_mutation_steps}"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Schedule {
    pub workload_id: String,
    pub layer: ExecutionLayer,
    pub seed: u64,
    pub dir_count: u32,
    pub steps: Vec<PlannedStep>,
    pub fault: ScriptedFault,
}

impl Schedule {
    pub fn omitted_steps(&self) -> u32 {
        self.steps.iter().filter(|step| step.omit).count() as u32
    }

    pub fn mutation_steps(&self) -> u32 {
        self.steps
            .iter()
            .filter(|step| step.kind.is_mutation())
            .count() as u32
    }
}

/// SplitMix64. The constants are the public-domain mixer (Steele, Lea, Flood),
/// written here so the script does not depend on a new crate. This is not
/// FoundationDB's simulated random source.
struct SimRng {
    state: u64,
}

impl SimRng {
    fn new(seed: u64) -> Self {
        Self {
            state: seed ^ 0x9E37_79B9_7F4A_7C15,
        }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn gen_range(&mut self, n: u32) -> u32 {
        debug_assert!(n > 0);
        (self.next_u64() % u64::from(n)) as u32
    }
}

/// Builds the script for `spec` and `seed`. Calling it twice with the same
/// arguments returns the same schedule. It does not open an engine.
pub fn plan_schedule(spec: &WorkloadSpec, seed: u64) -> Schedule {
    assert!(
        spec.dir_count >= 1,
        "workload {} needs at least one directory",
        spec.id
    );
    let mut rng = SimRng::new(seed);
    let mut steps = Vec::with_capacity(spec.steps as usize);
    let extended = spec.id == "metadata_setattr_cross_rename_split_restart";
    for _ in 0..spec.steps {
        let kind = if extended {
            StepKind::from_extended_roll(rng.gen_range(10))
        } else {
            StepKind::from_roll(rng.gen_range(8))
        };
        let dir_slot = rng.gen_range(spec.dir_count);
        steps.push(PlannedStep {
            index: 0,
            kind,
            dir_slot,
            omit: false,
        });
    }
    while steps.iter().filter(|step| step.kind.is_mutation()).count() < MIN_MUTATION_STEPS {
        steps.push(PlannedStep {
            index: 0,
            kind: StepKind::Create,
            dir_slot: rng.gen_range(spec.dir_count),
            omit: false,
        });
    }
    for (index, step) in steps.iter_mut().enumerate() {
        step.index = index as u32;
    }

    let fault = match spec.fault {
        FaultTemplate::None => ScriptedFault::None,
        FaultTemplate::ClientOmission => {
            let step_index = rng.gen_range(steps.len() as u32);
            steps[step_index as usize].omit = true;
            ScriptedFault::ClientOmission { step_index }
        }
        FaultTemplate::UnflushedRestart => ScriptedFault::UnflushedRestart {
            after_mutation_steps: restart_after(&mut rng, &steps),
        },
        FaultTemplate::FlushPurgeRestart => ScriptedFault::FlushPurgeRestart {
            after_mutation_steps: restart_after(&mut rng, &steps),
        },
        FaultTemplate::SplitFlushRestart => ScriptedFault::SplitFlushRestart {
            after_mutation_steps: restart_after(&mut rng, &steps),
        },
    };

    Schedule {
        workload_id: spec.id.to_string(),
        layer: spec.layer,
        seed,
        dir_count: spec.dir_count,
        steps,
        fault,
    }
}

fn restart_after(rng: &mut SimRng, steps: &[PlannedStep]) -> u32 {
    let mutations = steps.iter().filter(|step| step.kind.is_mutation()).count() as u32;
    debug_assert!(mutations >= 2);
    1 + rng.gen_range(mutations - 1)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SimReport {
    pub workload_id: String,
    pub layer: ExecutionLayer,
    pub seed: u64,
    pub steps: u32,
    pub acked_mutations: u32,
    pub omitted_steps: u32,
    pub fault: String,
    pub invariant_violations: Vec<String>,
    pub model_fingerprint: String,
    pub trace_hash: u64,
    pub trace_tail: Vec<String>,
}

impl std::fmt::Display for SimReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "metadb simulation workload={} layer={} seed={:#x} steps={} acked_mutations={} omitted_steps={} fault={} invariant_violations={} trace_hash={:016x}",
            self.workload_id,
            self.layer,
            self.seed,
            self.steps,
            self.acked_mutations,
            self.omitted_steps,
            self.fault,
            self.invariant_violations.len(),
            self.trace_hash,
        )?;
        for violation in &self.invariant_violations {
            write!(f, "\n  {violation}")?;
        }
        for line in &self.trace_tail {
            write!(f, "\n  {line}")?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum PlannedAction {
    Omit,
    Create {
        slot: usize,
        name: String,
        file_seq: u64,
    },
    Unlink {
        slot: usize,
        name: String,
    },
    Rename {
        slot: usize,
        from: String,
        to: String,
    },
    SetAttrSize {
        slot: usize,
        name: String,
        new_size: i64,
    },
    CrossDirRename {
        src_slot: usize,
        dst_slot: usize,
        from: String,
        to: String,
    },
    LookupPresent {
        slot: usize,
        name: String,
        size: i64,
    },
    LookupAbsent {
        slot: usize,
    },
    List {
        slot: usize,
    },
    Stat {
        slot: usize,
    },
}

impl PlannedAction {
    fn is_acked_mutation(&self) -> bool {
        matches!(
            self,
            Self::Create { .. }
                | Self::Unlink { .. }
                | Self::Rename { .. }
                | Self::SetAttrSize { .. }
                | Self::CrossDirRename { .. }
        )
    }

    fn label(&self) -> String {
        match self {
            Self::Omit => "omit".to_string(),
            Self::Create { slot, name, .. } => format!("create slot={slot} name={name}"),
            Self::Unlink { slot, name } => format!("unlink slot={slot} name={name}"),
            Self::Rename { slot, from, to } => format!("rename slot={slot} {from}->{to}"),
            Self::SetAttrSize {
                slot,
                name,
                new_size,
            } => format!("setattr slot={slot} name={name} size={new_size}"),
            Self::CrossDirRename {
                src_slot,
                dst_slot,
                from,
                to,
            } => format!("cross-rename {src_slot}:{from}->{dst_slot}:{to}"),
            Self::LookupPresent { slot, name, size } => {
                format!("lookup slot={slot} name={name} size={size}")
            }
            Self::LookupAbsent { slot } => format!("lookup-absent slot={slot}"),
            Self::List { slot } => format!("list slot={slot}"),
            Self::Stat { slot } => format!("stat slot={slot}"),
        }
    }
}

struct DirModel {
    name: String,
    pid: InodeId,
    files: BTreeMap<String, i64>,
}

struct NamespaceModel {
    dirs: Vec<DirModel>,
    seq: u32,
}

impl NamespaceModel {
    fn fingerprint(&self) -> String {
        let mut out = String::from("root=");
        for (index, dir) in self.dirs.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            out.push_str(&dir.name);
        }
        for dir in &self.dirs {
            out.push('|');
            out.push_str(&dir.name);
            out.push('=');
            for (index, (name, size)) in dir.files.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(name);
                out.push(':');
                out.push_str(&size.to_string());
            }
        }
        out
    }
}

struct ObservedDir {
    names: BTreeSet<String>,
    children: i64,
    bytes: i64,
}

fn note(violations: &mut Vec<String>, message: String) {
    if violations.len() < MAX_VIOLATIONS {
        violations.push(message);
    }
}

fn alloc_name(model: &mut NamespaceModel, prefix: char) -> (String, u64) {
    let file_seq = u64::from(model.seq);
    let name = format!("{prefix}{file_seq:04}");
    model.seq = model.seq.saturating_add(1);
    (name, file_seq)
}

fn first_file(model: &NamespaceModel, slot: usize) -> Option<(String, i64)> {
    let dir = model.dirs.get(slot)?;
    dir.files
        .iter()
        .next()
        .map(|(name, size)| (name.clone(), *size))
}

fn decide(model: &mut NamespaceModel, step: &PlannedStep) -> PlannedAction {
    if step.omit {
        return PlannedAction::Omit;
    }
    let slot = step.dir_slot as usize;
    match step.kind {
        StepKind::Create => {
            let (name, file_seq) = alloc_name(model, 'f');
            PlannedAction::Create {
                slot,
                name,
                file_seq,
            }
        }
        StepKind::Unlink => match first_file(model, slot) {
            Some((name, _)) => PlannedAction::Unlink { slot, name },
            None => PlannedAction::Stat { slot },
        },
        StepKind::Rename => match first_file(model, slot) {
            Some((from, _)) => {
                let (to, _) = alloc_name(model, 'r');
                PlannedAction::Rename { slot, from, to }
            }
            None => PlannedAction::Stat { slot },
        },
        StepKind::SetAttrSize => match first_file(model, slot) {
            Some((name, cur_size)) => PlannedAction::SetAttrSize {
                slot,
                name,
                new_size: cur_size.saturating_add(64),
            },
            None => PlannedAction::Stat { slot },
        },
        StepKind::CrossDirRename => match first_file(model, slot) {
            Some((from, _)) if model.dirs.len() > 1 => {
                let dst_slot = (slot + 1) % model.dirs.len();
                let (to, _) = alloc_name(model, 'x');
                PlannedAction::CrossDirRename {
                    src_slot: slot,
                    dst_slot,
                    from,
                    to,
                }
            }
            _ => PlannedAction::Stat { slot },
        },
        StepKind::Lookup => match first_file(model, slot) {
            Some((name, size)) => PlannedAction::LookupPresent { slot, name, size },
            None => PlannedAction::LookupAbsent { slot },
        },
        StepKind::List => PlannedAction::List { slot },
        StepKind::Stat => PlannedAction::Stat { slot },
    }
}

fn commit(model: &mut NamespaceModel, action: &PlannedAction) {
    match action {
        PlannedAction::Create { slot, name, .. } => {
            model.dirs[*slot].files.insert(name.clone(), FILE_BYTES);
        }
        PlannedAction::Unlink { slot, name } => {
            model.dirs[*slot].files.remove(name);
        }
        PlannedAction::Rename { slot, from, to } => {
            let size = model.dirs[*slot].files.remove(from).unwrap_or(FILE_BYTES);
            model.dirs[*slot].files.insert(to.clone(), size);
        }
        PlannedAction::SetAttrSize {
            slot,
            name,
            new_size,
        } => {
            if let Some(entry) = model.dirs[*slot].files.get_mut(name) {
                *entry = *new_size;
            }
        }
        PlannedAction::CrossDirRename {
            src_slot,
            dst_slot,
            from,
            to,
        } => {
            let size = model.dirs[*src_slot]
                .files
                .remove(from)
                .unwrap_or(FILE_BYTES);
            model.dirs[*dst_slot].files.insert(to.clone(), size);
        }
        _ => {}
    }
}

fn compare_observation(
    label: &str,
    want_names: &BTreeSet<String>,
    want_children: i64,
    want_bytes: i64,
    got: &ObservedDir,
    violations: &mut Vec<String>,
) {
    if &got.names != want_names {
        let missing: Vec<&str> = want_names
            .difference(&got.names)
            .take(3)
            .map(String::as_str)
            .collect();
        let extra: Vec<&str> = got
            .names
            .difference(want_names)
            .take(3)
            .map(String::as_str)
            .collect();
        note(
            violations,
            format!("{label} listing mismatch missing={missing:?} extra={extra:?}"),
        );
    }
    if got.children != want_children || got.bytes != want_bytes {
        note(
            violations,
            format!(
                "{label} dirstat children={} bytes={} want_children={want_children} want_bytes={want_bytes}",
                got.children, got.bytes
            ),
        );
    }
}

fn names_of(files: &BTreeMap<String, i64>) -> BTreeSet<String> {
    files.keys().cloned().collect()
}

fn push_model_mismatches(
    model: &NamespaceModel,
    root: &ObservedDir,
    dirs: &[ObservedDir],
    where_checked: &str,
    violations: &mut Vec<String>,
) {
    let root_names: BTreeSet<String> = model.dirs.iter().map(|dir| dir.name.clone()).collect();
    compare_observation(
        &format!("{where_checked} root"),
        &root_names,
        model.dirs.len() as i64,
        0,
        root,
        violations,
    );
    if dirs.len() != model.dirs.len() {
        note(
            violations,
            format!(
                "{where_checked} observed {} directories, model has {}",
                dirs.len(),
                model.dirs.len()
            ),
        );
        return;
    }
    for (dir, got) in model.dirs.iter().zip(dirs.iter()) {
        let bytes = dir.files.values().copied().sum::<i64>();
        compare_observation(
            &format!("{where_checked} {}", dir.name),
            &names_of(&dir.files),
            dir.files.len() as i64,
            bytes,
            got,
            violations,
        );
    }
}

fn hash_trace(lines: &[String], fingerprint: &str) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for line in lines {
        for byte in line.as_bytes() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash ^= u64::from(b'\n');
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    for byte in fingerprint.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

fn finish_report(
    schedule: &Schedule,
    acked_mutations: u32,
    violations: Vec<String>,
    fingerprint: String,
    trace: &[String],
) -> SimReport {
    let trace_tail = if violations.is_empty() {
        Vec::new()
    } else {
        let start = trace.len().saturating_sub(16);
        trace[start..].to_vec()
    };
    SimReport {
        workload_id: schedule.workload_id.clone(),
        layer: schedule.layer,
        seed: schedule.seed,
        steps: schedule.steps.len() as u32,
        acked_mutations,
        omitted_steps: schedule.omitted_steps(),
        fault: schedule.fault.as_label(),
        invariant_violations: violations,
        trace_hash: hash_trace(trace, &fingerprint),
        model_fingerprint: fingerprint,
        trace_tail,
    }
}

fn layer_refusal(schedule: &Schedule, expected: ExecutionLayer) -> Option<SimReport> {
    if schedule.layer == expected {
        return None;
    }
    Some(SimReport {
        workload_id: schedule.workload_id.clone(),
        layer: schedule.layer,
        seed: schedule.seed,
        steps: schedule.steps.len() as u32,
        acked_mutations: 0,
        omitted_steps: schedule.omitted_steps(),
        fault: schedule.fault.as_label(),
        invariant_violations: vec![format!(
            "workload {} is {}; this runner accepts {expected}",
            schedule.workload_id, schedule.layer
        )],
        model_fingerprint: String::new(),
        trace_hash: 0,
        trace_tail: Vec::new(),
    })
}

fn dir_name(slot: u32) -> String {
    format!("simdir{slot:02}")
}

/// Runs `spec` on the in-memory engine. `spec.layer` must be
/// [`ExecutionLayer::InMemorySameProcess`].
pub fn run_in_memory(spec: &WorkloadSpec, seed: u64) -> SimReport {
    let schedule = plan_schedule(spec, seed);
    if let Some(refused) = layer_refusal(&schedule, ExecutionLayer::InMemorySameProcess) {
        return refused;
    }
    let engine = MetaDbEngine::new_single_node(MetaDbConfig::default());
    let mut violations = Vec::new();
    let mut trace = Vec::new();
    let Some(mut model) = setup_in_memory(&engine, schedule.dir_count, &mut violations) else {
        return finish_report(&schedule, 0, violations, String::new(), &trace);
    };
    trace.push(format!("setup dirs={}", model.dirs.len()));
    check_in_memory(&engine, &model, "after-setup", &mut violations);
    let mut acked = 0u32;
    for step in &schedule.steps {
        if violations.len() >= MAX_VIOLATIONS {
            break;
        }
        let action = decide(&mut model, step);
        match apply_in_memory(&engine, &model, &action) {
            Ok(()) => {
                if action.is_acked_mutation() {
                    acked += 1;
                }
                commit(&mut model, &action);
                trace.push(format!("step {} {} ok", step.index, action.label()));
            }
            Err(err) => {
                note(
                    &mut violations,
                    format!("step {} {} failed: {err}", step.index, action.label()),
                );
                trace.push(format!("step {} {} err", step.index, action.label()));
            }
        }
        check_in_memory(
            &engine,
            &model,
            &format!("step {}", step.index),
            &mut violations,
        );
    }
    let fingerprint = model.fingerprint();
    finish_report(&schedule, acked, violations, fingerprint, &trace)
}

fn setup_in_memory(
    engine: &MetaDbEngine,
    dir_count: u32,
    violations: &mut Vec<String>,
) -> Option<NamespaceModel> {
    let mut model = NamespaceModel {
        dirs: Vec::with_capacity(dir_count as usize),
        seq: 0,
    };
    for slot in 0..dir_count {
        let name = dir_name(slot);
        let pid = 100 + u64::from(slot);
        if let Err(err) = engine.mkdir(ROOT_INODE_ID, &name, pid, FILE_UID, DIR_MODE) {
            note(violations, format!("mkdir {name} failed: {err}"));
            return None;
        }
        model.dirs.push(DirModel {
            name,
            pid,
            files: BTreeMap::new(),
        });
    }
    Some(model)
}

fn apply_in_memory(
    engine: &MetaDbEngine,
    model: &NamespaceModel,
    action: &PlannedAction,
) -> Result<(), String> {
    match action {
        PlannedAction::Omit | PlannedAction::List { .. } | PlannedAction::Stat { .. } => {
            // List and stat are checked by the directory invariant after the step.
            // Issuing them here makes the script actually call those paths.
            if let PlannedAction::List { slot } | PlannedAction::Stat { slot } = action {
                let pid = pid_of(model, *slot)?;
                if matches!(action, PlannedAction::List { .. }) {
                    engine.listdir(pid).map_err(|err| err.to_string())?;
                } else {
                    engine.dirstat(pid).map_err(|err| err.to_string())?;
                }
            }
            Ok(())
        }
        PlannedAction::Create {
            slot,
            name,
            file_seq,
        } => {
            let pid = pid_of(model, *slot)?;
            let inode = make_file_inode_id(file_seq.saturating_add(1));
            let fid = file_seq.saturating_add(1);
            let entry = engine
                .create_file(pid, name, inode, fid, FILE_UID, FILE_MODE, FILE_BYTES)
                .map_err(|err| err.to_string())?;
            if entry.attr.size_bytes != FILE_BYTES {
                return Err(format!(
                    "create {name} returned size {}",
                    entry.attr.size_bytes
                ));
            }
            Ok(())
        }
        PlannedAction::Unlink { slot, name } => {
            let pid = pid_of(model, *slot)?;
            engine
                .unlink(pid, name)
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
        PlannedAction::Rename { slot, from, to } => {
            let pid = pid_of(model, *slot)?;
            engine
                .rename(pid, from, pid, to)
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
        PlannedAction::SetAttrSize {
            slot,
            name,
            new_size,
        } => {
            let pid = pid_of(model, *slot)?;
            engine
                .setattr(
                    pid,
                    name,
                    metadb_tx::SetAttrPatch {
                        size_bytes: Some(*new_size),
                        ..Default::default()
                    },
                )
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
        PlannedAction::CrossDirRename {
            src_slot,
            dst_slot,
            from,
            to,
        } => {
            let src_pid = pid_of(model, *src_slot)?;
            let dst_pid = pid_of(model, *dst_slot)?;
            engine
                .rename(src_pid, from, dst_pid, to)
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
        PlannedAction::LookupPresent { slot, name, size } => {
            let pid = pid_of(model, *slot)?;
            match engine.lookup(pid, name).map_err(|err| err.to_string())? {
                Some(entry) if entry.attr.size_bytes == *size => Ok(()),
                Some(entry) => Err(format!(
                    "lookup {name} size {} want {size}",
                    entry.attr.size_bytes
                )),
                None => Err(format!("lookup {name} missing")),
            }
        }
        PlannedAction::LookupAbsent { slot } => {
            let pid = pid_of(model, *slot)?;
            match engine
                .lookup(pid, MISSING_NAME)
                .map_err(|err| err.to_string())?
            {
                None => Ok(()),
                Some(_) => Err(format!("lookup {MISSING_NAME} unexpectedly present")),
            }
        }
    }
}

fn pid_of(model: &NamespaceModel, slot: usize) -> Result<InodeId, String> {
    model
        .dirs
        .get(slot)
        .map(|dir| dir.pid)
        .ok_or_else(|| format!("directory slot {slot} is not in the model"))
}

fn observe_in_memory(engine: &MetaDbEngine, pid: InodeId) -> Result<ObservedDir, String> {
    let entries = engine.listdir(pid).map_err(|err| err.to_string())?;
    let mut names = BTreeSet::new();
    for entry in entries {
        if entry.key.name == ATTR_ENTRY_NAME {
            return Err(format!("listdir pid={pid} included {ATTR_ENTRY_NAME}"));
        }
        names.insert(entry.key.name);
    }
    let stat = engine.dirstat(pid).map_err(|err| err.to_string())?;
    Ok(ObservedDir {
        names,
        children: stat.num_children,
        bytes: stat.size_bytes,
    })
}

fn check_in_memory(
    engine: &MetaDbEngine,
    model: &NamespaceModel,
    where_checked: &str,
    violations: &mut Vec<String>,
) {
    let root = match observe_in_memory(engine, ROOT_INODE_ID) {
        Ok(root) => root,
        Err(err) => {
            note(
                violations,
                format!("{where_checked} root observe failed: {err}"),
            );
            return;
        }
    };
    let mut dirs = Vec::with_capacity(model.dirs.len());
    for dir in &model.dirs {
        match observe_in_memory(engine, dir.pid) {
            Ok(got) => dirs.push(got),
            Err(err) => {
                note(
                    violations,
                    format!("{where_checked} {} observe failed: {err}", dir.name),
                );
                return;
            }
        }
    }
    push_model_mismatches(model, &root, &dirs, where_checked, violations);
}

/// Runs `spec` on one persistent process. `spec.layer` must be
/// [`ExecutionLayer::SingleProcessPersistent`]. The caller owns `config`'s
/// data directory and keeps it alive until this function returns.
///
/// A restart fault shuts this process's cluster down and calls `open_and_serve`
/// again on the same directory. That is a second open in the same OS process,
/// not a second machine.
pub async fn run_persistent(
    spec: &WorkloadSpec,
    seed: u64,
    config: &ServerTomlConfig,
) -> SimReport {
    let schedule = plan_schedule(spec, seed);
    if let Some(refused) = layer_refusal(&schedule, ExecutionLayer::SingleProcessPersistent) {
        return refused;
    }
    let opened = PersistentMetaDbCluster::open_and_serve(config.clone()).await;
    let mut cluster = match opened {
        Ok((cluster, _addr)) => cluster,
        Err(err) => {
            return finish_report(
                &schedule,
                0,
                vec![format!("open_and_serve failed: {err}")],
                String::new(),
                &[],
            );
        }
    };
    let mut violations = Vec::new();
    let mut trace = Vec::new();
    let Some(mut model) = setup_persistent(&cluster, schedule.dir_count, &mut violations).await
    else {
        let _ = cluster.shutdown().await;
        return finish_report(&schedule, 0, violations, String::new(), &trace);
    };
    trace.push(format!("setup dirs={}", model.dirs.len()));
    check_persistent(&cluster, &model, "after-setup", &mut violations).await;

    let mut acked = 0u32;
    let mut mutation_steps = 0u32;
    let mut restarted = false;
    for step in &schedule.steps {
        if violations.len() >= MAX_VIOLATIONS {
            break;
        }
        if let Some(kind) = restart_due(&schedule.fault, mutation_steps, restarted) {
            match restart_process(cluster, config, kind).await {
                Ok(reopened) => {
                    cluster = reopened;
                    restarted = true;
                    trace.push(format!("reopen {}", kind.as_str()));
                    check_persistent(&cluster, &model, "after-reopen", &mut violations).await;
                }
                Err(err) => {
                    note(&mut violations, format!("restart failed: {err}"));
                    return finish_report(
                        &schedule,
                        acked,
                        violations,
                        model.fingerprint(),
                        &trace,
                    );
                }
            }
        }
        let action = decide(&mut model, step);
        match apply_persistent(&cluster, &model, &action).await {
            Ok(()) => {
                if action.is_acked_mutation() {
                    acked += 1;
                }
                commit(&mut model, &action);
                trace.push(format!("step {} {} ok", step.index, action.label()));
            }
            Err(err) => {
                note(
                    &mut violations,
                    format!("step {} {} failed: {err}", step.index, action.label()),
                );
                trace.push(format!("step {} {} err", step.index, action.label()));
            }
        }
        if step.kind.is_mutation() {
            mutation_steps += 1;
        }
        check_persistent(
            &cluster,
            &model,
            &format!("step {}", step.index),
            &mut violations,
        )
        .await;
    }
    if !restarted
        && matches!(
            schedule.fault,
            ScriptedFault::UnflushedRestart { .. }
                | ScriptedFault::FlushPurgeRestart { .. }
                | ScriptedFault::SplitFlushRestart { .. }
        )
    {
        note(
            &mut violations,
            "restart fault did not run; the script ended before the fault index".to_string(),
        );
    }
    let fingerprint = model.fingerprint();
    if let Err(err) = cluster.shutdown().await {
        note(&mut violations, format!("final shutdown failed: {err}"));
    }
    finish_report(&schedule, acked, violations, fingerprint, &trace)
}

#[derive(Clone, Copy)]
enum RestartKind {
    Unflushed,
    FlushPurge,
    SplitFlush,
}

impl RestartKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Unflushed => "unflushed",
            Self::FlushPurge => "flush-purge",
            Self::SplitFlush => "split-flush",
        }
    }
}

fn restart_due(fault: &ScriptedFault, mutation_steps: u32, restarted: bool) -> Option<RestartKind> {
    if restarted {
        return None;
    }
    match *fault {
        ScriptedFault::UnflushedRestart {
            after_mutation_steps,
        } if mutation_steps == after_mutation_steps => Some(RestartKind::Unflushed),
        ScriptedFault::FlushPurgeRestart {
            after_mutation_steps,
        } if mutation_steps == after_mutation_steps => Some(RestartKind::FlushPurge),
        ScriptedFault::SplitFlushRestart {
            after_mutation_steps,
        } if mutation_steps == after_mutation_steps => Some(RestartKind::SplitFlush),
        _ => None,
    }
}

async fn restart_process(
    cluster: Arc<PersistentMetaDbCluster>,
    config: &ServerTomlConfig,
    kind: RestartKind,
) -> Result<Arc<PersistentMetaDbCluster>, String> {
    let shutdown_result = match kind {
        RestartKind::Unflushed => cluster
            .shutdown_without_flush()
            .await
            .map_err(|err| err.to_string()),
        RestartKind::FlushPurge => match cluster.flush_all().await {
            Ok(()) => cluster.shutdown().await.map_err(|err| err.to_string()),
            Err(err) => {
                let message = err.to_string();
                let _ = cluster.shutdown_without_flush().await;
                Err(message)
            }
        },
        RestartKind::SplitFlush => {
            let _ = cluster
                .cell_manager()
                .maybe_split_or_isolate_shard(1, true)
                .await;
            match cluster.flush_all().await {
                Ok(()) => cluster.shutdown().await.map_err(|err| err.to_string()),
                Err(err) => {
                    let message = err.to_string();
                    let _ = cluster.shutdown_without_flush().await;
                    Err(message)
                }
            }
        }
    };
    // Drop the open cluster before the next `open_and_serve` so the LSM lock is released.
    drop(cluster);
    shutdown_result?;
    let (reopened, _addr) = PersistentMetaDbCluster::open_and_serve(config.clone())
        .await
        .map_err(|err| err.to_string())?;
    Ok(reopened)
}

async fn setup_persistent(
    cluster: &PersistentMetaDbCluster,
    dir_count: u32,
    violations: &mut Vec<String>,
) -> Option<NamespaceModel> {
    let mut model = NamespaceModel {
        dirs: Vec::with_capacity(dir_count as usize),
        seq: 0,
    };
    for slot in 0..dir_count {
        let name = dir_name(slot);
        match cluster
            .mkdir_auto_id(ROOT_INODE_ID, &name, FILE_UID, DIR_MODE)
            .await
        {
            Ok(entry) => {
                if !is_dir_inode_id(entry.attr.inode_id) {
                    note(
                        violations,
                        format!(
                            "mkdir {name} returned inode {:#x}, which is not a directory id",
                            entry.attr.inode_id
                        ),
                    );
                }
                model.dirs.push(DirModel {
                    name,
                    pid: entry.attr.inode_id,
                    files: BTreeMap::new(),
                });
            }
            Err(err) => {
                note(violations, format!("mkdir {name} failed: {err}"));
                return None;
            }
        }
    }
    Some(model)
}

async fn apply_persistent(
    cluster: &PersistentMetaDbCluster,
    model: &NamespaceModel,
    action: &PlannedAction,
) -> Result<(), String> {
    let tx = cluster.tx_router();
    match action {
        PlannedAction::Omit | PlannedAction::List { .. } | PlannedAction::Stat { .. } => {
            if let PlannedAction::List { slot } | PlannedAction::Stat { slot } = action {
                let pid = pid_of(model, *slot)?;
                if matches!(action, PlannedAction::List { .. }) {
                    tx.listdir_paginated(pid, None, None, 64)
                        .await
                        .map_err(|err| err.to_string())?;
                } else {
                    tx.dirstat(pid).await.map_err(|err| err.to_string())?;
                }
            }
            Ok(())
        }
        PlannedAction::Create { slot, name, .. } => {
            let pid = pid_of(model, *slot)?;
            let entry = cluster
                .create_file_auto_id(pid, name, FILE_UID, FILE_MODE, FILE_BYTES)
                .await
                .map_err(|err| err.to_string())?;
            if !is_file_inode_id(entry.attr.inode_id) {
                return Err(format!(
                    "create {name} returned inode {:#x}, which is not a file id",
                    entry.attr.inode_id
                ));
            }
            if entry.attr.size_bytes != FILE_BYTES {
                return Err(format!(
                    "create {name} returned size {}",
                    entry.attr.size_bytes
                ));
            }
            Ok(())
        }
        PlannedAction::Unlink { slot, name } => {
            let pid = pid_of(model, *slot)?;
            tx.unlink(pid, name)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
        PlannedAction::Rename { slot, from, to } => {
            let pid = pid_of(model, *slot)?;
            tx.rename(pid, from, pid, to, false)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
        PlannedAction::SetAttrSize {
            slot,
            name,
            new_size,
        } => {
            let pid = pid_of(model, *slot)?;
            tx.setattr(
                pid,
                name,
                metadb_tx::SetAttrPatch {
                    size_bytes: Some(*new_size),
                    ..Default::default()
                },
            )
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
        }
        PlannedAction::CrossDirRename {
            src_slot,
            dst_slot,
            from,
            to,
        } => {
            let src_pid = pid_of(model, *src_slot)?;
            let dst_pid = pid_of(model, *dst_slot)?;
            tx.rename(src_pid, from, dst_pid, to, false)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        }
        PlannedAction::LookupPresent { slot, name, size } => {
            let pid = pid_of(model, *slot)?;
            match tx.lookup(pid, name).await.map_err(|err| err.to_string())? {
                Some(entry) if entry.attr.size_bytes == *size => Ok(()),
                Some(entry) => Err(format!(
                    "lookup {name} size {} want {size}",
                    entry.attr.size_bytes
                )),
                None => Err(format!("lookup {name} missing")),
            }
        }
        PlannedAction::LookupAbsent { slot } => {
            let pid = pid_of(model, *slot)?;
            match tx
                .lookup(pid, MISSING_NAME)
                .await
                .map_err(|err| err.to_string())?
            {
                None => Ok(()),
                Some(_) => Err(format!("lookup {MISSING_NAME} unexpectedly present")),
            }
        }
    }
}

async fn observe_persistent(
    cluster: &PersistentMetaDbCluster,
    pid: InodeId,
) -> Result<ObservedDir, String> {
    let tx = cluster.tx_router();
    let mut names = BTreeSet::new();
    let mut cursor: Option<String> = None;
    for _ in 0..MAX_LIST_PAGES {
        let (page, next) = tx
            .listdir_paginated(pid, cursor.as_deref(), None, 64)
            .await
            .map_err(|err| err.to_string())?;
        if page.is_empty() {
            break;
        }
        for entry in page {
            if entry.key.name == ATTR_ENTRY_NAME {
                return Err(format!("listdir pid={pid} included {ATTR_ENTRY_NAME}"));
            }
            names.insert(entry.key.name);
        }
        match next {
            Some(next) => cursor = Some(next),
            None => {
                cursor = None;
                break;
            }
        }
    }
    if cursor.is_some() {
        return Err(format!(
            "listdir pid={pid} did not finish within {MAX_LIST_PAGES} pages"
        ));
    }
    let stat = tx.dirstat(pid).await.map_err(|err| err.to_string())?;
    Ok(ObservedDir {
        names,
        children: stat.num_children,
        bytes: stat.size_bytes,
    })
}

async fn check_persistent(
    cluster: &PersistentMetaDbCluster,
    model: &NamespaceModel,
    where_checked: &str,
    violations: &mut Vec<String>,
) {
    let root = match observe_persistent(cluster, ROOT_INODE_ID).await {
        Ok(root) => root,
        Err(err) => {
            note(
                violations,
                format!("{where_checked} root observe failed: {err}"),
            );
            return;
        }
    };
    let mut dirs = Vec::with_capacity(model.dirs.len());
    for dir in &model.dirs {
        match observe_persistent(cluster, dir.pid).await {
            Ok(got) => dirs.push(got),
            Err(err) => {
                note(
                    violations,
                    format!("{where_checked} {} observe failed: {err}", dir.name),
                );
                return;
            }
        }
    }
    push_model_mismatches(model, &root, &dirs, where_checked, violations);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schedule_is_a_pure_function_of_workload_and_seed() {
        let spec = ci_workload("metadata_crud").unwrap();
        assert_eq!(plan_schedule(spec, 1), plan_schedule(spec, 1));
        assert_ne!(plan_schedule(spec, 1), plan_schedule(spec, 2));
    }

    #[test]
    fn client_omission_skips_exactly_one_step() {
        let spec = ci_workload("metadata_crud").unwrap();
        let schedule = plan_schedule(spec, 0x00C0_FFEE);
        assert_eq!(schedule.layer, ExecutionLayer::InMemorySameProcess);
        assert_eq!(schedule.omitted_steps(), 1);
        match schedule.fault {
            ScriptedFault::ClientOmission { step_index } => {
                assert!(schedule.steps[step_index as usize].omit);
            }
            other => panic!("expected a client omission, got {other:?}"),
        }
    }

    #[test]
    fn restart_fault_sits_between_mutation_steps() {
        for id in [
            "metadata_crud_unflushed_restart",
            "metadata_crud_flush_purge_restart",
        ] {
            let spec = ci_workload(id).unwrap();
            assert_eq!(spec.layer, ExecutionLayer::SingleProcessPersistent);
            let schedule = plan_schedule(spec, 11);
            let after = match schedule.fault {
                ScriptedFault::UnflushedRestart {
                    after_mutation_steps,
                }
                | ScriptedFault::FlushPurgeRestart {
                    after_mutation_steps,
                } => after_mutation_steps,
                other => panic!("{id} planned {other:?}"),
            };
            let mutations = schedule.mutation_steps();
            assert!(after >= 1, "{id} after={after}");
            assert!(
                after < mutations,
                "{id} after={after} mutations={mutations}"
            );
        }
    }

    #[test]
    fn ci_catalog_leaves_real_multi_process_deferred() {
        let running: BTreeSet<&str> = ci_workloads().iter().map(|spec| spec.id).collect();
        assert!(running
            .iter()
            .all(|id| { ci_workload(id).unwrap().layer != ExecutionLayer::RealMultiProcess }));
        for deferred in deferred_capabilities() {
            assert_eq!(deferred.layer, ExecutionLayer::RealMultiProcess);
            assert!(
                !running.contains(deferred.id),
                "{} is both a CI workload and a deferred capability",
                deferred.id
            );
        }
    }

    #[test]
    fn directory_checker_reports_a_wrong_child_count() {
        let want = BTreeSet::from(["a.dat".to_string()]);
        let got = ObservedDir {
            names: want.clone(),
            children: 0,
            bytes: FILE_BYTES,
        };
        let mut violations = Vec::new();
        compare_observation("simdir00", &want, 1, FILE_BYTES, &got, &mut violations);
        assert_eq!(violations.len(), 1, "{violations:?}");
        assert!(violations[0].contains("want_children=1"));
    }

    #[test]
    fn in_memory_runner_refuses_a_persistent_workload() {
        let spec = ci_workload("metadata_crud_unflushed_restart").unwrap();
        let report = run_in_memory(spec, 1);
        assert_eq!(report.invariant_violations.len(), 1, "{report}");
        assert!(report.invariant_violations[0].contains("single-process-persistent"));
    }
}
