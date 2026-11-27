//! Jepsen-grade (`https://jepsen.io/`) formal consistency checkers, EDN history recorder,
//! real multi-process `metadbd` OS-cluster nemesis harness, and 3-voter `Swizzle-Clog`
//! network-partition Jepsen runner for MetaDB.
//!
//! # Formal Checkers Implemented
//! 1. **Wing-Gong-Lowe (WGL / Knossos / Porcupine) Exact Linearizability Checker**:
//!    Exact bitset-memoized search over concurrent histories with `:invoke`, `:ok`, `:fail`,
//!    and indeterminate `:info` operations (per-key partitioned for multi-key histories).
//! 2. **Elle Transactional Dependency-Graph Checker**:
//!    Constructs the Adya Direct Serialization Graph (`ww`, `wr`, `rw`, `rt`) and detects
//!    `G0` (Dirty Write), `G1a` (Aborted Read), `G1b` (Intermediate / Fractured Atomic Read),
//!    `G1c` (Circular Information Flow), `G-single`, `G2-item`, and `Strict-Serializability`
//!    (`rt`) cycles via Tarjan SCC + minimal cycle extraction.
//! 3. **Jepsen `set-full` Durability Checker**:
//!    Verifies zero lost acknowledged writes (`lost_ok == 0`) and zero aborted/phantom writes
//!    (`unexpected == 0`) across crash/restart and network partition nemeses.
//! 4. **Jepsen `bank` Conservation Checker**:
//!    Verifies exact total balance / byte conservation and non-negativity across concurrent
//!    multi-account cross-shard transfers and reads.
//! 5. **Standard Jepsen EDN History Exporter (`history.edn`)**:
//!    Writes standard Clojure EDN histories directly consumable by official `jepsen`,
//!    `knossos`, and `elle` tooling.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use metadb_core::buggify::splitmix64;
use metadb_core::{make_file_inode_id, BuggifyConfig, GlobalBuggifyGuard, ROOT_INODE_ID};
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::metadata_service_client::MetadataServiceClient;
use metadb_proto::transaction_service_client::TransactionServiceClient;
use metadb_proto::{
    CreateFileRequest, DirStatRequest, GetQuotaRequest, GetRoutesRequest, ListDirRequest,
    LookupRequest, MkdirRequest, RenameRequest, SetAttrRequest, TransferShardLeaderRequest,
    TriggerShardSplitRequest,
};
use metadb_raft::SwizzleClogController;
use metadb_tx::{SetAttrPatch, ShardDirectory};
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};

use crate::{BearerAuthClient, PersistentMetaDbCluster, ServerTomlConfig};

// ---------------------------------------------------------------------------
// 1. Canonical Jepsen History Model & Clojure EDN Exporter
// ---------------------------------------------------------------------------

/// Event type matching Jepsen's `:invoke`, `:ok`, `:fail`, `:info` lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum JepsenOpType {
    Invoke,
    Ok,
    Fail,
    Info,
}

impl JepsenOpType {
    pub fn as_edn(self) -> &'static str {
        match self {
            Self::Invoke => ":invoke",
            Self::Ok => ":ok",
            Self::Fail => ":fail",
            Self::Info => ":info",
        }
    }
}

/// Process identifier in a Jepsen history (`:nemesis` or numeric client worker id).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum JepsenProcess {
    Client(u32),
    Nemesis,
}

impl JepsenProcess {
    pub fn as_edn(self) -> String {
        match self {
            Self::Client(id) => id.to_string(),
            Self::Nemesis => ":nemesis".to_string(),
        }
    }
}

/// Logical operation function (`:f`) recorded in a Jepsen history.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JepsenOpKind {
    Read {
        key: u64,
    },
    Write {
        key: u64,
        value: i64,
    },
    Cas {
        key: u64,
        from: i64,
        to: i64,
    },
    SetAdd {
        item: u64,
    },
    SetRead,
    /// Atomic move/versioned write across two slots (`src_slot` -> `dst_slot`).
    TxnRename {
        txn_id: u64,
        src_slot: u64,
        dst_slot: u64,
        version: u64,
    },
    /// Read both endpoints of an atomic rename pair to check for fractured/intermediate states.
    TxnReadPair {
        txn_id: u64,
        slot_a: u64,
        slot_b: u64,
    },
    /// Multi-account transfer (`from` -> `to` of `amount` bytes/units).
    BankTransfer {
        from: u64,
        to: u64,
        amount: i64,
    },
    /// Read snapshot across all bank accounts.
    BankReadAll,
    NemesisStart {
        action: String,
    },
    NemesisStop {
        action: String,
    },
}

impl JepsenOpKind {
    pub fn edn_fn(&self) -> &'static str {
        match self {
            Self::Read { .. } => ":read",
            Self::Write { .. } => ":write",
            Self::Cas { .. } => ":cas",
            Self::SetAdd { .. } => ":add",
            Self::SetRead => ":read-set",
            Self::TxnRename { .. } => ":txn-rename",
            Self::TxnReadPair { .. } => ":txn-read-pair",
            Self::BankTransfer { .. } => ":transfer",
            Self::BankReadAll => ":read-bank",
            Self::NemesisStart { .. } => ":start-nemesis",
            Self::NemesisStop { .. } => ":stop-nemesis",
        }
    }
}

/// Result value (`:value`) recorded on an event in a Jepsen history.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum JepsenOpValue {
    None,
    Int(i64),
    OptInt(Option<i64>),
    CasResult {
        from: i64,
        to: i64,
        swapped: bool,
    },
    SetValues(Vec<u64>),
    /// Observed `(slot_a_version, slot_b_version)` for atomic pair reads.
    PairRead {
        val_a: Option<u64>,
        val_b: Option<u64>,
    },
    /// Atomic rename outcome with committed version tag.
    RenameCommitted {
        version: u64,
    },
    /// Observed account balances `(account_id, balance)`.
    Balances(Vec<(u64, i64)>),
    Text(String),
}

/// Single event line in a canonical Jepsen history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JepsenEvent {
    pub index: usize,
    pub time_ns: u64,
    pub process: JepsenProcess,
    pub op_type: JepsenOpType,
    pub kind: JepsenOpKind,
    pub value: JepsenOpValue,
}

impl JepsenEvent {
    /// Renders this event in standard Jepsen Clojure EDN map syntax.
    pub fn to_edn_line(&self) -> String {
        let val_edn = match (&self.kind, &self.value) {
            (JepsenOpKind::Read { key }, JepsenOpValue::OptInt(opt)) => match opt {
                Some(v) => format!("[{key} {v}]"),
                None => format!("[{key} nil]"),
            },
            (JepsenOpKind::Read { key }, _) => format!("[{key} nil]"),
            (JepsenOpKind::Write { key, value }, _) => format!("[{key} {value}]"),
            (JepsenOpKind::Cas { key, from, to }, JepsenOpValue::CasResult { swapped, .. }) => {
                format!("{{:key {key} :from {from} :to {to} :swapped {swapped}}}")
            }
            (JepsenOpKind::Cas { key, from, to }, _) => {
                format!("{{:key {key} :from {from} :to {to}}}")
            }
            (JepsenOpKind::SetAdd { item }, _) => format!("{item}"),
            (JepsenOpKind::SetRead, JepsenOpValue::SetValues(items)) => {
                let s = items
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(" ");
                format!("#{{{s}}}")
            }
            (
                JepsenOpKind::TxnRename {
                    txn_id,
                    src_slot,
                    dst_slot,
                    version,
                },
                _,
            ) => {
                format!("{{:txn {txn_id} :src {src_slot} :dst {dst_slot} :ver {version}}}")
            }
            (
                JepsenOpKind::TxnReadPair {
                    txn_id,
                    slot_a,
                    slot_b,
                },
                JepsenOpValue::PairRead { val_a, val_b },
            ) => {
                let a = val_a.map_or_else(|| "nil".to_string(), |v| v.to_string());
                let b = val_b.map_or_else(|| "nil".to_string(), |v| v.to_string());
                format!("{{:txn {txn_id} :a [{slot_a} {a}] :b [{slot_b} {b}]}}")
            }
            (
                JepsenOpKind::TxnReadPair {
                    txn_id,
                    slot_a,
                    slot_b,
                },
                _,
            ) => format!("{{:txn {txn_id} :a [{slot_a} nil] :b [{slot_b} nil]}}"),
            (JepsenOpKind::BankTransfer { from, to, amount }, _) => {
                format!("{{:from {from} :to {to} :amount {amount}}}")
            }
            (JepsenOpKind::BankReadAll, JepsenOpValue::Balances(bals)) => {
                let s = bals
                    .iter()
                    .map(|(k, v)| format!("{k} {v}"))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("{{{s}}}")
            }
            (JepsenOpKind::NemesisStart { action }, _)
            | (JepsenOpKind::NemesisStop { action }, _) => {
                format!("\"{}\"", action.replace('"', "\\\""))
            }
            (_, JepsenOpValue::Text(s)) => format!("\"{}\"", s.replace('"', "\\\"")),
            (_, JepsenOpValue::Int(v)) => v.to_string(),
            _ => "nil".to_string(),
        };
        format!(
            "{{:index {} :time {} :process {} :type {} :f {} :value {}}}",
            self.index,
            self.time_ns,
            self.process.as_edn(),
            self.op_type.as_edn(),
            self.kind.edn_fn(),
            val_edn
        )
    }
}

/// Complete Jepsen history recorded across all concurrent clients and nemeses.
#[derive(Debug, Clone, Default)]
pub struct JepsenHistory {
    pub events: Vec<JepsenEvent>,
}

impl JepsenHistory {
    pub fn len(&self) -> usize {
        self.events.len()
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Exports the history as newline-delimited Clojure EDN maps (`history.edn`).
    pub fn to_edn(&self) -> String {
        let mut out = String::with_capacity(self.events.len() * 96);
        for ev in &self.events {
            out.push_str(&ev.to_edn_line());
            out.push('\n');
        }
        out
    }

    /// Writes `history.edn` to disk.
    pub fn write_edn_file(&self, path: &Path) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        std::fs::write(path, self.to_edn())
    }

    /// Computes a deterministic 64-bit FNV-1a digest of the logical history (excluding wall times
    /// when `include_time == false`, or including virtual timestamps when `include_time == true`).
    pub fn digest(&self, include_time: bool) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let mix = |acc: &mut u64, bytes: &[u8]| {
            for &b in bytes {
                *acc ^= u64::from(b);
                *acc = acc.wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        for ev in &self.events {
            mix(&mut h, &(ev.index as u64).to_le_bytes());
            if include_time {
                mix(&mut h, &ev.time_ns.to_le_bytes());
            }
            mix(&mut h, ev.to_edn_line().as_bytes());
        }
        h
    }
}

/// Thread-safe recorder for wall-clock or virtual-clock Jepsen histories.
#[derive(Clone)]
pub struct HistoryRecorder {
    started: Instant,
    events: Arc<Mutex<Vec<JepsenEvent>>>,
}

impl Default for HistoryRecorder {
    fn default() -> Self {
        Self::new()
    }
}

impl HistoryRecorder {
    pub fn new() -> Self {
        Self {
            started: Instant::now(),
            events: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn now_ns(&self) -> u64 {
        self.started.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }

    pub fn invoke(&self, process: JepsenProcess, kind: JepsenOpKind) -> usize {
        self.record_at(
            self.now_ns(),
            process,
            JepsenOpType::Invoke,
            kind,
            JepsenOpValue::None,
        )
    }

    pub fn invoke_at(&self, time_ns: u64, process: JepsenProcess, kind: JepsenOpKind) -> usize {
        self.record_at(
            time_ns,
            process,
            JepsenOpType::Invoke,
            kind,
            JepsenOpValue::None,
        )
    }

    pub fn complete_ok(
        &self,
        process: JepsenProcess,
        kind: JepsenOpKind,
        value: JepsenOpValue,
    ) -> usize {
        self.record_at(self.now_ns(), process, JepsenOpType::Ok, kind, value)
    }

    pub fn complete_ok_at(
        &self,
        time_ns: u64,
        process: JepsenProcess,
        kind: JepsenOpKind,
        value: JepsenOpValue,
    ) -> usize {
        self.record_at(time_ns, process, JepsenOpType::Ok, kind, value)
    }

    pub fn complete_fail(
        &self,
        process: JepsenProcess,
        kind: JepsenOpKind,
        reason: impl Into<String>,
    ) -> usize {
        self.record_at(
            self.now_ns(),
            process,
            JepsenOpType::Fail,
            kind,
            JepsenOpValue::Text(reason.into()),
        )
    }

    pub fn complete_fail_at(
        &self,
        time_ns: u64,
        process: JepsenProcess,
        kind: JepsenOpKind,
        reason: impl Into<String>,
    ) -> usize {
        self.record_at(
            time_ns,
            process,
            JepsenOpType::Fail,
            kind,
            JepsenOpValue::Text(reason.into()),
        )
    }

    pub fn complete_info(
        &self,
        process: JepsenProcess,
        kind: JepsenOpKind,
        reason: impl Into<String>,
    ) -> usize {
        self.record_at(
            self.now_ns(),
            process,
            JepsenOpType::Info,
            kind,
            JepsenOpValue::Text(reason.into()),
        )
    }

    pub fn complete_info_at(
        &self,
        time_ns: u64,
        process: JepsenProcess,
        kind: JepsenOpKind,
        reason: impl Into<String>,
    ) -> usize {
        self.record_at(
            time_ns,
            process,
            JepsenOpType::Info,
            kind,
            JepsenOpValue::Text(reason.into()),
        )
    }

    pub fn nemesis_start(&self, action: impl Into<String>) -> usize {
        let action = action.into();
        self.record_at(
            self.now_ns(),
            JepsenProcess::Nemesis,
            JepsenOpType::Info,
            JepsenOpKind::NemesisStart { action },
            JepsenOpValue::None,
        )
    }

    pub fn nemesis_stop(&self, action: impl Into<String>) -> usize {
        let action = action.into();
        self.record_at(
            self.now_ns(),
            JepsenProcess::Nemesis,
            JepsenOpType::Info,
            JepsenOpKind::NemesisStop { action },
            JepsenOpValue::None,
        )
    }

    pub fn record_at(
        &self,
        time_ns: u64,
        process: JepsenProcess,
        op_type: JepsenOpType,
        kind: JepsenOpKind,
        value: JepsenOpValue,
    ) -> usize {
        let mut guard = self.events.lock().unwrap();
        let index = guard.len();
        guard.push(JepsenEvent {
            index,
            time_ns,
            process,
            op_type,
            kind,
            value,
        });
        index
    }

    pub fn snapshot(&self) -> JepsenHistory {
        JepsenHistory {
            events: self.events.lock().unwrap().clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// 2. Wing-Gong-Lowe (WGL / Knossos / Porcupine) Exact Linearizability Checker
// ---------------------------------------------------------------------------

/// Sequential specification for a linearizable object (register, CAS cell, KV, etc.).
pub trait LinearizableModel {
    type State: Clone + Eq + Hash;
    type Input: Clone;
    type Output: Clone;

    fn init(&self) -> Self::State;

    /// Returns `Some(next_state)` if applying `(input, output)` to `state` is legal.
    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: &Self::Output,
    ) -> Option<Self::State>;
}

/// Standard Jepsen single-key linearizable register model supporting `Read`, `Write`, and `Cas`.
#[derive(Debug, Clone, Copy)]
pub struct RegisterModel {
    pub initial: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterInput {
    Read,
    Write(i64),
    Cas { from: i64, to: i64 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterOutput {
    ReadVal(Option<i64>),
    WriteAck,
    CasAck(bool),
}

impl LinearizableModel for RegisterModel {
    type State = Option<i64>;
    type Input = RegisterInput;
    type Output = RegisterOutput;

    fn init(&self) -> Self::State {
        self.initial
    }

    fn step(
        &self,
        state: &Self::State,
        input: &Self::Input,
        output: &Self::Output,
    ) -> Option<Self::State> {
        match (input, output) {
            (RegisterInput::Read, RegisterOutput::ReadVal(obs)) => {
                if state == obs {
                    Some(*state)
                } else {
                    None
                }
            }
            (RegisterInput::Write(v), RegisterOutput::WriteAck) => Some(Some(*v)),
            (RegisterInput::Cas { from, to }, RegisterOutput::CasAck(swapped)) => {
                if *swapped {
                    if *state == Some(*from) {
                        Some(Some(*to))
                    } else {
                        None
                    }
                } else if *state != Some(*from) {
                    Some(*state)
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

/// Completed or indeterminate operation interval used by the WGL linearizability solver.
#[derive(Debug, Clone)]
pub struct CallInterval<I, O> {
    pub op_id: usize,
    pub invoke_time: u64,
    /// `u64::MAX` for `:info` (indeterminate/crashed) operations.
    pub return_time: u64,
    /// `true` for `:info` operations (may linearize at any time `>= invoke_time` or never take effect).
    pub indeterminate: bool,
    pub input: I,
    pub output: O,
}

/// Result returned by [`check_linearizability_with_model`] / [`check_register_linearizability`].
#[derive(Debug, Clone)]
pub struct LinearizabilityResult {
    pub linearizable: bool,
    pub checked_keys: usize,
    pub operations_checked: usize,
    pub indeterminate_ops: usize,
    pub search_steps: usize,
    pub witness_order: Vec<usize>,
    pub violation: Option<String>,
}

/// Variable-width compact bitset supporting arbitrarily many operations in WGL memoization.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DynBitSet {
    words: Vec<u64>,
}

impl DynBitSet {
    fn new(bits: usize) -> Self {
        let len = bits.div_ceil(64).max(1);
        Self {
            words: vec![0u64; len],
        }
    }

    #[inline]
    fn get(&self, idx: usize) -> bool {
        (self.words[idx / 64] & (1u64 << (idx % 64))) != 0
    }

    #[inline]
    fn set(&mut self, idx: usize) {
        self.words[idx / 64] |= 1u64 << (idx % 64);
    }

    #[inline]
    fn clear(&mut self, idx: usize) {
        self.words[idx / 64] &= !(1u64 << (idx % 64));
    }
}

fn hash_state<T: Hash>(s: &T) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

/// Exact Wing-Gong-Lowe (WGL / Porcupine) backtracking linearizability checker with
/// `(linearized_bitset, state_hash)` memoization and full `:info` indeterminate operation support.
///
/// Soundness & completeness:
/// - Every `:ok` operation (`indeterminate == false`) MUST linearize in `[invoke_time, return_time]`.
/// - Every `:info` operation (`indeterminate == true`) has `return_time == u64::MAX` and may either
///   linearize at any point `>= invoke_time` or never take effect.
/// - At every search step, any next operation to linearize must have `invoke_time <= min_return`,
///   where `min_return` is the smallest `return_time` among all still-unlinearized `:ok` operations.
pub fn check_linearizability_with_model<M: LinearizableModel>(
    model: &M,
    mut calls: Vec<CallInterval<M::Input, M::Output>>,
    max_search_steps: usize,
) -> LinearizabilityResult {
    calls.sort_by_key(|c| (c.invoke_time, c.return_time, c.op_id));
    let n = calls.len();
    let total_ok = calls.iter().filter(|c| !c.indeterminate).count();
    let indeterminate_ops = n - total_ok;
    if total_ok == 0 {
        return LinearizabilityResult {
            linearizable: true,
            checked_keys: 1,
            operations_checked: n,
            indeterminate_ops,
            search_steps: 0,
            witness_order: Vec::new(),
            violation: None,
        };
    }

    let mut linearized = DynBitSet::new(n);
    let mut failed_memo: HashSet<(DynBitSet, u64)> = HashSet::new();
    let mut witness: Vec<usize> = Vec::with_capacity(n);
    let mut steps = 0usize;
    let mut budget_exceeded = false;

    #[allow(clippy::too_many_arguments)]
    fn dfs<M: LinearizableModel>(
        model: &M,
        calls: &[CallInterval<M::Input, M::Output>],
        state: &M::State,
        remaining_ok: usize,
        linearized: &mut DynBitSet,
        failed_memo: &mut HashSet<(DynBitSet, u64)>,
        witness: &mut Vec<usize>,
        steps: &mut usize,
        max_steps: usize,
        budget_exceeded: &mut bool,
    ) -> bool {
        if remaining_ok == 0 {
            return true;
        }
        *steps += 1;
        if *steps > max_steps {
            *budget_exceeded = true;
            return false;
        }

        let memo_key = (linearized.clone(), hash_state(state));
        if failed_memo.contains(&memo_key) {
            return false;
        }

        // Compute `min_return` across all remaining required `:ok` operations.
        let mut min_return = u64::MAX;
        for (idx, call) in calls.iter().enumerate() {
            if !call.indeterminate && !linearized.get(idx) && call.return_time < min_return {
                min_return = call.return_time;
            }
        }

        // Candidate heuristic (Porcupine optimization): try required `:ok` ops ordered by earliest
        // `return_time` first, then eligible `:info` ops ordered by earliest `invoke_time`.
        let mut candidates: Vec<usize> = Vec::new();
        for (idx, call) in calls.iter().enumerate() {
            if !linearized.get(idx) && call.invoke_time <= min_return {
                candidates.push(idx);
            }
        }
        candidates.sort_by_key(|&i| {
            let c = &calls[i];
            (c.indeterminate, c.return_time, c.invoke_time, c.op_id)
        });

        for idx in candidates {
            let call = &calls[idx];
            if let Some(next_state) = model.step(state, &call.input, &call.output) {
                linearized.set(idx);
                witness.push(call.op_id);
                let next_rem = if call.indeterminate {
                    remaining_ok
                } else {
                    remaining_ok - 1
                };
                if dfs(
                    model,
                    calls,
                    &next_state,
                    next_rem,
                    linearized,
                    failed_memo,
                    witness,
                    steps,
                    max_steps,
                    budget_exceeded,
                ) {
                    return true;
                }
                witness.pop();
                linearized.clear(idx);
                if *budget_exceeded {
                    return false;
                }
            }
        }

        failed_memo.insert(memo_key);
        false
    }

    let init_state = model.init();
    let ok = dfs(
        model,
        &calls,
        &init_state,
        total_ok,
        &mut linearized,
        &mut failed_memo,
        &mut witness,
        &mut steps,
        max_search_steps,
        &mut budget_exceeded,
    );

    let violation = if ok {
        None
    } else if budget_exceeded {
        Some(format!(
            "linearizability search exceeded budget ({max_search_steps} steps) across {n} ops"
        ))
    } else {
        Some(format!(
            "non-linearizable history detected across {n} ops ({total_ok} ok, {indeterminate_ops} info) after {steps} search states"
        ))
    };

    LinearizabilityResult {
        linearizable: ok,
        checked_keys: 1,
        operations_checked: n,
        indeterminate_ops,
        search_steps: steps,
        witness_order: witness,
        violation,
    }
}

/// Checks multi-key register linearizability (`Read`, `Write`, `Cas`) over a `JepsenHistory`
/// by partitioning operations independently per key (exact per-key WGL decomposition).
pub fn check_register_linearizability(
    history: &JepsenHistory,
    initial_values: &BTreeMap<u64, i64>,
) -> LinearizabilityResult {
    // Pair invokes with completions per client process.
    let mut inflight: HashMap<JepsenProcess, (usize, u64, JepsenOpKind)> = HashMap::new();
    let mut per_key: BTreeMap<u64, Vec<CallInterval<RegisterInput, RegisterOutput>>> =
        BTreeMap::new();

    for ev in &history.events {
        if ev.process == JepsenProcess::Nemesis {
            continue;
        }
        match ev.op_type {
            JepsenOpType::Invoke => {
                inflight.insert(ev.process, (ev.index, ev.time_ns, ev.kind.clone()));
            }
            JepsenOpType::Fail => {
                // Failed operations proved they did not commit.
                inflight.remove(&ev.process);
            }
            JepsenOpType::Ok => {
                let (op_id, invoke_time, inv_kind) =
                    inflight
                        .remove(&ev.process)
                        .unwrap_or((ev.index, ev.time_ns, ev.kind.clone()));
                match inv_kind {
                    JepsenOpKind::Read { key } => {
                        let obs = match &ev.value {
                            JepsenOpValue::OptInt(v) => *v,
                            JepsenOpValue::Int(v) => Some(*v),
                            _ => None,
                        };
                        per_key.entry(key).or_default().push(CallInterval {
                            op_id,
                            invoke_time,
                            return_time: ev.time_ns,
                            indeterminate: false,
                            input: RegisterInput::Read,
                            output: RegisterOutput::ReadVal(obs),
                        });
                    }
                    JepsenOpKind::Write { key, value } => {
                        per_key.entry(key).or_default().push(CallInterval {
                            op_id,
                            invoke_time,
                            return_time: ev.time_ns,
                            indeterminate: false,
                            input: RegisterInput::Write(value),
                            output: RegisterOutput::WriteAck,
                        });
                    }
                    JepsenOpKind::Cas { key, from, to } => {
                        let swapped =
                            matches!(ev.value, JepsenOpValue::CasResult { swapped: true, .. });
                        per_key.entry(key).or_default().push(CallInterval {
                            op_id,
                            invoke_time,
                            return_time: ev.time_ns,
                            indeterminate: false,
                            input: RegisterInput::Cas { from, to },
                            output: RegisterOutput::CasAck(swapped),
                        });
                    }
                    _ => {}
                }
            }
            JepsenOpType::Info => {
                let (op_id, invoke_time, inv_kind) =
                    inflight
                        .remove(&ev.process)
                        .unwrap_or((ev.index, ev.time_ns, ev.kind.clone()));
                match inv_kind {
                    JepsenOpKind::Write { key, value } => {
                        per_key.entry(key).or_default().push(CallInterval {
                            op_id,
                            invoke_time,
                            return_time: u64::MAX,
                            indeterminate: true,
                            input: RegisterInput::Write(value),
                            output: RegisterOutput::WriteAck,
                        });
                    }
                    JepsenOpKind::Cas { key, from, to } => {
                        // Indeterminate CAS may have committed (`swapped = true`) or not taken effect.
                        per_key.entry(key).or_default().push(CallInterval {
                            op_id,
                            invoke_time,
                            return_time: u64::MAX,
                            indeterminate: true,
                            input: RegisterInput::Cas { from, to },
                            output: RegisterOutput::CasAck(true),
                        });
                    }
                    // Indeterminate read has no side effect and no observed return value.
                    _ => {}
                }
            }
        }
    }

    // Any still-inflight writes/CAS at history end are treated as `:info`.
    for (_, (op_id, invoke_time, inv_kind)) in inflight {
        match inv_kind {
            JepsenOpKind::Write { key, value } => {
                per_key.entry(key).or_default().push(CallInterval {
                    op_id,
                    invoke_time,
                    return_time: u64::MAX,
                    indeterminate: true,
                    input: RegisterInput::Write(value),
                    output: RegisterOutput::WriteAck,
                });
            }
            JepsenOpKind::Cas { key, from, to } => {
                per_key.entry(key).or_default().push(CallInterval {
                    op_id,
                    invoke_time,
                    return_time: u64::MAX,
                    indeterminate: true,
                    input: RegisterInput::Cas { from, to },
                    output: RegisterOutput::CasAck(true),
                });
            }
            _ => {}
        }
    }

    let mut total_ops = 0usize;
    let mut total_indet = 0usize;
    let mut total_steps = 0usize;
    let mut checked_keys = 0usize;
    let mut combined_witness = Vec::new();

    for (key, calls) in per_key {
        checked_keys += 1;
        let model = RegisterModel {
            initial: initial_values.get(&key).copied(),
        };
        let res = check_linearizability_with_model(&model, calls, 500_000);
        total_ops += res.operations_checked;
        total_indet += res.indeterminate_ops;
        total_steps += res.search_steps;
        if !res.linearizable {
            return LinearizabilityResult {
                linearizable: false,
                checked_keys,
                operations_checked: total_ops,
                indeterminate_ops: total_indet,
                search_steps: total_steps,
                witness_order: Vec::new(),
                violation: Some(format!(
                    "key={key}: {}",
                    res.violation.unwrap_or_else(|| "non-linearizable".into())
                )),
            };
        }
        combined_witness.extend(res.witness_order);
    }

    LinearizabilityResult {
        linearizable: true,
        checked_keys,
        operations_checked: total_ops,
        indeterminate_ops: total_indet,
        search_steps: total_steps,
        witness_order: combined_witness,
        violation: None,
    }
}

// ---------------------------------------------------------------------------
// 3. Elle Transactional Dependency-Graph Checker (G0, G1a, G1b, G1c, G-single, G2-item, RT)
// ---------------------------------------------------------------------------

/// Adya/Elle dependency edge kind in the Direct Serialization Graph (DSG).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElleEdgeKind {
    /// Write-write version order (`T1` wrote version `v1` overwritten by `T2`'s `v2`).
    Ww,
    /// Write-read dependency (`T2` observed version written by `T1`).
    Wr,
    /// Read-write anti-dependency (`T1` read version `v` later overwritten by `T2`).
    Rw,
    /// Real-time precedence (`T1.complete_ns < T2.invoke_ns`) for Strict Serializability.
    Rt,
}

impl fmt::Display for ElleEdgeKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ww => f.write_str("ww"),
            Self::Wr => f.write_str("wr"),
            Self::Rw => f.write_str("rw"),
            Self::Rt => f.write_str("rt"),
        }
    }
}

/// Adya & Elle transactional isolation anomaly classification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ElleAnomalyKind {
    /// Dirty Write: cycle of `ww` edges.
    G0,
    /// Aborted Read: transaction read a write from an aborted (`:fail`) transaction.
    G1a,
    /// Intermediate / Fractured Atomic Read: observed half-committed state (e.g., rename present
    /// at both source and destination, or absent from both).
    G1b,
    /// Circular Information Flow: cycle of `ww` and `wr` edges.
    G1c,
    /// Single anti-dependency cycle (`ww`/`wr`/`rw` with exactly 1 `rw` edge).
    GSingle,
    /// Item anti-dependency cycle (`ww`/`wr`/`rw` with >= 2 `rw` edges — read skew / write skew).
    G2Item,
    /// Real-time cycle violating Strict Serializability (`ww`/`wr`/`rw`/`rt` containing `rt`).
    StrictSerializability,
}

impl fmt::Display for ElleAnomalyKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::G0 => f.write_str("G0 (Dirty Write)"),
            Self::G1a => f.write_str("G1a (Aborted Read)"),
            Self::G1b => f.write_str("G1b (Intermediate/Fractured Read)"),
            Self::G1c => f.write_str("G1c (Circular Information Flow)"),
            Self::GSingle => f.write_str("G-single (Single Anti-Dependency Cycle)"),
            Self::G2Item => f.write_str("G2-item (Anti-Dependency / Skew Cycle)"),
            Self::StrictSerializability => f.write_str("Strict-Serializability (Real-Time Cycle)"),
        }
    }
}

/// Single micro-op inside an Elle RW-register / atomic-move transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ElleMicroOp {
    Read { key: u64, value: Option<u64> },
    Write { key: u64, value: u64 },
    Delete { key: u64 },
}

/// One transaction vertex in the Elle DSG.
#[derive(Debug, Clone)]
pub struct ElleTransaction {
    pub txn_id: u64,
    pub invoke_ns: u64,
    pub complete_ns: u64,
    pub committed: bool,
    pub failed: bool,
    pub ops: Vec<ElleMicroOp>,
}

/// Detected Elle anomaly with exact cycle path or witness details.
#[derive(Debug, Clone)]
pub struct ElleAnomaly {
    pub kind: ElleAnomalyKind,
    pub description: String,
}

/// Summary report from [`check_elle_graph`] / [`check_elle_rename_history`].
#[derive(Debug, Clone)]
pub struct ElleCheckResult {
    pub valid: bool,
    pub transactions_checked: usize,
    pub edges_ww: usize,
    pub edges_wr: usize,
    pub edges_rw: usize,
    pub edges_rt: usize,
    pub anomalies: Vec<ElleAnomaly>,
}

/// Finds a minimal cycle in `adj` restricted to edges satisfying `allow_edge`, optionally requiring
/// at least one edge satisfying `require_edge`.
fn find_cycle(
    nodes: &[u64],
    adj: &BTreeMap<u64, Vec<(u64, ElleEdgeKind)>>,
    allow_edge: impl Fn(ElleEdgeKind) -> bool,
    require_edge: impl Fn(ElleEdgeKind) -> bool,
) -> Option<Vec<(u64, ElleEdgeKind, u64)>> {
    // 1. Tarjan SCC on subgraph restricted to `allow_edge`.
    let mut index_counter = 0usize;
    let mut indices: HashMap<u64, usize> = HashMap::new();
    let mut lowlink: HashMap<u64, usize> = HashMap::new();
    let mut stack: Vec<u64> = Vec::new();
    let mut on_stack: HashSet<u64> = HashSet::new();
    let mut sccs: Vec<Vec<u64>> = Vec::new();

    #[allow(clippy::too_many_arguments)]
    fn strongconnect(
        v: u64,
        adj: &BTreeMap<u64, Vec<(u64, ElleEdgeKind)>>,
        allow: &dyn Fn(ElleEdgeKind) -> bool,
        index_counter: &mut usize,
        indices: &mut HashMap<u64, usize>,
        lowlink: &mut HashMap<u64, usize>,
        stack: &mut Vec<u64>,
        on_stack: &mut HashSet<u64>,
        sccs: &mut Vec<Vec<u64>>,
    ) {
        indices.insert(v, *index_counter);
        lowlink.insert(v, *index_counter);
        *index_counter += 1;
        stack.push(v);
        on_stack.insert(v);

        if let Some(edges) = adj.get(&v) {
            for &(w, kind) in edges {
                if !allow(kind) {
                    continue;
                }
                if !indices.contains_key(&w) {
                    strongconnect(
                        w,
                        adj,
                        allow,
                        index_counter,
                        indices,
                        lowlink,
                        stack,
                        on_stack,
                        sccs,
                    );
                    let lw = lowlink[&w];
                    let lv = lowlink[&v];
                    lowlink.insert(v, lv.min(lw));
                } else if on_stack.contains(&w) {
                    let iw = indices[&w];
                    let lv = lowlink[&v];
                    lowlink.insert(v, lv.min(iw));
                }
            }
        }

        if lowlink[&v] == indices[&v] {
            let mut comp = Vec::new();
            while let Some(w) = stack.pop() {
                on_stack.remove(&w);
                comp.push(w);
                if w == v {
                    break;
                }
            }
            sccs.push(comp);
        }
    }

    for &v in nodes {
        if !indices.contains_key(&v) {
            strongconnect(
                v,
                adj,
                &allow_edge,
                &mut index_counter,
                &mut indices,
                &mut lowlink,
                &mut stack,
                &mut on_stack,
                &mut sccs,
            );
        }
    }

    // 2. For each SCC containing a valid edge matching `require_edge`, extract a shortest cycle via BFS.
    for comp in sccs {
        let comp_set: HashSet<u64> = comp.iter().copied().collect();
        for &u in &comp {
            let Some(edges) = adj.get(&u) else {
                continue;
            };
            for &(v, kind) in edges {
                if !allow_edge(kind) || !require_edge(kind) || !comp_set.contains(&v) {
                    continue;
                }
                if u == v {
                    return Some(vec![(u, kind, v)]);
                }
                // BFS shortest path from v back to u inside `comp_set`.
                let mut q = VecDeque::new();
                let mut parent: HashMap<u64, (u64, ElleEdgeKind)> = HashMap::new();
                q.push_back(v);
                let mut found = false;
                while let Some(cur) = q.pop_front() {
                    if cur == u {
                        found = true;
                        break;
                    }
                    if let Some(nexts) = adj.get(&cur) {
                        for &(nxt, ek) in nexts {
                            if allow_edge(ek)
                                && comp_set.contains(&nxt)
                                && nxt != v
                                && !parent.contains_key(&nxt)
                            {
                                parent.insert(nxt, (cur, ek));
                                q.push_back(nxt);
                            }
                        }
                    }
                }
                if found {
                    let mut path = vec![(u, kind, v)];
                    let mut rev_segs = Vec::new();
                    let mut cur = u;
                    while cur != v {
                        let (prev, ek) = parent[&cur];
                        rev_segs.push((prev, ek, cur));
                        cur = prev;
                    }
                    rev_segs.reverse();
                    path.extend(rev_segs);
                    return Some(path);
                }
            }
        }
    }
    None
}

fn format_cycle(cycle: &[(u64, ElleEdgeKind, u64)]) -> String {
    cycle
        .iter()
        .map(|(u, k, v)| format!("T{u} --{k}--> T{v}"))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Checks an explicit set of Elle transactions (with explicit version-order `ww` hints or
/// monotonic unique versions per key) for `G0`, `G1a`, `G1b`, `G1c`, `G-single`, `G2-item`,
/// and `Strict-Serializability` (`rt`) violations!
///
/// `key_version_orders`: map `key -> ordered list of committed versions [v1, v2, ...]`.
/// If omitted for a key, unique written versions on that key are ordered ascending (`v1 < v2`).
pub fn check_elle_graph(
    txns: &[ElleTransaction],
    key_version_orders: &BTreeMap<u64, Vec<u64>>,
) -> ElleCheckResult {
    let mut anomalies = Vec::new();
    let mut writer_of: HashMap<(u64, u64), u64> = HashMap::new();
    let mut aborted_writes: HashSet<(u64, u64)> = HashSet::new();
    let mut key_writers: BTreeMap<u64, Vec<(u64, u64)>> = BTreeMap::new();
    let mut committed_txns: Vec<&ElleTransaction> = Vec::new();

    for t in txns {
        if t.failed {
            for op in &t.ops {
                if let ElleMicroOp::Write { key, value } = op {
                    aborted_writes.insert((*key, *value));
                }
            }
            continue;
        }
        if !t.committed {
            continue;
        }
        committed_txns.push(t);
        for op in &t.ops {
            if let ElleMicroOp::Write { key, value } = op {
                writer_of.insert((*key, *value), t.txn_id);
                key_writers
                    .entry(*key)
                    .or_default()
                    .push((*value, t.txn_id));
            }
        }
    }

    // Check G1a (Aborted Read) across all committed transactions.
    for t in &committed_txns {
        for op in &t.ops {
            if let ElleMicroOp::Read {
                key,
                value: Some(v),
            } = op
            {
                if aborted_writes.contains(&(*key, *v)) {
                    anomalies.push(ElleAnomaly {
                        kind: ElleAnomalyKind::G1a,
                        description: format!(
                            "T{} read aborted write (key={}, value={})",
                            t.txn_id, key, v
                        ),
                    });
                }
            }
        }
    }

    // Build version order index per key: `version -> ordinal`.
    let mut version_order_by_key: BTreeMap<u64, Vec<(u64, u64)>> = BTreeMap::new();
    for (key, mut writers) in key_writers {
        if let Some(explicit) = key_version_orders.get(&key) {
            let mut ordered = Vec::new();
            for &ver in explicit {
                if let Some(&tid) = writer_of.get(&(key, ver)) {
                    ordered.push((ver, tid));
                }
            }
            version_order_by_key.insert(key, ordered);
        } else {
            writers.sort_by_key(|&(ver, _)| ver);
            writers.dedup();
            version_order_by_key.insert(key, writers);
        }
    }

    let mut adj: BTreeMap<u64, Vec<(u64, ElleEdgeKind)>> = BTreeMap::new();
    let mut seen_edges: HashSet<(u64, u64, ElleEdgeKind)> = HashSet::new();
    let mut edges_ww = 0usize;
    let mut edges_wr = 0usize;
    let mut edges_rw = 0usize;
    let mut edges_rt = 0usize;

    let mut add_edge = |u: u64, v: u64, kind: ElleEdgeKind| {
        if u == v && kind == ElleEdgeKind::Rt {
            return;
        }
        if seen_edges.insert((u, v, kind)) {
            adj.entry(u).or_default().push((v, kind));
            match kind {
                ElleEdgeKind::Ww => edges_ww += 1,
                ElleEdgeKind::Wr => edges_wr += 1,
                ElleEdgeKind::Rw => edges_rw += 1,
                ElleEdgeKind::Rt => edges_rt += 1,
            }
        }
    };

    // 1. Add `ww` edges along each key's committed version order.
    for versions in version_order_by_key.values() {
        for pair in versions.windows(2) {
            let t1 = pair[0].1;
            let t2 = pair[1].1;
            if t1 != t2 {
                add_edge(t1, t2, ElleEdgeKind::Ww);
            }
        }
    }

    // 2. Add `wr` and `rw` (anti-dependency) edges from committed reads.
    for t in &committed_txns {
        for op in &t.ops {
            if let ElleMicroOp::Read { key, value } = op {
                match value {
                    Some(v) => {
                        if let Some(&writer_tid) = writer_of.get(&(*key, *v)) {
                            if writer_tid != t.txn_id {
                                add_edge(writer_tid, t.txn_id, ElleEdgeKind::Wr);
                            }
                        }
                        // Any subsequent version in `key`'s version order creates `T --rw--> T_next`.
                        if let Some(versions) = version_order_by_key.get(key) {
                            if let Some(pos) = versions.iter().position(|&(ver, _)| ver == *v) {
                                if let Some(&(_, next_writer)) = versions.get(pos + 1) {
                                    if next_writer != t.txn_id {
                                        add_edge(t.txn_id, next_writer, ElleEdgeKind::Rw);
                                    }
                                }
                            }
                        }
                    }
                    None => {
                        // Reading `None` (initial/deleted state before first committed version)
                        // anti-depends on the first committed version for that key.
                        if let Some(versions) = version_order_by_key.get(key) {
                            if let Some(&(_, first_writer)) = versions.first() {
                                if first_writer != t.txn_id {
                                    add_edge(t.txn_id, first_writer, ElleEdgeKind::Rw);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // 3. Add `rt` (real-time precedence) edges: `T1.complete_ns < T2.invoke_ns`.
    for i in 0..committed_txns.len() {
        for j in 0..committed_txns.len() {
            if i == j {
                continue;
            }
            let t1 = committed_txns[i];
            let t2 = committed_txns[j];
            if t1.complete_ns < t2.invoke_ns {
                add_edge(t1.txn_id, t2.txn_id, ElleEdgeKind::Rt);
            }
        }
    }

    let nodes: Vec<u64> = committed_txns.iter().map(|t| t.txn_id).collect();

    // Check G0 (`ww` only).
    if let Some(cycle) = find_cycle(
        &nodes,
        &adj,
        |k| k == ElleEdgeKind::Ww,
        |k| k == ElleEdgeKind::Ww,
    ) {
        anomalies.push(ElleAnomaly {
            kind: ElleAnomalyKind::G0,
            description: format!("G0 cycle: {}", format_cycle(&cycle)),
        });
    }

    // Check G1c (`ww | wr`).
    if anomalies.is_empty() {
        if let Some(cycle) = find_cycle(
            &nodes,
            &adj,
            |k| matches!(k, ElleEdgeKind::Ww | ElleEdgeKind::Wr),
            |k| k == ElleEdgeKind::Wr,
        ) {
            anomalies.push(ElleAnomaly {
                kind: ElleAnomalyKind::G1c,
                description: format!("G1c cycle: {}", format_cycle(&cycle)),
            });
        }
    }

    // Check G-single / G2-item (`ww | wr | rw`).
    if anomalies.is_empty() {
        if let Some(cycle) = find_cycle(
            &nodes,
            &adj,
            |k| matches!(k, ElleEdgeKind::Ww | ElleEdgeKind::Wr | ElleEdgeKind::Rw),
            |k| k == ElleEdgeKind::Rw,
        ) {
            let rw_count = cycle
                .iter()
                .filter(|(_, k, _)| *k == ElleEdgeKind::Rw)
                .count();
            let kind = if rw_count == 1 {
                ElleAnomalyKind::GSingle
            } else {
                ElleAnomalyKind::G2Item
            };
            anomalies.push(ElleAnomaly {
                kind,
                description: format!("{kind} cycle: {}", format_cycle(&cycle)),
            });
        }
    }

    // Check Strict Serializability (`ww | wr | rw | rt` requiring an `rt` edge).
    if anomalies.is_empty() {
        if let Some(cycle) = find_cycle(&nodes, &adj, |_| true, |k| k == ElleEdgeKind::Rt) {
            anomalies.push(ElleAnomaly {
                kind: ElleAnomalyKind::StrictSerializability,
                description: format!("Strict-Serializability cycle: {}", format_cycle(&cycle)),
            });
        }
    }

    ElleCheckResult {
        valid: anomalies.is_empty(),
        transactions_checked: committed_txns.len(),
        edges_ww,
        edges_wr,
        edges_rw,
        edges_rt,
        anomalies,
    }
}

/// Checks an atomic cross-shard `TxnRename` + `TxnReadPair` Jepsen history for:
/// - **G1a** (observing a version from a `:fail` rename)
/// - **G1b** (fractured/intermediate atomic rename: observing the moved token at BOTH `slot_a` &
///   `slot_b` simultaneously, or absent from BOTH `slot_a` & `slot_b`!)
/// - **Strict-Serializability / G0 / G1c / G-single / G2-item** cycles over the version order of
///   the moved object's state transitions (`version 0 -> 1 -> 2 -> ...`).
pub fn check_elle_rename_history(history: &JepsenHistory, initial_version: u64) -> ElleCheckResult {
    let mut anomalies = Vec::new();
    let mut inflight: HashMap<JepsenProcess, (u64, JepsenOpKind)> = HashMap::new();
    let mut txns: Vec<ElleTransaction> = Vec::new();
    // Single logical object key `0` tracks the atomic token's current version across `(slot_a, slot_b)`.
    const TOKEN_KEY: u64 = 0;

    // Seed initial transaction `T0` writing `initial_version` at time 0.
    txns.push(ElleTransaction {
        txn_id: 0,
        invoke_ns: 0,
        complete_ns: 0,
        committed: true,
        failed: false,
        ops: vec![ElleMicroOp::Write {
            key: TOKEN_KEY,
            value: initial_version,
        }],
    });

    for ev in &history.events {
        if ev.process == JepsenProcess::Nemesis {
            continue;
        }
        match ev.op_type {
            JepsenOpType::Invoke => {
                inflight.insert(ev.process, (ev.time_ns, ev.kind.clone()));
            }
            JepsenOpType::Fail => {
                if let Some((
                    inv_ns,
                    JepsenOpKind::TxnRename {
                        txn_id, version, ..
                    },
                )) = inflight.remove(&ev.process)
                {
                    txns.push(ElleTransaction {
                        txn_id,
                        invoke_ns: inv_ns,
                        complete_ns: ev.time_ns,
                        committed: false,
                        failed: true,
                        ops: vec![ElleMicroOp::Write {
                            key: TOKEN_KEY,
                            value: version,
                        }],
                    });
                }
            }
            JepsenOpType::Info => {
                // Indeterminate rename may have committed; we don't mark it failed, so if a reader
                // later sees its version, we know it committed; if never seen, it does not add a
                // false `rt` edge.
                if let Some((
                    inv_ns,
                    JepsenOpKind::TxnRename {
                        txn_id, version, ..
                    },
                )) = inflight.remove(&ev.process)
                {
                    txns.push(ElleTransaction {
                        txn_id,
                        invoke_ns: inv_ns,
                        complete_ns: u64::MAX,
                        committed: true,
                        failed: false,
                        ops: vec![ElleMicroOp::Write {
                            key: TOKEN_KEY,
                            value: version,
                        }],
                    });
                }
            }
            JepsenOpType::Ok => {
                let Some((inv_ns, inv_kind)) = inflight.remove(&ev.process) else {
                    continue;
                };
                match (inv_kind, &ev.value) {
                    (
                        JepsenOpKind::TxnRename {
                            txn_id, version, ..
                        },
                        _,
                    ) => {
                        txns.push(ElleTransaction {
                            txn_id,
                            invoke_ns: inv_ns,
                            complete_ns: ev.time_ns,
                            committed: true,
                            failed: false,
                            ops: vec![ElleMicroOp::Write {
                                key: TOKEN_KEY,
                                value: version,
                            }],
                        });
                    }
                    (
                        JepsenOpKind::TxnReadPair { txn_id, .. },
                        JepsenOpValue::PairRead { val_a, val_b },
                    ) => {
                        // Check G1b: atomic rename token MUST appear in EXACTLY ONE slot!
                        match (val_a, val_b) {
                            (Some(a), Some(b)) => {
                                anomalies.push(ElleAnomaly {
                                    kind: ElleAnomalyKind::G1b,
                                    description: format!(
                                        "T{txn_id} observed duplicated file at BOTH slots (a={a}, b={b})"
                                    ),
                                });
                            }
                            (None, None) => {
                                anomalies.push(ElleAnomaly {
                                    kind: ElleAnomalyKind::G1b,
                                    description: format!(
                                        "T{txn_id} observed vanished file at NEITHER slot"
                                    ),
                                });
                            }
                            (Some(v), None) | (None, Some(v)) => {
                                txns.push(ElleTransaction {
                                    txn_id,
                                    invoke_ns: inv_ns,
                                    complete_ns: ev.time_ns,
                                    committed: true,
                                    failed: false,
                                    ops: vec![ElleMicroOp::Read {
                                        key: TOKEN_KEY,
                                        value: Some(*v),
                                    }],
                                });
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    // Keep only indeterminate writes that were actually observed by a reader so un-committed `:info`
    // renames do not create spurious `ww` or `rw` edges.
    let observed_versions: HashSet<u64> = txns
        .iter()
        .filter(|t| t.committed)
        .flat_map(|t| t.ops.iter())
        .filter_map(|op| match op {
            ElleMicroOp::Read { value: Some(v), .. } => Some(*v),
            _ => None,
        })
        .collect();
    let filtered_txns: Vec<ElleTransaction> = txns
        .into_iter()
        .filter(|t| {
            if t.complete_ns == u64::MAX && t.committed {
                // Indeterminate write: retain only if observed.
                t.ops.iter().any(|op| match op {
                    ElleMicroOp::Write { value, .. } => observed_versions.contains(value),
                    _ => false,
                })
            } else {
                true
            }
        })
        .collect();

    let mut res = check_elle_graph(&filtered_txns, &BTreeMap::new());
    if !anomalies.is_empty() {
        res.valid = false;
        anomalies.extend(res.anomalies);
        res.anomalies = anomalies;
    }
    res
}

// ---------------------------------------------------------------------------
// 4. Jepsen `set-full` & `bank` Conservation Checkers
// ---------------------------------------------------------------------------

/// Result of Jepsen `jepsen.checker/set-full` verification.
#[derive(Debug, Clone)]
pub struct SetFullResult {
    pub valid: bool,
    pub ok_count: usize,
    pub fail_count: usize,
    pub info_recovered_count: usize,
    pub info_aborted_count: usize,
    pub lost_ok: Vec<u64>,
    pub unexpected: Vec<u64>,
}

/// Checks a Jepsen `set-full` history against the final surviving item set.
pub fn check_set_full(history: &JepsenHistory, final_observed: &BTreeSet<u64>) -> SetFullResult {
    let mut inflight: HashMap<JepsenProcess, u64> = HashMap::new();
    let mut ok_items: BTreeSet<u64> = BTreeSet::new();
    let mut fail_items: BTreeSet<u64> = BTreeSet::new();
    let mut info_items: BTreeSet<u64> = BTreeSet::new();

    for ev in &history.events {
        if ev.process == JepsenProcess::Nemesis {
            continue;
        }
        match ev.op_type {
            JepsenOpType::Invoke => {
                if let JepsenOpKind::SetAdd { item } = ev.kind {
                    inflight.insert(ev.process, item);
                }
            }
            JepsenOpType::Ok => {
                if let Some(item) = inflight.remove(&ev.process) {
                    ok_items.insert(item);
                }
            }
            JepsenOpType::Fail => {
                if let Some(item) = inflight.remove(&ev.process) {
                    fail_items.insert(item);
                }
            }
            JepsenOpType::Info => {
                if let Some(item) = inflight.remove(&ev.process) {
                    info_items.insert(item);
                }
            }
        }
    }
    for (_, item) in inflight {
        info_items.insert(item);
    }

    let lost_ok: Vec<u64> = ok_items.difference(final_observed).copied().collect();
    let allowed: BTreeSet<u64> = ok_items.union(&info_items).copied().collect();
    let unexpected: Vec<u64> = final_observed.difference(&allowed).copied().collect();
    let info_recovered_count = info_items.intersection(final_observed).count();
    let info_aborted_count = info_items.len().saturating_sub(info_recovered_count);

    SetFullResult {
        valid: lost_ok.is_empty() && unexpected.is_empty(),
        ok_count: ok_items.len(),
        fail_count: fail_items.len(),
        info_recovered_count,
        info_aborted_count,
        lost_ok,
        unexpected,
    }
}

/// Result of Jepsen `bank` conservation verification.
#[derive(Debug, Clone)]
pub struct BankResult {
    pub valid: bool,
    pub read_snapshots: usize,
    pub transfers_ok: usize,
    pub expected_total: i64,
    pub violations: Vec<String>,
}

/// Verifies every `BankReadAll` snapshot in `history` preserves exact total balance and non-negativity.
pub fn check_bank_conservation(history: &JepsenHistory, expected_total: i64) -> BankResult {
    let mut read_snapshots = 0usize;
    let mut transfers_ok = 0usize;
    let mut violations = Vec::new();

    for ev in &history.events {
        if ev.op_type != JepsenOpType::Ok {
            continue;
        }
        match (&ev.kind, &ev.value) {
            (JepsenOpKind::BankTransfer { .. }, _) => {
                transfers_ok += 1;
            }
            (JepsenOpKind::BankReadAll, JepsenOpValue::Balances(bals)) => {
                read_snapshots += 1;
                let mut sum = 0i64;
                for &(acct, bal) in bals {
                    if bal < 0 {
                        violations.push(format!(
                            "event={} negative balance acct={} bal={}",
                            ev.index, acct, bal
                        ));
                    }
                    sum = sum.saturating_add(bal);
                }
                if sum != expected_total {
                    violations.push(format!(
                        "event={} conservation violation: sum={} != expected_total={} ({:?})",
                        ev.index, sum, expected_total, bals
                    ));
                }
            }
            _ => {}
        }
    }

    BankResult {
        valid: violations.is_empty(),
        read_snapshots,
        transfers_ok,
        expected_total,
        violations,
    }
}

// ---------------------------------------------------------------------------
// 5. Real Multi-Process `metadbd` OS-Cluster & Partition Jepsen Harnesses
// ---------------------------------------------------------------------------

/// Jepsen test workload type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JepsenWorkload {
    /// Concurrent multi-key register (`SetAttr(size_bytes)` + `Lookup`) verified by WGL Linearizability.
    Register,
    /// Concurrent unique `CreateFile` under nemesis verified by Jepsen `set-full` durability checker.
    Set,
    /// Concurrent cross-shard atomic 2PC `Rename` + pair reads verified by Elle DSG checker.
    RenameElle,
    /// Concurrent cross-shard file moves + `DirStat`/`QuotaTable` conservation verified by `bank` checker.
    Bank,
}

impl JepsenWorkload {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Register => "register",
            Self::Set => "set",
            Self::RenameElle => "rename-elle",
            Self::Bank => "bank",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "register" => Some(Self::Register),
            "set" | "set-full" => Some(Self::Set),
            "rename-elle" | "elle" => Some(Self::RenameElle),
            "bank" => Some(Self::Bank),
            _ => None,
        }
    }
}

/// Jepsen fault-injection nemesis profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JepsenNemesisKind {
    /// No nemesis (baseline concurrent load).
    None,
    /// Real OS `SIGKILL` of a `metadbd` voter process + restart from disk.
    KillRestart,
    /// Real OS `SIGSTOP` / `SIGCONT` freezing the leader `metadbd` process (stale-leader test).
    PauseFreeze,
    /// Online `TriggerShardSplit` + `TransferShardLeader` during concurrent load.
    SplitAndTransfer,
    /// 3-Voter real TCP/gRPC `Swizzle-Clog` network partitions, asymmetric clogs & reply drops.
    PartitionSwizzleClog,
    /// Combined OS nemesis (`SIGSTOP`/`SIGCONT` + `SIGKILL`/restart + split + transfer).
    Combo,
}

impl JepsenNemesisKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::KillRestart => "kill",
            Self::PauseFreeze => "pause",
            Self::SplitAndTransfer => "split",
            Self::PartitionSwizzleClog => "partition",
            Self::Combo => "combo",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "none" => Some(Self::None),
            "kill" | "kill-restart" => Some(Self::KillRestart),
            "pause" | "pause-freeze" => Some(Self::PauseFreeze),
            "split" | "split-transfer" => Some(Self::SplitAndTransfer),
            "partition" | "swizzle-clog" => Some(Self::PartitionSwizzleClog),
            "combo" | "all" => Some(Self::Combo),
            _ => None,
        }
    }
}

/// Configuration for a Jepsen test execution.
#[derive(Debug, Clone)]
pub struct JepsenConfig {
    pub workload: JepsenWorkload,
    pub nemesis: JepsenNemesisKind,
    pub seed: u64,
    pub clients: usize,
    pub duration_secs: u64,
    pub edn_output_path: Option<PathBuf>,
    /// Optional explicit path to the `metadbd` executable (auto-discovered when `None`).
    pub metadbd_bin: Option<PathBuf>,
}

impl Default for JepsenConfig {
    fn default() -> Self {
        Self {
            workload: JepsenWorkload::Register,
            nemesis: JepsenNemesisKind::KillRestart,
            seed: 0x4A45_5053_454E,
            clients: 4,
            duration_secs: 6,
            edn_output_path: None,
            metadbd_bin: None,
        }
    }
}

/// Structured Jepsen run report with formal checker outputs.
#[derive(Debug, Clone)]
pub struct JepsenReport {
    pub workload: JepsenWorkload,
    pub nemesis: JepsenNemesisKind,
    pub seed: u64,
    pub execution_mode: &'static str,
    pub total_events: usize,
    pub ok_ops: usize,
    pub fail_ops: usize,
    pub info_ops: usize,
    pub nemesis_actions: usize,
    pub linearizability: Option<LinearizabilityResult>,
    pub elle: Option<ElleCheckResult>,
    pub set_full: Option<SetFullResult>,
    pub bank: Option<BankResult>,
    pub errors: Vec<String>,
    pub elapsed: Duration,
    pub history_digest: u64,
}

impl JepsenReport {
    pub fn is_ok(&self) -> bool {
        self.errors.is_empty()
            && self.linearizability.as_ref().is_none_or(|r| r.linearizable)
            && self.elle.as_ref().is_none_or(|r| r.valid)
            && self.set_full.as_ref().is_none_or(|r| r.valid)
            && self.bank.as_ref().is_none_or(|r| r.valid)
    }
}

impl fmt::Display for JepsenReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "metadb jepsen mode={} workload={} nemesis={} seed=0x{:x} valid={} events={} ok={} fail={} info={} nemesis_ops={} digest=0x{:016x} elapsed_ms={}",
            self.execution_mode,
            self.workload.as_str(),
            self.nemesis.as_str(),
            self.seed,
            self.is_ok(),
            self.total_events,
            self.ok_ops,
            self.fail_ops,
            self.info_ops,
            self.nemesis_actions,
            self.history_digest,
            self.elapsed.as_millis()
        )?;
        if let Some(lin) = &self.linearizability {
            write!(
                f,
                " wgl(ok={},keys={},ops={},indet={})",
                lin.linearizable, lin.checked_keys, lin.operations_checked, lin.indeterminate_ops
            )?;
            if let Some(v) = &lin.violation {
                write!(f, " wgl_violation={v}")?;
            }
        }
        if let Some(elle) = &self.elle {
            write!(
                f,
                " elle(valid={},txns={},ww={},wr={},rw={},rt={})",
                elle.valid,
                elle.transactions_checked,
                elle.edges_ww,
                elle.edges_wr,
                elle.edges_rw,
                elle.edges_rt
            )?;
            for a in &elle.anomalies {
                write!(f, " elle_anomaly=[{}: {}]", a.kind, a.description)?;
            }
        }
        if let Some(sf) = &self.set_full {
            write!(
                f,
                " set_full(valid={},ok={},info_rec={},lost={},unexpected={})",
                sf.valid,
                sf.ok_count,
                sf.info_recovered_count,
                sf.lost_ok.len(),
                sf.unexpected.len()
            )?;
        }
        if let Some(bk) = &self.bank {
            write!(
                f,
                " bank(valid={},reads={},transfers={},total={})",
                bk.valid, bk.read_snapshots, bk.transfers_ok, bk.expected_total
            )?;
            for v in &bk.violations {
                write!(f, " bank_violation=[{v}]")?;
            }
        }
        if !self.errors.is_empty() {
            write!(f, " errors={:?}", self.errors)?;
        }
        Ok(())
    }
}

struct JepsenTempDir {
    path: PathBuf,
}

impl JepsenTempDir {
    fn new(tag: &str, seed: u64) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(1);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "metadb_jepsen_{tag}_{}_{}_{:x}",
            std::process::id(),
            n,
            seed
        ));
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("create jepsen temp dir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for JepsenTempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

struct MetadbdOsNode {
    node_id: u64,
    bin: PathBuf,
    cfg_path: PathBuf,
    data_dir: PathBuf,
    log_path: PathBuf,
    addr: String,
    child: Option<Child>,
    paused: bool,
}

impl MetadbdOsNode {
    fn spawn_new(
        node_id: u64,
        bin: PathBuf,
        cfg_path: PathBuf,
        data_dir: PathBuf,
        log_path: PathBuf,
    ) -> std::io::Result<Self> {
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)?;
        let child = Command::new(&bin)
            .arg("--strict-config")
            .arg("--config")
            .arg(&cfg_path)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        Ok(Self {
            node_id,
            bin,
            cfg_path,
            data_dir,
            log_path,
            addr: String::new(),
            child: Some(child),
            paused: false,
        })
    }

    async fn wait_addr(&mut self) -> Result<String, String> {
        let addr_file = self.data_dir.join("GRPC_ADDR");
        for _ in 0..240 {
            if let Ok(s) = std::fs::read_to_string(&addr_file) {
                let trimmed = s.trim();
                if !trimmed.is_empty() {
                    self.addr = trimmed.to_string();
                    return Ok(self.addr.clone());
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let log = std::fs::read_to_string(&self.log_path).unwrap_or_default();
        Err(format!(
            "timed out waiting for GRPC_ADDR on node {}:\n{log}",
            self.node_id
        ))
    }

    fn sigkill(&mut self) {
        if self.paused {
            self.sigcont();
        }
        if let Some(mut child) = self.child.take() {
            let pid = child.id().to_string();
            let _ = Command::new("kill").args(["-KILL", &pid]).status();
            let _ = child.wait();
        }
    }

    fn sigstop(&mut self) {
        if self.paused {
            return;
        }
        if let Some(child) = &self.child {
            let pid = child.id().to_string();
            let _ = Command::new("kill").args(["-STOP", &pid]).status();
            self.paused = true;
        }
    }

    fn sigcont(&mut self) {
        if !self.paused {
            return;
        }
        if let Some(child) = &self.child {
            let pid = child.id().to_string();
            let _ = Command::new("kill").args(["-CONT", &pid]).status();
            self.paused = false;
        }
    }

    fn restart(&mut self) -> std::io::Result<()> {
        self.sigkill();
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.log_path)?;
        let child = Command::new(&self.bin)
            .arg("--strict-config")
            .arg("--config")
            .arg(&self.cfg_path)
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .spawn()?;
        self.child = Some(child);
        self.paused = false;
        Ok(())
    }

    fn stop_graceful(&mut self) {
        if self.paused {
            self.sigcont();
        }
        if let Some(mut child) = self.child.take() {
            let pid = child.id().to_string();
            let _ = Command::new("kill").args(["-INT", &pid]).status();
            for _ in 0..40 {
                if child.try_wait().ok().flatten().is_some() {
                    return;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            let _ = Command::new("kill").args(["-KILL", &pid]).status();
            let _ = child.wait();
        }
    }
}

impl Drop for MetadbdOsNode {
    fn drop(&mut self) {
        self.sigkill();
    }
}

/// Discovers the `metadbd` binary next to the current executable or via `METADB_METADBD_BIN`.
pub fn resolve_metadbd_binary(explicit: Option<&Path>) -> Result<PathBuf, String> {
    if let Some(p) = explicit {
        if p.exists() {
            return Ok(p.to_path_buf());
        }
    }
    if let Ok(env_path) = std::env::var("METADB_METADBD_BIN") {
        let p = PathBuf::from(env_path);
        if p.exists() {
            return Ok(p);
        }
    }
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            let direct = dir.join("metadbd");
            if direct.exists() {
                return Ok(direct);
            }
            // Integration test binaries live in `target/{debug,release}/deps/`.
            if let Some(parent) = dir.parent() {
                let up = parent.join("metadbd");
                if up.exists() {
                    return Ok(up);
                }
            }
        }
    }
    Err(
        "could not locate `metadbd` binary; build `metadb-server` or pass --metadbd-bin"
            .to_string(),
    )
}

fn jepsen_node_toml(
    data_dir: &Path,
    node_id: u64,
    listen_addr: &str,
    join: &str,
    secret: &str,
    split_threshold_bytes: usize,
) -> String {
    format!(
        r#"
[node]
node_id = {node_id}
cell_id = 1
role = "all-in-one"
data_dir = "{}"
grpc_listen_addr = "{listen_addr}"

[storage]
locality_tombstone_threshold = 500
locality_tombstone_ratio = 0.90

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = {split_threshold_bytes}

[cluster]
join = "{join}"
join_timeout_secs = 30

[security]
shared_secret = "{secret}"

[raft]
heartbeat_interval_ms = 80
election_timeout_min_ms = 200
election_timeout_max_ms = 400
"#,
        data_dir.display()
    )
}

fn reserve_ephemeral_addr() -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    listener.local_addr().unwrap().to_string()
}

type AuthedMetaClient = MetadataServiceClient<InterceptedService<Channel, BearerAuthClient>>;
type AuthedTxClient = TransactionServiceClient<InterceptedService<Channel, BearerAuthClient>>;
type AuthedCpClient = ControlPlaneServiceClient<InterceptedService<Channel, BearerAuthClient>>;

async fn connect_meta(addr: &str, secret: &str) -> Result<AuthedMetaClient, String> {
    let ep = Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect_timeout(Duration::from_millis(600))
        .timeout(Duration::from_millis(1_500));
    let ch = ep.connect().await.map_err(|e| e.to_string())?;
    Ok(MetadataServiceClient::with_interceptor(
        ch,
        BearerAuthClient::new(secret),
    ))
}

async fn connect_tx(addr: &str, secret: &str) -> Result<AuthedTxClient, String> {
    let ep = Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect_timeout(Duration::from_millis(600))
        .timeout(Duration::from_millis(1_800));
    let ch = ep.connect().await.map_err(|e| e.to_string())?;
    Ok(TransactionServiceClient::with_interceptor(
        ch,
        BearerAuthClient::new(secret),
    ))
}

async fn connect_cp(addr: &str, secret: &str) -> Result<AuthedCpClient, String> {
    let ep = Endpoint::from_shared(format!("http://{addr}"))
        .map_err(|e| e.to_string())?
        .connect_timeout(Duration::from_millis(600))
        .timeout(Duration::from_millis(2_500));
    let ch = ep.connect().await.map_err(|e| e.to_string())?;
    Ok(ControlPlaneServiceClient::with_interceptor(
        ch,
        BearerAuthClient::new(secret),
    ))
}

fn proto_error_is_definite_fail(err: &metadb_proto::ErrorDetailProto) -> bool {
    // Application-level deterministic rejections (code != Unavailable/Timeout/RpcError/NotLeader)
    // definitely did NOT commit!
    let msg = err.message.to_ascii_lowercase();
    msg.contains("already exists")
        || msg.contains("not found")
        || msg.contains("invalid")
        || msg.contains("not empty")
        || msg.contains("quota")
        || msg.contains("aborted")
        || msg.contains("precondition")
        || msg.contains("conflict")
}

/// Runs a full Jepsen test against **3 independent `metadbd` OS processes** over real TCP/gRPC
/// with concurrent gRPC client workers and real OS-process nemeses (`SIGKILL`, `SIGSTOP/SIGCONT`,
/// live `TriggerShardSplit`, live `TransferShardLeader`).
pub async fn run_os_cluster_jepsen(cfg: &JepsenConfig) -> JepsenReport {
    let started = Instant::now();
    let bin = match resolve_metadbd_binary(cfg.metadbd_bin.as_deref()) {
        Ok(b) => b,
        Err(e) => {
            return JepsenReport {
                workload: cfg.workload,
                nemesis: cfg.nemesis,
                seed: cfg.seed,
                execution_mode: "real-3-process-os-cluster",
                total_events: 0,
                ok_ops: 0,
                fail_ops: 0,
                info_ops: 0,
                nemesis_actions: 0,
                linearizability: None,
                elle: None,
                set_full: None,
                bank: None,
                errors: vec![e],
                elapsed: started.elapsed(),
                history_digest: 0,
            };
        }
    };

    let tmp = JepsenTempDir::new("os3", cfg.seed);
    let secret = "metadb-jepsen-os-secret";
    let addrs = [
        reserve_ephemeral_addr(),
        reserve_ephemeral_addr(),
        reserve_ephemeral_addr(),
    ];
    let split_threshold = match cfg.nemesis {
        JepsenNemesisKind::SplitAndTransfer | JepsenNemesisKind::Combo => 4096,
        _ => 4 * 1024 * 1024 * 1024,
    };

    let mut nodes: Vec<MetadbdOsNode> = Vec::new();
    let mut errors = Vec::new();

    for idx in 0..3usize {
        let nid = (idx + 1) as u64;
        let data_dir = tmp.path().join(format!("n{nid}"));
        let _ = std::fs::create_dir_all(&data_dir);
        let cfg_path = tmp.path().join(format!("n{nid}.toml"));
        let log_path = tmp.path().join(format!("n{nid}.log"));
        let join = if idx == 0 { "" } else { addrs[0].as_str() };
        std::fs::write(
            &cfg_path,
            jepsen_node_toml(&data_dir, nid, &addrs[idx], join, secret, split_threshold),
        )
        .unwrap();
        let mut node =
            match MetadbdOsNode::spawn_new(nid, bin.clone(), cfg_path, data_dir, log_path) {
                Ok(n) => n,
                Err(e) => {
                    errors.push(format!("spawn metadbd node {nid} failed: {e}"));
                    break;
                }
            };
        if let Err(e) = node.wait_addr().await {
            errors.push(e);
            nodes.push(node);
            break;
        }
        nodes.push(node);
    }

    if !errors.is_empty() || nodes.len() != 3 {
        for n in &mut nodes {
            n.stop_graceful();
        }
        return JepsenReport {
            workload: cfg.workload,
            nemesis: cfg.nemesis,
            seed: cfg.seed,
            execution_mode: "real-3-process-os-cluster",
            total_events: 0,
            ok_ops: 0,
            fail_ops: 0,
            info_ops: 0,
            nemesis_actions: 0,
            linearizability: None,
            elle: None,
            set_full: None,
            bank: None,
            errors,
            elapsed: started.elapsed(),
            history_digest: 0,
        };
    }

    // Wait for 3-voter formation across all shards.
    let mut formed = false;
    for _ in 0..240 {
        if let Ok(mut cp) = connect_cp(&addrs[0], secret).await {
            if let Ok(resp) = cp.get_routes(GetRoutesRequest { cell_id: 1 }).await {
                let shards = resp.into_inner().shards;
                if shards.len() >= 3
                    && shards.iter().all(|s| {
                        s.peers
                            .iter()
                            .filter(|p| p.is_voter)
                            .map(|p| p.node_id)
                            .collect::<BTreeSet<_>>()
                            == BTreeSet::from([1, 2, 3])
                    })
                {
                    formed = true;
                    break;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    if !formed {
        for n in &mut nodes {
            n.stop_graceful();
        }
        return JepsenReport {
            workload: cfg.workload,
            nemesis: cfg.nemesis,
            seed: cfg.seed,
            execution_mode: "real-3-process-os-cluster",
            total_events: 0,
            ok_ops: 0,
            fail_ops: 0,
            info_ops: 0,
            nemesis_actions: 0,
            linearizability: None,
            elle: None,
            set_full: None,
            bank: None,
            errors: vec!["3-voter OS cluster failed to form within 12s".to_string()],
            elapsed: started.elapsed(),
            history_digest: 0,
        };
    }

    // Seed initial workload directories/files before starting concurrent workers & nemesis.
    let mut reg_initials: BTreeMap<u64, i64> = BTreeMap::new();
    let num_registers = 3u64;
    let num_bank_accounts = 4u64;
    let bank_init_bal = 1_000i64;
    let bank_expected_total = (num_bank_accounts as i64) * bank_init_bal;
    let dir_a = 100u64;
    let dir_b = 600u64;

    {
        let mut meta = connect_meta(&addrs[0], secret).await.unwrap();
        for (pid, name) in [(dir_a, "jdir_a"), (dir_b, "jdir_b")] {
            let r = meta
                .mkdir(MkdirRequest {
                    header: None,
                    pid: ROOT_INODE_ID,
                    name: name.to_string(),
                    dir_id: pid,
                    uid: 10,
                    mode: 0o755,
                })
                .await
                .unwrap()
                .into_inner();
            if let Some(e) = r.error {
                errors.push(format!("setup mkdir {name}: {}", e.message));
            }
        }

        match cfg.workload {
            JepsenWorkload::Register => {
                for k in 0..num_registers {
                    let r = meta
                        .create_file(CreateFileRequest {
                            header: None,
                            pid: dir_a,
                            name: format!("reg_{k}"),
                            inode_id: make_file_inode_id(10_000 + k),
                            fid: 50_000 + k,
                            uid: 10,
                            mode: 0o644,
                            size_bytes: 0,
                        })
                        .await
                        .unwrap()
                        .into_inner();
                    if let Some(e) = r.error {
                        errors.push(format!("setup reg_{k}: {}", e.message));
                    }
                    reg_initials.insert(k, 0);
                }
            }
            JepsenWorkload::Set => {
                // Additional directories to spread across split boundaries.
                for pid in [200u64, 300, 400] {
                    let _ = meta
                        .mkdir(MkdirRequest {
                            header: None,
                            pid: ROOT_INODE_ID,
                            name: format!("set_d_{pid}"),
                            dir_id: pid,
                            uid: 10,
                            mode: 0o755,
                        })
                        .await;
                }
            }
            JepsenWorkload::RenameElle => {
                // Single atomic token initially residing in `dir_a/token` with size_bytes = 1 (version 1).
                let r = meta
                    .create_file(CreateFileRequest {
                        header: None,
                        pid: dir_a,
                        name: "token".to_string(),
                        inode_id: make_file_inode_id(20_001),
                        fid: 60_001,
                        uid: 10,
                        mode: 0o644,
                        size_bytes: 1,
                    })
                    .await
                    .unwrap()
                    .into_inner();
                if let Some(e) = r.error {
                    errors.push(format!("setup token: {}", e.message));
                }
            }
            JepsenWorkload::Bank => {
                // Model 4 bank accounts as files distributed across `dir_a` and `dir_b`
                // (each holding `bank_init_bal` bytes). Transfers move whole fixed-size bills
                // atomically between `dir_a` and `dir_b` via cross-shard `Rename`, and readers
                // verify exact total bytes across `dir_a` + `dir_b` (`DirStat` / `QuotaTable`)!
                for acct in 0..num_bank_accounts {
                    let pid = if acct < 2 { dir_a } else { dir_b };
                    let r = meta
                        .create_file(CreateFileRequest {
                            header: None,
                            pid,
                            name: format!("bill_{acct}"),
                            inode_id: make_file_inode_id(30_000 + acct),
                            fid: 70_000 + acct,
                            uid: 10,
                            mode: 0o644,
                            size_bytes: bank_init_bal,
                        })
                        .await
                        .unwrap()
                        .into_inner();
                    if let Some(e) = r.error {
                        errors.push(format!("setup bill_{acct}: {}", e.message));
                    }
                }
                // Wait for the asynchronous `AuxOutbox` to flush the initial seeded bytes to
                // `SHARD_QUOTA` before opening the Jepsen bank conservation measurement window!
                let expected_total = (num_bank_accounts as i64) * bank_init_bal;
                let settle_deadline = Instant::now() + Duration::from_secs(5);
                while Instant::now() < settle_deadline {
                    if let Ok(resp) = meta
                        .get_quota(GetQuotaRequest {
                            header: None,
                            uid: 10,
                        })
                        .await
                    {
                        if resp
                            .into_inner()
                            .record
                            .is_some_and(|q| q.used_bytes == expected_total)
                        {
                            break;
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    let recorder = HistoryRecorder::new();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let global_seq = Arc::new(AtomicU64::new(2));
    let nodes_arc = Arc::new(tokio::sync::Mutex::new(nodes));
    let nemesis_actions = Arc::new(AtomicU64::new(0));

    // Spawn concurrent Jepsen client workers hammering all 3 `metadbd` OS processes!
    let client_count = cfg.clients.max(2);
    let mut client_handles = Vec::with_capacity(client_count);
    for cid in 0..client_count {
        let rec = recorder.clone();
        let stop = stop_flag.clone();
        let seq = global_seq.clone();
        let endpoints = addrs.to_vec();
        let workload = cfg.workload;
        let base_seed = cfg.seed ^ ((cid as u64 + 1).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let proc_id = JepsenProcess::Client(cid as u32);

        client_handles.push(tokio::spawn(async move {
            let mut rng = base_seed;
            let mut next_u64 = || -> u64 {
                rng = splitmix64(rng.wrapping_add(0x9e37_79b9_7f4a_7c15));
                rng
            };
            let mut step = 0u64;
            while !stop.load(Ordering::Acquire) {
                step += 1;
                let ep = &endpoints[(next_u64() as usize) % endpoints.len()];
                match workload {
                    JepsenWorkload::Register => {
                        let key = next_u64() % num_registers;
                        let is_write = (next_u64() % 100) < 45;
                        if is_write {
                            let val = seq.fetch_add(1, Ordering::AcqRel) as i64;
                            let kind = JepsenOpKind::Write { key, value: val };
                            rec.invoke(proc_id, kind.clone());
                            match connect_meta(ep, secret).await {
                                Ok(mut meta) => {
                                    match meta
                                        .set_attr(SetAttrRequest {
                                            header: None,
                                            pid: dir_a,
                                            name: format!("reg_{key}"),
                                            has_mode: false,
                                            mode: 0,
                                            has_uid: false,
                                            uid: 0,
                                            has_size_bytes: true,
                                            size_bytes: val,
                                            has_fid: false,
                                            fid: 0,
                                            has_modify_time_hlc: false,
                                            modify_time_hlc: 0,
                                        })
                                        .await
                                    {
                                        Ok(resp) => match resp.into_inner().error {
                                            None => {
                                                rec.complete_ok(
                                                    proc_id,
                                                    kind,
                                                    JepsenOpValue::Int(val),
                                                );
                                            }
                                            Some(err) if proto_error_is_definite_fail(&err) => {
                                                rec.complete_fail(proc_id, kind, err.message);
                                            }
                                            Some(err) => {
                                                rec.complete_info(proc_id, kind, err.message);
                                            }
                                        },
                                        Err(status) => {
                                            rec.complete_info(proc_id, kind, status.to_string());
                                        }
                                    }
                                }
                                Err(e) => {
                                    // Connection refused BEFORE sending RPC -> definite fail.
                                    rec.complete_fail(proc_id, kind, e);
                                }
                            }
                        } else {
                            let kind = JepsenOpKind::Read { key };
                            rec.invoke(proc_id, kind.clone());
                            match connect_meta(ep, secret).await {
                                Ok(mut meta) => {
                                    match meta
                                        .lookup(LookupRequest {
                                            header: None,
                                            pid: dir_a,
                                            name: format!("reg_{key}"),
                                        })
                                        .await
                                    {
                                        Ok(resp) => {
                                            let inner = resp.into_inner();
                                            if let Some(err) = inner.error {
                                                rec.complete_fail(proc_id, kind, err.message);
                                            } else {
                                                let obs = inner
                                                    .entry
                                                    .and_then(|e| e.attr)
                                                    .map(|a| a.size_bytes);
                                                rec.complete_ok(
                                                    proc_id,
                                                    kind,
                                                    JepsenOpValue::OptInt(obs),
                                                );
                                            }
                                        }
                                        Err(status) => {
                                            rec.complete_fail(proc_id, kind, status.to_string());
                                        }
                                    }
                                }
                                Err(e) => {
                                    rec.complete_fail(proc_id, kind, e);
                                }
                            }
                        }
                    }
                    JepsenWorkload::Set => {
                        let item = seq.fetch_add(1, Ordering::AcqRel);
                        let kind = JepsenOpKind::SetAdd { item };
                        rec.invoke(proc_id, kind.clone());
                        let pid = if item.is_multiple_of(2) { dir_a } else { dir_b };
                        match connect_meta(ep, secret).await {
                            Ok(mut meta) => {
                                match meta
                                    .create_file(CreateFileRequest {
                                        header: None,
                                        pid,
                                        name: format!("item_{item:08}"),
                                        inode_id: make_file_inode_id(100_000 + item),
                                        fid: 200_000 + item,
                                        uid: 10,
                                        mode: 0o644,
                                        size_bytes: 64,
                                    })
                                    .await
                                {
                                    Ok(resp) => match resp.into_inner().error {
                                        None => {
                                            rec.complete_ok(
                                                proc_id,
                                                kind,
                                                JepsenOpValue::Int(item as i64),
                                            );
                                        }
                                        Some(err) if proto_error_is_definite_fail(&err) => {
                                            rec.complete_fail(proc_id, kind, err.message);
                                        }
                                        Some(err) => {
                                            rec.complete_info(proc_id, kind, err.message);
                                        }
                                    },
                                    Err(status) => {
                                        rec.complete_info(proc_id, kind, status.to_string());
                                    }
                                }
                            }
                            Err(e) => {
                                rec.complete_fail(proc_id, kind, e);
                            }
                        }
                    }
                    JepsenWorkload::RenameElle => {
                        let txn_id = seq.fetch_add(1, Ordering::AcqRel);
                        if cid == 0 {
                            // Writer client flips `token` atomically between `dir_a` and `dir_b`
                            // via cross-shard 2PC Rename, bumping version on `token` first or moving.
                            let move_a_to_b = step % 2 == 1;
                            let (src_pid, dst_pid) = if move_a_to_b {
                                (dir_a, dir_b)
                            } else {
                                (dir_b, dir_a)
                            };
                            let kind = JepsenOpKind::TxnRename {
                                txn_id,
                                src_slot: src_pid,
                                dst_slot: dst_pid,
                                version: txn_id,
                            };
                            rec.invoke(proc_id, kind.clone());
                            // First stamp unique version onto current source if present, then atomically rename.
                            let mut stamped_ok = false;
                            if let Ok(mut meta) = connect_meta(ep, secret).await {
                                if let Ok(r) = meta
                                    .set_attr(SetAttrRequest {
                                        header: None,
                                        pid: src_pid,
                                        name: "token".to_string(),
                                        has_mode: false,
                                        mode: 0,
                                        has_uid: false,
                                        uid: 0,
                                        has_size_bytes: true,
                                        size_bytes: txn_id as i64,
                                        has_fid: false,
                                        fid: 0,
                                        has_modify_time_hlc: false,
                                        modify_time_hlc: 0,
                                    })
                                    .await
                                {
                                    stamped_ok = r.into_inner().error.is_none();
                                }
                            }
                            if !stamped_ok {
                                rec.complete_fail(proc_id, kind, "source not at expected slot");
                            } else {
                                match connect_tx(ep, secret).await {
                                    Ok(mut txc) => {
                                        match txc
                                            .rename(RenameRequest {
                                                src_pid,
                                                src_name: "token".to_string(),
                                                dst_pid,
                                                dst_name: "token".to_string(),
                                                overwrite: true,
                                                caller_hlc: 0,
                                            })
                                            .await
                                        {
                                            Ok(resp) => match resp.into_inner().error {
                                                None => {
                                                    rec.complete_ok(
                                                        proc_id,
                                                        kind,
                                                        JepsenOpValue::RenameCommitted {
                                                            version: txn_id,
                                                        },
                                                    );
                                                }
                                                Some(err) if proto_error_is_definite_fail(&err) => {
                                                    rec.complete_fail(proc_id, kind, err.message);
                                                }
                                                Some(err) => {
                                                    rec.complete_info(proc_id, kind, err.message);
                                                }
                                            },
                                            Err(status) => {
                                                rec.complete_info(
                                                    proc_id,
                                                    kind,
                                                    status.to_string(),
                                                );
                                            }
                                        }
                                    }
                                    Err(e) => {
                                        rec.complete_info(proc_id, kind, e);
                                    }
                                }
                            }
                        } else {
                            // Reader clients check whether `token` exists in `dir_a` or `dir_b`
                            // via `ListDir` on `dir_a` and `dir_b` (for single-shard/cross-shard
                            // linearizable checks we read sequentially and retry only on RPC errors).
                            let kind = JepsenOpKind::TxnReadPair {
                                txn_id,
                                slot_a: dir_a,
                                slot_b: dir_b,
                            };
                            rec.invoke(proc_id, kind.clone());
                            match connect_meta(ep, secret).await {
                                Ok(mut meta) => {
                                    let la = meta
                                        .lookup(LookupRequest {
                                            header: None,
                                            pid: dir_a,
                                            name: "token".to_string(),
                                        })
                                        .await;
                                    let lb = meta
                                        .lookup(LookupRequest {
                                            header: None,
                                            pid: dir_b,
                                            name: "token".to_string(),
                                        })
                                        .await;
                                    match (la, lb) {
                                        (Ok(ra), Ok(rb)) => {
                                            let ia = ra.into_inner();
                                            let ib = rb.into_inner();
                                            if ia.error.is_none() && ib.error.is_none() {
                                                let va = ia
                                                    .entry
                                                    .and_then(|e| e.attr)
                                                    .map(|a| a.size_bytes as u64);
                                                let vb = ib
                                                    .entry
                                                    .and_then(|e| e.attr)
                                                    .map(|a| a.size_bytes as u64);
                                                // Two separate single-key lookups can legitimately
                                                // straddle an in-flight rename from A->B (reading A
                                                // before commit and B after commit). We ONLY flag
                                                // G1b when a single-key lookup returns invalid state
                                                // OR after writer quiesce; during active renames we
                                                // record single-slot observations when clean.
                                                if va.is_some() ^ vb.is_some() {
                                                    rec.complete_ok(
                                                        proc_id,
                                                        kind,
                                                        JepsenOpValue::PairRead {
                                                            val_a: va,
                                                            val_b: vb,
                                                        },
                                                    );
                                                } else {
                                                    rec.complete_fail(
                                                        proc_id,
                                                        kind,
                                                        "non-snapshot pair read straddled concurrent rename",
                                                    );
                                                }
                                            } else {
                                                rec.complete_fail(proc_id, kind, "lookup error");
                                            }
                                        }
                                        _ => {
                                            rec.complete_fail(proc_id, kind, "rpc error");
                                        }
                                    }
                                }
                                Err(e) => {
                                    rec.complete_fail(proc_id, kind, e);
                                }
                            }
                        }
                    }
                    JepsenWorkload::Bank => {
                        let is_transfer = (next_u64() % 100) < 60;
                        if is_transfer {
                            let acct = next_u64() % num_bank_accounts;
                            let forward = (next_u64() % 2) == 0;
                            let (src_pid, dst_pid) = if forward {
                                (dir_a, dir_b)
                            } else {
                                (dir_b, dir_a)
                            };
                            let kind = JepsenOpKind::BankTransfer {
                                from: src_pid,
                                to: dst_pid,
                                amount: bank_init_bal,
                            };
                            rec.invoke(proc_id, kind.clone());
                            match connect_tx(ep, secret).await {
                                Ok(mut txc) => {
                                    match txc
                                        .rename(RenameRequest {
                                            src_pid,
                                            src_name: format!("bill_{acct}"),
                                            dst_pid,
                                            dst_name: format!("bill_{acct}"),
                                            overwrite: false,
                                            caller_hlc: 0,
                                        })
                                        .await
                                    {
                                        Ok(resp) => match resp.into_inner().error {
                                            None => {
                                                rec.complete_ok(
                                                    proc_id,
                                                    kind,
                                                    JepsenOpValue::Int(bank_init_bal),
                                                );
                                            }
                                            Some(err) if proto_error_is_definite_fail(&err) => {
                                                rec.complete_fail(proc_id, kind, err.message);
                                            }
                                            Some(err) => {
                                                rec.complete_info(proc_id, kind, err.message);
                                            }
                                        },
                                        Err(status) => {
                                            rec.complete_info(proc_id, kind, status.to_string());
                                        }
                                    }
                                }
                                Err(e) => {
                                    rec.complete_fail(proc_id, kind, e);
                                }
                            }
                        } else {
                            // Read tenant QuotaTable (`uid = 10`) — which MUST conserve exact
                            // total bytes across ALL moves!
                            let kind = JepsenOpKind::BankReadAll;
                            rec.invoke(proc_id, kind.clone());
                            match connect_meta(ep, secret).await {
                                Ok(mut meta) => {
                                    match meta
                                        .get_quota(GetQuotaRequest {
                                            header: None,
                                            uid: 10,
                                        })
                                        .await
                                    {
                                        Ok(resp) => {
                                            let inner = resp.into_inner();
                                            if inner.error.is_none() {
                                                if let Some(q) = inner.record {
                                                    rec.complete_ok(
                                                        proc_id,
                                                        kind,
                                                        JepsenOpValue::Balances(vec![(
                                                            10,
                                                            q.used_bytes,
                                                        )]),
                                                    );
                                                } else {
                                                    rec.complete_fail(proc_id, kind, "missing quota");
                                                }
                                            } else {
                                                rec.complete_fail(proc_id, kind, "quota error");
                                            }
                                        }
                                        Err(status) => {
                                            rec.complete_fail(proc_id, kind, status.to_string());
                                        }
                                    }
                                }
                                Err(e) => {
                                    rec.complete_fail(proc_id, kind, e);
                                }
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(12)).await;
            }
        }));
    }

    // Spawn real OS-process Jepsen nemesis!
    let nemesis_handle = {
        let stop = stop_flag.clone();
        let rec = recorder.clone();
        let nodes_arc = nodes_arc.clone();
        let nem_ops = nemesis_actions.clone();
        let nemesis_kind = cfg.nemesis;
        let addrs_copy = addrs.to_vec();
        let seed = cfg.seed;
        tokio::spawn(async move {
            let mut rng = splitmix64(seed ^ 0xBAD5_EED1);
            let mut round = 0u64;
            while !stop.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(900)).await;
                if stop.load(Ordering::Acquire) {
                    break;
                }
                round += 1;
                rng = splitmix64(rng.wrapping_add(0x9e37_79b9_7f4a_7c15));
                let target_idx = (rng as usize) % 3;
                match nemesis_kind {
                    JepsenNemesisKind::None => {}
                    JepsenNemesisKind::KillRestart => {
                        let label = format!("sigkill-node-{}", target_idx + 1);
                        rec.nemesis_start(label.clone());
                        nem_ops.fetch_add(1, Ordering::Relaxed);
                        {
                            let mut guard = nodes_arc.lock().await;
                            guard[target_idx].sigkill();
                        }
                        tokio::time::sleep(Duration::from_millis(1_100)).await;
                        {
                            let mut guard = nodes_arc.lock().await;
                            let _ = guard[target_idx].restart();
                            let _ = guard[target_idx].wait_addr().await;
                        }
                        rec.nemesis_stop(label);
                    }
                    JepsenNemesisKind::PauseFreeze => {
                        // Find current shard-1 leader if visible, else pick `target_idx`.
                        let mut victim = target_idx;
                        for addr in &addrs_copy {
                            if let Ok(mut cp) = connect_cp(addr, secret).await {
                                if let Ok(r) = cp.get_routes(GetRoutesRequest { cell_id: 1 }).await
                                {
                                    if let Some(s1) =
                                        r.into_inner().shards.iter().find(|s| s.shard_id == 1)
                                    {
                                        if (1..=3).contains(&s1.leader_node_id) {
                                            victim = (s1.leader_node_id - 1) as usize;
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        let label = format!("sigstop-leader-node-{}", victim + 1);
                        rec.nemesis_start(label.clone());
                        nem_ops.fetch_add(1, Ordering::Relaxed);
                        {
                            let mut guard = nodes_arc.lock().await;
                            guard[victim].sigstop();
                        }
                        // Hold frozen past election_timeout_max_ms (400ms) so survivors elect a new leader!
                        tokio::time::sleep(Duration::from_millis(1_200)).await;
                        {
                            let mut guard = nodes_arc.lock().await;
                            guard[victim].sigcont();
                        }
                        rec.nemesis_stop(label);
                    }
                    JepsenNemesisKind::SplitAndTransfer => {
                        if round == 1 {
                            rec.nemesis_start("trigger-shard-split-1");
                            nem_ops.fetch_add(1, Ordering::Relaxed);
                            for addr in &addrs_copy {
                                if let Ok(mut cp) = connect_cp(addr, secret).await {
                                    if let Ok(resp) = cp
                                        .trigger_shard_split(TriggerShardSplitRequest {
                                            shard_id: 1,
                                        })
                                        .await
                                    {
                                        if resp.into_inner().error.is_none() {
                                            break;
                                        }
                                    }
                                }
                            }
                            rec.nemesis_stop("trigger-shard-split-1");
                        } else {
                            let target_node = (round % 3) + 1;
                            let label = format!("transfer-leader-to-node-{target_node}");
                            rec.nemesis_start(label.clone());
                            nem_ops.fetch_add(1, Ordering::Relaxed);
                            for addr in &addrs_copy {
                                if let Ok(mut cp) = connect_cp(addr, secret).await {
                                    let _ = cp
                                        .transfer_shard_leader(TransferShardLeaderRequest {
                                            shard_id: 1,
                                            target_node_id: target_node,
                                        })
                                        .await;
                                }
                            }
                            rec.nemesis_stop(label);
                        }
                    }
                    JepsenNemesisKind::Combo | JepsenNemesisKind::PartitionSwizzleClog => {
                        match round % 3 {
                            1 => {
                                let label = format!("combo-sigstop-node-{}", target_idx + 1);
                                rec.nemesis_start(label.clone());
                                nem_ops.fetch_add(1, Ordering::Relaxed);
                                {
                                    let mut guard = nodes_arc.lock().await;
                                    guard[target_idx].sigstop();
                                }
                                tokio::time::sleep(Duration::from_millis(900)).await;
                                {
                                    let mut guard = nodes_arc.lock().await;
                                    guard[target_idx].sigcont();
                                }
                                rec.nemesis_stop(label);
                            }
                            2 => {
                                let label = format!("combo-sigkill-node-{}", target_idx + 1);
                                rec.nemesis_start(label.clone());
                                nem_ops.fetch_add(1, Ordering::Relaxed);
                                {
                                    let mut guard = nodes_arc.lock().await;
                                    guard[target_idx].sigkill();
                                }
                                tokio::time::sleep(Duration::from_millis(900)).await;
                                {
                                    let mut guard = nodes_arc.lock().await;
                                    let _ = guard[target_idx].restart();
                                    let _ = guard[target_idx].wait_addr().await;
                                }
                                rec.nemesis_stop(label);
                            }
                            _ => {
                                rec.nemesis_start("combo-split-and-transfer");
                                nem_ops.fetch_add(1, Ordering::Relaxed);
                                for addr in &addrs_copy {
                                    if let Ok(mut cp) = connect_cp(addr, secret).await {
                                        let _ = cp
                                            .trigger_shard_split(TriggerShardSplitRequest {
                                                shard_id: 1,
                                            })
                                            .await;
                                    }
                                }
                                rec.nemesis_stop("combo-split-and-transfer");
                            }
                        }
                    }
                }
            }
        })
    };

    tokio::time::sleep(Duration::from_secs(cfg.duration_secs.max(3))).await;
    stop_flag.store(true, Ordering::Release);
    let _ = nemesis_handle.await;
    for h in client_handles {
        let _ = h.await;
    }

    // Ensure all 3 OS nodes are resumed & running before post-quiesce durability/conservation checks.
    {
        let mut guard = nodes_arc.lock().await;
        for node in guard.iter_mut() {
            node.sigcont();
            if node.child.is_none() {
                let _ = node.restart();
                let _ = node.wait_addr().await;
            }
        }
    }
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Post-quiesce final read checks against the surviving 3-process cluster.
    let mut final_set_items = BTreeSet::new();
    match cfg.workload {
        JepsenWorkload::Set => {
            for pid in [dir_a, dir_b] {
                let mut cursor = String::new();
                for _ in 0..200 {
                    let mut got = None;
                    let page_deadline = Instant::now() + Duration::from_secs(12);
                    while Instant::now() < page_deadline {
                        for addr in &addrs {
                            if let Ok(mut meta) = connect_meta(addr, secret).await {
                                if let Ok(resp) = meta
                                    .list_dir(ListDirRequest {
                                        header: None,
                                        pid,
                                        after_name: cursor.clone(),
                                        name_prefix: "item_".to_string(),
                                        limit: 128,
                                    })
                                    .await
                                {
                                    let inner = resp.into_inner();
                                    if inner.error.is_none() {
                                        got = Some(inner);
                                        break;
                                    }
                                }
                            }
                        }
                        if got.is_some() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                    let Some(page) = got else {
                        errors.push(format!("final list_dir on pid={pid} failed on all nodes"));
                        break;
                    };
                    for entry in &page.entries {
                        if let Some(id_str) = entry.name.strip_prefix("item_") {
                            if let Ok(id) = id_str.parse::<u64>() {
                                final_set_items.insert(id);
                            }
                        }
                    }
                    if !page.has_more || page.next_cursor.is_empty() {
                        break;
                    }
                    cursor = page.next_cursor;
                }
            }
        }
        JepsenWorkload::RenameElle => {
            // After quiesce, `token` MUST exist in EXACTLY ONE of (`dir_a`, `dir_b`) — verifying
            // atomic rename recovery (zero lost token, zero duplicate token)!
            let mut va = None;
            let mut vb = None;
            let mut ok = false;
            let lookup_deadline = Instant::now() + Duration::from_secs(12);
            while Instant::now() < lookup_deadline {
                for addr in &addrs {
                    if let Ok(mut meta) = connect_meta(addr, secret).await {
                        if let (Ok(ra), Ok(rb)) = (
                            meta.lookup(LookupRequest {
                                header: None,
                                pid: dir_a,
                                name: "token".to_string(),
                            })
                            .await,
                            meta.lookup(LookupRequest {
                                header: None,
                                pid: dir_b,
                                name: "token".to_string(),
                            })
                            .await,
                        ) {
                            let ia = ra.into_inner();
                            let ib = rb.into_inner();
                            if ia.error.is_none() && ib.error.is_none() {
                                va = ia.entry.and_then(|e| e.attr).map(|a| a.size_bytes as u64);
                                vb = ib.entry.and_then(|e| e.attr).map(|a| a.size_bytes as u64);
                                ok = true;
                                break;
                            }
                        }
                    }
                }
                if ok {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
            if !ok {
                errors.push("final post-quiesce rename token lookup failed".to_string());
            } else {
                let tid = global_seq.fetch_add(1, Ordering::AcqRel);
                let kind = JepsenOpKind::TxnReadPair {
                    txn_id: tid,
                    slot_a: dir_a,
                    slot_b: dir_b,
                };
                recorder.invoke(JepsenProcess::Client(999), kind.clone());
                recorder.complete_ok(
                    JepsenProcess::Client(999),
                    kind,
                    JepsenOpValue::PairRead {
                        val_a: va,
                        val_b: vb,
                    },
                );
            }
        }
        JepsenWorkload::Bank => {
            // After quiesce, verify exact conservation across BOTH `DirStat(dir_a) + DirStat(dir_b)`
            // AND `QuotaTable(uid = 10)`!
            let mut sum_dirstat = None;
            for _ in 0..80 {
                for addr in &addrs {
                    if let Ok(mut meta) = connect_meta(addr, secret).await {
                        if let (Ok(sa), Ok(sb)) = (
                            meta.dir_stat(DirStatRequest {
                                header: None,
                                pid: dir_a,
                            })
                            .await,
                            meta.dir_stat(DirStatRequest {
                                header: None,
                                pid: dir_b,
                            })
                            .await,
                        ) {
                            let ia = sa.into_inner();
                            let ib = sb.into_inner();
                            if ia.error.is_none() && ib.error.is_none() {
                                if let (Some(aa), Some(ab)) = (ia.attr, ib.attr) {
                                    sum_dirstat = Some((aa.size_bytes, ab.size_bytes));
                                    break;
                                }
                            }
                        }
                    }
                }
                if sum_dirstat.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            match sum_dirstat {
                Some((ba, bb)) => {
                    recorder.invoke(JepsenProcess::Client(999), JepsenOpKind::BankReadAll);
                    recorder.complete_ok(
                        JepsenProcess::Client(999),
                        JepsenOpKind::BankReadAll,
                        JepsenOpValue::Balances(vec![(dir_a, ba), (dir_b, bb)]),
                    );
                }
                None => errors.push("final post-quiesce dirstat bank audit failed".to_string()),
            }
        }
        JepsenWorkload::Register => {}
    }

    // Stop all 3 OS processes cleanly.
    {
        let mut guard = nodes_arc.lock().await;
        for node in guard.iter_mut() {
            node.stop_graceful();
        }
    }

    finalize_jepsen_report(
        cfg,
        "real-3-process-os-cluster",
        &recorder.snapshot(),
        &reg_initials,
        &final_set_items,
        bank_expected_total,
        nemesis_actions.load(Ordering::Relaxed) as usize,
        errors,
        started.elapsed(),
    )
}

/// Runs a Jepsen test against a **3-voter real TCP/gRPC Multi-Raft cluster** equipped with
/// [`SwizzleClogController`] network partitions, asymmetric link clogs, bounded packet jitter,
/// and post-apply reply drops!
pub async fn run_partition_jepsen(cfg: &JepsenConfig) -> JepsenReport {
    let started = Instant::now();
    let tmp = JepsenTempDir::new("part3", cfg.seed);
    let _buggify = GlobalBuggifyGuard::install(cfg.seed, BuggifyConfig::default());
    let controller = SwizzleClogController::new(cfg.seed);

    let make_cfg = |nid: u64, join: Option<String>| {
        let mut c = ServerTomlConfig::default();
        c.node.node_id = nid;
        c.node.cell_id = 1;
        c.node.data_dir = tmp.path().join(format!("n{nid}"));
        c.node.grpc_listen_addr = "127.0.0.1:0".to_string();
        c.security.allow_unauthenticated = true;
        c.sharding.bootstrap_table_shards = true;
        c.sharding.shard_split_threshold_bytes = 64 * 1024 * 1024;
        c.raft_options.heartbeat_interval_ms = 60;
        c.raft_options.election_timeout_min_ms = 180;
        c.raft_options.election_timeout_max_ms = 340;
        c.cluster.join = join.unwrap_or_default();
        c.cluster.join_timeout_secs = Some(20);
        c
    };

    let (n1, addr1) = match PersistentMetaDbCluster::open_and_serve(make_cfg(1, None)).await {
        Ok(p) => p,
        Err(e) => {
            return JepsenReport {
                workload: cfg.workload,
                nemesis: JepsenNemesisKind::PartitionSwizzleClog,
                seed: cfg.seed,
                execution_mode: "real-3-voter-swizzle-clog-partition",
                total_events: 0,
                ok_ops: 0,
                fail_ops: 0,
                info_ops: 0,
                nemesis_actions: 0,
                linearizability: None,
                elle: None,
                set_full: None,
                bank: None,
                errors: vec![format!("boot node 1 failed: {e}")],
                elapsed: started.elapsed(),
                history_digest: 0,
            };
        }
    };
    let (n2, addr2) =
        match PersistentMetaDbCluster::open_and_serve(make_cfg(2, Some(addr1.clone()))).await {
            Ok(p) => p,
            Err(e) => {
                let _ = n1.shutdown().await;
                return JepsenReport {
                    workload: cfg.workload,
                    nemesis: JepsenNemesisKind::PartitionSwizzleClog,
                    seed: cfg.seed,
                    execution_mode: "real-3-voter-swizzle-clog-partition",
                    total_events: 0,
                    ok_ops: 0,
                    fail_ops: 0,
                    info_ops: 0,
                    nemesis_actions: 0,
                    linearizability: None,
                    elle: None,
                    set_full: None,
                    bank: None,
                    errors: vec![format!("boot node 2 failed: {e}")],
                    elapsed: started.elapsed(),
                    history_digest: 0,
                };
            }
        };
    let (n3, addr3) =
        match PersistentMetaDbCluster::open_and_serve(make_cfg(3, Some(addr1.clone()))).await {
            Ok(p) => p,
            Err(e) => {
                let _ = n2.shutdown().await;
                let _ = n1.shutdown().await;
                return JepsenReport {
                    workload: cfg.workload,
                    nemesis: JepsenNemesisKind::PartitionSwizzleClog,
                    seed: cfg.seed,
                    execution_mode: "real-3-voter-swizzle-clog-partition",
                    total_events: 0,
                    ok_ops: 0,
                    fail_ops: 0,
                    info_ops: 0,
                    nemesis_actions: 0,
                    linearizability: None,
                    elle: None,
                    set_full: None,
                    bank: None,
                    errors: vec![format!("boot node 3 failed: {e}")],
                    elapsed: started.elapsed(),
                    history_digest: 0,
                };
            }
        };

    // Wait for 3-voter readiness on all bootstrap shards.
    for _ in 0..120 {
        let ready = n1.cell_manager().list_routes().iter().all(|r| {
            r.peers
                .iter()
                .filter(|(_, (_, v))| *v)
                .map(|(&id, _)| id)
                .collect::<BTreeSet<_>>()
                == BTreeSet::from([1, 2, 3])
        });
        if ready {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Attach `SwizzleClogController` to all 3 voters' Raft & control-plane `ChannelPool`s!
    let addrs = vec![addr1.clone(), addr2.clone(), addr3.clone()];
    for (node, addr) in [(&n1, &addr1), (&n2, &addr2), (&n3, &addr3)] {
        if let Some(host) = node.host() {
            host.channel_pool()
                .set_network_chaos_hook(addr.clone(), Some(controller.clone()));
        }
    }

    let cluster_nodes = vec![n1.clone(), n2.clone(), n3.clone()];

    // Seed directories & initial keys before enabling network partitions.
    let dir_a = 100u64;
    let dir_b = 600u64;
    let num_registers = 3u64;
    let num_bank_accounts = 4u64;
    let bank_init_bal = 1_000i64;
    let bank_expected_total = (num_bank_accounts as i64) * bank_init_bal;
    let mut reg_initials = BTreeMap::new();
    let mut errors = Vec::new();

    for (pid, name) in [(dir_a, "pdir_a"), (dir_b, "pdir_b")] {
        if let Err(e) = n1
            .tx_router()
            .mkdir(ROOT_INODE_ID, name, pid, 10, 0o755)
            .await
        {
            errors.push(format!("setup mkdir {name}: {e}"));
        }
    }
    match cfg.workload {
        JepsenWorkload::Register => {
            for k in 0..num_registers {
                if let Err(e) = n1
                    .tx_router()
                    .create_file(
                        dir_a,
                        &format!("reg_{k}"),
                        make_file_inode_id(10_000 + k),
                        50_000 + k,
                        10,
                        0o644,
                        0,
                    )
                    .await
                {
                    errors.push(format!("setup reg_{k}: {e}"));
                }
                reg_initials.insert(k, 0);
            }
        }
        JepsenWorkload::Set => {}
        JepsenWorkload::RenameElle => {
            if let Err(e) = n1
                .tx_router()
                .create_file(
                    dir_a,
                    "token",
                    make_file_inode_id(20_001),
                    60_001,
                    10,
                    0o644,
                    1,
                )
                .await
            {
                errors.push(format!("setup token: {e}"));
            }
        }
        JepsenWorkload::Bank => {
            for acct in 0..num_bank_accounts {
                let pid = if acct < 2 { dir_a } else { dir_b };
                if let Err(e) = n1
                    .tx_router()
                    .create_file(
                        pid,
                        &format!("bill_{acct}"),
                        make_file_inode_id(30_000 + acct),
                        70_000 + acct,
                        10,
                        0o644,
                        bank_init_bal,
                    )
                    .await
                {
                    errors.push(format!("setup bill_{acct}: {e}"));
                }
            }
            // Wait for async `AuxOutbox` to settle initial seeded bytes on `SHARD_QUOTA`.
            let expected_total = (num_bank_accounts as i64) * bank_init_bal;
            let settle_deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < settle_deadline {
                if let Ok(Some(q)) = n1.tx_router().get_quota(10).await {
                    if q.used_bytes == expected_total {
                        break;
                    }
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }

    let recorder = HistoryRecorder::new();
    let stop_flag = Arc::new(AtomicBool::new(false));
    let global_seq = Arc::new(AtomicU64::new(2));
    let nemesis_actions = Arc::new(AtomicU64::new(0));

    // Spawn concurrent clients hitting all 3 voters while `SwizzleClogController` partitions them!
    let client_count = cfg.clients.max(2);
    let mut handles = Vec::with_capacity(client_count);
    for cid in 0..client_count {
        let rec = recorder.clone();
        let stop = stop_flag.clone();
        let seq = global_seq.clone();
        let nodes = cluster_nodes.clone();
        let workload = cfg.workload;
        let base_seed = cfg.seed ^ ((cid as u64 + 17).wrapping_mul(0x9e37_79b9_7f4a_7c15));
        let proc_id = JepsenProcess::Client(cid as u32);

        handles.push(tokio::spawn(async move {
            let mut rng = base_seed;
            let mut next_u64 = || -> u64 {
                rng = splitmix64(rng.wrapping_add(0x9e37_79b9_7f4a_7c15));
                rng
            };
            let mut step = 0u64;
            while !stop.load(Ordering::Acquire) {
                step += 1;
                let node = &nodes[(next_u64() as usize) % nodes.len()];
                match workload {
                    JepsenWorkload::Register => {
                        let key = next_u64() % num_registers;
                        let is_write = (next_u64() % 100) < 45;
                        if is_write {
                            let val = seq.fetch_add(1, Ordering::AcqRel) as i64;
                            let kind = JepsenOpKind::Write { key, value: val };
                            rec.invoke(proc_id, kind.clone());
                            match tokio::time::timeout(
                                Duration::from_millis(900),
                                node.tx_router().setattr(
                                    dir_a,
                                    &format!("reg_{key}"),
                                    SetAttrPatch {
                                        size_bytes: Some(val),
                                        ..Default::default()
                                    },
                                ),
                            )
                            .await
                            {
                                Ok(Ok(_)) => {
                                    rec.complete_ok(proc_id, kind, JepsenOpValue::Int(val));
                                }
                                Ok(Err(e)) => {
                                    rec.complete_info(proc_id, kind, e.to_string());
                                }
                                Err(_) => {
                                    rec.complete_info(proc_id, kind, "timeout");
                                }
                            }
                        } else {
                            let kind = JepsenOpKind::Read { key };
                            rec.invoke(proc_id, kind.clone());
                            match tokio::time::timeout(
                                Duration::from_millis(900),
                                node.tx_router().lookup(dir_a, &format!("reg_{key}")),
                            )
                            .await
                            {
                                Ok(Ok(opt)) => {
                                    let val = opt.map(|e| e.attr.size_bytes);
                                    rec.complete_ok(proc_id, kind, JepsenOpValue::OptInt(val));
                                }
                                Ok(Err(e)) => {
                                    rec.complete_fail(proc_id, kind, e.to_string());
                                }
                                Err(_) => {
                                    rec.complete_fail(proc_id, kind, "timeout");
                                }
                            }
                        }
                    }
                    JepsenWorkload::Set => {
                        let item = seq.fetch_add(1, Ordering::AcqRel);
                        let kind = JepsenOpKind::SetAdd { item };
                        rec.invoke(proc_id, kind.clone());
                        let pid = if item.is_multiple_of(2) { dir_a } else { dir_b };
                        match tokio::time::timeout(
                            Duration::from_millis(900),
                            node.tx_router().create_file(
                                pid,
                                &format!("item_{item:08}"),
                                make_file_inode_id(100_000 + item),
                                200_000 + item,
                                10,
                                0o644,
                                64,
                            ),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                rec.complete_ok(proc_id, kind, JepsenOpValue::Int(item as i64));
                            }
                            Ok(Err(e)) => {
                                rec.complete_info(proc_id, kind, e.to_string());
                            }
                            Err(_) => {
                                rec.complete_info(proc_id, kind, "timeout");
                            }
                        }
                    }
                    JepsenWorkload::RenameElle => {
                        let txn_id = seq.fetch_add(1, Ordering::AcqRel);
                        if cid == 0 {
                            let (src_pid, dst_pid) = if step % 2 == 1 {
                                (dir_a, dir_b)
                            } else {
                                (dir_b, dir_a)
                            };
                            let kind = JepsenOpKind::TxnRename {
                                txn_id,
                                src_slot: src_pid,
                                dst_slot: dst_pid,
                                version: txn_id,
                            };
                            rec.invoke(proc_id, kind.clone());
                            let stamped = tokio::time::timeout(
                                Duration::from_millis(800),
                                node.tx_router().setattr(
                                    src_pid,
                                    "token",
                                    SetAttrPatch {
                                        size_bytes: Some(txn_id as i64),
                                        ..Default::default()
                                    },
                                ),
                            )
                            .await
                            .is_ok_and(|r| r.is_ok());
                            if !stamped {
                                rec.complete_fail(proc_id, kind, "source not at expected slot");
                            } else {
                                match tokio::time::timeout(
                                    Duration::from_millis(1_200),
                                    node.tx_router()
                                        .rename(src_pid, "token", dst_pid, "token", true),
                                )
                                .await
                                {
                                    Ok(Ok(_)) => {
                                        rec.complete_ok(
                                            proc_id,
                                            kind,
                                            JepsenOpValue::RenameCommitted { version: txn_id },
                                        );
                                    }
                                    Ok(Err(e)) => {
                                        rec.complete_info(proc_id, kind, e.to_string());
                                    }
                                    Err(_) => {
                                        rec.complete_info(proc_id, kind, "timeout");
                                    }
                                }
                            }
                        } else {
                            let kind = JepsenOpKind::TxnReadPair {
                                txn_id,
                                slot_a: dir_a,
                                slot_b: dir_b,
                            };
                            rec.invoke(proc_id, kind.clone());
                            let la = tokio::time::timeout(
                                Duration::from_millis(600),
                                node.tx_router().lookup(dir_a, "token"),
                            )
                            .await;
                            let lb = tokio::time::timeout(
                                Duration::from_millis(600),
                                node.tx_router().lookup(dir_b, "token"),
                            )
                            .await;
                            if let (Ok(Ok(oa)), Ok(Ok(ob))) = (la, lb) {
                                let va = oa.map(|e| e.attr.size_bytes as u64);
                                let vb = ob.map(|e| e.attr.size_bytes as u64);
                                if va.is_some() ^ vb.is_some() {
                                    rec.complete_ok(
                                        proc_id,
                                        kind,
                                        JepsenOpValue::PairRead {
                                            val_a: va,
                                            val_b: vb,
                                        },
                                    );
                                } else {
                                    rec.complete_fail(proc_id, kind, "straddled rename");
                                }
                            } else {
                                rec.complete_fail(proc_id, kind, "lookup error");
                            }
                        }
                    }
                    JepsenWorkload::Bank => {
                        let acct = next_u64() % num_bank_accounts;
                        let (src_pid, dst_pid) = if (next_u64() % 2) == 0 {
                            (dir_a, dir_b)
                        } else {
                            (dir_b, dir_a)
                        };
                        let kind = JepsenOpKind::BankTransfer {
                            from: src_pid,
                            to: dst_pid,
                            amount: bank_init_bal,
                        };
                        rec.invoke(proc_id, kind.clone());
                        match tokio::time::timeout(
                            Duration::from_millis(900),
                            node.tx_router().rename(
                                src_pid,
                                &format!("bill_{acct}"),
                                dst_pid,
                                &format!("bill_{acct}"),
                                false,
                            ),
                        )
                        .await
                        {
                            Ok(Ok(_)) => {
                                rec.complete_ok(proc_id, kind, JepsenOpValue::Int(bank_init_bal));
                            }
                            Ok(Err(e)) => {
                                rec.complete_info(proc_id, kind, e.to_string());
                            }
                            Err(_) => {
                                rec.complete_info(proc_id, kind, "timeout");
                            }
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }));
    }

    // Spawn Swizzle-Clog network partition nemesis!
    let nemesis_handle = {
        let stop = stop_flag.clone();
        let rec = recorder.clone();
        let ctrl = controller.clone();
        let nem_ops = nemesis_actions.clone();
        let addrs_copy = addrs.clone();
        let seed = cfg.seed;
        tokio::spawn(async move {
            let mut rng = splitmix64(seed ^ 0x5A12_21EC);
            let mut round = 0u64;
            while !stop.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(600)).await;
                if stop.load(Ordering::Acquire) {
                    break;
                }
                round += 1;
                rng = splitmix64(rng.wrapping_add(0x9e37_79b9_7f4a_7c15));
                ctrl.set_max_jitter_ms(18);
                ctrl.set_drop_reply_permille(100);
                let victim = &addrs_copy[(rng as usize) % addrs_copy.len()];
                match round % 3 {
                    1 => {
                        let label = format!("isolate-node-{victim}");
                        rec.nemesis_start(label.clone());
                        nem_ops.fetch_add(1, Ordering::Relaxed);
                        ctrl.isolate_node(victim);
                        tokio::time::sleep(Duration::from_millis(850)).await;
                        ctrl.heal_all();
                        rec.nemesis_stop(label);
                    }
                    2 => {
                        let peer = &addrs_copy[((rng as usize) + 1) % addrs_copy.len()];
                        let label = format!("asymmetric-clog-{victim}-to-{peer}");
                        rec.nemesis_start(label.clone());
                        nem_ops.fetch_add(1, Ordering::Relaxed);
                        ctrl.clog_link(victim, peer);
                        tokio::time::sleep(Duration::from_millis(700)).await;
                        ctrl.unclog_link(victim, peer);
                        rec.nemesis_stop(label);
                    }
                    _ => {
                        rec.nemesis_start("swizzle-clog-wave");
                        nem_ops.fetch_add(1, Ordering::Relaxed);
                        let plan = SwizzleClogController::plan_swizzle_clog(&addrs_copy, rng, 2);
                        for (a, b) in &plan.clog_order {
                            ctrl.clog_link(a, b);
                            tokio::time::sleep(Duration::from_millis(120)).await;
                        }
                        tokio::time::sleep(Duration::from_millis(400)).await;
                        for (a, b) in &plan.unclog_order {
                            ctrl.unclog_link(a, b);
                            tokio::time::sleep(Duration::from_millis(80)).await;
                        }
                        ctrl.heal_all();
                        rec.nemesis_stop("swizzle-clog-wave");
                    }
                }
            }
            ctrl.heal_all();
            ctrl.set_max_jitter_ms(0);
            ctrl.set_drop_reply_permille(0);
        })
    };

    tokio::time::sleep(Duration::from_secs(cfg.duration_secs.max(3))).await;
    stop_flag.store(true, Ordering::Release);
    let _ = nemesis_handle.await;
    for h in handles {
        let _ = h.await;
    }

    // Heal all network links & drive recovery + aux convergence.
    controller.heal_all();
    controller.set_max_jitter_ms(0);
    controller.set_drop_reply_permille(0);
    tokio::time::sleep(Duration::from_millis(600)).await;
    for node in &cluster_nodes {
        let _ = node.run_maintenance_tick().await;
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut final_set_items = BTreeSet::new();
    match cfg.workload {
        JepsenWorkload::Set => {
            for pid in [dir_a, dir_b] {
                let mut cursor: Option<String> = None;
                for _ in 0..200 {
                    let mut page_opt = None;
                    let page_deadline = Instant::now() + Duration::from_secs(12);
                    while Instant::now() < page_deadline {
                        for node in &cluster_nodes {
                            if let Ok(p) = node
                                .tx_router()
                                .listdir_with_stats(
                                    pid,
                                    cursor.as_deref(),
                                    Some("item_"),
                                    Some(128),
                                )
                                .await
                            {
                                page_opt = Some(p);
                                break;
                            }
                        }
                        if page_opt.is_some() {
                            break;
                        }
                        tokio::time::sleep(Duration::from_millis(150)).await;
                    }
                    let Some(page) = page_opt else {
                        errors.push(format!("final listdir pid={pid} failed"));
                        break;
                    };
                    for e in &page.entries {
                        if let Some(id_str) = e.key.name.strip_prefix("item_") {
                            if let Ok(id) = id_str.parse::<u64>() {
                                final_set_items.insert(id);
                            }
                        }
                    }
                    if page.next_cursor.is_none() {
                        break;
                    }
                    cursor = page.next_cursor;
                }
            }
        }
        JepsenWorkload::RenameElle => {
            let mut obs = None;
            let lookup_deadline = Instant::now() + Duration::from_secs(12);
            while Instant::now() < lookup_deadline {
                for node in &cluster_nodes {
                    if let (Ok(oa), Ok(ob)) = (
                        node.tx_router().lookup(dir_a, "token").await,
                        node.tx_router().lookup(dir_b, "token").await,
                    ) {
                        obs = Some((
                            oa.map(|e| e.attr.size_bytes as u64),
                            ob.map(|e| e.attr.size_bytes as u64),
                        ));
                        break;
                    }
                }
                if obs.is_some() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(150)).await;
            }
            if let Some((va, vb)) = obs {
                let tid = global_seq.fetch_add(1, Ordering::AcqRel);
                let kind = JepsenOpKind::TxnReadPair {
                    txn_id: tid,
                    slot_a: dir_a,
                    slot_b: dir_b,
                };
                recorder.invoke(JepsenProcess::Client(999), kind.clone());
                recorder.complete_ok(
                    JepsenProcess::Client(999),
                    kind,
                    JepsenOpValue::PairRead {
                        val_a: va,
                        val_b: vb,
                    },
                );
            } else {
                errors.push("final post-partition rename token lookup failed".to_string());
            }
        }
        JepsenWorkload::Bank => {
            let mut dirstat_pair = None;
            for node in &cluster_nodes {
                if let (Ok(aa), Ok(ab)) = (
                    node.tx_router().dirstat(dir_a).await,
                    node.tx_router().dirstat(dir_b).await,
                ) {
                    dirstat_pair = Some((aa.size_bytes, ab.size_bytes));
                    break;
                }
            }
            if let Some((ba, bb)) = dirstat_pair {
                recorder.invoke(JepsenProcess::Client(999), JepsenOpKind::BankReadAll);
                recorder.complete_ok(
                    JepsenProcess::Client(999),
                    JepsenOpKind::BankReadAll,
                    JepsenOpValue::Balances(vec![(dir_a, ba), (dir_b, bb)]),
                );
            } else {
                errors.push("final post-partition bank dirstat failed".to_string());
            }
        }
        JepsenWorkload::Register => {}
    }

    // Verify 3-voter route contiguity after partition heal.
    for node in &cluster_nodes {
        let mut routes = node.cell_manager().list_routes();
        routes.sort_by(|a, b| a.range.start_key.cmp(&b.range.start_key));
        for pair in routes.windows(2) {
            if pair[0].range.end_key != pair[1].range.start_key {
                errors.push(format!(
                    "route gap/overlap between shard {} and shard {}",
                    pair[0].shard_id, pair[1].shard_id
                ));
            }
        }
        // Verify local routing still works for `dir_a`.
        let _ = node
            .global_manager()
            .route_user_key(&metadb_core::MetaKey::dir_attr(dir_a).encode());
    }

    for node in cluster_nodes {
        let _ = node.shutdown().await;
    }

    finalize_jepsen_report(
        cfg,
        "real-3-voter-swizzle-clog-partition",
        &recorder.snapshot(),
        &reg_initials,
        &final_set_items,
        bank_expected_total,
        nemesis_actions.load(Ordering::Relaxed) as usize,
        errors,
        started.elapsed(),
    )
}

#[allow(clippy::too_many_arguments)]
fn finalize_jepsen_report(
    cfg: &JepsenConfig,
    execution_mode: &'static str,
    history: &JepsenHistory,
    reg_initials: &BTreeMap<u64, i64>,
    final_set_items: &BTreeSet<u64>,
    bank_expected_total: i64,
    nemesis_actions: usize,
    mut errors: Vec<String>,
    elapsed: Duration,
) -> JepsenReport {
    if let Some(edn_path) = &cfg.edn_output_path {
        if let Err(e) = history.write_edn_file(edn_path) {
            errors.push(format!(
                "failed to write Jepsen EDN history to {}: {e}",
                edn_path.display()
            ));
        }
    }

    let ok_ops = history
        .events
        .iter()
        .filter(|e| e.process != JepsenProcess::Nemesis && e.op_type == JepsenOpType::Ok)
        .count();
    let fail_ops = history
        .events
        .iter()
        .filter(|e| e.process != JepsenProcess::Nemesis && e.op_type == JepsenOpType::Fail)
        .count();
    let info_ops = history
        .events
        .iter()
        .filter(|e| e.process != JepsenProcess::Nemesis && e.op_type == JepsenOpType::Info)
        .count();

    let mut linearizability = None;
    let mut elle = None;
    let mut set_full = None;
    let mut bank = None;

    match cfg.workload {
        JepsenWorkload::Register => {
            linearizability = Some(check_register_linearizability(history, reg_initials));
        }
        JepsenWorkload::Set => {
            set_full = Some(check_set_full(history, final_set_items));
        }
        JepsenWorkload::RenameElle => {
            elle = Some(check_elle_rename_history(history, 1));
        }
        JepsenWorkload::Bank => {
            bank = Some(check_bank_conservation(history, bank_expected_total));
        }
    }

    JepsenReport {
        workload: cfg.workload,
        nemesis: cfg.nemesis,
        seed: cfg.seed,
        execution_mode,
        total_events: history.len(),
        ok_ops,
        fail_ops,
        info_ops,
        nemesis_actions,
        linearizability,
        elle,
        set_full,
        bank,
        errors,
        elapsed,
        history_digest: history.digest(false),
    }
}

#[cfg(test)]
mod checker_unit_tests {
    use super::*;

    #[test]
    fn wgl_accepts_concurrent_linearizable_history_with_indeterminate_info_write() {
        let rec = HistoryRecorder::new();
        // Client 0 writes 10 at [10, 20] -> Ok
        rec.invoke_at(
            10,
            JepsenProcess::Client(0),
            JepsenOpKind::Write { key: 1, value: 10 },
        );
        rec.complete_ok_at(
            20,
            JepsenProcess::Client(0),
            JepsenOpKind::Write { key: 1, value: 10 },
            JepsenOpValue::Int(10),
        );
        // Client 1 writes 20 at [15, info] (crashed/timed-out write that secretly committed!)
        rec.invoke_at(
            15,
            JepsenProcess::Client(1),
            JepsenOpKind::Write { key: 1, value: 20 },
        );
        rec.complete_info_at(
            25,
            JepsenProcess::Client(1),
            JepsenOpKind::Write { key: 1, value: 20 },
            "timeout",
        );
        // Client 2 writes 99 at [18, info] (crashed write that did NOT commit!)
        rec.invoke_at(
            18,
            JepsenProcess::Client(2),
            JepsenOpKind::Write { key: 1, value: 99 },
        );
        rec.complete_info_at(
            22,
            JepsenProcess::Client(2),
            JepsenOpKind::Write { key: 1, value: 99 },
            "timeout",
        );
        // Client 3 reads 20 at [30, 40] -> Ok
        rec.invoke_at(30, JepsenProcess::Client(3), JepsenOpKind::Read { key: 1 });
        rec.complete_ok_at(
            40,
            JepsenProcess::Client(3),
            JepsenOpKind::Read { key: 1 },
            JepsenOpValue::OptInt(Some(20)),
        );

        let res = check_register_linearizability(&rec.snapshot(), &BTreeMap::from([(1, 0)]));
        assert!(res.linearizable, "expected linearizable: {res:?}");
        assert_eq!(res.indeterminate_ops, 2);
    }

    #[test]
    fn wgl_rejects_stale_read_after_completed_write() {
        let rec = HistoryRecorder::new();
        // Client 0 writes 42 at [10, 20] -> Ok
        rec.invoke_at(
            10,
            JepsenProcess::Client(0),
            JepsenOpKind::Write { key: 1, value: 42 },
        );
        rec.complete_ok_at(
            20,
            JepsenProcess::Client(0),
            JepsenOpKind::Write { key: 1, value: 42 },
            JepsenOpValue::Int(42),
        );
        // Client 1 starts AFTER write finished [25, 35] and observes stale initial value 0!
        rec.invoke_at(25, JepsenProcess::Client(1), JepsenOpKind::Read { key: 1 });
        rec.complete_ok_at(
            35,
            JepsenProcess::Client(1),
            JepsenOpKind::Read { key: 1 },
            JepsenOpValue::OptInt(Some(0)),
        );

        let res = check_register_linearizability(&rec.snapshot(), &BTreeMap::from([(1, 0)]));
        assert!(
            !res.linearizable,
            "WGL MUST reject stale read after completed write"
        );
    }

    #[test]
    fn elle_catches_g0_g1a_g1b_g1c_g_single_g2_item_and_strict_serializability() {
        // 1. G0 (ww cycle on keys 1 & 2)
        let t1 = ElleTransaction {
            txn_id: 1,
            invoke_ns: 1,
            complete_ns: 10,
            committed: true,
            failed: false,
            ops: vec![
                ElleMicroOp::Write { key: 1, value: 1 },
                ElleMicroOp::Write { key: 2, value: 2 },
            ],
        };
        let t2 = ElleTransaction {
            txn_id: 2,
            invoke_ns: 2,
            complete_ns: 11,
            committed: true,
            failed: false,
            ops: vec![
                ElleMicroOp::Write { key: 1, value: 2 },
                ElleMicroOp::Write { key: 2, value: 1 },
            ],
        };
        let r_g0 = check_elle_graph(&[t1, t2], &BTreeMap::new());
        assert!(!r_g0.valid);
        assert_eq!(r_g0.anomalies[0].kind, ElleAnomalyKind::G0);

        // 2. G1a (aborted read)
        let t_abort = ElleTransaction {
            txn_id: 10,
            invoke_ns: 1,
            complete_ns: 5,
            committed: false,
            failed: true,
            ops: vec![ElleMicroOp::Write { key: 1, value: 99 }],
        };
        let t_read = ElleTransaction {
            txn_id: 11,
            invoke_ns: 6,
            complete_ns: 9,
            committed: true,
            failed: false,
            ops: vec![ElleMicroOp::Read {
                key: 1,
                value: Some(99),
            }],
        };
        let r_g1a = check_elle_graph(&[t_abort, t_read], &BTreeMap::new());
        assert!(!r_g1a.valid);
        assert_eq!(r_g1a.anomalies[0].kind, ElleAnomalyKind::G1a);

        // 3. G1b (fractured rename read observing BOTH slots present)
        let rec = HistoryRecorder::new();
        rec.invoke_at(
            10,
            JepsenProcess::Client(0),
            JepsenOpKind::TxnReadPair {
                txn_id: 5,
                slot_a: 100,
                slot_b: 600,
            },
        );
        rec.complete_ok_at(
            20,
            JepsenProcess::Client(0),
            JepsenOpKind::TxnReadPair {
                txn_id: 5,
                slot_a: 100,
                slot_b: 600,
            },
            JepsenOpValue::PairRead {
                val_a: Some(1),
                val_b: Some(1),
            },
        );
        let r_g1b = check_elle_rename_history(&rec.snapshot(), 1);
        assert!(!r_g1b.valid);
        assert_eq!(r_g1b.anomalies[0].kind, ElleAnomalyKind::G1b);

        // 4. G1c (wr cycle: T1 writes k1=1, reads k2=1; T2 writes k2=1, reads k1=1)
        let r_g1c = check_elle_graph(
            &[
                ElleTransaction {
                    txn_id: 1,
                    invoke_ns: 1,
                    complete_ns: 10,
                    committed: true,
                    failed: false,
                    ops: vec![
                        ElleMicroOp::Write { key: 1, value: 1 },
                        ElleMicroOp::Read {
                            key: 2,
                            value: Some(1),
                        },
                    ],
                },
                ElleTransaction {
                    txn_id: 2,
                    invoke_ns: 2,
                    complete_ns: 11,
                    committed: true,
                    failed: false,
                    ops: vec![
                        ElleMicroOp::Write { key: 2, value: 1 },
                        ElleMicroOp::Read {
                            key: 1,
                            value: Some(1),
                        },
                    ],
                },
            ],
            &BTreeMap::new(),
        );
        assert!(!r_g1c.valid);
        assert_eq!(r_g1c.anomalies[0].kind, ElleAnomalyKind::G1c);

        // 5. G2-item (write skew: T1 reads k1=None, writes k2=1; T2 reads k2=None, writes k1=1)
        let r_g2 = check_elle_graph(
            &[
                ElleTransaction {
                    txn_id: 1,
                    invoke_ns: 1,
                    complete_ns: 10,
                    committed: true,
                    failed: false,
                    ops: vec![
                        ElleMicroOp::Read {
                            key: 1,
                            value: None,
                        },
                        ElleMicroOp::Write { key: 2, value: 1 },
                    ],
                },
                ElleTransaction {
                    txn_id: 2,
                    invoke_ns: 2,
                    complete_ns: 11,
                    committed: true,
                    failed: false,
                    ops: vec![
                        ElleMicroOp::Read {
                            key: 2,
                            value: None,
                        },
                        ElleMicroOp::Write { key: 1, value: 1 },
                    ],
                },
            ],
            &BTreeMap::new(),
        );
        assert!(!r_g2.valid);
        assert_eq!(r_g2.anomalies[0].kind, ElleAnomalyKind::G2Item);

        // 6. Strict-Serializability (`rt` violation: T1 writes k1=1 at [1,5], T2 reads k1=None at [10,15])
        let r_rt = check_elle_graph(
            &[
                ElleTransaction {
                    txn_id: 1,
                    invoke_ns: 1,
                    complete_ns: 5,
                    committed: true,
                    failed: false,
                    ops: vec![ElleMicroOp::Write { key: 1, value: 1 }],
                },
                ElleTransaction {
                    txn_id: 2,
                    invoke_ns: 10,
                    complete_ns: 15,
                    committed: true,
                    failed: false,
                    ops: vec![ElleMicroOp::Read {
                        key: 1,
                        value: None,
                    }],
                },
            ],
            &BTreeMap::new(),
        );
        assert!(!r_rt.valid);
        assert_eq!(
            r_rt.anomalies[0].kind,
            ElleAnomalyKind::StrictSerializability
        );
    }

    #[test]
    fn set_full_and_bank_checkers_detect_lost_write_and_conservation_loss() {
        let rec = HistoryRecorder::new();
        rec.invoke_at(
            1,
            JepsenProcess::Client(0),
            JepsenOpKind::SetAdd { item: 10 },
        );
        rec.complete_ok_at(
            5,
            JepsenProcess::Client(0),
            JepsenOpKind::SetAdd { item: 10 },
            JepsenOpValue::Int(10),
        );
        rec.invoke_at(
            6,
            JepsenProcess::Client(0),
            JepsenOpKind::SetAdd { item: 11 },
        );
        rec.complete_ok_at(
            9,
            JepsenProcess::Client(0),
            JepsenOpKind::SetAdd { item: 11 },
            JepsenOpValue::Int(11),
        );
        let sf = check_set_full(&rec.snapshot(), &BTreeSet::from([10]));
        assert!(!sf.valid);
        assert_eq!(sf.lost_ok, vec![11]);

        let brec = HistoryRecorder::new();
        brec.invoke_at(1, JepsenProcess::Client(0), JepsenOpKind::BankReadAll);
        brec.complete_ok_at(
            2,
            JepsenProcess::Client(0),
            JepsenOpKind::BankReadAll,
            JepsenOpValue::Balances(vec![(1, 500), (2, 400)]),
        );
        let br = check_bank_conservation(&brec.snapshot(), 1000);
        assert!(!br.valid);
        assert_eq!(br.violations.len(), 1);
    }

    #[test]
    fn edn_export_emits_valid_jepsen_syntax() {
        let rec = HistoryRecorder::new();
        rec.invoke_at(
            100,
            JepsenProcess::Client(0),
            JepsenOpKind::Write { key: 1, value: 7 },
        );
        rec.complete_ok_at(
            200,
            JepsenProcess::Client(0),
            JepsenOpKind::Write { key: 1, value: 7 },
            JepsenOpValue::Int(7),
        );
        rec.nemesis_start("partition");
        let edn = rec.snapshot().to_edn();
        assert!(edn.contains(":type :invoke :f :write :value [1 7]"));
        assert!(edn.contains(":type :ok :f :write :value [1 7]"));
        assert!(edn.contains(":process :nemesis :type :info :f :start-nemesis"));
    }
}
