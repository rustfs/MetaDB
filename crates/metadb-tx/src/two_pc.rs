//! Unified Replicated Transaction Router (`DistributedTxRouter`), Linearizable Read Engine
//! (including `DualModeFineGrained` shared-`barrier_hlc` scatter-gather), and Crash-Recoverable
//! Primary-Anchored Two-Phase Commit (`2PC`) Coordinator.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use metadb_core::codec;
use metadb_core::schema::validate_user_entry_name;
use metadb_core::{
    negate_accounted_size, saturating_add_i64, validate_dir_inode_id, validate_file_size,
    AttrDeltaSpec, AuxIntentSpec, DirShardDeltaKey, DirShardDeltaRecord, EntryType, FileId,
    HlcTimestamp, HybridLogicalClock, InodeAttr, InodeId, IntentOp, MetaDbError, MetaEntry,
    MetaKey, QuotaKey, QuotaRecord, Result, SecondaryIndexKey, SecondaryIndexRecord, ShardEpoch,
    ShardId, TxnId, TxnRecordKey, TxnState, TxnStatusRecord, UserId, WriteIntentKey,
    WriteIntentRecord, ATTR_ENTRY_NAME, TABLE_PREFIX_QUOTA, TABLE_PREFIX_SECONDARY_INDEX,
};
use metadb_raft::ShardRaftGroup;
use metadb_storage::{ScanControl, SnapshotGuard};

use crate::leader_forward::{forward_op, ForwardedRouterOp, ForwardedRouterOut};
use crate::recovery_worker::TxnRecoveryWorker;
use crate::rename_cycle::RenameCycleWalker;
use crate::state_machine_impl::{
    encode_proposed_command, fresh_proposal_id, SetAttrPatch, TxRaftCmd, TxRaftOutput,
    TxStateMachineHandler, MAX_2PC_LEASE_LIFETIME_MS,
};

pub const DEFAULT_2PC_TTL_MS: u64 = 3_000;

/// Test seam. The coordinator notifies `prepared` after every participant has
/// prepared and before primary commit, then waits until `release` is notified.
/// Lease renewal keeps running during the wait.
pub struct TwoPcPrepareGate {
    prepared: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

impl TwoPcPrepareGate {
    pub fn new() -> Self {
        Self {
            prepared: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        }
    }

    pub async fn wait_prepared(&self) {
        self.prepared.notified().await;
    }

    pub fn release(&self) {
        self.release.notify_one();
    }
}

impl Default for TwoPcPrepareGate {
    fn default() -> Self {
        Self::new()
    }
}

fn publish_quota(mut record: QuotaRecord) -> QuotaRecord {
    record.used_bytes = record.used_bytes.max(0);
    record.used_inodes = record.used_inodes.max(0);
    record
}

// A split fences its parent before booting and publishing its child. That
// interval includes Raft recovery and can outlast a short count-based retry loop.
const ROUTE_RETRY_TIMEOUT: Duration = Duration::from_secs(30);
const ROUTE_RETRY_INTERVAL: Duration = Duration::from_millis(25);
/// A follower that already knows another leader must return `NotLeader` at once.
/// Only an election with no hint is waited out, and only briefly.
const ELECTION_RETRY_TIMEOUT: Duration = Duration::from_secs(2);

async fn retry_routing<T, F, Fut>(operation: F) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let deadline = tokio::time::Instant::now() + ROUTE_RETRY_TIMEOUT;
    retry_routing_until(deadline, operation).await
}

async fn retry_routing_until<T, F, Fut>(
    deadline: tokio::time::Instant,
    mut operation: F,
) -> Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let election_deadline = tokio::time::Instant::now() + ELECTION_RETRY_TIMEOUT;
    loop {
        match operation().await {
            // Split publication is local and can take the full routing budget.
            // A known leader cannot appear by retrying on this follower.
            Err(
                error @ MetaDbError::NotLeader {
                    leader_hint: Some(_),
                    ..
                },
            ) => return Err(error),
            Err(
                error @ (MetaDbError::NotLeader { .. }
                | MetaDbError::RetryableConsensus {
                    kind: metadb_core::RetryableConsensusKind::QuorumUnavailable,
                    ..
                }),
            ) => {
                let now = tokio::time::Instant::now();
                let limit = deadline.min(election_deadline);
                if now >= limit {
                    return Err(error);
                }
                tokio::time::sleep(ROUTE_RETRY_INTERVAL.min(limit - now)).await;
            }
            Err(
                error @ (MetaDbError::EpochMismatch { .. } | MetaDbError::ShardRoutingError(_)),
            ) => {
                let now = tokio::time::Instant::now();
                if now >= deadline {
                    return Err(error);
                }
                tokio::time::sleep(ROUTE_RETRY_INTERVAL.min(deadline - now)).await;
            }
            result => return result,
        }
    }
}

/// Fault-injection hook allowing tests to simulate coordinator crashes mid-2PC (`T-2PC-1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TwoPcCrashStage {
    None,
    StopAfterPrepareBeforePrimaryCommit,
    StopAfterPrimaryCommitBeforeSecondary,
}

/// Fenced directory-rename lease issued by the Control Plane (`Resolution B13`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirRenameLease {
    pub rename_fence_token: u64,
    pub expire_hlc: HlcTimestamp,
}

pub type RoutedShardHandle = (ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>);

/// One shard's directory partition, as returned by `GetDirPartitionStat`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirPartitionStat {
    pub is_anchor: bool,
    pub attr: Option<InodeAttr>,
    pub max_applied_hlc: u64,
    pub anchor_missing: bool,
}
type PreparedShardBatch = (ShardEpoch, Arc<ShardRaftGroup>, Vec<Vec<u8>>);
type ShardIntentGroup = (ShardEpoch, Arc<ShardRaftGroup>, Vec<(Vec<u8>, IntentOp)>);

/// Routing & lease provider implemented by `metadb-cluster` (and lightweight test clusters).
#[tonic::async_trait]
pub trait ShardDirectory: Send + Sync + 'static {
    /// Resolves `(ShardEpoch, Arc<ShardRaftGroup>, Arc<TxStateMachineHandler>)` for `user_key`.
    fn route_user_key(&self, user_key: &[u8]) -> Result<RoutedShardHandle>;

    /// Allocates an identity that must never be reused, including after coordinator restart.
    /// Implementations must reserve the sequence durably before returning it.
    fn allocate_txn_id(&self, _coord_node_id: u64) -> Result<TxnId> {
        Err(MetaDbError::TransactionAborted(
            "ShardDirectory does not provide durable transaction IDs".to_string(),
        ))
    }

    /// Resolves all shards intersecting `[start_user_key, end_user_key)` in ascending key order.
    fn route_user_range(
        &self,
        start_user_key: &[u8],
        end_user_key: &[u8],
    ) -> Result<Vec<RoutedShardHandle>>;

    /// Acquires a monotonic fenced `DirRenameLease` for cross-parent directory renames.
    async fn acquire_dir_rename_lease(
        &self,
        moved_dir_id: InodeId,
        ttl_ms: u64,
    ) -> Result<DirRenameLease>;

    /// Releases a `DirRenameLease` on completion.
    async fn release_dir_rename_lease(&self, token: u64);

    /// Extends the active directory-rename lease for `token`.
    ///
    /// `Ok(None)` means this directory does not renew rename leases. The
    /// coordinator then treats the granted `expire_hlc` as a hard cap and
    /// aborts when that cap passes. `Ok(Some(expire))` is the new deadline.
    /// `Err` means the token is not current, the lease has already expired,
    /// or the lifetime cap has been reached; the coordinator must abort.
    async fn extend_dir_rename_lease(
        &self,
        token: u64,
        ttl_ms: u64,
    ) -> Result<Option<HlcTimestamp>> {
        let _ = (token, ttl_ms);
        Ok(None)
    }

    /// Dial address of `node_id` in `shard_id`'s applied membership, when this process hosts it.
    fn raft_peer_addr(&self, _shard_id: u64, _node_id: u64) -> Option<String> {
        None
    }

    /// Local replica for `shard_id`, when this process hosts it.
    fn local_shard(&self, _shard_id: u64) -> Result<RoutedShardHandle> {
        Err(MetaDbError::ShardRoutingError(
            "directory cannot look up a shard by id".to_string(),
        ))
    }
}

struct LeaseKeeper {
    cancel: AtomicBool,
    failed: AtomicBool,
    prepared: Mutex<Vec<PreparedShardBatch>>,
    /// One durable `expire_hlc` per prepared participant batch. Empty before the
    /// first successful Prepare. The commit check uses the minimum.
    durable_expire_ms: Mutex<Vec<u64>>,
    /// Deadline assumed before any participant is prepared. Not raised by a renew
    /// that has not been applied.
    initial_expire_ms: AtomicU64,
    deadline_ms: u64,
    rename_cap_ms: AtomicU64,
    /// Mirrors the minimum durable deadline after each successful Prepare or a
    /// renew that every prepared participant applied. Tests read this.
    publish_probe: Option<Arc<AtomicU64>>,
    /// Test hook. Each remaining count skips one participant proposal as a
    /// transient failure and does not advance the in-memory deadline.
    transient_faults: Option<Arc<AtomicU32>>,
}

impl LeaseKeeper {
    fn new(
        initial_expire_ms: u64,
        deadline_ms: u64,
        rename_cap_ms: u64,
        publish_probe: Option<Arc<AtomicU64>>,
        transient_faults: Option<Arc<AtomicU32>>,
    ) -> Self {
        Self {
            cancel: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            prepared: Mutex::new(Vec::new()),
            durable_expire_ms: Mutex::new(Vec::new()),
            initial_expire_ms: AtomicU64::new(initial_expire_ms),
            deadline_ms,
            rename_cap_ms: AtomicU64::new(rename_cap_ms),
            publish_probe,
            transient_faults,
        }
    }

    fn failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    fn observe(&self, batch: PreparedShardBatch) {
        self.prepared.lock().unwrap().push(batch);
    }

    fn clear(&self) {
        self.prepared.lock().unwrap().clear();
        self.durable_expire_ms.lock().unwrap().clear();
    }

    fn note_prepared_expire(&self, expire_ms: u64) {
        let mut floors = self.durable_expire_ms.lock().unwrap();
        floors.push(expire_ms);
        let min = floors.iter().copied().min().unwrap_or(expire_ms);
        drop(floors);
        self.record_published(min);
    }

    /// Raises every prepared participant's remembered deadline only after each
    /// of those participants has applied the same `expire_ms`.
    fn publish_durable(&self, expire_ms: u64) {
        let mut floors = self.durable_expire_ms.lock().unwrap();
        if floors.is_empty() {
            return;
        }
        for floor in floors.iter_mut() {
            *floor = expire_ms.max(*floor);
        }
        let min = floors.iter().copied().min().unwrap_or(expire_ms);
        drop(floors);
        self.record_published(min);
    }

    fn record_published(&self, expire_ms: u64) {
        if let Some(probe) = &self.publish_probe {
            probe.store(expire_ms, Ordering::Release);
        }
    }

    fn durable_floor_ms(&self) -> Option<u64> {
        self.durable_expire_ms.lock().unwrap().iter().copied().min()
    }

    fn expire_for(&self, now: HlcTimestamp, ttl_ms: u64) -> HlcTimestamp {
        let base = self
            .durable_floor_ms()
            .unwrap_or_else(|| self.initial_expire_ms.load(Ordering::Acquire));
        let proposed = now
            .physical_ms()
            .saturating_add(ttl_ms)
            .min(self.deadline_ms)
            .min(self.rename_cap_ms.load(Ordering::Acquire));
        HlcTimestamp::new(proposed.max(base), 0)
    }

    fn published(&self) -> HlcTimestamp {
        let ms = self
            .durable_floor_ms()
            .unwrap_or_else(|| self.initial_expire_ms.load(Ordering::Acquire));
        HlcTimestamp::new(ms, 0)
    }
}

struct LeaseTask {
    keeper: Arc<LeaseKeeper>,
    handle: tokio::task::JoinHandle<()>,
}

impl Drop for LeaseTask {
    fn drop(&mut self) {
        self.keeper.cancel.store(true, Ordering::Release);
        self.handle.abort();
    }
}

async fn renew_open_lease(
    directory: &dyn ShardDirectory,
    clock: &HybridLogicalClock,
    keeper: &LeaseKeeper,
    txn_id: TxnId,
    coord_node_id: u64,
    primary_key: &[u8],
    start_hlc: HlcTimestamp,
    rename_token: Option<u64>,
    ttl_ms: u64,
) -> bool {
    if keeper.cancel.load(Ordering::Acquire) {
        return true;
    }
    let now = clock.now();
    if now.physical_ms() >= keeper.deadline_ms {
        return false;
    }
    let mut cap_ms = keeper.deadline_ms;
    if let Some(token) = rename_token {
        match directory.extend_dir_rename_lease(token, ttl_ms).await {
            Ok(Some(expire)) => {
                cap_ms = cap_ms.min(expire.physical_ms());
                keeper
                    .rename_cap_ms
                    .store(expire.physical_ms(), Ordering::Release);
            }
            Ok(None) => {
                cap_ms = cap_ms.min(keeper.rename_cap_ms.load(Ordering::Acquire));
            }
            Err(error) if lease_renew_is_fatal(&error) => return false,
            Err(_) => return true,
        }
    }
    let proposed_ms = now.physical_ms().saturating_add(ttl_ms).min(cap_ms);
    let Some(published) = keeper.durable_floor_ms() else {
        // Nothing is prepared yet, so there is no durable deadline to move.
        return true;
    };
    if now.physical_ms() >= published {
        // The durable expire has passed. A late Extend2pcLease cannot revive it.
        return false;
    }
    if proposed_ms <= published {
        return true;
    }
    let shards = keeper.prepared.lock().unwrap().clone();
    if shards.is_empty() {
        return true;
    }
    let expire_hlc = HlcTimestamp::new(proposed_ms, 0);
    let mut all_applied = true;
    for (_epoch, _group, keys) in shards {
        if keeper.transient_faults.as_ref().is_some_and(|faults| {
            faults
                .try_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                    remaining.checked_sub(1)
                })
                .is_ok()
        }) {
            all_applied = false;
            continue;
        }
        let route_key = keys
            .first()
            .cloned()
            .unwrap_or_else(|| primary_key.to_vec());
        let Ok((epoch, group, _)) = directory.route_user_key(&route_key) else {
            all_applied = false;
            continue;
        };
        let cmd = TxRaftCmd::Extend2pcLease {
            txn_id,
            coord_node_id,
            expire_hlc,
            start_hlc,
            rename_fence_token: rename_token,
            target_user_keys: keys,
        };
        let Ok(payload) = encode_proposed_command(fresh_proposal_id(), &cmd) else {
            all_applied = false;
            continue;
        };
        match group.propose(epoch, now, payload).await {
            Ok(response) => match response.result {
                Ok(_) => {}
                Err(error) if lease_renew_is_fatal(&error) => return false,
                Err(_) => all_applied = false,
            },
            Err(error) if lease_renew_is_fatal(&error) => return false,
            Err(_) => all_applied = false,
        }
    }
    if all_applied {
        keeper.publish_durable(proposed_ms);
    }
    true
}

/// Fatal renew results are the typed [`MetaDbError::LeaseRenewRejected`] variant,
/// including a prepared participant that no longer has a live intent.
/// Transport and routing errors are retried and must not move the in-memory deadline.
fn lease_renew_is_fatal(error: &MetaDbError) -> bool {
    matches!(error, MetaDbError::LeaseRenewRejected(_))
}

/// Executes both `1-RTT` single-shard pushdown and Primary-Anchored `2PC` across `ShardRaftGroup`s.
pub struct DistributedTxRouter {
    coord_node_id: u64,
    clock: Arc<HybridLogicalClock>,
    directory: Arc<dyn ShardDirectory>,
    ttl_ms: u64,
    /// Hard cap past `start_hlc`. Defaults to [`MAX_2PC_LEASE_LIFETIME_MS`].
    max_lease_lifetime_ms: u64,
    forward_secret: String,
    pre_prepare_hold: Duration,
    post_prepare_gate: Option<Arc<TwoPcPrepareGate>>,
    abandon_after_prepare_gate: bool,
    lease_publish_probe: Option<Arc<AtomicU64>>,
    renew_transient_faults: Option<Arc<AtomicU32>>,
}

impl DistributedTxRouter {
    pub fn new(
        coord_node_id: u64,
        clock: Arc<HybridLogicalClock>,
        directory: Arc<dyn ShardDirectory>,
    ) -> Self {
        Self {
            coord_node_id,
            clock,
            directory,
            ttl_ms: DEFAULT_2PC_TTL_MS,
            max_lease_lifetime_ms: MAX_2PC_LEASE_LIFETIME_MS,
            forward_secret: String::new(),
            pre_prepare_hold: Duration::ZERO,
            post_prepare_gate: None,
            abandon_after_prepare_gate: false,
            lease_publish_probe: None,
            renew_transient_faults: None,
        }
    }

    /// Records the minimum durable participant `expire_hlc` after Prepare and
    /// after a renew that every prepared participant applied.
    pub fn with_lease_publish_probe(mut self, probe: Arc<AtomicU64>) -> Self {
        self.lease_publish_probe = Some(probe);
        self
    }

    /// Skips this many participant renew proposals as transient failures.
    pub fn with_renew_transient_faults(mut self, faults: Arc<AtomicU32>) -> Self {
        self.renew_transient_faults = Some(faults);
        self
    }

    /// Lowers the renewal cap for tests. The value is clamped to the protocol
    /// maximum and to at least one TTL, so a test can prove work past the cap
    /// aborts without waiting out the production cap.
    pub fn with_max_lease_lifetime_ms(mut self, max_lease_lifetime_ms: u64) -> Self {
        self.max_lease_lifetime_ms = max_lease_lifetime_ms
            .max(self.ttl_ms)
            .min(MAX_2PC_LEASE_LIFETIME_MS);
        self
    }

    /// Sleeps once before the first Prepare proposal. Renewal is already running,
    /// and the Prepare command's `expire_hlc` is computed after the sleep.
    pub fn with_pre_prepare_hold(mut self, hold: Duration) -> Self {
        self.pre_prepare_hold = hold;
        self
    }

    pub fn with_post_prepare_gate(mut self, gate: Arc<TwoPcPrepareGate>) -> Self {
        self.post_prepare_gate = Some(gate);
        self
    }

    /// After the post-prepare gate is released, abort the prepared transaction
    /// instead of committing. Used to check rollback after a long lease.
    pub fn with_abandon_after_prepare_gate(mut self, abandon: bool) -> Self {
        self.abandon_after_prepare_gate = abandon;
        self
    }

    pub fn with_forward_secret(mut self, secret: impl Into<String>) -> Self {
        self.forward_secret = secret.into();
        self
    }

    pub fn with_ttl_ms(mut self, ttl_ms: u64) -> Self {
        self.ttl_ms = ttl_ms.max(10);
        if self.max_lease_lifetime_ms < self.ttl_ms {
            self.max_lease_lifetime_ms = self.ttl_ms.min(MAX_2PC_LEASE_LIFETIME_MS);
        }
        self
    }

    pub fn directory(&self) -> &Arc<dyn ShardDirectory> {
        &self.directory
    }

    pub fn clock(&self) -> &Arc<HybridLogicalClock> {
        &self.clock
    }

    fn route_user_key_once(&self, user_key: &[u8]) -> Result<RoutedShardHandle> {
        let (epoch, group, handler) = self.directory.route_user_key(user_key)?;
        if group.current_epoch() != epoch || !handler.key_range().contains_key(user_key) {
            return Err(MetaDbError::EpochMismatch {
                shard_id: group.shard_id(),
                expected: epoch,
                actual: group.current_epoch(),
            });
        }
        Ok((epoch, group, handler))
    }

    async fn route_user_key(&self, user_key: &[u8]) -> Result<RoutedShardHandle> {
        retry_routing(|| std::future::ready(self.route_user_key_once(user_key))).await
    }

    async fn route_user_range(&self, start: &[u8], end: &[u8]) -> Result<Vec<RoutedShardHandle>> {
        retry_routing(|| std::future::ready(self.directory.route_user_range(start, end))).await
    }

    fn validate_read_epoch(group: &ShardRaftGroup, epoch: ShardEpoch) -> Result<()> {
        let actual = group.current_epoch();
        if actual != epoch {
            return Err(MetaDbError::EpochMismatch {
                shard_id: group.shard_id(),
                expected: epoch,
                actual,
            });
        }
        Ok(())
    }

    async fn read_user_key(&self, user_key: &[u8]) -> Result<(Arc<ShardRaftGroup>, SnapshotGuard)> {
        let (epoch, group, handler) = self.route_user_key_once(user_key)?;
        let (_, snapshot) = group.linearizable_read(epoch).await?;
        // A range change can occur after routing and before the read cut.
        // Never turn a fenced parent's missing key into a successful miss.
        if group.current_epoch() != epoch || !handler.key_range().contains_key(user_key) {
            return Err(MetaDbError::EpochMismatch {
                shard_id: group.shard_id(),
                expected: epoch,
                actual: group.current_epoch(),
            });
        }
        Ok((group, snapshot))
    }

    async fn propose_cmd(
        group: &ShardRaftGroup,
        epoch: ShardEpoch,
        hlc: HlcTimestamp,
        cmd: &TxRaftCmd,
        deadline: tokio::time::Instant,
        proposal_id: u128,
    ) -> Result<(HlcTimestamp, TxRaftOutput)> {
        let payload = encode_proposed_command(proposal_id, cmd)?;
        let resp = group.propose_until(epoch, hlc, payload, deadline).await?;
        let bytes = resp.result?;
        let decoded: TxRaftOutput =
            codec::deserialize(&bytes).map_err(|e| MetaDbError::CodecError(e.to_string()))?;
        Ok((resp.applied_hlc, decoded))
    }

    /// Re-resolves `user_key` and proposes until the leader accepts, or the routing budget ends.
    ///
    /// `propose` itself forwards one hop when the local replica already knows the leader.
    /// This loop covers a stale hint and an epoch bumped by membership or split.
    async fn propose_participant_id(
        &self,
        user_key: &[u8],
        cmd: &TxRaftCmd,
        hlc: HlcTimestamp,
        proposal_id: u128,
    ) -> Result<(ShardEpoch, Arc<ShardRaftGroup>, HlcTimestamp, TxRaftOutput)> {
        let deadline = tokio::time::Instant::now() + ROUTE_RETRY_TIMEOUT;
        self.propose_participant_until(user_key, cmd, hlc, proposal_id, deadline)
            .await
    }

    async fn propose_participant_until(
        &self,
        user_key: &[u8],
        cmd: &TxRaftCmd,
        hlc: HlcTimestamp,
        proposal_id: u128,
        deadline: tokio::time::Instant,
    ) -> Result<(ShardEpoch, Arc<ShardRaftGroup>, HlcTimestamp, TxRaftOutput)> {
        let mut last = None;
        while tokio::time::Instant::now() < deadline {
            let (epoch, group, _) = match self.route_user_key_once(user_key) {
                Ok(routed) => routed,
                Err(
                    error @ (MetaDbError::EpochMismatch { .. } | MetaDbError::ShardRoutingError(_)),
                ) => {
                    last = Some(error);
                    tokio::time::sleep(ROUTE_RETRY_INTERVAL).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            match Self::propose_cmd(&group, epoch, hlc, cmd, deadline, proposal_id).await {
                Ok((applied_hlc, decoded)) => {
                    return Ok((epoch, group, applied_hlc, decoded));
                }
                Err(error) if error.is_retryable_write_consensus() => {
                    last = Some(error);
                    tokio::time::sleep(ROUTE_RETRY_INTERVAL).await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(last.unwrap_or_else(|| {
            MetaDbError::ShardRoutingError(
                "participant propose exhausted leader retries".to_string(),
            )
        }))
    }

    /// Re-routes a single-key command when a split moves that key.
    async fn propose_participant_retry_epoch(
        &self,
        user_key: &[u8],
        cmd: &TxRaftCmd,
        hlc: HlcTimestamp,
    ) -> Result<(ShardEpoch, Arc<ShardRaftGroup>, HlcTimestamp, TxRaftOutput)> {
        let deadline = tokio::time::Instant::now() + ROUTE_RETRY_TIMEOUT;
        let proposal_id = fresh_proposal_id();
        let mut last = None;
        while tokio::time::Instant::now() < deadline {
            match self
                .propose_participant_id(user_key, cmd, hlc, proposal_id)
                .await
            {
                Err(
                    error @ (MetaDbError::EpochMismatch { .. } | MetaDbError::ShardRoutingError(_)),
                ) => {
                    last = Some(error);
                    tokio::time::sleep(ROUTE_RETRY_INTERVAL).await;
                }
                other => return other,
            }
        }
        Err(last.unwrap_or_else(|| {
            MetaDbError::ShardRoutingError(
                "participant propose exhausted epoch retries".to_string(),
            )
        }))
    }

    async fn propose_key_with_retry(
        &self,
        user_key: &[u8],
        cmd: &TxRaftCmd,
    ) -> Result<TxRaftOutput> {
        let deadline = tokio::time::Instant::now() + ROUTE_RETRY_TIMEOUT;
        let proposal_id = fresh_proposal_id();
        for attempt in 0..12usize {
            let result = retry_routing_until(deadline, || async {
                let (epoch, group, _) = self.route_user_key_once(user_key)?;
                Self::propose_cmd(&group, epoch, self.clock.now(), cmd, deadline, proposal_id).await
            })
            .await;
            match result {
                Ok((_, out)) => return Ok(out),
                Err(error) if error.is_retryable_write_consensus() && attempt < 11 => {
                    tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt as u64 + 1)))
                        .await;
                }
                Err(MetaDbError::WriteIntentConflict {
                    conflicting_txn_id,
                    primary_key,
                    expire_hlc,
                }) if attempt < 11 => {
                    if self.clock.now().0 > expire_hlc {
                        let _ = TxnRecoveryWorker::resolve_single_txn(
                            self.directory.as_ref(),
                            &self.clock,
                            conflicting_txn_id,
                            &primary_key,
                        )
                        .await;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(5 * (attempt as u64 + 1)))
                        .await;
                }
                Err(e) => return Err(e),
            }
        }
        Err(MetaDbError::ShardRoutingError(
            "exceeded max route/conflict retries".to_string(),
        ))
    }

    /// Initializes `(dir_id, "_attr_")` (e.g., for `ROOT_INODE_ID = 1` at cluster bootstrap).
    pub async fn init_dir_attr(
        &self,
        dir_id: InodeId,
        parent_pid: InodeId,
        uid: UserId,
        mode: u32,
    ) -> Result<InodeAttr> {
        let key = MetaKey::dir_attr(dir_id).encode();
        let cmd = TxRaftCmd::InitDirAttr {
            dir_id,
            parent_pid,
            uid,
            mode,
        };
        match self.propose_key_with_retry(&key, &cmd).await? {
            TxRaftOutput::Attr(a) => Ok(a),
            _ => Err(MetaDbError::CodecError(
                "unexpected InitDirAttr output".to_string(),
            )),
        }
    }

    /// `1-RTT` single-shard file creation (`100%` single-shard even in `DualModeFineGrained`!).
    #[allow(clippy::too_many_arguments)]
    pub async fn create_file(
        &self,
        pid: InodeId,
        name: &str,
        inode_id: InodeId,
        fid: FileId,
        uid: UserId,
        mode: u32,
        size_bytes: i64,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(name)?;
        validate_file_size(size_bytes)?;
        let key = MetaKey::child(pid, name).encode();
        let cmd = TxRaftCmd::CreateFile {
            pid,
            name: name.to_string(),
            inode_id,
            fid,
            uid,
            mode,
            size_bytes,
        };
        match self.propose_key_with_retry(&key, &cmd).await? {
            TxRaftOutput::Entry(e) => Ok(e),
            _ => Err(MetaDbError::CodecError(
                "unexpected CreateFile output".to_string(),
            )),
        }
    }

    /// `1-RTT` single-shard file unlink.
    pub async fn unlink(&self, pid: InodeId, name: &str) -> Result<InodeAttr> {
        validate_user_entry_name(name)?;
        let key = MetaKey::child(pid, name).encode();
        let cmd = TxRaftCmd::Unlink {
            pid,
            name: name.to_string(),
        };
        match self.propose_key_with_retry(&key, &cmd).await? {
            TxRaftOutput::Attr(a) => Ok(a),
            _ => Err(MetaDbError::CodecError(
                "unexpected Unlink output".to_string(),
            )),
        }
    }

    /// Creates a subdirectory `name` (`new_dir_id`):
    /// - Executes `1-RTT` `MkdirSingleShard` when `Shard(pid, name) == Shard(new_dir_id, "_attr_")`.
    /// - Automatically executes crash-recoverable Primary-Anchored `2PC` across shards otherwise.
    pub async fn mkdir(
        &self,
        pid: InodeId,
        name: &str,
        new_dir_id: InodeId,
        uid: UserId,
        mode: u32,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(name)?;
        validate_dir_inode_id(new_dir_id)?;
        match self.dirstat(new_dir_id).await {
            Ok(_) => {
                return Err(MetaDbError::AlreadyExists {
                    pid: new_dir_id,
                    name: ATTR_ENTRY_NAME.to_string(),
                });
            }
            Err(MetaDbError::DirectoryNotFound(_)) => {}
            Err(e) => return Err(e),
        }
        let child_meta_key = MetaKey::child(pid, name);
        let child_bytes = child_meta_key.encode();
        let new_attr_bytes = MetaKey::dir_attr(new_dir_id).encode();

        let (_, g_parent, _) = self.route_user_key(&child_bytes).await?;
        let (_, g_new_dir, _) = self.route_user_key(&new_attr_bytes).await?;

        if g_parent.shard_id() == g_new_dir.shard_id() {
            let cmd = TxRaftCmd::MkdirSingleShard {
                pid,
                name: name.to_string(),
                new_dir_id,
                uid,
                mode,
            };
            return match self.propose_key_with_retry(&child_bytes, &cmd).await? {
                TxRaftOutput::Entry(e) => Ok(e),
                _ => Err(MetaDbError::CodecError(
                    "unexpected Mkdir output".to_string(),
                )),
            };
        }

        // Cross-shard 2PC `Mkdir`:
        if self.lookup(pid, name).await?.is_some() {
            return Err(MetaDbError::AlreadyExists {
                pid,
                name: name.to_string(),
            });
        }

        let start_hlc = self.clock.now();
        let dentry_attr = InodeAttr {
            inode_id: new_dir_id,
            parent_pid: pid,
            entry_type: EntryType::Directory,
            uid,
            mode,
            link_count: 1,
            num_children: 0,
            size_bytes: 0,
            create_time: start_hlc,
            modify_time: start_hlc,
            fid: None,
            applied_lsn: 0,
        };
        let self_attr = InodeAttr {
            entry_type: EntryType::DirectoryAttr,
            ..dentry_attr.clone()
        };

        let parent_attr_fence = MetaKey::dir_attr(pid).encode();
        let intents_by_key: Vec<(Vec<u8>, IntentOp)> = vec![
            (
                parent_attr_fence,
                IntentOp::DirExistsFenceSet {
                    holders: BTreeMap::new(),
                },
            ),
            (
                child_bytes.clone(),
                IntentOp::Insert {
                    value: dentry_attr.encode(),
                    attr_deltas: vec![AttrDeltaSpec {
                        pid,
                        delta_children: 1,
                        delta_bytes: 0,
                    }],
                    aux_specs: vec![AuxIntentSpec::QuotaDelta {
                        uid,
                        delta_bytes: 0,
                        delta_inodes: 1,
                    }],
                },
            ),
            (
                new_attr_bytes.clone(),
                IntentOp::Insert {
                    value: self_attr.encode(),
                    attr_deltas: vec![],
                    aux_specs: vec![],
                },
            ),
        ];

        let commit_hlc = self
            .execute_two_pc(
                &child_bytes,
                intents_by_key,
                vec![(child_bytes.clone(), None), (new_attr_bytes, None)],
                None,
                TwoPcCrashStage::None,
            )
            .await?;

        let mut committed_attr = dentry_attr;
        committed_attr.modify_time = commit_hlc;
        Ok(MetaEntry {
            key: child_meta_key,
            attr: committed_attr,
        })
    }

    /// Removes an empty directory `(pid, name)` with authoritative emptiness verification:
    /// - `1-RTT` `RmdirSingleShard` when colocated on a single shard.
    /// - Crash-recoverable `2PC` with `DeletingDir` intents across all sub-shards otherwise.
    pub async fn rmdir(&self, pid: InodeId, name: &str) -> Result<InodeAttr> {
        validate_user_entry_name(name)?;
        let Some(entry) = self.lookup(pid, name).await? else {
            return Err(MetaDbError::NotFound {
                pid,
                name: name.to_string(),
            });
        };
        if entry.attr.entry_type != EntryType::Directory {
            return Err(MetaDbError::InvalidEntryType {
                expected: "Directory".to_string(),
                actual: "File".to_string(),
            });
        }
        let target_dir_id = entry.attr.inode_id;
        let child_bytes = MetaKey::child(pid, name).encode();
        let dir_start = MetaKey::encode_pid_prefix_start(target_dir_id);
        let dir_end = MetaKey::encode_pid_prefix_end(target_dir_id);

        let (_, g_parent, _) = self.route_user_key(&child_bytes).await?;
        let dir_shards = self.route_user_range(&dir_start, &dir_end).await?;

        if dir_shards.len() == 1 && dir_shards[0].1.shard_id() == g_parent.shard_id() {
            let cmd = TxRaftCmd::RmdirSingleShard {
                pid,
                name: name.to_string(),
            };
            return match self.propose_key_with_retry(&child_bytes, &cmd).await? {
                TxRaftOutput::Attr(a) => Ok(a),
                _ => Err(MetaDbError::CodecError(
                    "unexpected Rmdir output".to_string(),
                )),
            };
        }

        // Cross-shard or `DualModeFineGrained` 2PC `Rmdir` (`DeletingDir` locks every sub-shard!).
        let mut intents_by_key: Vec<(Vec<u8>, IntentOp)> = Vec::new();
        intents_by_key.push((
            child_bytes.clone(),
            IntentOp::Delete {
                attr_deltas: vec![AttrDeltaSpec {
                    pid,
                    delta_children: -1,
                    delta_bytes: 0,
                }],
                aux_specs: vec![AuxIntentSpec::QuotaDelta {
                    uid: entry.attr.uid,
                    delta_bytes: 0,
                    delta_inodes: -1,
                }],
            },
        ));

        let anchor_attr_key = MetaKey::dir_attr(target_dir_id).encode();
        for (_, _, handler) in &dir_shards {
            let r = handler.key_range();
            let lock_key = if r.contains_key(&anchor_attr_key) {
                anchor_attr_key.clone()
            } else {
                r.start_key.clone()
            };
            intents_by_key.push((
                lock_key,
                IntentOp::DeletingDir {
                    dir_id: target_dir_id,
                    attr_deltas: vec![],
                    aux_specs: vec![],
                },
            ));
        }

        self.execute_two_pc(
            &child_bytes,
            intents_by_key,
            vec![(child_bytes.clone(), Some(entry.attr.encode()))],
            None,
            TwoPcCrashStage::None,
        )
        .await?;
        Ok(entry.attr)
    }

    /// Updates file or directory attributes (`Resolution B10`: directory `SetAttr` updates
    /// authoritative `(dir_id, "_attr_")` and any `[0x04][dir_id]` sub-shards via 2PC if cross-shard).
    pub async fn setattr(
        &self,
        pid: InodeId,
        name: &str,
        patch: SetAttrPatch,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(name)?;
        if let Some(sz) = patch.size_bytes {
            validate_file_size(sz)?;
        }
        let Some(existing) = self.lookup(pid, name).await? else {
            return Err(MetaDbError::NotFound {
                pid,
                name: name.to_string(),
            });
        };

        let child_bytes = existing.key.encode();
        if existing.attr.entry_type == EntryType::File {
            let cmd = TxRaftCmd::SetAttrSingleShard {
                pid,
                name: name.to_string(),
                patch,
            };
            return match self.propose_key_with_retry(&child_bytes, &cmd).await? {
                TxRaftOutput::Entry(e) => Ok(e),
                _ => Err(MetaDbError::CodecError(
                    "unexpected SetAttr output".to_string(),
                )),
            };
        }

        if patch.size_bytes.is_some() || patch.fid.is_some() {
            return Err(MetaDbError::CodecError(
                "directory size and fid are derived attributes".into(),
            ));
        }

        // Directory `SetAttr`: check if single-shard colocated or multi-shard (`Resolution B10`).
        let dir_id = existing.attr.inode_id;
        let dir_start = MetaKey::encode_pid_prefix_start(dir_id);
        let dir_end = MetaKey::encode_pid_prefix_end(dir_id);
        let (_, g_parent, _) = self.route_user_key(&child_bytes).await?;
        let dir_shards = self.route_user_range(&dir_start, &dir_end).await?;

        if dir_shards.len() == 1 && dir_shards[0].1.shard_id() == g_parent.shard_id() {
            let cmd = TxRaftCmd::SetAttrSingleShard {
                pid,
                name: name.to_string(),
                patch,
            };
            return match self.propose_key_with_retry(&child_bytes, &cmd).await? {
                TxRaftOutput::Entry(e) => Ok(e),
                _ => Err(MetaDbError::CodecError(
                    "unexpected SetAttr output".to_string(),
                )),
            };
        }

        let mut new_dentry = existing.attr.clone();
        let old_uid = new_dentry.uid;
        if let Some(m) = patch.mode {
            new_dentry.mode = m;
        }
        if let Some(u) = patch.uid {
            new_dentry.uid = u;
        }
        let mut aux_specs = Vec::new();
        if new_dentry.uid != old_uid {
            aux_specs.push(AuxIntentSpec::QuotaDelta {
                uid: old_uid,
                delta_bytes: 0,
                delta_inodes: -1,
            });
            aux_specs.push(AuxIntentSpec::QuotaDelta {
                uid: new_dentry.uid,
                delta_bytes: 0,
                delta_inodes: 1,
            });
        }

        let dir_attr_key = MetaKey::dir_attr(dir_id).encode();
        let dir_self_raw = self
            .read_raw_user_value(&dir_attr_key)
            .await?
            .ok_or(MetaDbError::DirectoryNotFound(dir_id))?;
        let mut dir_self = InodeAttr::decode(&dir_self_raw)?;
        dir_self.mode = new_dentry.mode;
        dir_self.uid = new_dentry.uid;

        let intents = vec![
            (
                child_bytes.clone(),
                IntentOp::Put {
                    value: new_dentry.encode(),
                    attr_deltas: vec![],
                    aux_specs,
                },
            ),
            (
                dir_attr_key.clone(),
                IntentOp::Put {
                    value: dir_self.encode(),
                    attr_deltas: vec![],
                    aux_specs: vec![],
                },
            ),
        ];
        let commit_hlc = self
            .execute_two_pc(
                &child_bytes,
                intents,
                vec![
                    (child_bytes.clone(), Some(existing.attr.encode())),
                    (dir_attr_key, Some(dir_self_raw)),
                ],
                None,
                TwoPcCrashStage::None,
            )
            .await?;
        new_dentry.modify_time = commit_hlc;
        Ok(MetaEntry {
            key: existing.key,
            attr: new_dentry,
        })
    }

    async fn ensure_directory_rename_acyclic(
        &self,
        src_dir_id: InodeId,
        dst_pid: InodeId,
    ) -> Result<()> {
        let mut walker = RenameCycleWalker::new(src_dir_id, dst_pid);
        let mut cursor = dst_pid;
        loop {
            if !walker.observe(cursor)? {
                return Ok(());
            }
            cursor = self.dirstat(cursor).await?.parent_pid;
        }
    }

    /// Atomic `Rename` (`1-RTT` within one shard, or crash-recoverable Primary-Anchored `2PC` across shards).
    pub async fn rename(
        &self,
        src_pid: InodeId,
        src_name: &str,
        dst_pid: InodeId,
        dst_name: &str,
        overwrite: bool,
    ) -> Result<MetaEntry> {
        self.rename_with_fault_injection(
            src_pid,
            src_name,
            dst_pid,
            dst_name,
            overwrite,
            TwoPcCrashStage::None,
        )
        .await
    }

    pub async fn rename_with_fault_injection(
        &self,
        src_pid: InodeId,
        src_name: &str,
        dst_pid: InodeId,
        dst_name: &str,
        overwrite: bool,
        crash_stage: TwoPcCrashStage,
    ) -> Result<MetaEntry> {
        validate_user_entry_name(src_name)?;
        validate_user_entry_name(dst_name)?;

        let src_meta_key = MetaKey::child(src_pid, src_name);
        let dst_meta_key = MetaKey::child(dst_pid, dst_name);
        let src_bytes = src_meta_key.encode();
        let dst_bytes = dst_meta_key.encode();

        let Some(src_entry) = self.lookup(src_pid, src_name).await? else {
            return Err(MetaDbError::NotFound {
                pid: src_pid,
                name: src_name.to_string(),
            });
        };

        let (_, g_src, _) = self.route_user_key(&src_bytes).await?;
        let (_, g_dst, _) = self.route_user_key(&dst_bytes).await?;

        let single_shard_eligible = if src_entry.attr.entry_type == EntryType::Directory {
            let d_start = MetaKey::encode_pid_prefix_start(src_entry.attr.inode_id);
            let d_end = MetaKey::encode_pid_prefix_end(src_entry.attr.inode_id);
            let d_shards = self.route_user_range(&d_start, &d_end).await?;
            g_src.shard_id() == g_dst.shard_id()
                && d_shards.len() == 1
                && d_shards[0].1.shard_id() == g_src.shard_id()
                && crash_stage == TwoPcCrashStage::None
        } else {
            g_src.shard_id() == g_dst.shard_id() && crash_stage == TwoPcCrashStage::None
        };

        if single_shard_eligible {
            let cmd = TxRaftCmd::RenameSingleShard {
                src_pid,
                src_name: src_name.to_string(),
                dst_pid,
                dst_name: dst_name.to_string(),
                overwrite,
            };
            return match self.propose_key_with_retry(&src_bytes, &cmd).await? {
                TxRaftOutput::Entry(e) => Ok(e),
                _ => Err(MetaDbError::CodecError(
                    "unexpected Rename output".to_string(),
                )),
            };
        }

        crate::validation::validate_size(src_entry.attr.size_bytes)?;
        // Cross-shard 2PC `Rename`:
        let lease_opt = if src_entry.attr.entry_type == EntryType::Directory && src_pid != dst_pid {
            let lease = self
                .directory
                .acquire_dir_rename_lease(src_entry.attr.inode_id, self.ttl_ms)
                .await?;
            // Walk the full ancestor chain. A truncated walk must not allow the rename.
            if let Err(e) = self
                .ensure_directory_rename_acyclic(src_entry.attr.inode_id, dst_pid)
                .await
            {
                self.directory
                    .release_dir_rename_lease(lease.rename_fence_token)
                    .await;
                return Err(e);
            }
            Some(lease)
        } else {
            None
        };

        // Check destination existence & victim accounting (`Resolution B11`).
        let dst_existing = self.lookup(dst_pid, dst_name).await?;
        let mut expected_values = vec![
            (src_bytes.clone(), Some(src_entry.attr.encode())),
            (
                dst_bytes.clone(),
                dst_existing.as_ref().map(|entry| entry.attr.encode()),
            ),
        ];
        let mut dst_aux_specs = Vec::new();
        let src_accounted = src_entry.attr.size_bytes.max(0);
        let mut dst_delta_children = 1i64;
        let mut dst_delta_bytes = src_accounted;

        if let Some(victim_entry) = dst_existing {
            crate::validation::validate_size(victim_entry.attr.size_bytes)?;
            if !overwrite {
                if let Some(l) = lease_opt {
                    self.directory
                        .release_dir_rename_lease(l.rename_fence_token)
                        .await;
                }
                return Err(MetaDbError::AlreadyExists {
                    pid: dst_pid,
                    name: dst_name.to_string(),
                });
            }
            if victim_entry.attr.entry_type != EntryType::File
                || src_entry.attr.entry_type != EntryType::File
            {
                if let Some(l) = lease_opt {
                    self.directory
                        .release_dir_rename_lease(l.rename_fence_token)
                        .await;
                }
                return Err(MetaDbError::InvalidEntryType {
                    expected: "File".to_string(),
                    actual: "Directory".to_string(),
                });
            }
            dst_delta_children = 0; // +1 - 1 = 0 net child delta on destination!
            dst_delta_bytes = saturating_add_i64(
                src_accounted,
                negate_accounted_size(victim_entry.attr.size_bytes),
            );
            dst_aux_specs.push(AuxIntentSpec::QuotaDelta {
                uid: victim_entry.attr.uid,
                delta_bytes: negate_accounted_size(victim_entry.attr.size_bytes),
                delta_inodes: -1,
            });
            if let Some(vfid) = victim_entry.attr.fid {
                dst_aux_specs.push(AuxIntentSpec::IndexDelete { fid: vfid });
            }
        }

        if let Some(fid) = src_entry.attr.fid {
            dst_aux_specs.push(AuxIntentSpec::IndexUpsert {
                fid,
                pid: dst_pid,
                name: dst_name.to_string(),
                create_time: src_entry.attr.create_time,
            });
        }

        let mut moved_attr = src_entry.attr.clone();
        moved_attr.parent_pid = dst_pid;

        let mut intents: Vec<(Vec<u8>, IntentOp)> = Vec::new();
        let dst_dir_attr_key = MetaKey::dir_attr(dst_pid).encode();
        intents.push((
            dst_dir_attr_key,
            IntentOp::DirExistsFenceSet {
                holders: BTreeMap::new(),
            },
        ));
        intents.push((
            src_bytes.clone(),
            IntentOp::Delete {
                attr_deltas: if src_pid != dst_pid {
                    vec![AttrDeltaSpec {
                        pid: src_pid,
                        delta_children: -1,
                        delta_bytes: negate_accounted_size(src_accounted),
                    }]
                } else {
                    vec![]
                },
                aux_specs: vec![],
            },
        ));
        intents.push((
            dst_bytes,
            IntentOp::Put {
                value: moved_attr.encode(),
                attr_deltas: if src_pid != dst_pid {
                    vec![AttrDeltaSpec {
                        pid: dst_pid,
                        delta_children: dst_delta_children,
                        delta_bytes: dst_delta_bytes,
                    }]
                } else {
                    vec![AttrDeltaSpec {
                        pid: dst_pid,
                        delta_children: dst_delta_children - 1,
                        delta_bytes: saturating_add_i64(
                            dst_delta_bytes,
                            negate_accounted_size(src_accounted),
                        ),
                    }]
                },
                aux_specs: dst_aux_specs,
            },
        ));

        if moved_attr.entry_type == EntryType::Directory && src_pid != dst_pid {
            let dir_self_key = MetaKey::dir_attr(moved_attr.inode_id).encode();
            let dir_self_raw = self
                .read_raw_user_value(&dir_self_key)
                .await?
                .ok_or(MetaDbError::DirectoryNotFound(moved_attr.inode_id))?;
            let mut dir_self = InodeAttr::decode(&dir_self_raw)?;
            dir_self.parent_pid = dst_pid;
            expected_values.push((dir_self_key.clone(), Some(dir_self_raw)));
            intents.push((
                dir_self_key,
                IntentOp::Put {
                    value: dir_self.encode(),
                    attr_deltas: vec![],
                    aux_specs: vec![],
                },
            ));
        }

        let res = self
            .execute_two_pc(&src_bytes, intents, expected_values, lease_opt, crash_stage)
            .await;

        if let Some(l) = lease_opt {
            self.directory
                .release_dir_rename_lease(l.rename_fence_token)
                .await;
        }

        let commit_hlc = res?;
        moved_attr.modify_time = commit_hlc;
        Ok(MetaEntry {
            key: dst_meta_key,
            attr: moved_attr,
        })
    }

    fn spawn_lease_task(
        &self,
        keeper: Arc<LeaseKeeper>,
        txn_id: TxnId,
        primary_key: Vec<u8>,
        start_hlc: HlcTimestamp,
        rename_token: Option<u64>,
    ) -> LeaseTask {
        let keeper_task = keeper.clone();
        let directory = self.directory.clone();
        let clock = self.clock.clone();
        let ttl_ms = self.ttl_ms;
        let coord_node_id = self.coord_node_id;
        let handle = tokio::spawn(async move {
            let interval = Duration::from_millis((ttl_ms / 3).max(1));
            loop {
                tokio::time::sleep(interval).await;
                if keeper_task.cancel.load(Ordering::Acquire) {
                    return;
                }
                let renewed = renew_open_lease(
                    directory.as_ref(),
                    clock.as_ref(),
                    keeper_task.as_ref(),
                    txn_id,
                    coord_node_id,
                    &primary_key,
                    start_hlc,
                    rename_token,
                    ttl_ms,
                )
                .await;
                if !renewed {
                    keeper_task.failed.store(true, Ordering::Release);
                    return;
                }
            }
        });
        LeaseTask { keeper, handle }
    }

    async fn execute_two_pc(
        &self,
        primary_key: &[u8],
        intents: Vec<(Vec<u8>, IntentOp)>,
        expected_values: Vec<(Vec<u8>, Option<Vec<u8>>)>,
        lease_opt: Option<DirRenameLease>,
        crash_stage: TwoPcCrashStage,
    ) -> Result<HlcTimestamp> {
        let txn_id = self.directory.allocate_txn_id(self.coord_node_id)?;
        let all_participant_keys: Vec<Vec<u8>> = intents.iter().map(|(k, _)| k.clone()).collect();
        let route_deadline = tokio::time::Instant::now() + ROUTE_RETRY_TIMEOUT;
        loop {
            if tokio::time::Instant::now() >= route_deadline {
                return Err(MetaDbError::ShardRoutingError(
                    "intents could not be routed before prepare".to_string(),
                ));
            }
            let mut ready = self.route_user_key(primary_key).await.is_ok();
            if ready {
                for (key, _) in &intents {
                    if self.route_user_key(key).await.is_err() {
                        ready = false;
                        break;
                    }
                }
            }
            if ready {
                break;
            }
            tokio::time::sleep(ROUTE_RETRY_INTERVAL).await;
        }

        // Waiting for split publication must not consume a transaction's lease
        // before its first prepare. A directory-rename lease is not renewed during
        // that wait; if it already expired, fail before writing intents.
        let start_hlc = self.clock.now();
        let rename_fence_token = lease_opt.map(|lease| lease.rename_fence_token);
        let rename_cap_ms = lease_opt
            .map(|lease| lease.expire_hlc.physical_ms())
            .unwrap_or(u64::MAX);
        if lease_opt.is_some_and(|lease| lease.expire_hlc <= start_hlc) {
            return Err(MetaDbError::TransactionAborted(
                "directory rename lease expired while waiting for shard routing".into(),
            ));
        }
        let deadline_ms = start_hlc
            .physical_ms()
            .saturating_add(self.max_lease_lifetime_ms);
        let initial_expire_ms = if lease_opt.is_some() {
            rename_cap_ms.min(deadline_ms)
        } else {
            start_hlc
                .physical_ms()
                .saturating_add(self.ttl_ms)
                .min(deadline_ms)
        };
        let keeper = Arc::new(LeaseKeeper::new(
            initial_expire_ms,
            deadline_ms,
            rename_cap_ms.min(deadline_ms),
            self.lease_publish_probe.clone(),
            self.renew_transient_faults.clone(),
        ));
        // Renewal starts before the first Prepare so a NotLeader retry that
        // outlasts the initial TTL can refresh expire_hlc and extend any shard
        // that has already prepared.
        let _lease_task = self.spawn_lease_task(
            keeper.clone(),
            txn_id,
            primary_key.to_vec(),
            start_hlc,
            rename_fence_token,
        );

        // Phase 1: regroup after a split. A batch that straddles two children must not
        // be retried unchanged against only the child that owns the routing key.
        let prepare_deadline = tokio::time::Instant::now() + ROUTE_RETRY_TIMEOUT;
        let mut prepared_shards: Vec<PreparedShardBatch> = Vec::new();
        let mut max_prepared_hlc = start_hlc;
        let mut held_before_prepare = false;
        let primary_shard_id = loop {
            if keeper.failed() {
                self.abort_prepared_shards(txn_id, &prepared_shards).await;
                return Err(MetaDbError::TransactionAborted(
                    "2PC lease expired while preparing".into(),
                ));
            }
            if tokio::time::Instant::now() >= prepare_deadline {
                self.abort_prepared_shards(txn_id, &prepared_shards).await;
                return Err(MetaDbError::ShardRoutingError(
                    "prepare exhausted retries while shard routes were changing".to_string(),
                ));
            }
            if !held_before_prepare && !self.pre_prepare_hold.is_zero() {
                held_before_prepare = true;
                tokio::time::sleep(self.pre_prepare_hold).await;
            }
            let now = self.clock.now();
            let expire_hlc = keeper.expire_for(now, self.ttl_ms);
            if expire_hlc <= now {
                self.abort_prepared_shards(txn_id, &prepared_shards).await;
                return Err(MetaDbError::TransactionAborted(
                    "2PC lease expired while preparing".into(),
                ));
            }
            let (_primary_epoch, primary_group, _) = match self.route_user_key(primary_key).await {
                Ok(routed) => routed,
                Err(MetaDbError::EpochMismatch { .. } | MetaDbError::ShardRoutingError(_)) => {
                    tokio::time::sleep(ROUTE_RETRY_INTERVAL).await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let shard_id = primary_group.shard_id();
            let mut grouped: HashMap<ShardId, ShardIntentGroup> = HashMap::new();
            let mut deferred = false;
            for (user_key, op) in &intents {
                match self.route_user_key(user_key).await {
                    Ok((epoch, group, _)) => {
                        grouped
                            .entry(group.shard_id())
                            .or_insert_with(|| (epoch, group, Vec::new()))
                            .2
                            .push((user_key.clone(), op.clone()));
                    }
                    Err(MetaDbError::EpochMismatch { .. } | MetaDbError::ShardRoutingError(_)) => {
                        deferred = true;
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            if deferred {
                tokio::time::sleep(ROUTE_RETRY_INTERVAL).await;
                continue;
            }
            let mut ordered_groups: Vec<(ShardId, ShardIntentGroup)> =
                grouped.into_iter().collect();
            ordered_groups.sort_by_key(|(sid, _)| *sid == shard_id);
            let mut prepared_this_round: Vec<PreparedShardBatch> = Vec::new();
            let mut regroup = false;
            for (sid, (_epoch, _group, shard_intents)) in ordered_groups {
                let shard_keys: Vec<Vec<u8>> =
                    shard_intents.iter().map(|(key, _)| key.clone()).collect();
                let still_together = shard_keys.iter().all(|key| {
                    self.directory
                        .route_user_key(key)
                        .map(|(_, group, _)| group.shard_id() == sid)
                        .unwrap_or(false)
                });
                if !still_together {
                    regroup = true;
                    break;
                }
                let shard_expected_values = expected_values
                    .iter()
                    .filter(|(key, _)| shard_keys.contains(key))
                    .cloned()
                    .collect();
                let route_key = shard_keys
                    .first()
                    .cloned()
                    .unwrap_or_else(|| primary_key.to_vec());
                // Recompute immediately before this proposal. A NotLeader retry
                // longer than the remaining TTL must not reuse a stale expire.
                let attempt_now = self.clock.now();
                let attempt_expire = keeper.expire_for(attempt_now, self.ttl_ms);
                if attempt_expire <= attempt_now {
                    self.abort_prepared_shards(txn_id, &prepared_shards).await;
                    self.abort_prepared_shards(txn_id, &prepared_this_round)
                        .await;
                    return Err(MetaDbError::TransactionAborted(
                        "2PC lease expired while preparing".into(),
                    ));
                }
                let prep_cmd = TxRaftCmd::Prepare2pcChecked {
                    txn_id,
                    is_primary: sid == shard_id,
                    primary_key: primary_key.to_vec(),
                    participant_keys: all_participant_keys.clone(),
                    start_hlc,
                    expire_hlc: attempt_expire,
                    rename_fence_token,
                    intents: shard_intents,
                    expected_values: shard_expected_values,
                };
                let attempt_budget = Duration::from_millis((self.ttl_ms / 2).max(1));
                let attempt_deadline = tokio::time::Instant::now() + attempt_budget;
                match self
                    .propose_participant_until(
                        &route_key,
                        &prep_cmd,
                        attempt_now,
                        fresh_proposal_id(),
                        attempt_deadline,
                    )
                    .await
                {
                    Ok((epoch, group, _, TxRaftOutput::Prepared { prepared_hlc })) => {
                        max_prepared_hlc = max_prepared_hlc.max(prepared_hlc);
                        let batch = (epoch, group, shard_keys);
                        keeper.observe(batch.clone());
                        keeper.note_prepared_expire(attempt_expire.physical_ms());
                        prepared_this_round.push(batch);
                    }
                    Ok(_) => {
                        prepared_shards.extend(prepared_this_round);
                        self.abort_prepared_shards(txn_id, &prepared_shards).await;
                        return Err(MetaDbError::CodecError(
                            "unexpected Prepare2pc output".to_string(),
                        ));
                    }
                    Err(
                        MetaDbError::EpochMismatch { .. }
                        | MetaDbError::ShardRoutingError(_)
                        | MetaDbError::RetryableConsensus { .. },
                    ) => {
                        regroup = true;
                        break;
                    }
                    Err(error) => {
                        prepared_shards.extend(prepared_this_round);
                        self.abort_prepared_shards(txn_id, &prepared_shards).await;
                        return Err(error);
                    }
                }
            }
            if regroup {
                keeper.clear();
                self.abort_prepared_shards(txn_id, &prepared_this_round)
                    .await;
                tokio::time::sleep(ROUTE_RETRY_INTERVAL).await;
                continue;
            }
            prepared_shards = prepared_this_round;
            break shard_id;
        };

        if keeper.failed() {
            self.abort_prepared_shards(txn_id, &prepared_shards).await;
            return Err(MetaDbError::TransactionAborted(
                "2PC lease expired while preparing".into(),
            ));
        }

        if crash_stage == TwoPcCrashStage::StopAfterPrepareBeforePrimaryCommit {
            return Err(MetaDbError::RpcError(
                "injected crash after Prepare before CommitPrimary2pc".to_string(),
            ));
        }

        if let Some(gate) = &self.post_prepare_gate {
            gate.prepared.notify_one();
            gate.release.notified().await;
            if self.abandon_after_prepare_gate {
                self.abort_prepared_shards(txn_id, &prepared_shards).await;
                return Err(MetaDbError::TransactionAborted(
                    "2PC abandoned after prepare".into(),
                ));
            }
        }
        if keeper.failed() || self.clock.now() > keeper.published() {
            self.abort_prepared_shards(txn_id, &prepared_shards).await;
            return Err(MetaDbError::TransactionAborted(
                "2PC lease expired before commit".into(),
            ));
        }

        // Phase 2a: Atomic Commit on Primary Shard (`[0x06] TxnRecord -> Committed`).
        let commit_hlc = self.clock.update(max_prepared_hlc);
        let commit_primary_cmd = TxRaftCmd::CommitPrimary2pc {
            txn_id,
            commit_hlc,
            rename_fence_token,
        };
        let (_, _, _, primary_out) = self
            .propose_participant_retry_epoch(primary_key, &commit_primary_cmd, commit_hlc)
            .await?;
        let final_commit_hlc = match primary_out {
            TxRaftOutput::Committed { commit_hlc: c, .. } => c,
            _ => {
                return Err(MetaDbError::CodecError(
                    "unexpected CommitPrimary2pc output".to_string(),
                ))
            }
        };

        if crash_stage == TwoPcCrashStage::StopAfterPrimaryCommitBeforeSecondary {
            return Err(MetaDbError::RpcError(
                "injected crash after CommitPrimary2pc before CommitSecondary2pc".to_string(),
            ));
        }

        // Phase 2b: Roll forward all Secondary Shards and mark durable ACKs on Primary (`Resolution B12`).
        let mut acked_secondary_keys = Vec::new();
        for (_ep, grp, keys) in &prepared_shards {
            if grp.shard_id() == primary_shard_id {
                continue;
            }
            let sec_cmd = TxRaftCmd::CommitSecondary2pc {
                txn_id,
                commit_hlc: final_commit_hlc,
                target_user_keys: keys.clone(),
            };
            let route_key = keys
                .first()
                .cloned()
                .unwrap_or_else(|| primary_key.to_vec());
            if self
                .propose_participant_retry_epoch(&route_key, &sec_cmd, final_commit_hlc)
                .await
                .is_ok()
            {
                acked_secondary_keys.extend(keys.clone());
            }
        }

        if !acked_secondary_keys.is_empty() {
            let ack_cmd = TxRaftCmd::MarkParticipantAcked {
                txn_id,
                acked_user_keys: acked_secondary_keys,
            };
            let _ = self
                .propose_participant_retry_epoch(primary_key, &ack_cmd, final_commit_hlc)
                .await;
        }

        Ok(final_commit_hlc)
    }

    /// Renews a pending transaction after this coordinator restarts.
    ///
    /// The deadline is the durable `expire_hlc` on the primary `TxnRecord`. A
    /// restart can push that deadline forward only while the record is still
    /// `Pending` and the caller is the coordinator that allocated `txn_id`.
    pub async fn resume_pending_lease(
        &self,
        primary_key: &[u8],
        txn_id: TxnId,
    ) -> Result<HlcTimestamp> {
        if (txn_id >> 64) as u64 != self.coord_node_id {
            return Err(MetaDbError::TransactionAborted(
                "2PC lease renewal is not held by this coordinator".into(),
            ));
        }
        let (epoch, group, _) = self.route_user_key(primary_key).await?;
        let (_, snap) = group.linearizable_read(epoch).await?;
        let raw = group
            .engine()
            .get_with_snapshot(&TxnRecordKey { txn_id }.encode(), &snap)?
            .ok_or_else(|| {
                MetaDbError::TransactionAborted(
                    "2PC lease renewal found no live transaction".into(),
                )
            })?;
        drop(snap);
        let rec = TxnStatusRecord::decode(&raw)?;
        match rec.state {
            TxnState::Aborted => {
                return Err(MetaDbError::TransactionAborted(
                    "2PC lease renewal cannot revive an aborted transaction".into(),
                ));
            }
            TxnState::Committed { .. } => {
                return Err(MetaDbError::TransactionAborted(
                    "2PC lease renewal found no live transaction".into(),
                ));
            }
            TxnState::Pending => {}
        }
        let now = self.clock.now();
        if now > rec.expire_hlc {
            return Err(MetaDbError::TransactionAborted(
                "2PC lease renewal rejected after expiry".into(),
            ));
        }
        let cap = rec
            .start_hlc
            .physical_ms()
            .saturating_add(self.max_lease_lifetime_ms.min(MAX_2PC_LEASE_LIFETIME_MS));
        if now.physical_ms() >= cap {
            return Err(MetaDbError::TransactionAborted(
                "2PC lease renewal exceeds lifetime cap".into(),
            ));
        }
        let mut proposed = now.physical_ms().saturating_add(self.ttl_ms).min(cap);
        if let Some(token) = rec.rename_fence_token {
            match self
                .directory
                .extend_dir_rename_lease(token, self.ttl_ms)
                .await
            {
                Ok(Some(expire)) => proposed = proposed.min(expire.physical_ms()),
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }
        let expire_hlc = HlcTimestamp::new(proposed.max(rec.expire_hlc.physical_ms()), 0);
        if expire_hlc.physical_ms() > cap {
            return Err(MetaDbError::TransactionAborted(
                "2PC lease renewal exceeds lifetime cap".into(),
            ));
        }
        let mut grouped: HashMap<ShardId, (Vec<u8>, Vec<Vec<u8>>)> = HashMap::new();
        for key in &rec.participant_keys {
            let (_, shard, _) = self.route_user_key(key).await?;
            let entry = grouped
                .entry(shard.shard_id())
                .or_insert_with(|| (key.clone(), Vec::new()));
            entry.1.push(key.clone());
        }
        if grouped.is_empty() {
            grouped.insert(0, (primary_key.to_vec(), vec![primary_key.to_vec()]));
        }
        for (_, (route_key, keys)) in grouped {
            let cmd = TxRaftCmd::Extend2pcLease {
                txn_id,
                coord_node_id: self.coord_node_id,
                expire_hlc,
                start_hlc: rec.start_hlc,
                rename_fence_token: rec.rename_fence_token,
                target_user_keys: keys,
            };
            self.propose_participant_retry_epoch(&route_key, &cmd, now)
                .await?;
        }
        Ok(expire_hlc)
    }

    async fn abort_prepared_shards(&self, txn_id: TxnId, prepared: &[PreparedShardBatch]) {
        let now = self.clock.now();
        for (_ep, _grp, keys) in prepared {
            let cmd = TxRaftCmd::Abort2pc {
                txn_id,
                target_user_keys: keys.clone(),
            };
            if let Some(route_key) = keys.first() {
                let _ = self
                    .propose_participant_retry_epoch(route_key, &cmd, now)
                    .await;
            }
        }
    }

    async fn finish_read<T>(
        &self,
        local: Result<T>,
        op: ForwardedRouterOp,
        decode: impl Fn(ForwardedRouterOut) -> Result<T>,
    ) -> Result<T> {
        match local {
            Err(MetaDbError::NotLeader {
                shard_id,
                leader_hint: Some(leader),
            }) => self.chase_forward(shard_id, leader, &op, decode).await,
            other => other,
        }
    }

    async fn chase_forward<T>(
        &self,
        mut shard_id: u64,
        mut leader: u64,
        op: &ForwardedRouterOp,
        decode: impl Fn(ForwardedRouterOut) -> Result<T>,
    ) -> Result<T> {
        let deadline = tokio::time::Instant::now() + ROUTE_RETRY_TIMEOUT;
        let mut last = MetaDbError::NotLeader {
            shard_id,
            leader_hint: Some(leader),
        };
        for _ in 0..3 {
            if tokio::time::Instant::now() >= deadline {
                return Err(last);
            }
            let Some(addr) = self.directory.raft_peer_addr(shard_id, leader) else {
                return Err(last);
            };
            match forward_op(&self.forward_secret, &addr, op, deadline).await {
                Ok(out) => return decode(out),
                Err(MetaDbError::NotLeader {
                    shard_id: next_shard,
                    leader_hint: Some(next),
                }) => {
                    shard_id = next_shard;
                    leader = next;
                    last = MetaDbError::NotLeader {
                        shard_id,
                        leader_hint: Some(leader),
                    };
                }
                Err(error) => return Err(error),
            }
        }
        Err(last)
    }

    /// Runs a forwarded client read locally. This does not forward again.
    pub async fn execute_forwarded_bytes(&self, bytes: &[u8]) -> Result<Vec<u8>> {
        let op: ForwardedRouterOp = codec::deserialize(bytes)
            .map_err(|error| MetaDbError::CodecError(error.to_string()))?;
        let out = match op {
            ForwardedRouterOp::Lookup { pid, name } => {
                ForwardedRouterOut::Lookup(retry_routing(|| self.lookup_once(pid, &name)).await?)
            }
            ForwardedRouterOp::DirStat { pid } => {
                ForwardedRouterOut::DirStat(retry_routing(|| self.dirstat_once(pid)).await?)
            }
            ForwardedRouterOp::ListDir {
                pid,
                after_name,
                name_prefix,
                limit,
            } => {
                let (entries, cursor) = retry_routing(|| {
                    self.listdir_paginated_once(
                        pid,
                        after_name.as_deref(),
                        name_prefix.as_deref(),
                        limit,
                    )
                })
                .await?;
                ForwardedRouterOut::ListDir(entries, cursor)
            }
            ForwardedRouterOp::GetQuota { uid } => {
                ForwardedRouterOut::GetQuota(retry_routing(|| self.get_quota_once(uid)).await?)
            }
            ForwardedRouterOp::ListQuotas { start_uid, limit } => ForwardedRouterOut::ListQuotas(
                retry_routing(|| self.list_quotas_once(start_uid, limit)).await?,
            ),
            ForwardedRouterOp::GetSecondaryIndex { fid } => ForwardedRouterOut::GetSecondaryIndex(
                retry_routing(|| self.get_secondary_index_once(fid)).await?,
            ),
            ForwardedRouterOp::ScanSecondaryIndex { after_fid, limit } => {
                ForwardedRouterOut::ScanSecondaryIndex(
                    retry_routing(|| self.scan_secondary_index_once(after_fid, limit)).await?,
                )
            }
            ForwardedRouterOp::ShardPointDirStat {
                shard_id,
                epoch,
                pid,
            } => ForwardedRouterOut::DirStat(self.local_point_dirstat(shard_id, epoch, pid).await?),
            ForwardedRouterOp::ShardAppliedHlc { shard_id, epoch } => {
                ForwardedRouterOut::AppliedHlc(self.local_applied_hlc(shard_id, epoch).await?.0)
            }
            ForwardedRouterOp::ShardBarrierStat {
                shard_id,
                epoch,
                pid,
                barrier,
            } => {
                let (anchor, children, bytes, mtime) = self
                    .local_barrier_stat(shard_id, epoch, pid, barrier)
                    .await?;
                ForwardedRouterOut::BarrierStat {
                    anchor,
                    children,
                    bytes,
                    mtime: mtime.0,
                }
            }
            ForwardedRouterOp::ShardMetaScan {
                shard_id,
                epoch,
                start,
                end,
                name_prefix,
                limit,
            } => ForwardedRouterOut::ListDir(
                self.local_meta_scan(shard_id, epoch, &start, &end, name_prefix.as_deref(), limit)
                    .await?,
                None,
            ),
            ForwardedRouterOp::ShardQuotaScan {
                shard_id,
                epoch,
                start,
                end,
                limit,
            } => ForwardedRouterOut::ListQuotas(
                self.local_quota_scan(shard_id, epoch, &start, &end, limit)
                    .await?,
            ),
            ForwardedRouterOp::ShardIndexScan {
                shard_id,
                epoch,
                start,
                end,
                limit,
            } => ForwardedRouterOut::ScanSecondaryIndex(
                self.local_index_scan(shard_id, epoch, &start, &end, limit)
                    .await?,
            ),
            ForwardedRouterOp::ShardDirPartition {
                shard_id,
                epoch,
                pid,
                barrier,
            } => {
                let stat = self
                    .local_dir_partition(shard_id, epoch, pid, barrier)
                    .await?;
                ForwardedRouterOut::DirPartition {
                    is_anchor: stat.is_anchor,
                    attr: stat.attr,
                    max_applied_hlc: stat.max_applied_hlc,
                    anchor_missing: stat.anchor_missing,
                }
            }
        };
        codec::serialize(&out).map_err(|error| MetaDbError::CodecError(error.to_string()))
    }

    async fn forward_shard<T>(
        &self,
        shard_id: u64,
        leader: u64,
        op: ForwardedRouterOp,
        decode: impl Fn(ForwardedRouterOut) -> Option<T>,
    ) -> Result<T> {
        self.finish_read(
            Err(MetaDbError::NotLeader {
                shard_id,
                leader_hint: Some(leader),
            }),
            op,
            |out| {
                decode(out).ok_or_else(|| {
                    MetaDbError::CodecError(
                        "leader forward returned a different shard result".to_string(),
                    )
                })
            },
        )
        .await
    }

    async fn local_point_dirstat(
        &self,
        shard_id: u64,
        epoch: ShardEpoch,
        pid: InodeId,
    ) -> Result<InodeAttr> {
        let (_, group, handler) = self.directory.local_shard(shard_id)?;
        let (_, snap) = group.linearizable_read(epoch).await?;
        Self::validate_read_epoch(&group, epoch)?;
        let raw = group
            .engine()
            .get_with_snapshot(&MetaKey::dir_attr(pid).encode(), &snap)?
            .ok_or(MetaDbError::DirectoryNotFound(pid))?;
        let reconciled =
            handler
                .attr_aggregator()
                .reconcile_inode_attr(pid, InodeAttr::decode(&raw)?, &snap);
        Self::validate_read_epoch(&group, epoch)?;
        Ok(reconciled)
    }

    async fn local_applied_hlc(&self, shard_id: u64, epoch: ShardEpoch) -> Result<HlcTimestamp> {
        let (_, group, _) = self.directory.local_shard(shard_id)?;
        Ok(group
            .read_batcher()
            .linearizable_token(epoch)
            .await?
            .max_applied_hlc)
    }

    async fn local_barrier_stat(
        &self,
        shard_id: u64,
        epoch: ShardEpoch,
        pid: InodeId,
        barrier: HlcTimestamp,
    ) -> Result<(Option<InodeAttr>, i64, i64, HlcTimestamp)> {
        let (_, group, handler) = self.directory.local_shard(shard_id)?;
        let (_, snap) = group.fixed_barrier_read(epoch, barrier).await?;
        Self::validate_read_epoch(&group, epoch)?;
        let range = handler.key_range();
        let attr_key = MetaKey::dir_attr(pid).encode();
        if range.contains_key(&attr_key) {
            let raw = group
                .engine()
                .get_with_snapshot(&attr_key, &snap)?
                .ok_or(MetaDbError::DirectoryNotFound(pid))?;
            let rec = handler.attr_aggregator().reconcile_inode_attr(
                pid,
                InodeAttr::decode(&raw)?,
                &snap,
            );
            Ok((
                Some(rec.clone()),
                rec.num_children,
                rec.size_bytes,
                rec.modify_time,
            ))
        } else if let Some(raw) = group
            .engine()
            .get_with_snapshot(&DirShardDeltaKey { pid }.encode(), &snap)?
        {
            let sub = handler.attr_aggregator().reconcile_dir_shard_delta(
                pid,
                DirShardDeltaRecord::decode(&raw)?,
                &snap,
            );
            Ok((None, sub.num_children, sub.size_bytes, sub.modify_time))
        } else {
            Ok((None, 0, 0, HlcTimestamp::ZERO))
        }
    }

    async fn local_meta_scan(
        &self,
        shard_id: u64,
        epoch: ShardEpoch,
        start: &[u8],
        end: &[u8],
        name_prefix: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MetaEntry>> {
        let (_, group, _) = self.directory.local_shard(shard_id)?;
        let (_, snap) = group.linearizable_read(epoch).await?;
        Self::validate_read_epoch(&group, epoch)?;
        let mut out = Vec::new();
        for (key, value) in group.engine().scan_with_snapshot(start, end, &snap)? {
            let meta_key = MetaKey::decode(&key)?;
            if meta_key.is_attr_key() {
                continue;
            }
            if let Some(prefix) = name_prefix {
                if !meta_key.name.starts_with(prefix) {
                    break;
                }
            }
            out.push(MetaEntry {
                key: meta_key,
                attr: InodeAttr::decode(&value)?,
            });
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn local_quota_scan(
        &self,
        shard_id: u64,
        epoch: ShardEpoch,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<QuotaRecord>> {
        let (_, group, _) = self.directory.local_shard(shard_id)?;
        let (_, snap) = group.linearizable_read(epoch).await?;
        Self::validate_read_epoch(&group, epoch)?;
        let mut out = Vec::new();
        for (_, value) in group.engine().scan_with_snapshot(start, end, &snap)? {
            out.push(publish_quota(QuotaRecord::decode(&value)?));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn local_index_scan(
        &self,
        shard_id: u64,
        epoch: ShardEpoch,
        start: &[u8],
        end: &[u8],
        limit: usize,
    ) -> Result<Vec<SecondaryIndexRecord>> {
        let (_, group, _) = self.directory.local_shard(shard_id)?;
        let (_, snap) = group.linearizable_read(epoch).await?;
        Self::validate_read_epoch(&group, epoch)?;
        let mut out = Vec::new();
        for (_, value) in group.engine().scan_with_snapshot(start, end, &snap)? {
            let record = SecondaryIndexRecord::decode(&value)?;
            if !record.is_deleted {
                out.push(record);
            }
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// Linearizable point `Lookup(pid, name)` resolving any encountered `[0x05]` `WriteIntent`s.
    pub async fn lookup(&self, pid: InodeId, name: &str) -> Result<Option<MetaEntry>> {
        let op = ForwardedRouterOp::Lookup {
            pid,
            name: name.to_string(),
        };
        self.finish_read(
            retry_routing(|| self.lookup_once(pid, name)).await,
            op,
            |out| match out {
                ForwardedRouterOut::Lookup(value) => Ok(value),
                _ => Err(MetaDbError::CodecError(
                    "leader forward returned a different operation result".to_string(),
                )),
            },
        )
        .await
    }

    async fn lookup_once(&self, pid: InodeId, name: &str) -> Result<Option<MetaEntry>> {
        validate_user_entry_name(name)?;
        let mk = MetaKey::child(pid, name);
        let key_bytes = mk.encode();

        for _ in 0..8 {
            let (group, snap) = self.read_user_key(&key_bytes).await?;
            let engine = group.engine();

            // Check `[0x05]` intent at `snap.cut()`.
            let ik = WriteIntentKey::new(key_bytes.clone()).encode();
            if let Some(raw_intent) = engine.get_with_snapshot(&ik, &snap)? {
                let intent = WriteIntentRecord::decode(&raw_intent)?;
                if !matches!(intent.op, IntentOp::DirExistsFenceSet { .. })
                    && intent.start_hlc <= snap.read_hlc()
                {
                    drop(snap);
                    TxnRecoveryWorker::resolve_single_txn(
                        self.directory.as_ref(),
                        &self.clock,
                        intent.txn_id,
                        &intent.primary_key,
                    )
                    .await?;
                    continue;
                }
            }

            let Some(raw_val) = engine.get_with_snapshot(&key_bytes, &snap)? else {
                return Ok(None);
            };
            return Ok(Some(MetaEntry {
                key: mk,
                attr: InodeAttr::decode(&raw_val)?,
            }));
        }
        Err(MetaDbError::ShardRoutingError(
            "lookup exceeded max intent/route retries".to_string(),
        ))
    }

    /// Reads committed bytes used as the predicate for an anchor attribute mutation.
    async fn read_raw_user_value(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        retry_routing(|| self.read_raw_user_value_once(key)).await
    }

    async fn read_raw_user_value_once(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        for _ in 0..12 {
            let (group, snap) = self.read_user_key(key).await?;
            let engine = group.engine();
            let intent_key = WriteIntentKey::new(key.to_vec()).encode();
            if let Some(raw) = engine.get_with_snapshot(&intent_key, &snap)? {
                let intent = WriteIntentRecord::decode(&raw)?;
                if !matches!(intent.op, IntentOp::DirExistsFenceSet { .. }) {
                    drop(snap);
                    TxnRecoveryWorker::resolve_single_txn(
                        self.directory.as_ref(),
                        &self.clock,
                        intent.txn_id,
                        &intent.primary_key,
                    )
                    .await?;
                    continue;
                }
            }
            return engine.get_with_snapshot(key, &snap);
        }
        Err(MetaDbError::ShardRoutingError(
            "anchor read exceeded max intent/route retries".to_string(),
        ))
    }

    /// Directory partition on `shard_id`. A follower that knows the leader forwards
    /// this read and does not return `NotLeader` to the caller.
    pub async fn dir_partition_stat(
        &self,
        shard_id: u64,
        epoch: ShardEpoch,
        pid: InodeId,
        barrier: Option<HlcTimestamp>,
    ) -> Result<DirPartitionStat> {
        // The caller already chose this shard and epoch. Retry only a missing
        // quorum or an election with no leader hint. An epoch or routing error
        // must return immediately so the caller can refresh the route.
        let deadline = tokio::time::Instant::now() + ELECTION_RETRY_TIMEOUT;
        loop {
            match self
                .dir_partition_stat_once(shard_id, epoch, pid, barrier)
                .await
            {
                Err(
                    error @ (MetaDbError::RetryableConsensus {
                        kind: metadb_core::RetryableConsensusKind::QuorumUnavailable,
                        ..
                    }
                    | MetaDbError::NotLeader {
                        leader_hint: None, ..
                    }),
                ) => {
                    let now = tokio::time::Instant::now();
                    if now >= deadline {
                        return Err(error);
                    }
                    tokio::time::sleep(ROUTE_RETRY_INTERVAL.min(deadline - now)).await;
                }
                other => return other,
            }
        }
    }

    async fn dir_partition_stat_once(
        &self,
        shard_id: u64,
        epoch: ShardEpoch,
        pid: InodeId,
        barrier: Option<HlcTimestamp>,
    ) -> Result<DirPartitionStat> {
        match self
            .local_dir_partition(shard_id, epoch, pid, barrier)
            .await
        {
            Err(MetaDbError::NotLeader {
                shard_id,
                leader_hint: Some(leader),
            }) => {
                self.forward_shard(
                    shard_id,
                    leader,
                    ForwardedRouterOp::ShardDirPartition {
                        shard_id,
                        epoch,
                        pid,
                        barrier,
                    },
                    |out| match out {
                        ForwardedRouterOut::DirPartition {
                            is_anchor,
                            attr,
                            max_applied_hlc,
                            anchor_missing,
                        } => Some(DirPartitionStat {
                            is_anchor,
                            attr,
                            max_applied_hlc,
                            anchor_missing,
                        }),
                        _ => None,
                    },
                )
                .await
            }
            other => other,
        }
    }

    async fn local_dir_partition(
        &self,
        shard_id: u64,
        epoch: ShardEpoch,
        pid: InodeId,
        barrier: Option<HlcTimestamp>,
    ) -> Result<DirPartitionStat> {
        let (_, group, handler) = self.directory.local_shard(shard_id)?;
        let (token, snap) = match barrier {
            Some(barrier) => group.fixed_barrier_read(epoch, barrier).await?,
            None => group.linearizable_read(epoch).await?,
        };
        Self::validate_read_epoch(&group, epoch)?;
        let max_applied_hlc = token.max_applied_hlc.0;
        let attr_key = MetaKey::dir_attr(pid).encode();
        if handler.key_range().contains_key(&attr_key) {
            let attr = match group.engine().get_with_snapshot(&attr_key, &snap)? {
                Some(raw) => {
                    let base = InodeAttr::decode(&raw)?;
                    Some(
                        handler
                            .attr_aggregator()
                            .reconcile_inode_attr(pid, base, &snap),
                    )
                }
                None => None,
            };
            return Ok(DirPartitionStat {
                is_anchor: true,
                anchor_missing: attr.is_none(),
                attr,
                max_applied_hlc,
            });
        }
        let delta_key = DirShardDeltaKey { pid }.encode();
        let attr = match group.engine().get_with_snapshot(&delta_key, &snap)? {
            Some(raw) => {
                let sub_base = DirShardDeltaRecord::decode(&raw)?;
                let sub = handler
                    .attr_aggregator()
                    .reconcile_dir_shard_delta(pid, sub_base, &snap);
                Some(InodeAttr {
                    inode_id: sub.inode_id,
                    parent_pid: sub.parent_pid,
                    entry_type: EntryType::DirectoryAttr,
                    uid: sub.uid,
                    mode: sub.mode,
                    link_count: 1,
                    num_children: sub.num_children,
                    size_bytes: sub.size_bytes,
                    create_time: sub.create_time,
                    modify_time: sub.modify_time,
                    fid: None,
                    applied_lsn: sub.applied_raft_index,
                })
            }
            None => None,
        };
        Ok(DirPartitionStat {
            is_anchor: false,
            attr,
            max_applied_hlc,
            anchor_missing: false,
        })
    }

    /// Linearizable `DirStat(pid)`:
    /// - Fast path (`1` shard covers `[pid, pid+1)`): single `ReadIndex` + `reconcile_inode_attr`.
    /// - `DualModeFineGrained` (`K` sub-shards): parallel `ReadIndex` + shared `barrier_hlc`
    ///   scatter-gather across `S_1 .. S_K` (`Resolution B1` / `G-P1`).
    pub async fn dirstat(&self, pid: InodeId) -> Result<InodeAttr> {
        retry_routing(|| self.dirstat_once(pid)).await
    }

    async fn dirstat_once(&self, pid: InodeId) -> Result<InodeAttr> {
        let start = MetaKey::encode_pid_prefix_start(pid);
        let end = MetaKey::encode_pid_prefix_end(pid);

        for _ in 0..8 {
            let shards = self.directory.route_user_range(&start, &end)?;
            if shards.len() == 1 {
                let (epoch, group, handler) = &shards[0];
                let (_, snap) = match group.linearizable_read(*epoch).await {
                    Ok(pair) => pair,
                    Err(MetaDbError::NotLeader {
                        shard_id,
                        leader_hint: Some(leader),
                    }) => {
                        return self
                            .forward_shard(
                                shard_id,
                                leader,
                                ForwardedRouterOp::ShardPointDirStat {
                                    shard_id,
                                    epoch: *epoch,
                                    pid,
                                },
                                |out| match out {
                                    ForwardedRouterOut::DirStat(attr) => Some(attr),
                                    _ => None,
                                },
                            )
                            .await;
                    }
                    Err(MetaDbError::EpochMismatch { .. } | MetaDbError::NotLeader { .. }) => {
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                Self::validate_read_epoch(group, *epoch)?;
                let attr_key = MetaKey::dir_attr(pid).encode();
                let Some(raw) = group.engine().get_with_snapshot(&attr_key, &snap)? else {
                    return Err(MetaDbError::DirectoryNotFound(pid));
                };
                let persisted = InodeAttr::decode(&raw)?;
                let reconciled = handler
                    .attr_aggregator()
                    .reconcile_inode_attr(pid, persisted, &snap);
                Self::validate_read_epoch(group, *epoch)?;
                return Ok(reconciled);
            }

            // `DualModeFineGrained` multi-shard scatter-gather with shared `barrier_hlc`:
            let mut max_hlc = self.clock.now();
            let mut retry_route = false;
            for (epoch, group, _) in &shards {
                match group.read_batcher().linearizable_token(*epoch).await {
                    Ok(tok) => max_hlc = max_hlc.max(tok.max_applied_hlc),
                    Err(MetaDbError::NotLeader {
                        shard_id,
                        leader_hint: Some(leader),
                    }) => {
                        match self
                            .forward_shard(
                                shard_id,
                                leader,
                                ForwardedRouterOp::ShardAppliedHlc {
                                    shard_id,
                                    epoch: *epoch,
                                },
                                |out| match out {
                                    ForwardedRouterOut::AppliedHlc(raw) => Some(HlcTimestamp(raw)),
                                    _ => None,
                                },
                            )
                            .await
                        {
                            Ok(hlc) => max_hlc = max_hlc.max(hlc),
                            Err(
                                MetaDbError::NotLeader { .. } | MetaDbError::EpochMismatch { .. },
                            ) => {
                                retry_route = true;
                                break;
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    Err(MetaDbError::EpochMismatch { .. } | MetaDbError::NotLeader { .. }) => {
                        retry_route = true;
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            if retry_route {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            let barrier_hlc = self.clock.update(max_hlc);
            let attr_key = MetaKey::dir_attr(pid).encode();
            let delta_key = DirShardDeltaKey { pid }.encode();

            let mut anchor_attr: Option<InodeAttr> = None;
            let mut total_children = 0i64;
            let mut total_bytes = 0i64;
            let mut latest_mtime = HlcTimestamp::ZERO;

            for (epoch, group, handler) in &shards {
                let range = handler.key_range();
                let (_, snap) = match group.fixed_barrier_read(*epoch, barrier_hlc).await {
                    Ok(pair) => pair,
                    Err(MetaDbError::NotLeader {
                        shard_id,
                        leader_hint: Some(leader),
                    }) => {
                        let (anchor, children, bytes, mtime) = self
                            .forward_shard(
                                shard_id,
                                leader,
                                ForwardedRouterOp::ShardBarrierStat {
                                    shard_id,
                                    epoch: *epoch,
                                    pid,
                                    barrier: barrier_hlc,
                                },
                                |out| match out {
                                    ForwardedRouterOut::BarrierStat {
                                        anchor,
                                        children,
                                        bytes,
                                        mtime,
                                    } => Some((anchor, children, bytes, HlcTimestamp(mtime))),
                                    _ => None,
                                },
                            )
                            .await?;
                        total_children = saturating_add_i64(total_children, children);
                        total_bytes = saturating_add_i64(total_bytes, bytes);
                        latest_mtime = latest_mtime.max(mtime);
                        if let Some(anchor) = anchor {
                            anchor_attr = Some(anchor);
                        }
                        continue;
                    }
                    Err(MetaDbError::EpochMismatch { .. } | MetaDbError::NotLeader { .. }) => {
                        retry_route = true;
                        break;
                    }
                    Err(error) => return Err(error),
                };
                Self::validate_read_epoch(group, *epoch)?;
                let eng = group.engine();
                if range.contains_key(&attr_key) {
                    let Some(raw) = eng.get_with_snapshot(&attr_key, &snap)? else {
                        return Err(MetaDbError::DirectoryNotFound(pid));
                    };
                    let rec = handler.attr_aggregator().reconcile_inode_attr(
                        pid,
                        InodeAttr::decode(&raw)?,
                        &snap,
                    );
                    total_children = saturating_add_i64(total_children, rec.num_children);
                    total_bytes = saturating_add_i64(total_bytes, rec.size_bytes);
                    latest_mtime = latest_mtime.max(rec.modify_time);
                    anchor_attr = Some(rec);
                } else if let Some(raw_sub) = eng.get_with_snapshot(&delta_key, &snap)? {
                    let sub = handler.attr_aggregator().reconcile_dir_shard_delta(
                        pid,
                        DirShardDeltaRecord::decode(&raw_sub)?,
                        &snap,
                    );
                    total_children = saturating_add_i64(total_children, sub.num_children);
                    total_bytes = saturating_add_i64(total_bytes, sub.size_bytes);
                    latest_mtime = latest_mtime.max(sub.modify_time);
                }
            }
            if retry_route {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                continue;
            }

            for (epoch, group, _) in &shards {
                Self::validate_read_epoch(group, *epoch)?;
            }

            let mut canonical = anchor_attr.ok_or(MetaDbError::DirectoryNotFound(pid))?;
            canonical.num_children = total_children.max(0);
            canonical.size_bytes = total_bytes.max(0);
            canonical.modify_time = canonical.modify_time.max(latest_mtime);
            return Ok(canonical);
        }

        Err(MetaDbError::ShardRoutingError(
            "dirstat exceeded max route retries".to_string(),
        ))
    }

    /// Keyset-paginated `ListDir(pid, after_name, prefix, limit)` unconditionally excluding `"_attr_"`.
    pub async fn listdir_paginated(
        &self,
        pid: InodeId,
        after_name: Option<&str>,
        name_prefix: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<MetaEntry>, Option<String>)> {
        retry_routing(|| self.listdir_paginated_once(pid, after_name, name_prefix, limit)).await
    }

    async fn listdir_paginated_once(
        &self,
        pid: InodeId,
        after_name: Option<&str>,
        name_prefix: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<MetaEntry>, Option<String>)> {
        let effective_limit = if limit == 0 { 10_000 } else { limit };
        let scan_start = listdir_scan_start(pid, after_name, name_prefix);
        let scan_end = MetaKey::encode_pid_prefix_end(pid);
        if scan_start >= scan_end {
            return Ok((Vec::new(), None));
        }

        let shards = self.directory.route_user_range(&scan_start, &scan_end)?;
        let mut collected: Vec<MetaEntry> = Vec::new();

        for (epoch, group, handler) in shards {
            if collected.len() >= effective_limit {
                break;
            }
            let r = handler.key_range();
            let sub_start = scan_start.as_slice().max(r.start_key.as_slice());
            let sub_end = scan_end.as_slice().min(r.end_key.as_slice());
            if sub_start >= sub_end {
                continue;
            }

            let (_, snap) = match group.linearizable_read(epoch).await {
                Ok(pair) => pair,
                Err(MetaDbError::NotLeader {
                    shard_id,
                    leader_hint: Some(leader),
                }) => {
                    let entries = self
                        .forward_shard(
                            shard_id,
                            leader,
                            ForwardedRouterOp::ShardMetaScan {
                                shard_id,
                                epoch,
                                start: sub_start.to_vec(),
                                end: sub_end.to_vec(),
                                name_prefix: name_prefix.map(str::to_string),
                                limit: effective_limit.saturating_sub(collected.len()),
                            },
                            |out| match out {
                                ForwardedRouterOut::ListDir(entries, _) => Some(entries),
                                _ => None,
                            },
                        )
                        .await?;
                    for entry in entries {
                        if let Some(prefix) = name_prefix {
                            if !entry.key.name.starts_with(prefix) {
                                return Ok((collected, None));
                            }
                        }
                        collected.push(entry);
                        if collected.len() >= effective_limit {
                            let next_cursor = collected.last().map(|entry| entry.key.name.clone());
                            return Ok((collected, next_cursor));
                        }
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            Self::validate_read_epoch(&group, epoch)?;
            let eng = group.engine();

            // Resolve any visible active `[0x05]` child write intents in `[sub_start, sub_end)`.
            let i_start = WriteIntentKey::encode_range_bound(sub_start);
            let i_end = WriteIntentKey::encode_range_bound(sub_end);
            let intents = eng.scan_with_snapshot(&i_start, &i_end, &snap)?;
            let mut resolved_any = false;
            for (_, raw_i) in &intents {
                let irec = WriteIntentRecord::decode(raw_i)?;
                if !matches!(irec.op, IntentOp::DirExistsFenceSet { .. })
                    && irec.start_hlc <= snap.read_hlc()
                {
                    resolved_any = true;
                    TxnRecoveryWorker::resolve_single_txn(
                        self.directory.as_ref(),
                        &self.clock,
                        irec.txn_id,
                        &irec.primary_key,
                    )
                    .await?;
                }
            }

            let final_snap = if resolved_any {
                drop(snap);
                group.linearizable_read(epoch).await?.1
            } else {
                snap
            };
            Self::validate_read_epoch(&group, epoch)?;

            let mut stop = ListDirStop::Continue;
            let need = effective_limit;
            eng.scan_live_until(sub_start, sub_end, &final_snap, |k_bytes, v_bytes| {
                let mk = MetaKey::decode(k_bytes)?;
                // Strict `_attr_` isolation rule (`Resolution N2` / `G-P1`): never expose `_attr_`!
                if mk.is_attr_key() {
                    return Ok(ScanControl::Continue);
                }
                if let Some(pfx) = name_prefix {
                    if !mk.name.starts_with(pfx) {
                        stop = ListDirStop::PrefixEnded;
                        return Ok(ScanControl::Stop);
                    }
                }
                collected.push(MetaEntry {
                    key: mk,
                    attr: InodeAttr::decode(v_bytes)?,
                });
                if collected.len() >= need {
                    stop = ListDirStop::PageFull;
                    Ok(ScanControl::Stop)
                } else {
                    Ok(ScanControl::Continue)
                }
            })?;
            match stop {
                ListDirStop::PrefixEnded => return Ok((collected, None)),
                ListDirStop::PageFull => {
                    let next_cursor = collected.last().map(|e| e.key.name.clone());
                    return Ok((collected, next_cursor));
                }
                ListDirStop::Continue => {}
            }
        }

        Ok((collected, None))
    }

    pub async fn set_quota_limit(
        &self,
        uid: UserId,
        quota_limit_bytes: u64,
    ) -> Result<QuotaRecord> {
        let qk = QuotaKey { uid }.encode();
        let cmd = TxRaftCmd::SetQuotaLimit {
            uid,
            quota_limit_bytes,
        };
        match self.propose_key_with_retry(&qk, &cmd).await? {
            TxRaftOutput::Quota(q) => Ok(q),
            _ => Err(MetaDbError::CodecError(
                "unexpected SetQuotaLimit output".to_string(),
            )),
        }
    }

    pub async fn upsert_quota_limit(
        &self,
        uid: UserId,
        insert_quota_limit_bytes: u64,
        update_quota_limit_bytes: u64,
    ) -> Result<QuotaRecord> {
        let qk = QuotaKey { uid }.encode();
        let cmd = TxRaftCmd::UpsertQuotaLimit {
            uid,
            insert_quota_limit_bytes,
            update_quota_limit_bytes,
        };
        match self.propose_key_with_retry(&qk, &cmd).await? {
            TxRaftOutput::Quota(q) => Ok(q),
            _ => Err(MetaDbError::CodecError(
                "unexpected UpsertQuotaLimit output".into(),
            )),
        }
    }

    pub async fn get_quota(&self, uid: UserId) -> Result<Option<QuotaRecord>> {
        let op = ForwardedRouterOp::GetQuota { uid };
        self.finish_read(
            retry_routing(|| self.get_quota_once(uid)).await,
            op,
            |out| match out {
                ForwardedRouterOut::GetQuota(record) => Ok(record),
                _ => Err(MetaDbError::CodecError(
                    "leader forward returned a different operation result".to_string(),
                )),
            },
        )
        .await
    }

    async fn get_quota_once(&self, uid: UserId) -> Result<Option<QuotaRecord>> {
        let qk = QuotaKey { uid }.encode();
        let (group, snap) = self.read_user_key(&qk).await?;
        let Some(raw) = group.engine().get_with_snapshot(&qk, &snap)? else {
            return Ok(None);
        };
        Ok(Some(publish_quota(QuotaRecord::decode(&raw)?)))
    }

    pub async fn list_quotas(&self, start_uid: UserId, limit: usize) -> Result<Vec<QuotaRecord>> {
        retry_routing(|| self.list_quotas_once(start_uid, limit)).await
    }

    async fn list_quotas_once(&self, start_uid: UserId, limit: usize) -> Result<Vec<QuotaRecord>> {
        let lim = if limit == 0 { 1000 } else { limit };
        let start = QuotaKey { uid: start_uid }.encode();
        let end = vec![TABLE_PREFIX_SECONDARY_INDEX];
        let shards = self.directory.route_user_range(&start, &end)?;
        let mut out = Vec::new();
        for (epoch, group, handler) in shards {
            let r = handler.key_range();
            let s = start.as_slice().max(r.start_key.as_slice());
            let e = end.as_slice().min(r.end_key.as_slice());
            if s >= e {
                continue;
            }
            let (_, snap) = match group.linearizable_read(epoch).await {
                Ok(pair) => pair,
                Err(MetaDbError::NotLeader {
                    shard_id,
                    leader_hint: Some(leader),
                }) => {
                    let records = self
                        .forward_shard(
                            shard_id,
                            leader,
                            ForwardedRouterOp::ShardQuotaScan {
                                shard_id,
                                epoch,
                                start: s.to_vec(),
                                end: e.to_vec(),
                                limit: lim.saturating_sub(out.len()),
                            },
                            |out| match out {
                                ForwardedRouterOut::ListQuotas(records) => Some(records),
                                _ => None,
                            },
                        )
                        .await?;
                    out.extend(records);
                    if out.len() >= lim {
                        return Ok(out);
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            Self::validate_read_epoch(&group, epoch)?;
            for (_, value) in group.engine().scan_with_snapshot(s, e, &snap)? {
                out.push(publish_quota(QuotaRecord::decode(&value)?));
                if out.len() >= lim {
                    return Ok(out);
                }
            }
        }
        Ok(out)
    }

    pub async fn get_secondary_index(&self, fid: FileId) -> Result<Option<SecondaryIndexRecord>> {
        let op = ForwardedRouterOp::GetSecondaryIndex { fid };
        self.finish_read(
            retry_routing(|| self.get_secondary_index_once(fid)).await,
            op,
            |out| match out {
                ForwardedRouterOut::GetSecondaryIndex(record) => Ok(record),
                _ => Err(MetaDbError::CodecError(
                    "leader forward returned a different operation result".to_string(),
                )),
            },
        )
        .await
    }

    async fn get_secondary_index_once(&self, fid: FileId) -> Result<Option<SecondaryIndexRecord>> {
        let ik = SecondaryIndexKey { fid }.encode();
        let (group, snap) = self.read_user_key(&ik).await?;
        let Some(raw) = group.engine().get_with_snapshot(&ik, &snap)? else {
            return Ok(None);
        };
        let rec = SecondaryIndexRecord::decode(&raw)?;
        if rec.is_deleted {
            Ok(None)
        } else {
            Ok(Some(rec))
        }
    }

    pub async fn scan_secondary_index(
        &self,
        after_fid: FileId,
        limit: usize,
    ) -> Result<Vec<SecondaryIndexRecord>> {
        retry_routing(|| self.scan_secondary_index_once(after_fid, limit)).await
    }

    async fn scan_secondary_index_once(
        &self,
        after_fid: FileId,
        limit: usize,
    ) -> Result<Vec<SecondaryIndexRecord>> {
        let lim = if limit == 0 { 1000 } else { limit };
        let Some(next_fid) = after_fid.checked_add(1) else {
            return Ok(Vec::new());
        };
        let start = SecondaryIndexKey { fid: next_fid }.encode();
        let end = vec![TABLE_PREFIX_SECONDARY_INDEX + 1];
        let shards = self.directory.route_user_range(&start, &end)?;
        let mut out = Vec::new();
        for (epoch, group, handler) in shards {
            let r = handler.key_range();
            let s = start.as_slice().max(r.start_key.as_slice());
            let e = end.as_slice().min(r.end_key.as_slice());
            if s >= e {
                continue;
            }
            let (_, snap) = match group.linearizable_read(epoch).await {
                Ok(pair) => pair,
                Err(MetaDbError::NotLeader {
                    shard_id,
                    leader_hint: Some(leader),
                }) => {
                    let records = self
                        .forward_shard(
                            shard_id,
                            leader,
                            ForwardedRouterOp::ShardIndexScan {
                                shard_id,
                                epoch,
                                start: s.to_vec(),
                                end: e.to_vec(),
                                limit: lim.saturating_sub(out.len()),
                            },
                            |out| match out {
                                ForwardedRouterOut::ScanSecondaryIndex(records) => Some(records),
                                _ => None,
                            },
                        )
                        .await?;
                    out.extend(records);
                    if out.len() >= lim {
                        return Ok(out);
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            Self::validate_read_epoch(&group, epoch)?;
            for (_, value) in group.engine().scan_with_snapshot(s, e, &snap)? {
                let rec = SecondaryIndexRecord::decode(&value)?;
                if !rec.is_deleted {
                    out.push(rec);
                    if out.len() >= lim {
                        return Ok(out);
                    }
                }
            }
        }
        Ok(out)
    }
}

#[allow(dead_code)]
const _ASSERT_QUOTA_PREFIX: u8 = TABLE_PREFIX_QUOTA;

#[derive(Clone, Copy)]
enum ListDirStop {
    Continue,
    PageFull,
    PrefixEnded,
}

/// Lower bound for a keyset `listdir`. When both a cursor and a prefix are set, the scan
/// starts at the later of the two so names before the prefix are not treated as the end
/// of the page.
pub(crate) fn listdir_scan_start(
    pid: InodeId,
    after_name: Option<&str>,
    name_prefix: Option<&str>,
) -> Vec<u8> {
    let mut start = match name_prefix {
        Some(pfx) if !pfx.is_empty() => MetaKey::child(pid, pfx).encode(),
        _ => MetaKey::encode_pid_prefix_start(pid),
    };
    if let Some(cursor) = after_name {
        if !cursor.is_empty() {
            let mut exclusive = MetaKey::child(pid, cursor).encode();
            // Names cannot contain NUL, so `cursor || 0x00` is the first key strictly after it.
            exclusive.push(0x00);
            if exclusive > start {
                start = exclusive;
            }
        }
    }
    start
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn listdir_prefix_is_not_skipped_when_cursor_is_before_it() {
        let prefixed = MetaKey::child(1, "b").encode();
        let start = listdir_scan_start(1, Some("a"), Some("b"));
        assert!(
            start >= prefixed,
            "cursor 'a' must not pull the scan below prefix 'b'"
        );
        let within = listdir_scan_start(1, Some("ba"), Some("b"));
        let mut after_ba = MetaKey::child(1, "ba").encode();
        after_ba.push(0x00);
        assert_eq!(within, after_ba);
    }
}
