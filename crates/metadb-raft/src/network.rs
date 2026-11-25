//! Multiplexed `tonic` gRPC transport (`RaftTransportService`) for Multi-Raft shard groups.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use openraft::async_runtime::watch::WatchReceiver;
use openraft::entry::RaftEntry;
use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, AppendResult, SnapshotResponse,
    TransferLeaderRequest, TransferLeaderResponse, VoteRequest, VoteResponse,
};
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::{BasicNode, OptionalSend, Raft, Snapshot};
use tokio::sync::Mutex as AsyncMutex;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

use metadb_core::codec;
use metadb_core::{MetaDbError, ShardId};
use metadb_proto::control_plane_service_client::ControlPlaneServiceClient;
use metadb_proto::raft_transport_service_client::RaftTransportServiceClient;
use metadb_proto::raft_transport_service_server::RaftTransportService;
use metadb_proto::{
    CloseRemovedShardRequest, OpenJoiningShardRequest, ProbeTransferLeaderRequest,
    ProbeTransferLeaderResponse, RaftRpcEnvelope, RaftRpcReply, SnapshotChunkRequest,
    SnapshotChunkResponse,
};
use metadb_security::TransportSecurity;

use crate::state_machine::ShardStateMachine;
use crate::types::{MetaDbRaftRequest, MetaDbRaftResponse, MetaDbTypeConfig, SnapshotArchive};

const SNAPSHOT_STREAM_CHUNK_BYTES: usize = 256 * 1024;

pub type MetaDbRaft = Raft<MetaDbTypeConfig, ShardStateMachine>;

/// Per-node registry mapping `ShardId -> MetaDbRaft` for incoming multiplexed Raft RPCs.
#[tonic::async_trait]
pub trait LocalProposer: Send + Sync {
    async fn propose_local_forwarded(
        &self,
        expected_epoch: metadb_core::ShardEpoch,
        caller_hlc: metadb_core::HlcTimestamp,
        payload: Vec<u8>,
    ) -> metadb_core::Result<MetaDbRaftResponse>;
}

#[derive(Clone, Default)]
pub struct MultiRaftRouter {
    groups: Arc<RwLock<HashMap<ShardId, MetaDbRaft>>>,
    local_proposers: Arc<RwLock<HashMap<ShardId, std::sync::Arc<dyn LocalProposer>>>>,
}

impl MultiRaftRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register_group(&self, shard_id: ShardId, raft: MetaDbRaft) {
        self.groups.write().unwrap().insert(shard_id, raft);
    }

    pub fn remove_group(&self, shard_id: ShardId) {
        self.groups.write().unwrap().remove(&shard_id);
    }

    pub fn get_group(&self, shard_id: ShardId) -> Option<MetaDbRaft> {
        self.groups.read().unwrap().get(&shard_id).cloned()
    }

    pub fn register_local_proposer(
        &self,
        shard_id: ShardId,
        proposer: std::sync::Arc<dyn LocalProposer>,
    ) {
        self.local_proposers
            .write()
            .unwrap()
            .insert(shard_id, proposer);
    }

    /// Drops the routed proposer for one shard so its LSM lock can be taken again.
    pub fn remove_local_proposer(&self, shard_id: ShardId) {
        self.local_proposers.write().unwrap().remove(&shard_id);
    }

    pub fn get_local_proposer(
        &self,
        shard_id: ShardId,
    ) -> Option<std::sync::Arc<dyn LocalProposer>> {
        self.local_proposers.read().unwrap().get(&shard_id).cloned()
    }

    /// Drops routed Raft handles so a later reopen can take the LSM lock.
    pub fn clear(&self) {
        self.local_proposers.write().unwrap().clear();
        self.groups.write().unwrap().clear();
    }
}

/// In-process hook allowing `DurableCellManager` peers in single-process tests (where
/// `ControlPlaneServiceServer` is not mounted) to open joining split/spare shards and GC
/// drained replicas directly, while multi-process deployments dial `ControlPlaneService` over gRPC.
#[tonic::async_trait]
pub trait LocalShardControlHook: Send + Sync {
    async fn open_joining_shard(
        &self,
        req: OpenJoiningShardRequest,
    ) -> std::result::Result<(), MetaDbError>;

    async fn close_removed_shard(
        &self,
        req: CloseRemovedShardRequest,
    ) -> std::result::Result<(), MetaDbError>;
}

/// Classification of outbound wire RPCs observed by [`NetworkChaosHook`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RpcWireKind {
    AppendEntries,
    Vote,
    InstallSnapshot,
    TransferLeader,
    ClientWrite,
    ControlPlane,
}

/// Fault-injection decision returned before sending an outbound RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireAction {
    Allow,
    Delay(Duration),
    DropBeforeSend(&'static str),
}

/// Deterministic or seeded network fault-injection hook for multi-node simulation/chaos tests.
pub trait NetworkChaosHook: Send + Sync {
    fn before_rpc(
        &self,
        from_addr: &str,
        to_addr: &str,
        shard_id: ShardId,
        kind: RpcWireKind,
    ) -> WireAction;

    fn after_rpc(
        &self,
        _from_addr: &str,
        _to_addr: &str,
        _shard_id: ShardId,
        _kind: RpcWireKind,
    ) -> bool {
        false
    }
}

/// Shared gRPC channel pool across Multi-Raft instances on the same host.
#[derive(Clone, Default)]
pub struct ChannelPool {
    channels: Arc<AsyncMutex<HashMap<String, Channel>>>,
    shared_secret: Arc<RwLock<Option<String>>>,
    security: Arc<RwLock<TransportSecurity>>,
    role: Arc<RwLock<Option<metadb_security::RolePresentation>>>,
    local_control_hooks: Arc<RwLock<HashMap<String, Arc<dyn LocalShardControlHook>>>>,
    local_addr: Arc<RwLock<String>>,
    has_chaos_hook: Arc<AtomicBool>,
    chaos_hook: Arc<RwLock<Option<Arc<dyn NetworkChaosHook>>>>,
}

impl ChannelPool {
    /// Installs a simulation/chaos network fault hook and binds `local_addr` as the sender identity.
    pub fn set_network_chaos_hook(
        &self,
        local_addr: impl Into<String>,
        hook: Option<Arc<dyn NetworkChaosHook>>,
    ) {
        *self.local_addr.write().unwrap() = local_addr.into();
        let active = hook.is_some();
        *self.chaos_hook.write().unwrap() = hook;
        self.has_chaos_hook.store(active, Ordering::Release);
    }

    pub(crate) async fn gate_before_rpc(
        &self,
        to_addr: &str,
        shard_id: ShardId,
        kind: RpcWireKind,
    ) -> Result<(), io::Error> {
        if !self.has_chaos_hook.load(Ordering::Relaxed) {
            return Ok(());
        }
        let hook = self.chaos_hook.read().unwrap().clone();
        let Some(hook) = hook else {
            return Ok(());
        };
        let from_addr = self.local_addr.read().unwrap().clone();
        match hook.before_rpc(&from_addr, to_addr, shard_id, kind) {
            WireAction::Allow => Ok(()),
            WireAction::Delay(d) => {
                metadb_core::code_probe("probe.raft.network_packet_delayed");
                if !d.is_zero() {
                    tokio::time::sleep(d).await;
                }
                Ok(())
            }
            WireAction::DropBeforeSend(reason) => {
                metadb_core::code_probe("probe.raft.network_link_clogged");
                Err(io::Error::new(io::ErrorKind::ConnectionRefused, reason))
            }
        }
    }

    pub(crate) fn gate_after_rpc(
        &self,
        to_addr: &str,
        shard_id: ShardId,
        kind: RpcWireKind,
    ) -> Result<(), io::Error> {
        if !self.has_chaos_hook.load(Ordering::Relaxed) {
            return Ok(());
        }
        let hook = self.chaos_hook.read().unwrap().clone();
        let Some(hook) = hook else {
            return Ok(());
        };
        let from_addr = self.local_addr.read().unwrap().clone();
        if hook.after_rpc(&from_addr, to_addr, shard_id, kind) {
            metadb_core::code_probe("probe.raft.network_reply_dropped");
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "simulated post-apply reply drop",
            ));
        }
        Ok(())
    }

    /// Registers an in-process control-plane hook for `addr` (used by in-process multi-voter tests).
    pub fn register_local_control_hook(&self, addr: String, hook: Arc<dyn LocalShardControlHook>) {
        self.local_control_hooks.write().unwrap().insert(addr, hook);
    }

    /// Configures authentication for all Raft clients sharing this pool.
    pub fn set_shared_secret(&self, secret: Option<String>) {
        *self.shared_secret.write().unwrap() = secret.filter(|s| !s.trim().is_empty());
    }

    /// Installs the process TLS policy. Plaintext is the default.
    ///
    /// Call this before the first outbound Raft or forwarded client RPC.
    pub fn set_transport_security(&self, security: TransportSecurity) {
        *self.security.write().unwrap() = security;
    }

    /// Presents the process RBAC principal on Raft RPCs. `None` sends no role headers.
    pub fn set_role_presentation(&self, role: Option<metadb_security::RolePresentation>) {
        *self.role.write().unwrap() = role;
    }

    fn authenticate<T>(&self, mut request: Request<T>) -> Result<Request<T>, io::Error> {
        if let Some(secret) = self.shared_secret.read().unwrap().as_ref() {
            let value = format!("Bearer {secret}").parse().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid Raft shared secret metadata",
                )
            })?;
            request.metadata_mut().insert("authorization", value);
        }
        if let Some(role) = self.role.read().unwrap().as_ref() {
            role.apply_metadata(request.metadata_mut())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        }
        Ok(request)
    }

    pub async fn connect(&self, addr: &str) -> Result<Channel, io::Error> {
        let security = self.security.read().unwrap().clone();
        let url = security
            .prepare_url(addr)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error.to_string()))?;
        {
            let guard = self.channels.lock().await;
            if let Some(ch) = guard.get(&url) {
                return Ok(ch.clone());
            }
        }
        let endpoint = Endpoint::from_shared(url.clone())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?
            .connect_timeout(Duration::from_secs(3));
        let channel = security
            .connect(endpoint)
            .await
            .map_err(|error| io::Error::new(io::ErrorKind::ConnectionRefused, error.to_string()))?;
        self.channels.lock().await.insert(url, channel.clone());
        Ok(channel)
    }

    /// Sends a client proposal to `addr`. The remote handler does not forward again.
    pub async fn client_write(
        &self,
        addr: &str,
        shard_id: ShardId,
        request: &MetaDbRaftRequest,
    ) -> std::result::Result<MetaDbRaftResponse, MetaDbError> {
        self.gate_before_rpc(addr, shard_id, RpcWireKind::ClientWrite)
            .await
            .map_err(|error| {
                MetaDbError::RpcError(format!("client_write to leader {addr} blocked: {error}"))
            })?;
        let channel = self.connect(addr).await.map_err(|error| {
            MetaDbError::RpcError(format!("connect to leader {addr} failed: {error}"))
        })?;
        let mut client = RaftTransportServiceClient::new(channel);
        let bincode_payload = codec::serialize(request)
            .map_err(|error| MetaDbError::CodecError(error.to_string()))?;
        let reply = client
            .client_write(
                self.authenticate(Request::new(RaftRpcEnvelope {
                    shard_id,
                    bincode_payload,
                }))
                .map_err(|error| MetaDbError::RpcError(error.to_string()))?,
            )
            .await
            .map_err(|error| {
                MetaDbError::RpcError(format!("client_write to leader {addr} failed: {error}"))
            })?
            .into_inner();
        self.gate_after_rpc(addr, shard_id, RpcWireKind::ClientWrite)
            .map_err(|error| {
                MetaDbError::RpcError(format!(
                    "client_write reply from leader {addr} dropped: {error}"
                ))
            })?;
        if !reply.error_message.is_empty() && reply.bincode_payload.is_empty() {
            return Err(MetaDbError::RpcError(reply.error_message));
        }
        codec::deserialize(&reply.bincode_payload)
            .map_err(|error| MetaDbError::CodecError(error.to_string()))?
    }

    /// Non-destructive pre-flight check verifying a target peer hosts `shard_id`, is an active
    /// voter, supports the leadership-transfer protocol, and has applied through `min_matched_index`.
    pub async fn probe_transfer_leader(
        &self,
        addr: &str,
        shard_id: ShardId,
        target_node_id: u64,
        min_matched_index: u64,
    ) -> std::result::Result<ProbeTransferLeaderResponse, MetaDbError> {
        self.gate_before_rpc(addr, shard_id, RpcWireKind::ControlPlane)
            .await
            .map_err(|error| {
                MetaDbError::RpcError(format!("probe_transfer_leader to {addr} blocked: {error}"))
            })?;
        let channel = self.connect(addr).await.map_err(|error| {
            MetaDbError::RpcError(format!("connect to transfer target {addr} failed: {error}"))
        })?;
        let mut client = RaftTransportServiceClient::new(channel);
        let reply = client
            .probe_transfer_leader(
                self.authenticate(Request::new(ProbeTransferLeaderRequest {
                    shard_id,
                    target_node_id,
                    min_matched_index,
                }))
                .map_err(|error| MetaDbError::RpcError(error.to_string()))?,
            )
            .await
            .map_err(|error| {
                MetaDbError::RpcError(format!("probe_transfer_leader to {addr} failed: {error}"))
            })?
            .into_inner();
        self.gate_after_rpc(addr, shard_id, RpcWireKind::ControlPlane)
            .map_err(|error| {
                MetaDbError::RpcError(format!(
                    "probe_transfer_leader reply from {addr} dropped: {error}"
                ))
            })?;
        Ok(reply)
    }

    /// Instructs a peer BE node (`addr`) to open an uninitialized joining replica for a split
    /// child shard or spare-voter onboarding before `ConfChangeOrchestrator::add_learners_and_promote_voters`.
    pub async fn open_joining_shard(
        &self,
        addr: &str,
        req: OpenJoiningShardRequest,
    ) -> std::result::Result<(), MetaDbError> {
        self.gate_before_rpc(addr, req.shard_id, RpcWireKind::ControlPlane)
            .await
            .map_err(|error| {
                MetaDbError::RpcError(format!("open_joining_shard to {addr} blocked: {error}"))
            })?;
        let local_hook = self.local_control_hooks.read().unwrap().get(addr).cloned();
        if let Some(hook) = local_hook {
            return hook.open_joining_shard(req).await;
        }
        let channel = self.connect(addr).await.map_err(|error| {
            MetaDbError::RpcError(format!("connect to peer {addr} failed: {error}"))
        })?;
        let mut client = ControlPlaneServiceClient::new(channel);
        let reply = client
            .open_joining_shard(
                self.authenticate(Request::new(req))
                    .map_err(|error| MetaDbError::RpcError(error.to_string()))?,
            )
            .await
            .map_err(|error| {
                MetaDbError::RpcError(format!("open_joining_shard to {addr} failed: {error}"))
            })?
            .into_inner();
        if let Some(err) = reply.error {
            return Err(MetaDbError::from(err));
        }
        Ok(())
    }

    /// Instructs a drained peer BE node (`addr`) to stop and optionally delete a removed shard replica.
    pub async fn close_removed_shard(
        &self,
        addr: &str,
        req: CloseRemovedShardRequest,
    ) -> std::result::Result<(), MetaDbError> {
        let local_hook = self.local_control_hooks.read().unwrap().get(addr).cloned();
        if let Some(hook) = local_hook {
            return hook.close_removed_shard(req).await;
        }
        let channel = self.connect(addr).await.map_err(|error| {
            MetaDbError::RpcError(format!("connect to peer {addr} failed: {error}"))
        })?;
        let mut client = ControlPlaneServiceClient::new(channel);
        let reply = client
            .close_removed_shard(
                self.authenticate(Request::new(req))
                    .map_err(|error| MetaDbError::RpcError(error.to_string()))?,
            )
            .await
            .map_err(|error| {
                MetaDbError::RpcError(format!("close_removed_shard to {addr} failed: {error}"))
            })?
            .into_inner();
        if let Some(err) = reply.error {
            return Err(MetaDbError::from(err));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct TonicRaftNetworkFactory {
    shard_id: ShardId,
    pool: ChannelPool,
}

impl TonicRaftNetworkFactory {
    pub fn new(shard_id: ShardId, pool: ChannelPool) -> Self {
        Self { shard_id, pool }
    }
}

pub struct TonicRaftNetworkClient {
    shard_id: ShardId,
    target_addr: String,
    pool: ChannelPool,
}

impl TonicRaftNetworkClient {
    fn unreachable(msg: impl std::fmt::Display) -> RPCError<MetaDbTypeConfig> {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, msg.to_string());
        RPCError::Unreachable(Unreachable::new(&io_err))
    }

    fn streaming_unreachable(msg: impl std::fmt::Display) -> StreamingError<MetaDbTypeConfig> {
        let io_err = io::Error::new(io::ErrorKind::ConnectionRefused, msg.to_string());
        StreamingError::Unreachable(Unreachable::new(&io_err))
    }
}

impl RaftNetworkFactory<MetaDbTypeConfig> for TonicRaftNetworkFactory {
    type Network = TonicRaftNetworkClient;

    async fn new_client(&mut self, _target: u64, node: &BasicNode) -> Self::Network {
        TonicRaftNetworkClient {
            shard_id: self.shard_id,
            target_addr: node.addr.clone(),
            pool: self.pool.clone(),
        }
    }
}

impl RaftNetworkV2<MetaDbTypeConfig> for TonicRaftNetworkClient {
    type SnapshotData = SnapshotArchive;

    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaDbTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendResult<MetaDbTypeConfig>, RPCError<MetaDbTypeConfig>> {
        let prev_log_id = rpc.prev_log_id;
        let last_log_id = rpc.entries.last().map(|e| e.log_id()).or(prev_log_id);

        self.pool
            .gate_before_rpc(&self.target_addr, self.shard_id, RpcWireKind::AppendEntries)
            .await
            .map_err(Self::unreachable)?;
        let ch = self
            .pool
            .connect(&self.target_addr)
            .await
            .map_err(Self::unreachable)?;
        let mut client = RaftTransportServiceClient::new(ch);

        let bincode_payload = codec::serialize_serde(&rpc).map_err(Self::unreachable)?;
        let reply = client
            .append_entries(
                self.pool
                    .authenticate(Request::new(RaftRpcEnvelope {
                        shard_id: self.shard_id,
                        bincode_payload,
                    }))
                    .map_err(Self::unreachable)?,
            )
            .await
            .map_err(Self::unreachable)?
            .into_inner();
        self.pool
            .gate_after_rpc(&self.target_addr, self.shard_id, RpcWireKind::AppendEntries)
            .map_err(Self::unreachable)?;

        if !reply.error_message.is_empty() {
            return Err(Self::unreachable(reply.error_message));
        }

        let wire_resp: AppendEntriesResponse<MetaDbTypeConfig> =
            codec::deserialize_serde(&reply.bincode_payload).map_err(Self::unreachable)?;
        Ok(wire_resp.into_append_result(prev_log_id, last_log_id))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<MetaDbTypeConfig>,
        _option: RPCOption,
    ) -> Result<VoteResponse<MetaDbTypeConfig>, RPCError<MetaDbTypeConfig>> {
        self.pool
            .gate_before_rpc(&self.target_addr, self.shard_id, RpcWireKind::Vote)
            .await
            .map_err(Self::unreachable)?;
        let ch = self
            .pool
            .connect(&self.target_addr)
            .await
            .map_err(Self::unreachable)?;
        let mut client = RaftTransportServiceClient::new(ch);

        let bincode_payload = codec::serialize_serde(&rpc).map_err(Self::unreachable)?;
        let reply = client
            .vote(
                self.pool
                    .authenticate(Request::new(RaftRpcEnvelope {
                        shard_id: self.shard_id,
                        bincode_payload,
                    }))
                    .map_err(Self::unreachable)?,
            )
            .await
            .map_err(Self::unreachable)?
            .into_inner();
        self.pool
            .gate_after_rpc(&self.target_addr, self.shard_id, RpcWireKind::Vote)
            .map_err(Self::unreachable)?;

        if !reply.error_message.is_empty() {
            return Err(Self::unreachable(reply.error_message));
        }
        codec::deserialize_serde(&reply.bincode_payload).map_err(Self::unreachable)
    }

    async fn transfer_leader(
        &mut self,
        rpc: TransferLeaderRequest<MetaDbTypeConfig>,
        _option: RPCOption,
    ) -> Result<TransferLeaderResponse<MetaDbTypeConfig>, RPCError<MetaDbTypeConfig>> {
        self.pool
            .gate_before_rpc(
                &self.target_addr,
                self.shard_id,
                RpcWireKind::TransferLeader,
            )
            .await
            .map_err(Self::unreachable)?;
        let channel = self
            .pool
            .connect(&self.target_addr)
            .await
            .map_err(Self::unreachable)?;
        let mut client = RaftTransportServiceClient::new(channel);
        let bincode_payload = codec::serialize_serde(&rpc).map_err(Self::unreachable)?;
        let reply = client
            .transfer_leader(
                self.pool
                    .authenticate(Request::new(RaftRpcEnvelope {
                        shard_id: self.shard_id,
                        bincode_payload,
                    }))
                    .map_err(Self::unreachable)?,
            )
            .await
            .map_err(Self::unreachable)?
            .into_inner();
        self.pool
            .gate_after_rpc(
                &self.target_addr,
                self.shard_id,
                RpcWireKind::TransferLeader,
            )
            .map_err(Self::unreachable)?;
        if !reply.error_message.is_empty() {
            return Err(Self::unreachable(reply.error_message));
        }
        codec::deserialize_serde(&reply.bincode_payload).map_err(Self::unreachable)
    }

    async fn full_snapshot(
        &mut self,
        vote: VoteOf<MetaDbTypeConfig>,
        snapshot: SnapshotOf<MetaDbTypeConfig, Self::SnapshotData>,
        _cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _option: RPCOption,
    ) -> Result<SnapshotResponse<MetaDbTypeConfig>, StreamingError<MetaDbTypeConfig>> {
        self.pool
            .gate_before_rpc(
                &self.target_addr,
                self.shard_id,
                RpcWireKind::InstallSnapshot,
            )
            .await
            .map_err(Self::streaming_unreachable)?;
        let ch = self
            .pool
            .connect(&self.target_addr)
            .await
            .map_err(Self::streaming_unreachable)?;
        let mut client = RaftTransportServiceClient::new(ch);

        let mut vote_bincode =
            codec::serialize_serde(&vote).map_err(Self::streaming_unreachable)?;
        let mut meta_bincode =
            codec::serialize_serde(&snapshot.meta).map_err(Self::streaming_unreachable)?;
        let archive_bytes =
            codec::serialize(&snapshot.snapshot).map_err(Self::streaming_unreachable)?;

        let shard_id = self.shard_id;
        let total = archive_bytes.len();
        // Allocate each chunk when tonic polls the stream. Explicit chunk
        // storage is bounded by the active chunk.
        let offsets = (0..total.max(1)).step_by(SNAPSHOT_STREAM_CHUNK_BYTES);
        let out_stream = tokio_stream::iter(offsets.map(move |off| {
            let end = off.saturating_add(SNAPSHOT_STREAM_CHUNK_BYTES).min(total);
            SnapshotChunkRequest {
                shard_id,
                vote_bincode: std::mem::take(&mut vote_bincode),
                snapshot_meta_bincode: std::mem::take(&mut meta_bincode),
                relative_file_path: if total == 0 {
                    String::new()
                } else {
                    "archive.bin".to_string()
                },
                offset: off as u64,
                data: archive_bytes[off..end].to_vec(),
                done: end == total,
            }
        }));
        let reply = client
            .install_snapshot(
                self.pool
                    .authenticate(Request::new(out_stream))
                    .map_err(Self::streaming_unreachable)?,
            )
            .await
            .map_err(Self::streaming_unreachable)?
            .into_inner();

        if !reply.error_message.is_empty() {
            return Err(Self::streaming_unreachable(reply.error_message));
        }

        codec::deserialize_serde(&reply.bincode_payload).map_err(Self::streaming_unreachable)
    }
}

/// `tonic` gRPC server dispatching `AppendEntries`, `Vote`, and streaming `InstallSnapshot`
/// to the target `ShardId`'s `MetaDbRaft` instance.
#[derive(Clone)]
pub struct MultiRaftTransportServer {
    router: MultiRaftRouter,
}

impl MultiRaftTransportServer {
    pub fn new(router: MultiRaftRouter) -> Self {
        Self { router }
    }
}

#[tonic::async_trait]
impl RaftTransportService for MultiRaftTransportServer {
    async fn append_entries(
        &self,
        request: Request<RaftRpcEnvelope>,
    ) -> Result<Response<RaftRpcReply>, Status> {
        let env = request.into_inner();
        let Some(raft) = self.router.get_group(env.shard_id) else {
            return Ok(Response::new(RaftRpcReply {
                bincode_payload: Vec::new(),
                error_message: format!("unknown shard {}", env.shard_id),
            }));
        };

        let req: AppendEntriesRequest<MetaDbTypeConfig> =
            codec::deserialize_serde(&env.bincode_payload)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

        match raft.append_entries(req).await {
            Ok(res) => {
                let wire = AppendEntriesResponse::<MetaDbTypeConfig>::from(res);
                let bincode_payload =
                    codec::serialize_serde(&wire).map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(RaftRpcReply {
                    bincode_payload,
                    error_message: String::new(),
                }))
            }
            Err(err) => Ok(Response::new(RaftRpcReply {
                bincode_payload: Vec::new(),
                error_message: err.to_string(),
            })),
        }
    }

    async fn vote(
        &self,
        request: Request<RaftRpcEnvelope>,
    ) -> Result<Response<RaftRpcReply>, Status> {
        let env = request.into_inner();
        let Some(raft) = self.router.get_group(env.shard_id) else {
            return Ok(Response::new(RaftRpcReply {
                bincode_payload: Vec::new(),
                error_message: format!("unknown shard {}", env.shard_id),
            }));
        };

        let req: VoteRequest<MetaDbTypeConfig> = codec::deserialize_serde(&env.bincode_payload)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        match raft.vote(req).await {
            Ok(res) => {
                let bincode_payload =
                    codec::serialize_serde(&res).map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(RaftRpcReply {
                    bincode_payload,
                    error_message: String::new(),
                }))
            }
            Err(err) => Ok(Response::new(RaftRpcReply {
                bincode_payload: Vec::new(),
                error_message: err.to_string(),
            })),
        }
    }

    /// Same bearer interceptor as `Vote` and `AppendEntries`. This handler does not
    /// decide to transfer. `consider_automatic_leadership_transfer` does not call
    /// `trigger().transfer_leader`.
    async fn transfer_leader(
        &self,
        request: Request<RaftRpcEnvelope>,
    ) -> Result<Response<RaftRpcReply>, Status> {
        let env = request.into_inner();
        let Some(raft) = self.router.get_group(env.shard_id) else {
            return Ok(Response::new(RaftRpcReply {
                bincode_payload: Vec::new(),
                error_message: format!("unknown shard {}", env.shard_id),
            }));
        };
        let req: TransferLeaderRequest<MetaDbTypeConfig> =
            codec::deserialize_serde(&env.bincode_payload)
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        match raft.handle_transfer_leader(req).await {
            Ok(response) => {
                let bincode_payload = codec::serialize_serde(&response)
                    .map_err(|error| Status::internal(error.to_string()))?;
                Ok(Response::new(RaftRpcReply {
                    bincode_payload,
                    error_message: String::new(),
                }))
            }
            Err(error) => Ok(Response::new(RaftRpcReply {
                bincode_payload: Vec::new(),
                error_message: error.to_string(),
            })),
        }
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunkRequest>>,
    ) -> Result<Response<SnapshotChunkResponse>, Status> {
        let mut stream = request.into_inner();
        let first = stream
            .next()
            .await
            .ok_or_else(|| Status::invalid_argument("empty snapshot stream"))??;
        if first.vote_bincode.is_empty() || first.snapshot_meta_bincode.is_empty() {
            return Err(Status::invalid_argument(
                "snapshot first chunk must contain vote and metadata",
            ));
        }
        if first.offset != 0 {
            return Err(Status::invalid_argument(
                "snapshot first chunk offset must be zero",
            ));
        }
        let shard_id = first.shard_id;
        let vote_bytes = first.vote_bincode;
        let meta_bytes = first.snapshot_meta_bincode;
        let mut archive_buf = first.data;
        let mut done = first.done;

        while !done {
            let chunk = stream
                .next()
                .await
                .ok_or_else(|| Status::invalid_argument("snapshot stream ended before done"))??;
            if chunk.shard_id != shard_id {
                return Err(Status::invalid_argument(
                    "snapshot shard changed within stream",
                ));
            }
            if !chunk.vote_bincode.is_empty() || !chunk.snapshot_meta_bincode.is_empty() {
                return Err(Status::invalid_argument(
                    "snapshot headers must occur only in the first chunk",
                ));
            }
            if chunk.offset != archive_buf.len() as u64 {
                return Err(Status::invalid_argument(
                    "snapshot chunk offset is not contiguous",
                ));
            }
            archive_buf.extend_from_slice(&chunk.data);
            done = chunk.done;
        }

        let Some(raft) = self.router.get_group(shard_id) else {
            return Ok(Response::new(SnapshotChunkResponse {
                bincode_payload: Vec::new(),
                error_message: format!("unknown shard {}", shard_id),
            }));
        };

        let vote: VoteOf<MetaDbTypeConfig> = codec::deserialize_serde(&vote_bytes)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let meta: SnapshotMetaOf<MetaDbTypeConfig> = codec::deserialize_serde(&meta_bytes)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let archive: SnapshotArchive = codec::deserialize(&archive_buf)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let snap = Snapshot {
            meta,
            snapshot: archive,
        };
        match raft.install_full_snapshot(vote, snap).await {
            Ok(resp) => {
                let bincode_payload =
                    codec::serialize_serde(&resp).map_err(|e| Status::internal(e.to_string()))?;
                Ok(Response::new(SnapshotChunkResponse {
                    bincode_payload,
                    error_message: String::new(),
                }))
            }
            Err(err) => Ok(Response::new(SnapshotChunkResponse {
                bincode_payload: Vec::new(),
                error_message: err.to_string(),
            })),
        }
    }

    async fn client_write(
        &self,
        request: Request<RaftRpcEnvelope>,
    ) -> Result<Response<RaftRpcReply>, Status> {
        let env = request.into_inner();
        let Some(raft) = self.router.get_group(env.shard_id) else {
            return Ok(Response::new(RaftRpcReply {
                bincode_payload: Vec::new(),
                error_message: format!("unknown shard {}", env.shard_id),
            }));
        };
        let req: MetaDbRaftRequest = match codec::deserialize(&env.bincode_payload) {
            Ok(req) => req,
            Err(error) => {
                return Ok(Response::new(RaftRpcReply {
                    bincode_payload: Vec::new(),
                    error_message: error.to_string(),
                }))
            }
        };
        let result = if let Some(proposer) = self.router.get_local_proposer(env.shard_id) {
            proposer
                .propose_local_forwarded(req.expected_epoch, req.hlc, req.payload)
                .await
        } else {
            match raft.client_write(req).await {
                Ok(response) => {
                    if let Err(error) = response.data.result.clone() {
                        Err(error)
                    } else {
                        Ok(response.data)
                    }
                }
                Err(error) => Err(crate::error_map::map_client_write_error(
                    env.shard_id,
                    error,
                )),
            }
        };
        match codec::serialize(&result) {
            Ok(bincode_payload) => Ok(Response::new(RaftRpcReply {
                bincode_payload,
                error_message: String::new(),
            })),
            Err(error) => Ok(Response::new(RaftRpcReply {
                bincode_payload: Vec::new(),
                error_message: error.to_string(),
            })),
        }
    }

    async fn probe_transfer_leader(
        &self,
        request: Request<ProbeTransferLeaderRequest>,
    ) -> Result<Response<ProbeTransferLeaderResponse>, Status> {
        let req = request.into_inner();
        let Some(raft) = self.router.get_group(req.shard_id) else {
            return Ok(Response::new(ProbeTransferLeaderResponse {
                eligible: false,
                last_applied_index: 0,
                reason: "unknown_shard".to_string(),
            }));
        };
        let metrics_rx = raft.metrics();
        let metrics = metrics_rx.borrow_watched().clone();
        if metrics.running_state.is_err() {
            return Ok(Response::new(ProbeTransferLeaderResponse {
                eligible: false,
                last_applied_index: 0,
                reason: "raft_stopped".to_string(),
            }));
        }
        let last_applied_index = metrics.last_applied.map(|id| id.index).unwrap_or(0);
        if req.target_node_id != 0 && metrics.id != req.target_node_id {
            return Ok(Response::new(ProbeTransferLeaderResponse {
                eligible: false,
                last_applied_index,
                reason: "node_id_mismatch".to_string(),
            }));
        }
        let is_voter = metrics
            .membership_config
            .membership()
            .voter_ids()
            .any(|id| id == metrics.id);
        if !is_voter {
            return Ok(Response::new(ProbeTransferLeaderResponse {
                eligible: false,
                last_applied_index,
                reason: "not_voter".to_string(),
            }));
        }
        if last_applied_index < req.min_matched_index {
            return Ok(Response::new(ProbeTransferLeaderResponse {
                eligible: false,
                last_applied_index,
                reason: "follower_lagging".to_string(),
            }));
        }
        Ok(Response::new(ProbeTransferLeaderResponse {
            eligible: true,
            last_applied_index,
            reason: "ready".to_string(),
        }))
    }
}

/// Deterministic schedule for FoundationDB's `Swizzle-Clog` network fault pattern:
/// clogs a subset of directed `(from, to)` links in permutation `clog_order`, then heals
/// those exact links in an independent permutation `unclog_order`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SwizzleClogPlan {
    pub clog_order: Vec<(String, String)>,
    pub unclog_order: Vec<(String, String)>,
}

/// Seeded multi-node network fault controller supporting:
/// - Symmetric & asymmetric directed link partitions (`clog_link` / `unclog_link`),
/// - Full node isolation (`isolate_node`),
/// - FoundationDB `Swizzle-Clog` schedules (`plan_swizzle_clog`),
/// - Bounded per-RPC random micro-jitter (`set_max_jitter_ms`),
/// - Post-apply reply drops (`set_drop_reply_permille`) testing idempotent proposal-receipt deduplication.
pub struct SwizzleClogController {
    rng_state: Mutex<u64>,
    clogged_links: RwLock<HashSet<(String, String)>>,
    isolated_nodes: RwLock<HashSet<String>>,
    max_jitter_ms: AtomicU64,
    drop_reply_permille: AtomicU64,
    dropped_before_send: AtomicU64,
    delayed_packets: AtomicU64,
    dropped_replies: AtomicU64,
}

impl SwizzleClogController {
    pub fn new(seed: u64) -> Arc<Self> {
        Arc::new(Self {
            rng_state: Mutex::new(seed ^ 0x9E37_79B9_7F4A_7C15),
            clogged_links: RwLock::new(HashSet::new()),
            isolated_nodes: RwLock::new(HashSet::new()),
            max_jitter_ms: AtomicU64::new(0),
            drop_reply_permille: AtomicU64::new(0),
            dropped_before_send: AtomicU64::new(0),
            delayed_packets: AtomicU64::new(0),
            dropped_replies: AtomicU64::new(0),
        })
    }

    fn next_rand(&self) -> u64 {
        let mut guard = self.rng_state.lock().unwrap();
        *guard = metadb_core::buggify::splitmix64(*guard);
        *guard
    }

    pub fn set_max_jitter_ms(&self, max_ms: u64) {
        self.max_jitter_ms.store(max_ms, Ordering::Release);
    }

    pub fn set_drop_reply_permille(&self, permille: u64) {
        self.drop_reply_permille
            .store(permille.min(1000), Ordering::Release);
    }

    pub fn clog_link(&self, from_addr: impl Into<String>, to_addr: impl Into<String>) {
        self.clogged_links
            .write()
            .unwrap()
            .insert((from_addr.into(), to_addr.into()));
    }

    pub fn unclog_link(&self, from_addr: &str, to_addr: &str) {
        self.clogged_links
            .write()
            .unwrap()
            .remove(&(from_addr.to_string(), to_addr.to_string()));
    }

    pub fn isolate_node(&self, addr: impl Into<String>) {
        self.isolated_nodes.write().unwrap().insert(addr.into());
    }

    pub fn heal_node(&self, addr: &str) {
        self.isolated_nodes.write().unwrap().remove(addr);
    }

    pub fn heal_all(&self) {
        self.clogged_links.write().unwrap().clear();
        self.isolated_nodes.write().unwrap().clear();
        self.max_jitter_ms.store(0, Ordering::Release);
        self.drop_reply_permille.store(0, Ordering::Release);
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        (
            self.dropped_before_send.load(Ordering::Acquire),
            self.delayed_packets.load(Ordering::Acquire),
            self.dropped_replies.load(Ordering::Acquire),
        )
    }

    /// Generates a pure deterministic FoundationDB `Swizzle-Clog` schedule over `addrs`.
    pub fn plan_swizzle_clog(addrs: &[String], seed: u64, max_links: usize) -> SwizzleClogPlan {
        let mut all_links = Vec::new();
        for a in addrs {
            for b in addrs {
                if a != b {
                    all_links.push((a.clone(), b.clone()));
                }
            }
        }
        if all_links.is_empty() || max_links == 0 {
            return SwizzleClogPlan {
                clog_order: Vec::new(),
                unclog_order: Vec::new(),
            };
        }
        let mut state = seed ^ 0xA5A5_5A5A_F0F0_0F0F;
        // Fisher-Yates shuffle for `clog_order`.
        for i in (1..all_links.len()).rev() {
            state = metadb_core::buggify::splitmix64(state);
            let j = (state as usize) % (i + 1);
            all_links.swap(i, j);
        }
        let count = max_links.min(all_links.len());
        let clog_order: Vec<(String, String)> = all_links.into_iter().take(count).collect();
        let mut unclog_order = clog_order.clone();
        // Independent Fisher-Yates shuffle for `unclog_order` ("swizzle").
        for i in (1..unclog_order.len()).rev() {
            state = metadb_core::buggify::splitmix64(state ^ 0x1234_5678_9ABC_DEF0);
            let j = (state as usize) % (i + 1);
            unclog_order.swap(i, j);
        }
        SwizzleClogPlan {
            clog_order,
            unclog_order,
        }
    }
}

impl NetworkChaosHook for SwizzleClogController {
    fn before_rpc(
        &self,
        from_addr: &str,
        to_addr: &str,
        _shard_id: ShardId,
        _kind: RpcWireKind,
    ) -> WireAction {
        if !from_addr.is_empty() && from_addr != to_addr {
            let isolated = {
                let g = self.isolated_nodes.read().unwrap();
                g.contains(from_addr) || g.contains(to_addr)
            };
            if isolated {
                self.dropped_before_send.fetch_add(1, Ordering::Relaxed);
                return WireAction::DropBeforeSend("simulated node isolation");
            }
            let clogged = self
                .clogged_links
                .read()
                .unwrap()
                .contains(&(from_addr.to_string(), to_addr.to_string()));
            if clogged {
                self.dropped_before_send.fetch_add(1, Ordering::Relaxed);
                return WireAction::DropBeforeSend("simulated swizzle-clogged link");
            }
        }
        let max_jitter = self.max_jitter_ms.load(Ordering::Relaxed);
        if max_jitter > 0 {
            let roll = self.next_rand() % (max_jitter + 1);
            if roll > 0 {
                self.delayed_packets.fetch_add(1, Ordering::Relaxed);
                return WireAction::Delay(Duration::from_millis(roll));
            }
        }
        WireAction::Allow
    }

    fn after_rpc(
        &self,
        from_addr: &str,
        to_addr: &str,
        _shard_id: ShardId,
        kind: RpcWireKind,
    ) -> bool {
        if from_addr == to_addr {
            return false;
        }
        if !matches!(kind, RpcWireKind::ClientWrite | RpcWireKind::AppendEntries) {
            return false;
        }
        let permille = self.drop_reply_permille.load(Ordering::Relaxed);
        if permille == 0 {
            return false;
        }
        let drop_it = (self.next_rand() % 1000) < permille;
        if drop_it {
            self.dropped_replies.fetch_add(1, Ordering::Relaxed);
        }
        drop_it
    }
}

#[cfg(test)]
mod chaos_tests {
    use super::*;

    #[test]
    fn swizzle_clog_plan_is_deterministic_and_preserves_link_set() {
        let addrs = vec![
            "127.0.0.1:7001".to_string(),
            "127.0.0.1:7002".to_string(),
            "127.0.0.1:7003".to_string(),
        ];
        let p1 = SwizzleClogController::plan_swizzle_clog(&addrs, 42, 4);
        let p2 = SwizzleClogController::plan_swizzle_clog(&addrs, 42, 4);
        assert_eq!(p1, p2);
        assert_eq!(p1.clog_order.len(), 4);
        let s1: HashSet<_> = p1.clog_order.iter().cloned().collect();
        let s2: HashSet<_> = p1.unclog_order.iter().cloned().collect();
        assert_eq!(s1, s2);
    }

    #[test]
    fn asymmetric_clog_blocks_one_direction_and_heal_restores() {
        let ctrl = SwizzleClogController::new(99);
        ctrl.clog_link("n1", "n2");
        assert!(matches!(
            ctrl.before_rpc("n1", "n2", 1, RpcWireKind::AppendEntries),
            WireAction::DropBeforeSend(_)
        ));
        assert_eq!(
            ctrl.before_rpc("n2", "n1", 1, RpcWireKind::AppendEntries),
            WireAction::Allow
        );
        ctrl.heal_all();
        assert_eq!(
            ctrl.before_rpc("n1", "n2", 1, RpcWireKind::AppendEntries),
            WireAction::Allow
        );
    }
}
