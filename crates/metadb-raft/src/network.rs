//! Multiplexed `tonic` gRPC transport (`RaftTransportService`) for Multi-Raft shard groups.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::{Arc, RwLock};
use std::time::Duration;

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
use metadb_proto::raft_transport_service_client::RaftTransportServiceClient;
use metadb_proto::raft_transport_service_server::RaftTransportService;
use metadb_proto::{RaftRpcEnvelope, RaftRpcReply, SnapshotChunkRequest, SnapshotChunkResponse};
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

/// Shared gRPC channel pool across Multi-Raft instances on the same host.
#[derive(Clone, Default)]
pub struct ChannelPool {
    channels: Arc<AsyncMutex<HashMap<String, Channel>>>,
    shared_secret: Arc<RwLock<Option<String>>>,
    security: Arc<RwLock<TransportSecurity>>,
    role: Arc<RwLock<Option<metadb_security::RolePresentation>>>,
}

impl ChannelPool {
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
        if !reply.error_message.is_empty() && reply.bincode_payload.is_empty() {
            return Err(MetaDbError::RpcError(reply.error_message));
        }
        codec::deserialize(&reply.bincode_payload)
            .map_err(|error| MetaDbError::CodecError(error.to_string()))?
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
}
