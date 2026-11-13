//! Multiplexed `tonic` gRPC transport (`RaftTransportService`) for Multi-Raft shard groups.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::sync::{Arc, RwLock};

use openraft::entry::RaftEntry;
use openraft::errors::{RPCError, ReplicationClosed, StreamingError, Unreachable};
use openraft::network::{RPCOption, RaftNetworkFactory, RaftNetworkV2};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, AppendResult, SnapshotResponse, VoteRequest,
    VoteResponse,
};
use openraft::type_config::alias::{SnapshotMetaOf, SnapshotOf, VoteOf};
use openraft::{BasicNode, OptionalSend, Raft, Snapshot};
use tokio::sync::Mutex as AsyncMutex;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Response, Status, Streaming};

use metadb_core::ShardId;
use metadb_proto::raft_transport_service_client::RaftTransportServiceClient;
use metadb_proto::raft_transport_service_server::RaftTransportService;
use metadb_proto::{RaftRpcEnvelope, RaftRpcReply, SnapshotChunkRequest, SnapshotChunkResponse};

use crate::state_machine::ShardStateMachine;
use crate::types::{MetaDbTypeConfig, SnapshotArchive};

const SNAPSHOT_STREAM_CHUNK_BYTES: usize = 256 * 1024;

pub type MetaDbRaft = Raft<MetaDbTypeConfig, ShardStateMachine>;

/// Per-node registry mapping `ShardId -> MetaDbRaft` for incoming multiplexed Raft RPCs.
#[derive(Clone, Default)]
pub struct MultiRaftRouter {
    groups: Arc<RwLock<HashMap<ShardId, MetaDbRaft>>>,
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
}

/// Shared gRPC channel pool across Multi-Raft instances on the same host.
#[derive(Clone, Default)]
pub struct ChannelPool {
    channels: Arc<AsyncMutex<HashMap<String, Channel>>>,
}

impl ChannelPool {
    pub async fn connect(&self, addr: &str) -> Result<Channel, io::Error> {
        let url = if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{}", addr)
        };
        {
            let guard = self.channels.lock().await;
            if let Some(ch) = guard.get(&url) {
                return Ok(ch.clone());
            }
        }
        let endpoint = Endpoint::from_shared(url.clone())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::ConnectionRefused, e.to_string()))?;
        self.channels.lock().await.insert(url, channel.clone());
        Ok(channel)
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

        let bincode_payload = bincode::serialize(&rpc).map_err(Self::unreachable)?;
        let reply = client
            .append_entries(Request::new(RaftRpcEnvelope {
                shard_id: self.shard_id,
                bincode_payload,
            }))
            .await
            .map_err(Self::unreachable)?
            .into_inner();

        if !reply.error_message.is_empty() {
            return Err(Self::unreachable(reply.error_message));
        }

        let wire_resp: AppendEntriesResponse<MetaDbTypeConfig> =
            bincode::deserialize(&reply.bincode_payload).map_err(Self::unreachable)?;
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

        let bincode_payload = bincode::serialize(&rpc).map_err(Self::unreachable)?;
        let reply = client
            .vote(Request::new(RaftRpcEnvelope {
                shard_id: self.shard_id,
                bincode_payload,
            }))
            .await
            .map_err(Self::unreachable)?
            .into_inner();

        if !reply.error_message.is_empty() {
            return Err(Self::unreachable(reply.error_message));
        }
        bincode::deserialize(&reply.bincode_payload).map_err(Self::unreachable)
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

        let vote_bincode = bincode::serialize(&vote).map_err(Self::streaming_unreachable)?;
        let meta_bincode =
            bincode::serialize(&snapshot.meta).map_err(Self::streaming_unreachable)?;
        let archive_bytes =
            bincode::serialize(&snapshot.snapshot).map_err(Self::streaming_unreachable)?;

        let mut chunks: Vec<SnapshotChunkRequest> = Vec::new();
        if archive_bytes.is_empty() {
            chunks.push(SnapshotChunkRequest {
                shard_id: self.shard_id,
                vote_bincode,
                snapshot_meta_bincode: meta_bincode,
                relative_file_path: String::new(),
                offset: 0,
                data: Vec::new(),
                done: true,
            });
        } else {
            let total = archive_bytes.len();
            let mut off = 0usize;
            while off < total {
                let end = (off + SNAPSHOT_STREAM_CHUNK_BYTES).min(total);
                let is_done = end == total;
                chunks.push(SnapshotChunkRequest {
                    shard_id: self.shard_id,
                    vote_bincode: if off == 0 {
                        vote_bincode.clone()
                    } else {
                        Vec::new()
                    },
                    snapshot_meta_bincode: if off == 0 {
                        meta_bincode.clone()
                    } else {
                        Vec::new()
                    },
                    relative_file_path: "archive.bin".to_string(),
                    offset: off as u64,
                    data: archive_bytes[off..end].to_vec(),
                    done: is_done,
                });
                off = end;
            }
        }

        let out_stream = tokio_stream::iter(chunks);
        let reply = client
            .install_snapshot(Request::new(out_stream))
            .await
            .map_err(Self::streaming_unreachable)?
            .into_inner();

        if !reply.error_message.is_empty() {
            return Err(Self::streaming_unreachable(reply.error_message));
        }

        bincode::deserialize(&reply.bincode_payload).map_err(Self::streaming_unreachable)
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
            bincode::deserialize(&env.bincode_payload)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;

        match raft.append_entries(req).await {
            Ok(res) => {
                let wire = AppendEntriesResponse::<MetaDbTypeConfig>::from(res);
                let bincode_payload =
                    bincode::serialize(&wire).map_err(|e| Status::internal(e.to_string()))?;
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

        let req: VoteRequest<MetaDbTypeConfig> = bincode::deserialize(&env.bincode_payload)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        match raft.vote(req).await {
            Ok(res) => {
                let bincode_payload =
                    bincode::serialize(&res).map_err(|e| Status::internal(e.to_string()))?;
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

    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunkRequest>>,
    ) -> Result<Response<SnapshotChunkResponse>, Status> {
        let mut stream = request.into_inner();
        let mut shard_id = 0u64;
        let mut vote_bytes = Vec::new();
        let mut meta_bytes = Vec::new();
        let mut archive_buf = Vec::new();

        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            shard_id = chunk.shard_id;
            if !chunk.vote_bincode.is_empty() {
                vote_bytes = chunk.vote_bincode;
            }
            if !chunk.snapshot_meta_bincode.is_empty() {
                meta_bytes = chunk.snapshot_meta_bincode;
            }
            archive_buf.extend_from_slice(&chunk.data);
            if chunk.done {
                break;
            }
        }

        let Some(raft) = self.router.get_group(shard_id) else {
            return Ok(Response::new(SnapshotChunkResponse {
                bincode_payload: Vec::new(),
                error_message: format!("unknown shard {}", shard_id),
            }));
        };

        let vote: VoteOf<MetaDbTypeConfig> = bincode::deserialize(&vote_bytes)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let meta: SnapshotMetaOf<MetaDbTypeConfig> = bincode::deserialize(&meta_bytes)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let archive: SnapshotArchive = bincode::deserialize(&archive_buf)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        let snap = Snapshot {
            meta,
            snapshot: archive,
        };
        match raft.install_full_snapshot(vote, snap).await {
            Ok(resp) => {
                let bincode_payload =
                    bincode::serialize(&resp).map_err(|e| Status::internal(e.to_string()))?;
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
}
