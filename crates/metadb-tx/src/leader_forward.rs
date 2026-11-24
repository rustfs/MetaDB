//! Forwards a client read to the Raft leader after this process was told who that is.
//!
//! `ForwardRouter` runs the operation on the receiving node and does not forward again.
//! The caller follows a newer `NotLeader` hint, bounded to a few hops.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

use metadb_core::codec;
use metadb_core::{
    FileId, InodeAttr, InodeId, MetaDbError, MetaEntry, QuotaRecord, Result, UserId,
};
use metadb_proto::transaction_service_client::TransactionServiceClient;
use metadb_proto::{ErrorDetailProto, ForwardRouterRequest};
use metadb_security::TransportSecurity;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub enum ForwardedRouterOp {
    Lookup {
        pid: InodeId,
        name: String,
    },
    DirStat {
        pid: InodeId,
    },
    ListDir {
        pid: InodeId,
        after_name: Option<String>,
        name_prefix: Option<String>,
        limit: usize,
    },
    GetQuota {
        uid: UserId,
    },
    ListQuotas {
        start_uid: UserId,
        limit: usize,
    },
    GetSecondaryIndex {
        fid: FileId,
    },
    ScanSecondaryIndex {
        after_fid: FileId,
        limit: usize,
    },
    /// One shard's directory attribute. The handler does not read other shards.
    ShardPointDirStat {
        shard_id: u64,
        epoch: metadb_core::ShardEpoch,
        pid: InodeId,
    },
    /// Quorum timestamp from one shard, used to build a shared dirstat barrier.
    ShardAppliedHlc {
        shard_id: u64,
        epoch: metadb_core::ShardEpoch,
    },
    /// One shard's reconciled contribution at a shared barrier.
    ShardBarrierStat {
        shard_id: u64,
        epoch: metadb_core::ShardEpoch,
        pid: InodeId,
        barrier: metadb_core::HlcTimestamp,
    },
    ShardMetaScan {
        shard_id: u64,
        epoch: metadb_core::ShardEpoch,
        start: Vec<u8>,
        end: Vec<u8>,
        name_prefix: Option<String>,
        limit: usize,
    },
    ShardQuotaScan {
        shard_id: u64,
        epoch: metadb_core::ShardEpoch,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    },
    ShardIndexScan {
        shard_id: u64,
        epoch: metadb_core::ShardEpoch,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: usize,
    },
    /// One shard's directory partition, anchor or delta, read on that shard's leader.
    ShardDirPartition {
        shard_id: u64,
        epoch: metadb_core::ShardEpoch,
        pid: InodeId,
        barrier: Option<metadb_core::HlcTimestamp>,
    },
}

#[derive(
    Debug,
    Clone,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub enum ForwardedRouterOut {
    Lookup(Option<MetaEntry>),
    DirStat(InodeAttr),
    ListDir(Vec<MetaEntry>, Option<String>, u64, u64),
    GetQuota(Option<QuotaRecord>),
    ListQuotas(Vec<QuotaRecord>),
    GetSecondaryIndex(Option<metadb_core::SecondaryIndexRecord>),
    ScanSecondaryIndex(Vec<metadb_core::SecondaryIndexRecord>),
    AppliedHlc(u64),
    BarrierStat {
        anchor: Option<InodeAttr>,
        children: i64,
        bytes: i64,
        mtime: u64,
    },
    DirPartition {
        is_anchor: bool,
        attr: Option<InodeAttr>,
        max_applied_hlc: u64,
        anchor_missing: bool,
    },
}

#[derive(Clone)]
struct ForwardAuth {
    secret: String,
    role: Option<metadb_security::RolePresentation>,
}

impl Interceptor for ForwardAuth {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        if !self.secret.is_empty() {
            let mut token = String::from("Bearer ");
            token.push_str(&self.secret);
            let mut value = tonic::metadata::MetadataValue::try_from(token).map_err(|_| {
                Status::internal("shared secret is not a valid authorization header value")
            })?;
            value.set_sensitive(true);
            request.metadata_mut().insert("authorization", value);
        }
        if let Some(role) = &self.role {
            role.apply_metadata(request.metadata_mut())
                .map_err(|_| Status::internal("RBAC principal headers could not be attached"))?;
        }
        request.metadata_mut().insert(
            "x-metadb-forwarded",
            tonic::metadata::MetadataValue::from_static("1"),
        );
        Ok(request)
    }
}

fn rpc_error(detail: Option<ErrorDetailProto>) -> Option<MetaDbError> {
    detail.map(MetaDbError::from).filter(|error| match error {
        MetaDbError::RpcError(message) => !message.is_empty(),
        _ => true,
    })
}

fn forward_channel_pool() -> &'static tokio::sync::Mutex<HashMap<String, Channel>> {
    static POOL: OnceLock<tokio::sync::Mutex<HashMap<String, Channel>>> = OnceLock::new();
    POOL.get_or_init(|| tokio::sync::Mutex::new(HashMap::new()))
}

pub async fn forward_op(
    secret: &str,
    addr: &str,
    op: &ForwardedRouterOp,
    deadline: tokio::time::Instant,
    security: &TransportSecurity,
    role: Option<&metadb_security::RolePresentation>,
) -> Result<ForwardedRouterOut> {
    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
    if remaining.is_zero() {
        return Err(MetaDbError::RpcError(
            "forwarded read exceeded the routing deadline".to_string(),
        ));
    }
    let url = security.prepare_url(addr).map_err(|error| {
        MetaDbError::RpcError(format!("invalid leader address {addr}: {error}"))
    })?;
    let cached = forward_channel_pool().lock().await.get(&url).cloned();
    let channel: Channel = if let Some(ch) = cached {
        ch
    } else {
        let endpoint = Endpoint::from_shared(url.clone())
            .map_err(|error| {
                MetaDbError::RpcError(format!("invalid leader address {url}: {error}"))
            })?
            .connect_timeout(remaining.min(Duration::from_secs(3)));
        let connected = match tokio::time::timeout(remaining, security.connect(endpoint)).await {
            Ok(channel) => channel.map_err(|error| {
                MetaDbError::RpcError(format!("connect to leader {url} failed: {error}"))
            })?,
            Err(_) => {
                return Err(MetaDbError::RpcError(
                    "forwarded read connect timed out".to_string(),
                ))
            }
        };
        forward_channel_pool()
            .lock()
            .await
            .insert(url.clone(), connected.clone());
        connected
    };
    let mut client = TransactionServiceClient::with_interceptor(
        channel,
        ForwardAuth {
            secret: secret.to_string(),
            role: role.cloned(),
        },
    );
    let payload =
        codec::serialize(op).map_err(|error| MetaDbError::CodecError(error.to_string()))?;
    let rpc_budget = deadline.saturating_duration_since(tokio::time::Instant::now());
    if rpc_budget.is_zero() {
        return Err(MetaDbError::RpcError(
            "forwarded read exceeded the routing deadline".to_string(),
        ));
    }
    let response = match tokio::time::timeout(
        rpc_budget,
        client.forward_router(ForwardRouterRequest { op: payload }),
    )
    .await
    {
        Ok(Ok(resp)) => resp.into_inner(),
        Ok(Err(error)) => {
            forward_channel_pool().lock().await.remove(&url);
            return Err(MetaDbError::RpcError(format!(
                "forward to leader {url} failed: {error}"
            )));
        }
        Err(_) => {
            return Err(MetaDbError::RpcError(
                "forwarded read timed out".to_string(),
            ));
        }
    };
    if let Some(error) = rpc_error(response.error) {
        return Err(error);
    }
    codec::deserialize(&response.result).map_err(|error| MetaDbError::CodecError(error.to_string()))
}
