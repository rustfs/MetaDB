//! Forwards a client read to the Raft leader after this process was told who that is.
//!
//! `ForwardRouter` runs the operation on the receiving node and does not forward again.
//! The caller follows a newer `NotLeader` hint, bounded to a few hops.

use std::time::Duration;

use metadb_core::codec;
use metadb_core::{
    FileId, InodeAttr, InodeId, MetaDbError, MetaEntry, QuotaRecord, Result, UserId,
};
use metadb_proto::transaction_service_client::TransactionServiceClient;
use metadb_proto::{ErrorDetailProto, ForwardRouterRequest};
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
    ListDir(Vec<MetaEntry>, Option<String>),
    GetQuota(Option<QuotaRecord>),
    ListQuotas(Vec<QuotaRecord>),
    GetSecondaryIndex(Option<metadb_core::SecondaryIndexRecord>),
    ScanSecondaryIndex(Vec<metadb_core::SecondaryIndexRecord>),
}

#[derive(Clone)]
struct ForwardAuth {
    secret: String,
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

pub async fn forward_op(
    secret: &str,
    addr: &str,
    op: &ForwardedRouterOp,
) -> Result<ForwardedRouterOut> {
    let url = if addr.starts_with("http://") || addr.starts_with("https://") {
        addr.to_string()
    } else {
        format!("http://{addr}")
    };
    let endpoint = Endpoint::from_shared(url.clone())
        .map_err(|error| MetaDbError::RpcError(format!("invalid leader address {url}: {error}")))?
        .connect_timeout(Duration::from_secs(3))
        .timeout(Duration::from_secs(5));
    let channel: Channel = endpoint.connect().await.map_err(|error| {
        MetaDbError::RpcError(format!("connect to leader {url} failed: {error}"))
    })?;
    let mut client = TransactionServiceClient::with_interceptor(
        channel,
        ForwardAuth {
            secret: secret.to_string(),
        },
    );
    let payload =
        codec::serialize(op).map_err(|error| MetaDbError::CodecError(error.to_string()))?;
    let response = client
        .forward_router(ForwardRouterRequest { op: payload })
        .await
        .map_err(|error| MetaDbError::RpcError(format!("forward to leader {url} failed: {error}")))?
        .into_inner();
    if let Some(error) = rpc_error(response.error) {
        return Err(error);
    }
    codec::deserialize(&response.result).map_err(|error| MetaDbError::CodecError(error.to_string()))
}
