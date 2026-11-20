//! `tonic` gRPC service implementations (`MetadataService`, `TransactionService`,
//! and `ControlPlaneService`) backed by `PersistentMetaDbCluster`.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use tonic::{Request, Response, Status};

use metadb_cluster::{
    AllocIdKind, CompositeShardRoute, DurableGlobalManager, RemotePrecheck, RemotePublication,
    ShardHealthState, ShardSplitMode,
};
use metadb_core::codec;
use metadb_core::{
    AuxOutboxEntry, CompositeKeyRange, HlcTimestamp, MetaDbError, ShardEpoch, TxnState,
    WriteIntentKey, WriteIntentRecord,
};
use metadb_proto::control_plane_service_server::ControlPlaneService;
use metadb_proto::metadata_service_server::MetadataService;
use metadb_proto::sql_param_value;
use metadb_proto::transaction_service_server::TransactionService;
use metadb_proto::*;
use metadb_proxy::{ProxySqlResult, SqlParam, StatelessProxyRouter};
use metadb_tx::{DistributedTxRouter, SetAttrPatch, ShardDirectory, TxRaftCmd, TxRaftOutput};

mod route_conv {
    use super::*;

    pub fn to_proto_route(r: &CompositeShardRoute) -> ShardDescriptorProto {
        let peers = r
            .peers
            .iter()
            .map(|(&node_id, (addr, is_voter))| ReplicaPeerProto {
                node_id,
                grpc_addr: addr.clone(),
                is_voter: *is_voter,
            })
            .collect();
        let (jumbo_isolated, dominant_pid) = match r.health_state {
            ShardHealthState::Normal => (false, r.dual_mode_pid.unwrap_or(0)),
            ShardHealthState::JumboIsolated { dominant_pid, .. } => (true, dominant_pid),
        };
        ShardDescriptorProto {
            cell_id: r.cell_id,
            shard_id: r.shard_id,
            start_key: r.range.start_key.clone(),
            end_key: r.range.end_key.clone(),
            epoch: Some(r.epoch.into()),
            leader_node_id: r.leader_node,
            peers,
            dual_mode_fine_grained: r.split_mode == ShardSplitMode::DualModeFineGrained,
            jumbo_isolated,
            dominant_pid,
        }
    }

    pub fn from_proto_route(
        p: ShardDescriptorProto,
    ) -> std::result::Result<CompositeShardRoute, MetaDbError> {
        let range = CompositeKeyRange::new(p.start_key, p.end_key)?;
        let epoch: ShardEpoch = p.epoch.unwrap_or_default().into();
        let mut peers = BTreeMap::new();
        for peer in p.peers {
            peers.insert(peer.node_id, (peer.grpc_addr, peer.is_voter));
        }
        let split_mode = if p.dual_mode_fine_grained {
            ShardSplitMode::DualModeFineGrained
        } else {
            ShardSplitMode::DirectoryColocated
        };
        let health_state = if p.jumbo_isolated {
            ShardHealthState::JumboIsolated {
                dedicated_node: p.leader_node_id,
                dominant_pid: p.dominant_pid,
            }
        } else {
            ShardHealthState::Normal
        };
        Ok(CompositeShardRoute {
            cell_id: p.cell_id,
            shard_id: p.shard_id,
            range,
            epoch,
            leader_node: p.leader_node_id,
            peers,
            split_mode,
            health_state,
            dual_mode_pid: if p.dominant_pid > 0 {
                Some(p.dominant_pid)
            } else {
                None
            },
        })
    }
}

pub fn composite_route_from_proto(
    descriptor: ShardDescriptorProto,
) -> std::result::Result<CompositeShardRoute, MetaDbError> {
    route_conv::from_proto_route(descriptor)
}

fn decode_req_txn_id(bytes: &[u8]) -> Result<u128, Status> {
    metadb_proto::convert::decode_u128_be(bytes)
        .map_err(|e| Status::invalid_argument(e.to_string()))
}

fn decode_sql_params(raw: &[SqlParamValue]) -> std::result::Result<Vec<SqlParam>, MetaDbError> {
    let mut params = Vec::with_capacity(raw.len());
    for p in raw {
        let Some(kind) = &p.kind else {
            return Err(MetaDbError::SqlParseError(
                "SQL parameter is missing a value".to_string(),
            ));
        };
        params.push(match kind {
            sql_param_value::Kind::U64Val(v) => SqlParam::U64(*v),
            sql_param_value::Kind::I64Val(v) => SqlParam::I64(*v),
            sql_param_value::Kind::StrVal(s) => SqlParam::Str(s.clone()),
        });
    }
    Ok(params)
}

fn decode_req_optional_uuid(bytes: &[u8]) -> Result<u128, Status> {
    metadb_proto::convert::decode_optional_u128_be(bytes)
        .map_err(|e| Status::invalid_argument(e.to_string()))
}

#[derive(Clone)]
pub struct MetaDbGrpcService {
    global_manager: Arc<DurableGlobalManager>,
    tx_router: Arc<DistributedTxRouter>,
    proxy_router: Arc<StatelessProxyRouter>,
    /// Bearer sent when this process probes another cell. Empty means the
    /// process was allowed to start without a secret, so probes stay on loopback.
    forward_secret: String,
    /// Outbound control probes use this policy. Plaintext is the only mode that loads.
    /// A certificate principal is not a role; method RBAC is not installed.
    transport_security: std::sync::Arc<metadb_security::TransportSecurity>,
    role: Option<metadb_security::RolePresentation>,
    /// `global-manager` publishes routes and does not mint ids or rename leases.
    directory_only: bool,
    /// `cell-manager` keeps its own cell and refuses to store a remote cell.
    accepts_remote_cells: bool,
}

impl MetaDbGrpcService {
    pub fn new(
        global_manager: Arc<DurableGlobalManager>,
        tx_router: Arc<DistributedTxRouter>,
        proxy_router: Arc<StatelessProxyRouter>,
    ) -> Self {
        Self {
            global_manager,
            tx_router,
            proxy_router,
            forward_secret: String::new(),
            transport_security: std::sync::Arc::new(metadb_security::TransportSecurity::plaintext()),
            role: None,
            directory_only: false,
            accepts_remote_cells: true,
        }
    }

    pub fn with_forward_secret(mut self, secret: String) -> Self {
        self.forward_secret = secret;
        self
    }

    pub fn with_transport_security(
        mut self,
        security: std::sync::Arc<metadb_security::TransportSecurity>,
    ) -> Self {
        self.transport_security = security;
        self
    }

    pub fn with_role_presentation(
        mut self,
        role: Option<metadb_security::RolePresentation>,
    ) -> Self {
        self.role = role;
        self
    }

    pub fn with_directory_only(mut self, directory_only: bool) -> Self {
        self.directory_only = directory_only;
        self
    }

    pub fn with_remote_cell_registration(mut self, accepts_remote_cells: bool) -> Self {
        self.accepts_remote_cells = accepts_remote_cells;
        self
    }

    fn directory_only_error() -> MetaDbError {
        MetaDbError::ShardRoutingError(
            "global-manager does not allocate ids or rename leases".to_string(),
        )
    }

    fn server_hlc(&self) -> u64 {
        self.tx_router.clock().now().0
    }

    fn observe_caller_hlc(&self, caller_hlc: u64) -> std::result::Result<(), MetaDbError> {
        if caller_hlc > 0 {
            let _ = self
                .tx_router
                .clock()
                .update_checked(HlcTimestamp(caller_hlc))?;
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl MetadataService for MetaDbGrpcService {
    async fn create_file(
        &self,
        request: Request<CreateFileRequest>,
    ) -> Result<Response<CreateFileResponse>, Status> {
        let req = request.into_inner();
        let caller_hlc = req.header.as_ref().map(|h| h.caller_hlc).unwrap_or(0);
        if let Err(e) = self.observe_caller_hlc(caller_hlc) {
            return Ok(Response::new(CreateFileResponse {
                error: Some(ErrorDetailProto::from(&e)),
                entry: None,
                server_hlc: self.server_hlc(),
            }));
        }
        match self
            .tx_router
            .create_file(
                req.pid,
                &req.name,
                req.inode_id,
                req.fid,
                req.uid,
                req.mode,
                req.size_bytes,
            )
            .await
        {
            Ok(entry) => Ok(Response::new(CreateFileResponse {
                error: None,
                entry: Some(MetaEntryProto::from(&entry)),
                server_hlc: self.server_hlc(),
            })),
            Err(e) => Ok(Response::new(CreateFileResponse {
                error: Some(ErrorDetailProto::from(&e)),
                entry: None,
                server_hlc: self.server_hlc(),
            })),
        }
    }

    async fn mkdir(
        &self,
        request: Request<MkdirRequest>,
    ) -> Result<Response<MkdirResponse>, Status> {
        let req = request.into_inner();
        let caller_hlc = req.header.as_ref().map(|h| h.caller_hlc).unwrap_or(0);
        if let Err(e) = self.observe_caller_hlc(caller_hlc) {
            return Ok(Response::new(MkdirResponse {
                error: Some(ErrorDetailProto::from(&e)),
                entry: None,
                server_hlc: self.server_hlc(),
            }));
        }
        match self
            .tx_router
            .mkdir(req.pid, &req.name, req.dir_id, req.uid, req.mode)
            .await
        {
            Ok(entry) => Ok(Response::new(MkdirResponse {
                error: None,
                entry: Some(MetaEntryProto::from(&entry)),
                server_hlc: self.server_hlc(),
            })),
            Err(e) => Ok(Response::new(MkdirResponse {
                error: Some(ErrorDetailProto::from(&e)),
                entry: None,
                server_hlc: self.server_hlc(),
            })),
        }
    }

    async fn unlink(
        &self,
        request: Request<UnlinkRequest>,
    ) -> Result<Response<UnlinkResponse>, Status> {
        let req = request.into_inner();
        match self.tx_router.unlink(req.pid, &req.name).await {
            Ok(attr) => Ok(Response::new(UnlinkResponse {
                error: None,
                removed_attr: Some(InodeAttrProto::from(&attr)),
                server_hlc: self.server_hlc(),
            })),
            Err(e) => Ok(Response::new(UnlinkResponse {
                error: Some(ErrorDetailProto::from(&e)),
                removed_attr: None,
                server_hlc: self.server_hlc(),
            })),
        }
    }

    async fn rmdir(
        &self,
        request: Request<RmdirRequest>,
    ) -> Result<Response<RmdirResponse>, Status> {
        let req = request.into_inner();
        match self.tx_router.rmdir(req.pid, &req.name).await {
            Ok(attr) => Ok(Response::new(RmdirResponse {
                error: None,
                removed_attr: Some(InodeAttrProto::from(&attr)),
                server_hlc: self.server_hlc(),
            })),
            Err(e) => Ok(Response::new(RmdirResponse {
                error: Some(ErrorDetailProto::from(&e)),
                removed_attr: None,
                server_hlc: self.server_hlc(),
            })),
        }
    }

    async fn set_attr(
        &self,
        request: Request<SetAttrRequest>,
    ) -> Result<Response<SetAttrResponse>, Status> {
        let req = request.into_inner();
        let patch = SetAttrPatch {
            mode: req.has_mode.then_some(req.mode),
            uid: req.has_uid.then_some(req.uid),
            size_bytes: req.has_size_bytes.then_some(req.size_bytes),
            fid: req.has_fid.then_some(req.fid),
            modify_time: req
                .has_modify_time_hlc
                .then_some(HlcTimestamp(req.modify_time_hlc)),
        };
        match self.tx_router.setattr(req.pid, &req.name, patch).await {
            Ok(entry) => Ok(Response::new(SetAttrResponse {
                error: None,
                entry: Some(MetaEntryProto::from(&entry)),
                server_hlc: self.server_hlc(),
            })),
            Err(e) => Ok(Response::new(SetAttrResponse {
                error: Some(ErrorDetailProto::from(&e)),
                entry: None,
                server_hlc: self.server_hlc(),
            })),
        }
    }

    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let req = request.into_inner();
        match self.tx_router.lookup(req.pid, &req.name).await {
            Ok(Some(entry)) => Ok(Response::new(LookupResponse {
                error: None,
                found: true,
                entry: Some(MetaEntryProto::from(&entry)),
                server_hlc: self.server_hlc(),
            })),
            Ok(None) => Ok(Response::new(LookupResponse {
                error: None,
                found: false,
                entry: None,
                server_hlc: self.server_hlc(),
            })),
            Err(e) => Ok(Response::new(LookupResponse {
                error: Some(ErrorDetailProto::from(&e)),
                found: false,
                entry: None,
                server_hlc: self.server_hlc(),
            })),
        }
    }

    async fn dir_stat(
        &self,
        request: Request<DirStatRequest>,
    ) -> Result<Response<DirStatResponse>, Status> {
        let req = request.into_inner();
        match self.tx_router.dirstat(req.pid).await {
            Ok(attr) => Ok(Response::new(DirStatResponse {
                error: None,
                attr: Some(InodeAttrProto::from(&attr)),
                server_hlc: self.server_hlc(),
            })),
            Err(e) => Ok(Response::new(DirStatResponse {
                error: Some(ErrorDetailProto::from(&e)),
                attr: None,
                server_hlc: self.server_hlc(),
            })),
        }
    }

    async fn get_dir_partition_stat(
        &self,
        request: Request<GetDirPartitionStatRequest>,
    ) -> Result<Response<GetDirPartitionStatResponse>, Status> {
        let req = request.into_inner();
        let Some(hdr) = req.header else {
            return Err(Status::invalid_argument("missing routing header"));
        };
        let epoch: ShardEpoch = hdr.expected_epoch.unwrap_or_default().into();
        let barrier = req
            .use_fixed_barrier_hlc
            .then_some(HlcTimestamp(req.barrier_hlc));
        match self
            .tx_router
            .dir_partition_stat(hdr.shard_id, epoch, req.pid, barrier)
            .await
        {
            Ok(stat) if stat.anchor_missing => {
                let error = MetaDbError::DirectoryNotFound(req.pid);
                Ok(Response::new(GetDirPartitionStatResponse {
                    error: Some(ErrorDetailProto::from(&error)),
                    is_anchor: true,
                    partition_attr: None,
                    max_applied_hlc: stat.max_applied_hlc,
                }))
            }
            Ok(stat) => Ok(Response::new(GetDirPartitionStatResponse {
                error: None,
                is_anchor: stat.is_anchor,
                partition_attr: stat.attr.as_ref().map(InodeAttrProto::from),
                max_applied_hlc: stat.max_applied_hlc,
            })),
            Err(error) => Ok(Response::new(GetDirPartitionStatResponse {
                error: Some(ErrorDetailProto::from(&error)),
                is_anchor: false,
                partition_attr: None,
                max_applied_hlc: 0,
            })),
        }
    }

    async fn list_dir(
        &self,
        request: Request<ListDirRequest>,
    ) -> Result<Response<ListDirResponse>, Status> {
        let req = request.into_inner();
        let after = (!req.after_name.is_empty()).then_some(req.after_name.as_str());
        let pfx = (!req.name_prefix.is_empty()).then_some(req.name_prefix.as_str());
        match self
            .tx_router
            .listdir_paginated(req.pid, after, pfx, req.limit as usize)
            .await
        {
            Ok((entries, next_cursor)) => Ok(Response::new(ListDirResponse {
                error: None,
                entries: entries.iter().map(MetaEntryProto::from).collect(),
                has_more: next_cursor.is_some(),
                next_cursor: next_cursor.unwrap_or_default(),
                server_hlc: self.server_hlc(),
            })),
            Err(e) => Ok(Response::new(ListDirResponse {
                error: Some(ErrorDetailProto::from(&e)),
                entries: vec![],
                next_cursor: String::new(),
                has_more: false,
                server_hlc: self.server_hlc(),
            })),
        }
    }

    async fn execute_sql(
        &self,
        request: Request<ExecuteSqlRequest>,
    ) -> Result<Response<ExecuteSqlResponse>, Status> {
        let req = request.into_inner();
        if let Err(e) = self.observe_caller_hlc(req.caller_hlc) {
            return Ok(Response::new(ExecuteSqlResponse {
                error: Some(ErrorDetailProto::from(&e)),
                ..Default::default()
            }));
        }
        let params = match decode_sql_params(&req.params) {
            Ok(params) => params,
            Err(e) => {
                return Ok(Response::new(ExecuteSqlResponse {
                    error: Some(ErrorDetailProto::from(&e)),
                    ..Default::default()
                }));
            }
        };

        match self.proxy_router.execute_sql(&req.sql, &params).await {
            Ok(res) => {
                let mut resp = ExecuteSqlResponse {
                    server_hlc: self.server_hlc(),
                    ..Default::default()
                };
                match res {
                    ProxySqlResult::Entry(e) => resp.entries.push(MetaEntryProto::from(&e)),
                    ProxySqlResult::OptionalEntry(Some(e)) => {
                        resp.entries.push(MetaEntryProto::from(&e))
                    }
                    ProxySqlResult::OptionalEntry(None) => {}
                    ProxySqlResult::Attr(a) => resp.single_attr = Some(InodeAttrProto::from(&a)),
                    ProxySqlResult::Entries {
                        entries,
                        next_cursor,
                    } => {
                        resp.entries = entries.iter().map(MetaEntryProto::from).collect();
                        resp.has_more = next_cursor.is_some();
                        resp.next_cursor = next_cursor.unwrap_or_default();
                    }
                    ProxySqlResult::Quota(q) => {
                        resp.single_quota = q.as_ref().map(QuotaRecordProto::from);
                    }
                    ProxySqlResult::Quotas(qs) => {
                        resp.quotas = qs.iter().map(QuotaRecordProto::from).collect();
                    }
                    ProxySqlResult::SecondaryIndex(idx) => {
                        resp.single_index = idx.as_ref().map(SecondaryIndexRecordProto::from);
                    }
                    ProxySqlResult::SecondaryIndices(idxs) => {
                        resp.indices = idxs.iter().map(SecondaryIndexRecordProto::from).collect();
                    }
                }
                Ok(Response::new(resp))
            }
            Err(e) => Ok(Response::new(ExecuteSqlResponse {
                error: Some(ErrorDetailProto::from(&e)),
                server_hlc: self.server_hlc(),
                ..Default::default()
            })),
        }
    }

    async fn get_quota(
        &self,
        request: Request<GetQuotaRequest>,
    ) -> Result<Response<GetQuotaResponse>, Status> {
        let req = request.into_inner();
        match self.tx_router.get_quota(req.uid).await {
            Ok(Some(rec)) => Ok(Response::new(GetQuotaResponse {
                error: None,
                found: true,
                record: Some(QuotaRecordProto::from(&rec)),
            })),
            Ok(None) => Ok(Response::new(GetQuotaResponse {
                error: None,
                found: false,
                record: None,
            })),
            Err(e) => Ok(Response::new(GetQuotaResponse {
                error: Some(ErrorDetailProto::from(&e)),
                found: false,
                record: None,
            })),
        }
    }

    async fn set_quota_limit(
        &self,
        request: Request<SetQuotaLimitRequest>,
    ) -> Result<Response<SetQuotaLimitResponse>, Status> {
        let req = request.into_inner();
        match self
            .tx_router
            .set_quota_limit(req.uid, req.quota_limit_bytes)
            .await
        {
            Ok(rec) => Ok(Response::new(SetQuotaLimitResponse {
                error: None,
                record: Some(QuotaRecordProto::from(&rec)),
            })),
            Err(e) => Ok(Response::new(SetQuotaLimitResponse {
                error: Some(ErrorDetailProto::from(&e)),
                record: None,
            })),
        }
    }

    async fn list_quotas(
        &self,
        request: Request<ListQuotasRequest>,
    ) -> Result<Response<ListQuotasResponse>, Status> {
        let req = request.into_inner();
        match self
            .tx_router
            .list_quotas(req.start_uid, req.limit as usize)
            .await
        {
            Ok(recs) => Ok(Response::new(ListQuotasResponse {
                error: None,
                records: recs.iter().map(QuotaRecordProto::from).collect(),
            })),
            Err(e) => Ok(Response::new(ListQuotasResponse {
                error: Some(ErrorDetailProto::from(&e)),
                records: vec![],
            })),
        }
    }

    async fn get_secondary_index(
        &self,
        request: Request<GetSecondaryIndexRequest>,
    ) -> Result<Response<GetSecondaryIndexResponse>, Status> {
        let req = request.into_inner();
        match self.tx_router.get_secondary_index(req.fid).await {
            Ok(Some(rec)) => Ok(Response::new(GetSecondaryIndexResponse {
                error: None,
                found: true,
                record: Some(SecondaryIndexRecordProto::from(&rec)),
            })),
            Ok(None) => Ok(Response::new(GetSecondaryIndexResponse {
                error: None,
                found: false,
                record: None,
            })),
            Err(e) => Ok(Response::new(GetSecondaryIndexResponse {
                error: Some(ErrorDetailProto::from(&e)),
                found: false,
                record: None,
            })),
        }
    }

    async fn scan_secondary_index(
        &self,
        request: Request<ScanSecondaryIndexRequest>,
    ) -> Result<Response<ScanSecondaryIndexResponse>, Status> {
        let req = request.into_inner();
        match self
            .tx_router
            .scan_secondary_index(req.after_fid, req.limit as usize)
            .await
        {
            Ok(recs) => Ok(Response::new(ScanSecondaryIndexResponse {
                error: None,
                records: recs.iter().map(SecondaryIndexRecordProto::from).collect(),
            })),
            Err(e) => Ok(Response::new(ScanSecondaryIndexResponse {
                error: Some(ErrorDetailProto::from(&e)),
                records: vec![],
            })),
        }
    }
}

#[tonic::async_trait]
impl TransactionService for MetaDbGrpcService {
    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<RenameResponse>, Status> {
        let req = request.into_inner();
        if let Err(e) = self.observe_caller_hlc(req.caller_hlc) {
            return Ok(Response::new(RenameResponse {
                error: Some(ErrorDetailProto::from(&e)),
                entry: None,
                commit_hlc: self.server_hlc(),
            }));
        }
        match self
            .tx_router
            .rename(
                req.src_pid,
                &req.src_name,
                req.dst_pid,
                &req.dst_name,
                req.overwrite,
            )
            .await
        {
            Ok(entry) => Ok(Response::new(RenameResponse {
                error: None,
                entry: Some(MetaEntryProto::from(&entry)),
                commit_hlc: entry.attr.modify_time.0,
            })),
            Err(e) => Ok(Response::new(RenameResponse {
                error: Some(ErrorDetailProto::from(&e)),
                entry: None,
                commit_hlc: self.server_hlc(),
            })),
        }
    }

    async fn prepare2pc(
        &self,
        request: Request<Prepare2pcRequest>,
    ) -> Result<Response<Prepare2pcResponse>, Status> {
        let req = request.into_inner();
        let Some(hdr) = req.header else {
            return Err(Status::invalid_argument("missing routing header"));
        };
        let ep: ShardEpoch = hdr.expected_epoch.unwrap_or_default().into();
        let txn_id = decode_req_txn_id(&req.txn_id_be)?;

        if req.encoded_intent_keys.len() != req.encoded_intent_records.len() {
            return Err(Status::invalid_argument(
                "intent keys and records must have equal lengths",
            ));
        }

        let mut intents = Vec::with_capacity(req.encoded_intent_keys.len());
        for (ik, ir) in req
            .encoded_intent_keys
            .iter()
            .zip(req.encoded_intent_records.iter())
        {
            let wk =
                WriteIntentKey::decode(ik).map_err(|e| Status::invalid_argument(e.to_string()))?;
            let wrec = WriteIntentRecord::decode(ir)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            intents.push((wk.target_user_key, wrec.op));
        }

        let cmd = TxRaftCmd::Prepare2pcChecked {
            txn_id,
            is_primary: req.is_primary,
            primary_key: req.primary_key.clone(),
            participant_keys: req.all_participant_keys,
            start_hlc: HlcTimestamp(req.start_hlc),
            expire_hlc: HlcTimestamp(req.expire_hlc),
            rename_fence_token: req.has_rename_fence_token.then_some(req.rename_fence_token),
            intents,
            expected_values: req
                .expected_values
                .into_iter()
                .map(|entry| (entry.user_key, entry.value))
                .collect(),
        };

        // Prepare runs on the participant named by the routing header. `primary_key`
        // identifies the transaction record's home shard, which is a different
        // participant when this call is a secondary prepare.
        let find_res = self
            .global_manager
            .all_cells()
            .into_iter()
            .find_map(|c| c.lookup_local_handle(hdr.shard_id).ok());
        let Some((_, grp, _)) = find_res else {
            let e = MetaDbError::ShardRoutingError(format!("unknown shard {}", hdr.shard_id));
            return Ok(Response::new(Prepare2pcResponse {
                error: Some(ErrorDetailProto::from(&e)),
                prepared_hlc: 0,
            }));
        };
        let payload =
            metadb_tx::command_payload(&cmd).map_err(|e| Status::internal(e.to_string()))?;
        match grp.propose(ep, HlcTimestamp(req.start_hlc), payload).await {
            Ok(resp) => Ok(Response::new(Prepare2pcResponse {
                error: None,
                prepared_hlc: resp.applied_hlc.0,
            })),
            Err(e) => Ok(Response::new(Prepare2pcResponse {
                error: Some(ErrorDetailProto::from(&e)),
                prepared_hlc: 0,
            })),
        }
    }

    async fn commit_primary2pc(
        &self,
        request: Request<CommitPrimary2pcRequest>,
    ) -> Result<Response<CommitPrimary2pcResponse>, Status> {
        let req = request.into_inner();
        let Some(hdr) = req.header else {
            return Err(Status::invalid_argument("missing header"));
        };
        let ep: ShardEpoch = hdr.expected_epoch.unwrap_or_default().into();
        let find_res = self
            .global_manager
            .all_cells()
            .into_iter()
            .find_map(|c| c.lookup_local_handle(hdr.shard_id).ok());
        let Some((_, grp, _)) = find_res else {
            let e = MetaDbError::ShardRoutingError(format!("unknown shard {}", hdr.shard_id));
            return Ok(Response::new(CommitPrimary2pcResponse {
                error: Some(ErrorDetailProto::from(&e)),
                commit_hlc: 0,
            }));
        };
        let cmd = TxRaftCmd::CommitPrimary2pc {
            txn_id: decode_req_txn_id(&req.txn_id_be)?,
            commit_hlc: HlcTimestamp(req.commit_hlc),
            rename_fence_token: req.has_rename_fence_token.then_some(req.rename_fence_token),
        };
        let payload =
            metadb_tx::command_payload(&cmd).map_err(|e| Status::internal(e.to_string()))?;
        match grp.propose(ep, HlcTimestamp(req.commit_hlc), payload).await {
            Ok(resp) => Ok(Response::new(CommitPrimary2pcResponse {
                error: None,
                commit_hlc: resp.applied_hlc.0,
            })),
            Err(e) => Ok(Response::new(CommitPrimary2pcResponse {
                error: Some(ErrorDetailProto::from(&e)),
                commit_hlc: 0,
            })),
        }
    }

    async fn commit_secondary2pc(
        &self,
        request: Request<CommitSecondary2pcRequest>,
    ) -> Result<Response<CommitSecondary2pcResponse>, Status> {
        let req = request.into_inner();
        let Some(hdr) = req.header else {
            return Err(Status::invalid_argument("missing header"));
        };
        let ep: ShardEpoch = hdr.expected_epoch.unwrap_or_default().into();
        let find_res = self
            .global_manager
            .all_cells()
            .into_iter()
            .find_map(|c| c.lookup_local_handle(hdr.shard_id).ok());
        let Some((_, grp, _)) = find_res else {
            let e = MetaDbError::ShardRoutingError(format!("unknown shard {}", hdr.shard_id));
            return Ok(Response::new(CommitSecondary2pcResponse {
                error: Some(ErrorDetailProto::from(&e)),
                already_applied: false,
            }));
        };
        let cmd = TxRaftCmd::CommitSecondary2pc {
            txn_id: decode_req_txn_id(&req.txn_id_be)?,
            commit_hlc: HlcTimestamp(req.commit_hlc),
            target_user_keys: req.target_user_keys,
        };
        let payload =
            metadb_tx::command_payload(&cmd).map_err(|e| Status::internal(e.to_string()))?;
        match grp.propose(ep, HlcTimestamp(req.commit_hlc), payload).await {
            Ok(resp) => {
                let out: TxRaftOutput = codec::deserialize(&resp.result.unwrap_or_default())
                    .unwrap_or(TxRaftOutput::Unit);
                let already_applied = matches!(
                    out,
                    TxRaftOutput::Committed {
                        already_applied: true,
                        ..
                    }
                );
                Ok(Response::new(CommitSecondary2pcResponse {
                    error: None,
                    already_applied,
                }))
            }
            Err(e) => Ok(Response::new(CommitSecondary2pcResponse {
                error: Some(ErrorDetailProto::from(&e)),
                already_applied: false,
            })),
        }
    }

    async fn abort2pc(
        &self,
        request: Request<Abort2pcRequest>,
    ) -> Result<Response<Abort2pcResponse>, Status> {
        let req = request.into_inner();
        let Some(hdr) = req.header else {
            return Err(Status::invalid_argument("missing header"));
        };
        let ep: ShardEpoch = hdr.expected_epoch.unwrap_or_default().into();
        let find_res = self
            .global_manager
            .all_cells()
            .into_iter()
            .find_map(|c| c.lookup_local_handle(hdr.shard_id).ok());
        let Some((_, grp, _)) = find_res else {
            let e = MetaDbError::ShardRoutingError(format!("unknown shard {}", hdr.shard_id));
            return Ok(Response::new(Abort2pcResponse {
                error: Some(ErrorDetailProto::from(&e)),
            }));
        };
        let cmd = TxRaftCmd::Abort2pc {
            txn_id: decode_req_txn_id(&req.txn_id_be)?,
            target_user_keys: req.target_user_keys,
        };
        let payload =
            metadb_tx::command_payload(&cmd).map_err(|e| Status::internal(e.to_string()))?;
        match grp.propose(ep, self.tx_router.clock().now(), payload).await {
            Ok(_) => Ok(Response::new(Abort2pcResponse { error: None })),
            Err(e) => Ok(Response::new(Abort2pcResponse {
                error: Some(ErrorDetailProto::from(&e)),
            })),
        }
    }

    async fn check_txn_status(
        &self,
        request: Request<CheckTxnStatusRequest>,
    ) -> Result<Response<CheckTxnStatusResponse>, Status> {
        let req = request.into_inner();
        let (_, grp, _) = match self.global_manager.route_user_key(&req.primary_key) {
            Ok(t) => t,
            Err(e) => {
                return Ok(Response::new(CheckTxnStatusResponse {
                    error: Some(ErrorDetailProto::from(&e)),
                    txn_state: 0,
                    commit_hlc: 0,
                }))
            }
        };
        let cmd = TxRaftCmd::CheckAndAbortExpiredTxn {
            txn_id: decode_req_txn_id(&req.txn_id_be)?,
            caller_hlc: HlcTimestamp(req.caller_hlc),
        };
        let payload =
            metadb_tx::command_payload(&cmd).map_err(|e| Status::internal(e.to_string()))?;
        match grp
            .propose(grp.current_epoch(), HlcTimestamp(req.caller_hlc), payload)
            .await
        {
            Ok(resp) => {
                let out: TxRaftOutput = codec::deserialize(&resp.result.unwrap_or_default())
                    .unwrap_or(TxRaftOutput::TxnStatus(Some(TxnState::Aborted)));
                let (code, chlc) = match out {
                    TxRaftOutput::TxnStatus(Some(TxnState::Pending)) => (1u32, 0u64),
                    TxRaftOutput::TxnStatus(Some(TxnState::Committed { commit_hlc })) => {
                        (2u32, commit_hlc.0)
                    }
                    _ => (0u32, 0u64),
                };
                Ok(Response::new(CheckTxnStatusResponse {
                    error: None,
                    txn_state: code,
                    commit_hlc: chlc,
                }))
            }
            Err(e) => Ok(Response::new(CheckTxnStatusResponse {
                error: Some(ErrorDetailProto::from(&e)),
                txn_state: 0,
                commit_hlc: 0,
            })),
        }
    }

    async fn apply_aux_batch(
        &self,
        request: Request<ApplyAuxBatchRequest>,
    ) -> Result<Response<ApplyAuxBatchResponse>, Status> {
        let req = request.into_inner();
        let Some(hdr) = req.header else {
            return Err(Status::invalid_argument("missing header"));
        };
        let ep: ShardEpoch = hdr.expected_epoch.unwrap_or_default().into();
        let mut entries = Vec::with_capacity(req.encoded_outbox_entries.len());
        for raw in &req.encoded_outbox_entries {
            entries.push(
                AuxOutboxEntry::decode(raw).map_err(|e| Status::invalid_argument(e.to_string()))?,
            );
        }
        let find_res = self
            .global_manager
            .all_cells()
            .into_iter()
            .find_map(|c| c.lookup_local_handle(hdr.shard_id).ok());
        let Some((_, grp, _)) = find_res else {
            let e = MetaDbError::ShardRoutingError(format!("unknown shard {}", hdr.shard_id));
            return Ok(Response::new(ApplyAuxBatchResponse {
                error: Some(ErrorDetailProto::from(&e)),
                applied_count: 0,
            }));
        };
        let cmd = TxRaftCmd::ApplyAuxBatch { entries };
        let payload =
            metadb_tx::command_payload(&cmd).map_err(|e| Status::internal(e.to_string()))?;
        match grp.propose(ep, self.tx_router.clock().now(), payload).await {
            Ok(resp) => {
                let out: TxRaftOutput = codec::deserialize(&resp.result.unwrap_or_default())
                    .unwrap_or(TxRaftOutput::AppliedCount(0));
                let cnt = match out {
                    TxRaftOutput::AppliedCount(c) => c as u32,
                    _ => 0,
                };
                Ok(Response::new(ApplyAuxBatchResponse {
                    error: None,
                    applied_count: cnt,
                }))
            }
            Err(e) => Ok(Response::new(ApplyAuxBatchResponse {
                error: Some(ErrorDetailProto::from(&e)),
                applied_count: 0,
            })),
        }
    }

    async fn forward_router(
        &self,
        request: Request<ForwardRouterRequest>,
    ) -> Result<Response<ForwardRouterResponse>, Status> {
        let req = request.into_inner();
        match self.tx_router.execute_forwarded_bytes(&req.op).await {
            Ok(result) => Ok(Response::new(ForwardRouterResponse {
                error: None,
                result,
            })),
            Err(error) => Ok(Response::new(ForwardRouterResponse {
                error: Some(ErrorDetailProto::from(&error)),
                result: Vec::new(),
            })),
        }
    }
}

#[tonic::async_trait]
impl ControlPlaneService for MetaDbGrpcService {
    async fn register_node(
        &self,
        request: Request<RegisterNodeRequest>,
    ) -> Result<Response<RegisterNodeResponse>, Status> {
        let req = request.into_inner();
        let Some(cell) = self.global_manager.get_cell(req.cell_id) else {
            let e = MetaDbError::ShardRoutingError(format!("unknown cell {}", req.cell_id));
            return Ok(Response::new(RegisterNodeResponse {
                error: Some(ErrorDetailProto::from(&e)),
            }));
        };
        let instance_id = match metadb_proto::convert::decode_optional_u128_be(&req.instance_id) {
            Ok(id) => id,
            Err(error) => {
                return Ok(Response::new(RegisterNodeResponse {
                    error: Some(ErrorDetailProto::from(&error)),
                }))
            }
        };
        match cell
            .register_node_instance(req.node_id, req.grpc_addr, instance_id)
            .await
        {
            Ok(()) => Ok(Response::new(RegisterNodeResponse { error: None })),
            Err(e) => Ok(Response::new(RegisterNodeResponse {
                error: Some(ErrorDetailProto::from(&e)),
            })),
        }
    }

    async fn store_heartbeat(
        &self,
        request: Request<StoreHeartbeatRequest>,
    ) -> Result<Response<StoreHeartbeatResponse>, Status> {
        // Shard descriptors in this RPC are annotations. They are not installed
        // into `CompositeMetaCache` and they do not schedule splits.
        let req = request.into_inner();
        let Some(cell) = self.global_manager.get_cell(req.cell_id) else {
            let error = MetaDbError::ShardRoutingError(format!(
                "unknown cell {}; client shard descriptors are not applied to routes",
                req.cell_id
            ));
            return Ok(Response::new(StoreHeartbeatResponse {
                error: Some(ErrorDetailProto::from(&error)),
            }));
        };
        let mut reported = Vec::new();
        for shard in &req.shards {
            if let Some(descriptor) = &shard.descriptor {
                if descriptor.shard_id != 0 {
                    reported.push(descriptor.shard_id);
                }
            }
        }
        let disk_free = req.disk_free_known.then_some(req.disk_free_bytes);
        let instance_id = match metadb_proto::convert::decode_optional_u128_be(&req.instance_id) {
            Ok(0) => None,
            Ok(id) => Some(id),
            Err(error) => {
                return Ok(Response::new(StoreHeartbeatResponse {
                    error: Some(ErrorDetailProto::from(&error)),
                }));
            }
        };
        let observation_generation =
            match metadb_proto::convert::decode_optional_u128_be(&req.observation_generation) {
                Ok(id) => id,
                Err(error) => {
                    return Ok(Response::new(StoreHeartbeatResponse {
                        error: Some(ErrorDetailProto::from(&error)),
                    }));
                }
            };
        match cell.accept_remote_heartbeat(
            req.node_id,
            req.monotonic_seq,
            disk_free,
            reported,
            instance_id,
            observation_generation,
        ) {
            Ok(()) => Ok(Response::new(StoreHeartbeatResponse { error: None })),
            Err(error) => Ok(Response::new(StoreHeartbeatResponse {
                error: Some(ErrorDetailProto::from(&error)),
            })),
        }
    }

    async fn get_routes(
        &self,
        request: Request<GetRoutesRequest>,
    ) -> Result<Response<GetRoutesResponse>, Status> {
        let req = request.into_inner();
        let (routes, cluster_id) = if req.cell_id == 0 {
            let cells = self.global_manager.all_cells();
            let cluster_ids: BTreeSet<u128> = cells
                .iter()
                .map(|cell| cell.cluster_id())
                .filter(|id| *id != 0)
                .collect();
            let mut cluster_id = if cluster_ids.len() == 1 {
                cluster_ids.into_iter().next().unwrap_or(0)
            } else {
                0
            };
            let mut routes = Vec::new();
            for cell in &cells {
                match cell.routes_for_live_status() {
                    Ok(live) => routes.extend(live),
                    Err(error) => {
                        return Ok(Response::new(GetRoutesResponse {
                            error: Some(ErrorDetailProto::from(&error)),
                            shards: Vec::new(),
                            cluster_id: Vec::new(),
                        }));
                    }
                }
            }
            for remote in self.global_manager.remote_publications() {
                if cluster_id != 0 && remote.cluster_id != cluster_id {
                    cluster_id = 0;
                }
                routes.extend(remote.routes);
            }
            (routes, cluster_id)
        } else if let Some(remote) = self.global_manager.remote_publication(req.cell_id) {
            (remote.routes, remote.cluster_id)
        } else if let Some(cell) = self.global_manager.get_cell(req.cell_id) {
            let cluster_id = cell.cluster_id();
            match cell.routes_for_live_status() {
                Ok(routes) => (routes, cluster_id),
                Err(error) => {
                    return Ok(Response::new(GetRoutesResponse {
                        error: Some(ErrorDetailProto::from(&error)),
                        shards: Vec::new(),
                        cluster_id: Vec::new(),
                    }));
                }
            }
        } else {
            let e = MetaDbError::ShardRoutingError(format!("unknown cell {}", req.cell_id));
            return Ok(Response::new(GetRoutesResponse {
                error: Some(ErrorDetailProto::from(&e)),
                shards: Vec::new(),
                cluster_id: Vec::new(),
            }));
        };
        let cluster_id = if cluster_id == 0 {
            Vec::new()
        } else {
            metadb_proto::convert::encode_u128_be(cluster_id).to_vec()
        };
        Ok(Response::new(GetRoutesResponse {
            error: None,
            shards: routes.iter().map(route_conv::to_proto_route).collect(),
            cluster_id,
        }))
    }

    async fn allocate_id_block(
        &self,
        request: Request<AllocateIdBlockRequest>,
    ) -> Result<Response<AllocateIdBlockResponse>, Status> {
        if self.directory_only {
            let error = Self::directory_only_error();
            return Ok(Response::new(AllocateIdBlockResponse {
                error: Some(ErrorDetailProto::from(&error)),
                start_id: 0,
                count: 0,
            }));
        }
        let req = request.into_inner();
        let kind = match req.id_kind {
            1 => AllocIdKind::FileInode,
            2 => AllocIdKind::DirInode,
            3 => AllocIdKind::FileId,
            4 => AllocIdKind::ShardId,
            _ => return Err(Status::invalid_argument("unknown id_kind")),
        };
        match self.global_manager.id_allocator().allocate_block(
            decode_req_optional_uuid(&req.request_uuid_be)?,
            kind,
            req.count,
        ) {
            Ok((start_id, count)) => Ok(Response::new(AllocateIdBlockResponse {
                error: None,
                start_id,
                count,
            })),
            Err(e) => Ok(Response::new(AllocateIdBlockResponse {
                error: Some(ErrorDetailProto::from(&e)),
                start_id: 0,
                count: 0,
            })),
        }
    }

    async fn alloc_split_plan(
        &self,
        request: Request<AllocSplitPlanRequest>,
    ) -> Result<Response<AllocSplitPlanResponse>, Status> {
        let req = request.into_inner();
        let Some(cell) = self.global_manager.get_cell(req.cell_id) else {
            let e = MetaDbError::ShardRoutingError(format!("unknown cell {}", req.cell_id));
            return Ok(Response::new(AllocSplitPlanResponse {
                error: Some(ErrorDetailProto::from(&e)),
                child_shard_id: 0,
            }));
        };
        match cell.alloc_split_plan(
            decode_req_optional_uuid(&req.request_uuid_be)?,
            req.parent_shard_id,
            req.split_key,
            req.target_version,
            None,
        ) {
            Ok(child_shard_id) => Ok(Response::new(AllocSplitPlanResponse {
                error: None,
                child_shard_id,
            })),
            Err(e) => Ok(Response::new(AllocSplitPlanResponse {
                error: Some(ErrorDetailProto::from(&e)),
                child_shard_id: 0,
            })),
        }
    }

    async fn activate_split(
        &self,
        request: Request<ActivateSplitRequest>,
    ) -> Result<Response<ActivateSplitResponse>, Status> {
        let req = request.into_inner();
        let Some(cell) = self.global_manager.get_cell(req.cell_id) else {
            let e = MetaDbError::ShardRoutingError(format!("unknown cell {}", req.cell_id));
            return Ok(Response::new(ActivateSplitResponse {
                error: Some(ErrorDetailProto::from(&e)),
            }));
        };
        let p_desc = req
            .parent_descriptor
            .ok_or_else(|| Status::invalid_argument("missing parent_descriptor"))?;
        let c_desc = req
            .child_descriptor
            .ok_or_else(|| Status::invalid_argument("missing child_descriptor"))?;
        let parent_route = route_conv::from_proto_route(p_desc)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let child_route = route_conv::from_proto_route(c_desc)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;

        match cell.activate_split(0, parent_route, child_route, None) {
            Ok(()) => Ok(Response::new(ActivateSplitResponse { error: None })),
            Err(e) => Ok(Response::new(ActivateSplitResponse {
                error: Some(ErrorDetailProto::from(&e)),
            })),
        }
    }

    async fn acquire_dir_rename_lease(
        &self,
        request: Request<AcquireDirRenameLeaseRequest>,
    ) -> Result<Response<AcquireDirRenameLeaseResponse>, Status> {
        if self.directory_only {
            let error = Self::directory_only_error();
            return Ok(Response::new(AcquireDirRenameLeaseResponse {
                error: Some(ErrorDetailProto::from(&error)),
                rename_fence_token: 0,
                expire_hlc: 0,
            }));
        }
        let req = request.into_inner();
        match self
            .global_manager
            .acquire_dir_rename_lease(req.moved_dir_id, req.ttl_ms)
            .await
        {
            Ok(lease) => Ok(Response::new(AcquireDirRenameLeaseResponse {
                error: None,
                rename_fence_token: lease.rename_fence_token,
                expire_hlc: lease.expire_hlc.0,
            })),
            Err(e) => Ok(Response::new(AcquireDirRenameLeaseResponse {
                error: Some(ErrorDetailProto::from(&e)),
                rename_fence_token: 0,
                expire_hlc: 0,
            })),
        }
    }

    async fn release_dir_rename_lease(
        &self,
        request: Request<ReleaseDirRenameLeaseRequest>,
    ) -> Result<Response<ReleaseDirRenameLeaseResponse>, Status> {
        if self.directory_only {
            let error = Self::directory_only_error();
            return Ok(Response::new(ReleaseDirRenameLeaseResponse {
                error: Some(ErrorDetailProto::from(&error)),
            }));
        }
        let req = request.into_inner();
        match self
            .global_manager
            .id_allocator()
            .release_dir_rename_lease(req.rename_fence_token)
        {
            Ok(()) => Ok(Response::new(ReleaseDirRenameLeaseResponse { error: None })),
            Err(e) => Ok(Response::new(ReleaseDirRenameLeaseResponse {
                error: Some(ErrorDetailProto::from(&e)),
            })),
        }
    }

    async fn trigger_shard_split(
        &self,
        request: Request<TriggerShardSplitRequest>,
    ) -> Result<Response<TriggerShardSplitResponse>, Status> {
        let req = request.into_inner();
        for cell in self.global_manager.all_cells() {
            if cell
                .list_routes()
                .iter()
                .any(|r| r.shard_id == req.shard_id)
            {
                return match cell.maybe_split_or_isolate_shard(req.shard_id, true).await {
                    Ok(Some(child_id)) => Ok(Response::new(TriggerShardSplitResponse {
                        error: None,
                        split_executed: true,
                        child_shard_id: child_id,
                    })),
                    Ok(None) => Ok(Response::new(TriggerShardSplitResponse {
                        error: None,
                        split_executed: false,
                        child_shard_id: 0,
                    })),
                    Err(e) => Ok(Response::new(TriggerShardSplitResponse {
                        error: Some(ErrorDetailProto::from(&e)),
                        split_executed: false,
                        child_shard_id: 0,
                    })),
                };
            }
        }
        let e = MetaDbError::ShardRoutingError(format!("unknown shard {}", req.shard_id));
        Ok(Response::new(TriggerShardSplitResponse {
            error: Some(ErrorDetailProto::from(&e)),
            split_executed: false,
            child_shard_id: 0,
        }))
    }

    async fn enable_dual_mode_split(
        &self,
        request: Request<EnableDualModeSplitRequest>,
    ) -> Result<Response<EnableDualModeSplitResponse>, Status> {
        let req = request.into_inner();
        for cell in self.global_manager.all_cells() {
            if cell
                .list_routes()
                .iter()
                .any(|r| r.shard_id == req.shard_id)
            {
                return match cell.enable_dual_mode_split(req.shard_id, req.dominant_pid) {
                    Ok(()) => Ok(Response::new(EnableDualModeSplitResponse { error: None })),
                    Err(e) => Ok(Response::new(EnableDualModeSplitResponse {
                        error: Some(ErrorDetailProto::from(&e)),
                    })),
                };
            }
        }
        let e = MetaDbError::ShardRoutingError(format!("unknown shard {}", req.shard_id));
        Ok(Response::new(EnableDualModeSplitResponse {
            error: Some(ErrorDetailProto::from(&e)),
        }))
    }

    async fn scale_shard_replicas(
        &self,
        request: Request<ScaleShardReplicasRequest>,
    ) -> Result<Response<ScaleShardReplicasResponse>, Status> {
        let req = request.into_inner();
        let Some(cell) = self.global_manager.get_cell(req.cell_id) else {
            let e = MetaDbError::ShardRoutingError(format!("unknown cell {}", req.cell_id));
            return Ok(Response::new(ScaleShardReplicasResponse {
                error: Some(ErrorDetailProto::from(&e)),
                upgraded_shards: 0,
            }));
        };
        let ensure_node = (req.ensure_node_id != 0).then_some(req.ensure_node_id);
        match cell
            .scale_out_replicas_ensuring(req.target_voters as usize, ensure_node)
            .await
        {
            Ok(cnt) => Ok(Response::new(ScaleShardReplicasResponse {
                error: None,
                upgraded_shards: cnt as u32,
            })),
            Err(e) => Ok(Response::new(ScaleShardReplicasResponse {
                error: Some(ErrorDetailProto::from(&e)),
                upgraded_shards: 0,
            })),
        }
    }

    async fn register_cell(
        &self,
        request: Request<RegisterCellRequest>,
    ) -> Result<Response<RegisterCellResponse>, Status> {
        if !self.accepts_remote_cells {
            let error = MetaDbError::ShardRoutingError(
                "cell-manager does not store a remote cell; register it with the global-manager"
                    .to_string(),
            );
            return Ok(Response::new(RegisterCellResponse {
                error: Some(ErrorDetailProto::from(&error)),
            }));
        }
        let req = request.into_inner();
        let range = match CompositeKeyRange::new(req.start_key, req.end_key) {
            Ok(range) => range,
            Err(error) => {
                return Ok(Response::new(RegisterCellResponse {
                    error: Some(ErrorDetailProto::from(&error)),
                }));
            }
        };
        let precheck = match self.global_manager.precheck_remote_cell(
            req.cell_id,
            &range,
            &req.cell_manager_endpoints,
        ) {
            Ok(precheck) => precheck,
            Err(error) => {
                return Ok(Response::new(RegisterCellResponse {
                    error: Some(ErrorDetailProto::from(&error)),
                }));
            }
        };
        if self.forward_secret.trim().is_empty() {
            for endpoint in &req.cell_manager_endpoints {
                if !crate::config::is_literal_loopback_addr(endpoint) {
                    let error = MetaDbError::ShardRoutingError(
                        "unauthenticated RegisterCell only probes a literal loopback address. Set a shared secret before registering a non-loopback cell, or bind that cell to 127.0.0.0/8 or ::1. TLS mode does not replace this rule"
                            .to_string(),
                    );
                    return Ok(Response::new(RegisterCellResponse {
                        error: Some(ErrorDetailProto::from(&error)),
                    }));
                }
            }
        }
        let probed = self
            .probe_cell_routes(req.cell_id, &req.cell_manager_endpoints)
            .await;
        let (cluster_id, routes) = match probed {
            Ok(probed) => probed,
            Err(error) => {
                if precheck == RemotePrecheck::SameIdentity
                    && error.to_string().contains("not reachable")
                {
                    return Ok(Response::new(RegisterCellResponse { error: None }));
                }
                return Ok(Response::new(RegisterCellResponse {
                    error: Some(ErrorDetailProto::from(&error)),
                }));
            }
        };
        let publication = RemotePublication {
            cell_id: req.cell_id,
            range,
            endpoints: req.cell_manager_endpoints,
            cluster_id,
            routes,
        };
        match self.global_manager.commit_remote_cell(publication) {
            Ok(_) => Ok(Response::new(RegisterCellResponse { error: None })),
            Err(error) => Ok(Response::new(RegisterCellResponse {
                error: Some(ErrorDetailProto::from(&error)),
            })),
        }
    }
}

impl MetaDbGrpcService {
    async fn probe_cell_routes(
        &self,
        cell_id: u64,
        endpoints: &[String],
    ) -> std::result::Result<(u128, Vec<CompositeShardRoute>), MetaDbError> {
        let Some(endpoint) = endpoints.first() else {
            return Err(MetaDbError::ShardRoutingError(
                "RegisterCell requires a cell manager endpoint".to_string(),
            ));
        };
        let endpoint = self
            .transport_security
            .prepare_url(endpoint)
            .map_err(|error| {
                MetaDbError::ShardRoutingError(format!(
                    "RegisterCell endpoint is not usable with the configured TLS mode: {error}"
                ))
            })?;
        let mut client = crate::control_plane_client(
            &endpoint,
            &self.forward_secret,
            &self.transport_security,
            self.role.as_ref(),
        )
        .await
        .map_err(|error| {
            MetaDbError::ShardRoutingError(format!(
                "cell {cell_id} manager at {endpoint} is not reachable: {error}"
            ))
        })?;
        let listed = client
            .get_routes(GetRoutesRequest { cell_id })
            .await
            .map_err(|error| {
                MetaDbError::ShardRoutingError(format!(
                    "cell {cell_id} manager at {endpoint} is not reachable: {error}"
                ))
            })?
            .into_inner();
        if let Some(error) = listed.error {
            if !error.message.is_empty() || !error.error_kind.is_empty() {
                return Err(MetaDbError::ShardRoutingError(format!(
                    "cell {cell_id} manager at {endpoint} did not publish routes: {}",
                    error.message
                )));
            }
        }
        let cluster_id = metadb_proto::convert::decode_optional_u128_be(&listed.cluster_id)
            .map_err(|error| {
                MetaDbError::ShardRoutingError(format!(
                    "cell {cell_id} cluster id is invalid: {error}"
                ))
            })?;
        let mut routes = Vec::with_capacity(listed.shards.len());
        for shard in listed.shards {
            routes.push(composite_route_from_proto(shard)?);
        }
        Ok((cluster_id, routes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_sql_params_rejects_an_unset_value() {
        let ok = decode_sql_params(&[
            SqlParamValue {
                kind: Some(sql_param_value::Kind::U64Val(1)),
            },
            SqlParamValue {
                kind: Some(sql_param_value::Kind::StrVal("a".to_string())),
            },
        ])
        .unwrap();
        assert_eq!(ok, vec![SqlParam::U64(1), SqlParam::Str("a".to_string())]);

        let err = decode_sql_params(&[
            SqlParamValue {
                kind: Some(sql_param_value::Kind::U64Val(1)),
            },
            SqlParamValue { kind: None },
            SqlParamValue {
                kind: Some(sql_param_value::Kind::U64Val(2)),
            },
        ])
        .unwrap_err();
        assert!(matches!(err, MetaDbError::SqlParseError(_)));
    }
}
