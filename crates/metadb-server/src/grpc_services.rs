//! `tonic` gRPC service implementations (`MetadataService`, `TransactionService`,
//! and `ControlPlaneService`) backed by `PersistentMetaDbCluster`.

use std::collections::BTreeMap;
use std::sync::Arc;

use tonic::{Request, Response, Status};

use metadb_cluster::{
    AllocIdKind, CompositeShardRoute, DurableGlobalManager, ShardHealthState, ShardSplitMode,
};
use metadb_core::{
    AuxOutboxEntry, CompositeKeyRange, DirShardDeltaKey, DirShardDeltaRecord, EntryType,
    HlcTimestamp, InodeAttr, MetaDbError, MetaKey, ShardEpoch, TxnState, WriteIntentKey,
    WriteIntentRecord,
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

fn decode_req_txn_id(bytes: &[u8]) -> Result<u128, Status> {
    metadb_proto::convert::decode_u128_be(bytes)
        .map_err(|e| Status::invalid_argument(e.to_string()))
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
        }
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
        let ep: ShardEpoch = hdr.expected_epoch.unwrap_or_default().into();

        let find_res = self
            .global_manager
            .all_cells()
            .into_iter()
            .find_map(|c| c.lookup_local_handle(hdr.shard_id).ok());
        let Some((_, grp, handler)) = find_res else {
            let err = MetaDbError::ShardRoutingError(format!("unknown shard {}", hdr.shard_id));
            return Ok(Response::new(GetDirPartitionStatResponse {
                error: Some(ErrorDetailProto::from(&err)),
                is_anchor: false,
                partition_attr: None,
                max_applied_hlc: 0,
            }));
        };

        let read_res = if req.use_fixed_barrier_hlc {
            grp.fixed_barrier_read(ep, HlcTimestamp(req.barrier_hlc))
                .await
        } else {
            grp.linearizable_read(ep).await
        };

        let (tok, snap) = match read_res {
            Ok(p) => p,
            Err(e) => {
                return Ok(Response::new(GetDirPartitionStatResponse {
                    error: Some(ErrorDetailProto::from(&e)),
                    is_anchor: false,
                    partition_attr: None,
                    max_applied_hlc: 0,
                }))
            }
        };

        let attr_key = MetaKey::dir_attr(req.pid).encode();
        if handler.key_range().contains_key(&attr_key) {
            match grp.engine().get_with_snapshot(&attr_key, &snap) {
                Ok(Some(raw)) => {
                    let base =
                        InodeAttr::decode(&raw).map_err(|e| Status::internal(e.to_string()))?;
                    let rec = handler
                        .attr_aggregator()
                        .reconcile_inode_attr(req.pid, base, &snap);
                    Ok(Response::new(GetDirPartitionStatResponse {
                        error: None,
                        is_anchor: true,
                        partition_attr: Some(InodeAttrProto::from(&rec)),
                        max_applied_hlc: tok.max_applied_hlc.0,
                    }))
                }
                Ok(None) => {
                    let e = MetaDbError::DirectoryNotFound(req.pid);
                    Ok(Response::new(GetDirPartitionStatResponse {
                        error: Some(ErrorDetailProto::from(&e)),
                        is_anchor: true,
                        partition_attr: None,
                        max_applied_hlc: tok.max_applied_hlc.0,
                    }))
                }
                Err(e) => Ok(Response::new(GetDirPartitionStatResponse {
                    error: Some(ErrorDetailProto::from(&e)),
                    is_anchor: true,
                    partition_attr: None,
                    max_applied_hlc: 0,
                })),
            }
        } else {
            let dk = DirShardDeltaKey { pid: req.pid }.encode();
            match grp.engine().get_with_snapshot(&dk, &snap) {
                Ok(Some(raw)) => {
                    let sub_base = DirShardDeltaRecord::decode(&raw)
                        .map_err(|e| Status::internal(e.to_string()))?;
                    let sub = handler
                        .attr_aggregator()
                        .reconcile_dir_shard_delta(req.pid, sub_base, &snap);
                    let synth = InodeAttr {
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
                    };
                    Ok(Response::new(GetDirPartitionStatResponse {
                        error: None,
                        is_anchor: false,
                        partition_attr: Some(InodeAttrProto::from(&synth)),
                        max_applied_hlc: tok.max_applied_hlc.0,
                    }))
                }
                Ok(None) => Ok(Response::new(GetDirPartitionStatResponse {
                    error: None,
                    is_anchor: false,
                    partition_attr: None,
                    max_applied_hlc: tok.max_applied_hlc.0,
                })),
                Err(e) => Ok(Response::new(GetDirPartitionStatResponse {
                    error: Some(ErrorDetailProto::from(&e)),
                    is_anchor: false,
                    partition_attr: None,
                    max_applied_hlc: 0,
                })),
            }
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
        let mut params = Vec::with_capacity(req.params.len());
        for p in req.params {
            if let Some(kind) = p.kind {
                match kind {
                    sql_param_value::Kind::U64Val(v) => params.push(SqlParam::U64(v)),
                    sql_param_value::Kind::I64Val(v) => params.push(SqlParam::I64(v)),
                    sql_param_value::Kind::StrVal(s) => params.push(SqlParam::Str(s)),
                }
            }
        }

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
                    ProxySqlResult::Entries { entries, .. } => {
                        resp.entries = entries.iter().map(MetaEntryProto::from).collect();
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

        let cmd = TxRaftCmd::Prepare2pc {
            txn_id,
            is_primary: req.is_primary,
            primary_key: req.primary_key.clone(),
            participant_keys: req.all_participant_keys,
            start_hlc: HlcTimestamp(req.start_hlc),
            expire_hlc: HlcTimestamp(req.expire_hlc),
            rename_fence_token: req.has_rename_fence_token.then_some(req.rename_fence_token),
            intents,
        };

        let (_, grp, _) = match self.global_manager.route_user_key(&req.primary_key) {
            Ok(t) => t,
            Err(e) => {
                return Ok(Response::new(Prepare2pcResponse {
                    error: Some(ErrorDetailProto::from(&e)),
                    prepared_hlc: 0,
                }))
            }
        };
        let payload = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;
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
        let payload = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;
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
        let payload = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;
        match grp.propose(ep, HlcTimestamp(req.commit_hlc), payload).await {
            Ok(resp) => {
                let out: TxRaftOutput = bincode::deserialize(&resp.result.unwrap_or_default())
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
        let payload = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;
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
        let payload = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;
        match grp
            .propose(grp.current_epoch(), HlcTimestamp(req.caller_hlc), payload)
            .await
        {
            Ok(resp) => {
                let out: TxRaftOutput = bincode::deserialize(&resp.result.unwrap_or_default())
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
        let payload = bincode::serialize(&cmd).map_err(|e| Status::internal(e.to_string()))?;
        match grp.propose(ep, self.tx_router.clock().now(), payload).await {
            Ok(resp) => {
                let out: TxRaftOutput = bincode::deserialize(&resp.result.unwrap_or_default())
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
        match cell.register_node(req.node_id, req.grpc_addr) {
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
        let req = request.into_inner();
        for s in req.shards {
            if let Some(desc) = s.descriptor {
                if let Ok(route) = route_conv::from_proto_route(desc) {
                    self.global_manager.meta_cache().upsert_route(route);
                }
            }
        }
        Ok(Response::new(StoreHeartbeatResponse { error: None }))
    }

    async fn get_routes(
        &self,
        request: Request<GetRoutesRequest>,
    ) -> Result<Response<GetRoutesResponse>, Status> {
        let req = request.into_inner();
        let routes = if req.cell_id == 0 {
            self.global_manager.meta_cache().all_routes()
        } else if let Some(cell) = self.global_manager.get_cell(req.cell_id) {
            cell.list_routes()
        } else {
            vec![]
        };
        Ok(Response::new(GetRoutesResponse {
            error: None,
            shards: routes.iter().map(route_conv::to_proto_route).collect(),
        }))
    }

    async fn allocate_id_block(
        &self,
        request: Request<AllocateIdBlockRequest>,
    ) -> Result<Response<AllocateIdBlockResponse>, Status> {
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
        let req = request.into_inner();
        self.global_manager
            .release_dir_rename_lease(req.rename_fence_token)
            .await;
        Ok(Response::new(ReleaseDirRenameLeaseResponse { error: None }))
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
        match cell.scale_out_replicas(req.target_voters as usize).await {
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
        let _req = request.into_inner();
        Ok(Response::new(RegisterCellResponse { error: None }))
    }
}
