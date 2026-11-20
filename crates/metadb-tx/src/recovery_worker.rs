//! Background `AuxDispatcher` (FIFO per-`(origin_shard, target_shard)` batching + `AckAuxTasks`)
//! and `TxnRecoveryWorker` (rolling forward committed 2PCs & aborting expired pending txns).

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use metadb_core::{
    lease_exceeds_lifetime, lease_expiry_is_proved, lease_preimage_is_orphan, lease_wall_ms,
    AuxOutboxEntry, AuxTaskPayload, HlcTimestamp, HybridLogicalClock, QuotaKey, Result,
    SecondaryIndexKey, ShardEpoch, ShardId, TxnId, TxnRecordKey, TxnState, TxnStatusRecord,
    WriteIntentKey, WriteIntentRecord, SYSTEM_PREFIX_LEASE_PREIMAGE, SYSTEM_PREFIX_TXN_RECORD,
    SYSTEM_PREFIX_WRITE_INTENT,
};
use metadb_raft::ShardRaftGroup;

use crate::state_machine_impl::{TxRaftCmd, TxStateMachineHandler};
use crate::two_pc::ShardDirectory;

/// Drains un-acknowledged `ShardAuxOutbox` entries in strict FIFO `(origin_raft_index, task_ordinal)`
/// order per destination shard and commits `TxRaftCmd::AckAuxTasks` upon durable ACK.
pub struct AuxDispatcher;

impl AuxDispatcher {
    pub async fn drain_shard_outbox(
        origin_group: &ShardRaftGroup,
        origin_handler: &TxStateMachineHandler,
        directory: &dyn ShardDirectory,
        clock: &HybridLogicalClock,
    ) -> Result<usize> {
        let pending = origin_handler.outbox().pending_entries();
        if pending.is_empty() {
            return Ok(0);
        }

        // Partition tasks by target shard while preserving strict `(origin_raft_index, task_ordinal)` order!
        let mut by_target: BTreeMap<
            ShardId,
            (ShardEpoch, Arc<ShardRaftGroup>, Vec<AuxOutboxEntry>),
        > = BTreeMap::new();

        for entry in pending {
            let target_key = match &entry.payload {
                AuxTaskPayload::QuotaDelta { uid, .. } => QuotaKey { uid: *uid }.encode(),
                AuxTaskPayload::IndexUpsert { fid, .. } | AuxTaskPayload::IndexDelete { fid } => {
                    SecondaryIndexKey { fid: *fid }.encode()
                }
            };
            let (ep, grp, _) = directory.route_user_key(&target_key)?;
            by_target
                .entry(grp.shard_id())
                .or_insert_with(|| (ep, grp, Vec::new()))
                .2
                .push(entry);
        }

        let mut acked_keys: Vec<(u64, u16)> = Vec::new();
        for (_, (target_ep, target_grp, batch)) in by_target {
            let ids: Vec<(u64, u16)> = batch
                .iter()
                .map(|e| (e.origin_raft_index, e.task_ordinal))
                .collect();
            let cmd = TxRaftCmd::ApplyAuxBatch { entries: batch };
            let payload = crate::command_payload(&cmd)?;
            if target_grp
                .propose(target_ep, clock.now(), payload)
                .await
                .is_ok()
            {
                acked_keys.extend(ids);
            }
        }

        let acked_len = acked_keys.len();
        if !acked_keys.is_empty() {
            let ack_cmd = TxRaftCmd::AckAuxTasks { acked_keys };
            let payload = crate::command_payload(&ack_cmd)?;
            let _ = origin_group
                .propose(origin_group.current_epoch(), clock.now(), payload)
                .await?;
        }
        Ok(acked_len)
    }
}

/// Resolves orphaned 2PC `[0x06] TxnRecord`s and `[0x05] WriteIntent`s on crash or timeout.
pub struct TxnRecoveryWorker;

impl TxnRecoveryWorker {
    /// Resolves a single `txn_id` anchored at `primary_key`.
    ///
    /// A committed or aborted record is driven immediately. A pending record is
    /// aborted only when the replicated HLC is already past `expire_hlc`, the
    /// stored window exceeds the lifetime cap, or [`lease_wall_ms`] proves
    /// expiry. An untrusted wall clock does not abort it and does not count as
    /// "still inside the lease".
    pub async fn resolve_single_txn(
        directory: &dyn ShardDirectory,
        clock: &HybridLogicalClock,
        txn_id: TxnId,
        primary_key: &[u8],
    ) -> Result<TxnState> {
        let (p_epoch, p_group, p_handler) = directory.route_user_key(primary_key)?;
        let tk = TxnRecordKey { txn_id }.encode();
        let (_, snap) = p_group.linearizable_read(p_epoch).await?;
        let raw = p_group.engine().get_with_snapshot(&tk, &snap)?;
        drop(snap);
        let Some(raw_rec) = raw else {
            return Ok(TxnState::Aborted);
        };
        let rec = TxnStatusRecord::decode(&raw_rec)?;
        match rec.state {
            TxnState::Committed { .. } | TxnState::Aborted => {
                Self::drive_participants(
                    directory,
                    clock,
                    &p_group,
                    p_handler.as_ref(),
                    &rec,
                    rec.state,
                )
                .await?;
                Ok(rec.state)
            }
            TxnState::Pending => {
                let last_applied = p_group.engine().mvcc().latest_watermarks().1;
                let wall = lease_wall_ms();
                let proved = lease_exceeds_lifetime(
                    rec.start_hlc,
                    rec.expire_hlc,
                    crate::MAX_2PC_LEASE_LIFETIME_MS,
                ) || lease_expiry_is_proved(last_applied, rec.expire_hlc, wall);
                if !proved {
                    return Ok(TxnState::Pending);
                }
                let proposal = wall
                    .filter(|ms| *ms > 1)
                    .map(|ms| HlcTimestamp::new(ms, 0))
                    .filter(|observed| *observed > last_applied)
                    .unwrap_or(last_applied);
                let check_cmd = TxRaftCmd::CheckAndAbortExpiredTxn {
                    txn_id,
                    caller_hlc: proposal,
                };
                let check_bytes = crate::command_payload(&check_cmd)?;
                let _ = p_group.propose(p_epoch, proposal, check_bytes).await?;
                let (_, snap) = p_group.linearizable_read(p_group.current_epoch()).await?;
                if let Some(raw_rec) = p_group.engine().get_with_snapshot(&tk, &snap)? {
                    drop(snap);
                    let rec = TxnStatusRecord::decode(&raw_rec)?;
                    let state = rec.state;
                    Self::drive_participants(
                        directory,
                        clock,
                        &p_group,
                        p_handler.as_ref(),
                        &rec,
                        state,
                    )
                    .await?;
                    Ok(state)
                } else {
                    Ok(TxnState::Aborted)
                }
            }
        }
    }

    /// Scans `[0x06]` (`TxnRecord`) and `[0x05]` (`WriteIntent`) on `group` and resolves any
    /// committed-but-unacked or expired pending transactions.
    pub async fn run_recovery_pass_on_shard(
        group: &ShardRaftGroup,
        handler: &TxStateMachineHandler,
        directory: &dyn ShardDirectory,
        clock: &HybridLogicalClock,
    ) -> Result<usize> {
        let last_applied = group.engine().mvcc().latest_watermarks().1;
        // Wall time is optional. A missing, fallback, or skewed reading must
        // not block roll-forward or orphan cleanup, and it must not be treated
        // as "not yet expired" or as proof that a lease is still valid.
        let wall = lease_wall_ms();
        let expired = |deadline: HlcTimestamp| lease_expiry_is_proved(last_applied, deadline, wall);
        let propose_hlc = wall
            .filter(|ms| *ms > 1)
            .map(|ms| HlcTimestamp::new(ms, 0))
            .unwrap_or(last_applied);
        let ep = group.current_epoch();
        let (_, snap) = group.linearizable_read(ep).await?;
        let eng = group.engine();

        let mut recovered_count = 0usize;

        // 1. Scan `[0x06]` (`TxnRecord`) on this shard.
        let txn_rows = eng.scan_with_snapshot(
            &[SYSTEM_PREFIX_TXN_RECORD],
            &[SYSTEM_PREFIX_TXN_RECORD + 1],
            &snap,
        )?;

        // 2. Scan `[0x05]` (`WriteIntent`) on this shard for orphaned intents past `expire_hlc`.
        let intent_rows = eng.scan_with_snapshot(
            &[SYSTEM_PREFIX_WRITE_INTENT],
            &[SYSTEM_PREFIX_WRITE_INTENT + 1],
            &snap,
        )?;
        let preimage_rows = eng.scan_with_snapshot(
            &[SYSTEM_PREFIX_LEASE_PREIMAGE],
            &[SYSTEM_PREFIX_LEASE_PREIMAGE.saturating_add(1)],
            &snap,
        )?;
        drop(snap);

        for (_, v) in &txn_rows {
            let rec = TxnStatusRecord::decode(v)?;
            let over_cap = matches!(rec.state, TxnState::Pending)
                && lease_exceeds_lifetime(
                    rec.start_hlc,
                    rec.expire_hlc,
                    crate::MAX_2PC_LEASE_LIFETIME_MS,
                );
            if over_cap {
                let check_cmd = TxRaftCmd::CheckAndAbortExpiredTxn {
                    txn_id: rec.txn_id,
                    caller_hlc: propose_hlc,
                };
                let bytes = crate::command_payload(&check_cmd)?;
                let _ = group.propose(ep, propose_hlc, bytes).await;
                recovered_count += 1;
                continue;
            }
            match rec.state {
                TxnState::Pending if expired(rec.expire_hlc) => {
                    let _ =
                        Self::resolve_single_txn(directory, clock, rec.txn_id, &rec.primary_key)
                            .await?;
                    recovered_count += 1;
                }
                TxnState::Committed { .. } | TxnState::Aborted
                    if !rec.pending_ack_keys.is_empty() =>
                {
                    Self::drive_participants(directory, clock, group, handler, &rec, rec.state)
                        .await?;
                    recovered_count += 1;
                }
                _ => {}
            }
        }

        for (ik_bytes, iv_bytes) in &intent_rows {
            let uk = WriteIntentKey::decode(&ik_bytes)?.target_user_key;
            let irec = WriteIntentRecord::decode(&iv_bytes)?;
            if let Some(holders) = irec.resolved_fence_holders() {
                for (tid, holder) in holders {
                    let over_cap = holder.start_hlc.is_some_and(|start| {
                        lease_exceeds_lifetime(
                            start,
                            holder.expire_hlc,
                            crate::MAX_2PC_LEASE_LIFETIME_MS,
                        )
                    });
                    if expired(holder.expire_hlc) || over_cap {
                        let st =
                            Self::resolve_single_txn(directory, clock, tid, &holder.primary_key)
                                .await?;
                        if matches!(st, TxnState::Aborted) {
                            let abort_cmd = TxRaftCmd::Abort2pc {
                                txn_id: tid,
                                target_user_keys: vec![uk.clone()],
                            };
                            let bytes = crate::command_payload(&abort_cmd).unwrap();
                            let _ = group
                                .propose(group.current_epoch(), clock.now(), bytes)
                                .await;
                        }
                        recovered_count += 1;
                    }
                }
            } else if expired(irec.expire_hlc)
                || lease_exceeds_lifetime(
                    irec.start_hlc,
                    irec.expire_hlc,
                    crate::MAX_2PC_LEASE_LIFETIME_MS,
                )
            {
                let st = Self::resolve_single_txn(directory, clock, irec.txn_id, &irec.primary_key)
                    .await?;
                match st {
                    TxnState::Aborted => {
                        let abort_cmd = TxRaftCmd::Abort2pc {
                            txn_id: irec.txn_id,
                            target_user_keys: vec![uk],
                        };
                        let bytes = crate::command_payload(&abort_cmd).unwrap();
                        let _ = group
                            .propose(group.current_epoch(), clock.now(), bytes)
                            .await;
                    }
                    TxnState::Committed { commit_hlc } => {
                        let sec_cmd = TxRaftCmd::CommitSecondary2pc {
                            txn_id: irec.txn_id,
                            commit_hlc,
                            target_user_keys: vec![uk],
                        };
                        let bytes = crate::command_payload(&sec_cmd).unwrap();
                        let _ = group
                            .propose(group.current_epoch(), commit_hlc, bytes)
                            .await;
                    }
                    TxnState::Pending => {}
                }
                recovered_count += 1;
            }
        }

        let mut live_txns = BTreeSet::new();
        for (_, raw) in &txn_rows {
            if let Ok(rec) = TxnStatusRecord::decode(raw) {
                if matches!(rec.state, TxnState::Pending) {
                    live_txns.insert(rec.txn_id);
                }
            }
        }
        for (_, raw) in &intent_rows {
            if let Ok(intent) = WriteIntentRecord::decode(raw) {
                if let Some(holders) = intent.resolved_fence_holders() {
                    live_txns.extend(holders.into_keys());
                } else {
                    live_txns.insert(intent.txn_id);
                }
            }
        }
        if preimage_rows
            .iter()
            .any(|(key, raw)| lease_preimage_is_orphan(key, raw, &live_txns))
        {
            let cmd = TxRaftCmd::SweepOrphanLeasePreimages;
            let bytes = crate::command_payload(&cmd)?;
            if group.propose(ep, propose_hlc, bytes).await.is_ok() {
                recovered_count += 1;
            }
        }

        Ok(recovered_count)
    }

    async fn drive_participants(
        directory: &dyn ShardDirectory,
        clock: &HybridLogicalClock,
        primary_group: &ShardRaftGroup,
        _primary_handler: &TxStateMachineHandler,
        rec: &TxnStatusRecord,
        state: TxnState,
    ) -> Result<()> {
        #[allow(clippy::type_complexity)]
        let mut by_shard: BTreeMap<
            ShardId,
            (ShardEpoch, Arc<ShardRaftGroup>, Vec<Vec<u8>>),
        > = BTreeMap::new();
        for uk in &rec.pending_ack_keys {
            let (ep, grp, _) = directory.route_user_key(uk)?;
            by_shard
                .entry(grp.shard_id())
                .or_insert_with(|| (ep, grp, Vec::new()))
                .2
                .push(uk.clone());
        }

        let mut newly_acked = Vec::new();
        for (_, (ep, grp, keys)) in by_shard {
            let cmd = match state {
                TxnState::Committed { commit_hlc } => TxRaftCmd::CommitSecondary2pc {
                    txn_id: rec.txn_id,
                    commit_hlc,
                    target_user_keys: keys.clone(),
                },
                TxnState::Aborted => TxRaftCmd::Abort2pc {
                    txn_id: rec.txn_id,
                    target_user_keys: keys.clone(),
                },
                TxnState::Pending => continue,
            };
            let bytes = crate::command_payload(&cmd)?;
            if grp.propose(ep, clock.now(), bytes).await.is_ok() {
                newly_acked.extend(keys);
            }
        }

        if !newly_acked.is_empty() {
            let ack_cmd = TxRaftCmd::MarkParticipantAcked {
                txn_id: rec.txn_id,
                acked_user_keys: newly_acked,
            };
            let bytes = crate::command_payload(&ack_cmd)?;
            let _ = primary_group
                .propose(primary_group.current_epoch(), clock.now(), bytes)
                .await;
        }
        Ok(())
    }
}
