//! Prometheus exposition formatter for shard, storage, MVCC, compaction, and Raft metrics.
//!
//! Operability series are unlabeled gauges. Shard ids, peer addresses, and
//! paths are not labels and are not values.

use metadb_cluster::{
    automatic_leadership_transfer_allowed, CommittedMove, DurableGlobalManager, PlacementPhase,
};
use metadb_raft::RaftNodeOptions;

use crate::operability::{ReadinessReport, ShardGate};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterMetricsSnapshot {
    pub cells_total: usize,
    pub shards_total: usize,
    pub in_memory_mvcc_writes_total: u64,
    pub pruned_mvcc_versions_total: u64,
    pub persistent_kv_writes_total: u64,
    pub active_physical_tombstones: usize,
    pub tombstones_traversed_on_scans_total: u64,
    pub locality_compactions_total: u64,
    pub min_raft_safe_purge_index: u64,
    pub ready: u64,
    pub live: u64,
    /// `ProcessPhase` numeric code: 0 starting, 1 serving, 2 shutting down, 3 failed.
    pub process_phase: u64,
    pub shards_leader_known: u64,
    pub shards_leader_unknown: u64,
    pub shards_recovery_pending: u64,
    pub shards_log_not_applied: u64,
    pub shards_not_voter: u64,
    pub shards_storage_unrecoverable: u64,
    pub shards_raft_stopped: u64,
    pub shards_raft_fatal: u64,
    pub raft_heartbeat_interval_ms: u64,
    pub raft_election_timeout_min_ms: u64,
    pub raft_election_timeout_max_ms: u64,
    pub raft_max_in_snapshot_log_to_keep: u64,
    pub raft_max_segment_bytes: u64,
    /// Local `CELL_CONTROL_PLANE` placement rows. Not a route count.
    pub placement_records: u64,
    pub placement_phase_recorded: u64,
    pub placement_phase_leader_on_dedicated: u64,
    pub placement_phase_dedicated_not_shared: u64,
    pub placement_phase_blocked: u64,
    pub placement_moves_leadership: u64,
    pub placement_moves_voter_replaced: u64,
    /// Rows whose transfer-attempt timestamp is non-zero. The reconciler does not stamp it.
    pub placement_rows_with_transfer_attempt: u64,
    /// Rows whose replace-attempt timestamp is non-zero.
    pub placement_rows_with_replace_attempt: u64,
    /// 1 only when this build calls `transfer_leader`. This build reports 0.
    pub placement_automatic_transfer_enabled: u64,
    /// Total unpurged Raft log entries between `last_applied` and `safe_purge_index` across local shards.
    pub raft_watermark_lag_entries: u64,
    /// Local shards currently running from a `MOVE_LIVE` dedicated jumbo disk directory.
    pub jumbo_file_moves_live: u64,
    /// Nodes with a fresh measured heartbeat hint on local cell managers.
    pub heartbeat_live_nodes: u64,
}

impl ClusterMetricsSnapshot {
    pub(crate) fn apply_operability(
        &mut self,
        report: &ReadinessReport,
        gates: &[ShardGate],
        raft: &RaftNodeOptions,
    ) {
        self.ready = u64::from(report.ready);
        self.live = u64::from(report.live);
        self.process_phase = report.phase.code();
        self.shards_leader_known = count(gates, |gate| gate.leader_known);
        self.shards_leader_unknown = count(gates, |gate| !gate.leader_known);
        self.shards_recovery_pending = count(gates, |gate| gate.recovery_pending);
        self.shards_log_not_applied = count(gates, |gate| !gate.applied);
        self.shards_not_voter = count(gates, |gate| !gate.voter);
        self.shards_storage_unrecoverable = count(gates, |gate| gate.storage_unrecoverable);
        self.shards_raft_stopped = count(gates, |gate| gate.stopped);
        self.shards_raft_fatal = count(gates, |gate| gate.fatal);
        self.raft_heartbeat_interval_ms = raft.heartbeat_interval_ms;
        self.raft_election_timeout_min_ms = raft.election_timeout_min_ms;
        self.raft_election_timeout_max_ms = raft.election_timeout_max_ms;
        self.raft_max_in_snapshot_log_to_keep = raft.max_in_snapshot_log_to_keep;
        self.raft_max_segment_bytes = raft.max_segment_bytes;
    }
}

fn count(gates: &[ShardGate], pred: impl Fn(&ShardGate) -> bool) -> u64 {
    gates.iter().filter(|gate| pred(gate)).count() as u64
}

pub fn collect_cluster_metrics(gm: &DurableGlobalManager) -> ClusterMetricsSnapshot {
    let cells = gm.all_cells();
    let mut snap = ClusterMetricsSnapshot {
        cells_total: gm.published_cell_count(),
        ..Default::default()
    };
    let mut min_purge: Option<u64> = None;
    let mut any_cell_auto_transfer = false;

    for cell in cells {
        any_cell_auto_transfer |= cell.automatic_leadership_transfer_enabled();
        snap.heartbeat_live_nodes += cell.live_heartbeat_node_count();
        let handles = cell.local_shard_handles();
        snap.shards_total += handles.len();
        for (_, grp, _) in handles {
            let m = grp.engine().metrics();
            snap.in_memory_mvcc_writes_total += m.in_memory_mvcc_writes;
            snap.pruned_mvcc_versions_total += m.pruned_mvcc_versions;
            snap.persistent_kv_writes_total += m.persistent_kv_writes;
            snap.active_physical_tombstones += m.active_physical_tombstones;
            snap.tombstones_traversed_on_scans_total += m.tombstones_traversed_on_scans;
            snap.locality_compactions_total += m.compaction_stats.localized_compactions_triggered;
            let p = grp.watermarks().safe_purge_index();
            let last_applied = grp.last_applied_index();
            snap.raft_watermark_lag_entries += last_applied.saturating_sub(p);
            if cell.jumbo_move_phase_name(grp.shard_id()) == "live" {
                snap.jumbo_file_moves_live += 1;
            }
            min_purge = Some(min_purge.map_or(p, |cur| cur.min(p)));
        }
        for placement in cell.stored_placements() {
            snap.placement_records += 1;
            match placement.phase {
                PlacementPhase::Recorded => snap.placement_phase_recorded += 1,
                PlacementPhase::LeaderOnDedicated => snap.placement_phase_leader_on_dedicated += 1,
                PlacementPhase::DedicatedNodeNotShared => {
                    snap.placement_phase_dedicated_not_shared += 1
                }
                PlacementPhase::Blocked => snap.placement_phase_blocked += 1,
            }
            for movement in &placement.committed_moves {
                match movement {
                    CommittedMove::LeadershipTransferred { .. } => {
                        snap.placement_moves_leadership += 1
                    }
                    CommittedMove::VoterReplaced { .. } => snap.placement_moves_voter_replaced += 1,
                }
            }
            if placement.last_transfer_attempt_unix_ms != 0 {
                snap.placement_rows_with_transfer_attempt += 1;
            }
            if placement.last_replace_attempt_unix_ms != 0 {
                snap.placement_rows_with_replace_attempt += 1;
            }
        }
    }
    snap.min_raft_safe_purge_index = min_purge.unwrap_or(0);
    snap.placement_automatic_transfer_enabled =
        u64::from(automatic_leadership_transfer_allowed() || any_cell_auto_transfer);
    snap
}

pub fn render_prometheus_metrics(m: &ClusterMetricsSnapshot) -> String {
    format!(
        concat!(
            "# TYPE metadb_cells_total gauge\n",
            "metadb_cells_total {}\n",
            "# TYPE metadb_shards_total gauge\n",
            "metadb_shards_total {}\n",
            "# TYPE metadb_in_memory_mvcc_writes_total counter\n",
            "metadb_in_memory_mvcc_writes_total {}\n",
            "# TYPE metadb_pruned_mvcc_versions_total counter\n",
            "metadb_pruned_mvcc_versions_total {}\n",
            "# TYPE metadb_persistent_kv_writes_total counter\n",
            "metadb_persistent_kv_writes_total {}\n",
            "# TYPE metadb_active_physical_tombstones gauge\n",
            "metadb_active_physical_tombstones {}\n",
            "# TYPE metadb_tombstones_traversed_on_scans_total counter\n",
            "metadb_tombstones_traversed_on_scans_total {}\n",
            "# TYPE metadb_locality_compactions_total counter\n",
            "metadb_locality_compactions_total {}\n",
            "# TYPE metadb_min_raft_safe_purge_index gauge\n",
            "metadb_min_raft_safe_purge_index {}\n",
            "# TYPE metadb_ready gauge\n",
            "metadb_ready {}\n",
            "# TYPE metadb_live gauge\n",
            "metadb_live {}\n",
            "# TYPE metadb_process_phase gauge\n",
            "metadb_process_phase {}\n",
            "# TYPE metadb_shards_leader_known gauge\n",
            "metadb_shards_leader_known {}\n",
            "# TYPE metadb_shards_leader_unknown gauge\n",
            "metadb_shards_leader_unknown {}\n",
            "# TYPE metadb_shards_recovery_pending gauge\n",
            "metadb_shards_recovery_pending {}\n",
            "# TYPE metadb_shards_log_not_applied gauge\n",
            "metadb_shards_log_not_applied {}\n",
            "# TYPE metadb_shards_not_voter gauge\n",
            "metadb_shards_not_voter {}\n",
            "# TYPE metadb_shards_storage_unrecoverable gauge\n",
            "metadb_shards_storage_unrecoverable {}\n",
            "# TYPE metadb_shards_raft_stopped gauge\n",
            "metadb_shards_raft_stopped {}\n",
            "# TYPE metadb_shards_raft_fatal gauge\n",
            "metadb_shards_raft_fatal {}\n",
            "# TYPE metadb_raft_heartbeat_interval_ms gauge\n",
            "metadb_raft_heartbeat_interval_ms {}\n",
            "# TYPE metadb_raft_election_timeout_min_ms gauge\n",
            "metadb_raft_election_timeout_min_ms {}\n",
            "# TYPE metadb_raft_election_timeout_max_ms gauge\n",
            "metadb_raft_election_timeout_max_ms {}\n",
            "# TYPE metadb_raft_max_in_snapshot_log_to_keep gauge\n",
            "metadb_raft_max_in_snapshot_log_to_keep {}\n",
            "# TYPE metadb_raft_max_segment_bytes gauge\n",
            "metadb_raft_max_segment_bytes {}\n",
            "# TYPE metadb_placement_records gauge\n",
            "metadb_placement_records {}\n",
            "# TYPE metadb_placement_phase_recorded gauge\n",
            "metadb_placement_phase_recorded {}\n",
            "# TYPE metadb_placement_phase_leader_on_dedicated gauge\n",
            "metadb_placement_phase_leader_on_dedicated {}\n",
            "# TYPE metadb_placement_phase_dedicated_not_shared gauge\n",
            "metadb_placement_phase_dedicated_not_shared {}\n",
            "# TYPE metadb_placement_phase_blocked gauge\n",
            "metadb_placement_phase_blocked {}\n",
            "# TYPE metadb_placement_moves_leadership gauge\n",
            "metadb_placement_moves_leadership {}\n",
            "# TYPE metadb_placement_moves_voter_replaced gauge\n",
            "metadb_placement_moves_voter_replaced {}\n",
            "# TYPE metadb_placement_rows_with_transfer_attempt gauge\n",
            "metadb_placement_rows_with_transfer_attempt {}\n",
            "# TYPE metadb_placement_rows_with_replace_attempt gauge\n",
            "metadb_placement_rows_with_replace_attempt {}\n",
            "# TYPE metadb_placement_automatic_transfer_enabled gauge\n",
            "metadb_placement_automatic_transfer_enabled {}\n",
            "# TYPE metadb_raft_watermark_lag_entries gauge\n",
            "metadb_raft_watermark_lag_entries {}\n",
            "# TYPE metadb_jumbo_file_moves_live gauge\n",
            "metadb_jumbo_file_moves_live {}\n",
            "# TYPE metadb_heartbeat_live_nodes gauge\n",
            "metadb_heartbeat_live_nodes {}\n",
        ),
        m.cells_total,
        m.shards_total,
        m.in_memory_mvcc_writes_total,
        m.pruned_mvcc_versions_total,
        m.persistent_kv_writes_total,
        m.active_physical_tombstones,
        m.tombstones_traversed_on_scans_total,
        m.locality_compactions_total,
        m.min_raft_safe_purge_index,
        m.ready,
        m.live,
        m.process_phase,
        m.shards_leader_known,
        m.shards_leader_unknown,
        m.shards_recovery_pending,
        m.shards_log_not_applied,
        m.shards_not_voter,
        m.shards_storage_unrecoverable,
        m.shards_raft_stopped,
        m.shards_raft_fatal,
        m.raft_heartbeat_interval_ms,
        m.raft_election_timeout_min_ms,
        m.raft_election_timeout_max_ms,
        m.raft_max_in_snapshot_log_to_keep,
        m.raft_max_segment_bytes,
        m.placement_records,
        m.placement_phase_recorded,
        m.placement_phase_leader_on_dedicated,
        m.placement_phase_dedicated_not_shared,
        m.placement_phase_blocked,
        m.placement_moves_leadership,
        m.placement_moves_voter_replaced,
        m.placement_rows_with_transfer_attempt,
        m.placement_rows_with_replace_attempt,
        m.placement_automatic_transfer_enabled,
        m.raft_watermark_lag_entries,
        m.jumbo_file_moves_live,
        m.heartbeat_live_nodes,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operability::{evaluate_readiness, ProcessPhase, ShardGate};

    #[test]
    fn operability_series_are_unlabeled_and_follow_readiness() {
        let gates = [ShardGate::servable()];
        let report = evaluate_readiness(ProcessPhase::Serving, &gates);
        let mut snap = ClusterMetricsSnapshot::default();
        snap.apply_operability(&report, &gates, &RaftNodeOptions::default());
        let text = render_prometheus_metrics(&snap);
        assert!(text.contains("metadb_ready 1\n"), "{text}");
        assert!(text.contains("metadb_live 1\n"), "{text}");
        assert!(text.contains("metadb_process_phase 1\n"), "{text}");
        assert!(
            text.contains("metadb_raft_heartbeat_interval_ms 150\n"),
            "{text}"
        );
        assert!(text.contains("metadb_shards_total"), "{text}");
        assert!(!text.contains('{'), "series must not grow labels: {text}");
        assert!(!text.contains("secret"), "{text}");

        let starting = evaluate_readiness(ProcessPhase::Starting, &gates);
        snap.apply_operability(&starting, &gates, &RaftNodeOptions::default());
        let text = render_prometheus_metrics(&snap);
        assert!(text.contains("metadb_ready 0\n"), "{text}");
        assert!(text.contains("metadb_process_phase 0\n"), "{text}");
        assert!(text.contains("metadb_shards_leader_known 1\n"), "{text}");
    }

    #[test]
    fn placement_series_are_unlabeled_counts() {
        let text = render_prometheus_metrics(&ClusterMetricsSnapshot::default());
        for series in [
            "metadb_placement_records 0\n",
            "metadb_placement_phase_recorded 0\n",
            "metadb_placement_phase_leader_on_dedicated 0\n",
            "metadb_placement_phase_dedicated_not_shared 0\n",
            "metadb_placement_phase_blocked 0\n",
            "metadb_placement_moves_leadership 0\n",
            "metadb_placement_moves_voter_replaced 0\n",
            "metadb_placement_rows_with_transfer_attempt 0\n",
            "metadb_placement_rows_with_replace_attempt 0\n",
            "metadb_placement_automatic_transfer_enabled 0\n",
        ] {
            assert!(text.contains(series), "{text}");
        }
        assert!(!text.contains('{'), "{text}");
        assert!(!text.contains("secret"), "{text}");

        let snap = ClusterMetricsSnapshot {
            placement_records: 4,
            placement_phase_recorded: 1,
            placement_phase_leader_on_dedicated: 1,
            placement_phase_dedicated_not_shared: 1,
            placement_phase_blocked: 1,
            placement_moves_leadership: 2,
            placement_moves_voter_replaced: 3,
            placement_rows_with_transfer_attempt: 0,
            placement_rows_with_replace_attempt: 1,
            placement_automatic_transfer_enabled: 0,
            ..ClusterMetricsSnapshot::default()
        };
        let text = render_prometheus_metrics(&snap);
        assert!(text.contains("metadb_placement_records 4\n"), "{text}");
        assert!(
            text.contains("metadb_placement_phase_recorded 1\n"),
            "{text}"
        );
        assert!(
            text.contains("metadb_placement_moves_voter_replaced 3\n"),
            "{text}"
        );
        assert!(
            text.contains("metadb_placement_rows_with_transfer_attempt 0\n"),
            "{text}"
        );
        assert!(!text.contains('{'), "{text}");
    }
}
