//! Prometheus exposition formatter for shard, storage, MVCC, compaction, and Raft metrics.
//!
//! Operability series are unlabeled gauges. Shard ids, peer addresses, and
//! paths are not labels and are not values.

use metadb_cluster::DurableGlobalManager;
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
        cells_total: cells.len(),
        ..Default::default()
    };
    let mut min_purge: Option<u64> = None;

    for cell in cells {
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
            min_purge = Some(min_purge.map_or(p, |cur| cur.min(p)));
        }
    }
    snap.min_raft_safe_purge_index = min_purge.unwrap_or(0);
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
}
