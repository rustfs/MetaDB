//! Prometheus exposition formatter for shard, storage, MVCC, compaction, and Raft metrics.

use metadb_cluster::DurableGlobalManager;

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
    )
}
