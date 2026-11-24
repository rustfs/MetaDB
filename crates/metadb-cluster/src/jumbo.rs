//! Jumbo Shard detection and dedicated BE node isolation scheduler (`sharding_and_control_plane.md` §6.1).

use std::collections::BTreeMap;

use metadb_core::{InodeId, NodeId, ShardId};

use crate::meta_cache::CompositeShardRoute;
use crate::shard::ShardHealthState;

pub struct JumboIsolationPlanner;

impl JumboIsolationPlanner {
    /// Selects a dedicated BE node for a newly detected single-`PID` Jumbo Shard by choosing
    /// the registered node currently hosting the fewest non-jumbo shards (preferring a node not
    /// already dedicated to another Jumbo Shard).
    pub fn select_dedicated_node(
        jumbo_shard_id: ShardId,
        dominant_pid: InodeId,
        current_leader: NodeId,
        available_nodes: &[NodeId],
        all_routes: &[CompositeShardRoute],
    ) -> ShardHealthState {
        if available_nodes.is_empty() {
            return ShardHealthState::JumboIsolated {
                dedicated_node: current_leader,
                dominant_pid,
            };
        }

        let mut load_by_node: BTreeMap<NodeId, (usize, usize, usize)> = available_nodes
            .iter()
            .map(|&n| (n, (0usize, 0usize, 0usize)))
            .collect();

        for r in all_routes {
            if r.shard_id == jumbo_shard_id {
                continue;
            }
            let is_jumbo = matches!(r.health_state, ShardHealthState::JumboIsolated { .. });
            let mut counted_voter = false;
            for (&nid, &(_, is_voter)) in &r.peers {
                if !is_voter {
                    continue;
                }
                counted_voter = true;
                if let Some(entry) = load_by_node.get_mut(&nid) {
                    if is_jumbo {
                        entry.0 += 1;
                    } else {
                        entry.1 += 1;
                    }
                    if nid == r.leader_node {
                        entry.2 += 1;
                    }
                }
            }
            if !counted_voter {
                if let Some(entry) = load_by_node.get_mut(&r.leader_node) {
                    if is_jumbo {
                        entry.0 += 1;
                    } else {
                        entry.1 += 1;
                    }
                    entry.2 += 1;
                }
            }
        }

        let dedicated_node = load_by_node
            .into_iter()
            .min_by_key(|&(_, (jumbo_cnt, voter_cnt, leader_cnt))| {
                (jumbo_cnt, voter_cnt, leader_cnt)
            })
            .map(|(nid, _)| nid)
            .unwrap_or(current_leader);

        ShardHealthState::JumboIsolated {
            dedicated_node,
            dominant_pid,
        }
    }
}
