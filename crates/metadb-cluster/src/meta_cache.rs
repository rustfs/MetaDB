//! Unified `[0x01, 0x04)` `CompositeMetaCache` routing user keys (`MetaTable`, `QuotaTable`,
//! `SecondaryIndexTable`) and `DualModeFineGrained` `(PID, name)` sub-ranges in $O(\log N)$ time.

use std::collections::BTreeMap;
use std::sync::RwLock;

use serde::{Deserialize, Serialize};

use metadb_core::{CellId, CompositeKeyRange, InodeId, NodeId, ShardEpoch, ShardId};

use crate::shard::{ShardHealthState, ShardSplitMode};

/// Complete shard routing descriptor stored in `CompositeMetaCache` and persisted by `DurableCellManager`.
#[derive(
    Debug, Clone, PartialEq, Eq, Serialize, Deserialize, wincode::SchemaWrite, wincode::SchemaRead,
)]
pub struct CompositeShardRoute {
    pub cell_id: CellId,
    pub shard_id: ShardId,
    pub range: CompositeKeyRange,
    pub epoch: ShardEpoch,
    pub leader_node: NodeId,
    /// Map of `NodeId -> (grpc_addr, is_voter)`.
    pub peers: BTreeMap<NodeId, (String, bool)>,
    pub split_mode: ShardSplitMode,
    pub health_state: ShardHealthState,
    pub dual_mode_pid: Option<InodeId>,
}

/// Thread-safe unified shard routing cache covering `[0x01, 0x04)`.
#[derive(Default)]
pub struct CompositeMetaCache {
    routes_by_start_key: RwLock<BTreeMap<Vec<u8>, CompositeShardRoute>>,
}

impl CompositeMetaCache {
    pub fn new() -> Self {
        Self::default()
    }

    fn evict_overlapping(
        guard: &mut BTreeMap<Vec<u8>, CompositeShardRoute>,
        range: &CompositeKeyRange,
    ) {
        let stale_keys: Vec<Vec<u8>> = guard
            .iter()
            .filter(|(_, existing)| existing.range.overlaps(&range.start_key, &range.end_key))
            .map(|(k, _)| k.clone())
            .collect();
        for k in stale_keys {
            guard.remove(&k);
        }
    }

    pub fn upsert_route(&self, route: CompositeShardRoute) {
        let mut guard = self.routes_by_start_key.write().unwrap();
        Self::evict_overlapping(&mut guard, &route.range);
        guard.insert(route.range.start_key.clone(), route);
    }

    /// Atomically replaces a parent shard route with `(updated_parent, new_child)` after `ExecuteSplit`.
    pub fn apply_split_routes(
        &self,
        parent_route: CompositeShardRoute,
        child_route: CompositeShardRoute,
    ) {
        let mut guard = self.routes_by_start_key.write().unwrap();
        Self::evict_overlapping(&mut guard, &parent_route.range);
        Self::evict_overlapping(&mut guard, &child_route.range);
        guard.insert(parent_route.range.start_key.clone(), parent_route);
        guard.insert(child_route.range.start_key.clone(), child_route);
    }

    pub fn invalidate_all(&self) {
        self.routes_by_start_key.write().unwrap().clear();
    }

    /// Resolves the `CompositeShardRoute` owning `user_key` in $O(\log N)$ time.
    pub fn lookup_key(&self, user_key: &[u8]) -> Option<CompositeShardRoute> {
        let guard = self.routes_by_start_key.read().unwrap();
        let (_, route) = guard
            .range::<[u8], _>((
                std::ops::Bound::Unbounded,
                std::ops::Bound::Included(user_key),
            ))
            .next_back()?;
        if route.range.contains_key(user_key) {
            Some(route.clone())
        } else {
            None
        }
    }

    /// Returns all `CompositeShardRoute`s intersecting `[start_key, end_key)` sorted ascending.
    pub fn lookup_range(&self, start_key: &[u8], end_key: &[u8]) -> Vec<CompositeShardRoute> {
        let guard = self.routes_by_start_key.read().unwrap();
        let mut out = Vec::new();
        // Start from the greatest shard with `route.start_key <= start_key` (if any), then scan forward.
        if let Some((_, first_candidate)) = guard
            .range::<[u8], _>((
                std::ops::Bound::Unbounded,
                std::ops::Bound::Included(start_key),
            ))
            .next_back()
        {
            if first_candidate.range.overlaps(start_key, end_key) {
                out.push(first_candidate.clone());
            }
        }
        for (k, route) in guard.range::<[u8], _>((
            std::ops::Bound::Excluded(start_key),
            std::ops::Bound::Excluded(end_key),
        )) {
            let _ = k;
            if route.range.overlaps(start_key, end_key) {
                out.push(route.clone());
            }
        }
        out
    }

    pub fn all_routes(&self) -> Vec<CompositeShardRoute> {
        self.routes_by_start_key
            .read()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }
}
