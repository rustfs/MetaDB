# EGM Goldfish Review Report & Resolutions (`/egm-review` & `/egm-resolve`)

* **Target Specifications**: [`docs/designs/metadb_architecture.md`](./metadb_architecture.md), [`Architecture.md`](../../Architecture.md)
* **Reviewer Persona**: Zero-Context Goldfish Subagent (Zoom-Out, Comprehension, Design Critic, Implementation Readiness)
* **Status**: All 7 Findings Resolved & Incorporated into Specification and Phase 5 Implementation

---

## Summary of Findings & Resolutions

| ID | Severity | Lens | Finding Summary | Resolution (`/egm-resolve`) |
| :--- | :--- | :--- | :--- | :--- |
| **F1** | Blocking | Design Critic | `AsyncAttrAggregator` reconciliation could undercount or double-count if `dirstat` races with a background delta flush. | Added monotonic `applied_lsn: u64` to `InodeAttr` (`_attr_`). Each queued `AttrDelta` carries its Raft `lsn`. Flushing atomically updates `persisted_attr.applied_lsn = flush_lsn` and prunes only deltas with `lsn <= flush_lsn`. `reconcile_attr()` folds strictly deltas where `delta.lsn > persisted_attr.applied_lsn`. |
| **F2** | Blocking | Design Critic | `rmdir` must not rely solely on asynchronously aggregated `num_children` to prove directory emptiness. | Specified and implemented `rmdir` to perform a serialized prefix scan on `(dir_inode_id, *)` (excluding `"_attr_"`) inside the shard pushdown executor, verifying zero live child entries before removing the directory. |
| **F3** | Blocking | Design Critic | Using a single scalar `last_applied_hlc` for additive `QuotaDelta` events drops out-of-order deltas from different shards; cross-shard `rename` could reorder index updates. | Changed `QuotaTable` deduplication to track per-origin-shard LSN watermarks (`applied_lsn_by_shard: BTreeMap<ShardId, u64>`) for exact-once additive accounting, while `SecondaryIndexTable` uses monotonic `HlcTimestamp` move semantics so stale updates never clobber newer `rename` locations. |
| **F4** | Blocking | Design Critic | Flushing `MemMvccStore` by discarding all older versions immediately could break an in-flight snapshot reader younger than the 5s timeout. | `MemMvccStore::flush_to_persistent()` writes only the latest committed state (or single physical tombstone) to the persistent KV store (preserving the 60% write reduction and zero double-tombstone guarantee), while retaining in-memory versions younger than `retention_window_ms` (`5,000ms`) until they age past the active snapshot window. |
| **F5** | Blocking | Design Critic | Raft log truncation watermark must account for replicated snapshot barriers and durable outbox state during `1-Voter -> 3-Voter` transitions. | Defined `safe_truncate_lsn = min(storage_flushed_lsn, attr_aggregated_lsn, aux_sink_acked_lsn)` and required `ConfChange` learner catch-up to transfer full snapshot + un-truncated Raft log entries before promotion to `Voter`. |
| **F6** | Non-Blocking | Comprehension | User creation of a literal file named `"_attr_"` could collide with the colocated directory attribute entry `(PID, "_attr_")`. | Added explicit validation in `PushdownExecutor` and `MetaKey` rejecting user-supplied child names equal to `ATTR_ENTRY_NAME ("_attr_")` with `MetaDbError::ReservedName`. |
| **F7** | Non-Blocking | Scope / Critic | `SecondaryIndexTable` keyed by `FID -> (PID, name)` assumes one primary namespace path per `FID`. | Documented that each file creation allocates a unique `FID` (`link_count = 1` primary namespace entry), matching object/file-system primary metadata semantics in Baidu's MetaDB. |
