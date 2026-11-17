# Recovery, transaction, and operator correctness hardening

This change hardens acknowledged-write recovery, transaction migration, input validation, and operator behavior. It includes deterministic reproductions, real TCP/gRPC tests, and real daemon/CLI tests. It does not implement disaggregated daemon roles or a replicated multi-Cell control plane.

## Recovery and snapshots

Recovery validates the entire unpurged committed log prefix, frame ordering and identity, and the durable vote/commit metadata before repairing files. Existing histories with missing `HARD_STATE` are rejected. A durable initialization marker also protects empty or fully purged stores from silently resetting their election identity.

Snapshot installation requires a complete existing database image. It verifies required and duplicate filenames, the recovered checkpoint, LogId and membership, MANIFEST-to-SST metadata equality, and all data blocks before replacing live data. An empty or stale archive cannot create a new empty database and advance its advertised applied index. Snapshot publication syncs the temporary directory before its rename.

A state-machine operation lock serializes complete apply operations, checkpoint creation, snapshot replacement, and read-cut acquisition. This favors correctness and can pause writers while a snapshot is built. A later optimization should pin immutable state under a shorter lock and stream it without weakening the atomic checkpoint boundary.

MANIFEST recovery rejects incomplete baselines, complete CRC failures, and missing CURRENT pointers in existing databases. Valid incomplete append tails remain recoverable. The existing v1 framing does not protect its length header independently, so simultaneous structural payload and length corruption still needs a future format upgrade for complete discrimination. Full SST metadata equality detects mismatched recorded histories; identical metadata with different payload identity requires a future content digest or file UUID.

## Transactions and accounting

Checked prepares compare the exact committed values read by a coordinator before installing any intent. New command variants are appended to preserve existing bincode variant indices. Atomic quota UPSERT keeps both SQL branches until serialized apply chooses between them.

Splitting waits until local intents and unacknowledged transaction decisions are drained. An expired lease is not permission to copy an unresolved intent. The replicated fence checks this condition again, closing the gap between the worker's scan and fence apply. Secondary commit/abort paths validate range ownership. The conservative drain currently covers the whole shard and may defer splits during long-running transactions or recovery.

Recovery keeps a deferred split plan pending and the original parent route available, allowing startup and transaction maintenance to proceed. Maintenance retries the plan after draining transaction state. Once the parent fence is committed, recovery completes the existing split without waiting for new transactions in the retained range. Only explicitly retryable drain conditions are deferred; corruption and storage errors still stop recovery.

Routing validates published epochs and ranges against the live shard before returning a handle. Client operations wait for route publication with a bounded retry deadline, and reads recheck ownership at their snapshot boundary. A fenced parent cannot report a moved key as absent or silently omit an unpublished child from a directory listing.

Directory creation rejects root/self/invalid IDs and existing authoritative directory identities. Mutations validate sizes and directory capacity before changing keys. Prepared transactions reserve positive directory capacity. Split cleanup reconstructs retained directory counters during apply, and metadata-only directory updates preserve pending and flushed child accounting.

Quota batches validate and stage their complete effects before mutation. Internal balances remain signed so independent origin streams commute: a delete delivered before the corresponding create can temporarily make a balance negative and then converge to zero. Overflow rejects the entire batch. Callers must not interpret a temporarily negative asynchronous balance as a final quota decision.

## IDs, names, and upgrade behavior

The allocator holds an OS directory lock, reserves fresh local blocks after restart, and never silently recreates counters when established state is missing. File inode sequences respect the bit-63 boundary, including the final local block.

`ID_ALLOCATOR_STATE` now uses a version-2 envelope containing request kind/count fingerprints. The envelope contains an invalid legacy Option tag, so old binaries fail to decode it instead of reinterpreting shifted counters. Legacy state can be read without reissuing reserved IDs. A cached legacy UUID has no verifiable request identity and is rejected with an explicit error; use a new UUID to request a fresh block. Matching version-2 UUID retries remain stable across restart, while different kind/count reuse is rejected. Deduplication remains bounded to the most recent 4,096 nonzero UUID grants. UUID zero explicitly disables deduplication for local allocation calls.

Cell directories persist `CELL_OWNER` independently of the cluster-wide registered-node map. A legacy singleton directory can establish ownership automatically. A legacy replicated directory without ownership evidence fails closed and requires an offline migration using verified original node/cell identities. Do not change a node ID to adopt another node's directory. Missing established Cell state requires repair rather than bootstrap.

Singleton listener changes are committed as guarded Raft node metadata updates before refreshed routes and epochs are persisted. Replicated nodes must restart at their persisted endpoint; online replicated endpoint migration remains a separate operation. An unsupported endpoint change fails explicitly instead of advertising a stale address.

User names are limited to 65,525 UTF-8 bytes so a complete MetaKey plus WriteIntent prefix fits an unshared SST restart key. Overlong names are rejected before successful mutation. Historical invalid records may require offline repair; operations do not silently delete or reinterpret them.

## SQL, CLI, benchmarks, and metrics

Malformed or unset SQL bindings are rejected without changing parameter positions or executing a mutation. SQL mode conversions reject values outside u32. Cursor and prefix bounds are combined before scanning.

`metadb-ctl sql` writes the complete response as JSON, including quota/index lists and pagination metadata. Application and transport errors use nonzero exits and JSON stderr. These output formats replace the former human-readable/partial success output and should be accounted for in scripts.

Benchmarks allocate a unique directory, inode IDs, and FIDs for every run. JSONL output announces `bench_pid` and reports exact requested attempts, application and transport failures, real successful operations, and latency statistics. Zero operations are supported; zero concurrency is rejected. Repeating a benchmark no longer measures duplicate-name failures as successful creates.

Metrics uses HTTP/1 with at most 64 concurrent connections, at most 64 headers, an 8 KiB header buffer, and a two-second connection deadline. Idle clients cannot block unrelated scrapes, and listener/connection tasks are stopped before cluster shutdown.

## Validation and remaining implementation scope

Run the same checks used by CI:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --no-fail-fast
```

The added tests cover both original failures and rejection boundaries, including complete-prefix recovery, malformed snapshot matrices, data corruption, ID migration/downgrade safety, transaction/split interleavings, signed quota delivery, input atomicity, address restart behavior, and actual CLI/daemon behavior. This is not a physical power-cut experiment or a proof that every distributed history is correct.

Only `all-in-one` is an implemented daemon role. Remote Cell registration, autonomous multi-process role bootstrap, replicated GM/CM state, and physical jumbo-node migration remain unimplemented. Directory scans and multi-shard statistics still have scalability work beyond limit pushdown and the corrected cursor semantics. These limitations are not changed into production-scale guarantees by passing regression tests.
