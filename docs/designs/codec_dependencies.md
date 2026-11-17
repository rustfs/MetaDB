# CRC and serialization dependency selection

## Decision

MetaDB uses crc-fast 1.10 with the CRC-32/ISCSI algorithm and wincode 0.6 with native schemas for project-owned types. The Serde bridge is restricted to OpenRaft boundary types. The shared implementation lives in `metadb-core::codec` and `metadb-core::checksum`.

## Production paths and compatibility requirements

| Path | Encoded content | Compatibility requirement |
| --- | --- | --- |
| Core system records | Directory deltas, write intents, transaction status, outbox entries | Preserve the version byte, field order, enum indices and CRC trailer |
| Transaction apply | `TxRaftCmd`, `TxRaftOutput`, shard routing metadata | Preserve log replay and serialized responses, including u128 transaction IDs |
| Raft log store | OpenRaft entries and HARD_STATE | Preserve committed entry recovery, vote and purge state |
| Raft snapshot | Membership, checkpoint, archive and snapshot metadata | Preserve recovery and installation of existing snapshots |
| Cluster metadata | Shard registry, allocator grants and routing state | Preserve legacy layout fallbacks and u128 request fingerprints |
| RPC | OpenRaft requests and replies, serialized core errors | Preserve the existing Protobuf field numbers and binary payload grammar |
| Storage framing | SSTable blocks, footer, MANIFEST and system-record checksums | Preserve Castagnoli checksums and each trailer's byte order |

The MANIFEST edit grammar and fixed-width core key/attribute codecs already use explicit byte layouts. Their checksum helper changes implementation. Checkpoint membership and descriptor byte arrays embed serialized payloads.

## CRC selection

`crc32_iscsi` implements the Castagnoli variant used by the previous `crc32c` dependency. Its check value for `123456789` is `0xe3069283`. The existing helper still returns u32. Big-endian and little-endian framing remain selected by the existing framing helpers.

crc-fast supports architecture-specific hardware acceleration on aarch64 and x86 families. Only its `std` feature is enabled. The crate's default FFI and panic-handler features are unnecessary for this workspace.

The previous crc32c 0.6.9 already dispatches to SSE4.2 on x86_64 and CRC instructions on supported aarch64 builds. The selected specialized `crc32_iscsi` function avoids generic algorithm selection in crc-fast. MetaDB checksums include 88-byte attributes, 4 KiB SSTable blocks, variable-sized log frames and snapshot files, so dispatch overhead and payload size both matter. The implementation uses the specialized one-shot function because every current caller supplies a complete payload.

The regression tests compare the selected implementation against an independent reflected Castagnoli reference across small inputs, SIMD-sized boundaries, 4 KiB blocks, 64 KiB inputs and unaligned slices. Frozen trailer checks exercise endian order and corruption rejection. Throughput improvement requires workload measurements on the deployment CPU.

## wincode and postcard comparison

| Property | wincode | postcard |
| --- | --- | --- |
| Existing bincode 1.x default bytes | Compatible schemas can retain fixed-width LE integers, u32 enum indices and u64 sequence lengths | Uses a different compact encoding, including variable-length integers |
| Project-owned Rust types | Native SchemaWrite/SchemaRead derives | Existing Serde derives |
| Foreign OpenRaft types | Serde adapter needed | Existing Serde support |
| u128 transaction and request IDs | Native schema support | Serde support |
| Existing logs, snapshots and registries | Can retain the deployed byte grammar | Requires explicit versioning, legacy decoding and migration |
| Large archive payloads | Default 4 MiB preallocation limit needs configuration | Allocation-enabled serialization needed |
| Performance relevance | Native schemas support direct writes and placement initialization | Compact encoding can reduce payload size |

MetaDB's existing recovery paths and mixed-version transport make byte compatibility the deciding factor. wincode supplies native schemas for local command, error, metadata and checkpoint types. Derives preserve declaration order and ordinal enum tags, including enums with explicit Rust discriminants.

Postcard has a documented stable wire specification from 1.0 onward. It is suitable for a newly versioned compact protocol. Adopting it for these existing unversioned payloads would expand this dependency replacement into a storage and cluster-protocol migration.

## Serde adapter boundary

serde-wincode 0.1.2 provides `SerdeCompat<T>` for foreign types. Source review found that its serializer omits `serialize_u128` and `serialize_i128`, although its size calculator and deserializer handle them. Project-owned u128-bearing types therefore use native wincode schemas. OpenRaft requests contain opaque command bytes, so transaction IDs are encoded by the native codec before entering the OpenRaft Serde boundary.

Persisted hard-state fields and snapshot metadata use explicit adapter field annotations. Other OpenRaft calls use the dedicated `serialize_serde` and `deserialize_serde` entry points. Future foreign types using 128-bit integers require revisiting this adapter boundary.

The writer accepts the existing payload sizes. Native readers retain the 4 MiB preallocation cap. A preallocation-limit error retries through Serde collection visitors, which allocate incrementally with cautious size hints. This preserves large archives while avoiding unbounded native preallocation from a forged dynamic-element count. The Serde bridge reads strings and byte buffers with the unlimited configuration after input-length validation by wincode. Tests exercise both huge dynamic-element counts and valid archives above 4 MiB.

## Review and validation

Review focuses on persisted enum ordinals, u128 fields, nested byte arrays, legacy registry/allocator fallback decoders, CRC polynomial and trailer ordering, external OpenRaft adapters, and large snapshots. Protobuf names containing `bincode` continue to describe the compatible byte format.

Permanent regression tests cover frozen legacy integer/enum/container layouts, native u128 encoding, Serde bridge reads, truncation, invalid enum/bool encodings, impossible byte-vector lengths, and payloads above the default preallocation limit.

A temporary bincode 1.3.3 differential oracle compares serialized bytes at the native and Serde codec entry points while running the existing workspace tests. The differential run passed 170 tests with one repository-defined ignored stress test. The oracle is removed before delivery. The delivered dependency graph uses wincode and crc-fast.

Final validation with Rust 1.100.0 passed `cargo test --workspace --locked --no-fail-fast` with 177 passed and one ignored stress test. The exact staged source tree passed `cargo clippy --workspace --all-targets --locked -- -D warnings`, formatting, the ignored-test policy check and `git diff --cached --check`. Dependency-tree inspection confirmed removal of bincode and crc32c. Throughput benchmarks and deployment-platform runs remain unmeasured.

## Follow-up snapshot review

Snapshot file byte vectors now override the native preallocation cap with the same u64 length encoding. The byte-vector reader checks the complete input window before allocating. Files larger than 4 MiB therefore stay on the native decoder, while dynamic-element collections retain the default cap and incremental fallback. A regression verifies native decoding, equality with Serde-compatible wire bytes and rejection of a forged u64::MAX file length.

The sender constructs each 256 KiB chunk as tonic polls the stream. This removes the eager vector containing copies of every archive chunk. The serialized archive and transport buffering still consume memory.

The receiver requires a nonempty stream, vote and metadata in the first chunk, a fixed shard ID, zero initial offset, contiguous subsequent offsets and an explicit final done marker. It rejects incomplete streams and repeated headers before calling OpenRaft installation. The existing sender already emits this framing.

Real gRPC regressions exercise malformed streams and valid single/multiple-chunk framing. Both authenticated and ordinary learner-promotion/failover tests transfer a snapshot file larger than 4 MiB, then verify its contents after leader failure.

Follow-up validation on Rust 1.100.0 passed 179 workspace tests with one ignored stress test. The exact staged source tree passed all-target Clippy with warnings denied, formatting, the ignored-test policy and diff checks. The optimization is verified by native-decoder coverage and the lazy stream construction. Throughput and peak-memory measurements remain unmeasured.

## Upstream references

- [crc-fast API and architecture support](https://docs.rs/crc-fast/1.10.0/crc_fast/)
- [wincode compatibility and schema documentation](https://docs.rs/wincode/0.6.3/wincode/)
- [wincode configuration](https://docs.rs/wincode/0.6.3/wincode/config/struct.Configuration.html)
- [serde-wincode implementation](https://docs.rs/serde-wincode/0.1.2/src/serde_wincode/lib.rs.html)
- [Postcard wire specification](https://postcard.jamesmunns.com/)
