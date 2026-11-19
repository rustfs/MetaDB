# Transport policy and the RDMA rejection

## Decision

MetaDB carries bytes on tonic gRPC over TCP. That remains the default and the required fallback. This increment adds `metadb-transport` so an operator can name RDMA and receive a structured refusal. It does not add an RDMA backend, a feature flag, a simulation, or a transport trait that could be mistaken for one.

`RDMA_BACKEND_IMPLEMENTED` is `false`. The constant is not a runtime probe. The crate does not open `/dev/infiniband`, does not load `rdma_rxe` or `siw`, and does not link or `dlopen` a C library.

## What was considered

| Option | What it is | Why it is not used here |
| --- | --- | --- |
| tonic gRPC over TCP | The current `TonicRaftNetworkFactory` and the public gRPC services | This is the carried transport. It is not RDMA, and this change does not rename it. |
| `async-rdma` | Async Rust API over `rdma-sys` | GPL-3.0. `cargo-deny` does not allow GPL-3.0. The API links rdma-core (`libibverbs` and `librdmacm`), which is C FFI forbidden by `Architecture.md`. |
| `rdma-sys` | MIT bindings to libibverbs and librdmacm | The binding crate is not a transport. rdma-core is `GPL-2.0 OR BSD-2-Clause`. BSD-2-Clause is not in the workspace allow list. Linking it breaks the zero C/C++ FFI claim and the build docs that say the Rust sources do not link a C or C++ library. |
| `sideway` / `rdma-mummy-sys` | MPL-2.0 wrappers that `dlopen` libibverbs and librdmacm | MPL-2.0 is not allow-listed. `dlopen` is still C FFI. A missing library returns `EOPNOTSUPP`, which must not be wrapped as "supported when the `.so` is present". |
| `ibverbs` / `ibverbs-sys` | MIT OR Apache-2.0 bindings that build or link rdma-core | Same FFI and system-library deployment. Upstream documents Soft-RoCE as a software device. Soft-RoCE was not provisioned here, and it is not a TCP fallback. |
| UCX and libfabric bindings | C fabrics that can use RDMA devices | FFI, extra system packages, and no hardware validation in this environment. |
| A from-scratch Rust uverbs client | Userspace verbs against the kernel ABI without linking rdma-core | No maintained implementation is vendored. It would still need `unsafe` device mappings and a real HCA or a software RDMA device to validate. That work is not started. |

Deployment of any of the C libraries would also require distro packages, device nodes, and typically `CAP_IPC_LOCK` or a raised `RLIMIT_MEMLOCK` for memory registration. Containers would need device passthrough. Cross compilation would depend on rdma-core headers. None of that is authorized for this increment, and no paid or Aliyun RDMA resource is requested.

The workspace allow list remains Apache-2.0, MIT, BSD-3-Clause, Unicode-3.0, and Zlib. This change does not add a license exception and does not change `deny.toml`.

## Operator contract

`[transport]` is optional. An omitted section is `mode = "tcp-grpc"`.

| Mode | Result |
| --- | --- |
| `tcp-grpc` | Select tonic gRPC over TCP. A peer that also names RDMA is ignored. The selection is not a fallback. |
| `prefer-rdma` | Reject RDMA with `backend-not-implemented`, select TCP/gRPC, and record an explicit fallback. Startup logs that rejection. The process still binds only the existing gRPC listener. |
| `rdma`, `roce`, `rocev2`, `infiniband`, `ib`, `ibverbs`, `verbs` | Required RDMA. Parsing and `open_and_serve` fail before a data directory is created or a socket is bound. TCP is not selected. |

`tcp` and `grpc` are unrecognized. They are not silent aliases, so a typo cannot be read as a second transport.

Unknown `[transport]` keys fail closed. Keys that ask for an insecure, anonymous, unverified, or authentication-bypass path are refused with that reason. Device, library, and provider keys are refused because no backend exists. The existing `[security].shared_secret` bearer is unchanged and remains the authorization check for every gRPC service and for `GET /metrics`.

Resource keys are ceilings only:

| Key | Default | Allowed |
| --- | --- | --- |
| `negotiation_timeout_ms` | 1000 | `1..=60000` |
| `max_inflight_negotiations` | 64 | `1..=4096` |
| `max_queue_pairs` | 16 | `1..=1024` |
| `max_registered_bytes` | 16 MiB | `1..=64 MiB` |
| `max_message_bytes` | 1 MiB | `1..=8 MiB` |

Zero means "missing", not "unlimited". A successful or failed negotiation reports `ResourceAllocation::NONE` (`queue_pairs = 0`, `registered_bytes = 0`, `messages_posted = 0`).

## Negotiation

`negotiate` is an in-process function. Startup does not run a network handshake and does not add a protobuf field. The assumed peer for startup is a process that was allowed to start, which is a process that accepts TCP/gRPC. A required-RDMA process does not start, so it never becomes that peer.

The function still accepts an explicit peer offer so the contract can be tested and so a future handshake has one place to call. Check order:

1. Authentication boundary. `AuthBoundary::enforced` is what startup uses. A separate RDMA auth domain or an unauthenticated bypass selects nothing. Configuration cannot construct those inputs.
2. Required RDMA. This is reported even when the attempt is also cancelled, past its deadline, or over the in-flight ceiling, so those conditions cannot hide the configuration error or select TCP.
3. Cancellation. The attempt selects nothing. No device resource exists to leak.
4. Deadline. `now_ms == deadline_ms` is still inside the budget. `now_ms > deadline_ms` selects nothing. The caller supplies both timestamps. This crate does not read the clock and does not spawn a timer.
5. In-flight ceiling. `inflight > max_inflight_negotiations` selects nothing. The value at the ceiling is accepted.
6. Intersection. Only `TransportName::TcpGrpc` is selectable. `TransportName::Rdma` is `None`. A peer that offers only the RDMA name, or an empty offer, yields `NoCommonCarriedTransport`.
7. Prefer-RDMA with mutual TCP yields `ExplicitFallback` and `RdmaRejection::BACKEND_NOT_IMPLEMENTED`. A TCP-only policy yields `Selected`. The selected transport is `CarriedTransport::TcpGrpc` in both cases.

A prefer-RDMA process advertises desire separately from implementation. Its peer offer lists TCP/gRPC and does not list RDMA, so a peer cannot treat the offer as an RDMA capability. A required-RDMA offer lists the RDMA name and does not list TCP, so the other side fails the intersection instead of assuming a fallback the operator refused.

There is no on-wire discovery of an external RDMA-only peer. That limitation is `wire-negotiation` in `missing_rdma_backend_work()`.

## Failure, retry, and idempotency

| Outcome | Retry class | Command effect |
| --- | --- | --- |
| TCP/gRPC selected, including an explicit RDMA fallback | `UnchangedTcpGrpcIdempotency` | Later commands use the existing gRPC path. Raft proposal identity and 2PC replay are unchanged. The fallback is not a successful RDMA send and must not be retried as one. |
| Cancelled, deadline exceeded, or in-flight ceiling exceeded | `RetryNegotiationOnly` | No transport was selected and no command bytes were submitted. Repeating the negotiation is not a command replay. |
| Authentication bypass, required RDMA, or no common carried transport | `NotRetriableConfiguration` | The same inputs fail again. Do not replay a Raft or 2PC command to get past them. |

2PC semantics and on-disk formats are not modified. `MetaDbError` did not gain a variant, so existing bincode tags stay where they are. Configuration failures are `MetaDbError::CodecError` strings.

## Boundaries with other work

| Area | Owner | What this increment does |
| --- | --- | --- |
| Bearer secret and listener authentication | Auth configuration | No change to `[security]` or the interceptor. A future RDMA session would have to enforce that same decision before posting work. |
| Readiness and metrics | Readiness / metrics | No new metric, readiness gate, or `/metrics` field. `prefer-rdma` emits one `tracing` warning. |
| Backup and restore | Backup | No change. |
| Scale and membership | Scale control plane | No change. |
| Raft network | `metadb-raft` | `TonicRaftNetworkFactory` is still the network. This crate does not implement `RaftNetwork`. |
| First-ship TCP path | Server | Default configuration does not depend on an RDMA library. Required RDMA fails before `TcpListener::bind`. |

## Work still required

`missing_rdma_backend_work()` is the canonical list. The ids are `ffi-policy`, `license-review`, `hardware-validation`, `queue-pair-lifecycle`, `authorization-binding`, `cancellation-and-deadlines`, `wire-negotiation`, `openraft-integration`, and `idempotency-review`.

A later change that sets `RDMA_BACKEND_IMPLEMENTED` to `true` without a reviewed FFI decision or a validated pure-Rust verbs implementation, and without hardware or an explicitly authorized software RDMA device, would be a false claim. Tests in `metadb-transport` lock the constant, the work ids, the zero allocation, and the refusal to select RDMA.
