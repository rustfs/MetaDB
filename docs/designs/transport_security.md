# Transport security and the RBAC boundary

This note records a fail-closed result. It is not a production acceptance and
not a trillion-scale result. This process does not terminate TLS.

## What runs

`[security.tls].mode` defaults to `off`. That mode is plaintext. Every
outbound client uses the same `TransportSecurity` value:

- Raft `ChannelPool`
- leader forwarding
- control-plane join, heartbeat, and `RegisterCell` probes
- `cell-manager` publishing itself with `register_cell_with_global_manager`
- `metadb-ctl` and the `metadbd` cell-manager client

`prepare_url` refuses `https://` while mode is `off`, and refuses `http://`
while mode is `tls` or `mtls`. There is no second dialer that can stay on
plaintext after TLS was requested. `mode = "tls"` or `"mtls"` returns before
`open_and_serve` creates a data directory or binds a socket. The certificate
paths are not opened.

The shared bearer is unchanged. An empty secret still refuses to serve unless
`allow_unauthenticated = true` and every listener is a literal loopback IP.
A whitespace-only secret is still refused.

Method RBAC is not installed. `authorize_method` always returns an error, and
the server does not call it. No peer certificate is parsed, so no
`AuthenticatedPrincipal` is produced. The shared bearer is not a role.

`metadb-transport` still rejects RDMA. TCP/gRPC remains the only carried
transport. Backup nofollow and crash-safe copy behavior are unchanged.
Readiness rules, including `directory_only` for `global-manager`, are
unchanged.

## Why TLS is not linked

The owner license allow list is Apache-2.0, MIT, BSD-3-Clause, Unicode-3.0,
and Zlib. `deny.toml` has no license exceptions. Checked SPDX fields:

| Crate | Version | SPDX in `Cargo.toml` | Fits the allow list |
| :--- | :--- | :--- | :--- |
| `rustls` | 0.23.46 | `Apache-2.0 OR ISC OR MIT` | yes, via Apache-2.0 or MIT |
| `rustls-webpki` | 0.103.17 | `ISC` | no |
| `untrusted` | 0.9.0 | `ISC` | no |
| `ring` | 0.17.14 | `Apache-2.0 AND ISC` | no. An ISC-only exception for other crates would not cover this AND expression. The crate also vendors C and assembly (`crypto/*.c`, `links = "ring_core_..."`). |
| `aws-lc-rs` | current rustls default provider | Apache-2.0 for the Rust crate, implemented by AWS-LC C | forbidden by the no-C/C++ rule |
| `rustls-pure` | 0.15.5 | `MIT` | the crate license fits, the stack does not |

`rustls` 0.23 always depends on `rustls-webpki`, which always depends on
`untrusted`. Any rustls build, with any crypto provider, needs those two ISC
crates. That exception was not approved and is not in `deny.toml`.

`rustls-pure` 0.15.5 was published on 2026-11-18 by a third party
(`Gilfeather/rustls-pure`, previously inside Furnace). crates.io showed 1063
downloads and 1 dependent. Its own README says it has had no outside security
audit. Being pure Rust is not an audit. Its changelog records a transitive
`crypto-bigint` timing issue (`RUSTSEC-2026-0387`, patched in `crypto-bigint`
0.7.6). A version pin or a rename does not make this a production provider.
It is not a dependency of this workspace.

The maintained rustls providers are `aws-lc-rs` and `ring`. Neither fits the
current rules. No other mature pure-Rust TLS stack was found that verifies
WebPKI chains without those dependencies. This tree does not implement its
own cryptography.

## Owner decision required before TLS can be implemented

Both of these have to be explicit. This change does not grant either.

1. Named license exceptions, not a blanket ISC allow: `rustls-webpki` (`ISC`)
   and `untrusted` (`ISC`). If the chosen provider is `ring`, a separate
   exception for `ring` (`Apache-2.0 AND ISC`) and an explicit acceptance of
   its vendored C/assembly. `aws-lc-rs` instead requires accepting C FFI.
2. A provider that is maintained and assessed. `rustls-pure` 0.15.5 is not
   that provider.

## Limits that are absent, not implied

Because TLS is not terminated:

- There is no CRL, OCSP, or stapled revocation check.
- There is no credential rotation, reload, or expiry watch.
- Private keys are not parsed. The earlier claim that PEM buffers were
  zeroized did not cover `PrivateKeyDer::clone_key` copies. That code is
  gone; there is no parsed key to wipe.
- Hostname, chain, and EKU checks are not implemented here, and there is no
  accept-all verifier. A future TLS implementation must do those checks. It
  must not add a skip-verify flag.

`global-manager` and `cell-manager` still run. `cell-manager` publishes to
`cluster.global_manager` through `TransportSecurity`, the same object Raft
and the other control clients use.
