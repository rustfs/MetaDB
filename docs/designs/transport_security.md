# Transport security and the RBAC boundary

This note describes the TLS 1.3 stack in this tree. It is not a production
security acceptance and not a trillion-scale result.

## Owner decision

On 2026-11-20 at 11:17 Asia/Shanghai the owner decided:

- TLS uses rustls with the maintained ring provider only.
- ring's bundled C and assembly are accepted for this TLS stack only.
- Named ISC exceptions are approved only for `ring`, `rustls-webpki`, and
  `untrusted`.
- Every other crate keeps the pure-Rust / no C/C++ FFI rule and the existing
  license allow list.
- `aws-lc-rs` and `aws-lc-sys` are not enabled.
- The exceptions are not a blanket ISC allow and are not widened to other crates.

`deny.toml` records the same limit. `ring` is `Apache-2.0 AND ISC`, so its
exception lists both conjuncts. `rustls-webpki` and `untrusted` are `ISC`.

## What runs

`[security.tls].mode` defaults to `off`. That mode is plaintext. Every
outbound client uses the same `TransportSecurity` value:

- Raft `ChannelPool`
- leader forwarding
- control-plane join, heartbeat, and `RegisterCell` probes
- `cell-manager` publishing itself with `register_cell_with_global_manager`
- `metadb-ctl` and the `metadbd` cell-manager client

`prepare_url` refuses `https://` while mode is `off`, and refuses `http://`
while mode is `tls` or `mtls`. There is no second dialer. Inbound gRPC
(metadata, transaction, control, and Raft) and the metrics/probe listener use
the same policy. `tls` and `mtls` refuse a wildcard listen address, and the
listen host must equal `server_name`.

`tls` and `mtls` are rustls 0.23.46 with `default-features = false` and
`features = ["std", "ring"]`. `tokio-rustls` uses `default-features = false`
and `features = ["ring"]`, because its default features enable `aws_lc_rs`.
The process passes `rustls::crypto::ring::default_provider()` into each
builder. It does not install a process-global provider. Protocol versions are
TLS 1.3 only. Session tickets are not sent and client resumption is disabled,
so each handshake checks the certificate.

Trust anchors come only from `ca_file`. The system root store is not used.
Server certificates are checked with `WebPkiServerVerifier`, including the
dial hostname or IP as SNI. Client certificates in `mtls` are required by
`WebPkiClientVerifier` without `allow_unauthenticated`. There is no accept-all
verifier and no skip-verify switch. In addition to WebPKI's
required-if-present extended-key-usage check, this process rejects a leaf
that omits the extension, rejects `anyExtendedKeyUsage`, and requires
`serverAuth` on a server leaf and `clientAuth` on a client leaf.

An mTLS principal is taken only from the verified leaf's URI SANs, then DNS
SANs, then IP SANs. A wildcard DNS name is not an identity. The principal is
not a role. `authorize_method` always returns an error, and the server does
not call it. TLS does not replace the shared bearer.

Startup reads the PEM files only after the mode, names, and URL checks. A
missing, unreadable, empty, expired, untrusted, or mismatched file returns
before `create_dir_all` and before bind. Parse errors do not include file
bytes.

The shared bearer is unchanged. An empty secret still refuses to serve unless
`allow_unauthenticated = true` and every listener is a literal loopback IP.
A whitespace-only secret is still refused. `localhost` is not loopback.

`metadb-transport` still rejects RDMA. TCP/gRPC remains the only carried
transport. Backup nofollow and crash-safe copy behavior are unchanged.
Readiness rules, including `directory_only` for `global-manager`, are
unchanged.

## Where ring is linked

`ring` 0.17.14 is a dependency of `rustls` and of `rustls-webpki` when the
`ring` feature is on. Those features are enabled only by `metadb-security`.
`metadb-raft` and `metadb-tx` link that crate because they dial through
`TransportSecurity`. `metadb-storage` and `metadb-core` do not. No other crate
enables `aws_lc_rs`. ring is statically linked. It is not `libssl` or
`libcrypto`. Its C and assembly are the vendored sources inside the `ring`
crate, accepted only for this TLS stack.

`rustls-pure` is not a dependency.

## Limits

- The PEM buffer is wiped with `zeroize` after parsing. `PrivateKeyDer` and
  the rustls `ServerConfig` / `ClientConfig` keep their own copies for the
  process lifetime. Wiping the PEM buffer does not wipe those copies.
- There is no certificate reload or rotation API. Replacing a certificate or
  private key means restarting the process.
- No CRL and no OCSP check is performed. WebPKI's revocation arguments are
  left empty. An expired certificate is rejected. A revoked-but-unexpired
  certificate is not detected.
- WebPKI accepts a missing extended-key-usage extension. This process does
  not, but that extra check is a local parse of the leaf after WebPKI. It is
  not a substitute for a policy engine.
- A verified SAN is not authorization. Method RBAC is not installed.
- This stack has not been accepted as production security.
