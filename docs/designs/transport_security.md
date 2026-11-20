# Transport security and the RBAC boundary

This note records what the TLS workstream implements and what it deliberately
does not implement. It is not a production acceptance record and not a
trillion-scale result.

## What is implemented

`metadb-security` terminates TLS 1.3 for this process. The crypto provider is
`rustls-pure` (RustCrypto, no C, no assembly, no `ring`, no `aws-lc-rs`).
RSA certificates are refused. The trust anchors are the PEM file in
`[security.tls].ca_file`, not the public web PKI.

`[security.tls].mode` is `off` (the default), `tls`, or `mtls`.

- `off` keeps the existing plaintext listeners and the existing bearer rules.
- `tls` presents this process's certificate and requires outbound clients to
  verify the peer certificate and the dialed host name.
- `mtls` does that and also requires a client certificate. The server checks
  the client chain with rustls's WebPKI client verifier and requires a
  subject alternative name. The authenticated principal is that name: a URI
  SAN if one exists, otherwise a DNS SAN, otherwise an IP SAN. A wildcard DNS
  name is not an identity.

The same policy covers:

- inbound gRPC (`MetadataService`, `TransactionService`, `ControlPlaneService`,
  `RaftTransportService`)
- inbound metrics and probes (`/metrics`, `/livez`, `/readyz`, `/healthz`)
- outbound Raft and forwarded client RPCs
- outbound control-plane probes, join, and heartbeats
- `metadb-ctl` and the `metadbd` cell-manager client

There is no configuration that disables certificate or hostname checks.
`danger_accept_invalid_certs` and an accept-all verifier are not used.
`http://` is refused while TLS is on. `https://` is refused while TLS is off.
A wildcard listen address is refused while TLS is on, because it is not a
certificate name. Incomplete or unreadable material is refused before a data
directory is created and before a listener is bound.

The shared bearer is unchanged. An empty secret still refuses to serve unless
`allow_unauthenticated = true` and every listener is a literal loopback IP.
A whitespace-only secret is still refused. TLS does not replace that rule.

Private key bytes are wiped after parsing and are not written into error
strings. This crate does not mint production credentials. The `testing`
feature mints ephemeral Ed25519 certificates for unit tests only.

`rustls-pure` and the RustCrypto crates under it have not had an external
security audit. That limit is part of this repository's status, not a claim
that the handshake is unaudited production PKI.

## What is not implemented: method RBAC

`authorize_method` always returns an error. The server does not call it.

An authenticated principal is not a role. The shared bearer is not a role
either. A client that presents a valid certificate and the shared bearer can
still call every RPC that the bearer already allowed, including control-plane
methods. That is transport authentication, not least privilege per method.

The scale stream owns process role separation (`be`, `proxy`, `cell-manager`,
`global-manager`) and the control-plane shape. It should not open a second
TLS stack and it should not relabel `[security].shared_secret` as a role.
The integration point is:

- read `AuthenticatedPrincipal` from `ConnectInfo<TlsConnectInfo>` on the
  gRPC request, or `MetricsIo::principal` on a metrics connection
- supply a method authorizer that allows each `(service, method)` only for
  the principals that need it
- replace `authorize_method` only when that table exists

Until that authorizer is installed, method RBAC is outstanding work.

## What this change does not touch

Backup copy rules, including nofollow and crash safety, are unchanged.
Readiness status rules are unchanged; probes on a TLS listener still use the
same bearer and the same `/readyz` body. 2PC lease renewal is unchanged. The
transaction stream owns leases. This change only passes the transport policy
into leader forwarding.

`metadb-transport` still rejects RDMA. TLS is not an RDMA path.
