# CI and release gates

[Overview](./README.md)

This page describes the gates that exist in this tree today. A green GitHub Actions run is not production readiness, and it is not evidence that the official million-operation load passed.

## Current CI

[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml) is the test and package workflow. It runs on pull requests to `main`, on pushes to every branch, and on tags that start with `v`.

### Format, Clippy, and tests

Job `rust-check-lint-test` runs on `ubuntu-24.04` with a **45-minute** timeout. In order, it:

1. Rejects `#[ignore]` tests that have an empty reason (`python3 .github/scripts/reject-bare-ignore.py --self-test crates`).
2. Installs `protobuf-compiler`.
3. Installs Rust **1.100.0** with rustfmt and clippy (`dtolnay/rust-toolchain@1.100.0`).
4. Checks formatting: `cargo fmt --all -- --check`.
5. Runs Clippy with warnings denied: `cargo clippy --workspace --all-targets --locked -- -D warnings`.
6. Runs the workspace tests: `cargo test --workspace --locked --no-fail-fast`.

That test command builds every integration test under `crates/*/tests/`, including the fixtures added with this document:

- `cargo test -p metadb-cluster --test membership_retarget_tests`
- `cargo test -p metadb-cluster --test id_namespace_join_tests`
- `cargo test -p metadb-server --test scale_in_restart_tests`
- `cargo test -p metadb-server --test ci_bounded_prefix_load`

No separate CI job was added for those tests. They run because the existing locked workspace test step picks them up.

### Packaging

Job `package` also has a **45-minute** timeout. It runs on pull requests, on pushes to `main`, and on `v*` tags. The matrix builds release binaries with `cargo build --release --locked -p metadb-server --bins` for:

- `x86_64-unknown-linux-gnu` on `ubuntu-24.04`
- `aarch64-unknown-linux-gnu` on `ubuntu-24.04-arm`

The job then runs `.github/scripts/package-binaries.sh` and uploads the archive. Job `release` publishes a GitHub Release only for a push of a `v*` tag, and only after the test job and the package job succeed. This document does not change branch protection.

## Bounded prefix load is not the official load

`ci_bounded_prefix_load` is a reduced CI fixture: **4 prefixes × 25 creates, then 50 overwrite/delete operations, at concurrency 4**. It asserts that application `errors` and `correctness_errors` are both zero on that fixture.

CI runs that fixture as part of `cargo test --workspace --locked`. It must not be reported as:

- the official load
- a 1,000,000-operation load
- a paper-scale or production benchmark

The official load stays on **three VMs**: **100 prefixes × 10,000 creates + 100,000 overwrite/delete operations at concurrency 64**. Passing the CI fixture does not mean that run passed.

## In-process scale-in is not three-VM evidence

`scale_in_restart_tests` starts three localhost voters inside **one OS process**, shrinks them to 2 and to 1 with `DurableCellManager::scale_out_replicas`, shuts them down, and reopens the same data directories. It checks that acknowledged writes are still readable, that the Raft voter set matches the shrink target, and that the checked path reports no application errors.

That restart is same-machine and in-process. It does **not** replace three-VM scale-out or scale-in evidence. A green CI run of this test is not a three-machine membership result.

## Dependency audit workflow

[`.github/workflows/dependency-audit.yml`](../../.github/workflows/dependency-audit.yml) is a separate workflow. It runs on `pull_request` and `push`. It installs `cargo-audit` 0.23.0 and `cargo-deny` 0.21.0, then runs both against this workspace. `cargo audit -D unmaintained -D unsound -D yanked` fails the job on a vulnerability or on an unmaintained, unsound, or yanked advisory. `cargo deny --locked check` still runs when that audit step has already failed, so a license, ban, or source finding is not hidden. The job stays failed if either command fails. `cargo deny` fails on a vulnerability advisory, on an unmaintained advisory anywhere in the graph, on an unsound advisory in a workspace crate, on a license outside the allow list, or on an unknown registry or git source. Yanked crates and duplicate crate versions are warnings in `cargo deny` and do not by themselves fail that command.

This is an **initial reporting job**. It is not a production-grade supply-chain program. It does not review transitive maintainers, pin a private advisory mirror, attest build provenance, or change who can merge. A green audit job is not proof that the tree is production ready, and a green `ci.yml` run is not that proof either. A red audit job means the tools reported a finding; it does not by itself say the workspace tests failed.

`cargo-deny` reads [`deny.toml`](../../deny.toml) at the repository root. The license allow list is Apache-2.0, MIT, BSD-3-Clause, Unicode-3.0, and Zlib, which is the set `cargo deny check licenses` matched in this workspace when the file was added. Duplicate crate versions are warnings and did not fail the bans check. Unknown registries and unknown git sources are denied.

When this file was added, `cargo audit` reported three unmaintained advisories and no vulnerability advisories: `chrono` 0.4.45 (`RUSTSEC-2026-0543`), `chrono-tz` 0.8.6 (`RUSTSEC-2026-0555`), and `chrono-tz-build` 0.2.1 (`RUSTSEC-2026-0559`). No fixed upgrade was listed. They come in through `openraft`. The dependency-audit job is expected to fail on those unmaintained findings until the lockfile no longer pulls them in. That failure is a report, not a production-readiness verdict.
