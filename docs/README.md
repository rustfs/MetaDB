# MetaDB Documentation Hub

Welcome to the official documentation hub for **MetaDB** (`rustfs/MetaDB`), a 100% pure-Rust, file-system structure-aware distributed metadata database engine built for [RustFS](https://github.com/rustfs).

---

## Multilingual User & Operator Guides

Select your preferred language below. Every guide suite documents the **verified Phases 1–6 pure-Rust implementation** across all 8 workspace crates (`metadb-core`, `metadb-proto`, `metadb-storage`, `metadb-raft`, `metadb-tx`, `metadb-cluster`, `metadb-proxy`, `metadb-server`).

| Language | Overview & Quickstart | Deployment & Scale-Out | Client APIs, gRPC & 20 SQL Templates | Operations, `metadb-ctl` & Tuning |
| :--- | :--- | :--- | :--- | :--- |
| **English (`en`)** | [Overview & Rust SDK](./guides/en/README.md) | [Single-Node to Multi-Cell](./guides/en/deployment.md) | [SDK, gRPC & SQL Reference](./guides/en/client-apis-and-sql.md) | [CLI, Tuning & Troubleshooting](./guides/en/operations-and-tuning.md) |
| **日本語 (`ja`)** | [概要・クイックスタート](./guides/ja/README.md) | [デプロイとオンライン拡張](./guides/ja/deployment.md) | [SDK・gRPC・SQL テンプレート](./guides/ja/client-apis-and-sql.md) | [運用・`metadb-ctl`・チューニング](./guides/ja/operations-and-tuning.md) |
| **Français (`fr`)** | [Présentation & Démarrage](./guides/fr/README.md) | [Déploiement & Montée en charge](./guides/fr/deployment.md) | [SDK, gRPC & 20 Modèles SQL](./guides/fr/client-apis-and-sql.md) | [Exploitation, `metadb-ctl` & Tuning](./guides/fr/operations-and-tuning.md) |
| **简体中文 (`zh`)** | [概览与快速开始](./guides/zh/README.md) | [单机与多单元集群部署](./guides/zh/deployment.md) | [Rust SDK、gRPC 与 20 条 SQL 模板](./guides/zh/client-apis-and-sql.md) | [运维管理、`metadb-ctl` 与调优排障](./guides/zh/operations-and-tuning.md) |
| **Deutsch (`de`)** | [Übersicht & Schnellstart](./guides/de/README.md) | [Bereitstellung & Skalierung](./guides/de/deployment.md) | [SDK, gRPC & 20 SQL-Vorlagen](./guides/de/client-apis-and-sql.md) | [Betrieb, `metadb-ctl` & Tuning](./guides/de/operations-and-tuning.md) |

---

## Architecture, EGM Specifications & Engineering Plan (English)

Technical specifications, formal Elephant-Goldfish Model (EGM) subsystem designs, and the phased Rust development plan are maintained in English:

* [`../Architecture.md`](../Architecture.md) — Master Architecture Specification (referencing Baidu's trillion-scale MetaDB paper, `arXiv:2610.32247v1`)
* [`./DEVELOPMENT_PLAN.md`](./DEVELOPMENT_PLAN.md) — Phased Pure-Rust Implementation Plan (Phases 1–6)
* [`./designs/SITEMAP.md`](./designs/SITEMAP.md) — EGM Design Index
  * [`./designs/concepts_brief.md`](./designs/concepts_brief.md) — 6-Lens Adversarial Concept Brief
  * [`./designs/metadb_architecture.md`](./designs/metadb_architecture.md) — Umbrella EGM Specification
  * [`./designs/storage_lsm_mvcc.md`](./designs/storage_lsm_mvcc.md) — Pure-Rust WAL-less LSM, `SnapshotGuard` MVCC & `LocalityCompactor`
  * [`./designs/consensus_and_transactions.md`](./designs/consensus_and_transactions.md) — `openraft` Multi-Raft, Off-Critical-Path Queues & Crash-Recoverable 2PC
  * [`./designs/sharding_and_control_plane.md`](./designs/sharding_and_control_plane.md) — Topology Sharding, Jumbo Dual-Mode, Two-Tier Control Plane & Wire Protocols
  * [`./designs/review_report.md`](./designs/review_report.md) — Zero-Context Goldfish Review Findings & Resolutions
