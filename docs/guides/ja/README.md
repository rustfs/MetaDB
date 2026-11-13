# MetaDB ユーザーガイド（日本語）

* **言語切替**: [English](../en/README.md) | **日本語** | [Français](../fr/README.md) | [简体中文](../zh/README.md) | [Deutsch](../de/README.md)
* **ドキュメントハブ**: [`docs/README.md`](../../README.md)

**MetaDB** (`rustfs/MetaDB`) は、[RustFS](https://github.com/rustfs) 向けに設計された **100% 純 Rust 製**のファイルシステム構造認識型・分散トランザクションメタデータデータベースエンジンです。アーキテクチャは Baidu の数兆レコード規模の本番メタデータ基盤論文（[*MetaDB: Putting File-System Structure Back into Distributed Databases*, `arXiv:2610.32247v1`](https://arxiv.org/html/2610.32247v1)）に基づいています。

---

## ガイド目次

1. **概要・組込みクイックスタート** *（本ページ）*
2. [**デプロイとオンラインクラスター拡張**](./deployment.md) — `metadbd` シングルノード構成、`1-Voter -> 3-Voter` Multi-Raft 無停止昇格、および Multi-Cell（`GlobalManager` / `CellLocalManager` / `MetaCache`）構成。
3. [**クライアント API・gRPC・20 種類の SQL テンプレート**](./client-apis-and-sql.md) — 組込み Rust SDK、`tonic` gRPC サービス、および 20 種類の軽量メタデータ SQL テンプレート一覧。
4. [**運用管理・`metadb-ctl` CLI・パフォーマンスチューニング**](./operations-and-tuning.md) — 管理 CLI、Jumbo Shard 分離とデュアルモード分割、`SnapshotGuard` メモリ内 MVCC・局所性認識コンパクションの調整、トラブルシューティング。

---

## 機能実装ステータスマトリクス（Phase 1–6 実装・検証完了）

| 機能領域 | リポジトリ実装状態 | 実装 Crate / バイナリ |
| :--- | :--- | :--- |
| 組込み Rust SDK (`PersistentMetaDbCluster` & `MetaDbEngine`) | **実装・検証済**（非同期永続化 + 同期インメモリ） | `metadb-server` |
| `(PID, "_attr_")` 共配置キー・`0x01..0x07` & `HLC` 時計 | **実装・検証済**（88B CRC32C + 500ms ドリフト保護） | `metadb-core` |
| ディスク永続化（純 Rust WAL レス SSTable LSM） | **実装・検証済** (`LsmStorageBackend`、`MANIFEST`、Bloom、LRU キャッシュ) | `metadb-storage` |
| `SnapshotGuard` `ReadCut` メモリ内 MVCC & `LocalityCompactor` | **実装・検証済** (`ActiveSnapshotRegistry` + 最下層安全範囲コンパクション) | `metadb-storage` |
| Multi-Raft 合意 (`1-Voter -> 3-Voter` + `ReadIndex`) | **実装・検証済** (`openraft` 0.10 + セグメント `.raftlog` + `tonic`) | `metadb-raft` |
| `1-RTT` プッシュダウン・`ReplicatedAttrAggregator`・`AuxOutbox` | **実装・検証済**（協調フラッシュバリア + `[0x07]` ローカル退避） | `metadb-tx` |
| クロスシャード 2PC (`Rename`, `Mkdir`, `Rmdir`, `SetAttr`) | **実装・検証済**（プライマリ固定 `TxnRecord` + `WriteIntent` 障害復旧） | `metadb-tx` |
| `PID` 境界分割・Jumbo 分離・`DualMode` 分割 | **実装・検証済** (`ShardEpoch` フェンシング + `DirShardDelta` 並列集約) | `metadb-cluster` |
| 軽量 SQL テンプレート実行 | **実装・検証済**（全 20 パターンの `?` バインド・エスケープ対応） | `metadb-proxy` |
| `metadbd` サーバーデーモン & `metadb-ctl` CLI | **実装・検証済** (`crates/metadb-server/src/bin/`) | `metadb-server` |

---

## クイックスタート：組込み Rust SDK（`PersistentMetaDbCluster` & `MetaDbEngine`）

```toml
[dependencies]
metadb-core = { path = "crates/metadb-core" }
metadb-server = { path = "crates/metadb-server" }
```

```rust
use metadb_core::ROOT_INODE_ID;
use metadb_server::{MetaDbConfig, MetaDbEngine};

fn main() -> metadb_core::Result<()> {
    // 1. Stage 1 シングルノードエンジンを初期化（ルート `/` PID=1 を自動生成）
    let engine = MetaDbEngine::new_single_node(MetaDbConfig::default());

    // 2. ディレクトリ `/checkpoints` とファイルエントリを 1-RTT プッシュダウンで作成
    engine.mkdir(ROOT_INODE_ID, "checkpoints", 100, /*uid=*/ 1000, 0o755)?;
    engine.create_file(100, "step_0001.pt", 2001, 9001, 1000, 0o644, 104_857_600)?;

    // 3. 読取り経路リコンシリエーションにより、非同期フラッシュ前でも線形化可能な dirstat を取得
    let dir_stat = engine.dirstat(100)?;
    assert_eq!(dir_stat.num_children, 1);

    // 4. ディレクトリ配下の有効なエントリ一覧を取得（内部キー `_attr_` は自動除外）
    let entries = engine.listdir(100)?;
    println!("Entries: {}", entries.len());

    // 5. 非同期属性デルタ・クォータ・セカンダリインデックス・MVCC のフラッシュを実行
    engine.run_background_maintenance()?;
    Ok(())
}
```

### 非同期永続化・gRPC クラスター SDK（`MetaDbEngine::open` / `PersistentMetaDbCluster`）

`MetaDbEngine::open(config).await?`（バインド済み gRPC アドレスも取得する場合は `PersistentMetaDbCluster::open_and_serve(config).await?`）は `LsmStorageBackend` と OpenRaft に基づく永続化、バックグラウンド維持処理、および gRPC サーバーを起動します：

```rust
use metadb_server::{MetaDbConfig, MetaDbEngine};

let engine = MetaDbEngine::open(
    MetaDbConfig::from_toml_file("/etc/metadb/standalone.toml")?,
).await?;
let dir = engine.mkdir_auto_id(1, "checkpoints", 1000, 0o755).await?;
let file = engine.create_file_auto_id(dir.attr.inode_id, "step_0002.pt", 1000, 0o644, 4096).await?;
engine.shutdown().await?;
```
