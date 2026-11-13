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

## 機能実装ステータスマトリクス（現行プロトタイプ vs. Phase 1–6）

| 機能領域 | 現行リポジトリ (`v0.1.0-prototype`) | 本番実装マイルストーン |
| :--- | :--- | :--- |
| 組込み Rust SDK (`MetaDbEngine`) | **実装済**（同期インメモリ・プロトタイプ） | **Phase 6**（非同期 `tokio` + 永続化 LSM / OpenRaft） |
| `(PID, "_attr_")` 共配置キー & `HLC` 時計 | **実装済** (`metadb-core`) | **Phase 1**（88B CRC32C エンコード + 500ms ドリフト保護） |
| ディスク永続化（純 Rust WAL レス SSTable LSM） | RAM 上の `BTreeMap` シミュレーション | **Phase 2** (`LsmStorageBackend`、`MANIFEST`、Bloom、LRU キャッシュ) |
| `SnapshotGuard` メモリ内 MVCC & `LocalityCompactor` | RAM プロトタイプ | **Phase 2** (`ActiveSnapshotRegistry` + 最下層安全範囲コンパクション) |
| Multi-Raft 合意 (`1-Voter -> 3-Voter` + `ReadIndex`) | RAM `VecDeque` シミュレーション | **Phase 3** (`openraft` + セグメント `.raftlog` + `tonic`) |
| `1-RTT` プッシュダウン・`AsyncAttrAggregator`・`AuxOutbox` | **実装済**（単一プロセス RAM） | **Phase 4**（協調フラッシュバリア + `[0x07]` ローカル退避） |
| クロスシャード 2PC (`Rename`, `Mkdir`, `Rmdir`) | RAM `Rename` プロトタイプ | **Phase 4**（プライマリ固定 `TxnRecord` + `WriteIntent` 障害復旧） |
| `PID` 境界分割・Jumbo 分離・`DualMode` 分割 | **実装済**（RAM 分割・分離） | **Phase 5** (`ShardEpoch` フェンシング + `DirShardDelta` 並列集約) |
| 軽量 SQL テンプレート実行 | **実装済**（8 パターンのリテラル SQL） | **Phase 5**（全 20 パターンの `?` バインド・エスケープ対応） |
| `metadbd` サーバーデーモン & `metadb-ctl` CLI | 計画中 | **Phase 6** (`crates/metadb-server/src/bin/`) |

---

## クイックスタート：組込み Rust SDK（`v0.1.0-prototype` 利用可能）

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
