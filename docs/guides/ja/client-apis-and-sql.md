# クライアント API・gRPC・20 種類の SQL テンプレート（日本語）

* **ガイド目次へ戻る**: [日本語概要](./README.md) | [ハブ](../../README.md)

---

## 1. クライアント接続インターフェース

1. **組込み Rust SDK (`metadb-server`)**：同一プロセス内でのゼロシリアライズ呼び出し（`PersistentMetaDbCluster` 非同期永続化 API および `MetaDbEngine` 同期 API）。
2. **`tonic` gRPC サービス (`metadb.v1` — 実装済)**：
   * `MetadataService`：`CreateFile`, `Mkdir`, `Unlink`, `Rmdir`, `SetAttr`, `Lookup`, `DirStat`, `ListDir`, `ExecuteSql`, `GetQuota`, `SetQuotaLimit`, `ListQuotas`, `GetSecondaryIndex`, `ScanSecondaryIndex`。
   * `TransactionService`：`Rename`（単一シャード `1-RTT` / クロスシャード `2PC` 自動切替）および内部 2PC / `ApplyAuxBatch` RPC。
   * `ControlPlaneService`：`GetRoutes`, `AllocateIdBlock`, `StoreHeartbeat`, `TriggerShardSplit`, `EnableDualModeSplit`, `ScaleShardReplicas`, `RegisterCell`。
3. **20 種類の軽量 SQL テンプレート (`metadb-proxy`)**：`sql_templates_tests` covers all 20 templates. A compile time under `5µs` is not a measurement of this repository.

> **予約名ルール**：ユーザーファイル／ディレクトリ名として `'_attr_'` または NULL 文字 `'\0'` を含む名前は使用できません（`MetaDbError::ReservedEntryName`）。`name = '_attr_'` はテンプレート #7 の `DirStat` 専用です。

---

## 2. 全 20 種類のメタデータ SQL テンプレート一覧

| # | SQL テンプレート | 実行セマンティクス | ステータス |
| :--- | :--- | :--- | :--- |
| **1** | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | ファイル作成（`1-RTT` プッシュダウン + 非同期属性/補助更新） | 実装済 |
| **2** | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | ディレクトリ作成（共配置 `1-RTT` または `2PC`） | 実装済 |
| **3** | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | 通常ファイルの削除 `Unlink` (`1-RTT`) | 実装済 |
| **4** | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | 空ディレクトリ削除 `Rmdir`（実子エントリ走査で空確認） | 実装済 |
| **5** | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | 排他的リネーム（移動先が存在する場合は `AlreadyExists`） | 実装済 |
| **6** | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | POSIX 上書きリネーム（既存ファイルのクォータ・FID を回収） | 実装済 |
| **7** | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | 線形化可能 `DirStat`（`ReadIndex` + メモリ内デルタ統合） | 実装済 |
| **8** | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | 線形化可能ポイント検索 `Lookup` | 実装済 |
| **9** | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | ディレクトリ一覧取得（既定ページ上限付き） | 実装済 |
| **10** | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | カーソルページング `ListDir`（大規模ディレクトリ推奨） | 実装済 |
| **11** | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | プレフィックス検索 `ListDir` (`'prefix%'`) | 実装済 |
| **12** | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | パーミッション変更 `chmod` (`1-RTT`) | 実装済 |
| **13** | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | 所有者変更 `chown` + 非同期クォータ振替 | 実装済 |
| **14** | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | ファイルサイズ・データプレーン `FID` の確定更新 | 実装済 |
| **15** | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | 更新時刻変更 `utimes` (`1-RTT`) | 実装済 |
| **16** | `SELECT * FROM QuotaTable WHERE uid = ?` | ユーザー別の使用容量・Inode 数・上限の取得 | 実装済 |
| **17** | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | ユーザークォータ上限の設定・更新 | 実装済 |
| **18** | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | ユーザークォータ一覧のページング走査 | 実装済 |
| **19** | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | `FID -> (PID, name, create_time)` 逆引き検索 | 実装済 |
| **20** | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | データプレーン GC / スクラブ用カーソル走査 | 実装済 |
