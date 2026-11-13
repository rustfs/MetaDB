# クライアント API・gRPC・20 種類の SQL テンプレート（日本語）

* **ガイド目次へ戻る**: [日本語概要](./README.md) | [ハブ](../../README.md)

---

## 1. クライアント接続インターフェース

1. **組込み Rust SDK (`metadb-server`)**：同一プロセス内でのゼロシリアライズ呼び出し（現行 `v0.1.0-prototype` で同期 API 利用可能、**Phase 6** で非同期 API 提供）。
2. **`tonic` gRPC サービス (`metadb.v1` — Phase 1 定義 / Phase 6 サーバー実装)**：
   * `MetadataService`：`CreateFile`, `Mkdir`, `Unlink`, `Rmdir`, `SetAttr`, `Lookup`, `DirStat`, `ListDir`, `ExecuteSql`, `GetQuota`, `SetQuotaLimit`, `ListQuotas`, `GetSecondaryIndex`, `ScanSecondaryIndex`。
   * `TransactionService`：`Rename`（単一シャード `1-RTT` / クロスシャード `2PC` 自動切替）および内部 2PC / `ApplyAuxBatch` RPC。
   * `ControlPlaneService`：`GetRoutes`, `AllocateIdBlock`, `StoreHeartbeat`, `TriggerShardSplit`, `EnableDualModeSplit`, `ScaleShardReplicas`, `RegisterCell`。
3. **20 種類の軽量 SQL テンプレート (`metadb-proxy`)**：汎用 SQL オプティマイザを経由せず `<5µs` でストレージ実行プランへ直接変換します（現行 8 パターン実装済、**Phase 5** で全 20 パターンおよび `?` プレースホルダに対応）。

> **予約名ルール**：ユーザーファイル／ディレクトリ名として `'_attr_'` または NULL 文字 `'\0'` を含む名前は使用できません（`MetaDbError::ReservedEntryName`）。`name = '_attr_'` はテンプレート #7 の `DirStat` 専用です。

---

## 2. 全 20 種類のメタデータ SQL テンプレート一覧

| # | SQL テンプレート | 実行セマンティクス | ステータス |
| :--- | :--- | :--- | :--- |
| **1** | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | ファイル作成（`1-RTT` プッシュダウン + 非同期属性/補助更新） | プロトタイプ / Phase 5(`?`) |
| **2** | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | ディレクトリ作成（共配置 `1-RTT` または `2PC`） | プロトタイプ / Phase 5(`?`) |
| **3** | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | 通常ファイルの削除 `Unlink` (`1-RTT`) | プロトタイプ / Phase 5(`?`) |
| **4** | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | 空ディレクトリ削除 `Rmdir`（実子エントリ走査で空確認） | プロトタイプ / Phase 5(`?`) |
| **5** | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | 排他的リネーム（移動先が存在する場合は `AlreadyExists`） | プロトタイプ / Phase 5(`?`) |
| **6** | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | POSIX 上書きリネーム（既存ファイルのクォータ・FID を回収） | Phase 5 |
| **7** | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | 線形化可能 `DirStat`（`ReadIndex` + メモリ内デルタ統合） | プロトタイプ / Phase 5(`?`) |
| **8** | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | 線形化可能ポイント検索 `Lookup` | プロトタイプ / Phase 5(`?`) |
| **9** | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | ディレクトリ一覧取得（既定ページ上限付き） | プロトタイプ / Phase 5(`?`) |
| **10** | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | カーソルページング `ListDir`（大規模ディレクトリ推奨） | Phase 5 |
| **11** | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | プレフィックス検索 `ListDir` (`'prefix%'`) | Phase 5 |
| **12** | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | パーミッション変更 `chmod` (`1-RTT`) | Phase 5 |
| **13** | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | 所有者変更 `chown` + 非同期クォータ振替 | Phase 5 |
| **14** | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | ファイルサイズ・データプレーン `FID` の確定更新 | Phase 5 |
| **15** | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | 更新時刻変更 `utimes` (`1-RTT`) | Phase 5 |
| **16** | `SELECT * FROM QuotaTable WHERE uid = ?` | ユーザー別の使用容量・Inode 数・上限の取得 | Phase 5 |
| **17** | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | ユーザークォータ上限の設定・更新 | Phase 5 |
| **18** | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | ユーザークォータ一覧のページング走査 | Phase 5 |
| **19** | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | `FID -> (PID, name, create_time)` 逆引き検索 | Phase 5 |
| **20** | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | データプレーン GC / スクラブ用カーソル走査 | Phase 5 |
