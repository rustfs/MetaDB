# 運用管理・`metadb-ctl` CLI・パフォーマンスチューニング（日本語）

* **ガイド目次へ戻る**: [日本語概要](./README.md) | [ハブ](../../README.md)

---

## 1. `metadb-ctl` 管理 CLI `[実装済]`

`[実装済]` means the `metadb-ctl` binary exists and talks to an all-in-one process. `cell register` does not create a cell. `metadbd --role be` is not a supported deployment. Output from a bench command is for the machine that ran it, not a checked-in result.

```bash
# クラスター全体の Cell・BE ノード状態・3 ウォーターマーク遅延を確認
metadb-ctl --endpoint 127.0.0.1:7800 cluster status

# Cell 1 のシャード一覧、キー範囲、ShardEpoch、Jumbo 分離状態を表示
metadb-ctl --endpoint 127.0.0.1:7800 shard list --cell-id 1

# シャードの PID 境界分割を手動トリガー
metadb-ctl --endpoint 127.0.0.1:7800 shard split --shard-id 12

# 超巨大ディレクトリに対して DualModeFineGrained (PID, name) 分割を有効化
metadb-ctl --endpoint 127.0.0.1:7800 shard enable-dual-mode --shard-id 42 --pid 900100

# オンラインでレプリカ数を 3-Voter へ拡張
metadb-ctl --endpoint 127.0.0.1:7800 raft scale-replicas --cell-id 1 --target-voters 3

# SQL テンプレートの実行とホットスポットベンチマーク
metadb-ctl --endpoint 127.0.0.1:7800 sql "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
metadb-ctl --endpoint 127.0.0.1:7800 bench --workload hotspot-create --concurrency 64 --ops 100000
```

---

## 2. Jumbo Shard（巨大単一ディレクトリ）の運用設計

1. **`JumboIsolated` route flag**: When one parent owns more than 85% of the shard's `MetaTable` bytes, or the shard has a single parent, `JumboIsolationPlanner` writes `dedicated_node` from nodes already registered in this cell. It does not migrate co-hosted peer shards onto another host. With one node, the flag names this process.
2. **デュアルモード細粒度分割 (`DualModeFineGrained`)**：1 台の物理容量を超える場合のみ `(PID, name)` 境界での分割を有効化します。各サブシャードが `[0x04] DirShardDelta` を保持するため、`create_file` / `unlink` は `1-RTT` ロックフリーのまま動作し、`dirstat` は `1-hop` 並列集約で正確な値を返します。

---

## 3. 主要チューニングパラメータ (`metadb-storage`)

* **`mvcc_retention_ms`（既定 `5000` ms）**：アクティブな `SnapshotGuard` と書込みコアレッシングの保持期間。5 秒を超える単一スキャンは行わず、テンプレート #10 のカーソルページングを使用してください。
* **`memtable_size_bytes`（既定 `32 MiB`）**：AI チェックポイント等の高並列書込み環境では `64 MiB` に増やすことでメモリ内 MVCC の不要バージョン削減率が向上します。
* **`block_cache_bytes`（既定 `512 MiB`）**：専用 BE ノードでは搭載 RAM の `25%–40%` を推奨します。
* **`locality_tombstone_threshold`（既定 `128`）**：9 バイト `[table_prefix][PID]` バケット内の物理トゥームストーン閾値（比率 `>= 0.50` と併用）。大量 TTL 削除時の `listdir` レイテンシをさらに抑える場合は `64` に下げます。

---

## 4. トラブルシューティング

* **`MetaDbError::TransactionTimeout`**：スナップショット保持時間が `5,000ms` を超過しました。`name > ? LIMIT 1000` によるページング取得へ変更してください。
* **`MetaDbError::ClockSkewExceeded`**：ノード間の時刻ずれが `500ms` を超えています。`chrony` / NTP 同期を確認してください。
* **`MetaDbError::EpochMismatch` / `NotLeader`**：シャード分割またはリーダー切替に伴う一時応答です。SDK / Proxy が `MetaCache` を自動更新して再試行します。
* **`.raftlog` のディスク使用量増加**：`min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)` を確認してください（リモート補助シャード障害時は L0 フラッシュ時にローカル `[0x07]` へ自動退避されるため、ログ削除は停止しません）。
