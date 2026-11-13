# デプロイとオンラインクラスター拡張（日本語）

* **ガイド目次へ戻る**: [日本語概要](./README.md) | [ハブ](../../README.md)

MetaDB はシングルノード構成から 1,000 台超の分散クラスターまで **完全に同一の WAL レス LSM エンジンと OpenRaft ステートマシン** を実行し、データ移行なしで段階的にスケールアウトできます。

---

## Stage 1：スタンドアロン・シングルノード構成 (`metadbd --role all-in-one`) `[実装済]`

単一プロセス内で `GlobalManager`、`CellLocalManager`、`MetaCache`、`Proxy`、および **`1-Voter` OpenRaft** バックエンドを実行します。ローカル KV の WAL は無効化され、CRC32C チェックサム付き `.raftlog` セグメントへの順次追記のみで耐久性を保証します。

```toml
[node]
node_id = 1
role = "all-in-one"
data_dir = "/var/lib/metadb"
grpc_listen_addr = "127.0.0.1:7800"
metrics_listen_addr = "127.0.0.1:9100"

[clock]
max_clock_drift_ms = 500

[storage]
memtable_size_bytes = 33554432          # 32 MiB
block_cache_bytes = 536870912           # 512 MiB (16-shard LRU)
mvcc_retention_ms = 5000                # 5 秒メモリ内 MVCC 保持ウィンドウ
bloom_bits_per_key = 10
locality_tombstone_threshold = 128
locality_tombstone_ratio = 0.50

[sharding]
bootstrap_table_shards = true
shard_split_threshold_bytes = 4294967296 # 4 GiB
```

```bash
metadbd --config /etc/metadb/standalone.toml
```

---

## Stage 2：単一ホスト上の自動 `PID` 境界シャード分割 `[実装済]`

シャードサイズが `shard_split_threshold_bytes` を超えると、`PolicyShardSplitter` が `group_key = PID`（`[0x01][pid: 8B BE]`）境界でシャードを分割し、親ディレクトリ属性 `(PID, "_attr_")` と全子エントリを同一シャードに維持します。

---

## Stage 3：`3-Voter` Multi-Raft および Multi-Cell 分散クラスターへの無停止拡張 `[実装済]`

### 3.1 `1-Voter` から `3-Voter` への無停止レプリカ昇格

```bash
# 新規 BE ノード 2 台を起動
metadbd --role be --node-id 2 --listen 10.0.1.12:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb
metadbd --role be --node-id 3 --listen 10.0.1.13:7800 --cell-manager 10.0.1.11:7800 --data-dir /var/lib/metadb

# Learner 追加 -> SSTable スナップショット転送 -> Joint Consensus による Voter 昇格
metadb-ctl raft scale-replicas --cell-id 1 --target-voters 3
```

### 3.2 Multi-Cell 2 階層コントロールプレーン構成（1,000 億〜10 兆+ レコード）

* **`global-manager`**：キー範囲と `CellId` の対応表（`<1,000` エントリ）および `65,536` 単位のグローバル ID ブロック割当てを管理。
* **`cell-manager`**：各 Cell 内のシャードハートビート（Cell あたり `20K–50K` シャード）、`PID` 分割、Jumbo 専用ノード分離を制御。
* **`be`**：Multi-Raft グループ、`MemMvccStore`、`LsmStorageBackend`、`PushdownExecutor` を実行。
* **`proxy`**：`MetaCache` による 1 ホップルーティング、20 種類の SQL テンプレートコンパイル、クロスシャード 2PC を担当。
