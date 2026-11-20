# 実装されているアーキテクチャ

[概要](./README.md) · [English](../en/architecture.md) · [Français](../fr/architecture.md) · [Deutsch](../de/architecture.md) · [简体中文](../zh/architecture.md)

このページは `crates/` のコードに従います。複数プロセスのセル配備は説明しません。その配備は実装されていません。 [運用](./operations.md#既知の制限) を参照してください。

## プロセスの形

`PersistentMetaDbCluster::open_and_serve`（`crates/metadb-server/src/lib.rs`）は次を行います。

1. `all-in-one` 以外の `node.role` を拒否する。
2. An empty `shared_secret` does not start the process unless `allow_unauthenticated = true`, and that flag is loopback-only. A whitespace-only secret is rejected even with the flag. A usable secret plus the flag is rejected. A wildcard or other non-loopback address cannot use the opt-in. It then binds `node.grpc_listen_addr` (default `127.0.0.1:7800`; `127.0.0.1:0` asks the OS for a port).
3. `<data_dir>/global_allocator` で `DurableIdAllocator` を開く。
4. `<data_dir>/be` で `ShardGroupHost` を開く。
5. `<data_dir>/cell_<cell_id>` で `DurableCellManager` を開くかブートストラップする。
6. その 1 つのセルを、キー範囲 `[0x01, 0x04)` で `DurableGlobalManager` に登録する。
7. ルートの inode `1`（`ROOT_INODE_ID`）に属性レコードがあることを保証する。
8. Serves four gRPC services on one listener. All four use the same bearer interceptor. When `metrics_listen_addr` is set, `GET /metrics` on that address requires the same secret. A non-empty secret rejects a missing or wrong bearer. An empty secret does not listen unless `allow_unauthenticated = true`, and that flag is loopback-only. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners and outbound calls are plaintext and `https` is refused. `tls` and `mtls` terminate TLS 1.3 with rustls and the ring provider, verify the configured CA, hostname, and extended key usage, and refuse `http`. A wildcard listen address is refused. TLS does not replace the bearer. There is no CRL or OCSP check and no certificate reload. This is not a production security acceptance. See `docs/designs/transport_security.md`.
9. 250 ms 間隔のメンテナンスタスクを起動する。

`metadbd` は薄いラッパです。TOML を読み、CLI の上書きを適用し、`open_and_serve` を呼び、任意で `--cell-manager` へ `RegisterNode` を呼び、Ctrl-C で `shutdown` を呼びます。メトリクスは設定されているとき `open_and_serve` の中で提供されます。

## キーとテーブル

ユーザテーブルの接頭辞（`crates/metadb-core/src/schema.rs`）:

| 接頭辞 | テーブル | `bootstrap_table_shards = true` のときのブートストラップ範囲 |
| :--- | :--- | :--- |
| `0x01` | `MetaTable` | シャード 1、`[0x01, 0x02)` |
| `0x02` | `QuotaTable` | シャード 2、`[0x02, 0x03)` |
| `0x03` | `SecondaryIndexTable` | シャード 3、`[0x03, 0x04)` |

`MetaTable` のキーは `[0x01][pid: 8 バイトビッグエンディアン][名前 UTF-8]` です。ディレクトリ自身の属性は予約された子 `(pid, "_attr_")` で、`(pid, child_name)` の隣に格納されます。空、NUL を含む、または `_attr_` と等しいユーザ名は拒否されます（`ReservedEntryName`）。

ディレクトリの inode ID はビット 63 が 0 です。ファイルの inode ID はビット 63 が 1 です（`FILE_INODE_TAG_BIT`）。ルートは inode `1` です。永続アロケータは新しいディレクトリ ID を `2` から、ファイル ID を `1` から、動的シャード ID を `4` から始めます（1–3 はブートストラップシャードです）。`LocalIdBuffer` は、付与を `ID_ALLOCATOR_STATE` へ書いたあと、ディレクトリ ID を 256 個単位、ファイル inode ID とファイル ID を 4,096 個単位で渡します。

シャードローカルのシステム接頭辞:

| 接頭辞 | レコード |
| :--- | :--- |
| `0x04` | デュアルモードの非アンカーサブシャード向け `DirShardDelta` |
| `0x05` | 2PC の `WriteIntent` |
| `0x06` | 2PC の `TxnRecord` |
| `0x07` | あふれた補助アウトボックスエントリ |

`bootstrap_table_shards = false` のとき、ブートストラップは ID `1` のシャードを 1 つ作り、`[0x01, 0x04)` を覆います。

## リクエストの経路

メタデータ呼び出しは、組み込みの `DistributedTxRouter`（`cluster.tx_router()`）か、gRPC の `MetadataService` / `TransactionService` から入ります。`StatelessProxyRouter` は 20 個の SQL テンプレートの 1 つをコンパイルし、同じルータを呼びます。汎用 SQL エンジンはありません。パーサが受け付けるのは次の文だけです（`crates/metadb-proxy/src/sql_templates.rs`）。`?` は `u64`、`i64`、または文字列のパラメータに束縛します。リテラル `name = '_attr_'` はディレクトリ統計です。`_attr_` に等しい束縛パラメータは拒否されます。

| # | 文 | 効果 |
| :--- | :--- | :--- |
| 1 | `INSERT INTO MetaTable (pid, name, id, type, fid, uid, mode, size) VALUES (?, ?, ?, 'FILE', ?, ?, ?, ?)` | ファイル作成 |
| 2 | `INSERT INTO MetaTable (pid, name, id, type, uid, mode) VALUES (?, ?, ?, 'DIR', ?, ?)` | mkdir |
| 3 | `DELETE FROM MetaTable WHERE pid = ? AND name = ?` | unlink |
| 4 | `RMDIR FROM MetaTable WHERE pid = ? AND name = ?` | rmdir |
| 5 | `UPDATE MetaTable SET pid = ?, name = ? WHERE pid = ? AND name = ?` | 上書きしないリネーム |
| 6 | `REPLACE INTO MetaTable (pid, name) SELECT ?, ? FROM MetaTable WHERE pid = ? AND name = ?` | 上書きするリネーム |
| 7 | `SELECT * FROM MetaTable WHERE pid = ? AND name = '_attr_'` | dirstat |
| 8 | `SELECT * FROM MetaTable WHERE pid = ? AND name = ?` | lookup |
| 9 | `SELECT * FROM MetaTable WHERE pid = ? ORDER BY name ASC` | listdir。上限は固定の 10,000 |
| 10 | `SELECT * FROM MetaTable WHERE pid = ? AND name > ? ORDER BY name ASC LIMIT ?` | listdir のページ |
| 11 | `SELECT * FROM MetaTable WHERE pid = ? AND name LIKE ? ORDER BY name ASC LIMIT ?` | 接頭辞 listdir（`LIKE` はこの接頭辞形だけ） |
| 12 | `UPDATE MetaTable SET mode = ? WHERE pid = ? AND name = ?` | setattr mode |
| 13 | `UPDATE MetaTable SET uid = ? WHERE pid = ? AND name = ?` | setattr uid |
| 14 | `UPDATE MetaTable SET size = ?, fid = ? WHERE pid = ? AND name = ?` | setattr のサイズと fid |
| 15 | `UPDATE MetaTable SET modify_time = ? WHERE pid = ? AND name = ?` | setattr の更新時刻 |
| 16 | `SELECT * FROM QuotaTable WHERE uid = ?` | クォータ取得 |
| 17 | `INSERT INTO QuotaTable (uid, quota_limit_bytes) VALUES (?, ?) ON CONFLICT (uid) DO UPDATE SET quota_limit_bytes = ?` | クォータ上限の設定（保存されるのは `ON CONFLICT` 側の値） |
| 18 | `SELECT * FROM QuotaTable WHERE uid >= ? ORDER BY uid ASC LIMIT ?` | クォータ一覧 |
| 19 | `SELECT * FROM SecondaryIndexTable WHERE fid = ?` | 二次索引の取得 |
| 20 | `SELECT * FROM SecondaryIndexTable WHERE fid > ? ORDER BY fid ASC LIMIT ?` | 二次索引の走査 |

`metadb-ctl sql` はパラメータリストが空の `ExecuteSql` を送るので、文は `?` ではなくリテラルを使います。

ルーティングは `CompositeMetaCache` を使います。ミスまたはエポック不一致のときは、ローカルセルからキャッシュを更新し、ローカルのシャードグループに対して再試行します。`DurableGlobalManager::route_user_key` は **ローカル** のシャードハンドルを探します。ユーザリクエストを別プロセスへ転送しません。

## ストレージ

各シャードのステートマシンは、`<data_dir>/be/shards/<20 桁の shard_id>/lsm` に WAL なし LSM を開きます。SSTable は 4 KiB の接頭辞圧縮データブロック（リスタート間隔 16）、ブルームフィルタ、`CURRENT` ポインタ付きの `MANIFEST` を使います。ブロックキャッシュは 16 分割の LRU です。合計予算は `storage.block_cache_bytes`（既定 64 MiB）です。16 個のキャッシュシャードはそれぞれ少なくとも 16 KiB を受け取ります。

MVCC の版は `storage.mvcc_retention_ms`（既定 5,000）のあいだメモリに留まります。協調フラッシュは memtable を封印し、属性集約フックを実行し、L0 SSTable を書きます。ユーザ KV 用の別 WAL はありません。クラッシュ回復は Raft ログと LSM マニフェストです。`LocalityCompactor` は 9 バイト接頭辞のバケットごとにトゥームストーンを追い、`locality_tombstone_threshold` と `locality_tombstone_ratio` の両方を超えたときにホットな削除範囲をコンパクトします。`ServerTomlConfig::to_lsm_config` は `auto_locality_compaction` を常に `true` にします。TOML では消せません。

`storage.memtable_size_bytes` はアクティブな memtable のフラッシュしきい値です。`storage.max_imm_memtables`（既定 4）は不変テーブルのバックプレッシャー上限です。書き手は、もう 1 つキューへ積む代わりに、不変テーブルがフラッシュされるまで待ちます。

## Raft

ローカルの各シャードは `openraft` 0.10 のグループです。ログは `.../shards/<id>/raft` 以下の分割された CRC32C ファイルです。Defaults are loaded from `[raft]` when that table or a field is omitted. `raft_options` is not `#[serde(skip)]`. Unknown keys in `[raft]` fail parsing. Values apply at group start and are not reloaded. `max_segment_bytes` does not rewrite segments already on disk. `/readyz` is 200 only after startup has finished, shutdown has not started, and every local Raft group has finished startup replay, applied a log entry, is a voter, and has a known leader. A bound socket is not enough. There is no gRPC health RPC. See [Operability](../en/operability.md).


| 項目 | 既定 |
| :--- | :--- |
| ハートビート | 150 ms |
| 選挙タイムアウト | 450–900 ms |
| `max_in_snapshot_log_to_keep` | 0 |
| ログセグメントサイズ | 64 MiB |

ログのパージは `min(state_flushed_index, aux_outbox_durable_index, last_snapshot_index)` で門がかかります。250 ms のメンテナンスループは、シャードがまだログエントリを保持しているときに `flush_snapshot_and_purge` を呼びます。ただしスナップショット水位がすでに `state_flushed_index` 以上で、残エントリが 64 未満のときは呼びません。

`ConfChangeOrchestrator::add_learners_and_promote_voters` は learner を追加し、複製の遅れが最大 64 エントリになるまで待ち（既定のタイムアウトは 10 秒。`scale_out_replicas` が渡す値）、それから `change_membership` を呼びます。プロセス内テスト `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` は、1 つの OS プロセスの中で 3 つの tonic リスナーと基本的な KV ステートマシンを使ってこれを行います。`metadbd` は起動しません。

## トランザクションと補助処理

単一ディレクトリの作成、unlink、setattr、lookup、dirstat、listdir、および同一シャードの mkdir/rmdir は、シャードリーダへ提案されます。親ディレクトリのカウンタはフォアグラウンド経路でロックされません。変異はデルタを記録し、`ReplicatedAttrAggregator` がフラッシュバリアで畳み込み、dirstat は読み取り経路で未フラッシュのデルタを照合します。

クォータと `fid -> (pid, name)` の更新は、シャードごとの補助アウトボックスを通ります。`AuxDispatcher` がメンテナンスの刻みで適用します。適用できないときは、エントリを接頭辞 `0x07` の下へあふれさせ、そのキューが Raft コンパクションを永久に止めないようにします。`TxnRecoveryWorker` は同じ刻みで `TxnRecord` / `WriteIntent` を走査します。

シャードをまたぐリネーム（および 2 つのシャードに触れる mkdir/rmdir）は、プライマリ固定の 2PC（`WriteIntent` + `TxnRecord`）を使います。ディレクトリのリネームは ID アロケータから有効な `DirRenameLease` を 1 つ取ります。最初のリースが解放されるか、その HLC 期限を過ぎるまで、2 つ目のリースは拒否されます。リース TTL は少なくとも 100 ms です。

## プロセス内のシャーディング

`maybe_split_or_isolate_shard` は、概算シャードサイズが `sharding.shard_split_threshold_bytes`（既定 4 GiB）に達したとき、または `TriggerShardSplit` が検査を強制したときに動きます。

`RaftShardSplitWorker::discover_split_key`:

- 生きているキーが少なくとも 2 つ必要です。
- クォータと二次索引のシャードはバイトの中点で分割します。
- `MetaTable` は親 ID ごとにバイトを合計します。1 つの親がそのバイトの 85% を超えるとき（`dominant_bytes * 100 > total_bytes * 85`）、またはシャードの親が 1 つだけのとき、候補は `Jumbo` です。
- それ以外は、バイトの半分に近い親 ID 境界を選びます。分割キーは PID 接頭辞なので、`_attr_` と 1 つの親の直接の子は一緒に残ります。
- その親についてルートがすでに `DualModeFineGrained` なら、分割キーは `_attr_` を除いた `(pid, name)` の中点です。

ジャンボ候補はシャードディレクトリを **移動しません**。`JumboIsolationPlanner` はルートへ `ShardHealthState::JumboIsolated { dedicated_node, dominant_pid }` を書きます。選ばれるノードは、登録済みノードのうちジャンボルートが最も少なく、次にその他のルートが最も少ないノードです。ブートストラップノードだけが登録されているときは、そのノードがこのプロセスです。

`enable_dual_mode_split` は `split_mode = DualModeFineGrained` と `dual_mode_pid` を永続化します。それ自体はキーをコピーしません。後の分割が `(pid, name)` の規則を使います。メモリ内シミュレータ（`control_plane.rs` の `CellLocalManager`）は別の型です。そのジャンボ経路は `UnsplittableJumboShard` を返し、「3 投票者へのスケール」は `BTreeMap` を書き換えるだけです。テスト `T1`–`T6` はこのシミュレータを動かします。

分割計画は `CELL_MANAGER_STATE` に格納されます。`split_crash_recovery_tests` が覆うクラッシュ点は、次の `maybe_split_or_isolate_shard` で計画を再開するか巻き戻します。子シャードのディレクトリは同じ `be/shards` ツリーの下に作られます。

## 名前より少ないことをするコントロールプレーン RPC

| RPC | このツリーでの挙動 |
| :--- | :--- |
| `RegisterNode` | このセルの `CELL_MANAGER_STATE` へ `(node_id, grpc_addr)` を入れる |
| `StoreHeartbeat` | 呼び出し元が渡したシャード記述子をルートキャッシュへ入れない。`metadbd` はこの RPC を送らない |
| `GetRoutes` | `cell_id = 0` はキャッシュ済みルートをすべて列挙する。未知で非ゼロの `cell_id` は空リストではなくエラーを返す |
| `AllocateIdBlock` | ローカルの永続アロケータから割り当てる |
| `AllocSplitPlan` / `ActivateSplit` | ローカルのセルマネージャへ転送する |
| `TriggerShardSplit` | ローカルシャードで `maybe_split_or_isolate_shard` を強制する |
| `EnableDualModeSplit` | ローカルのルートフラグを立てる。リクエストの `split_name` は無視される |
| `ScaleShardReplicas` | このセルが存在し、登録ノードが足りるときに `scale_out_replicas` を呼ぶ |
| `RegisterCell` | Stores a remote cell after its routes cover the requested range. This process does not forward those keys |
