# 運用

[概要](./README.md) · [English](../en/operations.md) · [Français](../fr/operations.md) · [Deutsch](../de/operations.md) · [简体中文](../zh/operations.md)

## データディレクトリ

`bootstrap_table_shards = true` でブートストラップしたあと:

```text
<data_dir>/
  global_allocator/ID_ALLOCATOR_STATE
  cell_<cell_id>/CELL_MANAGER_STATE
  be/NODE_SHARD_REGISTRY
  be/shards/00000000000000000001/lsm/     # MetaTable
  be/shards/00000000000000000001/raft/
  be/shards/00000000000000000002/...      # QuotaTable
  be/shards/00000000000000000003/...      # SecondaryIndexTable
```

シャード ID は 10 進で、20 桁になるまでゼロ埋めされます。分割で作られた子シャードは `be/shards/` の下の追加ディレクトリです。`CELL_MANAGER_STATE` と `ID_ALLOCATOR_STATE` は CRC32C フレームです。シャードディレクトリが残っているあいだ `NODE_SHARD_REGISTRY` を消さないでください。ホストは分割の子を回復するためにこれを使います。

ディレクトリをコピーする前に、Ctrl-C（または `PersistentMetaDbCluster::shutdown`）でプロセスを止めてください。生きているプロセスはシャードごとに LSM の `LOCK` ファイルを保持します。

## `metadb-ctl`

グローバルフラグ:

| フラグ | 既定 | 意味 |
| :--- | :--- | :--- |
| `--endpoint` | `127.0.0.1:7800` | `ホスト:ポート`、またはすでに `http://` か `https://` で始まる URL |
| `--auth-token` | 空 | ベアラトークン。`METADB_AUTH_TOKEN` からも読む |

クライアントはコマンドごとに新しい gRPC チャネルを開きます。`RaftTransportService` は話しません。

### `cluster status`

`cell_id = 0` で `GetRoutes` を呼びます。各シャードについて `shard`、`cell`、`leader`、投票者数、`epoch=(conf_ver, ver)`、`dual_mode`、`jumbo` を印字します。空きディスク、QPS、Raft 水位は印字しません。

### `shard list --cell-id <u64>`

そのセルの `GetRoutes` を呼びます（キャッシュされたすべてのルートには `0`）。`shard`、`cell`、生の開始キーと終了キー、エポック `(conf_ver, version)` を印字します。

### `shard split --shard-id <u64>`

`TriggerShardSplit` を呼び、シャードが `shard_split_threshold_bytes` 未満でも分割検査を強制します。`split_executed` と `child_shard_id` を印字します。`split_executed=false` は、ワーカーが境界を見つけなかったことを意味します（たとえばキーが 2 つ未満、またはルートフラグを反転するだけのジャンボ候補）。

### `shard enable-dual-mode --shard-id <u64> --pid <u64>`

`EnableDualModeSplit` を呼びます。CLI は空の `split_name` を送ります。サーバは `dual_mode_pid` を保存し、すぐには分割しません。

### `raft scale-replicas --cell-id <u64> --target-voters <u32>`

`ScaleShardReplicas` を呼びます。サーバは、そのセルのノード表にすでに `target_voters` 件あることを要求します。新しい all-in-one セルのノードは 1 つなので、`--target-voters 3` は `need 3 registered nodes in cell <id>, have 1` の形の合意エラーを返します。成功時は、メンバー変更へ渡したローカルシャードの数を印字します。

### `cell register --cell-id <u64> --endpoints <a,b,c>`

`RegisterCell` を呼びます。サーバはアプリケーションエラーを返し、セルを追加しません。`metadb-ctl` は非ゼロで終了し、`Registered Cell` を印字しません。連合の手順として使わないでください。

### `sql <文>`

バインドパラメータなしで `ExecuteSql` を呼びます。文は [アーキテクチャ](./architecture.md) の 20 テンプレートの 1 つで、`?` の代わりにリテラルを使います。

```bash
metadb-ctl --endpoint 127.0.0.1:7800 sql \
  "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
```

### `bench`

[テストとベンチ](./testing.md#metadb-ctl-bench) を参照してください。これは起動中のサーバに対するクライアントです。Cargo のベンチターゲットではありません。このリポジトリは、このコマンドのスループット数値を記録していません。

## メトリクス

`node.metrics_listen_addr` を設定します（例: `127.0.0.1:9100`）。`open_and_serve` がそのアドレスで TCP 接続を受けます。`GET /metrics` は Prometheus テキストを返します。他のパスは 404、他のメソッドは 405 です。`[security].shared_secret` が空でないとき、このリスナーのすべての要求は `Authorization: Bearer <shared_secret>` を送らなければなりません。欠落、空、または誤ったベアラは HTTP 401 を返し、本体はメトリクス本文ではありません。`/status` への要求も同じです。An empty secret does not start the process on any address, including loopback, unless `allow_unauthenticated = true`. That flag is loopback-only and still refuses a wildcard or other non-loopback address. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. This listener does not terminate TLS.

`render_prometheus_metrics` のゲージとカウンタ:

| 名前 | 種類 |
| :--- | :--- |
| `metadb_cells_total` | gauge |
| `metadb_shards_total` | gauge |
| `metadb_in_memory_mvcc_writes_total` | counter |
| `metadb_pruned_mvcc_versions_total` | counter |
| `metadb_persistent_kv_writes_total` | counter |
| `metadb_active_physical_tombstones` | gauge |
| `metadb_tombstones_traversed_on_scans_total` | counter |
| `metadb_locality_compactions_total` | counter |
| `metadb_min_raft_safe_purge_index` | gauge |

`metadb_raft_watermark_lag_entries` という系列はありません。`metadb_cells_total` は、このプロセス内に登録されたセルを数えます。`open_and_serve` はそれを 1 にします。

## スケールアウト

1 プロセスの中で動くもの:

- シャードを増やす。`shard_split_threshold_bytes` を超える成長、または `metadb-ctl shard split` は、同じ `be/shards` ディレクトリの下にシャードグループを追加できます。親は左側のキー範囲を保持し、子はこのノード上の別の 1 投票者グループとして起動します。再起動は両方を開き直します。
- デュアルモードフラグ。`shard enable-dual-mode` は 1 つの親 ID を印します。そのシャードの後の分割は `(pid, name)` で切ることがあります。
- ジャンボフラグ。1 つの親がシャード上の `MetaTable` バイトの 85% を超えるとき、またはシャードの親が 1 つのとき、ルートは `JumboIsolated` になります。専用ノード ID は、このセルにすでに登録されたノードから選ばれます。ノードが 1 つのとき、フラグはこのプロセスを指します。ファイルは別ホストへ移動しません。
- プロセス内の投票者。`DurableCellManager::scale_out_replicas` は、それらのノード ID が登録されており、**かつ** 各アドレスがそのシャードのグループに対してすでに `RaftTransportService` を提供しているときに、learner を追加して昇格できます。`crates/metadb-raft/tests/multi_raft_failover_tests.rs` は、1 つのテストプロセス内の 3 つのリスナーと基本的な KV ステートマシンでこれを行います。メタデータのステートマシンではなく、`metadbd` を 3 プロセス起動するのでもありません。

運用手順として動かないもの:

1. `metadbd --role be`、`--role proxy`、`--role cell-manager`、`--role global-manager` は `open_and_serve` で終了します。サブシステムはロールで切り出されず、プロセスはそれらの名前で起動することを拒否します。
2. 2 つ目の `metadbd --role all-in-one` は、データディレクトリが空で `--join` または `--cell-manager` が現在のリーダーを指すとき、既存の Raft グループに参加します。リーダーは空エントリを追記してスナップショットを取り、参加者を learner から voter へ昇格します。`--join` はそのリーダーに届く必要があります。すでに Raft 状態があるディレクトリはそれを開き直し、新しい join アドレスを無視します。ノード ID 2 から 32767 の新しいアロケータは、そのノード自身の inode、file id、shard id 名前空間から始まるため、ノード 1 が既に発行した ID を再発行しません。既に低い ID を発行したアロケータファイルは移し替えません。リーダーが分かっている follower への `Lookup`、ディレクトリ一覧、ディレクトリ統計、`GetDirPartitionStat`、クォータ読み取り、セカンダリインデックス読み取りは、そのリーダーへ転送します。リーダーが不明な follower は引き続き `NotLeader` を返します。メンバーシップが joint の間、`GetRoutes` は適用済みの全投票者、現在のリーダー、適用済み epoch を報告します。均一構成がコミットされた後、それらの投票者と `conf_ver` は Raft と一致します。参加は一時的なエラーを 45 秒間再試行し、その後リスナーとローカル Raft グループを止めてエラーを返します。`metadbd` が終わるのは、起動がそのエラーを返したためです。最初の失敗だけではプロセスを中断しません。
3. `metadb-ctl cell register` は 2 つ目のセルを作りません。`open_and_serve` はグローバルマネージャへローカルセルだけを登録します。
4. テスト `T1`–`T6` が使うメモリ内の `MetaDbEngine::new_single_node` は、メモリ上の投票者数を 3 に増やせます。そのオブジェクトにはディスクも Raft もありません。`metadbd` のスケールアウトではありません。

[未マージのプルリクエスト 2](https://github.com/rustfs/MetaDB/pull/2) は同じ境界を述べています。その分散テストは、1 つの OS プロセスの中で localhost の Raft 投票者 3 つに対し `DurableCellManager::scale_out_replicas` を呼びます。`metadbd` を 3 プロセス起動しません。分離されたロールと 2 つ目のセルも、そこでは稼働中の配備としてはテストされていません。そのプルリクエストのジャンボ専用ノード隔離は、メモリ内シミュレータの挙動のままです。

## 遭遇しうるエラー

変種は `crates/metadb-core/src/error.rs` で定義されています。gRPC メソッドは通常、非 OK ステータスではなく応答の `error` フィールドに変種を返します。

| 変種 | コードが上げるとき |
| :--- | :--- |
| `ReservedEntryName` | 空の名前、NUL、またはユーザエントリとしての `_attr_` |
| `AlreadyExists` / `NotFound` | `(pid, name)` の作成または検索 |
| `DirectoryNotEmpty` | 子が残っている `rmdir`。属性フラッシュの前も含む |
| `InvalidEntryType` | ファイルへの `rmdir`、またはディレクトリへの `unlink` |
| `TransactionTimeout` | スナップショット HLC がメモリ内 MVCC 窓より古い |
| `ClockSkewExceeded` | 遠隔 HLC がローカルの壁時計より `max_clock_drift_ms` を超えて進んでいる |
| `EpochMismatch` / `NotLeader` | 古いルートエポック、またはこのレプリカがリーダーではない。リーダーが分かっている follower の読み取りは転送される。複数キーの Prepare での `EpochMismatch` はそのラウンドを中止して再グループ化し、同じバッチを再提案しない |
| `RetryableConsensus` | リーダー交代で書き込みログが破棄された、またはクォーラムが応答しなかった。同一シャードの書き込みと 2PC は既存のルーティング予算内で再試行する。一つの論理書き込みは同じ提案 ID を使う。その ID が既にコミット済みなら、シャードは元の結果を返し、コマンドを再実行しない。ストレージ障害と停止した Raft ノードは `ConsensusError` のままで再試行しない |
| `UnsplittableJumboShard` | メモリ内スプリッタ（`shard.rs`）。永続ジャンボ経路ではない。永続経路は `JumboIsolated` を立て、子シャードなしで成功を返す |
| `WriteIntentConflict` / `TransactionAborted` | 2PC の衝突、またはディレクトリリネームのリースがすでに保持されている |
| `InvalidRenameCycle` | ディレクトリを自分自身または子孫へリネームする |
| `ConsensusError` | 登録ノード不足、learner の追いつきタイムアウト（10 秒）、または OpenRaft のメンバーシップエラー |
| `SqlParseError` | 文が 20 テンプレートのどれでもない |

未実装ロールの拒否と不正な TOML も `CodecError` です。

## 既知の制限

- OS プロセスは 1 つ、ロールは `all-in-one` だけ。
- 2 つ目の all-in-one プロセスは `[cluster] join` で同じセルに参加できる。これは 2 つ目のセルを作らない。`RegisterCell` はエラーを返す。
- 未知で非ゼロの `cell_id` に対する `GetRoutes` は失敗して閉じる。`StoreHeartbeat` はキャッシュ済みルートを置き換えない。
- ジャンボ隔離はルートフラグと、ローカルノード表から選んだノード ID である。他のシャードをディスクから追い出すことはしない。
- ハートビートは RPC であり、`metadbd` に送信側はない。`StoreHeartbeat` は永続化しない。
- A non-empty shared secret authenticates Raft, the other three gRPC services, and the metrics listener with the same bearer. An empty secret does not start the process, including on loopback. `allow_unauthenticated = true` is the explicit opt-in for empty-secret loopback development and still refuses a wildcard or any other non-loopback bind. A whitespace-only secret is rejected even with that opt-in. A usable secret combined with the opt-in is rejected. The process does not terminate TLS.
- `memtable_size_bytes` はアクティブな memtable を封印してフラッシュする。`max_imm_memtables` はキューに積まれた不変テーブルの数を抑える。Raft のタイマ項目は、TOML 経由で実行時の挙動を変えない。
- `hotspot-create` と `bulk-ttl-delete` の `metadb-ctl bench` は JSON 行を印字する。`bench_pid` を含むヘッダのあと、attempted、succeeded、failed とレイテンシのパーセンタイルを持つフェーズ報告が続く。要求した操作がすべて成功しない限り、プロセスは非ゼロで終了する。`--workload mixed` または `--embedded` はプロセス内のストレステストを実行する。`sql` は応答を JSON で印字し、アプリケーションエラーでは非ゼロで終了する。

## 文書化できない欠落

次の項目は製品に無いため、このガイドは手順を書けません。

1. `be`、`proxy`、`cell-manager`、`global-manager` の複数プロセス配置。
2. 既存の Raft グループへ 2 つ目と 3 つ目の `metadbd` OS プロセスを加えること。
3. 2 つ目のセルを作り、そこへルーティングすること。
4. ジャンボシャードを専用マシンに置き、他のシャードをそのマシンから移すこと。
5. プロセス内の TLS。このプロセスは TLS を終端しない。TLS や証明書を求める設定キーは、平文で提供する代わりに拒否される。終端は外部プロキシに置く。シークレットがあるとき、Raft とメトリクスは他のサービスと同じベアラを使う。これは相互 TLS ではない。ハートビート駆動の制御ループは未実装である。
6. 運用者が設定して観測できる Raft タイマ。
7. `metadb-ctl bench` のチェックインされたサービス水準の結果。コマンドは実行したマシンの数値を印字します。このリポジトリはそれを記録していません。未マージのプルリクエスト 2 は別の組み込みハーネスを報告しています。それらのコマンドはこのツリーにはありません。 [テストとベンチ](./testing.md) を参照してください。
