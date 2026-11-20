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

`cell_id = 0` で `GetRoutes` を呼びます。各シャードについて `shard`、`cell`、`leader`、投票者数、`epoch=(conf_ver, ver)`、`dual_mode`、`jumbo` を印字します。It then prints local `CELL_CONTROL_PLANE` placement rows (`phase`, `phase_code`, `dedicated_node`, `dominant_pid`, `moves`, attempt timestamps, `reason`) and `placement_automatic_transfer_enabled=false`. Those rows are not routes. A remote cell contributes no placement rows. It does not print disk free space, QPS, Raft watermarks, or `MOVE_STAGED` / `MOVE_LIVE`. The reconciler does not publish phase `recorded`. It does not stamp `transfer_attempt_unix_ms`. A new row stores 0. A later reconcile keeps a non-zero value already in the file.

### `shard list --cell-id <u64>`

そのセルの `GetRoutes` を呼びます（キャッシュされたすべてのルートには `0`）。`shard`、`cell`、生の開始キーと終了キー、エポック `(conf_ver, version)` を印字し、その後に同じローカル placement 行を印字します。A remote cell prints `Placements: 0`.

### `shard split --shard-id <u64>`

`TriggerShardSplit` を呼び、シャードが `shard_split_threshold_bytes` 未満でも分割検査を強制します。On success it prints `split_executed`, `child_shard_id`, and the placement fields. When no placement row exists, including an ordinary split, the line is `placement_phase=` and `placement_phase_code=0`. Code 0 is only that no-record case. Stored rows use codes 1–4. `split_executed=false` は子シャードを作らなかったことを意味します（キーが 2 つ未満、またはジャンボ候補が placement 行を書いても `split_executed=false` のまま）。

### `shard enable-dual-mode --shard-id <u64> --pid <u64>`

`EnableDualModeSplit` を呼びます。CLI は空の `split_name` を送ります。サーバは `dual_mode_pid` を保存し、すぐには分割しません。

### `raft scale-replicas --cell-id <u64> --target-voters <u32>`

`ScaleShardReplicas` を呼びます。サーバは、そのセルのノード表にすでに `target_voters` 件あることを要求します。新しい all-in-one セルのノードは 1 つなので、`--target-voters 3` は `need 3 registered nodes in cell <id>, have 1` の形の合意エラーを返します。成功時は、メンバー変更へ渡したローカルシャードの数を印字します。

### `cell register --cell-id <u64> --endpoints <a,b,c>`

Calls `RegisterCell`. The server fsyncs `GLOBAL_CELL_DIRECTORY` only when the endpoint's routes cover the requested range exactly. Omitting the keys sends `[01, 04)`, which overlaps a default cell, and `metadb-ctl` exits non-zero. The caller does not forward keys.

### `sql <文>`

バインドパラメータなしで `ExecuteSql` を呼びます。文は [アーキテクチャ](./architecture.md) の 20 テンプレートの 1 つで、`?` の代わりにリテラルを使います。

```bash
metadb-ctl --endpoint 127.0.0.1:7800 sql \
  "SELECT * FROM MetaTable WHERE pid = 1 AND name = '_attr_'"
```

### `bench`

See [Tests and benches](./testing.md#metadb-ctl-bench). Remote `hotspot-create` and `bulk-ttl-delete` are a client against a running server. They print JSON with `attempted`, `succeeded`, and `failed`, and the process exits non-zero unless `succeeded` equals `--ops`. `--embedded` and `--workload mixed` run the [in-process stress harness](./testing.md#in-process-stress-harness) instead of dialing the endpoint. That path passes when `errors` and `correctness_errors` are both zero, and it prints latency percentiles in microseconds. Neither path is a Cargo benchmark target. The repository does not check in a throughput number for either path. A printed line is not a service-level result and is not the official 1,000,000-create profile.

The separate prefix-load harness (100 prefixes × 10,000 creates, then 100,000 `SetAttr` overwrites, then 100,000 deletes, concurrency 64) is documented in [Tests and benches](../en/testing.md#prefix-load). GitHub Actions runs only the small profile. It does not run the 1,000,000-create profile.

## メトリクス

`node.metrics_listen_addr` を設定します（例: `127.0.0.1:9100`）。`open_and_serve` がそのアドレスで TCP 接続を受けます。`GET /metrics` は Prometheus テキストを返します。他のパスは 404、他のメソッドは 405 です。`[security].shared_secret` が空でないとき、このリスナーのすべての要求は `Authorization: Bearer <shared_secret>` を送らなければなりません。欠落、空、または誤ったベアラは HTTP 401 を返し、本体はメトリクス本文ではありません。`/status` への要求も同じです。An empty secret does not start the process on any address, including loopback, unless `allow_unauthenticated = true`. That flag is loopback-only and still refuses a wildcard or other non-loopback address. A whitespace-only secret is rejected even with the flag. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.

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
| `metadb_placement_records` | gauge, local placement rows |
| `metadb_placement_phase_recorded` | gauge; the reconciler does not publish this phase |
| `metadb_placement_phase_leader_on_dedicated` | gauge |
| `metadb_placement_phase_dedicated_not_shared` | gauge |
| `metadb_placement_phase_blocked` | gauge |
| `metadb_placement_moves_leadership` | gauge |
| `metadb_placement_moves_voter_replaced` | gauge |
| `metadb_placement_rows_with_transfer_attempt` | gauge; the reconciler does not stamp a new value and keeps one already in the file |
| `metadb_placement_rows_with_replace_attempt` | gauge |
| `metadb_placement_automatic_transfer_enabled` | gauge, 0 in this build |

There is no `metadb_raft_watermark_lag_entries` series and no per-shard placement label. There is no `MOVE_STAGED` or `MOVE_LIVE` series. `metadb_cells_total` counts rows in this process's `GLOBAL_CELL_DIRECTORY`, including a remote cell after `RegisterCell`. `metadb_shards_total` counts only local shards. A fresh process has one cell. The English operability page lists the readiness gauges that this table does not repeat.

## スケールアウト

1 プロセスの中で動くもの:

- シャードを増やす。`shard_split_threshold_bytes` を超える成長、または `metadb-ctl shard split` は、同じ `be/shards` ディレクトリの下にシャードグループを追加できます。親は左側のキー範囲を保持し、子はこのノード上の別の 1 投票者グループとして起動します。再起動は両方を開き直します。
- デュアルモードフラグ。`shard enable-dual-mode` は 1 つの親 ID を印します。そのシャードの後の分割は `(pid, name)` で切ることがあります。
- ジャンボフラグ。1 つの親がシャード上の `MetaTable` バイトの 85% を超えるとき、またはシャードの親が 1 つのとき、ルートは `JumboIsolated` になります。専用ノード ID は、このセルにすでに登録されたノードから選ばれます。ノードが 1 つのとき、フラグはこのプロセスを指します。フラグ自体はファイルを別ホストへ移しません。ループバックの `MOVE_STAGED` / `MOVE_LIVE` は `127.0.0.1` のリスナーへディレクトリを置けます。`metadbd` はそのリスナーを起動せず、Raft を開かず、`transfer_leader` を呼ばず、本番のホスト間コピーではありません。`docs/en/scale-control-plane-status.md` を参照。
- プロセス内の投票者。`DurableCellManager::scale_out_replicas` は、それらのノード ID が登録されており、**かつ** 各アドレスがそのシャードのグループに対してすでに `RaftTransportService` を提供しているときに、learner を追加して昇格できます。`crates/metadb-raft/tests/multi_raft_failover_tests.rs` は、1 つのテストプロセス内の 3 つのリスナーと基本的な KV ステートマシンでこれを行います。メタデータのステートマシンではなく、`metadbd` を 3 プロセス起動するのでもありません。

運用手順として動かないもの:

1. `metadbd --role be` hosts local Raft replicas and serves their metadata. It does not publish a cell or mint ids. `metadbd --role proxy` forwards metadata and transactions to one `[cluster] upstream` and does not host Raft. `--role global-manager` serves `GLOBAL_CELL_DIRECTORY` only and does not open Raft groups. `--role cell-manager` hosts one local cell and its Raft groups, and publishes that cell when `[cluster] global_manager` is set. The directory file is not a Raft group, and the global process does not forward keys.
2. 2 つ目の `metadbd --role all-in-one` は、データディレクトリが空で `--join` または `--cell-manager` が現在のリーダーを指すとき、既存の Raft グループに参加します。リーダーは空エントリを追記してスナップショットを取り、参加者を learner から voter へ昇格します。`--join` はそのリーダーに届く必要があります。すでに Raft 状態があるディレクトリはそれを開き直し、新しい join アドレスを無視します。ノード ID 2 から 32767 の新しいアロケータは、そのノード自身の inode、file id、shard id 名前空間から始まるため、ノード 1 が既に発行した ID を再発行しません。既に低い ID を発行したアロケータファイルは移し替えません。リーダーが分かっている follower への `Lookup`、ディレクトリ一覧、ディレクトリ統計、`GetDirPartitionStat`、クォータ読み取り、セカンダリインデックス読み取りは、そのリーダーへ転送します。リーダーが不明な follower は引き続き `NotLeader` を返します。メンバーシップが joint の間、`GetRoutes` は適用済みの全投票者、現在のリーダー、適用済み epoch を報告します。均一構成がコミットされた後、それらの投票者と `conf_ver` は Raft と一致します。参加は一時的なエラーを 45 秒間再試行し、その後リスナーとローカル Raft グループを止めてエラーを返します。`metadbd` が終わるのは、起動がそのエラーを返したためです。最初の失敗だけではプロセスを中断しません。
3. `metadb-ctl cell register` records a second cell only after that cell's routes cover the requested range. This process does not forward those keys. `open_and_serve` still starts one local cell.
4. テスト `T1`–`T6` が使うメモリ内の `MetaDbEngine::new_single_node` は、メモリ上の投票者数を 3 に増やせます。そのオブジェクトにはディスクも Raft もありません。`metadbd` のスケールアウトではありません。

`topology_distributed` still uses `DurableCellManager::scale_out_replicas` with localhost voters inside one OS process. `join_existing_group` starts real `metadbd` processes and joins them. The shared-secret case in that file opens two clusters inside the test process; it does not start two `metadbd` OS processes. Disaggregated roles are still not a live deployment. A second cell can be recorded in this process's directory; that is not a production multi-cell deployment. This process can copy its own jumbo shard directory onto `jumbo_dedicated_root`. A loopback `MOVE_STAGED` / `MOVE_LIVE` placement is specified in `docs/en/scale-control-plane-status.md`. It does not call `transfer_leader`, does not replicate the cell directory, and is not a production move.

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

- `all-in-one`, `cell-manager`, `global-manager`, `be`, and `proxy` start on a valid config. `be` hosts local replicas and does not mint ids. `proxy` forwards to one upstream and does not host Raft. `global-manager` has no local shards and does not forward. `cell-manager` still colocates its Raft groups.
- A second all-in-one process can join the same cell with `[cluster] join`. That does not create a second cell. `RegisterCell` records a different cell only after its routes cover the requested range, and this process does not forward keys.
- 未知で非ゼロの `cell_id` に対する `GetRoutes` は失敗して閉じる。`StoreHeartbeat` はキャッシュ済みルートを置き換えない。
- ジャンボ隔離はルートフラグと、ローカルノード表から選んだノード ID である。他のシャードをディスクから追い出すことはしない。
- Registered-node liveness is fsynced in `CELL_CONTROL_PLANE`. `metadbd` persists its own record about every 3 seconds and sends it to the current leader when this process is not the leader. Descriptors on that RPC are annotations, not routes. The observation does not schedule placement. See [Scale control plane](../en/scale-control-plane-status.md).
- A secret that contains a non-whitespace character authenticates Raft, the other three gRPC services, and the metrics listener, including `/livez`, `/readyz`, and `/healthz`, with the same bearer. An empty secret does not start the process. `allow_unauthenticated = true` is the explicit exception for literal loopback IPs (`127.0.0.0/8` or `::1`) and still refuses a wildcard bind. The hostname `localhost` is not accepted for that exception. The bound socket is checked again after listen. A whitespace-only secret is rejected and is not compared as a bearer. A usable secret combined with the flag is rejected. When `[security.tls].mode` is `off`, listeners are plaintext. `tls` and `mtls` are refused before listen. This process does not terminate TLS. See `docs/designs/transport_security.md`.
- `memtable_size_bytes` はアクティブな memtable を封印してフラッシュする。`max_imm_memtables` はキューに積まれた不変テーブルの数を抑える。`[raft]` timers and `max_segment_bytes` apply when a group starts. They are not reloaded from a changed file, and the segment size does not rewrite segments already on disk.
- `hotspot-create` と `bulk-ttl-delete` の `metadb-ctl bench` は JSON 行を印字する。`bench_pid` を含むヘッダのあと、attempted、succeeded、failed とレイテンシのパーセンタイルを持つフェーズ報告が続く。要求した操作がすべて成功しない限り、プロセスは非ゼロで終了する。`--workload mixed` または `--embedded` はプロセス内のストレステストを実行する。`sql` は応答を JSON で印字し、アプリケーションエラーでは非ゼロで終了する。

## 文書化できない欠落

次の項目は製品に無いため、このガイドは手順を書けません。

1. `be`、`proxy`、`cell-manager`、`global-manager` の複数プロセス配置。
2. A second Cell. `RegisterCell` stays an error. Join reaches only the current leader; a follower does not forward it.
3. ジャンボシャードを専用マシンに置き、他のシャードをそのマシンから移すこと。ループバックのリスナーは `MOVE_STAGED` / `MOVE_LIVE` でジャンボディレクトリを受け取れます。それは `metadbd` ではなく、認証もなく、Raft を開かず、`transfer_leader` を呼ばず、別ホストへの本番コピーではありません。
4. プロセス内の TLS。このプロセスは TLS を終端しない。TLS や証明書を求める設定キーは、平文で提供する代わりに拒否される。終端は外部プロキシに置く。シークレットがあるとき、Raft とメトリクスは他のサービスと同じベアラを使う。これは相互 TLS ではない。ハートビート駆動の制御ループは未実装である。
5. A gRPC health RPC, probes when `metrics_listen_addr` is unset, and per-shard Prometheus series. `/readyz` does not wait for the 250 ms 2PC recovery pass. A leader election makes `/readyz` and `/healthz` return 503 on replicas that do not know a leader. `[raft]` is loaded and is not `#[serde(skip)]`. Its values apply when a group starts and are not reloaded. See [Operability](../en/operability.md).
6. A checked-in service-level result for `metadb-ctl bench`. The command prints numbers for the machine you run it on. This repository does not record one. See [Tests and benches](./testing.md).
