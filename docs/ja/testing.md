# テストとベンチ

[概要](./README.md) · [English](../en/testing.md) · [Français](../fr/testing.md) · [Deutsch](../de/testing.md) · [简体中文](../zh/testing.md)

固定されたツールチェーン（Rust 1.100.0）で、リポジトリのルートから実行します。

## 検査

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` はライブラリ、統合テスト、両方のバイナリをビルドします。`[[bench]]` ターゲットも `benches/` ディレクトリもありません。このツリーに同梱される負荷生成は、下の `metadb-ctl bench` と `cargo test -p metadb-server --test stress_harness` です。

範囲を狭めた便利なコマンド:

```bash
cargo test -p metadb-server --test integration_tests
cargo test -p metadb-server --test e2e_cluster_evolution_tests
cargo test -p metadb-raft --test multi_raft_failover_tests
cargo test -p metadb-raft --test single_voter_recovery_tests
cargo test -p metadb-cluster --test epoch_split_tests
cargo test -p metadb-cluster --test dual_mode_jumbo_tests
cargo test -p metadb-cluster --test split_crash_recovery_tests
cargo test -p metadb-tx --test two_pc_crash_tests
cargo test -p metadb-tx --test aux_idempotency_tests
cargo test -p metadb-tx --test attr_reconcile_race_tests
cargo test -p metadb-proxy --test sql_templates_tests
cargo test -p metadb-storage --test lsm_recovery_tests
cargo test -p metadb-storage --test mvcc_snapshot_tests
cargo test -p metadb-storage --test locality_compaction_tests
cargo test -p metadb-server --test stress_harness
```

## 各スイートが実際に行使するもの

### メモリ内シミュレータ（`MetaDbEngine::new_single_node`）

`crates/metadb-server/tests/integration_tests.rs` の `T1`–`T6` はデータディレクトリを開きません。

| テスト | 確認すること |
| :--- | :--- |
| `test_t1_pid_boundary_sharding_preserves_directory_colocation` | 親 ID での分割がディレクトリの子を一緒に保つ |
| `test_t2_jumbo_shard_isolation_and_dual_mode_fallback` | メモリ内のジャンボエラーとデュアルモードのフォールバック |
| `test_t3_conflict_free_async_attr_aggregation_and_read_reconciliation` | フラッシュ前の dirstat から属性デルタが見える |
| `test_t4_decoupled_async_auxiliary_idempotency_and_cross_shard_rename` | クォータと索引のキュー、およびシャードをまたぐリネーム |
| `test_t5_in_memory_mvcc_and_locality_aware_compaction_on_bulk_deletes` | メモリ内バックエンドでの MVCC と局所性コンパクション |
| `test_t6_single_node_to_distributed_multi_cell_evolution_and_sql_proxy` | 予約名、SQL、空でない `rmdir`、その後 **メモリ上** のメンバー表に対する `register_node` + `scale_out_raft_replicas(3)` |

`T6` は Raft を起動せず、2 つ目のセルプロセスも起動しません。名前はシミュレータのメンバーオブジェクトを指しています。

### 永続的な単一プロセス

| テスト | 確認すること |
| :--- | :--- |
| `test_persistent_restart_lookup_dirstat_quota` | 一時ディレクトリで `open_and_serve`、停止、再オープン、lookup / dirstat / クォータ |
| `test_maintenance_tick_purges_raft_log` | 24 回の作成のあと、1 回のメンテナンス刻みが保持されていた Raft エントリを落とす |
| `test_e2e_persistent_grpc_cluster_restart_split_2pc_and_locality_compaction` | 永続プロセス 1 つ、実 gRPC、再起動、強制分割、リネーム 2PC、一括削除、クォータ、二次索引、Prometheus テキスト |

エンドツーエンドテストは、小さいフィクスチャが閾値を超えるように `shard_split_threshold_bytes = 2048` と `locality_tombstone_threshold = 20` を設定します。これらはテスト設定であり、配備の既定値ではありません。

### Raft。ただし OS プロセスは 1 つ

| テスト | 確認すること |
| :--- | :--- |
| `test_single_voter_walless_crash_recovery_and_three_watermark_pinning` | 投票者 1、WAL なし回復、パージ水位 |
| `test_suffix_only_truncate_final_crc_rejection_and_snapshot_verification` | ちぎれた末尾とチェックサム拒否 |
| `test_online_1_to_3_voter_snapshot_promotion_and_leader_failover` | プロセス内の tonic リスナー 3 つ、基本的な KV apply ハンドラ、3 投票者への `AddLearner`、元リーダの停止、フェイルオーバ後の読み取り |

フェイルオーバテストはテストプロセスの中で `127.0.0.1:0` を 3 回バインドします。OS プロセス 3 つではなく、`MetadataService` も提供しません。

### クラスタ、トランザクション、SQL、ストレージ

| テスト | 確認すること |
| :--- | :--- |
| `test_epoch_fenced_live_pid_split_and_durable_id_allocator` | エポックで囲った PID 分割と永続 ID アロケータ |
| `test_jumbo_isolation_and_three_way_dual_mode_scatter_gather_and_rmdir` | 永続ジャンボフラグ、デュアルモードのスキャッタギャザ、rmdir |
| `split_crash_recovery_tests` | 分割中の永続化失敗とクラッシュ点、その後の再オープン |
| `test_two_pc_crash_recovery_posix_overwrite_and_cycle_fencing` | 2PC 回復、上書きリネーム、循環の拒否 |
| `test_aux_outbox_spill_contiguous_frontiers_gap_rejection_and_cleanup` | `0x07` のあふれ、ギャップ、後始末 |
| `test_concurrent_attr_aggregation_read_reconciliation_and_l0_flush_barriers` | 並行する属性集約 |
| `test_all_twenty_metadata_sql_templates_and_attr_isolation` | 20 テンプレートすべて、リテラルと `?`、`_attr_` の隔離 |
| LSM / MVCC / 局所性のテスト | SSTable と `MANIFEST` の回復、`SnapshotGuard`、最下層コンパクション |

ユニットテストは `metadb-core`（`hlc`、`schema`）と `metadb-proto`（`convert`）にもあります。

## `metadb-ctl bench`

先に `metadbd` を起動します。そのあと:

```bash
cargo run -p metadb-server --bin metadb-ctl -- \
  --endpoint 127.0.0.1:7800 \
  bench --workload hotspot-create --concurrency 16 --ops 1000

cargo run -p metadb-server --bin metadb-ctl -- \
  --endpoint 127.0.0.1:7800 \
  bench --workload bulk-ttl-delete --concurrency 16 --ops 1000
```

フラグ: `--workload` は `hotspot-create`（既定）、`bulk-ttl-delete`、または `mixed` です。遠隔ワークロードでは、`--concurrency` を省略すると 16、`--ops` の既定は 1000 です。`--profile` を読むのは組み込み経路のときだけです。`--embedded` または `--workload mixed` は `--endpoint` を呼ばず、`--ops` も使いません。一時的な all-in-one ノードを起動し、プロセス内ストレスハーネスを実行します。[プロセス内ストレスハーネス](#プロセス内ストレスハーネス) を参照してください。

遠隔ワークロードについて `crates/metadb-server/src/bin/metadb-ctl.rs` の挙動:

- クライアントはディレクトリ ID を 1 つ取り、そのあと長さ `--ops` のファイル inode とファイル ID のブロックを取ります。個数や終端が溢れる払い出しは拒否します。
- `bench_pid`、`workload`、`requested_ops` を持つ JSON を 1 つ印字し、新しいディレクトリに 4096 バイトのファイルをその個数だけ作ります。
- 各フェーズは `attempted`、`succeeded`、`failed`、`transport_failed`、`application_failed`、`errors_by_kind`、レイテンシのパーセンタイル（`p50_ms`、`p99_ms`、`p999_ms`）を JSON で印字します。`succeeded` が `--ops` と等しくなければプロセスは非ゼロで終了します。
- `bulk-ttl-delete` は作成した名前を削除し、`limit = 1` でディレクトリを一覧し、そのページが空かどうかを印字します。
- 2 回目は新しい `bench_pid` を取ります。固定の inode は再利用せず、拒否された作成は成功に数えません。

このコマンドの数値をリポジトリの結果として扱わないでください。遠隔実行はマシン、ビルドプロファイル、向けたサーバに依存します。組み込み実行はマシンとビルドプロファイルに依存します。このツリーはそのどちらもサンプルとして記録していません。

## プロセス内ストレスハーネス

`crates/metadb-server/tests/stress_harness.rs` と `metadb-ctl bench --embedded` は、どちらも `metadb_server::stress`（`crates/metadb-server/src/stress.rs`）を呼びます。`mixed` という第 3 のプロファイルはありません。`--workload mixed` はこのハーネスを選びます。両方のプロファイルは同じ混合ワークロードを実行します。一時データディレクトリ上の永続 all-in-one ノードに対する create、lookup、dirstat、listdir、rename、unlink です。時間制限のループのあと、ハーネスはメンテナンスティックを実行し、成功した操作に対してディレクトリ一覧、dirstat、クォータを照合します。メンテナンスティックが失敗すると `errors` に 1 を足し、`correctness_errors` を 1 にして、その照合を飛ばします。この実行は OS プロセス 1 つとデータディレクトリ 1 つです。`metadbd` プロセス 3 つではなく、[公式のプレフィックス負荷](../en/testing.md#prefix-load) でもありません。

| プロファイル | テスト | 時間 | 並行数 | ディレクトリ数 | いつ実行されるか |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `smoke` | `stress_smoke_mixed_workload` | 5 秒 | 4 | 4 | `cargo test -p metadb-server --test stress_harness`。`cargo test --workspace` にも含まれる。`METADB_STRESS_*` は無視する。 |
| `local` | `stress_local_mixed_workload` | 上書きしなければ 30 秒 | 上書きしなければ 8 | 上書きしなければ 16 | `#[ignore]`。`cargo test --workspace` には含まれない。`METADB_STRESS_DURATION_SECS`、`METADB_STRESS_CONCURRENCY`、`METADB_STRESS_KEYSPACE`、`METADB_STRESS_SEED` を読む。 |

```bash
cargo test -p metadb-server --test stress_harness stress_smoke -- --nocapture

METADB_STRESS_DURATION_SECS=120 \
METADB_STRESS_CONCURRENCY=16 \
METADB_STRESS_KEYSPACE=32 \
METADB_STRESS_SEED=7 \
  cargo test -p metadb-server --test stress_harness stress_local -- --ignored --nocapture

cargo run -p metadb-server --bin metadb-ctl -- bench --embedded --profile smoke
cargo run -p metadb-server --bin metadb-ctl -- bench --embedded --profile local
```

`METADB_STRESS_*` は local プロファイルに適用されます。`metadb-ctl bench --embedded --profile local` も含みます。`metadb-ctl bench --embedded` では、環境変数を読んだあと `--duration-secs`、`--concurrency`、`--keyspace`、`--seed` が、選ばれたどちらのプロファイルも上書きします。`cargo test` の local プロファイルにフラグの上書きはなく、これらの変数だけを読みます。時間、並行数、ディレクトリ数が 1 未満のときは 1 に上げます。`--profile` の既定は `smoke` です。`--profile ci` は同じ smoke プロファイルです。それ以外の `--profile` はエラーで終了します。smoke プロファイルは `METADB_STRESS_*` を読みません。遠隔の `hotspot-create` と `bulk-ttl-delete` は `--profile` を無視します。

テストは 1 行を印字し、`errors = 0`、`correctness_errors = 0`、かつ `ops > 0` を要求します。`metadb-ctl bench --embedded` は `embedded metadbd listening on <addr>` と同じ行を印字し、`errors` または `correctness_errors` が 0 より大きいと非ゼロで終了します。行のフィールドは次のとおりです。

```text
metadb stress profile=<name> ops=<count> errors=<count> correctness_errors=<count> elapsed_ms=<ms> ops_per_s=<rate> latency_us p50=<us> p99=<us> max=<us>
```

`ops` は混合ループで成功した操作だけを数えます。`ops_per_s` はその数を経過時間で割ります。`errors` は混合ループで成功しなかった操作を数えます。失敗したメンテナンスティックも `errors` に加算され、照合を実行せずに `correctness_errors` を 1 にします。照合が実行されたとき、`correctness_errors` は一覧、dirstat、クォータの不一致と、失敗した照合呼び出し（`dirstat`、`listdir`、`get_quota`）の両方を数えます。`latency_us` の `p50`、`p99`、`max` は、失敗した試行を含む混合ループの全試行のレイテンシパーセンタイルで、単位はマイクロ秒です。`ops` のパーセンタイルではありません。印字されます。このツリーはレイテンシの閾値を課しません。

どちらのコマンドが印字した行も、印字したマシンの結果です。このリポジトリはそれを記録しません。サービス水準の結果ではなく、初回出荷の合格でもなく、公式の 1,000,000 作成プレフィックス負荷でもありません。

## 永続トポロジと参加

これらのコマンドはこのツリーにあります。CI は `cargo test --workspace --locked` で実行します。

- `cargo test -p metadb-server --test topology_single_node` は一時ディレクトリで `PersistentMetaDbCluster` を開きます。
- `cargo test -p metadb-server --test topology_scale_out` はそのプロセスで PID 境界の分割を強制します。
- `cargo test -p metadb-server --test topology_distributed` は 1 プロセスで `DurableCellManager::scale_out_replicas` を使い、localhost の投票者 3 つを作ります。作成負荷の下でのリーダー喪失を含みます。
- `cargo test -p metadb-server --test stress_crash_restart` は作成が `propose` の中にあるあいだ memtable をフラッシュせずに停止し、同じ `data_dir` を開き直します。
- `cargo test -p metadb-server --test join_existing_group` は 2 種類の参加を扱います。`metadbd_processes_join_existing_group_and_fail_over` は実際の `metadbd` プロセスを起動し、最初のプロセスの Raft グループに参加してフェイルオーバーします。`join_sends_the_shared_secret` はテストプロセスの中で `PersistentMetaDbCluster` を 2 つ開き、その参加で共有シークレットが送られることを確認します。このケースは `metadbd` の OS プロセスを 2 つ起動しません。
- `open_and_serve` は今も `all-in-one` 以外のロールを拒否します。2 つ目のセルは未実装のままです。`RegisterCell` はエラーを返します。

smoke プロファイルと無視される local プロファイルは [プロセス内ストレスハーネス](#プロセス内ストレスハーネス) です。`topology_distributed` は今も、localhost の Raft 投票者 3 つを持つ 1 つの OS プロセスであり、`metadbd` プロセス 3 つではありません。分離されたロールと 2 つ目のセルは、今も稼働中の配備ではありません。
