# テストとベンチ

[概要](./README.md) · [English](../en/testing.md) · [Français](../fr/testing.md) · [Deutsch](../de/testing.md) · [简体中文](../zh/testing.md)

固定されたツールチェーン（Rust 1.100.0）で、リポジトリのルートから実行します。

## 検査

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

`cargo test --workspace` はライブラリ、統合テスト、両方のバイナリをビルドします。`[[bench]]` ターゲットも `benches/` ディレクトリもありません。このツリーに同梱される負荷生成は、下の `metadb-ctl bench` です。

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

フラグ: `--workload` は `hotspot-create`（既定）、`bulk-ttl-delete`、または `mixed` です。`--concurrency` を省略すると 16 です。`--ops` の既定は 1000 です。`--embedded` または `--workload mixed` は `--endpoint` を呼ばず、一時的な all-in-one ノードとプロセス内ストレステストを起動します。

遠隔ワークロードについて `crates/metadb-server/src/bin/metadb-ctl.rs` の挙動:

- クライアントはディレクトリ ID を 1 つ取り、そのあと長さ `--ops` のファイル inode とファイル ID のブロックを取ります。個数や終端が不正な払い出しは拒否します。
- `bench_pid`、`workload`、`requested_ops` を持つ JSON を 1 つ印字し、新しいディレクトリに 4096 バイトのファイルをその個数だけ作ります。
- 各フェーズは `attempted`、`succeeded`、`failed`、`transport_failed`、`application_failed`、`errors_by_kind`、レイテンシのパーセンタイルを JSON で印字します。`succeeded` が `--ops` と等しくなければプロセスは非ゼロで終了します。
- `bulk-ttl-delete` は作成した名前を削除し、`limit = 1` でディレクトリを一覧します。
- 2 回目は新しい `bench_pid` を取ります。拒否された作成は成功に数えません。

このコマンドの数値をリポジトリの結果として扱わないでください。マシン、ビルドプロファイル、向けたサーバに依存します。このツリーはサンプルを記録していません。

## 未マージのプルリクエスト 2

[プルリクエスト 2](https://github.com/rustfs/MetaDB/pull/2) は開いており、このブランチの一部ではありません。永続トポロジのテストと組み込みストレスハーネスを追加します。その説明自身が次を述べています。

- 単一ノードのテストは一時ディレクトリで `PersistentMetaDbCluster::open_and_serve` を呼ぶ。
- スケールアウトのテストは同じプロセスで PID 境界の分割を強制する。
- 分散テストは、localhost の Raft 投票者 3 つを持つ 1 プロセスである。`DurableCellManager::scale_out_replicas` を呼ぶ。`metadbd` の OS プロセスを 3 つ起動しない。
- `open_and_serve` は今も `all-in-one` 以外のロールを拒否する。分離されたロールと 2 つ目のセルは未実装のままである。

そのプルリクエストの報告は、著者の VM 上の Rust 1.100.0（ハードウェア名は無い）で、このブランチに無いハーネスのスモーク行を 1 つ含みます。

```text
metadb stress profile=smoke ops=12219 errors=0 correctness_errors=0 elapsed_ms=5221 ops_per_s=2340.3 latency_us p50=1318 p99=5207 max=12449
```

この行は未マージテストの 1 回の実行です。サービスの目標値ではなく、このブランチの `metadb-ctl bench` では再現しません。`cargo test -p metadb-server --test topology_distributed` や `metadb-ctl bench --embedded` のようなコマンドは、そのプルリクエストのブランチにだけあります。
