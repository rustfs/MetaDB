//! Jepsen (`https://jepsen.io/`) Integration Test Suite for MetaDB.
//!
//! Verifies MetaDB against real multi-process `metadbd` OS clusters and 3-voter `Swizzle-Clog`
//! network-partitioned clusters using formal Jepsen checkers:
//! - Wing-Gong-Lowe (WGL / Knossos / Porcupine) exact linearizability checker (`register`)
//! - Elle Adya DSG transactional consistency checker (`G0`, `G1a`, `G1b`, `G1c`, `G-single`,
//!   `G2-item`, and `Strict-Serializability` `rt` cycles) (`rename-elle`)
//! - Jepsen `set-full` zero-lost-acknowledged-writes checker (`set`)
//! - Jepsen `bank` exact multi-shard / `QuotaTable` / `DirStat` conservation checker (`bank`)
//! - Real OS-process nemeses (`SIGKILL` + restart, `SIGSTOP`/`SIGCONT` leader freeze, online
//!   `TriggerShardSplit`, `TransferShardLeader`) and `Swizzle-Clog` network partitions
//! - Standard Clojure Jepsen `history.edn` export verification

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;
use std::sync::OnceLock;
use tempfile::tempdir;
use tokio::sync::Mutex;

use metadb_server::jepsen::{
    check_bank_conservation, check_elle_graph, check_elle_rename_history,
    check_register_linearizability, check_set_full, run_os_cluster_jepsen, run_partition_jepsen,
    ElleAnomalyKind, ElleMicroOp, ElleTransaction, HistoryRecorder, JepsenConfig,
    JepsenNemesisKind, JepsenOpKind, JepsenOpValue, JepsenProcess, JepsenWorkload,
};

fn jepsen_cluster_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

fn metadbd_exe() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_metadbd"))
}

#[test]
fn jepsen_checkers_catch_injected_linearizability_elle_set_and_bank_faults() {
    // 1. WGL catches stale read (non-linearizable history) & accepts valid concurrent history.
    let bad_lin = HistoryRecorder::new();
    bad_lin.invoke_at(
        10,
        JepsenProcess::Client(0),
        JepsenOpKind::Write { key: 0, value: 7 },
    );
    bad_lin.complete_ok_at(
        20,
        JepsenProcess::Client(0),
        JepsenOpKind::Write { key: 0, value: 7 },
        JepsenOpValue::Int(7),
    );
    bad_lin.invoke_at(30, JepsenProcess::Client(1), JepsenOpKind::Read { key: 0 });
    bad_lin.complete_ok_at(
        40,
        JepsenProcess::Client(1),
        JepsenOpKind::Read { key: 0 },
        JepsenOpValue::OptInt(Some(0)),
    );
    let lin_res = check_register_linearizability(&bad_lin.snapshot(), &BTreeMap::from([(0, 0)]));
    assert!(!lin_res.linearizable, "WGL must reject stale read");

    // 2. Elle catches G-single (read skew: T1 writes x=1,y=1; T2 reads x=None, y=1).
    let g_single = check_elle_graph(
        &[
            ElleTransaction {
                txn_id: 1,
                invoke_ns: 1,
                complete_ns: 20,
                committed: true,
                failed: false,
                ops: vec![
                    ElleMicroOp::Write { key: 1, value: 1 },
                    ElleMicroOp::Write { key: 2, value: 1 },
                ],
            },
            ElleTransaction {
                txn_id: 2,
                invoke_ns: 5,
                complete_ns: 15,
                committed: true,
                failed: false,
                ops: vec![
                    ElleMicroOp::Read {
                        key: 1,
                        value: None,
                    },
                    ElleMicroOp::Read {
                        key: 2,
                        value: Some(1),
                    },
                ],
            },
        ],
        &BTreeMap::new(),
    );
    assert!(!g_single.valid);
    assert_eq!(g_single.anomalies[0].kind, ElleAnomalyKind::GSingle);

    // 3. Elle rename checker catches G1b vanished token `(None, None)`.
    let bad_ren = HistoryRecorder::new();
    let rkind = JepsenOpKind::TxnReadPair {
        txn_id: 1,
        slot_a: 100,
        slot_b: 600,
    };
    bad_ren.invoke_at(10, JepsenProcess::Client(0), rkind.clone());
    bad_ren.complete_ok_at(
        20,
        JepsenProcess::Client(0),
        rkind,
        JepsenOpValue::PairRead {
            val_a: None,
            val_b: None,
        },
    );
    let ren_res = check_elle_rename_history(&bad_ren.snapshot(), 1);
    assert!(!ren_res.valid);
    assert_eq!(ren_res.anomalies[0].kind, ElleAnomalyKind::G1b);

    // 4. `set-full` catches phantom/unexpected item & `bank` catches negative balance.
    let set_rec = HistoryRecorder::new();
    set_rec.invoke_at(
        1,
        JepsenProcess::Client(0),
        JepsenOpKind::SetAdd { item: 5 },
    );
    set_rec.complete_fail_at(
        2,
        JepsenProcess::Client(0),
        JepsenOpKind::SetAdd { item: 5 },
        "aborted",
    );
    let sf = check_set_full(&set_rec.snapshot(), &BTreeSet::from([5]));
    assert!(!sf.valid);
    assert_eq!(sf.unexpected, vec![5]);

    let bank_rec = HistoryRecorder::new();
    bank_rec.invoke_at(1, JepsenProcess::Client(0), JepsenOpKind::BankReadAll);
    bank_rec.complete_ok_at(
        2,
        JepsenProcess::Client(0),
        JepsenOpKind::BankReadAll,
        JepsenOpValue::Balances(vec![(1, 1200), (2, -200)]),
    );
    let bk = check_bank_conservation(&bank_rec.snapshot(), 1000);
    assert!(!bk.valid);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn jepsen_os_cluster_register_linearizability_under_leader_pause_and_sigkill() {
    let _serial = jepsen_cluster_lock().lock().await;
    let dir = tempdir().unwrap();
    let edn_path = dir.path().join("register_pause.edn");

    // Nemesis 1: Real OS `SIGSTOP`/`SIGCONT` leader freeze (stale-leader attack!)
    let report_pause = run_os_cluster_jepsen(&JepsenConfig {
        workload: JepsenWorkload::Register,
        nemesis: JepsenNemesisKind::PauseFreeze,
        seed: 0x4A01,
        clients: 4,
        duration_secs: 4,
        edn_output_path: Some(edn_path.clone()),
        metadbd_bin: Some(metadbd_exe()),
    })
    .await;
    println!("{report_pause}");
    assert!(report_pause.is_ok(), "{report_pause}");
    assert!(report_pause.ok_ops >= 10, "{report_pause}");
    assert!(report_pause.nemesis_actions >= 1, "{report_pause}");

    let edn_text = std::fs::read_to_string(&edn_path).expect("read exported history.edn");
    assert!(
        edn_text.contains(":type :invoke")
            && edn_text.contains(":type :ok")
            && edn_text.contains(":process :nemesis"),
        "exported EDN missing expected Jepsen entries:\n{}",
        &edn_text[..edn_text.len().min(600)]
    );

    // Nemesis 2: Real OS `SIGKILL` + restart crash recovery under concurrent register load!
    let report_kill = run_os_cluster_jepsen(&JepsenConfig {
        workload: JepsenWorkload::Register,
        nemesis: JepsenNemesisKind::KillRestart,
        seed: 0x4A02,
        clients: 4,
        duration_secs: 4,
        edn_output_path: None,
        metadbd_bin: Some(metadbd_exe()),
    })
    .await;
    println!("{report_kill}");
    assert!(report_kill.is_ok(), "{report_kill}");
    assert!(report_kill.ok_ops >= 10, "{report_kill}");
    assert!(report_kill.nemesis_actions >= 1, "{report_kill}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn jepsen_os_cluster_set_full_durability_under_combo_nemesis() {
    let _serial = jepsen_cluster_lock().lock().await;
    let report = run_os_cluster_jepsen(&JepsenConfig {
        workload: JepsenWorkload::Set,
        nemesis: JepsenNemesisKind::Combo,
        seed: 0x4A03,
        clients: 4,
        duration_secs: 4,
        edn_output_path: None,
        metadbd_bin: Some(metadbd_exe()),
    })
    .await;
    println!("{report}");
    assert!(report.is_ok(), "{report}");
    assert!(report.ok_ops >= 5, "{report}");
    let sf = report.set_full.as_ref().expect("set_full report");
    assert!(sf.lost_ok.is_empty(), "{report}");
    assert!(sf.unexpected.is_empty(), "{report}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn jepsen_os_cluster_bank_conservation_and_elle_atomic_rename() {
    let _serial = jepsen_cluster_lock().lock().await;

    let bank_report = run_os_cluster_jepsen(&JepsenConfig {
        workload: JepsenWorkload::Bank,
        nemesis: JepsenNemesisKind::KillRestart,
        seed: 0x4A04,
        clients: 4,
        duration_secs: 4,
        edn_output_path: None,
        metadbd_bin: Some(metadbd_exe()),
    })
    .await;
    println!("{bank_report}");
    assert!(bank_report.is_ok(), "{bank_report}");
    let bk = bank_report.bank.as_ref().expect("bank report");
    assert!(bk.read_snapshots >= 1, "{bank_report}");

    let elle_report = run_os_cluster_jepsen(&JepsenConfig {
        workload: JepsenWorkload::RenameElle,
        nemesis: JepsenNemesisKind::PauseFreeze,
        seed: 0x4A05,
        clients: 3,
        duration_secs: 4,
        edn_output_path: None,
        metadbd_bin: Some(metadbd_exe()),
    })
    .await;
    println!("{elle_report}");
    assert!(elle_report.is_ok(), "{elle_report}");
    let elle = elle_report.elle.as_ref().expect("elle report");
    assert!(elle.valid && elle.anomalies.is_empty(), "{elle_report}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn jepsen_three_voter_swizzle_clog_partition_register_and_set() {
    let _serial = jepsen_cluster_lock().lock().await;

    let reg_report = run_partition_jepsen(&JepsenConfig {
        workload: JepsenWorkload::Register,
        nemesis: JepsenNemesisKind::PartitionSwizzleClog,
        seed: 0x4A06,
        clients: 4,
        duration_secs: 4,
        edn_output_path: None,
        metadbd_bin: None,
    })
    .await;
    println!("{reg_report}");
    assert!(reg_report.is_ok(), "{reg_report}");
    assert!(reg_report.ok_ops >= 5, "{reg_report}");
    assert!(reg_report.nemesis_actions >= 1, "{reg_report}");

    let set_report = run_partition_jepsen(&JepsenConfig {
        workload: JepsenWorkload::Set,
        nemesis: JepsenNemesisKind::PartitionSwizzleClog,
        seed: 0x4A07,
        clients: 4,
        duration_secs: 4,
        edn_output_path: None,
        metadbd_bin: None,
    })
    .await;
    println!("{set_report}");
    assert!(set_report.is_ok(), "{set_report}");
    assert!(set_report.ok_ops >= 5, "{set_report}");
    assert!(set_report.nemesis_actions >= 1, "{set_report}");
}

#[test]
fn metadb_ctl_jepsen_cli_runs_partition_and_exports_edn() {
    let dir = tempdir().unwrap();
    let edn = dir.path().join("cli_history.edn");
    let out = Command::new(env!("CARGO_BIN_EXE_metadb-ctl"))
        .args([
            "jepsen",
            "--workload",
            "register",
            "--nemesis",
            "partition",
            "--seed",
            "0x4a99",
            "--clients",
            "3",
            "--duration-secs",
            "3",
            "--edn-out",
            edn.to_str().unwrap(),
        ])
        .output()
        .expect("spawn metadb-ctl jepsen");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "metadb-ctl jepsen failed:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.contains("valid=true"), "stdout:\n{stdout}");
    let content = std::fs::read_to_string(&edn).expect("read cli_history.edn");
    assert!(content.contains(":type :invoke"), "empty EDN");
}
