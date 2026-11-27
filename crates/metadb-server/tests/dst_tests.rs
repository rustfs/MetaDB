//! Deterministic Simulation Testing (DST) Integration Test Suite for MetaDB.
//!
//! Exercises the true single-threaded virtual-clock discrete-event simulator (`metadb_server::dst`)
//! driving MetaDB's real `TxStateMachineHandler`, `LifecycleStorageEngine`, `HybridLogicalClock`,
//! `ThreadBuggifyGuard`, `SimDisk` (volatile cache, CRC32C torn tails, injected `fsync` failures),
//! `SimNetwork` (partitions, asymmetric clogs, packet reordering/duplication, reply drops), and
//! continuous Raft + 2PC + Split + Aux + Jepsen WGL/Elle invariant verification.

use std::process::Command;

use metadb_server::dst::{run_dst_ensemble, run_dst_seed, DstConfig, DstScenario};

#[test]
fn dst_bit_for_bit_determinism_across_all_four_scenarios() {
    for scenario in DstScenario::all() {
        let cfg = DstConfig::for_scenario(scenario, 0x00C0_FFEE);
        let r1 = run_dst_seed(&cfg);
        let r2 = run_dst_seed(&cfg);
        println!("replay-check: {r1}");
        assert!(r1.is_ok(), "DST run 1 failed: {r1}");
        assert!(r2.is_ok(), "DST run 2 failed: {r2}");
        assert_eq!(
            r1.events_processed,
            r2.events_processed,
            "scenario {} events_processed diverged across identical seed",
            scenario.as_str()
        );
        assert_eq!(
            r1.final_virtual_time_ms,
            r2.final_virtual_time_ms,
            "scenario {} final_virtual_time_ms diverged",
            scenario.as_str()
        );
        assert_eq!(
            r1.trace_hash,
            r2.trace_hash,
            "scenario {} trace_hash diverged (non-deterministic simulation!)",
            scenario.as_str()
        );
        assert_eq!(
            r1.state_fingerprint,
            r2.state_fingerprint,
            "scenario {} state_fingerprint diverged",
            scenario.as_str()
        );
        assert_eq!(
            r1.history_digest,
            r2.history_digest,
            "scenario {} history_digest diverged",
            scenario.as_str()
        );
        assert_eq!(
            r1.buggify_firings,
            r2.buggify_firings,
            "scenario {} buggify_firings diverged",
            scenario.as_str()
        );
    }
}

#[test]
fn dst_raft_linearizability_3_and_5_nodes_under_partitions_reorder_dup_and_crashes() {
    let report = run_dst_ensemble(DstScenario::RaftLinearizability, 0x1000, 60, 40, 4);
    println!("{report}");
    assert!(report.is_ok(), "{report}");
    assert_eq!(report.passed_seeds, 60);
    assert!(report.total_elections >= 60, "{report}");
    assert!(report.total_crashes > 0, "{report}");
    assert!(report.total_events > 20_000, "{report}");
}

#[test]
fn dst_two_pc_crash_recovery_clock_skew_and_elle_strict_serializability() {
    let report = run_dst_ensemble(DstScenario::TwoPcCrashAndClockSkew, 0x2000, 40, 36, 4);
    println!("{report}");
    assert!(report.is_ok(), "{report}");
    assert_eq!(report.passed_seeds, 40);
    assert!(
        report.total_skew_rejections > 0,
        "expected HLC drift protection rejections under clock skew: {report}"
    );
}

#[test]
fn dst_online_split_abort_restore_and_aux_outbox_convergence() {
    let report = run_dst_ensemble(DstScenario::SplitAndAuxConvergence, 0x3000, 30, 36, 4);
    println!("{report}");
    assert!(report.is_ok(), "{report}");
    assert_eq!(report.passed_seeds, 30);
    assert!(report.total_crashes > 0, "{report}");
}

#[test]
fn dst_disk_power_loss_torn_tail_crc_and_fsync_error_rollback() {
    let report = run_dst_ensemble(DstScenario::DiskFaultAndTornRecovery, 0x4000, 40, 36, 4);
    println!("{report}");
    assert!(report.is_ok(), "{report}");
    assert_eq!(report.passed_seeds, 40);
    assert!(
        report.total_torn_recovered > 0,
        "expected CRC32C torn tail recoveries: {report}"
    );
    assert!(
        report.total_fsync_rollbacks > 0,
        "expected injected fsync rollback exercises: {report}"
    );
}

#[test]
fn metadb_ctl_dst_cli_runs_all_scenarios() {
    let out = Command::new(env!("CARGO_BIN_EXE_metadb-ctl"))
        .args([
            "dst",
            "--scenario",
            "all",
            "--seed",
            "0x5000",
            "--seeds",
            "4",
            "--ops",
            "24",
            "--concurrency",
            "4",
        ])
        .output()
        .expect("spawn metadb-ctl dst");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "metadb-ctl dst failed:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(
        stdout.contains("scenario=raft_linearizability")
            && stdout.contains("scenario=two_pc_crash_and_clock_skew")
            && stdout.contains("scenario=split_and_aux_convergence")
            && stdout.contains("scenario=disk_fault_and_torn_recovery")
            && stdout.contains("passed=4 failed=0"),
        "unexpected metadb-ctl dst stdout:\n{stdout}"
    );
}
