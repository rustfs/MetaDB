//! FoundationDB-Grade Automated Simulation, `Buggify`, `CodeProbe`, and 3-Voter `Swizzle-Clog`
//! Ensemble Verification Suite.
//!
//! Exercises:
//! 1. 500-seed parallel in-memory full-stack `Buggify` ensemble + bit-for-bit deterministic replay.
//! 2. Multi-seed persistent WAL-less multi-fault cascade (online PID split + `DualModeFineGrained` +
//!    cross-shard 2PC + POSIX overwrite rename + unflushed crash restart + compaction + purge + reopen)
//!    with Invariants 1–5 and `CodeProbe` rare-path coverage gates.
//! 3. Multi-seed 3-Voter Multi-Raft `Swizzle-Clog` chaos ensemble (3-voter online split + asymmetric
//!    link partitions + bounded RPC jitter + post-apply reply drops + node isolation/failover +
//!    indeterminate ACID resolution) with ALL 6 INVARIANTS including byte-for-byte 3-replica convergence.
//! 4. `metadb-ctl sim` CLI hex-seed execution and exit-code gating.

use std::process::Command;
use std::sync::OnceLock;
use tokio::sync::Mutex;

use metadb_server::fdb_chaos::{
    run_ensemble, run_in_memory_full_stack, ChaosWorkloadKind, EnsembleConfig,
};

fn global_chaos_mutex() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn in_memory_500_seeds_buggify_replay_and_full_stack_invariants() {
    // 1. Verify exact bit-for-bit deterministic replay under ThreadBuggifyGuard!
    let r1 = run_in_memory_full_stack(0x00C0_FFEE, 48);
    let r2 = run_in_memory_full_stack(0x00C0_FFEE, 48);
    assert!(r1.is_ok(), "{r1}");
    assert_eq!(r1.model_fingerprint, r2.model_fingerprint);
    assert_eq!(r1.trace_hash, r2.trace_hash);
    assert_eq!(r1.buggify_firings, r2.buggify_firings);

    // 2. Run 500 consecutive seeds (16,000+ operations across multi-UID create, unlink, intra/cross
    //    rename, grow/shrink setattr, chown setattr, mkdir/rmdir, paginated listdir) checking
    //    DirectoryTree, DirStat, QuotaTable, and SecondaryIndexTable invariants on every seed!
    let report = run_ensemble(&EnsembleConfig {
        workload: ChaosWorkloadKind::InMemoryFullStackBuggify,
        start_seed: 1,
        seed_count: 500,
        steps_per_seed: 36,
        concurrency: 4,
        required_probes: Vec::new(),
    })
    .await;
    println!("{report}");
    assert!(report.is_ok(), "{report}");
    assert_eq!(report.passed_seeds, 500);
    assert!(report.total_committed_mutations >= 10_000, "{report}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn persistent_multi_fault_cascade_ensemble_and_code_probes() {
    let _serial = global_chaos_mutex().lock().await;
    let report = run_ensemble(&EnsembleConfig {
        workload: ChaosWorkloadKind::PersistentMultiFaultCascade,
        start_seed: 0x100,
        seed_count: 6,
        steps_per_seed: 30,
        concurrency: 1,
        required_probes: vec![
            "probe.cluster.online_split_completed",
            "probe.tx.outbox_spilled_to_0x07",
            "probe.storage.early_memtable_seal",
        ],
    })
    .await;
    println!("{report}");
    assert!(report.is_ok(), "{report}");
    assert_eq!(report.passed_seeds, 6);
    assert!(
        report.total_buggify_firings > 0,
        "expected buggify firings: {report}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn three_voter_swizzle_clog_chaos_ensemble_and_six_invariants() {
    let _serial = global_chaos_mutex().lock().await;
    let report = run_ensemble(&EnsembleConfig {
        workload: ChaosWorkloadKind::ThreeVoterSwizzleClogChaos,
        start_seed: 0x700,
        seed_count: 4,
        steps_per_seed: 28,
        concurrency: 1,
        required_probes: vec![
            "probe.cluster.online_split_completed",
            "probe.raft.network_link_clogged",
            "probe.raft.network_packet_delayed",
            "probe.raft.leader_failover_observed",
        ],
    })
    .await;
    println!("{report}");
    assert!(report.is_ok(), "{report}");
    assert_eq!(report.passed_seeds, 4);
}

#[test]
fn metadb_ctl_sim_cli_runs_hex_seed_campaign() {
    let out = Command::new(env!("CARGO_BIN_EXE_metadb-ctl"))
        .args([
            "sim",
            "--workload",
            "in-memory",
            "--seed",
            "0xdeadbeef",
            "--seeds",
            "25",
            "--steps",
            "24",
            "--concurrency",
            "4",
        ])
        .output()
        .expect("spawn metadb-ctl sim");
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        out.status.success(),
        "metadb-ctl sim failed:\nstdout:\n{stdout}\nstderr:\n{stderr}"
    );
    assert!(stdout.contains("passed=25 failed=0"), "stdout={stdout}");
}
