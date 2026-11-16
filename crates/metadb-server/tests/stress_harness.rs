//! Mixed-workload stress against a real persistent MetaDB process.
//!
//! `stress_smoke_mixed_workload` is part of `cargo test --workspace` and ignores
//! `METADB_STRESS_*`. `stress_local_mixed_workload` is `#[ignore]` and reads those
//! variables (30s / 8 clients / 16 directories unless overridden).

use metadb_server::stress::{persistent_config, run_mixed, StressProfile};
use metadb_server::PersistentMetaDbCluster;
use tempfile::tempdir;

async fn run_profile(profile: StressProfile) {
    let dir = tempdir().unwrap();
    let cfg = persistent_config(dir.path(), 4 * 1024 * 1024 * 1024);
    let (cluster, _) = PersistentMetaDbCluster::open_and_serve(cfg).await.unwrap();
    let report = run_mixed(cluster.clone(), &profile).await;
    println!("{report}");
    cluster.shutdown().await.unwrap();
    assert_eq!(report.errors, 0, "{report}");
    assert_eq!(report.correctness_errors, 0, "{report}");
    assert!(report.ops > 0, "{report}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn stress_smoke_mixed_workload() {
    run_profile(StressProfile::smoke()).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "longer local stress profile; set METADB_STRESS_* and pass --ignored --nocapture"]
async fn stress_local_mixed_workload() {
    run_profile(StressProfile::from_env()).await;
}
