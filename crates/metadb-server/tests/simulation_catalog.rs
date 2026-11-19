//! Seeded simulation catalog.
//!
//! The header names the three layers so a passing test stays tied to the layer
//! it actually ran:
//!
//! 1. In-memory / same-process simulation. `MetaDbEngine::new_single_node` does
//!    not open a data directory, an LSM, or OpenRaft. The same seed replays the
//!    same directory model.
//! 2. Single-process persistence. `PersistentMetaDbCluster` on a temporary
//!    directory, then either `shutdown_without_flush` or `flush_all` plus
//!    `shutdown`, then `open_and_serve` again in this same OS process.
//! 3. Real multi-process / multi-node. Not executed. Three-replica coverage/delete
//!    and concurrent `add_learner` stay deferred until the Aliyun 1-to-5 rehearsal.
//!    This file does not start those machines.
//!
//! A passing assertion is the scripted directory invariant for the seeds below.
//! It is not a first-ship pass, not the official million-operation load, and not
//! three-VM evidence.

use metadb_server::simulation::{ci_workload, run_in_memory, run_persistent};
use metadb_server::stress::persistent_config;
use tempfile::tempdir;

const IN_MEMORY_SEEDS: [u64; 4] = [1, 7, 99, 0x00C0_FFEE];

#[test]
fn in_memory_metadata_crud_holds_the_directory_invariant() {
    let spec = ci_workload("metadata_crud").expect("metadata_crud is in the CI catalog");
    for seed in IN_MEMORY_SEEDS {
        let report = run_in_memory(spec, seed);
        println!("{report}");
        assert_eq!(report.invariant_violations.len(), 0, "{report}");
        assert_eq!(report.omitted_steps, 1, "{report}");
        assert!(report.acked_mutations > 0, "{report}");
    }
}

#[test]
fn in_memory_same_seed_replays_the_directory_model() {
    let spec = ci_workload("metadata_crud").expect("metadata_crud is in the CI catalog");
    let first = run_in_memory(spec, 1);
    let second = run_in_memory(spec, 1);
    assert_eq!(first.invariant_violations.len(), 0, "{first}");
    assert_eq!(second.invariant_violations.len(), 0, "{second}");
    assert_eq!(first.trace_hash, second.trace_hash, "{first}\n{second}");
    assert_eq!(
        first.model_fingerprint, second.model_fingerprint,
        "{first}\n{second}"
    );
    assert_eq!(first.acked_mutations, second.acked_mutations);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_unflushed_restart_holds_the_directory_invariant() {
    let spec = ci_workload("metadata_crud_unflushed_restart")
        .expect("unflushed restart workload is in the CI catalog");
    let dir = tempdir().unwrap();
    let cfg = persistent_config(dir.path(), 4 * 1024 * 1024 * 1024);
    let report = run_persistent(spec, 11, &cfg).await;
    println!("{report}");
    assert_eq!(report.invariant_violations.len(), 0, "{report}");
    assert!(report.fault.starts_with("unflushed-restart"), "{report}");
    assert!(report.acked_mutations > 0, "{report}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_flush_purge_restart_holds_the_directory_invariant() {
    let spec = ci_workload("metadata_crud_flush_purge_restart")
        .expect("flush-purge restart workload is in the CI catalog");
    let dir = tempdir().unwrap();
    let cfg = persistent_config(dir.path(), 4 * 1024 * 1024 * 1024);
    let report = run_persistent(spec, 13, &cfg).await;
    println!("{report}");
    assert_eq!(report.invariant_violations.len(), 0, "{report}");
    assert!(report.fault.starts_with("flush-purge-restart"), "{report}");
    assert!(report.acked_mutations > 0, "{report}");
}
