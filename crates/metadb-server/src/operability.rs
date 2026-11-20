//! Process phase and readiness decision.
//!
//! The HTTP listener being bound is not an input. Readiness is false until
//! startup has finished, every local shard can accept or forward a client
//! request, and shutdown has not started. gRPC stays up while a probe says
//! not ready, so Raft peers can still reach a starting or recovering voter.

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

/// Stored lifecycle. Shard recovery is not a separate stored value: a serving
/// process with a pending replay reports `raft_recovery_pending` and is not ready.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ProcessPhase {
    Starting = 0,
    Serving = 1,
    ShuttingDown = 2,
    Failed = 3,
}

impl ProcessPhase {
    pub fn code(self) -> u64 {
        self as u8 as u64
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Serving => "serving",
            Self::ShuttingDown => "shutting_down",
            Self::Failed => "failed",
        }
    }

    fn from_code(code: u8) -> Self {
        match code {
            1 => Self::Serving,
            2 => Self::ShuttingDown,
            3 => Self::Failed,
            _ => Self::Starting,
        }
    }
}

/// One local Raft group, reduced to flags a probe is allowed to publish.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardGate {
    pub leader_known: bool,
    pub applied: bool,
    pub voter: bool,
    pub recovery_pending: bool,
    pub storage_unrecoverable: bool,
    pub stopped: bool,
    pub fatal: bool,
}

impl ShardGate {
    pub fn servable() -> Self {
        Self {
            leader_known: true,
            applied: true,
            voter: true,
            recovery_pending: false,
            storage_unrecoverable: false,
            stopped: false,
            fatal: false,
        }
    }
}

/// Result of one readiness evaluation. `reason` is a fixed token, never a path,
/// address, or secret.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadinessReport {
    pub live: bool,
    pub ready: bool,
    pub phase: ProcessPhase,
    pub reason: &'static str,
}

/// Shared phase flag. Shutdown does not clear `Failed`, so a startup failure
/// is not reported as a live graceful stop.
#[derive(Debug)]
pub struct Operability {
    phase: AtomicU8,
}

impl Operability {
    pub fn starting() -> Arc<Self> {
        Arc::new(Self {
            phase: AtomicU8::new(ProcessPhase::Starting as u8),
        })
    }

    pub fn phase(&self) -> ProcessPhase {
        ProcessPhase::from_code(self.phase.load(Ordering::Acquire))
    }

    pub fn mark_startup_finished(&self) {
        let _ = self.phase.compare_exchange(
            ProcessPhase::Starting as u8,
            ProcessPhase::Serving as u8,
            Ordering::AcqRel,
            Ordering::Acquire,
        );
    }

    pub fn fail(&self) {
        self.phase
            .store(ProcessPhase::Failed as u8, Ordering::Release);
    }

    pub fn begin_shutdown(&self) {
        let mut current = self.phase.load(Ordering::Acquire);
        while current != ProcessPhase::Failed as u8 && current != ProcessPhase::ShuttingDown as u8 {
            match self.phase.compare_exchange_weak(
                current,
                ProcessPhase::ShuttingDown as u8,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
}

/// Decides liveness and readiness.
///
/// While `phase` is `Serving`, the first failing shard condition wins, in this
/// order: storage unrecoverable, Raft fatal, stopped, startup replay pending,
/// this node is not an applied voter, no applied log, leader unknown.
/// An empty shard list is not ready. A healthy shard list does not override
/// `Starting`, `ShuttingDown`, or `Failed`.
pub fn evaluate_readiness(phase: ProcessPhase, shards: &[ShardGate]) -> ReadinessReport {
    evaluate_process_readiness(phase, shards, false)
}

/// `directory_only` is a `global-manager` with no local Raft groups.
/// An empty shard list is still not ready for every other role.
pub fn evaluate_process_readiness(
    phase: ProcessPhase,
    shards: &[ShardGate],
    directory_only: bool,
) -> ReadinessReport {
    let blocked = |reason: &'static str| ReadinessReport {
        live: phase != ProcessPhase::Failed,
        ready: false,
        phase,
        reason,
    };
    match phase {
        ProcessPhase::Failed => ReadinessReport {
            live: false,
            ready: false,
            phase,
            reason: "failed",
        },
        ProcessPhase::ShuttingDown => blocked("shutting_down"),
        ProcessPhase::Starting => blocked("starting"),
        ProcessPhase::Serving => {
            if shards.is_empty() {
                if directory_only {
                    return ReadinessReport {
                        live: true,
                        ready: true,
                        phase,
                        reason: "directory_only",
                    };
                }
                return blocked("no_local_shards");
            }
            if shards.iter().any(|shard| shard.storage_unrecoverable) {
                return blocked("storage_unrecoverable");
            }
            if shards.iter().any(|shard| shard.fatal) {
                return blocked("raft_fatal");
            }
            if shards.iter().any(|shard| shard.stopped) {
                return blocked("raft_stopped");
            }
            if shards.iter().any(|shard| shard.recovery_pending) {
                return blocked("raft_recovery_pending");
            }
            if shards.iter().any(|shard| !shard.voter) {
                return blocked("not_voter");
            }
            if shards.iter().any(|shard| !shard.applied) {
                return blocked("raft_log_not_applied");
            }
            if shards.iter().any(|shard| !shard.leader_known) {
                return blocked("leader_unknown");
            }
            ReadinessReport {
                live: true,
                ready: true,
                phase,
                reason: "ready",
            }
        }
    }
}

pub fn unavailable_report() -> ReadinessReport {
    ReadinessReport {
        live: false,
        ready: false,
        phase: ProcessPhase::Failed,
        reason: "unavailable",
    }
}

pub fn live_body(report: &ReadinessReport) -> String {
    if report.live {
        "live\n".to_owned()
    } else {
        format!("not live: {}\n", report.reason)
    }
}

pub fn ready_body(report: &ReadinessReport) -> String {
    if report.ready {
        "ready\n".to_owned()
    } else {
        format!("not ready: {}\n", report.reason)
    }
}

/// Diagnostic line. Callers that use the HTTP status as a traffic gate must
/// treat 200 as `report.ready`, not as "the listener accepted the socket".
pub fn health_body(report: &ReadinessReport) -> String {
    format!(
        "live={} ready={} phase={} reason={}\n",
        u8::from(report.live),
        u8::from(report.ready),
        report.phase.as_str(),
        report.reason,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ServerTomlConfig;

    #[test]
    fn listener_facts_do_not_make_a_starting_process_ready() {
        let report = evaluate_readiness(ProcessPhase::Starting, &[ShardGate::servable()]);
        assert!(report.live);
        assert!(!report.ready);
        assert_eq!(report.reason, "starting");
        assert_eq!(ready_body(&report), "not ready: starting\n");
        assert_eq!(live_body(&report), "live\n");
        let health = health_body(&report);
        assert!(health.contains("ready=0"));
        assert!(health.contains("phase=starting"));
    }

    #[test]
    fn serving_requires_every_local_shard_to_be_able_to_take_traffic() {
        let ready = evaluate_readiness(
            ProcessPhase::Serving,
            &[ShardGate::servable(), ShardGate::servable()],
        );
        assert!(ready.ready);
        assert_eq!(ready.reason, "ready");
        assert_eq!(ready_body(&ready), "ready\n");

        let mut leaderless = ShardGate::servable();
        leaderless.leader_known = false;
        let report =
            evaluate_readiness(ProcessPhase::Serving, &[ShardGate::servable(), leaderless]);
        assert!(!report.ready);
        assert_eq!(report.reason, "leader_unknown");

        let mut replaying = ShardGate::servable();
        replaying.recovery_pending = true;
        replaying.leader_known = true;
        let report = evaluate_readiness(ProcessPhase::Serving, &[replaying]);
        assert_eq!(report.reason, "raft_recovery_pending");

        let mut learner = ShardGate::servable();
        learner.voter = false;
        assert_eq!(
            evaluate_readiness(ProcessPhase::Serving, &[learner]).reason,
            "not_voter"
        );

        let mut unapplied = ShardGate::servable();
        unapplied.applied = false;
        assert_eq!(
            evaluate_readiness(ProcessPhase::Serving, &[unapplied]).reason,
            "raft_log_not_applied"
        );

        assert_eq!(
            evaluate_readiness(ProcessPhase::Serving, &[]).reason,
            "no_local_shards"
        );
        let directory = evaluate_process_readiness(ProcessPhase::Serving, &[], true);
        assert!(directory.ready);
        assert_eq!(directory.reason, "directory_only");
    }

    #[test]
    fn shutdown_is_not_ready_and_failure_is_not_live() {
        let stopping = evaluate_readiness(ProcessPhase::ShuttingDown, &[ShardGate::servable()]);
        assert!(stopping.live);
        assert!(!stopping.ready);
        assert_eq!(stopping.reason, "shutting_down");

        let failed = evaluate_readiness(ProcessPhase::Failed, &[ShardGate::servable()]);
        assert!(!failed.live);
        assert!(!failed.ready);
        assert_eq!(failed.reason, "failed");
        assert_eq!(live_body(&failed), "not live: failed\n");

        let ops = Operability::starting();
        ops.fail();
        ops.begin_shutdown();
        assert_eq!(ops.phase(), ProcessPhase::Failed);
        ops.mark_startup_finished();
        assert_eq!(ops.phase(), ProcessPhase::Failed);

        let ops = Operability::starting();
        ops.mark_startup_finished();
        ops.begin_shutdown();
        assert_eq!(ops.phase(), ProcessPhase::ShuttingDown);
        ops.mark_startup_finished();
        assert_eq!(ops.phase(), ProcessPhase::ShuttingDown);
    }

    #[test]
    fn probe_text_has_no_place_to_carry_a_secret() {
        let secret = "super-secret-token";
        for phase in [
            ProcessPhase::Starting,
            ProcessPhase::Serving,
            ProcessPhase::ShuttingDown,
            ProcessPhase::Failed,
        ] {
            let mut shard = ShardGate::servable();
            shard.storage_unrecoverable = true;
            let report = evaluate_readiness(phase, &[shard]);
            let text = format!(
                "{}{}{}",
                live_body(&report),
                ready_body(&report),
                health_body(&report)
            );
            assert!(!text.contains(secret), "{text}");
            assert!(!text.contains('/'), "{text}");
            assert!(!text.contains("Bearer"), "{text}");
        }
    }

    #[test]
    fn raft_table_is_loaded_and_rejected_keys_still_fail_closed() {
        let cfg = ServerTomlConfig::from_toml_str(
            r#"
            [raft]
            heartbeat_interval_ms = 80
            election_timeout_min_ms = 250
            election_timeout_max_ms = 450
            max_in_snapshot_log_to_keep = 4
            max_segment_bytes = 1048576
            "#,
        )
        .unwrap();
        assert_eq!(cfg.raft_options.heartbeat_interval_ms, 80);
        assert_eq!(cfg.raft_options.election_timeout_min_ms, 250);
        assert_eq!(cfg.raft_options.election_timeout_max_ms, 450);
        assert_eq!(cfg.raft_options.max_in_snapshot_log_to_keep, 4);
        assert_eq!(cfg.raft_options.max_segment_bytes, 1_048_576);

        let partial = ServerTomlConfig::from_toml_str(
            r#"
            [raft]
            heartbeat_interval_ms = 100
            "#,
        )
        .unwrap();
        assert_eq!(partial.raft_options.heartbeat_interval_ms, 100);
        assert_eq!(partial.raft_options.election_timeout_min_ms, 450);

        let omitted = ServerTomlConfig::from_toml_str("[node]\nnode_id = 3\n").unwrap();
        assert_eq!(omitted.node.node_id, 3);
        assert_eq!(omitted.raft_options.heartbeat_interval_ms, 150);

        let typo = ServerTomlConfig::from_toml_str(
            r#"
            [raft]
            heartbit_interval_ms = 80
            "#,
        );
        let typo_err = typo.unwrap_err().to_string();
        assert!(
            typo_err.contains("heartbit_interval_ms"),
            "a misspelled raft key must not be ignored: {typo_err}"
        );

        let too_fast = ServerTomlConfig::from_toml_str(
            r#"
            [raft]
            heartbeat_interval_ms = 900
            "#,
        );
        assert!(too_fast
            .unwrap_err()
            .to_string()
            .contains("invalid [raft] options"));

        let tls = ServerTomlConfig::from_toml_str(
            r#"
            [security]
            shared_secret = "secret"
            tls_cert = "/tmp/cert.pem"
            "#,
        );
        let tls_err = tls.unwrap_err().to_string();
        assert!(
            tls_err.contains("does not fall open to plaintext"),
            "raft parsing must not skip the security-key rejection: {tls_err}"
        );
    }
}
