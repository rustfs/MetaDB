//! Measured heartbeat samples and the only liveness scheduler this process runs.
//!
//! A durable `observed_unix_ms` stamp is not liveness. Neither is a local maintenance
//! tick, the historical 1 ms wall-clock fallback, or a wall clock that no longer
//! agrees with the monotonic age of the sample. A sample counts only when this
//! process recorded a receive or a completed send/ack round trip.
//!
//! [`decide_liveness`] returns [`LivenessDecision::HintLive`] only for a trusted,
//! fresh sample. Every other input is [`LivenessDecision::Refuse`]. A hint is not
//! applied: [`liveness_effects`] never enables `transfer_leader`, a cross-machine
//! Jumbo move, a membership change, or cell-directory replication. Splits stay on
//! local Raft and LSM state. Client shard descriptors stay annotations.
//!
//! This is not a cell-wide heartbeat bus and not a production scheduler.

use std::collections::BTreeMap;
use std::sync::OnceLock;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use metadb_core::{HlcTimestamp, NodeId, DEFAULT_MAX_CLOCK_DRIFT_MS};

use crate::control_plane_state::HEARTBEAT_INTERVAL_MS;

/// A measured sample older than three local publish intervals is not fresh.
pub const HEARTBEAT_FRESHNESS_MS: u64 = HEARTBEAT_INTERVAL_MS * 3;

/// `HybridLogicalClock::wall_clock_ms` returns this when `SystemTime` is before
/// the Unix epoch. It is not a heartbeat observation.
pub const WALL_CLOCK_FALLBACK_MS: u64 = 1;

/// A send/ack gap longer than this is not a usable round trip.
pub const MAX_MEASURED_RTT_NS: u64 = 2_000_000_000;

/// Monotonic nanoseconds since the first capture in this process. Never zero.
pub fn capture_mono_ns() -> u64 {
    static ORIGIN: OnceLock<Instant> = OnceLock::new();
    let origin = ORIGIN.get_or_init(Instant::now);
    let elapsed = Instant::now().saturating_duration_since(*origin);
    let capped = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX - 1);
    capped.saturating_add(1)
}

/// `Some` only for a wall reading this process will treat as a clock observation.
///
/// Zero, the 1 ms fallback, and a value that does not fit the 48-bit HLC physical
/// field are `None`.
pub fn trusted_unix_ms() -> Option<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_millis();
    trusted_wall_unix_ms(millis)
}

pub fn trusted_wall_unix_ms(millis: u128) -> Option<u64> {
    let ms = u64::try_from(millis).ok()?;
    wall_unix_ms_is_trusted(ms).then_some(ms)
}

pub fn wall_unix_ms_is_trusted(ms: u64) -> bool {
    ms > WALL_CLOCK_FALLBACK_MS && ms <= HlcTimestamp::MAX_PHYSICAL_MS
}

/// Elapsed publish interval on a monotonic clock.
///
/// `last_mono_ns == 0` means this process has not published yet. A backwards step
/// is due again. It is not evidence that the previous tick is still fresh.
pub fn publish_interval_elapsed(last_mono_ns: u64, now_mono_ns: u64, interval_ms: u64) -> bool {
    if last_mono_ns == 0 || now_mono_ns < last_mono_ns {
        return true;
    }
    let interval_ns = interval_ms.saturating_mul(1_000_000);
    now_mono_ns.saturating_sub(last_mono_ns) >= interval_ns
}

/// Clock reading supplied to the scheduler. Wall time is absent when it is not trusted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScheduleNow {
    pub mono_ns: u64,
    pub wall_unix_ms: Option<u64>,
}

impl ScheduleNow {
    pub fn capture() -> Self {
        Self {
            mono_ns: capture_mono_ns(),
            wall_unix_ms: trusted_unix_ms(),
        }
    }
}

/// One measured heartbeat. `sent_mono_ns` is set only for a completed round trip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeasuredSample {
    pub node_id: NodeId,
    pub seq: u64,
    pub sent_mono_ns: Option<u64>,
    pub recv_mono_ns: u64,
    pub wall_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MeasurementReject {
    UntrustedClock,
    UntrustedMeasurement,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LivenessRefusal {
    MissingHeartbeat,
    ExpiredHeartbeat,
    UntrustedClock,
    UntrustedMeasurement,
}

/// A scheduling result. `HintLive` is not an instruction to move anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LivenessDecision {
    HintLive {
        node_id: NodeId,
        seq: u64,
        rtt_ns: Option<u64>,
        age_ms: u64,
    },
    Refuse {
        node_id: NodeId,
        reason: LivenessRefusal,
    },
}

/// Effects this process will take from a liveness decision. All four stay false.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LivenessEffects {
    pub transfer_leader: bool,
    pub jumbo_cross_machine: bool,
    pub membership_change: bool,
    pub directory_replication: bool,
}

pub fn liveness_effects(_decision: &LivenessDecision) -> LivenessEffects {
    LivenessEffects {
        transfer_leader: false,
        jumbo_cross_machine: false,
        membership_change: false,
        directory_replication: false,
    }
}

/// In-memory measured samples. Lost on restart, which then fails closed until a
/// new measurement arrives. Not a copy of `CELL_CONTROL_PLANE`.
#[derive(Debug, Default)]
pub struct HeartbeatScheduler {
    samples: BTreeMap<NodeId, MeasuredSample>,
    last_publish_mono_ns: u64,
    /// `None` reads the process clock. `Some` replaces it, including `Some(None)`.
    wall_override: Option<Option<u64>>,
}

impl HeartbeatScheduler {
    pub fn record(&mut self, sample: MeasuredSample) -> Result<(), MeasurementReject> {
        validate_sample(&sample)?;
        let replace = match self.samples.get(&sample.node_id) {
            Some(existing) if sample.recv_mono_ns < existing.recv_mono_ns => false,
            Some(existing)
                if sample.recv_mono_ns == existing.recv_mono_ns && sample.seq <= existing.seq =>
            {
                false
            }
            _ => true,
        };
        if replace {
            self.samples.insert(sample.node_id, sample);
        }
        Ok(())
    }

    pub fn sample(&self, node_id: NodeId) -> Option<&MeasuredSample> {
        self.samples.get(&node_id)
    }

    pub fn decide(&self, node_id: NodeId, now: &ScheduleNow) -> LivenessDecision {
        decide_liveness(node_id, self.samples.get(&node_id), now)
    }

    pub fn decide_all(&self, now: &ScheduleNow) -> Vec<LivenessDecision> {
        self.samples
            .keys()
            .copied()
            .map(|node_id| self.decide(node_id, now))
            .collect()
    }

    pub fn publish_is_due(&self, now_mono_ns: u64) -> bool {
        publish_interval_elapsed(
            self.last_publish_mono_ns,
            now_mono_ns,
            HEARTBEAT_INTERVAL_MS,
        )
    }

    pub fn note_published(&mut self, now_mono_ns: u64) {
        self.last_publish_mono_ns = now_mono_ns;
    }

    pub fn set_wall_override(&mut self, wall: Option<u64>) {
        self.wall_override = Some(wall);
    }

    /// Wall time for a new heartbeat record and for a new sample.
    pub fn effective_wall_unix_ms(&self) -> Option<u64> {
        match self.wall_override {
            Some(wall) => wall.and_then(|ms| trusted_wall_unix_ms(u128::from(ms))),
            None => trusted_unix_ms(),
        }
    }

    pub fn schedule_now(&self) -> ScheduleNow {
        let mut now = ScheduleNow::capture();
        if self.wall_override.is_some() {
            now.wall_unix_ms = self.effective_wall_unix_ms();
        }
        now
    }
}

pub fn decide_liveness(
    node_id: NodeId,
    sample: Option<&MeasuredSample>,
    now: &ScheduleNow,
) -> LivenessDecision {
    let Some(now_wall) = now.wall_unix_ms.filter(|ms| wall_unix_ms_is_trusted(*ms)) else {
        return LivenessDecision::Refuse {
            node_id,
            reason: LivenessRefusal::UntrustedClock,
        };
    };
    let Some(sample) = sample else {
        return LivenessDecision::Refuse {
            node_id,
            reason: LivenessRefusal::MissingHeartbeat,
        };
    };
    if sample.node_id != node_id || !wall_unix_ms_is_trusted(sample.wall_unix_ms) {
        return LivenessDecision::Refuse {
            node_id,
            reason: LivenessRefusal::UntrustedMeasurement,
        };
    }
    if now.mono_ns < sample.recv_mono_ns {
        return LivenessDecision::Refuse {
            node_id,
            reason: LivenessRefusal::UntrustedClock,
        };
    }
    let rtt_ns = match round_trip_ns(sample) {
        Ok(rtt) => rtt,
        Err(reason) => {
            return LivenessDecision::Refuse { node_id, reason };
        }
    };
    let mono_age_ms = now.mono_ns.saturating_sub(sample.recv_mono_ns) / 1_000_000;
    let wall_age_ms = match wall_age_ms(now_wall, sample.wall_unix_ms) {
        Ok(age) => age,
        Err(reason) => {
            return LivenessDecision::Refuse { node_id, reason };
        }
    };
    if mono_age_ms.abs_diff(wall_age_ms) > DEFAULT_MAX_CLOCK_DRIFT_MS {
        return LivenessDecision::Refuse {
            node_id,
            reason: LivenessRefusal::UntrustedClock,
        };
    }
    if mono_age_ms >= HEARTBEAT_FRESHNESS_MS {
        return LivenessDecision::Refuse {
            node_id,
            reason: LivenessRefusal::ExpiredHeartbeat,
        };
    }
    LivenessDecision::HintLive {
        node_id,
        seq: sample.seq,
        rtt_ns,
        age_ms: mono_age_ms,
    }
}

fn validate_sample(sample: &MeasuredSample) -> Result<(), MeasurementReject> {
    if !wall_unix_ms_is_trusted(sample.wall_unix_ms) {
        return Err(MeasurementReject::UntrustedClock);
    }
    if sample.node_id == 0 || sample.seq == 0 || sample.recv_mono_ns == 0 {
        return Err(MeasurementReject::UntrustedMeasurement);
    }
    match round_trip_ns(sample) {
        Ok(_) => Ok(()),
        Err(_) => Err(MeasurementReject::UntrustedMeasurement),
    }
}

fn round_trip_ns(sample: &MeasuredSample) -> Result<Option<u64>, LivenessRefusal> {
    let Some(sent) = sample.sent_mono_ns else {
        return Ok(None);
    };
    if sample.recv_mono_ns <= sent {
        return Err(LivenessRefusal::UntrustedMeasurement);
    }
    let rtt = sample.recv_mono_ns - sent;
    if rtt > MAX_MEASURED_RTT_NS {
        return Err(LivenessRefusal::UntrustedMeasurement);
    }
    Ok(Some(rtt))
}

fn wall_age_ms(now_wall: u64, sample_wall: u64) -> Result<u64, LivenessRefusal> {
    if now_wall >= sample_wall {
        Ok(now_wall - sample_wall)
    } else if sample_wall - now_wall <= DEFAULT_MAX_CLOCK_DRIFT_MS {
        Ok(0)
    } else {
        Err(LivenessRefusal::UntrustedClock)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WALL: u64 = 1_700_000_000_000;

    fn round_trip(sent: u64, recv: u64) -> MeasuredSample {
        MeasuredSample {
            node_id: 4,
            seq: 3,
            sent_mono_ns: Some(sent),
            recv_mono_ns: recv,
            wall_unix_ms: WALL,
        }
    }

    fn now_at(mono_ns: u64, wall: u64) -> ScheduleNow {
        ScheduleNow {
            mono_ns,
            wall_unix_ms: Some(wall),
        }
    }

    fn effects_are_closed(decision: &LivenessDecision) {
        let effects = liveness_effects(decision);
        assert!(!effects.transfer_leader);
        assert!(!effects.jumbo_cross_machine);
        assert!(!effects.membership_change);
        assert!(!effects.directory_replication);
    }

    #[test]
    fn live_measured_round_trip_is_a_hint_only() {
        let mut scheduler = HeartbeatScheduler::default();
        let sample = round_trip(1_000_000, 1_500_000);
        scheduler.record(sample).unwrap();
        let decision = scheduler.decide(4, &now_at(1_500_000 + 100 * 1_000_000, WALL + 100));
        match decision {
            LivenessDecision::HintLive {
                node_id,
                seq,
                rtt_ns,
                age_ms,
            } => {
                assert_eq!(node_id, 4);
                assert_eq!(seq, 3);
                assert_eq!(rtt_ns, Some(500_000));
                assert_eq!(age_ms, 100);
            }
            LivenessDecision::Refuse { reason, .. } => panic!("live sample refused: {reason:?}"),
        }
        effects_are_closed(&scheduler.decide(4, &now_at(1_500_000 + 100 * 1_000_000, WALL + 100)));
        assert!(!crate::heartbeat_is_scheduling_truth());

        let recv_only = MeasuredSample {
            sent_mono_ns: None,
            ..round_trip(1_000_000, 2_000_000)
        };
        scheduler.record(recv_only).unwrap();
        let recv = scheduler.decide(4, &now_at(2_000_000 + 50 * 1_000_000, WALL + 50));
        assert!(matches!(
            recv,
            LivenessDecision::HintLive { rtt_ns: None, .. }
        ));
        effects_are_closed(&recv);
    }

    #[test]
    fn missing_and_expired_heartbeats_are_refused() {
        let scheduler = HeartbeatScheduler::default();
        let missing = scheduler.decide(4, &now_at(5_000_000_000, WALL));
        assert_eq!(
            missing,
            LivenessDecision::Refuse {
                node_id: 4,
                reason: LivenessRefusal::MissingHeartbeat,
            }
        );
        effects_are_closed(&missing);

        let mut scheduler = HeartbeatScheduler::default();
        scheduler
            .record(round_trip(1_000_000_000, 1_000_500_000))
            .unwrap();
        let expired_mono = 1_000_500_000 + HEARTBEAT_FRESHNESS_MS * 1_000_000;
        let expired = scheduler.decide(4, &now_at(expired_mono, WALL + HEARTBEAT_FRESHNESS_MS));
        assert_eq!(
            expired,
            LivenessDecision::Refuse {
                node_id: 4,
                reason: LivenessRefusal::ExpiredHeartbeat,
            }
        );
        effects_are_closed(&expired);

        assert_eq!(
            scheduler.record(MeasuredSample {
                node_id: 4,
                seq: 1,
                sent_mono_ns: Some(10),
                recv_mono_ns: 10,
                wall_unix_ms: WALL,
            }),
            Err(MeasurementReject::UntrustedMeasurement)
        );
        assert!(scheduler.sample(4).unwrap().recv_mono_ns != 10);
    }

    #[test]
    fn untrusted_clock_fails_closed() {
        let mut scheduler = HeartbeatScheduler::default();
        scheduler.record(round_trip(1_000_000, 1_500_000)).unwrap();
        let fresh = now_at(1_500_000 + 10 * 1_000_000, WALL + 10);

        let fallback = ScheduleNow {
            wall_unix_ms: Some(WALL_CLOCK_FALLBACK_MS),
            ..fresh
        };
        assert_eq!(
            scheduler.decide(4, &fallback),
            LivenessDecision::Refuse {
                node_id: 4,
                reason: LivenessRefusal::UntrustedClock,
            }
        );
        let absent = ScheduleNow {
            wall_unix_ms: None,
            ..fresh
        };
        assert!(matches!(
            scheduler.decide(4, &absent),
            LivenessDecision::Refuse {
                reason: LivenessRefusal::UntrustedClock,
                ..
            }
        ));
        let zero = ScheduleNow {
            wall_unix_ms: Some(0),
            ..fresh
        };
        assert!(matches!(
            scheduler.decide(4, &zero),
            LivenessDecision::Refuse {
                reason: LivenessRefusal::UntrustedClock,
                ..
            }
        ));

        assert_eq!(
            scheduler.record(MeasuredSample {
                wall_unix_ms: WALL_CLOCK_FALLBACK_MS,
                ..round_trip(2_000_000, 2_500_000)
            }),
            Err(MeasurementReject::UntrustedClock)
        );
        assert_eq!(scheduler.sample(4).unwrap().wall_unix_ms, WALL);

        let stalled = now_at(1_500_000 + 30_000 * 1_000_000, WALL + 10);
        assert_eq!(
            scheduler.decide(4, &stalled),
            LivenessDecision::Refuse {
                node_id: 4,
                reason: LivenessRefusal::UntrustedClock,
            }
        );
        effects_are_closed(&scheduler.decide(4, &fallback));
        assert!(trusted_wall_unix_ms(0).is_none());
        assert!(trusted_wall_unix_ms(u128::from(WALL_CLOCK_FALLBACK_MS)).is_none());
        assert_eq!(trusted_wall_unix_ms(u128::from(WALL)), Some(WALL));
    }

    #[test]
    fn publish_interval_does_not_treat_a_clock_step_as_fresh() {
        assert!(publish_interval_elapsed(0, 1, HEARTBEAT_INTERVAL_MS));
        assert!(!publish_interval_elapsed(
            1_000_000,
            1_000_000 + HEARTBEAT_INTERVAL_MS * 1_000_000 - 1,
            HEARTBEAT_INTERVAL_MS
        ));
        assert!(publish_interval_elapsed(
            5_000_000,
            1_000,
            HEARTBEAT_INTERVAL_MS
        ));
        let mut scheduler = HeartbeatScheduler::default();
        assert!(scheduler.publish_is_due(10));
        scheduler.note_published(10);
        assert!(!scheduler.publish_is_due(10));
        assert!(scheduler.publish_is_due(9));
    }
}
