use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::error::{MetaDbError, Result};

/// Default maximum allowed remote physical wall-clock drift ahead of local clock (`500 ms`).
pub const DEFAULT_MAX_CLOCK_DRIFT_MS: u64 = 500;

/// A 64-bit Hybrid Logical Clock (HLC) timestamp packing:
/// - Upper 48 bits: physical wall-clock time in milliseconds since UNIX epoch.
/// - Lower 16 bits: monotonically increasing logical counter within the same millisecond.
///
/// As described in §8 of the Baidu MetaDB paper (`arXiv:2610.32247v1`), replacing a
/// centralized Timestamp Oracle (TSO) with a decentralized HLC removes a cluster-wide
/// write availability bottleneck while preserving causal ordering across nodes.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    Serialize,
    Deserialize,
    wincode::SchemaWrite,
    wincode::SchemaRead,
)]
pub struct HlcTimestamp(pub u64);

impl HlcTimestamp {
    pub const ZERO: HlcTimestamp = HlcTimestamp(0);
    /// Maximum 48-bit physical millisecond component (`(1 << 48) - 1`, ~8,925 years).
    pub const MAX_PHYSICAL_MS: u64 = (1u64 << 48) - 1;

    /// Constructs an HLC timestamp from physical milliseconds (clamped to 48 bits) and a 16-bit logical counter.
    #[inline]
    pub const fn new(physical_ms: u64, logical: u16) -> Self {
        let clamped_phys = if physical_ms > Self::MAX_PHYSICAL_MS {
            Self::MAX_PHYSICAL_MS
        } else {
            physical_ms
        };
        HlcTimestamp((clamped_phys << 16) | (logical as u64))
    }

    /// Physical wall-clock component in milliseconds.
    #[inline]
    pub const fn physical_ms(self) -> u64 {
        self.0 >> 16
    }

    /// 16-bit logical counter component.
    #[inline]
    pub const fn logical(self) -> u16 {
        (self.0 & 0xFFFF) as u16
    }

    /// Raw 64-bit integer representation.
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the smallest timestamp strictly greater than `self`.
    ///
    /// When `logical == u16::MAX`, advances `physical_ms` by 1. At the 48-bit physical
    /// ceiling this returns `self` instead of wrapping `logical` back to 0.
    #[inline]
    pub const fn next_strict(self) -> Self {
        let phys = self.physical_ms();
        let log = self.logical();
        if log < u16::MAX {
            Self::new(phys, log + 1)
        } else if phys < Self::MAX_PHYSICAL_MS {
            Self::new(phys + 1, 0)
        } else {
            self
        }
    }
}

/// Thread-safe Hybrid Logical Clock generator with bounded clock-skew protection
/// and overflow-safe 16-bit logical counter progression (Goldfish Resolutions B2 & N5).
#[derive(Debug)]
pub struct HybridLogicalClock {
    state: AtomicU64,
    max_drift_ms: u64,
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridLogicalClock {
    /// Creates a new HLC initialized at zero with `DEFAULT_MAX_CLOCK_DRIFT_MS` (`500 ms`).
    pub const fn new() -> Self {
        Self {
            state: AtomicU64::new(0),
            max_drift_ms: DEFAULT_MAX_CLOCK_DRIFT_MS,
        }
    }

    /// Creates a new HLC with a custom `max_drift_ms` bound.
    pub const fn with_max_drift_ms(max_drift_ms: u64) -> Self {
        Self {
            state: AtomicU64::new(0),
            max_drift_ms,
        }
    }

    #[inline]
    pub const fn max_drift_ms(&self) -> u64 {
        self.max_drift_ms
    }

    /// Generates a strictly monotonic timestamp for a local event.
    pub fn now(&self) -> HlcTimestamp {
        let wall_ms = Self::wall_clock_ms();
        self.now_with_wall_ms(wall_ms)
    }

    /// Generates a timestamp strictly greater than both the current clock state and `floor`.
    pub fn now_strictly_after(&self, floor: HlcTimestamp) -> HlcTimestamp {
        let wall_ms = Self::wall_clock_ms();
        loop {
            let current = HlcTimestamp(self.state.load(Ordering::Acquire));
            let base = current.max(floor);
            let wall_ts = HlcTimestamp::new(wall_ms, 0);
            let next = if wall_ts > base {
                wall_ts
            } else {
                base.next_strict()
            };
            if self
                .state
                .compare_exchange_weak(current.0, next.0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return next;
            }
        }
    }

    /// Generates a strictly monotonic timestamp given an explicit physical millisecond reading.
    pub fn now_with_wall_ms(&self, wall_ms: u64) -> HlcTimestamp {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let cur_phys = current >> 16;
            let cur_log = (current & 0xFFFF) as u16;
            let wall_phys = wall_ms.min(HlcTimestamp::MAX_PHYSICAL_MS);

            let next = if wall_phys > cur_phys {
                HlcTimestamp::new(wall_phys, 0).0
            } else if cur_log < u16::MAX {
                HlcTimestamp::new(cur_phys, cur_log + 1).0
            } else if cur_phys < HlcTimestamp::MAX_PHYSICAL_MS {
                // Never wrap `u16` to 0 at the same physical_ms; advance physical_ms by 1.
                HlcTimestamp::new(cur_phys + 1, 0).0
            } else {
                current
            };

            if self
                .state
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return HlcTimestamp(next);
            }
        }
    }

    /// Updates the local HLC upon receiving a remote `HlcTimestamp` after enforcing
    /// `max_drift_ms` skew protection.
    pub fn update_checked(&self, remote: HlcTimestamp) -> Result<HlcTimestamp> {
        let wall_ms = Self::wall_clock_ms();
        self.update_checked_with_wall_ms(remote, wall_ms)
    }

    /// Deterministic drift-checked causal HLC update against an explicit `wall_ms`.
    pub fn update_checked_with_wall_ms(
        &self,
        remote: HlcTimestamp,
        wall_ms: u64,
    ) -> Result<HlcTimestamp> {
        let max_allowed = wall_ms.saturating_add(self.max_drift_ms);
        let rem_phys = remote.physical_ms();
        if rem_phys > max_allowed {
            return Err(MetaDbError::ClockSkewExceeded {
                remote_ms: rem_phys,
                local_ms: wall_ms,
                max_drift_ms: self.max_drift_ms,
            });
        }

        loop {
            let current = self.state.load(Ordering::Acquire);
            let cur_ts = HlcTimestamp(current);
            let max_ts = cur_ts.max(remote);
            let wall_ts = HlcTimestamp::new(wall_ms, 0);

            let next = if wall_ts > max_ts {
                wall_ts
            } else {
                max_ts.next_strict()
            };

            if next <= max_ts {
                return Err(MetaDbError::HlcSaturated);
            }

            if next.physical_ms() > max_allowed {
                return Err(MetaDbError::ClockSkewExceeded {
                    remote_ms: next.physical_ms(),
                    local_ms: wall_ms,
                    max_drift_ms: self.max_drift_ms,
                });
            }

            if self
                .state
                .compare_exchange_weak(current, next.0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Ok(next);
            }
        }
    }

    /// Updates the local HLC upon receiving a message with a remote `HlcTimestamp`
    /// and returns a new timestamp strictly greater than both local and remote states.
    pub fn update(&self, remote: HlcTimestamp) -> HlcTimestamp {
        let wall_ms = Self::wall_clock_ms();
        self.update_with_wall_ms(remote, wall_ms)
    }

    fn update_with_wall_ms(&self, remote: HlcTimestamp, wall_ms: u64) -> HlcTimestamp {
        loop {
            let current = self.state.load(Ordering::Acquire);
            let cur_ts = HlcTimestamp(current);
            let max_ts = cur_ts.max(remote);
            let wall_ts = HlcTimestamp::new(wall_ms, 0);

            let next = if wall_ts > max_ts {
                wall_ts
            } else {
                max_ts.next_strict()
            };

            if self
                .state
                .compare_exchange_weak(current, next.0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return next;
            }
        }
    }

    pub fn wall_clock_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(1)
    }

    /// Wall time suitable for a lease decision.
    ///
    /// `None` when the clock cannot be read, is the Unix epoch, or does not fit
    /// in the 48-bit HLC physical field. The `1` fallback used by [`Self::wall_clock_ms`]
    /// is not a lease observation.
    pub fn try_wall_clock_ms() -> Option<u64> {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()?
            .as_millis();
        observation_millis(millis)
    }
}

/// Accepts a millisecond reading as a lease observation.
///
/// Zero and values above the 48-bit physical ceiling are refused. They must not
/// become the historical `1 ms` fallback.
pub fn observation_millis(millis: u128) -> Option<u64> {
    if millis == 0 || millis > HlcTimestamp::MAX_PHYSICAL_MS as u128 {
        None
    } else {
        Some(millis as u64)
    }
}

/// Wall time that is not behind the last durable applied HLC by more than the
/// allowed drift. A failed read, an overflow, or a rollback returns `None`.
/// Callers must not serve or extend a lease from `None`.
pub fn trusted_lease_observation(
    last_applied: HlcTimestamp,
    wall_ms: Option<u64>,
) -> Option<HlcTimestamp> {
    let wall = wall_ms?;
    let floor = last_applied
        .physical_ms()
        .saturating_sub(DEFAULT_MAX_CLOCK_DRIFT_MS);
    if wall < floor {
        None
    } else {
        Some(HlcTimestamp::new(wall, 0))
    }
}

/// `expire` is inside the lifetime cap measured from both `start` and `observed`.
///
/// A rolled-back `observed` (far behind `expire`) or a checked-add overflow is
/// not a legal window. Equality with the cap is legal.
pub fn lease_window_is_legal(
    start: HlcTimestamp,
    expire: HlcTimestamp,
    observed: HlcTimestamp,
    max_lifetime_ms: u64,
) -> bool {
    if expire < start || observed > expire {
        return false;
    }
    let Some(from_start) = start.physical_ms().checked_add(max_lifetime_ms) else {
        return false;
    };
    let Some(from_observed) = observed.physical_ms().checked_add(max_lifetime_ms) else {
        return false;
    };
    expire.physical_ms() <= from_start && expire.physical_ms() <= from_observed
}

/// The stored window is longer than `max_lifetime_ms`, or `expire` is before `start`.
///
/// This does not read a clock. A missing or skewed wall clock cannot make an
/// over-cap record legal, and it cannot make a legal record look over-cap.
pub fn lease_exceeds_lifetime(
    start: HlcTimestamp,
    expire: HlcTimestamp,
    max_lifetime_ms: u64,
) -> bool {
    if expire < start {
        return true;
    }
    match start.physical_ms().checked_add(max_lifetime_ms) {
        Some(cap) => expire.physical_ms() > cap,
        None => true,
    }
}

/// `now` is inside the published lease and could still be the clock that holds it.
///
/// `ttl_ms` is the TTL that last published `expire`. A directory-rename renew uses
/// that same TTL, so an observation more than `ttl + drift` before `expire` did
/// not publish this deadline. A physical time of `0` or `1` is the unread-clock
/// fallback. Either case must not extend the lease.
pub fn lease_clock_can_extend(now: HlcTimestamp, expire: HlcTimestamp, ttl_ms: u64) -> bool {
    if now.physical_ms() <= 1 || now > expire {
        return false;
    }
    let slack = ttl_ms.saturating_add(DEFAULT_MAX_CLOCK_DRIFT_MS);
    now.physical_ms() >= expire.physical_ms().saturating_sub(slack)
}

/// `now` can extend a 2PC lease that started at `start` and ends at `expire`.
///
/// The deadline may already be many TTLs ahead: one renew can move it up to the
/// lifetime cap. The observation still has to be after `start` (within drift)
/// and not after `expire`. The 1 ms fallback is not inside the window.
pub fn lease_observation_inside_window(
    now: HlcTimestamp,
    start: HlcTimestamp,
    expire: HlcTimestamp,
) -> bool {
    if now.physical_ms() <= 1 || now > expire || expire < start {
        return false;
    }
    now.physical_ms().saturating_add(DEFAULT_MAX_CLOCK_DRIFT_MS) >= start.physical_ms()
}

/// Expiry proved without treating an untrusted or rolled-back wall as "not yet".
///
/// `last_applied > expire` is enough. A wall reading proves expiry only when
/// [`trusted_lease_observation`] accepts it and that observation is past `expire`.
/// `None` does not prove the lease is still inside the window; callers that
/// cannot prove expiry must leave the durable lock in place.
pub fn lease_expiry_is_proved(
    last_applied: HlcTimestamp,
    expire: HlcTimestamp,
    wall_ms: Option<u64>,
) -> bool {
    if last_applied > expire {
        return true;
    }
    let Some(wall) = wall_ms else {
        return false;
    };
    if wall <= 1 {
        return false;
    }
    trusted_lease_observation(last_applied, Some(wall)).is_some_and(|observed| observed > expire)
}

/// `wall` still moves with a monotonic anchor captured at `origin_wall`.
///
/// `elapsed_ms` is monotonic time since that anchor. A step larger than the
/// drift bound, or a wall that moved backwards past the bound, is skewed.
/// The historical `1` ms fallback is not an observation.
pub fn wall_tracks_monotonic(elapsed_ms: u128, origin_wall: u64, wall: u64) -> Option<u64> {
    if wall <= 1 || origin_wall <= 1 {
        return None;
    }
    if wall.saturating_add(DEFAULT_MAX_CLOCK_DRIFT_MS) < origin_wall {
        return None;
    }
    let elapsed = u64::try_from(elapsed_ms).unwrap_or(u64::MAX);
    let moved = wall.saturating_sub(origin_wall);
    if moved.abs_diff(elapsed) > DEFAULT_MAX_CLOCK_DRIFT_MS {
        None
    } else {
        Some(wall)
    }
}

/// `now_ms` is this process's wall clock, and that wall does not track monotonic time.
///
/// A timestamp more than the drift bound away from the process wall is a
/// caller-supplied clock, such as a test HLC, and is not this check. A true
/// result must not acquire, extend, replace, or abort a lease.
pub fn lease_live_wall_is_untracked(
    now_ms: u64,
    raw_wall_ms: Option<u64>,
    tracked_ms: Option<u64>,
) -> bool {
    let Some(raw) = raw_wall_ms else {
        return false;
    };
    if now_ms.abs_diff(raw) > DEFAULT_MAX_CLOCK_DRIFT_MS {
        return false;
    }
    tracked_ms.is_none()
}

/// Wall time for a lease decision, or `None` when it cannot be trusted.
///
/// The first accepted reading is the monotonic anchor. A later reading that
/// does not track that anchor is skewed and must not extend a lease or prove
/// that the lease is still inside its deadline. A clock that was already wrong
/// when the anchor was taken, and that then moves with monotonic time, is not
/// detected here.
pub fn lease_wall_ms() -> Option<u64> {
    let wall = HybridLogicalClock::try_wall_clock_ms()?;
    if wall <= 1 {
        return None;
    }
    static ANCHOR: std::sync::Mutex<Option<(std::time::Instant, u64)>> =
        std::sync::Mutex::new(None);
    let mut guard = ANCHOR.lock().unwrap_or_else(|poison| poison.into_inner());
    let (origin, origin_wall) = *guard.get_or_insert_with(|| (std::time::Instant::now(), wall));
    drop(guard);
    wall_tracks_monotonic(origin.elapsed().as_millis(), origin_wall, wall)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lease_observation_fails_closed_on_fallback_rollback_and_overflow() {
        assert_eq!(observation_millis(0), None);
        assert_eq!(observation_millis(1), Some(1));
        assert_eq!(
            observation_millis((HlcTimestamp::MAX_PHYSICAL_MS as u128) + 1),
            None
        );
        assert!(HybridLogicalClock::try_wall_clock_ms().is_some_and(|ms| ms > 1));

        let applied = HlcTimestamp::new(1_700_000_000_000, 0);
        assert!(trusted_lease_observation(applied, None).is_none());
        assert!(trusted_lease_observation(applied, Some(1)).is_none());
        assert_eq!(
            trusted_lease_observation(applied, Some(applied.physical_ms())),
            Some(applied)
        );
        assert!(trusted_lease_observation(
            HlcTimestamp::new(applied.physical_ms() + 10_000, 0),
            Some(applied.physical_ms())
        )
        .is_none());

        let start = HlcTimestamp::new(1_000, 0);
        let observed = HlcTimestamp::new(1_500, 0);
        assert!(lease_window_is_legal(
            start,
            HlcTimestamp::new(61_000, 0),
            observed,
            60_000
        ));
        assert!(!lease_window_is_legal(
            start,
            HlcTimestamp::new(61_001, 0),
            observed,
            60_000
        ));
        assert!(!lease_window_is_legal(
            start,
            HlcTimestamp::new(61_000, 0),
            HlcTimestamp::new(1, 0),
            60_000
        ));
        assert!(!lease_window_is_legal(
            start,
            HlcTimestamp::new(2_000, 0),
            observed,
            u64::MAX
        ));

        let expire = HlcTimestamp::new(11_000, 0);
        assert!(lease_clock_can_extend(
            HlcTimestamp::new(10_500, 0),
            expire,
            1_000
        ));
        assert!(
            !lease_clock_can_extend(HlcTimestamp::new(1, 0), expire, 1_000),
            "the 1 ms fallback must not look inside a real lease"
        );
        assert!(
            !lease_clock_can_extend(HlcTimestamp::new(1_000, 0), expire, 1_000),
            "a clock behind the published window must not extend"
        );
        let lease_start = HlcTimestamp::new(10_000, 0);
        assert!(lease_observation_inside_window(
            HlcTimestamp::new(15_000, 0),
            lease_start,
            HlcTimestamp::new(30_000, 0)
        ));
        assert!(!lease_observation_inside_window(
            HlcTimestamp::new(1, 0),
            lease_start,
            HlcTimestamp::new(30_000, 0)
        ));
        assert!(
            !lease_observation_inside_window(
                HlcTimestamp::new(1_000, 0),
                lease_start,
                HlcTimestamp::new(30_000, 0)
            ),
            "a clock before the transaction start must not extend"
        );
        assert!(!lease_exceeds_lifetime(
            HlcTimestamp::new(1_000, 0),
            HlcTimestamp::new(61_000, 0),
            60_000
        ));
        assert!(lease_exceeds_lifetime(
            HlcTimestamp::new(1_000, 0),
            HlcTimestamp::new(61_001, 0),
            60_000
        ));

        let applied = HlcTimestamp::new(20_000, 0);
        let deadline = HlcTimestamp::new(21_000, 0);
        assert!(!lease_expiry_is_proved(applied, deadline, None));
        assert!(!lease_expiry_is_proved(applied, deadline, Some(1)));
        assert!(!lease_expiry_is_proved(
            applied,
            deadline,
            Some(applied.physical_ms() - 10_000)
        ));
        assert!(lease_expiry_is_proved(
            applied,
            deadline,
            Some(deadline.physical_ms() + 1)
        ));
        assert!(lease_expiry_is_proved(
            HlcTimestamp::new(deadline.physical_ms() + 1, 0),
            deadline,
            None
        ));
        assert_eq!(wall_tracks_monotonic(1_000, 50_000, 51_000), Some(51_000));
        assert!(wall_tracks_monotonic(1_000, 50_000, 51_000 + 501).is_none());
        assert!(wall_tracks_monotonic(1_000, 50_000, 50_000).is_none());
        assert!(wall_tracks_monotonic(0, 50_000, 1).is_none());
        assert!(lease_live_wall_is_untracked(90_000, Some(90_100), None));
        assert!(!lease_live_wall_is_untracked(
            90_000,
            Some(90_100),
            Some(90_100)
        ));
        assert!(
            !lease_live_wall_is_untracked(1_000, Some(1_700_000_000_000), None),
            "a synthetic timestamp is not the process wall"
        );
    }

    #[test]
    fn test_hlc_monotonicity_overflow_and_drift_guard() {
        let clock = HybridLogicalClock::with_max_drift_ms(500);
        let t1 = clock.now_with_wall_ms(1_000);
        let t2 = clock.now_with_wall_ms(1_000);
        let t3 = clock.now_with_wall_ms(999);
        assert!(t1 < t2 && t2 < t3);
        assert_eq!(t3.physical_ms(), 1_000);
        assert_eq!(t3.logical(), 2);

        // Verify u16::MAX overflow advances physical_ms instead of wrapping logical to 0.
        let near_max = HlcTimestamp::new(2_000, u16::MAX);
        let after_max = clock.update_checked_with_wall_ms(near_max, 2_000).unwrap();
        assert_eq!(after_max, HlcTimestamp::new(2_001, 0));
        assert!(after_max > near_max);

        // Verify drift beyond max_drift_ms is rejected, including when `remote.logical == u16::MAX`
        // at the exact `wall_ms + max_drift_ms` boundary would push `next.physical_ms()` 1ms over!
        let skewed = HlcTimestamp::new(3_000, 0);
        let err = clock
            .update_checked_with_wall_ms(skewed, 2_200)
            .unwrap_err();
        assert!(matches!(err, MetaDbError::ClockSkewExceeded { .. }));

        let boundary_overflow = HlcTimestamp::new(2_700, u16::MAX);
        let err_boundary = clock
            .update_checked_with_wall_ms(boundary_overflow, 2_200)
            .unwrap_err();
        assert!(matches!(
            err_boundary,
            MetaDbError::ClockSkewExceeded { .. }
        ));

        // Verify 48-bit physical_ms clamping in `HlcTimestamp::new`.
        let clamped = HlcTimestamp::new(u64::MAX, 42);
        assert_eq!(clamped.physical_ms(), HlcTimestamp::MAX_PHYSICAL_MS);
        assert_eq!(clamped.logical(), 42);

        // A wall clock past the 48-bit ceiling must not reset a logical counter already at that ceiling.
        let ceiling_clock = HybridLogicalClock::with_max_drift_ms(500);
        let at_ceiling_phys = ceiling_clock.now_with_wall_ms(HlcTimestamp::MAX_PHYSICAL_MS);
        assert_eq!(
            at_ceiling_phys,
            HlcTimestamp::new(HlcTimestamp::MAX_PHYSICAL_MS, 0)
        );
        let advanced = ceiling_clock.now_with_wall_ms(u64::MAX);
        assert!(advanced > at_ceiling_phys);
        assert_eq!(advanced.logical(), 1);

        let mut prev = HlcTimestamp::ZERO;
        let saturate = HybridLogicalClock::with_max_drift_ms(500);
        for _ in 0..(u16::MAX as u32 + 2) {
            let next = saturate.now_with_wall_ms(HlcTimestamp::MAX_PHYSICAL_MS);
            assert!(
                next >= prev,
                "HLC moved backwards from {:?} to {:?}",
                prev,
                next
            );
            prev = next;
        }
        let saturated = HlcTimestamp::new(HlcTimestamp::MAX_PHYSICAL_MS, u16::MAX);
        assert_eq!(prev, saturated);
        assert_eq!(saturated.next_strict(), saturated);
        assert_ne!(
            saturated.next_strict(),
            HlcTimestamp::new(HlcTimestamp::MAX_PHYSICAL_MS, 0)
        );

        let err = saturate
            .update_checked_with_wall_ms(saturated, HlcTimestamp::MAX_PHYSICAL_MS)
            .unwrap_err();
        assert!(matches!(err, MetaDbError::HlcSaturated));
    }
}
