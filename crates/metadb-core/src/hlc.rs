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
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize,
)]
pub struct HlcTimestamp(pub u64);

impl HlcTimestamp {
    pub const ZERO: HlcTimestamp = HlcTimestamp(0);

    /// Constructs an HLC timestamp from physical milliseconds and a 16-bit logical counter.
    #[inline]
    pub const fn new(physical_ms: u64, logical: u16) -> Self {
        HlcTimestamp((physical_ms << 16) | (logical as u64))
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

    /// Returns the smallest timestamp strictly greater than `self`, advancing `physical_ms + 1`
    /// if `logical == u16::MAX` (never wrapping `u16` to `0` at the same physical millisecond).
    #[inline]
    pub const fn next_strict(self) -> Self {
        let phys = self.physical_ms();
        let log = self.logical();
        if log < u16::MAX {
            Self::new(phys, log + 1)
        } else {
            Self::new(phys + 1, 0)
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

            let next = if wall_ms > cur_phys {
                HlcTimestamp::new(wall_ms, 0).0
            } else if cur_log < u16::MAX {
                HlcTimestamp::new(cur_phys, cur_log + 1).0
            } else {
                // Never wrap `u16` to 0 at the same physical_ms; advance physical_ms by 1.
                HlcTimestamp::new(cur_phys + 1, 0).0
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
        let rem_phys = remote.physical_ms();
        if rem_phys > wall_ms.saturating_add(self.max_drift_ms) {
            return Err(MetaDbError::ClockSkewExceeded {
                remote_ms: rem_phys,
                local_ms: wall_ms,
                max_drift_ms: self.max_drift_ms,
            });
        }
        Ok(self.update_with_wall_ms(remote, wall_ms))
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
}

#[cfg(test)]
mod tests {
    use super::*;

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

        // Verify drift beyond max_drift_ms is rejected.
        let skewed = HlcTimestamp::new(3_000, 0);
        let err = clock
            .update_checked_with_wall_ms(skewed, 2_200)
            .unwrap_err();
        assert!(matches!(err, MetaDbError::ClockSkewExceeded { .. }));
    }
}
