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
