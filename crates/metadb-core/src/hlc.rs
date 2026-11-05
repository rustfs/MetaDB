use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// A 64-bit Hybrid Logical Clock (HLC) timestamp packing:
/// - Upper 48 bits: physical wall-clock time in milliseconds since UNIX epoch.
/// - Lower 16 bits: monotonically increasing logical counter within the same millisecond.
///
/// As described in §8 of the Baidu MetaDB paper (`arXiv:2610.32247v1`), replacing a
/// centralized Timestamp Oracle (TSO) with a decentralized HLC removes a cluster-wide
/// write availability bottleneck while preserving causal ordering across nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
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
}

/// Thread-safe Hybrid Logical Clock generator.
#[derive(Debug)]
pub struct HybridLogicalClock {
    state: AtomicU64,
}

impl Default for HybridLogicalClock {
    fn default() -> Self {
        Self::new()
    }
}

impl HybridLogicalClock {
    /// Creates a new HLC initialized at zero.
    pub const fn new() -> Self {
        Self {
            state: AtomicU64::new(0),
        }
    }

    /// Generates a strictly monotonic timestamp for a local event.
    pub fn now(&self) -> HlcTimestamp {
        let wall_ms = Self::wall_clock_ms();
        self.now_with_wall_ms(wall_ms)
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
                // If 65,535 events occur in the same millisecond, advance physical millisecond by 1.
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

    /// Updates the local HLC upon receiving a message with a remote `HlcTimestamp`
    /// and returns a new timestamp strictly greater than both local and remote states.
    pub fn update(&self, remote: HlcTimestamp) -> HlcTimestamp {
        let wall_ms = Self::wall_clock_ms();
        loop {
            let current = self.state.load(Ordering::Acquire);
            let cur_phys = current >> 16;
            let cur_log = (current & 0xFFFF) as u16;
            let rem_phys = remote.physical_ms();
            let rem_log = remote.logical();

            let max_phys = wall_ms.max(cur_phys).max(rem_phys);
            let next_log = if max_phys == cur_phys && max_phys == rem_phys {
                cur_log.max(rem_log).saturating_add(1)
            } else if max_phys == cur_phys {
                cur_log.saturating_add(1)
            } else if max_phys == rem_phys {
                rem_log.saturating_add(1)
            } else {
                0
            };

            let next = if next_log == u16::MAX {
                HlcTimestamp::new(max_phys + 1, 0).0
            } else {
                HlcTimestamp::new(max_phys, next_log).0
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

    fn wall_clock_ms() -> u64 {
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
    fn test_hlc_monotonicity_and_remote_causal_update() {
        let clock = HybridLogicalClock::new();
        let t1 = clock.now_with_wall_ms(1_000);
        let t2 = clock.now_with_wall_ms(1_000);
        let t3 = clock.now_with_wall_ms(999); // physical clock skew backward
        assert!(t1 < t2);
        assert!(t2 < t3);
        assert_eq!(t3.physical_ms(), 1_000);
        assert_eq!(t3.logical(), 2);

        let remote_future = HlcTimestamp::new(5_000, 7);
        let t4 = clock.update(remote_future);
        assert!(t4 > remote_future);
        assert!(t4.physical_ms() >= 5_000);
    }
}
