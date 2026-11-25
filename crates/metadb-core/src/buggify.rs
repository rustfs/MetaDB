//! Deterministic FoundationDB-style `Buggify` fault-activation and `CodeProbe` rare-path
//! coverage tracking for simulation and chaos testing.
//!
//! Design properties:
//! - **Zero production overhead**: When neither a thread-local nor global `Buggify` session is
//!   installed, [`buggify`] performs a single `Ordering::Relaxed` atomic load (`== 0`) and returns `false`.
//! - **Two-stage FDB `BUGGIFY` semantics**: Given a `seed`, each named site first deterministically
//!   decides whether that site is *enabled* for the entire session (`enable_permille`), and only
//!   enabled sites fire on individual calls (`fire_permille`). This creates realistic correlated
//!   fault bursts on specific subsystems per seed rather than uniform white noise.
//! - **Parallel-test safe `CodeProbe` snapshots**: [`code_probe`] increments monotonic per-site
//!   atomic counters without global resets, allowing concurrent tests or ensemble runners to measure
//!   exact hit deltas via [`CodeProbeSnapshot`].

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock};

static ACTIVE_BUGGIFY_COUNT: AtomicUsize = AtomicUsize::new(0);

#[derive(Debug, Clone, Copy)]
pub struct BuggifyConfig {
    /// Probability in permille (`0..=1000`) that any given named `buggify` site is enabled for this seed.
    pub enable_permille: u16,
    /// Probability in permille (`0..=1000`) that an enabled site fires on each evaluation.
    pub fire_permille: u16,
}

impl Default for BuggifyConfig {
    fn default() -> Self {
        Self {
            enable_permille: 450,
            fire_permille: 250,
        }
    }
}

impl BuggifyConfig {
    pub const fn aggressive() -> Self {
        Self {
            enable_permille: 700,
            fire_permille: 350,
        }
    }
}

#[inline]
pub fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[inline]
pub fn fnv1a_hash(s: &str) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64;
    for &b in s.as_bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[inline]
fn site_is_enabled(seed: u64, site_hash: u64, enable_permille: u16) -> bool {
    if enable_permille == 0 {
        return false;
    }
    if enable_permille >= 1000 {
        return true;
    }
    let roll = splitmix64(seed ^ site_hash) % 1000;
    roll < u64::from(enable_permille)
}

#[inline]
fn site_fires(seed: u64, site_hash: u64, call_index: u64, fire_permille: u16) -> bool {
    if fire_permille == 0 {
        return false;
    }
    if fire_permille >= 1000 {
        return true;
    }
    let mixed = splitmix64(
        seed ^ site_hash.rotate_left(17) ^ call_index.wrapping_mul(0xD6E8_FEB8_6659_FD93),
    );
    (mixed % 1000) < u64::from(fire_permille)
}

struct ThreadBuggifyState {
    seed: u64,
    config: BuggifyConfig,
    calls: u64,
    firings: u64,
}

thread_local! {
    static LOCAL_BUGGIFY: RefCell<Option<ThreadBuggifyState>> = const { RefCell::new(None) };
}

struct GlobalBuggifySession {
    seed: u64,
    config: BuggifyConfig,
    calls: AtomicU64,
    firings: AtomicU64,
}

fn global_session_slot() -> &'static RwLock<Option<Arc<GlobalBuggifySession>>> {
    static SLOT: OnceLock<RwLock<Option<Arc<GlobalBuggifySession>>>> = OnceLock::new();
    SLOT.get_or_init(|| RwLock::new(None))
}

/// RAII guard for a thread-local deterministic `Buggify` session (used by single-thread in-memory replay).
pub struct ThreadBuggifyGuard {
    active: bool,
}

impl ThreadBuggifyGuard {
    pub fn install(seed: u64, config: BuggifyConfig) -> Self {
        ACTIVE_BUGGIFY_COUNT.fetch_add(1, Ordering::AcqRel);
        LOCAL_BUGGIFY.with(|slot| {
            *slot.borrow_mut() = Some(ThreadBuggifyState {
                seed,
                config,
                calls: 0,
                firings: 0,
            });
        });
        Self { active: true }
    }

    pub fn firings(&self) -> u64 {
        LOCAL_BUGGIFY.with(|slot| slot.borrow().as_ref().map(|s| s.firings).unwrap_or(0))
    }
}

impl Drop for ThreadBuggifyGuard {
    fn drop(&mut self) {
        if self.active {
            LOCAL_BUGGIFY.with(|slot| {
                *slot.borrow_mut() = None;
            });
            ACTIVE_BUGGIFY_COUNT.fetch_sub(1, Ordering::AcqRel);
        }
    }
}

/// RAII guard for a process-wide `Buggify` session visible across Tokio worker threads and OpenRaft tasks.
pub struct GlobalBuggifyGuard {
    session: Arc<GlobalBuggifySession>,
}

impl GlobalBuggifyGuard {
    pub fn install(seed: u64, config: BuggifyConfig) -> Self {
        let session = Arc::new(GlobalBuggifySession {
            seed,
            config,
            calls: AtomicU64::new(0),
            firings: AtomicU64::new(0),
        });
        ACTIVE_BUGGIFY_COUNT.fetch_add(1, Ordering::AcqRel);
        *global_session_slot().write().unwrap() = Some(session.clone());
        Self { session }
    }

    pub fn firings(&self) -> u64 {
        self.session.firings.load(Ordering::Acquire)
    }

    pub fn calls(&self) -> u64 {
        self.session.calls.load(Ordering::Acquire)
    }
}

impl Drop for GlobalBuggifyGuard {
    fn drop(&mut self) {
        let mut guard = global_session_slot().write().unwrap();
        if guard
            .as_ref()
            .is_some_and(|cur| Arc::ptr_eq(cur, &self.session))
        {
            *guard = None;
        }
        drop(guard);
        ACTIVE_BUGGIFY_COUNT.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Evaluates a named `Buggify` fault injection site.
/// Returns `false` in ~1 CPU cycle when no simulation `Buggify` guard is installed.
#[inline]
pub fn buggify(site: &'static str) -> bool {
    if ACTIVE_BUGGIFY_COUNT.load(Ordering::Relaxed) == 0 {
        return false;
    }
    buggify_slow(site)
}

#[cold]
fn buggify_slow(site: &'static str) -> bool {
    let site_hash = fnv1a_hash(site);
    let local_decision = LOCAL_BUGGIFY.with(|slot| {
        let mut borrow = slot.borrow_mut();
        let state = borrow.as_mut()?;
        let call_idx = state.calls;
        state.calls = state.calls.wrapping_add(1);
        let fired = site_is_enabled(state.seed, site_hash, state.config.enable_permille)
            && site_fires(state.seed, site_hash, call_idx, state.config.fire_permille);
        if fired {
            state.firings = state.firings.wrapping_add(1);
        }
        Some(fired)
    });
    if let Some(fired) = local_decision {
        if fired {
            code_probe(site);
        }
        return fired;
    }

    let session = global_session_slot().read().unwrap().clone();
    let Some(session) = session else {
        return false;
    };
    let call_idx = session.calls.fetch_add(1, Ordering::Relaxed);
    let fired = site_is_enabled(session.seed, site_hash, session.config.enable_permille)
        && site_fires(
            session.seed,
            site_hash,
            call_idx,
            session.config.fire_permille,
        );
    if fired {
        session.firings.fetch_add(1, Ordering::Relaxed);
        code_probe(site);
    }
    fired
}

fn probe_map() -> &'static RwLock<BTreeMap<&'static str, Arc<AtomicU64>>> {
    static PROBES: OnceLock<RwLock<BTreeMap<&'static str, Arc<AtomicU64>>>> = OnceLock::new();
    PROBES.get_or_init(|| RwLock::new(BTreeMap::new()))
}

/// Records execution of a rare fault, recovery, or corner-case code path (FDB `CODE_PROBE` equivalent).
#[inline]
pub fn code_probe(probe_id: &'static str) {
    if let Some(counter) = probe_map().read().unwrap().get(probe_id) {
        counter.fetch_add(1, Ordering::Relaxed);
        return;
    }
    let mut writer = probe_map().write().unwrap();
    let counter = writer
        .entry(probe_id)
        .or_insert_with(|| Arc::new(AtomicU64::new(0)))
        .clone();
    drop(writer);
    counter.fetch_add(1, Ordering::Relaxed);
}

/// Monotonic snapshot of all `CodeProbe` counters for delta calculation across test/ensemble runs.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodeProbeSnapshot {
    counts: BTreeMap<&'static str, u64>,
}

impl CodeProbeSnapshot {
    pub fn capture() -> Self {
        let reader = probe_map().read().unwrap();
        let counts = reader
            .iter()
            .map(|(&k, v)| (k, v.load(Ordering::Acquire)))
            .collect();
        Self { counts }
    }

    /// Computes per-probe hit deltas accumulated since `earlier`.
    pub fn delta_since(&self, earlier: &Self) -> BTreeMap<&'static str, u64> {
        let mut out = BTreeMap::new();
        for (&k, &now_val) in &self.counts {
            let prev = earlier.counts.get(k).copied().unwrap_or(0);
            let diff = now_val.saturating_sub(prev);
            if diff > 0 {
                out.insert(k, diff);
            }
        }
        out
    }

    pub fn get(&self, probe_id: &str) -> u64 {
        self.counts.get(probe_id).copied().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_buggify_never_fires() {
        assert!(!buggify("test.disabled.site"));
    }

    #[test]
    fn thread_buggify_is_pure_function_of_seed() {
        let run_once = |seed: u64| -> (Vec<bool>, u64) {
            let guard = ThreadBuggifyGuard::install(
                seed,
                BuggifyConfig {
                    enable_permille: 600,
                    fire_permille: 400,
                },
            );
            let mut decisions = Vec::with_capacity(32);
            for _ in 0..16 {
                decisions.push(buggify("test.alpha"));
                decisions.push(buggify("test.beta"));
            }
            (decisions, guard.firings())
        };
        let a1 = run_once(0xDEAD_BEEF);
        let a2 = run_once(0xDEAD_BEEF);
        let b = run_once(0xCAFE_BABE);
        assert_eq!(a1, a2);
        assert_ne!(a1.0, b.0);
    }

    #[test]
    fn code_probe_snapshot_tracks_exact_deltas() {
        let before = CodeProbeSnapshot::capture();
        code_probe("probe.unit.example");
        code_probe("probe.unit.example");
        let after = CodeProbeSnapshot::capture();
        let delta = after.delta_since(&before);
        assert_eq!(delta.get("probe.unit.example"), Some(&2));
    }
}
