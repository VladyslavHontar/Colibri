//! Coverage measurement core — pure logic, no I/O.
//!
//! Records per-slot completion and produces a [`CoverageReport`] summarising
//! completeness and latency percentiles over the measurement window.

use std::{
    collections::HashMap,
    time::Instant,
};

// ── per-slot record ──────────────────────────────────────────────────────────

struct SlotRec {
    targeted_at:   Instant,
    completed_at:  Option<Instant>,
}

// ── public API ───────────────────────────────────────────────────────────────

/// Accumulates per-slot targeting and completion events; produces a
/// [`CoverageReport`] on demand.
pub struct CoverageMeter {
    slots: HashMap<u64, SlotRec>,
}

/// Summary snapshot returned by [`CoverageMeter::report`].
pub struct CoverageReport {
    pub targeted:         u64,
    pub completed:        u64,
    pub completeness_pct: f64,
    pub p50_ms:           u64,
    pub p99_ms:           u64,
    pub max_ms:           u64,
}

impl CoverageMeter {
    pub fn new() -> Self {
        Self { slots: HashMap::new() }
    }

    /// Record that `slot` was targeted at time `at`.  If the slot was already
    /// registered this call is a no-op (targeted_at is not moved).
    pub fn mark_targeted(&mut self, slot: u64, at: Instant) {
        self.slots.entry(slot).or_insert_with(|| SlotRec {
            targeted_at:  at,
            completed_at: None,
        });
    }

    /// Record that `slot` was completed at time `at`.
    ///
    /// Precondition: `mark_targeted` must have been called for `slot` first;
    /// completion of an unseen slot is silently ignored (no-op).
    ///
    /// Idempotent: if the slot was already completed, the first completion time
    /// is kept and subsequent calls are ignored.
    pub fn mark_complete(&mut self, slot: u64, at: Instant) {
        if let Some(rec) = self.slots.get_mut(&slot) {
            if rec.completed_at.is_none() {
                rec.completed_at = Some(at);
            }
        }
    }

    /// Produce a [`CoverageReport`] over all recorded slots.
    ///
    /// Percentiles are computed over per-slot durations
    /// (`completed_at − targeted_at`) for **completed** slots only, sorted
    /// ascending, using a floor-based formula:
    /// `index = floor(p / 100 * n)` (0-based), clamped to `[0, n−1]`.
    pub fn report(&self) -> CoverageReport {
        let targeted  = self.slots.len() as u64;
        let completed = self.slots.values()
            .filter(|r| r.completed_at.is_some())
            .count() as u64;

        let completeness_pct = if targeted == 0 {
            0.0
        } else {
            100.0 * completed as f64 / targeted as f64
        };

        // Collect durations (ms) for completed slots, sort ascending.
        let mut durations_ms: Vec<u64> = self.slots.values()
            .filter_map(|r| {
                r.completed_at.map(|c| {
                    c.saturating_duration_since(r.targeted_at).as_millis() as u64
                })
            })
            .collect();
        durations_ms.sort_unstable();

        let n = durations_ms.len();
        let (p50_ms, p99_ms, max_ms) = if n == 0 {
            (0, 0, 0)
        } else {
            // Index formula: floor(p/100 * n) used as 0-based index, clamped to [0, n-1].
            // For n=100, p=50: floor(0.50 * 100) = 50 → durations[50] = 50ms (matches test).
            let idx_p50 = ((50_f64 / 100.0 * n as f64).floor() as usize).min(n - 1);
            let idx_p99 = ((99_f64 / 100.0 * n as f64).floor() as usize).min(n - 1);
            (
                durations_ms[idx_p50],
                durations_ms[idx_p99],
                *durations_ms.last().unwrap(),
            )
        };

        CoverageReport { targeted, completed, completeness_pct, p50_ms, p99_ms, max_ms }
    }
}

// ── unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    #[test]
    fn completeness_pct_counts_only_full_slots() {
        let mut m = CoverageMeter::new();
        let t0 = Instant::now();
        for s in 0..1000u64 { m.mark_targeted(s, t0); }
        for s in 0..999u64 { m.mark_complete(s, t0 + Duration::from_millis(300)); }
        let r = m.report();
        assert_eq!(r.targeted, 1000);
        assert_eq!(r.completed, 999);
        assert!((r.completeness_pct - 99.9).abs() < 1e-9);
    }

    #[test]
    fn latency_percentiles_are_time_to_complete() {
        let mut m = CoverageMeter::new();
        let t0 = Instant::now();
        for s in 0..100u64 {
            m.mark_targeted(s, t0);
            m.mark_complete(s, t0 + Duration::from_millis(s));
        }
        let r = m.report();
        assert_eq!(r.p50_ms, 50);
        assert_eq!(r.max_ms, 99);
    }

    #[test]
    fn double_complete_is_idempotent() {
        let mut m = CoverageMeter::new();
        let t0 = Instant::now();
        m.mark_targeted(1, t0);
        m.mark_complete(1, t0 + Duration::from_millis(10));
        m.mark_complete(1, t0 + Duration::from_millis(20)); // ignored
        let r = m.report();
        assert_eq!(r.completed, 1);
        assert_eq!(r.max_ms, 10);
    }
}
