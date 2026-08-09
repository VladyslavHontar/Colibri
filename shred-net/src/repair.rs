//! Repair policy: decide what to ask peers for, per targeted slot.
//!
//! Pure decision core, re-seamed onto [`crate::reconstruct::Reconstructor`]: the
//! driver feeds in the slot's reconstruction state (`is_full`, `last_index`, and
//! the `missing` data-shred indices the Blockstore reports) and gets back the
//! single action to take this cycle. Ported from Colibri's Phase-0-proven
//! `next_repair_action`, but its inputs now come from the Blockstore's `SlotMeta`
//! rather than a hand-rolled `HashSet`/`last_index` tracker.

/// Actions the repair driver may take for a single slot in one cycle.
#[derive(Debug, PartialEq, Eq)]
pub enum RepairAction {
    /// Send `HighestWindowIndex(slot, 0)` to discover `last_index`.
    ProbeHighest,
    /// Send `WindowIndex(slot, i)` for each missing index.
    RequestWindows(Vec<u64>),
    /// Send `Orphan(slot)` — fallback when `last_index` stays unknown.
    RequestOrphan,
    /// Slot is fully assembled — extract & drop it.
    Complete,
    /// Nothing to do this round (waiting on a HighestWindowIndex response).
    Wait,
}

/// Pure, unit-testable repair-decision function.
///
/// * `is_full`        — the Blockstore says every data shred `0..=last_index` is present.
/// * `last_index`     — final shred index, or `None` if no `LAST_IN_SLOT` shred yet.
/// * `missing`        — missing data-shred indices (from `find_missing_data_indexes`).
/// * `highest_probed` — whether a `HighestWindowIndex` was already sent.
/// * `repair_rounds`  — repair cycles elapsed for this slot.
/// * `orphan_after`   — rounds after which we fall back to `Orphan`.
pub fn next_repair_action(
    is_full:        bool,
    last_index:     Option<u64>,
    missing:        &[u64],
    highest_probed: bool,
    repair_rounds:  u32,
    orphan_after:   u32,
) -> RepairAction {
    if is_full {
        return RepairAction::Complete;
    }
    match last_index {
        None if !highest_probed => RepairAction::ProbeHighest,
        None if repair_rounds >= orphan_after => RepairAction::RequestOrphan,
        None => RepairAction::Wait,
        Some(_) => {
            if missing.is_empty() {
                RepairAction::Complete
            } else {
                RepairAction::RequestWindows(missing.to_vec())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn probe_highest_when_no_last_index_and_not_probed() {
        assert_eq!(
            next_repair_action(false, None, &[], false, 0, 5),
            RepairAction::ProbeHighest,
        );
    }

    #[test]
    fn wait_after_probe_until_orphan_threshold() {
        // probed but rounds < orphan_after → Wait
        assert_eq!(
            next_repair_action(false, None, &[], true, 3, 5),
            RepairAction::Wait,
        );
    }

    #[test]
    fn request_orphan_after_threshold() {
        assert_eq!(
            next_repair_action(false, None, &[], true, 5, 5),
            RepairAction::RequestOrphan,
        );
    }

    #[test]
    fn complete_when_full() {
        // is_full short-circuits everything else.
        assert_eq!(
            next_repair_action(true, Some(2), &[], false, 0, 5),
            RepairAction::Complete,
        );
    }

    #[test]
    fn request_windows_for_reported_gaps() {
        assert_eq!(
            next_repair_action(false, Some(2), &[1], false, 0, 5),
            RepairAction::RequestWindows(vec![1]),
        );
    }

    #[test]
    fn complete_when_last_index_known_and_nothing_missing() {
        // Defensive: last_index known, no gaps reported, but is_full not yet set.
        assert_eq!(
            next_repair_action(false, Some(2), &[], false, 0, 5),
            RepairAction::Complete,
        );
    }
}
