//! Repair-driven block reconstruction over the deshredder.
//!
//! This is the "complete lane": every raw shred (turbine + repair) is pushed
//! into one [`Deshredder`], which rebuilds lost data shreds from coding
//! shreds (Reed–Solomon) and emits each entry batch the moment it is
//! contiguous. The repair driver asks [`Reconstructor::missing_indices`]
//! which data shreds are still absent and fetches them until
//! [`Reconstructor::is_full`]; nothing is stored on disk and nothing is read
//! back — the entries leave through the events `insert_batch` returns.

use {
    deshredder::{Deshredder, Event},
    std::sync::Mutex,
};

/// One assembler shared by the insert worker and the repair driver.
/// ponytail: a single mutex; insert and dispatch each hold it for microseconds.
pub struct Reconstructor {
    asm: Mutex<Deshredder>,
}

impl Default for Reconstructor {
    fn default() -> Self {
        Self::new()
    }
}

impl Reconstructor {
    pub fn new() -> Self {
        Self { asm: Mutex::new(Deshredder::new()) }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Deshredder> {
        self.asm.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Push many raw packets under one lock; events (entry batches, footers,
    /// slot completions) are appended to `out`. Returns the packet count.
    pub fn insert_batch(&self, raws: &[Vec<u8>], out: &mut Vec<Event>) -> usize {
        let mut asm = self.lock();
        for raw in raws {
            asm.push(raw, out);
        }
        raws.len()
    }

    /// Is `slot` fully reconstructed (every batch through LAST_IN_SLOT emitted)?
    pub fn is_full(&self, slot: u64) -> bool {
        self.lock().is_complete(slot)
    }

    /// The slot's final shred index, if a `LAST_IN_SLOT` shred has arrived.
    /// `None` means we still need a `HighestWindowIndex` probe to learn it.
    pub fn last_index(&self, slot: u64) -> Option<u64> {
        self.lock().last_index(slot).map(u64::from)
    }

    /// The slot this block was built on, once any data shred has arrived.
    /// Slots strictly between `parent_slot(slot)` and `slot` were never
    /// produced (skipped) — this drives skip detection in the repair driver.
    pub fn parent_slot(&self, slot: u64) -> Option<u64> {
        self.lock().parent_slot(slot)
    }

    /// Up to `max` missing data-shred indices for `slot` — the set that drives
    /// `WindowIndex` repair requests. Empty when the slot is full or untouched.
    pub fn missing_indices(&self, slot: u64, max: usize) -> Vec<u64> {
        self.lock().missing(slot, max).into_iter().map(u64::from).collect()
    }

    /// Forget every slot below `keep_from` (bounds memory; the assembler has
    /// no clock of its own).
    pub fn purge_below(&self, keep_from: u64) {
        self.lock().evict_below(keep_from);
    }

    /// Highest data index seen for `slot` (diagnostics).
    pub fn received(&self, slot: u64) -> Option<u64> {
        self.lock().received(slot).map(u64::from)
    }

    /// Slots currently being assembled.
    pub fn active_slots(&self) -> usize {
        self.lock().active_slots()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deshredder::Entry;
    use solana_hash::Hash;
    use solana_keypair::Keypair;
    use solana_ledger::shred::{ProcessShredsStats, ReedSolomonCache, Shred, Shredder};

    /// Build a real multi-data-shred slot (one shredding call → contiguous data
    /// indices, last shred carries LAST_IN_SLOT). Many empty entries guarantee
    /// several data shreds so we can model a mid-slot gap. Coding shreds are
    /// dropped so the gap cannot be erasure-recovered.
    fn make_multishred_slot(slot: u64) -> (Vec<Shred>, Vec<Entry>) {
        let parent = slot.saturating_sub(1);
        let keypair = Keypair::new();
        let entries: Vec<Entry> = (0..300)
            .map(|i| Entry { num_hashes: i as u64 % 4, hash: Hash::default(), transactions: vec![] })
            .collect();
        let shredder = Shredder::new(slot, parent, 0, 0).unwrap();
        let cache = ReedSolomonCache::default();
        let (data, _coding) = shredder.entries_to_merkle_shreds_for_tests(
            &keypair,
            &entries,
            true, // is_last_in_slot → final data shred gets LAST_IN_SLOT
            Hash::default(),
            0, 0,
            &cache,
            &mut ProcessShredsStats::default(),
        );
        (data, entries)
    }

    fn raw(shred: &Shred) -> Vec<u8> {
        shred.payload().bytes.to_vec()
    }

    fn entries_of(events: &[Event]) -> Vec<Entry> {
        events.iter().filter_map(|e| match e {
            Event::Entries { entries, .. } => Some(entries.clone()),
            _ => None,
        }).flatten().collect()
    }

    /// A whole slot inserted in ONE batched call completes and its events
    /// carry exactly the original entries, ending in SlotComplete.
    #[test]
    fn batch_insert_completes_slot_and_emits_entries() {
        let (shreds, entries) = make_multishred_slot(101);
        let raws: Vec<Vec<u8>> = shreds.iter().map(raw).collect();

        let r = Reconstructor::new();
        let mut events = Vec::new();
        assert_eq!(r.insert_batch(&raws, &mut events), shreds.len());

        assert!(r.is_full(101));
        assert_eq!(entries_of(&events), entries);
        assert!(matches!(events.last(), Some(Event::SlotComplete { slot: 101 })));
    }

    /// `parent_slot` reports the slot this block was built on — the basis for
    /// skip detection — and still answers after the slot completed.
    #[test]
    fn parent_slot_reports_the_chained_parent() {
        let (shreds, _entries) = make_multishred_slot(207);
        let r = Reconstructor::new();
        r.insert_batch(&[raw(&shreds[0])], &mut Vec::new());
        assert_eq!(r.parent_slot(207), Some(206));
        r.insert_batch(&shreds[1..].iter().map(raw).collect::<Vec<_>>(), &mut Vec::new());
        assert!(r.is_full(207));
        assert_eq!(r.parent_slot(207), Some(206), "known after completion, until purge");
        assert_eq!(r.parent_slot(999), None);
        r.purge_below(208);
        assert_eq!(r.parent_slot(207), None);
    }

    /// The core complete-lane flow: a slot with a mid-slot gap is incomplete and
    /// reports the missing index; once that shred is repaired the slot becomes
    /// full and the events add up to exactly the original entries.
    #[test]
    fn gap_then_repair_completes_slot_and_emits_entries() {
        let (shreds, entries) = make_multishred_slot(100);
        assert!(shreds.len() >= 3, "need a multi-shred slot to model a gap (got {})", shreds.len());

        // Withhold one MIDDLE data shred (keep the last so last_index is known).
        let withhold = shreds.len() / 2;
        let withheld_index = shreds[withhold].index() as u64;
        let last_index = (shreds.len() - 1) as u64;

        let r = Reconstructor::new();
        let mut events = Vec::new();
        let raws: Vec<Vec<u8>> = shreds.iter().enumerate()
            .filter(|(i, _)| *i != withhold).map(|(_, s)| raw(s)).collect();
        r.insert_batch(&raws, &mut events);

        assert_eq!(r.last_index(100), Some(last_index));
        assert!(!r.is_full(100), "slot must be incomplete with a gap");
        assert!(
            r.missing_indices(100, 16).contains(&withheld_index),
            "the withheld index must be reported as missing (drives repair)"
        );
        assert!(!matches!(events.last(), Some(Event::SlotComplete { .. })));

        // "Repair" delivers the missing shred.
        r.insert_batch(&[raw(&shreds[withhold])], &mut events);

        assert!(r.is_full(100), "slot is complete once the gap is filled");
        assert!(r.missing_indices(100, 16).is_empty());
        assert_eq!(entries_of(&events), entries, "reconstructed entries must match the original");
        assert!(matches!(events.last(), Some(Event::SlotComplete { slot: 100 })));
    }
}
