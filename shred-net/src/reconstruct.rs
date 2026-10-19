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
    std::{collections::BTreeMap, sync::Mutex},
};

/// Data shreds per FEC set. Agave shreds every set 32 data : 32 coding, and
/// batches end on a set boundary (all 213,222 coding shreds in a 75 s mainnet
/// capture, 2026-10-19).
const SET_DATA: u64 = 32;

/// One assembler shared by the insert worker and the repair driver.
/// ponytail: a single mutex; insert and dispatch each hold it for microseconds.
pub struct Reconstructor {
    asm: Mutex<Deshredder>,
    /// Coding positions seen per (slot, fec_set_index), as a bitmask, for
    /// slots not yet complete. The deshredder recovers a set once data +
    /// coding reach 32 but does not say how close a set is.
    code: Mutex<BTreeMap<(u64, u32), u64>>,
}

impl Default for Reconstructor {
    fn default() -> Self {
        Self::new()
    }
}

impl Reconstructor {
    pub fn new() -> Self {
        Self { asm: Mutex::new(Deshredder::new()), code: Mutex::new(BTreeMap::new()) }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Deshredder> {
        self.asm.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Push many raw packets under one lock; events (entry batches, footers,
    /// slot completions) are appended to `out`. Returns the packet count.
    pub fn insert_batch(&self, raws: &[Vec<u8>], out: &mut Vec<Event>) -> usize {
        let from = out.len();
        let mut asm = self.lock();
        let mut code = self.code.lock().unwrap_or_else(|e| e.into_inner());
        for raw in raws {
            if let Some((key, position)) = coding_position(raw) {
                *code.entry(key).or_default() |= 1 << position;
            }
            asm.push(raw, out);
        }
        for ev in &out[from..] {
            if let Event::SlotComplete { slot } = ev {
                code.retain(|&(s, _), _| s != *slot);
            }
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
    ///
    /// Only as many per FEC set as its recovery still lacks: a set holding
    /// `d` data and `c` coding shreds needs `32 - d - c` more, not all
    /// `32 - d`. Turbine gives an unstaked node ~56% of each set (75 s
    /// mainnet capture, 2026-10-19), so the lost sets sit just short of
    /// recovery: 145 missing data shreds per slot, 51 needed.
    pub fn missing_indices(&self, slot: u64, max: usize) -> Vec<u64> {
        let missing = self.lock().missing(slot, usize::MAX);
        let code = self.code.lock().unwrap_or_else(|e| e.into_inner());
        needed(&missing, |set| code.get(&(slot, set)).map_or(0, |m| m.count_ones()))
            .take(max)
            .collect()
    }

    /// Forget every slot below `keep_from` (bounds memory; the assembler has
    /// no clock of its own).
    pub fn purge_below(&self, keep_from: u64) {
        self.lock().evict_below(keep_from);
        let mut code = self.code.lock().unwrap_or_else(|e| e.into_inner());
        *code = code.split_off(&(keep_from, 0));
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

/// `(slot, fec_set_index)` and position of a Merkle coding shred.
fn coding_position(raw: &[u8]) -> Option<((u64, u32), u32)> {
    if !matches!(raw.get(64)? >> 4, 0x6 | 0x7) {
        return None;
    }
    let slot = u64::from_le_bytes(raw.get(65..73)?.try_into().ok()?);
    let set = u32::from_le_bytes(raw.get(79..83)?.try_into().ok()?);
    let position = u16::from_le_bytes(raw.get(87..89)?.try_into().ok()?);
    (position < 64).then_some(((slot, set), u32::from(position)))
}

/// From ascending missing data indices, the first `missing - coding` of each
/// FEC set: enough that the set can be rebuilt once they arrive.
fn needed<'a>(
    missing: &'a [u32],
    coding: impl Fn(u32) -> u32 + 'a,
) -> impl Iterator<Item = u64> + 'a {
    missing
        .chunk_by(|a, b| u64::from(*a) / SET_DATA == u64::from(*b) / SET_DATA)
        .flat_map(move |run| {
            let set = (u64::from(run[0]) / SET_DATA * SET_DATA) as u32;
            let need = run.len().saturating_sub(coding(set) as usize);
            run[..need].iter().map(|&i| u64::from(i))
        })
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

    #[test]
    fn asks_only_what_each_fec_set_still_lacks() {
        // Set 0 lacks 5 data shreds and holds 3 coding: 2 more rebuild it.
        // Set 32 lacks 2 and holds none: both. Set 64 is already covered.
        let missing = [1, 2, 3, 4, 5, 33, 40, 70];
        let coding = |set| match set { 0 => 3, 64 => 9, _ => 0 };
        assert_eq!(needed(&missing, coding).collect::<Vec<_>>(), vec![1, 2, 33, 40]);
    }

    /// Coding shreds count toward what a gap needs; a slot one coding shred
    /// short asks for one data shred.
    #[test]
    fn coding_shreds_shrink_the_repair_ask() {
        let slot = 300;
        let shredder = Shredder::new(slot, slot - 1, 0, 0).unwrap();
        let entries: Vec<Entry> = (0..300)
            .map(|i| Entry { num_hashes: i as u64 % 4, hash: Hash::default(), transactions: vec![] })
            .collect();
        let (data, code) = shredder.entries_to_merkle_shreds_for_tests(
            &Keypair::new(), &entries, true, Hash::default(), 0, 0,
            &ReedSolomonCache::default(), &mut ProcessShredsStats::default());
        assert!(data.len() >= 4 && data.len() as u64 <= SET_DATA, "one FEC set");
        let r = Reconstructor::new();
        // Keep the last data shred (length known), drop the first three, add
        // two coding shreds: one data shred short of recovery.
        let mut ev = Vec::new();
        r.insert_batch(&data[3..].iter().chain(&code[..2]).map(raw).collect::<Vec<_>>(), &mut ev);
        assert_eq!(r.missing_indices(slot, 128), vec![0]);
        r.insert_batch(&[raw(&data[0])], &mut ev);
        assert!(r.is_full(slot), "recovered with the one data shred asked for");
        assert_eq!(entries_of(&ev), entries);
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
