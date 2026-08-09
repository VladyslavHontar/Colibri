//! Ephemeral, repair-driven block reconstruction over agave's `Blockstore`.
//!
//! This is the "complete lane": raw shreds (turbine + repair) are parsed and
//! inserted into a throwaway `Blockstore`, which tracks per-slot
//! completed-data-sets and `is_full()`. The repair driver asks
//! [`Reconstructor::missing_indices`] which shreds are still missing and fetches
//! them until [`Reconstructor::is_full`]; then [`Reconstructor::take_complete`]
//! yields the slot's entries and purges it.
//!
//! No FEC/erasure recovery on insert (we use the no-recovery `insert_cow_shreds`
//! path). Completeness is achieved purely by repair-until-full — re-requesting
//! the actual missing data shreds, which Phase 0 proved peers serve at firehose
//! scale. FEC recovery is a future bandwidth optimization, not a correctness
//! requirement.

use {
    solana_entry::entry::Entry,
    solana_ledger::{
        blockstore::{blockstore_purge::PurgeType, Blockstore},
        shred::Shred,
    },
    std::borrow::Cow,
    tempfile::TempDir,
};

/// Owns an ephemeral `Blockstore` in a temp dir (deleted on drop).
pub struct Reconstructor {
    blockstore: Blockstore,
    /// Low-water mark for [`Reconstructor::purge_below`]: slots `< low_water`
    /// have already been purged (so the janitor never rescans them).
    low_water: std::sync::atomic::AtomicU64,
    // Held only to keep the temp dir alive for the Blockstore's lifetime.
    _dir: TempDir,
}

impl Reconstructor {
    /// Open a fresh ephemeral blockstore in a new temp directory.
    pub fn new() -> anyhow::Result<Self> {
        let dir = tempfile::tempdir()?;
        let blockstore = Blockstore::open(dir.path())?;
        Ok(Self {
            blockstore,
            low_water: std::sync::atomic::AtomicU64::new(0),
            _dir: dir,
        })
    }

    /// Insert one raw shred packet (from turbine or the repair socket).
    /// Parses to a `Shred` and inserts without FEC recovery. Non-shred or
    /// unparseable bytes are ignored. Returns `true` if a shred was inserted.
    pub fn insert_packet(&self, raw: &[u8]) -> bool {
        let shred = match Shred::new_from_serialized_shred(raw.to_vec()) {
            Ok(s) => s,
            Err(_) => return false,
        };
        // No-recovery insert path (broadcast-stage equivalent). Completeness is
        // achieved by repair-until-full, not FEC.
        self.blockstore
            .insert_cow_shreds([Cow::Owned(shred)], None, /*is_trusted=*/ false)
            .is_ok()
    }

    /// Insert many raw shred packets in ONE `insert_cow_shreds` call. Parses
    /// each (dropping unparseable bytes) and inserts the lot together, so the
    /// rocksdb lock + write-batch commit is amortized across the whole batch
    /// instead of paid per shred. Returns the number of shreds submitted.
    pub fn insert_batch(&self, raws: &[Vec<u8>]) -> usize {
        let shreds: Vec<Cow<Shred>> = raws
            .iter()
            .filter_map(|b| Shred::new_from_serialized_shred(b.clone()).ok())
            .map(Cow::Owned)
            .collect();
        let n = shreds.len();
        let _ = self.blockstore.insert_cow_shreds(shreds, None, /*is_trusted=*/ false);
        n
    }

    /// Is `slot` fully reconstructed (all data shreds `0..=last_index` present)?
    pub fn is_full(&self, slot: u64) -> bool {
        self.blockstore
            .meta(slot)
            .ok()
            .flatten()
            .is_some_and(|m| m.is_full())
    }

    /// The slot's final shred index, if a `LAST_IN_SLOT` shred has arrived.
    /// `None` means we still need a `HighestWindowIndex` probe to learn it.
    pub fn last_index(&self, slot: u64) -> Option<u64> {
        self.blockstore.meta(slot).ok().flatten().and_then(|m| m.last_index)
    }

    /// The slot this block was built on, once any shred has arrived. Slots
    /// strictly between `parent_slot(slot)` and `slot` were never produced
    /// (skipped) — this drives skip detection in the repair driver.
    pub fn parent_slot(&self, slot: u64) -> Option<u64> {
        self.blockstore.meta(slot).ok().flatten().and_then(|m| m.parent_slot)
    }

    /// Up to `max` missing data-shred indices for `slot` — the set that drives
    /// `WindowIndex` repair requests. Empty when the slot is full or untouched.
    pub fn missing_indices(&self, slot: u64, max: usize) -> Vec<u64> {
        let meta = match self.blockstore.meta(slot).ok().flatten() {
            Some(m) => m,
            None => return Vec::new(),
        };
        // Scan to last_index+1 when known, else to the highest index seen so far.
        // first_timestamp/defer = 0 disables the recency grace period so every
        // genuine gap is reported immediately (we drive repair ourselves).
        let end = meta.last_index.map(|l| l + 1).unwrap_or(meta.received);
        self.blockstore
            .find_missing_data_indexes(slot, 0, 0, 0, end, max)
    }

    /// If `slot` is full, extract its entries and purge it from the store.
    /// Returns `None` if the slot is not yet complete.
    pub fn take_complete(&self, slot: u64) -> Option<Vec<Entry>> {
        if !self.is_full(slot) {
            return None;
        }
        let entries = self.blockstore.get_slot_entries(slot, 0).ok()?;
        // Drop the slot so the ephemeral store stays bounded.
        let _ = self.blockstore.purge_slots(slot, slot, PurgeType::Exact);
        Some(entries)
    }

    /// Purge every slot below `keep_from` to bound the ephemeral store (turbine
    /// inserts all near-tip shreds, including slots we never targeted). Tracks a
    /// low-water mark so each slot range is purged at most once.
    pub fn purge_below(&self, keep_from: u64) {
        use std::sync::atomic::Ordering;
        let from = self.low_water.load(Ordering::Relaxed);
        if keep_from > from {
            let _ = self
                .blockstore
                .purge_slots(from, keep_from - 1, PurgeType::CompactionFilter);
            self.low_water.store(keep_from, Ordering::Relaxed);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use solana_hash::Hash;
    use solana_keypair::Keypair;
    use solana_ledger::shred::{ProcessShredsStats, ReedSolomonCache, Shredder};

    /// Build a real multi-data-shred slot (one shredding call → contiguous data
    /// indices, last shred carries LAST_IN_SLOT). Many empty entries guarantee
    /// several data shreds so we can model a mid-slot gap.
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

    fn raw(shred: &Shred) -> &[u8] {
        &shred.payload().bytes
    }

    /// A whole slot inserted in ONE batched call completes and extracts cleanly
    /// (the throughput path: amortize rocksdb lock/commit over many shreds).
    #[test]
    fn batch_insert_completes_slot_and_extracts_entries() {
        let (shreds, entries) = make_multishred_slot(101);
        let raws: Vec<Vec<u8>> = shreds.iter().map(|s| raw(s).to_vec()).collect();

        let r = Reconstructor::new().unwrap();
        let n = r.insert_batch(&raws);
        assert_eq!(n, shreds.len(), "all shreds parsed+submitted in one call");

        assert!(r.is_full(101));
        assert_eq!(r.take_complete(101).unwrap(), entries);
    }

    /// `parent_slot` reports the slot this block was built on — the basis for
    /// skip detection (slots strictly between a block and its parent were never
    /// produced).
    #[test]
    fn parent_slot_reports_the_chained_parent() {
        // make_multishred_slot(N) shreds with parent = N-1.
        let (shreds, _entries) = make_multishred_slot(207);
        let r = Reconstructor::new().unwrap();
        for s in &shreds {
            r.insert_packet(raw(s));
        }
        assert_eq!(r.parent_slot(207), Some(206));
        // Untouched slot has no meta → None.
        assert_eq!(r.parent_slot(999), None);
    }

    /// The core complete-lane flow: a slot with a mid-slot gap is incomplete and
    /// reports the missing index; once that shred is repaired the slot becomes
    /// full and yields exactly the original entries, then is purged.
    #[test]
    fn gap_then_repair_completes_slot_and_extracts_entries() {
        let (shreds, entries) = make_multishred_slot(100);
        assert!(shreds.len() >= 3, "need a multi-shred slot to model a gap (got {})", shreds.len());

        // Withhold one MIDDLE data shred (keep the last so last_index is known).
        let withhold = shreds.len() / 2;
        let withheld_index = shreds[withhold].index() as u64;
        let last_index = (shreds.len() - 1) as u64;

        let r = Reconstructor::new().unwrap();
        for (i, s) in shreds.iter().enumerate() {
            if i == withhold {
                continue;
            }
            assert!(r.insert_packet(raw(s)));
        }

        // Last shred present → last_index known; but a gap remains → not full.
        assert_eq!(r.last_index(100), Some(last_index));
        assert!(!r.is_full(100), "slot must be incomplete with a gap");
        assert!(
            r.missing_indices(100, 16).contains(&withheld_index),
            "the withheld index must be reported as missing (drives repair)"
        );
        assert!(r.take_complete(100).is_none(), "incomplete slot yields nothing");

        // "Repair" delivers the missing shred.
        assert!(r.insert_packet(raw(&shreds[withhold])));

        assert!(r.is_full(100), "slot is complete once the gap is filled");
        assert!(r.missing_indices(100, 16).is_empty());

        let got = r.take_complete(100).expect("full slot yields its entries");
        assert_eq!(got, entries, "reconstructed entries must match the original");

        // After extraction the slot is purged.
        assert!(r.take_complete(100).is_none());
    }
}
