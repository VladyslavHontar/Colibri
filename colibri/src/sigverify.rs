//! Shred signature verification, keyed by the slot's leader.
//!
//! # Why this exists
//!
//! Without leader-keyed sigverify, ANY host that can reach our TVU/repair sockets
//! can inject forged shreds; the deshredder will happily assemble them into fake
//! entries and stream them to gRPC subscribers → corrupted downstream state.
//! Agave verifies every shred against the slot leader's pubkey before ingest
//! (`ledger/src/sigverify_shreds.rs::verify_shred_cpu`) and DROPS on failure or
//! unknown leader. This module reproduces that check, and the ingest policy in
//! `main.rs` reproduces agave's fail-closed stance for the production feed.
//!
//! # Scheme (faithful to agave `verify_shred_cpu` / `Shred::verify`)
//!
//! For Merkle shreds (the only variant mainnet produces today) the leader signs
//! the shred's **Merkle root**, not the raw bytes. Verification is:
//!
//! ```text
//!   sig  = shred[0..64]                                   // common-header signature
//!   root = shred::layout::get_merkle_root(shred)          // walks the embedded proof
//!   ok   = sig.verify(leader_pubkey.as_ref(), root.as_ref())
//! ```
//!
//! This mirrors agave exactly (see `verify_shred_cpu`), minus the process-wide LRU
//! cache of `(signature, pubkey, merkle_root)` triples — that is a throughput
//! optimization, not part of the correctness contract.
//!
//! # Leader-schedule source (see `LeaderScheduleCache`)
//!
//! The cache holds one leader map PER EPOCH, keyed by epoch number. It is bounded
//! (`MAX_CACHED_EPOCHS`, oldest evicted). Epoch↔slot arithmetic comes from a single
//! `getEpochSchedule` fetch (`EpochMath`), matching agave's `EpochSchedule`.
//!
//! Two lookup/refresh phases, split so ingest NEVER blocks on an RPC round-trip:
//!   * `leader_for_slot(slot)` — pure, in-memory. Returns the slot's leader if its
//!     epoch schedule is loaded, else `None` AND records the missing epoch in a
//!     `pending` set (so a background thread can fetch it). No I/O, no lock held
//!     across a socket.
//!   * `bootstrap` / `service_pending` — the background schedule thread calls these.
//!     Each does its blocking RPC OUTSIDE the cache lock, then installs the result
//!     under a brief lock. This is the mutex-across-RPC fix.
//!
//! # Ingest policy (enforced in `main.rs`, agave-faithful, fail-closed)
//!   * leader known  → verify; DROP on signature failure (== agave).
//!   * leader unknown (schedule not loaded / unfetchable epoch) → DROP; record the
//!     epoch for on-demand fetch. The money path (deshredder → gRPC entry+tx stream)
//!     NEVER emits an unverified shred, `observed_tip` NEVER advances from one, and
//!     coverage NEVER completes from one. There is no admit-on-unknown branch.

use {
    solana_ledger::shred::layout,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::{HashMap, HashSet},
        str::FromStr,
    },
};

/// Upper bound on cached per-epoch schedules. The money path only needs the
/// current epoch (plus the one just crossed at a boundary); the Phase-0 harness
/// probes at most a few epochs back. Oldest epochs are evicted past this.
const MAX_CACHED_EPOCHS: usize = 4;

/// Verify a single shred's signature against the given slot leader.
///
/// Returns `true` iff the shred is a well-formed Merkle shred whose common-header
/// signature is a valid ed25519 signature by `leader` over the shred's Merkle root.
/// Legacy (non-Merkle) shreds and malformed buffers return `false`.
#[must_use]
pub fn verify_shred_signature(shred: &[u8], leader: &Pubkey) -> bool {
    // Signature occupies the first 64 bytes of the common header (agave
    // `shred::wire::get_signature`: `shred[0..64]`).
    let Some(sig_bytes) = shred.get(..64) else {
        return false;
    };
    let sig = Signature::from(<[u8; 64]>::try_from(sig_bytes).unwrap());

    // Merkle root is the signed message. `get_merkle_root` walks the proof embedded
    // in the shred payload; `None` for legacy/malformed shreds.
    let Some(root) = layout::get_merkle_root(shred) else {
        return false;
    };

    sig.verify(leader.as_ref(), root.as_ref())
}

/// Epoch↔slot arithmetic, mirroring agave's `EpochSchedule` in the post-warmup
/// (normal) regime. Sourced once from `getEpochSchedule`.
///
/// Warmup epochs (`slot < first_normal_slot`) return `None` — they are ancient
/// (mainnet left warmup years ago) and never fall in the tip or Phase-0 probe
/// range; refusing them is fail-closed, not a functional gap.
#[derive(Clone, Copy, Debug)]
pub struct EpochMath {
    slots_per_epoch:    u64,
    first_normal_epoch: u64,
    first_normal_slot:  u64,
}

impl EpochMath {
    /// Epoch containing `slot`, or `None` if `slot` is in the warmup region.
    pub fn epoch_of(&self, slot: u64) -> Option<u64> {
        if self.slots_per_epoch == 0 || slot < self.first_normal_slot {
            return None;
        }
        Some(self.first_normal_epoch + (slot - self.first_normal_slot) / self.slots_per_epoch)
    }

    /// Absolute first slot of `epoch`, or `None` for warmup epochs.
    pub fn first_slot_of_epoch(&self, epoch: u64) -> Option<u64> {
        if epoch < self.first_normal_epoch {
            return None;
        }
        Some(self.first_normal_slot + (epoch - self.first_normal_epoch) * self.slots_per_epoch)
    }
}

/// One epoch's slot→leader map.
struct EpochLeaders {
    first_slot: u64,
    end_slot:   u64, // exclusive
    leaders:    Vec<Pubkey>,
}

impl EpochLeaders {
    fn leader_for_slot(&self, slot: u64) -> Option<Pubkey> {
        if slot < self.first_slot || slot >= self.end_slot {
            return None;
        }
        self.leaders.get((slot - self.first_slot) as usize).copied()
    }
}

/// Per-epoch leader schedules + on-demand fetch bookkeeping.
///
/// Lookups are pure/in-memory; fetches are performed out-of-band by the
/// background schedule thread (see module docs) so ingest never blocks on RPC.
pub struct LeaderScheduleCache {
    math:      Option<EpochMath>,
    /// epoch → leader map. Bounded to `MAX_CACHED_EPOCHS` (oldest evicted).
    schedules: HashMap<u64, EpochLeaders>,
    /// Epochs a `leader_for_slot` miss asked for but which are not yet loaded.
    /// Drained by the background thread.
    pending:   HashSet<u64>,
}

impl Default for LeaderScheduleCache {
    fn default() -> Self {
        Self::new()
    }
}

impl LeaderScheduleCache {
    pub fn new() -> Self {
        Self {
            math:      None,
            schedules: HashMap::new(),
            pending:   HashSet::new(),
        }
    }

    // ── pure, in-memory lookup (ingest hot path) ─────────────────────────────

    /// Leader for `slot`, or `None` if unknown. On a miss (epoch not loaded) the
    /// missing epoch is recorded in `pending` for the background thread to fetch.
    /// Performs NO I/O and holds NO lock across a network call — safe to invoke
    /// per-shred under the cache mutex.
    pub fn leader_for_slot(&mut self, slot: u64) -> Option<Pubkey> {
        let Some(math) = self.math else {
            // Epoch math not bootstrapped yet → whole tip stream is unknown-leader
            // and must be dropped (fail-closed). Background thread bootstraps ASAP.
            return None;
        };
        let Some(epoch) = math.epoch_of(slot) else {
            return None; // warmup / nonsensical slot
        };
        match self.schedules.get(&epoch) {
            Some(sched) => sched.leader_for_slot(slot),
            None => {
                self.pending.insert(epoch);
                None
            }
        }
    }

    /// True once epoch math is known AND at least one epoch schedule is loaded.
    /// Until then the money path emits nothing (fail-closed startup).
    #[allow(dead_code)] // exercised in tests / diagnostics
    pub fn is_ready(&self) -> bool {
        self.math.is_some() && !self.schedules.is_empty()
    }

    /// (min_first_slot, max_end_slot) across loaded epochs, for logging.
    pub fn loaded_range(&self) -> (u64, u64) {
        let lo = self.schedules.values().map(|s| s.first_slot).min().unwrap_or(0);
        let hi = self.schedules.values().map(|s| s.end_slot).max().unwrap_or(0);
        (lo, hi)
    }

    // ── background-thread install/query helpers (brief locks only) ───────────

    pub fn math(&self) -> Option<EpochMath> {
        self.math
    }

    pub fn install_math(&mut self, math: EpochMath) {
        self.math = Some(math);
    }

    pub fn has_epoch(&self, epoch: u64) -> bool {
        self.schedules.contains_key(&epoch)
    }

    /// Install a freshly-fetched epoch schedule and evict the oldest epochs beyond
    /// `MAX_CACHED_EPOCHS`. Also clears the epoch from `pending`.
    pub fn install_epoch(&mut self, epoch: u64, first_slot: u64, end_slot: u64, leaders: Vec<Pubkey>) {
        self.schedules.insert(epoch, EpochLeaders { first_slot, end_slot, leaders });
        self.pending.remove(&epoch);
        while self.schedules.len() > MAX_CACHED_EPOCHS {
            if let Some(&oldest) = self.schedules.keys().min() {
                self.schedules.remove(&oldest);
            } else {
                break;
            }
        }
    }

    /// Take and clear the set of epochs awaiting fetch.
    pub fn take_pending(&mut self) -> Vec<u64> {
        let out: Vec<u64> = self.pending.iter().copied().collect();
        self.pending.clear();
        out
    }
}

// ── free-function RPC fetchers (called by the background thread OUTSIDE the lock)

/// Fetch `EpochMath` via `getEpochSchedule`. `None` on any failure (caller retries).
pub fn fetch_epoch_math(rpc_url: &str) -> Option<EpochMath> {
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"getEpochSchedule","params":[]}"#;
    let resp = crate::rpc_post(rpc_url, body)?;
    let v: serde_json::Value = serde_json::from_str(&resp).ok()?;
    let r = &v["result"];
    let slots_per_epoch    = r["slotsPerEpoch"].as_u64()?;
    let first_normal_epoch = r["firstNormalEpoch"].as_u64()?;
    let first_normal_slot  = r["firstNormalSlot"].as_u64()?;
    if slots_per_epoch == 0 {
        return None;
    }
    Some(EpochMath { slots_per_epoch, first_normal_epoch, first_normal_slot })
}

/// Fetch the current absolute slot via `getEpochInfo` (used only to derive the
/// current epoch number to preload at startup). `None` on failure.
pub fn fetch_absolute_slot(rpc_url: &str) -> Option<u64> {
    let body =
        r#"{"jsonrpc":"2.0","id":1,"method":"getEpochInfo","params":[{"commitment":"confirmed"}]}"#;
    let resp = crate::rpc_post(rpc_url, body)?;
    let v: serde_json::Value = serde_json::from_str(&resp).ok()?;
    v["result"]["absoluteSlot"].as_u64()
}

/// Fetch one epoch's leader map via `getLeaderSchedule(first_slot_of_epoch)`.
///
/// Returns `(first_slot, end_slot, leaders)` on success. Indices in the RPC reply
/// are relative to the epoch's first slot; `leaders[i]` is the leader of
/// `first_slot + i`. `None` if the RPC has no schedule for that epoch (e.g. a deep
/// historical epoch it no longer serves) — the caller stays fail-closed for it.
pub fn fetch_epoch_leaders(rpc_url: &str, epoch: u64, math: &EpochMath) -> Option<(u64, u64, Vec<Pubkey>)> {
    let first_slot = math.first_slot_of_epoch(epoch)?;
    let end_slot   = first_slot + math.slots_per_epoch;

    // getLeaderSchedule takes a slot IN the target epoch and returns that epoch's
    // schedule, keyed by validator identity → indices relative to its first slot.
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"getLeaderSchedule","params":[{first_slot}]}}"#
    );
    let resp = crate::rpc_post(rpc_url, &body)?;
    let v: serde_json::Value = serde_json::from_str(&resp).ok()?;
    let map = v["result"].as_object()?; // null result (unknown epoch) → None → fail-closed

    let mut leaders = vec![Pubkey::default(); math.slots_per_epoch as usize];
    let mut filled: u64 = 0;
    for (pubkey_str, indices) in map {
        let Ok(pk) = Pubkey::from_str(pubkey_str) else { continue };
        if let Some(arr) = indices.as_array() {
            for idx_v in arr {
                if let Some(idx) = idx_v.as_u64() {
                    if (idx as usize) < leaders.len() {
                        leaders[idx as usize] = pk;
                        filled += 1;
                    }
                }
            }
        }
    }
    if filled == 0 {
        return None; // empty/unexpected → treat as unavailable, stay fail-closed
    }
    Some((first_slot, end_slot, leaders))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn math() -> EpochMath {
        // Post-warmup mainnet-like: 432000 slots/epoch, first normal epoch 0.
        EpochMath { slots_per_epoch: 432_000, first_normal_epoch: 0, first_normal_slot: 0 }
    }

    #[test]
    fn epoch_math_maps_slots_and_first_slots() {
        let m = math();
        assert_eq!(m.epoch_of(0), Some(0));
        assert_eq!(m.epoch_of(431_999), Some(0));
        assert_eq!(m.epoch_of(432_000), Some(1));
        assert_eq!(m.epoch_of(864_001), Some(2));
        assert_eq!(m.first_slot_of_epoch(0), Some(0));
        assert_eq!(m.first_slot_of_epoch(1), Some(432_000));
        assert_eq!(m.first_slot_of_epoch(2), Some(864_000));
    }

    #[test]
    fn leader_for_slot_within_loaded_epoch() {
        let mut cache = LeaderScheduleCache::new();
        cache.install_math(math());
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        // epoch 1 spans [432000, 864000). Build a 4-slot toy at its head.
        let mut leaders = vec![Pubkey::default(); 432_000];
        leaders[0] = a; leaders[1] = a; leaders[2] = b; leaders[3] = b;
        cache.install_epoch(1, 432_000, 864_000, leaders);

        assert_eq!(cache.leader_for_slot(432_000), Some(a));
        assert_eq!(cache.leader_for_slot(432_002), Some(b));
        assert_eq!(cache.leader_for_slot(432_003), Some(b));
        assert!(cache.is_ready());
    }

    #[test]
    fn miss_records_pending_epoch_and_returns_none() {
        let mut cache = LeaderScheduleCache::new();
        cache.install_math(math());
        // Slot in epoch 2, which is not loaded → None + pending{2}.
        assert_eq!(cache.leader_for_slot(864_005), None);
        assert_eq!(cache.take_pending(), vec![2]);
        // Draining clears it.
        assert!(cache.take_pending().is_empty());
    }

    #[test]
    fn no_math_is_fail_closed() {
        let mut cache = LeaderScheduleCache::new();
        // Before bootstrap: every lookup is unknown → None (money path drops).
        assert_eq!(cache.leader_for_slot(432_000), None);
        assert!(!cache.is_ready());
        // No math ⇒ we cannot even name the epoch, so nothing is queued.
        assert!(cache.take_pending().is_empty());
    }

    #[test]
    fn install_epoch_bounds_cache_and_evicts_oldest() {
        let mut cache = LeaderScheduleCache::new();
        cache.install_math(math());
        for e in 0..(MAX_CACHED_EPOCHS as u64 + 2) {
            let mut leaders = vec![Pubkey::default(); 432_000];
            leaders[0] = Pubkey::new_unique();
            let first = e * 432_000;
            cache.install_epoch(e, first, first + 432_000, leaders);
        }
        assert_eq!(cache.schedules.len(), MAX_CACHED_EPOCHS);
        // Oldest epochs (0, 1) evicted; newest retained.
        assert!(!cache.has_epoch(0));
        assert!(!cache.has_epoch(1));
        assert!(cache.has_epoch(MAX_CACHED_EPOCHS as u64 + 1));
    }

    #[test]
    fn garbage_shred_fails_verification() {
        let leader = Pubkey::new_unique();
        assert!(!verify_shred_signature(&[0u8; 32], &leader), "too short → false");
        assert!(!verify_shred_signature(&[0u8; 1203], &leader), "not a valid merkle shred → false");
    }

    // NOTE: a positive round-trip test (create a real leader-signed Merkle shred
    // via `Shredder::entries_to_merkle_shreds_for_tests`, then assert it verifies)
    // would need solana-ledger's `dev-context-only-utils` feature. That feature
    // cannot be enabled here without a matching `dev-context-only-utils`
    // solana-runtime, which fails Cargo feature-unification because colibri already
    // pulls solana-runtime (via solana-gossip) WITHOUT it. The positive path is
    // instead a line-for-line mirror of agave `sigverify_shreds::verify_shred_cpu`
    // (sig = shred[0..64]; msg = shred::layout::get_merkle_root; ed25519 verify).
    // TODO(review): add an integration test in a separate crate, or a live check
    // against real mainnet shreds, to exercise the positive path end-to-end.
}
