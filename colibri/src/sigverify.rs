//! Shred signature verification, keyed by the slot's leader.
//!
//! # Why this exists
//!
//! Without leader-keyed sigverify, ANY host that can reach our TVU/repair sockets
//! can inject forged shreds; the deshredder will happily assemble them into fake
//! entries and stream them to gRPC subscribers → corrupted downstream state.
//! Agave verifies every shred against the slot leader's pubkey before ingest
//! (`ledger/src/sigverify_shreds.rs::verify_shred_cpu`). This module reproduces
//! that check.
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
//! We derive the slot→leader map from the RPC pair `getEpochInfo` + `getLeaderSchedule`
//! for the CURRENT epoch. This fully covers the steady-state shredstream / tip path
//! (the money path Lumen replays), which is always inside the current epoch.
//!
//! ## Stubbed / known limitation (flagged for review)
//!
//! `getLeaderSchedule` with no slot argument returns only the current epoch. Deep
//! repair-coverage probes (thousands of slots back, Phase-0 harness) can fall into a
//! PRIOR epoch for which we have no schedule loaded, so `leader_for_slot` returns
//! `None`. The ingest policy (see `main.rs`) is therefore:
//!   * leader known  → verify; DROP on signature failure (fail-closed, == agave).
//!   * leader unknown → ADMIT but count as unverified (`shreds_unverified_no_leader`).
//! The admit-on-unknown branch is the honest stub boundary: it keeps the Phase-0
//! coverage harness working while enforcing real sigverify on the current-epoch tip
//! path. TODO(prod): fetch per-epoch schedules on demand (`getLeaderSchedule(slot)`)
//! and switch the unknown-leader branch to fail-closed for the production feed.

use {
    solana_ledger::shred::layout,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{str::FromStr, time::Instant},
};

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

/// Slot→leader schedule for the current epoch, plus refresh bookkeeping.
///
/// Cheap to query (`leader_for_slot`); refreshed out-of-band via `refresh_if_stale`.
pub struct LeaderScheduleCache {
    rpc_url: String,
    /// Leaders indexed by `slot - epoch_first_slot`. Empty until first successful fetch.
    leaders: Vec<Pubkey>,
    epoch_first_slot: u64,
    /// One-past-the-last absolute slot of the loaded epoch.
    epoch_end_slot: u64,
    last_fetch: Option<Instant>,
}

impl LeaderScheduleCache {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url,
            leaders: Vec::new(),
            epoch_first_slot: 0,
            epoch_end_slot: 0,
            last_fetch: None,
        }
    }

    /// Leader for `slot`, or `None` if the slot is outside the loaded epoch (or no
    /// schedule has been fetched yet). `None` is the "unknown leader" signal the
    /// ingest policy treats as unverified-but-admitted (see module docs).
    pub fn leader_for_slot(&self, slot: u64) -> Option<Pubkey> {
        if self.leaders.is_empty() || slot < self.epoch_first_slot || slot >= self.epoch_end_slot {
            return None;
        }
        let idx = (slot - self.epoch_first_slot) as usize;
        self.leaders.get(idx).copied()
    }

    /// True once a schedule for some epoch has been loaded.
    #[allow(dead_code)] // public API; currently exercised only in tests
    pub fn is_loaded(&self) -> bool {
        !self.leaders.is_empty()
    }

    pub fn loaded_range(&self) -> (u64, u64) {
        (self.epoch_first_slot, self.epoch_end_slot)
    }

    /// Fetch (or re-fetch) the current epoch's schedule if we have never fetched,
    /// or `observed_slot` has moved into a new epoch. Returns `true` on a successful
    /// (re)load. Network/parse failures leave the previous schedule intact.
    pub fn refresh_if_stale(&mut self, observed_slot: u64) -> bool {
        let need = self.leaders.is_empty()
            || (observed_slot != 0 && observed_slot >= self.epoch_end_slot);
        if !need {
            return false;
        }
        self.fetch_current_epoch()
    }

    /// Fetch the current epoch's leader schedule via getEpochInfo + getLeaderSchedule.
    fn fetch_current_epoch(&mut self) -> bool {
        self.last_fetch = Some(Instant::now());

        // 1. getEpochInfo → absoluteSlot, slotIndex, slotsInEpoch.
        let ei_body =
            r#"{"jsonrpc":"2.0","id":1,"method":"getEpochInfo","params":[{"commitment":"confirmed"}]}"#;
        let ei_resp = match crate::rpc_post(&self.rpc_url, ei_body) {
            Some(r) => r,
            None => {
                eprintln!("[sigverify] getEpochInfo HTTP failed");
                return false;
            }
        };
        let ei: serde_json::Value = match serde_json::from_str(&ei_resp) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[sigverify] getEpochInfo parse error: {e}");
                return false;
            }
        };
        let (Some(absolute_slot), Some(slot_index), Some(slots_in_epoch)) = (
            ei["result"]["absoluteSlot"].as_u64(),
            ei["result"]["slotIndex"].as_u64(),
            ei["result"]["slotsInEpoch"].as_u64(),
        ) else {
            eprintln!("[sigverify] getEpochInfo unexpected shape");
            return false;
        };
        let epoch_first_slot = absolute_slot.saturating_sub(slot_index);
        let epoch_end_slot = epoch_first_slot + slots_in_epoch;

        // 2. getLeaderSchedule (null slot → current epoch): { "<identity>": [idx, ...] }.
        //    Indices are relative to epoch_first_slot.
        let ls_body =
            r#"{"jsonrpc":"2.0","id":1,"method":"getLeaderSchedule","params":[null]}"#;
        let ls_resp = match crate::rpc_post(&self.rpc_url, ls_body) {
            Some(r) => r,
            None => {
                eprintln!("[sigverify] getLeaderSchedule HTTP failed");
                return false;
            }
        };
        let ls: serde_json::Value = match serde_json::from_str(&ls_resp) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("[sigverify] getLeaderSchedule parse error: {e}");
                return false;
            }
        };
        let map = match ls["result"].as_object() {
            Some(m) => m,
            None => {
                eprintln!("[sigverify] getLeaderSchedule null/unexpected shape");
                return false;
            }
        };

        let mut leaders = vec![Pubkey::default(); slots_in_epoch as usize];
        let mut filled: u64 = 0;
        for (pubkey_str, indices) in map {
            let pk = match Pubkey::from_str(pubkey_str) {
                Ok(p) => p,
                Err(_) => continue,
            };
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
            eprintln!("[sigverify] getLeaderSchedule produced 0 assignments — keeping previous schedule");
            return false;
        }

        self.leaders = leaders;
        self.epoch_first_slot = epoch_first_slot;
        self.epoch_end_slot = epoch_end_slot;
        eprintln!(
            "[sigverify] leader schedule loaded: epoch slots [{epoch_first_slot}, {epoch_end_slot}) \
             assignments={filled}"
        );
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leader_for_slot_bounds() {
        let mut cache = LeaderScheduleCache::new("http://unused".into());
        // Manually populate as if a schedule were fetched.
        let a = Pubkey::new_unique();
        let b = Pubkey::new_unique();
        cache.leaders = vec![a, a, b, b];
        cache.epoch_first_slot = 1000;
        cache.epoch_end_slot = 1004;

        assert_eq!(cache.leader_for_slot(999), None, "before epoch → None");
        assert_eq!(cache.leader_for_slot(1000), Some(a));
        assert_eq!(cache.leader_for_slot(1002), Some(b));
        assert_eq!(cache.leader_for_slot(1003), Some(b));
        assert_eq!(cache.leader_for_slot(1004), None, "at epoch_end (exclusive) → None");
        assert_eq!(cache.leader_for_slot(9999), None, "far future → None");
    }

    #[test]
    fn unloaded_cache_returns_none() {
        let cache = LeaderScheduleCache::new("http://unused".into());
        assert!(!cache.is_loaded());
        assert_eq!(cache.leader_for_slot(1000), None);
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
