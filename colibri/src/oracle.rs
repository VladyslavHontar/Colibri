//! RPC oracle cross-check (throwaway measurement spike — Phase 0).
//!
//! Samples completed targeted slots and fetches `getBlock` from an RPC node to
//! compare the reconstructed transaction-signature set against ground truth.
//! This catches reconstruction bugs that self-certifying `is_full()` cannot
//! detect (e.g. wrong `last_index` or missing FEC recovery).
//!
//! **RPC is fully fenced here.** No network call exists outside this module
//! (except the pre-existing `rpc_post`/`fetch_tier1` in `main.rs`).  The entire
//! oracle path is inert unless `--oracle-rpc <URL>` is passed.
//!
//! # Wire format
//!
//! We request `transactionDetails:"signatures"`, which yields a compact flat
//! array at `result.signatures: ["sig1", "sig2", …]` — much smaller than full
//! transaction data.

use {
    serde_json::Value,
    std::collections::HashSet,
};

// ── public types ─────────────────────────────────────────────────────────────

/// Result of comparing a locally-reconstructed signature set against RPC ground
/// truth for a single slot.
pub struct SigDiff {
    /// Signatures present in RPC response but absent from local reconstruction.
    /// Non-zero means we missed transactions — a reconstruction bug.
    pub missing_locally: usize,
    /// Signatures present locally but absent from RPC response.
    /// Non-zero means we fabricated transactions — also a bug.
    pub extra_locally: usize,
    /// Signatures present in both sets.
    pub matched: usize,
}

// ── core diff logic ───────────────────────────────────────────────────────────

/// Compare `reconstructed` against the RPC `getBlock` JSON (with
/// `transactionDetails:"signatures"`).
///
/// The RPC response shape is:
/// ```json
/// { "result": { "signatures": ["AAA", "BBB", …] } }
/// ```
///
/// Returns `Some(SigDiff)` with set-difference counts, or `None` when the
/// block is unavailable (`{"result":null}`) or the response cannot be parsed.
/// `None` signals "no block data" — the caller must NOT treat this as
/// divergence; it counts it under `fetch_errors` / `skipped` instead.
pub fn diff_signatures(reconstructed: &HashSet<String>, rpc_block_json: &str) -> Option<SigDiff> {
    let rpc_set = parse_rpc_signatures(rpc_block_json)?;

    let matched         = reconstructed.intersection(&rpc_set).count();
    let missing_locally = rpc_set.difference(reconstructed).count();
    let extra_locally   = reconstructed.difference(&rpc_set).count();

    Some(SigDiff { matched, missing_locally, extra_locally })
}

/// Parse `result.signatures` (flat array) from a `getBlock` JSON response.
///
/// Returns `None` when:
/// - JSON parse fails (malformed body)
/// - `result` is JSON `null` (slot skipped / not yet confirmed by the RPC node)
/// - `result.signatures` is absent or not an array (unexpected shape)
///
/// Callers must treat `None` as "no block data available", not as divergence.
fn parse_rpc_signatures(json: &str) -> Option<HashSet<String>> {
    let v: Value = match serde_json::from_str(json) {
        Ok(v)  => v,
        Err(e) => {
            eprintln!("[oracle] JSON parse error: {e}");
            return None;
        }
    };

    // `getBlock` can legitimately return {"result":null} for skipped/missing
    // slots.  Treat this as "no block" rather than empty-RPC-set so the caller
    // never falsely counts local sigs as extra_locally divergence.
    if v["result"].is_null() {
        eprintln!("[oracle] result=null — slot skipped or not yet available on RPC");
        return None;
    }

    let arr = match v["result"]["signatures"].as_array() {
        Some(a) => a,
        None    => {
            eprintln!("[oracle] unexpected RPC shape: result.signatures not an array");
            return None;
        }
    };

    Some(
        arr.iter()
            .filter_map(|s| s.as_str().map(|s| s.to_string()))
            .collect()
    )
}

// ── network fetch ─────────────────────────────────────────────────────────────

/// Fetch `getBlock` for `slot` from `rpc_url`.
///
/// Uses the crate-internal `rpc_post` from `main.rs`; returns the raw JSON
/// response body, or `None` on any network/timeout error.
///
/// Request uses `transactionDetails:"signatures"` for compact payloads and
/// disables `rewards` to further reduce response size.
pub fn fetch_block(rpc_url: &str, slot: u64) -> Option<String> {
    let body = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"getBlock","params":[{slot},{{"encoding":"json","transactionDetails":"signatures","rewards":false,"maxSupportedTransactionVersion":0}}]}}"#,
        slot = slot,
    );
    crate::rpc_post(rpc_url, &body)
}

// ── oracle sampler state ──────────────────────────────────────────────────────

/// Shared state for the oracle sampler.  Wrap in `Arc<Mutex<OracleSampler>>`.
pub struct OracleSampler {
    /// RPC endpoint URL.
    pub rpc_url: String,
    /// Sample every Nth completed targeted slot.
    pub sample_every: u64,
    /// How many targeted completions we have seen (used to determine sampling).
    pub completion_count: u64,
    // Accumulated totals across all sampled slots.
    /// Slots for which `fetch_block` returned `Some` AND `diff_signatures`
    /// returned `Some` — i.e. slots we actually diffed.
    pub sampled:       u64,
    pub matched:       u64,
    pub missing_local: u64,
    pub extra_local:   u64,
    /// Slots selected for sampling where we got no usable block data:
    /// network timeout, RPC error, `result:null`, or unexpected JSON shape.
    pub fetch_errors:  u64,
}

impl OracleSampler {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url,
            sample_every: 50, // sample 1 in 50 completed targeted slots
            completion_count: 0,
            sampled: 0,
            matched: 0,
            missing_local: 0,
            extra_local: 0,
            fetch_errors: 0,
        }
    }

    /// Call each time a targeted slot is genuinely completed (`is_full()`).
    ///
    /// `sigs` = reconstructed transaction signatures for that slot.  If this
    /// slot is selected for sampling, fetches from RPC, diffs, and accumulates
    /// counters.  Logs loudly on any `missing_local > 0`.
    ///
    /// `sampled` is only incremented when `fetch_block` succeeds AND
    /// `diff_signatures` returns `Some` (i.e. we actually compared signatures).
    /// Any other outcome — network failure, `result:null`, bad JSON — increments
    /// `fetch_errors` instead so the report stays honest.
    pub fn on_complete(&mut self, slot: u64, sigs: &HashSet<String>) {
        self.completion_count += 1;
        if self.completion_count % self.sample_every != 0 {
            return;
        }

        let json = match fetch_block(&self.rpc_url, slot) {
            Some(j) => j,
            None    => {
                eprintln!("[oracle] slot={slot} fetch_block failed (RPC timeout or error)");
                self.fetch_errors += 1;
                return;
            }
        };

        let d = match diff_signatures(sigs, &json) {
            Some(d) => d,
            None    => {
                // result:null or unrecognised shape — not divergence, just skip
                eprintln!("[oracle] slot={slot} skipped (null/unparseable block)");
                self.fetch_errors += 1;
                return;
            }
        };

        // Only reach here when we have a real diff — count it as sampled.
        self.sampled       += 1;
        self.matched       += d.matched as u64;
        self.missing_local += d.missing_locally as u64;
        self.extra_local   += d.extra_locally as u64;

        if d.missing_locally > 0 {
            eprintln!(
                "[oracle] RECONSTRUCTION BUG slot={slot} \
                 missing_locally={} extra_locally={} matched={}",
                d.missing_locally, d.extra_locally, d.matched,
            );
        } else {
            eprintln!(
                "[oracle] slot={slot} ok matched={} extra_locally={}",
                d.matched, d.extra_locally,
            );
        }
    }

    /// One-line summary for the final Ctrl-C report.
    pub fn report_line(&self) -> String {
        format!(
            "oracle: sampled={} matched={} missing_local={} extra_local={} fetch_errors={}",
            self.sampled, self.matched, self.missing_local, self.extra_local, self.fetch_errors,
        )
    }
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn diff_detects_locally_missing_signatures() {
        // getBlock with transactionDetails:"signatures" returns a flat array
        // at result.signatures — NOT result.transactions[].transaction.signatures.
        let rpc = r#"{"result":{"signatures":["AAA","BBB"]}}"#;
        let local: HashSet<String> = ["AAA"].iter().map(|s| s.to_string()).collect();
        let d = diff_signatures(&local, rpc).unwrap();
        assert_eq!(d.matched, 1);
        assert_eq!(d.missing_locally, 1); // BBB absent locally
        assert_eq!(d.extra_locally, 0);
    }

    #[test]
    fn diff_detects_extra_locally() {
        let rpc = r#"{"result":{"signatures":["AAA"]}}"#;
        let local: HashSet<String> = ["AAA", "CCC"].iter().map(|s| s.to_string()).collect();
        let d = diff_signatures(&local, rpc).unwrap();
        assert_eq!(d.matched, 1);
        assert_eq!(d.missing_locally, 0);
        assert_eq!(d.extra_locally, 1); // CCC extra locally
    }

    #[test]
    fn diff_perfect_match() {
        let rpc = r#"{"result":{"signatures":["AAA","BBB","CCC"]}}"#;
        let local: HashSet<String> = ["AAA", "BBB", "CCC"].iter().map(|s| s.to_string()).collect();
        let d = diff_signatures(&local, rpc).unwrap();
        assert_eq!(d.matched, 3);
        assert_eq!(d.missing_locally, 0);
        assert_eq!(d.extra_locally, 0);
    }

    #[test]
    fn diff_malformed_json_returns_none() {
        // On parse failure diff_signatures returns None — not a SigDiff with
        // extra_locally inflated by every local sig being "extra".
        let local: HashSet<String> = ["AAA"].iter().map(|s| s.to_string()).collect();
        assert!(diff_signatures(&local, "not-json").is_none());
    }

    #[test]
    fn diff_empty_slot_both_empty() {
        let rpc = r#"{"result":{"signatures":[]}}"#;
        let local: HashSet<String> = HashSet::new();
        let d = diff_signatures(&local, rpc).unwrap();
        assert_eq!(d.matched, 0);
        assert_eq!(d.missing_locally, 0);
        assert_eq!(d.extra_locally, 0);
    }

    /// Fix 3: `{"result":null}` must return None, never count local sigs as
    /// extra_locally divergence.  This is the primary false-positive guard.
    #[test]
    fn diff_null_result_returns_none_not_extra_locally() {
        let rpc = r#"{"result":null}"#;
        // Even if we have local sigs, a null result is NOT divergence — it means
        // the slot is skipped/missing on the RPC side.
        let local: HashSet<String> = ["AAA", "BBB"].iter().map(|s| s.to_string()).collect();
        assert!(
            diff_signatures(&local, rpc).is_none(),
            "null result must signal no-block, not extra_locally=2"
        );
    }
}
