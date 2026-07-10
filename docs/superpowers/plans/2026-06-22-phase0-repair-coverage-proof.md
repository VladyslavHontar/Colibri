# Phase 0: Prove Repair Coverage — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Measure, on live mainnet, whether a validator-grade P2P repair client can reconstruct *complete* historical blocks across a realistic snapshot-gap depth — producing the GREEN/YELLOW/RED number that gates the whole shred-state-engine architecture.

**Architecture:** Extend the existing Colibri binary (`/Users/newuser/RustroverProjects/Colibri`) — which already does gossip join, turbine receive, FEC recovery, and `WindowIndex` repair — with the three pieces it lacks: a **ping-pong responder** (the gate that makes peers serve an unstaked node), the **`HighestShred` + `Orphan`** request types, and a **proactive historical-range repair driver** plus a **completeness + measurement harness** that emits the coverage metrics and cross-checks a sample against RPC.

**Tech Stack:** Rust, agave v3.1.5 crates (`solana-gossip`, `solana-ledger`, `solana-streamer`, `solana-net-utils`), the `deshredder` library, hand-rolled repair wire formats (bincode/manual), raw UDP sockets.

**Spike nature (read this):** This is throwaway measurement code, not the production client. Its job is a number, not maintainability. The protocol tasks (1–3) depend on agave wire formats extracted in Task 0 and are validated by **live mainnet integration**, not isolated unit tests — there is no way to unit-test "peers now serve us" offline. The pure-logic tasks (4, 6) get real TDD. After Phase 0 returns GREEN, the production `P2pRepairSource` is built fresh in Lumen (separate plan); this Colibri code is discarded.

## Global Constraints

- **Spec:** `/Users/newuser/RustroverProjects/Lumen/docs/superpowers/specs/2026-06-22-shred-state-engine-design.md` (commit `e777552`).
- **Agave version:** all agave crates pinned to tag `v3.1.5` (matches Colibri's existing `Cargo.toml` and deshredder). Do not bump.
- **RPC boundary:** RPC is permitted **only** in the Task 6 oracle, behind a `--oracle-rpc <URL>` flag, clearly fenced. No other task may call RPC. The `fetch_tier1`/`getVoteAccounts` path already in Colibri is allowed (it predates this plan and feeds repair peer scoring).
- **Completeness definition (verbatim from spec):** a slot is complete when it reaches `is_full()` — every shred index `0..=last_index` is present (received from turbine, FEC-recovered, or repaired). A slot is emitted/counted complete only when `is_full()`.
- **Go/no-go thresholds (verbatim from spec):** GREEN = ≥99.9% of slots reach `is_full()` across the realistic gap depth AND measured repair window > snapshot age; YELLOW = high completeness but repair window < snapshot age; RED = cannot sustain ~100% completeness within the window.
- **Run target:** the box with a public IP and open UDP ports (gossip/tvu/repair), same as Colibri runs today. Tasks 1/3/5/7 are validated there, not on the dev laptop.

---

### Task 0: Extract agave repair wire formats (reference, unblocks 1–3)

No code ships in this task — it produces the byte-layout reference that Tasks 1–3 implement against. Colibri hand-rolls `WindowIndex` (`main.rs:281-306`) because agave's `RepairProtocol` is not exposed as a stable public encoding; the other request types and Ping/Pong must be derived the same way.

**Files:**
- Create: `docs/repair-wire-format.md`
- Read (reference, do not modify): `/Users/newuser/RustroverProjects/agave/core/src/repair/serve_repair.rs`

**Steps:**

- [ ] **Step 1:** In `agave/core/src/repair/serve_repair.rs`, read the `RepairProtocol` enum and `RepairRequestHeader`. Record, for each of `WindowIndex`, `HighestWindowIndex`, `Orphan`: the bincode discriminant order, the header layout (`signature: [u8;64]`, `sender: Pubkey`, `recipient: Pubkey`, `timestamp: u64`, `nonce: u32`), and the trailing fields (`slot`, `shred_index` / `slot` only for Orphan). Cross-check the discriminant for `WindowIndex` against Colibri's working `3u32` tag at `main.rs:290` — the enum order must reproduce that `3`. Document any mismatch.
- [ ] **Step 2:** Read `RepairResponse` (the `Ping` variant) and the `Pong` construction (search `Pong::new`, `ping_pong` module, `REPAIR_PING_TOKEN_SIZE`). Record: how an inbound Ping response is recognized on the repair socket (discriminant/length), the 32-byte token location, and exactly what bytes a `Pong` must contain and sign (`Pong::new(ping, keypair)` → `from`, `hash`, `signature`). Record the byte layout to send back.
- [ ] **Step 3:** Record the **signed payload** for each request (which bytes are hashed/signed) — confirm it matches Colibri's existing `sign_data` (76 bytes: sender‖recipient‖ts‖nonce at `main.rs:298-303`) or document the difference per request type.
- [ ] **Step 4:** Write `docs/repair-wire-format.md` with one section per message (`WindowIndex`, `HighestWindowIndex`, `Orphan`, `Ping` inbound, `Pong` outbound), each giving exact byte offsets, total length, and the signed-bytes layout. Mark every field as either "confirmed against agave source line N" or "inferred — verify live."
- [ ] **Step 5: Commit**

```bash
git add docs/repair-wire-format.md
git commit -m "docs(phase0): extract agave repair wire formats (window/highest/orphan/ping/pong)"
```

---

### Task 1: Ping-pong responder on the repair socket

The make-or-break gate. Today the repair thread's drain loop (`main.rs:695-705`) sends every inbound packet to the deshredder as a shred. Peer ping-challenges land there and are dropped, so the peer never serves us. This task splits inbound traffic: Ping → Pong reply; everything else → shred channel as before.

**Files:**
- Modify: `colibri/src/main.rs` (repair thread, the drain loop at `main.rs:695-705`)
- Create: `colibri/src/repair_wire.rs` (encoders/decoders; `mod repair_wire;` added to `main.rs`)

**Interfaces:**
- Consumes: `docs/repair-wire-format.md` (Task 0).
- Produces:
  - `repair_wire::parse_inbound(buf: &[u8]) -> Inbound` where `enum Inbound { Ping([u8;32]), ShredResponse, Other }` — `Ping` carries the 32-byte token.
  - `repair_wire::build_pong(keypair: &Keypair, token: [u8;32]) -> Vec<u8>`.

**Steps:**

- [ ] **Step 1: Write the failing test** (pure decode/encode — the only unit-testable slice)

```rust
// colibri/src/repair_wire.rs  (tests at bottom)
#[cfg(test)]
mod tests {
    use super::*;
    use solana_keypair::Keypair;

    #[test]
    fn pong_has_expected_length_and_token_echo() {
        // Layout asserted here MUST match docs/repair-wire-format.md (Task 0).
        let kp = Keypair::new();
        let token = [7u8; 32];
        let pong = build_pong(&kp, token);
        // Pong = discriminant + from(32) + hash(32) + signature(64); fill exact
        // total from Task 0's recorded length.
        assert_eq!(pong.len(), PONG_WIRE_LEN);
        // from-pubkey must be our identity
        assert_eq!(&pong[PONG_FROM_OFFSET..PONG_FROM_OFFSET + 32], kp.pubkey().as_ref());
    }

    #[test]
    fn parse_inbound_classifies_ping_vs_shred() {
        // A real captured Ping (or a synthesized one per Task 0 layout) → Inbound::Ping(token)
        let ping_bytes = synth_ping([9u8; 32]);
        match parse_inbound(&ping_bytes) {
            Inbound::Ping(t) => assert_eq!(t, [9u8; 32]),
            _ => panic!("expected Ping"),
        }
        // A data-shred-shaped buffer (variant byte at [64]) → ShredResponse
        let mut shred = vec![0u8; 200];
        shred[64] = 0x80; // data shred variant, per parse_shred_header at main.rs:249
        assert!(matches!(parse_inbound(&shred), Inbound::ShredResponse));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p colibri repair_wire`
Expected: FAIL — `build_pong` / `parse_inbound` / constants not defined.

- [ ] **Step 3: Implement `repair_wire.rs`** against Task 0's layouts. Define `PONG_WIRE_LEN`, `PONG_FROM_OFFSET`, `enum Inbound`, `parse_inbound`, `build_pong` (using `keypair.sign_message` over the bytes Task 0 says a Pong signs), and a `synth_ping` test helper. Classification rule from Task 0: discriminant/length distinguishes Ping; otherwise if `buf.len() >= 88` and the shred-variant byte at `[64]` is a known variant (reuse the match in `parse_shred_header`, `main.rs:249-252`) → `ShredResponse`; else `Other`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p colibri repair_wire`
Expected: PASS (2 tests).

- [ ] **Step 5: Wire into the repair drain loop.** Replace `main.rs:697-705` so each inbound packet is classified: `Inbound::Ping(token)` → `repair_sock.send_to(&repair_wire::build_pong(&keypair_repair, token), &src)` and bump a `pongs_sent` counter; `Inbound::ShredResponse` → existing `repair_shred_tx.send(...)`; `Inbound::Other` → drop. Add `pongs_sent` and `pings_seen` to the 10-second `[repair]` log line (`main.rs:784`). Capture `src` from `recv_from` (currently discarded at line 698).

- [ ] **Step 6: Live acceptance (on the box).** Build and run against mainnet for 5 minutes. **PASS = the `[repair] responses=` counter rises substantially after pong support vs a baseline run without it** (peers now serve us). Record `pings_seen`, `pongs_sent`, `responses` before/after in the commit message. This is the proof the gate works; there is no offline substitute.

- [ ] **Step 7: Commit**

```bash
git add colibri/src/repair_wire.rs colibri/src/main.rs
git commit -m "feat(phase0): ping-pong responder on repair socket — unlocks peer serving

Live: responses <before> -> <after> over 5min, pings_seen=<n> pongs_sent=<n>"
```

---

### Task 2: `HighestShred` and `Orphan` request encoders

Colibri only sends `WindowIndex`. Historical-range repair needs `HighestShred(slot, idx)` to discover a slot's last index when we hold zero shreds, and `Orphan(slot)` to pull a parent chain. Pure encoders, unit-testable against Task 0's layout exactly like the existing `repair_window_index`.

**Files:**
- Modify: `colibri/src/repair_wire.rs`

**Interfaces:**
- Produces:
  - `repair_wire::highest_window_index(keypair, recipient: &[u8;32], slot: u64, shred_index: u64, nonce: u32) -> Vec<u8>`
  - `repair_wire::orphan(keypair, recipient: &[u8;32], slot: u64, nonce: u32) -> Vec<u8>`
  - (move the existing `repair_window_index` from `main.rs:281` here, renamed `window_index`, for one home.)

**Steps:**

- [ ] **Step 1: Write the failing test**

```rust
#[test]
fn highest_window_index_layout_matches_spec() {
    let kp = Keypair::new();
    let rcpt = [1u8; 32];
    let req = highest_window_index(&kp, &rcpt, 100, 5, 0xAA);
    assert_eq!(req.len(), HIGHEST_WIRE_LEN);          // from Task 0
    // discriminant for HighestWindowIndex per Task 0 (e.g. 2u32):
    assert_eq!(&req[0..4], &HIGHEST_DISCRIMINANT.to_le_bytes());
    assert_eq!(&req[SENDER_OFF..SENDER_OFF + 32], kp.pubkey().as_ref());
    assert_eq!(&req[RECIPIENT_OFF..RECIPIENT_OFF + 32], &rcpt);
}

#[test]
fn orphan_layout_matches_spec() {
    let kp = Keypair::new();
    let rcpt = [2u8; 32];
    let req = orphan(&kp, &rcpt, 100, 0xBB);
    assert_eq!(req.len(), ORPHAN_WIRE_LEN);            // from Task 0
    assert_eq!(&req[0..4], &ORPHAN_DISCRIMINANT.to_le_bytes());
}

#[test]
fn window_index_still_matches_colibri_baseline() {
    // Guards the move from main.rs: byte-identical to the proven 160-byte format.
    let kp = Keypair::new();
    let req = window_index(&kp, &[3u8; 32], 100, 5, 0xCC);
    assert_eq!(req.len(), 160);
    assert_eq!(&req[0..4], &3u32.to_le_bytes());
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p colibri repair_wire`
Expected: FAIL — `highest_window_index` / `orphan` not defined.

- [ ] **Step 3: Implement** both encoders and move `window_index` from `main.rs` into `repair_wire.rs` (update the call site at `main.rs:755`). Use Task 0's discriminants, offsets, lengths, and signed-bytes layout. Define the named constants used in the tests.

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p colibri repair_wire`
Expected: PASS (5 tests total in the module).

- [ ] **Step 5: Commit**

```bash
git add colibri/src/repair_wire.rs colibri/src/main.rs
git commit -m "feat(phase0): HighestShred + Orphan repair encoders; consolidate WindowIndex"
```

---

### Task 3: Proactive historical-range repair driver

Colibri only repairs slots turbine is *actively* delivering (it needs `last_index` from a seen last-in-slot shred, `main.rs:742`). Phase 0's test is repairing slots fully in the past where we hold **zero** shreds. This task adds a driver that, given a target slot, bootstraps it: `HighestShred(slot, 0)` to learn the last index, then `Shred(slot, i)` for the gaps, falling back to `Orphan(slot)` when the slot is unknown to peers.

**Files:**
- Modify: `colibri/src/main.rs` (repair thread)

**Interfaces:**
- Consumes: `repair_wire::{window_index, highest_window_index, orphan}` (Task 2), the `targets` peer list (`main.rs:732`), `repair_map` (`SlotRepairState`).
- Produces: a `request_target_slot(slot, &targets, ...)` helper driving the state machine below, and a `target_slots: VecDeque<u64>` the measurement harness (Task 5) fills.

**Steps:**

- [ ] **Step 1:** Extend `SlotRepairState` (`main.rs:257`) with `highest_probed: bool` and reset `first_seen` semantics so a *targeted* (vs turbine-observed) slot is not evicted by the 2-second `is_done()` (`main.rs:276`) before repair completes — targeted slots live until `is_full()` or a separate, longer `target_deadline` (e.g. 30s) elapses. Add `is_full(&self) -> bool` = `self.last_index.map_or(false, |l| (0..=l).all(|i| self.have.contains(&i)))`.

- [ ] **Step 2:** Add the per-target state machine in the repair loop: for each slot in `target_slots` not yet in `repair_map`, insert a `SlotRepairState`; if `last_index.is_none() && !highest_probed`, send `highest_window_index(.., slot, 0, ..)` to all `targets`, set `highest_probed = true`; once `last_index` is known, send `window_index` for missing indices (reuse the existing batch loop at `main.rs:751-769`); if after K rounds `last_index` is still `None`, send `orphan(.., slot, ..)` to discover the chain.

- [ ] **Step 3:** Feed repaired shred indices back into `repair_map.have`. In the drain loop (Task 1), when an `Inbound::ShredResponse` arrives, parse its header (`parse_shred_header`, `main.rs:242`) and insert `(slot,index)` into the matching `SlotRepairState.have` and set `last_index` if `last_in_slot`. **This closes the completeness-undercount bug** — today `have` only learns turbine shreds (`main.rs:619-627`), so repaired/`FEC`'d shreds were invisible to `is_full()`.

- [ ] **Step 4: Live acceptance (on the box).** Hand-set one `target_slots` entry to a slot ~2000 behind the live tip (read tip from the `[tvu]` max-slot log added in Task 5, or temporarily hardcode). **PASS = that slot reaches `is_full()` from a cold start (zero turbine shreds for it), logged as `[repair] target slot=<s> COMPLETE indices=<n>`.** This proves historical-range repair works at all — the core Phase 0 hypothesis.

- [ ] **Step 5: Commit**

```bash
git add colibri/src/main.rs
git commit -m "feat(phase0): proactive historical-range repair driver (HighestShred->Shred->Orphan)

Live: cold-start slot <s> reached is_full() from 0 turbine shreds"
```

---

### Task 4: Completeness tracker + metrics aggregator (pure logic, full TDD)

The measurement core. A pure struct that records per-slot completion and produces the coverage report, independent of sockets — so it is fully unit-testable.

**Files:**
- Create: `colibri/src/coverage.rs` (`mod coverage;` in `main.rs`)

**Interfaces:**
- Produces:
  - `coverage::CoverageMeter` with `mark_targeted(slot, at: Instant)`, `mark_complete(slot, at: Instant)`, `mark_last_index(slot, last: u32)`, and `report() -> CoverageReport`.
  - `coverage::CoverageReport { targeted: u64, completed: u64, completeness_pct: f64, p50_ms: u64, p99_ms: u64, max_ms: u64 }`.

**Steps:**

- [ ] **Step 1: Write the failing test**

```rust
// colibri/src/coverage.rs
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p colibri coverage`
Expected: FAIL — `CoverageMeter` undefined.

- [ ] **Step 3: Implement** `CoverageMeter` (a `HashMap<u64, SlotRec>` with `targeted_at`, `completed_at: Option<Instant>`, `last_index`), `report()` computing `completeness_pct = 100 * completed / targeted`, and sorted-duration percentiles (`p50`/`p99`/`max`) over completed slots. `mark_complete` ignores a slot already completed (idempotent).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p colibri coverage`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add colibri/src/coverage.rs colibri/src/main.rs
git commit -m "feat(phase0): CoverageMeter — per-slot completion + latency report (TDD)"
```

---

### Task 5: Measurement harness — drive the range, probe the window, emit the report

Ties the pieces together: observe the live tip from turbine, enqueue a target range `[tip−N … tip]`, drive repair (Task 3), record completion (Task 4), probe how deep peers still serve, and print the report on Ctrl-C.

**Files:**
- Modify: `colibri/src/main.rs`

**Interfaces:**
- Consumes: `coverage::CoverageMeter`, `target_slots` (Task 3), `SlotRepairState::is_full` (Task 3).
- Produces: a `--probe-depth <N>` flag (default 6000 ≈ realistic snapshot age in slots) and a `--probe-window-max <N>` flag (default 50000) for the repair-window probe.

**Steps:**

- [ ] **Step 1:** Track the live tip: in the TVU loop (`main.rs:614`), maintain `observed_tip = max(observed_tip, info.slot)` from `parse_shred_header`, shared via an `Arc<AtomicU64>`. Log it in the `[tvu]` line. (RPC-free tip discovery.)
- [ ] **Step 2:** On startup, once `observed_tip` is non-zero, enqueue `target_slots = (observed_tip - probe_depth ..= observed_tip - 64)` (stay 64 slots back so targets are settled history). Call `meter.mark_targeted(slot, now)` for each.
- [ ] **Step 3:** In the repair loop, when a `SlotRepairState` transitions to `is_full()`, call `meter.mark_complete(slot, now)` once and log `[repair] target slot=<s> COMPLETE`.
- [ ] **Step 4: Repair-window probe.** A separate counter: enqueue single probe targets at increasing depth (`tip−1000, tip−2000, … tip−probe_window_max`); for each, record whether it ever reaches `is_full()` within a 30s deadline. The deepest depth that still completes = the measured repair window. Log `[probe] window: completed to depth <D> slots, failed at <D2>`.
- [ ] **Step 5:** On Ctrl-C (extend the handler at `main.rs:500`), print the final report: `meter.report()` (completeness %, p50/p99/max ms) plus the probe-window result and `pings_seen/pongs_sent/responses`. Format as the GREEN/YELLOW/RED template (Task 7).
- [ ] **Step 6: Live acceptance (on the box).** Run with `--probe-depth 6000` for long enough to drain the range. **Deliverable = a printed report with a real completeness percentage and repair-window depth.** No pass threshold here — producing the number *is* the deliverable; Task 7 interprets it.
- [ ] **Step 7: Commit**

```bash
git add colibri/src/main.rs
git commit -m "feat(phase0): measurement harness — tip observe, range drive, window probe, report"
```

---

### Task 6: RPC oracle cross-check (throwaway, RPC fenced here only)

Self-certifying `is_full()` can be fooled by a wrong `last_index`. This samples completed slots, fetches `getBlock` over RPC, and compares the reconstructed transaction-signature set against ground truth — catching reconstruction bugs. RPC lives only in this file, behind `--oracle-rpc`.

**Files:**
- Create: `colibri/src/oracle.rs` (`mod oracle;` in `main.rs`)

**Interfaces:**
- Produces: `oracle::diff_signatures(reconstructed: &HashSet<String>, rpc_block_json: &str) -> SigDiff` where `SigDiff { missing_locally: usize, extra_locally: usize, matched: usize }`.

**Steps:**

- [ ] **Step 1: Write the failing test** (pure JSON diff — no network)

```rust
// colibri/src/oracle.rs
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn diff_detects_locally_missing_signatures() {
        // Minimal getBlock-shaped JSON: result.transactions[].transaction.signatures[0]
        let rpc = r#"{"result":{"transactions":[
            {"transaction":{"signatures":["AAA"]}},
            {"transaction":{"signatures":["BBB"]}}
        ]}}"#;
        let local: HashSet<String> = ["AAA"].iter().map(|s| s.to_string()).collect();
        let d = diff_signatures(&local, rpc);
        assert_eq!(d.matched, 1);
        assert_eq!(d.missing_locally, 1); // BBB absent locally
        assert_eq!(d.extra_locally, 0);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p colibri oracle`
Expected: FAIL — `diff_signatures` undefined.

- [ ] **Step 3: Implement** `diff_signatures` (parse `result.transactions[].transaction.signatures[0]` with `serde_json`, build the RPC set, compute set differences) and a `fetch_block(rpc_url, slot) -> Option<String>` reusing Colibri's `rpc_post` (`main.rs:152`) with a `getBlock` body (`maxSupportedTransactionVersion:0`, `transactionDetails:"signatures"` to keep payloads small).

- [ ] **Step 4: Run test to verify it passes**

Run: `cargo test -p colibri oracle`
Expected: PASS (1 test).

- [ ] **Step 5:** Wire a sampler into the harness: when `--oracle-rpc <URL>` is set, for every Nth completed slot collect the reconstructed signatures (from the deshredder's emitted transactions), call `fetch_block`, run `diff_signatures`, and accumulate totals into the final report (`oracle: sampled=<n> matched=<m> missing_local=<x> extra_local=<y>`). A non-zero `missing_local` is a reconstruction bug — flag it loudly.

- [ ] **Step 6: Commit**

```bash
git add colibri/src/oracle.rs colibri/src/main.rs
git commit -m "feat(phase0): RPC oracle cross-check of reconstructed signatures (fenced, throwaway)"
```

---

### Task 7: Run procedure + go/no-go report template

The deliverable of Phase 0 is a decision. This task documents how to run it on the box and the template that converts the numbers into GREEN/YELLOW/RED.

**Files:**
- Create: `docs/phase0-run.md`

**Steps:**

- [ ] **Step 1:** Write the run procedure: exact build (`cargo build --release -p colibri`), the box prerequisites (public IP, open UDP gossip/tvu/repair ports, `sysctl net.core.rmem_max`), and the invocation: `./target/release/colibri --ip <IP> --entrypoint entrypoint.mainnet-beta.solana.com:8001 --keypair colibri.json --probe-depth 6000 --oracle-rpc <URL>`. Note that the `--oracle-rpc` is measurement-only.
- [ ] **Step 2:** Write the results template:

```
PHASE 0 RESULT — <date>, run duration <t>
completeness:        <pct>%   (target ≥99.9%)
repair window depth: <D> slots (target > snapshot age ≈ 6000)
latency p50/p99/max: <..>/<..>/<..> ms
ping gate:           pings_seen=<n> pongs_sent=<n> responses=<n>
oracle:              sampled=<n> matched=<m> missing_local=<x>  (x must be 0)
VERDICT: GREEN | YELLOW | RED
  GREEN  → build production P2pRepairSource in Lumen (Phase 1 plan)
  YELLOW → high completeness, window < snapshot age → add snapshot-freshness guarantee, revise Phase 1
  RED    → completeness < ~100% within window → P2P repair alone insufficient → rethink
```

- [ ] **Step 3:** Add the decision rule explicitly: `missing_local > 0` forces at least YELLOW regardless of `is_full()` rate (self-certification was wrong); a true GREEN requires `missing_local == 0` AND completeness ≥ 99.9% AND window > probe_depth.
- [ ] **Step 4: Commit**

```bash
git add docs/phase0-run.md
git commit -m "docs(phase0): run procedure + GREEN/YELLOW/RED report template"
```

---

## Self-Review

**Spec coverage:** Phase 0 section of the spec → Tasks 0–7 cover ping-pong (T1), all-three request types (T2), `is_full` completeness incl. repaired/FEC shreds (T3+T4), self-certifying + RPC cross-check signals (T4/T6), the metrics list — completeness %, time-to-complete, repair-depth probe, ping-gate rate (T5), and the GREEN/YELLOW/RED bar (T7). The "extend Colibri" vehicle decision is honored throughout.

**Placeholder scan:** No "TBD"/"handle edge cases". The wire-format constants in Tasks 1–2 are deliberately *named references to Task 0's extracted layout* rather than fabricated byte values — inventing exact offsets before reading agave source would be false precision, so Task 0 is a hard dependency and the tests assert against its recorded constants.

**Type consistency:** `is_full()` defined once (T3) and consumed by T4/T5; `CoverageMeter`/`CoverageReport` fields consistent across T4→T5→T7; `repair_wire::{window_index, highest_window_index, orphan, build_pong, parse_inbound}` defined T1–T2 and consumed T3.

**Known limitation (honest):** Tasks 1, 3, 5 have no offline unit test — their acceptance is live mainnet behavior, by the nature of a network protocol spike. Tasks 0, 2, 4, 6 carry the unit-testable load. This is expected for Phase 0 and does not apply to the production Phase 1 client.
