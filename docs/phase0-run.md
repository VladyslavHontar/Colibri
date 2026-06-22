# Phase 0 — Run Procedure and Go/No-Go Report Template

Phase 0 is a throwaway measurement spike that answers one question: can
Colibri reconstruct a Solana mainnet slot reliably using pure P2P repair —
no RPC, no Geyser — with sufficient window depth to serve as the shred
source for Lumen's `P2pRepairSource`?

---

## 1. Box Prerequisites

| Requirement | Detail |
|---|---|
| Public IP | The machine must be reachable from the open internet on the ports below. A NAT/firewall that hides inbound UDP kills gossip. |
| Gossip port (UDP 8000) | Inbound + outbound. Solana peers send pull-responses here. |
| TVU port (UDP 8200) | Inbound only. Turbine shreds arrive here from stake-weighted validators. |
| Repair port (UDP 8210) | Inbound + outbound. Repair requests go out; shred responses come back here. |
| UDP recv buffer | `sudo sysctl -w net.core.rmem_max=134217728` — without this the kernel silently drops shred bursts. Set permanently in `/etc/sysctl.conf`. Colibri sets `SO_RCVBUF=32MB` on the TVU socket but the kernel caps it at `rmem_max`. |
| Rust toolchain | Stable Rust (edition 2021). `rustup update stable`. |
| RPC endpoint | Any Solana mainnet RPC that supports `getBlock` with `transactionDetails:"signatures"`. Passed only to `--oracle-rpc`; no other RPC traffic is generated. |

---

## 2. Build

```bash
cargo build --release -p colibri
```

The binary is placed at `./target/release/colibri`.

---

## 3. Keypair

Generate a stable gossip identity (one-time):

```bash
solana-keygen new --outfile colibri.json
```

Reusing the same keypair across restarts avoids gossip churn; validators
give preference to peers they already know.

---

## 4. Invocation

```bash
./target/release/colibri \
  --ip <YOUR_PUBLIC_IP> \
  --entrypoint entrypoint.mainnet-beta.solana.com:8001 \
  --keypair colibri.json \
  --probe-depth 6000 \
  --probe-window-max 50000 \
  --oracle-rpc <RPC_URL>
```

**Flag reference (all flags that affect Phase 0 measurements):**

| Flag | Default | Purpose |
|---|---|---|
| `--ip <IP>` | (required) | Public IP advertised in gossip |
| `--entrypoint <ADDR>` | (none) | Solana entrypoint; repeatable for redundancy |
| `--keypair <PATH>` | ephemeral | Keypair JSON for stable gossip identity |
| `--tvu-port <PORT>` | 8200 | UDP port where turbine shreds arrive |
| `--repair-port <PORT>` | 8210 | UDP port for repair request/response |
| `--port <PORT>` | 8000 | Gossip UDP port |
| `--shred-version <VER>` | 50093 | Mainnet shred version (auto-probed from entrypoint) |
| `--probe-depth <N>` | 6000 | Slots back from tip for the coverage range |
| `--probe-window-max <N>` | 50000 | Maximum depth for the repair-window probe (probed in 1000-slot steps) |
| `--oracle-rpc <URL>` | (none) | **Measurement-only.** Enables RPC cross-check: samples 1-in-50 completed slots via `getBlock` to catch reconstruction bugs that self-certifying `is_full()` cannot detect. This is the sole RPC use in Phase 0; all other data flow is pure P2P. |
| `--rpc <URL>` | mainnet default | Stake data for Tier-1 peer scoring (separate from `--oracle-rpc`) |
| `--tier1-fanout <N>` | 200 | How many top-staked validators to target for repairs |

**Recommended run duration:** at least 10–15 minutes to let gossip converge
and to sample enough slots for meaningful percentiles. The coverage range
(`--probe-depth 6000` slots) and all probe slots are enqueued once the TVU
observes the first turbine shred; each slot has a 30-second repair deadline.

---

## 5. Reading the Live Log

Progress is printed to stderr. Key lines to watch:

- `[tvu] shreds=… published=… tip=…` — turbine is flowing
- `[gossip] peers: … (Tier-1 visible: N/200)` — gossip health; need T1 > 0
- `[repair] total_sent=… responses=…` — repair traffic is reaching validators
- `[repair] target slot=… COMPLETE` — individual slot success
- `[probe] depth=… slot=… COMPLETE` / `TIMED_OUT` — window probe results
- `[oracle] slot=… ok matched=…` or `RECONSTRUCTION BUG` — oracle verdict per sample

---

## 6. Stop and Read the Report

Press **Ctrl-C**. Colibri prints the final report block before exiting:

```
════════════════════ COLIBRI COVERAGE REPORT ════════════════════
  status:         GREEN | YELLOW | RED
  targeted:       <N>
  completed:      <N>
  completeness:   <pct>%
  latency p50:    <ms> ms
  latency p99:    <ms> ms
  latency max:    <ms> ms
  repair window:  completed to depth <D> slots[, failed at <F>]
  pings_seen:     <N>
  pongs_sent:     <N>
  responses:      <N>
  uptime:         <T>s
  oracle: sampled=<N> matched=<M> missing_local=<X> extra_local=<Y> fetch_errors=<E>
═════════════════════════════════════════════════════════════════
```

---

## 7. Results Template

Fill in the numbers from the Ctrl-C report and commit this block alongside
your raw log:

```
PHASE 0 RESULT — <YYYY-MM-DD>, run duration <T> min
box:                 <IP>, <CPU>, <RAM>
uptime:              <T>s

completeness:        <pct>%    (target ≥99.9%)
targeted:            <N> slots
completed:           <N> slots

latency p50/p99/max: <p50> / <p99> / <max> ms

repair window:       <repair window line verbatim from report>
                     (target: depth > probe_depth = 6000 slots)

ping gate:           pings_seen=<N>  pongs_sent=<N>  responses=<N>

oracle:              sampled=<N>  matched=<M>  missing_local=<X>  extra_local=<Y>  fetch_errors=<E>
                     (missing_local must be 0)

VERDICT: GREEN | YELLOW | RED
```

---

## 8. Decision Rule (Explicit)

All three conditions must hold for **GREEN**:

1. `missing_local == 0` — no reconstruction bugs detected by the oracle.
   `missing_local > 0` forces at least **YELLOW** regardless of the
   `is_full()` completeness rate, because `is_full()` self-certifies only
   that we received every shred index 0..last_index — it cannot detect a
   wrong `last_index` or a missed FEC recovery. The oracle catches those
   cases. Even a single non-zero `missing_local` across all sampled slots
   indicates a reconstruction bug that must be fixed before going to
   production.

2. `completeness ≥ 99.9%` — at least 999 of every 1000 targeted slots
   reached genuine `is_full()` completion within 30 seconds of being
   enqueued.

3. `measured window depth > probe_depth (6000 slots)` — the repair window
   probe confirmed that the network can serve shreds at least as far back
   as the snapshot age Lumen needs to cover. "measured window depth" is the
   `deepest_ok` value from the `[probe] window:` line, i.e. the largest
   depth (in slots) at which a probe slot was genuinely completed.

**Verdict mapping:**

| Verdict | Conditions | Next step |
|---|---|---|
| **GREEN** | `missing_local == 0` AND `completeness ≥ 99.9%` AND `window_depth > 6000` | Build production `P2pRepairSource` in Lumen (Phase 1 plan). |
| **YELLOW** | High completeness (≥ 99.9%) but `window_depth ≤ probe_depth`, OR `missing_local > 0` with otherwise clean numbers | For window shortfall: add a snapshot-freshness guarantee (ensure Lumen only requests slots within the proven window) and revise Phase 1 accordingly. For reconstruction bugs: fix the bug, re-run. |
| **RED** | `completeness < ~100%` within the proven window — repair alone cannot reliably reconstruct slots | P2P repair alone is insufficient as a data source. Rethink the architecture (e.g. hybrid approach with a fallback, or pre-fetch via a different mechanism). |

> **Note on `missing_local > 0`:** the oracle samples 1 in 50 completed
> targeted slots (configurable via `OracleSampler::sample_every`). A single
> `missing_local > 0` event in a small sample is more alarming than the raw
> count suggests. Investigate the specific slot before downgrading the verdict
> — check whether it was a skipped slot (`fetch_errors` handles those) or a
> genuine reconstruction failure.
