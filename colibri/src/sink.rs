//! Colibri's `BlockSink`: the sigverify gate every shred passes, and the
//! fan-out of what the assembler emits to the gRPC streams.

use {
    crate::{coverage, oracle, server::{ProtoEntry, ProtoFooter, ProtoTransaction}, sigverify},
    deshredder::{Entry, Event, VersionedTransaction},
    shred_net::BlockSink,
    std::{
        collections::{HashMap, HashSet},
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        time::{Duration, Instant},
    },
    tokio::sync::broadcast,
};

/// Slots ahead of the tip whose leaders repair leaves alone: the leader now
/// and the next, a leader window being 4 slots.
const LEADERS_AHEAD: u64 = 8;
/// Verified `(root, signature)` pairs kept: a slot holds a few dozen FEC sets,
/// so this is the sets of several hundred slots, far more than are in flight.
const VERIFIED_ROOTS: usize = 16_384;
/// Slot of a raw shred (LE u64 at offset 65), gated on the shred-variant byte
/// at offset 64. Coding shreds (0x4_/0x6_/0x7_) must pass too: they are half
/// of turbine and what lets the deshredder rebuild lost data shreds. Until
/// 2026-10-19 this gate admitted data shreds only, so no FEC set was ever
/// recovered and every shred turbine lost had to be repaired (~7% of slots
/// completed in 170 s at repair_rate 500).
fn shred_slot(buf: &[u8]) -> Option<u64> {
    if buf.len() < 88 {
        return None;
    }
    let variant = buf[64];
    let is_shred =
        matches!(variant & 0xF0, 0x40 | 0x60 | 0x70 | 0x80 | 0x90 | 0xB0) || variant == 0xA5;
    if !is_shred {
        return None;
    }
    Some(u64::from_le_bytes(buf[65..73].try_into().ok()?))
}

/// How often the stream's counters are logged.
const LOG_EVERY: Duration = Duration::from_secs(10);

/// Stream counters, locked once per assembler event batch.
struct Stats {
    last_log:   Instant,
    /// Shreds admitted by sigverify (turbine + repair).
    admitted:   u64,
    published:  u64,
    /// Block footers seen — tells whether Alpenglow markers are live upstream.
    footers:    u64,
    /// Since the last log line, how long after its leader made each block's
    /// footer it was here, in ms: by the leader's clock against this host's.
    behind_ms:  Vec<i64>,
    /// Slots given up for holding two versions of their block.
    equivocated: u64,
    /// Since the last log line, how many ms ahead of its batch's end each
    /// transaction went out, as (ms, transactions): 0 for one that waited.
    ahead_ms:   Vec<(u64, u32)>,
}

/// Colibri's `BlockSink`: leader-keyed sigverify gate (fail-closed) and the
/// gRPC fan-out of everything the assembler emits. One assembler serves both
/// turbine and repair, so a batch streams the moment it is contiguous and the
/// slot's last batch carries `complete = true`; skipped slots get an empty
/// marker.
/// What the sigverify gate did with the shreds it saw.
#[derive(Default)]
struct Checked {
    verified:  AtomicU64,
    /// Shreds admitted on a root whose signature was checked before.
    cached:    AtomicU64,
    rejected:  AtomicU64,
    no_leader: AtomicU64,
}

impl std::fmt::Display for Checked {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let n = |c: &AtomicU64| c.load(Ordering::Relaxed);
        write!(f, "ok={} by_known_root={} rejected={} dropped_no_leader={}", n(&self.verified), n(&self.cached), n(&self.rejected), n(&self.no_leader))
    }
}

impl Stats {
    /// One line of what went through since the last, and the percentiles start anew.
    fn log(&mut self, tip: u64, checked: &Checked) {
        self.behind_ms.sort_unstable();
        let behind = |q: f64| self.behind_ms.get(((self.behind_ms.len().max(1) - 1) as f64 * q) as usize).copied();
        // Transactions ahead of their batch's end, weighted by count.
        self.ahead_ms.sort_unstable();
        let total: u64 = self.ahead_ms.iter().map(|(_, n)| u64::from(*n)).sum();
        let ahead = |q: f64| {
            let mut left = (total as f64 * q) as u64;
            self.ahead_ms.iter().find(|(_, n)| { let n = u64::from(*n); if left < n { true } else { left -= n; false } }).map(|(ms, _)| *ms)
        };
        let early: f64 = self.ahead_ms.iter().filter(|(ms, _)| *ms >= 1).map(|(_, n)| f64::from(*n)).sum();
        let early_share = if total > 0 { 100.0 * early / total as f64 } else { 0.0 };
        eprintln!(
            "[stream] admitted={} published={} footers={} tip={tip} equivocated={} \
             behind_leader_ms[p10={:?} p50={:?} p90={:?}] \
             txs_ahead_of_batch_end[{total} txs, {early_share:.0}% >=1ms, p50={:?} p90={:?} p99={:?} max={:?} ms] \
             sigverify[{checked}]",
            self.admitted, self.published, self.footers, self.equivocated,
            behind(0.1), behind(0.5), behind(0.9),
            ahead(0.5), ahead(0.9), ahead(0.99), self.ahead_ms.last().map(|(ms, _)| *ms),
        );
        self.last_log = Instant::now();
        self.behind_ms.clear();
        self.ahead_ms.clear();
    }
}

pub struct ColibriSink {
    stats:         Mutex<Stats>,
    leader_sched:  Arc<Mutex<sigverify::LeaderScheduleCache>>,
    checked:       Checked,
    /// `(Merkle root, signature)` pairs whose signature by the slot's leader
    /// checked out. Every shred of a FEC set proves its place under one root,
    /// signed once, so one ed25519 check admits the whole set, as agave's own
    /// cache does (`ledger/src/sigverify_shreds.rs`).
    verified_roots: Mutex<HashSet<([u8; 32], [u8; 64])>>,
    /// Per slot, when transactions of its open batch went out, and how many:
    /// at the batch's end, how far ahead of it each went.
    sent_at: Mutex<HashMap<u64, Vec<(Instant, u32)>>>,
    /// Per slot, the entries of its open batch read so far, for the entry
    /// stream to send whole at the batch's end.
    open_batch: Mutex<HashMap<u64, Vec<Entry>>>,
    entry_tx:      Arc<broadcast::Sender<ProtoEntry>>,
    tx_tx:         Arc<broadcast::Sender<ProtoTransaction>>,
    footer_tx:     Arc<broadcast::Sender<ProtoFooter>>,
    meter:         Arc<Mutex<coverage::CoverageMeter>>,
    oracle:        Arc<Mutex<Option<oracle::OracleSampler>>>,
    /// Signatures per in-flight slot, kept only while the RPC oracle is on
    /// (it cross-checks a completed slot's full signature set).
    slot_sigs:     Mutex<HashMap<u64, HashSet<String>>>,
    /// Highest verified slot seen (diagnostics).
    tip:           AtomicU64,
}

impl ColibriSink {
    pub fn new(
        leader_sched: Arc<Mutex<sigverify::LeaderScheduleCache>>,
        (entry_tx, tx_tx, footer_tx): (Arc<broadcast::Sender<ProtoEntry>>, Arc<broadcast::Sender<ProtoTransaction>>, Arc<broadcast::Sender<ProtoFooter>>),
        meter: Arc<Mutex<coverage::CoverageMeter>>,
        oracle: Arc<Mutex<Option<oracle::OracleSampler>>>,
    ) -> Self {
        Self {
            stats: Mutex::new(Stats { last_log: Instant::now(), admitted: 0, published: 0, footers: 0, behind_ms: Vec::new(), equivocated: 0, ahead_ms: Vec::new() }),
            leader_sched,
            checked: Checked::default(),
            verified_roots: Mutex::default(),
            sent_at: Mutex::default(),
            open_batch: Mutex::default(),
            entry_tx, tx_tx, footer_tx, meter, oracle,
            slot_sigs: Mutex::default(),
            tip: AtomicU64::new(0),
        }
    }

    /// What the sigverify gate did so far, for the final report.
    pub fn sigverify(&self) -> String { self.checked.to_string() }

    /// Serialize one batch and fan it out to both streams. `complete` marks
    /// the batch that finished the slot (see `Entry.complete` in the proto).
    /// wincode is byte-compatible with bincode, so subscribers keep decoding
    /// the payload exactly as before.
    /// One whole batch on the entry stream: its contract is one message per
    /// batch, so batch boundaries can be recorded off it. Transactions go out
    /// on their own stream as they are read (`emit_txs`).
    fn publish(&self, slot: u64, entries: Vec<Entry>, complete: bool) {
        let bytes = match wincode::serialize(&entries) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[fast] slot {slot}: wincode::serialize failed: {e}");
                return;
            }
        };
        let _ = self.entry_tx.send(ProtoEntry { slot, entries: bytes, complete });
    }

    fn emit_txs<'a>(&self, slot: u64, txs: impl Iterator<Item = &'a VersionedTransaction>, complete: bool) {
        // Per-tx base58 + bincode is the most expensive work here; skip it
        // entirely when nothing consumes it.
        if self.tx_tx.receiver_count() == 0 {
            return;
        }
        for tx in txs {
            let sig = tx.signatures.first().map(|s| s.to_string()).unwrap_or_default();
            if let Ok(raw) = bincode::serialize(tx) {
                let _ = self.tx_tx.send(ProtoTransaction {
                    slot, signature: sig, raw_tx: raw, complete,
                });
            }
        }
    }

    /// `n` transactions of `slot`'s open batch were read just now.
    fn read_txs(&self, slot: u64, n: usize) {
        if n > 0 {
            let mut sent = self.sent_at.lock().unwrap_or_else(|e| e.into_inner());
            // ponytail: leak guard, as for slot_sigs — a slot given up never ends its batch.
            if sent.len() > 4_096 { sent.clear(); }
            sent.entry(slot).or_default().push((Instant::now(), n as u32));
        }
    }
}

impl BlockSink for ColibriSink {
    fn leaders_near(&self, slot: u64) -> Vec<[u8; 32]> {
        let mut schedule = self.leader_sched.lock().unwrap_or_else(|e| e.into_inner());
        let mut leaders: Vec<[u8; 32]> = (slot..=slot + LEADERS_AHEAD).filter_map(|s| schedule.leader_for_slot(s)).map(|pk| pk.to_bytes()).collect();
        leaders.dedup();
        leaders
    }

    /// Sigverify gate, fail-closed either way, as agave's is:
    /// - leader known: its signature over the Merkle root is verified, and a
    ///   shred that fails is dropped;
    /// - leader unknown: the shred is dropped, and the lookup records the
    ///   epoch so the background thread fetches its schedule.
    ///
    /// Returning `false` keeps the shred out of the assembler (hence the gRPC
    /// stream) and keeps it from advancing the observed tip.
    fn on_raw_shred(&self, bytes: &[u8]) -> bool {
        let Some(slot) = shred_slot(bytes) else { return false };
        let (Some(root), Some(Ok(sig))) = (deshredder::merkle_root(bytes), bytes.get(..64).map(<[u8; 64]>::try_from)) else {
            self.checked.rejected.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        // The root binds the slot and the shred's place: a known pair is the
        // leader's set, whatever shred of it this is.
        let known = self.verified_roots.lock().unwrap_or_else(|e| e.into_inner()).contains(&(root, sig));
        if known {
            self.checked.cached.fetch_add(1, Ordering::Relaxed);
        } else {
            let leader = self
                .leader_sched
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .leader_for_slot(slot);
            match leader {
                Some(pk) => {
                    if !sigverify::verify_root(&sig, &root, &pk) {
                        self.checked.rejected.fetch_add(1, Ordering::Relaxed);
                        return false;
                    }
                    self.checked.verified.fetch_add(1, Ordering::Relaxed);
                    let mut roots = self.verified_roots.lock().unwrap_or_else(|e| e.into_inner());
                    // ponytail: dropped whole when full, about the sets of the
                    // last few hundred slots; an LRU if re-verifying them shows.
                    if roots.len() >= VERIFIED_ROOTS {
                        roots.clear();
                    }
                    roots.insert((root, sig));
                }
                None => {
                    self.checked.no_leader.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
            }
        }

        self.tip.fetch_max(slot, Ordering::Relaxed);
        self.stats.lock().unwrap_or_else(|e| e.into_inner()).admitted += 1;
        true
    }

    /// Wire contract: `complete = true` rides on the batch that finished the
    /// slot. Batches and completion are separate events, so hold each batch
    /// until we know whether completion follows it.
    fn on_events(&self, events: Vec<Event>) {
        let oracle_on = self.oracle.lock().map(|g| g.is_some()).unwrap_or(false);
        let mut pending: Option<(u64, Vec<Entry>)> = None;
        let mut ahead_ms: Vec<(u64, u32)> = Vec::new();
        let mut published = 0u64;
        let mut footers = 0u64;
        let mut behind_ms: Vec<i64> = Vec::new();
        let mut equivocated = 0u64;
        for ev in events {
            match ev {
                Event::Entries { slot, entries, announced } => {
                    self.read_txs(slot, entries.iter().map(|e| e.transactions.len()).sum::<usize>() - announced);
                    if oracle_on {
                        let mut m = self.slot_sigs.lock().unwrap_or_else(|e| e.into_inner());
                        // ponytail: leak guard — a slot that never completes stays here.
                        if m.len() > 4_096 { m.clear(); }
                        m.entry(slot).or_default().extend(
                            entries.iter().flat_map(|e| &e.transactions)
                                .filter_map(|tx| tx.signatures.first()).map(|s| s.to_string()));
                    }
                    // Transactions now, those announced ahead of their entry aside;
                    // the entries with their batch.
                    let txs = entries.iter().enumerate().flat_map(|(k, e)| e.transactions.iter().skip(if k == 0 { announced } else { 0 }));
                    self.emit_txs(slot, txs, false);
                    let mut open = self.open_batch.lock().unwrap_or_else(|e| e.into_inner());
                    // ponytail: leak guard, as for slot_sigs — a slot given up never ends its batch.
                    if open.len() > 4_096 { open.clear(); }
                    open.entry(slot).or_default().extend(entries);
                }
                Event::Transactions { slot, transactions } => {
                    self.read_txs(slot, transactions.len());
                    self.emit_txs(slot, transactions.iter(), false);
                }
                Event::BatchEnd { slot } => {
                    let batch = self.open_batch.lock().unwrap_or_else(|e| e.into_inner()).remove(&slot).unwrap_or_default();
                    if !batch.is_empty()
                        && let Some((s, e)) = pending.replace((slot, batch)) {
                            self.publish(s, e, false);
                            published += 1;
                        }
                    // How far ahead of this moment each of the batch's transactions was read.
                    let now = Instant::now();
                    if let Some(sent) = self.sent_at.lock().unwrap_or_else(|e| e.into_inner()).remove(&slot) {
                        ahead_ms.extend(sent.into_iter().map(|(t, n)| (now.duration_since(t).as_millis() as u64, n)));
                    }
                }
                Event::SlotComplete { slot } => {
                    self.meter
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .mark_complete(slot, Instant::now());
                    if oracle_on {
                        let sigs = self.slot_sigs.lock().unwrap_or_else(|e| e.into_inner())
                            .remove(&slot).unwrap_or_default();
                        if let Ok(mut guard) = self.oracle.lock()
                            && let Some(sampler) = guard.as_mut() {
                                sampler.on_complete(slot, &sigs);
                            }
                    }
                    let entries = match pending.take() {
                        Some((s, e)) if s == slot => e,
                        other => { if let Some((s, e)) = other { self.publish(s, e, false); published += 1; } Vec::new() }
                    };
                    self.publish(slot, entries, true);
                    published += 1;
                }
                Event::Footer { slot, bank_hash, producer_time_nanos } => {
                    footers += 1;
                    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos() as i64;
                    behind_ms.push((now - producer_time_nanos as i64) / 1_000_000);
                    let _ = self.footer_tx.send(ProtoFooter {
                        slot,
                        bank_hash: bank_hash.to_bytes().to_vec(),
                        producer_time_nanos,
                    });
                }
                Event::UndecodableBatch { slot, first_index, last_index } => {
                    eprintln!("[stream] slot {slot}: shreds {first_index}..={last_index} did not decode as a BlockComponent");
                }
                Event::Equivocated { slot } => {
                    // Which version is the block cannot be told from the shreds;
                    // repair finds no data for it and reports it skipped.
                    equivocated += 1;
                    self.open_batch.lock().unwrap_or_else(|e| e.into_inner()).remove(&slot);
                    eprintln!("[stream] slot {slot}: its leader sent two versions of the block; neither is streamed");
                }
            }
        }
        if let Some((s, e)) = pending.take() {
            self.publish(s, e, false);
            published += 1;
        }
        let mut st = self.stats.lock().unwrap_or_else(|e| e.into_inner());
        st.published += published;
        st.footers += footers;
        st.behind_ms.extend(behind_ms);
        st.ahead_ms.extend(ahead_ms);
        st.equivocated += equivocated;
        if st.last_log.elapsed() >= LOG_EVERY {
            st.log(self.tip.load(Ordering::Relaxed), &self.checked);
        }
    }

    /// The slot was never produced. Emit an EMPTY Entry message so an in-order
    /// replay consumer advances past it instead of stalling; count it as
    /// accounted-for in coverage.
    fn on_slot_skipped(&self, slot: u64) {
        self.meter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .mark_complete(slot, Instant::now());
        if let Ok(bytes) = wincode::serialize(&Vec::<Entry>::new()) {
            let _ = self.entry_tx.send(ProtoEntry { slot, entries: bytes, complete: true });
        }
    }
}

#[cfg(test)]
mod shred_slot_tests {
    use super::*;

    #[test]
    fn parses_slot_of_merkle_data_shred() {
        let mut buf = [0u8; 88];
        buf[64] = 0x80; // ShredVariant::MerkleData
        buf[65..73].copy_from_slice(&123u64.to_le_bytes());
        assert_eq!(shred_slot(&buf), Some(123));
    }

    #[test]
    fn parses_slot_of_merkle_coding_shred() {
        let mut buf = [0u8; 89];
        buf[64] = 0x66; // chained Merkle code, proof_size 6
        buf[65..73].copy_from_slice(&456u64.to_le_bytes());
        assert_eq!(shred_slot(&buf), Some(456));
    }

    #[test]
    fn rejects_non_shred_and_short_packets() {
        let mut buf = [0u8; 88];
        buf[64] = 0x00; // not a shred variant
        assert_eq!(shred_slot(&buf), None);
        assert_eq!(shred_slot(&[0u8; 40]), None);
    }
}
