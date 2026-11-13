//! Colibri: Solana shred-to-transaction node. Joins gossip, receives shreds via
//! TVU (Turbine), repairs missing slots over the repair protocol, and streams
//! entries/transactions to subscribers via a Jito-compatible gRPC service.
//!
//! One assembler, two feeds: every leader-sigverified shred — turbine or
//! repair — goes into `shred-net`'s in-memory `deshredder` (Reed–Solomon FEC
//! recovery), and entry batches stream out the moment they are contiguous,
//! `complete = true` on the batch that finishes the slot. `shred-net` drives
//! repair until every targeted slot is complete and reports empty-entry skip
//! markers for slots that were never produced; the targeted range follows the
//! consumer's `from-slot` frontier (gRPC metadata), falling back to
//! `tip - depth`.
//!
//! Sigverify is fail-closed: `on_raw_shred` returning `false` means the shred
//! is not emitted, never reaches the assembler, and does not advance the
//! observed tip.

#![allow(deprecated)]

mod config;
mod coverage;
mod leader_schedule;
mod oracle;
mod server;
mod sigverify;

use {
    anyhow::Result,
    deshredder::{Entry, Event, VersionedTransaction},
    serde_json::Value,
    server::{ProtoEntry, ProtoFooter, ProtoTransaction},
    shred_net::{BlockSink, ShredNet, ShredNetConfig},
    solana_keypair::{read_keypair_file, write_keypair_file, Keypair},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        collections::{HashMap, HashSet},
        env,
        net::SocketAddr,
        str::FromStr,
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            Arc, Mutex,
        },
        thread::sleep,
        time::{Duration, Instant},
    },
    tokio::sync::broadcast,
};

const TIER1_REFRESH_SECS: u64 = 600;
/// Verified `(root, signature)` pairs kept: a slot holds a few dozen FEC sets,
/// so this is the sets of several hundred slots, far more than are in flight.
const VERIFIED_ROOTS: usize = 16_384;
/// Slots ahead of the tip whose leaders repair leaves alone: the leader now
/// and the next, a leader window being 4 slots.
const LEADERS_AHEAD: u64 = 8;
/// How often gossip is read for what each node runs.
const NODES_REFRESH: Duration = Duration::from_secs(5);

/// One JSON-RPC POST; `None` on any transport or HTTP error. Plain and TLS
/// URLs both work — the previous hand-rolled HTTP/1.0 over a raw TcpStream
/// silently failed on `https://`, which read as "RPC down" in the logs.
pub(crate) fn rpc_post(url: &str, body: &str) -> Option<String> {
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .timeout_global(Some(Duration::from_secs(20)))
        .build()
        .into();
    agent
        .post(url)
        .header("Content-Type", "application/json")
        .send(body)
        .ok()?
        .body_mut()
        .read_to_string()
        .ok()
}

/// The `fanout` validators with the most activated stake, as (stake, node
/// identity): what repair weighs its peers by (`ShredNet::set_stakes`). Empty
/// when the node does not answer; the caller keeps what it had.
fn fetch_tier1(rpc_url: &str, fanout: usize) -> Vec<(u64, [u8; 32])> {
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"getVoteAccounts","params":[{"commitment":"finalized"}]}"#;
    let Some(response) = rpc_post(rpc_url, body) else {
        eprintln!("[tier1] HTTP request to {rpc_url} failed");
        return Vec::new();
    };
    let v: Value = serde_json::from_str(&response).unwrap_or_default();
    let Some(current) = v["result"]["current"].as_array() else {
        eprintln!("[tier1] unexpected RPC response shape");
        return Vec::new();
    };
    let mut validators: Vec<(u64, [u8; 32])> = current.iter().filter_map(|entry| {
        // The stake is a number on most nodes and a string on some.
        let stake = &entry["activatedStake"];
        let stake = stake.as_u64().or_else(|| stake.as_str()?.parse().ok())?;
        Some((stake, Pubkey::from_str(entry["nodePubkey"].as_str()?).ok()?.to_bytes()))
    }).collect();
    validators.sort_unstable_by_key(|v| std::cmp::Reverse(v.0));
    validators.truncate(fanout);
    eprintln!("[tier1] fetched {} validators (fanout={fanout})", validators.len());
    validators
}

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

// ─── the sink: sigverify gate + gRPC fan-out of assembler events ─────────────

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
struct ColibriSink {
    stats:         Mutex<Stats>,
    leader_sched:  Arc<Mutex<sigverify::LeaderScheduleCache>>,
    sv_verified:   AtomicU64,
    sv_rejected:   AtomicU64,
    sv_no_leader:  AtomicU64,
    /// Shreds admitted on a root whose signature was checked before.
    sv_cached:     AtomicU64,
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
            self.sv_rejected.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        // The root binds the slot and the shred's place: a known pair is the
        // leader's set, whatever shred of it this is.
        let known = self.verified_roots.lock().unwrap_or_else(|e| e.into_inner()).contains(&(root, sig));
        if known {
            self.sv_cached.fetch_add(1, Ordering::Relaxed);
        } else {
            let leader = self
                .leader_sched
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .leader_for_slot(slot);
            match leader {
                Some(pk) => {
                    if !sigverify::verify_root(&sig, &root, &pk) {
                        self.sv_rejected.fetch_add(1, Ordering::Relaxed);
                        return false;
                    }
                    self.sv_verified.fetch_add(1, Ordering::Relaxed);
                    let mut roots = self.verified_roots.lock().unwrap_or_else(|e| e.into_inner());
                    // ponytail: dropped whole when full, about the sets of the
                    // last few hundred slots; an LRU if re-verifying them shows.
                    if roots.len() >= VERIFIED_ROOTS {
                        roots.clear();
                    }
                    roots.insert((root, sig));
                }
                None => {
                    self.sv_no_leader.fetch_add(1, Ordering::Relaxed);
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
        if st.last_log.elapsed() >= Duration::from_secs(10) {
            st.behind_ms.sort_unstable();
            let at = |q: f64| st.behind_ms.get(((st.behind_ms.len().max(1) - 1) as f64 * q) as usize).copied();
            let (p10, p50, p90) = (at(0.1), at(0.5), at(0.9));
            // Transactions ahead of their batch's end, weighted by count.
            st.ahead_ms.sort_unstable();
            let total: u64 = st.ahead_ms.iter().map(|(_, n)| u64::from(*n)).sum();
            let ahead = |q: f64| {
                let mut left = (total as f64 * q) as u64;
                st.ahead_ms.iter().find(|(_, n)| { let n = u64::from(*n); if left < n { true } else { left -= n; false } }).map(|(ms, _)| *ms)
            };
            let early_share = if total > 0 { 100.0 * st.ahead_ms.iter().filter(|(ms, _)| *ms >= 1).map(|(_, n)| f64::from(*n)).sum::<f64>() / total as f64 } else { 0.0 };
            eprintln!(
                "[stream] admitted={} published={} footers={} tip={} equivocated={} \
                 behind_leader_ms[p10={p10:?} p50={p50:?} p90={p90:?}] \
                 txs_ahead_of_batch_end[{total} txs, {early_share:.0}% >=1ms, p50={:?} p90={:?} p99={:?} max={:?} ms] \
                 sigverify[ok={} by_known_root={} rejected={} dropped_no_leader={}]",
                st.admitted,
                st.published,
                st.footers,
                self.tip.load(Ordering::Relaxed),
                st.equivocated,
                ahead(0.5),
                ahead(0.9),
                ahead(0.99),
                st.ahead_ms.last().map(|(ms, _)| *ms),
                self.sv_verified.load(Ordering::Relaxed),
                self.sv_cached.load(Ordering::Relaxed),
                self.sv_rejected.load(Ordering::Relaxed),
                self.sv_no_leader.load(Ordering::Relaxed),
            );
            st.last_log = Instant::now();
            st.behind_ms.clear();
            st.ahead_ms.clear();
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

/// Load a Solana keypair JSON file, or generate a fresh one and save it so
/// gossip identity survives restarts. Format is the standard Solana JSON array
/// of 64 bytes — compatible with `solana-keygen new` output.
fn load_or_create_keypair(path: &str) -> Keypair {
    match read_keypair_file(path) {
        Ok(kp) => {
            eprintln!("[colibri] keypair loaded from {path}  pubkey={}", kp.pubkey());
            kp
        }
        Err(_) => {
            let kp = Keypair::new();
            match write_keypair_file(&kp, path) {
                Ok(_) => eprintln!("[colibri] generated new keypair, saved to {path}  pubkey={}", kp.pubkey()),
                Err(e) => eprintln!("[colibri] WARNING: could not write keypair to {path}: {e}  pubkey={}", kp.pubkey()),
            }
            kp
        }
    }
}

fn main() -> Result<()> {
    env_logger::init();
    let cfg = match config::Config::from_args(&env::args().collect::<Vec<_>>()) {
        Ok(Some(cfg)) => cfg,
        Ok(None) => { eprint!("{}", config::usage()); return Ok(()); }
        Err(e) => { eprint!("{e}\n\n{}", config::usage()); std::process::exit(1); }
    };

    let keypair = Arc::new(match &cfg.keypair_path {
        Some(path) => load_or_create_keypair(path),
        None => {
            let kp = Keypair::new();
            eprintln!("[colibri] keypair:       ephemeral (use --keypair <path> for stable identity)  pubkey={}", kp.pubkey());
            kp
        }
    });
    eprintln!("[colibri] pubkey:        {}", keypair.pubkey());
    match &cfg.stun_server {
        Some(stun) => eprintln!("[colibri] advertise ip:  discovered via STUN ({stun}) per socket at bind"),
        None       => eprintln!("[colibri] advertise ip:  {}", cfg.ip),
    }
    eprintln!("[colibri] rpc:           {}", cfg.rpc_url);
    eprintln!("[colibri] entrypoints:   {}", cfg.entrypoints.len());
    if cfg.entrypoints.len() < 3 {
        // --entrypoint REPLACES the list. One or two is the stall mode: a
        // single slow entrypoint leaves gossip at peers=0 indefinitely.
        eprintln!(
            "[colibri] WARNING: only {} entrypoint(s) — one dead entrypoint stalls gossip \
             (peers=0). Pass several, or omit --entrypoint for the built-in mainnet five.",
            cfg.entrypoints.len()
        );
    }
    eprintln!("[colibri] grpc port:     {}", cfg.grpc_port);
    eprintln!("[colibri] auth:          {}", if cfg.auth_token.is_some() { "token required" } else { "open (no auth)" });
    eprintln!("[colibri] tier1-fanout:  {}", cfg.tier1_fanout);
    eprintln!("[colibri] depth:         {}  window: {}  top-peers: {}  repair-rate: {}", cfg.depth, cfg.window, cfg.top_peers,
        if cfg.repair_rate == 0 { "no limit".to_string() } else { format!("{}/s", cfg.repair_rate) });

    if let (Some(cert_path), Some(key_path)) = (&cfg.tls_cert, &cfg.tls_key) {
        std::fs::metadata(cert_path)
            .map_err(|e| anyhow::anyhow!("cannot read TLS cert {cert_path}: {e}"))?;
        std::fs::metadata(key_path)
            .map_err(|e| anyhow::anyhow!("cannot read TLS key {key_path}: {e}"))?;
    }

    let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build()?;
    let exit = Arc::new(AtomicBool::new(false));

    {
        let exit_ctrlc = exit.clone();
        rt.spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            eprintln!("\n[colibri] Ctrl-C received, shutting down...");
            exit_ctrlc.store(true, Ordering::SeqCst);
        });
    }

    let (entry_tx, _) = broadcast::channel::<ProtoEntry>(8_192);
    let entry_tx = Arc::new(entry_tx);
    let (tx_tx, _) = broadcast::channel::<ProtoTransaction>(65_536);
    let tx_tx = Arc::new(tx_tx);
    // One footer per slot; 1_024 covers minutes of backlog for a slow subscriber.
    let (footer_tx, _) = broadcast::channel::<ProtoFooter>(1_024);
    let footer_tx = Arc::new(footer_tx);

    // Lowest slot a consumer asked to repair from (via `from-slot` metadata).
    let requested_from = Arc::new(AtomicU64::new(0));
    let nodes = Arc::new(server::NodeBook::new());

    let grpc_addr: SocketAddr = format!("0.0.0.0:{}", cfg.grpc_port).parse()?;
    let grpc_handle = {
        let _guard = rt.enter();
        server::start_grpc_server(
            grpc_addr,
            server::Streams {
                entries: entry_tx.clone(),
                txs:     tx_tx.clone(),
                footers: footer_tx.clone(),
                nodes:   nodes.clone(),
            },
            cfg.auth_token.clone(),
            cfg.tls_cert.clone().zip(cfg.tls_key.clone()),
            requested_from.clone(),
            exit.clone(),
        )
    };
    eprintln!("[colibri] gRPC on {grpc_addr}");
    if cfg.tls_cert.is_some() {
        eprintln!("[colibri] TLS:            enabled");
    } else {
        eprintln!("[colibri] TLS:            disabled (plain gRPC)");
    }

    // ── shred sigverify: the leader schedule, kept loaded in the background ──
    let leader_sched: Arc<Mutex<sigverify::LeaderScheduleCache>> =
        Arc::new(Mutex::new(sigverify::LeaderScheduleCache::new()));
    sigverify::spawn_schedule_fetcher(leader_sched.clone(), cfg.rpc_url.clone(), exit.clone());

    let meter: Arc<Mutex<coverage::CoverageMeter>> =
        Arc::new(Mutex::new(coverage::CoverageMeter::new()));

    // Oracle sampler (inert unless --oracle-rpc is passed).
    let oracle_sampler: Arc<Mutex<Option<oracle::OracleSampler>>> =
        Arc::new(Mutex::new(cfg.oracle_rpc.as_ref().map(|url| {
            eprintln!("[oracle] enabled — RPC oracle cross-check on {url}");
            oracle::OracleSampler::new(url.clone())
        })));

    let sink = Arc::new(ColibriSink {
        stats: Mutex::new(Stats {
            last_log:   Instant::now(),
            admitted:   0,
            published:  0,
            footers:    0,
            behind_ms:  Vec::new(),
            equivocated: 0,
            ahead_ms:   Vec::new(),
        }),
        leader_sched:  leader_sched.clone(),
        sv_verified:   AtomicU64::new(0),
        sv_rejected:   AtomicU64::new(0),
        sv_no_leader:  AtomicU64::new(0),
        sv_cached:     AtomicU64::new(0),
        verified_roots: Mutex::new(HashSet::new()),
        sent_at:       Mutex::new(HashMap::new()),
        open_batch:    Mutex::new(HashMap::new()),
        entry_tx,
        tx_tx,
        footer_tx,
        meter:         meter.clone(),
        oracle:        oracle_sampler.clone(),
        slot_sigs:     Mutex::new(HashMap::new()),
        tip:           AtomicU64::new(0),
    });

    // ── start the shred network (gossip + TVU + repair + reconstruction) ─────
    let net = ShredNet::start(
        ShredNetConfig {
            advertise_ip:  cfg.ip,
            gossip_port:   cfg.port,
            tvu_port:      cfg.tvu_port,
            repair_port:   cfg.repair_port,
            stun_server:   cfg.stun_server.clone(),
            shred_version: cfg.shred_version,
            entrypoints:   cfg.entrypoints.clone(),
            // Retain enough assembler history that a slow-completing backlog
            // slot near the consumer's floor isn't janitor-purged before it
            // finishes (the gap can be larger than --depth when driven by
            // from-slot).
            keep_window:   cfg.depth.max(30_000) + 2_000,
            target_window: cfg.window,
            top_peers:     cfg.top_peers,
            repair_rate:   cfg.repair_rate,
        },
        keypair,
        sink.clone(),
    )?;
    eprintln!("[colibri] ShredNet started — waiting for first turbine shred…");

    // ── tier-1 stakes: initial fetch + periodic refresh → repair peer scoring ─
    let to_stake_map = |v: Vec<(u64, [u8; 32])>| -> HashMap<[u8; 32], u64> {
        v.into_iter().map(|(stake, pk)| (pk, stake)).collect()
    };
    net.set_stakes(to_stake_map(fetch_tier1(&cfg.rpc_url, cfg.tier1_fanout)));
    let (stake_tx, stake_rx) = std::sync::mpsc::channel::<HashMap<[u8; 32], u64>>();
    {
        let rpc_url_c = cfg.rpc_url.clone();
        let fanout    = cfg.tier1_fanout;
        let exit_t1   = exit.clone();
        std::thread::spawn(move || loop {
            sleep(Duration::from_secs(TIER1_REFRESH_SECS));
            if exit_t1.load(Ordering::Relaxed) { break; }
            let updated = fetch_tier1(&rpc_url_c, fanout);
            if !updated.is_empty() && stake_tx.send(to_stake_map(updated)).is_err() {
                break;
            }
        });
    }

    // ── frontier loop: repair [floor .. live tip] ────────────────────────────
    // `floor` is the lowest `from-slot` any subscriber reported, else
    // `tip - depth`. `served_floor` = lowest slot requested; `last_req` =
    // highest.
    let start = Instant::now();
    let mut served_floor: Option<u64> = None;
    let mut last_req: u64 = 0;
    let mut nodes_read = Instant::now() - NODES_REFRESH;
    eprintln!("[colibri] serving + gap-filling (floor follows the consumer's from-slot)");

    let target = |a: u64, b: u64| {
        if a > b {
            return;
        }
        let now = Instant::now();
        let mut m = meter.lock().unwrap_or_else(|e| e.into_inner());
        for s in a..=b {
            m.mark_targeted(s, now);
        }
        drop(m);
        net.request_slots(a..=b);
    };

    loop {
        if exit.load(Ordering::Relaxed) {
            break;
        }
        while let Ok(map) = stake_rx.try_recv() {
            net.set_stakes(map);
        }
        // What gossip says each node runs. A node changes it when it restarts,
        // so every few seconds is far more often than it moves.
        if nodes_read.elapsed() >= NODES_REFRESH {
            nodes_read = Instant::now();
            let (changed, gone) = nodes.update(net.nodes().into_iter().map(|n| server::ProtoNode { identity: n.identity.to_vec(), client: n.client, version: n.version }));
            if (changed, gone) != (0, 0) { eprintln!("[nodes] {changed} appeared or changed what they run, {gone} left gossip"); }
        }
        // A node that hears nothing has lost the network (the host slept, the
        // NAT forgot its mapping, the link went), and on 2026-10-25 one sat so
        // for ~265,000 slots with every thread alive. Starting again rejoins
        // gossip and asks STUN anew; the supervisor does that.
        if let Some(silent) = net.silent_for().filter(|s| cfg.stall_secs > 0 && s.as_secs() >= cfg.stall_secs) {
            eprintln!("[colibri] no shred for {}s: the network is lost; exiting for the supervisor to start it again", silent.as_secs());
            std::process::exit(3);
        }
        let tip = net.observed_tip();
        if tip == 0 {
            sleep(Duration::from_millis(250));
            continue;
        }

        // Lower bound: the consumer's frontier, else --depth fallback.
        let rf = requested_from.load(Ordering::Relaxed);
        let floor = if rf > 0 { rf } else { tip.saturating_sub(cfg.depth) };

        match served_floor {
            None => {
                target(floor, tip.saturating_sub(1));
                eprintln!("[colibri] repairing from floor={floor} up to tip={tip}");
                served_floor = Some(floor);
                last_req = tip.saturating_sub(1);
            }
            // A (new/reconnected) consumer asked for an older floor → backfill.
            Some(sf) if floor < sf => {
                target(floor, sf.saturating_sub(1));
                eprintln!("[colibri] backfilling floor {sf} → {floor}");
                served_floor = Some(floor);
            }
            _ => {}
        }

        // Upper bound: follow the chain.
        if tip > last_req + 1 {
            target(last_req + 1, tip - 1);
            last_req = tip - 1;
        }
        sleep(Duration::from_millis(400));
    }

    // ── final report (printed on Ctrl-C) ─────────────────────────────────────
    {
        let rpt = meter.lock().unwrap_or_else(|e| e.into_inner()).report();
        let completeness_label = if rpt.completeness_pct >= 99.9 {
            "GREEN"
        } else if rpt.completeness_pct >= 50.0 {
            "YELLOW"
        } else {
            "RED"
        };

        eprintln!();
        eprintln!("════════════════════ COLIBRI COVERAGE REPORT ════════════════════");
        eprintln!("  status:         {completeness_label}");
        eprintln!("  targeted:       {}", rpt.targeted);
        eprintln!("  completed:      {}", rpt.completed);
        eprintln!("  completeness:   {:.2}%", rpt.completeness_pct);
        eprintln!("  latency p50:    {} ms", rpt.p50_ms);
        eprintln!("  latency p99:    {} ms", rpt.p99_ms);
        eprintln!("  latency max:    {} ms", rpt.max_ms);
        eprintln!(
            "  sigverify:      ok={} rejected={} dropped_no_leader={}",
            sink.sv_verified.load(Ordering::Relaxed),
            sink.sv_rejected.load(Ordering::Relaxed),
            sink.sv_no_leader.load(Ordering::Relaxed),
        );
        eprintln!("  uptime:         {}s", start.elapsed().as_secs());
        if let Ok(sampler_guard) = oracle_sampler.lock()
            && let Some(sampler) = sampler_guard.as_ref() {
                eprintln!("  {}", sampler.report_line());
            }
        eprintln!("═════════════════════════════════════════════════════════════════");
        eprintln!();
    }

    eprintln!("[colibri] waiting for gRPC server to shut down...");
    rt.block_on(grpc_handle).ok();
    net.shutdown();
    eprintln!("[colibri] shutdown complete");
    Ok(())
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
