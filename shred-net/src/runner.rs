//! `ShredNet` — the runnable wiring that turns the crate's pieces into a live
//! complete-lane: join gossip, receive turbine shreds, drive repair until each
//! targeted slot is `is_full`, and emit the reconstructed block to a sink.
//!
//! Threading (all `std::thread`, no imposed async runtime — Colibri embeds
//! this). The ingest path is decoupled from dispatch so a slow
//! assembly never stalls socket draining or request pacing:
//! - gossip threads, owned by `GossipService`;
//! - TVU recv: turbine shreds to the insert channel and the fast-lane hook;
//! - repair recv: the repair socket, a ping answered inline and a shred sent
//!   to the insert channel (no assembly on this thread, so it drains
//!   continuously and never drops bursts);
//! - insert worker: drains the channel and `insert_batch`es into the
//!   assembler, one lock for up to MAX_BATCH shreds (the throughput lever);
//! - dispatch, timer-paced: admits targets, picks peers, sends repair
//!   requests, emits completed blocks, prunes.
//!
//! The channel is BOUNDED: if the insert worker can't keep up, sends fail and
//! bump `chan_drop` — a direct signal that the bottleneck is the insert path,
//! not the network.
//!
//! Acceptance is a live mainnet run (like Phase 0), not unit tests: the socket
//! I/O and peer dynamics can only be exercised against the real cluster.

use {
    crate::{
        gossip::{self, GossipConfig},
        reconstruct::Reconstructor,
        repair::{next_repair_action, RepairAction},
        wire,
    },
    deshredder::Event,
    solana_keypair::Keypair,
    std::{
        collections::{BTreeMap, HashMap, HashSet, VecDeque},
        net::{IpAddr, Ipv4Addr, SocketAddr, UdpSocket},
        sync::{
            atomic::{AtomicBool, AtomicU64, Ordering},
            mpsc::{sync_channel, Receiver, RecvTimeoutError, SyncSender, TrySendError},
            Arc, Mutex,
        },
        thread::{sleep, JoinHandle},
        time::{Duration, Instant},
    },
};

/// How often the repair driver takes up newly asked slots, picks what each
/// slot in flight needs and sends it. Whoever asks for slots gains nothing by
/// asking more often than this.
pub const DISPATCH_PASS: Duration = Duration::from_millis(10);

/// Max shreds coalesced into a single `insert_batch` (one assembler lock) call.
const MAX_BATCH: usize = 1024;
/// Bounded insert-channel capacity. Full ⇒ insert worker is the bottleneck.
const INSERT_CHAN_CAP: usize = 65_536;

/// Receives reconstructed blocks (and, optionally, the raw fast-lane shred feed).
pub trait BlockSink: Send + Sync + 'static {
    /// Assembler output for one insert batch, in order: entry batches as soon
    /// as they are contiguous (turbine or repair alike), block footers, and
    /// `SlotComplete` after a slot's last batch. Default: ignore.
    fn on_events(&self, _events: Vec<Event>) {}
    /// A targeted slot was determined to be **skipped** — no block was ever
    /// produced for it, so there is nothing to reconstruct. Consumers should
    /// treat it as "accounted for" (advance replay past it). Default: ignore.
    fn on_slot_skipped(&self, _slot: u64) {}
    /// Every raw shred as it arrives (turbine + repair), BEFORE it reaches the
    /// assembler or advances the observed tip. Return `false` to drop the
    /// packet entirely (fail-closed sigverify gate); `true` to admit.
    fn on_raw_shred(&self, _bytes: &[u8]) -> bool {
        true
    }
    /// Identities leading at `slot` or soon after. Repair is not asked of
    /// them: a node that leads charges ten times the cost for what it serves
    /// (agave `serve_repair`, `LEADER_BYTE_COST_MULTIPLIER`), so it answers
    /// little while it builds blocks. Default: none known.
    fn leaders_near(&self, _slot: u64) -> Vec<[u8; 32]> {
        Vec::new()
    }
}

/// Configuration for a `ShredNet` instance.
pub struct ShredNetConfig {
    pub advertise_ip: IpAddr,
    pub gossip_port:  u16,
    pub tvu_port:     u16,
    pub repair_port:  u16,
    pub shred_version: Option<u16>,
    pub entrypoints:  Vec<String>,
    /// STUN server (`host:port`) for running behind a port-rewriting NAT.
    /// `Some` ⇒ advertise each socket's discovered external mapping instead of
    /// `advertise_ip` plus the bound port, and keep the TVU mapping alive.
    /// `None` on a host with a real public IP. See [`crate::stun`].
    pub stun_server: Option<String>,
    /// Purge slots below `tip - keep_window` to bound the ephemeral store.
    pub keep_window:  u64,
    /// How many slots to repair in parallel. Throughput is response-bound, so a
    /// wider window only helps if there's enough peer bandwidth to fill it.
    pub target_window: usize,
    /// How many distinct peers to spread repair requests across. Each peer
    /// rate-limits us independently, so aggregate serve-rate scales ~linearly
    /// with peer count (until the local insert path saturates).
    pub top_peers: usize,
    /// Most repair requests sent per second; 0 sends as fast as the window
    /// asks. A home uplink cannot carry the unlimited rate (15-28k/s measured
    /// at window 32).
    pub repair_rate: u64,
}

impl Default for ShredNetConfig {
    fn default() -> Self {
        Self {
            advertise_ip: IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            gossip_port: 8000,
            tvu_port: 8200,
            repair_port: 8210,
            shred_version: None,
            entrypoints: Vec::new(),
            stun_server: None,
            keep_window: 8_000,
            target_window: 64,
            // 128, not 64: measured 2026-09-23, response yield per request
            // 14.5% → 27.9% and completed slots 235 → 365 in 170 s.
            top_peers: 128,
            repair_rate: 0,
        }
    }
}

/// Cross-thread boundary counters for the diagnostic line.
#[derive(Default)]
struct Diag {
    reqs:      AtomicU64,
    send_err:  AtomicU64,
    resp:      AtomicU64,
    inserted:  AtomicU64,
    pings:     AtomicU64,
    pongs:     AtomicU64,
    chan_drop: AtomicU64,
    /// When the last admitted shred arrived, turbine or repair, unix ms; 0 before the first.
    last_shred_ms: AtomicU64,
    /// For each FEC set seen, `(slot, fec_set_index)`, the positions of it that
    /// came from turbine: data 0..32, coding 32..64. A set seen only through
    /// repair is here with none. What turbine gives a node decides how much
    /// repair it needs.
    coverage: Mutex<BTreeMap<(u64, u32), u64>>,
}

impl Diag {
    fn admitted(&self, bytes: &[u8], from_turbine: bool) {
        self.last_shred_ms.store(now_millis(), Ordering::Relaxed);
        if let Some((set, position)) = set_position(bytes) {
            let mut coverage = self.coverage.lock().unwrap_or_else(|e| e.into_inner());
            let seen = coverage.entry(set).or_default();
            if from_turbine {
                *seen |= 1 << position;
            }
        }
    }
}

/// A Merkle shred's `(slot, fec_set_index)` and its place in a 32:32 set:
/// data 0..32 by index, coding 32..64 by position. `None` for any other shape.
fn set_position(buf: &[u8]) -> Option<((u64, u32), u32)> {
    let slot = u64::from_le_bytes(buf.get(65..73)?.try_into().ok()?);
    let index = u32::from_le_bytes(buf.get(73..77)?.try_into().ok()?);
    let set = u32::from_le_bytes(buf.get(79..83)?.try_into().ok()?);
    let position = match buf.get(64)? >> 4 {
        0x9 | 0xb => index.checked_sub(set).filter(|p| *p < 32)?,
        0x6 | 0x7 => 32 + u32::from(u16::from_le_bytes(buf.get(87..89)?.try_into().ok()?)).min(32),
        _ => return None,
    };
    (position < 64).then_some(((slot, set), position))
}

/// How many of each settled FEC set came from turbine, as `[sets, p10, p50,
/// p90, recoverable %]`; recoverable is 32 or more of 64, enough to rebuild
/// the set with no repair. Takes the sets below `before` out of `coverage`.
fn coverage_below(coverage: &mut BTreeMap<(u64, u32), u64>, before: u64) -> Option<[f64; 5]> {
    let rest = coverage.split_off(&(before, 0));
    let settled = std::mem::replace(coverage, rest);
    let mut counts: Vec<u32> = settled.values().map(|m| m.count_ones()).collect();
    if counts.is_empty() {
        return None;
    }
    counts.sort_unstable();
    let at = |q: f64| f64::from(counts[((counts.len() - 1) as f64 * q) as usize]);
    let recoverable = counts.iter().filter(|c| **c >= 32).count();
    Some([counts.len() as f64, at(0.1), at(0.5), at(0.9), 100.0 * recoverable as f64 / counts.len() as f64])
}

/// A peer drawn with odds by `weights`; evenly where they are all 0.
// ponytail: xorshift, not a crate's RNG: it only spreads requests across peers.
fn weighted(weights: &[u64], rng: &mut u64) -> usize {
    *rng ^= *rng << 13;
    *rng ^= *rng >> 7;
    *rng ^= *rng << 17;
    let total: u64 = weights.iter().sum();
    if total == 0 {
        return (*rng % weights.len().max(1) as u64) as usize;
    }
    let mut at = *rng % total;
    weights.iter().position(|w| match at.checked_sub(*w) {
        Some(left) => {
            at = left;
            false
        }
        None => true,
    }).unwrap_or(0)
}

/// Per-targeted-slot driver bookkeeping (the have/last_index/is_full state lives
/// in the assembler via `Reconstructor`; only repair *policy* state is here).
struct DriverState {
    last_repair:    Instant,
    repair_rounds:  u32,
    highest_probed: bool,
    deadline:       Instant,
    /// How many times this slot has blown its deadline without completing.
    retries:        u32,
}

impl DriverState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            last_repair: now - Duration::from_secs(1),
            repair_rounds: 0,
            highest_probed: false,
            deadline: now + Duration::from_secs(30),
            retries: 0,
        }
    }

    /// The deadline passed. True when the slot should be reported skipped: it
    /// still has no block data after `skip_after` attempts that each sent a
    /// probe. Otherwise starts a fresh attempt. An attempt that never got a
    /// probe out (the repair budget went to older slots) is not counted, or
    /// a real block starved of budget would be reported as never produced.
    fn on_deadline(&mut self, has_data: bool, skip_after: u32) -> bool {
        if self.highest_probed {
            self.retries += 1;
        }
        if !has_data && self.retries >= skip_after {
            return true;
        }
        self.restart();
        false
    }

    /// Start a fresh repair attempt after a blown deadline (re-probe from scratch).
    fn restart(&mut self) {
        self.deadline = Instant::now() + Duration::from_secs(30);
        self.repair_rounds = 0;
        self.highest_probed = false;
    }
}

/// What the threads share.
struct Shared {
    exit:         Arc<AtomicBool>,
    /// Slots asked for and not yet taken up by the repair driver.
    targets:      Mutex<VecDeque<u64>>,
    /// Node identity to activated stake: what repair weighs its peers by.
    stakes:       Mutex<HashMap<[u8; 32], u64>>,
    /// Highest slot a turbine data shred was admitted for.
    observed_tip: AtomicU64,
    diag:         Diag,
    sink:         Arc<dyn BlockSink>,
}

impl Shared {
    fn exiting(&self) -> bool { self.exit.load(Ordering::Relaxed) }

    /// A shred off a socket: past the sink's gate (fail-closed: one it does
    /// not admit neither advances the tip nor reaches the assembler), then to
    /// the insert worker. False when that worker is gone and the thread
    /// should stop.
    fn take(&self, bytes: &[u8], from_turbine: bool, insert_tx: &SyncSender<Vec<u8>>) -> bool {
        if !self.sink.on_raw_shred(bytes) { return true; }
        self.diag.admitted(bytes, from_turbine);
        if from_turbine && let Some(slot) = slot_of(bytes) {
            self.observed_tip.fetch_max(slot, Ordering::Relaxed);
        }
        match insert_tx.try_send(bytes.to_vec()) {
            Ok(()) => true,
            // The insert worker is the bottleneck: counted, and the shred is repaired later.
            Err(TrySendError::Full(_)) => { self.diag.chan_drop.fetch_add(1, Ordering::Relaxed); true }
            Err(TrySendError::Disconnected(_)) => false,
        }
    }
}

/// A live shred-network participant + repair-driven reconstructor.
pub struct ShredNet {
    shared:       Arc<Shared>,
    handles:      Vec<JoinHandle<()>>,
    cluster_info: Arc<solana_gossip::cluster_info::ClusterInfo>,
    // Kept alive so gossip threads keep running; never read directly.
    _gossip:      solana_gossip::gossip_service::GossipService,
}

impl ShredNet {
    /// Join gossip and start the worker threads. Returns immediately; the node
    /// runs in the background until [`ShredNet::shutdown`].
    pub fn start(
        cfg: ShredNetConfig,
        keypair: Arc<Keypair>,
        sink: Arc<dyn BlockSink>,
    ) -> anyhow::Result<Self> {
        let exit = Arc::new(AtomicBool::new(false));
        let bind_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);

        // Bind TVU before joining gossip: behind a NAT the port we bind is not
        // the port turbine must be sent to, and ContactInfo has to carry the
        // mapping that actually exists.
        let tvu_socket = UdpSocket::bind(SocketAddr::new(bind_ip, cfg.tvu_port))?;
        tvu_socket.set_read_timeout(Some(Duration::from_millis(10)))?;
        set_recv_buffer(&tvu_socket, 32 * 1024 * 1024);
        let tvu_advertise = match &cfg.stun_server {
            Some(server) => {
                let mapped = crate::stun::external_addr(&tvu_socket, server)?;
                log::info!(
                    "[shred-net] tvu: bound :{}, advertising {mapped}",
                    cfg.tvu_port
                );
                mapped
            }
            None => SocketAddr::new(cfg.advertise_ip, cfg.tvu_port),
        };
        let tvu_socket = Arc::new(tvu_socket);

        let node = gossip::join(
            &GossipConfig {
                advertise_ip: cfg.advertise_ip,
                gossip_port: cfg.gossip_port,
                tvu_advertise,
                stun_server: cfg.stun_server.clone(),
                shred_version: cfg.shred_version,
                entrypoints: cfg.entrypoints.clone(),
            },
            keypair.clone(),
            exit.clone(),
        )?;

        let reconstructor = Arc::new(Reconstructor::new());
        let shared = Arc::new(Shared {
            exit: exit.clone(),
            targets: Mutex::default(),
            stakes: Mutex::default(),
            observed_tip: AtomicU64::new(0),
            diag: Diag::default(),
            sink,
        });

        // Bounded insert channel — backpressure reveals an insert-bound state.
        let (insert_tx, insert_rx) = sync_channel::<Vec<u8>>(INSERT_CHAN_CAP);

        // Repair socket is shared: the recv thread reads it (and sends pongs);
        // the dispatch thread sends requests. UDP send from multiple threads is
        // fine; only the recv thread calls recv_from.
        let repair_sock = UdpSocket::bind(SocketAddr::new(bind_ip, cfg.repair_port))?;
        repair_sock.set_read_timeout(Some(Duration::from_millis(5)))?;
        set_recv_buffer(&repair_sock, 32 * 1024 * 1024);
        let repair_sock = Arc::new(repair_sock);

        let mut handles = Vec::new();
        // TVU only ever receives, so nothing refreshes its NAT mapping; without
        // a keepalive the turbine feed stops when the router expires it.
        if let Some(server) = cfg.stun_server.clone() {
            handles.push(crate::stun::spawn_keepalive(
                tvu_socket.clone(),
                server,
                exit.clone(),
            ));
        }
        handles.push(spawn_tvu(tvu_socket, shared.clone(), insert_tx.clone()));
        handles.push(spawn_repair_recv(repair_sock.clone(), keypair.clone(), shared.clone(), insert_tx));
        handles.push(spawn_insert_worker(insert_rx, reconstructor.clone(), shared.clone()));
        handles.push(spawn_dispatch(&cfg, repair_sock, keypair, node.cluster_info.clone(), reconstructor, shared.clone()));

        Ok(Self {
            shared,
            handles,
            cluster_info: node.cluster_info,
            _gossip: node.gossip_service,
        })
    }

    /// Every node gossip knows now, with the client each says it runs.
    pub fn nodes(&self) -> Vec<gossip::Node> {
        gossip::nodes(&self.cluster_info)
    }

    /// Enqueue slots to repair-until-complete. Idempotent; duplicates coalesce.
    pub fn request_slots(&self, slots: impl IntoIterator<Item = u64>) {
        let mut q = self.shared.targets.lock().unwrap_or_else(|e| e.into_inner());
        q.extend(slots);
    }

    /// Update the stake table used to weight repair-peer selection (node pubkey
    /// → activated stake). Empty ⇒ recency-only peer scoring.
    pub fn set_stakes(&self, map: HashMap<[u8; 32], u64>) {
        *self.shared.stakes.lock().unwrap_or_else(|e| e.into_inner()) = map;
    }

    /// Highest slot observed on the TVU socket so far (0 until the first shred).
    pub fn observed_tip(&self) -> u64 {
        self.shared.observed_tip.load(Ordering::Relaxed)
    }

    /// How long since the last shred arrived, turbine or repair; `None`
    /// before the first. A node that hears nothing has lost the network,
    /// whatever its threads say.
    pub fn silent_for(&self) -> Option<Duration> {
        let last = self.shared.diag.last_shred_ms.load(Ordering::Relaxed);
        (last > 0).then(|| Duration::from_millis(now_millis().saturating_sub(last)))
    }

    /// Signal shutdown and join the worker threads.
    pub fn shutdown(self) {
        self.shared.exit.store(true, Ordering::SeqCst);
        for h in self.handles {
            let _ = h.join();
        }
    }
}

/// Extract a shred's slot from raw bytes (LE u64 at offset 65), gated on the
/// shred-variant byte at offset 64 — same recognizer as `wire::parse_inbound`.
fn slot_of(buf: &[u8]) -> Option<u64> {
    if buf.len() < 88 {
        return None;
    }
    let variant = buf[64];
    let is_shred = matches!(variant & 0xF0, 0x80 | 0x90 | 0xB0) || variant == 0xA5;
    if !is_shred {
        return None;
    }
    Some(u64::from_le_bytes(buf[65..73].try_into().ok()?))
}

/// Drain turbine. The socket is bound and configured by the caller so its NAT
/// mapping can be discovered before gossip advertises it.
fn spawn_tvu(tvu_socket: Arc<UdpSocket>, shared: Arc<Shared>, insert_tx: SyncSender<Vec<u8>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buf = [0u8; 1280];
        while !shared.exiting() {
            match tvu_socket.recv_from(&mut buf) {
                Ok((n, _)) => if !shared.take(&buf[..n], true, &insert_tx) { break },
                // Nothing came within the read timeout.
                Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => {}
                Err(e) => log::warn!("[shred-net][tvu] recv error: {e}"),
            }
        }
    })
}

/// Drains the repair socket continuously — pong replies inline (cheap), shred
/// responses handed to the insert worker, so assembly never stalls draining.
fn spawn_repair_recv(repair_sock: Arc<UdpSocket>, keypair: Arc<Keypair>, shared: Arc<Shared>, insert_tx: SyncSender<Vec<u8>>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buf = [0u8; 1500];
        let diag = &shared.diag;
        while !shared.exiting() {
            // An error is WouldBlock or TimedOut on the read timeout: nothing came.
            let Ok((n, src)) = repair_sock.recv_from(&mut buf) else { continue };
            match wire::parse_inbound(&buf[..n]) {
                wire::Inbound::Ping(token) => {
                    diag.pings.fetch_add(1, Ordering::Relaxed);
                    if repair_sock.send_to(&wire::build_pong(&keypair, token), src).is_ok() {
                        diag.pongs.fetch_add(1, Ordering::Relaxed);
                    }
                }
                wire::Inbound::ShredResponse => {
                    diag.resp.fetch_add(1, Ordering::Relaxed);
                    if !shared.take(&buf[..n], false, &insert_tx) { break; }
                }
                wire::Inbound::Other => {}
            }
        }
    })
}

/// Batches shreds off the channel into the assembler — one lock per
/// up-to-MAX_BATCH shreds — and hands whatever came out to the sink.
fn spawn_insert_worker(insert_rx: Receiver<Vec<u8>>, reconstructor: Arc<Reconstructor>, shared: Arc<Shared>) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(MAX_BATCH);
        let mut events: Vec<Event> = Vec::new();
        while !shared.exiting() {
            batch.clear();
            match insert_rx.recv_timeout(Duration::from_millis(20)) {
                Ok(first) => batch.push(first),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
            batch.extend(insert_rx.try_iter().take(MAX_BATCH - 1));
            let n = reconstructor.insert_batch(&batch, &mut events);
            shared.diag.inserted.fetch_add(n as u64, Ordering::Relaxed);
            if !events.is_empty() {
                shared.sink.on_events(std::mem::take(&mut events));
            }
        }
    })
}

/// Repair requests going out of one socket, each with the next nonce.
struct Asker<'a> {
    sock:    &'a UdpSocket,
    keypair: &'a Keypair,
    diag:    &'a Diag,
    nonce:   u32,
}

impl Asker<'_> {
    /// Send `peer` the request `build` makes of our key, its identity and a nonce.
    fn ask(&mut self, (peer, addr): &([u8; 32], SocketAddr), build: impl FnOnce(&Keypair, &[u8; 32], u32) -> Vec<u8>) {
        match self.sock.send_to(&build(self.keypair, peer, self.nonce), addr) {
            Ok(_) => self.diag.reqs.fetch_add(1, Ordering::Relaxed),
            Err(_) => self.diag.send_err.fetch_add(1, Ordering::Relaxed),
        };
        self.nonce = self.nonce.wrapping_add(1);
    }
}

/// How often gossip is read for repair peers: as often as gossip itself
/// runs, since its table holds nothing new in between. A read copies every
/// contact held, thousands of them, and was done on every 10 ms pass.
const GOSSIP_READ: Duration = Duration::from_millis(solana_gossip::cluster_info::GOSSIP_SLEEP_MILLIS);

fn spawn_dispatch(
    cfg: &ShredNetConfig,
    repair_sock: Arc<UdpSocket>,
    keypair: Arc<Keypair>,
    cluster_info: Arc<solana_gossip::cluster_info::ClusterInfo>,
    reconstructor: Arc<Reconstructor>,
    shared: Arc<Shared>,
) -> JoinHandle<()> {
    const ORPHAN_AFTER: u32 = 5;
    // Peers a HighestWindowIndex / Orphan goes to. One answer is enough, and
    // a broadcast to every repair peer made top_peers 128 cost 128 requests
    // per slot.
    const PROBE_PEERS: usize = 16;
    // Deadlines with NO block data (not one data shred) after this many probed
    // retries ⇒ the slot was skipped (leader never produced it). A real block
    // within the repair window yields its highest shred within seconds.
    const SKIP_AFTER_EMPTY_RETRIES: u32 = 2;
    let target_window = cfg.target_window.max(1);
    let top_peers = cfg.top_peers.max(1);
    let keep_window = cfg.keep_window;
    let repair_rate = (cfg.repair_rate > 0).then_some(cfg.repair_rate);

    std::thread::spawn(move || {
        // Ordered, so the repair budget goes oldest slot first. With a
        // HashMap the iteration order was fixed per map, the same few slots
        // took each second's budget and the rest never sent a request
        // (rounds=0 after 5 deadlines, measured 2026-10-19), while an in-order
        // consumer waits on the oldest.
        let mut driver: BTreeMap<u64, DriverState> = BTreeMap::new();
        // Slots determined to have no block (skipped). Never re-admitted; the
        // consumer was told via on_slot_skipped.
        let mut skipped: HashSet<u64> = HashSet::new();
        let (sink, diag) = (&shared.sink, &shared.diag);
        let mut asker = Asker { sock: &repair_sock, keypair: &keypair, diag, nonce: 0xdead_beef };
        let mut rng: u64 = now_millis() | 1;
        // The best repair peers gossip holds, with the stake each is drawn by,
        // and when gossip was read; of them, those not leading now, and the
        // leaders they were picked against.
        let mut scored: Vec<(([u8; 32], SocketAddr), u64)> = Vec::new();
        let mut scored_at = None::<Instant>;
        let mut peers: Vec<([u8; 32], SocketAddr)> = Vec::new();
        let mut weights: Vec<u64> = Vec::new();
        let mut leading: Option<Vec<[u8; 32]>> = None;
        let mut last_purge = Instant::now();
        let mut last_diag = Instant::now();
        // Requests that may go out now; `None` is no limit. A token bucket
        // holding at most BUDGET_BURST worth of the rate, not a budget reset
        // once a second: that went out as one burst of up to 1,500 packets
        // in the first ~10 ms, and the answers came back as a burst.
        let (mut budget, mut budget_since) = (repair_rate.map(|_| 0), Instant::now());

        while !shared.exiting() {
            // ── admit new targets up to the in-flight window (never re-admit a
            //    slot we've already concluded is skipped) ──
            {
                let mut room = target_window.saturating_sub(driver.len());
                let mut q = shared.targets.lock().unwrap_or_else(|e| e.into_inner());
                while room > 0 {
                    match q.pop_front() {
                        Some(slot) if skipped.contains(&slot) => continue,
                        Some(slot) => {
                            driver.entry(slot).or_insert_with(DriverState::new);
                            room -= 1;
                        }
                        None => break,
                    }
                }
            }

            // ── select peers ──
            if scored_at.is_none_or(|at| at.elapsed() >= GOSSIP_READ) {
                let stakes = shared.stakes.lock().unwrap_or_else(|e| e.into_inner());
                scored = gossip::repair_peers(&cluster_info, &stakes, now_millis(), top_peers).into_iter()
                    .map(|peer| (peer, stakes.get(&peer.0).copied().unwrap_or(0)))
                    .collect();
                (scored_at, leading) = (Some(Instant::now()), None);
            }
            // Who leads is asked on every pass: it moves with the slot, however
            // long a slot is, and the pick is redone only when it has moved.
            // Each request goes to a peer drawn by stake, as agave's own repair
            // draws them: a staked node answers more, and no peer is asked in turn.
            let now_leading = sink.leaders_near(shared.observed_tip.load(Ordering::Relaxed));
            if leading.as_ref() != Some(&now_leading) {
                (peers, weights) = scored.iter().filter(|(peer, _)| !now_leading.contains(&peer.0)).copied().unzip();
                leading = Some(now_leading);
            }
            let busy = leading.as_ref().map_or(0, Vec::len);

            if let (Some(left), Some(rate)) = (budget.as_mut(), repair_rate)
                && let Some(add) = refill(rate, budget_since.elapsed()) {
                    *left = (*left + add).min(rate * BUDGET_BURST.as_millis() as u64 / 1000);
                    budget_since = Instant::now();
                }

            // ── per-slot repair dispatch ──
            if !peers.is_empty() {
                let probe = &peers[..peers.len().min(PROBE_PEERS)];
                let mut done = Vec::new();             // completed → remove
                let mut backstop_skip = Vec::new();    // no-block-after-retries → skip
                let mut reveal: Vec<(u64, u64)> = Vec::new(); // (completed slot, its parent)
                for (&slot, state) in driver.iter_mut() {
                    if Instant::now() >= state.deadline {
                        // Don't drop incomplete slots — a complete lane must reach
                        // 100% of producible slots. Retry forever; only conclude
                        // "skipped" when sustained tries yield NO block data.
                        // Any data shred means the block exists; last_index
                        // alone is not enough, since turbine loses a slot's
                        // last FEC set in about half the slots (2026-10-19:
                        // a real block was reported skipped this way).
                        let has_data = reconstructor.received(slot).is_some();
                        if state.on_deadline(has_data, SKIP_AFTER_EMPTY_RETRIES) {
                            backstop_skip.push(slot);
                        }
                        continue;
                    }
                    // 200 ms, not 50: under a repair_rate cap, re-asking the
                    // oldest slot every 50 ms spent the whole budget on one
                    // or two slots at a time (88.6% → 96.1% of slots in
                    // 170 s at 1,500 req/s, 2026-10-19).
                    if state.last_repair.elapsed() < Duration::from_millis(200) {
                        continue;
                    }

                    let is_full = reconstructor.is_full(slot);
                    let last_index = reconstructor.last_index(slot);
                    let missing = if is_full {
                        Vec::new()
                    } else {
                        reconstructor.missing_indices(slot, 128)
                    };

                    let action = next_repair_action(
                        is_full,
                        last_index,
                        &missing,
                        state.highest_probed,
                        state.repair_rounds,
                        ORPHAN_AFTER,
                    );

                    match action {
                        RepairAction::Complete => {
                            // The entries already left through `on_events`;
                            // slots between the parent and this one were never
                            // produced (skip detection).
                            let parent = reconstructor.parent_slot(slot);
                            done.push(slot);
                            if let Some(p) = parent {
                                reveal.push((slot, p));
                            }
                        }
                        RepairAction::Wait => {}
                        RepairAction::ProbeHighest => {
                            let n = grant(&mut budget, probe.len());
                            if n == 0 {
                                continue;
                            }
                            state.last_repair = Instant::now();
                            state.repair_rounds += 1;
                            state.highest_probed = true;
                            for peer in &probe[..n] {
                                asker.ask(peer, |key, to, nonce| wire::highest_window_index(key, to, slot, 0, nonce));
                            }
                        }
                        RepairAction::RequestOrphan => {
                            let n = grant(&mut budget, probe.len());
                            if n == 0 {
                                continue;
                            }
                            state.last_repair = Instant::now();
                            state.repair_rounds += 1;
                            for peer in &probe[..n] {
                                asker.ask(peer, |key, to, nonce| wire::orphan(key, to, slot, nonce));
                            }
                        }
                        RepairAction::RequestWindows(batch) => {
                            // The extra HighestWindowIndex below rides along
                            // uncounted: at most one per slot round.
                            let n = grant(&mut budget, batch.len());
                            if n == 0 {
                                continue;
                            }
                            state.last_repair = Instant::now();
                            state.repair_rounds += 1;
                            // No per-index dedup beyond the 200 ms slot pace.
                            // Measured 2026-09-23 (170 s runs, depth 500, no
                            // rate cap):
                            // serve_repair answers ~15% of an unstaked
                            // requester's requests at 64 peers regardless of
                            // rate, so agave's own 150 ms re-ask timeout
                            // halved requests and completed slots with it
                            // (377 → 235). Re-sending is how a low-stake node
                            // gets through load shedding. Spreading over more
                            // peers is what raised the yield (28% at 128).
                            // Ask ONE peer per missing index (rotate across peers
                            // and rounds). Broadcasting every index to all peers is
                            // ~Nx redundant and trips serve_repair's per-requester
                            // rate limit; unserved indices retry next round against
                            // a different peer.
                            for &idx in &batch[..n] {
                                asker.ask(&peers[weighted(&weights, &mut rng)], |key, to, nonce| wire::window_index(key, to, slot, idx, nonce));
                            }
                            // Length still unknown: keep asking one peer for
                            // the highest shred, as agave's repair does.
                            if last_index.is_none() {
                                asker.ask(&peers[state.repair_rounds as usize % peers.len()], |key, to, nonce| wire::highest_window_index(key, to, slot, 0, nonce));
                            }
                        }
                    }
                }
                for slot in done {
                    driver.remove(&slot);
                }
                // Backstop skips: no block after sustained retries.
                for slot in backstop_skip {
                    driver.remove(&slot);
                    if skipped.insert(slot) {
                        sink.on_slot_skipped(slot);
                    }
                }
                // Parent-link skips: every slot strictly between a completed slot
                // and its parent was never produced. Bounded to slots we're
                // actively tracking (in `driver`) so we stay within the requested
                // range; the scan is capped to guard against a pathological gap.
                for (slot, parent) in reveal {
                    let mut s = parent + 1;
                    while s < slot && slot - s <= 1024 {
                        if driver.remove(&s).is_some() && skipped.insert(s) {
                            sink.on_slot_skipped(s);
                        }
                        s += 1;
                    }
                }
            }

            // ── janitor: bound the assembler ──
            if last_purge.elapsed() >= Duration::from_secs(5) {
                let tip = shared.observed_tip.load(Ordering::Relaxed);
                if tip > keep_window {
                    reconstructor.purge_below(tip - keep_window);
                    // A slot that far back is not asked for again: kept, the
                    // set grew by every slot ever skipped.
                    skipped.retain(|slot| *slot >= tip - keep_window);
                }
                last_purge = Instant::now();
            }

            // ── diagnostics: which boundary is the bottleneck? ──
            if last_diag.elapsed() >= Duration::from_secs(10) {
                let secs = last_diag.elapsed().as_secs_f64();
                let (mut known, mut full, mut gaps) = (0usize, 0usize, 0usize);
                for &slot in driver.keys() {
                    gaps += reconstructor.missing_indices(slot, usize::MAX).len();
                    if reconstructor.last_index(slot).is_some() {
                        known += 1;
                    }
                    if reconstructor.is_full(slot) {
                        full += 1;
                    }
                }
                // Per-slot view of the three oldest in-flight slots.
                for (slot, st) in driver.iter().take(3) {
                    let missing = reconstructor.missing_indices(*slot, 8);
                    eprintln!(
                        "[shred-net]   slot {slot}: last_index={:?} received={:?} full={} rounds={} retries={} probed={} missing[..8]={:?}",
                        reconstructor.last_index(*slot), reconstructor.received(*slot), reconstructor.is_full(*slot),
                        st.repair_rounds, st.retries, st.highest_probed, missing,
                    );
                }
                let take = |c: &AtomicU64| c.swap(0, Ordering::Relaxed) as f64 / secs;
                eprintln!(
                    "[shred-net] driver={} peers={} skipped={} | reqs/s={:.0} resp/s={:.0} inserted/s={:.0} \
                     chan_drop/s={:.0} send_err/s={:.0} pings/s={:.0} pongs/s={:.0} | \
                     last_idx_known={}/{} full_in_map={} asm_slots={} missing={}",
                    driver.len(), peers.len(), skipped.len(),
                    take(&diag.reqs), take(&diag.resp), take(&diag.inserted),
                    take(&diag.chan_drop), take(&diag.send_err),
                    take(&diag.pings), take(&diag.pongs),
                    known, driver.len(), full, reconstructor.active_slots(), gaps,
                );
                // Sets well behind the tip have had all their turbine shreds.
                let settled_before = shared.observed_tip.load(Ordering::Relaxed).saturating_sub(64);
                let mut coverage = diag.coverage.lock().unwrap_or_else(|e| e.into_inner());
                if let Some([sets, p10, p50, p90, recoverable]) = coverage_below(&mut coverage, settled_before) {
                    eprintln!(
                        "[coverage] {sets:.0} FEC sets, of 64 shreds each from turbine: p10={p10:.0} p50={p50:.0} p90={p90:.0}; \
                         {recoverable:.1}% rebuildable without repair | leaders skipped for repair={} conflicts={} \
                         off_spec={} entries streamed before their batch was whole={}",
                        busy, reconstructor.conflicts(), reconstructor.off_spec(), reconstructor.early_entries(),
                    );
                }
                last_diag = Instant::now();
            }

            sleep(DISPATCH_PASS);
        }
    })
}

/// Most repair requests that may leave back to back, as time at `repair_rate`.
const BUDGET_BURST: Duration = Duration::from_millis(50);

/// Whole requests earned at `rate`/s over `elapsed`; `None` below one, so the
/// fraction keeps accruing instead of being dropped.
fn refill(rate: u64, elapsed: Duration) -> Option<u64> {
    let add = (rate as u128 * elapsed.as_micros() / 1_000_000) as u64;
    (add > 0).then_some(add)
}

/// How many of `want` requests fit in what is left this tick; the rest wait
/// for the slot's next round. `None` is no limit.
fn grant(budget: &mut Option<u64>, want: usize) -> usize {
    match budget {
        None => want,
        Some(left) => {
            let n = want.min(*left as usize);
            *left -= n as u64;
            n
        }
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Best-effort enlarge the UDP receive buffer (turbine/repair bursts overflow
/// the default). Requires `sysctl net.core.rmem_max` headroom for full effect.
fn set_recv_buffer(socket: &UdpSocket, bytes: usize) {
    #[cfg(unix)]
    {
        use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
        let s2 = unsafe { socket2::Socket::from_raw_fd(socket.as_raw_fd()) };
        s2.set_recv_buffer_size(bytes).ok();
        let _ = s2.into_raw_fd(); // don't close the borrowed fd on drop
    }
    #[cfg(not(unix))]
    let _ = (socket, bytes);
}

#[cfg(test)]
mod tests {
    use {
        super::{coverage_below, grant, refill, set_position, weighted, DriverState},
        std::{collections::BTreeMap, time::Duration},
    };

    /// A packet with only the fields `set_position` reads.
    fn shred(variant: u8, slot: u64, index: u32, set: u32, position: u16) -> Vec<u8> {
        let mut b = vec![0u8; 1228];
        b[64] = variant;
        b[65..73].copy_from_slice(&slot.to_le_bytes());
        b[73..77].copy_from_slice(&index.to_le_bytes());
        b[79..83].copy_from_slice(&set.to_le_bytes());
        b[87..89].copy_from_slice(&position.to_le_bytes());
        b
    }

    #[test]
    fn places_a_shred_in_its_set() {
        assert_eq!(set_position(&shred(0x96, 7, 69, 64, 0)), Some(((7, 64), 5)), "data by index");
        assert_eq!(set_position(&shred(0x66, 7, 99, 64, 3)), Some(((7, 64), 35)), "coding after the data");
        assert_eq!(set_position(&shred(0x96, 7, 99, 64, 0)), None, "a data index past a 32-shred set");
        assert_eq!(set_position(&shred(0x66, 7, 0, 64, 40)), None, "a coding position past 32");
        assert_eq!(set_position(&shred(0xa5, 7, 1, 0, 0)), None, "legacy");
    }

    #[test]
    fn tells_how_much_of_each_settled_set_turbine_gave() {
        let mut coverage = BTreeMap::from([((1, 0), u64::MAX), ((1, 32), 0), ((2, 0), (1 << 31) - 1), ((9, 0), 1)]);
        let [sets, p10, p50, p90, recoverable] = coverage_below(&mut coverage, 5).unwrap();
        assert_eq!((sets, p10, p50, p90, recoverable), (3.0, 0.0, 31.0, 31.0, 100.0 / 3.0));
        assert_eq!(coverage.len(), 1, "the set not settled yet stays");
        assert!(coverage_below(&mut coverage, 5).is_none());
    }

    #[test]
    fn draws_peers_by_stake() {
        let mut rng = 42;
        let draws: Vec<usize> = (0..1000).map(|_| weighted(&[0, 30, 0, 70], &mut rng)).collect();
        assert!(draws.iter().all(|d| [1, 3].contains(d)), "no stake, never drawn");
        let heavy = draws.iter().filter(|d| **d == 3).count();
        assert!((600..800).contains(&heavy), "about 70% to the 70: {heavy}");
        assert!((0..100).map(|_| weighted(&[0, 0, 0], &mut rng)).collect::<std::collections::HashSet<_>>().len() == 3, "evenly with none staked");
    }

    #[test]
    fn refill_earns_whole_requests_only() {
        assert_eq!(refill(1500, Duration::from_millis(10)), Some(15));
        assert_eq!(refill(1500, Duration::from_micros(500)), None, "fraction keeps accruing");
    }

    #[test]
    fn only_probed_attempts_count_toward_skipped() {
        let mut st = DriverState::new();
        for _ in 0..5 {
            assert!(!st.on_deadline(false, 2), "never probed: starved, not skipped");
        }
        st.highest_probed = true;
        assert!(!st.on_deadline(false, 2));
        assert!(!st.highest_probed, "a new attempt re-probes");
        st.highest_probed = true;
        assert!(st.on_deadline(false, 2), "two probed attempts, no data: skipped");

        let mut st = DriverState::new();
        st.retries = 9;
        st.highest_probed = true;
        assert!(!st.on_deadline(true, 2), "a slot with data is never skipped");
    }

    #[test]
    fn repair_sends_what_fits_in_the_tick() {
        let mut budget = Some(100);
        assert_eq!(grant(&mut budget, 60), 60);
        assert_eq!(grant(&mut budget, 60), 40, "a round is cut to what is left");
        assert_eq!(grant(&mut budget, 1), 0, "then nothing until the next tick");
        assert_eq!(grant(&mut None, 10_000), 10_000, "no limit");
    }
}
