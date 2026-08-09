//! `ShredNet` — the runnable wiring that turns the crate's pieces into a live
//! complete-lane: join gossip, receive turbine shreds, drive repair until each
//! targeted slot is `is_full`, and emit the reconstructed block to a sink.
//!
//! Threading (all `std::thread`, no imposed async runtime — Colibri embeds
//! this). The ingest path is decoupled from dispatch so a slow
//! rocksdb write never stalls socket draining or request pacing:
//!   - gossip threads (owned by `GossipService`)
//!   - TVU recv      — recv turbine shreds → insert channel + fast-lane hook
//!   - repair recv   — recv repair socket: ping→pong inline, shred→insert channel
//!                     (NO rocksdb on this thread, so it drains continuously and
//!                     never drops bursts)
//!   - insert worker — drain the channel and `insert_batch` into the Blockstore,
//!                     amortizing the rocksdb lock/commit over up to MAX_BATCH
//!                     shreds per call (the throughput lever)
//!   - dispatch      — timer-paced: admit targets, pick peers, send repair
//!                     requests, emit completed blocks, prune
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
    solana_entry::entry::Entry,
    solana_keypair::Keypair,
    std::{
        collections::{HashMap, HashSet, VecDeque},
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

/// Max shreds coalesced into a single `insert_batch` (rocksdb commit) call.
const MAX_BATCH: usize = 1024;
/// Bounded insert-channel capacity. Full ⇒ insert worker is the bottleneck.
const INSERT_CHAN_CAP: usize = 65_536;

/// Receives reconstructed blocks (and, optionally, the raw fast-lane shred feed).
pub trait BlockSink: Send + Sync + 'static {
    /// A targeted slot reached `is_full()`; `entries` is the complete block.
    fn on_complete_block(&self, slot: u64, entries: Vec<Entry>);
    /// A targeted slot was determined to be **skipped** — no block was ever
    /// produced for it, so there is nothing to reconstruct. Consumers should
    /// treat it as "accounted for" (advance replay past it). Default: ignore.
    fn on_slot_skipped(&self, _slot: u64) {}
    /// Every raw shred as it arrives (turbine + repair), BEFORE it is inserted
    /// into the Blockstore or advances the observed tip. Return `false` to
    /// drop the packet entirely (fail-closed sigverify gate); `true` to admit.
    /// Colibri's fast lane verifies + deshreds here. Default: admit.
    fn on_raw_shred(&self, _bytes: &[u8]) -> bool {
        true
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
    /// Purge slots below `tip - keep_window` to bound the ephemeral store.
    pub keep_window:  u64,
    /// How many slots to repair in parallel. Throughput is response-bound, so a
    /// wider window only helps if there's enough peer bandwidth to fill it.
    pub target_window: usize,
    /// How many distinct peers to spread repair requests across. Each peer
    /// rate-limits us independently, so aggregate serve-rate scales ~linearly
    /// with peer count (until the local insert path saturates).
    pub top_peers: usize,
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
            keep_window: 8_000,
            target_window: 64,
            top_peers: 64,
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
}

/// Per-targeted-slot driver bookkeeping (the have/last_index/is_full state lives
/// in the Blockstore via `Reconstructor`; only repair *policy* state is here).
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

    /// Start a fresh repair attempt after a blown deadline (re-probe from scratch).
    fn restart(&mut self) {
        self.deadline = Instant::now() + Duration::from_secs(30);
        self.repair_rounds = 0;
        self.highest_probed = false;
    }
}

/// A live shred-network participant + repair-driven reconstructor.
pub struct ShredNet {
    exit:         Arc<AtomicBool>,
    targets:      Arc<Mutex<VecDeque<u64>>>,
    stakes:       Arc<Mutex<HashMap<[u8; 32], u64>>>,
    observed_tip: Arc<AtomicU64>,
    handles:      Vec<JoinHandle<()>>,
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
        let node = gossip::join(
            &GossipConfig {
                advertise_ip: cfg.advertise_ip,
                gossip_port: cfg.gossip_port,
                tvu_port: cfg.tvu_port,
                shred_version: cfg.shred_version,
                entrypoints: cfg.entrypoints.clone(),
            },
            keypair.clone(),
            exit.clone(),
        )?;

        let reconstructor = Arc::new(Reconstructor::new()?);
        let targets: Arc<Mutex<VecDeque<u64>>> = Arc::new(Mutex::new(VecDeque::new()));
        let stakes: Arc<Mutex<HashMap<[u8; 32], u64>>> = Arc::new(Mutex::new(HashMap::new()));
        let observed_tip = Arc::new(AtomicU64::new(0));
        let diag = Arc::new(Diag::default());

        // Bounded insert channel — backpressure reveals an insert-bound state.
        let (insert_tx, insert_rx) = sync_channel::<Vec<u8>>(INSERT_CHAN_CAP);

        // Repair socket is shared: the recv thread reads it (and sends pongs);
        // the dispatch thread sends requests. UDP send from multiple threads is
        // fine; only the recv thread calls recv_from.
        let bind_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let repair_sock = UdpSocket::bind(SocketAddr::new(bind_ip, cfg.repair_port))?;
        repair_sock.set_read_timeout(Some(Duration::from_millis(5)))?;
        set_recv_buffer(&repair_sock, 32 * 1024 * 1024);
        let repair_sock = Arc::new(repair_sock);

        let mut handles = Vec::new();
        handles.push(spawn_tvu(
            cfg.tvu_port,
            exit.clone(),
            insert_tx.clone(),
            sink.clone(),
            observed_tip.clone(),
            diag.clone(),
        )?);
        handles.push(spawn_repair_recv(
            repair_sock.clone(),
            exit.clone(),
            keypair.clone(),
            insert_tx,
            sink.clone(),
            diag.clone(),
        ));
        handles.push(spawn_insert_worker(
            exit.clone(),
            insert_rx,
            reconstructor.clone(),
            diag.clone(),
        ));
        handles.push(spawn_dispatch(
            &cfg,
            repair_sock,
            keypair,
            exit.clone(),
            node.cluster_info.clone(),
            reconstructor,
            sink,
            targets.clone(),
            stakes.clone(),
            observed_tip.clone(),
            diag,
        ));

        Ok(Self {
            exit,
            targets,
            stakes,
            observed_tip,
            handles,
            _gossip: node.gossip_service,
        })
    }

    /// Enqueue slots to repair-until-complete. Idempotent; duplicates coalesce.
    pub fn request_slots(&self, slots: impl IntoIterator<Item = u64>) {
        let mut q = self.targets.lock().unwrap_or_else(|e| e.into_inner());
        q.extend(slots);
    }

    /// Update the stake table used to weight repair-peer selection (node pubkey
    /// → activated stake). Empty ⇒ recency-only peer scoring.
    pub fn set_stakes(&self, map: HashMap<[u8; 32], u64>) {
        *self.stakes.lock().unwrap_or_else(|e| e.into_inner()) = map;
    }

    /// Highest slot observed on the TVU socket so far (0 until the first shred).
    pub fn observed_tip(&self) -> u64 {
        self.observed_tip.load(Ordering::Relaxed)
    }

    /// Signal shutdown and join the worker threads.
    pub fn shutdown(self) {
        self.exit.store(true, Ordering::SeqCst);
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

/// Hand a shred to the insert worker; count a drop if the channel is full
/// (insert-bound). Returns false if the channel is disconnected (shutdown).
fn submit(tx: &SyncSender<Vec<u8>>, bytes: &[u8], diag: &Diag) -> bool {
    match tx.try_send(bytes.to_vec()) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            diag.chan_drop.fetch_add(1, Ordering::Relaxed);
            true
        }
        Err(TrySendError::Disconnected(_)) => false,
    }
}

fn spawn_tvu(
    tvu_port: u16,
    exit: Arc<AtomicBool>,
    insert_tx: SyncSender<Vec<u8>>,
    sink: Arc<dyn BlockSink>,
    observed_tip: Arc<AtomicU64>,
    diag: Arc<Diag>,
) -> anyhow::Result<JoinHandle<()>> {
    let bind_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    let tvu_socket = UdpSocket::bind(SocketAddr::new(bind_ip, tvu_port))?;
    tvu_socket.set_read_timeout(Some(Duration::from_millis(10)))?;
    set_recv_buffer(&tvu_socket, 32 * 1024 * 1024);

    Ok(std::thread::spawn(move || {
        let mut buf = [0u8; 1280];
        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }
            match tvu_socket.recv_from(&mut buf) {
                Ok((n, _)) => {
                    let bytes = &buf[..n];
                    // Fail-closed: an unadmitted (unverified) shred neither
                    // advances the tip nor reaches the Blockstore.
                    if sink.on_raw_shred(bytes) {
                        if let Some(slot) = slot_of(bytes) {
                            observed_tip.fetch_max(slot, Ordering::Relaxed);
                        }
                        if !submit(&insert_tx, bytes, &diag) {
                            break;
                        }
                    }
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(e) => log::warn!("[shred-net][tvu] recv error: {e}"),
            }
        }
    }))
}

/// Drains the repair socket continuously — pong replies inline (cheap), shred
/// responses handed to the insert worker. No rocksdb here, so a slow write can
/// never stall draining and drop a burst.
fn spawn_repair_recv(
    repair_sock: Arc<UdpSocket>,
    exit: Arc<AtomicBool>,
    keypair: Arc<Keypair>,
    insert_tx: SyncSender<Vec<u8>>,
    sink: Arc<dyn BlockSink>,
    diag: Arc<Diag>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut buf = [0u8; 1500];
        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }
            match repair_sock.recv_from(&mut buf) {
                Ok((n, src)) => match wire::parse_inbound(&buf[..n]) {
                    wire::Inbound::Ping(token) => {
                        diag.pings.fetch_add(1, Ordering::Relaxed);
                        let pong = wire::build_pong(&keypair, token);
                        if repair_sock.send_to(&pong, src).is_ok() {
                            diag.pongs.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    wire::Inbound::ShredResponse => {
                        diag.resp.fetch_add(1, Ordering::Relaxed);
                        // Same fail-closed gate as the TVU path.
                        if sink.on_raw_shred(&buf[..n])
                            && !submit(&insert_tx, &buf[..n], &diag)
                        {
                            break;
                        }
                    }
                    wire::Inbound::Other => {}
                },
                Err(_) => {} // WouldBlock/TimedOut on the 5ms read timeout
            }
        }
    })
}

/// Batches shreds off the channel into the Blockstore — one `insert_batch`
/// (rocksdb commit) per up-to-MAX_BATCH shreds, instead of one per shred.
fn spawn_insert_worker(
    exit: Arc<AtomicBool>,
    insert_rx: Receiver<Vec<u8>>,
    reconstructor: Arc<Reconstructor>,
    diag: Arc<Diag>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let mut batch: Vec<Vec<u8>> = Vec::with_capacity(MAX_BATCH);
        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }
            batch.clear();
            match insert_rx.recv_timeout(Duration::from_millis(20)) {
                Ok(first) => batch.push(first),
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => break,
            }
            while batch.len() < MAX_BATCH {
                match insert_rx.try_recv() {
                    Ok(b) => batch.push(b),
                    Err(_) => break,
                }
            }
            let n = reconstructor.insert_batch(&batch);
            diag.inserted.fetch_add(n as u64, Ordering::Relaxed);
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn spawn_dispatch(
    cfg: &ShredNetConfig,
    repair_sock: Arc<UdpSocket>,
    keypair: Arc<Keypair>,
    exit: Arc<AtomicBool>,
    cluster_info: Arc<solana_gossip::cluster_info::ClusterInfo>,
    reconstructor: Arc<Reconstructor>,
    sink: Arc<dyn BlockSink>,
    targets: Arc<Mutex<VecDeque<u64>>>,
    stakes: Arc<Mutex<HashMap<[u8; 32], u64>>>,
    observed_tip: Arc<AtomicU64>,
    diag: Arc<Diag>,
) -> JoinHandle<()> {
    const ORPHAN_AFTER: u32 = 5;
    // Deadlines with NO block data (last_index never learned) after this many
    // retries ⇒ the slot was skipped (leader never produced it). A real block
    // within the repair window yields its highest shred within seconds.
    const SKIP_AFTER_EMPTY_RETRIES: u32 = 2;
    let target_window = cfg.target_window.max(1);
    let top_peers = cfg.top_peers.max(1);
    let keep_window = cfg.keep_window;

    std::thread::spawn(move || {
        let mut driver: HashMap<u64, DriverState> = HashMap::new();
        // Slots determined to have no block (skipped). Never re-admitted; the
        // consumer was told via on_slot_skipped.
        let mut skipped: HashSet<u64> = HashSet::new();
        let mut nonce: u32 = 0xdead_beef;
        let mut last_purge = Instant::now();
        let mut last_diag = Instant::now();

        loop {
            if exit.load(Ordering::Relaxed) {
                break;
            }

            // ── admit new targets up to the in-flight window (never re-admit a
            //    slot we've already concluded is skipped) ──
            {
                let mut room = target_window.saturating_sub(driver.len());
                let mut q = targets.lock().unwrap_or_else(|e| e.into_inner());
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
            let now_ms = now_millis();
            let stake_map = stakes.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let peers = gossip::repair_peers(&cluster_info, &stake_map, now_ms, top_peers);

            // ── per-slot repair dispatch ──
            if !peers.is_empty() {
                let mut done = Vec::new();             // completed → remove
                let mut backstop_skip = Vec::new();    // no-block-after-retries → skip
                let mut reveal: Vec<(u64, u64)> = Vec::new(); // (completed slot, its parent)
                for (&slot, state) in driver.iter_mut() {
                    if Instant::now() >= state.deadline {
                        // Don't drop incomplete slots — a complete lane must reach
                        // 100% of producible slots. Retry forever; only conclude
                        // "skipped" when sustained tries yield NO block data.
                        state.retries += 1;
                        if reconstructor.last_index(slot).is_none()
                            && state.retries >= SKIP_AFTER_EMPTY_RETRIES
                        {
                            backstop_skip.push(slot);
                        } else {
                            state.restart();
                        }
                        continue;
                    }
                    if state.last_repair.elapsed() < Duration::from_millis(50) {
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
                            // Read the parent BEFORE purging — slots between it and
                            // this one were never produced (skip detection).
                            let parent = reconstructor.parent_slot(slot);
                            if let Some(entries) = reconstructor.take_complete(slot) {
                                sink.on_complete_block(slot, entries);
                            }
                            done.push(slot);
                            if let Some(p) = parent {
                                reveal.push((slot, p));
                            }
                        }
                        RepairAction::Wait => {}
                        RepairAction::ProbeHighest => {
                            state.last_repair = Instant::now();
                            state.repair_rounds += 1;
                            state.highest_probed = true;
                            for (pk, addr) in &peers {
                                let req = wire::highest_window_index(&keypair, pk, slot, 0, nonce);
                                send_counted(&repair_sock, &req, addr, &diag);
                                nonce = nonce.wrapping_add(1);
                            }
                        }
                        RepairAction::RequestOrphan => {
                            state.last_repair = Instant::now();
                            state.repair_rounds += 1;
                            for (pk, addr) in &peers {
                                let req = wire::orphan(&keypair, pk, slot, nonce);
                                send_counted(&repair_sock, &req, addr, &diag);
                                nonce = nonce.wrapping_add(1);
                            }
                        }
                        RepairAction::RequestWindows(batch) => {
                            state.last_repair = Instant::now();
                            state.repair_rounds += 1;
                            // Ask ONE peer per missing index (rotate across peers
                            // and rounds). Broadcasting every index to all peers is
                            // ~Nx redundant and trips serve_repair's per-requester
                            // rate limit; unserved indices retry next round against
                            // a different peer.
                            for (i, idx) in batch.iter().enumerate() {
                                let (pk, addr) =
                                    &peers[(i + state.repair_rounds as usize) % peers.len()];
                                let req = wire::window_index(&keypair, pk, slot, *idx, nonce);
                                send_counted(&repair_sock, &req, addr, &diag);
                                nonce = nonce.wrapping_add(1);
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

            // ── janitor: bound the ephemeral store ──
            if last_purge.elapsed() >= Duration::from_secs(5) {
                let tip = observed_tip.load(Ordering::Relaxed);
                if tip > keep_window {
                    reconstructor.purge_below(tip - keep_window);
                }
                last_purge = Instant::now();
            }

            // ── diagnostics: which boundary is the bottleneck? ──
            if last_diag.elapsed() >= Duration::from_secs(10) {
                let secs = last_diag.elapsed().as_secs_f64();
                let (mut known, mut full) = (0usize, 0usize);
                for &slot in driver.keys() {
                    if reconstructor.last_index(slot).is_some() {
                        known += 1;
                    }
                    if reconstructor.is_full(slot) {
                        full += 1;
                    }
                }
                let take = |c: &AtomicU64| c.swap(0, Ordering::Relaxed) as f64 / secs;
                eprintln!(
                    "[shred-net] driver={} peers={} skipped={} | reqs/s={:.0} resp/s={:.0} inserted/s={:.0} \
                     chan_drop/s={:.0} send_err/s={:.0} pings/s={:.0} pongs/s={:.0} | \
                     last_idx_known={}/{} full_in_map={}",
                    driver.len(), peers.len(), skipped.len(),
                    take(&diag.reqs), take(&diag.resp), take(&diag.inserted),
                    take(&diag.chan_drop), take(&diag.send_err),
                    take(&diag.pings), take(&diag.pongs),
                    known, driver.len(), full,
                );
                last_diag = Instant::now();
            }

            sleep(Duration::from_millis(10));
        }
    })
}

fn send_counted(sock: &UdpSocket, req: &[u8], addr: &SocketAddr, diag: &Diag) {
    match sock.send_to(req, addr) {
        Ok(_) => diag.reqs.fetch_add(1, Ordering::Relaxed),
        Err(_) => diag.send_err.fetch_add(1, Ordering::Relaxed),
    };
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
