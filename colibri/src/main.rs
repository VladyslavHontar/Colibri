//! Colibri: Solana shred-to-transaction node. Joins gossip, receives shreds via
//! TVU (Turbine), repairs missing slots over the repair protocol, and streams
//! entries/transactions to subscribers via a Jito-compatible gRPC service.
//!
//! Two lanes over one ingest (both fed by `shred-net`'s sockets through
//! `BlockSink::on_raw_shred`):
//!
//!   - FAST lane — every leader-sigverified shred goes straight into the
//!     in-memory `deshredder`; entry batches stream out with minimal latency,
//!     `complete = true` on the batch that finishes the slot.
//!   - COMPLETE lane — `shred-net` drives repair until every targeted slot is
//!     `is_full()` in an ephemeral Blockstore, emitting whole blocks for slots
//!     the fast lane missed and empty-entry skip markers for slots that were
//!     never produced. The targeted range follows the consumer's `from-slot`
//!     frontier (gRPC metadata), falling back to `tip - depth`.
//!
//! Sigverify is fail-closed on BOTH lanes: `on_raw_shred` returning `false`
//! means the shred is not emitted, not inserted into the Blockstore, and does
//! not advance the observed tip.

#![allow(deprecated)]

mod coverage;
mod oracle;
mod server;
mod sigverify;

use {
    anyhow::Result,
    deshredder::{Deshredder, Entry, Event},
    serde_json::Value,
    server::{ProtoEntry, ProtoFooter, ProtoTransaction},
    shred_net::{BlockSink, ShredNet, ShredNetConfig},
    solana_keypair::{read_keypair_file, write_keypair_file, Keypair},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        collections::{HashMap, HashSet},
        env,
        net::{IpAddr, SocketAddr},
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

const TURBINE_FANOUT: usize = 200;

/// Used when neither the config file nor the command line names any. One
/// dead entrypoint stalls gossip, so the default is all five, not one.
const MAINNET_ENTRYPOINTS: [&str; 5] = [
    "entrypoint.mainnet-beta.solana.com:8001",
    "entrypoint2.mainnet-beta.solana.com:8001",
    "entrypoint3.mainnet-beta.solana.com:8001",
    "entrypoint4.mainnet-beta.solana.com:8001",
    "entrypoint5.mainnet-beta.solana.com:8001",
];
const DEFAULT_CONFIG_PATH: &str = "colibri.json";
const TIER1_REFRESH_SECS: u64 = 600;

fn print_usage() {
    eprintln!("Usage: colibri [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --config <PATH>         JSON config; keys are the flags below without dashes,");
    eprintln!("                          underscores for hyphens, `entrypoints` as an array.");
    eprintln!("                          ./colibri.json is loaded automatically if present.");
    eprintln!("                          Command-line flags override the file.");
    eprintln!("  --ip <IP>               Public IP to advertise in gossip");
    eprintln!("                          (required unless --stun is given)");
    eprintln!("  --port <PORT>           Gossip UDP port (default: 8000)");
    eprintln!("  --tvu-port <PORT>       TVU port where shreds arrive (default: 8200)");
    eprintln!("  --repair-port <PORT>    UDP port for repair responses (default: 8210)");
    eprintln!("  --stun <HOST:PORT>      STUN server; advertise each socket's real");
    eprintln!("                          external NAT mapping instead of --ip:port.");
    eprintln!("                          Needed behind a port-rewriting NAT; omit");
    eprintln!("                          on a host with a real public IP.");
    eprintln!("  --entrypoint <ADDR>     Solana entrypoint (repeatable). Default: the five");
    eprintln!("                          mainnet-beta entrypoints; flags replace the file's list.");
    eprintln!("  --shred-version <VER>   Shred version (default: fetched from entrypoint)");
    eprintln!("  --rpc <URL>             RPC endpoint for stake + leader-schedule data");
    eprintln!("  --tier1-fanout <N>      Stake table size for repair peer scoring (default: 200)");
    eprintln!("  --grpc-port <PORT>      gRPC listen port (default: 8888)");
    eprintln!("  --auth-token <TOKEN>    Bearer token required for gRPC subscribers (optional)");
    eprintln!("  --tls-cert <PATH>       TLS certificate PEM (enables TLS when combined with --tls-key)");
    eprintln!("  --tls-key <PATH>        TLS private key PEM");
    eprintln!("  --keypair <PATH>        Path to keypair JSON file (load or auto-create for stable gossip identity)");
    eprintln!("  --depth <N>             Backfill depth from tip when no consumer reports from-slot (default: 500)");
    eprintln!("  --window <N>            Slots repaired in parallel (default: 64)");
    eprintln!("  --top-peers <N>         Distinct peers repair requests are spread across (default: 128)");
    eprintln!("  --oracle-rpc <URL>      Enable RPC oracle cross-check (samples completed slots via getBlock)");
    eprintln!("  --help                  Print this help");
}

struct Config {
    ip:            IpAddr,
    port:          u16,
    tvu_port:      u16,
    repair_port:   u16,
    stun_server:   Option<String>,
    entrypoints:   Vec<String>,
    shred_version: Option<u16>,
    rpc_url:       String,
    tier1_fanout:  usize,
    grpc_port:     u16,
    auth_token:    Option<String>,
    tls_cert:      Option<String>, // path to PEM certificate
    tls_key:       Option<String>, // path to PEM private key
    keypair_path:  Option<String>,
    depth:         u64,
    window:        usize,
    top_peers:     usize,
    oracle_rpc:    Option<String>,
}

fn parse_args() -> Result<Config, Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mut ip: Option<IpAddr> = None;
    let mut port: u16 = 8000;
    let mut tvu_port: u16 = 8200;
    let mut repair_port: u16 = 8210;
    let mut stun_server: Option<String> = None;
    let mut entrypoints: Vec<String> = Vec::new();
    let mut shred_version: Option<u16> = None;
    let mut rpc_url = "http://api.mainnet-beta.solana.com".to_string();
    let mut tier1_fanout: usize = TURBINE_FANOUT;
    let mut grpc_port: u16 = 8888;
    let mut auth_token: Option<String> = None;
    let mut tls_cert: Option<String> = None;
    let mut tls_key: Option<String> = None;
    let mut keypair_path: Option<String> = None;
    let mut depth: u64 = 500;
    let mut window: usize = 64;
    let mut top_peers: usize = 128;
    let mut oracle_rpc: Option<String> = None;

    // Config file first, so every flag below simply overrides it. `--config`
    // names it explicitly; otherwise ./colibri.json is used when it exists.
    let config_path = args.iter().position(|a| a == "--config")
        .map(|i| args.get(i + 1).cloned().ok_or("--config needs a path"))
        .transpose()?
        .or_else(|| std::path::Path::new(DEFAULT_CONFIG_PATH).exists().then(|| DEFAULT_CONFIG_PATH.to_string()));
    let mut file_entrypoints: Vec<String> = Vec::new();
    if let Some(path) = &config_path {
        let text = std::fs::read_to_string(path).map_err(|e| format!("config {path}: {e}"))?;
        let v: Value = serde_json::from_str(&text).map_err(|e| format!("config {path}: {e}"))?;
        let obj = v.as_object().ok_or_else(|| format!("config {path}: top level must be an object"))?;
        let str_ = |k: &str| obj.get(k).map(|x| x.as_str().map(str::to_string).ok_or(format!("config {path}: `{k}` must be a string"))).transpose();
        let num = |k: &str| obj.get(k).map(|x| x.as_u64().ok_or(format!("config {path}: `{k}` must be an integer"))).transpose();
        for key in obj.keys() {
            const KNOWN: [&str; 18] = ["ip","port","tvu_port","repair_port","stun","entrypoints","shred_version","rpc",
                "tier1_fanout","grpc_port","auth_token","tls_cert","tls_key","keypair","depth","window","top_peers","oracle_rpc"];
            if !KNOWN.contains(&key.as_str()) {
                return Err(format!("config {path}: unknown key `{key}` (known: {})", KNOWN.join(", ")).into());
            }
        }
        if let Some(x) = str_("ip")?            { ip = Some(x.parse()?); }
        if let Some(x) = num("port")?           { port = x.try_into()?; }
        if let Some(x) = num("tvu_port")?       { tvu_port = x.try_into()?; }
        if let Some(x) = num("repair_port")?    { repair_port = x.try_into()?; }
        if let Some(x) = str_("stun")?          { stun_server = Some(x); }
        if let Some(x) = num("shred_version")?  { shred_version = Some(x.try_into()?); }
        if let Some(x) = str_("rpc")?           { rpc_url = x; }
        if let Some(x) = num("tier1_fanout")?   { tier1_fanout = x as usize; }
        if let Some(x) = num("grpc_port")?      { grpc_port = x.try_into()?; }
        if let Some(x) = str_("auth_token")?    { auth_token = Some(x); }
        if let Some(x) = str_("tls_cert")?      { tls_cert = Some(x); }
        if let Some(x) = str_("tls_key")?       { tls_key = Some(x); }
        if let Some(x) = str_("keypair")?       { keypair_path = Some(x); }
        if let Some(x) = num("depth")?          { depth = x; }
        if let Some(x) = num("window")?         { window = x as usize; }
        if let Some(x) = num("top_peers")?      { top_peers = x as usize; }
        if let Some(x) = str_("oracle_rpc")?    { oracle_rpc = Some(x); }
        if let Some(arr) = obj.get("entrypoints") {
            let arr = arr.as_array().ok_or_else(|| format!("config {path}: `entrypoints` must be an array"))?;
            for e in arr {
                file_entrypoints.push(e.as_str().ok_or_else(|| format!("config {path}: entrypoints must be strings"))?.to_string());
            }
        }
        eprintln!("[colibri] config:        {path}");
    }

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--config"        => { i += 1; } // consumed above
            "--ip"            => { i += 1; ip            = Some(args[i].parse()?); }
            "--port"          => { i += 1; port          = args[i].parse()?; }
            "--tvu-port"      => { i += 1; tvu_port      = args[i].parse()?; }
            "--repair-port"   => { i += 1; repair_port   = args[i].parse()?; }
            "--stun"          => { i += 1; stun_server   = Some(args[i].clone()); }
            "--entrypoint"    => { i += 1; entrypoints.push(args[i].clone()); }
            "--shred-version" => { i += 1; shred_version = Some(args[i].parse()?); }
            "--rpc"           => { i += 1; rpc_url       = args[i].clone(); }
            "--tier1-fanout"  => { i += 1; tier1_fanout  = args[i].parse()?; }
            "--grpc-port"     => { i += 1; grpc_port     = args[i].parse()?; }
            "--auth-token"    => { i += 1; auth_token    = Some(args[i].clone()); }
            "--tls-cert"      => { i += 1; tls_cert      = Some(args[i].clone()); }
            "--tls-key"       => { i += 1; tls_key       = Some(args[i].clone()); }
            "--keypair"       => { i += 1; keypair_path  = Some(args[i].clone()); }
            "--depth"         => { i += 1; depth         = args[i].parse()?; }
            "--window"        => { i += 1; window        = args[i].parse()?; }
            "--top-peers"     => { i += 1; top_peers     = args[i].parse()?; }
            "--oracle-rpc"    => { i += 1; oracle_rpc    = Some(args[i].clone()); }
            "--help" | "-h"   => { print_usage(); std::process::exit(0); }
            other => {
                eprintln!("Unknown argument: {other}");
                print_usage();
                std::process::exit(1);
            }
        }
        i += 1;
    }

    if entrypoints.is_empty() {
        entrypoints = file_entrypoints;
    }
    if entrypoints.is_empty() {
        entrypoints = MAINNET_ENTRYPOINTS.iter().map(|s| s.to_string()).collect();
    }

    // STUN derives the real advertised IP from the socket mapping itself, so
    // --ip is only a required fallback when there's no STUN server to ask.
    let ip = match ip {
        Some(ip) => ip,
        None if stun_server.is_some() => IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED),
        None => return Err("--ip <PUBLIC_IP> is required (or pass --stun)".into()),
    };
    Ok(Config {
        ip, port, tvu_port, repair_port, stun_server, entrypoints, shred_version,
        rpc_url, tier1_fanout, grpc_port, auth_token, tls_cert, tls_key,
        keypair_path, depth, window, top_peers, oracle_rpc,
    })
}

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

/// Fetch the top-`fanout` validators by activated stake: (stake, node pubkey).
/// Fed to shred-net's repair-peer scoring via `set_stakes`.
fn fetch_tier1(rpc_url: &str, fanout: usize) -> Vec<(u64, [u8; 32])> {
    let body = r#"{"jsonrpc":"2.0","id":1,"method":"getVoteAccounts","params":[{"commitment":"finalized"}]}"#;

    let resp = match rpc_post(rpc_url, body) {
        Some(r) => r,
        None => {
            eprintln!("[tier1] HTTP request to {rpc_url} failed");
            return Vec::new();
        }
    };

    let v: Value = match serde_json::from_str(&resp) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("[tier1] JSON parse error: {e}");
            return Vec::new();
        }
    };

    let current = match v["result"]["current"].as_array() {
        Some(arr) => arr,
        None => {
            eprintln!("[tier1] unexpected RPC response shape");
            return Vec::new();
        }
    };

    let mut validators: Vec<(u64, [u8; 32])> = current
        .iter()
        .filter_map(|entry| {
            let pubkey_str = entry["nodePubkey"].as_str()?;
            let stake: u64 = if let Some(n) = entry["activatedStake"].as_u64() {
                n
            } else if let Some(s) = entry["activatedStake"].as_str() {
                s.parse().ok()?
            } else {
                return None;
            };
            let arr: [u8; 32] = Pubkey::from_str(pubkey_str).ok()?.to_bytes();
            Some((stake, arr))
        })
        .collect();

    validators.sort_by(|a, b| b.0.cmp(&a.0));
    let result: Vec<(u64, [u8; 32])> = validators.into_iter().take(fanout).collect();
    eprintln!("[tier1] fetched {} validators (fanout={fanout})", result.len());
    result
}

/// Slot of a raw shred (LE u64 at offset 65), gated on the shred-variant byte
/// at offset 64 — same recognizer shred-net's ingest uses.
fn shred_slot(buf: &[u8]) -> Option<u64> {
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
    fn rejects_non_shred_and_short_packets() {
        let mut buf = [0u8; 88];
        buf[64] = 0x00; // not a shred variant
        assert_eq!(shred_slot(&buf), None);
        assert_eq!(shred_slot(&[0u8; 40]), None);
    }
}

// ─── the sink: sigverify gate + fast lane + complete lane ───────────────────

/// Mutable fast-lane state, locked once per admitted near-tip shred.
struct FastLane {
    deshredder: Deshredder,
    /// Reused across pushes so the quiet path allocates nothing.
    events:     Vec<Event>,
    last_log:   Instant,
    total:      u64,
    published:  u64,
    /// Block footers seen — tells whether Alpenglow markers are live upstream.
    footers:    u64,
}

/// Colibri's `BlockSink`: leader-keyed sigverify gate (fail-closed), inline
/// deshredder fast lane, and complete-lane block/skip emission.
struct ColibriSink {
    fast:          Mutex<FastLane>,
    leader_sched:  Arc<Mutex<sigverify::LeaderScheduleCache>>,
    sv_verified:   AtomicU64,
    sv_rejected:   AtomicU64,
    sv_no_leader:  AtomicU64,
    entry_tx:      Arc<broadcast::Sender<ProtoEntry>>,
    tx_tx:         Arc<broadcast::Sender<ProtoTransaction>>,
    footer_tx:     Arc<broadcast::Sender<ProtoFooter>>,
    meter:         Arc<Mutex<coverage::CoverageMeter>>,
    oracle:        Arc<Mutex<Option<oracle::OracleSampler>>>,
    /// Slots the fast lane finished — the complete lane skips re-emitting them
    /// (a subscriber already holds every entry batch of the slot).
    fast_complete: Mutex<HashSet<u64>>,
    /// Highest verified slot seen; used to keep deep-backfill repair shreds out
    /// of the fast lane (they belong to the complete lane only).
    tip:           AtomicU64,
}

impl ColibriSink {
    /// Serialize one batch and fan it out to both streams. `complete` marks
    /// the batch that finished the slot (see `Entry.complete` in the proto).
    /// wincode is byte-compatible with bincode, so subscribers keep decoding
    /// the payload exactly as before.
    fn publish(&self, slot: u64, entries: Vec<Entry>, complete: bool) {
        let bytes = match wincode::serialize(&entries) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("[fast] slot {slot}: wincode::serialize failed: {e}");
                return;
            }
        };
        let _ = self.entry_tx.send(ProtoEntry { slot, entries: bytes, complete });
        self.emit_txs(slot, &entries, complete);
    }

    fn emit_txs(&self, slot: u64, entries: &[Entry], complete: bool) {
        // Per-tx base58 + bincode is the most expensive work here; skip it
        // entirely when nothing consumes it.
        if self.tx_tx.receiver_count() == 0 {
            return;
        }
        for entry in entries {
            for tx in &entry.transactions {
                let sig = tx.signatures.first().map(|s| s.to_string()).unwrap_or_default();
                if let Ok(raw) = bincode::serialize(tx) {
                    let _ = self.tx_tx.send(ProtoTransaction {
                        slot, signature: sig, raw_tx: raw, complete,
                    });
                }
            }
        }
    }
}

impl BlockSink for ColibriSink {
    /// Sigverify gate + fast lane. Fail-closed for BOTH branches
    /// (agave-faithful):
    ///   * leader known  → verify signature over the Merkle root; DROP on
    ///                     failure.
    ///   * leader unknown → DROP; the lookup records the epoch so the
    ///                     background thread fetches its schedule on demand.
    /// Returning `false` keeps the shred out of the gRPC stream, out of the
    /// Blockstore, and keeps it from advancing the observed tip.
    fn on_raw_shred(&self, bytes: &[u8]) -> bool {
        let Some(slot) = shred_slot(bytes) else { return false };

        let leader = self
            .leader_sched
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .leader_for_slot(slot);
        match leader {
            Some(pk) => {
                if !sigverify::verify_shred_signature(bytes, &pk) {
                    self.sv_rejected.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
                self.sv_verified.fetch_add(1, Ordering::Relaxed);
            }
            None => {
                self.sv_no_leader.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        }

        let tip = self.tip.fetch_max(slot, Ordering::Relaxed).max(slot);

        // Fast lane only near the tip: deep-backfill repair shreds would
        // thrash the deshredder's dedup window; those slots are served by the
        // complete lane below.
        if slot + 1_000 >= tip {
            let mut fast = self.fast.lock().unwrap_or_else(|e| e.into_inner());
            fast.total += 1;
            // The deshredder has no clock: the same 1_000-slot window that
            // gates admission above is what bounds its memory.
            fast.deshredder.evict_below(tip.saturating_sub(1_000));
            let mut events = std::mem::take(&mut fast.events);
            events.clear();
            fast.deshredder.push(bytes, &mut events);
            // Wire contract: `complete = true` rides on the batch that finished
            // the slot. Batches and completion are separate events, so hold
            // each batch until we know whether completion follows it.
            let mut pending: Option<(u64, Vec<Entry>)> = None;
            for ev in events.drain(..) {
                match ev {
                    Event::Entries { slot, entries } => {
                        if let Some((s, e)) = pending.replace((slot, entries)) {
                            self.publish(s, e, false);
                            fast.published += 1;
                        }
                    }
                    Event::SlotComplete { slot } => {
                        self.meter
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .mark_complete(slot, Instant::now());
                        let mut fc = self.fast_complete.lock().unwrap_or_else(|e| e.into_inner());
                        fc.insert(slot);
                        // ponytail: leak guard — the complete lane removes each
                        // entry as it resolves the slot; clear if that ever stalls.
                        if fc.len() > 65_536 {
                            fc.clear();
                        }
                        drop(fc);
                        let entries = match pending.take() {
                            Some((s, e)) if s == slot => e,
                            other => { if let Some((s, e)) = other { self.publish(s, e, false); } Vec::new() }
                        };
                        self.publish(slot, entries, true);
                        fast.published += 1;
                    }
                    // Next step: carry the footer on the proto so consumers
                    // get the bank hash without waiting for votes.
                    Event::Footer { slot, bank_hash, producer_time_nanos } => {
                        fast.footers += 1;
                        let _ = self.footer_tx.send(ProtoFooter {
                            slot,
                            bank_hash: bank_hash.to_bytes().to_vec(),
                            producer_time_nanos,
                        });
                    }
                    Event::UndecodableBatch { slot, first_index, last_index } => {
                        eprintln!("[fast] slot {slot}: shreds {first_index}..={last_index} did not decode as a BlockComponent");
                    }
                }
            }
            if let Some((s, e)) = pending.take() {
                self.publish(s, e, false);
                fast.published += 1;
            }
            fast.events = events;
            if fast.last_log.elapsed() >= Duration::from_secs(10) {
                eprintln!(
                    "[fast] shreds={} published={} footers={} tip={} assembler_slots={} \
                     sigverify[ok={} rejected={} dropped_no_leader={}]",
                    fast.total,
                    fast.published,
                    fast.footers,
                    tip,
                    fast.deshredder.active_slots(),
                    self.sv_verified.load(Ordering::Relaxed),
                    self.sv_rejected.load(Ordering::Relaxed),
                    self.sv_no_leader.load(Ordering::Relaxed),
                );
                fast.last_log = Instant::now();
            }
        }
        true
    }

    /// A targeted slot reached `is_full()` in the Blockstore. Emit it as ONE
    /// whole-slot message — unless the fast lane already streamed the slot to
    /// completion, in which case subscribers hold every batch and a re-send
    /// would duplicate entries.
    fn on_complete_block(&self, slot: u64, entries: Vec<Entry>) {
        self.meter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .mark_complete(slot, Instant::now());

        if let Ok(mut guard) = self.oracle.lock() {
            if let Some(sampler) = guard.as_mut() {
                let sigs: HashSet<String> = entries
                    .iter()
                    .flat_map(|e| &e.transactions)
                    .filter_map(|tx| tx.signatures.first())
                    .map(|s| s.to_string())
                    .collect();
                sampler.on_complete(slot, &sigs);
            }
        }

        let fast_done = self
            .fast_complete
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&slot);
        if fast_done {
            return;
        }

        if let Ok(bytes) = wincode::serialize(&entries) {
            let _ = self.entry_tx.send(ProtoEntry { slot, entries: bytes, complete: true });
        }
        self.emit_txs(slot, &entries, true);
    }

    /// The slot was never produced. Emit an EMPTY Entry message so an in-order
    /// replay consumer advances past it instead of stalling; count it as
    /// accounted-for in coverage.
    fn on_slot_skipped(&self, slot: u64) {
        self.meter
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .mark_complete(slot, Instant::now());
        self.fast_complete
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&slot);
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
    let cfg = parse_args().map_err(|e| anyhow::anyhow!("{e}"))?;

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
    eprintln!("[colibri] grpc port:     {}", cfg.grpc_port);
    eprintln!("[colibri] auth:          {}", if cfg.auth_token.is_some() { "token required" } else { "open (no auth)" });
    eprintln!("[colibri] tier1-fanout:  {}", cfg.tier1_fanout);
    eprintln!("[colibri] depth:         {}  window: {}  top-peers: {}", cfg.depth, cfg.window, cfg.top_peers);

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

    let grpc_addr: SocketAddr = format!("0.0.0.0:{}", cfg.grpc_port).parse()?;
    let grpc_handle = {
        let _guard = rt.enter();
        server::start_grpc_server(
            grpc_addr,
            server::Streams {
                entries: entry_tx.clone(),
                txs:     tx_tx.clone(),
                footers: footer_tx.clone(),
            },
            cfg.auth_token.clone(),
            cfg.tls_cert.clone(),
            cfg.tls_key.clone(),
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

    // ── shred sigverify: leader schedule cache + background fetcher ──────────
    let leader_sched: Arc<Mutex<sigverify::LeaderScheduleCache>> =
        Arc::new(Mutex::new(sigverify::LeaderScheduleCache::new()));

    // Leader-schedule background thread: bootstrap epoch math + current epoch,
    // then service on-demand per-epoch fetch requests. ALL blocking RPCs happen
    // OUTSIDE the cache lock (fetch → then install under a brief lock) so the
    // per-shred ingest lookup never blocks on a network round-trip.
    //
    // Fail-closed startup: the money path drops every shred until epoch math AND
    // the current-epoch schedule are loaded (leader_for_slot → None until then).
    // Epoch rollover: a tip shred whose (new) epoch is not loaded is dropped and
    // its epoch is queued in `pending`; this loop drains `pending` every ~500ms,
    // so the blind window at a boundary is sub-second.
    {
        let sched_bg   = leader_sched.clone();
        let rpc_url_bg = cfg.rpc_url.clone();
        let exit_bg    = exit.clone();
        std::thread::spawn(move || {
            // Force the current-epoch preload on the first iteration.
            let mut last_current_check = Instant::now() - Duration::from_secs(3600);
            loop {
                if exit_bg.load(Ordering::Relaxed) { return; }

                // ── (1) bootstrap epoch math (getEpochSchedule), once ────────
                let math = sched_bg.lock().unwrap_or_else(|e| e.into_inner()).math();
                let math = match math {
                    Some(m) => m,
                    None => match sigverify::fetch_epoch_math(&rpc_url_bg) {
                        Some(m) => {
                            sched_bg.lock().unwrap_or_else(|e| e.into_inner()).install_math(m);
                            eprintln!("[sigverify] epoch math loaded: {m:?}");
                            m
                        }
                        None => {
                            eprintln!("[sigverify] getEpochSchedule failed; retrying in 1s");
                            for _ in 0..5 {
                                if exit_bg.load(Ordering::Relaxed) { return; }
                                sleep(Duration::from_millis(200));
                            }
                            continue;
                        }
                    },
                };

                // ── (2) ensure current epoch loaded (startup + rollover heal) ─
                if last_current_check.elapsed() >= Duration::from_secs(10) {
                    last_current_check = Instant::now();
                    if let Some(abs) = sigverify::fetch_absolute_slot(&rpc_url_bg) {
                        if let Some(cur_epoch) = math.epoch_of(abs) {
                            let need = !sched_bg.lock().unwrap_or_else(|e| e.into_inner())
                                .has_epoch(cur_epoch);
                            if need {
                                if let Some((fs, es, leaders)) =
                                    sigverify::fetch_epoch_leaders(&rpc_url_bg, cur_epoch, &math)
                                {
                                    let mut g = sched_bg.lock().unwrap_or_else(|e| e.into_inner());
                                    g.install_epoch(cur_epoch, fs, es, leaders);
                                    let (a, b) = g.loaded_range();
                                    drop(g);
                                    eprintln!(
                                        "[sigverify] current epoch {cur_epoch} loaded; \
                                         active schedule covers slots [{a}, {b})"
                                    );
                                }
                            }
                        }
                    }
                }

                // ── (3) service on-demand pending epochs (fetch OUTSIDE lock) ─
                let pending = sched_bg.lock().unwrap_or_else(|e| e.into_inner()).take_pending();
                for epoch in pending {
                    if exit_bg.load(Ordering::Relaxed) { return; }
                    if sched_bg.lock().unwrap_or_else(|e| e.into_inner()).has_epoch(epoch) {
                        continue; // already loaded via another path
                    }
                    match sigverify::fetch_epoch_leaders(&rpc_url_bg, epoch, &math) {
                        Some((fs, es, leaders)) => {
                            let mut g = sched_bg.lock().unwrap_or_else(|e| e.into_inner());
                            g.install_epoch(epoch, fs, es, leaders);
                            drop(g);
                            eprintln!("[sigverify] on-demand epoch {epoch} loaded [{fs}, {es})");
                        }
                        None => {
                            // RPC has no schedule for this epoch (deep historical):
                            // stay fail-closed — its shreds keep being dropped. A
                            // fresh ingest miss re-queues it if it becomes relevant.
                            eprintln!(
                                "[sigverify] on-demand epoch {epoch} unavailable — \
                                 shreds for it stay DROPPED (fail-closed)"
                            );
                        }
                    }
                }

                for _ in 0..5 {
                    if exit_bg.load(Ordering::Relaxed) { return; }
                    sleep(Duration::from_millis(100));
                }
            }
        });
    }

    let meter: Arc<Mutex<coverage::CoverageMeter>> =
        Arc::new(Mutex::new(coverage::CoverageMeter::new()));

    // Oracle sampler (inert unless --oracle-rpc is passed).
    let oracle_sampler: Arc<Mutex<Option<oracle::OracleSampler>>> =
        Arc::new(Mutex::new(cfg.oracle_rpc.as_ref().map(|url| {
            eprintln!("[oracle] enabled — RPC oracle cross-check on {url}");
            oracle::OracleSampler::new(url.clone())
        })));

    let sink = Arc::new(ColibriSink {
        fast: Mutex::new(FastLane {
            deshredder: Deshredder::new(),
            events:     Vec::new(),
            footers:    0,
            last_log:   Instant::now(),
            total:      0,
            published:  0,
        }),
        leader_sched:  leader_sched.clone(),
        sv_verified:   AtomicU64::new(0),
        sv_rejected:   AtomicU64::new(0),
        sv_no_leader:  AtomicU64::new(0),
        entry_tx,
        tx_tx,
        footer_tx,
        meter:         meter.clone(),
        oracle:        oracle_sampler.clone(),
        fast_complete: Mutex::new(HashSet::new()),
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
            // Retain enough Blockstore history that a slow-completing backlog
            // slot near the consumer's floor isn't janitor-purged before it
            // finishes (the gap can be larger than --depth when driven by
            // from-slot).
            keep_window:   cfg.depth.max(30_000) + 2_000,
            target_window: cfg.window,
            top_peers:     cfg.top_peers,
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
        if let Ok(sampler_guard) = oracle_sampler.lock() {
            if let Some(sampler) = sampler_guard.as_ref() {
                eprintln!("  {}", sampler.report_line());
            }
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
