//! Colibri: Solana gossip participant that receives shreds via TVU, assembles
//! entries inline (no UDP hop), and publishes them via a Jito-compatible gRPC
//! SubscribeEntries stream.
//!
//! Architecture:
//!   - GossipService (solana threads) — keeps us in the gossip network
//!   - TVU thread — receives raw shred packets, calls deshredder inline,
//!                  broadcasts ProtoEntry via broadcast channel to gRPC clients
//!   - Repair thread — requests missing shreds from Tier-1 validators
//!   - Tokio task (gRPC server) — streams entries to subscribers
//!
//! # Data reliability vs gist approach
//!
//! The gossip-writer gist (unordered-set/18a903da0237c4103f158ef97144d4aa) has:
//! - No repair protocol → one lost UDP packet = permanent gap
//! - No FEC recovery → one lost shred in an FEC set = missed entry group
//! - Sequential shred processing → burst packets overflow OS recv buffer
//!
//! Colibri improvements:
//! - FEC recovery via solana-ledger merkle::recover (one erasure per FEC set tolerated)
//! - Tier-1 repair protocol with stake-weighted peer scoring
//! - Repair responses fed back to deshredder (Task 8 fix)
//! - 32MB UDP recv buffer (Task 10)

#![allow(deprecated)]

mod coverage;
mod oracle;
mod server;
mod repair_wire;
mod sigverify;

use {
    anyhow::Result,
    deshredder::Deshredder,
    serde_json::Value,
    server::{ProtoEntry, ProtoTransaction},
    solana_gossip::{
        cluster_info::ClusterInfo,
        contact_info::{ContactInfo, Protocol},
        gossip_service::GossipService,
    },
    solana_hash::Hash,
    solana_keypair::{read_keypair_file, write_keypair_file, Keypair},
    solana_net_utils::bind_in_range,
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    solana_streamer::socket::SocketAddrSpace,
    solana_time_utils::timestamp,
    std::{
        collections::{HashMap, HashSet},
        env,
        io::{Read as IoRead, Write as IoWrite},
        net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs, UdpSocket},
        str::FromStr,
        collections::VecDeque,
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
const TIER1_REFRESH_SECS: u64 = 600;


fn print_usage() {
    eprintln!("Usage: colibri [OPTIONS]");
    eprintln!();
    eprintln!("Options:");
    eprintln!("  --ip <IP>               Public IP to advertise in gossip (required)");
    eprintln!("  --port <PORT>           Gossip UDP port (default: 8000)");
    eprintln!("  --tvu-port <PORT>       TVU port where shreds arrive (default: 8200)");
    eprintln!("  --repair-port <PORT>    UDP port for repair responses (default: 8210)");
    eprintln!("  --entrypoint <ADDR>     Solana entrypoint (repeatable)");
    eprintln!("  --shred-version <VER>   Shred version (default: 50093 = mainnet-beta)");
    eprintln!("  --rpc <URL>             RPC endpoint for stake data");
    eprintln!("  --tier1-fanout <N>      Tier-1 size (default: 200)");
    eprintln!("  --grpc-port <PORT>      gRPC listen port (default: 8888)");
    eprintln!("  --auth-token <TOKEN>    Bearer token required for gRPC subscribers (optional)");
    eprintln!("  --tls-cert <PATH>       TLS certificate PEM (enables TLS when combined with --tls-key)");
    eprintln!("  --tls-key <PATH>        TLS private key PEM");
    eprintln!("  --keypair <PATH>        Path to keypair JSON file (load or auto-create for stable gossip identity)");
    eprintln!("  --probe-depth <N>       Slots back from tip to start coverage range (default: 6000)");
    eprintln!("  --probe-window-max <N>  Maximum depth for repair-window probe (default: 50000)");
    eprintln!("  --oracle-rpc <URL>      Enable RPC oracle cross-check (samples 1-in-50 completed slots via getBlock)");
    eprintln!("  --help                  Print this help");
}

struct Config {
    ip:           IpAddr,
    port:         u16,
    tvu_port:     u16,
    repair_port:  u16,
    entrypoints:  Vec<String>,
    shred_version: u16,
    rpc_url:      String,
    tier1_fanout: usize,
    grpc_port:    u16,
    auth_token:   Option<String>,
    tls_cert:         Option<String>,  // path to PEM certificate
    tls_key:          Option<String>,  // path to PEM private key
    keypair_path:     Option<String>,
    probe_depth:      u64,
    probe_window_max: u64,
    oracle_rpc:       Option<String>,
}

fn parse_args() -> Result<Config, Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mut ip: Option<IpAddr> = None;
    let mut port: u16 = 8000;
    let mut tvu_port: u16 = 8200;
    let mut repair_port: u16 = 8210;
    let mut entrypoints: Vec<String> = Vec::new();
    let mut shred_version: u16 = 50093;
    let mut rpc_url = "http://api.mainnet-beta.solana.com".to_string();
    let mut tier1_fanout: usize = TURBINE_FANOUT;
    let mut grpc_port: u16 = 8888;
    let mut auth_token: Option<String> = None;
    let mut tls_cert: Option<String> = None;
    let mut tls_key:  Option<String> = None;
    let mut keypair_path: Option<String> = None;
    let mut probe_depth: u64      = 6000;
    let mut probe_window_max: u64 = 50000;
    let mut oracle_rpc: Option<String> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--ip"            => { i += 1; ip             = Some(args[i].parse()?); }
            "--port"          => { i += 1; port           = args[i].parse()?; }
            "--tvu-port"      => { i += 1; tvu_port       = args[i].parse()?; }
            "--repair-port"   => { i += 1; repair_port    = args[i].parse()?; }
            "--entrypoint"    => { i += 1; entrypoints.push(args[i].clone()); }
            "--shred-version" => { i += 1; shred_version  = args[i].parse()?; }
            "--rpc"           => { i += 1; rpc_url        = args[i].clone(); }
            "--tier1-fanout"  => { i += 1; tier1_fanout   = args[i].parse()?; }
            "--grpc-port"     => { i += 1; grpc_port      = args[i].parse()?; }
            "--auth-token"    => { i += 1; auth_token     = Some(args[i].clone()); }
            "--tls-cert"      => { i += 1; tls_cert       = Some(args[i].clone()); }
            "--tls-key"       => { i += 1; tls_key        = Some(args[i].clone()); }
            "--keypair"          => { i += 1; keypair_path      = Some(args[i].clone()); }
            "--probe-depth"      => { i += 1; probe_depth       = args[i].parse()?; }
            "--probe-window-max" => { i += 1; probe_window_max  = args[i].parse()?; }
            "--oracle-rpc"       => { i += 1; oracle_rpc        = Some(args[i].clone()); }
            "--help" | "-h"  => { print_usage(); std::process::exit(0); }
            other => {
                eprintln!("Unknown argument: {other}");
                print_usage();
                std::process::exit(1);
            }
        }
        i += 1;
    }

    let ip = ip.ok_or("--ip <PUBLIC_IP> is required")?;
    Ok(Config {
        ip, port, tvu_port, repair_port, entrypoints, shred_version,
        rpc_url, tier1_fanout, grpc_port, auth_token, tls_cert, tls_key,
        keypair_path, probe_depth, probe_window_max, oracle_rpc,
    })
}

pub(crate) fn rpc_post(url: &str, body: &str) -> Option<String> {
    let without_scheme = url.strip_prefix("http://").unwrap_or(url);
    let (host_port, path) = match without_scheme.find('/') {
        Some(i) => (&without_scheme[..i], &without_scheme[i..]),
        None    => (without_scheme, "/"),
    };
    let (host, port): (&str, u16) = match host_port.rsplit_once(':') {
        Some((h, p)) => (h, p.parse().unwrap_or(80)),
        None         => (host_port, 80),
    };

    let req = format!(
        "POST {path} HTTP/1.0\r\nHost: {host}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );

    let mut stream = std::net::TcpStream::connect((host, port)).ok()?;
    stream.set_read_timeout(Some(Duration::from_secs(20))).ok()?;
    stream.write_all(req.as_bytes()).ok()?;

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).ok()?;
    let text = String::from_utf8_lossy(&raw);
    let body_start = text.find("\r\n\r\n")? + 4;
    Some(text[body_start..].to_string())
}

fn score_peer(wallclock_ms: u64, now_ms: u64, stake_lamports: u64) -> f64 {
    let age_s        = (now_ms.saturating_sub(wallclock_ms)) as f64 / 1_000.0;
    let time_weight  = 1.0 / (1.0 + age_s);
    let stake_weight = ((stake_lamports + 1) as f64).ln();
    time_weight * stake_weight
}

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

struct ShredInfo {
    slot:         u64,
    index:        u32,
    is_data:      bool,
    last_in_slot: bool,
}

fn parse_shred_header(buf: &[u8]) -> Option<ShredInfo> {
    if buf.len() < 88 {
        return None;
    }
    let variant    = buf[64];
    let slot       = u64::from_le_bytes(buf[65..73].try_into().ok()?);
    let index      = u32::from_le_bytes(buf[73..77].try_into().ok()?);
    let is_data = match variant & 0xF0 {
        0x80 | 0x90 | 0xB0 => true,
        _ => variant == 0xA5,
    };
    // buf[85] is the DataShredHeader flags byte. Per agave `ledger/src/shred.rs`
    // ShredFlags: DATA_COMPLETE_SHRED = 0b0100_0000 (0x40) marks an entry-BATCH
    // boundary, while LAST_SHRED_IN_SLOT = 0b1100_0000 (0xC0) marks the slot's
    // final shred and *implies* DATA_COMPLETE_SHRED (both high bits set). Testing
    // only 0x40 fires at every batch boundary, stopping repair at the first batch
    // instead of slot end; require BOTH bits for the slot-end signal.
    let last_in_slot = is_data && variant != 0xA5 && ((buf[85] & 0xC0) == 0xC0);
    Some(ShredInfo { slot, index, is_data, last_in_slot })
}

/// Apply a shred's side effects to `observed_tip` + `repair_map`, returning
/// whether the shred should be pushed to the deshredder (i.e. emitted to the
/// gRPC entry/tx stream).
///
/// Fail-closed gate: when `admitted == false` (signature unverified OR leader
/// unknown) this is a NO-OP and returns `false` — `observed_tip` is NOT advanced,
/// `repair_map`/coverage is NOT seeded, and the caller must NOT push/emit. This is
/// what keeps forged and unknown-leader shreds off the money path and prevents
/// them from poisoning the tip or coverage accounting.
fn gate_socket_shred(
    admitted:     bool,
    info:         &ShredInfo,
    observed_tip: &AtomicU64,
    repair_map:   &Mutex<HashMap<u64, SlotRepairState>>,
) -> bool {
    if !admitted {
        return false;
    }
    observed_tip.fetch_max(info.slot, Ordering::Relaxed);
    if info.is_data {
        if let Ok(mut map) = repair_map.try_lock() {
            let state = map.entry(info.slot).or_insert_with(SlotRepairState::new);
            state.have.insert(info.index);
            if info.last_in_slot {
                state.last_index = Some(info.index);
            }
        }
    }
    true
}

struct SlotRepairState {
    have:            HashSet<u32>,
    last_index:      Option<u32>,
    first_seen:      Instant,
    last_repair:     Instant,
    repair_rounds:   u32,
    /// True when this slot was proactively targeted (not just turbine-observed).
    /// Targeted slots use `target_deadline` instead of the 2-second turbine eviction.
    targeted:        bool,
    /// True after we have sent at least one HighestWindowIndex probe for this slot.
    highest_probed:  bool,
    /// Absolute deadline for targeted slots (30s from insertion).
    target_deadline: Instant,
}

impl SlotRepairState {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            have: HashSet::new(),
            last_index: None,
            first_seen: now,
            last_repair: now - Duration::from_secs(1),
            repair_rounds: 0,
            targeted: false,
            highest_probed: false,
            target_deadline: now + Duration::from_secs(30),
        }
    }

    fn new_targeted() -> Self {
        let mut s = Self::new();
        s.targeted = true;
        s
    }

    /// Turbine-observed slots evict after 2 s; targeted slots live until
    /// `is_full()` returns true or the 30-second deadline expires.
    fn is_done(&self) -> bool {
        if self.targeted {
            self.is_full() || Instant::now() >= self.target_deadline
        } else {
            self.first_seen.elapsed() > Duration::from_secs(2)
        }
    }

    /// True when every shred index 0..=last_index is present in `have`.
    fn is_full(&self) -> bool {
        self.last_index
            .map_or(false, |l| (0..=l).all(|i| self.have.contains(&i)))
    }
}

// ─── per-slot repair decision ────────────────────────────────────────────────

/// Actions the repair driver may take for a single slot in one cycle.
#[derive(Debug, PartialEq)]
enum RepairAction {
    /// Send `HighestWindowIndex(slot, 0)` to discover `last_index`.
    ProbeHighest,
    /// Send `WindowIndex(slot, i)` for each missing index.
    RequestWindows(Vec<u32>),
    /// Send `Orphan(slot)` — fallback when `last_index` remains unknown after
    /// several rounds.
    RequestOrphan,
    /// Slot is fully assembled — remove from map.
    Complete,
    /// Nothing to do this round (waiting for HighestWindowIndex response).
    Wait,
}

/// Pure, unit-testable repair-decision function.
///
/// # Arguments
/// * `have`          — shred indices already received.
/// * `last_index`    — highest shred index in the slot, or `None` if unknown.
/// * `highest_probed`— whether we have already sent a `HighestWindowIndex`.
/// * `repair_rounds` — how many repair cycles have elapsed for this slot.
/// * `orphan_after`  — number of rounds after which we fall back to `Orphan`.
fn next_repair_action(
    have:           &HashSet<u32>,
    last_index:     Option<u32>,
    highest_probed: bool,
    repair_rounds:  u32,
    orphan_after:   u32,
) -> RepairAction {
    // Already complete?
    if last_index.map_or(false, |l| (0..=l).all(|i| have.contains(&i))) {
        return RepairAction::Complete;
    }

    match last_index {
        None if !highest_probed => RepairAction::ProbeHighest,
        None if repair_rounds >= orphan_after => RepairAction::RequestOrphan,
        None => RepairAction::Wait,
        Some(last) => {
            let missing: Vec<u32> = (0..=last)
                .filter(|i| !have.contains(i))
                .take(128)
                .collect();
            if missing.is_empty() {
                RepairAction::Complete
            } else {
                RepairAction::RequestWindows(missing)
            }
        }
    }
}

// ─── unit tests for next_repair_action ──────────────────────────────────────

#[cfg(test)]
mod repair_action_tests {
    use super::*;

    fn empty() -> HashSet<u32> { HashSet::new() }
    fn have(indices: &[u32]) -> HashSet<u32> { indices.iter().copied().collect() }

    #[test]
    fn probe_highest_when_no_last_index_and_not_probed() {
        let action = next_repair_action(&empty(), None, false, 0, 5);
        assert_eq!(action, RepairAction::ProbeHighest);
    }

    #[test]
    fn wait_after_probe_until_orphan_threshold() {
        // probed but rounds < orphan_after → Wait
        assert_eq!(
            next_repair_action(&empty(), None, true, 3, 5),
            RepairAction::Wait,
        );
    }

    #[test]
    fn request_orphan_after_threshold() {
        let action = next_repair_action(&empty(), None, true, 5, 5);
        assert_eq!(action, RepairAction::RequestOrphan);
    }

    #[test]
    fn complete_when_all_shreds_present() {
        let full = have(&[0, 1, 2]);
        let action = next_repair_action(&full, Some(2), false, 0, 5);
        assert_eq!(action, RepairAction::Complete);
    }

    #[test]
    fn request_windows_for_gaps() {
        let partial = have(&[0, 2]); // missing index 1
        let action = next_repair_action(&partial, Some(2), false, 0, 5);
        assert_eq!(action, RepairAction::RequestWindows(vec![1]));
    }

    #[test]
    fn request_windows_capped_at_128() {
        // last_index = 200 → 201 shreds needed; we have none → should return ≤128
        let action = next_repair_action(&empty(), Some(200), false, 0, 5);
        match action {
            RepairAction::RequestWindows(v) => assert_eq!(v.len(), 128),
            other => panic!("expected RequestWindows, got {other:?}"),
        }
    }
}

// ─── unit tests for parse_shred_header last_in_slot (agave ShredFlags) ────────

#[cfg(test)]
mod shred_flag_tests {
    use super::*;

    /// Build a minimal 88-byte Merkle data-shred header with the given flags byte.
    /// variant 0x80 = MerkleData (is_data && != 0xA5), slot/index arbitrary.
    fn header_with_flags(flags: u8) -> [u8; 88] {
        let mut buf = [0u8; 88];
        buf[64] = 0x80; // ShredVariant::MerkleData
        buf[65..73].copy_from_slice(&123u64.to_le_bytes()); // slot
        buf[73..77].copy_from_slice(&7u32.to_le_bytes());   // index
        buf[85] = flags;
        buf
    }

    #[test]
    fn data_complete_alone_is_not_last_in_slot() {
        // 0x40 = DATA_COMPLETE_SHRED only → entry-batch boundary, NOT slot end.
        let buf = header_with_flags(0x40);
        let info = parse_shred_header(&buf).expect("parses");
        assert!(info.is_data);
        assert!(!info.last_in_slot, "DATA_COMPLETE_SHRED alone must not signal slot end");
    }

    #[test]
    fn last_shred_in_slot_requires_both_bits() {
        // 0xC0 = LAST_SHRED_IN_SLOT (implies DATA_COMPLETE_SHRED) → slot end.
        let buf = header_with_flags(0xC0);
        let info = parse_shred_header(&buf).expect("parses");
        assert!(info.last_in_slot, "0xC0 must signal LAST_SHRED_IN_SLOT");
    }

    #[test]
    fn reference_tick_bits_do_not_trigger_last_in_slot() {
        // Low 6 bits are SHRED_TICK_REFERENCE_MASK; a high-tick value with only
        // DATA_COMPLETE set (0x40 | 0x2A) must still not be slot end.
        let buf = header_with_flags(0x40 | 0x2A);
        let info = parse_shred_header(&buf).expect("parses");
        assert!(!info.last_in_slot);
        // But 0xC0 | tick bits IS slot end.
        let buf2 = header_with_flags(0xC0 | 0x15);
        assert!(parse_shred_header(&buf2).unwrap().last_in_slot);
    }
}


// ─── unit tests for the fail-closed ingest gate (gate_socket_shred) ──────────

#[cfg(test)]
mod ingest_gate_tests {
    use super::*;

    fn data_shred(slot: u64, index: u32, last: bool) -> ShredInfo {
        ShredInfo { slot, index, is_data: true, last_in_slot: last }
    }

    #[test]
    fn unverified_shred_is_dropped_and_does_not_advance_tip_or_coverage() {
        // Mirrors an unknown-leader / forged shred: admit() returned false.
        let tip = AtomicU64::new(100);
        let map: Mutex<HashMap<u64, SlotRepairState>> = Mutex::new(HashMap::new());
        let info = data_shred(5_000, 7, true);

        let emit = gate_socket_shred(false, &info, &tip, &map);

        // Not emitted (caller pushes to deshredder/entry stream only if `emit`).
        assert!(!emit, "unverified shred must NOT be pushed to the entry stream");
        // Tip did not advance from the forged shred.
        assert_eq!(tip.load(Ordering::Relaxed), 100, "forged shred must not move observed_tip");
        // Coverage (repair_map) untouched → cannot mark the slot complete.
        assert!(map.lock().unwrap().is_empty(), "forged shred must not seed coverage");
    }

    #[test]
    fn verified_shred_advances_tip_and_seeds_coverage_and_emits() {
        let tip = AtomicU64::new(100);
        let map: Mutex<HashMap<u64, SlotRepairState>> = Mutex::new(HashMap::new());
        let info = data_shred(5_000, 7, true);

        let emit = gate_socket_shred(true, &info, &tip, &map);

        assert!(emit, "verified shred is pushed to the entry stream");
        assert_eq!(tip.load(Ordering::Relaxed), 5_000, "verified shred advances observed_tip");
        let guard = map.lock().unwrap();
        let state = guard.get(&5_000).expect("slot seeded in repair_map");
        assert!(state.have.contains(&7));
        assert_eq!(state.last_index, Some(7));
    }

    #[test]
    fn verified_but_older_shred_does_not_regress_tip() {
        let tip = AtomicU64::new(9_000);
        let map: Mutex<HashMap<u64, SlotRepairState>> = Mutex::new(HashMap::new());
        let info = data_shred(5_000, 0, false);
        gate_socket_shred(true, &info, &tip, &map);
        assert_eq!(tip.load(Ordering::Relaxed), 9_000, "fetch_max never regresses the tip");
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
    eprintln!("[colibri] advertise ip:  {}", cfg.ip);
    eprintln!("[colibri] rpc:           {}", cfg.rpc_url);
    eprintln!("[colibri] grpc port:     {}", cfg.grpc_port);
    eprintln!("[colibri] auth:          {}", if cfg.auth_token.is_some() { "token required" } else { "open (no auth)" });
    eprintln!("[colibri] tier1-fanout:  {}", cfg.tier1_fanout);

    // Bind sockets on 0.0.0.0 (all interfaces) but advertise the public --ip.
    // Binding to the specific DHCP-assigned public IP fails to receive the
    // entrypoint's gossip replies on some hosts (e.g. OVH); 0.0.0.0 is the
    // canonical agave pattern (matches Lumen's snapshot-fetch, which works).
    let bind_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    let (gport, gossip_socket) =
        bind_in_range(bind_ip, (cfg.port, cfg.port + 1))?;
    let gossip_addr = SocketAddr::new(cfg.ip, gport);
    eprintln!("[colibri] gossip addr:   {gossip_addr} (socket bound on 0.0.0.0:{gport})");
    eprintln!("[diag] gossip socket local_addr: {:?}", gossip_socket.local_addr());

    let (rpc_port, _rpc_socket) =
        bind_in_range(bind_ip, (cfg.port + 1000, cfg.port + 1100))?;
    let (tpu_port, _tpu_socket) =
        bind_in_range(bind_ip, (cfg.port + 1100, cfg.port + 1200))?;

    let tvu_socket =
        UdpSocket::bind(SocketAddr::new(bind_ip, cfg.tvu_port))?;
    tvu_socket.set_read_timeout(Some(Duration::from_millis(10)))?;
    {
        #[cfg(unix)]
        {
            use std::os::unix::io::{AsRawFd, FromRawFd, IntoRawFd};
            let sock2 = unsafe {
                socket2::Socket::from_raw_fd(tvu_socket.as_raw_fd())
            };
            sock2.set_recv_buffer_size(32 * 1024 * 1024).ok();
            // Prevent socket2 from closing the fd when dropped
            let _ = sock2.into_raw_fd();
        }
    }
    let tvu_addr = SocketAddr::new(cfg.ip, cfg.tvu_port);
    eprintln!("[colibri] tvu addr:      {tvu_addr}");
    eprintln!("[colibri] tvu recv buffer: 32MB (sysctl net.core.rmem_max=134217728 for full effect)");

    let repair_addr = SocketAddr::new(cfg.ip, cfg.repair_port);
    eprintln!("[colibri] repair addr:   {repair_addr}");

    let shred_version: u16 = {
        let fetched = cfg.entrypoints.first().and_then(|ep| {
            let addr = ep.to_socket_addrs().ok()?.next()?;
            solana_net_utils::get_cluster_shred_version(&addr).ok()
        });
        match fetched {
            Some(v) => {
                eprintln!("[colibri] shred version:  {v} (fetched from entrypoint)");
                v
            }
            None => {
                eprintln!(
                    "[colibri] shred version:  {} (fallback — entrypoint probe failed or no entrypoints given)",
                    cfg.shred_version
                );
                cfg.shred_version
            }
        }
    };

    // Advertise ONLY gossip + tvu. colibri serves no RPC/TPU, and as a repair
    // REQUESTER it receives shred responses on its repair socket (the source of
    // its requests), so it need not advertise serve_repair. Advertising sockets
    // it doesn't serve makes peers' ContactInfo sanitize reject the pull,
    // silently dropping it — which stalled gossip bootstrap entirely.
    let mut ci = ContactInfo::new(keypair.pubkey(), timestamp(), shred_version);
    ci.set_gossip(gossip_addr)?;
    ci.set_tvu(Protocol::UDP, tvu_addr)?;
    let _ = (rpc_port, tpu_port, repair_addr); // bound/advertised-elsewhere; not in ContactInfo

    let cluster_info = Arc::new(ClusterInfo::new(
        ci, keypair.clone(), SocketAddrSpace::Unspecified,
    ));
    cluster_info.push_snapshot_hashes((0, Hash::default()), vec![]).ok();

    for ep in &cfg.entrypoints {
        match ep.to_socket_addrs() {
            Ok(mut it) => {
                if let Some(addr) = it.next() {
                    cluster_info.set_entrypoint(ContactInfo::new_gossip_entry_point(&addr));
                    eprintln!("[colibri] entrypoint:    {addr}");
                }
            }
            Err(e) => eprintln!("[colibri] WARNING: cannot resolve {ep}: {e}"),
        }
    }
    if cfg.entrypoints.is_empty() {
        eprintln!("[colibri] WARNING: no entrypoints — isolated mode");
    }

    if let Some(ep) = cfg.entrypoints.first() {
        if let Ok(mut addrs) = ep.to_socket_addrs() {
            if let Some(ep_addr) = addrs.next() {
                eprintln!("[diag] pre-flight: testing UDP outbound to {ep_addr} from gossip socket...");
                match gossip_socket.try_clone() {
                    Ok(clone) => {
                        match clone.send_to(b"colibri-preflight", &ep_addr) {
                            Ok(n) => eprintln!("[diag] pre-flight: gossip socket send_to OK ({n} bytes)"),
                            Err(e) => eprintln!("[diag] pre-flight: gossip socket send_to FAILED: {e}"),
                        }
                        clone.set_read_timeout(Some(Duration::from_millis(500))).ok();
                        let mut rbuf = [0u8; 1500];
                        match clone.recv_from(&mut rbuf) {
                            Ok((n, src)) => eprintln!("[diag] pre-flight: gossip socket got {n} bytes from {src}"),
                            Err(e) => eprintln!("[diag] pre-flight: gossip socket recv timeout/err: {e} (expected)"),
                        }
                        drop(clone);
                    }
                    Err(e) => eprintln!("[diag] pre-flight: try_clone FAILED: {e}"),
                }

                eprintln!("[diag] pre-flight: testing inbound on gossip socket clone...");
                match gossip_socket.try_clone() {
                    Ok(clone) => {
                        clone.set_read_timeout(Some(Duration::from_millis(500))).ok();
                        let mut rbuf = [0u8; 1500];
                        match clone.recv_from(&mut rbuf) {
                            Ok((n, src)) => eprintln!("[diag] pre-flight: inbound data! {n} bytes from {src}"),
                            Err(e) => eprintln!("[diag] pre-flight: no inbound data: {e} (normal before gossip starts)"),
                        }
                        drop(clone);
                    }
                    Err(e) => eprintln!("[diag] pre-flight: inbound clone FAILED: {e}"),
                }

                eprintln!("[diag] pre-flight: testing ephemeral socket to {ep_addr}...");
                match UdpSocket::bind("0.0.0.0:0") {
                    Ok(eph) => {
                        eprintln!("[diag] pre-flight: ephemeral bound on {:?}", eph.local_addr());
                        match eph.send_to(b"colibri-preflight-eph", &ep_addr) {
                            Ok(n) => eprintln!("[diag] pre-flight: ephemeral send_to OK ({n} bytes)"),
                            Err(e) => eprintln!("[diag] pre-flight: ephemeral send_to FAILED: {e}"),
                        }
                        eph.set_read_timeout(Some(Duration::from_millis(500))).ok();
                        let mut rbuf = [0u8; 1500];
                        match eph.recv_from(&mut rbuf) {
                            Ok((n, src)) => eprintln!("[diag] pre-flight: ephemeral got {n} bytes from {src}"),
                            Err(e) => eprintln!("[diag] pre-flight: ephemeral recv timeout/err: {e} (expected)"),
                        }
                    }
                    Err(e) => eprintln!("[diag] pre-flight: ephemeral bind FAILED: {e}"),
                }
                eprintln!("[diag] pre-flight tests complete");
            }
        }
    }

    let exit = Arc::new(AtomicBool::new(false));

    let _gossip_service = GossipService::new(
        &cluster_info, None, Arc::from([gossip_socket]), None, true, None, exit.clone(),
    );
    eprintln!("[colibri] GossipService running");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;

    {
        let exit_ctrlc = exit.clone();
        rt.spawn(async move {
            tokio::signal::ctrl_c().await.ok();
            eprintln!("\n[colibri] Ctrl-C received, shutting down...");
            exit_ctrlc.store(true, std::sync::atomic::Ordering::SeqCst);
        });
    }

    let tier1_pubkeys: Arc<Mutex<Vec<(u64, [u8; 32])>>> = Arc::new(Mutex::new(Vec::new()));

    {
        eprintln!("[tier1] initial stake fetch…");
        let initial = fetch_tier1(&cfg.rpc_url, cfg.tier1_fanout);
        if initial.is_empty() {
            eprintln!("[tier1] WARNING: initial fetch returned 0 validators");
        }
        *tier1_pubkeys.lock().unwrap() = initial;
    }

    {
        let tier1_refresh = tier1_pubkeys.clone();
        let rpc_url_c = cfg.rpc_url.clone();
        let fanout    = cfg.tier1_fanout;
        let exit_t1   = exit.clone();
        std::thread::spawn(move || loop {
            sleep(Duration::from_secs(TIER1_REFRESH_SECS));
            if exit_t1.load(Ordering::Relaxed) { break; }
            let updated = fetch_tier1(&rpc_url_c, fanout);
            if !updated.is_empty() {
                *tier1_refresh.lock().unwrap() = updated;
            }
        });
    }

    // ── shred sigverify: leader schedule + counters ──────────────────────────
    // Leader-keyed shred signature verification (agave verify_shred_cpu). The
    // cache holds per-epoch schedules (fetched on demand, keyed by epoch); the
    // TVU thread verifies every shred against its slot leader before feeding the
    // deshredder. Policy is fail-closed for BOTH branches (agave-faithful):
    //   * leader known  → verify; DROP on signature failure.
    //   * leader unknown → DROP; record the epoch for background on-demand fetch.
    // The money path (deshredder → gRPC entry+tx stream) never emits an
    // unverified shred; observed_tip and coverage never advance from one.
    let leader_sched: Arc<Mutex<sigverify::LeaderScheduleCache>> =
        Arc::new(Mutex::new(sigverify::LeaderScheduleCache::new()));
    let sv_verified:          Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let sv_rejected:          Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let sv_dropped_no_leader: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));

    // ── measurement harness shared state ─────────────────────────────────────
    let observed_tip: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let meter: Arc<Mutex<coverage::CoverageMeter>> =
        Arc::new(Mutex::new(coverage::CoverageMeter::new()));

    // Atomic counters promoted from repair-thread-local so they survive to report time.
    let arc_pings_seen:  Arc<AtomicU64>  = Arc::new(AtomicU64::new(0));
    let arc_pongs_sent:  Arc<AtomicU64>  = Arc::new(AtomicU64::new(0));
    let arc_pong_errors: Arc<AtomicU64>  = Arc::new(AtomicU64::new(0));
    let arc_responses:   Arc<AtomicU64>  = Arc::new(AtomicU64::new(0));

    // Set of slots that reached genuine is_full() completion (not deadline eviction).
    // Written by the repair thread in the RepairAction::Complete arm (targeted only).
    // Read by the probe coordinator to distinguish success from timeout.
    let completed_set: Arc<Mutex<HashSet<u64>>> = Arc::new(Mutex::new(HashSet::new()));

    // Oracle sampler (inert unless --oracle-rpc is passed).
    // Wraps an OracleSampler; Arc<Mutex<Option<…>>> so the repair thread and
    // TVU thread can share it without knowing whether oracle is enabled.
    let oracle_sampler: Arc<Mutex<Option<oracle::OracleSampler>>> =
        Arc::new(Mutex::new(cfg.oracle_rpc.as_ref().map(|url| {
            eprintln!("[oracle] enabled — RPC oracle cross-check on {url}");
            oracle::OracleSampler::new(url.clone())
        })));

    // Per-slot signature accumulator for oracle sampling.
    // The TVU/repair deshredder emits ProtoTransaction per tx; we record
    // signatures here for any slot that is currently in `repair_map` as targeted.
    // Keyed by slot; populated by the TVU thread; consumed (and removed) in the
    // repair thread's Complete arm.
    let oracle_sig_map: Arc<Mutex<HashMap<u64, HashSet<String>>>> =
        Arc::new(Mutex::new(HashMap::new()));

    eprintln!("[colibri] probe-depth:      {}", cfg.probe_depth);
    eprintln!("[colibri] probe-window-max: {}", cfg.probe_window_max);

    let repair_map: Arc<Mutex<HashMap<u64, SlotRepairState>>> =
        Arc::new(Mutex::new(HashMap::new()));

    // Shared queue the measurement harness (Task 5) pushes target slots into.
    // Each cycle the repair thread drains up to 16 new entries into `repair_map`.
    let target_slots: Arc<Mutex<VecDeque<u64>>> =
        Arc::new(Mutex::new(VecDeque::new()));

    let (entry_tx, _) = broadcast::channel::<ProtoEntry>(1_024);
    let entry_tx = Arc::new(entry_tx);
    let (tx_tx, _) = broadcast::channel::<ProtoTransaction>(8_192);
    let tx_tx = Arc::new(tx_tx);

    let (repair_shred_tx, repair_shred_rx) =
        std::sync::mpsc::sync_channel::<Vec<u8>>(4_096);

    if let (Some(cert_path), Some(key_path)) = (&cfg.tls_cert, &cfg.tls_key) {
        std::fs::metadata(cert_path)
            .map_err(|e| anyhow::anyhow!("cannot read TLS cert {cert_path}: {e}"))?;
        std::fs::metadata(key_path)
            .map_err(|e| anyhow::anyhow!("cannot read TLS key {key_path}: {e}"))?;
    }

    let grpc_addr: SocketAddr = format!("0.0.0.0:{}", cfg.grpc_port).parse()?;
    let grpc_handle = {
        let _guard = rt.enter();
        server::start_grpc_server(
            grpc_addr,
            entry_tx.clone(),
            tx_tx.clone(),
            cfg.auth_token.clone(),
            cfg.tls_cert.clone(),
            cfg.tls_key.clone(),
            exit.clone(),
        )
    };
    eprintln!("[colibri] gRPC on {grpc_addr}");
    if cfg.tls_cert.is_some() {
        eprintln!("[colibri] TLS:            enabled");
    } else {
        eprintln!("[colibri] TLS:            disabled (plain gRPC)");
    }

    // Leader-schedule background thread: bootstrap epoch math + current epoch,
    // then service on-demand per-epoch fetch requests. ALL blocking RPCs happen
    // OUTSIDE the cache lock (fetch → then install under a brief lock) so the
    // per-shred ingest lookup never blocks on a network round-trip.
    //
    // Fail-closed startup: the money path drops every shred until epoch math AND
    // the current-epoch schedule are loaded (leader_for_slot → None until then).
    // Epoch rollover: a tip shred whose (new) epoch is not loaded is dropped and
    // its epoch is queued in `pending`; this loop drains `pending` every ~500ms,
    // so the blind window at a boundary is sub-second, not the old 30s.
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

    let repair_map_tvu     = repair_map.clone();
    let exit_tvu           = exit.clone();
    let leader_sched_tvu   = leader_sched.clone();
    let sv_verified_tvu    = sv_verified.clone();
    let sv_rejected_tvu    = sv_rejected.clone();
    let sv_dropped_tvu     = sv_dropped_no_leader.clone();
    let entry_tx_tvu       = entry_tx.clone();
    let tx_tx_tvu          = tx_tx.clone();
    let observed_tip_tvu   = observed_tip.clone();
    let oracle_sig_map_tvu = oracle_sig_map.clone();
    let oracle_enabled_tvu = oracle_sampler.lock().unwrap().is_some();
    let meter_tvu          = meter.clone();
    let completed_set_tvu  = completed_set.clone();
    std::thread::spawn(move || {
        let mut deshredder    = Deshredder::new(30_000, 200);
        let mut buf           = [0u8; 1280];
        let mut total: u64    = 0;
        let mut published: u64 = 0;
        let mut last_log      = Instant::now();
        let mut last_evict    = Instant::now();

        eprintln!("[tvu] ready — inline deshredding, no UDP forward");

        // Leader-keyed shred sigverify gate (agave verify_shred_cpu). Fail-closed
        // for BOTH branches — returns true ONLY for a signature-verified shred:
        //   * leader known  → verify signature over the Merkle root; DROP on
        //                      failure (== agave).
        //   * leader unknown → DROP and count `sv_dropped_no_leader`. The lookup
        //                      also records the missing epoch so the background
        //                      thread fetches its schedule on demand; until then
        //                      the money path emits nothing for that epoch.
        // A false return means: not emitted to the gRPC stream, observed_tip not
        // advanced, repair_map/coverage not touched.
        let admit = |bytes: &[u8], slot: u64| -> bool {
            let leader = leader_sched_tvu.lock().unwrap_or_else(|e| e.into_inner())
                .leader_for_slot(slot);
            match leader {
                Some(pk) => {
                    if sigverify::verify_shred_signature(bytes, &pk) {
                        sv_verified_tvu.fetch_add(1, Ordering::Relaxed);
                        true
                    } else {
                        sv_rejected_tvu.fetch_add(1, Ordering::Relaxed);
                        false
                    }
                }
                None => {
                    sv_dropped_tvu.fetch_add(1, Ordering::Relaxed);
                    false
                }
            }
        };

        loop {
            if exit_tvu.load(Ordering::Relaxed) { break; }

            if last_evict.elapsed() >= Duration::from_millis(100) {
                deshredder.evict_expired();
                last_evict = Instant::now();
            }

            while let Ok(bytes) = repair_shred_rx.try_recv() {
                // Verify repaired shreds against the slot leader before ingest.
                // Fail-closed: an unverified repair response is dropped — it is
                // NOT emitted AND does NOT advance coverage (`repair_map.have`),
                // so a forged repair response cannot mark a slot complete.
                let info = match parse_shred_header(&bytes) {
                    Some(info) => info,
                    None => continue,
                };
                if !admit(&bytes, info.slot) { continue; }
                // Coverage accounting for VERIFIED repaired shreds only. (The
                // repair thread no longer counts unverified responses.) Kept as
                // get_mut so we only credit slots we are actually tracking.
                if info.is_data {
                    if let Ok(mut map) = repair_map_tvu.try_lock() {
                        if let Some(state) = map.get_mut(&info.slot) {
                            state.have.insert(info.index);
                            if info.last_in_slot {
                                state.last_index = Some(info.index);
                            }
                        }
                    }
                }
                if let Some(se) = deshredder.push_raw(&bytes) {
                    if se.complete {
                        completed_set_tvu.lock().unwrap_or_else(|e| e.into_inner()).insert(se.slot);
                        meter_tvu.lock().unwrap_or_else(|e| e.into_inner()).mark_complete(se.slot, Instant::now());
                    }
                    let _ = entry_tx_tvu.send(ProtoEntry {
                        slot:     se.slot,
                        entries:  se.entries_bytes,
                        complete: se.complete,
                    });
                    for entry in &se.entries {
                        for tx in &entry.transactions {
                            let sig = tx.signatures.first()
                                .map(|s| s.to_string()).unwrap_or_default();
                            if oracle_enabled_tvu && !sig.is_empty() {
                                oracle_sig_map_tvu.lock()
                                    .unwrap_or_else(|e| e.into_inner())
                                    .entry(se.slot).or_insert_with(HashSet::new).insert(sig.clone());
                            }
                            if let Ok(raw) = bincode::serialize(tx) {
                                let _ = tx_tx_tvu.send(ProtoTransaction {
                                    slot: se.slot, signature: sig, raw_tx: raw, complete: se.complete,
                                });
                            }
                        }
                    }
                    published += 1;
                }
            }

            match tvu_socket.recv_from(&mut buf) {
                Ok((n, _)) => {
                    total += 1;

                    // Sigverify BEFORE any state update: a forged shred must not
                    // move observed_tip, seed repair_map, or reach the deshredder.
                    // gate_socket_shred applies side effects ONLY when admitted
                    // (verified); an unverified shred is a no-op → not pushed below.
                    let verified = match parse_shred_header(&buf[..n]) {
                        Some(info) => {
                            let ok = admit(&buf[..n], info.slot);
                            gate_socket_shred(ok, &info, &observed_tip_tvu, &repair_map_tvu)
                        }
                        None => false,
                    };

                    if verified { if let Some(se) = deshredder.push_raw(&buf[..n]) {
                        if se.complete {
                            completed_set_tvu.lock().unwrap_or_else(|e| e.into_inner()).insert(se.slot);
                            meter_tvu.lock().unwrap_or_else(|e| e.into_inner()).mark_complete(se.slot, Instant::now());
                        }
                        let _ = entry_tx_tvu.send(ProtoEntry {
                            slot:     se.slot,
                            entries:  se.entries_bytes,
                            complete: se.complete,
                        });
                        for entry in &se.entries {
                            for tx in &entry.transactions {
                                let sig = tx.signatures.first()
                                    .map(|s| s.to_string()).unwrap_or_default();
                                if oracle_enabled_tvu && !sig.is_empty() {
                                    oracle_sig_map_tvu.lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .entry(se.slot).or_insert_with(HashSet::new).insert(sig.clone());
                                }
                                if let Ok(raw) = bincode::serialize(tx) {
                                    let _ = tx_tx_tvu.send(ProtoTransaction {
                                        slot: se.slot, signature: sig, raw_tx: raw, complete: se.complete,
                                    });
                                }
                            }
                        }
                        published += 1;
                    } }

                    if last_log.elapsed() >= Duration::from_secs(10) {
                        let tip = observed_tip_tvu.load(Ordering::Relaxed);
                        let sv_ok  = sv_verified_tvu.load(Ordering::Relaxed);
                        let sv_bad = sv_rejected_tvu.load(Ordering::Relaxed);
                        let sv_unk = sv_dropped_tvu.load(Ordering::Relaxed);
                        eprintln!(
                            "[tvu] shreds={total} published={published} tip={tip} \
                             assembler_slots={} dedup_slots={} \
                             sigverify[ok={sv_ok} rejected={sv_bad} dropped_no_leader={sv_unk}]",
                            deshredder.active_slot_count(),
                            deshredder.tracked_slot_count(),
                        );
                        last_log = Instant::now();
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut => {}
                Err(e) => eprintln!("[tvu] error: {e}"),
            }
        }
    });

    {
        let cluster_repair    = cluster_info.clone();
        let keypair_repair    = keypair.clone();
        let exit_repair       = exit.clone();
        let repair_map_rep    = repair_map.clone();
        let target_slots_rep  = target_slots.clone();
        let tier1_repair      = tier1_pubkeys.clone();
        let repair_port       = cfg.repair_port;
        let repair_ip         = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
        let meter_rep         = meter.clone();
        let arc_pings_rep     = arc_pings_seen.clone();
        let arc_pongs_rep     = arc_pongs_sent.clone();
        let arc_pong_errs_rep = arc_pong_errors.clone();
        let arc_resp_rep      = arc_responses.clone();
        let completed_set_rep  = completed_set.clone();
        let oracle_sampler_rep = oracle_sampler.clone();
        let oracle_sig_map_rep = oracle_sig_map.clone();
        std::thread::spawn(move || {
            let repair_shred_tx = repair_shred_tx;
            let repair_sock = UdpSocket::bind(
                SocketAddr::new(repair_ip, repair_port)
            ).expect("bind repair sock");
            repair_sock.set_read_timeout(Some(Duration::from_millis(5))).ok();
            eprintln!("[repair] bound on {repair_ip}:{repair_port}");

            let mut nonce: u32       = 0xdead_beef;
            let mut total_sent: u64  = 0;
            let mut send_errors: u64 = 0;
            let mut last_log         = Instant::now();
            let mut recv_buf         = [0u8; 1500];

            eprintln!("[repair] thread started (50ms cycle, Tier-1 priority)");

            loop {
                if exit_repair.load(Ordering::Relaxed) { break; }

                // ── drain inbound (ping/pong + shred responses) ──────────────
                let mut drain_count = 0usize;
                while drain_count < 512 {
                    match repair_sock.recv_from(&mut recv_buf) {
                        Ok((n, src)) => {
                            match repair_wire::parse_inbound(&recv_buf[..n]) {
                                repair_wire::Inbound::Ping(token) => {
                                    arc_pings_rep.fetch_add(1, Ordering::Relaxed);
                                    let pong = repair_wire::build_pong(&keypair_repair, token);
                                    match repair_sock.send_to(&pong, src) {
                                        Ok(_) => { arc_pongs_rep.fetch_add(1, Ordering::Relaxed); }
                                        Err(e) => {
                                            arc_pong_errs_rep.fetch_add(1, Ordering::Relaxed);
                                            eprintln!("[repair] pong send_to {src} error: {e}");
                                        }
                                    }
                                }
                                repair_wire::Inbound::ShredResponse => {
                                    arc_resp_rep.fetch_add(1, Ordering::Relaxed);
                                    // Forward to the TVU thread, which sigverifies
                                    // the response and — only if it verifies —
                                    // credits repair_map.have and emits it. Coverage
                                    // accounting is intentionally NOT done here: a
                                    // forged/unverified repair response must not be
                                    // able to mark a slot complete (fail-closed).
                                    let _ = repair_shred_tx.send(recv_buf[..n].to_vec());
                                }
                                repair_wire::Inbound::Other => { /* discard */ }
                            }
                            drain_count += 1;
                        }
                        Err(_) => break,
                    }
                }

                sleep(Duration::from_millis(50));

                // Window: keep at most TARGET_WINDOW targeted slots in flight so we
                // don't thrash thousands at once (which evicts them before their
                // shreds arrive). Completed/timed-out slots free up room each cycle.
                const TARGET_WINDOW: usize = 256;
                if let (Ok(mut map), Ok(mut queue)) =
                    (repair_map_rep.try_lock(), target_slots_rep.try_lock())
                {
                    let active = map.values().filter(|s| s.targeted).count();
                    let mut room = TARGET_WINDOW.saturating_sub(active);
                    while room > 0 {
                        match queue.pop_front() {
                            Some(slot) => {
                                map.entry(slot).or_insert_with(SlotRepairState::new_targeted);
                                room -= 1;
                            }
                            None => break,
                        }
                    }
                }

                // Score all peers, take top 20.
                let tier1: Vec<(u64, [u8; 32])> = tier1_repair.lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clone();
                let stake_map: HashMap<[u8; 32], u64> =
                    tier1.iter().map(|(s, pk)| (*pk, *s)).collect();

                let all_peers = cluster_repair.all_peers();
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let mut scored: Vec<(f64, [u8; 32], SocketAddr)> = all_peers
                    .iter()
                    .filter_map(|(info, wc)| {
                        let addr  = info.serve_repair(Protocol::UDP)?;
                        let pk    = info.pubkey().to_bytes();
                        let stake = stake_map.get(&pk).copied().unwrap_or(0);
                        Some((score_peer(*wc, now_ms, stake), pk, addr))
                    })
                    .collect();
                scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
                let targets: Vec<([u8; 32], SocketAddr)> = scored
                    .into_iter().take(20).map(|(_, pk, addr)| (pk, addr)).collect();

                if targets.is_empty() { continue; }

                // ── per-slot repair dispatch (driven by next_repair_action) ──
                let mut done_slots = Vec::new();
                if let Ok(mut map) = repair_map_rep.try_lock() {
                    let completed_now: std::collections::HashSet<u64> =
                        completed_set_rep.lock().unwrap_or_else(|e| e.into_inner()).clone();
                    for (&slot, state) in map.iter_mut() {
                        if state.is_done() || completed_now.contains(&slot) { done_slots.push(slot); continue; }
                        if state.last_repair.elapsed() < Duration::from_millis(50) { continue; }

                        let action = next_repair_action(
                            &state.have,
                            state.last_index,
                            state.highest_probed,
                            state.repair_rounds,
                            5, // fall back to Orphan after 5 rounds with no last_index
                        );

                        match action {
                            RepairAction::Complete => {
                                if state.targeted {
                                    eprintln!(
                                        "[repair] target slot={slot} COMPLETE indices={}",
                                        state.have.len()
                                    );
                                    meter_rep.lock().unwrap_or_else(|e| e.into_inner())
                                        .mark_complete(slot, Instant::now());
                                    // Record genuine is_full() completion so the probe
                                    // coordinator can distinguish it from a 30s deadline
                                    // eviction (which also removes the slot from repair_map
                                    // but must NOT count as a successful repair).
                                    completed_set_rep.lock()
                                        .unwrap_or_else(|e| e.into_inner())
                                        .insert(slot);
                                    // Oracle cross-check: if enabled, consume accumulated
                                    // signatures for this slot and invoke the sampler.
                                    if let Ok(mut sampler_guard) = oracle_sampler_rep.try_lock() {
                                        if let Some(sampler) = sampler_guard.as_mut() {
                                            let sigs = oracle_sig_map_rep
                                                .lock()
                                                .unwrap_or_else(|e| e.into_inner())
                                                .remove(&slot)
                                                .unwrap_or_default();
                                            sampler.on_complete(slot, &sigs);
                                        }
                                    }
                                }
                                done_slots.push(slot);
                                continue;
                            }
                            RepairAction::Wait => continue,

                            RepairAction::ProbeHighest => {
                                state.last_repair  = Instant::now();
                                state.repair_rounds += 1;
                                state.highest_probed = true;
                                for (pk, addr) in &targets {
                                    let req = repair_wire::highest_window_index(
                                        &keypair_repair, pk, slot, 0, nonce,
                                    );
                                    match repair_sock.send_to(&req, addr) {
                                        Ok(_) => total_sent += 1,
                                        Err(e) => {
                                            send_errors += 1;
                                            if send_errors <= 5 {
                                                eprintln!("[repair] send_to {addr} error: {e}");
                                            }
                                        }
                                    }
                                    nonce = nonce.wrapping_add(1);
                                }
                                eprintln!("[repair] slot={slot} ProbeHighest sent to {} peers", targets.len());
                            }

                            RepairAction::RequestOrphan => {
                                state.last_repair  = Instant::now();
                                state.repair_rounds += 1;
                                for (pk, addr) in &targets {
                                    let req = repair_wire::orphan(
                                        &keypair_repair, pk, slot, nonce,
                                    );
                                    match repair_sock.send_to(&req, addr) {
                                        Ok(_) => total_sent += 1,
                                        Err(e) => {
                                            send_errors += 1;
                                            if send_errors <= 5 {
                                                eprintln!("[repair] send_to {addr} error: {e}");
                                            }
                                        }
                                    }
                                    nonce = nonce.wrapping_add(1);
                                }
                                eprintln!(
                                    "[repair] slot={slot} round={} RequestOrphan sent to {} peers",
                                    state.repair_rounds, targets.len()
                                );
                            }

                            RepairAction::RequestWindows(batch) => {
                                state.last_repair  = Instant::now();
                                state.repair_rounds += 1;

                                let sent_before = total_sent;
                                for &idx in &batch {
                                    for (pk, addr) in &targets {
                                        let req = repair_wire::window_index(
                                            &keypair_repair, pk, slot, idx as u64, nonce,
                                        );
                                        match repair_sock.send_to(&req, addr) {
                                            Ok(_) => total_sent += 1,
                                            Err(e) => {
                                                send_errors += 1;
                                                if send_errors <= 5 {
                                                    eprintln!("[repair] send_to {addr} error: {e}");
                                                }
                                            }
                                        }
                                        nonce = nonce.wrapping_add(1);
                                    }
                                }
                                if state.repair_rounds <= 3 || state.repair_rounds % 10 == 0 {
                                    eprintln!(
                                        "[repair] slot={slot} round={} missing={} sent=+{}",
                                        state.repair_rounds, batch.len(), total_sent - sent_before,
                                    );
                                }
                            }
                        }
                    }
                    for slot in done_slots { map.remove(&slot); }
                }

                if last_log.elapsed() >= Duration::from_secs(10) {
                    let t1_known = all_peers.iter()
                        .filter(|(info, _)| stake_map.contains_key(&info.pubkey().to_bytes()))
                        .count();
                    let pings_seen = arc_pings_rep.load(Ordering::Relaxed);
                    let pongs_sent = arc_pongs_rep.load(Ordering::Relaxed);
                    let responses  = arc_resp_rep.load(Ordering::Relaxed);
                    eprintln!(
                        "[repair] total_sent={total_sent} errors={send_errors} responses={responses} \
                         pings_seen={pings_seen} pongs_sent={pongs_sent} \
                         t1_visible={}/{} peers={}",
                        t1_known, tier1.len(), all_peers.len()
                    );
                    last_log = Instant::now();
                }
            }
        });
    }

    // ── coordinator: once tip is observed, enqueue coverage range + probe slots ──
    //
    // Probe bookkeeping: map from probe-depth (u64) → Option<bool> (None=pending,
    // Some(true)=completed, Some(false)=timed-out/not-full).
    // We track probe slots separately to distinguish them from the coverage range.
    // A probe slot is a single targeted slot at tip-depth; we watch it in the
    // repair_map to see if it reaches is_full() within 30s (the targeted deadline).
    //
    // The actual is_full() check happens naturally: the repair thread evicts
    // targeted slots on Complete or deadline, and logs COMPLETE.  We mirror
    // that by monitoring the repair_map here.

    let probe_depths_arc: Arc<Mutex<Vec<(u64, u64, Instant, Option<Instant>, Option<bool>)>>> =
        // (depth, slot, enqueued_at, first_seen_in_map, result)
        Arc::new(Mutex::new(Vec::new()));

    {
        let tip_c             = observed_tip.clone();
        let target_slots_c    = target_slots.clone();
        let meter_c           = meter.clone();
        let exit_c            = exit.clone();
        let probe_depths_c    = probe_depths_arc.clone();
        let probe_depth       = cfg.probe_depth;
        let probe_win_max     = cfg.probe_window_max;
        let completed_set_c   = completed_set.clone();
        let repair_map_coord  = repair_map.clone();

        std::thread::spawn(move || {
            // Wait until the TVU has seen at least one shred.
            loop {
                if exit_c.load(Ordering::Relaxed) { return; }
                let tip = tip_c.load(Ordering::Relaxed);
                if tip > 0 { break; }
                sleep(Duration::from_millis(200));
            }

            let tip = tip_c.load(Ordering::Relaxed);
            eprintln!("[harness] observed_tip={tip}  enqueuing coverage range [{}, {}]",
                tip.saturating_sub(probe_depth), tip.saturating_sub(64));

            // ── coverage range ─────────────────────────────────────────────
            let range_start = tip.saturating_sub(probe_depth);
            let range_end   = tip.saturating_sub(64);
            let now = Instant::now();
            // Mark targeted in meter (held briefly, then dropped before queue lock).
            {
                let mut m = meter_c.lock().unwrap_or_else(|e| e.into_inner());
                for s in range_start..=range_end {
                    m.mark_targeted(s, now);
                }
            }
            // Enqueue into the repair target queue.
            {
                let mut q = target_slots_c.lock().unwrap_or_else(|e| e.into_inner());
                for s in range_start..=range_end {
                    q.push_back(s);
                }
            }
            eprintln!("[harness] coverage range enqueued: {} slots", range_end.saturating_sub(range_start) + 1);

            // ── repair-window probe slots ──────────────────────────────────
            // Enqueue one slot per depth step (1000, 2000, … probe_win_max).
            {
                let mut q  = target_slots_c.lock().unwrap_or_else(|e| e.into_inner());
                let mut pd = probe_depths_c.lock().unwrap_or_else(|e| e.into_inner());
                let mut depth = 1000u64;
                while depth <= probe_win_max {
                    let probe_slot = tip.saturating_sub(depth);
                    // Only enqueue if not already in the coverage range
                    // (i.e. depth > probe_depth or depth < 64 — probe slots are beyond range).
                    if probe_slot < range_start || probe_slot > range_end {
                        q.push_back(probe_slot);
                    }
                    pd.push((depth, probe_slot, Instant::now(), None, None));
                    depth += 1000;
                }
                eprintln!("[harness] probe slots enqueued: {} depths", pd.len());
            }

            // ── poll probe results ─────────────────────────────────────────
            // Every 2s check repair_map for probe slots; once 30s pass mark failed.
            loop {
                if exit_c.load(Ordering::Relaxed) { break; }
                sleep(Duration::from_secs(2));

                let mut pd = probe_depths_c.lock().unwrap_or_else(|e| e.into_inner());
                let all_settled = pd.iter().all(|(_, _, _, _, r)| r.is_some());
                if all_settled { break; }

                for (depth, slot, _enqueued_at, first_seen_in_map, result) in pd.iter_mut() {
                    if result.is_some() { continue; }
                    // Check the completed_set first: only genuine is_full() completions
                    // are inserted there (not 30s deadline evictions).  A slot that times
                    // out is removed from repair_map at its 30s deadline but will NOT
                    // appear in completed_set, so map-absence must never be treated as
                    // success.
                    let genuinely_complete = completed_set_c.lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .contains(slot);
                    if genuinely_complete {
                        let elapsed = first_seen_in_map.map_or(0, |t: Instant| t.elapsed().as_millis() as u64);
                        *result = Some(true);
                        eprintln!("[probe] depth={depth} slot={slot} COMPLETE (elapsed_since_insert={elapsed}ms)");
                    } else {
                        // Record first_seen_in_map the moment the coordinator observes
                        // the slot present in repair_map.  The 35s deadline runs from
                        // THAT instant, not from enqueue time, so deep probe slots that
                        // wait in the queue are not penalised for queue-wait latency.
                        if first_seen_in_map.is_none() {
                            let in_map = repair_map_coord.try_lock()
                                .map(|m| m.contains_key(slot))
                                .unwrap_or(false);
                            if in_map {
                                *first_seen_in_map = Some(Instant::now());
                            }
                        }
                        // Only start the 35s timeout once first seen in repair_map.
                        if let Some(first) = *first_seen_in_map {
                            if first.elapsed() >= Duration::from_secs(35) {
                                *result = Some(false);
                                eprintln!("[probe] depth={depth} slot={slot} TIMED_OUT");
                            }
                        }
                        // Else: still pending insertion — stays None.
                    }
                    // Otherwise: still pending (None) — neither completed nor deadline-expired.
                }
            }

            // ── summarize probe window ─────────────────────────────────────
            let pd = probe_depths_c.lock().unwrap_or_else(|e| e.into_inner());
            let mut deepest_ok: u64 = 0;
            let mut first_fail: Option<u64> = None;
            for (depth, _slot, _, _, result) in pd.iter() {
                match result {
                    Some(true)  => { if *depth > deepest_ok { deepest_ok = *depth; } }
                    Some(false) => { if first_fail.map_or(true, |f| *depth < f) { first_fail = Some(*depth); } }
                    None        => {}
                }
            }
            match first_fail {
                Some(ff) => eprintln!("[probe] window: completed to depth {deepest_ok} slots, failed at {ff}"),
                None     => eprintln!("[probe] window: completed to depth {deepest_ok} slots (no failures within probe_window_max={probe_win_max})"),
            }
        });
    }

    let start      = Instant::now();
    let mut last_peers  = start;
    let mut last_status = start;
    let mut prev_crds_len: usize = 0;
    let mut prev_num_pulls: usize = 0;
    // current_slot for gossip epoch-slots advertisement (fake, incremented locally).
    // Do NOT use for targeting — targeting uses observed_tip from TVU shreds.
    let mut current_slot = 3_604_001_754u64;

    let entrypoint_addrs: Vec<SocketAddr> = cfg.entrypoints.iter()
        .filter_map(|ep| ep.to_socket_addrs().ok()?.next())
        .collect();

    eprintln!("[colibri] running (Ctrl-C to stop)");

    loop {
        if exit.load(Ordering::Relaxed) {
            break;
        }
        sleep(Duration::from_millis(400));
        current_slot += 1;
        cluster_info.push_epoch_slots(&[current_slot]);

        if last_peers.elapsed() >= Duration::from_secs(10) {
            let peers = cluster_info.all_peers();
            let crds_total = cluster_info.gossip.crds.read().unwrap().len();
            let t1: Vec<(u64, [u8; 32])> =
                tier1_pubkeys.lock().unwrap_or_else(|e| e.into_inner()).clone();
            let t1_set: HashSet<[u8; 32]> = t1.iter().map(|(_, pk)| *pk).collect();
            let t1_visible = peers.iter()
                .filter(|(info, _)| t1_set.contains(&info.pubkey().to_bytes()))
                .count();
            eprintln!("\n[gossip] peers: {} (Tier-1 visible: {}/{}) crds_entries: {}",
                peers.len(), t1_visible, t1.len(), crds_total);
            for (i, (info, wc)) in peers.iter().enumerate().take(5) {
                let tag = if t1_set.contains(&info.pubkey().to_bytes()) { " [T1]" } else { "" };
                eprintln!(
                    "  [{i}]{tag} {} tvu={:?} wc={wc}",
                    info.pubkey(),
                    info.tvu(Protocol::UDP),
                );
            }
            if peers.len() > 5 { eprintln!("  ... +{} more", peers.len() - 5); }

            let crds_delta = crds_total as isize - prev_crds_len as isize;
            let num_pulls = cluster_info.gossip.pull.num_pulls
                .load(std::sync::atomic::Ordering::Relaxed);
            let pulls_delta = num_pulls.wrapping_sub(prev_num_pulls);
            let my_sv = cluster_info.my_shred_version();
            eprintln!(
                "[diag] crds: total={crds_total} delta={crds_delta:+} peers={} \
                 non_contact={} | pulls_generated={num_pulls} (+{pulls_delta}) | my_shred_version={my_sv}",
                peers.len(),
                crds_total.saturating_sub(peers.len()),
            );
            prev_crds_len = crds_total;
            prev_num_pulls = num_pulls;

            eprintln!(
                "[diag] gossip_peers={}",
                cluster_info.gossip_peers().len(),
            );

            for ep_addr in &entrypoint_addrs {
                match cluster_info.lookup_contact_info_by_gossip_addr(ep_addr) {
                    Some(info) => eprintln!(
                        "[diag] entrypoint {ep_addr}: FOUND in CRDS — pubkey={}",
                        info.pubkey(),
                    ),
                    None => eprintln!(
                        "[diag] entrypoint {ep_addr}: NOT in CRDS — pull responses not received",
                    ),
                }
            }

            if let Ok(contents) = std::fs::read_to_string("/proc/net/udp") {
                let gossip_port_hex = format!("{:04X}", cfg.port);
                for line in contents.lines().skip(1) {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() > 4 {
                        // local_address is "IP:PORT" in hex
                        if let Some(port_hex) = parts[1].split(':').nth(1) {
                            if port_hex == gossip_port_hex {
                                eprintln!("[diag] /proc/net/udp gossip: tx_q:rx_q={}", parts[4]);
                            }
                        }
                    }
                }
            }

            eprintln!();
            last_peers = Instant::now();
        }

        if last_status.elapsed() >= Duration::from_secs(30) {
            eprintln!("[colibri] uptime={}s slot={current_slot}", start.elapsed().as_secs());
            last_status = Instant::now();
        }
    }

    // ── final report (printed on Ctrl-C) ─────────────────────────────────────
    {
        let rpt = meter.lock().unwrap_or_else(|e| e.into_inner()).report();
        let pings_seen  = arc_pings_seen.load(Ordering::Relaxed);
        let pongs_sent  = arc_pongs_sent.load(Ordering::Relaxed);
        let pong_errors = arc_pong_errors.load(Ordering::Relaxed);
        let responses   = arc_responses.load(Ordering::Relaxed);

        // Determine probe window summary
        let probe_summary = {
            let pd = probe_depths_arc.lock().unwrap_or_else(|e| e.into_inner());
            let mut deepest_ok: u64 = 0;
            let mut first_fail: Option<u64> = None;
            for (depth, _, _, _, result) in pd.iter() {
                match result {
                    Some(true)  => { if *depth > deepest_ok { deepest_ok = *depth; } }
                    Some(false) => { if first_fail.map_or(true, |f| *depth < f) { first_fail = Some(*depth); } }
                    None        => {}
                }
            }
            match first_fail {
                Some(ff) => format!("completed to depth {deepest_ok} slots, failed at {ff}"),
                None if deepest_ok == 0 => "no probe results yet (run longer or check repair)".to_string(),
                None     => format!("completed to depth {deepest_ok} slots (no failures within probe_window_max={}", cfg.probe_window_max),
            }
        };

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
        eprintln!("  repair window:  {probe_summary}");
        eprintln!("  pings_seen:     {pings_seen}");
        eprintln!("  pongs_sent:     {pongs_sent}  pong_errors={pong_errors}");
        eprintln!("  responses:      {responses}");
        eprintln!(
            "  sigverify:      ok={} rejected={} dropped_no_leader={}",
            sv_verified.load(Ordering::Relaxed),
            sv_rejected.load(Ordering::Relaxed),
            sv_dropped_no_leader.load(Ordering::Relaxed),
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
    eprintln!("[colibri] shutdown complete");
    Ok(())
}
