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

mod server;

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
        net::{IpAddr, SocketAddr, ToSocketAddrs, UdpSocket},
        str::FromStr,
        sync::{
            atomic::{AtomicBool, Ordering},
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
    tls_cert:     Option<String>,  // path to PEM certificate
    tls_key:      Option<String>,  // path to PEM private key
    keypair_path: Option<String>,
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
            "--keypair"       => { i += 1; keypair_path   = Some(args[i].clone()); }
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
        keypair_path,
    })
}

fn rpc_post(url: &str, body: &str) -> Option<String> {
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
    let last_in_slot = is_data && variant != 0xA5 && (buf[85] & 0x40 != 0);
    Some(ShredInfo { slot, index, is_data, last_in_slot })
}

struct SlotRepairState {
    have:         HashSet<u32>,
    last_index:   Option<u32>,
    first_seen:   Instant,
    last_repair:  Instant,
    repair_rounds: u32,
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
        }
    }
    fn is_done(&self) -> bool {
        self.first_seen.elapsed() > Duration::from_secs(2)
    }
}

fn repair_window_index(
    keypair:      &Arc<Keypair>,
    recipient:    &[u8; 32],
    slot:         u64,
    shred_index:  u64,
    nonce:        u32,
) -> [u8; 160] {
    let ts = timestamp();
    let mut buf = [0u8; 160];
    buf[0..4].copy_from_slice(&3u32.to_le_bytes());
    buf[68..100].copy_from_slice(keypair.pubkey().as_ref());
    buf[100..132].copy_from_slice(recipient);
    buf[132..140].copy_from_slice(&ts.to_le_bytes());
    buf[140..144].copy_from_slice(&nonce.to_le_bytes());
    buf[144..152].copy_from_slice(&slot.to_le_bytes());
    buf[152..160].copy_from_slice(&shred_index.to_le_bytes());

    let mut sign_data = [0u8; 76];
    sign_data[0..32].copy_from_slice(keypair.pubkey().as_ref());
    sign_data[32..64].copy_from_slice(recipient);
    sign_data[64..72].copy_from_slice(&ts.to_le_bytes());
    sign_data[72..76].copy_from_slice(&nonce.to_le_bytes());
    let sig = keypair.sign_message(&sign_data);
    buf[4..68].copy_from_slice(sig.as_ref());
    buf
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

    let (gport, gossip_socket) =
        bind_in_range(cfg.ip, (cfg.port, cfg.port + 1))?;
    let gossip_addr = SocketAddr::new(cfg.ip, gport);
    eprintln!("[colibri] gossip addr:   {gossip_addr}");
    eprintln!("[diag] gossip socket local_addr: {:?}", gossip_socket.local_addr());

    let (rpc_port, _rpc_socket) =
        bind_in_range(cfg.ip, (cfg.port + 1000, cfg.port + 1100))?;
    let (tpu_port, _tpu_socket) =
        bind_in_range(cfg.ip, (cfg.port + 1100, cfg.port + 1200))?;

    let tvu_socket =
        UdpSocket::bind(SocketAddr::new(cfg.ip, cfg.tvu_port))?;
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

    let mut ci = ContactInfo::new(keypair.pubkey(), timestamp(), shred_version);
    ci.set_gossip(gossip_addr)?;
    ci.set_rpc(SocketAddr::new(cfg.ip, rpc_port))?;
    ci.set_tpu(SocketAddr::new(cfg.ip, tpu_port))?;
    ci.set_tvu(Protocol::UDP, tvu_addr)?;
    match ci.set_serve_repair(Protocol::UDP, repair_addr) {
        Ok(_)  => eprintln!("[colibri] serve_repair:  {repair_addr} ✓"),
        Err(e) => eprintln!("[colibri] serve_repair:  {repair_addr} FAILED: {e}"),
    }

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

    let repair_map: Arc<Mutex<HashMap<u64, SlotRepairState>>> =
        Arc::new(Mutex::new(HashMap::new()));

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

    let repair_map_tvu   = repair_map.clone();
    let exit_tvu         = exit.clone();
    let entry_tx_tvu     = entry_tx.clone();
    let tx_tx_tvu        = tx_tx.clone();
    std::thread::spawn(move || {
        let mut deshredder    = Deshredder::new(3_000, 200);
        let mut buf           = [0u8; 1280];
        let mut total: u64    = 0;
        let mut published: u64 = 0;
        let mut last_log      = Instant::now();
        let mut last_evict    = Instant::now();

        eprintln!("[tvu] ready — inline deshredding, no UDP forward");

        loop {
            if exit_tvu.load(Ordering::Relaxed) { break; }

            if last_evict.elapsed() >= Duration::from_millis(100) {
                deshredder.evict_expired();
                last_evict = Instant::now();
            }

            while let Ok(bytes) = repair_shred_rx.try_recv() {
                if let Some(se) = deshredder.push_raw(&bytes) {
                    let _ = entry_tx_tvu.send(ProtoEntry {
                        slot:    se.slot,
                        entries: se.entries_bytes,
                    });
                    for entry in &se.entries {
                        for tx in &entry.transactions {
                            let sig = tx.signatures.first()
                                .map(|s| s.to_string()).unwrap_or_default();
                            if let Ok(raw) = bincode::serialize(tx) {
                                let _ = tx_tx_tvu.send(ProtoTransaction {
                                    slot: se.slot, signature: sig, raw_tx: raw,
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

                    if let Some(info) = parse_shred_header(&buf[..n]) {
                        if info.is_data {
                            if let Ok(mut map) = repair_map_tvu.try_lock() {
                                let state = map.entry(info.slot)
                                    .or_insert_with(SlotRepairState::new);
                                state.have.insert(info.index);
                                if info.last_in_slot {
                                    state.last_index = Some(info.index);
                                }
                            }
                        }
                    }

                    if let Some(se) = deshredder.push_raw(&buf[..n]) {
                        let _ = entry_tx_tvu.send(ProtoEntry {
                            slot:    se.slot,
                            entries: se.entries_bytes,
                        });
                        for entry in &se.entries {
                            for tx in &entry.transactions {
                                let sig = tx.signatures.first()
                                    .map(|s| s.to_string()).unwrap_or_default();
                                if let Ok(raw) = bincode::serialize(tx) {
                                    let _ = tx_tx_tvu.send(ProtoTransaction {
                                        slot: se.slot, signature: sig, raw_tx: raw,
                                    });
                                }
                            }
                        }
                        published += 1;
                    }

                    if last_log.elapsed() >= Duration::from_secs(10) {
                        eprintln!(
                            "[tvu] shreds={total} published={published} \
                             assembler_slots={} dedup_slots={}",
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
        let cluster_repair   = cluster_info.clone();
        let keypair_repair   = keypair.clone();
        let exit_repair      = exit.clone();
        let repair_map_rep   = repair_map.clone();
        let tier1_repair     = tier1_pubkeys.clone();
        let repair_port      = cfg.repair_port;
        let repair_ip        = cfg.ip;
        std::thread::spawn(move || {
            let repair_shred_tx = repair_shred_tx;
            let repair_sock = UdpSocket::bind(
                SocketAddr::new(repair_ip, repair_port)
            ).expect("bind repair sock");
            repair_sock.set_read_timeout(Some(Duration::from_millis(5))).ok();
            eprintln!("[repair] bound on {repair_ip}:{repair_port}");

            let mut nonce: u32       = 0xdead_beef;
            let mut responses: u64   = 0;
            let mut total_sent: u64  = 0;
            let mut send_errors: u64 = 0;
            let mut last_log         = Instant::now();
            let mut recv_buf         = [0u8; 1500];

            eprintln!("[repair] thread started (50ms cycle, Tier-1 priority)");

            loop {
                if exit_repair.load(Ordering::Relaxed) { break; }

                let mut drain_count = 0usize;
                while drain_count < 512 {
                    match repair_sock.recv_from(&mut recv_buf) {
                        Ok((n, _)) => {
                            responses += 1;
                            let _ = repair_shred_tx.send(recv_buf[..n].to_vec());
                            drain_count += 1;
                        }
                        Err(_) => break,
                    }
                }

                sleep(Duration::from_millis(50));

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

                // Scan for slots with gaps.
                let mut done_slots = Vec::new();
                if let Ok(mut map) = repair_map_rep.try_lock() {
                    for (&slot, state) in map.iter_mut() {
                        if state.is_done() { done_slots.push(slot); continue; }
                        let last = match state.last_index { Some(l) => l, None => continue };
                        let missing: Vec<u32> = (0..=last)
                            .filter(|i| !state.have.contains(i))
                            .collect();
                        if missing.is_empty() { done_slots.push(slot); continue; }
                        if state.last_repair.elapsed() < Duration::from_millis(50) { continue; }
                        state.last_repair  = Instant::now();
                        state.repair_rounds += 1;

                        let batch: Vec<u32> = missing.into_iter().take(128).collect();
                        let sent_before = total_sent;
                        for &idx in &batch {
                            for (pk, addr) in &targets {
                                let req = repair_window_index(
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
                    for slot in done_slots { map.remove(&slot); }
                }

                if last_log.elapsed() >= Duration::from_secs(10) {
                    let t1_known = all_peers.iter()
                        .filter(|(info, _)| stake_map.contains_key(&info.pubkey().to_bytes()))
                        .count();
                    eprintln!(
                        "[repair] total_sent={total_sent} errors={send_errors} responses={responses} \
                         t1_visible={}/{} peers={}",
                        t1_known, tier1.len(), all_peers.len()
                    );
                    last_log = Instant::now();
                }
            }
        });
    }

    let start      = Instant::now();
    let mut last_peers  = start;
    let mut last_status = start;
    let mut prev_crds_len: usize = 0;
    let mut prev_num_pulls: usize = 0;
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

    eprintln!("[colibri] waiting for gRPC server to shut down...");
    rt.block_on(grpc_handle).ok();
    eprintln!("[colibri] shutdown complete");
    Ok(())
}
