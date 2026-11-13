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


mod config;
mod coverage;
mod leader_schedule;
mod oracle;
mod server;
mod sigverify;
mod sink;

use {
    anyhow::Result,
    serde_json::Value,
    server::{ProtoEntry, ProtoFooter, ProtoTransaction},
    shred_net::{ShredNet, ShredNetConfig},
    solana_keypair::{read_keypair_file, write_keypair_file, Keypair},
    solana_pubkey::Pubkey,
    solana_signer::Signer,
    std::{
        collections::HashMap,
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

/// What the node was started with, on the log.
fn announce(cfg: &config::Config, keypair: &Keypair) {
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
}

/// Repair weighs its peers by stake: the table is fetched now and again every
/// `TIER1_REFRESH_SECS`, each new one sent to the returned channel for the
/// main loop to hand to the network.
fn follow_stakes(net: &ShredNet, cfg: &config::Config, exit: Arc<AtomicBool>) -> std::sync::mpsc::Receiver<HashMap<[u8; 32], u64>> {
    let by_node = |v: Vec<(u64, [u8; 32])>| -> HashMap<[u8; 32], u64> { v.into_iter().map(|(stake, node)| (node, stake)).collect() };
    net.set_stakes(by_node(fetch_tier1(&cfg.rpc_url, cfg.tier1_fanout)));
    let (stake_tx, stake_rx) = std::sync::mpsc::channel();
    let (rpc_url, fanout) = (cfg.rpc_url.clone(), cfg.tier1_fanout);
    std::thread::spawn(move || loop {
        sleep(Duration::from_secs(TIER1_REFRESH_SECS));
        if exit.load(Ordering::Relaxed) { break; }
        let updated = fetch_tier1(&rpc_url, fanout);
        // An empty table is a node that did not answer: what is held stays.
        if !updated.is_empty() && stake_tx.send(by_node(updated)).is_err() { break; }
    });
    stake_rx
}

/// What was covered and how fast, printed when the node is stopped.
fn report(meter: &Mutex<coverage::CoverageMeter>, sink: &sink::ColibriSink, oracle_sampler: &Mutex<Option<oracle::OracleSampler>>, uptime: Duration) {
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
    eprintln!("  sigverify:      {}", sink.sigverify());
    eprintln!("  uptime:         {}s", uptime.as_secs());
    if let Ok(sampler_guard) = oracle_sampler.lock()
        && let Some(sampler) = sampler_guard.as_ref() {
            eprintln!("  {}", sampler.report_line());
        }
    eprintln!("═════════════════════════════════════════════════════════════════");
    eprintln!();
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
    announce(&cfg, &keypair);

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

    let sink = Arc::new(sink::ColibriSink::new(leader_sched, entry_tx, tx_tx, footer_tx, meter.clone(), oracle_sampler.clone()));

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

    let stake_rx = follow_stakes(&net, &cfg, exit.clone());

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

    report(&meter, &sink, &oracle_sampler, start.elapsed());

    eprintln!("[colibri] waiting for gRPC server to shut down...");
    rt.block_on(grpc_handle).ok();
    net.shutdown();
    eprintln!("[colibri] shutdown complete");
    Ok(())
}
