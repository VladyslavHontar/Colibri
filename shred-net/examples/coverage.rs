//! Live coverage harness for the `shred-net` crate — reproduces the Phase-0
//! measurement through the NEW shared crate (gossip join → repair-until-full →
//! complete-block emission). This is the acceptance test for ShredNet: run it
//! against mainnet and watch the `is_full` completion rate.
//!
//! Example:
//!   cargo run -p shred-net --example coverage --release -- \
//!     --ip <PUBLIC_IP> --keypair id.json --probe-depth 6000 \
//!     --entrypoint entrypoint.mainnet-beta.solana.com:8001 \
//!     --entrypoint entrypoint2.mainnet-beta.solana.com:8001 \
//!     --entrypoint entrypoint3.mainnet-beta.solana.com:8001 \
//!     --entrypoint entrypoint4.mainnet-beta.solana.com:8001 \
//!     --entrypoint entrypoint5.mainnet-beta.solana.com:8001
//!
//! NB: pass SEVERAL entrypoints — a single dead one stalls gossip forever.

use {
    shred_net::{BlockSink, ShredNet, ShredNetConfig},
    solana_entry::entry::Entry,
    solana_keypair::{read_keypair_file, Keypair},
    solana_signer::Signer,
    std::{
        net::IpAddr,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        thread::sleep,
        time::{Duration, Instant},
    },
};

/// Counts completed slots, their transactions, and slots concluded to be skipped.
struct CoverageSink {
    completed: AtomicU64,
    skipped:   AtomicU64,
    txs:       AtomicU64,
}

impl BlockSink for CoverageSink {
    fn on_complete_block(&self, _slot: u64, entries: Vec<Entry>) {
        self.completed.fetch_add(1, Ordering::Relaxed);
        let n: usize = entries.iter().map(|e| e.transactions.len()).sum();
        self.txs.fetch_add(n as u64, Ordering::Relaxed);
    }
    fn on_slot_skipped(&self, _slot: u64) {
        self.skipped.fetch_add(1, Ordering::Relaxed);
    }
    // Fast-lane hook unused here; we measure the complete lane only.
}

struct Args {
    ip:           IpAddr,
    gossip_port:  u16,
    tvu_port:     u16,
    repair_port:  u16,
    entrypoints:  Vec<String>,
    keypair_path: Option<String>,
    probe_depth:  u64,
    top_peers:    usize,
    window:       usize,
}

fn parse_args() -> Result<Args, String> {
    let mut ip: Option<IpAddr> = None;
    let mut gossip_port = 8000u16;
    let mut tvu_port = 8200u16;
    let mut repair_port = 8210u16;
    let mut entrypoints = Vec::new();
    let mut keypair_path = None;
    let mut probe_depth = 6000u64;
    let mut top_peers = 64usize;
    let mut window = 64usize;

    let argv: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--ip"          => { i += 1; ip = Some(argv[i].parse().map_err(|e| format!("--ip: {e}"))?); }
            "--port"        => { i += 1; gossip_port = argv[i].parse().map_err(|e| format!("--port: {e}"))?; }
            "--tvu-port"    => { i += 1; tvu_port = argv[i].parse().map_err(|e| format!("--tvu-port: {e}"))?; }
            "--repair-port" => { i += 1; repair_port = argv[i].parse().map_err(|e| format!("--repair-port: {e}"))?; }
            "--entrypoint"  => { i += 1; entrypoints.push(argv[i].clone()); }
            "--keypair"     => { i += 1; keypair_path = Some(argv[i].clone()); }
            "--probe-depth" => { i += 1; probe_depth = argv[i].parse().map_err(|e| format!("--probe-depth: {e}"))?; }
            "--top-peers"   => { i += 1; top_peers = argv[i].parse().map_err(|e| format!("--top-peers: {e}"))?; }
            "--window"      => { i += 1; window = argv[i].parse().map_err(|e| format!("--window: {e}"))?; }
            other => return Err(format!("unknown arg: {other}")),
        }
        i += 1;
    }

    Ok(Args {
        ip: ip.ok_or("--ip <PUBLIC_IP> is required")?,
        gossip_port, tvu_port, repair_port, entrypoints, keypair_path, probe_depth,
        top_peers, window,
    })
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = parse_args().map_err(|e| anyhow::anyhow!(e))?;

    let keypair = Arc::new(match &args.keypair_path {
        Some(p) => read_keypair_file(p)
            .map_err(|e| anyhow::anyhow!("read keypair {p}: {e}"))?,
        None => Keypair::new(),
    });
    eprintln!("[coverage] pubkey={}  advertise_ip={}", keypair.pubkey(), args.ip);
    eprintln!("[coverage] entrypoints={}  probe_depth={}  top_peers={}  window={}",
        args.entrypoints.len(), args.probe_depth, args.top_peers, args.window);

    let sink = Arc::new(CoverageSink {
        completed: AtomicU64::new(0),
        skipped: AtomicU64::new(0),
        txs: AtomicU64::new(0),
    });

    let net = ShredNet::start(
        ShredNetConfig {
            advertise_ip: args.ip,
            gossip_port:  args.gossip_port,
            tvu_port:     args.tvu_port,
            repair_port:  args.repair_port,
            shred_version: None,
            entrypoints:  args.entrypoints,
            keep_window:  args.probe_depth + 2000,
            target_window: args.window,
            top_peers:    args.top_peers,
        },
        keypair,
        sink.clone(),
    )?;
    eprintln!("[coverage] ShredNet started — waiting for first turbine shred…");

    // Wait until gossip is up and turbine delivers a tip.
    loop {
        let tip = net.observed_tip();
        if tip > 0 {
            eprintln!("[coverage] observed_tip={tip}");
            break;
        }
        sleep(Duration::from_millis(250));
    }

    // Target a historical range [tip-probe_depth .. tip-64] and measure how many
    // reach is_full purely via P2P repair.
    let tip = net.observed_tip();
    let range_start = tip.saturating_sub(args.probe_depth);
    let range_end   = tip.saturating_sub(64);
    let requested   = range_end.saturating_sub(range_start) + 1;
    net.request_slots(range_start..=range_end);
    eprintln!("[coverage] requested {requested} slots [{range_start}, {range_end}] — repairing…");

    let started = Instant::now();
    loop {
        sleep(Duration::from_secs(10));
        let done = sink.completed.load(Ordering::Relaxed);
        let skipped = sink.skipped.load(Ordering::Relaxed);
        let txs  = sink.txs.load(Ordering::Relaxed);
        let accounted = done + skipped;
        let pct  = (accounted as f64 / requested as f64) * 100.0;
        eprintln!(
            "[coverage] t={:>4}s  complete={done} skipped={skipped} pending={}/{requested} ({pct:.2}% accounted)  txs={txs}  live_tip={}",
            requested.saturating_sub(accounted),
            started.elapsed().as_secs(),
            net.observed_tip(),
        );
        // Done when every producible slot is reconstructed and every skipped
        // slot is identified — i.e. nothing is left pending.
        if accounted >= requested {
            eprintln!(
                "[coverage] DONE — {done} reconstructed + {skipped} skipped = 100% of the range accounted for."
            );
            break;
        }
    }

    net.shutdown();
    Ok(())
}
