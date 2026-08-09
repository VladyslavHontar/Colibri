//! Gossip participation: join the cluster as an unstaked node and enumerate
//! repair-serving peers. Extracted from Colibri's Phase-0-proven setup.
//!
//! Hard-won operational invariants baked in here (each cost hours in Phase 0):
//!   - bind sockets on `0.0.0.0`, advertise the public `--ip`;
//!   - advertise a MINIMAL ContactInfo (gossip + tvu only) — advertising
//!     rpc/tpu/serve_repair you don't serve makes peers' sanitizer drop the
//!     pull and stalls bootstrap;
//!   - callers must pass MULTIPLE entrypoints — a single dead entrypoint
//!     stalls gossip forever (`pulls_generated=0`).

use {
    solana_gossip::{
        cluster_info::ClusterInfo,
        contact_info::{ContactInfo, Protocol},
        gossip_service::GossipService,
    },
    solana_hash::Hash,
    solana_keypair::Keypair,
    solana_net_utils::bind_in_range,
    solana_signer::Signer,
    solana_streamer::socket::SocketAddrSpace,
    solana_time_utils::timestamp,
    std::{
        collections::HashMap,
        net::{IpAddr, Ipv4Addr, SocketAddr, ToSocketAddrs},
        sync::{atomic::AtomicBool, Arc},
    },
};

/// Configuration for joining gossip.
pub struct GossipConfig {
    /// Public IP advertised to peers (sockets still bind on 0.0.0.0).
    pub advertise_ip: IpAddr,
    /// Gossip UDP port to advertise/bind.
    pub gossip_port: u16,
    /// TVU UDP port to advertise (shreds arrive here).
    pub tvu_port: u16,
    /// Shred version. `None` → fetch from the first reachable entrypoint.
    pub shred_version: Option<u16>,
    /// Cluster entrypoints. Pass several — one dead entrypoint stalls gossip.
    pub entrypoints: Vec<String>,
}

/// A live gossip participant. Drop / flip `exit` to stop.
pub struct GossipNode {
    pub cluster_info: Arc<ClusterInfo>,
    pub gossip_service: GossipService,
    /// The shred version actually in use (fetched or configured).
    pub shred_version: u16,
}

/// Join the cluster: bind the gossip socket, resolve the shred version, publish
/// a minimal ContactInfo, register entrypoints, and start the gossip threads.
pub fn join(
    cfg: &GossipConfig,
    keypair: Arc<Keypair>,
    exit: Arc<AtomicBool>,
) -> anyhow::Result<GossipNode> {
    // Bind on 0.0.0.0, advertise the public IP (canonical agave pattern).
    let bind_ip = IpAddr::V4(Ipv4Addr::UNSPECIFIED);
    let (gport, gossip_socket) = bind_in_range(bind_ip, (cfg.gossip_port, cfg.gossip_port + 1))?;
    let gossip_addr = SocketAddr::new(cfg.advertise_ip, gport);
    let tvu_addr = SocketAddr::new(cfg.advertise_ip, cfg.tvu_port);

    // Resolve shred version (config override, else probe the first entrypoint).
    let shred_version = cfg.shred_version.or_else(|| {
        cfg.entrypoints.first().and_then(|ep| {
            let addr = ep.to_socket_addrs().ok()?.next()?;
            solana_net_utils::get_cluster_shred_version(&addr).ok()
        })
    });
    let shred_version = match shred_version {
        Some(v) => v,
        None => anyhow::bail!(
            "could not determine shred_version: no override and no entrypoint answered"
        ),
    };

    // Minimal ContactInfo: gossip + tvu only.
    let mut ci = ContactInfo::new(keypair.pubkey(), timestamp(), shred_version);
    ci.set_gossip(gossip_addr)?;
    ci.set_tvu(Protocol::UDP, tvu_addr)?;

    let cluster_info = Arc::new(ClusterInfo::new(
        ci,
        keypair.clone(),
        SocketAddrSpace::Unspecified,
    ));
    cluster_info.push_snapshot_hashes((0, Hash::default()), vec![]).ok();

    let mut registered = 0usize;
    for ep in &cfg.entrypoints {
        match ep.to_socket_addrs() {
            Ok(mut it) => {
                if let Some(addr) = it.next() {
                    cluster_info.set_entrypoint(ContactInfo::new_gossip_entry_point(&addr));
                    registered += 1;
                }
            }
            Err(e) => log::warn!("[shred-net] cannot resolve entrypoint {ep}: {e}"),
        }
    }
    if registered == 0 {
        anyhow::bail!("no resolvable entrypoints — gossip cannot bootstrap");
    }
    if registered == 1 {
        log::warn!(
            "[shred-net] only one entrypoint registered — a single dead entrypoint \
             stalls gossip; pass several"
        );
    }

    let gossip_service = GossipService::new(
        &cluster_info,
        None,
        Arc::from([gossip_socket]),
        None,
        true,
        None,
        exit,
    );

    Ok(GossipNode { cluster_info, gossip_service, shred_version })
}

/// Score a peer by recency × log(stake). Newer contact + more stake ⇒ better.
/// With `stake = 0` (unknown), scoring degrades to pure recency, which still
/// produced the Phase-0 firehose.
pub fn score_peer(wallclock_ms: u64, now_ms: u64, stake_lamports: u64) -> f64 {
    let age_s = (now_ms.saturating_sub(wallclock_ms)) as f64 / 1_000.0;
    let time_weight = 1.0 / (1.0 + age_s);
    let stake_weight = ((stake_lamports + 1) as f64).ln();
    time_weight * stake_weight
}

/// Select the top-`n` repair-serving peers by [`score_peer`].
///
/// `stakes` maps node pubkey → activated stake (empty ⇒ recency-only). Returns
/// `(pubkey_bytes, serve_repair_addr)` pairs for the repair driver to query.
pub fn repair_peers(
    cluster_info: &ClusterInfo,
    stakes: &HashMap<[u8; 32], u64>,
    now_ms: u64,
    n: usize,
) -> Vec<([u8; 32], SocketAddr)> {
    let mut scored: Vec<(f64, [u8; 32], SocketAddr)> = cluster_info
        .all_peers()
        .iter()
        .filter_map(|(info, wc)| {
            let addr = info.serve_repair(Protocol::UDP)?;
            let pk = info.pubkey().to_bytes();
            let stake = stakes.get(&pk).copied().unwrap_or(0);
            Some((score_peer(*wc, now_ms, stake), pk, addr))
        })
        .collect();
    scored.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    scored.into_iter().take(n).map(|(_, pk, addr)| (pk, addr)).collect()
}
