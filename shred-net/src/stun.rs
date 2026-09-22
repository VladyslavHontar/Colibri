//! Minimal RFC 5389 client — lets Colibri run behind a port-rewriting NAT.
//!
//! Gossip advertises the port Colibri *binds*. A NAT that rewrites source ports
//! turns that into a lie: turbine addressed to the bound port hits a mapping
//! that does not exist and is dropped, so the node joins gossip cleanly and
//! then receives nothing. Asking a STUN server "what address do you see me as?"
//! *from the very socket that will receive* yields the mapping that really
//! exists, which is what belongs in ContactInfo.
//!
//! This only works behind a NAT with endpoint-independent mapping AND filtering
//! (full cone): the mapping is created by our outbound probe, and every
//! validator in the turbine tree must then be able to use it. Verify with
//! `stun.rs`'s sibling diagnostics before trusting it — an address-restricted
//! NAT will pass the STUN probe and still drop turbine.

use {
    anyhow::{anyhow, Context, Result},
    std::{
        net::{SocketAddr, ToSocketAddrs, UdpSocket},
        sync::{
            atomic::{AtomicBool, Ordering},
            Arc,
        },
        thread::{sleep, JoinHandle},
        time::{Duration, SystemTime, UNIX_EPOCH},
    },
};

const MAGIC: u32 = 0x2112_A442;
const BINDING_REQUEST: u16 = 0x0001;
/// Same Binding method, Indication class. RFC 5389 §7.3.3: a server receiving
/// an indication sends nothing back — which is exactly what a keepalive wants,
/// since the socket it refreshes is busy receiving turbine.
const BINDING_INDICATION: u16 = 0x0011;
const XOR_MAPPED_ADDRESS: u16 = 0x0020;
const ATTEMPTS: usize = 3;

/// Re-probe well inside the shortest UDP mapping timeouts seen in the wild
/// (consumer routers commonly expire at 30s).
const KEEPALIVE_PERIOD: Duration = Duration::from_secs(20);

fn transaction_id() -> [u8; 12] {
    // No `rand` in this crate's tree and none warranted: the txid only has to
    // be unique enough to match our own reply within a few seconds.
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64;
    let mut id = [0u8; 12];
    id[..8].copy_from_slice(&nanos.to_be_bytes());
    id[8..].copy_from_slice(&std::process::id().to_be_bytes()[..4]);
    id
}

fn stun_message(kind: u16, txid: &[u8; 12]) -> [u8; 20] {
    let mut req = [0u8; 20];
    req[0..2].copy_from_slice(&kind.to_be_bytes());
    req[2..4].copy_from_slice(&0u16.to_be_bytes()); // no attributes
    req[4..8].copy_from_slice(&MAGIC.to_be_bytes());
    req[8..20].copy_from_slice(txid);
    req
}

/// Pull the XOR-MAPPED-ADDRESS out of a success response, checking the txid.
fn parse_response(buf: &[u8], txid: &[u8; 12]) -> Option<SocketAddr> {
    if buf.len() < 20 || buf[8..20] != txid[..] {
        return None;
    }
    let attrs_len = u16::from_be_bytes([buf[2], buf[3]]) as usize;
    let end = (20 + attrs_len).min(buf.len());
    let mut i = 20;
    while i + 4 <= end {
        let kind = u16::from_be_bytes([buf[i], buf[i + 1]]);
        let len = u16::from_be_bytes([buf[i + 2], buf[i + 3]]) as usize;
        let val = buf.get(i + 4..i + 4 + len)?;
        if kind == XOR_MAPPED_ADDRESS && len >= 8 && val[1] == 0x01 {
            // Port and address are XORed with the magic cookie (RFC 5389 §15.2).
            let port = u16::from_be_bytes([val[2], val[3]]) ^ (MAGIC >> 16) as u16;
            let raw = u32::from_be_bytes([val[4], val[5], val[6], val[7]]) ^ MAGIC;
            return Some(SocketAddr::from((raw.to_be_bytes(), port)));
        }
        i += 4 + len + ((4 - len % 4) % 4); // attributes are 4-byte aligned
    }
    None
}

/// Discover how the outside world addresses `sock`.
///
/// Sends from `sock` itself, so the answer describes that socket's NAT mapping
/// and no other. Restores the socket's previous read timeout before returning.
pub fn external_addr(sock: &UdpSocket, server: &str) -> Result<SocketAddr> {
    let server_addr = server
        .to_socket_addrs()
        .with_context(|| format!("resolving STUN server {server}"))?
        .find(|a| a.is_ipv4())
        .ok_or_else(|| anyhow!("STUN server {server} has no IPv4 address"))?;

    let prev_timeout = sock.read_timeout().ok().flatten();
    sock.set_read_timeout(Some(Duration::from_secs(2)))?;

    let mut result = Err(anyhow!("STUN server {server} did not answer"));
    let mut buf = [0u8; 1024];
    'attempts: for _ in 0..ATTEMPTS {
        let txid = transaction_id();
        if sock.send_to(&stun_message(BINDING_REQUEST, &txid), server_addr).is_err() {
            continue;
        }
        // Drain until our own reply shows up: on a live socket other traffic
        // (or a stale STUN answer) can be queued ahead of it.
        while let Ok((n, from)) = sock.recv_from(&mut buf) {
            if from == server_addr
                && let Some(addr) = parse_response(&buf[..n], &txid)
            {
                result = Ok(addr);
                break 'attempts;
            }
        }
    }

    sock.set_read_timeout(prev_timeout)?;
    result
}

/// Keep a receive-only socket's NAT mapping alive.
///
/// The TVU socket never transmits — it only takes turbine — so without this its
/// mapping expires and the feed stops. Sent as a Binding *Indication*: the
/// mapping is refreshed by the outbound packet alone, and because the server
/// answers nothing, the turbine recv loop on this same socket never has to tell
/// a keepalive reply apart from a shred.
pub fn spawn_keepalive(
    sock: Arc<UdpSocket>,
    server: String,
    exit: Arc<AtomicBool>,
) -> JoinHandle<()> {
    std::thread::spawn(move || {
        let resolved = server
            .to_socket_addrs()
            .ok()
            .and_then(|mut it| it.find(|a| a.is_ipv4()));
        let Some(server_addr) = resolved else {
            log::error!("[stun] cannot resolve {server}; TVU mapping will expire");
            return;
        };
        while !exit.load(Ordering::Relaxed) {
            let txid = transaction_id();
            if let Err(e) = sock.send_to(&stun_message(BINDING_INDICATION, &txid), server_addr) {
                log::warn!("[stun] keepalive send failed: {e}");
            }
            // Wake often enough to honour `exit` promptly.
            for _ in 0..KEEPALIVE_PERIOD.as_secs() {
                if exit.load(Ordering::Relaxed) {
                    return;
                }
                sleep(Duration::from_secs(1));
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn success_response(txid: &[u8; 12], attrs: &[u8]) -> Vec<u8> {
        let mut m = Vec::new();
        m.extend_from_slice(&0x0101u16.to_be_bytes()); // Binding Success
        m.extend_from_slice(&(attrs.len() as u16).to_be_bytes());
        m.extend_from_slice(&MAGIC.to_be_bytes());
        m.extend_from_slice(txid);
        m.extend_from_slice(attrs);
        m
    }

    fn xor_mapped(ip: [u8; 4], port: u16) -> Vec<u8> {
        let mut v = vec![0x00, 0x01];
        v.extend_from_slice(&(port ^ (MAGIC >> 16) as u16).to_be_bytes());
        v.extend_from_slice(&(u32::from_be_bytes(ip) ^ MAGIC).to_be_bytes());
        let mut a = Vec::new();
        a.extend_from_slice(&XOR_MAPPED_ADDRESS.to_be_bytes());
        a.extend_from_slice(&(v.len() as u16).to_be_bytes());
        a.extend_from_slice(&v);
        a
    }

    #[test]
    fn indication_is_the_binding_method_with_the_indication_class() {
        let txid = transaction_id();
        let m = stun_message(BINDING_INDICATION, &txid);
        // RFC 5389 §6 interleaves the class bits into the type; getting this
        // wrong turns a silent keepalive back into one that draws a reply.
        assert_eq!(u16::from_be_bytes([m[0], m[1]]), 0x0011);
        assert_eq!(u16::from_be_bytes([m[0], m[1]]) & 0x0110, 0x0010, "class = indication");
        assert_eq!(u16::from_be_bytes([m[2], m[3]]), 0, "no attributes");
        assert_eq!(m[4..8], MAGIC.to_be_bytes(), "magic cookie");
    }

    #[test]
    fn decodes_xor_mapped_address() {
        let txid = transaction_id();
        let msg = success_response(&txid, &xor_mapped([37, 228, 210, 158], 27901));
        assert_eq!(
            parse_response(&msg, &txid),
            Some("37.228.210.158:27901".parse().unwrap())
        );
    }

    #[test]
    fn rejects_foreign_transaction_id() {
        let txid = transaction_id();
        let mut other = txid;
        other[0] ^= 0xff;
        let msg = success_response(&other, &xor_mapped([1, 2, 3, 4], 1234));
        assert_eq!(parse_response(&msg, &txid), None, "txid must be checked");
    }

    #[test]
    fn skips_preceding_attributes_with_padding() {
        let txid = transaction_id();
        // SOFTWARE (0x8022), 5 bytes + 3 pad — exercises the alignment step.
        let mut attrs = vec![0x80, 0x22, 0x00, 0x05, b'h', b'e', b'l', b'l', b'o', 0, 0, 0];
        attrs.extend_from_slice(&xor_mapped([10, 0, 0, 7], 40000));
        let msg = success_response(&txid, &attrs);
        assert_eq!(
            parse_response(&msg, &txid),
            Some("10.0.0.7:40000".parse().unwrap())
        );
    }

    #[test]
    fn truncated_message_does_not_panic() {
        let txid = transaction_id();
        let msg = success_response(&txid, &xor_mapped([1, 1, 1, 1], 99));
        for cut in 0..msg.len() {
            let _ = parse_response(&msg[..cut], &txid);
        }
    }

    /// The real thing, against public servers. Ignored by default so CI and
    /// offline builds stay green; run with `--ignored` on a networked box.
    #[test]
    #[ignore = "requires outbound UDP to a public STUN server"]
    fn discovers_a_real_mapping() {
        let sock = UdpSocket::bind("0.0.0.0:0").unwrap();
        let addr = external_addr(&sock, "stun.l.google.com:19302").unwrap();
        assert!(!addr.ip().is_unspecified() && addr.port() != 0);
    }
}
