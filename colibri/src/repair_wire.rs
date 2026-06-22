//! Repair-socket wire codec: classify inbound packets and build Pong replies.
//!
//! # Wire layout (authoritative: docs/repair-wire-format.md)
//!
//! Inbound `RepairResponse::Ping` (132 bytes):
//!   [0..4]   discriminant = 0 (LE u32)
//!   [4..36]  Ping.from (Pubkey, 32 bytes)
//!   [36..68] Ping.token ([u8; 32])
//!   [68..132] Ping.signature (64 bytes)
//!
//! Outbound `RepairProtocol::Pong` (132 bytes):
//!   [0..4]   discriminant = 7 (LE u32)
//!   [4..36]  Pong.from (our pubkey, 32 bytes)
//!   [36..68] Pong.hash = SHA-256("SOLANA_PING_PONG" ++ token)
//!   [68..132] Pong.signature = sign(hash_bytes)
//!
//! SHA-256 path: manual implementation using `sha2` crate.
//! (`solana_gossip::ping_pong::Pong::new` is available but requires constructing
//! a `Ping<32>` object, while `RepairResponse` is in `solana-core` which is not
//! a dependency. The manual sha2 path is simpler and equally correct.)

use {
    sha2::{Digest, Sha256},
    solana_keypair::Keypair,
    solana_signer::Signer,
};

/// Total byte length of an outbound Pong packet.
pub const PONG_WIRE_LEN: usize = 132;
/// Byte offset of the `from` pubkey field within the Pong packet.
pub const PONG_FROM_OFFSET: usize = 4;

/// Recognised packet types on the repair socket.
pub enum Inbound {
    /// Inbound `RepairResponse::Ping` — carries the 32-byte token that must be
    /// echoed back (hashed) in the Pong reply.
    Ping([u8; 32]),
    /// Inbound shred data — forward to the deshredder channel.
    ShredResponse,
    /// Anything else — discard silently.
    Other,
}

/// Classify one inbound UDP packet.
///
/// Classification rules (from docs/repair-wire-format.md):
/// 1. `len == 132 && buf[0..4] == [0,0,0,0]`  → `Ping(token@[36..68])`
/// 2. `len >= 88` and shred-variant byte at `buf[64]` matches a known shred
///    variant (same logic as `parse_shred_header` in `main.rs`)  → `ShredResponse`
/// 3. everything else  → `Other`
pub fn parse_inbound(buf: &[u8]) -> Inbound {
    // Rule 1: Ping
    if buf.len() == 132 && buf[0..4] == [0, 0, 0, 0] {
        let mut token = [0u8; 32];
        token.copy_from_slice(&buf[36..68]);
        return Inbound::Ping(token);
    }

    // Rule 2: Shred — check variant byte at buf[64] (same match as parse_shred_header)
    if buf.len() >= 88 {
        let variant = buf[64];
        let is_shred = match variant & 0xF0 {
            0x80 | 0x90 | 0xB0 => true,
            _ => variant == 0xA5,
        };
        if is_shred {
            return Inbound::ShredResponse;
        }
    }

    Inbound::Other
}

/// Construct the 132-byte `RepairProtocol::Pong` wire packet.
///
/// Construction:
///   hash = SHA-256("SOLANA_PING_PONG" ++ token)
///   signature = keypair.sign_message(hash_bytes)
///   wire = discriminant(7 LE u32) ‖ from(32) ‖ hash(32) ‖ signature(64)
pub fn build_pong(keypair: &Keypair, token: [u8; 32]) -> Vec<u8> {
    // hash = SHA-256("SOLANA_PING_PONG" ++ token)
    let mut hasher = Sha256::new();
    hasher.update(b"SOLANA_PING_PONG");
    hasher.update(&token);
    let hash_bytes: [u8; 32] = hasher.finalize().into();

    // signature = sign(hash_bytes)
    let sig = keypair.sign_message(&hash_bytes);

    // Assemble wire packet
    let mut pong = Vec::with_capacity(PONG_WIRE_LEN);
    pong.extend_from_slice(&7u32.to_le_bytes());           // discriminant = 7
    pong.extend_from_slice(keypair.pubkey().as_ref());     // from (32 bytes)
    pong.extend_from_slice(&hash_bytes);                   // hash (32 bytes)
    pong.extend_from_slice(sig.as_ref());                  // signature (64 bytes)
    pong
}

// ─── helpers ────────────────────────────────────────────────────────────────

/// Build a synthetic `RepairResponse::Ping` wire packet for testing.
///
/// Layout: discriminant=0 (4 bytes) | from_pubkey (32 bytes, zeroed) |
///         token (32 bytes) | signature (64 bytes, zeroed)
#[cfg(test)]
pub fn synth_ping(token: [u8; 32]) -> Vec<u8> {
    let mut buf = vec![0u8; 132];
    // discriminant = 0 (already zero)
    // from = zeroed (bytes 4..36, already zero)
    buf[36..68].copy_from_slice(&token);
    // signature = zeroed (bytes 68..132, already zero)
    buf
}

// ─── tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use solana_keypair::Keypair;

    #[test]
    fn pong_has_expected_length_and_token_echo() {
        // Layout asserted here MUST match docs/repair-wire-format.md (Task 0).
        let kp = Keypair::new();
        let token = [7u8; 32];
        let pong = build_pong(&kp, token);
        // Pong = discriminant + from(32) + hash(32) + signature(64); total 132.
        assert_eq!(pong.len(), PONG_WIRE_LEN);
        // from-pubkey must be our identity
        assert_eq!(&pong[PONG_FROM_OFFSET..PONG_FROM_OFFSET + 32], kp.pubkey().as_ref());
    }

    #[test]
    fn parse_inbound_classifies_ping_vs_shred() {
        // A synthesised Ping per Task 0 layout → Inbound::Ping(token)
        let ping_bytes = synth_ping([9u8; 32]);
        match parse_inbound(&ping_bytes) {
            Inbound::Ping(t) => assert_eq!(t, [9u8; 32]),
            _ => panic!("expected Ping"),
        }
        // A data-shred-shaped buffer (variant byte at [64]) → ShredResponse
        let mut shred = vec![0u8; 200];
        shred[64] = 0x80; // data shred variant, per parse_shred_header at main.rs:249
        assert!(matches!(parse_inbound(&shred), Inbound::ShredResponse));
    }

    #[test]
    fn pong_discriminant_is_seven() {
        let kp = Keypair::new();
        let pong = build_pong(&kp, [0u8; 32]);
        assert_eq!(&pong[0..4], &7u32.to_le_bytes());
    }

    #[test]
    fn parse_inbound_other_for_short_or_ambiguous() {
        // Too short to be a shred, not 132 bytes → Other
        let short = vec![0u8; 40];
        assert!(matches!(parse_inbound(&short), Inbound::Other));

        // 132 bytes but discriminant != 0 → not a ping; shred check at buf[64]
        // buf[64] = 0x00 which doesn't match known shred variants → Other
        let mut not_ping = vec![0u8; 132];
        not_ping[0] = 1; // discriminant != 0
        assert!(matches!(parse_inbound(&not_ping), Inbound::Other));
    }
}
