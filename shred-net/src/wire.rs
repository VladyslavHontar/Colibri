//! Repair-socket wire codec: classify inbound packets, build Pong replies,
//! and encode outbound repair requests.
//!
//! Ported verbatim from Colibri's Phase-0-proven `repair_wire.rs` (the modern
//! signed repair variants + ping-pong responder that unlocked the repair
//! firehose — 478/478 pongs, 1.26M responses). Legacy tag-3 requests are
//! silently rejected by agave's `Unsigned` arm; only these modern variants
//! (disc 8/9/10) plus the ping-pong gate get served.
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
//! Outbound repair request common header (offsets shared by all three):
//!   [0..4]    discriminant (LE u32)
//!   [4..68]   signature (64 bytes, written after signing)
//!   [68..100] sender pubkey (32 bytes)
//!   [100..132] recipient pubkey (32 bytes)
//!   [132..140] timestamp (u64 LE)
//!   [140..144] nonce (u32 LE)
//!   then variant-specific trailing fields.
//!
//! Signed bytes (agave canonical, all modern request types):
//!   sign_data = buf[0..4] ++ buf[68..]   (discriminant ‖ everything after sig)

use {
    sha2::{Digest, Sha256},
    solana_keypair::Keypair,
    solana_signer::Signer,
    solana_time_utils::timestamp,
};

// ─── repair request constants ────────────────────────────────────────────────

/// Discriminant for `RepairProtocol::WindowIndex` (modern, ordinal 8).
pub const WINDOW_INDEX_DISCRIMINANT: u32 = 8;
/// Discriminant for `RepairProtocol::HighestWindowIndex` (modern, ordinal 9).
pub const HIGHEST_DISCRIMINANT: u32 = 9;
/// Discriminant for `RepairProtocol::Orphan` (modern, ordinal 10).
pub const ORPHAN_DISCRIMINANT: u32 = 10;

/// Wire length of a `WindowIndex` request (160 bytes).
pub const WINDOW_INDEX_WIRE_LEN: usize = 160;
/// Wire length of a `HighestWindowIndex` request (160 bytes).
pub const HIGHEST_WIRE_LEN: usize = 160;
/// Wire length of an `Orphan` request (152 bytes, no shred_index field).
pub const ORPHAN_WIRE_LEN: usize = 152;

/// Byte offset of `header.sender` in all outbound repair request packets.
pub const SENDER_OFF: usize = 68;
/// Byte offset of `header.recipient` in all outbound repair request packets.
pub const RECIPIENT_OFF: usize = 100;

/// Total byte length of an outbound Pong packet.
pub const PONG_WIRE_LEN: usize = 132;
/// Byte offset of the `from` pubkey field within the Pong packet.
pub const PONG_FROM_OFFSET: usize = 4;

/// Recognised packet types on the repair socket.
pub enum Inbound {
    /// Inbound `RepairResponse::Ping` — carries the 32-byte token that must be
    /// echoed back (hashed) in the Pong reply.
    Ping([u8; 32]),
    /// Inbound shred data — forward to the reconstruction sink.
    ShredResponse,
    /// Anything else — discard silently.
    Other,
}

/// Classify one inbound UDP packet.
///
/// Classification rules (from docs/repair-wire-format.md):
/// 1. `len == 132 && buf[0..4] == [0,0,0,0]`  → `Ping(token@[36..68])`
/// 2. `len >= 88` and shred-variant byte at `buf[64]` matches a known shred
///    variant  → `ShredResponse`
/// 3. everything else  → `Other`
pub fn parse_inbound(buf: &[u8]) -> Inbound {
    // Rule 1: Ping — classify by size (132) first, then 4-byte LE discriminant == 0.
    // This mirrors agave's own RepairResponse recognizer, which gates on
    // `size == REPAIR_RESPONSE_SERIALIZED_PING_BYTES` before deserializing.
    // A real shred response landing at exactly 132 bytes with a zero discriminant is
    // vanishingly unlikely (full shreds are ~1228 bytes), so the collision risk is
    // negligible and the approach matches agave's reliability.
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
    debug_assert_eq!(
        &pong[PONG_FROM_OFFSET..PONG_FROM_OFFSET + 32],
        keypair.pubkey().as_ref(),
        "from-pubkey not at expected offset"
    );
    pong
}

// ─── outbound repair request encoders ───────────────────────────────────────

/// Encode a `RepairProtocol::WindowIndex` request (discriminant 8, 160 bytes).
///
/// Wire layout per `docs/repair-wire-format.md`:
///   [0..4]    discriminant = 8
///   [4..68]   signature (computed and written last)
///   [68..100] sender pubkey
///   [100..132] recipient pubkey
///   [132..140] timestamp (u64 LE)
///   [140..144] nonce (u32 LE)
///   [144..152] slot (u64 LE)
///   [152..160] shred_index (u64 LE)
///
/// Signed bytes (agave canonical): `buf[0..4] ++ buf[68..]` = 96 bytes.
pub fn window_index(
    keypair:     &Keypair,
    recipient:   &[u8; 32],
    slot:        u64,
    shred_index: u64,
    nonce:       u32,
) -> Vec<u8> {
    let ts = timestamp();
    let mut buf = vec![0u8; WINDOW_INDEX_WIRE_LEN];
    buf[0..4].copy_from_slice(&WINDOW_INDEX_DISCRIMINANT.to_le_bytes());
    // signature region [4..68] left zero until signing
    buf[SENDER_OFF..SENDER_OFF + 32].copy_from_slice(keypair.pubkey().as_ref());
    buf[RECIPIENT_OFF..RECIPIENT_OFF + 32].copy_from_slice(recipient);
    buf[132..140].copy_from_slice(&ts.to_le_bytes());
    buf[140..144].copy_from_slice(&nonce.to_le_bytes());
    buf[144..152].copy_from_slice(&slot.to_le_bytes());
    buf[152..160].copy_from_slice(&shred_index.to_le_bytes());
    let sign_data: Vec<u8> = [&buf[0..4], &buf[68..]].concat();
    let sig = keypair.sign_message(&sign_data);
    buf[4..68].copy_from_slice(sig.as_ref());
    buf
}

/// Encode a `RepairProtocol::HighestWindowIndex` request (discriminant 9, 160 bytes).
///
/// Wire layout is identical to `window_index` but with discriminant 9.
/// Used to discover the last shred index in a slot when we hold zero shreds.
///
/// Signed bytes (agave canonical): `buf[0..4] ++ buf[68..]` = 96 bytes.
pub fn highest_window_index(
    keypair:     &Keypair,
    recipient:   &[u8; 32],
    slot:        u64,
    shred_index: u64,
    nonce:       u32,
) -> Vec<u8> {
    let ts = timestamp();
    let mut buf = vec![0u8; HIGHEST_WIRE_LEN];
    buf[0..4].copy_from_slice(&HIGHEST_DISCRIMINANT.to_le_bytes());
    // signature region [4..68] left zero until signing
    buf[SENDER_OFF..SENDER_OFF + 32].copy_from_slice(keypair.pubkey().as_ref());
    buf[RECIPIENT_OFF..RECIPIENT_OFF + 32].copy_from_slice(recipient);
    buf[132..140].copy_from_slice(&ts.to_le_bytes());
    buf[140..144].copy_from_slice(&nonce.to_le_bytes());
    buf[144..152].copy_from_slice(&slot.to_le_bytes());
    buf[152..160].copy_from_slice(&shred_index.to_le_bytes());
    let sign_data: Vec<u8> = [&buf[0..4], &buf[68..]].concat();
    let sig = keypair.sign_message(&sign_data);
    buf[4..68].copy_from_slice(sig.as_ref());
    buf
}

/// Encode a `RepairProtocol::Orphan` request (discriminant 10, 152 bytes).
///
/// Wire layout per `docs/repair-wire-format.md`:
///   [0..4]    discriminant = 10
///   [4..68]   signature (computed and written last)
///   [68..100] sender pubkey
///   [100..132] recipient pubkey
///   [132..140] timestamp (u64 LE)
///   [140..144] nonce (u32 LE)
///   [144..152] slot (u64 LE)
///
/// Signed bytes (agave canonical): `buf[0..4] ++ buf[68..]` = 88 bytes.
/// No `shred_index` field — used to pull a parent chain for a given slot.
pub fn orphan(
    keypair:   &Keypair,
    recipient: &[u8; 32],
    slot:      u64,
    nonce:     u32,
) -> Vec<u8> {
    let ts = timestamp();
    let mut buf = vec![0u8; ORPHAN_WIRE_LEN];
    buf[0..4].copy_from_slice(&ORPHAN_DISCRIMINANT.to_le_bytes());
    // signature region [4..68] left zero until signing
    buf[SENDER_OFF..SENDER_OFF + 32].copy_from_slice(keypair.pubkey().as_ref());
    buf[RECIPIENT_OFF..RECIPIENT_OFF + 32].copy_from_slice(recipient);
    buf[132..140].copy_from_slice(&ts.to_le_bytes());
    buf[140..144].copy_from_slice(&nonce.to_le_bytes());
    buf[144..152].copy_from_slice(&slot.to_le_bytes());
    let sign_data: Vec<u8> = [&buf[0..4], &buf[68..]].concat();
    let sig = keypair.sign_message(&sign_data);
    buf[4..68].copy_from_slice(sig.as_ref());
    buf
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
        // A synthesised Ping → Inbound::Ping(token)
        let ping_bytes = synth_ping([9u8; 32]);
        match parse_inbound(&ping_bytes) {
            Inbound::Ping(t) => assert_eq!(t, [9u8; 32]),
            _ => panic!("expected Ping"),
        }
        // A data-shred-shaped buffer (variant byte at [64]) → ShredResponse
        let mut shred = vec![0u8; 200];
        shred[64] = 0x80; // data shred variant
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

    // ─── outbound repair request encoder tests ───────────────────────────────

    #[test]
    fn window_index_modern_layout() {
        let kp = Keypair::new();
        let req = window_index(&kp, &[3u8; 32], 100, 5, 0xCC);
        assert_eq!(req.len(), WINDOW_INDEX_WIRE_LEN);
        assert_eq!(req.len(), 160);
        assert_eq!(&req[0..4], &WINDOW_INDEX_DISCRIMINANT.to_le_bytes());
        assert_eq!(&req[0..4], &8u32.to_le_bytes());
        assert_eq!(&req[SENDER_OFF..SENDER_OFF + 32], kp.pubkey().as_ref());
        assert_eq!(&req[RECIPIENT_OFF..RECIPIENT_OFF + 32], &[3u8; 32]);
        // slot at [144..152]
        assert_eq!(&req[144..152], &100u64.to_le_bytes());
        // shred_index at [152..160]
        assert_eq!(&req[152..160], &5u64.to_le_bytes());
    }

    #[test]
    fn highest_window_index_layout_matches_spec() {
        let kp = Keypair::new();
        let rcpt = [1u8; 32];
        let req = highest_window_index(&kp, &rcpt, 100, 5, 0xAA);
        assert_eq!(req.len(), HIGHEST_WIRE_LEN);
        assert_eq!(req.len(), 160);
        assert_eq!(&req[0..4], &HIGHEST_DISCRIMINANT.to_le_bytes());
        assert_eq!(&req[0..4], &9u32.to_le_bytes());
        assert_eq!(&req[SENDER_OFF..SENDER_OFF + 32], kp.pubkey().as_ref());
        assert_eq!(&req[RECIPIENT_OFF..RECIPIENT_OFF + 32], &rcpt);
        assert_eq!(&req[144..152], &100u64.to_le_bytes());
        assert_eq!(&req[152..160], &5u64.to_le_bytes());
    }

    #[test]
    fn orphan_layout_matches_spec() {
        let kp = Keypair::new();
        let rcpt = [2u8; 32];
        let req = orphan(&kp, &rcpt, 100, 0xBB);
        assert_eq!(req.len(), ORPHAN_WIRE_LEN);
        assert_eq!(req.len(), 152);
        assert_eq!(&req[0..4], &ORPHAN_DISCRIMINANT.to_le_bytes());
        assert_eq!(&req[0..4], &10u32.to_le_bytes());
        assert_eq!(&req[SENDER_OFF..SENDER_OFF + 32], kp.pubkey().as_ref());
        assert_eq!(&req[RECIPIENT_OFF..RECIPIENT_OFF + 32], &rcpt);
        assert_eq!(&req[144..152], &100u64.to_le_bytes());
    }

    #[test]
    fn window_index_sign_data_is_96_bytes_excl_sig() {
        let kp = Keypair::new();
        let req = window_index(&kp, &[0u8; 32], 1, 0, 0);
        assert_eq!(req.len(), 160);
        // The signature at [4..68] must not be all-zero (it was filled by sign_message)
        assert_ne!(&req[4..68], &[0u8; 64][..]);
    }

    #[test]
    fn orphan_sign_data_is_88_bytes_excl_sig() {
        let kp = Keypair::new();
        let req = orphan(&kp, &[0u8; 32], 1, 0);
        assert_eq!(req.len(), 152);
        assert_ne!(&req[4..68], &[0u8; 64][..]);
    }
}
