# Solana Repair Wire Format Reference

Extracted from agave v3.1.5 (`core/src/repair/serve_repair.rs`) and
cross-checked against Colibri's working `repair_window_index` implementation
(`colibri/src/main.rs:281-306`).

All integers are little-endian (bincode default).
All byte counts listed below are for **bincode serialization** of the
respective types; bincode uses fixed-width LE for primitives, no length
prefix for fixed arrays.

---

## Common size constants

| Symbol                           | Value | Source |
|----------------------------------|-------|--------|
| `PUBKEY_BYTES`                   | 32    | confirmed — `solana_pubkey` crate; assert at `accounts-db/src/pubkey_bins.rs:10` |
| `HASH_BYTES`                     | 32    | confirmed — `solana_hash` crate |
| `SIGNATURE_BYTES`                | 64    | confirmed — `solana_signature` crate |
| `REPAIR_PING_TOKEN_SIZE`         | 32    | confirmed — `serve_repair.rs:94` (`= HASH_BYTES`) |
| `SIZE_OF_NONCE` (`Nonce = u32`)  | 4     | confirmed — `ledger/src/shred.rs:103-105` |

---

## RepairProtocol enum discriminants

Defined at `serve_repair.rs:430-475`. Bincode serializes enums as a 4-byte
LE discriminant followed by the variant payload. The discriminant is the
**zero-based ordinal position** in the enum definition.

| Ordinal | Variant name              | Notes |
|---------|---------------------------|-------|
| 0       | `LegacyWindowIndex`       | confirmed — first variant |
| 1       | `LegacyHighestWindowIndex`| confirmed |
| 2       | `LegacyOrphan`            | confirmed |
| 3       | `LegacyWindowIndexWithNonce` | confirmed |
| 4       | `LegacyHighestWindowIndexWithNonce` | confirmed |
| 5       | `LegacyOrphanWithNonce`   | confirmed |
| 6       | `LegacyAncestorHashes`    | confirmed |
| 7       | `Pong(ping_pong::Pong)`   | confirmed — also verified by `sample()` match arm `7 =>` at `serve_repair.rs:487` |
| 8       | `WindowIndex { header, slot, shred_index }` | confirmed — also verified by `sample()` match arm `8 =>` at `serve_repair.rs:494` |
| 9       | `HighestWindowIndex { header, slot, shred_index }` | confirmed — also verified by `sample()` match arm `9 =>` at `serve_repair.rs:499` |
| 10      | `Orphan { header, slot }` | confirmed — also verified by `sample()` match arm `10 =>` at `serve_repair.rs:504` |
| 11      | `AncestorHashes { header, slot }` | confirmed |
| 12      | `ParentAndFecSetCount { header, slot, block_id }` | confirmed |
| 13      | `FecSetRoot { header, slot, block_id, fec_set_index }` | confirmed |
| 14      | `WindowIndexForBlockId { header, slot, shred_index, fec_set_merkle_root, block_id }` | confirmed |

**Cross-check with Colibri:** Colibri writes `3u32.to_le_bytes()` at `buf[0..4]`
(`main.rs:290`) for `WindowIndex`. Per the table above `WindowIndex` = ordinal
8, **not** 3. This means Colibri's tag `3` sends `LegacyWindowIndexWithNonce`
(discriminant 3) rather than the modern `WindowIndex` (discriminant 8).
Despite this, Colibri reportedly receives repair responses — see the signed-data
discrepancy section below for the full analysis.

---

## RepairRequestHeader layout

Struct at `serve_repair.rs:373-379`:

```rust
pub struct RepairRequestHeader {
    signature: Signature,   // [u8; 64]
    sender:    Pubkey,      // [u8; 32]
    recipient: Pubkey,      // [u8; 32]
    timestamp: u64,         // 8 bytes LE
    nonce:     Nonce,       // u32, 4 bytes LE
}
```

Bincode field order matches struct declaration order (no reordering).
Total header size: 64 + 32 + 32 + 8 + 4 = **140 bytes**.
(confirmed — struct field order at `serve_repair.rs:374-378`)

---

## 1. WindowIndex request (discriminant 8)

**Total length: 160 bytes**

| Bytes     | Field            | Size | Source |
|-----------|------------------|------|--------|
| `[0..4]`  | discriminant = 8 | 4    | confirmed — `serve_repair.rs:430,439`; `sample()` arm at line 494 |
| `[4..68]` | `header.signature` | 64 | confirmed — first field of `RepairRequestHeader` at `serve_repair.rs:374` |
| `[68..100]` | `header.sender` | 32 | confirmed — `serve_repair.rs:375`; matches Colibri `buf[68..100]` at `main.rs:291` |
| `[100..132]` | `header.recipient` | 32 | confirmed — `serve_repair.rs:376`; matches Colibri `buf[100..132]` at `main.rs:292` |
| `[132..140]` | `header.timestamp` | 8 | confirmed — `serve_repair.rs:377`; matches Colibri `buf[132..140]` at `main.rs:293` |
| `[140..144]` | `header.nonce` (u32) | 4 | confirmed — `serve_repair.rs:378`; matches Colibri `buf[140..144]` at `main.rs:294` |
| `[144..152]` | `slot` (u64) | 8 | confirmed — `serve_repair.rs:441`; matches Colibri `buf[144..152]` at `main.rs:295` |
| `[152..160]` | `shred_index` (u64) | 8 | confirmed — `serve_repair.rs:442`; matches Colibri `buf[152..160]` at `main.rs:296` |

**Signed bytes (agave canonical):** `payload[0..4] ++ payload[68..]`
= discriminant(4) ‖ sender(32) ‖ recipient(32) ‖ timestamp(8) ‖ nonce(4) ‖ slot(8) ‖ shred_index(8)
= **96 bytes** total.
(confirmed — `repair_proto_to_bytes` at `serve_repair.rs:1801`)

---

## 2. HighestWindowIndex request (discriminant 9)

**Total length: 160 bytes** (identical layout to WindowIndex, different discriminant)

| Bytes     | Field            | Size | Source |
|-----------|------------------|------|--------|
| `[0..4]`  | discriminant = 9 | 4    | confirmed — `serve_repair.rs:444`; `sample()` arm at line 499 |
| `[4..68]` | `header.signature` | 64 | confirmed — `serve_repair.rs:374` |
| `[68..100]` | `header.sender` | 32 | confirmed |
| `[100..132]` | `header.recipient` | 32 | confirmed |
| `[132..140]` | `header.timestamp` | 8 | confirmed |
| `[140..144]` | `header.nonce` (u32) | 4 | confirmed |
| `[144..152]` | `slot` (u64) | 8 | confirmed — `serve_repair.rs:446` |
| `[152..160]` | `shred_index` (u64) | 8 | confirmed — `serve_repair.rs:447` |

**Signed bytes (agave canonical):** same construction as WindowIndex —
`payload[0..4] ++ payload[68..]` = 96 bytes.

---

## 3. Orphan request (discriminant 10)

**Total length: 152 bytes** (no `shred_index` field)

| Bytes     | Field             | Size | Source |
|-----------|-------------------|------|--------|
| `[0..4]`  | discriminant = 10 | 4    | confirmed — `serve_repair.rs:449`; `sample()` arm at line 504 |
| `[4..68]` | `header.signature` | 64  | confirmed |
| `[68..100]` | `header.sender` | 32  | confirmed |
| `[100..132]` | `header.recipient` | 32 | confirmed |
| `[132..140]` | `header.timestamp` | 8  | confirmed |
| `[140..144]` | `header.nonce` (u32) | 4 | confirmed |
| `[144..152]` | `slot` (u64) | 8      | confirmed — `serve_repair.rs:451` |

**Signed bytes (agave canonical):** `payload[0..4] ++ payload[68..]`
= discriminant(4) ‖ sender(32) ‖ recipient(32) ‖ timestamp(8) ‖ nonce(4) ‖ slot(8)
= **88 bytes** total.

---

## 4. RepairResponse::Ping (inbound, on repair socket)

Agave sends a `Ping` challenge before serving repair requests to new peers.
This is received **inbound** on the same repair UDP socket.

Struct: `RepairResponse` enum at `serve_repair.rs:530-533`:
```rust
pub(crate) enum RepairResponse {
    Ping(Ping),   // Ping = ping_pong::Ping<REPAIR_PING_TOKEN_SIZE> = Ping<32>
}
```

**Total length: 132 bytes** (`REPAIR_RESPONSE_SERIALIZED_PING_BYTES`)
(confirmed — `serve_repair.rs:98-99`: `4 + PUBKEY_BYTES + REPAIR_PING_TOKEN_SIZE + SIGNATURE_BYTES = 4+32+32+64`)

| Bytes      | Field                 | Size | Source |
|------------|-----------------------|------|--------|
| `[0..4]`   | discriminant = 0 (only variant of `RepairResponse`) | 4 | confirmed — `serve_repair.rs:532` |
| `[4..36]`  | `Ping.from: Pubkey`   | 32   | confirmed — `ping_pong.rs:32` |
| `[36..68]` | `Ping.token: [u8; 32]`| 32   | confirmed — `ping_pong.rs:33-34` (`REPAIR_PING_TOKEN_SIZE = HASH_BYTES = 32`) |
| `[68..132]`| `Ping.signature: Signature` | 64 | confirmed — `ping_pong.rs:35` |

**Recognition:** agave identifies a Ping by checking
`packet.meta().size == REPAIR_RESPONSE_SERIALIZED_PING_BYTES` (132) first,
then deserializing as `RepairResponse` and calling `ping.verify()`.
(confirmed — `serve_repair.rs:1760-1773`)

**Token:** the 32-byte token at bytes `[36..68]` is what must be hashed to
construct the Pong reply.

---

## 5. Pong outbound (RepairProtocol::Pong, discriminant 7)

To reply to an inbound Ping, send a `RepairProtocol::Pong(pong)` back to
the peer's repair address.

`Pong` struct at `ping_pong.rs:40-44`:
```rust
pub struct Pong {
    from:      Pubkey,     // [u8; 32]  — our pubkey
    hash:      Hash,       // [u8; 32]  — SHA-256("SOLANA_PING_PONG" ++ token)
    signature: Signature,  // [u8; 64]  — sign(hash.as_ref())
}
```

**Total length: 132 bytes**
(`REPAIR_REQUEST_PONG_SERIALIZED_BYTES` = PUBKEY_BYTES + HASH_BYTES + SIGNATURE_BYTES = 32+32+64 = 128,
plus the 4-byte discriminant for the outer `RepairProtocol` wrapper = **132 bytes total**)
(confirmed — `serve_repair.rs:517`)

| Bytes       | Field                              | Size | Source |
|-------------|----------------------------------  |------|--------|
| `[0..4]`    | discriminant = 7 (`RepairProtocol::Pong`) | 4 | confirmed — `serve_repair.rs:438`; `sample()` arm 7 at line 487 |
| `[4..36]`   | `Pong.from: Pubkey` — sender's pubkey | 32 | confirmed — `ping_pong.rs:41` |
| `[36..68]`  | `Pong.hash: Hash` — `SHA-256("SOLANA_PING_PONG" ++ inbound_token)` | 32 | confirmed — `ping_pong.rs:112-113`; hash function at `ping_pong.rs:292-293` |
| `[68..132]` | `Pong.signature: Signature` — `sign(hash.as_ref())` with our keypair | 64 | confirmed — `ping_pong.rs:117` |

**Construction (`Pong::new`)** confirmed at `ping_pong.rs:111-118`:
1. `hash = SHA-256("SOLANA_PING_PONG" ++ ping.token)`
2. `signature = keypair.sign_message(hash.as_ref())`  ← signs the 32-byte hash directly
3. `from = keypair.pubkey()`

**Agave production path:**
```
inbound RepairResponse::Ping(ping) → Pong::new(&ping, keypair) →
RepairProtocol::Pong(pong) → bincode::serialize(&pong) → send on repair socket
```
(confirmed — `serve_repair.rs:1776-1779`)

---

## Signed-bytes discrepancy: Colibri vs agave canonical

### Agave canonical (all modern request types)

`repair_proto_to_bytes` at `serve_repair.rs:1798-1804`:
```rust
let signable_data = [&payload[..4], &payload[4 + SIGNATURE_BYTES..]].concat();
```
= `discriminant(4) ‖ <everything after signature field>` of the full serialized packet.

For WindowIndex this equals **96 bytes**:
`discriminant(4) ‖ sender(32) ‖ recipient(32) ‖ timestamp(8) ‖ nonce(4) ‖ slot(8) ‖ shred_index(8)`

Agave's verifier (`verify_signed_packet`, `serve_repair.rs:1317-1335`) uses
exactly the same slice construction and calls `header.signature.verify(from_id, &signed_data)`.

### Colibri current (`main.rs:298-303`)

```rust
let mut sign_data = [0u8; 76];
sign_data[0..32].copy_from_slice(keypair.pubkey().as_ref());   // sender
sign_data[32..64].copy_from_slice(recipient);                   // recipient
sign_data[64..72].copy_from_slice(&ts.to_le_bytes());           // timestamp
sign_data[72..76].copy_from_slice(&nonce.to_le_bytes());        // nonce
```

This is **76 bytes**: sender(32) ‖ recipient(32) ‖ timestamp(8) ‖ nonce(4).
Missing: the 4-byte discriminant prefix and the 16-byte `slot ‖ shred_index` suffix.

### Also: the discriminant tag

Colibri writes `3u32` at `buf[0..4]` (`main.rs:290`), sending discriminant 3
= `LegacyWindowIndexWithNonce`. The modern `WindowIndex` variant has discriminant 8.
`LegacyWindowIndexWithNonce` deserialization format is not documented in the enum
(it's a unit variant — no fields after the discriminant), so the extra 156 bytes
would be ignored or cause a deserialization error.

### Hypothesis: why Colibri "works"

One possible explanation: validators may respond even when signature verification
fails, because repair responses are best-effort and the validator may not strictly
gate on sig-verify for all request types, or may have a fallback path. Another
possibility is that Colibri is sending to validators running an older agave version
or a fork with different verification. **This is inferred — verify live.**

**Action for Tasks 1–3:** The correct implementation for Tasks 1–3 must follow
the agave canonical format:
- Discriminant 8 for WindowIndex, 9 for HighestWindowIndex, 10 for Orphan
- Signed bytes = `discriminant(4) ‖ sender(32) ‖ recipient(32) ‖ timestamp(8) ‖ nonce(4) ‖ trailing-fields`
- The live validation run should confirm that the correctly-signed modern format
  produces repair responses and cross-check whether Colibri's legacy format also
  still works

---

## Summary table

| Message              | Direction | Discriminant | Total bytes | Signed bytes |
|----------------------|-----------|:------------:|:-----------:|:------------:|
| WindowIndex          | outbound  | 8            | 160         | 96 (discr+header+slot+shred_index) |
| HighestWindowIndex   | outbound  | 9            | 160         | 96 (discr+header+slot+shred_index) |
| Orphan               | outbound  | 10           | 152         | 88 (discr+header+slot) |
| RepairResponse::Ping | inbound   | 0            | 132         | n/a (we verify it, then pong) |
| RepairProtocol::Pong | outbound  | 7            | 132         | hash.as_ref() (32 bytes, by Pong::sign) |

All "confirmed" fields verified against agave v3.1.5,
`core/src/repair/serve_repair.rs` and `gossip/src/ping_pong.rs`.
