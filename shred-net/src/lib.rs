//! `shred-net` — Colibri's P2P "complete lane".
//!
//! NB: agave v3.1.5 marks the gossip/contact-info APIs `deprecated` pending the
//! formal `agave-unstable-api` gate in v4.0.0; we acknowledge and use them.
#![allow(deprecated)]
//!
//!
//! This crate owns the Phase-0-proven machinery for participating in Solana's
//! shred network as an unstaked node and reconstructing complete blocks:
//!
//!   - [`wire`]        — repair-socket codec: ping-pong responder + the modern
//!                       signed repair request encoders (the bytes that
//!                       unlocked the repair firehose in Phase 0).
//!   - `gossip`        — (next) join gossip, advertise ContactInfo, score peers.
//!   - `repair`        — (next) the repair driver: decide & dispatch
//!                       Window/HighestWindow/Orphan requests per slot.
//!   - `reconstruct`   — (next) agave `Blockstore`-backed slot reconstruction:
//!                       FEC + completed-data-sets + repair-until-`is_full`.
//!
//! The public surface is a `ShredNet` that, given a config + a peer source,
//! drives repair until each targeted slot reaches `is_full()` and emits the
//! complete block. Colibri wraps it as its complete lane.

pub mod gossip;
pub mod reconstruct;
pub mod repair;
pub mod runner;
pub mod wire;

pub use runner::{BlockSink, ShredNet, ShredNetConfig};
