//! `shred-net`: taking part in Solana's shred network as an unstaked node and
//! rebuilding whole blocks from it.
//!
//! - [`gossip`]: join gossip, advertise a contact, score repair peers, and
//!   read what each node says it runs.
//! - [`wire`]: the repair socket's codec: the ping-pong responder and the
//!   signed repair requests.
//! - [`repair`]: which request a slot needs next (window, highest window,
//!   orphan).
//! - [`reconstruct`]: slots rebuilt on `deshredder`: FEC recovery, entry
//!   batches as they complete, repair until full.
//! - [`runner`]: [`ShredNet`], the threads that tie these together and stream
//!   entry batches, turbine and repair alike, to a [`BlockSink`].

// agave marks the gossip and contact-info APIs deprecated pending its
// `agave-unstable-api` gate; they are what there is.
#![allow(deprecated)]

pub mod gossip;
pub mod reconstruct;
pub mod repair;
pub mod stun;
pub mod runner;
pub mod wire;

pub use runner::{BlockSink, ShredNet, ShredNetConfig, DISPATCH_PASS};
