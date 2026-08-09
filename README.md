# Colibri

Solana shred-to-transaction node. Joins the gossip network as a participant, receives shreds via TVU (Turbine), repairs missing slots over the repair protocol, assembles them into transactions, and streams them over gRPC.

Assembly is done by the standalone [Deshreder](https://github.com/VladyslavHontar/Deshreder) library; all networking lives here.

## How it works

Two lanes over one ingest:

1. **Gossip** — Colibri joins the Solana gossip network and discovers validators (the `shred-net` workspace crate)
2. **TVU** — Receives raw shred packets from the Turbine tree; every shred is verified against the slot leader's signature (fail-closed)
3. **Fast lane** — Verified shreds go straight into the in-memory [deshredder](https://github.com/VladyslavHontar/Deshreder) (Reed-Solomon FEC recovery); entry batches stream out with minimal latency, `complete = true` on the batch that finishes a slot
4. **Complete lane** — `shred-net` drives the repair protocol until every targeted slot is fully reconstructed, emitting whole blocks the fast lane missed and empty-entry skip markers for slots that were never produced; the targeted range follows the consumer's `from-slot` frontier, falling back to `tip - depth`
5. **gRPC** — Streams individual transactions (and raw entries) to subscribers

## Build

```bash
cargo build --release -p colibri
```
## Usage

```bash
colibri --ip <PUBLIC_IP> --entrypoint <ADDR> [OPTIONS]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--ip <IP>` | *required* | Public IP to advertise in gossip |
| `--port <PORT>` | `8000` | Gossip UDP port |
| `--tvu-port <PORT>` | `8200` | TVU port where shreds arrive |
| `--repair-port <PORT>` | `8210` | UDP port for repair responses |
| `--entrypoint <ADDR>` | — | Solana entrypoint host:port (repeatable) |
| `--shred-version <VER>` | auto-fetched | Override shred version (auto-fetched from entrypoint at startup) |
| `--rpc <URL>` | `http://api.mainnet-beta.solana.com` | RPC endpoint for stake + leader-schedule data |
| `--tier1-fanout <N>` | `200` | Number of top-staked peers for repair requests |
| `--grpc-port <PORT>` | `8888` | gRPC server listen port |
| `--auth-token <TOKEN>` | — | Bearer token required for gRPC subscribers |
| `--tls-cert <PATH>` | — | TLS certificate PEM file (enables TLS with `--tls-key`) |
| `--tls-key <PATH>` | — | TLS private key PEM file |
| `--keypair <PATH>` | — | Path to keypair JSON (auto-created if missing, gives stable gossip identity) |
| `--depth <N>` | `6000` | Backfill depth from tip when no consumer reports `from-slot` |
| `--window <N>` | `64` | Slots repaired in parallel |
| `--top-peers <N>` | `64` | Distinct peers repair requests are spread across |

### Example

```bash
colibri \
  --ip 203.0.113.10 \
  --entrypoint entrypoint.mainnet-beta.solana.com:8001 \
  --entrypoint entrypoint2.mainnet-beta.solana.com:8001 \
  --grpc-port 8888
```

## gRPC API

Proto definition: [`colibri/protos/shredstream.proto`](colibri/protos/shredstream.proto)

### `SubscribeTransactions`

Streams individual transactions as they are assembled from shreds.

```protobuf
message Transaction {
    uint64 slot       = 1;  // slot number
    string signature  = 2;  // base58 transaction signature
    bytes  raw_tx     = 3;  // bincode-serialized VersionedTransaction
    bool   complete   = 4;  // set on the batch that finished the slot
}
```

### `SubscribeEntries`

Streams slot entry blobs (for lower-level consumers).

```protobuf
message Entry {
    uint64 slot     = 1;
    bytes  entries  = 2;  // bincode-serialized Vec<solana_entry::Entry>
    bool   complete = 3;  // set on the emission that finished the slot
}
```

A skipped slot (never produced by its leader) is emitted as an empty `entries` blob with `complete = true`, so in-order consumers can advance past it.

Both endpoints support optional bearer token authentication via the `authorization` metadata header. A subscriber may also send a `from-slot` metadata header with its replay frontier; Colibri backfills complete blocks from that slot up to the live tip.

## Subscriber example

See [`colibri/examples/subscribe.rs`](colibri/examples/subscribe.rs) for a working gRPC subscriber.

```bash
cargo run -p colibri --example subscribe -- --url http://127.0.0.1:8888
```

## Docker

Build and run with Docker Compose (build context needs both Colibri and Deshreder repos side by side):

```bash
# From the parent directory containing both Colibri/ and Deshreder/
GOSSIP_IP=203.0.113.10 docker compose -f Colibri/docker-compose.yml up --build
```

Make sure UDP ports 8000, 8200, 8210 and TCP port 8888 are open in your firewall.

## Note on coverage

As a non-staked gossip participant, Colibri sits at the edge of the Turbine tree and receives a subset of all shreds. Transaction coverage depends on network conditions and the node's position in the tree. For full transaction coverage, consider pairing with a shredstream relay or running a full RPC node.
