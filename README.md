# Colibri

Solana shred-to-transaction pipeline. Joins the gossip network as a participant, receives shreds via TVU (Turbine), assembles them into transactions using FEC recovery, and streams them over gRPC.

## How it works

1. **Gossip** — Colibri joins the Solana gossip network and discovers validators
2. **TVU** — Receives raw shred packets from the Turbine tree
3. **FEC recovery** — Assembles shreds into complete entries using Reed-Solomon erasure coding
4. **Repair** — Requests missing shreds from top-staked validators
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
| `--rpc <URL>` | `http://api.mainnet-beta.solana.com` | RPC endpoint for fetching stake data |
| `--tier1-fanout <N>` | `200` | Number of top-staked peers for repair requests |
| `--grpc-port <PORT>` | `8888` | gRPC server listen port |
| `--auth-token <TOKEN>` | — | Bearer token required for gRPC subscribers |
| `--tls-cert <PATH>` | — | TLS certificate PEM file (enables TLS with `--tls-key`) |
| `--tls-key <PATH>` | — | TLS private key PEM file |
| `--keypair <PATH>` | — | Path to keypair JSON (auto-created if missing, gives stable gossip identity) |

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
}
```

### `SubscribeEntries`

Streams raw slot entry blobs (for lower-level consumers).

```protobuf
message Entry {
    uint64 slot    = 1;
    bytes  entries = 2;  // bincode-serialized Vec<solana_entry::Entry>
}
```

Both endpoints support optional bearer token authentication via the `authorization` metadata header.

## Subscriber example

See [Deshreder/examples/subscribe.rs](https://github.com/VladyslavHontar/Deshreder/blob/main/examples/subscribe.rs) for a working gRPC subscriber.

```bash
cargo run --example subscribe -- --url http://127.0.0.1:8888
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
