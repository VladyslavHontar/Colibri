# Colibri

Solana shred-to-transaction node. Joins gossip as a plain (non-staked) peer, receives shreds from Turbine, repairs the gaps, and streams the assembled transactions, entries and Alpenglow block footers over gRPC. Shred assembly lives in [Deshreder](https://github.com/VladyslavHontar/Deshreder); everything network-side lives here.

## Quick start

Prerequisites: Rust stable, `protobuf-compiler`, `cmake`, `libclang-dev`, `libssl-dev` (Linux; on macOS `brew install protobuf cmake llvm openssl`).

```bash
cargo build --release -p colibri
cp colibri.example.json colibri.json      # edit if needed
./target/release/colibri                  # ./colibri.json is picked up automatically
```

The example config uses the five mainnet-beta entrypoints, a public STUN server and a 500-slot backfill. That is enough for a machine behind a home or cloud NAT. Open UDP `8000`, `8200`, `8210` and TCP `8888` inbound.

Check it works with the bundled subscriber:

```bash
cargo run -p colibri --example subscribe -- --url http://127.0.0.1:8888
```

## Where does my node live?

Turbine sends shreds to the address Colibri **advertises** in gossip. Pick the one that matches your host:

| Host | What to set |
|------|-------------|
| Real public IP on the interface (bare metal, most VPS) | `"ip": "203.0.113.10"` |
| Behind a NAT (home router, cloud with a private interface, Docker) | `"stun": "stun.l.google.com:19302"` |

With `stun` Colibri asks the STUN server what address it sees, **from the very socket that will receive**, and advertises that mapping for gossip and TVU. A keep-alive re-probes every 20 s so the router never expires the TVU mapping (TVU never transmits on its own). Without it a NATed node joins gossip cleanly, shows up in peer tables, and then receives nothing.

Limits: STUN only helps behind a full-cone NAT (endpoint-independent mapping and filtering). An address-restricted NAT passes the probe and still drops turbine. If gossip peers appear but no shreds arrive, that is the first thing to check. `ip` and `stun` are mutually exclusive; `ip` wins if both are given.

## Configuration

`./colibri.json` is loaded when present, `--config <path>` names another file. Keys are the flags below without dashes (underscores for hyphens); `entrypoints` is an array. A flag on the command line overrides the file.

| Key / flag | Default | Meaning |
|------------|---------|---------|
| `ip` / `--ip` | — | Public IP to advertise (required unless `stun` is set) |
| `stun` / `--stun` | — | STUN server; advertise the NAT mapping instead of `ip` |
| `port` / `--port` | `8000` | Gossip UDP port |
| `tvu_port` / `--tvu-port` | `8200` | Port where Turbine shreds arrive |
| `repair_port` / `--repair-port` | `8210` | UDP port for repair responses |
| `entrypoints` / `--entrypoint` | five mainnet-beta entrypoints | Gossip entrypoints; repeatable flag, replaces the file's list |
| `shred_version` / `--shred-version` | fetched | Override the network shred version |
| `rpc` / `--rpc` | `http://api.mainnet-beta.solana.com` | RPC for stake and leader schedule |
| `grpc_port` / `--grpc-port` | `8888` | gRPC listen port |
| `auth_token` / `--auth-token` | — | Bearer token subscribers must send |
| `tls_cert`, `tls_key` / `--tls-cert`, `--tls-key` | — | PEM pair; both set = TLS on |
| `keypair` / `--keypair` | — | Identity keypair file, created if missing (stable gossip identity) |
| `depth` / `--depth` | `500` | Backfill depth from tip when no subscriber sends `from-slot` |
| `window` / `--window` | `64` | Slots repaired in parallel |
| `top_peers` / `--top-peers` | `128` | Distinct peers repair requests are spread across |
| `tier1_fanout` / `--tier1-fanout` | `200` | Stake table size for repair peer scoring |
| `oracle_rpc` / `--oracle-rpc` | — | Cross-check completed slots against `getBlock` (debug) |

`colibri --help` prints the same list.

## gRPC API

Proto: [`colibri/protos/shredstream.proto`](colibri/protos/shredstream.proto). Three server streams:

- `SubscribeTransactions` — one `Transaction {slot, signature, raw_tx, complete}` per transaction; `raw_tx` is a bincode `VersionedTransaction`.
- `SubscribeEntries` — one `Entry {slot, entries, complete}` per contiguous batch; `entries` is a bincode `Vec<solana_entry::Entry>`. A slot the leader never produced arrives as an empty blob with `complete = true`, so in-order consumers can advance past it.
- `SubscribeFooters` — Alpenglow block footers `{slot, bank_hash, producer_time_nanos}`: the leader's own bank hash and wall clock, straight from the block. Under Alpenglow this is the only in-band source of both.

`complete = true` marks the emission that finished a slot. Metadata headers: `authorization: Bearer <token>` when `auth_token` is set, and an optional `from-slot` with the subscriber's replay frontier; Colibri then backfills complete blocks from that slot to the tip instead of `tip - depth`.

## How it works

1. **Gossip** (`shred-net`) joins the cluster and learns validators and their stake.
2. **TVU** receives Turbine shreds; each one is verified against the slot leader's signature, fail-closed.
3. **Assemble** feeds turbine and repair shreds into one in-memory deshredder (Reed-Solomon recovery). Entry batches stream out as soon as they are contiguous.
4. **Repair** asks the deshredder what is missing and drives the repair protocol until every targeted slot is complete, marking never-produced slots as skipped.
5. **gRPC** fans the result out to subscribers.

## Docker

The build context is the directory that holds both `Colibri/` and `Deshreder/`:

```bash
# from the parent of Colibri/ and Deshreder/
GOSSIP_IP=203.0.113.10 docker compose -f Colibri/docker-compose.yml up --build
```

Docker Compose still passes `--ip`; on a NATed host edit the `command` to use `--stun stun.l.google.com:19302` instead.

## Coverage

A non-staked peer sits at the edge of the Turbine tree and receives only part of every block from turbine; repair fills the rest, so complete slots arrive later than on a staked node. Coverage depends on how many peers accept repair requests from you and on your position in the tree.
