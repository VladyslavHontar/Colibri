# Colibri

**Live Solana transactions without a validator.** Colibri joins gossip as a plain, unstaked peer, takes shreds from Turbine, repairs what is missing, and streams what the leader produced over gRPC. One small machine is enough, even behind a home router.

```
 gossip ──┐
 turbine ─┼─▶ verify ─▶ assemble ─▶ gRPC ─▶ transactions · entries · footers · nodes
 repair ──┘
```

Shred assembly lives in [Deshreder](https://github.com/VladyslavHontar/Deshreder); everything network-side lives here.

## What you get

| Stream | What it carries |
|---|---|
| `SubscribeTransactions` | Every transaction as it is read off the shreds |
| `SubscribeEntries` | Whole entry batches, with a marker on the one that finishes a slot |
| `SubscribeFooters` | Alpenglow block footers: the leader's own bank hash and clock |
| `SubscribeNodes` | What gossip says each node runs (`Agave`, `JitoLabs`, `AgaveBam`, `Firedancer`, ...) |

Details are under [gRPC API](#grpc-api).

## Quick start

```bash
docker compose up
```

That builds the image, joins mainnet gossip and serves gRPC on `:8888`. Nothing to configure: the public address is found through STUN, and the auth token is generated on first start, printed in the log and kept in `./colibri-data`.

Open UDP `8000`, `8200`, `8210` and TCP `8888` inbound, then watch it work:

```bash
cargo run -p colibri --example subscribe -- --url http://127.0.0.1:8888 --token <token>
cargo run -p colibri --example subscribe -- --url http://127.0.0.1:8888 --token <token> --nodes
```

Optional settings go in the `environment:` block of `docker-compose.yml`: `PUBLIC_IP` (the host has a real public IP, skips STUN), `AUTH_TOKEN`, `SOLANA_RPC`, `DEPTH`.

<details>
<summary>Without Docker</summary>

Needs Rust stable, `protobuf-compiler`, `cmake`, `libclang-dev`, `libssl-dev` (on macOS: `brew install protobuf cmake llvm openssl`).

```bash
cargo build --release -p colibri
cp colibri.example.json colibri.json      # edit if needed
./target/release/colibri                  # ./colibri.json is picked up automatically
```

The example config uses the five mainnet-beta entrypoints, a public STUN server and a 500-slot backfill, which is enough behind a home or cloud NAT.

</details>

## Is it for you?

**Yes**, if you want to build or test something that reacts to live transactions, measure how far behind the leader a plain node sits, or look at blocks on the wire under Alpenglow (no vote transactions, block footers, FEC sets) without a validator's stake, a paid shred feed or a full node's disk and CPU.

**No**, if you need the speed of a staked validator or of a paid shred stream. An unstaked peer sits at the edge of the Turbine tree: it gets part of each block from Turbine and the rest by repair, so whole slots arrive later. How much you get depends on your place in the tree and on how many peers answer your repair requests.

What that looked like on mainnet, unstaked, over a home link (October 2026):

| | |
|---|---|
| Shreds of each 64-shred FEC set that came from Turbine | 40, median |
| FEC sets rebuilt without repair | 99.0% |
| Transactions behind the leader's own timestamp | ~170 ms median, ~300 ms p90 |

The last one compares the leader's clock with yours, so your clock has to be in sync. The `[coverage]` and `behind_leader_ms` log lines print the same for your host.

## Your network

Turbine sends shreds to the address Colibri **advertises** in gossip. Tell it which one that is:

| Your host | Set |
|---|---|
| A real public IP on the interface (bare metal, most VPS) | `"ip": "203.0.113.10"` |
| Behind a NAT (home router, cloud with a private interface, Docker) | `"stun": "stun.l.google.com:19302"` |

With `stun`, Colibri asks the STUN server what address it sees **from the very socket that will receive**, and advertises that mapping for gossip and TVU. A keep-alive re-probes every 20 s so the router never expires the TVU mapping, which never transmits on its own. Without it, a node behind a NAT joins gossip cleanly, shows up in peer tables, and then receives nothing.

Good to know:

- STUN helps only behind a full-cone NAT (endpoint-independent mapping and filtering). An address-restricted NAT passes the probe and still drops Turbine. **Gossip peers appear but no shreds arrive** is the sign of it.
- When both are set, `stun` wins: the probed address is advertised and `ip` is ignored.

### On a home connection

Set `"repair_rate": 1500` and a small `depth`, and keep the default `window` (64) and `top_peers` (128). Unlimited repair sends tens of thousands of requests a second.

Gossip is trimmed for such a link too. The vendored `solana-gossip` (see [`vendor/solana-gossip/PATCH.md`](vendor/solana-gossip/PATCH.md)) pushes only this node's own values, prunes every peer that pushes to it, and pulls once a minute after the first 30 s. On a home Wi-Fi link it was gossip, not repair, that lost shreds:

| | Untrimmed | Trimmed |
|---|---|---|
| Gossip packets in | ~13,900/s (~100 Mbit/s) | ~2,000/s |
| Ping loss while Colibri runs | 40-46% at 1.2-1.7 s RTT | 1% |

In a 12-minute run with the trims on that link (2026-10-19), Colibri delivered every one of 3,277 slots except the last few in flight at shutdown, and stayed a median 2 slots (p99 14) behind the RPC's processed slot. Repair needed a median 19 requests a second and reached the 1,500 cap only while catching up at the start.

## Configuration

`./colibri.json` is loaded when present; `--config <path>` names another file. Keys are the flags without dashes, with underscores for hyphens; `entrypoints` is an array. A flag on the command line overrides the file. `colibri --help` prints the same list.

| Key / flag | Default | Meaning |
|---|---|---|
| `ip` / `--ip` | — | Public IP to advertise (required unless `stun` is set) |
| `stun` / `--stun` | — | STUN server; advertise the NAT mapping instead of `ip` |
| `port` / `--port` | `8000` | Gossip UDP port |
| `tvu_port` / `--tvu-port` | `8200` | Port where Turbine shreds arrive |
| `repair_port` / `--repair-port` | `8210` | UDP port for repair responses |
| `entrypoints` / `--entrypoint` | five mainnet-beta entrypoints | Gossip entrypoints; the flag is repeatable and replaces the file's list |
| `shred_version` / `--shred-version` | fetched | Override the network shred version |
| `rpc` / `--rpc` | `http://api.mainnet-beta.solana.com` | RPC for stake and leader schedule |
| `grpc_port` / `--grpc-port` | `8888` | gRPC listen port |
| `auth_token` / `--auth-token` | — | Bearer token subscribers must send |
| `tls_cert`, `tls_key` / `--tls-cert`, `--tls-key` | — | PEM pair; TLS is on when both are set |
| `keypair` / `--keypair` | — | Identity keypair file, created if missing, for a gossip identity that survives restarts |
| `depth` / `--depth` | `500` | Backfill depth from the tip when no subscriber sends `from-slot` |
| `window` / `--window` | `64` | Slots repaired in parallel |
| `top_peers` / `--top-peers` | `128` | Distinct peers repair requests are spread across |
| `repair_rate` / `--repair-rate` | `0` | Most repair requests a second; `0` is no limit. A home link wants ~1,500 |
| `tier1_fanout` / `--tier1-fanout` | `200` | Stake table size for repair peer scoring |
| `oracle_rpc` / `--oracle-rpc` | — | Cross-check completed slots against `getBlock` (debug) |
| `stall_secs` / `--stall-secs` | `60` | Exit after this many seconds without one shred, for the supervisor to start it again; `0` never |

Why `stall_secs`: a host that slept, or a NAT mapping the router forgot, leaves a live process that hears nothing. Exiting lets `restart: unless-stopped` bring it back, with a fresh STUN probe.

## gRPC API

Proto: [`colibri/protos/shredstream.proto`](colibri/protos/shredstream.proto).

**`SubscribeTransactions`** — one `Transaction {slot, signature, raw_tx, complete}` per transaction; `raw_tx` is a bincode `VersionedTransaction`.

**`SubscribeEntries`** — one `Entry {slot, entries, complete}` per contiguous batch; `entries` is a bincode `Vec<solana_entry::Entry>`. A slot the leader never produced arrives as an empty blob with `complete = true`, so an in-order consumer can advance past it.

**`SubscribeFooters`** — `Footer {slot, bank_hash, producer_time_nanos}`: the leader's own bank hash and wall clock, straight from the block. Under Alpenglow this is the only in-band source of both.

**`SubscribeNodes`** — `Node {identity, client, version}` for every node gossip holds. The first message holds all of them; each later one holds the nodes that appeared or changed, and the identities of those gossip dropped (`gone`). What a leader runs decides how it orders a block's transactions, so look the slot's leader up here by identity. It is what a node says of itself, not something checked.

`complete = true` marks the emission that finished a slot.

Metadata headers:

- `authorization: Bearer <token>`, when `auth_token` is set.
- `from-slot: <slot>`, optional: the subscriber's replay frontier. Colibri then backfills complete blocks from that slot to the tip instead of from `tip - depth`.

## How it works

1. **Gossip** joins the cluster and learns the nodes and their stake.
2. **TVU** receives Turbine shreds. Each is verified against the slot leader's signature, and one that fails is dropped. The leader signs a FEC set's Merkle root once, so the first shred of a set is checked with ed25519 and the rest by their Merkle proof against that root, as agave does.
3. **Assemble** feeds Turbine and repair shreds into one in-memory deshredder (Reed-Solomon recovery). Entry batches stream out as soon as they are contiguous.
4. **Repair** asks the deshredder what is missing, of each FEC set only what its recovery still lacks, and drives the repair protocol until every targeted slot is complete; slots that were never produced are marked skipped. Each request goes to a peer drawn by stake, as agave's repair draws them, and never to the current leader or the next one: a leading node charges ten times the cost for what it serves.
5. **gRPC** fans the result out to subscribers.

Gossip and repair are in the `shred-net` crate, the gRPC service and the sigverify gate in `colibri`.

## License

MIT, see [LICENSE](LICENSE). `vendor/solana-gossip` is a patched copy of agave's crate and stays under agave's Apache-2.0 license (its own `LICENSE`).
