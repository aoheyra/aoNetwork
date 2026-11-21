# aoNetwork

**The link layer of the aoheyra mesh.** A sans-IO Rust crate that turns a raw Bluetooth LE radio (or an in-memory simulator) into reliable, prioritised, abuse-resistant links between neighbouring phones.

> **Status: pre-alpha, M0 (Foundations).** NET-01 (scaffold, naming, MSRV, CI), NET-02 (core types, errors, validated config), NET-04 (frame codec), NET-05 (fragmenter and reassembler), NET-06 (beacon codec), NET-07 (fuzz targets), NET-08 (transport trait), NET-09 (simulator), NET-10 (link state machine), NET-11 (send queue), NET-12 (network facade), NET-13 (keepalive and link metrics), NET-14 (native transport), NET-15 (peer table), NET-16 (topology policy), NET-17 (duplicate-link resolution), NET-18 (inbound abuse protection), NET-19 (discovery modes), NET-20 (simulator scenario suite), NET-21 (send-queue coalescing and expiry), NET-22 (opportunistic links), NET-23 (observability), NET-24 (benchmarks and memory budget) and NET-25 (credit-based flow control) are implemented; NET-03 (native contract and GATT layout) is specified in [`docs/native-contract.md`](docs/native-contract.md); everything else below describes the design in the backlog (issue IDs `NET-xx`) and is not implemented until its issue is closed. See [Roadmap](#roadmap).

Cargo package name: `ao_network` (the repo name keeps its capitals; the package is snake_case).

---

## Where this crate sits

```
                aoheyra  (app: Rust core + Kotlin UI)
                   │  depends on all three
        ┌──────────┼───────────┐
        ▼          ▼           ▼
    aoMesh ───▶ aoNetwork    aoIdents
        │                       ▲
        └───────────────────────┘

aoNetwork and aoIdents never depend on each other.
```

aoNetwork knows about **radio peers and frames**. It does not know what a message, a contact, a mesh packet or a cryptographic identity is.

## What it does

- Discovers neighbours through BLE advertising and scan-response **beacons** and decides who to connect to, through a bounded **peer table** that evicts the unseen after 30 s and backs off after a failed attempt.
- Picks a **discovery mode** from its degree and battery: aggressive (low-latency scanning and advertising) below the stable target, normal (balanced) at it, and quiet (low power, scanning in 10 s windows with 50 s pauses) while the beacon says the battery is low or the app says the node is idle. A mode change is requested at most once per `discovery_interval` (10 s), well inside Android's limit on scan starts.
- Runs a **link state machine** (`Connecting → Negotiating → Handshaking → Ready → Closing → Closed`) with an unauthenticated `Hello` exchange.
- **Fragments and reassembles** frames over the BLE MTU (23 to 517).
- Schedules traffic through a **four-channel send queue** with per-link byte caps, coalescing and expiry.
- Applies **credit-based flow control** when both `Hello`s offer it (`CAP_CREDIT`, on by default): each receiver grants a window per channel and grants bytes back as it hands frames on, a frame waits until its channel has credit for all of it, and a peer that sends past what it was granted is a protocol violation. A receiver that stops reading stops its senders instead of filling its radio queue: what peers may have sent a node and it has not read is bounded by its receive windows, per link, and a bounded radio queue (`NativeConfig::max_pending_events`) sized to cover them at the link's MTU never overflows.
- Enforces the **degree cap** (how many links a phone keeps): admission fills stable slots up to `max_stable` (the stable target plus one) and then the single explore slot, candidates are scored by signal and by the peer's degree so the best neighbour is dialled first, a node over its target sheds its weakest degraded link (below `min_rssi` and past a 10 s minimum age) while strong links are kept, inbound links meet the same slots and floor, and three rejected attempts in a row lower the session's cap. With no slot free the node stops advertising `accepting`, so nobody dials it into a full table.
- Keeps **one link per neighbour**: of each pair, the side whose beacon hint sorts lower dials at once and the other waits 4 s first. If both connections still complete their handshakes, the one opened by the lower link tag is kept and the other closed with `Duplicate`; a peer presenting this node's own tag is dropped the same way.
- Takes **advisory per-peer link hints** from aoMesh through `set_link_hint(peer, preference)`: `Prefer` dials a peer before unhinted ones and without the 4 s initiation delay, `Deprioritize` puts it after them, and `Neutral` removes the advice. Advice is bounded by `max_peers`, survives the peer going out of range, and is dropped by lowest preference then greatest `PeerKey` when full. aoMesh only says which neighbours would help; this crate alone admits links and enforces the cap.
- Opens **opportunistic links** for the layer above to sync over: once at its stable target, a node gives a peer whose beacon digest is new to it (a node passing by) the single opportunistic slot. The link ends on `release(link)` or after a 60 s dwell, and the peer's hint is then barred from new links for 60 s unless its digest changes again.
- Detects dead links (**keepalive**) and tracks per-link metrics (RSSI, RTT, bytes, drops).
- Protects itself from hostile neighbours with **per-link rate limits, strikes and a temporary blacklist**. Every neighbour is treated as hostile: a peer that sends more frames or bytes than its link allows is disconnected with `AbuseDetected` and blacklisted, and malformed frames, broken chunk streams and oversize `Hello`s are strikes, three of which blacklist the peer too. A blacklisted peer is neither dialled nor accepted until its entry expires.
- Is **observable**: each link logs its lifecycle through `tracing` under a `link{id peer role class}` span, and `Network::snapshot(now)` returns the discovery mode, every link with its metrics and queued bytes, and cumulative counters (links up, links down by reason, failed connects, queue drops, refused sends, strikes, blacklistings) for a debug screen.
- Exposes a single sans-IO façade (`Network`) that the caller polls from its own async task, and a **native contract** (`NativeSink`) that the Kotlin BLE module implements.

## What it deliberately does not do

| Not here | Lives in |
|---|---|
| Node identity, keys, signatures, encryption, tag generation | `aoIdents` |
| Packet headers, TTL, routing, store-and-forward, dedupe | `aoMesh` |
| App payloads, persistence, UI, UniFFI/FFI | `aoheyra` |
| Android BLE code (GATT, advertiser, scanner) | Kotlin `:ble` module in `aoheyra/android` |

The rotating link tag is generated by aoIdents and passed down by aoMesh. aoNetwork carries it in `Hello` and uses it **only** for duplicate-link tie-breaking.

## Architecture

```
 aoMesh adapter / examples
          │  send, broadcast, links(), set_local_hello, set_beacon,
          │  set_link_hint (advice only)
          ▼
 ┌───────────────────────────────────────────────┐
 │ Network façade: owns all state; commands in,  │
 │  events out of poll_event(cx, now); timers    │
 │  due by next_deadline()                       │
 ├───────────────────────────────────────────────┤
 │ Topology: peer table · policy · link manager  │
 │           discovery mode · opportunistic links│
 ├───────────────────────────────────────────────┤
 │ Link: state machine · handshake · keepalive   │
 │       duplicate resolution · abuse guard      │
 ├───────────────────────────────────────────────┤
 │ Send queue (Control/Message/Telemetry/Bulk)   │
 │ Fragmenter · Reassembler · Frame codec        │
 ├───────────────────────────────────────────────┤
 │ Transport trait                               │
 │   ├─ SimWorld   (feature `sim`, default)      │
 │   └─ NativeTransport → NativeSink (Kotlin)    │
 └───────────────────────────────────────────────┘
```

### Channels

| Channel | Scheduling | Typical use |
|---|---|---|
| `Control` | strict priority | link management, acks |
| `Message` | deficit round-robin (~4) | direct and broadcast packets |
| `Telemetry` | deficit round-robin (~2), coalescing, oldest waiting frames evicted under pressure | latest-wins data such as location |
| `Bulk` | deficit round-robin (~1) | sync transfers |

### Frames and wire format

Hand-rolled, big-endian, version and type bytes plus canonical LEB128 lengths. **No serde at this layer**: the wire format must stay stable.

`Frame { Hello, Data{channel,payload}, Ping, Pong, Bye, Credit{channel,credits} }`

`Hello { proto_min, proto_max, caps, max_frame, tag, app_hello ≤ 256 B }`. The `app_hello` blob is opaque here; aoMesh puts a signed `MeshHello` inside it.

`Frame::encode` and `Frame::decode` implement the frame codec with typed
`FrameError` failures; decode checks every length against the input and the
receiver's `max_frame` before allocating. The version-1 layout is recorded in
[the NET-04 ADR](docs/adr/net-04-frame-wire-format.md), and the `Credit` body
and flow-control rules in [the NET-25 ADR](docs/adr/net-25-credit-flow-control.md).

Fragmentation uses a 1-byte chunk header (START, END, 3-bit sequence). Payload per chunk is `MTU − 3 − 1`. One frame is in flight per link. `Fragmenter` and `Reassembler` implement it; the chunk rules are recorded in [the NET-05 ADR](docs/adr/net-05-chunk-format.md).

Beacon (16 bytes, manufacturer data in the scan response): version, flags (accepting / relay-willing / low-battery), degree, max_links, 8-byte hint, 4-byte digest. Encoding and decoding live in Rust only; Kotlin passes raw bytes.

`Beacon::encode` and `Beacon::decode` implement the payload codec with typed
`BeaconError` failures. The exact version-1 offsets, flag bits and validation
rules are recorded in [the NET-06 ADR](docs/adr/net-06-beacon-wire-format.md).
This codec operates on the extracted manufacturer payload; Android advertising
and scanning are separate work.

### Defaults (validated at construction)

| Setting | Default |
|---|---|
| `max_links` | 5 (target stable 3, max stable 4, 1 explore, 1 opportunistic) |
| `max_peers` | 64 discovered peers, evicted after 30 s unseen |
| `min_rssi` | -90 dBm |
| Handshake timeout | 5 s |
| Reassembly timeout | 10 s from the first chunk |
| Keepalive | ping after 15 s with nothing received, then every 15 s until a pong; 3 missed pongs close the link (a silent peer is closed within 60 s) |
| `max_frame` | 16 KiB |
| Send-queue cap | 128 KiB per link, split Control 16, Message 48, Telemetry 16, Bulk 48 KiB; a payload counts its encoded frame plus 64 B for the slot that holds it |
| Inbound rate limit | 50 frames/s (burst 200) and 64 KiB/s (burst 256 KiB) per link |
| Strikes | 3 strikes blacklist a peer; blacklist and strike memory last 10 min |
| `discovery_interval` | 10 s between scan or advertise mode changes |
| Flow control | on; receive window per link Control 16, Message 48, Telemetry 16, Bulk 48 KiB, each at least `max_frame` |

These may change after the BLE spike (AND-02). Source of truth is `NetworkConfig`, not this table.

### Memory budget

Heap requested at default config on a 64-bit target, measured by `tests/memory_budget.rs` (allocator overhead not included). Each link is loaded to the worst it admits: every send-queue channel full, from empty payloads up to the largest, behind a largest frame being written at the smallest MTU, with a largest frame half received.

| | Ceiling | Measured worst |
|---|---|---|
| One link | 288 KiB | 258 KiB |
| A node: 5 links, a full blacklist and a full peer table | 1.5 MiB | 1.26 MiB |
| `NativeTransport`: 1024 pending events of 514 B and 64 scan results | 640 KiB | 568 KiB |

A link holds its send queue (the 128 KiB its channel caps charge, plus the spare slots of its ring buffers), the frame being written, already cut into chunks, and at most one partial frame of up to `max_frame`. The ceilings scale with `queue_cap`, `max_frame`, `max_links`, `max_peers` and `NativeConfig`. `cargo test` fails when a change exceeds one; a ceiling is raised here and in the test together, with the reason.

## Features

| Feature | Default | Meaning |
|---|---|---|
| `sim` | yes | In-memory `SimWorld` transport; use it as a `dev-dependency` from aoMesh and aoheyra. Seeded and driven by a paused clock (`SimWorld::advance`), so a run reproduces exactly |
| `native` | no | `NativeTransport` adapter for the Kotlin BLE shim. No UniFFI dependency in this crate |
| `serde` | no | `Serialize` for `Network::snapshot` and the types in it, for a diagnostics bundle. Never used for the wire format |

## API

`Network` reads no clock and spawns nothing. The caller owns it in one task,
passes the current time in, and polls again by `next_deadline()`.
`cargo run --example sim_chat` runs three simulated nodes.

```rust
let mut net = Network::new(config, transport);
net.set_local_hello(tag, app_hello);
net.set_beacon(hint, digest, flags);
// Optional advice from aoMesh: this peer is a contact worth linking now.
net.set_link_hint(peer, LinkHintPreference::Prefer);

loop {
    // Race this against a sleep until net.next_deadline().
    match net.next_event(now()).await {
        NetworkEvent::LinkUp { link, remote_hello } => { /* hand to aoMesh */ }
        NetworkEvent::Data { link, channel, payload } => { /* hand to aoMesh */ }
        NetworkEvent::LinkDown { link, reason } => { /* ... */ }
        _ => {}
    }
}
```

`send`, `broadcast`, `disconnect(link, reason)` and `links` are plain calls on the same value. Every
`SendError` returns the payload to the caller. Successful queue admission is not
a delivery acknowledgement: queued payloads are discarded if their link closes.

`set_link_hint(peer, preference)`, `clear_link_hint(peer)`, `link_hint(peer)` and
`link_hints()` manage the advisory preferences. They are keyed by the opaque
radio `PeerKey`, never by an identity tag, and do not change `set_beacon` or the
`Hello` tag. `links()` and `snapshot()` report `up_at`, the injected time each
handshake finished, so a seeded simulator run can compare time-to-link with
hints off and on.

`disconnect` closes a link through its state machine, sending `Bye` on a
connected link or cancelling a connection attempt. An up link reports one
`LinkDown` with the caller's reason after transport confirmation; the peer sees
`Bye`. Unknown or ended links are ignored, repeated closes retain the first
reason, and sends fail once closing begins. An in-flight frame finishes before
`Bye`; payloads still queued are discarded when the link ends.

## Design rules

- **Sans-IO and deterministic.** Time and randomness are injected. Same seed, same run, bit for bit.
- **Never panic on external input.** Every length field is bounded; malformed input returns a typed error.
- **One owner per concept.** Rate limiting here is per-link, protocol-blind (bytes and frames). Per-origin relay budgets belong to aoMesh.
- **Coalescing and expiry** are mechanisms here (`coalesce_key`, `expires_at`). aoMesh chooses keys and lifetimes.
- **aoMesh advises, aoNetwork enforces.** Link hints only order peers for dialing and can skip the initiation delay; slots, admission floors, eviction and `max_links` are enforced here. A hint cannot add a candidate, reserve a slot or bypass the cap.
- **Kotlin never blocks the Rust runtime.** All `NativeSink` calls return immediately.

## Development

Requires Rust 1.85+ (edition 2024; the MSRV is pinned as `rust-version` in `Cargo.toml` and enforced by CI).

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-features
cargo deny check
cargo doc --no-deps --all-features
cargo bench                             # criterion benches in benches/

# Instruction counts of the same workloads, as CI compares them (Linux,
# needs valgrind)
python3 .github/scripts/instructions.py count counts.json

# 3 simulated nodes exchanging data (after NET-12)
cargo run --example sim_chat

# The scenario suite on N seeded topologies instead of CI's fixed cases
AO_SCENARIO_SEEDS=64 cargo test --release --test scenarios

# Fuzzing (nightly toolchain): beacon_decode, frame_decode, reassembler_push.
# New inputs land in the ignored fuzz/corpus; fuzz/seeds is checked in,
# and each crash input is added there once fixed.
cargo +nightly fuzz run frame_decode fuzz/corpus/frame_decode fuzz/seeds/frame_decode
```

Tests are `cargo test` only: Miri and the instruction counts run in CI, and callgrind profiling is opt-in.

CI runs these commands on every PR and on `master`: fmt, clippy (`-D warnings`), tests (Linux, macOS, Windows; default and all features), `cargo doc` (`-D warnings`), cargo-deny, an MSRV build, miri and `cargo package`. On every PR a benchmark job counts the instructions each bench workload (codec, send queue, fragmentation and reassembly) executes under callgrind, for the base and the head, and fails if any grows by more than 5%; the counts are exact, so the limit cannot flake. The memory budget is checked by `cargo test`. A scheduled fuzzer runs every target every Sunday 00:00 IST (an equal share of 45 minutes within a 60-minute job), starting from the seeds checked in under `fuzz/seeds/`, and callgrind profiling runs on manual dispatch (`.github/workflows/`). The scenario suite (`tests/scenarios.rs`) runs with `cargo test` on fixed cases of 5, 23 and 50 nodes, under 2 minutes; every night at 02:00 IST it runs on 512 seeded topologies of 5 to 50 nodes. It checks the degree cap on every tick, one link per pair after a simultaneous start, churn, partition and heal, full-node avoidance, eviction hysteresis and an abusive peer; a failure names the seed and size it replays from.

### Using sibling crates locally

aoNetwork has no sibling dependencies. Downstream crates consume it by git tag; to develop against a local checkout, they use a documented `[patch]` override (see `aoMesh` and `aoheyra` READMEs and APP-01). Do not commit a `[patch]` section.

## Roadmap

| Milestone | Issues | Outcome |
|---|---|---|
| **M0 Foundations** | NET-01..03 | Scaffold, CI, core types, native contract and GATT layout ADR |
| **M1 Wire formats** | NET-04..07 | Frame codec, fragmenter/reassembler, beacon codec, fuzz targets |
| **M2 Two phones talk** | NET-08..14 | Transport trait, `SimWorld`, link state machine, send queue, façade, keepalive, `NativeTransport` |
| **M3 Topology** | NET-15..20, 23 | Peer table, topology policy, duplicate-link resolution, abuse protection, discovery modes, scenario suite, observability |
| **M4 Latest-wins** | NET-21, 22, 27 | Coalescing and expiry, opportunistic links, docs and `v0.1.0` |
| **M6 Hardening** | NET-24..26 | Benchmarks and memory budget, credit-based flow control, L2CAP CoC spike |

Full scope and "Done when" lines for every issue are in the project backlog. Issue titles carry the ID, e.g. `[NET-04] Frame codec`.

## Contributing

1. Pick an issue; read its scope and "Done when" line.
2. Read the [rules](AGENTS.md).
3. One issue per PR, with tests that demonstrate the "Done when" line.
4. CI must be green without loosened lints, skipped tests or weakened checks.

Security issues: do not open a public issue for a vulnerability; report them privately as described in [`SECURITY.md`](SECURITY.md).

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT licence](LICENSE-MIT) at your option. Decision: APP-01, applied to all four repos.
