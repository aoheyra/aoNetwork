# Changelog

## Unreleased

- Advisory per-peer link hints (NET-16): `Network::set_link_hint(peer, LinkHintPreference)`,
  `clear_link_hint`, `link_hint` and `link_hints` take typed advice from aoMesh,
  bounded by `max_peers`, keyed by the opaque `PeerKey` and never enforced as a
  cap. `Prefer` orders a peer first and skips the 4 s initiation delay;
  `Deprioritize` orders it last; `Neutral` or clearing removes advice. A full
  hint table drops the lowest preference, then the greatest key. Advice
  survives sightings and has no clock-based expiry. `LinkInfo` and
  `LinkSnapshot` gain `up_at`, and `Snapshot` gains `hints`, for time-to-link
  observability; struct literals of these public types need the new fields.

## 0.1.0 - 2026-11-21

First tagged release, for aoMesh to consume by git tag. Every entry below
is part of it.

- Docs (NET-27): every fallible `NetworkConfig` setter names the
  `ConfigError` it returns, and `NativeTransport` and `Network::snapshot`
  gain examples.
- Credit-based flow control (NET-25): `Hello.caps` bit `CAP_CREDIT`
  offers credits and a link uses them when both sides set it, which
  `NetworkConfig` does by default (`with_flow_control`). The `Credit`
  frame now carries `channel u8 | credits u32`, within frame version 1:
  no earlier build sends it or sets the bit. Each side grants its
  `receive_window` per channel on `Ready` and grants bytes back as it
  hands `Data` frames on; a frame waits until its channel has credit for
  all of it, and a peer that sends past what was written to it is closed
  with `ProtocolViolation`. `SendQueue::pop_within` takes the next frame
  that fits a per-channel allowance. Raising `max_frame` above a receive
  window is now refused with `ConfigError::ReceiveWindowOutOfRange`.
  `SimNode` gains `rx_buffer`, a bounded receive buffer that closes a
  link it cannot hold; struct literals of `SimNode` need the new field.
- Benchmarks and memory budget (NET-24): criterion benches for the frame
  and beacon codecs, the send queue, and fragmentation and reassembly,
  and an `instructions` bench that runs the same workloads under
  Callgrind; CI compares their instruction counts between base and head
  on every PR and fails above +5%. `tests/memory_budget.rs` measures a
  fully loaded node and fails above the ceilings in the README.
  Behaviour changes: each queued payload now also counts
  `SendQueue::ENTRY_COST` (64 B) against its channel cap, so a cap bounds
  the memory it holds (a queue of empty payloads used to hold about 20
  times its cap), and the minimum channel cap rises from 64 to 128 B. A
  payload's spare capacity is released when it is queued, the
  reassembly buffer never grows past `max_frame`, and `NativeTransport`
  closes a link given an `on_bytes` value longer than 514 bytes with
  `TransportLost`.
- Observability (NET-23): each link logs its lifecycle (opened, up,
  duplicate, evicted, dwell over, down with its reason, connect failed)
  through `tracing` under a `link{id peer role class}` span, and strikes,
  blacklistings, refused inbound links and discovery mode changes are
  logged too. `Network::snapshot(now)` returns a `Snapshot`: the
  discovery mode, every link as a `LinkSnapshot` (role, `LinkClass`,
  state, metrics, queued bytes), the peer table size, the blocked peer
  count and cumulative `Counters` (links up, `DownCounts` by reason,
  failed connects, `QueueDrops`, refused sends, strikes, blacklistings).
  `LinkMetrics` gains `queue_drops`, `SendQueue::drops` and
  `Link::queued_bytes` are new, and `LinkClass` is now public. The new
  `serde` feature derives `Serialize` for the snapshot and the types in
  it; `PeerKey` serializes as hex. New dependency: `tracing`.
- Simulator scenario suite (NET-20): `tests/scenarios.rs` runs the M3
  invariants on seeded random topologies - the degree cap on every tick
  under churn, one link per pair after a simultaneous start, partition
  and heal, full-node avoidance, eviction hysteresis and an abusive
  peer, each settling to a mesh where no pair in range is left unlinked
  while both ends advertise room. CI runs fixed cases of 5, 23 and 50
  nodes; `AO_SCENARIO_SEEDS=N` runs N seeded ones, and a nightly
  workflow runs 512 in release mode.
- Opportunistic links (NET-22): once a node holds its stable target, a
  peer whose beacon digest is new to it (a first sighting included, so a
  node passing by) is dialed into the single opportunistic slot, ahead of
  a fourth stable or the explore link, and an inbound link may take that
  slot when no stable or explore slot is left. The dial follows the
  hint rule and initiation delay of NET-17 and is retried on every
  sighting while the digest stays new to the node. The new
  `Network::release(now, link)` closes such a link once the layer above
  has synced over it; otherwise it ends after a 60 s dwell with
  `DegreeCap`. Its peer's hint is then barred from new links in either
  direction for 60 s unless the peer's digest changes again. Eviction no
  longer counts opportunistic links against the stable target.
- Discovery mode controller (NET-19): `Network` picks a `DiscoveryMode`
  from its degree and battery and scans and advertises accordingly -
  `Aggressive` (low latency) below the stable target, `Normal`
  (balanced) at it, and `Quiet` (low power, scanning in windows of one
  interval with pauses of five) while the beacon's `low_battery` flag
  is set or the new `Network::set_idle` says so. A mode change, or the
  start or stop of a quiet scan window, is requested no sooner than the
  new `discovery_interval` config (default 10 s) after the last one;
  beacon content updates are not held back. `Network::discovery_mode`
  reads the mode in force.
- Inbound abuse protection (NET-18): each link meters what its peer
  sends with token buckets for frames and bytes (`frame_limit`, default
  50/s with a burst of 200, and `byte_limit`, default 64 KiB/s with a
  burst of 256 KiB, configured with the new `RateLimit`). A peer over
  either limit is disconnected with `DisconnectReason::AbuseDetected`
  and blacklisted at once. A link closed for a malformed frame, a
  broken chunk stream or an oversize `Hello` is a strike against its
  peer, and `max_strikes` (default 3) strikes blacklist it. A
  blacklisted peer is neither dialed nor accepted, and loses any other
  link, for `blacklist_duration` (default 10 min), which is also how
  long a strike is remembered. Strikes and the blacklist are kept per
  `PeerKey`, bounded by `max_peers`.
- Duplicate-link resolution (NET-17): the side of a pair whose beacon
  hint sorts higher (or equal) now dials after a 4 s initiation delay
  instead of never, so pairs with equal hints, or whose lower side
  never dials, still link. An inbound connection from a peer this node
  is itself still dialing is accepted, and when a second link to the
  same remote tag completes its handshake, the one opened by the lower
  tag is kept and the other closed with `DisconnectReason::Duplicate`.
  A loser that was never announced ends without events. A link whose
  remote tag is this node's own is closed the same way.
- Add `NativeTransport` (feature `native`): the `Transport` over the Kotlin
  BLE shim, with `NativeSink`, `NativeEvents`, `NativeHandle`, `NativeConfig`,
  `NativeStatus` and the status mapping of `docs/native-contract.md`.
- Topology policy (NET-16): the link manager admits links through a
  `TopologyPolicy` derived from `max_links` and `min_rssi` - stable
  slots up to `max_stable` (the stable target plus one, so a healthy
  link never fights for its place at the target), then the single
  explore slot, and never the opportunistic slot (reserved for
  NET-22). With no slot free the advertised `accepting` flag goes
  false. Candidates are scored by `rssi * 4 - degree` and dialed
  best-first, a node over its stable target evicts its worst link once
  it is past a 10 s minimum age and below the admission floor (strong
  links are kept - the hysteresis that stops a neighbourhood
  churning), inbound links meet the same slots and the same floor, and
  three rejected outbound attempts in a row lower the session's
  effective cap by one (floor 1; a completed handshake clears the
  streak).
- Breaking: `SimNode` gains an `rssi` field for the signal other nodes
  report when hearing it (default `SIM_RSSI`), and `SimWorld::set_rssi`
  changes it, so a test can drift a peer below the floor while its
  link is held.
- Breaking: `Network::disconnect` takes the current monotonic `now`, so
  a link cancelled while still connecting starts that peer's connect
  backoff from the caller's clock.
- Peer table (NET-15): every beacon sighting is recorded with a
  moving-average RSSI and the latest valid beacon, bounded by the new
  `max_peers` config (default 64) and evicting entries unseen for 30 s.
  Connect attempts consult it: a peer that is already linked, still in
  its exponential backoff after a failed attempt (1 s doubling to 60 s,
  plus a deterministic per-peer jitter), below `min_rssi`, or not
  accepting links is never dialled, and a freed slot is immediately
  offered to the next candidate. The simulator now re-reports unchanged
  beacons every scan refresh (1 s), as a radio would, so a backoff-gated
  attempt can resume without waiting for the beacon to change.
- Breaking: `Network::send`, `Network::broadcast` and `Link::send` take
  the current monotonic `now`, and `SendQueue::pop` takes `now`, so every
  time-dependent queue decision (expiry pruning, what may still be
  written) uses the caller's clock instead of hidden state. The new
  `Network::send_opts` and `Link::send_opts` carry `SendOpts`.
- Send queue coalescing and expiry (NET-21): `SendOpts` adds a
  `coalesce_key` (a newer frame with the same key on the same channel
  replaces a still-queued one in place) and an `expires_at` deadline
  (expired frames are dropped silently before they are ever written, and
  `Link::next_deadline` wakes the owner at the deadline). Telemetry
  admits the newest update by evicting its oldest waiting frames, so it
  meets `QueueFull` only with a payload larger than its whole channel cap.
- Keepalive: a ready link pings after `keepalive_interval` with nothing
  received and closes with `KeepaliveTimeout` after `max_missed_pongs`
  unanswered pings; it answers every `Ping` with `Pong`.
- Add `LinkMetrics` (RSSI and RTT moving averages, tx/rx bytes, dropped
  frames, last receive time), read with `Link::metrics` and the new
  `LinkInfo::metrics` field.
- Add `Network::disconnect(link, reason)` for caller-requested link closure and
  connection cancellation through the existing link state machine.
