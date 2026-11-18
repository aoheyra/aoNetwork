# Changelog

## Unreleased

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
