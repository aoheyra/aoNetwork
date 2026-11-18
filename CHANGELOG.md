# Changelog

## Unreleased

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
