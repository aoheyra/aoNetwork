# NET-25: Credit-based flow control

Status: proposed for maintainer review.

## Context

A sender's pace was set only by the radio's write credit, which returns
once the stack has taken a chunk, not once the peer's app has read it. A
receiver that reads slowly keeps collecting chunks in its radio queue, and
when that queue is bounded (`NativeTransport::max_pending_events`) it
overflows and the link is lost. NET-04 reserved a `Credit` frame for this.

## Decision

### Negotiation

`Hello.caps` bit 0 (`CAP_CREDIT`) offers credits. A link uses them only
when both `Hello`s set it; otherwise nothing changes. `NetworkConfig`
offers them by default (`with_flow_control`). Bit 1 (`CAP_L2CAP`, NET-26)
is independent: it names the radio channel and neither requires nor
changes credit accounting.

### Frame

```text
Credit  0x06  channel u8 | credits u32
```

The channel byte uses the `Data` channel values and the same
`UnknownChannel` check. `credits` is big-endian and counts bytes of encoded
`Data` frames on that channel, the same bytes the send queue schedules.

### Rules

- Credits count `Data` frames only. `Hello`, `Ping`, `Pong`, `Bye` and
  `Credit` are never held back.
- On reaching `Ready`, each side grants its receive window per channel
  (`NetworkConfig::receive_window`, at least `max_frame`, at most 1 MiB).
- A frame is written only when its channel has credit for all of it. A
  channel without enough credit is skipped by the scheduler like an empty
  one; the other channels go on.
- The receiver grants back the bytes of each `Data` frame once it has
  handed the frame on. It sends a grant when half a window is owed, or
  when less than `max_frame` is granted and the peer may be waiting, so a
  sender can never stall with credit owed to it.
- A grant counts once its frame has been written. A peer that sends more
  on a channel than it has been written is closed with
  `ProtocolViolation`, a strike.

## Compatibility

The `Credit` body changes from the reserved `credits u32` to
`channel u8 | credits u32` within frame version 1, without a version bump.
No build has sent `Credit`: before NET-25 it was reserved and dropped on
receipt. A node sends it only to a peer whose `Hello` sets `CAP_CREDIT`,
which no earlier build does, so an earlier peer never receives the new
body. A version bump would change every frame for no gain over the
capability bit.

## Consequences

- What a peer may have sent and this node not yet read is bounded by the
  sum of the receive windows, per link. Sizing a bounded radio queue to
  that sum at the link's MTU means it never overflows for a peer that
  follows the rules.
- The first `Data` frame on a link waits one round trip for the initial
  grant.
- Raising `max_frame` above a receive window is refused until the window
  is raised.
