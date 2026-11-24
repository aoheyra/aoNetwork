# NET-05: Chunk format and reassembly

Status: proposed for maintainer review.

## Layout

An encoded frame is split into chunks, one per radio packet. Over GATT one
ATT value carries at most `mtu - 3` bytes, so a chunk holds 1 to `mtu - 4`
payload bytes. Over L2CAP CoC (NET-26) one packet carries `mtu` bytes, so a
chunk holds 1 to `mtu - 1` payload bytes. The chunk header is the same on
both transports: reassembly never depends on the radio channel.

| Bits | Field |
|---|---|
| 7 | START: first chunk of a frame |
| 6 | END: last chunk of a frame |
| 5-3 | Reserved: sent as zero, ignored on receive |
| 2-0 | Sequence: 0 on START, then +1 modulo 8 |

A frame that fits in one chunk sets START and END together. MTU must be in
23..=517.

## Reassembly rules

One `Reassembler` per link holds at most one partial frame. Each error below
drops that frame and the offending chunk; the next frame needs a new START.

| Error | Cause |
|---|---|
| `EmptyChunk` | chunk without payload |
| `MissingStart` | continuation with no frame in flight |
| `StartWhileBusy` | START while a frame is in flight |
| `SeqGap` | sequence not the expected value (START must be 0) |
| `TooLarge` | frame would exceed `NetworkConfig::max_frame` |
| `Timeout` | `NetworkConfig::reassembly_timeout` (default 10 s) has passed since START |

The deadline runs from START and is not extended by progress. It is checked on
every push and by `poll_timeout`, which the link calls from its timer. Time is
supplied by the caller.

The 3-bit sequence detects reordering and short gaps. It cannot detect the loss
of exactly eight consecutive chunks; both GATT and CoC deliver packets in
order on a live link, and the frame decoder rejects a malformed result.
