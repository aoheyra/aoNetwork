# NET-26: L2CAP CoC as a second link transport

Status: proposed for maintainer review. Implements the spike in issue NET-26.

## Context

NET-26 asks for a written, measured decision on carrying link frames over an
L2CAP connection-oriented channel (CoC) on Android 29+ instead of GATT. Its
"Done when" asks for a decision with measurements and code only if the gain is
significant. No real-phone harness is available in this repository: the Rust
core is sans-IO and the Kotlin shim is owned by the `:ble` module in aoheyra.
This ADR therefore reports a deterministic simulator measurement, states what
it cannot prove, and defines the contract the Kotlin owner must implement.

GATT carries one chunk in one ATT value: 3 bytes of ATT header are lost from
every 23..=517 MTU, and each ATT write or notification has stack cost. A CoC
carries one chunk in one L2CAP packet: the 1-byte chunk header is the only
per-packet overhead, and the frame, handshake and link rules above the
transport are unchanged.

## Options

| | GATT only (A) | Add CoC, negotiate in `Hello.caps` (B) | Replace GATT with CoC (C) |
|---|---|---|---|
| Delivery | 100% in the seeded run | 100% in the seeded run; GATT fallback for old peers | 100% in simulation, but no fallback on Android < 29 or before the shim ships |
| Latency | 276 ms for 64 KiB | 148 ms for 64 KiB (46% lower) | Same as B in simulation; unknown on phones |
| Overhead | 277 packets for 64 KiB; 3 ATT bytes per packet | 149 packets (46% fewer); no ATT bytes | Same as B, but removes the fallback |
| Memory | Bounded event and scan queues; GATT stack buffers one ATT value | Same Rust bounds; CoC socket buffers are owned by Android, one MTU-sized chunk in flight | Same as B |
| CPU / battery | One ATT operation and callback per chunk | Roughly half the radio operations for a Bulk sync; no ATT value encoding | Same as B; real phone battery not measured |
| Determinism | Seeded simulator, injected clock | Same engine and seed; transport kind is a node setting | Same as B |
| Testability | Existing GATT unit and simulator suites | Added bounded-queue, credit, teardown, status-mapping and seeded comparison tests | No A/B comparison possible after removal |
| Maintenance | No new cap or trait method | One caps bit, one default trait method, one feature-gated adapter; no wire version bump | Two code paths must be removed, and old peers break |

## Measurement

Command, on Rust 1.85.1:

```
TMPDIR=/home/Gaurav/tmp CARGO_TARGET_DIR=/home/Gaurav/tmp/aoNetwork-target \
  cargo +1.85 test --release --all-features --test l2cap_sim_tests \
  l2cap_coc_delivers -- --nocapture
```

`tests/l2cap_sim_tests.rs`, seed `0x4C26C0C0` (`1277608128`), two nodes in
range, `connect_latency = 200 ms`, `connect_jitter = 0`, `write_latency = 1 ms`,
default `NetworkConfig`, 64 KiB sent as sixteen 4 KiB `Bulk` payloads with
credit flow control. GATT uses the default `SimNode` MTU of 247; CoC uses the
default CoC MTU of 512. Writes are counted at the `Transport::write` seam.

| Transport | Delivered | Writes | Elapsed |
|---|---:|---:|---:|
| GATT | 65536 B | 277 | 276 ms |
| L2CAP CoC | 65536 B | 149 | 148 ms |

The same numbers occur in debug and release mode. The test fails if delivery
drops, CoC does not cut writes by at least a third, or CoC is slower. The gain
comes from the chunk payload rising from 244 to 511 bytes; the small remainder
is handshake and credit frames, which are unchanged.

**Limitation:** these are simulator numbers. They do not measure Android
throughput, real radio retransmissions, socket setup time or battery draw. A
phone measurement on API 29+ (the `:ble` shim's `L2capSink` plus `L2capHandle`)
is still required before CoC is enabled by default in aoheyra. That work is
outside this crate.

## Decision

Adopt option B. Add `CAP_L2CAP` as `Hello.caps` bit 1 and a default
`Transport::link_transport()` returning `Gatt`, overridden by
`L2capTransport`. A link is CoC only when both ends advertise the cap and its
transport reports CoC; GATT remains the default and the fallback. The frame
layout, frame version, handshake and chunk header do not change: unknown caps
bits are already carried and ignored, so no compatibility negotiation beyond
the cap bit is needed.

`L2capTransport` is additive and feature-gated behind `native`. It mirrors
`NativeTransport`: bounded event and scan queues, one write credit per link,
typed config and status errors, ids allocated only in Rust, exactly one
terminal event, and no I/O or clock in the core. A CoC packet holds at most one
chunk of `mtu` bytes; the MTU stays in 23..=517 so the existing link, queue and
memory bounds still hold. An endpoint that advertises CoC cannot link to a
GATT-only endpoint: they listen on different Android sockets.

## Consequences

- `docs/native-contract.md` section 9 specifies the Kotlin `L2capSink` and
  `L2capEvents` calls. Until the shim implements it, `L2capTransport` is usable
  only with a shim that follows that section; `NativeTransport` is unchanged.
- `LinkInfo`, `LinkSnapshot`, `Network::links()` and `Network::snapshot()`
  report the physical transport and whether both sides negotiated CoC, so the
  phone harness can confirm which path a measurement took.
- No new dependency and no change to the memory ceilings: the CoC adapter has
  the same queue bounds as the GATT adapter and holds one MTU-sized chunk in
  flight per link.
- Sim coverage includes a seeded GATT/CoC comparison, negotiation, mixed
  endpoints, and tests that would fail if CoC lost its fragmentation gain.
- The spike's phone measurement remains open. If it shows no significant
  gain, set `SimNode::l2cap` usage aside in aoheyra and keep the GATT default;
  the cap bit and adapter are inert unless configured.
