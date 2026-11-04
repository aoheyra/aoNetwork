# NET-06: Beacon payload version 1

Status: proposed for maintainer review.

## Wire layout

The payload is exactly 16 bytes. BLE AD headers and the manufacturer company
identifier are excluded; Kotlin extracts the payload before passing it to Rust.

| Offset | Bytes | Field |
|---|---|---|
| 0 | 1 | Version: `1` |
| 1 | 1 | Flags: bit 0 accepting, bit 1 relay-willing, bit 2 low-battery |
| 2 | 1 | Claimed degree |
| 3 | 1 | Claimed maximum links |
| 4 | 8 | Opaque hint from aoIdents, in supplied order |
| 12 | 4 | Opaque digest from aoMesh, in supplied order |

## Decode rules

- Check exact length, then version, then flags. Reject short or trailing bytes,
  versions other than `1`, and nonzero reserved flag bits 3-7.
- Copy hint and digest unchanged. The digest is bytes, not an integer.
- Preserve untrusted degree/capacity claims, including zero or degree greater
  than capacity. Topology policy handles those values; a beacon authenticates
  no sender.

Structural failures return `BeaconError`, convertible to `NetError::InvalidBeacon`.
Layout or flag-meaning changes require a new wire version and compatibility note.
Android advertising, scanning and radio interoperability remain untested.
