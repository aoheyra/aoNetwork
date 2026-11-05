# Fragmentation contract

Status: proposed for NET-05 review. Depends on the NET-04 frame codec
([PR #18](https://github.com/aoheyra/aoNetwork/pull/18)), still unmerged when
implemented. The core uses the `AsRef<[u8]>` trait seam for encoded bytes; it
does not duplicate or assume a `Frame` API. Encode first, fragment, reassemble,
then pass complete bytes to the frame decoder. Link integration remains separate.

Review questions: confirm START/END bit assignments, reserved-bit handling,
sequence-zero restart, and the new 10 s total reassembly timeout default. The
README and issue did not previously specify these values. This is an initial
proposed chunk layout, not a change to the versioned frame/beacon formats.
Incompatible chunk layouts must negotiate a new link protocol version; the
initial format has no per-chunk version byte and cannot autodetect alternatives.

For an ATT MTU in 23..=517, each ATT value is at most MTU - 3 bytes. Its first
byte is START (bit 7), END (bit 6), reserved zero (bits 5..3), sequence (bits 2..0).
The payload is nonempty and at most MTU - 4 bytes. First sequence is zero;
continuations advance modulo eight. A single chunk sets both START and END.

Use one sender stream and one Reassembler per link. START while busy, sequence
gap, reserved bits, empty/oversize chunk, frame overflow, timeout or decreasing
in-flight time discards the frame. The rejected chunk is not reused as a new
START. A fresh START is required. Disconnect or MTU change resets the state.
Length checks precede allocation/copy. A partial frame reserves at most the
configured max_frame request; completion moves that buffer to the caller.

Time is caller-injected monotonic Duration. The deadline is measured from START,
does not refresh with progress, and expires at elapsed >= reassembly_timeout.
The owner must call tick even when no bytes arrive. Comparing elapsed durations
avoids adding a deadline that could overflow. The 10 s default is a proposed
resource-retention bound, not a measured BLE transfer guarantee; callers can set
a nonzero limit suitable for frame size and tested throughput.

The borrowed iterator retains the frame and one chunk view; the receiver retains
one bounded buffer per link. Errors discard that buffer. This layer supplies no
retransmission. The transport must preserve accepted ATT value order. A 3-bit
counter wraps after eight chunks and cannot detect every delayed duplicate or
loss of eight consecutive chunks. Frame length checks and higher-layer integrity
remain necessary; this codec does not authenticate bytes or guarantee delivery.
