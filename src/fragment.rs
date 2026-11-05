//! Bounded, per-link fragmentation of opaque encoded frames.
//!
//! The byte-slice seam keeps this independent of the pending frame codec.
//! The proposed chunk contract is in `docs/adr/net-05-fragmentation.md`.

use std::time::Duration;

use crate::NetworkConfig;

/// Smallest supported ATT MTU.
pub const MIN_MTU: u16 = 23;
/// Largest supported ATT MTU.
pub const MAX_MTU: u16 = 517;
const ATT_OVERHEAD: usize = 3;
const START: u8 = 0x80;
const END: u8 = 0x40;
const SEQUENCE: u8 = 0x07;
const RESERVED: u8 = 0x38;

/// Fragmentation failures. Any receive error discards the in-flight frame.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FragmentError {
    /// ATT MTU is outside the supported range.
    #[error("unsupported ATT MTU: {0}")]
    InvalidMtu(u16),
    /// There are no encoded frame bytes to transmit.
    #[error("cannot fragment an empty frame")]
    EmptyFrame,
    /// A frame would exceed its configured byte cap.
    #[error("frame needs {actual} bytes, maximum is {max}")]
    TooLarge {
        /// Required bytes.
        actual: usize,
        /// Configured bound.
        max: usize,
    },
    /// Chunk has no header or no data byte.
    #[error("chunk must contain a header and payload")]
    EmptyChunk,
    /// A chunk exceeds the negotiated ATT value length.
    #[error("chunk has {actual} bytes, maximum is {max}")]
    ChunkTooLarge {
        /// Received bytes, including the chunk header.
        actual: usize,
        /// Maximum allowed bytes.
        max: usize,
    },
    /// Reserved chunk header bits were set.
    #[error("unsupported chunk header bits: {0:#04x}")]
    UnsupportedHeader(u8),
    /// Chunk sequence was not the next expected value modulo eight.
    #[error("chunk sequence gap: expected {expected}, got {actual}")]
    SeqGap {
        /// Expected sequence.
        expected: u8,
        /// Received sequence.
        actual: u8,
    },
    /// A new START arrived before the current frame completed.
    #[error("START received while a frame is in flight")]
    StartWhileBusy,
    /// A continuation arrived without a successfully accepted START.
    #[error("continuation received without START")]
    UnexpectedContinuation,
    /// The frame did not finish within its configured total deadline.
    #[error("frame reassembly timed out")]
    Timeout,
    /// Caller supplied decreasing time during an in-flight frame.
    #[error("reassembly time moved backwards")]
    TimeWentBackwards,
    /// Caller output cannot hold this chunk; no output was written.
    #[error("chunk output needs {required} bytes, got {actual}")]
    OutputTooSmall {
        /// Required output bytes.
        required: usize,
        /// Supplied output bytes.
        actual: usize,
    },
}

/// A chunk borrowing its payload from the original encoded frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chunk<'a> {
    header: u8,
    payload: &'a [u8],
}

impl Chunk<'_> {
    /// Chunk header byte, including START/END and sequence.
    pub fn header(&self) -> u8 {
        self.header
    }

    /// Borrowed chunk data, excluding its header.
    pub fn payload(&self) -> &[u8] {
        self.payload
    }

    /// Serializes into caller storage; validates capacity before writing.
    pub fn encode_into(&self, out: &mut [u8]) -> Result<usize, FragmentError> {
        let required = 1 + self.payload.len();
        if out.len() < required {
            return Err(FragmentError::OutputTooSmall {
                required,
                actual: out.len(),
            });
        }
        out[0] = self.header;
        out[1..required].copy_from_slice(self.payload);
        Ok(required)
    }
}

/// Streaming, allocation-free chunk iterator for a single encoded frame.
#[derive(Debug)]
pub struct Fragmenter<'a> {
    bytes: &'a [u8],
    offset: usize,
    payload_cap: usize,
    sequence: u8,
}

impl<'a> Fragmenter<'a> {
    /// Borrows any encoded-byte provider through the `AsRef<[u8]>` seam.
    ///
    /// The owner serializes all chunks of this frame before another frame on
    /// the same link. Frame encoding and decoding stay outside this module.
    pub fn new<T: AsRef<[u8]> + ?Sized>(
        frame: &'a T,
        mtu: u16,
        config: &NetworkConfig,
    ) -> Result<Self, FragmentError> {
        let payload_cap = chunk_cap(mtu)? - 1;
        let bytes = frame.as_ref();
        if bytes.is_empty() {
            return Err(FragmentError::EmptyFrame);
        }
        if bytes.len() > config.max_frame() {
            return Err(FragmentError::TooLarge {
                actual: bytes.len(),
                max: config.max_frame(),
            });
        }
        Ok(Self {
            bytes,
            offset: 0,
            payload_cap,
            sequence: 0,
        })
    }
}

impl<'a> Iterator for Fragmenter<'a> {
    type Item = Chunk<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset == self.bytes.len() {
            return None;
        }
        let end = self.bytes.len().min(self.offset + self.payload_cap);
        let header = self.sequence
            | if self.offset == 0 { START } else { 0 }
            | if end == self.bytes.len() { END } else { 0 };
        let payload = &self.bytes[self.offset..end];
        self.offset = end;
        self.sequence = (self.sequence + 1) & SEQUENCE;
        Some(Chunk { header, payload })
    }
}

impl std::iter::FusedIterator for Fragmenter<'_> {}

/// Result of accepting a chunk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reassembly {
    /// More chunks are required.
    Pending,
    /// Exactly one complete encoded frame, transferred to the caller.
    Complete(Vec<u8>),
}

#[derive(Debug)]
struct InFlight {
    bytes: Vec<u8>,
    expected: u8,
    started: Duration,
    last_now: Duration,
}

/// One bounded in-flight frame. Create a separate instance for each link.
#[derive(Debug)]
pub struct Reassembler {
    chunk_cap: usize,
    max_frame: usize,
    timeout: Duration,
    active: Option<InFlight>,
}

impl Reassembler {
    /// Fixes this link's MTU and takes a snapshot of its validated limits.
    pub fn new(mtu: u16, config: &NetworkConfig) -> Result<Self, FragmentError> {
        Ok(Self {
            chunk_cap: chunk_cap(mtu)?,
            max_frame: config.max_frame(),
            timeout: config.reassembly_timeout(),
            active: None,
        })
    }

    /// Accepts one complete ATT value at caller-supplied monotonic time.
    ///
    /// Every error drops the current frame, including a conflicting START.
    /// The next accepted frame must begin with a fresh START at sequence zero.
    pub fn push(&mut self, now: Duration, chunk: &[u8]) -> Result<Reassembly, FragmentError> {
        let result = self.push_inner(now, chunk);
        if result.is_err() {
            self.reset();
        }
        result
    }

    /// Checks the total deadline even when no further chunks arrive.
    ///
    /// The owner must call this from its time-driven steps; no timer is spawned.
    pub fn tick(&mut self, now: Duration) -> Result<(), FragmentError> {
        let result = self.check_time(now);
        if result.is_err() {
            self.reset();
        }
        result
    }

    /// Drops partial bytes, for example on disconnect or renegotiated MTU.
    pub fn reset(&mut self) {
        self.active = None;
    }

    /// Whether a frame is currently incomplete.
    pub fn is_busy(&self) -> bool {
        self.active.is_some()
    }

    /// Current retained frame bytes, always at most the configured frame cap.
    pub fn buffered_len(&self) -> usize {
        self.active.as_ref().map_or(0, |active| active.bytes.len())
    }

    fn check_time(&mut self, now: Duration) -> Result<(), FragmentError> {
        if let Some(active) = &mut self.active {
            if now < active.last_now {
                return Err(FragmentError::TimeWentBackwards);
            }
            if now - active.started >= self.timeout {
                return Err(FragmentError::Timeout);
            }
            active.last_now = now;
        }
        Ok(())
    }

    fn push_inner(&mut self, now: Duration, chunk: &[u8]) -> Result<Reassembly, FragmentError> {
        self.check_time(now)?;
        if chunk.len() < 2 {
            return Err(FragmentError::EmptyChunk);
        }
        if chunk.len() > self.chunk_cap {
            return Err(FragmentError::ChunkTooLarge {
                actual: chunk.len(),
                max: self.chunk_cap,
            });
        }
        let header = chunk[0];
        if header & RESERVED != 0 {
            return Err(FragmentError::UnsupportedHeader(header & RESERVED));
        }
        let sequence = header & SEQUENCE;
        let payload = &chunk[1..];
        if header & START != 0 {
            if self.active.is_some() {
                return Err(FragmentError::StartWhileBusy);
            }
            if sequence != 0 {
                return Err(FragmentError::SeqGap {
                    expected: 0,
                    actual: sequence,
                });
            }
            if payload.len() > self.max_frame {
                return Err(FragmentError::TooLarge {
                    actual: payload.len(),
                    max: self.max_frame,
                });
            }
            if header & END != 0 {
                return Ok(Reassembly::Complete(payload.to_vec()));
            }
            let mut bytes = Vec::with_capacity(self.max_frame);
            bytes.extend_from_slice(payload);
            self.active = Some(InFlight {
                bytes,
                expected: 1,
                started: now,
                last_now: now,
            });
            return Ok(Reassembly::Pending);
        }
        let mut active = self
            .active
            .take()
            .ok_or(FragmentError::UnexpectedContinuation)?;
        if sequence != active.expected {
            return Err(FragmentError::SeqGap {
                expected: active.expected,
                actual: sequence,
            });
        }
        let required = active.bytes.len() + payload.len();
        if required > self.max_frame {
            return Err(FragmentError::TooLarge {
                actual: required,
                max: self.max_frame,
            });
        }
        active.bytes.extend_from_slice(payload);
        active.expected = (sequence + 1) & SEQUENCE;
        if header & END != 0 {
            return Ok(Reassembly::Complete(active.bytes));
        }
        self.active = Some(active);
        Ok(Reassembly::Pending)
    }
}

fn chunk_cap(mtu: u16) -> Result<usize, FragmentError> {
    if !(MIN_MTU..=MAX_MTU).contains(&mtu) {
        return Err(FragmentError::InvalidMtu(mtu));
    }
    Ok(usize::from(mtu) - ATT_OVERHEAD)
}
