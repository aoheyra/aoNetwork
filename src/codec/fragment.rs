//! Splitting encoded frames into ATT-sized chunks and joining them again.
//!
//! ```text
//! chunk  = header u8 | payload (1..=mtu-4 bytes)
//! header = START (bit 7) | END (bit 6) | reserved (bits 5-3) | seq (bits 2-0)
//! ```
//!
//! The rules are recorded in `docs/adr/net-05-chunk-format.md`.

use std::time::Duration;

use crate::{FragmentError, NetworkConfig};

/// Smallest ATT MTU a BLE stack may negotiate.
pub const MIN_MTU: u16 = 23;

/// Largest ATT MTU a BLE stack may negotiate.
pub const MAX_MTU: u16 = 517;

const START: u8 = 0x80;
const END: u8 = 0x40;
const SEQ: u8 = 0x07;
/// ATT header bytes; absent on an L2CAP CoC (NET-26).
pub const ATT_OVERHEAD: usize = 3;
/// One-byte chunk header, present on both transports.
pub(crate) const CHUNK_OVERHEAD: usize = 1;
/// GATT overhead: ATT header plus the chunk header.
const OVERHEAD: usize = ATT_OVERHEAD + CHUNK_OVERHEAD;

/// Splits one encoded frame into chunks for a link with the given MTU.
///
/// Each item is one ATT value, ready for a single write or notification.
/// An empty input yields no chunks.
///
/// # Example
///
/// ```
/// use ao_network::Fragmenter;
///
/// let frame = vec![0xAB; 40];
/// let chunks: Vec<Vec<u8>> = Fragmenter::new(&frame, 23)?.collect();
/// // 19 payload bytes per chunk at the minimum MTU.
/// assert_eq!(chunks.len(), 3);
/// assert_eq!(chunks[0][0], 0x80); // START, seq 0
/// assert_eq!(chunks[2][0], 0x42); // END, seq 2
/// # Ok::<(), ao_network::FragmentError>(())
/// ```
#[derive(Debug, Clone)]
pub struct Fragmenter<'a> {
    rest: &'a [u8],
    payload_cap: usize,
    seq: u8,
    started: bool,
}

impl<'a> Fragmenter<'a> {
    /// Prepares to split `frame` for a link whose ATT MTU is `mtu`.
    ///
    /// # Errors
    ///
    /// [`FragmentError::InvalidMtu`] if `mtu` is outside
    /// [`MIN_MTU`]`..=`[`MAX_MTU`].
    pub fn new(frame: &'a [u8], mtu: u16) -> Result<Self, FragmentError> {
        Self::with_overhead(frame, mtu, OVERHEAD)
    }

    /// Splits `frame` for a transport whose packet loses `overhead` bytes
    /// to headers. GATT loses three ATT bytes; an L2CAP CoC
    /// loses none, keeping only the chunk header (NET-26).
    ///
    /// # Errors
    ///
    /// [`FragmentError::InvalidMtu`] if `mtu` is out of range or cannot
    /// hold the headers and a byte of payload.
    pub fn with_overhead(
        frame: &'a [u8],
        mtu: u16,
        overhead: usize,
    ) -> Result<Self, FragmentError> {
        if !(MIN_MTU..=MAX_MTU).contains(&mtu) || usize::from(mtu) <= overhead {
            return Err(FragmentError::InvalidMtu { mtu });
        }
        Ok(Self {
            rest: frame,
            payload_cap: usize::from(mtu) - overhead,
            seq: 0,
            started: false,
        })
    }
}

impl Iterator for Fragmenter<'_> {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Vec<u8>> {
        if self.rest.is_empty() {
            return None;
        }
        let take = self.rest.len().min(self.payload_cap);
        let (payload, rest) = self.rest.split_at(take);
        let mut header = self.seq;
        if !self.started {
            header |= START;
            self.started = true;
        }
        if rest.is_empty() {
            header |= END;
        }
        let mut chunk = Vec::with_capacity(1 + payload.len());
        chunk.push(header);
        chunk.extend_from_slice(payload);
        self.rest = rest;
        self.seq = (self.seq + 1) & SEQ;
        Some(chunk)
    }
}

/// Joins chunks from one link back into encoded frames.
///
/// Holds at most one partial frame, bounded by the configured `max_frame`
/// in length and in the memory it holds.
/// Every error drops that partial frame and the chunk that caused it; the
/// next frame must begin with a fresh START. Time is injected: `now` is any
/// monotonic clock the caller owns.
///
/// # Example
///
/// ```
/// use ao_network::{Fragmenter, NetworkConfig, Reassembler};
/// use std::time::Duration;
///
/// let frame = vec![0xCD; 100];
/// let mut reassembler = Reassembler::new(&NetworkConfig::default());
/// let mut out = None;
/// for chunk in Fragmenter::new(&frame, 23)? {
///     out = reassembler.push(Duration::ZERO, &chunk)?;
/// }
/// assert_eq!(out, Some(frame));
/// # Ok::<(), ao_network::FragmentError>(())
/// ```
#[derive(Debug, Clone)]
pub struct Reassembler {
    max_frame: usize,
    timeout: Duration,
    in_flight: Option<InFlight>,
}

#[derive(Debug, Clone)]
struct InFlight {
    buf: Vec<u8>,
    next_seq: u8,
    started_at: Duration,
}

impl Reassembler {
    /// Creates an idle reassembler using `max_frame` and
    /// `reassembly_timeout` from `config`.
    pub fn new(config: &NetworkConfig) -> Self {
        Self {
            max_frame: config.max_frame(),
            timeout: config.reassembly_timeout(),
            in_flight: None,
        }
    }

    /// Accepts one chunk received at `now`.
    ///
    /// Returns the complete frame on its last chunk, `None` while more
    /// chunks are expected.
    ///
    /// # Errors
    ///
    /// Each error drops the partial frame:
    /// [`EmptyChunk`](FragmentError::EmptyChunk) for a chunk without payload,
    /// [`Timeout`](FragmentError::Timeout) if the partial frame has expired,
    /// [`StartWhileBusy`](FragmentError::StartWhileBusy) for a START during a
    /// partial frame, [`MissingStart`](FragmentError::MissingStart) for a
    /// continuation without one, [`SeqGap`](FragmentError::SeqGap) for an
    /// out-of-order sequence, and [`TooLarge`](FragmentError::TooLarge) if
    /// the frame would exceed `max_frame`.
    pub fn push(&mut self, now: Duration, chunk: &[u8]) -> Result<Option<Vec<u8>>, FragmentError> {
        let result = self.accept(now, chunk);
        if result.is_err() {
            self.in_flight = None;
        }
        result
    }

    /// Drops the partial frame if it has expired at `now`.
    ///
    /// The link calls this from its timer so a stalled sender cannot hold
    /// the buffer while no chunks arrive.
    ///
    /// # Errors
    ///
    /// [`FragmentError::Timeout`] if a partial frame was dropped.
    pub fn poll_timeout(&mut self, now: Duration) -> Result<(), FragmentError> {
        if self.expired(now) {
            self.in_flight = None;
            return Err(FragmentError::Timeout);
        }
        Ok(())
    }

    /// When the partial frame expires, if one is in progress.
    pub fn deadline(&self) -> Option<Duration> {
        self.in_flight
            .as_ref()
            .map(|f| f.started_at.saturating_add(self.timeout))
    }

    fn expired(&self, now: Duration) -> bool {
        self.in_flight
            .as_ref()
            .is_some_and(|f| now.saturating_sub(f.started_at) >= self.timeout)
    }

    fn accept(&mut self, now: Duration, chunk: &[u8]) -> Result<Option<Vec<u8>>, FragmentError> {
        let Some((&header, payload)) = chunk.split_first() else {
            return Err(FragmentError::EmptyChunk);
        };
        if payload.is_empty() {
            return Err(FragmentError::EmptyChunk);
        }
        if self.expired(now) {
            return Err(FragmentError::Timeout);
        }
        let seq = header & SEQ;
        let is_start = header & START != 0;
        let is_end = header & END != 0;
        let max = self.max_frame;

        match self.in_flight.as_mut() {
            Some(_) if is_start => Err(FragmentError::StartWhileBusy),
            None if !is_start => Err(FragmentError::MissingStart),
            None => {
                check_seq(0, seq)?;
                check_len(payload.len(), max)?;
                if is_end {
                    return Ok(Some(payload.to_vec()));
                }
                self.in_flight = Some(InFlight {
                    buf: payload.to_vec(),
                    next_seq: 1,
                    started_at: now,
                });
                Ok(None)
            }
            Some(in_flight) => {
                check_seq(in_flight.next_seq, seq)?;
                let len = in_flight.buf.len() + payload.len();
                check_len(len, max)?;
                grow(&mut in_flight.buf, len, max);
                in_flight.buf.extend_from_slice(payload);
                in_flight.next_seq = (seq + 1) & SEQ;
                if is_end {
                    let frame = std::mem::take(&mut in_flight.buf);
                    self.in_flight = None;
                    return Ok(Some(frame));
                }
                Ok(None)
            }
        }
    }
}

/// Makes room for `len` bytes in `buf`, doubling as `Vec` would but never
/// past `max`, so a partial frame holds no more than `max_frame` bytes.
fn grow(buf: &mut Vec<u8>, len: usize, max: usize) {
    if len > buf.capacity() {
        let target = buf.capacity().saturating_mul(2).clamp(len, max);
        buf.reserve_exact(target - buf.len());
    }
}

fn check_seq(expected: u8, actual: u8) -> Result<(), FragmentError> {
    if actual != expected {
        return Err(FragmentError::SeqGap { expected, actual });
    }
    Ok(())
}

fn check_len(len: usize, max: usize) -> Result<(), FragmentError> {
    if len > max {
        return Err(FragmentError::TooLarge { len, max });
    }
    Ok(())
}
