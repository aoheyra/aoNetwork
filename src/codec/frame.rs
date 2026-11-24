//! Link-layer frames, wire version 1.
//!
//! ```text
//! version u8 | type u8 | body
//!
//! Hello   0x01  proto_min u8 | proto_max u8 | caps u32 | max_frame u32
//!               | tag [16] | app_hello_len LEB128 | app_hello
//! Data    0x02  channel u8 | payload_len LEB128 | payload
//! Ping    0x03  (empty)
//! Pong    0x04  (empty)
//! Bye     0x05  (empty)
//! Credit  0x06  channel u8 | credits u32
//! ```
//!
//! Integers are big-endian; lengths are canonical LEB128. The rules are
//! recorded in `docs/adr/net-04-frame-wire-format.md`, and the `Credit`
//! body in `docs/adr/net-25-credit-flow-control.md`.

use super::varint;
use crate::{Channel, FrameError, LocalTag};

/// Frame wire version written by [`Frame::encode`] and accepted by
/// [`Frame::decode`].
pub const FRAME_VERSION: u8 = 1;

/// Largest `app_hello` blob a [`Hello`] may carry, in bytes.
pub const MAX_APP_HELLO: usize = 256;

/// [`Hello::caps`] bit: the sender takes part in credit-based flow
/// control. A link uses credits only when both `Hello`s set it.
pub const CAP_CREDIT: u32 = 1;

/// [`Hello::caps`] bit: the sender's transport carries frames over an
/// L2CAP connection-oriented channel (NET-26). A link is CoC only when
/// both `Hello`s set it and the local transport is one; otherwise GATT
/// remains the data channel.
pub const CAP_L2CAP: u32 = 1 << 1;

const TYPE_HELLO: u8 = 0x01;
const TYPE_DATA: u8 = 0x02;
const TYPE_PING: u8 = 0x03;
const TYPE_PONG: u8 = 0x04;
const TYPE_BYE: u8 = 0x05;
const TYPE_CREDIT: u8 = 0x06;

/// `version` + `type`.
const HEADER_LEN: usize = 2;
/// `proto_min` + `proto_max` + `caps` + `max_frame` + `tag`.
const HELLO_FIXED_LEN: usize = 1 + 1 + 4 + 4 + 16;

/// Opaque handshake blob from the layer above, at most [`MAX_APP_HELLO`]
/// bytes.
///
/// The bound is checked on construction, so every `Hello` encodes to a
/// frame its peer will accept.
///
/// # Example
///
/// ```
/// use ao_network::{AppHello, FrameError, MAX_APP_HELLO};
///
/// let blob = AppHello::new(vec![1, 2, 3])?;
/// assert_eq!(blob.as_bytes(), &[1, 2, 3]);
/// assert!(matches!(
///     AppHello::new(vec![0; MAX_APP_HELLO + 1]),
///     Err(FrameError::AppHelloTooLong { .. })
/// ));
/// # Ok::<(), FrameError>(())
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AppHello(Vec<u8>);

impl AppHello {
    /// Wraps `bytes`.
    ///
    /// # Errors
    ///
    /// [`FrameError::AppHelloTooLong`] if `bytes` exceeds [`MAX_APP_HELLO`].
    pub fn new(bytes: Vec<u8>) -> Result<Self, FrameError> {
        if bytes.len() > MAX_APP_HELLO {
            return Err(FrameError::AppHelloTooLong {
                len: bytes.len(),
                max: MAX_APP_HELLO,
            });
        }
        Ok(Self(bytes))
    }

    /// The blob's bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consumes the wrapper and returns the bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

/// The unauthenticated link handshake.
///
/// The codec preserves every value as sent; whether a range or limit is
/// acceptable is decided by the handshake (NET-10), not here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hello {
    /// Lowest link protocol version the sender speaks.
    pub proto_min: u8,
    /// Highest link protocol version the sender speaks.
    pub proto_max: u8,
    /// Capability bits; unknown bits are carried, not rejected.
    pub caps: u32,
    /// Largest frame the sender will reassemble, in bytes.
    pub max_frame: u32,
    /// Rotating link tag, used only for duplicate-link tie-breaking.
    pub tag: LocalTag,
    /// Opaque blob for the layer above.
    pub app_hello: AppHello,
}

/// One link-layer frame.
///
/// # Example
///
/// ```
/// use ao_network::{Channel, Frame, FRAME_VERSION};
///
/// let frame = Frame::Data { channel: Channel::Message, payload: vec![0xAB; 3] };
/// let bytes = frame.encode();
/// assert_eq!(bytes, [FRAME_VERSION, 0x02, 0x01, 0x03, 0xAB, 0xAB, 0xAB]);
/// assert_eq!(Frame::decode(&bytes, 16 * 1024)?, frame);
/// # Ok::<(), ao_network::FrameError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Handshake.
    Hello(Hello),
    /// Payload for the layer above on one scheduling channel.
    Data {
        /// Channel the payload was scheduled on.
        channel: Channel,
        /// Opaque payload bytes.
        payload: Vec<u8>,
    },
    /// Keepalive probe.
    Ping,
    /// Keepalive answer.
    Pong,
    /// Orderly close.
    Bye,
    /// Flow control: the receiver lets the peer send `credits` more
    /// bytes of `Data` frames on `channel`, counted as encoded.
    Credit {
        /// The channel the credits are for.
        channel: Channel,
        /// Bytes granted.
        credits: u32,
    },
}

impl Frame {
    /// Encodes this frame in the version-1 layout.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.encoded_len());
        out.push(FRAME_VERSION);
        match self {
            Self::Hello(hello) => {
                out.push(TYPE_HELLO);
                out.push(hello.proto_min);
                out.push(hello.proto_max);
                out.extend_from_slice(&hello.caps.to_be_bytes());
                out.extend_from_slice(&hello.max_frame.to_be_bytes());
                out.extend_from_slice(hello.tag.as_bytes());
                let blob = hello.app_hello.as_bytes();
                varint::encode(blob.len() as u64, &mut out);
                out.extend_from_slice(blob);
            }
            Self::Data { channel, payload } => {
                out.push(TYPE_DATA);
                out.push(channel_to_wire(*channel));
                varint::encode(payload.len() as u64, &mut out);
                out.extend_from_slice(payload);
            }
            Self::Ping => out.push(TYPE_PING),
            Self::Pong => out.push(TYPE_PONG),
            Self::Bye => out.push(TYPE_BYE),
            Self::Credit { channel, credits } => {
                out.push(TYPE_CREDIT);
                out.push(channel_to_wire(*channel));
                out.extend_from_slice(&credits.to_be_bytes());
            }
        }
        debug_assert_eq!(out.len(), self.encoded_len());
        out
    }

    /// Decodes exactly one frame from `bytes`.
    ///
    /// `max_frame` is the receiver's limit (`NetworkConfig::max_frame`).
    /// Every length is checked against the bytes actually present before
    /// any allocation, so memory use is bounded by `bytes.len()`.
    ///
    /// # Errors
    ///
    /// [`FrameError`] if the input exceeds `max_frame`, ends early, has an
    /// unsupported version, type or channel, a malformed or out-of-bounds
    /// length, or trailing bytes.
    pub fn decode(bytes: &[u8], max_frame: usize) -> Result<Self, FrameError> {
        if bytes.len() > max_frame {
            return Err(FrameError::Oversize {
                len: bytes.len(),
                max: max_frame,
            });
        }
        let mut reader = Reader { rest: bytes };
        let version = reader.u8()?;
        if version != FRAME_VERSION {
            return Err(FrameError::UnsupportedVersion { version });
        }
        let frame = match reader.u8()? {
            TYPE_HELLO => Self::Hello(decode_hello(&mut reader)?),
            TYPE_DATA => {
                let channel = channel_from_wire(reader.u8()?)?;
                let len = reader.length()?;
                let payload = reader.bytes(len)?.to_vec();
                Self::Data { channel, payload }
            }
            TYPE_PING => Self::Ping,
            TYPE_PONG => Self::Pong,
            TYPE_BYE => Self::Bye,
            TYPE_CREDIT => Self::Credit {
                channel: channel_from_wire(reader.u8()?)?,
                credits: reader.u32()?,
            },
            frame_type => return Err(FrameError::UnknownType { frame_type }),
        };
        reader.finish()?;
        Ok(frame)
    }

    fn encoded_len(&self) -> usize {
        let body = match self {
            Self::Hello(hello) => {
                let len = hello.app_hello.as_bytes().len();
                HELLO_FIXED_LEN + varint::encoded_len(len as u64) + len
            }
            Self::Data { payload, .. } => return data_frame_len(payload.len()),
            Self::Ping | Self::Pong | Self::Bye => 0,
            Self::Credit { .. } => 1 + 4,
        };
        HEADER_LEN + body
    }
}

/// Encoded size of a `Data` frame carrying `payload_len` bytes.
pub(crate) fn data_frame_len(payload_len: usize) -> usize {
    HEADER_LEN + 1 + varint::encoded_len(payload_len as u64) + payload_len
}

fn decode_hello(reader: &mut Reader<'_>) -> Result<Hello, FrameError> {
    let proto_min = reader.u8()?;
    let proto_max = reader.u8()?;
    let caps = reader.u32()?;
    let max_frame = reader.u32()?;
    let tag = LocalTag::new(reader.array()?);
    let len = reader.length()?;
    // Checked before the bytes are read, so a hostile length never reaches
    // an allocation.
    if len > MAX_APP_HELLO {
        return Err(FrameError::AppHelloTooLong {
            len,
            max: MAX_APP_HELLO,
        });
    }
    let app_hello = AppHello(reader.bytes(len)?.to_vec());
    Ok(Hello {
        proto_min,
        proto_max,
        caps,
        max_frame,
        tag,
        app_hello,
    })
}

fn channel_to_wire(channel: Channel) -> u8 {
    match channel {
        Channel::Control => 0,
        Channel::Message => 1,
        Channel::Telemetry => 2,
        Channel::Bulk => 3,
    }
}

fn channel_from_wire(byte: u8) -> Result<Channel, FrameError> {
    match byte {
        0 => Ok(Channel::Control),
        1 => Ok(Channel::Message),
        2 => Ok(Channel::Telemetry),
        3 => Ok(Channel::Bulk),
        channel => Err(FrameError::UnknownChannel { channel }),
    }
}

/// Bounds-checked cursor; every read fails with `Truncated` instead of
/// indexing past the end.
struct Reader<'a> {
    rest: &'a [u8],
}

impl<'a> Reader<'a> {
    fn u8(&mut self) -> Result<u8, FrameError> {
        let (&byte, rest) = self.rest.split_first().ok_or(FrameError::Truncated)?;
        self.rest = rest;
        Ok(byte)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], FrameError> {
        let (head, rest) = self
            .rest
            .split_first_chunk::<N>()
            .ok_or(FrameError::Truncated)?;
        self.rest = rest;
        Ok(*head)
    }

    fn u32(&mut self) -> Result<u32, FrameError> {
        self.array().map(u32::from_be_bytes)
    }

    /// Reads a LEB128 length. A value that cannot fit in `usize` cannot fit
    /// in the remaining input either, so it is reported as truncation.
    fn length(&mut self) -> Result<usize, FrameError> {
        let (value, used) = varint::decode(self.rest)?;
        self.rest = self.rest.get(used..).ok_or(FrameError::Truncated)?;
        usize::try_from(value).map_err(|_| FrameError::Truncated)
    }

    fn bytes(&mut self, len: usize) -> Result<&'a [u8], FrameError> {
        let (head, rest) = self
            .rest
            .split_at_checked(len)
            .ok_or(FrameError::Truncated)?;
        self.rest = rest;
        Ok(head)
    }

    fn finish(self) -> Result<(), FrameError> {
        if self.rest.is_empty() {
            Ok(())
        } else {
            Err(FrameError::TrailingBytes {
                extra: self.rest.len(),
            })
        }
    }
}
