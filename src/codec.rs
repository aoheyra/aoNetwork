//! Hand-rolled, versioned wire codec for link-management frames.
//!
//! This module intentionally avoids `serde` entirely. It is the stable on-wire
//! format for every link-management frame and uses a type byte plus LEB128
//! lengths for all variable-length payloads.

mod beacon;

pub use beacon::{BEACON_LEN, BEACON_VERSION, Beacon, BeaconFlags};

use crate::Channel;

const FRAME_VERSION: u8 = 1;
const TYPE_HELLO: u8 = 1;
const TYPE_DATA: u8 = 2;
const TYPE_PING: u8 = 3;
const TYPE_PONG: u8 = 4;
const TYPE_BYE: u8 = 5;
const TYPE_CREDIT: u8 = 6;

/// Maximum number of bytes accepted in an `app_hello` payload.
const MAX_APP_HELLO_LEN: usize = 256;
/// Maximum payload size accepted as a `Data` frame body.
const MAX_DATA_PAYLOAD_LEN: usize = 1024 * 1024;
/// Maximum size accepted for a frame payload that carries its own implicit length.
const MAX_FRAME_LEN: usize = 1024 * 1024;

/// Errors returned when decoding a malformed or truncated frame.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FrameCodecError {
    /// The frame ended before a required byte or varint was available.
    #[error("truncated frame")]
    Truncated,

    /// The frame version is not one the implementation accepts.
    #[error("unsupported frame version: {0}")]
    UnsupportedVersion(u8),

    /// A type byte did not match any supported frame variant.
    #[error("unsupported frame type: {0}")]
    UnsupportedFrameType(u8),

    /// A length field for a variable-length payload is larger than the accepted bound.
    #[error("{field} length out of range: {value} exceeds {max}")]
    LengthOutOfRange {
        /// Which field was out of range.
        field: &'static str,
        /// The offending length value.
        value: usize,
        /// The accepted maximum length.
        max: usize,
    },

    /// The channel byte does not match a valid scheduling channel.
    #[error("invalid channel value: {0}")]
    InvalidChannel(u8),
}

/// A `Hello` frame payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HelloFrame {
    /// Minimum protocol version the sender supports.
    pub proto_min: u8,
    /// Maximum protocol version the sender supports.
    pub proto_max: u8,
    /// Capability flags for the link.
    pub caps: u32,
    /// Maximum frame size the sender can reassemble.
    pub max_frame: u32,
    /// Opaque link tag used for duplicate-link tie-breaking.
    pub tag: [u8; 16],
    /// Opaque app-specific hello bytes, bounded to 256 bytes.
    pub app_hello: Vec<u8>,
}

impl HelloFrame {
    /// Construct a `HelloFrame` while enforcing the protocol's strict upper bounds.
    pub fn new(
        proto_min: u8,
        proto_max: u8,
        caps: u32,
        max_frame: u32,
        tag: [u8; 16],
        app_hello: Vec<u8>,
    ) -> Result<Self, FrameCodecError> {
        if app_hello.len() > MAX_APP_HELLO_LEN {
            return Err(FrameCodecError::LengthOutOfRange {
                field: "app_hello",
                value: app_hello.len(),
                max: MAX_APP_HELLO_LEN,
            });
        }
        if max_frame as usize > MAX_FRAME_LEN {
            return Err(FrameCodecError::LengthOutOfRange {
                field: "max_frame",
                value: max_frame as usize,
                max: MAX_FRAME_LEN,
            });
        }
        Ok(Self {
            proto_min,
            proto_max,
            caps,
            max_frame,
            tag,
            app_hello,
        })
    }
}

/// The canonical link-layer frame types exchanged over the radio.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Link negotiation and capabilities exchange.
    Hello(HelloFrame),
    /// A payload frame sent on a scheduling channel.
    Data {
        /// The channel this frame belongs to.
        channel: Channel,
        /// The opaque payload bytes.
        payload: Vec<u8>,
    },
    /// Link keepalive request.
    Ping,
    /// Link keepalive reply.
    Pong,
    /// Graceful peer shutdown.
    Bye,
    /// Reserved credit indicator for future flow-control work.
    Credit {
        /// Reserved credit byte, currently protocol-defined to be opaque.
        reserved: u8,
    },
}

impl Frame {
    /// Encodes this frame into a stable big-endian byte buffer.
    pub fn encode(&self) -> Result<Vec<u8>, FrameCodecError> {
        match self {
            Self::Hello(hello) => encode_hello(hello),
            Self::Data { channel, payload } => encode_data(*channel, payload),
            Self::Ping => Ok(vec![TYPE_PING, FRAME_VERSION]),
            Self::Pong => Ok(vec![TYPE_PONG, FRAME_VERSION]),
            Self::Bye => Ok(vec![TYPE_BYE, FRAME_VERSION]),
            Self::Credit { reserved } => Ok(vec![TYPE_CREDIT, FRAME_VERSION, *reserved]),
        }
    }

    /// Decodes a byte slice into a frame value.
    pub fn decode(bytes: &[u8]) -> Result<Self, FrameCodecError> {
        if bytes.is_empty() {
            return Err(FrameCodecError::Truncated);
        }
        let frame_type = bytes[0];
        if bytes.len() < 2 {
            return Err(FrameCodecError::Truncated);
        }
        let version = bytes[1];
        if version != FRAME_VERSION {
            return Err(FrameCodecError::UnsupportedVersion(version));
        }

        match frame_type {
            TYPE_HELLO => decode_hello(&bytes[2..]),
            TYPE_DATA => decode_data(&bytes[2..]),
            TYPE_PING if bytes.len() == 2 => Ok(Self::Ping),
            TYPE_PONG if bytes.len() == 2 => Ok(Self::Pong),
            TYPE_BYE if bytes.len() == 2 => Ok(Self::Bye),
            TYPE_CREDIT if bytes.len() == 3 => Ok(Self::Credit { reserved: bytes[2] }),
            TYPE_PING | TYPE_PONG | TYPE_BYE => Err(FrameCodecError::Truncated),
            TYPE_CREDIT => Err(FrameCodecError::Truncated),
            other => Err(FrameCodecError::UnsupportedFrameType(other)),
        }
    }
}

fn encode_hello(frame: &HelloFrame) -> Result<Vec<u8>, FrameCodecError> {
    if frame.app_hello.len() > MAX_APP_HELLO_LEN {
        return Err(FrameCodecError::LengthOutOfRange {
            field: "app_hello",
            value: frame.app_hello.len(),
            max: MAX_APP_HELLO_LEN,
        });
    }
    let mut out = Vec::with_capacity(2 + 1 + 1 + 4 + 4 + 16 + 5 + frame.app_hello.len());
    out.push(TYPE_HELLO);
    out.push(FRAME_VERSION);
    out.push(frame.proto_min);
    out.push(frame.proto_max);
    out.extend_from_slice(&frame.caps.to_be_bytes());
    out.extend_from_slice(&frame.max_frame.to_be_bytes());
    out.extend_from_slice(&frame.tag);
    let len_bytes = encode_uleb128(frame.app_hello.len(), MAX_APP_HELLO_LEN)?;
    out.extend_from_slice(&len_bytes);
    out.extend_from_slice(&frame.app_hello);
    Ok(out)
}

fn decode_hello(bytes: &[u8]) -> Result<Frame, FrameCodecError> {
    if bytes.len() < 1 + 1 + 4 + 4 + 16 {
        return Err(FrameCodecError::Truncated);
    }
    let proto_min = bytes[0];
    let proto_max = bytes[1];
    let caps = u32::from_be_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);
    let max_frame = u32::from_be_bytes([bytes[6], bytes[7], bytes[8], bytes[9]]);
    if bytes.len() < 10 + 16 {
        return Err(FrameCodecError::Truncated);
    }
    let tag = {
        let start = 10;
        let end = start + 16;
        let mut value = [0u8; 16];
        value.copy_from_slice(&bytes[start..end]);
        value
    };
    let mut cursor = 10 + 16;
    let app_hello_len = decode_uleb128(bytes, &mut cursor, "app_hello", MAX_APP_HELLO_LEN)?;
    if cursor + app_hello_len > bytes.len() {
        return Err(FrameCodecError::Truncated);
    }
    let app_hello = bytes[cursor..cursor + app_hello_len].to_vec();
    let frame = HelloFrame::new(proto_min, proto_max, caps, max_frame, tag, app_hello)?;
    Ok(Frame::Hello(frame))
}

fn encode_data(channel: Channel, payload: &[u8]) -> Result<Vec<u8>, FrameCodecError> {
    if payload.len() > MAX_DATA_PAYLOAD_LEN {
        return Err(FrameCodecError::LengthOutOfRange {
            field: "payload",
            value: payload.len(),
            max: MAX_DATA_PAYLOAD_LEN,
        });
    }
    let channel_id = match channel {
        Channel::Control => 0,
        Channel::Message => 1,
        Channel::Telemetry => 2,
        Channel::Bulk => 3,
    };
    let mut out = Vec::with_capacity(2 + 1 + 5 + payload.len());
    out.push(TYPE_DATA);
    out.push(FRAME_VERSION);
    out.push(channel_id);
    let len_bytes = encode_uleb128(payload.len(), MAX_DATA_PAYLOAD_LEN)?;
    out.extend_from_slice(&len_bytes);
    out.extend_from_slice(payload);
    Ok(out)
}

fn decode_data(bytes: &[u8]) -> Result<Frame, FrameCodecError> {
    if bytes.is_empty() {
        return Err(FrameCodecError::Truncated);
    }
    let channel_id = bytes[0];
    let channel = match channel_id {
        0 => Channel::Control,
        1 => Channel::Message,
        2 => Channel::Telemetry,
        3 => Channel::Bulk,
        other => return Err(FrameCodecError::InvalidChannel(other)),
    };
    let mut cursor = 1;
    let payload_len = decode_uleb128(bytes, &mut cursor, "payload", MAX_DATA_PAYLOAD_LEN)?;
    if cursor + payload_len > bytes.len() {
        return Err(FrameCodecError::Truncated);
    }
    let payload = bytes[cursor..cursor + payload_len].to_vec();
    Ok(Frame::Data { channel, payload })
}

fn encode_uleb128(value: usize, max: usize) -> Result<Vec<u8>, FrameCodecError> {
    if value > max {
        return Err(FrameCodecError::LengthOutOfRange {
            field: "length",
            value,
            max,
        });
    }
    let mut encoded = Vec::new();
    let mut remaining = value as u64;
    loop {
        let mut byte = (remaining & 0x7F) as u8;
        remaining >>= 7;
        if remaining != 0 {
            byte |= 0x80;
        }
        encoded.push(byte);
        if remaining == 0 {
            break;
        }
    }
    Ok(encoded)
}

fn decode_uleb128(
    bytes: &[u8],
    cursor: &mut usize,
    field: &'static str,
    max: usize,
) -> Result<usize, FrameCodecError> {
    let mut value: usize = 0;
    let mut shift: usize = 0;

    loop {
        if *cursor >= bytes.len() {
            return Err(FrameCodecError::Truncated);
        }
        let byte = bytes[*cursor];
        *cursor += 1;

        let chunk = ((byte & 0x7F) as usize) << shift;
        value = value
            .checked_add(chunk)
            .ok_or(FrameCodecError::LengthOutOfRange {
                field,
                value: usize::MAX,
                max,
            })?;

        if byte & 0x80 == 0 {
            if value > max {
                return Err(FrameCodecError::LengthOutOfRange { field, value, max });
            }
            return Ok(value);
        }

        shift = shift
            .checked_add(7)
            .ok_or(FrameCodecError::LengthOutOfRange {
                field,
                value: usize::MAX,
                max,
            })?;
        if shift > usize::BITS as usize {
            return Err(FrameCodecError::LengthOutOfRange {
                field,
                value: value.saturating_add(1),
                max,
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn hello_round_trips() {
        let frame = Frame::Hello(
            HelloFrame::new(1, 2, 0x01020304, 4096, [9u8; 16], vec![1, 2, 3, 4, 5])
                .expect("valid hello frame should be constructible"),
        );
        let encoded = frame.encode().expect("hello should encode");
        let decoded = Frame::decode(&encoded).expect("hello should decode");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn data_round_trips() {
        let frame = Frame::Data {
            channel: Channel::Control,
            payload: vec![0xAA, 0xBB, 0xCC, 0xDD],
        };
        let encoded = frame.encode().expect("data should encode");
        let decoded = Frame::decode(&encoded).expect("data should decode");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn control_frames_round_trip() {
        for frame in [
            Frame::Ping,
            Frame::Pong,
            Frame::Bye,
            Frame::Credit { reserved: 7 },
        ] {
            let encoded = frame.encode().expect("control frame should encode");
            let decoded = Frame::decode(&encoded).expect("control frame should decode");
            assert_eq!(decoded, frame);
        }
    }

    #[test]
    fn malformed_frames_return_errors() {
        assert!(matches!(
            Frame::decode(&[]),
            Err(FrameCodecError::Truncated)
        ));
        assert!(matches!(
            Frame::decode(&[TYPE_HELLO, 0]),
            Err(FrameCodecError::UnsupportedVersion(0))
        ));
        assert!(matches!(
            Frame::decode(&[TYPE_HELLO, FRAME_VERSION]),
            Err(FrameCodecError::Truncated)
        ));
        assert!(matches!(
            Frame::decode(&[TYPE_DATA, FRAME_VERSION, 9]),
            Err(FrameCodecError::InvalidChannel(9))
        ));
        assert!(matches!(
            Frame::decode(&[TYPE_CREDIT, FRAME_VERSION]),
            Err(FrameCodecError::Truncated)
        ));
    }

    proptest! {
        #[test]
        fn arbitrary_valid_frames_round_trip(frame in any_valid_frame()) {
            let encoded = frame.encode().expect("valid frame should encode");
            let decoded = Frame::decode(&encoded).expect("valid frame should decode");
            assert_eq!(decoded, frame);
        }

        #[test]
        fn malformed_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
            let _ = Frame::decode(&bytes);
        }
    }

    fn any_valid_frame() -> impl Strategy<Value = Frame> {
        prop_oneof![
            (
                0u8..=255u8,
                0u8..=255u8,
                0u32..=0x00FF_FFFF,
                0u32..=4096,
                any::<[u8; 16]>(),
                prop::collection::vec(any::<u8>(), 0..=MAX_APP_HELLO_LEN)
            )
                .prop_filter(
                    "hello app_hello length",
                    |(_, _, _, _, _, app_hello)| app_hello.len() <= MAX_APP_HELLO_LEN
                )
                .prop_map(|(proto_min, proto_max, caps, max_frame, tag, app_hello)| {
                    Frame::Hello(
                        HelloFrame::new(proto_min, proto_max, caps, max_frame, tag, app_hello)
                            .unwrap(),
                    )
                }),
            (
                prop::sample::select(vec![
                    Channel::Control,
                    Channel::Message,
                    Channel::Telemetry,
                    Channel::Bulk
                ]),
                prop::collection::vec(any::<u8>(), 0..=64)
            )
                .prop_map(|(channel, payload)| Frame::Data { channel, payload }),
            Just(Frame::Ping),
            Just(Frame::Pong),
            Just(Frame::Bye),
            (0u8..=u8::MAX).prop_map(|reserved| Frame::Credit { reserved }),
        ]
    }
}
