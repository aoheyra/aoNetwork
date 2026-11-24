//! Wire fixtures and one trigger per `FrameError` variant. Round-trip,
//! truncation, oversize and trailing-byte properties live in
//! `frame_properties.rs`.

use ao_network::{
    AppHello, CAP_CREDIT, CAP_L2CAP, Channel, Frame, FrameError, Hello, LocalTag, MAX_APP_HELLO,
};

const MAX: usize = 16 * 1024;

// Independent fixture: version, type, proto 1..2, caps and max_frame
// big-endian, the 16-byte tag, app_hello length 2, then the blob.
const HELLO_WIRE: [u8; 31] = [
    1, 0x01, 1, 2, 0x01, 0x02, 0x03, 0x04, 0x00, 0x00, 0x40, 0x00, 0x00, 0x11, 0x22, 0x33, 0x44,
    0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x02, 0xDE, 0xAD,
];

#[test]
fn hello_matches_the_wire_fixture() {
    let frame = Frame::Hello(Hello {
        proto_min: 1,
        proto_max: 2,
        caps: 0x0102_0304,
        max_frame: 0x0000_4000,
        tag: LocalTag::new([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD,
            0xEE, 0xFF,
        ]),
        app_hello: AppHello::new(vec![0xDE, 0xAD]).expect("within bound"),
    });
    assert_eq!(frame.encode(), HELLO_WIRE);
    assert_eq!(Frame::decode(&HELLO_WIRE, MAX), Ok(frame));
}

/// NET-26: CoC gets its own caps bit. Unknown bits (including the upper
/// half of the fixture's caps) already round-trip, so no frame version
/// change is needed.
#[test]
fn capability_bits_are_independent() {
    assert_eq!(CAP_CREDIT, 1);
    assert_eq!(CAP_L2CAP, 2);
    assert_eq!(CAP_CREDIT & CAP_L2CAP, 0);
    let frame = Frame::Hello(Hello {
        proto_min: 1,
        proto_max: 1,
        caps: CAP_CREDIT | CAP_L2CAP | 0x8000_0000,
        max_frame: 1024,
        tag: LocalTag::new([0; 16]),
        app_hello: AppHello::new(Vec::new()).unwrap(),
    });
    assert_eq!(Frame::decode(&frame.encode(), MAX), Ok(frame));
}

#[test]
fn every_channel_has_its_wire_byte() {
    for (channel, byte) in [
        (Channel::Control, 0),
        (Channel::Message, 1),
        (Channel::Telemetry, 2),
        (Channel::Bulk, 3),
    ] {
        let frame = Frame::Data {
            channel,
            payload: vec![0x5A],
        };
        assert_eq!(frame.encode(), [1, 0x02, byte, 0x01, 0x5A], "{channel:?}");
    }
}

#[test]
fn bodiless_frames_and_credit_match_their_fixtures() {
    let cases: [(Frame, &[u8]); 4] = [
        (Frame::Ping, &[1, 0x03]),
        (Frame::Pong, &[1, 0x04]),
        (Frame::Bye, &[1, 0x05]),
        (
            Frame::Credit {
                channel: Channel::Bulk,
                credits: 0x0A0B_0C0D,
            },
            &[1, 0x06, 0x03, 0x0A, 0x0B, 0x0C, 0x0D],
        ),
    ];
    for (frame, wire) in cases {
        assert_eq!(frame.encode(), wire, "{frame:?}");
    }
}

/// NET-25: `Credit` names its channel. An unknown channel byte is
/// refused like `Data`'s, and the reserved body without a channel, which
/// no build ever sent, no longer decodes.
#[test]
fn credit_carries_a_known_channel() {
    assert_eq!(
        Frame::decode(&[1, 0x06, 0x04, 0, 0, 0, 1], 64),
        Err(FrameError::UnknownChannel { channel: 4 })
    );
    assert_eq!(
        Frame::decode(&[1, 0x06, 0, 0, 0, 1], 64),
        Err(FrameError::Truncated)
    );
}

#[test]
fn app_hello_bound_is_inclusive() {
    assert!(AppHello::new(vec![0; MAX_APP_HELLO]).is_ok());
    assert_eq!(
        AppHello::new(vec![0; MAX_APP_HELLO + 1]),
        Err(FrameError::AppHelloTooLong {
            len: MAX_APP_HELLO + 1,
            max: MAX_APP_HELLO,
        })
    );
}

#[test]
fn size_is_checked_before_any_parsing() {
    assert_eq!(
        Frame::decode(&[0xFF; 4], 3),
        Err(FrameError::Oversize { len: 4, max: 3 })
    );
}

#[test]
fn unsupported_version_is_rejected() {
    assert_eq!(
        Frame::decode(&[2, 0x03], MAX),
        Err(FrameError::UnsupportedVersion { version: 2 })
    );
}

#[test]
fn unknown_type_is_rejected() {
    assert_eq!(
        Frame::decode(&[1, 0x07], MAX),
        Err(FrameError::UnknownType { frame_type: 0x07 })
    );
}

#[test]
fn unknown_channel_is_rejected() {
    assert_eq!(
        Frame::decode(&[1, 0x02, 4, 0], MAX),
        Err(FrameError::UnknownChannel { channel: 4 })
    );
}

#[test]
fn payload_lengths_use_leb128() {
    for (len, prefix) in [
        (0, &[0x00][..]),
        (127, &[0x7F]),
        (128, &[0x80, 0x01]),
        (300, &[0xAC, 0x02]),
    ] {
        let wire = Frame::Data {
            channel: Channel::Control,
            payload: vec![0; len],
        }
        .encode();
        assert_eq!(&wire[3..3 + prefix.len()], prefix, "length {len}");
    }
}

#[test]
fn non_canonical_lengths_are_rejected() {
    // Overlong: a trailing zero group.
    assert_eq!(
        Frame::decode(&[1, 0x02, 0, 0x80, 0x00], MAX),
        Err(FrameError::InvalidVarint)
    );
    // Overflow: the tenth group carries more than bit 63.
    let mut wire = vec![1, 0x02, 0];
    wire.extend_from_slice(&[0xFF; 9]);
    wire.push(0x02);
    assert_eq!(Frame::decode(&wire, MAX), Err(FrameError::InvalidVarint));
}

#[test]
fn huge_declared_length_is_truncated_without_allocating() {
    // Claims u64::MAX payload bytes (the longest valid LEB128) in a 13-byte
    // input.
    let mut wire = vec![1, 0x02, 0];
    wire.extend_from_slice(&[0xFF; 9]);
    wire.push(0x01);
    assert_eq!(Frame::decode(&wire, MAX), Err(FrameError::Truncated));
}

#[test]
fn oversized_app_hello_length_is_rejected_before_reading() {
    // The fixture's header with length 257 (0x81 0x02) and no blob.
    let mut wire = HELLO_WIRE[..28].to_vec();
    wire.extend_from_slice(&[0x81, 0x02]);
    assert_eq!(
        Frame::decode(&wire, MAX),
        Err(FrameError::AppHelloTooLong {
            len: MAX_APP_HELLO + 1,
            max: MAX_APP_HELLO,
        })
    );
}
