use ao_network::{
    BEACON_LEN, BEACON_VERSION, Beacon, BeaconError, BeaconFlags, Channel, Frame, FrameCodecError,
    HelloFrame, NetError,
};
use proptest::prelude::*;
use proptest::test_runner::{Config, MapFailurePersistence, RngSeed};

const FRAME_VERSION: u8 = 1;
const TYPE_HELLO: u8 = 1;
const TYPE_DATA: u8 = 2;
const TYPE_PING: u8 = 3;
const TYPE_PONG: u8 = 4;
const TYPE_BYE: u8 = 5;
const TYPE_CREDIT: u8 = 6;

const WIRE: [u8; 16] = [
    1, 5, 3, 5, 0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0xDE, 0xAD, 0xBE, 0xEF,
];

fn announcement() -> Beacon {
    Beacon {
        flags: BeaconFlags {
            accepting: true,
            relay_willing: false,
            low_battery: true,
        },
        degree: 3,
        max_links: 5,
        hint: [0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77],
        digest: [0xDE, 0xAD, 0xBE, 0xEF],
    }
}

#[test]
fn frame_hello_round_trips() {
    let frame = Frame::Hello(
        HelloFrame::new(1, 2, 0x01020304, 4096, [9u8; 16], vec![1, 2, 3, 4, 5])
            .expect("valid hello frame should be constructible"),
    );
    let encoded = frame.encode().expect("hello should encode");
    let decoded = Frame::decode(&encoded).expect("hello should decode");
    assert_eq!(decoded, frame);
}

#[test]
fn frame_data_round_trips() {
    let frame = Frame::Data {
        channel: Channel::Control,
        payload: vec![0xAA, 0xBB, 0xCC, 0xDD],
    };
    let encoded = frame.encode().expect("data should encode");
    let decoded = Frame::decode(&encoded).expect("data should decode");
    assert_eq!(decoded, frame);
}

#[test]
fn frame_control_frames_round_trip() {
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
fn frame_malformed_frames_return_errors() {
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
    assert_eq!(Frame::decode(&[TYPE_PING, FRAME_VERSION]), Ok(Frame::Ping));
    assert_eq!(Frame::decode(&[TYPE_PONG, FRAME_VERSION]), Ok(Frame::Pong));
    assert_eq!(Frame::decode(&[TYPE_BYE, FRAME_VERSION]), Ok(Frame::Bye));
    assert!(matches!(
        Frame::decode(&[TYPE_DATA, FRAME_VERSION, 9]),
        Err(FrameCodecError::InvalidChannel(9))
    ));
    assert!(matches!(
        Frame::decode(&[TYPE_CREDIT, FRAME_VERSION]),
        Err(FrameCodecError::Truncated)
    ));
}

#[test]
fn beacon_encode_matches_the_independent_wire_fixture() {
    assert_eq!(announcement().encode(), WIRE);
}

#[test]
fn beacon_decode_matches_the_independent_wire_fixture() {
    assert_eq!(Beacon::decode(&WIRE), Ok(announcement()));
}

#[test]
fn beacon_every_flag_has_its_documented_bit() {
    for (flags, bits) in [
        (BeaconFlags::default(), 0),
        (
            BeaconFlags {
                accepting: true,
                ..BeaconFlags::default()
            },
            1,
        ),
        (
            BeaconFlags {
                relay_willing: true,
                ..BeaconFlags::default()
            },
            2,
        ),
        (
            BeaconFlags {
                low_battery: true,
                ..BeaconFlags::default()
            },
            4,
        ),
        (
            BeaconFlags {
                accepting: true,
                relay_willing: true,
                low_battery: true,
            },
            7,
        ),
    ] {
        let mut beacon = announcement();
        beacon.flags = flags;
        let mut bytes = WIRE;
        bytes[1] = bits;
        assert_eq!(beacon.encode(), bytes);
        assert_eq!(Beacon::decode(&bytes), Ok(beacon));
    }
}

#[test]
fn beacon_all_supported_flag_combinations_round_trip() {
    for bits in 0..=7 {
        let mut bytes = WIRE;
        bytes[1] = bits;
        let expected = Beacon {
            flags: BeaconFlags {
                accepting: bits & 1 != 0,
                relay_willing: bits & 2 != 0,
                low_battery: bits & 4 != 0,
            },
            ..announcement()
        };
        assert_eq!(Beacon::decode(&bytes), Ok(expected));
        assert_eq!(expected.encode(), bytes);
    }
}

#[test]
fn beacon_every_truncated_payload_is_rejected_before_indexing() {
    for length in 0..BEACON_LEN {
        assert_eq!(
            Beacon::decode(&WIRE[..length]),
            Err(BeaconError::InvalidLength {
                expected: BEACON_LEN,
                actual: length,
            })
        );
    }
}

#[test]
fn beacon_trailing_bytes_and_large_inputs_are_rejected() {
    let mut extra = WIRE.to_vec();
    extra.push(0);
    for bytes in [extra.as_slice(), &[0; 4096][..]] {
        assert_eq!(
            Beacon::decode(bytes),
            Err(BeaconError::InvalidLength {
                expected: BEACON_LEN,
                actual: bytes.len(),
            })
        );
    }
}

#[test]
fn beacon_every_unsupported_version_is_rejected() {
    for version in 0..=u8::MAX {
        if version == BEACON_VERSION {
            continue;
        }
        let mut bytes = WIRE;
        bytes[0] = version;
        assert_eq!(
            Beacon::decode(&bytes),
            Err(BeaconError::UnsupportedVersion { version })
        );
    }
}

#[test]
fn beacon_every_reserved_flag_combination_is_rejected() {
    for bits in 8..=u8::MAX {
        let mut bytes = WIRE;
        bytes[1] = bits;
        assert_eq!(
            Beacon::decode(&bytes),
            Err(BeaconError::UnsupportedFlags {
                unknown: bits & 0xF8,
            })
        );
    }
}

#[test]
fn beacon_peer_claims_and_opaque_bytes_are_preserved() {
    let beacon = Beacon {
        degree: u8::MAX,
        max_links: 0,
        hint: [0xFF; 8],
        digest: [0; 4],
        ..announcement()
    };
    assert_eq!(Beacon::decode(&beacon.encode()), Ok(beacon));
}

#[test]
fn beacon_error_mapping_keeps_the_original_cause() {
    let cause = BeaconError::UnsupportedVersion { version: 2 };
    assert_eq!(
        NetError::from(cause.clone()),
        NetError::InvalidBeacon(cause)
    );
}

#[test]
fn beacon_errors_report_the_rejected_header_or_length() {
    let length = BeaconError::InvalidLength {
        expected: 16,
        actual: 3,
    }
    .to_string();
    assert!(length.contains("16") && length.contains('3'));
    assert!(
        BeaconError::UnsupportedVersion { version: 99 }
            .to_string()
            .contains("99")
    );
    assert!(
        BeaconError::UnsupportedFlags { unknown: 0x80 }
            .to_string()
            .contains("0x80")
    );
}

fn config() -> Config {
    let mut config = Config::with_cases(256);
    config.rng_seed = RngSeed::Fixed(0xA006_0001);
    config.failure_persistence = Some(Box::new(MapFailurePersistence::default()));
    config
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn frame_arbitrary_valid_frames_round_trip(frame in any_valid_frame()) {
        let encoded = frame.encode().expect("valid frame should encode");
        let decoded = Frame::decode(&encoded).expect("valid frame should decode");
        assert_eq!(decoded, frame);
    }

    #[test]
    fn frame_malformed_bytes_never_panic(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        let _ = Frame::decode(&bytes);
    }

    #[test]
    fn beacon_supported_values_round_trip(
        accepting in any::<bool>(),
        relay_willing in any::<bool>(),
        low_battery in any::<bool>(),
        degree in any::<u8>(),
        max_links in any::<u8>(),
        hint in any::<[u8; 8]>(),
        digest in any::<[u8; 4]>(),
    ) {
        let beacon = Beacon {
            flags: BeaconFlags {
                accepting,
                relay_willing,
                low_battery,
            },
            degree,
            max_links,
            hint,
            digest,
        };
        prop_assert_eq!(Beacon::decode(&beacon.encode()), Ok(beacon));
    }

    #[test]
    fn beacon_arbitrary_buffers_are_panic_free_and_accepted_bytes_are_canonical(
        input in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        if let Ok(beacon) = Beacon::decode(&input) {
            prop_assert_eq!(input.len(), BEACON_LEN);
            prop_assert_eq!(input[0], BEACON_VERSION);
            let encoded = beacon.encode();
            prop_assert_eq!(encoded.as_slice(), input.as_slice());
        }
    }

    #[test]
    fn beacon_arbitrary_version_one_headers_preserve_every_accepted_byte(
        mut input in any::<[u8; BEACON_LEN]>(),
    ) {
        input[0] = BEACON_VERSION;
        if let Ok(beacon) = Beacon::decode(&input) {
            prop_assert_eq!(beacon.encode(), input);
        }
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
            prop::collection::vec(any::<u8>(), 0..=256)
        )
            .prop_filter(
                "hello app_hello length",
                |(_, _, _, _, _, app_hello)| app_hello.len() <= 256
            )
            .prop_map(|(proto_min, proto_max, caps, max_frame, tag, app_hello)| {
                Frame::Hello(
                    HelloFrame::new(proto_min, proto_max, caps, max_frame, tag, app_hello).unwrap(),
                )
            }),
        (
            prop::sample::select(vec![
                Channel::Control,
                Channel::Message,
                Channel::Telemetry,
                Channel::Bulk,
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
