//! Public-API tests for the beacon codec.

use ao_network::{BEACON_LEN, BEACON_VERSION, Beacon, BeaconError, BeaconFlags, NetError};

// Independent wire fixture: version=1, accepting+low-battery, degree=3,
// capacity=5, then distinct bytes at every hint/digest position.
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
fn encode_matches_the_independent_wire_fixture() {
    assert_eq!(announcement().encode(), WIRE);
}

#[test]
fn decode_matches_the_independent_wire_fixture() {
    assert_eq!(Beacon::decode(&WIRE), Ok(announcement()));
}

#[test]
fn every_flag_has_its_documented_bit() {
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
fn all_supported_flag_combinations_round_trip() {
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
fn every_truncated_payload_is_rejected_before_indexing() {
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
fn trailing_bytes_and_large_inputs_are_rejected() {
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
fn every_unsupported_version_is_rejected() {
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
fn every_reserved_flag_combination_is_rejected() {
    for bits in 8..=u8::MAX {
        let mut bytes = WIRE;
        bytes[1] = bits;
        assert_eq!(
            Beacon::decode(&bytes),
            Err(BeaconError::UnsupportedFlags {
                unknown: bits & 0xF8
            })
        );
    }
}

#[test]
fn peer_claims_and_opaque_bytes_are_preserved() {
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
fn error_mapping_keeps_the_original_cause() {
    let cause = BeaconError::UnsupportedVersion { version: 2 };
    assert_eq!(
        NetError::from(cause.clone()),
        NetError::InvalidBeacon(cause)
    );
}

#[test]
fn errors_report_the_rejected_header_or_length() {
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
