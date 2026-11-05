use ao_network::{
    ConfigError, FragmentError, Fragmenter, MAX_MTU, MIN_MTU, NetError, NetworkConfig, Reassembler,
    Reassembly,
};
use std::time::Duration;

fn wire(header: u8, len: usize) -> Vec<u8> {
    let mut bytes = vec![0x5a; len + 1];
    bytes[0] = header;
    bytes
}

fn round_trip(frame: &[u8], mtu: u16, config: &NetworkConfig) {
    let mut receiver = Reassembler::new(mtu, config).unwrap();
    let mut count = 0;
    let mut result = None;
    let mut out = [0xee; MAX_MTU as usize - 3];
    for chunk in Fragmenter::new(frame, mtu, config).unwrap() {
        assert_eq!(chunk.header() & 7, (count % 8) as u8);
        assert_eq!(chunk.header() & 0x80 != 0, count == 0);
        assert!(!chunk.payload().is_empty());
        let len = chunk.encode_into(&mut out).unwrap();
        assert!(len <= mtu as usize - 3);
        count += 1;
        match receiver.push(Duration::ZERO, &out[..len]).unwrap() {
            Reassembly::Pending => assert!(receiver.is_busy()),
            Reassembly::Complete(bytes) => {
                assert!(result.replace(bytes).is_none());
            }
        }
        assert!(receiver.buffered_len() <= config.max_frame());
    }
    assert_eq!(result.as_deref(), Some(frame));
    assert!(!receiver.is_busy());
    assert_eq!(receiver.buffered_len(), 0);
    assert_eq!(count, frame.len().div_ceil(mtu as usize - 4));
}

#[test]
fn boundary_frames_round_trip_at_every_supported_mtu() {
    let config = NetworkConfig::default();
    for mtu in MIN_MTU..=MAX_MTU {
        let cap = mtu as usize - 4;
        for len in [1, cap - 1, cap, cap + 1, 8 * cap + 1, 4096] {
            let frame: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
            round_trip(&frame, mtu, &config);
        }
    }
}

#[test]
fn independent_chunks_have_expected_bits_and_sequence_wrap() {
    let config = NetworkConfig::default();
    let frame: Vec<u8> = (0..172).collect();
    let mut fragments = Fragmenter::new(&frame, 23, &config).unwrap();
    let headers: Vec<u8> = fragments.by_ref().map(|chunk| chunk.header()).collect();
    assert_eq!(headers, [0x80, 1, 2, 3, 4, 5, 6, 7, 0, 0x41]);
    assert!(fragments.next().is_none());
    assert!(fragments.next().is_none());
    let chunk = Fragmenter::new(&[0xaa, 0xbb], 23, &config)
        .unwrap()
        .next()
        .unwrap();
    let mut out = [0xee; 5];
    assert_eq!(chunk.encode_into(&mut out).unwrap(), 3);
    assert_eq!(out, [0xc0, 0xaa, 0xbb, 0xee, 0xee]);
    let mut short = [0xee; 2];
    assert_eq!(
        chunk.encode_into(&mut short),
        Err(FragmentError::OutputTooSmall {
            required: 3,
            actual: 2
        })
    );
    assert_eq!(short, [0xee; 2]);
}

fn assert_reset(receiver: &mut Reassembler) {
    assert!(!receiver.is_busy());
    assert_eq!(receiver.buffered_len(), 0);
    assert_eq!(
        receiver.push(Duration::ZERO, &[0x41, 1]),
        Err(FragmentError::UnexpectedContinuation)
    );
    assert_eq!(
        receiver.push(Duration::ZERO, &[0xc0, 9]),
        Ok(Reassembly::Complete(vec![9]))
    );
}

#[test]
fn all_structural_receive_errors_drop_the_in_flight_frame() {
    let config = NetworkConfig::default().with_max_frame(64).unwrap();
    let cases = [
        (vec![], FragmentError::EmptyChunk),
        (vec![0x41], FragmentError::EmptyChunk),
        (
            wire(1, 20),
            FragmentError::ChunkTooLarge {
                actual: 21,
                max: 20,
            },
        ),
        (wire(0x09, 1), FragmentError::UnsupportedHeader(8)),
        (
            wire(2, 1),
            FragmentError::SeqGap {
                expected: 1,
                actual: 2,
            },
        ),
        (wire(0x80, 1), FragmentError::StartWhileBusy),
        (wire(0xc0, 1), FragmentError::StartWhileBusy),
    ];
    for (chunk, expected) in cases {
        let mut receiver = Reassembler::new(23, &config).unwrap();
        assert_eq!(
            receiver.push(Duration::ZERO, &wire(0x80, 19)),
            Ok(Reassembly::Pending)
        );
        assert_eq!(receiver.push(Duration::ZERO, &chunk), Err(expected));
        assert_reset(&mut receiver);
    }
}

#[test]
fn start_sequence_continuations_and_reserved_bits_fail_closed() {
    let config = NetworkConfig::default();
    for sequence in 1..=7 {
        let mut receiver = Reassembler::new(23, &config).unwrap();
        assert_eq!(
            receiver.push(Duration::ZERO, &wire(0x80 | sequence, 1)),
            Err(FragmentError::SeqGap {
                expected: 0,
                actual: sequence
            })
        );
        assert_reset(&mut receiver);
    }
    for header in 0..=255u8 {
        if header & 0x38 != 0 {
            let mut receiver = Reassembler::new(23, &config).unwrap();
            assert_eq!(
                receiver.push(Duration::ZERO, &wire(header, 1)),
                Err(FragmentError::UnsupportedHeader(header & 0x38))
            );
            assert_reset(&mut receiver);
        }
    }
    let mut receiver = Reassembler::new(23, &config).unwrap();
    assert_eq!(
        receiver.push(Duration::ZERO, &wire(0x41, 1)),
        Err(FragmentError::UnexpectedContinuation)
    );
    assert_reset(&mut receiver);
}

#[test]
fn duplicates_missing_chunks_and_reordering_abort_reassembly() {
    for headers in [vec![0x80, 1, 1], vec![0x80, 1, 3], vec![0x80, 2]] {
        let mut receiver = Reassembler::new(23, &NetworkConfig::default()).unwrap();
        let mut error = None;
        for header in headers {
            if let Err(cause) = receiver.push(Duration::ZERO, &wire(header, 1)) {
                error = Some(cause);
            }
        }
        assert!(matches!(error, Some(FragmentError::SeqGap { .. })));
        assert_reset(&mut receiver);
    }
}

#[test]
fn frame_cap_is_enforced_before_start_and_on_every_continuation() {
    let config = NetworkConfig::default().with_max_frame(64).unwrap();
    let mut receiver = Reassembler::new(517, &config).unwrap();
    for header in [0x80, 0xc0] {
        assert_eq!(
            receiver.push(Duration::ZERO, &wire(header, 65)),
            Err(FragmentError::TooLarge {
                actual: 65,
                max: 64
            })
        );
        assert_reset(&mut receiver);
    }
    receiver.push(Duration::ZERO, &wire(0x80, 64)).unwrap();
    assert_eq!(
        receiver.push(Duration::ZERO, &wire(0x41, 1)),
        Err(FragmentError::TooLarge {
            actual: 65,
            max: 64
        })
    );
    assert_reset(&mut receiver);
    assert!(matches!(
        Fragmenter::new(&[0; 65], 23, &config),
        Err(FragmentError::TooLarge {
            actual: 65,
            max: 64
        })
    ));
    round_trip(&[0xab; 64], 23, &config);
}

#[test]
fn total_deadline_does_not_refresh_and_timeout_drops_the_late_chunk() {
    let config = NetworkConfig::default()
        .with_reassembly_timeout(Duration::from_millis(10))
        .unwrap();
    let mut receiver = Reassembler::new(23, &config).unwrap();
    receiver
        .push(Duration::from_millis(100), &wire(0x80, 1))
        .unwrap();
    receiver
        .push(Duration::from_millis(109), &wire(1, 1))
        .unwrap();
    assert_eq!(
        receiver.push(Duration::from_millis(110), &wire(0x42, 1)),
        Err(FragmentError::Timeout)
    );
    assert_reset(&mut receiver);
    receiver.push(Duration::ZERO, &wire(0x80, 1)).unwrap();
    assert_eq!(
        receiver.tick(Duration::from_millis(10)),
        Err(FragmentError::Timeout)
    );
    assert_reset(&mut receiver);
    receiver.push(Duration::ZERO, &wire(0x80, 1)).unwrap();
    assert_eq!(
        receiver.push(Duration::from_millis(9), &wire(0x41, 1)),
        Ok(Reassembly::Complete(vec![0x5a; 2]))
    );
    assert_eq!(receiver.tick(Duration::MAX), Ok(()));
}

#[test]
fn injected_time_is_monotonic_and_never_overflows_a_deadline() {
    let config = NetworkConfig::default()
        .with_reassembly_timeout(Duration::from_secs(2))
        .unwrap();
    for through_tick in [false, true] {
        let mut receiver = Reassembler::new(23, &config).unwrap();
        receiver
            .push(Duration::from_secs(2), &wire(0x80, 1))
            .unwrap();
        let result = if through_tick {
            receiver.tick(Duration::from_secs(1))
        } else {
            receiver
                .push(Duration::from_secs(1), &wire(0x41, 1))
                .map(|_| ())
        };
        assert_eq!(result, Err(FragmentError::TimeWentBackwards));
        assert_reset(&mut receiver);
    }
    let mut receiver = Reassembler::new(23, &config).unwrap();
    receiver
        .push(Duration::MAX - Duration::from_secs(1), &wire(0x80, 1))
        .unwrap();
    assert_eq!(
        receiver.push(Duration::MAX, &wire(0x41, 1)),
        Ok(Reassembly::Complete(vec![0x5a; 2]))
    );
}

#[test]
fn disconnect_reset_keeps_links_independent() {
    let config = NetworkConfig::default();
    let mut a = Reassembler::new(23, &config).unwrap();
    let mut b = Reassembler::new(23, &config).unwrap();
    a.push(Duration::ZERO, &[0x80, 1]).unwrap();
    b.push(Duration::ZERO, &[0x80, 2]).unwrap();
    a.reset();
    assert_reset(&mut a);
    assert_eq!(
        b.push(Duration::ZERO, &[0x41, 3]),
        Ok(Reassembly::Complete(vec![2, 3]))
    );
}

#[test]
fn mtu_empty_frame_config_defaults_and_typed_error_mapping() {
    let default = NetworkConfig::default();
    assert_eq!(default.reassembly_timeout(), Duration::from_secs(10));
    assert_eq!(
        NetworkConfig::default().with_reassembly_timeout(Duration::ZERO),
        Err(ConfigError::ReassemblyTimeoutZero)
    );
    assert_eq!(
        NetworkConfig::default()
            .with_reassembly_timeout(Duration::from_nanos(1))
            .unwrap()
            .reassembly_timeout(),
        Duration::from_nanos(1)
    );
    for mtu in [0, 1, 22, 518, u16::MAX] {
        assert!(
            matches!(Fragmenter::new(&[1], mtu, &default), Err(FragmentError::InvalidMtu(v)) if v == mtu)
        );
        assert!(
            matches!(Reassembler::new(mtu, &default), Err(FragmentError::InvalidMtu(v)) if v == mtu)
        );
    }
    assert!(matches!(
        Fragmenter::new(&[], 23, &default),
        Err(FragmentError::EmptyFrame)
    ));
    assert_eq!(
        NetError::from(FragmentError::Timeout),
        NetError::InvalidFragment(FragmentError::Timeout)
    );
}
