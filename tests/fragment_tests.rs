//! Chunk layout fixtures and every reassembly error path. Round-trips at
//! every MTU live in `fragment_properties.rs`.

use std::time::Duration;

use ao_network::{FragmentError, Fragmenter, MAX_MTU, MIN_MTU, NetworkConfig, Reassembler};

const T0: Duration = Duration::ZERO;

fn reassembler() -> Reassembler {
    Reassembler::new(&NetworkConfig::default())
}

/// A lone START|END chunk completes only if no partial frame is held, so
/// this proves the previous error dropped it.
fn assert_idle(reassembler: &mut Reassembler, now: Duration) {
    assert_eq!(reassembler.push(now, &[0xC0, 7]), Ok(Some(vec![7])));
}

#[test]
fn chunk_layout_matches_the_fixture() {
    // 40 bytes at the minimum MTU: 19 + 19 + 2 payload bytes.
    let frame: Vec<u8> = (0..40).collect();
    let chunks: Vec<Vec<u8>> = Fragmenter::new(&frame, MIN_MTU).unwrap().collect();
    let headers: Vec<u8> = chunks.iter().map(|c| c[0]).collect();
    assert_eq!(headers, [0x80, 0x01, 0x42]);
    assert_eq!(chunks[0][1..], frame[..19]);
    assert_eq!(chunks[2][1..], frame[38..]);

    let single: Vec<Vec<u8>> = Fragmenter::new(&[9], MIN_MTU).unwrap().collect();
    assert_eq!(single, [vec![0xC0, 9]]);
}

#[test]
fn mtu_outside_the_ble_range_is_rejected() {
    for mtu in [MIN_MTU - 1, MAX_MTU + 1] {
        assert_eq!(
            Fragmenter::new(&[1], mtu).err(),
            Some(FragmentError::InvalidMtu { mtu })
        );
    }
}

#[test]
fn reserved_header_bits_are_ignored() {
    assert_eq!(reassembler().push(T0, &[0xF8, 1]), Ok(Some(vec![1])));
}

#[test]
fn empty_chunk_drops_the_frame() {
    let mut r = reassembler();
    assert_eq!(r.push(T0, &[]), Err(FragmentError::EmptyChunk));
    assert_eq!(r.push(T0, &[0x80, 1]), Ok(None));
    assert_eq!(r.push(T0, &[0x01]), Err(FragmentError::EmptyChunk));
    assert_idle(&mut r, T0);
}

#[test]
fn continuation_without_start_is_rejected() {
    let mut r = reassembler();
    assert_eq!(r.push(T0, &[0x41, 1]), Err(FragmentError::MissingStart));
    assert_idle(&mut r, T0);
}

#[test]
fn sequence_gap_drops_the_frame() {
    let mut r = reassembler();
    assert_eq!(
        r.push(T0, &[0xC3, 1]),
        Err(FragmentError::SeqGap {
            expected: 0,
            actual: 3
        })
    );
    assert_eq!(r.push(T0, &[0x80, 1]), Ok(None));
    assert_eq!(
        r.push(T0, &[0x02, 1]),
        Err(FragmentError::SeqGap {
            expected: 1,
            actual: 2
        })
    );
    assert_idle(&mut r, T0);
}

#[test]
fn start_while_busy_drops_the_frame() {
    let mut r = reassembler();
    assert_eq!(r.push(T0, &[0x80, 1]), Ok(None));
    assert_eq!(r.push(T0, &[0x80, 2]), Err(FragmentError::StartWhileBusy));
    assert_idle(&mut r, T0);
}

#[test]
fn frame_over_max_frame_is_dropped() {
    let config = NetworkConfig::default().with_max_frame(64).unwrap();
    let mut r = Reassembler::new(&config);

    let mut single = vec![0xC0];
    single.extend_from_slice(&[0; 65]);
    assert_eq!(
        r.push(T0, &single),
        Err(FragmentError::TooLarge { len: 65, max: 64 })
    );

    let mut first = vec![0x80];
    first.extend_from_slice(&[0; 40]);
    let mut second = vec![0x01];
    second.extend_from_slice(&[0; 40]);
    assert_eq!(r.push(T0, &first), Ok(None));
    assert_eq!(
        r.push(T0, &second),
        Err(FragmentError::TooLarge { len: 80, max: 64 })
    );
    assert_idle(&mut r, T0);
}

#[test]
fn timeout_is_measured_from_start_and_drops_the_frame() {
    let timeout = Duration::from_secs(1);
    let config = NetworkConfig::default()
        .with_reassembly_timeout(timeout)
        .unwrap();
    let mut r = Reassembler::new(&config);

    // On push: progress does not extend the deadline.
    assert_eq!(r.push(T0, &[0x80, 1]), Ok(None));
    assert_eq!(r.push(timeout / 2, &[0x01, 2]), Ok(None));
    assert_eq!(r.push(timeout, &[0x42, 3]), Err(FragmentError::Timeout));
    assert_idle(&mut r, timeout);

    // On the timer, with no chunks arriving.
    assert_eq!(r.push(timeout, &[0x80, 1]), Ok(None));
    assert_eq!(
        r.poll_timeout(timeout * 2 - Duration::from_nanos(1)),
        Ok(())
    );
    assert_eq!(r.poll_timeout(timeout * 2), Err(FragmentError::Timeout));
    assert_eq!(r.poll_timeout(timeout * 3), Ok(()));
    assert_idle(&mut r, timeout * 3);
}

/// NET-26: a CoC packet has no ATT header, so a chunk fills its MTU.
/// The chunk header and sequence stay on the wire unchanged.
#[test]
fn coc_fragmentation_uses_the_whole_packet() {
    let frame = vec![0x5A; 492];
    let gatt: Vec<Vec<u8>> = Fragmenter::new(&frame, 247).unwrap().collect();
    let coc: Vec<Vec<u8>> = Fragmenter::with_overhead(&frame, 247, 1).unwrap().collect();
    assert_eq!(gatt[0].len(), 244);
    assert_eq!(coc[0].len(), 247);
    assert!(coc.len() < gatt.len());

    let config = NetworkConfig::default();
    for chunks in [gatt, coc] {
        let mut reassembler = Reassembler::new(&config);
        let mut out = None;
        for chunk in &chunks {
            out = reassembler.push(T0, chunk).unwrap();
        }
        assert_eq!(out, Some(frame.clone()));
    }
}

#[test]
fn deadline_runs_from_the_first_chunk_of_a_partial_frame() {
    let config = NetworkConfig::default();
    let mut r = Reassembler::new(&config);
    assert_eq!(r.deadline(), None);
    let start = Duration::from_secs(1);
    // START without END: the frame stays partial.
    assert_eq!(r.push(start, &[0x80, 1]), Ok(None));
    assert_eq!(r.deadline(), Some(start + config.reassembly_timeout()));
    assert_eq!(r.push(start * 2, &[0x41, 2]), Ok(Some(vec![1, 2])));
    assert_eq!(r.deadline(), None);
}

/// A frame of exactly `max_frame` bytes comes back holding no more memory
/// than `max_frame`: the buffer doubles as it fills, but stops at the cap.
#[test]
fn a_full_size_frame_holds_no_more_than_max_frame() {
    let config = NetworkConfig::default();
    let max = config.max_frame();
    let frame = vec![0x5A; max];
    let mut reassembler = Reassembler::new(&config);
    let mut out = None;
    for chunk in Fragmenter::new(&frame, 247).unwrap() {
        out = reassembler.push(T0, &chunk).unwrap();
    }
    let out = out.expect("the last chunk completes the frame");
    assert_eq!(out, frame);
    assert!(out.capacity() <= max, "{} > {max}", out.capacity());
}
