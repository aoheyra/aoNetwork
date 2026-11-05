use ao_network::{Fragmenter, MAX_MTU, MIN_MTU, NetworkConfig, Reassembler, Reassembly};
use proptest::{
    prelude::*,
    test_runner::{Config, MapFailurePersistence, RngSeed},
};
use std::time::Duration;

fn config() -> Config {
    let mut config = Config::with_cases(32);
    config.rng_seed = RngSeed::Fixed(0xA005_0001);
    config.failure_persistence = Some(Box::new(MapFailurePersistence::default()));
    config
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn arbitrary_frames_round_trip_at_every_mtu(frame in proptest::collection::vec(any::<u8>(), 1..256)) {
        let config = NetworkConfig::default();
        for mtu in MIN_MTU..=MAX_MTU {
            let mut receiver = Reassembler::new(mtu, &config).unwrap();
            let mut complete = None;
            let mut out = [0; MAX_MTU as usize - 3];
            for chunk in Fragmenter::new(&frame, mtu, &config).unwrap() {
                let len = chunk.encode_into(&mut out).unwrap();
                prop_assert!(len <= mtu as usize - 3);
                match receiver.push(Duration::ZERO, &out[..len]).unwrap() {
                    Reassembly::Pending => {},
                    Reassembly::Complete(bytes) => { prop_assert!(complete.replace(bytes).is_none()); }
                }
            }
            prop_assert_eq!(complete.as_deref(), Some(frame.as_slice()));
            prop_assert_eq!(receiver.buffered_len(), 0);
        }
    }

    #[test]
    fn arbitrary_chunk_streams_never_panic_and_errors_clear_the_buffer(
        chunks in proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..530), 0..32),
        mtu in MIN_MTU..=MAX_MTU,
    ) {
        let config = NetworkConfig::default().with_max_frame(64).unwrap();
        let mut receiver = Reassembler::new(mtu, &config).unwrap();
        for (now, chunk) in chunks.iter().enumerate() {
            match receiver.push(Duration::from_millis(now as u64), chunk) {
                Err(_) => {
                    prop_assert_eq!(receiver.buffered_len(), 0);
                    prop_assert!(!receiver.is_busy());
                },
                Ok(Reassembly::Complete(bytes)) => {
                    prop_assert!(bytes.len() <= 64);
                    prop_assert!(!receiver.is_busy());
                },
                Ok(Reassembly::Pending) => prop_assert!(receiver.buffered_len() <= 64),
            }
        }
        receiver.tick(Duration::from_secs(100)).unwrap_or_else(|error| assert_eq!(error, ao_network::FragmentError::Timeout));
        prop_assert_eq!(receiver.buffered_len(), 0);
    }
}
