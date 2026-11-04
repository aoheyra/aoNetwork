//! Seeded properties exercise the public beacon API, independent of internals.

use ao_network::{BEACON_LEN, BEACON_VERSION, Beacon, BeaconFlags};
use proptest::prelude::*;
use proptest::test_runner::{Config, MapFailurePersistence, RngSeed};

fn config() -> Config {
    let mut config = Config::with_cases(256);
    config.rng_seed = RngSeed::Fixed(0xA006_0001);
    // File persistence calls getcwd, which Miri's isolated runner forbids.
    // Keep failures in memory on every platform; the fixed seed replays each
    // run. Promote any reported minimal failing input to a regression test.
    config.failure_persistence = Some(Box::new(MapFailurePersistence::default()));
    config
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn supported_values_round_trip(
        accepting in any::<bool>(),
        relay_willing in any::<bool>(),
        low_battery in any::<bool>(),
        degree in any::<u8>(),
        max_links in any::<u8>(),
        hint in any::<[u8; 8]>(),
        digest in any::<[u8; 4]>(),
    ) {
        let beacon = Beacon {
            flags: BeaconFlags { accepting, relay_willing, low_battery },
            degree, max_links, hint, digest,
        };
        prop_assert_eq!(Beacon::decode(&beacon.encode()), Ok(beacon));
    }

    #[test]
    fn arbitrary_buffers_are_panic_free_and_accepted_bytes_are_canonical(
        input in prop::collection::vec(any::<u8>(), 0..128),
    ) {
        // Proptest fails this property if decode panics. A typed error is the
        // normal outcome for arbitrary wire input, not an ignored failure.
        if let Ok(beacon) = Beacon::decode(&input) {
            prop_assert_eq!(input.len(), BEACON_LEN);
            prop_assert_eq!(input[0], BEACON_VERSION);
            let encoded = beacon.encode();
            prop_assert_eq!(encoded.as_slice(), input.as_slice());
        }
    }

    #[test]
    fn arbitrary_version_one_headers_preserve_every_accepted_byte(
        mut input in any::<[u8; BEACON_LEN]>(),
    ) {
        input[0] = BEACON_VERSION;
        if let Ok(beacon) = Beacon::decode(&input) {
            prop_assert_eq!(beacon.encode(), input);
        }
    }
}
