//! Versioned wire codecs. Radio framing is handled by the transport adapter.

mod beacon;

pub use beacon::{BEACON_LEN, BEACON_VERSION, Beacon, BeaconFlags};
