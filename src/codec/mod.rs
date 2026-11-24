//! Versioned wire codecs. Radio framing is handled by the transport adapter.

mod beacon;
pub(crate) mod fragment;
mod frame;
mod varint;

pub use beacon::{BEACON_LEN, BEACON_VERSION, Beacon, BeaconFlags};
pub use fragment::{Fragmenter, MAX_MTU, MIN_MTU, Reassembler};
pub(crate) use frame::data_frame_len;
pub use frame::{AppHello, CAP_CREDIT, CAP_L2CAP, FRAME_VERSION, Frame, Hello, MAX_APP_HELLO};
