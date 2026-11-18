//! The **link layer** of the aoheyra mesh.
//!
//! `ao_network` turns a raw Bluetooth LE radio (or an in-memory simulator)
//! into reliable, prioritised, abuse-resistant links between neighbouring
//! phones. It is sans-IO and deterministic: time and randomness are injected,
//! never read from the clock or a global RNG.
//!
//! # Where this crate sits
//!
//! ```text
//!                 aoheyra  (app: Rust core + Kotlin UI)
//!                    │  depends on all three
//!         ┌──────────┼───────────┐
//!         ▼          ▼           ▼
//!     aoMesh ───▶ aoNetwork    aoIdents
//!         │                       ▲
//!         └───────────────────────┘
//!
//! aoNetwork and aoIdents never depend on each other.
//! ```
//!
//! It knows about **radio peers and frames** only. It does not know what a
//! message, a contact, a mesh packet or a cryptographic identity is; those
//! belong to `aoMesh` and `aoIdents`.
//!
//! # Features
//!
//! | Feature | Default | Meaning |
//! |---|---|---|
//! | `sim` | yes | in-memory `SimWorld` transport (NET-09) |
//! | `native` | no | `NativeTransport` adapter for the Kotlin BLE shim (NET-14) |
//!
//! # Example
//!
//! ```
//! use ao_network::NetworkConfig;
//!
//! // Configurations are validated at construction: every `with_*` setter
//! // returns a typed error instead of accepting an invalid value.
//! let config = NetworkConfig::default()
//!     .with_max_links(3)?
//!     .with_min_rssi(-80)?;
//! assert_eq!(config.max_links(), 3);
//! assert_eq!(config.min_rssi(), -80);
//! # Ok::<(), ao_network::ConfigError>(())
//! ```
//!
//! The full design lives in the repository `README.md`; the layer diagram,
//! channel table and defaults are documented there.

#![deny(missing_docs)]

mod codec;
mod config;
mod error;
mod link;
mod metrics;
mod network;
mod queue;
#[cfg(feature = "sim")]
mod sim;
mod topology;
mod transport;
mod types;

pub use codec::{
    AppHello, BEACON_LEN, BEACON_VERSION, Beacon, BeaconFlags, FRAME_VERSION, Fragmenter, Frame,
    Hello, MAX_APP_HELLO, MAX_MTU, MIN_MTU, Reassembler,
};
pub use config::NetworkConfig;
pub use error::{
    BeaconError, ConfigError, ConnectError, DisconnectReason, FragmentError, FrameError,
    HandshakeFailure, NetError, SendError,
};
pub use link::{Link, LinkAction, LinkEnd, LinkState};
pub use metrics::LinkMetrics;
pub use network::{LinkInfo, Network, NetworkEvent};
pub use queue::{SendOpts, SendQueue};
#[cfg(feature = "sim")]
pub use sim::{NodeId, SIM_RSSI, SimConfig, SimError, SimNode, SimTransport, SimWorld};
pub use transport::{
    AdvProfile, PowerMode, RadioState, ScanProfile, Transport, TransportCaps, TransportEvent,
};
pub use types::{Channel, LinkId, LinkRole, LocalTag, PeerKey};
