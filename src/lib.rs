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
mod types;

pub use codec::{Frame, FrameCodecError, HelloFrame};
pub use config::NetworkConfig;
pub use error::{ConfigError, DisconnectReason, NetError, SendError};
pub use types::{Channel, LinkId, LinkRole, LocalTag, PeerKey};
