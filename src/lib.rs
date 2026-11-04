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
//! The full design lives in the repository `README.md`; the layer diagram,
//! channel table and defaults are documented there.

#![deny(missing_docs)]
