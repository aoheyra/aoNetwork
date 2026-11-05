//! `NetworkConfig`: validated, append-only configuration.
//!
//! Fields are private and every mutation goes through a `with_*` setter that
//! re-validates the whole struct, so an invalid configuration cannot exist
//! outside this module. Adding a field later is a non-breaking change: give
//! it a default in [`Default`] and add one setter.

use std::time::Duration;

use crate::error::ConfigError;

// Hard bounds. These catch unit errors and bound memory; they are not policy
// (policy is the app's choice within these limits).
const MIN_MAX_LINKS: u32 = 1;
const MAX_MAX_LINKS: u32 = 32;
const MIN_RSSI: i16 = -127;
const MAX_RSSI: i16 = 0;
const MIN_FRAME: usize = 64;
const MAX_FRAME: usize = 1024 * 1024;
const MAX_QUEUE_CAP: usize = 1024 * 1024;

// Defaults. Documented in the README and asserted verbatim by the tests.
const DEFAULT_MAX_LINKS: u32 = 5;
const DEFAULT_MIN_RSSI: i16 = -90;
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const DEFAULT_MAX_MISSED_PONGS: u8 = 3;
const DEFAULT_MAX_FRAME: usize = 16 * 1024;
const DEFAULT_QUEUE_CAP: usize = 128 * 1024;

/// Link-layer configuration with validated defaults.
///
/// | Setting | Default | Bound |
/// |---|---|---|
/// | `max_links` | 5 | 1..=32 |
/// | `min_rssi` | -90 dBm | -127..=0 dBm |
/// | `handshake_timeout` | 5 s | > 0 |
/// | `reassembly_timeout` | 10 s | > 0 |
/// | `keepalive_interval` | 15 s (x3 missed pongs) | > 0 |
/// | `max_missed_pongs` | 3 | >= 1 |
/// | `max_frame` | 16 KiB | 64 B..=1 MiB |
/// | `queue_cap` | 128 KiB per link | `max_frame`..=1 MiB |
///
/// Start from [`Default`] and chain the setters; each returns
/// `Result<Self, ConfigError>` and validates the **whole** configuration, so
/// a partially-applied or invalid config never escapes this module.
///
/// # Example
///
/// ```
/// use ao_network::{ConfigError, NetworkConfig};
/// use std::time::Duration;
///
/// let config = NetworkConfig::default()
///     .with_max_links(3)?
///     .with_handshake_timeout(Duration::from_secs(10))?;
/// assert_eq!(config.max_links(), 3);
/// assert_eq!(config.handshake_timeout(), Duration::from_secs(10));
///
/// // Invalid values are rejected with a typed error, not accepted silently.
/// assert!(matches!(
///     NetworkConfig::default().with_max_links(0),
///     Err(ConfigError::MaxLinksOutOfRange { value: 0, .. })
/// ));
/// # Ok::<(), ConfigError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkConfig {
    max_links: u32,
    min_rssi: i16,
    handshake_timeout: Duration,
    reassembly_timeout: Duration,
    keepalive_interval: Duration,
    max_missed_pongs: u8,
    max_frame: usize,
    queue_cap: usize,
}

impl NetworkConfig {
    /// Re-checks every field and the cross-field invariant.
    ///
    /// Called by each setter after mutation; private because the fields are
    /// private, so `Default` + setters is the only way to build a config.
    fn validate(&self) -> Result<(), ConfigError> {
        if !(MIN_MAX_LINKS..=MAX_MAX_LINKS).contains(&self.max_links) {
            return Err(ConfigError::MaxLinksOutOfRange {
                value: self.max_links,
                min: MIN_MAX_LINKS,
                max: MAX_MAX_LINKS,
            });
        }
        if !(MIN_RSSI..=MAX_RSSI).contains(&self.min_rssi) {
            return Err(ConfigError::MinRssiOutOfRange {
                value: self.min_rssi,
                min: MIN_RSSI,
                max: MAX_RSSI,
            });
        }
        if self.handshake_timeout.is_zero() {
            return Err(ConfigError::HandshakeTimeoutZero);
        }
        if self.reassembly_timeout.is_zero() {
            return Err(ConfigError::ReassemblyTimeoutZero);
        }
        if self.keepalive_interval.is_zero() {
            return Err(ConfigError::KeepaliveIntervalZero);
        }
        if self.max_missed_pongs == 0 {
            return Err(ConfigError::MaxMissedPongsZero);
        }
        if !(MIN_FRAME..=MAX_FRAME).contains(&self.max_frame) {
            return Err(ConfigError::MaxFrameOutOfRange {
                value: self.max_frame,
                min: MIN_FRAME,
                max: MAX_FRAME,
            });
        }
        // Cross-field invariant: the queue must hold at least one full frame,
        // otherwise a frame could never be queued and sends would wedge.
        if !(self.max_frame..=MAX_QUEUE_CAP).contains(&self.queue_cap) {
            return Err(ConfigError::QueueCapOutOfRange {
                value: self.queue_cap,
                min: self.max_frame,
                max: MAX_QUEUE_CAP,
            });
        }
        Ok(())
    }

    /// Sets how many links this node keeps at once (degree cap).
    ///
    /// Must be `1..=32`: zero links is meaningless, and no phone BLE stack
    /// sustains more than ~20 simultaneous connections, so anything larger
    /// is a unit error.
    pub fn with_max_links(mut self, max_links: u32) -> Result<Self, ConfigError> {
        self.max_links = max_links;
        self.validate()?;
        Ok(self)
    }

    /// Sets the minimum RSSI (dBm) at which a neighbour is accepted.
    ///
    /// Must be within the BLE RSSI domain `-127..=0`; a threshold above
    /// 0 dBm would reject every peer a stack can report, i.e. it is a unit
    /// error, not a policy choice.
    pub fn with_min_rssi(mut self, min_rssi: i16) -> Result<Self, ConfigError> {
        self.min_rssi = min_rssi;
        self.validate()?;
        Ok(self)
    }

    /// Sets how long the handshake may take before the link is dropped.
    ///
    /// Must be greater than zero. The bound is deliberately only structural:
    /// time is injected in this crate, so simulations legitimately use
    /// sub-millisecond values.
    pub fn with_handshake_timeout(
        mut self,
        handshake_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        self.handshake_timeout = handshake_timeout;
        self.validate()?;
        Ok(self)
    }

    /// Sets the total time allowed to reassemble one frame (default: 10 s).
    ///
    /// Must be nonzero. Progress does not extend the deadline.
    pub fn with_reassembly_timeout(mut self, timeout: Duration) -> Result<Self, ConfigError> {
        self.reassembly_timeout = timeout;
        self.validate()?;
        Ok(self)
    }

    /// Sets the keepalive ping interval (default: 15 s).
    ///
    /// Must be greater than zero, for the same reason as
    /// [`with_handshake_timeout`](Self::with_handshake_timeout).
    pub fn with_keepalive_interval(
        mut self,
        keepalive_interval: Duration,
    ) -> Result<Self, ConfigError> {
        self.keepalive_interval = keepalive_interval;
        self.validate()?;
        Ok(self)
    }

    /// Sets how many consecutive missed pongs close a link (default: 3).
    ///
    /// Must be at least 1: with 0 the link would close before any pong could
    /// be counted, making keepalive a busy-loop.
    pub fn with_max_missed_pongs(mut self, max_missed_pongs: u8) -> Result<Self, ConfigError> {
        self.max_missed_pongs = max_missed_pongs;
        self.validate()?;
        Ok(self)
    }

    /// Sets the largest frame this node will reassemble, in bytes.
    ///
    /// Must be `64 B..=1 MiB` and no larger than `queue_cap`. The bounds keep
    /// the reassembly buffer and the per-link queue bounded on a phone:
    /// worst-case memory is `max_links x queue_cap`, chosen by the app but
    /// capped here.
    pub fn with_max_frame(mut self, max_frame: usize) -> Result<Self, ConfigError> {
        self.max_frame = max_frame;
        self.validate()?;
        Ok(self)
    }

    /// Sets the per-link send-queue byte cap (default: 128 KiB).
    ///
    /// Must be at least `max_frame` (or the current `max_frame` is lowered
    /// first) and at most 1 MiB.
    pub fn with_queue_cap(mut self, queue_cap: usize) -> Result<Self, ConfigError> {
        self.queue_cap = queue_cap;
        self.validate()?;
        Ok(self)
    }

    /// The degree cap: how many links this node keeps at once.
    pub fn max_links(&self) -> u32 {
        self.max_links
    }

    /// The minimum RSSI (dBm) at which a neighbour is accepted.
    pub fn min_rssi(&self) -> i16 {
        self.min_rssi
    }

    /// How long the handshake may take before the link is dropped.
    pub fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }

    /// Total time allowed from the first chunk until frame completion.
    pub fn reassembly_timeout(&self) -> Duration {
        self.reassembly_timeout
    }

    /// The keepalive ping interval.
    pub fn keepalive_interval(&self) -> Duration {
        self.keepalive_interval
    }

    /// How many consecutive missed pongs close a link.
    pub fn max_missed_pongs(&self) -> u8 {
        self.max_missed_pongs
    }

    /// The largest frame this node will reassemble, in bytes.
    pub fn max_frame(&self) -> usize {
        self.max_frame
    }

    /// The per-link send-queue byte cap.
    pub fn queue_cap(&self) -> usize {
        self.queue_cap
    }
}

impl Default for NetworkConfig {
    /// The documented defaults; always valid, so constructing a config is
    /// infallible until you customise it.
    fn default() -> Self {
        Self {
            max_links: DEFAULT_MAX_LINKS,
            min_rssi: DEFAULT_MIN_RSSI,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            reassembly_timeout: DEFAULT_REASSEMBLY_TIMEOUT,
            keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
            max_missed_pongs: DEFAULT_MAX_MISSED_PONGS,
            max_frame: DEFAULT_MAX_FRAME,
            queue_cap: DEFAULT_QUEUE_CAP,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::NetError;

    #[test]
    fn every_default_matches_the_documented_table() {
        let cfg = NetworkConfig::default();
        assert_eq!(cfg.max_links(), 5);
        assert_eq!(cfg.min_rssi(), -90);
        assert_eq!(cfg.handshake_timeout(), Duration::from_secs(5));
        assert_eq!(cfg.keepalive_interval(), Duration::from_secs(15));
        assert_eq!(cfg.max_missed_pongs(), 3);
        assert_eq!(cfg.max_frame(), 16 * 1024);
        assert_eq!(cfg.queue_cap(), 128 * 1024);
    }

    #[test]
    fn default_config_validates() {
        assert_eq!(NetworkConfig::default().validate(), Ok(()));
    }

    #[test]
    fn max_links_bounds_are_enforced() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone().with_max_links(0).unwrap_err(),
            ConfigError::MaxLinksOutOfRange {
                value: 0,
                min: 1,
                max: 32
            }
        );
        assert_eq!(
            base.clone().with_max_links(33).unwrap_err(),
            ConfigError::MaxLinksOutOfRange {
                value: 33,
                min: 1,
                max: 32
            }
        );
        assert_eq!(base.clone().with_max_links(32).unwrap().max_links(), 32);
        assert_eq!(base.with_max_links(1).unwrap().max_links(), 1);
    }

    #[test]
    fn min_rssi_bounds_are_enforced() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone().with_min_rssi(-128).unwrap_err(),
            ConfigError::MinRssiOutOfRange {
                value: -128,
                min: -127,
                max: 0
            }
        );
        assert_eq!(
            base.clone().with_min_rssi(1).unwrap_err(),
            ConfigError::MinRssiOutOfRange {
                value: 1,
                min: -127,
                max: 0
            }
        );
        assert_eq!(base.clone().with_min_rssi(-127).unwrap().min_rssi(), -127);
        assert_eq!(base.with_min_rssi(0).unwrap().min_rssi(), 0);
    }

    #[test]
    fn zero_durations_are_rejected() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone()
                .with_handshake_timeout(Duration::ZERO)
                .unwrap_err(),
            ConfigError::HandshakeTimeoutZero
        );
        assert_eq!(
            base.clone()
                .with_keepalive_interval(Duration::ZERO)
                .unwrap_err(),
            ConfigError::KeepaliveIntervalZero
        );
        // Any non-zero duration is accepted: time is injected, so sim tests
        // may legitimately use sub-millisecond values.
        assert!(
            base.clone()
                .with_handshake_timeout(Duration::from_nanos(1))
                .is_ok()
        );
        assert!(
            base.with_keepalive_interval(Duration::from_millis(1))
                .is_ok()
        );
    }

    #[test]
    fn zero_missed_pongs_is_rejected() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone().with_max_missed_pongs(0).unwrap_err(),
            ConfigError::MaxMissedPongsZero
        );
        // No artificial upper bound: a high count is a slow timeout, not an
        // invalid state.
        assert_eq!(
            base.clone()
                .with_max_missed_pongs(1)
                .unwrap()
                .max_missed_pongs(),
            1
        );
        assert_eq!(
            base.with_max_missed_pongs(u8::MAX)
                .unwrap()
                .max_missed_pongs(),
            u8::MAX
        );
    }

    #[test]
    fn max_frame_bounds_are_enforced() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone().with_max_frame(63).unwrap_err(),
            ConfigError::MaxFrameOutOfRange {
                value: 63,
                min: 64,
                max: 1024 * 1024
            }
        );
        assert_eq!(
            base.clone().with_max_frame(1024 * 1024 + 1).unwrap_err(),
            ConfigError::MaxFrameOutOfRange {
                value: 1024 * 1024 + 1,
                min: 64,
                max: 1024 * 1024
            }
        );
        assert_eq!(base.clone().with_max_frame(64).unwrap().max_frame(), 64);
        // The 1 MiB ceiling is reachable only after the queue cap can hold
        // that frame (queue_cap >= max_frame).
        assert_eq!(
            base.with_queue_cap(1024 * 1024)
                .unwrap()
                .with_max_frame(1024 * 1024)
                .unwrap()
                .max_frame(),
            1024 * 1024
        );
    }

    #[test]
    fn queue_cap_must_hold_at_least_one_frame() {
        let base = NetworkConfig::default(); // max_frame = 16 KiB
        assert_eq!(
            base.clone().with_queue_cap(16 * 1024 - 1).unwrap_err(),
            ConfigError::QueueCapOutOfRange {
                value: 16 * 1024 - 1,
                min: 16 * 1024,
                max: 1024 * 1024
            }
        );
        assert_eq!(
            base.clone().with_queue_cap(16 * 1024).unwrap().queue_cap(),
            16 * 1024
        );

        // The invariant holds from the other direction too: raising max_frame
        // beyond the existing queue cap is rejected with the same error.
        assert_eq!(
            base.clone().with_max_frame(128 * 1024 + 1).unwrap_err(),
            ConfigError::QueueCapOutOfRange {
                value: 128 * 1024,
                min: 128 * 1024 + 1,
                max: 1024 * 1024
            }
        );
        assert_eq!(
            base.with_max_frame(128 * 1024).unwrap().max_frame(),
            128 * 1024
        );
    }

    #[test]
    fn queue_cap_upper_bound_is_enforced() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone().with_queue_cap(1024 * 1024 + 1).unwrap_err(),
            ConfigError::QueueCapOutOfRange {
                value: 1024 * 1024 + 1,
                min: 16 * 1024,
                max: 1024 * 1024
            }
        );
        assert_eq!(
            base.with_queue_cap(1024 * 1024).unwrap().queue_cap(),
            1024 * 1024
        );
    }

    #[test]
    fn setters_compose_into_one_valid_config() {
        let cfg = NetworkConfig::default()
            .with_max_links(3)
            .unwrap()
            .with_min_rssi(-80)
            .unwrap()
            .with_handshake_timeout(Duration::from_secs(10))
            .unwrap()
            .with_keepalive_interval(Duration::from_secs(30))
            .unwrap()
            .with_max_missed_pongs(5)
            .unwrap()
            .with_max_frame(4096)
            .unwrap()
            .with_queue_cap(64 * 1024)
            .unwrap();
        assert_eq!(cfg.max_links(), 3);
        assert_eq!(cfg.min_rssi(), -80);
        assert_eq!(cfg.handshake_timeout(), Duration::from_secs(10));
        assert_eq!(cfg.keepalive_interval(), Duration::from_secs(30));
        assert_eq!(cfg.max_missed_pongs(), 5);
        assert_eq!(cfg.max_frame(), 4096);
        assert_eq!(cfg.queue_cap(), 64 * 1024);
        assert_eq!(cfg.validate(), Ok(()));
    }

    #[test]
    fn a_failed_setter_reports_net_error_via_from() {
        // The path NET-12 will use: config error -> NetError.
        let err: NetError = NetworkConfig::default()
            .with_max_links(0)
            .map_err(NetError::from)
            .unwrap_err();
        assert!(matches!(
            err,
            NetError::InvalidConfig(ConfigError::MaxLinksOutOfRange { value: 0, .. })
        ));
    }

    #[test]
    fn configs_compare_by_value() {
        assert_eq!(NetworkConfig::default(), NetworkConfig::default());
        assert_ne!(
            NetworkConfig::default(),
            NetworkConfig::default().with_max_links(3).unwrap()
        );
    }
}
