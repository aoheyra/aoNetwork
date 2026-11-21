//! `NetworkConfig`: validated, append-only configuration.
//!
//! Fields are private and every mutation goes through a `with_*` setter that
//! re-validates the whole struct, so an invalid configuration cannot exist
//! outside this module. Adding a field later is a non-breaking change: give
//! it a default in [`Default`] and add one setter.

use std::time::Duration;

use crate::error::ConfigError;
use crate::{Channel, MAX_MTU, RateLimit, SendQueue};

// Hard bounds. These catch unit errors and bound memory; they are not policy
// (policy is the app's choice within these limits).
const MIN_MAX_LINKS: u32 = 1;
const MAX_MAX_LINKS: u32 = 32;
const MIN_MAX_PEERS: u32 = 1;
const MAX_MAX_PEERS: u32 = 1024;
const MIN_RSSI: i16 = -127;
const MAX_RSSI: i16 = 0;
const MIN_FRAME: usize = 64;
const MAX_FRAME: usize = 1024 * 1024;
const MAX_QUEUE_CAP: usize = 1024 * 1024;
// One smallest frame and the slot it is queued in.
const MIN_CHANNEL_CAP: usize = MIN_FRAME + SendQueue::ENTRY_COST;
// A peer's grant fits a credit's `u32`, and bounds what a link holds unread.
const MAX_RECEIVE_WINDOW: usize = 1024 * 1024;
// Both parts of a rate limit; the bucket arithmetic stays far from overflow.
const MAX_RATE: u32 = 16 * 1024 * 1024;
const MIN_FRAME_BURST: u32 = 1;
// A full chunk at the largest MTU always fits in the byte bucket.
const MIN_BYTE_BURST: u32 = MAX_MTU as u32;

// Defaults. Documented in the README and asserted verbatim by the tests.
const DEFAULT_MAX_LINKS: u32 = 5;
const DEFAULT_MAX_PEERS: u32 = 64;
const DEFAULT_MIN_RSSI: i16 = -90;
const DEFAULT_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_REASSEMBLY_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
const DEFAULT_MAX_MISSED_PONGS: u8 = 3;
const DEFAULT_MAX_FRAME: usize = 16 * 1024;
const DEFAULT_QUEUE_CAP: usize = 128 * 1024;
// Control, Message, Telemetry, Bulk, in default-size frames: one, three,
// one, three; together they fill `DEFAULT_QUEUE_CAP`. Each queued frame
// also pays `SendQueue::ENTRY_COST`, so a channel sized for n of them fits
// n - 1 at full size.
const DEFAULT_CHANNEL_CAPS: [usize; 4] = [16 * 1024, 48 * 1024, 16 * 1024, 48 * 1024];
const DEFAULT_FRAME_LIMIT: RateLimit = RateLimit {
    per_second: 50,
    burst: 200,
};
const DEFAULT_BYTE_LIMIT: RateLimit = RateLimit {
    per_second: 64 * 1024,
    burst: 256 * 1024,
};
const DEFAULT_MAX_STRIKES: u8 = 3;
const DEFAULT_BLACKLIST_DURATION: Duration = Duration::from_secs(10 * 60);
const DEFAULT_DISCOVERY_INTERVAL: Duration = Duration::from_secs(10);
// What a peer may have in flight to us per channel: as much as our own
// send queue holds on that channel.
const DEFAULT_RECEIVE_WINDOWS: [usize; 4] = DEFAULT_CHANNEL_CAPS;
const DEFAULT_FLOW_CONTROL: bool = true;

/// Link-layer configuration with validated defaults.
///
/// | Setting | Default | Bound |
/// |---|---|---|
/// | `max_links` | 5 | 1..=32 |
/// | `max_peers` | 64 discovered peers | 1..=1024 |
/// | `min_rssi` | -90 dBm | -127..=0 dBm |
/// | `handshake_timeout` | 5 s | > 0 |
/// | `reassembly_timeout` | 10 s | > 0 |
/// | `keepalive_interval` | 15 s (x3 missed pongs) | > 0 |
/// | `max_missed_pongs` | 3 | >= 1 |
/// | `max_frame` | 16 KiB | 64 B..=1 MiB |
/// | `queue_cap` | 128 KiB per link | `max_frame`..=1 MiB |
/// | `channel_cap` | Control 16, Message 48, Telemetry 16, Bulk 48 KiB | each >= 128 B, sum <= `queue_cap` |
/// | `frame_limit` | 50 frames/s, burst 200 per link | rate 1..=16 Mi, burst 1..=16 Mi |
/// | `byte_limit` | 64 KiB/s, burst 256 KiB per link | rate 1..=16 Mi, burst 517 B..=16 MiB |
/// | `max_strikes` | 3 | >= 1 |
/// | `blacklist_duration` | 10 min | > 0 |
/// | `discovery_interval` | 10 s | > 0 |
/// | `flow_control` | on | - |
/// | `receive_window` | Control 16, Message 48, Telemetry 16, Bulk 48 KiB | each `max_frame`..=1 MiB |
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
    max_peers: u32,
    min_rssi: i16,
    handshake_timeout: Duration,
    reassembly_timeout: Duration,
    keepalive_interval: Duration,
    max_missed_pongs: u8,
    max_frame: usize,
    queue_cap: usize,
    channel_caps: [usize; 4],
    frame_limit: RateLimit,
    byte_limit: RateLimit,
    max_strikes: u8,
    blacklist_duration: Duration,
    discovery_interval: Duration,
    flow_control: bool,
    receive_windows: [usize; 4],
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
        if !(MIN_MAX_PEERS..=MAX_MAX_PEERS).contains(&self.max_peers) {
            return Err(ConfigError::MaxPeersOutOfRange {
                value: self.max_peers,
                min: MIN_MAX_PEERS,
                max: MAX_MAX_PEERS,
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
        for channel in Channel::ALL {
            let value = self.channel_caps[channel.index()];
            if value < MIN_CHANNEL_CAP {
                return Err(ConfigError::ChannelCapTooSmall {
                    channel,
                    value,
                    min: MIN_CHANNEL_CAP,
                });
            }
        }
        // Saturating: a setter may pass any usize, and a wrapped sum could
        // pass the check.
        let total = self
            .channel_caps
            .iter()
            .fold(0usize, |sum, &cap| sum.saturating_add(cap));
        if total > self.queue_cap {
            return Err(ConfigError::ChannelCapsOverQueueCap {
                total,
                queue_cap: self.queue_cap,
            });
        }
        if !rate_ok(self.frame_limit, MIN_FRAME_BURST) {
            return Err(ConfigError::FrameLimitOutOfRange {
                limit: self.frame_limit,
                min_burst: MIN_FRAME_BURST,
                max: MAX_RATE,
            });
        }
        if !rate_ok(self.byte_limit, MIN_BYTE_BURST) {
            return Err(ConfigError::ByteLimitOutOfRange {
                limit: self.byte_limit,
                min_burst: MIN_BYTE_BURST,
                max: MAX_RATE,
            });
        }
        if self.max_strikes == 0 {
            return Err(ConfigError::MaxStrikesZero);
        }
        if self.blacklist_duration.is_zero() {
            return Err(ConfigError::BlacklistDurationZero);
        }
        if self.discovery_interval.is_zero() {
            return Err(ConfigError::DiscoveryIntervalZero);
        }
        // A window below `max_frame` could never admit the largest frame
        // the peer is allowed to send, and the channel would stall.
        for channel in Channel::ALL {
            let value = self.receive_windows[channel.index()];
            if !(self.max_frame..=MAX_RECEIVE_WINDOW).contains(&value) {
                return Err(ConfigError::ReceiveWindowOutOfRange {
                    channel,
                    value,
                    min: self.max_frame,
                    max: MAX_RECEIVE_WINDOW,
                });
            }
        }
        Ok(())
    }

    /// Sets how many links this node keeps at once (degree cap).
    ///
    /// Must be `1..=32`: zero links is meaningless, and no phone BLE stack
    /// sustains more than ~20 simultaneous connections, so anything larger
    /// is a unit error.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MaxLinksOutOfRange`] outside the bounds.
    pub fn with_max_links(mut self, max_links: u32) -> Result<Self, ConfigError> {
        self.max_links = max_links;
        self.validate()?;
        Ok(self)
    }

    /// Sets how many discovered peers the peer table tracks at once.
    ///
    /// Must be `1..=1024`. Every beacon sighting is recorded, and a full
    /// table evicts its stalest entry, so this bounds memory against an
    /// advertiser flood; the default of 64 leaves room to choose between
    /// several neighbours without holding a crowd.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MaxPeersOutOfRange`] outside the bounds.
    pub fn with_max_peers(mut self, max_peers: u32) -> Result<Self, ConfigError> {
        self.max_peers = max_peers;
        self.validate()?;
        Ok(self)
    }

    /// Sets the minimum RSSI (dBm) at which a neighbour is accepted.
    ///
    /// Must be within the BLE RSSI domain `-127..=0`; a threshold above
    /// 0 dBm would reject every peer a stack can report, i.e. it is a unit
    /// error, not a policy choice.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MinRssiOutOfRange`] outside the bounds.
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
    ///
    /// # Errors
    ///
    /// [`ConfigError::HandshakeTimeoutZero`] for zero.
    pub fn with_handshake_timeout(
        mut self,
        handshake_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        self.handshake_timeout = handshake_timeout;
        self.validate()?;
        Ok(self)
    }

    /// Sets how long one frame may take to reassemble (default: 10 s).
    ///
    /// Measured from the frame's first chunk; later chunks do not extend it.
    /// Must be greater than zero, for the same reason as
    /// [`with_handshake_timeout`](Self::with_handshake_timeout).
    ///
    /// # Errors
    ///
    /// [`ConfigError::ReassemblyTimeoutZero`] for zero.
    pub fn with_reassembly_timeout(
        mut self,
        reassembly_timeout: Duration,
    ) -> Result<Self, ConfigError> {
        self.reassembly_timeout = reassembly_timeout;
        self.validate()?;
        Ok(self)
    }

    /// Sets the keepalive ping interval (default: 15 s).
    ///
    /// Must be greater than zero, for the same reason as
    /// [`with_handshake_timeout`](Self::with_handshake_timeout).
    ///
    /// # Errors
    ///
    /// [`ConfigError::KeepaliveIntervalZero`] for zero.
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
    ///
    /// # Errors
    ///
    /// [`ConfigError::MaxMissedPongsZero`] for zero.
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
    ///
    /// # Errors
    ///
    /// [`ConfigError::MaxFrameOutOfRange`] outside the bounds,
    /// [`ConfigError::QueueCapOutOfRange`] above `queue_cap`, or
    /// [`ConfigError::ReceiveWindowOutOfRange`] above a receive window.
    pub fn with_max_frame(mut self, max_frame: usize) -> Result<Self, ConfigError> {
        self.max_frame = max_frame;
        self.validate()?;
        Ok(self)
    }

    /// Sets the per-link send-queue byte cap (default: 128 KiB).
    ///
    /// Must be at least `max_frame` (or the current `max_frame` is lowered
    /// first), at most 1 MiB, and no less than the sum of the channel caps
    /// (lower those first).
    ///
    /// # Errors
    ///
    /// [`ConfigError::QueueCapOutOfRange`] outside the bounds, or
    /// [`ConfigError::ChannelCapsOverQueueCap`] below the channel caps' sum.
    pub fn with_queue_cap(mut self, queue_cap: usize) -> Result<Self, ConfigError> {
        self.queue_cap = queue_cap;
        self.validate()?;
        Ok(self)
    }

    /// Sets the send-queue byte cap of one channel on each link.
    ///
    /// Must be at least 128 B, one smallest frame and its slot, and the
    /// four caps together must fit in `queue_cap`, so a flood on one
    /// channel never takes another's room. Each payload counts as its
    /// encoded `Data` frame plus [`SendQueue::ENTRY_COST`].
    ///
    /// # Errors
    ///
    /// [`ConfigError::ChannelCapTooSmall`] below 128 B, or
    /// [`ConfigError::ChannelCapsOverQueueCap`] if the caps no longer fit.
    pub fn with_channel_cap(mut self, channel: Channel, cap: usize) -> Result<Self, ConfigError> {
        self.channel_caps[channel.index()] = cap;
        self.validate()?;
        Ok(self)
    }

    /// Sets how many frames a peer may send on one link (default: 50 per
    /// second, burst 200).
    ///
    /// Both parts must be in `1..=16 Mi`. A peer that sends faster is
    /// disconnected with
    /// [`DisconnectReason::AbuseDetected`](crate::DisconnectReason::AbuseDetected)
    /// and blacklisted. Every frame counts, `Ping` included.
    ///
    /// # Errors
    ///
    /// [`ConfigError::FrameLimitOutOfRange`] outside the bounds.
    pub fn with_frame_limit(mut self, limit: RateLimit) -> Result<Self, ConfigError> {
        self.frame_limit = limit;
        self.validate()?;
        Ok(self)
    }

    /// Sets how many bytes a peer may send on one link (default: 64 KiB
    /// per second, burst 256 KiB), counted as received chunks.
    ///
    /// The rate must be in `1..=16 Mi` and the burst in `517 B..=16 MiB`,
    /// so one chunk at the largest MTU always fits. A peer that sends
    /// faster is treated as for [`with_frame_limit`](Self::with_frame_limit).
    ///
    /// # Errors
    ///
    /// [`ConfigError::ByteLimitOutOfRange`] outside the bounds.
    pub fn with_byte_limit(mut self, limit: RateLimit) -> Result<Self, ConfigError> {
        self.byte_limit = limit;
        self.validate()?;
        Ok(self)
    }

    /// Sets how many strikes blacklist a peer (default: 3).
    ///
    /// A strike is a link closed for a malformed frame, a broken chunk
    /// stream or an oversize `Hello`. Must be at least 1.
    ///
    /// # Errors
    ///
    /// [`ConfigError::MaxStrikesZero`] for zero.
    pub fn with_max_strikes(mut self, max_strikes: u8) -> Result<Self, ConfigError> {
        self.max_strikes = max_strikes;
        self.validate()?;
        Ok(self)
    }

    /// Sets how long a blacklisted peer stays blocked, and how long a
    /// strike is remembered after the last one (default: 10 min).
    ///
    /// Must be greater than zero.
    ///
    /// # Errors
    ///
    /// [`ConfigError::BlacklistDurationZero`] for zero.
    pub fn with_blacklist_duration(
        mut self,
        blacklist_duration: Duration,
    ) -> Result<Self, ConfigError> {
        self.blacklist_duration = blacklist_duration;
        self.validate()?;
        Ok(self)
    }

    /// Sets the shortest time between two discovery profile requests
    /// (default: 10 s).
    ///
    /// No scan or advertise mode change, and no start or stop of a quiet
    /// scan window, is requested sooner than this after the last. Android
    /// throttles apps that start scans more than five times in 30 s, which
    /// the default keeps well clear of. Must be greater than zero.
    ///
    /// # Errors
    ///
    /// [`ConfigError::DiscoveryIntervalZero`] for zero.
    pub fn with_discovery_interval(
        mut self,
        discovery_interval: Duration,
    ) -> Result<Self, ConfigError> {
        self.discovery_interval = discovery_interval;
        self.validate()?;
        Ok(self)
    }

    /// Sets whether this node offers credit-based flow control (default:
    /// on). A link uses it only when both `Hello`s set
    /// [`CAP_CREDIT`](crate::CAP_CREDIT); otherwise only the send-queue
    /// caps limit what a peer is sent.
    ///
    /// # Errors
    ///
    /// Never; it returns `Result` to chain like the other setters.
    pub fn with_flow_control(mut self, on: bool) -> Result<Self, ConfigError> {
        self.flow_control = on;
        self.validate()?;
        Ok(self)
    }

    /// Sets how many bytes of `Data` frames on `channel` a peer may have
    /// sent but this node not yet processed, on each link with flow
    /// control (default: the channel's default send-queue cap).
    ///
    /// Must be in `max_frame..=1 MiB`, so the largest frame the peer may
    /// send always fits; raise the windows before `max_frame`. A peer
    /// that sends past its window is closed with
    /// [`DisconnectReason::ProtocolViolation`](crate::DisconnectReason::ProtocolViolation).
    ///
    /// # Errors
    ///
    /// [`ConfigError::ReceiveWindowOutOfRange`] outside the bounds.
    pub fn with_receive_window(
        mut self,
        channel: Channel,
        window: usize,
    ) -> Result<Self, ConfigError> {
        self.receive_windows[channel.index()] = window;
        self.validate()?;
        Ok(self)
    }

    /// The degree cap: how many links this node keeps at once.
    pub fn max_links(&self) -> u32 {
        self.max_links
    }

    /// How many discovered peers the peer table tracks at once.
    pub fn max_peers(&self) -> u32 {
        self.max_peers
    }

    /// The minimum RSSI (dBm) at which a neighbour is a connect
    /// candidate: weaker sightings are still recorded in the peer table
    /// but never attempted (NET-15).
    pub fn min_rssi(&self) -> i16 {
        self.min_rssi
    }

    /// How long the handshake may take before the link is dropped.
    pub fn handshake_timeout(&self) -> Duration {
        self.handshake_timeout
    }

    /// How long one frame may take to reassemble, from its first chunk.
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

    /// The send-queue byte cap of `channel` on each link.
    pub fn channel_cap(&self, channel: Channel) -> usize {
        self.channel_caps[channel.index()]
    }

    /// How many frames a peer may send on one link.
    pub fn frame_limit(&self) -> RateLimit {
        self.frame_limit
    }

    /// How many bytes a peer may send on one link.
    pub fn byte_limit(&self) -> RateLimit {
        self.byte_limit
    }

    /// How many strikes blacklist a peer.
    pub fn max_strikes(&self) -> u8 {
        self.max_strikes
    }

    /// How long a blacklisted peer stays blocked and a strike is
    /// remembered.
    pub fn blacklist_duration(&self) -> Duration {
        self.blacklist_duration
    }

    /// The shortest time between two discovery profile requests.
    pub fn discovery_interval(&self) -> Duration {
        self.discovery_interval
    }

    /// Whether this node offers credit-based flow control.
    pub fn flow_control(&self) -> bool {
        self.flow_control
    }

    /// How many unprocessed bytes of `Data` frames on `channel` a peer
    /// may have in flight on a flow-controlled link.
    pub fn receive_window(&self, channel: Channel) -> usize {
        self.receive_windows[channel.index()]
    }
}

/// Whether `limit` has a rate in `1..=MAX_RATE` and a burst in
/// `min_burst..=MAX_RATE`.
fn rate_ok(limit: RateLimit, min_burst: u32) -> bool {
    (1..=MAX_RATE).contains(&limit.per_second) && (min_burst..=MAX_RATE).contains(&limit.burst)
}

impl Default for NetworkConfig {
    /// The documented defaults; always valid, so constructing a config is
    /// infallible until you customise it.
    fn default() -> Self {
        Self {
            max_links: DEFAULT_MAX_LINKS,
            max_peers: DEFAULT_MAX_PEERS,
            min_rssi: DEFAULT_MIN_RSSI,
            handshake_timeout: DEFAULT_HANDSHAKE_TIMEOUT,
            reassembly_timeout: DEFAULT_REASSEMBLY_TIMEOUT,
            keepalive_interval: DEFAULT_KEEPALIVE_INTERVAL,
            max_missed_pongs: DEFAULT_MAX_MISSED_PONGS,
            max_frame: DEFAULT_MAX_FRAME,
            queue_cap: DEFAULT_QUEUE_CAP,
            channel_caps: DEFAULT_CHANNEL_CAPS,
            frame_limit: DEFAULT_FRAME_LIMIT,
            byte_limit: DEFAULT_BYTE_LIMIT,
            max_strikes: DEFAULT_MAX_STRIKES,
            blacklist_duration: DEFAULT_BLACKLIST_DURATION,
            discovery_interval: DEFAULT_DISCOVERY_INTERVAL,
            flow_control: DEFAULT_FLOW_CONTROL,
            receive_windows: DEFAULT_RECEIVE_WINDOWS,
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
        assert_eq!(cfg.max_peers(), 64);
        assert_eq!(cfg.min_rssi(), -90);
        assert_eq!(cfg.handshake_timeout(), Duration::from_secs(5));
        assert_eq!(cfg.reassembly_timeout(), Duration::from_secs(10));
        assert_eq!(cfg.keepalive_interval(), Duration::from_secs(15));
        assert_eq!(cfg.max_missed_pongs(), 3);
        assert_eq!(cfg.max_frame(), 16 * 1024);
        assert_eq!(cfg.queue_cap(), 128 * 1024);
        assert_eq!(cfg.channel_cap(Channel::Control), 16 * 1024);
        assert_eq!(cfg.channel_cap(Channel::Message), 48 * 1024);
        assert_eq!(cfg.channel_cap(Channel::Telemetry), 16 * 1024);
        assert_eq!(cfg.channel_cap(Channel::Bulk), 48 * 1024);
        let frames = RateLimit {
            per_second: 50,
            burst: 200,
        };
        assert_eq!(cfg.frame_limit(), frames);
        let bytes = RateLimit {
            per_second: 64 * 1024,
            burst: 256 * 1024,
        };
        assert_eq!(cfg.byte_limit(), bytes);
        assert_eq!(cfg.max_strikes(), 3);
        assert_eq!(cfg.blacklist_duration(), Duration::from_secs(600));
        assert_eq!(cfg.discovery_interval(), Duration::from_secs(10));
        assert!(cfg.flow_control());
        assert_eq!(cfg.receive_window(Channel::Control), 16 * 1024);
        assert_eq!(cfg.receive_window(Channel::Message), 48 * 1024);
        assert_eq!(cfg.receive_window(Channel::Telemetry), 16 * 1024);
        assert_eq!(cfg.receive_window(Channel::Bulk), 48 * 1024);
    }

    #[test]
    fn a_receive_window_must_fit_one_frame_and_stay_bounded() {
        let base = NetworkConfig::default();
        let too_small = base
            .clone()
            .with_receive_window(Channel::Bulk, 16 * 1024 - 1)
            .unwrap_err();
        assert_eq!(
            too_small,
            ConfigError::ReceiveWindowOutOfRange {
                channel: Channel::Bulk,
                value: 16 * 1024 - 1,
                min: 16 * 1024,
                max: 1024 * 1024,
            }
        );
        assert!(matches!(
            base.clone().with_receive_window(Channel::Message, 1024 * 1024 + 1),
            Err(ConfigError::ReceiveWindowOutOfRange { value, .. }) if value == 1024 * 1024 + 1
        ));
        // Raising max_frame past a window is refused until the window grows.
        assert!(matches!(
            base.clone().with_max_frame(32 * 1024),
            Err(ConfigError::ReceiveWindowOutOfRange {
                channel: Channel::Control,
                ..
            })
        ));
        let lowered = base.with_flow_control(false).unwrap();
        assert!(!lowered.flow_control());
    }

    #[test]
    fn rate_limit_bounds_are_enforced() {
        let base = NetworkConfig::default();
        let limit = |per_second, burst| RateLimit { per_second, burst };
        let max = 16 * 1024 * 1024;
        for bad in [
            limit(0, 10),
            limit(10, 0),
            limit(max + 1, 10),
            limit(10, max + 1),
        ] {
            assert_eq!(
                base.clone().with_frame_limit(bad).unwrap_err(),
                ConfigError::FrameLimitOutOfRange {
                    limit: bad,
                    min_burst: 1,
                    max
                }
            );
        }
        for good in [limit(1, 1), limit(max, max)] {
            assert_eq!(
                base.clone().with_frame_limit(good).unwrap().frame_limit(),
                good
            );
        }
        for bad in [limit(0, 1024), limit(1024, 516), limit(1024, max + 1)] {
            assert_eq!(
                base.clone().with_byte_limit(bad).unwrap_err(),
                ConfigError::ByteLimitOutOfRange {
                    limit: bad,
                    min_burst: 517,
                    max
                }
            );
        }
        let good = limit(1, 517);
        assert_eq!(base.with_byte_limit(good).unwrap().byte_limit(), good);
    }

    #[test]
    fn strikes_and_blacklist_duration_must_be_positive() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone().with_max_strikes(0).unwrap_err(),
            ConfigError::MaxStrikesZero
        );
        assert_eq!(base.clone().with_max_strikes(1).unwrap().max_strikes(), 1);
        assert_eq!(
            base.clone()
                .with_blacklist_duration(Duration::ZERO)
                .unwrap_err(),
            ConfigError::BlacklistDurationZero
        );
        let one = Duration::from_nanos(1);
        assert_eq!(
            base.with_blacklist_duration(one)
                .unwrap()
                .blacklist_duration(),
            one
        );
    }

    #[test]
    fn discovery_interval_must_be_positive() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone()
                .with_discovery_interval(Duration::ZERO)
                .unwrap_err(),
            ConfigError::DiscoveryIntervalZero
        );
        let one = Duration::from_nanos(1);
        assert_eq!(
            base.with_discovery_interval(one)
                .unwrap()
                .discovery_interval(),
            one
        );
    }

    #[test]
    fn max_peers_bounds_are_enforced() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone().with_max_peers(0).unwrap_err(),
            ConfigError::MaxPeersOutOfRange {
                value: 0,
                min: 1,
                max: 1024
            }
        );
        assert_eq!(
            base.clone().with_max_peers(1025).unwrap_err(),
            ConfigError::MaxPeersOutOfRange {
                value: 1025,
                min: 1,
                max: 1024
            }
        );
        assert_eq!(base.clone().with_max_peers(1).unwrap().max_peers(), 1);
        assert_eq!(base.with_max_peers(1024).unwrap().max_peers(), 1024);
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
                .with_reassembly_timeout(Duration::ZERO)
                .unwrap_err(),
            ConfigError::ReassemblyTimeoutZero
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
        // The 1 MiB ceiling is reachable only after the queue cap and the
        // receive windows can hold that frame.
        assert_eq!(
            windows(&base, 1024 * 1024)
                .with_queue_cap(1024 * 1024)
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
            small_channels(&base)
                .with_queue_cap(16 * 1024)
                .unwrap()
                .queue_cap(),
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
            windows(&base, 128 * 1024)
                .with_max_frame(128 * 1024)
                .unwrap()
                .max_frame(),
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
            .with_channel_cap(Channel::Message, 16 * 1024)
            .unwrap()
            .with_channel_cap(Channel::Bulk, 16 * 1024)
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
        assert_eq!(cfg.channel_cap(Channel::Message), 16 * 1024);
        assert_eq!(cfg.channel_cap(Channel::Bulk), 16 * 1024);
        assert_eq!(cfg.validate(), Ok(()));
    }

    /// `base` with every channel's receive window at `window`.
    fn windows(base: &NetworkConfig, window: usize) -> NetworkConfig {
        Channel::ALL.into_iter().fold(base.clone(), |cfg, channel| {
            cfg.with_receive_window(channel, window).unwrap()
        })
    }

    /// `base` with every channel capped at 4 KiB (16 KiB in total).
    fn small_channels(base: &NetworkConfig) -> NetworkConfig {
        Channel::ALL.into_iter().fold(base.clone(), |cfg, channel| {
            cfg.with_channel_cap(channel, 4 * 1024).unwrap()
        })
    }

    #[test]
    fn channel_cap_below_the_smallest_frame_is_rejected() {
        let base = NetworkConfig::default();
        assert_eq!(
            base.clone()
                .with_channel_cap(Channel::Telemetry, 127)
                .unwrap_err(),
            ConfigError::ChannelCapTooSmall {
                channel: Channel::Telemetry,
                value: 127,
                min: 128
            }
        );
        assert_eq!(
            base.with_channel_cap(Channel::Telemetry, 128)
                .unwrap()
                .channel_cap(Channel::Telemetry),
            128
        );
    }

    #[test]
    fn channel_caps_must_fit_in_queue_cap() {
        let base = NetworkConfig::default();
        // Defaults fill queue_cap exactly, so any growth overflows it.
        assert_eq!(
            base.clone()
                .with_channel_cap(Channel::Control, 16 * 1024 + 1)
                .unwrap_err(),
            ConfigError::ChannelCapsOverQueueCap {
                total: 128 * 1024 + 1,
                queue_cap: 128 * 1024
            }
        );
        // Shrinking queue_cap below the caps fails the same way.
        assert_eq!(
            base.clone().with_queue_cap(64 * 1024).unwrap_err(),
            ConfigError::ChannelCapsOverQueueCap {
                total: 128 * 1024,
                queue_cap: 64 * 1024
            }
        );
        // A huge cap saturates the total instead of wrapping past the check.
        assert_eq!(
            base.clone()
                .with_channel_cap(Channel::Bulk, usize::MAX)
                .unwrap_err(),
            ConfigError::ChannelCapsOverQueueCap {
                total: usize::MAX,
                queue_cap: 128 * 1024
            }
        );
        let raised = base
            .with_queue_cap(256 * 1024)
            .unwrap()
            .with_channel_cap(Channel::Bulk, 176 * 1024)
            .unwrap();
        assert_eq!(raised.channel_cap(Channel::Bulk), 176 * 1024);
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
