//! Typed errors.
//!
//! Every failure path in this crate maps to one of these enums; nothing is
//! signalled by `panic!`, `None` or a bare `bool`. All error enums are
//! `#[non_exhaustive]` so later issues (codec, transport, topology) can add
//! variants without breaking downstream matches.

use crate::Channel;

/// A configuration value failed validation.
///
/// Returned by the `NetworkConfig::with_*` setters; no invalid configuration
/// can be constructed through the public API. Bounds are carried in the error
/// so a caller can fix the value without reading this source.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// `max_links` is outside the supported `min..=max` range.
    #[error("max_links must be in {min}..={max}, got {value}")]
    MaxLinksOutOfRange {
        /// The rejected value.
        value: u32,
        /// Inclusive lower bound.
        min: u32,
        /// Inclusive upper bound.
        max: u32,
    },

    /// `max_peers` is outside the supported `min..=max` range.
    #[error("max_peers must be in {min}..={max}, got {value}")]
    MaxPeersOutOfRange {
        /// The rejected value.
        value: u32,
        /// Inclusive lower bound.
        min: u32,
        /// Inclusive upper bound.
        max: u32,
    },

    /// `min_rssi` is outside the BLE RSSI domain `min..=max` (dBm).
    #[error("min_rssi must be in {min}..={max} dBm, got {value}")]
    MinRssiOutOfRange {
        /// The rejected value.
        value: i16,
        /// Inclusive lower bound.
        min: i16,
        /// Inclusive upper bound.
        max: i16,
    },

    /// `handshake_timeout` is zero, so the handshake could never time out.
    #[error("handshake_timeout must be greater than zero")]
    HandshakeTimeoutZero,

    /// `reassembly_timeout` is zero, so no multi-chunk frame could complete.
    #[error("reassembly_timeout must be greater than zero")]
    ReassemblyTimeoutZero,

    /// `keepalive_interval` is zero, so keepalive would fire every instant.
    #[error("keepalive_interval must be greater than zero")]
    KeepaliveIntervalZero,

    /// `max_missed_pongs` is zero, so the link would close before any pong
    /// could be counted.
    #[error("max_missed_pongs must be at least 1")]
    MaxMissedPongsZero,

    /// `max_frame` is outside the supported `min..=max` byte range.
    #[error("max_frame must be in {min}..={max} bytes, got {value}")]
    MaxFrameOutOfRange {
        /// The rejected value.
        value: usize,
        /// Inclusive lower bound.
        min: usize,
        /// Inclusive upper bound.
        max: usize,
    },

    /// `queue_cap` is outside `min..=max` bytes. The lower bound is the
    /// current `max_frame`: a queue that cannot hold one full frame could
    /// never make progress.
    #[error("queue_cap must be in {min}..={max} bytes, got {value}")]
    QueueCapOutOfRange {
        /// The rejected value.
        value: usize,
        /// Inclusive lower bound (the current `max_frame`).
        min: usize,
        /// Inclusive upper bound.
        max: usize,
    },

    /// A channel's byte cap is below the smallest allowed frame.
    #[error("{channel:?} cap must be at least {min} bytes, got {value}")]
    ChannelCapTooSmall {
        /// The channel.
        channel: Channel,
        /// The rejected value.
        value: usize,
        /// Inclusive lower bound.
        min: usize,
    },

    /// The channel caps add up to more than `queue_cap`, the per-link
    /// memory budget they share.
    #[error("channel caps total {total} bytes, over queue_cap {queue_cap}")]
    ChannelCapsOverQueueCap {
        /// Sum of the four channel caps.
        total: usize,
        /// The current `queue_cap`.
        queue_cap: usize,
    },
}

/// A beacon payload failed structural validation.
///
/// These errors describe untrusted discovery data, not an authenticated peer.
/// They contain only the rejected length or header, never the hint or digest.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum BeaconError {
    /// The input is not exactly one fixed-size beacon payload.
    #[error("invalid beacon length: expected {expected} bytes, got {actual}")]
    InvalidLength {
        /// Required payload length for this codec.
        expected: usize,
        /// Received payload length.
        actual: usize,
    },

    /// The sender uses a version this codec does not understand.
    #[error("unsupported beacon version: {version}")]
    UnsupportedVersion {
        /// Received version byte.
        version: u8,
    },

    /// Reserved flag bits were set in a version-1 beacon.
    #[error("unsupported beacon flag bits: {unknown:#04x}")]
    UnsupportedFlags {
        /// Only the unsupported bits from the received flag byte.
        unknown: u8,
    },
}

/// A link frame failed structural validation.
///
/// Frames come from untrusted neighbours; every variant is a normal outcome
/// of decoding hostile bytes. Variants carry lengths and header bytes only,
/// never payload contents.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FrameError {
    /// The input is longer than the receiver's `max_frame`.
    #[error("frame of {len} bytes exceeds max_frame {max}")]
    Oversize {
        /// Received length.
        len: usize,
        /// The receiver's limit.
        max: usize,
    },

    /// The input ends before the frame does.
    #[error("frame truncated")]
    Truncated,

    /// The sender uses a frame version this codec does not understand.
    #[error("unsupported frame version: {version}")]
    UnsupportedVersion {
        /// Received version byte.
        version: u8,
    },

    /// The type byte names no known frame.
    #[error("unknown frame type: {frame_type:#04x}")]
    UnknownType {
        /// Received type byte.
        frame_type: u8,
    },

    /// A `Data` frame names no known channel.
    #[error("unknown channel: {channel}")]
    UnknownChannel {
        /// Received channel byte.
        channel: u8,
    },

    /// A LEB128 length is overlong or overflows 64 bits.
    #[error("malformed LEB128 length")]
    InvalidVarint,

    /// An `app_hello` blob is longer than allowed.
    #[error("app_hello of {len} bytes exceeds {max}")]
    AppHelloTooLong {
        /// Received or supplied length.
        len: usize,
        /// The allowed maximum.
        max: usize,
    },

    /// Bytes remain after a complete frame.
    #[error("{extra} trailing bytes after frame")]
    TrailingBytes {
        /// Number of unread bytes.
        extra: usize,
    },
}

/// A chunk could not be produced or reassembled.
///
/// Every receive-side variant means the partial frame was dropped; the next
/// frame must begin with a fresh START chunk.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum FragmentError {
    /// The ATT MTU is outside the range a BLE stack may negotiate.
    #[error("unsupported ATT MTU: {mtu}")]
    InvalidMtu {
        /// The rejected MTU.
        mtu: u16,
    },

    /// A chunk carries no payload.
    #[error("chunk has no payload")]
    EmptyChunk,

    /// A chunk's sequence number is not the next one expected.
    #[error("chunk sequence gap: expected {expected}, got {actual}")]
    SeqGap {
        /// Expected sequence number.
        expected: u8,
        /// Received sequence number.
        actual: u8,
    },

    /// A START chunk arrived while another frame was still in flight.
    #[error("START received while a frame is in flight")]
    StartWhileBusy,

    /// A continuation chunk arrived with no frame in flight.
    #[error("continuation received without START")]
    MissingStart,

    /// The frame would exceed the receiver's `max_frame`.
    #[error("frame of {len} bytes exceeds max_frame {max}")]
    TooLarge {
        /// Bytes the frame would hold with this chunk.
        len: usize,
        /// The receiver's limit.
        max: usize,
    },

    /// The frame did not complete within `reassembly_timeout`.
    #[error("frame reassembly timed out")]
    Timeout,
}

/// Why an outbound link never came up.
///
/// The mapping from Android status codes is specified in
/// `docs/native-contract.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, thiserror::Error)]
#[non_exhaustive]
pub enum ConnectError {
    /// The radio or our own timer gave up waiting.
    #[error("connect timed out")]
    Timeout,
    /// We closed the link before it came up.
    #[error("connect cancelled")]
    Cancelled,
    /// The peer refused or had no room.
    #[error("connect rejected by peer")]
    Rejected,
    /// The peer could not be reached.
    #[error("peer unreachable")]
    Unreachable,
    /// Connected, but service discovery or subscription failed.
    #[error("link setup failed")]
    SetupFailed,
    /// The peer does not offer the aoheyra GATT service.
    #[error("peer lacks the aoheyra service")]
    ServiceMissing,
    /// The radio has no address for the peer (not seen in a scan).
    #[error("peer unknown to the radio")]
    PeerUnknown,
    /// The radio is off or not permitted.
    #[error("radio unavailable")]
    RadioUnavailable,
}

/// Errors from network-level operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum NetError {
    /// The supplied configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(#[from] ConfigError),

    /// A received discovery beacon is malformed or unsupported.
    #[error("invalid beacon: {0}")]
    InvalidBeacon(#[from] BeaconError),

    /// A received link frame is malformed or unsupported.
    #[error("invalid frame: {0}")]
    InvalidFrame(#[from] FrameError),
}

/// A send could not be queued.
///
/// Nothing is ever dropped silently: everything the caller handed over either
/// reaches the queue or comes back here.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SendError {
    /// The link's send queue is at its configured byte cap. The payload is
    /// returned to the caller untouched so it can be retried or surfaced.
    #[error("send queue full; payload returned to the caller")]
    QueueFull(Vec<u8>),
    /// The link is closing or closed. The payload is returned untouched.
    #[error("link closed; payload returned to the caller")]
    LinkClosed(Vec<u8>),
    /// The payload's frame exceeds the peer's `max_frame`, so the peer
    /// would reject it. The payload is returned untouched.
    #[error("payload too large for the peer; payload returned to the caller")]
    TooLarge(Vec<u8>),
}

impl SendError {
    /// Returns the payload that could not be queued.
    ///
    /// # Example
    ///
    /// ```
    /// use ao_network::SendError;
    ///
    /// let err = SendError::QueueFull(vec![0xAA; 4]);
    /// assert_eq!(err.into_payload(), vec![0xAA; 4]);
    /// ```
    pub fn into_payload(self) -> Vec<u8> {
        match self {
            Self::QueueFull(payload) | Self::LinkClosed(payload) | Self::TooLarge(payload) => {
                payload
            }
        }
    }
}

/// Why a link came down.
///
/// Handed to the owner of the link so the layer above can decide whether to
/// reconnect, back off or report. The mapping from Android status codes is
/// specified in `docs/native-contract.md`. `#[non_exhaustive]`: later issues
/// add reasons as the state machine, guard and duplicate resolution land.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// Closed on our side, deliberately.
    Closed,
    /// The peer sent a `Bye` frame.
    Bye,
    /// The handshake did not complete.
    HandshakeFailed(HandshakeFailure),
    /// Keepalive saw `max_missed_pongs` consecutive missed pongs.
    KeepaliveTimeout,
    /// Malformed or unexpected traffic from the peer.
    ProtocolViolation,
    /// The abuse guard struck or blacklisted the peer.
    AbuseDetected,
    /// Evicted to stay within the degree cap.
    DegreeCap,
    /// Lost a duplicate-link tie-break against a better link.
    Duplicate,
    /// The underlying transport or radio went away.
    TransportLost,
}

/// Why a handshake failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HandshakeFailure {
    /// The two sides share no link protocol version.
    VersionMismatch,
    /// The peer's `app_hello` exceeds `MAX_APP_HELLO`.
    OversizeHello,
    /// No valid `Hello` arrived within `handshake_timeout`.
    Timeout,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_full_returns_the_payload() {
        let payload = vec![1, 2, 3, 4, 5];
        let err = SendError::QueueFull(payload.clone());
        assert_eq!(err.into_payload(), payload);
    }

    #[test]
    fn queue_full_display_is_safe() {
        let err = SendError::QueueFull(vec![0; 1024]);
        let text = err.to_string();
        assert!(!text.is_empty());
    }

    #[test]
    fn net_error_wraps_config_error_without_loss() {
        let cause = ConfigError::HandshakeTimeoutZero;
        let err = NetError::from(cause.clone());
        assert_eq!(err, NetError::InvalidConfig(cause));
        assert!(err.to_string().contains("handshake_timeout"));
    }

    #[test]
    fn config_error_display_carries_the_bounds() {
        let err = ConfigError::MaxLinksOutOfRange {
            value: 0,
            min: 1,
            max: 32,
        };
        let text = err.to_string();
        assert!(text.contains("1..=32"), "message was: {text}");
        assert!(text.contains("0"), "message was: {text}");
    }

    #[test]
    fn every_disconnect_reason_exists_and_is_distinct() {
        // Compile-time guard for the documented set; distinctness catches a
        // copy-paste of the wrong variant into two arms later on.
        let reasons = [
            DisconnectReason::Closed,
            DisconnectReason::Bye,
            DisconnectReason::HandshakeFailed(HandshakeFailure::VersionMismatch),
            DisconnectReason::HandshakeFailed(HandshakeFailure::OversizeHello),
            DisconnectReason::HandshakeFailed(HandshakeFailure::Timeout),
            DisconnectReason::KeepaliveTimeout,
            DisconnectReason::ProtocolViolation,
            DisconnectReason::AbuseDetected,
            DisconnectReason::DegreeCap,
            DisconnectReason::Duplicate,
            DisconnectReason::TransportLost,
        ];
        for (i, a) in reasons.iter().enumerate() {
            for (j, b) in reasons.iter().enumerate() {
                assert_eq!(i == j, a == b, "{a:?} vs {b:?}");
            }
        }
    }
}
