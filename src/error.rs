//! Typed errors.
//!
//! Every failure path in this crate maps to one of these enums; nothing is
//! signalled by `panic!`, `None` or a bare `bool`. All error enums are
//! `#[non_exhaustive]` so later issues (codec, transport, topology) can add
//! variants without breaking downstream matches.

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
}

/// Errors from network-level operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum NetError {
    /// The supplied configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(#[from] ConfigError),
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
            Self::QueueFull(payload) => payload,
        }
    }
}

/// Why a link came down.
///
/// Handed to the owner of the link (and eventually mapped to Android status
/// codes in NET-03) so the layer above can decide whether to reconnect,
/// back off or report. `#[non_exhaustive]`: later issues add reasons as the
/// state machine, guard and duplicate resolution land.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum DisconnectReason {
    /// Closed on our side, deliberately.
    Closed,
    /// The peer sent a `Bye` frame.
    Bye,
    /// The handshake did not complete (timeout or rejection).
    HandshakeFailed,
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
            DisconnectReason::HandshakeFailed,
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
