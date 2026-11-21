//! What a node shows of itself (NET-23).
//!
//! [`Network`](crate::Network) logs each link's lifecycle through
//! [`tracing`], in one span per link, keeps cumulative [`Counters`], and
//! hands out a [`Snapshot`] for a debug screen or a diagnostics bundle.
//! None of it changes what the node does.

use std::fmt;
use std::time::Duration;

use crate::{
    DisconnectReason, DiscoveryMode, LinkClass, LinkId, LinkMetrics, LinkRole, LinkState, PeerKey,
    QueueDrops,
};

/// Links that ended after connecting, by reason. A handshake that failed
/// counts here too, under `handshake_failed`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct DownCounts {
    /// [`DisconnectReason::Closed`].
    pub closed: u64,
    /// [`DisconnectReason::Bye`].
    pub bye: u64,
    /// [`DisconnectReason::HandshakeFailed`], whatever the failure.
    pub handshake_failed: u64,
    /// [`DisconnectReason::KeepaliveTimeout`].
    pub keepalive_timeout: u64,
    /// [`DisconnectReason::ProtocolViolation`].
    pub protocol_violation: u64,
    /// [`DisconnectReason::AbuseDetected`].
    pub abuse_detected: u64,
    /// [`DisconnectReason::DegreeCap`].
    pub degree_cap: u64,
    /// [`DisconnectReason::Duplicate`].
    pub duplicate: u64,
    /// [`DisconnectReason::TransportLost`].
    pub transport_lost: u64,
}

impl DownCounts {
    pub(crate) fn count(&mut self, reason: DisconnectReason) {
        let slot = match reason {
            DisconnectReason::Closed => &mut self.closed,
            DisconnectReason::Bye => &mut self.bye,
            DisconnectReason::HandshakeFailed(_) => &mut self.handshake_failed,
            DisconnectReason::KeepaliveTimeout => &mut self.keepalive_timeout,
            DisconnectReason::ProtocolViolation => &mut self.protocol_violation,
            DisconnectReason::AbuseDetected => &mut self.abuse_detected,
            DisconnectReason::DegreeCap => &mut self.degree_cap,
            DisconnectReason::Duplicate => &mut self.duplicate,
            DisconnectReason::TransportLost => &mut self.transport_lost,
        };
        *slot = slot.saturating_add(1);
    }

    /// Every link counted, whatever the reason.
    pub fn total(&self) -> u64 {
        [
            self.closed,
            self.bye,
            self.handshake_failed,
            self.keepalive_timeout,
            self.protocol_violation,
            self.abuse_detected,
            self.degree_cap,
            self.duplicate,
            self.transport_lost,
        ]
        .iter()
        .fold(0, |sum, &n| sum.saturating_add(n))
    }
}

/// What a node has done since it was created. Every count only grows,
/// saturating at `u64::MAX`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct Counters {
    /// Links that finished their handshake and were announced.
    pub links_up: u64,
    /// Links that ended after connecting, by reason.
    pub links_down: DownCounts,
    /// Connection attempts that never connected.
    pub connects_failed: u64,
    /// Frames the send queues discarded unwritten, links that have ended
    /// included.
    pub queue_drops: QueueDrops,
    /// Sends refused with [`SendError::QueueFull`](crate::SendError::QueueFull).
    pub queue_full: u64,
    /// Strikes recorded against peers.
    pub strikes: u64,
    /// Times a peer was blacklisted.
    pub blacklisted: u64,
}

/// One link in a [`Snapshot`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct LinkSnapshot {
    /// The link.
    pub link: LinkId,
    /// The radio peer at the other end.
    pub peer: PeerKey,
    /// Which side opened it.
    pub role: LinkRole,
    /// The slot it holds.
    pub class: LinkClass,
    /// Where it is in its lifecycle.
    pub state: LinkState,
    /// The injected time its handshake finished, or `ZERO` while it has
    /// not come up.
    pub up_at: Duration,
    /// What it has seen so far.
    pub metrics: LinkMetrics,
    /// Bytes waiting in its send queue.
    pub queued_bytes: usize,
}

/// A node as it stands, from [`Network::snapshot`](crate::Network::snapshot).
#[derive(Debug, Clone, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub struct Snapshot {
    /// The time it was taken at.
    pub at: Duration,
    /// The discovery mode in force.
    pub mode: DiscoveryMode,
    /// Every link, connecting, up or closing, in `LinkId` order.
    pub links: Vec<LinkSnapshot>,
    /// Peers in the peer table.
    pub peers: usize,
    /// Advisory link hints held for peers.
    pub hints: usize,
    /// Peers blacklisted at `at`.
    pub blocked: usize,
    /// What the node has done so far.
    pub counters: Counters,
}

/// Bytes as lowercase hex, for log fields.
pub(crate) struct Hex<'a>(pub(crate) &'a [u8]);

impl fmt::Display for Hex<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HandshakeFailure;

    /// Every reason lands in its own field, and the total adds them up.
    #[test]
    fn every_reason_has_its_count() {
        let reasons = [
            DisconnectReason::Closed,
            DisconnectReason::Bye,
            DisconnectReason::HandshakeFailed(HandshakeFailure::Timeout),
            DisconnectReason::HandshakeFailed(HandshakeFailure::VersionMismatch),
            DisconnectReason::KeepaliveTimeout,
            DisconnectReason::ProtocolViolation,
            DisconnectReason::AbuseDetected,
            DisconnectReason::DegreeCap,
            DisconnectReason::Duplicate,
            DisconnectReason::TransportLost,
        ];
        let mut counts = DownCounts::default();
        for reason in reasons {
            counts.count(reason);
        }
        assert_eq!(counts.handshake_failed, 2);
        let singles = [
            counts.closed,
            counts.bye,
            counts.keepalive_timeout,
            counts.protocol_violation,
            counts.abuse_detected,
            counts.degree_cap,
            counts.duplicate,
            counts.transport_lost,
        ];
        assert!(singles.iter().all(|&n| n == 1), "{counts:?}");
        assert_eq!(counts.total(), 10);

        counts.closed = u64::MAX;
        counts.count(DisconnectReason::Closed);
        assert_eq!(counts.closed, u64::MAX, "saturates");
        assert_eq!(counts.total(), u64::MAX);
    }

    #[test]
    fn hex_is_lowercase_and_padded() {
        assert_eq!(Hex(&[0x0a, 0xff, 0x00]).to_string(), "0aff00");
    }
}
