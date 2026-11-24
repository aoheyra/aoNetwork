//! The seam between the link layer and a radio.
//!
//! The link layer runs against [`Transport`] only. `SimWorld` (NET-09)
//! implements it in memory; `NativeTransport` (NET-14) and `L2capTransport`
//! (NET-26) implement it over the Kotlin BLE shim described in
//! `docs/native-contract.md`. A transport names its radio channel through
//! [`Transport::link_transport`], which defaults to GATT.

use std::task::{Context, Poll};

use crate::{BEACON_LEN, ConnectError, DisconnectReason, LinkId, LinkRole, LinkTransport, PeerKey};

/// A radio the link layer can drive.
///
/// Rules every implementation follows:
///
/// - **Commands return immediately.** Nothing blocks or waits on the radio;
///   outcomes arrive later as [`TransportEvent`]s.
/// - **The transport allocates every [`LinkId`]**: in [`connect`] for
///   outbound links, and in [`TransportEvent::Connected`] for inbound ones.
///   Ids are never reused.
/// - **One chunk in flight per link.** After [`TransportEvent::Connected`]
///   the link may [`write`] once; each [`TransportEvent::WriteReady`] allows
///   one more.
/// - **Per-link order.** `Connected` comes first. `Received`, `WriteReady`,
///   `MtuChanged` and `Rssi` keep the order the radio produced them. Exactly
///   one `ConnectFailed` (never connected) or `Disconnected` (was connected)
///   ends every link, and nothing follows it. Different links are not
///   ordered relative to each other.
/// - **Local closes are not explained.** After [`disconnect`] the link ends
///   with `ConnectFailed(Cancelled)` or `Disconnected(Closed)`; the caller
///   keeps its own reason for closing.
/// - Commands naming a link that has already ended are ignored.
///
/// [`connect`]: Transport::connect
/// [`write`]: Transport::write
/// [`disconnect`]: Transport::disconnect
///
/// # Example
///
/// ```
/// use ao_network::{ConnectError, LinkId, PeerKey, Transport, TransportCaps, TransportEvent};
/// use std::collections::VecDeque;
/// use std::task::{Context, Poll, Waker};
///
/// /// A transport with no peers: every connect fails.
/// #[derive(Default)]
/// struct Empty {
///     next_link: u64,
///     events: VecDeque<TransportEvent>,
/// }
///
/// impl Transport for Empty {
///     fn caps(&self) -> TransportCaps {
///         TransportCaps { can_advertise: false, max_mtu: 23, max_links: 0 }
///     }
///     fn start(&mut self) {}
///     fn stop(&mut self) {}
///     fn set_advertising(&mut self, _: Option<ao_network::AdvProfile>) {}
///     fn set_scanning(&mut self, _: Option<ao_network::ScanProfile>) {}
///     fn connect(&mut self, _: PeerKey) -> LinkId {
///         self.next_link += 1;
///         let link = LinkId::new(self.next_link);
///         self.events.push_back(TransportEvent::ConnectFailed {
///             link,
///             error: ConnectError::PeerUnknown,
///         });
///         link
///     }
///     fn disconnect(&mut self, _: LinkId) {}
///     fn write(&mut self, _: LinkId, _: Vec<u8>) {}
///     fn poll_event(&mut self, _: &mut Context<'_>) -> Poll<TransportEvent> {
///         // A real transport stores the waker and wakes it on new events.
///         self.events.pop_front().map_or(Poll::Pending, Poll::Ready)
///     }
/// }
///
/// let mut radio: Box<dyn Transport> = Box::new(Empty::default());
/// let link = radio.connect(PeerKey::new([1; 16]));
/// let mut cx = Context::from_waker(Waker::noop());
/// assert_eq!(
///     radio.poll_event(&mut cx),
///     Poll::Ready(TransportEvent::ConnectFailed { link, error: ConnectError::PeerUnknown })
/// );
/// ```
pub trait Transport: Send {
    /// What this radio can do. Constant for the transport's lifetime.
    fn caps(&self) -> TransportCaps;

    /// The channel this transport carries frames on. The link layer
    /// advertises it in `Hello.caps` so both ends know what is in use.
    /// Transports that predate NET-26 carry GATT.
    fn link_transport(&self) -> LinkTransport {
        LinkTransport::Gatt
    }

    /// Begins producing events, starting with the current
    /// [`TransportEvent::RadioState`].
    fn start(&mut self);

    /// Stops advertising and scanning and closes every link; each still
    /// gets its terminal event.
    fn stop(&mut self);

    /// Starts or updates advertising, or stops it with `None`.
    fn set_advertising(&mut self, profile: Option<AdvProfile>);

    /// Starts or updates scanning, or stops it with `None`.
    fn set_scanning(&mut self, profile: Option<ScanProfile>);

    /// Opens a link to `peer` and returns its id. The outcome arrives as
    /// `Connected` or `ConnectFailed` for that id.
    fn connect(&mut self, peer: PeerKey) -> LinkId;

    /// Closes `link`, whether connecting or connected.
    fn disconnect(&mut self, link: LinkId);

    /// Sends one chunk on `link`: at most `mtu - 3` bytes over GATT, or
    /// `mtu` over CoC. Only valid while the link holds its write credit
    /// (see the trait rules).
    fn write(&mut self, link: LinkId, chunk: Vec<u8>);

    /// Returns the next event, or registers `cx`'s waker and returns
    /// `Pending` if there is none yet.
    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<TransportEvent>;
}

/// Something the radio reports.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransportEvent {
    /// The radio's availability changed.
    RadioState(RadioState),
    /// A peer was seen while scanning. `beacon` is the raw, unvalidated
    /// beacon payload (empty if none was advertised).
    Discovered {
        /// The advertising peer.
        peer: PeerKey,
        /// Received signal strength, dBm.
        rssi: i16,
        /// Raw beacon payload; decode with `Beacon::decode`.
        beacon: Vec<u8>,
    },
    /// The radio reports `peer` is no longer in range. Optional: not every
    /// radio reports it, so peer expiry must not depend on it.
    Lost {
        /// The peer that went away.
        peer: PeerKey,
    },
    /// A link is up, opened by us or by the peer.
    Connected {
        /// The link's id; new for inbound links.
        link: LinkId,
        /// The remote radio.
        peer: PeerKey,
        /// Who opened the link.
        role: LinkRole,
        /// Negotiated packet size: the ATT MTU over GATT or the CoC MTU
        /// over L2CAP, 23..=517.
        mtu: u16,
    },
    /// An outbound link never came up. Terminal for `link`.
    ConnectFailed {
        /// The link from [`Transport::connect`].
        link: LinkId,
        /// Why it failed.
        error: ConnectError,
    },
    /// The packet size of a connected link changed.
    MtuChanged {
        /// The link.
        link: LinkId,
        /// New packet size, 23..=517.
        mtu: u16,
    },
    /// One chunk arrived, exactly as sent: never split or merged.
    Received {
        /// The link.
        link: LinkId,
        /// Raw chunk bytes, untrusted.
        chunk: Vec<u8>,
    },
    /// The previous write was accepted; the link may write again.
    WriteReady {
        /// The link.
        link: LinkId,
    },
    /// A signal-strength reading for a connected link, if the radio
    /// measures one.
    Rssi {
        /// The link.
        link: LinkId,
        /// Received signal strength, dBm.
        rssi: i16,
    },
    /// A connected link went down. Terminal for `link`.
    Disconnected {
        /// The link.
        link: LinkId,
        /// Why, as far as the radio knows.
        reason: DisconnectReason,
    },
}

/// Whether the radio can be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RadioState {
    /// On and permitted.
    Ready,
    /// Switched off.
    Off,
    /// The app lacks permission to use it.
    PermissionDenied,
}

/// Fixed abilities of a transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransportCaps {
    /// Can advertise and accept inbound links. Some phones are
    /// central-only.
    pub can_advertise: bool,
    /// Largest packet size the radio will negotiate: the ATT MTU over
    /// GATT or the CoC MTU over L2CAP.
    pub max_mtu: u16,
    /// Most simultaneous links the radio sustains.
    pub max_links: u32,
}

/// How hard the radio works while advertising or scanning: more power buys
/// faster discovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PowerMode {
    /// Slowest, cheapest.
    LowPower,
    /// The usual trade-off.
    Balanced,
    /// Fastest, most battery.
    LowLatency,
}

/// What to advertise and how.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdvProfile {
    /// Encoded beacon (`Beacon::encode`).
    pub beacon: [u8; BEACON_LEN],
    /// Advertising duty cycle.
    pub mode: PowerMode,
}

/// How to scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScanProfile {
    /// Scanning duty cycle.
    pub mode: PowerMode,
}
