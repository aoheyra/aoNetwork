//! The facade: one [`Network`] per node, commands down and events up.
//!
//! [`Network`] is sans-IO and single-owner. It owns the [`Transport`], every
//! [`Link`] and the local `Hello` and beacon; it reads no clock and spawns
//! nothing. The owner polls it with the current time and wakes it again at
//! [`Network::next_deadline`], so the same core runs in an app's async task
//! and, step for step, in the simulator.
//!
//! # Commands and events
//!
//! | Command | Events |
//! |---|---|
//! | [`set_local_hello`](Network::set_local_hello) | starts scanning; with a beacon also advertising; then `LinkUp` per neighbour whose handshake succeeds |
//! | [`set_beacon`](Network::set_beacon) | with a local `Hello`, starts advertising; then `LinkUp` as above |
//! | [`send`](Network::send), [`send_opts`](Network::send_opts) | `Data` at the peer, or a [`SendError`] here |
//! | [`broadcast`](Network::broadcast) | `send` to every up link |
//! | [`disconnect`](Network::disconnect) | `LinkDown` after an up link closes; no event for a cancelled connection attempt |
//! | [`links`](Network::links) | none; lists the up links |
//!
//! `LinkUp` comes once per link, after a successful handshake, and
//! `LinkDown` once after it, when that link ends. Links that never come up
//! produce no events.

use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, poll_fn};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::{
    AdvProfile, AppHello, Beacon, BeaconFlags, Channel, DisconnectReason, FRAME_VERSION, Hello,
    Link, LinkAction, LinkEnd, LinkId, LinkMetrics, LinkRole, LocalTag, NetworkConfig, PeerKey,
    PowerMode, RadioState, ScanProfile, SendError, SendOpts, Transport, TransportEvent,
};

/// Something the layer above must know about.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum NetworkEvent {
    /// A link finished its handshake and can carry data.
    LinkUp {
        /// The link.
        link: LinkId,
        /// The peer's `Hello`, as received; unauthenticated.
        remote_hello: Hello,
    },
    /// A payload arrived on an up link.
    Data {
        /// The link it arrived on.
        link: LinkId,
        /// Its channel.
        channel: Channel,
        /// Its payload.
        payload: Vec<u8>,
    },
    /// An up link ended.
    LinkDown {
        /// The link.
        link: LinkId,
        /// Why it ended.
        reason: DisconnectReason,
    },
}

/// An up link, as listed by [`Network::links`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct LinkInfo {
    /// The link.
    pub link: LinkId,
    /// The radio peer at the other end.
    pub peer: PeerKey,
    /// Which side opened it.
    pub role: LinkRole,
    /// What the link has seen so far.
    pub metrics: LinkMetrics,
}

/// The beacon fields the layer above supplies.
#[derive(Debug, Clone, Copy)]
struct LocalBeacon {
    hint: [u8; 8],
    digest: [u8; 4],
    flags: BeaconFlags,
}

#[derive(Debug)]
struct Entry {
    machine: Link,
    peer: PeerKey,
    role: LinkRole,
    up: bool,
}

/// The link layer of one node.
///
/// Until both [`set_local_hello`](Self::set_local_hello) and
/// [`set_beacon`](Self::set_beacon) are called the node forms no links:
/// it needs a `Hello` to handshake and a beacon to be found. It connects to
/// a discovered peer when under `max_links`, when the peer's beacon says it
/// is accepting, and when its own beacon hint sorts below the peer's, so
/// that exactly one side of each pair dials. Inbound links beyond
/// `max_links`, or from a peer already linked, are refused.
///
/// # Example
///
/// ```
/// # #[cfg(feature = "sim")] {
/// use std::task::{Context, Waker};
/// use std::time::Duration;
///
/// use ao_network::{
///     AppHello, BeaconFlags, LocalTag, Network, NetworkConfig, NetworkEvent, SimConfig,
///     SimNode, SimWorld,
/// };
///
/// let world = SimWorld::new(SimConfig::default());
/// let accepting = BeaconFlags { accepting: true, ..BeaconFlags::default() };
/// let mut nodes: Vec<Network<_>> = (0..2u8)
///     .map(|i| {
///         let radio = world.add_node(SimNode::default()).unwrap();
///         let mut net = Network::new(NetworkConfig::default(), radio);
///         net.set_local_hello(LocalTag::new([i; 16]), AppHello::new(Vec::new()).unwrap());
///         net.set_beacon([i; 8], [0; 4], accepting);
///         net
///     })
///     .collect();
/// world.set_in_range(nodes[0].transport().node(), nodes[1].transport().node(), true);
///
/// let mut cx = Context::from_waker(Waker::noop());
/// let mut ups = 0;
/// for _ in 0..100 {
///     world.advance(Duration::from_millis(10));
///     for net in &mut nodes {
///         while let std::task::Poll::Ready(event) = net.poll_event(&mut cx, world.now()) {
///             ups += usize::from(matches!(event, NetworkEvent::LinkUp { .. }));
///         }
///     }
/// }
/// assert_eq!(ups, 2);
/// # }
/// ```
#[derive(Debug)]
pub struct Network<T> {
    config: NetworkConfig,
    transport: T,
    hello: Option<Hello>,
    beacon: Option<LocalBeacon>,
    /// Whether the transport is scanning, to skip repeats.
    scanning: bool,
    /// The beacon last handed to the transport, to skip repeats.
    advertised: Option<[u8; 16]>,
    links: BTreeMap<LinkId, Entry>,
    /// Every link, in any state, by peer.
    peers: BTreeMap<PeerKey, LinkId>,
    events: VecDeque<NetworkEvent>,
}

impl<T: Transport> Network<T> {
    /// A node on `transport`, which it starts.
    pub fn new(config: NetworkConfig, mut transport: T) -> Self {
        transport.start();
        Self {
            config,
            transport,
            hello: None,
            beacon: None,
            scanning: false,
            advertised: None,
            links: BTreeMap::new(),
            peers: BTreeMap::new(),
            events: VecDeque::new(),
        }
    }

    /// The transport, for inspection.
    pub fn transport(&self) -> &T {
        &self.transport
    }

    /// Sets the `Hello` new links introduce themselves with: `tag` for
    /// duplicate-link tie-breaking and `app_hello` for the layer above.
    /// Links already open keep the `Hello` they sent.
    pub fn set_local_hello(&mut self, tag: LocalTag, app_hello: AppHello) {
        self.hello = Some(Hello {
            proto_min: FRAME_VERSION,
            proto_max: FRAME_VERSION,
            caps: 0,
            // At most 1 MiB, validated by the config.
            max_frame: u32::try_from(self.config.max_frame()).unwrap_or(u32::MAX),
            tag,
            app_hello,
        });
        self.apply_profiles();
    }

    /// Sets the beacon fields the layer above owns. The node fills in its
    /// degree and `max_links`, and advertises `accepting` only while it has
    /// room for another link.
    pub fn set_beacon(&mut self, hint: [u8; 8], digest: [u8; 4], flags: BeaconFlags) {
        self.beacon = Some(LocalBeacon {
            hint,
            digest,
            flags,
        });
        self.apply_profiles();
    }

    /// Queues `payload` on `channel` of an up `link`, as of `now`.
    ///
    /// `now` is the same monotonic network time passed to
    /// [`poll_event`](Self::poll_event): it prunes expired frames before
    /// admission and decides what the link may still write.
    ///
    /// # Errors
    ///
    /// [`SendError::LinkClosed`] if `link` is not up, otherwise as
    /// [`Link::send`]. The payload always comes back unchanged.
    pub fn send(
        &mut self,
        now: Duration,
        link: LinkId,
        channel: Channel,
        payload: Vec<u8>,
    ) -> Result<(), SendError> {
        self.send_opts(now, link, channel, payload, SendOpts::default())
    }

    /// [`send`](Self::send) with [`SendOpts`]: a coalesce key (latest-wins
    /// replacement of a waiting frame) or a deadline after which the
    /// payload is dropped without being written.
    ///
    /// # Errors
    ///
    /// As [`send`](Self::send).
    pub fn send_opts(
        &mut self,
        now: Duration,
        link: LinkId,
        channel: Channel,
        payload: Vec<u8>,
        opts: SendOpts,
    ) -> Result<(), SendError> {
        let Some(entry) = self.links.get_mut(&link).filter(|e| e.up) else {
            return Err(SendError::LinkClosed(payload));
        };
        let actions = entry.machine.send_opts(now, channel, payload, opts)?;
        self.apply(link, actions);
        Ok(())
    }

    /// Sends a copy of `payload` on `channel` of every up link, as of
    /// `now`. Returns the links that refused it, with why.
    pub fn broadcast(
        &mut self,
        now: Duration,
        channel: Channel,
        payload: &[u8],
    ) -> Vec<(LinkId, SendError)> {
        let up: Vec<LinkId> = self.links().iter().map(|info| info.link).collect();
        up.into_iter()
            .filter_map(|link| {
                self.send(now, link, channel, payload.to_vec())
                    .err()
                    .map(|err| (link, err))
            })
            .collect()
    }

    /// Closes `link` for `reason`, or cancels it if it is still connecting.
    ///
    /// Connected links send `Bye` before disconnecting. An up link produces
    /// one [`NetworkEvent::LinkDown`] with the local reason when the transport
    /// confirms closure; its peer sees [`DisconnectReason::Bye`]. Links that
    /// never came up produce no event. Unknown or already-ended links are
    /// ignored, and repeating a close keeps its original reason.
    ///
    /// Sends are refused once closing begins. Already queued payloads are
    /// discarded with the link; an in-flight frame finishes before `Bye`.
    pub fn disconnect(&mut self, link: LinkId, reason: DisconnectReason) {
        if let Some(entry) = self.links.get_mut(&link) {
            let actions = entry.machine.close(reason);
            self.apply(link, actions);
        }
    }

    /// The up links, in `LinkId` order.
    pub fn links(&self) -> Vec<LinkInfo> {
        self.links
            .iter()
            .filter(|(_, entry)| entry.up)
            .map(|(&link, entry)| LinkInfo {
                link,
                peer: entry.peer,
                role: entry.role,
                metrics: entry.machine.metrics(),
            })
            .collect()
    }

    /// The earliest time a timer is due; poll again by then.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.links
            .values()
            .filter_map(|entry| entry.machine.next_deadline())
            .min()
    }

    /// Runs the node at `now` and returns its next event.
    ///
    /// Drains the transport through the link state machines, then runs
    /// the timers due at `now`. `Pending` registers `cx` with the
    /// transport; timers wake nobody, so the caller also polls at
    /// [`next_deadline`](Self::next_deadline).
    pub fn poll_event(&mut self, cx: &mut Context<'_>, now: Duration) -> Poll<NetworkEvent> {
        loop {
            if let Some(event) = self.events.pop_front() {
                return Poll::Ready(event);
            }
            match self.transport.poll_event(cx) {
                Poll::Ready(event) => self.on_transport(now, event),
                Poll::Pending => break,
            }
        }
        self.tick(now);
        self.events.pop_front().map_or(Poll::Pending, Poll::Ready)
    }

    /// [`poll_event`](Self::poll_event) as a future, at a fixed `now`.
    pub fn next_event(&mut self, now: Duration) -> impl Future<Output = NetworkEvent> + '_ {
        poll_fn(move |cx| self.poll_event(cx, now))
    }

    fn on_transport(&mut self, now: Duration, event: TransportEvent) {
        let link = match &event {
            TransportEvent::RadioState(RadioState::Ready) => {
                // A restarted radio has forgotten its profiles.
                self.scanning = false;
                self.advertised = None;
                self.apply_profiles();
                return;
            }
            TransportEvent::Discovered { peer, beacon, .. } => {
                self.maybe_connect(*peer, beacon);
                return;
            }
            TransportEvent::Connected {
                link,
                peer,
                role: LinkRole::Responder,
                ..
            } => {
                if !self.accept(*link, *peer) {
                    self.transport.disconnect(*link);
                    return;
                }
                *link
            }
            TransportEvent::Connected { link, .. }
            | TransportEvent::ConnectFailed { link, .. }
            | TransportEvent::Rssi { link, .. }
            | TransportEvent::MtuChanged { link, .. }
            | TransportEvent::Received { link, .. }
            | TransportEvent::WriteReady { link }
            | TransportEvent::Disconnected { link, .. } => *link,
            TransportEvent::RadioState(_) | TransportEvent::Lost { .. } => return,
        };
        if let Some(entry) = self.links.get_mut(&link) {
            let actions = entry.machine.on_event(now, event);
            self.apply(link, actions);
        }
    }

    fn maybe_connect(&mut self, peer: PeerKey, beacon: &[u8]) {
        let (Some(hello), Some(own)) = (&self.hello, &self.beacon) else {
            return;
        };
        let Ok(theirs) = Beacon::decode(beacon) else {
            return;
        };
        if !theirs.flags.accepting
            || own.hint >= theirs.hint
            || !self.has_room()
            || self.peers.contains_key(&peer)
        {
            return;
        }
        let machine = Link::new(&self.config, hello.clone());
        let link = self.transport.connect(peer);
        self.add(link, peer, LinkRole::Initiator, machine);
    }

    /// Admits an inbound link, if there is room and a `Hello` to send.
    fn accept(&mut self, link: LinkId, peer: PeerKey) -> bool {
        let Some(hello) = &self.hello else {
            return false;
        };
        if !self.has_room() || self.peers.contains_key(&peer) {
            return false;
        }
        let machine = Link::new(&self.config, hello.clone());
        self.add(link, peer, LinkRole::Responder, machine);
        true
    }

    fn has_room(&self) -> bool {
        self.links.len() < self.max_links()
    }

    fn max_links(&self) -> usize {
        usize::try_from(self.config.max_links()).unwrap_or(usize::MAX)
    }

    fn add(&mut self, link: LinkId, peer: PeerKey, role: LinkRole, machine: Link) {
        self.links.insert(
            link,
            Entry {
                machine,
                peer,
                role,
                up: false,
            },
        );
        self.peers.insert(peer, link);
        self.apply_profiles();
    }

    fn tick(&mut self, now: Duration) {
        let due: Vec<LinkId> = self
            .links
            .iter()
            .filter(|(_, entry)| entry.machine.next_deadline().is_some_and(|d| d <= now))
            .map(|(&link, _)| link)
            .collect();
        for link in due {
            if let Some(entry) = self.links.get_mut(&link) {
                let actions = entry.machine.on_tick(now);
                self.apply(link, actions);
            }
        }
    }

    fn apply(&mut self, link: LinkId, actions: Vec<LinkAction>) {
        for action in actions {
            match action {
                LinkAction::Write(chunk) => self.transport.write(link, chunk),
                LinkAction::Disconnect => self.transport.disconnect(link),
                LinkAction::Ready { remote, .. } => {
                    if let Some(entry) = self.links.get_mut(&link) {
                        entry.up = true;
                    }
                    self.events.push_back(NetworkEvent::LinkUp {
                        link,
                        remote_hello: remote,
                    });
                }
                LinkAction::Data { channel, payload } => {
                    self.events.push_back(NetworkEvent::Data {
                        link,
                        channel,
                        payload,
                    });
                }
                LinkAction::Down(end) => self.remove(link, end),
            }
        }
    }

    fn remove(&mut self, link: LinkId, end: LinkEnd) {
        let Some(entry) = self.links.remove(&link) else {
            return;
        };
        if self.peers.get(&entry.peer) == Some(&link) {
            self.peers.remove(&entry.peer);
        }
        if let (true, LinkEnd::Disconnected(reason)) = (entry.up, end) {
            self.events
                .push_back(NetworkEvent::LinkDown { link, reason });
        }
        self.apply_profiles();
    }

    /// Scans once a `Hello` is set; advertises once a beacon is set too.
    fn apply_profiles(&mut self) {
        if self.hello.is_none() {
            return;
        }
        let mode = PowerMode::Balanced;
        if !self.scanning {
            self.scanning = true;
            self.transport.set_scanning(Some(ScanProfile { mode }));
        }
        let Some(own) = self.beacon else {
            return;
        };
        if !self.transport.caps().can_advertise {
            return;
        }
        let beacon = Beacon {
            flags: BeaconFlags {
                accepting: own.flags.accepting && self.has_room(),
                ..own.flags
            },
            degree: u8::try_from(self.links.len()).unwrap_or(u8::MAX),
            // At most 32, validated by the config.
            max_links: u8::try_from(self.config.max_links()).unwrap_or(u8::MAX),
            hint: own.hint,
            digest: own.digest,
        }
        .encode();
        if self.advertised != Some(beacon) {
            self.advertised = Some(beacon);
            self.transport
                .set_advertising(Some(AdvProfile { beacon, mode }));
        }
    }
}
