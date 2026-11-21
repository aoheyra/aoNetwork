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
//! | [`release`](Network::release) | as `disconnect` with `Closed`, for an opportunistic link only |
//! | [`links`](Network::links) | none; lists the up links |
//! | [`set_idle`](Network::set_idle) | none; switches discovery to [`DiscoveryMode::Quiet`] from the next poll |
//! | [`set_link_hint`](Network::set_link_hint) / [`clear_link_hint`](Network::clear_link_hint) | none; changes the order peers are dialed in, never the degree cap |
//!
//! `LinkUp` comes once per link, after a successful handshake, and
//! `LinkDown` once after it, when that link ends. Links that never come up
//! produce no events, nor does a link closed at its handshake as a
//! duplicate.
//!
//! # Observability
//!
//! Each link has a `tracing` span, `link{id peer role class}`, under which
//! its opening, coming up, closing and ending are logged; strikes,
//! blacklisting and discovery mode changes are logged too. Nothing is
//! logged unless the app installs a subscriber.
//! [`snapshot`](Network::snapshot) returns the node's state and its
//! cumulative [`Counters`]. Each [`LinkInfo`] and [`LinkSnapshot`] carries
//! `up_at`, the injected time its handshake finished, so a simulator run can
//! compare time-to-link with and without advice from the layer above.

use std::collections::{BTreeMap, VecDeque};
use std::future::{Future, poll_fn};
use std::task::{Context, Poll};
use std::time::Duration;

use crate::discovery::{DiscoveryController, Inputs};
use crate::guard::AbuseGuard;
use crate::observe::{Counters, Hex, LinkSnapshot, Snapshot};
use crate::topology::{
    ClassCounts, Cooldowns, HintTable, LinkClass, PeerTable, ReadyLink, TopologyPolicy,
    duplicate_loser,
};
use crate::{
    AdvProfile, AppHello, Beacon, BeaconFlags, CAP_CREDIT, Channel, ConnectError, DisconnectReason,
    DiscoveryMode, FRAME_VERSION, HandshakeFailure, Hello, Link, LinkAction, LinkEnd,
    LinkHintPreference, LinkId, LinkMetrics, LinkRole, LinkState, LocalTag, NetworkConfig, PeerKey,
    RadioState, ScanProfile, SendError, SendOpts, Transport, TransportEvent,
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
    /// The injected time its handshake finished, for time-to-link
    /// measurements.
    pub up_at: Duration,
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
    /// Which slot admitted it, so admission and eviction count the
    /// classes separately.
    class: LinkClass,
    /// When it was admitted: eviction spares links younger than the
    /// policy's minimum age.
    opened_at: Duration,
    /// The tag this node sent on it.
    local_tag: LocalTag,
    /// The tag the peer sent, once the link is up.
    remote_tag: Option<LocalTag>,
    up: bool,
    /// The injected time the handshake finished, while not yet ended.
    up_at: Duration,
    /// Everything logged about this link is logged under this span.
    span: tracing::Span,
}

/// The link layer of one node.
///
/// Until both [`set_local_hello`](Self::set_local_hello) and
/// [`set_beacon`](Self::set_beacon) are called the node forms no links:
/// it needs a `Hello` to handshake and a beacon to be found. It connects to
/// a discovered peer when the topology policy has a slot free and the
/// peer's beacon says it is accepting. Of each pair, the side whose beacon
/// hint sorts lower dials at once and the other waits 4 s first, so one
/// connection normally lands before the second is tried. The peer
/// table gates the rest: a peer must not already be linked, must be past
/// the backoff of its last failed attempt, must be at or above
/// `min_rssi`, and must have been seen within the last 30 s. Inbound
/// links are admitted against the same slots and the same floor, so
/// links of every class together stay within `max_links`; beyond that,
/// below the floor, or from a peer already linked, they are refused.
/// Once over its stable target the node sheds its weakest degraded link
/// and keeps strong ones, and three rejected outbound attempts in a
/// row lower the session's cap.
///
/// A peer can still end up with two links, when both sides dial at once
/// or one radio shows up under two addresses. When a handshake brings up
/// a second link to the same remote tag, the link opened by the lower tag
/// is kept and the other closed with [`DisconnectReason::Duplicate`]. A
/// link whose remote tag is this node's own is closed the same way.
///
/// Every peer is treated as hostile. A peer that sends faster than the
/// configured frame or byte limit is disconnected with
/// [`DisconnectReason::AbuseDetected`] and blacklisted at once. A link
/// closed for a malformed frame, a broken chunk stream or an oversize
/// `Hello` is a strike against its peer, and `max_strikes` strikes
/// blacklist it too. A blacklisted peer is never dialed, its inbound
/// links are refused, and any link still open to it is closed, until
/// `blacklist_duration` has passed.
///
/// How hard it scans and advertises follows its [`DiscoveryMode`]:
/// aggressive while it holds fewer up links than its stable target,
/// normal at the target, and quiet while the beacon's `low_battery` flag
/// is set or [`set_idle`](Self::set_idle) says so. A mode change is
/// requested from the radio no sooner than `discovery_interval` after
/// the last request, and quiet scanning runs in windows of one interval
/// with pauses of five, so the radio never restarts its scanner faster
/// than Android allows. Beacon content changes are not held back.
///
/// Once at its stable target, the node also opens a short opportunistic
/// link to a peer whose beacon digest is new to it, such as a node
/// passing by, so the layer above can sync with it. The link holds the
/// single opportunistic slot until [`release`](Self::release) or a 60 s
/// dwell, and the peer's hint is then barred from new links for 60 s
/// unless its digest changes again.
///
/// The layer above can advise which peers are worth linking through
/// [`set_link_hint`](Self::set_link_hint). Such a hint only orders the
/// candidates this node dials and may skip the initiation delay; `aoMesh`
/// never chooses a link or enforces a cap. `max_links`, slots and floors
/// stay under this node's control, whatever advice it is given.
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
    /// The scan profile last handed to the transport, to skip repeats.
    scan: Option<Option<ScanProfile>>,
    /// The advertisement last handed to the transport, to skip repeats.
    advertised: Option<AdvProfile>,
    links: BTreeMap<LinkId, Entry>,
    /// Every link, in any state, by peer.
    peers: BTreeMap<PeerKey, LinkId>,
    /// Peers seen in beacons, with their backoff and eviction.
    table: PeerTable,
    /// The slots, scores and eviction rules links are admitted under.
    policy: TopologyPolicy,
    /// Advisory preferences from the layer above; never a slot or a cap.
    hints: HintTable,
    /// Strikes and the blacklist.
    guard: AbuseGuard,
    /// The discovery mode and the profiles it asks for.
    discovery: DiscoveryController,
    /// The caller says the node is idle.
    idle: bool,
    /// Hints barred after their opportunistic link ended.
    cooldowns: Cooldowns,
    /// What the node has done so far; queue drops cover ended links only,
    /// live ones are added by `snapshot`.
    counters: Counters,
    events: VecDeque<NetworkEvent>,
}

impl<T: Transport> Network<T> {
    /// A node on `transport`, which it starts.
    pub fn new(config: NetworkConfig, mut transport: T) -> Self {
        transport.start();
        let table = PeerTable::new(&config);
        let policy = TopologyPolicy::new(&config);
        let hints = HintTable::new(&config);
        let guard = AbuseGuard::new(&config);
        let inputs = Inputs {
            degree: 0,
            target: discovery_target(&policy),
            low_battery: false,
            idle: false,
        };
        let discovery = DiscoveryController::new(&config, &inputs);
        let cooldowns = Cooldowns::new(&config);
        Self {
            config,
            transport,
            hello: None,
            beacon: None,
            scan: None,
            advertised: None,
            links: BTreeMap::new(),
            peers: BTreeMap::new(),
            table,
            policy,
            hints,
            guard,
            discovery,
            idle: false,
            cooldowns,
            counters: Counters::default(),
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
            caps: if self.config.flow_control() {
                CAP_CREDIT
            } else {
                0
            },
            // At most 1 MiB, validated by the config.
            max_frame: u32::try_from(self.config.max_frame()).unwrap_or(u32::MAX),
            tag,
            app_hello,
        });
        self.apply_profiles(None);
    }

    /// Sets the beacon fields the layer above owns. The node fills in its
    /// degree and `max_links`, and advertises `accepting` only while it has
    /// room for another link. `flags.low_battery` also puts discovery in
    /// [`DiscoveryMode::Quiet`] from the next poll.
    pub fn set_beacon(&mut self, hint: [u8; 8], digest: [u8; 4], flags: BeaconFlags) {
        self.beacon = Some(LocalBeacon {
            hint,
            digest,
            flags,
        });
        self.apply_profiles(None);
    }

    /// Advises this node's link policy about `peer`.
    ///
    /// `peer` is the opaque radio key reported by [`LinkInfo::peer`] and
    /// [`NetworkEvent::LinkUp`]; `aoNetwork` never interprets it and does not
    /// know a mesh identity from it. [`LinkHintPreference::Prefer`] puts the
    /// peer ahead of unhinted candidates and dials it without the initiation
    /// delay, so a contact in range is linked sooner.
    /// [`LinkHintPreference::Deprioritize`] puts it behind them, and
    /// [`LinkHintPreference::Neutral`] removes any advice.
    ///
    /// This is advisory input from `aoMesh`, not a command. The caller says
    /// which neighbours it would benefit from; `aoNetwork` alone decides
    /// whether a link is formed. A hint never reserves a slot, changes the
    /// admission floor, evicts a link or raises `max_links`.
    ///
    /// At most `max_peers` hints are held (64 by default). A new hint for a
    /// full table displaces the lowest preference, then the greatest
    /// `PeerKey` on a tie, so the result is deterministic. A hint lasts
    /// until it is cleared, set to `Neutral`, or displaced; it survives the
    /// peer going out of range. There is no time-based expiry: the core
    /// reads no clock and takes `now` only on the operations that need it.
    /// Advice takes effect at the next dial or candidate selection; links
    /// already up are left alone.
    pub fn set_link_hint(&mut self, peer: PeerKey, preference: LinkHintPreference) {
        self.hints.set(peer, preference);
    }

    /// Removes the advice for `peer`, returning whether any was held.
    ///
    /// The peer then competes under the ordinary topology rules, exactly as
    /// if it had never been hinted.
    pub fn clear_link_hint(&mut self, peer: PeerKey) -> bool {
        self.hints.clear(peer)
    }

    /// The advice held for `peer`, or [`LinkHintPreference::Neutral`].
    pub fn link_hint(&self, peer: PeerKey) -> LinkHintPreference {
        self.hints.get(&peer)
    }

    /// All advice held, in [`PeerKey`] order, for diagnostics.
    pub fn link_hints(&self) -> Vec<(PeerKey, LinkHintPreference)> {
        self.hints.entries().collect()
    }

    /// Says whether the node is idle, as the app judges it (in the
    /// background, say). While idle, discovery is
    /// [`DiscoveryMode::Quiet`] from the next poll.
    pub fn set_idle(&mut self, idle: bool) {
        self.idle = idle;
    }

    /// The discovery mode in force.
    pub fn discovery_mode(&self) -> DiscoveryMode {
        self.discovery.mode()
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
        match entry.machine.send_opts(now, channel, payload, opts) {
            Ok(actions) => {
                self.apply(now, link, actions);
                Ok(())
            }
            Err(err) => {
                if matches!(err, SendError::QueueFull(_)) {
                    self.counters.queue_full = self.counters.queue_full.saturating_add(1);
                }
                Err(err)
            }
        }
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
    ///
    /// `now` is the current monotonic time: a link closed while it is
    /// still connecting ends the attempt at once, and the peer table
    /// starts that peer's backoff from it.
    pub fn disconnect(&mut self, now: Duration, link: LinkId, reason: DisconnectReason) {
        if let Some(entry) = self.links.get_mut(&link) {
            let actions = entry.machine.close(reason);
            self.apply(now, link, actions);
        }
    }

    /// Ends an opportunistic `link` the layer above has finished with,
    /// as [`disconnect`](Self::disconnect) with
    /// [`DisconnectReason::Closed`]. Its peer's hint then cools down.
    /// Links of any other class, and unknown links, are left alone.
    pub fn release(&mut self, now: Duration, link: LinkId) {
        if self
            .links
            .get(&link)
            .is_some_and(|entry| entry.class == LinkClass::Opportunistic)
        {
            self.disconnect(now, link, DisconnectReason::Closed);
        }
    }

    /// The up links, in `LinkId` order, with the injected time each came up.
    pub fn links(&self) -> Vec<LinkInfo> {
        self.links
            .iter()
            .filter(|(_, entry)| entry.up)
            .map(|(&link, entry)| LinkInfo {
                link,
                peer: entry.peer,
                role: entry.role,
                up_at: entry.up_at,
                metrics: entry.machine.metrics(),
            })
            .collect()
    }

    /// The node as it stands at `now`, for a debug screen or a
    /// diagnostics bundle: its discovery mode, every link in any state,
    /// the size of its peer table and its hint table, how many peers are
    /// blacklisted, and its cumulative [`Counters`]. With the `serde`
    /// feature it serializes.
    ///
    /// # Example
    ///
    /// ```
    /// # #[cfg(feature = "sim")] {
    /// use std::time::Duration;
    /// use ao_network::{Network, NetworkConfig, SimConfig, SimNode, SimWorld};
    ///
    /// let world = SimWorld::new(SimConfig::default());
    /// let radio = world.add_node(SimNode::default()).unwrap();
    /// let net = Network::new(NetworkConfig::default(), radio);
    /// let snapshot = net.snapshot(Duration::ZERO);
    /// assert!(snapshot.links.is_empty());
    /// assert_eq!(snapshot.hints, 0);
    /// assert_eq!(snapshot.counters.links_up, 0);
    /// # }
    /// ```
    pub fn snapshot(&self, now: Duration) -> Snapshot {
        let mut counters = self.counters;
        let links = self
            .links
            .iter()
            .map(|(&link, entry)| {
                let metrics = entry.machine.metrics();
                counters.queue_drops.add(metrics.queue_drops);
                LinkSnapshot {
                    link,
                    peer: entry.peer,
                    role: entry.role,
                    class: entry.class,
                    state: entry.machine.state(),
                    up_at: entry.up_at,
                    metrics,
                    queued_bytes: entry.machine.queued_bytes(),
                }
            })
            .collect();
        Snapshot {
            at: now,
            mode: self.discovery.mode(),
            links,
            peers: self.table.tracked(),
            hints: self.hints.tracked(),
            blocked: self.guard.blocked(now),
            counters,
        }
    }

    /// The earliest time a timer is due; poll again by then.
    pub fn next_deadline(&self) -> Option<Duration> {
        self.links
            .values()
            .filter_map(|entry| entry.machine.next_deadline())
            .chain(self.discovery.next_deadline(&self.discovery_inputs()))
            .chain(
                self.links
                    .values()
                    .filter_map(|entry| self.dwell_end(entry)),
            )
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
                self.scan = None;
                self.advertised = None;
                self.apply_profiles(Some(now));
                return;
            }
            TransportEvent::Discovered { peer, rssi, beacon } => {
                let fresh = self.table.observe(now, *peer, *rssi, beacon);
                // Retried on every sighting while the digest stays new, so a
                // dial held back by the delay, the cooldown or a busy slot
                // is not lost.
                if fresh && self.table.unsynced(peer) {
                    self.connect_opportunistic(now, *peer);
                }
                if fresh {
                    self.connect_to(now, *peer);
                }
                // A sighting may have moved a held link's peer below
                // the floor, which is when it becomes evictable.
                self.enforce_eviction(now);
                return;
            }
            TransportEvent::Connected {
                link,
                peer,
                role: LinkRole::Responder,
                ..
            } => {
                if !self.accept(now, *link, *peer) {
                    tracing::debug!(peer = %Hex(peer.as_bytes()), "inbound link refused");
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
            self.apply(now, link, actions);
        }
    }

    /// Starts an outbound connection to `peer` when the peer table and
    /// the local policy allow one now: the peer must be a candidate
    /// (not linked, past its backoff, accepting, strong, recently seen),
    /// and unless our hint sorts below the peer's, the initiation delay
    /// must have passed. The link is admitted into the class the policy
    /// grants from the current inventory: stable first, then explore.
    ///
    /// A held-back dial is retried on the peer's next sighting, as a dial
    /// held by backoff is.
    fn connect_to(&mut self, now: Duration, peer: PeerKey) {
        let (Some(hello), Some(own)) = (&self.hello, &self.beacon) else {
            return;
        };
        let Some(class) = self.policy.admit(&self.counts()) else {
            return;
        };
        if !self.table.eligible(&peer, now, &self.peers)
            || self.guard.is_blocked(&peer, now)
            || self.cooling(&peer, now)
        {
            return;
        }
        let Some(hint) = self.table.hint_of(&peer) else {
            return;
        };
        // Advice to prefer a peer lets us dial at once, as if its beacon
        // hint sorted lower. The peer's beacon, backoff, slots and the
        // degree cap still decide whether the dial happens at all.
        if !self.hints.preferred(&peer)
            && own.hint >= hint
            && !self.table.waited(&peer, now, self.policy.initiation_delay)
        {
            return;
        }
        let hello = hello.clone();
        let link = self.transport.connect(peer);
        self.add(now, link, peer, LinkRole::Initiator, class, hello);
    }

    /// Starts an opportunistic link to `peer`, whose beacon digest is new
    /// to this node, if the policy has the opportunistic slot free and the
    /// peer is a candidate that is neither blacklisted nor cooling down.
    /// The same hint rule and initiation delay as
    /// [`connect_to`](Self::connect_to) decide which side dials first.
    fn connect_opportunistic(&mut self, now: Duration, peer: PeerKey) {
        let (Some(hello), Some(own)) = (&self.hello, &self.beacon) else {
            return;
        };
        if !self.policy.admit_opportunistic(&self.counts())
            || !self.table.eligible(&peer, now, &self.peers)
            || self.guard.is_blocked(&peer, now)
            || self.cooling(&peer, now)
        {
            return;
        }
        let Some(hint) = self.table.hint_of(&peer) else {
            return;
        };
        if !self.hints.preferred(&peer)
            && own.hint >= hint
            && !self.table.waited(&peer, now, self.policy.initiation_delay)
        {
            return;
        }
        let hello = hello.clone();
        let link = self.transport.connect(peer);
        self.add(
            now,
            link,
            peer,
            LinkRole::Initiator,
            LinkClass::Opportunistic,
            hello,
        );
    }

    /// Whether `peer`'s hint is barred at `now` by the cooldown that
    /// followed its last opportunistic link.
    fn cooling(&self, peer: &PeerKey, now: Duration) -> bool {
        self.table
            .beacon_of(peer)
            .is_some_and(|beacon| self.cooldowns.bars(now, &beacon))
    }

    /// When an opportunistic link that is still open must end.
    fn dwell_end(&self, entry: &Entry) -> Option<Duration> {
        let open = !matches!(
            entry.machine.state(),
            LinkState::Closing | LinkState::Closed
        );
        (entry.class == LinkClass::Opportunistic && open)
            .then(|| entry.opened_at.saturating_add(self.policy.dwell))
    }

    /// Admits an inbound link, if a `Hello` to send and a slot of some
    /// class are free. The policy decides, not the caller: an inbound
    /// link consumes the same stable or explore slot an outbound one
    /// would, and a peer below the policy's admission floor is refused the same way
    /// a candidate would be, so an evicted weak peer cannot walk
    /// straight back in while it is still weak. A peer that has never
    /// been seen cannot be judged and is admitted.
    ///
    /// A peer this node is itself still dialing is admitted too: both
    /// connections run their handshakes and the duplicate tie-break
    /// keeps one. Refusing would leave the two ends each refusing the
    /// other's connection.
    ///
    /// A blacklisted peer is refused.
    ///
    /// With no stable or explore slot left, the link may take the free
    /// opportunistic slot. A peer whose hint is cooling down is refused.
    fn accept(&mut self, now: Duration, link: LinkId, peer: PeerKey) -> bool {
        let Some(hello) = &self.hello else {
            return false;
        };
        let counts = self.counts();
        let class = match self.policy.admit(&counts) {
            Some(class) => class,
            None if self.policy.admit_opportunistic(&counts) => LinkClass::Opportunistic,
            None => return false,
        };
        let held = self.peers.get(&peer).and_then(|held| self.links.get(held));
        if held.is_some_and(|held| held.up || held.role == LinkRole::Responder)
            || self.guard.is_blocked(&peer, now)
            || self.cooling(&peer, now)
        {
            return false;
        }
        if self
            .table
            .rssi_of(&peer)
            .is_some_and(|rssi| rssi < self.policy.admit_floor)
        {
            return false;
        }
        let hello = hello.clone();
        self.add(now, link, peer, LinkRole::Responder, class, hello);
        true
    }

    /// The current inventory, for admission and eviction decisions.
    fn counts(&self) -> ClassCounts {
        let mut counts = ClassCounts {
            total: self.links.len(),
            stable: 0,
            explore: 0,
            opportunistic: 0,
        };
        for entry in self.links.values() {
            match entry.class {
                LinkClass::Stable => counts.stable += 1,
                LinkClass::Explore => counts.explore += 1,
                LinkClass::Opportunistic => counts.opportunistic += 1,
            }
        }
        counts
    }

    /// Whether any class would admit one more link right now.
    fn has_room(&self) -> bool {
        let counts = self.counts();
        self.policy.admit(&counts).is_some() || self.policy.admit_opportunistic(&counts)
    }

    /// Tracks a new link that introduces itself with `hello`.
    fn add(
        &mut self,
        now: Duration,
        link: LinkId,
        peer: PeerKey,
        role: LinkRole,
        class: LinkClass,
        hello: Hello,
    ) {
        let local_tag = hello.tag;
        let span = tracing::info_span!(
            "link",
            id = link.raw(),
            peer = %Hex(peer.as_bytes()),
            role = ?role,
            class = ?class,
        );
        tracing::debug!(parent: &span, "opened");
        self.links.insert(
            link,
            Entry {
                machine: Link::new(&self.config, hello),
                peer,
                role,
                class,
                opened_at: now,
                local_tag,
                remote_tag: None,
                up: false,
                up_at: Duration::ZERO,
                span,
            },
        );
        self.peers.insert(peer, link);
        self.table.mark_connecting(&peer);
        self.apply_profiles(Some(now));
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
                self.apply(now, link, actions);
            }
        }
        let dwelt: Vec<LinkId> = self
            .links
            .iter()
            .filter(|(_, entry)| self.dwell_end(entry).is_some_and(|end| end <= now))
            .map(|(&link, _)| link)
            .collect();
        for link in dwelt {
            if let Some(entry) = self.links.get(&link) {
                tracing::info!(parent: &entry.span, "dwell over, closing");
            }
            self.disconnect(now, link, DisconnectReason::DegreeCap);
        }
        let inputs = self.discovery_inputs();
        let mode = self.discovery.mode();
        if self.discovery.poll(now, &inputs) {
            if self.discovery.mode() != mode {
                tracing::info!(mode = ?self.discovery.mode(), "discovery mode changed");
            }
            self.apply_profiles(Some(now));
        }
    }

    /// What the discovery mode is decided from, as things stand.
    fn discovery_inputs(&self) -> Inputs {
        Inputs {
            degree: self.links.values().filter(|entry| entry.up).count(),
            target: discovery_target(&self.policy),
            low_battery: self.beacon.is_some_and(|own| own.flags.low_battery),
            idle: self.idle,
        }
    }

    fn apply(&mut self, now: Duration, link: LinkId, actions: Vec<LinkAction>) {
        for action in actions {
            match action {
                LinkAction::Write(chunk) => self.transport.write(link, chunk),
                LinkAction::Disconnect => self.transport.disconnect(link),
                LinkAction::Ready { remote, version } => {
                    if let Some(loser) = self.duplicate_to_close(link, remote.tag) {
                        if let Some(entry) = self.links.get(&loser) {
                            tracing::info!(parent: &entry.span, "duplicate, closing");
                        }
                        self.disconnect(now, loser, DisconnectReason::Duplicate);
                        if loser == link {
                            // Never announced, so it ends without events.
                            continue;
                        }
                    }
                    if let Some(entry) = self.links.get_mut(&link) {
                        entry.up = true;
                        entry.up_at = now;
                        entry.remote_tag = Some(remote.tag);
                        self.table.mark_linked(&entry.peer);
                        tracing::info!(parent: &entry.span, version, "up");
                    }
                    self.counters.links_up = self.counters.links_up.saturating_add(1);
                    self.policy.note_success();
                    self.events.push_back(NetworkEvent::LinkUp {
                        link,
                        remote_hello: remote,
                    });
                    // A new link may have taken the node over its
                    // stable target.
                    self.enforce_eviction(now);
                }
                LinkAction::Data { channel, payload } => {
                    // A duplicate closed at its handshake was never up.
                    if !self.links.get(&link).is_some_and(|entry| entry.up) {
                        continue;
                    }
                    self.events.push_back(NetworkEvent::Data {
                        link,
                        channel,
                        payload,
                    });
                }
                LinkAction::Down(end) => self.remove(now, link, end),
            }
        }
    }

    fn remove(&mut self, now: Duration, link: LinkId, end: LinkEnd) {
        let Some(entry) = self.links.remove(&link) else {
            return;
        };
        match end {
            LinkEnd::Disconnected(reason) => {
                tracing::info!(parent: &entry.span, ?reason, "down");
                self.counters.links_down.count(reason);
            }
            LinkEnd::ConnectFailed(error) => {
                tracing::debug!(parent: &entry.span, ?error, "connect failed");
                self.counters.connects_failed = self.counters.connects_failed.saturating_add(1);
            }
        }
        self.counters
            .queue_drops
            .add(entry.machine.metrics().queue_drops);
        if entry.role == LinkRole::Initiator
            && matches!(end, LinkEnd::ConnectFailed(ConnectError::Rejected))
        {
            // Our own outbound attempt was turned away: count the
            // strike that lowers the session's effective cap.
            self.policy.note_rejection();
        }
        let sibling = self
            .links
            .iter()
            .find(|(_, other)| other.peer == entry.peer)
            .map(|(&other, _)| other);
        if let Some(sibling) = sibling {
            // Another link to the same radio carries on, and the peer's
            // table state and backoff stay with it.
            self.peers.insert(entry.peer, sibling);
        } else {
            self.table.mark_down(now, &entry.peer);
            if self.peers.get(&entry.peer) == Some(&link) {
                self.peers.remove(&entry.peer);
            }
        }
        if let (true, LinkEnd::Disconnected(reason)) = (entry.up, end) {
            self.events
                .push_back(NetworkEvent::LinkDown { link, reason });
        }
        if let LinkEnd::Disconnected(reason) = end {
            self.judge(now, entry.peer, reason);
        }
        // A duplicate closed beside a surviving link does not end the
        // peer's time here, so it starts no cooldown.
        let last = !self.peers.contains_key(&entry.peer);
        if entry.class == LinkClass::Opportunistic && entry.up && last {
            if let Some(beacon) = self.table.beacon_of(&entry.peer) {
                self.cooldowns.start(now, beacon.hint, beacon.digest);
            }
        }
        self.apply_profiles(Some(now));
        // A slot just freed: dial the next candidate now rather than
        // waiting for a sighting to say the neighbourhood changed.
        for peer in self
            .table
            .candidates_with_hints(now, &self.peers, &self.hints)
        {
            self.connect_to(now, peer);
        }
    }

    /// The link to close now that `link` has completed its handshake
    /// with a peer that sent `remote`: `link` itself when `remote` is
    /// this node's own tag, otherwise the tie-break's pick between it
    /// and a ready link already up to the same tag.
    fn duplicate_to_close(&self, link: LinkId, remote: LocalTag) -> Option<LinkId> {
        let entry = self.links.get(&link)?;
        if remote == entry.local_tag {
            return Some(link);
        }
        let ready = |link, entry: &Entry| ReadyLink {
            link,
            role: entry.role,
            local_tag: entry.local_tag,
            remote_tag: remote,
        };
        let (&held, held_entry) = self.links.iter().find(|(other, other_entry)| {
            **other != link
                && other_entry.remote_tag == Some(remote)
                && other_entry.machine.state() == LinkState::Ready
        })?;
        duplicate_loser(&ready(held, held_entry), &ready(link, entry))
    }

    /// Holds a link that ended for `reason` against its `peer`: a flood
    /// blacklists the peer at once, misbehaviour is a strike, anything
    /// else is not the peer's fault. A newly blacklisted peer loses every
    /// other link it holds here.
    fn judge(&mut self, now: Duration, peer: PeerKey, reason: DisconnectReason) {
        let was_blocked = self.guard.is_blocked(&peer, now);
        let blocked = match reason {
            DisconnectReason::AbuseDetected => {
                self.guard.block(now, peer);
                true
            }
            DisconnectReason::ProtocolViolation
            | DisconnectReason::HandshakeFailed(HandshakeFailure::OversizeHello) => {
                self.counters.strikes = self.counters.strikes.saturating_add(1);
                tracing::warn!(peer = %Hex(peer.as_bytes()), ?reason, "strike");
                self.guard.strike(now, peer)
            }
            _ => false,
        };
        if !blocked {
            return;
        }
        if !was_blocked {
            self.counters.blacklisted = self.counters.blacklisted.saturating_add(1);
            tracing::warn!(peer = %Hex(peer.as_bytes()), ?reason, "blacklisted");
        }
        let held: Vec<LinkId> = self
            .links
            .iter()
            .filter(|(_, entry)| entry.peer == peer)
            .map(|(&link, _)| link)
            .collect();
        for link in held {
            self.disconnect(now, link, DisconnectReason::AbuseDetected);
        }
    }

    /// Sheds the worst link while the node holds more healthy stable or
    /// explore links than its stable target (an opportunistic link is
    /// bounded by its dwell instead): only a link that is up and ready, has
    /// survived the policy's minimum age, and whose peer average has
    /// fallen below the admission floor may go, and the weakest score
    /// goes first. Links that are merely over the target but strong
    /// are kept - that is the hysteresis that stops a node from
    /// churning its neighbourhood. The evicted peer inherits its
    /// normal backoff through [`PeerTable::mark_down`], so it cannot
    /// be redialed the moment a slot frees.
    fn enforce_eviction(&mut self, now: Duration) {
        loop {
            let held = |entry: &&Entry| {
                entry.up
                    && entry.machine.state() == LinkState::Ready
                    && entry.class != LinkClass::Opportunistic
            };
            let ready = self.links.values().filter(held).count();
            if ready <= self.policy.target_stable {
                return;
            }
            let worst = self
                .links
                .iter()
                .filter(|(_, entry)| held(entry))
                .filter(|(_, entry)| {
                    now.saturating_sub(entry.opened_at) >= self.policy.link_min_age
                })
                .filter_map(|(&link, entry)| {
                    let rssi = self.table.rssi_of(&entry.peer)?;
                    if rssi >= self.policy.admit_floor {
                        return None;
                    }
                    // Without a beacon there is nothing to compare
                    // against; such a link is left to end on its own.
                    let score = self.table.score_of(&entry.peer)?;
                    Some((link, score))
                })
                .min_by_key(|&(link, score)| (score, link))
                .map(|(link, _)| link);
            let Some(worst) = worst else {
                return;
            };
            if let Some(entry) = self.links.get(&worst) {
                tracing::info!(parent: &entry.span, "degraded, evicting");
            }
            self.disconnect(now, worst, DisconnectReason::DegreeCap);
        }
    }

    /// Scans once a `Hello` is set, and advertises once a beacon is set
    /// too, with the profiles of the discovery mode in force. A request
    /// that starts, stops or changes the mode of either is reported to
    /// the controller as made at `now`, or at the next poll when the
    /// command had no time; a new beacon on its own is not.
    fn apply_profiles(&mut self, now: Option<Duration>) {
        if self.hello.is_none() {
            return;
        }
        let profiles = self.discovery.profiles();
        let mut requested = false;
        let scan = profiles.scan.map(|mode| ScanProfile { mode });
        if self.scan != Some(scan) {
            self.scan = Some(scan);
            self.transport.set_scanning(scan);
            requested = true;
        }
        if let Some(beacon) = self.local_beacon() {
            let advert = AdvProfile {
                beacon,
                mode: profiles.advertise,
            };
            if self.advertised != Some(advert) {
                requested |= self.advertised.map(|old| old.mode) != Some(advert.mode);
                self.advertised = Some(advert);
                self.transport.set_advertising(Some(advert));
            }
        }
        if requested {
            self.discovery.requested(now);
        }
    }

    /// The beacon to advertise, once the caller has set its fields and
    /// if the radio can advertise at all.
    fn local_beacon(&self) -> Option<[u8; 16]> {
        let own = self.beacon?;
        if !self.transport.caps().can_advertise {
            return None;
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
        };
        Some(beacon.encode())
    }
}

/// The degree below which discovery is aggressive: the stable target, or
/// the session's lowered cap if that is smaller.
fn discovery_target(policy: &TopologyPolicy) -> usize {
    policy.target_stable.min(policy.effective_cap)
}

#[cfg(all(test, feature = "sim"))]
mod tests {
    use super::*;
    use crate::{SimConfig, SimNode, SimTransport, SimWorld};
    use proptest::prelude::*;
    use proptest::test_runner::{Config, MapFailurePersistence, RngSeed};
    use std::task::Waker;

    /// A world with one `Network` per radio, hello and beacon
    /// configured, and only the given pairs in radio range.
    fn world_with(
        radios: &[SimNode],
        hints: &[[u8; 8]],
        pairs: &[(usize, usize)],
    ) -> (SimWorld, Vec<Network<SimTransport>>) {
        let world = SimWorld::new(SimConfig::default());
        let nets: Vec<Network<SimTransport>> = radios
            .iter()
            .enumerate()
            .map(|(i, radio)| {
                let radio = world.add_node(*radio).unwrap();
                let mut net = Network::new(NetworkConfig::default(), radio);
                net.set_local_hello(
                    LocalTag::new([(i + 1) as u8; 16]),
                    AppHello::new(Vec::new()).unwrap(),
                );
                net.set_beacon(
                    hints[i],
                    [0; 4],
                    BeaconFlags {
                        accepting: true,
                        ..BeaconFlags::default()
                    },
                );
                net
            })
            .collect();
        for &(a, b) in pairs {
            let (na, nb) = (nets[a].transport().node(), nets[b].transport().node());
            world.set_in_range(na, nb, true);
        }
        (world, nets)
    }

    fn settle(world: &SimWorld, nets: &mut [Network<SimTransport>]) {
        let mut cx = Context::from_waker(Waker::noop());
        for net in nets.iter_mut() {
            while net.poll_event(&mut cx, world.now()).is_ready() {}
        }
    }

    fn run(world: &SimWorld, nets: &mut [Network<SimTransport>], duration: Duration) {
        let end = world.now() + duration;
        while world.now() < end {
            world.advance(Duration::from_millis(10));
            settle(world, nets);
        }
    }

    /// Admission walks stable to the stable cap and then uses the
    /// explore slot for the fifth link. A fifth peer new to the node is
    /// first given the opportunistic slot (NET-22); once that link is
    /// released and its cooldown has passed, the same peer comes back
    /// through normal admission into the explore slot. The outbound
    /// explore pick comes from the candidates, which never offer a peer
    /// that is already linked, so all five links are to distinct peers
    /// and a re-sighting of an existing neighbour never opens a sixth.
    #[test]
    fn admission_walks_stable_then_explore() {
        // Node 0 is the target: hint [3] sorts below P3 and P4, so it
        // dials them, and above P1 and P2, so they dial it.
        let radios = vec![SimNode::default(); 6];
        let hints = [[3; 8], [1; 8], [2; 8], [4; 8], [5; 8], [6; 8]];
        let pairs: Vec<(usize, usize)> = (1..5).map(|i| (0, i)).collect();
        let (world, mut nets) = world_with(&radios, &hints, &pairs);
        run(&world, &mut nets, Duration::from_secs(3));

        let target = &nets[0];
        assert_eq!(target.links().len(), 4, "four inbound and outbound links");
        let counts = target.counts();
        assert_eq!((counts.total, counts.stable, counts.explore), (4, 4, 0));

        // The fifth peer arrives; the re-sightings of the linked
        // peers cannot be dialed again, so only this one is.
        let (na, np5) = (nets[0].transport().node(), nets[5].transport().node());
        world.set_in_range(na, np5, true);
        run(&world, &mut nets, Duration::from_secs(3));
        let counts = nets[0].counts();
        assert_eq!((counts.total, counts.opportunistic), (5, 1));
        let p5 = nets[5].transport().peer_key();
        let opportunistic = nets[0].links().into_iter().find(|info| info.peer == p5);
        let now = world.now();
        nets[0].release(now, opportunistic.expect("linked to P5").link);
        // The cooldown holds P5 off, and then it is an ordinary candidate.
        let cooldown = nets[0].policy.cooldown;
        run(&world, &mut nets, cooldown + Duration::from_secs(3));

        let target = &nets[0];
        assert_eq!(target.links().len(), 5, "the explore slot opened");
        let counts = target.counts();
        assert_eq!((counts.total, counts.stable, counts.explore), (5, 4, 1));
        let explore = target
            .links
            .values()
            .find(|entry| entry.class == LinkClass::Explore)
            .map(|entry| entry.peer);
        assert_eq!(explore, Some(nets[5].transport().peer_key()));
        let mut peers: Vec<PeerKey> = target.links().iter().map(|info| info.peer).collect();
        peers.sort();
        peers.dedup();
        assert_eq!(peers.len(), 5, "every link is to a distinct peer");
    }

    /// An opportunistic link closed as the losing duplicate, while the
    /// winning link to the same peer stays up, starts no cooldown: the
    /// peer has not left. Run with the tags both ways round, so that in
    /// one of them the losing link was already up when it lost.
    #[test]
    fn a_duplicate_opportunistic_link_starts_no_cooldown() {
        let mut swapped = 0;
        for tags in [[1u8, 2], [2, 1]] {
            let world = SimWorld::new(SimConfig {
                connect_jitter: Duration::ZERO,
                ..SimConfig::default()
            });
            // Three links: a stable target of one, so one neighbour each
            // puts A and B at their target and opens the opportunistic slot.
            let config = NetworkConfig::default().with_max_links(3).unwrap();
            let accepting = BeaconFlags {
                accepting: true,
                ..BeaconFlags::default()
            };
            // A and B have equal hints, so both dial each other after the
            // delay; each has one neighbour with a higher hint.
            let mut nets: Vec<Network<SimTransport>> =
                [(tags[0], 7u8), (tags[1], 7), (3, 9), (4, 9)]
                    .into_iter()
                    .map(|(tag, hint)| {
                        let radio = world.add_node(SimNode::default()).unwrap();
                        let mut net = Network::new(config.clone(), radio);
                        let app_hello = AppHello::new(Vec::new()).unwrap();
                        net.set_local_hello(LocalTag::new([tag; 16]), app_hello);
                        net.set_beacon([hint; 8], [0; 4], accepting);
                        net
                    })
                    .collect();
            let node = |i: usize, nets: &[Network<SimTransport>]| nets[i].transport().node();
            world.set_in_range(node(0, &nets), node(2, &nets), true);
            world.set_in_range(node(1, &nets), node(3, &nets), true);
            run(&world, &mut nets, Duration::from_secs(2));

            world.set_in_range(node(0, &nets), node(1, &nets), true);
            let mut cx = Context::from_waker(Waker::noop());
            let mut events: [Vec<NetworkEvent>; 2] = [Vec::new(), Vec::new()];
            for _ in 0..800 {
                world.advance(Duration::from_millis(10));
                for (i, net) in nets.iter_mut().enumerate() {
                    while let Poll::Ready(event) = net.poll_event(&mut cx, world.now()) {
                        if i < 2 {
                            events[i].push(event);
                        }
                    }
                }
            }
            // The higher tag's own opportunistic dial loses the tie-break.
            let (loser, winner) = if tags[0] > tags[1] { (0, 1) } else { (1, 0) };
            let peer = nets[winner].transport().peer_key();
            let links = nets[loser].links();
            let to_winner: Vec<&LinkInfo> = links.iter().filter(|l| l.peer == peer).collect();
            assert_eq!(to_winner.len(), 1, "{links:?}");
            assert_eq!(to_winner[0].role, LinkRole::Responder);
            assert_eq!(nets[loser].cooldowns.tracked(), 0, "tags {tags:?}");
            swapped += events[loser]
                .iter()
                .filter(|e| {
                    matches!(
                        e,
                        NetworkEvent::LinkDown {
                            reason: DisconnectReason::Duplicate,
                            ..
                        }
                    )
                })
                .count();
        }
        assert!(swapped > 0, "no up opportunistic link lost the tie-break");
    }

    /// Closing the losing duplicate leaves the peer linked: its table
    /// entry does not enter backoff, and the peer map points at the
    /// surviving link.
    #[test]
    fn a_closed_duplicate_leaves_the_peer_linked() {
        use crate::topology::PeerState;
        let world = SimWorld::new(SimConfig {
            connect_jitter: Duration::ZERO,
            ..SimConfig::default()
        });
        let mut nets: Vec<Network<SimTransport>> = (1..=2u8)
            .map(|i| {
                let radio = world.add_node(SimNode::default()).unwrap();
                let mut net = Network::new(NetworkConfig::default(), radio);
                net.set_local_hello(LocalTag::new([i; 16]), AppHello::new(Vec::new()).unwrap());
                // Equal hints: both dial once the delay has passed.
                net.set_beacon(
                    [7; 8],
                    [0; 4],
                    BeaconFlags {
                        accepting: true,
                        ..BeaconFlags::default()
                    },
                );
                net
            })
            .collect();
        let (a, b) = (nets[0].transport().node(), nets[1].transport().node());
        world.set_in_range(a, b, true);
        run(&world, &mut nets, Duration::from_secs(8));

        for (net, other) in [(&nets[0], &nets[1]), (&nets[1], &nets[0])] {
            let peer = other.transport().peer_key();
            let [info] = net.links()[..] else {
                panic!("one link expected: {:?}", net.links());
            };
            assert_eq!(net.links.len(), 1, "the duplicate is gone");
            assert_eq!(net.peers.get(&peer), Some(&info.link));
            assert_eq!(net.table.state(&peer), Some(PeerState::Linked));
        }
    }

    /// A peer blacklisted over one link loses every other link it holds
    /// here, and is then neither redialed nor accepted. Strikes below the
    /// limit and ordinary endings leave its link alone.
    #[test]
    fn a_blacklisted_peer_loses_its_other_links() {
        let radios = vec![SimNode::default(); 2];
        let (world, mut nets) = world_with(&radios, &[[1; 8], [2; 8]], &[(0, 1)]);
        run(&world, &mut nets, Duration::from_secs(2));
        let peer = nets[1].transport().peer_key();
        assert_eq!(nets[0].links().len(), 1);

        let now = world.now();
        nets[0].judge(now, peer, DisconnectReason::KeepaliveTimeout);
        nets[0].judge(now, peer, DisconnectReason::ProtocolViolation);
        nets[0].judge(now, peer, DisconnectReason::ProtocolViolation);
        assert_eq!(nets[0].links().len(), 1, "two strikes are not a block");

        // As if a second link to the same peer had ended in a flood.
        nets[0].judge(now, peer, DisconnectReason::AbuseDetected);
        let mut cx = Context::from_waker(Waker::noop());
        let mut downs = Vec::new();
        for _ in 0..100 {
            world.advance(Duration::from_millis(10));
            while let Poll::Ready(event) = nets[0].poll_event(&mut cx, world.now()) {
                downs.push(event);
            }
            settle(&world, &mut nets[1..]);
        }
        assert!(
            matches!(
                downs[..],
                [NetworkEvent::LinkDown {
                    reason: DisconnectReason::AbuseDetected,
                    ..
                }]
            ),
            "{downs:?}"
        );
        // Sightings keep coming both ways: node 0 does not dial the
        // blocked peer, and the peer's own delayed dial is refused.
        run(&world, &mut nets, Duration::from_secs(10));
        assert!(nets[0].links().is_empty());
        assert!(nets[0].links.is_empty(), "not even a connecting entry");
    }

    /// Three rejected outbound attempts in a row lower the session's
    /// effective cap by one, and the node then refuses an inbound it
    /// would otherwise have taken.
    #[test]
    fn rejections_lower_the_cap_end_to_end() {
        // 0 A: the node under test. 1 B: a radio capped at two links,
        // filled by 2 C and 3 D before A ever dials, so A's attempts
        // come back Rejected. 4..8: dialers for the final phase.
        let radios = vec![
            SimNode::default(),
            SimNode {
                max_links: 2,
                ..SimNode::default()
            },
            SimNode::default(),
            SimNode::default(),
            SimNode::default(),
            SimNode::default(),
            SimNode::default(),
            SimNode::default(),
            SimNode::default(),
        ];
        let hints = [
            [2; 8], [5; 8], [3; 8], [4; 8], [0; 8], [0; 8], [0; 8], [0; 8], [0; 8],
        ];
        let (world, mut nets) = world_with(&radios, &hints, &[(1, 2), (1, 3)]);
        run(&world, &mut nets, Duration::from_secs(2));
        assert_eq!(nets[1].links().len(), 2, "B's radio is full");

        // A comes into range and dials; B's radio refuses the third
        // connection at the transport level. The loop stops on the
        // first observation of the lowered cap, which can only be the
        // third rejection: a fourth attempt is still in backoff.
        let (na, nb) = (nets[0].transport().node(), nets[1].transport().node());
        world.set_in_range(na, nb, true);
        while nets[0].policy.effective_cap == nets[0].policy.max_total {
            world.advance(Duration::from_millis(250));
            settle(&world, &mut nets);
            assert!(
                world.now() < Duration::from_secs(30),
                "three rejections never arrived"
            );
        }
        assert_eq!(nets[0].policy.effective_cap, 4);

        // Five peers come into range at once, from a sighting taken
        // while A was still advertising room: four are admitted, the
        // fifth meets the lowered cap and is refused.
        for e in 4..9 {
            let (na, ne) = (nets[0].transport().node(), nets[e].transport().node());
            world.set_in_range(na, ne, true);
        }
        run(&world, &mut nets, Duration::from_secs(2));
        assert_eq!(nets[0].links().len(), 4, "the fifth inbound was refused");
        let dialed: usize = nets[4..].iter().map(|net| net.links().len()).sum();
        assert_eq!(dialed, 4, "exactly one of the five has no link");
    }

    #[derive(Debug)]
    enum Action {
        Advance(u16),
        Toggle(u8, u8, bool),
    }

    fn action() -> impl Strategy<Value = Action> {
        prop_oneof![
            (1..50u16).prop_map(Action::Advance),
            (0..6u8, 0..6u8, any::<bool>()).prop_map(|(a, b, on)| Action::Toggle(a, b, on)),
        ]
    }

    fn churn() -> Config {
        let mut config = Config::with_cases(24);
        config.rng_seed = RngSeed::Fixed(0x0016_1616);
        // In-memory persistence: Miri forbids the getcwd that file
        // persistence needs. The fixed seed replays every run;
        // promote any minimal failing input to a regression test.
        config.failure_persistence = Some(Box::new(MapFailurePersistence::default()));
        config
    }

    // Done when: total links never exceed the cap under random
    // join/leave churn. Admission counts connecting and closing
    // entries against the cap, so no interleaving gets past it.
    proptest! {
        #![proptest_config(churn())]
        #[test]
        fn links_never_exceed_the_cap_under_churn(
            actions in proptest::collection::vec(action(), 30..90),
        ) {
            let world = SimWorld::new(SimConfig::default());
            let config = NetworkConfig::default();
            let max = usize::try_from(config.max_links()).unwrap_or(usize::MAX);
            let mut nets: Vec<Network<SimTransport>> = (0..6u8)
                .map(|i| {
                    let radio = world.add_node(SimNode::default()).unwrap();
                    let mut net = Network::new(config.clone(), radio);
                    net.set_local_hello(
                        LocalTag::new([i; 16]),
                        AppHello::new(Vec::new()).unwrap(),
                    );
                    net.set_beacon(
                        [i; 8],
                        [0; 4],
                        BeaconFlags {
                            accepting: true,
                            ..BeaconFlags::default()
                        },
                    );
                    net
                })
                .collect();
            for a in 0..nets.len() {
                for b in a + 1..nets.len() {
                    let (na, nb) = (nets[a].transport().node(), nets[b].transport().node());
                    world.set_in_range(na, nb, true);
                }
            }
            for step in actions {
                match step {
                    Action::Advance(ms) => world.advance(Duration::from_millis(u64::from(ms))),
                    Action::Toggle(a, b, on) => {
                        let (a, b) = (a as usize, b as usize);
                        if a != b && a < nets.len() && b < nets.len() {
                            let (na, nb) = (nets[a].transport().node(), nets[b].transport().node());
                            world.set_in_range(na, nb, on);
                        }
                    }
                }
                settle(&world, &mut nets);
                for net in &nets {
                    assert!(
                        net.links.len() <= max,
                        "cap exceeded: {} links, cap {max}, after {step:?}",
                        net.links.len()
                    );
                }
            }
        }
    }
}
