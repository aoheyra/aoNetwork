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

use crate::topology::{ClassCounts, LinkClass, PeerTable, TopologyPolicy};
use crate::{
    AdvProfile, AppHello, Beacon, BeaconFlags, Channel, ConnectError, DisconnectReason,
    FRAME_VERSION, Hello, Link, LinkAction, LinkEnd, LinkId, LinkMetrics, LinkRole, LinkState,
    LocalTag, NetworkConfig, PeerKey, PowerMode, RadioState, ScanProfile, SendError, SendOpts,
    Transport, TransportEvent,
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
    /// Which slot admitted it, so admission and eviction count the
    /// classes separately.
    class: LinkClass,
    /// When it was admitted: eviction spares links younger than the
    /// policy's minimum age.
    opened_at: Duration,
    up: bool,
}

/// The link layer of one node.
///
/// Until both [`set_local_hello`](Self::set_local_hello) and
/// [`set_beacon`](Self::set_beacon) are called the node forms no links:
/// it needs a `Hello` to handshake and a beacon to be found. It connects to
/// a discovered peer when the topology policy has a slot free, when the
/// peer's beacon says it is accepting, and when its own beacon hint sorts
/// below the peer's, so that exactly one side of each pair dials. The peer
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
    /// Peers seen in beacons, with their backoff and eviction.
    table: PeerTable,
    /// The slots, scores and eviction rules links are admitted under.
    policy: TopologyPolicy,
    events: VecDeque<NetworkEvent>,
}

impl<T: Transport> Network<T> {
    /// A node on `transport`, which it starts.
    pub fn new(config: NetworkConfig, mut transport: T) -> Self {
        transport.start();
        let table = PeerTable::new(&config);
        let policy = TopologyPolicy::new(&config);
        Self {
            config,
            transport,
            hello: None,
            beacon: None,
            scanning: false,
            advertised: None,
            links: BTreeMap::new(),
            peers: BTreeMap::new(),
            table,
            policy,
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
        self.apply(now, link, actions);
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
            TransportEvent::Discovered { peer, rssi, beacon } => {
                let fresh = self.table.observe(now, *peer, *rssi, beacon);
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
    /// (not linked, past its backoff, accepting, strong, recently seen)
    /// and its hint must sort below ours, so that exactly one side of
    /// each pair dials. The link is admitted into the class the policy
    /// grants from the current inventory: stable first, then explore.
    fn connect_to(&mut self, now: Duration, peer: PeerKey) {
        let (Some(hello), Some(own)) = (&self.hello, &self.beacon) else {
            return;
        };
        let Some(class) = self.policy.admit(&self.counts()) else {
            return;
        };
        if !self.table.eligible(&peer, now, &self.peers) {
            return;
        }
        let Some(hint) = self.table.hint_of(&peer) else {
            return;
        };
        if own.hint >= hint {
            return;
        }
        let machine = Link::new(&self.config, hello.clone());
        let link = self.transport.connect(peer);
        self.add(now, link, peer, LinkRole::Initiator, class, machine);
    }

    /// Admits an inbound link, if a `Hello` to send and a slot of some
    /// class are free. The policy decides, not the caller: an inbound
    /// link consumes the same stable or explore slot an outbound one
    /// would, and never the opportunistic slot (NET-22 owns it), and a
    /// peer below the policy's admission floor is refused the same way
    /// a candidate would be, so an evicted weak peer cannot walk
    /// straight back in while it is still weak. A peer that has never
    /// been seen cannot be judged and is admitted.
    fn accept(&mut self, now: Duration, link: LinkId, peer: PeerKey) -> bool {
        let Some(hello) = &self.hello else {
            return false;
        };
        let Some(class) = self.policy.admit(&self.counts()) else {
            return false;
        };
        if self.peers.contains_key(&peer) {
            return false;
        }
        if self
            .table
            .rssi_of(&peer)
            .is_some_and(|rssi| rssi < self.policy.admit_floor)
        {
            return false;
        }
        let machine = Link::new(&self.config, hello.clone());
        self.add(now, link, peer, LinkRole::Responder, class, machine);
        true
    }

    /// The current inventory, for admission and eviction decisions.
    fn counts(&self) -> ClassCounts {
        let mut counts = ClassCounts {
            total: self.links.len(),
            stable: 0,
            explore: 0,
        };
        for entry in self.links.values() {
            match entry.class {
                LinkClass::Stable => counts.stable += 1,
                LinkClass::Explore => counts.explore += 1,
                // Opportunistic links count towards the total only;
                // NET-22 admits them against their own slot.
                LinkClass::Opportunistic => {}
            }
        }
        counts
    }

    /// Whether any class would admit one more link right now.
    fn has_room(&self) -> bool {
        self.policy.admit(&self.counts()).is_some()
    }

    fn add(
        &mut self,
        now: Duration,
        link: LinkId,
        peer: PeerKey,
        role: LinkRole,
        class: LinkClass,
        machine: Link,
    ) {
        self.links.insert(
            link,
            Entry {
                machine,
                peer,
                role,
                class,
                opened_at: now,
                up: false,
            },
        );
        self.peers.insert(peer, link);
        self.table.mark_connecting(&peer);
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
                self.apply(now, link, actions);
            }
        }
    }

    fn apply(&mut self, now: Duration, link: LinkId, actions: Vec<LinkAction>) {
        for action in actions {
            match action {
                LinkAction::Write(chunk) => self.transport.write(link, chunk),
                LinkAction::Disconnect => self.transport.disconnect(link),
                LinkAction::Ready { remote, .. } => {
                    if let Some(entry) = self.links.get_mut(&link) {
                        entry.up = true;
                        self.table.mark_linked(&entry.peer);
                    }
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
        if entry.role == LinkRole::Initiator
            && matches!(end, LinkEnd::ConnectFailed(ConnectError::Rejected))
        {
            // Our own outbound attempt was turned away: count the
            // strike that lowers the session's effective cap.
            self.policy.note_rejection();
        }
        self.table.mark_down(now, &entry.peer);
        if self.peers.get(&entry.peer) == Some(&link) {
            self.peers.remove(&entry.peer);
        }
        if let (true, LinkEnd::Disconnected(reason)) = (entry.up, end) {
            self.events
                .push_back(NetworkEvent::LinkDown { link, reason });
        }
        self.apply_profiles();
        // A slot just freed: dial the next candidate now rather than
        // waiting for a sighting to say the neighbourhood changed.
        for peer in self.table.candidates(now, &self.peers) {
            self.connect_to(now, peer);
        }
    }

    /// Sheds the worst link while the node holds more healthy links
    /// than its stable target: only a link that is up and ready, has
    /// survived the policy's minimum age, and whose peer average has
    /// fallen below the admission floor may go, and the weakest score
    /// goes first. Links that are merely over the target but strong
    /// are kept - that is the hysteresis that stops a node from
    /// churning its neighbourhood. The evicted peer inherits its
    /// normal backoff through [`PeerTable::mark_down`], so it cannot
    /// be redialed the moment a slot frees.
    fn enforce_eviction(&mut self, now: Duration) {
        loop {
            let ready = self
                .links
                .values()
                .filter(|entry| entry.up && entry.machine.state() == LinkState::Ready)
                .count();
            if ready <= self.policy.target_stable {
                return;
            }
            let worst = self
                .links
                .iter()
                .filter(|(_, entry)| entry.up && entry.machine.state() == LinkState::Ready)
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
            self.disconnect(now, worst, DisconnectReason::DegreeCap);
        }
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
    /// explore slot for the fifth link: the outbound explore pick
    /// comes from the candidates, which never offer a peer that is
    /// already linked, so all five links are to distinct peers and a
    /// re-sighting of an existing neighbour never opens a sixth.
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
