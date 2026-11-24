//! In-memory radio world for tests (feature `sim`).
//!
//! [`SimWorld`] holds every simulated node, the links between them and a
//! paused clock that moves only when [`SimWorld::advance`] is called. Each
//! node's radio is a [`SimTransport`], which implements [`Transport`] and can
//! be handed to the link layer unchanged.
//!
//! Runs are deterministic: actions due at the same instant run in the order
//! they were scheduled, and the only randomness is connect-latency jitter
//! drawn from [`SimConfig::seed`]. The same seed and the same calls give the
//! same events at the same times.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use crate::{
    AdvProfile, BEACON_LEN, ConnectError, DisconnectReason, LinkId, LinkRole, LinkTransport,
    MAX_MTU, MIN_MTU, PeerKey, RadioState, ScanProfile, Transport, TransportCaps, TransportEvent,
};

/// Signal strength a node reports for every in-range peer by default;
/// override it per node with [`SimNode::rssi`].
pub const SIM_RSSI: i16 = -60;

/// How often a scanner re-reports an unchanged beacon, as a radio would
/// on each scan window. The refresh is what lets a connect attempt that
/// backoff gated resume without waiting for the beacon to change.
const SCAN_REFRESH: Duration = Duration::from_secs(1);

/// ATT header bytes; a GATT chunk may use the rest of the MTU.
/// L2CAP CoC packets have no ATT header (NET-26).
const ATT_HEADER: usize = 3;

/// World-wide timing and the seed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimConfig {
    /// Seeds connect-latency jitter.
    pub seed: u64,
    /// Fixed part of the time from `connect` to its outcome.
    pub connect_latency: Duration,
    /// Up to this much is added to each connect, drawn from the seed.
    pub connect_jitter: Duration,
    /// Time from `write` to `Received` at the peer and `WriteReady` back.
    pub write_latency: Duration,
}

impl Default for SimConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            connect_latency: Duration::from_millis(200),
            connect_jitter: Duration::from_millis(100),
            write_latency: Duration::from_millis(10),
        }
    }
}

/// One simulated phone's radio.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimNode {
    /// Largest ATT MTU it negotiates; a link uses the smaller of the two.
    pub mtu: u16,
    /// Most links it holds at once.
    pub max_links: u32,
    /// Whether it can advertise and accept links.
    pub can_advertise: bool,
    /// Signal strength other nodes report when they hear it advertise,
    /// as `Discovered` RSSI.
    pub rssi: i16,
    /// Most bytes of received chunks it holds before they are polled,
    /// over all its links, or `None` for no limit. A chunk that does not
    /// fit is dropped and its link closed with `TransportLost`, as a
    /// phone's overflowing receive queue would.
    pub rx_buffer: Option<usize>,
    /// Carries link data over an L2CAP CoC instead of GATT (NET-26).
    /// Discovery still uses advertising.
    pub l2cap: bool,
    /// CoC packet size used when both ends set `l2cap`, 23..=517.
    pub coc_mtu: u16,
}

impl Default for SimNode {
    fn default() -> Self {
        Self {
            mtu: 247,
            max_links: 7,
            can_advertise: true,
            rssi: SIM_RSSI,
            rx_buffer: None,
            l2cap: false,
            coc_mtu: 512,
        }
    }
}

/// Index of a node in its [`SimWorld`], in the order nodes were added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(usize);

impl NodeId {
    /// The node's position in the order nodes were added, from 0.
    pub const fn index(self) -> usize {
        self.0
    }
}

/// A [`SimNode`] could not be added.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum SimError {
    /// The MTU is outside the range a BLE stack may negotiate.
    #[error("unsupported ATT MTU: {mtu}")]
    InvalidMtu {
        /// The rejected MTU.
        mtu: u16,
    },
    /// The CoC MTU is outside the range the link layer can fragment for.
    #[error("unsupported CoC MTU: {mtu}")]
    InvalidCocMtu {
        /// The rejected MTU.
        mtu: u16,
    },
}

/// A shared, deterministic radio world. Clones refer to the same world.
///
/// # Example
///
/// ```
/// use ao_network::{SimConfig, SimNode, SimWorld, Transport, TransportEvent};
/// use std::task::{Context, Poll, Waker};
/// use std::time::Duration;
///
/// let world = SimWorld::new(SimConfig::default());
/// let mut a = world.add_node(SimNode::default())?;
/// let mut b = world.add_node(SimNode::default())?;
/// world.set_in_range(a.node(), b.node(), true);
/// a.start();
/// b.start();
///
/// let link = a.connect(b.peer_key());
/// world.advance(Duration::from_secs(1));
///
/// let mut cx = Context::from_waker(Waker::noop());
/// assert!(matches!(a.poll_event(&mut cx), Poll::Ready(TransportEvent::RadioState(_))));
/// // b is not advertising, so it cannot be reached.
/// assert!(matches!(
///     a.poll_event(&mut cx),
///     Poll::Ready(TransportEvent::ConnectFailed { link: l, .. }) if l == link
/// ));
/// # Ok::<(), ao_network::SimError>(())
/// ```
#[derive(Debug, Clone)]
pub struct SimWorld {
    world: Arc<Mutex<World>>,
}

impl SimWorld {
    /// Creates an empty world at time zero.
    pub fn new(config: SimConfig) -> Self {
        Self {
            world: Arc::new(Mutex::new(World {
                rng: SplitMix64(config.seed),
                config,
                now: Duration::ZERO,
                next_seq: 0,
                schedule: BTreeMap::new(),
                nodes: Vec::new(),
                in_range: BTreeSet::new(),
                wakes: Vec::new(),
            })),
        }
    }

    /// Adds a node and returns its radio. The radio is idle until
    /// [`Transport::start`].
    ///
    /// # Errors
    ///
    /// [`SimError::InvalidMtu`] if `node.mtu` is outside 23..=517.
    pub fn add_node(&self, node: SimNode) -> Result<SimTransport, SimError> {
        if !(MIN_MTU..=MAX_MTU).contains(&node.mtu) {
            return Err(SimError::InvalidMtu { mtu: node.mtu });
        }
        if !(MIN_MTU..=MAX_MTU).contains(&node.coc_mtu) {
            return Err(SimError::InvalidCocMtu { mtu: node.coc_mtu });
        }
        let id = with_world(&self.world, |w| w.add_node(node));
        Ok(SimTransport {
            world: Arc::clone(&self.world),
            node: id,
            key: peer_key(id),
        })
    }

    /// Puts two nodes in or out of radio range of each other. Going out of
    /// range drops their links with [`DisconnectReason::TransportLost`].
    pub fn set_in_range(&self, a: NodeId, b: NodeId, in_range: bool) {
        with_world(&self.world, |w| w.set_in_range(a, b, in_range));
    }

    /// Reports `node` at `rssi` to every scanner from the next scan
    /// refresh on, as if its signal had moved; tests use it to drift a
    /// peer below the admission floor while a link is held.
    pub fn set_rssi(&self, node: NodeId, rssi: i16) {
        with_world(&self.world, |w| w.set_rssi(node, rssi));
    }

    /// Drops one link on both sides, as if the radio connection broke.
    pub fn kill_link(&self, node: NodeId, link: LinkId) {
        with_world(&self.world, |w| w.kill_link(node, link));
    }

    /// Moves the clock forward by `by`, running everything due on the way.
    pub fn advance(&self, by: Duration) {
        with_world(&self.world, |w| w.advance(by));
    }

    /// The current simulated time.
    pub fn now(&self) -> Duration {
        with_world(&self.world, |w| w.now)
    }
}

/// One node's radio in a [`SimWorld`].
#[derive(Debug)]
pub struct SimTransport {
    world: Arc<Mutex<World>>,
    node: NodeId,
    key: PeerKey,
}

impl SimTransport {
    /// This node's id in the world.
    pub fn node(&self) -> NodeId {
        self.node
    }

    /// The key other nodes see this node under.
    pub fn peer_key(&self) -> PeerKey {
        self.key
    }
}

impl Transport for SimTransport {
    fn caps(&self) -> TransportCaps {
        let node = with_world(&self.world, |w| w.nodes[self.node.0].config);
        TransportCaps {
            can_advertise: node.can_advertise,
            max_mtu: if node.l2cap { node.coc_mtu } else { node.mtu },
            max_links: node.max_links,
        }
    }

    fn link_transport(&self) -> LinkTransport {
        let node = with_world(&self.world, |w| w.nodes[self.node.0].config);
        if node.l2cap {
            LinkTransport::L2capCoc
        } else {
            LinkTransport::Gatt
        }
    }

    fn start(&mut self) {
        with_world(&self.world, |w| w.start(self.node));
    }

    fn stop(&mut self) {
        with_world(&self.world, |w| w.stop(self.node));
    }

    fn set_advertising(&mut self, profile: Option<AdvProfile>) {
        with_world(&self.world, |w| w.set_advertising(self.node, profile));
    }

    fn set_scanning(&mut self, profile: Option<ScanProfile>) {
        with_world(&self.world, |w| w.set_scanning(self.node, profile));
    }

    fn connect(&mut self, peer: PeerKey) -> LinkId {
        with_world(&self.world, |w| w.connect(self.node, peer))
    }

    fn disconnect(&mut self, link: LinkId) {
        with_world(&self.world, |w| w.disconnect(self.node, link));
    }

    fn write(&mut self, link: LinkId, chunk: Vec<u8>) {
        with_world(&self.world, |w| w.write(self.node, link, chunk));
    }

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<TransportEvent> {
        with_world(&self.world, |w| {
            let node = &mut w.nodes[self.node.0];
            match node.events.pop_front() {
                Some(event) => {
                    if let TransportEvent::Received { chunk, .. } = &event {
                        node.rx_held -= chunk.len();
                    }
                    Poll::Ready(event)
                }
                None => {
                    node.waker = Some(cx.waker().clone());
                    Poll::Pending
                }
            }
        })
    }
}

/// Runs `f` under the lock, then wakes pollers outside it so a waker that
/// polls synchronously cannot deadlock.
fn with_world<R>(world: &Mutex<World>, f: impl FnOnce(&mut World) -> R) -> R {
    let (result, wakes) = {
        let mut guard: MutexGuard<'_, World> = world.lock().unwrap_or_else(PoisonError::into_inner);
        let result = f(&mut guard);
        (result, std::mem::take(&mut guard.wakes))
    };
    for waker in wakes {
        waker.wake();
    }
    result
}

fn peer_key(node: NodeId) -> PeerKey {
    // Laid out like a 6-byte BLE address followed by zeros (native
    // contract, section 3); the index fills the low four address bytes.
    let mut bytes = [0u8; 16];
    let index = u32::try_from(node.0).unwrap_or(u32::MAX);
    bytes[2..6].copy_from_slice(&index.to_be_bytes());
    PeerKey::new(bytes)
}

/// `SplitMix64`: a small, well-distributed seeded generator. Statistical
/// quality is irrelevant here; reproducibility is the point.
#[derive(Debug)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

#[derive(Debug)]
struct World {
    config: SimConfig,
    rng: SplitMix64,
    now: Duration,
    next_seq: u64,
    /// Keyed by (due time, scheduling order) so ties run first-scheduled.
    schedule: BTreeMap<(Duration, u64), Action>,
    nodes: Vec<NodeState>,
    /// Unordered pairs, stored as (smaller, larger).
    in_range: BTreeSet<(NodeId, NodeId)>,
    wakes: Vec<Waker>,
}

#[derive(Debug)]
struct NodeState {
    config: SimNode,
    started: bool,
    advertising: Option<[u8; BEACON_LEN]>,
    scanning: bool,
    /// Advertisers this scanner has reported: the beacon it last saw
    /// and when it was last reported.
    seen: BTreeMap<NodeId, ([u8; BEACON_LEN], Duration)>,
    next_link: u64,
    links: BTreeMap<LinkId, Link>,
    events: VecDeque<TransportEvent>,
    /// Bytes of `Received` chunks in `events`.
    rx_held: usize,
    waker: Option<Waker>,
}

#[derive(Debug, Clone, Copy)]
struct Link {
    peer: NodeId,
    state: LinkState,
}

#[derive(Debug, Clone, Copy)]
enum LinkState {
    Connecting,
    Up {
        remote: LinkId,
        mtu: u16,
        /// CoC packets lose only the chunk header to ATT.
        coc: bool,
        can_write: bool,
    },
}

#[derive(Debug)]
enum Action {
    Connect {
        node: NodeId,
        link: LinkId,
    },
    Write {
        from: (NodeId, LinkId),
        to: (NodeId, LinkId),
        chunk: Vec<u8>,
    },
}

impl World {
    fn add_node(&mut self, config: SimNode) -> NodeId {
        self.nodes.push(NodeState {
            config,
            started: false,
            advertising: None,
            scanning: false,
            seen: BTreeMap::new(),
            next_link: 0,
            links: BTreeMap::new(),
            events: VecDeque::new(),
            rx_held: 0,
            waker: None,
        });
        NodeId(self.nodes.len() - 1)
    }

    fn emit(&mut self, node: NodeId, event: TransportEvent) {
        let state = &mut self.nodes[node.0];
        state.events.push_back(event);
        if let Some(waker) = state.waker.take() {
            self.wakes.push(waker);
        }
    }

    fn schedule(&mut self, delay: Duration, action: Action) {
        let key = (self.now + delay, self.next_seq);
        self.next_seq += 1;
        self.schedule.insert(key, action);
    }

    fn alloc_link(&mut self, node: NodeId) -> LinkId {
        let state = &mut self.nodes[node.0];
        state.next_link += 1;
        LinkId::new(state.next_link)
    }

    fn node_by_key(&self, key: PeerKey) -> Option<NodeId> {
        (0..self.nodes.len())
            .map(NodeId)
            .find(|&id| peer_key(id) == key)
    }

    fn in_range(&self, a: NodeId, b: NodeId) -> bool {
        self.in_range.contains(&(a.min(b), a.max(b)))
    }

    fn up_links(&self, node: NodeId) -> usize {
        self.nodes[node.0]
            .links
            .values()
            .filter(|l| matches!(l.state, LinkState::Up { .. }))
            .count()
    }

    fn start(&mut self, node: NodeId) {
        self.nodes[node.0].started = true;
        self.emit(node, TransportEvent::RadioState(RadioState::Ready));
        self.refresh_discovery();
    }

    fn stop(&mut self, node: NodeId) {
        let state = &mut self.nodes[node.0];
        state.started = false;
        state.advertising = None;
        state.scanning = false;
        state.seen.clear();
        let links: Vec<LinkId> = state.links.keys().copied().collect();
        for link in links {
            self.close(
                node,
                link,
                DisconnectReason::Closed,
                ConnectError::Cancelled,
            );
        }
        self.refresh_discovery();
    }

    fn set_advertising(&mut self, node: NodeId, profile: Option<AdvProfile>) {
        let state = &mut self.nodes[node.0];
        if state.config.can_advertise {
            state.advertising = profile.map(|p| p.beacon);
            self.refresh_discovery();
        }
    }

    fn set_scanning(&mut self, node: NodeId, profile: Option<ScanProfile>) {
        let state = &mut self.nodes[node.0];
        state.scanning = profile.is_some();
        if !state.scanning {
            state.seen.clear();
        }
        self.refresh_discovery();
    }

    fn set_rssi(&mut self, node: NodeId, rssi: i16) {
        if node.0 >= self.nodes.len() {
            return;
        }
        self.nodes[node.0].config.rssi = rssi;
        self.refresh_discovery();
    }

    fn set_in_range(&mut self, a: NodeId, b: NodeId, in_range: bool) {
        if a == b || a.0 >= self.nodes.len() || b.0 >= self.nodes.len() {
            return;
        }
        let pair = (a.min(b), a.max(b));
        if in_range {
            self.in_range.insert(pair);
        } else {
            self.in_range.remove(&pair);
            let doomed: Vec<LinkId> = self.nodes[a.0]
                .links
                .iter()
                .filter(|(_, l)| l.peer == b && matches!(l.state, LinkState::Up { .. }))
                .map(|(&id, _)| id)
                .collect();
            for link in doomed {
                self.kill_link(a, link);
            }
        }
        self.refresh_discovery();
    }

    /// Reports what each scanner can see: new or changed beacons as
    /// `Discovered`, vanished advertisers as `Lost`, and an unchanged
    /// beacon re-reported every [`SCAN_REFRESH`], as a radio would on
    /// each scan window. The re-report lets a connect attempt that
    /// backoff gated resume without waiting for the beacon to change.
    fn refresh_discovery(&mut self) {
        for scanner in (0..self.nodes.len()).map(NodeId) {
            let scanner_state = &self.nodes[scanner.0];
            if !(scanner_state.started && scanner_state.scanning) {
                continue;
            }
            let visible: BTreeMap<NodeId, [u8; BEACON_LEN]> = (0..self.nodes.len())
                .map(NodeId)
                .filter(|&other| self.in_range(scanner, other))
                .filter_map(|other| {
                    let state = &self.nodes[other.0];
                    state
                        .advertising
                        .filter(|_| state.started)
                        .map(|beacon| (other, beacon))
                })
                .collect();
            let seen = std::mem::take(&mut self.nodes[scanner.0].seen);
            let mut reported: BTreeMap<NodeId, ([u8; BEACON_LEN], Duration)> = BTreeMap::new();
            for (&other, beacon) in &visible {
                let previous = seen.get(&other);
                let changed = previous.is_none_or(|(last, _)| last != beacon);
                let due =
                    previous.is_none_or(|(_, at)| self.now.saturating_sub(*at) >= SCAN_REFRESH);
                if changed || due {
                    self.emit(
                        scanner,
                        TransportEvent::Discovered {
                            peer: peer_key(other),
                            rssi: self.nodes[other.0].config.rssi,
                            beacon: beacon.to_vec(),
                        },
                    );
                    reported.insert(other, (*beacon, self.now));
                } else if let Some(&entry) = previous {
                    reported.insert(other, entry);
                }
            }
            for &other in seen.keys().filter(|id| !visible.contains_key(id)) {
                self.emit(
                    scanner,
                    TransportEvent::Lost {
                        peer: peer_key(other),
                    },
                );
            }
            self.nodes[scanner.0].seen = reported;
        }
    }

    fn connect(&mut self, node: NodeId, peer: PeerKey) -> LinkId {
        let link = self.alloc_link(node);
        let target = match (self.nodes[node.0].started, self.node_by_key(peer)) {
            (true, Some(target)) => target,
            (started, _) => {
                let error = if started {
                    ConnectError::PeerUnknown
                } else {
                    ConnectError::RadioUnavailable
                };
                self.emit(node, TransportEvent::ConnectFailed { link, error });
                return link;
            }
        };
        self.nodes[node.0].links.insert(
            link,
            Link {
                peer: target,
                state: LinkState::Connecting,
            },
        );
        let delay = self.config.connect_latency + self.jitter();
        self.schedule(delay, Action::Connect { node, link });
        link
    }

    fn jitter(&mut self) -> Duration {
        let max = u64::try_from(self.config.connect_jitter.as_nanos()).unwrap_or(u64::MAX);
        if max == 0 {
            return Duration::ZERO;
        }
        Duration::from_nanos(self.rng.next() % max.saturating_add(1))
    }

    fn finish_connect(&mut self, node: NodeId, link: LinkId) {
        let Some(&Link {
            peer: target,
            state: LinkState::Connecting,
        }) = self.nodes[node.0].links.get(&link)
        else {
            return; // Cancelled meanwhile.
        };
        let target_state = &self.nodes[target.0];
        let reachable = self.in_range(node, target)
            && target_state.started
            && target_state.advertising.is_some();
        let local = &self.nodes[node.0].config;
        let coc = local.l2cap && target_state.config.l2cap;
        let mtu = if coc {
            local.coc_mtu.min(target_state.config.coc_mtu)
        } else {
            local.mtu.min(target_state.config.mtu)
        };
        let error = if !reachable || local.l2cap != target_state.config.l2cap {
            // A CoC endpoint cannot reach a GATT-only one: they listen on
            // different Android sockets (NET-26).
            Some(ConnectError::Unreachable)
        } else if self.up_links(node) >= self.cap(node) || self.up_links(target) >= self.cap(target)
        {
            Some(ConnectError::Rejected)
        } else {
            None
        };
        if let Some(error) = error {
            self.nodes[node.0].links.remove(&link);
            self.emit(node, TransportEvent::ConnectFailed { link, error });
            return;
        }

        let remote = self.alloc_link(target);
        let up = |remote| LinkState::Up {
            remote,
            mtu,
            coc,
            can_write: true,
        };
        self.nodes[node.0].links.insert(
            link,
            Link {
                peer: target,
                state: up(remote),
            },
        );
        self.nodes[target.0].links.insert(
            remote,
            Link {
                peer: node,
                state: up(link),
            },
        );
        self.emit(
            node,
            TransportEvent::Connected {
                link,
                peer: peer_key(target),
                role: LinkRole::Initiator,
                mtu,
            },
        );
        self.emit(
            target,
            TransportEvent::Connected {
                link: remote,
                peer: peer_key(node),
                role: LinkRole::Responder,
                mtu,
            },
        );
    }

    fn cap(&self, node: NodeId) -> usize {
        usize::try_from(self.nodes[node.0].config.max_links).unwrap_or(usize::MAX)
    }

    fn disconnect(&mut self, node: NodeId, link: LinkId) {
        self.close(
            node,
            link,
            DisconnectReason::Closed,
            ConnectError::Cancelled,
        );
    }

    fn kill_link(&mut self, node: NodeId, link: LinkId) {
        if node.0 < self.nodes.len() {
            self.close(
                node,
                link,
                DisconnectReason::TransportLost,
                ConnectError::Unreachable,
            );
        }
    }

    /// Ends `link` on `node` with `local` (or `pending` if it never came
    /// up); the far side of an up link always sees `TransportLost`.
    fn close(
        &mut self,
        node: NodeId,
        link: LinkId,
        local: DisconnectReason,
        pending: ConnectError,
    ) {
        let Some(closed) = self.nodes[node.0].links.remove(&link) else {
            return;
        };
        match closed.state {
            LinkState::Connecting => self.emit(
                node,
                TransportEvent::ConnectFailed {
                    link,
                    error: pending,
                },
            ),
            LinkState::Up { remote, .. } => {
                self.emit(
                    node,
                    TransportEvent::Disconnected {
                        link,
                        reason: local,
                    },
                );
                if self.nodes[closed.peer.0].links.remove(&remote).is_some() {
                    self.emit(
                        closed.peer,
                        TransportEvent::Disconnected {
                            link: remote,
                            reason: DisconnectReason::TransportLost,
                        },
                    );
                }
            }
        }
    }

    fn write(&mut self, node: NodeId, link: LinkId, chunk: Vec<u8>) {
        let Some(Link {
            peer,
            state:
                LinkState::Up {
                    remote,
                    mtu,
                    coc,
                    can_write,
                },
        }) = self.nodes[node.0].links.get_mut(&link)
        else {
            return; // Unknown, ended or still connecting: ignored.
        };
        debug_assert!(*can_write, "write without a write credit on {link:?}");
        // A CoC packet is the MTU; ATT steals three bytes from it.
        let overhead = if *coc { 0 } else { ATT_HEADER };
        debug_assert!(
            chunk.len() <= usize::from(*mtu) - overhead,
            "chunk of {} bytes exceeds mtu {mtu}",
            chunk.len()
        );
        if !*can_write {
            return;
        }
        *can_write = false;
        let to = (*peer, *remote);
        let delay = self.config.write_latency;
        self.schedule(
            delay,
            Action::Write {
                from: (node, link),
                to,
                chunk,
            },
        );
    }

    fn finish_write(&mut self, from: (NodeId, LinkId), to: (NodeId, LinkId), chunk: Vec<u8>) {
        let receiver = &self.nodes[to.0.0];
        if receiver.links.contains_key(&to.1) {
            let held = receiver.rx_held + chunk.len();
            if receiver.config.rx_buffer.is_some_and(|limit| held > limit) {
                // Closes the sender's end too, so no `WriteReady` follows.
                self.kill_link(to.0, to.1);
            } else {
                self.nodes[to.0.0].rx_held = held;
                self.emit(to.0, TransportEvent::Received { link: to.1, chunk });
            }
        }
        if let Some(Link {
            state: LinkState::Up { can_write, .. },
            ..
        }) = self.nodes[from.0.0].links.get_mut(&from.1)
        {
            *can_write = true;
            self.emit(from.0, TransportEvent::WriteReady { link: from.1 });
        }
    }

    fn advance(&mut self, by: Duration) {
        let until = self.now + by;
        while let Some(entry) = self.schedule.first_entry() {
            let (due, _) = *entry.key();
            if due > until {
                break;
            }
            let action = entry.remove();
            self.now = due;
            match action {
                Action::Connect { node, link } => self.finish_connect(node, link),
                Action::Write { from, to, chunk } => self.finish_write(from, to, chunk),
            }
        }
        self.now = until;
        self.refresh_discovery();
    }
}
