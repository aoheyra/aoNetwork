//! The simulator scenario suite (NET-20): the M3 invariants on seeded
//! random topologies of 5 to 50 nodes.
//!
//! Each case is a seed and a node count. Nodes sit at seeded positions in
//! a square sized for about six radio neighbours each, and are in range
//! within a fixed radius. The default cases run in CI on every change;
//! `AO_SCENARIO_SEEDS=N` instead runs N seeds with seeded sizes, which the
//! nightly workflow does in release mode. A failure names its seed and
//! size, and the run replays from them exactly.
#![cfg(feature = "sim")]
// Each case simulates minutes of up to 50 nodes, which under Miri takes
// hours; the code they drive is checked under Miri by the unit and
// integration tests.
#![cfg(not(miri))]

use std::collections::{BTreeMap, VecDeque};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ao_network::{
    AdvProfile, AppHello, Beacon, BeaconFlags, Channel, DisconnectReason, FRAME_VERSION,
    Fragmenter, Frame, Hello, LinkClass, LinkId, LinkRole, LocalTag, Network, NetworkConfig,
    NetworkEvent, PeerKey, ScanProfile, SimConfig, SimNode, SimTransport, SimWorld, Transport,
    TransportCaps, TransportEvent,
};

/// How often the networks are polled while settling. The simulator runs
/// every scheduled event in order whatever the step, so this only bounds
/// how late a node reacts.
const TICK: Duration = Duration::from_millis(50);
/// The poll step while the abuser runs: its writes go out once per step,
/// and at the simulator's write latency they flood at about 100 frames/s.
const FLOOD_TICK: Duration = Duration::from_millis(10);
/// Radio range, in the units of node positions.
const RADIUS: i64 = 100;
/// Long enough for a node to work through the first sightings of all its
/// neighbours: an opportunistic link holds its slot for a 60 s dwell and
/// bars its peer for another 60 s, so a node with several new neighbours
/// can need several such cycles before ordinary links settle. Seed
/// `0x118` needed the seventh cycle in a 36-node mesh.
const SETTLE: Duration = Duration::from_secs(900);
/// The fixed cases CI runs: the smallest and largest sizes, and one in
/// between.
const CI_CASES: [(u64, usize); 3] = [(0x20A, 5), (0x20B, 23), (0x20C, 50)];

/// The cases to run: CI's, or `AO_SCENARIO_SEEDS` seeded ones.
fn cases() -> Vec<(u64, usize)> {
    match std::env::var("AO_SCENARIO_SEEDS") {
        Ok(count) => {
            let count: u64 = count.parse().expect("AO_SCENARIO_SEEDS is a number");
            (0..count)
                .map(|seed| {
                    let mut rng = seed;
                    (seed, 5 + usize::try_from(next(&mut rng) % 46).unwrap())
                })
                .collect()
        }
        Err(_) => CI_CASES.to_vec(),
    }
}

/// SplitMix64: the suite's only source of randomness, seeded per case.
fn next(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A simulator radio that counts the inbound connections it is offered
/// and keeps the beacon it last advertised.
struct Watched {
    radio: SimTransport,
    inbound: usize,
    advert: Option<[u8; 16]>,
}

impl Transport for Watched {
    fn caps(&self) -> TransportCaps {
        self.radio.caps()
    }
    fn start(&mut self) {
        self.radio.start();
    }
    fn stop(&mut self) {
        self.radio.stop();
    }
    fn set_advertising(&mut self, profile: Option<AdvProfile>) {
        self.advert = profile.map(|p| p.beacon);
        self.radio.set_advertising(profile);
    }
    fn set_scanning(&mut self, profile: Option<ScanProfile>) {
        self.radio.set_scanning(profile);
    }
    fn connect(&mut self, peer: PeerKey) -> LinkId {
        self.radio.connect(peer)
    }
    fn disconnect(&mut self, link: LinkId) {
        self.radio.disconnect(link);
    }
    fn write(&mut self, link: LinkId, chunk: Vec<u8>) {
        self.radio.write(link, chunk);
    }
    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<TransportEvent> {
        let event = self.radio.poll_event(cx);
        if let Poll::Ready(TransportEvent::Connected {
            role: LinkRole::Responder,
            ..
        }) = &event
        {
            self.inbound += 1;
        }
        event
    }
}

struct Node {
    net: Network<Watched>,
    key: PeerKey,
    /// Events since the last [`Mesh::take_events`].
    events: Vec<NetworkEvent>,
}

/// A seeded random topology and the networks on it.
struct Mesh {
    case: String,
    world: SimWorld,
    nodes: Vec<Node>,
    index: BTreeMap<PeerKey, usize>,
    pos: Vec<(i64, i64)>,
    side: i64,
    /// While set, nodes either side of this x coordinate are out of range.
    cut: Option<i64>,
    rng: u64,
    max_links: usize,
    target: usize,
    /// The abusive radio, when the scenario has one.
    abuser: Option<Abuser>,
    /// The poll step.
    tick: Duration,
    /// Which pairs the world currently has in range.
    ranged: Vec<Vec<bool>>,
}

impl Mesh {
    /// `n` nodes at seeded positions, all started at once and in range of
    /// their neighbours from the first instant.
    fn new(seed: u64, n: usize) -> Self {
        let config = NetworkConfig::default();
        let world = SimWorld::new(SimConfig {
            seed,
            ..SimConfig::default()
        });
        let mut rng = seed;
        // Expected neighbours ~ n * pi * r^2 / side^2; aim for six.
        let side = ((n as f64 * std::f64::consts::PI / 6.0).sqrt() * RADIUS as f64) as i64;
        let side = side.max(RADIUS);
        let accepting = BeaconFlags {
            accepting: true,
            ..BeaconFlags::default()
        };
        let mut nodes = Vec::new();
        let mut pos = Vec::new();
        for i in 0..n {
            let radio = world.add_node(SimNode::default()).unwrap();
            let key = radio.peer_key();
            let mut net = Network::new(
                config.clone(),
                Watched {
                    radio,
                    inbound: 0,
                    advert: None,
                },
            );
            let mut tag = next(&mut rng).to_be_bytes().repeat(2);
            // Distinct tags: the first byte is the index.
            tag[0] = u8::try_from(i).unwrap();
            let tag: [u8; 16] = tag.try_into().unwrap();
            net.set_local_hello(LocalTag::new(tag), AppHello::new(Vec::new()).unwrap());
            net.set_beacon(next(&mut rng).to_be_bytes(), [0; 4], accepting);
            nodes.push(Node {
                net,
                key,
                events: Vec::new(),
            });
            let x = i64::try_from(next(&mut rng) % u64::try_from(side).unwrap()).unwrap();
            let y = i64::try_from(next(&mut rng) % u64::try_from(side).unwrap()).unwrap();
            pos.push((x, y));
        }
        let index = nodes.iter().enumerate().map(|(i, n)| (n.key, i)).collect();
        let max_links = usize::try_from(config.max_links()).unwrap();
        let mut mesh = Self {
            case: format!("seed {seed:#x}, {n} nodes"),
            world,
            nodes,
            index,
            pos,
            side,
            cut: None,
            rng,
            max_links,
            // The default policy: max_links less one explore and one
            // opportunistic slot.
            target: max_links - 2,
            abuser: None,
            tick: TICK,
            ranged: vec![vec![false; n]; n],
        };
        mesh.apply_ranges();
        mesh
    }

    fn in_range(&self, a: usize, b: usize) -> bool {
        let ((ax, ay), (bx, by)) = (self.pos[a], self.pos[b]);
        let near = (ax - bx).pow(2) + (ay - by).pow(2) <= RADIUS.pow(2);
        let split = self.cut.is_some_and(|cut| (ax < cut) != (bx < cut));
        near && !split
    }

    /// Brings the world's radio ranges in line with the positions and
    /// the cut, touching only the pairs that changed.
    fn apply_ranges(&mut self) {
        for a in 0..self.nodes.len() {
            for b in a + 1..self.nodes.len() {
                let on = self.in_range(a, b);
                if self.ranged[a][b] != on {
                    self.ranged[a][b] = on;
                    let (na, nb) = (self.radio(a), self.radio(b));
                    self.world.set_in_range(na, nb, on);
                }
            }
        }
    }

    fn radio(&self, i: usize) -> ao_network::NodeId {
        self.nodes[i].net.transport().radio.node()
    }

    /// Moves a random node to a random position.
    fn move_one(&mut self) {
        let n = u64::try_from(self.nodes.len()).unwrap();
        let side = u64::try_from(self.side).unwrap();
        let i = usize::try_from(next(&mut self.rng) % n).unwrap();
        let x = i64::try_from(next(&mut self.rng) % side).unwrap();
        let y = i64::try_from(next(&mut self.rng) % side).unwrap();
        self.pos[i] = (x, y);
        self.apply_ranges();
    }

    /// Runs for `duration`, checking the degree cap after every tick.
    fn run(&mut self, duration: Duration) {
        let mut cx = Context::from_waker(Waker::noop());
        let end = self.world.now() + duration;
        while self.world.now() < end {
            self.world.advance(self.tick);
            let now = self.world.now();
            for node in &mut self.nodes {
                while let Poll::Ready(event) = node.net.poll_event(&mut cx, now) {
                    node.events.push(event);
                }
            }
            if let Some(abuser) = &mut self.abuser {
                abuser.step();
            }
            for (i, node) in self.nodes.iter().enumerate() {
                let links = node.net.links().len();
                assert!(
                    links <= self.max_links,
                    "{}: node {i} holds {links} links at {now:?}",
                    self.case
                );
            }
        }
    }

    /// Runs `duration` with one node moved every `every`.
    fn churn(&mut self, duration: Duration, every: Duration) {
        let end = self.world.now() + duration;
        while self.world.now() < end {
            self.move_one();
            self.run(every);
        }
    }

    fn take_events(&mut self) -> Vec<Vec<NetworkEvent>> {
        self.nodes
            .iter_mut()
            .map(|node| std::mem::take(&mut node.events))
            .collect()
    }

    /// The peers of node `i`'s up links, as node indices; the abuser is
    /// left out.
    fn peers_of(&self, i: usize) -> Vec<usize> {
        self.nodes[i]
            .net
            .links()
            .iter()
            .filter_map(|info| self.index.get(&info.peer).copied())
            .collect()
    }

    fn linked(&self, a: usize, b: usize) -> bool {
        self.peers_of(a).contains(&b)
    }

    /// Every link is up at both ends, and no pair holds two.
    fn assert_symmetric(&self) {
        for a in 0..self.nodes.len() {
            let mut peers = self.peers_of(a);
            peers.sort_unstable();
            let count = peers.len();
            peers.dedup();
            assert_eq!(
                peers.len(),
                count,
                "{}: node {a} has a duplicate",
                self.case
            );
            for b in peers {
                assert!(
                    self.peers_of(b).contains(&a),
                    "{}: {a} -> {b} is up at one end only",
                    self.case
                );
            }
        }
    }

    /// Whether node `i` advertises room for another link: its own word on
    /// its slots, which also counts a cap lowered by rejections.
    fn accepting(&self, i: usize) -> bool {
        let advert = self.nodes[i].net.transport().advert;
        advert
            .and_then(|beacon| Beacon::decode(&beacon).ok())
            .is_some_and(|beacon| beacon.flags.accepting)
    }

    /// Settled, no pair in range is left unlinked while both ends have
    /// room.
    fn assert_maximal(&self) {
        for a in 0..self.nodes.len() {
            for b in a + 1..self.nodes.len() {
                if !self.in_range(a, b) || self.linked(a, b) {
                    continue;
                }
                assert!(
                    !self.accepting(a) || !self.accepting(b),
                    "{}: {a} and {b} are in range with room but unlinked",
                    self.case
                );
            }
        }
    }

    fn assert_settled(&self) {
        self.assert_symmetric();
        self.assert_maximal();
    }
}

fn downs(events: &[Vec<NetworkEvent>]) -> Vec<(usize, DisconnectReason)> {
    events
        .iter()
        .enumerate()
        .flat_map(|(i, events)| {
            events.iter().filter_map(move |event| match event {
                NetworkEvent::LinkDown { reason, .. } => Some((i, *reason)),
                _ => None,
            })
        })
        .collect()
}

fn ups(events: &[Vec<NetworkEvent>]) -> usize {
    events
        .iter()
        .flatten()
        .filter(|event| matches!(event, NetworkEvent::LinkUp { .. }))
        .count()
}

/// Every node powered at once, every neighbour in range from the first
/// instant: simultaneous connects everywhere settle into one link per
/// pair, and the mesh is maximal.
#[test]
fn a_simultaneous_start_settles_into_one_link_per_pair() {
    for (seed, n) in cases() {
        let mut mesh = Mesh::new(seed, n);
        mesh.run(SETTLE);
        mesh.assert_settled();
        // Every link the events announced is one the nodes hold.
        let events = mesh.take_events();
        let held: usize = (0..n).map(|i| mesh.nodes[i].net.links().len()).sum();
        assert_eq!(ups(&events) - downs(&events).len(), held, "{}", mesh.case);
    }
}

/// Regression for the nightly seed that exposed too short a settle:
/// six-neighbour meshes work through several opportunistic dwell and
/// cooldown cycles before the lasting topology appears. With the old
/// 300 s settle, nodes 3 and 9 in this seed were still cooling and this
/// assertion failed.
#[test]
fn a_dense_mesh_settles_after_all_opportunistic_cycles() {
    let mut mesh = Mesh::new(0x118, 36);
    mesh.run(SETTLE);
    mesh.assert_settled();
}

/// Regression for the nightly seed where a degraded-peer holder also had
/// a time-bounded opportunistic slot. Counting that slot as a stable link
/// made the eviction assertion fire even though the stable count was at
/// the target and the degraded peer was correctly kept.
#[test]
fn an_opportunistic_slot_does_not_trigger_eviction() {
    let n = 47;
    let mut mesh = Mesh::new(0x12c, n);
    mesh.run(SETTLE);
    let weak = (0..n).max_by_key(|&i| (mesh.peers_of(i).len(), i)).unwrap();
    let holders = (0..n).filter(|&i| mesh.linked(i, weak)).count();
    mesh.world.set_rssi(mesh.radio(weak), -95);
    mesh.run(Duration::from_secs(30));
    for i in (0..n).filter(|&i| mesh.linked(i, weak)) {
        let held = mesh.nodes[i]
            .net
            .snapshot(mesh.world.now())
            .links
            .iter()
            .filter(|link| link.class != LinkClass::Opportunistic)
            .count();
        assert!(held <= mesh.target, "node {i} holds {held} stable links");
    }
    assert!(holders > 0);
}

/// Degree-cap invariant: with a node moving every second for two
/// minutes, no node ever holds more than `max_links` links (checked every
/// tick), and once the churn stops the mesh settles again.
#[test]
fn the_degree_cap_holds_under_churn() {
    for (seed, n) in cases() {
        let mut mesh = Mesh::new(seed, n);
        mesh.run(Duration::from_secs(10));
        mesh.churn(Duration::from_secs(120), Duration::from_secs(1));
        mesh.run(SETTLE);
        mesh.assert_settled();
    }
}

/// Partition and heal: cut down the middle, no link crosses the cut and
/// each side settles on its own; heal, and the whole mesh settles again.
#[test]
fn a_partitioned_mesh_heals() {
    for (seed, n) in cases() {
        let mut mesh = Mesh::new(seed, n);
        mesh.run(SETTLE);
        let cut = mesh.side / 2;
        mesh.cut = Some(cut);
        mesh.apply_ranges();
        mesh.run(SETTLE);
        for a in 0..n {
            for b in mesh.peers_of(a) {
                let sides = (mesh.pos[a].0 < cut, mesh.pos[b].0 < cut);
                assert_eq!(sides.0, sides.1, "{}: {a}-{b} crosses the cut", mesh.case);
            }
        }
        mesh.assert_settled();

        mesh.cut = None;
        mesh.apply_ranges();
        mesh.run(SETTLE);
        mesh.assert_settled();
    }
}

/// Full-node avoidance: once settled, a node with no slot free advertises
/// that it is not accepting, and nobody dials it.
#[test]
fn full_nodes_are_not_dialed() {
    let mut full_seen = 0;
    for (seed, n) in cases() {
        let mut mesh = Mesh::new(seed, n);
        mesh.run(SETTLE);
        let full: Vec<(usize, usize)> = (0..n)
            .filter(|&i| !mesh.accepting(i))
            .map(|i| (i, mesh.nodes[i].net.transport().inbound))
            .collect();
        full_seen += full.len();
        mesh.run(Duration::from_secs(60));
        for (i, inbound) in full {
            assert_eq!(
                mesh.nodes[i].net.transport().inbound,
                inbound,
                "{}: full node {i} was dialed",
                mesh.case
            );
        }
    }
    assert!(full_seen > 0, "no case filled a node");
}

/// Eviction hysteresis: a settled mesh with nothing changing loses and
/// gains no links. When one node's signal falls below the floor, only
/// nodes over their stable target shed it; nodes at the target keep it.
#[test]
fn links_are_kept_until_a_degraded_peer_is_over_the_target() {
    for (seed, n) in cases() {
        let mut mesh = Mesh::new(seed, n);
        mesh.run(SETTLE);
        mesh.take_events();
        mesh.run(Duration::from_secs(120));
        let events = mesh.take_events();
        assert_eq!(downs(&events), [], "{}: a static mesh churned", mesh.case);
        assert_eq!(ups(&events), 0, "{}: a static mesh churned", mesh.case);

        // The best-connected node fades below the floor.
        let weak = (0..n).max_by_key(|&i| (mesh.peers_of(i).len(), i)).unwrap();
        let holders = (0..n).filter(|&i| mesh.linked(i, weak)).count();
        let node = mesh.radio(weak);
        mesh.world.set_rssi(node, -95);
        mesh.run(Duration::from_secs(30));
        for i in (0..n).filter(|&i| mesh.linked(i, weak)) {
            // Eviction keeps the stable count at the target. An
            // opportunistic slot is a separate, time-bounded reservation,
            // so its holder can be over the target without owing an eviction.
            let held = mesh.nodes[i]
                .net
                .snapshot(mesh.world.now())
                .links
                .iter()
                .filter(|link| link.class != LinkClass::Opportunistic)
                .count();
            assert!(
                held <= mesh.target,
                "{}: node {i} holds {held} stable links and kept the degraded peer",
                mesh.case
            );
        }
        let events = mesh.take_events();
        for (i, reason) in downs(&events) {
            assert!(
                matches!(reason, DisconnectReason::DegreeCap | DisconnectReason::Bye),
                "{}: node {i} lost a link for {reason:?}",
                mesh.case
            );
        }
        assert!(holders > 0, "{}", mesh.case);
    }
}

/// What the abusive radio does: dial a victim, introduce itself, then
/// send small frames as fast as the radio allows; when cut off, dial
/// the next victim.
struct Abuser {
    radio: SimTransport,
    victims: Vec<PeerKey>,
    turn: usize,
    link: Option<LinkId>,
    dialing: bool,
    can_write: bool,
    out: VecDeque<Vec<u8>>,
}

const ABUSER_TAG: LocalTag = LocalTag::new([0xEE; 16]);
const MTU: u16 = 247;

impl Abuser {
    fn step(&mut self) {
        let mut cx = Context::from_waker(Waker::noop());
        while let Poll::Ready(event) = self.radio.poll_event(&mut cx) {
            match event {
                TransportEvent::Connected { link, .. } => {
                    self.link = Some(link);
                    self.dialing = false;
                    self.can_write = true;
                    let hello = Hello {
                        proto_min: FRAME_VERSION,
                        proto_max: FRAME_VERSION,
                        caps: 0,
                        max_frame: 16 * 1024,
                        tag: ABUSER_TAG,
                        app_hello: AppHello::new(Vec::new()).unwrap(),
                    };
                    self.out = chunks(&Frame::Hello(hello).encode()).into();
                }
                TransportEvent::WriteReady { .. } => self.can_write = true,
                TransportEvent::Disconnected { .. } | TransportEvent::ConnectFailed { .. } => {
                    self.link = None;
                    self.dialing = false;
                }
                _ => {}
            }
        }
        let Some(link) = self.link else {
            if !self.dialing {
                let victim = self.victims[self.turn % self.victims.len()];
                self.turn += 1;
                self.radio.connect(victim);
                self.dialing = true;
            }
            return;
        };
        if self.out.is_empty() {
            let frame = Frame::Data {
                channel: Channel::Message,
                payload: vec![0xAB; 16],
            };
            self.out.extend(chunks(&frame.encode()));
        }
        if self.can_write {
            if let Some(chunk) = self.out.pop_front() {
                self.can_write = false;
                self.radio.write(link, chunk);
            }
        }
    }
}

fn chunks(frame: &[u8]) -> Vec<Vec<u8>> {
    Fragmenter::new(frame, MTU).unwrap().collect()
}

/// Abusive peer: a flooding radio takes turns at up to six victims,
/// coming back to each one again and again. Each victim cuts it off as
/// abuse at most once and refuses it every time after, and no honest
/// link anywhere goes down meanwhile.
#[test]
fn an_abusive_peer_is_cut_off_without_disturbing_the_mesh() {
    for (seed, n) in cases() {
        let mut mesh = Mesh::new(seed, n);
        mesh.run(SETTLE);
        mesh.take_events();
        let mut radio = mesh.world.add_node(SimNode::default()).unwrap();
        radio.start();
        let victims: Vec<PeerKey> = (0..n.min(6))
            .map(|i| {
                let node = mesh.radio(i);
                mesh.world.set_in_range(radio.node(), node, true);
                mesh.nodes[i].key
            })
            .collect();
        let rounds = 2 * victims.len();
        mesh.tick = FLOOD_TICK;
        mesh.abuser = Some(Abuser {
            radio,
            victims,
            turn: 0,
            link: None,
            dialing: false,
            can_write: false,
            out: VecDeque::new(),
        });
        mesh.run(Duration::from_secs(120));

        let events = mesh.take_events();
        for (i, node_events) in events.iter().enumerate() {
            let floods: Vec<LinkId> = node_events
                .iter()
                .filter_map(|event| match event {
                    NetworkEvent::LinkUp { link, remote_hello }
                        if remote_hello.tag == ABUSER_TAG =>
                    {
                        Some(*link)
                    }
                    _ => None,
                })
                .collect();
            assert!(
                floods.len() <= 1,
                "{}: node {i} took the abuser twice",
                mesh.case
            );
            for event in node_events {
                if let NetworkEvent::LinkDown { link, reason } = event {
                    assert!(
                        floods.contains(link) && *reason == DisconnectReason::AbuseDetected,
                        "{}: node {i} lost link {link:?} for {reason:?}",
                        mesh.case
                    );
                }
            }
        }
        let abuser = mesh.abuser.as_ref().unwrap();
        assert!(
            abuser.turn > rounds,
            "{}: the abuser came back only {} times",
            mesh.case,
            abuser.turn
        );
        mesh.assert_symmetric();
    }
}
