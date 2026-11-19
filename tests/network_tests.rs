//! The `Network` facade over the simulator: each command and the events it
//! produces.
#![cfg(feature = "sim")]

use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ao_network::{
    AppHello, BeaconFlags, Channel, DisconnectReason, LinkId, LocalTag, Network, NetworkConfig,
    NetworkEvent, NodeId, PeerKey, SendError, SendOpts, SimConfig, SimNode, SimTransport, SimWorld,
};

const SEED: u64 = 0x0012_5EED;
const TICK: Duration = Duration::from_millis(10);

struct Node {
    net: Network<SimTransport>,
    key: PeerKey,
    id: NodeId,
    tag: LocalTag,
    events: Vec<NetworkEvent>,
}

impl Node {
    fn hello(&mut self) {
        let app_hello = AppHello::new(vec![self.tag.as_bytes()[0]; 8]).unwrap();
        self.net.set_local_hello(self.tag, app_hello);
    }

    fn beacon(&mut self) {
        let hint = [self.tag.as_bytes()[0]; 8];
        let flags = BeaconFlags {
            accepting: true,
            ..BeaconFlags::default()
        };
        self.net.set_beacon(hint, [0; 4], flags);
    }

    /// This node's up link to `peer`.
    fn link_to(&self, peer: &Node) -> LinkId {
        let info = self.net.links().into_iter().find(|i| i.peer == peer.key);
        info.expect("linked").link
    }

    /// Events since the last call.
    fn take(&mut self) -> Vec<NetworkEvent> {
        std::mem::take(&mut self.events)
    }
}

/// Nodes all in range of each other, with no `Hello` or beacon yet.
fn nodes(seed: u64, configs: &[NetworkConfig]) -> (SimWorld, Vec<Node>) {
    let world = SimWorld::new(SimConfig {
        seed,
        ..SimConfig::default()
    });
    let mut nodes: Vec<Node> = configs
        .iter()
        .zip(1u8..)
        .map(|(config, i)| {
            let radio = world.add_node(SimNode::default()).unwrap();
            let (key, id) = (radio.peer_key(), radio.node());
            Node {
                net: Network::new(config.clone(), radio),
                key,
                id,
                tag: LocalTag::new([i; 16]),
                events: Vec::new(),
            }
        })
        .collect();
    for a in 0..nodes.len() {
        for b in a + 1..nodes.len() {
            world.set_in_range(nodes[a].id, nodes[b].id, true);
        }
    }
    for node in &mut nodes {
        run(&world, std::slice::from_mut(node), TICK);
    }
    (world, nodes)
}

fn run(world: &SimWorld, nodes: &mut [Node], duration: Duration) {
    let mut cx = Context::from_waker(Waker::noop());
    let end = world.now() + duration;
    while world.now() < end {
        world.advance(TICK);
        for node in nodes.iter_mut() {
            while let Poll::Ready(event) = node.net.poll_event(&mut cx, world.now()) {
                node.events.push(event);
            }
        }
    }
}

fn data(link: LinkId, channel: Channel, payload: &[u8]) -> NetworkEvent {
    NetworkEvent::Data {
        link,
        channel,
        payload: payload.to_vec(),
    }
}

/// Every command in turn, with the events each must produce. Returns the
/// whole event trace.
fn command_scenario(seed: u64) -> Vec<Vec<NetworkEvent>> {
    let (world, mut n) = nodes(
        seed,
        &[
            NetworkConfig::default(),
            NetworkConfig::default(),
            NetworkConfig::default(),
        ],
    );
    let mut trace = Vec::new();
    let second = Duration::from_secs(1);

    // A Hello alone is not enough to be found.
    for node in &mut n {
        node.hello();
    }
    run(&world, &mut n, second);
    for node in &mut n {
        assert!(node.take().is_empty());
        assert!(node.net.links().is_empty());
    }

    // With beacons every pair links up once, seeing the other's Hello.
    for node in &mut n {
        node.beacon();
    }
    run(&world, &mut n, 2 * second);
    let tags: Vec<LocalTag> = n.iter().map(|node| node.tag).collect();
    for (i, node) in n.iter_mut().enumerate() {
        let events = node.take();
        let mut seen: Vec<LocalTag> = events
            .iter()
            .map(|event| match event {
                NetworkEvent::LinkUp { remote_hello, .. } => remote_hello.tag,
                other => panic!("unexpected {other:?}"),
            })
            .collect();
        seen.sort();
        let others: Vec<LocalTag> = (0..3).filter(|&j| j != i).map(|j| tags[j]).collect();
        assert_eq!(seen, others, "seed {seed:#x}");
        assert_eq!(node.net.links().len(), 2);
        trace.push(events);
    }

    // send: Data at that peer only.
    let (a_to_b, b_to_a) = (n[0].link_to(&n[1]), n[1].link_to(&n[0]));
    n[0].net
        .send(world.now(), a_to_b, Channel::Message, b"hi B".to_vec())
        .unwrap();
    run(&world, &mut n, second);
    assert_eq!(n[1].take(), [data(b_to_a, Channel::Message, b"hi B")]);
    assert!(n[0].take().is_empty() && n[2].take().is_empty());

    // broadcast: Data at every peer.
    assert!(
        n[2].net
            .broadcast(world.now(), Channel::Bulk, b"hi all")
            .is_empty()
    );
    run(&world, &mut n, second);
    let (a_to_c, b_to_c) = (n[0].link_to(&n[2]), n[1].link_to(&n[2]));
    assert_eq!(n[0].take(), [data(a_to_c, Channel::Bulk, b"hi all")]);
    assert_eq!(n[1].take(), [data(b_to_c, Channel::Bulk, b"hi all")]);

    // A link that ends is reported down on both sides and refuses sends;
    // the freed slot shows in both beacons, so the pair links up again.
    // The redial waits out the drop's backoff (1-2 s) and then the next
    // scan refresh of the unchanged beacon (1 s), so give it 4 s.
    world.kill_link(n[0].id, a_to_b);
    run(&world, &mut n, 4 * second);
    let lost = |link| NetworkEvent::LinkDown {
        link,
        reason: DisconnectReason::TransportLost,
    };
    for (node, old, peer) in [(0, a_to_b, 1), (1, b_to_a, 0)] {
        let events = n[node].take();
        assert_eq!(events[0], lost(old), "seed {seed:#x}");
        let new = n[node].link_to(&n[peer]);
        assert!(matches!(&events[1..], [NetworkEvent::LinkUp { link, .. }] if *link == new));
        assert_ne!(new, old);
        trace.push(events);
    }
    assert_eq!(
        n[0].net
            .send(world.now(), a_to_b, Channel::Message, vec![7]),
        Err(SendError::LinkClosed(vec![7]))
    );
    trace
}

#[test]
fn commands_produce_their_documented_events() {
    let trace = command_scenario(SEED);
    assert_eq!(trace, command_scenario(SEED), "seed {SEED:#x}");
}

#[test]
fn payloads_over_the_peers_max_frame_come_back() {
    let small = NetworkConfig::default().with_max_frame(64).unwrap();
    let (world, mut n) = nodes(SEED, &[NetworkConfig::default(), small]);
    for node in &mut n {
        node.hello();
        node.beacon();
    }
    run(&world, &mut n, Duration::from_secs(2));
    let link = n[0].link_to(&n[1]);
    // A 60-byte payload is a 64-byte frame; one more is too many.
    assert_eq!(
        n[0].net
            .send(world.now(), link, Channel::Message, vec![1; 61]),
        Err(SendError::TooLarge(vec![1; 61]))
    );
    n[0].net
        .send(world.now(), link, Channel::Message, vec![1; 60])
        .unwrap();
    run(&world, &mut n, Duration::from_secs(1));
    let back = n[1].link_to(&n[0]);
    assert_eq!(
        n[1].take().last(),
        Some(&data(back, Channel::Message, &[1; 60]))
    );
    // links() reports each link's metrics; nothing is in flight now.
    let sent = n[0].net.links()[0].metrics;
    let got = n[1].net.links()[0].metrics;
    assert!(sent.tx_bytes > 64);
    assert_eq!(sent.tx_bytes, got.rx_bytes);
}

#[test]
fn a_full_node_takes_no_more_links() {
    let one = NetworkConfig::default().with_max_links(1).unwrap();
    let (world, mut n) = nodes(
        SEED,
        &[one, NetworkConfig::default(), NetworkConfig::default()],
    );
    for node in &mut n {
        node.hello();
        node.beacon();
    }
    run(&world, &mut n, Duration::from_secs(3));
    assert_eq!(n[0].net.links().len(), 1, "seed {SEED:#x}");
    // The two others still link to each other; nothing ever went down.
    assert!(n[1].net.links().iter().any(|i| i.peer == n[2].key));
    for node in &mut n {
        let downs = node.take();
        assert!(
            !downs
                .iter()
                .any(|e| matches!(e, NetworkEvent::LinkDown { .. }))
        );
    }
}

fn ready_pair() -> (SimWorld, Vec<Node>) {
    let (world, mut n) = nodes(SEED, &[NetworkConfig::default(), NetworkConfig::default()]);
    for node in &mut n {
        node.hello();
        node.beacon();
    }
    run(&world, &mut n, Duration::from_secs(2));
    for node in &mut n {
        assert_eq!(node.net.links().len(), 1);
        node.take();
        // Keep closure observable without the discovery policy reconnecting.
        node.net
            .set_beacon([node.tag.as_bytes()[0]; 8], [0; 4], BeaconFlags::default());
    }
    (world, n)
}

#[test]
fn disconnect_reports_local_reason_and_peer_bye_once() {
    let (world, mut n) = ready_pair();
    let (a, b) = (n[0].link_to(&n[1]), n[1].link_to(&n[0]));
    n[0].net
        .disconnect(world.now(), a, DisconnectReason::Closed);
    assert_eq!(
        n[0].net.send(world.now(), a, Channel::Message, vec![7]),
        Err(SendError::LinkClosed(vec![7]))
    );
    run(&world, &mut n, Duration::from_secs(1));
    assert_eq!(
        n[0].take(),
        [NetworkEvent::LinkDown {
            link: a,
            reason: DisconnectReason::Closed
        }]
    );
    assert_eq!(
        n[1].take(),
        [NetworkEvent::LinkDown {
            link: b,
            reason: DisconnectReason::Bye
        }]
    );
    assert!(n.iter().all(|node| node.net.links().is_empty()));
    n[0].net
        .disconnect(world.now(), a, DisconnectReason::ProtocolViolation);
    run(&world, &mut n, Duration::from_secs(1));
    assert!(n.iter_mut().all(|node| node.take().is_empty()));
}

#[test]
fn repeated_disconnect_preserves_the_first_reason() {
    let (world, mut n) = ready_pair();
    let a = n[0].link_to(&n[1]);
    n[0].net
        .disconnect(world.now(), a, DisconnectReason::ProtocolViolation);
    n[0].net
        .disconnect(world.now(), a, DisconnectReason::Closed);
    run(&world, &mut n, Duration::from_secs(1));
    assert_eq!(
        n[0].take(),
        [NetworkEvent::LinkDown {
            link: a,
            reason: DisconnectReason::ProtocolViolation
        }]
    );
    assert_eq!(n[1].take().len(), 1);
}

#[test]
fn disconnecting_an_unknown_link_does_not_interrupt_delivery() {
    let (world, mut n) = ready_pair();
    let (a, b) = (n[0].link_to(&n[1]), n[1].link_to(&n[0]));
    n[0].net
        .disconnect(world.now(), LinkId::new(u64::MAX), DisconnectReason::Closed);
    n[0].net
        .send(world.now(), a, Channel::Message, b"still linked".to_vec())
        .unwrap();
    run(&world, &mut n, Duration::from_secs(1));
    assert!(n[0].take().is_empty());
    assert_eq!(n[1].take(), [data(b, Channel::Message, b"still linked")]);
}

/// NET-21 (Done when): queueing fifty updates with one coalesce key to a
/// link that is still draining a bulk frame sends only the newest one.
#[test]
fn fifty_keyed_updates_to_a_slow_link_send_only_the_newest() {
    let (world, mut n) = ready_pair();
    let a = n[0].link_to(&n[1]);
    // Saturate the link: the bulk frame keeps the transport busy, so
    // nothing newly queued can be written while it drains.
    n[0].net
        .send(world.now(), a, Channel::Bulk, vec![0xB0; 2000])
        .unwrap();
    let opts = SendOpts {
        coalesce_key: Some([9; 16]),
        expires_at: None,
    };
    for update in 0..50u8 {
        n[0].net
            .send_opts(world.now(), a, Channel::Telemetry, vec![update], opts)
            .unwrap();
    }
    run(&world, &mut n, Duration::from_secs(2));
    let telemetry: Vec<Vec<u8>> = n[1]
        .take()
        .into_iter()
        .filter_map(|event| match event {
            NetworkEvent::Data {
                channel: Channel::Telemetry,
                payload,
                ..
            } => Some(payload),
            _ => None,
        })
        .collect();
    assert_eq!(telemetry, [vec![49]]);
}

/// NET-15 (Done when): a dropped link is not redialled before the
/// peer's backoff passes, however often both nodes keep seeing each
/// other, and it does come back once the backoff and the next scan
/// refresh have gone by.
#[test]
fn a_dropped_link_is_not_redialled_before_its_backoff() {
    let (world, mut n) = nodes(SEED, &[NetworkConfig::default(), NetworkConfig::default()]);
    for node in &mut n {
        node.hello();
        node.beacon();
    }
    run(&world, &mut n, Duration::from_secs(2));
    assert_eq!(n[0].net.links().len(), 1);
    let link = n[0].link_to(&n[1]);
    world.kill_link(n[0].id, link);
    // One failure waits at least 1 s, jitter included, before the next
    // attempt; both nodes advertise and see each other the whole time.
    run(&world, &mut n, Duration::from_millis(500));
    assert!(
        n[0].net.links().is_empty() && n[1].net.links().is_empty(),
        "redialled before the backoff passed"
    );
    // By then the backoff (at most 2 s) and the next 1 s scan refresh
    // of the unchanged beacon have both gone by.
    run(&world, &mut n, Duration::from_secs(4));
    assert_eq!(
        n[0].net.links().len(),
        1,
        "never redialled after the backoff"
    );
    assert_eq!(n[1].net.links().len(), 1);
}

/// A world where only the pairs `(last node, peer)` are in range and
/// every peer's beacon hint sorts below the last node's, so the peers
/// dial it and it admits them as inbound links.
fn a_with_peers(radios: &[SimNode]) -> (SimWorld, Vec<Node>) {
    let world = SimWorld::new(SimConfig {
        seed: SEED,
        ..SimConfig::default()
    });
    let nodes: Vec<Node> = radios
        .iter()
        .enumerate()
        .map(|(i, radio)| {
            let radio = world.add_node(*radio).unwrap();
            let (key, id) = (radio.peer_key(), radio.node());
            let tag = (i + 1) as u8;
            let mut node = Node {
                net: Network::new(NetworkConfig::default(), radio),
                key,
                id,
                tag: LocalTag::new([tag; 16]),
                events: Vec::new(),
            };
            node.hello();
            node.beacon();
            node
        })
        .collect();
    let a = nodes.len() - 1;
    for peer in 0..a {
        world.set_in_range(nodes[a].id, nodes[peer].id, true);
    }
    (world, nodes)
}

/// NET-16: four links is one over the stable target, but a link whose
/// peer is still strong is never shed - the hysteresis that stops a
/// node from churning a healthy neighbourhood.
#[test]
fn links_over_the_target_survive_while_strong() {
    let (world, mut nodes) = a_with_peers(&[SimNode::default(); 5]);
    run(&world, &mut nodes, Duration::from_secs(2));
    let a = nodes.len() - 1;
    assert_eq!(
        nodes[a].net.links().len(),
        4,
        "four links, one over the stable target"
    );
    nodes[a].take();
    // Long past the minimum link age, with every scan refresh firing
    // the eviction check again.
    run(&world, &mut nodes, Duration::from_secs(20));
    assert_eq!(
        nodes[a].net.links().len(),
        4,
        "a strong link was shed anyway"
    );
    assert!(
        !nodes[a]
            .take()
            .into_iter()
            .any(|event| matches!(event, NetworkEvent::LinkDown { .. })),
        "a strong link was shed anyway"
    );
}

/// NET-16: the same four links, one of them to a peer that then drifts
/// below the admission floor: once the node is over its target and the
/// link is old enough, only that one goes, and it goes with DegreeCap
/// after a clean Bye, not as a silent drop.
#[test]
fn a_degraded_link_over_the_target_is_evicted() {
    // Four strong peers and A; the last peer is degraded later.
    let (world, mut nodes) = a_with_peers(&[SimNode::default(); 5]);
    run(&world, &mut nodes, Duration::from_secs(2));
    let (w, a) = (3, nodes.len() - 1);
    assert_eq!(nodes[a].net.links().len(), 4);
    let w_link = nodes[a].link_to(&nodes[w]);
    nodes[a].take();

    // W's signal sinks below the floor; the scan refreshes carry the
    // new reading into the table until the average crosses it.
    world.set_rssi(nodes[w].id, -95);
    run(&world, &mut nodes, Duration::from_secs(16));
    let downs: Vec<(LinkId, DisconnectReason)> = nodes[a]
        .take()
        .into_iter()
        .filter_map(|event| match event {
            NetworkEvent::LinkDown { link, reason } => Some((link, reason)),
            _ => None,
        })
        .collect();
    assert_eq!(downs, [(w_link, DisconnectReason::DegreeCap)]);
    assert_eq!(
        nodes[a].net.links().len(),
        3,
        "only the degraded link was shed"
    );
    // The survivors are still there by peer, not just by count.
    for peer in 0..nodes.len() - 1 {
        if peer == w {
            continue;
        }
        let link = nodes[a].link_to(&nodes[peer]);
        assert_ne!(link, w_link);
    }
    // And the weak peer cannot walk straight back in: its backoff
    // passes within this window, so it does retry, but the floor
    // refuses the inbound again.
    run(&world, &mut nodes, Duration::from_secs(4));
    assert!(
        !nodes[a]
            .take()
            .into_iter()
            .any(|event| matches!(event, NetworkEvent::LinkUp { .. })),
        "the evicted peer was linked again while still weak"
    );
    assert_eq!(nodes[a].net.links().len(), 3);
}

/// NET-16: eviction only runs under pressure - at the stable target a
/// link that has degraded below the floor is kept, because shedding it
/// would only cost a connection the node has room for.
#[test]
fn a_degraded_link_is_kept_while_at_the_target() {
    // Two strong peers, W (to be degraded later), then A.
    let (world, mut nodes) = a_with_peers(&[SimNode::default(); 4]);
    run(&world, &mut nodes, Duration::from_secs(2));
    let (w, a) = (2, nodes.len() - 1);
    assert_eq!(nodes[a].net.links().len(), 3, "three links: at the target");
    nodes[a].take();

    world.set_rssi(nodes[w].id, -95);
    run(&world, &mut nodes, Duration::from_secs(16));
    assert_eq!(
        nodes[a].net.links().len(),
        3,
        "the degraded link was shed with room to spare"
    );
    nodes[a].link_to(&nodes[w]);
    assert!(
        !nodes[a]
            .take()
            .into_iter()
            .any(|event| matches!(event, NetworkEvent::LinkDown { .. }))
    );
}

/// NET-16: the floor binds inbound links - a peer the node would never
/// dial is not one it holds, whatever order the sighting and the
/// connection arrive in.
#[test]
fn a_peer_below_the_floor_is_not_admitted() {
    let mut radios = vec![SimNode::default(); 5];
    radios[3] = SimNode {
        rssi: -95,
        ..SimNode::default()
    };
    let (world, mut nodes) = a_with_peers(&radios);
    run(&world, &mut nodes, Duration::from_secs(3));
    let (w, a) = (3, nodes.len() - 1);
    assert_eq!(
        nodes[a].net.links().len(),
        3,
        "the weak peer was admitted inbound"
    );
    assert!(nodes[w].net.links().is_empty(), "the weak peer got a link");
}
