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
    world.kill_link(n[0].id, a_to_b);
    run(&world, &mut n, second);
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
    n[0].net.disconnect(a, DisconnectReason::Closed);
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
    n[0].net.disconnect(a, DisconnectReason::ProtocolViolation);
    run(&world, &mut n, Duration::from_secs(1));
    assert!(n.iter_mut().all(|node| node.take().is_empty()));
}

#[test]
fn repeated_disconnect_preserves_the_first_reason() {
    let (world, mut n) = ready_pair();
    let a = n[0].link_to(&n[1]);
    n[0].net.disconnect(a, DisconnectReason::ProtocolViolation);
    n[0].net.disconnect(a, DisconnectReason::Closed);
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
        .disconnect(LinkId::new(u64::MAX), DisconnectReason::Closed);
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
