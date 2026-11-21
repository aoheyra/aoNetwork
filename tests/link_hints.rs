//! Advisory per-peer link hints (NET-16, issue 64): bounded advice from the
//! layer above, applied by aoNetwork without changing the degree cap.
#![cfg(feature = "sim")]

use std::task::{Context, Waker};
use std::time::Duration;

use ao_network::{
    AppHello, BeaconFlags, LinkHintPreference, LocalTag, Network, NetworkConfig, NodeId, PeerKey,
    SimConfig, SimNode, SimTransport, SimWorld,
};

const SEED: u64 = 0x0004_81D7;
const TICK: Duration = Duration::from_millis(10);

struct Node {
    net: Network<SimTransport>,
    key: PeerKey,
    id: NodeId,
}

/// Nodes with equal beacon hints, so the ordinary initiation delay holds
/// every dial for four seconds. `prefer` applies to node 0's advice to
/// `contact`; `extra` adds a second, stronger peer for the one-slot case.
fn world_with(capacity: u32, prefer_contact: bool, prefer_stranger: bool) -> (SimWorld, Vec<Node>) {
    let world = SimWorld::new(SimConfig {
        seed: SEED,
        ..SimConfig::default()
    });
    let config = NetworkConfig::default().with_max_links(capacity).unwrap();
    let accepting = BeaconFlags {
        accepting: true,
        ..BeaconFlags::default()
    };
    let radios = [
        SimNode::default(),
        SimNode::default(),
        SimNode {
            rssi: -80,
            ..SimNode::default()
        },
    ];
    let mut nodes: Vec<Node> = radios
        .into_iter()
        .enumerate()
        .map(|(i, radio)| {
            let radio = world.add_node(radio).unwrap();
            let key = radio.peer_key();
            let id = radio.node();
            let mut net = Network::new(config.clone(), radio);
            let tag = LocalTag::new([(i + 1) as u8; 16]);
            net.set_local_hello(tag, AppHello::new(Vec::new()).unwrap());
            net.set_beacon([9; 8], [0; 4], accepting);
            Node { net, key, id }
        })
        .collect();
    let (contact, stranger) = (nodes[2].key, nodes[1].key);
    if prefer_contact {
        nodes[0]
            .net
            .set_link_hint(contact, LinkHintPreference::Prefer);
    }
    if prefer_stranger {
        nodes[0]
            .net
            .set_link_hint(stranger, LinkHintPreference::Prefer);
    }
    // Only node 0 is in range of the other two; they never discover each
    // other, so the two dials really do compete for node 0's slots.
    world.set_in_range(nodes[0].id, nodes[1].id, true);
    world.set_in_range(nodes[0].id, nodes[2].id, true);
    (world, nodes)
}

fn settle(world: &SimWorld, nodes: &mut [Node]) {
    let mut cx = Context::from_waker(Waker::noop());
    for node in nodes {
        while node.net.poll_event(&mut cx, world.now()).is_ready() {}
    }
}

/// Runs at a fixed tick, optionally checking the degree cap on every step.
fn run(world: &SimWorld, nodes: &mut [Node], duration: Duration, cap: Option<usize>) {
    let end = world.now() + duration;
    while world.now() < end {
        world.advance(TICK);
        settle(world, nodes);
        if let Some(cap) = cap {
            for node in &mut *nodes {
                assert!(
                    node.net.links().len() <= cap,
                    "hint bypassed the cap at {:?}",
                    world.now()
                );
            }
        }
    }
}

/// When the link to `peer` came up on `node`, if it is up.
fn up_at(node: &Node, peer: PeerKey) -> Option<Duration> {
    node.net
        .links()
        .into_iter()
        .find(|info| info.peer == peer)
        .map(|info| info.up_at)
}

/// With no advice, two peers with equal beacon hints both wait four seconds.
/// Advising `Prefer` for the contact makes this node dial at once, so the
/// contact is linked four seconds sooner with the same seed.
#[test]
fn a_hinted_contact_links_sooner_with_the_same_seed() {
    let mut off = world_with(5, false, false);
    let mut on = world_with(5, true, false);
    settle(&off.0, &mut off.1);
    settle(&on.0, &mut on.1);

    run(&off.0, &mut off.1, Duration::from_secs(8), None);
    run(&on.0, &mut on.1, Duration::from_secs(8), None);

    let contact = off.1[2].key;
    let unhinted = up_at(&off.1[0], contact).expect("the contact links eventually");
    let hinted = up_at(&on.1[0], contact).expect("the hinted contact links");
    assert!(
        hinted + Duration::from_secs(3) < unhinted,
        "hints {hinted:?}, no hints {unhinted:?}, seed {SEED:#x}"
    );
}

/// With one slot and two peers in range, no advice leaves the contact to
/// wait for the four-second race. Advising the weaker contact links it at
/// once, without opening a second link or raising the cap.
#[test]
fn a_hinted_contact_takes_the_only_slot_first() {
    let mut off = world_with(1, false, false);
    let mut on = world_with(1, true, false);
    settle(&off.0, &mut off.1);
    settle(&on.0, &mut on.1);

    run(&off.0, &mut off.1, Duration::from_secs(8), Some(1));
    run(&on.0, &mut on.1, Duration::from_secs(8), Some(1));

    let (contact, stranger) = (off.1[2].key, off.1[1].key);
    let off_contact = up_at(&off.1[0], contact).expect("unhinted contact links eventually");
    let on_contact = up_at(&on.1[0], contact).expect("hinted contact links");
    assert_eq!(up_at(&off.1[0], stranger), None, "the slot is still one");
    assert!(
        on_contact + Duration::from_secs(3) < off_contact,
        "hinted {on_contact:?}, unhinted {off_contact:?}, seed {SEED:#x}"
    );
    assert_eq!(up_at(&on.1[0], stranger), None, "the slot is still one");
    assert_eq!(on.1[0].net.links().len(), 1);
    assert_eq!(on.1[0].net.link_hint(contact), LinkHintPreference::Prefer);
}

/// Advice on several peers, including ones already linked, never lets the
/// node hold more links than `max_links` at any point of the run.
#[test]
fn hints_never_bypass_the_degree_cap() {
    let (world, mut nodes) = world_with(1, true, true);
    settle(&world, &mut nodes);
    run(&world, &mut nodes, Duration::from_secs(12), Some(1));
    assert_eq!(nodes[0].net.snapshot(world.now()).hints, 2);
}

/// The facade bounds advice by `max_peers`, removes it explicitly or by
/// setting `Neutral`, and exposes what is held for diagnostics.
#[test]
fn hint_bounds_and_removal_on_the_facade() {
    let world = SimWorld::new(SimConfig::default());
    let radio = world.add_node(SimNode::default()).unwrap();
    let config = NetworkConfig::default().with_max_peers(2).unwrap();
    let mut net = Network::new(config, radio);
    let (low, high, newcomer) = (
        PeerKey::new([1; 16]),
        PeerKey::new([2; 16]),
        PeerKey::new([3; 16]),
    );
    net.set_link_hint(low, LinkHintPreference::Prefer);
    net.set_link_hint(high, LinkHintPreference::Deprioritize);
    assert_eq!(net.snapshot(Duration::ZERO).hints, 2);
    net.set_link_hint(newcomer, LinkHintPreference::Prefer);
    assert_eq!(net.link_hint(high), LinkHintPreference::Neutral);
    assert_eq!(net.link_hints().len(), 2);

    net.set_link_hint(newcomer, LinkHintPreference::Neutral);
    assert_eq!(net.link_hint(newcomer), LinkHintPreference::Neutral);
    assert!(net.clear_link_hint(low));
    assert!(!net.clear_link_hint(low));
    assert_eq!(net.snapshot(Duration::ZERO).hints, 0);
}
