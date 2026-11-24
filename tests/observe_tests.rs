//! Observability (NET-23): the counters and the snapshot. The log test
//! lives alone in `observe_log.rs`.
#![cfg(feature = "sim")]

use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ao_network::{
    AppHello, BeaconFlags, Channel, DisconnectReason, DiscoveryMode, FRAME_VERSION, Fragmenter,
    Frame, Hello, LinkClass, LinkRole, LinkState, LocalTag, Network, NetworkConfig, QueueDrops,
    SendError, SendOpts, SendQueue, SimConfig, SimNode, SimTransport, SimWorld, Transport,
    TransportEvent,
};

const TICK: Duration = Duration::from_millis(10);

/// Two nodes, A (hint 1, so it dials) and B, in range of each other.
fn pair() -> (SimWorld, Vec<Network<SimTransport>>) {
    let world = SimWorld::new(SimConfig::default());
    let accepting = BeaconFlags {
        accepting: true,
        ..BeaconFlags::default()
    };
    let nets: Vec<Network<SimTransport>> = (1..=2u8)
        .map(|i| {
            let radio = world.add_node(SimNode::default()).unwrap();
            let mut net = Network::new(NetworkConfig::default(), radio);
            net.set_local_hello(LocalTag::new([i; 16]), AppHello::new(Vec::new()).unwrap());
            net.set_beacon([i; 8], [0; 4], accepting);
            net
        })
        .collect();
    let (a, b) = (nets[0].transport().node(), nets[1].transport().node());
    world.set_in_range(a, b, true);
    (world, nets)
}

fn run(world: &SimWorld, nets: &mut [Network<SimTransport>], duration: Duration) {
    let mut cx = Context::from_waker(Waker::noop());
    let end = world.now() + duration;
    while world.now() < end {
        world.advance(TICK);
        for net in nets.iter_mut() {
            while net.poll_event(&mut cx, world.now()).is_ready() {}
        }
    }
}

/// The counters follow the lifecycle at both ends: one link up, its end
/// by reason, and a send refused for a full queue.
#[test]
fn counters_follow_the_lifecycle() {
    let (world, mut nets) = pair();
    run(&world, &mut nets, Duration::from_secs(1));
    let link = nets[0].links()[0].link;

    // Fill the Control channel past its cap: the write in flight takes
    // one payload, the rest wait, and the last is refused.
    let now = world.now();
    let cap = NetworkConfig::default().channel_cap(Channel::Control);
    let mut refused = 0;
    for _ in 0..3 {
        if let Err(SendError::QueueFull(_)) = nets[0].send(
            now,
            link,
            Channel::Control,
            vec![0; cap - SendQueue::ENTRY_COST - 8],
        ) {
            refused += 1;
        }
    }
    assert!(refused > 0);
    let snapshot = nets[0].snapshot(now);
    assert_eq!(snapshot.counters.queue_full, refused);
    assert_eq!(snapshot.counters.links_up, 1);
    assert_eq!(snapshot.links.len(), 1);
    let only = snapshot.links[0];
    assert_eq!(
        (only.link, only.role, only.class, only.state),
        (
            link,
            LinkRole::Initiator,
            LinkClass::Stable,
            LinkState::Ready
        )
    );
    assert!(only.queued_bytes > 0);
    assert_eq!(snapshot.mode, DiscoveryMode::Aggressive);
    assert_eq!((snapshot.peers, snapshot.blocked), (1, 0));
    assert_eq!(snapshot.at, now);

    nets[0].disconnect(now, link, DisconnectReason::DegreeCap);
    run(&world, &mut nets, Duration::from_secs(2));
    let a = nets[0].snapshot(world.now()).counters;
    let b = nets[1].snapshot(world.now()).counters;
    assert_eq!((a.links_down.degree_cap, a.links_down.total()), (1, 1));
    assert_eq!((b.links_down.bye, b.links_down.total()), (1, 1));
    assert_eq!(b.links_up, 1);
}

/// Every kind of queue drop is counted, and the network keeps a link's
/// drops after the link has ended.
#[test]
fn queue_drops_are_counted_and_outlive_their_link() {
    let mut queue = SendQueue::new(&NetworkConfig::default());
    let expiring = SendOpts {
        expires_at: Some(Duration::from_secs(1)),
        ..SendOpts::default()
    };
    queue
        .push_opts(Channel::Message, expiring, vec![1; 8])
        .unwrap();
    let keyed = SendOpts {
        coalesce_key: Some([7; 16]),
        ..SendOpts::default()
    };
    queue
        .push_opts(Channel::Message, keyed, vec![2; 8])
        .unwrap();
    queue
        .push_opts(Channel::Message, keyed, vec![3; 8])
        .unwrap();
    let telemetry = NetworkConfig::default().channel_cap(Channel::Telemetry);
    queue
        .push(Channel::Telemetry, vec![4; telemetry / 2])
        .unwrap();
    queue
        .push(Channel::Telemetry, vec![5; telemetry / 2])
        .unwrap();
    queue.prune(Duration::from_secs(1));
    let drops = queue.drops();
    assert_eq!(
        (drops.expired, drops.evicted, drops.coalesced),
        (1, 1, 1),
        "{drops:?}"
    );

    // Through a network: a frame expires on a link, then the link ends.
    let (world, mut nets) = pair();
    run(&world, &mut nets, Duration::from_secs(1));
    let link = nets[0].links()[0].link;
    let now = world.now();
    let soon = SendOpts {
        expires_at: Some(now + Duration::from_millis(50)),
        ..SendOpts::default()
    };
    // The first payload goes out at once; the second waits and expires.
    for _ in 0..2 {
        nets[0]
            .send_opts(now, link, Channel::Bulk, vec![9; 4000], soon)
            .unwrap();
    }
    run(&world, &mut nets, Duration::from_millis(100));
    let live = nets[0].snapshot(world.now()).counters.queue_drops;
    assert_eq!(live.expired, 1, "{live:?}");
    let now = world.now();
    nets[0].disconnect(now, link, DisconnectReason::Closed);
    run(&world, &mut nets, Duration::from_secs(1));
    let after = nets[0].snapshot(world.now());
    assert!(after.links.is_empty());
    assert_eq!(
        after.counters.queue_drops, live,
        "kept after the link ended"
    );
    assert_eq!(QueueDrops::default().expired, 0);
}

/// A peer that keeps sending malformed frames is struck each time, then
/// blacklisted, and the snapshot shows it blocked.
#[test]
fn strikes_and_blacklisting_are_counted() {
    let (world, mut nets) = pair();
    let mut radio = world.add_node(SimNode::default()).unwrap();
    radio.start();
    world.set_in_range(radio.node(), nets[1].transport().node(), true);
    let victim = nets[1].transport().peer_key();
    let hello = Hello {
        proto_min: FRAME_VERSION,
        proto_max: FRAME_VERSION,
        caps: 0,
        max_frame: 16 * 1024,
        tag: LocalTag::new([0xEE; 16]),
        app_hello: AppHello::new(Vec::new()).unwrap(),
    };
    let mut script: Vec<Vec<u8>> = Fragmenter::new(&Frame::Hello(hello).encode(), 247)
        .unwrap()
        .collect();
    // Version 0xFF: no such frame layout.
    script.extend(Fragmenter::new(&[0xFF, 0x01, 0x00], 247).unwrap());

    let mut cx = Context::from_waker(Waker::noop());
    let mut pending: Vec<Vec<u8>> = Vec::new();
    let mut link = None;
    let mut can_write = false;
    run(&world, &mut nets, Duration::from_secs(1));
    for _ in 0..1_000 {
        while let Poll::Ready(event) = radio.poll_event(&mut cx) {
            match event {
                TransportEvent::Connected { link: l, .. } => {
                    link = Some(l);
                    can_write = true;
                    pending = script.iter().rev().cloned().collect();
                }
                TransportEvent::WriteReady { .. } => can_write = true,
                TransportEvent::Disconnected { .. } | TransportEvent::ConnectFailed { .. } => {
                    link = None;
                }
                _ => {}
            }
        }
        match link {
            None => {
                link = Some(radio.connect(victim));
                can_write = false;
            }
            Some(l) if can_write => {
                if let Some(chunk) = pending.pop() {
                    can_write = false;
                    radio.write(l, chunk);
                }
            }
            Some(_) => {}
        }
        run(&world, &mut nets, TICK);
    }
    let snapshot = nets[1].snapshot(world.now());
    let counters = snapshot.counters;
    assert_eq!(counters.strikes, 3, "{counters:?}");
    assert_eq!(counters.blacklisted, 1, "{counters:?}");
    assert_eq!(counters.links_down.protocol_violation, 3, "{counters:?}");
    assert_eq!(snapshot.blocked, 1);
    // Nothing of the abuse reached A.
    let a = nets[0].snapshot(world.now()).counters;
    assert_eq!((a.strikes, a.blacklisted), (0, 0));
}

#[cfg(feature = "serde")]
fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            use std::fmt::Write as _;
            write!(out, "{byte:02x}").expect("writing to a String cannot fail");
            out
        })
}

/// Done when: the snapshot serializes cleanly, with every field under a
/// readable name.
#[cfg(feature = "serde")]
#[test]
fn a_snapshot_serializes() {
    let (world, mut nets) = pair();
    run(&world, &mut nets, Duration::from_secs(1));
    let snapshot = nets[0].snapshot(world.now());
    let json = serde_json::to_value(&snapshot).unwrap();
    println!("{}", serde_json::to_string_pretty(&json).unwrap());

    assert_eq!(json["mode"], "Aggressive");
    assert_eq!(json["peers"], 1);
    assert_eq!(json["blocked"], 0);
    assert_eq!(json["counters"]["links_up"], 1);
    assert_eq!(json["counters"]["links_down"]["degree_cap"], 0);
    assert_eq!(json["counters"]["queue_drops"]["expired"], 0);
    let link = &json["links"][0];
    assert_eq!(link["link"], 1);
    assert_eq!(link["role"], "Initiator");
    assert_eq!(link["class"], "Stable");
    assert_eq!(link["state"], "Ready");
    let peer = hex(nets[1].transport().peer_key().as_bytes());
    assert_eq!(link["peer"], peer.as_str());
    assert!(link["metrics"]["tx_bytes"].as_u64().unwrap() > 0);
    assert_eq!(link["queued_bytes"], 0);
}
