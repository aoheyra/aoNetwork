//! SimWorld behaviour through the public `Transport` API.
#![cfg(feature = "sim")]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use ao_network::{
    AdvProfile, ConnectError, DisconnectReason, LinkId, LinkRole, PowerMode, RadioState, SIM_RSSI,
    ScanProfile, SimConfig, SimError, SimNode, SimTransport, SimWorld, Transport, TransportEvent,
};

const SECOND: Duration = Duration::from_secs(1);

fn drain(radio: &mut SimTransport) -> Vec<TransportEvent> {
    let mut cx = Context::from_waker(Waker::noop());
    let mut events = Vec::new();
    while let Poll::Ready(event) = radio.poll_event(&mut cx) {
        events.push(event);
    }
    events
}

fn adv(byte: u8) -> Option<AdvProfile> {
    Some(AdvProfile {
        beacon: [byte; 16],
        mode: PowerMode::Balanced,
    })
}

const SCAN: Option<ScanProfile> = Some(ScanProfile {
    mode: PowerMode::Balanced,
});

/// Two started nodes in range; `b` advertises. Startup events drained.
fn pair(world: &SimWorld, a: SimNode, b: SimNode) -> (SimTransport, SimTransport) {
    let mut a = world.add_node(a).unwrap();
    let mut b = world.add_node(b).unwrap();
    world.set_in_range(a.node(), b.node(), true);
    a.start();
    b.start();
    b.set_advertising(adv(1));
    drain(&mut a);
    drain(&mut b);
    (a, b)
}

/// Connects `a` to `b` and returns both link ids.
fn connected(world: &SimWorld, a: &mut SimTransport, b: &mut SimTransport) -> (LinkId, LinkId) {
    let link = a.connect(b.peer_key());
    world.advance(SECOND);
    drain(a);
    let Some(TransportEvent::Connected { link: remote, .. }) = drain(b).pop() else {
        panic!("b did not accept");
    };
    (link, remote)
}

#[test]
fn invalid_mtu_is_rejected() {
    let world = SimWorld::new(SimConfig::default());
    for mtu in [22, 518] {
        assert_eq!(
            world
                .add_node(SimNode {
                    mtu,
                    ..SimNode::default()
                })
                .err(),
            Some(SimError::InvalidMtu { mtu })
        );
    }
}

#[test]
fn discovery_follows_range_and_beacon_changes() {
    let world = SimWorld::new(SimConfig::default());
    let mut scanner = world.add_node(SimNode::default()).unwrap();
    let mut advertiser = world.add_node(SimNode::default()).unwrap();
    scanner.start();
    advertiser.start();
    scanner.set_scanning(SCAN);
    advertiser.set_advertising(adv(1));
    assert_eq!(
        drain(&mut scanner),
        [TransportEvent::RadioState(RadioState::Ready)]
    );

    let peer = advertiser.peer_key();
    let discovered = |byte| TransportEvent::Discovered {
        peer,
        rssi: SIM_RSSI,
        beacon: vec![byte; 16],
    };
    world.set_in_range(scanner.node(), advertiser.node(), true);
    assert_eq!(drain(&mut scanner), [discovered(1)]);
    advertiser.set_advertising(adv(2));
    assert_eq!(drain(&mut scanner), [discovered(2)]);
    // An unchanged beacon is re-reported once the scan refresh is due,
    // as a radio would on the next scan window, and not before.
    world.advance(Duration::from_millis(999));
    assert_eq!(drain(&mut scanner), []);
    world.advance(Duration::from_millis(1));
    assert_eq!(drain(&mut scanner), [discovered(2)]);
    world.set_in_range(scanner.node(), advertiser.node(), false);
    assert_eq!(drain(&mut scanner), [TransportEvent::Lost { peer }]);
}

#[test]
fn connect_uses_latency_and_the_smaller_mtu() {
    let config = SimConfig {
        connect_jitter: Duration::ZERO,
        ..SimConfig::default()
    };
    let world = SimWorld::new(config);
    let small = SimNode {
        mtu: 100,
        ..SimNode::default()
    };
    let (mut a, mut b) = pair(&world, small, SimNode::default());

    let link = a.connect(b.peer_key());
    world.advance(config.connect_latency - Duration::from_nanos(1));
    assert!(drain(&mut a).is_empty());
    world.advance(Duration::from_nanos(1));
    assert_eq!(
        drain(&mut a),
        [TransportEvent::Connected {
            link,
            peer: b.peer_key(),
            role: LinkRole::Initiator,
            mtu: 100
        }]
    );
    assert!(matches!(
        drain(&mut b)[..],
        [TransportEvent::Connected { role: LinkRole::Responder, mtu: 100, peer, .. }] if peer == a.peer_key()
    ));
}

#[test]
fn chunks_arrive_in_order_with_one_write_in_flight() {
    let world = SimWorld::new(SimConfig::default());
    let (mut a, mut b) = pair(&world, SimNode::default(), SimNode::default());
    let (link, remote) = connected(&world, &mut a, &mut b);

    for byte in 0..3u8 {
        a.write(link, vec![byte; 20]);
        world.advance(SECOND);
        assert_eq!(drain(&mut a), [TransportEvent::WriteReady { link }]);
        assert_eq!(
            drain(&mut b),
            [TransportEvent::Received {
                link: remote,
                chunk: vec![byte; 20]
            }]
        );
    }
}

#[test]
fn every_connect_failure_has_its_reason() {
    let world = SimWorld::new(SimConfig::default());
    let one_link = SimNode {
        max_links: 1,
        ..SimNode::default()
    };
    let (mut a, mut b) = pair(&world, one_link, SimNode::default());
    let mut idle = world.add_node(SimNode::default()).unwrap();
    let mut silent = world.add_node(SimNode::default()).unwrap();
    silent.start();
    world.set_in_range(a.node(), silent.node(), true);

    let failed = |radio: &mut SimTransport| match drain(radio).pop() {
        Some(TransportEvent::ConnectFailed { error, .. }) => error,
        other => panic!("expected ConnectFailed, got {other:?}"),
    };

    idle.connect(b.peer_key());
    assert_eq!(failed(&mut idle), ConnectError::RadioUnavailable);
    a.connect(ao_network::PeerKey::new([0xEE; 16]));
    assert_eq!(failed(&mut a), ConnectError::PeerUnknown);
    a.connect(silent.peer_key());
    world.advance(SECOND);
    assert_eq!(failed(&mut a), ConnectError::Unreachable);
    let link = a.connect(b.peer_key());
    a.disconnect(link);
    assert_eq!(failed(&mut a), ConnectError::Cancelled);

    connected(&world, &mut a, &mut b);
    a.connect(b.peer_key());
    world.advance(SECOND);
    assert_eq!(failed(&mut a), ConnectError::Rejected);
}

#[test]
fn each_way_a_link_ends_is_terminal_on_both_sides() {
    let world = SimWorld::new(SimConfig::default());
    let (mut a, mut b) = pair(&world, SimNode::default(), SimNode::default());
    let lost = |link| TransportEvent::Disconnected {
        link,
        reason: DisconnectReason::TransportLost,
    };

    let (link, remote) = connected(&world, &mut a, &mut b);
    a.disconnect(link);
    assert_eq!(
        drain(&mut a),
        [TransportEvent::Disconnected {
            link,
            reason: DisconnectReason::Closed
        }]
    );
    assert_eq!(drain(&mut b), [lost(remote)]);

    let (link, remote) = connected(&world, &mut a, &mut b);
    world.kill_link(b.node(), remote);
    assert_eq!(drain(&mut a), [lost(link)]);
    assert_eq!(drain(&mut b), [lost(remote)]);
    // Commands on an ended link are ignored.
    a.write(link, vec![1]);
    a.disconnect(link);
    world.advance(SECOND);
    assert!(drain(&mut a).is_empty());

    let (link, remote) = connected(&world, &mut a, &mut b);
    world.set_in_range(a.node(), b.node(), false);
    assert_eq!(drain(&mut a), [lost(link)]);
    assert_eq!(drain(&mut b), [lost(remote)]);

    world.set_in_range(a.node(), b.node(), true);
    let (link, remote) = connected(&world, &mut a, &mut b);
    b.stop();
    assert_eq!(drain(&mut a), [lost(link)]);
    assert_eq!(
        drain(&mut b),
        [TransportEvent::Disconnected {
            link: remote,
            reason: DisconnectReason::Closed
        }]
    );
}

#[test]
fn new_events_wake_the_poller() {
    struct Count(AtomicUsize);
    impl Wake for Count {
        fn wake(self: Arc<Self>) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&count));

    let world = SimWorld::new(SimConfig::default());
    let mut radio = world.add_node(SimNode::default()).unwrap();
    assert!(
        radio
            .poll_event(&mut Context::from_waker(&waker))
            .is_pending()
    );
    radio.start();
    assert_eq!(count.0.load(Ordering::SeqCst), 1);
}

/// Ten nodes on a ring with chords discover, connect, exchange chunks and
/// lose links. Returns every event with its node and time.
fn ten_node_run(seed: u64) -> Vec<(Duration, usize, TransportEvent)> {
    const NODES: usize = 10;
    const CHUNKS: usize = 5;
    let world = SimWorld::new(SimConfig {
        seed,
        ..SimConfig::default()
    });
    let mut radios: Vec<SimTransport> = (0..NODES)
        .map(|i| {
            world
                .add_node(SimNode {
                    mtu: 23 + 50 * u16::try_from(i).unwrap(),
                    max_links: 3,
                    can_advertise: true,
                    ..SimNode::default()
                })
                .unwrap()
        })
        .collect();
    let keys: Vec<_> = radios.iter().map(SimTransport::peer_key).collect();
    for i in 0..NODES {
        for step in [1, 2] {
            world.set_in_range(radios[i].node(), radios[(i + step) % NODES].node(), true);
        }
    }
    for (i, radio) in radios.iter_mut().enumerate() {
        radio.start();
        radio.set_advertising(adv(u8::try_from(i).unwrap()));
        radio.set_scanning(SCAN);
    }

    let mut sent = vec![std::collections::BTreeMap::<LinkId, usize>::new(); NODES];
    let mut trace = Vec::new();
    for tick in 0..300 {
        world.advance(Duration::from_millis(10));
        if tick == 100 {
            world.kill_link(radios[0].node(), LinkId::new(1));
        }
        if tick == 150 {
            world.set_in_range(radios[3].node(), radios[4].node(), false);
        }
        for i in 0..NODES {
            for event in drain(&mut radios[i]) {
                match &event {
                    TransportEvent::Discovered { peer, .. } => {
                        if keys.iter().position(|k| k == peer) > Some(i) {
                            radios[i].connect(*peer);
                        }
                    }
                    TransportEvent::Connected { link, .. }
                    | TransportEvent::WriteReady { link } => {
                        let count = sent[i].entry(*link).or_default();
                        if *count < CHUNKS {
                            *count += 1;
                            radios[i].write(*link, vec![u8::try_from(i).unwrap(); 16]);
                        }
                    }
                    _ => {}
                }
                trace.push((world.now(), i, event));
            }
        }
    }
    trace
}

#[test]
fn ten_node_scenario_is_reproducible() {
    let first = ten_node_run(7);
    assert_eq!(first, ten_node_run(7));
    // Every kind of interaction happened, so the comparison means something.
    let has = |f: fn(&TransportEvent) -> bool| first.iter().any(|(_, _, e)| f(e));
    assert!(has(|e| matches!(e, TransportEvent::Connected { .. })));
    assert!(has(|e| matches!(e, TransportEvent::Received { .. })));
    assert!(has(|e| matches!(e, TransportEvent::Disconnected { .. })));
    assert!(has(|e| matches!(e, TransportEvent::Lost { .. })));
    // The seed matters: other jitter, other timeline.
    assert_ne!(first, ten_node_run(8));
}
