//! Simulator coverage and the GATT/CoC measurement for NET-26.
#![cfg(feature = "sim")]

use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ao_network::{
    AdvProfile, AppHello, BeaconFlags, Channel, ConnectError, LinkId, LinkTransport, LocalTag,
    Network, NetworkConfig, NetworkEvent, PeerKey, ScanProfile, SendError, SimConfig, SimNode,
    SimTransport, SimWorld, Transport, TransportCaps, TransportEvent,
};

const SEED: u64 = 0x4C_26_C0_C0;
const TICK: Duration = Duration::from_millis(1);
const PAYLOAD: usize = 4 * 1024;
const TOTAL: usize = 64 * 1024;

/// Counts writes at the transport seam without changing behaviour.
#[derive(Debug)]
struct Counting<T> {
    inner: T,
    writes: usize,
}

impl<T: Transport> Transport for Counting<T> {
    fn caps(&self) -> TransportCaps {
        self.inner.caps()
    }

    fn link_transport(&self) -> LinkTransport {
        self.inner.link_transport()
    }

    fn start(&mut self) {
        self.inner.start();
    }

    fn stop(&mut self) {
        self.inner.stop();
    }

    fn set_advertising(&mut self, profile: Option<AdvProfile>) {
        self.inner.set_advertising(profile);
    }

    fn set_scanning(&mut self, profile: Option<ScanProfile>) {
        self.inner.set_scanning(profile);
    }

    fn connect(&mut self, peer: PeerKey) -> LinkId {
        self.inner.connect(peer)
    }

    fn disconnect(&mut self, link: LinkId) {
        self.inner.disconnect(link);
    }

    fn write(&mut self, link: LinkId, chunk: Vec<u8>) {
        self.writes += 1;
        self.inner.write(link, chunk);
    }

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<TransportEvent> {
        self.inner.poll_event(cx)
    }
}

#[derive(Debug)]
struct Node {
    net: Network<Counting<SimTransport>>,
    id: ao_network::NodeId,
    events: Vec<NetworkEvent>,
}

fn pair(world: &SimWorld, l2cap: bool) -> (Node, Node) {
    let make = |tag: u8| {
        let radio = world
            .add_node(SimNode {
                l2cap,
                ..SimNode::default()
            })
            .unwrap();
        let id = radio.node();
        let mut net = Network::new(
            NetworkConfig::default(),
            Counting {
                inner: radio,
                writes: 0,
            },
        );
        net.set_local_hello(LocalTag::new([tag; 16]), AppHello::new(Vec::new()).unwrap());
        net.set_beacon(
            [tag; 8],
            [0; 4],
            BeaconFlags {
                accepting: true,
                ..BeaconFlags::default()
            },
        );
        Node {
            net,
            id,
            events: Vec::new(),
        }
    };
    let (a, b) = (make(1), make(2));
    world.set_in_range(a.id, b.id, true);
    (a, b)
}

fn pump(world: &SimWorld, a: &mut Node, b: &mut Node) {
    let mut cx = Context::from_waker(Waker::noop());
    for node in [a, b] {
        while let Poll::Ready(event) = node.net.poll_event(&mut cx, world.now()) {
            node.events.push(event);
        }
    }
}

fn up_link(world: &SimWorld, a: &mut Node, b: &mut Node) -> LinkId {
    for _ in 0..5_000 {
        world.advance(TICK);
        pump(world, a, b);
        if a.net.links().len() == 1 && b.net.links().len() == 1 {
            return a.net.links()[0].link;
        }
    }
    panic!("no link within 5 s");
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Report {
    delivered: usize,
    writes: usize,
    millis: u64,
}

fn transfer(world: &SimWorld, a: &mut Node, b: &mut Node, link: LinkId) -> Report {
    let mut pending: Vec<Vec<u8>> = (0..TOTAL / PAYLOAD)
        .map(|i| vec![(i % 251) as u8; PAYLOAD])
        .collect();
    let mut delivered = 0;
    let started = world.now();
    for _ in 0..20_000 {
        while let Some(payload) = pending.first() {
            match a
                .net
                .send(world.now(), link, Channel::Bulk, payload.clone())
            {
                Ok(()) => {
                    pending.remove(0);
                }
                Err(SendError::QueueFull(_)) => break,
                Err(err) => panic!("unexpected send failure: {err:?}"),
            }
        }
        world.advance(TICK);
        pump(world, a, b);
        for event in b.events.drain(..) {
            if let NetworkEvent::Data { payload, .. } = event {
                delivered += payload.len();
            }
        }
        a.events.clear();
        if delivered == TOTAL {
            break;
        }
    }
    assert_eq!(
        delivered, TOTAL,
        "simulator did not deliver the whole transfer"
    );
    Report {
        delivered,
        writes: a.net.transport().writes,
        millis: u64::try_from((world.now() - started).as_millis()).unwrap_or(u64::MAX),
    }
}

fn measure(l2cap: bool) -> (Report, LinkTransport, bool) {
    let world = SimWorld::new(SimConfig {
        seed: SEED,
        connect_latency: Duration::from_millis(200),
        connect_jitter: Duration::ZERO,
        write_latency: TICK,
    });
    let (mut a, mut b) = pair(&world, l2cap);
    let link = up_link(&world, &mut a, &mut b);
    let info = a.net.links()[0];
    let report = transfer(&world, &mut a, &mut b, link);
    (report, info.transport, info.l2cap_negotiated)
}

#[test]
fn coc_is_negotiated_and_gatt_is_the_fallback() {
    let (_, kind, negotiated) = measure(true);
    assert_eq!(kind, LinkTransport::L2capCoc);
    assert!(negotiated);

    let (_, kind, negotiated) = measure(false);
    assert_eq!(kind, LinkTransport::Gatt);
    assert!(!negotiated);
}

#[test]
fn coc_hello_advertises_the_capability() {
    let world = SimWorld::new(SimConfig::default());
    let (mut a, mut b) = pair(&world, true);
    let link = up_link(&world, &mut a, &mut b);
    assert!(a.net.links()[0].l2cap_negotiated);
    assert!(b.net.links()[0].l2cap_negotiated);
    assert_eq!(a.net.links()[0].link, link);
}

#[test]
fn mixed_transports_cannot_link() {
    let world = SimWorld::new(SimConfig::default());
    let radio_a = world.add_node(SimNode::default()).unwrap();
    let radio_b = world
        .add_node(SimNode {
            l2cap: true,
            ..SimNode::default()
        })
        .unwrap();
    world.set_in_range(radio_a.node(), radio_b.node(), true);
    let mut a = radio_a;
    let mut b = radio_b;
    a.start();
    b.start();
    let link = a.connect(b.peer_key());
    let mut cx = Context::from_waker(Waker::noop());
    let mut outcome = None;
    for _ in 0..1_000 {
        world.advance(TICK);
        while let Poll::Ready(event) = a.poll_event(&mut cx) {
            if let TransportEvent::ConnectFailed {
                link: failed,
                error,
            } = event
            {
                outcome = Some((failed, error));
            }
        }
        if outcome.is_some() {
            break;
        }
    }
    assert_eq!(
        outcome,
        Some((link, ConnectError::Unreachable)),
        "a CoC endpoint must not link to a GATT-only endpoint"
    );
}

#[test]
fn l2cap_coc_delivers_a_sync_transfer_with_fewer_writes_than_gatt() {
    let (gatt, gatt_kind, gatt_caps) = measure(false);
    let (coc, coc_kind, coc_caps) = measure(true);
    eprintln!(
        "NET-26 seed {SEED}: GATT {gatt:?} ({gatt_kind:?}, caps {gatt_caps}); \
         CoC {coc:?} ({coc_kind:?}, caps {coc_caps})"
    );
    assert_eq!((gatt_kind, gatt_caps), (LinkTransport::Gatt, false));
    assert_eq!((coc_kind, coc_caps), (LinkTransport::L2capCoc, true));
    assert_eq!(gatt.delivered, coc.delivered);
    assert!(
        coc.writes * 3 < gatt.writes * 2,
        "CoC wrote {} packets, GATT wrote {}",
        coc.writes,
        gatt.writes
    );
    assert!(
        coc.millis <= gatt.millis,
        "CoC took {} ms, GATT took {} ms",
        coc.millis,
        gatt.millis
    );
}
