//! Link lifecycle over the simulator: the handshake outcome and the exact
//! reason each link goes down.
#![cfg(feature = "sim")]

use std::collections::{BTreeMap, VecDeque};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ao_network::{
    AdvProfile, AppHello, Channel, DisconnectReason, Fragmenter, Frame, HandshakeFailure, Hello,
    Link, LinkAction, LinkEnd, LinkId, LinkMetrics, LinkRole, LinkState, LocalTag, NetworkConfig,
    PeerKey, PowerMode, SendError, SendOpts, SimConfig, SimNode, SimTransport, SimWorld, Transport,
    TransportEvent,
};

const TICK: Duration = Duration::from_millis(10);
const MTU: u16 = 23;

fn hello(proto_min: u8, proto_max: u8, app_hello: usize) -> Hello {
    Hello {
        proto_min,
        proto_max,
        caps: 0,
        max_frame: 16 * 1024,
        tag: LocalTag::new([proto_max; 16]),
        app_hello: AppHello::new(vec![0xA5; app_hello]).unwrap(),
    }
}

/// A node that runs `Link`s, or a raw one (`hello: None`) that only sends
/// what the test queues, to play a broken peer.
struct Node {
    radio: SimTransport,
    hello: Option<Hello>,
    links: BTreeMap<LinkId, Link>,
    log: Vec<LinkAction>,
    raw_out: VecDeque<Vec<u8>>,
    raw_link: Option<LinkId>,
    raw_can_write: bool,
}

impl Node {
    fn new(world: &SimWorld, hello: Option<Hello>) -> Self {
        let mut radio = world
            .add_node(SimNode {
                mtu: MTU,
                ..SimNode::default()
            })
            .unwrap();
        radio.start();
        radio.set_advertising(Some(AdvProfile {
            beacon: [0; 16],
            mode: PowerMode::Balanced,
        }));
        Self {
            radio,
            hello,
            links: BTreeMap::new(),
            log: Vec::new(),
            raw_out: VecDeque::new(),
            raw_link: None,
            raw_can_write: false,
        }
    }

    fn connect(&mut self, peer: &Node) {
        let link = self.radio.connect(peer.radio.peer_key());
        let hello = self.hello.clone().expect("only link nodes connect");
        self.links
            .insert(link, Link::new(&NetworkConfig::default(), hello));
    }

    fn apply(&mut self, link: LinkId, actions: Vec<LinkAction>) {
        for action in actions {
            match action {
                LinkAction::Write(chunk) => self.radio.write(link, chunk),
                LinkAction::Disconnect => self.radio.disconnect(link),
                other => self.log.push(other),
            }
        }
    }

    fn step(&mut self, now: Duration) {
        let mut cx = Context::from_waker(Waker::noop());
        while let Poll::Ready(event) = self.radio.poll_event(&mut cx) {
            let link = match &event {
                TransportEvent::Connected { link, .. }
                | TransportEvent::ConnectFailed { link, .. }
                | TransportEvent::Received { link, .. }
                | TransportEvent::WriteReady { link }
                | TransportEvent::MtuChanged { link, .. }
                | TransportEvent::Disconnected { link, .. } => *link,
                _ => continue,
            };
            let Some(hello) = self.hello.clone() else {
                self.raw_event(&event);
                continue;
            };
            if let TransportEvent::Connected {
                role: LinkRole::Responder,
                ..
            } = event
            {
                self.links
                    .insert(link, Link::new(&NetworkConfig::default(), hello));
            }
            if let Some(machine) = self.links.get_mut(&link) {
                let actions = machine.on_event(now, event);
                self.apply(link, actions);
            }
        }
        let ids: Vec<LinkId> = self.links.keys().copied().collect();
        for link in ids {
            let actions = self.links.get_mut(&link).unwrap().on_tick(now);
            self.apply(link, actions);
        }
        self.raw_flush();
    }

    /// Sends on this node's only link.
    fn send(&mut self, now: Duration, channel: Channel, payload: Vec<u8>) {
        let (&link, machine) = self.links.iter_mut().next().unwrap();
        let actions = machine.send(now, channel, payload).unwrap();
        self.apply(link, actions);
    }

    /// Data payloads received so far, in order.
    fn data(&self) -> Vec<(Channel, Vec<u8>)> {
        self.log
            .iter()
            .filter_map(|action| match action {
                LinkAction::Data { channel, payload } => Some((*channel, payload.clone())),
                _ => None,
            })
            .collect()
    }

    fn raw_event(&mut self, event: &TransportEvent) {
        match event {
            TransportEvent::Connected { link, .. } => {
                self.raw_link = Some(*link);
                self.raw_can_write = true;
            }
            TransportEvent::WriteReady { .. } => self.raw_can_write = true,
            _ => {}
        }
    }

    fn raw_flush(&mut self) {
        if let (Some(link), true) = (self.raw_link, self.raw_can_write) {
            if let Some(chunk) = self.raw_out.pop_front() {
                self.raw_can_write = false;
                self.radio.write(link, chunk);
            }
        }
    }

    fn down(&self) -> Option<LinkEnd> {
        self.log.iter().find_map(|action| match action {
            LinkAction::Down(end) => Some(*end),
            _ => None,
        })
    }
}

/// `a` (a link node) connects to `b`; runs for `duration` of simulated time.
fn run(a: Option<Hello>, b: Option<Hello>, raw_frame: Vec<u8>, duration: Duration) -> (Node, Node) {
    let world = SimWorld::new(SimConfig::default());
    let mut a = Node::new(&world, a);
    let mut b = Node::new(&world, b);
    world.set_in_range(a.radio.node(), b.radio.node(), true);
    b.raw_out = Fragmenter::new(&raw_frame, MTU).unwrap().collect();
    a.connect(&b);
    let mut elapsed = Duration::ZERO;
    while elapsed < duration {
        world.advance(TICK);
        elapsed += TICK;
        a.step(world.now());
        b.step(world.now());
    }
    (a, b)
}

fn down(reason: DisconnectReason) -> Option<LinkEnd> {
    Some(LinkEnd::Disconnected(reason))
}

#[test]
fn hellos_meet_at_the_highest_shared_version() {
    // A full 256-byte app_hello spans many chunks at the minimum MTU.
    let (a, b) = run(
        Some(hello(1, 3, 256)),
        Some(hello(2, 5, 256)),
        Vec::new(),
        Duration::from_secs(2),
    );
    for (node, peer) in [(&a, &b), (&b, &a)] {
        assert_eq!(
            node.log,
            [LinkAction::Ready {
                remote: peer.hello.clone().unwrap(),
                version: 3
            }]
        );
    }
}

#[test]
fn version_mismatch_closes_both_sides() {
    let (a, b) = run(
        Some(hello(1, 1, 0)),
        Some(hello(2, 2, 0)),
        Vec::new(),
        Duration::from_secs(2),
    );
    let mismatch = down(DisconnectReason::HandshakeFailed(
        HandshakeFailure::VersionMismatch,
    ));
    assert_eq!(a.down(), mismatch);
    assert_eq!(b.down(), mismatch);
}

#[test]
fn oversize_app_hello_closes_the_link() {
    // A Hello frame claiming a 257-byte app_hello, which no honest encoder
    // produces: header, proto 1..1, caps, max_frame, tag, LEB128 257.
    let mut frame = vec![1, 0x01, 1, 1, 0, 0, 0, 0, 0, 0, 0x40, 0];
    frame.extend_from_slice(&[7; 16]);
    frame.extend_from_slice(&[0x81, 0x02]);
    frame.extend_from_slice(&[0; 257]);
    let (a, _) = run(Some(hello(1, 1, 0)), None, frame, Duration::from_secs(2));
    assert_eq!(
        a.down(),
        down(DisconnectReason::HandshakeFailed(
            HandshakeFailure::OversizeHello
        ))
    );
}

#[test]
fn silent_peer_times_out_after_handshake_timeout() {
    let timeout = NetworkConfig::default().handshake_timeout();
    // Connect latency is at most 300 ms by default, so the link is up by
    // then; the deadline falls between these two runs.
    let (early, _) = run(Some(hello(1, 1, 0)), None, Vec::new(), timeout);
    assert_eq!(early.down(), None);
    let (late, _) = run(
        Some(hello(1, 1, 0)),
        None,
        Vec::new(),
        timeout + Duration::from_millis(400),
    );
    assert_eq!(
        late.down(),
        down(DisconnectReason::HandshakeFailed(HandshakeFailure::Timeout))
    );
}

#[test]
fn close_says_bye_and_each_side_reports_its_reason() {
    let world = SimWorld::new(SimConfig::default());
    let mut a = Node::new(&world, Some(hello(1, 1, 0)));
    let mut b = Node::new(&world, Some(hello(1, 1, 0)));
    world.set_in_range(a.radio.node(), b.radio.node(), true);
    a.connect(&b);
    for _ in 0..100 {
        world.advance(TICK);
        a.step(world.now());
        b.step(world.now());
    }
    let link = *a.links.keys().next().unwrap();
    let actions = a
        .links
        .get_mut(&link)
        .unwrap()
        .close(DisconnectReason::DegreeCap);
    a.apply(link, actions);
    for _ in 0..100 {
        world.advance(TICK);
        a.step(world.now());
        b.step(world.now());
    }
    assert_eq!(a.down(), down(DisconnectReason::DegreeCap));
    assert_eq!(b.down(), down(DisconnectReason::Bye));
}

#[test]
fn control_waits_at_most_one_frame_behind_bulk() {
    let config = SimConfig::default();
    let world = SimWorld::new(config);
    let mut a = Node::new(&world, Some(hello(1, 1, 0)));
    let mut b = Node::new(&world, Some(hello(1, 1, 0)));
    world.set_in_range(a.radio.node(), b.radio.node(), true);
    a.connect(&b);
    // Queued before the handshake; flows once the link is ready.
    let bulk: Vec<Vec<u8>> = (0..10).map(|i| vec![i; 500]).collect();
    for payload in &bulk {
        a.send(world.now(), Channel::Bulk, payload.clone());
    }
    let tick = |a: &mut Node, b: &mut Node| {
        world.advance(TICK);
        a.step(world.now());
        b.step(world.now());
    };
    while b.data().len() < 2 {
        tick(&mut a, &mut b);
    }

    let sent_at = world.now();
    a.send(world.now(), Channel::Control, vec![0xC0; 4]);
    while !b.data().contains(&(Channel::Control, vec![0xC0; 4])) {
        tick(&mut a, &mut b);
    }
    // One chunk per write: the rest of the Bulk frame in flight, then the
    // Control frame itself, plus one tick of polling.
    let chunks = |payload: usize| (payload + 5).div_ceil(usize::from(MTU) - 4) as u32;
    let bound = config.write_latency * (chunks(500) + chunks(4)) + TICK;
    assert!(
        world.now() - sent_at <= bound,
        "{:?}",
        world.now() - sent_at
    );

    while b.data().len() < bulk.len() + 1 {
        tick(&mut a, &mut b);
    }
    // Control overtook the queued Bulk, which arrived complete and in order.
    let data = b.data();
    assert!(data[data.len() - 1].0 == Channel::Bulk);
    let received: Vec<_> = data
        .into_iter()
        .filter(|(channel, _)| *channel == Channel::Bulk)
        .map(|(_, payload)| payload)
        .collect();
    assert_eq!(received, bulk);
}

fn connected(mtu: u16) -> TransportEvent {
    TransportEvent::Connected {
        link: LinkId::new(1),
        peer: PeerKey::new([1; 16]),
        role: LinkRole::Initiator,
        mtu,
    }
}

#[test]
fn send_on_a_closing_link_returns_the_payload() {
    let mut link = Link::new(&NetworkConfig::default(), hello(1, 1, 0));
    link.on_event(Duration::ZERO, connected(MTU));
    link.close(DisconnectReason::Closed);
    assert_eq!(
        link.send(Duration::ZERO, Channel::Message, vec![9; 3]),
        Err(SendError::LinkClosed(vec![9; 3]))
    );
}

/// NET-21: a frame past its deadline is dropped before anything is
/// written, and the deadline itself is what the owner is woken for.
#[test]
fn a_frame_past_its_deadline_is_never_written() {
    let mut link = Link::new(&NetworkConfig::default(), hello(1, 1, 0));
    link.on_event(Duration::ZERO, connected(MTU));
    let opts = SendOpts {
        coalesce_key: None,
        expires_at: Some(Duration::from_millis(1)),
    };
    link.send_opts(Duration::ZERO, Channel::Message, vec![9; 40], opts)
        .unwrap();
    // The owner wakes at the frame's deadline, well before the handshake
    // timeout, so an idle link never holds an expired payload.
    assert_eq!(link.next_deadline(), Some(Duration::from_millis(1)));

    // The handshake completes only after the deadline: the link comes up,
    // writes what it owes, and never writes the expired payload.
    let hello_chunks: Vec<_> = Fragmenter::new(&Frame::Hello(hello(1, 1, 0)).encode(), MTU)
        .unwrap()
        .collect();
    let mut out = Vec::new();
    for chunk in hello_chunks {
        out.extend(link.on_event(
            Duration::from_millis(2),
            TransportEvent::Received {
                link: LinkId::new(1),
                chunk,
            },
        ));
    }
    assert_eq!(link.state(), LinkState::Ready);
    assert!(
        out.iter()
            .any(|action| matches!(action, LinkAction::Ready { .. })),
        "the handshake must complete: {out:?}"
    );
    assert!(
        !out.iter()
            .any(|action| matches!(action, LinkAction::Data { .. })),
        "an expired frame was written: {out:?}"
    );

    // And a tick at or past the deadline prunes the queue in place, with
    // no action at all, so the deadline does not fire again.
    let mut idle = Link::new(&NetworkConfig::default(), hello(1, 1, 0));
    idle.on_event(Duration::ZERO, connected(MTU));
    idle.send_opts(Duration::ZERO, Channel::Message, vec![9; 40], opts)
        .unwrap();
    assert!(idle.on_tick(Duration::from_millis(1)).is_empty());
    // The frame's wake-up is gone: only the handshake deadline remains,
    // so this tick does not fire again for an empty queue.
    assert_ne!(idle.next_deadline(), Some(Duration::from_millis(1)));
}

#[test]
fn an_mtu_outside_the_ble_range_closes_the_link() {
    let lost = [LinkAction::Down(LinkEnd::Disconnected(
        DisconnectReason::TransportLost,
    ))];
    let closed = TransportEvent::Disconnected {
        link: LinkId::new(1),
        reason: DisconnectReason::Closed,
    };

    let mut link = Link::new(&NetworkConfig::default(), hello(1, 1, 0));
    assert_eq!(
        link.on_event(Duration::ZERO, connected(MTU - 1)),
        [LinkAction::Disconnect]
    );
    assert_eq!(link.on_event(Duration::ZERO, closed.clone()), lost);

    let mut link = Link::new(&NetworkConfig::default(), hello(1, 1, 0));
    link.on_event(Duration::ZERO, connected(MTU));
    let changed = TransportEvent::MtuChanged {
        link: LinkId::new(1),
        mtu: 518,
    };
    assert_eq!(
        link.on_event(Duration::ZERO, changed),
        [LinkAction::Disconnect]
    );
    assert_eq!(link.on_event(Duration::ZERO, closed), lost);
}

/// A node's only link's metrics.
fn metrics(node: &Node) -> LinkMetrics {
    node.links.values().next().unwrap().metrics()
}

#[test]
fn silent_peer_is_closed_within_the_keepalive_bound() {
    let config = NetworkConfig::default();
    // The peer finishes the handshake, then never sends again; its Hello
    // arrives within the first second.
    let raw_hello = Frame::Hello(hello(1, 1, 0)).encode();
    let bound = config.keepalive_interval() * (u32::from(config.max_missed_pongs()) + 1);
    let (early, _) = run(Some(hello(1, 1, 0)), None, raw_hello.clone(), bound);
    assert!(matches!(early.log[..], [LinkAction::Ready { .. }]));
    let (late, _) = run(
        Some(hello(1, 1, 0)),
        None,
        raw_hello,
        bound + Duration::from_secs(1),
    );
    assert_eq!(late.down(), down(DisconnectReason::KeepaliveTimeout));
}

#[test]
fn an_answering_peer_stays_up_and_its_rtt_is_measured() {
    let (a, b) = run(
        Some(hello(1, 1, 0)),
        Some(hello(1, 1, 0)),
        Vec::new(),
        Duration::from_secs(100),
    );
    for node in [&a, &b] {
        assert_eq!(node.down(), None);
        let rtt = metrics(node).rtt.expect("a pong arrived");
        // One chunk each way at the simulator's 10 ms write latency.
        assert!(
            rtt > Duration::ZERO && rtt <= Duration::from_millis(100),
            "{rtt:?}"
        );
        assert!(metrics(node).last_rx.is_some());
    }
    // Nothing is in flight at the end, so every byte sent was received.
    assert_eq!(metrics(&a).tx_bytes, metrics(&b).rx_bytes);
    assert_eq!(metrics(&b).tx_bytes, metrics(&a).rx_bytes);
}

#[test]
fn rssi_is_averaged_and_ignored_frames_are_counted() {
    let receive = |frame: Frame| {
        let chunks: Vec<_> = Fragmenter::new(&frame.encode(), MTU).unwrap().collect();
        chunks.into_iter().map(|chunk| TransportEvent::Received {
            link: LinkId::new(1),
            chunk,
        })
    };
    let rssi = |rssi| TransportEvent::Rssi {
        link: LinkId::new(1),
        rssi,
    };
    let mut link = Link::new(&NetworkConfig::default(), hello(1, 1, 0));
    let mut events = vec![connected(MTU)];
    events.extend(receive(Frame::Hello(hello(1, 1, 0))));
    events.extend([rssi(-60), rssi(-68)]);
    events.extend(receive(Frame::Credit { credits: 1 }));
    // A pong no ping asked for.
    events.extend(receive(Frame::Pong));
    for event in events {
        link.on_event(Duration::ZERO, event);
    }
    assert_eq!(link.state(), LinkState::Ready);
    // A new sample moves the average by an eighth of the difference.
    assert_eq!(link.metrics().rssi, Some(-61));
    assert_eq!(link.metrics().dropped_frames, 2);
}
