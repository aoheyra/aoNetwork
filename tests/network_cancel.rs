//! Cancelling an outbound attempt through the facade, before a link is up.

use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ao_network::{
    AdvProfile, AppHello, Beacon, BeaconFlags, ConnectError, DisconnectReason, LinkId, LocalTag,
    Network, NetworkConfig, PeerKey, ScanProfile, Transport, TransportCaps, TransportEvent,
};

#[derive(Default)]
struct PendingTransport {
    events: VecDeque<TransportEvent>,
    attempted: Option<LinkId>,
    closed: Vec<LinkId>,
}

impl Transport for PendingTransport {
    fn caps(&self) -> TransportCaps {
        TransportCaps {
            can_advertise: false,
            max_mtu: 23,
            max_links: 1,
        }
    }

    fn start(&mut self) {}
    fn stop(&mut self) {}
    fn set_advertising(&mut self, _: Option<AdvProfile>) {}
    fn set_scanning(&mut self, _: Option<ScanProfile>) {}

    fn connect(&mut self, _: PeerKey) -> LinkId {
        let link = LinkId::new(42);
        self.attempted = Some(link);
        link
    }

    fn disconnect(&mut self, link: LinkId) {
        self.closed.push(link);
        self.events.push_back(TransportEvent::ConnectFailed {
            link,
            error: ConnectError::Cancelled,
        });
    }

    fn write(&mut self, _: LinkId, _: Vec<u8>) {
        panic!("an unconnected transport must never receive a write");
    }

    fn poll_event(&mut self, _: &mut Context<'_>) -> Poll<TransportEvent> {
        self.events.pop_front().map_or(Poll::Pending, Poll::Ready)
    }
}

#[test]
fn cancelling_an_attempt_disconnects_once_without_a_link_down_event() {
    let beacon = Beacon {
        flags: BeaconFlags {
            accepting: true,
            ..BeaconFlags::default()
        },
        degree: 0,
        max_links: 1,
        hint: [2; 8],
        digest: [0; 4],
    };
    let mut radio = PendingTransport::default();
    radio.events.push_back(TransportEvent::Discovered {
        peer: PeerKey::new([2; 16]),
        rssi: -50,
        beacon: beacon.encode().to_vec(),
    });
    let mut net = Network::new(NetworkConfig::default(), radio);
    net.set_local_hello(LocalTag::new([1; 16]), AppHello::new(Vec::new()).unwrap());
    net.set_beacon([1; 8], [0; 4], BeaconFlags::default());
    let mut cx = Context::from_waker(Waker::noop());
    assert!(net.poll_event(&mut cx, Duration::ZERO).is_pending());
    let link = net
        .transport()
        .attempted
        .expect("discovery started a connection");
    net.disconnect(link, DisconnectReason::Closed);
    net.disconnect(link, DisconnectReason::ProtocolViolation);
    assert_eq!(net.transport().closed, [link]);
    assert!(net.poll_event(&mut cx, Duration::ZERO).is_pending());
    assert!(net.links().is_empty());
    net.disconnect(link, DisconnectReason::Closed);
    assert_eq!(net.transport().closed, [link]);
}
