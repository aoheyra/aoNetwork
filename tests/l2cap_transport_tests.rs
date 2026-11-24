//! The L2CAP CoC transport: connect, accept, backpressure and teardown.
#![cfg(feature = "native")]

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use ao_network::{
    AdvProfile, ConnectError, DEFAULT_COC_PSM, DisconnectReason, L2capConfig, L2capError,
    L2capEvents, L2capHandle, L2capSink, L2capStage, L2capStatus, L2capTransport, LinkId, LinkRole,
    LinkTransport, PeerKey, RadioState, ScanProfile, Transport, TransportCaps, TransportEvent,
};

#[derive(Debug, Default)]
struct Calls {
    server: Vec<Option<u16>>,
    connects: Vec<(LinkId, PeerKey, u16)>,
    writes: Vec<(LinkId, Vec<u8>)>,
    disconnects: Vec<LinkId>,
}

#[derive(Debug, Default)]
struct Shim {
    calls: Mutex<Calls>,
}

impl L2capSink for Shim {
    fn set_advertising(&self, _: Option<[u8; 16]>, _: ao_network::AdvertiseMode) {}
    fn set_scanning(&self, _: Option<ao_network::ScanMode>) {}
    fn set_server(&self, psm: Option<u16>) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .server
            .push(psm);
    }
    fn connect(&self, link: LinkId, peer: PeerKey, psm: u16) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .connects
            .push((link, peer, psm));
    }
    fn write(&self, link: LinkId, bytes: Vec<u8>) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .writes
            .push((link, bytes));
    }
    fn disconnect(&self, link: LinkId) {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .disconnects
            .push(link);
    }
}

const PEER: PeerKey = PeerKey::new([7; 16]);

fn caps() -> TransportCaps {
    TransportCaps {
        can_advertise: true,
        max_mtu: 517,
        max_links: 5,
    }
}

fn radio(config: L2capConfig) -> (L2capTransport, L2capHandle, Arc<Shim>) {
    let shim = Arc::new(Shim::default());
    let (transport, handle) = L2capTransport::new(Arc::clone(&shim) as Arc<dyn L2capSink>, config);
    (transport, handle, shim)
}

fn next<T>(transport: &mut T) -> TransportEvent
where
    T: Transport,
{
    let mut cx = Context::from_waker(Waker::noop());
    match transport.poll_event(&mut cx) {
        Poll::Ready(event) => event,
        Poll::Pending => panic!("expected an event"),
    }
}

fn pending<T: Transport>(transport: &mut T) {
    let mut cx = Context::from_waker(Waker::noop());
    assert_eq!(transport.poll_event(&mut cx), Poll::Pending);
}

/// Closes decided inside a callback are sent on the next transport call.
fn flush<T: Transport>(transport: &mut T) {
    let mut cx = Context::from_waker(Waker::noop());
    let _ = transport.poll_event(&mut cx);
}

#[test]
fn config_rejects_invalid_psm_and_mtu() {
    assert_eq!(L2capConfig::new(caps()).psm(), DEFAULT_COC_PSM);
    assert_eq!(
        L2capConfig::new(caps()).with_psm(0x1000),
        Err(L2capError::InvalidPsm { psm: 0x1000 })
    );
    assert_eq!(
        L2capConfig::new(caps()).with_psm(0x0800),
        Err(L2capError::InvalidPsm { psm: 0x0800 })
    );
    assert_eq!(
        L2capConfig::new(caps()).with_psm(0x1003).unwrap().psm(),
        0x1003
    );
    assert_eq!(
        L2capConfig::new(caps()).with_mtu(22),
        Err(L2capError::InvalidMtu { mtu: 22 })
    );
    assert_eq!(
        L2capConfig::new(caps()).with_mtu(518),
        Err(L2capError::InvalidMtu { mtu: 518 })
    );
    let config = L2capConfig::new(caps()).with_mtu(256).unwrap();
    let (transport, _, _) = radio(config);
    assert_eq!(transport.caps().max_mtu, 256);
    assert_eq!(transport.link_transport(), LinkTransport::L2capCoc);
}

#[test]
fn outbound_connect_reports_setup_and_outcome() {
    let (mut transport, handle, shim) = radio(L2capConfig::new(caps()));
    transport.start();
    handle.on_radio_state(RadioState::Ready);
    assert_eq!(
        next(&mut transport),
        TransportEvent::RadioState(RadioState::Ready)
    );
    assert_eq!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .server,
        vec![Some(DEFAULT_COC_PSM)]
    );

    let link = transport.connect(PEER);
    assert_eq!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .connects,
        vec![(link, PEER, DEFAULT_COC_PSM)]
    );
    pending(&mut transport);

    handle.on_connected(link, 512);
    assert_eq!(
        next(&mut transport),
        TransportEvent::Connected {
            link,
            peer: PEER,
            role: LinkRole::Initiator,
            mtu: 512,
        }
    );
    transport.disconnect(link);
    handle.on_closed(link, L2capStatus::Requested);
    assert_eq!(
        next(&mut transport),
        TransportEvent::Disconnected {
            link,
            reason: DisconnectReason::Closed,
        }
    );
}

#[test]
fn inbound_accept_allocates_an_id_and_reports_responder_role() {
    let (mut transport, handle, _) = radio(L2capConfig::new(caps()));
    transport.start();
    let link = handle.on_accepted(PEER, 512);
    assert_eq!(
        next(&mut transport),
        TransportEvent::Connected {
            link,
            peer: PEER,
            role: LinkRole::Responder,
            mtu: 512,
        }
    );
    // A second accept gets a fresh id; ids are never reused after close.
    handle.on_closed(link, L2capStatus::Requested);
    let other = handle.on_accepted(PEER, 512);
    assert_ne!(other, link);
}

#[test]
fn an_accept_while_stopped_is_never_reported() {
    let (mut transport, handle, shim) = radio(L2capConfig::new(caps()));
    let link = handle.on_accepted(PEER, 512);
    pending(&mut transport);
    assert_eq!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .disconnects,
        vec![link]
    );
    handle.on_closed(link, L2capStatus::Requested);
    pending(&mut transport);
}

#[test]
fn write_credit_is_one_packet_and_oversize_packets_close_the_link() {
    let (mut transport, handle, shim) = radio(L2capConfig::new(caps()).with_mtu(64).unwrap());
    transport.start();
    let link = transport.connect(PEER);
    handle.on_connected(link, 64);
    let _ = next(&mut transport);

    transport.write(link, vec![1; 63]);
    transport.write(link, vec![2; 63]);
    let calls = shim.calls.lock().unwrap_or_else(PoisonError::into_inner);
    assert_eq!(calls.writes.len(), 1, "a second write needs on_write_ready");
    assert_eq!(calls.writes[0].1, vec![1; 63]);
    drop(calls);

    handle.on_write_ready(link);
    assert_eq!(next(&mut transport), TransportEvent::WriteReady { link });
    transport.write(link, vec![3; 63]);
    assert_eq!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .writes
            .len(),
        2
    );

    transport.write(link, vec![4; 65]);
    assert_eq!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .disconnects,
        vec![link]
    );
    handle.on_closed(
        link,
        L2capStatus::Failed {
            stage: L2capStage::Write,
            code: 0,
        },
    );
    assert_eq!(
        next(&mut transport),
        TransportEvent::Disconnected {
            link,
            reason: DisconnectReason::TransportLost,
        }
    );
}

#[test]
fn a_full_event_queue_closes_the_link_but_keeps_its_terminal_event() {
    let config = L2capConfig::new(caps()).with_max_pending_events(NonZeroUsize::MIN);
    let (mut transport, handle, shim) = radio(config);
    transport.start();
    let link = transport.connect(PEER);
    handle.on_connected(link, 512);
    let _ = next(&mut transport);

    handle.on_write_ready(link); // fills the single queue slot
    handle.on_bytes(link, vec![0xAB; 4]); // no slot left: close the link
    assert_eq!(next(&mut transport), TransportEvent::WriteReady { link });
    assert_eq!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .disconnects,
        vec![link]
    );
    // A later event for the closing link is ignored; the terminal event
    // is queued past the cap.
    handle.on_bytes(link, vec![0xCD; 4]);
    handle.on_closed(link, L2capStatus::Requested);
    assert_eq!(
        next(&mut transport),
        TransportEvent::Disconnected {
            link,
            reason: DisconnectReason::TransportLost,
        }
    );
    pending(&mut transport);
}

#[test]
fn oversize_and_empty_packets_close_with_transport_lost() {
    let (mut transport, handle, shim) = radio(L2capConfig::new(caps()).with_mtu(64).unwrap());
    transport.start();
    let link = transport.connect(PEER);
    handle.on_connected(link, 64);
    let _ = next(&mut transport);

    handle.on_bytes(link, vec![0; 65]);
    flush(&mut transport);
    assert_eq!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .disconnects,
        vec![link]
    );
    handle.on_closed(link, L2capStatus::Requested);
    assert_eq!(
        next(&mut transport),
        TransportEvent::Disconnected {
            link,
            reason: DisconnectReason::TransportLost,
        }
    );

    let other = transport.connect(PEER);
    handle.on_connected(other, 64);
    let _ = next(&mut transport);
    handle.on_bytes(other, Vec::new());
    handle.on_closed(other, L2capStatus::Requested);
    assert!(matches!(
        next(&mut transport),
        TransportEvent::Disconnected {
            reason: DisconnectReason::TransportLost,
            ..
        }
    ));
}

#[test]
fn an_invalid_reported_mtu_never_reaches_the_link_layer() {
    let (mut transport, handle, shim) = radio(L2capConfig::new(caps()).with_mtu(64).unwrap());
    transport.start();
    let link = transport.connect(PEER);
    handle.on_connected(link, 65);
    flush(&mut transport);
    assert_eq!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .disconnects,
        vec![link]
    );
    handle.on_closed(link, L2capStatus::Requested);
    assert_eq!(
        next(&mut transport),
        TransportEvent::ConnectFailed {
            link,
            error: ConnectError::Cancelled,
        }
    );
}

#[test]
fn teardown_closes_every_link_and_stops_the_server() {
    let (mut transport, handle, shim) = radio(L2capConfig::new(caps()));
    transport.start();
    let a = transport.connect(PEER);
    let b = handle.on_accepted(PeerKey::new([8; 16]), 512);
    handle.on_connected(a, 512);
    let _ = next(&mut transport);
    let _ = next(&mut transport);

    transport.stop();
    assert_eq!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .server,
        vec![Some(DEFAULT_COC_PSM), None]
    );
    let mut closed = shim
        .calls
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .disconnects
        .clone();
    closed.sort();
    assert_eq!(closed, vec![a.min(b), a.max(b)]);

    handle.on_closed(a, L2capStatus::Requested);
    handle.on_closed(b, L2capStatus::Requested);
    for _ in 0..2 {
        assert!(matches!(
            next(&mut transport),
            TransportEvent::Disconnected {
                reason: DisconnectReason::Closed,
                ..
            }
        ));
    }
    pending(&mut transport);
}

#[test]
fn status_mapping_covers_both_sides_of_the_handshake() {
    let cases = [
        (
            L2capStatus::Timeout,
            ConnectError::Timeout,
            DisconnectReason::TransportLost,
        ),
        (
            L2capStatus::PeerUnknown,
            ConnectError::PeerUnknown,
            DisconnectReason::TransportLost,
        ),
        (
            L2capStatus::PsmUnavailable,
            ConnectError::ServiceMissing,
            DisconnectReason::TransportLost,
        ),
        (
            L2capStatus::RadioOff,
            ConnectError::RadioUnavailable,
            DisconnectReason::TransportLost,
        ),
        (
            L2capStatus::Failed {
                stage: L2capStage::Connect,
                code: 0x22,
            },
            ConnectError::Timeout,
            DisconnectReason::TransportLost,
        ),
        (
            L2capStatus::Refused {
                stage: L2capStage::Write,
                code: -1,
            },
            ConnectError::SetupFailed,
            DisconnectReason::TransportLost,
        ),
        (
            L2capStatus::PeerMisbehaved,
            ConnectError::Unreachable,
            DisconnectReason::ProtocolViolation,
        ),
    ];
    for (status, connect, disconnect) in cases {
        let (mut transport, handle, _) = radio(L2capConfig::new(caps()));
        transport.start();
        let link = transport.connect(PEER);
        handle.on_closed(link, status);
        assert_eq!(
            next(&mut transport),
            TransportEvent::ConnectFailed {
                link,
                error: connect
            }
        );

        let up = transport.connect(PEER);
        handle.on_connected(up, 512);
        let _ = next(&mut transport);
        handle.on_closed(up, status);
        assert_eq!(
            next(&mut transport),
            TransportEvent::Disconnected {
                link: up,
                reason: disconnect,
            }
        );
    }
}

#[test]
fn scan_results_are_latest_wins_and_bounded() {
    let config = L2capConfig::new(caps()).with_max_pending_scans(NonZeroUsize::MIN);
    let (mut transport, handle, _) = radio(config);
    transport.start();
    let other = PeerKey::new([9; 16]);
    handle.on_scan_result(PEER, -40, vec![0; 31]);
    handle.on_scan_result(PEER, -50, vec![1; 31]);
    handle.on_scan_result(other, -60, vec![2; 32]); // too long, dropped
    assert_eq!(
        next(&mut transport),
        TransportEvent::Discovered {
            peer: PEER,
            rssi: -50,
            beacon: vec![1; 31],
        }
    );
    pending(&mut transport);
}

#[test]
fn callbacks_after_drop_are_no_ops() {
    let (transport, handle, _) = radio(L2capConfig::new(caps()));
    drop(transport);
    let link = handle.on_accepted(PEER, 512);
    assert_eq!(link, LinkId::new(0));
    handle.on_radio_state(RadioState::Ready);
    handle.on_bytes(link, vec![1]);
}

#[test]
fn advertising_profiles_reach_the_sink_only_while_running() {
    let (mut transport, _, shim) = radio(L2capConfig::new(caps()));
    let beacon = AdvProfile {
        beacon: [3; 16],
        mode: ao_network::PowerMode::LowLatency,
    };
    transport.set_advertising(Some(beacon));
    transport.set_scanning(Some(ScanProfile {
        mode: ao_network::PowerMode::Balanced,
    }));
    assert!(
        shim.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .server
            .is_empty()
    );
    transport.start();
    transport.set_advertising(Some(beacon));
    transport.set_scanning(None);
    transport.stop();
}
