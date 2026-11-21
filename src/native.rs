//! `NativeTransport`: [`Transport`] over the Kotlin BLE shim.
//!
//! Implements `docs/native-contract.md`. Kotlin implements [`NativeSink`]
//! and reports back through a [`NativeHandle`]; this crate has no FFI, the
//! binding lives in aoheyra.
//!
//! Each Kotlin callback is checked and turned into a [`TransportEvent`]
//! under a short lock, so per-link order is exactly callback order. Rust
//! never calls the sink from inside a callback: disconnects it decides on
//! there (an overflowing link, an inbound link while stopped) are made on
//! the next [`Transport`] call or poll.

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::{
    AdvProfile, ConnectError, DisconnectReason, LinkId, LinkRole, MAX_MTU, PeerKey, PowerMode,
    RadioState, ScanProfile, Transport, TransportCaps, TransportEvent,
};

/// The longest manufacturer payload legacy advertising can carry; longer
/// scan results cannot come from a real radio and are dropped.
const MAX_SCAN_BEACON: usize = 31;

/// The longest ATT value any MTU allows (`MAX_MTU` less the ATT header);
/// a longer `on_bytes` value cannot come from a real radio.
const MAX_ATT_VALUE: usize = MAX_MTU as usize - 3;

/// Rust to Kotlin. Implemented by the Kotlin BLE shim; every call returns
/// immediately (contract section 4).
pub trait NativeSink: Send + Sync {
    /// Starts or updates advertising with `beacon`, or stops it with `None`.
    fn set_advertising(&self, beacon: Option<[u8; 16]>, mode: AdvertiseMode);
    /// Starts or changes scanning, or stops it with `None`.
    fn set_scanning(&self, mode: Option<ScanMode>);
    /// Opens `link` to `peer` as central.
    fn connect(&self, link: LinkId, peer: PeerKey);
    /// Sends `bytes` as one ATT value on `link`.
    fn write(&self, link: LinkId, bytes: Vec<u8>);
    /// Tears `link` down, then reports `on_closed(link, Requested)`.
    fn disconnect(&self, link: LinkId);
}

/// Kotlin to Rust. Implemented by [`NativeHandle`]; every method returns
/// immediately (contract section 5).
pub trait NativeEvents: Send + Sync {
    /// The adapter or permission state, at start and on every change.
    fn on_radio_state(&self, state: RadioState);
    /// Advertising or scanning failed to start, with the raw Android code.
    fn on_discovery_failed(&self, kind: DiscoveryKind, code: i32);
    /// A scan result; `beacon` is the raw manufacturer payload.
    fn on_scan_result(&self, peer: PeerKey, rssi: i16, beacon: Vec<u8>);
    /// An outbound link finished setup.
    fn on_connected(&self, link: LinkId, mtu: u16);
    /// An inbound link finished setup; returns the id to use for it.
    fn on_accepted(&self, peer: PeerKey, mtu: u16) -> LinkId;
    /// The MTU of a set-up link changed.
    fn on_mtu_changed(&self, link: LinkId, mtu: u16);
    /// One ATT value arrived on `link`. A value longer than 514 bytes,
    /// which no ATT MTU allows, closes the link with `TransportLost`.
    fn on_bytes(&self, link: LinkId, bytes: Vec<u8>);
    /// The stack accepted the previous write on `link`.
    fn on_write_ready(&self, link: LinkId);
    /// `link` is gone. The last event for it.
    fn on_closed(&self, link: LinkId, status: NativeStatus);
}

/// Advertising duty cycle (Android `AdvertiseSettings`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdvertiseMode {
    /// `ADVERTISE_MODE_LOW_POWER`.
    LowPower,
    /// `ADVERTISE_MODE_BALANCED`.
    Balanced,
    /// `ADVERTISE_MODE_LOW_LATENCY`.
    LowLatency,
}

/// Scanning duty cycle (Android `ScanSettings`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScanMode {
    /// `SCAN_MODE_LOW_POWER`.
    LowPower,
    /// `SCAN_MODE_BALANCED`.
    Balanced,
    /// `SCAN_MODE_LOW_LATENCY`.
    LowLatency,
}

/// Which discovery procedure failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DiscoveryKind {
    /// Advertising.
    Advertise,
    /// Scanning.
    Scan,
}

/// The GATT step a status came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Stage {
    /// Connecting, or the connection dropping.
    Connect,
    /// Service discovery.
    Discover,
    /// Enabling notifications.
    Subscribe,
    /// A write or notification.
    Write,
}

/// Why Kotlin closed a link, as raw values (contract section 7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NativeStatus {
    /// Rust called `disconnect`.
    Requested,
    /// Non-success status from an async GATT callback.
    Gatt {
        /// Where it happened.
        stage: Stage,
        /// The raw GATT or HCI status.
        code: i32,
    },
    /// A GATT call was refused synchronously.
    Refused {
        /// Where it happened.
        stage: Stage,
        /// The raw `BluetoothStatusCodes` value; -1 for `false` or `null`.
        code: i32,
    },
    /// `connect` named a peer with no known device.
    PeerUnknown,
    /// Service, RX or TX not found after discovery.
    ServiceMissing,
    /// The peer broke the peripheral procedure.
    PeerMisbehaved,
    /// The adapter was turned off.
    RadioOff,
    /// A BLE permission was revoked.
    PermissionDenied,
}

/// Settings for a [`NativeTransport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct NativeConfig {
    /// What the phone's radio can do.
    pub caps: TransportCaps,
    /// Most link and radio events held for the poller. A link whose event
    /// does not fit is closed with `TransportLost`. Default 1024.
    pub max_pending_events: NonZeroUsize,
    /// Most peers with an undelivered scan result; each keeps only its
    /// latest. Results for further peers are dropped. Default 64.
    pub max_pending_scans: NonZeroUsize,
}

impl NativeConfig {
    /// Defaults for a radio with `caps`.
    pub fn new(caps: TransportCaps) -> Self {
        Self {
            caps,
            max_pending_events: NonZeroUsize::MIN.saturating_add(1023),
            max_pending_scans: NonZeroUsize::MIN.saturating_add(63),
        }
    }
}

/// [`Transport`] over a Kotlin [`NativeSink`].
///
/// # Example
///
/// The binding layer hands Rust a sink that forwards to Kotlin, and gives
/// Kotlin the handle to report back through:
///
/// ```
/// use std::sync::Arc;
/// use ao_network::{
///     AdvertiseMode, LinkId, NativeConfig, NativeEvents, NativeSink, NativeTransport, Network,
///     NetworkConfig, PeerKey, RadioState, ScanMode, TransportCaps,
/// };
///
/// /// Stands in for the Kotlin shim; each call would cross the FFI.
/// struct Shim;
/// impl NativeSink for Shim {
///     fn set_advertising(&self, _: Option<[u8; 16]>, _: AdvertiseMode) {}
///     fn set_scanning(&self, _: Option<ScanMode>) {}
///     fn connect(&self, _: LinkId, _: PeerKey) {}
///     fn write(&self, _: LinkId, _: Vec<u8>) {}
///     fn disconnect(&self, _: LinkId) {}
/// }
///
/// let caps = TransportCaps { can_advertise: true, max_mtu: 517, max_links: 5 };
/// let (transport, handle) = NativeTransport::new(Arc::new(Shim), NativeConfig::new(caps));
/// let net = Network::new(NetworkConfig::default(), transport);
/// // Kotlin reports through the handle, from any thread.
/// handle.on_radio_state(RadioState::Ready);
/// # drop(net);
/// ```
#[derive(Debug)]
pub struct NativeTransport {
    sink: Arc<dyn NativeSink>,
    caps: TransportCaps,
    shared: Arc<Mutex<Inner>>,
}

/// Kotlin's way back into a [`NativeTransport`]. Calls after the transport
/// is dropped do nothing.
#[derive(Debug, Clone)]
pub struct NativeHandle {
    shared: Arc<Mutex<Inner>>,
}

impl std::fmt::Debug for dyn NativeSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeSink")
    }
}

/// Why Rust closed a link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cause {
    /// The owner called `disconnect` or `stop`.
    Asked,
    /// Its events overflowed the queue.
    Overflow,
    /// Kotlin passed a value longer than any ATT MTU allows.
    Oversize,
}

#[derive(Debug)]
struct Slot {
    peer: PeerKey,
    up: bool,
    credit: bool,
    closing: Option<Cause>,
    /// Never reported to the owner (accepted while stopped).
    hidden: bool,
}

#[derive(Debug)]
struct Inner {
    shut_down: bool,
    running: bool,
    radio: Option<RadioState>,
    next_link: u64,
    links: BTreeMap<LinkId, Slot>,
    events: VecDeque<TransportEvent>,
    scans: BTreeMap<PeerKey, (i16, Vec<u8>)>,
    /// Links Rust decided to close inside a callback.
    to_disconnect: Vec<LinkId>,
    waker: Option<Waker>,
    max_events: usize,
    max_scans: usize,
}

impl Inner {
    fn alloc(&mut self, peer: PeerKey, up: bool, hidden: bool) -> LinkId {
        self.next_link += 1;
        let link = LinkId::new(self.next_link);
        self.links.insert(
            link,
            Slot {
                peer,
                up,
                credit: up,
                closing: None,
                hidden,
            },
        );
        link
    }

    /// The slot of a set-up link that may still report events.
    fn live(&mut self, link: LinkId) -> Option<&mut Slot> {
        self.links
            .get_mut(&link)
            .filter(|s| s.up && !s.hidden && s.closing.is_none())
    }

    /// Queues a non-terminal event for `link`, or closes the link if the
    /// queue is full: a gap in its events would break their order.
    fn push_link_event(&mut self, link: LinkId, event: TransportEvent) {
        if self.events.len() < self.max_events {
            self.events.push_back(event);
        } else {
            self.close(link, Cause::Overflow);
        }
    }

    /// Closes `link` on Rust's own decision, at the next transport call.
    fn close(&mut self, link: LinkId, cause: Cause) {
        if let Some(slot) = self.links.get_mut(&link) {
            slot.closing = Some(cause);
            self.to_disconnect.push(link);
        }
    }
}

fn lock(shared: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    // Nothing under this lock panics on input; a poisoned lock still holds
    // consistent state.
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}

impl NativeTransport {
    /// A transport that drives `sink`, and the handle Kotlin reports to.
    pub fn new(sink: Arc<dyn NativeSink>, config: NativeConfig) -> (Self, NativeHandle) {
        let shared = Arc::new(Mutex::new(Inner {
            shut_down: false,
            running: false,
            radio: None,
            next_link: 0,
            links: BTreeMap::new(),
            events: VecDeque::new(),
            scans: BTreeMap::new(),
            to_disconnect: Vec::new(),
            waker: None,
            max_events: config.max_pending_events.get(),
            max_scans: config.max_pending_scans.get(),
        }));
        let handle = NativeHandle {
            shared: Arc::clone(&shared),
        };
        let transport = Self {
            sink,
            caps: config.caps,
            shared,
        };
        (transport, handle)
    }

    /// Sends the disconnects decided inside callbacks.
    fn flush_disconnects(&self, links: Vec<LinkId>) {
        for link in links {
            self.sink.disconnect(link);
        }
    }

    fn take_disconnects(inner: &mut Inner) -> Vec<LinkId> {
        std::mem::take(&mut inner.to_disconnect)
    }
}

fn advertise_mode(mode: PowerMode) -> AdvertiseMode {
    match mode {
        PowerMode::LowPower => AdvertiseMode::LowPower,
        PowerMode::Balanced => AdvertiseMode::Balanced,
        PowerMode::LowLatency => AdvertiseMode::LowLatency,
    }
}

fn scan_mode(mode: PowerMode) -> ScanMode {
    match mode {
        PowerMode::LowPower => ScanMode::LowPower,
        PowerMode::Balanced => ScanMode::Balanced,
        PowerMode::LowLatency => ScanMode::LowLatency,
    }
}

impl Transport for NativeTransport {
    fn caps(&self) -> TransportCaps {
        self.caps
    }

    fn start(&mut self) {
        let mut inner = lock(&self.shared);
        inner.running = true;
        if let Some(state) = inner.radio {
            inner.events.push_back(TransportEvent::RadioState(state));
        }
    }

    fn stop(&mut self) {
        let links = {
            let mut inner = lock(&self.shared);
            inner.running = false;
            inner.scans.clear();
            let mut links = Self::take_disconnects(&mut inner);
            for (&link, slot) in &mut inner.links {
                if slot.closing.is_none() {
                    slot.closing = Some(Cause::Asked);
                    links.push(link);
                }
            }
            links
        };
        self.sink.set_advertising(None, AdvertiseMode::Balanced);
        self.sink.set_scanning(None);
        self.flush_disconnects(links);
    }

    fn set_advertising(&mut self, profile: Option<AdvProfile>) {
        if !lock(&self.shared).running {
            return;
        }
        match profile {
            Some(p) => self
                .sink
                .set_advertising(Some(p.beacon), advertise_mode(p.mode)),
            None => self.sink.set_advertising(None, AdvertiseMode::Balanced),
        }
    }

    fn set_scanning(&mut self, profile: Option<ScanProfile>) {
        if !lock(&self.shared).running {
            return;
        }
        self.sink.set_scanning(profile.map(|p| scan_mode(p.mode)));
    }

    fn connect(&mut self, peer: PeerKey) -> LinkId {
        let (link, running) = {
            let mut inner = lock(&self.shared);
            let link = inner.alloc(peer, false, false);
            if !inner.running {
                inner.links.remove(&link);
                inner.events.push_back(TransportEvent::ConnectFailed {
                    link,
                    error: ConnectError::RadioUnavailable,
                });
            }
            (link, inner.running)
        };
        if running {
            self.sink.connect(link, peer);
        }
        link
    }

    fn disconnect(&mut self, link: LinkId) {
        let asked = {
            let mut inner = lock(&self.shared);
            match inner.links.get_mut(&link) {
                Some(slot) if slot.closing.is_none() && !slot.hidden => {
                    slot.closing = Some(Cause::Asked);
                    true
                }
                _ => false,
            }
        };
        if asked {
            self.sink.disconnect(link);
        }
    }

    fn write(&mut self, link: LinkId, chunk: Vec<u8>) {
        let allowed = {
            let mut inner = lock(&self.shared);
            match inner.live(link) {
                Some(slot) if slot.credit => {
                    slot.credit = false;
                    true
                }
                // Without its credit the write is not sent (contract rule
                // 4.3); the link layer never does this.
                _ => false,
            }
        };
        if allowed {
            self.sink.write(link, chunk);
        }
    }

    fn poll_event(&mut self, cx: &mut Context<'_>) -> Poll<TransportEvent> {
        let (event, links) = {
            let mut inner = lock(&self.shared);
            let links = Self::take_disconnects(&mut inner);
            let event = inner.events.pop_front().or_else(|| {
                inner
                    .scans
                    .pop_first()
                    .map(|(peer, (rssi, beacon))| TransportEvent::Discovered { peer, rssi, beacon })
            });
            if event.is_none() {
                inner.waker = Some(cx.waker().clone());
            }
            (event, links)
        };
        self.flush_disconnects(links);
        event.map_or(Poll::Pending, Poll::Ready)
    }
}

impl Drop for NativeTransport {
    /// Closes every link Kotlin still holds; later callbacks are no-ops.
    fn drop(&mut self) {
        self.stop();
        lock(&self.shared).shut_down = true;
    }
}

impl NativeHandle {
    /// Runs `f` on the shared state unless the transport is gone, then
    /// wakes the poller if an event is waiting.
    fn with<R>(&self, f: impl FnOnce(&mut Inner) -> R) -> Option<R> {
        let (result, waker) = {
            let mut inner = lock(&self.shared);
            if inner.shut_down {
                return None;
            }
            let result = f(&mut inner);
            let ready = !inner.events.is_empty()
                || !inner.scans.is_empty()
                || !inner.to_disconnect.is_empty();
            let waker = if ready { inner.waker.take() } else { None };
            (result, waker)
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        Some(result)
    }
}

impl NativeEvents for NativeHandle {
    fn on_radio_state(&self, state: RadioState) {
        self.with(|inner| {
            inner.radio = Some(state);
            if inner.running {
                inner.events.push_back(TransportEvent::RadioState(state));
            }
        });
    }

    /// Ignored: discovery restarts on the next profile change.
    fn on_discovery_failed(&self, _kind: DiscoveryKind, _code: i32) {}

    fn on_scan_result(&self, peer: PeerKey, rssi: i16, beacon: Vec<u8>) {
        self.with(|inner| {
            let fits = inner.scans.len() < inner.max_scans || inner.scans.contains_key(&peer);
            if inner.running && fits && beacon.len() <= MAX_SCAN_BEACON {
                inner.scans.insert(peer, (rssi, beacon));
            }
        });
    }

    fn on_connected(&self, link: LinkId, mtu: u16) {
        self.with(|inner| {
            let Some(slot) = inner.links.get_mut(&link) else {
                return;
            };
            if slot.up || slot.closing.is_some() {
                return;
            }
            slot.up = true;
            slot.credit = true;
            let peer = slot.peer;
            inner.events.push_back(TransportEvent::Connected {
                link,
                peer,
                role: LinkRole::Initiator,
                mtu,
            });
        });
    }

    fn on_accepted(&self, peer: PeerKey, mtu: u16) -> LinkId {
        let link = self.with(|inner| {
            if !inner.running {
                let link = inner.alloc(peer, true, true);
                inner.to_disconnect.push(link);
                return link;
            }
            let link = inner.alloc(peer, true, false);
            inner.events.push_back(TransportEvent::Connected {
                link,
                peer,
                role: LinkRole::Responder,
                mtu,
            });
            link
        });
        // After shutdown no id is ever used again; 0 is never allocated.
        link.unwrap_or(LinkId::new(0))
    }

    fn on_mtu_changed(&self, link: LinkId, mtu: u16) {
        self.with(|inner| {
            if inner.live(link).is_some() {
                inner.push_link_event(link, TransportEvent::MtuChanged { link, mtu });
            }
        });
    }

    fn on_bytes(&self, link: LinkId, bytes: Vec<u8>) {
        self.with(|inner| {
            if inner.live(link).is_none() {
                return;
            }
            // Dropping the value would leave a gap in the link's bytes.
            if bytes.len() > MAX_ATT_VALUE {
                inner.close(link, Cause::Oversize);
                return;
            }
            let event = TransportEvent::Received { link, chunk: bytes };
            inner.push_link_event(link, event);
        });
    }

    fn on_write_ready(&self, link: LinkId) {
        self.with(|inner| {
            if let Some(slot) = inner.live(link) {
                slot.credit = true;
                inner.push_link_event(link, TransportEvent::WriteReady { link });
            }
        });
    }

    fn on_closed(&self, link: LinkId, status: NativeStatus) {
        self.with(|inner| {
            let Some(slot) = inner.links.remove(&link) else {
                return;
            };
            if slot.hidden {
                return;
            }
            // Terminal events are always queued, past the cap: one per link.
            inner.events.push_back(if slot.up {
                TransportEvent::Disconnected {
                    link,
                    reason: disconnect_reason(status, slot.closing),
                }
            } else {
                TransportEvent::ConnectFailed {
                    link,
                    error: connect_error(status, slot.closing),
                }
            });
        });
    }
}

/// Contract section 8.1, for a link that never came up.
fn connect_error(status: NativeStatus, cause: Option<Cause>) -> ConnectError {
    if cause.is_some() {
        return ConnectError::Cancelled;
    }
    match status {
        NativeStatus::Gatt {
            stage: Stage::Connect,
            code,
        } => match code {
            0x08 | 0x22 => ConnectError::Timeout,
            0x09 | 0x0D | 0x13 | 0x14 | 0x15 => ConnectError::Rejected,
            _ => ConnectError::Unreachable,
        },
        NativeStatus::Gatt { .. } | NativeStatus::Refused { .. } => ConnectError::SetupFailed,
        NativeStatus::ServiceMissing => ConnectError::ServiceMissing,
        NativeStatus::PeerUnknown => ConnectError::PeerUnknown,
        NativeStatus::RadioOff | NativeStatus::PermissionDenied => ConnectError::RadioUnavailable,
        // `Requested` without a request, or `PeerMisbehaved` before any
        // inbound link exists: Kotlin broke the contract.
        NativeStatus::Requested | NativeStatus::PeerMisbehaved => ConnectError::Unreachable,
    }
}

/// Contract section 8.2, for a link that was up.
fn disconnect_reason(status: NativeStatus, cause: Option<Cause>) -> DisconnectReason {
    match (cause, status) {
        (Some(Cause::Asked), _) => DisconnectReason::Closed,
        (Some(Cause::Overflow | Cause::Oversize), _) => DisconnectReason::TransportLost,
        (None, NativeStatus::PeerMisbehaved) => DisconnectReason::ProtocolViolation,
        (None, _) => DisconnectReason::TransportLost,
    }
}
