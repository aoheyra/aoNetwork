//! `L2capTransport`: [`Transport`] over an L2CAP connection-oriented
//! channel (NET-26).
//!
//! Discovery still uses BLE advertising, but data travels in CoC packets
//! with no ATT header. Kotlin owns the Android sockets; this module owns
//! every id, queue, credit and state transition and stays sans-IO. The
//! Kotlin side of the contract is in `docs/native-contract.md`, section 10.

use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::task::{Context, Poll, Waker};

use crate::{
    AdvProfile, AdvertiseMode, ConnectError, DisconnectReason, LinkId, LinkRole, LinkTransport,
    MAX_MTU, MIN_MTU, PeerKey, PowerMode, RadioState, ScanMode, ScanProfile, Transport,
    TransportCaps, TransportEvent,
};

/// Default dynamic PSM, in the range Android reserves for L2CAP CoC.
pub const DEFAULT_COC_PSM: u16 = 0x1001;

/// Smallest dynamic PSM Kotlin may listen on.
const MIN_COC_PSM: u16 = 0x1001;
/// Largest manufacturer payload legacy advertising can carry.
const MAX_SCAN_BEACON: usize = 31;
/// The chunk header is the only per-packet overhead on a CoC.
const CHUNK_OVERHEAD: usize = 1;

/// A CoC setting was rejected.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum L2capError {
    /// Android dynamic PSMs are odd and start at `0x1001`.
    #[error("invalid L2CAP PSM: {psm}")]
    InvalidPsm {
        /// The rejected value.
        psm: u16,
    },
    /// The MTU is outside the range the link layer can fragment for.
    #[error("unsupported CoC MTU: {mtu}")]
    InvalidMtu {
        /// The rejected value.
        mtu: u16,
    },
}

/// Validated settings for [`L2capTransport`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L2capConfig {
    caps: TransportCaps,
    psm: u16,
    mtu: u16,
    max_pending_events: NonZeroUsize,
    max_pending_scans: NonZeroUsize,
}

impl L2capConfig {
    /// Defaults for `caps`: PSM [`DEFAULT_COC_PSM`], a 512-byte CoC
    /// packet, 1024 pending events and 64 pending scans.
    pub fn new(caps: TransportCaps) -> Self {
        Self {
            caps,
            psm: DEFAULT_COC_PSM,
            mtu: 512,
            max_pending_events: NonZeroUsize::MIN.saturating_add(1023),
            max_pending_scans: NonZeroUsize::MIN.saturating_add(63),
        }
    }

    /// Sets the PSM Kotlin listens on and clients connect to.
    ///
    /// # Errors
    ///
    /// [`L2capError::InvalidPsm`] below `0x1001` or for an even value.
    pub fn with_psm(mut self, psm: u16) -> Result<Self, L2capError> {
        if psm < MIN_COC_PSM || psm % 2 == 0 {
            return Err(L2capError::InvalidPsm { psm });
        }
        self.psm = psm;
        Ok(self)
    }

    /// Sets the largest CoC packet the radio accepts, `23..=517`.
    ///
    /// # Errors
    ///
    /// [`L2capError::InvalidMtu`] outside that range.
    pub fn with_mtu(mut self, mtu: u16) -> Result<Self, L2capError> {
        if !(MIN_MTU..=MAX_MTU).contains(&mtu) {
            return Err(L2capError::InvalidMtu { mtu });
        }
        self.mtu = mtu;
        Ok(self)
    }

    /// Sets how many events may wait for the poller; a link whose event
    /// does not fit is closed with `TransportLost`.
    pub fn with_max_pending_events(mut self, max: NonZeroUsize) -> Self {
        self.max_pending_events = max;
        self
    }

    /// Sets how many peers may hold an undelivered scan result.
    pub fn with_max_pending_scans(mut self, max: NonZeroUsize) -> Self {
        self.max_pending_scans = max;
        self
    }

    /// The PSM for this transport's server socket.
    pub const fn psm(&self) -> u16 {
        self.psm
    }

    /// The CoC packet size used for fragmentation.
    pub const fn mtu(&self) -> u16 {
        self.mtu
    }
}

/// Rust to Kotlin for an L2CAP CoC. Implemented by the BLE shim; every
/// call returns immediately (contract section 9.2).
pub trait L2capSink: Send + Sync {
    /// Starts or updates advertising with `beacon`, or stops it with `None`.
    ///
    /// Contract section 10.2.
    fn set_advertising(&self, beacon: Option<[u8; 16]>, mode: AdvertiseMode);
    /// Starts or changes scanning, or stops it with `None`.
    fn set_scanning(&self, mode: Option<ScanMode>);
    /// Listens for inbound CoCs on `psm`, or stops listening.
    fn set_server(&self, psm: Option<u16>);
    /// Opens a CoC to `peer` on `psm`.
    fn connect(&self, link: LinkId, peer: PeerKey, psm: u16);
    /// Sends `bytes` as one CoC packet on `link`.
    fn write(&self, link: LinkId, bytes: Vec<u8>);
    /// Tears `link` down, then reports `on_closed(link, Requested)`.
    fn disconnect(&self, link: LinkId);
}

/// Kotlin to Rust for an L2CAP CoC. Implemented by [`L2capHandle`]; every
/// method returns immediately (contract section 10.3).
pub trait L2capEvents: Send + Sync {
    /// The adapter or permission state, at start and on every change.
    fn on_radio_state(&self, state: RadioState);
    /// A scan result; `beacon` is the raw manufacturer payload.
    fn on_scan_result(&self, peer: PeerKey, rssi: i16, beacon: Vec<u8>);
    /// An outbound CoC finished setup.
    fn on_connected(&self, link: LinkId, mtu: u16);
    /// An inbound CoC finished setup; returns the id to use for it.
    fn on_accepted(&self, peer: PeerKey, mtu: u16) -> LinkId;
    /// One CoC packet arrived on `link`.
    fn on_bytes(&self, link: LinkId, bytes: Vec<u8>);
    /// The stack accepted the previous write on `link`.
    fn on_write_ready(&self, link: LinkId);
    /// `link` is gone. The last event for it.
    fn on_closed(&self, link: LinkId, status: L2capStatus);
}

/// The CoC step a status came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum L2capStage {
    /// Opening or closing the listening socket.
    Listen,
    /// Opening the channel to a peer.
    Connect,
    /// Writing a packet.
    Write,
}

/// Why Kotlin closed a CoC link, as reported by the shim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum L2capStatus {
    /// Rust called `disconnect`.
    Requested,
    /// A CoC API call failed asynchronously; `code` is the raw Android value.
    Failed {
        /// Where it happened.
        stage: L2capStage,
        /// Raw Android status.
        code: i32,
    },
    /// A CoC API call was refused synchronously.
    Refused {
        /// Where it happened.
        stage: L2capStage,
        /// Raw Android status; -1 for `false` or `null`.
        code: i32,
    },
    /// No `BluetoothDevice` is known for `peer`.
    PeerUnknown,
    /// No server socket is listening on the PSM.
    PsmUnavailable,
    /// The channel setup or teardown took too long.
    Timeout,
    /// The peer broke the CoC procedure.
    PeerMisbehaved,
    /// The adapter was turned off.
    RadioOff,
    /// A BLE permission was revoked.
    PermissionDenied,
}

/// [`Transport`] over a Kotlin [`L2capSink`].
///
/// The event queue is bounded, ids are never reused, and one write credit
/// per link is kept exactly as on GATT. A packet longer than the
/// negotiated CoC MTU, or an event that overflows the queue, closes its
/// link with `TransportLost` rather than being dropped.
#[derive(Debug)]
pub struct L2capTransport {
    sink: Arc<dyn L2capSink>,
    caps: TransportCaps,
    config: L2capConfig,
    shared: Arc<Mutex<Inner>>,
}

/// Kotlin's way back into an [`L2capTransport`]. Calls after the transport
/// is dropped do nothing.
#[derive(Debug, Clone)]
pub struct L2capHandle {
    shared: Arc<Mutex<Inner>>,
}

impl std::fmt::Debug for dyn L2capSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("L2capSink")
    }
}

/// Why Rust closed a link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Cause {
    /// The owner called `disconnect` or `stop`.
    Asked,
    /// Its events overflowed the queue.
    Overflow,
    /// A packet or MTU outside the negotiated size, or an empty packet.
    Oversize,
}

#[derive(Debug)]
struct Slot {
    peer: PeerKey,
    up: bool,
    credit: bool,
    closing: Option<Cause>,
    /// Accepted while stopped; never reported to the owner.
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
    chunk_cap: usize,
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

    /// Queues a non-terminal event, or closes the link if the queue is
    /// full: a gap in its events would break their order.
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
            if slot.closing.is_none() {
                slot.closing = Some(cause);
                self.to_disconnect.push(link);
            }
        }
    }
}

fn lock(shared: &Mutex<Inner>) -> MutexGuard<'_, Inner> {
    // Nothing under this lock panics on input; a poisoned lock still holds
    // consistent state.
    shared.lock().unwrap_or_else(PoisonError::into_inner)
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

impl L2capTransport {
    /// A transport driving `sink`, and the handle Kotlin reports through.
    pub fn new(sink: Arc<dyn L2capSink>, config: L2capConfig) -> (Self, L2capHandle) {
        let caps = TransportCaps {
            max_mtu: config.mtu,
            ..config.caps
        };
        // The chunk header is inside the CoC packet, so a full chunk is
        // exactly one MTU; there is no ATT header to subtract.
        let chunk_cap = usize::from(config.mtu);
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
            chunk_cap,
        }));
        let handle = L2capHandle {
            shared: Arc::clone(&shared),
        };
        let transport = Self {
            sink,
            caps,
            config,
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

impl Transport for L2capTransport {
    fn caps(&self) -> TransportCaps {
        self.caps
    }

    fn link_transport(&self) -> LinkTransport {
        LinkTransport::L2capCoc
    }

    fn start(&mut self) {
        let mut inner = lock(&self.shared);
        inner.running = true;
        if let Some(state) = inner.radio {
            inner.events.push_back(TransportEvent::RadioState(state));
        }
        self.sink.set_server(Some(self.config.psm));
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
        self.sink.set_server(None);
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
        let psm = self.config.psm;
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
            self.sink.connect(link, peer, psm);
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
        let oversize = lock(&self.shared).chunk_cap < chunk.len();
        if oversize {
            let mut inner = lock(&self.shared);
            inner.close(link, Cause::Oversize);
            let links = Self::take_disconnects(&mut inner);
            drop(inner);
            self.flush_disconnects(links);
            return;
        }
        let allowed = {
            let mut inner = lock(&self.shared);
            match inner.live(link) {
                Some(slot) if slot.credit => {
                    slot.credit = false;
                    true
                }
                // Without its credit the write is not sent; the link
                // layer never does this.
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

impl Drop for L2capTransport {
    fn drop(&mut self) {
        self.stop();
        lock(&self.shared).shut_down = true;
    }
}

impl L2capHandle {
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

impl L2capEvents for L2capHandle {
    fn on_radio_state(&self, state: RadioState) {
        self.with(|inner| {
            inner.radio = Some(state);
            if inner.running {
                inner.events.push_back(TransportEvent::RadioState(state));
            }
        });
    }

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
            let peer = slot.peer;
            if !(MIN_MTU..=MAX_MTU).contains(&mtu)
                || usize::from(mtu) <= CHUNK_OVERHEAD
                || usize::from(mtu) > inner.chunk_cap
            {
                inner.close(link, Cause::Oversize);
                return;
            }
            slot.up = true;
            slot.credit = true;
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
            if !inner.running
                || !(MIN_MTU..=MAX_MTU).contains(&mtu)
                || usize::from(mtu) <= CHUNK_OVERHEAD
                || usize::from(mtu) > inner.chunk_cap
            {
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

    fn on_bytes(&self, link: LinkId, bytes: Vec<u8>) {
        self.with(|inner| {
            if inner.live(link).is_none() {
                return;
            }
            // Dropping a packet would leave a gap in the link's bytes.
            if bytes.is_empty() || bytes.len() > inner.chunk_cap {
                inner.close(link, Cause::Oversize);
                return;
            }
            inner.push_link_event(link, TransportEvent::Received { link, chunk: bytes });
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

    fn on_closed(&self, link: LinkId, status: L2capStatus) {
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

/// Contract section 10.5, for a link that never came up.
fn connect_error(status: L2capStatus, cause: Option<Cause>) -> ConnectError {
    if cause.is_some() {
        return ConnectError::Cancelled;
    }
    match status {
        L2capStatus::Failed {
            stage: L2capStage::Connect,
            code: 0x08 | 0x22,
        }
        | L2capStatus::Timeout => ConnectError::Timeout,
        L2capStatus::Failed {
            stage: L2capStage::Connect,
            ..
        } => ConnectError::Rejected,
        L2capStatus::Failed { .. } | L2capStatus::Refused { .. } => ConnectError::SetupFailed,
        L2capStatus::PeerUnknown => ConnectError::PeerUnknown,
        L2capStatus::PsmUnavailable => ConnectError::ServiceMissing,
        L2capStatus::RadioOff | L2capStatus::PermissionDenied => ConnectError::RadioUnavailable,
        // `Requested` without a request, or a misbehaving peer before an
        // inbound link exists: Kotlin broke the contract.
        L2capStatus::Requested | L2capStatus::PeerMisbehaved => ConnectError::Unreachable,
    }
}

/// Contract section 10.5, for a link that was up.
fn disconnect_reason(status: L2capStatus, cause: Option<Cause>) -> DisconnectReason {
    match (cause, status) {
        (Some(Cause::Asked), _) => DisconnectReason::Closed,
        (Some(Cause::Overflow | Cause::Oversize), _) => DisconnectReason::TransportLost,
        (None, L2capStatus::PeerMisbehaved) => DisconnectReason::ProtocolViolation,
        (None, _) => DisconnectReason::TransportLost,
    }
}
