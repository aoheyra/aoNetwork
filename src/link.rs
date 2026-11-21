//! One link's lifecycle: from the transport's `Connected` through the
//! `Hello` exchange to `Ready`, and down again with a typed reason.
//!
//! [`Link`] is sans-IO. The owner routes this link's [`TransportEvent`]s
//! in, submits payloads with [`Link::send`], calls [`Link::on_tick`] from
//! its timer, and carries out the returned [`LinkAction`]s. Time is
//! injected.

use std::collections::VecDeque;
use std::time::Duration;

use crate::codec::data_frame_len;
use crate::guard::TokenBucket;
use crate::{
    CAP_CREDIT, Channel, ConnectError, DisconnectReason, Fragmenter, Frame, FrameError,
    HandshakeFailure, Hello, LinkMetrics, MAX_MTU, MIN_MTU, NetworkConfig, Reassembler, SendError,
    SendOpts, SendQueue, TransportEvent,
};

/// Where a link is in its lifecycle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub enum LinkState {
    /// Outbound link waiting for the transport to connect.
    Connecting,
    /// Connected; settling the MTU the link fragments for. The transports
    /// report a settled MTU in `Connected`, so this passes straight to
    /// [`Handshaking`](Self::Handshaking) in the same step.
    Negotiating,
    /// Our `Hello` is queued; waiting for a valid `Hello` from the peer.
    Handshaking,
    /// Both sides agreed on a version; data may flow.
    Ready,
    /// We are closing; waiting for the transport to confirm.
    Closing,
    /// Ended. No further actions.
    Closed,
}

/// Something the owner must do for this link.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkAction {
    /// Write this chunk with `Transport::write`.
    Write(Vec<u8>),
    /// Close the transport link with `Transport::disconnect`.
    Disconnect,
    /// The handshake succeeded.
    Ready {
        /// The peer's `Hello`, as received.
        remote: Hello,
        /// The highest link protocol version both sides speak.
        version: u8,
    },
    /// A data frame arrived on a ready link.
    Data {
        /// Its channel.
        channel: Channel,
        /// Its payload.
        payload: Vec<u8>,
    },
    /// The link has ended. Always the last action.
    Down(LinkEnd),
}

/// How a link ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LinkEnd {
    /// It never connected.
    ConnectFailed(ConnectError),
    /// It connected and then went down.
    Disconnected(DisconnectReason),
}

/// The state machine for one link.
///
/// One outgoing frame is in flight at a time and one chunk of it per
/// transport write credit. Payloads wait in a [`SendQueue`] and the next
/// one is taken only when the frame before it is fully written, once the
/// link is ready. Payloads still queued when the link closes are discarded
/// with it. A peer frame other than `Hello` before the
/// handshake, a malformed frame or a broken chunk stream closes the link
/// with [`DisconnectReason::ProtocolViolation`]. A peer that sends more
/// frames or bytes than the configured limits allow closes it with
/// [`DisconnectReason::AbuseDetected`].
///
/// When both `Hello`s set [`CAP_CREDIT`], each side grants the other its
/// receive window per channel on reaching `Ready`, and grants the bytes
/// of each `Data` frame back once it has handed the frame on. A frame
/// waits until its channel has credit for all of it, so a receiver that
/// stops processing stops the sender; a peer that sends past its grant
/// closes the link with [`DisconnectReason::ProtocolViolation`].
#[derive(Debug)]
pub struct Link {
    local: Hello,
    max_frame: usize,
    handshake_timeout: Duration,
    keepalive_interval: Duration,
    max_missed_pongs: u8,
    state: LinkState,
    mtu: u16,
    deadline: Duration,
    reassembler: Reassembler,
    /// Chunks of the frames being written, in send order.
    outbox: VecDeque<Vec<u8>>,
    queue: SendQueue,
    can_write: bool,
    /// The peer's `max_frame`, once its `Hello` has arrived.
    remote_max_frame: Option<usize>,
    /// Why we are closing; reported when the transport confirms.
    closing: Option<DisconnectReason>,
    /// Disconnect once the outbox drains (a `Bye` is in it).
    disconnect_when_sent: bool,
    /// When the unanswered ping went out.
    ping_sent_at: Option<Duration>,
    /// Pings in a row that went unanswered while nothing arrived.
    missed_pongs: u8,
    metrics: LinkMetrics,
    /// Meters the peer's frames, each counted once reassembled.
    frames_in: TokenBucket,
    /// Meters the peer's bytes, counted per received chunk.
    bytes_in: TokenBucket,
    /// Our receive window per channel, granted to a flow-controlled peer.
    windows: [usize; 4],
    /// Credit accounting, once both sides have agreed to it.
    flow: Option<Flow>,
    /// Chunks written so far, to tell when a queued grant has gone out.
    written: u64,
}

/// Per-channel credit accounting on a flow-controlled link, in bytes of
/// encoded `Data` frames. What the peer may send counts only grants
/// already written to it: it cannot have seen the rest.
#[derive(Debug, Default)]
struct Flow {
    /// What the peer has granted us and we have not spent.
    credit: [u64; 4],
    /// What we have granted and written, and the peer has not sent.
    allowed: [usize; 4],
    /// What we have granted that still waits in the outbox.
    granting: [usize; 4],
    /// Those grants in outbox order: the write count that completes each
    /// `Credit` frame, its channel index and its bytes.
    unsent: VecDeque<(u64, usize, usize)>,
    /// What the peer sent, we handed on, and we have not granted back.
    owed: [usize; 4],
}

impl Link {
    /// A link that will introduce itself with `local`. It starts in
    /// [`LinkState::Connecting`]; feed it the transport's `Connected`
    /// (outbound or inbound) to begin the handshake.
    pub fn new(config: &NetworkConfig, local: Hello) -> Self {
        Self {
            local,
            max_frame: config.max_frame(),
            handshake_timeout: config.handshake_timeout(),
            keepalive_interval: config.keepalive_interval(),
            max_missed_pongs: config.max_missed_pongs(),
            state: LinkState::Connecting,
            mtu: 0,
            deadline: Duration::ZERO,
            reassembler: Reassembler::new(config),
            outbox: VecDeque::new(),
            queue: SendQueue::new(config),
            can_write: false,
            remote_max_frame: None,
            closing: None,
            disconnect_when_sent: false,
            ping_sent_at: None,
            missed_pongs: 0,
            metrics: LinkMetrics::default(),
            // Full from the start, so the time they were made at
            // does not matter.
            frames_in: TokenBucket::new(config.frame_limit(), Duration::ZERO),
            bytes_in: TokenBucket::new(config.byte_limit(), Duration::ZERO),
            windows: Channel::ALL.map(|channel| config.receive_window(channel)),
            flow: None,
            written: 0,
        }
    }

    /// What this link has seen so far.
    pub fn metrics(&self) -> LinkMetrics {
        LinkMetrics {
            queue_drops: self.queue.drops(),
            ..self.metrics
        }
    }

    /// Bytes waiting in the send queue, as counted against its caps.
    pub fn queued_bytes(&self) -> usize {
        Channel::ALL
            .iter()
            .map(|&channel| self.queue.queued_bytes(channel))
            .sum()
    }

    /// The current state.
    pub fn state(&self) -> LinkState {
        self.state
    }

    /// When [`on_tick`](Self::on_tick) next has work: the handshake
    /// deadline, or on a ready link the next keepalive step, the
    /// reassembly deadline of a partial frame, or the earliest queued
    /// frame deadline - ticking then prunes it, so an idle link never
    /// holds an expired payload.
    pub fn next_deadline(&self) -> Option<Duration> {
        let expiry = self.queue.earliest_expiry();
        let at = |due: Duration| expiry.map_or(due, |e| due.min(e));
        match self.state {
            LinkState::Negotiating | LinkState::Handshaking => Some(at(self.deadline)),
            LinkState::Ready => {
                let keepalive = self.keepalive_at();
                Some(at(self
                    .reassembler
                    .deadline()
                    .map_or(keepalive, |d| d.min(keepalive))))
            }
            _ => None,
        }
    }

    /// Queues `payload` on `channel` at `now`. Before the handshake
    /// completes it waits in the queue, unchecked against the peer's
    /// `max_frame`, which is not known yet.
    ///
    /// # Errors
    ///
    /// [`SendError::QueueFull`] if the channel is at its cap,
    /// [`SendError::TooLarge`] if its frame exceeds the peer's `max_frame`,
    /// or [`SendError::LinkClosed`] once the link is closing; each returns
    /// the payload unchanged.
    pub fn send(
        &mut self,
        now: Duration,
        channel: Channel,
        payload: Vec<u8>,
    ) -> Result<Vec<LinkAction>, SendError> {
        self.send_opts(now, channel, payload, SendOpts::default())
    }

    /// [`send`](Self::send) with [`SendOpts`]: a coalesce key or a frame
    /// deadline for this payload. Expired frames waiting in the queue are
    /// dropped at `now` before admission, so a deadline cannot hold cap
    /// bytes against a new push.
    ///
    /// # Errors
    ///
    /// As [`send`](Self::send).
    pub fn send_opts(
        &mut self,
        now: Duration,
        channel: Channel,
        payload: Vec<u8>,
        opts: SendOpts,
    ) -> Result<Vec<LinkAction>, SendError> {
        if matches!(self.state, LinkState::Closing | LinkState::Closed) {
            return Err(SendError::LinkClosed(payload));
        }
        if self
            .remote_max_frame
            .is_some_and(|max| data_frame_len(payload.len()) > max)
        {
            return Err(SendError::TooLarge(payload));
        }
        self.queue.prune(now);
        self.queue.push_opts(channel, opts, payload)?;
        let mut out = Vec::new();
        self.fill(now);
        self.flush(&mut out);
        Ok(out)
    }

    /// Feeds one transport event for this link, received at `now`.
    /// Events for other links or about discovery are ignored.
    pub fn on_event(&mut self, now: Duration, event: TransportEvent) -> Vec<LinkAction> {
        let mut out = Vec::new();
        if self.state == LinkState::Closed {
            return out;
        }
        match event {
            TransportEvent::Connected { mtu, .. } if self.state == LinkState::Connecting => {
                // Negotiating ends at once: the MTU arrives settled.
                self.mtu = mtu;
                self.can_write = true;
                self.deadline = now + self.handshake_timeout;
                self.state = LinkState::Handshaking;
                if !self.mtu_ok(&mut out) {
                    return out;
                }
                self.enqueue(&Frame::Hello(self.local.clone()));
                self.flush(&mut out);
            }
            TransportEvent::MtuChanged { mtu, .. } if self.state != LinkState::Closing => {
                self.mtu = mtu;
                self.mtu_ok(&mut out);
            }
            TransportEvent::WriteReady { .. } => {
                self.can_write = true;
                self.fill(now);
                self.flush(&mut out);
            }
            TransportEvent::Received { chunk, .. } => {
                self.metrics.on_rx(now, chunk.len());
                // Anything from the peer shows it is alive.
                self.missed_pongs = 0;
                self.receive(now, &chunk, &mut out);
            }
            TransportEvent::Rssi { rssi, .. } => self.metrics.on_rssi(rssi),
            TransportEvent::ConnectFailed { error, .. } => {
                self.end(LinkEnd::ConnectFailed(error), &mut out);
            }
            TransportEvent::Disconnected { reason, .. } => {
                let reason = self.closing.unwrap_or(reason);
                self.end(LinkEnd::Disconnected(reason), &mut out);
            }
            _ => {}
        }
        out
    }

    /// Runs the handshake, keepalive and reassembly timers at `now`.
    ///
    /// A ready link pings after `keepalive_interval` with nothing
    /// received, and again each `keepalive_interval` without a pong. It
    /// closes with [`DisconnectReason::KeepaliveTimeout`] after
    /// `max_missed_pongs` such pings in a row, so a silent peer is closed
    /// `keepalive_interval x (max_missed_pongs + 1)` after it last sent.
    pub fn on_tick(&mut self, now: Duration) -> Vec<LinkAction> {
        let mut out = Vec::new();
        // An idle queue expires on time too; without this the deadline
        // above would fire again on every poll.
        self.queue.prune(now);
        let handshaking = matches!(self.state, LinkState::Negotiating | LinkState::Handshaking);
        if !handshaking && self.state != LinkState::Ready {
            return out;
        }
        if handshaking && now >= self.deadline {
            self.fail(
                DisconnectReason::HandshakeFailed(HandshakeFailure::Timeout),
                &mut out,
            );
        } else if self.reassembler.poll_timeout(now).is_err() {
            self.fail(DisconnectReason::ProtocolViolation, &mut out);
        } else if self.state == LinkState::Ready && now >= self.keepalive_at() {
            self.keepalive(now, &mut out);
        }
        out
    }

    /// When the next ping is due: an interval after the unanswered ping,
    /// or after the last chunk received.
    fn keepalive_at(&self) -> Duration {
        let since = self
            .ping_sent_at
            .or(self.metrics.last_rx)
            .unwrap_or(Duration::ZERO);
        since.saturating_add(self.keepalive_interval)
    }

    fn keepalive(&mut self, now: Duration, out: &mut Vec<LinkAction>) {
        if self.ping_sent_at.is_some() {
            self.missed_pongs = self.missed_pongs.saturating_add(1);
            if self.missed_pongs >= self.max_missed_pongs {
                return self.fail(DisconnectReason::KeepaliveTimeout, out);
            }
        }
        self.ping_sent_at = Some(now);
        self.enqueue(&Frame::Ping);
        self.fill(now);
        self.flush(out);
    }

    /// Closes the link for `reason`. A connected link says `Bye` first.
    pub fn close(&mut self, reason: DisconnectReason) -> Vec<LinkAction> {
        let mut out = Vec::new();
        match self.state {
            LinkState::Closing | LinkState::Closed => {}
            LinkState::Connecting => self.fail(reason, &mut out),
            _ => {
                self.closing = Some(reason);
                self.state = LinkState::Closing;
                self.enqueue(&Frame::Bye);
                self.disconnect_when_sent = true;
                self.flush(&mut out);
            }
        }
        out
    }

    fn receive(&mut self, now: Duration, chunk: &[u8], out: &mut Vec<LinkAction>) {
        if self.state == LinkState::Closing {
            return;
        }
        let len = u64::try_from(chunk.len()).unwrap_or(u64::MAX);
        if !self.bytes_in.take(now, len) {
            return self.fail(DisconnectReason::AbuseDetected, out);
        }
        let bytes = match self.reassembler.push(now, chunk) {
            Ok(Some(bytes)) => bytes,
            Ok(None) => return,
            Err(_) => return self.fail(DisconnectReason::ProtocolViolation, out),
        };
        // Counted before decoding, so malformed frames count too.
        if !self.frames_in.take(now, 1) {
            return self.fail(DisconnectReason::AbuseDetected, out);
        }
        let frame = match Frame::decode(&bytes, self.max_frame) {
            Ok(frame) => frame,
            Err(FrameError::AppHelloTooLong { .. }) if self.state == LinkState::Handshaking => {
                return self.fail(
                    DisconnectReason::HandshakeFailed(HandshakeFailure::OversizeHello),
                    out,
                );
            }
            Err(_) => return self.fail(DisconnectReason::ProtocolViolation, out),
        };
        match (self.state, frame) {
            (LinkState::Handshaking, Frame::Hello(remote)) => match self.agree(&remote) {
                Some(version) => {
                    self.state = LinkState::Ready;
                    self.remote_max_frame =
                        Some(usize::try_from(remote.max_frame).unwrap_or(usize::MAX));
                    if self.local.caps & remote.caps & CAP_CREDIT != 0 {
                        // Owing the whole window is what grants it.
                        self.flow = Some(Flow {
                            owed: self.windows,
                            ..Flow::default()
                        });
                        self.grant();
                    }
                    out.push(LinkAction::Ready { remote, version });
                    self.fill(now);
                    self.flush(out);
                }
                None => self.fail(
                    DisconnectReason::HandshakeFailed(HandshakeFailure::VersionMismatch),
                    out,
                ),
            },
            (LinkState::Ready, Frame::Data { channel, payload }) => {
                if let Some(flow) = self.flow.as_mut() {
                    let i = channel.index();
                    let cost = data_frame_len(payload.len());
                    if cost > flow.allowed[i] {
                        return self.fail(DisconnectReason::ProtocolViolation, out);
                    }
                    // Handed on below, so it is ours to grant back.
                    flow.allowed[i] -= cost;
                    flow.owed[i] += cost;
                }
                out.push(LinkAction::Data { channel, payload });
                self.grant();
                self.flush(out);
            }
            (LinkState::Ready | LinkState::Handshaking, Frame::Bye) => {
                self.fail(DisconnectReason::Bye, out);
            }
            (LinkState::Ready, Frame::Ping) => {
                self.enqueue(&Frame::Pong);
                self.fill(now);
                self.flush(out);
            }
            (LinkState::Ready, Frame::Pong) => match self.ping_sent_at.take() {
                Some(sent) => self.metrics.on_rtt(now.saturating_sub(sent)),
                None => self.metrics.on_dropped(),
            },
            (LinkState::Ready, Frame::Credit { channel, credits }) => match self.flow.as_mut() {
                Some(flow) => {
                    let i = channel.index();
                    flow.credit[i] = flow.credit[i].saturating_add(u64::from(credits));
                    self.fill(now);
                    self.flush(out);
                }
                // Not agreed: nothing is waiting on credit.
                None => self.metrics.on_dropped(),
            },
            _ => self.fail(DisconnectReason::ProtocolViolation, out),
        }
    }

    /// The highest version in both ranges, if they overlap.
    fn agree(&self, remote: &Hello) -> Option<u8> {
        let low = self.local.proto_min.max(remote.proto_min);
        let high = self.local.proto_max.min(remote.proto_max);
        (low <= high).then_some(high)
    }

    /// Closes the link if the transport reported an MTU outside the BLE
    /// range: no frame could be fragmented for it.
    fn mtu_ok(&mut self, out: &mut Vec<LinkAction>) -> bool {
        let ok = (MIN_MTU..=MAX_MTU).contains(&self.mtu);
        if !ok {
            self.fail(DisconnectReason::TransportLost, out);
        }
        ok
    }

    /// Grants back what a channel owes once that is worth a frame of its
    /// own: when half the window is owed, or when the peer has less than
    /// `max_frame` granted and may be waiting. Whatever is owed is
    /// therefore granted before the peer can stall, and the window is
    /// never exceeded: allowed, granting and owed together never pass it.
    fn grant(&mut self) {
        let Some(flow) = self.flow.as_mut() else {
            return;
        };
        let mut grants = Vec::new();
        for channel in Channel::ALL {
            let i = channel.index();
            let owed = flow.owed[i];
            let granted = flow.allowed[i] + flow.granting[i];
            if owed == 0 || (granted >= self.max_frame && owed < self.windows[i] / 2) {
                continue;
            }
            // Windows are at most 1 MiB, so this always fits.
            let credits = u32::try_from(owed).unwrap_or(u32::MAX);
            let bytes = credits as usize;
            flow.owed[i] -= bytes;
            flow.granting[i] += bytes;
            grants.push((channel, credits));
        }
        for (channel, credits) in grants {
            self.enqueue(&Frame::Credit { channel, credits });
            let done = self.written + self.outbox.len() as u64;
            if let Some(flow) = self.flow.as_mut() {
                flow.unsent
                    .push_back((done, channel.index(), credits as usize));
            }
        }
    }

    fn enqueue(&mut self, frame: &Frame) {
        // Cannot fail: the MTU is checked whenever it changes.
        if let Ok(chunks) = Fragmenter::new(&frame.encode(), self.mtu) {
            self.outbox.extend(chunks);
        }
    }

    /// Takes the next payload from the queue into the outbox at `now`,
    /// once the transport has a write credit and nothing else is going
    /// out. `pop` drops expired frames first, so a frame past its
    /// deadline is never written.
    fn fill(&mut self, now: Duration) {
        if !self.can_write {
            return;
        }
        if self.outbox.is_empty() && self.state == LinkState::Ready {
            let next = match self.flow.as_mut() {
                None => self.queue.pop(now),
                Some(flow) => {
                    let credit = flow.credit;
                    let next = self.queue.pop_within(now, |channel| {
                        usize::try_from(credit[channel.index()]).unwrap_or(usize::MAX)
                    });
                    if let Some((channel, payload)) = &next {
                        // Fits: `pop_within` checked it against this credit.
                        let cost = data_frame_len(payload.len()) as u64;
                        flow.credit[channel.index()] -= cost;
                    }
                    next
                }
            };
            if let Some((channel, payload)) = next {
                self.enqueue(&Frame::Data { channel, payload });
            }
        }
    }

    fn flush(&mut self, out: &mut Vec<LinkAction>) {
        if !self.can_write {
            return;
        }
        if let Some(chunk) = self.outbox.pop_front() {
            self.can_write = false;
            self.metrics.on_tx(chunk.len());
            out.push(LinkAction::Write(chunk));
            self.written += 1;
            if let Some(flow) = self.flow.as_mut() {
                // A grant counts once its last chunk is written.
                while let Some(&(done, i, bytes)) = flow.unsent.front() {
                    if done > self.written {
                        break;
                    }
                    flow.unsent.pop_front();
                    flow.granting[i] -= bytes;
                    flow.allowed[i] += bytes;
                }
            }
        } else if self.disconnect_when_sent {
            self.disconnect_when_sent = false;
            out.push(LinkAction::Disconnect);
        }
    }

    /// Closes at once, without `Bye`, recording `reason` for the
    /// transport's confirmation.
    fn fail(&mut self, reason: DisconnectReason, out: &mut Vec<LinkAction>) {
        self.closing = Some(reason);
        self.state = LinkState::Closing;
        self.outbox.clear();
        self.disconnect_when_sent = false;
        out.push(LinkAction::Disconnect);
    }

    fn end(&mut self, end: LinkEnd, out: &mut Vec<LinkAction>) {
        self.state = LinkState::Closed;
        self.outbox.clear();
        out.push(LinkAction::Down(end));
    }
}
