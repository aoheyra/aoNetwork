//! The per-link send queue: what waits, what goes next, what is refused.

use std::collections::VecDeque;
use std::time::Duration;

use crate::codec::data_frame_len;
use crate::{Channel, NetworkConfig, SendError};

/// Bytes of deficit one unit of weight earns per round. A few BLE chunks,
/// so channels interleave finely; a larger frame waits until its channel
/// has saved up enough rounds.
const QUANTUM: usize = 1024;

/// Deficit round-robin channels in turn order, with their weights.
const DRR: [(Channel, usize); 3] = [
    (Channel::Message, 4),
    (Channel::Telemetry, 2),
    (Channel::Bulk, 1),
];

/// Admission options for one queued payload (NET-21).
///
/// Both fields are optional; [`SendOpts::default()`] queues exactly as a
/// plain push does.
///
/// # Example
///
/// Fifty latest-wins updates admitted under one key leave only the newest
/// one queued:
///
/// ```
/// use std::time::Duration;
/// use ao_network::{Channel, NetworkConfig, SendOpts, SendQueue};
///
/// let mut queue = SendQueue::new(&NetworkConfig::default());
/// let opts = SendOpts {
///     coalesce_key: Some([7; 16]),
///     expires_at: None,
/// };
/// for update in 0..50u8 {
///     queue.push_opts(Channel::Telemetry, opts, vec![update]).unwrap();
/// }
/// assert_eq!(queue.pop(Duration::ZERO), Some((Channel::Telemetry, vec![49])));
/// ```
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SendOpts {
    /// Latest-wins key: a newer push with the same key on the same
    /// channel replaces the still-queued frame with that key, wherever it
    /// waits, and inherits its place in line. A frame that has already
    /// been popped for sending is in flight and is never replaced.
    ///
    /// Keys are opaque to this crate; `None` never coalesces.
    pub coalesce_key: Option<[u8; 16]>,
    /// Absolute deadline on the caller's monotonic timeline - the same
    /// `now` passed to `Network::send`, `Network::poll_event` and
    /// `Network::next_deadline` - after which the payload is dropped
    /// silently instead of written. Past deadlines are rejected by
    /// admission pruning rather than queued and forgotten.
    pub expires_at: Option<Duration>,
}

/// One queued payload, with the options it was admitted under.
#[derive(Debug)]
struct Queued {
    payload: Vec<u8>,
    coalesce_key: Option<[u8; 16]>,
    expires_at: Option<Duration>,
}

/// One link's outbound payloads, by channel.
///
/// [`Channel::Control`] has strict priority. Message, Telemetry and Bulk
/// share the rest by deficit round-robin in bytes, weighted 4, 2 and 1.
/// Each channel has its own byte cap
/// ([`NetworkConfig::channel_cap`]); an entry costs its encoded `Data`
/// frame size, so even empty payloads use up the cap. A push that does not
/// fit is refused with the payload handed back.
///
/// On top of the caps, admission honours [`SendOpts`]: a same-key push
/// replaces a still-queued frame instead of adding bytes, a Telemetry push
/// that has no room evicts the oldest queued Telemetry frames (latest
/// wins, so Telemetry returns [`SendError::QueueFull`] only for a payload
/// larger than the whole channel cap), and expired frames are dropped by
/// [`prune`](Self::prune) and [`pop`](Self::pop) before anything is
/// written. The queue holds no clock: every time-dependent call takes
/// `now` from the caller, and [`Link`](crate::Link) prunes before it
/// admits a push.
///
/// The owner takes one payload at a time with [`pop`](Self::pop) and pops
/// again only once it is sent, so Control waits behind at most the one
/// frame already in flight.
///
/// # Example
///
/// ```
/// use std::time::Duration;
/// use ao_network::{Channel, NetworkConfig, SendQueue};
///
/// let mut queue = SendQueue::new(&NetworkConfig::default());
/// queue.push(Channel::Bulk, vec![1; 100]).unwrap();
/// queue.push(Channel::Control, vec![2; 4]).unwrap();
/// assert_eq!(queue.pop(Duration::ZERO), Some((Channel::Control, vec![2; 4])));
/// assert_eq!(queue.pop(Duration::ZERO), Some((Channel::Bulk, vec![1; 100])));
/// assert_eq!(queue.pop(Duration::ZERO), None);
/// ```
#[derive(Debug)]
pub struct SendQueue {
    caps: [usize; 4],
    queued: [usize; 4],
    entries: [VecDeque<Queued>; 4],
    deficit: [usize; 4],
    /// Index into [`DRR`] of the channel whose turn it is.
    turn: usize,
    /// Whether that channel has had its quantum for this turn.
    granted: bool,
}

impl SendQueue {
    /// An empty queue with the caps from `config`.
    pub fn new(config: &NetworkConfig) -> Self {
        Self {
            caps: Channel::ALL.map(|channel| config.channel_cap(channel)),
            queued: [0; 4],
            entries: Default::default(),
            deficit: [0; 4],
            turn: 0,
            granted: false,
        }
    }

    /// Queues `payload` on `channel` with [`SendOpts::default()`].
    ///
    /// # Errors
    ///
    /// [`SendError::QueueFull`] with the payload unchanged if it does not
    /// fit in what is left of the channel's cap.
    pub fn push(&mut self, channel: Channel, payload: Vec<u8>) -> Result<(), SendError> {
        self.push_opts(channel, SendOpts::default(), payload)
    }

    /// Queues `payload` on `channel` under `opts`.
    ///
    /// A same-key payload replaces the still-queued frame with that key in
    /// place, and the admission check then runs against the freed bytes,
    /// so replacing cannot overflow the cap. Telemetry that does not fit
    /// evicts its own oldest frames first: latest-wins data supersedes
    /// what is waiting. Expired frames are dropped by the next
    /// [`prune`](Self::prune) or [`pop`](Self::pop); this queue holds no
    /// clock, and [`Link`](crate::Link) prunes at the caller's `now`
    /// before calling this.
    ///
    /// # Errors
    ///
    /// [`SendError::QueueFull`] with the payload unchanged if it does not
    /// fit in the channel's cap even after a replacement frees its bytes,
    /// or (for Telemetry) after every waiting frame has been evicted.
    pub fn push_opts(
        &mut self,
        channel: Channel,
        opts: SendOpts,
        payload: Vec<u8>,
    ) -> Result<(), SendError> {
        let i = channel.index();
        let cost = data_frame_len(payload.len());
        if let Some(key) = opts.coalesce_key {
            if let Some(slot) = self.entries[i]
                .iter_mut()
                .find(|entry| entry.coalesce_key == Some(key))
            {
                // The replacement must fit once the old bytes are freed; if
                // it does not, the old frame stays and the new payload
                // comes back.
                let freed = data_frame_len(slot.payload.len());
                if cost > self.caps[i] - self.queued[i] + freed {
                    return Err(SendError::QueueFull(payload));
                }
                // `freed` is part of `queued`, so the subtraction cannot underflow.
                self.queued[i] = self.queued[i] - freed + cost;
                slot.payload = payload;
                slot.expires_at = opts.expires_at;
                return Ok(());
            }
        }
        // Refuse a payload that cannot fit an empty channel before any
        // eviction: evicting everything for an unsendable frame helps nobody.
        if cost > self.caps[i] {
            return Err(SendError::QueueFull(payload));
        }
        if cost > self.caps[i] - self.queued[i] {
            if channel == Channel::Telemetry {
                while cost > self.caps[i] - self.queued[i] {
                    match self.entries[i].pop_front() {
                        Some(oldest) => {
                            self.queued[i] -= data_frame_len(oldest.payload.len());
                        }
                        None => break,
                    }
                }
            }
            if cost > self.caps[i] - self.queued[i] {
                return Err(SendError::QueueFull(payload));
            }
        }
        self.queued[i] += cost;
        self.entries[i].push_back(Queued {
            payload,
            coalesce_key: opts.coalesce_key,
            expires_at: opts.expires_at,
        });
        Ok(())
    }

    /// Takes the next payload to send at `now`, if any, after dropping
    /// every frame whose deadline has passed.
    pub fn pop(&mut self, now: Duration) -> Option<(Channel, Vec<u8>)> {
        self.prune(now);
        if let Some(payload) = self.take(Channel::Control) {
            return Some((Channel::Control, payload));
        }
        if DRR
            .iter()
            .all(|(channel, _)| self.entries[channel.index()].is_empty())
        {
            return None;
        }
        // Terminates: some channel is non-empty and gains a quantum every
        // round until its head fits.
        loop {
            let (channel, weight) = DRR[self.turn];
            let i = channel.index();
            match self.entries[i]
                .front()
                .map(|e| data_frame_len(e.payload.len()))
            {
                Some(cost) if cost <= self.deficit[i] => {
                    self.deficit[i] -= cost;
                    let payload = self.take(channel)?;
                    if self.entries[i].is_empty() {
                        self.deficit[i] = 0;
                    }
                    return Some((channel, payload));
                }
                Some(_) if !self.granted => {
                    self.deficit[i] += weight * QUANTUM;
                    self.granted = true;
                    continue;
                }
                Some(_) => {}
                // An idle channel does not bank deficit.
                None => self.deficit[i] = 0,
            }
            self.turn = (self.turn + 1) % DRR.len();
            self.granted = false;
        }
    }

    /// Drops every queued payload whose `expires_at` has passed `now`,
    /// returning its bytes to the channel cap. Silent: an expired payload
    /// is gone, not reported, and is never written.
    pub fn prune(&mut self, now: Duration) {
        for channel in Channel::ALL {
            let i = channel.index();
            let mut freed = 0usize;
            self.entries[i].retain(|entry| {
                if entry.expires_at.is_some_and(|deadline| deadline <= now) {
                    freed += data_frame_len(entry.payload.len());
                    false
                } else {
                    true
                }
            });
            self.queued[i] -= freed;
        }
    }

    /// The earliest deadline still queued, so the owner can wake in time
    /// to prune an idle link.
    pub fn earliest_expiry(&self) -> Option<Duration> {
        self.entries
            .iter()
            .flatten()
            .filter_map(|entry| entry.expires_at)
            .min()
    }

    /// Bytes queued on `channel`, as counted against its cap.
    ///
    /// Expired frames count until the next [`prune`](Self::prune),
    /// [`pop`](Self::pop) or push through [`Link`](crate::Link), which run
    /// at the caller's `now`.
    pub fn queued_bytes(&self, channel: Channel) -> usize {
        self.queued[channel.index()]
    }

    /// Whether nothing is queued.
    pub fn is_empty(&self) -> bool {
        self.entries.iter().all(VecDeque::is_empty)
    }

    fn take(&mut self, channel: Channel) -> Option<Vec<u8>> {
        let i = channel.index();
        let entry = self.entries[i].pop_front()?;
        self.queued[i] -= data_frame_len(entry.payload.len());
        Some(entry.payload)
    }
}
