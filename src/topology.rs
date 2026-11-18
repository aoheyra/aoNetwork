//! The bounded table of peers seen in beacons (NET-15).
//!
//! Every `Discovered` sighting is recorded here with a moving-average
//! RSSI and the latest beacon that decoded. The table answers the link
//! manager's question "who may be connected to right now" through
//! [`PeerTable::candidates`]: a peer must not already be linked or
//! connecting, must be past the backoff of its last failed attempt,
//! must say it accepts links, must be strong enough, and must have been
//! seen within [`IDLE_TIMEOUT`]. Failed attempts wait out an
//! exponential backoff with a deterministic per-peer jitter, so peers
//! that fail together do not retry together while a run still
//! reproduces from its inputs.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::metrics::rssi_ewma;
use crate::{Beacon, LinkId, NetworkConfig, PeerKey};

/// Entries unseen for longer than this are evicted, so a peer that stops
/// advertising does not hold memory or a candidate slot forever.
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// The first retry delay before jitter: it doubles with each failure.
const BACKOFF_BASE_MS: u64 = 1_000;
/// Retries never wait longer than this, jitter included.
const BACKOFF_MAX: Duration = Duration::from_secs(60);

/// Where a discovered peer stands with this node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerState {
    /// Known and connectable once its backoff has passed.
    Idle,
    /// A connection attempt to it is in flight.
    Connecting,
    /// A link to it is up.
    Linked,
    /// The last attempt or link failed; waiting out its backoff.
    Cooldown,
}

/// One peer seen in beacons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiscoveredPeer {
    /// When it was first seen.
    pub(crate) first_seen: Duration,
    /// When it was last seen.
    pub(crate) last_seen: Duration,
    /// Moving average of its beacon RSSI in dBm.
    pub(crate) rssi: i16,
    /// The latest beacon that decoded; `None` while it has only ever
    /// advertised nothing or bytes that do not decode.
    pub(crate) beacon: Option<Beacon>,
    /// Where it stands with this node.
    pub(crate) state: PeerState,
    /// Failed attempts since the last successful handshake, saturating
    /// and capped by the backoff formula itself.
    pub(crate) failures: u8,
    /// Earliest time a connect attempt may start.
    pub(crate) next_attempt_at: Duration,
}

/// The peer table: bounded by `max_peers`, evicting the stalest
/// connectable entry when full and anything unseen past [`IDLE_TIMEOUT`]
/// on every observation.
#[derive(Debug)]
pub(crate) struct PeerTable {
    entries: HashMap<PeerKey, DiscoveredPeer>,
    max: usize,
    min_rssi: i16,
}

impl PeerTable {
    pub(crate) fn new(config: &NetworkConfig) -> Self {
        Self {
            entries: HashMap::new(),
            max: usize::try_from(config.max_peers()).unwrap_or(usize::MAX),
            min_rssi: config.min_rssi(),
        }
    }

    /// Records a sighting of `peer` and returns whether *this*
    /// sighting decoded as a beacon.
    ///
    /// An existing entry is refreshed in place; a new peer makes room by
    /// evicting the stalest entry that is not mid-attempt, and is
    /// dropped only if every entry is owed its attempt result.
    pub(crate) fn observe(&mut self, now: Duration, peer: PeerKey, rssi: i16, raw: &[u8]) -> bool {
        let beacon = Beacon::decode(raw).ok();
        // A connecting attempt is still owed its result, so it is never
        // evicted; everything else past the idle timeout goes first.
        let stale = |entry: &DiscoveredPeer| {
            entry.state != PeerState::Connecting
                && now.saturating_sub(entry.last_seen) > IDLE_TIMEOUT
        };
        self.entries.retain(|_, entry| !stale(entry));
        if let Some(entry) = self.entries.get_mut(&peer) {
            entry.last_seen = now;
            entry.rssi = rssi_ewma(entry.rssi, rssi);
            if let Some(decoded) = beacon {
                entry.beacon = Some(decoded);
            }
            return beacon.is_some();
        }
        while self.entries.len() >= self.max {
            let stalest = self
                .entries
                .iter()
                .filter(|(_, entry)| entry.state != PeerState::Connecting)
                .min_by_key(|(_, entry)| entry.last_seen)
                .map(|(&key, _)| key);
            match stalest {
                Some(key) => {
                    self.entries.remove(&key);
                }
                None => break,
            }
        }
        if self.entries.len() >= self.max {
            return beacon.is_some();
        }
        self.entries.insert(
            peer,
            DiscoveredPeer {
                first_seen: now,
                last_seen: now,
                rssi,
                beacon,
                state: PeerState::Idle,
                failures: 0,
                next_attempt_at: Duration::ZERO,
            },
        );
        beacon.is_some()
    }

    /// Records that an attempt to `peer` is in flight.
    pub(crate) fn mark_connecting(&mut self, peer: &PeerKey) {
        if let Some(entry) = self.entries.get_mut(peer) {
            entry.state = PeerState::Connecting;
        }
    }

    /// Records a successful handshake: linked, and the failure streak is
    /// over, so a later drop backs off from a clean slate.
    pub(crate) fn mark_linked(&mut self, peer: &PeerKey) {
        if let Some(entry) = self.entries.get_mut(peer) {
            entry.state = PeerState::Linked;
            entry.failures = 0;
        }
    }

    /// Records that the attempt or link to `peer` ended at `now` without
    /// success: the peer enters `Cooldown` and no attempt may start
    /// before its backoff passes. A peer already cooling down is left
    /// alone, so repeated reports cannot stack delays.
    pub(crate) fn mark_down(&mut self, now: Duration, peer: &PeerKey) {
        let Some(entry) = self.entries.get_mut(peer) else {
            return;
        };
        if !matches!(entry.state, PeerState::Connecting | PeerState::Linked) {
            return;
        }
        entry.state = PeerState::Cooldown;
        entry.failures = entry.failures.saturating_add(1);
        entry.next_attempt_at = now
            .checked_add(backoff(entry.failures, *peer))
            .unwrap_or(Duration::MAX);
    }

    /// Whether `peer` may be connected to at `now`: not already linked
    /// or connecting, past its backoff, advertising that it accepts
    /// links, at or above the configured minimum RSSI, and seen within
    /// [`IDLE_TIMEOUT`].
    pub(crate) fn eligible(
        &self,
        peer: &PeerKey,
        now: Duration,
        linked: &BTreeMap<PeerKey, LinkId>,
    ) -> bool {
        let Some(entry) = self.entries.get(peer) else {
            return false;
        };
        !linked.contains_key(peer)
            && matches!(entry.state, PeerState::Idle | PeerState::Cooldown)
            && entry.next_attempt_at <= now
            && entry
                .beacon
                .as_ref()
                .is_some_and(|beacon| beacon.flags.accepting)
            && entry.rssi >= self.min_rssi
            && now.saturating_sub(entry.last_seen) <= IDLE_TIMEOUT
    }

    /// Every peer connectable at `now`, in `PeerKey` order so a run
    /// makes the same choices every time. `linked` is the live set of
    /// links and in-flight attempts, which the table cannot know for a
    /// peer it has evicted and seen again.
    pub(crate) fn candidates(
        &self,
        now: Duration,
        linked: &BTreeMap<PeerKey, LinkId>,
    ) -> Vec<PeerKey> {
        let mut out: Vec<PeerKey> = self
            .entries
            .keys()
            .copied()
            .filter(|peer| self.eligible(peer, now, linked))
            .collect();
        out.sort();
        out
    }

    /// The peer's latest advertised hint, for the connect tie-break.
    pub(crate) fn hint_of(&self, peer: &PeerKey) -> Option<[u8; 8]> {
        self.entries.get(peer)?.beacon.map(|beacon| beacon.hint)
    }

    /// The number of tracked peers; tests only.
    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.entries.len()
    }

    /// The recorded state of `peer`; tests only.
    #[cfg(test)]
    fn state(&self, peer: &PeerKey) -> Option<PeerState> {
        self.entries.get(peer).map(|entry| entry.state)
    }
}

/// The wait before the next attempt after `failures` failures:
/// `BACKOFF_BASE_MS` doubling per failure up to `BACKOFF_MAX`, plus a
/// jitter of `0..BACKOFF_BASE_MS` derived from the peer key and the
/// attempt count with FNV-1a. The jitter spreads a group of peers that
/// failed together without a source of randomness, which this crate
/// does not carry.
fn backoff(failures: u8, peer: PeerKey) -> Duration {
    let doubling = u32::from(failures.saturating_sub(1)).min(16);
    let delay = Duration::from_millis(BACKOFF_BASE_MS)
        .checked_mul(1u32 << doubling)
        .unwrap_or(BACKOFF_MAX)
        .min(BACKOFF_MAX);
    let jitter = Duration::from_millis(jitter_ms(peer, failures));
    delay
        .checked_add(jitter)
        .unwrap_or(BACKOFF_MAX)
        .min(BACKOFF_MAX)
}

/// The jitter for one peer and attempt count, in milliseconds.
fn jitter_ms(peer: PeerKey, failures: u8) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in peer.as_bytes().iter().chain(std::iter::once(&failures)) {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash % BACKOFF_BASE_MS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BeaconFlags;

    const PEER: PeerKey = PeerKey::new([1; 16]);
    const OTHER: PeerKey = PeerKey::new([2; 16]);

    fn raw(accepting: bool) -> Vec<u8> {
        Beacon {
            flags: BeaconFlags {
                accepting,
                ..BeaconFlags::default()
            },
            degree: 0,
            max_links: 5,
            hint: [7; 8],
            digest: [0; 4],
        }
        .encode()
        .to_vec()
    }

    fn table() -> PeerTable {
        PeerTable::new(&NetworkConfig::default())
    }

    fn no_links() -> BTreeMap<PeerKey, LinkId> {
        BTreeMap::new()
    }

    /// A fresh, strong, accepting sighting is a candidate immediately.
    #[test]
    fn a_fresh_sighting_is_a_candidate() {
        let mut table = table();
        assert!(table.observe(Duration::ZERO, PEER, -60, &raw(true)));
        assert_eq!(table.entries[&PEER].first_seen, Duration::ZERO);
        assert_eq!(table.candidates(Duration::ZERO, &no_links()), [PEER]);
    }

    /// Done when: no connect attempt before `next_attempt_at`.
    #[test]
    fn no_attempt_before_the_backoff_passes() {
        let mut table = table();
        table.observe(Duration::ZERO, PEER, -60, &raw(true));
        table.mark_connecting(&PEER);
        table.mark_down(Duration::ZERO, &PEER);
        let failures = table.entries[&PEER].failures;
        assert_eq!(failures, 1);

        assert!(
            table
                .candidates(Duration::from_millis(999), &no_links())
                .is_empty()
        );
        let due = table.entries[&PEER].next_attempt_at;
        // One failure: base (1 s) plus jitter (0..1 s), so in [1 s, 2 s).
        assert!(due >= Duration::from_secs(1), "{due:?}");
        assert!(due < Duration::from_secs(2), "{due:?}");
        assert_eq!(table.candidates(due, &no_links()), [PEER]);
    }

    /// The delay grows with each failure and never leaves the cap.
    #[test]
    fn backoff_doubles_and_caps() {
        let mut table = table();
        table.observe(Duration::ZERO, PEER, -60, &raw(true));
        let mut previous = Duration::ZERO;
        for _ in 0..300 {
            table.mark_connecting(&PEER);
            table.mark_down(Duration::ZERO, &PEER);
            let due = table.entries[&PEER].next_attempt_at;
            assert!(due >= Duration::from_secs(1), "{due:?}");
            assert!(due <= BACKOFF_MAX, "{due:?}");
            assert!(
                due >= previous,
                "backoff must not shrink: {due:?} {previous:?}"
            );
            previous = due;
        }
        // Saturating failures must not panic or escape the cap.
        assert_eq!(table.entries[&PEER].failures, u8::MAX);
    }

    /// The same peer and count always wait the same time: jitter is
    /// deterministic, not random.
    #[test]
    fn jitter_is_deterministic() {
        assert_eq!(backoff(3, PEER), backoff(3, PEER));
        assert_eq!(jitter_ms(PEER, 3), jitter_ms(PEER, 3));
        assert!(jitter_ms(PEER, 3) < BACKOFF_BASE_MS);
    }

    /// Done when: linked hints never appear as candidates.
    #[test]
    fn linked_peers_are_never_candidates() {
        let mut table = table();
        table.observe(Duration::ZERO, PEER, -60, &raw(true));
        table.mark_connecting(&PEER);
        table.mark_linked(&PEER);
        assert_eq!(table.state(&PEER), Some(PeerState::Linked));
        assert!(table.candidates(Duration::ZERO, &no_links()).is_empty());

        // The table's own state is not the only gate: a peer whose
        // entry says Idle but that the caller already has a link to
        // (evicted and seen again, say) is still excluded.
        let mut table = PeerTable::new(&NetworkConfig::default());
        table.observe(Duration::ZERO, PEER, -60, &raw(true));
        let mut linked = no_links();
        linked.insert(PEER, LinkId::new(1));
        assert!(table.candidates(Duration::ZERO, &linked).is_empty());
    }

    /// "Full" nodes, nodes without a decodable beacon, and weak signals
    /// are filtered out; the rest still come through.
    #[test]
    fn full_and_weak_peers_are_not_candidates() {
        let mut table = table();
        table.observe(Duration::ZERO, PEER, -60, &raw(false));
        table.observe(Duration::ZERO, OTHER, -95, &raw(true));
        let undecodable = PeerKey::new([3; 16]);
        table.observe(Duration::ZERO, undecodable, -60, &[0xFF; 4]);
        assert!(table.candidates(Duration::ZERO, &no_links()).is_empty());

        let strong = PeerKey::new([4; 16]);
        table.observe(Duration::ZERO, strong, -89, &raw(true));
        assert_eq!(table.candidates(Duration::ZERO, &no_links()), [strong]);
    }

    /// An RSSI average moves one eighth per sample, as on a link, and a
    /// weak streak drops a peer below the threshold.
    #[test]
    fn rssi_is_averaged_over_sightings() {
        let mut table = table();
        table.observe(Duration::ZERO, PEER, -95, &raw(true));
        assert!(table.candidates(Duration::ZERO, &no_links()).is_empty());
        for _ in 0..30 {
            table.observe(Duration::from_secs(1), PEER, -60, &raw(true));
        }
        let entry = &table.entries[&PEER];
        // Converges to within one 1/8 step of the sample and then
        // stalls on integer truncation: -67 dBm here.
        assert!(entry.rssi >= -70, "{}", entry.rssi);
        assert_eq!(
            table.candidates(Duration::from_secs(1), &no_links()),
            [PEER]
        );
    }

    /// An entry unseen past the idle timeout is neither a candidate nor
    /// held in memory once anything else is observed.
    #[test]
    fn unseen_peers_are_evicted_after_the_idle_timeout() {
        let mut table = table();
        table.observe(Duration::ZERO, PEER, -60, &raw(true));
        let just_stale = IDLE_TIMEOUT + Duration::from_millis(1);
        assert!(table.candidates(just_stale, &no_links()).is_empty());
        table.observe(just_stale, OTHER, -60, &raw(true));
        assert_eq!(table.tracked(), 1);
        assert!(!table.entries.contains_key(&PEER));
    }

    /// The table never grows past `max_peers`: a full table evicts its
    /// stalest connectable entry for a new peer, and refuses to grow
    /// further when every entry is mid-attempt.
    #[test]
    fn the_table_stays_bounded_under_a_flood() {
        let mut table = PeerTable::new(&NetworkConfig::default().with_max_peers(4).unwrap());
        for byte in 0..8u8 {
            table.observe(Duration::ZERO, PeerKey::new([byte; 16]), -60, &raw(true));
            assert!(table.tracked() <= 4);
        }
        // Four fresh entries now: a fifth cannot displace an attempt
        // that is still owed its result.
        let mid_attempt = table.entries.keys().next().copied().unwrap();
        table.mark_connecting(&mid_attempt);
        for byte in 8..16u8 {
            table.observe(
                Duration::from_secs(1),
                PeerKey::new([byte; 16]),
                -60,
                &raw(true),
            );
            assert!(table.tracked() <= 4);
        }
    }

    /// A failure cooldown does not stack: a second report of the same
    /// down link leaves the first backoff in place.
    #[test]
    fn repeated_down_reports_do_not_stack_delays() {
        let mut table = table();
        table.observe(Duration::ZERO, PEER, -60, &raw(true));
        table.mark_connecting(&PEER);
        table.mark_down(Duration::ZERO, &PEER);
        let due = table.entries[&PEER].next_attempt_at;
        table.mark_down(Duration::ZERO, &PEER);
        assert_eq!(table.entries[&PEER].next_attempt_at, due);
        assert_eq!(table.entries[&PEER].failures, 1);
    }
}
