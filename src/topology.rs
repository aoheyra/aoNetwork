//! The peer table and the topology policy (NET-15, NET-16).
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
//!
//! [`TopologyPolicy`] turns the configured degree cap into the slots a
//! node admits links into (stable, with one explore slot reserved
//! above the stable target), scores candidates so the best neighbour
//! is dialed first, decides when a held link has degraded far enough
//! to evict, and lowers the session's effective cap after repeated
//! rejections.
//!
//! Duplicate links (NET-17) are kept rare and then settled. The side
//! whose beacon hint sorts lower dials a new neighbour at once; the
//! other side waits [`INITIATION_DELAY`] first, so its own connection
//! is normally made redundant by the inbound one. When both connect
//! anyway, [`duplicate_loser`] picks the one link both ends close.
//!
//! The opportunistic slot (NET-22) holds a short link to a peer whose
//! beacon digest changed, once the node has reached its stable target,
//! so the layers above can sync with a passing node. It ends on release
//! or after [`DWELL`], and [`Cooldowns`] then bars the peer's hint for
//! [`COOLDOWN`] unless its digest changes again.
//!
//! [`LinkHintPreference`] values from the layer above only advise which peer
//! to dial first. [`HintTable`] holds at most `max_peers` of them; the degree
//! cap, admission floor and slots stay entirely in [`TopologyPolicy`].

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use crate::metrics::rssi_ewma;
use crate::{Beacon, LinkHintPreference, LinkId, LinkRole, LocalTag, NetworkConfig, PeerKey};

/// Entries unseen for longer than this are evicted, so a peer that stops
/// advertising does not hold memory or a candidate slot forever.
pub(crate) const IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// The first retry delay before jitter: it doubles with each failure.
const BACKOFF_BASE_MS: u64 = 1_000;
/// Retries never wait longer than this, jitter included.
const BACKOFF_MAX: Duration = Duration::from_secs(60);
/// A link younger than this is never evicted, so an established link
/// survives the churn that created it.
pub(crate) const LINK_MIN_AGE: Duration = Duration::from_secs(10);
/// How long an opportunistic link may hold its slot (NET-22).
pub(crate) const DWELL: Duration = Duration::from_secs(60);
/// How long a peer that lost a slot stays barred from getting it back
/// unless its beacon digest changes (NET-22).
pub(crate) const COOLDOWN: Duration = Duration::from_secs(60);
/// How long the side whose hint does not sort lower waits before dialing
/// a newly connectable peer itself. It covers the other side's scan lag
/// plus its connect time, so simultaneous connects stay rare.
pub(crate) const INITIATION_DELAY: Duration = Duration::from_secs(4);
/// Consecutive outbound rejections that lower the effective cap.
const REJECTION_STRIKES: u8 = 3;
/// The effective cap never drops below this.
const MIN_EFFECTIVE_CAP: usize = 1;

/// The slot a link holds under the topology policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
#[non_exhaustive]
pub enum LinkClass {
    /// A link inside the stable target: kept while it is healthy.
    Stable,
    /// The single slot above the stable target that keeps a node
    /// looking past its immediate neighbourhood.
    Explore,
    /// A temporary slot for a peer whose beacon digest changed, held
    /// until released or for at most the dwell time.
    Opportunistic,
}

/// A snapshot of the link inventory admission and eviction ask about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ClassCounts {
    /// Every entry, connecting or up, in any class.
    pub(crate) total: usize,
    /// Entries admitted as stable.
    pub(crate) stable: usize,
    /// Entries admitted as explore.
    pub(crate) explore: usize,
    /// Entries admitted as opportunistic.
    pub(crate) opportunistic: usize,
}

/// The candidate score: strong signal first, lightly loaded peers
/// first. Shared by candidate ordering and the eviction pick, so the
/// link a node dials and the link it sheds are judged the same way.
pub(crate) fn score(rssi: i16, degree: u8) -> i32 {
    i32::from(rssi) * 4 - i32::from(degree)
}

/// How many links a node holds, in which classes, and when to evict.
///
/// The knobs are derived from [`NetworkConfig`]: `max_links` becomes
/// the total cap and, when it is at least 3, one explore and one
/// opportunistic slot above the stable target; the admission floor is
/// `min_rssi`. Every formula is deterministic, so the same
/// configuration always makes the same decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TopologyPolicy {
    /// Links held as stable before the explore slot is the only room
    /// left: `max_total - explore_slots - opportunistic_slots`.
    pub(crate) target_stable: usize,
    /// Stable links that may be held at once: one above the target,
    /// so a healthy link never fights for its place at the target
    /// itself.
    pub(crate) max_stable: usize,
    /// Explore links that may be held at once.
    pub(crate) explore_slots: usize,
    /// Opportunistic links that may be held at once.
    pub(crate) opportunistic_slots: usize,
    /// The configured cap on total links.
    pub(crate) max_total: usize,
    /// A link whose peer average falls below this is degraded: it is
    /// never dialed and is evicted first when over the target.
    pub(crate) admit_floor: i16,
    /// A link younger than this is never evicted.
    pub(crate) link_min_age: Duration,
    /// How long the higher-hint side of a pair holds back its dial.
    pub(crate) initiation_delay: Duration,
    /// How long an opportunistic link may hold its slot.
    pub(crate) dwell: Duration,
    /// How long a hint stays barred after its opportunistic link ends,
    /// unless its digest changes.
    pub(crate) cooldown: Duration,
    /// The cap admission uses now: `max_total`, lowered for the
    /// session after repeated rejections.
    pub(crate) effective_cap: usize,
    /// Consecutive outbound rejections since the last success.
    rejections: u8,
}

impl TopologyPolicy {
    /// The policy for `config`.
    pub(crate) fn new(config: &NetworkConfig) -> Self {
        let max_total = usize::try_from(config.max_links()).unwrap_or(usize::MAX);
        // The explore and opportunistic slots exist only once there is
        // room for a stable link beside them.
        let slots = usize::from(max_total >= 3);
        let explore_slots = slots;
        let opportunistic_slots = slots;
        let target_stable = max_total - explore_slots - opportunistic_slots;
        Self {
            target_stable,
            max_stable: (target_stable + 1).min(max_total),
            explore_slots,
            opportunistic_slots,
            max_total,
            admit_floor: config.min_rssi(),
            link_min_age: LINK_MIN_AGE,
            initiation_delay: INITIATION_DELAY,
            dwell: DWELL,
            cooldown: COOLDOWN,
            effective_cap: max_total,
            rejections: 0,
        }
    }

    /// The class the next inbound or outbound link is admitted as, or
    /// `None` when no slot is free: stable first, then the explore
    /// slot, and never the opportunistic slot, which
    /// [`admit_opportunistic`](Self::admit_opportunistic) decides.
    pub(crate) fn admit(&self, counts: &ClassCounts) -> Option<LinkClass> {
        if counts.total >= self.effective_cap {
            return None;
        }
        if counts.stable < self.max_stable {
            return Some(LinkClass::Stable);
        }
        if counts.explore < self.explore_slots {
            return Some(LinkClass::Explore);
        }
        None
    }

    /// Whether an opportunistic link may be admitted now: the node has
    /// reached its stable target, the opportunistic slot is free, and
    /// the total stays within the effective cap.
    pub(crate) fn admit_opportunistic(&self, counts: &ClassCounts) -> bool {
        counts.stable >= self.target_stable
            && counts.opportunistic < self.opportunistic_slots
            && counts.total < self.effective_cap
    }

    /// Records that an outbound attempt came back
    /// [`ConnectError::Rejected`](crate::ConnectError::Rejected): three
    /// in a row lower the session's effective cap by one, so a node
    /// whose radio or neighbour is refusing it stops claiming as many
    /// slots. A success clears the streak; the cap itself never
    /// recovers within the session.
    pub(crate) fn note_rejection(&mut self) {
        self.rejections = self.rejections.saturating_add(1);
        if self.rejections >= REJECTION_STRIKES {
            self.rejections = 0;
            self.effective_cap = self.effective_cap.saturating_sub(1).max(MIN_EFFECTIVE_CAP);
        }
    }

    /// Records a completed handshake: the rejection streak is over.
    pub(crate) fn note_success(&mut self) {
        self.rejections = 0;
    }
}

/// Bounded advisory preferences, one per radio peer (NET-16).
///
/// Advice outlives sightings: a contact that goes away keeps its preference
/// until it is cleared or displaced. There is no clock here to expire it.
/// A full table drops the lowest preference first, then the greatest
/// [`PeerKey`], so advice that arrives in a different order still converges
/// to the same table.
#[derive(Debug)]
pub(crate) struct HintTable {
    entries: BTreeMap<PeerKey, LinkHintPreference>,
    max: usize,
}

impl HintTable {
    pub(crate) fn new(config: &NetworkConfig) -> Self {
        Self {
            entries: BTreeMap::new(),
            max: usize::try_from(config.max_peers()).unwrap_or(usize::MAX),
        }
    }

    /// Sets `peer`'s preference. `Neutral` removes any advice, restoring
    /// the ordinary rules. A new entry in a full table displaces the
    /// lowest preference, then the greatest key on a tie.
    pub(crate) fn set(&mut self, peer: PeerKey, preference: LinkHintPreference) {
        if preference == LinkHintPreference::Neutral {
            self.entries.remove(&peer);
            return;
        }
        if !self.entries.contains_key(&peer) && self.entries.len() >= self.max {
            let victim = self
                .entries
                .iter()
                .min_by(|(a_key, a), (b_key, b)| a.cmp(b).then_with(|| b_key.cmp(a_key)))
                .map(|(&key, _)| key);
            if let Some(key) = victim {
                self.entries.remove(&key);
            }
        }
        self.entries.insert(peer, preference);
    }

    /// Removes `peer`'s advice, returning whether there was any.
    pub(crate) fn clear(&mut self, peer: PeerKey) -> bool {
        self.entries.remove(&peer).is_some()
    }

    /// The stored preference, or `Neutral` for an unhinted peer.
    pub(crate) fn get(&self, peer: &PeerKey) -> LinkHintPreference {
        self.entries.get(peer).copied().unwrap_or_default()
    }

    /// Whether this node wants to dial `peer` at once, ahead of the
    /// beacon-hint tie-break delay.
    pub(crate) fn preferred(&self, peer: &PeerKey) -> bool {
        self.get(peer) == LinkHintPreference::Prefer
    }

    /// The active advice, in `PeerKey` order.
    pub(crate) fn entries(&self) -> impl Iterator<Item = (PeerKey, LinkHintPreference)> + '_ {
        self.entries
            .iter()
            .map(|(&peer, &preference)| (peer, preference))
    }

    /// The number of active hints.
    pub(crate) fn tracked(&self) -> usize {
        self.entries.len()
    }
}

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
    /// Its digest is new to this node: it changed, or first decoded,
    /// since a link to it last came up.
    pub(crate) unsynced: bool,
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
                let old = entry.beacon.map(|old| old.digest);
                entry.unsynced |= old != Some(decoded.digest);
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
                unsynced: beacon.is_some(),
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

    /// Records a successful handshake: linked, the failure streak is
    /// over, so a later drop backs off from a clean slate, and the peer's
    /// digest is no longer new.
    pub(crate) fn mark_linked(&mut self, peer: &PeerKey) {
        if let Some(entry) = self.entries.get_mut(peer) {
            entry.state = PeerState::Linked;
            entry.failures = 0;
            entry.unsynced = false;
        }
    }

    /// Whether `peer`'s digest is new to this node since a link to it
    /// last came up: the trigger for an opportunistic link.
    pub(crate) fn unsynced(&self, peer: &PeerKey) -> bool {
        self.entries.get(peer).is_some_and(|entry| entry.unsynced)
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

    /// Every peer connectable at `now`: advisory hints first, then signal
    /// score (strong, lightly loaded), then `PeerKey`, so a run makes the
    /// same choices every time. Advice changes only the ordering:
    /// eligibility, floors, backoffs and the degree cap are the same, so a
    /// hint can never add a candidate or a slot. `linked` is the live set of
    /// links and in-flight attempts, which the table cannot know for a peer
    /// it has evicted and seen again.
    pub(crate) fn candidates_with_hints(
        &self,
        now: Duration,
        linked: &BTreeMap<PeerKey, LinkId>,
        hints: &HintTable,
    ) -> Vec<PeerKey> {
        let mut out: Vec<(i8, i32, PeerKey)> = self
            .entries
            .keys()
            .filter(|peer| self.eligible(peer, now, linked))
            .filter_map(|&peer| {
                self.score_of(&peer)
                    .map(|score| (hints.get(&peer).rank(), score, peer))
            })
            .collect();
        out.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then_with(|| b.1.cmp(&a.1))
                .then_with(|| a.2.cmp(&b.2))
        });
        out.into_iter().map(|(_, _, peer)| peer).collect()
    }

    /// The candidates with no advice; tests only.
    #[cfg(test)]
    pub(crate) fn candidates(
        &self,
        now: Duration,
        linked: &BTreeMap<PeerKey, LinkId>,
    ) -> Vec<PeerKey> {
        self.candidates_with_hints(now, linked, &HintTable::new(&NetworkConfig::default()))
    }

    /// The peer's moving-average RSSI, if it has ever been seen.
    pub(crate) fn rssi_of(&self, peer: &PeerKey) -> Option<i16> {
        Some(self.entries.get(peer)?.rssi)
    }

    /// The peer's [`score`] from its latest sighting, if it has
    /// advertised a beacon to score against.
    pub(crate) fn score_of(&self, peer: &PeerKey) -> Option<i32> {
        let entry = self.entries.get(peer)?;
        let beacon = entry.beacon.as_ref()?;
        Some(score(entry.rssi, beacon.degree))
    }

    /// The peer's latest advertised hint, for the connect tie-break.
    pub(crate) fn hint_of(&self, peer: &PeerKey) -> Option<[u8; 8]> {
        self.beacon_of(peer).map(|beacon| beacon.hint)
    }

    /// The peer's latest decoded beacon.
    pub(crate) fn beacon_of(&self, peer: &PeerKey) -> Option<Beacon> {
        self.entries.get(peer)?.beacon
    }

    /// Whether `delay` has passed at `now` since `peer` last became
    /// connectable: since it was first seen or since its backoff ended,
    /// whichever is later.
    pub(crate) fn waited(&self, peer: &PeerKey, now: Duration, delay: Duration) -> bool {
        self.entries.get(peer).is_some_and(|entry| {
            entry
                .first_seen
                .max(entry.next_attempt_at)
                .checked_add(delay)
                .is_some_and(|due| now >= due)
        })
    }

    /// The number of tracked peers.
    pub(crate) fn tracked(&self) -> usize {
        self.entries.len()
    }

    /// The recorded state of `peer`; tests only.
    #[cfg(test)]
    pub(crate) fn state(&self, peer: &PeerKey) -> Option<PeerState> {
        self.entries.get(peer).map(|entry| entry.state)
    }
}

/// A ready link as the duplicate-link tie-break sees it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ReadyLink {
    pub(crate) link: LinkId,
    pub(crate) role: LinkRole,
    /// The tag this node sent on it.
    pub(crate) local_tag: LocalTag,
    /// The tag the peer sent on it.
    pub(crate) remote_tag: LocalTag,
}

impl ReadyLink {
    /// The tag of the side that opened the link.
    fn initiator(&self) -> LocalTag {
        match self.role {
            LinkRole::Initiator => self.local_tag,
            LinkRole::Responder => self.remote_tag,
        }
    }
}

/// Which of two ready links with the same remote tag this node closes,
/// `held` being up already and `new` having just finished its handshake.
///
/// The link opened by the lower tag stays. Both ends compute the same
/// answer whatever order their handshakes finish in, so they never close
/// both links. Two links opened by the same side are settled by that
/// side alone, which closes the newer; the other end closes neither.
pub(crate) fn duplicate_loser(held: &ReadyLink, new: &ReadyLink) -> Option<LinkId> {
    match held.initiator().cmp(&new.initiator()) {
        Ordering::Less => Some(new.link),
        Ordering::Greater => Some(held.link),
        Ordering::Equal => (new.role == LinkRole::Initiator).then_some(new.link),
    }
}

/// Hints barred from new links after their opportunistic link ended.
///
/// Each entry remembers the digest the peer advertised when the link
/// ended; a different digest lifts the bar at once. Bounded by
/// `max_peers`: when full, the entry that ends soonest makes room.
#[derive(Debug)]
pub(crate) struct Cooldowns {
    entries: HashMap<[u8; 8], (Duration, [u8; 4])>,
    max: usize,
    duration: Duration,
}

impl Cooldowns {
    pub(crate) fn new(config: &NetworkConfig) -> Self {
        Self {
            entries: HashMap::new(),
            max: usize::try_from(config.max_peers()).unwrap_or(usize::MAX),
            duration: COOLDOWN,
        }
    }

    /// Bars `hint` from `now` while it keeps advertising `digest`.
    pub(crate) fn start(&mut self, now: Duration, hint: [u8; 8], digest: [u8; 4]) {
        self.entries.retain(|_, (until, _)| now < *until);
        if !self.entries.contains_key(&hint) && self.entries.len() >= self.max {
            let soonest = self
                .entries
                .iter()
                .min_by_key(|(key, (until, _))| (*until, **key))
                .map(|(&key, _)| key);
            if let Some(key) = soonest {
                self.entries.remove(&key);
            }
        }
        let until = now.checked_add(self.duration).unwrap_or(Duration::MAX);
        self.entries.insert(hint, (until, digest));
    }

    /// Whether a peer advertising `beacon` is barred at `now`.
    pub(crate) fn bars(&self, now: Duration, beacon: &Beacon) -> bool {
        self.entries
            .get(&beacon.hint)
            .is_some_and(|(until, digest)| now < *until && *digest == beacon.digest)
    }

    /// The number of barred hints; tests only.
    #[cfg(test)]
    pub(crate) fn tracked(&self) -> usize {
        self.entries.len()
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

    fn raw_with_degree(degree: u8) -> Vec<u8> {
        Beacon {
            flags: BeaconFlags {
                accepting: true,
                ..BeaconFlags::default()
            },
            degree,
            max_links: 5,
            hint: [7; 8],
            digest: [0; 4],
        }
        .encode()
        .to_vec()
    }

    fn counts(total: usize, stable: usize, explore: usize) -> ClassCounts {
        ClassCounts {
            total,
            stable,
            explore,
            opportunistic: 0,
        }
    }

    /// The opportunistic slot opens once the stable target is reached,
    /// holds one link, and never takes the total past the cap.
    #[test]
    fn the_opportunistic_slot_opens_at_the_stable_target() {
        let policy = TopologyPolicy::new(&NetworkConfig::default());
        assert!(
            !policy.admit_opportunistic(&counts(2, 2, 0)),
            "below target"
        );
        assert!(policy.admit_opportunistic(&counts(3, 3, 0)));
        assert!(policy.admit_opportunistic(&counts(4, 3, 1)));
        let mut held = counts(4, 3, 0);
        held.opportunistic = 1;
        assert!(!policy.admit_opportunistic(&held), "the slot is taken");
        assert!(!policy.admit_opportunistic(&counts(5, 4, 1)), "at the cap");
        // Too small a cap has no opportunistic slot at all.
        let two = TopologyPolicy::new(&NetworkConfig::default().with_max_links(2).unwrap());
        assert!(!two.admit_opportunistic(&counts(1, 2, 0)));
    }

    fn beacon(hint: u8, digest: u8) -> Beacon {
        Beacon {
            flags: BeaconFlags::default(),
            degree: 0,
            max_links: 5,
            hint: [hint; 8],
            digest: [digest; 4],
        }
    }

    /// A digest is new on the first decoded sighting and on every change,
    /// stays new across sightings until a link comes up, and an unchanged
    /// digest after that is not new.
    #[test]
    fn a_digest_stays_new_until_a_link_comes_up() {
        let mut table = table();
        let with_digest = |digest: u8| {
            let mut b = beacon(7, digest);
            b.flags.accepting = true;
            b.encode().to_vec()
        };
        table.observe(Duration::ZERO, PEER, -60, &[0xFF; 4]);
        assert!(!table.unsynced(&PEER), "nothing decoded yet");
        table.observe(Duration::ZERO, PEER, -60, &with_digest(1));
        assert!(table.unsynced(&PEER), "first decoded sighting");
        table.observe(Duration::ZERO, PEER, -60, &with_digest(1));
        assert!(table.unsynced(&PEER), "still new until linked");
        table.mark_connecting(&PEER);
        table.mark_linked(&PEER);
        assert!(!table.unsynced(&PEER));
        table.observe(Duration::ZERO, PEER, -60, &with_digest(1));
        assert!(!table.unsynced(&PEER), "unchanged");
        table.observe(Duration::ZERO, PEER, -60, &with_digest(2));
        assert!(table.unsynced(&PEER), "changed");
        assert!(!table.unsynced(&OTHER), "unknown peer");
    }

    /// A cooldown bars its hint for its duration, a new digest lifts it,
    /// and the table stays bounded.
    #[test]
    fn a_cooldown_bars_a_hint_until_it_lapses_or_the_digest_changes() {
        let mut cooldowns = Cooldowns::new(&NetworkConfig::default());
        cooldowns.start(Duration::ZERO, [1; 8], [9; 4]);
        assert!(cooldowns.bars(Duration::from_secs(59), &beacon(1, 9)));
        assert!(
            !cooldowns.bars(Duration::from_secs(59), &beacon(1, 8)),
            "new digest"
        );
        assert!(
            !cooldowns.bars(Duration::from_secs(59), &beacon(2, 9)),
            "other hint"
        );
        assert!(!cooldowns.bars(COOLDOWN, &beacon(1, 9)), "lapsed");

        let config = NetworkConfig::default().with_max_peers(3).unwrap();
        let mut cooldowns = Cooldowns::new(&config);
        for hint in 0..10u8 {
            cooldowns.start(Duration::from_secs(u64::from(hint)), [hint; 8], [0; 4]);
            assert!(cooldowns.tracked() <= 3);
        }
        // The newest survive; the oldest made room.
        assert!(cooldowns.bars(Duration::from_secs(9), &beacon(9, 0)));
        assert!(!cooldowns.bars(Duration::from_secs(9), &beacon(0, 0)));
    }

    /// Done when: the default configuration produces the slots the
    /// README documents: target stable 3, max stable 4, 1 explore,
    /// 1 opportunistic, max total 5.
    #[test]
    fn policy_defaults_match_the_readme() {
        let policy = TopologyPolicy::new(&NetworkConfig::default());
        assert_eq!(policy.target_stable, 3);
        assert_eq!(policy.max_stable, 4);
        assert_eq!(policy.explore_slots, 1);
        assert_eq!(policy.opportunistic_slots, 1);
        assert_eq!(policy.max_total, 5);
        assert_eq!(policy.effective_cap, 5);
        assert_eq!(policy.admit_floor, -90);
        assert_eq!(policy.link_min_age, Duration::from_secs(10));
        assert_eq!(policy.initiation_delay, Duration::from_secs(4));
        assert_eq!(policy.dwell, Duration::from_secs(60));
        assert_eq!(policy.cooldown, Duration::from_secs(60));

        // Too small for anything but stable links.
        let one = TopologyPolicy::new(&NetworkConfig::default().with_max_links(1).unwrap());
        assert_eq!(one.target_stable, 1);
        assert_eq!(one.max_stable, 1);
        assert_eq!(one.explore_slots, 0);
        assert_eq!(one.opportunistic_slots, 0);
        let two = TopologyPolicy::new(&NetworkConfig::default().with_max_links(2).unwrap());
        assert_eq!(two.target_stable, 2);
        assert_eq!(two.max_stable, 2);
        assert_eq!(two.explore_slots, 0);

        // The explore slot appears with room for stable plus extras.
        let three = TopologyPolicy::new(&NetworkConfig::default().with_max_links(3).unwrap());
        assert_eq!(three.target_stable, 1);
        assert_eq!(three.max_stable, 2);
        assert_eq!(three.explore_slots, 1);
        assert_eq!(three.opportunistic_slots, 1);

        let max = TopologyPolicy::new(&NetworkConfig::default().with_max_links(32).unwrap());
        assert_eq!(max.target_stable, 30);
        assert_eq!(max.max_stable, 31);
        assert_eq!(max.max_total, 32);
    }

    /// Admission walks stable, then explore, then refuses; the
    /// effective cap gates the total before any class is considered.
    #[test]
    fn admission_walks_the_classes_then_refuses() {
        let policy = TopologyPolicy::new(&NetworkConfig::default());
        let mut c = counts(0, 0, 0);
        for _ in 0..4 {
            assert_eq!(policy.admit(&c), Some(LinkClass::Stable));
            c.total += 1;
            c.stable += 1;
        }
        // Stable is full but the explore slot still has room.
        assert_eq!(policy.admit(&c), Some(LinkClass::Explore));
        c.total += 1;
        c.explore += 1;
        // Five links: no class has room, even with stable slots free
        // in a stale count, because the total is at the cap.
        assert_eq!(policy.admit(&c), None);
        let over = counts(5, 0, 0);
        assert_eq!(policy.admit(&over), None);
    }

    /// Three rejections in a row lower the effective cap by one, a
    /// success clears the streak, and the floor is one link.
    #[test]
    fn rejections_lower_the_effective_cap_and_success_resets() {
        let mut policy = TopologyPolicy::new(&NetworkConfig::default());
        assert_eq!(policy.effective_cap, 5);

        policy.note_rejection();
        policy.note_rejection();
        assert_eq!(policy.effective_cap, 5, "two strikes are not enough");
        policy.note_success();
        policy.note_rejection();
        policy.note_rejection();
        assert_eq!(policy.effective_cap, 5, "a success cleared the streak");
        policy.note_rejection();
        policy.note_rejection();
        policy.note_rejection();
        assert_eq!(policy.effective_cap, 4);

        // The cap never falls below one link, however many rejections.
        for _ in 0..10 {
            policy.note_rejection();
            policy.note_rejection();
            policy.note_rejection();
        }
        assert_eq!(policy.effective_cap, 1);
    }

    /// The score rewards signal and penalises the peer's degree, so a
    /// strong, lightly loaded peer sorts first.
    #[test]
    fn score_weights_signal_and_penalises_degree() {
        assert_eq!(score(-60, 0), -240);
        assert!(score(-50, 3) > score(-60, 0), "stronger wins");
        assert!(score(-60, 0) > score(-60, 4), "lighter wins");
    }

    /// Candidates come back best-first, with the key breaking ties so
    /// the order stays deterministic.
    #[test]
    fn candidates_are_scored_best_first() {
        let mut table = table();
        let weak = PeerKey::new([1; 16]);
        let strong = PeerKey::new([2; 16]);
        let loaded = PeerKey::new([3; 16]);
        table.observe(Duration::ZERO, weak, -80, &raw_with_degree(0));
        table.observe(Duration::ZERO, strong, -50, &raw_with_degree(0));
        table.observe(Duration::ZERO, loaded, -50, &raw_with_degree(6));
        // Strong beats weak; equal signal goes to the lighter peer,
        // and an exact tie would fall back to the key order.
        assert_eq!(
            table.candidates(Duration::ZERO, &no_links()),
            [strong, loaded, weak]
        );
    }

    /// Advice is bounded by `max_peers`: a full table drops the lowest
    /// preference first and the greatest key on a tie. Neutral removes
    /// advice, and clearing restores the ordinary rules.
    #[test]
    fn hints_are_bounded_and_removable() {
        let config = NetworkConfig::default().with_max_peers(2).unwrap();
        let mut hints = HintTable::new(&config);
        let low = PeerKey::new([1; 16]);
        let high = PeerKey::new([2; 16]);
        let newcomer = PeerKey::new([3; 16]);
        hints.set(low, LinkHintPreference::Prefer);
        hints.set(high, LinkHintPreference::Deprioritize);
        assert_eq!(hints.tracked(), 2);
        // The deprioritized peer makes room for a preferred one.
        hints.set(newcomer, LinkHintPreference::Prefer);
        assert_eq!(hints.tracked(), 2);
        assert_eq!(hints.get(&low), LinkHintPreference::Prefer);
        assert_eq!(hints.get(&high), LinkHintPreference::Neutral);
        assert_eq!(hints.get(&newcomer), LinkHintPreference::Prefer);

        // An equal-rank tie evicts the greatest key, whatever the order
        // the advice arrived in.
        let mut hints = HintTable::new(&config);
        hints.set(newcomer, LinkHintPreference::Prefer);
        hints.set(high, LinkHintPreference::Prefer);
        hints.set(low, LinkHintPreference::Prefer);
        assert_eq!(hints.get(&newcomer), LinkHintPreference::Neutral);
        assert_eq!(hints.get(&high), LinkHintPreference::Prefer);
        assert_eq!(hints.get(&low), LinkHintPreference::Prefer);
        assert_eq!(hints.tracked(), 2);

        hints.set(low, LinkHintPreference::Neutral);
        assert_eq!(hints.get(&low), LinkHintPreference::Neutral);
        assert!(hints.clear(high));
        assert!(!hints.clear(high));
        assert_eq!(hints.tracked(), 0);
    }

    /// Advice orders candidates before signal score, with `PeerKey`
    /// breaking ties. It never makes an ineligible peer a candidate.
    #[test]
    fn hints_reorder_candidates_deterministically() {
        let hints = HintTable::new(&NetworkConfig::default());
        let weak = PeerKey::new([1; 16]);
        let strong = PeerKey::new([2; 16]);
        let loaded = PeerKey::new([3; 16]);
        let mut table = table();
        table.observe(Duration::ZERO, weak, -80, &raw_with_degree(0));
        table.observe(Duration::ZERO, strong, -60, &raw_with_degree(0));
        table.observe(Duration::ZERO, loaded, -60, &raw_with_degree(4));
        assert_eq!(
            table.candidates_with_hints(Duration::ZERO, &no_links(), &hints),
            [strong, loaded, weak]
        );

        let mut preferred = hints;
        preferred.set(weak, LinkHintPreference::Prefer);
        assert_eq!(
            table.candidates_with_hints(Duration::ZERO, &no_links(), &preferred),
            [weak, strong, loaded]
        );
        // Equal advice falls back to score, then the key.
        preferred.set(strong, LinkHintPreference::Prefer);
        assert_eq!(
            table.candidates_with_hints(Duration::ZERO, &no_links(), &preferred),
            [strong, weak, loaded]
        );
        preferred.set(loaded, LinkHintPreference::Prefer);
        assert_eq!(
            table.candidates_with_hints(Duration::ZERO, &no_links(), &preferred),
            [strong, loaded, weak]
        );
        // Deprioritizing pushes a peer behind unhinted ones, never out
        // of the candidate list.
        let mut shunned = HintTable::new(&NetworkConfig::default());
        shunned.set(strong, LinkHintPreference::Deprioritize);
        assert_eq!(
            table.candidates_with_hints(Duration::ZERO, &no_links(), &shunned),
            [loaded, weak, strong]
        );
    }

    /// A peer without a beacon cannot be scored, so it is not offered
    /// as a candidate even once it would otherwise be eligible.
    #[test]
    fn an_unscoreable_peer_is_not_a_candidate() {
        let mut table = table();
        table.observe(Duration::ZERO, PEER, -60, &[0xFF; 4]);
        assert_eq!(table.score_of(&PEER), None);
        assert!(table.candidates(Duration::ZERO, &no_links()).is_empty());
        table.observe(Duration::ZERO, PEER, -60, &raw(true));
        assert!(table.score_of(&PEER).is_some());
        assert_eq!(table.candidates(Duration::ZERO, &no_links()), [PEER]);
    }

    /// The higher-hint side may dial once the delay has passed since the
    /// peer was first seen, and after a failure only that long after the
    /// backoff ends.
    #[test]
    fn the_initiation_delay_runs_from_the_latest_connectable_moment() {
        let mut table = table();
        let delay = INITIATION_DELAY;
        assert!(!table.waited(&PEER, delay, delay), "unknown peer");
        table.observe(Duration::from_secs(1), PEER, -60, &raw(true));
        assert!(!table.waited(&PEER, Duration::from_secs(4), delay));
        assert!(table.waited(&PEER, Duration::from_secs(5), delay));

        table.mark_connecting(&PEER);
        table.mark_down(Duration::from_secs(5), &PEER);
        let due = table.entries[&PEER].next_attempt_at;
        assert!(!table.waited(&PEER, due + delay - Duration::from_millis(1), delay));
        assert!(table.waited(&PEER, due + delay, delay));
        // A delay that overflows the clock never elapses.
        assert!(!table.waited(&PEER, Duration::MAX, Duration::MAX));
    }

    fn ready(link: u64, role: LinkRole, local: u8, remote: u8) -> ReadyLink {
        ReadyLink {
            link: LinkId::new(link),
            role,
            local_tag: LocalTag::new([local; 16]),
            remote_tag: LocalTag::new([remote; 16]),
        }
    }

    /// Simultaneous connects: each end holds one link it opened and one
    /// it accepted. Both ends close the link opened by the higher tag,
    /// whichever handshake finished first.
    #[test]
    fn the_link_opened_by_the_lower_tag_stays() {
        use LinkRole::{Initiator, Responder};
        // Node 1 (tag 1) opened the connection seen as 10 at its end and
        // 20 at node 2's; node 2 (tag 2) opened 21, seen as 11 at node 1.
        let one_out = ready(10, Initiator, 1, 2);
        let one_in = ready(11, Responder, 1, 2);
        let two_in = ready(20, Responder, 2, 1);
        let two_out = ready(21, Initiator, 2, 1);
        // Node 1 drops 11 and node 2 drops 21: the same connection.
        assert_eq!(duplicate_loser(&one_out, &one_in), Some(one_in.link));
        assert_eq!(duplicate_loser(&one_in, &one_out), Some(one_in.link));
        assert_eq!(duplicate_loser(&two_in, &two_out), Some(two_out.link));
        assert_eq!(duplicate_loser(&two_out, &two_in), Some(two_out.link));
    }

    /// Two links opened by the same side: only that side closes one,
    /// the newer, so the ends never close both.
    #[test]
    fn a_double_dial_is_settled_by_the_dialer() {
        use LinkRole::{Initiator, Responder};
        let (old, new) = (ready(1, Initiator, 1, 2), ready(2, Initiator, 1, 2));
        assert_eq!(duplicate_loser(&old, &new), Some(new.link));
        let (old, new) = (ready(1, Responder, 2, 1), ready(2, Responder, 2, 1));
        assert_eq!(duplicate_loser(&old, &new), None);
    }
}
