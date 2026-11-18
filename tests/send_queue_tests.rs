//! Send queue scheduling, caps and refusal.

use std::collections::VecDeque;
use std::time::Duration;

use ao_network::{Channel, NetworkConfig, SendError, SendOpts, SendQueue};

/// Encoded `Data` frame size of a payload: header, channel byte, the
/// varint length and the payload itself, exactly as `codec` lays it out.
fn cost(len: usize) -> usize {
    let bits = (usize::BITS - (len as u64).leading_zeros())
        .div_ceil(7)
        .max(1);
    3 + bits as usize + len
}

#[test]
fn control_goes_ahead_of_queued_traffic() {
    let mut queue = SendQueue::new(&NetworkConfig::default());
    for byte in 0..10 {
        queue.push(Channel::Bulk, vec![byte; 100]).unwrap();
        queue.push(Channel::Message, vec![byte; 100]).unwrap();
    }
    // One frame is in flight when Control arrives; Control is next.
    assert!(queue.pop(Duration::ZERO).is_some());
    queue.push(Channel::Control, vec![0xC0; 4]).unwrap();
    assert_eq!(
        queue.pop(Duration::ZERO),
        Some((Channel::Control, vec![0xC0; 4]))
    );
}

#[test]
fn queue_full_returns_the_payload_unchanged() {
    let config = NetworkConfig::default()
        .with_channel_cap(Channel::Message, 1000)
        .unwrap();
    let mut queue = SendQueue::new(&config);
    queue.push(Channel::Message, vec![1; 900]).unwrap();
    let payload: Vec<u8> = (0..=255).collect();
    assert_eq!(
        queue.push(Channel::Message, payload.clone()),
        Err(SendError::QueueFull(payload))
    );
    // The refusal changed nothing, and other channels are unaffected.
    assert_eq!(queue.queued_bytes(Channel::Message), 900 + 5);
    queue.push(Channel::Bulk, vec![2; 900]).unwrap();
}

#[test]
fn empty_payloads_still_use_up_the_cap() {
    let config = NetworkConfig::default()
        .with_channel_cap(Channel::Telemetry, 64)
        .unwrap()
        .with_channel_cap(Channel::Message, 64)
        .unwrap();
    let mut queue = SendQueue::new(&config);
    for _ in 0..64 / cost(0) {
        queue.push(Channel::Telemetry, Vec::new()).unwrap();
        queue.push(Channel::Message, Vec::new()).unwrap();
    }
    assert_eq!(queue.queued_bytes(Channel::Telemetry), 64);
    assert_eq!(queue.queued_bytes(Channel::Message), 64);
    // Telemetry is latest-wins (NET-21): the next push evicts the oldest
    // frame instead of refusing, and the cap still holds exactly.
    queue.push(Channel::Telemetry, Vec::new()).unwrap();
    assert_eq!(queue.queued_bytes(Channel::Telemetry), 64);
    // Message keeps the refusal contract: the payload comes back unchanged.
    assert_eq!(
        queue.push(Channel::Message, Vec::new()),
        Err(SendError::QueueFull(Vec::new()))
    );
}

#[test]
fn busy_channels_share_bytes_by_weight() {
    let mut queue = SendQueue::new(&NetworkConfig::default());
    for channel in [Channel::Message, Channel::Telemetry, Channel::Bulk] {
        // Fill to the cap exactly: Telemetry would otherwise evict instead
        // of stopping (NET-21), so count bytes rather than push to failure.
        let cap = NetworkConfig::default().channel_cap(channel);
        while queue.queued_bytes(channel) + cost(96) <= cap {
            queue.push(channel, vec![0; 96]).unwrap();
        }
    }
    // Seven rounds of 7 KiB; every channel still has frames queued after.
    let mut sent = [0usize; 3];
    let mut total = 0;
    while total < 7 * 7 * 1024 {
        let (channel, payload) = queue.pop(Duration::ZERO).unwrap();
        let i = match channel {
            Channel::Message => 0,
            Channel::Telemetry => 1,
            Channel::Bulk => 2,
            Channel::Control => unreachable!(),
        };
        sent[i] += cost(payload.len());
        total += cost(payload.len());
    }
    // Within one frame of 4 : 2 : 1 per round.
    for (i, weight) in [4, 2, 1].into_iter().enumerate() {
        let share = 7 * weight * 1024;
        assert!(sent[i].abs_diff(share) <= 2 * cost(96), "{sent:?}");
    }
}

/// SplitMix64, so the flood is reproducible from `SEED`.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

#[test]
fn caps_hold_under_a_hostile_flood() {
    const SEED: u64 = 0x5EED_0011;
    let config = NetworkConfig::default()
        .with_max_frame(4096)
        .unwrap()
        .with_channel_cap(Channel::Control, 1024)
        .unwrap()
        .with_channel_cap(Channel::Message, 4096)
        .unwrap()
        .with_channel_cap(Channel::Telemetry, 2048)
        .unwrap()
        .with_channel_cap(Channel::Bulk, 8192)
        .unwrap();
    let channels = [
        Channel::Control,
        Channel::Message,
        Channel::Telemetry,
        Channel::Bulk,
    ];
    let mut rng = Rng(SEED);
    let mut queue = SendQueue::new(&config);
    let mut expected: [VecDeque<Vec<u8>>; 4] = Default::default();
    let mut refused = 0;
    for step in 0..50_000u32 {
        // Pushes outnumber pops nine to one, so every channel stays full.
        if rng.below(10) == 0 {
            if let Some((channel, payload)) = queue.pop(Duration::ZERO) {
                let i = channels.iter().position(|c| *c == channel).unwrap();
                assert_eq!(expected[i].pop_front(), Some(payload), "seed {SEED:#x}");
            }
        } else {
            let i = rng.below(4);
            let payload = vec![step as u8; rng.below(3000)];
            let frame = cost(payload.len());
            let cap = config.channel_cap(channels[i]);
            if channels[i] == Channel::Telemetry && frame <= cap {
                // NET-21: Telemetry admits what fits its cap by evicting
                // its oldest frames, so the model mirrors that rule
                // instead of expecting a refusal.
                let mut held: usize = expected[i].iter().map(|p| cost(p.len())).sum();
                while held + frame > cap {
                    match expected[i].pop_front() {
                        Some(dropped) => held -= cost(dropped.len()),
                        // Unreachable: `frame <= cap`, so the queue empties first.
                        None => break,
                    }
                }
                expected[i].push_back(payload.clone());
                queue.push(channels[i], payload).unwrap();
            } else {
                match queue.push(channels[i], payload.clone()) {
                    Ok(()) => expected[i].push_back(payload),
                    Err(err) => {
                        assert_eq!(err.into_payload(), payload, "seed {SEED:#x}");
                        refused += 1;
                    }
                }
            }
        }
        for channel in channels {
            assert!(
                queue.queued_bytes(channel) <= config.channel_cap(channel),
                "seed {SEED:#x}, step {step}, {channel:?}"
            );
        }
    }
    assert!(refused > 10_000, "the flood never filled the queue");
}

/// NET-21: fifty latest-wins updates admitted under one key on a link that
/// is not draining leave exactly one frame, the newest.
#[test]
fn fifty_updates_with_one_key_send_only_the_newest() {
    let mut queue = SendQueue::new(&NetworkConfig::default());
    let opts = SendOpts {
        coalesce_key: Some([9; 16]),
        expires_at: None,
    };
    for update in 0..50u8 {
        queue
            .push_opts(Channel::Telemetry, opts, vec![update])
            .unwrap();
    }
    assert_eq!(queue.queued_bytes(Channel::Telemetry), cost(1));
    assert_eq!(
        queue.pop(Duration::ZERO),
        Some((Channel::Telemetry, vec![49]))
    );
    assert_eq!(queue.pop(Duration::ZERO), None);
}

/// NET-21: a frame past its deadline is dropped silently, before anything
/// is written, and its bytes return to the cap.
#[test]
fn expired_frames_are_never_written() {
    let config = NetworkConfig::default()
        .with_channel_cap(Channel::Message, 1000)
        .unwrap();
    let mut queue = SendQueue::new(&config);
    let deadline = Duration::from_millis(50);
    let expiring = SendOpts {
        coalesce_key: None,
        expires_at: Some(deadline),
    };
    queue
        .push_opts(Channel::Message, expiring, vec![1; 100])
        .unwrap();
    queue.push(Channel::Message, vec![2; 100]).unwrap();
    assert_eq!(queue.queued_bytes(Channel::Message), 2 * cost(100));

    // One millisecond before the deadline nothing has expired.
    queue.prune(deadline - Duration::from_millis(1));
    assert_eq!(queue.queued_bytes(Channel::Message), 2 * cost(100));

    // At the deadline the frame is gone, with no error and no event, and
    // only the frame without a deadline can still be written.
    assert_eq!(queue.queued_bytes(Channel::Message), 2 * cost(100));
    queue.prune(deadline);
    assert_eq!(queue.queued_bytes(Channel::Message), cost(100));
    assert_eq!(queue.pop(deadline), Some((Channel::Message, vec![2; 100])));
    assert_eq!(queue.pop(deadline), None);

    // `pop` prunes on its own: no separate prune call stands between an
    // expired frame and a write.
    let mut alone = SendQueue::new(&config);
    alone
        .push_opts(Channel::Message, expiring, vec![5; 10])
        .unwrap();
    assert_eq!(alone.pop(deadline), None);
    assert!(alone.is_empty());
}

/// NET-21: expiry frees cap bytes, so a push that was refused before the
/// deadline is admitted after it.
#[test]
fn expiry_returns_the_cap_for_a_new_push() {
    let config = NetworkConfig::default()
        .with_channel_cap(Channel::Message, 1000)
        .unwrap();
    let mut queue = SendQueue::new(&config);
    let expiring = SendOpts {
        coalesce_key: None,
        expires_at: Some(Duration::from_millis(50)),
    };
    queue
        .push_opts(Channel::Message, expiring, vec![1; 900])
        .unwrap();
    let newer: Vec<u8> = vec![2; 900];
    assert_eq!(
        queue.push(Channel::Message, newer.clone()),
        Err(SendError::QueueFull(newer))
    );
    queue.prune(Duration::from_millis(50));
    queue.push(Channel::Message, vec![2; 900]).unwrap();
    assert_eq!(queue.queued_bytes(Channel::Message), cost(900));
}

/// NET-21: coalescing replaces only a frame still waiting in the queue;
/// an in-flight frame is gone and never replaced, and the key is scoped
/// to one channel.
#[test]
fn coalescing_replaces_only_frames_still_waiting() {
    let mut queue = SendQueue::new(&NetworkConfig::default());
    let opts = SendOpts {
        coalesce_key: Some([3; 16]),
        expires_at: None,
    };
    queue.push_opts(Channel::Telemetry, opts, vec![1]).unwrap();
    // Popped: this frame is in flight.
    assert_eq!(
        queue.pop(Duration::ZERO),
        Some((Channel::Telemetry, vec![1]))
    );
    // The same key queues again rather than deduping against a sent frame.
    queue.push_opts(Channel::Telemetry, opts, vec![2]).unwrap();
    assert_eq!(queue.queued_bytes(Channel::Telemetry), cost(1));
    // The waiting frame with that key is replaced in place.
    queue.push_opts(Channel::Telemetry, opts, vec![3]).unwrap();
    assert_eq!(queue.queued_bytes(Channel::Telemetry), cost(1));
    assert_eq!(
        queue.pop(Duration::ZERO),
        Some((Channel::Telemetry, vec![3]))
    );

    // The same key on another channel is a different frame.
    queue.push_opts(Channel::Message, opts, vec![4]).unwrap();
    queue.push_opts(Channel::Bulk, opts, vec![5]).unwrap();
    assert_eq!(queue.queued_bytes(Channel::Message), cost(1));
    assert_eq!(queue.queued_bytes(Channel::Bulk), cost(1));
}

/// NET-21: a replacement is checked against the cap after freeing the old
/// bytes, so coalescing can never push a channel over its limit.
#[test]
fn a_replacement_cannot_overflow_the_cap() {
    let config = NetworkConfig::default()
        .with_channel_cap(Channel::Message, 1000)
        .unwrap();
    let mut queue = SendQueue::new(&config);
    let key = SendOpts {
        coalesce_key: Some([4; 16]),
        expires_at: None,
    };
    queue
        .push_opts(Channel::Message, key, vec![1; 900])
        .unwrap();
    queue.push(Channel::Message, vec![2; 90]).unwrap();
    // Freeing the 905 bytes of the first frame still cannot make room for
    // the 912-byte replacement: refused, and the waiting frame keeps its
    // place and its payload.
    let too_big: Vec<u8> = vec![3; 907];
    assert_eq!(
        queue.push_opts(Channel::Message, key, too_big),
        Err(SendError::QueueFull(vec![3; 907]))
    );
    assert_eq!(queue.queued_bytes(Channel::Message), cost(900) + cost(90));
    // A replacement of exactly the freed bytes is admitted, in place.
    queue
        .push_opts(Channel::Message, key, vec![4; 901])
        .unwrap();
    assert_eq!(queue.queued_bytes(Channel::Message), cost(901) + cost(90));
    assert_eq!(
        queue.pop(Duration::ZERO),
        Some((Channel::Message, vec![4; 901]))
    );
    assert_eq!(
        queue.pop(Duration::ZERO),
        Some((Channel::Message, vec![2; 90]))
    );
}

/// NET-21: Telemetry under pressure admits the newest by evicting the
/// oldest - it never returns QueueFull for a payload that fits its cap,
/// while a payload larger than the whole cap is still refused.
#[test]
fn telemetry_admits_the_newest_by_evicting_the_oldest() {
    let config = NetworkConfig::default()
        .with_channel_cap(Channel::Telemetry, 400)
        .unwrap();
    let mut queue = SendQueue::new(&config);
    let frame = cost(60);
    let fits = 400 / frame;
    for update in 0..fits as u8 {
        queue
            .push_opts(
                Channel::Telemetry,
                SendOpts {
                    coalesce_key: Some([update; 16]),
                    expires_at: None,
                },
                vec![update; 60],
            )
            .unwrap();
    }
    assert_eq!(queue.queued_bytes(Channel::Telemetry), fits * frame);
    // The queue is full, yet every later update is admitted.
    for update in fits as u8..(fits + 5) as u8 {
        queue
            .push_opts(
                Channel::Telemetry,
                SendOpts {
                    coalesce_key: Some([update; 16]),
                    expires_at: None,
                },
                vec![update; 60],
            )
            .unwrap();
        assert!(queue.queued_bytes(Channel::Telemetry) <= 400);
    }
    // The oldest are gone, the newest wait: exactly `fits` frames survive,
    // the newest ones.
    let mut seen = Vec::new();
    while let Some((_, payload)) = queue.pop(Duration::ZERO) {
        seen.push(payload[0]);
    }
    let total = fits + 5;
    assert_eq!(
        seen,
        ((total - fits) as u8..total as u8).collect::<Vec<u8>>()
    );

    // A payload larger than the whole cap cannot be queued by any rule.
    let oversized: Vec<u8> = vec![7; 401];
    assert_eq!(
        queue.push(Channel::Telemetry, oversized.clone()),
        Err(SendError::QueueFull(oversized))
    );
}
