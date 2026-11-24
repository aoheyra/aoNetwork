//! Observability (NET-23): the log a simulator run produces.
//!
//! This file holds a single test on purpose. The subscriber is installed
//! for the test's own thread, and `tracing` caches per call site whether
//! anyone listens; other tests running the same code on other threads of
//! this process would race that cache and the log would come out empty.
//! In its own test binary nothing else runs beside it.
#![cfg(feature = "sim")]

use std::io;
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Waker};
use std::time::Duration;

use ao_network::{
    AppHello, BeaconFlags, Channel, DisconnectReason, LocalTag, Network, NetworkConfig, SimConfig,
    SimNode, SimTransport, SimWorld,
};

const TICK: Duration = Duration::from_millis(10);

/// A log sink the test can read back.
#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

impl Captured {
    fn text(&self) -> String {
        let bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        String::from_utf8_lossy(&bytes).into_owned()
    }
}

impl io::Write for Captured {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut bytes = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        bytes.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn run(world: &SimWorld, nets: &mut [Network<SimTransport>], duration: Duration) {
    let mut cx = Context::from_waker(Waker::noop());
    let end = world.now() + duration;
    while world.now() < end {
        world.advance(TICK);
        for net in nets.iter_mut() {
            while net.poll_event(&mut cx, world.now()).is_ready() {}
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes
        .iter()
        .fold(String::with_capacity(bytes.len() * 2), |mut out, byte| {
            use std::fmt::Write as _;
            write!(out, "{byte:02x}").expect("writing to a String cannot fail");
            out
        })
}

/// Done when: a simulator run produces a readable log. Two nodes link up,
/// exchange a message, and A closes the link for the degree cap; every
/// line of the link's lifecycle carries its span, so the link's id, peer,
/// role and slot read off each line, at both ends.
#[test]
fn a_simulator_run_produces_a_readable_log() {
    let captured = Captured::default();
    let writer = captured.clone();
    let subscriber = tracing_subscriber::fmt()
        .with_writer(move || writer.clone())
        .with_ansi(false)
        .without_time()
        .with_target(false)
        .with_max_level(tracing::Level::DEBUG)
        .finish();
    let keys = tracing::subscriber::with_default(subscriber, || {
        let world = SimWorld::new(SimConfig::default());
        let accepting = BeaconFlags {
            accepting: true,
            ..BeaconFlags::default()
        };
        let mut nets: Vec<Network<SimTransport>> = (1..=2u8)
            .map(|i| {
                let radio = world.add_node(SimNode::default()).unwrap();
                let mut net = Network::new(NetworkConfig::default(), radio);
                net.set_local_hello(LocalTag::new([i; 16]), AppHello::new(Vec::new()).unwrap());
                net.set_beacon([i; 8], [0; 4], accepting);
                net
            })
            .collect();
        let (na, nb) = (nets[0].transport().node(), nets[1].transport().node());
        world.set_in_range(na, nb, true);
        run(&world, &mut nets, Duration::from_secs(1));
        let link = nets[0].links()[0].link;
        let now = world.now();
        nets[0]
            .send(now, link, Channel::Message, b"hello".to_vec())
            .unwrap();
        run(&world, &mut nets, Duration::from_millis(500));
        let now = world.now();
        nets[0].disconnect(now, link, DisconnectReason::DegreeCap);
        run(&world, &mut nets, Duration::from_millis(500));
        [0, 1].map(|i| hex(nets[i].transport().peer_key().as_bytes()))
    });
    let log = captured.text();
    println!("{log}");

    let [a, b] = keys;
    let a_end = format!("link{{id=1 peer={b} role=Initiator class=Stable}}");
    let b_end = format!("link{{id=1 peer={a} role=Responder class=Stable}}");
    let expect = [
        format!("DEBUG {a_end}: opened"),
        format!("INFO {a_end}: up version=1"),
        format!("INFO {a_end}: down reason=DegreeCap"),
        format!("DEBUG {b_end}: opened"),
        format!("INFO {b_end}: up version=1"),
        format!("INFO {b_end}: down reason=Bye"),
    ];
    let lines: Vec<&str> = log.lines().map(str::trim).collect();
    for line in &expect {
        assert!(
            lines.contains(&line.as_str()),
            "missing {line:?} in:\n{log}"
        );
    }
}
