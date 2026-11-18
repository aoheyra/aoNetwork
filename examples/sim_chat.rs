//! Three simulated phones find each other, link up and chat.
//!
//! Run with `cargo run --example sim_chat`. The run is seeded, so it prints
//! the same transcript every time.

use std::task::{Context, Poll, Waker};
use std::time::Duration;

use ao_network::{
    AppHello, BeaconFlags, Channel, LocalTag, Network, NetworkConfig, NetworkEvent, SimConfig,
    SimNode, SimTransport, SimWorld,
};

const NAMES: [&str; 3] = ["alice", "bob", "carol"];

fn main() {
    let world = SimWorld::new(SimConfig {
        seed: 7,
        ..SimConfig::default()
    });
    let mut nodes: Vec<Network<SimTransport>> = Vec::new();
    for (i, name) in (1u8..).zip(NAMES) {
        let radio = world.add_node(SimNode::default()).expect("valid node");
        let mut net = Network::new(NetworkConfig::default(), radio);
        // aoMesh would put a signed hello and real tags here.
        let app_hello = AppHello::new(name.as_bytes().to_vec()).expect("short name");
        net.set_local_hello(LocalTag::new([i; 16]), app_hello);
        let accepting = BeaconFlags {
            accepting: true,
            ..BeaconFlags::default()
        };
        net.set_beacon([i; 8], [0; 4], accepting);
        nodes.push(net);
    }
    for a in 0..nodes.len() {
        for b in a + 1..nodes.len() {
            world.set_in_range(
                nodes[a].transport().node(),
                nodes[b].transport().node(),
                true,
            );
        }
    }

    let mut said_hello = [false; 3];
    let mut cx = Context::from_waker(Waker::noop());
    while world.now() < Duration::from_secs(3) {
        world.advance(Duration::from_millis(10));
        for (i, net) in nodes.iter_mut().enumerate() {
            while let Poll::Ready(event) = net.poll_event(&mut cx, world.now()) {
                print(world.now(), NAMES[i], &event);
            }
            // Once linked to both others, greet everyone.
            if !said_hello[i] && net.links().len() == 2 {
                said_hello[i] = true;
                let text = format!("hello from {}", NAMES[i]);
                for (link, err) in net.broadcast(world.now(), Channel::Message, text.as_bytes()) {
                    println!(
                        "{:>6} {:<5} could not send on {link:?}: {err}",
                        "", NAMES[i]
                    );
                }
            }
        }
    }
}

fn print(now: Duration, me: &str, event: &NetworkEvent) {
    let at = format!("{}ms", now.as_millis());
    match event {
        NetworkEvent::LinkUp { link, remote_hello } => {
            let peer = String::from_utf8_lossy(remote_hello.app_hello.as_bytes());
            println!("{at:>6} {me:<5} link {} up with {peer}", link.raw());
        }
        NetworkEvent::Data { link, payload, .. } => {
            let text = String::from_utf8_lossy(payload);
            println!("{at:>6} {me:<5} got on link {}: {text}", link.raw());
        }
        NetworkEvent::LinkDown { link, reason } => {
            println!("{at:>6} {me:<5} link {} down: {reason:?}", link.raw());
        }
        other => println!("{at:>6} {me:<5} {other:?}"),
    }
}
