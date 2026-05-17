use std::{
    env,
    error::Error,
    io::{Read, Write},
    thread,
    time::Instant,
};

use simple_shmem::{DualRingBuffers, StdDualRingBuffers};

fn main() -> Result<(), Box<dyn Error>> {
    const ROUNDS: usize = 10_000;

    let spin_limit: u32 = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let mut start = Instant::now();
    let mut ring_buffer = StdDualRingBuffers::connect("/dev/shm/pingpong")?;
    eprintln!("Connecting took {} µs", start.elapsed().as_micros());

    ring_buffer.set_spin_limit(spin_limit);

    start = Instant::now();
    let mut buf = [0u8; 4];
    for _ in 0..ROUNDS {
        ring_buffer.read_exact(&mut buf)?;
        ring_buffer.write_all(b"pong")?;
    }

    eprintln!(
        "Average round-trip time: {} ns",
        start.elapsed().as_nanos() / ROUNDS as u128
    );

    ring_buffer.write_all(b"gbye")?;

    Ok(())
}
