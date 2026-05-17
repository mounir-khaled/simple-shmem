use std::{
    env,
    error::Error,
    io::{Read, Write},
    time::Instant,
};

use simple_shmem::{FastDualRingBuffers, StdDualRingBuffers};

fn main() -> Result<(), Box<dyn Error>> {
    const ROUNDS: usize = 100_000;

    let spin_limit: u32 = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let mut start = Instant::now();
    let mut ring_buffer = FastDualRingBuffers::connect::<4088, _>("/dev/shm/pingpong")?;
    eprintln!("Connecting took {} µs", start.elapsed().as_micros());

    ring_buffer.set_spin_limit(spin_limit);

    start = Instant::now();
    let mut buf = [0u8; 4];
    for _ in 0..ROUNDS {
        ring_buffer.read_fixed(&mut buf)?;
        ring_buffer.write_fixed(b"pong")?;
    }

    eprintln!(
        "Average round-trip time: {} ns",
        start.elapsed().as_nanos() / ROUNDS as u128
    );

    ring_buffer.write_fixed(b"gbye")?;

    Ok(())
}
