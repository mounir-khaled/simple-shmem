use std::{
    env,
    error::Error,
    io::{Read, Write},
    os::unix::net::UnixStream,
    time::Instant,
};

use simple_shmem::{FastDualRingBuffers, StdDualRingBuffers};

fn main() -> Result<(), Box<dyn Error>> {
    const ROUNDS: usize = 100_000;

    let mut start = Instant::now();
    let stream = UnixStream::connect("/tmp/ping.sock")?;
    let mut ring_buffer = FastDualRingBuffers::connect(&stream)?;
    eprintln!("Connecting took {} µs", start.elapsed().as_micros());

    let spin_limit: u32 = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    start = Instant::now();
    let mut buf = [0u8; 4];
    for _ in 0..ROUNDS {
        ring_buffer.set_spin_limit(spin_limit);
        ring_buffer.read_exact(&mut buf)?;
        assert_eq!(&buf, b"ping");
        ring_buffer.write_all(b"pong")?;
    }

    eprintln!(
        "Average round-trip time: {} ns",
        start.elapsed().as_nanos() / ROUNDS as u128
    );

    ring_buffer.write_all(b"gbye")?;
    Ok(())
}
