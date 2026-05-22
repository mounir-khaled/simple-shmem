use std::{
    env,
    error::Error,
    io::{Read, Write},
    os::unix::net::{UnixDatagram, UnixStream},
    time::Instant,
};

use simple_shmem::{FastDualRingBuffers, StdDualRingBuffers};

fn main() -> Result<(), Box<dyn Error>> {
    const ROUNDS: usize = 100_000;

    let mut uds = UnixDatagram::bind("/tmp/pong.sock")?;

    let spin_limit: u32 = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let mut start = Instant::now();
    eprintln!("Connecting took {} µs", start.elapsed().as_micros());

    start = Instant::now();
    let mut buf = [0u8; 4];
    for _ in 0..ROUNDS {
        let mut ring_buffer =
            FastDualRingBuffers::connect(&mut uds, "/tmp/ping.sock", |uid, _| {
                uid == 1000 || uid == 0
            })?;

        ring_buffer.set_spin_limit(spin_limit);
        ring_buffer.read_fixed(&mut buf)?;
        assert_eq!(&buf, b"ping");
        ring_buffer.write_fixed(b"pong")?;
        ring_buffer.write_fixed(b"gbye")?;
    }

    eprintln!(
        "Average round-trip time: {} ns",
        start.elapsed().as_nanos() / ROUNDS as u128
    );

    Ok(())
}
