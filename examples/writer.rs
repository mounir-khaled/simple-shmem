use std::{error::Error, fs::remove_file, io::Write, os::unix::net::UnixDatagram};

use simple_shmem::{FastDualRingBuffers, StdDualRingBuffers};

fn main() -> Result<(), Box<dyn Error>> {
    const ROUNDS: usize = 100000;
    const MSG_SIZE: usize = 1024;

    let _ = remove_file("/tmp/writer.sock");
    let mut uds = UnixDatagram::bind("/tmp/writer.sock")?;
    let mut rb = FastDualRingBuffers::connect(&mut uds, "/tmp/reader.sock", |_, _| true)?;

    let mut msg = [0u8; MSG_SIZE];
    for i in 0..MSG_SIZE {
        msg[i] = i as u8;
    }

    let start = std::time::Instant::now();

    for _ in 0..ROUNDS {
        rb.write_all(&msg)?;
    }

    eprintln!(
        "throughput: {} MiB/s",
        MSG_SIZE as f64 * ROUNDS as f64 / (1024.0 * 1024.0) / start.elapsed().as_secs_f64()
    );

    msg[..4].copy_from_slice(b"gbye");
    rb.write_all(&msg)?;

    Ok(())
}
