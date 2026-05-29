use std::error::Error;
use std::io::Read;
use std::os::unix::net::UnixListener;
use std::{env, io::Write};

use simple_shmem::{FastDualRingBuffers, StdDualRingBuffers};

fn main() -> Result<(), Box<dyn Error>> {
    let spin_limit: u32 = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    let listener = UnixListener::bind("/tmp/ping.sock")?;

    loop {
        let (stream, _) = listener.accept()?;
        let mut ring_buffer = FastDualRingBuffers::accept(&stream)?;

        // ring_buffer.set_timeout(Some(Duration::from_secs(30)));
        ring_buffer.set_spin_limit(spin_limit);

        let mut buf = [0u8; 4];
        loop {
            if let Err(e) = ring_buffer.write_all(b"ping") {
                eprintln!("Error writing to ring buffer: {}", e);
                break;
            }

            if let Err(e) = ring_buffer.read_exact(&mut buf) {
                eprintln!("Error reading from ring buffer: {}", e);
                break;
            }

            if buf == *b"gbye" {
                eprintln!("Client said goodbye, closing connection");
                break;
            }

            assert_eq!(&buf, b"pong");
        }
    }
}
