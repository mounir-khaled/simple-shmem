use std::env;
use std::error::Error;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;

use simple_shmem::{Listener, StdListener};

fn main() -> Result<(), Box<dyn Error>> {
    let mut listener = Listener::<_, 4088>::new("/dev/shm/pingpong/")?;

    let spin_limit: u32 = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    loop {
        let (client_metadata, mut ring_buffer) =
            listener.accept::<32, _>(|metadata| metadata.uid() == 1000)?;

        // ring_buffer.set_timeout(Some(Duration::from_secs(30)));
        ring_buffer.set_spin_limit(spin_limit);

        eprintln!(
            "Accepted connection from uid={}, gid={}",
            client_metadata.uid(),
            client_metadata.gid()
        );

        let mut buf = [0u8; 4];
        loop {
            if let Err(e) = ring_buffer.write_fixed(b"ping") {
                eprintln!("Error writing to ring buffer: {}", e);
                break;
            }

            if let Err(e) = ring_buffer.read_fixed(&mut buf) {
                eprintln!("Error reading from ring buffer: {}", e);
                break;
            }

            if buf == *b"gbye" {
                eprintln!("Client said goodbye, closing connection");
                break;
            }
        }
    }
}
