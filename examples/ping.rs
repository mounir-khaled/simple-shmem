use std::error::Error;
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;

use simple_shmem::StdListener;

fn main() -> Result<(), Box<dyn Error>> {
    let mut listener = StdListener::new("/dev/shm/pingpong/")?;

    loop {
        let (client_metadata, mut ring_buffer) =
            listener.accept(|metadata| metadata.uid() == 1000)?;

        // ring_buffer.set_timeout(Some(Duration::from_secs(30)));

        eprintln!(
            "Accepted connection from uid={}, gid={}",
            client_metadata.uid(),
            client_metadata.gid()
        );

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
        }
    }
}
