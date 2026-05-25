use std::{error::Error, fs::remove_file, io::Read, os::unix::net::UnixDatagram};

use simple_shmem::{FastDualRingBuffers, StdDualRingBuffers, listener::Listener};

fn main() -> Result<(), Box<dyn Error>> {
    const MSG_SIZE: usize = 1024;

    let _ = remove_file("/tmp/reader.sock");
    let uds = UnixDatagram::bind("/tmp/reader.sock")?;
    let mut listener = Listener::new(uds)?;

    let mut msg = [0u8; MSG_SIZE];
    loop {
        let mut rb: FastDualRingBuffers = listener.accept(|_, _| true)?;
        eprintln!("Accepted connection");
        loop {
            rb.read_exact(&mut msg)?;
            if msg[..4] == *b"gbye" {
                eprintln!("Writer said goodbye, exiting");
                break;
            }
        }
    }

    Ok(())
}
