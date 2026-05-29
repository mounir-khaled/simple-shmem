use std::{error::Error, fs::remove_file, io::Read, os::unix::net::UnixListener};

use simple_shmem::{FastDualRingBuffers, StdDualRingBuffers};

fn main() -> Result<(), Box<dyn Error>> {
    const MSG_SIZE: usize = 1024;

    let _ = remove_file("/tmp/reader.sock");
    let listener = UnixListener::bind("/tmp/reader.sock")?;

    let mut msg = [0u8; MSG_SIZE];
    loop {
        let (stream, _) = listener.accept()?;
        let mut rb = StdDualRingBuffers::accept(&stream)?;
        eprintln!("Accepted connection");
        loop {
            rb.read_exact(&mut msg)?;
            if msg[..4] == *b"gbye" {
                eprintln!("Writer said goodbye, exiting");
                break;
            }

            // sign with some secret signing key
            // and send back to the writer
            // for them to send an authenticated message
            // that proves that it came from the machine
            // with the signing key
        }
    }

    Ok(())
}
