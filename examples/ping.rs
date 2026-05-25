use std::error::Error;
use std::{env, os::unix::net::UnixDatagram};

use simple_shmem::FastDualRingBuffers;
use simple_shmem::listener::Listener;

fn main() -> Result<(), Box<dyn Error>> {
    // let mut listener = rb_listener::Listener::<_, 4088>::new("/dev/shm/pingpong/")?;
    let listener = UnixDatagram::bind("/tmp/ping.sock")?;
    let mut listener = Listener::new(listener)?;

    let spin_limit: u32 = env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000);

    loop {
        let mut ring_buffer: FastDualRingBuffers = listener.accept(|uid, _| uid == 1000)?;

        // ring_buffer.set_timeout(Some(Duration::from_secs(30)));
        ring_buffer.set_spin_limit(spin_limit);

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

            assert_eq!(&buf, b"pong");
        }
    }
}
