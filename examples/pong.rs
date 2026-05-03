use std::io::{Read, Write};

use simple_shmem::DualRingBuffer;

fn main() {
    let mut ring_buffer = DualRingBuffer::<64>::new_client("/dev/shm/pingpong")
        .expect("failed to create DualRingBuffer");

    let mut buf = [0u8; 4];
    loop {
        ring_buffer
            .read_exact(buf.as_mut_slice())
            .expect("failed to read from ring buffer");

        ring_buffer
            .write_all("pong".as_bytes())
            .expect("failed to write to ring buffer");
    }
}
