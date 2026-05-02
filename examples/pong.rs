use std::io::{Read, Write};

use simple_shmem::DualRingBuffer;

fn main() {
    let mut ring_buffer = DualRingBuffer::<1024>::new_client("/dev/shm/pingpong")
        .expect("failed to create DualRingBuffer");

    let mut buf = [0u8; 4];
    loop {
        let mut bytes_read = ring_buffer
            .read(buf.as_mut_slice())
            .expect("failed to read from ring buffer");

        while bytes_read == 0 {
            bytes_read = ring_buffer
                .read(buf.as_mut_slice())
                .expect("failed to read from ring buffer");
        }

        ring_buffer
            .write_all("pong".as_bytes())
            .expect("failed to write to ring buffer");
    }
}
