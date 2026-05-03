use std::io::{Read, Write};
use std::time::Instant;

use simple_shmem::DualRingBuffer;

fn main() {
    let mut ring_buffer = DualRingBuffer::<64>::new_server("/dev/shm/pingpong")
        .expect("failed to create DualRingBuffer");

    let mut buf = vec![0u8; 4];
    let mut start = Instant::now();
    let rounds = 1000000;
    for i in 0..rounds + 1 {
        if i == 1 {
            start = Instant::now();
        }

        buf.copy_from_slice("ping".as_bytes());
        ring_buffer
            .write_all(buf.as_slice())
            .expect("failed to write to ring buffer");

        ring_buffer
            .read_exact(buf.as_mut_slice())
            .expect("failed to read from ring buffer");
    }

    let avg = start.elapsed().as_nanos() / rounds;
    eprintln!("Average round-trip time: {} ns", avg);
}
