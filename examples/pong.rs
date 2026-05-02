use std::io::{Read, Write};

use simple_shmem::DualRingBuffer;

fn main() {
    let shmem_fd = DualRingBuffer::<1024>::open_mmapped_file("/pingpong".into())
        .expect("failed to open mmapped file");

    let mut ring_buffer =
        DualRingBuffer::<1024>::new_client(shmem_fd).expect("failed to create DualRingBuffer");

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
