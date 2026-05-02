use std::{cmp, io};

pub struct RingBuffer<const N: usize> {
    read_ptr: u32,
    write_ptr: u32,
    buffer: [u8; N],
}

impl<const N: usize> RingBuffer<N> {
    pub fn new(read_ptr: u32, write_ptr: u32, buffer: [u8; N]) -> Self {
        assert!(read_ptr < N as u32, "Head index out of bounds");
        assert!(write_ptr < N as u32, "Tail index out of bounds");

        Self {
            read_ptr,
            write_ptr,
            buffer,
        }
    }

    pub fn initialize(&mut self) {
        self.read_ptr = 0;
        self.write_ptr = 0;
    }

    pub fn capacity(&self) -> usize {
        N
    }

    pub fn len(&self) -> usize {
        if self.write_ptr >= self.read_ptr {
            (self.write_ptr - self.read_ptr) as usize
        } else {
            (N as u32 - self.read_ptr + self.write_ptr) as usize
        }
    }

    pub fn empty_space(&self) -> usize {
        // We need to keep one byte empty to distinguish between full and empty states
        N - 1 - self.len()
    }
}

impl<const N: usize> Default for RingBuffer<N> {
    fn default() -> Self {
        Self {
            read_ptr: 0,
            write_ptr: 0,
            buffer: [0; N],
        }
    }
}

impl<const N: usize> io::Read for RingBuffer<N> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let bytes_read = cmp::min(self.len(), buf.len());
        let read_ptr = self.read_ptr as usize;
        if read_ptr + bytes_read <= N {
            buf[..bytes_read].copy_from_slice(&self.buffer[read_ptr..(read_ptr + bytes_read)]);
        } else {
            let first_part_len = N - read_ptr;
            buf[..first_part_len].copy_from_slice(&self.buffer[read_ptr..]);
            buf[first_part_len..bytes_read]
                .copy_from_slice(&self.buffer[..(bytes_read - first_part_len)]);
        }

        self.read_ptr = ((read_ptr + bytes_read) % N) as u32;
        Ok(bytes_read)
    }
}

impl<const N: usize> io::Write for RingBuffer<N> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = cmp::min(self.empty_space(), buf.len());
        let write_ptr = self.write_ptr as usize;
        if write_ptr + bytes_written <= N {
            self.buffer[write_ptr..(write_ptr + bytes_written)]
                .copy_from_slice(&buf[..bytes_written]);
        } else {
            let first_part_len = N - write_ptr;
            self.buffer[write_ptr..].copy_from_slice(&buf[..first_part_len]);
            self.buffer[..(bytes_written - first_part_len)]
                .copy_from_slice(&buf[first_part_len..bytes_written]);
        }

        self.write_ptr = ((write_ptr + bytes_written) % N) as u32;
        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
