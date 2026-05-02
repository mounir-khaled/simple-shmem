#![allow(dead_code)]

mod futex;
mod mmapped_rb;
mod page_size;
mod ringbuffer;

use std::io;

use crate::mmapped_rb::MmappedRingBuffer;
use crate::page_size::page_size;

struct Consumer<const N: usize>(MmappedRingBuffer<N>);
struct Producer<const N: usize>(MmappedRingBuffer<N>);

pub struct DualRingBuffer<const N: usize> {
    consumer: Consumer<N>,
    producer: Producer<N>,
}

impl<const N: usize> Consumer<N> {
    pub fn new(fd: i32, offset: i64) -> io::Result<Self> {
        Ok(Self(MmappedRingBuffer::new(fd, offset)?))
    }

    pub fn object_size() -> usize {
        MmappedRingBuffer::<N>::object_size()
    }
}

impl<const N: usize> io::Read for Consumer<N> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl<const N: usize> Producer<N> {
    pub fn new(fd: i32, offset: i64) -> io::Result<Self> {
        Ok(Self(MmappedRingBuffer::new(fd, offset)?))
    }

    pub fn object_size() -> usize {
        MmappedRingBuffer::<N>::object_size()
    }
}

impl<const N: usize> io::Write for Producer<N> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<const N: usize> DualRingBuffer<N> {
    fn rb_offset() -> i64 {
        assert_eq!(Consumer::<N>::object_size(), Producer::<N>::object_size());
        let rb_size = Consumer::<N>::object_size() as i64;
        let page_size = page_size() as i64;
        let offset = ((rb_size - 1) / page_size + 1) * page_size;

        offset
    }

    fn mmap_size() -> i64 {
        assert_eq!(Consumer::<N>::object_size(), Producer::<N>::object_size());
        Self::rb_offset() + Consumer::<N>::object_size() as i64
    }

    pub fn new_server(fd: i32) -> io::Result<Self> {
        let mut consumer = Consumer::new(fd, 0)?;
        consumer.0.initialize();

        let mut producer = Producer::new(fd, Self::rb_offset())?;
        producer.0.initialize();

        Ok(Self { consumer, producer })
    }

    pub fn new_client(fd: i32) -> io::Result<Self> {
        Ok(Self {
            consumer: Consumer::new(fd, Self::rb_offset())?,
            producer: Producer::new(fd, 0)?,
        })
    }

    pub fn open_mmapped_file(mut name: String) -> io::Result<i32> {
        name.push('\0');
        let file = unsafe {
            libc::shm_open(
                name.as_bytes().as_ptr() as *const libc::c_char,
                libc::O_RDWR | libc::O_CREAT,
                libc::S_IRUSR | libc::S_IWUSR | libc::S_IRGRP | libc::S_IWGRP,
            )
        };

        if file == -1 {
            return Err(io::Error::last_os_error());
        }

        let status = unsafe { libc::ftruncate(file, Self::mmap_size()) };
        if status == -1 {
            unsafe { libc::close(file) };
            return Err(io::Error::last_os_error());
        }

        Ok(file)
    }
}

impl<const N: usize> io::Read for DualRingBuffer<N> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.consumer.read(buf)
    }
}

impl<const N: usize> io::Write for DualRingBuffer<N> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.producer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
