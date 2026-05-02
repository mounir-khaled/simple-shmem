#![allow(dead_code)]

mod futex;
mod mmapped_rb;
mod page_size;
mod producer_consumer;
mod ringbuffer;

use producer_consumer::{Consumer, Producer};
use std::fs::{File, OpenOptions};
use std::io;
use std::os::fd::AsRawFd;
use std::path::Path;

use crate::page_size::page_size;

pub struct DualRingBuffer<const N: usize> {
    consumer: Consumer<N>,
    producer: Producer<N>,
}

impl<const N: usize> DualRingBuffer<N> {
    fn rb_offset() -> usize {
        assert_eq!(Consumer::<N>::object_size(), Producer::<N>::object_size());
        let rb_size = Consumer::<N>::object_size();
        let page_size = page_size();
        let offset = ((rb_size - 1) / page_size + 1) * page_size;

        offset
    }

    fn mmap_size() -> usize {
        assert_eq!(Consumer::<N>::object_size(), Producer::<N>::object_size());
        Self::rb_offset() + Consumer::<N>::object_size()
    }

    pub fn new_server<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = Self::open_mmapped_file(path, true)?;
        let fd = file.as_raw_fd();

        let mut consumer = Consumer::new(fd, 0)?;
        consumer.initialize();

        let mut producer = Producer::new(fd, Self::rb_offset() as i64)?;
        producer.initialize();

        Ok(Self { consumer, producer })
    }

    pub fn new_client<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = Self::open_mmapped_file(path, false)?;
        let fd = file.as_raw_fd();

        Ok(Self {
            consumer: Consumer::new(fd, Self::rb_offset() as i64)?,
            producer: Producer::new(fd, 0)?,
        })
    }

    fn open_mmapped_file<P: AsRef<Path>>(path: P, create: bool) -> io::Result<File> {
        let file = OpenOptions::new()
            .write(true)
            .read(true)
            .create(create)
            .open(path)?;

        file.set_len(Self::mmap_size() as u64)?;

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
