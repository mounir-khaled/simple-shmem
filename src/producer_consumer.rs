use crate::mmapped_rb::MmappedRingBuffer;
use std::io;

pub struct Consumer<const N: usize>(MmappedRingBuffer<N>);
pub struct Producer<const N: usize>(MmappedRingBuffer<N>);

impl<const N: usize> Consumer<N> {
    pub fn new(fd: i32, offset: i64) -> io::Result<Self> {
        Ok(Self(MmappedRingBuffer::new(fd, offset)?))
    }

    pub fn object_size() -> usize {
        MmappedRingBuffer::<N>::object_size()
    }

    pub fn initialize(&mut self) {
        self.0.initialize();
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

    pub fn initialize(&mut self) {
        self.0.initialize();
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
