use std::{io, time::Duration};

use crate::producer_consumer::{Consumer, Producer};

pub struct DualRingBuffers<const N: usize> {
    consumer: Consumer<N>,
    producer: Producer<N>,
}

pub(crate) enum FileOrder {
    ConsumerFirst,
    ProducerFirst,
}

impl<const N: usize> DualRingBuffers<N> {
    pub fn new(consumer: Consumer<N>, producer: Producer<N>) -> Self {
        Self { consumer, producer }
    }

    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.consumer.set_timeout(timeout);
        self.producer.set_timeout(timeout);
    }

    pub fn set_spin_limit(&mut self, spin_limit: u32) {
        self.consumer.set_spin_limit(spin_limit);
        self.producer.set_spin_limit(spin_limit);
    }
}

impl<const N: usize> io::Read for DualRingBuffers<N> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.consumer.read(buf)
    }
}

impl<const N: usize> io::Write for DualRingBuffers<N> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.producer.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.producer.flush()
    }
}
