use std::{cmp, hint, io, os::fd::BorrowedFd, sync::atomic::Ordering, time::Duration};

use thiserror::Error;

use crate::ringbuffer_parts::{ConsumerOwned, ProducerOwned, munmap};

const SPIN_LIMIT: u32 = 100;

/// A ring buffer that can only write data
pub struct Producer<const N: usize> {
    timeout: Option<Duration>,
    owned: &'static ProducerOwned<N>,
    readonly: &'static ConsumerOwned<N>,
}

/// A ring buffer that can only read data
pub struct Consumer<const N: usize> {
    timeout: Option<Duration>,
    owned: &'static ConsumerOwned<N>,
    readonly: &'static ProducerOwned<N>,
}

#[derive(Error, Debug)]
pub enum ConsumerProducerError {
    #[error("Failed to mmap owned memory: {0}")]
    OwnedMmap(io::Error),
    #[error("Failed to mmap readonly memory: {0}")]
    ReadonlyMmap(io::Error),
}

impl<const N: usize> Consumer<N> {
    pub fn new(
        owned_fd: BorrowedFd,
        owned_offset: isize,
        readonly_fd: BorrowedFd,
        readonly_offset: isize,
    ) -> Result<Self, ConsumerProducerError> {
        let owned = ConsumerOwned::mmap_rw(owned_fd, owned_offset)
            .map_err(ConsumerProducerError::OwnedMmap)?;

        let readonly = ProducerOwned::mmap_ro(readonly_fd, readonly_offset)
            .map_err(ConsumerProducerError::ReadonlyMmap)?;

        Ok(Self {
            timeout: None,
            owned,
            readonly,
        })
    }

    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
    }

    fn len(read_ptr: u32, write_ptr: u32) -> usize {
        if write_ptr >= read_ptr {
            (write_ptr - read_ptr) as usize
        } else {
            (N as u32 - read_ptr + write_ptr) as usize
        }
    }
}

impl<const N: usize> Drop for Consumer<N> {
    fn drop(&mut self) {
        let _ = unsafe { munmap(self.owned as *const ConsumerOwned<N>) };
        let _ = unsafe { munmap(self.readonly as *const ProducerOwned<N>) };
    }
}

impl<const N: usize> io::Read for Consumer<N> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut read_ptr = self.owned.read_ptr().load(Ordering::Acquire);
        let mut write_ptr = self.readonly.write_ptr().load(Ordering::Acquire);
        let mut bytes_read = cmp::min(Self::len(read_ptr, write_ptr), buf.len());
        let mut i = SPIN_LIMIT;
        while bytes_read == 0 {
            // spin for a while before sleeping to reduce latency in the case of short waits
            if i == 0 {
                self.owned.write_ptr_contended().store(1, Ordering::SeqCst);
                // Re-check write_ptr AFTER the SeqCst store. If the writer updated
                // write_ptr and then read the flag (seeing 0) before our store, we will
                // see the new write_ptr here and skip the sleep, preventing a missed wake.
                // SeqCst is required (not just Acquire) so that this load participates in
                // the global SeqCst total order and cannot be reordered past the store above
                // on weakly-ordered architectures.
                write_ptr = self.readonly.write_ptr().load(Ordering::SeqCst);
                bytes_read = cmp::min(Self::len(read_ptr, write_ptr), buf.len());
                if bytes_read == 0 {
                    if let Err(e) = self
                        .readonly
                        .write_ptr()
                        .wait(write_ptr, self.timeout.as_ref())
                    {
                        // Clear the flag before returning so it is not left permanently set,
                        // which would cause unnecessary wake_one calls on every future write.
                        self.owned.write_ptr_contended().store(0, Ordering::SeqCst);
                        return Err(e);
                    }
                }
            } else {
                i -= 1;
                hint::spin_loop();
            }

            read_ptr = self.owned.read_ptr().load(Ordering::Acquire);
            write_ptr = self.readonly.write_ptr().load(Ordering::Acquire);
            bytes_read = cmp::min(Self::len(read_ptr, write_ptr), buf.len());
        }

        if i == 0 {
            self.owned.write_ptr_contended().store(0, Ordering::SeqCst);
        }

        let buffer = self.readonly.buffer();
        let read_ptr_usize = read_ptr as usize;
        if read_ptr_usize + bytes_read <= N {
            buf[..bytes_read]
                .copy_from_slice(&buffer[read_ptr_usize..(read_ptr_usize + bytes_read)]);
        } else {
            let first_part_len = N - read_ptr_usize;
            buf[..first_part_len].copy_from_slice(&buffer[read_ptr_usize..]);
            buf[first_part_len..bytes_read]
                .copy_from_slice(&buffer[..(bytes_read - first_part_len)]);
        }

        read_ptr = ((read_ptr_usize + bytes_read) % N) as u32;
        // SeqCst prevents the store from being reordered after the flag load below.
        // Without this, the waker could read the flag before the pointer update is visible.
        self.owned.read_ptr().store(read_ptr, Ordering::SeqCst);

        // SeqCst ensures this load is ordered after the SeqCst store to read_ptr above,
        // preventing a missed wake on weakly-ordered architectures.
        if self.readonly.read_ptr_contended().load(Ordering::SeqCst) != 0 {
            self.owned.read_ptr().wake_one()?;
        }

        Ok(bytes_read)
    }
}

impl<const N: usize> Producer<N> {
    pub fn new(
        owned_fd: BorrowedFd,
        owned_offset: isize,
        readonly_fd: BorrowedFd,
        readonly_offset: isize,
    ) -> Result<Self, ConsumerProducerError> {
        let owned = ProducerOwned::mmap_rw(owned_fd, owned_offset)
            .map_err(ConsumerProducerError::OwnedMmap)?;

        let readonly = ConsumerOwned::mmap_ro(readonly_fd, readonly_offset)
            .map_err(ConsumerProducerError::ReadonlyMmap)?;

        Ok(Self {
            timeout: None,
            owned,
            readonly,
        })
    }

    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
    }

    fn empty_space(read_ptr: u32, write_ptr: u32) -> usize {
        N - 1 - Self::len(read_ptr, write_ptr)
    }

    fn len(read_ptr: u32, write_ptr: u32) -> usize {
        if write_ptr >= read_ptr {
            (write_ptr - read_ptr) as usize
        } else {
            (N as u32 - read_ptr + write_ptr) as usize
        }
    }
}

impl<const N: usize> Drop for Producer<N> {
    fn drop(&mut self) {
        let _ = unsafe { munmap(self.owned as *const ProducerOwned<N>) };
        let _ = unsafe { munmap(self.readonly as *const ConsumerOwned<N>) };
    }
}

impl<const N: usize> io::Write for Producer<N> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut write_ptr = self.owned.write_ptr().load(Ordering::Acquire);
        let mut read_ptr = self.readonly.read_ptr().load(Ordering::Acquire);
        let mut bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
        let mut i = SPIN_LIMIT;
        while bytes_written == 0 {
            // spin for a while before sleeping to reduce latency in the case of short waits
            if i == 0 {
                self.owned.read_ptr_contended().store(1, Ordering::SeqCst);
                // Re-check read_ptr AFTER the SeqCst store. If the reader advanced
                // read_ptr and then read the flag (seeing 0) before our store, we will
                // see the new read_ptr here and skip the sleep, preventing a missed wake.
                // SeqCst is required (not just Acquire) so that this load participates in
                // the global SeqCst total order and cannot be reordered past the store above
                // on weakly-ordered architectures.
                read_ptr = self.readonly.read_ptr().load(Ordering::SeqCst);
                bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
                if bytes_written == 0 {
                    if let Err(e) = self
                        .readonly
                        .read_ptr()
                        .wait(read_ptr, self.timeout.as_ref())
                    {
                        // Clear the flag before returning so it is not left permanently set,
                        // which would cause unnecessary wake_one calls on every future read.
                        self.owned.read_ptr_contended().store(0, Ordering::SeqCst);
                        return Err(e);
                    }
                }
            } else {
                i -= 1;
                hint::spin_loop();
            }

            write_ptr = self.owned.write_ptr().load(Ordering::Acquire);
            read_ptr = self.readonly.read_ptr().load(Ordering::Acquire);
            bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
        }

        if i == 0 {
            self.owned.read_ptr_contended().store(0, Ordering::SeqCst);
        }

        let buffer = self.owned.buffer_mut();
        let write_ptr_usize = write_ptr as usize;
        if write_ptr_usize + bytes_written <= N {
            buffer[write_ptr_usize..(write_ptr_usize + bytes_written)]
                .copy_from_slice(&buf[..bytes_written]);
        } else {
            let first_part_len = N - write_ptr_usize;
            buffer[write_ptr_usize..].copy_from_slice(&buf[..first_part_len]);
            buffer[..(bytes_written - first_part_len)]
                .copy_from_slice(&buf[first_part_len..bytes_written]);
        }

        write_ptr = ((write_ptr_usize + bytes_written) % N) as u32;
        // SeqCst prevents the store from being reordered after the flag load below.
        // Without this, the waker could read the flag before the pointer update is visible.
        self.owned.write_ptr().store(write_ptr, Ordering::SeqCst);

        // SeqCst ensures this load is ordered after the SeqCst store to write_ptr above,
        // preventing a missed wake on weakly-ordered architectures.
        if self.readonly.write_ptr_contended().load(Ordering::SeqCst) != 0 {
            self.owned.write_ptr().wake_one()?;
        }

        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
