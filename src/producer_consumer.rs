use std::{
    cmp,
    fs::File,
    hint, io,
    sync::atomic::{Ordering, fence},
    time::Duration,
};

use thiserror::Error;

use crate::ringbuffer_parts::{ConsumerOwned, Mmapped, MmappedMut, ProducerOwned};

const DEFAULT_SPIN_LIMIT: u32 = 100;

/// A ring buffer that can only write data
pub struct Producer<const N: usize> {
    timeout: Option<Duration>,
    /// Total number of spin iterations before sleeping via futex.
    spin_limit: u32,
    owned: MmappedMut<ProducerOwned<N>>,
    readonly: Mmapped<ConsumerOwned<N>>,
}

/// A ring buffer that can only read data
pub struct Consumer<const N: usize> {
    timeout: Option<Duration>,
    /// Total number of spin iterations before sleeping via futex.
    spin_limit: u32,
    owned: MmappedMut<ConsumerOwned<N>>,
    readonly: Mmapped<ProducerOwned<N>>,
}

#[derive(Error, Debug)]
pub enum ConsumerProducerError {
    #[error("Failed to mmap owned memory: {0}")]
    OwnedMmap(io::Error),
    #[error("Failed to mmap readonly memory: {0}")]
    ReadonlyMmap(io::Error),
}

impl<const N: usize> Consumer<N> {
    pub unsafe fn new(
        owned: MmappedMut<ConsumerOwned<N>>,
        readonly: Mmapped<ProducerOwned<N>>,
    ) -> Self {
        Self {
            timeout: None,
            spin_limit: DEFAULT_SPIN_LIMIT,
            owned,
            readonly,
        }
    }

    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
    }

    pub fn set_spin_limit(&mut self, spin_limit: u32) {
        self.spin_limit = spin_limit;
    }

    fn len(read_ptr: u32, write_ptr: u32) -> usize {
        if write_ptr >= read_ptr {
            (write_ptr - read_ptr) as usize
        } else {
            (N as u32 - read_ptr + write_ptr) as usize
        }
    }

    fn invalid_peer_ptr() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "peer sent out-of-range ring buffer pointer",
        )
    }
}

impl<const N: usize> io::Read for Consumer<N> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // Cache the two pointers in locals so LLVM can keep them in registers
        // across the spin loop rather than reloading through &mut self every iteration.
        let owned = &self.owned;
        let readonly = &self.readonly;
        let mut read_ptr = owned.read_ptr().load(Ordering::Acquire);
        let mut write_ptr = readonly.write_ptr().load(Ordering::Acquire);
        let mut i = self.spin_limit;
        // Spin until write_ptr != read_ptr (buffer non-empty).
        // read_ptr is owned by this consumer and does not change during the spin.
        while write_ptr == read_ptr {
            if i == 0 {
                owned.write_ptr_contended().store(1, Ordering::Release);
                // SeqCst fence pairs with the SeqCst fence in Producer between
                // store(write_ptr) and load(write_ptr_contended), preventing a lost wakeup.
                fence(Ordering::SeqCst);
                write_ptr = readonly.write_ptr().load(Ordering::Acquire);
                if write_ptr == read_ptr {
                    if let Err(e) = readonly.write_ptr().wait(write_ptr, self.timeout.as_ref()) {
                        owned.write_ptr_contended().store(0, Ordering::Release);
                        return Err(e);
                    }
                    write_ptr = readonly.write_ptr().load(Ordering::Acquire);
                }
            } else {
                i -= 1;
                hint::spin_loop();
                write_ptr = readonly.write_ptr().load(Ordering::Acquire);
            }
        }

        if i == 0 {
            owned.write_ptr_contended().store(0, Ordering::Release);
        }

        // Reject an out-of-range write_ptr supplied by the untrusted peer.
        // Without this check a malicious producer can set write_ptr = u32::MAX,
        // making len() huge and causing an OOB panic in the wrap-around slice below.
        if write_ptr >= N as u32 {
            return Err(Self::invalid_peer_ptr());
        }

        let bytes_read = cmp::min(Self::len(read_ptr, write_ptr), buf.len());

        let buffer = readonly.buffer();
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
        owned.read_ptr().store(read_ptr, Ordering::Release);
        // SeqCst fence pairs with the SeqCst fence in Producer between
        // store(read_ptr_contended) and load(read_ptr), preventing a lost wakeup.
        fence(Ordering::SeqCst);
        if readonly.read_ptr_contended().load(Ordering::Acquire) != 0 {
            owned.read_ptr().wake_one()?;
        }

        Ok(bytes_read)
    }
}

impl<const N: usize> Producer<N> {
    pub unsafe fn new(
        owned: MmappedMut<ProducerOwned<N>>,
        readonly: Mmapped<ConsumerOwned<N>>,
    ) -> Self {
        Self {
            timeout: None,
            spin_limit: DEFAULT_SPIN_LIMIT,
            owned,
            readonly,
        }
    }

    pub fn with_files_and_offsets(
        owned_fd: &File,
        owned_offset: isize,
        readonly_fd: &File,
        readonly_offset: isize,
    ) -> Result<Self, ConsumerProducerError> {
        let owned =
            MmappedMut::new(owned_fd, owned_offset).map_err(ConsumerProducerError::OwnedMmap)?;

        let readonly = Mmapped::new(readonly_fd, readonly_offset)
            .map_err(ConsumerProducerError::ReadonlyMmap)?;

        Ok(Self {
            timeout: None,
            spin_limit: DEFAULT_SPIN_LIMIT,
            owned,
            readonly,
        })
    }

    pub fn set_timeout(&mut self, timeout: Option<Duration>) {
        self.timeout = timeout;
    }

    pub fn set_spin_limit(&mut self, spin_limit: u32) {
        self.spin_limit = spin_limit;
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

    fn invalid_peer_ptr() -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "peer sent out-of-range ring buffer pointer",
        )
    }
}

impl<const N: usize> io::Write for Producer<N> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Cache the two pointers in locals so LLVM can keep them in registers
        // across the spin loop rather than reloading through &mut self every iteration.
        let owned = &self.owned;
        let readonly = &self.readonly;
        let write_ptr = owned.write_ptr().load(Ordering::Acquire);
        let mut read_ptr = readonly.read_ptr().load(Ordering::Acquire);
        if read_ptr >= N as u32 {
            return Err(Self::invalid_peer_ptr());
        }
        let mut bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
        let mut i = self.spin_limit;
        // Spin until the consumer frees enough space.
        // write_ptr is owned by this producer and does not change during the spin.
        while bytes_written == 0 {
            if i == 0 {
                owned.read_ptr_contended().store(1, Ordering::Release);
                // SeqCst fence pairs with the SeqCst fence in Consumer between
                // store(read_ptr) and load(read_ptr_contended), preventing a lost wakeup.
                fence(Ordering::SeqCst);
                read_ptr = readonly.read_ptr().load(Ordering::Acquire);
                if read_ptr >= N as u32 {
                    return Err(Self::invalid_peer_ptr());
                }
                bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
                if bytes_written == 0 {
                    if let Err(e) = readonly.read_ptr().wait(read_ptr, self.timeout.as_ref()) {
                        owned.read_ptr_contended().store(0, Ordering::Release);
                        return Err(e);
                    }
                    read_ptr = readonly.read_ptr().load(Ordering::Acquire);
                    if read_ptr >= N as u32 {
                        return Err(Self::invalid_peer_ptr());
                    }
                    bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
                }
            } else {
                i -= 1;
                hint::spin_loop();
                read_ptr = readonly.read_ptr().load(Ordering::Acquire);
                if read_ptr >= N as u32 {
                    return Err(Self::invalid_peer_ptr());
                }
                bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
            }
        }

        if i == 0 {
            owned.read_ptr_contended().store(0, Ordering::Release);
        }

        let buffer = owned.buffer_mut();
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

        let write_ptr = ((write_ptr_usize + bytes_written) % N) as u32;
        owned.write_ptr().store(write_ptr, Ordering::Release);
        // SeqCst fence pairs with the SeqCst fence in Consumer between
        // store(write_ptr_contended) and load(write_ptr), preventing a lost wakeup.
        fence(Ordering::SeqCst);
        if readonly.write_ptr_contended().load(Ordering::Acquire) != 0 {
            owned.write_ptr().wake_one()?;
        }

        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
