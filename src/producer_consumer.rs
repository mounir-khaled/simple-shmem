use std::{cmp, hint, io, os::fd::BorrowedFd, sync::atomic::Ordering, time::Duration};

use thiserror::Error;

use crate::ringbuffer_parts::{ConsumerOwned, ProducerOwned, munmap};

const DEFAULT_SPIN_LIMIT: u32 = 100;

/// A ring buffer that can only write data
pub struct Producer<const N: usize> {
    timeout: Option<Duration>,
    /// Total number of spin iterations before sleeping via futex.
    spin_limit: u32,
    owned: &'static ProducerOwned<N>,
    readonly: &'static ConsumerOwned<N>,
}

/// A ring buffer that can only read data
pub struct Consumer<const N: usize> {
    timeout: Option<Duration>,
    /// Total number of spin iterations before sleeping via futex.
    spin_limit: u32,
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

    fn len(read_ptr: u32, write_ptr: u32) -> usize {
        if write_ptr >= read_ptr {
            (write_ptr - read_ptr) as usize
        } else {
            (N as u32 - read_ptr + write_ptr) as usize
        }
    }

    /// Read exactly `LEN` bytes, spinning until they are available.
    ///
    /// Unlike `io::Read::read`, the size is a compile-time constant so LLVM
    /// inlines the memory copy as a direct register-width load/store instead
    /// of an indirect `memcpy` call.
    pub fn read_fixed<const LEN: usize>(&mut self, buf: &mut [u8; LEN]) -> io::Result<()> {
        let owned = self.owned;
        let readonly = self.readonly;
        let read_ptr = owned.read_ptr().load(Ordering::Acquire);
        let mut write_ptr = readonly.write_ptr().load(Ordering::Acquire);
        let mut i = self.spin_limit;
        while write_ptr == read_ptr {
            if i == 0 {
                owned.write_ptr_contended().store(1, Ordering::Release);
                write_ptr = readonly.write_ptr().load(Ordering::Acquire);
                if write_ptr == read_ptr {
                    if let Err(e) = readonly
                        .write_ptr()
                        .wait(write_ptr, self.timeout.as_ref())
                    {
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
        let read_ptr_usize = read_ptr as usize;
        let buffer = readonly.buffer();
        if read_ptr_usize + LEN <= N {
            // LEN is a compile-time constant: LLVM emits an inline register-width copy.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buffer.as_ptr().add(read_ptr_usize),
                    buf.as_mut_ptr(),
                    LEN,
                );
            }
        } else {
            let first_part_len = N - read_ptr_usize;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buffer.as_ptr().add(read_ptr_usize),
                    buf.as_mut_ptr(),
                    first_part_len,
                );
                std::ptr::copy_nonoverlapping(
                    buffer.as_ptr(),
                    buf.as_mut_ptr().add(first_part_len),
                    LEN - first_part_len,
                );
            }
        }
        let new_read_ptr = ((read_ptr_usize + LEN) % N) as u32;
        owned.read_ptr().store(new_read_ptr, Ordering::Release);
        if readonly.read_ptr_contended().load(Ordering::Acquire) != 0 {
            owned.read_ptr().wake_one()?;
        }
        Ok(())
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
        // Cache the two pointers in locals so LLVM can keep them in registers
        // across the spin loop rather than reloading through &mut self every iteration.
        let owned = self.owned;
        let readonly = self.readonly;
        let mut read_ptr = owned.read_ptr().load(Ordering::Acquire);
        let mut write_ptr = readonly.write_ptr().load(Ordering::Acquire);
        let mut i = self.spin_limit;
        // Spin until write_ptr != read_ptr (buffer non-empty).
        // read_ptr is owned by this consumer and does not change during the spin.
        while write_ptr == read_ptr {
            if i == 0 {
                owned.write_ptr_contended().store(1, Ordering::Release);
                write_ptr = readonly.write_ptr().load(Ordering::Acquire);
                if write_ptr == read_ptr {
                    if let Err(e) = readonly
                        .write_ptr()
                        .wait(write_ptr, self.timeout.as_ref())
                    {
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

        if readonly.read_ptr_contended().load(Ordering::Acquire) != 0 {
            owned.read_ptr().wake_one()?;
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

    /// Write exactly `LEN` bytes, spinning until space is available.
    ///
    /// Unlike `io::Write::write`, the size is a compile-time constant so LLVM
    /// inlines the memory copy as a direct register-width load/store instead
    /// of an indirect `memcpy` call.
    pub fn write_fixed<const LEN: usize>(&mut self, buf: &[u8; LEN]) -> io::Result<()> {
        let owned = self.owned;
        let readonly = self.readonly;
        let write_ptr = owned.write_ptr().load(Ordering::Acquire);
        let mut read_ptr = readonly.read_ptr().load(Ordering::Acquire);
        let mut i = self.spin_limit;
        while Self::empty_space(read_ptr, write_ptr) < LEN {
            if i == 0 {
                owned.read_ptr_contended().store(1, Ordering::Release);
                read_ptr = readonly.read_ptr().load(Ordering::Acquire);
                if Self::empty_space(read_ptr, write_ptr) < LEN {
                    if let Err(e) = readonly
                        .read_ptr()
                        .wait(read_ptr, self.timeout.as_ref())
                    {
                        owned.read_ptr_contended().store(0, Ordering::Release);
                        return Err(e);
                    }
                    read_ptr = readonly.read_ptr().load(Ordering::Acquire);
                }
            } else {
                i -= 1;
                hint::spin_loop();
                read_ptr = readonly.read_ptr().load(Ordering::Acquire);
            }
        }
        if i == 0 {
            owned.read_ptr_contended().store(0, Ordering::Release);
        }
        let write_ptr_usize = write_ptr as usize;
        let buffer = owned.buffer_mut();
        if write_ptr_usize + LEN <= N {
            // LEN is a compile-time constant: LLVM emits an inline register-width copy.
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buf.as_ptr(),
                    buffer.as_mut_ptr().add(write_ptr_usize),
                    LEN,
                );
            }
        } else {
            let first_part_len = N - write_ptr_usize;
            unsafe {
                std::ptr::copy_nonoverlapping(
                    buf.as_ptr(),
                    buffer.as_mut_ptr().add(write_ptr_usize),
                    first_part_len,
                );
                std::ptr::copy_nonoverlapping(
                    buf.as_ptr().add(first_part_len),
                    buffer.as_mut_ptr(),
                    LEN - first_part_len,
                );
            }
        }
        let new_write_ptr = ((write_ptr_usize + LEN) % N) as u32;
        owned.write_ptr().store(new_write_ptr, Ordering::Release);
        if readonly.write_ptr_contended().load(Ordering::Acquire) != 0 {
            owned.write_ptr().wake_one()?;
        }
        Ok(())
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
        // Cache the two pointers in locals so LLVM can keep them in registers
        // across the spin loop rather than reloading through &mut self every iteration.
        let owned = self.owned;
        let readonly = self.readonly;
        let write_ptr = owned.write_ptr().load(Ordering::Acquire);
        let mut read_ptr = readonly.read_ptr().load(Ordering::Acquire);
        let mut bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
        let mut i = self.spin_limit;
        // Spin until the consumer frees enough space.
        // write_ptr is owned by this producer and does not change during the spin.
        while bytes_written == 0 {
            if i == 0 {
                owned.read_ptr_contended().store(1, Ordering::Release);
                read_ptr = readonly.read_ptr().load(Ordering::Acquire);
                bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
                if bytes_written == 0 {
                    if let Err(e) = readonly
                        .read_ptr()
                        .wait(read_ptr, self.timeout.as_ref())
                    {
                        owned.read_ptr_contended().store(0, Ordering::Release);
                        return Err(e);
                    }
                    read_ptr = readonly.read_ptr().load(Ordering::Acquire);
                    bytes_written = cmp::min(Self::empty_space(read_ptr, write_ptr), buf.len());
                }
            } else {
                i -= 1;
                hint::spin_loop();
                read_ptr = readonly.read_ptr().load(Ordering::Acquire);
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

        if readonly.write_ptr_contended().load(Ordering::Acquire) != 0 {
            owned.write_ptr().wake_one()?;
        }

        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
