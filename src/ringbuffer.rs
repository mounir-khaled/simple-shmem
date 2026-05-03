use std::{
    cell::UnsafeCell,
    cmp, hint, io,
    sync::atomic::{AtomicU32, Ordering},
};

const SPIN_LIMIT: u32 = 1_000;

fn futex_wait(futex: &AtomicU32, expected: u32) -> io::Result<()> {
    loop {
        let status = unsafe {
            libc::syscall(
                libc::SYS_futex,
                futex as *const AtomicU32,
                libc::FUTEX_WAIT,
                expected,
                std::ptr::null::<libc::timespec>(),
            )
        };

        if status == -1 {
            let e = io::Error::last_os_error();
            match e.kind() {
                io::ErrorKind::Interrupted => {}
                io::ErrorKind::WouldBlock => return Ok(()), // WouldBlock aliases EAGAIN, which means the value at the futex address did not match the expected value, so we should just return and let the caller try again
                _ => return Err(e),
            }
        } else {
            return Ok(());
        }
    }
}

fn futex_wake(futex: &AtomicU32) -> io::Result<()> {
    // SAFETY: This is safe because we are only reading from the futex and not modifying it.
    let status = unsafe {
        libc::syscall(
            libc::SYS_futex,
            futex as *const AtomicU32,
            libc::FUTEX_WAKE,
            1,
            std::ptr::null::<libc::timespec>(),
        )
    };

    if status == -1 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

#[repr(C)]
pub struct RingBuffer<const N: usize> {
    read_ptr: AtomicU32,
    write_ptr: AtomicU32,
    read_ptr_contended: AtomicU32,
    write_ptr_contended: AtomicU32,
    buffer: UnsafeCell<[u8; N]>,
}

impl<const N: usize> RingBuffer<N> {
    pub fn new(read_ptr: u32, write_ptr: u32, buffer: [u8; N]) -> Self {
        assert!(N > 2, "Ring buffer size must be greater than 2");
        assert!(read_ptr < N as u32, "Head index out of bounds");
        assert!(write_ptr < N as u32, "Tail index out of bounds");

        Self {
            read_ptr: AtomicU32::new(read_ptr),
            write_ptr: AtomicU32::new(write_ptr),
            read_ptr_contended: AtomicU32::new(0),
            write_ptr_contended: AtomicU32::new(0),
            buffer: UnsafeCell::new(buffer),
        }
    }

    pub fn initialize(&mut self) {
        self.read_ptr.store(0, std::sync::atomic::Ordering::SeqCst);
        self.write_ptr.store(0, std::sync::atomic::Ordering::SeqCst);
        self.read_ptr_contended
            .store(0, std::sync::atomic::Ordering::SeqCst);
        self.write_ptr_contended
            .store(0, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn capacity(&self) -> usize {
        N
    }

    fn len(&self, read_ptr: u32, write_ptr: u32) -> usize {
        if write_ptr >= read_ptr {
            (write_ptr - read_ptr) as usize
        } else {
            (N as u32 - read_ptr + write_ptr) as usize
        }
    }

    fn empty_space(&self, read_ptr: u32, write_ptr: u32) -> usize {
        N - 1 - self.len(read_ptr, write_ptr)
    }
}

impl<const N: usize> Default for RingBuffer<N> {
    fn default() -> Self {
        Self {
            read_ptr: AtomicU32::new(0),
            write_ptr: AtomicU32::new(0),
            read_ptr_contended: AtomicU32::new(0),
            write_ptr_contended: AtomicU32::new(0),
            buffer: UnsafeCell::new([0; N]),
        }
    }
}

impl<const N: usize> io::Read for RingBuffer<N> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut read_ptr = self.read_ptr.load(Ordering::Acquire);
        let mut write_ptr = self.write_ptr.load(Ordering::Acquire);
        let mut bytes_read = cmp::min(self.len(read_ptr, write_ptr), buf.len());
        let mut i = SPIN_LIMIT;
        while bytes_read == 0 {
            // spin for a while before sleeping to reduce latency in the case of short waits
            if i == 0 {
                self.write_ptr_contended.store(1, Ordering::SeqCst);
                // Re-check write_ptr AFTER the SeqCst store. If the writer updated
                // write_ptr and then read the flag (seeing 0) before our store, we will
                // see the new write_ptr here and skip the sleep, preventing a missed wake.
                write_ptr = self.write_ptr.load(Ordering::Acquire);
                bytes_read = cmp::min(self.len(read_ptr, write_ptr), buf.len());
                if bytes_read == 0 {
                    futex_wait(&self.write_ptr, write_ptr)?;
                }
            } else {
                i -= 1;
                hint::spin_loop();
            }

            read_ptr = self.read_ptr.load(Ordering::Acquire);
            write_ptr = self.write_ptr.load(Ordering::Acquire);
            bytes_read = cmp::min(self.len(read_ptr, write_ptr), buf.len());
        }

        if i == 0 {
            self.write_ptr_contended.store(0, Ordering::SeqCst);
        }

        let read_ptr_usize = read_ptr as usize;
        if read_ptr_usize + bytes_read <= N {
            buf[..bytes_read].copy_from_slice(
                &unsafe { &*self.buffer.get() }[read_ptr_usize..(read_ptr_usize + bytes_read)],
            );
        } else {
            let first_part_len = N - read_ptr_usize;
            buf[..first_part_len]
                .copy_from_slice(&unsafe { &*self.buffer.get() }[read_ptr_usize..]);
            buf[first_part_len..bytes_read]
                .copy_from_slice(&unsafe { &*self.buffer.get() }[..(bytes_read - first_part_len)]);
        }

        read_ptr = ((read_ptr_usize + bytes_read) % N) as u32;
        // SeqCst prevents the store from being reordered after the flag load below.
        // Without this, the waker could read the flag before the pointer update is visible.
        self.read_ptr.store(read_ptr, Ordering::SeqCst);

        if self.read_ptr_contended.load(Ordering::Acquire) != 0 {
            futex_wake(&self.read_ptr)?;
        }

        Ok(bytes_read)
    }
}

impl<const N: usize> io::Write for RingBuffer<N> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut write_ptr = self.write_ptr.load(Ordering::Acquire);
        let mut read_ptr = self.read_ptr.load(Ordering::Acquire);
        let mut bytes_written = cmp::min(self.empty_space(read_ptr, write_ptr), buf.len());
        let mut i = SPIN_LIMIT;
        while bytes_written == 0 {
            // spin for a while before sleeping to reduce latency in the case of short waits
            if i == 0 {
                self.read_ptr_contended.store(1, Ordering::SeqCst);
                // Re-check read_ptr AFTER the SeqCst store. If the reader advanced
                // read_ptr and then read the flag (seeing 0) before our store, we will
                // see the new read_ptr here and skip the sleep, preventing a missed wake.
                read_ptr = self.read_ptr.load(Ordering::Acquire);
                bytes_written = cmp::min(self.empty_space(read_ptr, write_ptr), buf.len());
                if bytes_written == 0 {
                    futex_wait(&self.read_ptr, read_ptr)?;
                }
            } else {
                i -= 1;
                hint::spin_loop();
            }

            write_ptr = self.write_ptr.load(Ordering::Acquire);
            read_ptr = self.read_ptr.load(Ordering::Acquire);
            bytes_written = cmp::min(self.empty_space(read_ptr, write_ptr), buf.len());
        }

        if i == 0 {
            self.read_ptr_contended.store(0, Ordering::Release);
        }

        let write_ptr_usize = write_ptr as usize;
        if write_ptr_usize + bytes_written <= N {
            unsafe {
                (&mut *self.buffer.get())[write_ptr_usize..(write_ptr_usize + bytes_written)]
                    .copy_from_slice(&buf[..bytes_written]);
            }
        } else {
            let first_part_len = N - write_ptr_usize;
            unsafe {
                (&mut *self.buffer.get())[write_ptr_usize..]
                    .copy_from_slice(&buf[..first_part_len]);

                (&mut *self.buffer.get())[..(bytes_written - first_part_len)]
                    .copy_from_slice(&buf[first_part_len..bytes_written]);
            }
        }

        write_ptr = ((write_ptr_usize + bytes_written) % N) as u32;
        // SeqCst prevents the store from being reordered after the flag load below.
        // Without this, the waker could read the flag before the pointer update is visible.
        self.write_ptr.store(write_ptr, Ordering::SeqCst);

        if self.write_ptr_contended.load(Ordering::Acquire) != 0 {
            futex_wake(&self.write_ptr)?;
        }

        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
