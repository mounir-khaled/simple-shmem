use std::{io, ptr::NonNull};

use crate::{futex::Futex, ringbuffer::RingBuffer};

pub struct MmappedRingBuffer<const N: usize> {
    rb: NonNull<Futex<RingBuffer<N>>>,
}

impl<const N: usize> MmappedRingBuffer<N> {
    pub fn new(fd: i32, offset: libc::off_t) -> io::Result<Self> {
        let size = Self::object_size();
        let prot = libc::PROT_READ | libc::PROT_WRITE;
        let addr = std::ptr::null_mut();
        let mem = unsafe { libc::mmap(addr, size, prot, libc::MAP_SHARED, fd, offset) };

        if mem == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }

        let ringbuffer_futex = NonNull::new(mem as *mut Futex<RingBuffer<N>>).ok_or_else(|| {
            unsafe { libc::munmap(mem, size) };
            io::Error::new(io::ErrorKind::Other, "Failed to create NonNull pointer")
        })?;

        Ok(Self {
            rb: ringbuffer_futex,
        })
    }

    pub fn initialize(&mut self) {
        unsafe { *self.rb.as_ptr() = Futex::new(RingBuffer::default()) };
    }

    pub const fn object_size() -> usize {
        size_of::<Futex<RingBuffer<N>>>()
    }
}

impl<const N: usize> io::Read for MmappedRingBuffer<N> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let futex = unsafe { self.rb.as_mut() };
        let mut guard = futex.lock()?;
        guard.read(buf)
    }
}

impl<const N: usize> io::Write for MmappedRingBuffer<N> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let futex = unsafe { self.rb.as_mut() };
        let mut guard = futex.lock()?;
        guard.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<const N: usize> Drop for MmappedRingBuffer<N> {
    fn drop(&mut self) {
        let size = Self::object_size();
        let status = unsafe { libc::munmap(self.rb.as_ptr() as *mut libc::c_void, size) };
        if status == -1 {
            let err = unsafe { *libc::__errno_location() };
            panic!("Failed to unmap memory: {}", err);
        }
    }
}
