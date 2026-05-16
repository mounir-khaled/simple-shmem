use std::{
    cell::UnsafeCell,
    io, mem,
    os::fd::{AsRawFd, BorrowedFd},
    ptr,
    sync::atomic::AtomicU32,
};

use crate::{futex::Futex, page_size::page_size};

unsafe fn mmap<T>(fd: BorrowedFd, offset: isize, prot: i32) -> io::Result<&'static mut T> {
    let size = mem::size_of::<T>();
    let addr = ptr::null_mut();
    let offset = libc::off_t::try_from(offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset out of range"))?;

    let mem = unsafe { libc::mmap(addr, size, prot, libc::MAP_SHARED, fd.as_raw_fd(), offset) };

    if mem == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }

    Ok(unsafe { &mut *(mem as *mut T) })
}

pub unsafe fn munmap<T>(ptr: *const T) -> io::Result<()> {
    let size = mem::size_of::<T>();
    if unsafe { libc::munmap(ptr as *mut libc::c_void, size) } != 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

/// Ring buffer part that is owned by the producer.
/// This gets mmapped into the producer as read-write
/// and into the consumer as read-only
///
/// Layout: control fields occupy exactly one 64-byte cache line, and the
/// buffer starts on the next cache line.  This prevents buffer writes from
/// invalidating the cache line that the consumer spins on (write_ptr).
#[repr(C)]
pub struct ProducerOwned<const N: usize> {
    write_ptr: Futex,
    read_ptr_contended: AtomicU32,
    buffer: UnsafeCell<[u8; N]>,
}

/// Ring buffer part that is owned by the consumer
/// This gets mmapped into the consumer as read-write
/// and into the producer as read-only
#[repr(C)]
pub struct ConsumerOwned<const N: usize> {
    read_ptr: Futex,
    write_ptr_contended: AtomicU32,
    buffer: UnsafeCell<[u8; N]>,
}

impl<const N: usize> ConsumerOwned<N> {
    pub fn mmap_rw(fd: BorrowedFd, offset: isize) -> io::Result<&'static mut Self> {
        unsafe { mmap(fd, offset, libc::PROT_READ | libc::PROT_WRITE) }
    }

    pub fn mmap_ro(fd: BorrowedFd, offset: isize) -> io::Result<&'static mut Self> {
        unsafe { mmap(fd, offset, libc::PROT_READ) }
    }

    pub fn read_ptr(&self) -> &Futex {
        &self.read_ptr
    }

    pub fn write_ptr_contended(&self) -> &AtomicU32 {
        &self.write_ptr_contended
    }

    pub fn buffer(&self) -> &[u8; N] {
        unsafe { &*self.buffer.get() }
    }

    pub fn page_aligned_size() -> isize {
        let page_size = page_size() as isize;
        page_size * ((mem::size_of::<Self>() as isize + page_size - 1) / page_size)
    }
}

impl<const N: usize> ProducerOwned<N> {
    pub fn mmap_rw(fd: BorrowedFd, offset: isize) -> io::Result<&'static mut Self> {
        unsafe { mmap(fd, offset, libc::PROT_READ | libc::PROT_WRITE) }
    }

    pub fn mmap_ro(fd: BorrowedFd, offset: isize) -> io::Result<&'static mut Self> {
        unsafe { mmap(fd, offset, libc::PROT_READ) }
    }

    pub fn write_ptr(&self) -> &Futex {
        &self.write_ptr
    }

    pub fn read_ptr_contended(&self) -> &AtomicU32 {
        &self.read_ptr_contended
    }

    pub fn buffer(&self) -> &[u8; N] {
        unsafe { &*self.buffer.get() }
    }

    pub fn buffer_mut(&self) -> &mut [u8; N] {
        unsafe { &mut *self.buffer.get() }
    }

    pub fn page_aligned_size() -> isize {
        let page_size = page_size() as isize;
        page_size * ((mem::size_of::<Self>() as isize + page_size - 1) / page_size)
    }
}
