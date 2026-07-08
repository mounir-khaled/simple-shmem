use std::{
    cell::UnsafeCell,
    fs::File,
    io, mem,
    ops::{Deref, DerefMut},
    os::fd::AsRawFd,
    ptr,
    sync::atomic::AtomicU32,
};

use crate::{futex::Futex, page_size::page_size};

unsafe fn mmap<T>(file: &File, offset: isize, prot: i32) -> io::Result<&'static mut T> {
    let size = mem::size_of::<T>();
    let addr = ptr::null_mut();
    let offset = libc::off_t::try_from(offset)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "offset out of range"))?;

    let mem = unsafe { libc::mmap(addr, size, prot, libc::MAP_SHARED, file.as_raw_fd(), offset) };

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

pub struct MmappedMut<T> {
    ptr: *mut T,
}

pub struct Mmapped<T> {
    ptr: *const T,
}

impl<T: 'static> MmappedMut<T> {
    pub fn new(file: &File, offset: isize) -> io::Result<Self> {
        let mmapped = unsafe { mmap(file, offset, libc::PROT_READ | libc::PROT_WRITE)? };
        Ok(Self {
            ptr: mmapped as *mut T,
        })
    }
}

impl<T> Deref for MmappedMut<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

impl<T> DerefMut for MmappedMut<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        unsafe { &mut *self.ptr }
    }
}

impl<T> Drop for MmappedMut<T> {
    fn drop(&mut self) {
        let size = mem::size_of::<T>();
        if unsafe { libc::munmap(self.ptr as *mut libc::c_void, size) } != 0 {
            eprintln!("Failed to unmap memory: {}", io::Error::last_os_error());
        }
    }
}

impl<T: 'static> Mmapped<T> {
    pub fn new(file: &File, offset: isize) -> io::Result<Self> {
        let mmapped = unsafe { mmap(file, offset, libc::PROT_READ)? };
        Ok(Self {
            ptr: mmapped as *const T,
        })
    }
}

impl<T> Deref for Mmapped<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        unsafe { &*self.ptr }
    }
}

impl<T> Drop for Mmapped<T> {
    fn drop(&mut self) {
        let size = mem::size_of::<T>();
        if unsafe { libc::munmap(self.ptr as *mut libc::c_void, size) } != 0 {
            eprintln!("Failed to unmap memory: {}", io::Error::last_os_error());
        }
    }
}

/// Ring buffer part that is owned by the producer.
/// This gets mmapped into the producer as read-write
/// and into the consumer as read-only
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
