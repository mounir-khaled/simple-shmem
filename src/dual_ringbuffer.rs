use std::{
    fs::{File, OpenOptions},
    io,
    os::{
        fd::{AsFd, AsRawFd, FromRawFd},
        raw::c_void,
        unix::{fs::OpenOptionsExt, net::UnixDatagram},
    },
    path::Path,
    ptr,
    time::Duration,
};

use thiserror::Error;

use crate::{
    parse_fd_cmsg, parse_ucred_cmsg,
    producer_consumer::{Consumer, ConsumerProducerError, Producer},
    ringbuffer_parts::ProducerOwned,
    send_file_descriptor, sockaddr_un_from_str,
};

// TODO: implement Drop and delete owned file
pub struct DualRingBuffers<const N: usize> {
    consumer: Consumer<N>,
    producer: Producer<N>,
}

enum FileOrder {
    ConsumerFirst,
    ProducerFirst,
}

#[derive(Error, Debug)]
pub enum DualRingBuffersError {
    #[error("Consumer error: {0}")]
    Consumer(ConsumerProducerError),
    #[error("Producer error: {0}")]
    Producer(ConsumerProducerError),
    #[error("Owned file error: {0}")]
    OwnedFile(io::Error),
    #[error("Shared file error: {0}")]
    SharedFile(io::Error),
    #[error("Directory error: {0}")]
    Dir(io::Error),
}

impl<const N: usize> DualRingBuffers<N> {
    pub fn connect<A: FnOnce(u32, u32) -> bool>(
        uds: &mut UnixDatagram,
        server_name: &str,
        accept_fn: A,
    ) -> Result<Self, DualRingBuffersError> {
        const TRUE: i32 = 1;
        let status_isize;
        let status_int;

        {
            let uds_fd = uds.as_fd();
            status_int = unsafe {
                libc::setsockopt(
                    uds_fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_PASSCRED,
                    ptr::from_ref(&TRUE) as *const libc::c_void,
                    std::mem::size_of::<libc::c_int>() as _,
                )
            };
        }

        if status_int == -1 {
            return Err(DualRingBuffersError::OwnedFile(io::Error::last_os_error()));
        }

        let (owned_file, shared) = Self::open_owned_and_shared("/dev/shm/")?;

        let (server_addr, _) =
            sockaddr_un_from_str(server_name).map_err(DualRingBuffersError::SharedFile)?;

        send_file_descriptor(uds, &server_addr, shared.as_raw_fd())
            .map_err(DualRingBuffersError::OwnedFile)?;

        let mut cmsg_buffer = [0u8; 4096];

        let mut buf = [0u8; 1];

        let iovec = libc::iovec {
            iov_base: ptr::from_mut(&mut buf) as *mut c_void,
            iov_len: 1,
        };

        let mut msghdr = libc::msghdr {
            msg_name: ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: &iovec as *const libc::iovec as *mut libc::iovec,
            msg_iovlen: 1,
            msg_control: ptr::from_mut(&mut cmsg_buffer) as *mut libc::c_void,
            msg_controllen: cmsg_buffer.len() as _,
            msg_flags: 0,
        };

        let uds_fd = uds.as_fd();
        status_isize = unsafe { libc::recvmsg(uds_fd.as_raw_fd(), &mut msghdr, 0) };
        if status_isize == -1 {
            return Err(DualRingBuffersError::SharedFile(io::Error::last_os_error()));
        }

        let ucred_hdr = unsafe { libc::CMSG_FIRSTHDR(&msghdr).as_mut() };
        let Some(ucred_hdr) = ucred_hdr else {
            return Err(DualRingBuffersError::SharedFile(io::Error::new(
                io::ErrorKind::InvalidData,
                "No control message received",
            )));
        };

        let ucred = parse_ucred_cmsg(ucred_hdr).map_err(DualRingBuffersError::SharedFile)?;
        if !accept_fn(ucred.uid, ucred.gid) {
            return Err(DualRingBuffersError::SharedFile(io::Error::from(
                io::ErrorKind::PermissionDenied,
            )));
        }

        let fd_hdr = unsafe { libc::CMSG_NXTHDR(&msghdr, ucred_hdr).as_mut() };
        let Some(fd_hdr) = fd_hdr else {
            return Err(DualRingBuffersError::SharedFile(io::Error::new(
                io::ErrorKind::InvalidData,
                "No file descriptor control message received",
            )));
        };

        let shared_fd = parse_fd_cmsg(fd_hdr).map_err(DualRingBuffersError::SharedFile)?;
        let shared_file = unsafe { File::from_raw_fd(shared_fd) };

        Self::new_producer_first(owned_file, shared_file)
    }

    pub fn new_consumer_first(
        owned_file: File,
        shared_file: File,
    ) -> Result<DualRingBuffers<N>, DualRingBuffersError> {
        Self::new(owned_file, shared_file, FileOrder::ConsumerFirst)
    }

    pub fn new_producer_first(
        owned_file: File,
        shared_file: File,
    ) -> Result<DualRingBuffers<N>, DualRingBuffersError> {
        Self::new(owned_file, shared_file, FileOrder::ProducerFirst)
    }

    pub(crate) fn open_owned_and_shared<P: AsRef<Path>>(
        dir: P,
    ) -> Result<(File, File), DualRingBuffersError> {
        let template_path = dir.as_ref().join("drb.XXXXXX");
        let template = template_path.to_str().ok_or_else(|| {
            DualRingBuffersError::Dir(io::Error::new(
                io::ErrorKind::InvalidInput,
                "Directory path is not valid UTF-8",
            ))
        })?;

        let template = std::ffi::CString::new(template)
            .map_err(|_| {
                DualRingBuffersError::Dir(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Directory path contains interior null byte",
                ))
            })?
            .into_raw();

        let owned_fd = unsafe { libc::mkstemp(template) };
        let tempfile_name = unsafe { std::ffi::CString::from_raw(template) };
        if owned_fd == -1 {
            return Err(DualRingBuffersError::OwnedFile(io::Error::last_os_error()));
        }

        let owned_file = unsafe { File::from_raw_fd(owned_fd) };
        let page_aligned_buffer_size = ProducerOwned::<N>::page_aligned_size();

        owned_file
            .set_len(page_aligned_buffer_size as u64 * 2)
            .map_err(DualRingBuffersError::OwnedFile)?;

        let tempfile_name = tempfile_name.to_str().expect("invalid UTF-8 from mkstemp");
        let shared_file = OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(tempfile_name)
            .map_err(DualRingBuffersError::SharedFile)?;

        std::fs::remove_file(tempfile_name).map_err(DualRingBuffersError::SharedFile)?;

        Ok((owned_file, shared_file))
    }

    fn new(
        owned_file: File,
        shared_file: File,
        order: FileOrder,
    ) -> Result<DualRingBuffers<N>, DualRingBuffersError> {
        let page_aligned_buffer_size = ProducerOwned::<N>::page_aligned_size();

        // Reject a truncated or malicious shared file from the peer before mapping it.
        // Accessing a mapping that extends beyond the file's end causes SIGBUS, which
        // bypasses all Rust error handling and crashes the process.
        let shared_len = shared_file
            .metadata()
            .map_err(DualRingBuffersError::SharedFile)?
            .len();
        let required_len = page_aligned_buffer_size as u64 * 2;
        if shared_len < required_len {
            return Err(DualRingBuffersError::SharedFile(io::Error::new(
                io::ErrorKind::InvalidData,
                "shared file from peer is smaller than expected",
            )));
        }

        let (consumer_offset, producer_offset) = match order {
            FileOrder::ConsumerFirst => (0, page_aligned_buffer_size),
            FileOrder::ProducerFirst => (page_aligned_buffer_size, 0),
        };

        let consumer = Consumer::new(
            owned_file.as_fd(),
            consumer_offset,
            shared_file.as_fd(),
            consumer_offset,
        )
        .map_err(DualRingBuffersError::Consumer)?;

        let producer = Producer::new(
            owned_file.as_fd(),
            producer_offset,
            shared_file.as_fd(),
            producer_offset,
        )
        .map_err(DualRingBuffersError::Producer)?;

        Ok(DualRingBuffers { consumer, producer })
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

impl<const N: usize> DualRingBuffers<N> {
    /// Read exactly `LEN` bytes with a compile-time-known size so LLVM can
    /// inline the copy as a direct load/store instead of a `memcpy` call.
    pub fn read_fixed<const LEN: usize>(&mut self, buf: &mut [u8; LEN]) -> io::Result<()> {
        self.consumer.read_fixed(buf)
    }

    /// Write exactly `LEN` bytes with a compile-time-known size so LLVM can
    /// inline the copy as a direct load/store instead of a `memcpy` call.
    pub fn write_fixed<const LEN: usize>(&mut self, buf: &[u8; LEN]) -> io::Result<()> {
        self.producer.write_fixed(buf)
    }
}
