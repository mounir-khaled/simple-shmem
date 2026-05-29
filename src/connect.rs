use std::{
    fs::File,
    io,
    os::{
        fd::{AsRawFd, FromRawFd, RawFd},
        unix::net::UnixStream,
    },
    pin::Pin,
    ptr,
};

use crate::{
    DualRingBuffers,
    producer_consumer::{Consumer, Producer},
    ringbuffer_parts::{ConsumerOwned, Mmapped, MmappedMut, ProducerOwned},
};

struct MessageHeader<'m, 'c> {
    _cmsg_phantom: std::marker::PhantomData<&'c ()>,
    _msg_phantom: std::marker::PhantomData<&'m ()>,
    iovec: Pin<Box<libc::iovec>>,
    msghdr: libc::msghdr,
}

impl<'m, 'c> MessageHeader<'m, 'c> {
    fn new(msg_buffer: &'m mut [u8], cmsg_buffer: &'c mut [u8]) -> Self {
        let mut iovec = libc::iovec {
            iov_base: std::ptr::from_mut(msg_buffer) as *mut libc::c_void,
            iov_len: msg_buffer.len(),
        };

        let msghdr = libc::msghdr {
            msg_name: std::ptr::null_mut(),
            msg_namelen: 0,
            msg_iov: ptr::from_mut(&mut iovec),
            msg_iovlen: 1,
            msg_control: cmsg_buffer.as_mut_ptr() as *mut libc::c_void,
            msg_controllen: cmsg_buffer.len() as _,
            msg_flags: 0,
        };

        let mut new_self = Self {
            _cmsg_phantom: std::marker::PhantomData,
            _msg_phantom: std::marker::PhantomData,
            iovec: Box::pin(iovec),
            msghdr,
        };

        new_self.msghdr.msg_iov = ptr::from_mut(&mut new_self.iovec);
        new_self
    }
}

fn fd_cmsg(fd: RawFd) -> Vec<u8> {
    let fds = [fd];
    let cmsg_len = unsafe { libc::CMSG_LEN(size_of::<RawFd>() as u32) as usize };
    let cmsg_space = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) as usize };

    let mut cmsg_buffer = vec![0u8; cmsg_space];
    let cmsg_hdr = unsafe { &mut *(cmsg_buffer.as_mut_ptr() as *mut libc::cmsghdr) };
    cmsg_hdr.cmsg_level = libc::SOL_SOCKET;
    cmsg_hdr.cmsg_type = libc::SCM_RIGHTS;
    cmsg_hdr.cmsg_len = cmsg_len;

    unsafe {
        std::ptr::copy_nonoverlapping(
            fds.as_ptr() as *const u8,
            libc::CMSG_DATA(cmsg_hdr) as *mut u8,
            size_of::<RawFd>(),
        );
    }

    cmsg_buffer
}

fn parse_fd_cmsg(fd_hdr: &mut libc::cmsghdr) -> io::Result<File> {
    if fd_hdr.cmsg_level != libc::SOL_SOCKET || fd_hdr.cmsg_type != libc::SCM_RIGHTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unexpected file descriptor control message type",
        ));
    }

    let expected_len = unsafe { libc::CMSG_LEN(size_of::<RawFd>() as u32) };
    if fd_hdr.cmsg_len != expected_len as usize {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unexpected control message length for file descriptor",
        ));
    }

    let fd_data = unsafe { libc::CMSG_DATA(fd_hdr) };
    let fd = unsafe { std::ptr::read_unaligned(fd_data as *const RawFd) };
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn memfd_create() -> io::Result<File> {
    let name = std::ffi::CString::new("").unwrap();
    let memfd = unsafe { libc::memfd_create(name.as_ptr(), libc::MFD_ALLOW_SEALING) };
    if memfd == -1 {
        return Err(io::Error::last_os_error());
    }

    Ok(unsafe { File::from_raw_fd(memfd) })
}

fn add_seals(file: &File, seals: libc::c_int) -> io::Result<()> {
    let result = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_ADD_SEALS, seals) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

fn recv_shared_memfd(stream: &UnixStream, file_len: u64) -> io::Result<File> {
    let mut msg_buffer = [0u8; 1];
    let cmsg_buffer_size = unsafe { libc::CMSG_SPACE(size_of::<RawFd>() as u32) as usize };
    let mut cmsg_buffer = vec![0u8; cmsg_buffer_size];
    let mut msghdr = MessageHeader::new(&mut msg_buffer, cmsg_buffer.as_mut_slice());

    let status = unsafe { libc::recvmsg(stream.as_raw_fd(), &mut msghdr.msghdr, 0) };
    if status == -1 {
        return Err(io::Error::last_os_error());
    }

    let cmsg_hdr = unsafe { libc::CMSG_FIRSTHDR(&msghdr.msghdr) };
    let cmsg_hdr = unsafe { cmsg_hdr.as_mut() };
    let Some(cmsg_hdr) = cmsg_hdr else {
        return Err(io::Error::from(io::ErrorKind::NotFound));
    };

    let shared_memfd = parse_fd_cmsg(cmsg_hdr)?;
    add_seals(&shared_memfd, libc::F_SEAL_SHRINK)?;
    shared_memfd.set_len(file_len)?;

    Ok(shared_memfd)
}

fn send_owned_memfd(stream: &UnixStream, file: &File) -> io::Result<()> {
    add_seals(&file, libc::F_SEAL_FUTURE_WRITE)?;

    let mut cmsg_buffer = fd_cmsg(file.as_raw_fd());

    let mut msg_buffer = [0u8; 1];
    let msghdr = MessageHeader::new(&mut msg_buffer, cmsg_buffer.as_mut_slice());
    let status = unsafe { libc::sendmsg(stream.as_raw_fd(), &msghdr.msghdr, 0) };
    if status == -1 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

impl<const N: usize> DualRingBuffers<N> {
    pub fn accept(stream: &UnixStream) -> io::Result<Self> {
        let page_aligned_buffer_size = ProducerOwned::<N>::page_aligned_size();

        let shared_memfd = recv_shared_memfd(stream, page_aligned_buffer_size as u64 * 2)?;
        let shared_consumer_owned = Mmapped::<ConsumerOwned<N>>::new(&shared_memfd, 0)?;
        let shared_producer_owned =
            Mmapped::<ProducerOwned<N>>::new(&shared_memfd, page_aligned_buffer_size)?;

        let my_memfd = memfd_create()?;
        my_memfd.set_len(page_aligned_buffer_size as u64 * 2)?;
        let my_consumer_owned = MmappedMut::<ConsumerOwned<N>>::new(&my_memfd, 0)?;
        let my_producer_owned =
            MmappedMut::<ProducerOwned<N>>::new(&my_memfd, page_aligned_buffer_size)?;
        send_owned_memfd(stream, &my_memfd)?;

        let consumer = unsafe { Consumer::new(my_consumer_owned, shared_producer_owned) };
        let producer = unsafe { Producer::new(my_producer_owned, shared_consumer_owned) };
        Ok(Self::new(consumer, producer))
    }

    pub fn connect(stream: &UnixStream) -> io::Result<Self> {
        let page_aligned_buffer_size = ProducerOwned::<N>::page_aligned_size();
        let my_memfd = memfd_create()?;
        my_memfd.set_len(page_aligned_buffer_size as u64 * 2)?;

        let my_consumer_owned = MmappedMut::<ConsumerOwned<N>>::new(&my_memfd, 0)?;
        let my_producer_owned =
            MmappedMut::<ProducerOwned<N>>::new(&my_memfd, page_aligned_buffer_size)?;

        send_owned_memfd(stream, &my_memfd)?;

        let shared_memfd = recv_shared_memfd(stream, page_aligned_buffer_size as u64 * 2)?;

        let shared_consumer_owned = Mmapped::<ConsumerOwned<N>>::new(&shared_memfd, 0)?;
        let shared_producer_owned =
            Mmapped::<ProducerOwned<N>>::new(&shared_memfd, page_aligned_buffer_size)?;

        let consumer = unsafe { Consumer::new(my_consumer_owned, shared_producer_owned) };
        let producer = unsafe { Producer::new(my_producer_owned, shared_consumer_owned) };
        Ok(Self::new(consumer, producer))
    }
}
