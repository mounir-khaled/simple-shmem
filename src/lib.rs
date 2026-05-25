#![allow(dead_code)]

mod dual_ringbuffer;
mod futex;
mod page_size;
mod producer_consumer;
mod ringbuffer_parts;
mod umask_context;

pub mod listener;

use std::{
    fs::Metadata,
    io,
    os::{
        fd::{AsRawFd, RawFd},
        unix::net::UnixDatagram,
    },
};

use thiserror::Error;

pub use crate::dual_ringbuffer::{DualRingBuffers, DualRingBuffersError};

// exactly one 4096-byte page
pub type StdDualRingBuffers = DualRingBuffers<4088>;

// 8-byte header + 32-byte buffer = 40 bytes, fits in one 64-byte cache line.
// write_ptr, read_ptr_contended, and the entire buffer share one cache line,
// so a single L3 transfer delivers both the "data ready" signal and the payload.
// N=32 (power of 2) allows the ring pointer modulo to compile to a single AND
// instruction instead of a multiply-shift sequence.
pub type FastDualRingBuffers = DualRingBuffers<32>;

#[derive(Error, Debug)]
pub enum ConnectionError {
    #[error("Ring buffer error: {0}")]
    RingBufferError(DualRingBuffersError),
    #[error("I/O error: {0}")]
    Io(io::Error),
    #[error("Peer rejected: {0:?}")]
    PeerRejected(Metadata),
    #[error("Client error")]
    ClientError,
}

fn parse_ucred_cmsg(cred_hdr: &mut libc::cmsghdr) -> io::Result<libc::ucred> {
    if cred_hdr.cmsg_type != libc::SCM_CREDENTIALS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "Unexpected ucred control message type {}",
                cred_hdr.cmsg_type
            ),
        ));
    }

    if cred_hdr.cmsg_len as usize != size_of::<libc::cmsghdr>() + size_of::<libc::ucred>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unexpected control message length",
        ));
    }

    let ucred_data = unsafe { libc::CMSG_DATA(cred_hdr) };
    let ucred = unsafe { std::ptr::read_unaligned(ucred_data as *const libc::ucred) };
    Ok(ucred)
}

fn parse_fd_cmsg(fd_hdr: &mut libc::cmsghdr) -> io::Result<RawFd> {
    if fd_hdr.cmsg_level != libc::SOL_SOCKET || fd_hdr.cmsg_type != libc::SCM_RIGHTS {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unexpected file descriptor control message type",
        ));
    }

    if fd_hdr.cmsg_len as usize != size_of::<libc::cmsghdr>() + size_of::<RawFd>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Unexpected control message length for file descriptor",
        ));
    }

    let fd_data = unsafe { libc::CMSG_DATA(fd_hdr) };
    let fd = unsafe { std::ptr::read_unaligned(fd_data as *const RawFd) };
    Ok(fd)
}

fn send_file_descriptor(
    uds: &mut UnixDatagram,
    dest_addr: &libc::sockaddr_un,
    fd: RawFd,
) -> io::Result<()> {
    let fds = [fd];
    let cmsg_len = unsafe { libc::CMSG_LEN(size_of::<RawFd>() as u32) as usize };

    let mut cmsg_buffer = vec![0u8; cmsg_len];
    let cmsg_hdr = unsafe { &mut *(cmsg_buffer.as_mut_ptr() as *mut libc::cmsghdr) };
    cmsg_hdr.cmsg_level = libc::SOL_SOCKET;
    cmsg_hdr.cmsg_type = libc::SCM_RIGHTS;
    cmsg_hdr.cmsg_len = unsafe { libc::CMSG_LEN(size_of::<RawFd>() as u32) as usize };

    unsafe {
        std::ptr::copy_nonoverlapping(
            fds.as_ptr() as *const u8,
            libc::CMSG_DATA(cmsg_hdr) as *mut u8,
            size_of::<RawFd>(),
        );
    }

    let mut buf = [0u8; 1];

    let iovec = libc::iovec {
        iov_base: std::ptr::from_mut(&mut buf) as *mut libc::c_void,
        iov_len: 1,
    };

    let msghdr = libc::msghdr {
        msg_name: dest_addr as *const _ as *mut libc::c_void,
        msg_namelen: std::mem::size_of_val(dest_addr) as _,
        msg_iov: &iovec as *const libc::iovec as *mut libc::iovec,
        msg_iovlen: 1,
        msg_control: cmsg_buffer.as_mut_ptr() as *mut libc::c_void,
        msg_controllen: cmsg_buffer.len() as _,
        msg_flags: 0,
    };

    let uds_fd = uds.as_raw_fd();
    let result = unsafe { libc::sendmsg(uds_fd, &msghdr, 0) };
    if result == -1 {
        return Err(io::Error::last_os_error());
    }

    Ok(())
}

fn sockaddr_un_from_str(path: &str) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
    let path_bytes = path.as_bytes();
    if path_bytes.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "paths must not contain interior null bytes",
        ));
    }

    if path_bytes.len() >= 108 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "path must be shorter than 108 bytes",
        ));
    }

    let mut sockaddr = libc::sockaddr_un {
        sun_family: libc::AF_UNIX as _,
        sun_path: [0; 108],
    };

    for (i, &b) in path_bytes.iter().enumerate() {
        sockaddr.sun_path[i] = b as i8;
    }

    Ok((
        sockaddr,
        std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
    ))
}
