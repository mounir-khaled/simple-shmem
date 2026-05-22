use core::{slice, str};
use std::{
    fs::File,
    intrinsics::copy_nonoverlapping,
    io,
    os::{
        fd::{AsRawFd, FromRawFd},
        unix::net::UnixDatagram,
    },
    ptr,
};

use crate::{
    DualRingBuffers, DualRingBuffersError, parse_fd_cmsg, parse_ucred_cmsg, send_file_descriptor,
};

pub struct Listener(UnixDatagram);

impl Listener {
    pub fn new(uds: UnixDatagram) -> io::Result<Self> {
        let passcred: libc::c_int = 1;
        let passcred_ptr = ptr::from_ref(&passcred) as *const libc::c_void;
        let passcred_size = std::mem::size_of_val(&passcred);
        let passcred_size =
            libc::socklen_t::try_from(passcred_size).expect("sizeof(c_int) cannot fit in a u32");

        let uds_fd = uds.as_raw_fd();
        let status = unsafe {
            libc::setsockopt(
                uds_fd,
                libc::SOL_SOCKET,
                libc::SO_PASSCRED,
                passcred_ptr,
                passcred_size,
            )
        };

        if status == -1 {
            return Err(io::Error::last_os_error());
        }

        Ok(Self(uds))
    }

    pub fn accept<A: FnOnce(u32, u32) -> bool, const N: usize>(
        &mut self,
        accept_fn: A,
    ) -> Result<DualRingBuffers<N>, DualRingBuffersError> {
        let mut cmsg_buffer = [0u8; 4096];

        let mut buf = [0u8; 1];

        let iovec = libc::iovec {
            iov_base: ptr::from_mut(&mut buf) as *mut libc::c_void,
            iov_len: 1,
        };

        let mut dest_addr = libc::sockaddr_un {
            sun_family: libc::AF_UNIX as libc::sa_family_t,
            sun_path: [0; 108],
        };

        let mut msghdr = libc::msghdr {
            msg_name: ptr::from_mut(&mut dest_addr) as *mut libc::c_void,
            msg_namelen: std::mem::size_of_val(&dest_addr) as _,
            msg_iov: &iovec as *const libc::iovec as *mut libc::iovec,
            msg_iovlen: 1,
            msg_control: ptr::from_mut(&mut cmsg_buffer) as *mut libc::c_void,
            msg_controllen: cmsg_buffer.len() as _,
            msg_flags: 0,
        };

        let status_isize = unsafe { libc::recvmsg(self.0.as_raw_fd(), &mut msghdr, 0) };
        if status_isize == -1 {
            return Err(DualRingBuffersError::SharedFile(io::Error::last_os_error()));
        }

        let ucred_hdr = unsafe { libc::CMSG_FIRSTHDR(&msghdr).as_mut() };
        let Some(ucred_hdr) = ucred_hdr else {
            return Err(DualRingBuffersError::SharedFile(io::Error::new(
                io::ErrorKind::InvalidData,
                "No ucred control message received",
            )));
        };

        let fd_hdr = unsafe { libc::CMSG_NXTHDR(&msghdr, ucred_hdr).as_mut() };
        let Some(fd_hdr) = fd_hdr else {
            return Err(DualRingBuffersError::SharedFile(io::Error::new(
                io::ErrorKind::InvalidData,
                "No file descriptor control message received",
            )));
        };

        let ucred = parse_ucred_cmsg(ucred_hdr).map_err(DualRingBuffersError::SharedFile)?;
        if !accept_fn(ucred.uid, ucred.gid) {
            return Err(DualRingBuffersError::SharedFile(io::Error::from(
                io::ErrorKind::PermissionDenied,
            )));
        }

        let shared_fd = parse_fd_cmsg(fd_hdr).map_err(DualRingBuffersError::SharedFile)?;
        let shared_file = unsafe { File::from_raw_fd(shared_fd) };

        let owned_file = DualRingBuffers::<N>::owned_memfd()?;

        send_file_descriptor(&mut self.0, &dest_addr, owned_file.as_raw_fd())
            .map_err(DualRingBuffersError::OwnedFile)?;

        DualRingBuffers::<N>::new_consumer_first(owned_file, shared_file)
    }
}
