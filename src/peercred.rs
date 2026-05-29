use std::{
    io,
    mem::MaybeUninit,
    os::{fd::AsRawFd, unix::net::UnixStream},
};

pub(crate) trait PeerCred {
    fn peer_ucred(&self) -> io::Result<libc::ucred>;
}

impl PeerCred for UnixStream {
    fn peer_ucred(&self) -> io::Result<libc::ucred> {
        let mut ucred = MaybeUninit::<libc::ucred>::uninit();
        let mut ucred_size = std::mem::size_of::<libc::ucred>() as libc::socklen_t;

        let status = unsafe {
            libc::getsockopt(
                self.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                ucred.as_mut_ptr() as *mut libc::c_void,
                &mut ucred_size,
            )
        };

        if status == -1 {
            return Err(io::Error::last_os_error());
        }

        debug_assert_eq!(ucred_size as usize, std::mem::size_of::<libc::ucred>());
        Ok(unsafe { ucred.assume_init() })
    }
}
