use std::{io, ops::Deref, sync::atomic::AtomicU32, time::Duration};

pub fn timespec_from_duration(duration: &Duration) -> libc::timespec {
    libc::timespec {
        tv_sec: duration.as_secs() as libc::time_t,
        tv_nsec: duration.subsec_nanos() as libc::c_long,
    }
}

#[derive(Default, Debug)]
pub struct Futex(AtomicU32);

impl Futex {
    pub fn new(value: u32) -> Self {
        Self(AtomicU32::new(value))
    }

    /// Call SYS_futex with FUTEX_WAIT and the provided expected value and timeout duration.
    /// If the value at the futex address does not match the expected value,
    /// this function will return immediately with Ok.
    /// If the value matches, this function will block until either the futex is woken or the timeout expires.
    /// If the futex is woken or the value doesn't match, this function will return Ok
    /// Otherwise, return Err with the error from the futex syscall.
    /// Note: the syscall is always called
    pub fn wait_with_timeout(&self, expected: u32, duration: &Duration) -> io::Result<()> {
        let timespec = timespec_from_duration(duration);
        self.futex_wait(expected, &timespec)
    }

    /// Call SYS_futex with FUTEX_WAIT and the provided expected value.
    /// If the value at the futex address does not match the expected value,
    /// this function will return immediately with Ok.
    /// If the value matches, this function will block forever until the futex is woken.
    /// If the futex is woken or the value doesn't match, this function will return Ok
    /// Otherwise, return Err with the error from the futex syscall.
    /// Note: the syscall is always called
    pub fn wait_forever(&self, expected: u32) -> io::Result<()> {
        self.futex_wait(expected, std::ptr::null())
    }

    /// Call SYS_futex with FUTEX_WAIT and the provided expected value and timeout duration.
    /// If the value at the futex address does not match the expected value,
    /// this function will return immediately with Ok.
    /// If the value matches, this function will block until either the futex is woken or the timeout expires.
    /// If timeout is None, this function will block forever until the futex is woken.
    /// If the futex is woken or the value doesn't match, this function will return Ok
    /// Otherwise, return Err with the error from the futex syscall.
    /// Note: the syscall is always called
    pub fn wait(&self, expected: u32, duration: Option<&Duration>) -> io::Result<()> {
        match duration {
            Some(d) => self.wait_with_timeout(expected, d),
            None => self.wait_forever(expected),
        }
    }

    /// Call SYS_futex with FUTEX_WAKE and the provided count to wake up to count waiters on this futex.
    pub fn wake(&self, count: i32) -> io::Result<u32> {
        let status: i64 =
            unsafe { libc::syscall(libc::SYS_futex, &self.0, libc::FUTEX_WAKE, count) };

        if status == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(status as u32)
        }
    }

    /// Call SYS_futex with FUTEX_WAKE to wake one waiter on this futex.
    pub fn wake_one(&self) -> io::Result<u32> {
        self.wake(1)
    }

    /// Call SYS_futex with FUTEX_WAKE to wake all (i32::MAX) waiters on this futex.
    pub fn wake_all(&self) -> io::Result<u32> {
        self.wake(i32::MAX)
    }

    fn futex_wait(&self, expected: u32, timeout: *const libc::timespec) -> io::Result<()> {
        let status = unsafe {
            libc::syscall(
                libc::SYS_futex,
                &self.0,
                libc::FUTEX_WAIT,
                expected,
                timeout,
            )
        };

        if status == -1 {
            let e = io::Error::last_os_error();
            match e.kind() {
                io::ErrorKind::WouldBlock => return Ok(()), // WouldBlock aliases EAGAIN, which means the value at the futex address did not match the expected value, so we should just return
                _ => return Err(e),
            }
        } else {
            return Ok(());
        }
    }
}

impl Deref for Futex {
    type Target = AtomicU32;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
