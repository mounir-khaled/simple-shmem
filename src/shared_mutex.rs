use std::{
    cell::UnsafeCell,
    io,
    sync::atomic::{AtomicU32, Ordering},
};

pub const UNLOCKED: u32 = 0;
pub const LOCKED: u32 = 1;
pub const CONTENDED: u32 = 2;

#[repr(C)]
pub struct SharedMutex<T> {
    state: AtomicU32,
    data: UnsafeCell<T>,
}

pub struct FutexGuard<'a, T> {
    futex: &'a SharedMutex<T>,
    data: &'a mut T,
}

impl<T> SharedMutex<T> {
    pub fn new(data: T) -> Self {
        Self {
            state: AtomicU32::new(UNLOCKED),
            data: UnsafeCell::new(data),
        }
    }

    pub fn lock<'a>(&'a self) -> Result<FutexGuard<'a, T>, io::Error> {
        loop {
            // if the state is UNLOCKED change it to LOCKED and return the guard
            let mut state =
                self.state
                    .compare_exchange(UNLOCKED, LOCKED, Ordering::Acquire, Ordering::Relaxed);

            let Err(_) = state else {
                break Ok(FutexGuard {
                    futex: self,
                    data: unsafe { &mut *self.data.get() },
                });
            };

            // the state was not UNLOCKED so mark it as CONTENDED if was not already
            state = self.state.compare_exchange(
                LOCKED,
                CONTENDED,
                Ordering::Acquire,
                Ordering::Relaxed,
            );

            if let Err(e) = state {
                if e != CONTENDED {
                    // the state was not LOCKED or CONTENDED, so it might have been UNLOCKED
                    // so we can try to acquire the lock again
                    continue;
                }
            }

            // the state was CONTENDED or has been marked as CONTENDED, so we need to wait
            let status = unsafe {
                libc::syscall(
                    libc::SYS_futex,
                    &self.state,
                    libc::FUTEX_WAIT,
                    CONTENDED,
                    std::ptr::null::<libc::timespec>(),
                )
            };

            if status == -1 {
                let err = unsafe { *libc::__errno_location() };
                // if it is EINTR, we were interrupted by a signal
                // if it is EAGAIN, the state was not contended anymore
                // in both cases, we can try to acquire the lock again
                if err != libc::EINTR && err != libc::EAGAIN {
                    break Err(io::Error::from_raw_os_error(err));
                }
            }
        }
    }
}

impl<'a, T> Drop for FutexGuard<'a, T> {
    fn drop(&mut self) {
        if self.futex.state.swap(UNLOCKED, Ordering::Release) == CONTENDED {
            let status =
                unsafe { libc::syscall(libc::SYS_futex, &self.futex.state, libc::FUTEX_WAKE, 1) };

            if status == -1 {
                let err = unsafe { *libc::__errno_location() };
                if err != libc::EINTR {
                    panic!("Futex wake failed: {}", err);
                }
            }
        }
    }
}

impl<'a, T> std::ops::Deref for FutexGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        self.data
    }
}

impl<'a, T> std::ops::DerefMut for FutexGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data
    }
}
