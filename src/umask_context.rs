use std::sync::{Mutex, MutexGuard, PoisonError, TryLockError};

static PHANTOM_UMASK: Mutex<()> = Mutex::new(());

#[must_use = "UmaskContext must be held in a variable to maintain the umask setting"]
pub struct UmaskContext<'a> {
    old_umask: u32,
    umask_guard: MutexGuard<'a, ()>,
}

impl<'a> UmaskContext<'a> {
    pub fn new(mask: u32) -> Result<Self, PoisonError<MutexGuard<'a, ()>>> {
        let guard = PHANTOM_UMASK.lock()?;
        let old_umask = unsafe { libc::umask(mask) };
        Ok(Self {
            old_umask,
            umask_guard: guard,
        })
    }

    pub fn try_new(mask: u32) -> Result<Self, TryLockError<MutexGuard<'a, ()>>> {
        let guard = PHANTOM_UMASK.try_lock()?;
        let old_umask = unsafe { libc::umask(mask) };
        Ok(Self {
            old_umask,
            umask_guard: guard,
        })
    }
}

impl<'a> Drop for UmaskContext<'a> {
    fn drop(&mut self) {
        unsafe { libc::umask(self.old_umask) };
    }
}
