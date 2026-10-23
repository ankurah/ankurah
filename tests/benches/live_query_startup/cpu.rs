//! Process CPU readings. A reading covers both nodes and every background thread, so the
//! difference between two readings is all the work the process did in between.

use serde::Serialize;

/// The user and system CPU time this process has used.
#[derive(Clone, Copy, Serialize)]
pub struct ProcessCpu {
    user_us: u64,
    system_us: u64,
}

impl ProcessCpu {
    /// Reads this process's CPU time, or `None` on hosts without `getrusage`.
    pub fn now() -> anyhow::Result<Option<Self>> {
        #[cfg(unix)]
        {
            let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
            // getrusage initializes the output structure only on success.
            if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let usage = unsafe { usage.assume_init() };
            let micros = |time: libc::timeval| time.tv_sec as u64 * 1_000_000 + time.tv_usec as u64;
            Ok(Some(Self { user_us: micros(usage.ru_utime), system_us: micros(usage.ru_stime) }))
        }
        #[cfg(not(unix))]
        {
            Ok(None)
        }
    }

    /// The CPU time used between two readings.
    pub fn between(start: Option<Self>, end: Option<Self>) -> Option<Self> {
        start.zip(end).map(|(start, end)| Self { user_us: end.user_us - start.user_us, system_us: end.system_us - start.system_us })
    }
}
