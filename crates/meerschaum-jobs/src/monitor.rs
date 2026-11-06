//! The restart monitor an API server runs: v4's `start_check_jobs_thread()`
//! (`meerschaum/jobs/__init__.py`, started by the API's startup event in `api/_events.py`).
//!
//! Every `check_restart_seconds` (static config, 1 s) it restarts the root's stopped `local`
//! jobs which were created with `--restart` (or `--loop`, `-s`) and were not stopped by hand,
//! like `check_restart_jobs('local', silent=True)` under `CHECK_JOBS_LOCK_PATH`. A container
//! or machine restart kills a job's supervisor without a manual stop, so the API brings the job
//! back once it starts. Restarts go through the startup gate as bulk starts.
//!
//! `systemd` jobs are not checked: systemd restarts its own units.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::daemon::JobsContext;
use crate::executor::{check_restart_jobs, get_restart_jobs};

/// A running restart monitor; dropping it stops the thread (within one interval).
#[derive(Debug)]
pub struct RestartMonitor {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl RestartMonitor {
    /// Stop the monitor and wait for its thread.
    pub fn stop(mut self) {
        self.halt();
    }

    fn halt(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for RestartMonitor {
    fn drop(&mut self) {
        self.halt();
    }
}

/// The interval of v4's monitor: `STATIC_CONFIG['jobs']['check_restart_seconds']`.
pub fn default_interval() -> Duration {
    let seconds = meerschaum_core::static_config::static_config()
        .jobs
        .check_restart_seconds;
    Duration::from_secs_f64(if seconds.is_finite() && seconds > 0.0 {
        seconds
    } else {
        1.0
    })
}

/// One check: restart the stopped restart-jobs of `ctx`'s root under the check-jobs lock.
/// `None` when another process holds the lock (it is doing the same check).
pub fn check_once(ctx: &JobsContext) -> Option<meerschaum_core::success::SuccessTuple> {
    let _lock = lock(ctx)?;
    let mut jobs = get_restart_jobs(ctx, true);
    jobs.retain(|_, job| !job.is_running() && job.stop_time().is_none());
    if jobs.is_empty() {
        return Some(meerschaum_core::success::SuccessTuple::ok(
            "No jobs to restart.",
        ));
    }
    Some(check_restart_jobs(&mut jobs))
}

/// Start the monitor thread for `ctx`'s root.
pub fn spawn(ctx: JobsContext, interval: Duration) -> std::io::Result<RestartMonitor> {
    let stop = Arc::new(AtomicBool::new(false));
    let flag = stop.clone();
    let handle = std::thread::Builder::new()
        .name("mrsm-restart-jobs".into())
        .spawn(move || {
            let tick = Duration::from_millis(50).min(interval);
            while !flag.load(Ordering::SeqCst) {
                // PARITY: `silent=True`; a failed restart is retried on the next check.
                let _ = check_once(&ctx);
                let mut waited = Duration::ZERO;
                while waited < interval && !flag.load(Ordering::SeqCst) {
                    std::thread::sleep(tick);
                    waited += tick;
                }
            }
        })?;
    Ok(RestartMonitor {
        stop,
        handle: Some(handle),
    })
}

#[cfg(unix)]
fn lock(ctx: &JobsContext) -> Option<nix::fcntl::Flock<std::fs::File>> {
    use nix::fcntl::{Flock, FlockArg};
    let path = &ctx.paths.check_jobs_lock;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).ok()?;
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
        .ok()?;
    Flock::lock(file, FlockArg::LockExclusiveNonblock).ok()
}

#[cfg(not(unix))]
fn lock(_ctx: &JobsContext) -> Option<()> {
    Some(())
}
