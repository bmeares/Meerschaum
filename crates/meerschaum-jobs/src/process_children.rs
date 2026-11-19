//! Retire only descendants of one native lifecycle supervisor, including detached sessions.

/// A dedicated native supervisor becomes a subreaper before starting its first worker.
/// This must never be installed on a shared application process that owns unrelated children.
pub struct OwnedChildren {
    owner: i32,
}

impl OwnedChildren {
    /// Verify native Linux process ownership before admitting any confined child.
    pub fn prepare() -> std::io::Result<Self> {
        #[cfg(target_os = "linux")]
        {
            use rustix::process::{Pid, PidfdFlags, Signal};
            let pid = rustix::process::getpid();
            let owner = pid.as_raw_nonzero().get();
            crate::process::children(owner).ok_or_else(|| {
                std::io::Error::other("Native descendant inspection is unavailable.")
            })?;
            let descriptor = rustix::process::pidfd_open(pid, PidfdFlags::empty())?;
            rustix::process::pidfd_send_signal(&descriptor, Signal::CONT)?;
            rustix::process::set_child_subreaper(Pid::from_raw(1))?;
            Ok(Self { owner })
        }
        #[cfg(not(target_os = "linux"))]
        Err(std::io::Error::other(
            "Native descendant confinement requires Linux.",
        ))
    }

    /// Freeze and kill direct children by pidfd, then repeat for descendants adopted by this
    /// subreaper. Parent checks after opening each descriptor exclude recycled bystander PIDs.
    pub fn retire(&self) {
        #[cfg(target_os = "linux")]
        {
            use rustix::process::{Pid, PidfdFlags, Signal};
            if i32::try_from(std::process::id()).ok() != Some(self.owner) {
                return;
            }
            let parent = self.owner.to_string();
            loop {
                let Some(children) = crate::process::children(self.owner) else {
                    std::thread::sleep(std::time::Duration::from_millis(10));
                    continue;
                };
                if children.is_empty() {
                    return;
                }
                for child in children {
                    let Some(pid) = Pid::from_raw(child) else {
                        continue;
                    };
                    let Ok(descriptor) = rustix::process::pidfd_open(pid, PidfdFlags::empty())
                    else {
                        continue;
                    };
                    let ours = std::fs::read_to_string(format!("/proc/{child}/status"))
                        .ok()
                        .is_some_and(|text| {
                            text.lines().any(|line| {
                                line.strip_prefix("PPid:")
                                    .is_some_and(|value| value.trim() == parent)
                            })
                        });
                    if ours {
                        let _ = rustix::process::pidfd_send_signal(&descriptor, Signal::STOP);
                        let _ = rustix::process::pidfd_send_signal(&descriptor, Signal::KILL);
                    }
                }
                while matches!(
                    rustix::process::wait(rustix::process::WaitOptions::NOHANG),
                    Ok(Some(_))
                ) {}
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = self.owner;
        }
    }
}

impl Drop for OwnedChildren {
    fn drop(&mut self) {
        self.retire();
    }
}
