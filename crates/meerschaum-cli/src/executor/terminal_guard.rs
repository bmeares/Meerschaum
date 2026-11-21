//! Native lifetime owner for an API-only terminal. It loads no plugins; its native child
//! installs the mandatory execution boundary before opening the shell.

#[cfg(target_os = "linux")]
use meerschaum_jobs::supervisor::LifecycleOwner;
use meerschaum_server::webterm::ShellCommand;

pub(crate) const ARG: &str = "__mrsm_terminal_guard__";
const ROLE: &str = "terminal";
pub(crate) const PENDING_ENV: &str = "MRSM_TERMINAL_LAUNCH";

/// A Team terminal's initial pane and each new window re-exec the same native image.
/// This preserves the controlling PTY, PID and selected standard streams. No terminal
/// setup descriptor crosses into code that reads the Team environment or loads plugins.
pub(crate) const TEAM_EXEC_ARG: &str = "__mrsm_team_terminal_exec__";

pub(crate) fn exec_team_shell(arguments: &[String]) -> std::io::Error {
    #[cfg(target_os = "linux")]
    {
        use std::os::unix::process::CommandExt;
        let mut command = std::process::Command::new("/proc/self/exe");
        command.args(arguments);
        if let Err(error) = meerschaum_jobs::supervisor::seal_native_command(&mut command) {
            return error;
        }
        command.exec()
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = arguments;
        std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "The Team terminal requires the Linux native descriptor barrier.",
        )
    }
}

/// Wrap a complete host-prepared API-only shell, retaining its private environment and cwd.
pub(crate) fn wrap(shell: ShellCommand) -> Result<ShellCommand, String> {
    #[cfg(not(target_os = "linux"))]
    {
        let _ = shell;
        Err("API-only terminals require the Linux native lifetime guardian.".into())
    }
    #[cfg(target_os = "linux")]
    {
        let mut shell = shell;
        let native = meerschaum_core::self_exe::native_executable()
            .ok_or("An API-only terminal requires the native launcher.")?;
        let program = std::fs::canonicalize(&shell.program)
            .map_err(|_| "The native terminal launcher is unavailable.")?;
        if !shell.clear_env || program != native {
            return Err("The API-only terminal must use its complete native runtime.".into());
        }
        let spec = shell
            .env
            .iter()
            .rev()
            .find(|(key, _)| key == "MRSM_TEAM_SANDBOX")
            .map(|(_, value)| std::path::PathBuf::from(value))
            .ok_or("The API-only terminal has no mandatory context spec.")?;
        let sealed = spec
            .parent()
            .filter(|_| spec.is_absolute())
            .ok_or("The API-only terminal has no sealed context directory.")?;
        let cwd = shell
            .cwd
            .as_ref()
            .ok_or("The API-only terminal has no private state.")?;
        if cwd == sealed || !cwd.starts_with(sealed) {
            return Err("The terminal state cannot contain its sealed context metadata.".into());
        }
        let owner =
            LifecycleOwner::current().map_err(|_| "The API runtime owner cannot be identified.")?;
        let pending = meerschaum_jobs::supervisor::prepare_terminal_launch(&spec, &owner)
            .map_err(|_| "The terminal's pending lifecycle proof could not be recorded.")?;
        let mut arguments = vec![
            ARG.into(),
            serde_json::to_string(&owner).map_err(|_| "Could not encode the API runtime owner.")?,
            spec.display().to_string(),
            "--".into(),
        ];
        arguments.append(&mut shell.args);
        shell.args = arguments;
        let marker = meerschaum_core::team_scope::sandbox::runtime::LIFECYCLE_VAR;
        shell.env.retain(|(key, _)| {
            key != marker && key != PENDING_ENV && !matches!(key.as_str(), "TMUX" | "TMUX_PANE")
        });
        shell.env.push((marker.into(), ROLE.into()));
        shell
            .env
            .push((PENDING_ENV.into(), pending.to_string_lossy().into_owned()));
        Ok(shell)
    }
}

/// Only this exact host-selected native role precedes confinement. A forged descendant still
/// inherits its kernel restrictions. Neither the owner PID nor a supplied program is signalled.
pub(crate) fn run_if_requested(arguments: &[String]) -> Option<i32> {
    let marker = meerschaum_core::team_scope::sandbox::runtime::LIFECYCLE_VAR;
    if std::env::var(marker).as_deref() != Ok(ROLE) {
        return None;
    }
    if meerschaum_core::self_exe::native_executable().is_none()
        || arguments.first().map(String::as_str) != Some(ARG)
    {
        eprintln!("Invalid native terminal lifetime wrapper.");
        return Some(1);
    }
    #[cfg(not(target_os = "linux"))]
    {
        Some(1)
    }
    #[cfg(target_os = "linux")]
    {
        Some(match linux::run(arguments) {
            Ok(code) => code,
            Err(error) => {
                eprintln!("The terminal lifetime guardian could not start: {error}");
                1
            }
        })
    }
}

#[cfg(target_os = "linux")]
mod linux {
    use super::*;
    use std::io::{self, Read};
    use std::path::{Path, PathBuf};
    use std::process::{Command, Stdio};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    use std::time::{Duration, Instant};

    struct Signals(Vec<signal_hook::SigId>);
    impl Drop for Signals {
        fn drop(&mut self) {
            for signal in self.0.drain(..) {
                signal_hook::low_level::unregister(signal);
            }
        }
    }

    struct ProcessTree {
        children: meerschaum_jobs::process_children::OwnedChildren,
        spec: PathBuf,
        pending: PathBuf,
        owner: LifecycleOwner,
    }
    impl ProcessTree {
        fn finish(&self) -> io::Result<()> {
            self.children.retire();
            meerschaum_jobs::supervisor::complete_terminal_launch(
                &self.spec,
                &self.pending,
                &self.owner,
            )
        }
    }
    impl Drop for ProcessTree {
        fn drop(&mut self) {
            let _ = self.finish();
        }
    }

    #[derive(serde::Deserialize)]
    struct Lease {
        expires_at: i64,
    }
    struct Deadline {
        expiry: i64,
        deadline: Instant,
    }
    impl Deadline {
        fn read(&mut self, spec: &Path) -> io::Result<bool> {
            let path = spec
                .parent()
                .ok_or_else(|| io::Error::other("No context directory."))?
                .join("credential.json");
            let metadata = std::fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.len() > 32 * 1024 {
                return Ok(false);
            }
            let mut bytes = Vec::new();
            std::fs::File::open(path)?
                .take(32 * 1024 + 1)
                .read_to_end(&mut bytes)?;
            if bytes.len() > 32 * 1024 {
                return Ok(false);
            }
            let lease: Lease = serde_json::from_slice(&bytes)
                .map_err(|_| io::Error::other("Invalid terminal lease."))?;
            let now = chrono::Utc::now().timestamp();
            if lease.expires_at > now.saturating_add(35) || lease.expires_at < self.expiry {
                return Ok(false);
            }
            if lease.expires_at != self.expiry {
                let remaining = lease.expires_at.saturating_add(60).saturating_sub(now);
                if !(1..=95).contains(&remaining) {
                    return Ok(false);
                }
                self.expiry = lease.expires_at;
                self.deadline = Instant::now() + Duration::from_secs(remaining as u64);
            }
            Ok(Instant::now() < self.deadline)
        }
    }

    pub(super) fn run(arguments: &[String]) -> io::Result<i32> {
        if arguments.len() < 4 || arguments[1].len() > 512 || arguments[3] != "--" {
            return Err(io::Error::other("Invalid terminal guardian arguments."));
        }
        let owner: LifecycleOwner = serde_json::from_str(&arguments[1])
            .map_err(|_| io::Error::other("Invalid terminal runtime owner."))?;
        if !owner.alive() {
            return Err(io::Error::other("The API runtime owner ended."));
        }
        let spec = PathBuf::from(&arguments[2]);
        if !spec.is_absolute() || !spec.is_file() {
            return Err(io::Error::other(
                "The mandatory terminal context spec is unavailable.",
            ));
        }
        let pid = rustix::process::getpid();
        if rustix::process::getpgrp() != pid {
            return Err(io::Error::other(
                "The terminal guardian needs its own pane process group.",
            ));
        }
        let children = meerschaum_jobs::process_children::OwnedChildren::prepare()?;
        let stop = Arc::new(AtomicBool::new(false));
        let ignored = Arc::new(AtomicBool::new(false));
        let mut signals = Signals(Vec::new());
        for number in [signal_hook::consts::SIGINT, signal_hook::consts::SIGTSTP] {
            signals
                .0
                .push(signal_hook::flag::register(number, ignored.clone())?);
        }
        for number in [
            signal_hook::consts::SIGTERM,
            signal_hook::consts::SIGHUP,
            signal_hook::consts::SIGQUIT,
        ] {
            signals
                .0
                .push(signal_hook::flag::register(number, stop.clone())?);
        }
        let pending = std::env::var_os(PENDING_ENV)
            .map(PathBuf::from)
            .ok_or_else(|| io::Error::other("The terminal has no pending lifecycle proof."))?;
        meerschaum_jobs::supervisor::claim_terminal_launch(&spec, &pending, &owner)?;
        let tree = ProcessTree {
            children,
            spec: spec.clone(),
            pending,
            owner: owner.clone(),
        };
        let mut deadline = Deadline {
            expiry: i64::MIN,
            deadline: Instant::now(),
        };
        if !deadline.read(&spec)? {
            return Err(io::Error::other("The terminal lease expired."));
        }
        let native = meerschaum_core::self_exe::native_executable()
            .ok_or_else(|| io::Error::other("No native launcher."))?;
        let mut command = Command::new(native);
        command
            .args(&arguments[4..])
            .env_remove(meerschaum_core::team_scope::sandbox::runtime::LIFECYCLE_VAR)
            .env_remove(PENDING_ENV)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        // The guardian is trusted native code; its shell is the first user-code boundary.
        // Preserve the PTY standard streams while refusing inherited Main/guardian handles.
        meerschaum_jobs::supervisor::seal_native_command(&mut command)?;
        let mut child = command.spawn()?;
        let child_pid = i32::try_from(child.id()).map_err(io::Error::other)?;
        let descriptor = meerschaum_jobs::supervisor::record_terminal_launch_child(
            child_pid,
            &spec,
            &tree.pending,
            &owner,
        )?;
        let code = loop {
            if stop.load(Ordering::Acquire)
                || !owner.alive()
                || !deadline.read(&spec).unwrap_or(false)
            {
                break 1;
            }
            if let Some(status) = child.try_wait()? {
                break status.code().unwrap_or(1);
            }
            std::thread::sleep(Duration::from_millis(50));
        };
        tree.finish()?;
        let _ = child.wait();
        let _ = std::fs::remove_file(descriptor);
        Ok(code)
    }
}
