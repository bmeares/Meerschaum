//! Confined compiler runner. No interpreter/compiler runs in Main preparation.

use super::Prepared;
use meerschaum_plugin::source::compiler::{CompileRequest, SourceCompiler};
#[cfg(target_os = "linux")]
use std::collections::VecDeque;
use std::ffi::OsString;
use std::fmt::{Debug, Formatter};
use std::io::ErrorKind;
#[cfg(target_os = "linux")]
use std::io::Read;
use std::path::Path;
#[cfg(target_os = "linux")]
use std::process::{Child, Stdio};
use std::process::{Command, Output};
use std::sync::{Mutex, TryLockError};
use std::time::{Duration, Instant};

#[cfg(target_os = "linux")]
const MAX_RAW: usize = 2 * 1024 * 1024;
#[cfg(target_os = "linux")]
const MAX_TAIL: usize = 64 * 1024;

pub(crate) struct Runner {
    prepared: Prepared,
    deadline: Instant,
    gate: Mutex<()>,
}
impl Debug for Runner {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfinedSourceCompiler")
            .field("identity", &self.prepared.identity)
            .finish_non_exhaustive()
    }
}

impl Runner {
    /// `prepared` must come only from the stored sealed native Api/Metadata generation.
    /// Factory/Context integration must verify that provenance before this constructor.
    pub(crate) fn admitted(prepared: Prepared) -> Result<Self, String> {
        if std::env::var(meerschaum_core::team_scope::api_execution::PROFILE_VAR).as_deref()
            != Ok("1")
        {
            return Err("Native compilation requires the private Api execution profile.".into());
        }
        meerschaum_core::team_scope::sandbox::current()
            .ok_or("Native compilation requires its mandatory native sandbox.")?
            .require_sockets_verdict()
            .map_err(|_| "Native compilation requires filesystem, socket and signal isolation.")?;
        let generation = prepared
            .cargo_home
            .parent()
            .ok_or("Missing native compiler generation.")?;
        if prepared
            .cargo_home
            .file_name()
            .and_then(|name| name.to_str())
            != Some("native-cargo-home")
            || prepared.state.parent() != Some(generation)
            || !matches!(
                prepared.state.file_name().and_then(|name| name.to_str()),
                Some("state" | ".mrsm_team_work")
            )
            || prepared.cargo_home.canonicalize().ok().as_deref()
                != Some(prepared.cargo_home.as_path())
            || prepared.lock_files
                != [".package-cache", ".package-cache-mutate"]
                    .iter()
                    .map(|name| prepared.cargo_home.join(name))
                    .collect::<Vec<_>>()
        {
            return Err(
                "The native compiler material is not bound to this private generation.".into(),
            );
        }
        // The context grant, rather than owner-writable chmod bits, must make Cargo config
        // immutable. Probe creation only before source discovery; a broad/wrong policy is
        // refused and the fresh canary is removed without writing any configuration.
        let probe = prepared.cargo_home.join(format!(
            ".compiler-write-probe-{}",
            uuid::Uuid::new_v4().simple()
        ));
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&probe)
        {
            Ok(file) => {
                drop(file);
                let _ = std::fs::remove_file(&probe);
                return Err("The native compiler Cargo home remains writable.".into());
            }
            Err(error) if error.kind() == ErrorKind::PermissionDenied => {}
            Err(_) => {
                return Err(
                    "The native compiler Cargo-home boundary could not be verified.".into(),
                );
            }
        }
        let remaining = prepared.owner_deadline - chrono::Utc::now().timestamp();
        if !(1..=60).contains(&remaining) {
            return Err(
                "The sealed native compiler owner deadline is unavailable or expired.".into(),
            );
        }
        // Captured once: a later source/build cannot restart this budget or extend it by
        // changing a credential file, local environment or wall clock.
        let deadline = Instant::now() + Duration::from_secs(remaining.min(20) as u64);
        Ok(Self {
            prepared,
            deadline,
            gate: Mutex::new(()),
        })
    }

    fn private_environment(&self) -> Result<Vec<(OsString, OsString)>, String> {
        let toolchain = self
            .prepared
            .cargo
            .parent()
            .ok_or("Missing owned compiler directory.")?;
        let mut path = OsString::from(toolchain);
        path.push(":/usr/bin:/bin");
        Ok(vec![
            ("PATH".into(), path),
            (
                "CARGO_HOME".into(),
                self.prepared.cargo_home.as_os_str().into(),
            ),
            ("RUSTC".into(), self.prepared.rustc.as_os_str().into()),
            ("RUSTC_WRAPPER".into(), "".into()),
            ("RUSTC_WORKSPACE_WRAPPER".into(), "".into()),
            (
                "HOME".into(),
                self.prepared.state.join("home").into_os_string(),
            ),
            (
                "TMPDIR".into(),
                self.prepared.state.join("tmp").into_os_string(),
            ),
            ("CARGO_NET_OFFLINE".into(), "true".into()),
            ("CARGO_INCREMENTAL".into(), "0".into()),
            ("CARGO_TERM_COLOR".into(), "never".into()),
            ("LANG".into(), "C".into()),
            ("TZ".into(), "UTC".into()),
        ])
    }
}

impl SourceCompiler for Runner {
    fn identity(&self) -> &str {
        &self.prepared.identity
    }
    fn cargo(&self) -> &Path {
        &self.prepared.cargo
    }
    fn lockfile(&self, name: &str) -> Result<String, String> {
        self.prepared.source_lock(name)
    }

    fn compile(&self, request: &CompileRequest<'_>) -> Result<Output, String> {
        let _guard = loop {
            if Instant::now() >= self.deadline {
                return Err("The native compiler owner deadline expired before admission.".into());
            }
            match self.gate.try_lock() {
                Ok(guard) => break guard,
                Err(TryLockError::WouldBlock) => std::thread::sleep(Duration::from_millis(5)),
                Err(TryLockError::Poisoned(_)) => {
                    return Err("The private compiler is unavailable after a failed build.".into());
                }
            }
        };
        // A generated crate and target must stay under this generation's writable state.
        // The fixed discovery adapter must separately prove exact single-file generation
        // and write the pinned lock before fingerprinting; NativePackage is unsupported.
        if !meerschaum_server::executor::plugins::valid_name(request.name) {
            return Err("Invalid generated native source identity.".into());
        }
        let manifest = request
            .manifest
            .canonicalize()
            .map_err(|_| "Missing generated native manifest.")?;
        let state = self
            .prepared
            .state
            .canonicalize()
            .map_err(|_| "Missing private native state.")?;
        if !manifest.starts_with(&state)
            || !request.target.starts_with(&state)
            || request
                .target
                .components()
                .any(|part| matches!(part, std::path::Component::ParentDir))
            || manifest.file_name().and_then(|name| name.to_str()) != Some("Cargo.toml")
        {
            return Err("Generated native build paths cannot escape their private state.".into());
        }
        let mut command = Command::new(&self.prepared.cargo);
        command
            .current_dir(&self.prepared.cwd)
            .args([
                "build",
                "--release",
                "--frozen",
                "--offline",
                "--jobs",
                "2",
                "--manifest-path",
            ])
            .arg(manifest)
            .arg("--target-dir")
            .arg(request.target);
        run_owned(command, &self.private_environment()?, self.deadline)
    }
}

/// Hold the leader unreaped until all signal/drain work is done. waitid(NOWAIT) observes
/// completion without freeing its PID/process-group identity for reuse.
#[cfg(target_os = "linux")]
struct OwnedChild {
    child: Child,
    reaped: bool,
}
#[cfg(target_os = "linux")]
impl OwnedChild {
    fn exited(&mut self) -> Result<bool, String> {
        let pid = rustix::process::Pid::from_raw(self.child.id() as i32)
            .ok_or("Invalid owned compiler PID.")?;
        match rustix::process::waitid(
            rustix::process::WaitId::Pid(pid),
            rustix::process::WaitIdOptions::EXITED
                | rustix::process::WaitIdOptions::NOHANG
                | rustix::process::WaitIdOptions::NOWAIT,
        ) {
            Ok(status) => Ok(status.is_some()),
            Err(rustix::io::Errno::INTR) => Ok(false),
            Err(rustix::io::Errno::CHILD) => {
                // Another reaper would invalidate our PID ownership. Never signal a
                // possibly reused process group after losing that proof.
                self.reaped = true;
                Err("The compiler's owned wait identity was lost.".into())
            }
            Err(_) => Err("Could not observe the owned compiler process.".into()),
        }
    }
    fn kill_group(&mut self) {
        if self.reaped {
            return;
        }
        if let Some(pid) = rustix::process::Pid::from_raw(self.child.id() as i32) {
            let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
        }
        let _ = self.child.kill();
    }
    fn wait(&mut self) -> Result<std::process::ExitStatus, String> {
        match self.child.wait() {
            Ok(status) => {
                self.reaped = true;
                Ok(status)
            }
            Err(error) => {
                if error.raw_os_error() == Some(rustix::io::Errno::CHILD.raw_os_error()) {
                    self.reaped = true;
                }
                Err("Could not reap the owned compiler process.".into())
            }
        }
    }
}
#[cfg(target_os = "linux")]
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.reaped {
            self.kill_group();
            let _ = self.child.wait();
            self.reaped = true;
        }
    }
}

#[cfg(target_os = "linux")]
fn drain<R: Read>(
    pipe: &mut Option<R>,
    tail: &mut VecDeque<u8>,
    total: &mut usize,
) -> Result<(), String> {
    let mut buffer = [0u8; 16 * 1024];
    for _ in 0..8 {
        let Some(reader) = pipe.as_mut() else {
            return Ok(());
        };
        match reader.read(&mut buffer) {
            Ok(0) => {
                *pipe = None;
                return Ok(());
            }
            Ok(count) => {
                *total = total
                    .checked_add(count)
                    .ok_or("Compiler output count overflowed.")?;
                if *total > MAX_RAW {
                    return Err(
                        "Native compiler stdout/stderr exceeded the combined byte budget.".into(),
                    );
                }
                for byte in &buffer[..count] {
                    if tail.len() == MAX_TAIL {
                        tail.pop_front();
                    }
                    tail.push_back(*byte);
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(()),
            Err(error) if error.kind() == ErrorKind::Interrupted => continue,
            Err(_) => return Err("Could not drain the owned compiler output.".into()),
        }
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn run_owned(
    mut command: Command,
    environment: &[(OsString, OsString)],
    deadline: Instant,
) -> Result<Output, String> {
    use std::os::fd::AsFd;
    use std::os::unix::process::CommandExt;
    if Instant::now() >= deadline {
        return Err("The native compiler deadline expired before spawn.".into());
    }
    command
        .env_clear()
        .envs(environment.iter().map(|(key, value)| (key, value)))
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .process_group(0);
    let child = command
        .spawn()
        .map_err(|_| "Could not spawn the owned native compiler.".to_string())?;
    let mut owned = OwnedChild {
        child,
        reaped: false,
    };
    let mut stdout = owned.child.stdout.take();
    let mut stderr = owned.child.stderr.take();
    for fd in [
        stdout.as_ref().map(AsFd::as_fd),
        stderr.as_ref().map(AsFd::as_fd),
    ]
    .into_iter()
    .flatten()
    {
        let flags = rustix::fs::fcntl_getfl(fd)
            .map_err(|_| "Could not bound the compiler output pipes.")?;
        rustix::fs::fcntl_setfl(fd, flags | rustix::fs::OFlags::NONBLOCK)
            .map_err(|_| "Could not make compiler output nonblocking.")?;
    }
    let mut out = VecDeque::new();
    let mut err = VecDeque::new();
    let mut total = 0usize;
    let mut drain_deadline = None;
    loop {
        drain(&mut stdout, &mut out, &mut total)?;
        drain(&mut stderr, &mut err, &mut total)?;
        let exited = owned.exited()?;
        if exited && stdout.is_none() && stderr.is_none() {
            // No descendant may outlive this build in the exact owned process group.
            owned.kill_group();
            let status = owned.wait()?;
            return Ok(Output {
                status,
                stdout: out.into_iter().collect(),
                stderr: err.into_iter().collect(),
            });
        }
        if exited && drain_deadline.is_none() {
            drain_deadline = Some(Instant::now() + Duration::from_secs(1));
        }
        if Instant::now() >= deadline || drain_deadline.is_some_and(|until| Instant::now() >= until)
        {
            return Err(
                "The native compiler owner deadline or output drain budget expired.".into(),
            );
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
#[cfg(not(target_os = "linux"))]
fn run_owned(_: Command, _: &[(OsString, OsString)], _: Instant) -> Result<Output, String> {
    Err("The confined native compiler runner is unsupported on this platform.".into())
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn owned_compiler_capture_clears_explicit_parent_env_and_retains_both_streams() {
        let mut command = Command::new("/bin/sh");
        command.env("MRSM_CONFIG", "private-parent-marker").args([
            "-c",
            "[ -z \"${MRSM_CONFIG+x}\" ] || exit 9; printf source-stdout; printf source-stderr >&2",
        ]);
        let output = run_owned(command, &[], Instant::now() + Duration::from_secs(3)).unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"source-stdout");
        assert_eq!(output.stderr, b"source-stderr");
    }

    #[test]
    fn owned_compiler_capture_caps_stdout_and_stderr_in_real_children() {
        for redirect in ["", " >&2"] {
            let mut command = Command::new("/bin/sh");
            command.args([
                "-c",
                &format!("while :; do printf '{}'{redirect}; done", "x".repeat(512)),
            ]);
            let error =
                run_owned(command, &[], Instant::now() + Duration::from_secs(5)).unwrap_err();
            assert!(error.contains("byte budget"), "{error}");
        }
    }

    #[test]
    fn owned_compiler_deadline_kills_and_reaps_the_exact_child() {
        let temp = tempfile::tempdir().unwrap();
        let pidfile = temp.path().join("compiler.pid");
        let mut command = Command::new("/bin/sh");
        command
            .args([
                "-c",
                "printf '%s' \"$$\" > \"$1\"; exec /bin/sleep 10",
                "mrsm-owned-compiler",
            ])
            .arg(&pidfile);
        let started = Instant::now();
        let error = run_owned(command, &[], started + Duration::from_millis(500)).unwrap_err();
        assert!(error.contains("deadline"), "{error}");
        assert!(started.elapsed() < Duration::from_secs(3));
        let pid = std::fs::read_to_string(pidfile)
            .unwrap()
            .parse::<i32>()
            .unwrap();
        let pid = rustix::process::Pid::from_raw(pid).unwrap();
        assert_eq!(
            rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG).unwrap_err(),
            rustix::io::Errno::CHILD
        );
    }
}
