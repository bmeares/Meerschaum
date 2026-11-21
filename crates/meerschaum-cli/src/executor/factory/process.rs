//! Bounded, owned administrator subprocess. No image executable is started here.

use crate::dispatch::CommandSpec;
use std::io::{self, Read};
use std::time::Duration;

#[cfg(not(target_os = "linux"))]
pub(crate) fn stream(
    _: &CommandSpec,
    _: Duration,
    _: u64,
    _: &mut (dyn FnMut(&mut dyn Read) -> io::Result<()> + Send),
) -> io::Result<i32> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "Protected native Factory image inspection requires the supported Linux host.",
    ))
}

#[cfg(target_os = "linux")]
pub(crate) fn stream(
    spec: &CommandSpec,
    timeout: Duration,
    maximum_stdout: u64,
    consume: &mut (dyn FnMut(&mut dyn Read) -> io::Result<()> + Send),
) -> io::Result<i32> {
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Stdio};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Instant;

    fn unavailable() -> io::Error {
        io::Error::other(
            "The bounded administrator command failed or exceeded its output/deadline bound.",
        )
    }
    if timeout.is_zero()
        || timeout > Duration::from_secs(300)
        || maximum_stdout > 16 * 1024 * 1024 * 1024
    {
        return Err(unavailable());
    }
    struct Owned {
        child: Child,
        reaped: bool,
    }
    impl Owned {
        fn exited(&mut self) -> io::Result<bool> {
            let pid =
                rustix::process::Pid::from_raw(self.child.id() as i32).ok_or_else(unavailable)?;
            match rustix::process::waitid(
                rustix::process::WaitId::Pid(pid),
                rustix::process::WaitIdOptions::EXITED
                    | rustix::process::WaitIdOptions::NOHANG
                    | rustix::process::WaitIdOptions::NOWAIT,
            ) {
                Ok(status) => Ok(status.is_some()),
                Err(rustix::io::Errno::INTR) => Ok(false),
                Err(rustix::io::Errno::CHILD) => {
                    self.reaped = true;
                    Err(unavailable())
                }
                Err(_) => Err(unavailable()),
            }
        }
        fn kill(&mut self) {
            if !self.reaped {
                if let Some(pid) = rustix::process::Pid::from_raw(self.child.id() as i32) {
                    let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
                }
                let _ = self.child.kill();
            }
        }
        fn wait(&mut self) -> io::Result<std::process::ExitStatus> {
            let result = self.child.wait();
            if result.is_ok()
                || result.as_ref().err().is_some_and(|error| {
                    error.raw_os_error() == Some(rustix::io::Errno::CHILD.raw_os_error())
                })
            {
                self.reaped = true;
            }
            result
        }
    }
    impl Drop for Owned {
        fn drop(&mut self) {
            if !self.reaped {
                self.kill();
                let _ = self.wait();
            }
        }
    }
    struct Limited<R> {
        inner: R,
        remaining: u64,
    }
    impl<R: Read> Read for Limited<R> {
        fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
            if output.is_empty() {
                return Ok(0);
            }
            if self.remaining == 0 {
                let mut extra = [0];
                return match self.inner.read(&mut extra)? {
                    0 => Ok(0),
                    _ => Err(unavailable()),
                };
            }
            let maximum = output
                .len()
                .min(self.remaining.min(usize::MAX as u64) as usize);
            let count = self.inner.read(&mut output[..maximum])?;
            self.remaining -= count as u64;
            Ok(count)
        }
    }

    let mut command = crate::native::terminal::command(spec);
    command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .process_group(0);
    let mut owned = Owned {
        child: command.spawn()?,
        reaped: false,
    };
    let stdout = owned.child.stdout.take().ok_or_else(unavailable)?;
    let stderr = owned.child.stderr.take().ok_or_else(unavailable)?;
    let deadline = Instant::now() + timeout;
    let failed_stderr = Arc::new(AtomicBool::new(false));
    let stderr_done = Arc::new(AtomicBool::new(false));
    std::thread::scope(|scope| {
        let (send, receive) = std::sync::mpsc::sync_channel(1);
        scope.spawn(move || {
            let mut stdout = Limited {
                inner: stdout,
                remaining: maximum_stdout,
            };
            let result = consume(&mut stdout)
                .and_then(|()| io::copy(&mut stdout, &mut io::sink()).map(|_| ()));
            let _ = send.send(result);
        });
        let failed = failed_stderr.clone();
        let done = stderr_done.clone();
        scope.spawn(move || {
            let result = io::copy(
                &mut Limited {
                    inner: stderr,
                    remaining: 64 * 1024,
                },
                &mut io::sink(),
            );
            if result.is_err() {
                failed.store(true, Ordering::Release);
            }
            done.store(true, Ordering::Release);
        });
        let mut output = None;
        let result = loop {
            if output.is_none() {
                match receive.try_recv() {
                    Ok(result) => output = Some(result),
                    Err(std::sync::mpsc::TryRecvError::Empty) => {}
                    Err(_) => break Err(unavailable()),
                }
            }
            if failed_stderr.load(Ordering::Acquire)
                || output.as_ref().is_some_and(Result::is_err)
                || Instant::now() >= deadline
            {
                break Err(unavailable());
            }
            match owned.exited() {
                Ok(true) if output.is_some() && stderr_done.load(Ordering::Acquire) => break Ok(()),
                Ok(_) => {}
                Err(error) => break Err(error),
            }
            std::thread::sleep(Duration::from_millis(10));
        };
        if result.is_err() {
            owned.kill();
        }
        // NOWAIT above retains the exact group leader identity until every required
        // signal is sent. Never signal after wait() permits PID/group reuse.
        let status = owned.wait()?;
        result?;
        output.ok_or_else(unavailable)??;
        Ok(status.code().unwrap_or(1))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dispatch::Host;
    struct Closed;
    impl Host for Closed {
        fn run(&mut self, _: &CommandSpec) -> io::Result<i32> {
            panic!("A stream must never fall through to process execution.")
        }
        fn read_line(&mut self, _: &str) -> Option<String> {
            None
        }
    }
    #[test]
    fn alternate_host_streaming_defaults_closed_without_running_any_command() {
        let mut consumed = false;
        let error = Closed
            .output_stream(
                &CommandSpec::default(),
                Duration::from_secs(1),
                1,
                &mut |_| {
                    consumed = true;
                    Ok(())
                },
            )
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        assert!(!consumed);
    }
    // Merge inside `executor::factory::process::tests` after Root clears registered sources.
    // Each child is a fixed owned /bin/sh, with its own group. No services or tmux.
    #[cfg(target_os = "linux")]
    fn owned_spec(script: &str, directory: &std::path::Path) -> CommandSpec {
        CommandSpec {
            program: "/bin/sh".into(),
            args: vec![
                "-c".into(),
                script.into(),
                "mrsm-owned-stream-fixture".into(),
                directory.join("leader").to_string_lossy().into_owned(),
                directory.join("child").to_string_lossy().into_owned(),
            ],
            env_clear: true,
            env: vec![
                ("PATH".into(), "/usr/bin:/bin".into()),
                ("TMUX".into(), "synthetic-unread-user-server".into()),
                ("TMUX_PANE".into(), "%synthetic".into()),
            ],
            ..Default::default()
        }
    }

    #[cfg(target_os = "linux")]
    fn assert_exact_leader_reaped(directory: &std::path::Path) {
        let raw = std::fs::read_to_string(directory.join("leader")).unwrap();
        let pid = rustix::process::Pid::from_raw(raw.trim().parse().unwrap()).unwrap();
        assert!(
            matches!(
                rustix::process::waitid(
                    rustix::process::WaitId::Pid(pid),
                    rustix::process::WaitIdOptions::EXITED
                        | rustix::process::WaitIdOptions::NOHANG
                        | rustix::process::WaitIdOptions::NOWAIT
                ),
                Err(rustix::io::Errno::CHILD)
            ),
            "The exact fixture leader must already have been awaited/reaped."
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_system_stream_positive_removes_tmux_and_reaps_exact_child() {
        let directory = tempfile::tempdir().unwrap();
        let spec = owned_spec(
            r#"printf '%s\n' "$$" > "$1"; printf '%s:%s' "${TMUX-unset}" "${TMUX_PANE-unset}"; printf diagnostic >&2"#,
            directory.path(),
        );
        let mut output = Vec::new();
        let status = crate::dispatch::SystemHost
            .output_stream(&spec, Duration::from_secs(5), 11, &mut |stream| {
                stream.read_to_end(&mut output).map(|_| ())
            })
            .unwrap();
        assert_eq!(status, 0);
        assert_eq!(output, b"unset:unset");
        assert_exact_leader_reaped(directory.path());
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_system_stream_rejects_stdout_and_stderr_overflow_and_reaps() {
        for stderr in [false, true] {
            let directory = tempfile::tempdir().unwrap();
            let script = if stderr {
                r#"printf '%s\n' "$$" > "$1"; i=0; while [ "$i" -lt 8192 ]; do printf 0123456789abcdef >&2; i=$((i+1)); done"#
            } else {
                r#"printf '%s\n' "$$" > "$1"; i=0; while [ "$i" -lt 8192 ]; do printf 0123456789abcdef; i=$((i+1)); done"#
            };
            let spec = owned_spec(script, directory.path());
            let mut output = Vec::new();
            assert!(
                crate::dispatch::SystemHost
                    .output_stream(&spec, Duration::from_secs(5), 64, &mut |stream| stream
                        .read_to_end(&mut output)
                        .map(|_| ()))
                    .is_err()
            );
            assert!(output.len() <= 64);
            assert_exact_leader_reaped(directory.path());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn bounded_system_stream_deadline_closes_descendant_pipes_and_reaps_leader() {
        let directory = tempfile::tempdir().unwrap();
        let script = r#"printf '%s\n' "$$" > "$1"; /bin/sh -c 'while :; do :; done' & child=$!; IFS= read -r birth < "/proc/$child/stat"; printf '%s\n' "$birth" > "$2"; wait "$child""#;
        let spec = owned_spec(script, directory.path());
        assert!(
            crate::dispatch::SystemHost
                .output_stream(&spec, Duration::from_secs(1), 64, &mut |stream| {
                    std::io::copy(stream, &mut std::io::sink()).map(|_| ())
                })
                .is_err()
        );
        assert_exact_leader_reaped(directory.path());
        let original = std::fs::read_to_string(directory.path().join("child")).unwrap();
        let (pid, _) = original.split_once(' ').unwrap();
        let original_fields = original
            .rsplit_once(')')
            .unwrap()
            .1
            .split_whitespace()
            .collect::<Vec<_>>();
        match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Ok(current) => {
                let current = current
                    .rsplit_once(')')
                    .unwrap()
                    .1
                    .split_whitespace()
                    .collect::<Vec<_>>();
                assert!(
                    current[19] != original_fields[19] || matches!(current[0], "Z" | "X"),
                    "The birth-bound original descendant must no longer execute or hold pipes."
                );
                // Only its own parent can reap this grandchild; a transient adopted zombie
                // is terminal, not claimed as an independently reaped child of the test.
            }
            Err(error) => panic!("Could not inspect the exact fixture descendant: {error}"),
        }
    }
}
