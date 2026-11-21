//! Real PTY lifetime checks. API-profile confinement and policy have separate process fixtures.

use meerschaum_jobs::process::{self, ProcState, Sig};
use portable_pty::{CommandBuilder, PtySize, native_pty_system};
use serde_json::json;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Fixture {
    _root: tempfile::TempDir,
    owner: Child,
    bystander: Child,
    pane: Box<dyn portable_pty::Child + Send + Sync>,
    guardian: (i32, chrono::NaiveDateTime),
    _pty: Box<dyn portable_pty::MasterPty + Send>,
    spec: PathBuf,
    worker: Option<(i32, chrono::NaiveDateTime)>,
}

impl Fixture {
    fn start(expiry: i64) -> Self {
        let root = tempfile::tempdir().unwrap();
        let state = root.path().join("state");
        std::fs::create_dir(&state).unwrap();
        let spec = root.path().join("sandbox.json");
        // Lifetime is independent of the kernel profile. Only this owned native core command
        // uses legacy off mode; signed Api terminals use their mandatory prepared profile.
        std::fs::write(
            &spec,
            json!({"mode":"off","team":"guardian fixture","rules":[],
            "protected":[],"canary":root.path().join("canary")})
            .to_string(),
        )
        .unwrap();
        let config = root.path().join("config.json");
        std::fs::write(
            &config,
            json!({"meerschaum":{"instance":"sql:main","connectors":{
                "sql":{"main":{"flavor":"sqlite","database":state.join("main.db")}}
            }}})
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            root.path().join("credential.json"),
            json!({
                "token":"synthetic-guardian-fixture","expires_at":expiry
            })
            .to_string(),
        )
        .unwrap();
        let sleep = || {
            Command::new("/bin/sleep")
                .arg("180")
                .env_clear()
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap()
        };
        let owner = sleep();
        let bystander = sleep();
        let pid = i32::try_from(owner.id()).unwrap();
        let birth = process::process_start_time(pid).unwrap();
        let pair = native_pty_system()
            .openpty(PtySize {
                rows: 24,
                cols: 100,
                pixel_width: 0,
                pixel_height: 0,
            })
            .unwrap();
        let mut command = CommandBuilder::new(env!("CARGO_BIN_EXE_mrsm-cli"));
        command.env_clear();
        command.env("PATH", "/usr/bin:/bin");
        command.env("TERM", "xterm-256color");
        command.env("MRSM_ROOT_DIR", &state);
        command.env("MRSM_CONFIG_FILE", config);
        command.env("MRSM_PLUGINS_DIR", state.join("plugins"));
        command.env("MRSM_NO_UPDATE_CHECK", "1");
        command.env("MRSM_NONINTERACTIVE", "1");
        command.env("MRSM_TEAM_SANDBOX", &spec);
        let pending = meerschaum_jobs::supervisor::prepare_terminal_launch(
            &spec,
            &serde_json::from_value::<meerschaum_jobs::supervisor::LifecycleOwner>(
                json!({"pid":pid,"started_at":birth}),
            )
            .unwrap(),
        )
        .unwrap();
        command.env("MRSM_TERMINAL_LAUNCH", pending);
        command.env(
            meerschaum_core::team_scope::sandbox::runtime::LIFECYCLE_VAR,
            "terminal",
        );
        command.cwd(&state);
        command.args([
            "__mrsm_terminal_guard__",
            &json!({"pid":pid,"started_at":birth}).to_string(),
            spec.to_str().unwrap(),
            "--",
            "show",
            "version",
            "--loop",
            "--min-seconds",
            "30",
        ]);
        let pane = pair.slave.spawn_command(command).unwrap();
        let guardian_pid = i32::try_from(pane.process_id().unwrap()).unwrap();
        let guardian = (
            guardian_pid,
            process::process_start_time(guardian_pid).unwrap(),
        );
        drop(pair.slave);
        Self {
            _root: root,
            owner,
            bystander,
            pane,
            guardian,
            _pty: pair.master,
            spec,
            worker: None,
        }
    }

    fn worker(&mut self) -> i32 {
        let pane = i32::try_from(self.pane.process_id().unwrap()).unwrap();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if let Some(pid) = meerschaum_jobs::supervisor::terminal_child_pid(pane, &self.spec) {
                self.worker = Some((pid, process::process_start_time(pid).unwrap()));
                return pid;
            }
            assert!(
                self.pane.try_wait().unwrap().is_none(),
                "The native guardian exited before its worker."
            );
            assert!(
                Instant::now() < deadline,
                "The native guardian did not record its owned worker."
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    fn ended(&mut self, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            let worker_gone = self.worker.is_none_or(|(pid, birth)| {
                process::process_start_time(pid) != Some(birth)
                    || matches!(
                        process::proc_state(pid),
                        ProcState::Gone | ProcState::Zombie
                    )
            });
            if self.pane.try_wait().unwrap().is_some() && worker_gone {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "The native terminal outlived its runtime authority."
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        assert!(
            self.bystander.try_wait().unwrap().is_none(),
            "Guardian reached an unrelated fixture process."
        );
        let pane = i32::try_from(self.pane.process_id().unwrap()).unwrap();
        assert!(meerschaum_jobs::supervisor::terminal_child_pid(pane, &self.spec).is_none());
        assert!(meerschaum_jobs::supervisor::terminal_launches_inactive(&self.spec).unwrap());
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        if process::process_start_time(self.guardian.0) == Some(self.guardian.1) {
            let _ = process::send_signal(self.guardian.0, Sig::Term);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        while self.pane.try_wait().ok().flatten().is_none() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        if let Some((pid, birth)) = self.worker
            && process::process_start_time(pid) == Some(birth)
        {
            let _ = process::send_signal(pid, Sig::Kill);
        }
        let _ = self.pane.kill();
        let _ = self.pane.wait();
        if let Some((pid, _)) = self.worker {
            // In the killed-guardian case this fixture is the worker's subreaper.
            // Reap only its exact adopted PID; never collect another fixture's child.
            let deadline = Instant::now() + Duration::from_secs(3);
            while let Some(pid) = rustix::process::Pid::from_raw(pid) {
                match rustix::process::waitpid(Some(pid), rustix::process::WaitOptions::NOHANG) {
                    Ok(None) if Instant::now() < deadline => {
                        std::thread::sleep(Duration::from_millis(10));
                    }
                    _ => break,
                }
            }
        }
        let _ = self.owner.kill();
        let _ = self.owner.wait();
        let _ = self.bystander.kill();
        let _ = self.bystander.wait();
    }
}

#[test]
fn api_owner_death_stops_running_and_paused_terminal_workers_without_reaching_bystanders() {
    for paused in [false, true] {
        let mut fixture = Fixture::start(chrono::Utc::now().timestamp() + 30);
        let worker = fixture.worker();
        if paused {
            process::send_signal(worker, Sig::Stop).unwrap();
            let deadline = Instant::now() + Duration::from_secs(2);
            while process::proc_state(worker) != ProcState::Stopped {
                assert!(Instant::now() < deadline);
                std::thread::sleep(Duration::from_millis(10));
            }
        }
        fixture.owner.kill().unwrap();
        fixture.owner.wait().unwrap();
        fixture.ended(Duration::from_secs(5));
    }
}

#[test]
fn unchanged_expired_credential_stops_terminal_even_while_its_api_owner_is_alive() {
    let mut fixture = Fixture::start(chrono::Utc::now().timestamp() + 1);
    fixture.worker();
    fixture.ended(Duration::from_secs(65));
    assert!(fixture.owner.try_wait().unwrap().is_none());
}

#[test]
fn killed_guardian_without_cleanup_is_not_terminal_retirement_proof() {
    struct Subreaper(Option<rustix::process::Pid>);
    impl Drop for Subreaper {
        fn drop(&mut self) {
            let _ = rustix::process::set_child_subreaper(self.0);
        }
    }
    let _subreaper = Subreaper(rustix::process::child_subreaper().unwrap());
    rustix::process::set_child_subreaper(rustix::process::Pid::from_raw(1)).unwrap();
    let mut fixture = Fixture::start(chrono::Utc::now().timestamp() + 30);
    let worker = fixture.worker();
    let guardian = i32::try_from(fixture.pane.process_id().unwrap()).unwrap();
    process::send_signal(guardian, Sig::Kill).unwrap();
    fixture.pane.wait().unwrap();
    if let Some((pid, birth)) = fixture.worker
        && pid == worker
        && process::process_start_time(pid) == Some(birth)
    {
        let _ = process::send_signal(pid, Sig::Kill);
    }
    assert!(!meerschaum_jobs::supervisor::terminal_launches_inactive(&fixture.spec).unwrap());
}

#[test]
fn team_terminal_keeps_the_selected_standard_streams_across_native_exec() {
    // Read kernel bookkeeping for our own child after its native exec. Neither a tmux
    // server nor a plugin is required. The extra files live entirely inside this fixture.
    let root = tempfile::tempdir().unwrap();
    let names = ["stdin", "stdout", "stderr", "first-extra", "second-extra"];
    for name in names {
        std::fs::write(root.path().join(name), b"owned terminal fixture").unwrap();
    }
    let args = ["show", "version", "--loop", "--min-seconds", "30"];
    let mut command = Command::new("/bin/sh");
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", root.path())
        .env("MRSM_ROOT_DIR", root.path())
        .env("MRSM_NONINTERACTIVE", "1")
        .env("MRSM_NO_UPDATE_CHECK", "1")
        .current_dir(root.path())
        .stdin(std::fs::File::open(root.path().join("stdin")).unwrap())
        .stdout(std::fs::File::create(root.path().join("stdout")).unwrap())
        .stderr(std::fs::File::create(root.path().join("stderr")).unwrap())
        .args([
            "-c",
            r#"exec 8<first-extra 9<second-extra; exec "$@""#,
            "owned-team-terminal",
            env!("CARGO_BIN_EXE_mrsm-cli"),
            "__mrsm_team_terminal_exec__",
        ])
        .args(args);
    struct OwnedShell(Child);
    impl Drop for OwnedShell {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = OwnedShell(command.spawn().unwrap());
    let proc = PathBuf::from(format!("/proc/{}", child.0.id()));
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        assert_eq!(child.0.try_wait().unwrap(), None);
        let cmdline = std::fs::read(proc.join("cmdline")).unwrap();
        let argv: Vec<_> = cmdline
            .strip_suffix(&[0])
            .unwrap_or(&cmdline)
            .split(|byte| *byte == 0)
            .collect();
        if argv.first().copied() == Some(b"/proc/self/exe".as_slice())
            && argv
                .iter()
                .skip(1)
                .copied()
                .eq(args.iter().map(|arg| arg.as_bytes()))
        {
            break;
        }
        assert!(Instant::now() < until, "The Team shell did not re-exec.");
        std::thread::sleep(Duration::from_millis(5));
    }
    for (fd, name) in names.iter().take(3).enumerate() {
        assert_eq!(
            std::fs::read_link(proc.join(format!("fd/{fd}"))).unwrap(),
            root.path().join(name)
        );
    }
    for (fd, name) in [(8, "first-extra"), (9, "second-extra")] {
        assert_ne!(
            std::fs::read_link(proc.join(format!("fd/{fd}"))).ok(),
            Some(root.path().join(name))
        );
    }
}
