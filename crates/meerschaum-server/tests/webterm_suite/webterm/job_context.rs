//! A terminal never carries the server's job context, plain or under tmux, and `mrsm` stays
//! interactive on a jobs server.

use super::*;

/// What a `systemd` job's unit gives the server (the API running as a Meerschaum job), plus its
/// Compose stamp. The paths never exist: a shell that took them as its job would fail.
const JOB_ENV: &[(&str, &str)] = &[
    ("MRSM_DAEMON_ID", "rc25-api"),
    ("MRSM_SYSTEMD_LOG_PATH", "/nonexistent/rc25-api.log"),
    (
        "MRSM_SYSTEMD_RESULT_PATH",
        "/nonexistent/rc25-api.result.json",
    ),
    ("MRSM_SYSTEMD_STDIN_PATH", "/nonexistent/rc25-api.stdin"),
    ("MRSM_SYSTEMD_DELETE_JOB", "false"),
    ("INVOCATION_ID", "rc25invocation"),
    ("JOURNAL_STREAM", "8:1234"),
    ("MRSM__COMPOSE_PROJECT", "rc25"),
    // Every job's environment (`meerschaum_jobs::Job::env`): a shell with it has no editor.
    ("MRSM_NONINTERACTIVE", "true"),
    // A server restarted by an unattended script carries it; a shell with it asks nothing
    // (`python` exits at once, `bootstrap pipes` refuses to ask).
    ("MRSM_NOASK", "1"),
];

/// The shell prints each job variable (or `-` when unset) and a kept variable.
const JOB_ENV_PROBE: &str = "echo ctx:${MRSM_DAEMON_ID--}:${MRSM_SYSTEMD_LOG_PATH--}:${MRSM_SYSTEMD_RESULT_PATH--}:${MRSM_SYSTEMD_STDIN_PATH--}:${MRSM_SYSTEMD_DELETE_JOB--}:${INVOCATION_ID--}:${JOURNAL_STREAM--}:${MRSM__COMPOSE_PROJECT--}:${MRSM_NONINTERACTIVE--}:${MRSM_NOASK--} keep:$MRSM_RC25_KEEP\n";
const JOB_ENV_CLEAN: &str = "ctx:-:-:-:-:-:-:-:-:-:- keep:kept";

/// On a server that runs as a Meerschaum `systemd` job, a terminal's `mrsm` once inherited the
/// job's variables, wrapped itself as that job ("an interactive shell cannot start in a job") and
/// the terminal showed `[CLOSED]`. The terminal's shell never carries the job context, also when
/// the caller passes it explicitly; its own configuration stays.
#[tokio::test]
async fn a_plain_terminal_never_carries_the_servers_job_context() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let mut shell = sh();
    for (key, value) in JOB_ENV {
        shell.env.push((key.to_string(), value.to_string()));
    }
    shell.env.push(("MRSM_RC25_KEEP".into(), "kept".into()));
    let addr = serve(webterm::routes(state.clone(), config(&state, shell, None))).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    send_json(&mut socket, json!(["stdin", JOB_ENV_PROBE])).await;
    let out = read_until(&mut socket, |out| has_line_matching(out, "ctx:", "")).await;
    assert!(has_line(&out, JOB_ENV_CLEAN), "{}", plain(&out));
}

/// The same on the tmux path, where the shell starts in a window of the terminal's own tmux
/// server: a server already running with the job's variables in its global environment (started
/// by an earlier release, or from the job) loses them, and neither the shell nor a new window gets
/// them.
#[tokio::test]
async fn a_tmux_terminal_never_carries_the_servers_job_context() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP a_tmux_terminal_never_carries_the_servers_job_context: no tmux");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let tmux_dir = short_tmp();
    let _server = PrivateTmux(tmux_dir.path().to_path_buf());
    let tmux_cmd = |args: &[&str]| {
        std::process::Command::new("tmux")
            .arg("-S")
            .arg(tmux_socket(tmux_dir.path()))
            .args(args)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap()
    };
    // The dedicated server already runs, started from the job's environment.
    let started = std::process::Command::new("tmux")
        .arg("-S")
        .arg(tmux_socket(tmux_dir.path()))
        .args([
            "-f",
            "/dev/null",
            "new-session",
            "-d",
            "-s",
            "rc25_pre",
            "sh",
        ])
        .envs(JOB_ENV.iter().copied())
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output()
        .unwrap();
    assert!(started.status.success(), "{started:?}");
    let global = String::from_utf8(tmux_cmd(&["show-environment", "-g"]).stdout).unwrap();
    assert!(
        global.lines().any(|l| l == "MRSM_DAEMON_ID=rc25-api"),
        "{global}"
    );

    let state = state(root.path());
    let mut shell = sh();
    shell.env.push(("MRSM_RC25_KEEP".into(), "kept".into()));
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: Some(tmux_socket(tmux_dir.path())),
        session_suffix: "_t".into(),
        env_dir: root.path().join("webterm"),
        config_file: Some("/dev/null".into()),
    };
    let cfg = config(&state, shell, Some(tmux));
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    send_json(&mut socket, json!(["stdin", JOB_ENV_PROBE])).await;
    let out = read_until(&mut socket, |out| has_line_matching(out, "ctx:", "")).await;
    assert!(has_line(&out, JOB_ENV_CLEAN), "{}", plain(&out));

    // The server's global environment no longer holds them, and a new window starts clean.
    let global = String::from_utf8(tmux_cmd(&["show-environment", "-g"]).stdout).unwrap();
    for (key, _) in JOB_ENV {
        assert!(
            !global.lines().any(|l| l.starts_with(&format!("{key}="))),
            "{key}: {global}"
        );
    }
    send_json(&mut socket, json!(["new_window"])).await;
    for _ in 0..50 {
        let windows =
            String::from_utf8(tmux_cmd(&["list-windows", "-t", "admin_t"]).stdout).unwrap();
        if windows.lines().count() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    send_json(&mut socket, json!(["stdin", JOB_ENV_PROBE])).await;
    // The new window is the active one: the probe answers there (tmux repaints on the switch).
    let out = read_until(&mut socket, |out| has_line_matching(out, "ctx:", "")).await;
    assert!(
        plain(&out)
            .split(['\r', '\n'])
            .filter(|l| l.starts_with("ctx:"))
            .all(|l| l == JOB_ENV_CLEAN),
        "{}",
        plain(&out)
    );
    drop(socket);
}

/// A dedicated server whose user session itself holds job variables (an `update-environment`
/// setting, or an explicit `set-environment -t`) loses them too: a window opened afterwards with
/// tmux's own commands (not the terminal's wrapper) starts without them.
#[tokio::test]
async fn a_tmux_sessions_own_job_context_is_unset() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP a_tmux_sessions_own_job_context_is_unset: no tmux");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let tmux_dir = short_tmp();
    let _server = PrivateTmux(tmux_dir.path().to_path_buf());
    let tmux_cmd = |args: &[&str]| {
        let mut command = std::process::Command::new("tmux");
        command
            .arg("-S")
            .arg(tmux_socket(tmux_dir.path()))
            .args(args)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE");
        for (key, _) in JOB_ENV {
            command.env_remove(key);
        }
        command.output().unwrap()
    };
    // The user's session exists (its shell started clean), then gains job variables of its own.
    assert!(
        tmux_cmd(&[
            "-f",
            "/dev/null",
            "new-session",
            "-d",
            "-s",
            "admin_t",
            "sh"
        ])
        .status
        .success()
    );
    for (key, value) in JOB_ENV {
        assert!(
            tmux_cmd(&["set-environment", "-t", "=admin_t", key, value])
                .status
                .success()
        );
    }
    let session =
        String::from_utf8(tmux_cmd(&["show-environment", "-t", "=admin_t"]).stdout).unwrap();
    assert!(
        session.lines().any(|l| l == "MRSM_DAEMON_ID=rc25-api"),
        "{session}"
    );

    let state = state(root.path());
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: Some(tmux_socket(tmux_dir.path())),
        session_suffix: "_t".into(),
        env_dir: root.path().join("webterm"),
        config_file: Some("/dev/null".into()),
    };
    let cfg = config(&state, sh(), Some(tmux));
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    send_json(&mut socket, json!(["stdin", "echo attached\n"])).await;
    read_until(&mut socket, |out| has_line(out, "attached")).await;

    let session =
        String::from_utf8(tmux_cmd(&["show-environment", "-t", "=admin_t"]).stdout).unwrap();
    for (key, _) in JOB_ENV {
        assert!(
            !session.lines().any(|l| l.starts_with(&format!("{key}="))),
            "{key}: {session}"
        );
    }
    // A window tmux opens by itself (as its own keys would) gets neither scope's job variables.
    let dump = root.path().join("window-env.txt");
    let script = format!("env > '{}'", dump.display());
    assert!(
        tmux_cmd(&["new-window", "-d", "-t", "=admin_t", script.as_str()])
            .status
            .success()
    );
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !dump.exists()
        || std::fs::read_to_string(&dump)
            .unwrap_or_default()
            .is_empty()
    {
        assert!(std::time::Instant::now() < deadline, "the window never ran");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(200)).await;
    let env = std::fs::read_to_string(&dump).unwrap();
    for (key, _) in JOB_ENV {
        assert!(
            !env.lines().any(|l| l.starts_with(&format!("{key}="))),
            "{key}: {env}"
        );
    }
    drop(socket);
}

/// End to end through the terminal's route: the real `mrsm` shell in a server
/// whose environment is a `systemd` job's stays interactive, and the job's log, result and FIFO
/// are untouched. Unscrubbed, the shell wraps itself as the job and the page shows `[CLOSED]`.
/// Workspace tests build `mrsm-cli` beside this test's `deps` directory. `MRSM_TEST_BIN` may
/// override it; only local single-crate runs without either binary may skip this regression.
#[tokio::test]
async fn the_mrsm_shell_stays_interactive_on_a_jobs_server() {
    let explicit = std::env::var_os("MRSM_TEST_BIN");
    let binary = explicit
        .as_ref()
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::current_exe()
                .expect("integration test executable")
                .parent()
                .and_then(Path::parent)
                .expect("integration test profile directory")
                .join("mrsm-cli")
        });
    if !binary.is_file() {
        assert!(
            explicit.is_none() && std::env::var_os("CI").is_none(),
            "the real CLI regression requires a built mrsm-cli or valid MRSM_TEST_BIN: {}",
            binary.display()
        );
        eprintln!(
            "SKIP the_mrsm_shell_stays_interactive_on_a_jobs_server: run workspace tests or set MRSM_TEST_BIN"
        );
        return;
    }
    eprintln!("webterm CLI regression uses {}", binary.display());
    let root = tempfile::tempdir().unwrap();
    let job = tempfile::tempdir().unwrap();
    let fifo = job.path().join("rc25-api.stdin");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&fifo)
            .status()
            .unwrap()
            .success()
    );
    let log = job.path().join("rc25-api.log");
    let result = job.path().join("rc25-api.result.json");
    let mut shell = ShellCommand::new(binary.to_string_lossy());
    shell.args = vec!["-i".into(), "sql:local".into()];
    for (key, value) in [
        (
            "MRSM_ROOT_DIR",
            root.path().join("root").display().to_string(),
        ),
        (
            "MRSM_PLUGINS_DIR",
            root.path().join("plugins").display().to_string(),
        ),
        ("MRSM_NO_UPDATE_CHECK", "1".into()),
        ("MRSM_NO_AUTO_INSTALL", "1".into()),
        ("HOME", root.path().display().to_string()),
        // The job context, as the API's own `systemd` unit gives it.
        ("MRSM_DAEMON_ID", "rc25-api".into()),
        ("INVOCATION_ID", "rc25invocation".into()),
        ("JOURNAL_STREAM", "8:1234".into()),
        ("MRSM_SYSTEMD_LOG_PATH", log.display().to_string()),
        ("MRSM_SYSTEMD_RESULT_PATH", result.display().to_string()),
        ("MRSM_SYSTEMD_STDIN_PATH", fifo.display().to_string()),
        // What every job's environment sets (`meerschaum_jobs::Job::env`).
        ("MRSM_NONINTERACTIVE", "true".into()),
    ] {
        shell.env.push((key.to_string(), value));
    }
    std::fs::create_dir_all(root.path().join("root")).unwrap();
    std::fs::create_dir_all(root.path().join("plugins")).unwrap();
    shell.cwd = Some(root.path().to_path_buf());

    let state = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, shell, None))).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    send_json(&mut socket, json!(["set_size", 24, 100, 480, 1000])).await;
    // The shell's line editor draws its bottom toolbar (the instance, then the executor); a job
    // wrapper prints nothing and disconnects, and the plain line reader `MRSM_NONINTERACTIVE`
    // selects draws no toolbar.
    let out = read_until(&mut socket, |out| plain(out).contains("sql:local | ")).await;
    assert!(!out.contains("cannot start in a job"), "{out:?}");
    // Still running two seconds later: no `["disconnect", 1]`, no close.
    let quiet = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) if text.as_str().contains("disconnect") => {
                    return Some(text.to_string());
                }
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return Some("closed".into()),
                Some(Ok(_)) => continue,
            }
        }
    })
    .await;
    assert!(quiet.is_err(), "the shell ended: {quiet:?}");
    // The console's Exec button asks whether the shell waits at an empty prompt; the
    // editor's shell says so (the plain line reader never did, so Exec was always refused).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    loop {
        send_json(&mut socket, json!(["shell_state", "rc28"])).await;
        let answer = loop {
            let message = next_message(&mut socket).await.expect("an open terminal");
            if message[0] == "shell_state" && message[1] == "rc28" {
                break message[2].clone();
            }
        };
        if answer == json!({"idle": true, "empty": true}) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the shell never reported an idle, empty prompt: {answer}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    use std::os::unix::fs::FileTypeExt;
    assert!(
        !log.exists() && !result.exists(),
        "the shell acted as the job"
    );
    assert!(
        std::fs::symlink_metadata(&fifo)
            .unwrap()
            .file_type()
            .is_fifo()
    );
    drop(socket);
}
