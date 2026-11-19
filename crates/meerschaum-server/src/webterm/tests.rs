use super::*;

#[test]
fn terminals_never_carry_the_servers_job_context() {
    let vars = job_context_vars();
    for name in [
        "MRSM_SYSTEMD_LOG_PATH",
        "MRSM_SYSTEMD_RESULT_PATH",
        "MRSM_SYSTEMD_STDIN_PATH",
        "MRSM_SYSTEMD_DELETE_JOB",
        "MRSM_SYSTEMD_JOB_CHILD",
        "MRSM_DAEMON_ID",
        "MRSM__COMPOSE_PROJECT",
        "MRSM__COMPOSE_ROOT",
        "MRSM_JOB_RESULT_PATH",
        "MRSM_JOB_SUPERVISOR",
        "INVOCATION_ID",
        "JOURNAL_STREAM",
        // Every job's environment sets it; the terminal's shell would lose its editor.
        "MRSM_NONINTERACTIVE",
        // An unattended restart's environment; the shell would answer no question.
        "MRSM_NOASK",
    ] {
        assert!(vars.iter().any(|var| var == name), "{name} must be removed");
    }
    // The shell's own configuration stays.
    for name in [
        "MRSM_ROOT_DIR",
        "MRSM_PLUGINS_DIR",
        "MRSM_CONFIG",
        "MRSM_PATCH",
        "MRSM_SQL_MAIN",
        "MRSM_PYTHON",
        "PATH",
        "HOME",
    ] {
        assert!(!vars.iter().any(|var| var == name), "{name} must be kept");
    }
    assert!(vars.iter().all(|var| valid_env_name(var)));

    let mut builder = CommandBuilder::new("sh");
    builder.env("MRSM_SYSTEMD_LOG_PATH", "/job/log");
    builder.env("MRSM_DAEMON_ID", "api");
    builder.env("MRSM_SQL_MAIN", "sqlite:///main.db");
    scrub_job_context(&mut builder);
    assert!(builder.get_env("MRSM_SYSTEMD_LOG_PATH").is_none());
    assert!(builder.get_env("MRSM_DAEMON_ID").is_none());
    assert!(builder.get_env("MRSM_SQL_MAIN").is_some());
}

#[test]
fn the_tmux_window_unsets_the_job_context_and_its_env_file_omits_it() {
    let dir = tempfile::tempdir().unwrap();
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: None,
        session_suffix: String::new(),
        env_dir: dir.path().join("env"),
        config_file: None,
    };
    let mut shell = ShellCommand::new("mrsm");
    // Explicit overrides are scrubbed too; ordinary ones are written.
    shell.env.push(("MRSM_DAEMON_ID".into(), "api".into()));
    shell
        .env
        .push(("MRSM_SYSTEMD_LOG_PATH".into(), "/job/log".into()));
    shell.env.push(("MRSM_RC25_KEEP".into(), "yes".into()));
    let argv = tmux_shell_argv(&tmux, &shell).unwrap();
    assert!(argv[2].contains("unset "), "{}", argv[2]);
    for var in job_context_vars() {
        assert!(
            argv[2]
                .split(' ')
                .any(|word| word.trim_end_matches(';') == var),
            "{var}"
        );
    }
    assert!(argv[2].ends_with("exec \"$@\""), "{}", argv[2]);
    let body = std::fs::read_to_string(&argv[3]).unwrap();
    assert!(body.contains("MRSM_RC25_KEEP='yes'"), "{body}");
    for var in job_context_vars() {
        assert!(
            !body
                .lines()
                .any(|line| line.starts_with(&format!("{var}="))),
            "{var}"
        );
    }
}

#[test]
#[cfg(unix)]
fn an_isolated_tmux_window_uses_only_its_supplied_environment() {
    let root = tempfile::tempdir().unwrap();
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: None,
        session_suffix: String::new(),
        env_dir: root.path().join("env"),
        config_file: None,
    };
    let mut shell = ShellCommand::new("/bin/sh");
    shell.clear_env = true;
    shell.env = vec![
        ("PATH".into(), "/bin:/usr/bin".into()),
        ("TPR_CURRENT_CONTEXT".into(), "current".into()),
    ];
    shell.args = vec!["-c".into(), "env".into()];
    let argv = tmux_shell_argv(&tmux, &shell).unwrap();
    // Exercise the actual window wrapper without starting any tmux server.
    let output = std::process::Command::new(&argv[0])
        .args(&argv[1..])
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .env(
            "TPR_UNEXPECTED_PARENT_CREDENTIAL",
            "synthetic-parent-secret",
        )
        .output()
        .unwrap();
    assert!(output.status.success());
    let environment = String::from_utf8(output.stdout).unwrap();
    assert!(
        environment
            .lines()
            .any(|line| line == "TPR_CURRENT_CONTEXT=current")
    );
    assert!(!environment.contains("TPR_UNEXPECTED_PARENT_CREDENTIAL"));
    assert!(!environment.contains("synthetic-parent-secret"));
    assert!(!environment.contains("CARGO_MANIFEST_DIR"));
}

#[test]
fn a_restricted_shell_carries_the_servers_restriction_and_no_python_escape() {
    let mut shell = ShellCommand::new("mrsm");
    // Whatever the server's own command carries is replaced.
    shell.env.push((RESTRICTED_ENV.into(), "".into()));
    shell.env.push((GRANT_ENV.into(), "old".into()));
    shell.env.push(("PYTHONINSPECT".into(), "1".into()));
    let restricted = restricted_shell(&shell, "unix:/p.sock", "g");
    let get = |key: &str| -> Vec<&str> {
        restricted
            .env
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.as_str())
            .collect()
    };
    assert_eq!(get(RESTRICTED_ENV), ["1"]);
    assert_eq!(get(POLICY_ADDRESS_ENV), ["unix:/p.sock"]);
    assert_eq!(get(GRANT_ENV), ["g"]);
    assert_eq!(get("PYTHONINSPECT"), [""]);
    assert_eq!(get("PYTHONSTARTUP"), [""]);
    assert_eq!(get("PYTHONBREAKPOINT"), ["0"]);
    // Restricted sessions never use the default tmux server.
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: None,
        session_suffix: String::new(),
        env_dir: PathBuf::from("/e"),
        config_file: None,
    };
    assert!(restricted_tmux(&tmux).is_none());
    let tmux = TmuxSettings {
        socket: Some(PathBuf::from("/w/tmux.sock")),
        ..tmux
    };
    let restricted = restricted_tmux(&tmux).unwrap();
    assert_eq!(restricted.socket, Some(PathBuf::from("/w/tmux-r.sock")));
    assert_eq!(
        restricted.config_file.as_deref(),
        Some("/e/tmux-restricted.conf")
    );
}

#[test]
fn utf8_stream_joins_split_characters() {
    let mut stream = Utf8Stream::default();
    let arrow = "→".as_bytes();
    assert_eq!(stream.push(&arrow[..1]), "");
    assert_eq!(stream.push(&arrow[1..]), "→");
    assert_eq!(stream.push(b"ok\xff!"), "ok\u{FFFD}!");
    assert_eq!(stream.push(&arrow[..2]), "");
    assert_eq!(stream.finish(), "\u{FFFD}");
}

#[test]
fn client_messages_follow_terminado() {
    assert!(matches!(
        parse_client_message(r#"["stdin","ls\r"]"#),
        Some(ClientMessage::Stdin(s)) if s == "ls\r"
    ));
    let Some(ClientMessage::SetSize(size)) = parse_client_message(r#"["set_size",30,100,600,800]"#)
    else {
        panic!("set_size");
    };
    assert_eq!(
        (size.rows, size.cols, size.pixel_height, size.pixel_width),
        (30, 100, 600, 800)
    );
    assert!(matches!(
        parse_client_message(r#"["new_window"]"#),
        Some(ClientMessage::NewWindow)
    ));
    for ignored in [
        r#"["set_size",0,10]"#,
        r#"["stdin"]"#,
        r#"{"a":1}"#,
        "nope",
        "[]",
    ] {
        assert!(parse_client_message(ignored).is_none(), "{ignored}");
    }
}

#[test]
fn tmux_session_names_avoid_reserved_characters_and_never_collide() {
    assert_eq!(tmux_session_name("a.b:c", "_mrsm"), "a_db_cc_mrsm");
    assert_eq!(tmux_session_name("alice", "_mrsm"), "alice_mrsm");
    // Two users never share a session (`a.b` and `a_b` both were `a_b`).
    let names = ["a.b", "a_b", "a__b", "a_db", "a:b", "a_.b", "a._b"];
    let mut sessions: Vec<String> = names.iter().map(|n| tmux_session_name(n, "_s.x")).collect();
    assert!(
        sessions
            .iter()
            .all(|s| !s.contains('.') && !s.contains(':'))
    );
    sessions.sort();
    sessions.dedup();
    assert_eq!(sessions.len(), names.len());
}

#[test]
fn readiness_uses_the_exact_live_process_and_actual_buffer_state() {
    let dir = tempfile::tempdir().unwrap();
    let pid = std::process::id();
    let path = dir.path().join(format!("{pid}.json"));
    assert!(read_shell_state(dir.path(), pid).is_none());
    let start = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|text| {
            text.rsplit_once(") ")
                .and_then(|(_, fields)| fields.split_whitespace().nth(19))
                .map(str::to_string)
        })
        .unwrap_or_default();
    let mut state = json!({"pid": pid, "start": start, "idle": true, "empty": false});
    std::fs::write(&path, state.to_string()).unwrap();
    assert_eq!(
        read_shell_state(dir.path(), pid),
        Some(json!({"idle": true, "empty": false}))
    );
    state["empty"] = json!(true);
    std::fs::write(&path, state.to_string()).unwrap();
    assert_eq!(
        read_shell_state(dir.path(), pid),
        Some(json!({"idle": true, "empty": true}))
    );
    state["idle"] = json!(false);
    std::fs::write(&path, state.to_string()).unwrap();
    assert_eq!(read_shell_state(dir.path(), pid).unwrap()["idle"], false);
    state["pid"] = json!(pid + 1);
    std::fs::write(&path, state.to_string()).unwrap();
    assert!(read_shell_state(dir.path(), pid).is_none());
    #[cfg(target_os = "linux")]
    {
        state["pid"] = json!(pid);
        state["start"] = json!("stale-process");
        std::fs::write(&path, state.to_string()).unwrap();
        assert!(read_shell_state(dir.path(), pid).is_none());
    }
    std::fs::remove_file(&path).unwrap();
    assert!(read_shell_state(dir.path(), pid).is_none());
}

fn current_readiness_snapshot() -> Value {
    let pid = std::process::id();
    let start = std::fs::read_to_string("/proc/self/stat")
        .ok()
        .and_then(|text| {
            text.rsplit_once(") ")?
                .1
                .split_whitespace()
                .nth(19)
                .map(str::to_string)
        })
        .unwrap_or_default();
    json!({"pid":pid,"start":start,"idle":true,"empty":true})
}

#[cfg(unix)]
#[test]
fn readiness_refuses_outside_file_and_directory_symlinks() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let state = root.path().join("state");
    let outside = root.path().join("outside");
    std::fs::create_dir(&state).unwrap();
    std::fs::create_dir(&outside).unwrap();
    let pid = std::process::id();
    let name = format!("{pid}.json");
    std::fs::write(
        outside.join(&name),
        current_readiness_snapshot().to_string(),
    )
    .unwrap();
    assert_eq!(
        read_shell_state(&outside, pid),
        Some(json!({"idle":true,"empty":true}))
    );
    symlink(outside.join(&name), state.join(&name)).unwrap();
    assert!(read_shell_state(&state, pid).is_none());
    std::fs::remove_file(state.join(&name)).unwrap();
    std::fs::remove_dir(&state).unwrap();
    symlink(&outside, &state).unwrap();
    assert!(read_shell_state(&state, pid).is_none());
}

#[cfg(unix)]
#[test]
fn readiness_refuses_a_named_fifo_promptly_without_reading_it() {
    use rustix::fs::{CWD, Mode, mkfifoat};
    const CHILD: &str = "MRSM_TEST_SHELL_STATE_FIFO";
    if let Some(directory) = std::env::var_os(CHILD) {
        let directory = Path::new(&directory);
        let leaf = directory.join(format!("{}.json", std::process::id()));
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while std::fs::symlink_metadata(&leaf).is_err() {
            assert!(
                std::time::Instant::now() < deadline,
                "The owned FIFO was not prepared."
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(read_shell_state(directory, std::process::id()).is_none());
        std::fs::write(directory.join("finished"), "1").unwrap();
        return;
    }
    let root = tempfile::tempdir().unwrap();
    // The child waits for this owned leaf. Parent-side timeout can kill an accidental
    // blocking open/read without leaving a stuck test thread or FIFO writer behind.
    let name = format!(
        "{}::readiness_refuses_a_named_fifo_promptly_without_reading_it",
        module_path!()
    );
    let name = name.split_once("::").unwrap().1;
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--test-threads=1"])
        .env_clear()
        .env(CHILD, root.path())
        .env("MRSM_ROOT_DIR", root.path())
        .env("MRSM_PLUGINS_DIR", root.path().join("plugins"))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let path = root.path().join(format!("{}.json", child.id()));
    // The child waits for its own numeric leaf before entering the status reader.
    mkfifoat(CWD, &path, Mode::RUSR | Mode::WUSR).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    let result = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break Some(status);
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            break None;
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        result.is_some_and(|status| status.success()),
        "A FIFO status read blocked or failed."
    );
    assert!(
        root.path().join("finished").is_file(),
        "The exact FIFO reader test did not run."
    );
}

#[test]
fn readiness_refuses_oversized_regular_json_but_accepts_the_live_small_snapshot() {
    let root = tempfile::tempdir().unwrap();
    let pid = std::process::id();
    let path = root.path().join(format!("{pid}.json"));
    let mut snapshot = current_readiness_snapshot();
    std::fs::write(&path, snapshot.to_string()).unwrap();
    assert_eq!(
        read_shell_state(root.path(), pid),
        Some(json!({"idle":true,"empty":true}))
    );
    snapshot["padding"] = json!("x".repeat(MAX_SHELL_STATE_BYTES as usize));
    std::fs::write(&path, snapshot.to_string()).unwrap();
    assert!(read_shell_state(root.path(), pid).is_none());
}

#[test]
fn simultaneous_launches_never_share_an_env_temp_file() {
    let dir = std::env::temp_dir().join(format!("mrsm-webterm-env-{}", std::process::id()));
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: None,
        session_suffix: "_t".into(),
        env_dir: dir.clone(),
        config_file: None,
    };
    let shell = ShellCommand::new("sh");
    let handles: Vec<_> = (0..16)
        .map(|_| {
            let (tmux, shell) = (tmux.clone(), shell.clone());
            std::thread::spawn(move || write_env_file(&tmux, &shell))
        })
        .collect();
    let mut paths: Vec<PathBuf> = handles
        .into_iter()
        .map(|handle| handle.join().unwrap().unwrap())
        .collect();
    paths.sort();
    paths.dedup();
    // One file per launch (each window reads and removes its own), every temporary renamed
    // away.
    assert_eq!(paths.len(), 16);
    let mut names: Vec<_> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(names.len(), 16, "{names:?}");
    assert!(
        names
            .iter()
            .all(|n| n.starts_with("webterm.") && n.ends_with(".env"))
    );
    // The shared file of earlier releases is removed by the next launch.
    std::fs::write(dir.join("webterm.env"), "OLD=1\n").unwrap();
    write_env_file(&tmux, &shell).unwrap();
    assert!(!dir.join("webterm.env").exists());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn terminals_get_their_own_tmux_server_and_config() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = crate::config::ServerConfig::for_root("sql:main", root.path());
    // A dedicated socket under the root, in a private directory.
    let socket = tmux_socket_path(&cfg).unwrap();
    assert_eq!(socket, cfg.webterm_resources_path.join("tmux.sock"));
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(socket.parent().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700);
    }
    // Meerschaum's configuration (the window list on top), private, rewritten when it
    // changed.
    let conf = tmux_config_path(&cfg).unwrap();
    assert_eq!(std::fs::read_to_string(&conf).unwrap(), TMUX_CONF);
    assert!(TMUX_CONF.contains("set -g status on"));
    assert!(TMUX_CONF.contains("set -g status-position top"));
    // What a running server gets again is what the file sets.
    let lines: Vec<&str> = TMUX_CONF.lines().collect();
    for (name, value) in TMUX_WINDOW_LIST {
        let plain = format!("set -g {name} {value}");
        let quoted = format!("set -g {name} \"{value}\"");
        assert!(
            lines.iter().any(|line| *line == plain || *line == quoted),
            "{name} {value:?} is not in TMUX_CONF"
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&conf).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }
    std::fs::write(&conf, "set -g status on\n").unwrap();
    assert_eq!(
        std::fs::read_to_string(tmux_config_path(&cfg).unwrap()).unwrap(),
        TMUX_CONF
    );
    // `api:webterm:tmux:config` names another file, which must exist.
    let custom = root.path().join("mine.conf");
    cfg.webterm_tmux_config = Some(custom.clone());
    assert!(tmux_config_path(&cfg).is_err());
    std::fs::write(&custom, "set -g status on\n").unwrap();
    assert_eq!(tmux_config_path(&cfg).unwrap(), custom);
    // A root too long for a socket path gets a short one in the temporary directory.
    cfg.webterm_resources_path = root.path().join("x".repeat(120));
    let short = tmux_socket_path(&cfg).unwrap();
    assert!(short.as_os_str().len() <= MAX_SOCKET_PATH + 64, "{short:?}");
    assert!(short.starts_with(std::env::temp_dir()));
    assert_eq!(short, tmux_socket_path(&cfg).unwrap());
}

#[test]
fn env_values_are_quoted_for_sh() {
    assert_eq!(sh_quote("it's"), "'it'\\''s'");
    assert!(valid_env_name("MRSM_SQL_MAIN"));
    assert!(!valid_env_name("1X"));
    assert!(!valid_env_name("A-B"));
}
