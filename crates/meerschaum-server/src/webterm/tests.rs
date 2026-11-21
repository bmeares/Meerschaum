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
    let mux = TmuxMux::new(tmux, TmuxProfile::Full);
    let argv = mux.shell_argv(&shell).unwrap();
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
    let mux = TmuxMux::new(tmux, TmuxProfile::Full);
    let argv = mux.shell_argv(&shell).unwrap();
    assert_eq!(argv[0], env_program());
    assert_eq!(argv[1], "-i");
    assert_eq!(argv[2], "/bin/sh");
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
        Some(remote::Input::Stdin(s)) if s == "ls\r"
    ));
    let Some(remote::Input::Resize {
        rows,
        cols,
        height,
        width,
    }) = parse_client_message(r#"["set_size",30,100,600,800]"#)
    else {
        panic!("set_size");
    };
    assert_eq!((rows, cols, height, width), (30, 100, 600, 800));
    assert!(matches!(
        parse_client_message(r#"["new_window"]"#),
        Some(remote::Input::NewWindow)
    ));
    for ignored in [
        r#"["set_size",0,10]"#,
        r#"["stdin"]"#,
        r#"["shell_state",""]"#,
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
    let mux = TmuxMux::new(tmux, TmuxProfile::Full);
    let shell = ShellCommand::new("sh");
    let handles: Vec<_> = (0..16)
        .map(|_| {
            let (mux, shell) = (mux.clone(), shell.clone());
            std::thread::spawn(move || mux.write_env_file(&shell))
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
    mux.write_env_file(&shell).unwrap();
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

// --- Fast in-memory bridge & multiplexer unit tests (zero PTY / zero tmux) ---

mod in_memory_bridge {
    use std::collections::HashMap;
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    use std::task::{Context, Poll};

    use futures_util::{Sink, Stream};

    use super::*;

    type SendFuture =
        Pin<Box<dyn Future<Output = Result<(), mpsc::error::SendError<Message>>> + Send>>;

    /// In-memory WebSocket transport backed by bounded `tokio::sync::mpsc` channels so tests
    /// exercise real async backpressure without sockets, PTYs, or tmux.
    struct MemorySocket {
        incoming: mpsc::Receiver<Message>,
        outgoing: mpsc::Sender<Message>,
        pending_send: Option<SendFuture>,
    }

    struct MemoryClient {
        to_server: mpsc::Sender<Message>,
        from_server: mpsc::Receiver<Message>,
    }

    fn memory_socket_pair(
        to_server_cap: usize,
        to_client_cap: usize,
    ) -> (MemorySocket, MemoryClient) {
        let (to_server, incoming) = mpsc::channel(to_server_cap);
        let (outgoing, from_server) = mpsc::channel(to_client_cap);
        (
            MemorySocket {
                incoming,
                outgoing,
                pending_send: None,
            },
            MemoryClient {
                to_server,
                from_server,
            },
        )
    }

    impl Stream for MemorySocket {
        type Item = Result<Message, ()>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.incoming.poll_recv(cx).map(|opt| opt.map(Ok))
        }
    }

    impl Sink<Message> for MemorySocket {
        type Error = ();

        fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
            self.as_mut().poll_flush(cx)
        }

        fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), ()> {
            let tx = self.outgoing.clone();
            self.pending_send = Some(Box::pin(async move { tx.send(item).await }));
            Ok(())
        }

        fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
            let Some(fut) = self.pending_send.as_mut() else {
                return Poll::Ready(Ok(()));
            };
            match fut.as_mut().poll(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Ok(())) => {
                    self.pending_send = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(_)) => {
                    self.pending_send = None;
                    Poll::Ready(Err(()))
                }
            }
        }

        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), ()>> {
            self.poll_flush(cx)
        }
    }

    fn fast_timeouts(reauthorize_every: Duration) -> BridgeTimeouts {
        BridgeTimeouts {
            write: Duration::from_secs(2),
            close: Duration::from_secs(2),
            flow: Duration::from_secs(2),
            ping: Duration::from_secs(60),
            reauthorize_every,
            reauthorize_timeout: Duration::from_millis(200),
        }
    }

    fn parse_text(message: Message) -> Value {
        match message {
            Message::Text(raw) => serde_json::from_str(raw.as_str()).expect("json text frame"),
            other => panic!("expected text frame, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn framing_forwards_all_terminado_commands_events_and_pongs() {
        let (socket, mut client) = memory_socket_pair(16, 16);
        let (input_tx, mut input_rx) = mpsc::channel(16);
        let (event_tx, event_rx) = mpsc::channel(16);
        let session = TerminalSession::new(
            remote::Terminal {
                input: input_tx,
                events: event_rx,
            },
            RevokeAction::None,
        );
        let (_drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
        let bridge = tokio::spawn(async move {
            bridge_session(
                socket,
                session,
                async {
                    let _ = drain_rx.await;
                },
                fast_timeouts(Duration::from_secs(60)),
                || async { None },
                "alice",
            )
            .await;
        });

        event_tx.send(remote::Event::Setup).await.unwrap();
        assert_eq!(
            parse_text(client.from_server.recv().await.unwrap()),
            json!(["setup", {}])
        );

        // Send every valid client command plus an invalid frame (ignored) and a Ping.
        for raw in [
            json!(["stdin", "ls\r"]).to_string(),
            json!(["set_size", 40, 120, 800, 1200]).to_string(),
            json!(["new_window"]).to_string(),
            json!(["shell_state", "req-1"]).to_string(),
            "not json".to_string(),
        ] {
            client
                .to_server
                .send(Message::Text(raw.into()))
                .await
                .unwrap();
        }
        client
            .to_server
            .send(Message::Ping(b"hi".to_vec().into()))
            .await
            .unwrap();

        assert_eq!(
            input_rx.recv().await.unwrap(),
            remote::Input::Stdin("ls\r".into())
        );
        assert_eq!(
            input_rx.recv().await.unwrap(),
            remote::Input::Resize {
                rows: 40,
                cols: 120,
                height: 800,
                width: 1200,
            }
        );
        assert_eq!(input_rx.recv().await.unwrap(), remote::Input::NewWindow);
        assert_eq!(
            input_rx.recv().await.unwrap(),
            remote::Input::ShellState("req-1".into())
        );
        assert_eq!(
            client.from_server.recv().await.unwrap(),
            Message::Pong(b"hi".to_vec().into())
        );

        event_tx
            .send(remote::Event::Output("hello\r\n".into()))
            .await
            .unwrap();
        event_tx
            .send(remote::Event::ShellState {
                id: "req-1".into(),
                state: Some(remote::ShellState {
                    idle: true,
                    empty: false,
                }),
            })
            .await
            .unwrap();
        event_tx.send(remote::Event::Disconnected).await.unwrap();

        assert_eq!(
            parse_text(client.from_server.recv().await.unwrap()),
            json!(["stdout", "hello\r\n"])
        );
        assert_eq!(
            parse_text(client.from_server.recv().await.unwrap()),
            json!(["shell_state", "req-1", {"idle": true, "empty": false}])
        );
        assert_eq!(
            parse_text(client.from_server.recv().await.unwrap()),
            json!(["disconnect", 1])
        );
        assert!(matches!(
            client.from_server.recv().await.unwrap(),
            Message::Close(Some(CloseFrame { code: 1000, .. }))
        ));

        bridge.await.unwrap();
        assert!(input_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn input_backpressure_buffers_client_burst_without_dropping_or_closing() {
        let (socket, mut client) = memory_socket_pair(32, 32);
        // Intentionally tiny backend queue (capacity 2) to force bridge backpressure.
        let (input_tx, mut input_rx) = mpsc::channel(2);
        let (event_tx, event_rx) = mpsc::channel(16);
        let session = TerminalSession::new(
            remote::Terminal {
                input: input_tx,
                events: event_rx,
            },
            RevokeAction::None,
        );
        let (_drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
        let bridge = tokio::spawn(async move {
            bridge_session(
                socket,
                session,
                async {
                    let _ = drain_rx.await;
                },
                fast_timeouts(Duration::from_secs(60)),
                || async { None },
                "alice",
            )
            .await;
        });

        event_tx.send(remote::Event::Setup).await.unwrap();
        assert_eq!(
            parse_text(client.from_server.recv().await.unwrap()),
            json!(["setup", {}])
        );

        // Burst 8 commands while the backend has not yet drained its capacity-2 queue.
        for i in 0..8 {
            client
                .to_server
                .send(Message::Text(
                    json!(["stdin", format!("cmd-{i}")]).to_string().into(),
                ))
                .await
                .unwrap();
        }
        // Let the bridge fill the queue and park a pending command.
        tokio::time::sleep(Duration::from_millis(20)).await;

        let mut received = Vec::new();
        for _ in 0..8 {
            let remote::Input::Stdin(cmd) =
                tokio::time::timeout(Duration::from_secs(2), input_rx.recv())
                    .await
                    .expect("no stall")
                    .expect("bridge stayed open")
            else {
                panic!("expected stdin");
            };
            received.push(cmd);
        }
        assert_eq!(
            received,
            (0..8).map(|i| format!("cmd-{i}")).collect::<Vec<_>>()
        );

        drop(client.to_server);
        bridge.await.unwrap();
    }

    #[tokio::test]
    async fn revocation_closes_terminal_even_when_browser_stops_reading() {
        // Outbound channel of capacity 1 fills immediately when the client stops reading.
        let (socket, client) = memory_socket_pair(8, 1);
        let (input_tx, mut input_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = mpsc::channel(2);
        let session = TerminalSession::new(
            remote::Terminal {
                input: input_tx,
                events: event_rx,
            },
            RevokeAction::None,
        );
        let revoked = Arc::new(AtomicBool::new(false));
        let revoked_flag = revoked.clone();
        let (_drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
        let mut timeouts = fast_timeouts(Duration::from_millis(20));
        timeouts.close = Duration::from_millis(100);
        let bridge = tokio::spawn(async move {
            bridge_session(
                socket,
                session,
                async {
                    let _ = drain_rx.await;
                },
                timeouts,
                move || {
                    let flag = revoked_flag.clone();
                    async move {
                        flag.load(Ordering::SeqCst)
                            .then(|| "Your session has ended.".to_string())
                    }
                },
                "alice",
            )
            .await;
        });

        // Flood outbound frames without reading from `client.from_server` so writer blocks.
        let _ = event_tx.send(remote::Event::Setup).await;
        let _ = event_tx.send(remote::Event::Output("flood-1".into())).await;
        let _ = event_tx.send(remote::Event::Output("flood-2".into())).await;
        let _ = event_tx.send(remote::Event::Output("flood-3".into())).await;
        tokio::time::sleep(Duration::from_millis(30)).await;

        // Revoke the session while the writer is completely stalled.
        revoked.store(true, Ordering::SeqCst);

        // Backend input MUST close promptly without waiting for the stalled browser.
        let closed = tokio::time::timeout(Duration::from_millis(500), input_rx.recv())
            .await
            .expect("revocation dropped backend input promptly");
        assert!(closed.is_none());

        // Drain what reached the client (or finishes once client reads again).
        drop(client.from_server);
        tokio::time::timeout(Duration::from_secs(2), bridge)
            .await
            .expect("bridge exited promptly")
            .unwrap();
    }

    #[tokio::test]
    async fn revocation_delivers_reason_disconnect_and_close_1008() {
        let (socket, mut client) = memory_socket_pair(8, 16);
        let (input_tx, mut input_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = mpsc::channel(8);
        let session = TerminalSession::new(
            remote::Terminal {
                input: input_tx,
                events: event_rx,
            },
            RevokeAction::None,
        );
        let revoked = Arc::new(AtomicBool::new(false));
        let revoked_flag = revoked.clone();
        let (_drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
        let bridge = tokio::spawn(async move {
            bridge_session(
                socket,
                session,
                async {
                    let _ = drain_rx.await;
                },
                fast_timeouts(Duration::from_millis(20)),
                move || {
                    let flag = revoked_flag.clone();
                    async move {
                        flag.load(Ordering::SeqCst)
                            .then(|| "Your permission was removed.".to_string())
                    }
                },
                "bob",
            )
            .await;
        });

        event_tx.send(remote::Event::Setup).await.unwrap();
        assert_eq!(
            parse_text(client.from_server.recv().await.unwrap()),
            json!(["setup", {}])
        );

        revoked.store(true, Ordering::SeqCst);

        let mut stdout = String::new();
        let mut saw_disconnect = false;
        let mut close_code = None;
        while let Some(msg) =
            tokio::time::timeout(Duration::from_secs(2), client.from_server.recv())
                .await
                .expect("revocation frames")
        {
            match msg {
                Message::Text(raw) => {
                    let v: Value = serde_json::from_str(raw.as_str()).unwrap();
                    if v[0] == "stdout" {
                        stdout.push_str(v[1].as_str().unwrap());
                    } else if v[0] == "disconnect" && v[1] == 1 {
                        saw_disconnect = true;
                    }
                }
                Message::Close(Some(frame)) => {
                    close_code = Some(frame.code);
                    break;
                }
                _ => {}
            }
        }
        assert!(
            stdout.contains("Your permission was removed."),
            "{stdout:?}"
        );
        assert!(saw_disconnect);
        assert_eq!(close_code, Some(CLOSE_REVOKED));
        bridge.await.unwrap();
        assert!(input_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn drain_sends_going_away_1001_without_disconnect_and_stops_backend() {
        let (socket, mut client) = memory_socket_pair(8, 16);
        let (input_tx, mut input_rx) = mpsc::channel(8);
        let (event_tx, event_rx) = mpsc::channel(8);
        let session = TerminalSession::new(
            remote::Terminal {
                input: input_tx,
                events: event_rx,
            },
            RevokeAction::None,
        );
        let (drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
        let bridge = tokio::spawn(async move {
            bridge_session(
                socket,
                session,
                async {
                    let _ = drain_rx.await;
                },
                fast_timeouts(Duration::from_secs(60)),
                || async { None },
                "alice",
            )
            .await;
        });

        event_tx.send(remote::Event::Setup).await.unwrap();
        assert_eq!(
            parse_text(client.from_server.recv().await.unwrap()),
            json!(["setup", {}])
        );

        drain_tx.send(()).unwrap();

        let mut stdout = String::new();
        let mut saw_disconnect = false;
        let mut close_code = None;
        while let Some(msg) =
            tokio::time::timeout(Duration::from_secs(2), client.from_server.recv())
                .await
                .expect("drain frames")
        {
            match msg {
                Message::Text(raw) => {
                    let v: Value = serde_json::from_str(raw.as_str()).unwrap();
                    if v[0] == "stdout" {
                        stdout.push_str(v[1].as_str().unwrap());
                    } else if v[0] == "disconnect" {
                        saw_disconnect = true;
                    }
                }
                Message::Close(Some(frame)) => {
                    close_code = Some(frame.code);
                    break;
                }
                _ => {}
            }
        }
        assert!(stdout.contains(SHUTTING_DOWN), "{stdout:?}");
        assert!(
            !saw_disconnect,
            "drain must not emit disconnect so browser can reconnect"
        );
        assert_eq!(close_code, Some(1001));
        bridge.await.unwrap();
        assert!(input_rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn shell_state_coalescer_runs_one_query_at_a_time_and_answers_displaced_with_none() {
        let (socket, mut client) = memory_socket_pair(32, 32);
        let (input_tx, mut input_rx) = mpsc::channel(32);
        let (event_tx, event_rx) = mpsc::channel(32);
        let session = TerminalSession::new(
            remote::Terminal {
                input: input_tx,
                events: event_rx,
            },
            RevokeAction::None,
        );

        let running = Arc::new(AtomicUsize::new(0));
        let max_running = Arc::new(AtomicUsize::new(0));
        let total_queries = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(tokio::sync::Notify::new());

        let (running_c, max_c, total_c, gate_c) = (
            running.clone(),
            max_running.clone(),
            total_queries.clone(),
            gate.clone(),
        );
        // In-memory backend exercising the exact `ShellStateCoalescer` used by `LocalPtyAdapter`.
        let backend = tokio::spawn(async move {
            let mut coalescer = ShellStateCoalescer::default();
            let mut job: Option<tokio::task::JoinHandle<Option<remote::ShellState>>> = None;
            event_tx.send(remote::Event::Setup).await.unwrap();
            let spawn_query = || {
                let (running, max, total, gate) = (
                    running_c.clone(),
                    max_c.clone(),
                    total_c.clone(),
                    gate_c.clone(),
                );
                tokio::spawn(async move {
                    let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                    max.fetch_max(now, Ordering::SeqCst);
                    total.fetch_add(1, Ordering::SeqCst);
                    gate.notified().await;
                    running.fetch_sub(1, Ordering::SeqCst);
                    Some(remote::ShellState {
                        idle: true,
                        empty: true,
                    })
                })
            };
            loop {
                tokio::select! {
                    result = joined(&mut job), if job.is_some() => {
                        job = None;
                        let (finished, next) = coalescer.complete();
                        if let Some(id) = finished {
                            let _ = event_tx.send(remote::Event::ShellState {
                                id,
                                state: result.ok().flatten(),
                            }).await;
                        }
                        if next.is_some() {
                            job = Some(spawn_query());
                        }
                    }
                    command = input_rx.recv() => {
                        let Some(command) = command else { break; };
                        if let remote::Input::ShellState(id) = command {
                            let (start_now, displaced) = coalescer.submit(id);
                            if start_now {
                                job = Some(spawn_query());
                            } else if let Some(dropped) = displaced {
                                let _ = event_tx.send(remote::Event::ShellState {
                                    id: dropped,
                                    state: None,
                                }).await;
                            }
                        }
                    }
                }
            }
        });

        let (_drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
        let bridge = tokio::spawn(async move {
            bridge_session(
                socket,
                session,
                async {
                    let _ = drain_rx.await;
                },
                fast_timeouts(Duration::from_secs(60)),
                || async { None },
                "alice",
            )
            .await;
        });

        assert_eq!(
            parse_text(client.from_server.recv().await.unwrap()),
            json!(["setup", {}])
        );

        let burst: Vec<String> = (0..12).map(|i| format!("q{i}")).collect();
        for id in &burst {
            client
                .to_server
                .send(Message::Text(json!(["shell_state", id]).to_string().into()))
                .await
                .unwrap();
        }

        // Wait for the 10 displaced middle requests (q1..q10) to be answered immediately with null.
        let mut answers: HashMap<String, Value> = HashMap::new();
        while answers.len() < 10 {
            let msg = parse_text(
                tokio::time::timeout(Duration::from_secs(2), client.from_server.recv())
                    .await
                    .expect("displaced reply")
                    .unwrap(),
            );
            assert_eq!(msg[0], "shell_state");
            answers.insert(msg[1].as_str().unwrap().to_string(), msg[2].clone());
        }
        for i in 1..11 {
            assert_eq!(answers.get(&format!("q{i}")), Some(&Value::Null));
        }

        // Release q0, then release the coalesced latest request q11.
        gate.notify_one();
        let msg = parse_text(client.from_server.recv().await.unwrap());
        answers.insert(msg[1].as_str().unwrap().to_string(), msg[2].clone());
        tokio::time::sleep(Duration::from_millis(10)).await;
        gate.notify_one();
        let msg = parse_text(client.from_server.recv().await.unwrap());
        answers.insert(msg[1].as_str().unwrap().to_string(), msg[2].clone());

        assert_eq!(
            answers.get("q0"),
            Some(&json!({"idle": true, "empty": true}))
        );
        assert_eq!(
            answers.get("q11"),
            Some(&json!({"idle": true, "empty": true}))
        );
        assert_eq!(max_running.load(Ordering::SeqCst), 1);
        assert_eq!(total_queries.load(Ordering::SeqCst), 2);

        drop(client.to_server);
        bridge.await.unwrap();
        backend.await.unwrap();
    }

    #[test]
    fn window_budget_enforces_per_connection_cap_and_serializes_spawns() {
        let mut budget = WindowBudget::default();
        // First request starts immediately; requests 2..=MAX_NEW_WINDOWS queue serially;
        // anything past MAX_NEW_WINDOWS is refused.
        assert!(budget.request(MAX_NEW_WINDOWS));
        for _ in 1..MAX_NEW_WINDOWS {
            assert!(!budget.request(MAX_NEW_WINDOWS));
        }
        for _ in 0..10 {
            assert!(!budget.request(MAX_NEW_WINDOWS));
        }
        let mut started = 1usize;
        while budget.complete(true) {
            started += 1;
        }
        assert_eq!(started, MAX_NEW_WINDOWS);
        // Once the connection cap is reached, even after completion no more windows start.
        assert!(!budget.request(MAX_NEW_WINDOWS));
    }

    #[test]
    fn a_refused_queued_window_leaves_new_windows_working() {
        let mut budget = WindowBudget::default();
        assert!(budget.request(MAX_NEW_WINDOWS));
        // Queued behind the first; when it finishes, windows may not open (the terminal limit,
        // or tmux refused): the queued one is dropped ...
        assert!(!budget.request(MAX_NEW_WINDOWS));
        assert!(!budget.request(MAX_NEW_WINDOWS));
        assert!(!budget.complete(false));
        // ... and the next request starts at once instead of waiting behind a window that
        // never started.
        assert!(budget.request(MAX_NEW_WINDOWS));
        assert!(!budget.complete(true));
    }

    /// When the backend ends while the browser is still sending, its last output and
    /// `["disconnect", 1]` still reach the browser, so it stops instead of reconnecting.
    #[tokio::test]
    async fn a_backend_that_ends_first_still_delivers_its_last_events() {
        // The browser reads one frame at a time, so the writer is still sending when the bridge
        // finds the backend gone.
        let (socket, mut client) = memory_socket_pair(16, 1);
        let (input_tx, input_rx) = mpsc::channel(1);
        let (event_tx, event_rx) = mpsc::channel(16);
        let session = TerminalSession::new(
            remote::Terminal {
                input: input_tx,
                events: event_rx,
            },
            RevokeAction::None,
        );
        // The backend's last events, then it stops reading input.
        for event in [
            remote::Event::Output("earlier output".into()),
            remote::Event::Output("\r\n[input queue full]\r\n".into()),
            remote::Event::Disconnected,
        ] {
            event_tx.send(event).await.unwrap();
        }
        drop(event_tx);
        drop(input_rx);
        client
            .to_server
            .send(Message::Text(json!(["stdin", "x"]).to_string().into()))
            .await
            .unwrap();
        let bridge = tokio::spawn(bridge_session(
            socket,
            session,
            std::future::pending::<()>(),
            fast_timeouts(Duration::from_secs(60)),
            || async { None },
            "alice",
        ));
        // The bridge reads the browser's input, finds the backend gone and ends, while the
        // writer still waits for the browser.
        tokio::time::sleep(Duration::from_millis(100)).await;
        for expected in [
            json!(["stdout", "earlier output"]),
            json!(["stdout", "\r\n[input queue full]\r\n"]),
            json!(["disconnect", 1]),
        ] {
            assert_eq!(
                parse_text(client.from_server.recv().await.unwrap()),
                expected
            );
        }
        assert!(matches!(
            client.from_server.recv().await.unwrap(),
            Message::Close(Some(CloseFrame { code: 1000, .. }))
        ));
        bridge.await.unwrap();
    }

    #[test]
    fn an_isolated_host_api_window_keeps_its_term() {
        let dir = tempfile::tempdir().unwrap();
        let tmux = TmuxSettings {
            binary: "tmux".into(),
            socket: Some(dir.path().join("tmux-r.sock")),
            session_suffix: String::new(),
            env_dir: dir.path().join("env"),
            config_file: None,
        };
        let mut shell = ShellCommand::new("/bin/mrsm");
        shell.clear_env = true;
        shell.env = vec![
            ("TERM".into(), "xterm-256color".into()),
            ("PATH".into(), "/bin:/usr/bin".into()),
            ("SHLVL".into(), "3".into()),
        ];
        for profile in [TmuxProfile::Restricted, TmuxProfile::Full] {
            let argv = TmuxMux::new(tmux.clone(), profile)
                .shell_argv(&shell)
                .unwrap();
            // `env -i` wipes tmux's `TERM`, so the window's own file must carry it.
            assert_eq!(argv[1], "-i");
            let body = std::fs::read_to_string(&argv[5]).unwrap();
            assert!(body.contains("TERM='xterm-256color'"), "{body}");
            assert!(!body.contains("SHLVL="), "{body}");
        }
        // A window that inherits the server's environment keeps tmux's own `TERM`.
        shell.clear_env = false;
        let argv = TmuxMux::new(tmux, TmuxProfile::Restricted)
            .shell_argv(&shell)
            .unwrap();
        let body = std::fs::read_to_string(&argv[3]).unwrap();
        assert!(!body.lines().any(|line| line.starts_with("TERM=")));
    }

    #[test]
    fn tmux_profile_team_scrubs_job_context_and_uses_probed_env_i() {
        let dir = tempfile::tempdir().unwrap();
        let tmux = TmuxSettings {
            binary: "tmux".into(),
            socket: Some(dir.path().join("team.sock")),
            session_suffix: "_t".into(),
            env_dir: dir.path().join("env"),
            config_file: None,
        };
        let mut shell = ShellCommand::new("/bin/mrsm");
        shell.env = vec![
            ("TERM".into(), "xterm-256color".into()),
            ("TEAM_VAR".into(), "ok".into()),
            ("MRSM_DAEMON_ID".into(), "leaked-job".into()),
            ("TMUX".into(), "/tmp/host,1,0".into()),
        ];
        let mux = TmuxMux::new(tmux, TmuxProfile::Team("alpha".into()));
        let argv = mux.shell_argv(&shell).unwrap();
        assert_eq!(argv[0], env_program());
        assert_eq!(argv[1], "-i");
        assert_eq!(argv[2], "/bin/sh");
        assert_eq!(argv[3], "-c");
        assert!(argv[4].contains("unset "), "{}", argv[4]);
        let body = std::fs::read_to_string(&argv[5]).unwrap();
        assert!(body.contains("TEAM_VAR='ok'"), "{body}");
        assert!(body.contains("TERM='xterm-256color'"), "{body}");
        assert!(!body.contains("MRSM_DAEMON_ID="), "{body}");
        assert!(!body.contains("TMUX="), "{body}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let file_mode = std::fs::metadata(&argv[5]).unwrap().permissions().mode();
            assert_eq!(file_mode & 0o777, 0o600);
            let dir_mode = std::fs::metadata(dir.path().join("env"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(dir_mode & 0o777, 0o700);
        }
    }
}
