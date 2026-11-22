//! The tmux path: one readiness query at a time, sessions that survive reconnects, bounded windows,
//! a dedicated tmux server with Meerschaum's config, and the window list on a running server.

use super::*;

/// A fake `tmux` for [`shell_state_queries_run_one_at_a_time`]: `new-session` runs the wrapped
/// shell itself (no tmux server at all), and `display-message` (the readiness query) takes a
/// second and records how many queries run at once. Anything else succeeds silently.
const FAKE_TMUX: &str = r#"#!/bin/sh
while [ $# -gt 0 ]; do
    case "$1" in
        -S|-f) shift 2 ;;
        -u) shift ;;
        *) break ;;
    esac
done
command="$1"
shift
case "$command" in
    new-session)
        while [ $# -gt 0 ]; do
            case "$1" in
                -A) shift ;;
                -s|-n) shift 2 ;;
                *) break ;;
            esac
        done
        exec "$@"
        ;;
    display-message)
        mkdir -p "$FAKE_TMUX_DIR/running"
        : > "$FAKE_TMUX_DIR/running/$$"
        ls "$FAKE_TMUX_DIR/running" | wc -l >> "$FAKE_TMUX_DIR/concurrency"
        sleep 1
        rm -f "$FAKE_TMUX_DIR/running/$$"
        echo 1
        ;;
esac
exit 0
"#;

/// A burst of `["shell_state", id]` requests runs one readiness query at a time per connection:
/// the latest request waits for the running one, every other is answered at once without a
/// snapshot. The terminal still closes promptly on revocation while a query runs.
#[tokio::test]
async fn shell_state_queries_run_one_at_a_time() {
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let fake = tempfile::tempdir().unwrap();
    let binary = fake.path().join("tmux");
    std::fs::write(&binary, FAKE_TMUX).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    let state = state(root.path());
    let sessions = sessions();
    let mut shell = sh();
    shell
        .env
        .push(("FAKE_TMUX_DIR".into(), fake.path().display().to_string()));
    // Readiness is only read where the shell reports it: without this, no query runs tmux.
    let state_dir = root.path().join("shell-state");
    std::fs::create_dir_all(&state_dir).unwrap();
    shell.env.push((
        "MRSM_WEBTERM_STATE_DIR".into(),
        state_dir.display().to_string(),
    ));
    let tmux = TmuxSettings {
        binary: binary.display().to_string(),
        socket: Some(fake.path().join("never.sock")),
        session_suffix: "_s".into(),
        env_dir: root.path().join("webterm"),
        config_file: Some("/dev/null".into()),
    };
    let cfg =
        config_with(&state, sessions.clone(), shell, Some(tmux)).with_reauthorize_every(RECHECK);
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    send_json(&mut socket, json!(["stdin", "echo fake-ready\n"])).await;
    read_until(&mut socket, |out| has_line(out, "fake-ready")).await;

    // Within the socket's control queue: replies past it are dropped (the browser gives up on a
    // probe after two seconds), and this runtime drains the queue only between messages.
    let burst: Vec<String> = (0..12).map(|i| format!("q{i}")).collect();
    for id in &burst {
        send_json(&mut socket, json!(["shell_state", id])).await;
    }
    let mut answered = HashMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while answered.len() < burst.len() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "unanswered requests; got {answered:?}"
        );
        match try_next_message(&mut socket)
            .await
            .expect("the terminal went quiet")
        {
            Some(message) if message[0] == "shell_state" => {
                answered.insert(message[1].as_str().unwrap().to_string(), message[2].clone());
            }
            Some(_) => {}
            None => panic!("the terminal closed; got {answered:?}"),
        }
    }
    assert!(burst.iter().all(|id| answered.contains_key(id)));
    let counts: Vec<usize> = std::fs::read_to_string(fake.path().join("concurrency"))
        .unwrap()
        .lines()
        .map(|line| line.trim().parse().unwrap())
        .collect();
    assert!(
        counts.iter().all(|&running| running == 1),
        "readiness queries ran at once: {counts:?}"
    );
    // The first request and the latest one ran (a loaded host may split the burst once more).
    assert!(
        (2..=3).contains(&counts.len()),
        "queries run for a burst of {}: {counts:?}",
        burst.len()
    );

    // The session ends while a query runs: the terminal still closes at its next check.
    send_json(&mut socket, json!(["shell_state", "last"])).await;
    tokio::time::sleep(Duration::from_millis(100)).await;
    sessions.end(ADMIN_SESSION);
    let (out, disconnected, code) = ending(&mut socket, RECHECK * 10).await;
    assert!(out.contains("Your session has ended"), "{out:?}");
    assert!(disconnected);
    assert_eq!(code, Some(1008));
}

#[tokio::test]
async fn tmux_sessions_survive_reconnects_and_carry_the_environment() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP tmux_sessions_survive_reconnects_and_carry_the_environment: no tmux");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let tmux_dir = short_tmp();
    let _server = PrivateTmux(tmux_dir.path().to_path_buf());
    let env_dir = root.path().join("webterm");
    let state = state(root.path());
    let mut shell = sh();
    // An MRSM variable the shell must receive through the environment file even when the
    // explicitly selected private tmux server already runs.
    shell
        .env
        .push(("MRSM_WEBTERM_TEST".into(), "it's=ok $HOME".into()));
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: Some(tmux_socket(tmux_dir.path())),
        session_suffix: "_t".into(),
        env_dir: env_dir.clone(),
        // Never the developer's `~/.tmux.conf` (plugins, status line, extra sessions).
        config_file: Some("/dev/null".into()),
    };
    let cfg = config(&state, shell, Some(tmux));
    assert!(cfg.uses_tmux());
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
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
    // The names of the sessions, polled: a loaded host can take a moment to settle after a client
    // dies, and the listing is only empty while the server is not answering.
    let wait_for_session = |name: &str| {
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let out = tmux_cmd(&["list-sessions", "-F", "#{session_name}"]);
            let listing = String::from_utf8_lossy(&out.stdout).into_owned();
            if listing.lines().any(|l| l == name) {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "session {name} never listed: {listing:?} stderr={:?}",
                String::from_utf8_lossy(&out.stderr)
            );
            std::thread::sleep(Duration::from_millis(100));
        }
    };

    let mut first = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut first).await;
    send_json(
        &mut first,
        json!([
            "stdin",
            "KEPT=reattach$((6*7)); echo env:$MRSM_WEBTERM_TEST\n"
        ]),
    )
    .await;
    read_until(&mut first, |out| has_line(out, "env:it's=ok $HOME")).await;
    // The shell sees TERM from tmux, and the session is named after the user.
    wait_for_session("admin_t");
    // The window read its environment file and removed it: none stays behind (5.1).
    let left: Vec<String> = std::fs::read_dir(&env_dir)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("webterm.") && name.ends_with(".env"))
        .collect();
    assert!(left.is_empty(), "{left:?}");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&env_dir).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o700);
    }

    // Closing the tab kills the tmux client, not the session.
    first.close(None).await.unwrap();
    drop(first);
    wait_for_session("admin_t");

    // A second tab of the same user attaches to the same shell.
    let mut second = connect(addr, ADMIN_TAB_TWO).await;
    expect_setup(&mut second).await;
    send_json(&mut second, json!(["stdin", "echo kept:$KEPT\n"])).await;
    read_until(&mut second, |out| has_line(out, "kept:reattach42")).await;

    // `["new_window"]` opens a second tmux window that also has the environment.
    send_json(&mut second, json!(["new_window"])).await;
    for _ in 0..50 {
        let windows =
            String::from_utf8(tmux_cmd(&["list-windows", "-t", "admin_t"]).stdout).unwrap();
        if windows.lines().count() == 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let windows = String::from_utf8(tmux_cmd(&["list-windows", "-t", "admin_t"]).stdout).unwrap();
    assert_eq!(windows.lines().count(), 2, "{windows}");
    send_json(
        &mut second,
        json!(["stdin", "echo window2:$MRSM_WEBTERM_TEST\n"]),
    )
    .await;
    read_until(&mut second, |out| has_line(out, "window2:it's=ok $HOME")).await;

    // A connection may open only a bounded number of windows (this one has used one).
    for _ in 0..15 {
        send_json(&mut second, json!(["new_window"])).await;
    }
    let count = || {
        String::from_utf8(tmux_cmd(&["list-windows", "-t", "admin_t"]).stdout)
            .unwrap()
            .lines()
            .count()
    };
    for _ in 0..100 {
        if count() >= 9 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        count(),
        9,
        "1 original window + 8 requested by one connection"
    );
    drop(second);
}

/// Windows outlive the connection that opened them: a reconnect does not start a fresh allowance,
/// so the user's tmux session stays within `MAX_SESSION_WINDOWS` however often the tab reconnects.
#[tokio::test]
async fn tmux_windows_are_bounded_across_reconnects() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP tmux_windows_are_bounded_across_reconnects: no tmux");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let tmux_dir = short_tmp();
    let _server = PrivateTmux(tmux_dir.path().to_path_buf());
    let state = state(root.path());
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: Some(tmux_socket(tmux_dir.path())),
        session_suffix: "_w".into(),
        env_dir: root.path().join("webterm"),
        config_file: Some("/dev/null".into()),
    };
    let addr = serve(webterm::routes(
        state.clone(),
        config(&state, sh(), Some(tmux)),
    ))
    .await;
    let count = || {
        let out = std::process::Command::new("tmux")
            .arg("-S")
            .arg(tmux_socket(tmux_dir.path()))
            .args(["list-windows", "-t", "=admin_w", "-F", "#{window_id}"])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap();
        String::from_utf8_lossy(&out.stdout).lines().count()
    };
    let mut expected = 1;
    for (i, session) in [ADMIN_SESSION, ADMIN_TAB_TWO, ADMIN_SESSION]
        .into_iter()
        .enumerate()
    {
        let mut socket = connect(addr, session).await;
        expect_setup(&mut socket).await;
        let token = format!("ready{i}");
        send_json(&mut socket, json!(["stdin", format!("echo {token}\n")])).await;
        read_until(&mut socket, |out| has_line(out, &token)).await;
        // More than one connection's allowance (8).
        for _ in 0..12 {
            send_json(&mut socket, json!(["new_window"])).await;
        }
        expected = (expected + 8).min(webterm::MAX_SESSION_WINDOWS);
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while count() < expected && std::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        // Anything past the bound would arrive by now.
        tokio::time::sleep(Duration::from_millis(800)).await;
        assert_eq!(count(), expected, "after connection {i}");
        socket.close(None).await.unwrap();
        drop(socket);
    }
    assert_eq!(count(), webterm::MAX_SESSION_WINDOWS);
}

/// With tmux enabled, the terminal's sessions live on a tmux server of their own (a socket
/// under the root, never the user's default server), started with Meerschaum's configuration
/// (the window list on top), named as before; the server this process started stops with the API.
#[tokio::test]
async fn terminals_run_on_their_own_tmux_server_with_meerschaums_config() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP terminals_run_on_their_own_tmux_server_with_meerschaums_config: no tmux");
        return;
    }
    // A short root, so the socket fits under it.
    let root = short_tmp();
    let state = state(root.path());
    let socket = webterm::tmux_socket_path(&state.config).unwrap();
    assert!(socket.starts_with(root.path()), "{socket:?}");
    // Kill this test's server by its socket (never the default one) even if an assertion fails.
    struct KillOnDrop(std::path::PathBuf);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = std::process::Command::new("tmux")
                .arg("-S")
                .arg(&self.0)
                .arg("kill-server")
                .env_remove("TMUX")
                .env_remove("TMUX_PANE")
                .output();
        }
    }
    let _kill = KillOnDrop(socket.clone());
    let cfg = WebtermConfig::new(
        &state,
        sessions(),
        Arc::new(|id| format!("<html>{id}</html>")),
        sh(),
    );
    let tmux = cfg.tmux.clone().expect("the tmux path");
    assert_eq!(tmux.socket.as_deref(), Some(socket.as_path()));
    let conf = tmux.config_file.clone().expect("a configuration file");
    assert_eq!(
        std::fs::read_to_string(&conf).unwrap(),
        webterm::TMUX_CONF,
        "{conf}"
    );
    let tmux_cmd = |args: &[&str]| {
        std::process::Command::new("tmux")
            .arg("-S")
            .arg(&socket)
            .args(args)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap()
    };
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    let mut tab = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut tab).await;
    send_json(&mut tab, json!(["stdin", "echo ready:$((6*7))\n"])).await;
    read_until(&mut tab, |out| has_line(out, "ready:42")).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let listing =
            String::from_utf8_lossy(&tmux_cmd(&["list-sessions", "-F", "#{session_name}"]).stdout)
                .into_owned();
        if listing.lines().any(|l| l == "admin_mrsm") {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no admin_mrsm session on the terminal's own server: {listing:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    // Meerschaum's configuration: the window list on top, mouse on.
    let status = String::from_utf8_lossy(&tmux_cmd(&["show-options", "-gv", "status"]).stdout)
        .trim()
        .to_string();
    assert_eq!(status, "on");
    let position =
        String::from_utf8_lossy(&tmux_cmd(&["show-options", "-gv", "status-position"]).stdout)
            .trim()
            .to_string();
    assert_eq!(position, "top");
    // The first window is named for its tab.
    let windows = String::from_utf8_lossy(
        &tmux_cmd(&[
            "list-windows",
            "-t",
            "=admin_mrsm",
            "-F",
            "#{window_index}:#{window_name}",
        ])
        .stdout,
    )
    .trim()
    .to_string();
    assert_eq!(windows, format!("1:{}", webterm::TMUX_WINDOW_NAME));
    let mouse = String::from_utf8_lossy(&tmux_cmd(&["show-options", "-gv", "mouse"]).stdout)
        .trim()
        .to_string();
    assert_eq!(mouse, "on");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(socket.parent().unwrap())
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o700, "the socket's directory is private");
    }
    // This process started the server, so it stops it with the API.
    assert!(webterm::owned_server_sockets().contains(&socket));
    drop(tab);
    assert!(webterm::stop_owned_server(&socket));
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while tmux_cmd(&["list-sessions"]).status.success() {
        assert!(
            std::time::Instant::now() < deadline,
            "the terminal's tmux server outlived the API"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(!webterm::stop_owned_server(&socket));
}

/// A tmux server that already runs on the terminal's own socket (started by an earlier
/// release, whose configuration turned the status line off, or left behind by an API that did not
/// stop cleanly) gets Meerschaum's configuration again when a terminal attaches, so the window
/// list shows; it is not this process's to stop.
#[tokio::test]
async fn a_running_tmux_server_gets_the_window_list_again() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP a_running_tmux_server_gets_the_window_list_again: no tmux");
        return;
    }
    let root = short_tmp();
    let state = state(root.path());
    let socket = webterm::tmux_socket_path(&state.config).unwrap();
    // Kill this test's server by its socket (never the default one) even if an assertion fails.
    struct KillOnDrop(std::path::PathBuf);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = std::process::Command::new("tmux")
                .arg("-S")
                .arg(&self.0)
                .arg("kill-server")
                .env_remove("TMUX")
                .env_remove("TMUX_PANE")
                .output();
        }
    }
    let _kill = KillOnDrop(socket.clone());
    let tmux_cmd = |args: &[&str]| {
        std::process::Command::new("tmux")
            .arg("-S")
            .arg(&socket)
            .args(args)
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output()
            .unwrap()
    };
    // What an earlier release left running: no status line.
    let started = tmux_cmd(&[
        "-f",
        "/dev/null",
        "new-session",
        "-d",
        "-s",
        "rc28_pre",
        "sh",
        ";",
        "set",
        "-g",
        "status",
        "off",
    ]);
    assert!(started.status.success(), "{started:?}");
    let option = |name: &str| {
        String::from_utf8_lossy(&tmux_cmd(&["show-options", "-gv", name]).stdout)
            .trim()
            .to_string()
    };
    assert_eq!(option("status"), "off");

    let cfg = WebtermConfig::new(
        &state,
        sessions(),
        Arc::new(|id| format!("<html>{id}</html>")),
        sh(),
    );
    assert_eq!(
        cfg.tmux.as_ref().and_then(|t| t.socket.as_deref()),
        Some(socket.as_path())
    );
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    let mut tab = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut tab).await;
    send_json(&mut tab, json!(["stdin", "echo ready:$((6*7))\n"])).await;
    read_until(&mut tab, |out| has_line(out, "ready:42")).await;
    assert_eq!(option("status"), "on");
    assert_eq!(option("status-position"), "top");
    // Applying it again changes nothing else: the server's own options stay.
    let overrides = || {
        String::from_utf8_lossy(&tmux_cmd(&["show-options", "-s", "terminal-overrides"]).stdout)
            .into_owned()
    };
    let before = overrides();
    drop(tab);
    let mut again = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut again).await;
    send_json(&mut again, json!(["stdin", "echo again:$((6*7))\n"])).await;
    read_until(&mut again, |out| has_line(out, "again:42")).await;
    assert_eq!(overrides(), before);
    assert_eq!(option("status"), "on");
    // The server was running before: it is not this process's.
    assert!(!webterm::owned_server_sockets().contains(&socket));
}
