//! Plain PTY shells: input, resize and independence, closing the socket ends the shell, a shell that
//! cannot start, `--no-webterm`, and a plain shell per tab when tmux is missing.

use super::*;

#[tokio::test]
async fn stdin_round_trips_through_a_pty_and_resize_reaches_it() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, sh(), None))).await;

    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;

    // The PTY echoes the typed line; only the shell's answer is a line of its own.
    send_json(&mut socket, json!(["stdin", "echo $((20+22))\n"])).await;
    let out = read_until(&mut socket, |out| has_line(out, "42")).await;
    assert!(out.contains("echo $((20+22))"), "{out:?}");

    // `["set_size", rows, cols, height, width]` resizes the PTY.
    send_json(&mut socket, json!(["set_size", 31, 101, 620, 808])).await;
    send_json(&mut socket, json!(["stdin", "stty size\n"])).await;
    read_until(&mut socket, |out| has_line(out, "31 101")).await;

    // Wide characters survive the UTF-8 round trip, and malformed frames are ignored.
    send_json(&mut socket, json!({"not": "an array"})).await;
    socket.send(Message::Text("nonsense".into())).await.unwrap();
    send_json(&mut socket, json!(["resize_me", 1, 2])).await;
    send_json(
        &mut socket,
        json!(["stdin", "printf '\\342\\206\\222 ok\\n'\n"]),
    )
    .await;
    read_until(&mut socket, |out| has_line(out, "→ ok")).await;

    // The shell ending closes the socket after `["disconnect", 1]`.
    send_json(&mut socket, json!(["stdin", "exit\n"])).await;
    loop {
        match next_message(&mut socket).await {
            Some(message) if message == json!(["disconnect", 1]) => break,
            Some(_) => continue,
            None => panic!("the socket closed without a disconnect message"),
        }
    }
    assert!(next_message(&mut socket).await.is_none());
}

#[tokio::test]
async fn concurrent_terminals_are_independent() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, sh(), None))).await;

    // Two users, and two tabs of one user (the plain path starts one shell per connection).
    let mut sockets = Vec::new();
    for session in [ADMIN_SESSION, ADMIN_TAB_TWO, OTHER_ADMIN_SESSION] {
        let mut socket = connect(addr, session).await;
        expect_setup(&mut socket).await;
        sockets.push(socket);
    }
    let mut pids = Vec::new();
    for (i, socket) in sockets.iter_mut().enumerate() {
        send_json(socket, json!(["stdin", format!("X={i}; echo pid$$:$X\n")])).await;
    }
    for (i, socket) in sockets.iter_mut().enumerate() {
        let end = format!(":{i}");
        let out = read_until(socket, |out| has_line_matching(out, "pid", &end)).await;
        let text = plain(&out);
        let line = text
            .split(['\r', '\n'])
            .find(|l| l.starts_with("pid") && l.ends_with(&end))
            .unwrap();
        pids.push(line.split(':').next().unwrap().to_string());
    }
    pids.sort();
    pids.dedup();
    assert_eq!(pids.len(), 3, "each connection owns its shell: {pids:?}");

    // One tab never sees what another typed.
    send_json(&mut sockets[0], json!(["stdin", "echo only-tab-zero\n"])).await;
    read_until(&mut sockets[0], |out| has_line(out, "only-tab-zero")).await;
    send_json(&mut sockets[1], json!(["stdin", "echo tab-one-ping\n"])).await;
    let out = read_until(&mut sockets[1], |out| has_line(out, "tab-one-ping")).await;
    assert!(!out.contains("only-tab-zero"), "{out:?}");
}

#[tokio::test]
async fn closing_the_socket_ends_the_shell() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, sh(), None))).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    send_json(&mut socket, json!(["stdin", "echo shellpid:$$:end\n"])).await;
    let out = read_until(&mut socket, |out| {
        has_line_matching(out, "shellpid:", ":end")
    })
    .await;
    let pid: i32 = plain(&out)
        .split(['\r', '\n'])
        .find(|l| l.starts_with("shellpid:") && l.ends_with(":end"))
        .and_then(|l| l.split(':').nth(1))
        .and_then(|n| n.parse().ok())
        .expect("the shell's pid");
    let mut other = connect(addr, OTHER_ADMIN_SESSION).await;
    expect_setup(&mut other).await;
    send_json(&mut other, json!(["stdin", "echo shellpid:$$:end\n"])).await;
    let out = read_until(&mut other, |out| {
        has_line_matching(out, "shellpid:", ":end")
    })
    .await;
    let other_pid: i32 = plain(&out)
        .split(['\r', '\n'])
        .find(|line| line.starts_with("shellpid:") && line.ends_with(":end"))
        .and_then(|line| line.split(':').nth(1))
        .and_then(|pid| pid.parse().ok())
        .expect("the other shell's pid");
    let state_dir = state.config.webterm_resources_path.join("shell-state");
    let readiness = state_dir.join(format!("{pid}.json"));
    let other_readiness = state_dir.join(format!("{other_pid}.json"));
    // `sh` has no readiness guard: simulate files left behind by abruptly killed shells.
    std::fs::write(
        &readiness,
        json!({"pid":pid,"idle":true,"empty":true}).to_string(),
    )
    .unwrap();
    std::fs::write(
        &other_readiness,
        json!({"pid":other_pid,"idle":true,"empty":true}).to_string(),
    )
    .unwrap();
    let alive = |pid: i32| std::path::Path::new(&format!("/proc/{pid}")).exists();
    // (`/proc` is Linux-only; elsewhere the pid check below is skipped.)
    let checkable = alive(pid);
    socket.close(None).await.unwrap();
    drop(socket);
    for _ in 0..100 {
        if !readiness.exists() && (!checkable || !alive(pid)) {
            assert!(
                other_readiness.exists(),
                "another terminal's readiness was removed"
            );
            send_json(&mut other, json!(["stdin", "echo still-alive\n"])).await;
            read_until(&mut other, |out| has_line(out, "still-alive")).await;
            other.close(None).await.unwrap();
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the shell {pid} or its readiness outlived its socket");
}

#[tokio::test]
async fn a_shell_that_cannot_start_reports_it() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let shell = ShellCommand::new("/nonexistent/mrsm-shell");
    let addr = serve(webterm::routes(state.clone(), config(&state, shell, None))).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    let first = next_message(&mut socket).await.expect("a message");
    assert_eq!(first[0], "stdout");
    assert!(
        first[1]
            .as_str()
            .unwrap()
            .contains("Failed to start the shell")
    );
    assert_eq!(
        next_message(&mut socket).await.unwrap(),
        json!(["disconnect", 1])
    );
}

#[tokio::test]
async fn no_webterm_leaves_both_paths_unmatched() {
    // `--no-webterm` (and `--no-dash`) never merge the routes: the plain API app answers `404`,
    // and the host proxy is not asked about the terminal paths either.
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let app = meerschaum_server::app(state.clone());
    for path in [
        format!("/webterm/{ADMIN_SESSION}"),
        format!("/websocket/{ADMIN_SESSION}"),
    ] {
        let response = send(&app, Request::get(path).body(Body::empty()).unwrap()).await;
        assert_eq!(response.status, StatusCode::NOT_FOUND);
        assert!(response.unmatched);
    }
    assert!(!meerschaum_server::routes::is_proxyable(
        &state.config,
        "/webterm/abc"
    ));
    assert!(!meerschaum_server::routes::is_proxyable(
        &state.config,
        "/websocket/abc"
    ));
    assert!(meerschaum_server::routes::is_proxyable(
        &state.config,
        "/plugin/x"
    ));
}

#[tokio::test]
async fn without_tmux_on_the_path_each_tab_gets_a_plain_shell() {
    // The fallback path, whether or not this host has tmux: the PATH the terminal is configured
    // with holds no `tmux`, so `WebtermConfig::new` must not choose the tmux path even though
    // `api:webterm:tmux:enabled` defaults to true.
    let root = tempfile::tempdir().unwrap();
    let empty = tempfile::tempdir().unwrap();
    let state = state(root.path());
    assert!(state.config.webterm_tmux_enabled);
    let mut shell = ShellCommand::new("/bin/sh");
    shell
        .env
        .push(("PATH".into(), empty.path().display().to_string()));
    shell.env.push(("PS1".into(), "$ ".into()));
    let cfg = WebtermConfig::new(
        &state,
        sessions(),
        Arc::new(|id| format!("<html>{id}</html>")),
        shell,
    );
    assert!(!cfg.uses_tmux());
    assert!(!webterm::tmux_available(
        "tmux",
        &[("PATH".into(), empty.path().display().to_string())]
    ));
    let addr = serve(webterm::routes(state.clone(), cfg)).await;

    let mut first = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut first).await;
    let mut second = connect(addr, ADMIN_TAB_TWO).await;
    expect_setup(&mut second).await;
    send_json(&mut first, json!(["stdin", "KEPT=one; echo a:$KEPT\n"])).await;
    read_until(&mut first, |out| has_line(out, "a:one")).await;
    // Without tmux the second tab of the same user is a different shell.
    send_json(&mut second, json!(["stdin", "echo b:${KEPT:-unset}\n"])).await;
    read_until(&mut second, |out| has_line(out, "b:unset")).await;
}
