//! Open terminals are authorized again: they close when the session ends, the session store
//! fails, the user loses access or the server drains, and a revoked terminal ends its shell.

use super::*;

/// An open terminal is authorized again every `reauthorize_every`: signing out (or an
/// administrator ending the user's sessions) closes it with a reason and ends its shell. Another
/// user's terminal keeps running.
#[tokio::test]
async fn an_open_terminal_closes_when_its_session_ends() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let sessions = sessions();
    let cfg = config_with(&state, sessions.clone(), sh(), None).with_reauthorize_every(RECHECK);
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    let pid = shell_pid(&mut socket).await;
    let mut other = connect(addr, OTHER_ADMIN_SESSION).await;
    expect_setup(&mut other).await;
    let other_pid = shell_pid(&mut other).await;

    // Several checks pass while the session is valid.
    tokio::time::sleep(RECHECK * 3).await;
    send_json(&mut socket, json!(["stdin", "echo still-mine\n"])).await;
    read_until(&mut socket, |out| has_line(out, "still-mine")).await;

    sessions.end(ADMIN_SESSION);
    let (out, disconnected, code) = ending(&mut socket, RECHECK * 10).await;
    assert!(out.contains("Your session has ended"), "{out:?}");
    assert!(disconnected);
    assert_eq!(code, Some(1008));
    assert_shell_ends(pid).await;
    assert_no_echo(&mut socket).await;

    // The other user's terminal is untouched.
    if Path::new("/proc/self").exists() {
        assert!(alive(other_pid));
    }
    send_json(&mut other, json!(["stdin", "echo other-alive\n"])).await;
    read_until(&mut other, |out| has_line(out, "other-alive")).await;
}

/// A session store that fails while a terminal is open closes it (fail closed).
#[tokio::test]
async fn an_open_terminal_closes_when_the_session_store_fails() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let sessions = sessions();
    let cfg = config_with(&state, sessions.clone(), sh(), None).with_reauthorize_every(RECHECK);
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    let pid = shell_pid(&mut socket).await;
    sessions.fail(true);
    let (out, disconnected, code) = ending(&mut socket, RECHECK * 10).await;
    assert!(out.contains("verify"), "{out:?}");
    assert!(disconnected);
    assert_eq!(code, Some(1008));
    assert_shell_ends(pid).await;
    assert_no_echo(&mut socket).await;
}

/// A client that leaves the kernel as little room as it can for unread data (a tiny receive
/// buffer), so a noisy shell fills the connection as soon as the client stops reading.
async fn connect_with_a_small_window(addr: std::net::SocketAddr, session: &str) -> Socket {
    let tcp = tokio::net::TcpSocket::new_v4().unwrap();
    tcp.set_recv_buffer_size(4096).unwrap();
    let stream = tcp.connect(addr).await.unwrap();
    let (socket, _) = tokio_tungstenite::client_async(
        format!("ws://{addr}/websocket/{session}"),
        MaybeTlsStream::Plain(stream),
    )
    .await
    .expect("handshake");
    socket
}

/// A browser that stops reading while its shell floods the terminal fills the connection. The
/// session is still checked, and a revocation ends the shell although the browser never reads
/// the reason (before, the bridge waited on that write and never checked the session again).
#[tokio::test]
async fn a_revoked_terminal_ends_its_shell_while_the_browser_is_not_reading() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let sessions = sessions();
    let cfg = config_with(&state, sessions.clone(), sh(), None).with_reauthorize_every(RECHECK);
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    let mut socket = connect_with_a_small_window(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    let pid = shell_pid(&mut socket).await;
    // The shell itself prints a long line forever; from here on the client reads nothing.
    send_json(
        &mut socket,
        json!([
            "stdin",
            "x=$(printf '%01000d' 0); while :; do echo \"$x\"; done\n"
        ]),
    )
    .await;
    // Long enough for the connection to fill; the session's checks pass meanwhile.
    tokio::time::sleep(Duration::from_secs(4)).await;
    if Path::new("/proc/self").exists() {
        assert!(alive(pid), "the noisy shell ended by itself");
    }
    sessions.end(ADMIN_SESSION);
    assert_shell_ends(pid).await;
    drop(socket);
}

/// A user who loses the permission to use the terminal, or is deleted, loses an open terminal.
#[tokio::test]
async fn an_open_terminal_closes_when_its_user_loses_access() {
    use meerschaum_connector::InstanceConnector;
    let root = tempfile::tempdir().unwrap();
    let (state, instance) =
        state_and_instance(server_config(root.path(), ServerFlags::default(), None));
    let cfg = config_with(&state, sessions(), sh(), None).with_reauthorize_every(RECHECK);
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    // `bob` is an operator (`webterm:use`); `root2` an administrator.
    let mut operator = connect(addr, USER_SESSION).await;
    expect_setup(&mut operator).await;
    let mut admin = connect(addr, OTHER_ADMIN_SESSION).await;
    expect_setup(&mut admin).await;
    let admin_pid = shell_pid(&mut admin).await;

    instance.seed_user_scopes("bob", &[]);
    let (out, _, code) = ending(&mut operator, Duration::from_secs(5)).await;
    assert!(out.contains("permission"), "{out:?}");
    assert_eq!(code, Some(1008));

    let deleted = instance
        .delete_user(&meerschaum_core::users::User::new("root2", None))
        .await;
    assert!(deleted.success, "{deleted:?}");
    let (out, disconnected, code) = ending(&mut admin, Duration::from_secs(5)).await;
    assert!(out.contains("Your session has ended"), "{out:?}");
    assert!(disconnected);
    assert_eq!(code, Some(1008));
    assert_shell_ends(admin_pid).await;
}

/// On the tmux path a revoked terminal ends its own tmux client, as a closed tab does: the
/// user's tmux session stays (only an authorized connection reattaches to it), and another
/// user's attached terminal keeps running.
#[tokio::test]
async fn a_revoked_tmux_terminal_ends_only_its_own_client() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP a_revoked_tmux_terminal_ends_only_its_own_client: no tmux");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let tmux_dir = short_tmp();
    let _server = PrivateTmux(tmux_dir.path().to_path_buf());
    let state = state(root.path());
    let sessions = sessions();
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: Some(tmux_socket(tmux_dir.path())),
        session_suffix: "_r".into(),
        env_dir: root.path().join("webterm"),
        config_file: Some("/dev/null".into()),
    };
    let cfg =
        config_with(&state, sessions.clone(), sh(), Some(tmux)).with_reauthorize_every(RECHECK);
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
    let clients = |session: &str| {
        let target = format!("={session}");
        String::from_utf8_lossy(
            &tmux_cmd(&["list-clients", "-t", &target, "-F", "#{client_pid}"]).stdout,
        )
        .lines()
        .count()
    };
    let mut mine = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut mine).await;
    send_json(&mut mine, json!(["stdin", "echo mine-ready\n"])).await;
    read_until(&mut mine, |out| has_line(out, "mine-ready")).await;
    let mut theirs = connect(addr, OTHER_ADMIN_SESSION).await;
    expect_setup(&mut theirs).await;
    send_json(&mut theirs, json!(["stdin", "echo theirs-ready\n"])).await;
    read_until(&mut theirs, |out| has_line(out, "theirs-ready")).await;
    assert_eq!((clients("admin_r"), clients("root2_r")), (1, 1));

    sessions.end(ADMIN_SESSION);
    let (out, disconnected, code) = ending(&mut mine, RECHECK * 10).await;
    assert!(out.contains("Your session has ended"), "{out:?}");
    assert!(disconnected);
    assert_eq!(code, Some(1008));
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while clients("admin_r") > 0 {
        assert!(
            std::time::Instant::now() < deadline,
            "the revoked terminal's tmux client outlived it"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(
        tmux_cmd(&["has-session", "-t", "=admin_r"])
            .status
            .success()
    );

    assert_eq!(clients("root2_r"), 1);
    send_json(&mut theirs, json!(["stdin", "echo theirs-alive\n"])).await;
    read_until(&mut theirs, |out| has_line(out, "theirs-alive")).await;
}

/// A draining server closes its terminals at once (`1001 Going Away`) and their plain shells
/// end; without `["disconnect", 1]`, so the browser may reconnect once a server is back.
#[tokio::test]
async fn a_draining_server_closes_its_terminals() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let cfg = config(&state, sh(), None);
    let addr = serve(webterm::routes(state.clone(), cfg)).await;
    let mut socket = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut socket).await;
    let pid = shell_pid(&mut socket).await;
    state.lifecycle.begin_drain();
    let (out, disconnected, code) = ending(&mut socket, Duration::from_secs(3)).await;
    assert!(out.contains("server shutting down"), "{out:?}");
    assert!(!disconnected);
    assert_eq!(code, Some(1001));
    assert_shell_ends(pid).await;
}
