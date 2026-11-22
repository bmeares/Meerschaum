//! With isolated teams, tmux is used only where the sandbox stops Unix sockets.

use super::*;

/// 5.1: tmux's socket runs commands for any process of the server's user that connects, and
/// tmux cannot ask for a secret. While an isolated team whose members may install plugins is
/// recorded and Unix sockets are not denied to team code (neither Landlock, Linux 7.1, nor the
/// seccomp filter of older kernels, as the server's probe finds), terminals run without tmux, so
/// nothing listens; where sockets are denied, tmux is used as before.
#[tokio::test]
async fn with_isolated_teams_tmux_is_used_only_where_their_sandbox_stops_unix_sockets() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP: no tmux");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let scope = meerschaum_core::team_scope::TeamScope {
        team: "wetlands".into(),
        team_id: Some("id-wetlands".into()),
        ..Default::default()
    };
    meerschaum_core::team_scope::write_scope_record(root.path(), &scope).unwrap();
    assert!(meerschaum_core::team_scope::isolated_teams_recorded(
        root.path()
    ));
    let state = state(root.path());
    for stops in [false, true] {
        let tmux_dir = short_tmp();
        let _server = PrivateTmux(tmux_dir.path().to_path_buf());
        let tmux = TmuxSettings {
            binary: "tmux".into(),
            socket: Some(tmux_socket(tmux_dir.path())),
            session_suffix: format!("_t{}", stops as u8),
            env_dir: root.path().join("webterm"),
            config_file: Some("/dev/null".into()),
        };
        let mut cfg = config(&state, sh(), Some(tmux));
        cfg.sandbox_stops_sockets = Some(stops);
        assert_eq!(cfg.tmux_refused().is_some(), !stops);
        let addr = serve(webterm::routes(state.clone(), cfg)).await;
        let mut first = connect(addr, ADMIN_SESSION).await;
        expect_setup(&mut first).await;
        send_json(&mut first, json!(["stdin", "KEPT=one; echo a:$KEPT\n"])).await;
        read_until(&mut first, |out| has_line(out, "a:one")).await;
        let mut second = connect(addr, ADMIN_TAB_TWO).await;
        expect_setup(&mut second).await;
        send_json(&mut second, json!(["stdin", "echo b:${KEPT:-unset}\n"])).await;
        if stops {
            // tmux: the second tab attaches to the same shell, on the private tmux server.
            read_until(&mut second, |out| has_line(out, "b:one")).await;
            assert!(tmux_socket(tmux_dir.path()).exists());
        } else {
            // No tmux: a shell of its own, and no tmux socket for anyone to connect to.
            read_until(&mut second, |out| has_line(out, "b:unset")).await;
            assert!(!tmux_socket(tmux_dir.path()).exists());
        }
        // Why: any process of this user that reaches the socket runs commands through it (only a
        // sandbox that refuses the connection stops a team's code from doing so).
        let planted = root.path().join(format!("planted-{}", stops as u8));
        let _ = std::process::Command::new("tmux")
            .arg("-S")
            .arg(tmux_socket(tmux_dir.path()))
            .args(["run-shell", &format!("touch '{}'", planted.display())])
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output();
        assert_eq!(planted.exists(), stops);
    }
}

/// 5.1: an isolated team that may not install plugins runs only plugins an administrator
/// granted it, so its existence keeps tmux even where Unix sockets are not denied to team code;
/// switching the team's plugin installs on refuses tmux there.
#[tokio::test]
async fn an_isolated_team_without_plugin_installs_keeps_tmux() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP: no tmux");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let mut scope = meerschaum_core::team_scope::TeamScope {
        team: "reviewers".into(),
        team_id: Some("id-reviewers".into()),
        plugins_write: Some(false),
        ..Default::default()
    };
    meerschaum_core::team_scope::write_scope_record(root.path(), &scope).unwrap();
    assert!(meerschaum_core::team_scope::isolated_teams_recorded(
        root.path()
    ));
    let state = state(root.path());
    let tmux_dir = short_tmp();
    let _server = PrivateTmux(tmux_dir.path().to_path_buf());
    let tmux = TmuxSettings {
        binary: "tmux".into(),
        socket: Some(tmux_socket(tmux_dir.path())),
        session_suffix: "_np".into(),
        env_dir: root.path().join("webterm"),
        config_file: Some("/dev/null".into()),
    };
    let mut cfg = config(&state, sh(), Some(tmux));
    cfg.sandbox_stops_sockets = Some(false);
    assert_eq!(cfg.tmux_refused(), None);
    let addr = serve(webterm::routes(state.clone(), cfg.clone())).await;
    let mut first = connect(addr, ADMIN_SESSION).await;
    expect_setup(&mut first).await;
    send_json(&mut first, json!(["stdin", "KEPT=one; echo a:$KEPT\n"])).await;
    read_until(&mut first, |out| has_line(out, "a:one")).await;
    let mut second = connect(addr, ADMIN_TAB_TWO).await;
    expect_setup(&mut second).await;
    send_json(&mut second, json!(["stdin", "echo b:${KEPT:-unset}\n"])).await;
    read_until(&mut second, |out| has_line(out, "b:one")).await;
    assert!(tmux_socket(tmux_dir.path()).exists());
    // The switch on: the team's own code could reach the socket, so tmux is refused.
    scope.plugins_write = Some(true);
    meerschaum_core::team_scope::write_scope_record(root.path(), &scope).unwrap();
    let why = cfg.tmux_refused().expect("refused");
    assert!(
        why.contains("'reviewers' may install its own plugins")
            && why.contains("Unix sockets are not denied to team code"),
        "{why}"
    );
}

/// 5.1: the restricted terminals follow the same rule as the administrators': while isolated
/// teams are recorded and their sandbox does not refuse pathname Unix sockets, a restricted
/// terminal runs without tmux, so neither the administrators' nor the restricted tmux server
/// listens for a team's process to connect to; where the sandbox refuses them, it uses the
/// restricted tmux server (`<socket>-r.sock`).
#[tokio::test]
async fn with_isolated_teams_the_bridge_leaves_local_tmux_sockets_unused() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!("SKIP: no tmux");
        return;
    }
    let root = tempfile::tempdir().unwrap();
    let scope = meerschaum_core::team_scope::TeamScope {
        team: "wetlands".into(),
        team_id: Some("id-wetlands".into()),
        plugins_write: Some(true),
        ..Default::default()
    };
    meerschaum_core::team_scope::write_scope_record(root.path(), &scope).unwrap();
    let state = state(root.path());
    for stops in [false, true] {
        let tmux_dir = short_tmp();
        let _server = PrivateTmux(tmux_dir.path().to_path_buf());
        let socket = tmux_socket(tmux_dir.path());
        let restricted = webterm::restricted_socket(&socket);
        struct KillSocket(std::path::PathBuf);
        impl Drop for KillSocket {
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
        let _restricted_server = KillSocket(restricted.clone());
        let tmux = TmuxSettings {
            binary: "tmux".into(),
            socket: Some(socket.clone()),
            session_suffix: format!("_r{}", stops as u8),
            env_dir: root.path().join(format!("webterm{}", stops as u8)),
            config_file: Some("/dev/null".into()),
        };
        let mut cfg = config(&state, sh(), Some(tmux));
        cfg.sandbox_stops_sockets = Some(stops);
        let addr = serve(webterm::routes(state.clone(), cfg)).await;
        let mut term = connect(addr, USER_SESSION).await;
        expect_setup(&mut term).await;
        send_json(&mut term, json!(["stdin", "fixture command\n"])).await;
        read_until(&mut term, |out| has_line(out, "fixture response")).await;
        assert!(!socket.exists(), "stops={stops}");
        assert!(!restricted.exists(), "stops={stops}");
    }
}
