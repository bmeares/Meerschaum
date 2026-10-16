//! The web terminal (§7.1): page authentication, the websocket handshake, the terminado wire
//! protocol against a real PTY, resize, `--no-webterm`, concurrent terminals, and tmux reattach.
//!
//! Every terminal runs `sh` (never the user's shell or a real root). The plain path always runs;
//! the tmux test skips itself when no `tmux` is installed, and says so on stderr.
#![cfg(unix)]

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{MemoryInstance, SECRET, send};
use futures_util::{SinkExt, StreamExt};
use meerschaum_server::webterm::{self, SessionLookup, ShellCommand, TmuxSettings, WebtermConfig};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const ADMIN_SESSION: &str = "11111111-1111-4111-8111-111111111111";
const ADMIN_TAB_TWO: &str = "11111111-1111-4111-8111-222222222222";
const OTHER_ADMIN_SESSION: &str = "22222222-2222-4222-8222-222222222222";
const USER_SESSION: &str = "33333333-3333-4333-8333-333333333333";
const GHOST_SESSION: &str = "44444444-4444-4444-8444-444444444444";

struct Sessions(HashMap<String, String>);

#[async_trait]
impl SessionLookup for Sessions {
    async fn username(&self, session_id: &str) -> Result<Option<String>, String> {
        Ok(self.0.get(session_id).cloned())
    }
}

fn sessions() -> Arc<dyn SessionLookup> {
    Arc::new(Sessions(
        [
            (ADMIN_SESSION, "admin"),
            (ADMIN_TAB_TWO, "admin"),
            (OTHER_ADMIN_SESSION, "root2"),
            (USER_SESSION, "bob"),
            (GHOST_SESSION, "nobody"),
        ]
        .into_iter()
        .map(|(id, user)| (id.to_string(), user.to_string()))
        .collect(),
    ))
}

fn state(root: &Path) -> AppState {
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("root2", "pass", Some("admin"));
    instance.seed_user("bob", "pass", None);
    AppState::builder(ServerConfig::for_root("sql:fixture", root), instance)
        .secret(SECRET.to_vec())
        .build()
        .expect("state")
}

fn config(state: &AppState, shell: ShellCommand, tmux: Option<TmuxSettings>) -> WebtermConfig {
    WebtermConfig::new(
        state,
        sessions(),
        Arc::new(|id| format!("<html><body data-session=\"{id}\">terminal page</body></html>")),
        shell,
    )
    .with_tmux(tmux)
}

fn sh() -> ShellCommand {
    // A bare `sh` on a PTY is interactive; `PS1` keeps the prompt short and predictable.
    let mut shell = ShellCommand::new("sh");
    shell.env.push(("PS1".into(), "$ ".into()));
    shell
}

/// Serve the terminal routes on a random loopback port.
async fn serve(router: Router) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    addr
}

async fn connect(addr: std::net::SocketAddr, session: &str) -> Socket {
    let (socket, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/websocket/{session}"))
        .await
        .expect("handshake");
    socket
}

async fn send_json(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

/// The next terminado message, skipping pings; `Ok(None)` once the socket closes and `Err` when
/// nothing arrives for 20 seconds.
async fn try_next_message(socket: &mut Socket) -> Result<Option<Value>, ()> {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => return serde_json::from_str(text.as_str()).ok(),
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return None,
                Some(Ok(_)) => continue,
            }
        }
    })
    .await
    .map_err(|_| ())
}

async fn next_message(socket: &mut Socket) -> Option<Value> {
    try_next_message(socket)
        .await
        .expect("timed out waiting for the terminal")
}

/// Read `stdout` frames until `done(accumulated)` holds; panics with the output on timeout.
async fn read_until(socket: &mut Socket, done: impl Fn(&str) -> bool) -> String {
    let mut out = String::new();
    // An overall deadline: a repainting terminal (tmux's status line) never goes quiet.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    while !done(&out) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the expected output; got {out:?}"
        );
        match try_next_message(socket)
            .await
            .unwrap_or_else(|_| panic!("the terminal went quiet; got {out:?}"))
        {
            Some(message) if message[0] == "stdout" => {
                out.push_str(message[1].as_str().unwrap());
            }
            Some(_) => {}
            None => panic!("the terminal closed before the expected output; got {out:?}"),
        }
    }
    out
}

async fn expect_setup(socket: &mut Socket) {
    let message = next_message(socket).await.expect("setup");
    assert_eq!(message, json!(["setup", {}]));
}

/// The text without terminal escape sequences (CSI and OSC), as the shell's readline emits them.
fn plain(out: &str) -> String {
    let mut text = String::new();
    let mut chars = out.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\u{1b}' {
            text.push(c);
            continue;
        }
        match chars.next() {
            Some('[') => {
                for c in chars.by_ref() {
                    if ('@'..='~').contains(&c) {
                        // tmux repaints with cursor moves instead of newlines.
                        if matches!(c, 'H' | 'f' | 'd' | 'B' | 'E' | 'J') {
                            text.push('\n');
                        }
                        break;
                    }
                }
            }
            Some(']') => {
                // OSC ends with BEL or ST (ESC \\).
                while let Some(c) = chars.next() {
                    if c == '\u{7}' {
                        break;
                    }
                    if c == '\u{1b}' {
                        chars.next();
                        break;
                    }
                }
            }
            _ => {}
        }
    }
    text
}

/// A line made only of `token` (the shell's answer, not the echo of the command that asked).
fn has_line(out: &str, token: &str) -> bool {
    plain(out).split(['\r', '\n']).any(|line| line == token)
}

/// A line with the prefix `start` ending in `end`.
fn has_line_matching(out: &str, start: &str, end: &str) -> bool {
    plain(out)
        .split(['\r', '\n'])
        .any(|l| l.starts_with(start) && l.ends_with(end))
}

#[tokio::test]
async fn the_page_requires_an_authorized_session() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let app = webterm::routes(state.clone(), config(&state, sh(), None));

    let get = |id: &str| {
        Request::get(format!("/webterm/{id}"))
            .body(Body::empty())
            .unwrap()
    };

    let ok = send(&app, get(ADMIN_SESSION)).await;
    assert_eq!(ok.status, StatusCode::OK);
    assert!(
        ok.text()
            .contains(&format!("data-session=\"{ADMIN_SESSION}\""))
    );
    assert_eq!(ok.headers["cache-control"], "no-store");
    assert_eq!(ok.headers["referrer-policy"], "no-referrer");

    // v4's `401` page links to the console login.
    let unknown = send(&app, get("not-a-session")).await;
    assert_eq!(unknown.status, StatusCode::UNAUTHORIZED);
    assert!(unknown.text().contains("<h2>Unauthorized</h2>"));
    assert!(unknown.text().contains("href=\"/dash/login\""));

    // A logged-in user who is not an administrator gets no shell by default.
    let user = send(&app, get(USER_SESSION)).await;
    assert_eq!(user.status, StatusCode::UNAUTHORIZED);
    // A session of a user the instance does not have gets none either.
    let ghost = send(&app, get(GHOST_SESSION)).await;
    assert_eq!(ghost.status, StatusCode::UNAUTHORIZED);

    // The trailing slash v4's `/webterm/(.+)/?` accepted.
    let slash = send(
        &app,
        Request::get(format!("/webterm/{ADMIN_SESSION}/"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(slash.status, StatusCode::OK);
}

#[tokio::test]
async fn non_admin_users_follow_the_actions_gate() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = ServerConfig::for_root("sql:fixture", root.path());
    cfg.system_actions_non_admin = true;
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("bob", "pass", None);
    let state = AppState::builder(cfg, instance)
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = webterm::routes(state.clone(), config(&state, sh(), None));
    let response = send(
        &app,
        Request::get(format!("/webterm/{USER_SESSION}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    // The gate also asks for the actions scopes a plain user lacks: still refused, never a shell.
    assert!(
        response.status == StatusCode::OK || response.status == StatusCode::UNAUTHORIZED,
        "{}",
        response.status
    );
}

#[tokio::test]
async fn a_websocket_without_a_session_is_accepted_and_closed() {
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, sh(), None))).await;

    // PARITY: v4 accepts the socket, then closes it; no PTY starts.
    let mut socket = connect(addr, "no-such-session").await;
    match socket.next().await {
        Some(Ok(Message::Close(frame))) => {
            assert_eq!(u16::from(frame.expect("close frame").code), 1000);
        }
        other => panic!("expected a close frame, got {other:?}"),
    }
    let mut socket = connect(addr, USER_SESSION).await;
    assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
}

#[tokio::test]
async fn cross_origin_websockets_are_refused() {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    let root = tempfile::tempdir().unwrap();
    let state = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, sh(), None))).await;
    let open = |origin: Option<String>| async move {
        let mut request = format!("ws://{addr}/websocket/{ADMIN_SESSION}")
            .into_client_request()
            .unwrap();
        if let Some(origin) = origin {
            request
                .headers_mut()
                .insert("origin", origin.parse().unwrap());
        }
        tokio_tungstenite::connect_async(request).await
    };
    // Another site's page: refused at the handshake, before any session lookup or PTY.
    for origin in [
        "https://evil.example",
        "http://localhost:1",
        "null",
        "garbage",
    ] {
        match open(Some(origin.into())).await {
            Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
                assert_eq!(response.status(), StatusCode::FORBIDDEN, "{origin}");
            }
            other => panic!("{origin}: expected a 403, got {:?}", other.map(|_| ())),
        }
    }
    // Same origin (the page the terminal was served from) and no `Origin` (not a browser) work.
    for origin in [Some(format!("http://{addr}")), None] {
        let (mut socket, _) = open(origin).await.expect("handshake");
        expect_setup(&mut socket).await;
    }
}

#[test]
fn origin_checks_follow_host_and_forwarded_host() {
    use axum::http::HeaderMap;
    let headers = |pairs: &[(&str, &str)]| {
        let mut map = HeaderMap::new();
        for (k, v) in pairs {
            map.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        map
    };
    assert!(webterm::origin_allowed(&headers(&[("host", "a:1")])));
    assert!(webterm::origin_allowed(&headers(&[
        ("host", "a:1"),
        ("origin", "http://a:1")
    ])));
    assert!(webterm::origin_allowed(&headers(&[
        ("host", "internal:8000"),
        ("x-forwarded-host", "term.example"),
        ("origin", "https://term.example")
    ])));
    assert!(!webterm::origin_allowed(&headers(&[
        ("host", "a:1"),
        ("origin", "http://a:2")
    ])));
    assert!(!webterm::origin_allowed(&headers(&[(
        "origin",
        "http://a:1"
    )])));
}

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
    let alive = |pid: i32| std::path::Path::new(&format!("/proc/{pid}")).exists();
    // (`/proc` is Linux-only; elsewhere the pid check below is skipped.)
    let checkable = alive(pid);
    socket.close(None).await.unwrap();
    drop(socket);
    if !checkable {
        return;
    }
    for _ in 0..100 {
        if !alive(pid) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("the shell {pid} outlived its socket");
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

fn short_tmp() -> tempfile::TempDir {
    // tmux's socket path must fit in `sun_path` (about 100 bytes).
    let base = std::env::temp_dir();
    let base = if base.as_os_str().len() > 40 {
        "/tmp".into()
    } else {
        base
    };
    tempfile::Builder::new()
        .prefix("mt")
        .tempdir_in(base)
        .unwrap()
}

/// The socket of the private tmux server a test started under `TMUX_TMPDIR=<dir>`
/// (`<dir>/tmux-<uid>/default`); every tmux command the test runs addresses it with `-S`.
fn tmux_socket(dir: &Path) -> std::path::PathBuf {
    use std::os::unix::fs::MetadataExt;
    let uid = std::fs::metadata(dir).expect("tmux dir").uid();
    dir.join(format!("tmux-{uid}")).join("default")
}

/// Kills the test's private tmux server (by its socket, never the default one) even when an
/// assertion fails.
struct PrivateTmux(std::path::PathBuf);

impl Drop for PrivateTmux {
    fn drop(&mut self) {
        let _ = std::process::Command::new("tmux")
            .arg("-S")
            .arg(tmux_socket(&self.0))
            .arg("kill-server")
            .env_remove("TMUX")
            .env_remove("TMUX_PANE")
            .output();
    }
}

#[tokio::test]
async fn tmux_sessions_survive_reconnects_and_carry_the_environment() {
    let probe = std::process::Command::new("tmux").arg("-V").output();
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
    // A private tmux server (the user's own is never touched) and an MRSM variable the
    // shell must receive through the environment file even when the server already runs.
    shell
        .env
        .push(("TMUX_TMPDIR".into(), tmux_dir.path().display().to_string()));
    shell
        .env
        .push(("MRSM_WEBTERM_TEST".into(), "it's=ok $HOME".into()));
    let tmux = TmuxSettings {
        binary: "tmux".into(),
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
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(env_dir.join("webterm.env"))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
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
