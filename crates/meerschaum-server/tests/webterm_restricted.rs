//! 5.1: the restricted web terminal, server side (`meerschaum_server::webterm::restricted`).
//!
//! A user who may not run system commands gets a shell started with the restricted variables
//! (the policy socket's address and a grant); the policy socket answers with the decision of
//! `POST /execute` for the grant's user, read fresh; a grant ends with its terminal; a change of
//! role closes the terminal; on the tmux path the restricted sessions live on their own, locked
//! down tmux server, and an unrestricted session of the same user is ended rather than
//! reattached. Terminals run `sh` in scratch roots: the shell's own enforcement is
//! `meerschaum-cli`'s (`tests/webterm_restricted.rs` there drives the real `mrsm` shell).
#![cfg(unix)]

mod common;

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use common::{MemoryInstance, SECRET};
use futures_util::{SinkExt, StreamExt};
use meerschaum_core::{Map, SuccessTuple};
use meerschaum_server::backends::ActionsBackend;
use meerschaum_server::webterm::{self, SessionLookup, ShellCommand, TmuxSettings, WebtermConfig};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_tungstenite::tungstenite::Message;

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

const ADMIN_SESSION: &str = "11111111-1111-4111-8111-111111111111";
/// `bob`, an operator (`webterm:use`, `actions:execute`, `jobs:execute`).
const BOB_SESSION: &str = "33333333-3333-4333-8333-333333333333";

/// Actions the dispatcher knows (`show`, `sync`, `sh`); any other first word is the shell.
struct Actions;

#[async_trait]
impl ActionsBackend for Actions {
    fn list(&self) -> Vec<String> {
        vec!["show".into(), "sync".into(), "sh".into()]
    }
    async fn execute(&self, _action: &str, _keywords: Map) -> SuccessTuple {
        SuccessTuple::fail("not in this test")
    }
}

struct Sessions(Mutex<HashMap<String, String>>);

#[async_trait]
impl SessionLookup for Sessions {
    async fn username(&self, session_id: &str) -> Result<Option<String>, String> {
        Ok(self.0.lock().unwrap().get(session_id).cloned())
    }
}

fn sessions() -> Arc<Sessions> {
    Arc::new(Sessions(Mutex::new(
        [(ADMIN_SESSION, "admin"), (BOB_SESSION, "bob")]
            .into_iter()
            .map(|(id, user)| (id.to_string(), user.to_string()))
            .collect(),
    )))
}

fn state(root: &Path) -> (AppState, Arc<MemoryInstance>) {
    let mut cfg = ServerConfig::for_root("sql:fixture", root);
    cfg.actions_non_admin = true;
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("bob", "pass", None);
    let operator: Vec<String> = meerschaum_core::access::Role::Operator
        .permissions()
        .into_iter()
        .collect();
    let operator: Vec<&str> = operator.iter().map(String::as_str).collect();
    instance.seed_user_scopes("bob", &operator);
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .actions(Arc::new(Actions))
        .build()
        .expect("state");
    (state, instance)
}

fn sh() -> ShellCommand {
    let mut shell = ShellCommand::new("sh");
    shell.env.push(("PS1".into(), "$ ".into()));
    shell
}

fn config(state: &AppState, tmux: Option<TmuxSettings>) -> WebtermConfig {
    WebtermConfig::new(state, sessions(), Arc::new(|_| String::new()), sh())
        .with_tmux(tmux)
        .with_reauthorize_every(Duration::from_millis(300))
}

async fn serve(router: axum::Router) -> std::net::SocketAddr {
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

async fn type_line(socket: &mut Socket, line: &str) {
    socket
        .send(Message::Text(
            json!(["stdin", format!("{line}\n")]).to_string().into(),
        ))
        .await
        .unwrap();
}

/// Everything the terminal prints until `done` holds (or 30 seconds pass), and whether it closed.
async fn read_until(socket: &mut Socket, done: impl Fn(&str) -> bool) -> (String, bool) {
    let mut out = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !done(&out) {
        match tokio::time::timeout_at(deadline, socket.next()).await {
            Ok(Some(Ok(Message::Text(text)))) => {
                let value: Value = serde_json::from_str(text.as_str()).unwrap_or(Value::Null);
                if value[0] == "stdout" {
                    out.push_str(value[1].as_str().unwrap_or(""));
                }
            }
            Ok(Some(Ok(_))) => {}
            Ok(None | Some(Err(_))) | Err(_) => return (out, true),
        }
    }
    (out, false)
}

/// The text without terminal escape sequences; tmux's cursor moves become line breaks.
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
                        if matches!(c, 'H' | 'f' | 'd' | 'B' | 'E' | 'J') {
                            text.push('\n');
                        }
                        break;
                    }
                }
            }
            Some(']') | Some('P') | Some('k') => {
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

/// The value the shell prints after `KEY=` (`echo "KEY=$VAR"`), on a line of its own.
fn printed(out: &str, key: &str) -> Option<String> {
    plain(out)
        .split(['\r', '\n'])
        .filter_map(|line| line.trim().strip_prefix(&format!("{key}=")))
        .map(str::to_string)
        .find(|value| !value.starts_with('$') && !value.contains('"'))
}

/// The restricted variables the terminal's shell sees.
async fn restriction(socket: &mut Socket) -> (Option<String>, Option<String>, Option<String>) {
    type_line(
        socket,
        "echo \"R=${MRSM_WEBTERM_RESTRICTED:-none}\"; echo \"A=${MRSM_WEBTERM_POLICY:-none}\"; \
         echo \"G=${MRSM_WEBTERM_GRANT:-none}\"; echo \"END=done\"",
    )
    .await;
    let (out, _) = read_until(socket, |out| printed(out, "END").is_some()).await;
    let get = |key| printed(&out, key).filter(|v| v != "none");
    (get("R"), get("A"), get("G"))
}

/// Ask the policy socket at `address` (`unix:<path>`).
async fn ask(address: &str, grant: &str, sysargs: &[&str]) -> Value {
    let path = address
        .strip_prefix("unix:")
        .expect("a Unix socket address");
    let mut stream = tokio::net::UnixStream::connect(path).await.unwrap();
    let question = json!({"grant": grant, "sysargs": sysargs});
    stream
        .write_all(format!("{question}\n").as_bytes())
        .await
        .unwrap();
    let mut reply = String::new();
    stream.read_to_string(&mut reply).await.unwrap();
    serde_json::from_str(reply.trim()).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_non_administrators_shell_asks_the_server_which_decides_as_post_execute_does() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, None))).await;

    // The administrator's shell is not restricted.
    let mut admin = connect(addr, ADMIN_SESSION).await;
    let (restricted, address, grant) = restriction(&mut admin).await;
    assert_eq!((restricted, address, grant), (None, None, None));

    // bob's is, with the policy socket's address and a grant.
    let mut bob = connect(addr, BOB_SESSION).await;
    let (restricted, address, grant) = restriction(&mut bob).await;
    assert_eq!(restricted.as_deref(), Some("1"));
    let address = address.expect("the policy address");
    let grant = grant.expect("a grant");
    assert_eq!(grant.len(), 64);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let path = address.strip_prefix("unix:").unwrap();
        let mode = std::fs::metadata(path).unwrap().permissions().mode();
        assert_eq!(mode & 0o777, 0o600);
    }

    // What `POST /execute` allows bob, and nothing else.
    let yes = ask(&address, &grant, &["show", "version"]).await;
    assert_eq!(yes["success"], true, "{yes}");
    for refused in [
        &["touch", "/tmp/tpr-x"][..],
        &["!touch", "/tmp/tpr-x"],
        &["sh", "touch", "/tmp/tpr-x"],
        &["show", "version", "+", "touch", "/tmp/tpr-x"],
        &["show", "version", "++", "touch", "/tmp/tpr-x"],
        &["touch", "/tmp/tpr-x", ":", "--loop"],
        &["python"],
        &["show", "version", "--shell"],
        &["show", "config"],
        &["instance", "sql:other"],
        &["stack", "up"],
        &["start", "job", "touch", "/tmp/tpr-x"],
        &["chain", "-P", r#"{"sub_args_line": "touch /tmp/tpr-x"}"#],
    ] {
        let answer = ask(&address, &grant, refused).await;
        assert_eq!(answer["success"], false, "{refused:?}: {answer}");
        assert!(
            answer["message"].as_str().is_some_and(|m| !m.is_empty()),
            "{answer}"
        );
    }
    let shell = ask(&address, &grant, &["touch", "/tmp/tpr-x"]).await;
    assert!(
        shell["message"]
            .as_str()
            .unwrap()
            .contains("not a Meerschaum action"),
        "{shell}"
    );
    // A made-up grant, or none, is refused.
    let forged = ask(&address, &"0".repeat(64), &["show", "version"]).await;
    assert_eq!(forged["success"], false);
    assert!(
        forged["message"]
            .as_str()
            .unwrap()
            .contains("no longer authorized")
    );
    assert_eq!(
        ask(&address, "", &["show", "version"]).await["success"],
        false
    );

    // The decision reads the user now: without `webterm:use` (which a scope list written before
    // 5.0 derives from `actions:execute`), nothing runs.
    instance.seed_user_scopes("bob", &["pipes:read"]);
    let answer = ask(&address, &grant, &["show", "version"]).await;
    assert_eq!(answer["success"], false, "{answer}");
    // (The terminal itself closes at its next check.)
    let (out, closed) = read_until(&mut bob, |out| out.contains("permission")).await;
    assert!(out.contains("permission") || closed, "{out:?}");
    drop(bob);
    // The grant ended with the terminal.
    let operator: Vec<String> = meerschaum_core::access::Role::Operator
        .permissions()
        .into_iter()
        .collect();
    let operator: Vec<&str> = operator.iter().map(String::as_str).collect();
    instance.seed_user_scopes("bob", &operator);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let ended = ask(&address, &grant, &["show", "version"]).await;
    assert_eq!(ended["success"], false, "{ended}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_change_of_role_closes_the_terminal_in_either_direction() {
    use meerschaum_connector::InstanceConnector;
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = state(root.path());
    let addr = serve(webterm::routes(state.clone(), config(&state, None))).await;

    let mut bob = connect(addr, BOB_SESSION).await;
    assert!(restriction(&mut bob).await.0.is_some());
    let mut promoted = meerschaum_core::users::User::new("bob", None);
    promoted.user_type = Some("admin".into());
    assert!(instance.edit_user(&promoted).await.success);
    let (out, _) = read_until(&mut bob, |out| out.contains("Your role changed")).await;
    assert!(out.contains("Your role changed"), "{out:?}");
    let mut bob = connect(addr, BOB_SESSION).await;
    assert!(restriction(&mut bob).await.0.is_none());

    let mut demoted = meerschaum_core::users::User::new("bob", None);
    demoted.user_type = Some("user".into());
    assert!(instance.edit_user(&demoted).await.success);
    let (out, _) = read_until(&mut bob, |out| out.contains("Your role changed")).await;
    assert!(out.contains("Your role changed"), "{out:?}");
    let mut bob = connect(addr, BOB_SESSION).await;
    assert!(restriction(&mut bob).await.0.is_some());
}

fn tmux(socket: &Path, args: &[&str]) -> std::process::Output {
    std::process::Command::new("tmux")
        .arg("-S")
        .arg(socket)
        .args(args)
        .env_remove("TMUX")
        .env_remove("TMUX_PANE")
        .stdin(std::process::Stdio::null())
        .output()
        .unwrap()
}

/// Kills the test's tmux servers (by their sockets, never the default one) however it ends.
struct PrivateTmux(Vec<std::path::PathBuf>);

impl Drop for PrivateTmux {
    fn drop(&mut self) {
        for socket in &self.0 {
            let _ = tmux(socket, &["kill-server"]);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn restricted_tmux_sessions_live_on_a_locked_down_server_of_their_own() {
    let probe = std::process::Command::new("tmux")
        .arg("-V")
        .env_remove("TMUX")
        .output();
    if !probe.is_ok_and(|o| o.status.success()) {
        eprintln!(
            "SKIP restricted_tmux_sessions_live_on_a_locked_down_server_of_their_own: no tmux"
        );
        return;
    }
    let root = tempfile::tempdir().unwrap();
    // tmux's socket path must fit in `sun_path`.
    let dir = tempfile::Builder::new()
        .prefix("mt")
        .tempdir_in("/tmp")
        .unwrap();
    let socket = dir.path().join("tmux.sock");
    let restricted_socket = webterm::restricted_socket(&socket);
    let _servers = PrivateTmux(vec![socket.clone(), restricted_socket.clone()]);
    let (state, _instance) = state(root.path());
    let settings = TmuxSettings {
        binary: "tmux".into(),
        socket: Some(socket.clone()),
        session_suffix: "_t".into(),
        env_dir: dir.path().join("env"),
        config_file: Some("/dev/null".into()),
    };
    let addr = serve(webterm::routes(
        state.clone(),
        config(&state, Some(settings)),
    ))
    .await;

    // A session of bob's on the administrators' server (made while bob could run anything).
    let made = tmux(
        &socket,
        &[
            "-f",
            "/dev/null",
            "new-session",
            "-d",
            "-s",
            "bob_t",
            "sleep 600",
        ],
    );
    assert!(made.status.success(), "{made:?}");

    let mut bob = connect(addr, BOB_SESSION).await;
    let (restricted, _, grant) = restriction(&mut bob).await;
    assert_eq!(restricted.as_deref(), Some("1"));
    assert!(grant.is_some());
    // The old session was ended rather than reattached; bob's is on the restricted server.
    let sessions = |socket: &Path| {
        String::from_utf8_lossy(&tmux(socket, &["list-sessions", "-F", "#{session_name}"]).stdout)
            .lines()
            .map(str::to_string)
            .collect::<Vec<_>>()
    };
    assert!(!sessions(&socket).contains(&"bob_t".to_string()));
    assert_eq!(sessions(&restricted_socket), ["bob_t"]);
    // Locked down: exactly Meerschaum's bindings, no prefix key, panes started otherwise run
    // nothing.
    let keys =
        String::from_utf8_lossy(&tmux(&restricted_socket, &["list-keys"]).stdout).into_owned();
    assert_eq!(
        webterm::restricted::only_allowed_bindings(&keys),
        Ok(()),
        "{keys}"
    );
    for (option, value) in [
        ("prefix", "None"),
        ("prefix2", "None"),
        ("default-command", "exit 0"),
    ] {
        let shown = tmux(&restricted_socket, &["show-options", "-gv", option]);
        assert_eq!(
            String::from_utf8_lossy(&shown.stdout).trim(),
            value,
            "{option}"
        );
    }
    // The environment file was read and removed.
    let left: Vec<_> = std::fs::read_dir(dir.path().join("env"))
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.ends_with(".env"))
        .collect();
    assert!(left.is_empty(), "{left:?}");

    // The console's new window runs the restricted shell in bob's session, on that server.
    bob.send(Message::Text(json!(["new_window"]).to_string().into()))
        .await
        .unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let windows = tmux(
            &restricted_socket,
            &["list-windows", "-t", "=bob_t", "-F", "#I"],
        );
        if String::from_utf8_lossy(&windows.stdout).lines().count() == 2 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "no second window");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (restricted, _, _) = restriction(&mut bob).await;
    assert_eq!(restricted.as_deref(), Some("1"));
    // A reconnect reattaches bob's restricted session (its grant is known).
    drop(bob);
    let mut bob = connect(addr, BOB_SESSION).await;
    assert_eq!(restriction(&mut bob).await.0.as_deref(), Some("1"));
    assert_eq!(sessions(&restricted_socket), ["bob_t"]);
    drop(bob);

    // The administrator's session is on the administrators' server, unrestricted.
    let mut admin = connect(addr, ADMIN_SESSION).await;
    assert!(restriction(&mut admin).await.0.is_none());
    assert!(sessions(&socket).contains(&"admin_t".to_string()));
}
