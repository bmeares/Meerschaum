//! The restricted web terminal (5.1): what a user who may not run system commands
//! ([`crate::execute_policy::may_run_system_commands`]) gets in the terminal.
//!
//! - **The shell asks the server.** The terminal's `mrsm` shell starts with
//!   [`RESTRICTED_ENV`], the address of this server's policy socket ([`POLICY_ADDRESS_ENV`]) and
//!   a grant ([`GRANT_ENV`]): a random token this server minted for the user when it started the
//!   shell. Before the shell runs a line (the command line its dispatcher would run, `+` chains,
//!   `:` flags and all), it sends it here, and this server answers with
//!   [`crate::execute_policy::authorize_command`] for the grant's user, read fresh: what `POST
//!   /execute` would allow that user. Anything else (no answer, an unknown grant, a malformed
//!   reply) is a refusal. The shell starts no other program (`meerschaum-cli`'s
//!   `shell::restricted`).
//! - **The mode is the server's.** The server decides it from the session's user when the
//!   terminal opens and again at every check of an open terminal ([`super::REAUTHORIZE_EVERY`]);
//!   a change of role closes the terminal, and the next one opens in the new mode. Nothing typed
//!   in the terminal can change the shell's environment (the variables are read once when the
//!   shell starts), and every decision is made here, from the grant's user, not from anything the
//!   shell says about itself.
//! - **tmux.** Restricted sessions live on a tmux server of their own (a socket next to the
//!   administrators' one, [`restricted_socket`]) whose configuration
//!   ([`TMUX_RESTRICTED_CONF`]) unbinds every key of every table (no prefix key, no command
//!   prompt, no `new-window`, `run-shell`, `respawn-pane`, `choose-tree`, copy-mode pipes or
//!   menus), keeps only scrolling and the window tabs, and makes a pane created any other way run
//!   nothing (`default-command "exit 0"`). It is applied again every time a restricted terminal
//!   opens, and a terminal whose configuration cannot be applied does not open.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};

use crate::state::AppState;

/// Set (to `1`) in the environment of a restricted terminal's shell.
pub const RESTRICTED_ENV: &str = "MRSM_WEBTERM_RESTRICTED";

/// The address of the policy socket a restricted shell asks: `unix:<path>` (Unix) or
/// `tcp:127.0.0.1:<port>` (elsewhere).
pub const POLICY_ADDRESS_ENV: &str = "MRSM_WEBTERM_POLICY";

/// The grant a restricted shell presents with every question.
pub const GRANT_ENV: &str = "MRSM_WEBTERM_GRANT";

/// Host-selected marker for an API-only shell's rotating Main HTTP policy transport.
/// Ordinary restricted shells never interpret this as a local policy address.
pub const API_POLICY_ADDRESS: &str = "api:callback";

/// The largest question accepted (a command line).
const MAX_QUESTION_BYTES: usize = 1024 * 1024;

/// How long a question may take to arrive.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// How long the server may take to decide (instance reads).
const DECIDE_TIMEOUT: Duration = Duration::from_secs(60);

/// Meerschaum's tmux configuration for the restricted terminals' own tmux server: the window list
/// of [`super::TMUX_CONF`], and nothing a key, a click or a menu could use to start a program,
/// change a pane's command, switch to another session or run a tmux command. Only options every
/// tmux release in use knows (an unknown one fails `source-file`, and the terminal does not
/// open).
pub const TMUX_RESTRICTED_CONF: &str = "\
# The restricted web terminal's tmux configuration, written by Meerschaum. This tmux server runs
# only the terminals of users who may not run system commands: no key reaches tmux itself.
set -g prefix None
set -g prefix2 None
# A table whose last key is unbound stops existing, and unbinding a table that does not exist is
# an error: give each one a harmless key first, so that applying this file again succeeds.
bind-key -T prefix F12 select-pane
bind-key -T root F12 select-pane
bind-key -T copy-mode F12 send-keys -X cancel
bind-key -T copy-mode-vi F12 send-keys -X cancel
unbind-key -a -T prefix
unbind-key -a -T root
unbind-key -a -T copy-mode
unbind-key -a -T copy-mode-vi
# A pane started any other way than Meerschaum's runs nothing.
set -g default-shell /bin/sh
set -g default-command \"exit 0\"
set -g copy-command \"\"
set -g lock-after-time 0
set -g lock-command \"true\"
setw -g remain-on-exit off
setw -g automatic-rename off
set -g allow-rename off
set -g set-titles off
set -g destroy-unattached off
set -g exit-empty on
# The session's windows, as tabs on the first row (a click selects one).
set -g status on
set -g status-position top
set -g status-interval 0
set -g status-justify left
set -g status-style \"bg=#17212b,fg=#a8becf\"
set -g status-left \"\"
set -g status-right \"\"
set -g window-status-separator \"\"
set -g window-status-format \" #I #W \"
set -g window-status-current-format \" #I #W \"
set -g window-status-current-style \"bg=#213449,fg=#85caff,bold\"
set -g base-index 1
set -g renumber-windows on
setw -g pane-base-index 1
set -g history-limit 50000
set -g default-terminal \"screen-256color\"
set -ga terminal-overrides \",xterm-256color:Tc\"
set -sg escape-time 10
set -g focus-events on
setw -g aggressive-resize on
# Scrolling with the mouse wheel, and selecting text, without anything that pipes or pastes.
set -g mouse on
bind-key -T root MouseDown1Status select-window -t =
bind-key -T root WheelUpPane if-shell -F \"#{||:#{pane_in_mode},#{mouse_any_flag}}\" \"send-keys -M\" \"copy-mode -e\"
bind-key -T copy-mode WheelUpPane send-keys -X scroll-up
bind-key -T copy-mode WheelDownPane send-keys -X scroll-down
bind-key -T copy-mode Up send-keys -X scroll-up
bind-key -T copy-mode Down send-keys -X scroll-down
bind-key -T copy-mode PPage send-keys -X page-up
bind-key -T copy-mode NPage send-keys -X page-down
bind-key -T copy-mode q send-keys -X cancel
bind-key -T copy-mode Escape send-keys -X cancel
bind-key -T copy-mode MouseDrag1Pane send-keys -X begin-selection
bind-key -T copy-mode MouseDragEnd1Pane send-keys -X copy-selection-and-cancel
";

/// The key tables [`TMUX_RESTRICTED_CONF`] keeps keys in; every other table (tmux has more than
/// the four it empties, and releases add tables) is emptied when the configuration is applied.
pub const KEPT_TABLES: &[&str] = &["root", "copy-mode"];

/// The bindings [`TMUX_RESTRICTED_CONF`] makes, as `(table, key, command)` (the placeholders it
/// removes again excepted): after the configuration is applied, tmux must list exactly these.
pub fn allowed_bindings() -> Vec<(String, String, String)> {
    TMUX_RESTRICTED_CONF
        .lines()
        .filter(|line| line.starts_with("bind-key "))
        .filter_map(binding)
        .filter(|(_, key, _)| key != "F12")
        .collect()
}

/// The table, key and command of a `bind-key` line, as written in a configuration or as
/// `list-keys` prints it (`bind-key [-r] -T <table> <key> <command>`), the command's words
/// separated by single spaces.
pub fn binding(line: &str) -> Option<(String, String, String)> {
    let mut words = line.split_whitespace();
    words.find(|word| *word == "-T")?;
    let table = words.next()?.to_string();
    let key = words.next()?.to_string();
    let command = words.collect::<Vec<_>>().join(" ");
    Some((table, key, command))
}

/// `Ok` when a `list-keys` listing has nothing but [`allowed_bindings`].
pub fn only_allowed_bindings(listing: &str) -> Result<(), String> {
    let allowed = allowed_bindings();
    for line in listing.lines().filter(|l| !l.trim().is_empty()) {
        match binding(line) {
            Some(found) if allowed.contains(&found) => {}
            _ => return Err(format!("an unexpected tmux key binding: {}", line.trim())),
        }
    }
    Ok(())
}

/// The restricted terminals' tmux socket next to `socket` (the administrators' one):
/// `tmux.sock` -> `tmux-r.sock`.
pub fn restricted_socket(socket: &Path) -> PathBuf {
    let stem = socket
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_else(|| "tmux".into());
    socket.with_file_name(format!("{stem}-r.sock"))
}

/// One shell's grant: the user it asks for.
#[derive(Debug, Clone)]
struct Grant {
    username: String,
}

#[derive(Default)]
struct Grants {
    /// Token -> grant.
    by_token: HashMap<String, Grant>,
    /// tmux session name -> the token of the shell it runs.
    by_session: HashMap<String, String>,
}

/// The policy socket and the grants of the restricted shells this server started. Clones share
/// both.
#[derive(Clone, Default)]
pub struct Policy {
    grants: Arc<Mutex<Grants>>,
    address: Arc<tokio::sync::OnceCell<String>>,
}

/// Removes a connection's own grant when the connection ends (the plain path: one shell per
/// connection).
pub struct GrantGuard {
    policy: Policy,
    token: String,
}

impl GrantGuard {
    /// The grant's token.
    pub fn token(&self) -> &str {
        &self.token
    }
}

impl Drop for GrantGuard {
    fn drop(&mut self) {
        self.policy.revoke(&self.token);
    }
}

fn new_token() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

impl Policy {
    fn lock(&self) -> std::sync::MutexGuard<'_, Grants> {
        self.grants.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A grant for one shell of `username` that lives as long as the returned guard.
    pub fn grant_for_connection(&self, username: &str) -> GrantGuard {
        let token = new_token();
        self.lock().by_token.insert(
            token.clone(),
            Grant {
                username: username.to_string(),
            },
        );
        GrantGuard {
            policy: self.clone(),
            token,
        }
    }

    /// A grant for the shells of the tmux `session` of `username` (replacing the session's
    /// previous one).
    pub fn grant_for_session(&self, username: &str, session: &str) -> String {
        let token = new_token();
        let mut grants = self.lock();
        if let Some(old) = grants.by_session.insert(session.to_string(), token.clone()) {
            grants.by_token.remove(&old);
        }
        grants.by_token.insert(
            token.clone(),
            Grant {
                username: username.to_string(),
            },
        );
        token
    }

    /// The grant of the tmux `session`'s shells when this server started them for `username`.
    pub fn session_grant(&self, username: &str, session: &str) -> Option<String> {
        let grants = self.lock();
        let token = grants.by_session.get(session)?;
        (grants.by_token.get(token)?.username == username).then(|| token.clone())
    }

    /// Forget the grant of the tmux `session` (it was ended).
    pub fn forget_session(&self, session: &str) {
        let mut grants = self.lock();
        if let Some(token) = grants.by_session.remove(session) {
            grants.by_token.remove(&token);
        }
    }

    fn revoke(&self, token: &str) {
        self.lock().by_token.remove(token);
    }

    fn username(&self, token: &str) -> Option<String> {
        self.lock()
            .by_token
            .get(token)
            .map(|grant| grant.username.clone())
    }

    /// The address restricted shells ask ([`POLICY_ADDRESS_ENV`]), listening from the first
    /// call on (in `dir`, the webterm resources directory).
    pub async fn listen(&self, state: &AppState, dir: &Path) -> std::io::Result<String> {
        let address = self
            .address
            .get_or_try_init(|| async {
                let policy = self.clone();
                let upgrade = state.weak_upgrade();
                bind_and_serve(dir, move |question| {
                    let policy = policy.clone();
                    let state = upgrade();
                    async move {
                        match state {
                            Some(state) => policy.answer(&state, question).await,
                            None => refusal("The server is shutting down."),
                        }
                    }
                })
                .await
            })
            .await?;
        Ok(address.clone())
    }

    /// The answer to one question (`{"grant": ..., "sysargs": [...]}`).
    async fn answer(&self, state: &AppState, question: Value) -> Value {
        let grant = question.get("grant").and_then(Value::as_str).unwrap_or("");
        let sysargs: Option<Vec<String>> = question
            .get("sysargs")
            .and_then(Value::as_array)
            .and_then(|items| {
                items
                    .iter()
                    .map(|v| v.as_str().map(str::to_string))
                    .collect()
            });
        let Some(username) = self.username(grant) else {
            return refusal(
                "This terminal is no longer authorized (the server restarted, or your role \
                 changed). Close it and open the terminal again.",
            );
        };
        let Some(sysargs) = sysargs.filter(|words| !words.is_empty()) else {
            return refusal("Nothing to run.");
        };
        match tokio::time::timeout(DECIDE_TIMEOUT, decide(state, &username, &sysargs)).await {
            Ok(Ok(())) => json!({"success": true, "message": "You may run this command."}),
            Ok(Err(message)) => refusal(&message),
            Err(_) => refusal("The server took too long to decide; nothing was run."),
        }
    }
}

fn refusal(message: &str) -> Value {
    json!({"success": false, "message": message})
}

/// What `POST /execute` would decide for `username` and `sysargs`, with the user's identity read
/// now: the user must still exist and still be allowed the terminal.
async fn decide(state: &AppState, username: &str, sysargs: &[String]) -> Result<(), String> {
    let user = meerschaum_core::users::User::new(username, None);
    match state.instance().get_user_id(&user).await {
        Ok(Some(id)) if !id.is_null() => {}
        Ok(_) => return Err("Your account no longer exists.".into()),
        Err(error) => return Err(format!("Could not read your account: {error}")),
    }
    let principal = crate::auth::Principal::User(user);
    let granted = crate::auth::Scopes::List(vec!["*".into()]);
    let access = crate::access::access_of_fresh(state, principal, &granted)
        .await
        .map_err(|e| e.detail_text())?;
    if !access.is_admin() && (!state.config.actions_non_admin || !access.has("webterm:use")) {
        return Err("You no longer have permission to use the terminal.".into());
    }
    access
        .require(&["jobs:execute"], &[])
        .map_err(|e| e.detail_text())?;
    // The restricted shell runs in this server's process, with its credentials.
    crate::execute_policy::authorize_command(
        state,
        &access,
        sysargs,
        crate::execute_policy::Place::Server,
    )
    .await
    .map(|_| ())
    .map_err(|e| e.detail_text())
}

/// Read one JSON line, answer it with one JSON line.
async fn serve_one<S, F, Fut>(stream: S, answer: F)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
    F: FnOnce(Value) -> Fut,
    Fut: std::future::Future<Output = Value>,
{
    let (reader, mut writer) = tokio::io::split(stream);
    let mut reader = tokio::io::BufReader::new(reader).take(MAX_QUESTION_BYTES as u64 + 1);
    let mut line = Vec::new();
    let reply = match tokio::time::timeout(READ_TIMEOUT, reader.read_until(b'\n', &mut line)).await
    {
        Ok(Ok(_)) if line.len() <= MAX_QUESTION_BYTES => match serde_json::from_slice(&line) {
            Ok(question) => answer(question).await,
            Err(_) => refusal("The question is not JSON."),
        },
        _ => refusal("The question did not arrive."),
    };
    let mut text = reply.to_string();
    text.push('\n');
    let _ = writer.write_all(text.as_bytes()).await;
    let _ = writer.shutdown().await;
}

#[cfg(unix)]
async fn bind_and_serve<F, Fut>(dir: &Path, answer: F) -> std::io::Result<String>
where
    F: Fn(Value) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Value> + Send + 'static,
{
    let path = socket_path(dir)?;
    let _ = std::fs::remove_file(&path);
    let listener = tokio::net::UnixListener::bind(&path)?;
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    let answer = Arc::new(answer);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            let answer = answer.clone();
            tokio::spawn(async move { serve_one(stream, |q| answer(q)).await });
        }
    });
    Ok(format!("unix:{}", path.display()))
}

#[cfg(not(unix))]
async fn bind_and_serve<F, Fut>(_dir: &Path, answer: F) -> std::io::Result<String>
where
    F: Fn(Value) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Value> + Send + 'static,
{
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0)).await?;
    let port = listener.local_addr()?.port();
    let answer = Arc::new(answer);
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                tokio::time::sleep(Duration::from_millis(50)).await;
                continue;
            };
            let answer = answer.clone();
            tokio::spawn(async move { serve_one(stream, |q| answer(q)).await });
        }
    });
    Ok(format!("tcp:127.0.0.1:{port}"))
}

/// This process's policy socket: `policy-<pid>.sock` in the webterm resources directory, or a
/// short path in the temporary directory when that one is too long for a socket. Sockets left by
/// processes that are gone are removed.
#[cfg(unix)]
fn socket_path(dir: &Path) -> std::io::Result<PathBuf> {
    let name = format!("policy-{}.sock", std::process::id());
    let preferred = dir.join(&name);
    let dir = if preferred.as_os_str().len() <= super::MAX_SOCKET_PATH {
        dir.to_path_buf()
    } else {
        use sha2::Digest;
        let digest = sha2::Sha256::digest(dir.to_string_lossy().as_bytes());
        let hash: String = digest.iter().take(8).map(|b| format!("{b:02x}")).collect();
        std::env::temp_dir().join(format!("mrsm-webterm-policy-{hash}"))
    };
    super::create_private_dir(&dir)?;
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            let file = entry.file_name().to_string_lossy().into_owned();
            if let Some(pid) = file
                .strip_prefix("policy-")
                .and_then(|rest| rest.strip_suffix(".sock"))
                .and_then(|pid| pid.parse::<u32>().ok())
                && super::owner_gone(pid)
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
    }
    Ok(dir.join(name))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_restricted_configuration_binds_nothing_that_runs_a_program() {
        // Every table is emptied before anything is bound, and no binding names a command that
        // starts a program, a pane, a prompt, a menu, or reaches another session.
        let lines: Vec<&str> = TMUX_RESTRICTED_CONF
            .lines()
            .filter(|l| !l.trim_start().starts_with('#'))
            .collect();
        for table in ["prefix", "root", "copy-mode", "copy-mode-vi"] {
            let unbind = format!("unbind-key -a -T {table}");
            let at = lines.iter().position(|l| *l == unbind).expect(&unbind);
            // Only the placeholder key that keeps the table in existence comes before.
            for line in lines[..at]
                .iter()
                .filter(|l| l.starts_with("bind-key") && l.contains(&format!("-T {table} ")))
            {
                assert!(
                    line.contains(" F12 "),
                    "{table} bound before it is emptied: {line}"
                );
            }
        }
        for forbidden in [
            "run-shell",
            "new-window",
            "split-window",
            "respawn",
            "command-prompt",
            "choose-",
            "switch-client",
            "attach-session",
            "display-menu",
            "display-popup",
            "pipe",
            "paste-buffer",
            "source-file",
            "set-hook",
            "#(",
        ] {
            assert!(
                !lines.iter().any(|l| l.contains(forbidden)),
                "{forbidden} in the restricted configuration"
            );
        }
        // `if-shell -F` tests a format; it never starts a shell.
        for line in lines.iter().filter(|l| l.contains("if-shell")) {
            assert!(line.contains("if-shell -F"), "{line}");
        }
        assert!(lines.contains(&"set -g prefix None"));
        assert!(lines.contains(&"set -g default-command \"exit 0\""));
        assert!(lines.contains(&"set -g copy-command \"\""));
    }

    #[test]
    fn listings_with_any_other_binding_are_refused() {
        let allowed = allowed_bindings();
        let has = |table: &str, key: &str| allowed.iter().any(|(t, k, _)| t == table && k == key);
        assert!(has("root", "WheelUpPane") && has("copy-mode", "NPage"));
        assert!(
            allowed
                .iter()
                .all(|(table, _, _)| KEPT_TABLES.contains(&table.as_str()))
        );
        // As tmux 3.8 lists them.
        let listing = "bind-key  -T copy-mode q                 send-keys -X cancel\n\
                       bind-key  -T root      MouseDown1Status  select-window -t =\n\
                       bind-key  -T root      WheelUpPane       if-shell -F \"#{||:#{pane_in_mode},#{mouse_any_flag}}\" \"send-keys -M\" \"copy-mode -e\"\n";
        assert_eq!(only_allowed_bindings(listing), Ok(()));
        for extra in [
            "bind-key -T prefix c new-window",
            "bind-key -r -T prefix Up select-pane -U",
            "bind-key -T move 0 resize-pane",
            "bind-key -T root F12 select-pane",
            "bind-key -T copy-mode q send-keys -X copy-pipe-and-cancel 'sh'",
            "garbage",
        ] {
            let listing = format!("{listing}{extra}\n");
            assert!(only_allowed_bindings(&listing).is_err(), "{extra}");
        }
    }

    #[test]
    fn restricted_sockets_sit_next_to_the_administrators() {
        assert_eq!(
            restricted_socket(Path::new("/r/webterm/tmux.sock")),
            PathBuf::from("/r/webterm/tmux-r.sock")
        );
        assert_eq!(
            restricted_socket(Path::new("/tmp/x/abcd.sock")),
            PathBuf::from("/tmp/x/abcd-r.sock")
        );
    }

    #[test]
    fn grants_name_their_user_and_end_with_their_session_or_connection() {
        let policy = Policy::default();
        let token = policy.grant_for_session("alice", "alice_s");
        assert_eq!(policy.username(&token).as_deref(), Some("alice"));
        assert_eq!(
            policy.session_grant("alice", "alice_s").as_deref(),
            Some(token.as_str())
        );
        // Another user's name never matches the session's grant.
        assert!(policy.session_grant("bob", "alice_s").is_none());
        // A new grant for the session replaces the old one.
        let again = policy.grant_for_session("alice", "alice_s");
        assert!(policy.username(&token).is_none());
        policy.forget_session("alice_s");
        assert!(policy.username(&again).is_none());
        let guard = policy.grant_for_connection("bob");
        let token = guard.token.clone();
        assert_eq!(policy.username(&token).as_deref(), Some("bob"));
        drop(guard);
        assert!(policy.username(&token).is_none());
        assert_ne!(new_token(), new_token());
        assert_eq!(new_token().len(), 64);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn the_policy_socket_answers_one_json_line_and_refuses_garbage() {
        let dir = tempfile::tempdir().unwrap();
        let address = bind_and_serve(dir.path(), |question: Value| async move {
            json!({"success": question["sysargs"][0] == "show", "message": "m"})
        })
        .await
        .unwrap();
        let path = address.strip_prefix("unix:").unwrap().to_string();
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&path).unwrap().permissions().mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let ask = |body: &'static [u8]| {
            let path = path.clone();
            async move {
                let mut stream = tokio::net::UnixStream::connect(&path).await.unwrap();
                stream.write_all(body).await.unwrap();
                let mut reply = String::new();
                stream.read_to_string(&mut reply).await.unwrap();
                serde_json::from_str::<Value>(reply.trim()).unwrap()
            }
        };
        assert_eq!(
            ask(b"{\"grant\":\"g\",\"sysargs\":[\"show\"]}\n").await["success"],
            true
        );
        assert_eq!(
            ask(b"{\"grant\":\"g\",\"sysargs\":[\"sh\"]}\n").await["success"],
            false
        );
        assert_eq!(ask(b"not json\n").await["success"], false);
    }
}
