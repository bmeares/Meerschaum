//! HTTP routes, refusal pages, and session authorization for the web terminal.

use super::*;

/// Which router mounted the terminal handler: console routes vs private executor websocket.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum RouteKind {
    /// `/webterm/{session_id}` and `/websocket/{session_id}` on the console router.
    Console,
    /// Private executor `/websocket/{session_id}` requiring signed gateway identity and leased
    /// sessions (never interprets console session UUIDs or permits `--no-auth`).
    Executor,
}

#[derive(Clone)]
pub(super) struct Inner {
    pub(super) state: AppState,
    pub(super) config: WebtermConfig,
    pub(super) kind: RouteKind,
}

impl Inner {
    pub(super) fn is_executor(&self) -> bool {
        self.kind == RouteKind::Executor
    }
}

/// The `/webterm/{session_id}` and `/websocket/{session_id}` routes. Merge them only when the
/// terminal is served (`!no_webterm`, the console is on); without them both paths answer `404`.
pub fn routes(state: AppState, config: WebtermConfig) -> Router {
    Router::new()
        .route("/webterm/{session_id}", get(page))
        .route("/webterm/{session_id}/", get(page))
        .route("/websocket/{session_id}", get(socket))
        .route("/websocket/{session_id}/", get(socket))
        .with_state(Inner {
            state,
            config,
            kind: RouteKind::Console,
        })
}

/// Private executor websocket only. Requires signed gateway identity and leased sessions;
/// it never interprets a main-console session UUID or permits the ordinary no-auth bypass.
pub fn executor_routes(state: AppState, config: WebtermConfig) -> Router {
    Router::new()
        .route("/websocket/{session_id}", get(socket))
        .with_state(Inner {
            state,
            config,
            kind: RouteKind::Executor,
        })
}

/// How a terminal's shell runs (5.1, [`crate::execute_policy::may_run_system_commands`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TermMode {
    /// The full `mrsm` shell on the server (administrators).
    Full,
    /// The restricted shell: every line decided by the server ([`restricted`]).
    Restricted,
    /// A team terminal: an `mrsm` shell that is itself a process of the team (its configuration,
    /// the environment allowlist and the sandbox applied before the shell starts), on a tmux
    /// server of the team's own that runs nothing else ([`team::team_tmux`]).
    Team(String),
    /// A main-owned terminal transported to one immutable team executor identity.
    Remote(uuid::Uuid),
    /// A bounded API-only shell on Main, bound to its registered primary team or Everyone.
    HostApi(Option<uuid::Uuid>),
}

/// The user a session may open a terminal as, or why not.
pub(super) enum Access {
    /// The terminal's name (the user's), how its shell runs, and the team record of a team
    /// terminal.
    Granted {
        name: String,
        mode: TermMode,
        team: Option<Box<meerschaum_core::access::Team>>,
    },
    /// No session (log in).
    Denied,
    /// A session whose user may not use the terminal (`webterm:use`).
    Forbidden,
    Unavailable,
    /// A member of an isolated team whose team terminal is not available here, and why.
    NoTeamTerminal(String),
}

impl Access {
    /// What a refused websocket shows before it closes (v4 closed it without a word, leaving a
    /// blank terminal).
    pub(super) fn refusal(&self) -> Option<String> {
        match self {
            Access::Granted { .. } => None,
            Access::Denied => Some(
                "Not logged in, or the session has ended. Log in again to use the terminal.".into(),
            ),
            Access::Forbidden => Some("You do not have permission to use the terminal.".into()),
            Access::Unavailable => Some("Could not check your session. Try again later.".into()),
            Access::NoTeamTerminal(reason) => Some(team_terminal_refusal(reason)),
        }
    }

    /// Why an open terminal of `term_name` (started in `mode`) closes after authorizing its
    /// session again; `None` keeps it open.
    pub(super) fn revocation(&self, term_name: &str, mode: &TermMode) -> Option<String> {
        match self {
            Access::Granted {
                name, mode: now, ..
            } if name == term_name => (now != mode).then(|| ROLE_CHANGED.to_string()),
            Access::Granted { .. } | Access::Denied => {
                Some("Your session has ended. Log in again to use the terminal.".into())
            }
            Access::Forbidden => Some("Your permission to use the terminal was removed.".into()),
            Access::Unavailable => {
                Some("Could not verify your session, so the terminal was closed.".into())
            }
            Access::NoTeamTerminal(reason) => Some(team_terminal_refusal(reason)),
        }
    }
}

/// What a member of an isolated team hears when their team terminal is not available.
pub(super) fn team_terminal_refusal(reason: &str) -> String {
    format!("Your team's terminal is not available on this server: {reason}.")
}

/// Who `session_id` may open a terminal as. `fresh` reads the user's identity now instead of
/// from the access cache (a websocket, and every check of an open one).
pub(super) async fn authorize(inner: &Inner, session_id: &str, fresh: bool) -> Access {
    match inner.kind {
        RouteKind::Executor => authorize_executor(&*inner.config.sessions, session_id).await,
        RouteKind::Console => authorize_console(inner, session_id, fresh).await,
    }
}

async fn authorize_executor(sessions: &dyn SessionLookup, session_id: &str) -> Access {
    let Some(caller) = crate::executor::gateway::current() else {
        return Access::Denied;
    };
    let name = match sessions.username(session_id).await {
        Ok(Some(name)) => name,
        Ok(None) => return Access::Denied,
        Err(_) => return Access::Unavailable,
    };
    match sessions.restricted(session_id).await {
        Ok(restricted) => Access::Granted {
            name,
            mode: if restricted {
                TermMode::Restricted
            } else {
                TermMode::Team(caller.claims.team_id.to_string())
            },
            team: None,
        },
        Err(_) => Access::Unavailable,
    }
}

async fn authorize_console(inner: &Inner, session_id: &str, fresh: bool) -> Access {
    // PARITY: `is_session_authenticated` is true for everyone under `--no-auth`; the terminal is
    // named after the session then (`username or session_id`).
    if inner.state.config.no_auth {
        return Access::Granted {
            name: session_id.to_string(),
            mode: TermMode::Full,
            team: None,
        };
    }
    let username = match inner.config.sessions.username(session_id).await {
        Ok(Some(username)) if !username.is_empty() => username,
        Ok(_) => return Access::Denied,
        Err(error) => {
            tracing::warn!("The session store failed for a terminal: {error}");
            return Access::Unavailable;
        }
    };
    authorize_user(&inner.state, &username, fresh).await
}

/// Who `username` may open a terminal as (the part of [`authorize`] after the session).
pub(super) async fn authorize_user(state: &AppState, username: &str, fresh: bool) -> Access {
    let username = username.to_string();
    // FIX (intentional v4 change, rc15 access model): v4 gave every logged-in user a shell
    // unless `--secure`; the terminal now needs `webterm:use` (administrators, the role
    // `operator`, or the permission given), and `--secure` keeps it for administrators. v4 also
    // asked only the session store: the session's user must still be a user of the instance.
    let user = meerschaum_core::users::User::new(&username, None);
    match state.instance().get_user_id(&user).await {
        Ok(Some(id)) if !id.is_null() => {}
        Ok(_) => return Access::Denied,
        Err(error) => {
            tracing::warn!("Could not read the user of a terminal session: {error}");
            return Access::Unavailable;
        }
    }
    let principal = crate::auth::Principal::User(user);
    let granted = crate::auth::Scopes::List(vec!["*".into()]);
    let access = if fresh {
        crate::access::access_of_fresh(state, principal, &granted).await
    } else {
        crate::access::access_of(state, principal, &granted).await
    };
    let access = match access {
        Ok(access) => access,
        Err(e) if e.status.as_u16() == 503 => return Access::Unavailable,
        Err(_) => return Access::Denied,
    };
    if !access.is_admin() && (!state.config.actions_non_admin || !access.has("webterm:use")) {
        return Access::Forbidden;
    }
    let selected_remote = (!access.full_admin())
        .then(|| {
            access
                .team_records
                .iter()
                .find(|team| {
                    meerschaum_core::access::same_team(Some(&team.name), Some(&access.primary_team))
                        && access.in_team(Some(&team.name))
                        && access.principal.permits_team(Some(&team.name))
                })
                .cloned()
        })
        .flatten()
        .filter(|team| {
            team.execution()
                .is_ok_and(|settings| settings.mode.is_remote())
        });
    if let Some(team) =
        selected_remote.or_else(|| crate::team_execution::code_running_team(&access))
        && team.execution().is_ok_and(|s| s.mode.is_remote())
    {
        if !["webterm:use", "actions:execute", "jobs:execute"]
            .iter()
            .all(|p| access.has(p))
        {
            return Access::Forbidden;
        }
        let Some(id) = team
            .id
            .as_deref()
            .and_then(|id| uuid::Uuid::parse_str(id).ok())
        else {
            return Access::NoTeamTerminal("the selected team has no registered identity".into());
        };
        return Access::Granted {
            name: username,
            mode: TermMode::Remote(id),
            team: Some(Box::new(team)),
        };
    }
    // 5.1: the one answer to "may this user run system commands" decides the shell's mode. A
    // member of an isolated team gets their team's terminal or none: never a shell in this
    // server's process.
    use crate::execute_policy::SystemCommands;
    match crate::execute_policy::may_run_system_commands(state, &access).await {
        SystemCommands::Anywhere => Access::Granted {
            name: username,
            mode: TermMode::Full,
            team: None,
        },
        SystemCommands::TeamOnly { team } => match access.acting_team(None) {
            Some(record) if meerschaum_core::access::same_team(Some(&record.name), Some(&team)) => {
                Access::Granted {
                    name: username,
                    mode: TermMode::Team(team),
                    team: Some(Box::new(record)),
                }
            }
            _ => Access::NoTeamTerminal("the team you act for changed".into()),
        },
        SystemCommands::No { .. } => {
            if !["webterm:use", "actions:execute", "jobs:execute"]
                .iter()
                .all(|permission| access.has(permission))
            {
                return Access::Forbidden;
            }
            let team_id = if meerschaum_core::access::is_everyone(Some(&access.primary_team)) {
                None
            } else {
                let Some(id) = access
                    .team_records
                    .iter()
                    .find(|team| {
                        meerschaum_core::access::same_team(
                            Some(&team.name),
                            Some(&access.primary_team),
                        )
                    })
                    .and_then(|team| team.id.as_deref())
                    .and_then(|value| uuid::Uuid::parse_str(value).ok())
                    .filter(|id| !id.is_nil())
                else {
                    return Access::NoTeamTerminal(
                        "the primary team has no current registered identity".into(),
                    );
                };
                Some(id)
            };
            Access::Granted {
                name: username,
                mode: TermMode::HostApi(team_id),
                team: None,
            }
        }
    }
}

/// Why an open terminal closes when its user's role changed (they may now run system commands,
/// or no longer may).
pub(super) const ROLE_CHANGED: &str = "Your role changed. Open the terminal again.";

pub(super) const UNAUTHORIZED_PAGE: &str = r#"<html>
    <head>
        <style>
        body { background-color: #222222; }
        #message-div { padding: 15px; font-size: x-large; font-family: sans-serif; }
        h2 { color: #FFFFFF; }
        p { color: #FFFFFF; }
        a { color: #FFFFFF; text-decoration: underline; }
        </style>
    </head>
    <body>
        <div id="message-div">
            <h2>Unauthorized</h2>
            <p>Please <a href="/dash/login">log in</a> and try again.</p>
        </div>
    </body>
</html>
"#;

/// The page of a signed-in user without the permission `webterm:use`.
pub(super) const FORBIDDEN_PAGE: &str = r#"<html>
    <head>
        <style>
        body { background-color: #222222; }
        #message-div { padding: 15px; font-size: x-large; font-family: sans-serif; }
        h2 { color: #FFFFFF; }
        p { color: #FFFFFF; }
        </style>
    </head>
    <body>
        <div id="message-div">
            <h2>No terminal access</h2>
            <p>Ask an administrator for the permission to use the terminal.</p>
        </div>
    </body>
</html>
"#;

/// The page of a member of an isolated team whose team terminal is not available.
pub(super) fn no_team_terminal_page(reason: &str) -> String {
    let escaped = reason
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;");
    FORBIDDEN_PAGE
        .replace("No terminal access", "No terminal for your team")
        .replace(
            "Ask an administrator for the permission to use the terminal.",
            &format!("Your team's terminal is not available on this server: {escaped}."),
        )
}

pub(super) async fn page(
    State(inner): State<Inner>,
    UrlPath(session_id): UrlPath<String>,
) -> Response {
    let mut response = match authorize(&inner, &session_id, false).await {
        Access::Granted { .. } => Html((inner.config.page)(&session_id)).into_response(),
        Access::Denied => (StatusCode::UNAUTHORIZED, Html(UNAUTHORIZED_PAGE)).into_response(),
        Access::Forbidden => (StatusCode::FORBIDDEN, Html(FORBIDDEN_PAGE)).into_response(),
        Access::NoTeamTerminal(reason) => {
            (StatusCode::FORBIDDEN, Html(no_team_terminal_page(&reason))).into_response()
        }
        Access::Unavailable => (
            StatusCode::SERVICE_UNAVAILABLE,
            "The session store is unavailable.",
        )
            .into_response(),
    };
    let headers = response.headers_mut();
    headers.insert(header::CACHE_CONTROL, "no-store".parse().expect("header"));
    headers.insert("x-content-type-options", "nosniff".parse().expect("header"));
    // The session id is in this URL: never leak it as a referrer.
    headers.insert("referrer-policy", "no-referrer".parse().expect("header"));
    response
}

/// Whether a websocket's `Origin` may open a terminal: absent (not a browser), or the same
/// authority as the page that was asked for (`Host`, or `X-Forwarded-Host` behind a proxy).
/// Browsers always send `Origin` on a websocket handshake, and the session id travels in the URL,
/// not a cookie, so this is what stops another site from driving a terminal (cross-site
/// websocket hijacking), notably under `--no-auth`, where every session id is accepted.
pub fn origin_allowed(headers: &axum::http::HeaderMap) -> bool {
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Some(authority) = origin
        .to_str()
        .ok()
        .and_then(|o| o.split_once("://"))
        .map(|(_, rest)| rest.split('/').next().unwrap_or(""))
    else {
        return false;
    };
    let hosts = [header::HOST.as_str(), "x-forwarded-host"]
        .into_iter()
        .filter_map(|name| headers.get(name)?.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(str::trim);
    hosts
        .into_iter()
        .any(|host| !host.is_empty() && host.eq_ignore_ascii_case(authority))
}
