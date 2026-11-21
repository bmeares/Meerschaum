//! Bounded terminal messages across an untrusted executor boundary and `RemoteExecutorAdapter`.

use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use super::session::{OpenError, RevokeAction, TerminalSession};
use super::{Inner, TermMode};

pub const MAX_BYTES: usize = 1024 * 1024;
pub const QUEUE: usize = 16;

/// A backend-owned remote connection. Dropping `input` closes its transport task.
pub struct Terminal {
    pub input: mpsc::Sender<Input>,
    pub events: mpsc::Receiver<Event>,
}

enum Placement {
    Remote(uuid::Uuid, meerschaum_core::access::Team),
    Host(Option<uuid::Uuid>),
}

impl Placement {
    fn mode(&self) -> TermMode {
        match self {
            Self::Remote(id, _) => TermMode::Remote(*id),
            Self::Host(id) => TermMode::HostApi(*id),
        }
    }

    fn revoke_action(
        &self,
        state: &crate::AppState,
        owner: &str,
        credential: &str,
    ) -> RevokeAction {
        match self {
            Self::Remote(id, _) => RevokeAction::RemoteExecutor {
                state: state.clone(),
                team_id: *id,
                owner: owner.to_string(),
            },
            Self::Host(id) => RevokeAction::HostApi {
                state: state.clone(),
                team_id: *id,
                owner: owner.to_string(),
                credential: credential.to_string(),
            },
        }
    }
}

/// Connects Main's console session to either a remote team executor terminal or a host API
/// terminal and adapts it into the unified [`TerminalSession`] seam driven by [`super::bridge`].
pub(super) struct RemoteExecutorAdapter;

impl RemoteExecutorAdapter {
    pub(super) async fn open_remote(
        inner: &Inner,
        session_id: &str,
        owner: &str,
        team_id: uuid::Uuid,
        team: Option<Box<meerschaum_core::access::Team>>,
    ) -> Result<TerminalSession, OpenError> {
        let Some(team) = team else {
            return Err(OpenError::refused(
                "The terminal's executor placement is unavailable.",
                1011,
            ));
        };
        Self::open_placement(inner, session_id, owner, Placement::Remote(team_id, *team)).await
    }

    pub(super) async fn open_host_api(
        inner: &Inner,
        session_id: &str,
        owner: &str,
        team_id: Option<uuid::Uuid>,
    ) -> Result<TerminalSession, OpenError> {
        Self::open_placement(inner, session_id, owner, Placement::Host(team_id)).await
    }

    /// Resolve the session credential binding, provision the remote/host backend under caller
    /// scope, re-verify authorization after provisioning, and return a [`TerminalSession`].
    /// Console session UUIDs and cookies never cross into the remote transport.
    async fn open_placement(
        inner: &Inner,
        session_id: &str,
        owner: &str,
        placement: Placement,
    ) -> Result<TerminalSession, OpenError> {
        let binding = inner.config.sessions.binding(session_id).await;
        let credential = match binding {
            Ok(Some((username, credential))) if username == owner => credential,
            _ => {
                return Err(OpenError::refused(
                    "Could not verify the terminal session's credential.",
                    1008,
                ));
            }
        };
        let caller = crate::access::access_of_fresh(
            &inner.state,
            crate::auth::Principal::User(meerschaum_core::users::User::new(owner, None)),
            &crate::auth::Scopes::List(vec!["*".into()]),
        )
        .await;
        let mut caller = match caller {
            Ok(caller) => caller,
            Err(_) => {
                return Err(OpenError::refused(
                    "Could not verify terminal permissions.",
                    1008,
                ));
            }
        };
        caller.credential = Some(credential.clone());
        let opened = crate::executor::context::scope(Some(Arc::new(caller.clone())), async {
            match &placement {
                Placement::Remote(_, team) => {
                    inner.state.actions.executor_terminal(team, &caller).await
                }
                Placement::Host(id) => inner.state.actions.host_api_terminal(*id, &caller).await,
            }
        })
        .await;
        let terminal = match opened {
            Ok(terminal) => terminal,
            Err(_) => {
                tracing::debug!(
                    target: "mrsm::terminal_transport",
                    layer = "main_bridge",
                    reason = "runtime_open_refused",
                    "The execution terminal could not open."
                );
                return Err(OpenError::refused(
                    "Your execution terminal is unavailable. Ask an administrator to check its deployment and native runtime.",
                    1011,
                ));
            }
        };
        let on_revoke = placement.revoke_action(&inner.state, owner, &credential);
        // Provisioning may outlive the session that requested it. Check the console binding again
        // before accepting input or displaying output from the newly opened connection.
        let checked = tokio::time::timeout(
            super::REAUTHORIZE_TIMEOUT,
            super::authorize(inner, session_id, true),
        )
        .await
        .unwrap_or(super::Access::Unavailable);
        if let Some(reason) = checked.revocation(owner, &placement.mode()) {
            tracing::debug!(
                target: "mrsm::terminal_transport",
                layer = "main_bridge",
                reason = "session_changed_during_open",
                "The execution terminal could not open."
            );
            drop(terminal);
            on_revoke.run(&reason).await;
            return Err(OpenError::refused(
                "Your terminal session changed while connecting. Log in again.",
                1008,
            ));
        }
        Ok(TerminalSession::new(terminal, on_revoke))
    }
}

/// Only these commands may cross from a browser to the executor.
#[derive(Debug, PartialEq, Eq)]
pub enum Input {
    Stdin(String),
    Resize {
        rows: u16,
        cols: u16,
        height: u16,
        width: u16,
    },
    NewWindow,
    ShellState(String),
}

impl Input {
    pub fn wire(&self) -> Result<String, String> {
        let value = match self {
            Self::Stdin(text) => json!(["stdin", text]),
            Self::Resize {
                rows,
                cols,
                height,
                width,
            } if *rows != 0 && *cols != 0 => {
                json!(["set_size", rows, cols, height, width])
            }
            Self::NewWindow => json!(["new_window"]),
            Self::ShellState(id) if valid_id(id) => json!(["shell_state", id]),
            _ => return Err("Invalid terminal command.".into()),
        };
        let wire = value.to_string();
        if wire.len() > MAX_BYTES {
            return Err("Terminal input exceeds its limit.".into());
        }
        Ok(wire)
    }
}

/// Deliberately excludes executor-chosen URLs, metadata, close codes and opaque JSON objects.
#[derive(Debug, PartialEq, Eq)]
pub enum Event {
    Setup,
    Output(String),
    ShellState {
        id: String,
        state: Option<ShellState>,
    },
    Disconnected,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellState {
    pub idle: bool,
    pub empty: bool,
}

impl Event {
    pub fn wire(&self) -> String {
        match self {
            Self::Setup => json!(["setup", {}]),
            Self::Output(text) => json!(["stdout", text]),
            Self::ShellState { id, state } => json!(["shell_state", id, state]),
            Self::Disconnected => json!(["disconnect", 1]),
        }
        .to_string()
    }
}

pub(super) fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 128 && !id.chars().any(char::is_control)
}

/// Stateful decoding strips terminal string controls even when split across output frames.
#[derive(Default)]
pub struct Decoder {
    controls: Controls,
}

impl Decoder {
    pub fn decode(&mut self, raw: &str) -> Result<Event, String> {
        if raw.len() > MAX_BYTES {
            return Err("Terminal output exceeds its limit.".into());
        }
        let value: Value =
            serde_json::from_str(raw).map_err(|_| "Invalid executor terminal message.")?;
        let items = value
            .as_array()
            .ok_or("Invalid executor terminal message.")?;
        match items.first().and_then(Value::as_str) {
            Some("setup")
                if items.len() == 2 && items[1].as_object().is_some_and(|m| m.is_empty()) =>
            {
                Ok(Event::Setup)
            }
            Some("stdout") if items.len() == 2 => {
                let output = items[1]
                    .as_str()
                    .ok_or("Invalid executor terminal output.")?;
                Ok(Event::Output(self.controls.filter(output)))
            }
            Some("shell_state") if items.len() == 3 => {
                let id = items[1]
                    .as_str()
                    .filter(|id| valid_id(id))
                    .ok_or("Invalid shell probe identity.")?
                    .into();
                let state = serde_json::from_value(items[2].clone())
                    .map_err(|_| "Invalid executor shell state.")?;
                Ok(Event::ShellState { id, state })
            }
            Some("disconnect") if items.len() == 2 && items[1] == 1 => Ok(Event::Disconnected),
            _ => Err("Unsupported executor terminal message.".into()),
        }
    }
}

#[derive(Default)]
enum Escape {
    #[default]
    Text,
    Start,
    Charset(char),
    Csi(String),
    String,
    StringEnd,
}

/// Stateful ANSI/control-sequence filter that strips OSC/DCS/APC/PM/SOS string sequences even
/// when split across output frames while keeping cursor movement, styles, and charset selection.
#[derive(Default)]
pub(super) struct Controls {
    escape: Escape,
}

impl Controls {
    pub(super) fn filter(&mut self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        for c in text.chars() {
            self.escape = match std::mem::take(&mut self.escape) {
                Escape::Text => match c {
                    '\x1b' => Escape::Start,
                    '\u{90}' | '\u{9d}' | '\u{9e}' | '\u{9f}' => Escape::String,
                    '\u{9b}' => Escape::Csi(String::new()),
                    '\u{80}'..='\u{9f}' => Escape::Text,
                    _ => {
                        if !c.is_control() || matches!(c, '\n' | '\r' | '\t' | '\x08' | '\x07') {
                            out.push(c);
                        }
                        Escape::Text
                    }
                },
                Escape::Start => match c {
                    ']' | 'P' | '^' | '_' | 'X' => Escape::String,
                    '[' => Escape::Csi(String::new()),
                    '(' | ')' | '*' | '+' => Escape::Charset(c),
                    // Cursor save/restore, index/reverse index, next line, reset and keypad.
                    '7' | '8' | 'D' | 'E' | 'M' | 'c' | '=' | '>' => {
                        out.push('\x1b');
                        out.push(c);
                        Escape::Text
                    }
                    '\x1b' => Escape::Start,
                    _ => Escape::Text,
                },
                Escape::Csi(mut sequence) => {
                    if ('\x40'..='\x7e').contains(&c) {
                        out.push_str("\x1b[");
                        out.push_str(&sequence);
                        out.push(c);
                        Escape::Text
                    } else if ('\x20'..='\x3f').contains(&c) && sequence.len() < 128 {
                        sequence.push(c);
                        Escape::Csi(sequence)
                    } else {
                        Escape::Text
                    }
                }
                Escape::Charset(selector) => {
                    if matches!(c, '0' | '1' | '2' | 'A' | 'B') {
                        out.push('\x1b');
                        out.push(selector);
                        out.push(c);
                    }
                    Escape::Text
                }
                Escape::String => match c {
                    '\x07' | '\u{9c}' => Escape::Text,
                    '\x1b' => Escape::StringEnd,
                    _ => Escape::String,
                },
                Escape::StringEnd => match c {
                    '\\' | '\x07' | '\u{9c}' => Escape::Text,
                    '\x1b' => Escape::StringEnd,
                    _ => Escape::String,
                },
            };
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executor_messages_are_projected_to_known_typed_data() {
        let mut decoder = Decoder::default();
        assert_eq!(decoder.decode(r#"["setup",{}]"#).unwrap(), Event::Setup);
        assert_eq!(
            decoder
                .decode(r#"["shell_state","request",{"idle":true,"empty":false}]"#)
                .unwrap(),
            Event::ShellState {
                id: "request".into(),
                state: Some(ShellState {
                    idle: true,
                    empty: false
                })
            }
        );
        assert!(decoder.decode(r#"["shell_state","request",{"idle":true,"empty":false,"url":"https://invalid.example"}]"#).is_err());
        assert!(
            decoder
                .decode(r#"["setup",{"url":"https://invalid.example"}]"#)
                .is_err()
        );
        assert!(decoder.decode(r#"["execute","anything"]"#).is_err());
        assert!(decoder.decode(r#"["disconnect",1000]"#).is_err());
        assert!(decoder.decode(&"x".repeat(MAX_BYTES + 1)).is_err());
        assert!(Input::ShellState("\n".into()).wire().is_err());
    }

    #[test]
    fn terminal_controls_preserve_cursor_and_style_but_remove_cross_frame_strings() {
        let mut filter = Controls::default();
        assert_eq!(
            filter.filter("a\x1b[2J\x1b[31mred\x1b[0m\r\nb\x1b]52;c;"),
            "a\x1b[2J\x1b[31mred\x1b[0m\r\nb"
        );
        assert_eq!(filter.filter("clipboard\x1b"), "");
        assert_eq!(
            filter.filter("\\safe\x1b]8;;https://invalid.example\x07link\x1b]8;;\x1b\\"),
            "safelink"
        );
        assert_eq!(
            filter.filter("\x1bPopaque\x1b\\ok\u{9d}52;c;hidden\u{9c}done"),
            "okdone"
        );
        assert_eq!(filter.filter("\x1b["), "");
        assert_eq!(filter.filter("4;2Hcursor"), "\x1b[4;2Hcursor");
        assert_eq!(filter.filter("\x1b(0lines\x1b(B"), "\x1b(0lines\x1b(B");
    }
}
