//! Bounded terminal messages across an untrusted executor boundary.

use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;

pub const MAX_BYTES: usize = 1024 * 1024;
pub const QUEUE: usize = 16;

/// A backend-owned remote connection. Dropping input closes its transport task.
pub struct Terminal {
    pub input: mpsc::Sender<Input>,
    pub events: mpsc::Receiver<Event>,
}

enum Placement {
    Remote(uuid::Uuid, meerschaum_core::access::Team),
    Host(Option<uuid::Uuid>),
}

impl Placement {
    fn mode(&self) -> super::TermMode {
        match self {
            Self::Remote(id, _) => super::TermMode::Remote(*id),
            Self::Host(id) => super::TermMode::HostApi(*id),
        }
    }

    async fn revoke(&self, state: &crate::AppState, owner: &str, credential: &str) {
        match self {
            Self::Remote(id, _) => state.actions.revoke_executor_terminal(*id, owner).await,
            Self::Host(id) => {
                state
                    .actions
                    .revoke_host_api_terminal(*id, owner, credential)
                    .await
            }
        }
    }
}

/// Bridge the console session locally; its UUID/cookies never enter the remote transport.
pub(super) async fn bridge(
    inner: super::Inner,
    session_id: String,
    owner: String,
    team_id: uuid::Uuid,
    team: meerschaum_core::access::Team,
    socket: axum::extract::ws::WebSocket,
) {
    bridge_to(
        inner,
        session_id,
        owner,
        Placement::Remote(team_id, team),
        socket,
    )
    .await;
}

pub(super) async fn bridge_host(
    inner: super::Inner,
    session_id: String,
    owner: String,
    team_id: Option<uuid::Uuid>,
    socket: axum::extract::ws::WebSocket,
) {
    bridge_to(inner, session_id, owner, Placement::Host(team_id), socket).await;
}

async fn bridge_to(
    inner: super::Inner,
    session_id: String,
    owner: String,
    placement: Placement,
    socket: axum::extract::ws::WebSocket,
) {
    use axum::extract::ws::Message;
    use std::{sync::Arc, time::Duration};
    let binding = inner.config.sessions.binding(&session_id).await;
    let credential = match binding {
        Ok(Some((username, credential))) if username == owner => credential,
        _ => {
            super::refuse(
                socket,
                "Could not verify the terminal session's credential.",
                1008,
            )
            .await;
            return;
        }
    };
    let caller = crate::access::access_of_fresh(
        &inner.state,
        crate::auth::Principal::User(meerschaum_core::users::User::new(&owner, None)),
        &crate::auth::Scopes::List(vec!["*".into()]),
    )
    .await;
    let mut caller = match caller {
        Ok(caller) => caller,
        Err(_) => {
            super::refuse(socket, "Could not verify terminal permissions.", 1008).await;
            return;
        }
    };
    caller.credential = Some(credential);
    let opened = crate::executor::context::scope(Some(Arc::new(caller.clone())), async {
        match &placement {
            Placement::Remote(_, team) => {
                inner.state.actions.executor_terminal(team, &caller).await
            }
            Placement::Host(id) => inner.state.actions.host_api_terminal(*id, &caller).await,
        }
    })
    .await;
    let mut terminal = match opened {
        Ok(terminal) => terminal,
        Err(_) => {
            tracing::debug!(target: "mrsm::terminal_transport", layer = "main_bridge", reason = "runtime_open_refused", "The execution terminal could not open.");
            super::refuse(socket, "Your execution terminal is unavailable. Ask an administrator to check its deployment and native runtime.", 1011).await;
            return;
        }
    };
    // Provisioning may outlive the session that requested it. Check the console binding again
    // before accepting input or displaying output from the newly opened remote connection.
    let checked = tokio::time::timeout(
        super::REAUTHORIZE_TIMEOUT,
        super::authorize(&inner, &session_id, true),
    )
    .await
    .unwrap_or(super::Access::Unavailable);
    if checked.revocation(&owner, &placement.mode()).is_some() {
        tracing::debug!(target: "mrsm::terminal_transport", layer = "main_bridge", reason = "session_changed_during_open", "The execution terminal could not open.");
        drop(terminal);
        let _ = tokio::time::timeout(
            Duration::from_secs(10),
            placement.revoke(
                &inner.state,
                &owner,
                caller.credential.as_deref().unwrap_or_default(),
            ),
        )
        .await;
        super::refuse(
            socket,
            "Your terminal session changed while connecting. Log in again.",
            1008,
        )
        .await;
        return;
    }
    let (mut sink, mut source) = socket.split();
    let (outgoing, mut pending) = mpsc::channel(QUEUE);
    let mut writer = tokio::spawn(async move {
        while let Some(message) = pending.recv().await {
            let close = matches!(message, Message::Close(_));
            if !super::send_within(&mut sink, message).await {
                return "websocket_send_failed";
            }
            if close {
                return "websocket_close_sent";
            }
        }
        "websocket_output_queue_closed"
    });
    let every = inner
        .config
        .reauthorize_every
        .max(Duration::from_millis(10));
    let mut recheck = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let drain = inner.state.lifecycle.signal().wait();
    tokio::pin!(drain);
    let mut writer_done = false;
    let mut revoked = false;
    let mode = placement.mode();
    // Provisioning can buffer a legitimate browser burst before this loop starts. Keep one
    // bounded message in each direction while waiting for queue capacity, and keep draining
    // the other direction and rechecking the original session. Awaiting send inline would
    // deadlock when the backend's full event queue prevents it from reading more input.
    let mut pending_input = None;
    let mut input_deadline = None;
    let mut pending_output = None;
    let mut output_deadline = None;
    let mut native_disconnected = false;
    let reason = loop {
        tokio::select! {
            () = &mut drain => {
                let _ = outgoing.try_send(super::close_message(1001, super::SHUTTING_DOWN));
                break "main_shutdown";
            }
            result = &mut writer => { writer_done = true; break result.unwrap_or("websocket_writer_task_failed"); }
            () = capacity_deadline(input_deadline), if pending_input.is_some() => break "native_input_stalled",
            () = capacity_deadline(output_deadline), if pending_output.is_some() => break "websocket_output_stalled",
            capacity = terminal.input.reserve(), if pending_input.is_some() => {
                let Ok(capacity) = capacity else { break "native_input_queue_closed"; };
                capacity.send(pending_input.take().expect("pending input checked in the same task"));
                input_deadline = None;
            }
            capacity = outgoing.reserve(), if pending_output.is_some() => {
                let Ok(capacity) = capacity else { break "websocket_output_queue_closed"; };
                capacity.send(pending_output.take().expect("pending output checked in the same task"));
                output_deadline = None;
                if native_disconnected { break "native_disconnected"; }
            }
            event = terminal.events.recv(), if pending_output.is_none() => {
                let Some(event) = event else { break "native_event_stream_eof"; };
                native_disconnected = event == Event::Disconnected;
                pending_output = Some(Message::Text(event.wire().into()));
                output_deadline = Some(tokio::time::Instant::now() + FLOW_TIMEOUT);
            }
            message = source.next(), if pending_input.is_none() && pending_output.is_none() => match message {
                Some(Ok(Message::Text(raw))) => {
                    pending_input = match super::parse_client_message(&raw) {
                        Some(super::ClientMessage::Stdin(text)) => Some(Input::Stdin(text)),
                        Some(super::ClientMessage::SetSize(s)) => Some(Input::Resize { rows: s.rows, cols: s.cols, height: s.pixel_height, width: s.pixel_width }),
                        Some(super::ClientMessage::NewWindow) => Some(Input::NewWindow),
                        Some(super::ClientMessage::ShellState(id)) => Some(Input::ShellState(id)),
                        None => None,
                    };
                    if pending_input.is_some() { input_deadline = Some(tokio::time::Instant::now() + FLOW_TIMEOUT); }
                }
                Some(Ok(Message::Ping(bytes))) => {
                    pending_output = Some(Message::Pong(bytes));
                    output_deadline = Some(tokio::time::Instant::now() + FLOW_TIMEOUT);
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) => break "browser_close_received",
                Some(Err(_)) => break "browser_transport_error",
                None => break "browser_transport_eof",
                _ => break "browser_input_refused",
            },
            _ = recheck.tick() => {
                let access = tokio::time::timeout(super::REAUTHORIZE_TIMEOUT, super::authorize(&inner, &session_id, true)).await
                    .unwrap_or(super::Access::Unavailable);
                if let Some(reason) = access.revocation(&owner, &mode) {
                    revoked = true;
                    super::queue(&outgoing, [super::reason_message(&reason), super::close_message(1008, "Terminal access ended.")]);
                    break "session_reauthorization_refused";
                }
            }
        }
    };
    tracing::debug!(target: "mrsm::terminal_transport", layer = "main_bridge", reason, "The execution terminal bridge ended.");
    drop(terminal); // Stops this websocket client; a valid detached lease keeps tmux alive.
    if revoked {
        let _ = tokio::time::timeout(
            Duration::from_secs(10),
            placement.revoke(
                &inner.state,
                &owner,
                caller.credential.as_deref().unwrap_or_default(),
            ),
        )
        .await;
    }
    drop(outgoing);
    if !writer_done
        && tokio::time::timeout(super::CLOSE_TIMEOUT, &mut writer)
            .await
            .is_err()
    {
        writer.abort();
    }
}

// Match the private Host transport's bounded sends; this changes neither queue size nor
// message limits, and a stalled direction does not suspend session revocation or shutdown.
const FLOW_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
async fn capacity_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

/// Only these commands may cross from a browser to the executor.
#[derive(Debug)]
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

fn valid_id(id: &str) -> bool {
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

#[derive(Default)]
struct Controls {
    escape: Escape,
}

impl Controls {
    fn filter(&mut self, text: &str) -> String {
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
