//! Single WebSocket bridge, terminado wire protocol, and active terminal accounting for all 5
//! terminal modes (`Full`, `Restricted`, `Team`, `Remote`, `HostApi`).

use super::*;

/// How long a full backend input queue may stall a buffered client command before closing.
const FLOW_TIMEOUT: Duration = Duration::from_secs(5);

/// Periodic WebSocket keepalive interval.
const PING_EVERY: Duration = Duration::from_secs(30);

pub(super) async fn socket(
    State(inner): State<Inner>,
    UrlPath(session_id): UrlPath<String>,
    verified: Option<axum::Extension<crate::executor::gateway::VerifiedRequest>>,
    headers: axum::http::HeaderMap,
    ws: WebSocketUpgrade,
) -> Response {
    if !origin_allowed(&headers) {
        return (StatusCode::FORBIDDEN, "Cross-origin websocket refused.").into_response();
    }
    ws.max_message_size(MAX_MESSAGE_BYTES)
        .on_upgrade(move |socket| async move {
            let verified = inner
                .is_executor()
                .then(|| verified.map(|axum::Extension(caller)| caller))
                .flatten();
            crate::executor::gateway::scope(verified, async move {
                // PARITY: v4 accepts the websocket, then closes it when the session is not authorized
                // (v5 says why first).
                match authorize(&inner, &session_id, true).await {
                    Access::Granted { name, mode, team } => {
                        run(inner, session_id, name, mode, team, socket).await
                    }
                    refused => {
                        let reason = refused.refusal().unwrap_or_default();
                        refuse(socket, &reason, 1000).await
                    }
                }
            })
            .await;
        })
}

pub(super) fn close_message(code: u16, reason: &str) -> Message {
    // RFC 6455 control frames hold at most 125 bytes (2-byte status code + 123-byte UTF-8 reason).
    let truncated = if reason.len() <= 123 {
        reason
    } else {
        let mut end = 123;
        while !reason.is_char_boundary(end) {
            end -= 1;
        }
        &reason[..end]
    };
    Message::Close(Some(CloseFrame {
        code,
        reason: truncated.into(),
    }))
}

/// Send one message, giving up after `timeout`; `false` when it was not sent.
pub(super) async fn send_within<S, E>(sink: &mut S, message: Message, timeout: Duration) -> bool
where
    S: futures_util::Sink<Message, Error = E> + Unpin,
{
    matches!(
        tokio::time::timeout(timeout, sink.send(message)).await,
        Ok(Ok(()))
    )
}

/// The terminal's line for `reason` (the `[CLOSED]` that follows is the browser's).
pub(super) fn reason_message(reason: &str) -> Message {
    text(json!(["stdout", format!("\r\n[{reason}]\r\n")]))
}

/// Tell the browser why, end the terminal (`["disconnect", 1]`, so it does not reconnect) and
/// close the socket with `code`, each write bounded.
pub(super) async fn refuse(mut socket: WebSocket, reason: &str, code: u16) {
    if send_within(&mut socket, reason_message(reason), WRITE_TIMEOUT).await
        && send_within(&mut socket, text(json!(["disconnect", 1])), WRITE_TIMEOUT).await
    {
        let _ = send_within(&mut socket, close_message(code, ""), WRITE_TIMEOUT).await;
    }
}

async fn refuse_open_error(mut socket: WebSocket, error: OpenError) {
    let first = if error.bracketed {
        reason_message(&error.message)
    } else {
        text(json!(["stdout", error.message]))
    };
    if send_within(&mut socket, first, WRITE_TIMEOUT).await
        && send_within(&mut socket, text(json!(["disconnect", 1])), WRITE_TIMEOUT).await
    {
        let _ = send_within(&mut socket, close_message(error.code, ""), WRITE_TIMEOUT).await;
    }
}

/// Queue messages for the socket's writer without waiting: a full queue (a browser that stopped
/// reading) drops them.
pub(super) fn queue(control: &mpsc::Sender<Message>, messages: impl IntoIterator<Item = Message>) {
    for message in messages {
        let _ = control.try_send(message);
    }
}

/// How a terminal's socket writer ended.
pub(super) enum WriterEnd {
    /// The backend sent `Event::Disconnected`: `["disconnect", 1]` and a close frame were sent.
    ShellExited,
    /// It sent the bridge's close frame, or the backend event stream closed.
    Closed,
    /// A write failed or did not finish within the write timeout (the browser is gone or stopped
    /// reading).
    PeerGone,
}

/// The only task that writes to a terminal's socket: the bridge's control frames (`control`,
/// first) and the session's events (`events`), each write bounded by `write_timeout`. A browser
/// that stops reading stalls only this task; the bridge keeps reading input and reauthorizing.
async fn write_terminal<S, E>(
    mut tx: futures_util::stream::SplitSink<S, Message>,
    mut events: mpsc::Receiver<remote::Event>,
    mut control: mpsc::Receiver<Message>,
    write_timeout: Duration,
) -> WriterEnd
where
    S: futures_util::Sink<Message, Error = E> + Unpin,
{
    loop {
        tokio::select! {
            biased;
            message = control.recv() => {
                let Some(message) = message else {
                    return WriterEnd::Closed;
                };
                let close = matches!(message, Message::Close(_));
                if !send_within(&mut tx, message, write_timeout).await {
                    return WriterEnd::PeerGone;
                }
                if close {
                    return WriterEnd::Closed;
                }
            }
            event = events.recv() => match event {
                Some(remote::Event::Disconnected) => {
                    if send_within(&mut tx, text(json!(["disconnect", 1])), write_timeout).await {
                        let _ = send_within(&mut tx, close_message(1000, ""), write_timeout).await;
                    }
                    return WriterEnd::ShellExited;
                }
                Some(event) => {
                    if !send_within(&mut tx, Message::Text(event.wire().into()), write_timeout)
                        .await
                    {
                        return WriterEnd::PeerGone;
                    }
                }
                None => {
                    return WriterEnd::Closed;
                }
            },
        }
    }
}

pub(super) fn text(parts: Value) -> Message {
    Message::Text(parts.to_string().into())
}

/// Timing knobs for [`bridge_session`]; production uses [`BridgeTimeouts::production`], while
/// in-memory unit tests can shorten them for millisecond determinism.
#[derive(Clone, Copy)]
pub(super) struct BridgeTimeouts {
    pub(super) write: Duration,
    pub(super) close: Duration,
    pub(super) flow: Duration,
    pub(super) ping: Duration,
    pub(super) reauthorize_every: Duration,
    pub(super) reauthorize_timeout: Duration,
}

impl BridgeTimeouts {
    pub(super) fn production(reauthorize_every: Duration) -> Self {
        Self {
            write: WRITE_TIMEOUT,
            close: CLOSE_TIMEOUT,
            flow: FLOW_TIMEOUT,
            ping: PING_EVERY,
            reauthorize_every,
            reauthorize_timeout: REAUTHORIZE_TIMEOUT,
        }
    }
}

/// Open the right [`TerminalSession`] adapter for `mode`, then drive it through [`bridge_session`].
pub(super) async fn run(
    inner: Inner,
    session_id: String,
    term_name: String,
    mode: TermMode,
    team: Option<Box<meerschaum_core::access::Team>>,
    socket: WebSocket,
) {
    let guard = match TerminalGuard::acquire() {
        Some(guard) => guard,
        None => {
            refuse(socket, TOO_MANY_TERMINALS, CLOSE_TRY_AGAIN).await;
            return;
        }
    };
    let _guard = guard;

    let session = match &mode {
        TermMode::Full | TermMode::Restricted | TermMode::Team(_) => {
            LocalPtyAdapter::open(&inner, &session_id, &term_name, &mode, team).await
        }
        TermMode::Remote(team_id) => {
            remote::RemoteExecutorAdapter::open_remote(
                &inner,
                &session_id,
                &term_name,
                *team_id,
                team,
            )
            .await
        }
        TermMode::HostApi(team_id) => {
            remote::RemoteExecutorAdapter::open_host_api(&inner, &session_id, &term_name, *team_id)
                .await
        }
    };
    let session = match session {
        Ok(session) => session,
        Err(error) => {
            refuse_open_error(socket, error).await;
            return;
        }
    };

    let timeouts = BridgeTimeouts::production(inner.config.reauthorize_every);
    let drain = inner.state.lifecycle.signal().wait();
    let reauth_inner = inner.clone();
    let reauth_session = session_id.clone();
    let reauth_name = term_name.clone();
    let reauth_mode = mode.clone();
    bridge_session(
        socket,
        session,
        drain,
        timeouts,
        move || {
            let inner = reauth_inner.clone();
            let session_id = reauth_session.clone();
            let term_name = reauth_name.clone();
            let mode = reauth_mode.clone();
            async move {
                let access = authorize(&inner, &session_id, true).await;
                access.revocation(&term_name, &mode)
            }
        },
        &term_name,
    )
    .await;
}

/// The single WebSocket bridge driving every [`TerminalSession`] across all 5 terminal modes.
///
/// Handles terminado framing, bounded input backpressure, ping/pong keepalives, periodic
/// reauthorization (fail-closed), graceful drain (`1001 Going Away`), and ordered teardown.
pub(super) async fn bridge_session<S, E, F, Fut>(
    socket: S,
    session: TerminalSession,
    drain: impl std::future::Future<Output = ()>,
    timeouts: BridgeTimeouts,
    mut reauthorize: F,
    term_name: &str,
) where
    S: futures_util::Stream<Item = Result<Message, E>>
        + futures_util::Sink<Message, Error = E>
        + Send
        + Unpin
        + 'static,
    E: Send + 'static,
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Option<String>>,
{
    let TerminalSession {
        input,
        events,
        on_revoke,
    } = session;
    let (tx, mut rx) = socket.split();
    // Every write goes through one task with bounded writes, so nothing here ever waits on a
    // browser that stopped reading: input, the session's checks and the teardown stay live.
    let (control, control_rx) = mpsc::channel::<Message>(CONTROL_QUEUE);
    let mut writer = tokio::spawn(write_terminal(tx, events, control_rx, timeouts.write));
    let mut writer_done = false;

    let mut ping = tokio::time::interval(timeouts.ping);
    ping.tick().await;

    let every = timeouts.reauthorize_every.max(Duration::from_millis(10));
    let mut recheck = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    tokio::pin!(drain);

    let mut closed_by_client = false;
    let mut revoked_reason: Option<String> = None;
    // Keep one bounded client input parked while waiting for backend queue capacity so a startup
    // burst applies backpressure instead of dropping or closing, while drain and reauthorization
    // stay live.
    let mut pending_input: Option<remote::Input> = None;
    let mut input_deadline: Option<tokio::time::Instant> = None;

    loop {
        tokio::select! {
            () = &mut drain => {
                queue(&control, [
                    reason_message(SHUTTING_DOWN),
                    close_message(1001, SHUTTING_DOWN),
                ]);
                break;
            }
            ended = &mut writer, if !writer_done => {
                writer_done = true;
                if matches!(ended, Ok(WriterEnd::PeerGone)) {
                    tracing::debug!("A browser stopped reading the terminal of '{term_name}'.");
                }
                break;
            }
            () = capacity_deadline(input_deadline), if pending_input.is_some() => {
                tracing::debug!("Terminal input backpressure stalled for '{term_name}'.");
                break;
            }
            permit = input.reserve(), if pending_input.is_some() => {
                let Ok(permit) = permit else {
                    break;
                };
                permit.send(pending_input.take().expect("pending input checked in the same task"));
                input_deadline = None;
            }
            message = rx.next(), if pending_input.is_none() => match message {
                Some(Ok(Message::Text(raw))) => {
                    if let Some(command) = parse_client_message(raw.as_str()) {
                        match input.try_send(command) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(command)) => {
                                pending_input = Some(command);
                                input_deadline = Some(tokio::time::Instant::now() + timeouts.flow);
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                break;
                            }
                        }
                    }
                }
                Some(Ok(Message::Ping(bytes))) => {
                    queue(&control, [Message::Pong(bytes)]);
                }
                Some(Ok(Message::Pong(_))) => {}
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                    closed_by_client = true;
                    break;
                }
                Some(Ok(_)) => {}
            },
            _ = ping.tick() => {
                // A browser that is gone or stopped reading is found by the writer's bounded
                // writes.
                queue(&control, [Message::Ping(Vec::new().into())]);
            }
            _ = recheck.tick() => {
                // Fail closed: an error or a store that does not answer ends the terminal too.
                let revocation = tokio::time::timeout(
                    timeouts.reauthorize_timeout,
                    reauthorize(),
                )
                .await
                .unwrap_or_else(|_| {
                    Some("Could not verify your session, so the terminal was closed.".into())
                });
                if let Some(reason) = revocation {
                    tracing::info!("Closing the terminal of '{term_name}': {reason}");
                    queue(&control, [
                        reason_message(&reason),
                        text(json!(["disconnect", 1])),
                        close_message(CLOSE_REVOKED, &reason),
                    ]);
                    revoked_reason = Some(reason);
                    break;
                }
            }
        }
    }

    // End the backend transport first (killing the local PTY client or closing the upstream
    // executor socket, never a valid detached tmux session), whether or not the browser ever reads
    // the closing reason.
    drop(input);
    // Let the socket writer deliver any queued closing frames while revocation side-effects run.
    drop(control);
    if let Some(reason) = revoked_reason {
        on_revoke.run(&reason).await;
    }
    if !writer_done
        && (closed_by_client
            || tokio::time::timeout(timeouts.close, &mut writer)
                .await
                .is_err())
    {
        writer.abort();
    }
}

async fn capacity_deadline(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(deadline) => tokio::time::sleep_until(deadline).await,
        None => std::future::pending::<()>().await,
    }
}

/// The result of an in-flight job; never ready without one.
pub(super) async fn joined<T>(
    job: &mut Option<tokio::task::JoinHandle<T>>,
) -> Result<T, tokio::task::JoinError> {
    match job {
        Some(handle) => handle.await,
        None => std::future::pending().await,
    }
}

/// Read the shell's readiness off the bridge (it may run tmux, bounded by `TMUX_TIMEOUT`).
pub(super) fn spawn_shell_state(
    config: &WebtermConfig,
    mux: Option<&TmuxMux>,
    session: Option<&str>,
    process_id: Option<u32>,
    guardian: Option<&NativeGuardian>,
) -> tokio::task::JoinHandle<Option<Value>> {
    let (config, mux, session, guardian) = (
        config.clone(),
        mux.cloned(),
        session.map(str::to_string),
        guardian.cloned(),
    );
    tokio::task::spawn_blocking(move || {
        shell_state(
            &config,
            mux.as_ref(),
            session.as_deref(),
            process_id,
            guardian.as_ref(),
        )
    })
}

/// Decode terminado's JSON arrays into typed [`remote::Input`]; anything else is ignored, as
/// terminado does.
pub(super) fn parse_client_message(raw: &str) -> Option<remote::Input> {
    let value: Value = serde_json::from_str(raw).ok()?;
    let items = value.as_array()?;
    match items.first()?.as_str()? {
        "stdin" => Some(remote::Input::Stdin(items.get(1)?.as_str()?.to_string())),
        "set_size" => {
            let dim = |i: usize| items.get(i).and_then(Value::as_u64);
            let (rows, cols) = (dim(1)?, dim(2)?);
            if rows == 0 || cols == 0 {
                return None;
            }
            let clamp = |n: u64| u16::try_from(n).unwrap_or(u16::MAX);
            Some(remote::Input::Resize {
                rows: clamp(rows),
                cols: clamp(cols),
                height: clamp(dim(3).unwrap_or(0)),
                width: clamp(dim(4).unwrap_or(0)),
            })
        }
        "new_window" => Some(remote::Input::NewWindow),
        "shell_state" => {
            let request = items.get(1)?.as_str()?;
            remote::valid_id(request).then(|| remote::Input::ShellState(request.to_string()))
        }
        _ => None,
    }
}

/// Counts running terminals against [`MAX_TERMINALS`].
pub(super) struct TerminalGuard;

impl TerminalGuard {
    pub(super) fn acquire() -> Option<TerminalGuard> {
        if ACTIVE.fetch_add(1, Ordering::SeqCst) >= MAX_TERMINALS {
            ACTIVE.fetch_sub(1, Ordering::SeqCst);
            return None;
        }
        Some(TerminalGuard)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        ACTIVE.fetch_sub(1, Ordering::SeqCst);
    }
}

/// How many terminals are running right now.
pub fn active_terminals() -> usize {
    ACTIVE.load(Ordering::SeqCst)
}

/// Incremental UTF-8 decoding of PTY output (a multi-byte character may straddle two reads);
/// invalid bytes become U+FFFD, as terminado's `replace` decoder does.
#[derive(Default)]
pub(super) struct Utf8Stream {
    pending: Vec<u8>,
}

impl Utf8Stream {
    pub(super) fn push(&mut self, bytes: &[u8]) -> String {
        self.pending.extend_from_slice(bytes);
        let mut out = String::new();
        loop {
            match std::str::from_utf8(&self.pending) {
                Ok(valid) => {
                    out.push_str(valid);
                    self.pending.clear();
                    return out;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    out.push_str(&String::from_utf8_lossy(&self.pending[..valid]));
                    match error.error_len() {
                        Some(bad) => {
                            out.push('\u{FFFD}');
                            self.pending.drain(..valid + bad);
                        }
                        None => {
                            self.pending.drain(..valid);
                            return out;
                        }
                    }
                }
            }
        }
    }

    pub(super) fn finish(&mut self) -> String {
        let rest = String::from_utf8_lossy(&self.pending).into_owned();
        self.pending.clear();
        rest
    }
}
