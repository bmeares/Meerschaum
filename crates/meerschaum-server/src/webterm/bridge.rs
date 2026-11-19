//! WebSocket upgrade, terminado wire protocol, and active terminal accounting.

use super::*;

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
                .executor
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
    Message::Close(Some(CloseFrame {
        code,
        reason: reason.into(),
    }))
}

/// Send one message, giving up after [`WRITE_TIMEOUT`]; `false` when it was not sent.
pub(super) async fn send_within<S>(sink: &mut S, message: Message) -> bool
where
    S: futures_util::Sink<Message> + Unpin,
{
    matches!(
        tokio::time::timeout(WRITE_TIMEOUT, sink.send(message)).await,
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
    if send_within(&mut socket, reason_message(reason)).await
        && send_within(&mut socket, text(json!(["disconnect", 1]))).await
    {
        let _ = send_within(&mut socket, close_message(code, "")).await;
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
    /// The PTY closed (the shell exited): its last output, `["disconnect", 1]` and a close frame
    /// were sent.
    ShellExited,
    /// It sent the bridge's close frame, or the bridge let go of it.
    Closed,
    /// A write failed or did not finish within [`WRITE_TIMEOUT`] (the browser is gone or stopped
    /// reading).
    PeerGone,
}

/// The only task that writes to a terminal's socket: the bridge's messages (`control`, first)
/// and the PTY's output, each write bounded by [`WRITE_TIMEOUT`]. A browser that stops reading
/// stalls only this task; the bridge keeps reading input and checking the session.
pub(super) async fn write_terminal(
    mut tx: futures_util::stream::SplitSink<WebSocket, Message>,
    mut output: mpsc::Receiver<Vec<u8>>,
    mut control: mpsc::Receiver<Message>,
) -> WriterEnd {
    let mut decoder = Utf8Stream::default();
    loop {
        tokio::select! {
            biased;
            message = control.recv() => {
                let Some(message) = message else {
                    return WriterEnd::Closed;
                };
                let close = matches!(message, Message::Close(_));
                if !send_within(&mut tx, message).await {
                    return WriterEnd::PeerGone;
                }
                if close {
                    return WriterEnd::Closed;
                }
            }
            chunk = output.recv() => match chunk {
                Some(bytes) => {
                    let out = decoder.push(&bytes);
                    if !out.is_empty()
                        && !send_within(&mut tx, text(json!(["stdout", out]))).await
                    {
                        return WriterEnd::PeerGone;
                    }
                }
                None => {
                    // Each write waits only while the previous one went through.
                    let rest = decoder.finish();
                    if (rest.is_empty()
                        || send_within(&mut tx, text(json!(["stdout", rest]))).await)
                        && send_within(&mut tx, text(json!(["disconnect", 1]))).await
                    {
                        let _ = send_within(&mut tx, close_message(1000, "")).await;
                    }
                    return WriterEnd::ShellExited;
                }
            },
        }
    }
}

pub(super) fn text(parts: Value) -> Message {
    Message::Text(parts.to_string().into())
}

/// Bridge one websocket to one PTY until either side ends, or the session's access does.
pub(super) async fn run(
    inner: Inner,
    session_id: String,
    term_name: String,
    mode: TermMode,
    team: Option<Box<meerschaum_core::access::Team>>,
    mut socket: WebSocket,
) {
    let restricted = mode == TermMode::Restricted;
    let mut config = inner.config.clone();
    let guard = match TerminalGuard::acquire() {
        Some(guard) => guard,
        None => {
            refuse(socket, TOO_MANY_TERMINALS, CLOSE_TRY_AGAIN).await;
            return;
        }
    };
    let _guard = guard;
    if let TermMode::HostApi(team_id) = mode {
        remote::bridge_host(inner, session_id, term_name, team_id, socket).await;
        return;
    }
    if let TermMode::Remote(team_id) = mode {
        if let Some(team) = team {
            remote::bridge(inner, session_id, term_name, team_id, *team, socket).await;
        } else {
            refuse(
                socket,
                "The terminal's executor placement is unavailable.",
                1011,
            )
            .await;
        }
        return;
    }
    if inner.executor {
        match config.sessions.shell(&session_id, &config.shell).await {
            Ok(shell) => config.shell = shell,
            Err(_) => {
                refuse(
                    socket,
                    "This terminal's execution environment is unavailable.",
                    1011,
                )
                .await;
                return;
            }
        }
    }
    // A restricted shell asks this server's policy socket before it runs anything.
    let policy_address = if restricted && inner.executor {
        Ok(Some(restricted::API_POLICY_ADDRESS.to_string()))
    } else if restricted {
        config
            .policy
            .listen(&inner.state, &config.policy_dir)
            .await
            .map(Some)
    } else {
        Ok(None)
    };
    // A team terminal's shell is a process of the team (5.1), from the host of the actions.
    let team_shell = match (&mode, &team) {
        (TermMode::Team(_), None) if inner.executor => Some(Ok(config.shell.clone())),
        (TermMode::Team(_), Some(record)) => Some(
            inner
                .state
                .actions
                .team_shell(record)
                .map_err(std::io::Error::other),
        ),
        (TermMode::Team(_), None) => Some(Err(std::io::Error::other("no team record"))),
        _ => None,
    };
    let launch = match (policy_address, team_shell) {
        (_, Some(Err(error))) => Ok(Err(error)),
        (_, Some(Ok(shell))) => {
            let config = config.clone();
            let name = term_name.clone();
            let team_name = match &mode {
                TermMode::Team(team) => team.clone(),
                _ => String::new(),
            };
            let state = inner.state.clone();
            let every = config.reauthorize_every;
            // Factory lease expiry retires its own sessions, including detached windows.
            let check_membership = !inner.executor;
            let launched = tokio::task::spawn_blocking(move || {
                launch_team(&config, &name, &team_name, &shell, check_membership)
            })
            .await;
            if check_membership && matches!(launched, Ok(Ok(_))) {
                start_team_sweeper(&state, every);
            }
            launched
        }
        (Ok(address), None) => {
            let config = config.clone();
            let name = term_name.clone();
            tokio::task::spawn_blocking(move || launch(&config, &name, address.as_deref())).await
        }
        (Err(error), None) => Ok(Err(error)),
    };
    let Launched {
        pty,
        session: tmux_session,
        tmux: active_tmux,
        shell: active_shell,
        grant: _grant,
        team_window,
    } = match launch {
        Ok(Ok(started)) => started,
        Ok(Err(error)) => {
            tracing::warn!("Failed to start a terminal for '{term_name}': {error}");
            let reason = text(json!([
                "stdout",
                format!("Failed to start the shell: {error}\r\n")
            ]));
            if send_within(&mut socket, reason).await
                && send_within(&mut socket, text(json!(["disconnect", 1]))).await
            {
                let _ = send_within(&mut socket, close_message(1011, "")).await;
            }
            return;
        }
        Err(_) => return,
    };
    let Pty {
        process_id,
        master,
        input,
        output,
        killer,
    } = pty;
    let (tx, mut rx) = socket.split();
    // Every write goes through one task with bounded writes, so nothing here ever waits on a
    // browser that stopped reading: input, the session's checks and the teardown stay live.
    let (control, control_rx) = mpsc::channel::<Message>(CONTROL_QUEUE);
    queue(&control, [text(json!(["setup", {}]))]);
    let mut writer = tokio::spawn(write_terminal(tx, output, control_rx));
    let mut writer_done = false;
    let mut ping = tokio::time::interval(Duration::from_secs(30));
    ping.tick().await;
    // The session, its user and the permission are checked again while the terminal is open: a
    // logout, a deleted user or a removed permission must not leave a shell running.
    let every = config.reauthorize_every.max(Duration::from_millis(10));
    let mut recheck = tokio::time::interval_at(tokio::time::Instant::now() + every, every);
    recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut closed_by_client = false;
    let mut new_windows = 0usize;
    // At most one `shell_state` query runs per connection (each may run tmux). Requests that
    // arrive meanwhile are coalesced: the latest waits for the next query, an older one is
    // answered at once without a snapshot (the browser's probe treats that as "unknown").
    let mut shell_job: Option<tokio::task::JoinHandle<Option<Value>>> = None;
    let mut shell_request: Option<String> = None;
    let mut shell_waiting: Option<String> = None;
    // A draining server closes the terminal at once (`1001 Going Away`): the process on the PTY
    // ends (a tmux session survives it), and the browser may reconnect once a server is back.
    let drain = inner.state.lifecycle.signal().wait();
    tokio::pin!(drain);
    loop {
        tokio::select! {
            () = &mut drain => {
                queue(&control, [
                    reason_message(SHUTTING_DOWN),
                    close_message(1001, SHUTTING_DOWN),
                ]);
                break;
            }
            snapshot = joined(&mut shell_job), if shell_job.is_some() => {
                shell_job = None;
                let snapshot = snapshot.ok().flatten();
                if let Some(request) = shell_request.take() {
                    queue(&control, [text(json!(["shell_state", request, snapshot]))]);
                }
                if let Some(request) = shell_waiting.take() {
                    let session = tmux_session.as_deref();
                    shell_job = Some(spawn_shell_state(
                        &config,
                        active_tmux.as_ref(),
                        session,
                        process_id,
                    ));
                    shell_request = Some(request);
                }
            }
            ended = &mut writer, if !writer_done => {
                // The shell exited (its last output and close frame are sent), or the browser is
                // gone or stopped reading.
                writer_done = true;
                if matches!(ended, Ok(WriterEnd::PeerGone)) {
                    tracing::debug!("A browser stopped reading the terminal of '{term_name}'.");
                }
                break;
            }
            message = rx.next() => match message {
                Some(Ok(Message::Text(raw))) => {
                    match parse_client_message(raw.as_str()) {
                        Some(ClientMessage::Stdin(data)) => {
                            // Never block the bridge on a stuck shell (its output could not be
                            // drained meanwhile): a full queue ends the connection.
                            if let Err(std::sync::mpsc::TrySendError::Full(_)) =
                                input.try_send(data.into_bytes())
                            {
                                tracing::warn!("A terminal's input queue is full; closing it.");
                                queue(&control, [
                                    text(json!(["stdout", "\r\n[input queue full]\r\n"])),
                                    text(json!(["disconnect", 1])),
                                    close_message(1000, ""),
                                ]);
                                break;
                            }
                        }
                        Some(ClientMessage::SetSize(size)) => {
                            let _ = master.resize(size);
                        }
                        Some(ClientMessage::NewWindow) => {
                            // Each window is a process: bounded per connection and per user
                            // session (`MAX_SESSION_WINDOWS`, across reconnects), and refused
                            // while the server is at its terminal limit. tmux runs off the
                            // bridge (and bounded): it never delays input or a revocation.
                            // A restricted terminal's window runs its restricted shell on the
                            // restricted tmux server (`active_tmux`, `active_shell`).
                            if new_windows < MAX_NEW_WINDOWS
                                && active_terminals() < MAX_TERMINALS
                                && let (Some(settings), Some(session)) =
                                    (&active_tmux, &tmux_session)
                                && config.tmux_refused().is_none()
                            {
                                new_windows += 1;
                                let (settings, shell, session) =
                                    (settings.clone(), active_shell.clone(), session.clone());
                                tokio::task::spawn_blocking(move || {
                                    if team_window {
                                        team_new_window(&settings, &shell, &session)
                                    } else {
                                        tmux_new_window(&settings, &shell, &session)
                                    }
                                });
                            }
                        }
                        Some(ClientMessage::ShellState(request)) => {
                            if shell_job.is_none() {
                                let session = tmux_session.as_deref();
                                shell_job = Some(spawn_shell_state(
                                    &config,
                                    active_tmux.as_ref(),
                                    session,
                                    process_id,
                                ));
                                shell_request = Some(request);
                            } else if let Some(dropped) = shell_waiting.replace(request) {
                                queue(&control, [text(json!(["shell_state", dropped, null]))]);
                            }
                        }
                        None => {}
                    }
                }
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
                let access = tokio::time::timeout(
                    REAUTHORIZE_TIMEOUT,
                    authorize(&inner, &session_id, true),
                )
                .await
                .unwrap_or(Access::Unavailable);
                if let Some(reason) = access.revocation(&term_name, &mode) {
                    tracing::info!("Closing the terminal of '{term_name}': {reason}");
                    // A changed role ends the user's tmux session of the old mode too: an
                    // unrestricted shell cannot be restricted after the fact, so a demoted user
                    // never reattaches one (and its windows and commands end now). A team
                    // terminal's session ends with any revocation (5.1).
                    if (reason == ROLE_CHANGED || team_window)
                        && let (Some(settings), Some(session)) = (&active_tmux, &tmux_session)
                    {
                        let (settings, session, policy) =
                            (settings.clone(), session.clone(), config.policy.clone());
                        tokio::task::spawn_blocking(move || {
                            end_tmux_session(&settings, &session);
                            policy.forget_session(&session);
                        });
                    }
                    queue(&control, [
                        reason_message(&reason),
                        text(json!(["disconnect", 1])),
                        close_message(CLOSE_REVOKED, &reason),
                    ]);
                    break;
                }
            }
        }
    }
    // End the process on the PTY (the tmux client, never the tmux session) first, whether or not
    // the browser ever reads the reason, then close the input queue: the writer thread drops its
    // writer only after the process has exited, so the EOF it sends reaches no one (see
    // `spawn_pty`).
    drop(killer);
    drop(input);
    // A `shell_state` query still running is this connection's: stop it (its tmux command is
    // bounded by `TMUX_TIMEOUT` either way).
    if let Some(job) = shell_job.take() {
        job.abort();
    }
    // Then let the socket's writer deliver what was queued (the reason, the close frame), for a
    // bounded time.
    drop(control);
    if !writer_done
        && (closed_by_client
            || tokio::time::timeout(CLOSE_TIMEOUT, &mut writer)
                .await
                .is_err())
    {
        writer.abort();
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
    tmux: Option<&TmuxSettings>,
    session: Option<&str>,
    process_id: Option<u32>,
) -> tokio::task::JoinHandle<Option<Value>> {
    let (config, tmux, session) = (config.clone(), tmux.cloned(), session.map(str::to_string));
    tokio::task::spawn_blocking(move || {
        shell_state(&config, tmux.as_ref(), session.as_deref(), process_id)
    })
}

/// What a browser may send.
pub(super) enum ClientMessage {
    Stdin(String),
    SetSize(PtySize),
    NewWindow,
    ShellState(String),
}

/// Decode terminado's JSON arrays; anything else is ignored, as terminado does.
pub(super) fn parse_client_message(raw: &str) -> Option<ClientMessage> {
    let value: Value = serde_json::from_str(raw).ok()?;
    let items = value.as_array()?;
    match items.first()?.as_str()? {
        "stdin" => Some(ClientMessage::Stdin(items.get(1)?.as_str()?.to_string())),
        "set_size" => {
            let dim = |i: usize| items.get(i).and_then(Value::as_u64);
            let (rows, cols) = (dim(1)?, dim(2)?);
            if rows == 0 || cols == 0 {
                return None;
            }
            let clamp = |n: u64| u16::try_from(n).unwrap_or(u16::MAX);
            Some(ClientMessage::SetSize(PtySize {
                rows: clamp(rows),
                cols: clamp(cols),
                pixel_height: clamp(dim(3).unwrap_or(0)),
                pixel_width: clamp(dim(4).unwrap_or(0)),
            }))
        }
        "new_window" => Some(ClientMessage::NewWindow),
        "shell_state" => {
            let request = items.get(1)?.as_str()?;
            (request.len() <= 128).then(|| ClientMessage::ShellState(request.to_string()))
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
