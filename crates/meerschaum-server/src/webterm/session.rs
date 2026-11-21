//! Unified `TerminalSession` seam and the `LocalPtyAdapter` production adapter.
//!
//! Every terminal mode (`Full`, `Restricted`, `Team`, `Remote`, `HostApi`) adapts into a
//! [`TerminalSession`] — a bounded `(Input, Event)` channel pair plus a typed [`RevokeAction`] —
//! so [`super::bridge`] drives framing, backpressure, ping/pong, reauthorization, drain, and
//! teardown exactly once.

use std::collections::VecDeque;
use std::time::Duration;

use super::*;

/// An active terminal backend connected to the single WebSocket bridge in [`super::bridge`].
pub(super) struct TerminalSession {
    pub(super) input: mpsc::Sender<remote::Input>,
    pub(super) events: mpsc::Receiver<remote::Event>,
    pub(super) on_revoke: RevokeAction,
}

impl TerminalSession {
    pub(super) fn new(terminal: remote::Terminal, on_revoke: RevokeAction) -> Self {
        Self {
            input: terminal.input,
            events: terminal.events,
            on_revoke,
        }
    }
}

/// Server-side teardown performed when an open terminal's session is revoked.
pub(super) enum RevokeAction {
    /// No additional teardown beyond dropping the session's channels.
    None,
    /// End a local tmux session and forget its policy grant.
    LocalTmux {
        mux: TmuxMux,
        session: String,
        policy: Policy,
        /// `true` ends on any revocation (`Team`); `false` ends only on [`ROLE_CHANGED`].
        always: bool,
    },
    /// Revoke a remote executor terminal lease.
    RemoteExecutor {
        state: AppState,
        team_id: uuid::Uuid,
        owner: String,
    },
    /// Revoke a host API terminal lease.
    HostApi {
        state: AppState,
        team_id: Option<uuid::Uuid>,
        owner: String,
        credential: String,
    },
}

impl RevokeAction {
    pub(super) async fn run(self, reason: &str) {
        match self {
            Self::None => {}
            Self::LocalTmux {
                mux,
                session,
                policy,
                always,
            } => {
                if always || reason == ROLE_CHANGED {
                    tokio::task::spawn_blocking(move || {
                        mux.end_session(&session);
                        policy.forget_session(&session);
                    });
                }
            }
            Self::RemoteExecutor {
                state,
                team_id,
                owner,
            } => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(10),
                    state.actions.revoke_executor_terminal(team_id, &owner),
                )
                .await;
            }
            Self::HostApi {
                state,
                team_id,
                owner,
                credential,
            } => {
                let _ = tokio::time::timeout(
                    Duration::from_secs(10),
                    state
                        .actions
                        .revoke_host_api_terminal(team_id, &owner, &credential),
                )
                .await;
            }
        }
    }
}

/// Why opening a [`TerminalSession`] failed before the bridge loop started.
pub(super) struct OpenError {
    pub(super) message: String,
    pub(super) code: u16,
    /// When `true`, wrap `message` as `\r\n[message]\r\n`; otherwise send verbatim.
    pub(super) bracketed: bool,
}

impl OpenError {
    pub(super) fn refused(message: impl Into<String>, code: u16) -> Self {
        Self {
            message: message.into(),
            code,
            bracketed: true,
        }
    }

    pub(super) fn launch_failed(error: impl std::fmt::Display) -> Self {
        Self {
            message: format!("Failed to start the shell: {error}\r\n"),
            code: 1011,
            bracketed: false,
        }
    }
}

/// Per-adapter policy knobs for [`LocalPtyAdapter`].
pub(super) struct LocalPtyOptions {
    /// Strip dangerous terminal string controls directly on UTF-8 output (Host API boundary).
    pub(super) filter_controls: bool,
    /// Close the transport if resize or a `new_window` blocking task returns an error.
    pub(super) strict_pty_errors: bool,
    /// Optional terminal slot guard owned by this adapter task (used by `open_api_terminal`).
    pub(super) guard: Option<TerminalGuard>,
    /// Optional launch identifier for debug transport tracing.
    pub(super) trace_launch: Option<String>,
}

/// Coalesces `shell_state` readiness queries so at most one runs per connection.
///
/// The latest arriving request waits behind an in-flight query; any older waiting request is
/// displaced and returned immediately so it can be answered with `null`.
#[derive(Default)]
pub(super) struct ShellStateCoalescer {
    in_flight: Option<String>,
    waiting: Option<String>,
}

impl ShellStateCoalescer {
    /// Record an incoming request. Returns `(start_now, displaced_id)`.
    pub(super) fn submit(&mut self, id: String) -> (bool, Option<String>) {
        if self.in_flight.is_none() {
            self.in_flight = Some(id);
            (true, None)
        } else {
            (false, self.waiting.replace(id))
        }
    }

    /// Complete the in-flight query. Returns `(completed_id, next_to_start)`.
    pub(super) fn complete(&mut self) -> (Option<String>, Option<String>) {
        let finished = self.in_flight.take();
        let next = self.waiting.take();
        if let Some(ref next_id) = next {
            self.in_flight = Some(next_id.clone());
        }
        (finished, next)
    }
}

/// Enforces the per-connection [`MAX_NEW_WINDOWS`] cap and serializes window creation.
#[derive(Default)]
pub(super) struct WindowBudget {
    opened: usize,
    queued: usize,
    in_flight: bool,
}

impl WindowBudget {
    /// Request a new window. Returns `true` if the caller should spawn a window task now.
    pub(super) fn request(&mut self, max_per_connection: usize) -> bool {
        if self.opened >= max_per_connection {
            return false;
        }
        self.opened += 1;
        if self.in_flight {
            self.queued += 1;
            false
        } else {
            self.in_flight = true;
            true
        }
    }

    /// Mark the active window task complete. Returns `true` if another queued window should start
    /// now; when `may_open` is `false` (the terminal limit, or tmux refused), the queued windows
    /// are dropped instead, and the next request starts at once.
    pub(super) fn complete(&mut self, may_open: bool) -> bool {
        if self.queued > 0 && may_open {
            self.queued -= 1;
            self.in_flight = true;
            true
        } else {
            self.queued = 0;
            self.in_flight = false;
            false
        }
    }
}

/// Adapts a local PTY (`Full`, `Restricted`, `Team`, and `HostApi`) into the unified
/// `(Input, Event)` channel pair.
pub(super) struct LocalPtyAdapter;

impl LocalPtyAdapter {
    /// Open a local PTY terminal for `Full`, `Restricted`, or `Team` mode.
    pub(super) async fn open(
        inner: &Inner,
        session_id: &str,
        term_name: &str,
        mode: &TermMode,
        team: Option<Box<meerschaum_core::access::Team>>,
    ) -> Result<TerminalSession, OpenError> {
        let mut config = inner.config.clone();
        if inner.is_executor() {
            config.shell = config
                .sessions
                .shell(session_id, &config.shell)
                .await
                .map_err(|_| {
                    OpenError::refused(
                        "This terminal's execution environment is unavailable.",
                        1011,
                    )
                })?;
        }
        let launch_result = match mode {
            TermMode::Full => {
                let config = config.clone();
                let name = term_name.to_string();
                tokio::task::spawn_blocking(move || launch(&config, &name, None)).await
            }
            TermMode::Restricted => {
                let address = if inner.is_executor() {
                    restricted::API_POLICY_ADDRESS.to_string()
                } else {
                    config
                        .policy
                        .listen(&inner.state, &config.policy_dir)
                        .await
                        .map_err(|error| {
                            tracing::warn!("Failed to start a terminal for '{term_name}': {error}");
                            OpenError::launch_failed(error)
                        })?
                };
                let config = config.clone();
                let name = term_name.to_string();
                tokio::task::spawn_blocking(move || launch(&config, &name, Some(&address))).await
            }
            TermMode::Team(team_name) => {
                let shell = match &team {
                    None if inner.is_executor() => Ok(config.shell.clone()),
                    Some(record) => inner
                        .state
                        .actions
                        .team_shell(record)
                        .map_err(std::io::Error::other),
                    None => Err(std::io::Error::other("no team record")),
                }
                .map_err(|error| {
                    tracing::warn!("Failed to start a terminal for '{term_name}': {error}");
                    OpenError::launch_failed(error)
                })?;
                let config_copy = config.clone();
                let name = term_name.to_string();
                let team_name = team_name.clone();
                let state = inner.state.clone();
                let every = config.reauthorize_every;
                // Factory lease expiry retires its own sessions, including detached windows.
                let check_membership = !inner.is_executor();
                let launched = tokio::task::spawn_blocking(move || {
                    launch_team(&config_copy, &name, &team_name, &shell, check_membership)
                })
                .await;
                if check_membership && matches!(launched, Ok(Ok(_))) {
                    start_team_sweeper(&state, every);
                }
                launched
            }
            TermMode::Remote(_) | TermMode::HostApi(_) => {
                return Err(OpenError::refused(
                    "The terminal's execution mode is unavailable.",
                    1011,
                ));
            }
        };
        let launched = match launch_result {
            Ok(Ok(started)) => started,
            Ok(Err(error)) => {
                tracing::warn!("Failed to start a terminal for '{term_name}': {error}");
                return Err(OpenError::launch_failed(error));
            }
            Err(_) => {
                return Err(OpenError::launch_failed("terminal launch task cancelled"));
            }
        };
        let on_revoke = match (&launched.mux, &launched.session) {
            (Some(mux), Some(session)) => RevokeAction::LocalTmux {
                mux: mux.clone(),
                session: session.clone(),
                policy: config.policy.clone(),
                always: matches!(mux.profile, TmuxProfile::Team(_)),
            },
            _ => RevokeAction::None,
        };
        let terminal = Self::spawn(
            config,
            launched,
            LocalPtyOptions {
                filter_controls: false,
                strict_pty_errors: false,
                guard: None,
                trace_launch: None,
            },
        );
        Ok(TerminalSession::new(terminal, on_revoke))
    }

    /// Spawn the single PTY select loop driving `launched` over a bounded `(Input, Event)` pair.
    pub(super) fn spawn(
        config: WebtermConfig,
        launched: Launched,
        options: LocalPtyOptions,
    ) -> remote::Terminal {
        use remote::{Controls, Event, Input, QUEUE};

        let Launched {
            pty,
            session,
            mux,
            shell,
            grant,
            guardian,
        } = launched;
        let Pty {
            process_id,
            master,
            input: writer,
            mut output,
            killer,
        } = pty;
        let (input, mut commands) = mpsc::channel::<Input>(QUEUE);
        let (events_tx, events) = mpsc::channel::<Event>(QUEUE);

        tokio::spawn(async move {
            let (_guard, _grant) = (options.guard, grant);
            let mut utf8 = Utf8Stream::default();
            let mut controls = options.filter_controls.then(Controls::default);
            let mut windows = WindowBudget::default();
            let mut window_job: Option<tokio::task::JoinHandle<std::io::Result<bool>>> = None;
            let mut state_job: Option<tokio::task::JoinHandle<Option<Value>>> = None;
            let mut coalescer = ShellStateCoalescer::default();
            let mut pending_events = VecDeque::from([Event::Setup]);

            if let Some(ref launch) = options.trace_launch {
                tracing::debug!(
                    target: "mrsm::terminal_transport",
                    layer = "host_pty",
                    launch = %launch,
                    reason = "opened",
                    "The private terminal transport opened."
                );
            }

            let spawn_window = |mux: &TmuxMux,
                                shell: &ShellCommand,
                                guardian: Option<&NativeGuardian>,
                                session: &str| {
                let (mux, shell, guardian, session) = (
                    mux.clone(),
                    shell.clone(),
                    guardian.cloned(),
                    session.to_string(),
                );
                tokio::task::spawn_blocking(move || {
                    mux.new_window(&shell, guardian.as_ref(), &session)
                })
            };

            let (reason, notify_disconnect) = loop {
                tokio::select! {
                    () = events_tx.closed() => break ("browser_event_receiver_closed", false),
                    permit = events_tx.reserve(), if !pending_events.is_empty() => {
                        let Ok(permit) = permit else {
                            break ("browser_event_receiver_closed", false);
                        };
                        if let Some(event) = pending_events.pop_front() {
                            permit.send(event);
                        }
                    }
                    result = joined(&mut window_job), if window_job.is_some() => {
                        window_job = None;
                        if options.strict_pty_errors && !matches!(result, Ok(Ok(_))) {
                            break ("window_task_failed", true);
                        }
                        let may_open = active_terminals() < MAX_TERMINALS
                            && config.tmux_refused().is_none();
                        if windows.complete(may_open)
                            && let (Some(mux), Some(session)) = (&mux, &session)
                        {
                            window_job = Some(spawn_window(mux, &shell, guardian.as_ref(), session));
                        }
                    }
                    result = joined(&mut state_job), if state_job.is_some() => {
                        state_job = None;
                        let (finished, next) = coalescer.complete();
                        if let Some(id) = finished {
                            let state = result
                                .ok()
                                .flatten()
                                .and_then(|value| serde_json::from_value(value).ok());
                            if pending_events.len() < QUEUE {
                                pending_events.push_back(Event::ShellState { id, state });
                            }
                        }
                        if next.is_some() {
                            state_job = Some(spawn_shell_state(
                                &config,
                                mux.as_ref(),
                                session.as_deref(),
                                process_id,
                                guardian.as_ref(),
                            ));
                        }
                    }
                    command = commands.recv() => {
                        let Some(command) = command else {
                            break ("browser_input_closed", false);
                        };
                        if options.strict_pty_errors && command.wire().is_err() {
                            break ("invalid_browser_input", true);
                        }
                        match command {
                            Input::Stdin(value) => match writer.try_send(value.into_bytes()) {
                                Ok(()) => {}
                                Err(std::sync::mpsc::TrySendError::Full(_)) => {
                                    tracing::warn!("A terminal's input queue is full; closing it.");
                                    pending_events.push_back(Event::Output(
                                        "\r\n[input queue full]\r\n".into(),
                                    ));
                                    break ("pty_input_queue_unavailable", true);
                                }
                                Err(std::sync::mpsc::TrySendError::Disconnected(_)) => {
                                    break ("pty_input_queue_unavailable", true);
                                }
                            },
                            Input::Resize {
                                rows,
                                cols,
                                height,
                                width,
                            } => {
                                let resized = master.resize(PtySize {
                                    rows,
                                    cols,
                                    pixel_height: height,
                                    pixel_width: width,
                                });
                                if options.strict_pty_errors && resized.is_err() {
                                    break ("pty_resize_failed", true);
                                }
                            }
                            Input::NewWindow => {
                                if active_terminals() < MAX_TERMINALS
                                    && config.tmux_refused().is_none()
                                    && let (Some(mux), Some(session)) = (&mux, &session)
                                    && windows.request(MAX_NEW_WINDOWS)
                                {
                                    window_job =
                                        Some(spawn_window(mux, &shell, guardian.as_ref(), session));
                                }
                            }
                            Input::ShellState(id) => {
                                let (start_now, displaced) = coalescer.submit(id);
                                if start_now {
                                    state_job = Some(spawn_shell_state(
                                        &config,
                                        mux.as_ref(),
                                        session.as_deref(),
                                        process_id,
                                        guardian.as_ref(),
                                    ));
                                } else if let Some(dropped) = displaced
                                    && pending_events.len() < QUEUE
                                {
                                    pending_events.push_back(Event::ShellState {
                                        id: dropped,
                                        state: None,
                                    });
                                }
                            }
                        }
                    }
                    chunk = output.recv(), if pending_events.is_empty() => {
                        let Some(chunk) = chunk else {
                            break ("pty_output_eof", true);
                        };
                        let text = utf8.push(&chunk);
                        // Filter directly on UTF-8 without serializing to JSON and re-parsing.
                        let text = match controls.as_mut() {
                            Some(filter) => filter.filter(&text),
                            None => text,
                        };
                        if !text.is_empty() {
                            pending_events.push_back(Event::Output(text));
                        }
                    }
                }
            };

            if let Some(ref launch) = options.trace_launch {
                tracing::debug!(
                    target: "mrsm::terminal_transport",
                    layer = "host_pty",
                    launch = %launch,
                    reason,
                    "The private terminal transport ended."
                );
            }
            if let Some(job) = state_job {
                job.abort();
            }
            if let Some(job) = window_job {
                job.abort();
            }
            if notify_disconnect {
                let rest = utf8.finish();
                let rest = match controls.as_mut() {
                    Some(filter) => filter.filter(&rest),
                    None => rest,
                };
                if !rest.is_empty() {
                    pending_events.push_back(Event::Output(rest));
                }
                pending_events.push_back(Event::Disconnected);
                let deadline = tokio::time::Instant::now() + CLOSE_TIMEOUT;
                while let Some(event) = pending_events.pop_front() {
                    if !matches!(
                        tokio::time::timeout_at(deadline, events_tx.send(event)).await,
                        Ok(Ok(()))
                    ) {
                        break;
                    }
                }
            }
            // Killing the attach client before its writer avoids sending EOF into the pane.
            drop(killer);
            drop(writer);
        });

        remote::Terminal { input, events }
    }
}
