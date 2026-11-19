//! A host-selected API shell over the ordinary bounded terminal channel, without a listener.

use super::*;

/// Attach an approved private API terminal. The caller owns its immutable identity and source
/// lease; this transport owns only the PTY client. Disconnecting detaches a valid tmux session.
pub async fn open_api_terminal(
    config: WebtermConfig,
    term_name: String,
) -> Result<remote::Terminal, String> {
    use remote::{Event, Input, QUEUE};
    let valid = term_name
        .strip_prefix("host-api-terminal-")
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .is_some_and(|id| {
            !id.is_nil() && term_name == format!("host-api-terminal-{}", id.simple())
        });
    if !valid || !config.shell.clear_env {
        return Err("The host API terminal has no approved private runtime.".into());
    }
    let environment = |key: &str| {
        config
            .shell
            .env
            .iter()
            .rev()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    };
    if environment(meerschaum_core::team_scope::api_execution::PROFILE_VAR) != Some("1")
        || environment(meerschaum_core::team_scope::sandbox::runtime::LIFECYCLE_VAR)
            != Some("terminal")
    {
        return Err("The host API terminal has no native confinement guardian.".into());
    }
    let spec = environment(meerschaum_core::team_scope::sandbox::SPEC_VAR)
        .ok_or("The host API terminal has no mandatory spec.")?;
    let spec = meerschaum_core::team_scope::sandbox::Spec::read(Path::new(spec))
        .map_err(|_| "The host API terminal spec is unavailable.")?;
    if spec.mode != meerschaum_core::team_scope::sandbox::Mode::RequireSockets
        || spec.runtime.as_ref().is_none_or(|runtime| {
            std::fs::canonicalize(&config.shell.program).ok().as_ref() != Some(&runtime.launcher)
                || meerschaum_core::self_exe::native_executable().as_ref()
                    != Some(&runtime.launcher)
        })
    {
        return Err("The host API terminal must use its mandatory native runtime.".into());
    }
    let guard = TerminalGuard::acquire().ok_or(TOO_MANY_TERMINALS)?;
    let trace_launch = term_name.clone();
    let launch_config = config.clone();
    let launched = tokio::task::spawn_blocking(move || {
        launch(
            &launch_config,
            &term_name,
            Some(restricted::API_POLICY_ADDRESS),
        )
    })
    .await
    .map_err(|_| "The API terminal launch task failed.")?
    .map_err(|_| "The confined API terminal could not start.")?;
    let Launched {
        pty,
        session,
        tmux,
        shell,
        grant,
        ..
    } = launched;
    let Pty {
        process_id,
        master,
        input: writer,
        mut output,
        killer,
    } = pty;
    let (input, mut commands) = mpsc::channel::<Input>(QUEUE);
    let (sender, events) = mpsc::channel::<Event>(QUEUE);
    tokio::spawn(async move {
        let (_guard, _grant) = (guard, grant);
        let mut utf8 = Utf8Stream::default();
        let mut decoder = remote::Decoder::default();
        let mut windows = 0usize;
        let mut window_job: Option<tokio::task::JoinHandle<std::io::Result<bool>>> = None;
        let mut state_job: Option<tokio::task::JoinHandle<Option<Value>>> = None;
        let mut state_id: Option<String> = None;
        if !send(&sender, Event::Setup).await {
            tracing::debug!(target: "mrsm::terminal_transport", layer = "host_pty", launch = %trace_launch, reason = "setup_send_failed", "The private terminal transport ended.");
            drop(killer);
            drop(writer);
            return;
        }
        tracing::debug!(target: "mrsm::terminal_transport", layer = "host_pty", launch = %trace_launch, reason = "opened", "The private terminal transport opened.");
        let reason = loop {
            tokio::select! {
                () = sender.closed() => break "browser_event_receiver_closed",
                result = joined(&mut window_job), if window_job.is_some() => {
                    window_job = None;
                    if !matches!(result, Ok(Ok(_))) { break "window_task_failed"; }
                }
                result = joined(&mut state_job), if state_job.is_some() => {
                    state_job = None;
                    if let Some(id) = state_id.take() {
                        let state = result.ok().flatten()
                            .and_then(|value| serde_json::from_value(value).ok());
                        if !send(&sender, Event::ShellState { id, state }).await { break "readiness_send_failed"; }
                    }
                }
                command = commands.recv() => {
                    let Some(command) = command else { break "browser_input_closed"; };
                    if command.wire().is_err() { break "invalid_browser_input"; }
                    match command {
                        Input::Stdin(value) => {
                            if writer.try_send(value.into_bytes()).is_err() { break "pty_input_queue_unavailable"; }
                        }
                        Input::Resize { rows, cols, height, width } => {
                            if master.resize(PtySize { rows, cols, pixel_height: height, pixel_width: width }).is_err() { break "pty_resize_failed"; }
                        }
                        Input::NewWindow => {
                            if windows < MAX_NEW_WINDOWS && window_job.is_none()
                                && active_terminals() < MAX_TERMINALS
                                && let (Some(settings), Some(session)) = (&tmux, &session)
                                && config.tmux_refused().is_none()
                            {
                                windows += 1;
                                let (settings, session, shell) = (settings.clone(), session.clone(), shell.clone());
                                window_job = Some(tokio::task::spawn_blocking(move || tmux_new_window(&settings, &shell, &session)));
                            }
                        }
                        Input::ShellState(id) => {
                            if state_job.is_none() {
                                state_job = Some(spawn_shell_state(&config, tmux.as_ref(), session.as_deref(), process_id));
                                state_id = Some(id);
                            } else if !send(&sender, Event::ShellState { id, state: None }).await { break "busy_readiness_send_failed"; }
                        }
                    }
                }
                chunk = output.recv() => {
                    let Some(chunk) = chunk else { break "pty_output_eof"; };
                    let text = utf8.push(&chunk);
                    if !text.is_empty() {
                        let Ok(event) = decoder.decode(&Event::Output(text).wire()) else { break "pty_output_protocol_refused"; };
                        if !send(&sender, event).await { break "pty_output_send_failed"; }
                    }
                }
            }
        };
        tracing::debug!(target: "mrsm::terminal_transport", layer = "host_pty", launch = %trace_launch, reason, "The private terminal transport ended.");
        if let Some(job) = state_job {
            job.abort();
        }
        if let Some(job) = window_job {
            job.abort();
        }
        let rest = utf8.finish();
        if !rest.is_empty()
            && let Ok(event) = decoder.decode(&Event::Output(rest).wire())
        {
            let _ = send(&sender, event).await;
        }
        let _ = send(&sender, Event::Disconnected).await;
        // Killing the attach client before its writer avoids sending EOF into the pane.
        drop(killer);
        drop(writer);
    });
    Ok(remote::Terminal { input, events })
}

async fn send(sender: &mpsc::Sender<remote::Event>, event: remote::Event) -> bool {
    matches!(
        tokio::time::timeout(Duration::from_secs(5), sender.send(event)).await,
        Ok(Ok(()))
    )
}
