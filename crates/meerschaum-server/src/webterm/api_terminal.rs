//! A host-selected API shell over the ordinary bounded terminal channel, without a listener.

use super::*;

/// Attach an approved private API terminal. The caller owns its immutable identity and source
/// lease; this transport owns only the PTY client. Disconnecting detaches a valid tmux session.
pub async fn open_api_terminal(
    config: WebtermConfig,
    term_name: String,
) -> Result<remote::Terminal, String> {
    let valid = term_name
        .strip_prefix("host-api-terminal-")
        .and_then(|value| uuid::Uuid::parse_str(value).ok())
        .is_some_and(|id| {
            !id.is_nil() && term_name == format!("host-api-terminal-{}", id.simple())
        });
    if !valid {
        return Err("The host API terminal has no approved private runtime.".into());
    }
    let guardian = NativeGuardian::require(&config.shell).map_err(|error| error.to_string())?;
    let guard = TerminalGuard::acquire().ok_or(TOO_MANY_TERMINALS)?;
    let trace_launch = term_name.clone();
    let launch_config = config.clone();
    let launched = tokio::task::spawn_blocking(move || {
        launch_with_guardian(
            &launch_config,
            &term_name,
            Some(restricted::API_POLICY_ADDRESS),
            Some(guardian),
        )
    })
    .await
    .map_err(|_| "The API terminal launch task failed.")?
    .map_err(|_| "The confined API terminal could not start.")?;
    Ok(LocalPtyAdapter::spawn(
        config,
        launched,
        LocalPtyOptions {
            filter_controls: true,
            strict_pty_errors: true,
            guard: Some(guard),
            trace_launch: Some(trace_launch),
        },
    ))
}
