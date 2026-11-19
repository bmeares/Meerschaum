//! Standalone network roles. None opens an instance or discovers a team's plugins.

use super::*;
use meerschaum_server::executor::egress::PinnedRelay;
pub(super) fn start(
    role: &str,
    parameters: &Map,
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
) -> ActionResult {
    if !matches!(role, "proxy" | "relay") {
        return Err(ActionError::new(
            "ValueError",
            "Unknown executor network role.",
        ));
    }
    let destination = if role == "relay" {
        let value = parameters.get("destination").ok_or_else(|| {
            ActionError::new(
                "ValueError",
                "A relay needs declared ports and upstream IP pins.",
            )
        })?;
        let relay: PinnedRelay =
            serde_json::from_value(serde_json::to_value(value).map_err(io_error)?)
                .map_err(|_| ActionError::new("ValueError", "Invalid pinned relay destination."))?;
        relay.validate().map_err(io_error)?;
        Some(relay)
    } else {
        None
    };
    let flags = flags(args)?;
    let host = flags.host.unwrap_or_else(|| "0.0.0.0".into());
    let port = flags.port.unwrap_or(3128);
    native::info(rt, &format!("Executor {role} starting on {host}."));
    crate::native::jobs::own_signals();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(io_error)?;
    let result = runtime.block_on(async {
        let mut shutdown = ShutdownSignal::new().map_err(io_error)?;
        let serving = async move {
            match destination {
                Some(destination) => {
                    meerschaum_server::executor::egress::serve_pinned_relays(&host, destination)
                        .await
                }
                None => {
                    let listener = tokio::net::TcpListener::bind((host.as_str(), port)).await?;
                    axum::serve(
                        listener,
                        meerschaum_server::executor::egress::proxy_router(),
                    )
                    .await
                }
            }
        };
        tokio::select! {
            result = serving => result.map(|()| SuccessTuple::success()).map_err(io_error),
            result = shutdown.wait() => result.map(|()| SuccessTuple::success()).map_err(io_error),
        }
    });
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}
