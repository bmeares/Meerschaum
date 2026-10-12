//! Route paths (`STATIC_CONFIG['api']['endpoints']`) and path builders shared by the client and
//! the server.

/// `/`
pub const INDEX: &str = "/";
/// `/favicon.ico`
pub const FAVICON: &str = "/favicon.ico";
/// `/plugins`
pub const PLUGINS: &str = "/plugins";
/// `/pipes`
pub const PIPES: &str = "/pipes";
/// `/metadata`
pub const METADATA: &str = "/metadata";
/// `/actions`
pub const ACTIONS: &str = "/actions";
/// `/jobs`
pub const JOBS: &str = "/jobs";
/// `/logs`
pub const LOGS: &str = "/logs";
/// `/users`
pub const USERS: &str = "/users";
/// `/tokens`
pub const TOKENS: &str = "/tokens";
/// `/login`
pub const LOGIN: &str = "/login";
/// `/connectors`
pub const CONNECTORS: &str = "/connectors";
/// `/version`
pub const VERSION: &str = "/version";
/// `/chaining`
pub const CHAINING: &str = "/chaining";
/// `/ws`
pub const WEBSOCKET: &str = "/ws";
/// `/dash`
pub const DASH: &str = "/dash";
/// `/mcp`
pub const MCP: &str = "/mcp";
/// `/webterm/{session_id}`
pub const WEBTERM: &str = "/webterm/{session_id}";
/// `/websocket/{session_id}`
pub const WEBTERM_WEBSOCKET: &str = "/websocket/{session_id}";
/// `/info`
pub const INFO: &str = "/info";
/// `/healthcheck`
pub const HEALTHCHECK: &str = "/healthcheck";
/// `/docs`
pub const DOCS: &str = "/docs";
/// `/redoc`
pub const REDOC: &str = "/redoc";
/// `/openapi.json`
pub const OPENAPI: &str = "/openapi.json";
/// `/id` (registered only when the server runs with `--debug`).
pub const ID: &str = "/id";
/// `/engine` (§5.8, new in v5).
pub const ENGINE: &str = "/engine";
/// `/engine/ledger`
pub const ENGINE_LEDGER: &str = "/engine/ledger";
/// `/engine/cursors`
pub const ENGINE_CURSORS: &str = "/engine/cursors";
/// `/engine/state`
pub const ENGINE_STATE: &str = "/engine/state";
/// `/engine/runs`
pub const ENGINE_RUNS: &str = "/engine/runs";
/// `/engine/events`
pub const ENGINE_EVENTS: &str = "/engine/events";
/// `/engine/leases`
pub const ENGINE_LEASES: &str = "/engine/leases";

/// `STATIC_CONFIG['api']['jobs']['stdin_message']`.
pub const JOBS_STDIN_MESSAGE: &str = "MRSM_STDIN";
/// `STATIC_CONFIG['api']['jobs']['stop_message']`.
pub const JOBS_STOP_MESSAGE: &str = "MRSM_STOP";
/// `STATIC_CONFIG['api']['jobs']['metadata_cache_seconds']`.
pub const JOB_METADATA_CACHE_SECONDS: u64 = 5;
/// `STATIC_CONFIG['api']['jobs']['temp_prefix']`.
pub const JOBS_TEMP_PREFIX: &str = ".api-temp-";
/// `STATIC_CONFIG['api']['default_timeout']` (seconds).
pub const DEFAULT_TIMEOUT_SECONDS: u64 = 600;
/// `STATIC_CONFIG['api']['oauth']['token_expires_minutes']`.
pub const TOKEN_EXPIRES_MINUTES: i64 = 720;

/// Targets the server refuses to sync into or read from (`api/routes/_pipes.py`).
pub const PROTECTED_TARGETS: &[&str] = &["mrsm_users", "mrsm_plugins", "mrsm_pipes", "mrsm_tokens"];

/// `pipe_r_url(pipe)`: `/pipes/{connector_keys}/{metric_key}/{location_key}`, with a null
/// location sent as `[None]`.
pub fn pipe_r_url(connector_keys: &str, metric_key: &str, location_key: Option<&str>) -> String {
    format!(
        "{PIPES}/{connector_keys}/{metric_key}/{}",
        location_key.unwrap_or("[None]")
    )
}

/// `plugin_r_url(plugin)`: `/plugins/{name}`.
pub fn plugin_r_url(name: &str) -> String {
    format!("{PLUGINS}/{name}")
}

/// The server-side normalization of a location key path segment
/// (`location_key in ('[None]', 'None', 'null')` means `None`).
pub fn parse_location_segment(segment: &str) -> Option<&str> {
    match segment {
        "[None]" | "None" | "null" => None,
        other => Some(other),
    }
}

/// The server rule at `api/__init__.py:270`: pipes with connector keys `mrsm` are never served
/// over the generic routes (engine state uses `/engine/*`).
///
/// PARITY: the 403 detail text is preserved verbatim.
pub fn refuse_mrsm_connector(connector_keys: &str) -> Option<(u16, &'static str)> {
    (connector_keys == "mrsm").then_some((
        403,
        "Unable to serve any pipes with connector keys `mrsm` over the API.",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipe_urls() {
        assert_eq!(
            pipe_r_url("sql:main", "weather", None),
            "/pipes/sql:main/weather/[None]"
        );
        assert_eq!(pipe_r_url("a", "b", Some("c")), "/pipes/a/b/c");
        assert_eq!(parse_location_segment("[None]"), None);
        assert_eq!(parse_location_segment("home"), Some("home"));
        assert!(refuse_mrsm_connector("mrsm").is_some());
        assert!(refuse_mrsm_connector("sql:main").is_none());
    }
}
