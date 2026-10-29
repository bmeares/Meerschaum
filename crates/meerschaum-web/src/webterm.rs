//! The web terminal of the console: `/webterm/{session_id}` (+ its websocket) is the standalone
//! page v4 served, which the Dash console embeds in an iframe (D21). The PTY bridge and its
//! authentication live in `meerschaum_server::webterm`; this module gives it the console's
//! session store, the page and the assets (`/webterm/assets/{name}`).
use super::*;
use meerschaum_server::webterm::{SessionLookup, WebtermConfig};

pub use meerschaum_server::webterm::ShellCommand;

/// What the console needs to serve the terminal. `None` in [`WebConfig::webterm`] (or
/// `--no-webterm`) serves no terminal at all.
#[derive(Clone)]
pub struct WebtermSettings {
    /// The program each terminal runs (`<mrsm> -i <instance>`).
    pub shell: ShellCommand,
    /// `Some(false)` forces a plain shell per connection even when tmux is enabled and installed.
    pub use_tmux: Option<bool>,
}

impl WebtermSettings {
    /// Settings for a shell, with tmux per `api:webterm:tmux:enabled`.
    pub fn new(shell: ShellCommand) -> Self {
        Self {
            shell,
            use_tmux: None,
        }
    }
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

/// A URL path segment: everything but RFC 3986's unreserved characters is escaped (a session id
/// is a UUID, so normally nothing is).
fn segment(id: &str) -> String {
    const UNRESERVED: &percent_encoding::AsciiSet = &percent_encoding::NON_ALPHANUMERIC
        .remove(b'-')
        .remove(b'_')
        .remove(b'.')
        .remove(b'~');
    percent_encoding::utf8_percent_encode(id, UNRESERVED).to_string()
}

/// The console session store as the terminal's authentication.
struct Lookup(Arc<dyn sessions::SessionStore>);

#[async_trait::async_trait]
impl SessionLookup for Lookup {
    async fn username(&self, session_id: &str) -> Result<Option<String>, String> {
        // Session ids are UUIDs, like the cookie's.
        if uuid::Uuid::parse_str(session_id).is_err() {
            return Ok(None);
        }
        Ok(self
            .0
            .get(session_id)
            .await?
            .and_then(|data| data["username"].as_str().map(str::to_string)))
    }
}

/// The terminal's configuration for this console, if one is served.
pub(crate) fn config(server: &AppState, config: &WebConfig) -> Option<WebtermConfig> {
    let settings = config.webterm.as_ref()?;
    if server.config.no_webterm {
        return None;
    }
    let mut webterm = WebtermConfig::new(
        server,
        Arc::new(Lookup(config.sessions.clone())),
        Arc::new(standalone_page),
        settings.shell.clone(),
    );
    if settings.use_tmux == Some(false) {
        webterm = webterm.with_tmux(None);
    }
    Some(webterm)
}

const SCRIPTS: [&str; 4] = [
    "xterm.js",
    "addon-fit.js",
    "addon-unicode11.js",
    "webterm.js",
];

fn scripts() -> String {
    SCRIPTS
        .iter()
        .map(|name| format!("<script defer src=\"/webterm/assets/{name}\"></script>"))
        .collect()
}

/// `/webterm/{session_id}`: the page v4's `termpage.html` was, on the vendored assets.
pub(crate) fn standalone_page(session_id: &str) -> String {
    format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<meta name=\"referrer\" content=\"no-referrer\"><title>Meerschaum Shell</title>\
<link rel=\"stylesheet\" href=\"/webterm/assets/xterm.css\">\
<link rel=\"stylesheet\" href=\"/webterm/assets/webterm.css\"></head>\
<body class=\"mrsm-webterm-standalone\">\
<div class=\"mrsm-webterm-screen\" id=\"xterm-div\" data-mrsm-webterm data-mode=\"standalone\" data-ws=\"/websocket/{id}\"></div>\
{scripts}</body></html>",
        id = escape(&segment(session_id)),
        scripts = scripts(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_ids_stay_readable_in_urls() {
        assert_eq!(
            segment("a7a50e0b-d588-495e-8aed-9d40d2f73560"),
            "a7a50e0b-d588-495e-8aed-9d40d2f73560"
        );
        assert_eq!(segment("a/b c\"<"), "a%2Fb%20c%22%3C");
        let page = standalone_page("11111111-1111-4111-8111-111111111111");
        assert!(page.contains("data-ws=\"/websocket/11111111-1111-4111-8111-111111111111\""));
    }
}
