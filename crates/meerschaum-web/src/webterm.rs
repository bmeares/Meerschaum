//! The web terminal in the console: the `/dash/webterm` page embeds xterm.js in the console
//! chrome, and `/webterm/{session_id}` (+ its websocket) is the standalone page v4 served. The PTY
//! bridge and its authentication live in `meerschaum_server::webterm`; this module gives it the
//! console's session store and the pages.
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
        .map(|name| format!("<script defer src=\"/dash/assets/{name}\"></script>"))
        .collect()
}

/// `/webterm/{session_id}`: the page v4's `termpage.html` was, on the vendored assets.
pub(crate) fn standalone_page(session_id: &str) -> String {
    format!(
        "<!DOCTYPE html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\
<meta name=\"referrer\" content=\"no-referrer\"><title>Meerschaum Shell</title>\
<link rel=\"stylesheet\" href=\"/dash/assets/xterm.css\">\
<link rel=\"stylesheet\" href=\"/dash/assets/webterm.css\"></head>\
<body class=\"mrsm-webterm-standalone\">\
<div class=\"mrsm-webterm-screen\" id=\"xterm-div\" data-mrsm-webterm data-mode=\"standalone\" data-ws=\"/websocket/{id}\"></div>\
{scripts}</body></html>",
        id = escape(&segment(session_id)),
        scripts = scripts(),
    )
}

fn key(name: &str, label: &str, title: &str, area: &str) -> String {
    format!(
        "<button type=\"button\" class=\"mrsm-webterm-key\" data-key=\"{name}\" title=\"{title}\" aria-label=\"{title}\"{area}>{label}</button>"
    )
}

/// The `/dash/webterm` page content: the terminal inside the console, with v4's extra key row
/// (ESC / CTRL / SHIFT / TAB and the arrows; CTRL and SHIFT are sticky) and its controls.
pub(crate) fn embedded_content(state: &WebState, session_id: &str) -> String {
    let id = escape(&segment(session_id));
    let mods = [
        ("esc", "ESC", "Escape"),
        ("ctrl", "CTRL", "Control (sticky: applies to the next key)"),
        ("shift", "SHIFT", "Shift (sticky: applies to the next key)"),
        ("tab", "TAB", "Tab"),
    ]
    .iter()
    .map(|(name, label, title)| key(name, label, title, ""))
    .collect::<String>();
    let arrows = [
        ("up", "\u{2191}", "Up arrow"),
        ("left", "\u{2190}", "Left arrow"),
        ("down", "\u{2193}", "Down arrow"),
        ("right", "\u{2192}", "Right arrow"),
    ]
    .iter()
    .map(|(name, label, title)| key(name, label, title, &format!(" style=\"grid-area:{name}\"")))
    .collect::<String>();
    let control = |action: &str, label: &str, title: &str| {
        format!(
            "<button type=\"button\" class=\"mrsm-webterm-control\" data-webterm-action=\"{action}\" title=\"{title}\" aria-label=\"{title}\">{label}</button>"
        )
    };
    let mut controls = control("refresh", "\u{27F3}", "Refresh terminal")
        + &control("fullscreen", "\u{26F6}", "Toggle fullscreen");
    if state.webterm_tmux {
        controls += &control("new-window", "<b>+</b>", "New terminal window");
    }
    format!(
        "<link rel=\"stylesheet\" href=\"/dash/assets/xterm.css\">\
<link rel=\"stylesheet\" href=\"/dash/assets/webterm.css\">\
<div class=\"mrsm-webterm\">\
<div class=\"mrsm-webterm-bar\">\
<div class=\"mrsm-webterm-keys\" role=\"toolbar\" aria-label=\"Extra keys\"><div class=\"mrsm-webterm-mods\">{mods}</div><div class=\"mrsm-webterm-arrows\">{arrows}</div></div>\
<div class=\"mrsm-webterm-controls\">{controls}</div></div>\
<div class=\"mrsm-webterm-screen\" data-mrsm-webterm data-mode=\"embedded\" data-ws=\"/websocket/{id}\" aria-label=\"Terminal\"></div>\
<p class=\"mrsm-webterm-note\"><a href=\"/webterm/{id}\">Open the terminal in its own page</a></p>\
</div>{scripts}",
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
