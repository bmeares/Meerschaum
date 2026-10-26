//! The CLI's `tracing` subscriber: debug output (`--debug`, the shell's `debug` toggle) and
//! `RUST_LOG`.
//!
//! Two independent layers share one registry:
//!
//! - **debug output**: while [`meerschaum_core::debug::enabled`] (the switch `--debug` and the
//!   shell toggle set at run time), the `DEBUG` and `INFO` events of the Meerschaum crates
//!   ([`dprint!`](meerschaum_core::dprint) and plain `tracing::debug!`) are printed on **stderr**
//!   the way v4's `dprint()` printed them:
//!
//!   ```text
//!    🐞 14:03:07.512 | meerschaum_connector_sql::instance:654
//!   SELECT ...
//!   ```
//!
//!   The header has the configured `formatting:debug` icon and style (`formatting:unicode` off
//!   gives `DEBUG`, `formatting:ansi` off or `NO_COLOR` no colors) and the progress line is
//!   suspended around each write. No `RUST_LOG` is needed, and nothing is formatted while the
//!   switch is off.
//! - **`RUST_LOG`**: when set, the usual `tracing_subscriber::fmt` output on stderr, filtered by
//!   it (as before; it does not depend on the switch).

use std::fmt::Write as _;
use std::io::Write as _;
use std::sync::{Arc, RwLock};

use tracing::field::{Field, Visit};
use tracing::subscriber::Interest;
use tracing::{Event, Level, Metadata, Subscriber};
use tracing_subscriber::Layer;
use tracing_subscriber::layer::{Context, Filter, SubscriberExt};

use crate::format::Formatter;
use crate::format::rich::style::{Style, get_style};

/// How the header is drawn (from the config, see [`configure`]).
#[derive(Debug, Clone)]
pub struct DprintStyle {
    /// `formatting:debug:<charset>:icon`.
    pub icon: String,
    /// `formatting:debug:ansi:rich:style` (`None`: no ANSI).
    pub style: Option<Style>,
}

impl Default for DprintStyle {
    /// The default config's look, before any config was read.
    fn default() -> DprintStyle {
        let no_color = std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty());
        DprintStyle {
            icon: if cfg!(windows) { "DEBUG" } else { "🐞" }.into(),
            style: get_style("cyan")
                .ok()
                .map(|s| if no_color { s.without_color() } else { s }),
        }
    }
}

impl DprintStyle {
    /// The style a formatter's settings and config give.
    pub fn from_formatter(f: &Formatter<'_>) -> DprintStyle {
        let icon = f.formatting_str(&["debug", f.charset(), "icon"]);
        let style = f.settings.ansi.then(|| {
            let name = f.formatting_str(&["debug", "ansi", "rich", "style"]);
            let style = get_style(&name).unwrap_or_else(|_| Style::null());
            if f.settings.no_color {
                style.without_color()
            } else {
                style
            }
        });
        DprintStyle { icon, style }
    }

    /// v4's `dprint` text for one message: ` <icon> HH:MM:SS.mmm | <module>:<line>` (colored),
    /// a newline, the message and a newline.
    pub fn render(
        &self,
        time: chrono::NaiveTime,
        module: &str,
        line: Option<u32>,
        message: &str,
    ) -> String {
        let mut premsg = String::from(" ");
        if !self.icon.is_empty() {
            premsg.push_str(&self.icon);
            premsg.push(' ');
        }
        premsg.push_str(&meerschaum_core::debug::header(time, module, line));
        if let Some(style) = self.style.as_ref().filter(|s| !s.is_null()) {
            premsg = style.render(&premsg);
        }
        let mut out = premsg;
        out.push('\n');
        out.push_str(message);
        if !out.ends_with('\n') {
            out.push('\n');
        }
        out
    }
}

static STYLE: RwLock<Option<DprintStyle>> = RwLock::new(None);

/// Draw the debug header with this formatter's settings from now on (called when `--debug` or
/// the shell toggle turns the switch on, once the config is loaded).
pub fn configure(f: &Formatter<'_>) {
    if let Ok(mut style) = STYLE.write() {
        *style = Some(DprintStyle::from_formatter(f));
    }
}

fn current_style() -> DprintStyle {
    STYLE
        .read()
        .ok()
        .and_then(|s| s.clone())
        .unwrap_or_default()
}

/// The message of an event, then its other fields as ` key=value`.
#[derive(Default)]
struct MessageVisitor {
    message: String,
    fields: String,
}

impl Visit for MessageVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            let _ = write!(self.message, "{value:?}");
        } else {
            let _ = write!(self.fields, " {}={value:?}", field.name());
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message.push_str(value);
        } else {
            let _ = write!(self.fields, " {}={value}", field.name());
        }
    }
}

/// The text of an event (message and fields), as the debug output and the Python sink print it.
pub fn event_message(event: &Event<'_>) -> String {
    let mut visitor = MessageVisitor::default();
    event.record(&mut visitor);
    let mut message = visitor.message;
    if !visitor.fields.is_empty() {
        if message.is_empty() {
            message.push_str(visitor.fields.trim_start());
        } else {
            message.push_str(&visitor.fields);
        }
    }
    message
}

/// Whether an event is debug output: a Meerschaum crate's `DEBUG` / `INFO` event.
pub fn is_debug_output(metadata: &Metadata<'_>) -> bool {
    matches!(*metadata.level(), Level::DEBUG | Level::INFO)
        && meerschaum_core::debug::is_meerschaum_target(metadata.target())
}

/// Lets debug events through while the switch is on. Interest is `sometimes` so the switch is
/// asked at every event (a cached `always` / `never` would ignore later toggles).
struct SwitchFilter;

impl<S> Filter<S> for SwitchFilter {
    fn enabled(&self, metadata: &Metadata<'_>, _: &Context<'_, S>) -> bool {
        meerschaum_core::debug::enabled() && is_debug_output(metadata)
    }

    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest {
        if is_debug_output(metadata) {
            Interest::sometimes()
        } else {
            Interest::never()
        }
    }

    fn max_level_hint(&self) -> Option<tracing::level_filters::LevelFilter> {
        Some(tracing::level_filters::LevelFilter::DEBUG)
    }
}

/// Where debug text goes.
pub type DebugWriter = Arc<dyn Fn(&str) + Send + Sync>;

/// Debug text held while a full-screen view owns the terminal (see [`hold_output`]).
struct Held {
    on: bool,
    messages: std::collections::VecDeque<String>,
    dropped: usize,
}

static HELD: std::sync::Mutex<Held> = std::sync::Mutex::new(Held {
    on: false,
    messages: std::collections::VecDeque::new(),
    dropped: 0,
});

/// How many messages a full-screen view holds back (the oldest are dropped beyond this).
const HELD_MAX: usize = 2000;

fn write_stderr(text: &str) {
    crate::format::progress::suspend();
    {
        let mut err = std::io::stderr().lock();
        let _ = err.write_all(text.as_bytes());
        let _ = err.flush();
    }
    crate::format::progress::resume(true);
}

/// Hold debug output back while a full-screen view (`watch run`, `watch jobs`) shows the
/// alternate screen in raw mode, where it would be drawn over the view; `hold_output(false)`
/// prints what was held (the last 2000 messages) once the main screen is back.
pub fn hold_output(on: bool) {
    let mut held = HELD.lock().unwrap_or_else(|e| e.into_inner());
    held.on = on;
    if on {
        return;
    }
    let dropped = std::mem::take(&mut held.dropped);
    let messages = std::mem::take(&mut held.messages);
    if dropped > 0 {
        write_stderr(&format!(
            "({dropped} earlier debug messages from the full-screen view were dropped.)\n"
        ));
    }
    for text in messages {
        write_stderr(&text);
    }
}

/// Write to the process's stderr, with the progress line out of the way (held back while a
/// full-screen view is shown, see [`hold_output`]).
fn stderr_writer() -> DebugWriter {
    Arc::new(|text: &str| {
        let mut held = HELD.lock().unwrap_or_else(|e| e.into_inner());
        if held.on {
            if held.messages.len() >= HELD_MAX {
                held.messages.pop_front();
                held.dropped += 1;
            }
            held.messages.push_back(text.to_string());
            return;
        }
        // Written under the lock so a view cannot enter the alternate screen mid-message.
        write_stderr(text);
    })
}

struct DprintLayer {
    write: DebugWriter,
}

impl<S: Subscriber> Layer<S> for DprintLayer {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        let metadata = event.metadata();
        let module = metadata.module_path().unwrap_or(metadata.target());
        let text = current_style().render(
            chrono::Utc::now().time(),
            module,
            metadata.line(),
            // A last line of defense: a URI password or a quoted secret in any message.
            &meerschaum_core::debug::redact_uris(&event_message(event)),
        );
        (self.write)(&text);
    }
}

/// The CLI's subscriber, writing debug output with `write` (the process's stderr by default).
pub fn subscriber_with_writer(write: Option<DebugWriter>) -> Box<dyn Subscriber + Send + Sync> {
    let rust_log = std::env::var_os("RUST_LOG").map(|_| {
        tracing_subscriber::fmt::layer()
            .with_writer(std::io::stderr)
            .with_filter(tracing_subscriber::EnvFilter::from_default_env())
    });
    let dprint = DprintLayer {
        write: write.unwrap_or_else(stderr_writer),
    }
    .with_filter(SwitchFilter);
    Box::new(tracing_subscriber::registry().with(rust_log).with(dprint))
}

/// The CLI's subscriber (see the module documentation).
pub fn subscriber() -> Box<dyn Subscriber + Send + Sync> {
    subscriber_with_writer(None)
}

/// Install [`subscriber`] as the process's global subscriber (once; nothing happens when one is
/// already installed: the Python extension routes to its own copy, see `meerschaum-py`'s
/// `logging.rs`).
pub fn install() {
    let _ = tracing::subscriber::set_global_default(subscriber());
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    fn time() -> chrono::NaiveTime {
        chrono::NaiveTime::from_hms_milli_opt(14, 3, 7, 512).unwrap()
    }

    #[test]
    fn render_plain() {
        let style = DprintStyle {
            icon: "DEBUG".into(),
            style: None,
        };
        assert_eq!(
            style.render(time(), "meerschaum_cli::dispatch", Some(9), "SELECT 1"),
            " DEBUG 14:03:07.512 | meerschaum_cli::dispatch:9\nSELECT 1\n"
        );
    }

    #[test]
    fn render_colored_header_only() {
        let style = DprintStyle {
            icon: "🐞".into(),
            style: get_style("cyan").ok(),
        };
        let text = style.render(time(), "m", Some(1), "msg\n");
        let (header, rest) = text.split_once('\n').unwrap();
        assert!(header.starts_with("\x1b["), "{header:?}");
        assert!(header.contains(" 🐞 14:03:07.512 | m:1"));
        assert_eq!(rest, "msg\n");
    }

    #[test]
    fn events_print_only_while_the_switch_is_on() {
        let out = Arc::new(Mutex::new(String::new()));
        let sink = Arc::clone(&out);
        let sub = subscriber_with_writer(Some(Arc::new(move |t: &str| {
            sink.lock().unwrap().push_str(t);
        })));
        tracing::subscriber::with_default(sub, || {
            meerschaum_core::dprint!("hidden");
            tracing::debug!("also hidden");
            {
                let _on = meerschaum_core::debug::scope(true);
                meerschaum_core::dprint!(rows = 3, "Fetched.");
                tracing::debug!("plain debug");
                tracing::warn!("a warning is not debug output");
                tracing::debug!(target: "sqlx::query", "dependency chatter");
            }
            meerschaum_core::dprint!("hidden again");
        });
        let out = out.lock().unwrap().clone();
        assert!(!out.contains("hidden"), "{out}");
        assert!(out.contains("Fetched. rows=3\n"), "{out}");
        assert!(out.contains("plain debug\n"), "{out}");
        assert!(out.contains("| meerschaum_cli::debug_log::tests:"), "{out}");
        assert!(!out.contains("warning"), "{out}");
        assert!(!out.contains("chatter"), "{out}");
    }
}
