//! Full-screen terminal views (ratatui over crossterm): the `watch` action and the shell's
//! function-key shortcuts.
//!
//! - [`VIEWS`]: the view registry. Each view is a `watch` subaction (`watch run`, `watch jobs`)
//!   and may have a default shell key (F2, F3). Adding a view means one [`ViewDef`] entry, its
//!   subaction in [`crate::actions::CORE_ACTIONS`] (and [`crate::actions::V5_ONLY_SUBACTIONS`]) and
//!   its docstring in [`crate::action_docs`]; the unit tests check the three agree.
//! - [`app`]: the loop every view shares (alternate screen, raw mode, resize, `q` / Esc,
//!   `?` help, confirmations; the terminal is restored on return, on a panic and on Ctrl-C).
//! - [`theme`]: colors and glyphs from the formatting config (`NO_COLOR`, `formatting:ansi`,
//!   `formatting:unicode`).
//! - [`run_view`]: the run monitor (live Gantt, node table, event log, node details).
//! - [`jobs_view`]: the jobs monitor (job list, the selected job's log, start / stop / pause).
//! - [`watch`]: the `watch` action, which falls back to line output off a terminal.
//! - [`keys`]: the shell's key bindings (`shell:keybindings`).
//!
//! Every view needs a terminal: [`available`] says whether one can run here.

pub mod app;
pub mod jobs_view;
pub mod keys;
pub mod run_view;
pub mod theme;
pub mod watch;

use std::io::IsTerminal;
use std::sync::Arc;

use meerschaum_engine::EngineReader;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::native::{Instance, Session};
use crate::value::{ArgsMap, PyValue};

/// Where the run monitor reads a run's events from.
pub enum RunFeed {
    /// Events of a run in this process (from `Engine::subscribe()`). The events persisted before
    /// the receiver was created are read from the instance until they reach the broadcast ones
    /// (which are held back meanwhile and then applied in order), so the monitor may be opened
    /// in the middle of a run.
    Live(tokio::sync::broadcast::Receiver<meerschaum_engine::Event>),
    /// A run in another process: read its persisted events (`EngineReader`, `after_seq`) from
    /// the instance.
    Stored,
}

/// How the run monitor ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MonitorExit {
    /// The user quit (`q`, Esc, Ctrl-C).
    Quit,
    /// The user detached back to the inline view (`d`, only offered for a live feed).
    BackToInline,
}

/// One full-screen view: a `watch` subaction.
#[derive(Debug, Clone, Copy)]
pub struct ViewDef {
    /// The `watch` subaction (`runs`; `watch run` resolves to it like `show run`).
    pub name: &'static str,
    /// The action function name (`_watch_runs`).
    pub function: &'static str,
    /// What the view shows (completion and docs).
    pub summary: &'static str,
    /// The command a default shell key runs, and the key.
    pub shell_key: Option<(&'static str, &'static str)>,
    /// The action: full-screen on a terminal, line output otherwise.
    pub action: fn(&ArgsMap, &mut Runtime<'_>) -> ActionResult,
}

/// The view registry, in `watch` subaction order.
pub static VIEWS: &[ViewDef] = &[
    ViewDef {
        name: "jobs",
        function: "_watch_jobs",
        summary: "the jobs of an executor and the selected job's log",
        shell_key: Some(("f3", "watch jobs")),
        action: watch::watch_jobs,
    },
    ViewDef {
        name: "logs",
        function: "_watch_logs",
        summary: "jobs' logs as they are written (`show logs`)",
        shell_key: None,
        action: crate::native::jobs::show_logs,
    },
    ViewDef {
        name: "runs",
        function: "_watch_runs",
        summary: "an engine run: live Gantt chart, nodes and events",
        shell_key: Some(("f2", "watch run")),
        action: watch::watch_runs,
    },
];

/// The view whose `watch` function is `function`.
pub fn view_for_function(function: &str) -> Option<&'static ViewDef> {
    VIEWS.iter().find(|v| v.function == function)
}

/// Whether a full-screen view can run here: a TTY on stdin and stdout (the process runtime),
/// not `--nopretty`, ANSI allowed (`formatting:ansi`), not `MRSM_NONINTERACTIVE` and not a dumb
/// terminal. `NO_COLOR` keeps the views (monochrome).
pub fn available(rt: &Runtime<'_>, args: &ArgsMap) -> bool {
    if !rt.live || args.get("nopretty").is_some_and(PyValue::is_truthy) {
        return false;
    }
    if crate::env::noninteractive(rt.env) || (rt.env)("TERM").is_some_and(|t| t == "dumb") {
        return false;
    }
    let ansi = match (&rt.format, &rt.config) {
        (Some(format), _) => format.ansi,
        (None, Some(config)) => config
            .get(&["formatting", "ansi"])
            .is_none_or(|v| v.truthy()),
        (None, None) => true,
    };
    ansi && std::io::stdin().is_terminal() && std::io::stdout().is_terminal()
}

/// The engine's read API over `instance` (run history, events).
pub fn engine_reader(
    rt: &mut Runtime<'_>,
    session: &Session,
    instance: &Instance,
) -> Result<EngineReader, ActionError> {
    let bundle = crate::native::engine_registry(rt.config(), session, instance, &[], false)?;
    Ok(EngineReader::new(bundle.registry, bundle.config))
}

/// Run a view full-screen with the runtime's theme. Spinners are held while it is up.
pub fn show(rt: &mut Runtime<'_>, view: &mut dyn app::View) -> Result<app::Exit, ActionError> {
    let theme = theme::Theme::from_settings(&rt.settings());
    crate::format::progress::suspend();
    let result = app::run(view, &theme);
    crate::format::progress::resume(true);
    result.map_err(|e| ActionError::new("OSError", format!("Cannot draw the view: {e}")))
}

/// Open the full-screen run monitor; returns when the user quits or detaches.
pub fn run_monitor(
    rt: &mut Runtime<'_>,
    instance: &Instance,
    run_id: &str,
    feed: RunFeed,
) -> Result<MonitorExit, ActionError> {
    let session = crate::native::session(rt);
    let reader = Arc::new(engine_reader(rt, &session, instance)?);
    let source = match feed {
        RunFeed::Live(rx) => run_view::Source::live(
            rx,
            Some(run_view::StoredPoller::start(
                &session, reader, run_id, false,
            )),
        ),
        RunFeed::Stored => run_view::Source::Stored(run_view::StoredPoller::start(
            &session, reader, run_id, false,
        )),
    };
    let mut view = run_view::RunMonitor::new(run_id, &instance.keys(), source);
    Ok(match show(rt, &mut view)? {
        app::Exit::Quit => MonitorExit::Quit,
        app::Exit::Detach => MonitorExit::BackToInline,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::{CORE_ACTIONS, get_subactions, is_v5_only_subaction};

    #[test]
    fn registry_matches_the_watch_subactions_and_docs() {
        let subs = get_subactions(CORE_ACTIONS, "watch");
        assert_eq!(subs.len(), VIEWS.len());
        for view in VIEWS {
            let sub = subs
                .iter()
                .find(|s| s.name == view.name)
                .unwrap_or_else(|| panic!("watch {} is not a subaction", view.name));
            assert_eq!(sub.function, view.function);
            assert!(is_v5_only_subaction("watch", view.name));
            assert!(sub.doc().is_some(), "{} has no docstring", view.function);
            assert!(view_for_function(view.function).is_some());
        }
        assert!(crate::action_docs::docstring("watch").is_some());
    }

    #[test]
    fn unavailable_off_a_terminal_and_with_nopretty() {
        let mut out = Vec::new();
        let rt = Runtime::new(crate::types::DateContext::system(), 80, &mut out, |_| None);
        assert!(
            !available(&rt, &ArgsMap::new()),
            "tests have no live runtime"
        );
        let mut args = ArgsMap::new();
        args.insert("nopretty".into(), PyValue::Bool(true));
        assert!(!available(&rt, &args));
    }
}
