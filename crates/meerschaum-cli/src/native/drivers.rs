//! `install driver`, `uninstall driver` and `show drivers` (v5): the native client libraries a
//! database flavor loads at run time.
//!
//! DuckDB is not compiled into `mrsm`. Its own shared library (`libduckdb`) is found through
//! `MRSM_DUCKDB_LIB`, the `<root>/drivers` cache or the system loader path, and downloaded (with a
//! pinned SHA-256) on first use unless `MRSM_NO_AUTO_INSTALL` is set; `install driver duckdb`
//! does that download on purpose. Oracle's `libclntsh` is Oracle's own: it is only reported.

use std::path::Path;
use std::sync::{Arc, Mutex};

use meerschaum_connector_sql::client_libs;
use meerschaum_connector_sql::duckdb_lib::{self, Reporter, Settings};
use meerschaum_core::SuccessTuple;

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::progress::{LineStyle, Progress, ProgressSink};
use crate::format::rich::console::{Overflow, Renderable};
use crate::format::rich::table::{Column, HEAVY_HEAD, Table};
use crate::value::ArgsMap;

use super::{flag, info, words, yes_no};

/// The drivers `install driver` and `show drivers` know.
const DRIVERS: &[&str] = &["duckdb", "oracle"];

/// The DuckDB settings of this runtime: `MRSM_DUCKDB_LIB` and `MRSM_NO_AUTO_INSTALL` from its
/// environment, the cache under its root directory. The root is also made the process's DuckDB
/// root, so a connection opened later in this process (the implicit first-use install) looks in
/// the same place as `install driver` and `show drivers`.
fn duckdb_settings(rt: &mut Runtime<'_>) -> Settings {
    let mut settings = Settings::from_env(&|name| (rt.env)(name));
    let root = rt.config().paths().root_dir.clone();
    duckdb_lib::set_root_override(Some(root.clone()));
    settings.root = Some(root);
    settings
}

/// Draws the install on the CLI progress line.
struct CliReporter {
    progress: Progress,
}

impl Reporter for CliReporter {
    fn start(&self, version: &str, bytes: u64) {
        self.progress.set_message(format!(
            "Installing the DuckDB driver (v{version}, {} MB)",
            (bytes + 500_000) / 1_000_000
        ));
    }

    fn progress(&self, done: u64, total: u64) {
        self.progress.set_total((total / 1_000_000) as usize);
        self.progress.set_done((done / 1_000_000) as usize);
    }

    fn finish(&self, _result: Result<&Path, &str>) {}
}

/// The reporter of an implicit (first-use) install: it starts its progress line only when the
/// download begins, on whichever thread runs it.
struct LazyReporter {
    sink: ProgressSink,
    style: LineStyle,
    lines: bool,
    progress: Mutex<Option<Progress>>,
}

impl LazyReporter {
    fn lock(&self) -> std::sync::MutexGuard<'_, Option<Progress>> {
        self.progress.lock().unwrap_or_else(|e| e.into_inner())
    }
}

impl Reporter for LazyReporter {
    fn start(&self, version: &str, bytes: u64) {
        *self.lock() = Some(Progress::begin(
            self.sink.clone(),
            self.style,
            self.lines,
            format!(
                "Installing the DuckDB driver (v{version}, {} MB)",
                (bytes + 500_000) / 1_000_000
            ),
        ));
    }

    fn progress(&self, done: u64, total: u64) {
        if let Some(progress) = self.lock().as_ref() {
            progress.set_total((total / 1_000_000) as usize);
            progress.set_done((done / 1_000_000) as usize);
        }
    }

    fn finish(&self, _result: Result<&Path, &str>) {
        *self.lock() = None;
    }
}

/// Route the first-use download of a driver library (DuckDB, started by whichever action first
/// connects to a `duckdb` instance) through the CLI progress line when feedback is on; otherwise
/// the library's plain stderr lines stay.
pub fn register_install_reporter(args: &ArgsMap, rt: &mut Runtime<'_>) {
    let reporter: Option<Arc<dyn Reporter>> =
        Progress::context(rt, flag(args, "nopretty"), flag(args, "debug")).map(
            |(sink, style, lines)| {
                Arc::new(LazyReporter {
                    sink,
                    style,
                    lines,
                    progress: Mutex::new(None),
                }) as Arc<dyn Reporter>
            },
        );
    duckdb_lib::set_reporter(reporter);
}

fn unknown_driver(name: &str) -> SuccessTuple {
    SuccessTuple::fail(format!(
        "Unknown driver '{name}'. Choose one: {}.",
        DRIVERS.join(", ")
    ))
}

/// `install driver duckdb [--force]`: download and verify DuckDB's shared library into
/// `<root>/drivers`.
pub fn install_drivers(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let names = words(args, "action");
    if names.is_empty() {
        return Ok(SuccessTuple::fail(format!(
            "No driver to install. Choose one: {}.",
            DRIVERS.join(", ")
        )));
    }
    let force = flag(args, "force");
    let mut messages = Vec::new();
    for name in &names {
        match name.to_lowercase().as_str() {
            "duckdb" => {
                let settings = duckdb_settings(rt);
                let mb = duckdb_lib::download_megabytes().unwrap_or_default();
                let reporter = CliReporter {
                    progress: Progress::start(
                        rt,
                        args,
                        format!(
                            "Installing the DuckDB driver (v{}, {mb} MB)",
                            duckdb_lib::VERSION
                        ),
                    ),
                };
                let session = super::session(rt);
                let result = session.block_on(duckdb_lib::install(&settings, force, &reporter));
                drop(reporter);
                match result {
                    Ok(path) => messages.push(format!(
                        "The DuckDB driver (v{}) is installed at '{}'.",
                        duckdb_lib::VERSION,
                        path.display()
                    )),
                    Err(e) => return Ok(SuccessTuple::fail(e.to_string())),
                }
            }
            "oracle" => {
                return Ok(SuccessTuple::fail(
                    "The Oracle driver loads Oracle's own client library (`libclntsh`), which \
                     Meerschaum cannot download. Install Oracle Instant Client from Oracle and \
                     put it on the library path (`LD_LIBRARY_PATH`, `DYLD_LIBRARY_PATH` or \
                     `PATH`); `mrsm show drivers` reports whether it is found.",
                ));
            }
            other => return Ok(unknown_driver(other)),
        }
    }
    Ok(SuccessTuple::ok(messages.join("\n")))
}

/// `uninstall driver duckdb`: remove the cached DuckDB library (`<root>/drivers/duckdb`).
pub fn uninstall_drivers(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let names = words(args, "action");
    if names.is_empty() {
        return Ok(SuccessTuple::fail(format!(
            "No driver to uninstall. Choose one: {}.",
            DRIVERS.join(", ")
        )));
    }
    let yes = flag(args, "yes") || flag(args, "force");
    let mut messages = Vec::new();
    for name in &names {
        match name.to_lowercase().as_str() {
            "duckdb" => {
                let settings = duckdb_settings(rt);
                if !yes && flag(args, "noask") {
                    return Ok(SuccessTuple::fail(
                        "Skipping uninstallation. Add `-y` or `-f` to agree to the uninstall prompt.",
                    ));
                }
                let question = "Remove the downloaded DuckDB driver?";
                if !yes && !yes_no(rt, question, "y", yes, flag(args, "noask"))? {
                    return Ok(SuccessTuple::fail("Nothing was uninstalled."));
                }
                match duckdb_lib::uninstall(&settings) {
                    Ok(true) => messages.push("Removed the DuckDB driver.".to_string()),
                    Ok(false) => messages.push("The DuckDB driver is not installed.".to_string()),
                    Err(e) => return Ok(SuccessTuple::fail(e.to_string())),
                }
            }
            "oracle" => {
                return Ok(SuccessTuple::fail(
                    "The Oracle client library (`libclntsh`) is Oracle's own and was not \
                     installed by Meerschaum; remove it with the tool you used to install it.",
                ));
            }
            other => return Ok(unknown_driver(other)),
        }
    }
    Ok(SuccessTuple::ok(messages.join("\n")))
}

/// One row of `show drivers`.
struct DriverRow {
    name: &'static str,
    version: String,
    source: String,
    path: String,
}

fn rows(rt: &mut Runtime<'_>) -> Vec<DriverRow> {
    let settings = duckdb_settings(rt);
    let status = duckdb_lib::status(&settings);
    let duckdb = match (&status.source, &status.error) {
        (Some(source), _) => DriverRow {
            name: "duckdb",
            // A library on the system path is reported by path and not loaded (no version).
            version: status
                .version
                .clone()
                .unwrap_or_else(|| "(found, not loaded)".into()),
            source: source.as_str().to_string(),
            path: status.path.clone().unwrap_or_default(),
        },
        (None, Some(error)) => DriverRow {
            name: "duckdb",
            version: String::new(),
            source: "error".into(),
            path: error.clone(),
        },
        (None, None) => DriverRow {
            name: "duckdb",
            version: format!("v{} (on first use)", duckdb_lib::VERSION),
            source: "missing".into(),
            path: format!(
                "`mrsm install driver duckdb` downloads it ({} MB)",
                duckdb_lib::download_megabytes().unwrap_or_default()
            ),
        },
    };
    let oracle = match client_libs::oracle_client_library() {
        Some(lib) => DriverRow {
            name: "oracle",
            version: "(found, not loaded)".into(),
            source: "system".into(),
            path: lib.display().to_string(),
        },
        None => DriverRow {
            name: "oracle",
            version: String::new(),
            source: "missing".into(),
            path: format!(
                "{} not found (install Oracle Instant Client)",
                client_libs::oracle_client_candidates()[0]
            ),
        },
    };
    vec![duckdb, oracle]
}

/// `show drivers [names]`: each driver's library version, where it comes from (`env`, `cache`,
/// `system` or `missing`) and its path.
pub fn show_drivers(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let wanted = words(args, "action");
    if let Some(unknown) = wanted
        .iter()
        .find(|w| !DRIVERS.contains(&w.to_lowercase().as_str()))
    {
        return Ok(unknown_driver(unknown));
    }
    let mut rows = rows(rt);
    if !wanted.is_empty() {
        rows.retain(|r| wanted.iter().any(|w| w.eq_ignore_ascii_case(r.name)));
    }
    if flag(args, "nopretty") {
        let mut out = String::new();
        for row in &rows {
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\n",
                row.name, row.version, row.source, row.path
            ));
        }
        rt.print(&out);
        return Ok(SuccessTuple::ok("Success"));
    }
    let text =
        rt.with_formatter(|f| {
            let mut table = Table::new();
            table.box_chars = Some(HEAVY_HEAD);
            table.show_lines = true;
            table.header_style = crate::format::rich::style::StyleRef::Name("bold".into());
            for header in ["Driver", "Version", "Source", "Path"] {
                table.add_column(Column {
                    header: crate::format::rich::text::Text::plain_text(header),
                    overflow: Overflow::Fold,
                    ..Column::default()
                });
            }
            for row in &rows {
                let cell = |s: &str| {
                    Some(Box::new(crate::format::rich::text::Text::plain_text(s))
                        as Box<dyn Renderable>)
                };
                table.add_row(vec![
                    cell(row.name),
                    cell(&row.version),
                    cell(&row.source),
                    cell(&row.path),
                ]);
            }
            f.console().print(&table)
        });
    rt.print(&text);
    info(
        rt,
        "DuckDB loads its own shared library at run time: set `MRSM_DUCKDB_LIB` to use a copy you \
         provide (air-gapped hosts) or run `mrsm install driver duckdb`.",
    );
    Ok(SuccessTuple::ok("Success"))
}
