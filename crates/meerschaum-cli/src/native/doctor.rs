//! `mrsm doctor` (v5): one command that says what is wrong with an installation and how to fix it.
//!
//! Every check yields a [`Check`] (`group`, `name`, [`Status`], a one-line `detail` and, for a WARN
//! or FAIL, the exact `fix`). The run is read-only: it never installs, creates, migrates or
//! writes anything. Config writes (`write_missing`) are switched off for its duration, the plugin
//! manifest is read through the session like every command does (it was validated by the
//! dispatcher before the action started and is rebuilt only if a plugin source changed), jobs are
//! scanned without the status calls that unlink stale PID files, and a SQLite instance whose
//! database file does not exist yet is reported, not created.
//!
//! The decision logic of every check is a pure function over plain data (thresholds, version
//! comparisons, manifest facts, the probe's JSON) so it is unit-tested without a machine in a
//! given state; the functions that gather the data are thin.
//!
//! Layout: [`Status`] / [`Check`] / [`Report`] (the model and both renderings), the `judge_*`
//! functions (decisions), the `gather_*` functions (I/O), and [`doctor`] (the action).

mod decisions;
mod gather;
mod instance;
mod python;
use decisions::*;
pub use decisions::{
    ConfigFile, DEPRECATED_KEYS, DISK_FAIL_BYTES, DISK_WARN_BYTES, DirFact, DirRole, DiskFact,
    DriverFact, INODE_FAIL_PERCENT, INODE_WARN_PERCENT, JobFact, MIN_PYTHON, PluginFacts,
    ProcessState, ServiceFact, VenvFact, deprecated_keys, driver_users, exit_code, human_bytes,
    judge_api_version, judge_config, judge_dir, judge_disk, judge_driver, judge_extra, judge_jobs,
    judge_manifest, judge_plugins, judge_service, judge_unique_key, judge_venvs, parse_major_minor,
    parse_services, plugin_facts, split_env_vars, unique_key_check_name, versions_match,
};
pub(crate) use gather::probe_python;
use gather::*;
pub use gather::{flavor_of, sqlite_path};
use instance::*;
pub use instance::{DuckdbGate, judge_duckdb_gate};
use python::*;
pub use python::{
    CopyProbe, EXTRA_MODULES, ModuleProbe, PROBE_MODULES, PackageProbe, PyProbe, judge_interpreter,
    judge_package, parse_probe,
};

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_connector_sql::instance::unique_keys::{
    Duplicate, KeyStatus, PIPES_KEY, PLUGINS_KEY, UNIQUE_KEYS, UniqueKey,
};
use meerschaum_connector_sql::{Flavor, SqlConnector, driver::DuckdbHostDiagnostic};
use meerschaum_core::SuccessTuple;
use meerschaum_core::config::{Config, GetOptions};
use meerschaum_core::value::{Map, Value};
use meerschaum_worker::manifest::Manifest;
use serde_json::{Value as Json, json};

use crate::builtin::ActionResult;
use crate::dispatch::{CommandSpec, Runtime};
use crate::format::rich::console::{Overflow, Renderable};
use crate::format::rich::table::{Column, HEAVY_HEAD, Table};
use crate::format::rich::text::Text;
use crate::value::ArgsMap;

use super::flag;

/// The outcome of one check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Status {
    /// Nothing to do.
    Pass,
    /// Works, but something deserves attention.
    Warn,
    /// Something is broken.
    Fail,
    /// Not applicable here (or not checkable): the detail says why.
    Skip,
}

impl Status {
    /// The word printed and serialized (`PASS`, `WARN`, `FAIL`, `SKIP`).
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Pass => "PASS",
            Status::Warn => "WARN",
            Status::Fail => "FAIL",
            Status::Skip => "SKIP",
        }
    }

    fn style(self) -> &'static str {
        match self {
            Status::Pass => "bold green",
            Status::Warn => "bold yellow",
            Status::Fail => "bold red",
            Status::Skip => "dim",
        }
    }
}

/// One line of the report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Check {
    /// The group (`install`, `paths`, `config`, `instance`, `drivers`, `extras`, `plugins`,
    /// `jobs`, `stack`, `extensions`).
    pub group: &'static str,
    /// The check's name within its group.
    pub name: String,
    /// The outcome.
    pub status: Status,
    /// One line of what was found.
    pub detail: String,
    /// For a WARN or FAIL: the command or setting that fixes it.
    pub fix: Option<String>,
    /// How long the check took (shown with `--debug`).
    pub millis: Option<u128>,
}

impl Check {
    fn new(group: &'static str, name: impl Into<String>, status: Status, detail: String) -> Check {
        Check {
            group,
            name: name.into(),
            status,
            detail,
            fix: None,
            millis: None,
        }
    }

    /// A passing check.
    pub fn pass(group: &'static str, name: impl Into<String>, detail: impl Into<String>) -> Check {
        Check::new(group, name, Status::Pass, detail.into())
    }

    /// A check that does not apply.
    pub fn skip(group: &'static str, name: impl Into<String>, detail: impl Into<String>) -> Check {
        Check::new(group, name, Status::Skip, detail.into())
    }

    /// A warning with its fix.
    pub fn warn(
        group: &'static str,
        name: impl Into<String>,
        detail: impl Into<String>,
        fix: impl Into<String>,
    ) -> Check {
        Check::new(group, name, Status::Warn, detail.into()).with_fix(fix)
    }

    /// A failure with its fix.
    pub fn fail(
        group: &'static str,
        name: impl Into<String>,
        detail: impl Into<String>,
        fix: impl Into<String>,
    ) -> Check {
        Check::new(group, name, Status::Fail, detail.into()).with_fix(fix)
    }

    fn with_fix(mut self, fix: impl Into<String>) -> Check {
        self.fix = Some(fix.into());
        self
    }

    /// `(status, detail, fix)` as one check of `group`/`name`.
    pub fn from_verdict(group: &'static str, name: impl Into<String>, v: Verdict) -> Check {
        Check {
            group,
            name: name.into(),
            status: v.status,
            detail: v.detail,
            fix: v.fix,
            millis: None,
        }
    }
}

/// What a decision function returns: the pieces of a [`Check`] without its identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verdict {
    /// The outcome.
    pub status: Status,
    /// What was found.
    pub detail: String,
    /// The fix (WARN and FAIL).
    pub fix: Option<String>,
}

impl Verdict {
    fn pass(detail: impl Into<String>) -> Verdict {
        Verdict {
            status: Status::Pass,
            detail: detail.into(),
            fix: None,
        }
    }

    fn skip(detail: impl Into<String>) -> Verdict {
        Verdict {
            status: Status::Skip,
            detail: detail.into(),
            fix: None,
        }
    }

    fn warn(detail: impl Into<String>, fix: impl Into<String>) -> Verdict {
        Verdict {
            status: Status::Warn,
            detail: detail.into(),
            fix: Some(fix.into()),
        }
    }

    fn fail(detail: impl Into<String>, fix: impl Into<String>) -> Verdict {
        Verdict {
            status: Status::Fail,
            detail: detail.into(),
            fix: Some(fix.into()),
        }
    }
}

/// The groups in print order.
pub const GROUPS: &[&str] = &[
    "install",
    "paths",
    "config",
    "instance",
    "drivers",
    "extras",
    "plugins",
    "jobs",
    "stack",
    "extensions",
];

/// Every check of one run.
#[derive(Debug, Clone, Default)]
pub struct Report {
    /// The checks, in group order (see [`GROUPS`]).
    pub checks: Vec<Check>,
}

/// The counts of a [`Report`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Summary {
    /// PASS count.
    pub pass: usize,
    /// WARN count.
    pub warn: usize,
    /// FAIL count.
    pub fail: usize,
    /// SKIP count.
    pub skip: usize,
}

impl Summary {
    /// `3 passed, 1 warning, 0 failed, 2 skipped`.
    pub fn text(&self) -> String {
        format!(
            "{} passed, {} warning{}, {} failed, {} skipped",
            self.pass,
            self.warn,
            super::plural(self.warn),
            self.fail,
            self.skip
        )
    }
}

impl Report {
    /// Add checks, then keep the report sorted by group (stable within a group).
    pub fn extend(&mut self, checks: impl IntoIterator<Item = Check>) {
        self.checks.extend(checks);
        self.checks.sort_by_key(|c| {
            GROUPS
                .iter()
                .position(|g| *g == c.group)
                .unwrap_or(GROUPS.len())
        });
    }

    /// The counts.
    pub fn summary(&self) -> Summary {
        let mut s = Summary::default();
        for c in &self.checks {
            match c.status {
                Status::Pass => s.pass += 1,
                Status::Warn => s.warn += 1,
                Status::Fail => s.fail += 1,
                Status::Skip => s.skip += 1,
            }
        }
        s
    }

    /// The names (`group: name`) of the failed checks.
    pub fn failed(&self) -> Vec<String> {
        self.checks
            .iter()
            .filter(|c| c.status == Status::Fail)
            .map(|c| format!("{}: {}", c.group, c.name))
            .collect()
    }

    /// The `SuccessTuple` of the run: success unless a check FAILed.
    pub fn tuple(&self) -> SuccessTuple {
        let summary = self.summary();
        if summary.fail == 0 {
            SuccessTuple::ok(format!("No problems found ({}).", summary.text()))
        } else {
            SuccessTuple::fail(format!(
                "{} check{} failed ({}); {}.",
                summary.fail,
                super::plural(summary.fail),
                self.failed().join(", "),
                summary.text()
            ))
        }
    }

    /// The `--nopretty` document: stable keys `version`, `summary`, `checks`
    /// (`group`, `name`, `status`, `detail`, `fix`); `duration_ms` is added to a check only when
    /// it was timed (`--debug`).
    pub fn to_json(&self, version: &str) -> Json {
        let summary = self.summary();
        let checks: Vec<Json> = self
            .checks
            .iter()
            .map(|c| {
                let mut check = json!({
                    "group": c.group,
                    "name": c.name,
                    "status": c.status.as_str(),
                    "detail": c.detail,
                    "fix": c.fix,
                });
                if let (Some(ms), Some(map)) = (c.millis, check.as_object_mut()) {
                    map.insert("duration_ms".into(), json!(ms as u64));
                }
                check
            })
            .collect();
        json!({
            "version": version,
            "summary": {
                "ok": summary.fail == 0,
                "pass": summary.pass,
                "warn": summary.warn,
                "fail": summary.fail,
                "skip": summary.skip,
                "text": summary.text(),
            },
            "checks": checks,
        })
    }
}

/// The instance the run checks: the SQL connector (when it is one) for the groups that follow.
pub struct InstanceContext {
    /// The instance keys (`sql:main`).
    pub keys: String,
    /// The SQL connector, when the instance is a reachable `sql:` one (built lazily; the doctor
    /// has already run one read-only query through it).
    pub sql: Option<Arc<SqlConnector>>,
    /// The session (runtime) the connector belongs to.
    pub session: super::Session,
    /// The instance is the Docker stack's database (`extensions::is_stack_instance`).
    pub stack_db: bool,
    /// The stack config turns TimescaleDB's telemetry off (`extensions::stack_telemetry_off`).
    pub stack_telemetry_off: bool,
}

// ---------------------------------------------------------------------------------------------
// The action.
// ---------------------------------------------------------------------------------------------

/// `doctor`: run every check and print the report.
pub fn doctor(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let debug = flag(args, "debug");
    let nopretty = flag(args, "nopretty");
    // Read-only: no `write_missing` config file writes while the checks run (a registry that
    // already exists is switched off for the run and restored, even if a check panics: in-process
    // callers such as `mrsm.entry` keep their config registry).
    let was_writable = rt.config_read_only().with_state(|s| {
        let was = s.allow_writes();
        s.set_allow_writes(false);
        was
    });
    let checked = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| run_checks(args, rt)));
    rt.config().with_state(|s| s.set_allow_writes(was_writable));
    let mut report = match checked {
        Ok(report) => report,
        Err(panic) => std::panic::resume_unwind(panic),
    };
    if !debug {
        // Timings are a `--debug` feature: keep the default document small and reproducible.
        for check in &mut report.checks {
            check.millis = None;
        }
    }
    report.checks.push(Check::pass(
        "installation",
        "upgrade owner",
        super::self_upgrade::current_installation(rt).to_string(),
    ));
    render(rt, &report, nopretty, debug);
    Ok(report.tuple())
}

fn run_checks(args: &ArgsMap, rt: &mut Runtime<'_>) -> Report {
    let env = rt.env;
    let vars = (rt.vars)();
    let session = super::session(rt);
    let instance_keys = super::instance_keys(args, rt);
    let pip = super::pip::PipEnv::new(rt);
    let manifest = crate::python::manifest(rt);
    let duckdb_host = meerschaum_connector_sql::driver::duckdb_host_diagnostic();
    // Python prospective/selected/refused modes must not dlopen a separate native library.
    let driver_rows = super::drivers::rows_with_duckdb(
        rt,
        duckdb_host
            .as_ref()
            .is_none_or(|host| matches!(host, DuckdbHostDiagnostic::Native)),
    );
    let stack_db = super::extensions::is_stack_instance(rt, &instance_keys);
    let stack_telemetry_off = super::extensions::stack_telemetry_off(rt);
    let config: &Config = rt.config();
    let paths = config.paths();
    let python_plugins = meerschaum_worker::manifest::discover_plugins(&paths.plugins_dirs).len();
    // The doctor never validates or rebuilds the manifest (that imports plugin code in a worker
    // and writes the cache): it reads the cached one and says when it is stale or missing.
    let cached = meerschaum_worker::manifest::Manifest::read(
        &meerschaum_worker::manifest::manifest_path(&paths.root_dir),
    )
    .ok()
    .flatten();
    let on_disk = meerschaum_worker::manifest::discover_plugins(&paths.plugins_dirs);
    let (stale, changed) = cached
        .as_ref()
        .map(|m| meerschaum_worker::manifest::stale_plugins(m, &paths.plugins_dirs))
        .unwrap_or((false, Vec::new()));
    let listed = manifest.as_ref().map(|m| m.plugins.len()).unwrap_or(0);
    let manifest_verdict = judge_manifest(cached.is_some(), stale, &changed, on_disk.len(), listed);
    let facts = manifest.as_ref().ok().map(|m| {
        let mut f = plugin_facts(m);
        for source in &on_disk {
            if !f.names.contains(&source.name) {
                f.names.push(source.name.clone());
                f.unbuilt.push(source.name.clone());
            }
        }
        f
    });
    let duckdb_gate = DuckdbGate {
        problem: driver_rows
            .iter()
            .find(|r| r.name == "duckdb")
            .and_then(|r| match r.source.as_str() {
                "missing" => Some("not installed".to_string()),
                "error" => Some(format!("not loadable: {}", r.path)),
                _ => None,
            }),
        auto_install: pip.auto_install,
        host: duckdb_host,
    };
    let explicit_instance = super::string(args, "mrsm_instance").is_some();
    // The types `apply_environment_uris` accepts besides the built-in ones: the loaded
    // manifest's, the cached manifests' and the `@make_connector` classes of the plugin sources.
    let custom_types: Vec<String> = manifest
        .as_ref()
        .map(|m| {
            m.connector_types
                .keys()
                .cloned()
                .collect::<BTreeSet<String>>()
        })
        .unwrap_or_default()
        .into_iter()
        .chain(
            meerschaum_core::config::connector_types::plugin_connector_types(
                &paths.root_dir,
                &paths.plugins_dirs,
            ),
        )
        .collect::<BTreeSet<String>>()
        .into_iter()
        .collect();
    let jobs_dir = paths.path("DAEMON_RESOURCES_PATH");
    let venvs_dir = paths
        .get("VIRTENV_RESOURCES_PATH")
        .map(|p| PathBuf::from(p.as_posix()))
        .unwrap_or_else(|| paths.root_dir.join("venvs"));
    let path_env = env("PATH");

    let mut report = Report::default();
    std::thread::scope(|scope| {
        let probe_handle = scope.spawn(|| {
            let start = Instant::now();
            (probe_python(&pip.python), start.elapsed().as_millis())
        });
        let instance_handle = scope.spawn(|| {
            let start = Instant::now();
            let (mut checks, sql) = gather_instance(&session, config, &instance_keys, &duckdb_gate);
            let ms = start.elapsed().as_millis();
            for c in &mut checks {
                c.millis.get_or_insert(ms);
            }
            (checks, sql)
        });
        let stack_handle = scope.spawn(|| timed(|| gather_stack(config, path_env.clone())));

        report.extend(timed(|| path_checks(config)));
        report.extend(timed(|| team_sandbox_checks(config)));
        report.extend(timed(|| {
            config_checks(
                config,
                &vars,
                env,
                &instance_keys,
                explicit_instance,
                &custom_types,
            )
        }));
        let connectors = sql_connectors(config);
        let instance_label = instance_keys
            .strip_prefix("sql:")
            .map(str::to_string)
            .or_else(|| (instance_keys == "sql").then(|| "main".to_string()));
        report.extend(timed(|| {
            driver_checks(
                &driver_rows,
                &connectors,
                instance_label.as_deref(),
                pip.auto_install,
                duckdb_gate.host.as_ref(),
            )
        }));
        report.extend(timed(|| job_checks(&jobs_dir)));
        let venvs = venv_facts(&venvs_dir);

        let (probe, probe_ms) = probe_handle
            .join()
            .unwrap_or_else(|_| (Err("the probe thread panicked".to_string()), 0));
        let mut install = install_checks(env, &pip.python, &probe, python_plugins);
        for c in &mut install {
            c.millis.get_or_insert(probe_ms);
        }
        report.extend(install);
        report.extend(extras_checks(&probe, facts.as_ref(), &pip.python));
        report.extend(plugin_checks(
            &manifest,
            facts.as_ref(),
            &manifest_verdict,
            &venvs,
            &probe,
        ));

        let (instance_checks, sql) = instance_handle.join().unwrap_or_else(|_| {
            (
                vec![Check::fail(
                    "instance",
                    "connection",
                    "the instance probe panicked",
                    "re-run with --debug",
                )],
                None,
            )
        });
        report.extend(instance_checks);
        let ctx = InstanceContext {
            keys: instance_keys.clone(),
            sql,
            session: session.clone(),
            stack_db,
            stack_telemetry_off,
        };
        report.extend(timed(|| extension_checks(&ctx)));
        report.extend(timed(|| unique_key_checks(&ctx)));
        report.extend(stack_handle.join().unwrap_or_default());
    });
    report
}

/// Print the report: one JSON document with `--nopretty`, else a grouped table.
fn render(rt: &mut Runtime<'_>, report: &Report, nopretty: bool, debug: bool) {
    if nopretty {
        let text = serde_json::to_string_pretty(&report.to_json(crate::VERSION))
            .unwrap_or_else(|e| format!("{{\"error\": \"{e}\"}}"));
        rt.print(&format!("{text}\n"));
        return;
    }
    let text = rt.with_formatter(|f| {
        let mut table = Table::new();
        table.box_chars = Some(HEAVY_HEAD);
        table.header_style = crate::format::rich::style::StyleRef::Name("bold".into());
        for header in ["Group", "Check", "Status", "Detail"] {
            table.add_column(Column {
                header: Text::plain_text(header),
                overflow: Overflow::Fold,
                ..Column::default()
            });
        }
        let cell = |t: Text| Some(Box::new(t) as Box<dyn Renderable>);
        let mut last_group = "";
        for check in &report.checks {
            let group = if check.group == last_group {
                ""
            } else {
                check.group
            };
            last_group = check.group;
            let mut detail = Text::plain_text(&check.detail);
            if debug && let Some(ms) = check.millis {
                detail.append_str(&format!(" ({ms} ms)"), "dim");
            }
            if let Some(fix) = &check.fix {
                detail.append_str("\nFix: ", "bold");
                detail.append_str(fix, "cyan");
            }
            table.add_row(vec![
                cell(Text::new(group, "bold")),
                cell(Text::plain_text(&check.name)),
                cell(Text::new(check.status.as_str(), check.status.style())),
                cell(detail),
            ]);
        }
        f.console().print(&table)
    });
    rt.print(&text);
}

#[cfg(test)]
mod tests;
