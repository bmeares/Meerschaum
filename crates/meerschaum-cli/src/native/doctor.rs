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

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use meerschaum_connector_sql::instance::unique_keys::{
    Duplicate, KeyStatus, PIPES_KEY, UNIQUE_KEYS, UniqueKey,
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

/// EXTENSIONS HOOK (agent K2, wave 17c): the PostgreSQL extensions check of group `extensions`
/// goes here. It is called with the instance of the run (`ctx.sql` is `Some` only when the
/// instance is a reachable `sql:` connector; otherwise return one `Check::skip("extensions", ...)`).
/// Run async primitives with `ctx.session.block_on(..)`, keep the call short (it runs inside the
/// doctor's time budget) and never write to the database.
fn extension_checks(ctx: &InstanceContext) -> Vec<Check> {
    let Some(sql) = &ctx.sql else {
        return vec![Check::skip(
            "extensions",
            "postgresql",
            "the instance is not a reachable SQL instance",
        )];
    };
    // One read-only query on its own short-lived connection, bounded.
    let timeout = Duration::from_secs(3);
    let status = ctx
        .session
        .block_on(async {
            tokio::time::timeout(
                timeout + Duration::from_millis(500),
                meerschaum_connector_sql::extensions::status(sql.spec(), None, timeout),
            )
            .await
        })
        .map_err(|_| "the database did not answer in time".to_string())
        .and_then(|r| r.map_err(|e| e.to_string()));
    super::extensions::doctor_checks(&status, &ctx.keys, ctx.stack_db, ctx.stack_telemetry_off)
}

/// The unique keys of the instance tables (5.0: pipe keys, usernames), read-only.
fn unique_key_checks(ctx: &InstanceContext) -> Vec<Check> {
    let Some(sql) = &ctx.sql else {
        return vec![Check::skip(
            "instance",
            "unique keys",
            "the instance is not a reachable SQL instance",
        )];
    };
    UNIQUE_KEYS
        .iter()
        .map(|key| {
            let status = ctx.session.block_on(async {
                tokio::time::timeout(PROBE_TIMEOUT, sql.unique_key_status(key))
                    .await
                    .map_err(|_| "the database did not answer in time".to_string())
                    .and_then(|r| r.map_err(|e| e.to_string()))
            });
            let table = sql.instance_table_name(key.table);
            Check::from_verdict(
                "instance",
                unique_key_check_name(key),
                judge_unique_key(key, &status, &table),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------------------------
// Decisions: pure functions over plain data (unit-tested).
// ---------------------------------------------------------------------------------------------

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

/// Free space under which a filesystem is a WARN / a FAIL.
pub const DISK_WARN_BYTES: u64 = GIB;
/// See [`DISK_WARN_BYTES`].
pub const DISK_FAIL_BYTES: u64 = 64 * MIB;
/// Free inodes (percent) under which a filesystem is a WARN / a FAIL.
pub const INODE_WARN_PERCENT: f64 = 5.0;
/// See [`INODE_WARN_PERCENT`].
pub const INODE_FAIL_PERCENT: f64 = 1.0;
/// The oldest Python the `meerschaum` package supports.
pub const MIN_PYTHON: (u32, u32) = (3, 11);

/// `1.5 GB` (binary units, like `df -h`).
pub fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = n as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{n} B")
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Whether two version strings name the same release (`5.0.0-rc.1` is `5.0.0rc1`, PEP 440);
/// strings that do not parse compare as text.
pub fn versions_match(a: &str, b: &str) -> bool {
    match (
        meerschaum_core::version::parse_version(a.trim()),
        meerschaum_core::version::parse_version(b.trim()),
    ) {
        (Ok(x), Ok(y)) => x == y,
        _ => a.trim() == b.trim(),
    }
}

/// `3.13.1` -> `(3, 13)`.
pub fn parse_major_minor(version: &str) -> Option<(u32, u32)> {
    let mut parts = version.split('.');
    let major = parts.next()?.trim().parse().ok()?;
    let minor = parts
        .next()?
        .trim()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>()
        .parse()
        .ok()?;
    Some((major, minor))
}

/// A module the probe looked for.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ModuleProbe {
    /// `importlib.util.find_spec` found it.
    pub found: bool,
    /// The installed distribution's version.
    pub version: Option<String>,
}

/// One `meerschaum` package directory on the interpreter's `sys.path`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CopyProbe {
    /// The package directory.
    pub dir: String,
    /// `meerschaum/config/_version.py`'s `__version__`.
    pub version: Option<String>,
}

/// What the interpreter says about the `meerschaum` package.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PackageProbe {
    /// The package is importable.
    pub found: bool,
    /// The directory the import would load.
    pub dir: Option<String>,
    /// Its version.
    pub version: Option<String>,
    /// Every copy on `sys.path`, in order (the first one wins).
    pub copies: Vec<CopyProbe>,
    /// The interpreter's user site directory.
    pub user_site: Option<String>,
    /// The package has a `worker` module (v4 has not).
    pub has_worker: bool,
    /// The `PROTOCOL_VERSION` in `meerschaum/worker/__init__.py`, read as text.
    pub protocol: Option<u32>,
}

/// The worker interpreter's answers (one subprocess, no plugin code, no `meerschaum` import).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PyProbe {
    /// `3.13.1`.
    pub version: String,
    /// `sys.executable`.
    pub executable: String,
    /// `sys.prefix`.
    pub prefix: String,
    /// `sys.base_prefix`.
    pub base_prefix: String,
    /// The modules of [`PROBE_MODULES`].
    pub modules: BTreeMap<String, ModuleProbe>,
    /// The `meerschaum` package.
    pub meerschaum: PackageProbe,
}

/// `(import name, distribution name)` the probe looks up.
pub const PROBE_MODULES: &[(&str, &str)] = &[
    ("pandas", "pandas"),
    ("pyarrow", "pyarrow"),
    ("polars", "polars"),
    ("shapely", "shapely"),
    ("geopandas", "geopandas"),
    ("pyproj", "pyproj"),
    ("sqlalchemy", "SQLAlchemy"),
    ("fastapi", "fastapi"),
    ("uvicorn", "uvicorn"),
    ("jwt", "PyJWT"),
    ("dash", "dash"),
    ("dash_bootstrap_components", "dash-bootstrap-components"),
    ("ptpython", "ptpython"),
    ("prompt_toolkit", "prompt-toolkit"),
];

/// The extras (`pip install 'meerschaum[<extra>]'`) and the modules that show they are installed.
pub const EXTRA_MODULES: &[(&str, &[&str])] = &[
    ("pandas", &["pandas", "pyarrow"]),
    ("polars", &["polars"]),
    ("geo", &["shapely", "geopandas", "pyproj"]),
    ("sql", &["sqlalchemy"]),
    ("api", &["fastapi", "uvicorn", "jwt"]),
    ("dash", &["dash", "dash_bootstrap_components"]),
    ("shell", &["ptpython", "prompt_toolkit"]),
];

/// Run by `python -c`; `argv[1]` is the JSON list of `[module, distribution]`. It never imports
/// `meerschaum` (or any probed module): `find_spec` on top-level names only reads the path.
const PROBE_SCRIPT: &str = r#"
import sys, os, json, re
out = {
    "version": "%d.%d.%d" % tuple(sys.version_info[:3]),
    "executable": sys.executable,
    "prefix": sys.prefix,
    "base_prefix": getattr(sys, "base_prefix", sys.prefix),
}
try:
    import importlib.util as _u
    import importlib.metadata as _m
except Exception:
    _u = _m = None
def find(name):
    try:
        spec = _u.find_spec(name)
    except Exception:
        return None
    return spec
def dist(name):
    try:
        return _m.version(name)
    except Exception:
        return None
def version_of(directory):
    try:
        with open(os.path.join(directory, "config", "_version.py")) as f:
            m = re.search(r"__version__\s*=\s*['\"]([^'\"]+)", f.read())
        return m.group(1) if m else None
    except Exception:
        return None
mods = json.loads(sys.argv[1])
out["modules"] = {m: {"found": find(m) is not None, "version": dist(d)} for m, d in mods}
copies = []
for p in sys.path:
    try:
        d = os.path.join(p or os.getcwd(), "meerschaum")
        if os.path.isfile(os.path.join(d, "__init__.py")) and d not in copies:
            copies.append(d)
    except Exception:
        pass
spec = find("meerschaum")
directory = None
if spec is not None and spec.origin and spec.origin != "namespace":
    directory = os.path.dirname(spec.origin)
elif spec is not None and spec.submodule_search_locations:
    directory = list(spec.submodule_search_locations)[0]
try:
    import site
    user_site = site.getusersitepackages()
except Exception:
    user_site = None
def protocol_of(directory):
    try:
        with open(os.path.join(directory, "worker", "__init__.py")) as f:
            m = re.search(r"PROTOCOL_VERSION\s*(?::\s*int\s*)?=\s*(\d+)", f.read())
        return int(m.group(1)) if m else None
    except Exception:
        return None
out["meerschaum"] = {
    "found": spec is not None,
    "dir": directory,
    "version": version_of(directory) if directory else None,
    "has_worker": bool(directory) and os.path.isfile(os.path.join(directory, "worker", "__init__.py")),
    "protocol": protocol_of(directory) if directory else None,
    "copies": [{"dir": c, "version": version_of(c)} for c in copies],
    "user_site": user_site,
}
print(json.dumps(out))
"#;

/// Read the probe's output: the last line that is a JSON object.
pub fn parse_probe(stdout: &str) -> Result<PyProbe, String> {
    let line = stdout
        .lines()
        .rev()
        .find(|l| l.trim_start().starts_with('{'))
        .ok_or_else(|| "the interpreter printed no answer".to_string())?;
    let json: Json = serde_json::from_str(line).map_err(|e| format!("unreadable answer: {e}"))?;
    let text = |v: &Json, key: &str| {
        v.get(key)
            .and_then(Json::as_str)
            .map(str::to_string)
            .unwrap_or_default()
    };
    let opt = |v: &Json, key: &str| v.get(key).and_then(Json::as_str).map(str::to_string);
    let mut probe = PyProbe {
        version: text(&json, "version"),
        executable: text(&json, "executable"),
        prefix: text(&json, "prefix"),
        base_prefix: text(&json, "base_prefix"),
        ..PyProbe::default()
    };
    if let Some(modules) = json.get("modules").and_then(Json::as_object) {
        for (name, info) in modules {
            probe.modules.insert(
                name.clone(),
                ModuleProbe {
                    found: info.get("found").and_then(Json::as_bool).unwrap_or(false),
                    version: opt(info, "version"),
                },
            );
        }
    }
    if let Some(m) = json.get("meerschaum") {
        probe.meerschaum = PackageProbe {
            found: m.get("found").and_then(Json::as_bool).unwrap_or(false),
            dir: opt(m, "dir"),
            version: opt(m, "version"),
            user_site: opt(m, "user_site"),
            has_worker: m.get("has_worker").and_then(Json::as_bool).unwrap_or(false),
            protocol: m
                .get("protocol")
                .and_then(Json::as_u64)
                .and_then(|p| u32::try_from(p).ok()),
            copies: m
                .get("copies")
                .and_then(Json::as_array)
                .map(|list| {
                    list.iter()
                        .map(|c| CopyProbe {
                            dir: text(c, "dir"),
                            version: opt(c, "version"),
                        })
                        .collect()
                })
                .unwrap_or_default(),
        };
    }
    if probe.version.is_empty() {
        return Err("the answer has no Python version".into());
    }
    Ok(probe)
}

/// The worker interpreter: found, new enough.
pub fn judge_interpreter(
    probe: &Result<PyProbe, String>,
    command: &str,
    python_plugins: usize,
) -> Verdict {
    match probe {
        Err(e) if python_plugins > 0 => Verdict::fail(
            format!(
                "cannot run '{command}': {e}; {python_plugins} Python plugin{} need it",
                super::plural(python_plugins)
            ),
            "install Python 3.11 or newer, or set MRSM_PYTHON to an interpreter that has `meerschaum`",
        ),
        Err(e) => Verdict::warn(
            format!(
                "cannot run '{command}': {e}. Plugins, Python connectors and the API host need it; \
                 the native CLI works without"
            ),
            "install Python 3.11 or newer, or set MRSM_PYTHON to an interpreter that has `meerschaum`",
        ),
        Ok(p) => match parse_major_minor(&p.version) {
            Some(mm) if mm < MIN_PYTHON => Verdict::fail(
                format!(
                    "Python {} at {}: `meerschaum` needs Python {}.{} or newer",
                    p.version, p.executable, MIN_PYTHON.0, MIN_PYTHON.1
                ),
                "set MRSM_PYTHON to a Python 3.11+ interpreter",
            ),
            _ => Verdict::pass(format!("Python {} at {}", p.version, p.executable)),
        },
    }
}

/// The `meerschaum` package the worker interpreter imports, judged by the same policy as the
/// guard the worker and API hosts run (`meerschaum_core::python_guard`): another major version,
/// a copy that looks like v4 and another worker protocol are refused (a FAIL when Python plugins
/// need that interpreter, a WARN otherwise); another release of the same major version starts and
/// is a WARN naming both versions.
pub fn judge_package(
    probe: &Result<PyProbe, String>,
    binary_version: &str,
    expected_protocol: u32,
    python_plugins: usize,
) -> Verdict {
    use meerschaum_core::python_guard::{self, Refusal};
    let Ok(p) = probe else {
        return Verdict::skip("no Python interpreter to ask");
    };
    let m = &p.meerschaum;
    let expected = python_guard::expected_version();
    let install_matching = format!("`{} -m pip install 'meerschaum=={expected}'`", p.executable);
    if !m.found {
        let detail = format!("`meerschaum` is not importable by {}", p.executable);
        let fix = format!(
            "install it there ({install_matching}) or set MRSM_PYTHON to an interpreter that has it"
        );
        return needs_python(detail, fix, python_plugins);
    }
    let dir = m.dir.clone().unwrap_or_else(|| "(unknown location)".into());
    let found = m.version.as_deref();
    let verdict = python_guard::judge(
        binary_version,
        expected_protocol,
        found,
        m.has_worker,
        m.protocol,
    );
    let shown = found.unwrap_or("(unknown version)");
    let in_user_site = m
        .user_site
        .as_deref()
        .is_some_and(|u| !u.is_empty() && dir.starts_with(u));
    let site = if in_user_site { " (the user site)" } else { "" };
    match verdict {
        python_guard::Verdict::Refuse(reason) => {
            let why = match reason {
                Refusal::Major => {
                    format!("another major version than this program ({binary_version})")
                }
                Refusal::LooksLikeV4 => {
                    "it looks like Meerschaum 4 (no `worker` module, no readable version)"
                        .to_string()
                }
                Refusal::Protocol(n) => format!(
                    "it speaks worker protocol {n}, this program speaks {expected_protocol}"
                ),
            };
            let detail = format!(
                "{} imports `meerschaum` {shown} from {dir}{site}: {why}; the worker host refuses it",
                p.executable
            );
            // Another copy behind it: uninstall this one. The only copy: install the matching one.
            let fix = if m.copies.len() > 1 {
                format!(
                    "uninstall this copy (delete {dir}, or `{} -m pip uninstall meerschaum`, once per copy) so that the right one is found first, or set MRSM_PYTHON to an interpreter that has {expected}",
                    p.executable
                )
            } else {
                format!(
                    "install the matching release ({install_matching}), or set MRSM_PYTHON to an interpreter that has it"
                )
            };
            needs_python(detail, fix, python_plugins)
        }
        python_guard::Verdict::OtherRelease => Verdict::warn(
            format!(
                "`meerschaum` {shown} loads from {dir}{site} and this program is {binary_version}: the same major version and worker protocol, so the worker starts (and warns once)"
            ),
            format!("align them: {install_matching}, or upgrade this program to {shown}"),
        ),
        python_guard::Verdict::Match => {
            let others: Vec<String> = m
                .copies
                .iter()
                .filter(|c| Some(&c.dir) != m.dir.as_ref())
                .map(|c| match &c.version {
                    Some(v) => format!("{} ({v})", c.dir),
                    None => c.dir.clone(),
                })
                .collect();
            if !others.is_empty() {
                return Verdict::warn(
                    format!(
                        "`meerschaum` {shown} loads from {dir}; other copies on sys.path: {}",
                        others.join(", ")
                    ),
                    "remove the extra copies (`pip uninstall meerschaum` until none is left, then reinstall)",
                );
            }
            Verdict::pass(format!("`meerschaum` {shown} loads from {dir}"))
        }
    }
}

/// FAIL when Python plugins run in the interpreter (they cannot), else WARN (only plugins need it).
fn needs_python(detail: String, fix: String, python_plugins: usize) -> Verdict {
    if python_plugins > 0 {
        Verdict::fail(
            format!(
                "{detail}; {python_plugins} Python plugin{} cannot run",
                super::plural(python_plugins)
            ),
            fix,
        )
    } else {
        Verdict::warn(format!("{detail}; only plugins need it"), fix)
    }
}

/// A filesystem's free space and inodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiskFact {
    /// Bytes available to an unprivileged process.
    pub free_bytes: u64,
    /// Bytes in the filesystem.
    pub total_bytes: u64,
    /// Free inodes (`None`: the filesystem has no inode count).
    pub free_inodes: Option<u64>,
    /// Total inodes.
    pub total_inodes: Option<u64>,
}

impl DiskFact {
    /// Free inodes as a percentage (`None` without an inode count).
    pub fn inode_free_percent(&self) -> Option<f64> {
        match (self.free_inodes, self.total_inodes) {
            (Some(free), Some(total)) if total > 0 => Some(free as f64 * 100.0 / total as f64),
            _ => None,
        }
    }
}

/// Free space and inodes of the filesystem holding `what`.
pub fn judge_disk(what: &str, f: &DiskFact) -> Verdict {
    let percent = f.inode_free_percent();
    let inodes = percent
        .map(|p| format!(", inodes {p:.0}% free"))
        .unwrap_or_default();
    let detail = format!(
        "{what}: {} free of {}{inodes}",
        human_bytes(f.free_bytes),
        human_bytes(f.total_bytes)
    );
    let fix = format!(
        "free space on the filesystem of {what} (or point it elsewhere: MRSM_ROOT_DIR, TMPDIR)"
    );
    if f.free_bytes < DISK_FAIL_BYTES || percent.is_some_and(|p| p < INODE_FAIL_PERCENT) {
        return Verdict::fail(detail, fix);
    }
    if f.free_bytes < DISK_WARN_BYTES || percent.is_some_and(|p| p < INODE_WARN_PERCENT) {
        return Verdict::warn(detail, fix);
    }
    Verdict::pass(detail)
}

/// What a directory is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DirRole {
    /// `MRSM_ROOT_DIR`: everything is written below it.
    Root,
    /// The config directory.
    Config,
    /// A plugins directory.
    Plugins,
}

/// A directory's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DirFact {
    /// The path exists.
    pub exists: bool,
    /// It is a directory.
    pub is_dir: bool,
    /// The current user may write to it.
    pub writable: bool,
}

/// Exists and is writable.
pub fn judge_dir(path: &str, role: DirRole, f: &DirFact) -> Verdict {
    let (what, severe) = match role {
        DirRole::Root => ("the root directory (config, jobs, venvs, caches)", true),
        DirRole::Config => ("the config directory", false),
        DirRole::Plugins => ("the plugins directory", false),
    };
    if f.exists && !f.is_dir {
        return Verdict::fail(
            format!("{path} exists but is not a directory"),
            format!("move it away or point {what} elsewhere"),
        );
    }
    if !f.exists {
        return match role {
            DirRole::Root => Verdict::fail(
                format!("{path} does not exist"),
                "create it, or set MRSM_ROOT_DIR to an existing directory",
            ),
            DirRole::Config => Verdict::pass(format!("{path} (created on first write)")),
            DirRole::Plugins => Verdict::pass(format!(
                "{path} (not created yet; `mrsm install plugins` creates it)"
            )),
        };
    }
    if !f.writable {
        let fix = format!(
            "make {path} writable for this user (e.g. `chmod u+w {path}`), or use another directory"
        );
        let detail = format!("{path} is not writable: {what} cannot be updated");
        return if severe {
            Verdict::fail(detail, fix)
        } else {
            Verdict::warn(detail, fix)
        };
    }
    Verdict::pass(format!("{path} (writable)"))
}

/// One config file of the config directory.
#[derive(Debug, Clone, PartialEq)]
pub struct ConfigFile {
    /// The file name (`meerschaum.yaml`).
    pub name: String,
    /// The config key it holds (`meerschaum`).
    pub stem: String,
    /// The parse error, if any.
    pub error: Option<String>,
    /// The parsed content.
    pub value: Option<Value>,
}

/// v4-only config keys v5 ignores when they are set: `(path, advice)`. A key v5's own default
/// config still writes (`join_fetch: false`) only counts when it is switched on.
pub const DEPRECATED_KEYS: &[(&[&str], &str)] = &[(
    &["system", "experimental", "join_fetch"],
    "`join_fetch: true` is ignored in v5 (it was undocumented and off by default): remove it",
)];

/// The keys of [`DEPRECATED_KEYS`] set in the parsed files: `(file, dotted path, advice)`.
pub fn deprecated_keys(files: &[ConfigFile]) -> Vec<(String, String, &'static str)> {
    let mut out = Vec::new();
    for file in files {
        let Some(value) = &file.value else { continue };
        for (path, advice) in DEPRECATED_KEYS {
            if path[0] != file.stem {
                continue;
            }
            if value.get_path(&path[1..]).is_some_and(Value::truthy) {
                out.push((file.name.clone(), path.join(":"), *advice));
            }
        }
    }
    out
}

/// The config files and keys: parse errors, files that are not a config key, unknown
/// `meerschaum:` sections and deprecated keys. `known_top` and `known_meerschaum` come from the
/// registry's defaults.
pub fn judge_config(
    files: &[ConfigFile],
    known_top: &[String],
    known_meerschaum: &[String],
) -> Vec<(&'static str, Verdict)> {
    let mut out = Vec::new();
    let broken: Vec<String> = files
        .iter()
        .filter_map(|f| f.error.as_ref().map(|e| format!("{}: {e}", f.name)))
        .collect();
    if !broken.is_empty() {
        out.push((
            "files",
            Verdict::fail(
                format!(
                    "{} config file{} not parse (the defaults are used instead): {}",
                    broken.len(),
                    if broken.len() == 1 { " does" } else { "s do" },
                    broken.join("; ")
                ),
                "fix the syntax (`mrsm edit config <key>`), or move the file away so defaults are rewritten",
            ),
        ));
    } else if files.is_empty() {
        out.push((
            "files",
            Verdict::pass("no config files yet (defaults in use)"),
        ));
    } else {
        out.push((
            "files",
            Verdict::pass(format!("{} config files parsed", files.len())),
        ));
    }
    let mut notes = Vec::new();
    for f in files {
        if !known_top.contains(&f.stem) {
            notes.push(format!(
                "{} is not a config key (known: {})",
                f.name,
                known_top.join(", ")
            ));
        }
        if f.stem == "meerschaum"
            && let Some(map) = f.value.as_ref().and_then(Value::as_map)
        {
            for key in map.keys().filter(|k| !known_meerschaum.contains(k)) {
                notes.push(format!("meerschaum:{key} in {} is unknown", f.name));
            }
        }
    }
    let deprecated = deprecated_keys(files);
    for (file, path, advice) in &deprecated {
        notes.push(format!("{path} in {file}: {advice}"));
    }
    if notes.is_empty() {
        out.push(("keys", Verdict::pass("no unknown or deprecated keys")));
    } else {
        out.push((
            "keys",
            Verdict::warn(
                notes.join("; "),
                "edit or remove them (`mrsm edit config <key>`); unknown files are ignored unless a plugin reads them",
            ),
        ));
    }
    out
}

/// `MRSM_*` variable names split into connector variables (`MRSM_<TYPE>_<LABEL>` of a connector
/// type that exists) and others that merely look like one (`MRSM_DUCKDB_LIB`, `MRSM_TEST_*`).
pub fn split_env_vars(names: &[String], custom_types: &[String]) -> (Vec<String>, Vec<String>) {
    let mut connectors = Vec::new();
    let mut others = Vec::new();
    for name in names {
        let known =
            meerschaum_core::config::parse_connector_env_var(name).is_some_and(|(typ, _)| {
                meerschaum_core::config::connector_types::is_builtin_connector_type(&typ)
                    || custom_types.contains(&typ)
            });
        if known {
            connectors.push(name.clone());
        } else {
            others.push(name.clone());
        }
    }
    (connectors, others)
}

/// What a connector section needs from the native drivers: `(label, flavor)` of every SQL
/// connector (the default instance's label is `instance`).
pub fn driver_users<'a>(connectors: &'a [(String, String)], flavor: &str) -> Vec<&'a str> {
    connectors
        .iter()
        .filter(|(_, f)| f == flavor)
        .map(|(l, _)| l.as_str())
        .collect()
}

/// A native driver library (DuckDB, Oracle client).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DriverFact {
    /// `duckdb` or `oracle`.
    pub name: &'static str,
    /// `show drivers`' source (`env`, `cache`, `system`, `error`, `missing`).
    pub source: String,
    /// Its version, if known.
    pub version: String,
    /// Its path (or the error text when the source is `error`).
    pub path: String,
}

/// Found, or missing and needed by a configured connector (FAIL when the default instance needs
/// it and nothing will fetch it, WARN otherwise).
pub fn judge_driver(
    d: &DriverFact,
    needed_by: &[&str],
    instance_label: Option<&str>,
    auto_install: bool,
) -> Verdict {
    let need_instance = instance_label.is_some_and(|l| needed_by.contains(&l));
    let needed = needed_by.join(", ");
    match d.source.as_str() {
        "missing" | "error" => {
            let broken = d.source == "error";
            let what = if broken {
                format!("{} cannot be loaded: {}", d.name, d.path)
            } else {
                format!("{} is not installed", d.name)
            };
            let fix = match d.name {
                "duckdb" => {
                    "`mrsm install driver duckdb` (or set MRSM_DUCKDB_LIB to a libduckdb you provide)"
                }
                _ => {
                    "install Oracle Instant Client from Oracle and put it on the library path (LD_LIBRARY_PATH, DYLD_LIBRARY_PATH or PATH)"
                }
            };
            if needed_by.is_empty() {
                return Verdict::skip(format!("{what}; no configured connector needs it"));
            }
            let first_use = !broken && d.name == "duckdb" && auto_install;
            if need_instance && !first_use {
                return Verdict::fail(format!("{what}; the default instance needs it"), fix);
            }
            if first_use {
                return Verdict::warn(
                    format!("{what}; it is downloaded on first use (needed by {needed})"),
                    fix,
                );
            }
            Verdict::warn(format!("{what}; needed by {needed}"), fix)
        }
        _ => {
            let version = d.version.trim();
            let mut detail = if version.starts_with('(') || version.is_empty() {
                "found".to_string()
            } else {
                version.to_string()
            };
            if !d.path.is_empty() {
                detail = format!("{detail} at {} ({})", d.path, d.source);
            }
            Verdict::pass(detail)
        }
    }
}

/// What the plugin manifest says (no plugin code is imported).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PluginFacts {
    /// The plugins (Python and native), by name.
    pub names: Vec<String>,
    /// `(plugin, first line of the import error)`.
    pub import_errors: Vec<(String, String)>,
    /// Errors raised importing all plugins at once.
    pub load_errors: Vec<(String, String)>,
    /// Plugins with `attempt_import` findings of undeclared packages.
    pub venv_warnings: Vec<String>,
    /// Plugins that need the API host (`api` extra).
    pub needs_api: Vec<String>,
    /// Plugins that need Dash (`dash` extra).
    pub needs_dash: Vec<String>,
    /// Plugins on disk the manifest does not list (it was never built, or is stale): their
    /// imports have not been checked, and the doctor never imports plugin code.
    pub unbuilt: Vec<String>,
}

fn first_line(s: &str) -> String {
    s.lines().next().unwrap_or("").trim().to_string()
}

/// Read the manifest.
pub fn plugin_facts(m: &Manifest) -> PluginFacts {
    let mut f = PluginFacts::default();
    for (name, p) in &m.plugins {
        f.names.push(name.clone());
        if let Some(e) = &p.import_error {
            f.import_errors.push((name.clone(), first_line(e)));
        }
        if !p.venv_warnings.is_empty() {
            f.venv_warnings.push(name.clone());
        }
        let host_pages = p.web_pages.iter().any(|w| w.mode != "dash");
        if !p.api_plugins.is_empty() || host_pages || !p.mcp_tools.is_empty() {
            f.needs_api.push(name.clone());
        }
        if !p.dash_plugins.is_empty() || p.web_pages.iter().any(|w| w.mode == "dash") {
            f.needs_dash.push(name.clone());
            // Dash apps are served by the API host.
            if !f.needs_api.contains(name) {
                f.needs_api.push(name.clone());
            }
        }
    }
    f.load_errors = m
        .load_errors
        .iter()
        .map(|(k, v)| (k.clone(), first_line(v)))
        .collect();
    f
}

/// The plugins checks: how many, import errors, undeclared packages.
pub fn judge_plugins(f: &PluginFacts) -> Vec<(&'static str, Verdict)> {
    let mut out = Vec::new();
    if f.names.is_empty() {
        out.push(("installed", Verdict::skip("no plugins installed")));
        return out;
    }
    out.push((
        "installed",
        Verdict::pass(format!(
            "{} plugin{}: {}",
            f.names.len(),
            super::plural(f.names.len()),
            f.names.join(", ")
        )),
    ));
    let mut errors: Vec<String> = f
        .import_errors
        .iter()
        .map(|(n, e)| format!("{n}: {e}"))
        .collect();
    errors.extend(f.load_errors.iter().map(|(n, e)| format!("{n}: {e}")));
    if errors.is_empty() && !f.unbuilt.is_empty() {
        out.push((
            "imports",
            Verdict::skip(format!(
                "not imported yet: {} (the doctor never imports plugins; `mrsm reload` builds the manifest)",
                f.unbuilt.join(", ")
            )),
        ));
    } else if errors.is_empty() {
        out.push(("imports", Verdict::pass("every plugin imported")));
    } else {
        out.push((
            "imports",
            Verdict::warn(
                format!("import errors: {}", errors.join("; ")),
                "fix the plugin (`mrsm verify plugins` imports each one and prints the traceback); missing packages: `mrsm install required <plugin>`",
            ),
        ));
    }
    if !f.venv_warnings.is_empty() {
        out.push((
            "requirements",
            Verdict::warn(
                format!(
                    "plugins importing packages they do not list in `required`: {}",
                    f.venv_warnings.join(", ")
                ),
                "add the packages to the plugin's `required` list",
            ),
        ));
    }
    out
}

/// The cached plugin manifest against the plugins on disk (read-only: the doctor never builds
/// it). `stale` is `stale_plugins` of the cached manifest, `changed` the plugins it names.
pub fn judge_manifest(
    cached: bool,
    stale: bool,
    changed: &[String],
    on_disk: usize,
    listed: usize,
) -> Verdict {
    const FIX: &str = "`mrsm reload` rebuilds it (any command that loads plugins also rebuilds it when a plugin changed)";
    match (cached, stale, on_disk) {
        (false, _, 0) => Verdict::pass("no plugins on disk, so no manifest is needed"),
        (false, _, n) => Verdict::warn(
            format!(
                "the plugin manifest is missing: {n} plugin{} on disk {} not been imported (the doctor never imports plugins)",
                super::plural(n),
                if n == 1 { "has" } else { "have" }
            ),
            FIX,
        ),
        (true, true, _) => Verdict::warn(
            format!(
                "the plugin manifest is stale{}: the plugin details below are from the last build",
                if changed.is_empty() {
                    String::new()
                } else {
                    format!(" (changed: {})", changed.join(", "))
                }
            ),
            FIX,
        ),
        (true, false, _) => Verdict::pass(format!(
            "up to date ({listed} plugin{})",
            super::plural(listed)
        )),
    }
}

/// One extra: installed, or missing while a plugin needs it.
pub fn judge_extra(
    extra: &str,
    modules: &[(&str, Option<&ModuleProbe>)],
    needed_by: &[String],
    python: &str,
) -> Verdict {
    let found: Vec<String> = modules
        .iter()
        .filter_map(|(m, p)| {
            p.filter(|p| p.found).map(|p| match &p.version {
                Some(v) => format!("{m} {v}"),
                None => m.to_string(),
            })
        })
        .collect();
    let missing: Vec<&str> = modules
        .iter()
        .filter(|(_, p)| !p.is_some_and(|p| p.found))
        .map(|(m, _)| *m)
        .collect();
    let fix = format!("`{python} -m pip install 'meerschaum[{extra}]'`");
    if missing.is_empty() {
        return Verdict::pass(found.join(", "));
    }
    if !needed_by.is_empty() {
        return Verdict::warn(
            format!(
                "plugins {} need the `{extra}` extra; missing: {}",
                needed_by.join(", "),
                missing.join(", ")
            ),
            fix,
        );
    }
    if found.is_empty() {
        return Verdict::skip("not installed (optional)");
    }
    // Optional and needed by nothing installed: informational (the missing packages are named).
    Verdict::pass(format!(
        "{}; not installed: {}",
        found.join(", "),
        missing.join(", ")
    ))
}

/// A virtual environment on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VenvFact {
    /// The directory name (`mrsm`, or a plugin's name).
    pub name: String,
    /// The `X.Y` of every `lib/pythonX.Y/site-packages`.
    pub minors: Vec<(u32, u32)>,
    /// `bin/python*` entries that no longer resolve (dangling links).
    pub dangling: Vec<String>,
}

/// Venvs built by another Python than the worker's, or whose interpreter link is gone.
pub fn judge_venvs(venvs: &[VenvFact], worker: Option<(u32, u32)>) -> Verdict {
    if venvs.is_empty() {
        return Verdict::skip("no virtual environments");
    }
    let mut stale = Vec::new();
    for v in venvs {
        if !v.dangling.is_empty() {
            stale.push(format!(
                "{} (interpreter {} is gone)",
                v.name,
                v.dangling.join(", ")
            ));
        } else if let Some((major, minor)) = worker
            && !v.minors.is_empty()
            && !v.minors.contains(&(major, minor))
        {
            let built: Vec<String> = v.minors.iter().map(|(a, b)| format!("{a}.{b}")).collect();
            stale.push(format!(
                "{} (built for Python {}, the worker is {major}.{minor})",
                v.name,
                built.join("/")
            ));
        }
    }
    if stale.is_empty() {
        return Verdict::pass(format!(
            "{} virtual environment{} match the worker interpreter",
            venvs.len(),
            super::plural(venvs.len())
        ));
    }
    Verdict::warn(
        format!("stale virtual environments: {}", stale.join("; ")),
        "`mrsm verify venvs` relinks a lost interpreter; after a Python upgrade `mrsm delete venvs <name>` then `mrsm install required <plugin>` rebuilds it",
    )
}

/// One job directory, read without any status call (those unlink stale PID files).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JobFact {
    /// The job's name.
    pub name: String,
    /// The PID file's content.
    pub pid: Option<i32>,
    /// The process state: `None` without a PID.
    pub process: Option<ProcessState>,
    /// The recorded result: `(success, message)`.
    pub result: Option<(bool, String)>,
}

/// What the doctor distinguishes of a process.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessState {
    /// Running.
    Alive,
    /// Stopped by a signal (a paused job).
    Paused,
    /// No such process (or a zombie).
    Gone,
}

/// Jobs: how many run, which failed, which PID files are stale.
pub fn judge_jobs(jobs: &[JobFact]) -> Vec<(&'static str, Verdict)> {
    if jobs.is_empty() {
        return vec![("state", Verdict::skip("no jobs"))];
    }
    let running = jobs
        .iter()
        .filter(|j| j.process == Some(ProcessState::Alive))
        .count();
    let paused = jobs
        .iter()
        .filter(|j| j.process == Some(ProcessState::Paused))
        .count();
    let stale: Vec<&str> = jobs
        .iter()
        .filter(|j| j.pid.is_some() && j.process == Some(ProcessState::Gone))
        .map(|j| j.name.as_str())
        .collect();
    let failed: Vec<String> = jobs
        .iter()
        .filter(|j| matches!(j.process, None | Some(ProcessState::Gone)))
        .filter_map(|j| match &j.result {
            Some((false, message)) => Some(format!("{} ({})", j.name, first_line(message))),
            _ => None,
        })
        .collect();
    let mut out = vec![(
        "state",
        Verdict::pass(format!(
            "{} job{}: {running} running, {paused} paused, {} stopped",
            jobs.len(),
            super::plural(jobs.len()),
            jobs.len() - running - paused
        )),
    )];
    if !failed.is_empty() {
        out.push((
            "failed jobs",
            Verdict::warn(
                format!("the last run failed: {}", failed.join("; ")),
                "`mrsm show logs <name>` for the output, then `mrsm restart jobs <name>`",
            ),
        ));
    }
    if !stale.is_empty() {
        out.push((
            "pid files",
            Verdict::warn(
                format!(
                    "stale PID files (the process is gone): {}",
                    stale.join(", ")
                ),
                "`mrsm show jobs` removes them; `mrsm start jobs <name>` starts the job again",
            ),
        ));
    }
    out
}

/// One container of the stack.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceFact {
    /// The compose service (`db`).
    pub service: String,
    /// The container name.
    pub container: String,
    /// The image reference.
    pub image: String,
    /// `running`, `exited`, `restarting`, ...
    pub state: String,
    /// `Up 3 hours (healthy)`.
    pub status: String,
}

/// Parse `docker ps --format '{{label service}}\t{{.Names}}\t{{.Image}}\t{{.State}}\t{{.Status}}'`.
pub fn parse_services(text: &str) -> Vec<ServiceFact> {
    text.lines()
        .filter_map(|line| {
            let parts: Vec<&str> = line.split('\t').collect();
            (parts.len() >= 5).then(|| ServiceFact {
                service: parts[0].to_string(),
                container: parts[1].to_string(),
                image: parts[2].to_string(),
                state: parts[3].to_string(),
                status: parts[4].to_string(),
            })
        })
        .collect()
}

/// The exit code of `Exited (137) 2 minutes ago`.
pub fn exit_code(status: &str) -> Option<i32> {
    let rest = status.strip_prefix("Exited (")?;
    rest.split(')').next()?.trim().parse().ok()
}

/// One stack service's check.
pub fn judge_service(s: &ServiceFact, project: &str) -> Verdict {
    let status = s.status.to_lowercase();
    let detail = format!("{} ({}): {}", s.image, s.container, s.status);
    let service = if s.service.is_empty() {
        "<service>"
    } else {
        &s.service
    };
    if s.state == "running" && status.contains("unhealthy") {
        return Verdict::fail(
            detail,
            format!(
                "`docker logs {}` shows why; `mrsm stack restart {service}` restarts it",
                s.container
            ),
        );
    }
    if s.state == "running" && status.contains("health: starting") {
        return Verdict::warn(detail, "wait a moment and run `mrsm doctor` again");
    }
    if s.state == "running" {
        return Verdict::pass(detail);
    }
    // A service stopped on purpose (`docker stop`, `mrsm stack stop`) exits 0, or 137 / 143 for
    // the signal that stopped it: a warning, not a failure.
    if s.state == "exited" && matches!(exit_code(&s.status), Some(0 | 137 | 143)) {
        return Verdict::warn(
            format!("stopped: {detail}"),
            format!("`mrsm stack up -d {service}` starts it again (project {project})"),
        );
    }
    Verdict::fail(
        detail,
        format!(
            "`mrsm stack up -d {service}` (project {project}); `docker logs {}` shows why it stopped",
            s.container
        ),
    )
}

/// The API server's version against this client's.
pub fn judge_api_version(server: Option<&str>, client: &str) -> Verdict {
    match server {
        None => Verdict::warn(
            "the server did not report its version (`/version/mrsm`)",
            "check the server is Meerschaum and reachable at the configured host and port",
        ),
        Some(v) if versions_match(v, client) => {
            Verdict::pass(format!("server and client are {client}"))
        }
        Some(v) => Verdict::warn(
            format!("the server runs {v}, this client is {client}"),
            "upgrade the older side so both run the same release",
        ),
    }
}

/// The doctor's name for the check of `key` (`pipes key`, `users key`).
pub fn unique_key_check_name(key: &UniqueKey) -> String {
    format!("{} key", key.table.trim_start_matches("mrsm_"))
}

/// An instance table has its unique key (5.0: `mrsm_pipes` on the pipe keys, `mrsm_users` on the
/// username). A table created by v4 gets it the first time 5.0 writes to the instance, unless a
/// key value is held by more than one row.
pub fn judge_unique_key(
    key: &UniqueKey,
    status: &Result<KeyStatus, String>,
    table: &str,
) -> Verdict {
    let (what, rows, keep) = if *key == PIPES_KEY {
        (
            "the pipe keys",
            "pipe(s) are registered",
            "the parameters you want",
        )
    } else {
        (
            "the username",
            "user(s) are registered",
            "the password and plugins you want",
        )
    };
    match status {
        Ok(KeyStatus::Present) => {
            Verdict::pass(format!("{} has a unique key on {what}", key.table))
        }
        Ok(KeyStatus::NoTable) => {
            Verdict::skip(format!("the instance has no {} table yet", key.table))
        }
        Ok(KeyStatus::Missing { duplicates }) if duplicates.is_empty() => Verdict::warn(
            format!("{} has no unique key on {what} yet", key.table),
            "it is added the next time Meerschaum registers, syncs or edits a pipe or user on the instance with permission to create indexes",
        ),
        Ok(KeyStatus::Missing { duplicates }) => {
            let shown: Vec<String> = duplicates
                .iter()
                .take(5)
                .map(|d| describe_duplicate(key, d))
                .collect();
            let more = duplicates.len().saturating_sub(shown.len());
            let extra: Vec<String> = duplicates
                .iter()
                .flat_map(|d| d.ids.iter().skip(1))
                .map(i64::to_string)
                .collect();
            Verdict::warn(
                format!(
                    "{} {rows} more than once, so {} has no unique key on {what}: {}{}",
                    duplicates.len(),
                    key.table,
                    shown.join("; "),
                    if more > 0 {
                        format!("; and {more} more")
                    } else {
                        String::new()
                    },
                ),
                format!(
                    "keep one row of each and delete the others (rows other than the first: `DELETE FROM {table} WHERE {} IN ({})`; check which row holds {keep} first); the key is added the next time a pipe or user is registered, synced or edited",
                    key.id_column,
                    extra.join(", ")
                ),
            )
        }
        Err(e) => Verdict::warn(
            format!("could not check {} for its unique key: {e}", key.table),
            "re-run `mrsm doctor` once the instance is reachable",
        ),
    }
}

fn describe_duplicate(key: &UniqueKey, d: &Duplicate) -> String {
    let ids: Vec<String> = d.ids.iter().map(i64::to_string).collect();
    let ids = format!("({} {})", key.id_column, ids.join(", "));
    let text = |v: &Option<String>| v.clone().unwrap_or_default();
    if *key == PIPES_KEY {
        let location = match d.values.get(2) {
            Some(Some(lk)) => format!(", '{lk}'"),
            _ => String::new(),
        };
        format!(
            "Pipe('{}', '{}'{location}) {ids}",
            d.values.first().map(text).unwrap_or_default(),
            d.values.get(1).map(text).unwrap_or_default(),
        )
    } else {
        let values: Vec<String> = d.values.iter().map(|v| format!("'{}'", text(v))).collect();
        format!("{} {ids}", values.join(", "))
    }
}

// ---------------------------------------------------------------------------------------------
// Gathering: thin wrappers over the system.
// ---------------------------------------------------------------------------------------------

/// How long a network, database or subprocess probe may take.
const PROBE_TIMEOUT: Duration = Duration::from_secs(5);

fn timed(f: impl FnOnce() -> Vec<Check>) -> Vec<Check> {
    let start = Instant::now();
    let mut checks = f();
    let ms = start.elapsed().as_millis();
    for c in &mut checks {
        c.millis.get_or_insert(ms);
    }
    checks
}

/// `flavor` of a SQL connector's attributes: the attribute, else the URI scheme.
pub fn flavor_of(attrs: &Map) -> String {
    if let Some(f) = attrs
        .get("flavor")
        .and_then(Value::as_str)
        .filter(|f| !f.is_empty())
    {
        return f.to_lowercase();
    }
    let flavor = attrs
        .get("uri")
        .and_then(Value::as_str)
        .and_then(|u| u.split("://").next())
        .and_then(|s| s.split('+').next())
        .unwrap_or("")
        .to_lowercase();
    if flavor == "postgres" {
        "postgresql".into()
    } else {
        flavor
    }
}

/// The database file a SQLite-like connector names (`None` when it names none).
pub fn sqlite_path(attrs: &Map, label: &str, root_db: &str) -> Option<String> {
    let from_uri = attrs.get("uri").and_then(Value::as_str).map(|u| {
        let rest = u.split_once("://").map(|(_, r)| r).unwrap_or(u);
        let rest = rest.strip_prefix('/').unwrap_or(rest);
        rest.split('?').next().unwrap_or(rest).to_string()
    });
    let path = from_uri
        .or_else(|| {
            attrs
                .get("database")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .or_else(|| (label == "local").then(|| root_db.to_string()))?;
    Some(path.replace("{SQLITE_DB_PATH}", root_db))
}

fn config_opts() -> GetOptions {
    GetOptions {
        write_missing: false,
        warn: false,
        ..GetOptions::default()
    }
}

pub(super) fn probe_python(python: &str) -> Result<PyProbe, String> {
    let modules: Vec<Json> = PROBE_MODULES.iter().map(|(m, d)| json!([m, d])).collect();
    let spec = CommandSpec {
        program: python.to_string(),
        args: vec![
            "-c".into(),
            PROBE_SCRIPT.into(),
            Json::Array(modules).to_string(),
        ],
        env: vec![("PYTHONSAFEPATH".into(), "1".into())],
        ..CommandSpec::default()
    };
    let out = super::terminal::capture(&spec, Some(Duration::from_secs(10)))
        .map_err(|e| format!("{e}"))?;
    if out.timed_out {
        return Err("it did not answer within 10 seconds".into());
    }
    if out.code != 0 {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "it exited with {}: {}",
            out.code,
            stderr.lines().last().unwrap_or("").trim()
        ));
    }
    parse_probe(&String::from_utf8_lossy(&out.stdout))
}

fn how_it_runs(env: fn(&str) -> Option<String>) -> String {
    let command = meerschaum_core::self_exe::self_command();
    let mut text = if meerschaum_core::self_exe::runs_in_python() {
        format!(
            "Python extension, in-process (re-launches itself as `{}`)",
            command.join(" ")
        )
    } else {
        format!(
            "standalone binary {}",
            command.first().map(String::as_str).unwrap_or("mrsm")
        )
    };
    if let Some(bin) = env("MRSM_BIN").filter(|b| !b.is_empty()) {
        text.push_str(&format!("; MRSM_BIN={bin}"));
    }
    text
}

fn os_description() -> String {
    let base = format!("{}/{}", std::env::consts::OS, std::env::consts::ARCH);
    let pretty = std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|t| {
            t.lines()
                .find_map(|l| l.strip_prefix("PRETTY_NAME="))
                .map(|v| v.trim_matches('"').to_string())
        });
    match pretty {
        Some(p) => format!("{base} ({p})"),
        None => base,
    }
}

fn install_checks(
    env: fn(&str) -> Option<String>,
    python_command: &str,
    probe: &Result<PyProbe, String>,
    python_plugins: usize,
) -> Vec<Check> {
    let build = format!(
        "{} build; oracle driver {}; DuckDB loads its own library at run time",
        if cfg!(debug_assertions) {
            "debug"
        } else {
            "release"
        },
        if cfg!(feature = "oracle") {
            "compiled in"
        } else {
            "not compiled in"
        },
    );
    vec![
        Check::pass(
            "install",
            "meerschaum",
            format!("{}; {}", crate::VERSION, how_it_runs(env)),
        ),
        Check::pass("install", "build", build),
        Check::pass("install", "platform", os_description()),
        Check::from_verdict(
            "install",
            "worker python",
            judge_interpreter(probe, python_command, python_plugins),
        ),
        Check::from_verdict(
            "install",
            "meerschaum package",
            judge_package(
                probe,
                crate::VERSION,
                meerschaum_core::python_guard::WORKER_PROTOCOL_VERSION,
                python_plugins,
            ),
        ),
    ]
}

fn writable(path: &Path) -> bool {
    #[cfg(unix)]
    {
        rustix::fs::access(path, rustix::fs::Access::WRITE_OK).is_ok()
    }
    #[cfg(not(unix))]
    {
        std::fs::metadata(path).is_ok_and(|m| !m.permissions().readonly())
    }
}

fn dir_fact(path: &Path) -> DirFact {
    match std::fs::metadata(path) {
        Ok(m) => DirFact {
            exists: true,
            is_dir: m.is_dir(),
            writable: writable(path),
        },
        Err(_) => DirFact {
            exists: false,
            is_dir: false,
            writable: false,
        },
    }
}

#[cfg(unix)]
fn disk_fact(path: &Path) -> Option<DiskFact> {
    let s = rustix::fs::statvfs(path).ok()?;
    let unit = if s.f_frsize > 0 {
        s.f_frsize
    } else {
        s.f_bsize
    };
    let total_inodes = (s.f_files > 0).then_some(s.f_files);
    Some(DiskFact {
        free_bytes: s.f_bavail.saturating_mul(unit),
        total_bytes: s.f_blocks.saturating_mul(unit),
        free_inodes: total_inodes.map(|_| s.f_ffree),
        total_inodes,
    })
}

#[cfg(not(unix))]
fn disk_fact(_path: &Path) -> Option<DiskFact> {
    None
}

fn path_checks(config: &Config) -> Vec<Check> {
    let paths = config.paths();
    let mut out = Vec::new();
    let show = |p: &Path| p.display().to_string();
    out.push(Check::from_verdict(
        "paths",
        "root",
        judge_dir(
            &show(&paths.root_dir),
            DirRole::Root,
            &dir_fact(&paths.root_dir),
        ),
    ));
    out.push(Check::from_verdict(
        "paths",
        "config",
        judge_dir(
            &show(&paths.config_dir),
            DirRole::Config,
            &dir_fact(&paths.config_dir),
        ),
    ));
    for dir in &paths.plugins_dirs {
        out.push(Check::from_verdict(
            "paths",
            "plugins",
            judge_dir(&show(dir), DirRole::Plugins, &dir_fact(dir)),
        ));
    }
    let temp = std::env::temp_dir();
    for (name, path) in [("disk: root", paths.root_dir.clone()), ("disk: temp", temp)] {
        let existing = if path.exists() {
            path.clone()
        } else {
            PathBuf::from(".")
        };
        match disk_fact(&existing) {
            Some(fact) => out.push(Check::from_verdict(
                "paths",
                name,
                judge_disk(&show(&path), &fact),
            )),
            None => out.push(Check::skip(
                "paths",
                name,
                "free space is not available on this platform",
            )),
        }
    }
    out
}

fn read_config_files(dir: &Path) -> Vec<ConfigFile> {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files: Vec<ConfigFile> = entries
        .flatten()
        .filter_map(|e| {
            let path = e.path();
            let ext = path.extension()?.to_str()?.to_lowercase();
            if !matches!(ext.as_str(), "yaml" | "yml" | "json") || !path.is_file() {
                return None;
            }
            let name = path.file_name()?.to_string_lossy().into_owned();
            let stem = path.file_stem()?.to_string_lossy().into_owned();
            let parsed = std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|text| {
                    if ext == "json" {
                        Value::from_json_str(&text).map_err(|e| e.to_string())
                    } else {
                        meerschaum_core::yaml::load(&text).map_err(|e| e.0)
                    }
                });
            Some(match parsed {
                Ok(value) => ConfigFile {
                    name,
                    stem,
                    error: None,
                    value: Some(value),
                },
                Err(error) => ConfigFile {
                    name,
                    stem,
                    error: Some(first_line(&error)),
                    value: None,
                },
            })
        })
        .collect();
    files.sort_by(|a, b| a.name.cmp(&b.name));
    files
}

fn config_checks(
    config: &Config,
    vars: &[(String, String)],
    env: fn(&str) -> Option<String>,
    instance_keys: &str,
    explicit_instance: bool,
    custom_types: &[String],
) -> Vec<Check> {
    let mut out = Vec::new();
    let defaults = config.defaults();
    let known_top: Vec<String> = defaults.keys().cloned().collect();
    let known_meerschaum: Vec<String> = defaults
        .get("meerschaum")
        .and_then(Value::as_map)
        .map(|m| m.keys().cloned().collect())
        .unwrap_or_default();
    let files = read_config_files(&config.paths().config_dir);
    for (name, verdict) in judge_config(&files, &known_top, &known_meerschaum) {
        out.push(Check::from_verdict("config", name, verdict));
    }
    let (connector_vars, other_vars) = split_env_vars(
        &meerschaum_core::config::get_connector_env_vars(vars),
        custom_types,
    );
    let mut env_detail = if connector_vars.is_empty() {
        "no MRSM_<TYPE>_<LABEL> connector variables".to_string()
    } else {
        format!("connector variables: {}", connector_vars.join(", "))
    };
    if !other_vars.is_empty() {
        env_detail.push_str(&format!(
            "; other MRSM_ variables: {}",
            other_vars.join(", ")
        ));
    }
    let bad_patch = ["MRSM_CONFIG", "MRSM_PATCH"].iter().find_map(|var| {
        env(var)
            .filter(|v| !v.trim().is_empty())
            .and_then(|v| serde_json::from_str::<Json>(&v).err().map(|e| (*var, e)))
    });
    match bad_patch {
        Some((var, e)) => out.push(Check::warn(
            "config",
            "environment",
            format!("{env_detail}; {var} is not valid JSON and is ignored ({e})"),
            format!("fix the JSON in {var}"),
        )),
        None => {
            env_detail.push_str("; values are never printed");
            out.push(Check::pass("config", "environment", env_detail));
        }
    }
    out.push(Check::pass(
        "config",
        "instance",
        if explicit_instance {
            format!("instance is {instance_keys} (`-i`)")
        } else {
            format!("default instance is {instance_keys}")
        },
    ));
    out
}

fn driver_checks(
    rows: &[super::drivers::DriverRow],
    connectors: &[(String, String)],
    instance_label: Option<&str>,
    auto_install: bool,
    host: Option<&DuckdbHostDiagnostic>,
) -> Vec<Check> {
    let mut checks: Vec<Check> = rows
        .iter()
        .map(|row| {
            let fact = DriverFact {
                name: row.name,
                source: row.source.clone(),
                version: row.version.clone(),
                path: row.path.clone(),
            };
            let needed = driver_users(connectors, row.name);
            Check::from_verdict(
                "drivers",
                row.name,
                judge_driver(&fact, &needed, instance_label, auto_install),
            )
        })
        .collect();
    if let Some(host) = host
        && !matches!(host, DuckdbHostDiagnostic::Native)
    {
        let needed = driver_users(connectors, "duckdb");
        let needed_instance = instance_label.is_some_and(|label| needed.contains(&label));
        checks.push(Check::from_verdict(
            "drivers",
            "duckdb",
            judge_duckdb_host(host, !needed.is_empty(), needed_instance),
        ));
    }
    checks
}

/// The extra table is the packaging-checked source of the minimum, not another diagnostic
/// version literal. DuckDB Engine is optional for Pipe-only public-driver transport.
fn duckdb_requirement() -> &'static str {
    crate::extras::EXTRAS
        .iter()
        .find(|(name, _)| *name == "sql-duckdb")
        .and_then(|(_, requirements)| {
            requirements.iter().find_map(|requirement| {
                requirement.strip_prefix("duckdb").filter(|rest| {
                    rest.chars()
                        .next()
                        .is_some_and(|c| matches!(c, '>' | '<' | '=' | '!' | '~'))
                })
            })
        })
        .expect("sql-duckdb extra specifies its driver minimum")
}

/// Host metadata is never represented as a successful import or database connection.
fn judge_duckdb_host(host: &DuckdbHostDiagnostic, needed: bool, needed_instance: bool) -> Verdict {
    let problem = |detail: String, fix: &str| {
        if !needed {
            Verdict::skip(detail)
        } else if needed_instance {
            Verdict::fail(detail, fix)
        } else {
            Verdict::warn(detail, fix)
        }
    };
    match host {
        DuckdbHostDiagnostic::Native => Verdict::skip("native transport uses the library checks"),
        DuckdbHostDiagnostic::Refused(reason) => problem(
            reason.clone(),
            "restart with a fresh interpreter (spawn or exec); keep MRSM_DUCKDB_LIB unchanged after selecting a transport",
        ),
        DuckdbHostDiagnostic::Python {
            version,
            path,
            pinned,
        } => {
            let policy = if *pinned {
                "selected"
            } else {
                "prospective (policy unchanged)"
            };
            let location = path
                .as_deref()
                .map(|p| format!(" at {p}"))
                .unwrap_or_default();
            let unverified = "; driver import and database connection were not probed";
            let Some(version) = version else {
                return Verdict::warn(
                    format!(
                        "Python DuckDB {policy}; installed version metadata unavailable{unverified}"
                    ),
                    "install or repair the sql-duckdb extra and restart the interpreter",
                );
            };
            let requirement = duckdb_requirement();
            match meerschaum_core::version::version_satisfies(version, requirement) {
                Some(true) => Verdict::pass(format!(
                    "Python DuckDB {version} metadata{location}; {policy}{unverified}"
                )),
                Some(false) => problem(
                    format!(
                        "Python DuckDB {version} metadata{location} does not meet duckdb{requirement}{unverified}"
                    ),
                    "upgrade the sql-duckdb extra and restart the interpreter",
                ),
                None => Verdict::warn(
                    format!(
                        "Python DuckDB version metadata {version:?} cannot be checked against duckdb{requirement}{unverified}"
                    ),
                    "repair the sql-duckdb extra and restart the interpreter",
                ),
            }
        }
    }
}

fn sql_connectors(config: &Config) -> Vec<(String, String)> {
    let (_, section) = config.get_with(&["meerschaum", "connectors", "sql"], config_opts());
    section
        .as_map()
        .map(|m| {
            m.keys()
                .filter(|label| *label != "default")
                .map(|label| {
                    let attrs = if let Some(v) = m.get(label).and_then(Value::as_map) {
                        v.clone()
                    } else {
                        Map::new()
                    };
                    (label.clone(), flavor_of(&attrs))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn jobs_dir_facts(jobs_dir: &Path) -> Vec<JobFact> {
    let Ok(entries) = std::fs::read_dir(jobs_dir) else {
        return Vec::new();
    };
    let mut jobs: Vec<JobFact> = entries
        .flatten()
        .filter(|e| e.path().join("properties.json").is_file())
        .map(|e| {
            let dir = e.path();
            let pid = std::fs::read_to_string(dir.join("process.pid"))
                .ok()
                .and_then(|t| t.trim().parse::<i32>().ok());
            let process = pid.map(|pid| match meerschaum_jobs::process::proc_state(pid) {
                meerschaum_jobs::process::ProcState::Alive => ProcessState::Alive,
                meerschaum_jobs::process::ProcState::Stopped => ProcessState::Paused,
                _ => ProcessState::Gone,
            });
            let result = std::fs::read_to_string(dir.join("properties.json"))
                .ok()
                .and_then(|t| serde_json::from_str::<Json>(&t).ok())
                .and_then(|p| p.get("result").cloned())
                .and_then(|r| {
                    let list = r.as_array()?;
                    Some((
                        list.first()?.as_bool()?,
                        list.get(1).and_then(Json::as_str).unwrap_or("").to_string(),
                    ))
                });
            JobFact {
                name: e.file_name().to_string_lossy().into_owned(),
                pid,
                process,
                result,
            }
        })
        .collect();
    jobs.sort_by(|a, b| a.name.cmp(&b.name));
    jobs
}

fn venv_facts(venvs_dir: &Path) -> Vec<VenvFact> {
    let Ok(entries) = std::fs::read_dir(venvs_dir) else {
        return Vec::new();
    };
    let mut venvs: Vec<VenvFact> = entries
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| {
            let root = e.path();
            let sites = super::venvs::site_packages(&root);
            if sites.is_empty() {
                return None;
            }
            let minors = sites
                .iter()
                .filter_map(|s| {
                    let version = s.parent()?.file_name()?.to_string_lossy().into_owned();
                    parse_major_minor(version.strip_prefix("python")?)
                })
                .collect();
            let bin = root.join(if cfg!(windows) { "Scripts" } else { "bin" });
            let dangling = std::fs::read_dir(&bin)
                .map(|entries| {
                    entries
                        .flatten()
                        .filter(|b| {
                            let name = b.file_name().to_string_lossy().into_owned();
                            name.starts_with("python")
                                && std::fs::symlink_metadata(b.path())
                                    .is_ok_and(|m| m.file_type().is_symlink())
                                && std::fs::metadata(b.path()).is_err()
                        })
                        .map(|b| b.file_name().to_string_lossy().into_owned())
                        .collect()
                })
                .unwrap_or_default();
            Some(VenvFact {
                name: e.file_name().to_string_lossy().into_owned(),
                minors,
                dangling,
            })
        })
        .collect();
    venvs.sort_by(|a, b| a.name.cmp(&b.name));
    venvs
}

fn extras_checks(
    probe: &Result<PyProbe, String>,
    facts: Option<&PluginFacts>,
    python: &str,
) -> Vec<Check> {
    let Ok(p) = probe else {
        return vec![Check::skip(
            "extras",
            "python extras",
            "no Python interpreter to ask",
        )];
    };
    let interpreter = if p.executable.is_empty() {
        python
    } else {
        &p.executable
    };
    EXTRA_MODULES
        .iter()
        .map(|(extra, modules)| {
            let mods: Vec<(&str, Option<&ModuleProbe>)> =
                modules.iter().map(|m| (*m, p.modules.get(*m))).collect();
            let needed_by: Vec<String> = match (*extra, facts) {
                ("api", Some(f)) => f.needs_api.clone(),
                ("dash", Some(f)) => f.needs_dash.clone(),
                _ => Vec::new(),
            };
            Check::from_verdict(
                "extras",
                *extra,
                judge_extra(extra, &mods, &needed_by, interpreter),
            )
        })
        .collect()
}

fn plugin_checks(
    manifest: &Result<Arc<Manifest>, crate::builtin::ActionError>,
    facts: Option<&PluginFacts>,
    cache: &Verdict,
    venvs: &[VenvFact],
    probe: &Result<PyProbe, String>,
) -> Vec<Check> {
    let mut out = vec![Check::from_verdict("plugins", "manifest", cache.clone())];
    match (manifest, facts) {
        (Err(e), _) => out.push(Check::warn(
            "plugins",
            "manifest read",
            format!(
                "the plugin manifest could not be read: {}",
                first_line(&e.message)
            ),
            "`mrsm reload` rebuilds it; `mrsm verify plugins` shows the import errors",
        )),
        (_, Some(f)) => {
            for (name, verdict) in judge_plugins(f) {
                out.push(Check::from_verdict("plugins", name, verdict));
            }
        }
        _ => {}
    }
    let worker = probe
        .as_ref()
        .ok()
        .and_then(|p| parse_major_minor(&p.version));
    out.push(Check::from_verdict(
        "plugins",
        "venvs",
        judge_venvs(venvs, worker),
    ));
    out
}

fn job_checks(jobs_dir: &Path) -> Vec<Check> {
    let facts = jobs_dir_facts(jobs_dir);
    judge_jobs(&facts)
        .into_iter()
        .map(|(name, verdict)| Check::from_verdict("jobs", name, verdict))
        .collect()
}

fn gather_stack(config: &Config, path: Option<String>) -> Vec<Check> {
    let compose = config.paths().path("STACK_COMPOSE_PATH");
    if !compose.exists() {
        return vec![Check::skip(
            "stack",
            "services",
            "no stack file (`mrsm stack` writes it on first use)",
        )];
    }
    let Some(docker) = super::pip::which("docker", path.as_deref()) else {
        return vec![Check::skip(
            "stack",
            "services",
            "the `docker` command was not found",
        )];
    };
    let (_, project) = config.get_with(&["stack", "project_name"], config_opts());
    let project = project.as_str().unwrap_or("mrsm").to_string();
    // `docker ps` with the compose project label: read-only, needs no compose file parsing and
    // never prints a container's environment.
    let spec = CommandSpec {
        program: docker.to_string_lossy().into_owned(),
        args: vec![
            "ps".into(),
            "-a".into(),
            "--filter".into(),
            format!("label=com.docker.compose.project={project}"),
            "--format".into(),
            "{{.Label \"com.docker.compose.service\"}}\t{{.Names}}\t{{.Image}}\t{{.State}}\t{{.Status}}"
                .into(),
        ],
        ..CommandSpec::default()
    };
    let out = match super::terminal::capture(&spec, Some(Duration::from_secs(4))) {
        Ok(o) if !o.timed_out && o.code == 0 => o,
        Ok(o) => {
            let why = if o.timed_out {
                "did not answer within 4 seconds".to_string()
            } else {
                first_line(&String::from_utf8_lossy(&o.stderr))
            };
            return vec![Check::skip(
                "stack",
                "services",
                format!("Docker does not answer ({why})"),
            )];
        }
        Err(e) => {
            return vec![Check::skip(
                "stack",
                "services",
                format!("Docker does not answer ({e})"),
            )];
        }
    };
    let services = parse_services(&String::from_utf8_lossy(&out.stdout));
    if services.is_empty() {
        return vec![Check::skip(
            "stack",
            "services",
            format!("no containers for project '{project}' (`mrsm stack up -d` starts them)"),
        )];
    }
    services
        .iter()
        .map(|s| {
            Check::from_verdict(
                "stack",
                format!("service: {}", s.service),
                judge_service(s, &project),
            )
        })
        .collect()
}

/// Whether DuckDB's library can be loaded (`show drivers` logic), and whether a missing one
/// would be downloaded on first use.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DuckdbGate {
    /// Why the library cannot be loaded (`None`: it is found).
    pub problem: Option<String>,
    /// `MRSM_NO_AUTO_INSTALL` is not set.
    pub auto_install: bool,
    /// Side-effect-free embedding-host facts; no host is present in standalone processes.
    pub host: Option<DuckdbHostDiagnostic>,
}

/// The instance is a DuckDB database but its library is missing: connecting would load it, and
/// loading downloads and installs it on first use, so the doctor does not connect. `None`: go on.
pub fn judge_duckdb_gate(g: &DuckdbGate) -> Option<Verdict> {
    let problem = g.problem.as_ref()?;
    let fix = "`mrsm install driver duckdb` (or set MRSM_DUCKDB_LIB to a libduckdb you provide)";
    Some(if g.auto_install && !problem.starts_with("not loadable") {
        Verdict::warn(
            format!(
                "not probed: DuckDB's library is {problem}; it is downloaded on first use and the doctor never installs"
            ),
            fix,
        )
    } else {
        Verdict::fail(
            format!("the instance is a DuckDB database but its library is {problem}"),
            fix,
        )
    })
}

/// The instance group. Returns the checks and, for a reachable `sql:` instance, its connector.
fn gather_instance(
    session: &super::Session,
    config: &Config,
    keys: &str,
    duckdb: &DuckdbGate,
) -> (Vec<Check>, Option<Arc<SqlConnector>>) {
    use super::session::split_keys;
    let (typ, label) = split_keys(keys);
    let (valid, section) =
        config.get_with(&["meerschaum", "connectors", &typ, &label], config_opts());
    let defined = valid && section.as_map().is_some_and(|m| !m.is_empty());
    if !defined && matches!(typ.as_str(), "sql" | "api" | "valkey") {
        let var = format!("MRSM_{}_{}", typ.to_uppercase(), label.to_uppercase());
        return (
            vec![Check::fail(
                "instance",
                "connection",
                format!(
                    "the instance '{typ}:{label}' is not defined: no connector section and no {var} variable"
                ),
                format!(
                    "define it (`mrsm bootstrap connector {typ}:{label}`, or set {var}='<uri>'), or choose an existing instance with `-i` or `mrsm edit config meerschaum`"
                ),
            )],
            None,
        );
    }
    match typ.as_str() {
        "sql" => gather_sql(session, config, &label, keys, duckdb),
        "api" => (gather_api(session, config, &label), None),
        "valkey" => (gather_valkey(session, config, &label), None),
        _ => (
            vec![Check::skip(
                "instance",
                "connection",
                format!(
                    "'{keys}' is a plugin's instance connector: the doctor never imports plugin code"
                ),
            )],
            None,
        ),
    }
}

/// Whether a TCP connection to `host:port` opens within `timeout` (the OS error otherwise).
fn tcp_reachable(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    use std::net::ToSocketAddrs;
    let addrs: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("cannot resolve '{host}': {e}"))?
        .collect();
    let mut last = format!("no address for '{host}'");
    for addr in addrs {
        match std::net::TcpStream::connect_timeout(&addr, timeout) {
            Ok(_) => return Ok(()),
            Err(e) => last = format!("{e}"),
        }
    }
    Err(last)
}

fn gather_sql(
    session: &super::Session,
    config: &Config,
    label: &str,
    keys: &str,
    duckdb: &DuckdbGate,
) -> (Vec<Check>, Option<Arc<SqlConnector>>) {
    use super::session::{connector_attributes, sql_spec};
    let attrs = connector_attributes(config, "sql", label);
    let flavor = flavor_of(&attrs);
    if flavor == "duckdb"
        && let Some(host) = &duckdb.host
    {
        match host {
            DuckdbHostDiagnostic::Python { .. } => {
                return (
                    vec![Check::skip(
                        "instance",
                        "connection",
                        "Python DuckDB transport: database connection not probed to preserve read-only diagnostics (no database or shared anchor was opened)",
                    )],
                    None,
                );
            }
            DuckdbHostDiagnostic::Refused(reason) => {
                return (
                    vec![Check::fail(
                        "instance",
                        "connection",
                        reason.clone(),
                        "restart with a fresh interpreter (spawn or exec)",
                    )],
                    None,
                );
            }
            DuckdbHostDiagnostic::Native => {}
        }
    }
    if matches!(flavor.as_str(), "sqlite" | "geopackage" | "duckdb") {
        let root_db = config
            .paths()
            .path("SQLITE_DB_PATH")
            .to_string_lossy()
            .into_owned();
        let raw = config
            .get(&["meerschaum", "connectors", "sql", label])
            .map(|v| v.to_map())
            .unwrap_or_default();
        if let Some(path) =
            sqlite_path(&raw, label, &root_db).or_else(|| sqlite_path(&attrs, label, &root_db))
            && path != ":memory:"
            && !Path::new(&path).exists()
        {
            return (
                vec![Check::warn(
                    "instance",
                    "connection",
                    format!(
                        "the {flavor} database {path} does not exist yet (nothing was created)"
                    ),
                    "any command that writes to the instance creates it (e.g. `mrsm bootstrap pipe`); check the path if you expected data there",
                )],
                None,
            );
        }
    }
    if flavor == "duckdb"
        && let Some(verdict) = judge_duckdb_gate(duckdb)
    {
        return (
            vec![Check::from_verdict("instance", "connection", verdict)],
            None,
        );
    }
    let spec = match sql_spec(label, config) {
        Ok(spec) => spec,
        Err(e) => {
            return (
                vec![Check::fail(
                    "instance",
                    "connection",
                    format!(
                        "the connector '{keys}' is invalid: {}",
                        first_line(&e.message)
                    ),
                    "fix its attributes (`mrsm edit config meerschaum`): a flavor or a uri, plus host, port, database and credentials",
                )],
                None,
            );
        }
    };
    let target = match (&spec.host, spec.port, &spec.database) {
        (Some(h), Some(p), Some(d)) => format!("{h}:{p}/{d}"),
        (Some(h), None, Some(d)) => format!("{h}/{d}"),
        (_, _, Some(d)) => d.clone(),
        _ => keys.to_string(),
    };
    let flavor = spec.flavor;
    // A closed port answers at once; without this the connection pool retries until its timeout.
    if let (Some(host), Some(port)) = (spec.host.as_deref(), spec.port.or(flavor.default_port()))
        && !host.starts_with('/')
        && let Err(e) = tcp_reachable(host, port, Duration::from_secs(3))
    {
        return (
            vec![Check::fail(
                "instance",
                "connection",
                format!("{keys} ({flavor}) at {target}: {e}"),
                "check the server is running and that the connector's host, port and credentials are right (`mrsm edit config meerschaum`), or choose another instance with `-i`",
            )],
            None,
        );
    }
    let settings =
        meerschaum_connector_sql::instance::InstanceSettings::from_config(flavor, config);
    let conn = {
        let _guard = session.handle().enter();
        let connection = if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
            SqlConnector::connect_lazy_sqlite_read_only(label, spec, settings)
        } else {
            SqlConnector::connect_lazy(label, spec, settings)
        };
        match connection {
            Ok(c) => Arc::new(c),
            Err(e) => {
                return (
                    vec![Check::fail(
                        "instance",
                        "connection",
                        format!("cannot create the connector: {e}"),
                        "check the connector's attributes (`mrsm edit config meerschaum`)",
                    )],
                    None,
                );
            }
        }
    };
    let query = meerschaum_connector_sql::queries::table::get_db_version_query(flavor);
    let reached =
        session.block_on(async { tokio::time::timeout(PROBE_TIMEOUT, conn.read(&query)).await });
    let fix = "check the server is running and that the connector's host, port and credentials are right (`mrsm edit config meerschaum`), or choose another instance with `-i`";
    match reached {
        Err(_) => {
            return (
                vec![Check::fail(
                    "instance",
                    "connection",
                    format!(
                        "{keys} ({flavor}) at {target} did not answer within {} seconds",
                        PROBE_TIMEOUT.as_secs()
                    ),
                    fix,
                )],
                None,
            );
        }
        Ok(Err(e)) => {
            return (
                vec![Check::fail(
                    "instance",
                    "connection",
                    format!(
                        "{keys} ({flavor}) at {target}: {}",
                        first_line(&e.to_string())
                    ),
                    fix,
                )],
                None,
            );
        }
        Ok(Ok(_)) => {}
    }
    let version = session
        .block_on(async { tokio::time::timeout(PROBE_TIMEOUT, conn.db_version()).await })
        .ok()
        .flatten()
        .map(|v| first_line(&v))
        .unwrap_or_else(|| "unknown".into());
    let mut out = vec![
        Check::pass(
            "instance",
            "connection",
            format!("{keys} ({flavor}) at {target}"),
        ),
        Check::pass("instance", "server version", version),
    ];
    let schema = session
        .block_on(async { tokio::time::timeout(PROBE_TIMEOUT, conn.require_schema()).await })
        .ok()
        .and_then(Result::ok)
        .map(|_| conn.instance_schema().map(str::to_string))
        .unwrap_or(None);
    let names = ["mrsm_pipes", "mrsm_users", "mrsm_plugins"];
    let present: Vec<bool> = names
        .iter()
        .map(|t| {
            session
                .block_on(async {
                    tokio::time::timeout(PROBE_TIMEOUT, conn.table_exists(t, schema.as_deref()))
                        .await
                })
                .unwrap_or(false)
        })
        .collect();
    let missing: Vec<&str> = names
        .iter()
        .zip(&present)
        .filter(|(_, p)| !**p)
        .map(|(t, _)| *t)
        .collect();
    out.push(if missing.is_empty() {
        Check::pass("instance", "tables", format!("{} exist", names.join(", ")))
    } else if missing.len() == names.len() {
        Check::warn(
            "instance",
            "tables",
            "no Meerschaum tables yet: this is a new instance (nothing was created)",
            "they are created by the first command that writes (`mrsm bootstrap pipe`, `mrsm register pipes`, `mrsm sync pipes`)",
        )
    } else {
        Check::warn(
            "instance",
            "tables",
            format!("missing: {}", missing.join(", ")),
            "`mrsm register pipes` or any write command creates the missing tables; if they were dropped on purpose, ignore this",
        )
    });
    if let Some(pipe) = conn.engine_state_pipe(keys, "engine_runs") {
        let exists = session
            .block_on(async { tokio::time::timeout(PROBE_TIMEOUT, conn.pipe_exists(&pipe)).await })
            .ok()
            .and_then(Result::ok)
            .unwrap_or(false);
        out.push(Check::pass(
            "instance",
            "engine state",
            if exists {
                "the Syncing Engine's run history exists"
            } else {
                "no engine runs recorded yet (the first graph run creates the state tables)"
            },
        ));
    }
    (out, Some(conn))
}

fn gather_api(session: &super::Session, config: &Config, label: &str) -> Vec<Check> {
    let mut connector = match meerschaum_connector_api::APIConnector::from_config(config, label) {
        Ok(c) => c,
        Err(e) => {
            return vec![Check::fail(
                "instance",
                "connection",
                format!(
                    "the connector 'api:{label}' is invalid: {}",
                    first_line(&e.to_string())
                ),
                "set its host, port and protocol (`mrsm edit config meerschaum`)",
            )];
        }
    };
    connector.set_timeout(PROBE_TIMEOUT);
    let uri = connector.uri();
    let fix = "start the API (`mrsm start api`) or fix the connector's host, port and protocol (`mrsm edit config meerschaum`)";
    let health = session.block_on(async {
        tokio::time::timeout(
            PROBE_TIMEOUT + Duration::from_secs(1),
            connector.healthcheck(),
        )
        .await
    });
    let mut out = Vec::new();
    // The URI carries credentials only as a password the connector keeps separately; print host only.
    let shown = uri.split('@').next_back().unwrap_or(&uri).to_string();
    match health {
        Err(_) => {
            out.push(Check::fail(
                "instance",
                "healthcheck",
                format!(
                    "{shown} did not answer within {} seconds",
                    PROBE_TIMEOUT.as_secs()
                ),
                fix,
            ));
            return out;
        }
        Ok(Err(e)) => {
            out.push(Check::fail(
                "instance",
                "healthcheck",
                format!("{shown}: {}", first_line(&e.to_string())),
                fix,
            ));
            return out;
        }
        Ok(Ok(h)) if h.success => {
            out.push(Check::pass(
                "instance",
                "healthcheck",
                format!("{shown}: {}", h.message),
            ));
        }
        Ok(Ok(h)) => {
            out.push(Check::fail(
                "instance",
                "healthcheck",
                format!(
                    "{shown} answers, but its own instance does not: {}",
                    first_line(&h.message)
                ),
                "check the database behind the API server (run `mrsm doctor` on that host)",
            ));
        }
    }
    let version = session
        .block_on(async { tokio::time::timeout(PROBE_TIMEOUT, connector.get_mrsm_version()).await })
        .ok()
        .flatten();
    out.push(Check::from_verdict(
        "instance",
        "version",
        judge_api_version(version.as_deref(), crate::VERSION),
    ));
    out
}

fn gather_valkey(session: &super::Session, config: &Config, label: &str) -> Vec<Check> {
    let attrs = meerschaum_connector_valkey::ValkeyConnector::attributes_from_config(config, label);
    let result = session.block_on(async {
        tokio::time::timeout(
            PROBE_TIMEOUT,
            meerschaum_connector_valkey::ValkeyConnector::connect(label, attrs),
        )
        .await
    });
    match result {
        Ok(Ok(_)) => vec![Check::pass(
            "instance",
            "connection",
            format!("valkey:{label} connected"),
        )],
        Ok(Err(e)) => vec![Check::fail(
            "instance",
            "connection",
            format!("valkey:{label}: {}", first_line(&e.to_string())),
            "check the server is running and the connector's host, port and password (`mrsm edit config meerschaum`)",
        )],
        Err(_) => vec![Check::fail(
            "instance",
            "connection",
            format!(
                "valkey:{label} did not answer within {} seconds",
                PROBE_TIMEOUT.as_secs()
            ),
            "check the server is running and the connector's host and port",
        )],
    }
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
mod tests {
    use super::*;

    /// The probe prints one line of JSON; the fixtures below are wrapped for reading.
    fn probe(json: &str) -> Result<PyProbe, String> {
        parse_probe(&json.replace('\n', " "))
    }

    const OK_PROBE: &str = r#"{"version": "3.13.1", "executable": "/usr/bin/python3",
        "prefix": "/usr", "base_prefix": "/usr",
        "modules": {"pandas": {"found": true, "version": "3.0.1"},
                    "fastapi": {"found": false, "version": null}},
        "meerschaum": {"found": true, "dir": "/site/meerschaum", "version": "5.0.0rc1",
                       "copies": [{"dir": "/site/meerschaum", "version": "5.0.0rc1"}],
                       "user_site": "/home/u/.local/lib/python3.13/site-packages"}}"#;

    #[test]
    fn a_missing_unique_key_names_the_duplicates_and_the_rows_to_delete() {
        use meerschaum_connector_sql::instance::unique_keys::USERS_KEY;
        let ok = |status| judge_unique_key(&PIPES_KEY, &Ok(status), "mrsm_pipes");
        assert_eq!(ok(KeyStatus::Present).status, Status::Pass);
        assert_eq!(ok(KeyStatus::NoTable).status, Status::Skip);
        let pending = ok(KeyStatus::Missing { duplicates: vec![] });
        assert_eq!(pending.status, Status::Warn);
        let dup = |values: &[Option<&str>], ids: &[i64]| Duplicate {
            values: values.iter().map(|v| v.map(str::to_string)).collect(),
            ids: ids.to_vec(),
        };
        let v = ok(KeyStatus::Missing {
            duplicates: vec![
                dup(&[Some("sql:a"), Some("m"), None], &[3, 7, 9]),
                dup(&[Some("sql:a"), Some("m"), Some("x")], &[4, 8]),
            ],
        });
        assert_eq!(v.status, Status::Warn);
        assert!(
            v.detail
                .starts_with("2 pipe(s) are registered more than once, so mrsm_pipes"),
            "{}",
            v.detail
        );
        assert!(
            v.detail.contains("Pipe('sql:a', 'm') (pipe_id 3, 7, 9)"),
            "{}",
            v.detail
        );
        assert!(
            v.detail.contains("Pipe('sql:a', 'm', 'x') (pipe_id 4, 8)"),
            "{}",
            v.detail
        );
        let fix = v.fix.unwrap();
        assert!(
            fix.contains("DELETE FROM mrsm_pipes WHERE pipe_id IN (7, 9, 8)"),
            "{fix}"
        );
        let users = judge_unique_key(
            &USERS_KEY,
            &Ok(KeyStatus::Missing {
                duplicates: vec![dup(&[Some("alice")], &[2, 5])],
            }),
            "mrsm_users",
        );
        assert!(
            users
                .detail
                .contains("1 user(s) are registered more than once, so mrsm_users has no unique key on the username: 'alice' (user_id 2, 5)"),
            "{}",
            users.detail
        );
        assert!(
            users
                .fix
                .unwrap()
                .contains("DELETE FROM mrsm_users WHERE user_id IN (5)")
        );
        assert_eq!(unique_key_check_name(&PIPES_KEY), "pipes key");
        assert_eq!(unique_key_check_name(&USERS_KEY), "users key");
        let err = judge_unique_key(&PIPES_KEY, &Err("timeout".into()), "mrsm_pipes");
        assert_eq!(err.status, Status::Warn);
    }

    #[test]
    fn statuses_print_and_sort() {
        assert_eq!(Status::Pass.as_str(), "PASS");
        assert_eq!(Status::Fail.as_str(), "FAIL");
        assert!(Status::Pass < Status::Warn && Status::Warn < Status::Fail);
    }

    #[test]
    fn human_bytes_uses_binary_units() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.5 KB");
        assert_eq!(human_bytes(GIB), "1.0 GB");
        assert_eq!(human_bytes(5 * GIB / 2), "2.5 GB");
    }

    #[test]
    fn versions_compare_as_pep_440() {
        assert!(versions_match("5.0.0-rc.1", "5.0.0rc1"));
        assert!(versions_match("5.0.0-a1", "5.0.0a1"));
        assert!(!versions_match("4.1.4", "5.0.0rc1"));
        assert!(!versions_match("5.0.0rc1", "5.0.0"));
        assert!(versions_match("not a version", "not a version"));
    }

    #[test]
    fn python_versions_parse() {
        assert_eq!(parse_major_minor("3.13.1"), Some((3, 13)));
        assert_eq!(parse_major_minor("3.14.0rc2"), Some((3, 14)));
        assert_eq!(parse_major_minor("3"), None);
        assert!(parse_major_minor("3.10.9").unwrap() < MIN_PYTHON);
    }

    #[test]
    fn the_probe_output_is_read_from_its_last_json_line() {
        let noisy = format!(
            "a banner from sitecustomize\n{}\n",
            OK_PROBE.replace('\n', " ")
        );
        let p = parse_probe(&noisy).unwrap();
        assert_eq!(p.version, "3.13.1");
        assert!(p.modules["pandas"].found);
        assert_eq!(p.modules["pandas"].version.as_deref(), Some("3.0.1"));
        assert!(!p.modules["fastapi"].found);
        assert_eq!(p.meerschaum.copies.len(), 1);
        assert!(probe("nothing useful").is_err());
        assert!(probe("{\"executable\": \"x\"}").is_err());
    }

    #[test]
    fn the_interpreter_must_exist_and_be_new_enough() {
        let ok = probe(OK_PROBE);
        assert_eq!(judge_interpreter(&ok, "python3", 0).status, Status::Pass);
        let old = probe(&OK_PROBE.replace("3.13.1", "3.9.2"));
        let v = judge_interpreter(&old, "python3", 0);
        assert_eq!(v.status, Status::Fail);
        assert!(v.fix.unwrap().contains("MRSM_PYTHON"));
        let missing: Result<PyProbe, String> = Err("No such file".into());
        assert_eq!(
            judge_interpreter(&missing, "python3", 0).status,
            Status::Warn
        );
        // A Python plugin cannot run without it.
        assert_eq!(
            judge_interpreter(&missing, "python3", 2).status,
            Status::Fail
        );
    }

    #[test]
    fn a_matching_package_passes() {
        let v = judge_package(&probe(OK_PROBE), "5.0.0-rc.1", 1, 0);
        assert_eq!(v.status, Status::Pass, "{v:?}");
        assert!(v.detail.contains("/site/meerschaum"));
    }

    #[test]
    fn a_stale_package_is_a_failure_naming_its_path() {
        let stale = OK_PROBE
            .replace("\"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\",\n", "\"dir\": \"/home/u/.local/lib/python3.13/site-packages/meerschaum\", \"version\": \"4.1.4\",\n");
        // With nothing that runs in that interpreter, a stale copy is a warning.
        let idle = judge_package(&probe(&stale), "5.0.0-rc.1", 1, 0);
        assert_eq!(idle.status, Status::Warn, "{idle:?}");
        assert!(idle.detail.contains("only plugins need it"));
        let v = judge_package(&probe(&stale), "5.0.0-rc.1", 1, 2);
        assert_eq!(v.status, Status::Fail, "{v:?}");
        assert!(v.detail.contains("2 Python plugins cannot run"));
        assert!(v.detail.contains("4.1.4"));
        assert!(
            v.detail
                .contains(".local/lib/python3.13/site-packages/meerschaum")
        );
        assert!(v.detail.contains("user site"));
        assert!(v.fix.unwrap().contains("MRSM_PYTHON"));
    }

    fn with_package(fields: &str) -> Result<PyProbe, String> {
        probe(&OK_PROBE.replace(
            "\"found\": true, \"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\",",
            &format!("\"found\": true, \"dir\": \"/site/meerschaum\", {fields},"),
        ))
    }

    #[test]
    fn another_release_of_the_same_major_warns_and_starts() {
        let p = with_package("\"version\": \"5.0.3\", \"has_worker\": true, \"protocol\": 1");
        for plugins in [0, 4] {
            let v = judge_package(&p, "5.0.0-rc.1", 1, plugins);
            assert_eq!(v.status, Status::Warn, "{v:?}");
            assert!(v.detail.contains("5.0.3") && v.detail.contains("5.0.0-rc.1"));
            assert!(v.detail.contains("the worker starts"));
            assert!(v.fix.unwrap().contains("pip install 'meerschaum=="));
        }
    }

    #[test]
    fn another_worker_protocol_is_refused() {
        let p = with_package("\"version\": \"5.0.0rc1\", \"has_worker\": true, \"protocol\": 2");
        let idle = judge_package(&p, "5.0.0rc1", 1, 0);
        assert_eq!(idle.status, Status::Warn);
        assert!(idle.detail.contains("worker protocol 2") && idle.detail.contains("refuses"));
        assert_eq!(judge_package(&p, "5.0.0rc1", 1, 1).status, Status::Fail);
    }

    #[test]
    fn a_copy_that_looks_like_v4_is_refused() {
        let p = with_package("\"version\": null, \"has_worker\": false, \"protocol\": null");
        let v = judge_package(&p, "5.0.0rc1", 1, 1);
        assert_eq!(v.status, Status::Fail, "{v:?}");
        assert!(v.detail.contains("looks like Meerschaum 4"));
    }

    #[test]
    fn the_advice_depends_on_whether_another_copy_exists() {
        let stale = "\"version\": \"4.1.4\", \"has_worker\": false, \"protocol\": null";
        // The only copy: install the matching release.
        let only = judge_package(&with_package(stale), "5.0.0rc1", 1, 1);
        let fix = only.fix.unwrap();
        assert!(fix.starts_with("install the matching release"), "{fix}");
        assert!(!fix.contains("uninstall"));
        // Another copy behind it: uninstall this one.
        let two = OK_PROBE.replace(
            "\"found\": true, \"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\",",
            &format!("\"found\": true, \"dir\": \"/site/meerschaum\", {stale},"),
        ).replace(
            "\"copies\": [{\"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\"}]",
            "\"copies\": [{\"dir\": \"/site/meerschaum\", \"version\": \"4.1.4\"}, {\"dir\": \"/other/meerschaum\", \"version\": \"5.0.0rc1\"}]",
        );
        let fix = judge_package(&probe(&two), "5.0.0rc1", 1, 1).fix.unwrap();
        assert!(
            fix.starts_with("uninstall this copy (delete /site/meerschaum"),
            "{fix}"
        );
    }

    #[test]
    fn extra_copies_on_the_path_warn() {
        let two = OK_PROBE.replace(
            "\"copies\": [{\"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\"}]",
            "\"copies\": [{\"dir\": \"/site/meerschaum\", \"version\": \"5.0.0rc1\"}, {\"dir\": \"/other/meerschaum\", \"version\": \"5.0.0rc1\"}]",
        );
        let v = judge_package(&probe(&two), "5.0.0rc1", 1, 0);
        assert_eq!(v.status, Status::Warn);
        assert!(v.detail.contains("/other/meerschaum"));
    }

    #[test]
    fn a_missing_package_fails_only_when_plugins_need_it() {
        let none = OK_PROBE.replace(
            "\"found\": true, \"dir\": \"/site/meerschaum\"",
            "\"found\": false, \"dir\": null",
        );
        assert_eq!(
            judge_package(&probe(&none), "5.0.0rc1", 1, 0).status,
            Status::Warn
        );
        assert_eq!(
            judge_package(&probe(&none), "5.0.0rc1", 1, 1).status,
            Status::Fail
        );
        let err: Result<PyProbe, String> = Err("x".into());
        assert_eq!(judge_package(&err, "5.0.0rc1", 1, 3).status, Status::Skip);
    }

    #[test]
    fn the_manifest_is_judged_without_being_built() {
        let none: Vec<String> = Vec::new();
        assert_eq!(
            judge_manifest(false, false, &none, 0, 0).status,
            Status::Pass
        );
        let missing = judge_manifest(false, false, &none, 2, 0);
        assert_eq!(missing.status, Status::Warn);
        assert!(missing.detail.contains("missing") && missing.fix.unwrap().contains("mrsm reload"));
        let stale = judge_manifest(true, true, &["a".to_string(), "b".to_string()], 2, 2);
        assert_eq!(stale.status, Status::Warn);
        assert!(stale.detail.contains("stale (changed: a, b)"));
        assert_eq!(
            judge_manifest(true, false, &none, 1, 1).status,
            Status::Pass
        );
    }

    #[test]
    fn plugins_not_in_the_manifest_are_not_reported_as_imported() {
        let mut f = plugin_facts(&manifest(MANIFEST));
        f.import_errors.clear();
        f.load_errors.clear();
        f.names.push("fresh".into());
        f.unbuilt.push("fresh".into());
        let checks = judge_plugins(&f);
        let imports = &checks.iter().find(|(n, _)| *n == "imports").unwrap().1;
        assert_eq!(imports.status, Status::Skip);
        assert!(imports.detail.contains("fresh"));
    }

    #[test]
    fn connector_variables_are_told_from_lookalikes() {
        let names: Vec<String> = [
            "MRSM_SQL_MAIN",
            "MRSM_API_PROD",
            "MRSM_VALKEY_CACHE",
            "MRSM_PLUGIN_TOOLS",
            "MRSM_SYSTEMD_UNITS",
            "MRSM_DUCKDB_LIB",
            "MRSM_FOO_BAR",
            "MRSM_TEST_PG_URI",
        ]
        .map(String::from)
        .to_vec();
        let (connectors, others) = split_env_vars(&names, &["foo".to_string()]);
        assert_eq!(
            connectors,
            [
                "MRSM_SQL_MAIN",
                "MRSM_API_PROD",
                "MRSM_VALKEY_CACHE",
                "MRSM_PLUGIN_TOOLS",
                "MRSM_SYSTEMD_UNITS",
                "MRSM_FOO_BAR"
            ]
        );
        assert_eq!(others, ["MRSM_DUCKDB_LIB", "MRSM_TEST_PG_URI"]);
    }

    #[test]
    fn python_duckdb_metadata_never_claims_import_or_connection_success() {
        let fact = |version: Option<&str>| DuckdbHostDiagnostic::Python {
            version: version.map(str::to_string),
            path: Some("/site".into()),
            pinned: false,
        };
        let ready = judge_duckdb_host(&fact(Some("99.0.0")), true, true);
        assert_eq!(ready.status, Status::Pass);
        assert!(ready.detail.contains("metadata") && ready.detail.contains("not probed"));
        assert!(ready.detail.contains("policy unchanged"));
        assert_eq!(
            judge_duckdb_host(&fact(None), true, true).status,
            Status::Warn
        );
        let old = judge_duckdb_host(&fact(Some("0.1.0")), true, true);
        assert_eq!(old.status, Status::Fail);
        assert!(old.detail.contains(duckdb_requirement()));
        assert!(old.fix.unwrap().contains("restart"));
        assert_eq!(
            judge_duckdb_host(&fact(Some("0.1.0")), true, false).status,
            Status::Warn
        );
        assert_eq!(
            judge_duckdb_host(&fact(Some("0.1.0")), false, false).status,
            Status::Skip
        );
        let refused = DuckdbHostDiagnostic::Refused("fresh interpreter required".into());
        assert_eq!(judge_duckdb_host(&refused, true, true).status, Status::Fail);
    }

    #[test]
    fn duckdb_is_never_probed_without_its_library() {
        let gate = |problem: Option<&str>, auto_install| DuckdbGate {
            problem: problem.map(str::to_string),
            auto_install,
            host: None,
        };
        assert!(judge_duckdb_gate(&gate(None, true)).is_none());
        let first_use = judge_duckdb_gate(&gate(Some("not installed"), true)).unwrap();
        assert_eq!(first_use.status, Status::Warn);
        assert!(
            first_use.detail.contains("not probed") && first_use.detail.contains("never installs")
        );
        assert!(first_use.fix.unwrap().contains("install driver duckdb"));
        assert_eq!(
            judge_duckdb_gate(&gate(Some("not installed"), false))
                .unwrap()
                .status,
            Status::Fail
        );
        assert_eq!(
            judge_duckdb_gate(&gate(Some("not loadable: bad ELF"), true))
                .unwrap()
                .status,
            Status::Fail
        );
    }

    #[test]
    fn disk_thresholds() {
        let fact = |free: u64, inodes: Option<(u64, u64)>| DiskFact {
            free_bytes: free,
            total_bytes: 100 * GIB,
            free_inodes: inodes.map(|i| i.0),
            total_inodes: inodes.map(|i| i.1),
        };
        assert_eq!(
            judge_disk("/", &fact(50 * GIB, Some((90, 100)))).status,
            Status::Pass
        );
        assert_eq!(judge_disk("/", &fact(GIB - 1, None)).status, Status::Warn);
        assert_eq!(judge_disk("/", &fact(GIB, None)).status, Status::Pass);
        assert_eq!(judge_disk("/", &fact(10 * MIB, None)).status, Status::Fail);
        // A tmpfs near its inode limit: plenty of bytes, few inodes.
        assert_eq!(
            judge_disk("/tmp", &fact(50 * GIB, Some((4, 100)))).status,
            Status::Warn
        );
        assert_eq!(
            judge_disk("/tmp", &fact(50 * GIB, Some((0, 100)))).status,
            Status::Fail
        );
        assert_eq!(
            judge_disk("/", &fact(50 * GIB, Some((0, 0)))).status,
            Status::Pass
        );
        let warned = judge_disk("/tmp", &fact(50 * GIB, Some((4, 100))));
        assert!(warned.detail.contains("4% free"), "{}", warned.detail);
    }

    #[test]
    fn directories() {
        let fact = |exists, is_dir, writable| DirFact {
            exists,
            is_dir,
            writable,
        };
        assert_eq!(
            judge_dir("/r", DirRole::Root, &fact(true, true, true)).status,
            Status::Pass
        );
        assert_eq!(
            judge_dir("/r", DirRole::Root, &fact(false, false, false)).status,
            Status::Fail
        );
        assert_eq!(
            judge_dir("/r", DirRole::Root, &fact(true, true, false)).status,
            Status::Fail
        );
        assert_eq!(
            judge_dir("/r", DirRole::Root, &fact(true, false, false)).status,
            Status::Fail
        );
        assert_eq!(
            judge_dir("/p", DirRole::Plugins, &fact(true, true, false)).status,
            Status::Warn
        );
        assert_eq!(
            judge_dir("/p", DirRole::Plugins, &fact(false, false, false)).status,
            Status::Pass
        );
        assert_eq!(
            judge_dir("/c", DirRole::Config, &fact(false, false, false)).status,
            Status::Pass
        );
    }

    fn file(name: &str, content: Option<&str>, error: Option<&str>) -> ConfigFile {
        ConfigFile {
            name: name.into(),
            stem: name.split('.').next().unwrap().into(),
            error: error.map(str::to_string),
            value: content.map(|c| Value::from_json_str(c).unwrap()),
        }
    }

    fn known() -> (Vec<String>, Vec<String>) {
        (
            ["meerschaum", "system", "plugins"]
                .map(String::from)
                .to_vec(),
            ["instance", "connectors"].map(String::from).to_vec(),
        )
    }

    #[test]
    fn config_files_that_do_not_parse_fail() {
        let (top, mrsm) = known();
        let files = vec![
            file("meerschaum.yaml", Some("{}"), None),
            file(
                "system.json",
                None,
                Some("Expecting value: line 1 column 1"),
            ),
        ];
        let checks = judge_config(&files, &top, &mrsm);
        let files_check = checks.iter().find(|(n, _)| *n == "files").unwrap();
        assert!(
            files_check
                .1
                .detail
                .starts_with("1 config file does not parse"),
            "{}",
            files_check.1.detail
        );
        assert_eq!(files_check.1.status, Status::Fail);
        assert!(files_check.1.detail.contains("system.json"));
        assert!(files_check.1.fix.is_some());
    }

    #[test]
    fn unknown_and_deprecated_keys_warn() {
        let (top, mrsm) = known();
        let files = vec![
            file(
                "meerschaum.yaml",
                Some(r#"{"instance": "sql:x", "weird": 1}"#),
                None,
            ),
            file("stray.json", Some("{}"), None),
            file(
                "system.json",
                Some(r#"{"experimental": {"join_fetch": true}}"#),
                None,
            ),
        ];
        let checks = judge_config(&files, &top, &mrsm);
        let keys = &checks.iter().find(|(n, _)| *n == "keys").unwrap().1;
        assert_eq!(keys.status, Status::Warn);
        assert!(keys.detail.contains("stray.json"));
        assert!(keys.detail.contains("meerschaum:weird"));
        assert!(keys.detail.contains("join_fetch"));
    }

    #[test]
    fn clean_config_passes() {
        let (top, mrsm) = known();
        // v5's own default file holds `join_fetch: false`: not a problem.
        let files = vec![
            file(
                "meerschaum.yaml",
                Some(r#"{"instance": "sql:x", "connectors": {}}"#),
                None,
            ),
            file(
                "system.json",
                Some(r#"{"experimental": {"join_fetch": false}}"#),
                None,
            ),
        ];
        let checks = judge_config(&files, &top, &mrsm);
        assert!(
            checks.iter().all(|(_, v)| v.status == Status::Pass),
            "{checks:?}"
        );
        assert_eq!(judge_config(&[], &top, &mrsm)[0].1.status, Status::Pass);
    }

    #[test]
    fn flavors_come_from_the_attribute_or_the_uri() {
        let attrs = |json: &str| Value::from_json_str(json).unwrap().to_map();
        assert_eq!(
            flavor_of(&attrs(r#"{"flavor": "PostgreSQL"}"#)),
            "postgresql"
        );
        assert_eq!(
            flavor_of(&attrs(r#"{"uri": "oracle+oracledb://u:p@h/db"}"#)),
            "oracle"
        );
        assert_eq!(
            flavor_of(&attrs(r#"{"uri": "postgres://u@h/db"}"#)),
            "postgresql"
        );
        assert_eq!(flavor_of(&attrs(r#"{}"#)), "");
    }

    #[test]
    fn sqlite_paths() {
        let attrs = |json: &str| Value::from_json_str(json).unwrap().to_map();
        assert_eq!(
            sqlite_path(&attrs(r#"{"uri": "sqlite:////abs/x.db"}"#), "x", "/root/db").as_deref(),
            Some("/abs/x.db")
        );
        assert_eq!(
            sqlite_path(
                &attrs(r#"{"uri": "sqlite:///rel.db?mode=ro"}"#),
                "x",
                "/root/db"
            )
            .as_deref(),
            Some("rel.db")
        );
        assert_eq!(
            sqlite_path(
                &attrs(r#"{"database": "{SQLITE_DB_PATH}"}"#),
                "x",
                "/root/db"
            )
            .as_deref(),
            Some("/root/db")
        );
        assert_eq!(
            sqlite_path(&attrs("{}"), "local", "/root/db").as_deref(),
            Some("/root/db")
        );
        assert_eq!(sqlite_path(&attrs("{}"), "other", "/root/db"), None);
    }

    fn driver(name: &'static str, source: &str) -> DriverFact {
        DriverFact {
            name,
            source: source.into(),
            version: "v1.5.6".into(),
            path: "/lib/x.so".into(),
        }
    }

    #[test]
    fn drivers_matter_only_when_a_connector_needs_them() {
        let d = driver("oracle", "missing");
        assert_eq!(
            judge_driver(&d, &[], Some("main"), true).status,
            Status::Skip
        );
        assert_eq!(
            judge_driver(&d, &["ora"], Some("main"), true).status,
            Status::Warn
        );
        assert_eq!(
            judge_driver(&d, &["main"], Some("main"), true).status,
            Status::Fail
        );
        assert_eq!(
            judge_driver(&driver("oracle", "system"), &["main"], Some("main"), true).status,
            Status::Pass
        );
    }

    #[test]
    fn duckdb_is_fetched_on_first_use_unless_installs_are_off() {
        let d = driver("duckdb", "missing");
        let auto = judge_driver(&d, &["main"], Some("main"), true);
        assert_eq!(auto.status, Status::Warn);
        assert!(auto.detail.contains("first use"));
        assert_eq!(
            judge_driver(&d, &["main"], Some("main"), false).status,
            Status::Fail
        );
        // A library that cannot be loaded is not fetched again.
        assert_eq!(
            judge_driver(&driver("duckdb", "error"), &["main"], Some("main"), true).status,
            Status::Fail
        );
    }

    fn manifest(json: &str) -> Manifest {
        serde_json::from_str(json).unwrap()
    }

    const MANIFEST: &str = r#"{
        "schema_version": 3, "meerschaum_version": "5.0.0rc1", "python_version": "3.13.1",
        "fingerprint": null,
        "plugins": {
            "api_one": {"name": "api_one", "api_plugins": ["setup_api"]},
            "dash_one": {"name": "dash_one", "dash_plugins": ["init"]},
            "page_one": {"name": "page_one", "web_pages": [{"path": "/p", "mode": "html"}]},
            "broken": {"name": "broken", "import_error": "ModuleNotFoundError: no module named 'x'\n  at line 3",
                       "venv_warnings": ["pandas"]},
            "plain": {"name": "plain", "data_functions": ["fetch"]}
        },
        "load_errors": {"all": "boom\nsecond line"}
    }"#;

    #[test]
    fn the_manifest_says_what_plugins_need() {
        let f = plugin_facts(&manifest(MANIFEST));
        assert_eq!(f.names.len(), 5);
        assert_eq!(f.needs_api, ["api_one", "dash_one", "page_one"]);
        assert_eq!(f.needs_dash, ["dash_one"]);
        assert_eq!(
            f.import_errors,
            [(
                "broken".to_string(),
                "ModuleNotFoundError: no module named 'x'".to_string()
            )]
        );
        assert_eq!(f.venv_warnings, ["broken"]);
        assert_eq!(f.load_errors, [("all".to_string(), "boom".to_string())]);
    }

    #[test]
    fn plugin_checks_report_import_errors_and_undeclared_packages() {
        let checks = judge_plugins(&plugin_facts(&manifest(MANIFEST)));
        let by_name = |n: &str| &checks.iter().find(|(name, _)| *name == n).unwrap().1;
        assert_eq!(by_name("installed").status, Status::Pass);
        assert_eq!(by_name("imports").status, Status::Warn);
        assert!(
            by_name("imports")
                .detail
                .contains("broken: ModuleNotFoundError")
        );
        assert_eq!(by_name("requirements").status, Status::Warn);
        let none = judge_plugins(&PluginFacts::default());
        assert_eq!(none[0].1.status, Status::Skip);
    }

    fn borrowed<'a>(
        v: &'a [(&'static str, Option<ModuleProbe>)],
    ) -> Vec<(&'static str, Option<&'a ModuleProbe>)> {
        v.iter().map(|(n, m)| (*n, m.as_ref())).collect()
    }

    #[test]
    fn extras_warn_when_a_plugin_needs_a_missing_one() {
        let p = probe(OK_PROBE).unwrap();
        let mods = |names: &[&'static str]| -> Vec<(&'static str, Option<ModuleProbe>)> {
            names
                .iter()
                .map(|n| (*n, p.modules.get(*n).cloned()))
                .collect()
        };
        let pandas = mods(&["pandas"]);
        assert_eq!(
            judge_extra("pandas", &borrowed(&pandas), &[], "py").status,
            Status::Pass
        );
        let api = mods(&["fastapi"]);
        assert_eq!(
            judge_extra("api", &borrowed(&api), &[], "py").status,
            Status::Skip
        );
        let needed = judge_extra(
            "api",
            &borrowed(&api),
            &["my_plugin".to_string()],
            "/usr/bin/python3",
        );
        assert_eq!(needed.status, Status::Warn);
        assert!(needed.fix.unwrap().contains("meerschaum[api]"));
        // Partly installed and needed by nothing: a PASS that names what is missing.
        let partial = mods(&["pandas", "fastapi"]);
        let v = judge_extra("x", &borrowed(&partial), &[], "py");
        assert_eq!(v.status, Status::Pass);
        assert!(v.detail.contains("pandas 3.0.1") && v.detail.contains("not installed: fastapi"));
        assert!(v.fix.is_none());
        // ... and a WARN once an installed plugin needs the extra.
        assert_eq!(
            judge_extra("x", &borrowed(&partial), &["p".to_string()], "py").status,
            Status::Warn
        );
    }

    #[test]
    fn venvs_for_another_python_are_stale() {
        let venv = |name: &str, minors: &[(u32, u32)], dangling: &[&str]| VenvFact {
            name: name.into(),
            minors: minors.to_vec(),
            dangling: dangling.iter().map(|s| s.to_string()).collect(),
        };
        assert_eq!(judge_venvs(&[], Some((3, 13))).status, Status::Skip);
        let fine = [
            venv("mrsm", &[(3, 13)], &[]),
            venv("a", &[(3, 12), (3, 13)], &[]),
        ];
        assert_eq!(judge_venvs(&fine, Some((3, 13))).status, Status::Pass);
        let old = [venv("a", &[(3, 11)], &[])];
        let v = judge_venvs(&old, Some((3, 13)));
        assert_eq!(v.status, Status::Warn);
        assert!(
            v.detail
                .contains("a (built for Python 3.11, the worker is 3.13)")
        );
        let gone = [venv("b", &[(3, 13)], &["python3"])];
        assert_eq!(judge_venvs(&gone, Some((3, 13))).status, Status::Warn);
        // Without a worker interpreter only a dangling link is known.
        assert_eq!(judge_venvs(&old, None).status, Status::Pass);
    }

    #[test]
    fn jobs() {
        let job = |name: &str, pid: Option<i32>, process, result: Option<(bool, &str)>| JobFact {
            name: name.into(),
            pid,
            process,
            result: result.map(|(s, m)| (s, m.to_string())),
        };
        assert_eq!(judge_jobs(&[])[0].1.status, Status::Skip);
        let facts = vec![
            job("up", Some(10), Some(ProcessState::Alive), None),
            job("paused", Some(11), Some(ProcessState::Paused), None),
            job("ok", None, None, Some((true, "Success"))),
            job("bad", None, None, Some((false, "Failed to sync\nmore"))),
            job(
                "crashed",
                Some(12),
                Some(ProcessState::Gone),
                Some((false, "x")),
            ),
        ];
        let checks = judge_jobs(&facts);
        let by_name = |n: &str| checks.iter().find(|(name, _)| *name == n).map(|(_, v)| v);
        assert_eq!(by_name("state").unwrap().status, Status::Pass);
        assert!(
            by_name("state")
                .unwrap()
                .detail
                .contains("1 running, 1 paused, 3 stopped")
        );
        let failed = by_name("failed jobs").unwrap();
        assert!(
            failed.detail.contains("bad (Failed to sync)") && failed.detail.contains("crashed")
        );
        assert!(!failed.detail.contains("ok"));
        assert!(by_name("pid files").unwrap().detail.contains("crashed"));
    }

    #[test]
    fn stack_services() {
        let text = "db\tmrsm-db-1\ttimescale/timescaledb-ha:pg18\trunning\tUp 3 hours (healthy)\n\
                    api\tmrsm-api-1\tbmeares/meerschaum:api\trunning\tUp 3 hours\n\
                    valkey\tmrsm-valkey-1\tvalkey/valkey:latest\texited\tExited (1) 2 minutes ago\n\
                    grafana\tmrsm-grafana-1\tgrafana\trunning\tUp 1 minute (unhealthy)\n\
                    junk line\n";
        let services = parse_services(text);
        assert_eq!(services.len(), 4);
        assert_eq!(services[0].image, "timescale/timescaledb-ha:pg18");
        assert_eq!(judge_service(&services[0], "mrsm").status, Status::Pass);
        assert_eq!(judge_service(&services[1], "mrsm").status, Status::Pass);
        let exited = judge_service(&services[2], "mrsm");
        assert_eq!(exited.status, Status::Fail);
        assert!(exited.fix.unwrap().contains("mrsm stack up -d valkey"));
        assert_eq!(judge_service(&services[3], "mrsm").status, Status::Fail);
        // Stopped on purpose (exit 0, or the signal that stopped it): a warning, not a failure.
        for status in [
            "Exited (0) 2 minutes ago",
            "Exited (137) 1 hour ago",
            "Exited (143) 3 days ago",
        ] {
            let stopped = ServiceFact {
                state: "exited".into(),
                status: status.into(),
                ..services[2].clone()
            };
            let v = judge_service(&stopped, "mrsm");
            assert_eq!(v.status, Status::Warn, "{status}");
            assert!(v.detail.starts_with("stopped:"));
        }
        assert_eq!(exit_code("Exited (1) 2 minutes ago"), Some(1));
        assert_eq!(exit_code("Up 3 hours"), None);
        let starting = ServiceFact {
            status: "Up 2 seconds (health: starting)".into(),
            ..services[0].clone()
        };
        assert_eq!(judge_service(&starting, "mrsm").status, Status::Warn);
    }

    #[test]
    fn api_versions() {
        assert_eq!(
            judge_api_version(Some("5.0.0rc1"), "5.0.0-rc.1").status,
            Status::Pass
        );
        assert_eq!(
            judge_api_version(Some("4.1.4"), "5.0.0-rc.1").status,
            Status::Warn
        );
        assert_eq!(judge_api_version(None, "5.0.0-rc.1").status, Status::Warn);
    }

    fn report() -> Report {
        let mut r = Report::default();
        r.extend(vec![
            Check::warn("paths", "disk: temp", "low", "free it"),
            Check::pass("install", "meerschaum", "5.0.0"),
            Check::skip("stack", "services", "no stack file"),
            Check::fail("instance", "connection", "refused", "start it"),
        ]);
        r
    }

    #[test]
    fn a_report_sorts_by_group_and_counts() {
        let r = report();
        let groups: Vec<&str> = r.checks.iter().map(|c| c.group).collect();
        assert_eq!(groups, ["install", "paths", "instance", "stack"]);
        let s = r.summary();
        assert_eq!((s.pass, s.warn, s.fail, s.skip), (1, 1, 1, 1));
        assert_eq!(s.text(), "1 passed, 1 warning, 1 failed, 1 skipped");
    }

    #[test]
    fn the_tuple_fails_only_on_a_failure() {
        let t = report().tuple();
        assert!(!t.success);
        assert!(
            t.message.contains("1 check failed (instance: connection)"),
            "{}",
            t.message
        );
        let mut ok = Report::default();
        ok.extend(vec![
            Check::warn("paths", "x", "d", "f"),
            Check::pass("install", "y", "d"),
        ]);
        assert!(ok.tuple().success);
    }

    #[test]
    fn the_json_document_has_stable_keys() {
        let mut r = report();
        r.checks[0].millis = Some(12);
        let j = r.to_json("5.0.0-rc.1");
        assert_eq!(j["version"], "5.0.0-rc.1");
        assert_eq!(j["summary"]["ok"], false);
        assert_eq!(j["summary"]["fail"], 1);
        assert_eq!(j["summary"]["warn"], 1);
        let checks = j["checks"].as_array().unwrap();
        assert_eq!(checks.len(), 4);
        for key in ["group", "name", "status", "detail", "fix"] {
            assert!(checks[0].get(key).is_some(), "{key}");
        }
        assert_eq!(checks[0]["status"], "PASS");
        assert!(checks[0]["fix"].is_null());
        assert_eq!(checks[0]["duration_ms"], 12);
        assert_eq!(checks[1]["fix"], "free it");
        assert!(checks[1].get("duration_ms").is_none());
    }
}
