//! Python interpreter probing and package version decisions for `mrsm doctor`.

use super::*;

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
pub(super) const PROBE_SCRIPT: &str = r#"
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
                crate::native::plural(python_plugins)
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
    // The copies behind the one imported (a stale `~/.local` copy in front of a newer system or
    // environment copy is the usual case): named, so the fix is to remove the one in front.
    let others: Vec<String> = m
        .copies
        .iter()
        .filter(|c| Some(&c.dir) != m.dir.as_ref())
        .map(|c| match &c.version {
            Some(v) => format!("{} ({v})", c.dir),
            None => c.dir.clone(),
        })
        .collect();
    let shadows = if others.is_empty() {
        String::new()
    } else {
        format!("; it shadows {}", others.join(", "))
    };
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
                "{} imports `meerschaum` {shown} from {dir}{site}{shadows}: {why}; the worker host refuses it",
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
        python_guard::Verdict::OtherRelease if !others.is_empty() => Verdict::warn(
            format!(
                "`meerschaum` {shown} loads from {dir}{site}{shadows}, and this program is {binary_version}"
            ),
            format!(
                "remove the copy in {dir} that shadows the others (`{} -m pip uninstall meerschaum` removes the copy found first), then check again",
                p.executable
            ),
        ),
        python_guard::Verdict::OtherRelease => Verdict::warn(
            format!(
                "`meerschaum` {shown} loads from {dir}{site} and this program is {binary_version}: the same major version and worker protocol, so the worker starts (and warns once)"
            ),
            format!("align them: {install_matching}, or upgrade this program to {shown}"),
        ),
        python_guard::Verdict::Match => {
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
pub(super) fn needs_python(detail: String, fix: String, python_plugins: usize) -> Verdict {
    if python_plugins > 0 {
        Verdict::fail(
            format!(
                "{detail}; {python_plugins} Python plugin{} cannot run",
                crate::native::plural(python_plugins)
            ),
            fix,
        )
    } else {
        Verdict::warn(format!("{detail}; only plugins need it"), fix)
    }
}
