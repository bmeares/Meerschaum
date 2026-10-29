//! Entry-point plugins: pip-installed packages that register a plugin in the
//! `meerschaum.plugins` entry-point group.
//!
//! v4 discovers them whatever the plugins directory holds (`sync_plugins_symlinks`
//! symlinks each one into `<root>/.internal/plugins`). The Rust host decides whether to start
//! Python at all from the plugin tree, so an empty tree must not hide them, yet `mrsm show
//! version` and the shell must not start Python when there is no plugin. The set of entry-point
//! plugins is a property of the interpreter's `sys.path`, which only the interpreter knows, so
//! a tiny probe asks it once and the answer is cached at [`cache_path`]:
//!
//! - the cache records the interpreter (resolved path, modification time, size), the
//!   environment variables that shape `sys.path`, and every `sys.path` entry with its
//!   modification time;
//! - a valid cache costs a handful of `stat` calls: `pip install` / `uninstall` changes the
//!   modification time of the directory that holds the `*.dist-info`, which invalidates it;
//! - an invalid or missing cache costs one probe (about 30 ms, a bare `python -c` start plus a
//!   directory scan, no `meerschaum` import);
//! - a probe that cannot run (no interpreter, a plugin import in progress) answers "none"
//!   and caches nothing.
//!
//! The probe applies v4's own filter: an entry point counts only when its distribution lists
//! `<name>.py` or `<name>/__init__.py` (`RECORD` for a `*.dist-info`, `SOURCES.txt` for an
//! `*.egg-info`). Limitation: an in-place edit of an existing `entry_points.txt` that does not
//! change the modification time of a `sys.path` directory is not noticed until `mrsm reload`.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::worker::{WorkerOptions, spawn_refused};

/// The cache file name under `<root>/cache/`.
pub const CACHE_FILE_NAME: &str = "entry_points.json";

const CACHE_VERSION: u32 = 1;

/// How long a probe may run before it is abandoned.
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);

/// Environment variables that change the interpreter's `sys.path`.
const PATH_VARIABLES: &[&str] = &[
    "PYTHONPATH",
    "PYTHONHOME",
    "PYTHONNOUSERSITE",
    "PYTHONUSERBASE",
    "PYTHONSAFEPATH",
];

/// The probe: standard library only. Prints `{"dirs": [[path, mtime_ns], ...], "plugins": [...]}`.
const PROBE: &str = r#"
import csv, json, os, sys

def normalize(name):
    return name.lower().replace('-', '_').replace('.', '_')

def plugin_names(path):
    try:
        with open(os.path.join(path, 'entry_points.txt'), encoding='utf-8') as f:
            lines = f.read().splitlines()
    except (OSError, UnicodeDecodeError):
        return []
    names, group = [], None
    for line in lines:
        line = line.strip()
        if not line or line[0] in '#;':
            continue
        if line.startswith('[') and line.endswith(']'):
            group = line[1:-1].strip()
        elif group == 'meerschaum.plugins' and '=' in line:
            names.append(line.split('=', 1)[0].strip())
    return names

def listed_files(path, kind):
    try:
        if kind == 'dist-info':
            with open(os.path.join(path, 'RECORD'), encoding='utf-8', newline='') as f:
                return {row[0] for row in csv.reader(f) if row}
        with open(os.path.join(path, 'SOURCES.txt'), encoding='utf-8') as f:
            return {line.strip() for line in f}
    except (OSError, UnicodeDecodeError, csv.Error):
        return None

dirs, plugins, seen = [], set(), set()
for entry in sys.path:
    if not entry:
        continue
    try:
        modified = os.stat(entry).st_mtime_ns
    except OSError:
        dirs.append([entry, 0])
        continue
    dirs.append([entry, modified])
    try:
        children = sorted(os.listdir(entry))
    except OSError:
        continue
    for child in children:
        for kind in ('dist-info', 'egg-info'):
            if not child.endswith('.' + kind):
                continue
            dist = normalize(child[:-len(kind) - 1].split('-')[0])
            if dist in seen:
                continue
            seen.add(dist)
            path = os.path.join(entry, child)
            names = plugin_names(path)
            if not names:
                continue
            files = listed_files(path, kind)
            if not files:
                continue
            for name in names:
                if name + '.py' in files or name + '/__init__.py' in files:
                    plugins.add(name)
print(json.dumps({'dirs': dirs, 'plugins': sorted(plugins)}))
"#;

/// A probe result and what it is valid for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Probe {
    version: u32,
    /// The interpreter: resolved path, modification time and size.
    interpreter: String,
    /// The `sys.path`-shaping environment.
    environment: String,
    /// Every `sys.path` entry and its modification time (nanoseconds; 0 when missing).
    dirs: Vec<(String, i64)>,
    /// The entry-point plugin names, sorted.
    plugins: Vec<String>,
}

/// Where the probe is cached, next to the manifest at `manifest_path`.
pub fn cache_path(manifest_path: &Path) -> PathBuf {
    manifest_path.with_file_name(CACHE_FILE_NAME)
}

/// The entry-point plugins visible to the interpreter `options` starts, sorted. Cheap when the
/// cache at `cache` is valid; `force` probes regardless (`reload`). Never fails: whatever
/// cannot be determined is "none".
pub fn plugins(options: &WorkerOptions, cache: &Path, force: bool) -> Vec<String> {
    let interpreter = interpreter_identity(&options.python);
    let environment = environment_identity(options);
    let cached = read(cache);
    if !force
        && let Some(probe) = &cached
        && probe.interpreter == interpreter
        && probe.environment == environment
        && probe
            .dirs
            .iter()
            .all(|(dir, stamp)| modified(dir) == *stamp)
    {
        return probe.plugins.clone();
    }
    if spawn_refused() {
        // A plugin is being imported: never start a process from there.
        return cached.map(|p| p.plugins).unwrap_or_default();
    }
    let Some((dirs, plugins)) = run_probe(options) else {
        return Vec::new();
    };
    let probe = Probe {
        version: CACHE_VERSION,
        interpreter,
        environment,
        dirs,
        plugins: plugins.clone(),
    };
    write(cache, &probe);
    plugins
}

fn read(path: &Path) -> Option<Probe> {
    let text = std::fs::read_to_string(path).ok()?;
    let probe: Probe = serde_json::from_str(&text).ok()?;
    (probe.version == CACHE_VERSION).then_some(probe)
}

fn write(path: &Path, probe: &Probe) {
    let Ok(text) = serde_json::to_string(probe) else {
        return;
    };
    if let Some(parent) = path.parent()
        && std::fs::create_dir_all(parent).is_err()
    {
        return;
    }
    let tmp = path.with_extension(format!("json.{}.tmp", std::process::id()));
    if std::fs::write(&tmp, text).is_ok() && std::fs::rename(&tmp, path).is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
}

fn modified(path: &str) -> i64 {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// The executable `program` runs (a `PATH` search for a bare name), without following symlinks:
/// two virtual environments often share one real binary.
fn resolve_program(program: &Path) -> Option<PathBuf> {
    if program.components().count() > 1 || program.is_absolute() {
        return program.is_file().then(|| program.to_path_buf());
    }
    let paths = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&paths) {
        let mut candidate = dir.join(program);
        if candidate.is_file() {
            return Some(candidate);
        }
        if cfg!(windows) {
            candidate.set_extension("exe");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }
    None
}

fn interpreter_identity(python: &Path) -> String {
    match resolve_program(python) {
        Some(path) => {
            let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            format!(
                "{}|{}|{size}",
                path.to_string_lossy(),
                modified(&path.to_string_lossy())
            )
        }
        None => python.to_string_lossy().into_owned(),
    }
}

fn environment_identity(options: &WorkerOptions) -> String {
    let mut parts = Vec::new();
    for name in PATH_VARIABLES {
        let value = if options.env_remove.iter().any(|k| k == name) {
            String::new()
        } else if let Some((_, v)) = options.env.iter().rev().find(|(k, _)| k == name) {
            v.clone()
        } else {
            std::env::var(name).unwrap_or_default()
        };
        parts.push(format!("{name}={value}"));
    }
    parts.join("\n")
}

/// What a probe prints: the `sys.path` entries with their modification times, and the plugins.
type ProbeOutput = (Vec<(String, i64)>, Vec<String>);

fn run_probe(options: &WorkerOptions) -> Option<ProbeOutput> {
    let mut command = Command::new(&options.python);
    command
        .arg(meerschaum_core::self_exe::PYTHON_SAFE_PATH_FLAG)
        .arg("-c")
        .arg(PROBE)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null());
    for key in &options.env_remove {
        command.env_remove(key);
    }
    command.envs(options.env.iter().map(|(k, v)| (k, v)));
    if let Some(dir) = &options.current_dir {
        command.current_dir(dir);
    }
    let mut child = command.spawn().ok()?;
    let deadline = std::time::Instant::now() + PROBE_TIMEOUT;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(Duration::from_millis(2));
            }
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    };
    if !status.success() {
        return None;
    }
    let mut text = String::new();
    std::io::Read::read_to_string(&mut child.stdout.take()?, &mut text).ok()?;
    let value: serde_json::Value = serde_json::from_str(text.trim()).ok()?;
    let dirs = value
        .get("dirs")?
        .as_array()?
        .iter()
        .filter_map(|entry| {
            let entry = entry.as_array()?;
            Some((entry.first()?.as_str()?.to_owned(), entry.get(1)?.as_i64()?))
        })
        .collect();
    let plugins = value
        .get("plugins")?
        .as_array()?
        .iter()
        .filter_map(|name| name.as_str().map(str::to_owned))
        .collect();
    Some((dirs, plugins))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Options for the system `python3`, or `None` when it cannot run the probe (it needs `-P`).
    fn options(path: &Path) -> Option<WorkerOptions> {
        let mut options = WorkerOptions {
            python: PathBuf::from(if cfg!(windows) { "python" } else { "python3" }),
            ..WorkerOptions::default()
        };
        let ok = Command::new(&options.python)
            .args(["-P", "-c", "pass"])
            .stdin(Stdio::null())
            .status()
            .is_ok_and(|s| s.success());
        ok.then(|| {
            options.env = vec![("PYTHONPATH".into(), path.to_string_lossy().into_owned())];
            options
        })
    }

    /// A package the way `pip` installs one: the module plus a `*.dist-info`.
    fn install(site: &Path, dist: &str, plugin: &str, with_file: bool) {
        let info = site.join(format!("{dist}-1.0.dist-info"));
        std::fs::create_dir_all(&info).unwrap();
        std::fs::write(
            info.join("entry_points.txt"),
            format!(
                "[console_scripts]\nother = x:y\n\n[meerschaum.plugins]\n{plugin} = {plugin}\n"
            ),
        )
        .unwrap();
        let record = if with_file {
            format!("{plugin}.py,sha256=x,1\n{dist}-1.0.dist-info/RECORD,,\n")
        } else {
            format!("{dist}-1.0.dist-info/RECORD,,\n")
        };
        std::fs::write(info.join("RECORD"), record).unwrap();
        std::fs::write(site.join(format!("{plugin}.py")), "x = 1\n").unwrap();
    }

    #[test]
    fn finds_entry_point_plugins_and_applies_v4s_file_filter() {
        let site = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let Some(options) = options(site.path()) else {
            return;
        };
        let cache = root.path().join("cache").join(CACHE_FILE_NAME);
        assert!(plugins(&options, &cache, false).is_empty());
        install(site.path(), "w17b_one", "w17bone", true);
        // No `<name>.py` in RECORD: v4 skips it.
        install(site.path(), "w17b_two", "w17btwo", false);
        assert_eq!(plugins(&options, &cache, false), vec!["w17bone"]);
    }

    #[test]
    fn a_valid_cache_skips_the_probe_and_an_install_invalidates_it() {
        let site = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let Some(mut options) = options(site.path()) else {
            return;
        };
        let cache = root.path().join(CACHE_FILE_NAME);
        install(site.path(), "w17b_one", "w17bone", true);
        assert_eq!(plugins(&options, &cache, false), vec!["w17bone"]);
        // The interpreter is gone, so only a valid cache can answer.
        let python = options.python.clone();
        options.python = root.path().join("no-python");
        assert!(plugins(&options, &cache, false).is_empty());
        options.python = python;
        let before = std::fs::metadata(&cache).unwrap().modified().unwrap();
        assert_eq!(plugins(&options, &cache, false), vec!["w17bone"]);
        assert_eq!(
            std::fs::metadata(&cache).unwrap().modified().unwrap(),
            before,
            "a valid cache is not rewritten"
        );
        std::thread::sleep(Duration::from_millis(20));
        install(site.path(), "w17b_three", "w17bthree", true);
        assert_eq!(
            plugins(&options, &cache, false),
            vec!["w17bone", "w17bthree"]
        );
        assert_eq!(
            plugins(&options, &cache, true),
            vec!["w17bone", "w17bthree"]
        );
    }

    #[test]
    fn a_probe_that_cannot_run_answers_none_and_caches_nothing() {
        let root = tempfile::tempdir().unwrap();
        let options = WorkerOptions {
            python: root.path().join("python-does-not-exist"),
            ..WorkerOptions::default()
        };
        let cache = root.path().join(CACHE_FILE_NAME);
        assert!(plugins(&options, &cache, false).is_empty());
        assert!(!cache.exists());
    }

    #[test]
    fn the_environment_is_part_of_the_cache_key() {
        let one = tempfile::tempdir().unwrap();
        let two = tempfile::tempdir().unwrap();
        let root = tempfile::tempdir().unwrap();
        let Some(mut options) = options(one.path()) else {
            return;
        };
        install(two.path(), "w17b_two", "w17btwo", true);
        let cache = root.path().join(CACHE_FILE_NAME);
        assert!(plugins(&options, &cache, false).is_empty());
        options.env = vec![(
            "PYTHONPATH".into(),
            two.path().to_string_lossy().into_owned(),
        )];
        assert_eq!(plugins(&options, &cache, false), vec!["w17btwo"]);
    }
}
