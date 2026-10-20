//! Project resolution: a port of `meerschaum/compose/utils/config.py`, `utils/stack.py` and
//! `utils/__init__.py::init` (locating the compose file, reading it with environment
//! substitution, resolving connectors and directories, and the per-project environment).
//!
//! The resolved compose configuration is kept as an ordered [`Map`] with the same keys, values
//! and key order as Python's `compose_config` dictionary (paths are stored as POSIX strings,
//! which is how Python serializes its `pathlib.Path` values into `MRSM__COMPOSE_CONFIG`).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use meerschaum_core::config::{Config, ConfigOptions, GetOptions, paths::resolve_path};
use meerschaum_core::pyfmt::{JsonStyle, json_dumps, py_str};
use meerschaum_core::static_config::static_config;
use meerschaum_core::value::{Map, Value, apply_patch};
use sha2::{Digest, Sha256};

use crate::dotenv;
use crate::env::{EnvDict, ProcessEnv};
use crate::envyaml::{EnvYaml, EnvYamlError, EnvYamlOptions};
use crate::error::{ComposeError, ErrorKind, Messages};

/// The top-level keys Python keeps from the compose file (`COMPOSE_KEYS`), in order.
pub const COMPOSE_KEYS: &[&str] = &[
    "root_dir",
    "plugins_dir",
    "plugins",
    "sync",
    "config",
    "connectors",
    "instance",
    "environment",
    "project_name",
    "pipes",
    "jobs",
    "isolation",
    "daemon",
];

/// The default compose file names (`DEFAULT_COMPOSE_FILE_CANDIDATES`).
pub const DEFAULT_COMPOSE_FILE_CANDIDATES: &[&str] = &["mrsm-compose.yaml", "mrsm-compose.yml"];

/// The config cache file name inside the project root.
pub const CONFIG_CACHE_FILENAME: &str = ".compose-cache.pkl";

/// `Path.as_posix()`.
pub fn posix(p: &Path) -> String {
    meerschaum_core::config::paths::posix(p)
}

/// `pathlib.Path(p).resolve()` against an explicit base directory for relative paths.
pub fn resolve_against(base: &Path, p: &Path) -> PathBuf {
    if p.is_absolute() {
        resolve_path(p)
    } else {
        resolve_path(&base.join(p))
    }
}

/// `pathlib.PurePath(name).stem` of a file or directory name.
pub fn py_stem(name: &str) -> String {
    match name.rfind('.') {
        Some(i) if i > 0 && i < name.len() - 1 => name[..i].to_string(),
        _ => name.to_string(),
    }
}

/// Python's `type(x).__name__` for a [`Value`].
pub fn py_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "NoneType",
        Value::Bool(_) => "bool",
        Value::Int(_) => "int",
        Value::Float(_) => "float",
        Value::Str(_) => "str",
        Value::List(_) => "list",
        Value::Map(_) => "dict",
    }
}

/// `meerschaum.utils.misc.items_str(items)` with the default arguments.
pub fn items_str(items: &[String]) -> String {
    let q = "'";
    match items.len() {
        0 => String::new(),
        1 => format!("{q}{}{q}", items[0]),
        2 => format!("{q}{}{q} and {q}{}{q}", items[0], items[1]),
        n => {
            let sep = format!("{q}, {q}");
            let mut out = format!("{q}{}{q}", items[..n - 1].join(&sep));
            out.push(',');
            out.push_str(&format!(" and {q}{}{q}", items[n - 1]));
            out
        }
    }
}

/// The host environment compose runs in: the working directory, the process environment and
/// the host's config registry (used for `MRSM{}` symlinks and `'{type:label}'` references).
#[derive(Debug)]
pub struct Host {
    /// The working directory (Python resolves relative `--file` paths and envyaml's default
    /// `.env` against it).
    pub cwd: PathBuf,
    /// The process environment (`os.environ`) before compose runs.
    pub env: ProcessEnv,
    /// The host config registry.
    pub config: Config,
}

impl Host {
    /// Build the host context. The registry never writes files.
    pub fn new(cwd: impl Into<PathBuf>, env: ProcessEnv) -> Result<Host, ComposeError> {
        let mut opts =
            ConfigOptions::with_env(env.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
        opts.allow_writes = false;
        if let Some(home) = env.get("HOME") {
            opts.home = PathBuf::from(home);
        }
        let config =
            Config::new(opts).map_err(|e| ComposeError::new(ErrorKind::Os, e.to_string()))?;
        Ok(Host {
            cwd: cwd.into(),
            env,
            config,
        })
    }

    /// `get_config(*keys)` on the host registry, forwarding its warnings.
    pub fn get_config(&self, keys: &[&str], msgs: &mut Messages) -> Option<Value> {
        let (valid, value) = self.config.get_with(keys, GetOptions::default());
        for w in self.config.take_warnings() {
            msgs.warn(w);
        }
        valid.then_some(value)
    }

    /// A named path from the host registry (`PLUGINS_RESOURCES_PATH`, `ROOT_DIR_PATH`, ...).
    pub fn path(&self, name: &str) -> PathBuf {
        self.config.paths().path(name)
    }
}

/// The per-process compose state Python keeps in module globals: the config-cache memo
/// (`CONFIG_METADATA`) and whether the obsolete plugin warning was shown.
#[derive(Debug, Clone, Default)]
pub struct Session {
    /// `CONFIG_METADATA`: cache path -> (hash, has_changed).
    pub config_metadata: HashMap<String, (String, bool)>,
    /// `_warned_about_plugin`.
    pub warned_about_plugin: bool,
}

/// A loaded compose project.
#[derive(Debug, Clone)]
pub struct Project {
    /// The resolved compose configuration (`compose_config`).
    pub config: Map,
    /// The compose file.
    pub file: PathBuf,
    /// The process environment after loading `.env` and blanking undefined variables (the
    /// `os.environ` that Python's subactions see before the project environment is applied).
    pub process_env: ProcessEnv,
    /// The compose file's YAML document as parsed (after substitution, including unknown keys),
    /// for the model and the lint.
    pub document: Map,
    /// References that could not be resolved while reading the file (host connector references
    /// `'{type:label}'` and `MRSM{...}` symlinks): the lint reports each one and `up` / `run` /
    /// `sync` refuse to start on a missing connector.
    pub unresolved: Vec<UnresolvedRef>,
    /// The root compose file of the tree this project runs in (`None` for a project that runs
    /// alone). Jobs started from a root carry it as the `MRSM__COMPOSE_ROOT` label.
    pub compose_root: Option<PathBuf>,
    /// A suffix for the config cache file of an included project (several projects share one
    /// root directory), `None` for the root project.
    pub cache_suffix: Option<String>,
    /// Pipes an ancestor project defines (the ancestor's entry wins): `(connector, metric,
    /// location, instance)` keys this project must not define.
    pub excluded_pipes: Vec<PipeKey>,
}

/// The keys that identify a pipe in a project tree: connector, metric, location and instance.
pub type PipeKey = (String, String, Option<String>, String);

/// A reference in the compose file that does not resolve on this host.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnresolvedRef {
    /// `connector` (a `'{type:label}'` host reference) or `symlink` (`MRSM{a:b}`).
    pub kind: &'static str,
    /// What is referenced (`mqtt:x`, `meerschaum:foo:bar`).
    pub name: String,
    /// Where it is referenced (`config.meerschaum.connectors.mqtt.y`).
    pub location: String,
}

/// The label compose stamps on the jobs it starts from a root compose file.
pub const COMPOSE_ROOT_VAR: &str = "MRSM__COMPOSE_ROOT";

impl Project {
    /// The project root directory (`compose_config['root_dir']`).
    pub fn root_dir(&self) -> PathBuf {
        PathBuf::from(self.config.get("root_dir").map(py_str).unwrap_or_default())
    }

    /// The plugins directories (`compose_config['plugins_dir']`).
    pub fn plugins_dirs(&self) -> Vec<PathBuf> {
        match self.config.get("plugins_dir") {
            Some(Value::List(items)) => items.iter().map(|v| PathBuf::from(py_str(v))).collect(),
            _ => Vec::new(),
        }
    }

    /// `get_project_name(compose_config)` as a string.
    pub fn project_name(&self) -> String {
        self.config
            .get("project_name")
            .map(py_str)
            .unwrap_or_default()
    }

    /// `get_env_dict(compose_config)`, plus the `MRSM__COMPOSE_ROOT` label when the project runs
    /// in a tree.
    pub fn env_dict(&self) -> Result<EnvDict, ComposeError> {
        get_env_dict_in(
            &self.config,
            &self.process_env,
            self.compose_root.as_deref(),
        )
    }

    /// The project's `config:` block (`compose_config.get('config', {})`).
    pub fn config_block(&self) -> Map {
        match self.config.get("config") {
            Some(Value::Map(m)) => m.clone(),
            _ => Map::new(),
        }
    }
}

/// `infer_compose_file_path(file)`.
pub fn infer_compose_file_path(
    file: Option<&Path>,
    cwd: &Path,
    msgs: &mut Messages,
) -> Option<PathBuf> {
    if let Some(file) = file {
        let abs = if file.is_absolute() {
            file.to_path_buf()
        } else {
            cwd.join(file)
        };
        return abs.exists().then(|| resolve_path(&abs));
    }
    let found: Vec<PathBuf> = DEFAULT_COMPOSE_FILE_CANDIDATES
        .iter()
        .map(|name| resolve_path(&cwd.join(name)))
        .filter(|p| p.exists())
        .collect();
    if found.len() > 1 {
        msgs.warn(format!(
            "Found multiple YAML files. Compose will use this file:\n{}.",
            found[0].display()
        ));
    }
    found.into_iter().next()
}

/// `init_env(compose_file_path, env_file)`: load the project's `.env` (or `env_file`) into the
/// process environment with python-dotenv (existing variables win).
pub fn init_env(
    compose_file: &Path,
    env_file: Option<&Path>,
    env: &mut ProcessEnv,
    msgs: &mut Messages,
) {
    let env_path = env_file.map_or_else(|| PathBuf::from(".env"), Path::to_path_buf);
    let env_path = if env_path.is_absolute() {
        env_path
    } else {
        compose_file
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(env_path)
    };
    if env_path.exists() {
        match std::fs::read_to_string(&env_path) {
            Ok(text) => {
                dotenv::load_into(&text, env);
            }
            Err(e) => msgs.warn(format!("Failed to load '{}':\n{}", env_path.display(), e)),
        }
    }
}

/// The hacky parse of envyaml's `ValueError` message in `read_compose_config`.
fn missing_vars_from_message(message: &str) -> Vec<String> {
    // PARITY: `str(ve).split(' variables ', 1)[-1].split(' are not ', 1)[0]
    //          .replace(' ', '').split(',')` (config.py:110). A "defined several times" error
    // yields garbage names, exactly like Python.
    let after = message
        .split_once(" variables ")
        .map_or(message, |(_, rest)| rest);
    let before = after
        .split_once(" are not ")
        .map_or(after, |(head, _)| head);
    before
        .replace(' ', "")
        .split(',')
        .map(String::from)
        .collect()
}

fn load_envyaml(
    compose_file: &Path,
    env_file: Option<&Path>,
    cwd: &Path,
    process_env: &mut ProcessEnv,
    msgs: &mut Messages,
) -> Result<EnvYaml, ComposeError> {
    let env_file_path = env_file.map(|p| {
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            compose_file
                .parent()
                .unwrap_or_else(|| Path::new("."))
                .join(p)
        }
    });
    let attempt = |environ: &ProcessEnv| {
        EnvYaml::load(&EnvYamlOptions {
            yaml_file: compose_file,
            env_file: env_file_path.clone(),
            cwd,
            environ,
            strict: true,
        })
    };
    match attempt(process_env) {
        Ok(env) => Ok(env),
        Err(e @ (EnvYamlError::Undefined(_) | EnvYamlError::DefinedSeveralTimes(_))) => {
            let missing_vars = missing_vars_from_message(&e.to_string());
            let singular = missing_vars.len() == 1;
            msgs.warn(format!(
                "{} {} not defined!\n     Using {} empty string{} for {}.",
                items_str(&missing_vars),
                if singular { "is" } else { "are" },
                // PARITY: Python builds "Using  empty strings" (two spaces) in the plural.
                if singular { "an" } else { "" },
                if singular { "" } else { "s" },
                if singular {
                    "this variable"
                } else {
                    "these variables"
                },
            ));
            for var in &missing_vars {
                process_env.insert(var.trim_start_matches('$').to_string(), String::new());
            }
            attempt(process_env).map_err(envyaml_error)
        }
        Err(e) => Err(envyaml_error(e)),
    }
}

fn envyaml_error(e: EnvYamlError) -> ComposeError {
    match e {
        EnvYamlError::Io(m) => ComposeError::new(ErrorKind::FileNotFound, m),
        EnvYamlError::Yaml(m) => ComposeError::new(ErrorKind::Yaml, m),
        other => ComposeError::value(other.to_string()),
    }
}

fn map_get_map<'a>(m: &'a Map, key: &str) -> Option<&'a Map> {
    m.get(key).and_then(Value::as_map)
}

/// `read_compose_config(compose_file_path, env_file, isolated)`.
///
/// `process_env` is `os.environ` at the call (after [`init_env`]); undefined variables are added
/// to it as empty strings, as Python does.
pub fn read_compose_config(
    host: &Host,
    compose_file: &Path,
    env_file: Option<&Path>,
    isolated: bool,
    process_env: &mut ProcessEnv,
    msgs: &mut Messages,
) -> Result<(Map, Map), ComposeError> {
    let read = read_compose_config_in(
        host,
        &ReadOptions {
            cwd: &host.cwd,
            compose_file,
            env_file,
            isolated,
            inherit_root: None,
        },
        process_env,
        msgs,
    )?;
    Ok((read.config, read.document))
}

/// The inputs of [`read_compose_config_in`].
#[derive(Debug, Clone, Copy)]
pub struct ReadOptions<'a> {
    /// The working directory (envyaml's default `.env` is read from here).
    pub cwd: &'a Path,
    /// The compose file.
    pub compose_file: &'a Path,
    /// `--env-file`.
    pub env_file: Option<&'a Path>,
    /// `--isolated`.
    pub isolated: bool,
    /// An included project without a `root_dir` of its own uses the including project's root.
    pub inherit_root: Option<&'a Path>,
}

/// What reading a compose file produced.
#[derive(Debug, Clone)]
pub struct ReadResult {
    /// The resolved compose configuration.
    pub config: Map,
    /// The parsed document (after substitution).
    pub document: Map,
    /// References that do not resolve on this host.
    pub unresolved: Vec<UnresolvedRef>,
}

/// Every `MRSM{a:b:c}` in a value that does not resolve on the host, with where it sits.
fn unresolved_symlinks(host: &Host, value: &Value, path: &str, out: &mut Vec<UnresolvedRef>) {
    static RE: std::sync::LazyLock<regex::Regex> =
        std::sync::LazyLock::new(|| regex::Regex::new(r"MRSM\{([^{}]*?)\}").expect("valid regex"));
    match value {
        Value::Map(m) => {
            for (k, v) in m {
                unresolved_symlinks(host, v, &format!("{path}.{k}"), out);
            }
        }
        Value::List(l) => {
            for (i, v) in l.iter().enumerate() {
                unresolved_symlinks(host, v, &format!("{path}[{i}]"), out);
            }
        }
        Value::Str(s) => {
            for caps in RE.captures_iter(s) {
                let keys_text = &caps[1];
                // `MRSM{!...}` is a literal, not a symlink.
                if keys_text.starts_with('!') || keys_text.is_empty() {
                    continue;
                }
                let keys: Vec<&str> = keys_text.split(':').collect();
                let opts = GetOptions {
                    warn: false,
                    write_missing: false,
                    ..GetOptions::default()
                };
                let (valid, _) = host.config.get_with(&keys, opts);
                let _ = host.config.take_warnings();
                if !valid {
                    out.push(UnresolvedRef {
                        kind: "symlink",
                        name: keys_text.to_string(),
                        location: path.to_string(),
                    });
                }
            }
        }
        _ => {}
    }
}

/// [`read_compose_config`] with the working directory, the inherited root directory of an
/// included project, and the unresolved references.
pub fn read_compose_config_in(
    host: &Host,
    opts: &ReadOptions<'_>,
    process_env: &mut ProcessEnv,
    msgs: &mut Messages,
) -> Result<ReadResult, ComposeError> {
    let ReadOptions {
        cwd,
        compose_file,
        env_file,
        isolated,
        inherit_root,
    } = *opts;
    let mut unresolved: Vec<UnresolvedRef> = Vec::new();
    let env = load_envyaml(compose_file, env_file, cwd, process_env, msgs)?;
    let document = match &env.document {
        Value::Map(m) => m.clone(),
        _ => Map::new(),
    };

    // PARITY: keys are taken from envyaml's merged configuration, so a lower-case environment
    // variable named like a compose key (e.g. `project_name` in `.env`) fills a missing key.
    let mut cc = Map::new();
    for key in COMPOSE_KEYS {
        if let Some(v) = env.cfg.get(*key) {
            cc.insert(key.to_string(), v.clone());
        }
    }

    let compose_cf = cc.get("config").cloned().unwrap_or(Value::new_map());
    unresolved_symlinks(host, &compose_cf, "config", &mut unresolved);
    if compose_cf.truthy() {
        let Value::Map(cf_map) = &compose_cf else {
            return Err(ComposeError::attribute(format!(
                "'{}' object has no attribute 'items'",
                py_type_name(&compose_cf)
            )));
        };
        let substituted = host
            .config
            .search_and_substitute(cf_map)
            .map_err(ComposeError::value)?;
        for w in host.config.take_warnings() {
            msgs.warn(w);
        }
        cc.insert("config".into(), Value::Map(substituted));
    } else {
        let mut connectors = Map::new();
        connectors.insert("connectors".into(), Value::new_map());
        let mut mrsm = Map::new();
        mrsm.insert("meerschaum".into(), Value::Map(connectors));
        cc.insert("config".into(), Value::Map(mrsm));
    }

    // Consolidate the connectors and resolve the `'{type:label}'` host references.
    let root_connectors = match cc.get("connectors") {
        Some(v) if v.truthy() => v.clone(),
        _ => Value::new_map(),
    };
    let nested_connectors = cc
        .get("config")
        .and_then(Value::as_map)
        .and_then(|c| map_get_map(c, "meerschaum"))
        .and_then(|m| m.get("connectors"))
        .filter(|v| v.truthy())
        .cloned()
        .unwrap_or(Value::new_map());
    let connectors_config = apply_patch(&nested_connectors, &root_connectors);

    let mut patch = Map::new();
    if let Value::Map(types) = &connectors_config {
        for (typ, labels) in types {
            patch.entry(typ.clone()).or_insert_with(Value::new_map);
            let labels = match labels {
                Value::Map(m) => m,
                other => {
                    return Err(ComposeError::attribute(format!(
                        "'{}' object has no attribute 'items'",
                        py_type_name(other)
                    )));
                }
            };
            for (label, conn_cf) in labels {
                let target = patch
                    .get_mut(typ)
                    .and_then(Value::as_map_mut)
                    .expect("inserted above");
                match conn_cf {
                    Value::Null => {
                        target.insert(label.clone(), Value::new_map());
                    }
                    Value::Str(s) => {
                        let valid = s.starts_with('{')
                            && s.ends_with('}')
                            && s.matches(':').count() == 1
                            && s.chars().count() >= 3;
                        if !valid {
                            msgs.warn(format!("Invalid config for connector '{typ}:{label}': {s}"));
                            target.insert(label.clone(), Value::new_map());
                            continue;
                        }
                        let (host_typ, host_label) = s.split_once(':').expect("one colon");
                        let host_typ = host_typ.trim_start_matches('{');
                        let host_label = host_label.trim_end_matches('}');
                        let host_cf = host
                            .get_config(&["meerschaum", "connectors", host_typ, host_label], msgs)
                            .unwrap_or(Value::Null);
                        if host_cf.is_null() {
                            unresolved.push(UnresolvedRef {
                                kind: "connector",
                                name: format!("{host_typ}:{host_label}"),
                                location: format!("config.meerschaum.connectors.{typ}.{label}"),
                            });
                        }
                        target.insert(label.clone(), host_cf);
                    }
                    _ => {}
                }
            }
        }
    }
    let connectors_config = apply_patch(&connectors_config, &Value::Map(patch));
    if connectors_config.truthy() {
        let config = cc
            .entry("config".to_string())
            .or_insert_with(Value::new_map);
        if let Value::Map(config) = config {
            let mrsm = config
                .entry("meerschaum".to_string())
                .or_insert_with(Value::new_map);
            if let Value::Map(mrsm) = mrsm {
                mrsm.insert("connectors".into(), connectors_config.clone());
            }
        }
        cc.insert("connectors".into(), connectors_config);
    }

    // PARITY: the root `instance:` key is copied to `config.instance` (not
    // `config.meerschaum.instance`), so it does not change the pipes' default instance.
    let root_instance = cc.get("instance").filter(|v| !v.is_null()).cloned();
    let nested_instance = cc
        .get("config")
        .and_then(Value::as_map)
        .and_then(|c| c.get("instance"))
        .filter(|v| !v.is_null())
        .cloned();
    if let Some(inst) = root_instance {
        if let Some(Value::Map(config)) = cc.get_mut("config") {
            config.insert("instance".into(), inst);
        }
    } else if let Some(inst) = nested_instance {
        cc.insert("instance".into(), inst);
    }

    let isolation = if isolated || cc.get("isolation").and_then(Value::as_str) == Some("subprocess")
    {
        "subprocess"
    } else {
        "config"
    };
    cc.insert("isolation".into(), Value::Str(isolation.into()));
    let daemon = cc.get("daemon").cloned().unwrap_or(Value::Bool(true));
    cc.insert("daemon".into(), daemon);

    cc.insert("__file__".into(), Value::Str(posix(compose_file)));
    ensure_dir_keys_in(&mut cc, host, inherit_root, msgs)?;
    ensure_project_name(&mut cc, msgs)?;

    let root_dir = cc.get("root_dir").map(py_str).unwrap_or_default();
    let mut value = Value::Map(cc);
    replace_config_paths(&mut value, &root_dir, "{MRSM_ROOT_DIR}");
    replace_config_paths(&mut value, &posix(compose_file), "{__file__}");
    let Value::Map(cc) = value else {
        unreachable!("a map stays a map")
    };
    Ok(ReadResult {
        config: cc,
        document,
        unresolved,
    })
}

/// `ensure_dir_keys(compose_config)`.
pub fn ensure_dir_keys(
    config: &mut Map,
    host: &Host,
    msgs: &mut Messages,
) -> Result<(), ComposeError> {
    ensure_dir_keys_in(config, host, None, msgs)
}

/// [`ensure_dir_keys`] where an included project with no `root_dir` (and no `MRSM_ROOT_DIR` in
/// its `environment:`) uses `inherit_root`, the including project's root directory.
pub fn ensure_dir_keys_in(
    config: &mut Map,
    host: &Host,
    inherit_root: Option<&Path>,
    msgs: &mut Messages,
) -> Result<(), ComposeError> {
    let inherits = inherit_root.is_some()
        && !config.contains_key("root_dir")
        && environment_get(config, "MRSM_ROOT_DIR")?.is_none();
    let root = match inherit_root {
        Some(root) if inherits => vec![root.to_path_buf()],
        _ => get_dir_paths(config, "root", host, msgs)?,
    };
    config.insert(
        "root_dir".into(),
        Value::Str(root.first().map(|p| posix(p)).unwrap_or_default()),
    );
    let plugins = get_dir_paths(config, "plugins", host, msgs)?;
    config.insert(
        "plugins_dir".into(),
        Value::List(plugins.iter().map(|p| Value::Str(posix(p))).collect()),
    );
    Ok(())
}

fn environment_get<'a>(config: &'a Map, key: &str) -> Result<Option<&'a Value>, ComposeError> {
    match config.get("environment") {
        None => Ok(None),
        Some(Value::Map(m)) => Ok(m.get(key)),
        Some(other) => Err(ComposeError::attribute(format!(
            "'{}' object has no attribute 'get'",
            py_type_name(other)
        ))),
    }
}

fn parse_json_paths(s: &str) -> Result<Vec<String>, ComposeError> {
    let v = Value::from_json_str(s).map_err(|e| ComposeError::value(e.to_string()))?;
    match v {
        Value::List(items) => Ok(items.iter().map(py_str).collect()),
        other => Err(ComposeError::type_error(format!(
            "'{}' object is not iterable",
            py_type_name(&other)
        ))),
    }
}

/// `get_dir_paths(compose_config, dir_name)` for `root` and `plugins`.
pub fn get_dir_paths(
    config: &Map,
    dir_name: &str,
    host: &Host,
    msgs: &mut Messages,
) -> Result<Vec<PathBuf>, ComposeError> {
    let compose_file = config.get("__file__").map(|v| PathBuf::from(py_str(v)));
    let base_dir = compose_file
        .as_ref()
        .and_then(|f| f.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| host.cwd.clone());
    let configured = config.get(&format!("{dir_name}_dir"));
    let env_dir = environment_get(config, &format!("MRSM_{}_DIR", dir_name.to_uppercase()))?;
    let local_dir = compose_file
        .as_ref()
        .and_then(|f| f.parent().map(|p| p.join(dir_name)));

    let mut env_paths = Vec::new();
    if let Some(Value::Str(env_dir)) = env_dir {
        if env_dir.trim_start().starts_with('[') {
            for s in parse_json_paths(env_dir)? {
                env_paths.push(resolve_against(&base_dir, Path::new(&s)));
            }
        } else {
            env_paths.push(resolve_against(&base_dir, Path::new(env_dir)));
        }
    }

    let configured_vals: Vec<Value> = match configured {
        None => Vec::new(),
        Some(Value::List(items)) => items.clone(),
        Some(v) => vec![v.clone()],
    };
    let mut configured_paths: Vec<PathBuf> = Vec::new();
    for val in configured_vals {
        let path = match &val {
            Value::Null => null_dir(dir_name, host, msgs),
            Value::Str(s) if s.is_empty() => null_dir(dir_name, host, msgs),
            Value::Str(s) => resolve_against(&base_dir, Path::new(s)),
            other => {
                return Err(ComposeError::type_error(format!(
                    "argument should be a str or an os.PathLike object where __fspath__ returns a str, not '{}'",
                    py_type_name(other)
                )));
            }
        };
        if !configured_paths.contains(&path) {
            configured_paths.push(path);
        }
    }

    let mut paths = configured_paths;
    if let Some(local) = local_dir {
        paths.push(resolve_path(&local));
    }
    paths.extend(env_paths);

    let mut unique: Vec<PathBuf> = Vec::new();
    for p in paths {
        let real = resolve_path(&p);
        if !unique.contains(&real) {
            unique.push(real);
        }
    }
    let existing = unique.iter().filter(|p| p.exists()).count();
    if existing > 1 && dir_name != "plugins" {
        let path = unique[0].clone();
        msgs.warn(format!(
            "Detected multiple values for {dir_name}_dir.\n   Compose will use '{}' for {dir_name}_dir.",
            path.display()
        ));
        return Ok(vec![path]);
    }
    Ok(unique)
}

fn null_dir(dir_name: &str, host: &Host, msgs: &mut Messages) -> PathBuf {
    if dir_name == "plugins" {
        msgs.info(
            "A null value for `plugins_dir` will include your regular Meerschaum plugins.\n    \
             This will be less isolated than project-specific plugins directories.",
        );
        host.path("PLUGINS_RESOURCES_PATH")
    } else {
        msgs.info("A null value for `root_dir` will use the host Meerschaum root directory.");
        host.path("ROOT_DIR_PATH")
    }
}

/// `replace_config_paths(config, path, pattern)`: replace a pattern in every string.
pub fn replace_config_paths(value: &mut Value, path: &str, pattern: &str) {
    match value {
        Value::Map(m) => m
            .values_mut()
            .for_each(|v| replace_config_paths(v, path, pattern)),
        Value::List(l) => l
            .iter_mut()
            .for_each(|v| replace_config_paths(v, path, pattern)),
        Value::Str(s) if s.contains(pattern) => {
            *s = s.replace(pattern, path);
        }
        _ => {}
    }
}

/// `get_project_name(compose_config)`.
pub fn get_project_name(config: &Map, msgs: &mut Messages) -> Result<Value, ComposeError> {
    if let Some(name) = config.get("project_name").filter(|v| v.truthy()) {
        return Ok(name.clone());
    }
    // PARITY: Python reads a top-level `stack` key here, which `read_compose_config` never
    // keeps (stack.py:17), so this branch only fires for hand-built configs.
    if let Some(name) = config
        .get("stack")
        .and_then(Value::as_map)
        .and_then(|s| s.get("project_name"))
        .filter(|v| v.truthy())
    {
        msgs.warn(format!(
            "Detected a project_name '{}' under 'config:stack:project_name'.\n    \
             Will use this as the project name for this compose file.",
            py_str(name)
        ));
        return Ok(name.clone());
    }
    let Some(file) = config.get("__file__").map(|v| PathBuf::from(py_str(v))) else {
        return Err(ComposeError::value(
            "Could not determine the project name. Is `project_name` set?",
        ));
    };
    // PARITY: the default is the parent directory's *stem* (`my.project` -> `my`).
    let dir_name = file
        .parent()
        .and_then(|p| p.file_name())
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_default();
    Ok(Value::Str(py_stem(&dir_name)))
}

/// `ensure_project_name(compose_config)`.
pub fn ensure_project_name(config: &mut Map, msgs: &mut Messages) -> Result<(), ComposeError> {
    let name = get_project_name(config, msgs)?;
    config.insert("project_name".into(), name.clone());
    let cfg = config
        .entry("config".to_string())
        .or_insert_with(Value::new_map);
    if let Value::Map(cfg) = cfg {
        let stack = cfg
            .entry("stack".to_string())
            .or_insert_with(Value::new_map);
        if let Value::Map(stack) = stack {
            stack.insert("project_name".into(), name);
        }
    }
    Ok(())
}

/// `json.dumps(value, separators=(',', ':'))`.
pub fn json_compact(value: &Value) -> String {
    json_dumps(value, JsonStyle::Compact, true)
}

fn resolve_abs(path_str: &str, base: &Path) -> String {
    posix(&resolve_against(base, Path::new(path_str)))
}

/// `get_env_dict(compose_config)`: the environment every subaction runs under.
pub fn get_env_dict(config: &Map, process_env: &ProcessEnv) -> Result<EnvDict, ComposeError> {
    get_env_dict_in(config, process_env, None)
}

/// [`get_env_dict`] for a project that runs in a tree: `compose_root` (the root compose file)
/// is added as the [`COMPOSE_ROOT_VAR`] label the jobs carry.
pub fn get_env_dict_in(
    config: &Map,
    process_env: &ProcessEnv,
    compose_root: Option<&Path>,
) -> Result<EnvDict, ComposeError> {
    let mut env: EnvDict = process_env
        .iter()
        .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
        .collect();

    if cfg!(windows) {
        let app_data = process_env.get("AppData").cloned().unwrap_or_default();
        let home = process_env.get("HOME").cloned().unwrap_or_default();
        let homepath = process_env.get("HOMEPATH").cloned().unwrap_or(home.clone());
        env.insert("AppData".into(), Value::Str(app_data));
        env.insert("HOME".into(), Value::Str(home));
        env.insert("HOMEPATH".into(), Value::Str(homepath));
    }

    if let Some(Value::Str(root)) = config.get("root_dir") {
        env.insert("MRSM_ROOT_DIR".into(), Value::Str(root.clone()));
    }
    match config.get("plugins_dir") {
        Some(Value::List(items)) if !items.is_empty() => {
            env.insert(
                "MRSM_PLUGINS_DIR".into(),
                Value::Str(json_compact(&Value::List(items.clone()))),
            );
        }
        Some(v @ Value::Str(_)) if v.truthy() => {
            env.insert("MRSM_PLUGINS_DIR".into(), v.clone());
        }
        _ => {}
    }

    env.insert(
        "MRSM__COMPOSE_CONFIG".into(),
        Value::Str(json_compact(&Value::Map(config.clone()))),
    );

    let statics = static_config();
    let env_names = statics.get(&["environment"]).and_then(Value::as_map);
    let env_name = |key: &str, default: &str| -> String {
        env_names
            .and_then(|m| m.get(key))
            .and_then(Value::as_str)
            .unwrap_or(default)
            .to_string()
    };
    let mut scratch = Messages::default();
    if let Ok(name) = get_project_name(config, &mut scratch) {
        env.insert(env_name("compose_project", "MRSM__COMPOSE_PROJECT"), name);
    }

    if let Some(cfg) = config.get("config").filter(|v| v.truthy()) {
        env.insert("MRSM_CONFIG".into(), Value::Str(json_compact(cfg)));
    }

    match config.get("environment") {
        Some(Value::Map(m)) if !m.is_empty() => {
            for (k, v) in m {
                env.insert(k.clone(), v.clone());
            }
        }
        Some(Value::List(l)) if !l.is_empty() => {
            return Err(ComposeError::value(
                "dictionary update sequence element #0 has length 1; 2 is required",
            ));
        }
        _ => {}
    }

    let compose_file = config.get("__file__").map(|v| PathBuf::from(py_str(v)));
    let base_dir = compose_file
        .as_ref()
        .and_then(|f| f.parent().map(Path::to_path_buf))
        .unwrap_or_else(|| PathBuf::from("."));
    for key in ["root", "config_dir", "plugins", "venvs", "work_dir"] {
        let Some(var) = env_names
            .and_then(|m| m.get(key))
            .and_then(Value::as_str)
            .map(String::from)
        else {
            continue;
        };
        let Some(Value::Str(val)) = env.get(&var).cloned() else {
            continue;
        };
        if val.is_empty() {
            continue;
        }
        if val.trim_start().starts_with('[') {
            let Ok(items) = parse_json_paths(&val) else {
                continue;
            };
            let resolved: Vec<Value> = items
                .iter()
                .map(|s| Value::Str(resolve_abs(s, &base_dir)))
                .collect();
            env.insert(var, Value::Str(json_compact(&Value::List(resolved))));
        } else {
            env.insert(var, Value::Str(resolve_abs(&val, &base_dir)));
        }
    }

    if let Some(root) = compose_root {
        env.insert(COMPOSE_ROOT_VAR.into(), Value::Str(posix(root)));
    }

    for v in env.values_mut() {
        if v.is_null() {
            *v = Value::Str(String::new());
        }
    }
    Ok(env)
}

/// Apply an [`EnvDict`] as a process environment (`replace_env(env)`): every value must be a
/// string, as `os.environ` requires.
pub fn env_dict_as_process_env(env: &EnvDict) -> Result<ProcessEnv, ComposeError> {
    let mut out = ProcessEnv::new();
    for (k, v) in env {
        match v {
            Value::Str(s) => {
                out.insert(k.clone(), s.clone());
            }
            other => {
                return Err(ComposeError::type_error(format!(
                    "str expected, not {}",
                    py_type_name(other)
                )));
            }
        }
    }
    Ok(out)
}

fn sort_keys(value: &Value) -> Value {
    match value {
        Value::Map(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            Value::Map(
                keys.into_iter()
                    .map(|k| (k.clone(), sort_keys(&m[k])))
                    .collect(),
            )
        }
        Value::List(l) => Value::List(l.iter().map(sort_keys).collect()),
        other => other.clone(),
    }
}

/// `json.dumps(value, sort_keys=True, separators=(',', ':'))`.
pub fn json_sorted_compact(value: &Value) -> String {
    json_dumps(&sort_keys(value), JsonStyle::Compact, true)
}

/// `hash_config(compose_config)`: the SHA-256 of `json.dumps(config, sort_keys=True, default=str)`.
pub fn hash_config(config: &Map) -> String {
    let text = json_dumps(
        &sort_keys(&Value::Map(config.clone())),
        JsonStyle::Default,
        true,
    );
    let digest = Sha256::digest(text.as_bytes());
    digest.iter().map(|b| format!("{b:02x}")).collect()
}

/// `get_config_cache_path(compose_config)`.
pub fn config_cache_path(project: &Project) -> PathBuf {
    match &project.cache_suffix {
        // Included projects share the root directory: one cache file each.
        Some(suffix) => project.root_dir().join(format!(
            ".compose-cache.{}.pkl",
            suffix.replace(['/', '\\'], "_")
        )),
        None => project.root_dir().join(CONFIG_CACHE_FILENAME),
    }
}

/// `read_config_cache(compose_config)`: the cached hash, if valid.
pub fn read_config_cache(project: &Project) -> Option<String> {
    let text = std::fs::read(config_cache_path(project)).ok()?;
    let text = String::from_utf8(text).ok()?;
    let text = text.trim();
    (text.len() == 64 && text.chars().all(|c| "0123456789abcdef".contains(c)))
        .then(|| text.to_string())
}

/// `write_config_cache(compose_config)`.
pub fn write_config_cache(project: &Project) -> std::io::Result<()> {
    std::fs::write(config_cache_path(project), hash_config(&project.config))
}

/// `config_has_changed(compose_config)`, memoized per process like Python's `CONFIG_METADATA`.
pub fn config_has_changed(project: &Project, session: &mut Session) -> bool {
    let key = posix(&resolve_path(&config_cache_path(project)));
    let hashed = hash_config(&project.config);
    if let Some((h, changed)) = session.config_metadata.get(&key)
        && *h == hashed
    {
        return *changed;
    }
    let changed = read_config_cache(project).as_deref() != Some(hashed.as_str());
    session.config_metadata.insert(key, (hashed, changed));
    changed
}

/// `meerschaum.compose.utils.init(file, env_file, isolated)` without `init_root` (see
/// [`crate::subactions::init_root`]): locate, load and resolve the project.
pub fn load_project(
    host: &Host,
    file: Option<&Path>,
    env_file: Option<&Path>,
    isolated: bool,
    msgs: &mut Messages,
) -> Result<Project, ComposeError> {
    load_project_in(
        host,
        &LoadOptions {
            file,
            env_file,
            isolated,
            ..LoadOptions::default()
        },
        msgs,
    )
}

/// How to load a project (see [`load_project_in`]).
#[derive(Debug, Clone, Default)]
pub struct LoadOptions<'a> {
    /// `--file`.
    pub file: Option<&'a Path>,
    /// `--env-file`.
    pub env_file: Option<&'a Path>,
    /// `--isolated`.
    pub isolated: bool,
    /// The working directory (default: the host's). An included project resolves its `.env`
    /// from its own directory.
    pub cwd: Option<&'a Path>,
    /// The process environment the file is read under (default: the host's). An included
    /// project is read under the including project's environment, so the root's variables win.
    pub env: Option<&'a ProcessEnv>,
    /// The root directory an included project without a `root_dir` uses.
    pub inherit_root: Option<&'a Path>,
}

/// [`load_project`] with [`LoadOptions`].
pub fn load_project_in(
    host: &Host,
    opts: &LoadOptions<'_>,
    msgs: &mut Messages,
) -> Result<Project, ComposeError> {
    let cwd = opts.cwd.unwrap_or(&host.cwd);
    let Some(compose_file) = infer_compose_file_path(opts.file, cwd, msgs) else {
        return Err(ComposeError::new(
            ErrorKind::FileNotFound,
            "No compose file could be found.\n    \
             Create a file mrsm-compose.yaml or specify a path with `--file`.",
        ));
    };
    let mut process_env = opts.env.cloned().unwrap_or_else(|| host.env.clone());
    init_env(&compose_file, opts.env_file, &mut process_env, msgs);
    let read = read_compose_config_in(
        host,
        &ReadOptions {
            cwd,
            compose_file: &compose_file,
            env_file: opts.env_file,
            isolated: opts.isolated,
            inherit_root: opts.inherit_root,
        },
        &mut process_env,
        msgs,
    )?;
    Ok(Project {
        config: read.config,
        file: compose_file,
        process_env,
        document: read.document,
        unresolved: read.unresolved,
        compose_root: None,
        cache_suffix: None,
        excluded_pipes: Vec::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_root_label_is_the_static_environment_name() {
        assert_eq!(COMPOSE_ROOT_VAR, static_config().environment.compose_root);
    }

    #[test]
    fn items_str_matches_python() {
        let v = |xs: &[&str]| xs.iter().map(|s| s.to_string()).collect::<Vec<_>>();
        assert_eq!(items_str(&v(&["a"])), "'a'");
        assert_eq!(items_str(&v(&["a", "b"])), "'a' and 'b'");
        assert_eq!(items_str(&v(&["a", "b", "c"])), "'a', 'b', and 'c'");
    }

    #[test]
    fn stems_like_pathlib() {
        assert_eq!(py_stem("my.project"), "my");
        assert_eq!(py_stem(".hidden"), ".hidden");
        assert_eq!(py_stem("plain"), "plain");
    }

    #[test]
    fn parses_missing_vars_like_python() {
        assert_eq!(
            missing_vars_from_message("Strict mode enabled, variables $A, $B are not defined!"),
            vec!["$A".to_string(), "$B".to_string()]
        );
    }
}
