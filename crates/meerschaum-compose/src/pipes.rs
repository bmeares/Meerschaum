//! The pipes defined in a compose project: a port of `meerschaum/compose/utils/pipes.py`.

use indexmap::IndexMap;
use meerschaum_core::config::GetOptions;
use meerschaum_core::pipe::{Pipe, PipeDefaults};
use meerschaum_core::value::{Map, Value};

use crate::error::{ComposeError, Messages};
use crate::project::{Host, Project, get_project_name, py_type_name};

/// The project's view of the config registry while a subaction runs: Python swaps the in-memory
/// config for the project's `config:` block (`replace_config`), so lookups hit that block first
/// and fall back to the defaults.
#[derive(Debug)]
pub struct ProjectConfig {
    registry: meerschaum_core::Config,
}

impl ProjectConfig {
    /// Build the project registry from the host's options and the project's `config:` block.
    pub fn new(host: &Host, project: &Project) -> Result<ProjectConfig, ComposeError> {
        let mut opts = meerschaum_core::config::ConfigOptions::with_env(
            host.env
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        );
        opts.allow_writes = false;
        if let Some(home) = host.env.get("HOME") {
            opts.home = std::path::PathBuf::from(home);
        }
        let registry = meerschaum_core::Config::new(opts)
            .map_err(|e| ComposeError::new(crate::error::ErrorKind::Os, e.to_string()))?;
        registry.set_config(project.config_block());
        Ok(ProjectConfig { registry })
    }

    /// `get_config(*keys)` inside the project.
    pub fn get(&self, keys: &[&str]) -> Option<Value> {
        let opts = GetOptions {
            warn: false,
            write_missing: false,
            ..GetOptions::default()
        };
        let (valid, value) = self.registry.get_with(keys, opts);
        let _ = self.registry.take_warnings();
        valid.then_some(value)
    }

    /// The underlying registry (for `MRSM{}` symlinks in pipe parameters).
    pub fn registry(&self) -> &meerschaum_core::Config {
        &self.registry
    }

    /// The pipe construction defaults inside the project (the default instance is the
    /// project's `meerschaum:instance`).
    pub fn pipe_defaults(&self) -> PipeDefaults {
        PipeDefaults::from_config(&self.registry)
    }
}

/// `get_defined_pipes(compose_config, as_meta=True)`: the keyword arguments for each pipe, with
/// the project tag appended, the default instance filled in and `cache` defaulted.
pub fn get_defined_pipes_meta(
    config: &Map,
    project_config: &ProjectConfig,
    cache: bool,
    msgs: &mut Messages,
) -> Result<Vec<Map>, ComposeError> {
    let project_name = get_project_name(config, msgs)?;
    let default_instance = config
        .get("config")
        .and_then(Value::as_map)
        .and_then(|c| c.get("meerschaum"))
        .and_then(Value::as_map)
        .and_then(|m| m.get("instance"))
        .cloned()
        .or_else(|| project_config.get(&["meerschaum", "instance"]))
        .unwrap_or(Value::Null);

    let sync_pipes = match config.get("sync") {
        None => Value::List(Vec::new()),
        Some(Value::Map(sync)) => sync
            .get("pipes")
            .cloned()
            .unwrap_or(Value::List(Vec::new())),
        Some(other) => {
            return Err(ComposeError::attribute(format!(
                "'{}' object has no attribute 'get'",
                py_type_name(other)
            )));
        }
    };
    let global_pipes = config
        .get("pipes")
        .cloned()
        .unwrap_or(Value::List(Vec::new()));
    let (sync_pipes, global_pipes) = match (sync_pipes, global_pipes) {
        (Value::List(a), Value::List(b)) => (a, b),
        (Value::List(_), other) => {
            return Err(ComposeError::type_error(format!(
                "can only concatenate list (not \"{}\") to list",
                py_type_name(&other)
            )));
        }
        (other, b) => {
            return Err(ComposeError::type_error(format!(
                "unsupported operand type(s) for +: '{}' and '{}'",
                py_type_name(&other),
                py_type_name(&b)
            )));
        }
    };

    let mut out = Vec::new();
    // PARITY: pipes nested under `sync:` (a common mistake) are loaded, before the top-level
    // `pipes:` (pipes.py:37), without a warning; the lint reports them.
    for meta in sync_pipes.into_iter().chain(global_pipes) {
        let Value::Map(mut meta) = meta else {
            return Err(ComposeError::type_error(format!(
                "'{}' object does not support item assignment",
                py_type_name(&meta)
            )));
        };
        let tags = meta
            .entry("tags".to_string())
            .or_insert_with(|| Value::List(Vec::new()));
        match tags {
            Value::List(l) => l.push(project_name.clone()),
            other => {
                return Err(ComposeError::attribute(format!(
                    "'{}' object has no attribute 'append'",
                    py_type_name(other)
                )));
            }
        }
        if !meta.get("instance").is_some_and(Value::truthy)
            && !meta.get("mrsm_instance").is_some_and(Value::truthy)
        {
            meta.insert("instance".into(), default_instance.clone());
        }
        if !meta.contains_key("cache") {
            meta.insert("cache".into(), Value::Bool(cache));
        }
        out.push(meta);
    }
    Ok(out)
}

/// `mrsm.Pipe(**pipe_meta)` for each metadata dictionary.
pub fn build_pipes(metas: &[Map], defaults: &PipeDefaults) -> Result<Vec<Pipe>, ComposeError> {
    metas
        .iter()
        .map(|meta| {
            Pipe::from_call(&[], meta, defaults)
                .map_err(|e| ComposeError::type_error(e.to_string()))
        })
        .collect()
}

/// `instance_pipes_from_pipes_list(pipes)`: indices of the pipes grouped by instance keys, in
/// order of first appearance.
pub fn instance_pipes(pipes: &[Pipe]) -> IndexMap<String, Vec<usize>> {
    let mut out: IndexMap<String, Vec<usize>> = IndexMap::new();
    for (i, pipe) in pipes.iter().enumerate() {
        out.entry(pipe.instance_keys().to_string())
            .or_default()
            .push(i);
    }
    out
}

/// The custom connectors to build (`build_custom_connectors`): `(keys, attributes)` for every
/// connector under `config:meerschaum:connectors`.
pub fn custom_connectors(config: &Map) -> Result<Vec<(String, Map)>, ComposeError> {
    let Some(types) = config
        .get("config")
        .and_then(Value::as_map)
        .and_then(|c| c.get("meerschaum"))
        .and_then(Value::as_map)
        .and_then(|m| m.get("connectors"))
        .and_then(Value::as_map)
    else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    for (typ, labels) in types {
        let Value::Map(labels) = labels else {
            return Err(ComposeError::attribute(format!(
                "'{}' object has no attribute 'items'",
                py_type_name(labels)
            )));
        };
        for (label, attrs) in labels {
            match attrs {
                Value::Map(m) => out.push((format!("{typ}:{label}"), m.clone())),
                // PARITY: `mrsm.get_connector(keys, **None)` raises (a stale `'{sql:x}'` host
                // reference resolves to `None`).
                other => {
                    return Err(ComposeError::type_error(format!(
                        "meerschaum.connectors.get_connector() argument after ** must be a mapping, not {}",
                        py_type_name(other)
                    )));
                }
            }
        }
    }
    Ok(out)
}

/// `str(pipe.location_key)` as passed to `-l` (`None` for no location).
pub fn location_arg(pipe: &Pipe) -> String {
    pipe.location_key()
        .map_or_else(|| "None".to_string(), String::from)
}
