//! A typed, serde-friendly model of `mrsm-compose.yaml` (see `docs/zensical/reference/compose.md`
//! and §6.3 of the v5 design).
//!
//! The model is a *view*: Python never validates the file, so every field is lenient (values
//! that don't fit the documented shape are kept as raw [`Value`]s) and unknown keys are kept in
//! [`ComposeFile::extra`] (reported by the lint, never rejected). Key order is preserved.
//! Behavior (resolution, environments, commands) runs on the resolved [`Map`] in
//! [`crate::project::Project`], which is what the Python implementation operates on.

use indexmap::IndexMap;
use meerschaum_core::value::{Map, Value};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// `plugins_dir`: a path, a list of paths, `null` entries meaning the host's plugins.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum PluginsDir {
    /// A single path (or `null`).
    One(Option<String>),
    /// Several paths (`null` entries include the host plugins).
    Many(Vec<Option<String>>),
    /// Anything else (kept as written).
    Other(Value),
}

impl PluginsDir {
    /// The configured entries.
    pub fn entries(&self) -> Vec<Option<String>> {
        match self {
            PluginsDir::One(p) => vec![p.clone()],
            PluginsDir::Many(v) => v.clone(),
            PluginsDir::Other(_) => Vec::new(),
        }
    }
}

/// A required plugin: `name` or `name@api:repo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginSpec {
    /// The plugin name.
    pub name: String,
    /// The repository keys after `@`, if any.
    pub repository: Option<String>,
}

impl PluginSpec {
    /// Parse `name@repo` (split on the configured `repo_separator`).
    pub fn parse(s: &str) -> PluginSpec {
        let sep = meerschaum_core::static_config::static_config()
            .get(&["plugins", "repo_separator"])
            .and_then(Value::as_str)
            .unwrap_or("@")
            .to_string();
        let mut parts = s.split(sep.as_str());
        PluginSpec {
            name: parts.next().unwrap_or_default().to_string(),
            repository: parts.next().map(String::from),
        }
    }
}

impl Serialize for PluginSpec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match &self.repository {
            Some(r) => s.serialize_str(&format!("{}@{}", self.name, r)),
            None => s.serialize_str(&self.name),
        }
    }
}

impl<'de> Deserialize<'de> for PluginSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let v = Value::deserialize(d)?;
        Ok(PluginSpec::parse(&meerschaum_core::pyfmt::py_str(&v)))
    }
}

/// A connector definition under `connectors:`.
#[derive(Debug, Clone, PartialEq)]
pub enum ConnectorSpec {
    /// A compose host reference: `'{type:label}'`.
    HostReference {
        /// The host connector type.
        typ: String,
        /// The host connector label.
        label: String,
    },
    /// Connector attributes.
    Attributes(Map),
    /// `null` (an empty connector).
    Null,
    /// A string that is not a valid reference (Python warns and uses `{}`).
    Invalid(String),
    /// Any other value (ignored by Python).
    Other(Value),
}

impl ConnectorSpec {
    /// Classify a raw value like `read_compose_config` does.
    pub fn from_value(v: &Value) -> ConnectorSpec {
        match v {
            Value::Null => ConnectorSpec::Null,
            Value::Map(m) => ConnectorSpec::Attributes(m.clone()),
            Value::Str(s) => {
                if s.starts_with('{')
                    && s.ends_with('}')
                    && s.matches(':').count() == 1
                    && s.chars().count() >= 3
                {
                    let (t, l) = s.split_once(':').expect("one colon");
                    ConnectorSpec::HostReference {
                        typ: t.trim_start_matches('{').to_string(),
                        label: l.trim_end_matches('}').to_string(),
                    }
                } else {
                    ConnectorSpec::Invalid(s.clone())
                }
            }
            other => ConnectorSpec::Other(other.clone()),
        }
    }

    /// The value as written.
    pub fn to_value(&self) -> Value {
        match self {
            ConnectorSpec::HostReference { typ, label } => Value::Str(format!("{{{typ}:{label}}}")),
            ConnectorSpec::Attributes(m) => Value::Map(m.clone()),
            ConnectorSpec::Null => Value::Null,
            ConnectorSpec::Invalid(s) => Value::Str(s.clone()),
            ConnectorSpec::Other(v) => v.clone(),
        }
    }
}

impl Serialize for ConnectorSpec {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        self.to_value().serialize(s)
    }
}

impl<'de> Deserialize<'de> for ConnectorSpec {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(ConnectorSpec::from_value(&Value::deserialize(d)?))
    }
}

/// `sync.args`: a command-line string or a list of arguments.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SyncArgs {
    /// A string, split with `shlex`.
    Line(String),
    /// A list of arguments.
    List(Vec<String>),
    /// Anything else.
    Other(Value),
}

/// A pipe definition: the keyword arguments to `mrsm.Pipe(...)`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PipeSpec {
    /// `connector` (connector keys).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connector: Option<Value>,
    /// `metric`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric: Option<Value>,
    /// `location`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<Value>,
    /// `instance`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<Value>,
    /// `parameters`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<Value>,
    /// `columns` (alias for `parameters:columns`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub columns: Option<Value>,
    /// `target` (alias for `parameters:target`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<Value>,
    /// `tags` (alias for `parameters:tags`; must be a list).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Value>,
    /// `dtypes` (alias for `parameters:dtypes`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dtypes: Option<Value>,
    /// Every other keyword argument (`location_key`, `mrsm_instance`, `temporary`, `upsert`,
    /// ... and misspellings, which make `Pipe()` raise).
    #[serde(flatten)]
    pub extra: IndexMap<String, Value>,
}

/// The `sync:` section.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SyncSpec {
    /// `schedule` (`-s`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schedule: Option<Value>,
    /// `min_seconds` (`--min-seconds`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_seconds: Option<Value>,
    /// `timeout_seconds` (`--timeout-seconds`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<Value>,
    /// `args`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub args: Option<SyncArgs>,
    /// `pipes` nested under `sync:` by mistake (loaded before the top-level pipes).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipes: Option<Vec<PipeSpec>>,
    /// Unknown keys.
    #[serde(flatten)]
    pub extra: IndexMap<String, Value>,
}

/// A compose file.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ComposeFile {
    /// `project_name` (custom file stem, or directory name for `mrsm-compose.yaml` / `.yml`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_name: Option<Value>,
    /// `root_dir` (default `./root`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub root_dir: Option<Value>,
    /// `plugins_dir` (default `./plugins`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugins_dir: Option<PluginsDir>,
    /// `plugins`: names and `name@api:repo`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plugins: Option<Vec<PluginSpec>>,
    /// `sync`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync: Option<SyncSpec>,
    /// `config`: patched onto the host configuration (`MRSM{}` symlinks allowed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config: Option<Value>,
    /// `connectors` (alias for `config:meerschaum:connectors`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connectors: Option<IndexMap<String, IndexMap<String, ConnectorSpec>>>,
    /// `instance` (copied to `config:instance`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance: Option<Value>,
    /// `environment`: extra environment variables.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub environment: Option<IndexMap<String, Value>>,
    /// `pipes`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipes: Option<Vec<PipeSpec>>,
    /// `jobs`: job name to command line.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub jobs: Option<IndexMap<String, Value>>,
    /// `isolation` (`subprocess` runs every command in a subprocess).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub isolation: Option<Value>,
    /// `daemon` (`false` appends `--no-daemon`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub daemon: Option<Value>,
    /// `include` (v5): compose files, directories and globs this project includes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub include: Option<Value>,
    /// `parent` (v5): the compose file or directory of the project that includes this one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent: Option<Value>,
    /// Unknown top-level keys (ignored by compose, reported by the lint).
    #[serde(flatten)]
    pub extra: IndexMap<String, Value>,
}

impl ComposeFile {
    /// Build the model from a parsed YAML document. Fails only when a section has a shape the
    /// model can't hold (e.g. `pipes:` is not a list); see [`ComposeFile::from_value_lenient`].
    pub fn from_value(doc: &Value) -> Result<ComposeFile, serde_json::Error> {
        let text = serde_json::to_string(doc)?;
        serde_json::from_str(&text)
    }

    /// Like [`ComposeFile::from_value`], but sections that don't fit are moved to
    /// [`ComposeFile::extra`] under their own key instead of failing.
    pub fn from_value_lenient(doc: &Value) -> ComposeFile {
        if let Ok(model) = ComposeFile::from_value(doc) {
            return model;
        }
        let Value::Map(map) = doc else {
            return ComposeFile::default();
        };
        let mut kept = Map::new();
        let mut extra = IndexMap::new();
        for (k, v) in map {
            let mut single = Map::new();
            single.insert(k.clone(), v.clone());
            if ComposeFile::from_value(&Value::Map(single)).is_ok() {
                kept.insert(k.clone(), v.clone());
            } else {
                extra.insert(k.clone(), v.clone());
            }
        }
        let mut model = ComposeFile::from_value(&Value::Map(kept)).unwrap_or_default();
        model.extra.extend(extra);
        model
    }

    /// Serialize back to a value (key order preserved).
    pub fn to_value(&self) -> Result<Value, serde_json::Error> {
        let text = serde_json::to_string(self)?;
        Value::from_json_str(&text)
    }
}

/// Deserialize a [`ComposeFile`] from YAML text (no environment substitution).
pub fn parse_yaml(text: &str) -> Result<ComposeFile, String> {
    let v = meerschaum_core::yaml::load(text).map_err(|e| e.to_string())?;
    ComposeFile::from_value(&v).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn model_round_trips_with_order_and_unknown_keys() {
        let text = "project_name: p\nzeta: 1\nplugins:\n  - a\n  - b@api:custom\nconnectors:\n  sql:\n    x: '{sql:main}'\n    y:\n      flavor: sqlite\n    z:\nalpha: 2\nsync:\n  pipes:\n    - connector: a:b\n      metric: c\n      dtype: {}\n";
        let model = parse_yaml(text).unwrap();
        assert_eq!(
            model.extra.keys().collect::<Vec<_>>(),
            vec!["zeta", "alpha"]
        );
        assert_eq!(
            model.plugins.as_ref().unwrap()[1],
            PluginSpec {
                name: "b".into(),
                repository: Some("api:custom".into())
            }
        );
        let conns = &model.connectors.as_ref().unwrap()["sql"];
        assert!(matches!(conns["x"], ConnectorSpec::HostReference { .. }));
        assert!(matches!(conns["z"], ConnectorSpec::Null));
        let nested = model.sync.as_ref().unwrap().pipes.as_ref().unwrap();
        assert!(nested[0].extra.contains_key("dtype"));
        let back = model.to_value().unwrap();
        let keys: Vec<&String> = back.as_map().unwrap().keys().collect();
        assert_eq!(
            keys,
            vec![
                "project_name",
                "plugins",
                "sync",
                "connectors",
                "zeta",
                "alpha"
            ]
        );
    }
}
