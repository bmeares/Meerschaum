//! Catalog records: the filters and entries of `fetch_pipes_keys`, column index information
//! and plugin registrations.

use std::fmt;

use indexmap::IndexMap;
use meerschaum_core::{Map, Value};
use serde::{Deserialize, Serialize};

use super::PipeId;

/// The filters of `fetch_pipes_keys` (`-c`, `-m`, `-l`, `--tags`, `params`).
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PipesKeysQuery {
    /// `connector_keys` (`-c`); a `_` prefix negates.
    pub connector_keys: Vec<String>,
    /// `metric_keys` (`-m`).
    pub metric_keys: Vec<String>,
    /// `location_keys` (`-l`); `None` matches null locations.
    pub location_keys: Vec<Option<String>>,
    /// `tags` (`--tags`): comma-separated groups are ANDed, groups are ORed.
    pub tags: Vec<String>,
    /// `params`: extra filters against the pipes table.
    pub params: Option<Map>,
}

/// One entry of `fetch_pipes_keys`: `(connector_keys, metric_key, location_key[, extra])`.
#[derive(Debug, Clone, PartialEq)]
pub struct PipeKeysEntry {
    /// The connector keys.
    pub connector_keys: String,
    /// The metric key.
    pub metric_key: String,
    /// The location key (`None` or the string `"None"` for null locations).
    pub location_key: Option<String>,
    /// The optional fourth element: the parameters dict or the tags list.
    pub extra: Option<Value>,
}

impl PipeKeysEntry {
    /// Build an entry from a JSON list `[ck, mk, lk, extra?]`.
    pub fn from_list(items: &[Value]) -> Option<PipeKeysEntry> {
        let s = |v: &Value| match v {
            Value::Null => None,
            other => Some(meerschaum_core::pyfmt::py_str(other)),
        };
        Some(PipeKeysEntry {
            connector_keys: s(items.first()?)?,
            metric_key: s(items.get(1)?)?,
            location_key: items.get(2).and_then(s),
            extra: items.get(3).cloned(),
        })
    }

    /// The entry as a JSON-like list.
    pub fn to_list(&self) -> Vec<Value> {
        let mut out = vec![
            Value::Str(self.connector_keys.clone()),
            Value::Str(self.metric_key.clone()),
            self.location_key.clone().into(),
        ];
        if let Some(extra) = &self.extra {
            out.push(extra.clone());
        }
        out
    }
}

/// The return value of `fetch_pipes_keys`: a list of key tuples, or a dict mapping pipe IDs to
/// key tuples (the dict form returns parameters in one round trip, §5.1).
#[derive(Debug, Clone, PartialEq)]
pub enum PipesKeys {
    /// `[(ck, mk, lk[, extra]), ...]`.
    List(Vec<PipeKeysEntry>),
    /// `{pipe_id: (ck, mk, lk[, extra])}`.
    Dict(IndexMap<PipeId, PipeKeysEntry>),
}

impl PipesKeys {
    /// The entries regardless of form, in order.
    pub fn entries(&self) -> Vec<&PipeKeysEntry> {
        match self {
            PipesKeys::List(v) => v.iter().collect(),
            PipesKeys::Dict(m) => m.values().collect(),
        }
    }

    /// The number of entries.
    pub fn len(&self) -> usize {
        match self {
            PipesKeys::List(v) => v.len(),
            PipesKeys::Dict(m) => m.len(),
        }
    }

    /// Whether there are no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// One index on a column (`get_pipe_columns_indices`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnIndexInfo {
    /// The index name.
    pub name: String,
    /// The index type (`'INDEX'`, `'PRIMARY KEY'`, ...).
    #[serde(rename = "type")]
    pub index_type: String,
}

/// A plugin's registration (`meerschaum.core.Plugin` as instances see it). Plugins themselves
/// stay in Python (§6.1); instances only store these fields.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PluginRecord {
    /// The plugin name.
    pub name: String,
    /// The plugin version.
    pub version: Option<String>,
    /// The owner's user ID.
    pub user_id: Option<Value>,
    /// `plugin.attributes`.
    pub attributes: Map,
}

impl fmt::Display for PluginRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.name)
    }
}
