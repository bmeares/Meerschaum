//! Node identity (§5.1): [`PipeRef`] and [`PipeRecord`].
//!
//! Pipe IDs are instance-local, so the engine never keys persisted state on `pipe_id`. Every
//! node, edge, ledger entry and cursor uses the `pipe_ref` string
//! `"{instance_keys}|{connector_keys}|{metric_key}|{location_key}"`, with a missing location
//! written as `None` (the same string core's `Pipe::pipe_ref` produces).

use std::fmt;

use meerschaum_core::pipe::{PipeDefaults, PipeError};
use meerschaum_core::pyfmt::py_str;
use meerschaum_core::{Map, Pipe, Value};
use serde::{Deserialize, Serialize};

/// A node's identity: `"{instance_keys}|{connector_keys}|{metric_key}|{location_key}"`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PipeRef(String);

/// The four keys of a [`PipeRef`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PipeRefParts<'a> {
    /// The instance keys.
    pub instance_keys: &'a str,
    /// The connector keys.
    pub connector_keys: &'a str,
    /// The metric key.
    pub metric_key: &'a str,
    /// The location key (`None` when the stored text is `None`).
    pub location_key: Option<&'a str>,
}

impl PipeRef {
    /// Build a `pipe_ref` from its four keys.
    pub fn new(
        instance_keys: &str,
        connector_keys: &str,
        metric_key: &str,
        location_key: Option<&str>,
    ) -> PipeRef {
        PipeRef(format!(
            "{instance_keys}|{connector_keys}|{metric_key}|{}",
            location_key.unwrap_or("None")
        ))
    }

    /// Wrap an existing `pipe_ref` string without validation.
    pub fn from_string(s: impl Into<String>) -> PipeRef {
        PipeRef(s.into())
    }

    /// The `pipe_ref` of a core [`Pipe`].
    pub fn from_pipe(pipe: &Pipe) -> PipeRef {
        PipeRef(pipe.pipe_ref())
    }

    /// The `pipe_ref` string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Split into the four keys. Keys are split on the first three `|` characters.
    pub fn parts(&self) -> PipeRefParts<'_> {
        let mut it = self.0.splitn(4, '|');
        let instance_keys = it.next().unwrap_or("");
        let connector_keys = it.next().unwrap_or("");
        let metric_key = it.next().unwrap_or("");
        let location_key = it.next().filter(|l| *l != "None");
        PipeRefParts {
            instance_keys,
            connector_keys,
            metric_key,
            location_key,
        }
    }

    /// The instance keys.
    pub fn instance_keys(&self) -> &str {
        self.parts().instance_keys
    }

    /// A short human label without the instance: `connector/metric[/location]`.
    pub fn label(&self) -> String {
        let p = self.parts();
        match p.location_key {
            Some(loc) => format!("{}/{}/{loc}", p.connector_keys, p.metric_key),
            None => format!("{}/{}", p.connector_keys, p.metric_key),
        }
    }

    /// [`PipeRef::label`] followed by `@instance`.
    pub fn label_with_instance(&self) -> String {
        format!("{}@{}", self.label(), self.instance_keys())
    }
}

impl fmt::Display for PipeRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// One registered pipe as the graph builder sees it: the four keys plus the parameters, in the
/// shape every `fetch_pipes_keys` dict form can produce.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PipeRecord {
    /// The instance the pipe is registered on.
    #[serde(alias = "instance")]
    pub instance_keys: String,
    /// The connector keys.
    #[serde(alias = "connector")]
    pub connector_keys: String,
    /// The metric key.
    #[serde(alias = "metric")]
    pub metric_key: String,
    /// The location key.
    #[serde(default, alias = "location")]
    pub location_key: Option<String>,
    /// The registered parameters (the ordered core value).
    #[serde(default)]
    pub parameters: Map,
    /// The instance-local pipe ID, when the instance returned one. Informational only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipe_id: Option<Value>,
    /// Whether the pipe's connector is a stream source (`IS_STREAMING`, §5.6). The caller
    /// knows the connector registry; the graph does not.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub streaming: bool,
    /// Whether the pipe syncs through the Python adapter (counts against the Python worker
    /// pool limit, §5.5).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub python: bool,
}

impl PipeRecord {
    /// A record with empty parameters.
    pub fn new(
        instance_keys: &str,
        connector_keys: &str,
        metric_key: &str,
        location_key: Option<&str>,
    ) -> PipeRecord {
        PipeRecord {
            instance_keys: instance_keys.to_string(),
            connector_keys: connector_keys.to_string(),
            metric_key: metric_key.to_string(),
            location_key: location_key.map(str::to_string),
            parameters: Map::new(),
            pipe_id: None,
            streaming: false,
            python: false,
        }
    }

    /// Builder-style: set the parameters.
    pub fn with_parameters(mut self, parameters: Map) -> PipeRecord {
        self.parameters = parameters;
        self
    }

    /// The node identity.
    pub fn pipe_ref(&self) -> PipeRef {
        PipeRef::new(
            &self.instance_keys,
            &self.connector_keys,
            &self.metric_key,
            self.location_key.as_deref(),
        )
    }

    /// Construct the core [`Pipe`] (no I/O).
    pub fn to_pipe(&self, defaults: &PipeDefaults) -> Result<Pipe, PipeError> {
        Pipe::builder(&self.connector_keys, &self.metric_key)
            .location(self.location_key.as_deref())
            .instance(&self.instance_keys)
            .parameters(self.parameters.clone())
            .defaults(defaults.clone())
            .build()
    }

    /// Parse the return value of `fetch_pipes_keys` from one instance.
    ///
    /// Accepts a list of key tuples or a dict mapping pipe IDs to key tuples. Tuples have three
    /// items `(connector_keys, metric_key, location_key)` or four, where the fourth is the
    /// parameters dict or a list of tags (stored as `parameters['tags']`). The location
    /// `"None"` / `"[None]"` / `"null"` means no location.
    pub fn from_fetch_pipes_keys(
        instance_keys: &str,
        keys: &Value,
    ) -> Result<Vec<PipeRecord>, String> {
        let rows: Vec<(Option<Value>, &Value)> = match keys {
            Value::List(items) => items.iter().map(|v| (None, v)).collect(),
            Value::Map(m) => m
                .iter()
                .map(|(id, v)| {
                    let id = id
                        .parse::<i64>()
                        .map_or_else(|_| Value::Str(id.clone()), Value::Int);
                    (Some(id), v)
                })
                .collect(),
            Value::Null => Vec::new(),
            other => {
                return Err(format!(
                    "fetch_pipes_keys returned a '{}', expected a list or dict",
                    other.type_name()
                ));
            }
        };
        let mut out = Vec::with_capacity(rows.len());
        for (pipe_id, row) in rows {
            let Some(items) = row.as_list() else {
                return Err(format!("expected a key tuple, got '{}'", row.type_name()));
            };
            if !(3..=4).contains(&items.len()) {
                return Err(format!(
                    "expected a key tuple of length 3 or 4, got {}",
                    items.len()
                ));
            }
            let location = match &items[2] {
                Value::Null => None,
                v => {
                    let s = py_str(v);
                    (!matches!(s.as_str(), "None" | "[None]" | "null")).then_some(s)
                }
            };
            let mut record = PipeRecord::new(
                instance_keys,
                &py_str(&items[0]),
                &py_str(&items[1]),
                location.as_deref(),
            );
            record.pipe_id = pipe_id;
            match items.get(3) {
                Some(Value::Map(params)) => record.parameters = params.clone(),
                Some(Value::List(tags)) => {
                    record
                        .parameters
                        .insert("tags".into(), Value::List(tags.clone()));
                }
                Some(Value::Null) | None => {}
                Some(other) => {
                    return Err(format!(
                        "expected parameters or tags as the fourth item, got '{}'",
                        other.type_name()
                    ));
                }
            }
            out.push(record);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipe_ref_round_trip() {
        let r = PipeRef::new("sql:main", "sql:main", "raw", None);
        assert_eq!(r.as_str(), "sql:main|sql:main|raw|None");
        assert_eq!(r.parts().location_key, None);
        assert_eq!(r.label(), "sql:main/raw");
        let r = PipeRef::new("sql:main", "plugin:noaa", "weather", Some("us"));
        assert_eq!(r.parts().location_key, Some("us"));
        let pipe = PipeRecord::new("sql:main", "plugin:noaa", "weather", Some("us"))
            .to_pipe(&PipeDefaults::default())
            .unwrap();
        assert_eq!(PipeRef::from_pipe(&pipe), r);
    }

    #[test]
    fn fetch_pipes_keys_forms() {
        let list =
            Value::from_json_str(r#"[["sql:main", "a", null], ["sql:main", "b", "None", ["x"]]]"#)
                .unwrap();
        let recs = PipeRecord::from_fetch_pipes_keys("sql:main", &list).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[1].location_key, None);
        assert_eq!(
            recs[1].parameters.get("tags"),
            Some(&Value::from_json_str(r#"["x"]"#).unwrap())
        );
        let dict = Value::from_json_str(
            r#"{"1": ["sql:main", "a", "us", {"columns": {"datetime": "ts"}}]}"#,
        )
        .unwrap();
        let recs = PipeRecord::from_fetch_pipes_keys("sql:main", &dict).unwrap();
        assert_eq!(recs[0].pipe_id, Some(Value::Int(1)));
        assert_eq!(recs[0].location_key.as_deref(), Some("us"));
        assert!(recs[0].parameters.contains_key("columns"));
        assert!(PipeRecord::from_fetch_pipes_keys("x", &Value::Int(1)).is_err());
    }
}
