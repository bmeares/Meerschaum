//! A pipe as the SQL instance connector sees it: [`meerschaum_core::Pipe`] plus the values Python
//! computes from the instance connector and the config registry (the flavor-truncated `target`,
//! `pipes:parameters:*` defaults).
//!
//! Every pipe attribute (`columns`, `dtypes`, `indices`, `precision`, `get_chunk_interval`,
//! `get_backtrack_interval`, ...) comes from `meerschaum-core`; [`SqlPipe`] dereferences to the
//! core [`Pipe`]. Only the SQL-specific views live here: the `indices` entries as column lists
//! ([`IndexColumns`]) and the `hypertable` / `hypercore` flags the SQL connector reads.

use std::ops::Deref;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use meerschaum_core::Value;
use meerschaum_core::dtypes::PrecisionUnit;
pub use meerschaum_core::pipe::chunk::total_seconds;
pub use meerschaum_core::pipe::{
    ChunkInterval, ChunkIntervalArg, DEFAULT_BACKTRACK_MINUTES, Pipe, PipeDefaults,
};

use crate::error::Result;
use crate::flavor::Flavor;

/// One `indices` entry: a single column or a list of columns.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum IndexColumns {
    /// `'col'`.
    One(String),
    /// `['a', 'b']`.
    Many(Vec<String>),
}

impl IndexColumns {
    /// Read an `indices` value: a string, a list (items via `str()`), `None` (no columns), or
    /// anything else via `str()`.
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Str(s) => IndexColumns::One(s.clone()),
            Value::List(items) => IndexColumns::Many(items.iter().map(Value::py_str).collect()),
            Value::Null => IndexColumns::Many(Vec::new()),
            other => IndexColumns::One(other.py_str()),
        }
    }

    /// The columns as a list (`[cols]` for a single column).
    pub fn as_vec(&self) -> Vec<String> {
        match self {
            IndexColumns::One(c) => vec![c.clone()],
            IndexColumns::Many(cs) => cs.clone(),
        }
    }

    /// `'_'.join(cols)` for lists, `str(cols)` otherwise.
    pub fn joined(&self) -> String {
        match self {
            IndexColumns::One(c) => c.clone(),
            IndexColumns::Many(cs) => cs.join("_"),
        }
    }

    fn is_truthy(&self) -> bool {
        match self {
            IndexColumns::One(c) => !c.is_empty(),
            IndexColumns::Many(cs) => !cs.is_empty(),
        }
    }
}

/// A `{key: str(value)}` view of a parameters dict (`columns`, `dtypes`).
pub fn str_map(map: &meerschaum_core::Map) -> IndexMap<String, String> {
    map.iter().map(|(k, v)| (k.clone(), v.py_str())).collect()
}

/// A [`Pipe`] bound to a SQL instance: the core pipe, its resolved `target` table name, and the
/// configuration defaults Python reads with `get_config` while computing pipe attributes.
#[derive(Debug, Clone)]
pub struct SqlPipe {
    pipe: Pipe,
    /// `pipe.target` as the instance resolves it (truncated to the flavor's identifier limit).
    pub target: String,
    /// `pipes:parameters:verify:chunk_minutes`, the default instance, the autotime column.
    pub defaults: PipeDefaults,
    /// `pipes:parameters:fetch:backtrack_minutes`.
    pub backtrack_minutes: i64,
}

impl Deref for SqlPipe {
    type Target = Pipe;

    fn deref(&self) -> &Pipe {
        &self.pipe
    }
}

impl SqlPipe {
    /// Bind a pipe to a SQL flavor: `pipe.target` with the flavor's truncation.
    pub fn new(pipe: Pipe, flavor: Flavor) -> Result<Self> {
        let target = pipe.target(Some(flavor.as_str()))?;
        Ok(Self::with_target(pipe, target))
    }

    /// Bind a pipe with an already-resolved target name.
    pub fn with_target(pipe: Pipe, target: impl Into<String>) -> Self {
        SqlPipe {
            pipe,
            target: target.into(),
            defaults: PipeDefaults::default(),
            backtrack_minutes: DEFAULT_BACKTRACK_MINUTES,
        }
    }

    /// Use configured defaults instead of the built-in ones.
    pub fn with_defaults(mut self, defaults: PipeDefaults, backtrack_minutes: i64) -> Self {
        self.defaults = defaults;
        self.backtrack_minutes = backtrack_minutes;
        self
    }

    /// The core pipe.
    pub fn pipe(&self) -> &Pipe {
        &self.pipe
    }

    /// `pipe.parameters.get('hypertable', True)`.
    pub fn hypertable(&self) -> bool {
        self.param("hypertable").is_none_or(Value::truthy)
    }

    /// `pipe.parameters.get('hypercore', True)`.
    pub fn hypercore(&self) -> bool {
        self.param("hypercore").is_none_or(Value::truthy)
    }

    /// `pipe.indices` with each entry as an [`IndexColumns`].
    pub fn index_columns(&self) -> IndexMap<String, IndexColumns> {
        self.indices()
            .iter()
            .map(|(ix, cols)| (ix.clone(), IndexColumns::from_value(cols)))
            .collect()
    }

    /// Indices with truthy column values (the `_column_names` filter in `get_pipe_index_names`).
    pub fn truthy_index_columns(&self) -> IndexMap<String, IndexColumns> {
        self.index_columns()
            .into_iter()
            .filter(|(_, c)| c.is_truthy())
            .collect()
    }

    /// `pipe.precision['unit']` (`Pipe.get_precision`).
    pub fn precision_unit(&self) -> Result<PrecisionUnit> {
        Ok(self.pipe.precision(&self.defaults)?.unit)
    }

    /// `pipe.get_chunk_interval()`.
    pub fn chunk_interval(&self) -> Result<ChunkInterval> {
        Ok(self.pipe.get_chunk_interval(None, &self.defaults)?)
    }

    /// `pipe.get_chunk_interval(chunk_interval)`.
    pub fn chunk_interval_with(&self, chunk_interval: ChunkIntervalArg) -> Result<ChunkInterval> {
        Ok(self
            .pipe
            .get_chunk_interval(Some(chunk_interval), &self.defaults)?)
    }

    /// `pipe.get_backtrack_interval(check_existing)`.
    pub fn backtrack_interval(&self, check_existing: bool) -> Result<ChunkInterval> {
        Ok(self.pipe.get_backtrack_interval(
            check_existing,
            self.backtrack_minutes,
            &self.defaults,
        )?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipe(params: &str) -> SqlPipe {
        let parameters = Value::from_json_str(params).unwrap().to_map();
        let pipe = Pipe::builder("plugin:x", "m")
            .parameters(parameters)
            .build()
            .unwrap();
        SqlPipe::new(pipe, Flavor::Postgresql).unwrap()
    }

    #[test]
    fn indices_order() {
        let p =
            pipe(r#"{"columns": {"id": "id", "datetime": "ts"}, "indices": {"v": ["a", "b"]}}"#);
        let ix = p.index_columns();
        assert_eq!(
            ix.keys().collect::<Vec<_>>(),
            vec!["unique", "id", "datetime", "v"]
        );
        assert_eq!(ix["unique"].joined(), "ts_id");
        assert_eq!(p.target, "plugin_x_m");
    }

    #[test]
    fn chunk_interval() {
        let p = pipe(r#"{"columns": {"datetime": "ts"}}"#);
        assert_eq!(p.chunk_interval().unwrap(), ChunkInterval::minutes(43200));
        let p = pipe(r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}}"#);
        assert_eq!(p.chunk_interval().unwrap(), ChunkInterval::Int(43200));
        let p = pipe(
            r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}, "precision": "s", "verify": {"chunk_days": 1}}"#,
        );
        assert_eq!(p.chunk_interval().unwrap(), ChunkInterval::Int(86400));
        assert!(p.hypertable());
        assert!(!pipe(r#"{"hypertable": false}"#).hypertable());
    }
}
