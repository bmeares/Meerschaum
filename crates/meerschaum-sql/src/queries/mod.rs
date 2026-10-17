//! Flavor-aware query builders, ported function by function from `meerschaum/utils/sql.py` and
//! `meerschaum/connectors/sql/`.
//!
//! Functions that need live database state in Python (existing columns, indices, whether a
//! table is a hypertable, ...) take that state as explicit inputs here; the connector gathers
//! it with the introspection queries in [`table`] and the driver layer.

pub mod compress;
pub mod cte;
pub mod index;
pub mod maintenance;
pub mod metadef;
pub mod partition;
pub mod table;
pub mod templates;
pub mod update;

use crate::flavor::Flavor;
use crate::pipe::SqlPipe;
use meerschaum_core::Value;

/// The connector attributes the pipe-level builders read (`self.flavor`, `self.schema`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectorCtx {
    /// `self.flavor`.
    pub flavor: Flavor,
    /// `self.schema` (the connector's default schema, `None` on schema-less flavors).
    pub schema: Option<String>,
}

impl ConnectorCtx {
    /// A context with the flavor's default schema (`DEFAULT_SCHEMA_FLAVORS`, `None` for
    /// schema-less flavors), which is what `SQLConnector.schema` resolves to on a fresh database.
    pub fn new(flavor: Flavor) -> Self {
        ConnectorCtx {
            flavor,
            schema: if flavor.no_schema() {
                None
            } else {
                flavor.default_schema().map(str::to_string)
            },
        }
    }

    /// A context with an explicit connector schema.
    pub fn with_schema(flavor: Flavor, schema: Option<&str>) -> Self {
        ConnectorCtx {
            flavor,
            schema: schema.map(str::to_string),
        }
    }

    /// `SQLConnector.get_pipe_schema(pipe)` (`_pipes.py:4215`).
    pub fn get_pipe_schema(&self, pipe: &SqlPipe) -> Option<String> {
        if matches!(self.flavor, Flavor::Sqlite | Flavor::Geopackage) {
            return self.schema.clone();
        }
        // `pipe.parameters.get('schema', self.schema)`: an explicit `None` wins.
        match pipe.param("schema") {
            None => self.schema.clone(),
            Some(Value::Null) => None,
            Some(v) => Some(v.py_str()),
        }
    }
}
