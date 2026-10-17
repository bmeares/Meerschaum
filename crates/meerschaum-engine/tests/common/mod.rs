//! Shared harness for the engine's SQLite-backed integration tests: one temporary SQLite
//! instance (`sql:eng`) that is also the source connector of its `sql:` pipes, a static
//! registry, and helpers to register pipes and read their tables in a comparable form.
#![allow(dead_code)]

pub mod mqtt;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use meerschaum_connector::pipe_ops::frame_to_docs;
use meerschaum_connector::{DataQuery, InstanceConnector};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_engine::{Engine, EngineConfig, PipeRef, StaticRegistry};
use meerschaum_kernel::value::Value as KValue;
use meerschaum_sql::{Flavor, SqlConnector};

/// The instance (and source) keys used by every test.
pub const INSTANCE: &str = "sql:eng";

/// A temporary SQLite instance.
pub struct Env {
    /// The temporary directory (database, lease files).
    pub dir: PathBuf,
    /// The database file.
    pub db: PathBuf,
    /// The connector.
    pub conn: Arc<SqlConnector>,
    /// The registry over it.
    pub registry: Arc<StaticRegistry>,
}

impl Drop for Env {
    fn drop(&mut self) {
        if std::env::var_os("MRSM_KEEP_TEST_DB").is_none() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

/// A fresh temporary directory.
pub fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "mrsm_engine_{tag}_{}",
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&dir).expect("temp dir");
    dir
}

/// Connect to the SQLite database at `db`.
pub async fn connect(db: &std::path::Path) -> Arc<SqlConnector> {
    let uri = format!("sqlite:///{}", db.display());
    Arc::new(
        SqlConnector::from_uri("eng", &uri)
            .await
            .expect("connect to SQLite"),
    )
}

/// A registry with `sql:eng` as the instance and the source connector.
pub fn registry(conn: &Arc<SqlConnector>) -> Arc<StaticRegistry> {
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let source: Arc<dyn meerschaum_connector::Connector> = conn.clone();
    Arc::new(
        StaticRegistry::new()
            .with_instance(INSTANCE, instance)
            .with_source(INSTANCE, source)
            .with_table_reference(|pipe: &Pipe| {
                let target = pipe.target(None).map_err(|e| e.to_string())?;
                Ok(meerschaum_sql::ident::sql_item_name(
                    &target,
                    Flavor::Sqlite,
                    None,
                ))
            }),
    )
}

/// A fresh environment.
pub async fn env(tag: &str) -> Env {
    let dir = temp_dir(tag);
    let db = dir.join("eng.db");
    let conn = connect(&db).await;
    let registry = registry(&conn);
    Env {
        dir,
        db,
        conn,
        registry,
    }
}

impl Env {
    /// Engine configuration for this environment.
    pub fn config(&self) -> EngineConfig {
        EngineConfig {
            home_instance: INSTANCE.into(),
            lease_dir: self.dir.join("leases"),
            lease_wait: Duration::from_secs(5),
            lease_poll: Duration::from_millis(50),
            state_flush: Duration::from_millis(20),
            holder: format!("test:{}", std::process::id()),
            ..EngineConfig::default()
        }
    }

    /// An engine over this environment.
    pub fn engine(&self) -> Engine {
        Engine::new(self.registry.clone(), self.config())
    }

    /// An engine with a modified configuration.
    pub fn engine_with(&self, f: impl FnOnce(&mut EngineConfig)) -> Engine {
        let mut c = self.config();
        f(&mut c);
        Engine::new(self.registry.clone(), c)
    }

    /// Execute SQL on the database.
    pub async fn exec(&self, sql: &str) {
        assert!(self.conn.exec(sql).await.is_some(), "failed: {sql}");
    }

    /// The pipe `sql:eng / metric` with `parameters` (JSON).
    pub fn pipe(&self, metric: &str, parameters: &serde_json::Value) -> Pipe {
        Pipe::builder(INSTANCE, metric)
            .instance(INSTANCE)
            .parameters(json_map(parameters))
            .build()
            .expect("valid pipe")
    }

    /// Register a pipe.
    pub async fn register(&self, metric: &str, parameters: &serde_json::Value) -> Pipe {
        let pipe = self.pipe(metric, parameters);
        let res = self.conn.register_pipe(&pipe).await;
        assert!(res.success, "register {metric}: {}", res.message);
        pipe
    }

    /// The `pipe_ref` of `sql:eng / metric`.
    pub fn pipe_ref(&self, metric: &str) -> PipeRef {
        PipeRef::new(INSTANCE, INSTANCE, metric, None)
    }

    /// The pipe's table in a comparable form (see [`Table`]).
    pub async fn table(&self, metric: &str) -> Table {
        self.table_of(INSTANCE, metric).await
    }

    /// The table of the pipe `connector_keys / metric` on this instance (see [`Table`]).
    pub async fn table_of(&self, connector_keys: &str, metric: &str) -> Table {
        let pipe = Pipe::builder(connector_keys, metric)
            .instance(INSTANCE)
            .build()
            .expect("valid pipe");
        let df = self
            .conn
            .get_pipe_data(&pipe, &DataQuery::default())
            .await
            .expect("read pipe data");
        match df {
            None => Table::default(),
            Some(df) => {
                let docs = frame_to_docs(&df);
                let mut columns: Vec<String> = df.names();
                columns.sort();
                let mut rows: Vec<String> = docs
                    .iter()
                    .map(|d| {
                        columns
                            .iter()
                            .map(|c| norm_kvalue(d.get(c).unwrap_or(&KValue::Null)))
                            .collect::<Vec<_>>()
                            .join(" | ")
                    })
                    .collect();
                rows.sort();
                Table { columns, rows }
            }
        }
    }

    /// The number of rows in a pipe's table.
    pub async fn rowcount(&self, metric: &str) -> usize {
        self.table(metric).await.rows.len()
    }
}

/// A JSON object as a core [`Map`].
pub fn json_map(v: &serde_json::Value) -> Map {
    Value::from_json_str(&v.to_string())
        .expect("valid JSON")
        .to_map()
}

/// A table: sorted column names and rows rendered as sorted text.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Table {
    /// Sorted column names.
    pub columns: Vec<String>,
    /// One line per row, columns in sorted order, rows sorted.
    pub rows: Vec<String>,
}

impl Table {
    /// A table recorded by the v4 fixture generator (`{"columns": [...], "rows": [[...]]}`,
    /// or `[]` for a pipe without a table).
    pub fn from_fixture(v: &serde_json::Value) -> Table {
        let Some(obj) = v.as_object() else {
            return Table::default();
        };
        let columns: Vec<String> = obj["columns"]
            .as_array()
            .expect("columns")
            .iter()
            .map(|c| c.as_str().expect("column name").to_string())
            .collect();
        let mut rows: Vec<String> = obj["rows"]
            .as_array()
            .expect("rows")
            .iter()
            .map(|r| {
                r.as_array()
                    .expect("row")
                    .iter()
                    .map(norm_json)
                    .collect::<Vec<_>>()
                    .join(" | ")
            })
            .collect();
        rows.sort();
        Table { columns, rows }
    }
}

fn norm_number(f: f64) -> String {
    format!("{f:.6}")
}

/// A kernel value rendered for comparison (datetimes as UTC ISO, numbers as `{:.6}`).
pub fn norm_kvalue(v: &KValue) -> String {
    match v {
        v if v.is_na() => "null".into(),
        KValue::Int(i) => norm_number(*i as f64),
        KValue::Float(f) => norm_number(*f),
        KValue::Decimal(d) => norm_number(d.to_f64()),
        KValue::Bool(b) => b.to_string(),
        KValue::Datetime(d) => d.utc.format("%Y-%m-%dT%H:%M:%SZ").to_string(),
        KValue::Str(s) => s.clone(),
        other => other.py_str(),
    }
}

/// A fixture JSON value rendered like [`norm_kvalue`].
pub fn norm_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Null => "null".into(),
        serde_json::Value::Number(n) => norm_number(n.as_f64().expect("number")),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}
