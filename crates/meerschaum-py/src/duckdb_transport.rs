//! Python hosts use DuckDB's public DBAPI transport; Pipe algorithms remain in Rust.
//! Standalone/base installs retain the connector crate's dynamically loaded C API.
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use meerschaum_connector_sql::driver::{
    ColumnKind, ColumnPlan, DuckdbHostDiagnostic, NativeConn, NativeConnector, NativeEvent,
    SqlParam, SqlValue,
};
use meerschaum_connector_sql::{SqlError, uri::ConnectionSpec};
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyTuple};
use tokio::sync::mpsc;

type Result<T> = std::result::Result<T, SqlError>;
fn error(e: impl std::fmt::Display) -> SqlError {
    SqlError::Database(e.to_string())
}

pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    meerschaum_connector_sql::driver::register_duckdb_host_factory(factory);
    meerschaum_connector_sql::driver::register_duckdb_host_diagnostic(diagnostic);
    module.add_function(wrap_pyfunction!(_duckdb_fork_quarantine, module)?)?;
    module.add_function(wrap_pyfunction!(_duckdb_diagnostic, module)?)?;
    Ok(())
}

/// Read existing policy and installed metadata only. In particular, do not import the helper
/// (which registers fork callbacks), DuckDB or SQLAlchemy, or call the policy-selecting hook.
fn diagnostic() -> DuckdbHostDiagnostic {
    Python::attach(diagnostic_with_python).unwrap_or_else(|e| {
        DuckdbHostDiagnostic::Refused(format!(
            "Cannot inspect Python DuckDB transport metadata: {e}"
        ))
    })
}

fn diagnostic_with_python(py: Python<'_>) -> PyResult<DuckdbHostDiagnostic> {
    let sys = py.import("sys")?;
    let modules = sys.getattr("modules")?.cast_into::<PyDict>()?;
    let override_value: String = py
        .import("os")?
        .getattr("environ")?
        .call_method1("get", ("MRSM_DUCKDB_LIB", ""))?
        .extract()?;
    let mut pinned = false;
    let mut loaded_version = None;
    if let Some(helper) = modules.get_item("meerschaum.connectors.sql._duckdb_transport")? {
        let owner: u32 = helper.getattr("_PID")?.extract()?;
        if owner != std::process::id() {
            return Ok(DuckdbHostDiagnostic::Refused(
                "DuckDB state belongs to the parent process; start a fresh interpreter (spawn or exec).".into(),
            ));
        }
        let policy: Option<String> = helper.getattr("_POLICY")?.extract()?;
        pinned = policy.is_some();
        if pinned {
            let selected_override: Option<String> = helper.getattr("_OVERRIDE")?.extract()?;
            if selected_override.as_deref().unwrap_or_default() != override_value {
                return Ok(DuckdbHostDiagnostic::Refused(
                    "MRSM_DUCKDB_LIB changed after DuckDB transport selection; restart the interpreter.".into(),
                ));
            }
        }
        if policy.as_deref() == Some("native") {
            return Ok(DuckdbHostDiagnostic::Native);
        }
        if let Ok(driver) = helper.getattr("_DRIVER")
            && !driver.is_none()
        {
            loaded_version = driver.getattr("__version__")?.extract::<String>().ok();
        }
    }
    if !override_value.is_empty() {
        return Ok(DuckdbHostDiagnostic::Native);
    }
    if !pinned {
        // An unqualified find_spec does not import the driver. Match policy()'s refusal to
        // guess when a custom finder cannot provide a spec; never select/pin the policy here.
        let found = py
            .import("importlib.util")?
            .call_method1("find_spec", ("duckdb",))
            .is_ok_and(|spec| !spec.is_none());
        if !found {
            return Ok(DuckdbHostDiagnostic::Native);
        }
    }
    let distribution = py
        .import("importlib.metadata")?
        .call_method1("distribution", ("duckdb",));
    let (version, path) = match distribution {
        Ok(distribution) => (
            distribution.getattr("version")?.extract::<String>().ok(),
            distribution
                .call_method1("locate_file", ("",))?
                .str()
                .ok()
                .map(|s| s.to_string()),
        ),
        Err(_) => (None, None),
    };
    Ok(DuckdbHostDiagnostic::Python {
        version: loaded_version.or(version),
        path,
        pinned,
    })
}

/// A private observable diagnostic for read-only regression tests; no optional imports or
/// connection operations are performed by this function.
#[pyfunction]
fn _duckdb_diagnostic() -> String {
    let value = match diagnostic() {
        DuckdbHostDiagnostic::Native => serde_json::json!({"transport": "native"}),
        DuckdbHostDiagnostic::Python {
            version,
            path,
            pinned,
        } => {
            serde_json::json!({"transport": "python", "version": version, "path": path, "pinned": pinned})
        }
        DuckdbHostDiagnostic::Refused(problem) => {
            serde_json::json!({"transport": "refused", "problem": problem})
        }
    };
    value.to_string()
}

/// Intentionally leak a child-only reference: inherited driver destructors must not checkpoint
/// a parent's database. The child must spawn/exec before using DuckDB again.
#[pyfunction]
fn _duckdb_fork_quarantine(objects: &Bound<'_, PyAny>) {
    let _ = objects.clone().unbind().into_ptr();
}

fn check_pid(pid: u32) -> Result<()> {
    if pid != std::process::id() {
        return Err(error(
            "DuckDB state belongs to the parent process; start a fresh interpreter (spawn or exec).",
        ));
    }
    Ok(())
}

fn factory(spec: &ConnectionSpec) -> Result<Option<Box<dyn NativeConnector>>> {
    // Private SDK identity comes only from trusted attributes, never a URI / options map
    // that can overwrite those attributes during ConnectionSpec resolution.
    let reserved = |key: &str| key.to_ascii_lowercase().starts_with("_duckdb_");
    if let Some(uri) = &spec.uri {
        let parsed = meerschaum_connector_sql::uri::parse_uri(uri)?;
        if parsed.keys().any(|key| reserved(key)) {
            return Err(error(
                "Reserved DuckDB transport keys cannot be supplied in URI/options.",
            ));
        }
    }
    if spec
        .options
        .as_ref()
        .is_some_and(|options| options.pairs().iter().any(|(key, _)| reserved(key)))
    {
        return Err(error(
            "Reserved DuckDB transport keys cannot be supplied in URI/options.",
        ));
    }
    Python::attach(|py| {
        let module = py.import("meerschaum.connectors.sql._duckdb_transport")?;
        let options = PyDict::new(py);
        for (key, value) in &spec.params {
            if !key.starts_with('_') {
                options.set_item(key, value)?;
            }
        }
        if let Some(config) = &spec.options {
            for (key, value) in config.pairs() {
                if !key.starts_with('_') {
                    options.set_item(key, value)?;
                }
            }
        }
        // SDK engine creation freezes relative endpoints before a later chdir.
        let database = spec
            .params
            .get("_duckdb_database")
            .map(String::as_str)
            .or(spec.database.as_deref())
            .unwrap_or(":memory:");
        // Internal identity must never be forwarded as a DuckDB configuration option.
        let token = spec
            .params
            .get("_duckdb_memory_id")
            .map(String::as_str)
            .unwrap_or("");
        let anchor = module.call_method1(
            "native_anchor",
            (
                database,
                spec.label.as_deref().unwrap_or("main"),
                token,
                options,
            ),
        )?;
        Ok::<_, PyErr>(if anchor.is_none() {
            None
        } else {
            Some(Box::new(Connector {
                anchor: Some(anchor.unbind()),
                pid: std::process::id(),
            }) as Box<dyn NativeConnector>)
        })
    })
    .map_err(error)
}

struct Connector {
    anchor: Option<Py<PyAny>>,
    pid: u32,
}
impl Drop for Connector {
    fn drop(&mut self) {
        if self.pid != std::process::id()
            && let Some(anchor) = self.anchor.take()
        {
            std::mem::forget(anchor);
        }
    }
}
struct Connection {
    object: Option<Py<PyAny>>,
    pid: u32,
    broken: Arc<AtomicBool>,
}
impl Drop for Connection {
    fn drop(&mut self) {
        if self.pid != std::process::id() {
            if let Some(object) = self.object.take() {
                std::mem::forget(object);
            }
            return;
        }
        let _ = Python::try_attach(|py| {
            if let Some(object) = &self.object {
                let _ = object.bind(py).call_method0("close");
            }
        });
    }
}
struct Conn {
    pid: u32,
    connection: Arc<Mutex<Connection>>,
    broken: Arc<AtomicBool>,
}

#[async_trait]
impl NativeConnector for Connector {
    fn owner_pid(&self) -> Option<u32> {
        Some(self.pid)
    }
    async fn connect(&self) -> Result<Box<dyn NativeConn>> {
        check_pid(self.pid)?;
        let anchor = Python::attach(|py| self.anchor.as_ref().expect("live anchor").clone_ref(py));
        let object = tokio::task::spawn_blocking(move || {
            Python::attach(|py| {
                let kwargs = PyDict::new(py);
                kwargs.set_item("native", true)?;
                Ok::<_, PyErr>(
                    anchor
                        .bind(py)
                        .call_method("connect", (), Some(&kwargs))?
                        .unbind(),
                )
            })
        })
        .await
        .map_err(error)?
        .map_err(error)?;
        let broken = Arc::new(AtomicBool::new(false));
        Ok(Box::new(Conn {
            pid: std::process::id(),
            connection: Arc::new(Mutex::new(Connection {
                object: Some(object),
                pid: std::process::id(),
                broken: broken.clone(),
            })),
            broken,
        }))
    }
}

fn parameters<'py>(py: Python<'py>, params: &[SqlParam]) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    let datetime = py.import("datetime")?;
    for param in params {
        match param {
            SqlParam::Null => out.append(py.None())?,
            SqlParam::Bool(v) => out.append(v)?,
            SqlParam::Int(v) => out.append(v)?,
            SqlParam::Float(v) => out.append(v)?,
            SqlParam::Text(v) => out.append(v)?,
            SqlParam::Json(v) => out.append(v.to_string())?,
            SqlParam::Bytes(v) => out.append(PyBytes::new(py, v))?,
            SqlParam::Timestamp(v) => out.append(
                datetime
                    .getattr("datetime")?
                    .call_method1("fromisoformat", (v.to_string(),))?,
            )?,
            SqlParam::TimestampTz(v) => out.append(
                datetime
                    .getattr("datetime")?
                    .call_method1("fromisoformat", (v.to_rfc3339(),))?,
            )?,
            SqlParam::Date(v) => out.append(
                datetime
                    .getattr("date")?
                    .call_method1("fromisoformat", (v.to_string(),))?,
            )?,
        }
    }
    Ok(out)
}

fn plan(name: String, ty: String) -> ColumnPlan {
    let upper = ty.to_ascii_uppercase();
    let kind = match upper.as_str() {
        "BOOLEAN" => ColumnKind::Boolean,
        "TINYINT" => ColumnKind::Int8,
        "SMALLINT" => ColumnKind::Int16,
        "INTEGER" => ColumnKind::Int32,
        "BIGINT" => ColumnKind::Int64,
        "UTINYINT" => ColumnKind::UInt8,
        "USMALLINT" => ColumnKind::UInt16,
        "UINTEGER" => ColumnKind::UInt32,
        "UBIGINT" => ColumnKind::UInt64,
        "HUGEINT" | "UHUGEINT" => ColumnKind::NumericText,
        "FLOAT" => ColumnKind::Float32,
        "DOUBLE" => ColumnKind::Float64,
        "BLOB" | "GEOMETRY" => ColumnKind::Bytes,
        "UUID" => ColumnKind::Uuid,
        "JSON" => ColumnKind::Json,
        "DATE" => ColumnKind::Date,
        "TIME" | "TIME_NS" => ColumnKind::Time,
        "TIMESTAMP WITH TIME ZONE" => ColumnKind::TimestampTz,
        _ if upper.starts_with("TIMESTAMP") => ColumnKind::Timestamp,
        _ if upper.starts_with("DECIMAL(") => ColumnKind::Decimal,
        _ => ColumnKind::Utf8,
    };
    let mut p = ColumnPlan::new(name, kind, ty);
    if kind == ColumnKind::Decimal {
        p.scale = upper
            .trim_end_matches(')')
            .rsplit(',')
            .next()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0);
    }
    p
}

/// DuckDB renders extended and BC years as SQL text, while chrono uses signed ISO years.
fn normalize_date_text(text: &str) -> String {
    let bc = text.contains(" (BC)");
    let clean = text.replace(" (BC)", "");
    let Some((year, rest)) = clean.split_once('-') else {
        return clean;
    };
    let Ok(year) = year.parse::<i32>() else {
        return clean;
    };
    if bc {
        let year = 1 - year;
        if year < 0 {
            format!("-{:04}-{rest}", -year)
        } else {
            format!("{year:04}-{rest}")
        }
    } else if year > 9999 {
        format!("+{year}-{rest}")
    } else {
        clean
    }
}

fn cell(value: &Bound<'_, PyAny>, kind: ColumnKind) -> PyResult<SqlValue> {
    if value.is_none() {
        return Ok(SqlValue::Null);
    }
    if value.is_instance_of::<PyBool>() {
        return Ok(SqlValue::Bool(value.extract()?));
    }
    if value.is_instance_of::<PyInt>() {
        if matches!(kind, ColumnKind::NumericText) {
            return Ok(SqlValue::Text(value.str()?.to_string()));
        }
        if let Ok(v) = value.extract::<i64>() {
            return Ok(SqlValue::Int(v));
        }
        if let Ok(v) = value.extract::<u64>() {
            return Ok(SqlValue::UInt(v));
        }
    }
    if value.is_instance_of::<PyFloat>() {
        return Ok(SqlValue::Float(value.extract()?));
    }
    if value.is_instance_of::<PyBytes>() {
        return Ok(SqlValue::Bytes(value.extract()?));
    }
    let raw = value.str()?.to_string();
    let text = if matches!(
        kind,
        ColumnKind::Date | ColumnKind::Timestamp | ColumnKind::TimestampTz
    ) {
        normalize_date_text(&raw)
    } else {
        raw
    };
    Ok(match kind {
        ColumnKind::Uuid => SqlValue::Uuid(
            uuid::Uuid::parse_str(&text)
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
        ),
        ColumnKind::Date => SqlValue::Date(
            chrono::NaiveDate::parse_from_str(&text, "%Y-%m-%d")
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
        ),
        ColumnKind::Time => SqlValue::Time(
            chrono::NaiveTime::parse_from_str(&text, "%H:%M:%S%.f")
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
        ),
        ColumnKind::Timestamp => SqlValue::Timestamp(
            chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f")
                .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?,
        ),
        ColumnKind::TimestampTz => {
            let value = if text == "infinity" {
                chrono::NaiveDate::from_ymd_opt(9999, 12, 31)
                    .unwrap()
                    .and_hms_micro_opt(23, 59, 59, 999_999)
                    .unwrap()
                    .and_utc()
            } else if text == "-infinity" {
                chrono::NaiveDate::from_ymd_opt(1, 1, 1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .unwrap()
                    .and_utc()
            } else {
                chrono::DateTime::parse_from_rfc3339(&text.replace(' ', "T"))
                    .or_else(|_| chrono::DateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f%#z"))
                    .map(|value| value.to_utc())
                    .or_else(|_| {
                        chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f")
                            .map(|value| value.and_utc())
                    })
                    .map_err(|e| pyo3::exceptions::PyValueError::new_err(e.to_string()))?
            };
            SqlValue::TimestampTz(value)
        }
        ColumnKind::Decimal => rust_decimal::Decimal::from_str_exact(&text)
            .map(SqlValue::Decimal)
            .unwrap_or_else(|_| SqlValue::Text(text)),
        ColumnKind::Json => SqlValue::Json(text),
        _ => SqlValue::Text(text),
    })
}

#[async_trait]
impl NativeConn for Conn {
    async fn execute(&mut self, sql: &str, params: &[SqlParam]) -> Result<u64> {
        check_pid(self.pid)?;
        self.broken.store(true, Ordering::Release);
        let connection = self.connection.clone();
        let sql = sql.to_string();
        let params = params.to_vec();
        tokio::task::spawn_blocking(move || {
            let connection = connection.lock().map_err(error)?;
            let result = Python::attach(|py| {
                let module = py.import("meerschaum.connectors.sql._duckdb_transport")?;
                module
                    .call_method1(
                        "execute",
                        (
                            connection
                                .object
                                .as_ref()
                                .expect("live connection")
                                .bind(py),
                            sql,
                            parameters(py, &params)?,
                        ),
                    )?
                    .extract::<u64>()
            })
            .map_err(error);
            if result.is_ok() {
                connection.broken.store(false, Ordering::Release);
            }
            result
        })
        .await
        .map_err(error)?
    }
    async fn query(
        &mut self,
        sql: &str,
        params: &[SqlParam],
        chunksize: usize,
        sink: mpsc::Sender<NativeEvent>,
    ) -> Result<()> {
        check_pid(self.pid)?;
        self.broken.store(true, Ordering::Release);
        let connection = self.connection.clone();
        let sql = sql.to_string();
        let params = params.to_vec();
        tokio::task::spawn_blocking(move || {
            let connection = connection.lock().map_err(error)?;
            let (cursor, plans) = Python::attach(|py| {
                let module = py.import("meerschaum.connectors.sql._duckdb_transport")?;
                let result = module.call_method1(
                    "query",
                    (
                        connection
                            .object
                            .as_ref()
                            .expect("live connection")
                            .bind(py),
                        sql,
                        parameters(py, &params)?,
                    ),
                )?;
                let tuple = result.cast::<PyTuple>()?;
                let columns: Vec<(String, String)> = tuple.get_item(1)?.extract()?;
                Ok::<_, PyErr>((
                    tuple.get_item(0)?.unbind(),
                    columns
                        .into_iter()
                        .map(|(n, t)| plan(n, t))
                        .collect::<Vec<_>>(),
                ))
            })
            .map_err(error)?;
            if sink
                .blocking_send(NativeEvent::Columns(plans.clone()))
                .is_err()
            {
                return Ok(());
            }
            loop {
                let rows = Python::attach(|py| {
                    let batch = cursor
                        .bind(py)
                        .call_method1("fetchmany", (chunksize.max(1),))?;
                    batch
                        .try_iter()?
                        .map(|row| {
                            row?.try_iter()?
                                .enumerate()
                                .map(|(i, v)| cell(&v?, plans[i].kind))
                                .collect::<PyResult<Vec<_>>>()
                        })
                        .collect::<PyResult<Vec<_>>>()
                })
                .map_err(error)?;
                if rows.is_empty() {
                    break;
                }
                // Never hold the GIL across bounded channel backpressure.
                if sink.blocking_send(NativeEvent::Rows(rows)).is_err() {
                    return Ok(());
                }
            }
            connection.broken.store(false, Ordering::Release);
            Ok(())
        })
        .await
        .map_err(error)?
    }
    async fn begin(&mut self) -> Result<()> {
        self.execute("BEGIN TRANSACTION", &[]).await.map(|_| ())
    }
    async fn commit(&mut self) -> Result<()> {
        self.execute("COMMIT", &[]).await.map(|_| ())
    }
    async fn rollback(&mut self) -> Result<()> {
        self.execute("ROLLBACK", &[]).await.map(|_| ())
    }
    fn is_broken(&self) -> bool {
        self.pid != std::process::id() || self.broken.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Datelike;

    #[test]
    fn duckdb_extended_and_bc_years_follow_chrono() {
        for (input, expected) in [
            ("10000-01-01", 10000),
            ("0001-01-01 (BC)", 0),
            ("0002-01-01 (BC)", -1),
        ] {
            let value =
                chrono::NaiveDate::parse_from_str(&normalize_date_text(input), "%Y-%m-%d").unwrap();
            assert_eq!(value.year(), expected);
        }
        for input in ["10000-01-01 01:02:03+00", "0001-01-01 (BC) 01:02:03+00"] {
            let text = normalize_date_text(input);
            chrono::DateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S%.f%#z").unwrap();
        }
    }
}
