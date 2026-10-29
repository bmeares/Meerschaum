//! rc15 instance features bound into `meerschaum._core`:
//!
//! - **Protected connectors** (`meerschaum_core::protected`): `protected_allowed()`,
//!   `allow_protected_enter()` / `allow_protected_exit()` (the
//!   `meerschaum.connectors.allow_protected()` context manager) and
//!   `protected_refusal_message(op, keys)` (the one refusal text, contract C6), and
//!   `sql_is_read_statement(query, flavor)`, the check `SQLConnector.read()` / `value()` apply
//!   to a query on a protected connector (`meerschaum_connector_sql::readonly`), and
//!   `uri_marks_protected(uri)` (`?protected=true` on a connector URI).
//! - **`Pipe.verify_schema()`**: `pipe_verify_schema(pipe, context, apply=False, dry_run=False,
//!   debug=False)` plans (and with `apply`, applies) the instance's schema changes for the pipe
//!   (`InstanceConnector::get_pipe_schema_plan` / `apply_pipe_schema`), returning a
//!   `(success, message)` tuple whose message carries the plan table (or the SQL with
//!   `dry_run`).

use meerschaum_connector::ConnectorError;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};

use crate::facade;
use crate::pipe_methods_api::open;
use crate::sync_api::DebugGuard;

/// `_core.protected_allowed()`: whether writes to protected connectors are allowed right now
/// (`MRSM_ALLOW_PROTECTED`, `--allow-protected`, an `allow_protected()` block).
#[pyfunction]
pub fn protected_allowed() -> bool {
    meerschaum_core::protected::allowed()
}

/// `_core.allow_protected_enter()`: enter one allowance (process-wide; pair with
/// `allow_protected_exit()`).
#[pyfunction]
pub fn allow_protected_enter() {
    meerschaum_core::protected::enter();
}

/// `_core.allow_protected_exit()`: leave an allowance entered with `allow_protected_enter()`.
#[pyfunction]
pub fn allow_protected_exit() {
    meerschaum_core::protected::leave();
}

/// `_core.protected_refusal_message(op, keys)`: `Refusing to <op> on protected instance
/// '<keys>'. Pass --allow-protected (or set MRSM_ALLOW_PROTECTED=1).`
#[pyfunction]
pub fn protected_refusal_message(op: &str, keys: &str) -> String {
    meerschaum_core::protected::refusal_message(op, keys)
}

/// `_core.uri_marks_protected(uri)`: whether a connector URI carries `?protected=true` (any
/// truthy value), which marks the connector protected (`Connector.protected`).
#[pyfunction]
pub fn uri_marks_protected(uri: &str) -> bool {
    meerschaum_core::protected::uri_marks_protected(uri)
}

/// `_core.sql_is_read_statement(query, flavor=None)`: whether `query` is one statement that only
/// reads, with quotes and comments read the way `flavor` reads them (every way for an unknown
/// flavor). `SQLConnector.read()` / `value()` refuse any other query on a protected connector
/// without the allowance.
#[pyfunction]
#[pyo3(signature = (query, flavor=None))]
pub fn sql_is_read_statement(query: &str, flavor: Option<&str>) -> bool {
    let flavor = flavor.and_then(|f| f.parse::<meerschaum_connector_sql::Flavor>().ok());
    meerschaum_connector_sql::readonly::is_read_statement(query, flavor)
}

/// `_core.pipe_verify_schema(pipe, context, apply=False, dry_run=False, debug=False)`:
/// `Pipe.verify_schema()`. Without `apply`, nothing is written and the message is the plan; with
/// `dry_run`, the message is the SQL that would run; with `apply`, every applicable change is
/// made (the instance plans again first) and the message ends with what changed.
#[pyfunction]
#[pyo3(signature = (pipe, context, apply=false, dry_run=false, debug=false))]
pub fn pipe_verify_schema<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    apply: bool,
    dry_run: bool,
    debug: bool,
) -> PyResult<Bound<'py, PyTuple>> {
    let _debug = DebugGuard::new(debug);
    let opened = open(py, pipe, context, false)?;
    let core = opened.core.clone();
    let (success, message) = facade::spawn_block_on(py, async move {
        let reader = opened.reader()?;
        let plan = match reader.instance.get_pipe_schema_plan(&core).await {
            Ok(plan) => plan,
            Err(ConnectorError::NotImplemented(message)) => return Ok((false, message)),
            Err(e) => return Err(e.to_string()),
        };
        if dry_run {
            let sql = meerschaum_cli::native::verify_schema::plan_sql(&plan, false);
            return Ok((
                true,
                if sql.is_empty() {
                    format!("{core}: nothing to change.")
                } else {
                    sql
                },
            ));
        }
        if !plan.exists || plan.is_empty() {
            let mut lines = vec![format!(
                "The table of {core} {}.",
                if plan.exists {
                    "matches its dtypes"
                } else {
                    "does not exist yet"
                }
            )];
            lines.extend(plan.notes.iter().map(|n| format!("    Note: {n}")));
            return Ok((true, lines.join("\n")));
        }
        let table = meerschaum_cli::native::verify_schema::plan_table(&plan);
        let header = format!(
            "The table {} of {core} differs from its dtypes:",
            plan.table
        );
        if !apply {
            return Ok((true, format!("{header}\n{table}")));
        }
        let result = reader.instance.apply_pipe_schema(&core, &plan, false).await;
        Ok((
            result.success,
            format!("{header}\n{table}\n\n{}", result.message),
        ))
    })
    .map_err(|e| PyRuntimeError::new_err(e.to_string()))?;
    (success, message).into_pyobject(py)
}

/// Register the functions on `meerschaum._core`.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(protected_allowed, m)?)?;
    m.add_function(wrap_pyfunction!(allow_protected_enter, m)?)?;
    m.add_function(wrap_pyfunction!(allow_protected_exit, m)?)?;
    m.add_function(wrap_pyfunction!(protected_refusal_message, m)?)?;
    m.add_function(wrap_pyfunction!(sql_is_read_statement, m)?)?;
    m.add_function(wrap_pyfunction!(uri_marks_protected, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_verify_schema, m)?)?;
    Ok(())
}
