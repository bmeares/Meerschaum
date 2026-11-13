//! What the console's New team dialog needs beyond the team records (5.1):
//! `_core.team_database_create(...)` (`meerschaum_connector_sql::team_database`) and
//! `_core.grafana_ensure_team(...)` / `_core.grafana_rename_team(...)`
//! (`meerschaum_web::grafana`), the same code `bootstrap teams` runs.
//!
//! The caller (Python) owns the configuration: it passes the base connector's attributes and
//! `api:grafana`, and writes the new connector itself.

use meerschaum_cli::native::team_setup::grafana::{self, GrafanaClient, GrafanaSettings};
use meerschaum_cli::native::team_setup::grafana_datasource;
use meerschaum_connector_sql::instance::SqlConnector;
use meerschaum_connector_sql::team_database::{self, TeamDatabase, TeamDatabaseKind};
use meerschaum_connector_sql::uri::ConnectionSpec;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::convert::{core_to_py, py_to_core};
use crate::facade;

fn settings_of(section: &Bound<'_, PyAny>) -> PyResult<Option<GrafanaSettings>> {
    if section.is_none() {
        return Ok(None);
    }
    Ok(GrafanaSettings::from_section(
        &py_to_core(section)?.to_map(),
    ))
}

fn made_to_py<'py>(
    py: Python<'py>,
    team: &str,
    connector_keys: &str,
    base_keys: &str,
    made: &TeamDatabase,
) -> PyResult<Bound<'py, PyDict>> {
    let out = PyDict::new(py);
    out.set_item(
        "attributes",
        core_to_py(
            py,
            &meerschaum_core::Value::Map(made.connector_attributes(team)),
        )?,
    )?;
    out.set_item(
        "summary",
        core_to_py(
            py,
            &meerschaum_core::Value::Map(made.summary(connector_keys, base_keys)),
        )?,
    )?;
    out.set_item("notes", made.notes.clone())?;
    Ok(out)
}

/// Create the database (`kind='database'`) or schema (`'schema'`) of `team` and its roles on the
/// server of the connector `base_keys` (`base_attributes`: its attributes).
///
/// Returns `(success, message, result)`: `result` is `{'attributes'` (the new connector's, with
/// the team's credentials: write them to the configuration as `connector_keys`), `'summary'`
/// (for the team's `attributes['database']`, no credentials), `'notes'`, `'grafana'` (the
/// organization id when `grafana` (`api:grafana`) is enabled, else `None`), `'grafana_error'}`.
#[pyfunction]
#[pyo3(signature = (base_keys, base_attributes, team, kind, connector_keys, name=None, grafana=None))]
#[allow(clippy::too_many_arguments)]
pub fn team_database_create<'py>(
    py: Python<'py>,
    base_keys: &str,
    base_attributes: &Bound<'py, PyDict>,
    team: &str,
    kind: &str,
    connector_keys: &str,
    name: Option<String>,
    grafana: Option<&Bound<'py, PyAny>>,
) -> PyResult<(bool, String, Option<Bound<'py, PyDict>>)> {
    let Some(kind) = TeamDatabaseKind::parse(kind) else {
        return Err(PyValueError::new_err(format!(
            "Unknown kind '{kind}': use 'database' or 'schema'."
        )));
    };
    let label = base_keys.split_once(':').map_or(base_keys, |(_, l)| l);
    let attributes = crate::sql_api::attributes_from_py(label, base_attributes)?;
    let spec = match ConnectionSpec::from_attributes(&attributes) {
        Ok(spec) => spec,
        Err(e) => return Ok((false, format!("Cannot use '{base_keys}': {e}"), None)),
    };
    let settings = match grafana {
        Some(section) => settings_of(section)?,
        None => None,
    };
    let team_name = team.to_string();
    let label = label.to_string();
    let result = facade::spawn_block_on(py, async move {
        let base = SqlConnector::connect(&label, spec)
            .await
            .map_err(|e| format!("Cannot connect to the base connector: {e}"))?;
        let made = team_database::create(&base, &team_name, kind, name.as_deref()).await?;
        let org = match &settings {
            Some(settings) => {
                let source = grafana_datasource(settings, &team_name, &made);
                Some(
                    grafana::ensure_team(settings, &team_name, Some(&source))
                        .await
                        .map(|(org, _)| org),
                )
            }
            None => None,
        };
        Ok::<_, String>((made, org))
    });
    let (made, org) = match result {
        Ok(result) => result,
        Err(e) => return Ok((false, e, None)),
    };
    let out = made_to_py(py, team, connector_keys, base_keys, &made)?;
    match org {
        Some(Ok(org)) => {
            out.set_item("grafana", org)?;
            out.set_item("grafana_error", py.None())?;
        }
        Some(Err(e)) => {
            out.set_item("grafana", py.None())?;
            out.set_item("grafana_error", e)?;
        }
        None => {
            out.set_item("grafana", py.None())?;
            out.set_item("grafana_error", py.None())?;
        }
    }
    Ok((
        true,
        format!(
            "Created the {} '{}'.",
            made.kind.name(),
            made.schema.as_deref().unwrap_or(&made.database)
        ),
        Some(out),
    ))
}

/// Make the Grafana organization of `team` (`grafana`: `api:grafana`): `(success, message,
/// organization id)`. A disabled integration succeeds with no id.
#[pyfunction]
pub fn grafana_ensure_team(
    py: Python<'_>,
    grafana: &Bound<'_, PyAny>,
    team: &str,
) -> PyResult<(bool, String, Option<i64>)> {
    let Some(settings) = settings_of(grafana)? else {
        return Ok((true, "Grafana is not enabled.".to_string(), None));
    };
    let team_name = team.to_string();
    let result = facade::spawn_block_on(py, async move {
        grafana::ensure_team(&settings, &team_name, None).await
    });
    Ok(match result {
        Ok((org, _)) => (true, "Success".to_string(), Some(org)),
        Err(e) => (false, e, None),
    })
}

/// Rename the Grafana organization of a renamed team: `(success, message)`.
#[pyfunction]
pub fn grafana_rename_team(
    py: Python<'_>,
    grafana: &Bound<'_, PyAny>,
    old: &str,
    new: &str,
) -> PyResult<(bool, String)> {
    let Some(settings) = settings_of(grafana)? else {
        return Ok((true, "Grafana is not enabled.".to_string()));
    };
    let (old, new) = (old.to_string(), new.to_string());
    let result = facade::spawn_block_on(py, async move {
        GrafanaClient::new(settings)?.rename_org(&old, &new).await
    });
    Ok(match result {
        Ok(_) => (true, "Success".to_string()),
        Err(e) => (false, e),
    })
}

/// `_core.team_database_slug(name)`: the default database, schema and connector label of a team
/// (`None` when the name has no letters or digits), and `_core.team_database_valid_name(name)`.
#[pyfunction]
pub fn team_database_slug(name: &str) -> Option<String> {
    team_database::slug(name)
}

/// `(valid, message)` for a database, schema or connector label.
#[pyfunction]
pub fn team_database_valid_name(name: &str) -> (bool, String) {
    match team_database::valid_identifier(name) {
        Ok(()) => (true, "Success".to_string()),
        Err(e) => (false, e),
    }
}

/// Whether a team database can be made on a connector of `flavor`.
#[pyfunction]
pub fn team_database_supported(flavor: &str) -> bool {
    flavor.parse().is_ok_and(team_database::supported)
}

/// Register this module's functions.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(team_database_create, m)?)?;
    m.add_function(wrap_pyfunction!(grafana_ensure_team, m)?)?;
    m.add_function(wrap_pyfunction!(grafana_rename_team, m)?)?;
    m.add_function(wrap_pyfunction!(team_database_slug, m)?)?;
    m.add_function(wrap_pyfunction!(team_database_valid_name, m)?)?;
    m.add_function(wrap_pyfunction!(team_database_supported, m)?)?;
    Ok(())
}
