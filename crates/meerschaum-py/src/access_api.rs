//! The access model for the Python façade (rc15 contract K2): `meerschaum.core.access` calls
//! these, so the console and the Python API apply exactly the rules the API server applies
//! ([`meerschaum_core::access`]).
//!
//! Two kinds of bindings:
//!
//! - **Rules** (pure, no I/O): permissions, roles, pipe access, locks, `can_see` / `can_edit` /
//!   `can_manage_access` / `can_lock` / `can_see_email`. Principals, accesses and teams cross the
//!   boundary as plain values: a principal is a dict (or object) with `username`, `is_admin`,
//!   `permissions` and `teams` (`[(name, role), ...]`), an access a dict with `owner`, `team`,
//!   `visibility` and `edit`.
//! - **Stores** over an instance (the `pipe` / `context` arguments of the other `_core.pipe_*`
//!   bindings, as `status_api` takes them): teams, a pipe's locks and access, a user's
//!   permissions. They call the instance connector's trait methods, so an `api:` instance asks
//!   its server, SQL and Valkey instances write directly, and custom instances go through the
//!   worker.

use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{
    self, EditMode, LockOp, Locks, PipeAccess, Principal, Role, Team, TeamRole, Visibility,
};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Value};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};

use crate::convert::{core_to_py, py_to_core};
use crate::facade;
use crate::pipe_bridge::{ParamsView, core_pipe};
use crate::status_api::instance_of;
use crate::sync_api::DebugGuard;

// ---------------------------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------------------------

/// `obj[name]` for a mapping, else `getattr(obj, name)`; `None` when missing or `None`.
fn field<'py>(obj: &Bound<'py, PyAny>, name: &str) -> PyResult<Option<Bound<'py, PyAny>>> {
    let value = if let Ok(d) = obj.cast::<PyDict>() {
        d.get_item(name)?
    } else if obj.hasattr(name)? {
        Some(obj.getattr(name)?)
    } else {
        None
    };
    Ok(value.filter(|v| !v.is_none()))
}

fn strings(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<String>> {
    let Some(obj) = obj else {
        return Ok(Vec::new());
    };
    if let Ok(s) = obj.extract::<String>() {
        return Ok(s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect());
    }
    let mut out = Vec::new();
    for item in obj.try_iter()? {
        out.push(item?.str()?.to_string());
    }
    Ok(out)
}

fn team_role(obj: &Bound<'_, PyAny>) -> PyResult<TeamRole> {
    let text = obj.str()?.to_string();
    TeamRole::parse(&text).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Invalid team role '{text}': use 'member' or 'manager'."
        ))
    })
}

/// A principal's teams: `[(name, role), ...]`, `[{"name", "role"}, ...]` or `{name: role}`.
fn teams_of(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Vec<(String, TeamRole)>> {
    let Some(obj) = obj else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    if let Ok(d) = obj.cast::<PyDict>() {
        for (k, v) in d.iter() {
            out.push((k.str()?.to_string(), team_role(&v)?));
        }
        return Ok(out);
    }
    for item in obj.try_iter()? {
        let item = item?;
        if let Some(name) = field(&item, "name")?.filter(|_| item.cast::<PyDict>().is_ok()) {
            let role = match field(&item, "role")? {
                Some(r) => team_role(&r)?,
                None => TeamRole::Member,
            };
            out.push((name.str()?.to_string(), role));
            continue;
        }
        let (name, role): (Bound<'_, PyAny>, Bound<'_, PyAny>) = item.extract()?;
        out.push((name.str()?.to_string(), team_role(&role)?));
    }
    Ok(out)
}

/// A principal from a dict or an object with `username`, `is_admin`, `permissions`, `teams`.
fn principal_of(obj: &Bound<'_, PyAny>) -> PyResult<Principal> {
    let username = field(obj, "username")?
        .map(|u| u.str().map(|s| s.to_string()))
        .transpose()?
        .unwrap_or_default();
    let is_admin = field(obj, "is_admin")?
        .map(|v| v.is_truthy())
        .transpose()?
        .unwrap_or(false);
    let permissions = strings(field(obj, "permissions")?.as_ref())?
        .into_iter()
        .collect();
    let teams = teams_of(field(obj, "teams")?.as_ref())?;
    Ok(Principal::new(&username, is_admin, permissions, teams))
}

fn access_of(obj: Option<&Bound<'_, PyAny>>) -> PyResult<PipeAccess> {
    match obj {
        None => Ok(PipeAccess::default()),
        Some(o) if o.is_none() => Ok(PipeAccess::default()),
        Some(o) => Ok(PipeAccess::from_value(Some(&py_to_core(o)?))),
    }
}

fn access_to_py<'py>(py: Python<'py>, access: &PipeAccess) -> PyResult<Bound<'py, PyAny>> {
    core_to_py(py, &access.to_value())
}

fn locks_of(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Locks> {
    match obj {
        None => Ok(Locks::new()),
        Some(o) if o.is_none() => Ok(Locks::new()),
        Some(o) => Ok(Locks::from_value(Some(&py_to_core(o)?))),
    }
}

fn op_of(text: &str) -> PyResult<LockOp> {
    LockOp::parse(text).ok_or_else(|| {
        PyValueError::new_err(format!(
            "Unknown write '{text}': use edit, sync, replace, deduplicate, clear, drop, rebuild, \
             schema, delete or delete_and_drop."
        ))
    })
}

fn role_of(text: Option<&str>) -> PyResult<Role> {
    match text {
        None => Ok(access::DEFAULT_ROLE),
        Some(t) => Role::parse(t).ok_or_else(|| {
            PyValueError::new_err(format!(
                "Invalid role '{t}': use viewer, editor, operator or admin."
            ))
        }),
    }
}

fn map_of(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<Map>> {
    match obj {
        None => Ok(None),
        Some(o) if o.is_none() => Ok(None),
        Some(o) => match py_to_core(o)? {
            Value::Map(m) => Ok(Some(m)),
            Value::Null => Ok(None),
            other => Err(PyValueError::new_err(format!(
                "Expected a dictionary, not {}.",
                other.type_name()
            ))),
        },
    }
}

fn team_of(obj: &Bound<'_, PyAny>) -> PyResult<Team> {
    let mut value = py_to_core(obj)?;
    if !value.is_map() {
        // An object with `name`, `id`, `members` and `attributes` (`mrsm.Team`).
        let dict = PyDict::new(obj.py());
        for key in ["id", "name", "members", "attributes"] {
            if let Some(v) = field(obj, key)? {
                dict.set_item(key, v)?;
            }
        }
        value = py_to_core(dict.as_any())?;
    }
    Team::from_value(&value).ok_or_else(|| PyValueError::new_err("A team needs a name."))
}

fn team_to_py<'py>(py: Python<'py>, team: &Team) -> PyResult<Bound<'py, PyAny>> {
    core_to_py(py, &team.to_value())
}

fn names(permissions: &access::Permissions) -> Vec<String> {
    access::ordered_permissions(permissions)
}

fn user_teams_to_py<'py>(
    py: Python<'py>,
    teams: &[(String, TeamRole)],
) -> PyResult<Bound<'py, PyList>> {
    let out = PyList::empty(py);
    for (name, role) in teams {
        out.append(PyTuple::new(py, [name.as_str(), role.name()])?)?;
    }
    Ok(out)
}

fn tuple_of(res: meerschaum_core::SuccessTuple) -> (bool, String) {
    (res.success, res.message)
}

// ---------------------------------------------------------------------------------------------
// Rules
// ---------------------------------------------------------------------------------------------

/// `_core.access_all_permissions()`: every permission, in order.
#[pyfunction]
pub fn access_all_permissions() -> Vec<String> {
    access::ALL_PERMISSIONS
        .iter()
        .map(|p| p.to_string())
        .collect()
}

/// `_core.access_permission_descriptions()`: `{permission: description}`.
#[pyfunction]
pub fn access_permission_descriptions(py: Python<'_>) -> PyResult<Bound<'_, PyDict>> {
    let out = PyDict::new(py);
    for p in access::ALL_PERMISSIONS {
        out.set_item(p, access::permission_description(p).unwrap_or_default())?;
    }
    Ok(out)
}

/// `_core.access_role_permissions(role)`: a role preset's permissions.
#[pyfunction]
pub fn access_role_permissions(role: &str) -> PyResult<Vec<String>> {
    Ok(names(&role_of(Some(role))?.permissions()))
}

/// `_core.access_resolve_default_role(configured, legacy_actions_non_admin)`.
#[pyfunction]
#[pyo3(signature = (configured=None, legacy_actions_non_admin=None))]
pub fn access_resolve_default_role(
    configured: Option<&str>,
    legacy_actions_non_admin: Option<bool>,
) -> String {
    access::resolve_default_role(configured, legacy_actions_non_admin)
        .name()
        .to_string()
}

/// `_core.access_effective_permissions(user_type, scopes, default_role)`.
#[pyfunction]
#[pyo3(signature = (user_type=None, scopes=None, default_role=None))]
pub fn access_effective_permissions(
    user_type: Option<&str>,
    scopes: Option<&Bound<'_, PyAny>>,
    default_role: Option<&str>,
) -> PyResult<Vec<String>> {
    let scopes = match scopes.filter(|s| !s.is_none()) {
        Some(s) => Some(strings(Some(s))?),
        None => None,
    };
    Ok(names(&access::effective_permissions(
        user_type,
        scopes.as_deref(),
        role_of(default_role)?,
    )))
}

/// `_core.access_user_permissions(user_type, attributes, default_role)`.
#[pyfunction]
#[pyo3(signature = (user_type=None, attributes=None, default_role=None))]
pub fn access_user_permissions(
    user_type: Option<&str>,
    attributes: Option<&Bound<'_, PyAny>>,
    default_role: Option<&str>,
) -> PyResult<Vec<String>> {
    let attributes = map_of(attributes)?;
    Ok(names(&access::user_permissions(
        user_type,
        attributes.as_ref(),
        role_of(default_role)?,
    )))
}

/// `_core.access_user_role(user_type, attributes, default_role)`.
#[pyfunction]
#[pyo3(signature = (user_type=None, attributes=None, default_role=None))]
pub fn access_user_role(
    user_type: Option<&str>,
    attributes: Option<&Bound<'_, PyAny>>,
    default_role: Option<&str>,
) -> PyResult<String> {
    let attributes = map_of(attributes)?;
    Ok(
        access::user_role(user_type, attributes.as_ref(), role_of(default_role)?)
            .name()
            .to_string(),
    )
}

/// `_core.access_attributes_with_role(attributes, role)`.
#[pyfunction]
#[pyo3(signature = (attributes, role))]
pub fn access_attributes_with_role<'py>(
    py: Python<'py>,
    attributes: Option<&Bound<'py, PyAny>>,
    role: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let attributes = map_of(attributes)?.unwrap_or_default();
    core_to_py(
        py,
        &Value::Map(access::attributes_with_role(
            &attributes,
            role_of(Some(role))?,
        )),
    )
}

/// `_core.access_attributes_with_permissions(attributes, permissions)`.
#[pyfunction]
#[pyo3(signature = (attributes, permissions))]
pub fn access_attributes_with_permissions<'py>(
    py: Python<'py>,
    attributes: Option<&Bound<'py, PyAny>>,
    permissions: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let attributes = map_of(attributes)?.unwrap_or_default();
    let permissions = strings(Some(permissions))?;
    core_to_py(
        py,
        &Value::Map(access::attributes_with_permissions(
            &attributes,
            &permissions,
        )),
    )
}

/// `_core.access_token_permissions(token_scopes, user_permissions)`.
#[pyfunction]
pub fn access_token_permissions(
    token_scopes: &Bound<'_, PyAny>,
    user_permissions: &Bound<'_, PyAny>,
) -> PyResult<Vec<String>> {
    let scopes = strings(Some(token_scopes))?;
    let user: access::Permissions = strings(Some(user_permissions))?.into_iter().collect();
    Ok(names(&access::token_permissions(&scopes, &user)))
}

/// `_core.access_pipe_access(parameters)`: the access in a pipe's parameters (normalized).
#[pyfunction]
pub fn access_pipe_access<'py>(
    py: Python<'py>,
    parameters: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let parameters = map_of(Some(parameters))?.unwrap_or_default();
    access_to_py(py, &PipeAccess::from_parameters(&parameters))
}

/// `_core.access_parse_access(value)`: a strict parse of an `access` value (raises `ValueError`
/// on unknown keys, visibilities or edit modes); the normalized dict.
#[pyfunction]
pub fn access_parse_access<'py>(
    py: Python<'py>,
    value: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let access = PipeAccess::parse(&py_to_core(value)?).map_err(PyValueError::new_err)?;
    access_to_py(py, &access)
}

/// `_core.access_visibilities()` and `_core.access_edit_modes()`.
#[pyfunction]
pub fn access_visibilities() -> Vec<&'static str> {
    Visibility::ALL.iter().map(Visibility::name).collect()
}

/// See [`access_visibilities`].
#[pyfunction]
pub fn access_edit_modes() -> Vec<&'static str> {
    EditMode::ALL.iter().map(EditMode::name).collect()
}

/// `_core.access_locks(value)`: lock names from a `locks` value (or a pipe's parameters, when
/// `value` is a dict), unknown names left out.
#[pyfunction]
pub fn access_locks(value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let core = py_to_core(value)?;
    let locks = match &core {
        Value::Map(m) => Locks::from_parameters(m),
        other => Locks::from_value(Some(other)),
    };
    Ok(locks.names())
}

/// `_core.access_parse_locks(value)`: a strict parse of locks a person gave (names or a
/// comma-separated text; empty is `all`); raises `ValueError` on an unknown name.
#[pyfunction]
pub fn access_parse_locks(value: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    let text = strings(Some(value))?.join(",");
    Locks::parse_list(&text)
        .map(|l| l.names())
        .map_err(PyValueError::new_err)
}

/// `_core.access_locks_with(current, added)` / `_core.access_locks_without(current, removed)`.
#[pyfunction]
#[pyo3(signature = (current, added))]
pub fn access_locks_with(
    current: Option<&Bound<'_, PyAny>>,
    added: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<String>> {
    Ok(locks_of(current)?.with(&locks_of(added)?).names())
}

/// See [`access_locks_with`].
#[pyfunction]
#[pyo3(signature = (current, removed))]
pub fn access_locks_without(
    current: Option<&Bound<'_, PyAny>>,
    removed: Option<&Bound<'_, PyAny>>,
) -> PyResult<Vec<String>> {
    Ok(locks_of(current)?.without(&locks_of(removed)?).names())
}

/// `_core.access_check_lock(locks, op)`: `None` when `locks` allow the write `op`, else the
/// reason (`it is locked (sync)`).
#[pyfunction]
pub fn access_check_lock(locks: Option<&Bound<'_, PyAny>>, op: &str) -> PyResult<Option<String>> {
    Ok(access::check_lock(&locks_of(locks)?, op_of(op)?).err())
}

/// `_core.access_lock_refusal(pipe, locks, op)`: the refusal message of the write `op` on
/// `pipe` under `locks` (`None` when allowed).
#[pyfunction]
pub fn access_lock_refusal(
    pipe: &Bound<'_, PyAny>,
    locks: Option<&Bound<'_, PyAny>>,
    op: &str,
) -> PyResult<Option<String>> {
    let core = core_pipe(pipe, ParamsView::Keys)?;
    Ok(access::lock_refusal(&core, &locks_of(locks)?, op_of(op)?))
}

/// `_core.access_is_lock_refusal(message)`.
#[pyfunction]
pub fn access_is_lock_refusal(message: &str) -> bool {
    access::is_lock_refusal(message)
}

/// `_core.access_edit_refusal(pipe, registered, new)`: the refusal of replacing the registered
/// parameters with `new` under a `parameters` lock (`None` when allowed).
#[pyfunction]
pub fn access_edit_refusal(
    pipe: &Bound<'_, PyAny>,
    registered: &Bound<'_, PyAny>,
    new: &Bound<'_, PyAny>,
) -> PyResult<Option<String>> {
    let core = core_pipe(pipe, ParamsView::Keys)?;
    let registered = map_of(Some(registered))?.unwrap_or_default();
    let new = map_of(Some(new))?.unwrap_or_default();
    let locks = Locks::from_parameters(&registered);
    if access::parameters_edit_allowed(&registered, &new, &locks, false) {
        return Ok(None);
    }
    Ok(access::lock_refusal(&core, &locks, LockOp::EditParameters))
}

/// `_core.access_can(rule, principal, access)`: `rule` is `see`, `edit`, `manage_access` or
/// `lock`.
#[pyfunction]
#[pyo3(signature = (rule, principal, access=None))]
pub fn access_can(
    rule: &str,
    principal: &Bound<'_, PyAny>,
    access: Option<&Bound<'_, PyAny>>,
) -> PyResult<bool> {
    let p = principal_of(principal)?;
    let a = access_of(access)?;
    Ok(match rule {
        "see" => access::can_see(&p, &a),
        "edit" => access::can_edit(&p, &a),
        "manage_access" => access::can_manage_access(&p, &a),
        "lock" => access::can_lock(&p, &a),
        other => {
            return Err(PyValueError::new_err(format!(
                "Unknown rule '{other}': use see, edit, manage_access or lock."
            )));
        }
    })
}

/// `_core.access_can_see_email(viewer, target_user_teams, target_username=None)`.
#[pyfunction]
#[pyo3(signature = (viewer, target_user_teams, target_username=None))]
pub fn access_can_see_email(
    viewer: &Bound<'_, PyAny>,
    target_user_teams: Option<&Bound<'_, PyAny>>,
    target_username: Option<&str>,
) -> PyResult<bool> {
    let viewer = principal_of(viewer)?;
    let teams = teams_of(target_user_teams.filter(|t| !t.is_none()))?;
    Ok(match target_username {
        Some(name) => access::can_see_email_of(&viewer, name, &teams),
        None => access::can_see_email(&viewer, &teams),
    })
}

/// `_core.access_can_manage_team(principal, team)` / `_core.access_can_administer_teams(principal)`.
#[pyfunction]
pub fn access_can_manage_team(principal: &Bound<'_, PyAny>, team: &str) -> PyResult<bool> {
    Ok(access::can_manage_team(&principal_of(principal)?, team))
}

/// See [`access_can_manage_team`].
#[pyfunction]
pub fn access_can_administer_teams(principal: &Bound<'_, PyAny>) -> PyResult<bool> {
    Ok(access::can_administer_teams(&principal_of(principal)?))
}

/// `_core.access_check_access_change(principal, old, new)`: `None` when allowed, else why not.
#[pyfunction]
pub fn access_check_access_change(
    principal: &Bound<'_, PyAny>,
    old: Option<&Bound<'_, PyAny>>,
    new: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<String>> {
    Ok(access::check_access_change(
        &principal_of(principal)?,
        &access_of(old)?,
        &access_of(new)?,
    )
    .err())
}

/// `_core.access_for_new_pipe(principal, requested, primary_team)`.
#[pyfunction]
#[pyo3(signature = (principal, requested=None, primary_team="Everyone"))]
pub fn access_for_new_pipe<'py>(
    py: Python<'py>,
    principal: &Bound<'py, PyAny>,
    requested: Option<&Bound<'py, PyAny>>,
    primary_team: &str,
) -> PyResult<Bound<'py, PyAny>> {
    let requested = match requested.filter(|r| !r.is_none()) {
        Some(r) => Some(access_of(Some(r))?),
        None => None,
    };
    access_to_py(
        py,
        &access::access_for_new_pipe(&principal_of(principal)?, requested.as_ref(), primary_team),
    )
}

/// `_core.access_primary_team(attributes, user_teams)`.
#[pyfunction]
#[pyo3(signature = (attributes=None, user_teams=None))]
pub fn access_primary_team(
    attributes: Option<&Bound<'_, PyAny>>,
    user_teams: Option<&Bound<'_, PyAny>>,
) -> PyResult<String> {
    let attributes = map_of(attributes)?;
    let teams = teams_of(user_teams.filter(|t| !t.is_none()))?;
    Ok(access::primary_team(attributes.as_ref(), &teams))
}

/// `_core.access_valid_team_name(name)`: `(success, message)`.
#[pyfunction]
pub fn access_valid_team_name(name: &str) -> (bool, String) {
    tuple_of(access::valid_team_name(name))
}

/// `_core.access_same_team(a, b)`.
#[pyfunction]
#[pyo3(signature = (a=None, b=None))]
pub fn access_same_team(a: Option<&str>, b: Option<&str>) -> bool {
    access::same_team(a, b)
}

/// `_core.access_sql_definition_changed(connector_keys, old, new)`.
#[pyfunction]
pub fn access_sql_definition_changed(
    connector_keys: &str,
    old: &Bound<'_, PyAny>,
    new: &Bound<'_, PyAny>,
) -> PyResult<bool> {
    let old = map_of(Some(old))?.unwrap_or_default();
    let new = map_of(Some(new))?.unwrap_or_default();
    Ok(access::sql_definition_changed(connector_keys, &old, &new))
}

// ---------------------------------------------------------------------------------------------
// Stores over an instance
// ---------------------------------------------------------------------------------------------

fn conn_of(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
) -> PyResult<(std::sync::Arc<dyn InstanceConnector>, meerschaum_core::Pipe)> {
    let core = core_pipe(pipe, ParamsView::Keys)?;
    let conn = instance_of(py, &core, context)?;
    Ok((conn, core))
}

fn runtime_err(e: impl std::fmt::Display) -> PyErr {
    PyRuntimeError::new_err(e.to_string())
}

/// `_core.access_pipe_targets(pipe, context)`: configured, legacy and physical table aliases,
/// with the backend's identifier clipping rule. This only inspects connector metadata.
#[pyfunction]
pub fn access_pipe_targets<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
) -> PyResult<Bound<'py, PyDict>> {
    let core = core_pipe(pipe, ParamsView::Unresolved)?;
    let conn = instance_of(py, &core, context)?;
    let out = PyDict::new(py);
    out.set_item(
        "targets",
        meerschaum_server::access::pipe_targets_on(conn.as_ref(), &core),
    )?;
    out.set_item("identifier_byte_limit", conn.sql_identifier_byte_limit())?;
    out.set_item(
        "definition_instance_keys",
        meerschaum_server::access::guard::definition_instance_keys(&core),
    )?;
    Ok(out)
}

/// `_core.access_sql_guard(text, hidden, identifier_byte_limit=None, target=False)`: the
/// server's SQL guard refusal (`None` when allowed), shared by console Preview and writes.
#[pyfunction]
#[pyo3(signature = (text, hidden, identifier_byte_limit=None, target=false))]
pub fn access_sql_guard(
    text: &str,
    hidden: Vec<String>,
    identifier_byte_limit: Option<usize>,
    target: bool,
) -> Option<String> {
    let hidden = meerschaum_server::sql_guard::HiddenTables::new(hidden)
        .with_identifier_byte_limit(identifier_byte_limit);
    if target {
        meerschaum_server::sql_guard::check_target(text, &hidden).err()
    } else {
        meerschaum_server::sql_guard::check_definition(text, &hidden).err()
    }
}

/// `_core.get_teams(pipe, context)`: every team on the instance of `pipe` (any pipe of that
/// instance; only its instance is read), Everyone first.
#[pyfunction]
#[pyo3(signature = (pipe, context, debug=false))]
pub fn get_teams<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    debug: bool,
) -> PyResult<Bound<'py, PyList>> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let teams =
        facade::spawn_block_on(py, async move { conn.get_teams().await }).map_err(runtime_err)?;
    let out = PyList::empty(py);
    for team in &teams {
        out.append(team_to_py(py, team)?)?;
    }
    Ok(out)
}

/// `_core.get_team(pipe, context, name)`: the team (`None` if it does not exist).
#[pyfunction]
#[pyo3(signature = (pipe, context, name, debug=false))]
pub fn get_team<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    name: &str,
    debug: bool,
) -> PyResult<Bound<'py, PyAny>> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let name = name.to_string();
    let found = facade::spawn_block_on(py, async move { conn.get_team(&name).await })
        .map_err(runtime_err)?;
    match found {
        Some(team) => team_to_py(py, &team),
        None => Ok(py.None().into_bound(py)),
    }
}

/// `_core.register_team(pipe, context, team)`: `(success, message, team)` (the team with its
/// new id).
#[pyfunction]
#[pyo3(signature = (pipe, context, team, debug=false))]
pub fn register_team<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    team: &Bound<'py, PyAny>,
    debug: bool,
) -> PyResult<(bool, String, Bound<'py, PyAny>)> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let team = team_of(team)?;
    let (res, team) = facade::spawn_block_on(py, async move {
        let mut team = team;
        let res = conn.register_team(&mut team).await;
        (res, team)
    });
    Ok((res.success, res.message, team_to_py(py, &team)?))
}

/// `_core.edit_team(pipe, context, team)`: `(success, message)`.
#[pyfunction]
#[pyo3(signature = (pipe, context, team, debug=false))]
pub fn edit_team(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    team: &Bound<'_, PyAny>,
    debug: bool,
) -> PyResult<(bool, String)> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let team = team_of(team)?;
    Ok(tuple_of(facade::spawn_block_on(py, async move {
        conn.edit_team(&team).await
    })))
}

/// `_core.delete_team(pipe, context, name)`: `(success, message)`.
#[pyfunction]
#[pyo3(signature = (pipe, context, name, debug=false))]
pub fn delete_team(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    name: &str,
    debug: bool,
) -> PyResult<(bool, String)> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let name = name.to_string();
    Ok(tuple_of(facade::spawn_block_on(py, async move {
        conn.delete_team(&name).await
    })))
}

/// `_core.rename_team(pipe, context, old_name, new_name)`: `(success, message)`.
#[pyfunction]
#[pyo3(signature = (pipe, context, old_name, new_name, debug=false))]
pub fn rename_team(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    old_name: &str,
    new_name: &str,
    debug: bool,
) -> PyResult<(bool, String)> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let (old_name, new_name) = (old_name.to_string(), new_name.to_string());
    Ok(tuple_of(facade::spawn_block_on(py, async move {
        conn.rename_team(&old_name, &new_name).await
    })))
}

/// `_core.get_user_teams(pipe, context, username)`: `[(team, role), ...]`, Everyone first.
#[pyfunction]
#[pyo3(signature = (pipe, context, username, debug=false))]
pub fn get_user_teams<'py>(
    py: Python<'py>,
    pipe: &Bound<'py, PyAny>,
    context: &Bound<'py, PyDict>,
    username: &str,
    debug: bool,
) -> PyResult<Bound<'py, PyList>> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let username = username.to_string();
    let teams = facade::spawn_block_on(py, async move { conn.get_user_teams(&username).await })
        .map_err(runtime_err)?;
    user_teams_to_py(py, &teams)
}

/// `_core.team_pipes_count(pipe, context, name)`: how many pipes (and saved Pipelines) of the
/// instance name the team (what renaming or deleting it changes).
#[pyfunction]
#[pyo3(signature = (pipe, context, name, debug=false))]
pub fn team_pipes_count(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    name: &str,
    debug: bool,
) -> PyResult<usize> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let name = name.to_string();
    facade::spawn_block_on(py, async move {
        meerschaum_connector::teams::team_pipes(conn.as_ref(), &name).await
    })
    .map(|p| p.len())
    .map_err(runtime_err)
}

/// `_core.pipe_lock(pipe, context, locks)`: add locks to the registered pipe.
#[pyfunction]
#[pyo3(signature = (pipe, context, locks, debug=false))]
pub fn pipe_lock(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    locks: &Bound<'_, PyAny>,
    debug: bool,
) -> PyResult<(bool, String)> {
    let _debug = DebugGuard::new(debug);
    let locks = locks_of(Some(locks))?;
    let (conn, core) = conn_of(py, pipe, context)?;
    Ok(tuple_of(facade::spawn_block_on(py, async move {
        conn.lock_pipe(&core, &locks).await
    })))
}

/// `_core.pipe_unlock(pipe, context, locks)`: remove locks from the registered pipe.
#[pyfunction]
#[pyo3(signature = (pipe, context, locks, debug=false))]
pub fn pipe_unlock(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    locks: &Bound<'_, PyAny>,
    debug: bool,
) -> PyResult<(bool, String)> {
    let _debug = DebugGuard::new(debug);
    let locks = locks_of(Some(locks))?;
    let (conn, core) = conn_of(py, pipe, context)?;
    Ok(tuple_of(facade::spawn_block_on(py, async move {
        conn.unlock_pipe(&core, &locks).await
    })))
}

/// `_core.pipe_set_access(pipe, context, access)`: store the registered pipe's access (a
/// strict parse first: `ValueError` on unknown keys or names).
#[pyfunction]
#[pyo3(signature = (pipe, context, access, debug=false))]
pub fn pipe_set_access(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    access: &Bound<'_, PyAny>,
    debug: bool,
) -> PyResult<(bool, String)> {
    let _debug = DebugGuard::new(debug);
    let access = PipeAccess::parse(&py_to_core(access)?).map_err(PyValueError::new_err)?;
    let (conn, core) = conn_of(py, pipe, context)?;
    Ok(tuple_of(facade::spawn_block_on(py, async move {
        conn.set_pipe_access(&core, &access).await
    })))
}

/// `_core.pipe_lock_refusal(pipe, context, op)`: the refusal of the write `op` under the
/// pipe's registered locks (read now; `None` when allowed).
#[pyfunction]
#[pyo3(signature = (pipe, context, op, debug=false))]
pub fn pipe_lock_refusal(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    op: &str,
    debug: bool,
) -> PyResult<Option<String>> {
    let _debug = DebugGuard::new(debug);
    let op = op_of(op)?;
    let core = core_pipe(pipe, ParamsView::Raw)?;
    let conn = instance_of(py, &core, context)?;
    Ok(facade::spawn_block_on(py, async move {
        meerschaum_connector::lock_refusal(conn.as_ref(), &core, op).await
    })
    .map(|r| r.message))
}

fn user_arg(username: &str) -> User {
    User::new(username, None)
}

/// `_core.get_user_permissions(pipe, context, username)`: the user's effective permissions on
/// the instance of `pipe` (an `api:` instance answers with its server's).
#[pyfunction]
#[pyo3(signature = (pipe, context, username, debug=false))]
pub fn get_user_permissions(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    username: &str,
    debug: bool,
) -> PyResult<Vec<String>> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let user = user_arg(username);
    facade::spawn_block_on(py, async move { conn.get_user_permissions(&user).await })
        .map_err(runtime_err)
}

/// `_core.get_user_role(pipe, context, username)`: the user's role name.
#[pyfunction]
#[pyo3(signature = (pipe, context, username, debug=false))]
pub fn get_user_role(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    username: &str,
    debug: bool,
) -> PyResult<String> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let user = user_arg(username);
    facade::spawn_block_on(py, async move { conn.get_user_role(&user).await })
        .map(|r| r.name().to_string())
        .map_err(runtime_err)
}

/// `_core.set_user_attributes(pipe, context, username, attributes)`: `(success, message)`.
#[pyfunction]
#[pyo3(signature = (pipe, context, username, attributes, debug=false))]
pub fn set_user_attributes(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    username: &str,
    attributes: &Bound<'_, PyAny>,
    debug: bool,
) -> PyResult<(bool, String)> {
    let _debug = DebugGuard::new(debug);
    let attributes = map_of(Some(attributes))?.unwrap_or_default();
    let (conn, _) = conn_of(py, pipe, context)?;
    let user = user_arg(username);
    Ok(tuple_of(facade::spawn_block_on(py, async move {
        conn.set_user_attributes(&user, &attributes).await
    })))
}

/// `_core.forget_user_access(pipe, context, username)`: after a user was deleted, remove them
/// from every team and clear their ownership of pipes (`(success, message)`).
#[pyfunction]
#[pyo3(signature = (pipe, context, username, debug=false))]
pub fn forget_user_access(
    py: Python<'_>,
    pipe: &Bound<'_, PyAny>,
    context: &Bound<'_, PyDict>,
    username: &str,
    debug: bool,
) -> PyResult<(bool, String)> {
    let _debug = DebugGuard::new(debug);
    let (conn, _) = conn_of(py, pipe, context)?;
    let username = username.to_string();
    Ok(tuple_of(facade::spawn_block_on(py, async move {
        meerschaum_connector::teams::forget_user(conn.as_ref(), &username).await
    })))
}

/// Register the bindings.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(access_all_permissions, m)?)?;
    m.add_function(wrap_pyfunction!(access_permission_descriptions, m)?)?;
    m.add_function(wrap_pyfunction!(access_role_permissions, m)?)?;
    m.add_function(wrap_pyfunction!(access_resolve_default_role, m)?)?;
    m.add_function(wrap_pyfunction!(access_effective_permissions, m)?)?;
    m.add_function(wrap_pyfunction!(access_user_permissions, m)?)?;
    m.add_function(wrap_pyfunction!(access_user_role, m)?)?;
    m.add_function(wrap_pyfunction!(access_attributes_with_role, m)?)?;
    m.add_function(wrap_pyfunction!(access_attributes_with_permissions, m)?)?;
    m.add_function(wrap_pyfunction!(access_token_permissions, m)?)?;
    m.add_function(wrap_pyfunction!(access_pipe_access, m)?)?;
    m.add_function(wrap_pyfunction!(access_parse_access, m)?)?;
    m.add_function(wrap_pyfunction!(access_visibilities, m)?)?;
    m.add_function(wrap_pyfunction!(access_edit_modes, m)?)?;
    m.add_function(wrap_pyfunction!(access_locks, m)?)?;
    m.add_function(wrap_pyfunction!(access_parse_locks, m)?)?;
    m.add_function(wrap_pyfunction!(access_locks_with, m)?)?;
    m.add_function(wrap_pyfunction!(access_locks_without, m)?)?;
    m.add_function(wrap_pyfunction!(access_check_lock, m)?)?;
    m.add_function(wrap_pyfunction!(access_lock_refusal, m)?)?;
    m.add_function(wrap_pyfunction!(access_is_lock_refusal, m)?)?;
    m.add_function(wrap_pyfunction!(access_edit_refusal, m)?)?;
    m.add_function(wrap_pyfunction!(access_can, m)?)?;
    m.add_function(wrap_pyfunction!(access_can_see_email, m)?)?;
    m.add_function(wrap_pyfunction!(access_can_manage_team, m)?)?;
    m.add_function(wrap_pyfunction!(access_can_administer_teams, m)?)?;
    m.add_function(wrap_pyfunction!(access_check_access_change, m)?)?;
    m.add_function(wrap_pyfunction!(access_for_new_pipe, m)?)?;
    m.add_function(wrap_pyfunction!(access_primary_team, m)?)?;
    m.add_function(wrap_pyfunction!(access_valid_team_name, m)?)?;
    m.add_function(wrap_pyfunction!(access_same_team, m)?)?;
    m.add_function(wrap_pyfunction!(access_sql_definition_changed, m)?)?;
    m.add_function(wrap_pyfunction!(access_pipe_targets, m)?)?;
    m.add_function(wrap_pyfunction!(access_sql_guard, m)?)?;
    m.add_function(wrap_pyfunction!(get_teams, m)?)?;
    m.add_function(wrap_pyfunction!(get_team, m)?)?;
    m.add_function(wrap_pyfunction!(register_team, m)?)?;
    m.add_function(wrap_pyfunction!(edit_team, m)?)?;
    m.add_function(wrap_pyfunction!(delete_team, m)?)?;
    m.add_function(wrap_pyfunction!(rename_team, m)?)?;
    m.add_function(wrap_pyfunction!(get_user_teams, m)?)?;
    m.add_function(wrap_pyfunction!(team_pipes_count, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_lock, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_unlock, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_set_access, m)?)?;
    m.add_function(wrap_pyfunction!(pipe_lock_refusal, m)?)?;
    m.add_function(wrap_pyfunction!(get_user_permissions, m)?)?;
    m.add_function(wrap_pyfunction!(get_user_role, m)?)?;
    m.add_function(wrap_pyfunction!(set_user_attributes, m)?)?;
    m.add_function(wrap_pyfunction!(forget_user_access, m)?)?;
    Ok(())
}
