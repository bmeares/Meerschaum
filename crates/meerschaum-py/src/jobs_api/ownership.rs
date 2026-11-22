//! Job ownership (`meerschaum_core::job_access`, `meerschaum_jobs::ownership`): who may see a
//! job, its access and ownership properties, and a team's jobs.

use super::*;

/// The access principal of `username` (`""`: anonymous), an administrator or not, in `teams`
/// (`(team, role)` pairs; roles other than `manager` are members).
fn viewer_principal(
    username: Option<&str>,
    is_admin: bool,
    teams: Vec<(String, String)>,
    isolated: bool,
) -> meerschaum_core::access::Principal {
    use meerschaum_core::access::{EVERYONE, Principal, TeamGrants, TeamRole};
    let mut all = vec![(EVERYONE.to_string(), TeamRole::Member)];
    for (team, role) in teams {
        all.push((team, TeamRole::parse(&role).unwrap_or(TeamRole::Member)));
    }
    Principal::new(username.unwrap_or(""), is_admin, Default::default(), all).with_grants(
        TeamGrants {
            isolated,
            ..Default::default()
        },
    )
}

/// Whether the person `username` (an administrator or not, in `teams`) may see the job `name`
/// whose properties are `properties` (`meerschaum.jobs.filter_visible_jobs`).
#[pyfunction]
#[pyo3(signature = (name, properties, username=None, is_admin=false, teams=None, isolated=false))]
pub(super) fn jobs_can_see(
    name: &str,
    properties: &Bound<'_, PyAny>,
    username: Option<&str>,
    is_admin: bool,
    teams: Option<Vec<(String, String)>>,
    isolated: bool,
) -> PyResult<bool> {
    let props = if properties.is_none() {
        Map::new()
    } else {
        py_to_core(properties)?.to_map()
    };
    let principal = viewer_principal(username, is_admin, teams.unwrap_or_default(), isolated);
    Ok(meerschaum_core::job_access::can_see_job_properties(
        &principal, name, &props,
    ))
}

/// Who a job's properties say it belongs to: `{"owner", "team", "private"}`.
#[pyfunction]
pub(super) fn jobs_access_of<'py>(
    py: Python<'py>,
    name: &str,
    properties: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let props = if properties.is_none() {
        Map::new()
    } else {
        py_to_core(properties)?.to_map()
    };
    let access = meerschaum_core::job_access::JobAccess::from_properties(name, &props);
    map_to_py(py, job_access_map(&access))
}

/// The properties that record who started a job (`owner`, `team`; Everyone is `None`), for
/// `Job(..., _properties=...)`.
#[pyfunction]
#[pyo3(signature = (owner=None, team=None))]
pub(super) fn jobs_ownership_properties<'py>(
    py: Python<'py>,
    owner: Option<&str>,
    team: Option<&str>,
) -> PyResult<Bound<'py, PyAny>> {
    let mut props = Map::new();
    meerschaum_core::job_access::JobAccess::stamp(&mut props, owner, team);
    map_to_py(py, props)
}

/// The jobs of this process's root (local and `systemd`) whose team is `team`.
#[pyfunction]
pub(super) fn jobs_team_jobs(py: Python<'_>, team: &str) -> PyResult<Vec<String>> {
    let ctx = build_ctx(py, None)?;
    let team = team.to_string();
    Ok(py.detach(|| meerschaum_jobs::ownership::team_jobs(&ctx.jobs, &team)))
}

/// Rename team `old` to `new` in this root's jobs that name it (`None`: move them to Everyone).
/// Returns `(updated job names, [(job, why it failed), ...])`.
#[pyfunction]
#[pyo3(signature = (old, new=None))]
#[allow(clippy::type_complexity)]
pub(super) fn jobs_retag_team(
    py: Python<'_>,
    old: &str,
    new: Option<&str>,
) -> PyResult<(Vec<String>, Vec<(String, String)>)> {
    let ctx = build_ctx(py, None)?;
    let (old, new) = (old.to_string(), new.map(str::to_string));
    let retagged =
        py.detach(|| meerschaum_jobs::ownership::retag_team(&ctx.jobs, &old, new.as_deref()));
    Ok((retagged.jobs, retagged.failed))
}
