//! Teams over HTTP (rc15 access model, AC3, AC12), in the `users` route group and the v5 shape
//! (`{"success", "message", ...}`, [`crate::envelope`]). Teams live on the server's instance (its
//! user store) like users and tokens ([`meerschaum_connector::teams`]).
//!
//! | Route | Scopes | Who | What it does |
//! |---|---|---|---|
//! | `GET /teams` | `users:read` | anyone | Every team (`teams`): Everyone first, then by name |
//! | `GET /teams/{name}` | `users:read` | anyone | One team (`team`); `?counts=true` adds its `pipes` and `jobs` counts |
//! | `POST /teams/{name}/register` | `users:write` | administrators | A new team (`members`, `managers`, `attributes`) |
//! | `PATCH /teams/{name}/edit` | `users:write` | administrators; the team's managers for members | Change members (`members`, `add_members`, `remove_members`), `managers`, `attributes` |
//! | `POST /teams/{name}/rename` | `users:write` | administrators | Rename (`{"name": new}`): in its pipes, saved Pipelines, primary teams and jobs too |
//! | `DELETE /teams/{name}/delete` | `users:write` | administrators | Delete: its pipes, saved Pipelines and jobs move to Everyone |
//!
//! A team is `{"id", "name", "members": {username: "member" | "manager"}, "managers": [...],
//! "attributes": {...}}` (managers are members too). The built-in team Everyone has every user
//! as a member; its record lists only the users with a role of their own (its managers); it
//! cannot be registered, renamed or deleted. A team's managers add and remove its members but
//! neither choose its managers nor change its attributes.

use axum::Router;
use axum::body::Body;
use axum::extract::Path;
use axum::routing::{delete, get, patch, post};
use meerschaum_core::Map;
use meerschaum_core::access::{Team, TeamRole};
use serde_json::{Map as JsonMap, Value as Json, json};

use super::{Ctx, body_bytes};
use crate::access::{self, Access, rules};
use crate::config::ServerConfig;
use crate::envelope;
use crate::error::{ApiError, ApiResult};
use crate::json::{from_core, to_core};
use crate::state::AppState;

/// Register the teams routes (part of the `users` group).
pub fn routes(app: Router<AppState>, _cfg: &ServerConfig) -> Router<AppState> {
    app.route("/teams", get(list))
        .route("/teams/{name}", get(detail))
        .route("/teams/{name}/register", post(register))
        .route("/teams/{name}/edit", patch(edit))
        .route("/teams/{name}/rename", post(rename))
        .route("/teams/{name}/delete", delete(remove))
}

/// A team as JSON: [`Team::to_value`] plus its `managers`.
pub fn team_json(team: &Team) -> Json {
    let mut out = from_core(&team.to_value());
    if let Some(o) = out.as_object_mut() {
        o.insert("managers".into(), json!(team.managers()));
        o.insert("everyone".into(), json!(team.is_everyone()));
    }
    out
}

fn failed(context: &str, e: impl std::fmt::Display) -> ApiError {
    ApiError::exception(context, e)
}

async fn json_body(body: Body) -> Result<Json, ApiError> {
    let body = super::pipes::parse_json_body(&body_bytes(body).await?)?
        .unwrap_or(Json::Object(JsonMap::new()));
    if !body.is_object() {
        return Err(ApiError::new(
            400,
            "The request body must be a JSON object.",
        ));
    }
    Ok(body)
}

fn names(body: &Json, key: &str) -> Result<Option<Vec<String>>, ApiError> {
    match body.get(key) {
        None | Some(Json::Null) => Ok(None),
        Some(Json::Array(items)) => Ok(Some(
            items
                .iter()
                .map(|i| match i {
                    Json::String(s) => Ok(s.trim().to_string()),
                    other => Err(ApiError::new(
                        400,
                        format!("`{key}` must be a list of usernames, got {other}."),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter(|s| !s.is_empty())
                .collect(),
        )),
        Some(Json::String(s)) => Ok(Some(
            s.split(',')
                .map(|n| n.trim().to_string())
                .filter(|n| !n.is_empty())
                .collect(),
        )),
        Some(other) => Err(ApiError::new(
            400,
            format!("`{key}` must be a list of usernames, got {other}."),
        )),
    }
}

fn attributes(body: &Json) -> Result<Option<Map>, ApiError> {
    match body.get("attributes") {
        None | Some(Json::Null) => Ok(None),
        Some(Json::Object(o)) => Ok(Some(to_core(&Json::Object(o.clone())).to_map())),
        Some(other) => Err(ApiError::new(
            400,
            format!("`attributes` must be a dictionary, got {other}."),
        )),
    }
}

async fn team_or_404(state: &AppState, name: &str) -> Result<Team, ApiError> {
    state
        .instance()
        .get_team(name)
        .await
        .map_err(|e| failed("get_team", e))?
        .ok_or_else(|| ApiError::new(404, format!("Team '{name}' does not exist.")))
}

fn require_admin(caller: &Access, what: &str) -> Result<(), ApiError> {
    if caller.no_auth || rules::can_administer_teams(&caller.principal) {
        return Ok(());
    }
    Err(ApiError::new(
        403,
        format!("Only administrators may {what}."),
    ))
}

/// After a change of teams: who is in which team is read again.
fn forget(state: &AppState) {
    state.access_cache().forget_identities();
    state.access_cache().forget_pipes();
}

async fn list(ctx: Ctx) -> ApiResult {
    access::require(&ctx.state, &ctx.headers, &["users:read"]).await?;
    let teams = ctx
        .state
        .instance()
        .get_teams()
        .await
        .map_err(|e| failed("get_teams", e))?;
    let n = teams.len();
    Ok(envelope::ok_with(
        format!("Found {n} team{}.", if n == 1 { "" } else { "s" }),
        "teams",
        Json::Array(teams.iter().map(team_json).collect()),
    ))
}

async fn detail(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["users:read"]).await?;
    let team = team_or_404(&ctx.state, &name).await?;
    let mut fields = JsonMap::new();
    fields.insert("team".into(), team_json(&team));
    if ctx.query.bool("counts", false)? {
        // What deleting the team would move to Everyone: its pipes and jobs (administrators see
        // every count; others count only what they can see).
        let conn = ctx.state.instance();
        let pipes = meerschaum_connector::teams::team_pipes(conn.as_ref(), &team.name)
            .await
            .map_err(|e| failed("team_pipes", e))?;
        let visible = pipes
            .iter()
            .filter(|p| caller.can_see(p.raw_parameters()))
            .count();
        fields.insert("pipes".into(), json!(visible));
        fields.insert(
            "jobs".into(),
            json!(if caller.in_team(Some(&team.name)) {
                ctx.state.jobs.team_jobs(&team.name).await.len()
            } else {
                0
            }),
        );
    }
    Ok(envelope::ok(format!("Team '{}'.", team.name), fields))
}

async fn register(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["users:write"]).await?;
    let body = json_body(body).await?;
    require_admin(&caller, "create teams")?;
    let valid = rules::valid_team_name(&name);
    if !valid.success {
        return Err(ApiError::new(400, valid.message));
    }
    let mut team = Team::new(&name);
    for member in names(&body, "members")?.unwrap_or_default() {
        team.add_member(&member, TeamRole::Member);
    }
    for manager in names(&body, "managers")?.unwrap_or_default() {
        team.add_member(&manager, TeamRole::Manager);
    }
    if let Some(attrs) = attributes(&body)? {
        team.attributes = attrs;
    }
    let conn = ctx.state.instance();
    if conn
        .get_team(&name)
        .await
        .map_err(|e| failed("get_team", e))?
        .is_some()
    {
        return Err(ApiError::new(409, format!("Team '{name}' already exists.")));
    }
    let res = conn.register_team(&mut team).await;
    forget(&ctx.state);
    if !res.success {
        return Err(ApiError::new(400, res.message));
    }
    let team = team_or_404(&ctx.state, &name).await?;
    Ok(envelope::ok_with(res.message, "team", team_json(&team)))
}

async fn edit(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["users:write"]).await?;
    let body = json_body(body).await?;
    let mut team = team_or_404(&ctx.state, &name).await?;
    let admin = caller.no_auth || rules::can_administer_teams(&caller.principal);
    if !admin && !rules::can_manage_team(&caller.principal, &team.name) {
        return Err(ApiError::new(
            403,
            format!(
                "Only administrators and the managers of team '{}' may change it.",
                team.name
            ),
        ));
    }
    let managers = names(&body, "managers")?;
    let new_attributes = attributes(&body)?;
    if !admin && (managers.is_some() || new_attributes.is_some()) {
        return Err(ApiError::new(
            403,
            "A team's managers add and remove its members; only administrators choose its \
             managers or change its attributes.",
        ));
    }
    let old_managers = team.managers();
    if let Some(members) = names(&body, "members")? {
        // The full list of members (managers stay managers unless `managers` says otherwise).
        if team.is_everyone() {
            return Err(ApiError::new(
                400,
                "Every user is a member of Everyone: change its `managers` instead.",
            ));
        }
        let keep: Vec<String> = members;
        for name in team.member_names().iter().filter(|n| !keep.contains(n)) {
            if !admin && old_managers.contains(name) {
                return Err(ApiError::new(
                    403,
                    format!("Only administrators may remove the manager '{name}'."),
                ));
            }
            team.remove_member(name);
        }
        for name in keep {
            if team.role_of(&name).is_none() {
                team.add_member(&name, TeamRole::Member);
            }
        }
    }
    for name in names(&body, "add_members")?.unwrap_or_default() {
        if team.is_everyone() {
            continue;
        }
        if !team.members.contains_key(&name) {
            team.add_member(&name, TeamRole::Member);
        }
    }
    for name in names(&body, "remove_members")?.unwrap_or_default() {
        if !admin && old_managers.contains(&name) {
            return Err(ApiError::new(
                403,
                format!("Only administrators may remove the manager '{name}'."),
            ));
        }
        team.remove_member(&name);
    }
    if let Some(managers) = managers {
        for (name, role) in team.members.iter_mut() {
            if *role == TeamRole::Manager && !managers.contains(name) {
                *role = TeamRole::Member;
            }
        }
        if team.is_everyone() {
            team.members.retain(|_, role| *role == TeamRole::Manager);
        }
        for manager in managers {
            team.add_member(&manager, TeamRole::Manager);
        }
    }
    if let Some(attrs) = new_attributes {
        team.attributes = attrs;
    }
    let res = ctx.state.instance().edit_team(&team).await;
    forget(&ctx.state);
    if !res.success {
        return Err(ApiError::new(400, res.message));
    }
    let team = team_or_404(&ctx.state, &name).await?;
    Ok(envelope::ok_with(res.message, "team", team_json(&team)))
}

async fn rename(ctx: Ctx, Path(name): Path<String>, body: Body) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["users:write"]).await?;
    let body = json_body(body).await?;
    require_admin(&caller, "rename teams")?;
    let Some(new_name) = body
        .get("name")
        .or_else(|| body.get("new_name"))
        .and_then(Json::as_str)
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .map(str::to_string)
    else {
        return Err(ApiError::new(400, "Give the team's new `name`."));
    };
    let team = team_or_404(&ctx.state, &name).await?;
    let res = ctx
        .state
        .instance()
        .rename_team(&team.name, &new_name)
        .await;
    forget(&ctx.state);
    if !res.success {
        return Err(ApiError::new(400, res.message));
    }
    let jobs = ctx.state.jobs.retag_team(&team.name, Some(&new_name)).await;
    let renamed = team_or_404(&ctx.state, &new_name).await?;
    let mut fields = JsonMap::new();
    fields.insert("team".into(), team_json(&renamed));
    fields.insert("jobs".into(), json!(jobs));
    Ok(envelope::ok(res.message, fields))
}

async fn remove(ctx: Ctx, Path(name): Path<String>) -> ApiResult {
    let caller = access::require(&ctx.state, &ctx.headers, &["users:write"]).await?;
    require_admin(&caller, "delete teams")?;
    let team = team_or_404(&ctx.state, &name).await?;
    if team.is_everyone() {
        return Err(ApiError::new(
            400,
            "Everyone is the built-in team of every user and cannot be deleted.",
        ));
    }
    let res = ctx.state.instance().delete_team(&team.name).await;
    forget(&ctx.state);
    if !res.success {
        return Err(ApiError::new(400, res.message));
    }
    let jobs = ctx.state.jobs.retag_team(&team.name, None).await;
    Ok(envelope::ok_with(res.message, "jobs", json!(jobs)))
}

/// A user's teams as JSON (`[{"name", "role"}]`, Everyone first).
pub fn user_teams_json(teams: &[(String, TeamRole)]) -> Json {
    Json::Array(
        teams
            .iter()
            .map(|(name, role)| json!({"name": name, "role": role.name()}))
            .collect(),
    )
}

/// A map of usernames to their teams, from every team (one read).
pub fn teams_by_user(teams: &[Team]) -> indexmap::IndexMap<String, Vec<(String, TeamRole)>> {
    let mut out: indexmap::IndexMap<String, Vec<(String, TeamRole)>> = indexmap::IndexMap::new();
    for team in teams {
        for (user, role) in &team.members {
            out.entry(user.clone())
                .or_default()
                .push((team.name.clone(), *role));
        }
    }
    out
}

/// `teams` with Everyone first (a member unless its record says manager).
pub fn with_everyone(mut teams: Vec<(String, TeamRole)>) -> Vec<(String, TeamRole)> {
    if !teams.iter().any(|(t, _)| rules::is_everyone(Some(t))) {
        teams.insert(0, (rules::EVERYONE.to_string(), TeamRole::Member));
    }
    teams
}
