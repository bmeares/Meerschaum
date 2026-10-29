//! The access model over HTTP (rc15, decisions AC3, AC4, AC5, AC12, AC13): teams, a pipe's access
//! and locks, and users' permissions, roles, primary teams and email addresses, through the
//! routes the server enforces:
//!
//! | Client method | Route |
//! |---|---|
//! | [`APIConnector::get_teams`], [`APIConnector::get_team`] | `GET /teams`, `GET /teams/{name}` |
//! | [`APIConnector::register_team`] | `POST /teams/{name}/register` |
//! | [`APIConnector::edit_team`] | `PATCH /teams/{name}/edit` (only what changed) |
//! | [`APIConnector::rename_team`], [`APIConnector::delete_team`] | `POST /teams/{name}/rename`, `DELETE /teams/{name}/delete` |
//! | [`APIConnector::get_team_counts`] | `GET /teams/{name}?counts=true` |
//! | [`APIConnector::get_pipe_access`] | `GET /pipes/{keys}/access` (`/pipelines/{name}/access`) |
//! | [`APIConnector::set_pipe_access`] | `PATCH /pipes/{keys}/access` |
//! | [`APIConnector::lock_pipe`], [`APIConnector::unlock_pipe`] | `POST /pipes/{keys}/lock`, `/unlock` |
//! | [`APIConnector::get_user_access`], [`APIConnector::get_users_access`] | `GET /users/{name}/permissions`, `GET /users/permissions` |
//! | [`APIConnector::set_user_attributes`] | `PATCH /users/{name}/permissions` |
//!
//! Saved Pipelines (`Pipe('mrsm', 'pipeline', name)`) use their own routes
//! (`/pipelines/{name}/access`, `/lock`, `/unlock`). A server older than 5.0.0rc15 has none of
//! these routes: reads fail with [`ConnectorError::NotImplemented`] and changes with a message
//! that says so.

use meerschaum_core::access::{Locks, PipeAccess, Role, Team, TeamRole};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, json};

use crate::routes;
use crate::traits::{ConnectorError, ConnectorResult};
use crate::wire::QueryValue;

use super::{APIConnector, ApiResponse, Body, RequestOptions};

/// The message of a server without the access model's routes.
pub const UNSUPPORTED: &str = "This API server has no teams, pipe access or locks (it runs a \
     Meerschaum older than 5.0.0rc15).";

/// `/teams`.
pub const TEAMS: &str = "/teams";

/// One user as `GET /users/{name}/permissions` describes them.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UserAccess {
    /// The username.
    pub username: String,
    /// The user ID.
    #[serde(default)]
    pub user_id: Option<Json>,
    /// The user type (`admin` for administrators).
    #[serde(default)]
    pub user_type: Option<String>,
    /// The role (`viewer`, `editor`, `operator`, `admin` or `custom`).
    #[serde(default)]
    pub role: Option<String>,
    /// The effective permissions.
    #[serde(default)]
    pub permissions: Vec<String>,
    /// Whether the permissions are the user's own (`attributes['scopes']`) rather than a role's.
    #[serde(default)]
    pub explicit: bool,
    /// The teams and the user's role in each (`[{"name", "role"}]`, Everyone first).
    #[serde(default)]
    pub teams: Vec<UserTeam>,
    /// The primary team (the default team of the pipes the user creates).
    #[serde(default)]
    pub primary_team: Option<String>,
    /// The email address, when the caller may see it (their own; administrators; the managers
    /// of a team the user is in).
    #[serde(default)]
    pub email: Option<String>,
}

/// One team of a user ([`UserAccess::teams`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserTeam {
    /// The team's name.
    pub name: String,
    /// `member` or `manager`.
    #[serde(default)]
    pub role: Option<String>,
}

impl UserAccess {
    /// The teams as `(name, role)` pairs (unknown roles read as members).
    pub fn team_roles(&self) -> Vec<(String, TeamRole)> {
        self.teams
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    t.role
                        .as_deref()
                        .and_then(TeamRole::parse)
                        .unwrap_or(TeamRole::Member),
                )
            })
            .collect()
    }

    /// The role ([`Role::Custom`] when the server named none it knows).
    pub fn role(&self) -> Role {
        self.role
            .as_deref()
            .and_then(Role::parse)
            .unwrap_or(Role::Custom)
    }
}

/// A pipe's access and locks as `GET /pipes/{keys}/access` describes them.
#[derive(Debug, Clone, PartialEq)]
pub struct PipeAccessInfo {
    /// Owner, team, visibility and edit mode.
    pub access: PipeAccess,
    /// The locks.
    pub locks: Locks,
    /// What the caller may do: `edit`, `manage_access`, `lock`.
    pub may: Map,
}

/// Whether a response says the route does not exist (a server before 5.0.0rc15).
fn missing_route(r: &ApiResponse) -> bool {
    if r.status == 405 {
        return true;
    }
    if r.status != 404 {
        return false;
    }
    // A v5 route answers its own 404s with `{"success": false, ...}`; an unknown route answers
    // Starlette's `{"detail": "Not Found"}`.
    matches!(
        r.json().ok().as_ref().and_then(|j| j.get("detail")),
        Some(Json::String(d)) if d == "Not Found"
    )
}

/// The `{"success", "message", ...}` object of a v5 answer, or the error (the server's message).
fn envelope(r: &ApiResponse) -> ConnectorResult<Json> {
    if missing_route(r) {
        return Err(ConnectorError::NotImplemented(UNSUPPORTED.into()));
    }
    r.envelope()
}

/// A change's answer as a `SuccessTuple` (the server's message either way).
fn change_tuple(r: ConnectorResult<ApiResponse>, what: &str) -> SuccessTuple {
    let r = match r {
        Ok(r) => r,
        Err(e) => return SuccessTuple::fail(format!("Failed to {what}:\n{e}")),
    };
    if missing_route(&r) {
        return SuccessTuple::fail(UNSUPPORTED);
    }
    match r.envelope() {
        Ok(j) => SuccessTuple::ok(
            j.get("message")
                .and_then(Json::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| "Success".into()),
        ),
        Err(ConnectorError::Http { body, .. }) => SuccessTuple::fail(body),
        Err(e) => SuccessTuple::fail(format!("Failed to {what}:\n{e}")),
    }
}

/// A team from the server's JSON (`{"id", "name", "members", "attributes", ...}`).
fn team_of(j: &Json) -> Option<Team> {
    Team::from_value(&crate::pipe_ops::json_to_core(j))
}

fn json_of(v: &Value) -> Json {
    serde_json::to_value(v).unwrap_or(Json::Null)
}

/// The names with `role` in `team`.
fn names_with(team: &Team, role: TeamRole) -> Vec<String> {
    team.members
        .iter()
        .filter(|(_, r)| **r == role)
        .map(|(u, _)| u.clone())
        .collect()
}

/// The path segment of a name (a team or a user).
fn segment(name: &str) -> String {
    crate::wire::quote_plus(name.trim())
}

impl APIConnector {
    // ----------------------------------------------------------------------------------------
    // Teams
    // ----------------------------------------------------------------------------------------

    /// `GET /teams`: every team (Everyone first).
    pub async fn get_teams(&self) -> ConnectorResult<Vec<Team>> {
        let r = self.get(TEAMS, RequestOptions::default()).await?;
        let j = envelope(&r)?;
        Ok(j.get("teams")
            .and_then(Json::as_array)
            .map(|items| items.iter().filter_map(team_of).collect())
            .unwrap_or_default())
    }

    /// `GET /teams/{name}`: the team (`None` when it does not exist).
    pub async fn get_team(&self, name: &str) -> ConnectorResult<Option<Team>> {
        let r = self
            .get(
                &format!("{TEAMS}/{}", segment(name)),
                RequestOptions::default(),
            )
            .await?;
        if r.status == 404 && !missing_route(&r) {
            return Ok(None);
        }
        let j = envelope(&r)?;
        Ok(j.get("team").and_then(team_of))
    }

    /// `GET /teams/{name}?counts=true`: how many pipes (and saved Pipelines) and jobs name the
    /// team, as far as the caller can see them (what deleting it moves to Everyone).
    pub async fn get_team_counts(&self, name: &str) -> ConnectorResult<(usize, usize)> {
        let r = self
            .get(
                &format!("{TEAMS}/{}", segment(name)),
                RequestOptions::query(vec![("counts", QueryValue::One("true".into()))]),
            )
            .await?;
        let j = envelope(&r)?;
        let count = |key: &str| j.get(key).and_then(Json::as_u64).unwrap_or(0) as usize;
        Ok((count("pipes"), count("jobs")))
    }

    /// `POST /teams/{name}/register`: a new team with its members, managers and attributes (its
    /// new id is written to `team.id`). Administrators only.
    pub async fn register_team(&self, team: &mut Team) -> SuccessTuple {
        let body = json!({
            "members": names_with(team, TeamRole::Member),
            "managers": names_with(team, TeamRole::Manager),
            "attributes": json_of(&Value::Map(team.attributes.clone())),
        });
        let r = self
            .post(
                &format!("{TEAMS}/{}/register", segment(&team.name)),
                RequestOptions {
                    body: Body::Json(body.to_string()),
                    ..Default::default()
                },
            )
            .await;
        if let Ok(r) = &r
            && let Ok(j) = r.envelope()
            && let Some(registered) = j.get("team").and_then(team_of)
        {
            team.id = registered.id;
        }
        change_tuple(r, &format!("register team '{}'", team.name))
    }

    /// `PATCH /teams/{name}/edit` with what changed from the stored team: members added
    /// (`add_members`) and removed (`remove_members`), then the managers and the attributes
    /// when they changed. A team's managers may add and remove its members; only
    /// administrators choose its managers and change its attributes.
    pub async fn edit_team(&self, team: &Team) -> SuccessTuple {
        let current = match self.get_team(&team.name).await {
            Ok(Some(t)) => t,
            Ok(None) => return SuccessTuple::fail(format!("Team '{}' does not exist.", team.name)),
            Err(ConnectorError::NotImplemented(m)) => return SuccessTuple::fail(m),
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let mut body = serde_json::Map::new();
        if !current.is_everyone() {
            let added: Vec<&String> = team
                .members
                .keys()
                .filter(|m| !current.members.contains_key(*m))
                .collect();
            let removed: Vec<&String> = current
                .members
                .keys()
                .filter(|m| !team.members.contains_key(*m))
                .collect();
            if !added.is_empty() {
                body.insert("add_members".into(), json!(added));
            }
            if !removed.is_empty() {
                body.insert("remove_members".into(), json!(removed));
            }
        }
        let mut wanted = names_with(team, TeamRole::Manager);
        let mut stored = current.managers();
        wanted.sort();
        stored.sort();
        if wanted != stored {
            body.insert(
                "managers".into(),
                json!(names_with(team, TeamRole::Manager)),
            );
        }
        if team.attributes != current.attributes {
            body.insert(
                "attributes".into(),
                json_of(&Value::Map(team.attributes.clone())),
            );
        }
        let r = self
            .patch(
                &format!("{TEAMS}/{}/edit", segment(&current.name)),
                RequestOptions {
                    body: Body::Json(Json::Object(body).to_string()),
                    ..Default::default()
                },
            )
            .await;
        change_tuple(r, &format!("edit team '{}'", team.name))
    }

    /// `POST /teams/{old}/rename`: rename a team (in the pipes, saved Pipelines, primary teams
    /// and jobs that name it too). Administrators only.
    pub async fn rename_team(&self, old_name: &str, new_name: &str) -> SuccessTuple {
        let r = self
            .post(
                &format!("{TEAMS}/{}/rename", segment(old_name)),
                RequestOptions {
                    body: Body::Json(json!({"name": new_name.trim()}).to_string()),
                    ..Default::default()
                },
            )
            .await;
        change_tuple(r, &format!("rename team '{old_name}'"))
    }

    /// `DELETE /teams/{name}/delete`: delete a team (its pipes, saved Pipelines and jobs move to
    /// Everyone). Administrators only.
    pub async fn delete_team(&self, name: &str) -> SuccessTuple {
        let r = self
            .delete(
                &format!("{TEAMS}/{}/delete", segment(name)),
                RequestOptions::default(),
            )
            .await;
        change_tuple(r, &format!("delete team '{name}'"))
    }

    // ----------------------------------------------------------------------------------------
    // Users' permissions, roles, teams and emails
    // ----------------------------------------------------------------------------------------

    /// `GET /users/{name}/permissions` (`None` when the user does not exist).
    pub async fn get_user_access(&self, username: &str) -> ConnectorResult<Option<UserAccess>> {
        let r = self
            .get(
                &format!("{}/{}/permissions", routes::USERS, segment(username)),
                RequestOptions::default(),
            )
            .await?;
        if r.status == 404 && !missing_route(&r) {
            return Ok(None);
        }
        let j = envelope(&r)?;
        match j.get("user") {
            Some(user) if !user.is_null() => Ok(Some(serde_json::from_value(user.clone())?)),
            _ => Ok(None),
        }
    }

    /// `GET /users/permissions`: every user ([`UserAccess`]; emails as the caller may see them).
    pub async fn get_users_access(&self) -> ConnectorResult<Vec<UserAccess>> {
        let r = self
            .get(
                &format!("{}/permissions", routes::USERS),
                RequestOptions::default(),
            )
            .await?;
        let j = envelope(&r)?;
        Ok(match j.get("users") {
            Some(users) => serde_json::from_value(users.clone())?,
            None => Vec::new(),
        })
    }

    /// `PATCH /users/{name}/permissions` with `body` (`role`, `permissions`, `primary_team`,
    /// `attributes`).
    pub async fn patch_user_access(&self, username: &str, body: &Json) -> SuccessTuple {
        let r = self
            .patch(
                &format!("{}/{}/permissions", routes::USERS, segment(username)),
                RequestOptions {
                    body: Body::Json(body.to_string()),
                    ..Default::default()
                },
            )
            .await;
        change_tuple(r, &format!("edit user '{username}'"))
    }

    /// Replace a user's attributes: `PATCH /users/{name}/permissions` with `attributes` (the
    /// server keeps the attributes that hold rights unless the caller is an administrator).
    pub async fn set_user_attributes(&self, user: &User, attributes: &Map) -> SuccessTuple {
        self.patch_user_access(
            &user.username,
            &json!({"attributes": json_of(&Value::Map(attributes.clone()))}),
        )
        .await
    }

    /// The user's teams with their roles (Everyone first; only Everyone for an unknown user).
    pub async fn get_user_teams(&self, username: &str) -> ConnectorResult<Vec<(String, TeamRole)>> {
        let mut teams = match self.get_user_access(username).await? {
            Some(user) => user.team_roles(),
            None => Vec::new(),
        };
        if !teams
            .iter()
            .any(|(t, _)| meerschaum_core::access::is_everyone(Some(t)))
        {
            teams.insert(
                0,
                (
                    meerschaum_core::access::EVERYONE.to_string(),
                    TeamRole::Member,
                ),
            );
        }
        Ok(teams)
    }

    // ----------------------------------------------------------------------------------------
    // A pipe's access and locks
    // ----------------------------------------------------------------------------------------

    /// The route of a pipe's access and locks (`/pipes/{keys}/{what}`, or
    /// `/pipelines/{name}/{what}` for a saved Pipeline).
    fn access_r_url(pipe: &Pipe, what: &str) -> String {
        format!("{}/{what}", Self::metadata_r_url(pipe))
    }

    fn access_query(&self, pipe: &Pipe) -> Vec<(String, QueryValue)> {
        vec![(
            "instance_keys".into(),
            QueryValue::opt(self.get_pipe_instance_keys(pipe)),
        )]
    }

    /// `GET /pipes/{keys}/access`: the pipe's access, locks and what the caller may do.
    pub async fn get_pipe_access(&self, pipe: &Pipe) -> ConnectorResult<PipeAccessInfo> {
        let r = self
            .get(
                &Self::access_r_url(pipe, "access"),
                RequestOptions {
                    query: self.access_query(pipe),
                    ..Default::default()
                },
            )
            .await?;
        let j = envelope(&r)?;
        let core = crate::pipe_ops::json_to_core(&j);
        Ok(PipeAccessInfo {
            access: PipeAccess::from_value(core.get("access")),
            locks: Locks::from_value(core.get("locks")),
            may: core.get("may").map(Value::to_map).unwrap_or_default(),
        })
    }

    async fn pipe_access_change(
        &self,
        pipe: &Pipe,
        what: &str,
        body: Json,
        verb: &str,
    ) -> SuccessTuple {
        let path = Self::access_r_url(pipe, what);
        let opts = RequestOptions {
            query: self.access_query(pipe),
            body: Body::Json(body.to_string()),
            ..Default::default()
        };
        let r = if what == "access" {
            self.patch(&path, opts).await
        } else {
            self.post(&path, opts).await
        };
        // The pipe's parameters changed on the server: read them again next time.
        self.invalidate_pipe_cache(pipe);
        change_tuple(r, &format!("{verb} {pipe}"))
    }

    /// `PATCH /pipes/{keys}/access`: store the pipe's access (its owner, its team's managers and
    /// administrators may; a `parameters` lock refuses it).
    pub async fn set_pipe_access(&self, pipe: &Pipe, access: &PipeAccess) -> SuccessTuple {
        self.pipe_access_change(
            pipe,
            "access",
            json_of(&access.to_value()),
            "set the access of",
        )
        .await
    }

    /// `POST /pipes/{keys}/lock`: add `locks` (its owner, its team's managers and administrators
    /// may).
    pub async fn lock_pipe(&self, pipe: &Pipe, locks: &Locks) -> SuccessTuple {
        self.pipe_access_change(pipe, "lock", json!({"locks": locks.names()}), "lock")
            .await
    }

    /// `POST /pipes/{keys}/unlock`: remove `locks` (removing `all` removes every lock).
    pub async fn unlock_pipe(&self, pipe: &Pipe, locks: &Locks) -> SuccessTuple {
        self.pipe_access_change(pipe, "unlock", json!({"locks": locks.names()}), "unlock")
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(status: u16, body: &str) -> ApiResponse {
        ApiResponse {
            status,
            content_type: "application/json".into(),
            bytes: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn an_older_server_is_told_apart_from_a_missing_team() {
        assert!(missing_route(&response(404, r#"{"detail": "Not Found"}"#)));
        assert!(missing_route(&response(405, "")));
        assert!(!missing_route(&response(
            404,
            r#"{"success": false, "message": "Team 'x' does not exist."}"#
        )));
        let old = change_tuple(Ok(response(404, r#"{"detail": "Not Found"}"#)), "lock it");
        assert!(!old.success && old.message.contains("older than 5.0.0rc15"));
        let refused = change_tuple(
            Ok(response(
                403,
                r#"{"success": false, "message": "Only administrators may create teams."}"#,
            )),
            "x",
        );
        assert_eq!(
            (refused.success, refused.message.as_str()),
            (false, "Only administrators may create teams.")
        );
        let done = change_tuple(
            Ok(response(
                200,
                r#"{"success": true, "message": "Locked it."}"#,
            )),
            "x",
        );
        assert!(done.success && done.message == "Locked it.");
    }

    #[test]
    fn user_rows_parse() {
        let row: UserAccess = serde_json::from_str(
            r#"{"username": "a", "user_id": "1", "user_type": null, "role": "editor",
                "permissions": ["pipes:read"], "explicit": false,
                "teams": [{"name": "Everyone", "role": "member"}, {"name": "data", "role": "manager"}],
                "primary_team": "data", "email": null}"#,
        )
        .unwrap();
        assert_eq!(row.role(), Role::Editor);
        assert_eq!(
            row.team_roles(),
            vec![
                ("Everyone".to_string(), TeamRole::Member),
                ("data".to_string(), TeamRole::Manager)
            ]
        );
        assert!(row.email.is_none());
    }
}
