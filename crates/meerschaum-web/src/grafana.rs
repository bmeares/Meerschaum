//! Grafana for teams (5.1): one Grafana organization per Meerschaum team, and a reverse proxy
//! that signs the console's users in to Grafana with their Meerschaum session.
//!
//! - **Organizations.** Grafana (OSS) keeps data sources apart per organization only, so each
//!   team is an organization named after it ([`GrafanaClient::ensure_org`]). A team with a
//!   database of its own gets a data source there that reads it with the team's read-only role
//!   ([`GrafanaClient::ensure_datasource`]).
//! - **Sign-in.** `/grafana/*` ([`routes`]) forwards to Grafana with the session's username in
//!   the auth-proxy header (`X-WEBAUTH-USER`). Grafana must trust only this server for that
//!   header (`GF_AUTH_PROXY_WHITELIST`) and be served under the same sub-path
//!   (`GF_SERVER_SERVE_FROM_SUB_PATH`). Before forwarding, the user's organizations are made to
//!   match their teams ([`GrafanaClient::sync_user`], at most once a minute per user): members
//!   are editors, managers are organization administrators, Meerschaum administrators are
//!   Grafana administrators, and the members of isolated teams are taken out of the main
//!   organization. When that cannot be done the request is refused: a user is never forwarded
//!   with memberships that may be stale.
//!
//! The configuration is `api:grafana`: `enabled`, `url` (how this server reaches Grafana),
//! `admin_user` and `admin_password` (Grafana's server administrator, for its HTTP API).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::any;
use meerschaum_core::access::{self, Team, TeamGrants, TeamRole};
use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};

use crate::WebState;

/// The path Grafana is served under.
pub const PATH: &str = "/grafana";

/// The header carrying the signed-in username to Grafana (its `auth.proxy` `header_name`).
pub const USER_HEADER: &str = "x-webauth-user";

/// Grafana's main organization (anonymous dashboards, users without a team).
pub const MAIN_ORG_ID: i64 = 1;

/// How long a user's synchronized memberships are trusted.
const SYNC_TTL: Duration = Duration::from_secs(60);

/// `api:grafana`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrafanaSettings {
    /// How this server reaches Grafana (`http://grafana:3000`), without a trailing slash.
    pub url: String,
    /// Grafana's server administrator.
    pub admin_user: String,
    pub admin_password: String,
    /// `database_url`: how Grafana reaches the teams' database server (`host:port`), when it
    /// differs from the host and port of the connector the teams branch from.
    pub database_url: Option<String>,
}

impl GrafanaSettings {
    /// The settings of an enabled integration (`None` when `enabled` is not true or there is no
    /// `url`).
    pub fn from_section(section: &Map) -> Option<GrafanaSettings> {
        if !section.get("enabled").is_some_and(Value::truthy) {
            return None;
        }
        let text = |key: &str| {
            section
                .get(key)
                .filter(|v| !v.is_null())
                .map(Value::py_str)
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
        };
        let url = text("url")?.trim_end_matches('/').to_string();
        Some(GrafanaSettings {
            url,
            admin_user: text("admin_user").unwrap_or_else(|| "admin".into()),
            admin_password: text("admin_password").unwrap_or_else(|| "admin".into()),
            database_url: text("database_url"),
        })
    }
}

/// A PostgreSQL data source of a team's organization.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Datasource {
    pub name: String,
    /// `host:port`, as Grafana reaches the database.
    pub url: String,
    pub database: String,
    pub user: String,
    pub password: String,
    pub timescaledb: bool,
}

/// Who a user is to Grafana.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrafanaUser {
    pub username: String,
    /// A Meerschaum administrator (a Grafana server administrator).
    pub is_admin: bool,
    /// Walled off: not in the main organization.
    pub isolated: bool,
    /// Their teams other than Everyone, with whether they manage them.
    pub teams: Vec<(String, bool)>,
    /// The organization they land in (their primary team's).
    pub primary: Option<String>,
}

impl GrafanaUser {
    /// A user in `user_teams`, given the instance's team `records`.
    pub fn new(
        username: &str,
        is_admin: bool,
        user_teams: &[(String, TeamRole)],
        records: &[Team],
        primary: &str,
    ) -> GrafanaUser {
        let grants = TeamGrants::of(user_teams, records, Some(primary));
        let mut teams: Vec<(String, bool)> = user_teams
            .iter()
            .filter(|(name, _)| !access::is_everyone(Some(name)))
            .map(|(name, role)| (name.clone(), *role == TeamRole::Manager))
            .collect();
        teams.sort();
        GrafanaUser {
            username: username.to_string(),
            is_admin,
            isolated: !is_admin && grants.isolated,
            teams,
            primary: (!access::is_everyone(Some(primary))).then(|| primary.to_string()),
        }
    }
}

/// Make the organization of `team` (and, with `database`, its data source reading the team's
/// database with the read-only role). `(organization id, created)`.
pub async fn ensure_team(
    settings: &GrafanaSettings,
    team: &str,
    datasource: Option<&Datasource>,
) -> Result<(i64, bool), String> {
    let client = GrafanaClient::new(settings.clone())?;
    let (org, created) = client.ensure_org(team).await?;
    if let Some(source) = datasource {
        client.ensure_datasource(org, source).await?;
    }
    Ok((org, created))
}

/// What a synchronization changed (for tests and logs).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub created_user: bool,
    pub created_orgs: Vec<String>,
    pub joined: Vec<String>,
    pub left: Vec<String>,
}

/// Grafana's HTTP API, as its server administrator.
#[derive(Debug, Clone)]
pub struct GrafanaClient {
    settings: GrafanaSettings,
    http: reqwest::Client,
}

fn failed(what: &str, e: impl std::fmt::Display) -> String {
    format!("Grafana: {what} failed: {e}")
}

impl GrafanaClient {
    /// A client for `settings`.
    pub fn new(settings: GrafanaSettings) -> Result<GrafanaClient, String> {
        let http = reqwest::Client::builder()
            .http1_only()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| failed("building the client", e))?;
        Ok(GrafanaClient { settings, http })
    }

    /// The settings.
    pub fn settings(&self) -> &GrafanaSettings {
        &self.settings
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        org: Option<i64>,
        body: Option<Json>,
    ) -> Result<(u16, Json), String> {
        let mut req = self
            .http
            .request(method, format!("{}{path}", self.settings.url))
            .basic_auth(
                &self.settings.admin_user,
                Some(&self.settings.admin_password),
            )
            .timeout(Duration::from_secs(15));
        if let Some(org) = org {
            req = req.header("X-Grafana-Org-Id", org.to_string());
        }
        if let Some(body) = body {
            req = req.json(&body);
        }
        let resp = req.send().await.map_err(|e| failed(path, e))?;
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        Ok((
            status,
            serde_json::from_str(&text).unwrap_or(Json::String(text)),
        ))
    }

    fn check(what: &str, (status, body): (u16, Json)) -> Result<Json, String> {
        if (200..300).contains(&status) {
            return Ok(body);
        }
        let message = body["message"]
            .as_str()
            .map(str::to_string)
            .unwrap_or_else(|| body.to_string());
        Err(failed(what, format!("{status}: {message}")))
    }

    fn encoded(text: &str) -> String {
        percent_encoding::utf8_percent_encode(text, percent_encoding::NON_ALPHANUMERIC).to_string()
    }

    /// The id of the organization named `name`, if it exists.
    pub async fn org_id(&self, name: &str) -> Result<Option<i64>, String> {
        let res = self
            .call(
                reqwest::Method::GET,
                &format!("/api/orgs/name/{}", Self::encoded(name)),
                None,
                None,
            )
            .await?;
        if res.0 == 404 {
            return Ok(None);
        }
        Ok(Self::check("reading an organization", res)?["id"].as_i64())
    }

    /// The organization named `name`, created when missing: `(id, created)`.
    pub async fn ensure_org(&self, name: &str) -> Result<(i64, bool), String> {
        if let Some(id) = self.org_id(name).await? {
            return Ok((id, false));
        }
        let res = self
            .call(
                reqwest::Method::POST,
                "/api/orgs",
                None,
                Some(json!({"name": name})),
            )
            .await?;
        // Another request made it in between.
        if res.0 == 409 {
            return self
                .org_id(name)
                .await?
                .map(|id| (id, false))
                .ok_or_else(|| failed("creating an organization", "it exists but was not found"));
        }
        Self::check("creating an organization", res)?["orgId"]
            .as_i64()
            .map(|id| (id, true))
            .ok_or_else(|| failed("creating an organization", "no id was returned"))
    }

    /// Rename the organization `old` to `new` (nothing when `old` does not exist).
    pub async fn rename_org(&self, old: &str, new: &str) -> Result<bool, String> {
        let Some(id) = self.org_id(old).await? else {
            return Ok(false);
        };
        let res = self
            .call(
                reqwest::Method::PUT,
                &format!("/api/orgs/{id}"),
                None,
                Some(json!({"name": new})),
            )
            .await?;
        Self::check("renaming an organization", res).map(|_| true)
    }

    /// The data source `source` in organization `org`, created or updated.
    pub async fn ensure_datasource(&self, org: i64, source: &Datasource) -> Result<(), String> {
        let body = json!({
            "name": source.name,
            "type": "postgres",
            "access": "proxy",
            "url": source.url,
            "user": source.user,
            "isDefault": true,
            "jsonData": {
                "database": source.database,
                "sslmode": "disable",
                "timescaledb": source.timescaledb,
            },
            "secureJsonData": {"password": source.password},
        });
        let existing = self
            .call(
                reqwest::Method::GET,
                &format!("/api/datasources/name/{}", Self::encoded(&source.name)),
                Some(org),
                None,
            )
            .await?;
        let res = match existing.1["uid"].as_str().filter(|_| existing.0 == 200) {
            Some(uid) => {
                self.call(
                    reqwest::Method::PUT,
                    &format!("/api/datasources/uid/{uid}"),
                    Some(org),
                    Some(body),
                )
                .await?
            }
            None => {
                self.call(
                    reqwest::Method::POST,
                    "/api/datasources",
                    Some(org),
                    Some(body),
                )
                .await?
            }
        };
        Self::check("saving a data source", res).map(|_| ())
    }

    async fn user_id(&self, username: &str) -> Result<Option<i64>, String> {
        let res = self
            .call(
                reqwest::Method::GET,
                &format!("/api/users/lookup?loginOrEmail={}", Self::encoded(username)),
                None,
                None,
            )
            .await?;
        if res.0 == 404 {
            return Ok(None);
        }
        Ok(Self::check("looking a user up", res)?["id"].as_i64())
    }

    async fn ensure_user(&self, username: &str) -> Result<(i64, bool), String> {
        if let Some(id) = self.user_id(username).await? {
            return Ok((id, false));
        }
        // The password is never used (the auth proxy signs the user in) and never kept.
        let password = format!(
            "{}{}",
            uuid::Uuid::new_v4().simple(),
            uuid::Uuid::new_v4().simple()
        );
        let res = self
            .call(
                reqwest::Method::POST,
                "/api/admin/users",
                None,
                Some(json!({"login": username, "name": username, "password": password})),
            )
            .await?;
        Self::check("creating a user", res)?["id"]
            .as_i64()
            .map(|id| (id, true))
            .ok_or_else(|| failed("creating a user", "no id was returned"))
    }

    /// Make `user`'s organizations match their teams. `team_names` is every team of the
    /// instance: an organization named after one is managed here (the user leaves those of the
    /// teams they are not in); other organizations are left alone.
    pub async fn sync_user(
        &self,
        user: &GrafanaUser,
        team_names: &[String],
    ) -> Result<SyncReport, String> {
        let mut report = SyncReport::default();
        let (id, created) = self.ensure_user(&user.username).await?;
        report.created_user = created;
        let res = self
            .call(
                reqwest::Method::PUT,
                &format!("/api/admin/users/{id}/permissions"),
                None,
                Some(json!({"isGrafanaAdmin": user.is_admin})),
            )
            .await?;
        Self::check("setting a user's permissions", res)?;

        let current = Self::check(
            "reading a user's organizations",
            self.call(
                reqwest::Method::GET,
                &format!("/api/users/{id}/orgs"),
                None,
                None,
            )
            .await?,
        )?;
        let current: Vec<(i64, String, String)> = current
            .as_array()
            .map(|orgs| {
                orgs.iter()
                    .filter_map(|org| {
                        Some((
                            org["orgId"].as_i64()?,
                            org["name"].as_str()?.to_string(),
                            org["role"].as_str().unwrap_or("").to_string(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();

        let mut landing = None;
        for (team, manages) in &user.teams {
            let (org, made) = self.ensure_org(team).await?;
            if made {
                report.created_orgs.push(team.clone());
            }
            let role = if *manages || user.is_admin {
                "Admin"
            } else {
                "Editor"
            };
            match current.iter().find(|(org_id, _, _)| *org_id == org) {
                Some((_, _, existing)) if existing == role => {}
                Some(_) => {
                    let res = self
                        .call(
                            reqwest::Method::PATCH,
                            &format!("/api/orgs/{org}/users/{id}"),
                            None,
                            Some(json!({"role": role})),
                        )
                        .await?;
                    Self::check("changing a role", res)?;
                }
                None => {
                    let res = self
                        .call(
                            reqwest::Method::POST,
                            &format!("/api/orgs/{org}/users"),
                            None,
                            Some(json!({"loginOrEmail": user.username, "role": role})),
                        )
                        .await?;
                    // Already a member (a concurrent request): fine.
                    if res.0 != 409 {
                        Self::check("adding a user to an organization", res)?;
                    }
                    report.joined.push(team.clone());
                }
            }
            if landing.is_none() || user.primary.as_deref() == Some(team.as_str()) {
                landing = Some(org);
            }
        }

        // Leave the organizations of the teams the user is not in, and, when walled off, the
        // main organization. A user always keeps one organization (Grafana requires it).
        let in_team = |name: &str| {
            user.teams
                .iter()
                .any(|(team, _)| access::same_team(Some(team), Some(name)))
        };
        let managed = |name: &str| {
            team_names
                .iter()
                .any(|team| access::same_team(Some(team), Some(name)))
        };
        for (org, name, _) in &current {
            let leave = if *org == MAIN_ORG_ID {
                user.isolated && !user.teams.is_empty()
            } else {
                managed(name) && !in_team(name)
            };
            if !leave {
                continue;
            }
            let res = self
                .call(
                    reqwest::Method::DELETE,
                    &format!("/api/orgs/{org}/users/{id}"),
                    None,
                    None,
                )
                .await?;
            Self::check("removing a user from an organization", res)?;
            report.left.push(name.clone());
        }

        if let Some(org) = landing {
            let landed = current
                .iter()
                .any(|(org_id, _, _)| *org_id == org && report.left.is_empty() && !created);
            if !landed || !report.joined.is_empty() {
                let res = self
                    .call(
                        reqwest::Method::POST,
                        &format!("/api/users/{id}/using/{org}"),
                        None,
                        None,
                    )
                    .await?;
                Self::check("choosing a user's organization", res)?;
            }
        }
        Ok(report)
    }
}

// ---------------------------------------------------------------------------------------------
// The reverse proxy
// ---------------------------------------------------------------------------------------------

/// The proxy's state: the client and when each user was last synchronized.
pub(crate) struct GrafanaProxy {
    client: GrafanaClient,
    synced: Mutex<HashMap<String, (Instant, GrafanaUser)>>,
}

impl GrafanaProxy {
    pub(crate) fn from_state(server: &meerschaum_server::AppState) -> Option<Arc<GrafanaProxy>> {
        let settings = GrafanaSettings::from_section(&server.config.grafana)?;
        let client = GrafanaClient::new(settings).ok()?;
        Some(Arc::new(GrafanaProxy {
            client,
            synced: Mutex::new(HashMap::new()),
        }))
    }

    fn is_synced(&self, user: &GrafanaUser) -> bool {
        self.synced
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&user.username)
            .is_some_and(|(at, last)| at.elapsed() < SYNC_TTL && last == user)
    }

    fn mark_synced(&self, user: GrafanaUser) {
        let mut synced = self.synced.lock().unwrap_or_else(|e| e.into_inner());
        if synced.len() > 4096 {
            synced.retain(|_, (at, _)| at.elapsed() < SYNC_TTL);
        }
        synced.insert(user.username.clone(), (Instant::now(), user));
    }
}

/// Register `/grafana` and `/grafana/*` when `api:grafana` is enabled.
pub(crate) fn routes(router: Router<WebState>, state: &WebState) -> Router<WebState> {
    if state.grafana.is_none() {
        return router;
    }
    router
        .route(PATH, any(forward))
        .route(&format!("{PATH}/"), any(forward))
        .route(&format!("{PATH}/{{*rest}}"), any(forward))
}

fn unavailable(message: &str) -> Response {
    (StatusCode::BAD_GATEWAY, message.to_string()).into_response()
}

/// The user of the request's console session, as Grafana should see them.
async fn grafana_user(
    state: &WebState,
    headers: &HeaderMap,
) -> Result<GrafanaUserTeams, Box<Response>> {
    let to_login = || Box::new(Redirect::to("/dash/login").into_response());
    let data = match crate::session(state, headers).await {
        Ok(data) => data,
        Err(StatusCode::SERVICE_UNAVAILABLE) => {
            return Err(Box::new(StatusCode::SERVICE_UNAVAILABLE.into_response()));
        }
        Err(_) => return Err(to_login()),
    };
    let username = match crate::bound_username(&state.server, &data).await {
        Ok(Some(username)) => username,
        Ok(None) => return Err(to_login()),
        Err(e) => return Err(Box::new(e.into_response())),
    };
    let conn = state.server.instance();
    let busy = |_| Box::new(StatusCode::SERVICE_UNAVAILABLE.into_response());
    let mut user = meerschaum_core::users::User::new(&username, None);
    user.instance_keys = Some(conn.connector_keys());
    let user_type = conn.get_user_type(&user).await.map_err(busy)?;
    let attributes = conn.get_user_attributes(&user).await.map_err(busy)?;
    let records = conn.get_teams().await.map_err(busy)?;
    let user_teams: Vec<(String, TeamRole)> = records
        .iter()
        .filter_map(|team| {
            team.role_of(&username)
                .map(|role| (team.name.clone(), role))
        })
        .collect();
    let primary = access::primary_team(attributes.as_ref(), &user_teams);
    let is_admin = state.server.config.no_auth || access::is_admin_type(user_type.as_deref());
    Ok(GrafanaUserTeams {
        user: GrafanaUser::new(&username, is_admin, &user_teams, &records, &primary),
        team_names: records
            .iter()
            .filter(|team| !team.is_everyone())
            .map(|team| team.name.clone())
            .collect(),
    })
}

struct GrafanaUserTeams {
    user: GrafanaUser,
    team_names: Vec<String>,
}

/// Headers never forwarded to Grafana: hop-by-hop ones, and anything a browser could use to
/// claim an identity (the auth-proxy headers) or Grafana's own credentials.
fn forwarded_headers(headers: &HeaderMap, upgrade: bool) -> HeaderMap {
    const DROPPED: &[&str] = &[
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
        "host",
        "content-length",
        "authorization",
        "x-grafana-org-id",
    ];
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        let lower = name.as_str().to_ascii_lowercase();
        if DROPPED.contains(&lower.as_str()) || lower.starts_with("x-webauth-") {
            continue;
        }
        if lower == "cookie" {
            // The console's session stays here.
            let kept: Vec<&str> = value
                .to_str()
                .unwrap_or("")
                .split(';')
                .map(str::trim)
                .filter(|item| {
                    !item.is_empty()
                        && item.split_once('=').map(|(k, _)| k) != Some(crate::sessions::COOKIE)
                })
                .collect();
            if let Ok(value) = HeaderValue::from_str(&kept.join("; "))
                && !kept.is_empty()
            {
                out.insert(header::COOKIE, value);
            }
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    if upgrade {
        out.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
        if let Some(value) = headers.get(header::UPGRADE) {
            out.insert(header::UPGRADE, value.clone());
        }
    }
    out
}

fn response_headers(headers: &reqwest::header::HeaderMap, upgrade: bool) -> HeaderMap {
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        let lower = name.as_str().to_ascii_lowercase();
        let hop = matches!(
            lower.as_str(),
            "connection" | "keep-alive" | "transfer-encoding" | "upgrade" | "content-length"
        );
        if hop && !(upgrade && matches!(lower.as_str(), "connection" | "upgrade")) {
            continue;
        }
        if let (Ok(name), Ok(value)) = (
            HeaderName::from_bytes(name.as_str().as_bytes()),
            HeaderValue::from_bytes(value.as_bytes()),
        ) {
            out.append(name, value);
        }
    }
    out
}

async fn forward(State(state): State<WebState>, mut req: Request) -> Response {
    let Some(proxy) = state.grafana.clone() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let who = match grafana_user(&state, req.headers()).await {
        Ok(who) => who,
        Err(resp) => return *resp,
    };
    if !proxy.is_synced(&who.user) {
        if let Err(e) = proxy.client.sync_user(&who.user, &who.team_names).await {
            meerschaum_core::dprint!("{e}");
            return unavailable("Grafana is not available right now. Try again in a moment.");
        }
        proxy.mark_synced(who.user.clone());
    }

    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| PATH.to_string());
    let is_upgrade = req.headers().contains_key(header::UPGRADE);
    let mut headers = forwarded_headers(req.headers(), is_upgrade);
    let Ok(username) = HeaderValue::from_str(&who.user.username) else {
        return unavailable("This username cannot be sent to Grafana.");
    };
    headers.insert(HeaderName::from_static(USER_HEADER), username);
    let client_upgrade = is_upgrade.then(|| hyper::upgrade::on(&mut req));
    let method = req.method().clone();
    let has_body = !matches!(
        method,
        axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
    );
    let mut upstream = proxy
        .client
        .http
        .request(
            method,
            format!("{}{path_and_query}", proxy.client.settings.url),
        )
        .headers(reqwest_headers(&headers));
    if has_body {
        upstream = upstream.body(reqwest::Body::wrap_stream(
            req.into_body().into_data_stream(),
        ));
    }
    let resp = match upstream.send().await {
        Ok(resp) => resp,
        Err(_) => return unavailable("Grafana is not reachable."),
    };
    let status =
        StatusCode::from_u16(resp.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
    let switching = status == StatusCode::SWITCHING_PROTOCOLS;
    let out_headers = response_headers(resp.headers(), switching);
    if switching {
        let Some(client_upgrade) = client_upgrade else {
            return unavailable("Grafana answered an upgrade nobody asked for.");
        };
        tokio::spawn(async move {
            let (Ok(client), Ok(mut server)) = (client_upgrade.await, resp.upgrade().await) else {
                return;
            };
            let mut client = hyper_util::rt::TokioIo::new(client);
            let _ = tokio::io::copy_bidirectional(&mut client, &mut server).await;
        });
        let mut out = Response::new(Body::empty());
        *out.status_mut() = status;
        *out.headers_mut() = out_headers;
        return out;
    }
    let mut out = Response::new(Body::from_stream(resp.bytes_stream()));
    *out.status_mut() = status;
    *out.headers_mut() = out_headers;
    out
}

fn reqwest_headers(headers: &HeaderMap) -> reqwest::header::HeaderMap {
    let mut out = reqwest::header::HeaderMap::new();
    for (name, value) in headers {
        if let (Ok(name), Ok(value)) = (
            reqwest::header::HeaderName::from_bytes(name.as_str().as_bytes()),
            reqwest::header::HeaderValue::from_bytes(value.as_bytes()),
        ) {
            out.append(name, value);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn section(json: &str) -> Map {
        Value::from_json_str(json).unwrap().to_map()
    }

    #[test]
    fn settings_need_enabled_and_a_url() {
        assert_eq!(GrafanaSettings::from_section(&Map::new()), None);
        assert_eq!(
            GrafanaSettings::from_section(&section(r#"{"url": "http://grafana:3000"}"#)),
            None
        );
        assert_eq!(
            GrafanaSettings::from_section(&section(r#"{"enabled": true}"#)),
            None
        );
        let settings = GrafanaSettings::from_section(&section(
            r#"{"enabled": true, "url": "http://grafana:3000/", "admin_password": "s3"}"#,
        ))
        .unwrap();
        assert_eq!(settings.url, "http://grafana:3000");
        assert_eq!(settings.admin_user, "admin");
        assert_eq!(settings.admin_password, "s3");
    }

    #[test]
    fn identity_headers_from_the_browser_are_never_forwarded() {
        let mut headers = HeaderMap::new();
        headers.insert("x-webauth-user", HeaderValue::from_static("root"));
        headers.insert("X-WEBAUTH-ROLE", HeaderValue::from_static("Admin"));
        headers.insert("authorization", HeaderValue::from_static("Basic YWRtaW4="));
        headers.insert("x-grafana-org-id", HeaderValue::from_static("1"));
        headers.insert("accept", HeaderValue::from_static("text/html"));
        headers.insert(
            "cookie",
            HeaderValue::from_static(
                "mrsm-session-id=7b0c3a52-5a5b-4d5e-9d0c-2f3f1f1f1f1f; grafana_session=abc; theme=dark",
            ),
        );
        let out = forwarded_headers(&headers, false);
        assert!(out.get("x-webauth-user").is_none());
        assert!(out.get("x-webauth-role").is_none());
        assert!(out.get("authorization").is_none());
        assert!(out.get("x-grafana-org-id").is_none());
        assert_eq!(out.get("accept").unwrap(), "text/html");
        assert_eq!(
            out.get("cookie").unwrap(),
            "grafana_session=abc; theme=dark"
        );
        headers.insert(
            "cookie",
            HeaderValue::from_static("mrsm-session-id=7b0c3a52-5a5b-4d5e-9d0c-2f3f1f1f1f1f"),
        );
        assert!(forwarded_headers(&headers, false).get("cookie").is_none());
    }

    #[test]
    fn upgrades_keep_their_headers() {
        let mut headers = HeaderMap::new();
        headers.insert("upgrade", HeaderValue::from_static("websocket"));
        headers.insert("connection", HeaderValue::from_static("Upgrade"));
        headers.insert("sec-websocket-key", HeaderValue::from_static("k"));
        let out = forwarded_headers(&headers, true);
        assert_eq!(out.get("upgrade").unwrap(), "websocket");
        assert_eq!(out.get("connection").unwrap(), "upgrade");
        assert_eq!(out.get("sec-websocket-key").unwrap(), "k");
        assert!(forwarded_headers(&headers, false).get("upgrade").is_none());
    }

    #[test]
    fn grafana_users_follow_their_teams() {
        let mut wetlands = Team::new("Wetlands");
        wetlands
            .attributes
            .insert("isolated".into(), Value::Bool(true));
        let data = Team::new("Data");
        let records = vec![Team::everyone(), wetlands, data];
        let teams = vec![
            (access::EVERYONE.to_string(), TeamRole::Member),
            ("Wetlands".to_string(), TeamRole::Manager),
        ];
        let ann = GrafanaUser::new("ann", false, &teams, &records, "Wetlands");
        assert!(ann.isolated && !ann.is_admin);
        assert_eq!(ann.teams, vec![("Wetlands".to_string(), true)]);
        assert_eq!(ann.primary.as_deref(), Some("Wetlands"));
        // An administrator is never walled off; a user without teams lands in the main org.
        let root = GrafanaUser::new("root", true, &teams, &records, "Wetlands");
        assert!(!root.isolated && root.is_admin);
        let everyone = vec![(access::EVERYONE.to_string(), TeamRole::Member)];
        let bob = GrafanaUser::new("bob", false, &everyone, &records, access::EVERYONE);
        assert!(!bob.isolated && bob.teams.is_empty() && bob.primary.is_none());
    }
}
