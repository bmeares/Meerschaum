//! Grafana for teams (5.1): one Grafana organization per Meerschaum team, and a reverse proxy
//! that signs the console's users in to Grafana with their Meerschaum session.
//!
//! - **Organizations.** Grafana (OSS) keeps data sources apart per organization only, so each
//!   team gets an organization when an administrator sets the team up
//!   ([`GrafanaClient::ensure_team_org`]); its id is saved in the team's attributes
//!   (`grafana: {org_id}`), and from then on the team's organization is that id, never a name.
//!   A team with a database of its own gets a data source there that reads it with the team's
//!   read-only role ([`GrafanaClient::ensure_datasource`]). Renaming a team renames its
//!   organization ([`GrafanaClient::rename_org`]); deleting it retires the organization
//!   ([`GrafanaClient::retire_org`]: a tombstone name, no members but the server's administrator,
//!   no data sources; the dashboards stay).
//! - **Anonymous access.** Grafana's own anonymous policy (`auth.anonymous`, read from Grafana)
//!   is left as it is. A team's organization is never the anonymous organization nor the main
//!   one, and a team's data source is only ever made in its team's organization.
//! - **Sign-in.** `/grafana/*` (`routes`) forwards to Grafana with the session's username in
//!   the auth-proxy header (`X-WEBAUTH-USER`). Grafana must trust only this server for that
//!   header (`GF_AUTH_PROXY_WHITELIST`) and be served under the same sub-path
//!   (`GF_SERVER_SERVE_FROM_SUB_PATH`). Before forwarding, the user's organizations are made to
//!   match their teams ([`GrafanaClient::sync_user`], at most once a minute per user): members
//!   are editors, managers are organization administrators, Meerschaum administrators are
//!   Grafana administrators, and the members of isolated teams are taken out of the main
//!   organization. When that cannot be done the request is refused: a user is never forwarded
//!   with memberships that may be stale. A signed-out request is forwarded without any identity
//!   (Grafana's session cookies removed) when Grafana's anonymous access is on, else it is sent
//!   to the console's login.
//!
//! The configuration is `api:grafana`: `enabled`, `url` (how this server reaches Grafana),
//! `admin_user` and `admin_password` (Grafana's server administrator, for its HTTP API). Without
//! `admin_password`, a strong one is generated once and kept beside the API's secret key
//! ([`admin_password_path`]); Grafana's default `admin` password is never used.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::any;
use meerschaum_core::access::{self, TEAM_GRAFANA_ATTRIBUTE, Team, TeamGrants, TeamRole};
use meerschaum_core::{Map, Value};
use serde_json::{Value as Json, json};

use crate::WebState;

/// The path Grafana is served under.
pub const PATH: &str = "/grafana";

/// The header carrying the signed-in username to Grafana (its `auth.proxy` `header_name`).
pub const USER_HEADER: &str = "x-webauth-user";

/// Grafana's main organization (the stack's dashboards, users without a team).
pub const MAIN_ORG_ID: i64 = 1;

/// The file (in `API_CONFIG_RESOURCES_PATH`, beside `.api_secret_key`) that keeps the generated
/// password of Grafana's administrator when `api:grafana:admin_password` is not set.
pub const ADMIN_PASSWORD_FILENAME: &str = meerschaum_server::config::GRAFANA_PASSWORD_FILENAME;

/// Grafana's own default administrator password: never used, and replaced when found.
pub const DEFAULT_ADMIN_PASSWORD: &str = "admin";

/// `api:grafana:proxy_subnet` by default: the network between the stack's API server and its
/// Grafana (outside Docker's default address pools).
pub const DEFAULT_PROXY_SUBNET: &str = "10.213.71.0/29";

/// How a retired organization's name starts. Team names never contain a space, so a tombstone
/// can never be taken for a team's organization.
pub const TOMBSTONE_PREFIX: &str = "Deleted team ";

/// How long a user's synchronized memberships are trusted.
const SYNC_TTL: Duration = Duration::from_secs(60);

/// How long Grafana's anonymous policy is trusted.
const POLICY_TTL: Duration = Duration::from_secs(30);

/// How long rejected administrator credentials are not tried again (Grafana locks an account
/// out after five failed logins in five minutes).
const ADMIN_RETRY: Duration = Duration::from_secs(300);

/// Tell the server's administrator (stderr, where the server's other notices go, and `tracing`).
fn tell_administrator(warning: bool, message: &str) {
    if warning {
        tracing::warn!("{message}");
        eprintln!("Warning: {message}");
    } else {
        tracing::info!("{message}");
        eprintln!("Info: {message}");
    }
}

/// Where the generated administrator password is kept: `<API_CONFIG_RESOURCES_PATH>/`
/// [`ADMIN_PASSWORD_FILENAME`] (`api_config_dir` is `<root>/api`).
pub fn admin_password_path(api_config_dir: &Path) -> PathBuf {
    api_config_dir.join(ADMIN_PASSWORD_FILENAME)
}

/// The password in `path`, or a new one (32 random bytes as hex) written there, as the API's
/// secret key is (`generate_secret_key`). The file is readable by its owner only.
pub fn read_or_generate_admin_password(path: &Path) -> Result<String, String> {
    read_or_generate_secret(path, "Grafana's administrator password")
}

pub use meerschaum_core::config::grafana_readonly::{
    MAIN_DATASOURCE_NAME, READONLY_PASSWORD_FILENAME, read_or_generate_secret,
    readonly_password_path,
};

/// A setting of `api:grafana` as text: a string (trimmed), or a list joined with commas (as
/// Grafana reads lists such as `GF_AUTH_PROXY_WHITELIST`). `None` when unset or empty.
pub fn setting_text(section: &Map, key: &str) -> Option<String> {
    let text = match section.get(key)? {
        Value::Null => return None,
        Value::List(items) => items
            .iter()
            .filter(|v| !v.is_null())
            .map(|v| v.py_str().trim().to_string())
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(","),
        Value::Str(s) => s.trim().to_string(),
        other => other.py_str().trim().to_string(),
    };
    (!text.is_empty()).then_some(text)
}

/// Whether two organization names are the same to Grafana. Grafana's lookups by name follow
/// its database's collation (case-sensitive on SQLite and PostgreSQL, not on MySQL); they are
/// compared without case here, so a name that matches on any backend is refused.
pub fn same_org_name(a: &str, b: &str) -> bool {
    a.trim().to_lowercase() == b.trim().to_lowercase()
}

/// Whether `username` is (to Grafana, which compares logins without case) the login of the
/// account this server authenticates as.
pub fn is_admin_login(username: &str, admin_user: &str) -> bool {
    username.to_lowercase() == admin_user.to_lowercase()
}

/// The organization id saved in a team's attributes (`grafana: {org_id}`), if any.
pub fn team_org_id(team: &Team) -> Option<i64> {
    let record = team.attributes.get(TEAM_GRAFANA_ATTRIBUTE)?;
    match record.get("org_id")? {
        Value::Int(id) => Some(*id),
        Value::Str(text) => text.trim().parse().ok(),
        other => other.as_i64(),
    }
    .filter(|id| *id > 0)
}

/// The name a deleted team's organization is given (never a team's name: it has spaces).
pub fn tombstone_name(team: &str, org: i64) -> String {
    format!("{TOMBSTONE_PREFIX}{team} #{org}")
}

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
    /// The settings of an enabled integration (`Ok(None)` when `enabled` is not true). `url`
    /// defaults to the stack's Grafana (`http://grafana:3000`, which resolves inside the stack's
    /// network only), the administrator to `admin`. Without `admin_password`, the password in
    /// `password_file` is used (generated there the first time). Grafana's default password
    /// (`admin`) is refused.
    pub fn from_section(
        section: &Map,
        password_file: &Path,
    ) -> Result<Option<GrafanaSettings>, String> {
        if !section.get("enabled").is_some_and(Value::truthy) {
            return Ok(None);
        }
        // The stack's own Grafana by default.
        let url = setting_text(section, "url")
            .unwrap_or_else(|| "http://grafana:3000".into())
            .trim_end_matches('/')
            .to_string();
        let admin_password = match setting_text(section, "admin_password") {
            Some(password) if password == DEFAULT_ADMIN_PASSWORD => {
                return Err(
                    "api:grafana:admin_password is Grafana's default password ('admin'): set \
                     another one, or remove it to have Meerschaum generate one."
                        .to_string(),
                );
            }
            Some(password) => password,
            None => read_or_generate_admin_password(password_file)?,
        };
        Ok(Some(GrafanaSettings {
            url,
            admin_user: setting_text(section, "admin_user").unwrap_or_else(|| "admin".into()),
            admin_password,
            database_url: setting_text(section, "database_url"),
        }))
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

/// One of a user's teams, as Grafana sees it.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TeamOrg {
    pub name: String,
    /// The user manages the team.
    pub manages: bool,
    /// The team's saved organization id (`None`: the team has no organization).
    pub org: Option<i64>,
}

/// Who a user is to Grafana.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct GrafanaUser {
    pub username: String,
    /// A Meerschaum administrator (a Grafana server administrator).
    pub is_admin: bool,
    /// Walled off: not in the main organization.
    pub isolated: bool,
    /// Their teams other than Everyone.
    pub teams: Vec<TeamOrg>,
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
        let mut teams: Vec<TeamOrg> = user_teams
            .iter()
            .filter(|(name, _)| !access::is_everyone(Some(name)))
            .map(|(name, role)| TeamOrg {
                name: name.clone(),
                manages: *role == TeamRole::Manager,
                org: records
                    .iter()
                    .find(|team| access::same_team(Some(&team.name), Some(name)))
                    .and_then(team_org_id),
            })
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

/// The organizations recorded for the instance's teams: `(team, org id)`.
pub fn recorded_orgs(records: &[Team]) -> Vec<(String, i64)> {
    records
        .iter()
        .filter(|team| !team.is_everyone())
        .filter_map(|team| team_org_id(team).map(|org| (team.name.clone(), org)))
        .collect()
}

/// Make the organization of `team` (the administrator's setup: an organization of that name is
/// adopted unless `recorded` gives it to another team) and, with `database`, its data source
/// reading the team's database with the read-only role. `(organization id, created)`.
pub async fn ensure_team(
    settings: &GrafanaSettings,
    team: &str,
    datasource: Option<&Datasource>,
    recorded: &[(String, i64)],
) -> Result<(i64, bool), String> {
    let client = GrafanaClient::new(settings.clone())?;
    let (org, created) = client.ensure_team_org(team, recorded).await?;
    if let Some(source) = datasource {
        client.ensure_datasource(org, source).await?;
    }
    Ok((org, created))
}

/// What a synchronization changed (for tests and logs).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SyncReport {
    pub created_user: bool,
    pub joined: Vec<String>,
    pub left: Vec<String>,
    /// Teams whose organization was not used, with why.
    pub skipped: Vec<String>,
}

/// Grafana's anonymous access (`auth.anonymous`), as Grafana reports it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnonymousPolicy {
    pub enabled: bool,
    /// The anonymous organization's name (`org_name`).
    pub org_name: String,
    /// Its id, when it exists.
    pub org_id: Option<i64>,
    /// `org_role`.
    pub role: String,
    /// Grafana's session cookies (`auth.login_cookie_name` and its `_expiry`).
    pub cookie_names: Vec<String>,
}

impl AnonymousPolicy {
    /// Why `org` (`name`) cannot be a team's organization, if it cannot.
    fn refuses(&self, org: Option<i64>, name: Option<&str>) -> Option<String> {
        if org == Some(MAIN_ORG_ID) {
            return Some("it is Grafana's main organization".into());
        }
        if org.is_some() && org == self.org_id {
            return Some(format!(
                "it is Grafana's anonymous organization ('{}')",
                self.org_name
            ));
        }
        if name.is_some_and(|name| same_org_name(name, &self.org_name)) {
            return Some(format!(
                "Grafana's anonymous organization is named '{}'",
                self.org_name
            ));
        }
        None
    }
}

/// The cookies Grafana keeps its own sign-in in, whatever `login_cookie_name` says.
const GRAFANA_COOKIES: &[&str] = &[
    "grafana_session",
    "grafana_session_expiry",
    "grafana_remember",
    "grafana_user",
];

#[derive(Debug, Default)]
enum AdminCheck {
    #[default]
    Unknown,
    Ready,
    Failed(Instant, String),
}

#[derive(Debug, Default)]
struct ClientState {
    admin: AdminCheck,
    policy: Option<(Instant, AnonymousPolicy)>,
}

/// Grafana's HTTP API, as its server administrator.
#[derive(Debug, Clone)]
pub struct GrafanaClient {
    settings: GrafanaSettings,
    http: reqwest::Client,
    state: Arc<tokio::sync::Mutex<ClientState>>,
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
        Ok(GrafanaClient {
            settings,
            http,
            state: Arc::new(tokio::sync::Mutex::new(ClientState::default())),
        })
    }

    /// The settings.
    pub fn settings(&self) -> &GrafanaSettings {
        &self.settings
    }

    /// Whether `username` is the account this client authenticates as.
    pub fn is_server_account(&self, username: &str) -> bool {
        is_admin_login(username, &self.settings.admin_user)
    }

    async fn send(
        &self,
        method: reqwest::Method,
        path: &str,
        org: Option<i64>,
        body: Option<Json>,
        password: &str,
    ) -> Result<(u16, Json), String> {
        let mut req = self
            .http
            .request(method, format!("{}{path}", self.settings.url))
            .basic_auth(&self.settings.admin_user, Some(password))
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

    /// Whether `password` signs the administrator in (`Ok(false)`: rejected).
    async fn accepted(&self, password: &str) -> Result<bool, String> {
        let (status, body) = self
            .send(reqwest::Method::GET, "/api/user", None, None, password)
            .await?;
        match status {
            200 if body["isGrafanaAdmin"].as_bool() == Some(true) => Ok(true),
            200 => Err(format!(
                "Grafana: '{}' is not a Grafana server administrator.",
                self.settings.admin_user
            )),
            401 | 403 => Ok(false),
            other => Err(failed("signing the administrator in", other)),
        }
    }

    /// Make sure the configured administrator credentials work: when Grafana rejects them but
    /// still takes its default password, its administrator's password becomes the configured
    /// one (once). When neither works the integration is inactive (retried after five minutes).
    pub async fn ensure_admin(&self) -> Result<(), String> {
        let mut state = self.state.lock().await;
        match &state.admin {
            AdminCheck::Ready => return Ok(()),
            AdminCheck::Failed(at, e) if at.elapsed() < ADMIN_RETRY => return Err(e.clone()),
            _ => {}
        }
        let result = self.check_admin().await;
        if let Err(e) = &result {
            tell_administrator(true, e);
        }
        state.admin = match &result {
            Ok(()) => AdminCheck::Ready,
            Err(e) => AdminCheck::Failed(Instant::now(), e.clone()),
        };
        result
    }

    async fn check_admin(&self) -> Result<(), String> {
        let configured = self.settings.admin_password.clone();
        if configured == DEFAULT_ADMIN_PASSWORD {
            return Err("Grafana: the administrator password must not be 'admin'.".into());
        }
        if self.accepted(&configured).await? {
            return Ok(());
        }
        let rejected = || {
            format!(
                "Grafana rejects the administrator credentials of `api:grafana` (user '{}'), \
                 so the Grafana integration is inactive. Set `api:grafana:admin_password` to \
                 Grafana's administrator password, or reset Grafana's (`grafana cli admin \
                 reset-admin-password <password>`) to the one Meerschaum uses.",
                self.settings.admin_user
            )
        };
        if !self.accepted(DEFAULT_ADMIN_PASSWORD).await? {
            return Err(rejected());
        }
        // A Grafana initialized before Meerschaum owned its password: rotate it.
        let res = self
            .send(
                reqwest::Method::PUT,
                "/api/user/password",
                None,
                Some(json!({
                    "oldPassword": DEFAULT_ADMIN_PASSWORD,
                    "newPassword": configured,
                    "confirmNew": configured,
                })),
                DEFAULT_ADMIN_PASSWORD,
            )
            .await?;
        Self::check("changing the administrator's password", res)?;
        if !self.accepted(&configured).await? {
            return Err(rejected());
        }
        tell_administrator(
            false,
            &format!(
                "Grafana's administrator '{}' still had Grafana's default password: it was \
                 changed to the one Meerschaum uses (`api:grafana:admin_password`, or the \
                 generated one).",
                self.settings.admin_user
            ),
        );
        Ok(())
    }

    async fn call(
        &self,
        method: reqwest::Method,
        path: &str,
        org: Option<i64>,
        body: Option<Json>,
    ) -> Result<(u16, Json), String> {
        self.ensure_admin().await?;
        let res = self
            .send(method, path, org, body, &self.settings.admin_password)
            .await?;
        if res.0 == 401 {
            // The password changed under this client: check the credentials again (at most
            // once in [`ADMIN_RETRY`] when they keep failing).
            let mut state = self.state.lock().await;
            if matches!(state.admin, AdminCheck::Ready) {
                state.admin = AdminCheck::Unknown;
            }
        }
        Ok(res)
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

    /// Grafana's anonymous access (cached for a few seconds).
    pub async fn anonymous_policy(&self) -> Result<AnonymousPolicy, String> {
        if let Some((at, policy)) = &self.state.lock().await.policy
            && at.elapsed() < POLICY_TTL
        {
            return Ok(policy.clone());
        }
        let settings = Self::check(
            "reading Grafana's settings",
            self.call(reqwest::Method::GET, "/api/admin/settings", None, None)
                .await?,
        )?;
        let anonymous = &settings["auth.anonymous"];
        let text = |value: &Json| value.as_str().unwrap_or("").trim().to_string();
        let org_name = text(&anonymous["org_name"]);
        let org_id = if org_name.is_empty() {
            None
        } else {
            self.org_id(&org_name).await?
        };
        let mut cookie_names: Vec<String> = GRAFANA_COOKIES.iter().map(|c| c.to_string()).collect();
        let login_cookie = text(&settings["auth"]["login_cookie_name"]);
        if !login_cookie.is_empty() {
            cookie_names.push(format!("{login_cookie}_expiry"));
            cookie_names.push(login_cookie);
        }
        let policy = AnonymousPolicy {
            enabled: text(&anonymous["enabled"]).eq_ignore_ascii_case("true"),
            org_name,
            org_id,
            role: text(&anonymous["org_role"]),
            cookie_names,
        };
        self.state.lock().await.policy = Some((Instant::now(), policy.clone()));
        Ok(policy)
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

    /// The name of organization `id`, if it exists.
    pub async fn org_name(&self, id: i64) -> Result<Option<String>, String> {
        let res = self
            .call(reqwest::Method::GET, &format!("/api/orgs/{id}"), None, None)
            .await?;
        if res.0 == 404 {
            return Ok(None);
        }
        Ok(Self::check("reading an organization", res)?["name"]
            .as_str()
            .map(str::to_string))
    }

    /// The server's administrator in organization `org` as an Admin (what using it with
    /// `X-Grafana-Org-Id` needs, and what keeps it from being left without an Admin).
    async fn ensure_admin_member(&self, org: i64) -> Result<(), String> {
        let res = self
            .call(
                reqwest::Method::POST,
                &format!("/api/orgs/{org}/users"),
                None,
                Some(json!({"loginOrEmail": self.settings.admin_user, "role": "Admin"})),
            )
            .await?;
        if res.0 == 409 {
            return Ok(());
        }
        Self::check("adding the administrator to an organization", res).map(|_| ())
    }

    /// The organization of `team` for the administrator's setup: `(id, created)`. An existing
    /// organization of the team's name is adopted unless `recorded` (every team's saved
    /// organization) gives it to another team. Never the main or the anonymous organization.
    pub async fn ensure_team_org(
        &self,
        team: &str,
        recorded: &[(String, i64)],
    ) -> Result<(i64, bool), String> {
        let policy = self.anonymous_policy().await?;
        let refused = |why: String| {
            format!("Grafana: team '{team}' cannot have the organization it would get: {why}.")
        };
        if let Some(why) = policy.refuses(None, Some(team)) {
            return Err(refused(why));
        }
        if let Some((_, id)) = recorded
            .iter()
            .find(|(name, _)| access::same_team(Some(name), Some(team)))
            && let Some(name) = self.org_name(*id).await?
        {
            if let Some(why) = policy.refuses(Some(*id), Some(&name)) {
                return Err(refused(why));
            }
            return Ok((*id, false));
        }
        if let Some(id) = self.org_id(team).await? {
            if let Some((owner, _)) = recorded
                .iter()
                .find(|(name, org)| *org == id && !access::same_team(Some(name), Some(team)))
            {
                return Err(refused(format!(
                    "the organization '{team}' (id {id}) belongs to team '{owner}'"
                )));
            }
            if let Some(why) = policy.refuses(Some(id), Some(team)) {
                return Err(refused(why));
            }
            self.ensure_admin_member(id).await?;
            return Ok((id, false));
        }
        let res = self
            .call(
                reqwest::Method::POST,
                "/api/orgs",
                None,
                Some(json!({"name": team})),
            )
            .await?;
        let id = Self::check("creating an organization", res)?["orgId"]
            .as_i64()
            .ok_or_else(|| failed("creating an organization", "no id was returned"))?;
        if let Some(why) = policy.refuses(Some(id), Some(team)) {
            return Err(refused(why));
        }
        Ok((id, true))
    }

    /// Rename organization `id` to `new` (`Ok(false)`: it does not exist). Never the main or the
    /// anonymous organization, nor to the anonymous organization's name.
    pub async fn rename_org(&self, id: i64, new: &str) -> Result<bool, String> {
        let policy = self.anonymous_policy().await?;
        if let Some(why) = policy
            .refuses(Some(id), None)
            .or_else(|| policy.refuses(None, Some(new)))
        {
            return Err(format!(
                "Grafana: the organization {id} was not renamed to '{new}': {why}."
            ));
        }
        if self.org_name(id).await?.is_none() {
            return Ok(false);
        }
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

    /// Retire the organization `id` of the deleted team `team` (`Ok(false)`: it does not exist).
    /// Its dashboards stay; it is renamed to [`tombstone_name`], every member but the server's
    /// administrator leaves it, and its data sources (the team's credentials) are deleted.
    pub async fn retire_org(&self, team: &str, id: i64) -> Result<bool, String> {
        let policy = self.anonymous_policy().await?;
        if let Some(why) = policy.refuses(Some(id), None) {
            return Err(format!(
                "Grafana: the organization {id} of team '{team}' was left as it is: {why}."
            ));
        }
        if self.org_name(id).await?.is_none() {
            return Ok(false);
        }
        let res = self
            .call(
                reqwest::Method::PUT,
                &format!("/api/orgs/{id}"),
                None,
                Some(json!({"name": tombstone_name(team, id)})),
            )
            .await?;
        Self::check("renaming an organization", res)?;
        self.ensure_admin_member(id).await?;
        let users = Self::check(
            "reading an organization's users",
            self.call(
                reqwest::Method::GET,
                &format!("/api/orgs/{id}/users"),
                None,
                None,
            )
            .await?,
        )?;
        for user in users.as_array().into_iter().flatten() {
            let (Some(user_id), Some(login)) = (user["userId"].as_i64(), user["login"].as_str())
            else {
                continue;
            };
            if self.is_server_account(login) {
                continue;
            }
            let res = self
                .call(
                    reqwest::Method::DELETE,
                    &format!("/api/orgs/{id}/users/{user_id}"),
                    None,
                    None,
                )
                .await?;
            Self::check("removing a user from an organization", res)?;
        }
        let sources = Self::check(
            "reading an organization's data sources",
            self.call(reqwest::Method::GET, "/api/datasources", Some(id), None)
                .await?,
        )?;
        for source in sources.as_array().into_iter().flatten() {
            // `X-Grafana-Org-Id` is honored for members only; never delete another org's.
            if source["orgId"].as_i64() != Some(id) {
                return Err(failed(
                    "deleting a data source",
                    "Grafana answered for another organization",
                ));
            }
            let Some(uid) = source["uid"].as_str() else {
                continue;
            };
            let res = self
                .call(
                    reqwest::Method::DELETE,
                    &format!("/api/datasources/uid/{}", Self::encoded(uid)),
                    Some(id),
                    None,
                )
                .await?;
            Self::check("deleting a data source", res)?;
        }
        Ok(true)
    }

    /// The data source `source` in the team organization `org`, created or updated. Never in
    /// the main or the anonymous organization.
    pub async fn ensure_datasource(&self, org: i64, source: &Datasource) -> Result<(), String> {
        let policy = self.anonymous_policy().await?;
        if let Some(why) = policy.refuses(Some(org), None) {
            return Err(format!(
                "Grafana: the data source '{}' was not saved in organization {org}: {why}.",
                source.name
            ));
        }
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

    /// Make `user`'s organizations match their teams. A team's organization is its saved id
    /// only: a team without one, or whose organization is gone, the main or the anonymous one,
    /// is skipped (and logged); nothing is created or looked up by name here. `team_orgs` is
    /// every team's saved organization: the user leaves those of the teams they are not in (and
    /// retired ones); other organizations are left alone. The server's own account is never
    /// demoted nor taken out of an organization.
    pub async fn sync_user(
        &self,
        user: &GrafanaUser,
        team_orgs: &[i64],
    ) -> Result<SyncReport, String> {
        let server_account = self.is_server_account(&user.username);
        if server_account && !user.is_admin {
            return Err(format!(
                "Grafana: '{}' is Grafana's administrator login and the Meerschaum user is not \
                 an administrator.",
                user.username
            ));
        }
        let policy = self.anonymous_policy().await?;
        let mut report = SyncReport::default();
        let (id, created) = self.ensure_user(&user.username).await?;
        report.created_user = created;
        if !server_account {
            let res = self
                .call(
                    reqwest::Method::PUT,
                    &format!("/api/admin/users/{id}/permissions"),
                    None,
                    Some(json!({"isGrafanaAdmin": user.is_admin})),
                )
                .await?;
            Self::check("setting a user's permissions", res)?;
        }

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
        let mut joined_orgs: Vec<i64> = Vec::new();
        for team in &user.teams {
            let mut skip = |why: String| {
                let note = format!(
                    "Grafana: team '{}' is skipped for '{}': {why}.",
                    team.name, user.username
                );
                tell_administrator(true, &note);
                report.skipped.push(note);
            };
            let Some(org) = team.org else {
                skip("it has no saved organization (an administrator sets it up)".into());
                continue;
            };
            let name = match current.iter().find(|(org_id, _, _)| *org_id == org) {
                Some((_, name, _)) => name.clone(),
                None => match self.org_name(org).await? {
                    Some(name) => name,
                    None => {
                        skip(format!("its organization {org} does not exist"));
                        continue;
                    }
                },
            };
            if name.starts_with(TOMBSTONE_PREFIX) {
                skip(format!("its organization {org} was retired"));
                continue;
            }
            if let Some(why) = policy.refuses(Some(org), Some(&name)) {
                skip(why);
                continue;
            }
            let role = if team.manages || user.is_admin {
                "Admin"
            } else {
                "Editor"
            };
            match current.iter().find(|(org_id, _, _)| *org_id == org) {
                Some((_, _, existing)) if existing == role || server_account => {}
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
                    report.joined.push(team.name.clone());
                }
            }
            joined_orgs.push(org);
            if landing.is_none() || user.primary.as_deref() == Some(team.name.as_str()) {
                landing = Some(org);
            }
        }

        // A walled-off user without a usable team organization is not let in: they would land
        // in the main organization.
        if user.isolated && joined_orgs.is_empty() {
            return Err(format!(
                "Grafana: '{}' is in isolated teams only and none of them has a usable \
                 organization.",
                user.username
            ));
        }

        // Leave the organizations of the teams the user is not in and the retired ones, and,
        // when walled off, the main organization. A user always keeps one organization.
        for (org, name, _) in &current {
            if server_account {
                break;
            }
            let leave = if *org == MAIN_ORG_ID {
                user.isolated
            } else {
                (team_orgs.contains(org) || name.starts_with(TOMBSTONE_PREFIX))
                    && !joined_orgs.contains(org)
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

impl GrafanaClient {
    /// One warning about the data sources signed-out visitors can query (`None` when anonymous
    /// access is off, or there is nothing to say): every data source of the main and the
    /// anonymous organization except "Meerschaum Main" connecting as `readonly_role` (the stack's
    /// main read-only role). Meerschaum cannot know what the others' credentials may read.
    /// `owner` (`sql:main`'s user) singles out a data source that connects as the instance's
    /// owner.
    pub async fn anonymous_datasources_warning(
        &self,
        readonly_role: &str,
        owner: Option<&str>,
    ) -> Result<Option<String>, String> {
        let policy = self.anonymous_policy().await?;
        if !policy.enabled {
            return Ok(None);
        }
        let mut orgs = vec![MAIN_ORG_ID];
        if let Some(id) = policy.org_id
            && id != MAIN_ORG_ID
        {
            orgs.push(id);
        }
        let mut listed: Vec<String> = Vec::new();
        let mut as_owner: Vec<String> = Vec::new();
        for org in orgs {
            let sources = Self::check(
                "reading an organization's data sources",
                self.call(reqwest::Method::GET, "/api/datasources", Some(org), None)
                    .await?,
            )?;
            for source in sources.as_array().into_iter().flatten() {
                // `X-Grafana-Org-Id` is honored for members only: skip another org's answer.
                if source["orgId"].as_i64() != Some(org) {
                    continue;
                }
                let name = source["name"].as_str().unwrap_or("").to_string();
                let user = source["user"].as_str().unwrap_or("").to_string();
                if name == MAIN_DATASOURCE_NAME && user == readonly_role {
                    continue;
                }
                let kind = source["type"].as_str().unwrap_or("?");
                let line = if user.is_empty() {
                    format!("'{name}' ({kind}, organization {org})")
                } else {
                    format!("'{name}' ({kind}, organization {org}, user '{user}')")
                };
                if owner.is_some_and(|owner| !owner.is_empty() && owner == user) {
                    as_owner.push(format!("'{name}'"));
                }
                listed.push(line);
            }
        }
        if listed.is_empty() {
            return Ok(None);
        }
        let mut message = format!(
            "Grafana's anonymous access is on ('{}' as {}): anyone who can open a public \
             dashboard can run any query with the credentials of these data sources: {}. \
             Meerschaum cannot know what they may read; give them read-only credentials that \
             reach public data only.",
            policy.org_name,
            policy.role,
            listed.join(", ")
        );
        if !as_owner.is_empty() {
            message.push_str(&format!(
                " These connect as the instance's owner, who can read every team's data: {}.",
                as_owner.join(", ")
            ));
        }
        Ok(Some(message))
    }
}

/// [`GrafanaClient::anonymous_datasources_warning`] for the API server's start: tried a few
/// times while Grafana starts (about two minutes), then the warning (or why Grafana could not
/// be asked) is told to the administrator once.
pub async fn warn_about_anonymous_datasources(
    settings: GrafanaSettings,
    readonly_role: String,
    owner: Option<String>,
) {
    let client = match GrafanaClient::new(settings) {
        Ok(client) => client,
        Err(e) => {
            tell_administrator(true, &e);
            return;
        }
    };
    let mut last = String::new();
    for attempt in 0..8 {
        if attempt > 0 {
            tokio::time::sleep(Duration::from_secs(15)).await;
        }
        match client
            .anonymous_datasources_warning(&readonly_role, owner.as_deref())
            .await
        {
            Ok(Some(warning)) => {
                tell_administrator(true, &warning);
                return;
            }
            Ok(None) => return,
            Err(e) => last = e,
        }
    }
    tell_administrator(
        true,
        &format!(
            "Could not check which data sources Grafana's anonymous visitors can query: {last}"
        ),
    );
}

/// Keeps the teams' Grafana organizations in step with changes made through the API server
/// (`meerschaum_server::backends::TeamEvents`); nothing when `api:grafana` is not enabled.
#[derive(Debug, Clone)]
pub struct GrafanaTeams(Result<Option<GrafanaSettings>, String>);

impl GrafanaTeams {
    /// For `api:grafana` (`section`); `password_file`: [`admin_password_path`].
    pub fn from_section(section: &Map, password_file: &Path) -> GrafanaTeams {
        GrafanaTeams(GrafanaSettings::from_section(section, password_file))
    }

    /// Why the enabled integration cannot be used, if it cannot.
    pub fn error(&self) -> Option<&str> {
        self.0.as_ref().err().map(String::as_str)
    }
}

/// Rename the organization (`org`: the team's saved id) of a team renamed to `new` (`Ok(false)`:
/// it has none, or Grafana is off).
pub async fn rename_team(
    settings: Option<&GrafanaSettings>,
    org: Option<i64>,
    new: &str,
) -> Result<bool, String> {
    match (settings, org) {
        (Some(settings), Some(org)) => {
            GrafanaClient::new(settings.clone())?
                .rename_org(org, new)
                .await
        }
        _ => Ok(false),
    }
}

/// Retire the organization (`org`: its saved id) of the deleted team `team` (`Ok(false)`: it has
/// none, or Grafana is off).
pub async fn retire_team(
    settings: Option<&GrafanaSettings>,
    team: &str,
    org: Option<i64>,
) -> Result<bool, String> {
    match (settings, org) {
        (Some(settings), Some(org)) => {
            GrafanaClient::new(settings.clone())?
                .retire_org(team, org)
                .await
        }
        _ => Ok(false),
    }
}

#[async_trait::async_trait]
impl meerschaum_server::backends::TeamEvents for GrafanaTeams {
    async fn renamed(&self, team: &Team, _old: &str) -> Result<(), String> {
        let settings = self.0.clone()?;
        rename_team(settings.as_ref(), team_org_id(team), &team.name)
            .await
            .map(|_| ())
    }

    async fn deleted(&self, team: &Team) -> Result<(), String> {
        let settings = self.0.clone()?;
        retire_team(settings.as_ref(), &team.name, team_org_id(team))
            .await
            .map(|_| ())
    }
}

// ---------------------------------------------------------------------------------------------
// The reverse proxy
// ---------------------------------------------------------------------------------------------

/// The proxy's state: the client (or why the integration is inactive) and when each user was
/// last synchronized.
pub(crate) struct GrafanaProxy {
    client: Result<GrafanaClient, String>,
    synced: Mutex<HashMap<String, (Instant, GrafanaUser)>>,
}

impl GrafanaProxy {
    pub(crate) fn from_state(server: &meerschaum_server::AppState) -> Option<Arc<GrafanaProxy>> {
        let client = match GrafanaSettings::from_section(
            &server.config.grafana,
            &server.config.grafana_password_path,
        ) {
            Ok(None) => return None,
            Ok(Some(settings)) => GrafanaClient::new(settings),
            Err(e) => Err(e),
        };
        if let Err(e) = &client {
            tracing::warn!("The Grafana integration is inactive: {e}");
        }
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

/// What the browser is told when Grafana cannot be used (the reason is logged for the
/// administrator).
const UNAVAILABLE: &str = "Grafana is not available right now. Try again in a moment.";

fn unavailable(message: &str) -> Response {
    (StatusCode::BAD_GATEWAY, message.to_string()).into_response()
}

fn to_login() -> Response {
    Redirect::to("/dash/login").into_response()
}

/// The user of the request's console session, as Grafana should see them (`Ok(None)`: signed
/// out).
async fn grafana_user(
    state: &WebState,
    headers: &HeaderMap,
) -> Result<Option<GrafanaUserTeams>, Box<Response>> {
    let data = match crate::session(state, headers).await {
        Ok(data) => data,
        Err(StatusCode::SERVICE_UNAVAILABLE) => {
            return Err(Box::new(StatusCode::SERVICE_UNAVAILABLE.into_response()));
        }
        Err(_) => return Ok(None),
    };
    let username = match crate::bound_username(&state.server, &data).await {
        Ok(Some(username)) => username,
        Ok(None) => return Ok(None),
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
    Ok(Some(GrafanaUserTeams {
        user: GrafanaUser::new(&username, is_admin, &user_teams, &records, &primary),
        team_orgs: recorded_orgs(&records)
            .into_iter()
            .map(|(_, org)| org)
            .collect(),
    }))
}

struct GrafanaUserTeams {
    user: GrafanaUser,
    team_orgs: Vec<i64>,
}

/// Whether a signed-in user may be named to Grafana: never as Grafana's administrator login
/// (compared without case, as Grafana does) unless they are a Meerschaum administrator.
pub fn may_sign_in_as(username: &str, is_admin: bool, admin_user: &str) -> bool {
    is_admin || !is_admin_login(username, admin_user)
}

/// Headers never forwarded to Grafana: hop-by-hop ones, and anything a browser could use to
/// claim an identity (the auth-proxy headers) or Grafana's own credentials. `Host` is kept:
/// Grafana compares it with `Origin` before it accepts a change. The console's session cookie
/// stays here, and so do the cookies named in `dropped_cookies` (Grafana's own session, for a
/// signed-out request).
fn forwarded_headers(headers: &HeaderMap, upgrade: bool, dropped_cookies: &[String]) -> HeaderMap {
    const DROPPED: &[&str] = &[
        "connection",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
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
                    let name = item.split_once('=').map_or(*item, |(k, _)| k).trim();
                    !item.is_empty()
                        && name != crate::sessions::COOKIE
                        && !dropped_cookies.iter().any(|c| c.eq_ignore_ascii_case(name))
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
    let client = match &proxy.client {
        Ok(client) => client,
        Err(e) => {
            tracing::warn!("The Grafana integration is inactive: {e}");
            return match who {
                Some(_) => unavailable(UNAVAILABLE),
                None => to_login(),
            };
        }
    };
    let is_upgrade = req.headers().contains_key(header::UPGRADE);
    let headers = match who {
        // Signed out: Grafana's own anonymous access, when it has any, without any identity.
        None => match client.anonymous_policy().await {
            Ok(policy) if policy.enabled => {
                forwarded_headers(req.headers(), is_upgrade, &policy.cookie_names)
            }
            Ok(_) => return to_login(),
            Err(e) => {
                tracing::warn!("{e}");
                return to_login();
            }
        },
        Some(who) => {
            if !may_sign_in_as(
                &who.user.username,
                who.user.is_admin,
                &client.settings().admin_user,
            ) {
                return (
                    StatusCode::FORBIDDEN,
                    "This account cannot be signed in to Grafana.",
                )
                    .into_response();
            }
            if !proxy.is_synced(&who.user) {
                if let Err(e) = client.sync_user(&who.user, &who.team_orgs).await {
                    tracing::warn!("{e}");
                    return unavailable(UNAVAILABLE);
                }
                proxy.mark_synced(who.user.clone());
            }
            let mut headers = forwarded_headers(req.headers(), is_upgrade, &[]);
            let Ok(username) = HeaderValue::from_str(&who.user.username) else {
                return unavailable("This username cannot be sent to Grafana.");
            };
            headers.insert(HeaderName::from_static(USER_HEADER), username);
            headers
        }
    };
    let path_and_query = req
        .uri()
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| PATH.to_string());
    let client_upgrade = is_upgrade.then(|| hyper::upgrade::on(&mut req));
    let method = req.method().clone();
    let has_body = !matches!(
        method,
        axum::http::Method::GET | axum::http::Method::HEAD | axum::http::Method::OPTIONS
    );
    let mut upstream = client
        .http
        .request(method, format!("{}{path_and_query}", client.settings.url))
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
mod tests;
