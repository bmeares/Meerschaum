//! The Grafana integration against a fake Grafana (its HTTP API as the integration uses it):
//! the administrator's password, organizations by saved id, retired organizations, the
//! anonymous and main organizations, and the signed-in / signed-out proxy.
#[path = "../../meerschaum-server/tests/common/mod.rs"]
mod common;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, to_bytes};
use axum::extract::{Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Team, TeamRole};
use meerschaum_core::{Map, Value};
use meerschaum_web::grafana::{
    Datasource, GrafanaClient, GrafanaSettings, GrafanaUser, MAIN_ORG_ID, TOMBSTONE_PREFIX,
    tombstone_name,
};
use serde_json::{Value as Json, json};

// ------------------------------------------------------------------------------------------------
// The fake
// ------------------------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct FakeUser {
    login: String,
    password: String,
    server_admin: bool,
}

#[derive(Debug, Default)]
struct Fake {
    users: BTreeMap<i64, FakeUser>,
    /// id -> name
    orgs: BTreeMap<i64, String>,
    /// (org, user) -> role
    members: BTreeMap<(i64, i64), String>,
    /// uid -> (org, name, user)
    datasources: BTreeMap<String, (i64, String, String)>,
    /// org -> dashboards
    dashboards: BTreeMap<i64, usize>,
    anonymous_enabled: bool,
    anonymous_org: String,
    /// Every request: (method, path, basic-auth login or "-", headers).
    log: Vec<(String, String, String, HeaderMap)>,
    next: i64,
}

type Shared = Arc<Mutex<Fake>>;

impl Fake {
    fn new(admin_password: &str) -> Fake {
        let mut fake = Fake {
            anonymous_enabled: true,
            anonymous_org: "public".into(),
            next: 10,
            ..Fake::default()
        };
        fake.users.insert(
            1,
            FakeUser {
                login: "root".into(),
                password: admin_password.into(),
                server_admin: true,
            },
        );
        fake.orgs.insert(MAIN_ORG_ID, "Main Org.".into());
        fake.members.insert((MAIN_ORG_ID, 1), "Admin".into());
        fake
    }

    fn id(&mut self) -> i64 {
        self.next += 1;
        self.next
    }

    fn add_org(&mut self, name: &str) -> i64 {
        let id = self.id();
        self.orgs.insert(id, name.into());
        self.members.insert((id, 1), "Admin".into());
        id
    }

    fn add_user(&mut self, login: &str, orgs: &[(i64, &str)]) -> i64 {
        let id = self.id();
        self.users.insert(
            id,
            FakeUser {
                login: login.into(),
                password: "x".into(),
                server_admin: false,
            },
        );
        for (org, role) in orgs {
            self.members.insert((*org, id), role.to_string());
        }
        id
    }

    fn add_datasource(&mut self, org: i64, name: &str) -> String {
        let uid = format!("ds{}", self.id());
        self.datasources
            .insert(uid.clone(), (org, name.into(), "ro".into()));
        uid
    }

    fn org_of(&self, name: &str) -> Option<i64> {
        self.orgs
            .iter()
            .find(|(_, n)| n.as_str() == name)
            .map(|(id, _)| *id)
    }

    fn user_of(&self, login: &str) -> Option<i64> {
        self.users
            .iter()
            .find(|(_, u)| u.login.eq_ignore_ascii_case(login))
            .map(|(id, _)| *id)
    }

    fn orgs_of_user(&self, user: i64) -> Vec<i64> {
        self.members
            .keys()
            .filter(|(_, u)| *u == user)
            .map(|(o, _)| *o)
            .collect()
    }

    fn requests(&self, method: &str, prefix: &str) -> usize {
        self.log
            .iter()
            .filter(|(m, p, _, _)| m == method && p.starts_with(prefix))
            .count()
    }
}

fn reply(status: u16, body: Json) -> Response {
    (StatusCode::from_u16(status).unwrap(), axum::Json(body)).into_response()
}

fn basic(headers: &HeaderMap) -> Option<(String, String)> {
    use base64_lite::decode;
    let value = headers.get("authorization")?.to_str().ok()?;
    let encoded = value.strip_prefix("Basic ")?;
    let text = String::from_utf8(decode(encoded)?).ok()?;
    let (user, password) = text.split_once(':')?;
    Some((user.into(), password.into()))
}

/// A tiny base64 decoder (no new dependency for the tests).
mod base64_lite {
    pub fn decode(text: &str) -> Option<Vec<u8>> {
        const TABLE: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let mut out = Vec::new();
        let mut buffer = 0u32;
        let mut bits = 0;
        for c in text.bytes().filter(|c| *c != b'=') {
            let v = TABLE.iter().position(|t| *t == c)? as u32;
            buffer = (buffer << 6) | v;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                out.push((buffer >> bits) as u8);
                buffer &= (1 << bits) - 1;
            }
        }
        Some(out)
    }
}

async fn handle(State(fake): State<Shared>, req: Request) -> Response {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();
    let headers = req.headers().clone();
    let body: Json = serde_json::from_slice(&to_bytes(req.into_body(), 1 << 20).await.unwrap())
        .unwrap_or(Json::Null);
    let mut f = fake.lock().unwrap();
    let auth = basic(&headers);
    f.log.push((
        method.clone(),
        path.clone(),
        auth.as_ref().map(|(u, _)| u.clone()).unwrap_or("-".into()),
        headers.clone(),
    ));
    // Anything outside `/api/` is the proxied UI: say who Grafana would see.
    if !path.starts_with("/api/") {
        return reply(
            200,
            json!({
                "path": path,
                "user": headers.get("x-webauth-user").and_then(|v| v.to_str().ok()),
                "cookie": headers.get("cookie").and_then(|v| v.to_str().ok()),
                "org": headers.get("x-grafana-org-id").and_then(|v| v.to_str().ok()),
                "authorization": headers.get("authorization").is_some(),
            }),
        );
    }
    let Some((login, password)) = auth else {
        return reply(401, json!({"message": "Unauthorized"}));
    };
    let Some(caller) = f.user_of(&login) else {
        return reply(401, json!({"message": "invalid username or password"}));
    };
    if f.users[&caller].password != password {
        return reply(401, json!({"message": "invalid username or password"}));
    }
    let header_org = headers
        .get("x-grafana-org-id")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<i64>().ok());
    let parts: Vec<&str> = path.trim_start_matches("/api/").split('/').collect();
    let decode = |s: &str| {
        percent_encoding::percent_decode_str(s)
            .decode_utf8_lossy()
            .into_owned()
    };
    match (method.as_str(), parts.as_slice()) {
        ("GET", ["user"]) => reply(
            200,
            json!({"login": f.users[&caller].login, "isGrafanaAdmin": f.users[&caller].server_admin}),
        ),
        ("PUT", ["user", "password"]) => {
            if body["oldPassword"] != json!(f.users[&caller].password) {
                return reply(401, json!({"message": "Invalid old password"}));
            }
            f.users.get_mut(&caller).unwrap().password =
                body["newPassword"].as_str().unwrap().into();
            reply(200, json!({"message": "User password changed"}))
        }
        ("GET", ["admin", "settings"]) => reply(
            200,
            json!({
                "auth": {"login_cookie_name": "grafana_session"},
                "auth.anonymous": {
                    "enabled": if f.anonymous_enabled { "true" } else { "false" },
                    "org_name": f.anonymous_org,
                    "org_role": "Viewer",
                },
            }),
        ),
        ("GET", ["orgs", "name", name]) => match f.org_of(&decode(name)) {
            Some(id) => reply(200, json!({"id": id, "name": f.orgs[&id]})),
            None => reply(404, json!({"message": "Organization not found"})),
        },
        ("POST", ["orgs"]) => {
            let name = body["name"].as_str().unwrap().to_string();
            if f.org_of(&name).is_some() {
                return reply(409, json!({"message": "Organization name taken"}));
            }
            let id = f.add_org(&name);
            reply(200, json!({"orgId": id}))
        }
        ("GET", ["orgs", id]) => match id.parse::<i64>().ok().and_then(|id| f.orgs.get(&id)) {
            Some(name) => reply(200, json!({"id": id.parse::<i64>().unwrap(), "name": name})),
            None => reply(404, json!({"message": "Organization not found"})),
        },
        ("PUT", ["orgs", id]) => {
            let id: i64 = id.parse().unwrap();
            let name = body["name"].as_str().unwrap().to_string();
            if f.org_of(&name).is_some_and(|other| other != id) {
                return reply(409, json!({"message": "Organization name taken"}));
            }
            match f.orgs.get_mut(&id) {
                Some(n) => {
                    *n = name;
                    reply(200, json!({"message": "Organization updated"}))
                }
                None => reply(404, json!({"message": "Organization not found"})),
            }
        }
        ("GET", ["orgs", id, "users"]) => {
            let id: i64 = id.parse().unwrap();
            let users: Vec<Json> = f
                .members
                .iter()
                .filter(|((o, _), _)| *o == id)
                .map(|((_, u), role)| json!({"userId": u, "login": f.users[u].login, "role": role}))
                .collect();
            reply(200, Json::Array(users))
        }
        ("POST", ["orgs", id, "users"]) => {
            let id: i64 = id.parse().unwrap();
            if !f.orgs.contains_key(&id) {
                return reply(404, json!({"message": "Organization not found"}));
            }
            let user = f.user_of(body["loginOrEmail"].as_str().unwrap()).unwrap();
            if f.members.contains_key(&(id, user)) {
                return reply(
                    409,
                    json!({"message": "User is already member of this organization"}),
                );
            }
            f.members
                .insert((id, user), body["role"].as_str().unwrap().into());
            reply(200, json!({"message": "User added to organization"}))
        }
        ("PATCH", ["orgs", id, "users", user]) => {
            let key = (id.parse().unwrap(), user.parse().unwrap());
            f.members.insert(key, body["role"].as_str().unwrap().into());
            reply(200, json!({}))
        }
        ("DELETE", ["orgs", id, "users", user]) => {
            let key = (id.parse().unwrap(), user.parse().unwrap());
            f.members.remove(&key);
            reply(200, json!({}))
        }
        ("GET", ["users", "lookup"]) => {
            let login = decode(query.trim_start_matches("loginOrEmail="));
            match f.user_of(&login) {
                Some(id) => reply(200, json!({"id": id})),
                None => reply(404, json!({"message": "user not found"})),
            }
        }
        ("POST", ["admin", "users"]) => {
            let id = f.add_user(body["login"].as_str().unwrap(), &[(MAIN_ORG_ID, "Viewer")]);
            reply(200, json!({"id": id}))
        }
        ("PUT", ["admin", "users", id, "permissions"]) => {
            let id: i64 = id.parse().unwrap();
            f.users.get_mut(&id).unwrap().server_admin = body["isGrafanaAdmin"].as_bool().unwrap();
            reply(200, json!({}))
        }
        ("GET", ["users", id, "orgs"]) => {
            let id: i64 = id.parse().unwrap();
            let orgs: Vec<Json> = f
                .members
                .iter()
                .filter(|((_, u), _)| *u == id)
                .map(|((o, _), role)| json!({"orgId": o, "name": f.orgs[o], "role": role}))
                .collect();
            reply(200, Json::Array(orgs))
        }
        ("POST", ["users", _, "using", _]) => reply(200, json!({})),
        // Data sources: the org is `X-Grafana-Org-Id` for its members, else the caller's main.
        (_, ["datasources", ..]) => {
            let org = header_org
                .filter(|o| f.members.contains_key(&(*o, caller)))
                .unwrap_or(MAIN_ORG_ID);
            match (method.as_str(), &parts[1..]) {
                ("GET", []) => {
                    let list: Vec<Json> = f
                        .datasources
                        .iter()
                        .filter(|(_, (o, _, _))| *o == org)
                        .map(|(uid, (o, n, u))| {
                            json!({"uid": uid, "orgId": o, "name": n, "user": u, "type": "postgres"})
                        })
                        .collect();
                    reply(200, Json::Array(list))
                }
                ("GET", ["name", name]) => {
                    let name = decode(name);
                    match f
                        .datasources
                        .iter()
                        .find(|(_, (o, n, _))| *o == org && *n == name)
                    {
                        Some((uid, _)) => reply(200, json!({"uid": uid})),
                        None => reply(404, json!({"message": "not found"})),
                    }
                }
                ("POST", []) => {
                    let uid = format!("ds{}", f.id());
                    f.datasources.insert(
                        uid.clone(),
                        (
                            org,
                            body["name"].as_str().unwrap().into(),
                            body["user"].as_str().unwrap_or("").into(),
                        ),
                    );
                    reply(200, json!({"datasource": {"uid": uid}}))
                }
                ("PUT", ["uid", uid]) => {
                    let uid = uid.to_string();
                    if let Some(source) = f.datasources.get_mut(&uid) {
                        source.2 = body["user"].as_str().unwrap_or("").into();
                    }
                    reply(200, json!({}))
                }
                ("DELETE", ["uid", uid]) => {
                    let uid = decode(uid);
                    match f.datasources.get(&uid) {
                        Some((o, _, _)) if *o == org => {
                            f.datasources.remove(&uid);
                            reply(200, json!({}))
                        }
                        _ => reply(404, json!({"message": "not found"})),
                    }
                }
                _ => reply(404, json!({"message": "no such route"})),
            }
        }
        _ => reply(
            404,
            json!({"message": format!("no such route {method} {path}")}),
        ),
    }
}

/// A running fake Grafana: its URL and state.
async fn fake_grafana(admin_password: &str) -> (String, Shared) {
    let fake: Shared = Arc::new(Mutex::new(Fake::new(admin_password)));
    let app = Router::new().fallback(handle).with_state(fake.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (url, fake)
}

fn settings(url: &str, password: &str) -> GrafanaSettings {
    GrafanaSettings {
        url: url.into(),
        admin_user: "root".into(),
        admin_password: password.into(),
        database_url: None,
    }
}

fn client(url: &str, password: &str) -> GrafanaClient {
    GrafanaClient::new(settings(url, password)).unwrap()
}

fn team_with_org(name: &str, org: Option<i64>, members: &[(&str, TeamRole)]) -> Team {
    let mut team = Team::new(name);
    for (user, role) in members {
        team.add_member(user, *role);
    }
    if let Some(org) = org {
        let mut record = Map::new();
        record.insert("org_id".into(), Value::Int(org));
        team.attributes.insert("grafana".into(), Value::Map(record));
    }
    team
}

fn source(name: &str) -> Datasource {
    Datasource {
        name: name.into(),
        url: "db:5432".into(),
        database: "wetlands".into(),
        user: "mrsm_wetlands_ro".into(),
        password: "ro-pw".into(),
        timescaledb: false,
    }
}

// ------------------------------------------------------------------------------------------------
// 1a. The administrator's password
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_default_password_is_rotated_once_to_the_configured_one() {
    let (url, fake) = fake_grafana("admin").await;
    let grafana = client(&url, "generated-secret");
    grafana.ensure_admin().await.unwrap();
    assert_eq!(fake.lock().unwrap().users[&1].password, "generated-secret");
    assert_eq!(
        fake.lock().unwrap().requests("PUT", "/api/user/password"),
        1
    );
    // Once: the next client signs in with the configured password directly.
    let again = client(&url, "generated-secret");
    again.ensure_admin().await.unwrap();
    again.org_id("Main Org.").await.unwrap();
    assert_eq!(
        fake.lock().unwrap().requests("PUT", "/api/user/password"),
        1
    );
}

#[tokio::test]
async fn neither_password_working_makes_the_integration_inactive() {
    let (url, fake) = fake_grafana("something-else").await;
    let grafana = client(&url, "generated-secret");
    let error = grafana.ensure_admin().await.unwrap_err();
    assert!(error.contains("inactive"), "{error}");
    // Nothing is attempted with the default credentials beyond the check, and the failure is
    // remembered (Grafana locks accounts out after repeated failures).
    let error = grafana.org_id("Main Org.").await.unwrap_err();
    assert!(error.contains("inactive"), "{error}");
    let f = fake.lock().unwrap();
    assert_eq!(f.requests("GET", "/api/user"), 2);
    assert_eq!(f.requests("PUT", "/api/user/password"), 0);
    assert_eq!(f.requests("GET", "/api/orgs"), 0);
}

#[tokio::test]
async fn a_password_changed_under_the_server_is_noticed_again() {
    let (url, fake) = fake_grafana("generated-secret").await;
    let grafana = client(&url, "generated-secret");
    grafana.org_id("Main Org.").await.unwrap();
    // Someone resets Grafana's administrator to the default password.
    fake.lock().unwrap().users.get_mut(&1).unwrap().password = "admin".into();
    // The call that meets the change fails; the next one checks again and rotates.
    assert!(grafana.org_id("Main Org.").await.is_err());
    grafana.org_id("Main Org.").await.unwrap();
    assert_eq!(fake.lock().unwrap().users[&1].password, "generated-secret");
}

// ------------------------------------------------------------------------------------------------
// 2. Organizations by saved id
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_renamed_team_keeps_its_organization_and_never_gets_a_deleted_teams() {
    let (url, fake) = fake_grafana("pw").await;
    let (org_b, org_x) = {
        let mut f = fake.lock().unwrap();
        let b = f.add_org("B");
        let x = f.add_org("X");
        f.add_datasource(b, "B");
        f.add_user("bea", &[(b, "Editor")]);
        (b, x)
    };
    let grafana = client(&url, "pw");
    // Team B is deleted: its organization is retired.
    assert!(grafana.retire_org("B", org_b).await.unwrap());
    // Team X is renamed to B: the organization of X (by id) follows.
    assert!(grafana.rename_org(org_x, "B").await.unwrap());
    {
        let f = fake.lock().unwrap();
        assert_eq!(f.orgs[&org_x], "B");
        assert_eq!(f.orgs[&org_b], tombstone_name("B", org_b));
    }
    // A member of (the former) X lands in X's organization, never in B's leftover one.
    let records = vec![
        Team::everyone(),
        team_with_org("B", Some(org_x), &[("xavier", TeamRole::Member)]),
    ];
    let user = GrafanaUser::new(
        "xavier",
        false,
        &[("B".into(), TeamRole::Member)],
        &records,
        "B",
    );
    grafana.sync_user(&user, &[org_x]).await.unwrap();
    let f = fake.lock().unwrap();
    let xavier = f.user_of("xavier").unwrap();
    assert!(f.members.contains_key(&(org_x, xavier)));
    assert!(!f.members.contains_key(&(org_b, xavier)));
}

#[tokio::test]
async fn sync_never_finds_or_makes_an_organization_by_name() {
    let (url, fake) = fake_grafana("pw").await;
    let leftover = fake.lock().unwrap().add_org("Data");
    let grafana = client(&url, "pw");
    let orgs_before = fake.lock().unwrap().orgs.len();
    // Team Data has no saved organization; an organization named "Data" exists.
    let records = vec![
        Team::everyone(),
        team_with_org("Data", None, &[("dana", TeamRole::Member)]),
        team_with_org("Gone", Some(999), &[("dana", TeamRole::Member)]),
    ];
    let user = GrafanaUser::new(
        "dana",
        false,
        &[
            ("Data".into(), TeamRole::Member),
            ("Gone".into(), TeamRole::Member),
        ],
        &records,
        "Data",
    );
    let report = grafana.sync_user(&user, &[999]).await.unwrap();
    assert_eq!(report.skipped.len(), 2, "{report:?}");
    assert!(report.joined.is_empty());
    let f = fake.lock().unwrap();
    assert_eq!(f.orgs.len(), orgs_before);
    let dana = f.user_of("dana").unwrap();
    assert!(!f.members.contains_key(&(leftover, dana)));
}

#[tokio::test]
async fn setup_adopts_by_name_only_what_no_other_team_has() {
    let (url, fake) = fake_grafana("pw").await;
    let taken = fake.lock().unwrap().add_org("Ops");
    let free = fake.lock().unwrap().add_org("Field");
    let grafana = client(&url, "pw");
    // "Ops" is recorded for team "Research": not adopted for a new team "Ops".
    let recorded = vec![("Research".to_string(), taken)];
    let error = grafana.ensure_team_org("Ops", &recorded).await.unwrap_err();
    assert!(error.contains("Research"), "{error}");
    // An unrecorded organization of the name is adopted; a missing one is created.
    assert_eq!(
        grafana.ensure_team_org("Field", &recorded).await.unwrap(),
        (free, false)
    );
    let (made, created) = grafana.ensure_team_org("Lab", &recorded).await.unwrap();
    assert!(created && made != free && made != taken);
}

#[tokio::test]
async fn a_retired_organization_keeps_its_dashboards_and_loses_its_members_and_sources() {
    let (url, fake) = fake_grafana("pw").await;
    let org = {
        let mut f = fake.lock().unwrap();
        let org = f.add_org("Wetlands");
        f.add_user("ann", &[(org, "Admin")]);
        f.add_user("cal", &[(org, "Editor"), (MAIN_ORG_ID, "Viewer")]);
        f.add_datasource(org, "Wetlands");
        f.add_datasource(MAIN_ORG_ID, "Meerschaum Main");
        f.dashboards.insert(org, 3);
        org
    };
    let grafana = client(&url, "pw");
    assert!(grafana.retire_org("Wetlands", org).await.unwrap());
    let f = fake.lock().unwrap();
    assert!(f.orgs[&org].starts_with(TOMBSTONE_PREFIX));
    let members: Vec<&String> = f
        .members
        .keys()
        .filter(|(o, _)| *o == org)
        .map(|(_, u)| &f.users[u].login)
        .collect();
    assert_eq!(members, vec!["root"]);
    assert!(!f.datasources.values().any(|(o, _, _)| *o == org));
    // Other organizations' data sources and memberships are untouched; dashboards stay.
    assert!(f.datasources.values().any(|(o, _, _)| *o == MAIN_ORG_ID));
    let cal = f.user_of("cal").unwrap();
    assert!(f.members.contains_key(&(MAIN_ORG_ID, cal)));
    assert_eq!(f.dashboards[&org], 3);
}

// ------------------------------------------------------------------------------------------------
// 1c. The anonymous and main organizations are never a team's
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_team_cannot_use_the_anonymous_or_the_main_organization() {
    let (url, fake) = fake_grafana("pw").await;
    let (anonymous, team_org) = {
        let mut f = fake.lock().unwrap();
        (f.add_org("public"), f.add_org("Wetlands"))
    };
    let grafana = client(&url, "pw");
    // Setup of a team named like the anonymous organization (compared without case).
    for name in ["public", "Public"] {
        let error = grafana.ensure_team_org(name, &[]).await.unwrap_err();
        assert!(error.contains("anonymous"), "{error}");
    }
    // A rename into the anonymous organization's name, or of the main / anonymous organization.
    assert!(grafana.rename_org(team_org, "PUBLIC").await.is_err());
    assert!(grafana.rename_org(MAIN_ORG_ID, "Wetlands2").await.is_err());
    assert!(grafana.rename_org(anonymous, "Wetlands2").await.is_err());
    // Retiring the main or the anonymous organization is refused, and touches nothing.
    let members_before = fake.lock().unwrap().members.len();
    assert!(grafana.retire_org("x", MAIN_ORG_ID).await.is_err());
    assert!(grafana.retire_org("x", anonymous).await.is_err());
    assert_eq!(fake.lock().unwrap().members.len(), members_before);
    assert_eq!(fake.lock().unwrap().orgs[&anonymous], "public");
    // A member whose team records the anonymous organization is not added to it.
    let records = vec![
        Team::everyone(),
        team_with_org("Pub", Some(anonymous), &[("pat", TeamRole::Member)]),
    ];
    let user = GrafanaUser::new(
        "pat",
        false,
        &[("Pub".into(), TeamRole::Member)],
        &records,
        "Pub",
    );
    let report = grafana.sync_user(&user, &[anonymous]).await.unwrap();
    assert_eq!(report.skipped.len(), 1, "{report:?}");
    let f = fake.lock().unwrap();
    let pat = f.user_of("pat").unwrap();
    assert!(!f.members.contains_key(&(anonymous, pat)));
}

#[tokio::test]
async fn team_data_sources_only_land_in_their_teams_organization() {
    let (url, fake) = fake_grafana("pw").await;
    let anonymous = fake.lock().unwrap().add_org("public");
    let settings = settings(&url, "pw");
    let grafana = GrafanaClient::new(settings.clone()).unwrap();
    // Directly: never the main or the anonymous organization.
    assert!(
        grafana
            .ensure_datasource(MAIN_ORG_ID, &source("x"))
            .await
            .is_err()
    );
    assert!(
        grafana
            .ensure_datasource(anonymous, &source("x"))
            .await
            .is_err()
    );
    // Through setup: a team named like the anonymous organization gets no data source at all.
    assert!(
        meerschaum_web::grafana::ensure_team(&settings, "public", Some(&source("public")), &[])
            .await
            .is_err()
    );
    // A team's setup puts its data source in its own organization only.
    let (org, _) =
        meerschaum_web::grafana::ensure_team(&settings, "Wetlands", Some(&source("Wetlands")), &[])
            .await
            .unwrap();
    // Rename and retire never create one.
    grafana.rename_org(org, "Marsh").await.unwrap();
    grafana.retire_org("Marsh", org).await.unwrap();
    let f = fake.lock().unwrap();
    assert!(
        !f.datasources
            .values()
            .any(|(o, _, _)| *o == MAIN_ORG_ID || *o == anonymous),
        "{:?}",
        f.datasources
    );
    assert_eq!(f.requests("POST", "/api/datasources"), 1);
}

/// The startup check lists every data source anonymous visitors can query except the stack's
/// read-only "Meerschaum Main", singles out the instance owner's, and says nothing when anonymous
/// access is off.
#[tokio::test]
async fn the_anonymous_data_sources_are_listed_once() {
    let (url, fake) = fake_grafana("pw").await;
    let grafana = client(&url, "pw");
    {
        let mut f = fake.lock().unwrap();
        let uid = f.add_datasource(MAIN_ORG_ID, "Meerschaum Main");
        f.datasources.get_mut(&uid).unwrap().2 = "mrsm__main_ro".into();
    }
    assert_eq!(
        grafana
            .anonymous_datasources_warning("mrsm__main_ro", Some("owner"))
            .await
            .unwrap(),
        None,
        "the read-only main data source alone needs no warning"
    );
    {
        let mut f = fake.lock().unwrap();
        let uid = f.add_datasource(MAIN_ORG_ID, "Hand made");
        f.datasources.get_mut(&uid).unwrap().2 = "owner".into();
    }
    let warning = grafana
        .anonymous_datasources_warning("mrsm__main_ro", Some("owner"))
        .await
        .unwrap()
        .unwrap();
    assert!(warning.contains("'Hand made'"), "{warning}");
    assert!(warning.contains("instance's owner"), "{warning}");
    assert!(!warning.contains("'Meerschaum Main'"), "{warning}");
    // A "Meerschaum Main" pointed at another user is listed too.
    {
        let mut f = fake.lock().unwrap();
        for source in f.datasources.values_mut() {
            if source.1 == "Meerschaum Main" {
                source.2 = "reader".into();
            }
        }
    }
    let warning = grafana
        .anonymous_datasources_warning("mrsm__main_ro", Some("owner"))
        .await
        .unwrap()
        .unwrap();
    assert!(warning.contains("'Meerschaum Main'"), "{warning}");
    // Anonymous access off: nothing to say.
    fake.lock().unwrap().anonymous_enabled = false;
    let grafana = client(&url, "pw");
    assert_eq!(
        grafana
            .anonymous_datasources_warning("mrsm__main_ro", Some("owner"))
            .await
            .unwrap(),
        None
    );
}

// ------------------------------------------------------------------------------------------------
// 3. The server's own account
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn sync_never_demotes_the_servers_own_account() {
    let (url, fake) = fake_grafana("pw").await;
    let grafana = client(&url, "pw");
    let records = vec![Team::everyone()];
    let everyone = vec![(
        meerschaum_core::access::EVERYONE.to_string(),
        TeamRole::Member,
    )];
    // A non-administrator named like the account (any case) is refused before any change.
    let impostor = GrafanaUser::new("ROOT", false, &everyone, &records, "Everyone");
    assert!(grafana.sync_user(&impostor, &[]).await.is_err());
    // An administrator of that name changes nothing about the account's permissions.
    let admin = GrafanaUser::new("root", true, &everyone, &records, "Everyone");
    grafana.sync_user(&admin, &[]).await.unwrap();
    let f = fake.lock().unwrap();
    assert!(f.users[&1].server_admin);
    assert_eq!(f.requests("PUT", "/api/admin/users"), 0);
}

// ------------------------------------------------------------------------------------------------
// The proxy
// ------------------------------------------------------------------------------------------------

async fn console_session(state: &meerschaum_server::AppState, username: &str) -> Json {
    let user = meerschaum_core::users::User::new(username, None);
    let credential = meerschaum_server::auth::user_credential(state, &user)
        .await
        .unwrap();
    json!({"username": username, "credential": credential})
}

struct Proxy {
    app: Router,
    web: meerschaum_web::WebConfig,
    state: meerschaum_server::AppState,
    fake: Shared,
    _root: tempfile::TempDir,
}

async fn proxy(anonymous: bool) -> Proxy {
    let (url, fake) = fake_grafana("pw").await;
    fake.lock().unwrap().anonymous_enabled = anonymous;
    let root = tempfile::tempdir().unwrap();
    let mut cfg = common::config(root.path());
    cfg.grafana = Value::from_json_str(&format!(
        r#"{{"enabled": true, "url": "{url}", "admin_user": "root", "admin_password": "pw"}}"#
    ))
    .unwrap()
    .to_map();
    let instance = Arc::new(common::MemoryInstance::new("grafana-proxy"));
    instance.seed_user("boss", "pass", Some("admin"));
    instance.seed_user("ROOT", "pass", None);
    instance.seed_user("ann", "pass", None);
    let org = fake.lock().unwrap().add_org("Wetlands");
    assert!(
        instance
            .register_team(&mut team_with_org(
                "Wetlands",
                Some(org),
                &[("ann", TeamRole::Member)]
            ))
            .await
            .success
    );
    let state = meerschaum_server::AppState::builder(cfg, instance)
        .secret(common::SECRET.to_vec())
        .build()
        .unwrap();
    let web = meerschaum_web::WebConfig::for_state(&state);
    let app = meerschaum_web::app(state.clone(), web.clone()).unwrap();
    Proxy {
        app,
        web,
        state,
        fake,
        _root: root,
    }
}

impl Proxy {
    async fn sign_in(&self, username: &str) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        self.web
            .sessions
            .set(&id, console_session(&self.state, username).await, 100)
            .await
            .unwrap();
        id
    }

    async fn get(&self, path: &str, cookie: Option<String>) -> common::Resp {
        let mut req = axum::http::Request::get(path)
            .header("x-webauth-user", "root")
            .header("x-grafana-org-id", "1")
            .header("authorization", "Basic cm9vdDpwdw==");
        if let Some(cookie) = cookie {
            req = req.header("cookie", cookie);
        }
        common::send(&self.app, req.body(Body::empty()).unwrap()).await
    }

    fn proxied(&self) -> usize {
        self.fake
            .lock()
            .unwrap()
            .log
            .iter()
            .filter(|(_, path, _, _)| !path.starts_with("/api/"))
            .count()
    }
}

#[tokio::test]
async fn signed_out_requests_get_grafanas_anonymous_access_without_any_identity() {
    let p = proxy(true).await;
    let r = p
        .get(
            "/grafana/d/abc/public",
            Some("grafana_session=stolen; grafana_session_expiry=1; theme=dark".into()),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let seen = r.json();
    assert_eq!(seen["path"], "/grafana/d/abc/public");
    assert_eq!(seen["user"], Json::Null);
    assert_eq!(seen["org"], Json::Null);
    assert_eq!(seen["authorization"], false);
    assert_eq!(seen["cookie"], "theme=dark");
    // A signed-out user's leftover session (the console cookie) is not a sign-in.
    let ghost = uuid::Uuid::new_v4();
    let r = p
        .get(
            "/grafana/",
            Some(format!("mrsm-session-id={ghost}; grafana_session=stolen")),
        )
        .await;
    assert_eq!(r.json()["user"], Json::Null);
    assert_eq!(r.json()["cookie"], Json::Null);
}

#[tokio::test]
async fn without_anonymous_access_signed_out_requests_go_to_the_login() {
    let p = proxy(false).await;
    let r = p
        .get(
            "/grafana/d/abc/public",
            Some("grafana_session=stolen".into()),
        )
        .await;
    assert!(r.status.is_redirection(), "{}", r.status);
    assert_eq!(r.headers.get("location").unwrap(), "/dash/login");
    assert_eq!(p.proxied(), 0);
}

#[tokio::test]
async fn grafanas_administrator_login_is_for_administrators_only() {
    let p = proxy(true).await;
    let session = p.sign_in("ROOT").await;
    let r = p
        .get("/grafana/", Some(format!("mrsm-session-id={session}")))
        .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    // Grafana is not contacted at all.
    assert!(p.fake.lock().unwrap().log.is_empty());
}

#[tokio::test]
async fn signed_in_members_are_named_and_land_in_their_teams_organization() {
    let p = proxy(true).await;
    let session = p.sign_in("ann").await;
    let r = p
        .get(
            "/grafana/",
            Some(format!("mrsm-session-id={session}; grafana_session=kept")),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let seen = r.json();
    assert_eq!(seen["user"], "ann");
    assert_eq!(seen["cookie"], "grafana_session=kept");
    assert_eq!(seen["org"], Json::Null);
    let f = p.fake.lock().unwrap();
    let ann = f.user_of("ann").unwrap();
    let wetlands = f.org_of("Wetlands").unwrap();
    assert!(f.orgs_of_user(ann).contains(&wetlands));
}
