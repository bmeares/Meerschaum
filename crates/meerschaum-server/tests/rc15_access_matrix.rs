//! rc15 wave 2, the access model over HTTP and MCP (contract K3; decisions AC1, AC2, AC3, AC4,
//! AC5, AC7, AC8, AC12, AC13): who sees and changes what.
//!
//! One instance with two teams and a principal of every kind:
//!
//! | user | role | teams |
//! |---|---|---|
//! | `admin` | administrator (user type `admin`) | |
//! | `vera` | viewer | Data |
//! | `ed` | editor (the default role: no permissions of their own) | Data |
//! | `opal` | operator | Data |
//! | `mia` | editor | Data (manager) |
//! | `otto` | editor | Ops |
//! | `oscar` | every permission (`scopes: ['*']`), not an administrator | Ops |
//!
//! and five pipes:
//!
//! | pipe | access |
//! |---|---|
//! | `plugin:data / temperature` | team Data |
//! | `plugin:data / secret` | owner `ed`, team Data, private |
//! | `plugin:ops / pressure` | team Ops |
//! | `plugin:ops / public` | owner `otto`, team Ops, visible to everyone, changed by its owner |
//! | `plugin:shared / legacy` | no `access` key (Everyone's, as every pipe before rc15) |

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use axum::Router;
use axum::http::StatusCode;
use common::fakes::RecordingJobs;
use common::{MemoryInstance, Resp, SECRET, login, req, send};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Role, Team, TeamRole};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_server::auth::{Principal, Scopes};
use meerschaum_server::backends::{ActionsBackend, InstanceResolver, MemoryEngine};
use meerschaum_server::mcp::{Caller, McpBackend, McpServer, SqlReadError};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};

const INSTANCE: &str = "sql:fixture";
const TEMPERATURE: (&str, &str) = ("plugin:data", "temperature");
const SECRET_PIPE: (&str, &str) = ("plugin:data", "secret");
const PRESSURE: (&str, &str) = ("plugin:ops", "pressure");
const PUBLIC: (&str, &str) = ("plugin:ops", "public");
const LEGACY: (&str, &str) = ("plugin:shared", "legacy");

fn map(v: Json) -> Map {
    serde_json::from_value::<Value>(v).unwrap().to_map()
}

/// Every action "succeeds" (the `/actions` route needs a list to check against).
struct Actions;

#[async_trait]
impl ActionsBackend for Actions {
    fn list(&self) -> Vec<String> {
        ["show", "sync", "sh", "python", "edit"]
            .iter()
            .map(|a| a.to_string())
            .collect()
    }
    async fn execute(&self, action: &str, _keywords: Map) -> SuccessTuple {
        SuccessTuple::ok(format!("ran {action}"))
    }
}

/// MCP's sync and SQL backends (never reached by a refused call).
#[derive(Default)]
struct Mcp {
    reads: AtomicUsize,
}

#[async_trait]
impl McpBackend for Mcp {
    async fn sync_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::ok("synced")
    }
    async fn verify_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::ok("verified")
    }
    async fn read_sql(&self, _: &str, _: &str, _: usize) -> Result<Vec<Value>, SqlReadError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Ok(vec![Value::Map(map(json!({"x": 1})))])
    }
}

struct Instances(Vec<(String, Arc<dyn InstanceConnector>)>);

impl InstanceResolver for Instances {
    fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
        self.0
            .iter()
            .find(|(key, _)| key == keys)
            .map(|(_, conn)| conn.clone())
    }
}

struct Fixture {
    app: Router,
    state: AppState,
    instance: Arc<MemoryInstance>,
    mcp: McpServer,
    _dir: tempfile::TempDir,
}

fn pipe(keys: (&str, &str)) -> Pipe {
    Pipe::builder(keys.0, keys.1)
        .instance(INSTANCE)
        .build()
        .unwrap()
}

fn pipe_ref(keys: (&str, &str)) -> String {
    format!("{INSTANCE}|{}|{}|None", keys.0, keys.1)
}

fn url(keys: (&str, &str), suffix: &str) -> String {
    format!("/pipes/{}/{}/None{suffix}", keys.0, keys.1)
}

async fn register(instance: &MemoryInstance, keys: (&str, &str), parameters: Json) {
    let mut p = pipe(keys);
    p.set_parameters(map(parameters));
    let res = instance.register_pipe(&p).await;
    assert!(res.success, "{res}");
}

async fn team(instance: &MemoryInstance, name: &str, members: &[(&str, TeamRole)]) {
    let mut t = Team::new(name);
    for (member, role) in members {
        t.add_member(member, *role);
    }
    let res = instance.register_team(&mut t).await;
    assert!(res.success, "{res}");
}

fn config_for(root: &std::path::Path) -> ServerConfig {
    let mut cfg = common::config(root);
    cfg.connectors = map(json!({
        "sql": {
            "main": {"flavor": "sqlite", "database": "/tmp/never-opened.db"},
            "secure": {"flavor": "sqlite", "database": "/tmp/never-opened-2.db", "teams": ["Data"]},
            "default": {},
        },
    }));
    cfg
}

async fn fixture_with(tweak: impl FnOnce(&mut ServerConfig)) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user_email("admin", "admin@example.com");
    for (name, attributes) in [
        ("vera", json!({"role": "viewer"})),
        ("ed", json!({})),
        ("opal", json!({"role": "operator"})),
        ("mia", json!({})),
        ("otto", json!({})),
        ("oscar", json!({"scopes": ["*"]})),
    ] {
        instance.seed_user(name, "pw", None);
        instance.seed_user_attributes(name, map(attributes));
        instance.seed_user_email(name, &format!("{name}@example.com"));
    }
    team(
        &instance,
        "Data",
        &[
            ("vera", TeamRole::Member),
            ("ed", TeamRole::Member),
            ("opal", TeamRole::Member),
            ("mia", TeamRole::Manager),
        ],
    )
    .await;
    team(
        &instance,
        "Ops",
        &[("otto", TeamRole::Member), ("oscar", TeamRole::Member)],
    )
    .await;
    register(
        &instance,
        TEMPERATURE,
        json!({"columns": {"datetime": "ts"}, "access": {"team": "Data"}}),
    )
    .await;
    register(
        &instance,
        SECRET_PIPE,
        json!({"access": {"owner": "ed", "team": "Data", "visibility": "private"}}),
    )
    .await;
    register(&instance, PRESSURE, json!({"access": {"team": "Ops"}})).await;
    register(
        &instance,
        PUBLIC,
        json!({"access": {"owner": "otto", "team": "Ops", "visibility": "everyone", "edit": "owner"}}),
    )
    .await;
    register(&instance, LEGACY, json!({"columns": {"datetime": "ts"}})).await;
    let mut cfg = config_for(dir.path());
    tweak(&mut cfg);
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        // These are explicit aliases of this fake backend, subject to the API's instance policy.
        .resolver(Arc::new(Instances(vec![
            ("sql:main".into(), instance.clone()),
            ("sql:secure".into(), instance.clone()),
        ])))
        .actions(Arc::new(Actions))
        .jobs(Arc::new(RecordingJobs::default()))
        .engine(Arc::new(MemoryEngine::default()))
        .build()
        .unwrap();
    let mcp = McpServer::for_state(&state).with_backend(Arc::new(Mcp::default()));
    Fixture {
        app: meerschaum_server::app(state.clone()),
        state,
        instance,
        mcp,
        _dir: dir,
    }
}

async fn fixture() -> Fixture {
    fixture_with(|_| {}).await
}

async fn token(f: &Fixture, user: &str) -> String {
    login(
        &f.app,
        user,
        if user == "admin" { "pass" } else { "pw" },
        None,
    )
    .await
}

/// An API key of `user` with `scopes`, made on the instance (as `mrsm register tokens` does,
/// with no cap on what it lists).
async fn key_for(f: &Fixture, user: &str, scopes: &[&str]) -> String {
    let mut t = meerschaum_core::users::Token::new(
        Some("test"),
        meerschaum_core::users::Expiration::Never,
        Some(scopes.iter().map(|s| s.to_string()).collect()),
        366,
        chrono::Utc::now(),
    );
    t.user_id = f
        .instance
        .get_user_id(&User::new(user, None))
        .await
        .unwrap();
    let res = f.instance.register_token(&mut t).await;
    assert!(res.success, "{res}");
    format!("Bearer {}", t.get_api_key())
}

async fn get(f: &Fixture, auth: &str, uri: &str) -> Resp {
    send(&f.app, req("GET", uri, Some(auth), None)).await
}

async fn call(f: &Fixture, method: &str, auth: &str, uri: &str, body: Option<Json>) -> Resp {
    let body = body.map(|b| ("application/json", b.to_string().into_bytes()));
    send(&f.app, req(method, uri, Some(auth), body)).await
}

/// `ck|mk` of every pipe a `GET /pipes/keys` answer lists.
fn listed(r: &Resp) -> BTreeSet<String> {
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let rows: Vec<Json> = match r.json() {
        Json::Array(rows) => rows,
        Json::Object(o) => o.into_values().collect(),
        other => panic!("unexpected keys: {other}"),
    };
    rows.iter()
        .map(|row| format!("{}|{}", row[0].as_str().unwrap(), row[1].as_str().unwrap()))
        .filter(|k| !k.starts_with("mrsm|"))
        .collect()
}

fn names(keys: &[(&str, &str)]) -> BTreeSet<String> {
    keys.iter().map(|(c, m)| format!("{c}|{m}")).collect()
}

/// A v4 `[success, message]` answer.
fn tuple_of(r: &Resp) -> (bool, String) {
    let j = r.json();
    (
        j[0].as_bool().unwrap_or(false),
        j[1].as_str().unwrap_or_default().to_string(),
    )
}

fn message(r: &Resp) -> String {
    let j = r.json();
    j.get("message")
        .or_else(|| j.get("detail"))
        .and_then(Json::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| r.text())
}

fn stored_access(f: &Fixture, keys: (&str, &str)) -> Json {
    let attrs = futures_block(f.instance.get_pipe_attributes(&pipe(keys))).unwrap();
    serde_json::to_value(
        attrs
            .get("parameters")
            .and_then(|p| p.get("access"))
            .cloned(),
    )
    .unwrap()
}

fn futures_block<F: std::future::Future>(future: F) -> F::Output {
    // The in-memory instance answers without awaiting anything.
    futures_util::FutureExt::now_or_never(future).expect("an in-memory read")
}

// ------------------------------------------------------------------------------------------------
// Visibility (AC4)
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn listings_show_each_principal_the_pipes_it_can_see() {
    let f = fixture().await;
    let expected: [(&str, &[(&str, &str)]); 7] = [
        (
            "admin",
            &[TEMPERATURE, SECRET_PIPE, PRESSURE, PUBLIC, LEGACY],
        ),
        ("vera", &[TEMPERATURE, PUBLIC, LEGACY]),
        // The owner of the private pipe sees it, and so does its team's manager.
        ("ed", &[TEMPERATURE, SECRET_PIPE, PUBLIC, LEGACY]),
        ("opal", &[TEMPERATURE, PUBLIC, LEGACY]),
        ("mia", &[TEMPERATURE, SECRET_PIPE, PUBLIC, LEGACY]),
        ("otto", &[PRESSURE, PUBLIC, LEGACY]),
        // Every permission is not every pipe: only administrators see everything.
        ("oscar", &[PRESSURE, PUBLIC, LEGACY]),
    ];
    for (user, keys) in expected {
        let auth = token(&f, user).await;
        let r = get(&f, &auth, "/pipes/keys").await;
        assert_eq!(listed(&r), names(keys), "{user}");
    }
    // The nested listings too.
    let otto = token(&f, "otto").await;
    let r = get(&f, &otto, "/pipes").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(r.json().get("plugin:data").is_none(), "{}", r.text());
    assert!(r.json().get("plugin:ops").is_some(), "{}", r.text());
    let r = get(&f, &otto, "/pipes/plugin:data").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    let r = get(&f, &otto, "/pipes/plugin:data/temperature").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    // Anonymous callers see nothing.
    let r = send(&f.app, req("GET", "/pipes/keys", None, None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_hidden_pipe_answers_like_a_missing_one_on_every_route() {
    let f = fixture().await;
    // `oscar` holds every permission but is not in Data.
    let oscar = token(&f, "oscar").await;
    for (method, suffix, body) in [
        ("GET", "", None),
        ("GET", "/attributes", None),
        ("GET", "/id", None),
        ("GET", "/exists", None),
        ("GET", "/data", None),
        ("GET", "/docs", None),
        ("GET", "/csv", None),
        ("GET", "/rowcount", None),
        ("GET", "/sync_time", None),
        ("GET", "/columns/types", None),
        ("GET", "/columns/indices", None),
        ("GET", "/size", None),
        ("GET", "/access", None),
        ("PATCH", "/edit", Some(json!({"tags": ["x"]}))),
        ("POST", "/data", Some(json!([{"ts": "2026-01-01"}]))),
        ("DELETE", "/clear", None),
        ("DELETE", "/drop", None),
        ("DELETE", "/delete", None),
        ("POST", "/lock", Some(json!({}))),
        ("POST", "/unlock", Some(json!({}))),
        ("PATCH", "/access", Some(json!({"visibility": "everyone"}))),
        ("POST", "/analyze", None),
    ] {
        let r = call(&f, method, &oscar, &url(TEMPERATURE, suffix), body).await;
        assert_eq!(
            r.status,
            StatusCode::NOT_FOUND,
            "{method} {suffix}: {}",
            r.text()
        );
        assert!(
            !r.text().contains("Data"),
            "{method} {suffix}: {}",
            r.text()
        );
    }
    // Private: a member who is neither the owner nor a manager.
    let vera = token(&f, "vera").await;
    let r = get(&f, &vera, &url(SECRET_PIPE, "/attributes")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    // Nothing changed.
    assert_eq!(stored_access(&f, TEMPERATURE)["team"], "Data");
    assert!(
        f.instance
            .get_pipe_id(&pipe(TEMPERATURE))
            .await
            .unwrap()
            .is_some()
    );
}

// ------------------------------------------------------------------------------------------------
// Roles and permissions (AC2)
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn roles_decide_what_each_user_may_do() {
    let f = fixture().await;
    let (vera, ed, opal, admin) = (
        token(&f, "vera").await,
        token(&f, "ed").await,
        token(&f, "opal").await,
        token(&f, "admin").await,
    );
    // A viewer reads.
    assert_eq!(
        get(&f, &vera, &url(TEMPERATURE, "/attributes"))
            .await
            .status,
        StatusCode::OK
    );
    let r = call(
        &f,
        "POST",
        &vera,
        &url(("plugin:data", "new"), "/register"),
        None,
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(message(&r), "Missing required scope: 'pipes:register'");
    let r = call(
        &f,
        "PATCH",
        &vera,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"tags": ["a"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(message(&r), "Missing required scope: 'pipes:write'");
    // An editor (the default role) changes pipes, but deletes no rows.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"tags": ["a"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(tuple_of(&r).0, "{}", r.text());
    for auth in [&ed, &opal] {
        let r = call(&f, "DELETE", auth, &url(TEMPERATURE, "/clear"), None).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN);
        assert_eq!(message(&r), "Missing required scope: 'pipes:delete'");
    }
    let r = call(&f, "DELETE", &admin, &url(TEMPERATURE, "/clear"), None).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());

    // Actions need `actions:execute` (operators); the console's pipe operations do not.
    let execute = |sysargs: Json| Some(json!({"sysargs": sysargs}));
    let r = call(
        &f,
        "POST",
        &ed,
        "/execute",
        execute(json!(["show", "version"])),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert_eq!(message(&r), "Missing required scope: 'actions:execute'");
    let r = call(
        &f,
        "POST",
        &opal,
        "/execute",
        execute(json!(["show", "version"])),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        "/execute",
        execute(json!([
            "sync",
            "pipes",
            "-c",
            "plugin:data",
            "-m",
            "temperature"
        ])),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // ... on pipes the caller may change: every pipe includes a hidden one and one whose owner
    // keeps the changes.
    let r = call(
        &f,
        "POST",
        &ed,
        "/execute",
        execute(json!(["sync", "pipes"])),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        message(&r).contains("selects 2 pipes you may not change"),
        "{}",
        r.text()
    );
    // A viewer runs nothing.
    let r = call(
        &f,
        "POST",
        &vera,
        "/execute",
        execute(json!(["sync", "pipes", "-c", "plugin:shared"])),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    // `/actions/{action}`: the console's policy and the same pipe checks for non-administrators.
    let r = call(
        &f,
        "POST",
        &ed,
        "/actions/show",
        Some(json!({"action": ["version"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &opal,
        "/actions/sh",
        Some(json!({"action": ["id"]})),
    )
    .await;
    assert!(!tuple_of(&r).0, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &opal,
        "/actions/show",
        Some(json!({"action": ["config"]})),
    )
    .await;
    assert!(!tuple_of(&r).0, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &opal,
        "/actions/show",
        Some(json!({"action": ["pipes"]})),
    )
    .await;
    let (ok, text) = tuple_of(&r);
    assert!(!ok && text.contains("you may not see"), "{}", r.text());
    let r = call(
        &f,
        "POST",
        &opal,
        "/actions/show",
        Some(json!({"action": ["pipes"], "connector_keys": ["plugin:shared"]})),
    )
    .await;
    assert_eq!(tuple_of(&r), (true, "ran show".to_string()), "{}", r.text());
    let r = call(
        &f,
        "POST",
        &admin,
        "/actions/sh",
        Some(json!({"action": ["id"]})),
    )
    .await;
    assert_eq!(tuple_of(&r), (true, "ran sh".to_string()), "{}", r.text());
}

#[tokio::test]
async fn a_pipe_changed_by_its_owner_is_still_seen_by_everyone() {
    let f = fixture().await;
    let (ed, otto, admin) = (
        token(&f, "ed").await,
        token(&f, "otto").await,
        token(&f, "admin").await,
    );
    let edit = Some(json!({"tags": ["b"]}));
    assert_eq!(
        get(&f, &ed, &url(PUBLIC, "/attributes")).await.status,
        StatusCode::OK
    );
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(PUBLIC, "/edit?patch=true"),
        edit.clone(),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("You cannot change"), "{}", r.text());
    let r = get(&f, &ed, &url(PUBLIC, "/access")).await;
    assert_eq!(
        r.json()["may"],
        json!({"edit": false, "manage_access": false, "lock": false})
    );
    for auth in [&otto, &admin] {
        let r = call(
            &f,
            "PATCH",
            auth,
            &url(PUBLIC, "/edit?patch=true"),
            edit.clone(),
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
        assert!(tuple_of(&r).0, "{}", r.text());
    }
}

// ------------------------------------------------------------------------------------------------
// New pipes, access and locks (AC4, AC5)
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn new_pipes_belong_to_their_creator_and_primary_team() {
    let f = fixture().await;
    let (ed, otto, admin) = (
        token(&f, "ed").await,
        token(&f, "otto").await,
        token(&f, "admin").await,
    );
    // An owner named by someone else than an administrator is the caller anyway.
    let r = call(
        &f,
        "POST",
        &ed,
        &url(("plugin:data", "fresh"), "/register"),
        Some(json!({"columns": {"datetime": "ts"}, "access": {"owner": "otto"}})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    let access = stored_access(&f, ("plugin:data", "fresh"));
    assert_eq!(access["owner"], "ed");
    assert_eq!(access["team"], "Data");
    // Only teams of one's own.
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("plugin:ops", "fresh"), "/register"),
        Some(json!({"access": {"team": "Data"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert_eq!(message(&r), "You are not a member of the team 'Data'.");
    let r = call(
        &f,
        "POST",
        &ed,
        &url(("plugin:data", "typo"), "/register"),
        Some(json!({"access": {"visibility": "secret"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
    // An administrator gives a pipe to anyone.
    let r = call(
        &f,
        "POST",
        &admin,
        &url(("plugin:ops", "given"), "/register"),
        Some(json!({"access": {"owner": "otto", "team": "Ops"}})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    let access = stored_access(&f, ("plugin:ops", "given"));
    assert_eq!(
        (access["owner"].clone(), access["team"].clone()),
        (json!("otto"), json!("Ops"))
    );
}

#[tokio::test]
async fn access_and_locks_are_changed_by_the_owner_the_managers_and_administrators() {
    let f = fixture().await;
    let (vera, ed, mia) = (
        token(&f, "vera").await,
        token(&f, "ed").await,
        token(&f, "mia").await,
    );
    let r = get(&f, &ed, &url(TEMPERATURE, "/access")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["success"], true);
    assert_eq!(r.json()["access"]["team"], "Data");
    assert_eq!(
        r.json()["may"],
        json!({"edit": true, "manage_access": false, "lock": false})
    );
    // A member who is not the owner changes neither the access nor the locks.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/access"),
        Some(json!({"visibility": "private"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        &url(TEMPERATURE, "/lock"),
        Some(json!({"locks": ["sync"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    // Nor through a parameter edit.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"locks": ["all"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"access": {"team": "Data", "visibility": "everyone"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    // A viewer does not even hold `pipes:write`.
    let r = call(&f, "POST", &vera, &url(TEMPERATURE, "/lock"), None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    // The team's manager does.
    let r = call(
        &f,
        "POST",
        &mia,
        &url(TEMPERATURE, "/lock"),
        Some(json!({"locks": ["sync"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["locks"], json!(["sync"]));
    // The lock refuses everyone's syncs, and says how to unlock.
    let r = call(
        &f,
        "POST",
        &ed,
        &url(TEMPERATURE, "/data"),
        Some(json!([{"ts": "2026-01-01"}])),
    )
    .await;
    let (ok, text) = tuple_of(&r);
    assert!(
        !ok && text.contains("it is locked (sync)") && text.contains("mrsm unlock pipes"),
        "{}",
        r.text()
    );
    // Other edits still work under a `sync` lock.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"tags": ["c"]})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // A `parameters` lock refuses edits but not the change of the locks themselves.
    let r = call(
        &f,
        "POST",
        &mia,
        &url(TEMPERATURE, "/lock"),
        Some(json!({"locks": "parameters"})),
    )
    .await;
    assert_eq!(
        r.json()["locks"],
        json!(["parameters", "sync"]),
        "{}",
        r.text()
    );
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(TEMPERATURE, "/edit?patch=true"),
        Some(json!({"tags": ["d"]})),
    )
    .await;
    let (ok, text) = tuple_of(&r);
    assert!(
        !ok && text.contains("it is locked (parameters)"),
        "{}",
        r.text()
    );
    let r = call(&f, "POST", &mia, &url(TEMPERATURE, "/unlock"), None).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["locks"], json!([]));

    // The manager makes the pipe private: the members who are not managers no longer see it.
    let r = call(
        &f,
        "PATCH",
        &mia,
        &url(TEMPERATURE, "/access"),
        Some(json!({"visibility": "private"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(
        get(&f, &vera, &url(TEMPERATURE, "/attributes"))
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    // ... and gives it only to teams they are in.
    let r = call(
        &f,
        "PATCH",
        &mia,
        &url(TEMPERATURE, "/access"),
        Some(json!({"team": "Ops"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    // The owner gives up ownership but gives the pipe to nobody else.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(SECRET_PIPE, "/access"),
        Some(json!({"owner": "vera"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(SECRET_PIPE, "/access"),
        Some(json!({"owner": null})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(stored_access(&f, SECRET_PIPE)["owner"], Json::Null);
    // No longer its owner, `ed` (a member, not a manager) cannot see the private pipe.
    assert_eq!(
        get(&f, &ed, &url(SECRET_PIPE, "/attributes")).await.status,
        StatusCode::NOT_FOUND
    );
}

// ------------------------------------------------------------------------------------------------
// Teams (AC3, AC12)
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn teams_are_created_by_administrators_and_staffed_by_their_managers() {
    let f = fixture().await;
    let (vera, mia, otto, admin) = (
        token(&f, "vera").await,
        token(&f, "mia").await,
        token(&f, "otto").await,
        token(&f, "admin").await,
    );
    let r = get(&f, &vera, "/teams").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let team_names: Vec<String> = r.json()["teams"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(team_names, ["Everyone", "Data", "Ops"]);
    // Creating, renaming and deleting teams is the administrators'.
    let r = call(&f, "POST", &mia, "/teams/Lab/register", Some(json!({}))).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &admin,
        "/teams/Lab/register",
        Some(json!({"members": ["vera"], "managers": ["otto"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["team"]["managers"], json!(["otto"]));
    let r = call(&f, "POST", &admin, "/teams/lab/register", Some(json!({}))).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &admin,
        "/teams/Everyone/register",
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
    // A manager adds and removes members of their team, and nothing else.
    let r = get(&f, &otto, &url(TEMPERATURE, "/attributes")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = call(
        &f,
        "PATCH",
        &mia,
        "/teams/Data/edit",
        Some(json!({"add_members": ["otto"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // The new member sees the team's pipes at once (the server forgets what it cached).
    let r = get(&f, &otto, &url(TEMPERATURE, "/attributes")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &mia,
        "/teams/Data/edit",
        Some(json!({"managers": ["mia", "otto"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &mia,
        "/teams/Ops/edit",
        Some(json!({"add_members": ["vera"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &mia,
        "/teams/Data/edit",
        Some(json!({"remove_members": ["otto"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(
        get(&f, &otto, &url(TEMPERATURE, "/attributes"))
            .await
            .status,
        StatusCode::NOT_FOUND
    );
    // Renaming a team renames it in its pipes; deleting one gives them to Everyone.
    let r = call(
        &f,
        "POST",
        &admin,
        "/teams/Ops/rename",
        Some(json!({"name": "Operations"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(stored_access(&f, PRESSURE)["team"], "Operations");
    let r = get(&f, &admin, "/teams/Operations?counts=true").await;
    assert_eq!(r.json()["pipes"], json!(2), "{}", r.text());
    let r = call(&f, "DELETE", &admin, "/teams/Operations/delete", None).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(stored_access(&f, PRESSURE)["team"], Json::Null);
    let r = call(&f, "DELETE", &admin, "/teams/Everyone/delete", None).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
    let r = call(&f, "DELETE", &mia, "/teams/Data/delete", None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

// ------------------------------------------------------------------------------------------------
// Users: permissions, roles, emails (AC2, AC12, AC13)
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn emails_are_shown_to_their_owner_administrators_and_team_managers() {
    let f = fixture().await;
    let emails = |r: &Resp| -> Vec<(String, Json)> {
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
        r.json()["users"]
            .as_array()
            .unwrap()
            .iter()
            .map(|u| {
                (
                    u["username"].as_str().unwrap().to_string(),
                    u["email"].clone(),
                )
            })
            .collect()
    };
    let visible = |rows: &[(String, Json)]| -> Vec<String> {
        rows.iter()
            .filter(|(_, e)| !e.is_null())
            .map(|(u, _)| u.clone())
            .collect()
    };
    let vera = token(&f, "vera").await;
    assert_eq!(
        visible(&emails(&get(&f, &vera, "/users/permissions").await)),
        ["vera"]
    );
    let mia = token(&f, "mia").await;
    assert_eq!(
        visible(&emails(&get(&f, &mia, "/users/permissions").await)),
        ["ed", "mia", "opal", "vera"]
    );
    let admin = token(&f, "admin").await;
    assert_eq!(
        visible(&emails(&get(&f, &admin, "/users/permissions").await)).len(),
        7
    );
    let r = get(&f, &mia, "/users/otto/permissions").await;
    assert_eq!(r.json()["user"]["email"], Json::Null, "{}", r.text());
    let otto = token(&f, "otto").await;
    let r = get(&f, &otto, "/users/otto/permissions").await;
    assert_eq!(r.json()["user"]["email"], "otto@example.com");
    // The rows name the roles.
    let roles: Vec<(String, String)> = get(&f, &admin, "/users/permissions").await.json()["users"]
        .as_array()
        .unwrap()
        .iter()
        .map(|u| {
            (
                u["username"].as_str().unwrap().into(),
                u["role"].as_str().unwrap().into(),
            )
        })
        .collect();
    for (user, role) in [
        ("admin", "admin"),
        ("vera", "viewer"),
        ("ed", "editor"),
        ("opal", "operator"),
    ] {
        assert!(
            roles.contains(&(user.into(), role.into())),
            "{user}: {roles:?}"
        );
    }
}

#[tokio::test]
async fn permissions_change_through_the_api_and_reach_existing_credentials() {
    let f = fixture().await;
    let (vera, ed, admin) = (
        token(&f, "vera").await,
        token(&f, "ed").await,
        token(&f, "admin").await,
    );
    let r = call(
        &f,
        "POST",
        &ed,
        "/tokens/register",
        Some(json!({"label": "k", "scopes": ["pipes:read", "pipes:write"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let key = format!("Bearer {}", r.json()["api_key"].as_str().unwrap());
    let edit = Some(json!({"tags": ["k"]}));
    let r = call(
        &f,
        "PATCH",
        &key,
        &url(LEGACY, "/edit?patch=true"),
        edit.clone(),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // Only administrators change rights; a user may change their own primary team.
    let r = call(
        &f,
        "PATCH",
        &vera,
        "/users/ed/permissions",
        Some(json!({"role": "operator"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        "/users/ed/permissions",
        Some(json!({"role": "operator"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        "/users/ed/permissions",
        Some(json!({"primary_team": "Ops"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &ed,
        "/users/ed/permissions",
        Some(json!({"primary_team": "Data"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // A user edits their own attributes, never the ones that hold rights: refused, not dropped.
    for attributes in [
        json!({"scopes": ["*"], "color": "blue"}),
        json!({"role": "operator"}),
    ] {
        let r = call(
            &f,
            "PATCH",
            &ed,
            "/users/ed/permissions",
            Some(json!({"attributes": attributes})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        assert_eq!(
            message(&r),
            "Only administrators may change a user's role or permissions."
        );
    }
    let r = call(
        &f,
        "PATCH",
        &ed,
        "/users/ed/permissions",
        Some(json!({"attributes": {"color": "blue"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let stored = f.instance.user_attributes("ed").unwrap();
    assert!(
        stored.get("scopes").is_none() && stored.get("role").is_none(),
        "{stored:?}"
    );
    assert_eq!(stored.get("color"), Some(&Value::Str("blue".into())));
    // v4's form route refuses the same.
    let r = send(
        &f.app,
        req(
            "POST",
            "/users/edit",
            Some(&ed),
            form(&[("username", "ed"), ("attributes", r#"{"scopes": ["*"]}"#)]),
        ),
    )
    .await;
    assert_eq!(
        tuple_of(&r),
        (
            false,
            "Only administrators may change a user's role or permissions.".to_string()
        ),
        "{}",
        r.text()
    );

    // An administrator makes `ed` a viewer: the key and the session lose `pipes:write` at once.
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"role": "viewer"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["user"]["role"], "viewer");
    for auth in [&key, &ed] {
        let r = call(
            &f,
            "PATCH",
            auth,
            &url(LEGACY, "/edit?patch=true"),
            edit.clone(),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        assert_eq!(message(&r), "Missing required scope: 'pipes:write'");
    }
    // Explicit permissions that match no preset are a custom role.
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"permissions": ["pipes:read", "webterm:use"]})),
    )
    .await;
    assert_eq!(r.json()["user"]["role"], "custom", "{}", r.text());
    assert_eq!(
        r.json()["user"]["permissions"],
        json!(["pipes:read", "webterm:use"])
    );
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"role": "admin"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
}

#[tokio::test]
async fn edited_permission_grids_do_not_regain_unchecked_permissions() {
    let f = fixture().await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let key = key_for(&f, "ed", &["*"]).await;
    for permissions in [vec!["pipes:write"], vec!["actions:execute"]] {
        let r = call(
            &f,
            "PATCH",
            &admin,
            "/users/ed/permissions",
            Some(json!({"permissions": permissions})),
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
        assert_eq!(r.json()["user"]["permissions"], json!(permissions));
        assert_eq!(r.json()["user"]["role"], "custom");
        let stored = f.instance.user_attributes("ed").unwrap();
        assert_eq!(stored.get("role"), Some(&Value::Str("custom".into())));
        let fresh = token(&f, "ed").await;
        for auth in [&ed, &key, &fresh] {
            let mut headers = axum::http::HeaderMap::new();
            headers.insert(axum::http::header::AUTHORIZATION, auth.parse().unwrap());
            let access = meerschaum_server::access::resolve(&f.state, &headers)
                .await
                .unwrap();
            assert!(
                !access.has("pipes:register")
                    && !access.has("pipes:define_sql")
                    && !access.has("webterm:use")
            );
            assert!(!access.has("*"));
            let r = call(
                &f,
                "POST",
                auth,
                &url(("plugin:data", "grid-new"), "/register"),
                Some(json!({})),
            )
            .await;
            assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
            let r = call(
                &f,
                "POST",
                auth,
                "/pipelines/grid-new/register",
                Some(json!({"pipeline": {"name": "grid-new", "targets": []}})),
            )
            .await;
            assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        }
        let caller = Caller::api(
            Scopes::List(permissions.iter().map(|p| p.to_string()).collect()),
            Some(Principal::User(User::new("ed", None))),
        );
        let r = tool(
            &f,
            &caller,
            "register_pipe",
            json!({"connector_keys": "plugin:data", "metric_key": "grid-new"}),
        )
        .await;
        assert!(
            r.as_ref().is_err_and(|e| e.contains("pipes:register")),
            "{r:?}"
        );
    }
    // Registration is independent of editing for new permission lists, including MCP.
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"permissions": ["pipes:register"]})),
    )
    .await;
    assert_eq!(r.json()["user"]["permissions"], json!(["pipes:register"]));
    let fresh = token(&f, "ed").await;
    let r = call(
        &f,
        "POST",
        &fresh,
        &url(("plugin:data", "grid-register"), "/register"),
        Some(json!({})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    let caller = Caller::api(
        Scopes::List(vec!["pipes:register".into()]),
        Some(Principal::User(User::new("ed", None))),
    );
    let r = tool(
        &f,
        &caller,
        "register_pipe",
        json!({"connector_keys": "plugin:data", "metric_key": "grid-mcp"}),
    )
    .await;
    assert!(r.is_ok(), "{r:?}");
    // A genuine v4 scopes-only record retains the old meaning of pipes:write.
    f.instance
        .seed_user_attributes("ed", map(json!({"scopes": ["pipes:write"]})));
    f.state.access_cache().forget_identities();
    let legacy = token(&f, "ed").await;
    let r = call(
        &f,
        "POST",
        &legacy,
        &url(("plugin:data", "legacy-register"), "/register"),
        Some(json!({})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
}

#[tokio::test]
async fn revoking_every_permission_revokes_existing_credentials() {
    let f = fixture().await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let key = key_for(&f, "ed", &["*"]).await;
    assert_eq!(get(&f, &ed, "/pipes/keys").await.status, StatusCode::OK);
    let r = call(
        &f,
        "PATCH",
        &admin,
        "/users/ed/permissions",
        Some(json!({"permissions": []})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["user"]["permissions"], json!([]));
    for auth in [&ed, &key] {
        assert_eq!(
            get(&f, auth, "/pipes/keys").await.status,
            StatusCode::FORBIDDEN
        );
        assert_eq!(get(&f, auth, "/teams").await.status, StatusCode::FORBIDDEN);
    }
}

#[tokio::test]
async fn a_credential_never_holds_more_than_its_owner() {
    let f = fixture().await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    // `ed` (an editor) cannot mint a key with more than they hold.
    let r = call(
        &f,
        "POST",
        &ed,
        "/tokens/register",
        Some(json!({"label": "x", "scopes": ["actions:execute"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    // A `*` key made on the instance for `ed`'s account still holds only what `ed` holds when
    // it is used.
    let key = key_for(&f, "ed", &["*"]).await;
    let r = call(
        &f,
        "POST",
        &key,
        "/execute",
        Some(json!({"sysargs": ["show", "version"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert_eq!(message(&r), "Missing required scope: 'actions:execute'");
    // An administrator's narrow key holds its scopes only (and sees every pipe).
    let r = call(
        &f,
        "POST",
        &admin,
        "/tokens/register",
        Some(json!({"label": "ro", "scopes": ["pipes:read"]})),
    )
    .await;
    let ro = format!("Bearer {}", r.json()["api_key"].as_str().unwrap());
    assert_eq!(listed(&get(&f, &ro, "/pipes/keys").await).len(), 5);
    let r = call(
        &f,
        "PATCH",
        &ro,
        &url(LEGACY, "/edit?patch=true"),
        Some(json!({"tags": []})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

// ------------------------------------------------------------------------------------------------
// First run and registration (AC1)
// ------------------------------------------------------------------------------------------------

fn form(fields: &[(&str, &str)]) -> Option<(&'static str, Vec<u8>)> {
    let body: Vec<String> = fields
        .iter()
        .map(|(k, v)| format!("{k}={}", meerschaum_connector_api::wire::quote_plus(v)))
        .collect();
    Some((
        "application/x-www-form-urlencoded",
        body.join("&").into_bytes(),
    ))
}

fn empty_state(
    tweak: impl FnOnce(&mut ServerConfig),
) -> (Router, Arc<MemoryInstance>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(MemoryInstance::new("fixture"));
    let mut cfg = common::config(dir.path());
    tweak(&mut cfg);
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    (meerschaum_server::app(state), instance, dir)
}

#[tokio::test]
async fn the_first_account_is_an_administrator_and_then_registration_is_closed() {
    let (app, instance, _dir) = empty_state(|_| {});
    let register = |fields: &[(&str, &str)], auth: Option<&str>| {
        req("POST", "/users/register", auth, form(fields))
    };
    let r = send(
        &app,
        register(&[("username", "first"), ("password", "first-pass")], None),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let first = User::new("first", None);
    assert_eq!(
        instance.get_user_type(&first).await.unwrap().as_deref(),
        Some("admin")
    );
    // Registration is now closed (`api:permissions:registration:users` ships `false`).
    let r = send(
        &app,
        register(&[("username", "second"), ("password", "second-pass")], None),
    )
    .await;
    assert_eq!(r.json()[0], json!(false), "{}", r.text());
    assert!(
        r.json()[1]
            .as_str()
            .unwrap()
            .contains("has not allowed user registration")
    );
    // The administrator still registers users, administrators included.
    let admin = login(&app, "first", "first-pass", None).await;
    let r = send(
        &app,
        register(
            &[
                ("username", "third"),
                ("password", "third-pass"),
                ("type", "admin"),
            ],
            Some(&admin),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    assert_eq!(
        instance
            .get_user_type(&User::new("third", None))
            .await
            .unwrap()
            .as_deref(),
        Some("admin")
    );
}

#[tokio::test]
async fn concurrent_account_deletions_keep_an_administrator() {
    let f = fixture().await;
    f.instance.seed_user("second", "pw", Some("admin"));
    let (admin, second) = (token(&f, "admin").await, token(&f, "second").await);
    let (a, b) = tokio::join!(
        call(&f, "DELETE", &admin, "/users/admin", None),
        call(&f, "DELETE", &second, "/users/second", None),
    );
    assert_ne!(
        tuple_of(&a).0,
        tuple_of(&b).0,
        "{} / {}",
        a.text(),
        b.text()
    );
    let mut remaining = 0;
    for username in f.instance.get_users().await.unwrap() {
        remaining += usize::from(
            f.instance
                .get_user_type(&User::new(&username, None))
                .await
                .unwrap()
                .as_deref()
                == Some("admin"),
        );
    }
    assert_eq!(remaining, 1);
}

#[tokio::test]
async fn open_registration_gives_the_default_role_and_never_rights() {
    let (app, instance, _dir) = empty_state(|cfg| cfg.registration_users = true);
    instance.seed_user("admin", "pass", Some("admin"));
    let fields = [
        ("username", "newbie"),
        ("password", "newbie-pass"),
        (
            "attributes",
            r#"{"scopes": ["*"], "role": "operator", "color": "red"}"#,
        ),
    ];
    let r = send(&app, req("POST", "/users/register", None, form(&fields))).await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let attributes = instance.user_attributes("newbie").unwrap();
    assert!(
        attributes.get("scopes").is_none() && attributes.get("role").is_none(),
        "{attributes:?}"
    );
    assert_eq!(attributes.get("color"), Some(&Value::Str("red".into())));
    for user_type in ["admin", "Admin", " ADMIN "] {
        let r = send(
            &app,
            req(
                "POST",
                "/users/register",
                None,
                form(&[
                    ("username", "boss"),
                    ("password", "boss-pass"),
                    ("type", user_type),
                ]),
            ),
        )
        .await;
        assert_eq!(r.json()[0], json!(false), "{user_type}: {}", r.text());
    }
    // The new user is an editor (the default role).
    let newbie = login(&app, "newbie", "newbie-pass", None).await;
    let r = send(
        &app,
        req("GET", "/users/newbie/permissions", Some(&newbie), None),
    )
    .await;
    assert_eq!(r.json()["user"]["role"], "editor", "{}", r.text());
}

// ------------------------------------------------------------------------------------------------
// The SQL guard and reserved connectors (AC8)
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn implicit_targets_cannot_alias_hidden_or_internal_tables() {
    let f = fixture().await;
    let otto = token(&f, "otto").await;
    for keys in [
        ("plugin", "data_temperature"),
        ("mrsm", "users"),
        ("mrsm", "tokens"),
    ] {
        let r = call(&f, "POST", &otto, &url(keys, "/register"), Some(json!({}))).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{keys:?}: {}", r.text());
    }
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("plugin:ops", "aliased"), "/register"),
        Some(json!({"target": "{{ self.parameters['destination'] }}", "destination": "plugin_data_temperature"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn raw_internal_pipes_cannot_bypass_their_filtered_routes() {
    let f = fixture().await;
    register(
        &f.instance,
        ("plugin:shared", "raw_engine"),
        json!({"target": "mrsm_engine_nodes"}),
    )
    .await;
    register(
        &f.instance,
        ("plugin:shared", "raw_teams"),
        json!({"target": "mrsm_teams"}),
    )
    .await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    for keys in [
        ("plugin:shared", "raw_engine"),
        ("plugin:shared", "raw_teams"),
    ] {
        for suffix in ["/attributes", "/data", "/docs", "/csv", "/rowcount"] {
            let r = get(&f, &ed, &url(keys, suffix)).await;
            assert_eq!(
                r.status,
                StatusCode::NOT_FOUND,
                "{keys:?}{suffix}: {}",
                r.text()
            );
        }
    }
    let r = get(&f, &ed, "/pipes/keys").await;
    assert!(
        !r.text().contains("raw_engine") && !r.text().contains("raw_teams"),
        "{}",
        r.text()
    );
    let r = get(&f, &ed, "/pipes").await;
    assert!(
        !r.text().contains("raw_engine") && !r.text().contains("raw_teams"),
        "{}",
        r.text()
    );
    assert_eq!(
        get(
            &f,
            &admin,
            &url(("plugin:shared", "raw_engine"), "/attributes")
        )
        .await
        .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn definitions_never_reach_hidden_or_internal_tables() {
    let f = fixture_with(|cfg| {
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec![INSTANCE.into(), "sql:main".into()];
    })
    .await;
    let otto = token(&f, "otto").await;
    let mut n = 0;
    let mut try_definition = async |auth: &str, definition: &str| {
        n += 1;
        call(
            &f,
            "POST",
            auth,
            &url(("sql:main", &format!("peek{n}")), "/register"),
            Some(json!({"fetch": {"definition": definition}})),
        )
        .await
    };
    for definition in [
        "SELECT * FROM plugin_data_temperature",
        "SELECT * FROM \"plugin_data_temperature\"",
        "SELECT * FROM [dbo].[plugin_data_temperature]",
        "SELECT * FROM public.Plugin_Data_Temperature",
        "SELECT * FROM `plugin_data_temperature` -- a comment",
        "SELECT * FROM {{ Pipe('plugin:data', 'temperature', instance='sql:fixture') }}",
        "SELECT * FROM mrsm_users",
        "SELECT * FROM \"mrsm_tokens\"",
        "SELECT table_name FROM information_schema.tables",
        "SELECT 'MRSM{meerschaum:connectors:sql:main:database}' AS x",
    ] {
        let r = try_definition(&otto, definition).await;
        assert_eq!(
            r.status,
            StatusCode::FORBIDDEN,
            "{definition}: {}",
            r.text()
        );
    }
    let r = try_definition(&otto, "SELECT * FROM plugin_ops_pressure").await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // A team member reads their team's table.
    let ed = token(&f, "ed").await;
    let r = try_definition(&ed, "SELECT * FROM plugin_data_temperature").await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // A target never names a hidden or internal table.
    for target in ["plugin_data_temperature", "mrsm_users"] {
        let r = call(
            &f,
            "POST",
            &otto,
            &url(("plugin:ops", target), "/register"),
            Some(json!({"target": target})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{target}: {}", r.text());
    }
    // Setting a definition needs `pipes:define_sql` (a list naming the 5.0 permissions is taken
    // as written; `pipes:write` alone, as written before 5.0, still grants it).
    let key = key_for(&f, "otto", &["pipes:read", "pipes:register", "pipes:write"]).await;
    let r = try_definition(&key, "SELECT 1 AS one").await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("pipes:define_sql"), "{}", r.text());
    let r = call(
        &f,
        "POST",
        &key,
        &url(("plugin:ops", "plain"), "/register"),
        Some(json!({})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // An edit is checked like a registration.
    let r = call(
        &f,
        "PATCH",
        &otto,
        &url(PRESSURE, "/edit?patch=true"),
        Some(json!({"sql": "SELECT * FROM \"plugin_data_secret\""})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn definitions_use_the_sources_visibility_byte_limit_and_instance_policy() {
    let mut f = fixture_with(|cfg| {
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec![INSTANCE.into(), "sql:other".into()];
    })
    .await;
    let other = Arc::new(MemoryInstance::new("other").with_identifier_byte_limit(63));
    register(
        &other,
        LEGACY,
        json!({"access": {"owner": "ed", "visibility": "private"}}),
    )
    .await;
    register(&other, PUBLIC, json!({})).await;
    let unicode = "界".repeat(21);
    register(
        &other,
        ("plugin:source", "unicode"),
        json!({"target": unicode, "access": {"owner": "ed", "visibility": "private"}}),
    )
    .await;
    let resolver = Arc::new(Instances(vec![("sql:other".into(), other)]));
    f.state = AppState::builder((*f.state.config).clone(), f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver.clone())
        .build()
        .unwrap();
    f.app = meerschaum_server::app(f.state.clone());
    f.mcp = McpServer::for_state(&f.state).with_backend(Arc::new(Mcp::default()));
    let otto = token(&f, "otto").await;
    // The same table on the destination is public, including as a visible template reference.
    for (metric, definition) in [
        ("raw", "SELECT * FROM plugin_shared_legacy".to_string()),
        (
            "resolved",
            "SELECT * FROM {{ Pipe('plugin:shared', 'legacy', instance='sql:fixture') }}"
                .to_string(),
        ),
        (
            "unicode",
            format!("SELECT * FROM \"public\".\"{unicode}different_suffix\""),
        ),
    ] {
        let r = call(
            &f,
            "POST",
            &otto,
            &url(("sql:other", metric), "/register"),
            Some(json!({"fetch": {"definition": definition}})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{metric}: {}", r.text());
        assert!(message(&r).contains("cannot see"), "{metric}: {}", r.text());
    }
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("sql:other", "allowed"), "/register"),
        Some(json!({"fetch": {"definition": "SELECT * FROM plugin_ops_public"}})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // A destination target is checked against the destination, even if it is private only at source.
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("sql:other", "destination_target"), "/register"),
        Some(json!({"target": "plugin_shared_legacy", "fetch": {"definition": "SELECT * FROM plugin_ops_public"}})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    // MCP registration shares the same source guard.
    let r = tool(
        &f,
        &caller("otto", Role::Operator),
        "register_pipe",
        json!({"connector_keys": "sql:other", "metric_key": "mcp_source", "parameters": {
            "fetch": {"definition": "SELECT * FROM plugin_shared_legacy"}
        }}),
    )
    .await;
    assert!(r.as_ref().is_err_and(|e| e.contains("cannot see")), "{r:?}");
    let narrow_admin = key_for(
        &f,
        "admin",
        &["pipes:read", "pipes:write", "pipes:register"],
    )
    .await;
    let r = call(
        &f,
        "POST",
        &narrow_admin,
        &url(("sql:other", "narrow_admin"), "/register"),
        Some(json!({"fetch": {"definition": "SELECT 1"}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("pipes:define_sql"), "{}", r.text());
    let mut cfg = (*f.state.config).clone();
    cfg.allowed_instance_keys = vec![INSTANCE.into()];
    f.state = AppState::builder(cfg, f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver)
        .build()
        .unwrap();
    f.app = meerschaum_server::app(f.state.clone());
    let admin = token(&f, "admin").await;
    for auth in [&otto, &admin] {
        let r = call(
            &f,
            "POST",
            auth,
            &url(("sql:other", "unserved_source"), "/register"),
            Some(json!({"fetch": {"definition": "SELECT 1"}})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        assert!(
            message(&r).contains("allowed instances patterns"),
            "{}",
            r.text()
        );
    }
}

#[tokio::test]
async fn a_connector_reserved_for_teams_serves_only_their_pipes() {
    let f = fixture().await;
    let (ed, otto, admin) = (
        token(&f, "ed").await,
        token(&f, "otto").await,
        token(&f, "admin").await,
    );
    let sql_labels = |r: Resp| -> Vec<String> {
        r.json()["sql"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_str().unwrap().to_string())
            .collect()
    };
    assert_eq!(sql_labels(get(&f, &otto, "/connectors").await), ["main"]);
    assert_eq!(
        sql_labels(get(&f, &ed, "/connectors").await),
        ["main", "secure"]
    );
    let r = call(
        &f,
        "POST",
        &otto,
        &url(("sql:secure", "x"), "/register"),
        Some(json!({})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("reserved"), "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        &url(("sql:secure", "y"), "/register"),
        Some(json!({})),
    )
    .await;
    assert!(tuple_of(&r).0, "{}", r.text());
    assert_eq!(stored_access(&f, ("sql:secure", "y"))["team"], "Data");
    // Its pipes stay with the team.
    let r = call(
        &f,
        "PATCH",
        &ed,
        &url(("sql:secure", "y"), "/access"),
        Some(json!({"team": null})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(message(&r).contains("'Data'"), "{}", r.text());
    let r = call(
        &f,
        "PATCH",
        &admin,
        &url(("sql:secure", "y"), "/access"),
        Some(json!({"team": null})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
}

// ------------------------------------------------------------------------------------------------
// Derived objects: Pipelines, runs, sync statuses (AC7)
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn pipelines_count_hidden_members_and_runs_need_edit_rights() {
    let f = fixture().await;
    let mut registration = Pipe::builder("mrsm", "pipeline")
        .location(Some("daily"))
        .instance(INSTANCE)
        .build()
        .unwrap();
    registration.set_parameters(map(json!({"pipeline": {
        "name": "daily", "id": "id-daily", "revision": 1,
        "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)],
    }})));
    assert!(f.instance.register_pipe(&registration).await.success);
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let r = call(&f, "POST", &admin, "/engine/runs", Some(json!([
        {"run_id": "daily-review", "started": "2026-10-27T00:00:00Z", "status": "success",
         "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)], "message": "pressure details",
         "selection": {"pipeline_definitions": [{"name": "daily", "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)]}]}}
    ]))).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = get(&f, &ed, "/pipelines/daily").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["hidden_members"], json!(1));
    assert_eq!(r.json()["members"].as_array().unwrap().len(), 1);
    assert!(message(&r).contains("1 hidden pipe"), "{}", r.text());
    assert!(!r.text().contains("pressure"), "{}", r.text());
    // Generic pipe metadata cannot return the Pipeline's unfiltered definition.
    for uri in [
        "/pipes",
        "/pipes/mrsm",
        "/pipes/mrsm/pipeline",
        "/pipes/mrsm/pipeline/daily",
    ] {
        let r = get(&f, &ed, uri).await;
        assert!(!r.text().contains("pressure"), "{uri}: {}", r.text());
    }
    let r = get(&f, &admin, "/pipelines/daily").await;
    assert_eq!(r.json()["hidden_members"], json!(0));
    let r = call(&f, "POST", &ed, "/pipelines/daily/sync", None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        message(&r).contains("you may not change 1 pipe"),
        "{}",
        r.text()
    );
}

#[tokio::test]
async fn runs_and_sync_statuses_show_only_what_the_caller_can_see() {
    let f = fixture().await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let started = "2026-10-27T00:00:00Z";
    let runs = json!([
        {"run_id": "both", "pipe_ref": null, "started": started, "status": "success",
         "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)]},
        {"run_id": "both", "pipe_ref": pipe_ref(TEMPERATURE), "started": started, "status": "success"},
        {"run_id": "both", "pipe_ref": pipe_ref(PRESSURE), "started": started, "status": "success",
         "message": "synced plugin:ops pressure"},
        {"run_id": "ops", "pipe_ref": null, "started": started, "status": "success",
         "targets": [pipe_ref(PRESSURE)]},
        {"run_id": "ops", "pipe_ref": pipe_ref(PRESSURE), "started": started, "status": "success"},
    ]);
    let r = call(&f, "POST", &admin, "/engine/runs", Some(runs)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let run_ids = |r: &Resp| -> BTreeSet<String> {
        r.json()["runs"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|run| run["pipe_ref"].is_null())
            .map(|run| run["run_id"].as_str().unwrap().to_string())
            .collect()
    };
    let r = get(&f, &admin, "/engine/runs").await;
    assert_eq!(run_ids(&r), ["both".to_string(), "ops".to_string()].into());
    let r = get(&f, &ed, "/engine/runs").await;
    assert_eq!(run_ids(&r), ["both".to_string()].into(), "{}", r.text());
    assert!(!r.text().contains("pressure"), "{}", r.text());
    let r = get(&f, &ed, "/engine/runs/both").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(!r.text().contains("pressure"), "{}", r.text());
    assert!(r.text().contains("\"hidden\""), "{}", r.text());
    let r = get(&f, &ed, "/engine/runs/ops").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    // Engine state: readable for visible pipes, writable for pipes the caller may change.
    let r = get(
        &f,
        &ed,
        &format!(
            "/engine/state?pipe_ref={}",
            pipe_ref(PRESSURE).replace('|', "%7C")
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    let lease = json!({"pipe_ref": pipe_ref(PUBLIC), "holder": "ed", "ttl_ms": 1000});
    let r = call(&f, "POST", &ed, "/engine/leases", Some(lease)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn projected_engine_rows_events_and_run_selectors_keep_the_access_boundary() {
    use meerschaum_connector_sql::SqlConnector;
    use meerschaum_server::engine_store::InstanceEngine;
    let mut f = fixture().await;
    let store = Arc::new(
        SqlConnector::from_uri(
            "access_review",
            &format!("sqlite:///{}", f._dir.path().join("engine.db").display()),
        )
        .await
        .unwrap(),
    );
    f.state = AppState::builder((*f.state.config).clone(), f.instance.clone())
        .secret(SECRET.to_vec())
        .engine(Arc::new(InstanceEngine::new(store, INSTANCE)))
        .build()
        .unwrap();
    f.app = meerschaum_server::app(f.state.clone());
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let started = "2026-10-27T00:00:00Z";
    let records = json!([
        {"run_id": "review-both", "started": started, "status": "success", "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)], "message": "hidden pressure details"},
        {"run_id": "review-both", "pipe_ref": pipe_ref(TEMPERATURE), "started": started, "status": "success", "message": "visible result"},
        {"run_id": "review-both", "pipe_ref": pipe_ref(PRESSURE), "started": started, "status": "success", "message": "hidden pressure result"},
        {"run_id": "review-ops", "started": started, "status": "success", "targets": [pipe_ref(PRESSURE)]},
        {"run_id": "review-ops", "pipe_ref": pipe_ref(PRESSURE), "started": started, "status": "success", "message": "hidden pressure run"},
    ]);
    let r = call(&f, "POST", &admin, "/engine/runs", Some(records)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        "/engine/pipes/engine_nodes/read",
        Some(json!({"select_columns": ["message"]})),
    )
    .await;
    assert_eq!(
        r.json()["rows"],
        json!([{"message": "visible result"}]),
        "{}",
        r.text()
    );
    let events = json!([
        {"run_id": "review-both", "created": started, "event": "run_started", "payload": {"targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)]}},
        {"run_id": "review-both", "created": started, "event": "run_finished", "payload": {"message": "hidden pressure details", "success": true}},
        {"run_id": "review-ops", "created": started, "event": "run_started", "payload": {"targets": [pipe_ref(PRESSURE)]}},
    ]);
    let r = call(&f, "POST", &admin, "/engine/events", Some(events)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        "/engine/pipes/engine_events/read",
        Some(json!({"select_columns": ["run_id", "payload"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(
        !r.text().contains("pressure") && !r.text().contains("review-ops"),
        "{}",
        r.text()
    );
    let r = get(&f, &ed, "/engine/runs/review-both").await;
    assert!(!r.text().contains("pressure"), "{}", r.text());
    for run_id in ["review-both", "review-ops"] {
        let r = call(
            &f,
            "POST",
            &ed,
            "/engine/pipes/engine_nodes/clear",
            Some(json!({"params": {"run_id": run_id}})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        let r = call(&f, "POST", &ed, "/engine/runs", Some(json!([
            {"run_id": run_id, "started": started, "status": "success", "targets": [pipe_ref(TEMPERATURE)]}
        ]))).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    }
    let r = call(&f, "POST", &ed, "/engine/runs", Some(json!([
        {"run_id": "forged", "started": started, "status": "success", "targets": [pipe_ref(PRESSURE)]}
    ]))).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn run_started_events_do_not_name_hidden_targets() {
    use meerschaum_connector_api::models::EngineRunEvent;
    use meerschaum_server::access::{self, RefFilter};
    let f = fixture().await;
    let auth = meerschaum_server::auth::Principal::User(User::new("ed", None));
    let caller = access::access_of_fresh(&f.state, auth, &Scopes::List(vec!["*".into()]))
        .await
        .unwrap();
    let mut filter = RefFilter::new(&f.state, &caller, INSTANCE);
    let event = EngineRunEvent::from_row(
        "both",
        1,
        chrono::Utc::now(),
        None,
        "run_started",
        json!({"targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)]}),
    );
    let event = access::runs::visible_event(&mut filter, event).await;
    let json = serde_json::to_value(event).unwrap();
    assert_eq!(json["targets"], json!([pipe_ref(TEMPERATURE)]));
    assert_eq!(json["hidden_targets"], 1);
    assert!(!json.to_string().contains("pressure"));
}

#[tokio::test]
async fn engine_refs_from_a_clients_api_label_use_the_servers_access() {
    let f = fixture().await;
    let ed = token(&f, "ed").await;
    let client_ref = |keys: (&str, &str)| format!("api:remote|{}|{}|None", keys.0, keys.1);
    let r = call(
        &f,
        "POST",
        &ed,
        "/engine/leases",
        Some(json!({"pipe_ref": client_ref(TEMPERATURE), "holder": "client", "ttl_ms": 1000})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        "/engine/leases",
        Some(json!({"pipe_ref": client_ref(PRESSURE), "holder": "client", "ttl_ms": 1000})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

// ------------------------------------------------------------------------------------------------
// MCP (AC6, AC7, AC8)
// ------------------------------------------------------------------------------------------------

fn caller(user: &str, role: Role) -> Caller {
    Caller::api(
        Scopes::List(role.permissions().into_iter().collect()),
        Some(Principal::User(User::new(user, None))),
    )
}

async fn tool(f: &Fixture, caller: &Caller, name: &str, arguments: Json) -> Result<Json, String> {
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                         "params": {"name": name, "arguments": arguments}});
    let reply = f
        .mcp
        .handle_payload(&f.state, serde_json::from_value(message).unwrap(), caller)
        .await
        .expect("a reply");
    let reply = serde_json::to_value(reply).unwrap();
    let result = &reply["result"];
    let text = result["content"][0]["text"]
        .as_str()
        .unwrap_or("")
        .to_string();
    if result["isError"] == json!(true) {
        return Err(text);
    }
    Ok(serde_json::from_str(&text).unwrap_or(Json::String(text)))
}

#[tokio::test]
async fn mcp_tools_apply_the_same_rules() {
    let f = fixture_with(|cfg| {
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec![INSTANCE.into(), "sql:main".into(), "sql:secure".into()];
    })
    .await;
    let otto = caller("otto", Role::Editor);
    let ed = caller("ed", Role::Editor);
    let listing = tool(&f, &otto, "list_pipes", json!({})).await.unwrap();
    let text = listing.to_string();
    assert!(
        text.contains("pressure") && !text.contains("temperature"),
        "{text}"
    );
    let keys = |k: (&str, &str)| json!({"connector_keys": k.0, "metric_key": k.1});
    let r = tool(&f, &otto, "get_pipe_attributes", keys(TEMPERATURE)).await;
    assert!(
        r.as_ref().is_err_and(|e| e.contains("was not found")),
        "{r:?}"
    );
    let mut edit = keys(PUBLIC);
    edit["parameters"] = json!({"tags": ["m"]});
    let r = tool(&f, &ed, "edit_pipe", edit).await;
    assert!(
        r.as_ref().is_err_and(|e| e.contains("You cannot change")),
        "{r:?}"
    );
    let r = tool(
        &f,
        &otto,
        "read_sql",
        json!({"connector_keys": "sql:main", "query": "SELECT * FROM \"plugin_data_temperature\""}),
    )
    .await;
    assert!(r.as_ref().is_err_and(|e| e.contains("cannot see")), "{r:?}");
    let r = tool(
        &f,
        &otto,
        "read_sql",
        json!({"connector_keys": "sql:secure", "query": "SELECT 1"}),
    )
    .await;
    assert!(r.as_ref().is_err_and(|e| e.contains("reserved")), "{r:?}");
    let r = tool(
        &f,
        &otto,
        "read_sql",
        json!({"connector_keys": "sql:main", "query": "SELECT * FROM plugin_ops_pressure"}),
    )
    .await;
    assert!(r.is_ok(), "{r:?}");
    let r = tool(&f, &otto, "list_connectors", json!({})).await.unwrap();
    assert!(!r.to_string().contains("sql:secure"), "{r}");
    let r = tool(&f, &ed, "list_connectors", json!({})).await.unwrap();
    assert!(r.to_string().contains("sql:secure"), "{r}");
    let r = tool(
        &f,
        &otto,
        "register_pipe",
        json!({"connector_keys": "sql:secure", "metric_key": "z"}),
    )
    .await;
    assert!(r.as_ref().is_err_and(|e| e.contains("reserved")), "{r:?}");
    let operator = caller("opal", Role::Operator);
    let r = tool(&f, &operator, "execute_action", json!({"action": "sh id"})).await;
    let text = format!("{r:?}");
    assert!(text.contains("false") || r.is_err(), "{text}");
}

#[tokio::test]
async fn mcp_named_sql_uses_the_selected_instances_access_and_policy_before_its_backend() {
    let mut f = fixture_with(|cfg| {
        cfg.allow_multiple_instances = true;
        cfg.allowed_instance_keys = vec![INSTANCE.into(), "sql:other".into()];
    })
    .await;
    let other = Arc::new(MemoryInstance::new("other"));
    // This target is public on the default instance and private on the named instance.
    register(
        &other,
        LEGACY,
        json!({"access": {"owner": "ed", "visibility": "private"}}),
    )
    .await;
    register(&other, PUBLIC, json!({})).await;
    let resolver = Arc::new(Instances(vec![("sql:other".into(), other)]));
    let state = AppState::builder((*f.state.config).clone(), f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver.clone())
        .build()
        .unwrap();
    let backend = Arc::new(Mcp::default());
    f.mcp = McpServer::for_state(&state).with_backend(backend.clone());
    f.state = state;
    let otto = caller("otto", Role::Editor);
    let query = |connector: &str, target: &str| json!({"connector_keys": connector, "query": format!("SELECT * FROM {target}")});
    assert!(
        tool(
            &f,
            &otto,
            "read_sql",
            query(INSTANCE, "plugin_shared_legacy")
        )
        .await
        .is_ok()
    );
    assert_eq!(backend.reads.load(Ordering::SeqCst), 1);
    let refused = tool(
        &f,
        &otto,
        "read_sql",
        query("sql:other", "plugin_shared_legacy"),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("cannot see"), "{refused}");
    assert_eq!(backend.reads.load(Ordering::SeqCst), 1);
    assert!(
        tool(
            &f,
            &otto,
            "read_sql",
            query("sql:other", "plugin_ops_public")
        )
        .await
        .is_ok()
    );
    assert_eq!(backend.reads.load(Ordering::SeqCst), 2);
    for who in [&otto, &caller("admin", Role::Admin)] {
        let refused = tool(&f, who, "read_sql", query("sql:secure", "arbitrary"))
            .await
            .unwrap_err();
        assert!(refused.contains("allowed instances patterns"), "{refused}");
        assert_eq!(backend.reads.load(Ordering::SeqCst), 2);
    }
    let mut cfg = (*f.state.config).clone();
    cfg.connectors = map(json!({"sql": {"other": {"teams": ["Data"]}}}));
    f.state = AppState::builder(cfg.clone(), f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver.clone())
        .build()
        .unwrap();
    let refused = tool(
        &f,
        &otto,
        "read_sql",
        query("sql:other", "plugin_ops_public"),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("reserved"), "{refused}");
    assert_eq!(backend.reads.load(Ordering::SeqCst), 2);
    assert!(
        tool(
            &f,
            &caller("ed", Role::Editor),
            "read_sql",
            query("sql:other", "plugin_ops_public")
        )
        .await
        .is_ok()
    );
    assert_eq!(backend.reads.load(Ordering::SeqCst), 3);
    cfg.allow_multiple_instances = false;
    f.state = AppState::builder(cfg, f.instance.clone())
        .secret(SECRET.to_vec())
        .resolver(resolver)
        .build()
        .unwrap();
    let refused = tool(
        &f,
        &otto,
        "read_sql",
        query("sql:other", "plugin_ops_public"),
    )
    .await
    .unwrap_err();
    assert!(refused.contains("additional instances"), "{refused}");
    assert_eq!(backend.reads.load(Ordering::SeqCst), 3);
    // Stdio retains local trust and does not inherit HTTP instance restrictions.
    assert!(
        tool(
            &f,
            &Caller::stdio(),
            "read_sql",
            query("sql:unserved", "arbitrary")
        )
        .await
        .is_ok()
    );
    assert_eq!(backend.reads.load(Ordering::SeqCst), 4);
}

// ------------------------------------------------------------------------------------------------
// Performance: one bulk read of the pipes' access per instance, reused for a few seconds
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_listing_reads_the_pipes_in_bulk_and_reuses_them() {
    let f = fixture().await;
    for i in 0..120 {
        let team = if i % 2 == 0 { "Data" } else { "Ops" };
        register(
            &f.instance,
            ("plugin:bulk", &format!("m{i}")),
            json!({"access": {"team": team}}),
        )
        .await;
    }
    let ed = token(&f, "ed").await;
    let (listings, reads) = (
        f.instance.pipes_listings.load(Ordering::SeqCst),
        f.instance.attribute_reads.load(Ordering::SeqCst),
    );
    for _ in 0..5 {
        let r = get(&f, &ed, "/pipes/keys").await;
        assert_eq!(
            listed(&r)
                .iter()
                .filter(|k| k.starts_with("plugin:bulk"))
                .count(),
            60
        );
    }
    // Each listing reads the keys once, and the access index is read once for all of them
    // (twice when the requests outlast `INDEX_TTL`): never once per pipe.
    let listed_now = f.instance.pipes_listings.load(Ordering::SeqCst) - listings;
    assert!(listed_now <= 7, "{listed_now} listings for 5 requests");
    assert_eq!(f.instance.attribute_reads.load(Ordering::SeqCst), reads);
    // One pipe's routes read that pipe, not every pipe.
    let before = f.instance.pipes_listings.load(Ordering::SeqCst);
    for i in (0..20).step_by(2) {
        let r = get(
            &f,
            &ed,
            &url(("plugin:bulk", &format!("m{i}")), "/attributes"),
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    }
    assert!(f.instance.pipes_listings.load(Ordering::SeqCst) - before <= 2);
}
