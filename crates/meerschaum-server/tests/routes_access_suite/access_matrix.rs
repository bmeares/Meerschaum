//! The access model over HTTP and MCP: who sees and changes what.
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
//! | `plugin:shared / legacy` | no `access` key (Everyone's, as every pipe before 5.0.0rc15) |

use crate::common;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::common::fakes::RecordingJobs;
use crate::common::{MemoryInstance, Resp, SECRET, login, req, send};
use async_trait::async_trait;
use axum::Router;
use axum::http::StatusCode;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Role, Team, TeamRole};
use meerschaum_core::users::User;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_server::auth::{Principal, Scopes};
use meerschaum_server::backends::{ActionsBackend, InstanceResolver, MemoryEngine};
use meerschaum_server::mcp::{Caller, McpBackend, McpServer, SqlReadError};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};

mod commands;
mod mcp;
mod ownership;
mod pipelines_and_runs;
mod roles;
mod sql_guard;
mod teams;
mod users;
mod visibility;

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
        [
            "show",
            "sync",
            "sh",
            "python",
            "edit",
            "verify",
            "deduplicate",
            "clear",
            "drop",
            "delete",
            "lock",
            "unlock",
            "partition",
            "tag",
            "copy",
            "register",
            "index",
            "start",
            "help",
        ]
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

/// Capture a fixture's immutable registration before constructing its completed history.
async fn captured_registration(instance: &MemoryInstance, keys: (&str, &str)) -> String {
    let attributes = instance.get_pipe_attributes(&pipe(keys)).await.unwrap();
    meerschaum_connector::sync_status::registration_from_attributes(&attributes).unwrap()
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
    tool_result(reply)
}

async fn register_mcp_pipe(f: &Fixture, auth: &str, arguments: Json) -> Result<Json, String> {
    // Registration revalidates the original HTTP credential after the connector hook.
    let reply = call(
        f,
        "POST",
        auth,
        "/mcp",
        Some(json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                    "params": {"name": "register_pipe", "arguments": arguments}})),
    )
    .await;
    assert_eq!(reply.status, StatusCode::OK, "{}", reply.text());
    tool_result(reply.json())
}

fn tool_result(reply: Json) -> Result<Json, String> {
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
