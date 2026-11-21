//! rc15 wave 2 (AC9): who sees and controls a job over HTTP.
//!
//! A job started through the API (`POST /jobs/{name}`, `POST /execute`) records its `owner` (the
//! caller) and `team` (the caller's primary team), whatever the request says. Its team's members
//! and administrators see it, its logs and its existence; jobs created locally are Everyone's;
//! console runs are private to their owner, their team's managers and administrators. Controlling
//! a job needs the job permission on a job you can see (administrators only under `--secure`); a
//! hidden job answers `404`, and a Pipeline's scheduled jobs of other teams are only counted.

use crate::common;
use std::sync::{Arc, Mutex};

use crate::common::{SECRET, req, send};
use async_trait::async_trait;
use axum::Router;
use axum::http::StatusCode;
use indexmap::IndexMap;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::access::{Role, Team, TeamRole};
use meerschaum_core::{Map, SuccessTuple, Value};
use meerschaum_server::AppState;
use meerschaum_server::backends::{CreateJob, JobsBackend, LogMonitor};
use serde_json::{Value as Json, json};

/// Jobs in memory: `(sysargs, properties)` by name, plus the team renames asked of them.
#[derive(Default)]
struct FakeJobs {
    jobs: Mutex<IndexMap<String, (Vec<String>, Map)>>,
    retagged: Mutex<Vec<(String, Option<String>)>>,
    /// How many full listings (each job's status) were asked for.
    full_listings: std::sync::atomic::AtomicUsize,
}

impl FakeJobs {
    /// A job created locally (no owner, no team: Everyone's).
    fn seed_local(&self, name: &str) {
        self.jobs.lock().unwrap().insert(
            name.into(),
            (vec!["sync".into(), "pipes".into()], Map::new()),
        );
    }

    fn properties(&self, name: &str) -> Map {
        self.jobs.lock().unwrap()[name].1.clone()
    }

    fn meta(&self, name: &str) -> Option<Json> {
        let jobs = self.jobs.lock().unwrap();
        let (sysargs, properties) = jobs.get(name)?;
        Some(json!({
            "sysargs": sysargs,
            "result": [true, "ok"],
            "restart": false,
            "status": "stopped",
            "daemon": {"status": "stopped", "pid": null,
                       "properties": serde_json::to_value(properties).unwrap()},
        }))
    }
}

#[async_trait]
impl JobsBackend for FakeJobs {
    async fn list(&self) -> IndexMap<String, Json> {
        self.full_listings
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let names: Vec<String> = self.jobs.lock().unwrap().keys().cloned().collect();
        names
            .into_iter()
            .filter_map(|n| self.meta(&n).map(|m| (n, m)))
            .collect()
    }
    // Commands and properties only, as the real backend reads them from the jobs' files.
    async fn list_definitions(&self) -> IndexMap<String, Json> {
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .map(|(name, (sysargs, properties))| {
                let properties = serde_json::to_value(properties).unwrap();
                let meta = json!({"sysargs": sysargs, "daemon": {"properties": properties}});
                (name.clone(), meta)
            })
            .collect()
    }
    async fn get(&self, name: &str) -> Option<Json> {
        self.meta(name)
    }
    async fn exists(&self, name: &str) -> bool {
        self.jobs.lock().unwrap().contains_key(name)
    }
    async fn create(&self, name: &str, sysargs: Vec<String>, properties: Option<Map>) -> CreateJob {
        let mut jobs = self.jobs.lock().unwrap();
        if jobs.contains_key(name) {
            return CreateJob::Exists;
        }
        jobs.insert(name.into(), (sysargs, properties.unwrap_or_default()));
        CreateJob::Started(SuccessTuple::ok("started"))
    }
    async fn delete(&self, name: &str) -> SuccessTuple {
        self.jobs.lock().unwrap().shift_remove(name);
        SuccessTuple::success()
    }
    async fn start(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::ok("Started.")
    }
    async fn stop(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::ok("Stopped.")
    }
    async fn pause(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::ok("Paused.")
    }
    async fn logs(&self, name: &str) -> Option<String> {
        self.exists(name)
            .await
            .then(|| format!("output of {name}\n"))
    }
    async fn stop_time(&self, _name: &str) -> Option<chrono::DateTime<chrono::Utc>> {
        None
    }
    async fn is_blocking_on_stdin(&self, _name: &str) -> bool {
        false
    }
    async fn prompt_kwargs(&self, _name: &str) -> Map {
        Map::new()
    }
    async fn monitor_logs(&self, _name: &str) -> Option<LogMonitor> {
        None
    }
    async fn team_jobs(&self, team: &str) -> Vec<String> {
        self.jobs
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, (_, p))| p.get("team").and_then(Value::as_str) == Some(team))
            .map(|(n, _)| n.clone())
            .collect()
    }
    // What the real backend does (`meerschaum_jobs::ownership::retag_team`).
    async fn retag_team(&self, old: &str, new: Option<&str>) -> Vec<String> {
        self.retagged
            .lock()
            .unwrap()
            .push((old.to_string(), new.map(str::to_string)));
        let mut moved = Vec::new();
        for (name, (_, props)) in self.jobs.lock().unwrap().iter_mut() {
            if props.get("team").and_then(Value::as_str) == Some(old) {
                props.insert(
                    "team".into(),
                    new.map_or(Value::Null, |t| Value::Str(t.into())),
                );
                moved.push(name.clone());
            }
        }
        moved
    }
}

struct Fixture {
    app: Router,
    state: AppState,
    jobs: Arc<FakeJobs>,
    instance: Arc<common::MemoryInstance>,
    _dir: tempfile::TempDir,
}

fn permissions(role: Role) -> Vec<String> {
    role.permissions().into_iter().collect()
}

/// Users (password `pw`, `admin` / `pass`):
///
/// | user    | role     | teams                    |
/// |---------|----------|--------------------------|
/// | `alice` | operator | Data                     |
/// | `bob`   | operator | Data                     |
/// | `carol` | operator | Data (manager)           |
/// | `dave`  | operator | Ops                      |
/// | `erin`  | viewer   | Data                     |
/// | `frank` | editor   | Data                     |
/// | `gina`  | operator | none (Everyone)          |
/// | `hank`  | `pipes:read`, `pipes:delete`, `jobs:read` | Data |
async fn fixture(no_auth: bool) -> Fixture {
    fixture_with(|cfg| cfg.no_auth = no_auth).await
}

/// [`fixture`] with the server's configuration changed by `tweak`.
async fn fixture_with(tweak: impl FnOnce(&mut meerschaum_server::ServerConfig)) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    for (name, role) in [
        ("alice", Role::Operator),
        ("bob", Role::Operator),
        ("carol", Role::Operator),
        ("dave", Role::Operator),
        ("erin", Role::Viewer),
        ("frank", Role::Editor),
        ("gina", Role::Operator),
    ] {
        instance.seed_user(name, "pw", None);
        let perms = permissions(role);
        let perms: Vec<&str> = perms.iter().map(String::as_str).collect();
        instance.seed_user_scopes(name, &perms);
    }
    instance.seed_user("hank", "pw", None);
    instance.seed_user_scopes("hank", &["pipes:read", "pipes:delete", "jobs:read"]);
    for (team, members) in [
        (
            "Data",
            vec![
                ("alice", TeamRole::Member),
                ("bob", TeamRole::Member),
                ("carol", TeamRole::Manager),
                ("erin", TeamRole::Member),
                ("frank", TeamRole::Member),
                ("hank", TeamRole::Member),
            ],
        ),
        ("Ops", vec![("dave", TeamRole::Member)]),
    ] {
        let mut t = Team::new(team);
        for (member, role) in members {
            t.add_member(member, role);
        }
        let registered = instance.register_team(&mut t).await;
        assert!(registered.success, "{registered}");
    }
    let mut cfg = common::config(dir.path());
    tweak(&mut cfg);
    let jobs = Arc::new(FakeJobs::default());
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .jobs(jobs.clone())
        .actions(Arc::new(ListedActions))
        .build()
        .unwrap();
    Fixture {
        app: meerschaum_server::app(state.clone()),
        state,
        jobs,
        instance,
        _dir: dir,
    }
}

/// The actions the jobs here run (a non-administrator's step must name one, 5.1).
struct ListedActions;

#[async_trait::async_trait]
impl meerschaum_server::backends::ActionsBackend for ListedActions {
    fn list(&self) -> Vec<String> {
        [
            "sync", "show", "attach", "watch", "stop", "pause", "restart", "delete",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }
    async fn execute(&self, _action: &str, _keywords: Map) -> meerschaum_core::SuccessTuple {
        meerschaum_core::SuccessTuple::success()
    }
}

async fn token(f: &Fixture, user: &str) -> String {
    let password = if user == "admin" { "pass" } else { "pw" };
    common::login(&f.app, user, password, None).await
}

async fn get(f: &Fixture, auth: &str, uri: &str) -> common::Resp {
    send(&f.app, req("GET", uri, Some(auth), None)).await
}

async fn post(f: &Fixture, auth: &str, uri: &str, body: Option<Json>) -> common::Resp {
    let body = body.map(|b| ("application/json", b.to_string().into_bytes()));
    send(&f.app, req("POST", uri, Some(auth), body)).await
}

fn names(r: &common::Resp) -> Vec<String> {
    let mut v: Vec<String> = r
        .json()
        .as_object()
        .map(|o| o.keys().cloned().collect())
        .unwrap_or_default();
    v.sort();
    v
}

/// Create a job as `auth` through `POST /jobs/{name}` (the `api:` executor's call).
async fn create(f: &Fixture, auth: &str, name: &str, properties: Json) -> common::Resp {
    post(
        f,
        auth,
        &format!("/jobs/{name}"),
        Some(json!({"sysargs": ["sync", "pipes", "--loop"], "properties": properties})),
    )
    .await
}

#[tokio::test]
async fn a_job_records_who_started_it_whatever_the_request_says() {
    let f = fixture(false).await;
    let alice = token(&f, "alice").await;
    // A client that claims another owner and team is recorded as itself.
    let r = create(
        &f,
        &alice,
        "nightly",
        json!({"owner": "admin", "team": "Ops", "mrsm_owner": "admin"}),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let props = f.jobs.properties("nightly");
    assert_eq!(props.get("owner").and_then(Value::as_str), Some("alice"));
    assert_eq!(props.get("team").and_then(Value::as_str), Some("Data"));
    assert_eq!(
        props.get("mrsm_owner").and_then(Value::as_str),
        Some("alice")
    );
    // A user in no team: Everyone (`null`).
    let gina = token(&f, "gina").await;
    assert_eq!(
        create(&f, &gina, "mine", json!({})).await.status,
        StatusCode::OK
    );
    let props = f.jobs.properties("mine");
    assert_eq!(props.get("owner").and_then(Value::as_str), Some("gina"));
    assert_eq!(props.get("team"), Some(&Value::Null));
    // A console run (`POST /execute`) records the same.
    let r = post(
        &f,
        &alice,
        "/execute",
        Some(json!({"sysargs": ["show", "version"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let run = r.json()["job"].as_str().unwrap().to_string();
    let props = f.jobs.properties(&run);
    assert_eq!(props.get("owner").and_then(Value::as_str), Some("alice"));
    assert_eq!(props.get("team").and_then(Value::as_str), Some("Data"));
    assert_eq!(
        props
            .get("mrsm_execution")
            .and_then(|e| e.get("kind"))
            .and_then(Value::as_str),
        Some("temporary_execution")
    );
}

#[tokio::test]
async fn jobs_and_logs_are_visible_to_their_team_and_administrators() {
    let f = fixture(false).await;
    f.jobs.seed_local("local-job");
    let (admin, alice, bob, carol, dave, erin, gina) = (
        token(&f, "admin").await,
        token(&f, "alice").await,
        token(&f, "bob").await,
        token(&f, "carol").await,
        token(&f, "dave").await,
        token(&f, "erin").await,
        token(&f, "gina").await,
    );
    assert_eq!(
        create(&f, &alice, "data-job", json!({})).await.status,
        StatusCode::OK
    );
    assert_eq!(
        create(&f, &dave, "ops-job", json!({})).await.status,
        StatusCode::OK
    );
    let r = post(
        &f,
        &alice,
        "/execute",
        Some(json!({"sysargs": ["show", "version"]})),
    )
    .await;
    let run = r.json()["job"].as_str().unwrap().to_string();

    let listing = |names: &[&str]| {
        let mut v: Vec<String> = names.iter().map(|n| n.to_string()).collect();
        v.sort();
        v
    };
    let everyone = listing(&["local-job", "data-job", "ops-job", &run]);
    assert_eq!(names(&get(&f, &admin, "/jobs").await), everyone);
    // The owner sees their own run; teammates see the team's jobs, not someone's console run.
    assert_eq!(
        names(&get(&f, &alice, "/jobs").await),
        listing(&["local-job", "data-job", &run])
    );
    assert_eq!(
        names(&get(&f, &bob, "/jobs").await),
        listing(&["local-job", "data-job"])
    );
    assert_eq!(
        names(&get(&f, &erin, "/jobs").await),
        listing(&["local-job", "data-job"])
    );
    // The team's manager sees its members' console runs.
    assert_eq!(
        names(&get(&f, &carol, "/jobs").await),
        listing(&["local-job", "data-job", &run])
    );
    assert_eq!(
        names(&get(&f, &dave, "/jobs").await),
        listing(&["local-job", "ops-job"])
    );
    assert_eq!(
        names(&get(&f, &gina, "/jobs").await),
        listing(&["local-job"])
    );

    // Every route that names a job hides it the same way.
    for (who, auth, visible) in [("bob", &bob, true), ("dave", &dave, false)] {
        let r = get(&f, auth, "/jobs/data-job").await;
        assert_eq!(r.status == StatusCode::OK, visible, "{who}: {}", r.text());
        let r = get(&f, auth, "/logs/data-job").await;
        assert_eq!(r.status == StatusCode::OK, visible, "{who}: {}", r.text());
        if !visible {
            assert_eq!(r.status, StatusCode::NOT_FOUND);
            assert!(!r.text().contains("output of"), "{}", r.text());
        }
        assert_eq!(
            get(&f, auth, "/jobs/data-job/exists").await.json(),
            json!(visible),
            "{who}"
        );
    }
    assert_eq!(
        get(&f, &bob, &format!("/logs/{run}")).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&f, &carol, &format!("/logs/{run}")).await.status,
        StatusCode::OK
    );
    assert_eq!(
        get(&f, &alice, &format!("/logs/{run}")).await.status,
        StatusCode::OK
    );
    // Anonymous callers are refused outright.
    let r = send(&f.app, req("GET", "/jobs", None, None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "{}", r.text());
}

#[tokio::test]
async fn controlling_a_job_needs_the_job_permission_on_a_job_you_can_see() {
    let f = fixture(false).await;
    let (alice, bob, dave, erin, frank) = (
        token(&f, "alice").await,
        token(&f, "bob").await,
        token(&f, "dave").await,
        token(&f, "erin").await,
        token(&f, "frank").await,
    );
    assert_eq!(
        create(&f, &alice, "data-job", json!({})).await.status,
        StatusCode::OK
    );

    // Creating a job: an operator may; a viewer or an editor may not.
    for (who, auth) in [("erin", &erin), ("frank", &frank)] {
        let r = create(&f, auth, &format!("by-{who}"), json!({})).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{who}: {}", r.text());
    }
    // Starting: an editor may start a job of their team; a viewer may not; another team's
    // operator does not see it.
    let r = post(&f, &frank, "/jobs/data-job/start", None).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json(), json!([true, "Started."]));
    let r = post(&f, &erin, "/jobs/data-job/start", None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = post(&f, &dave, "/jobs/data-job/start", None).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    // Stopping and pausing: operators of the team; an editor may not.
    for action in ["stop", "pause"] {
        let r = post(&f, &frank, &format!("/jobs/data-job/{action}"), None).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{action}: {}", r.text());
        let r = post(&f, &dave, &format!("/jobs/data-job/{action}"), None).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{action}: {}", r.text());
        let r = post(&f, &bob, &format!("/jobs/data-job/{action}"), None).await;
        assert_eq!(r.status, StatusCode::OK, "{action}: {}", r.text());
    }
    // Deleting: another team's operator cannot; a teammate operator can.
    let r = send(&f.app, req("DELETE", "/jobs/data-job", Some(&dave), None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    assert!(f.jobs.exists("data-job").await, "still there");
    let r = send(&f.app, req("DELETE", "/jobs/data-job", Some(&bob), None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(!f.jobs.exists("data-job").await);
}

#[tokio::test]
async fn a_token_sees_what_its_owner_sees_within_its_scopes() {
    let f = fixture(false).await;
    let alice = token(&f, "alice").await;
    let dave = token(&f, "dave").await;
    assert_eq!(
        create(&f, &alice, "data-job", json!({})).await.status,
        StatusCode::OK
    );
    assert_eq!(
        create(&f, &dave, "ops-job", json!({})).await.status,
        StatusCode::OK
    );
    // Bob's credential narrowed to reading jobs: his team's jobs, and no control.
    let narrow = common::login(&f.app, "bob", "pw", Some("jobs:read")).await;
    assert_eq!(names(&get(&f, &narrow, "/jobs").await), ["data-job"]);
    let r = post(&f, &narrow, "/jobs/data-job/start", None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn without_authentication_every_job_is_everyone_s() {
    let f = fixture(true).await;
    f.jobs.seed_local("local-job");
    let r = send(
        &f.app,
        req(
            "POST",
            "/jobs/anon",
            None,
            Some((
                "application/json",
                json!({"sysargs": ["show", "version"], "properties": {"owner": "x", "team": "Ops"}})
                    .to_string()
                    .into_bytes(),
            )),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let props = f.jobs.properties("anon");
    assert_eq!(props.get("owner"), Some(&Value::Null));
    assert_eq!(props.get("team"), Some(&Value::Null));
    let r = send(&f.app, req("GET", "/jobs", None, None)).await;
    let mut listed = names(&r);
    listed.sort();
    assert_eq!(listed, ["anon", "local-job"]);
}

/// Call an MCP tool as `username` (over the API): `Ok(result)` or `Err(error text)`.
async fn mcp(f: &Fixture, username: &str, tool: &str, arguments: Json) -> Result<Json, String> {
    use meerschaum_server::auth::{Principal, Scopes};
    use meerschaum_server::mcp::{Caller, McpServer};
    let caller = Caller::api(
        Scopes::List(vec!["*".into()]),
        Some(Principal::User(meerschaum_core::users::User::new(
            username, None,
        ))),
    );
    let message = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": tool, "arguments": arguments}});
    let reply = McpServer::for_state(&f.state)
        .handle_payload(&f.state, serde_json::from_value(message).unwrap(), &caller)
        .await
        .expect("a reply");
    let reply = serde_json::to_value(reply).unwrap();
    let text = reply["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or("")
        .to_string();
    if reply["result"]["isError"] == json!(true) {
        return Err(text);
    }
    Ok(serde_json::from_str(&text).unwrap_or(Json::String(text)))
}

#[tokio::test]
async fn mcp_job_tools_apply_the_same_rule() {
    let f = fixture(false).await;
    f.jobs.seed_local("local-job");
    let alice = token(&f, "alice").await;
    let dave = token(&f, "dave").await;
    assert_eq!(
        create(&f, &alice, "data-job", json!({})).await.status,
        StatusCode::OK
    );
    assert_eq!(
        create(&f, &dave, "ops-job", json!({})).await.status,
        StatusCode::OK
    );
    let listed = |out: &Json| -> Vec<String> {
        let mut v: Vec<String> = out["jobs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|j| j["name"].as_str().unwrap().to_string())
            .collect();
        v.sort();
        v
    };
    let bobs = mcp(&f, "bob", "list_jobs", json!({})).await.unwrap();
    assert_eq!(listed(&bobs), ["data-job", "local-job"]);
    let daves = mcp(&f, "dave", "list_jobs", json!({})).await.unwrap();
    assert_eq!(listed(&daves), ["local-job", "ops-job"]);
    let admins = mcp(&f, "admin", "list_jobs", json!({})).await.unwrap();
    assert_eq!(listed(&admins), ["data-job", "local-job", "ops-job"]);
    let hidden = mcp(&f, "dave", "get_job_logs", json!({"name": "data-job"})).await;
    assert!(hidden.unwrap_err().contains("does not exist"));
    let logs = mcp(&f, "bob", "get_job_logs", json!({"name": "data-job"}))
        .await
        .unwrap();
    assert_eq!(logs["logs"], "output of data-job");
}

/// 5.1: which isolated team a job runs for (its `team_scope`, the team's configuration and
/// credentials) is the server's to record, from the authenticated caller. A caller of the open
/// side, an administrator and a member of another isolated team who send another team's scope
/// (and its name) get a job of their own team, with no scope of the request's.
#[tokio::test]
async fn a_client_never_chooses_a_jobs_team_scope() {
    let f = fixture(false).await;
    // An isolated team (`Drylands`, member `ivan`) besides the open teams of the fixture.
    let instance = f.instance.clone();
    instance.seed_user("ivan", "pw", None);
    let perms = permissions(Role::Operator);
    let perms: Vec<&str> = perms.iter().map(String::as_str).collect();
    instance.seed_user_scopes("ivan", &perms);
    let mut drylands = Team::new("Drylands");
    drylands.id = Some("id-drylands".into());
    drylands
        .attributes
        .insert("isolated".into(), Value::Bool(true));
    drylands.add_member("ivan", TeamRole::Member);
    assert!(instance.register_team(&mut drylands).await.success);
    let claimed = json!({
        "team_scope": "id-wetlands",
        "team": "Wetlands",
        "owner": "someone-else",
    });
    for (user, team) in [
        ("alice", Some("Data")),
        ("admin", None),
        ("ivan", Some("Drylands")),
    ] {
        let auth = token(&f, user).await;
        let name = format!("{user}-claims-wetlands");
        let r = create(&f, &auth, &name, claimed.clone()).await;
        assert_eq!(r.status, StatusCode::OK, "{user}: {}", r.text());
        if !f.jobs.exists(&name).await {
            // Refused outright (a walled-off caller holds no `actions:execute`): no job at all.
            assert_eq!(user, "ivan", "{}", r.text());
            continue;
        }
        let props = f.jobs.properties(&name);
        assert_eq!(props.get("team_scope"), None, "{user}: {props:?}");
        assert_eq!(props.get("team").and_then(Value::as_str), team, "{user}");
        assert_eq!(props.get("owner").and_then(Value::as_str), Some(user));
    }
}

#[tokio::test]
async fn renaming_and_deleting_a_team_updates_job_labels_and_preserves_history_access() {
    let f = fixture(false).await;
    let (admin, alice, bob, gina) = (
        token(&f, "admin").await,
        token(&f, "alice").await,
        token(&f, "bob").await,
        token(&f, "gina").await,
    );
    assert_eq!(
        create(&f, &alice, "data-job", json!({})).await.status,
        StatusCode::OK
    );
    assert_eq!(names(&get(&f, &gina, "/jobs").await), Vec::<String>::new());

    let r = post(
        &f,
        &admin,
        "/teams/Data/rename",
        Some(json!({"name": "Analytics"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["jobs"], json!(["data-job"]));
    assert_eq!(
        f.jobs
            .properties("data-job")
            .get("team")
            .and_then(Value::as_str),
        Some("Analytics")
    );
    // The renamed team's members still see it; nobody else does.
    assert_eq!(names(&get(&f, &bob, "/jobs").await), ["data-job"]);
    assert_eq!(names(&get(&f, &gina, "/jobs").await), Vec::<String>::new());

    let r = send(
        &f.app,
        req("DELETE", "/teams/Analytics/delete", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["jobs"], json!(["data-job"]));
    assert_eq!(
        f.jobs.properties("data-job").get("team"),
        Some(&Value::Null)
    );
    // Retagging clears the display name. Retained history still identifies its
    // original Team and registered owner.
    assert_eq!(names(&get(&f, &gina, "/jobs").await), Vec::<String>::new());
    assert_eq!(names(&get(&f, &alice, "/jobs").await), ["data-job"]);
    assert_eq!(
        *f.jobs.retagged.lock().unwrap(),
        [
            ("Data".to_string(), Some("Analytics".to_string())),
            ("Analytics".to_string(), None)
        ]
    );
}

#[tokio::test]
async fn under_secure_only_administrators_control_jobs() {
    // `--secure` (`api:permissions:actions:non_admin: false`): only administrators act on jobs,
    // as in the console; reading what one may see is unchanged. v4's shape: `[false, "..."]`.
    let f = fixture_with(|cfg| cfg.actions_non_admin = false).await;
    f.jobs.seed_local("local-job");
    let (admin, bob) = (token(&f, "admin").await, token(&f, "bob").await);
    let refusal = json!([
        false,
        "The administrator for this server has not allowed users to perform actions."
    ]);
    assert_eq!(names(&get(&f, &bob, "/jobs").await), ["local-job"]);
    assert_eq!(
        get(&f, &bob, "/logs/local-job").await.status,
        StatusCode::OK
    );
    for action in ["start", "stop", "pause"] {
        let r = post(&f, &bob, &format!("/jobs/local-job/{action}"), None).await;
        assert_eq!(r.status, StatusCode::OK, "{action}: {}", r.text());
        assert_eq!(r.json(), refusal, "{action}");
    }
    let r = send(&f.app, req("DELETE", "/jobs/local-job", Some(&bob), None)).await;
    assert_eq!(r.json(), refusal);
    assert!(f.jobs.exists("local-job").await, "still there");
    let r = create(&f, &bob, "by-bob", json!({})).await;
    assert_eq!(r.json(), refusal, "{}", r.text());
    assert!(!f.jobs.exists("by-bob").await);
    // An administrator may.
    let r = post(&f, &admin, "/jobs/local-job/start", None).await;
    assert_eq!(r.json(), json!([true, "Started."]));
    let r = create(&f, &admin, "by-admin", json!({})).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(
        f.jobs
            .properties("by-admin")
            .get("owner")
            .and_then(Value::as_str),
        Some("admin")
    );
}

#[tokio::test]
async fn a_pipeline_s_jobs_of_other_teams_are_only_counted() {
    let f = fixture(false).await;
    let (admin, alice, dave, hank) = (
        token(&f, "admin").await,
        token(&f, "alice").await,
        token(&f, "dave").await,
        token(&f, "hank").await,
    );
    let body = json!({"pipeline": {"name": "daily", "targets": ["sql:fixture|sql:a|raw|None"]}});
    let r = post(&f, &admin, "/pipelines/daily/register", Some(body)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // A schedule of team Data's and one of team Ops'.
    for (auth, name) in [(&alice, "data-daily"), (&dave, "ops-daily")] {
        let r = post(
            &f,
            auth,
            &format!("/jobs/{name}"),
            Some(json!({"sysargs": ["sync", "pipelines", "daily", "--loop"], "properties": {}})),
        )
        .await;
        assert_eq!(r.status, StatusCode::OK, "{name}: {}", r.text());
    }
    let attributes = |r: common::Resp| r.json()["attributes"].clone();
    let listings = f
        .jobs
        .full_listings
        .load(std::sync::atomic::Ordering::SeqCst);
    let mine = attributes(get(&f, &alice, "/pipelines/daily/attributes").await);
    assert_eq!(
        mine["pipeline_referencing_jobs"],
        json!(["data-daily", "1 hidden job"])
    );
    assert_eq!(mine["pipeline_hidden_referencing_jobs"], json!(1));
    assert!(!mine.to_string().contains("ops-daily"), "{mine}");
    let all = attributes(get(&f, &admin, "/pipelines/daily/attributes").await);
    assert_eq!(
        all["pipeline_referencing_jobs"],
        json!(["data-daily", "ops-daily"])
    );
    assert_eq!(all["pipeline_hidden_referencing_jobs"], json!(0));
    // Matching a Pipeline's jobs never asks every job for its status (a `systemctl` query each).
    assert_eq!(
        f.jobs
            .full_listings
            .load(std::sync::atomic::Ordering::SeqCst),
        listings
    );
    // Deleting the Pipeline is refused while jobs run it, naming only the visible ones.
    let r = send(
        &f.app,
        req("DELETE", "/pipelines/daily/delete", Some(&hank), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    assert!(
        r.text().contains("data-daily and 1 hidden job."),
        "{}",
        r.text()
    );
    assert!(!r.text().contains("ops-daily"), "{}", r.text());
    let r = send(
        &f.app,
        req("DELETE", "/pipelines/daily/delete", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    assert!(
        r.text().contains("data-daily and ops-daily."),
        "{}",
        r.text()
    );
}

/// 5.1: a job's input is its program's. A caller who may not run system commands answers only
/// the prompts of a job whose own command they could run (`POST /execute`'s decision); an
/// administrator answers any.
#[tokio::test]
async fn only_a_caller_who_could_run_a_jobs_command_answers_its_prompts() {
    let f = fixture(false).await;
    for (name, sysargs) in [
        ("tpr-sync", vec!["sync", "pipes"]),
        ("tpr-shell", vec!["sh", "read", "line"]),
        ("tpr-word", vec!["touch", "/tmp/tpr-x"]),
        ("tpr-python", vec!["python"]),
    ] {
        f.jobs.jobs.lock().unwrap().insert(
            name.into(),
            (sysargs.into_iter().map(String::from).collect(), Map::new()),
        );
    }
    let access_of = |user: &str| {
        let state = f.state.clone();
        let user = meerschaum_core::users::User::new(user, None);
        async move {
            meerschaum_server::access::access_of_fresh(
                &state,
                meerschaum_server::auth::Principal::User(user),
                &meerschaum_server::auth::Scopes::List(vec!["*".into()]),
            )
            .await
            .unwrap()
        }
    };
    let bob = access_of("bob").await;
    let admin = access_of("admin").await;
    let allowed = |access: meerschaum_server::access::Access, name: &'static str| {
        let state = f.state.clone();
        async move {
            meerschaum_server::routes::jobs::job_input_allowed(&state, &access, name)
                .await
                .is_ok()
        }
    };
    assert!(allowed(bob.clone(), "tpr-sync").await);
    for name in ["tpr-shell", "tpr-word", "tpr-python", "tpr-missing"] {
        assert!(!allowed(bob.clone(), name).await, "{name}");
    }
    for name in ["tpr-sync", "tpr-shell", "tpr-word", "tpr-python"] {
        assert!(allowed(admin.clone(), name).await, "{name}");
    }
}

/// 5.1: a non-administrator cannot make a job's stored command (or where it runs and writes)
/// something the policy would refuse: re-registering a name answers 409 and keeps the stored
/// command, a refused command is refused, `edit job` is refused, and the properties that choose
/// the working directory, the log file and the command are dropped (an administrator's are kept).
#[tokio::test]
async fn a_non_administrator_cannot_change_what_a_job_runs_or_where() {
    let f = fixture(false).await;
    let bob = token(&f, "bob").await;
    let admin = token(&f, "admin").await;
    // An administrator's shell job, visible to everyone (created locally).
    f.jobs.jobs.lock().unwrap().insert(
        "tpr-admin".into(),
        (vec!["sh".into(), "backup.sh".into()], Map::new()),
    );
    let stored = |name: &str| f.jobs.jobs.lock().unwrap().get(name).map(|j| j.0.clone());

    // Re-registering the name (with an allowed command) neither replaces nor starts it.
    let r = post(
        &f,
        &bob,
        "/jobs/tpr-admin",
        Some(json!({"sysargs": ["sync", "pipes"], "properties": {}})),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    assert_eq!(stored("tpr-admin").unwrap(), ["sh", "backup.sh"]);
    // A refused command is never stored, and `edit job` never runs.
    let r = post(
        &f,
        &bob,
        "/jobs/tpr-new",
        Some(json!({"sysargs": ["touch", "/tmp/tpr-x"], "properties": {}})),
    )
    .await;
    assert_eq!(r.json()[0], false, "{}", r.text());
    assert!(stored("tpr-new").is_none());
    for sysargs in [json!(["edit", "job", "tpr-admin"]), json!(["edit", "jobs"])] {
        let r = post(
            &f,
            &bob,
            "/execute",
            Some(json!({"sysargs": sysargs, "name": "execute-tpr"})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{sysargs}: {}", r.text());
    }

    // The properties that choose where it runs and writes are the server's.
    let chosen = json!({
        "cwd": "/tmp/tpr-elsewhere",
        "logs": {"path": "/tmp/tpr-elsewhere/plugin.py", "write_timestamps": false},
        "target": {"args": [["sh", "touch", "/tmp/tpr-x"]]},
        "restart": true,
    });
    let r = post(
        &f,
        &bob,
        "/jobs/tpr-bob",
        Some(json!({"sysargs": ["sync", "pipes"], "properties": chosen})),
    )
    .await;
    assert_eq!(r.json()[0], true, "{}", r.text());
    let properties = f.jobs.properties("tpr-bob");
    for key in ["cwd", "logs", "target"] {
        assert!(!properties.contains_key(key), "{key}: {properties:?}");
    }
    assert_eq!(properties.get("restart"), Some(&Value::Bool(true)));
    assert_eq!(stored("tpr-bob").unwrap(), ["sync", "pipes"]);
    let r = post(
        &f,
        &admin,
        "/jobs/tpr-root",
        Some(json!({"sysargs": ["sync", "pipes"], "properties": chosen})),
    )
    .await;
    assert_eq!(r.json()[0], true, "{}", r.text());
    let properties = f.jobs.properties("tpr-root");
    assert!(properties.contains_key("cwd") && properties.contains_key("logs"));
}

/// 5.1: a command line that names a job (`show jobs`, `attach logs`, `stop jobs`, `delete jobs`,
/// `-d --name <existing job>`) runs for a non-administrator only on jobs they may act on over the
/// job routes, with the same permissions; one that names none acts on every job, so every job
/// must pass. Asked through `POST /execute/validate`, the decision the restricted web terminal
/// and `POST /execute` share.
#[tokio::test]
async fn a_command_naming_jobs_is_held_to_the_job_routes() {
    let f = fixture(false).await;
    let alice = token(&f, "alice").await;
    let bob = token(&f, "bob").await;
    let dave = token(&f, "dave").await;
    let erin = token(&f, "erin").await;
    let admin = token(&f, "admin").await;
    assert_eq!(
        create(&f, &alice, "data-job", json!({})).await.status,
        StatusCode::OK
    );
    assert_eq!(
        create(&f, &dave, "ops-job", json!({})).await.status,
        StatusCode::OK
    );
    let validate = |auth: String, sysargs: &'static [&'static str]| {
        let f = &f;
        async move {
            post(
                f,
                &auth,
                "/execute/validate",
                Some(json!({"sysargs": sysargs})),
            )
            .await
            .status
        }
    };
    // bob (Data) acts on Data's job, never on Ops's (which answers as missing).
    for sysargs in [
        &["show", "jobs", "data-job"][..],
        &["attach", "logs", "data-job"],
        &["stop", "jobs", "data-job", "-y"],
        &["delete", "jobs", "data-job", "-y"],
        &["show", "version", "-d", "--name", "a-new-job"],
    ] {
        assert_eq!(
            validate(bob.clone(), sysargs).await,
            StatusCode::OK,
            "{sysargs:?}"
        );
    }
    for sysargs in [
        &["show", "jobs", "ops-job"][..],
        &["show", "daemons", "ops-job"],
        &["attach", "jobs", "ops-job"],
        &["attach", "logs", "ops-job"],
        &["watch", "logs", "ops-job"],
        &["stop", "jobs", "data-job", "ops-job", "-y"],
        &["pause", "jobs", "ops-job"],
        &["restart", "jobs", "ops-job"],
        &["delete", "jobs", "ops-job", "-y"],
        &["show", "version", "-d", "--name", "ops-job"],
        &["show", "version", "--daemon", "--name=ops-job"],
        &["show", "version", "+", "delete", "jobs", "ops-job", "-y"],
    ] {
        assert_eq!(
            validate(bob.clone(), sysargs).await,
            StatusCode::NOT_FOUND,
            "{sysargs:?}"
        );
    }
    // Naming no job is every job: bob may not see Ops's.
    for sysargs in [
        &["show", "jobs"][..],
        &["stop", "jobs", "-y"],
        &["watch", "jobs"],
    ] {
        assert_eq!(
            validate(bob.clone(), sysargs).await,
            StatusCode::FORBIDDEN,
            "{sysargs:?}"
        );
    }
    // The permissions of the job routes: a viewer reads but does not stop.
    let erin_stop = validate(erin.clone(), &["stop", "jobs", "data-job", "-y"]).await;
    assert_eq!(erin_stop, StatusCode::FORBIDDEN);
    // Administrators are unaffected.
    for sysargs in [&["show", "jobs"][..], &["delete", "jobs", "ops-job", "-y"]] {
        assert_eq!(
            validate(admin.clone(), sysargs).await,
            StatusCode::OK,
            "{sysargs:?}"
        );
    }
    // `POST /execute` decides the same.
    let r = post(
        &f,
        &bob,
        "/execute",
        Some(json!({"sysargs": ["delete", "jobs", "ops-job", "-y"], "name": "execute-tpr"})),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    assert!(f.jobs.jobs.lock().unwrap().contains_key("ops-job"));
}
