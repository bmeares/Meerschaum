//! Wave 18 (FX7): the authorization of `POST /execute` and the exposure of `GET /jobs`.
//!
//! - A non-administrator runs through `/execute` only what the console would offer: the actions
//!   the console omits stay refused, and so do the flags it does not offer (`--shell`, ...);
//!   administrators keep v4's `/actions` latitude. The gate reads
//!   `api:permissions:actions:non_admin` (`ServerConfig::actions_non_admin`), not v4's `system:`
//!   key, and (rc15's access model) the caller's permissions: running an action needs
//!   `actions:execute` (the role `operator`).
//! - `GET /jobs` and `GET /jobs/{name}` never return a credential (connector URIs, secret-like
//!   environment variables), and the `execute-*` jobs of the console are visible to the user who
//!   started them and to administrators only.

mod common;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::Router;
use axum::http::StatusCode;
use common::{SECRET, req, send};
use indexmap::IndexMap;
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Map, SuccessTuple, Value};
use meerschaum_server::AppState;
use meerschaum_server::backends::{ActionsBackend, CreateJob, JobsBackend};
use serde_json::{Value as Json, json};

/// A job as the fake backend keeps it: the sysargs and the properties `create` was given.
type Stored = (Vec<String>, Option<Map>);

/// Jobs in memory; each job's metadata carries the `env` the real backend would (the server's
/// whole environment, here a few credentials).
#[derive(Default)]
struct FakeJobs {
    jobs: Mutex<IndexMap<String, Stored>>,
}

#[derive(Default)]
struct FakeActions {
    calls: Mutex<Vec<String>>,
}

#[async_trait]
impl ActionsBackend for FakeActions {
    fn list(&self) -> Vec<String> {
        vec![
            "show".into(),
            "chain".into(),
            "start".into(),
            "edit".into(),
            "register".into(),
        ]
    }

    async fn execute(&self, action: &str, _keywords: Map) -> SuccessTuple {
        self.calls.lock().unwrap().push(action.into());
        SuccessTuple::ok("synthetic action executed")
    }
}

impl FakeJobs {
    fn seed(&self, name: &str, owner: Option<&str>) {
        let mut properties = Map::new();
        if let Some(owner) = owner {
            properties.insert("mrsm_owner".into(), Value::Str(owner.into()));
        }
        self.jobs.lock().unwrap().insert(
            name.into(),
            (vec!["show".into(), "version".into()], Some(properties)),
        );
    }

    fn meta(&self, name: &str) -> Option<Json> {
        let jobs = self.jobs.lock().unwrap();
        let (sysargs, properties) = jobs.get(name)?;
        let mut props = serde_json::to_value(properties.clone().unwrap_or_default()).unwrap();
        props["env"] = json!({
            "NONINTERACTIVE": "1",
            "HOME": "/home/someone",
            "MRSM_SQL_MAIN": "postgresql://mrsm:hunter2-db@db.internal:5432/meerschaum",
            "MRSM_API_UP": "https://svc:s3cret-api@api.example.com:8000",
            "MRSM_SQL_LITE": "sqlite:////data/x.db",
            "MRSM_CONFIG": "{\"meerschaum\": {\"connectors\": {\"sql\": {\"main\": {\"password\": \"cfg-pass\", \"host\": \"h\"}}}}}",
            "AWS_SECRET_ACCESS_KEY": "aws-secret-value",
            "GITHUB_TOKEN": "ghp_token_value",
            "DB_PASSWORD": "plain-password-value",
            "SOME_SERVICE_KEY": "service-key-value",
            "LD_LIBRARY_PATH": "/usr/lib",
        });
        Some(json!({
            "sysargs": sysargs,
            "result": [true, "ok"],
            "restart": false,
            "status": "stopped",
            "daemon": {"status": "stopped", "pid": null, "properties": props},
        }))
    }
}

#[async_trait]
impl JobsBackend for FakeJobs {
    async fn list(&self) -> IndexMap<String, Json> {
        let names: Vec<String> = self.jobs.lock().unwrap().keys().cloned().collect();
        names
            .into_iter()
            .filter_map(|n| self.meta(&n).map(|m| (n, m)))
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
        jobs.insert(name.into(), (sysargs, properties));
        CreateJob::Started(SuccessTuple::ok("started"))
    }
    async fn delete(&self, name: &str) -> SuccessTuple {
        self.jobs.lock().unwrap().shift_remove(name);
        SuccessTuple::success()
    }
    async fn start(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn stop(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn pause(&self, _name: &str) -> SuccessTuple {
        SuccessTuple::success()
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
    async fn monitor_logs(&self, _name: &str) -> Option<meerschaum_server::backends::LogMonitor> {
        None
    }
}

struct Fixture {
    app: Router,
    jobs: Arc<FakeJobs>,
    actions: Arc<FakeActions>,
    instance: Arc<common::MemoryInstance>,
    _dir: tempfile::TempDir,
}

/// Users: `admin`, `bob` and `eve` (non-administrators with every permission, `scopes: ['*']`:
/// v4 gave a user without a `scopes` attribute every scope; since rc15 such a user has the
/// default role, `editor`, which runs the console's pipe operations but no other action).
fn fixture(tweak: impl FnOnce(&mut meerschaum_server::ServerConfig)) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    for name in ["bob", "eve"] {
        instance.seed_user(name, "pw", None);
        instance.seed_user_scopes(name, &["*"]);
    }
    let mut cfg = common::config(dir.path());
    tweak(&mut cfg);
    let jobs = Arc::new(FakeJobs::default());
    let actions = Arc::new(FakeActions::default());
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .jobs(jobs.clone())
        .actions(actions.clone())
        .build()
        .unwrap();
    Fixture {
        app: meerschaum_server::app(state),
        jobs,
        actions,
        instance,
        _dir: dir,
    }
}

fn post_json(uri: &str, auth: &str, body: Json) -> axum::http::Request<axum::body::Body> {
    req(
        "POST",
        uri,
        Some(auth),
        Some(("application/json", body.to_string().into_bytes())),
    )
}

async fn execute(f: &Fixture, auth: &str, sysargs: &[&str]) -> common::Resp {
    send(
        &f.app,
        post_json("/execute", auth, json!({"sysargs": sysargs})),
    )
    .await
}

fn started(r: &common::Resp) -> bool {
    r.status == StatusCode::OK && r.json()["success"] == true
}

#[tokio::test]
async fn a_non_admin_cannot_run_the_actions_the_console_omits() {
    let f = fixture(|cfg| cfg.actions_non_admin = true);
    let bob = common::login(&f.app, "bob", "pw", None).await;
    for sysargs in [
        // The console's own omit list (`api` and the executor flag are cleaned out of the line
        // before it runs, as `POST /jobs/{name}` does).
        vec!["sh", "id"],
        vec!["os", "id"],
        vec!["sql", "main", "SELECT 1"],
        vec!["stack", "up"],
        vec!["reload"],
        vec!["repo", "show"],
        vec!["instance", "sql:main"],
        // What the console does not list but that runs arbitrary code or edits the server.
        vec!["python", "-c", "print(1)"],
        vec!["install", "plugins", "x"],
        vec!["upgrade", "packages"],
        vec!["start", "jobs", "sh id"],
        vec!["compose", "up"],
        // After a chain, after the pipeline separator, and in a different case.
        vec!["show", "version", "+", "python", "-c", "1"],
        vec!["sync", "pipes", "+", "+", "sh", "id"],
        vec!["PYTHON", "-c", "1"],
        vec!["show", "logs"],
        // The subactions that show or change the server's configuration.
        vec!["show", "config"],
        vec!["show", "environment"],
        vec!["edit", "config"],
        vec!["delete", "config"],
        vec!["register", "users", "x"],
        // No action first: a flag cannot stand in for it.
        vec!["--shell", "show", "version"],
    ] {
        let r = execute(&f, &bob, &sysargs).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{sysargs:?}: {}", r.text());
        assert!(
            r.json()["message"].as_str().is_some_and(|d| !d.is_empty()),
            "{sysargs:?} has a clear message: {}",
            r.text()
        );
        assert_eq!(r.json()["success"], json!(false));
    }
    assert!(f.jobs.jobs.lock().unwrap().is_empty(), "nothing ran");
}

#[tokio::test]
async fn a_non_admin_cannot_use_the_flags_the_console_does_not_offer() {
    let f = fixture(|cfg| cfg.actions_non_admin = true);
    let bob = common::login(&f.app, "bob", "pw", None).await;
    for flag in [
        "--shell",
        "--sh",
        "--use-bash",
        "--use_bash",
        "--allow-shell-job",
        "--gui",
        "--trace",
        "--version",
        "--help",
        "--he",
        "-h",
        "-V",
        "-yh",
        "--shell=1",
    ] {
        let r = execute(&f, &bob, &["show", "pipes", flag]).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{flag}: {}", r.text());
        assert!(r.text().contains("flag"), "{flag}: {}", r.text());
    }
    // A flag after the pipeline separator is a flag too.
    let r = execute(&f, &bob, &["sync", "pipes", ":", "--shell"]).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(f.jobs.jobs.lock().unwrap().is_empty(), "nothing ran");
}

#[tokio::test]
async fn a_non_admin_runs_what_the_console_offers() {
    let f = fixture(|cfg| cfg.actions_non_admin = true);
    let bob = common::login(&f.app, "bob", "pw", None).await;
    for sysargs in [
        vec!["show", "pipes", "-i", "sql:fixture"],
        vec![
            "sync",
            "pipes",
            "-c",
            "plugin:x",
            "-m",
            "m",
            "-l",
            "None",
            "-i",
            "sql:fixture",
            "--debug",
            "--begin",
            "2024-01-01",
            "--loop",
            "--min-seconds",
            "30",
            "-y",
        ],
        vec!["verify", "pipes", "-t", "daily", "--bounded"],
        vec!["delete", "pipes", "-c", "a", "-m", "b", "--yes"],
        vec!["edit", "pipes", "-P", r#"{"description":"safe metadata"}"#],
        vec!["show", "jobs"],
        // Another pipe's metric may be called anything, including an omitted action's name.
        vec!["show", "pipes", "-m", "python", "-c", "sh"],
        vec!["show", "pipes", "-m", "connectors"],
    ] {
        let r = execute(&f, &bob, &sysargs).await;
        assert!(started(&r), "{sysargs:?}: {} {}", r.status, r.text());
    }
    // rc15: pipes on an instance this server does not serve cannot be checked against the
    // caller's rights: refused.
    let r = execute(&f, &bob, &["show", "pipes", "-i", "sql:main"]).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        r.json()["message"].as_str().unwrap().contains("sql:main"),
        "{}",
        r.text()
    );
}

#[tokio::test]
async fn an_administrator_keeps_the_latitude_of_actions() {
    let f = fixture(|cfg| cfg.actions_non_admin = false);
    let admin = common::login(&f.app, "admin", "pass", None).await;
    for sysargs in [
        vec!["python", "-c", "print(1)"],
        vec!["show", "config"],
        vec!["show", "connector"],
        vec!["show", "connectors", "sql:private"],
        vec!["chain", "-P", r#"{"sub_args_line":"show connectors"}"#],
        vec!["stack", "ps"],
        vec!["show", "pipes", "--shell"],
    ] {
        let r = execute(&f, &admin, &sysargs).await;
        assert!(started(&r), "{sysargs:?}: {} {}", r.status, r.text());
    }
}

#[tokio::test]
async fn connector_inventory_and_serialized_wrappers_are_denied_at_every_common_route() {
    let f = fixture(|cfg| cfg.actions_non_admin = true);
    let bob = common::login(&f.app, "bob", "pw", None).await;
    for (index, sysargs) in [
        vec!["show", "connector"],
        vec!["show", "connectors", "sql:private"],
        vec!["show", "--nopretty", "connectors"],
        vec!["show", "version", "+", "show", "connectors"],
        vec!["chain", "-P", r#"{"sub_args_line":"show connectors"}"#],
    ]
    .iter()
    .enumerate()
    {
        for uri in ["/execute", "/execute/validate"] {
            let response = send(&f.app, post_json(uri, &bob, json!({"sysargs":sysargs}))).await;
            assert_eq!(
                response.status,
                StatusCode::FORBIDDEN,
                "{uri} {sysargs:?}: {}",
                response.text()
            );
            assert!(
                response.text().contains("non-administrators"),
                "{}",
                response.text()
            );
        }
        // The v4 job API keeps its success tuple/status contract while refusing the action.
        let response = send(
            &f.app,
            post_json(&format!("/jobs/review-{index}"), &bob, json!(sysargs)),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert_eq!(response.json()[0], false, "{}", response.text());
    }
    for (action, keywords) in [
        ("show", json!({"action":["connectors","sql:private"]})),
        ("show", json!({"action":["connector"]})),
        (
            "chain",
            json!({"params":{"sub_args_line":"show connectors"}}),
        ),
    ] {
        let response = send(
            &f.app,
            post_json(&format!("/actions/{action}"), &bob, keywords),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert_eq!(response.json()[0], false, "{}", response.text());
    }
    assert!(f.jobs.jobs.lock().unwrap().is_empty(), "no job started");
    assert!(
        f.actions.calls.lock().unwrap().is_empty(),
        "no action executed"
    );
    let policy = send(&f.app, req("GET", "/execute/policy", Some(&bob), None))
        .await
        .json();
    assert!(
        policy["denied_subactions"]["show"]
            .as_array()
            .unwrap()
            .contains(&json!("connector"))
    );
}

async fn api_key(f: &Fixture, admin: &str, label: &str, scopes: Json) -> String {
    let response = send(
        &f.app,
        post_json(
            "/tokens/register",
            admin,
            json!({"label":label,"scopes":scopes}),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    format!("Bearer {}", response.json()["api_key"].as_str().unwrap())
}

#[tokio::test]
async fn narrowed_admin_keys_follow_policy_while_full_admin_keys_keep_admin_actions() {
    let f = fixture(|cfg| cfg.actions_non_admin = true);
    let admin = common::login(&f.app, "admin", "pass", None).await;
    let narrow = api_key(
        &f,
        &admin,
        "bounded-actions",
        json!(["actions:execute", "jobs:execute", "jobs:write"]),
    )
    .await;
    for sub in ["connector", "connectors", "config"] {
        for uri in ["/execute", "/execute/validate"] {
            let response = send(
                &f.app,
                post_json(uri, &narrow, json!({"sysargs":["show",sub]})),
            )
            .await;
            assert_eq!(
                response.status,
                StatusCode::FORBIDDEN,
                "{uri}: {}",
                response.text()
            );
        }
        for (uri, body) in [
            (format!("/jobs/scoped-{sub}"), json!(["show", sub])),
            ("/actions/show".into(), json!({"action":[sub]})),
        ] {
            let response = send(&f.app, post_json(&uri, &narrow, body)).await;
            assert_eq!(response.status, StatusCode::OK, "{}", response.text());
            assert_eq!(response.json()[0], false, "{}", response.text());
        }
    }
    assert!(f.jobs.jobs.lock().unwrap().is_empty());
    assert!(f.actions.calls.lock().unwrap().is_empty());
    // Full admin keys remain admin even when ordinary users cannot execute actions.
    let secured = fixture(|cfg| cfg.actions_non_admin = false);
    let admin = common::login(&secured.app, "admin", "pass", None).await;
    let full = api_key(&secured, &admin, "admin-all", json!(["*"])).await;
    assert!(started(
        &execute(&secured, &full, &["show", "connectors"]).await
    ));
    let validation = send(
        &secured.app,
        post_json(
            "/execute/validate",
            &full,
            json!({"sysargs":["show","config"]}),
        ),
    )
    .await;
    assert_eq!(validation.status, StatusCode::OK, "{}", validation.text());
    for (uri, body) in [
        ("/actions/show", json!({"action":["config"]})),
        ("/jobs/admin-inventory", json!(["show", "connectors"])),
    ] {
        let response = send(&secured.app, post_json(uri, &full, body)).await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert_eq!(response.json()[0], true, "{}", response.text());
    }
}

#[tokio::test]
async fn legacy_chains_preserve_safe_commands_and_check_effective_selection_patches() {
    let f = fixture(|cfg| cfg.actions_non_admin = true);
    let bob = common::login(&f.app, "bob", "pw", None).await;
    for (connector, metric, owner, visibility) in [
        ("plugin:public", "public", "bob", "public"),
        ("plugin:private", "private", "eve", "private"),
    ] {
        let mut pipe = meerschaum_core::Pipe::builder(connector, metric)
            .instance("sql:fixture")
            .build()
            .unwrap();
        pipe.set_parameters(
            Value::from_json_str(
                &json!({"access":{"owner":owner,"visibility":visibility}}).to_string(),
            )
            .unwrap()
            .to_map(),
        );
        assert!(f.instance.register_pipe(&pipe).await.success);
    }
    let line = "show pipes -i sql:fixture -c plugin:public -m public";
    let safe = json!({"mrsm_instance":"sql:fixture","connector_keys":["plugin:public"],"metric_keys":["public"],
                    "debug":true,"nopretty":true,"executor_keys":null,"action":null});
    for prefix in [vec!["chain"], vec!["start", "pipeline"]] {
        for patch in [Json::Null, json!({}), safe.clone()] {
            let payload =
                json!({"sub_args_line":format!("{line} + show version"),"patch_args":patch})
                    .to_string();
            let mut args = prefix.clone();
            args.extend(["-P", payload.as_str()]);
            let response = execute(&f, &bob, &args).await;
            assert!(started(&response), "{args:?}: {}", response.text());
        }
    }
    for patch in [
        json!({"connector_keys":["plugin:private"],"metric_keys":["private"]}),
        json!({"connector_keys":null,"metric_keys":null}),
        json!({"mrsm_instance":"sql:unserved"}),
        json!({"action":["show","connectors"]}),
        json!({"executor_keys":"api:private"}),
    ] {
        let payload = json!({"sub_args_line":line,"patch_args":patch}).to_string();
        let response = execute(&f, &bob, &["start", "pipeline", "-P", &payload]).await;
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{patch}: {}",
            response.text()
        );
    }
    // Default None fields are not automatically rejected as dangerous changes.
    let payload=json!({"sub_args_line":"show version","patch_args":{"action":null,"executor_keys":null,"params":null}}).to_string();
    assert!(started(
        &execute(&f, &bob, &["start", "pipeline", "-P", &payload]).await
    ));
    // The legacy keyword API carries the same params dictionary, not an unrelated action.
    let response = send(
        &f.app,
        post_json(
            "/actions/chain",
            &bob,
            json!({"params":{"sub_args_line":line}}),
        ),
    )
    .await;
    assert_eq!(response.json()[0], true, "{}", response.text());
    let response = send(
        &f.app,
        post_json(
            "/actions/start",
            &bob,
            json!({"action":["pipeline"],"params":{"sub_args_line":line}}),
        ),
    )
    .await;
    assert_eq!(response.json()[0], true, "{}", response.text());
}

#[tokio::test]
async fn non_admin_executors_cannot_retarget_checked_commands_to_private_credentials() {
    let f = fixture(|cfg| cfg.actions_non_admin = true);
    let bob = common::login(&f.app, "bob", "pw", None).await;
    for flag in [
        "--executor-keys=api:private",
        "--exec=api:private",
        "-eapi:private",
        "-yeapi:private",
        "-vyeapi:private",
    ] {
        for uri in ["/execute", "/execute/validate"] {
            let response = send(
                &f.app,
                post_json(uri, &bob, json!({"sysargs":["show","version",flag]})),
            )
            .await;
            assert_eq!(
                response.status,
                StatusCode::FORBIDDEN,
                "{uri} {flag}: {}",
                response.text()
            );
        }
    }
    let response = send(
        &f.app,
        post_json(
            "/actions/show",
            &bob,
            json!({"action":["version"],"executor_keys":"api:private"}),
        ),
    )
    .await;
    assert_eq!(response.json()[0], false, "{}", response.text());
    assert!(f.actions.calls.lock().unwrap().is_empty());
    assert!(f.jobs.jobs.lock().unwrap().is_empty());
    for executor in [Json::Null, json!("local")] {
        let response = send(
            &f.app,
            post_json(
                "/actions/show",
                &bob,
                json!({"action":["version"],"executor_keys":executor}),
            ),
        )
        .await;
        assert_eq!(response.json()[0], true, "{}", response.text());
    }
    assert!(started(
        &execute(&f, &bob, &["show", "-e", "local", "version"]).await
    ));
    // The normal job/execute cleaner already strips the separate remote flag before running.
    let response = execute(&f, &bob, &["show", "version", "-e", "api:private"]).await;
    assert!(started(&response), "{}", response.text());
    let jobs = f.jobs.jobs.lock().unwrap();
    assert!(
        jobs.values()
            .all(|(words, _)| !words.iter().any(|word| word.contains("api:private")))
    );
}

#[tokio::test]
async fn no_auth_connector_inspection_keeps_existing_local_privileges() {
    let f = fixture(|cfg| cfg.no_auth = true);
    let response = send(
        &f.app,
        req(
            "POST",
            "/execute",
            None,
            Some((
                "application/json",
                json!({"sysargs":["show","connectors"]})
                    .to_string()
                    .into_bytes(),
            )),
        ),
    )
    .await;
    assert!(started(&response), "{}", response.text());
}

#[tokio::test]
async fn sensitive_pipe_mutations_cannot_bypass_dedicated_authorization_via_actions() {
    let f = fixture(|cfg| cfg.actions_non_admin = true);
    let bob = common::login(&f.app, "bob", "pw", None).await;
    let narrow_admin = common::login(&f.app, "admin", "pass", None).await;
    let narrow = api_key(
        &f,
        &narrow_admin,
        "sensitive-pipe-writes",
        json!([
            "actions:execute",
            "jobs:execute",
            "jobs:write",
            "pipes:read",
            "pipes:write"
        ]),
    )
    .await;
    for auth in [&bob, &narrow] {
        for (index, args) in [
            json!(["register", "pipes"]),
            json!(["edit", "definition"]),
            json!(["edit", "pipes", "definition"]),
            json!([
                "edit",
                "pipes",
                "-P",
                r#"{"fetch":{"definition":"SELECT 42"}}"#
            ]),
            json!(["edit", "pipes", "--params={\"access\":null}"]),
            json!(["edit", "pipes", "-vyP{\"locks\":[]}"]),
            json!([
                "start",
                "pipeline",
                "-P",
                r#"{"sub_args_line":"edit pipes","patch_args":{"params":{"sql":"SELECT 42"}}}"#
            ]),
        ]
        .iter()
        .enumerate()
        {
            for uri in ["/execute", "/execute/validate"] {
                let response = send(&f.app, post_json(uri, auth, json!({"sysargs":args}))).await;
                assert_eq!(
                    response.status,
                    StatusCode::FORBIDDEN,
                    "{uri} {args}: {}",
                    response.text()
                );
            }
            let response = send(
                &f.app,
                post_json(&format!("/jobs/mutation-{index}"), auth, args.clone()),
            )
            .await;
            assert_eq!(response.json()[0], false, "{args}: {}", response.text());
        }
        for (action, keywords) in [
            (
                "register",
                json!({"action":["pipes"],"params":{"access":{"owner":"alice"}}}),
            ),
            (
                "edit",
                json!({"action":["pipes"],"params":{"fetch":{"definition":"SELECT 42"}}}),
            ),
            ("edit", json!({"action":["pipes"],"params":{"locks":[]}})),
            (
                "chain",
                json!({"params":{"sub_args_line":"edit pipes","patch_args":{"params":{"access":null}}}}),
            ),
        ] {
            let response = send(
                &f.app,
                post_json(&format!("/actions/{action}"), auth, keywords),
            )
            .await;
            assert_eq!(response.json()[0], false, "{action}: {}", response.text());
        }
    }
    assert!(f.actions.calls.lock().unwrap().is_empty());
    assert!(f.jobs.jobs.lock().unwrap().is_empty());
    let safe = send(
        &f.app,
        post_json(
            "/actions/edit",
            &bob,
            json!({"action":["pipes"],"params":{"tags":["daily"],"description":"notes"}}),
        ),
    )
    .await;
    assert_eq!(safe.json()[0], true, "{}", safe.text());
    // The full administrator still has the trusted local CLI path.
    let full = api_key(&f, &narrow_admin, "sensitive-pipe-admin", json!(["*"])).await;
    for args in [
        vec!["register", "pipes"],
        vec!["edit", "pipes", "definition"],
    ] {
        assert!(started(&execute(&f, &full, &args).await));
    }
    let open = fixture(|cfg| cfg.no_auth = true);
    let response = send(
        &open.app,
        req(
            "POST",
            "/execute",
            None,
            Some((
                "application/json",
                json!({"sysargs":["register","pipes"]})
                    .to_string()
                    .into_bytes(),
            )),
        ),
    )
    .await;
    assert!(started(&response), "{}", response.text());
}

#[tokio::test]
async fn execute_reads_the_api_permissions_actions_non_admin_key() {
    // `api:permissions:actions:non_admin` (the console key; `ServerConfig::actions_non_admin`)
    // decides, whatever `system:api:permissions:actions:non_admin` says. The log stream asks the
    // same function (`is_user_allowed_to_execute_console`; see the web crate's tests).
    for (console_key, system_key) in [(true, false), (true, true), (false, true), (false, false)] {
        let f = fixture(|cfg| {
            cfg.actions_non_admin = console_key;
            cfg.system_actions_non_admin = system_key;
        });
        let bob = common::login(&f.app, "bob", "pw", None).await;
        let r = execute(&f, &bob, &["show", "version"]).await;
        let label = format!("console key {console_key}, system key {system_key}");
        if console_key {
            assert!(started(&r), "{label}: {} {}", r.status, r.text());
        } else {
            // v4's message, refused with `403` (the v5 routes' shape).
            assert_eq!(r.status, StatusCode::FORBIDDEN, "{label}: {}", r.text());
            assert_eq!(r.json()["success"], json!(false), "{label}: {}", r.text());
            assert!(
                r.json()["message"]
                    .as_str()
                    .unwrap()
                    .contains("has not allowed users to perform actions")
            );
            assert!(f.jobs.jobs.lock().unwrap().is_empty(), "{label}");
        }
    }
}

#[tokio::test]
async fn the_policy_is_published_for_the_console() {
    let f = fixture(|_| {});
    let bob = common::login(&f.app, "bob", "pw", None).await;
    let r = send(&f.app, req("GET", "/execute/policy", Some(&bob), None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let policy = r.json();
    let listed = |key: &str, name: &str| {
        policy[key]
            .as_array()
            .is_some_and(|a| a.iter().any(|v| v == name))
    };
    for action in [
        "api", "sh", "os", "sql", "stack", "reload", "repo", "instance",
    ] {
        assert!(listed("omit_actions", action), "{action}");
    }
    assert!(listed("non_admin_omit_actions", "python"));
    assert!(listed("omit_flags", "--shell"));
}

#[tokio::test]
async fn jobs_never_expose_credentials() {
    let f = fixture(|_| {});
    f.jobs.seed("plain-job", None);
    f.jobs.seed("execute-aaaa0001", Some("bob"));
    let admin = common::login(&f.app, "admin", "pass", None).await;

    let list = send(&f.app, req("GET", "/jobs", Some(&admin), None)).await;
    assert_eq!(list.status, StatusCode::OK);
    let one = send(&f.app, req("GET", "/jobs/plain-job", Some(&admin), None)).await;
    assert_eq!(one.status, StatusCode::OK);
    for (label, body) in [("GET /jobs", list.text()), ("GET /jobs/{name}", one.text())] {
        for secret in [
            "hunter2-db",
            "s3cret-api",
            "cfg-pass",
            "aws-secret-value",
            "ghp_token_value",
            "plain-password-value",
            "service-key-value",
        ] {
            assert!(!body.contains(secret), "{label} leaks {secret}: {body}");
        }
    }
    let props = &one.json()["daemon"]["properties"]["env"];
    // What is not a credential stays, and so do the non-secret parts of a connector URI.
    assert_eq!(props["HOME"], "/home/someone");
    assert_eq!(props["LD_LIBRARY_PATH"], "/usr/lib");
    assert_eq!(props["MRSM_SQL_LITE"], "sqlite:////data/x.db");
    let sql = props["MRSM_SQL_MAIN"].as_str().unwrap();
    assert!(sql.starts_with("postgresql://mrsm:") && sql.ends_with("@db.internal:5432/meerschaum"));
    assert!(props["MRSM_CONFIG"].as_str().unwrap().contains("\"host\""));
}

#[tokio::test]
async fn execute_jobs_are_visible_to_their_creator_and_administrators_only() {
    let f = fixture(|cfg| {
        cfg.actions_non_admin = true;
        // The job routes read v4's `system:` key.
        cfg.system_actions_non_admin = true;
    });
    let (admin, bob, eve) = (
        common::login(&f.app, "admin", "pass", None).await,
        common::login(&f.app, "bob", "pw", None).await,
        common::login(&f.app, "eve", "pw", None).await,
    );
    f.jobs.seed("plain-job", None);
    let r = execute(&f, &bob, &["show", "version"]).await;
    assert!(started(&r), "{}", r.text());
    let name = r.json()["job"].as_str().unwrap().to_string();
    let r = execute(&f, &admin, &["show", "version"]).await;
    let admin_name = r.json()["job"].as_str().unwrap().to_string();

    let names = |r: &common::Resp| -> Vec<String> {
        let mut v: Vec<String> = r.json().as_object().unwrap().keys().cloned().collect();
        v.sort();
        v
    };
    let get = |auth: &str, uri: String| send(&f.app, req("GET", &uri, Some(auth), None));

    // The listing.
    let for_bob = get(&bob, "/jobs".into()).await;
    assert!(names(&for_bob).contains(&name) && !names(&for_bob).contains(&admin_name));
    assert!(names(&for_bob).contains(&"plain-job".to_string()));
    let for_eve = get(&eve, "/jobs".into()).await;
    assert!(!names(&for_eve).contains(&name) && !names(&for_eve).contains(&admin_name));
    assert!(names(&for_eve).contains(&"plain-job".to_string()));
    let for_admin = get(&admin, "/jobs".into()).await;
    assert!(names(&for_admin).contains(&name) && names(&for_admin).contains(&admin_name));

    // The routes that name the job.
    for suffix in ["", "/exists", "/stop_time", "/is_blocking_on_stdin"] {
        let uri = format!("/jobs/{name}{suffix}");
        assert_eq!(get(&bob, uri.clone()).await.status, StatusCode::OK, "{uri}");
        assert_eq!(
            get(&admin, uri.clone()).await.status,
            StatusCode::OK,
            "{uri}"
        );
    }
    let r = get(&eve, format!("/jobs/{name}")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    assert!(!r.text().contains("output of"));
    assert_eq!(
        get(&eve, format!("/jobs/{name}/exists")).await.json(),
        json!(false)
    );
    assert_eq!(
        get(&bob, format!("/jobs/{name}/exists")).await.json(),
        json!(true)
    );
    assert_eq!(
        get(&eve, format!("/logs/{name}")).await.status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        get(&bob, format!("/logs/{name}")).await.status,
        StatusCode::OK
    );
    assert_eq!(
        get(&admin, format!("/logs/{name}")).await.status,
        StatusCode::OK
    );
    assert_eq!(
        get(&bob, format!("/logs/{admin_name}")).await.status,
        StatusCode::NOT_FOUND
    );

    // Acting on someone else's job.
    for action in ["start", "stop", "pause"] {
        let r = send(
            &f.app,
            req("POST", &format!("/jobs/{name}/{action}"), Some(&eve), None),
        )
        .await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{action}: {}", r.text());
    }
    let r = send(
        &f.app,
        req("DELETE", &format!("/jobs/{name}"), Some(&eve), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    assert!(
        f.jobs.jobs.lock().unwrap().contains_key(&name),
        "still there"
    );
    let r = send(
        &f.app,
        req("DELETE", &format!("/jobs/{name}"), Some(&bob), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(!f.jobs.jobs.lock().unwrap().contains_key(&name));
}

#[tokio::test]
async fn a_job_name_chosen_by_a_non_admin_keeps_the_execute_prefix() {
    let f = fixture(|cfg| cfg.actions_non_admin = true);
    let bob = common::login(&f.app, "bob", "pw", None).await;
    // A name outside the `execute-` family would escape the owner rule.
    let r = send(
        &f.app,
        post_json(
            "/execute",
            &bob,
            json!({"sysargs": ["show", "version"], "name": "mine"}),
        ),
    )
    .await;
    assert_eq!(r.status.as_u16(), 400, "{}", r.text());
    let r = send(
        &f.app,
        post_json(
            "/execute",
            &bob,
            json!({"sysargs": ["show", "version"], "name": "execute-mine"}),
        ),
    )
    .await;
    assert!(started(&r), "{}", r.text());
}
