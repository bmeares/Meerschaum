//! Wave 18 (FX7): the authorization of `POST /execute` and the exposure of `GET /jobs`.
//!
//! - A non-administrator runs through `/execute` only what the console would offer: the actions
//!   the console omits stay refused, and so do the flags it does not offer (`--shell`, ...);
//!   administrators keep v4's `/actions` latitude. The gate reads
//!   `api:permissions:actions:non_admin` (`ServerConfig::actions_non_admin`), not the `system:`
//!   key the `/actions` and `/jobs` routes read.
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
use meerschaum_core::{Map, SuccessTuple, Value};
use meerschaum_server::AppState;
use meerschaum_server::backends::{CreateJob, JobsBackend};
use serde_json::{Value as Json, json};

/// A job as the fake backend keeps it: the sysargs and the properties `create` was given.
type Stored = (Vec<String>, Option<Map>);

/// Jobs in memory; each job's metadata carries the `env` the real backend would (the server's
/// whole environment, here a few credentials).
#[derive(Default)]
struct FakeJobs {
    jobs: Mutex<IndexMap<String, Stored>>,
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
    _dir: tempfile::TempDir,
}

/// Users: `admin`, `bob` and `eve` (non-administrators with every scope).
fn fixture(tweak: impl FnOnce(&mut meerschaum_server::ServerConfig)) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("bob", "pw", None);
    instance.seed_user("eve", "pw", None);
    let mut cfg = common::config(dir.path());
    tweak(&mut cfg);
    let jobs = Arc::new(FakeJobs::default());
    let state = AppState::builder(cfg, instance)
        .secret(SECRET.to_vec())
        .jobs(jobs.clone())
        .build()
        .unwrap();
    Fixture {
        app: meerschaum_server::app(state),
        jobs,
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
        vec!["show", "pipes", "-i", "sql:main"],
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
            "sql:main",
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
        vec!["edit", "pipes", "definition"],
        vec!["register", "pipes"],
        vec!["show", "jobs"],
        // Another pipe's metric may be called anything, including an omitted action's name.
        vec!["show", "pipes", "-m", "python", "-c", "sh"],
    ] {
        let r = execute(&f, &bob, &sysargs).await;
        assert!(started(&r), "{sysargs:?}: {} {}", r.status, r.text());
    }
}

#[tokio::test]
async fn an_administrator_keeps_the_latitude_of_actions() {
    let f = fixture(|cfg| cfg.actions_non_admin = false);
    let admin = common::login(&f.app, "admin", "pass", None).await;
    for sysargs in [
        vec!["python", "-c", "print(1)"],
        vec!["show", "config"],
        vec!["stack", "ps"],
        vec!["show", "pipes", "--shell"],
    ] {
        let r = execute(&f, &admin, &sysargs).await;
        assert!(started(&r), "{sysargs:?}: {} {}", r.status, r.text());
    }
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
