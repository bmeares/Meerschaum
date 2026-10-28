//! The 5.0.0rc15 HTTP changes: the v5-only routes' one JSON shape (Q13), the saved Pipelines'
//! detail, plan and sync routes and the run filters (Q12), fail-closed scopes (S1), both
//! rate-limit spellings and path prefixes (S2), engine reads with `pipes:read` and failed reads
//! that are errors (B6), the public preset (Q34) and the row-count estimate parameter (C1).

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use common::fakes::{BrokenStore, FakePlans, RecordingJobs};
use common::{MemoryInstance, SECRET, config, login, req, send};
use meerschaum_connector::InstanceConnector;
use meerschaum_core::{Pipe, Value};
use meerschaum_server::backends::{EngineBackend, MemoryEngine};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};

fn body(j: Json) -> Option<(&'static str, Vec<u8>)> {
    Some(("application/json", j.to_string().into_bytes()))
}

struct App {
    app: axum::Router,
    state: AppState,
    instance: Arc<MemoryInstance>,
    jobs: Arc<RecordingJobs>,
    _root: tempfile::TempDir,
}

fn build(cfg: impl FnOnce(&mut ServerConfig), engine: Option<Arc<dyn EngineBackend>>) -> App {
    let root = tempfile::tempdir().unwrap();
    let mut c = config(root.path());
    cfg(&mut c);
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("ron", "pass", None);
    instance.seed_user_scopes("ron", &["pipes:read"]);
    instance.seed_user("wes", "pass", None);
    instance.seed_user_scopes("wes", &["pipes:read", "pipes:write", "jobs:execute"]);
    let jobs = Arc::new(RecordingJobs::default());
    let state = AppState::builder(c, instance.clone())
        .secret(SECRET.to_vec())
        .jobs(jobs.clone())
        .pipelines(Arc::new(FakePlans))
        .engine(engine.unwrap_or_else(|| Arc::new(MemoryEngine::default())))
        .build()
        .unwrap();
    App {
        app: meerschaum_server::app(state.clone()),
        state,
        instance,
        jobs,
        _root: root,
    }
}

fn is_envelope(j: &Json, success: bool) -> bool {
    j["success"] == json!(success) && j["message"].is_string()
}

async fn register_pipeline(app: &axum::Router, auth: &str, name: &str, targets: Json) {
    let r = send(
        app,
        req(
            "POST",
            &format!("/pipelines/{name}/register"),
            Some(auth),
            body(
                json!({"pipeline": {"name": name, "id": format!("id-{name}"), "revision": 3,
                "targets": targets}}),
            ),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(is_envelope(&r.json(), true), "{}", r.text());
}

/// Capture a registered member before the fixture produces its run history.
async fn captured_member(a: &App, connector: &str, metric: &str) -> (String, String) {
    let pipe = Pipe::builder(connector, metric)
        .instance("sql:fixture")
        .build()
        .unwrap();
    assert!(a.instance.register_pipe(&pipe).await.success);
    let attributes = a.instance.get_pipe_attributes(&pipe).await.unwrap();
    let identity =
        meerschaum_connector::sync_status::registration_from_attributes(&attributes).unwrap();
    (pipe.pipe_ref(), identity)
}

#[tokio::test]
async fn an_api_started_run_waits_for_its_trusted_header_and_rejects_run_id_collisions() {
    let a = build(|_| {}, None);
    let admin = login(&a.app, "admin", "pass", None).await;
    let wes = login(&a.app, "wes", "pass", None).await;
    let (member, registration) = captured_member(&a, "sql:b", "raw").await;
    register_pipeline(
        &a.app,
        &admin,
        "early",
        json!(["sql:fixture|sql:b|raw|None"]),
    )
    .await;
    let saved = Pipe::builder("mrsm", "pipeline")
        .location(Some("early"))
        .instance("sql:fixture")
        .build()
        .unwrap();
    let attributes = a.instance.get_pipe_attributes(&saved).await.unwrap();
    let pipeline_identity = attributes
        .get("parameters")
        .and_then(|parameters| parameters.get("pipeline"))
        .and_then(|definition| definition.get("id"))
        .and_then(Value::as_str)
        .unwrap()
        .to_string();
    let response = send(
        &a.app,
        req(
            "POST",
            "/pipelines/early/sync",
            Some(&wes),
            body(json!({"run_id":"early-open"})),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let stream_url = response.json()["stream"].as_str().unwrap().to_string();
    // Job metadata reserves the accepted ID even before the runner has persisted history.
    assert!(
        a.state
            .engine
            .get_runs(&meerschaum_server::backends::RunsFilter {
                run_id: Some("early-open".into()),
                ..Default::default()
            })
            .await
            .unwrap()
            .is_empty()
    );
    let duplicate = send(
        &a.app,
        req(
            "POST",
            "/pipelines/early/sync",
            Some(&wes),
            body(json!({"run_id":"early-open"})),
        ),
    )
    .await;
    assert_eq!(
        duplicate.status,
        StatusCode::CONFLICT,
        "{}",
        duplicate.text()
    );
    let now = chrono::Utc::now();
    assert!(
        a.state
            .engine
            .record_events(
                serde_json::from_value(json!([
                    {"run_id":"early-open","created":now,"event":"run_started","payload":{}}
                ]))
                .unwrap()
            )
            .await
            .success
    );
    let no_header = send(
        &a.app,
        req(
            "GET",
            "/engine/runs/early-open/events/stream",
            Some(&wes),
            None,
        ),
    )
    .await;
    assert_eq!(no_header.status, StatusCode::NOT_FOUND);
    let engine = a.state.engine.clone();
    let producer = tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        // As in the native writer, persist the immutable node snapshot before its header.
        assert!(
            engine
                .record_runs(
                    serde_json::from_value(json!([
                        {"run_id":"early-open","pipe_ref":member,"registration_id":registration,
                         "started":now,"finished":now,"status":"succeeded"}
                    ]))
                    .unwrap()
                )
                .await
                .success
        );
        assert!(engine.record_runs(serde_json::from_value(json!([
            {"run_id":"early-open","started":now,"status":"success","finished":now,"num_nodes":1,"targets":[member],
             "selection":{"pipeline_definitions":[{"id":pipeline_identity,"name":"early","registry":"sql:fixture"}]}}
        ])).unwrap()).await.success);
        assert!(engine.record_events(serde_json::from_value(json!([
            {"run_id":"early-open","created":now,"event":"run_finished","payload":{"success":true}}
        ])).unwrap()).await.success);
    });
    let stream = send(&a.app, req("GET", &stream_url, Some(&wes), None)).await;
    assert_eq!(stream.status, StatusCode::OK, "{}", stream.text());
    assert!(stream.text().contains("run_started") && stream.text().contains("run_finished"));
    producer.await.unwrap();

    let launch = || {
        send(
            &a.app,
            req(
                "POST",
                "/pipelines/early/sync",
                Some(&wes),
                body(json!({"run_id":"simultaneous"})),
            ),
        )
    };
    let (first, second) = tokio::join!(launch(), launch());
    let mut statuses = [first.status, second.status];
    statuses.sort();
    assert_eq!(statuses, [StatusCode::OK, StatusCode::CONFLICT]);
    assert_eq!(
        a.jobs
            .created
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, args, _)| args.iter().any(|arg| arg == "simultaneous"))
            .count(),
        1
    );

    // An existing hidden run cannot be overwritten by launching a public Pipeline.
    assert!(a.state.engine.record_runs(serde_json::from_value(json!([
        {"run_id":"hidden-existing","started":now,"status":"running",
         "selection":{"pipeline_definitions":[{"name":"not-visible","id":"private-id","registry":"sql:fixture"}]}}
    ])).unwrap()).await.success);
    let before = a.jobs.created.lock().unwrap().len();
    let conflict = send(
        &a.app,
        req(
            "POST",
            "/pipelines/early/sync",
            Some(&wes),
            body(json!({"run_id":"hidden-existing"})),
        ),
    )
    .await;
    assert_eq!(conflict.status, StatusCode::CONFLICT, "{}", conflict.text());
    assert_eq!(a.jobs.created.lock().unwrap().len(), before);
}

#[derive(Default)]
struct DelayedRunRead {
    inner: MemoryEngine,
    block: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait::async_trait]
impl EngineBackend for DelayedRunRead {
    async fn append_ledger(
        &self,
        v: Vec<meerschaum_connector_api::models::LedgerEntry>,
    ) -> meerschaum_core::SuccessTuple {
        self.inner.append_ledger(v).await
    }
    async fn ledger_after(
        &self,
        r: &str,
        s: Option<i64>,
    ) -> Vec<meerschaum_connector_api::models::LedgerEntry> {
        self.inner.ledger_after(r, s).await
    }
    async fn put_cursors(
        &self,
        v: Vec<meerschaum_connector_api::models::EdgeCursor>,
    ) -> meerschaum_core::SuccessTuple {
        self.inner.put_cursors(v).await
    }
    async fn get_state(&self, r: &str) -> Option<meerschaum_connector_api::models::EngineState> {
        self.inner.get_state(r).await
    }
    async fn put_states(
        &self,
        v: Vec<meerschaum_connector_api::models::EngineState>,
    ) -> meerschaum_core::SuccessTuple {
        self.inner.put_states(v).await
    }
    async fn record_runs(
        &self,
        v: Vec<meerschaum_connector_api::models::RunRecord>,
    ) -> meerschaum_core::SuccessTuple {
        self.inner.record_runs(v).await
    }
    async fn get_runs(
        &self,
        f: &meerschaum_server::backends::RunsFilter,
    ) -> Result<
        Vec<meerschaum_connector_api::models::RunRecord>,
        meerschaum_server::backends::EngineStoreError,
    > {
        if self.block.swap(false, std::sync::atomic::Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        self.inner.get_runs(f).await
    }
    async fn record_events(
        &self,
        v: Vec<meerschaum_connector_api::models::EngineEvent>,
    ) -> meerschaum_core::SuccessTuple {
        self.inner.record_events(v).await
    }
    async fn acquire_lease(&self, r: &str, h: &str, t: std::time::Duration) -> bool {
        self.inner.acquire_lease(r, h, t).await
    }
    async fn release_lease(&self, r: &str, h: &str) -> bool {
        self.inner.release_lease(r, h).await
    }
}

#[tokio::test]
async fn cancelling_a_launch_request_leaves_one_job_reservation_and_no_running_placeholder() {
    let engine = Arc::new(DelayedRunRead::default());
    engine
        .block
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let a = build(|_| {}, Some(engine.clone()));
    let admin = login(&a.app, "admin", "pass", None).await;
    register_pipeline(&a.app, &admin, "cancelled", json!([])).await;
    let app = a.app.clone();
    let auth = admin.clone();
    let request = tokio::spawn(async move {
        send(
            &app,
            req(
                "POST",
                "/pipelines/cancelled/sync",
                Some(&auth),
                body(json!({"run_id":"cancelled-request"})),
            ),
        )
        .await
    });
    tokio::time::timeout(std::time::Duration::from_secs(1), engine.entered.notified())
        .await
        .unwrap();
    request.abort();
    assert!(matches!(request.await,Err(error) if error.is_cancelled()));
    let blocked = send(
        &a.app,
        req(
            "POST",
            "/pipelines/cancelled/sync",
            Some(&admin),
            body(json!({"run_id":"cancelled-request"})),
        ),
    )
    .await;
    assert_eq!(blocked.status, StatusCode::CONFLICT, "{}", blocked.text());
    engine.release.notify_one();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if a.jobs.created.lock().unwrap().len() == 1 {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        engine
            .inner
            .get_runs(&Default::default())
            .await
            .unwrap()
            .is_empty()
    );
    let second = send(
        &a.app,
        req(
            "POST",
            "/pipelines/cancelled/sync",
            Some(&admin),
            body(json!({"run_id":"cancelled-request"})),
        ),
    )
    .await;
    assert_eq!(second.status, StatusCode::CONFLICT, "{}", second.text());
    assert_eq!(a.jobs.created.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn a_timed_out_launch_releases_its_reservation_without_fabricating_history() {
    let engine = Arc::new(DelayedRunRead::default());
    engine
        .block
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let a = build(|_| {}, Some(engine.clone()));
    let admin = login(&a.app, "admin", "pass", None).await;
    register_pipeline(&a.app, &admin, "timeout", json!([])).await;
    let app = a.app.clone();
    let auth = admin.clone();
    let request = tokio::spawn(async move {
        send(
            &app,
            req(
                "POST",
                "/pipelines/timeout/sync",
                Some(&auth),
                body(json!({"run_id":"timed-out"})),
            ),
        )
        .await
    });
    engine.entered.notified().await;
    tokio::time::advance(std::time::Duration::from_secs(61)).await;
    let response = request.await.unwrap();
    assert_eq!(
        response.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "{}",
        response.text()
    );
    assert!(a.jobs.created.lock().unwrap().is_empty());
    assert!(
        engine
            .inner
            .get_runs(&Default::default())
            .await
            .unwrap()
            .is_empty()
    );
    let retry = send(
        &a.app,
        req(
            "POST",
            "/pipelines/timeout/sync",
            Some(&admin),
            body(json!({"run_id":"timed-out"})),
        ),
    )
    .await;
    assert_eq!(retry.status, StatusCode::OK, "{}", retry.text());
    assert_eq!(a.jobs.created.lock().unwrap().len(), 1);
}

// ------------------------------------------------------------------------------------------------
// Q13: one shape for the v5-only routes, real status codes; v4 routes unchanged
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn v5_routes_answer_success_and_message_with_real_status_codes() {
    let a = build(|_| {}, None);
    let admin = login(&a.app, "admin", "pass", None).await;
    // Authentication.
    let r = send(&a.app, req("GET", "/pipelines", None, None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert!(is_envelope(&r.json(), false), "{}", r.text());
    // Scopes.
    let ron = login(&a.app, "ron", "pass", None).await;
    let r = send(
        &a.app,
        req(
            "POST",
            "/pipelines/daily/register",
            Some(&ron),
            body(json!({})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    // Registering needs `pipes:register` (rc15's access model; `pipes:write` stands in for it).
    assert_eq!(
        r.json()["message"],
        "Missing required scope: 'pipes:register'"
    );
    // Validation (pydantic's 422 is a 400 here, with the errors).
    let r = send(&a.app, req("POST", "/execute", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.text());
    assert!(is_envelope(&r.json(), false));
    assert!(r.json()["errors"].is_array());
    assert!(r.json()["message"].as_str().unwrap().contains("body"));
    // Missing, conflict.
    let r = send(&a.app, req("GET", "/pipelines/nope", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(is_envelope(&r.json(), false));
    register_pipeline(
        &a.app,
        &admin,
        "daily",
        json!(["sql:fixture|sql:b|raw|None"]),
    )
    .await;
    let r = send(
        &a.app,
        req(
            "POST",
            "/pipelines/daily/register",
            Some(&admin),
            body(json!({"pipeline": {"name": "daily"}})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::CONFLICT);
    assert!(is_envelope(&r.json(), false));
    let r = send(
        &a.app,
        req("GET", "/pipelines/daily/id", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(r.json()["id"].is_number(), "{}", r.text());
    let r = send(
        &a.app,
        req("GET", "/pipelines/daily/attributes", Some(&admin), None),
    )
    .await;
    assert_eq!(
        r.json()["attributes"]["parameters"]["pipeline"]["name"],
        "daily"
    );
    let r = send(&a.app, req("GET", "/pipelines/keys", Some(&admin), None)).await;
    assert_eq!(r.json()["keys"][0][2], "daily", "{}", r.text());
    let r = send(&a.app, req("GET", "/pipelines", Some(&admin), None)).await;
    assert_eq!(r.json()["pipelines"][0]["name"], "daily");
    // Unknown paths and methods under the v5 prefixes.
    let r = send(&a.app, req("GET", "/engine/nope", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(is_envelope(&r.json(), false));
    let r = send(&a.app, req("DELETE", "/engine/runs", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::METHOD_NOT_ALLOWED);
    assert!(is_envelope(&r.json(), false));
    // v4 routes keep their shapes.
    let r = send(&a.app, req("GET", "/pipes/keys", None, None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert!(r.json()["detail"].is_string(), "{}", r.text());
    let r = send(
        &a.app,
        req("GET", "/pipes/sql:x/m/None/nope", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert_eq!(r.json(), json!({"detail": "Not Found"}));
}

#[tokio::test]
async fn failures_name_what_failed_only_to_administrators() {
    let a = build(|_| {}, Some(Arc::new(BrokenStore)));
    let admin = login(&a.app, "admin", "pass", None).await;
    let ron = login(&a.app, "ron", "pass", None).await;
    let r = send(&a.app, req("GET", "/engine/runs", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(is_envelope(&r.json(), false));
    assert!(
        r.json()["message"]
            .as_str()
            .unwrap()
            .contains("sql:secret-host"),
        "{}",
        r.text()
    );
    let r = send(&a.app, req("GET", "/engine/runs", Some(&ron), None)).await;
    assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(
        r.json()["message"],
        meerschaum_server::envelope::GENERIC_FAILURE
    );
    assert!(!r.text().contains("secret-host"));
    // A failed write is a failure with the store's message (for an administrator).
    let r = send(
        &a.app,
        req(
            "POST",
            "/engine/runs",
            Some(&admin),
            body(json!([{"run_id": "r1", "started": "2026-01-01T00:00:00Z", "status": "success"}])),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(r.json()["message"].as_str().unwrap().contains("locked"));
}

// ------------------------------------------------------------------------------------------------
// B6: reads with `pipes:read`, failed reads are errors, `/execute` names only served streams
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn engine_reads_need_only_pipes_read_and_failed_reads_are_errors() {
    let a = build(|_| {}, None);
    let admin = login(&a.app, "admin", "pass", None).await;
    let ron = login(&a.app, "ron", "pass", None).await;
    let (member, registration) = captured_member(&a, "sql:x", "m").await;
    let record = |id: &str, started: &str, status: &str| {
        json!({"run_id": id, "started": started, "status": status, "num_nodes": 1, "targets": [member],
            "finished": if status == "running" { Json::Null } else { json!(started) }})
    };
    let r = send(
        &a.app,
        req(
            "POST",
            "/engine/runs",
            Some(&admin),
            body(json!([
                {"run_id": "r1", "pipe_ref": member, "registration_id": registration,
                 "started": "2026-01-01T00:00:00Z", "status": "success"},
                record("r1", "2026-01-01T00:00:00Z", "success")
            ])),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    // Reads take `pipes:read`; writes still need `pipes:write`.
    for path in [
        "/engine/runs",
        "/engine/runs/r1",
        "/engine/ledger?child_ref=x",
    ] {
        let r = send(&a.app, req("GET", path, Some(&ron), None)).await;
        assert_eq!(r.status, StatusCode::OK, "{path}: {}", r.text());
        assert!(is_envelope(&r.json(), true), "{path}");
    }
    let r = send(
        &a.app,
        req("POST", "/engine/runs", Some(&ron), body(json!([]))),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    // The stream accepts `pipes:read` (the run exists, so it opens).
    let r = send(
        &a.app,
        req(
            "GET",
            "/engine/runs/missing/events/stream",
            Some(&ron),
            None,
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    // A store that cannot be read: an error, never "no runs" or "not found".
    let b = build(|_| {}, Some(Arc::new(BrokenStore)));
    let admin = login(&b.app, "admin", "pass", None).await;
    let r = send(
        &b.app,
        req("GET", "/engine/runs/r1/events/stream", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR, "{}", r.text());
    let r = send(&b.app, req("GET", "/engine/runs/r1", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
async fn execute_names_the_log_stream_only_when_it_is_served() {
    let a = build(|_| {}, None);
    let admin = login(&a.app, "admin", "pass", None).await;
    let run = |sysargs: Json| {
        req(
            "POST",
            "/execute",
            Some(&admin),
            body(json!({ "sysargs": sysargs })),
        )
    };
    let r = send(&a.app, run(json!(["show", "version"]))).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let j = r.json();
    assert!(is_envelope(&j, true), "{j}");
    assert!(j["job"].as_str().unwrap().starts_with("execute-"));
    // `meerschaum-web` is not mounted: no stream, but the log route.
    assert!(j["stream"].is_null(), "{j}");
    assert_eq!(
        j["logs"],
        json!(format!("/logs/{}", j["job"].as_str().unwrap()))
    );
    a.state.mark_log_stream_served();
    let j = send(&a.app, run(json!(["show", "version"]))).await.json();
    assert_eq!(
        j["stream"],
        json!(format!("/logs/{}/stream", j["job"].as_str().unwrap()))
    );
    // Without the `jobs` group neither.
    let b = build(|c| c.routes_disabled = vec!["jobs".into()], None);
    let admin = login(&b.app, "admin", "pass", None).await;
    let r = send(
        &b.app,
        req(
            "POST",
            "/execute",
            Some(&admin),
            body(json!({"sysargs": ["show", "version"]})),
        ),
    )
    .await;
    let j = r.json();
    assert!(j["stream"].is_null() && j["logs"].is_null(), "{j}");
    // The policy and validation answer in the same shape.
    let j = send(&b.app, req("GET", "/execute/policy", None, None))
        .await
        .json();
    assert!(is_envelope(&j, true) && j["omit_actions"].is_array(), "{j}");
}

// ------------------------------------------------------------------------------------------------
// Q12: one Pipeline, its plan, a background sync, and the run filters
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn a_pipelines_detail_plan_and_sync() {
    let a = build(|_| {}, None);
    let admin = login(&a.app, "admin", "pass", None).await;
    register_pipeline(
        &a.app,
        &admin,
        "daily",
        json!([
            "sql:fixture|sql:b|raw|None",
            "sql:elsewhere|sql:b|other|None"
        ]),
    )
    .await;
    // A run of it (its definition recorded in `selection`) and one of another Pipeline.
    let selection = json!({"pipeline_definitions": [{"name": "daily", "id": "id-daily"}]});
    let r = send(
        &a.app,
        req(
            "POST",
            "/engine/runs",
            Some(&admin),
            body(json!([
                {"run_id": "old", "started": "2026-01-01T00:00:00Z", "status": "success",
                 "finished": "2026-01-01T00:01:00Z", "selection": selection.to_string()},
                {"run_id": "other", "started": "2026-01-02T00:00:00Z", "status": "failed",
                 "finished": "2026-01-02T00:01:00Z"},
            ])),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());

    let j = send(&a.app, req("GET", "/pipelines/daily", Some(&admin), None))
        .await
        .json();
    assert!(is_envelope(&j, true), "{j}");
    assert_eq!(j["pipeline"]["name"], "daily");
    assert_eq!(j["members"].as_array().unwrap().len(), 2);
    assert_eq!(j["members"][0]["metric_key"], "raw");
    assert_eq!(j["members"][0]["registered"], false);
    // An instance the server cannot open is not checked.
    assert!(j["members"][1]["registered"].is_null(), "{j}");
    assert_eq!(j["last_run"]["run_id"], "old", "{j}");

    // The plan: read-only, and the planner's refusals are the Pipeline's (400).
    let r = send(
        &a.app,
        req(
            "POST",
            "/pipelines/daily/plan",
            Some(&admin),
            body(json!({"begin": "2026-01-01"})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let j = r.json();
    assert_eq!(j["plan"]["nodes"][0]["action"], "sync");
    assert!(j["text"].as_str().unwrap().contains("2026-01-01"));
    assert!(j["message"].as_str().unwrap().contains("1 pipe"), "{j}");
    let ron = login(&a.app, "ron", "pass", None).await;
    let r = send(
        &a.app,
        req("POST", "/pipelines/daily/plan", Some(&ron), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "plans need only pipes:read");
    register_pipeline(&a.app, &admin, "broken", json!([])).await;
    let r = send(
        &a.app,
        req("POST", "/pipelines/broken/plan", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert!(r.json()["message"].as_str().unwrap().contains("Missing"));
    let r = send(
        &a.app,
        req("POST", "/pipelines/nope/plan", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = send(
        &a.app,
        req(
            "POST",
            "/pipelines/daily/plan",
            Some(&admin),
            body(json!({"begin": [1]})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);

    // The sync starts a background job the way the console does.
    let r = send(
        &a.app,
        req(
            "POST",
            "/pipelines/daily/sync",
            Some(&admin),
            body(json!({"end": "2026-02-01", "run_id": "my-run-1"})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let j = r.json();
    assert!(is_envelope(&j, true), "{j}");
    assert_eq!(j["run_id"], "my-run-1");
    assert!(j["job"].as_str().unwrap().starts_with("execute-"));
    assert_eq!(
        j["stream"], "/engine/runs/my-run-1/events/stream?wait=30",
        "{j}"
    );
    let created = a.jobs.created.lock().unwrap().clone();
    let (name, sysargs, properties) = created.last().unwrap().clone();
    assert_eq!(name, j["job"].as_str().unwrap());
    assert_eq!(
        sysargs,
        [
            "sync",
            "pipelines",
            "daily",
            "-i",
            "sql:fixture",
            "--run-id",
            "my-run-1",
            "--end",
            "2026-02-01"
        ]
    );
    let execution = serde_json::to_value(&properties.unwrap()["mrsm_execution"]).unwrap();
    assert_eq!(execution["kind"], "temporary_execution");
    assert_eq!(execution["origin"], "api_pipeline_sync");
    assert_eq!(execution["pipeline"]["name"], "daily");
    assert_eq!(execution["pipeline"]["revision"], 3);
    assert_eq!(execution["owner"], "admin");
    // Scopes: `pipes:write` and `jobs:execute`.
    let r = send(
        &a.app,
        req("POST", "/pipelines/daily/sync", Some(&ron), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    // A non-administrator may run a Pipeline only on instances they may use.
    let wes = login(&a.app, "wes", "pass", None).await;
    let r = send(
        &a.app,
        req("POST", "/pipelines/broken/sync", Some(&wes), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = send(
        &a.app,
        req("POST", "/pipelines/daily/sync", Some(&wes), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        r.json()["message"]
            .as_str()
            .unwrap()
            .contains("sql:elsewhere")
    );
    let r = send(
        &a.app,
        req(
            "POST",
            "/pipelines/daily/sync",
            Some(&admin),
            body(json!({"run_id": "_bad"})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn runs_filter_by_pipeline_pipe_status_and_since() {
    let a = build(|_| {}, None);
    let admin = login(&a.app, "admin", "pass", None).await;
    let daily = json!({"pipeline_definitions": [{"name": "daily"}]}).to_string();
    let r = send(
        &a.app,
        req(
            "POST",
            "/engine/runs",
            Some(&admin),
            body(json!([
                {"run_id": "a", "started": "2026-01-01T00:00:00Z", "status": "success",
                 "finished": "2026-01-01T00:01:00Z", "selection": daily},
                {"run_id": "b", "started": "2026-01-02T00:00:00Z", "status": "failed",
                 "finished": "2026-01-02T00:01:00Z", "selection": daily},
                {"run_id": "c", "started": "2026-01-03T00:00:00Z", "status": "running"},
                {"run_id": "b", "pipe_ref": "sql:fixture|sql:x|m|None",
                 "started": "2026-01-02T00:00:00Z", "status": "failed"},
                {"run_id": "c", "pipe_ref": "sql:fixture|sql:x|m|None",
                 "started": "2026-01-03T00:00:00Z", "status": "running"},
            ])),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let ids = |query: &'static str| {
        let app = a.app.clone();
        let admin = admin.clone();
        async move {
            let r = send(
                &app,
                req("GET", &format!("/engine/runs{query}"), Some(&admin), None),
            )
            .await;
            assert_eq!(r.status, StatusCode::OK, "{query}: {}", r.text());
            r.json()["runs"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|r| r["pipe_ref"].is_null())
                .map(|r| r["run_id"].as_str().unwrap().to_string())
                .collect::<Vec<_>>()
        }
    };
    assert_eq!(ids("").await, ["c", "b", "a"]);
    assert_eq!(ids("?pipeline=daily").await, ["b", "a"]);
    assert_eq!(ids("?status=failed").await, ["b"]);
    assert_eq!(ids("?status=running").await, ["c"]);
    assert_eq!(
        ids("?pipe=sql:fixture%7Csql:x%7Cm%7CNone").await,
        ["c", "b"]
    );
    assert_eq!(ids("?since=2026-01-02T00:00:00Z").await, ["c", "b"]);
    assert_eq!(ids("?limit=1").await, ["c"]);
    assert_eq!(ids("?pipeline=daily&status=success").await, ["a"]);
    // One run and its nodes.
    let j = send(&a.app, req("GET", "/engine/runs/c", Some(&admin), None))
        .await
        .json();
    assert_eq!(j["run"]["run_id"], "c");
    assert_eq!(j["nodes"].as_array().unwrap().len(), 1, "{j}");
    let r = send(&a.app, req("GET", "/engine/runs/zzz", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    // Bad filters are bad requests.
    for query in [
        "?status=maybe",
        "?since=yesterdayish",
        "?limit=0",
        "?limit=x",
    ] {
        let r = send(
            &a.app,
            req("GET", &format!("/engine/runs{query}"), Some(&admin), None),
        )
        .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{query}: {}", r.text());
    }
}

// ------------------------------------------------------------------------------------------------
// S1: fail closed
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn login_and_scoped_requests_fail_closed_when_permissions_cannot_be_read() {
    let a = build(|_| {}, None);
    let admin = login(&a.app, "admin", "pass", None).await;
    let ron = login(&a.app, "ron", "pass", None).await;
    // An API key with `pipes:read`.
    let r = send(
        &a.app,
        req(
            "POST",
            "/tokens/register",
            Some(&admin),
            body(json!({"label": "reader", "scopes": ["pipes:read"]})),
        ),
    )
    .await;
    let key = format!("Bearer {}", r.json()["api_key"].as_str().unwrap());
    assert_eq!(
        send(&a.app, req("GET", "/pipes/keys", Some(&key), None))
            .await
            .status,
        StatusCode::OK
    );

    a.instance.fail_permission_reads(true);
    // A password login cannot read the user's scopes: it fails, it does not grant everything.
    let form = |user: &str| {
        Some((
            "application/x-www-form-urlencoded",
            format!("username={user}&password=pass").into_bytes(),
        ))
    };
    for user in ["ron", "admin"] {
        let r = send(&a.app, req("POST", "/login", None, form(user))).await;
        assert_eq!(
            r.status,
            StatusCode::SERVICE_UNAVAILABLE,
            "{user}: {}",
            r.text()
        );
        assert!(r.json()["detail"].as_str().unwrap().contains("permissions"));
    }
    // An API key's scopes are read on every request: refused.
    let r = send(&a.app, req("GET", "/pipes/keys", Some(&key), None)).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{}", r.text());
    // A JWT carries its scopes; what needs a fresh read refuses.
    let r = send(&a.app, req("GET", "/pipes/keys", Some(&ron), None)).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = send(
        &a.app,
        req(
            "POST",
            "/tokens/register",
            Some(&ron),
            body(json!({"label": "t", "scopes": ["pipes:read"]})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{}", r.text());

    a.instance.fail_permission_reads(false);
    let r = send(&a.app, req("POST", "/login", None, form("ron"))).await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn a_valkey_instance_fails_closed_when_scopes_cannot_be_read() {
    use meerschaum_connector_valkey::{MemoryBackend, ValkeyBackend, ValkeyConnector};
    let root = tempfile::tempdir().unwrap();
    let backend = Arc::new(MemoryBackend::new());
    let conn = Arc::new(ValkeyConnector::with_backend(
        "s1",
        meerschaum_core::Map::new(),
        backend.clone(),
    ));
    let mut ron = meerschaum_core::users::User::new("ron", Some("pass"));
    ron.attributes = Some(
        serde_json::from_value::<Value>(json!({"scopes": ["pipes:read"]}))
            .unwrap()
            .to_map(),
    );
    assert!(conn.valkey_register_user(&mut ron).await.success);
    let mut c = config(root.path());
    c.instance_keys = "valkey:s1".into();
    let state = AppState::builder(c, conn.clone())
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let jwt = login(&app, "ron", "pass", None).await;
    let r = send(&app, req("GET", "/pipes/keys", Some(&jwt), None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());

    // Ron's attributes cannot be read (a key of the wrong type answers `WRONGTYPE`): the login
    // and the token registration are refused instead of granting every scope.
    let id = conn
        .valkey_get_user_id(&meerschaum_core::users::User::new("ron", None))
        .await
        .unwrap()
        .unwrap();
    let key = format!("mrsm_user:user_id:{id}:attributes");
    backend.del(std::slice::from_ref(&key)).await.unwrap();
    backend.sadd(&key, "not a string").await.unwrap();
    let form = Some((
        "application/x-www-form-urlencoded",
        b"username=ron&password=pass".to_vec(),
    ));
    let r = send(&app, req("POST", "/login", None, form)).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{}", r.text());
    assert!(r.json()["detail"].as_str().unwrap().contains("permissions"));
    let r = send(
        &app,
        req(
            "POST",
            "/tokens/register",
            Some(&jwt),
            body(json!({"label": "wide", "scopes": ["pipes:write"]})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{}", r.text());

    // Readable again: the login works and carries ron's own scope only.
    backend.del(std::slice::from_ref(&key)).await.unwrap();
    backend
        .set(&key, r#"{"scopes": ["pipes:read"]}"#)
        .await
        .unwrap();
    let jwt = login(&app, "ron", "pass", None).await;
    let r = send(
        &app,
        req(
            "POST",
            "/tokens/register",
            Some(&jwt),
            body(json!({"label": "wide", "scopes": ["pipes:write"]})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn credentials_that_cannot_be_read_are_refused_with_503_not_401() {
    let a = build(|_| {}, None);
    let admin = login(&a.app, "admin", "pass", None).await;
    let r = send(
        &a.app,
        req(
            "POST",
            "/tokens/register",
            Some(&admin),
            body(json!({"label": "reader", "scopes": ["pipes:read"]})),
        ),
    )
    .await;
    let key = format!("Bearer {}", r.json()["api_key"].as_str().unwrap());
    a.instance.fail_identity_reads(true);
    // A JWT's user (is the account still there?) and an API key's token cannot be read.
    for auth in [&admin, &key] {
        let r = send(&a.app, req("GET", "/pipes/keys", Some(auth), None)).await;
        assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{}", r.text());
    }
    let r = send(&a.app, req("GET", "/engine/runs", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(is_envelope(&r.json(), false));
    a.instance.fail_identity_reads(false);
    let r = send(&a.app, req("GET", "/pipes/keys", Some(&key), None)).await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn console_executions_refuse_when_scopes_cannot_be_read() {
    let a = build(|_| {}, None);
    a.instance.seed_user_scopes("ron", &["*"]);
    let ron = login(&a.app, "ron", "pass", None).await;
    a.instance.fail_permission_reads(true);
    let r = send(
        &a.app,
        req(
            "POST",
            "/execute",
            Some(&ron),
            body(json!({"sysargs": ["show", "version"]})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        r.json()["message"]
            .as_str()
            .unwrap()
            .contains("permissions")
    );
    assert!(a.jobs.created.lock().unwrap().is_empty());
}

// ------------------------------------------------------------------------------------------------
// S2: rate limits
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn rate_limits_take_both_spellings_and_path_prefixes() {
    let a = build(
        |c| {
            let mut notices = Vec::new();
            let limits: Value = serde_json::from_value(json!({
                "login": {"max": 2, "window_seconds": 60},
                "/my-plugin": {"requests": 1, "seconds": 60},
                "engine": {"requests": 1, "seconds": 60},
            }))
            .unwrap();
            c.rate_limits =
                meerschaum_server::config::parse_rate_limits(Some(&limits), &mut notices);
            assert!(notices.is_empty(), "{notices:?}");
        },
        None,
    );
    let form = || {
        Some((
            "application/x-www-form-urlencoded",
            b"username=admin&password=pass".to_vec(),
        ))
    };
    for _ in 0..2 {
        let r = send(&a.app, req("POST", "/login", None, form())).await;
        assert_eq!(r.status, StatusCode::OK);
    }
    let r = send(&a.app, req("POST", "/login", None, form())).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    assert!(r.headers.contains_key("retry-after"));
    // A plugin's paths, by prefix, in one bucket; a longer name is not under it.
    let r = send(&a.app, req("GET", "/my-plugin/a", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = send(&a.app, req("GET", "/my-plugin", None, None)).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    let r = send(&a.app, req("GET", "/my-plugins", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    // Encoded spellings are the same paths (the API host decodes them): same buckets.
    for path in ["/my%2Dplugin/a", "/%6Dy-plugin", "/l%6Fgin"] {
        let r = send(&a.app, req("GET", path, None, None)).await;
        assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS, "{path}");
    }
    // The v5 routes' 429 is in their shape.
    let _ = send(&a.app, req("GET", "/engine/runs", None, None)).await;
    let r = send(&a.app, req("GET", "/engine/runs", None, None)).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(
        r.json(),
        json!({"success": false, "message": "Rate limit exceeded."})
    );
    assert!(r.headers.contains_key("retry-after"));
}

// ------------------------------------------------------------------------------------------------
// Q34: the public preset
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn the_public_preset_serves_only_public_routes() {
    let a = build(
        |c| {
            let root = c.root_dir.clone();
            *c = ServerConfig::from_map(
                &serde_json::from_value::<Value>(json!({
                    "api": {"permissions": {"preset": "public"}}
                }))
                .unwrap()
                .to_map(),
                &meerschaum_server::ServerFlags {
                    instance: Some("sql:fixture".into()),
                    ..Default::default()
                },
            );
            let paths = ServerConfig::for_root("sql:fixture", &root);
            c.secret_key_path = paths.secret_key_path;
            c.root_dir = root;
        },
        None,
    );
    assert!(a.state.config.public);
    // Kept: login, health, version.
    let admin = login(&a.app, "admin", "pass", None).await;
    let r = send(&a.app, req("GET", "/healthcheck", None, None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = send(&a.app, req("GET", "/version", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
    // Off: actions, `/execute`, registration, the docs, every other core group and MCP.
    for (method, path) in [
        ("POST", "/actions/show"),
        ("GET", "/actions"),
        ("POST", "/execute"),
        ("GET", "/execute/policy"),
        ("POST", "/users/register"),
        ("GET", "/docs"),
        ("GET", "/redoc"),
        ("GET", "/openapi.json"),
        ("GET", "/pipes/keys"),
        ("GET", "/pipelines"),
        ("GET", "/engine/runs"),
        ("GET", "/jobs"),
        ("GET", "/connectors"),
        ("GET", "/info"),
        ("GET", "/chaining"),
        ("GET", "/mcp"),
        ("POST", "/mcp"),
        ("GET", "/"),
    ] {
        let r = send(&a.app, req(method, path, Some(&admin), None)).await;
        assert!(
            matches!(r.status.as_u16(), 404 | 405),
            "{method} {path}: {} {}",
            r.status,
            r.text()
        );
    }
    // Plugin routes still reach the API host; the docs and the terminal never do.
    let cfg = &a.state.config;
    assert!(meerschaum_server::routes::is_proxyable(
        cfg,
        "/my-plugin/data"
    ));
    assert!(meerschaum_server::routes::is_proxyable(
        cfg,
        "/_pages/report"
    ));
    assert!(meerschaum_server::routes::is_proxyable(
        cfg,
        "/dash/my-page"
    ));
    for path in [
        "/docs",
        "/openapi.json",
        "/info",
        "/pipes/keys",
        "/webterm/x",
        "/execute",
    ] {
        assert!(
            !meerschaum_server::routes::is_proxyable(cfg, path),
            "{path}"
        );
    }
    assert!(cfg.no_webterm && !cfg.registration_users);
    // The OpenAPI document of the preset lists only what it serves.
    let doc = serde_json::to_value(meerschaum_server::openapi::document(cfg)).unwrap();
    assert!(doc["paths"].get("/pipes/keys").is_none());
    assert!(doc["paths"].get("/healthcheck").is_some());
}

#[tokio::test]
async fn an_allowlist_adds_groups_to_the_public_preset() {
    let a = build(
        |c| {
            c.public = true;
            c.openapi_enabled = false;
            c.no_webterm = true;
            c.routes_allowlist = vec!["*".into(), "pipes".into()];
        },
        None,
    );
    let admin = login(&a.app, "admin", "pass", None).await;
    let r = send(&a.app, req("GET", "/pipes/keys", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = send(&a.app, req("GET", "/jobs", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

// ------------------------------------------------------------------------------------------------
// C1: the row-count estimate and `params` in the query string
// ------------------------------------------------------------------------------------------------

#[tokio::test]
async fn rowcounts_take_estimate_and_query_string_params() {
    let a = build(|_| {}, None);
    let admin = login(&a.app, "admin", "pass", None).await;
    let r = send(
        &a.app,
        req(
            "POST",
            "/pipes/sql:x/m/None/register",
            Some(&admin),
            body(json!({"columns": {"id": "id"}})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = send(
        &a.app,
        req(
            "POST",
            "/pipes/sql:x/m/None/data",
            Some(&admin),
            body(json!([{"id": 1, "color": "red"}, {"id": 2, "color": "blue"}])),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let count = |query: &'static str| {
        let app = a.app.clone();
        let admin = admin.clone();
        async move {
            let r = send(
                &app,
                req(
                    "GET",
                    &format!("/pipes/sql:x/m/None/rowcount{query}"),
                    Some(&admin),
                    None,
                ),
            )
            .await;
            assert_eq!(r.status, StatusCode::OK, "{query}: {}", r.text());
            r.json()
        }
    };
    assert_eq!(count("").await, json!(2));
    // The test instance keeps no estimate: the exact count.
    assert_eq!(count("?estimate=true").await, json!(2));
    assert_eq!(
        count("?params=%7B%22color%22%3A%20%22red%22%7D").await,
        json!(1)
    );
    let r = send(
        &a.app,
        req(
            "GET",
            "/pipes/sql:x/m/None/rowcount?params=nope",
            Some(&admin),
            None,
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNPROCESSABLE_ENTITY);
}
