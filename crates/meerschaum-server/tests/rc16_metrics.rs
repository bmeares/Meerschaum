//! Fresh scopes, object/team visibility and canonical transport for persisted numeric metrics.

mod common;

use axum::Router;
use axum::http::StatusCode;
use common::{SECRET, login, req, send};
use meerschaum_connector::InstanceConnector;
use meerschaum_connector::sync_metrics::{MetricsQuery, SyncMetric};
use meerschaum_connector::sync_status::{SyncOutcome, SyncSource};
use meerschaum_connector_api::APIConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_core::{Map, Pipe, Value, users::User};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::json;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

fn map(value: serde_json::Value) -> Map {
    serde_json::from_value::<Value>(value).unwrap().to_map()
}

struct Fixture {
    app: Router,
    state: AppState,
    conn: Arc<SqlConnector>,
    public: Pipe,
    private: Pipe,
    _dir: tempfile::TempDir,
}

async fn fixture(no_auth: bool) -> Fixture {
    fixture_with(no_auth, |_| {}).await
}

async fn fixture_with(no_auth: bool, tweak: impl FnOnce(&mut ServerConfig)) -> Fixture {
    let dir = tempfile::tempdir().unwrap();
    let conn = Arc::new(
        SqlConnector::from_uri(
            "metrics",
            &format!("sqlite:///{}", dir.path().join("data.db").display()),
        )
        .await
        .unwrap(),
    );
    for name in ["admin", "alice", "bob"] {
        let mut user = User::new(name, Some("fixture-password"));
        if name == "admin" {
            user.user_type = Some("admin".into());
        }
        assert!(conn.register_user(&mut user).await.success);
    }
    let public = Pipe::builder("plugin:source", "public")
        .instance("sql:metrics")
        .parameters(map(json!({"access":{"owner":"alice", "edit":"owner"}})))
        .build()
        .unwrap();
    let private = Pipe::builder("plugin:source", "private")
        .instance("sql:metrics")
        .parameters(map(
            json!({"access":{"owner":"bob", "visibility":"private"}}),
        ))
        .build()
        .unwrap();
    assert!(conn.register_pipe(&public).await.success);
    assert!(conn.register_pipe(&private).await.success);
    for (pipe, rows) in [(&public, 3), (&private, 999)] {
        conn.get_pipe_id(pipe).await.unwrap().unwrap();
        let metric = SyncMetric::from_outcome(
            pipe,
            &SyncOutcome::new(
                true,
                "SECRET SQL ERROR",
                SyncSource::Python,
                Some("PRIVATE JOB".into()),
            )
            .with_counts(rows, 0, 0)
            .with_duration(Some(Duration::from_secs(2))),
        );
        conn.record_sync_metrics(&[metric]).await.unwrap();
    }
    let mut config = ServerConfig::for_root("sql:metrics", dir.path());
    config.no_auth = no_auth;
    config.no_dash = true;
    config.no_webterm = true;
    tweak(&mut config);
    let state = AppState::builder(config, conn.clone())
        .secret(SECRET.to_vec())
        .engine(Arc::new(
            meerschaum_server::engine_store::InstanceEngine::new(conn.clone(), "sql:metrics"),
        ))
        .build()
        .unwrap();
    Fixture {
        app: meerschaum_server::app(state.clone()),
        state,
        conn,
        public,
        private,
        _dir: dir,
    }
}

async fn saved_pipeline(fixture: &Fixture, identity: &str, private: bool) -> Pipe {
    let pipe = Pipe::builder("mrsm", "pipeline")
        .location(Some("restricted-flow"))
        .instance("sql:metrics")
        .parameters(map(json!({
            "pipeline":{"name":"restricted-flow","id":identity,"registry":"sql:metrics"},
            "access":{"owner":"bob","visibility":if private { "private" } else { "public" }}
        })))
        .build()
        .unwrap();
    assert!(fixture.conn.register_pipe(&pipe).await.success);
    pipe
}

async fn seed_pipeline_history(fixture: &Fixture, run_id: &str, snapshot: serde_json::Value) {
    let finished = chrono::Utc::now() - chrono::TimeDelta::seconds(1);
    let started = finished - chrono::TimeDelta::seconds(2);
    let identity =
        meerschaum_connector::sync_metrics::observed_registration(&fixture.public).unwrap();
    let records = serde_json::from_value(json!([
        {"run_id":run_id,"started":started,"finished":finished,"status":"success",
         "targets":[fixture.public.pipe_ref()],"num_nodes":1,
         "selection":{"pipeline_definitions":[snapshot.clone()]}},
        {"run_id":run_id,"pipe_ref":fixture.public.pipe_ref(),"started":started,
         "finished":finished,"status":"succeeded","attempts":1,
         "registration_id":identity,"registration_ref":fixture.public.pipe_ref(),"inserted":5}
    ]))
    .unwrap();
    assert!(fixture.state.engine.record_runs(records).await.success);
    let events = serde_json::from_value(json!([
        {"run_id":run_id,"created":started,"event":"run_started",
         "payload":{"selection":{"pipeline_definitions":[snapshot]}}},
        {"run_id":run_id,"created":finished,"event":"run_finished","payload":{"success":true}}
    ]))
    .unwrap();
    assert!(fixture.state.engine.record_events(events).await.success);
    let mut outcome = SyncOutcome::new(true, "", SyncSource::Engine, None).with_counts(5, 0, 0);
    outcome.run_id = Some(run_id.into());
    fixture
        .conn
        .record_sync_metrics(&[SyncMetric::from_outcome(&fixture.public, &outcome)])
        .await
        .unwrap();
}

async fn history_tool(
    fixture: &Fixture,
    name: &str,
    arguments: serde_json::Value,
) -> serde_json::Value {
    use meerschaum_server::auth::{Principal, Scopes};
    use meerschaum_server::mcp::{Caller, McpServer};
    let caller = Caller::api(
        Scopes::List(vec!["pipes:read".into()]),
        Some(Principal::User(User::new("alice", None))),
    );
    let reply = McpServer::for_state(&fixture.state)
        .handle_payload(
            &fixture.state,
            serde_json::from_value(json!({"jsonrpc":"2.0","id":1,"method":"tools/call",
            "params":{"name":name,"arguments":arguments}}))
            .unwrap(),
            &caller,
        )
        .await
        .unwrap();
    serde_json::to_value(reply).unwrap()
}

async fn generation_rpc(fixture: &Fixture, token: &str, run_id: &str) -> bool {
    let response = send(
        &fixture.app,
        req(
            "POST",
            "/engine/run-generations",
            Some(token),
            Some((
                "application/json",
                json!({"run_ids":[run_id]}).to_string().into_bytes(),
            )),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    response.json()["generations"][run_id].as_bool().unwrap()
}

#[tokio::test]
async fn private_saved_pipeline_metadata_is_withheld_across_history_surfaces_and_live_acl_changes()
{
    let fixture = fixture(false).await;
    let mut metadata = saved_pipeline(&fixture, "original-pipeline", true).await;
    let run_id = "restricted-flow-2026-10-28T12-00-00.000000Z-0123abcd";
    seed_pipeline_history(
        &fixture,
        run_id,
        json!({"name":"restricted-flow","id":"original-pipeline",
        "registry":"api:original","registration_registry":"sql:metrics"}),
    )
    .await;
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let admin = login(&fixture.app, "admin", "fixture-password", None).await;
    assert!(!generation_rpc(&fixture, &alice, run_id).await);
    assert!(
        generation_rpc(&fixture, &admin, run_id).await,
        "valid canonical stored proof is checked before stripping"
    );
    let listed = send(&fixture.app, req("GET", "/engine/runs", Some(&alice), None)).await;
    assert_eq!(listed.status, StatusCode::OK, "{}", listed.text());
    assert!(
        listed.json()["runs"].as_array().unwrap().is_empty(),
        "{}",
        listed.text()
    );
    for uri in [
        format!("/engine/runs/{run_id}"),
        format!("/engine/runs/{run_id}/events/stream"),
    ] {
        assert_eq!(
            send(&fixture.app, req("GET", &uri, Some(&alice), None))
                .await
                .status,
            StatusCode::NOT_FOUND
        );
    }
    for metric in ["engine_runs", "engine_nodes", "engine_events"] {
        let response = send(
            &fixture.app,
            req(
                "POST",
                &format!("/engine/pipes/{metric}/read"),
                Some(&alice),
                Some((
                    "application/json",
                    json!({"select_columns":["run_id","selection","payload"]})
                        .to_string()
                        .into_bytes(),
                )),
            ),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert!(
            response.json()["rows"].as_array().unwrap().is_empty(),
            "{metric}: {}",
            response.text()
        );
    }
    let mcp = history_tool(&fixture, "list_runs", json!({})).await;
    assert!(!mcp.to_string().contains("restricted-flow"), "{mcp}");
    let mcp = history_tool(&fixture, "get_run", json!({"run_id":run_id})).await;
    assert_eq!(mcp["result"]["isError"], true, "{mcp}");
    let metrics = send(
        &fixture.app,
        req("GET", "/engine/metrics", Some(&alice), None),
    )
    .await
    .json()["metrics"]
        .clone();
    assert_eq!(metrics["summary"]["rows"], 8, "{metrics}");
    assert!(
        metrics["history"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["run_id"].is_null())
    );
    assert!(!metrics.to_string().contains("restricted-flow"));
    for (route, body) in [
        (
            "/engine/runs",
            json!([{"run_id":run_id,"started":chrono::Utc::now(),"status":"success","targets":[fixture.public.pipe_ref()]}]),
        ),
        (
            "/engine/events",
            json!([{"run_id":run_id,"created":chrono::Utc::now(),"event":"run_finished","payload":{}}]),
        ),
        (
            "/engine/pipes/engine_runs/clear",
            json!({"params":{"run_id":run_id}}),
        ),
    ] {
        let response = send(
            &fixture.app,
            req(
                "POST",
                route,
                Some(&alice),
                Some(("application/json", body.to_string().into_bytes())),
            ),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::NOT_FOUND,
            "{route}: {}",
            response.text()
        );
    }

    // Grant/revoke is read from fresh registration metadata, not a worker-local filtered cache.
    metadata.update_parameters(map(json!({"access":{"owner":"bob","visibility":"public"}})));
    assert!(fixture.conn.edit_pipe(&metadata, false).await.success);
    let visible = send(
        &fixture.app,
        req("GET", &format!("/engine/runs/{run_id}"), Some(&alice), None),
    )
    .await;
    assert_eq!(visible.status, StatusCode::OK, "{}", visible.text());
    assert!(generation_rpc(&fixture, &alice, run_id).await);
    assert!(visible.text().contains("api:original"));
    assert!(
        !visible.text().contains("registration_registry")
            && !visible.text().contains("registration_ref")
    );
    let admin_view = send(
        &fixture.app,
        req("GET", &format!("/engine/runs/{run_id}"), Some(&admin), None),
    )
    .await;
    assert_eq!(admin_view.status, StatusCode::OK);
    assert!(!admin_view.text().contains("registration_registry"));
    let stream = send(
        &fixture.app,
        req(
            "GET",
            &format!("/engine/runs/{run_id}/events/stream"),
            Some(&alice),
            None,
        ),
    )
    .await;
    assert_eq!(stream.status, StatusCode::OK);
    assert!(
        !stream.text().contains("registration_registry"),
        "{}",
        stream.text()
    );
    metadata.update_parameters(map(
        json!({"access":{"owner":"bob","visibility":"private"}}),
    ));
    assert!(fixture.conn.edit_pipe(&metadata, false).await.success);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", &format!("/engine/runs/{run_id}"), Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert!(fixture.conn.delete_pipe(&metadata).await.success);
    saved_pipeline(&fixture, "replacement-pipeline", false).await;
    assert!(!generation_rpc(&fixture, &alice, run_id).await);
    assert!(
        !generation_rpc(&fixture, &admin, run_id).await,
        "admin transport must not authorize a recreated saved UUID"
    );
    assert_eq!(
        send(
            &fixture.app,
            req("GET", &format!("/engine/runs/{run_id}"), Some(&admin), None)
        )
        .await
        .status,
        StatusCode::OK,
        "ordinary administrative diagnostics remain available"
    );
    assert_eq!(
        send(
            &fixture.app,
            req("GET", &format!("/engine/runs/{run_id}"), Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn submitted_pipeline_and_node_proofs_are_stripped_or_validated_on_both_write_protocols() {
    let fixture = fixture(false).await;
    saved_pipeline(&fixture, "current-pipeline", false).await;
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let now = chrono::Utc::now().to_rfc3339();
    for route in ["/engine/runs", "/engine/pipes/engine_runs/write"] {
        let id = if route == "/engine/runs" {
            "forged-high-level"
        } else {
            "forged-raw"
        };
        let record = json!({"run_id":id,"started":now,"status":"success","success":true,
            "targets":[fixture.public.pipe_ref()],
            "selection":{"pipeline_definitions":[{"name":"restricted-flow","id":"current-pipeline",
                "registry":"api:foreign","registration_registry":"sql:metrics"}]}});
        let response = send(
            &fixture.app,
            req(
                "POST",
                route,
                Some(&alice),
                Some(("application/json", json!([record]).to_string().into_bytes())),
            ),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        let stored = fixture
            .state
            .engine
            .get_runs(&meerschaum_server::backends::RunsFilter {
                run_id: Some(id.into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(
            !serde_json::to_string(&stored)
                .unwrap()
                .contains("registration_registry")
        );
        assert_eq!(
            send(
                &fixture.app,
                req("GET", &format!("/engine/runs/{id}"), Some(&alice), None)
            )
            .await
            .status,
            StatusCode::NOT_FOUND
        );
    }
    for identity in ["current-pipeline", "old-pipeline"] {
        let record = json!({"run_id":format!("validated-{identity}"),"started":now,"status":"success",
            "targets":[fixture.public.pipe_ref()],
            "selection":{"pipeline_definitions":[{"name":"restricted-flow","id":identity,"registry":"api:client"}]}});
        let mut request = req(
            "POST",
            "/engine/runs",
            Some(&alice),
            Some(("application/json", json!([record]).to_string().into_bytes())),
        );
        request.headers_mut().insert(
            meerschaum_connector_api::engine_pipes::CLIENT_INSTANCE_HEADER,
            axum::http::HeaderValue::from_static("api:client"),
        );
        let response = send(&fixture.app, request).await;
        assert_eq!(
            response.status,
            if identity == "current-pipeline" {
                StatusCode::OK
            } else {
                StatusCode::CONFLICT
            },
            "{}",
            response.text()
        );
    }
    let node = json!({"run_id":"forged-node-high-level","pipe_ref":fixture.public.pipe_ref(),"started":now,"status":"succeeded",
        "registration_id":meerschaum_connector::sync_metrics::observed_registration(&fixture.public),
        "registration_ref":"sql:metrics|plugin:source|private|None"});
    let response = send(
        &fixture.app,
        req(
            "POST",
            "/engine/runs",
            Some(&alice),
            Some(("application/json", json!([node]).to_string().into_bytes())),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert!(
        fixture
            .conn
            .value(
                "SELECT registration_ref FROM mrsm_engine_nodes WHERE run_id = 'forged-node-high-level'"
            )
            .await
            .is_none()
    );
}

#[tokio::test]
async fn native_history_page_bounds_reach_sql_and_apply_after_visibility_for_restricted_readers() {
    use meerschaum_connector::DataQuery;
    use meerschaum_connector_api::engine_pipes::EnginePipeQuery;
    let fixture = fixture(false).await;
    let now = chrono::Utc::now();
    let records = (0..10)
        .flat_map(|index| {
            let pipe = if index % 2 == 0 {
                &fixture.private
            } else {
                &fixture.public
            };
            let registration =
                meerschaum_connector::sync_metrics::observed_registration(pipe).unwrap();
            [
                json!({"run_id":format!("page-{index}"),
        "started":now-chrono::TimeDelta::seconds(10-index),"status":"running",
        "targets":[pipe.pipe_ref()],"num_nodes":1}),
                json!({"run_id":format!("page-{index}"),
        "pipe_ref":pipe.pipe_ref(),"registration_id":registration,
        "started":now-chrono::TimeDelta::seconds(10-index),"status":"running"}),
            ]
        })
        .collect::<Vec<_>>();
    assert!(
        fixture
            .state
            .engine
            .record_runs(serde_json::from_value(json!(records)).unwrap())
            .await
            .success
    );
    let admin = login(&fixture.app, "admin", "fixture-password", None).await;
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let query = EnginePipeQuery::from_data_query(&DataQuery {
        limit: Some(2),
        order: Some("desc".into()),
        ..Default::default()
    });
    fixture.conn.enable_query_log();
    let response = send(
        &fixture.app,
        req(
            "POST",
            "/engine/pipes/engine_runs/read",
            Some(&admin),
            Some(("application/json", serde_json::to_vec(&query).unwrap())),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let rows = response.json()["rows"].as_array().unwrap().clone();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["run_id"], "page-9");
    assert_eq!(rows[1]["run_id"], "page-8");
    let sql = fixture.conn.take_query_log();
    assert!(
        sql.iter().any(|query| query.contains("mrsm_engine_runs")
            && query.contains("DESC")
            && query.contains("LIMIT 2")),
        "{sql:?}"
    );
    let response = send(
        &fixture.app,
        req(
            "POST",
            "/engine/pipes/engine_runs/read",
            Some(&alice),
            Some(("application/json", serde_json::to_vec(&query).unwrap())),
        ),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let rows = response.json()["rows"].as_array().unwrap().clone();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["run_id"], "page-9");
    assert_eq!(
        rows[1]["run_id"], "page-7",
        "a short filtered raw prefix must not signal exhausted history"
    );
    for invalid in [
        json!({"limit":0}),
        json!({"limit":-1}),
        json!({"order":"desc; SELECT secret"}),
    ] {
        let response = send(
            &fixture.app,
            req(
                "POST",
                "/engine/pipes/engine_runs/read",
                Some(&admin),
                Some(("application/json", invalid.to_string().into_bytes())),
            ),
        )
        .await;
        assert_eq!(response.status, StatusCode::BAD_REQUEST);
    }
    let response = send(
        &fixture.app,
        req(
            "POST",
            "/engine/pipes/engine_runs/clear",
            Some(&admin),
            Some((
                "application/json",
                json!({"params":{"run_id":"page-9"},"limit":2})
                    .to_string()
                    .into_bytes(),
            )),
        ),
    )
    .await;
    assert_eq!(
        response.status,
        StatusCode::BAD_REQUEST,
        "read limits must not imply partial deletion"
    );
}

#[tokio::test]
async fn api_pipeline_identities_cannot_be_reintroduced_and_live_rename_preserves_history_and_access()
 {
    let fixture = fixture(false).await;
    let mut editor = User::new("alice", None);
    editor.attributes = Some(map(
        json!({"role":"editor","scopes":["pipes:read","pipes:write","pipes:register","pipes:delete"]}),
    ));
    assert!(fixture.conn.edit_user(&editor).await.success);
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let admin = login(&fixture.app, "admin", "fixture-password", None).await;
    let body = |identity: &str, private: bool| {
        json!({
            "pipeline":{"name":"hardened","id":identity,"registry":"forged-registry","revision":0,"targets":[fixture.public.pipe_ref()]},
            "access":{"visibility":if private {"private"}else{"public"},"edit":"owner"}
        })
    };
    let post = |route: &str, token: &str, value: serde_json::Value| {
        req(
            "POST",
            route,
            Some(token),
            Some(("application/json", value.to_string().into_bytes())),
        )
    };
    let created = send(
        &fixture.app,
        post(
            "/pipelines/hardened/register",
            &admin,
            body("caller-chosen", true),
        ),
    )
    .await;
    assert_eq!(created.status, StatusCode::OK, "{}", created.text());
    let registered = Pipe::builder("mrsm", "pipeline")
        .location(Some("hardened"))
        .instance("sql:metrics")
        .build()
        .unwrap();
    let old = fixture.conn.get_pipe_attributes(&registered).await.unwrap();
    let old_json = serde_json::to_value(&old).unwrap();
    let old_id = old_json["parameters"]["pipeline"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(old_id, "caller-chosen");
    assert_eq!(uuid::Uuid::parse_str(&old_id).unwrap().get_version_num(), 4);
    seed_pipeline_history(
        &fixture,
        "old-hardened-run",
        json!({"name":"hardened","id":old_id,"registry":"sql:metrics"}),
    )
    .await;
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/old-hardened-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            req("DELETE", "/pipelines/hardened/delete", Some(&admin), None)
        )
        .await
        .status,
        StatusCode::OK
    );
    assert_eq!(
        send(
            &fixture.app,
            post("/pipelines/hardened/register", &alice, body(&old_id, false))
        )
        .await
        .status,
        StatusCode::OK
    );
    let current = fixture.conn.get_pipe_attributes(&registered).await.unwrap();
    let current_json = serde_json::to_value(&current).unwrap();
    let identity = current_json["parameters"]["pipeline"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(
        identity, old_id,
        "a known deleted UUID must not be reusable"
    );
    let mut forged = current_json["parameters"].clone();
    forged["pipeline"]["id"] = json!(old_id);
    let changed = send(
        &fixture.app,
        req(
            "PATCH",
            "/pipelines/hardened/edit",
            Some(&alice),
            Some(("application/json", forged.to_string().into_bytes())),
        ),
    )
    .await;
    assert_eq!(changed.status, StatusCode::CONFLICT, "{}", changed.text());
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/old-hardened-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert!(!generation_rpc(&fixture, &admin, "old-hardened-run").await);
    seed_pipeline_history(
        &fixture,
        "new-hardened-run",
        json!({"name":"hardened","id":identity,"registry":"sql:metrics"}),
    )
    .await;
    assert!(generation_rpc(&fixture, &alice, "new-hardened-run").await);
    let renamed = send(
        &fixture.app,
        post(
            "/pipelines/hardened/rename",
            &alice,
            json!({"new_name":"hardened-renamed","expected_id":identity,"expected_revision":0}),
        ),
    )
    .await;
    assert_eq!(renamed.status, StatusCode::OK, "{}", renamed.text());
    assert_eq!(renamed.json()["pipeline"]["id"], identity);
    let destination = Pipe::builder("mrsm", "pipeline")
        .location(Some("hardened-renamed"))
        .instance("sql:metrics")
        .build()
        .unwrap();
    let moved = fixture
        .conn
        .get_pipe_attributes(&destination)
        .await
        .unwrap();
    assert_eq!(moved.get("pipe_id"), current.get("pipe_id"));
    assert!(
        fixture
            .conn
            .get_pipe_attributes(&registered)
            .await
            .unwrap()
            .is_empty()
    );
    let moved_json = serde_json::to_value(&moved).unwrap();
    assert_eq!(
        moved_json["parameters"]["access"],
        current_json["parameters"]["access"]
    );
    assert_eq!(moved_json["parameters"]["pipeline"]["revision"], 1);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/new-hardened-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::OK,
        "authorized rename retains prior history by stable live UUID"
    );
    assert!(generation_rpc(&fixture, &alice, "new-hardened-run").await);
    let named = send(
        &fixture.app,
        req(
            "GET",
            "/engine/runs?pipeline=hardened-renamed",
            Some(&alice),
            None,
        ),
    )
    .await;
    assert_eq!(
        named.json()["runs"].as_array().unwrap().len(),
        1,
        "{}",
        named.text()
    );
    assert_eq!(named.json()["runs"][0]["run_id"], "new-hardened-run");
    assert_eq!(
        send(
            &fixture.app,
            post(
                "/pipelines/hardened/rename",
                &alice,
                json!({"new_name":"resurrected"})
            )
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            post(
                "/pipelines/hardened-renamed/rename",
                &alice,
                json!({"new_name":"stale","expected_id":old_id})
            )
        )
        .await
        .status,
        StatusCode::CONFLICT
    );
    let read_only = login(
        &fixture.app,
        "alice",
        "fixture-password",
        Some("pipes:read"),
    )
    .await;
    assert_eq!(
        send(
            &fixture.app,
            post(
                "/pipelines/hardened-renamed/rename",
                &read_only,
                json!({"new_name":"denied"})
            )
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
    let duplicate = Pipe::builder("mrsm","pipeline").location(Some("duplicate-hidden")).instance("sql:metrics")
        .parameters(map(json!({"pipeline":{"id":identity,"name":"duplicate-hidden"},"access":{"owner":"bob","visibility":"private"}}))).build().unwrap();
    assert!(fixture.conn.register_pipe(&duplicate).await.success);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/new-hardened-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND,
        "hidden duplicate IDs cannot create authority"
    );
    assert!(!generation_rpc(&fixture, &admin, "new-hardened-run").await);
    assert!(fixture.conn.delete_pipe(&duplicate).await.success);
    assert!(generation_rpc(&fixture, &alice, "new-hardened-run").await);
    // A no-op move still returns the caller's member projection, including legacy metadata
    // that contains a member the Pipeline owner cannot currently see.
    let mut hidden_member = destination.clone();
    let mut parameters = moved.get("parameters").unwrap().to_map();
    parameters
        .get_mut("pipeline")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert(
            "targets".into(),
            Value::List(vec![
                Value::Str(fixture.public.pipe_ref()),
                Value::Str(fixture.private.pipe_ref()),
            ]),
        );
    hidden_member.set_parameters(parameters);
    assert!(fixture.conn.edit_pipe(&hidden_member, false).await.success);
    let unchanged = send(
        &fixture.app,
        post(
            "/pipelines/hardened-renamed/rename",
            &alice,
            json!({"new_name":"hardened-renamed"}),
        ),
    )
    .await;
    assert_eq!(unchanged.status, StatusCode::OK, "{}", unchanged.text());
    assert_eq!(
        unchanged.json()["pipeline"]["targets"],
        json!([fixture.public.pipe_ref()])
    );
    assert_eq!(unchanged.json()["pipeline"]["hidden_members"], 1);
    assert!(!unchanged.text().contains(&fixture.private.pipe_ref()));
}

#[tokio::test]
async fn a_name_only_legacy_pipeline_cannot_prove_retained_history_identity() {
    let fixture = fixture(false).await;
    let legacy = Pipe::builder("mrsm", "pipeline")
        .location(Some("legacy-flow"))
        .instance("sql:metrics")
        .parameters(map(json!({"pipeline":{"name":"legacy-flow"}})))
        .build()
        .unwrap();
    assert!(fixture.conn.register_pipe(&legacy).await.success);
    seed_pipeline_history(
        &fixture,
        "legacy-flow-run",
        json!({
            "name":"legacy-flow","id":"legacy:sql:metrics:legacy-flow","registry":"sql:metrics"
        }),
    )
    .await;
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let admin = login(&fixture.app, "admin", "fixture-password", None).await;
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/legacy-flow-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/legacy-flow-run", Some(&admin), None)
        )
        .await
        .status,
        StatusCode::OK
    );
    assert!(!generation_rpc(&fixture, &alice, "legacy-flow-run").await);
    assert!(!generation_rpc(&fixture, &admin, "legacy-flow-run").await);
    assert!(fixture.conn.delete_pipe(&legacy).await.success);
    let mut forged = legacy.clone();
    forged.set_parameters(map(
        json!({"pipeline":{"name":"legacy-flow","id":"legacy:sql:metrics:legacy-flow"}}),
    ));
    assert!(fixture.conn.register_pipe(&forged).await.success);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/legacy-flow-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND,
        "persisting a predictable legacy string cannot manufacture a saved UUID"
    );
    assert!(!generation_rpc(&fixture, &alice, "legacy-flow-run").await);
    assert!(!generation_rpc(&fixture, &admin, "legacy-flow-run").await);
}

#[tokio::test]
async fn a_restricted_native_api_writer_persists_queued_proof_before_its_header_and_sse() {
    use meerschaum_connector_api::engine_pipes::{docs_from_records, engine_frame};
    use tower::ServiceExt;
    let fixture = fixture(false).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = fixture.app.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = APIConnector::new(
        Some("history_writer"),
        map(json!({
            "host":"127.0.0.1", "port":port, "username":"alice", "password":"fixture-password"
        })),
    )
    .unwrap();
    let pipe = Pipe::builder(fixture.public.connector_keys(), fixture.public.metric_key())
        .instance("api:history_writer")
        .build()
        .unwrap();
    let attributes = InstanceConnector::get_pipe_attributes(&client, &pipe)
        .await
        .unwrap();
    let registration =
        meerschaum_connector::sync_status::registration_from_attributes(&attributes).unwrap();
    let now = chrono::Utc::now();
    let nodes = engine_frame(
        "engine_nodes",
        docs_from_records(&[json!({
            "run_id":"api-queued", "pipe_ref":pipe.pipe_ref(), "registration_id":registration,
            "started":now,"status":"pending","attempts":0
        })]),
    )
    .unwrap();
    let written = client.write_engine_pipe("engine_nodes", &nodes).await;
    assert!(written.success, "{}", written.message);
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/api-queued", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    let runs = engine_frame(
        "engine_runs",
        docs_from_records(&[json!({
            "run_id":"api-queued","started":now,"success":null,
            "targets":[pipe.pipe_ref()],"num_nodes":1
        })]),
    )
    .unwrap();
    let written = client.write_engine_pipe("engine_runs", &runs).await;
    assert!(written.success, "{}", written.message);
    let detail = send(
        &fixture.app,
        req("GET", "/engine/runs/api-queued", Some(&alice), None),
    )
    .await;
    assert_eq!(detail.status, StatusCode::OK, "{}", detail.text());
    assert!(
        !detail.text().contains("registration_id") && !detail.text().contains("registration_ref")
    );
    let stream = fixture
        .app
        .clone()
        .oneshot(req(
            "GET",
            "/engine/runs/api-queued/events/stream?wait=0",
            Some(&alice),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    drop(stream);
    let proof = client
        .get_remote_run_generations(&["api-queued".into(), "missing".into()])
        .await
        .unwrap();
    assert_eq!(proof.get("api-queued"), Some(&true));
    assert_eq!(proof.get("missing"), Some(&false));
    let many: Vec<_> = (0..257).map(|index| format!("missing-{index}")).collect();
    let proof = client.get_remote_run_generations(&many).await.unwrap();
    assert_eq!(proof.len(), 257);
    assert!(proof.values().all(|valid| !valid));
    let mut metadata = Pipe::builder("mrsm", "pipeline")
        .location(Some("native-roundtrip"))
        .instance("api:history_writer")
        .parameters(map(json!({"pipeline":{
            "name":"native-roundtrip","id":"native-roundtrip-id","registry":"api:history_writer",
            "targets":[pipe.pipe_ref()]
        }})))
        .build()
        .unwrap();
    let registered = client.register_pipe(&metadata).await;
    assert!(registered.success, "{}", registered.message);
    let server_metadata = Pipe::builder("mrsm", "pipeline")
        .location(Some("native-roundtrip"))
        .instance("sql:metrics")
        .build()
        .unwrap();
    let stored = fixture
        .conn
        .get_pipe_attributes(&server_metadata)
        .await
        .unwrap();
    let stored = serde_json::to_value(stored).unwrap();
    assert_eq!(
        stored["parameters"]["pipeline"]["registry"].as_str(),
        Some("sql:metrics")
    );
    assert_eq!(
        stored["parameters"]["pipeline"]["targets"],
        json!([fixture.public.pipe_ref()])
    );
    let loaded = client.get_pipe_attributes(&metadata).await.unwrap();
    let loaded_json = serde_json::to_value(&loaded).unwrap();
    assert_eq!(
        loaded_json["parameters"]["pipeline"]["registry"].as_str(),
        Some("api:history_writer")
    );
    assert_eq!(
        loaded_json["parameters"]["pipeline"]["targets"],
        json!([pipe.pipe_ref()])
    );
    let mut edited = loaded["parameters"].as_map().unwrap().clone();
    edited
        .get_mut("pipeline")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert("description".into(), Value::Str("roundtrip".into()));
    metadata.set_parameters(edited);
    let changed = client.edit_pipe(&metadata, false).await;
    assert!(changed.success, "{}", changed.message);
    for (name, registry, target) in [
        ("hidden-member", "sql:metrics", fixture.private.pipe_ref()),
        (
            "legacy-foreign",
            "api:foreign",
            format!(
                "api:foreign|{}|{}|None",
                pipe.connector_keys(),
                pipe.metric_key()
            ),
        ),
    ] {
        let stored = Pipe::builder("mrsm", "pipeline")
            .location(Some(name))
            .instance("sql:metrics")
            .parameters(map(
                json!({"pipeline":{"name":name,"id":name,"registry":registry,"targets":[target]}}),
            ))
            .build()
            .unwrap();
        assert!(fixture.conn.register_pipe(&stored).await.success);
    }
    let keys = client
        .fetch_pipes_keys(&meerschaum_connector::PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let meerschaum_connector::PipesKeys::Dict(keys) = keys else {
        panic!("bulk metadata must retain parameters");
    };
    let entry = keys
        .values()
        .find(|entry| entry.location_key.as_deref() == Some("native-roundtrip"))
        .unwrap();
    assert_eq!(
        serde_json::to_value(entry.extra.as_ref().unwrap()).unwrap()["pipeline"]["targets"],
        json!([pipe.pipe_ref()])
    );
    for name in ["hidden-member", "legacy-foreign"] {
        let entry = keys
            .values()
            .find(|entry| entry.location_key.as_deref() == Some(name))
            .unwrap();
        let parameters = serde_json::to_value(entry.extra.as_ref().unwrap()).unwrap();
        assert_eq!(parameters["pipeline"]["targets"], json!([]));
        assert_eq!(parameters["pipeline"]["hidden_members"], 1);
    }
    let legacy_list = send(
        &fixture.app,
        req("GET", "/pipelines/keys", Some(&alice), None),
    )
    .await;
    assert!(
        legacy_list.json()["keys"].is_array(),
        "the default key-list wire shape stays compatible"
    );
    assert!(!legacy_list.text().contains(&fixture.private.pipe_ref()));
    for reference in [
        format!(
            "api:foreign|{}|{}|None",
            pipe.connector_keys(),
            pipe.metric_key()
        ),
        format!(
            "api:history_writer|{}|{}|None",
            fixture.private.connector_keys(),
            fixture.private.metric_key()
        ),
    ] {
        let mut parameters = metadata.raw_parameters().clone();
        parameters
            .get_mut("pipeline")
            .unwrap()
            .as_map_mut()
            .unwrap()
            .insert("targets".into(), Value::List(vec![Value::Str(reference)]));
        let mut bad = metadata.clone();
        bad.set_parameters(parameters);
        assert!(
            !client.edit_pipe(&bad, false).await.success,
            "foreign or private keys cannot be treated as visible local members"
        );
    }
    let canonical = send(
        &fixture.app,
        req("GET", "/pipelines/native-roundtrip", Some(&alice), None),
    )
    .await;
    assert_eq!(canonical.status, StatusCode::OK, "{}", canonical.text());
    assert_eq!(
        canonical.json()["pipeline"]["targets"][0],
        fixture.public.pipe_ref()
    );
    assert_eq!(canonical.json()["members"].as_array().unwrap().len(), 1);
    server.abort();
    // The optional authorization RPC does not give old servers unproven authority.
    let old_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old_port = old_listener.local_addr().unwrap().port();
    let old_server = tokio::spawn(async move {
        axum::serve(old_listener, Router::new()).await.unwrap();
    });
    let old = APIConnector::new(
        Some("old_history"),
        map(json!({"host":"127.0.0.1","port":old_port})),
    )
    .unwrap();
    assert_eq!(
        old.get_remote_run_generations(&["legacy".into()])
            .await
            .unwrap()
            .get("legacy"),
        Some(&false)
    );
    old_server.abort();
}

#[tokio::test]
async fn retained_history_cannot_inherit_recreated_public_registration_or_forged_aliases() {
    let fixture = fixture(false).await;
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let admin = login(&fixture.app, "admin", "fixture-password", None).await;
    let old_id = fixture
        .conn
        .get_pipe_id(&fixture.private)
        .await
        .unwrap()
        .unwrap();
    let old_stamp =
        meerschaum_connector::sync_metrics::observed_registration(&fixture.private).unwrap();
    let seed = |run_id: &str, reference: String, stamp: Option<String>| {
        let at = chrono::Utc::now();
        serde_json::from_value(json!([
            {"run_id":run_id,"started":at,"finished":at,"status":"failed",
             "targets":[reference.clone()],"num_nodes":1,"message":"PRIVATE RUN PASSWORD"},
            {"run_id":run_id,"pipe_ref":reference,"started":at,"finished":at,
             "status":"failed","registration_id":stamp,"message":"PRIVATE NODE PASSWORD"}
        ]))
        .unwrap()
    };
    assert!(
        fixture
            .state
            .engine
            .record_runs(seed(
                "old-private",
                fixture.private.pipe_ref(),
                Some(old_stamp)
            ))
            .await
            .success
    );
    assert!(fixture.state.engine.record_events(serde_json::from_value(json!([
        {"run_id":"old-private","created":chrono::Utc::now(),"event":"run_finished","payload":{"message":"PRIVATE EVENT PASSWORD"}}
    ])).unwrap()).await.success);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/old-private", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert!(
        InstanceConnector::delete_pipe(fixture.conn.as_ref(), &fixture.private)
            .await
            .success
    );
    let public = Pipe::builder(
        fixture.private.connector_keys(),
        fixture.private.metric_key(),
    )
    .instance("sql:metrics")
    .parameters(map(json!({"access":{"owner":"alice"}})))
    .build()
    .unwrap();
    assert!(fixture.conn.register_pipe(&public).await.success);
    assert_eq!(
        fixture.conn.get_pipe_id(&public).await.unwrap().unwrap(),
        old_id
    );
    let fresh_stamp = meerschaum_connector::sync_metrics::observed_registration(&public).unwrap();
    // Refresh the ACL index too: old cached privacy must not be what hides retained history.
    fixture.state.access_cache().forget_pipes();
    for uri in [
        "/engine/runs",
        "/engine/runs/old-private",
        "/engine/runs/old-private/events/stream?wait=0",
    ] {
        let response = send(&fixture.app, req("GET", uri, Some(&alice), None)).await;
        assert!(!response.text().contains("PRIVATE"), "{}", response.text());
        if uri == "/engine/runs" {
            assert!(response.json()["runs"].as_array().unwrap().is_empty());
        } else {
            assert_eq!(response.status, StatusCode::NOT_FOUND);
        }
    }
    for metric in ["engine_runs", "engine_nodes", "engine_events"] {
        let response = send(
            &fixture.app,
            req(
                "POST",
                &format!("/engine/pipes/{metric}/read"),
                Some(&alice),
                Some(("application/json", b"{}".to_vec())),
            ),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert!(
            response.json()["rows"].as_array().unwrap().is_empty(),
            "{}",
            response.text()
        );
    }
    for (tool, args) in [
        ("list_runs", json!({})),
        ("get_run", json!({"run_id":"old-private"})),
    ] {
        let response = history_tool(&fixture, tool, args).await;
        assert!(!response.to_string().contains("PRIVATE"), "{response}");
        if tool == "get_run" {
            assert_eq!(response["result"]["isError"], true);
        }
    }
    let mutation = send(
        &fixture.app,
        req(
            "POST",
            "/engine/events",
            Some(&alice),
            Some((
                "application/json",
                json!([
                    {"run_id":"old-private","created":chrono::Utc::now(),"event":"run_finished"}
                ])
                .to_string()
                .into_bytes(),
            )),
        ),
    )
    .await;
    assert_eq!(
        mutation.status,
        StatusCode::NOT_FOUND,
        "{}",
        mutation.text()
    );
    let trusted = send(
        &fixture.app,
        req("GET", "/engine/runs/old-private", Some(&admin), None),
    )
    .await;
    assert_eq!(trusted.status, StatusCode::OK);
    assert!(trusted.text().contains("PRIVATE"));
    assert!(
        !trusted.text().contains("registration_id") && !trusted.text().contains("registration_ref")
    );
    assert!(
        fixture
            .state
            .engine
            .record_runs(seed("legacy", public.pipe_ref(), None))
            .await
            .success
    );
    assert!(
        fixture
            .state
            .engine
            .record_runs(seed(
                "foreign",
                format!(
                    "api:foreign|{}|{}|None",
                    public.connector_keys(),
                    public.metric_key()
                ),
                Some(fresh_stamp.clone())
            ))
            .await
            .success
    );
    for run_id in ["legacy", "foreign"] {
        assert_eq!(
            send(
                &fixture.app,
                req("GET", &format!("/engine/runs/{run_id}"), Some(&alice), None)
            )
            .await
            .status,
            StatusCode::NOT_FOUND
        );
    }
    for (run_id, count) in [("missing-count", None), ("incomplete", Some(2))] {
        let mut records: Vec<meerschaum_connector_api::models::RunRecord> =
            seed(run_id, public.pipe_ref(), Some(fresh_stamp.clone()));
        let header = records
            .iter_mut()
            .find(|row| row.pipe_ref.is_none())
            .unwrap();
        header.extra.shift_remove("num_nodes");
        if let Some(count) = count {
            header.extra.insert("num_nodes".into(), Value::Int(count));
        }
        assert!(fixture.state.engine.record_runs(records).await.success);
        let response = send(
            &fixture.app,
            req("GET", &format!("/engine/runs/{run_id}"), Some(&alice), None),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::NOT_FOUND,
            "{}",
            response.text()
        );
    }
    // A real current segment remains readable, and its public result never contains proof.
    assert!(
        fixture
            .state
            .engine
            .record_runs(seed("fresh", public.pipe_ref(), Some(fresh_stamp)))
            .await
            .success
    );
    let fresh = send(
        &fixture.app,
        req("GET", "/engine/runs/fresh", Some(&alice), None),
    )
    .await;
    assert_eq!(fresh.status, StatusCode::OK, "{}", fresh.text());
    assert!(
        !fresh.text().contains("registration_id") && !fresh.text().contains("registration_ref")
    );
    let ids = [
        "fresh",
        "old-private",
        "legacy",
        "foreign",
        "incomplete",
        "missing-count",
        "missing",
    ];
    for token in [&alice, &admin] {
        let proof = send(
            &fixture.app,
            req(
                "POST",
                "/engine/run-generations",
                Some(token),
                Some((
                    "application/json",
                    json!({"run_ids":ids}).to_string().into_bytes(),
                )),
            ),
        )
        .await;
        assert_eq!(proof.status, StatusCode::OK, "{}", proof.text());
        for id in ids {
            assert_eq!(
                proof.json()["generations"][id],
                id == "fresh",
                "{}",
                proof.text()
            );
        }
        assert!(!proof.text().contains("registration_") && !proof.text().contains("PRIVATE"));
    }
    for ids in [json!(vec!["fresh"; 257]), json!([""]), json!(["bad\nrun"])] {
        assert_eq!(
            send(
                &fixture.app,
                req(
                    "POST",
                    "/engine/run-generations",
                    Some(&alice),
                    Some((
                        "application/json",
                        json!({"run_ids":ids}).to_string().into_bytes()
                    ))
                )
            )
            .await
            .status,
            StatusCode::BAD_REQUEST
        );
    }
}

#[tokio::test]
async fn status_generation_guards_single_bulk_legacy_and_unregistered_api_views() {
    use meerschaum_connector::sync_status::{self, SyncStatus};
    let fixture = fixture(false).await;
    let old = Pipe::builder("plugin:source", "status-recreated")
        .instance("sql:metrics")
        .parameters(map(
            json!({"access":{"owner":"alice","visibility":"private","edit":"owner"}}),
        ))
        .build()
        .unwrap();
    assert!(fixture.conn.register_pipe(&old).await.success);
    let old_id = fixture.conn.get_pipe_id(&old).await.unwrap().unwrap();
    let old_stamp = meerschaum_connector::sync_metrics::observed_registration(&old).unwrap();
    assert!(
        InstanceConnector::delete_pipe(fixture.conn.as_ref(), &old)
            .await
            .success
    );
    let public = Pipe::builder("plugin:source", "status-recreated")
        .instance("sql:metrics")
        .parameters(map(json!({"access":{"owner":"alice","edit":"owner"}})))
        .build()
        .unwrap();
    assert!(fixture.conn.register_pipe(&public).await.success);
    assert_eq!(
        fixture.conn.get_pipe_id(&public).await.unwrap().unwrap(),
        old_id
    );
    let current = meerschaum_connector::sync_metrics::observed_registration(&public).unwrap();
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let outcome = |message: &str, stamp: Option<&str>| {
        let mut value = json!({"success":false,"finished":chrono::Utc::now(),"source":"python","message":message});
        if let Some(stamp) = stamp {
            value
                .as_object_mut()
                .unwrap()
                .insert("registration_id".into(), json!(stamp));
        }
        value
    };
    let post = |metric: &str, value: serde_json::Value| {
        req(
            "POST",
            "/engine/sync-status",
            Some(&alice),
            Some((
                "application/json",
                json!({"connector_keys":"plugin:source","metric_key":metric,"outcomes":[value]})
                    .to_string()
                    .into_bytes(),
            )),
        )
    };
    let stale = send(
        &fixture.app,
        post(
            "status-recreated",
            outcome("PRIVATE SQL PASSWORD", Some(&old_stamp)),
        ),
    )
    .await;
    assert_eq!(stale.status, StatusCode::CONFLICT);
    let legacy = send(
        &fixture.app,
        post("status-recreated", outcome("LEGACY PRIVATE PASSWORD", None)),
    )
    .await;
    assert_eq!(legacy.status, StatusCode::OK, "{}", legacy.text());
    assert!(
        legacy.json()["status"].is_null(),
        "legacy outcomes must not receive current proof"
    );
    // Existing rows from an older release have no evidence either.
    let mut legacy_row = SyncStatus::for_pipe(&public).apply(&SyncOutcome::new(
        false,
        "LEGACY PRIVATE ROW",
        SyncSource::Python,
        None,
    ));
    legacy_row.registration_id = None;
    sync_status::write_status(fixture.conn.as_ref(), &legacy_row)
        .await
        .unwrap();
    for uri in [
        "/engine/sync-status",
        "/engine/sync-status?pipe_ref=plugin:source%7Cstatus-recreated%7CNone",
    ] {
        let response = send(&fixture.app, req("GET", uri, Some(&alice), None)).await;
        assert_eq!(response.status, StatusCode::OK);
        assert!(
            response.json()["statuses"].as_array().unwrap().is_empty(),
            "{}",
            response.text()
        );
        assert!(!response.text().contains("PRIVATE"));
    }
    let fresh = send(
        &fixture.app,
        post(
            "status-recreated",
            outcome("current result", Some(&current)),
        ),
    )
    .await;
    assert_eq!(fresh.status, StatusCode::OK, "{}", fresh.text());
    assert_eq!(fresh.json()["status"]["message"], "current result");
    assert_eq!(
        fresh.json()["status"]["consecutive_failures"],
        1,
        "legacy/private failures must not merge into a new segment"
    );
    assert!(!fresh.text().contains("registration_id") && !fresh.text().contains(&current));
    let unregistered = send(
        &fixture.app,
        post("never-registered", outcome("unregistered diagnostic", None)),
    )
    .await;
    assert_eq!(
        unregistered.status,
        StatusCode::OK,
        "{}",
        unregistered.text()
    );
    assert_eq!(
        unregistered.json()["status"]["message"],
        "unregistered diagnostic"
    );
    let all = send(
        &fixture.app,
        req("GET", "/engine/sync-status", Some(&alice), None),
    )
    .await;
    assert_eq!(
        all.json()["statuses"].as_array().unwrap().len(),
        2,
        "{}",
        all.text()
    );
    assert!(!all.text().contains("PRIVATE") && !all.text().contains("registration_id"));
    for operation in ["read", "write", "clear"] {
        let raw = send(
            &fixture.app,
            req(
                "POST",
                &format!("/engine/pipes/engine_sync_status/{operation}"),
                Some(&alice),
                Some(("application/json", b"{}".to_vec())),
            ),
        )
        .await;
        assert_eq!(raw.status, StatusCode::NOT_FOUND, "{}", raw.text());
        assert!(!raw.text().contains("PRIVATE"));
    }
    let exists = send(
        &fixture.app,
        req(
            "GET",
            "/engine/pipes/engine_sync_status/exists",
            Some(&alice),
            None,
        ),
    )
    .await;
    assert_eq!(exists.status, StatusCode::NOT_FOUND);
    let admin = login(&fixture.app, "admin", "fixture-password", None).await;
    let admin_raw = send(
        &fixture.app,
        req(
            "POST",
            "/engine/pipes/engine_sync_status/read",
            Some(&admin),
            Some(("application/json", b"{}".to_vec())),
        ),
    )
    .await;
    assert_eq!(admin_raw.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn metrics_require_read_scope_and_filter_private_rows_before_every_aggregate() {
    let fixture = fixture(false).await;
    let denied = send(&fixture.app, req("GET", "/engine/metrics", None, None)).await;
    assert_eq!(denied.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        send(
            &fixture.app,
            req(
                "POST",
                "/engine/run-generations",
                None,
                Some(("application/json", br#"{"run_ids":[]}"#.to_vec()))
            )
        )
        .await
        .status,
        StatusCode::UNAUTHORIZED
    );
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let answer = send(
        &fixture.app,
        req("GET", "/engine/metrics", Some(&alice), None),
    )
    .await;
    assert_eq!(answer.status, StatusCode::OK, "{}", answer.text());
    let metrics = answer.json()["metrics"].clone();
    assert_eq!(metrics["summary"]["syncs"], 1);
    assert_eq!(metrics["summary"]["rows"], 3);
    let text = answer.text();
    assert!(!text.contains("private") && !text.contains("SECRET") && !text.contains("PRIVATE JOB"));
    let narrow = login(
        &fixture.app,
        "admin",
        "fixture-password",
        Some("pipes:write"),
    )
    .await;
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/metrics", Some(&narrow), None)
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
    assert_eq!(
        send(
            &fixture.app,
            req(
                "POST",
                "/engine/run-generations",
                Some(&narrow),
                Some(("application/json", br#"{"run_ids":[]}"#.to_vec()))
            )
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
    let empty = send(
        &fixture.app,
        req(
            "GET",
            "/engine/metrics?visible_refs=%5B%5D",
            Some(&alice),
            None,
        ),
    )
    .await;
    assert_eq!(empty.json()["metrics"]["summary"]["syncs"], 0);
    let raw = send(
        &fixture.app,
        req(
            "POST",
            "/engine/pipes/engine_metrics/read",
            Some(&alice),
            Some(("application/json", b"{}".to_vec())),
        ),
    )
    .await;
    assert_eq!(
        raw.status,
        StatusCode::NOT_FOUND,
        "raw history must not bypass the aggregate filter"
    );
    let generic = send(
        &fixture.app,
        req(
            "GET",
            "/pipes/mrsm/engine_metrics/None/data",
            Some(&alice),
            None,
        ),
    )
    .await;
    assert_eq!(
        generic.status,
        StatusCode::FORBIDDEN,
        "ordinary Pipe data reads must not expose unfiltered internal history"
    );
}

#[tokio::test]
async fn another_workers_scope_or_privacy_change_is_applied_without_a_cache_wait() {
    let fixture = fixture(false).await;
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/metrics", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::OK
    );
    let mut user = User::new("alice", None);
    user.attributes = Some(map(json!({"role":"editor", "scopes":[]})));
    assert!(fixture.conn.edit_user(&user).await.success);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/metrics", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn telemetry_cannot_claim_another_pipe_or_relabel_an_old_privacy_segment() {
    let fixture = fixture(false).await;
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let private = SyncMetric::from_outcome(
        &fixture.private,
        &SyncOutcome::new(true, "", SyncSource::Python, None),
    );
    let answer = send(
        &fixture.app,
        req(
            "POST",
            "/engine/metrics",
            Some(&alice),
            Some((
                "application/json",
                json!({"metrics":[private]}).to_string().into_bytes(),
            )),
        ),
    )
    .await;
    assert_eq!(answer.status, StatusCode::NOT_FOUND);
    let mut old = SyncMetric::from_outcome(
        &fixture.public,
        &SyncOutcome::new(true, "", SyncSource::Python, None),
    );
    old.registration_id = "old-private-segment".into();
    let answer = send(
        &fixture.app,
        req(
            "POST",
            "/engine/metrics",
            Some(&alice),
            Some((
                "application/json",
                json!({"metrics":[old]}).to_string().into_bytes(),
            )),
        ),
    )
    .await;
    assert_eq!(answer.status, StatusCode::CONFLICT);
    let answer = send(
        &fixture.app,
        req("POST", "/engine/metrics/initialize", Some(&alice), None),
    )
    .await;
    assert_eq!(answer.status, StatusCode::FORBIDDEN);
    for (pipe, identity, expected) in [
        (
            &fixture.private,
            meerschaum_connector::sync_metrics::observed_registration(&fixture.private).unwrap(),
            StatusCode::NOT_FOUND,
        ),
        (
            &fixture.public,
            "old-registration".into(),
            StatusCode::CONFLICT,
        ),
    ] {
        let body = json!([{"run_id":"untrusted", "pipe_ref":format!(
            "api:client|{}|{}|None", pipe.connector_keys(), pipe.metric_key()),
            "registration_id":identity}]);
        let mut request = req(
            "POST",
            "/engine/pipes/engine_nodes/write",
            Some(&alice),
            Some(("application/json", body.to_string().into_bytes())),
        );
        request.headers_mut().insert(
            meerschaum_connector_api::engine_pipes::CLIENT_INSTANCE_HEADER,
            axum::http::HeaderValue::from_static("api:client"),
        );
        assert_eq!(send(&fixture.app, request).await.status, expected);
    }
}

#[tokio::test]
async fn native_api_client_reads_and_records_the_same_canonical_numeric_history() {
    let fixture = fixture(true).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = fixture.app.clone();
    let task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = APIConnector::new(
        Some("metrics_client"),
        map(json!({"host":"127.0.0.1", "port":port})),
    )
    .unwrap();
    let mut metric = SyncMetric::from_outcome(
        &fixture.public,
        &SyncOutcome::new(true, "", SyncSource::Python, None).with_counts(5, 0, 0),
    );
    metric.pipe_ref = "forged|keys|are|ignored".into();
    client.post_sync_metrics(&[metric]).await.unwrap();
    let result = client
        .get_remote_engine_metrics(&MetricsQuery::default())
        .await
        .unwrap();
    assert_eq!(result.summary.rows, 1007);
    assert!(
        result
            .history
            .iter()
            .all(|row| row.pipe_ref.starts_with("api:metrics_client|"))
    );
    // The native client records one logical sync, even when it posts multiple chunks.
    // An empty replacement must carry the same header or the server would add a duplicate.
    use meerschaum_connector::{DateBound, SyncOptions, pipe_ops, sync_status};
    use meerschaum_kernel::{
        enforce::{PipeInput, pipe_enforce_dtypes},
        value::Value as KValue,
    };
    let pipe = Pipe::builder("plugin:metrics", "protocol")
        .instance("api:metrics_client")
        .parameters(map(
            json!({"columns":{"datetime":"id"},"dtypes":{"id":"int"}}),
        ))
        .build()
        .unwrap();
    assert!(client.register_pipe(&pipe).await.success);
    InstanceConnector::get_pipe_id(&client, &pipe)
        .await
        .unwrap()
        .unwrap();
    let context = pipe_ops::dtypes_context(&client, &pipe, pipe.parameters())
        .await
        .unwrap();
    let frame = pipe_enforce_dtypes(
        PipeInput::Docs(vec![
            [("id".to_string(), KValue::Int(1))].into_iter().collect(),
            [("id".to_string(), KValue::Int(2))].into_iter().collect(),
        ]),
        &context,
    )
    .unwrap();
    let inserted = sync_status::recorded_scope(client.sync_pipe_counted(
        &pipe,
        frame.clone(),
        &SyncOptions {
            chunksize: Some(1),
            ..Default::default()
        },
    ))
    .await;
    assert!(inserted.success, "{}", inserted.message);
    let metric = SyncMetric::from_outcome(
        &pipe,
        &SyncOutcome::new(true, inserted.message, SyncSource::Python, None).with_counts(
            inserted.inserted,
            inserted.updated,
            inserted.upserted,
        ),
    );
    client
        .post_sync_metrics(std::slice::from_ref(&metric))
        .await
        .unwrap();
    client
        .post_sync_metrics(std::slice::from_ref(&metric))
        .await
        .unwrap();
    let replaced = sync_status::recorded_scope(client.sync_pipe_counted(
        &pipe,
        frame.take(&[]),
        &SyncOptions {
            replace: true,
            begin: Some(DateBound::Int(1)),
            end: Some(DateBound::Int(3)),
            ..Default::default()
        },
    ))
    .await;
    assert!(replaced.success, "{}", replaced.message);
    let mut outcome = SyncOutcome::new(true, replaced.message, SyncSource::Python, None)
        .with_counts(replaced.inserted, replaced.updated, replaced.upserted);
    outcome.deleted = replaced.deleted;
    client
        .post_sync_metrics(&[SyncMetric::from_outcome(&pipe, &outcome)])
        .await
        .unwrap();
    sync_status::flush(Duration::from_secs(4)).await;
    let result = client
        .get_remote_engine_metrics(&MetricsQuery {
            pipe_ref: Some(pipe.pipe_ref()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.summary.syncs, 2, "{result:?}");
    assert_eq!(result.summary.rows, 4);
    assert_eq!(result.summary.deleted, 2);

    let metadata = Pipe::builder("mrsm", "pipeline")
        .location(Some("api-daily"))
        .instance("sql:metrics")
        .parameters(map(
            json!({"pipeline":{"name":"api-daily", "id":"api-pipeline"}}),
        ))
        .build()
        .unwrap();
    assert!(fixture.conn.register_pipe(&metadata).await.success);
    // Older hosts have an existing node table without the new registration columns.
    assert!(
        fixture
            .conn
            .exec("CREATE TABLE mrsm_engine_nodes (started TIMESTAMP, run_id TEXT, pipe_ref TEXT)")
            .await
            .is_some()
    );
    let started = (chrono::Utc::now() - chrono::TimeDelta::seconds(10)).to_rfc3339();
    let finished = (chrono::Utc::now() - chrono::TimeDelta::seconds(1)).to_rfc3339();
    let registration = meerschaum_connector::sync_metrics::observed_registration(&pipe).unwrap();
    for (run_id, reference, rows) in [
        ("api-run", pipe.pipe_ref(), 12),
        (
            "foreign-secret-run",
            "api:foreign|plugin:metrics|protocol|None".into(),
            900,
        ),
    ] {
        let runs = meerschaum_connector_api::engine_pipes::engine_frame("engine_runs",
            meerschaum_connector_api::engine_pipes::docs_from_records(&[json!({
                "run_id":run_id,"started":started,"finished":finished,"success":true,
                "targets":[reference],"num_nodes":1,
                "selection":json!({"pipeline_definitions":[{"name":"api-daily","id":"api-pipeline", "registry":"api:metrics_client"}]}).to_string()
            })])).unwrap();
        assert!(client.write_engine_pipe("engine_runs", &runs).await.success);
        let nodes = meerschaum_connector_api::engine_pipes::engine_frame(
            "engine_nodes",
            meerschaum_connector_api::engine_pipes::docs_from_records(&[json!({
                "run_id":run_id,"pipe_ref":reference,"registration_id":registration,
                "registration_ref":"sql:metrics|plugin:metrics|protocol|None",
                "started":started,"finished":finished,"status":"succeeded", "attempts":1,
                "inserted":rows
            })]),
        )
        .unwrap();
        let written = client.write_engine_pipe("engine_nodes", &nodes).await;
        assert!(written.success, "{}", written.message);
    }
    let result = client
        .get_remote_engine_metrics(&MetricsQuery {
            pipeline: Some("api-daily".into()),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.pipelines.len(), 1, "{result:?}");
    assert_eq!(result.pipelines[0].summary.syncs, 1);
    assert_eq!(result.pipelines[0].summary.rows, 12);
    assert_eq!(result.pipelines[0].last_run_id.as_deref(), Some("api-run"));
    assert!(
        !serde_json::to_string(&result)
            .unwrap()
            .contains("foreign-secret")
    );
    let stale = meerschaum_connector_api::engine_pipes::engine_frame(
        "engine_nodes",
        meerschaum_connector_api::engine_pipes::docs_from_records(&[json!({
            "run_id":"api-run","pipe_ref":pipe.pipe_ref(),"registration_id":"old-registration",
            "started":started,"finished":finished,"status":"succeeded"
        })]),
    )
    .unwrap();
    assert!(
        !client
            .write_engine_pipe("engine_nodes", &stale)
            .await
            .success
    );
    let renamed = client
        .rename_pipeline(&metadata, "api-daily-renamed", None, Some("api-pipeline"))
        .await
        .unwrap();
    assert_eq!(
        renamed.get("id").and_then(Value::as_str),
        Some("api-pipeline")
    );
    assert_eq!(
        renamed.get("name").and_then(Value::as_str),
        Some("api-daily-renamed")
    );
    assert_eq!(
        renamed.get("registry").and_then(Value::as_str),
        Some("api:metrics_client")
    );
    assert_eq!(
        InstanceConnector::saved_pipeline_identity(&client, "api-daily-renamed")
            .await
            .unwrap(),
        Some(("api-pipeline".into(), "sql:metrics".into())),
        "history identity retains the exact server registry before public alias rebinding"
    );
    assert!(
        InstanceConnector::saved_pipeline_identity(&client, "missing-pipeline")
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        fixture
            .conn
            .get_pipe_attributes(&metadata)
            .await
            .unwrap()
            .is_empty()
    );
    task.abort();
}

#[tokio::test]
async fn connector_team_policy_applies_before_metrics_reads_and_to_source_visibility() {
    let restricted = fixture_with(false, |config| {
        config.connectors = map(json!({"sql":{"metrics":{"teams":["Restricted"]}}}));
    })
    .await;
    let alice = login(&restricted.app, "alice", "fixture-password", None).await;
    restricted.conn.enable_query_log();
    let response = send(
        &restricted.app,
        req("GET", "/engine/metrics", Some(&alice), None),
    )
    .await;
    assert_eq!(response.status, StatusCode::FORBIDDEN);
    for uri in [
        "/engine/runs",
        "/engine/runs/missing",
        "/engine/runs/missing/events/stream",
    ] {
        let response = send(&restricted.app, req("GET", uri, Some(&alice), None)).await;
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{uri}: {}",
            response.text()
        );
    }
    for (uri, body) in [
        ("/engine/pipes/engine_runs/read", json!({})),
        ("/engine/runs", json!([])),
        ("/engine/run-generations", json!({"run_ids":[]})),
    ] {
        let response = send(
            &restricted.app,
            req(
                "POST",
                uri,
                Some(&alice),
                Some(("application/json", body.to_string().into_bytes())),
            ),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{uri}: {}",
            response.text()
        );
    }
    for (tool, args) in [
        ("list_runs", json!({})),
        ("get_run", json!({"run_id":"missing"})),
    ] {
        let response = history_tool(&restricted, tool, args).await;
        assert_eq!(response["result"]["isError"], true, "{response}");
    }
    assert!(
        !restricted
            .conn
            .take_query_log()
            .iter()
            .any(|sql| sql.contains("mrsm_engine_"))
    );
    let sources = fixture_with(false, |config| {
        config.connectors = map(json!({"plugin":{"source":{"teams":["Restricted"]}}}));
    })
    .await;
    let alice = login(&sources.app, "alice", "fixture-password", None).await;
    let response = send(
        &sources.app,
        req("GET", "/engine/metrics", Some(&alice), None),
    )
    .await;
    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.json()["metrics"]["summary"]["syncs"], 0);
    assert_eq!(
        send(
            &sources.app,
            req(
                "GET",
                "/engine/metrics?instance_keys=sql:unserved",
                Some(&alice),
                None
            )
        )
        .await
        .status,
        StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn valkey_list_metadata_cannot_hide_private_access_parameters() {
    use meerschaum_connector_valkey::{MemoryBackend, ValkeyConnector};
    let conn = Arc::new(ValkeyConnector::with_backend(
        "metrics",
        Map::new(),
        Arc::new(MemoryBackend::new()),
    ));
    let mut user = User::new("alice", Some("fixture-password"));
    assert!(conn.register_user(&mut user).await.success);
    for (name, parameters, rows) in [
        ("public", json!({}), 2),
        (
            "private",
            json!({"access":{"owner":"bob","visibility":"private"}}),
            900,
        ),
    ] {
        let pipe = Pipe::builder("plugin:metrics", name)
            .instance("valkey:metrics")
            .parameters(map(parameters))
            .build()
            .unwrap();
        assert!(conn.register_pipe(&pipe).await.success);
        conn.get_pipe_id(&pipe).await.unwrap().unwrap();
        let metric = SyncMetric::from_outcome(
            &pipe,
            &SyncOutcome::new(true, "", SyncSource::Python, None).with_counts(rows, 0, 0),
        );
        conn.record_sync_metrics(&[metric]).await.unwrap();
    }
    let dir = tempfile::tempdir().unwrap();
    let mut config = ServerConfig::for_root("valkey:metrics", dir.path());
    config.no_dash = true;
    config.no_webterm = true;
    let state = AppState::builder(config, conn)
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let alice = login(&app, "alice", "fixture-password", None).await;
    let response = send(&app, req("GET", "/engine/metrics", Some(&alice), None)).await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    assert_eq!(response.json()["metrics"]["summary"]["rows"], 2);
    assert!(!response.text().contains("private"));
    let private = send(
        &app,
        req(
            "GET",
            "/pipes/plugin:metrics/private/None/attributes",
            Some(&alice),
            None,
        ),
    )
    .await;
    assert_eq!(private.status, StatusCode::NOT_FOUND);
}
