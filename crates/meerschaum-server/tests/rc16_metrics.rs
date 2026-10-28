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
        app: meerschaum_server::app(state),
        conn,
        public,
        private,
        _dir: dir,
    }
}

#[tokio::test]
async fn metrics_require_read_scope_and_filter_private_rows_before_every_aggregate() {
    let fixture = fixture(false).await;
    let denied = send(&fixture.app, req("GET", "/engine/metrics", None, None)).await;
    assert_eq!(denied.status, StatusCode::UNAUTHORIZED);
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
                "selection":json!({"pipeline_definitions":[{"name":"api-daily","id":"api-pipeline"}]}).to_string()
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
    assert!(
        !restricted
            .conn
            .take_query_log()
            .iter()
            .any(|sql| sql.contains("mrsm_engine_metrics"))
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
