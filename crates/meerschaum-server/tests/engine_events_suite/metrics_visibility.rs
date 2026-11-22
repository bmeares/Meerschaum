//! Fresh scopes, object/team visibility and canonical transport for persisted numeric metrics.

use crate::common::{SECRET, login, req, send};
use axum::Router;
use axum::http::StatusCode;
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

mod canonical_transport;
mod history_identity;
mod history_visibility;
mod metrics_access;

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
