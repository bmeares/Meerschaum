//! Data and CSV routes read a pipe's current registration and parameters from the instance.

use std::sync::Arc;

use crate::common::{MemoryInstance, SECRET, login, req, send};
use axum::Router;
use axum::http::StatusCode;
use meerschaum_connector::{InstanceConnector, SyncOptions, pipe_ops};
use meerschaum_connector_sql::SqlConnector;
use meerschaum_connector_sql::driver::SqlParam;
use meerschaum_connector_valkey::{MemoryBackend, ValkeyConnector};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::Value as KValue;
use meerschaum_server::{AppState, ServerConfig};

const SOURCE: &str = "sql:source";

struct Served {
    conn: Arc<SqlConnector>,
    app: Router,
    uri: String,
    _dir: tempfile::TempDir,
}

/// A server (`--no-auth`) over a fresh SQLite instance.
async fn served() -> Served {
    let dir = tempfile::tempdir().unwrap();
    let uri = format!("sqlite:///{}", dir.path().join("lookup.db").display());
    let conn = Arc::new(SqlConnector::from_uri("lookup", &uri).await.unwrap());
    let mut cfg = ServerConfig::for_root(&conn.keys(), dir.path());
    cfg.no_auth = true;
    cfg.no_dash = true;
    cfg.no_webterm = true;
    let instance: Arc<dyn InstanceConnector> = conn.clone();
    let state = AppState::builder(cfg, instance)
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    Served {
        conn,
        app: meerschaum_server::app(state),
        uri,
        _dir: dir,
    }
}

fn pipe(conn: &dyn InstanceConnector, metric: &str) -> Pipe {
    let parameters: Value =
        serde_json::from_value(serde_json::json!({"columns": {"datetime": "ts"}})).unwrap();
    Pipe::builder(SOURCE, metric)
        .instance(&conn.connector_keys())
        .parameters(parameters.to_map())
        .build()
        .unwrap()
}

/// Register `metric` with one row.
async fn register(conn: &dyn InstanceConnector, metric: &str) {
    register_pipe(conn, &pipe(conn, metric)).await;
}

async fn register_pipe(conn: &dyn InstanceConnector, pipe: &Pipe) {
    let res = conn.register_pipe(pipe).await;
    assert!(res.success, "{res}");
    let context = pipe_ops::dtypes_context(conn, pipe, pipe.parameters())
        .await
        .unwrap();
    let frame = pipe_enforce_dtypes(
        PipeInput::Docs(vec![
            [
                ("ts".into(), KValue::Str("2026-01-01".into())),
                ("value".into(), KValue::Int(1)),
            ]
            .into_iter()
            .collect(),
        ]),
        &context,
    )
    .unwrap();
    let res = conn.sync_pipe(pipe, frame, &SyncOptions::default()).await;
    assert!(res.success, "{res}");
}

/// Register `metric` without data.
async fn register_only(conn: &dyn InstanceConnector, metric: &str) {
    let res = conn.register_pipe(&pipe(conn, metric)).await;
    assert!(res.success, "{res}");
}

/// Register `count` other pipes (`filler_*`).
async fn register_fillers(conn: &SqlConnector, count: usize) {
    for i in 0..count {
        register_only(conn, &format!("filler_{i}")).await;
    }
}

async fn get(app: &Router, uri: &str) -> crate::common::Resp {
    send(app, req("GET", uri, None, None)).await
}

/// `GET .../data` of `metric`: its status and the statements the instance ran for it.
async fn data_read(s: &Served, metric: &str) -> (StatusCode, Vec<String>) {
    s.conn.enable_query_log();
    let r = get(&s.app, &format!("/pipes/{SOURCE}/{metric}/None/data")).await;
    let log = s.conn.take_query_log();
    (r.status, log)
}

#[tokio::test]
async fn a_cold_sqlite_data_read_searches_the_registration_index() {
    let s = served().await;
    register(s.conn.as_ref(), "target").await;
    register_fillers(&s.conn, 60).await;
    let (status, log) = data_read(&s, "target").await;
    assert_eq!(status, StatusCode::OK, "{log:#?}");
    let catalog_reads: Vec<_> = log
        .iter()
        .filter(|s| s.contains("FROM mrsm_pipes"))
        .collect();
    // The session-table guard checks the named pipe before the read handler.
    let [sessions, lookup] = catalog_reads.as_slice() else {
        panic!("Expected two keyed registration reads: {log:#?}");
    };
    let params = [
        SqlParam::Text(SOURCE.into()),
        SqlParam::Text("target".into()),
        SqlParam::Text("None".into()),
        SqlParam::Text("mrsm".into()),
    ];
    for (query, binds) in [(sessions, &params[..2]), (lookup, &params[..])] {
        let plan = s
            .conn
            .read_params(&format!("EXPLAIN QUERY PLAN {query}"), binds)
            .await
            .unwrap();
        let details = pipe_ops::frame_to_docs(&plan)
            .into_iter()
            .filter_map(|row| row.get("detail").map(KValue::py_str))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            details.contains("SEARCH mrsm_pipes USING INDEX uq_mrsm_pipes_keys"),
            "{details}"
        );
    }
}

#[tokio::test]
async fn an_unregistered_pipe_is_refused_on_the_registered_only_reads() {
    let s = served().await;
    register_fillers(&s.conn, 3).await;
    let r = get(&s.app, &format!("/pipes/{SOURCE}/missing/None/data")).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    assert!(
        r.text()
            .contains("Pipe must be registered with the datetime column specified."),
        "{}",
        r.text()
    );
    let r = get(&s.app, &format!("/pipes/{SOURCE}/missing/None/csv")).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    // Same keys, another location: not the registered pipe.
    register(s.conn.as_ref(), "located").await;
    let r = get(&s.app, &format!("/pipes/{SOURCE}/located/elsewhere/data")).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
    let r = get(&s.app, &format!("/pipes/{SOURCE}/located/None/data")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
}

#[tokio::test]
async fn a_pipe_another_process_deletes_is_unregistered_at_the_next_read() {
    let s = served().await;
    register(s.conn.as_ref(), "target").await;
    assert_eq!(data_read(&s, "target").await.0, StatusCode::OK);
    let other = SqlConnector::from_uri("lookup_other", &s.uri)
        .await
        .unwrap();
    let res = other.delete_pipe(&pipe(&other, "target")).await;
    assert!(res.success, "{res}");
    assert_eq!(data_read(&s, "target").await.0, StatusCode::CONFLICT);
    let r = get(&s.app, &format!("/pipes/{SOURCE}/target/None/csv")).await;
    assert_eq!(r.status, StatusCode::CONFLICT, "{}", r.text());
}

#[tokio::test]
async fn a_pipe_another_process_registers_is_registered_at_the_next_read() {
    let s = served().await;
    assert_eq!(data_read(&s, "target").await.0, StatusCode::CONFLICT);
    let other = SqlConnector::from_uri("lookup_other", &s.uri)
        .await
        .unwrap();
    register(&other, "target").await;
    assert_eq!(data_read(&s, "target").await.0, StatusCode::OK);
    let r = get(&s.app, &format!("/pipes/{SOURCE}/target/None/csv")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
}

#[tokio::test]
async fn both_read_routes_use_another_connections_edits_and_replacements() {
    let s = served().await;
    register(s.conn.as_ref(), "target").await;
    let other = SqlConnector::from_uri("lookup_other", &s.uri)
        .await
        .unwrap();
    for suffix in ["data", "csv"] {
        let r = get(&s.app, &format!("/pipes/{SOURCE}/target/None/{suffix}")).await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
        assert!(r.text().contains("value"), "{}", r.text());
    }

    let mut pipe = pipe(&other, "target");
    for (step, target, marker) in [
        ("edit", "new_lookup_target", "edited_value"),
        ("replace", "replacement_lookup_target", "replacement_value"),
    ] {
        assert!(
            other
                .exec(&format!("CREATE TABLE {target} ({marker} INTEGER)"))
                .await
                .is_some()
        );
        assert!(
            other
                .exec(&format!("INSERT INTO {target} VALUES (42)"))
                .await
                .is_some()
        );
        let parameters: Value =
            serde_json::from_value(serde_json::json!({"target": target})).unwrap();
        if step == "edit" {
            pipe.set_parameters(parameters.to_map());
            let result = other.edit_pipe(&pipe, false).await;
            assert!(result.success, "{result}");
        } else {
            let result = other.delete_pipe(&pipe).await;
            assert!(result.success, "{result}");
            pipe.set_parameters(parameters.to_map());
            let result = other.register_pipe(&pipe).await;
            assert!(result.success, "{result}");
        }
        for suffix in ["data", "csv"] {
            let r = get(&s.app, &format!("/pipes/{SOURCE}/target/None/{suffix}")).await;
            assert_eq!(r.status, StatusCode::OK, "{step}/{suffix}: {}", r.text());
            assert!(r.text().contains(marker), "{step}/{suffix}: {}", r.text());
        }
    }
}

#[tokio::test]
async fn keys_that_read_as_filters_are_matched_exactly() {
    let s = served().await;
    // The wildcard is filter syntax in a keys query; a numeric key must stay text.
    for metric in ["*", "123"] {
        register(s.conn.as_ref(), metric).await;
    }
    let r = get(&s.app, &format!("/pipes/{SOURCE}/%2A/None/data")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let (status, _) = data_read(&s, "123").await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = data_read(&s, "1234").await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn internal_pipes_are_refused_on_the_generic_routes() {
    let s = served().await;
    for suffix in ["/data", "/csv"] {
        let r = get(&s.app, &format!("/pipes/mrsm/engine_runs/None{suffix}")).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{suffix}: {}", r.text());
    }
}

#[tokio::test]
async fn a_valkey_instance_sees_registrations_by_their_exact_keys() {
    let backend = Arc::new(MemoryBackend::new());
    let conn = Arc::new(ValkeyConnector::with_backend(
        "lookup",
        Map::new(),
        backend.clone(),
    ));
    let other = ValkeyConnector::with_backend("lookup", Map::new(), backend);
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = ServerConfig::for_root("valkey:lookup", dir.path());
    cfg.no_auth = true;
    cfg.no_dash = true;
    cfg.no_webterm = true;
    let state = AppState::builder(cfg, conn.clone())
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    for metric in ["target", "123", "*"] {
        register_only(conn.as_ref(), metric).await;
    }
    for metric in ["target", "123", "*"] {
        assert!(
            state
                .is_registered(&pipe(conn.as_ref(), metric), None)
                .await
                .unwrap(),
            "{metric}"
        );
    }
    assert!(
        !state
            .is_registered(&pipe(conn.as_ref(), "missing"), None)
            .await
            .unwrap()
    );
    let res = other.delete_pipe(&pipe(&other, "target")).await;
    assert!(res.success, "{res}");
    assert!(
        !state
            .is_registered(&pipe(conn.as_ref(), "target"), None)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn a_hidden_registered_pipe_stays_hidden_and_an_unregistered_one_stays_unregistered() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("otto", "pw", None);
    let mut private = pipe(instance.as_ref(), "private");
    let parameters: Value = serde_json::from_value(serde_json::json!({
        "columns": {"datetime": "ts"},
        "access": {"owner": "admin", "visibility": "private"}
    }))
    .unwrap();
    private.set_parameters(parameters.to_map());
    register_pipe(instance.as_ref(), &private).await;
    let mut cfg = crate::common::config(dir.path());
    cfg.no_dash = true;
    cfg.no_webterm = true;
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let otto = login(&app, "otto", "pw", None).await;
    let admin = login(&app, "admin", "pass", None).await;
    for suffix in ["data", "csv"] {
        let uri = format!("/pipes/{SOURCE}/private/None/{suffix}");
        let r = send(&app, req("GET", &uri, Some(&otto), None)).await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{suffix}: {}", r.text());
        let r = send(&app, req("GET", &uri, Some(&admin), None)).await;
        assert_eq!(r.status, StatusCode::OK, "{suffix}: {}", r.text());
        let uri = format!("/pipes/{SOURCE}/missing/None/{suffix}");
        let r = send(&app, req("GET", &uri, Some(&otto), None)).await;
        assert_eq!(r.status, StatusCode::CONFLICT, "{suffix}: {}", r.text());
    }
}
