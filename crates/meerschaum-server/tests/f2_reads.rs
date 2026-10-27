//! rc15 Q17 over HTTP: a read the instance fails answers `500` (data, row count, sync time),
//! so `api:` clients raise instead of reading an outage as an empty pipe, while a registered
//! pipe without rows still reads as empty.

mod common;

use std::sync::Arc;

use axum::http::StatusCode;
use common::{MemoryInstance, SECRET, config, login, req, send};
use meerschaum_server::AppState;
use serde_json::json;

fn body(j: serde_json::Value) -> Option<(&'static str, Vec<u8>)> {
    Some(("application/json", j.to_string().into_bytes()))
}

#[tokio::test]
async fn failed_reads_are_server_errors() {
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let state = AppState::builder(config(root.path()), instance.clone())
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    for (method, path, payload) in [
        (
            "POST",
            "/pipes/sql:x/m/None/register",
            json!({"columns": {"datetime": "ts", "id": "id"}}),
        ),
        (
            "POST",
            "/pipes/sql:x/m/None/data",
            json!([{"ts": "2024-01-01 00:00:00", "id": 1}, {"ts": "2024-01-02 00:00:00", "id": 2}]),
        ),
    ] {
        let r = send(&app, req(method, path, Some(&admin), body(payload))).await;
        assert_eq!(r.status, StatusCode::OK, "{path}: {}", r.text());
    }
    let get = |path: &'static str| {
        let app = app.clone();
        let admin = admin.clone();
        async move { send(&app, req("GET", path, Some(&admin), None)).await }
    };
    let r = get("/pipes/sql:x/m/None/rowcount").await;
    assert_eq!((r.status, r.json()), (StatusCode::OK, json!(2)));

    instance.fail_data_reads(true);
    for path in [
        "/pipes/sql:x/m/None/rowcount",
        "/pipes/sql:x/m/None/rowcount?estimate=true",
        "/pipes/sql:x/m/None/sync_time",
        "/pipes/sql:x/m/None/data",
    ] {
        let r = get(path).await;
        assert_eq!(
            r.status,
            StatusCode::INTERNAL_SERVER_ERROR,
            "{path}: {}",
            r.text()
        );
    }
    instance.fail_data_reads(false);

    // A registered pipe without rows: 0 rows, no sync time.
    let r = send(
        &app,
        req(
            "POST",
            "/pipes/sql:x/empty/None/register",
            Some(&admin),
            body(json!({"columns": {"datetime": "ts"}})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = get("/pipes/sql:x/empty/None/rowcount").await;
    assert_eq!((r.status, r.json()), (StatusCode::OK, json!(0)));
    let r = get("/pipes/sql:x/empty/None/sync_time").await;
    assert_eq!((r.status, r.json()), (StatusCode::OK, json!(null)));
}
