//! `/engine/*` over the instance-backed store (§5.8, R2): the engine-pipe primitives
//! (`/engine/pipes/{metric}/{read,write,clear,exists}`) with their scopes and refusals, the
//! per-shape routes mapped onto the same rows, persistence across a new server on the same
//! instance, and `501` from the in-memory backend.

mod common;

use std::path::Path;
use std::sync::Arc;

use axum::http::StatusCode;
use common::{SECRET, login, req, send};
use meerschaum_connector::InstanceConnector;
use meerschaum_connector_sql::SqlConnector;
use meerschaum_server::AppState;
use meerschaum_server::engine_store::InstanceEngine;
use serde_json::json;

/// A router whose users come from the in-memory fixture instance (`admin` / `pass`) and whose
/// engine state lives on the SQLite database at `db`.
async fn app(dir: &Path, db: &Path) -> axum::Router {
    let store = Arc::new(
        SqlConnector::from_uri("w17bgstore", &format!("sqlite:///{}", db.display()))
            .await
            .unwrap(),
    );
    let store: Arc<dyn InstanceConnector> = store;
    let users = Arc::new(common::MemoryInstance::new("fixture"));
    users.seed_user("admin", "pass", Some("admin"));
    let state = AppState::builder(common::config(dir), users)
        .secret(SECRET.to_vec())
        .engine(Arc::new(InstanceEngine::new(store, "sql:w17bgstore")))
        .build()
        .unwrap();
    meerschaum_server::app(state)
}

fn body(j: serde_json::Value) -> Option<(&'static str, Vec<u8>)> {
    Some(("application/json", j.to_string().into_bytes()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_pipe_routes_scopes_and_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("store.db");
    let app = app(dir.path(), &db).await;
    let admin = login(&app, "admin", "pass", None).await;
    let reader = login(&app, "admin", "pass", Some("pipes:read")).await;
    let read = "/engine/pipes/engine_ledger/read";
    let write = "/engine/pipes/engine_ledger/write";
    let clear = "/engine/pipes/engine_ledger/clear";
    let exists = "/engine/pipes/engine_ledger/exists";

    // Refusals: no token, a read-only token on writes, and any metric but the six.
    let r = send(&app, req("POST", read, None, body(json!({})))).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let rows = json!([{"created": "2026-01-01T00:00:00+00:00", "pipe_ref": "api:x|a|b|None",
                       "seq": 1, "begin": {"datetime": "2026-01-01T00:00:00+00:00"}, "end": null}]);
    let r = send(&app, req("POST", write, Some(&reader), body(rows.clone()))).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let r = send(&app, req("POST", clear, Some(&reader), body(json!({})))).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    for metric in ["users", "pipes", "engine_secret"] {
        let r = send(
            &app,
            req(
                "POST",
                &format!("/engine/pipes/{metric}/read"),
                Some(&admin),
                body(json!({})),
            ),
        )
        .await;
        assert_eq!(r.status, StatusCode::NOT_FOUND, "{metric}: {}", r.text());
    }

    // Nothing yet.
    let r = send(&app, req("GET", exists, Some(&reader), None)).await;
    assert_eq!(r.json(), json!(false));
    let r = send(&app, req("POST", read, Some(&reader), body(json!({})))).await;
    assert_eq!(r.json(), json!([]));

    // Write, read back with params, clear.
    let r = send(&app, req("POST", write, Some(&admin), body(rows))).await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let r = send(&app, req("GET", exists, Some(&reader), None)).await;
    assert_eq!(r.json(), json!(true));
    let r = send(
        &app,
        req(
            "POST",
            read,
            Some(&reader),
            body(json!({"params": {"pipe_ref": "api:x|a|b|None"}})),
        ),
    )
    .await;
    let got = r.json();
    assert_eq!(got.as_array().unwrap().len(), 1, "{got}");
    assert_eq!(got[0]["seq"], 1);
    assert_eq!(
        got[0]["begin"],
        json!({"datetime": "2026-01-01T00:00:00+00:00"})
    );
    let r = send(
        &app,
        req(
            "POST",
            read,
            Some(&reader),
            body(json!({"params": {"pipe_ref": "other"}})),
        ),
    )
    .await;
    assert_eq!(r.json(), json!([]));
    let r = send(
        &app,
        req(
            "POST",
            clear,
            Some(&admin),
            body(json!({"params": {"pipe_ref": "api:x|a|b|None"}})),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let r = send(&app, req("POST", read, Some(&reader), body(json!({})))).await;
    assert_eq!(r.json(), json!([]));

    // The rows live in the instance's own engine table.
    let check = SqlConnector::from_uri("w17bgchk", &format!("sqlite:///{}", db.display()))
        .await
        .unwrap();
    assert!(check.table_exists("mrsm_engine_ledger", None).await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn per_shape_routes_persist_across_servers() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("store.db");
    let app = app(dir.path(), &db).await;
    let admin = login(&app, "admin", "pass", None).await;
    let reader = login(&app, "admin", "pass", Some("pipes:read")).await;

    let r = send(
        &app,
        req("PUT", "/engine/cursors", Some(&reader), body(json!([]))),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let r = send(
        &app,
        req(
            "PUT",
            "/engine/cursors",
            Some(&admin),
            body(json!([{"parent_ref": "p", "child_ref": "c", "seq": 1,
                         "parent_watermark_at_cursor": "2024-01-01T00:00:00+00:00"}])),
        ),
    )
    .await;
    assert_eq!(r.json(), json!([true, "Upserted 1 cursors."]));
    let r = send(
        &app,
        req(
            "POST",
            "/engine/ledger",
            Some(&admin),
            body(json!([
                {"pipe_ref": "p", "seq": 1, "created": "2024-01-01T00:00:00Z"},
                {"pipe_ref": "p", "seq": 2, "created": "2024-01-01T00:00:00Z", "begin": "2024-01-01T00:00:00+00:00", "end": 5}
            ])),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    send(
        &app,
        req(
            "PUT",
            "/engine/state",
            Some(&admin),
            body(json!([{"pipe_ref": "p", "definition_hash": "h"}])),
        ),
    )
    .await;
    let r = send(
        &app,
        req(
            "POST",
            "/engine/runs",
            Some(&admin),
            body(json!([
                {"run_id": "r1", "started": "2024-01-01T00:00:00Z", "status": "success", "message": "ok"},
                {"run_id": "r1", "pipe_ref": "c", "started": "2024-01-01T00:00:00Z", "status": "succeeded", "inserted": 3, "action": "incremental"}
            ])),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let r = send(
        &app,
        req(
            "POST",
            "/engine/events",
            Some(&admin),
            body(json!([
                {"run_id": "r1", "created": "2024-01-01T00:00:00Z", "event": "run_started"},
                {"run_id": "r1", "created": "2024-01-01T00:00:01Z", "event": "run_finished", "payload": {"success": true}}
            ])),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let lease = json!({"pipe_ref": "api:x|sql:a|b|None", "holder": "a", "ttl_ms": 60000});
    let r = send(
        &app,
        req("POST", "/engine/leases", Some(&admin), body(lease.clone())),
    )
    .await;
    assert_eq!(r.json(), json!({"success": true}));
    drop(app);

    // A new server on the same instance (a restart) reads everything back.
    let app = app_again(dir.path(), &db).await;
    let admin = login(&app, "admin", "pass", None).await;
    let r = send(
        &app,
        req(
            "GET",
            "/engine/ledger?child_ref=c&after_seq=1",
            Some(&admin),
            None,
        ),
    )
    .await;
    let entries = r.json();
    assert_eq!(entries.as_array().unwrap().len(), 1, "{entries}");
    assert_eq!(entries[0]["seq"], 2);
    assert_eq!(entries[0]["begin"], json!("2024-01-01T00:00:00+00:00"));
    assert_eq!(entries[0]["end"], json!(5));
    let r = send(
        &app,
        req("GET", "/engine/state?pipe_ref=p", Some(&admin), None),
    )
    .await;
    assert_eq!(r.json()["definition_hash"], "h");
    let r = send(
        &app,
        req("GET", "/engine/runs?run_id=r1", Some(&admin), None),
    )
    .await;
    let runs = r.json();
    let runs = runs.as_array().unwrap();
    assert_eq!(runs.len(), 2, "{runs:?}");
    assert_eq!(runs[0]["status"], "success");
    assert_eq!(runs[0]["message"], "ok");
    let node = runs.iter().find(|r| r["pipe_ref"] == "c").unwrap();
    assert_eq!(node["inserted"], 3);
    assert_eq!(node["action"], "incremental");
    // The lease is the instance's: another holder is refused after the restart.
    let other = json!({"pipe_ref": "api:x|sql:a|b|None", "holder": "b", "ttl_ms": 60000});
    let r = send(
        &app,
        req("POST", "/engine/leases", Some(&admin), body(other)),
    )
    .await;
    assert_eq!(r.json(), json!({"success": false}));
    let r = send(
        &app,
        req("DELETE", "/engine/leases", Some(&admin), body(lease)),
    )
    .await;
    assert_eq!(r.json(), json!({"success": true}));
}

async fn app_again(dir: &Path, db: &Path) -> axum::Router {
    app(dir, db).await
}

#[tokio::test]
async fn the_in_memory_backend_keeps_no_state_pipes() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _) = common::state_with(common::config(dir.path()));
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    let r = send(
        &app,
        req(
            "POST",
            "/engine/pipes/engine_runs/read",
            Some(&admin),
            body(json!({})),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_IMPLEMENTED, "{}", r.text());
}

/// Review fixes: an unbounded clear is refused, undeclared columns never reach a state table,
/// and the body is read only after authorization and only up to the engine routes' cap.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_pipe_routes_refuse_unbounded_clears_extra_columns_and_huge_bodies() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("store.db");
    let app = app(dir.path(), &db).await;
    let admin = login(&app, "admin", "pass", None).await;
    let write = "/engine/pipes/engine_ledger/write";
    let read = "/engine/pipes/engine_ledger/read";
    let clear = "/engine/pipes/engine_ledger/clear";
    let rows = json!([
        {"created": "2026-01-01T00:00:00+00:00", "pipe_ref": "a", "seq": 1, "evil": "x"},
        {"created": "2026-01-02T00:00:00+00:00", "pipe_ref": "b", "seq": 1, "evil": "y"}
    ]);
    let r = send(&app, req("POST", write, Some(&admin), body(rows))).await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());

    // Undeclared columns are dropped: not returned, not in the table.
    let r = send(&app, req("POST", read, Some(&admin), body(json!({})))).await;
    let got = r.json();
    assert_eq!(got.as_array().unwrap().len(), 2, "{got}");
    assert!(
        got.as_array()
            .unwrap()
            .iter()
            .all(|row| row.get("evil").is_none())
    );
    let check = SqlConnector::from_uri("w17bgchk2", &format!("sqlite:///{}", db.display()))
        .await
        .unwrap();
    let evil = check
        .value("SELECT COUNT(*) FROM pragma_table_info('mrsm_engine_ledger') WHERE name = 'evil'")
        .await;
    assert!(
        matches!(
            evil,
            Some(meerschaum_connector_sql::driver::SqlValue::Int(0))
        ),
        "{evil:?}"
    );

    // Cursors and definition hashes to protect as well.
    let r = send(
        &app,
        req(
            "POST",
            "/engine/pipes/engine_cursors/write",
            Some(&admin),
            body(json!([{"parent_ref": "a", "child_ref": "b", "seq": 1}])),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let r = send(
        &app,
        req(
            "POST",
            "/engine/pipes/engine_state/write",
            Some(&admin),
            body(json!([{"pipe_ref": "a", "definition_hash": "h",
                         "updated": "2026-01-01T00:00:00+00:00"}])),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let count = |metric: &'static str| {
        let app = app.clone();
        let admin = admin.clone();
        async move {
            let r = send(
                &app,
                req(
                    "POST",
                    &format!("/engine/pipes/{metric}/read"),
                    Some(&admin),
                    body(json!({})),
                ),
            )
            .await;
            r.json().as_array().map(Vec::len).unwrap_or(0)
        }
    };

    // Unbounded or not-really-bounded clears are a 400 and delete nothing: no params, unknown
    // columns (the connector would drop them and match every row), negations and nulls, and
    // `end` alone outside the retention metrics (ignored without a datetime column, or on a
    // guessed one).
    let refused = [
        ("engine_ledger", None),
        ("engine_ledger", body(json!(null))),
        ("engine_ledger", body(json!({}))),
        ("engine_ledger", body(json!({"params": {}}))),
        ("engine_ledger", body(json!({"params": null, "end": null}))),
        ("engine_ledger", body(json!({"params": {"zzz": 1}}))),
        (
            "engine_ledger",
            body(json!({"params": {"zzz": 1, "pipe_ref": "a"}})),
        ),
        (
            "engine_ledger",
            body(json!({"params": {"pipe_ref": "_zzz"}})),
        ),
        (
            "engine_ledger",
            body(json!({"params": {"pipe_ref": "_None"}})),
        ),
        (
            "engine_ledger",
            body(json!({"params": {"pipe_ref": "None"}})),
        ),
        ("engine_ledger", body(json!({"params": {"pipe_ref": null}}))),
        ("engine_ledger", body(json!({"params": {"pipe_ref": []}}))),
        (
            "engine_ledger",
            body(json!({"params": {"pipe_ref": ["a", "_b"]}})),
        ),
        (
            "engine_ledger",
            body(json!({"params": {"pipe_ref": {"x": 1}}})),
        ),
        (
            "engine_ledger",
            body(json!({"end": "2999-01-01T00:00:00+00:00"})),
        ),
        (
            "engine_cursors",
            body(json!({"end": "2000-01-01T00:00:00+00:00"})),
        ),
        ("engine_cursors", body(json!({"params": {"zzz": "a"}}))),
        (
            "engine_state",
            body(json!({"end": "2999-01-01T00:00:00+00:00"})),
        ),
        ("engine_state", body(json!({"params": {"updated": null}}))),
    ];
    for (metric, b) in refused {
        let shown = format!("{metric} {b:?}");
        let r = send(
            &app,
            req(
                "POST",
                &format!("/engine/pipes/{metric}/clear"),
                Some(&admin),
                b,
            ),
        )
        .await;
        assert_eq!(r.status, StatusCode::BAD_REQUEST, "{shown}: {}", r.text());
    }
    assert_eq!(count("engine_ledger").await, 2);
    assert_eq!(count("engine_cursors").await, 1);
    assert_eq!(count("engine_state").await, 1);

    // Bounded by declared columns, it works; `end` alone is retention on runs/nodes/events.
    let r = send(
        &app,
        req(
            "POST",
            clear,
            Some(&admin),
            body(json!({"params": {"pipe_ref": "a"}})),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let r = send(
        &app,
        req(
            "POST",
            clear,
            Some(&admin),
            body(json!({"params": {"pipe_ref": ["b"], "seq": [1, 2]}})),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    assert_eq!(count("engine_ledger").await, 0);
    let r = send(
        &app,
        req(
            "POST",
            "/engine/pipes/engine_runs/write",
            Some(&admin),
            body(json!([{"run_id": "r1", "started": "2026-01-01T00:00:00+00:00"}])),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let r = send(
        &app,
        req(
            "POST",
            "/engine/pipes/engine_runs/clear",
            Some(&admin),
            body(json!({"end": "2026-02-01T00:00:00+00:00"})),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    assert_eq!(count("engine_runs").await, 0);
    assert_eq!(count("engine_cursors").await, 1);
    assert_eq!(count("engine_state").await, 1);

    // Authorization comes before the body; a body over the cap is a 413.
    let huge = vec![b' '; meerschaum_server::routes::engine::ENGINE_BODY_LIMIT + 1];
    let r = send(
        &app,
        req(
            "POST",
            write,
            None,
            Some(("application/json", huge.clone())),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let r = send(
        &app,
        req(
            "POST",
            write,
            Some(&admin),
            Some(("application/json", huge)),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::PAYLOAD_TOO_LARGE, "{}", r.text());
}
