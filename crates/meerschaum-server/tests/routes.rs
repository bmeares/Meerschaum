//! Route-level tests beyond the recorded contract: JWT compatibility with 4.1, scopes and API
//! keys, the shared pipe cache, the allowlist, rate limits, the engine routes, the jobs and
//! actions backends (including the logs websocket), static-route quirks, and that every
//! documented operation is routed.

mod common;

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::http::StatusCode;
use chrono::{DateTime, Utc};
use common::{SECRET, login, req, send, state_with};
use futures_util::{SinkExt, StreamExt};
use indexmap::IndexMap;
use meerschaum_core::{Map, SuccessTuple, Value};
use meerschaum_server::auth::{jwt_decode, jwt_encode};
use meerschaum_server::backends::{CreateJob, JobsBackend, LogEvent, LogMonitor};
use meerschaum_server::{AppState, ServerConfig};
use serde_json::{Value as Json, json};
use tokio::sync::mpsc;

const CONTRACT: &str = include_str!("fixtures/contract.json");

fn app_with(cfg: ServerConfig) -> axum::Router {
    meerschaum_server::app(state_with(cfg).0)
}

fn default_app() -> (axum::Router, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    (app_with(common::config(dir.path())), dir)
}

#[test]
fn jwts_match_pyjwt_and_jose_byte_for_byte() {
    let fixture: Json = serde_json::from_str(CONTRACT).unwrap();
    for v in fixture["jwt_vectors"].as_array().unwrap() {
        let secret = v["secret"].as_str().unwrap().as_bytes();
        let claims: Map = serde_json::from_value(v["claims"].clone()).unwrap();
        let ours = jwt_encode(&claims, secret);
        assert_eq!(ours, v["pyjwt"].as_str().unwrap());
        assert_eq!(ours, v["jose"].as_str().unwrap());
        let payload = jwt_decode(v["pyjwt"].as_str().unwrap(), secret, Utc::now()).unwrap();
        assert_eq!(Json::Object(payload), v["claims"]);
    }
}

#[tokio::test]
async fn tokens_issued_by_python_authenticate() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::config(dir.path());
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("fixture_admin", "x", Some("admin"));
    let fixture: Json = serde_json::from_str(CONTRACT).unwrap();
    let v = &fixture["jwt_vectors"][0];
    let state = AppState::builder(cfg, instance)
        .secret(v["secret"].as_str().unwrap().as_bytes().to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let auth = format!("Bearer {}", v["pyjwt"].as_str().unwrap());
    let r = send(&app, req("GET", "/users/me", Some(&auth), None)).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.json()["username"], "fixture_admin");
}

#[tokio::test]
async fn secret_key_file_is_shared_with_python() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = common::config(dir.path());
    let path = cfg.secret_key_path.clone();
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "abcdef0123456789").unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let state = AppState::builder(cfg, instance).build().unwrap();
    assert_eq!(state.secret(), b"abcdef0123456789");
}

#[tokio::test]
async fn scopes_and_api_keys() {
    let (app, _dir) = default_app();
    let admin = login(&app, "admin", "pass", None).await;
    // A token with only `pipes:read`.
    let r = send(
        &app,
        req(
            "POST",
            "/tokens/register",
            Some(&admin),
            Some((
                "application/json",
                br#"{"label": "reader", "scopes": ["pipes:read"]}"#.to_vec(),
            )),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let j = r.json();
    assert!(
        j["expiration"].is_null(),
        "API tokens without expiration never expire"
    );
    let key = format!("Bearer {}", j["api_key"].as_str().unwrap());
    let id = j["id"].as_str().unwrap().to_string();
    let secret = j["secret"].as_str().unwrap().to_string();

    let r = send(&app, req("GET", "/pipes/keys", Some(&key), None)).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = send(&app, req("POST", "/pipes/a/b/c/register", Some(&key), None)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(r.json()["detail"], "Missing required scope: 'pipes:write'");
    let r = send(&app, req("POST", "/tokens/validate", Some(&key), None)).await;
    assert_eq!(r.json(), json!([true, "Success"]));

    // Client credentials through the `Authorization` header (`mrsm-key:` prefix allowed).
    let basic = format!(
        "Basic mrsm-key:{}",
        base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!("{id}:{secret}")
        )
    );
    let r = send(&app, req("POST", "/login", Some(&basic), None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let jwt = r.json()["access_token"].as_str().unwrap().to_string();
    let claims = jwt_decode(&jwt, SECRET, Utc::now()).unwrap();
    assert_eq!(claims["sub"], json!(id));
    assert_eq!(claims["scopes"], json!(["pipes:read"]));
    assert!(claims["type"].is_null());
    let r = send(
        &app,
        req("GET", "/pipes/keys", Some(&format!("Bearer {jwt}")), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);

    // Invalidated keys stop working on the next request (API keys hit the instance each time).
    let r = send(
        &app,
        req(
            "POST",
            &format!("/tokens/{id}/invalidate"),
            Some(&admin),
            None,
        ),
    )
    .await;
    assert_eq!(r.json(), json!([true, "Success"]));
    let r = send(&app, req("GET", "/pipes/keys", Some(&key), None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert_eq!(r.json()["detail"], "Invalid or revoked token.");
    let r = send(&app, req("POST", "/login", Some(&basic), None)).await;
    assert_eq!(
        r.status,
        StatusCode::UNAUTHORIZED,
        "revoked keys cannot mint JWTs"
    );
    let r = send(
        &app,
        req("GET", "/pipes/keys", Some(&format!("Bearer {jwt}")), None),
    )
    .await;
    assert_eq!(
        r.status,
        StatusCode::UNAUTHORIZED,
        "revocation also ends existing JWT access"
    );

    let r = send(
        &app,
        req("GET", &format!("/tokens/{id}"), Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert!(
        r.text().contains("\"is_valid\": false"),
        "json.dumps style: {}",
        r.text()
    );
}

#[tokio::test]
async fn expired_api_tokens_cannot_log_in_or_use_existing_jwts() {
    let (app, _dir) = default_app();
    let admin = login(&app, "admin", "pass", None).await;
    let (key, token) = register_token_with(&app, &admin, "expiring", r#"["pipes:read"]"#).await;
    let r = send(&app, req("POST", "/login", Some(&key), None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let jwt = format!("Bearer {}", r.json()["access_token"].as_str().unwrap());
    let id = token["id"].as_str().unwrap();
    let model = send(
        &app,
        req("GET", &format!("/tokens/{id}"), Some(&admin), None),
    )
    .await
    .json();
    // Move the backing token's expiry into the past after issuing a JWT. This exercises
    // expiry independently of the JWT's own exp and without relying on wall-clock sleeps.
    let r = send(
        &app,
        req(
            "POST",
            &format!("/tokens/{id}/edit"),
            Some(&admin),
            Some((
                "application/json",
                json!({
                "creation": model["creation"],
                    "expiration": "2000-01-01T00:00:00Z",
                    "label": "expiring",
                })
                .to_string()
                .into_bytes(),
            )),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()[0], true, "{}", r.text());
    let r = send(&app, req("GET", "/pipes/keys", Some(&jwt), None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let r = send(&app, req("POST", "/login", Some(&key), None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "{}", r.text());
}

#[tokio::test]
async fn deleting_a_user_ends_existing_jwt_access_even_after_repeated_requests() {
    let (app, _instance, _dir) = jobs_scope_app();
    let admin = login(&app, "admin", "pass", None).await;
    let eve = login(&app, "eve", "pass", None).await;
    for _ in 0..2 {
        let r = send(&app, req("GET", "/pipes/keys", Some(&eve), None)).await;
        assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    }
    let r = send(&app, req("DELETE", "/users/eve", Some(&admin), None)).await;
    assert_eq!(r.json()[0], true, "{}", r.text());
    let r = send(&app, req("GET", "/pipes/keys", Some(&eve), None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "{}", r.text());
}

#[tokio::test]
async fn wrong_secret_and_expired_jwts_are_rejected() {
    let (app, _dir) = default_app();
    let mut claims = Map::new();
    claims.insert("sub".into(), Value::Str("admin".into()));
    claims.insert("scopes".into(), Value::List(vec![Value::Str("*".into())]));
    claims.insert("exp".into(), Value::Int(4_102_444_800));
    let forged = format!("Bearer {}", jwt_encode(&claims, b"other"));
    let r = send(&app, req("GET", "/pipes/keys", Some(&forged), None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    assert_eq!(r.headers["www-authenticate"], "Bearer");
    claims.insert("exp".into(), Value::Int(1));
    let expired = format!("Bearer {}", jwt_encode(&claims, SECRET));
    let r = send(&app, req("GET", "/pipes/keys", Some(&expired), None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn no_auth_mode() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    cfg.no_auth = true;
    let app = app_with(cfg);
    let r = send(&app, req("GET", "/users/me", None, None)).await;
    assert_eq!(
        r.json(),
        json!({"username": "no_auth", "user_id": null, "user_type": "admin", "attributes": {}})
    );
    let r = send(&app, req("GET", "/pipes/keys", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn private_mode_requires_auth_on_public_routes() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    cfg.private = true;
    let app = app_with(cfg);
    let r = send(&app, req("GET", "/version", None, None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let admin = login(&app, "admin", "pass", None).await;
    let r = send(&app, req("GET", "/version", Some(&admin), None)).await;
    assert_eq!(r.json(), json!(meerschaum_server::VERSION));
    // `/healthcheck` stays public.
    let r = send(&app, req("GET", "/healthcheck", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
}

#[tokio::test]
async fn shared_pipe_cache_and_delete_cache() {
    let dir = tempfile::tempdir().unwrap();
    let (state, _instance) = state_with(common::config(dir.path()));
    let app = meerschaum_server::app(state.clone());
    let admin = login(&app, "admin", "pass", None).await;
    let body = br#"{"columns": {"datetime": "ts"}}"#.to_vec();
    let r = send(
        &app,
        req(
            "POST",
            "/pipes/a/b/c/register",
            Some(&admin),
            Some(("application/json", body)),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let sync = |rows: &str| {
        req(
            "POST",
            "/pipes/a/b/c/data",
            Some(&admin),
            Some(("application/json", rows.as_bytes().to_vec())),
        )
    };
    let r = send(&app, sync(r#"[{"ts": "2024-01-01", "x": 1}]"#)).await;
    assert_eq!(r.json(), json!([true, "Inserted 1, updated 0 rows."]));
    let types = send(
        &app,
        req("GET", "/pipes/a/b/c/columns/types", Some(&admin), None),
    )
    .await;
    assert_eq!(types.json()["x"], "int64");

    // `get_pipe(refresh=False)` hands out the shared handle: its columns-types cache survives
    // a schema change until `DELETE .../cache`.
    let h1 = state
        .get_pipe("a", "b", Some("c"), None, false)
        .await
        .unwrap();
    let h2 = state
        .get_pipe("a", "b", Some("c"), None, false)
        .await
        .unwrap();
    assert!(Arc::ptr_eq(&h1, &h2));
    let throwaway = state
        .get_pipe("a", "b", Some("c"), None, true)
        .await
        .unwrap();
    assert!(!Arc::ptr_eq(&h1, &throwaway));

    let r = send(&app, sync(r#"[{"ts": "2024-01-02", "x": 1, "y": "z"}]"#)).await;
    assert!(r.json()[0].as_bool().unwrap());
    let stale = send(
        &app,
        req("GET", "/pipes/a/b/c/columns/types", Some(&admin), None),
    )
    .await;
    assert!(
        stale.json().get("y").is_none(),
        "cached for 60 s: {}",
        stale.text()
    );
    let r = send(
        &app,
        req("DELETE", "/pipes/a/b/c/cache", Some(&admin), None),
    )
    .await;
    assert_eq!(r.json(), json!([true, "Success"]));
    let fresh = send(
        &app,
        req("GET", "/pipes/a/b/c/columns/types", Some(&admin), None),
    )
    .await;
    assert_eq!(fresh.json()["y"], "string");
}

// v4's `/docs` route is `json.dumps(pipe.get_docs(), default=json_serialize_value)`: the
// instance's own raw documents. A SQLite row keeps its stored datetime text
// (`2024-01-02 03:04:05.000000`, no `T`, no offset), MySQL-family naive datetimes are `...Z`,
// the PostgreSQL family's aware ones `...+00:00`, and a SQL `NULL` (a `NaN` in a float column
// of the frame) is `null`. Verified against the released v4.1.4 on SQLite, PostgreSQL,
// TimescaleDB, MySQL and MariaDB.
#[tokio::test]
async fn pipe_docs_are_the_instances_raw_documents() {
    use meerschaum_kernel::value::{DateTimeValue, Value as KValue};
    let dir = tempfile::tempdir().unwrap();
    let (state, instance) = common::state_with(common::config(dir.path()));
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    let body = br#"{"columns": {"datetime": "ts", "id": "id"}}"#.to_vec();
    send(
        &app,
        req(
            "POST",
            "/pipes/a/b/[None]/register",
            Some(&admin),
            Some(("application/json", body)),
        ),
    )
    .await;
    let r = send(
        &app,
        req(
            "POST",
            "/pipes/a/b/[None]/data",
            Some(&admin),
            Some((
                "text/plain",
                b"ts:'2024-01-01 00:00:00',id:1,v:1.5".to_vec(),
            )),
        ),
    )
    .await;
    assert_eq!(r.json()[0], json!(true), "{}", r.text());
    let day = chrono::NaiveDate::from_ymd_opt(2024, 1, 2).unwrap();
    let stamp = day.and_hms_opt(3, 4, 5).unwrap();
    let row = |ts: KValue, v: KValue| -> indexmap::IndexMap<String, KValue> {
        [("ts".to_string(), ts), ("v".to_string(), v)]
            .into_iter()
            .collect()
    };
    instance.set_raw_docs(vec![
        row(
            KValue::Str("2024-01-02 03:04:05.000000".into()),
            KValue::Float(f64::NAN),
        ),
        row(
            KValue::Datetime(DateTimeValue::naive(stamp)),
            KValue::Float(1.5),
        ),
        row(KValue::Datetime(DateTimeValue::utc(stamp)), KValue::Null),
    ]);
    let r = send(
        &app,
        req("GET", "/pipes/a/b/[None]/docs", Some(&admin), None),
    )
    .await;
    assert_eq!(
        r.text(),
        concat!(
            r#"[{"ts": "2024-01-02 03:04:05.000000", "v": null}, "#,
            r#"{"ts": "2024-01-02T03:04:05Z", "v": 1.5}, "#,
            r#"{"ts": "2024-01-02T03:04:05+00:00", "v": null}]"#
        )
    );
}

#[tokio::test]
async fn data_formats_and_csv() {
    let (app, _dir) = default_app();
    let admin = login(&app, "admin", "pass", None).await;
    let body = br#"{"columns": {"datetime": "ts", "id": "id"}}"#.to_vec();
    send(
        &app,
        req(
            "POST",
            "/pipes/a/b/[None]/register",
            Some(&admin),
            Some(("application/json", body)),
        ),
    )
    .await;
    // The simple-lines text format and the `orient='columns'` JSON the client posts.
    let r = send(
        &app,
        req(
            "POST",
            "/pipes/a/b/[None]/data",
            Some(&admin),
            Some((
                "text/plain",
                b"ts:'2024-01-01 00:00:00',id:1,v:1.5\nts:'2024-01-02 00:00:00',id:2,v:2.5"
                    .to_vec(),
            )),
        ),
    )
    .await;
    assert_eq!(
        r.json(),
        json!([true, "Inserted 2, updated 0 rows."]),
        "{}",
        r.text()
    );
    let r = send(&app, req("POST", "/pipes/a/b/[None]/data", Some(&admin), Some(("application/json", br#"{"ts": {"5": "2024-01-03T00:00:00+00:00"}, "id": {"5": 3}, "v": {"5": 1.25e-20}}"#.to_vec())))).await;
    assert_eq!(r.json(), json!([true, "Inserted 1, updated 0 rows."]));
    let r = send(
        &app,
        req(
            "GET",
            "/pipes/a/b/[None]/data?date_format=epoch&date_unit=s",
            Some(&admin),
            None,
        ),
    )
    .await;
    let rows = r.json();
    assert_eq!(rows[0]["ts"], json!(1_704_067_200));
    let r = send(
        &app,
        req(
            "GET",
            "/pipes/a/b/[None]/data?date_unit=ms",
            Some(&admin),
            None,
        ),
    )
    .await;
    assert_eq!(r.json()[0]["ts"], "2024-01-01T00:00:00.000Z");
    assert!(
        r.text().contains("1.25e-20"),
        "pandas renders tiny doubles with %g: {}",
        r.text()
    );
    let r = send(
        &app,
        req("GET", "/pipes/a/b/[None]/csv", Some(&admin), None),
    )
    .await;
    assert_eq!(r.headers["content-type"], "text/csv; charset=utf-8");
    assert!(
        r.text()
            .starts_with("ts,id,v\n2024-01-01 00:00:00+00:00,1,1.5\n"),
        "{}",
        r.text()
    );
    let r = send(
        &app,
        req(
            "POST",
            "/pipes/a/b/[None]/data?blocking=false",
            Some(&admin),
            Some((
                "application/json",
                br#"[{"ts": "2024-01-04", "id": 4}]"#.to_vec(),
            )),
        ),
    )
    .await;
    assert_eq!(
        r.json(),
        json!([
            true,
            "Spawned asyncronous sync for Pipe('a', 'b', instance='sql:fixture')."
        ])
    );
}

#[tokio::test]
async fn route_allowlist_and_login_always_served() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    cfg.routes_allowlist = vec!["version".into()];
    let app = app_with(cfg);
    let r = send(&app, req("GET", "/version", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = send(&app, req("GET", "/pipes/keys", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    assert!(r.unmatched);
    let _ = login(&app, "admin", "pass", None).await;
    let doc: Json = send(&app, req("GET", "/openapi.json", None, None))
        .await
        .json();
    let paths = doc["paths"].as_object().unwrap();
    assert!(paths.contains_key("/login") && paths.contains_key("/version"));
    assert!(!paths.keys().any(|p| p.starts_with("/pipes")));

    let mut cfg = common::config(dir.path());
    cfg.routes_allowlist = vec![];
    let app = app_with(cfg);
    let r = send(&app, req("GET", "/version", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn rate_limits_per_route_group() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    cfg.rate_limits.insert(
        "login".into(),
        meerschaum_server::config::RateLimit {
            requests: 2,
            per: Duration::from_secs(60),
        },
    );
    let app = app_with(cfg);
    let bad = || {
        req(
            "POST",
            "/login",
            None,
            Some((
                "application/x-www-form-urlencoded",
                b"username=admin&password=no".to_vec(),
            )),
        )
    };
    assert_eq!(send(&app, bad()).await.status, StatusCode::UNAUTHORIZED);
    assert_eq!(send(&app, bad()).await.status, StatusCode::UNAUTHORIZED);
    let r = send(&app, bad()).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS);
    assert!(r.headers.contains_key("retry-after"));
    // Other groups are unaffected.
    assert_eq!(
        send(&app, req("GET", "/version", None, None)).await.status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn engine_routes() {
    let (app, _dir) = default_app();
    let admin = login(&app, "admin", "pass", None).await;
    let reader = login(&app, "admin", "pass", Some("pipes:read")).await;
    let j = |s: &str| Some(("application/json", s.as_bytes().to_vec()));
    let r = send(&app, req("PUT", "/engine/cursors", Some(&reader), j("[]"))).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    let r = send(
        &app,
        req(
            "PUT",
            "/engine/cursors",
            Some(&admin),
            j(r#"[{"parent_ref": "p", "child_ref": "c", "seq": 1}]"#),
        ),
    )
    .await;
    assert_eq!(r.json(), json!([true, "Upserted 1 cursors."]));
    let r = send(&app, req("POST", "/engine/ledger", Some(&admin), j(r#"[{"pipe_ref": "p", "seq": 1, "created": "2024-01-01T00:00:00Z"}, {"pipe_ref": "p", "seq": 2, "created": "2024-01-01T00:00:00Z", "begin": "2024-01-01"}]"#))).await;
    assert!(r.json()[0].as_bool().unwrap());
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
    assert_eq!(entries.as_array().unwrap().len(), 1);
    assert_eq!(entries[0]["seq"], 2);
    let r = send(
        &app,
        req("GET", "/engine/state?pipe_ref=p", Some(&admin), None),
    )
    .await;
    assert!(r.json().is_null());
    send(
        &app,
        req(
            "PUT",
            "/engine/state",
            Some(&admin),
            j(r#"[{"pipe_ref": "p", "definition_hash": "h"}]"#),
        ),
    )
    .await;
    let r = send(
        &app,
        req("GET", "/engine/state?pipe_ref=p", Some(&admin), None),
    )
    .await;
    assert_eq!(r.json()["definition_hash"], "h");
    let lease = r#"{"pipe_ref": "p", "holder": "a", "ttl_ms": 60000}"#;
    let r = send(&app, req("POST", "/engine/leases", Some(&admin), j(lease))).await;
    assert_eq!(r.json(), json!({"success": true}));
    let other = r#"{"pipe_ref": "p", "holder": "b", "ttl_ms": 60000}"#;
    let r = send(&app, req("POST", "/engine/leases", Some(&admin), j(other))).await;
    assert_eq!(r.json(), json!({"success": false}));
    let r = send(
        &app,
        req("DELETE", "/engine/leases", Some(&admin), j(lease)),
    )
    .await;
    assert_eq!(r.json(), json!({"success": true}));
    send(&app, req("POST", "/engine/runs", Some(&admin), j(r#"[{"run_id": "r1", "started": "2024-01-01T00:00:00Z", "status": "success", "inserted": 3}]"#))).await;
    let r = send(
        &app,
        req("GET", "/engine/runs?run_id=r1", Some(&admin), None),
    )
    .await;
    assert_eq!(r.json()[0]["inserted"], 3);
    let r = send(
        &app,
        req(
            "POST",
            "/engine/events",
            Some(&admin),
            j(r#"[{"run_id": "r1", "created": "2024-01-01T00:00:00Z", "event": "run_started"}]"#),
        ),
    )
    .await;
    assert!(r.json()[0].as_bool().unwrap());
}

/// A jobs backend with one job whose logs are scripted.
#[derive(Default)]
struct FakeJobs;

#[async_trait]
impl JobsBackend for FakeJobs {
    async fn list(&self) -> IndexMap<String, Json> {
        let mut m = IndexMap::new();
        m.insert("j".into(), json!({"sysargs": ["show", "version"]}));
        m
    }
    async fn get(&self, name: &str) -> Option<Json> {
        (name == "j").then(|| json!({"sysargs": ["show", "version"]}))
    }
    async fn exists(&self, name: &str) -> bool {
        name == "j"
    }
    async fn create(&self, _: &str, sysargs: Vec<String>, _: Option<Map>) -> CreateJob {
        CreateJob::Started(SuccessTuple::ok(sysargs.join(" ")))
    }
    async fn delete(&self, _: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn start(&self, _: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn stop(&self, _: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn pause(&self, _: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn logs(&self, _: &str) -> Option<String> {
        Some("log text".into())
    }
    async fn stop_time(&self, _: &str) -> Option<DateTime<Utc>> {
        None
    }
    async fn is_blocking_on_stdin(&self, _: &str) -> bool {
        false
    }
    async fn prompt_kwargs(&self, _: &str) -> Map {
        let mut m = Map::new();
        m.insert("question".into(), Value::Str("?".into()));
        m
    }
    async fn monitor_logs(&self, _: &str) -> Option<LogMonitor> {
        let (tx, events) = mpsc::channel(8);
        let (input, mut input_rx) = mpsc::channel::<String>(8);
        tokio::spawn(async move {
            let _ = tx.send(LogEvent::Line("line 1\n".into())).await;
            let _ = tx.send(LogEvent::StdinRequested).await;
            let answer = input_rx.recv().await.unwrap_or_default();
            let _ = tx.send(LogEvent::Line(format!("got {answer}\n"))).await;
            let _ = tx.send(LogEvent::Stopped(SuccessTuple::success())).await;
        });
        Some(LogMonitor { events, input })
    }
}

#[tokio::test]
async fn jobs_backend_and_logs_websocket() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    let state = AppState::builder(common::config(dir.path()), instance)
        .secret(SECRET.to_vec())
        .jobs(Arc::new(FakeJobs))
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    let r = send(
        &app,
        req(
            "POST",
            "/jobs/new",
            Some(&admin),
            Some((
                "application/json",
                br#"["api", "main", "sync", "pipes"]"#.to_vec(),
            )),
        ),
    )
    .await;
    assert_eq!(r.json(), json!([true, "sync pipes"]), "sysargs are cleaned");
    let r = send(&app, req("GET", "/jobsj/prompt_kwargs", Some(&admin), None)).await;
    assert_eq!(r.json(), json!({"question": "?"}));
    let r = send(&app, req("GET", "/logs/j", Some(&admin), None)).await;
    assert_eq!(r.json(), json!("log text"));

    // The websocket needs a real connection for the upgrade.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/logs/j/ws"))
        .await
        .unwrap();
    use tokio_tungstenite::tungstenite::Message;
    ws.send(Message::Text(admin.trim_start_matches("Bearer ").into()))
        .await
        .unwrap();
    let mut texts = Vec::new();
    while let Some(Ok(msg)) = ws.next().await {
        if let Message::Text(t) = msg {
            let t = t.to_string();
            if t == "MRSM_STDIN" {
                ws.send(Message::Text("yes".into())).await.unwrap();
            }
            texts.push(t);
            if texts.last().is_some_and(|t| t.starts_with('[')) {
                break;
            }
        }
    }
    assert_eq!(
        texts,
        vec![
            "line 1\n",
            "MRSM_STDIN",
            "got yes\n",
            "MRSM_STOP",
            "[true,\"Success\"]"
        ]
    );

    let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/logs/j/ws"))
        .await
        .unwrap();
    ws.send(Message::Text("not a jwt".into())).await.unwrap();
    let first = ws.next().await.unwrap().unwrap();
    assert_eq!(first, Message::Text("Invalid credentials.".into()));
}

#[tokio::test]
async fn logs_websocket_requires_read_scopes_and_separate_execution_scopes_for_input() {
    use tokio_tungstenite::tungstenite::Message;

    let (app, _instance, _dir) = jobs_scope_app();
    let admin = login(&app, "admin", "pass", None).await;
    let reader = login(&app, "eve", "pass", Some("jobs:read logs:read")).await;
    let missing_logs = login(&app, "eve", "pass", Some("jobs:read")).await;
    let missing_jobs = login(&app, "eve", "pass", Some("logs:read")).await;
    let read_key =
        register_token_with(&app, &admin, "logs-reader", r#"["jobs:read", "logs:read"]"#)
            .await
            .0;
    let execute_key = register_token_with(
        &app,
        &admin,
        "logs-operator",
        r#"["jobs:read", "logs:read", "actions:execute", "jobs:execute"]"#,
    )
    .await
    .0;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    for (credentials, expected) in [
        (&missing_logs, "Missing required scope: 'logs:read'"),
        (&missing_jobs, "Missing required scope: 'jobs:read'"),
    ] {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/logs/j/ws"))
            .await
            .unwrap();
        ws.send(Message::Text(
            credentials.trim_start_matches("Bearer ").into(),
        ))
        .await
        .unwrap();
        let first = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(first, Message::Text(expected.into()));
        assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
    }

    // Both JWTs and API keys may follow logs with only the read scopes, but cannot answer
    // a prompt. An operator key holding execution scopes may still interact normally.
    for (credentials, may_answer) in [(&reader, false), (&read_key, false), (&execute_key, true)] {
        let (mut ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/logs/j/ws"))
            .await
            .unwrap();
        ws.send(Message::Text(
            credentials.trim_start_matches("Bearer ").into(),
        ))
        .await
        .unwrap();
        let next = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(next, Message::Text("line 1\n".into()));
        assert_eq!(
            ws.next().await.unwrap().unwrap(),
            Message::Text("MRSM_STDIN".into())
        );
        ws.send(Message::Text("yes".into())).await.unwrap();
        let next = tokio::time::timeout(Duration::from_secs(5), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let expected = if may_answer {
            "got yes\n"
        } else {
            "Missing required scope: 'actions:execute'"
        };
        assert_eq!(next, Message::Text(expected.into()));
    }
    server.abort();
}

#[tokio::test]
async fn static_routes_shadowing_parameters() {
    let (app, _dir) = default_app();
    let admin = login(&app, "admin", "pass", None).await;
    // Starlette falls through to `/tokens/{token_id}` and `/users/{username}`.
    let r = send(&app, req("GET", "/tokens/register", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST);
    assert_eq!(r.json()["detail"], "Invalid token ID.");
    let r = send(&app, req("DELETE", "/users/me", Some(&admin), None)).await;
    assert_eq!(
        r.json(),
        json!([false, "User 'me' is not registered and cannot be deleted."])
    );
    let r = send(&app, req("PUT", "/version", None, None)).await;
    assert_eq!(r.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(r.json(), json!({"detail": "Method Not Allowed"}));
}

#[tokio::test]
async fn every_documented_operation_is_routed() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    cfg.debug = true;
    let app = app_with(cfg.clone());
    for op in meerschaum_server::openapi::operations() {
        let path = op
            .path
            .replace("{connector_keys}", "c")
            .replace("{metric_key}", "m")
            .replace("{location_key}", "l")
            .replace("{name}", "n")
            .replace("{username}", "u")
            .replace("{token_id}", "t")
            .replace("{action}", "a")
            .replace("{type}", "sql");
        let r = send(&app, req(&op.method.to_uppercase(), &path, None, None)).await;
        assert!(!r.unmatched, "{} {} is not routed", op.method, op.path);
        assert_ne!(
            r.status,
            StatusCode::METHOD_NOT_ALLOWED,
            "{} {}",
            op.method,
            op.path
        );
    }
    let r = send(&app, req("GET", "/id", None, None)).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = send(&app, req("GET", "/docs", None, None)).await;
    assert!(r.text().contains("Meerschaum API - Swagger UI"));
    let r = send(&app, req("GET", "/", None, None)).await;
    assert_eq!(r.status, StatusCode::TEMPORARY_REDIRECT);
    assert_eq!(r.headers["location"], "/dash");
}

/// A server over [`FakeJobs`] (job `j`) whose non-admins may run actions
/// (`system:api:permissions:actions:non_admin`), with an admin and a plain user `eve` whose
/// stored scopes are `["*"]`.
fn jobs_scope_app() -> (axum::Router, Arc<common::MemoryInstance>, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("eve", "pass", None);
    instance.seed_user_scopes("eve", &["*"]);
    let mut cfg = common::config(dir.path());
    cfg.system_actions_non_admin = true;
    let state = AppState::builder(cfg, instance.clone())
        .secret(SECRET.to_vec())
        .jobs(Arc::new(FakeJobs))
        .build()
        .unwrap();
    (meerschaum_server::app(state), instance, dir)
}

/// Register a token with `scopes` (JSON text) as the admin; returns its API key header and the
/// registered response.
async fn register_token_with(
    app: &axum::Router,
    admin: &str,
    label: &str,
    scopes: &str,
) -> (String, Json) {
    let body = format!(r#"{{"label": "{label}", "scopes": {scopes}}}"#);
    let r = send(
        app,
        req(
            "POST",
            "/tokens/register",
            Some(admin),
            Some(("application/json", body.into_bytes())),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let j = r.json();
    (format!("Bearer {}", j["api_key"].as_str().unwrap()), j)
}

#[tokio::test]
async fn stopping_a_job_needs_jobs_stop_and_still_accepts_the_legacy_spelling() {
    let (app, _instance, _dir) = jobs_scope_app();
    let admin = login(&app, "admin", "pass", None).await;
    let stop = |key: &str| req("POST", "/jobs/j/stop", Some(key), None);

    // `jobs:execute` + `jobs:stop` (plus `actions:execute`, which the non-admin actions gate
    // asks of every non-admin).
    let (key, _) = register_token_with(
        &app,
        &admin,
        "stopper",
        r#"["actions:execute", "jobs:execute", "jobs:stop"]"#,
    )
    .await;
    let r = send(&app, stop(&key)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()[0], json!(true));

    // Without `jobs:stop`: 403 naming the real scope (v4's text, with the typo fixed).
    let (key, _) = register_token_with(
        &app,
        &admin,
        "runner",
        r#"["actions:execute", "jobs:execute"]"#,
    )
    .await;
    let r = send(&app, stop(&key)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(r.json()["detail"], "Missing required scope: 'jobs:stop'");

    // A token that explicitly holds the legacy `josb:stop` still works.
    let (key, _) = register_token_with(
        &app,
        &admin,
        "legacy",
        r#"["actions:execute", "jobs:execute", "josb:stop"]"#,
    )
    .await;
    let r = send(&app, stop(&key)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()[0], json!(true));

    // The legacy name does not stand in for `jobs:execute`.
    let (key, _) = register_token_with(
        &app,
        &admin,
        "only-legacy",
        r#"["actions:execute", "josb:stop"]"#,
    )
    .await;
    let r = send(&app, stop(&key)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(r.json()["detail"], "Missing required scope: 'jobs:execute'");
}

#[tokio::test]
async fn a_star_scope_is_stored_literally_and_grants_every_scope() {
    let (app, _instance, _dir) = jobs_scope_app();
    let admin = login(&app, "admin", "pass", None).await;

    // `*` and custom (plugin-defined) scopes are stored exactly as given, never expanded to the
    // built-in list, and nothing rejects a scope name it does not know.
    let (star_key, star) = register_token_with(&app, &admin, "everything", r#"["*"]"#).await;
    let (custom_key, custom) = register_token_with(
        &app,
        &admin,
        "custom",
        r#"["myplugin:read", "myplugin:write", "pipes:read"]"#,
    )
    .await;
    let listed = send(&app, req("GET", "/tokens", Some(&admin), None)).await;
    assert_eq!(listed.status, StatusCode::OK, "{}", listed.text());
    let listed = listed.json();
    let scopes_of = |id: &Json| {
        listed
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["id"] == *id)
            .unwrap_or_else(|| panic!("{id} is not listed in {listed}"))["scopes"]
            .clone()
    };
    assert_eq!(scopes_of(&star["id"]), json!(["*"]));
    assert_eq!(
        scopes_of(&custom["id"]),
        json!(["myplugin:read", "myplugin:write", "pipes:read"])
    );

    // A `*` token passes every scope check, built-in or not; the custom token only its own.
    let register = |key: &str| req("POST", "/pipes/a/b/c/register", Some(key), None);
    let r = send(&app, register(&star_key)).await;
    assert_ne!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    let r = send(&app, register(&custom_key)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
    assert_eq!(r.json()["detail"], "Missing required scope: 'pipes:write'");

    // Client credentials: the JWT carries `*`, or exactly the scopes asked for (a custom one
    // included) when the token holds `*`; a token without `*` is filtered to what it holds.
    let login_with = |id: &Json, secret: &Json, scope: &str| {
        let form = format!(
            "grant_type=client_credentials&client_id={}&client_secret={}&scope={scope}",
            id.as_str().unwrap(),
            secret.as_str().unwrap()
        );
        req(
            "POST",
            "/login",
            None,
            Some(("application/x-www-form-urlencoded", form.into_bytes())),
        )
    };
    let claims = |resp: common::Resp| {
        let jwt = resp.json()["access_token"].as_str().unwrap().to_string();
        Json::Object(jwt_decode(&jwt, SECRET, Utc::now()).unwrap())["scopes"].clone()
    };
    let r = send(&app, login_with(&star["id"], &star["secret"], "")).await;
    assert_eq!(claims(r), json!(["*"]));
    let r = send(
        &app,
        login_with(&star["id"], &star["secret"], "myplugin:admin"),
    )
    .await;
    assert_eq!(claims(r), json!(["myplugin:admin"]));
    let r = send(
        &app,
        login_with(
            &custom["id"],
            &custom["secret"],
            "myplugin:read%20myplugin:admin",
        ),
    )
    .await;
    assert_eq!(claims(r), json!(["myplugin:read"]));

    // A user whose stored scopes are `["*"]` holds the necessary scopes of the actions gate.
    let eve = login(&app, "eve", "pass", None).await;
    let r = send(&app, req("POST", "/jobs/j/start", Some(&eve), None)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()[0], json!(true));
}

#[tokio::test]
async fn a_token_carries_at_most_its_owners_scopes() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("star", "pass", None);
    instance.seed_user_scopes("star", &["*"]);
    instance.seed_user("ron", "pass", None);
    instance.seed_user_scopes("ron", &["pipes:read", "jobs:read", "myplugin:read"]);
    let state = AppState::builder(common::config(dir.path()), instance.clone())
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let register = |auth: &str, scopes: &str| {
        let body = format!(r#"{{"label": "t", "scopes": {scopes}}}"#);
        req(
            "POST",
            "/tokens/register",
            Some(auth),
            Some(("application/json", body.into_bytes())),
        )
    };

    // A restricted user may grant what they hold, custom scopes included.
    let ron = login(&app, "ron", "pass", None).await;
    let r = send(&app, register(&ron, r#"["pipes:read", "myplugin:read"]"#)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let own_id = r.json()["id"].as_str().unwrap().to_string();

    // ... but not a scope they lack, `*`, or a custom scope of their own invention: refused
    // (never trimmed), naming every disallowed scope.
    for (scopes, named) in [
        (r#"["pipes:read", "pipes:write"]"#, vec!["pipes:write"]),
        (r#"["*"]"#, vec!["*"]),
        (
            r#"["jobs:read", "other:read", "pipes:drop"]"#,
            vec!["other:read", "pipes:drop"],
        ),
    ] {
        let r = send(&app, register(&ron, scopes)).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{scopes}: {}", r.text());
        let detail = r.json()["detail"].as_str().unwrap().to_string();
        for scope in named {
            assert!(detail.contains(&format!("'{scope}'")), "{detail}");
        }
        assert!(!detail.contains("'pipes:read'"), "{detail}");
    }

    // No scopes asked for: the owner's own, not the built-in list.
    let r = send(&app, register(&ron, "[]")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let id = r.json()["id"].as_str().unwrap().to_string();
    let listed = send(&app, req("GET", "/tokens", Some(&ron), None))
        .await
        .json();
    let scopes_of = |id: &str| {
        listed
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["id"] == id)
            .unwrap()["scopes"]
            .clone()
    };
    assert_eq!(
        scopes_of(&id),
        json!(["pipes:read", "jobs:read", "myplugin:read"])
    );

    // Editing is capped the same way; scopes the token already holds stay.
    let edit = |auth: &str, id: &str, scopes: &str| {
        let body = format!(
            r#"{{"creation": "2024-01-01T00:00:00Z", "expiration": null, "label": "renamed", "scopes": {scopes}}}"#
        );
        req(
            "POST",
            &format!("/tokens/{id}/edit"),
            Some(auth),
            Some(("application/json", body.into_bytes())),
        )
    };
    let r = send(
        &app,
        edit(&ron, &own_id, r#"["pipes:read", "myplugin:read"]"#),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = send(
        &app,
        edit(&ron, &own_id, r#"["pipes:read", "pipes:write"]"#),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        r.json()["detail"]
            .as_str()
            .unwrap()
            .contains("'pipes:write'")
    );
    let r = send(&app, edit(&ron, &own_id, r#"["*"]"#)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    // Administrators and holders of `*` may grant anything, `*` and custom scopes included.
    for (user, label) in [("admin", "admin"), ("star", "star")] {
        let auth = login(&app, user, "pass", None).await;
        for scopes in [r#"["*"]"#, r#"["brand:new", "pipes:write"]"#] {
            let r = send(&app, register(&auth, scopes)).await;
            assert_eq!(r.status, StatusCode::OK, "{label} {scopes}: {}", r.text());
        }
        let r = send(&app, register(&auth, "[]")).await;
        assert_eq!(r.status, StatusCode::OK, "{label}: {}", r.text());
        let id = r.json()["id"].as_str().unwrap().to_string();
        let r = send(&app, edit(&auth, &id, r#"["*"]"#)).await;
        assert_eq!(r.status, StatusCode::OK, "{label}: {}", r.text());
    }
}

#[tokio::test]
async fn tokens_can_only_be_changed_by_their_owner_or_an_administrator() {
    let dir = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user("admin", "pass", Some("admin"));
    instance.seed_user("ann", "pass", None);
    instance.seed_user("bob", "pass", None);
    let state = AppState::builder(common::config(dir.path()), instance)
        .secret(SECRET.to_vec())
        .build()
        .unwrap();
    let app = meerschaum_server::app(state);
    let admin = login(&app, "admin", "pass", None).await;
    let ann = login(&app, "ann", "pass", None).await;
    let bob = login(&app, "bob", "pass", None).await;

    let register = |auth: &str, label: &str, scopes: &str| {
        let body = format!(r#"{{"label": "{label}", "scopes": {scopes}}}"#);
        req(
            "POST",
            "/tokens/register",
            Some(auth),
            Some(("application/json", body.into_bytes())),
        )
    };
    let admins = send(&app, register(&admin, "admins", r#"["*"]"#))
        .await
        .json()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let anns = send(&app, register(&ann, "anns", r#"["pipes:read"]"#))
        .await
        .json()["id"]
        .as_str()
        .unwrap()
        .to_string();
    let bobs = send(&app, register(&bob, "bobs", r#"["jobs:read"]"#))
        .await
        .json()["id"]
        .as_str()
        .unwrap()
        .to_string();

    let model = |id: &str| {
        let app = app.clone();
        let admin = admin.clone();
        let id = id.to_string();
        async move {
            let r = send(
                &app,
                req("GET", &format!("/tokens/{id}"), Some(&admin), None),
            )
            .await;
            assert_eq!(r.status, StatusCode::OK, "{}", r.text());
            r.json()
        }
    };
    let edit = |auth: &str, id: &str, body: &str| {
        req(
            "POST",
            &format!("/tokens/{id}/edit"),
            Some(auth),
            Some(("application/json", body.as_bytes().to_vec())),
        )
    };
    const BASE: &str = r#""creation": "2024-01-01T00:00:00Z", "expiration": null"#;

    // Another user's token (an ordinary user's or an administrator's `*` token) cannot be
    // relabelled, re-scoped, invalidated or deleted; nothing changes.
    for victim in [&anns, &admins] {
        let relabel = format!(r#"{{{BASE}, "label": "mine now", "scopes": ["jobs:read"]}}"#);
        let r = send(&app, edit(&bob, victim, &relabel)).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        assert_eq!(r.json()["detail"], "Cannot edit another user's token.");
        let r = send(
            &app,
            req(
                "POST",
                &format!("/tokens/{victim}/invalidate"),
                Some(&bob),
                None,
            ),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        let r = send(
            &app,
            req("DELETE", &format!("/tokens/{victim}"), Some(&bob), None),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    }
    let m = model(&admins).await;
    assert_eq!(
        (
            m["label"].clone(),
            m["scopes"].clone(),
            m["is_valid"].clone()
        ),
        (json!("admins"), json!(["*"]), json!(true))
    );
    let m = model(&anns).await;
    assert_eq!(
        (m["label"].clone(), m["scopes"].clone()),
        (json!("anns"), json!(["pipes:read"]))
    );

    // The owner edits their own; leaving `scopes` out keeps the token's scopes (never the
    // owner's, never the built-in list).
    let r = send(
        &app,
        edit(&ann, &anns, &format!(r#"{{{BASE}, "label": "renamed"}}"#)),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let m = model(&anns).await;
    assert_eq!(
        (
            m["label"].clone(),
            m["scopes"].clone(),
            m["is_valid"].clone()
        ),
        (json!("renamed"), json!(["pipes:read"]), json!(true))
    );
    // An administrator edits anyone's, and may widen a token to `*`.
    let r = send(
        &app,
        edit(
            &admin,
            &bobs,
            &format!(r#"{{{BASE}, "label": "bobs", "scopes": ["*"]}}"#),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(model(&bobs).await["scopes"], json!(["*"]));
    // The owner still may not go beyond their own scopes (an ordinary user holds the built-in
    // list: no custom scope, no `*`).
    let r = send(
        &app,
        edit(
            &ann,
            &anns,
            &format!(r#"{{{BASE}, "label": "x", "scopes": ["brand:new"]}}"#),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());

    // Invalidating: the owner or an administrator. Only an administrator makes it valid again.
    let r = send(
        &app,
        req(
            "POST",
            &format!("/tokens/{anns}/invalidate"),
            Some(&ann),
            None,
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(model(&anns).await["is_valid"], json!(false));
    let revalidate = format!(r#"{{{BASE}, "label": "renamed", "is_valid": true}}"#);
    let r = send(&app, edit(&ann, &anns, &revalidate)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        r.json()["detail"]
            .as_str()
            .unwrap()
            .contains("administrator")
    );
    assert_eq!(model(&anns).await["is_valid"], json!(false));
    // An edit that does not mention `is_valid` leaves an invalidated token invalid.
    let r = send(
        &app,
        edit(&ann, &anns, &format!(r#"{{{BASE}, "label": "still off"}}"#)),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(model(&anns).await["is_valid"], json!(false));
    let r = send(&app, edit(&admin, &anns, &revalidate)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(model(&anns).await["is_valid"], json!(true));
    let r = send(
        &app,
        req(
            "POST",
            &format!("/tokens/{bobs}/invalidate"),
            Some(&admin),
            None,
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());

    // Deleting: the owner or an administrator; a missing token is a 404.
    let r = send(
        &app,
        req("DELETE", &format!("/tokens/{anns}"), Some(&ann), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = send(
        &app,
        req("DELETE", &format!("/tokens/{bobs}"), Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = send(
        &app,
        req("DELETE", &format!("/tokens/{bobs}"), Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    let r = send(
        &app,
        req("GET", &format!("/tokens/{anns}"), Some(&admin), None),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
}
