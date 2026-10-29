//! Narrow Pipeline metadata authorization and route-group boundaries.
mod common;
use axum::http::StatusCode;
use common::{config, req, send, state_with};
use serde_json::json;

#[tokio::test]
async fn metadata_requires_auth_and_obeys_its_own_route_group() {
    let root = tempfile::tempdir().unwrap();
    let (state, _instance) = state_with(config(root.path()));
    let app = meerschaum_server::app(state);
    let body = json!({"pipeline":{"name":"daily", "targets":["sql:a|sql:b|raw|None"]}})
        .to_string()
        .into_bytes();
    let anonymous = send(
        &app,
        req(
            "POST",
            "/pipelines/daily/register",
            None,
            Some(("application/json", body.clone())),
        ),
    )
    .await;
    assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED);
    let admin = common::login(&app, "admin", "pass", None).await;
    assert_eq!(
        send(
            &app,
            req(
                "POST",
                "/pipelines/daily/register",
                Some(&admin),
                Some(("application/json", body))
            )
        )
        .await
        .status,
        StatusCode::OK
    );
    let listing = send(&app, req("GET", "/pipelines", Some(&admin), None))
        .await
        .json();
    assert_eq!(listing["success"], json!(true));
    assert_eq!(listing["pipelines"][0]["name"], "daily");
    assert_eq!(
        listing["pipelines"][0]["targets"],
        json!(["sql:a|sql:b|raw|None"])
    );
    assert!(
        !send(
            &app,
            req("GET", "/pipes/mrsm/pipeline/daily/data", Some(&admin), None)
        )
        .await
        .status
        .is_success()
    );

    let mut cfg = config(root.path());
    cfg.routes_disabled = vec!["pipelines".into()];
    assert_eq!(
        meerschaum_server::routes::route_group_of("/pipelines/daily/attributes"),
        "pipelines"
    );
    assert!(!meerschaum_server::routes::is_proxyable(
        &cfg,
        "/pipelines/daily/attributes"
    ));
    let (state, _) = state_with(cfg);
    let app = meerschaum_server::app(state);
    assert_eq!(
        send(&app, req("GET", "/pipelines", None, None))
            .await
            .status,
        StatusCode::NOT_FOUND
    );
}

#[test]
fn openapi_discovers_pipeline_metadata_and_execution_controls() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    let document = serde_json::to_value(meerschaum_server::openapi::document(&cfg)).unwrap();
    for (path, method) in [
        ("/pipelines", "get"),
        ("/pipelines/keys", "get"),
        ("/pipelines/{name}/register", "post"),
        ("/pipelines/{name}/edit", "patch"),
        ("/pipelines/{name}/delete", "delete"),
        ("/pipelines/{name}/id", "get"),
        ("/pipelines/{name}/attributes", "get"),
        ("/pipelines/leases", "post"),
        ("/pipelines/leases", "delete"),
        ("/pipelines/{name}", "get"),
        ("/pipelines/{name}/plan", "post"),
        ("/pipelines/{name}/sync", "post"),
        ("/engine/runs/{run_id}", "get"),
        ("/execute/validate", "post"),
        ("/execute/runs", "get"),
        ("/execute/runs/{name}/retain", "post"),
    ] {
        assert!(
            document["paths"][path][method].is_object(),
            "{method} {path}"
        );
    }
    for name in ["run_id", "pipeline", "pipe", "status", "since", "limit"] {
        assert!(
            document["paths"]["/engine/runs"]["get"]["parameters"]
                .as_array()
                .unwrap()
                .iter()
                .any(|parameter| parameter["name"] == name),
            "{name}"
        );
    }
    // The v5 routes say what they do and which scopes they need.
    let sync = &document["paths"]["/pipelines/{name}/sync"]["post"];
    assert!(sync["description"].as_str().unwrap().contains("run_id"));
    assert_eq!(
        sync["security"][0]["OAuth2PasswordBearer"],
        json!(["pipes:write", "jobs:execute"])
    );
    assert_eq!(
        document["paths"]["/engine/runs"]["get"]["security"][0]["OAuth2PasswordBearer"],
        json!(["pipes:read"])
    );
    cfg.routes_disabled = vec!["pipelines".into(), "login".into()];
    let document = serde_json::to_value(meerschaum_server::openapi::document(&cfg)).unwrap();
    assert!(document["paths"].get("/pipelines").is_none());
    assert!(document["paths"].get("/login").is_none());
}
