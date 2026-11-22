//! Derived objects: saved Pipelines, engine runs, events and sync statuses keep the access
//! boundary.

use super::*;

#[tokio::test]
async fn pipelines_count_hidden_members_and_runs_need_edit_rights() {
    let f = fixture().await;
    let mut registration = Pipe::builder("mrsm", "pipeline")
        .location(Some("daily"))
        .instance(INSTANCE)
        .build()
        .unwrap();
    registration.set_parameters(map(json!({"pipeline": {
        "name": "daily", "id": "id-daily", "revision": 1,
        "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)],
    }})));
    assert!(f.instance.register_pipe(&registration).await.success);
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let r = call(&f, "POST", &admin, "/engine/runs", Some(json!([
        {"run_id": "daily-review", "started": "2026-10-27T00:00:00Z", "status": "success",
         "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)], "message": "pressure details",
         "selection": {"pipeline_definitions": [{"name": "daily", "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)]}]}}
    ]))).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = get(&f, &ed, "/pipelines/daily").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert_eq!(r.json()["hidden_members"], json!(1));
    assert_eq!(r.json()["members"].as_array().unwrap().len(), 1);
    assert!(message(&r).contains("1 hidden pipe"), "{}", r.text());
    assert!(!r.text().contains("pressure"), "{}", r.text());
    // Generic pipe metadata cannot return the Pipeline's unfiltered definition.
    for uri in [
        "/pipes",
        "/pipes/mrsm",
        "/pipes/mrsm/pipeline",
        "/pipes/mrsm/pipeline/daily",
    ] {
        let r = get(&f, &ed, uri).await;
        assert!(!r.text().contains("pressure"), "{uri}: {}", r.text());
    }
    let r = get(&f, &admin, "/pipelines/daily").await;
    assert_eq!(r.json()["hidden_members"], json!(0));
    let r = call(&f, "POST", &ed, "/pipelines/daily/sync", None).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(
        message(&r).contains("you may not change 1 pipe"),
        "{}",
        r.text()
    );
}

#[tokio::test]
async fn runs_and_sync_statuses_show_only_what_the_caller_can_see() {
    let f = fixture().await;
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let started = "2026-10-27T00:00:00Z";
    let temperature = captured_registration(&f.instance, TEMPERATURE).await;
    let pressure = captured_registration(&f.instance, PRESSURE).await;
    let runs = json!([
        {"run_id": "both", "pipe_ref": null, "started": started, "status": "success", "num_nodes": 2,
         "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)]},
        {"run_id": "both", "pipe_ref": pipe_ref(TEMPERATURE), "registration_id": temperature, "started": started, "status": "success"},
        {"run_id": "both", "pipe_ref": pipe_ref(PRESSURE), "started": started, "status": "success",
         "registration_id": pressure, "message": "synced plugin:ops pressure"},
        {"run_id": "ops", "pipe_ref": null, "started": started, "status": "success", "num_nodes": 1,
         "targets": [pipe_ref(PRESSURE)]},
        {"run_id": "ops", "pipe_ref": pipe_ref(PRESSURE), "registration_id": pressure, "started": started, "status": "success"},
    ]);
    let r = call(&f, "POST", &admin, "/engine/runs", Some(runs)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let run_ids = |r: &Resp| -> BTreeSet<String> {
        r.json()["runs"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|run| run["pipe_ref"].is_null())
            .map(|run| run["run_id"].as_str().unwrap().to_string())
            .collect()
    };
    let r = get(&f, &admin, "/engine/runs").await;
    assert_eq!(run_ids(&r), ["both".to_string(), "ops".to_string()].into());
    let r = get(&f, &ed, "/engine/runs").await;
    assert_eq!(run_ids(&r), ["both".to_string()].into(), "{}", r.text());
    assert!(!r.text().contains("pressure"), "{}", r.text());
    let r = get(&f, &ed, "/engine/runs/both").await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(!r.text().contains("pressure"), "{}", r.text());
    assert!(r.text().contains("\"hidden\""), "{}", r.text());
    let r = get(&f, &ed, "/engine/runs/ops").await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    // Engine state: readable for visible pipes, writable for pipes the caller may change.
    let r = get(
        &f,
        &ed,
        &format!(
            "/engine/state?pipe_ref={}",
            pipe_ref(PRESSURE).replace('|', "%7C")
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::NOT_FOUND, "{}", r.text());
    let lease = json!({"pipe_ref": pipe_ref(PUBLIC), "holder": "ed", "ttl_ms": 1000});
    let r = call(&f, "POST", &ed, "/engine/leases", Some(lease)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn projected_engine_rows_events_and_run_selectors_keep_the_access_boundary() {
    use meerschaum_connector_sql::SqlConnector;
    use meerschaum_server::engine_store::InstanceEngine;
    let mut f = fixture().await;
    let store = Arc::new(
        SqlConnector::from_uri(
            "access_review",
            &format!("sqlite:///{}", f._dir.path().join("engine.db").display()),
        )
        .await
        .unwrap(),
    );
    f.state = AppState::builder((*f.state.config).clone(), f.instance.clone())
        .secret(SECRET.to_vec())
        .engine(Arc::new(InstanceEngine::new(store, INSTANCE)))
        .build()
        .unwrap();
    f.app = meerschaum_server::app(f.state.clone());
    let (ed, admin) = (token(&f, "ed").await, token(&f, "admin").await);
    let started = "2026-10-27T00:00:00Z";
    let temperature = captured_registration(&f.instance, TEMPERATURE).await;
    let pressure = captured_registration(&f.instance, PRESSURE).await;
    let records = json!([
        {"run_id": "review-both", "started": started, "status": "success", "num_nodes": 2, "targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)], "message": "hidden pressure details"},
        {"run_id": "review-both", "pipe_ref": pipe_ref(TEMPERATURE), "registration_id": temperature, "started": started, "status": "success", "message": "visible result"},
        {"run_id": "review-both", "pipe_ref": pipe_ref(PRESSURE), "registration_id": pressure, "started": started, "status": "success", "message": "hidden pressure result"},
        {"run_id": "review-ops", "started": started, "status": "success", "num_nodes": 1, "targets": [pipe_ref(PRESSURE)]},
        {"run_id": "review-ops", "pipe_ref": pipe_ref(PRESSURE), "registration_id": pressure, "started": started, "status": "success", "message": "hidden pressure run"},
    ]);
    let r = call(&f, "POST", &admin, "/engine/runs", Some(records)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        "/engine/pipes/engine_nodes/read",
        Some(json!({"select_columns": ["message"]})),
    )
    .await;
    assert_eq!(
        r.json()["rows"],
        json!([{"message": "visible result"}]),
        "{}",
        r.text()
    );
    let events = json!([
        {"run_id": "review-both", "created": started, "event": "run_started", "payload": {"targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)]}},
        {"run_id": "review-both", "created": started, "event": "run_finished", "payload": {"message": "hidden pressure details", "success": true}},
        {"run_id": "review-ops", "created": started, "event": "run_started", "payload": {"targets": [pipe_ref(PRESSURE)]}},
    ]);
    let r = call(&f, "POST", &admin, "/engine/events", Some(events)).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        "/engine/pipes/engine_events/read",
        Some(json!({"select_columns": ["run_id", "payload"]})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    assert!(
        !r.text().contains("pressure") && !r.text().contains("review-ops"),
        "{}",
        r.text()
    );
    let r = get(&f, &ed, "/engine/runs/review-both").await;
    assert!(!r.text().contains("pressure"), "{}", r.text());
    for run_id in ["review-both", "review-ops"] {
        let r = call(
            &f,
            "POST",
            &ed,
            "/engine/pipes/engine_nodes/clear",
            Some(json!({"params": {"run_id": run_id}})),
        )
        .await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
        let r = call(&f, "POST", &ed, "/engine/runs", Some(json!([
            {"run_id": run_id, "started": started, "status": "success", "targets": [pipe_ref(TEMPERATURE)]}
        ]))).await;
        assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    }
    let r = call(&f, "POST", &ed, "/engine/runs", Some(json!([
        {"run_id": "forged", "started": started, "status": "success", "targets": [pipe_ref(PRESSURE)]}
    ]))).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}

#[tokio::test]
async fn run_started_events_do_not_name_hidden_targets() {
    use meerschaum_connector_api::models::EngineRunEvent;
    use meerschaum_server::access::{self, RefFilter};
    let f = fixture().await;
    let auth = meerschaum_server::auth::Principal::User(User::new("ed", None));
    let caller = access::access_of_fresh(&f.state, auth, &Scopes::List(vec!["*".into()]))
        .await
        .unwrap();
    let mut filter = RefFilter::new(&f.state, &caller, INSTANCE);
    let event = EngineRunEvent::from_row(
        "both",
        1,
        chrono::Utc::now(),
        None,
        "run_started",
        json!({"targets": [pipe_ref(TEMPERATURE), pipe_ref(PRESSURE)]}),
    );
    let event = access::runs::visible_event(&mut filter, event).await;
    let json = serde_json::to_value(event).unwrap();
    assert_eq!(json["targets"], json!([pipe_ref(TEMPERATURE)]));
    assert_eq!(json["hidden_targets"], 1);
    assert!(!json.to_string().contains("pressure"));
}

#[tokio::test]
async fn engine_refs_from_a_clients_api_label_use_the_servers_access() {
    let f = fixture().await;
    let ed = token(&f, "ed").await;
    let client_ref = |keys: (&str, &str)| format!("api:remote|{}|{}|None", keys.0, keys.1);
    let r = call(
        &f,
        "POST",
        &ed,
        "/engine/leases",
        Some(json!({"pipe_ref": client_ref(TEMPERATURE), "holder": "client", "ttl_ms": 1000})),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let r = call(
        &f,
        "POST",
        &ed,
        "/engine/leases",
        Some(json!({"pipe_ref": client_ref(PRESSURE), "holder": "client", "ttl_ms": 1000})),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
}
