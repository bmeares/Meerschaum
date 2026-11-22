//! Sync status generations and metric reads: read scope, private rows filtered before every
//! aggregate, fresh scope and privacy changes, telemetry claims, connector team policy and Valkey
//! metadata.

use super::*;

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
