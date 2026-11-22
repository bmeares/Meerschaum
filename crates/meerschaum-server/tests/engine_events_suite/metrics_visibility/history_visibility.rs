//! A saved Pipeline's run history: private metadata is withheld on every history surface,
//! submitted proofs are stripped or validated, and history pages apply visibility after bounds.

use super::*;

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
        assert_eq!(
            response.status,
            StatusCode::NOT_FOUND,
            "{route}: {}",
            response.text()
        );
        let stored = fixture
            .state
            .engine
            .get_runs(&meerschaum_server::backends::RunsFilter {
                run_id: Some(id.into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(stored.is_empty(), "{route} persisted a forged run");
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
