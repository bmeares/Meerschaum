//! A Pipeline's history identity: it cannot be reintroduced, proven by name alone, inherited by
//! a recreated registration or forged aliases, and a restricted writer persists its proof first.

use super::*;

#[tokio::test]
async fn api_pipeline_identities_cannot_be_reintroduced_and_live_rename_preserves_history_and_access()
 {
    let fixture = fixture(false).await;
    let mut editor = User::new("alice", None);
    editor.attributes = Some(map(
        json!({"role":"editor","scopes":["pipes:read","pipes:write","pipes:register","pipes:delete"]}),
    ));
    assert!(fixture.conn.edit_user(&editor).await.success);
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let admin = login(&fixture.app, "admin", "fixture-password", None).await;
    let body = |identity: &str, private: bool| {
        json!({
            "pipeline":{"name":"hardened","id":identity,"registry":"forged-registry","revision":0,"targets":[fixture.public.pipe_ref()]},
            "access":{"visibility":if private {"private"}else{"public"},"edit":"owner"}
        })
    };
    let post = |route: &str, token: &str, value: serde_json::Value| {
        req(
            "POST",
            route,
            Some(token),
            Some(("application/json", value.to_string().into_bytes())),
        )
    };
    let created = send(
        &fixture.app,
        post(
            "/pipelines/hardened/register",
            &admin,
            body("caller-chosen", true),
        ),
    )
    .await;
    assert_eq!(created.status, StatusCode::OK, "{}", created.text());
    let registered = Pipe::builder("mrsm", "pipeline")
        .location(Some("hardened"))
        .instance("sql:metrics")
        .build()
        .unwrap();
    let old = fixture.conn.get_pipe_attributes(&registered).await.unwrap();
    let old_json = serde_json::to_value(&old).unwrap();
    let old_id = old_json["parameters"]["pipeline"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(old_id, "caller-chosen");
    assert_eq!(uuid::Uuid::parse_str(&old_id).unwrap().get_version_num(), 4);
    seed_pipeline_history(
        &fixture,
        "old-hardened-run",
        json!({"name":"hardened","id":old_id,"registry":"sql:metrics"}),
    )
    .await;
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/old-hardened-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            req("DELETE", "/pipelines/hardened/delete", Some(&admin), None)
        )
        .await
        .status,
        StatusCode::OK
    );
    assert_eq!(
        send(
            &fixture.app,
            post("/pipelines/hardened/register", &alice, body(&old_id, false))
        )
        .await
        .status,
        StatusCode::OK
    );
    let current = fixture.conn.get_pipe_attributes(&registered).await.unwrap();
    let current_json = serde_json::to_value(&current).unwrap();
    let identity = current_json["parameters"]["pipeline"]["id"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(
        identity, old_id,
        "a known deleted UUID must not be reusable"
    );
    let mut forged = current_json["parameters"].clone();
    forged["pipeline"]["id"] = json!(old_id);
    let changed = send(
        &fixture.app,
        req(
            "PATCH",
            "/pipelines/hardened/edit",
            Some(&alice),
            Some(("application/json", forged.to_string().into_bytes())),
        ),
    )
    .await;
    assert_eq!(changed.status, StatusCode::CONFLICT, "{}", changed.text());
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/old-hardened-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert!(!generation_rpc(&fixture, &admin, "old-hardened-run").await);
    seed_pipeline_history(
        &fixture,
        "new-hardened-run",
        json!({"name":"hardened","id":identity,"registry":"sql:metrics"}),
    )
    .await;
    assert!(generation_rpc(&fixture, &alice, "new-hardened-run").await);
    let renamed = send(
        &fixture.app,
        post(
            "/pipelines/hardened/rename",
            &alice,
            json!({"new_name":"hardened-renamed","expected_id":identity,"expected_revision":0}),
        ),
    )
    .await;
    assert_eq!(renamed.status, StatusCode::OK, "{}", renamed.text());
    assert_eq!(renamed.json()["pipeline"]["id"], identity);
    let destination = Pipe::builder("mrsm", "pipeline")
        .location(Some("hardened-renamed"))
        .instance("sql:metrics")
        .build()
        .unwrap();
    let moved = fixture
        .conn
        .get_pipe_attributes(&destination)
        .await
        .unwrap();
    assert_eq!(moved.get("pipe_id"), current.get("pipe_id"));
    assert!(
        fixture
            .conn
            .get_pipe_attributes(&registered)
            .await
            .unwrap()
            .is_empty()
    );
    let moved_json = serde_json::to_value(&moved).unwrap();
    assert_eq!(
        moved_json["parameters"]["access"],
        current_json["parameters"]["access"]
    );
    assert_eq!(moved_json["parameters"]["pipeline"]["revision"], 1);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/new-hardened-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::OK,
        "authorized rename retains prior history by stable live UUID"
    );
    assert!(generation_rpc(&fixture, &alice, "new-hardened-run").await);
    let named = send(
        &fixture.app,
        req(
            "GET",
            "/engine/runs?pipeline=hardened-renamed",
            Some(&alice),
            None,
        ),
    )
    .await;
    assert_eq!(
        named.json()["runs"].as_array().unwrap().len(),
        1,
        "{}",
        named.text()
    );
    assert_eq!(named.json()["runs"][0]["run_id"], "new-hardened-run");
    assert_eq!(
        send(
            &fixture.app,
            post(
                "/pipelines/hardened/rename",
                &alice,
                json!({"new_name":"resurrected"})
            )
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            post(
                "/pipelines/hardened-renamed/rename",
                &alice,
                json!({"new_name":"stale","expected_id":old_id})
            )
        )
        .await
        .status,
        StatusCode::CONFLICT
    );
    let read_only = login(
        &fixture.app,
        "alice",
        "fixture-password",
        Some("pipes:read"),
    )
    .await;
    assert_eq!(
        send(
            &fixture.app,
            post(
                "/pipelines/hardened-renamed/rename",
                &read_only,
                json!({"new_name":"denied"})
            )
        )
        .await
        .status,
        StatusCode::FORBIDDEN
    );
    let duplicate = Pipe::builder("mrsm","pipeline").location(Some("duplicate-hidden")).instance("sql:metrics")
        .parameters(map(json!({"pipeline":{"id":identity,"name":"duplicate-hidden"},"access":{"owner":"bob","visibility":"private"}}))).build().unwrap();
    assert!(fixture.conn.register_pipe(&duplicate).await.success);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/new-hardened-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND,
        "hidden duplicate IDs cannot create authority"
    );
    assert!(!generation_rpc(&fixture, &admin, "new-hardened-run").await);
    assert!(fixture.conn.delete_pipe(&duplicate).await.success);
    assert!(generation_rpc(&fixture, &alice, "new-hardened-run").await);
    // A no-op move still returns the caller's member projection, including legacy metadata
    // that contains a member the Pipeline owner cannot currently see.
    let mut hidden_member = destination.clone();
    let mut parameters = moved.get("parameters").unwrap().to_map();
    parameters
        .get_mut("pipeline")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert(
            "targets".into(),
            Value::List(vec![
                Value::Str(fixture.public.pipe_ref()),
                Value::Str(fixture.private.pipe_ref()),
            ]),
        );
    hidden_member.set_parameters(parameters);
    assert!(fixture.conn.edit_pipe(&hidden_member, false).await.success);
    let unchanged = send(
        &fixture.app,
        post(
            "/pipelines/hardened-renamed/rename",
            &alice,
            json!({"new_name":"hardened-renamed"}),
        ),
    )
    .await;
    assert_eq!(unchanged.status, StatusCode::OK, "{}", unchanged.text());
    assert_eq!(
        unchanged.json()["pipeline"]["targets"],
        json!([fixture.public.pipe_ref()])
    );
    assert_eq!(unchanged.json()["pipeline"]["hidden_members"], 1);
    assert!(!unchanged.text().contains(&fixture.private.pipe_ref()));
}

#[tokio::test]
async fn a_name_only_legacy_pipeline_cannot_prove_retained_history_identity() {
    let fixture = fixture(false).await;
    let legacy = Pipe::builder("mrsm", "pipeline")
        .location(Some("legacy-flow"))
        .instance("sql:metrics")
        .parameters(map(json!({"pipeline":{"name":"legacy-flow"}})))
        .build()
        .unwrap();
    assert!(fixture.conn.register_pipe(&legacy).await.success);
    seed_pipeline_history(
        &fixture,
        "legacy-flow-run",
        json!({
            "name":"legacy-flow","id":"legacy:sql:metrics:legacy-flow","registry":"sql:metrics"
        }),
    )
    .await;
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let admin = login(&fixture.app, "admin", "fixture-password", None).await;
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/legacy-flow-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/legacy-flow-run", Some(&admin), None)
        )
        .await
        .status,
        StatusCode::OK
    );
    assert!(!generation_rpc(&fixture, &alice, "legacy-flow-run").await);
    assert!(!generation_rpc(&fixture, &admin, "legacy-flow-run").await);
    assert!(fixture.conn.delete_pipe(&legacy).await.success);
    let mut forged = legacy.clone();
    forged.set_parameters(map(
        json!({"pipeline":{"name":"legacy-flow","id":"legacy:sql:metrics:legacy-flow"}}),
    ));
    assert!(fixture.conn.register_pipe(&forged).await.success);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/legacy-flow-run", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND,
        "persisting a predictable legacy string cannot manufacture a saved UUID"
    );
    assert!(!generation_rpc(&fixture, &alice, "legacy-flow-run").await);
    assert!(!generation_rpc(&fixture, &admin, "legacy-flow-run").await);
}

#[tokio::test]
async fn a_restricted_native_api_writer_persists_queued_proof_before_its_header_and_sse() {
    use meerschaum_connector_api::engine_pipes::{docs_from_records, engine_frame};
    use tower::ServiceExt;
    let fixture = fixture(false).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let app = fixture.app.clone();
    let server = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .unwrap();
    });
    let client = APIConnector::new(
        Some("history_writer"),
        map(json!({
            "host":"127.0.0.1", "port":port, "username":"alice", "password":"fixture-password"
        })),
    )
    .unwrap();
    let pipe = Pipe::builder(fixture.public.connector_keys(), fixture.public.metric_key())
        .instance("api:history_writer")
        .build()
        .unwrap();
    let attributes = InstanceConnector::get_pipe_attributes(&client, &pipe)
        .await
        .unwrap();
    let registration =
        meerschaum_connector::sync_status::registration_from_attributes(&attributes).unwrap();
    let now = chrono::Utc::now();
    let nodes = engine_frame(
        "engine_nodes",
        docs_from_records(&[json!({
            "run_id":"api-queued", "pipe_ref":pipe.pipe_ref(), "registration_id":registration,
            "started":now,"status":"pending","attempts":0
        })]),
    )
    .unwrap();
    let written = client.write_engine_pipe("engine_nodes", &nodes).await;
    assert!(written.success, "{}", written.message);
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/api-queued", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    let runs = engine_frame(
        "engine_runs",
        docs_from_records(&[json!({
            "run_id":"api-queued","started":now,"success":null,
            "targets":[pipe.pipe_ref()],"num_nodes":1
        })]),
    )
    .unwrap();
    let written = client.write_engine_pipe("engine_runs", &runs).await;
    assert!(written.success, "{}", written.message);
    let detail = send(
        &fixture.app,
        req("GET", "/engine/runs/api-queued", Some(&alice), None),
    )
    .await;
    assert_eq!(detail.status, StatusCode::OK, "{}", detail.text());
    assert!(
        !detail.text().contains("registration_id") && !detail.text().contains("registration_ref")
    );
    let stream = fixture
        .app
        .clone()
        .oneshot(req(
            "GET",
            "/engine/runs/api-queued/events/stream?wait=0",
            Some(&alice),
            None,
        ))
        .await
        .unwrap();
    assert_eq!(stream.status(), StatusCode::OK);
    drop(stream);
    let proof = client
        .get_remote_run_generations(&["api-queued".into(), "missing".into()])
        .await
        .unwrap();
    assert_eq!(proof.get("api-queued"), Some(&true));
    assert_eq!(proof.get("missing"), Some(&false));
    let many: Vec<_> = (0..257).map(|index| format!("missing-{index}")).collect();
    let proof = client.get_remote_run_generations(&many).await.unwrap();
    assert_eq!(proof.len(), 257);
    assert!(proof.values().all(|valid| !valid));
    let mut metadata = Pipe::builder("mrsm", "pipeline")
        .location(Some("native-roundtrip"))
        .instance("api:history_writer")
        .parameters(map(json!({"pipeline":{
            "name":"native-roundtrip","id":"native-roundtrip-id","registry":"api:history_writer",
            "targets":[pipe.pipe_ref()]
        }})))
        .build()
        .unwrap();
    let registered = client.register_pipe(&metadata).await;
    assert!(registered.success, "{}", registered.message);
    let server_metadata = Pipe::builder("mrsm", "pipeline")
        .location(Some("native-roundtrip"))
        .instance("sql:metrics")
        .build()
        .unwrap();
    let stored = fixture
        .conn
        .get_pipe_attributes(&server_metadata)
        .await
        .unwrap();
    let stored = serde_json::to_value(stored).unwrap();
    assert_eq!(
        stored["parameters"]["pipeline"]["registry"].as_str(),
        Some("sql:metrics")
    );
    assert_eq!(
        stored["parameters"]["pipeline"]["targets"],
        json!([fixture.public.pipe_ref()])
    );
    let loaded = client.get_pipe_attributes(&metadata).await.unwrap();
    let loaded_json = serde_json::to_value(&loaded).unwrap();
    assert_eq!(
        loaded_json["parameters"]["pipeline"]["registry"].as_str(),
        Some("api:history_writer")
    );
    assert_eq!(
        loaded_json["parameters"]["pipeline"]["targets"],
        json!([pipe.pipe_ref()])
    );
    let mut edited = loaded["parameters"].as_map().unwrap().clone();
    edited
        .get_mut("pipeline")
        .unwrap()
        .as_map_mut()
        .unwrap()
        .insert("description".into(), Value::Str("roundtrip".into()));
    metadata.set_parameters(edited);
    let changed = client.edit_pipe(&metadata, false).await;
    assert!(changed.success, "{}", changed.message);
    for (name, registry, target) in [
        ("hidden-member", "sql:metrics", fixture.private.pipe_ref()),
        (
            "legacy-foreign",
            "api:foreign",
            format!(
                "api:foreign|{}|{}|None",
                pipe.connector_keys(),
                pipe.metric_key()
            ),
        ),
    ] {
        let stored = Pipe::builder("mrsm", "pipeline")
            .location(Some(name))
            .instance("sql:metrics")
            .parameters(map(
                json!({"pipeline":{"name":name,"id":name,"registry":registry,"targets":[target]}}),
            ))
            .build()
            .unwrap();
        assert!(fixture.conn.register_pipe(&stored).await.success);
    }
    let keys = client
        .fetch_pipes_keys(&meerschaum_connector::PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    let meerschaum_connector::PipesKeys::Dict(keys) = keys else {
        panic!("bulk metadata must retain parameters");
    };
    let entry = keys
        .values()
        .find(|entry| entry.location_key.as_deref() == Some("native-roundtrip"))
        .unwrap();
    assert_eq!(
        serde_json::to_value(entry.extra.as_ref().unwrap()).unwrap()["pipeline"]["targets"],
        json!([pipe.pipe_ref()])
    );
    for name in ["hidden-member", "legacy-foreign"] {
        let entry = keys
            .values()
            .find(|entry| entry.location_key.as_deref() == Some(name))
            .unwrap();
        let parameters = serde_json::to_value(entry.extra.as_ref().unwrap()).unwrap();
        assert_eq!(parameters["pipeline"]["targets"], json!([]));
        assert_eq!(parameters["pipeline"]["hidden_members"], 1);
    }
    let legacy_list = send(
        &fixture.app,
        req("GET", "/pipelines/keys", Some(&alice), None),
    )
    .await;
    assert!(
        legacy_list.json()["keys"].is_array(),
        "the default key-list wire shape stays compatible"
    );
    assert!(!legacy_list.text().contains(&fixture.private.pipe_ref()));
    for reference in [
        format!(
            "api:foreign|{}|{}|None",
            pipe.connector_keys(),
            pipe.metric_key()
        ),
        format!(
            "api:history_writer|{}|{}|None",
            fixture.private.connector_keys(),
            fixture.private.metric_key()
        ),
    ] {
        let mut parameters = metadata.raw_parameters().clone();
        parameters
            .get_mut("pipeline")
            .unwrap()
            .as_map_mut()
            .unwrap()
            .insert("targets".into(), Value::List(vec![Value::Str(reference)]));
        let mut bad = metadata.clone();
        bad.set_parameters(parameters);
        assert!(
            !client.edit_pipe(&bad, false).await.success,
            "foreign or private keys cannot be treated as visible local members"
        );
    }
    let canonical = send(
        &fixture.app,
        req("GET", "/pipelines/native-roundtrip", Some(&alice), None),
    )
    .await;
    assert_eq!(canonical.status, StatusCode::OK, "{}", canonical.text());
    assert_eq!(
        canonical.json()["pipeline"]["targets"][0],
        fixture.public.pipe_ref()
    );
    assert_eq!(canonical.json()["members"].as_array().unwrap().len(), 1);
    server.abort();
    // The optional authorization RPC does not give old servers unproven authority.
    let old_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old_port = old_listener.local_addr().unwrap().port();
    let old_server = tokio::spawn(async move {
        axum::serve(old_listener, Router::new()).await.unwrap();
    });
    let old = APIConnector::new(
        Some("old_history"),
        map(json!({"host":"127.0.0.1","port":old_port})),
    )
    .unwrap();
    assert_eq!(
        old.get_remote_run_generations(&["legacy".into()])
            .await
            .unwrap()
            .get("legacy"),
        Some(&false)
    );
    old_server.abort();
}

#[tokio::test]
async fn retained_history_cannot_inherit_recreated_public_registration_or_forged_aliases() {
    let fixture = fixture(false).await;
    let alice = login(&fixture.app, "alice", "fixture-password", None).await;
    let admin = login(&fixture.app, "admin", "fixture-password", None).await;
    let old_id = fixture
        .conn
        .get_pipe_id(&fixture.private)
        .await
        .unwrap()
        .unwrap();
    let old_stamp =
        meerschaum_connector::sync_metrics::observed_registration(&fixture.private).unwrap();
    let seed = |run_id: &str, reference: String, stamp: Option<String>| {
        let at = chrono::Utc::now();
        serde_json::from_value(json!([
            {"run_id":run_id,"started":at,"finished":at,"status":"failed",
             "targets":[reference.clone()],"num_nodes":1,"message":"PRIVATE RUN PASSWORD"},
            {"run_id":run_id,"pipe_ref":reference,"started":at,"finished":at,
             "status":"failed","registration_id":stamp,"message":"PRIVATE NODE PASSWORD"}
        ]))
        .unwrap()
    };
    assert!(
        fixture
            .state
            .engine
            .record_runs(seed(
                "old-private",
                fixture.private.pipe_ref(),
                Some(old_stamp)
            ))
            .await
            .success
    );
    assert!(fixture.state.engine.record_events(serde_json::from_value(json!([
        {"run_id":"old-private","created":chrono::Utc::now(),"event":"run_finished","payload":{"message":"PRIVATE EVENT PASSWORD"}}
    ])).unwrap()).await.success);
    assert_eq!(
        send(
            &fixture.app,
            req("GET", "/engine/runs/old-private", Some(&alice), None)
        )
        .await
        .status,
        StatusCode::NOT_FOUND
    );
    assert!(
        InstanceConnector::delete_pipe(fixture.conn.as_ref(), &fixture.private)
            .await
            .success
    );
    let public = Pipe::builder(
        fixture.private.connector_keys(),
        fixture.private.metric_key(),
    )
    .instance("sql:metrics")
    .parameters(map(json!({"access":{"owner":"alice"}})))
    .build()
    .unwrap();
    assert!(fixture.conn.register_pipe(&public).await.success);
    assert_eq!(
        fixture.conn.get_pipe_id(&public).await.unwrap().unwrap(),
        old_id
    );
    let fresh_stamp = meerschaum_connector::sync_metrics::observed_registration(&public).unwrap();
    // Refresh the ACL index too: old cached privacy must not be what hides retained history.
    fixture.state.access_cache().forget_pipes();
    for uri in [
        "/engine/runs",
        "/engine/runs/old-private",
        "/engine/runs/old-private/events/stream?wait=0",
    ] {
        let response = send(&fixture.app, req("GET", uri, Some(&alice), None)).await;
        assert!(!response.text().contains("PRIVATE"), "{}", response.text());
        if uri == "/engine/runs" {
            assert!(response.json()["runs"].as_array().unwrap().is_empty());
        } else {
            assert_eq!(response.status, StatusCode::NOT_FOUND);
        }
    }
    for metric in ["engine_runs", "engine_nodes", "engine_events"] {
        let response = send(
            &fixture.app,
            req(
                "POST",
                &format!("/engine/pipes/{metric}/read"),
                Some(&alice),
                Some(("application/json", b"{}".to_vec())),
            ),
        )
        .await;
        assert_eq!(response.status, StatusCode::OK, "{}", response.text());
        assert!(
            response.json()["rows"].as_array().unwrap().is_empty(),
            "{}",
            response.text()
        );
    }
    for (tool, args) in [
        ("list_runs", json!({})),
        ("get_run", json!({"run_id":"old-private"})),
    ] {
        let response = history_tool(&fixture, tool, args).await;
        assert!(!response.to_string().contains("PRIVATE"), "{response}");
        if tool == "get_run" {
            assert_eq!(response["result"]["isError"], true);
        }
    }
    let mutation = send(
        &fixture.app,
        req(
            "POST",
            "/engine/events",
            Some(&alice),
            Some((
                "application/json",
                json!([
                    {"run_id":"old-private","created":chrono::Utc::now(),"event":"run_finished"}
                ])
                .to_string()
                .into_bytes(),
            )),
        ),
    )
    .await;
    assert_eq!(
        mutation.status,
        StatusCode::NOT_FOUND,
        "{}",
        mutation.text()
    );
    let trusted = send(
        &fixture.app,
        req("GET", "/engine/runs/old-private", Some(&admin), None),
    )
    .await;
    assert_eq!(trusted.status, StatusCode::OK);
    assert!(trusted.text().contains("PRIVATE"));
    assert!(
        !trusted.text().contains("registration_id") && !trusted.text().contains("registration_ref")
    );
    assert!(
        fixture
            .state
            .engine
            .record_runs(seed("legacy", public.pipe_ref(), None))
            .await
            .success
    );
    assert!(
        fixture
            .state
            .engine
            .record_runs(seed(
                "foreign",
                format!(
                    "api:foreign|{}|{}|None",
                    public.connector_keys(),
                    public.metric_key()
                ),
                Some(fresh_stamp.clone())
            ))
            .await
            .success
    );
    for run_id in ["legacy", "foreign"] {
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
    for (run_id, count) in [("missing-count", None), ("incomplete", Some(2))] {
        let mut records: Vec<meerschaum_connector_api::models::RunRecord> =
            seed(run_id, public.pipe_ref(), Some(fresh_stamp.clone()));
        let header = records
            .iter_mut()
            .find(|row| row.pipe_ref.is_none())
            .unwrap();
        header.extra.shift_remove("num_nodes");
        if let Some(count) = count {
            header.extra.insert("num_nodes".into(), Value::Int(count));
        }
        assert!(fixture.state.engine.record_runs(records).await.success);
        let response = send(
            &fixture.app,
            req("GET", &format!("/engine/runs/{run_id}"), Some(&alice), None),
        )
        .await;
        assert_eq!(
            response.status,
            StatusCode::NOT_FOUND,
            "{}",
            response.text()
        );
    }
    // A real current segment remains readable, and its public result never contains proof.
    assert!(
        fixture
            .state
            .engine
            .record_runs(seed("fresh", public.pipe_ref(), Some(fresh_stamp)))
            .await
            .success
    );
    let fresh = send(
        &fixture.app,
        req("GET", "/engine/runs/fresh", Some(&alice), None),
    )
    .await;
    assert_eq!(fresh.status, StatusCode::OK, "{}", fresh.text());
    assert!(
        !fresh.text().contains("registration_id") && !fresh.text().contains("registration_ref")
    );
    let ids = [
        "fresh",
        "old-private",
        "legacy",
        "foreign",
        "incomplete",
        "missing-count",
        "missing",
    ];
    for token in [&alice, &admin] {
        let proof = send(
            &fixture.app,
            req(
                "POST",
                "/engine/run-generations",
                Some(token),
                Some((
                    "application/json",
                    json!({"run_ids":ids}).to_string().into_bytes(),
                )),
            ),
        )
        .await;
        assert_eq!(proof.status, StatusCode::OK, "{}", proof.text());
        for id in ids {
            assert_eq!(
                proof.json()["generations"][id],
                id == "fresh",
                "{}",
                proof.text()
            );
        }
        assert!(!proof.text().contains("registration_") && !proof.text().contains("PRIVATE"));
    }
    for ids in [json!(vec!["fresh"; 257]), json!([""]), json!(["bad\nrun"])] {
        assert_eq!(
            send(
                &fixture.app,
                req(
                    "POST",
                    "/engine/run-generations",
                    Some(&alice),
                    Some((
                        "application/json",
                        json!({"run_ids":ids}).to_string().into_bytes()
                    ))
                )
            )
            .await
            .status,
            StatusCode::BAD_REQUEST
        );
    }
}
