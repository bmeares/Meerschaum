//! Logical identities, rotating credentials and complete canonical-source reads.

use crate::common::{Canned, MockServer};
use meerschaum_connector_api::{APIConnector, Connector, DataQuery};
use meerschaum_core::team_scope::api_execution::{Alias, VERSION};
use meerschaum_core::team_scope::write_private;
use meerschaum_core::{Pipe, Value};

fn alias(origin: &str, credential_file: &std::path::Path, keys: &str) -> APIConnector {
    APIConnector::from_execution_alias(&Alias {
        version: VERSION,
        logical_keys: keys.into(),
        callback_url: origin.into(),
        credential_file: credential_file.into(),
    })
    .unwrap()
}

fn credential(path: &std::path::Path, token: &str) {
    write_private(
        path,
        &serde_json::json!({"token":token,
        "expires_at":chrono::Utc::now().timestamp()+30})
        .to_string(),
    )
    .unwrap();
}

fn response(status: u16, complete: bool) -> Canned {
    Canned {
        status,
        content_type: "application/json".into(),
        body: serde_json::json!({
            "version":1,"dtypes":{"id":"int64","value":"string"},
            "rows":[{"id":1,"value":"safe"}],"complete":complete
        })
        .to_string()
        .into_bytes(),
    }
}

#[tokio::test]
async fn sql_alias_preserves_identity_rotates_auth_and_never_sends_local_definition() {
    let server = MockServer::start().await;
    let root = tempfile::tempdir().unwrap();
    let token_file = root.path().join("credential.json");
    credential(&token_file, "scoped-first");
    let conn = alias(
        &format!("http://127.0.0.1:{}", server.port),
        &token_file,
        "sql:source",
    );
    assert_eq!(conn.connector_type(), "sql");
    assert_eq!(conn.label(), "source");
    assert_eq!(conn.connector_keys(), "sql:source");
    assert_eq!(conn.to_string(), "sql:source");
    assert_eq!(conn.instance_keys().as_deref(), Some("sql:source"));
    let mut pipe = Pipe::builder("sql:source", "metric")
        .instance("sql:target")
        .build()
        .unwrap();
    pipe.set_parameters(Value::from_json_str(r#"{"fetch":{"definition":"SELECT synthetic_forbidden_local_definition"},"password":"synthetic_client_secret"}"#).unwrap().to_map());
    for token in ["scoped-first", "scoped-second"] {
        credential(&token_file, token);
        server.push_response(response(200, true));
        let frame = conn
            .fetch(&pipe, &DataQuery::default())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(frame.height(), 1);
        let (requests, _, _) = server.take();
        assert_eq!(requests.len(), 1);
        assert_eq!(
            requests[0].target,
            "/pipes/sql:source/metric/%5BNone%5D/fetch"
        );
        assert_eq!(
            requests[0].header("authorization"),
            Some(format!("Bearer {token}").as_str())
        );
        let encoded = String::from_utf8(requests[0].body.clone()).unwrap();
        assert!(!encoded.contains("synthetic_forbidden_local_definition"));
        assert!(!encoded.contains("synthetic_client_secret"));
        let request: serde_json::Value = serde_json::from_str(&encoded).unwrap();
        assert_eq!(request["source_keys"], "sql:source");
        assert_eq!(request["instance_keys"], "sql:target");
        assert_eq!(request["row_limit"], 100_000);
        assert!(request["limit"].is_null());
    }
}

#[tokio::test]
async fn incomplete_refused_and_unsupported_sources_never_become_partial_success() {
    let server = MockServer::start().await;
    let root = tempfile::tempdir().unwrap();
    let token_file = root.path().join("credential.json");
    credential(&token_file, "scoped-only");
    let origin = format!("http://127.0.0.1:{}", server.port);
    let conn = alias(&origin, &token_file, "sql:source");
    let pipe = Pipe::builder("sql:source", "metric")
        .instance("sql:target")
        .build()
        .unwrap();
    server.push_response(response(200, false));
    assert!(
        conn.fetch(&pipe, &DataQuery::default())
            .await
            .unwrap_err()
            .to_string()
            .contains("complete")
    );
    server.push_response(response(403, true));
    assert!(conn.fetch(&pipe, &DataQuery::default()).await.is_err());
    let (requests, _, _) = server.take();
    assert_eq!(requests.len(), 2, "no login or retry after a refusal");
    let valkey = alias(&origin, &token_file, "valkey:source");
    assert!(valkey.fetch(&pipe, &DataQuery::default()).await.is_err());
    let (requests, _, _) = server.take();
    assert!(
        requests.is_empty(),
        "unsupported source never reaches a native driver or endpoint"
    );
}

#[test]
fn callback_aliases_reject_authentication_and_routing_overrides() {
    for origin in [
        "https://u:p@main.example",
        "https://main.example/path",
        "https://main.example?token=x",
        "https://main.example/#fragment",
        "http://main.example:0",
        "file:///secret",
    ] {
        assert!(
            APIConnector::from_execution_alias(&Alias {
                version: VERSION,
                logical_keys: "sql:main".into(),
                callback_url: origin.into(),
                credential_file: "/launch/credential.json".into()
            })
            .is_err(),
            "{origin}"
        );
    }
}

#[tokio::test]
async fn logical_alias_sync_sends_special_dtype_hints_as_json_with_typed_payload_values() {
    let server = MockServer::start().await;
    let root = tempfile::tempdir().unwrap();
    let token_file = root.path().join("credential.json");
    credential(&token_file, "scoped-bytes");
    let conn = alias(
        &format!("http://127.0.0.1:{}", server.port),
        &token_file,
        "sql:main",
    );
    let mut pipe = Pipe::builder("manual:posted", "typed")
        .instance("sql:main")
        .build()
        .unwrap();
    // This transport-only case needs no catalog read before sending its data.
    pipe.temporary = true;
    let frame = meerschaum_kernel::docs::frame_from_docs(&[indexmap::IndexMap::from_iter([(
        "payload".into(),
        meerschaum_kernel::value::Value::Bytes(vec![1, 2, 3]),
    )])])
    .unwrap();
    for text in [
        r#"[true,"Inserted 1, updated 0 rows."]"#,
        r#"[true,"Cleared cache"]"#,
    ] {
        server.push_response(Canned {
            status: 200,
            content_type: "application/json".into(),
            body: text.as_bytes().to_vec(),
        });
    }
    let hints = Value::from_json_str(r#"{"payload":"bytes"}"#).unwrap();
    let result = conn
        .sync_pipe_counted(
            &pipe,
            frame,
            &meerschaum_connector_api::SyncOptions {
                chunksize: Some(-1),
                extra: meerschaum_core::Map::from_iter([("new_special_cols".into(), hints)]),
                ..Default::default()
            },
        )
        .await;
    assert!(result.success, "{}", result.message);
    let (requests, _, _) = server.take();
    let request = requests
        .iter()
        .find(|request| request.method == "POST")
        .unwrap();
    let url = reqwest::Url::parse(&format!("http://fixture{}", request.target)).unwrap();
    let encoded = url
        .query_pairs()
        .find(|(key, _)| key == "new_special_cols")
        .unwrap()
        .1
        .into_owned();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
        serde_json::json!({"payload":"bytes"})
    );
    assert!(
        String::from_utf8(request.body.clone())
            .unwrap()
            .contains("AQID")
    );
    assert_eq!(request.header("authorization"), Some("Bearer scoped-bytes"));
}

#[tokio::test]
async fn logical_alias_engine_requests_keep_native_identity_without_api_relabeling() {
    let server = MockServer::start().await;
    let root = tempfile::tempdir().unwrap();
    let token_file = root.path().join("credential.json");
    credential(&token_file, "scoped-engine");
    let conn = alias(
        &format!("http://127.0.0.1:{}", server.port),
        &token_file,
        "sql:source",
    );
    server.push_response(Canned {
        status: 200,
        content_type: "application/json".into(),
        body: br#"[true,"Recorded"]"#.to_vec(),
    });
    assert!(conn.post_engine_runs(&[]).await.success);
    server.push_response(Canned {
        status: 200,
        content_type: "application/json".into(),
        body: br#"{"success":true,"message":"Found runs","runs":[]}"#.to_vec(),
    });
    assert!(conn.get_engine_runs(None, None).await.unwrap().is_empty());
    let (requests, _, _) = server.take();
    assert_eq!(requests.len(), 2);
    for request in requests {
        let url = reqwest::Url::parse(&format!("http://fixture{}", request.target)).unwrap();
        assert_eq!(
            url.query_pairs()
                .filter(|(key, _)| key == "instance_keys")
                .collect::<Vec<_>>(),
            vec![("instance_keys".into(), "sql:source".into())]
        );
        assert!(
            request
                .header(meerschaum_connector_api::engine_pipes::CLIENT_INSTANCE_HEADER)
                .is_none()
        );
        assert_eq!(
            request.header("authorization"),
            Some("Bearer scoped-engine")
        );
    }
}

#[tokio::test]
async fn logical_alias_pipe_requests_pin_the_canonical_instance_and_refuse_read_denials() {
    let server = MockServer::start().await;
    let root = tempfile::tempdir().unwrap();
    let token_file = root.path().join("credential.json");
    credential(&token_file, "scoped-only");
    let conn = alias(
        &format!("http://127.0.0.1:{}", server.port),
        &token_file,
        "sql:source",
    );
    let mut pipe = Pipe::builder("manual:posted", "copied")
        .instance("sql:source")
        .build()
        .unwrap();
    pipe.set_parameters(
        Value::from_json_str(r#"{"instance_keys":"sql:caller_override"}"#)
            .unwrap()
            .to_map(),
    );
    for body in ["42", "true", "[true,\"Cleared\"]"] {
        server.push_response(Canned {
            status: 200,
            content_type: "application/json".into(),
            body: body.as_bytes().to_vec(),
        });
    }
    assert!(conn.get_pipe_id_checked(&pipe).await.unwrap().is_some());
    assert!(conn.pipe_exists_checked(&pipe).await.unwrap());
    assert!(conn.clear_pipe(&pipe, &DataQuery::default()).await.success);
    server.push_response(Canned {
        status: 403,
        content_type: "application/json".into(),
        body: br#"{"detail":"source revoked"}"#.to_vec(),
    });
    assert!(
        conn.get_pipe_data(&pipe, &DataQuery::default())
            .await
            .is_err()
    );
    let (requests, _, _) = server.take();
    assert_eq!(requests.len(), 4);
    for request in requests {
        let url = reqwest::Url::parse(&format!("http://fixture{}", request.target)).unwrap();
        let query: Vec<_> = url.query_pairs().collect();
        assert_eq!(
            query
                .iter()
                .filter(|(key, _)| key == "instance_keys")
                .count(),
            1
        );
        assert!(
            query
                .iter()
                .any(|(key, value)| key == "instance_keys" && value == "sql:source")
        );
        assert!(
            !query
                .iter()
                .any(|(key, value)| key == "instance" || value.contains("caller_override"))
        );
    }
}
