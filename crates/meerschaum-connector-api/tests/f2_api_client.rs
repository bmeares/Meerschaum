//! rc15 on the API client (a scripted server): `get_rowcount(estimate=True)` sends
//! `estimate=true` and falls back to the exact count on a `4xx` (C1), internal `mrsm` pipes stay
//! out of `fetch_pipes_keys` (B2), and a protected `api:` connector refuses writes before any
//! request (Q20). One test: the protected allowance is process-wide.

mod common;

use common::{Canned, MockServer};
use meerschaum_connector::traits::{Connector, DataQuery, InstanceConnector, PipesKeysQuery};
use meerschaum_connector_api::APIConnector;
use meerschaum_core::{Map, Pipe, Value};

fn json(status: u16, body: &str) -> Canned {
    Canned {
        status,
        content_type: "application/json".into(),
        body: body.as_bytes().to_vec(),
    }
}

fn connector(port: u16, protected: bool) -> APIConnector {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    attrs.insert("api_key".into(), Value::Str("k".into()));
    if protected {
        attrs.insert("protected".into(), Value::Bool(true));
    }
    APIConnector::new(Some("f2"), attrs).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn estimates_hidden_pipes_and_protected_writes() {
    assert!(std::env::var("MRSM_ALLOW_PROTECTED").is_err());
    let server = MockServer::start().await;
    let conn = connector(server.port, false);
    let pipe = Pipe::builder("plugin:noaa", "weather").build().unwrap();

    // The estimate asks with `estimate=true`.
    server.push_response(json(200, "1234567"));
    let n = InstanceConnector::get_pipe_rowcount_estimate(&conn, &pipe, &DataQuery::default())
        .await
        .unwrap();
    assert_eq!(n, Some(1_234_567));
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    assert!(
        requests[0].target.contains("/rowcount"),
        "{}",
        requests[0].target
    );
    assert!(
        requests[0].target.contains("estimate=True"),
        "{}",
        requests[0].target
    );

    // A server that refuses the parameter is asked for the exact count.
    server.push_response(json(422, r#"{"detail": "unknown parameter"}"#));
    server.push_response(json(200, "42"));
    let n = InstanceConnector::get_pipe_rowcount_estimate(&conn, &pipe, &DataQuery::default())
        .await
        .unwrap();
    assert_eq!(n, Some(42));
    let (requests, _, _) = server.take();
    assert_eq!(requests.len(), 2);
    assert!(
        !requests[1].target.contains("estimate="),
        "{}",
        requests[1].target
    );

    // Internal pipes are left out unless named (an older server may still return them).
    let keys = r#"{"1": ["plugin:noaa", "weather", null], "2": ["mrsm", "pipeline", "daily"]}"#;
    server.push_response(json(200, keys));
    let all = conn
        .fetch_pipes_keys(&PipesKeysQuery::default())
        .await
        .unwrap();
    assert_eq!(all.len(), 1);
    server.push_response(json(200, keys));
    let named = conn
        .fetch_pipes_keys(&PipesKeysQuery {
            connector_keys: vec!["plugin:noaa".into(), "mrsm".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(named.len(), 2);
    let _ = server.take();

    // A protected connector refuses before sending anything.
    let prot = connector(server.port, true);
    assert!(Connector::is_protected(&prot));
    for (op, res) in [
        ("register", prot.register_pipe(&pipe).await),
        ("edit", prot.edit_pipe(&pipe, false).await),
        ("delete", prot.delete_pipe(&pipe).await),
        ("drop", prot.drop_pipe(&pipe).await),
        ("clear", prot.clear_pipe(&pipe, &DataQuery::default()).await),
        (
            "sync",
            prot.sync_pipe(
                &pipe,
                meerschaum_kernel::Frame::empty(),
                &Default::default(),
            )
            .await,
        ),
        ("compress", prot.compress_pipe(&pipe).await),
    ] {
        assert!(!res.success, "{op}");
        assert!(
            res.message.starts_with(&format!("Refusing to {op} ")),
            "{}",
            res.message
        );
        assert!(
            res.message.contains("protected instance 'api:f2'"),
            "{}",
            res.message
        );
    }
    let (requests, _, _) = server.take();
    assert!(requests.is_empty(), "{requests:?}");

    // `?protected=true` on a URI (an `MRSM_API_<LABEL>` variable) marks it too, and stays out
    // of the server's address.
    let by_uri = APIConnector::from_uri(
        &format!("http://u:p@127.0.0.1:{}?protected=true", server.port),
        Some("f2uri"),
    )
    .unwrap();
    assert!(Connector::is_protected(&by_uri));
    assert_eq!(by_uri.url, format!("http://127.0.0.1:{}", server.port));
    let res = by_uri.clear_pipe(&pipe, &DataQuery::default()).await;
    assert!(
        res.message.starts_with("Refusing to clear "),
        "{}",
        res.message
    );
    let unmarked = APIConnector::from_uri(
        &format!("http://u:p@127.0.0.1:{}?protected=false", server.port),
        Some("f2uri"),
    )
    .unwrap();
    assert!(!Connector::is_protected(&unmarked));
    let (requests, _, _) = server.take();
    assert!(requests.is_empty(), "{requests:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failed_reads_are_errors_and_empty_answers_stay_empty() {
    // rc15 Q17: a read the server fails (`5xx`) or cannot answer (unreachable) is an error;
    // its `4xx` answers (an unregistered pipe, a pipe without a table) keep the empty results.
    let server = MockServer::start().await;
    let conn = connector(server.port, false);
    let pipe = Pipe::builder("plugin:noaa", "weather").build().unwrap();
    let all = DataQuery::default();

    server.push_response(json(500, r#"{"detail": "Internal Server Error"}"#));
    let err = conn.get_pipe_data(&pipe, &all).await.unwrap_err();
    assert!(
        err.to_string().starts_with(
            "Failed to get data for Pipe('plugin:noaa', 'weather'): the API server answered 500"
        ),
        "{err}"
    );
    server.push_response(json(
        400,
        r#"{"detail": "Could not fetch data with the given parameters."}"#,
    ));
    assert!(conn.get_pipe_data(&pipe, &all).await.unwrap().is_none());
    server.push_response(json(
        409,
        r#"{"detail": "Pipe must be registered with the datetime column specified."}"#,
    ));
    assert!(conn.get_pipe_data(&pipe, &all).await.unwrap().is_none());

    server.push_response(json(503, "{}"));
    assert!(
        InstanceConnector::get_pipe_rowcount(&conn, &pipe, &all)
            .await
            .is_err()
    );
    server.push_response(json(500, "{}"));
    assert!(
        InstanceConnector::get_pipe_rowcount_estimate(&conn, &pipe, &all)
            .await
            .is_err()
    );
    server.push_response(json(404, r#"{"detail": "Not Found"}"#));
    assert_eq!(
        InstanceConnector::get_pipe_rowcount(&conn, &pipe, &all)
            .await
            .unwrap(),
        Some(0)
    );

    server.push_response(json(500, "{}"));
    assert!(conn.get_sync_time(&pipe, None, true).await.is_err());
    server.push_response(json(404, r#"{"detail": "Not Found"}"#));
    assert_eq!(conn.get_sync_time(&pipe, None, true).await.unwrap(), None);

    server.push_response(json(500, "{}"));
    assert!(conn.get_pipe_docs(&pipe, &all).await.is_err());
    let (_, pending, _) = server.take();
    assert_eq!(pending, 0);

    // An unreachable server.
    let port = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap().port()
    };
    let dead = connector(port, false);
    assert!(dead.get_pipe_data(&pipe, &all).await.is_err());
    assert!(dead.get_pipe_docs(&pipe, &all).await.is_err());
}
