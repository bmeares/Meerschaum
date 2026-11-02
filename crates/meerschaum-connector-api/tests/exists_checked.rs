//! `pipe_exists_checked` and `get_pipe_id_checked` on the API client (a scripted server): only
//! the server's `true` or `false` settles whether a source has a table, and only an ID or a `404`
//! whether a destination is registered, so `copy pipes` never reports a source copied without
//! its rows, nor registers over a destination, after a refused or failed check. `pipe_exists`
//! and `get_pipe_id` keep v4's reading.

mod common;

use common::{Canned, MockServer};
use meerschaum_connector_api::APIConnector;
use meerschaum_core::{Map, Pipe, Value};

fn json(status: u16, body: &str) -> Canned {
    Canned {
        status,
        content_type: "application/json".into(),
        body: body.as_bytes().to_vec(),
    }
}

fn connector(port: u16) -> APIConnector {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    attrs.insert("api_key".into(), Value::Str("k".into()));
    APIConnector::new(Some("exists"), attrs).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_a_true_or_false_answer_settles_whether_a_source_exists() {
    let server = MockServer::start().await;
    let conn = connector(server.port);
    let pipe = Pipe::builder("plugin:noaa", "weather").build().unwrap();

    for (canned, want) in [(json(200, "true"), true), (json(200, "false"), false)] {
        server.push_response(canned);
        assert_eq!(conn.pipe_exists_checked(&pipe).await.unwrap(), want);
    }
    let refused = [
        json(500, r#"{"detail":"the database is down"}"#),
        json(403, r#"{"detail":"Missing scopes."}"#),
        json(429, r#"{"detail":"Rate limit exceeded."}"#),
        json(200, r#"{"detail":"Pipe is not registered."}"#),
        json(200, r#""maybe""#),
    ];
    for canned in refused {
        let shown = String::from_utf8_lossy(&canned.body).to_string();
        server.push_response(canned.clone());
        assert!(conn.pipe_exists_checked(&pipe).await.is_err(), "{shown}");
        // v4's `pipe_exists` still answers (`False` for a refusal), unchanged.
        server.push_response(canned);
        assert!(conn.pipe_exists(&pipe).await.is_ok(), "{shown}");
    }
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    assert!(requests.iter().all(|r| r.target.contains("/exists")));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_an_id_or_a_404_settles_whether_a_destination_is_registered() {
    use meerschaum_connector_api::traits::PipeId;
    let server = MockServer::start().await;
    let conn = connector(server.port);
    let pipe = Pipe::builder("plugin:noaa", "weather").build().unwrap();

    server.push_response(json(200, "7"));
    assert!(matches!(
        conn.get_pipe_id_checked(&pipe).await,
        Ok(Some(PipeId::Int(7)))
    ));
    // Unregistered (or hidden from this user, which the server answers alike).
    server.push_response(json(404, r#"{"detail":"Pipe is not registered."}"#));
    assert!(matches!(conn.get_pipe_id_checked(&pipe).await, Ok(None)));
    for canned in [
        json(503, r#"{"detail":"the database is down"}"#),
        json(403, r#"{"detail":"Missing scopes."}"#),
        json(200, r#"{"detail":"unexpected"}"#),
    ] {
        let shown = String::from_utf8_lossy(&canned.body).to_string();
        server.push_response(canned.clone());
        assert!(conn.get_pipe_id_checked(&pipe).await.is_err(), "{shown}");
        // v4's `get_pipe_id` still reads a refusal as unregistered, unchanged.
        server.push_response(canned);
        assert!(matches!(conn.get_pipe_id(&pipe).await, Ok(None)), "{shown}");
    }
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    assert!(requests.iter().all(|r| r.target.contains("/id")));
}
