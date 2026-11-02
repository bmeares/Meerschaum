//! `get_pipe_data` sends an explicit row `limit` only when one is asked for (every other request
//! is unchanged), and a limit the server refuses (`413`, over its `max_response_row_limit`) is an
//! error, never "no data": `copy pipes` relies on both to read an `api:` source without losing
//! the rows past the server's limit.

mod common;

use common::{Canned, MockServer};
use meerschaum_connector::traits::DataQuery;
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
    APIConnector::new(Some("limit"), attrs).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_explicit_limit_is_sent_and_a_refused_one_is_an_error() {
    let server = MockServer::start().await;
    let conn = connector(server.port);
    let pipe = Pipe::builder("plugin:noaa", "weather").build().unwrap();

    let refused = r#"{"detail":"Requested limit 5 exceeds the maximum response size of 3 rows."}"#;
    server.push_response(json(413, refused));
    let limited = DataQuery {
        limit: Some(5),
        order: Some("desc".into()),
        ..Default::default()
    };
    let answer = conn.get_pipe_data(&pipe, &limited).await;
    let error = answer.expect_err("a refused limit is not an empty pipe");
    assert!(
        error.to_string().contains("maximum response size"),
        "{error}"
    );

    // Without a limit the request is what it always was, and an unregistered pipe has no data.
    server.push_response(json(409, r#"{"detail":"Pipe is not registered."}"#));
    let answer = conn.get_pipe_data(&pipe, &DataQuery::default()).await;
    assert!(matches!(answer, Ok(None)), "{answer:?}");

    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    let targets: Vec<&str> = requests.iter().map(|r| r.target.as_str()).collect();
    assert_eq!(targets.len(), 2, "{targets:?}");
    assert!(
        targets[0].contains("/data?")
            && targets[0].contains("limit=5")
            && targets[0].contains("order=desc"),
        "{targets:?}"
    );
    assert!(!targets[1].contains("limit="), "{targets:?}");
}
