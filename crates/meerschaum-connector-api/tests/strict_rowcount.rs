//! `pipe_has_rows_checked` on the API client (a scripted server): only a successful integer
//! count proves an `api:` pipe empty, so an incremental `copy pipes` never fills a destination
//! whose rows it could not count. The v4-compatible `get_pipe_rowcount` keeps reading a refused
//! or malformed answer as 0.

mod common;

use common::{Canned, MockServer};
use meerschaum_connector::traits::{DataQuery, InstanceConnector};
use meerschaum_connector_api::APIConnector;
use meerschaum_core::{Map, Pipe, Value};

fn answer(status: u16, content_type: &str, body: &str) -> Canned {
    Canned {
        status,
        content_type: content_type.into(),
        body: body.as_bytes().to_vec(),
    }
}

fn json(status: u16, body: &str) -> Canned {
    answer(status, "application/json", body)
}

fn connector(port: u16) -> APIConnector {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    attrs.insert("api_key".into(), Value::Str("k".into()));
    APIConnector::new(Some("strict"), attrs).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_a_counted_answer_proves_an_api_pipe_empty() {
    let server = MockServer::start().await;
    let conn = connector(server.port);
    let pipe = Pipe::builder("plugin:noaa", "weather").build().unwrap();

    // Refused, unsupported, a `detail` body, or not a number.
    let unknown = [
        json(404, r#"{"detail":"Not Found"}"#),
        json(403, r#"{"detail":"Missing scopes."}"#),
        json(200, r#"{"detail":"Pipe is not registered."}"#),
        json(200, r#""many""#),
        json(200, "null"),
        answer(200, "text/plain", "oops"),
    ];
    for canned in unknown {
        let shown = String::from_utf8_lossy(&canned.body).to_string();
        server.push_response(canned.clone());
        let legacy = InstanceConnector::get_pipe_rowcount(&conn, &pipe, &DataQuery::default())
            .await
            .unwrap();
        assert_eq!(legacy, Some(0), "legacy count for {shown}");
        server.push_response(canned);
        let checked = InstanceConnector::pipe_has_rows_checked(&conn, &pipe, None).await;
        assert!(checked.is_err(), "{shown} must not prove the pipe empty");
    }

    // Not a count either, whatever the legacy reading.
    for body in ["-1", "2.5", r#""-4""#] {
        server.push_response(json(200, body));
        let checked = InstanceConnector::pipe_has_rows_checked(&conn, &pipe, None).await;
        assert!(checked.is_err(), "{body} must not prove the pipe empty");
    }

    // A counted answer, with the filters in the body.
    server.push_response(json(200, "0"));
    assert!(
        !InstanceConnector::pipe_has_rows_checked(&conn, &pipe, None)
            .await
            .unwrap()
    );
    let mut params = Map::new();
    params.insert("station".into(), Value::Str("x".into()));
    server.push_response(json(200, "7"));
    assert!(
        InstanceConnector::pipe_has_rows_checked(&conn, &pipe, Some(&params))
            .await
            .unwrap()
    );
    let (requests, pending, _) = server.take();
    assert_eq!(pending, 0);
    let last = requests.last().unwrap();
    assert!(last.target.contains("/rowcount"), "{}", last.target);
    assert!(
        last.target.to_lowercase().contains("remote=false"),
        "{}",
        last.target
    );
    assert_eq!(String::from_utf8_lossy(&last.body), r#"{"station": "x"}"#);
}
