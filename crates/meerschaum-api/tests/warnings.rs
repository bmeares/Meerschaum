//! The client's `warn()` calls go through the host's sink (`client::warnings`), with v4's text
//! (`connectors/api/_pipes.py`), except for the engine's own `mrsm:` state pipes.
//!
//! One test, because the sink is process-wide.

mod common;

use std::sync::{Arc, Mutex};

use common::{Canned, MockServer};
use meerschaum_api::APIConnector;
use meerschaum_api::client::warnings::set_warning_sink;
use meerschaum_core::{Map, Pipe, Value};

fn canned(status: u16, body: &str) -> Canned {
    Canned {
        status,
        content_type: "text/plain".into(),
        body: body.as_bytes().to_vec(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn client_warnings_reach_the_host_sink_with_v4_text() {
    let server = MockServer::start().await;
    let seen: Arc<Mutex<Vec<String>>> = Arc::default();
    let sink = seen.clone();
    set_warning_sink(Some(Arc::new(move |message: &str| {
        sink.lock().unwrap().push(message.to_string());
    })));

    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(server.port as i64));
    // An API key needs no login request.
    attrs.insert("api_key".into(), Value::Str("k".into()));
    let conn = APIConnector::new(Some("warn"), attrs).unwrap();

    // `get_pipe_size`, `pipe_exists`, `get_sync_time` and `get_pipe_rowcount` warn on a failed
    // response with the response text.
    let pipe = Pipe::builder("demo", "temperature").build().unwrap();
    server.push_response(canned(500, "boom"));
    assert!(!conn.pipe_exists(&pipe).await.unwrap());
    server.push_response(canned(500, "bang"));
    assert_eq!(conn.get_pipe_size(&pipe).await.unwrap(), None);
    // A `detail` in an OK answer is warned as `str(detail)`, without JSON quotes.
    server.push_response(Canned {
        status: 200,
        content_type: "application/json".into(),
        body: br#"{"detail": "no such pipe"}"#.to_vec(),
    });
    assert_eq!(conn.get_pipe_size(&pipe).await.unwrap(), None);

    // The engine's state pipes are refused by the API and probed by the engine: no warning.
    let internal = Pipe::builder("mrsm", "engine_runs").build().unwrap();
    server.push_response(canned(500, "refused"));
    assert!(!conn.pipe_exists(&internal).await.unwrap());

    let messages = seen.lock().unwrap().clone();
    set_warning_sink(None);
    assert_eq!(
        messages,
        vec![
            format!("Failed to check if {pipe} exists:\nboom"),
            format!("Failed to get the size for {pipe}:\nbang"),
            "no such pipe".to_string(),
        ]
    );
}
