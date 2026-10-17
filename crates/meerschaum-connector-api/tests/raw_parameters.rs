//! Raw-vs-resolved parameters: metadata forwarded to the server keeps `{{ ... }}` template text.

mod common;

use common::{Canned, MockServer};
use meerschaum_connector_api::APIConnector;
use meerschaum_core::{Map, Pipe, Value};
use serde_json::{Value as J, json};

fn pipe() -> Pipe {
    let mut params = Map::new();
    params.insert("target".into(), Value::Str("{{ self.metric }}_tbl".into()));
    params.insert(
        "sql".into(),
        Value::Str("SELECT * FROM {{ Pipe('sql:main', 'other') }}".into()),
    );
    Pipe::builder("sql:main", "weather")
        .parameters(params)
        .build()
        .unwrap()
}

fn connector(port: u16) -> APIConnector {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str("127.0.0.1".into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    attrs.insert("api_key".into(), Value::Str("k".into()));
    APIConnector::new(Some("raw"), attrs).unwrap()
}

fn json_ok(body: J) -> Canned {
    Canned {
        status: 200,
        content_type: "application/json".into(),
        body: body.to_string().into_bytes(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn register_and_edit_forward_raw_templates() {
    let server = MockServer::start().await;
    let conn = connector(server.port);
    let p = pipe();
    // Resolution happens for execution, but persistence must see the template text.
    assert_eq!(p.parameters()["target"], Value::Str("weather_tbl".into()));

    server.push_response(json_ok(json!([true, "ok"])));
    assert!(conn.register_pipe(&p).await.success);

    // `edit_pipe` first loads `/attributes` (the server's stored parameters).
    server.push_response(json_ok(
        json!({"parameters": {"columns": {"datetime": "ts"}}}),
    ));
    server.push_response(json_ok(json!([true, "ok"])));
    assert!(conn.edit_pipe(&p, true).await.success);

    let (recorded, _, _) = server.take();
    let register = recorded
        .iter()
        .find(|r| r.target.contains("/register"))
        .unwrap();
    let body: J = serde_json::from_slice(&register.body).unwrap();
    assert_eq!(body["target"], "{{ self.metric }}_tbl");
    assert_eq!(body["sql"], "SELECT * FROM {{ Pipe('sql:main', 'other') }}");

    let edit = recorded
        .iter()
        .find(|r| r.target.contains("/edit"))
        .unwrap();
    let body: J = serde_json::from_slice(&edit.body).unwrap();
    assert_eq!(body["target"], "{{ self.metric }}_tbl");
    assert_eq!(body["sql"], "SELECT * FROM {{ Pipe('sql:main', 'other') }}");
    assert_eq!(body["columns"]["datetime"], "ts");

    // Execution-side parameters are resolved.
    server.push_response(json_ok(json!({"parameters": {}})));
    let resolved = conn.pipe_resolved_parameters(&p).await;
    assert_eq!(resolved["target"], Value::Str("weather_tbl".into()));
}
