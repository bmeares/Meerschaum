//! Diff the Rust OpenAPI document against the Python v4 server's (`fixtures/openapi.json`, from
//! `gen_openapi.py`): every Python path and method must exist with the same tags, parameters
//! (name, location, requiredness, type) and request bodies, `/mcp` included.

mod common;

use std::collections::BTreeSet;

use serde_json::Value as Json;

const FIXTURE: &str = include_str!("fixtures/openapi.json");

fn schema_type(s: &Json) -> String {
    let mut types = BTreeSet::new();
    if let Some(items) = s.get("anyOf").and_then(Json::as_array) {
        for i in items {
            for t in schema_type(i).split('|') {
                types.insert(t.to_string());
            }
        }
    }
    match s.get("type") {
        Some(Json::String(t)) => {
            types.insert(t.clone());
        }
        Some(Json::Array(ts)) => {
            for t in ts {
                types.insert(t.as_str().unwrap_or("").to_string());
            }
        }
        _ => {}
    }
    if types.is_empty() {
        "any".into()
    } else {
        types.into_iter().collect::<Vec<_>>().join("|")
    }
}

#[test]
fn rust_document_covers_python_document() {
    let python: Json = serde_json::from_str(FIXTURE).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = common::config(dir.path());
    cfg.no_dash = true;
    let rust = serde_json::to_value(meerschaum_server::openapi::document(&cfg)).unwrap();
    let mut problems = Vec::new();
    let mut checked = 0;
    for (path, methods) in python["paths"].as_object().unwrap() {
        for (method, py_op) in methods.as_object().unwrap() {
            checked += 1;
            let Some(op) = rust["paths"].get(path).and_then(|p| p.get(method)) else {
                problems.push(format!("missing {} {path}", method.to_uppercase()));
                continue;
            };
            let tags = op.get("tags").cloned().unwrap_or(Json::Array(vec![]));
            if tags != py_op["tags"] {
                problems.push(format!("{method} {path}: tags {tags} != {}", py_op["tags"]));
            }
            let mut params: Vec<Json> = op
                .get("parameters")
                .and_then(Json::as_array)
                .cloned()
                .unwrap_or_default()
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "name": p["name"],
                        "in": p["in"],
                        "required": p["required"].as_bool().unwrap_or(false),
                        "type": schema_type(&p["schema"]),
                    })
                })
                .collect();
            params.sort_by_key(|p| (p["in"].to_string(), p["name"].to_string()));
            if Json::Array(params.clone()) != py_op["parameters"] {
                problems.push(format!(
                    "{method} {path}: parameters\n   rust: {}\n python: {}",
                    Json::Array(params),
                    py_op["parameters"]
                ));
            }
            let body = op.get("requestBody").map(|b| {
                let mut types: Vec<String> =
                    b["content"].as_object().unwrap().keys().cloned().collect();
                types.sort();
                (types, b["required"].as_bool().unwrap_or(false))
            });
            let py_body = py_op["request_body"].as_array().map(|a| {
                (
                    a.iter()
                        .map(|t| t.as_str().unwrap().to_string())
                        .collect::<Vec<_>>(),
                    py_op["body_required"].as_bool().unwrap_or(false),
                )
            });
            if body != py_body {
                problems.push(format!("{method} {path}: body {body:?} != {py_body:?}"));
            }
        }
    }
    assert_eq!(
        serde_json::to_value(&rust["components"]["securitySchemes"])
            .unwrap()
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>(),
        python["security_schemes"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s.as_str().unwrap().to_string())
            .collect::<BTreeSet<_>>()
    );
    assert_eq!(rust["security"], python["security"]);
    eprintln!("checked {checked} Python operations");
    assert!(problems.is_empty(), "{}", problems.join("\n"));
}

#[test]
fn swagger_page_matches_fastapi() {
    let python: Json = serde_json::from_str(FIXTURE).unwrap();
    assert_eq!(
        meerschaum_server::routes::docs::SWAGGER_HTML,
        python["docs_html"].as_str().unwrap()
    );
}

/// v4.1.4 lists `GET /webterm/{session_id}` (tag `Webterm`, one required string path parameter, no
/// body) when the terminal is served; the fixture is recorded with `--no-webterm`, so this test pins
/// the shape recorded from a v4.1.4 server started without that flag. `/websocket/{session_id}` is a
/// websocket route and is in neither document.
#[test]
fn webterm_is_listed_exactly_when_it_is_served() {
    let dir = tempfile::tempdir().unwrap();
    let doc = |no_dash: bool, no_webterm: bool| {
        let mut cfg = common::config(dir.path());
        cfg.no_dash = no_dash;
        cfg.no_webterm = no_webterm;
        serde_json::to_value(meerschaum_server::openapi::document(&cfg)).unwrap()
    };
    let served = doc(false, false);
    let op = &served["paths"]["/webterm/{session_id}"]["get"];
    assert_eq!(op["tags"], serde_json::json!(["Webterm"]));
    assert_eq!(
        op["parameters"],
        serde_json::json!([{
            "name": "session_id",
            "in": "path",
            "required": true,
            "schema": {"type": "string", "title": "Session Id"},
        }])
    );
    assert!(op.get("requestBody").is_none());
    assert!(served["paths"].get("/websocket/{session_id}").is_none());
    assert!(
        doc(true, false)["paths"]
            .get("/webterm/{session_id}")
            .is_none()
    );
    assert!(
        doc(false, true)["paths"]
            .get("/webterm/{session_id}")
            .is_none()
    );
}
