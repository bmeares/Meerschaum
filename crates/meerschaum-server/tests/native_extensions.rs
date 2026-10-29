//! Native plugin routes and MCP entries through the server, with in-test implementations of
//! `NativeRouteHandler` and `NativeMcp`. The CLI's implementations over a real packaged plugin
//! are covered end to end by `meerschaum-cli/tests/native_api_mcp.rs` (this crate does not load
//! plugins, so its tests drive the two traits directly).

mod common;

use std::sync::Arc;

use async_trait::async_trait;
use axum::http::StatusCode;
use common::{config, login, req, send, state_with};
use meerschaum_core::{Map, Value};
use meerschaum_server::mcp::registry::{Prompt, Resource, Source, Tool};
use meerschaum_server::mcp::{
    Caller, McpServer, NativeMcp, NativeResource, NativeResourceContents,
};
use meerschaum_server::native::{
    NativeAdmission, NativeRoute, NativeRouteHandler, NativeRouteRequest, NativeRouteResponse,
    native_router,
};
use serde_json::json;

struct Echo;

#[async_trait]
impl NativeRouteHandler for Echo {
    async fn handle(
        &self,
        route: &NativeRoute,
        request: NativeRouteRequest,
    ) -> Result<NativeRouteResponse, String> {
        if route.name == "fail" {
            return Err("synthetic failure".into());
        }
        let body = json!({
            "route": route.name,
            "method": request.method,
            "path_params": request.path_params,
            "query_params": request.query_params,
            "headers": request.headers,
            "body": request.body,
            "principal": request.principal,
            "scopes": request.scopes,
        });
        Ok(NativeRouteResponse {
            status: 299,
            headers: vec![
                ("content-type".into(), "application/json".into()),
                ("x-a".into(), "1".into()),
                ("x-a".into(), "2".into()),
            ],
            body: serde_json::to_vec(&body).unwrap(),
        })
    }
}

fn route(name: &str, method: &str, path: &str, scopes: &[&str], public: bool) -> NativeRoute {
    NativeRoute {
        plugin: "demo".into(),
        name: name.into(),
        method: method.into(),
        path: path.into(),
        scopes: scopes.iter().map(|s| s.to_string()).collect(),
        public,
    }
}

#[tokio::test]
async fn routes_carry_the_full_request_and_response() {
    let root = tempfile::tempdir().unwrap();
    let (state, instance) = state_with(config(root.path()));
    instance.seed_user("reader", "pass", None);
    let routes = vec![
        route("item", "get", "/demo/items/{id}", &["pipes:read"], false),
        route("put", "PUT", "/demo/items/{id}", &["pipes:write"], false),
        route("files", "*", "/demo/files/{*rest}", &[], false),
        route("open", "GET", "/demo/open", &[], true),
        route("fail", "GET", "/demo/fail", &[], true),
        // One mount, told apart by suffix and converter (Starlette syntax).
        route("doc_json", "GET", "/demo/docs/{name}.json", &[], true),
        route("doc_num", "GET", "/demo/docs/{n:int}", &[], true),
        route("tail", "GET", "/demo/tail/{p:path}", &[], true),
        // Refused: shadowing, the same path under another name, a duplicate method, bad syntax,
        // the console's pages, a public route with scopes.
        route("shadow", "GET", "/pipes/demo", &[], true),
        route("conflict", "GET", "/demo/items/{other}", &[], true),
        route("dup", "GET", "/demo/items/{id}", &[], true),
        route("first", "GET", "/{x}/y", &[], true),
        route("method", "NOT A METHOD", "/demo/m", &[], true),
        route("pages", "GET", "/_pages/{*rest}", &[], true),
        route("public_scoped", "GET", "/demo/ps", &["pipes:read"], true),
    ];
    let (native, notices) = native_router(&state, routes, Arc::new(Echo));
    assert_eq!(notices.len(), 7, "{notices:#?}");
    assert!(
        notices[0]
            .contains("GET /pipes/demo is refused: it would shadow the core route group 'pipes'")
    );
    assert!(notices[1].contains("already serves GET /demo/items/{id}"));
    assert!(notices[2].contains("already serves GET /demo/items/{id}"));
    assert!(notices[3].contains("first segment"));
    assert!(notices[4].contains("not an HTTP method"));
    assert!(notices[5].contains("'/_pages'"));
    assert!(notices[6].contains("cannot require scopes"));
    let app = meerschaum_server::app(state.clone()).merge(native);

    // Credentials are required, then the route's scopes.
    let r = send(&app, req("GET", "/demo/items/1", None, None)).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    let reader = login(&app, "reader", "pass", Some("pipes:read")).await;
    let r = send(
        &app,
        req(
            "PUT",
            "/demo/items/1",
            Some(&reader),
            Some(("text/plain", b"x".to_vec())),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::FORBIDDEN, "{}", r.text());
    assert!(r.text().contains("pipes:write"));

    let mut request = req(
        "GET",
        "/demo/items/a%20b?q=1&q=2&x=y",
        Some(&reader),
        Some(("application/octet-stream", vec![0, 255])),
    );
    request
        .headers_mut()
        .insert("x-custom", "v".parse().unwrap());
    let r = send(&app, request).await;
    assert_eq!(r.status.as_u16(), 299, "{}", r.text());
    let all: Vec<_> = r.headers.get_all("x-a").iter().collect();
    assert_eq!(all.len(), 2);
    let body = r.json();
    assert_eq!(body["route"], "item");
    assert_eq!(body["path_params"], json!({"id": "a b"}));
    assert_eq!(body["query_params"], json!({"q": ["1", "2"], "x": ["y"]}));
    assert_eq!(body["headers"]["x-custom"], json!(["v"]));
    assert!(body["headers"].get("authorization").is_none());
    assert_eq!(body["body"], json!([0, 255]));
    assert_eq!(body["principal"]["kind"], "user");
    assert_eq!(body["principal"]["username"], "reader");
    assert_eq!(body["scopes"], json!(["pipes:read"]));

    // `*` takes any method, including extension methods; catch-alls keep slashes.
    let admin = login(&app, "admin", "pass", None).await;
    let r = send(
        &app,
        req("PROPFIND", "/demo/files/a/b/c", Some(&admin), None),
    )
    .await;
    assert_eq!(r.status.as_u16(), 299);
    assert_eq!(r.json()["path_params"], json!({"rest": "a/b/c"}));
    assert_eq!(r.json()["method"], "PROPFIND");

    // A method no route declares is a 405 naming the allowed ones; HEAD falls back to GET.
    let r = send(&app, req("DELETE", "/demo/items/1", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(r.headers["allow"], "GET, PUT");
    let r = send(&app, req("HEAD", "/demo/items/1", Some(&admin), None)).await;
    assert_eq!(r.status.as_u16(), 299);
    assert!(r.body.is_empty());

    // Public routes need nothing; valid credentials still identify the caller.
    let r = send(&app, req("GET", "/demo/open", None, None)).await;
    assert_eq!(r.json()["principal"], json!(null));
    let r = send(&app, req("GET", "/demo/open", Some(&admin), None)).await;
    assert_eq!(r.json()["principal"]["username"], "admin");
    let r = send(&app, req("GET", "/demo/fail", None, None)).await;
    assert_eq!(r.status, StatusCode::INTERNAL_SERVER_ERROR);
    assert_eq!(r.json()["detail"], "synthetic failure");

    // The refused routes are not served by the plugin.
    let r = send(&app, req("GET", "/demo/m", Some(&admin), None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);

    // Suffixes and converters pick the route; a path no route takes is a 404.
    let r = send(&app, req("GET", "/demo/docs/a.b.json", None, None)).await;
    assert_eq!(
        (r.json()["route"].clone(), r.json()["path_params"].clone()),
        (json!("doc_json"), json!({"name": "a.b"}))
    );
    let r = send(&app, req("GET", "/demo/docs/42", None, None)).await;
    assert_eq!(r.json()["route"], "doc_num");
    let r = send(&app, req("GET", "/demo/docs/x", None, None)).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = send(&app, req("GET", "/demo/tail/a/b", None, None)).await;
    assert_eq!(r.json()["path_params"], json!({"p": "a/b"}));

    // Cookies reach the plugin, except the console's session cookie.
    let mut request = req("GET", "/demo/open", None, None);
    request.headers_mut().insert(
        "cookie",
        "theme=dark; mrsm-session-id=67e55044-10b1-426f-9247-bb680e5fe0c8"
            .parse()
            .unwrap(),
    );
    let r = send(&app, request).await;
    assert_eq!(r.json()["headers"]["cookie"], json!(["theme=dark"]));

    // An API key (a token principal) authenticates like a login, with the token's scopes.
    let r = send(
        &app,
        req(
            "POST",
            "/tokens/register",
            Some(&admin),
            Some((
                "application/json",
                br#"{"label": "native", "scopes": ["pipes:read"]}"#.to_vec(),
            )),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    let key = format!("Bearer {}", r.json()["api_key"].as_str().unwrap());
    let r = send(&app, req("GET", "/demo/items/9", Some(&key), None)).await;
    assert_eq!(r.status.as_u16(), 299, "{}", r.text());
    let body = r.json();
    assert_eq!(body["principal"]["kind"], "token");
    assert_eq!(body["principal"]["label"], "native");
    assert!(body["principal"]["token_id"].is_string());
    assert_eq!(body["scopes"], json!(["pipes:read"]));
    let r = send(&app, req("PUT", "/demo/items/9", Some(&key), None)).await;
    assert_eq!(r.status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn no_auth_opens_every_route_with_every_scope() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    cfg.no_auth = true;
    let (state, _) = state_with(cfg);
    let (native, notices) = native_router(
        &state,
        vec![route("item", "GET", "/demo/{id}", &["pipes:write"], false)],
        Arc::new(Echo),
    );
    assert!(notices.is_empty());
    let app = meerschaum_server::app(state).merge(native);
    let r = send(&app, req("GET", "/demo/1", None, None)).await;
    assert_eq!(r.status.as_u16(), 299);
    assert_eq!(r.json()["scopes"], json!(["*"]));
}

/// Admits one request at a time and refuses the rest with `503`.
struct OneAtATime {
    slot: Arc<tokio::sync::Semaphore>,
}

#[async_trait]
impl NativeRouteHandler for OneAtATime {
    async fn admit(&self, _route: &NativeRoute) -> Result<NativeAdmission, NativeRouteResponse> {
        match self.slot.clone().try_acquire_owned() {
            Ok(permit) => Ok(NativeAdmission::new(permit)),
            Err(_) => Err(NativeRouteResponse {
                status: 503,
                headers: vec![("retry-after".into(), "1".into())],
                body: b"busy".to_vec(),
            }),
        }
    }

    async fn handle(
        &self,
        _route: &NativeRoute,
        request: NativeRouteRequest,
    ) -> Result<NativeRouteResponse, String> {
        assert!(request.admission.is_held());
        assert_eq!(
            self.slot.available_permits(),
            0,
            "the slot is held while handling"
        );
        Ok(NativeRouteResponse {
            status: 200,
            headers: vec![],
            body: request.body,
        })
    }
}

#[tokio::test]
async fn admission_is_decided_before_the_body_is_read() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let slot = Arc::new(tokio::sync::Semaphore::new(1));
    let (native, notices) = native_router(
        &state,
        vec![route("up", "POST", "/demo/up", &[], true)],
        Arc::new(OneAtATime { slot: slot.clone() }),
    );
    assert!(notices.is_empty());
    let app = meerschaum_server::app(state).merge(native);

    // Admitted: the body is read and handled, and the slot is released afterwards.
    let r = send(
        &app,
        req(
            "POST",
            "/demo/up",
            None,
            Some(("text/plain", b"hi".to_vec())),
        ),
    )
    .await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(&r.body[..], b"hi");
    assert_eq!(slot.available_permits(), 1);

    // Busy: refused without polling the body at all.
    let held = slot.clone().try_acquire_owned().unwrap();
    let polled = Arc::new(AtomicBool::new(false));
    let flag = polled.clone();
    let stream = futures_util::stream::poll_fn(move |_| {
        flag.store(true, Ordering::SeqCst);
        std::task::Poll::Ready(Some(Ok::<_, std::io::Error>(bytes::Bytes::from_static(
            b"x",
        ))))
    });
    let request = axum::http::Request::builder()
        .method("POST")
        .uri("/demo/up")
        .body(axum::body::Body::from_stream(stream))
        .unwrap();
    let r = send(&app, request).await;
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(r.headers["retry-after"], "1");
    assert!(
        !polled.load(Ordering::SeqCst),
        "a refused request's body was read"
    );
    drop(held);
}

struct Entries;

fn core(v: serde_json::Value) -> Value {
    serde_json::from_value(v).unwrap()
}

#[async_trait]
impl NativeMcp for Entries {
    fn tools(&self) -> Vec<Tool> {
        [
            json!({"name": "native_add", "description": "Add.", "read_only": true,
                   "input_schema": {"type": "object", "required": ["a"]}}),
            json!({"name": "native_write", "description": "Write.", "scopes": ["native:write"]}),
            // A built-in name is never shadowed.
            json!({"name": "list_pipes", "description": "Shadow."}),
        ]
        .into_iter()
        .filter_map(|t| Tool::from_value(&core(t), Source::Native))
        .collect()
    }
    fn resources(&self) -> Vec<Resource> {
        [
            json!({"name": "native_item", "description": "Item.", "uri_template": "native://items/{id}"}),
            json!({"name": "native_blob", "description": "Blob.", "uri": "native://blob",
                   "scopes": ["native:blob"]}),
        ]
        .into_iter()
        .filter_map(|r| Resource::from_value(&core(r), Source::Native))
        .collect()
    }
    fn prompts(&self) -> Vec<Prompt> {
        Prompt::from_value(
            &core(json!({"name": "native_greet", "description": "Greet.",
                         "arguments": [{"name": "who", "required": true}]})),
            Source::Native,
        )
        .into_iter()
        .collect()
    }
    async fn call_tool(&self, name: &str, args: &Map, caller: &Caller) -> Result<Value, String> {
        match name {
            "native_add" => Ok(core(
                json!({"sum": args.get("a").and_then(Value::as_i64).unwrap_or(0) + 1,
                                           "scopes": caller.scope_list()}),
            )),
            "native_write" => Ok(Value::Str("plain text".into())),
            _ => Err("no such tool".into()),
        }
    }
    async fn read_resource(
        &self,
        name: &str,
        _uri: &str,
        vars: &Map,
        _caller: &Caller,
    ) -> Result<NativeResource, String> {
        Ok(match name {
            "native_item" => NativeResource {
                contents: NativeResourceContents::Text(format!(
                    "item {}",
                    vars.get("id").map(Value::py_str).unwrap_or_default()
                )),
                mime_type: Some("text/plain".into()),
            },
            _ => NativeResource {
                contents: NativeResourceContents::Blob("AAE=".into()),
                mime_type: None,
            },
        })
    }
    async fn get_prompt(
        &self,
        _name: &str,
        args: &Map,
        _caller: &Caller,
    ) -> Result<(Option<String>, Vec<Value>), String> {
        let who = args.get("who").map(Value::py_str).unwrap_or_default();
        Ok((
            None,
            vec![core(
                json!({"role": "user", "content": {"type": "text", "text": format!("hi {who}")}}),
            )],
        ))
    }
}

async fn rpc(
    app: &axum::Router,
    auth: &str,
    method: &str,
    params: serde_json::Value,
) -> serde_json::Value {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    let mut request = req(
        "POST",
        "/mcp",
        Some(auth),
        Some(("application/json", serde_json::to_vec(&body).unwrap())),
    );
    request.headers_mut().insert(
        "accept",
        "application/json, text/event-stream".parse().unwrap(),
    );
    let r = send(app, request).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.text());
    r.json()
}

#[tokio::test]
async fn mcp_lists_and_calls_native_entries_with_scope_checks() {
    let root = tempfile::tempdir().unwrap();
    let (state, _) = state_with(config(root.path()));
    let mcp = Arc::new(McpServer::for_state(&state).with_native(Some(Arc::new(Entries))));
    let app = meerschaum_server::app_with_mcp(state.clone(), mcp);
    let admin = login(&app, "admin", "pass", None).await;
    let reader = login(&app, "admin", "pass", Some("pipes:read")).await;

    let names = |v: &serde_json::Value, key: &str| -> Vec<String> {
        v["result"][key]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect()
    };
    let tools = rpc(&app, &admin, "tools/list", json!({})).await;
    let listed = names(&tools, "tools");
    assert!(listed.contains(&"native_add".into()) && listed.contains(&"native_write".into()));
    let shadow = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == "list_pipes")
        .unwrap();
    assert_ne!(shadow["description"], "Shadow.");
    let limited = names(&rpc(&app, &reader, "tools/list", json!({})).await, "tools");
    assert!(limited.contains(&"native_add".into()));
    assert!(!limited.contains(&"native_write".into()));

    let called = rpc(
        &app,
        &reader,
        "tools/call",
        json!({"name": "native_add", "arguments": {"a": 2}}),
    )
    .await;
    assert_eq!(called["result"]["isError"], false);
    let text: serde_json::Value =
        serde_json::from_str(called["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(text, json!({"sum": 3, "scopes": ["pipes:read"]}));
    let missing = rpc(
        &app,
        &reader,
        "tools/call",
        json!({"name": "native_add", "arguments": {}}),
    )
    .await;
    assert_eq!(missing["result"]["isError"], true);
    let denied = rpc(&app, &reader, "tools/call", json!({"name": "native_write"})).await;
    assert!(
        denied["result"]["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("native:write")
    );
    let plain = rpc(&app, &admin, "tools/call", json!({"name": "native_write"})).await;
    assert_eq!(plain["result"]["content"][0]["text"], "plain text");

    let templates = rpc(&app, &reader, "resources/templates/list", json!({})).await;
    assert!(names(&templates, "resourceTemplates").contains(&"native_item".into()));
    let statics = rpc(&app, &reader, "resources/list", json!({})).await;
    assert!(!names(&statics, "resources").contains(&"native_blob".into()));
    let item = rpc(
        &app,
        &reader,
        "resources/read",
        json!({"uri": "native://items/7"}),
    )
    .await;
    assert_eq!(item["result"]["contents"][0]["text"], "item 7");
    assert_eq!(item["result"]["contents"][0]["mimeType"], "text/plain");
    let blob = rpc(
        &app,
        &admin,
        "resources/read",
        json!({"uri": "native://blob"}),
    )
    .await;
    assert_eq!(blob["result"]["contents"][0]["blob"], "AAE=");
    assert_eq!(
        blob["result"]["contents"][0]["mimeType"],
        "application/json"
    );

    let prompts = rpc(&app, &reader, "prompts/list", json!({})).await;
    assert!(names(&prompts, "prompts").contains(&"native_greet".into()));
    let got = rpc(
        &app,
        &reader,
        "prompts/get",
        json!({"name": "native_greet", "arguments": {"who": "Ada"}}),
    )
    .await;
    assert_eq!(got["result"]["description"], "Greet.");
    assert_eq!(got["result"]["messages"][0]["content"]["text"], "hi Ada");
    let bad = rpc(
        &app,
        &reader,
        "prompts/get",
        json!({"name": "native_greet"}),
    )
    .await;
    assert!(bad["error"]["message"].as_str().unwrap().contains("who"));
}

#[tokio::test]
async fn read_only_servers_hide_native_tools_that_write() {
    let root = tempfile::tempdir().unwrap();
    let mut cfg = config(root.path());
    cfg.mcp.read_only = true;
    let (state, _) = state_with(cfg);
    let mcp = Arc::new(McpServer::for_state(&state).with_native(Some(Arc::new(Entries))));
    let app = meerschaum_server::app_with_mcp(state, mcp);
    let admin = login(&app, "admin", "pass", None).await;
    let tools = rpc(&app, &admin, "tools/list", json!({})).await;
    let listed: Vec<&str> = tools["result"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .map(|t| t["name"].as_str().unwrap())
        .collect();
    assert!(listed.contains(&"native_add") && !listed.contains(&"native_write"));
}
