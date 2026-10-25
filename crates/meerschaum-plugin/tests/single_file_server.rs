//! Single-file attributes for the server and job surfaces, at the ABI: `#[api_route]`,
//! `#[mcp_tool]`, `#[mcp_resource]`, `#[mcp_prompt]`, `#[make_executor]` and `fn subscribe`
//! (in a `#[make_connector]` module and at the top level). The generated crate is compiled with
//! the lightweight runtime and called through the same host methods the server, the jobs layer
//! and the engine use. All data is synthetic; every test uses temporary directories.
use meerschaum_core::{Pipe, Value};
use meerschaum_plugin::{
    LoadedPlugin,
    api::RouteRequest,
    discovery,
    executor::{LogEvent, MonitorLogs},
    mcp::{PromptGet, ResourceContents, ResourceRead, ToolCall},
    serde_json::json,
    subscribe::NativeBatch,
};
use std::{sync::Arc, time::Duration};
mod support {
    pub mod compile_single_file;
}

const SOURCE: &str = r##"//! Every server and job surface of a single file.
use std::sync::Mutex;

static JOBS: Mutex<Vec<(String, Vec<String>, String)>> = Mutex::new(Vec::new());
static ACKS: Mutex<Vec<u64>> = Mutex::new(Vec::new());
static STDIN: Mutex<Vec<String>> = Mutex::new(Vec::new());

/// One item, with what the request carried.
#[api_route(GET, "/sf/items/{id}", scopes = ["pipes:read"])]
fn item(request: &RouteRequest) -> Value {
    json!({
        "id": request.path_param("id"), "q": request.query_param("q"),
        "header": request.header("X-Sf"), "theme": request.cookie("theme"),
        "principal": request.principal, "read": request.has_scope("pipes:read"),
    })
}

#[api_route("post", "/sf/echo/{*rest}", public = true, name = "echo")]
fn echo_body(request: &RouteRequest) -> RouteResponse {
    RouteResponse::bytes("application/octet-stream", request.body.clone())
        .with_status(201)
        .header("x-rest", request.path_param("rest").unwrap_or_default())
}

#[api_route("*", "/sf/any")]
fn any(request: &RouteRequest) -> (u16, String) {
    (202, request.method.clone())
}

#[api_route("/sf/fail", public = true)]
fn fail() -> Result<String> {
    Err(Error::Message("synthetic failure".into()))
}

/// Add two integers.
#[mcp_tool(input_schema = r#"{"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}}, "required": ["a", "b"]}"#, read_only = true)]
fn sf_add(arguments: &Value) -> Value {
    json!({"sum": arguments["a"].as_i64().unwrap_or(0) + arguments["b"].as_i64().unwrap_or(0)})
}

#[mcp_tool(description = "Who called.", scopes = ["sf:secret"], title = "Who am I")]
fn sf_whoami(call: &ToolCall) -> Result<Value> {
    Ok(json!({"name": call.name, "principal": call.principal, "scopes": call.scopes}))
}

/// A synthetic item.
#[mcp_resource(uri_template = "sf://items/{id}")]
fn sf_item(resource: &ResourceRead) -> ResourceContents {
    ResourceContents::json(&json!({"id": resource.variable("id"), "uri": resource.uri}))
}

#[mcp_resource(uri = "sf://logo", description = "Three bytes.", mime_type = "application/octet-stream")]
fn sf_logo() -> Result<ResourceContents> {
    Ok(ResourceContents::blob(vec![1u8, 2, 3], "application/octet-stream"))
}

/// Greet someone.
#[mcp_prompt(arguments = ["who"])]
fn sf_greet(prompt: &PromptGet) -> String {
    format!("Say hello to {}.", prompt.argument("who").unwrap_or("nobody"))
}

/// Keeps jobs in memory.
#[make_executor]
mod memory {
    use super::{JOBS, STDIN};

    fn get_job_names() -> Vec<String> {
        JOBS.lock().unwrap().iter().map(|(n, _, _)| n.clone()).collect()
    }

    fn get_job_metadata(name: &str) -> Value {
        let jobs = JOBS.lock().unwrap();
        let Some((_, sysargs, label)) = jobs.iter().find(|(n, _, _)| n == name) else {
            return Value::Null;
        };
        json!({"sysargs": sysargs, "result": [true, label], "restart": false,
            "daemon": {"status": "running", "pid": null,
                "properties": {"process": {"began": "2026-01-01T00:00:00+00:00"}}}})
    }

    fn create_job(executor: &ExecutorInfo, name: &str, sysargs: &[String], properties: Value) -> (bool, String) {
        let note = format!("{} {} {}", executor.keys(), executor.root_dir.as_deref().unwrap_or("-"), properties["tag"]);
        JOBS.lock().unwrap().push((name.into(), sysargs.to_vec(), note.clone()));
        (true, note)
    }

    fn delete_job(name: &str) -> SuccessTuple {
        JOBS.lock().unwrap().retain(|(n, _, _)| n != name);
        SuccessTuple::ok(format!("Deleted {name}."))
    }

    fn write_stdin(name: &str, data: &str) -> Result<()> {
        STDIN.lock().unwrap().push(format!("{name}:{data}"));
        Ok(())
    }

    fn get_logs(name: &str) -> Option<String> {
        Some(STDIN.lock().unwrap().iter().filter(|l| l.starts_with(name)).cloned().collect::<Vec<_>>().join("\n"))
    }

    fn monitor_logs(name: &str, stop_on_exit: bool) -> impl FnMut(Duration) -> LogEvent {
        let mut lines = vec![format!("{name} line 1"), format!("stop_on_exit={stop_on_exit}")];
        move |_timeout| {
            if lines.is_empty() {
                LogEvent::End(Some(SuccessTuple::ok("done")))
            } else {
                LogEvent::Lines(vec![lines.remove(0)])
            }
        }
    }
}

/// Counts up to `fetch.count`.
#[make_connector]
mod ticker {
    use super::ACKS;

    fn subscribe(pipe: &Pipe, kwargs: &Value, connector: &Connector) -> Result<impl FnMut(Duration) -> Batch> {
        let count = pipe.parameters()["fetch"]["count"].as_u64().unwrap_or(3);
        let tag = format!("{}:{}", connector.label(), kwargs["chunksize"]);
        let mut next = 0u64;
        Ok(move |_timeout: Duration| {
            next += 1;
            match next {
                n if n > count => Batch::End,
                // A filtered message: no rows, but its token is acknowledged.
                2 => Batch::RowsAck(Vec::new(), 2),
                n => Batch::RowsAck(vec![json!({"id": n, "tag": tag})], n),
            }
        })
    }

    fn ack(tokens: &[u64]) {
        ACKS.lock().unwrap().extend_from_slice(tokens);
    }
}

/// A plugin-level stream that implements the trait on a type.
fn subscribe(pipe: &Pipe) -> Pulses {
    Pulses { left: pipe.parameters()["fetch"]["count"].as_u64().unwrap_or(1), closed: false }
}

struct Pulses {
    left: u64,
    closed: bool,
}

impl Subscription for Pulses {
    fn next_batch(&mut self, _timeout: Duration) -> Result<Batch> {
        if self.left == 0 {
            return Ok(Batch::Timeout);
        }
        self.left -= 1;
        Ok(Batch::Data { rows: vec![json!({"left": self.left})], ack: None, redelivered: true })
    }
    fn close(&mut self) -> Result<()> {
        self.closed = true;
        Ok(())
    }
}

#[make_action]
fn acks() -> (bool, String) {
    (true, format!("{:?}", ACKS.lock().unwrap()))
}
"##;

fn compiled(source: &str) -> (tempfile::TempDir, Arc<LoadedPlugin>) {
    let root = tempfile::tempdir().unwrap();
    let plugins = root.path().join("plugins");
    std::fs::create_dir(&plugins).unwrap();
    std::fs::write(plugins.join("surfaces.rs"), source).unwrap();
    let found = discovery::discover_report_in(&[plugins], &root.path().join("build"));
    assert!(found.skipped.is_empty(), "{:?}", found.skipped);
    support::compile_single_file::compile(&found.candidates[0]);
    // SAFETY: the fixture above is trusted code built with this crate's runtime.
    let plugin = unsafe { LoadedPlugin::load(&found.candidates[0].library) }.unwrap();
    (root, plugin)
}

fn pipe(connector: &str, count: u64) -> Pipe {
    let mut pipe = Pipe::builder(connector, "single_file_stream")
        .build()
        .unwrap();
    pipe.set_parameters(
        Value::from_json_str(&json!({"fetch": {"count": count}}).to_string())
            .unwrap()
            .to_map(),
    );
    pipe
}

fn route(name: &str, method: &str, path: &str) -> RouteRequest {
    RouteRequest {
        name: name.into(),
        method: method.into(),
        path: path.into(),
        ..Default::default()
    }
}

#[test]
fn routes_and_mcp_entries_reach_the_manifest_and_their_functions() {
    let (_root, plugin) = compiled(SOURCE);
    let entry = &plugin.manifest["plugins"]["surfaces"];
    assert_eq!(
        entry["api_routes"],
        json!([
            {"name": "item", "method": "GET", "path": "/sf/items/{id}", "scopes": ["pipes:read"],
             "public": false, "description": "One item, with what the request carried."},
            {"name": "echo", "method": "POST", "path": "/sf/echo/{*rest}", "scopes": [], "public": true},
            {"name": "any", "method": "*", "path": "/sf/any", "scopes": [], "public": false},
            {"name": "fail", "method": "GET", "path": "/sf/fail", "scopes": [], "public": true},
        ])
    );
    let tools = meerschaum_plugin::mcp::entries_of(entry).unwrap();
    assert_eq!(tools.tools[0].name, "sf_add");
    assert_eq!(tools.tools[0].description, "Add two integers.");
    assert!(tools.tools[0].hint("readOnlyHint"));
    assert!(tools.tools[0].validate(&json!({"a": 1})).is_err());
    assert_eq!(tools.tools[1].scopes, vec!["sf:secret"]);
    assert_eq!(tools.tools[1].title.as_deref(), Some("Who am I"));
    assert_eq!(
        tools.resources[0].uri_template.as_deref(),
        Some("sf://items/{id}")
    );
    assert_eq!(tools.resources[1].uri.as_deref(), Some("sf://logo"));
    assert!(tools.prompts[0].arguments[0].required);

    // Routes: path parameters, query, headers, cookies, the principal and scopes.
    let mut request = route("item", "GET", "/sf/items/7");
    request.path_params.insert("id".into(), "7".into());
    request.query_params.insert("q".into(), vec!["a".into()]);
    request.headers.insert("x-sf".into(), vec!["yes".into()]);
    request
        .headers
        .insert("cookie".into(), vec!["theme=dark; x=1".into()]);
    request.principal = Some(meerschaum_plugin::api::Principal {
        kind: "user".into(),
        username: Some("ada".into()),
        ..Default::default()
    });
    request.scopes = vec!["pipes:read".into()];
    let reply = plugin.api_route(&request).unwrap();
    assert_eq!(reply.status, 200);
    let body: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
    assert_eq!(
        body,
        json!({"id": "7", "q": "a", "header": "yes", "theme": "dark",
            "principal": {"kind": "user", "username": "ada"}, "read": true})
    );
    let mut echo = route("echo", "POST", "/sf/echo/a/b");
    echo.body = vec![0, 159, 255];
    echo.path_params.insert("rest".into(), "a/b".into());
    let reply = plugin.api_route(&echo).unwrap();
    assert_eq!((reply.status, reply.body.clone()), (201, vec![0, 159, 255]));
    assert!(reply.headers.contains(&("x-rest".into(), "a/b".into())));
    let reply = plugin.api_route(&route("any", "PATCH", "/sf/any")).unwrap();
    assert_eq!(
        (reply.status, reply.body.as_slice()),
        (202, b"PATCH".as_slice())
    );
    let reply = plugin.api_route(&route("fail", "GET", "/sf/fail")).unwrap();
    assert_eq!(reply.status, 500);
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&reply.body).unwrap(),
        json!({"detail": "synthetic failure"})
    );
    assert!(plugin.api_route(&route("missing", "GET", "/x")).is_err());

    // MCP tools, resources and prompts.
    let call = |name: &str, arguments: serde_json::Value| ToolCall {
        name: name.into(),
        arguments: arguments.as_object().cloned().unwrap_or_default(),
        scopes: vec!["*".into()],
        ..Default::default()
    };
    assert_eq!(
        plugin
            .mcp_tool(&call("sf_add", json!({"a": 2, "b": 40})))
            .unwrap(),
        json!({"sum": 42})
    );
    assert_eq!(
        plugin.mcp_tool(&call("sf_whoami", json!({}))).unwrap(),
        json!({"name": "sf_whoami", "principal": null, "scopes": ["*"]})
    );
    let read = |name: &str, uri: &str, variables: serde_json::Value| ResourceRead {
        name: name.into(),
        uri: uri.into(),
        variables: variables.as_object().cloned().unwrap_or_default(),
        ..Default::default()
    };
    let item = plugin
        .mcp_resource(&read("sf_item", "sf://items/9", json!({"id": "9"})))
        .unwrap();
    let ResourceContents::Text { text, mime_type } = item else {
        panic!("{item:?}")
    };
    assert_eq!(mime_type.as_deref(), Some("application/json"));
    assert!(text.contains("\"9\""), "{text}");
    assert_eq!(
        plugin
            .mcp_resource(&read("sf_logo", "sf://logo", json!({})))
            .unwrap(),
        ResourceContents::blob(vec![1, 2, 3], "application/octet-stream")
    );
    let (_, messages) = plugin
        .mcp_prompt(&PromptGet {
            name: "sf_greet".into(),
            arguments: json!({"who": "Ada"}).as_object().cloned().unwrap(),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(messages[0]["content"]["text"], "Say hello to Ada.");
}

#[test]
fn executor_modules_answer_their_methods_and_derive_the_rest() {
    let (root, plugin) = compiled(SOURCE);
    assert_eq!(
        plugin.manifest["executor_types"]["memory"],
        json!({"type": "memory", "plugin": "surfaces", "class_name": "memory",
            "methods": ["get_job_names", "get_job_metadata", "create_job", "delete_job",
                "write_stdin", "get_logs", "monitor_logs"],
            "doc": "Keeps jobs in memory."})
    );
    let executor = plugin
        .executor("memory", "test", json!({}))
        .unwrap()
        .with_root_dir(root.path().to_string_lossy());
    let created = executor
        .call(
            "create_job",
            json!({"name": "a", "sysargs": ["show", "version"], "properties": {"tag": "t"}}),
        )
        .unwrap();
    assert_eq!(
        created,
        json!([true, format!("memory:test {} \"t\"", root.path().display())])
    );
    assert_eq!(
        executor.call("get_job_names", json!({})).unwrap(),
        json!(["a"])
    );
    // Derived as in the packaged SDK: exists, status, properties, began, jobs.
    assert_eq!(
        executor
            .call("get_job_exists", json!({"name": "a"}))
            .unwrap(),
        json!(true)
    );
    assert_eq!(
        executor
            .call("get_job_exists", json!({"name": "b"}))
            .unwrap(),
        json!(false)
    );
    assert_eq!(
        executor
            .call("get_job_status", json!({"name": "a"}))
            .unwrap(),
        json!("running")
    );
    assert_eq!(
        executor
            .call("get_job_began", json!({"name": "a"}))
            .unwrap(),
        json!("2026-01-01T00:00:00+00:00")
    );
    assert_eq!(
        executor
            .call("get_job_paused", json!({"name": "a"}))
            .unwrap(),
        json!(null)
    );
    assert_eq!(
        executor.call("get_jobs", json!({})).unwrap()["a"]["sysargs"],
        json!(["show", "version"])
    );
    // Methods the module leaves out are unsupported.
    for method in [
        "pause_job",
        "get_job_stop_time",
        "get_job_is_blocking_on_stdin",
    ] {
        assert!(
            matches!(
                executor.call(method, json!({"name": "a"})),
                Err(meerschaum_plugin::Error::Unsupported(m)) if m.contains(method)
            ),
            "{method}"
        );
    }
    executor
        .call("write_stdin", json!({"name": "a", "data": "y\n"}))
        .unwrap();
    assert_eq!(
        executor.call("get_logs", json!({"name": "a"})).unwrap(),
        json!("a:y\n")
    );
    let mut logs = executor
        .monitor_logs(
            "a",
            MonitorLogs {
                stop_on_exit: true,
                ..Default::default()
            },
        )
        .unwrap();
    let mut lines = Vec::new();
    loop {
        match logs.next_lines(Duration::from_millis(100)).unwrap() {
            LogEvent::Lines(l) => lines.extend(l),
            LogEvent::Timeout => continue,
            LogEvent::End(result) => {
                assert_eq!(result.unwrap().message, "done");
                break;
            }
        }
    }
    assert_eq!(lines, vec!["a line 1", "stop_on_exit=true"]);
    logs.close().unwrap();
    assert_eq!(
        executor.call("delete_job", json!({"name": "a"})).unwrap(),
        json!([true, "Deleted a."])
    );
    assert_eq!(executor.call("get_jobs", json!({})).unwrap(), json!({}));
}

#[test]
fn subscribe_streams_batches_and_acknowledges_tokens() {
    let (_root, plugin) = compiled(SOURCE);
    let ticker = &plugin.manifest["connector_types"]["ticker"];
    assert_eq!(ticker["is_streaming"], true);
    assert_eq!(ticker["methods"], json!(["subscribe"]));
    assert_eq!(
        plugin.manifest["plugins"]["surfaces"]["data_functions"],
        json!(["subscribe"])
    );
    let connector = plugin.connector("ticker", "abi", json!({})).unwrap();
    assert!(connector.is_streaming());
    let subscription = connector
        .subscribe(&pipe("ticker:abi", 3), json!({"chunksize": 3}))
        .unwrap();
    let mut seen = Vec::new();
    loop {
        match subscription.next_batch(Duration::from_millis(100)).unwrap() {
            NativeBatch::Data {
                frame,
                ack,
                redelivered,
            } => {
                assert!(!redelivered);
                seen.push((frame.height(), ack));
                if frame.height() == 1 {
                    assert_eq!(
                        frame.row(0)[1],
                        meerschaum_kernel::Value::Str("abi:3".into())
                    );
                }
            }
            NativeBatch::Timeout => continue,
            NativeBatch::End => break,
        }
    }
    // The filtered message arrives as an empty batch that still carries its token.
    assert_eq!(seen, vec![(1, Some(1)), (0, Some(2)), (1, Some(3))]);
    subscription.ack(&[1, 2, 3]).unwrap();
    subscription.close().unwrap();
    assert_eq!(
        plugin.action("acks", json!({})).unwrap().message,
        "[1, 2, 3]"
    );
    // A closed handle is gone; closing again is a no-op.
    assert!(subscription.next_batch(Duration::from_millis(10)).is_err());
    subscription.close().unwrap();

    // `plugin:<name>`: a type implementing `Subscription`, with `redelivered`.
    let source = plugin.source(json!({}));
    assert!(source.is_streaming());
    let pulses = source
        .subscribe(&pipe("plugin:surfaces", 1), json!({}))
        .unwrap();
    assert!(matches!(
        pulses.next_batch(Duration::from_millis(10)).unwrap(),
        NativeBatch::Data {
            redelivered: true,
            ack: None,
            ..
        }
    ));
    assert!(matches!(
        pulses.next_batch(Duration::from_millis(10)).unwrap(),
        NativeBatch::Timeout
    ));
}

fn prepare(
    source: &str,
) -> (
    tempfile::TempDir,
    std::path::PathBuf,
    meerschaum_plugin::Result<String>,
) {
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("minimal.rs");
    std::fs::write(&file, source).unwrap();
    let output = root.path().join("generated");
    let result = meerschaum_plugin::source::prepare_single_file(&file, "minimal", &output)
        .map(|_| std::fs::read_to_string(output.join("lib.rs")).unwrap());
    (root, file, result)
}

#[test]
fn server_and_executor_attribute_errors_are_reported_at_their_lines() {
    for (source, message) in [
        (
            "#[api_route(GET)]\nfn r() {}",
            "the path is a string literal",
        ),
        ("#[api_route]\nfn r() {}", "use #[api_route(GET"),
        (
            "#[api_route(\"/a\", public = true, scopes = [\"x\"])]\nfn r() {}",
            "cannot require scopes",
        ),
        (
            "#[api_route(GET, \"/a\", colour = 1)]\nfn r() {}",
            "unknown #[api_route] option 'colour'",
        ),
        (
            "#[api_route(1, \"/a\")]\nfn r() {}",
            "the method is an HTTP method",
        ),
        (
            "#[api_route(\"/a\")]\nfn fetch() {}",
            "reserved plugin functions",
        ),
        (
            "#[api_route(\"/a\")]\n#[api_route(\"/b\")]\nfn r() {}",
            "duplicate #[api_route]",
        ),
        (
            "#[api_route(\"/a\")]\nfn r() {}\n#[api_route(\"/b\", name = \"r\")]\nfn s() {}",
            "duplicate #[api_route] name 'r'",
        ),
        (
            "#[api_route(\"/a\")]\nfn r(pipe: &Pipe) {}",
            "routes accept request",
        ),
        (
            "#[api_route(\"/a\")]\nfn r(request: &Value) {}",
            "must have type &RouteRequest",
        ),
        (
            "#[api_route(\"/a\")]\nstruct R;",
            "belongs on a top-level function",
        ),
        ("#[mcp_tool]\nfn t() {}", "needs a description"),
        (
            "#[mcp_tool(description = \"d\", input_schema = \"{\")]\nfn t() {}",
            "not valid JSON",
        ),
        (
            "#[mcp_tool(description = \"d\", input_schema = \"[]\")]\nfn t() {}",
            "must be a JSON object",
        ),
        (
            "#[mcp_tool(description = \"d\", input_schema = r#\"{\"type\": 3}\"#)]\nfn t() {}",
            "not a valid JSON Schema",
        ),
        (
            "#[mcp_tool(\"d\")]\nfn t() {}",
            "takes only key = value options",
        ),
        (
            "#[mcp_tool(description = \"d\")]\nfn t(kwargs: &Value) {}",
            "tools accept call",
        ),
        (
            "#[mcp_resource(description = \"d\")]\nfn r() {}",
            "exactly one of uri",
        ),
        (
            "#[mcp_resource(description = \"d\", uri = \"x://a\")]\nfn r() {}\n#[mcp_resource(description = \"d\", uri = \"x://a\")]\nfn s() {}",
            "duplicate resource URI",
        ),
        (
            "#[mcp_prompt(arguments = \"[1]\")]\nfn p() {}",
            "arguments must be",
        ),
        (
            "#[make_executor]\nmod local { fn get_job_names() -> Vec<String> { vec![] } }",
            "executor type 'local' is reserved",
        ),
        (
            "#[make_executor(kind = \"x\")]\nmod e { fn get_job_names() -> Vec<String> { vec![] } }",
            "expected name",
        ),
        (
            "#[make_executor]\nmod e { fn helper() {} }",
            "at least one of its methods",
        ),
        (
            "#[make_executor]\nmod e { fn start_job(name: &str, force: bool) -> (bool, String) { (force, name.into()) } }",
            "unsupported start_job parameter 'force'",
        ),
        (
            "#[make_executor]\nmod e { fn start_job(name: String) -> (bool, String) { (true, name) } }",
            "must have type &str",
        ),
        (
            "#[make_executor]\n#[make_connector]\nmod e { fn fetch() {} }",
            "either a #[make_connector] or a #[make_executor]",
        ),
        (
            "#[make_executor]\nfn e() {}",
            "belongs on a top-level inline module",
        ),
        (
            "#[make_executor]\nmod e;",
            "#[make_executor] needs an inline module",
        ),
        (
            "#[make_connector]\nmod t { fn subscribe() {} }",
            "subscribe must return",
        ),
        (
            "#[make_connector]\nmod t { fn subscribe(begin: Option<Bound>) -> u8 { 0 } }",
            "subscribe accepts pipe",
        ),
        (
            "#[make_connector]\nmod t { fn subscribe() -> impl FnMut(Duration) -> Batch { |_| Batch::End } fn ack(tokens: Vec<u64>) {} }",
            "ack parameter 'tokens' must have type &[u64]",
        ),
        (
            "#[make_connector]\nmod t { fn subscribe(pipe: &Pipe) -> Vec<Value> { vec![] } }",
            "subscribe must return impl FnMut(Duration) -> Batch, impl Subscription, Box<dyn Subscription>",
        ),
        (
            "#[api_route(\"/{id}\")]\nfn r() {}",
            "a parameter in the first segment would shadow every core route",
        ),
        (
            "#[api_route(\"/a/{x:float32}\")]\nfn r() {}",
            "unknown converter 'float32'",
        ),
        (
            "#[api_route(\"/a/{*rest}/b\")]\nfn r() {}",
            "a catch-all must be the last segment",
        ),
        (
            "#[api_route(\"/pipes/x\")]\nfn r() {}",
            "it would shadow the core route group 'pipes'",
        ),
        ("#[api_route(\"/_pages/x\")]\nfn r() {}", "('/_pages')"),
        (
            "#[api_route(POST, \"/dash/x\")]\nfn r() {}",
            "the route path \"/dash/x\" is refused: it would shadow the web console",
        ),
        (
            "#[web_page(\"a/{b}\")]\nfn p() -> String { String::new() }",
            "is not a console page path",
        ),
        (
            "#[web_page(\"a\", colour = true)]\nfn p() -> String { String::new() }",
            "unknown #[web_page] option 'colour'",
        ),
        (
            "#[web_page(login_required = false, \"a\")]\nfn p() -> String { String::new() }",
            "the page path comes first",
        ),
        (
            "#[web_page(\"a\")]\nfn p() -> String { String::new() }\n#[web_page(\"/dash/a/\")]\nfn q() -> String { String::new() }",
            "duplicate #[web_page] path \"/dash/a\"",
        ),
        (
            "#[web_page]\nfn p(pipe: &Pipe) -> String { String::new() }",
            "pages accept page: &PageRequest",
        ),
        (
            "#[web_page]\nstruct P;",
            "#[web_page] belongs on a top-level function",
        ),
        (
            "#[make_connector(instance)]\nmod m { fn register_pipe(colour: bool) -> SuccessTuple { SuccessTuple::success() } }",
            "unsupported register_pipe parameter 'colour'; instance methods accept pipe",
        ),
        (
            "#[make_connector(instance)]\nmod m { fn sync_pipe(data: Frame) -> SuccessTuple { SuccessTuple::success() } }",
            "sync_pipe parameter 'data' must have type Option<Frame> or Vec<Value>",
        ),
        (
            "#[make_connector(instance)]\nmod m { fn get_sync_time(params: &Map) -> Value { Value::Null } }",
            "get_sync_time parameter 'params' must have type &Value, &str or an owned type",
        ),
        (
            "#[make_connector(instance = false)]\nmod m { fn register_pipe() {} }",
            "a source connector must define fetch, sync and/or subscribe",
        ),
    ] {
        let (_root, file, result) = prepare(source);
        let error = result.unwrap_err().to_string();
        assert!(error.contains(message), "expected {message:?}: {error}");
        assert!(error.contains(&file.display().to_string()), "{error}");
    }
    for (source, position) in [
        (
            "\n#[mcp_tool(description = \"d\", input_schema = \"{\")]\nfn t() {}",
            "2:46",
        ),
        (
            "#[make_executor]\nmod e {\n    fn stop_job(name: u8) {}\n}",
            "3:23",
        ),
        // A route path is reported at its literal.
        ("\n\n#[api_route(GET, \"/pipes/x\")]\nfn r() {}", "3:18"),
        (
            "#[web_page(\n  \"..\")]\nfn p() -> String { String::new() }",
            "2:3",
        ),
    ] {
        let (_root, file, result) = prepare(source);
        let error = result.unwrap_err().to_string();
        let expected = format!("{}:{position}:", file.display());
        assert!(error.starts_with(&expected), "expected {expected}: {error}");
    }
}

#[test]
fn names_that_were_helpers_before_stay_helpers() {
    // A `subscribe` that takes other parameters, an action named `subscribe` and a connector's
    // `ack` without `subscribe` keep their earlier meaning.
    let (_root, _file, lib) = prepare(
        "fn subscribe(topic: &str) -> usize { topic.len() }\n#[make_action]\nfn go() -> (bool, String) { (true, subscribe(\"x\").to_string()) }\n#[make_connector]\nmod data { fn fetch() -> Vec<Value> { ack(); vec![] } fn ack() {} }",
    );
    let lib = lib.unwrap();
    assert!(lib.contains(r#"\"data_functions\":[]"#), "{lib}");
    assert!(!lib.contains("is_streaming"), "{lib}");
    let (_root, _file, lib) = prepare("#[make_action]\nfn subscribe() {}");
    assert!(lib.unwrap().contains(r#"\"name\":\"subscribe\""#));
    // The returned closure captures no parameter lifetime: `+ use<>` is added in place.
    let root = tempfile::tempdir().unwrap();
    let file = root.path().join("minimal.rs");
    std::fs::write(
        &file,
        "#[make_connector]\nmod t {\n    fn subscribe() -> Result<impl FnMut(Duration) -> Batch> { Ok(|_| Batch::End) }\n}\n",
    )
    .unwrap();
    let output = root.path().join("generated");
    meerschaum_plugin::source::prepare_single_file(&file, "minimal", &output).unwrap();
    let user = std::fs::read_to_string(output.join("user.rs")).unwrap();
    assert!(
        user.lines()
            .nth(2)
            .unwrap()
            .contains("Result<impl FnMut(Duration) -> Batch + use<>>"),
        "{user}"
    );
}
