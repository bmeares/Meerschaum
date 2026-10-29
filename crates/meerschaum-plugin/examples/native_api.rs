//! API routes and MCP entries of a packaged native plugin; all data is synthetic.
//!
//! Routes (`api_routes` in the manifest, served by `Plugin::api_route`):
//! - `GET /native-api/items/{id}` (`pipes:read`): the path, query and header values as JSON.
//! - `PUT /native-api/blobs/{*key}` (`pipes:write`): echoes the body bytes with its own headers.
//! - `GET /native-api/whoami` (public): the principal and scopes the server passed.
//! - `* /native-api/any`: any method; replies `202` with the method.
//! - `GET /native-api/docs/{name}.json` (public): a parameter with a static suffix.
//! - `GET /native-api/prefs` (public): the `theme` cookie the browser sent.
//! - `GET /native-api/slow` (public): sleeps two seconds (server calls run concurrently).
//! - `GET /pipes/native-api`: refused when the server starts (it would shadow `/pipes`).
//!
//! MCP (`mcp_tools`, `mcp_resources`, `mcp_prompts`): the tool `native_api_add` (read-only),
//! the tool `native_api_secret` (scope `native:secret`), the tool `native_api_whoami`, the resource template
//! `native-api://items/{id}`, the binary resource `native-api://logo` and the prompt
//! `native_api_greet`. The tool `list_pipes` is hidden at startup (a built-in has that name).
use meerschaum_plugin::{
    Error, Plugin, Result,
    api::{RouteRequest, RouteResponse, RouteSpec},
    mcp::{PromptGet, ResourceContents, ResourceRead, ToolCall, ToolSpec, message},
    serde_json::{Value, json},
};

#[derive(Default)]
struct NativeApi;

impl Plugin for NativeApi {
    const NAME: &'static str = "native_api";
    const VERSION: &'static str = "1.0.0";

    fn manifest(&self) -> Value {
        let routes = [
            RouteSpec::new("item", "GET", "/native-api/items/{id}").scopes(["pipes:read"]),
            RouteSpec::new("blob", "PUT", "/native-api/blobs/{*key}").scopes(["pipes:write"]),
            RouteSpec::new("whoami", "GET", "/native-api/whoami").public(),
            RouteSpec::new("any", "*", "/native-api/any"),
            RouteSpec::new("doc", "GET", "/native-api/docs/{name}.json").public(),
            RouteSpec::new("prefs", "GET", "/native-api/prefs").public(),
            RouteSpec::new("slow", "GET", "/native-api/slow").public(),
            RouteSpec::new("shadow", "GET", "/pipes/native-api").public(),
        ];
        let tools = [
            ToolSpec::new("native_api_add", "Add two integers.")
                .input_schema(json!({
                    "type": "object",
                    "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}},
                    "required": ["a", "b"],
                    "additionalProperties": false,
                }))
                .read_only(),
            ToolSpec::new(
                "native_api_secret",
                "Return a synthetic secret and the caller.",
            )
            .scopes(["native:secret"]),
            ToolSpec::new("native_api_whoami", "The caller's principal and scopes.").read_only(),
            ToolSpec::new("list_pipes", "Hidden: a built-in tool has this name."),
        ];
        json!({
            "schema_version": meerschaum_plugin::MANIFEST_SCHEMA_VERSION,
            "meerschaum_version": "5.0.0", "python_version": "", "fingerprint": null,
            "plugins": {"native_api": {
                "name": "native_api", "version": "1.0.0",
                "api_routes": routes.iter().map(RouteSpec::to_json).collect::<Vec<_>>(),
                "mcp_tools": tools.iter().map(ToolSpec::to_json).collect::<Vec<_>>(),
                "mcp_resources": [
                    {"name": "native_api_item", "description": "A synthetic item.",
                     "uri_template": "native-api://items/{id}", "scopes": ["pipes:read"]},
                    {"name": "native_api_logo", "description": "Four synthetic bytes.",
                     "uri": "native-api://logo", "mime_type": "application/octet-stream"}
                ],
                "mcp_prompts": [
                    {"name": "native_api_greet", "description": "Greet someone.",
                     "arguments": [{"name": "who", "description": "The name.", "required": true}]}
                ]
            }}
        })
    }

    fn api_route(&self, request: &RouteRequest) -> Result<RouteResponse> {
        Ok(match request.name.as_str() {
            "item" => RouteResponse::json(&json!({
                "id": request.path_param("id"),
                "route_path": request.route_path,
                "q": request.query_params.get("q"),
                "header": request.header("x-native-api"),
                "has_authorization": request.header("authorization").is_some(),
                "principal": request.principal,
            })),
            "blob" => RouteResponse::bytes("application/octet-stream", request.body.clone())
                .with_status(201)
                .header(
                    "x-native-key",
                    request.path_param("key").unwrap_or_default(),
                )
                .header("set-cookie", "a=1")
                .header("set-cookie", "b=2"),
            "whoami" => RouteResponse::json(&json!({
                "principal": request.principal, "scopes": request.scopes,
            })),
            "any" => RouteResponse::text(request.method.clone()).with_status(202),
            "doc" => RouteResponse::json(&json!({"name": request.path_param("name")})),
            "prefs" => RouteResponse::json(&json!({"theme": request.cookie("theme")})),
            "slow" => {
                std::thread::sleep(std::time::Duration::from_secs(2));
                RouteResponse::text("slept")
            }
            other => return Err(Error::Message(format!("unknown route {other}"))),
        })
    }

    fn mcp_tool(&self, call: &ToolCall) -> Result<Value> {
        match call.name.as_str() {
            "native_api_add" => {
                let a = call.argument("a").and_then(Value::as_i64).unwrap_or(0);
                let b = call.argument("b").and_then(Value::as_i64).unwrap_or(0);
                Ok(json!({"sum": a + b}))
            }
            "native_api_whoami" => Ok(json!({
                "principal": call.principal, "scopes": call.scopes,
            })),
            "native_api_secret" => Ok(json!({
                "secret": "synthetic", "principal": call.principal, "scopes": call.scopes,
            })),
            other => Err(Error::Message(format!("unknown tool {other}"))),
        }
    }

    fn mcp_resource(&self, read: &ResourceRead) -> Result<ResourceContents> {
        match read.name.as_str() {
            "native_api_item" => Ok(ResourceContents::json(
                &json!({"id": read.variable("id"), "uri": read.uri}),
            )),
            "native_api_logo" => Ok(ResourceContents::blob(
                vec![0x89, b'M', b'R', 0],
                "application/octet-stream",
            )),
            other => Err(Error::Message(format!("unknown resource {other}"))),
        }
    }

    fn mcp_prompt(&self, get: &PromptGet) -> Result<Value> {
        let who = get.argument("who").unwrap_or("nobody");
        Ok(json!({
            "description": format!("A greeting for {who}."),
            "messages": [message("user", format!("Say hello to {who}."))],
        }))
    }
}

meerschaum_plugin::export_plugin!(NativeApi);
