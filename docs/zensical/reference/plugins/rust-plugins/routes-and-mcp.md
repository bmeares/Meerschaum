# Routes and MCP in Rust Plugins

A Rust plugin can serve **HTTP routes** next to the core API (the native counterpart of
`@api_plugin`) and expose **MCP tools, resources and prompts** to agents (the counterpart of
`meerschaum.mcp.tool(...)`). Both are declared in the plugin's manifest, so `mrsm start api` and
`mrsm start mcp` know them without loading the library, and both run in-process: no Python API
host is started for them.

The complete, tested example is
[`crates/meerschaum-plugin/examples/native_api.rs`](https://github.com/bmeares/Meerschaum/blob/v5/crates/meerschaum-plugin/examples/native_api.rs).

## 🛣️ Routes

Declare each route under `api_routes` in your plugin's manifest entry and implement
`Plugin::api_route`:

```rust
use meerschaum_plugin::{
    Plugin, Result,
    api::{RouteRequest, RouteResponse, RouteSpec},
    serde_json::{Value, json},
};

#[derive(Default)]
struct Items;

impl Plugin for Items {
    const NAME: &'static str = "items";
    const VERSION: &'static str = "1.0.0";

    fn manifest(&self) -> Value {
        let routes = [
            RouteSpec::new("get_item", "GET", "/items/{id}").scopes(["pipes:read"]),
            RouteSpec::new("upload", "PUT", "/items/files/{*key}").scopes(["pipes:write"]),
            RouteSpec::new("health", "GET", "/items-health").public(),
        ];
        json!({
            "schema_version": meerschaum_plugin::MANIFEST_SCHEMA_VERSION,
            "meerschaum_version": "5.0.0", "python_version": "", "fingerprint": null,
            "plugins": {"items": {
                "name": "items", "version": "1.0.0",
                "api_routes": routes.iter().map(RouteSpec::to_json).collect::<Vec<_>>(),
            }}
        })
    }

    fn api_route(&self, request: &RouteRequest) -> Result<RouteResponse> {
        Ok(match request.name.as_str() {
            "get_item" => RouteResponse::json(&json!({
                "id": request.path_param("id"),
                "fields": request.query_params.get("field"),
                "caller": request.principal,
            })),
            "upload" => RouteResponse::empty(204).header("x-stored", request.body.len().to_string()),
            _ => RouteResponse::text("ok"),
        })
    }
}

meerschaum_plugin::export_plugin!(Items);
```

### Declaring a route

| Key | Meaning |
|---|---|
| `name` | The route's name within the plugin; the request carries it back (`request.name`). |
| `method` | Any HTTP method (`GET`, `POST`, `PUT`, `PATCH`, `DELETE`, `OPTIONS`, or an extension method such as `PROPFIND`), or `*` for every method. Default `GET`. Several routes may share a path with different methods; another method is a `405` with an `Allow` header, and `HEAD` falls back to the `GET` route (without a body). |
| `path` | A path with parameters (see [Paths](#paths)). |
| `scopes` | Scopes the caller must hold, such as `pipes:read`. A caller holding `*` holds all of them. |
| `public` | Serve without credentials. When the caller sends valid credentials anyway, the principal is still passed. A public route may not declare `scopes` (it is refused at startup). |
| `description` | Optional text for people reading the manifest. |

#### Paths

Paths follow Starlette's syntax (what `@api_plugin` routes use), one parameter per segment:

| Segment | Matches |
|---|---|
| `items` | The literal text. |
| `{id}` or `{id:str}` | Any non-empty segment. |
| `{id}.json`, `v{n}` | A parameter with a static prefix or suffix (`/docs/{name}.json` matches `/docs/a.b.json` with `name = "a.b"`). |
| `{n:int}`, `{x:float}`, `{u:uuid}` | Digits; digits with an optional fraction; a hyphenated UUID. Values still arrive as strings. |
| `{*rest}` or `{rest:path}` | The rest of the path, slashes included (last segment only, not empty). |

Values are percent-decoded. When several routes of a plugin match the same request (for example
`/docs/{name}.json` and `/docs/{n:int}`), each is tried in declaration order and the first one
that matches the path and the method serves it. A path that matches no route is a `404`.

A route needs the core API's credentials (a login token or an API key) and every scope it
declares, with the same `401` and `403 {"detail": "Missing required scope: '<scope>'"}`
answers as the core routes. Under `--no-auth`, every route is open and the caller holds `*`.

### Refused routes

When the server starts, it refuses (and skips, with one line on stderr naming the plugin, the
route and the reason) any route that:

- would **shadow a core route**: a core route group (`/pipes`, `/users`, `/actions`, `/jobs`,
  `/engine`, `/mcp`, `/login`, `/docs`, `/`, ...), the console (`/dash`) and its page routes
  (`/_pages`, which serve plugin pages and their assets), `/static`, the web terminal, or the
  server's internal `/_mrsm/` routes;
- has a parameter in its first segment (`/{x}/...`, `/x{id}`), which would capture unmatched paths;
- conflicts with a route another native plugin mounted first, or repeats a method on the same
  path (`/a/{id}` and `/a/{other}` are the same path);
- is `public` and declares `scopes`;
- belongs to a route group that `api:permissions:routes` disables;
- has invalid syntax (`:id` instead of `{id}`, two parameters in one segment, an unknown
  converter, a catch-all that is not last or not a whole segment, `..`, `%`).

```text
Native plugin 'items': route GET /pipes/items is refused: it would shadow the core route group 'pipes'.
```

The rest of the plugin's routes are still served.

### The request

`RouteRequest` is the whole HTTP request:

| Field | Contents |
|---|---|
| `name`, `route_path` | The matched route's name and declared path. |
| `method`, `path` | The request method (uppercase) and path. |
| `path_params` | `{"id": "42"}`, percent-decoded. `request.path_param("id")`. |
| `query`, `query_params` | The raw query string, and every decoded value of each key in order (`{"q": ["a", "b"]}`). `request.query_param("q")` returns the first. |
| `headers` | Lowercase names to every value. `authorization` is removed (the server already authenticated the caller), and so is the console's session cookie (`mrsm-session-id`) from `cookie`; other cookies are kept. `request.header("X-Name")` is case-insensitive and `request.cookie("theme")` reads one cookie. |
| `body` | The body as bytes (up to 64 MiB: a larger body is a `413`, one that cannot be read a `400`). `request.text()` and `request.json::<T>()` decode it. |
| `principal` | `None` when anonymous, else `{"kind": "user", "username", "user_id", "user_type"}` or `{"kind": "token", "token_id", "label", "user_id"}`. No secrets. |
| `scopes` | The caller's granted scopes (`["*"]` under `--no-auth`). `request.has_scope("x")`. |

### The response

`RouteResponse` carries a status, headers (several values of one name are kept, such as two
`set-cookie` headers) and body bytes:

```rust
RouteResponse::json(&value)                         // 200 application/json
RouteResponse::text("hello")                        // 200 text/plain; charset=utf-8
RouteResponse::bytes("image/png", png_bytes)        // any content type
RouteResponse::empty(204)
RouteResponse::error(404, "No such item.")          // {"detail": "No such item."}, like the core API
RouteResponse::text("moved").with_status(307).header("location", "/items/1")
```

An `Err` from `api_route` becomes `500 {"detail": "<message>"}`.

## ⚙️ How calls run

- **Concurrently.** Like FastAPI with `@api_plugin`, the server runs requests in parallel: each
  `api_route`, `mcp_tool`, `mcp_resource` and `mcp_prompt` call runs on its own thread and is
  not serialized with the plugin's other calls, so a slow route holds up neither the plugin's
  other routes nor its MCP entries. These methods must therefore be thread-safe; the SDK builds a
  fresh `P::default()` for every call, so a plugin with no shared state needs nothing more.
  Host services (`Pipe` calls back into Meerschaum) are not available in these calls.
- **Bounded.** At most 32 server calls of one plugin run at once (routes and MCP entries are
  counted separately). Up to 16 further requests wait up to 30 seconds for a free slot; any
  beyond those, and any that wait too long, answer
  `503 {"detail": "Native plugin '<name>' is busy ..."}` with `Retry-After: 1` (an MCP call
  returns that message as its error). A route's request body is read only once it holds a slot,
  so waiting and refused requests buffer nothing. A call that never returns keeps its slot, so a
  stuck handler degrades into `503`s rather than unbounded threads and memory.
- **Fixed at startup.** Routes and MCP entries are read from the manifests when `mrsm start api`
  (or `mrsm start mcp`) starts. Installing, updating or removing a native plugin takes effect
  after a restart; unlike Python plugins, the running server does not pick it up.

## 🤖 MCP tools, resources and prompts

Declare entries under `mcp_tools`, `mcp_resources` and `mcp_prompts` and implement the matching
`Plugin` methods:

```rust
use meerschaum_plugin::mcp::{
    PromptGet, ResourceContents, ResourceRead, ToolCall, ToolSpec, message,
};

// In `manifest()`, inside the plugin's entry:
//   "mcp_tools": [ToolSpec::new("count_items", "Count the items of a kind.")
//       .input_schema(json!({"type": "object",
//           "properties": {"kind": {"type": "string"}}, "required": ["kind"]}))
//       .scopes(["pipes:read"])
//       .read_only()
//       .to_json()],
//   "mcp_resources": [{"name": "item", "description": "One item.",
//       "uri_template": "items://{id}", "scopes": ["pipes:read"]}],
//   "mcp_prompts": [{"name": "summarize", "description": "Summarize an item.",
//       "arguments": [{"name": "id", "required": true}]}],

fn mcp_tool(&self, call: &ToolCall) -> Result<Value> {
    let kind = call.argument("kind").and_then(Value::as_str).unwrap_or_default();
    Ok(json!({"kind": kind, "count": 3}))
}

fn mcp_resource(&self, read: &ResourceRead) -> Result<ResourceContents> {
    Ok(ResourceContents::json(&json!({"id": read.variable("id")})))
}

fn mcp_prompt(&self, get: &PromptGet) -> Result<Value> {
    Ok(json!({"messages": [message("user", format!("Summarize item {}.", get.argument("id").unwrap_or("?")))]}))
}
```

| Entry | Keys | Called with | Returns |
|---|---|---|---|
| `mcp_tools` | `name`, `description`, `input_schema` (JSON Schema), `output_schema`?, `scopes`, `title`?, `annotations`? (`readOnlyHint`, `destructiveHint`, `idempotentHint`, `openWorldHint`) | `ToolCall {name, arguments, principal, scopes}` | Any JSON value. A string is sent as the text content as is; anything else as indented JSON, and also as `structuredContent` when the tool declares an `output_schema` and the value is an object. |
| `mcp_resources` | `name`, `description`, `uri` or `uri_template` (`{var}` matches one path segment), `mime_type`? (default `application/json`), `scopes`, `title`? | `ResourceRead {name, uri, variables, principal, scopes}` | `ResourceContents::text`, `::json` or `::blob` (sent base64-encoded). |
| `mcp_prompts` | `name`, `description`?, `title`?, `arguments` (`[{name, description?, required?}]`), `scopes` | `PromptGet {name, arguments, principal, scopes}` | A string (one user message), a list of messages, or `{"description"?, "messages"}`. |

Native entries follow the same rules as built-in and Python plugin entries:

- An entry whose `scopes` the caller does not hold is **hidden** from the lists and refused if
  called. Over stdio (`mrsm start mcp`) the caller holds `*`.
- A read-only server (`api:mcp:read_only`) hides tools without `readOnlyHint: true`, and prompts
  whose scopes end in `:write`, `:delete`, `:drop` or `:execute`.
- Tool arguments are **validated against `input_schema`** before your method runs; a violation
  is a tool error (`Invalid arguments for '<tool>': ...`) that the model sees. Prompt arguments
  are checked against the declared `arguments`.
- A native tool or prompt never replaces a built-in one of the same name, and a native resource
  never takes a built-in resource's URI: such an entry is hidden, with one line on stderr at
  startup (`Native plugin 'items': MCP tool 'list_pipes' is hidden: a built-in tool has that
  name.`), as is an entry whose name another native plugin declared first. A native tool or
  prompt replaces a Python plugin's tool or prompt of the same name.
- An `Err` from a tool is a tool error (`isError: true`) with your message; from a resource or a
  prompt, a JSON-RPC error.

## 🧱 Without the typed methods

Plugins that implement `Plugin::call` themselves receive these call kinds as `request.method`
with JSON contexts, and can forward them with `meerschaum_plugin::sdk::call_server_extension`:

| Method | Context | Reply |
|---|---|---|
| `api_route` | `RouteRequest` as JSON; the body is `body_base64` | `{"status"?, "headers"?: {"name": ["v", ...]}, "body_base64"?}`; the host also accepts `"json": <value>`, `"body": "<text>"` and `"content_type"` |
| `mcp_tool` | `{"name", "arguments", "principal", "scopes"}` | the result value |
| `mcp_resource` | `{"name", "uri", "variables", "principal", "scopes"}` | a string, `{"text", "mime_type"?}` or `{"blob_base64", "mime_type"?}` |
| `mcp_prompt` | `{"name", "arguments", "principal", "scopes"}` | a string, a list of messages, or `{"description"?, "messages"}` |

These travel through the same `invoke` entry point as every other method: the ABI version is
unchanged, and plugins built before these call kinds existed keep loading.

### Plugins with `routes` and `route`

The first native route adapter asked a plugin whose manifest lists a name under `api_plugins`
for `routes` (`[{"method", "path", "public"?}]`) and served each request through `route`
(`{method, path, query, body, user}` with a text body; reply `{status, content_type, body}`).
Such plugins keep working unchanged, with static paths: their replies are parsed as before (an
invalid or missing `status` is `200`, `content_type` defaults to `text/plain; charset=utf-8`),
and their `route` calls stay serialized with the plugin's other calls. They now authenticate
like the core routes, so an API key works on a non-public route as well as a login token.
Declare `api_routes` instead to get path parameters, scopes, headers, byte bodies and concurrent
calls; a plugin that declares `api_routes` is never asked for `routes`.

## 📄 Single-file plugins

A single `.rs` file declares routes and MCP entries with attributes on top-level functions;
Meerschaum writes the manifest entries and the dispatch for you:

```rust
/// One item (the doc comment is the route's description).
#[api_route(GET, "/items/{id:int}", scopes = ["pipes:read"])]
fn item(request: &RouteRequest) -> Value {
    json!({"id": request.path_param("id"), "user": request.principal})
}

/// Add two integers.
#[mcp_tool(read_only = true, input_schema = r#"{"type": "object", "properties": {"a": {"type": "integer"}, "b": {"type": "integer"}}, "required": ["a", "b"]}"#)]
fn add(arguments: &Value) -> Value {
    json!({"sum": arguments["a"].as_i64().unwrap_or(0) + arguments["b"].as_i64().unwrap_or(0)})
}

/// One item as a resource.
#[mcp_resource(uri_template = "items://{id}")]
fn item_resource(resource: &ResourceRead) -> ResourceContents {
    ResourceContents::json(&json!({"id": resource.variable("id")}))
}

#[mcp_prompt(description = "Greet someone.", arguments = ["who"])]
fn greet(prompt: &PromptGet) -> String {
    format!("Say hello to {}.", prompt.argument("who").unwrap_or("nobody"))
}
```

`#[api_route]` takes the method (`GET`, `"post"`, `"*"`; `GET` when only the path is given),
the path and `scopes`, `public`, `name` and `description`. The MCP attributes take `name`,
`description` (default: the doc comment), `title` and `scopes`, plus `input_schema`,
`output_schema`, `read_only` and `annotations` for tools, `uri` or `uri_template` and
`mime_type` for resources, and `arguments` for prompts. The request, call, read and prompt
types and the accepted return types match the SDK's; the
[single-file reference](advanced.md#api-routes) lists them. The same server rules apply:
calls run concurrently, a public route may not require scopes, a path must follow the
[path rules](#paths) and may not shadow a core route (the plugin then fails to build with the
message, at the path's line, instead of the server refusing the route at startup), and routes
and entries are fixed when the server starts. A prompt message with another role is
`meerschaum_plugin::mcp::message("assistant", text)`.

## 🧪 Testing

```bash
cargo test -p meerschaum-plugin --test api_mcp           # the call kinds through the ABI
cargo test -p meerschaum-server --test native_extensions  # mounting, auth, refusals, MCP dispatch
cargo test -p meerschaum-cli --test native_api_mcp        # `mrsm start api` and `mrsm start mcp`
```

To try your own plugin, start a server on a scratch root and call a route and a tool:

```bash
mrsm start api -i sql:local --host 127.0.0.1 -p 8224 --no-dash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8224/items/42?field=a
curl -H "Authorization: Bearer $TOKEN" -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' http://127.0.0.1:8224/mcp \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"count_items","arguments":{"kind":"a"}}}'
```
