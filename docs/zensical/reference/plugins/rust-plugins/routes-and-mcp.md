# Routes and MCP in Rust Plugins

A Rust plugin can serve **HTTP routes** (counterpart of `@api_plugin`) and **MCP tools, resources, and prompts** (counterpart of `meerschaum.mcp.tool(...)`). Both are declared in the manifest for discovery without loading the library and run in-process without Python.

Full tested example: [`native_api.rs`](https://github.com/bmeares/Meerschaum/blob/v5/crates/meerschaum-plugin/examples/native_api.rs).

## 🛣️ Routes

Declare routes under `api_routes` in `manifest()` and implement `Plugin::api_route`:

```rust
use meerschaum_plugin::api::{RouteRequest, RouteResponse, RouteSpec};
use meerschaum_plugin::prelude::*;

#[derive(Default)]
struct Items;

impl Plugin for Items {
    fn manifest(&self) -> Value {
        Manifest::new()
            .api_route(RouteSpec::new("get_item", "GET", "/items/{id}").scopes(["pipes:read"]))
            .api_route(RouteSpec::new("upload", "PUT", "/items/files/{*key}").scopes(["pipes:write"]))
            .api_route(RouteSpec::new("health", "GET", "/items-health").public())
            .into()
    }

    fn api_route(&self, req: &RouteRequest) -> Result<RouteResponse> {
        Ok(match req.name.as_str() {
            "get_item" => RouteResponse::json(&json!({
                "id": req.path_param("id"), "caller": req.principal,
            })),
            "upload" => RouteResponse::empty(204).header("x-stored", req.body.len().to_string()),
            _ => RouteResponse::text("ok"),
        })
    }
}

meerschaum_plugin::export_plugin!(Items);
```

### Route options

| Key | Meaning |
|---|---|
| `name` | Identifier returned on `request.name`. |
| `method` | Any HTTP method or `*` (default `GET`). `HEAD` falls back to `GET`; unmatched methods return `405` + `Allow`. |
| `path` | Starlette-style path (see [Paths](#paths)). |
| `scopes` | Required scopes (e.g. `pipes:read`; `*` matches all). Uses core API auth (`401`/`403`; open under `--no-auth`). |
| `public` | Serve without auth (still passes `principal` if valid credentials are sent). Cannot combine with `scopes`. |
| `description` | Optional description. |

### Paths

Starlette syntax, one parameter per segment (percent-decoded, matched in declaration order):

| Segment | Matches |
|---|---|
| `items` | Literal text. |
| `{id}` / `{id:str}` | Any non-empty segment. |
| `{id}.json`, `v{n}` | Parameter with literal prefix/suffix. |
| `{n:int}`, `{x:float}`, `{u:uuid}` | Digits, decimal, or hyphenated UUID (passed as strings). |
| `{*rest}` / `{rest:path}` | Rest of path including `/` (final segment only, non-empty). |

### Refused routes

At startup, the server skips (with a stderr warning) routes that shadow core groups (`/pipes`, `/users`, `/actions`, `/jobs`, `/engine`, `/mcp`, `/login`, `/docs`, `/`, `/dash`, `/_pages`, `/static`, `/_mrsm/`), put a parameter in the first segment (`/{x}/...`), conflict with an earlier route, combine `public` + `scopes`, fall under disabled `api:permissions:routes`, or have invalid syntax (`..`, `%`, non-final catch-all).

### Request and response

| `RouteRequest` field | Contents & helpers |
|---|---|
| `name`, `route_path`, `method`, `path` | Matched route info and uppercase HTTP method. |
| `path_params`, `query`, `query_params` | Decoded path/query maps (`path_param("id")`, `query_param("q")`). |
| `headers` | Lowercase header map (`header("X-Name")`, `cookie("theme")`); `authorization` and `mrsm-session-id` stripped. |
| `body` | Raw bytes up to 64 MiB (`413` if larger); `text()`, `json::<T>()`. |
| `principal`, `scopes` | `None` if anonymous, else user/token identity (no secrets); `has_scope("x")`. |

`RouteResponse` builders (`Err` from `api_route` returns `500 {"detail": "..."}`):

```rust
RouteResponse::json(&value)
RouteResponse::text("hello").with_status(307).header("location", "/items/1")
RouteResponse::bytes("image/png", png_bytes)
RouteResponse::empty(204)
RouteResponse::error(404, "No such item.")
```

## ⚙️ How calls run

- **Concurrent:** `api_route`, `mcp_tool`, `mcp_resource`, and `mcp_prompt` run in parallel on separate threads (fresh `P::default()` per call; shared state must be thread-safe).
- **Caller-scoped host calls:** [Host calls](advanced.md#talking-to-meerschaum) inherit the caller's scopes, visibility, and allowed instances, and block `mrsm_*` internal tables. Public routes may add non-admin scopes via `host_scopes = ["pipes:read"]` (`RouteSpec::host_scopes(...)`).
- **Bounded:** Max 32 active calls per plugin (routes and MCP separately) + 16 queued up to 30s; overflow returns `503` + `Retry-After: 1` without buffering bodies.
- **Fixed at startup:** Restart `mrsm start api` or `mrsm start mcp` after updating native plugins.

## 🔑 Tokens and draining

In 5.1.0+, isolated-team callers using confined native routes need `plugins:read`, the route's built-in scopes, and an attested offline Rust compiler; non-isolated callers (`Everyone`, portal users, device tokens) use the host runtime below.

Use `host::mint_token(label, scopes, expiration)`, `host::revoke_token(id)`, and `host::draining()` (Meerschaum 5.0.0rc34+) for device token rotation and graceful long-poll shutdown:

```rust
#[api_route(POST, "/devices/heartbeat", scopes = ["devices:ingest"])]
fn heartbeat(req: &RouteRequest) -> Result<Value> {
    let caller = req.principal.clone().unwrap_or_default();
    let next = host::mint_token(caller.label.as_deref().unwrap_or_default(), &["devices:ingest"], None)?;
    host::revoke_token(caller.token_id.as_deref().unwrap_or_default())?;
    Ok(next.to_json())
}

#[api_route(GET, "/devices/commands", scopes = ["devices:ingest"])]
fn commands() -> Result<Value> {
    let deadline = std::time::Instant::now() + Duration::from_secs(25);
    while !host::draining()? && std::time::Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    Ok(json!({"commands": []}))
}
```

- **Auth required:** Only signed-in API requests can mint (never anonymous or `--no-auth`). Minted tokens belong to the caller's user (or calling token's owner).
- **Plugin-owned scopes:** Can only mint/revoke custom scopes declared by the same plugin (never `*`, core scopes, or other plugins' scopes) that the caller holds. Revoked tokens immediately fail (`401`) on subsequent calls and active host calls.
- **Draining:** `host::draining()` becomes `true` when shutdown starts (before `api.shutdown.grace_seconds`) and is `false` outside API requests.

## 🤖 MCP tools, resources and prompts

Declare `mcp_tools`, `mcp_resources`, and `mcp_prompts` in `manifest()` and implement `mcp_tool`, `mcp_resource`, and `mcp_prompt`:

```rust
use meerschaum_plugin::mcp::{PromptGet, ResourceContents, ResourceRead, ToolCall, message};

fn mcp_tool(&self, call: &ToolCall) -> Result<Value> {
    Ok(json!({"kind": call.argument("kind"), "count": 3}))
}
fn mcp_resource(&self, read: &ResourceRead) -> Result<ResourceContents> {
    Ok(ResourceContents::json(&json!({"id": read.variable("id")})))
}
fn mcp_prompt(&self, get: &PromptGet) -> Result<Value> {
    Ok(json!({"messages": [message("user", format!("Summarize {}.", get.argument("id").unwrap_or("?")))]}))
}
```

| Section | Manifest keys | Input | Return |
|---|---|---|---|
| `mcp_tools` | `name`, `description`, `input_schema`, `output_schema`?, `scopes`, `title`?, `annotations`? | `ToolCall` | JSON (strings as text; objects also fill `structuredContent` if `output_schema` is set). |
| `mcp_resources` | `name`, `description`, `uri` or `uri_template`, `mime_type`?, `scopes`, `title`? | `ResourceRead` | `ResourceContents::text`, `::json`, or `::blob`. |
| `mcp_prompts` | `name`, `description`?, `title`?, `arguments`, `scopes` | `PromptGet` | String, message list, or `{"description"?, "messages"}`. |

Entries missing caller `scopes` are hidden (`*` over stdio); `api:mcp:read_only` hides non-`readOnlyHint` tools and mutating prompts. Arguments validate against `input_schema` / `arguments`. Native entries never shadow built-ins (skipped with stderr warning) but override Python plugin entries. Tool `Err` becomes `isError: true`; resource/prompt `Err` becomes a JSON-RPC error.

## 📄 Single-file plugins

Decorate top-level functions in `.rs` plugins (doc comments set descriptions; paths and schemas validate at build time):

```rust
/// Fetch one item.
#[api_route(GET, "/items/{id:int}", scopes = ["pipes:read"])]
fn item(req: &RouteRequest) -> Value { json!({"id": req.path_param("id")}) }

/// Add two integers.
#[mcp_tool(read_only = true, input_schema = r#"{"type":"object","properties":{"a":{"type":"integer"},"b":{"type":"integer"}},"required":["a","b"]}"#)]
fn add(arguments: &Value) -> Value { json!({"sum": arguments["a"].as_i64().unwrap_or(0) + arguments["b"].as_i64().unwrap_or(0)}) }

/// One item resource.
#[mcp_resource(uri_template = "items://{id}")]
fn item_resource(res: &ResourceRead) -> ResourceContents { ResourceContents::json(&json!({"id": res.variable("id")})) }

#[mcp_prompt(description = "Greet someone.", arguments = ["who"])]
fn greet(prompt: &PromptGet) -> String { format!("Say hello to {}.", prompt.argument("who").unwrap_or("nobody")) }
```

| Attribute | Key options | Parameters | Returns (also `Result<T>`) |
|---|---|---|---|
| `#[api_route]` | Method (default `GET`), path, `scopes`, `public`, `host_scopes`, `name`, `description` | `request: &RouteRequest` (optional) | `RouteResponse`, `Value`, `String`/`&str`, `Vec<u8>`, `(u16, T)` |
| `#[mcp_tool]` | `input_schema`, `output_schema`, `read_only`, `annotations`, `name`, `title`, `description`, `scopes` | `call: &ToolCall` or `arguments: &Value` | Any serializable value, `Value`, `String` |
| `#[mcp_resource]` | `uri` or `uri_template`, `mime_type`, `name`, `title`, `description`, `scopes` | `resource: &ResourceRead` | `ResourceContents`, `String`, `Value` |
| `#[mcp_prompt]` | `arguments`, `name`, `title`, `description`, `scopes` | `prompt: &PromptGet` or `arguments: &Value` | `String`, `Vec<Value>`, `Value` |

## 🧱 Low-level ABI & testing

Custom `Plugin::call` handlers can forward `api_route`, `mcp_tool`, `mcp_resource`, and `mcp_prompt` via `meerschaum_plugin::sdk::call_server_extension` (legacy `api_plugins` `routes`/`route` handlers remain supported for static text routes, but prefer `api_routes`).

```bash
mrsm start api -i sql:local --host 127.0.0.1 -p 8224 --no-dash
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:8224/items/42
curl -H "Authorization: Bearer $TOKEN" -H 'Accept: application/json, text/event-stream' \
  -H 'Content-Type: application/json' http://127.0.0.1:8224/mcp \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"count_items","arguments":{"kind":"a"}}}'
```
