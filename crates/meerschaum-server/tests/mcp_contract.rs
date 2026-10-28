//! MCP contract tests (§7.2, §14): replay the JSON-RPC exchanges recorded from the Python v4 API
//! server (`tests/fixtures/mcp.json`, from `gen_mcp.py`) against the Rust MCP server, backed by
//! the in-memory instance seeded identically, and compare statuses and normalized bodies.
//!
//! Normalization is `contract.rs`'s (credentials become `{slot}`s, volatile keys `<volatile>`,
//! ISO timestamps one canonical form, the version `<version>`), plus: a `text` field holding
//! JSON (a tool result or a JSON resource) is compared as the parsed document. `shape`
//! exchanges compare the JSON structure recursively (keys and value types, not values).
//!
//! v5 adds to v4's registry (`mcp::v5`: new tools, `sync_pipe`'s `parents` / `children`, bounds
//! typed as strings or integers, a primer that mentions Pipelines): each served entry is first
//! checked to be exactly the v5 one and then mapped back to v4's ([`v4_view`]), so every other
//! byte is still compared with the recording. The one paged listing (`tools_list_cursor`) is
//! checked against the v5 list instead, since v5's tools move the page.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use async_trait::async_trait;
use axum::body::Body;
use axum::http::Request;
use chrono::{DateTime, NaiveDateTime};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_server::backends::ActionsBackend;
use meerschaum_server::mcp::registry::{Tool, builtin, primer, tools};
use meerschaum_server::mcp::{McpBackend, McpConfig, McpServer, SqlReadError, docs, v5};
use meerschaum_server::{AppState, ServerConfig};
use regex::Regex;
use serde_json::{Value as Json, json};

const FIXTURE: &str = include_str!("fixtures/mcp.json");
const VOLATILE_KEYS: &[&str] = &["access_token", "expires", "creation", "secret", "api_key"];

static ISO_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$")
        .expect("regex")
});

fn canonical_datetime(s: &str) -> Option<String> {
    let c = ISO_RE.captures(s)?;
    let frac = c.get(3).map(|m| &m.as_str()[1..]).unwrap_or("0");
    let micros: String = format!("{frac}000000").chars().take(6).collect();
    let naive = NaiveDateTime::parse_from_str(
        &format!("{}T{}.{micros}", &c[1], &c[2]),
        "%Y-%m-%dT%H:%M:%S%.6f",
    )
    .ok()?;
    match c.get(4).map(|m| m.as_str()) {
        None => Some(naive.format("%Y-%m-%dT%H:%M:%S%.6f").to_string()),
        Some(tz) => {
            let tz = if tz == "Z" {
                "+00:00".to_string()
            } else if tz.len() == 5 {
                format!("{}:{}", &tz[..3], &tz[3..])
            } else {
                tz.to_string()
            };
            let dt = DateTime::parse_from_str(
                &format!("{}{tz}", naive.format("%Y-%m-%dT%H:%M:%S%.6f")),
                "%Y-%m-%dT%H:%M:%S%.6f%:z",
            )
            .ok()?;
            Some(format!(
                "{}+00:00",
                dt.naive_utc().format("%Y-%m-%dT%H:%M:%S%.6f")
            ))
        }
    }
}

#[derive(Default)]
struct Slots {
    jwts: HashMap<String, String>,
}

fn normalize(v: &Json, key: Option<&str>, version: &str) -> Json {
    match v {
        Json::Object(o) => Json::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), normalize(v, Some(k), version)))
                .collect(),
        ),
        Json::Array(a) => Json::Array(a.iter().map(|v| normalize(v, None, version)).collect()),
        Json::String(s) => {
            if key.is_some_and(|k| VOLATILE_KEYS.contains(&k)) {
                return json!("<volatile>");
            }
            if s == version || s == "<version>" {
                return json!("<version>");
            }
            if key == Some("text")
                && let Ok(parsed @ (Json::Object(_) | Json::Array(_))) =
                    serde_json::from_str::<Json>(s)
            {
                return json!({"<json>": normalize(&parsed, None, version)});
            }
            Json::String(canonical_datetime(s).unwrap_or_else(|| s.clone()))
        }
        Json::Null => Json::Null,
        _ if key.is_some_and(|k| VOLATILE_KEYS.contains(&k)) => json!("<volatile>"),
        other => other.clone(),
    }
}

/// Recursive structural equality: same keys and value types; arrays compare their first items.
fn same_shape(a: &Json, b: &Json) -> bool {
    match (a, b) {
        (Json::Object(x), Json::Object(y)) => {
            x.len() == y.len()
                && x.iter()
                    .all(|(k, v)| y.get(k).is_some_and(|w| same_shape(v, w)))
        }
        (Json::Array(x), Json::Array(y)) => match (x.first(), y.first()) {
            (Some(p), Some(q)) => same_shape(p, q),
            _ => true,
        },
        // A nullable field may be null on one side (e.g. a sync time the instance rounds).
        (Json::Null, _) | (_, Json::Null) => true,
        (Json::Number(_), Json::Number(_)) => true,
        _ => std::mem::discriminant(a) == std::mem::discriminant(b),
    }
}

fn tool_json(t: &Tool) -> Json {
    serde_json::to_value(t.to_dict()).unwrap()
}

/// v4's version of a served tool list: v5's new tools dropped, each v5 tool (checked to be
/// exactly the served one) replaced by v4's.
fn v4_tools(list: &mut Vec<Json>) {
    list.retain(|t| !v5::NEW_TOOLS.contains(&t["name"].as_str().unwrap_or("")));
    for t in list.iter_mut() {
        let name = t["name"].as_str().unwrap_or("").to_string();
        let served = tools().iter().find(|e| e.name == name).map(tool_json);
        if served.as_ref() == Some(t)
            && let Some(v4) = builtin().tools.iter().find(|e| e.name == name)
        {
            *t = tool_json(v4);
        }
    }
}

/// v4's view of a v5 response (see the module docs): tools, the primer and the reference.
fn v4_view(got: &mut Json, read_only: bool) {
    // RC15-P3-003: registration has its own permission. The recorded missing-scope
    // exchange still checks the same refusal, whose message now names that permission.
    if let Some(text) = got.pointer_mut("/result/content/0/text")
        && text.as_str() == Some("Missing required scope for 'register_pipe': pipes:register.")
    {
        *text = json!("Missing required scope for 'register_pipe': pipes:write.");
    }
    if let Some(Json::Array(list)) = got.pointer_mut("/result/tools") {
        v4_tools(list);
    }
    if let Some(Json::Array(list)) = got.get_mut("tools") {
        v4_tools(list);
    }
    for pointer in ["/result/instructions", "/instructions"] {
        if let Some(text) = got.pointer_mut(pointer)
            && text.as_str() == Some(primer())
        {
            *text = json!(builtin().primer);
        }
    }
    let config = McpConfig {
        read_only,
        ..McpConfig::default()
    };
    let b = builtin();
    let v5_reference = docs::render(tools(), &b.resources, &b.prompts, primer(), &config);
    if let Some(Json::Array(contents)) = got.pointer_mut("/result/contents") {
        for item in contents {
            if item["uri"] == "mrsm://docs" && item["text"].as_str() == Some(&v5_reference) {
                item["text"] = json!(docs::render(
                    &b.tools,
                    &b.resources,
                    &b.prompts,
                    &b.primer,
                    &config
                ));
            }
        }
    }
}

/// rc15's access model (PARITY.md RC15-P3-002): a pipe registered through the API is its
/// creator's, so its parameters gain `access` with the caller as owner (Everyone's team, the
/// default visibility and edit mode), which v4 never stored. Read as v4's answer.
fn without_stamped_access(v: Json, owner: &str) -> Json {
    let stamped = json!({"owner": owner, "team": null, "visibility": "team", "edit": "team"});
    match v {
        Json::Object(o) => Json::Object(
            o.into_iter()
                .filter(|(k, v)| !(k == "access" && *v == stamped))
                .map(|(k, v)| (k, without_stamped_access(v, owner)))
                .collect(),
        ),
        Json::Array(a) => Json::Array(
            a.into_iter()
                .map(|v| without_stamped_access(v, owner))
                .collect(),
        ),
        other => other,
    }
}

/// The action table of the recorded server; every action "succeeds".
struct FixtureActions(Vec<String>);

#[async_trait]
impl ActionsBackend for FixtureActions {
    fn list(&self) -> Vec<String> {
        self.0.clone()
    }
    async fn execute(&self, _action: &str, _keywords: Map) -> SuccessTuple {
        SuccessTuple::success()
    }
}

/// Source syncs succeed; `read_sql` answers the recorded `SELECT`.
struct FixtureBackend;

#[async_trait]
impl McpBackend for FixtureBackend {
    async fn sync_pipe(&self, pipe: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::ok(format!("{pipe} does not support fetching; nothing to do."))
    }
    async fn verify_pipe(&self, _: &Pipe, _: Option<String>, _: Option<String>) -> SuccessTuple {
        SuccessTuple::ok("Successfully synced 2 chunks.")
    }
    async fn read_sql(&self, _: &str, query: &str, _: usize) -> Result<Vec<Value>, SqlReadError> {
        if query.starts_with("SELECT 1 AS one") {
            let row: Map = [
                ("one".to_string(), Value::Int(1)),
                ("two".to_string(), Value::Str("a;b".into())),
            ]
            .into_iter()
            .collect();
            return Ok(vec![Value::Map(row)]);
        }
        Err(SqlReadError::Failed)
    }
}

fn config(fixture: &Json, root: &std::path::Path, read_only: bool) -> ServerConfig {
    let mut cfg: ServerConfig = common::config(root);
    let mut connectors = Map::new();
    for (typ, labels) in fixture["connectors"].as_object().expect("connectors") {
        let mut m = Map::new();
        for l in labels.as_array().expect("labels") {
            m.insert(l.as_str().unwrap_or("").to_string(), Value::new_map());
        }
        connectors.insert(typ.clone(), Value::Map(m));
    }
    cfg.connectors = connectors;
    cfg.no_dash = true;
    // The recording serves its default instance (`meerschaum:instance`).
    cfg.pipe_defaults.instance_keys = cfg.instance_keys.clone();
    cfg.mcp.read_only = read_only;
    cfg
}

#[tokio::test]
async fn replay_python_mcp_contract() {
    let fixture: Json = serde_json::from_str(FIXTURE).expect("fixture");
    let root = tempfile::tempdir().unwrap();
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user(
        fixture["username"].as_str().unwrap(),
        fixture["password"].as_str().unwrap(),
        Some("admin"),
    );
    let actions: Vec<String> = fixture["actions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap().to_string())
        .collect();
    let mut apps = HashMap::new();
    let mut version = String::new();
    for (name, read_only) in [("default", false), ("read_only", true)] {
        let state = AppState::builder(config(&fixture, root.path(), read_only), instance.clone())
            .secret(common::SECRET.to_vec())
            .actions(Arc::new(FixtureActions(actions.clone())))
            .build()
            .expect("state");
        version = state.config.version.clone();
        let mcp = Arc::new(McpServer::for_state(&state).with_backend(Arc::new(FixtureBackend)));
        apps.insert(name, meerschaum_server::app_with_mcp(state, mcp));
    }
    let mut slots = Slots::default();
    let mut failures = Vec::new();
    let mut counts: HashMap<String, usize> = HashMap::new();

    for (i, ex) in fixture["exchanges"].as_array().unwrap().iter().enumerate() {
        let step = ex["step"].as_str().unwrap_or("?");
        let app = &apps[ex["server"].as_str().unwrap_or("default")];
        let method = ex["method"].as_str().unwrap();
        let mut uri = ex["path"].as_str().unwrap().to_string();
        let query = ex["query"].as_str().unwrap_or("");
        if !query.is_empty() {
            uri.push('?');
            uri.push_str(query);
        }
        let mut b = Request::builder().method(method).uri(&uri);
        for (k, v) in ex["headers"].as_object().unwrap() {
            b = b.header(k.as_str(), v.as_str().unwrap_or(""));
        }
        match &ex["auth"] {
            Json::Object(a) if a.contains_key("slot") => {
                if let Some(jwt) = slots.jwts.get(a["slot"].as_str().unwrap()) {
                    b = b.header("authorization", format!("Bearer {jwt}"));
                }
            }
            Json::Object(a) if a.contains_key("raw") => {
                b = b.header("authorization", a["raw"].as_str().unwrap());
            }
            _ => {}
        }
        let body = match &ex["body"] {
            Json::Object(o) if o.contains_key("text") => {
                Body::from(o["text"].as_str().unwrap().to_string())
            }
            _ => Body::empty(),
        };
        let resp = common::send(app, b.body(body).unwrap()).await;
        let mode = ex["compare"].as_str().unwrap_or("exact");
        *counts.entry(mode.to_string()).or_default() += 1;
        let mut rust_json: Option<Json> = serde_json::from_slice(&resp.body).ok();
        if step == "tools_list_cursor" {
            // The page after the first ten of every tool (an administrator sees them all).
            let mut all: Vec<Json> = tools().iter().map(tool_json).collect();
            all.sort_by(|a, b| a["name"].as_str().cmp(&b["name"].as_str()));
            let got = rust_json.as_ref().map(|j| j["result"]["tools"].clone());
            if got != Some(Json::Array(all[10..].to_vec())) {
                failures.push(format!(
                    "#{i} {step}: not the v5 page after ten tools: {got:?}"
                ));
            }
            *counts.entry("v5".to_string()).or_default() += 1;
            continue;
        }
        if let Some(j) = rust_json.as_mut() {
            v4_view(j, ex["server"].as_str() == Some("read_only"));
        }
        if let (Some(capture), Some(j)) = (ex["capture"].as_object(), rust_json.as_ref()) {
            for (slot, key) in capture {
                if let (Some(name), Some(v)) = (
                    slot.strip_prefix("jwt."),
                    j.get(key.as_str().unwrap()).and_then(Json::as_str),
                ) {
                    slots.jwts.insert(name.to_string(), v.to_string());
                }
            }
        }
        let status = ex["status"].as_u64().unwrap() as u16;
        let mut problems = Vec::new();
        if resp.status.as_u16() != status {
            problems.push(format!("status {} != {status}", resp.status.as_u16()));
        }
        let expected = ex["response"]
            .get("json")
            .map(|e| normalize(e, None, &version));
        match (expected, rust_json.as_ref()) {
            (Some(Json::Null), _) if resp.body.is_empty() => {}
            (Some(exp), Some(got)) => {
                let owner = fixture["username"].as_str().unwrap_or_default();
                let got = without_stamped_access(normalize(got, None, &version), owner);
                let ok = match mode {
                    "exact" => got == exp,
                    "shape" => same_shape(&got, &exp),
                    _ => true,
                };
                if !ok {
                    problems.push(format!("body\n      got: {got}\n expected: {exp}"));
                }
            }
            (Some(exp), None) => {
                problems.push(format!(
                    "body\n      got: {:?}\n expected: {exp}",
                    resp.text()
                ));
            }
            (None, _) => {}
        }
        let ct = ex["content_type"].as_str().unwrap_or("");
        let got_ct = resp
            .headers
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .split(';')
            .next()
            .unwrap_or("")
            .to_string();
        if got_ct != ct {
            problems.push(format!("content-type {got_ct:?} != {ct:?}"));
        }
        if !problems.is_empty() {
            failures.push(format!(
                "#{i} {step}: {method} {uri}\n  {}",
                problems.join("\n  ")
            ));
        }
    }
    eprintln!("compared MCP exchanges by mode: {counts:?}");
    assert!(
        failures.is_empty(),
        "{} of {} exchanges differ:\n{}",
        failures.len(),
        fixture["exchanges"].as_array().unwrap().len(),
        failures.join("\n")
    );
}

#[test]
fn shapes() {
    assert!(same_shape(&json!({"a": [1, 2]}), &json!({"a": [3]})));
    assert!(!same_shape(&json!({"a": 1}), &json!({"b": 1})));
    assert!(!same_shape(&json!({"a": "x"}), &json!({"a": true})));
    assert_eq!(
        canonical_datetime("2024-01-01T00:00:00Z").unwrap(),
        "2024-01-01T00:00:00.000000+00:00"
    );
}
