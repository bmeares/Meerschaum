//! Server contract tests (§14): replay the request/response pairs recorded from the Python v4
//! server (`tests/fixtures/contract.json`, from `gen_contract.py`) against the axum app backed by
//! the in-memory instance seeded identically, and compare statuses and normalized bodies.
//!
//! Normalization mirrors `gen_contract.py`: credentials minted during the run become `{slot}`
//! placeholders, `access_token` / `expires` / `creation` / `secret` / `api_key` values become
//! `<volatile>`, ISO timestamps are rewritten to one canonical form, the server version becomes
//! `<version>`. JSON objects compare without regard to key order.

mod common;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use axum::body::Body;
use axum::http::Request;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use chrono::{DateTime, NaiveDateTime};
use meerschaum_core::{Map, Value};
use meerschaum_server::{AppState, ServerConfig};
use regex::Regex;
use serde_json::{Value as Json, json};

const FIXTURE: &str = include_str!("fixtures/contract.json");
const VOLATILE_KEYS: &[&str] = &["access_token", "expires", "creation", "secret", "api_key"];

static ISO_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^(\d{4}-\d{2}-\d{2})[T ](\d{2}:\d{2}:\d{2})(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$")
        .expect("regex")
});

/// `canonical_datetime()` of `gen_contract.py`.
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
    values: HashMap<String, String>,
    jwts: HashMap<String, String>,
}

impl Slots {
    fn sub_text(&self, text: &str) -> String {
        let mut pairs: Vec<(&String, &String)> = self.values.iter().collect();
        pairs.sort_by_key(|(_, v)| std::cmp::Reverse(v.len()));
        let mut out = text.to_string();
        for (name, value) in pairs {
            if !value.is_empty() {
                out = out.replace(value.as_str(), &format!("{{{name}}}"));
            }
        }
        out
    }

    /// Fill `{slot}` placeholders with this run's values.
    fn fill(&self, text: &str) -> String {
        let mut out = text.to_string();
        for (name, value) in &self.values {
            out = out.replace(&format!("{{{name}}}"), value);
        }
        out
    }
}

fn normalize(v: &Json, key: Option<&str>, slots: &Slots, version: &str) -> Json {
    match v {
        Json::Object(o) => Json::Object(
            o.iter()
                .map(|(k, v)| (k.clone(), normalize(v, Some(k), slots, version)))
                .collect(),
        ),
        Json::Array(a) => Json::Array(
            a.iter()
                .map(|v| normalize(v, None, slots, version))
                .collect(),
        ),
        Json::String(s) => {
            if key.is_some_and(|k| VOLATILE_KEYS.contains(&k)) {
                return json!("<volatile>");
            }
            if s == version {
                return json!("<version>");
            }
            let s = slots.sub_text(s);
            Json::String(canonical_datetime(&s).unwrap_or(s))
        }
        Json::Null => Json::Null,
        _ if key.is_some_and(|k| VOLATILE_KEYS.contains(&k)) => json!("<volatile>"),
        other => other.clone(),
    }
}

/// The rc15 access model's intended changes read as v4's (PARITY.md RC15-P3-002, -003): a pipe
/// registered through the API is its creator's (`parameters.access` with the caller as owner,
/// Everyone's team, the default visibility and edit mode), and registering needs
/// `pipes:register` (`pipes:write` still works).
fn v4_view(v: Json, owner: &str) -> Json {
    let stamped = json!({"owner": owner, "team": null, "visibility": "team", "edit": "team"});
    let text = format!(
        ", \"access\": {{\"owner\": \"{owner}\", \"team\": null, \"visibility\": \"team\", \"edit\": \"team\"}}"
    );
    match v {
        Json::Object(o) => Json::Object(
            o.into_iter()
                .filter(|(k, v)| !(k == "access" && *v == stamped))
                .map(|(k, v)| (k, v4_view(v, owner)))
                .collect(),
        ),
        Json::Array(a) => Json::Array(a.into_iter().map(|v| v4_view(v, owner)).collect()),
        Json::String(s) => Json::String(s.replace(&text, "").replace(
            "Missing required scope: 'pipes:register'",
            "Missing required scope: 'pipes:write'",
        )),
        other => other,
    }
}

/// Exchanges whose v4 answer the access model changes on purpose (PARITY.md RC15-P3-001): the
/// recording's user had every scope (v4: no `scopes` attribute), while an rc15 user without
/// permissions of their own has the default role (`editor`), which creates no jobs and deletes no
/// accounts. `(step, status, detail)`.
const RC15_EXPECTED: &[(&str, u16, &str)] = &[
    (
        "user_create_job_denied",
        403,
        "Missing required scope: 'jobs:write'",
    ),
    (
        "user_delete_other",
        403,
        "Missing required scope: 'users:delete'",
    ),
];

fn same_shape(a: &Json, b: &Json) -> bool {
    std::mem::discriminant(a) == std::mem::discriminant(b)
}

fn seeded_state(fixture: &Json, root: &std::path::Path) -> (AppState, Arc<common::MemoryInstance>) {
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
    // The recording ran with v4's defaults: open self-registration (off by default since rc15).
    cfg.registration_users = true;
    let instance = Arc::new(common::MemoryInstance::new("fixture"));
    instance.seed_user(
        fixture["username"].as_str().unwrap(),
        fixture["password"].as_str().unwrap(),
        Some("admin"),
    );
    let state = AppState::builder(cfg, instance.clone())
        .secret(common::SECRET.to_vec())
        .build()
        .expect("state");
    (state, instance)
}

#[tokio::test]
async fn replay_python_contract() {
    let fixture: Json = serde_json::from_str(FIXTURE).expect("fixture");
    let root = tempfile::tempdir().unwrap();
    let (state, _instance) = seeded_state(&fixture, root.path());
    let version = state.config.version.clone();
    let app = meerschaum_server::app(state);
    let mut slots = Slots::default();
    let mut failures = Vec::new();
    let mut counts: HashMap<String, usize> = HashMap::new();

    for (i, ex) in fixture["exchanges"].as_array().unwrap().iter().enumerate() {
        let step = ex["step"].as_str().unwrap_or("?");
        let method = ex["method"].as_str().unwrap();
        let mut uri = slots.fill(ex["path"].as_str().unwrap());
        let query = slots.fill(ex["query"].as_str().unwrap_or(""));
        if !query.is_empty() {
            uri.push('?');
            uri.push_str(&query);
        }
        let mut b = Request::builder().method(method).uri(&uri);
        for (k, v) in ex["headers"].as_object().unwrap() {
            b = b.header(k.as_str(), v.as_str().unwrap_or(""));
        }
        match &ex["auth"] {
            Json::Object(a) if a.contains_key("slot") => {
                let slot = a["slot"].as_str().unwrap();
                if let Some(jwt) = slots.jwts.get(slot) {
                    b = b.header("authorization", format!("Bearer {jwt}"));
                }
            }
            Json::Object(a) if a.contains_key("raw") => {
                b = b.header("authorization", slots.fill(a["raw"].as_str().unwrap()));
            }
            _ => {}
        }
        let body = match &ex["body"] {
            Json::Object(o) if o.contains_key("text") => {
                Body::from(slots.fill(o["text"].as_str().unwrap()))
            }
            Json::Object(o) if o.contains_key("base64") => {
                Body::from(B64.decode(o["base64"].as_str().unwrap()).unwrap())
            }
            _ => Body::empty(),
        };
        let resp = common::send(&app, b.body(body).unwrap()).await;
        let mode = ex["compare"].as_str().unwrap_or("exact");
        *counts.entry(mode.to_string()).or_default() += 1;

        let expected = &ex["response"];
        let rust_json: Option<Json> = serde_json::from_slice(&resp.body).ok();
        if let (Some(capture), Some(j)) = (ex["capture"].as_object(), rust_json.as_ref()) {
            for (slot, key) in capture {
                if let Some(v) = j.get(key.as_str().unwrap()).and_then(Json::as_str) {
                    match slot.strip_prefix("jwt.") {
                        Some(name) => {
                            slots.jwts.insert(name.to_string(), v.to_string());
                        }
                        None => {
                            slots.values.insert(slot.clone(), v.to_string());
                        }
                    }
                }
            }
        }
        if mode == "skip" {
            continue;
        }
        if let Some((_, status, detail)) = RC15_EXPECTED.iter().find(|(s, _, _)| *s == step) {
            let got = rust_json.clone().unwrap_or(Json::Null);
            if resp.status.as_u16() != *status || got["detail"] != *detail {
                failures.push(format!(
                    "#{i} {step}: {method} {uri}\n  rc15: {} {got} != {status} {detail}",
                    resp.status.as_u16()
                ));
            }
            continue;
        }
        let status = ex["status"].as_u64().unwrap() as u16;
        let mut problems = Vec::new();
        if resp.status.as_u16() != status {
            problems.push(format!("status {} != {status}", resp.status.as_u16()));
        }
        if mode == "exact" || mode == "shape" {
            if let Some(exp) = expected.get("json") {
                let owner = fixture["username"].as_str().unwrap_or_default();
                let got = rust_json
                    .as_ref()
                    .map(|j| v4_view(normalize(j, None, &slots, &version), owner))
                    .unwrap_or(Json::String(resp.text()));
                let ok = if mode == "exact" {
                    &got == exp
                } else {
                    same_shape(&got, exp)
                };
                if !ok {
                    problems.push(format!("body\n      got: {got}\n expected: {exp}"));
                }
            } else if let Some(exp) = expected.get("text") {
                let got = slots.sub_text(&resp.text());
                if mode == "exact" && got != exp.as_str().unwrap() {
                    problems.push(format!("text\n      got: {got:?}\n expected: {exp}"));
                }
            } else if let Some(exp) = expected.get("base64") {
                let exp = B64.decode(exp.as_str().unwrap()).unwrap();
                if mode == "exact" && resp.body.as_ref() != exp.as_slice() {
                    problems.push(format!(
                        "bytes differ ({} vs {})",
                        resp.body.len(),
                        exp.len()
                    ));
                }
            }
            if let Some(cd) = ex["content_disposition"].as_str() {
                let got = resp
                    .headers
                    .get("content-disposition")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                if got != cd {
                    problems.push(format!("content-disposition {got:?} != {cd:?}"));
                }
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
        }
        if !problems.is_empty() {
            failures.push(format!(
                "#{i} {step}: {method} {uri}\n  {}",
                problems.join("\n  ")
            ));
        }
    }
    eprintln!("compared exchanges by mode: {counts:?}");
    assert!(
        failures.is_empty(),
        "{} of {} exchanges differ:\n{}",
        failures.len(),
        fixture["exchanges"].as_array().unwrap().len(),
        failures.join("\n")
    );
}

#[test]
fn canonical_datetimes() {
    assert_eq!(
        canonical_datetime("2024-01-01T00:00:00Z").unwrap(),
        "2024-01-01T00:00:00.000000+00:00"
    );
    assert_eq!(
        canonical_datetime("2024-01-01 02:00:00.5+02:00").unwrap(),
        "2024-01-01T00:00:00.500000+00:00"
    );
    assert_eq!(
        canonical_datetime("2024-01-01T00:00:00").unwrap(),
        "2024-01-01T00:00:00.000000"
    );
    assert!(canonical_datetime("a").is_none());
}
