//! HTTP contract tests for the `APIConnector`, against `fixtures/api_sequence.json` (recorded by
//! `fixtures/gen_api.py` from the Python client talking to the Python API server).
//!
//! - `contract_replay_mock` (offline): a scripted server answers with the recorded responses;
//!   every request the Rust client sends must equal the Python client's (method, path, query,
//!   `Content-Type`, presence of `Authorization`, body), and every parsed result must equal the
//!   Python method's return value.
//! - `contract_replay_live`: when `MRSM_API_CONTRACT_URL` points at a fresh server started with
//!   `gen_api.py --serve`, the same sequence runs through a recording proxy to the real server.
//!   Requests are compared exactly; results are compared where they do not depend on state the
//!   Python sequence created outside the client (the directly registered token). Skipped with a
//!   message otherwise.

mod common;

use base64::Engine;
use common::{Canned, MockServer, Recorded};
use meerschaum_api::APIConnector;
use meerschaum_api::client::{LogsMonitor, RequestOptions};
use meerschaum_api::traits::{
    DataQuery, DateBound, PipeId, PipesKeys, PipesKeysQuery, PluginRecord, SyncOptions,
};
use meerschaum_api::wire::QueryValue;
use meerschaum_core::users::{Expiration, Token, User};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::frame::{Column, ColumnData, values_to_series};
use meerschaum_kernel::value::{Value as KValue, parse_datetime_value};
use serde_json::{Value as J, json};
use std::sync::{Arc, Mutex};

fn fixture() -> J {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/api_sequence.json"
    );
    serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
}

fn b64(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD.decode(s).unwrap()
}

fn body_bytes(j: &J) -> Vec<u8> {
    match j {
        J::Null => Vec::new(),
        J::Object(o) if o.contains_key("text") => o["text"].as_str().unwrap().as_bytes().to_vec(),
        J::Object(o) => b64(o["base64"].as_str().unwrap()),
        _ => Vec::new(),
    }
}

/// The canned response for a recorded request; `/login` expirations are moved far into the
/// future so the replay never re-authenticates.
fn canned(r: &J) -> Canned {
    let mut body = if r["response"].get("reset").is_some() {
        Vec::new()
    } else {
        body_bytes(&r["response"])
    };
    if r["url"].as_str().unwrap().ends_with("/login") && r["status"] == 200 {
        let mut j: J = serde_json::from_slice(&body).unwrap();
        j["expires"] = J::String("2999-01-01T00:00:00.000000".into());
        body = serde_json::to_vec(&j).unwrap();
    }
    Canned {
        // `null`: the server reset the connection (status 0 makes the mock drop it).
        status: r["status"].as_u64().unwrap_or(0) as u16,
        content_type: r["content_type"].as_str().unwrap().to_string(),
        body,
    }
}

fn target_of(url: &str) -> String {
    let rest = url.split_once("://").map(|x| x.1).unwrap_or(url);
    rest.find('/')
        .map(|i| rest[i..].to_string())
        .unwrap_or_else(|| "/".into())
}

fn boundary(content_type: &str) -> Option<String> {
    content_type
        .split("boundary=")
        .nth(1)
        .map(|b| b.trim_matches('"').to_string())
}

fn replace_bytes(data: &[u8], from: &[u8], to: &[u8]) -> Vec<u8> {
    if from.is_empty() {
        return data.to_vec();
    }
    let mut out = Vec::new();
    let mut i = 0;
    while i < data.len() {
        if data[i..].starts_with(from) {
            out.extend_from_slice(to);
            i += from.len();
        } else {
            out.push(data[i]);
            i += 1;
        }
    }
    out
}

fn compare_request(step: &str, i: usize, expected: &J, actual: &Recorded) -> Vec<String> {
    let mut errs = Vec::new();
    if expected["method"] != actual.method.as_str() {
        errs.push(format!(
            "{step}[{i}] method {} != {}",
            expected["method"], actual.method
        ));
    }
    let et = target_of(expected["url"].as_str().unwrap());
    if et != actual.target {
        errs.push(format!(
            "{step}[{i}] target\n    python {et}\n    rust   {}",
            actual.target
        ));
    }
    let ect = expected["headers"]
        .as_object()
        .and_then(|h| {
            h.iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("content-type"))
        })
        .map(|(_, v)| v.as_str().unwrap().to_string());
    let act = actual.header("content-type").map(String::from);
    let (eb, ab) = (
        ect.as_deref().and_then(boundary),
        act.as_deref().and_then(boundary),
    );
    let norm_ct = |c: &Option<String>| {
        c.as_ref()
            .map(|c| c.split("boundary=").next().unwrap().trim().to_string())
    };
    if norm_ct(&ect) != norm_ct(&act) {
        errs.push(format!("{step}[{i}] content-type {ect:?} != {act:?}"));
    }
    let e_auth = expected["has_auth"].as_bool().unwrap();
    let a_auth = actual.header("authorization").is_some();
    if e_auth != a_auth {
        errs.push(format!(
            "{step}[{i}] authorization present {e_auth} != {a_auth}"
        ));
    }
    let mut ebody = body_bytes(&expected["body"]);
    let mut abody = actual.body.clone();
    if let (Some(eb), Some(ab)) = (eb, ab) {
        ebody = replace_bytes(&ebody, eb.as_bytes(), b"BOUNDARY");
        abody = replace_bytes(&abody, ab.as_bytes(), b"BOUNDARY");
    }
    if ebody != abody {
        errs.push(format!(
            "{step}[{i}] body\n    python {}\n    rust   {}",
            String::from_utf8_lossy(&ebody),
            String::from_utf8_lossy(&abody)
        ));
    }
    errs
}

// ------------------------------------------------------------------------------------------------
// Encoding Rust results like the generator's `tag()`.
// ------------------------------------------------------------------------------------------------

fn tuple(t: SuccessTuple) -> J {
    json!([t.success, t.message])
}

fn core_json(v: &Value) -> J {
    serde_json::to_value(v).unwrap()
}

fn ktag(v: &KValue) -> J {
    match v {
        v if v.is_na() => J::Null,
        KValue::Bool(b) => json!(b),
        KValue::Int(i) => json!(i),
        KValue::Float(f) => json!(f),
        KValue::Str(s) => json!(s),
        KValue::Datetime(d) => json!({"t": "dt", "v": d.isoformat()}),
        KValue::Uuid(u) => json!({"t": "uuid", "v": u.to_string()}),
        KValue::Json(j) => j.clone(),
        other => json!(other.py_str()),
    }
}

fn frame_json(df: &Frame) -> J {
    let mut data = serde_json::Map::new();
    let mut dtypes = serde_json::Map::new();
    for c in df.columns() {
        dtypes.insert(c.name.clone(), json!(c.data.dtype_str()));
        data.insert(
            c.name.clone(),
            J::Array(c.data.values().iter().map(ktag).collect()),
        );
    }
    json!({"__df__": {"columns": df.names(), "dtypes": dtypes, "data": data}})
}

fn bound_json(b: Option<DateBound>) -> J {
    match b {
        Some(DateBound::Datetime(d)) => json!({"t": "dt", "v": d.isoformat()}),
        Some(DateBound::Int(i)) => json!(i),
        None => J::Null,
    }
}

fn keys_json(k: PipesKeys) -> J {
    match k {
        PipesKeys::Dict(m) => J::Object(
            m.iter()
                .map(|(id, e)| (id.to_string(), core_json(&Value::List(e.to_list()))))
                .collect(),
        ),
        PipesKeys::List(v) => J::Array(
            v.iter()
                .map(|e| core_json(&Value::List(e.to_list())))
                .collect(),
        ),
    }
}

fn token_json(m: &meerschaum_api::models::TokenModel) -> J {
    json!({
        "id": m.id.map(|u| json!({"t": "uuid", "v": u.to_string()})).unwrap_or(J::Null),
        "label": m.label,
        "scopes": m.scopes,
        "is_valid": m.is_valid,
    })
}

/// Normalize for comparisons: nulls, integral floats, sync durations, token models' unstable
/// fields.
fn norm(j: &J) -> J {
    match j {
        J::Object(o)
            if matches!(
                o.get("t").and_then(|t| t.as_str()),
                Some("na" | "nan" | "nat")
            ) =>
        {
            J::Null
        }
        J::Object(o) => J::Object(o.iter().map(|(k, v)| (k.clone(), norm(v))).collect()),
        J::Array(a) => J::Array(a.iter().map(norm).collect()),
        J::String(s) if s.starts_with("It took ") => J::String(
            s.split_once(" to sync ")
                .map(|(_, r)| format!("It took <t> to sync {r}"))
                .unwrap_or(s.clone()),
        ),
        J::Number(n) if n.as_i64().is_none() && n.as_f64().is_some_and(|f| f.fract() == 0.0) => {
            json!(n.as_f64().unwrap() as i64)
        }
        other => other.clone(),
    }
}

// ------------------------------------------------------------------------------------------------
// The sequence.
// ------------------------------------------------------------------------------------------------

struct Ctx {
    conn: APIConnector,
    host: String,
    port: u16,
    pipe: Pipe,
    nloc: Pipe,
    fx_steps: Vec<J>,
    extra_conns: Vec<APIConnector>,
}

fn sync_frame(j: &J) -> Frame {
    let d = &j["__df__"];
    let mut cols = Vec::new();
    for name in d["columns"].as_array().unwrap() {
        let name = name.as_str().unwrap();
        let dtype = d["dtypes"][name].as_str().unwrap();
        let values: Vec<KValue> = d["data"][name]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| match v {
                J::Null => KValue::Null,
                J::Bool(b) => KValue::Bool(*b),
                J::Number(n) => n
                    .as_i64()
                    .map(KValue::Int)
                    .unwrap_or(KValue::Float(n.as_f64().unwrap())),
                J::String(s) => KValue::Str(s.clone()),
                J::Object(o) => match o["t"].as_str().unwrap() {
                    "dt" => {
                        KValue::Datetime(parse_datetime_value(o["v"].as_str().unwrap()).unwrap())
                    }
                    _ => KValue::Null,
                },
                _ => KValue::Null,
            })
            .collect();
        let pd = meerschaum_kernel::dtypes::polars_dtype_for_pandas(dtype).unwrap();
        let series = values_to_series(name, &values, &pd).unwrap();
        cols.push(Column::new(name, ColumnData::Native(series)));
    }
    Frame::new(cols).unwrap()
}

fn step_fx<'a>(ctx: &'a Ctx, name: &str) -> &'a J {
    ctx.fx_steps.iter().find(|s| s["step"] == name).unwrap()
}

fn connector(host: &str, port: u16, extra: &[(&str, &str)]) -> APIConnector {
    let mut attrs = Map::new();
    attrs.insert("host".into(), Value::Str(host.into()));
    attrs.insert("port".into(), Value::Int(port as i64));
    for (k, v) in extra {
        attrs.insert(k.to_string(), Value::Str(v.to_string()));
    }
    let label = extra
        .iter()
        .find(|(k, _)| *k == "label")
        .map(|(_, v)| *v)
        .unwrap_or("fixture");
    APIConnector::new(Some(label), attrs).unwrap()
}

/// Run one step; `Err(())` marks a Python exception.
async fn run_step(ctx: &mut Ctx, name: &str) -> Result<J, ()> {
    let conn = &ctx.conn;
    let pipe = &ctx.pipe;
    let raw = |r: Result<meerschaum_api::client::ApiResponse, meerschaum_api::ConnectorError>| {
        r.map_err(|_| ()).and_then(|r| r.json().map_err(|_| ()))
    };
    let utc = |s: &str| DateBound::parse(s).unwrap();
    Ok(match name {
        "login" => tuple(conn.login().await),
        "get_mrsm_version" => json!(conn.get_mrsm_version().await),
        "get_chaining_status" => json!(conn.get_chaining_status().await),
        "get_actions" => json!(conn.get_actions().await.map_err(|_| ())?),
        n if n.starts_with("raw_get_")
            && ![
                "raw_get_indices_names",
                "raw_get_chunk_bounds",
                "raw_get_csv",
            ]
            .contains(&n) =>
        {
            let path = format!("/{}", &n["raw_get_".len()..]).replace('_', "/");
            raw(conn.get(&path, RequestOptions::default()).await)?
        }
        "create_metadata" => json!(conn.create_metadata().await),
        "register_pipe" | "register_pipe_again" => tuple(conn.register_pipe(pipe).await),
        "register_pipe_null_location" => tuple(conn.register_pipe(&ctx.nloc).await),
        "mrsm_connector_403" => {
            let p = Pipe::builder("mrsm", "secrets")
                .instance("api:fixture")
                .build()
                .unwrap();
            tuple(conn.register_pipe(&p).await)
        }
        "fetch_pipes_keys" => keys_json(
            conn.fetch_pipes_keys(&PipesKeysQuery::default())
                .await
                .map_err(|_| ())?,
        ),
        "fetch_pipes_keys_filtered" => {
            let mut params = Map::new();
            params.insert("connector_keys".into(), Value::Str("demo".into()));
            keys_json(
                conn.fetch_pipes_keys(&PipesKeysQuery {
                    connector_keys: vec!["demo".into()],
                    metric_keys: vec!["temperature".into()],
                    location_keys: vec![Some("home".into())],
                    tags: vec!["outdoor".into()],
                    params: Some(params),
                })
                .await
                .map_err(|_| ())?,
            )
        }
        "get_pipe_id" => match conn.get_pipe_id(pipe).await.map_err(|_| ())? {
            Some(PipeId::Int(i)) => json!(i),
            Some(PipeId::Str(s)) => json!(s),
            None => J::Null,
        },
        "get_pipe_attributes" => core_json(&Value::Map(
            conn.get_pipe_attributes(pipe).await.map_err(|_| ())?,
        )),
        "pipe_exists_before" | "pipe_exists_after" => {
            json!(conn.pipe_exists(pipe).await.map_err(|_| ())?)
        }
        "sync_pipe_chunked" | "sync_pipe_default_chunks" => {
            let fx = step_fx(ctx, name);
            let df = sync_frame(&fx["sync_df"]);
            let extra: Map = fx["sync_kwargs"]
                .as_array()
                .unwrap()
                .iter()
                .map(|kv| {
                    (
                        kv[0].as_str().unwrap().to_string(),
                        meerschaum_api::pipe_ops::json_to_core(&kv[1]),
                    )
                })
                .collect();
            let chunksize = fx["chunksize"].as_i64();
            let check_existing = extra
                .get("check_existing")
                .and_then(|v| v.as_bool())
                .unwrap_or(true);
            tuple(
                conn.sync_pipe(
                    pipe,
                    df,
                    &SyncOptions {
                        check_existing,
                        chunksize,
                        extra,
                        ..Default::default()
                    },
                )
                .await,
            )
        }
        "get_sync_time_newest" => {
            bound_json(conn.get_sync_time(pipe, None, true).await.map_err(|_| ())?)
        }
        "get_sync_time_oldest" => bound_json(
            conn.get_sync_time(pipe, None, false)
                .await
                .map_err(|_| ())?,
        ),
        "get_sync_time_params" => {
            let mut p = Map::new();
            p.insert("station".into(), Value::Str("a".into()));
            bound_json(
                conn.get_sync_time(pipe, Some(&p), true)
                    .await
                    .map_err(|_| ())?,
            )
        }
        "get_pipe_rowcount" => json!(
            conn.get_pipe_rowcount(pipe, &DataQuery::default())
                .await
                .map_err(|_| ())?
        ),
        "get_pipe_rowcount_filtered" => {
            let mut p = Map::new();
            p.insert(
                "station".into(),
                Value::List(vec![Value::Str("a".into()), Value::Str("é".into())]),
            );
            json!(
                conn.get_pipe_rowcount(
                    pipe,
                    &DataQuery {
                        begin: Some(utc("2024-01-02T00:00:00+00:00")),
                        params: Some(p),
                        ..Default::default()
                    }
                )
                .await
                .map_err(|_| ())?
            )
        }
        "get_pipe_data" => match conn
            .get_pipe_data(pipe, &DataQuery::default())
            .await
            .map_err(|_| ())?
        {
            Some(df) => frame_json(&df),
            None => J::Null,
        },
        "get_pipe_data_filtered" => {
            let mut p = Map::new();
            p.insert("station".into(), Value::Str("_b/c".into()));
            let q = DataQuery {
                select_columns: Some(vec!["ts".into(), "station".into(), "val".into()]),
                begin: Some(utc("2024-01-01T00:00:00+00:00")),
                end: Some(utc("2024-01-03T00:00:00+00:00")),
                params: Some(p),
                ..Default::default()
            };
            match conn.get_pipe_data(pipe, &q).await.map_err(|_| ())? {
                Some(df) => frame_json(&df),
                None => J::Null,
            }
        }
        "get_pipe_docs" => {
            let mut p = Map::new();
            p.insert("station".into(), Value::Str("a".into()));
            let docs = conn
                .get_pipe_docs(
                    pipe,
                    &DataQuery {
                        params: Some(p),
                        limit: Some(5),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|_| ())?;
            J::Array(
                docs.iter()
                    .map(|d| J::Object(d.iter().map(|(k, v)| (k.clone(), ktag(v))).collect()))
                    .collect(),
            )
        }
        "get_pipe_columns_types" => json!(
            conn.get_pipe_columns_types_opt(pipe)
                .await
                .map_err(|_| ())?
        ),
        "get_pipe_columns_indices" => json!(
            conn.get_pipe_columns_indices_opt(pipe)
                .await
                .map_err(|_| ())?
        ),
        // The v4 class lacks the method (AttributeError before any request).
        "get_pipe_index_names" => return Err(()),
        "raw_get_indices_names" => core_json(&Value::Map(
            conn.get_pipe_index_names(pipe).await.map_err(|_| ())?,
        )),
        "get_pipe_size" => json!(conn.get_pipe_size(pipe).await.map_err(|_| ())?),
        "vacuum_pipe" => tuple(conn.vacuum_pipe(pipe, true).await),
        "analyze_pipe" => tuple(conn.analyze_pipe(pipe).await),
        "compress_pipe" => tuple(conn.compress_pipe(pipe).await),
        "decompress_pipe" => tuple(conn.decompress_pipe(pipe, true).await),
        "partition_pipe" => tuple(conn.partition_pipe(pipe, Some(1440)).await),
        "raw_get_chunk_bounds" => raw(conn
            .get(
                "/pipes/demo/temperature/home/chunk_bounds",
                RequestOptions::query(vec![
                    ("begin", "2024-01-01".into()),
                    ("end", "2024-01-03".into()),
                    ("chunk_interval_minutes", QueryValue::from(1440)),
                ]),
            )
            .await)?,
        "raw_get_csv" => json!(
            conn.get_pipe_csv(pipe, &DataQuery::default())
                .await
                .map_err(|_| ())?
        ),
        "edit_pipe" => tuple(conn.edit_pipe(pipe, false).await),
        "edit_pipe_patch" => tuple(conn.edit_pipe(pipe, true).await),
        "clear_pipe" => {
            let mut p = Map::new();
            p.insert("station".into(), Value::Str("a".into()));
            tuple(
                conn.clear_pipe(
                    pipe,
                    &DataQuery {
                        begin: Some(utc("2024-01-02T00:00:00+00:00")),
                        params: Some(p),
                        ..Default::default()
                    },
                )
                .await,
            )
        }
        "delete_pipe_cache" => tuple(conn.delete_pipe_cache(pipe).await),
        "drop_pipe" => tuple(conn.drop_pipe(pipe).await),
        "delete_pipe" => tuple(conn.delete_pipe(pipe).await),
        "delete_pipe_null_location" => tuple(conn.delete_pipe(&ctx.nloc).await),
        "register_user"
        | "get_user_id"
        | "get_user_attributes"
        | "get_user_type"
        | "get_user_password_hash"
        | "edit_user"
        | "delete_user" => {
            let mut user = User::new("fixture_user", Some("fixture_user_pass"));
            user.email = Some("u@example.com".into());
            let mut attrs = Map::new();
            attrs.insert("a".into(), Value::Int(1));
            user.attributes = Some(attrs);
            match name {
                "register_user" => tuple(conn.register_user(&user).await),
                "get_user_id" => conn
                    .get_user_id(&user)
                    .await
                    .map_err(|_| ())?
                    .map_or(J::Null, |v| core_json(&v)),
                "get_user_attributes" => conn
                    .get_user_attributes(&user)
                    .await
                    .map_err(|_| ())?
                    .map_or(J::Null, |v| core_json(&v)),
                "get_user_type" => json!(conn.get_user_type(&user).await.map_err(|_| ())?),
                "get_user_password_hash" => {
                    json!(conn.get_user_password_hash(&user).await.map_err(|_| ())?)
                }
                "edit_user" => {
                    user.email = Some("new@example.com".into());
                    user.user_type = Some("basic".into());
                    tuple(conn.edit_user(&user).await)
                }
                _ => tuple(conn.delete_user(&user).await),
            }
        }
        "get_users" => json!(conn.get_users().await.map_err(|_| ())?),
        "register_token" => {
            let mut token = Token::new(
                Some("fixture-token"),
                Expiration::At(
                    chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
                        .unwrap()
                        .into(),
                ),
                Some(vec!["pipes:read".into(), "pipes:write".into()]),
                366,
                chrono::Utc::now(),
            );
            tuple(conn.register_token(&mut token).await)
        }
        "get_tokens" => J::Array(
            conn.get_tokens(None)
                .await
                .map_err(|_| ())?
                .iter()
                .map(token_json)
                .collect(),
        ),
        "get_tokens_labels" => J::Array(
            conn.get_tokens(Some(&["fixture-token".to_string()]))
                .await
                .map_err(|_| ())?
                .iter()
                .map(token_json)
                .collect(),
        ),
        "get_token_model" | "token_exists" | "get_token_scopes" | "edit_token"
        | "invalidate_token" | "delete_token" => {
            let id = token_id(ctx);
            match name {
                "get_token_model" => conn
                    .get_token_model(id)
                    .await
                    .map_err(|_| ())?
                    .map_or(J::Null, |m| token_json(&m)),
                "token_exists" => json!(conn.token_exists(id).await.map_err(|_| ())?),
                "get_token_scopes" => json!(conn.get_token_scopes(id).await.map_err(|_| ())?),
                _ => {
                    let mut token = Token::new(
                        Some("fixture-token-edited"),
                        Expiration::At(
                            chrono::DateTime::parse_from_rfc3339("2030-01-01T00:00:00Z")
                                .unwrap()
                                .into(),
                        ),
                        Some(vec!["pipes:read".into(), "pipes:write".into()]),
                        366,
                        chrono::Utc::now(),
                    );
                    token.id = id;
                    match name {
                        "edit_token" => tuple(conn.edit_token(&token).await),
                        "invalidate_token" => tuple(conn.invalidate_token(&token).await),
                        _ => tuple(conn.delete_token(&token).await),
                    }
                }
            }
        }
        "login_client_credentials" | "client_credentials_request" => {
            let fx = step_fx(ctx, "login_client_credentials");
            let cc = connector(
                &ctx.host,
                ctx.port,
                &[
                    ("label", "fixture_cc"),
                    ("client_id", fx["client_id"].as_str().unwrap()),
                    ("client_secret", fx["client_secret"].as_str().unwrap()),
                ],
            );
            if name == "login_client_credentials" {
                let res = tuple(cc.login().await);
                ctx.extra_conns.push(cc);
                res
            } else {
                let cc = ctx.extra_conns.last().unwrap();
                keys_json(
                    cc.fetch_pipes_keys(&PipesKeysQuery::default())
                        .await
                        .map_err(|_| ())?,
                )
            }
        }
        "login_api_key" | "api_key_request" => {
            let fx = step_fx(ctx, "login_api_key");
            let ak = connector(
                &ctx.host,
                ctx.port,
                &[
                    ("label", "fixture_ak"),
                    ("api_key", fx["api_key"].as_str().unwrap()),
                ],
            );
            if name == "login_api_key" {
                tuple(ak.login().await)
            } else {
                json!(ak.get_users().await.map_err(|_| ())?)
            }
        }
        "api_key_validate" => raw(conn
            .post(
                "/tokens/validate",
                RequestOptions {
                    headers: vec![(
                        "Authorization".into(),
                        "Bearer mrsm-key:aW52YWxpZA==".into(),
                    )],
                    use_token: false,
                    ..Default::default()
                },
            )
            .await)?,
        "register_plugin" => {
            let fx = step_fx(ctx, name);
            let plugin = PluginRecord {
                name: "fixture_plugin".into(),
                version: fx["plugin"]["version"].as_str().map(String::from),
                user_id: None,
                attributes: meerschaum_api::pipe_ops::json_to_core(&fx["plugin"]["attributes"])
                    .to_map(),
            };
            tuple(
                conn.register_plugin(
                    &plugin,
                    b64(fx["archive"].as_str().unwrap()),
                    "fixture_plugin.tar.gz",
                )
                .await,
            )
        }
        "get_plugins" => json!(conn.get_plugins(None, None).await.map_err(|_| ())?),
        "get_plugins_search" => json!(conn.get_plugins(None, Some("fix")).await.map_err(|_| ())?),
        "get_plugin_attributes" | "get_plugin_version" | "delete_plugin" => {
            let plugin = PluginRecord {
                name: "fixture_plugin".into(),
                ..Default::default()
            };
            match name {
                "get_plugin_attributes" => core_json(&Value::Map(
                    conn.get_plugin_attributes(&plugin).await.map_err(|_| ())?,
                )),
                "get_plugin_version" => {
                    json!(conn.get_plugin_version(&plugin).await.map_err(|_| ())?)
                }
                _ => tuple(conn.delete_plugin(&plugin).await),
            }
        }
        "raw_download_plugin" => json!(
            conn.download_plugin("fixture_plugin")
                .await
                .map_err(|_| ())?
                .len()
        ),
        "create_job" => {
            let mut props = Map::new();
            props.insert("tags".into(), Value::List(vec![Value::Str("x".into())]));
            tuple(
                conn.create_job(
                    "fixture-job",
                    &[
                        "python",
                        "print('ready', flush=True); import time; time.sleep(3600)",
                    ]
                    .map(String::from),
                    Some(props),
                )
                .await,
            )
        }
        "get_job_exists" => json!(conn.get_job_exists("fixture-job").await.map_err(|_| ())?),
        "start_job_missing" => tuple(conn.start_job("missing-job").await),
        "monitor_logs" => {
            let lines = Arc::new(Mutex::new(0usize));
            let result: Arc<Mutex<Option<SuccessTuple>>> = Arc::new(Mutex::new(None));
            let (l2, r2) = (lines.clone(), result.clone());
            conn.monitor_logs(
                "fixture-job",
                LogsMonitor {
                    on_line: Box::new(move |_| *l2.lock().unwrap() += 1),
                    on_input: Some(Box::new(String::new)),
                    on_stop: Some(Box::new(move |r| *r2.lock().unwrap() = Some(r))),
                    stop_on_exit: true,
                    strip_timestamps: false,
                },
            )
            .await
            .map_err(|_| ())?;
            let r = result.lock().unwrap().clone();
            json!({"lines": *lines.lock().unwrap(), "result": r.map(tuple)})
        }
        "get_jobs" => json!(
            conn.get_jobs()
                .await
                .map_err(|_| ())?
                .keys()
                .cloned()
                .collect::<Vec<_>>()
        ),
        "get_job_metadata" => {
            let mut keys: Vec<String> = conn
                .get_job_metadata("fixture-job")
                .await
                .map_err(|_| ())?
                .keys()
                .cloned()
                .collect();
            keys.sort();
            json!(keys)
        }
        "get_logs" => json!(conn.get_logs("fixture-job").await.is_ok()),
        "get_job_stop_time" => json!(
            conn.get_job_stop_time("fixture-job")
                .await
                .map_err(|_| ())?
                .is_some()
        ),
        "get_job_is_blocking_on_stdin" => json!(
            conn.get_job_is_blocking_on_stdin("fixture-job")
                .await
                .map_err(|_| ())?
        ),
        "get_job_prompt_kwargs" => core_json(&Value::Map(
            conn.get_job_prompt_kwargs("fixture-job")
                .await
                .map_err(|_| ())?,
        )),
        "pause_job" => tuple(conn.pause_job("fixture-job").await),
        "stop_job" => tuple(conn.stop_job("fixture-job").await),
        "delete_job" => tuple(conn.delete_job("fixture-job").await),
        "get_job_missing" => core_json(&Value::Map(
            conn.get_job_metadata("missing-job").await.map_err(|_| ())?,
        )),
        "do_action_legacy" => tuple(
            conn.do_action_legacy(&["show".into(), "version".into()], Map::new())
                .await,
        ),
        other => panic!("unhandled step {other}"),
    })
}

fn token_id(ctx: &Ctx) -> Option<uuid::Uuid> {
    let fx = step_fx(ctx, "get_tokens");
    fx["result"][0]["id"]["v"]
        .as_str()
        .and_then(|s| uuid::Uuid::parse_str(s).ok())
}

/// Steps whose Python results depend on a token registered directly on the instance.
const DIRECT_TOKEN_STEPS: &[&str] = &[
    "get_tokens",
    "get_tokens_labels",
    "get_token_model",
    "edit_token",
    "token_exists",
    "get_token_scopes",
    "login_client_credentials",
    "client_credentials_request",
    "login_api_key",
    "api_key_request",
    "invalidate_token",
    "delete_token",
];

async fn wait_for_job(conn: &APIConnector, step: &str) {
    for _ in 0..240 {
        let done = match step {
            "create_job" => conn
                .get_logs("fixture-job")
                .await
                .is_ok_and(|l| l.lines().any(|line| line.ends_with("| ready"))),
            "stop_job" => {
                conn.clear_job_metadata_cache();
                conn.get_job_status("fixture-job")
                    .await
                    .is_ok_and(|s| s == "stopped")
            }
            _ => return,
        };
        if done {
            if step == "create_job" {
                tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            }
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }
}

async fn replay(server: &MockServer, live: bool) -> (Vec<String>, usize) {
    let fx = fixture();
    let steps = fx["steps"].as_array().unwrap().clone();
    let host = "localhost".to_string();
    let conn = connector(
        &host,
        server.port,
        &[
            ("username", fx["username"].as_str().unwrap()),
            ("password", fx["password"].as_str().unwrap()),
        ],
    );
    let mut columns = Map::new();
    columns.insert("datetime".into(), Value::Str("ts".into()));
    columns.insert("id".into(), Value::Str("station".into()));
    let pipe = Pipe::builder("demo", "temperature")
        .location(Some("home"))
        .instance("api:fixture")
        .kwarg("columns", Value::Map(columns))
        .kwarg("tags", Value::List(vec![Value::Str("outdoor".into())]))
        .build()
        .unwrap();
    let mut room = Map::new();
    room.insert("id".into(), Value::Str("room".into()));
    let nloc = Pipe::builder("demo", "humidity")
        .instance("api:fixture")
        .kwarg("columns", Value::Map(room))
        .build()
        .unwrap();
    let mut ctx = Ctx {
        conn,
        host,
        port: server.port,
        pipe,
        nloc,
        fx_steps: steps.clone(),
        extra_conns: Vec::new(),
    };

    let mut errors = Vec::new();
    let mut n_requests = 0;
    for step in &steps {
        let name = step["step"].as_str().unwrap();
        if live && DIRECT_TOKEN_STEPS.contains(&name) {
            continue;
        }
        if !live {
            for r in step["requests"].as_array().unwrap() {
                server.push_response(canned(r));
            }
            for ws in step["websockets"].as_array().unwrap() {
                let script = ws["events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|e| {
                        (
                            e[0].as_str().unwrap().to_string(),
                            e[1].as_str().map(String::from),
                        )
                    })
                    .collect();
                server.push_ws_script(script);
            }
        }
        let result = run_step(&mut ctx, name).await;
        let (recorded, pending, ws_received) = server.take();
        if live {
            // The generator's unrecorded waits: the job's first output after `create_job`, and
            // the stopped state after `stop_job`.
            wait_for_job(&ctx.conn, name).await;
            let _ = server.take();
        }
        let expected = step["requests"].as_array().unwrap();
        if !live && pending > 0 {
            errors.push(format!(
                "{name}: {pending} recorded responses were not requested"
            ));
        }
        let http: Vec<&Recorded> = recorded
            .iter()
            .filter(|r| r.header("upgrade").is_none())
            .collect();
        if http.len() != expected.len() {
            errors.push(format!(
                "{name}: {} requests != python {}\n    rust {:?}",
                http.len(),
                expected.len(),
                http.iter()
                    .map(|r| format!("{} {}", r.method, r.target))
                    .collect::<Vec<_>>()
            ));
        }
        for (i, (e, a)) in expected.iter().zip(http.iter()).enumerate() {
            errors.extend(compare_request(name, i, e, a));
            n_requests += 1;
        }
        // Websocket traffic: the client's messages (the token is masked).
        for (ws, got) in step["websockets"]
            .as_array()
            .unwrap()
            .iter()
            .zip(ws_received.iter())
        {
            let sent: Vec<String> = ws["events"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|e| e[0] == "send")
                .map(|e| e[1].as_str().unwrap_or("").to_string())
                .collect();
            let got: Vec<String> = got
                .iter()
                .enumerate()
                .map(|(i, m)| {
                    if i == 0 {
                        "<token>".to_string()
                    } else {
                        m.clone()
                    }
                })
                .collect();
            if sent != got {
                errors.push(format!("{name}: websocket sent {sent:?} != {got:?}"));
            }
            let target = recorded
                .iter()
                .find(|r| r.header("upgrade").is_some())
                .map(|r| r.target.clone());
            let expected_target = target_of(ws["uri"].as_str().unwrap());
            if target.as_deref() != Some(expected_target.as_str()) {
                errors.push(format!(
                    "{name}: websocket target {target:?} != {expected_target}"
                ));
            }
        }
        let python_error = !step["error"].is_null();
        match result {
            Err(()) if python_error => {}
            Err(()) => errors.push(format!(
                "{name}: rust failed, python returned {}",
                step["result"]
            )),
            Ok(actual)
                if python_error
                    && step["error"]
                        .as_str()
                        .unwrap_or("")
                        .starts_with("ConnectionError")
                    && actual.get(0) == Some(&J::Bool(false)) =>
            {
                // A transport failure raises in v4 and is a failed `SuccessTuple` here.
            }
            Ok(_) if python_error => errors.push(format!(
                "{name}: python raised {}, rust succeeded",
                step["error"]
            )),
            Ok(actual) => {
                if live
                    && ["get_jobs", "sync_pipe_chunked", "sync_pipe_default_chunks"].contains(&name)
                {
                    continue;
                }
                let mut expected = norm(&step["result"]);
                let mut actual = norm(&actual);
                if let (Some(e), Some(a)) = (
                    expected.get("__df__").cloned(),
                    actual.get("__df__").cloned(),
                ) {
                    // Values must match; dtype names are kernel-specific and reported only.
                    expected = e["data"].clone();
                    actual = a["data"].clone();
                    if e["columns"] != a["columns"] {
                        errors.push(format!(
                            "{name}: columns {} != {}",
                            e["columns"], a["columns"]
                        ));
                    }
                }
                if matches!(name, "get_tokens" | "get_tokens_labels" | "get_token_model") {
                    let pick = |j: &J| -> J {
                        match j {
                            J::Array(a) => J::Array(a.iter().map(|t| json!({"id": t["id"], "label": t["label"], "scopes": t["scopes"], "is_valid": t["is_valid"]})).collect()),
                            J::Null => J::Null,
                            t => json!({"id": t["id"], "label": t["label"], "scopes": t["scopes"], "is_valid": t["is_valid"]}),
                        }
                    };
                    expected = pick(&expected);
                    actual = pick(&actual);
                }
                if expected != actual {
                    errors.push(format!(
                        "{name}: result\n    python {expected}\n    rust   {actual}"
                    ));
                }
            }
        }
    }
    (errors, n_requests)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contract_replay_mock() {
    let server = MockServer::start().await;
    let (errors, n) = replay(&server, false).await;
    for e in &errors {
        eprintln!("MISMATCH {e}");
    }
    eprintln!("compared {n} requests");
    assert!(errors.is_empty(), "{} mismatches", errors.len());
    assert!(n > 90);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn contract_replay_live() {
    let Ok(url) = std::env::var("MRSM_API_CONTRACT_URL") else {
        eprintln!(
            "SKIPPED contract_replay_live: set MRSM_API_CONTRACT_URL (see fixtures/gen_api.py --serve)"
        );
        return;
    };
    let upstream = url
        .split_once("://")
        .map(|x| x.1)
        .unwrap_or(&url)
        .rsplit('@')
        .next()
        .unwrap()
        .trim_end_matches('/')
        .to_string();
    let server = MockServer::start().await;
    server.set_upstream(Some(upstream));
    let (errors, n) = replay(&server, true).await;
    for e in &errors {
        eprintln!("MISMATCH {e}");
    }
    eprintln!("compared {n} live requests");
    assert!(errors.is_empty(), "{} mismatches", errors.len());
}
