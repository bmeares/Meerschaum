//! Pipe routes (`meerschaum/connectors/api/_pipes.py`).

use std::time::Instant;

use indexmap::IndexMap;
use meerschaum_core::pyfmt::{JsonStyle, json_dumps, py_str};
use meerschaum_core::success::SyncResult;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::serialize::GeometryFormat;

use crate::pipe_ops::{self, Doc};
use crate::routes;
use crate::traits::{
    ColumnIndexInfo, ConnectorError, ConnectorResult, DataQuery, DateBound, PipeId, PipeKeysEntry,
    PipesKeys, PipesKeysQuery, SyncOptions,
};
use crate::wire::{QueryValue, ToJsonOptions, to_json};

use super::{
    APIConnector, ATTRIBUTES_CACHE_SECONDS, ApiResponse, Body, COLUMNS_TYPES_CACHE_SECONDS,
    EXISTS_CACHE_SECONDS, PipeCacheEntry, RequestOptions, STATIC_SCHEMA_CACHE_SECONDS,
};

/// `json.dumps(value)` (default separators, `ensure_ascii=True`), as `requests` sends `json=`
/// bodies and as v4 encodes JSON query parameters.
pub fn py_json(value: &Value) -> String {
    json_dumps(value, JsonStyle::Default, true)
}

fn py_json_opt_list(v: &Option<Vec<String>>) -> String {
    match v {
        Some(items) => py_json(&Value::List(
            items.iter().map(|s| Value::Str(s.clone())).collect(),
        )),
        None => "null".into(),
    }
}

fn py_json_opt_map(v: &Option<Map>) -> String {
    match v {
        Some(m) => py_json(&Value::Map(m.clone())),
        None => "null".into(),
    }
}

fn bound_query(b: &Option<DateBound>) -> QueryValue {
    match b {
        Some(b) => QueryValue::One(b.py_str()),
        None => QueryValue::None,
    }
}

/// A core value as a query parameter (`str(value)`, `None` dropped, lists repeated).
pub fn query_value(v: &Value) -> QueryValue {
    match v {
        Value::Null => QueryValue::None,
        Value::List(items) => QueryValue::Many(items.iter().map(py_str).collect()),
        other => QueryValue::One(py_str(other)),
    }
}

/// `humanfriendly.format_timespan(seconds)` as `interval_str(timedelta)` uses it.
pub fn format_timespan(seconds: f64) -> String {
    fn round_number(x: f64) -> String {
        let s = format!("{x:.2}");
        let s = s.trim_end_matches('0');
        s.trim_end_matches('.').to_string()
    }
    fn pluralize(count: &str, singular: &str) -> String {
        let n: f64 = count.parse().unwrap_or(0.0);
        if n.floor() == 1.0 {
            format!("{count} {singular}")
        } else {
            format!("{count} {singular}s")
        }
    }
    if seconds < 60.0 {
        return pluralize(&round_number(seconds), "second");
    }
    let units: [(&str, f64); 6] = [
        ("year", 31_536_000.0),
        ("week", 604_800.0),
        ("day", 86_400.0),
        ("hour", 3_600.0),
        ("minute", 60.0),
        ("second", 1.0),
    ];
    let mut remaining = seconds;
    let mut parts = Vec::new();
    for (i, (name, divider)) in units.iter().enumerate() {
        let count = remaining / divider;
        remaining %= divider;
        let text = if i + 1 < units.len() {
            (count.trunc() as i64).to_string()
        } else {
            round_number(count)
        };
        if text != "0" {
            parts.push(pluralize(&text, name));
        }
    }
    if parts.len() == 1 {
        return parts.remove(0);
    }
    parts.truncate(3);
    let last = parts.pop().unwrap_or_default();
    if parts.is_empty() {
        last
    } else {
        format!("{} and {last}", parts.join(", "))
    }
}

/// `f"{n:,}"`.
fn thousands(n: usize) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// The error of a read the server failed (`5xx`: its instance errored or could not be reached),
/// rc15 Q17: a failed read is an error, not an empty pipe. `4xx` answers (an unregistered pipe,
/// a table not created yet) keep v4's empty results.
fn server_failed(pipe: &Pipe, what: &str, response: &ApiResponse) -> Option<ConnectorError> {
    (response.status >= 500).then(|| {
        let body = response.text();
        let detail = serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|j| {
                j.get("detail")
                    .or_else(|| j.get("message"))
                    .and_then(|d| d.as_str().map(str::to_string))
            })
            .unwrap_or(body);
        ConnectorError::Backend(format!(
            "Failed to get {what} for {pipe}: the API server answered {}{}",
            response.status,
            if detail.trim().is_empty() {
                ".".to_string()
            } else {
                format!(":\n{}", detail.trim())
            }
        ))
    })
}

/// The v4 pattern for maintenance and delete routes: a JSON list is the tuple, a `detail` body
/// keeps the response's truthiness, anything else returns the raw text. The v5-only routes'
/// `{"success": bool, "message": str}` (the saved Pipelines' metadata) is read as its fields.
fn tuple_from_json(response: &ApiResponse, data: &serde_json::Value) -> SuccessTuple {
    if let serde_json::Value::Object(o) = data
        && let Some(success) = o.get("success").and_then(serde_json::Value::as_bool)
        && let Some(message) = o.get("message").and_then(serde_json::Value::as_str)
    {
        return SuccessTuple {
            success: success && response.ok(),
            message: message.to_string(),
        };
    }
    match data {
        serde_json::Value::Array(items) => SuccessTuple {
            success: items.first().and_then(|b| b.as_bool()).unwrap_or(false),
            message: items
                .get(1)
                .map(|m| match m {
                    serde_json::Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .unwrap_or_default(),
        },
        serde_json::Value::Object(o) if o.contains_key("detail") => SuccessTuple {
            success: response.ok(),
            message: match &o["detail"] {
                serde_json::Value::String(s) => s.clone(),
                other => crate::models::detail_repr(other),
            },
        },
        _ => SuccessTuple {
            success: response.ok(),
            message: response.text(),
        },
    }
}

fn kernel_docs_from_json(items: &[serde_json::Value]) -> Vec<Doc> {
    items
        .iter()
        .map(|item| match item {
            serde_json::Value::Object(o) => o
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        meerschaum_kernel::frame::json_to_value(v.clone()),
                    )
                })
                .collect(),
            _ => Doc::new(),
        })
        .collect()
}

fn now_secs() -> Instant {
    Instant::now()
}

impl APIConnector {
    /// `pipe_r_url(pipe)`.
    pub fn pipe_r_url(pipe: &Pipe) -> String {
        routes::pipe_r_url(
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        )
    }

    /// Whether `pipe` is a saved Pipeline's registration (`Pipe('mrsm', 'pipeline', name)`),
    /// whose metadata travels over `/pipelines/{name}/*` (the v5 shape).
    fn is_pipeline_metadata(pipe: &Pipe) -> bool {
        pipe.connector_keys() == "mrsm" && pipe.metric_key() == "pipeline"
    }

    /// Metadata transport is narrowly reserved for registered Pipeline definitions.
    pub(crate) fn metadata_r_url(pipe: &Pipe) -> String {
        if Self::is_pipeline_metadata(pipe) {
            format!(
                "{}/{}",
                routes::PIPELINES,
                crate::wire::quote_plus(pipe.location_key().unwrap_or(""))
            )
        } else {
            Self::pipe_r_url(pipe)
        }
    }

    /// `get_pipe_instance_keys(pipe)`: `parameters['instance_keys']` if set, else the
    /// connector's `instance_keys`.
    pub fn get_pipe_instance_keys(&self, pipe: &Pipe) -> Option<String> {
        match pipe.parameters().get("instance_keys") {
            Some(Value::Null) => None,
            Some(v) => Some(py_str(v)),
            None => self.instance_keys(),
        }
    }

    fn instance_query(&self, pipe: &Pipe, key: &str) -> (String, QueryValue) {
        (
            key.to_string(),
            QueryValue::opt(self.get_pipe_instance_keys(pipe)),
        )
    }

    // ---------------------------------------------------------------------------------------
    // Client-side pipe caches (`Pipe.attributes`, `Pipe.exists()`, `Pipe.get_columns_types()`).
    // ---------------------------------------------------------------------------------------

    fn cache_entry(&self, pipe: &Pipe) -> PipeCacheEntry {
        self.pipe_cache
            .lock()
            .map(|c| c.get(&pipe.pipe_ref()).cloned().unwrap_or_default())
            .unwrap_or_default()
    }

    fn update_cache(&self, pipe: &Pipe, f: impl FnOnce(&mut PipeCacheEntry)) {
        if let Ok(mut c) = self.pipe_cache.lock() {
            f(c.entry(pipe.pipe_ref()).or_default());
        }
    }

    /// `pipe.get_parameters()`: the instance's parameters patched with the in-memory ones,
    /// refreshed from `/attributes` every 600 s (temporary pipes use the in-memory ones).
    ///
    /// Returns the RAW (unresolved) parameters: this is metadata forwarded to the server, so
    /// `{{ ... }}` templates must survive. Use [`Self::pipe_resolved_parameters`] to execute.
    pub async fn pipe_parameters(&self, pipe: &Pipe) -> Map {
        if pipe.temporary {
            return pipe.raw_parameters().clone();
        }
        let cached = self.cache_entry(pipe).attributes;
        let remote = match cached {
            Some((at, params)) if at.elapsed().as_secs_f64() < ATTRIBUTES_CACHE_SECONDS => params,
            _ => match self.get_pipe_attributes(pipe).await {
                Ok(attrs) => {
                    let params = attrs
                        .get("parameters")
                        .and_then(|p| p.as_map().cloned())
                        .unwrap_or_default();
                    let p2 = params.clone();
                    self.update_cache(pipe, |e| e.attributes = Some((now_secs(), p2)));
                    params
                }
                Err(e) => {
                    super::warnings::warn_pipe_fmt!(
                        pipe,
                        "Failed to load attributes for {pipe}: {e}"
                    );
                    return pipe.raw_parameters().clone();
                }
            },
        };
        meerschaum_core::value::apply_patch_maps(&remote, pipe.raw_parameters())
    }

    /// [`Self::pipe_parameters`] with templates and config symlinks resolved, for execution
    /// (dtype contexts, `static` / `autotime` flags).
    pub async fn pipe_resolved_parameters(&self, pipe: &Pipe) -> Map {
        let raw = self.pipe_parameters(pipe).await;
        let mut merged = pipe.clone();
        merged.set_parameters(raw);
        merged.parameters().clone()
    }

    /// `pipe.exists()`: `True` results are cached for 60 s.
    pub async fn pipe_exists_cached(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        if let Some(at) = self.cache_entry(pipe).exists
            && at.elapsed().as_secs_f64() < EXISTS_CACHE_SECONDS
        {
            return Ok(true);
        }
        let exists = self.pipe_exists(pipe).await?;
        if exists {
            self.update_cache(pipe, |e| e.exists = Some(now_secs()));
        }
        Ok(exists)
    }

    /// `pipe.get_columns_types()`: non-empty results are cached for 60 s (3600 s for static
    /// pipes).
    pub async fn pipe_columns_types_cached(
        &self,
        pipe: &Pipe,
        parameters: &Map,
    ) -> ConnectorResult<IndexMap<String, String>> {
        let ttl = if pipe_ops::bool_param(parameters, "static", false) {
            STATIC_SCHEMA_CACHE_SECONDS
        } else {
            COLUMNS_TYPES_CACHE_SECONDS
        };
        if let Some((at, types)) = self.cache_entry(pipe).columns_types
            && !types.is_empty()
            && at.elapsed().as_secs_f64() < ttl
        {
            return Ok(types);
        }
        let types = self
            .get_pipe_columns_types_opt(pipe)
            .await?
            .unwrap_or_default();
        let t2 = types.clone();
        self.update_cache(pipe, |e| e.columns_types = Some((now_secs(), t2)));
        Ok(types)
    }

    /// `pipe.dtypes` (`Pipe.get_dtypes(infer=True)`) through the client-side caches.
    pub async fn pipe_dtypes(&self, pipe: &Pipe) -> ConnectorResult<IndexMap<String, String>> {
        let parameters = self.pipe_resolved_parameters(pipe).await;
        let remote = if self.pipe_exists_cached(pipe).await? {
            self.pipe_columns_types_cached(pipe, &parameters)
                .await?
                .into_iter()
                .map(|(c, t)| {
                    let pd = if meerschaum_core::pyfmt::py_isupper(&t) {
                        meerschaum_core::dtypes::get_pd_type_from_db_type(&t, true)
                    } else {
                        meerschaum_kernel::dtypes::to_pandas_dtype(&t)
                    };
                    (c, pd)
                })
                .collect()
        } else {
            IndexMap::new()
        };
        let mut merged = pipe.clone();
        merged.set_parameters(parameters.clone());
        Ok(meerschaum_kernel::enforce::pipe_get_dtypes(
            &pipe_ops::configured_dtypes(&parameters),
            &remote,
            &pipe_ops::columns_map(&merged),
            pipe_ops::bool_param(&parameters, "autoincrement", false),
        ))
    }

    // ---------------------------------------------------------------------------------------
    // Routes
    // ---------------------------------------------------------------------------------------

    /// `register_pipe(pipe)`: `POST /pipes/{keys}/register` with the in-memory parameters.
    pub async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::protected_refusal(self, pipe, "register") {
            return refused;
        }
        let r_url = Self::metadata_r_url(pipe) + "/register";
        let opts = RequestOptions {
            query: vec![self.instance_query(pipe, "instance_keys")],
            body: Body::Json(py_json(&Value::Map(pipe.raw_parameters().clone()))),
            ..Default::default()
        };
        let response = match self.post(&r_url, opts).await {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if !response.ok() {
            // A saved Pipeline's registration answers `{"success": false, "message": ...}`.
            if Self::is_pipeline_metadata(pipe)
                && let Ok(data) = response.json()
            {
                return tuple_from_json(&response, &data);
            }
            return SuccessTuple::fail(response.text());
        }
        match response.json() {
            Ok(data) => tuple_from_json(&response, &data),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `edit_pipe(pipe, patch)`: `PATCH /pipes/{keys}/edit` with `pipe.get_parameters()`.
    pub async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::protected_refusal(self, pipe, "edit") {
            return refused;
        }
        let parameters = if patch {
            self.pipe_parameters(pipe).await
        } else {
            pipe.raw_parameters().clone()
        };
        let r_url = Self::metadata_r_url(pipe) + "/edit";
        let opts = RequestOptions {
            query: vec![
                ("patch".into(), QueryValue::bool(patch)),
                self.instance_query(pipe, "instance_keys"),
            ],
            body: Body::Json(py_json(&Value::Map(parameters))),
            ..Default::default()
        };
        let response = match self.patch(&r_url, opts).await {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match response.json() {
            Ok(data) => tuple_from_json(&response, &data),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `fetch_pipes_keys(...)`: `GET /pipes/keys` with JSON-encoded filters and `as_dict=True`.
    pub async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        let strs = |v: &[String]| Value::List(v.iter().map(|s| Value::Str(s.clone())).collect());
        let lks = Value::List(
            query
                .location_keys
                .iter()
                .map(|l| l.clone().map(Value::Str).unwrap_or(Value::Null))
                .collect(),
        );
        let opts = RequestOptions::query(vec![
            (
                "connector_keys",
                py_json(&strs(&query.connector_keys)).into(),
            ),
            ("metric_keys", py_json(&strs(&query.metric_keys)).into()),
            ("location_keys", py_json(&lks).into()),
            ("tags", py_json(&strs(&query.tags)).into()),
            ("params", py_json_opt_map(&query.params).into()),
            ("instance_keys", QueryValue::opt(self.instance_keys())),
            ("as_dict", QueryValue::bool(true)),
        ]);
        let pipelines = query.connector_keys == ["mrsm"] && query.metric_keys == ["pipeline"];
        let base = if pipelines {
            routes::PIPELINES
        } else {
            routes::PIPES
        };
        let response = self.get(&format!("{base}/keys"), opts).await?;
        // The saved Pipelines' keys come as `{"success": true, ..., "keys": [...]}`.
        let j = if pipelines {
            response.envelope_field("keys")?
        } else {
            response.json_or_detail()?
        };
        let entry = |v: &serde_json::Value| -> Option<PipeKeysEntry> {
            let items: Vec<Value> = v.as_array()?.iter().map(pipe_ops::json_to_core).collect();
            PipeKeysEntry::from_list(&items)
        };
        let keys = match j {
            serde_json::Value::Object(o) => PipesKeys::Dict(
                o.iter()
                    .filter_map(|(k, v)| Some((PipeId::parse(k), entry(v)?)))
                    .collect(),
            ),
            serde_json::Value::Array(a) => PipesKeys::List(a.iter().filter_map(entry).collect()),
            _ => PipesKeys::List(Vec::new()),
        };
        // Meerschaum's internal pipes stay out of listings unless the query names `mrsm` (the
        // server leaves them out too; an older server may not).
        Ok(meerschaum_connector::hide_internal_pipes(query, keys))
    }

    /// `sync_pipe(pipe, df, chunksize, **kw)`: post the frame in chunks
    /// (`to_json(chunk, orient='columns', geometry_format='wkb_hex')`), forwarding the keyword
    /// arguments as query parameters, then invalidate the server's pipe cache.
    pub async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        self.sync_pipe_counted(pipe, df, opts).await.into()
    }

    /// [`APIConnector::sync_pipe`] with the row counts the server reported for the chunks it
    /// synced (`Inserted 2, updated 1 rows.`, `Upserted 3 rows.`, `Replaced 1 row with 2
    /// rows.`), summed. The message is `sync_pipe`'s (v4's `It took ... to sync 3 rows across 2
    /// chunks ...`), which has none (`InstanceConnector::sync_pipe_with_counts`).
    pub async fn sync_pipe_counted(
        &self,
        pipe: &Pipe,
        df: Frame,
        opts: &SyncOptions,
    ) -> SyncResult {
        if let Some(refused) = meerschaum_connector::protected_refusal(self, pipe, "sync") {
            return refused.into();
        }
        if crate::engine_pipes::is_engine_pipe(pipe) {
            return self.write_engine_pipe(pipe.metric_key(), &df).await.into();
        }
        let begin = Instant::now();
        // A replacing sync (rc15): `opts.replace` with the window the engine resolved, or
        // `replace=True` among the keyword arguments of a direct `APIConnector.sync_pipe(...)`
        // call, whose `begin` / `end` (when given) go along as they are and whose missing
        // bounds the server takes from the posted rows (never the whole table).
        let replace = opts.replace || opts.extra.get("replace").is_some_and(Value::truthy);
        // PARITY: `chunksize=None` posts one row per request (`1 if chunksize is None`).
        // A replacing sync posts every row in one request: the server replaces the window
        // once, in one transaction.
        let chunksize = if replace {
            df.height()
        } else {
            match opts.chunksize {
                None => 1,
                Some(-1) => self.sync_chunksize,
                Some(n) => n,
            }
            .max(1) as usize
        }
        .max(1);

        let mut query: Vec<(String, QueryValue)> = opts
            .extra
            .iter()
            .filter(|(k, _)| {
                !opts.replace || !matches!(k.as_str(), "replace" | "begin" | "end" | "params")
            })
            .map(|(k, v)| (k.clone(), query_value(v)))
            .collect();
        if !opts.extra.contains_key("check_existing") && !opts.check_existing {
            query.push(("check_existing".into(), QueryValue::bool(false)));
        }
        if opts.replace {
            // The resolved window: an unbounded side is sent as `None` (not left out, which the
            // server would read as "the rows' own range").
            let window_bound = |b: &Option<DateBound>| match b {
                Some(_) => bound_query(b),
                None => QueryValue::One("None".into()),
            };
            query.push(("replace".into(), QueryValue::bool(true)));
            query.push(("begin".into(), window_bound(&opts.begin)));
            query.push(("end".into(), window_bound(&opts.end)));
            query.push(("params".into(), py_json_opt_map(&opts.params).into()));
        }
        let parameters = self.pipe_resolved_parameters(pipe).await;
        let mut merged = pipe.clone();
        merged.set_parameters(parameters);
        let columns = merged.columns();
        if !columns.is_empty() {
            query.push(("columns".into(), py_json(&Value::Map(columns)).into()));
        }
        query.push(self.instance_query(pipe, "instance_keys"));
        let r_url = Self::pipe_r_url(pipe) + "/data";

        let n = df.height();
        let json_opts = ToJsonOptions {
            columns_orient: true,
            geometry_format: GeometryFormat::WkbHex,
            ..Default::default()
        };
        let mut rowcount = 0usize;
        let mut num_success_chunks = 0usize;
        // The server's counts for each chunk (`Inserted 2, updated 1 rows.`), summed.
        let mut counts = SyncResult::new(true, String::new());
        let mut start = 0usize;
        if replace && n == 0 {
            // No rows: the window is cleared.
            let response = match self
                .post(
                    &r_url,
                    RequestOptions {
                        query: query.clone(),
                        body: Body::Text(String::new()),
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(r) => r,
                Err(e) => return SuccessTuple::fail(format!("Failed to sync {pipe}:\n{e}")).into(),
            };
            let _ = self.delete_pipe_cache(pipe).await;
            return match response.json() {
                Ok(j @ serde_json::Value::Array(_)) => {
                    let t = tuple_from_json(&response, &j);
                    pipe_ops::sync_result_from_message(t.success, t.message)
                }
                // A refused window (`400`: no rows and no bounds) explains itself.
                Ok(serde_json::Value::Object(o)) if o.contains_key("detail") => {
                    SuccessTuple::fail(match &o["detail"] {
                        serde_json::Value::String(s) => s.clone(),
                        other => crate::models::detail_repr(other),
                    })
                    .into()
                }
                _ => SuccessTuple::fail(response.text()).into(),
            };
        }
        while start < n {
            let end = (start + chunksize).min(n);
            let indices: Vec<usize> = (start..end).collect();
            start = end;
            if indices.is_empty() {
                continue;
            }
            let chunk = df.take(&indices);
            let json_str = to_json(&chunk, Some(&indices), &json_opts);
            let response = match self
                .post(
                    &r_url,
                    RequestOptions {
                        query: query.clone(),
                        body: Body::Text(json_str),
                        // A sync path records this sync's outcome itself, through this client
                        // (`client/sync_status.rs`): the server must not record the posts.
                        headers: if meerschaum_connector::sync_status::in_recorded_sync() {
                            vec![(
                                meerschaum_connector::sync_status::CLIENT_RECORDS_HEADER.into(),
                                "client".into(),
                            )]
                        } else {
                            Vec::new()
                        },
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(r) => r,
                Err(e) => {
                    let msg = format!("Failed to post a chunk to {pipe}:\n{e}");
                    super::warnings::warn_pipe_fmt!(pipe, "{msg}");
                    return SuccessTuple::fail(msg).into();
                }
            };
            if !response.ok() {
                return SuccessTuple::fail(format!("Failed to sync a chunk:\n{}", response.text()))
                    .into();
            }
            let j = match response.json() {
                Ok(j) => j,
                Err(e) => {
                    return SuccessTuple::fail(format!(
                        "Failed to parse response from syncing {pipe}:\n{e}"
                    ))
                    .into();
                }
            };
            if let Some(d) = j.as_object().and_then(|o| o.get("detail")) {
                return SuccessTuple::fail(match d {
                    serde_json::Value::String(s) => s.clone(),
                    other => crate::models::detail_repr(other),
                })
                .into();
            }
            let t = match &j {
                serde_json::Value::Array(_) => tuple_from_json(&response, &j),
                _ => return SuccessTuple::fail(response.text()).into(),
            };
            if !t.success {
                return t.into();
            }
            let chunk_counts = pipe_ops::sync_result_from_message(t.success, t.message);
            if replace {
                // The server's `Replaced {deleted} rows with {inserted} rows.` (the counts).
                let _ = self.delete_pipe_cache(pipe).await;
                return chunk_counts;
            }
            counts.inserted += chunk_counts.inserted;
            counts.updated += chunk_counts.updated;
            counts.upserted += chunk_counts.upserted;
            counts.deleted += chunk_counts.deleted;
            rowcount += indices.len();
            num_success_chunks += 1;
        }
        let _ = self.delete_pipe_cache(pipe).await;
        let elapsed = begin.elapsed().as_secs_f64();
        counts.message = format!(
            "It took {} to sync {} row{} across {} chunk{} to {pipe}.",
            format_timespan(elapsed),
            thousands(rowcount),
            if rowcount != 1 { "s" } else { "" },
            thousands(num_success_chunks),
            if num_success_chunks != 1 { "s" } else { "" },
        );
        counts
    }

    /// `delete_pipe_cache(pipe)`: `DELETE /pipes/{keys}/cache`.
    pub async fn delete_pipe_cache(&self, pipe: &Pipe) -> SuccessTuple {
        let r_url = Self::pipe_r_url(pipe) + "/cache";
        let response = match self
            .delete(
                &r_url,
                RequestOptions {
                    query: vec![self.instance_query(pipe, "instance_keys")],
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if !response.ok() {
            return SuccessTuple::fail(format!(
                "Failed to invalidate cache for {pipe}: {}",
                response.text()
            ));
        }
        match response.json() {
            Ok(j @ serde_json::Value::Array(_)) => tuple_from_json(&response, &j),
            _ => SuccessTuple {
                success: response.ok(),
                message: response.text(),
            },
        }
    }

    /// `delete_pipe(pipe)`: `DELETE /pipes/{keys}/delete`.
    pub async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::protected_refusal(self, pipe, "delete") {
            return refused;
        }
        let r_url = Self::metadata_r_url(pipe) + "/delete";
        let response = match self
            .delete(
                &r_url,
                RequestOptions {
                    query: vec![self.instance_query(pipe, "instance_keys")],
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match response.json() {
            Ok(data) => tuple_from_json(&response, &data),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// `get_pipe_data(pipe, ...)`: `GET /pipes/{keys}/data`, parsed with `parse_df_datetimes`
    /// (non-datetime dtypes ignored, timezones stripped for naive pipes).
    ///
    /// PARITY: the instance is sent as `instance`, which the server ignores (it reads
    /// `instance_keys`), so the server's default instance answers (`_pipes.py:392`). The same
    /// holds for `id`, `attributes`, `sync_time`, `exists`, `rowcount`, `drop`, `clear`,
    /// `columns/*`, `indices/names` and `size`.
    pub async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        if crate::engine_pipes::is_engine_pipe(pipe) {
            let q = crate::engine_pipes::EnginePipeQuery::from_data_query(query);
            return self.read_engine_pipe(pipe.metric_key(), &q).await;
        }
        let r_url = Self::pipe_r_url(pipe) + "/data";
        let params_json = py_json_opt_map(&query.params);
        let opts = RequestOptions::query(vec![
            (
                "select_columns",
                py_json_opt_list(&query.select_columns).into(),
            ),
            ("omit_columns", py_json_opt_list(&query.omit_columns).into()),
            ("begin", bound_query(&query.begin)),
            ("end", bound_query(&query.end)),
            ("params", params_json.into()),
            (
                "instance",
                QueryValue::opt(self.get_pipe_instance_keys(pipe)),
            ),
            ("as_chunks", QueryValue::bool(query.as_chunks)),
        ]);
        // Q17: an unreachable server or a failed read (`5xx`) is an error, not an empty pipe;
        // an unregistered pipe (`409`) or one without a table yet (`400`) has no data.
        let response = self.get(&r_url, opts).await?;
        if !response.ok() {
            if let Some(e) = server_failed(pipe, "data", &response) {
                return Err(e);
            }
            return Ok(None);
        }
        let j = response.json()?;
        if let Some(d) = j.as_object().and_then(|o| o.get("detail")) {
            // PARITY: v4 returns the tuple `(False, detail)` here instead of a DataFrame.
            return Err(ConnectorError::Detail(match d {
                serde_json::Value::String(s) => s.clone(),
                other => crate::models::detail_repr(other),
            }));
        }
        let dtypes = self.pipe_dtypes(pipe).await?;
        let ignore_cols: Vec<String> = dtypes
            .iter()
            .filter(|(_, t)| !meerschaum_core::dtypes::are_dtypes_equal(t, "datetime"))
            .map(|(c, _)| c.clone())
            .collect();
        let parameters = self.pipe_resolved_parameters(pipe).await;
        let mut merged = pipe.clone();
        merged.set_parameters(parameters.clone());
        let tz_aware = meerschaum_kernel::enforce::pipe_is_tz_aware(
            &pipe_ops::columns_map(&merged),
            &dtypes,
            pipe_ops::bool_param(&parameters, "autotime", false),
            "ts",
        );
        let items = j.as_array().cloned().unwrap_or_default();
        let docs = kernel_docs_from_json(&items);
        let df = meerschaum_kernel::docs::frame_from_docs(&docs)
            .map_err(|e| ConnectorError::Invalid(e.to_string()))?;
        let df = meerschaum_kernel::docs::parse_df_datetimes(&df, &ignore_cols, !tz_aware, false);
        if df.width() == 0 {
            return Ok(Some(meerschaum_kernel::diff::add_missing_cols(
                &df, &dtypes,
            )));
        }
        Ok(Some(df))
    }

    /// `get_pipe_docs(pipe, ...)`: `GET /pipes/{keys}/docs`.
    pub async fn get_pipe_docs(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Vec<Doc>> {
        let r_url = Self::pipe_r_url(pipe) + "/docs";
        let opts = RequestOptions::query(vec![
            (
                "select_columns",
                py_json_opt_list(&query.select_columns).into(),
            ),
            ("omit_columns", py_json_opt_list(&query.omit_columns).into()),
            ("begin", bound_query(&query.begin)),
            ("end", bound_query(&query.end)),
            ("params", py_json_opt_map(&query.params).into()),
            (
                "order",
                QueryValue::One(query.order.clone().unwrap_or_else(|| "asc".into())),
            ),
            (
                "limit",
                query
                    .limit
                    .map(QueryValue::from)
                    .unwrap_or(QueryValue::None),
            ),
            (
                "instance_keys",
                QueryValue::opt(self.get_pipe_instance_keys(pipe)),
            ),
        ]);
        let response = self.get(&r_url, opts).await?;
        if !response.ok() {
            if let Some(e) = server_failed(pipe, "docs", &response) {
                return Err(e);
            }
            super::warnings::warn_pipe_fmt!(
                pipe,
                "Failed to get docs for {pipe}:\n{}",
                response.text()
            );
            return Ok(Vec::new());
        }
        Ok(match response.json() {
            Ok(serde_json::Value::Array(items)) => kernel_docs_from_json(&items),
            _ => Vec::new(),
        })
    }

    /// `get_pipe_id(pipe)`: `GET /pipes/{keys}/id`.
    ///
    /// PARITY: a string ID is returned as the raw response text (with its JSON quotes).
    pub async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        let r_url = Self::metadata_r_url(pipe) + "/id";
        let response = self
            .get(
                &r_url,
                RequestOptions {
                    query: vec![self.instance_query(pipe, "instance")],
                    ..Default::default()
                },
            )
            .await?;
        if Self::is_pipeline_metadata(pipe) {
            // `{"success": true, "message": ..., "id": ...}`, or `404` when not registered.
            if response.status == 404 {
                return Ok(None);
            }
            return Ok(match response.envelope_field("id")? {
                serde_json::Value::Number(n) => n.as_i64().map(PipeId::Int),
                serde_json::Value::String(s) if !s.is_empty() => Some(PipeId::Str(s)),
                _ => None,
            });
        }
        let text = response.text();
        if meerschaum_core::pyfmt::is_int(&text)
            && let Ok(i) = text.trim().parse::<i64>()
        {
            return Ok(Some(PipeId::Int(i)));
        }
        if !text.is_empty() && !text.starts_with('{') {
            return Ok(Some(PipeId::Str(text)));
        }
        Ok(None)
    }

    /// `get_pipe_attributes(pipe)`: `GET /pipes/{keys}/attributes` (errors raise).
    pub async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        let r_url = Self::metadata_r_url(pipe) + "/attributes";
        let response = self
            .get(
                &r_url,
                RequestOptions {
                    query: vec![self.instance_query(pipe, "instance")],
                    ..Default::default()
                },
            )
            .await?;
        if Self::is_pipeline_metadata(pipe) {
            // `{"success": true, ..., "attributes": {...}}`; an unregistered Pipeline (`404`) has
            // no attributes, like an unregistered pipe.
            if response.status == 404 {
                return Ok(Map::new());
            }
            return match pipe_ops::json_to_core(&response.envelope_field("attributes")?) {
                Value::Map(m) => Ok(m),
                Value::Null => Ok(Map::new()),
                _ => Err(ConnectorError::Invalid(format!(
                    "Invalid attributes response for {pipe}."
                ))),
            };
        }
        if !response.ok() {
            return Err(ConnectorError::Http {
                status: response.status,
                body: response.text(),
            });
        }
        let attrs = pipe_ops::json_to_core(&response.json()?);
        match attrs {
            Value::Map(m) if m.is_empty() || matches!(m.get("parameters"), Some(Value::Map(_))) => {
                Ok(m)
            }
            _ => Err(ConnectorError::Invalid(format!(
                "Invalid attributes response for {pipe}."
            ))),
        }
    }

    /// `get_sync_time(pipe, params, newest)`: `GET /pipes/{keys}/sync_time` with the params as a
    /// JSON body.
    pub async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        let r_url = Self::pipe_r_url(pipe) + "/sync_time";
        let opts = RequestOptions {
            query: vec![
                self.instance_query(pipe, "instance"),
                ("newest".into(), QueryValue::bool(newest)),
                ("debug".into(), QueryValue::bool(false)),
            ],
            body: match params {
                Some(p) => Body::Json(py_json(&Value::Map(p.clone()))),
                None => Body::None,
            },
            ..Default::default()
        };
        let response = self.get(&r_url, opts).await?;
        if !response.ok() {
            if let Some(e) = server_failed(pipe, "the sync time", &response) {
                return Err(e);
            }
            super::warnings::warn_pipe_fmt!(
                pipe,
                "Failed to get the sync time for {pipe}:\n{}",
                response.text()
            );
            return Ok(None);
        }
        Ok(match response.json()? {
            serde_json::Value::Null => None,
            serde_json::Value::Number(n) => n.as_i64().map(DateBound::Int),
            serde_json::Value::String(s) => {
                if meerschaum_core::pyfmt::is_int(&s) {
                    s.trim().parse().ok().map(DateBound::Int)
                } else {
                    match meerschaum_kernel::value::parse_datetime_value(&s) {
                        Some(d) => Some(DateBound::Datetime(d)),
                        None => {
                            // `datetime.fromisoformat` raises `Invalid isoformat string: '...'`.
                            super::warnings::warn_pipe_fmt!(
                                pipe,
                                "Failed to parse the sync time '{s}' for {pipe}:\nInvalid isoformat string: '{s}'"
                            );
                            None
                        }
                    }
                }
            }
            _ => None,
        })
    }

    /// `pipe_exists(pipe)`: `GET /pipes/{keys}/exists`.
    pub async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        if crate::engine_pipes::is_engine_pipe(pipe) {
            // The engine probes its state pipes; a server that cannot answer (an older server,
            // a failure) means "not there yet", without a warning, as before the routes existed.
            return Ok(self
                .engine_pipe_exists(pipe.metric_key())
                .await
                .unwrap_or(false));
        }
        let r_url = Self::pipe_r_url(pipe) + "/exists";
        let response = self
            .get(
                &r_url,
                RequestOptions {
                    query: vec![self.instance_query(pipe, "instance")],
                    ..Default::default()
                },
            )
            .await?;
        if !response.ok() {
            super::warnings::warn_pipe_fmt!(
                pipe,
                "Failed to check if {pipe} exists:\n{}",
                response.text()
            );
            return Ok(false);
        }
        Ok(match response.json()? {
            serde_json::Value::Bool(b) => b,
            // PARITY: v4 returns the `{"detail": ...}` dict itself (truthy).
            serde_json::Value::Object(o) => {
                if let Some(d) = o.get("detail") {
                    super::warnings::warn_pipe(pipe, &super::warnings::detail_text(d));
                }
                !o.is_empty()
            }
            other => other.as_i64().is_some_and(|i| i != 0),
        })
    }

    /// `create_metadata()`: `POST /metadata`.
    ///
    /// PARITY: v4 always returns `False` (`_pipes.py:607`).
    pub async fn create_metadata(&self) -> bool {
        if let Ok(r) = self.post(routes::METADATA, RequestOptions::default()).await
            && let Err(e) = r.json()
        {
            super::warnings::warn_fmt!("Failed to create metadata on {self}:\n{e}");
        }
        false
    }

    /// `get_pipe_rowcount(pipe, begin, end, params, remote)`: `GET /pipes/{keys}/rowcount` with
    /// the params as a JSON body (0 on failure).
    pub async fn get_pipe_rowcount(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<i64> {
        let r_url = Self::pipe_r_url(pipe) + "/rowcount";
        let opts = RequestOptions {
            query: vec![
                ("begin".into(), bound_query(&query.begin)),
                ("end".into(), bound_query(&query.end)),
                ("remote".into(), QueryValue::bool(query.remote)),
                self.instance_query(pipe, "instance"),
            ],
            body: match &query.params {
                Some(p) => Body::Json(py_json(&Value::Map(p.clone()))),
                None => Body::None,
            },
            ..Default::default()
        };
        let response = self.get(&r_url, opts).await?;
        if !response.ok() {
            if let Some(e) = server_failed(pipe, "the rowcount", &response) {
                return Err(e);
            }
            super::warnings::warn_pipe_fmt!(
                pipe,
                "Failed to get the rowcount for {pipe}:\n{}",
                response.text()
            );
            return Ok(0);
        }
        Ok(match response.json() {
            Ok(serde_json::Value::Number(n)) => n.as_i64().unwrap_or(0),
            Ok(serde_json::Value::String(s)) => s.trim().parse().unwrap_or(0),
            _ => 0,
        })
    }

    /// `get_pipe_rowcount(pipe, estimate=True)`: `GET /pipes/{keys}/rowcount?estimate=true`
    /// (the server's catalog estimate, contract C1). Bounds, `params` and `remote` make the
    /// count exact; a server that refuses the parameter (a `4xx`) is asked for the exact count.
    pub async fn get_pipe_rowcount_estimate(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<i64> {
        let filtered = query.remote
            || query.begin.is_some()
            || query.end.is_some()
            || query.params.as_ref().is_some_and(|p| !p.is_empty());
        if filtered {
            return self.get_pipe_rowcount(pipe, query).await;
        }
        let r_url = Self::pipe_r_url(pipe) + "/rowcount";
        let opts = RequestOptions {
            query: vec![
                ("begin".into(), bound_query(&None)),
                ("end".into(), bound_query(&None)),
                ("remote".into(), QueryValue::bool(false)),
                ("estimate".into(), QueryValue::bool(true)),
                self.instance_query(pipe, "instance"),
            ],
            ..Default::default()
        };
        let response = self.get(&r_url, opts).await?;
        if (400..500).contains(&response.status) {
            return self.get_pipe_rowcount(pipe, query).await;
        }
        if !response.ok() {
            if let Some(e) = server_failed(pipe, "the rowcount", &response) {
                return Err(e);
            }
            super::warnings::warn_pipe_fmt!(
                pipe,
                "Failed to get the rowcount for {pipe}:\n{}",
                response.text()
            );
            return Ok(0);
        }
        Ok(match response.json() {
            Ok(serde_json::Value::Number(n)) => n.as_i64().unwrap_or(0),
            Ok(serde_json::Value::String(s)) => s.trim().parse().unwrap_or(0),
            _ => 0,
        })
    }

    /// `drop_pipe(pipe)`: `DELETE /pipes/{keys}/drop`.
    pub async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::protected_refusal(self, pipe, "drop") {
            return refused;
        }
        let r_url = Self::pipe_r_url(pipe) + "/drop";
        let response = match self
            .delete(
                &r_url,
                RequestOptions {
                    query: vec![self.instance_query(pipe, "instance")],
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match response.json() {
            Ok(data) => tuple_from_json(&response, &data),
            Err(_) => SuccessTuple::fail(format!("Failed to drop {pipe}.")),
        }
    }

    /// `clear_pipe(pipe, begin, end, params)`: `DELETE /pipes/{keys}/clear`.
    pub async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        if let Some(refused) = meerschaum_connector::protected_refusal(self, pipe, "clear") {
            return refused;
        }
        if crate::engine_pipes::is_engine_pipe(pipe) {
            let q = crate::engine_pipes::EnginePipeQuery::from_data_query(query);
            return self.clear_engine_pipe(pipe.metric_key(), &q).await;
        }
        let r_url = Self::pipe_r_url(pipe) + "/clear";
        let opts = RequestOptions::query(vec![
            ("begin", bound_query(&query.begin)),
            ("end", bound_query(&query.end)),
            ("params", py_json_opt_map(&query.params).into()),
            (
                "instance",
                QueryValue::opt(self.get_pipe_instance_keys(pipe)),
            ),
        ]);
        let response = match self.delete(&r_url, opts).await {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match response.json() {
            Ok(data) => tuple_from_json(&response, &data),
            Err(_) => SuccessTuple::fail(format!(
                "Failed to clear {pipe} with constraints begin={}, end={}, params={}.",
                query.begin.as_ref().map_or("None".into(), |b| b.py_str()),
                query.end.as_ref().map_or("None".into(), |b| b.py_str()),
                query.params.as_ref().map_or("None".into(), |p| {
                    meerschaum_core::pyfmt::py_repr(&Value::Map(p.clone()))
                }),
            )),
        }
    }

    async fn get_json_dict(
        &self,
        pipe: &Pipe,
        suffix: &str,
    ) -> ConnectorResult<Option<serde_json::Map<String, serde_json::Value>>> {
        let r_url = Self::pipe_r_url(pipe) + suffix;
        let response = self
            .get(
                &r_url,
                RequestOptions {
                    query: vec![self.instance_query(pipe, "instance")],
                    ..Default::default()
                },
            )
            .await?;
        let j = response.json()?;
        match j {
            serde_json::Value::Object(o) if o.len() == 1 && o.contains_key("detail") => {
                super::warnings::warn_pipe(pipe, &super::warnings::detail_text(&o["detail"]));
                Ok(None)
            }
            serde_json::Value::Object(o) => Ok(Some(o)),
            _ => {
                super::warnings::warn_pipe_fmt!(pipe, "{}", response.text());
                Ok(None)
            }
        }
    }

    /// `get_pipe_columns_types(pipe)`: `GET /pipes/{keys}/columns/types` (`None` on errors).
    pub async fn get_pipe_columns_types_opt(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<Option<IndexMap<String, String>>> {
        Ok(self.get_json_dict(pipe, "/columns/types").await?.map(|o| {
            o.into_iter()
                .map(|(k, v)| {
                    let t = match v {
                        serde_json::Value::String(s) => s,
                        other => other.to_string(),
                    };
                    (k, t)
                })
                .collect()
        }))
    }

    /// `get_pipe_columns_indices(pipe)`: `GET /pipes/{keys}/columns/indices`.
    pub async fn get_pipe_columns_indices_opt(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<Option<IndexMap<String, Vec<ColumnIndexInfo>>>> {
        Ok(self
            .get_json_dict(pipe, "/columns/indices")
            .await?
            .map(|o| {
                o.into_iter()
                    .map(|(k, v)| (k, serde_json::from_value(v).unwrap_or_default()))
                    .collect()
            }))
    }

    /// `get_pipe_index_names(pipe)`: `GET /pipes/{keys}/indices/names` (`{}` on errors).
    ///
    /// Defined in v4's `_pipes.py` but not attached to the class; ported as written.
    pub async fn get_pipe_index_names(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        Ok(self
            .get_json_dict(pipe, "/indices/names")
            .await?
            .map(|o| {
                o.into_iter()
                    .map(|(k, v)| (k, pipe_ops::json_to_core(&v)))
                    .collect()
            })
            .unwrap_or_default())
    }

    /// `get_pipe_size(pipe)`: `GET /pipes/{keys}/size`.
    pub async fn get_pipe_size(&self, pipe: &Pipe) -> ConnectorResult<Option<i64>> {
        let r_url = Self::pipe_r_url(pipe) + "/size";
        let response = self
            .get(
                &r_url,
                RequestOptions {
                    query: vec![self.instance_query(pipe, "instance")],
                    ..Default::default()
                },
            )
            .await?;
        if !response.ok() {
            super::warnings::warn_pipe_fmt!(
                pipe,
                "Failed to get the size for {pipe}:\n{}",
                response.text()
            );
            return Ok(None);
        }
        Ok(match response.json() {
            Ok(serde_json::Value::Number(n)) => n.as_i64().or_else(|| n.as_f64().map(|f| f as i64)),
            Ok(serde_json::Value::String(s)) => s.trim().parse().ok(),
            Ok(serde_json::Value::Object(o)) => {
                if let Some(d) = o.get("detail") {
                    super::warnings::warn_pipe(pipe, &super::warnings::detail_text(d));
                }
                None
            }
            _ => None,
        })
    }

    async fn maintenance(
        &self,
        pipe: &Pipe,
        route: &str,
        verb: &str,
        extra: Vec<(String, QueryValue)>,
    ) -> SuccessTuple {
        if verb != "analyze"
            && let Some(refused) = meerschaum_connector::protected_refusal(self, pipe, verb)
        {
            return refused;
        }
        let r_url = format!("{}/{route}", Self::pipe_r_url(pipe));
        let mut query = vec![self.instance_query(pipe, "instance_keys")];
        query.extend(extra);
        let response = match self
            .post(
                &r_url,
                RequestOptions {
                    query,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(r) => r,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        match response.json() {
            Ok(data) => tuple_from_json(&response, &data),
            Err(_) => SuccessTuple::fail(format!("Failed to {verb} {pipe}.")),
        }
    }

    /// `compress_pipe(pipe)`: `POST /pipes/{keys}/compress`.
    pub async fn compress_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.maintenance(pipe, "compress", "compress", vec![]).await
    }

    /// `decompress_pipe(pipe, no_policy)`: `POST /pipes/{keys}/decompress`.
    pub async fn decompress_pipe(&self, pipe: &Pipe, no_policy: bool) -> SuccessTuple {
        self.maintenance(
            pipe,
            "decompress",
            "decompress",
            vec![("no_policy".into(), QueryValue::bool(no_policy))],
        )
        .await
    }

    /// `vacuum_pipe(pipe, full)`: `POST /pipes/{keys}/vacuum`.
    pub async fn vacuum_pipe(&self, pipe: &Pipe, full: bool) -> SuccessTuple {
        self.maintenance(
            pipe,
            "vacuum",
            "vacuum",
            vec![("full".into(), QueryValue::bool(full))],
        )
        .await
    }

    /// `analyze_pipe(pipe)`: `POST /pipes/{keys}/analyze`.
    pub async fn analyze_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.maintenance(pipe, "analyze", "analyze", vec![]).await
    }

    /// `partition_pipe(pipe, chunk_minutes)`: `POST /pipes/{keys}/partition`.
    pub async fn partition_pipe(&self, pipe: &Pipe, chunk_minutes: Option<i64>) -> SuccessTuple {
        let extra = chunk_minutes
            .map(|m| vec![("chunk_minutes".to_string(), QueryValue::from(m))])
            .unwrap_or_default();
        self.maintenance(pipe, "partition", "repartition", extra)
            .await
    }

    /// `GET /pipes/{keys}/chunk_bounds` (no v4 client method; used by `verify` over the API).
    pub async fn get_pipe_chunk_bounds(
        &self,
        pipe: &Pipe,
        begin: Option<&DateBound>,
        end: Option<&DateBound>,
        bounded: bool,
        chunk_interval_minutes: Option<i64>,
    ) -> ConnectorResult<Vec<(Option<DateBound>, Option<DateBound>)>> {
        let r_url = Self::pipe_r_url(pipe) + "/chunk_bounds";
        let opts = RequestOptions::query(vec![
            (
                "instance_keys",
                QueryValue::opt(self.get_pipe_instance_keys(pipe)),
            ),
            ("begin", bound_query(&begin.cloned())),
            ("end", bound_query(&end.cloned())),
            ("bounded", QueryValue::bool(bounded)),
            (
                "chunk_interval_minutes",
                chunk_interval_minutes
                    .map(QueryValue::from)
                    .unwrap_or(QueryValue::None),
            ),
        ]);
        let j = self.get(&r_url, opts).await?.json_or_detail()?;
        let parse = |v: &serde_json::Value| match v {
            serde_json::Value::Number(n) => n.as_i64().map(DateBound::Int),
            serde_json::Value::String(s) => DateBound::parse(s),
            _ => None,
        };
        Ok(j.as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|pair| {
                        let p = pair.as_array()?;
                        Some((parse(p.first()?), parse(p.get(1)?)))
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// `GET /pipes/{keys}/csv`: the pipe's data as CSV text.
    pub async fn get_pipe_csv(&self, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<String> {
        let r_url = Self::pipe_r_url(pipe) + "/csv";
        let opts = RequestOptions::query(vec![
            (
                "instance_keys",
                QueryValue::opt(self.get_pipe_instance_keys(pipe)),
            ),
            ("begin", bound_query(&query.begin)),
            ("end", bound_query(&query.end)),
            (
                "params",
                QueryValue::opt(
                    query
                        .params
                        .as_ref()
                        .map(|p| py_json(&Value::Map(p.clone()))),
                ),
            ),
        ]);
        let r = self.get(&r_url, opts).await?;
        if !r.ok() {
            return Err(ConnectorError::Http {
                status: r.status,
                body: r.text(),
            });
        }
        Ok(r.text())
    }

    /// `GET /pipes` (and `/pipes/{connector_keys}[/{metric_key}[/{location_key}]]`): registered
    /// pipes' attributes, nested by keys.
    pub async fn get_pipes_attributes(
        &self,
        connector_keys: Option<&str>,
        metric_key: Option<&str>,
        location_key: Option<Option<&str>>,
    ) -> ConnectorResult<Value> {
        let mut path = routes::PIPES.to_string();
        if let Some(ck) = connector_keys {
            path.push('/');
            path.push_str(ck);
            if let Some(mk) = metric_key {
                path.push('/');
                path.push_str(mk);
                if let Some(lk) = location_key {
                    path.push('/');
                    path.push_str(lk.unwrap_or("[None]"));
                }
            }
        }
        let opts = RequestOptions::query(vec![(
            "instance_keys",
            QueryValue::opt(self.instance_keys()),
        )]);
        Ok(pipe_ops::json_to_core(
            &self.get(&path, opts).await?.json_or_detail()?,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn timespans() {
        assert_eq!(format_timespan(0.48), "0.48 seconds");
        assert_eq!(format_timespan(1.0), "1 second");
        assert_eq!(format_timespan(1.5), "1.5 second");
        assert_eq!(format_timespan(0.0), "0 seconds");
        assert_eq!(format_timespan(61.0), "1 minute and 1 second");
        assert_eq!(format_timespan(3723.5), "1 hour, 2 minutes and 3.5 seconds");
        assert_eq!(thousands(1234567), "1,234,567");
        assert_eq!(
            py_json(&Value::List(vec![Value::Str("a".into()), Value::Int(1)])),
            "[\"a\", 1]"
        );
    }
}
