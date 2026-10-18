//! Pipe methods (`meerschaum/connectors/valkey/_pipes.py`).

use indexmap::IndexMap;
use meerschaum_connector::pipe_ops::{self, Doc};
use meerschaum_connector::traits::{
    ConnectorError, ConnectorResult, DataQuery, DateBound, PipeKeysEntry, PipesKeysQuery,
    SyncOptions,
};
use meerschaum_core::pyfmt::{JsonStyle, json_dumps, py_str};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::diff::{BacktrackRequest, FilterExistingParams, filter_existing};
use meerschaum_kernel::enforce::{PipeInput, pandas_astype, pipe_enforce_dtypes};
use meerschaum_kernel::query::{QueryOptions, query_df};
use meerschaum_kernel::value::Value as KValue;

use crate::connector::{ValkeyConnector, be, docs_frame};
use crate::keys::{
    COLON, PIPES_COUNTER, PIPES_TABLE, get_document_key, get_pipe_key, get_pipe_parameters_key,
    quote_table,
};
use crate::pyjson::{loads_doc, serialize_document};

/// `pipe.target` on a Valkey instance (no per-flavor truncation).
pub fn pipe_target(pipe: &Pipe) -> String {
    pipe.target(None).unwrap_or_else(|_| pipe.target_legacy())
}

/// `[col for col in pipe.columns.values() if col]`.
pub fn index_columns(pipe: &Pipe) -> Vec<String> {
    pipe.columns()
        .values()
        .filter(|v| v.truthy())
        .map(py_str)
        .collect()
}

/// `pipe.parameters.get('valkey', {}).get('dtypes', {})`.
pub fn valkey_dtypes(parameters: &Map) -> IndexMap<String, String> {
    match parameters.get("valkey").and_then(|v| v.get("dtypes")) {
        Some(Value::Map(m)) => m.iter().map(|(k, v)| (k.clone(), py_str(v))).collect(),
        _ => IndexMap::new(),
    }
}

/// `json.dumps(parameters, separators=(',', ':'))`.
pub fn dumps_parameters(parameters: &Map) -> String {
    json_dumps(&Value::Map(parameters.clone()), JsonStyle::Compact, true)
}

/// The dtype names pandas reports for a frame after `Pipe.enforce_dtypes()` (what v4 stores
/// under `parameters['valkey']['dtypes']`).
///
/// PARITY: the kernel names Polars types after NumPy (`int64`, `float64`,
/// `datetime64[us, UTC]`), but v4 sees Arrow-backed or nullable pandas dtypes after
/// enforcement: explicitly typed columns become `[pyarrow]` types and inferred ones nullable
/// extension types. This maps the kernel names onto the names v4 writes.
pub fn python_dtype_name(kernel_dtype: &str, configured: Option<&str>) -> String {
    let configured_int =
        configured.is_some_and(|c| meerschaum_core::dtypes::are_dtypes_equal(c, "int"));
    if let Some(rest) = kernel_dtype.strip_prefix("datetime64[") {
        let inner = rest.trim_end_matches(']');
        let mut parts = inner.splitn(2, ',');
        let unit = parts.next().unwrap_or("us").trim();
        return match parts.next() {
            Some(tz) => format!("timestamp[{unit}, tz={}][pyarrow]", tz.trim()),
            None => format!("timestamp[{unit}][pyarrow]"),
        };
    }
    match kernel_dtype {
        "int64" if configured_int => "int64[pyarrow]".into(),
        "int64" => "Int64".into(),
        "int32" => "Int32".into(),
        "int16" => "Int16".into(),
        "int8" => "Int8".into(),
        "float64" => "Float64".into(),
        "float32" => "Float32".into(),
        "bool" => "boolean".into(),
        other => other.to_string(),
    }
}

/// The parameters `Pipe.get_parameters(refresh=True)` returns: the persisted parameters of a
/// registered, non-temporary pipe, else the in-memory ones.
async fn refreshed_parameters(conn: &ValkeyConnector, pipe: &Pipe) -> ConnectorResult<Map> {
    if pipe.temporary {
        return Ok(pipe.raw_parameters().clone());
    }
    Ok(conn
        .get_pipe_parameters(pipe)
        .await?
        .unwrap_or_else(|| pipe.raw_parameters().clone()))
}

/// The `DataQuery` for a `filter_existing` backtrack request.
fn backtrack_query(req: &BacktrackRequest) -> DataQuery {
    let params: Map = req
        .params
        .iter()
        .map(|(k, vals)| {
            (
                k.clone(),
                Value::List(vals.iter().map(pipe_ops::kernel_to_core).collect()),
            )
        })
        .collect();
    DataQuery {
        begin: DateBound::from_value(&req.begin),
        end: DateBound::from_value(&req.end),
        params: Some(params),
        ..Default::default()
    }
}

impl ValkeyConnector {
    /// The persisted parameters of a pipe (`None` if it is not registered).
    pub async fn get_pipe_parameters(&self, pipe: &Pipe) -> ConnectorResult<Option<Map>> {
        if self.get_pipe_id(pipe).await?.is_none() {
            return Ok(None);
        }
        let key = get_pipe_parameters_key(
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        );
        Ok(Some(match self.get(&key).await? {
            Some(s) if !s.is_empty() => Value::from_json_str(&s)
                .map(|v| v.to_map())
                .unwrap_or_default(),
            _ => Map::new(),
        }))
    }

    /// Insert the pipe's attributes into the internal `pipes` table.
    pub async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let pipe_key = get_pipe_key(
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        );
        let parameters_key = get_pipe_parameters_key(
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        );
        let parameters_str = dumps_parameters(pipe.raw_parameters());
        let result: ConnectorResult<Option<SuccessTuple>> = async {
            if self.get(&pipe_key).await?.is_some() {
                return Ok(Some(SuccessTuple::fail(format!(
                    "{pipe} is already registered."
                ))));
            }
            let pipe_id = self.backend.incr(PIPES_COUNTER).await.map_err(be)?;
            let doc: Doc = [
                ("pipe_id".to_string(), KValue::Int(pipe_id)),
                (
                    "connector_keys".to_string(),
                    KValue::Str(pipe.connector_keys().to_string()),
                ),
                (
                    "metric_key".to_string(),
                    KValue::Str(pipe.metric_key().to_string()),
                ),
                (
                    "location_key".to_string(),
                    KValue::Str(pipe.location_key().unwrap_or("None").to_string()),
                ),
            ]
            .into_iter()
            .collect();
            self.push_docs(&[doc], PIPES_TABLE, Some("pipe_id")).await?;
            self.set(&pipe_key, &pipe_id.to_string()).await?;
            self.set(&parameters_key, &parameters_str).await?;
            Ok(None)
        }
        .await;
        match result {
            Ok(Some(fail)) => fail,
            Ok(None) => SuccessTuple::success(),
            Err(e) => SuccessTuple::fail(format!("Failed to register {pipe}:\n{e}")),
        }
    }

    /// Return the ID for the pipe if it exists (`int(get(pipe_key))`).
    pub async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<i64>> {
        let pipe_key = get_pipe_key(
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        );
        Ok(self
            .get(&pipe_key)
            .await
            .ok()
            .flatten()
            .and_then(|s| s.trim().parse::<i64>().ok()))
    }

    /// Return the pipe's document: keys, `parameters` and `pipe_id` (`{}` if unregistered).
    pub async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        let Some(pipe_id) = self.get_pipe_id(pipe).await? else {
            return Ok(Map::new());
        };
        let parameters = self.get_pipe_parameters(pipe).await?.unwrap_or_default();
        let mut attrs = Map::new();
        attrs.insert(
            "connector_keys".into(),
            Value::Str(pipe.connector_keys().to_string()),
        );
        attrs.insert(
            "metric_key".into(),
            Value::Str(pipe.metric_key().to_string()),
        );
        attrs.insert(
            "location_key".into(),
            pipe.location_key().map(String::from).into(),
        );
        attrs.insert("parameters".into(), Value::Map(parameters));
        attrs.insert("pipe_id".into(), Value::Int(pipe_id));
        Ok(attrs)
    }

    /// Persist the pipe's in-memory parameters.
    pub async fn edit_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        match self.get_pipe_id(pipe).await {
            Ok(Some(_)) => {}
            Ok(None) => return SuccessTuple::fail(format!("{pipe} is not registered.")),
            Err(e) => return SuccessTuple::fail(e.to_string()),
        }
        let key = get_pipe_parameters_key(
            pipe.connector_keys(),
            pipe.metric_key(),
            pipe.location_key(),
        );
        match self
            .set(&key, &dumps_parameters(pipe.raw_parameters()))
            .await
        {
            Ok(()) => SuccessTuple::success(),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// Check whether a pipe's target "table" exists.
    pub async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        let table_name = quote_table(&pipe_target(pipe));
        Ok(self.backend.exists(&table_name).await.map_err(be)? != 0)
    }

    /// Drop a pipe's documents, its table and the stored dtypes.
    pub async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        match self.pipe_exists(pipe).await {
            Ok(true) => {}
            Ok(false) => {
                return SuccessTuple::ok(format!("{pipe} does not exist, so it was not dropped."));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        }
        let target = pipe_target(pipe);
        let table_name = quote_table(&target);
        let dt_col = pipe.column("datetime");

        let deleted: ConnectorResult<()> = async {
            let members: Vec<String> = if dt_col.is_some() {
                self.backend
                    .zrange(&table_name, 0, -1, false)
                    .await
                    .map_err(be)?
                    .into_iter()
                    .map(|(m, _)| m)
                    .collect()
            } else {
                self.backend.smembers(&table_name).await.map_err(be)?
            };
            let mut keys_to_delete = Vec::new();
            for member in members {
                let doc = loads_doc(&member)?;
                let ix_str = match doc.get("ix") {
                    Some(KValue::Str(s)) if !s.is_empty() => s.clone(),
                    _ => continue,
                };
                let ix_doc = ix_string_to_doc(&ix_str);
                let keys: Vec<String> = ix_doc.keys().cloned().collect();
                keys_to_delete.push(get_document_key(&ix_doc, &keys, Some(&table_name)));
            }
            for batch in keys_to_delete.chunks(1000) {
                self.backend.del(batch).await.map_err(be)?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = deleted {
            return SuccessTuple::fail(format!("Failed to delete documents for {pipe}:\n{e}"));
        }
        if let Err(e) = self.drop_table(&target).await {
            return SuccessTuple::fail(format!("Failed to drop {pipe}:\n{e}"));
        }

        // PARITY: v4 resets `valkey.dtypes` in the *in-memory* parameters and persists them all
        // (`_pipes.py`, `drop_pipe`). A pipe built without `valkey` in memory falls back to the
        // persisted parameters so the reset is not silently skipped.
        let mut parameters = pipe.raw_parameters().clone();
        if !parameters.contains_key("valkey")
            && !pipe.temporary
            && let Ok(Some(persisted)) = self.get_pipe_parameters(pipe).await
            && persisted.contains_key("valkey")
        {
            parameters = persisted;
        }
        if !parameters.contains_key("valkey") {
            return SuccessTuple::success();
        }
        if let Some(Value::Map(vk)) = parameters.get_mut("valkey") {
            vk.insert("dtypes".into(), Value::Map(Map::new()));
        }
        if !pipe.temporary {
            let mut edited = pipe.clone();
            edited.set_parameters(parameters);
            let res = self.edit_pipe(&edited).await;
            if !res.success {
                return res;
            }
        }
        SuccessTuple::success()
    }

    /// Drop the pipe and delete its registration.
    pub async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let dropped = self.drop_pipe(pipe).await;
        if !dropped.success {
            return dropped;
        }
        let pipe_id = match self.get_pipe_id(pipe).await {
            Ok(Some(id)) => id,
            Ok(None) => return SuccessTuple::fail(format!("{pipe} is not registered.")),
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let res: ConnectorResult<()> = async {
            let pipe_key = get_pipe_key(
                pipe.connector_keys(),
                pipe.metric_key(),
                pipe.location_key(),
            );
            let parameters_key = get_pipe_parameters_key(
                pipe.connector_keys(),
                pipe.metric_key(),
                pipe.location_key(),
            );
            self.backend.del(&[pipe_key]).await.map_err(be)?;
            self.backend.del(&[parameters_key]).await.map_err(be)?;
            let mut params = Map::new();
            params.insert("pipe_id".into(), Value::Int(pipe_id));
            let df = self
                .read(PIPES_TABLE, None, None, Some(&params), None, None, None)
                .await?;
            if let Some(doc) = df
                .map(|df| pipe_ops::frame_to_docs(&df))
                .and_then(|d| d.into_iter().next())
            {
                self.backend
                    .zrem(PIPES_TABLE, &serialize_document(&doc))
                    .await
                    .map_err(be)?;
            }
            Ok(())
        }
        .await;
        match res {
            Ok(()) => SuccessTuple::success(),
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    /// Query a pipe's documents and return the DataFrame (`None` if the table does not exist).
    pub async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        if !self.pipe_exists(pipe).await? {
            return Ok(None);
        }
        let parameters = pipe.parameters();
        let vk_dtypes = valkey_dtypes(parameters);
        let dt_col = pipe.column("datetime");
        let target = pipe_target(pipe);
        let table_name = quote_table(&target);
        let indices = index_columns(pipe);
        let begin = query.begin.as_ref().map(DateBound::to_value);
        let end = query.end.as_ref().map(DateBound::to_value);

        let ix_docs: Vec<Doc> = self
            .read_docs(&target, begin.as_ref(), end.as_ref())
            .await?
            .into_iter()
            .map(|doc| match doc.get("ix") {
                Some(KValue::Str(s)) => ix_string_to_doc(s),
                _ => Doc::new(),
            })
            .collect();
        let mut docs: Vec<Doc> = Vec::new();
        for ix_doc in &ix_docs {
            let key = get_document_key(ix_doc, &indices, Some(&table_name));
            match self.get(&key).await {
                Ok(Some(s)) if !s.is_empty() => docs.push(loads_doc(&s)?),
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!("Failed to fetch documents for {pipe}:\n{e}");
                    docs.clear();
                    break;
                }
            }
        }

        let ctx = pipe_ops::dtypes_context(self, pipe, parameters).await?;
        let ignore_dt_cols: Vec<String> = ctx
            .dtypes
            .iter()
            .filter(|(_, t)| !meerschaum_core::dtypes::are_dtypes_equal(t, "datetime"))
            .map(|(c, _)| c.clone())
            .collect();
        let mut df = docs_frame(&docs, &ignore_dt_cols, !ctx.tz_aware)?;
        for (col, typ) in &vk_dtypes {
            let Ok(data) = df.data(col) else { continue };
            if let Ok(casted) = pandas_astype(col, data, typ) {
                let _ = df.set(col, casted);
            }
        }
        let df = pipe_enforce_dtypes(PipeInput::Frame(df), &ctx)
            .map_err(|e| ConnectorError::Backend(e.to_string()))?;

        let params = query
            .params
            .as_ref()
            .map(pipe_ops::params_to_query)
            .map(|p| pipe_ops::coerce_params_to_frame(p, &df))
            .unwrap_or_default();
        let opts = if df.height() == 0 {
            QueryOptions {
                select_columns: query.select_columns.clone(),
                omit_columns: query.omit_columns.clone(),
                ..Default::default()
            }
        } else {
            QueryOptions {
                params,
                begin,
                end,
                datetime_column: dt_col,
                select_columns: query.select_columns.clone(),
                omit_columns: query.omit_columns.clone(),
                reset_index: true,
                coerce_types: false,
            }
        };
        let result = query_df(&df, &opts).map_err(|e| ConnectorError::Backend(e.to_string()))?;
        Ok(Some(result.frame))
    }

    /// Upsert new documents into the pipe's "table".
    ///
    /// With `check_existing` (and without `upsert`), existing rows are diffed with
    /// `filter_existing`; new rows are inserted and changed rows merged into their stored
    /// documents. Returns `Inserted {n}, updated {m} rows.` (`Upserted {m} rows.` for upserts).
    pub async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        match self.sync_pipe_inner(pipe, df, opts).await {
            Ok(t) => t,
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    async fn sync_pipe_inner(
        &self,
        pipe: &Pipe,
        mut df: Frame,
        opts: &SyncOptions,
    ) -> ConnectorResult<SuccessTuple> {
        let mut pipe = pipe.clone();
        let dt_col = pipe.column("datetime");
        let indices = index_columns(&pipe);
        let target = pipe_target(&pipe);
        let table_name = quote_table(&target);
        let upsert = pipe.parameters().get("upsert").is_some_and(|v| v.truthy());
        let static_ = pipe.parameters().get("static").is_some_and(|v| v.truthy());

        let refreshed = refreshed_parameters(self, &pipe).await?;
        pipe.set_parameters(refreshed);
        let configured = pipe_ops::configured_dtypes(pipe.parameters());
        let mut vk_dtypes = valkey_dtypes(pipe.parameters());
        let mut new_dtypes: IndexMap<String, String> = df
            .columns()
            .iter()
            .filter(|c| !vk_dtypes.contains_key(&c.name))
            .map(|c| {
                (
                    c.name.clone(),
                    python_dtype_name(
                        &c.data.dtype_str(),
                        configured.get(&c.name).map(|s| s.as_str()),
                    ),
                )
            })
            .collect();
        for (col, typ) in vk_dtypes.clone() {
            let Ok(data) = df.data(&col) else { continue };
            match pandas_astype(&col, data, &typ) {
                Ok(casted) => {
                    let _ = df.set(&col, casted);
                }
                Err(_) => {
                    vk_dtypes.insert(col.clone(), "string".into());
                    new_dtypes.insert(col.clone(), "string".into());
                    if let Ok(casted) = pandas_astype(
                        &col,
                        df.data(&col)
                            .map_err(|e| ConnectorError::Backend(e.to_string()))?,
                        "string",
                    ) {
                        let _ = df.set(&col, casted);
                    }
                }
            }
        }
        if !new_dtypes.is_empty() && (!static_ || vk_dtypes.is_empty()) {
            vk_dtypes.extend(new_dtypes);
            let mut patch = Map::new();
            let mut vk = Map::new();
            vk.insert(
                "dtypes".into(),
                Value::Map(
                    vk_dtypes
                        .iter()
                        .map(|(k, v)| (k.clone(), Value::Str(v.clone())))
                        .collect(),
                ),
            );
            patch.insert("valkey".into(), Value::Map(vk));
            pipe.update_parameters(patch);
            if !pipe.temporary {
                let res = self.edit_pipe(&pipe).await;
                if !res.success {
                    return Ok(res);
                }
            }
        }

        let (unseen_df, update_df, delta_df) = if opts.check_existing && !upsert {
            let ctx = pipe_ops::dtypes_context(self, &pipe, pipe.parameters()).await?;
            let fe_params = FilterExistingParams {
                columns: pipe_ops::columns_map(&pipe),
                configured_dtypes: configured.clone(),
                dtypes_ctx: ctx,
                autoincrement: pipe_ops::bool_param(pipe.parameters(), "autoincrement", false),
                autotime: pipe_ops::bool_param(pipe.parameters(), "autotime", false),
                static_,
                include_unchanged_columns: true,
                date_bound_only: false,
                enforce_dtypes: false,
                filter_params_index_limit: 250,
            };
            // The kernel's callback is synchronous: capture the backtrack request first, fetch
            // it, then run the diff with the fetched frame.
            let mut request: Option<BacktrackRequest> = None;
            let _ = filter_existing(PipeInput::Frame(df.clone()), &fe_params, |req| {
                request = Some(req.clone());
                None
            });
            let backtrack = match &request {
                Some(req) => {
                    let q = backtrack_query(req);
                    self.get_pipe_data(&pipe, &q)
                        .await?
                        .map(|f| pipe_ops::pipe_postprocess(&pipe, f, &q))
                }
                None => None,
            };
            let (u, up, d) = filter_existing(PipeInput::Frame(df), &fe_params, move |_| backtrack)
                .map_err(|e| ConnectorError::Backend(e.to_string()))?;
            (Some(u), Some(up), d)
        } else {
            (None, Some(df.clone()), df)
        };

        let num_insert = unseen_df.as_ref().map_or(0, |d| d.height());
        let num_update = update_df.as_ref().map_or(0, |d| d.height());
        let msg = if !upsert {
            format!("Inserted {num_insert}, updated {num_update} rows.")
        } else {
            format!("Upserted {num_update} rows.")
        };
        if delta_df.height() == 0 {
            return Ok(SuccessTuple::ok(msg));
        }

        let serialize_indices_docs = |docs: &[Doc]| -> Vec<Doc> {
            docs.iter()
                .map(|doc| {
                    let mut ix = Doc::new();
                    ix.insert(
                        "ix".into(),
                        KValue::Str(get_document_key(doc, &indices, None)),
                    );
                    if let Some(dt) = &dt_col {
                        ix.insert(dt.clone(), doc.get(dt).cloned().unwrap_or(KValue::Int(0)));
                    }
                    ix
                })
                .collect()
        };

        let unseen_docs = unseen_df
            .as_ref()
            .map(pipe_ops::frame_to_docs)
            .unwrap_or_default();
        let unseen_indices_docs = serialize_indices_docs(&unseen_docs);
        let mut unseen_ix_vals: IndexMap<String, String> = IndexMap::new();
        for doc in &unseen_docs {
            unseen_ix_vals.insert(
                get_document_key(doc, &indices, Some(&table_name)),
                serialize_document(doc),
            );
        }
        for (key, val) in &unseen_ix_vals {
            if let Err(e) = self.set(key, val).await {
                return Ok(SuccessTuple::fail(format!(
                    "Failed to set keys for {pipe}:\n{e}"
                )));
            }
        }
        if let Err(e) = self
            .push_docs(&unseen_indices_docs, &target, dt_col.as_deref())
            .await
        {
            return Ok(SuccessTuple::fail(format!(
                "Failed to push docs to '{target}':\n{e}"
            )));
        }

        let update_docs = update_df
            .as_ref()
            .map(pipe_ops::frame_to_docs)
            .unwrap_or_default();
        let mut update_ix_docs: IndexMap<String, Doc> = IndexMap::new();
        for doc in update_docs {
            update_ix_docs.insert(get_document_key(&doc, &indices, Some(&table_name)), doc);
        }
        let mut existing_docs: IndexMap<String, Doc> = IndexMap::new();
        if self.pipe_exists(&pipe).await? {
            for key in update_ix_docs.keys() {
                if let Some(data) = self.get(key).await?.filter(|d| !d.is_empty()) {
                    existing_docs.insert(key.clone(), loads_doc(&data)?);
                }
            }
        }
        let new_update_docs: Vec<Doc> = update_ix_docs
            .iter()
            .filter(|(k, _)| !existing_docs.contains_key(*k))
            .map(|(_, d)| d.clone())
            .collect();
        let mut new_ix_vals: IndexMap<String, String> = IndexMap::new();
        for doc in &new_update_docs {
            new_ix_vals.insert(
                get_document_key(doc, &indices, Some(&table_name)),
                serialize_document(doc),
            );
        }
        for (key, val) in &new_ix_vals {
            if let Err(e) = self.set(key, val).await {
                return Ok(SuccessTuple::fail(format!(
                    "Failed to set keys for {pipe}:\n{e}"
                )));
            }
        }
        let old_update_docs: Vec<(String, Doc)> = update_ix_docs
            .iter()
            .filter_map(|(key, doc)| {
                existing_docs.get(key).map(|existing| {
                    let mut merged = existing.clone();
                    for (k, v) in doc {
                        merged.insert(k.clone(), v.clone());
                    }
                    (key.clone(), merged)
                })
            })
            .collect();
        let new_indices_docs = serialize_indices_docs(&new_update_docs);
        if !new_indices_docs.is_empty()
            && let Err(e) = self
                .push_docs(&new_indices_docs, &target, dt_col.as_deref())
                .await
        {
            return Ok(SuccessTuple::fail(format!(
                "Failed to upsert '{target}':\n{e}"
            )));
        }
        for (key, doc) in &old_update_docs {
            if let Err(e) = self.set(key, &serialize_document(doc)).await {
                return Ok(SuccessTuple::fail(format!(
                    "Failed to set keys for {pipe}:\n{e}"
                )));
            }
        }
        Ok(SuccessTuple::ok(msg))
    }

    /// Return the PostgreSQL-style column types of the stored `valkey.dtypes`.
    pub async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        if !self.pipe_exists(pipe).await? {
            return Ok(IndexMap::new());
        }
        Ok(valkey_dtypes(pipe.parameters())
            .into_iter()
            .map(|(col, typ)| (col, crate::dbtypes::get_postgresql_type_from_pd_type(&typ)))
            .collect())
    }

    /// Delete rows within `begin`, `end` and `params`.
    ///
    /// PARITY: v4 removes the index entry with `ZREM` / `SREM` of the bare index key, which never
    /// matches the stored `{"ix": ...}` members, so only the document keys are deleted and the
    /// sorted set keeps stale entries (`_pipes.py`, `clear_pipe`). Reads skip them; the
    /// unfiltered rowcount still counts them.
    pub async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        let dt_col = pipe.column("datetime");
        let q = DataQuery {
            begin: query.begin.clone(),
            end: query.end.clone(),
            params: query.params.clone(),
            ..Default::default()
        };
        let existing = match self.get_pipe_data(pipe, &q).await {
            Ok(Some(df)) => pipe_ops::pipe_postprocess(pipe, df, &q),
            Ok(None) => return SuccessTuple::ok("Deleted 0 rows."),
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        if existing.height() == 0 {
            return SuccessTuple::ok("Deleted 0 rows.");
        }
        let docs = pipe_ops::frame_to_docs(&existing);
        let table_name = quote_table(&pipe_target(pipe));
        let indices = index_columns(pipe);
        for doc in &docs {
            let set_doc_key = get_document_key(doc, &indices, None);
            let table_doc_key = get_document_key(doc, &indices, Some(&table_name));
            let res = async {
                if dt_col.is_some() {
                    self.backend.zrem(&table_name, &set_doc_key).await?;
                } else {
                    self.backend.srem(&table_name, &set_doc_key).await?;
                }
                self.backend.del(&[table_doc_key]).await
            }
            .await;
            if let Err(e) = res {
                return SuccessTuple::fail(format!("Failed to delete documents:\n{e}"));
            }
        }
        let n = docs.len();
        SuccessTuple::ok(format!("Deleted {n} row{}.", if n != 1 { "s" } else { "" }))
    }

    /// Return the newest (or oldest) value of the datetime axis.
    ///
    /// PARITY: `params` is ignored on Valkey (`_pipes.py`, `get_sync_time`).
    pub async fn get_sync_time(
        &self,
        pipe: &Pipe,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        let Some(dt_col) = pipe.column("datetime") else {
            return Ok(None);
        };
        let dt_typ = pipe_ops::configured_dtypes(pipe.parameters())
            .get(&dt_col)
            .cloned()
            .unwrap_or_else(|| "datetime".into());
        let table_name = quote_table(&pipe_target(pipe));
        let vals = match self.backend.zrange(&table_name, 0, 0, newest).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!("{e}");
                return Ok(None);
            }
        };
        let Some((member, _)) = vals.into_iter().next() else {
            return Ok(None);
        };
        let doc = loads_doc(&member)?;
        let Some(val) = doc.get(&dt_col).filter(|v| !v.is_na()) else {
            return Ok(None);
        };
        if meerschaum_core::dtypes::are_dtypes_equal(&dt_typ, "int") {
            return Ok(match val {
                KValue::Int(i) => Some(DateBound::Int(*i)),
                other => other.py_str().trim().parse().ok().map(DateBound::Int),
            });
        }
        Ok(match val {
            KValue::Datetime(d) => Some(DateBound::Datetime(d.clone())),
            other => match meerschaum_kernel::value::parse_datetime_value(&other.py_str()) {
                Some(d) => Some(DateBound::Datetime(d)),
                None => {
                    tracing::warn!("Failed to parse sync time for {pipe}:\n{}", other.py_str());
                    None
                }
            },
        })
    }

    /// Return the number of documents in the pipe's set (filtered through `get_data` when
    /// `begin`, `end` or `params` are given).
    pub async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        if !self.pipe_exists(pipe).await? {
            return Ok(Some(0));
        }
        let table_name = quote_table(&pipe_target(pipe));
        let no_params = query.params.as_ref().is_none_or(|p| p.is_empty());
        if query.begin.is_none() && query.end.is_none() && no_params {
            let n = if pipe.column("datetime").is_some() {
                self.backend.zcard(&table_name).await
            } else {
                self.backend.scard(&table_name).await
            };
            return Ok(n.ok());
        }
        let q = DataQuery {
            begin: query.begin.clone(),
            end: query.end.clone(),
            params: query.params.clone(),
            ..Default::default()
        };
        Ok(Some(
            self.get_pipe_data(pipe, &q)
                .await?
                .map_or(0, |df| df.height() as i64),
        ))
    }

    /// Return the keys for the registered pipes (list form with an empty parameters dict).
    ///
    /// PARITY: the pipes table holds no parameters, so `tags` never match an inclusive tag
    /// (`_pipes.py`, `fetch_pipes_keys`).
    pub async fn fetch_pipes_keys(
        &self,
        query: &PipesKeysQuery,
    ) -> ConnectorResult<Vec<PipeKeysEntry>> {
        let Ok(Some(df)) = self
            .read(PIPES_TABLE, None, None, None, None, None, None)
            .await
        else {
            return Ok(Vec::new());
        };
        if df.height() == 0 {
            return Ok(Vec::new());
        }
        let mut q = Map::new();
        let list = |v: &[String]| Value::List(v.iter().map(|s| Value::Str(s.clone())).collect());
        if !query.connector_keys.is_empty() {
            q.insert("connector_keys".into(), list(&query.connector_keys));
        }
        if !query.metric_keys.is_empty() {
            q.insert("metric_key".into(), list(&query.metric_keys));
        }
        if !query.location_keys.is_empty() {
            let lks: Vec<String> = query
                .location_keys
                .iter()
                .map(|l| l.clone().unwrap_or_else(|| "None".into()))
                .collect();
            q.insert("location_key".into(), list(&lks));
        }
        if let Some(p) = &query.params {
            for (k, v) in p {
                q.insert(k.clone(), v.clone());
            }
        }
        let filtered = if q.is_empty() {
            df
        } else {
            query_df(
                &df,
                &QueryOptions {
                    params: pipe_ops::params_to_query(&q),
                    ..Default::default()
                },
            )
            .map_err(|e| ConnectorError::Backend(e.to_string()))?
            .frame
        };
        let keys: Vec<PipeKeysEntry> = pipe_ops::frame_to_docs(&filtered)
            .iter()
            .map(|doc| {
                let s = |k: &str| doc.get(k).map(|v| v.py_str()).unwrap_or_default();
                PipeKeysEntry {
                    connector_keys: s("connector_keys"),
                    metric_key: s("metric_key"),
                    location_key: Some(s("location_key")),
                    extra: Some(Value::Map(Map::new())),
                }
            })
            .collect();
        if query.tags.is_empty() {
            return Ok(keys);
        }
        let groups: Vec<(Vec<String>, Vec<String>)> = query
            .tags
            .iter()
            .map(|t| {
                let mut inc = Vec::new();
                let mut exc = Vec::new();
                for tag in t.split(',') {
                    match tag.strip_prefix('_') {
                        Some(rest) => exc.push(rest.to_string()),
                        None => inc.push(tag.to_string()),
                    }
                }
                (inc, exc)
            })
            .collect();
        Ok(keys
            .into_iter()
            .filter(|entry| {
                let pipe_tags: Vec<String> = match &entry.extra {
                    Some(Value::Map(m)) => match m.get("tags") {
                        Some(Value::List(items)) => items.iter().map(py_str).collect(),
                        _ => Vec::new(),
                    },
                    _ => Vec::new(),
                };
                // PARITY: a failing group only `continue`s, so every group must pass.
                groups.iter().all(|(inc, exc)| {
                    inc.iter().all(|t| pipe_tags.contains(t))
                        && !exc.iter().any(|t| pipe_tags.contains(t))
                })
            })
            .collect())
    }
}

/// `string_to_dict(ix.replace(COLON, ':'))` as a document of kernel values.
pub fn ix_string_to_doc(ix: &str) -> Doc {
    match meerschaum_core::pyfmt::string_to_dict(&ix.replace(COLON, ":")) {
        Ok(m) => m
            .iter()
            .map(|(k, v)| (k.clone(), pipe_ops::core_to_kernel(v)))
            .collect(),
        Err(_) => Doc::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_dtype_names() {
        assert_eq!(
            python_dtype_name("datetime64[us, UTC]", Some("datetime")),
            "timestamp[us, tz=UTC][pyarrow]"
        );
        assert_eq!(python_dtype_name("int64", Some("int")), "int64[pyarrow]");
        assert_eq!(python_dtype_name("int64", None), "Int64");
        assert_eq!(python_dtype_name("float64", None), "Float64");
        assert_eq!(python_dtype_name("string", None), "string");
    }

    #[test]
    fn ix_round_trip() {
        let doc = ix_string_to_doc("station-_a,ts-_1704067200");
        assert!(matches!(doc.get("station"), Some(KValue::Str(s)) if s == "a"));
        assert!(matches!(doc.get("ts"), Some(KValue::Int(1704067200))));
        let keys: Vec<String> = doc.keys().cloned().collect();
        assert_eq!(
            get_document_key(&doc, &keys, Some("t")),
            "t:indices:station-_a,ts-_1704067200"
        );
    }
}
