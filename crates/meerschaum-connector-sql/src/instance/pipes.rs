//! Pipe registration in the `mrsm_pipes` table (`_pipes.py`: `register_pipe`, `edit_pipe`,
//! `delete_pipe`, `get_pipe_id`, `get_pipe_attributes`, `fetch_pipes_keys`).
//!
//! v4 compiles these queries with SQLAlchemy Core; the Rust statements keep the same predicates
//! and bind every user value as a parameter.

use indexmap::IndexMap;
use meerschaum_connector::{ConnectorResult, PipeId, PipeKeysEntry, PipesKeys, PipesKeysQuery};
use meerschaum_core::value::apply_patch_maps;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::value::Value as KValue;

use crate::driver::SqlParam;
use crate::driver::params::{placeholder, placeholders};
use crate::flavor::Flavor;
use crate::where_clause::negation_prefix;

use super::{SqlConnector, be, frame_docs, kv_int, kv_str};

/// Process-wide count of successful pipe metadata writes (`register_pipe`, `edit_pipe`,
/// `delete_pipe`, including the edits a sync makes) by any [`SqlConnector`].
static METADATA_WRITES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// The number of pipe metadata writes this process has made through any [`SqlConnector`].
///
/// The Python façade keys its memo of cross-pipe `{{ Pipe() }}` template resolutions on it,
/// so a sync that persists new dtypes refreshes the pipes whose templates read them.
pub fn pipe_metadata_epoch() -> u64 {
    METADATA_WRITES.load(std::sync::atomic::Ordering::Acquire)
}

fn bump_metadata_epoch(res: SuccessTuple) -> SuccessTuple {
    if res.success {
        METADATA_WRITES.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
    }
    res
}

/// `separate_negation_values(vals)`.
pub fn separate_negation_values(values: &[String]) -> (Vec<String>, Vec<String>) {
    let prefix = negation_prefix();
    let mut inc = Vec::new();
    let mut exc = Vec::new();
    for v in values {
        match v.strip_prefix(prefix) {
            Some(rest) => exc.push(rest.to_string()),
            None => inc.push(v.clone()),
        }
    }
    (inc, exc)
}

/// Parse the stored `parameters` (JSONB on PostgreSQL, JSON text elsewhere).
///
/// PARITY: a JSON string holding a JSON document is decoded twice; anything unparseable becomes
/// `{}` (`get_pipe_attributes`, `_pipes.py:1718`).
pub fn parse_parameters(v: Option<&KValue>) -> Map {
    match v {
        Some(KValue::Json(j)) => meerschaum_connector::pipe_ops::json_to_core(j).to_map(),
        Some(KValue::Str(s)) => match Value::from_json_str(s) {
            Ok(Value::Map(m)) => m,
            Ok(Value::Str(inner)) if inner.starts_with('{') => Value::from_json_str(&inner)
                .map(|v| v.to_map())
                .unwrap_or_default(),
            _ => Map::new(),
        },
        _ => Map::new(),
    }
}

impl SqlConnector {
    /// The `parameters` bind for this flavor (`json.dumps(parameters)` unless JSONB).
    pub(crate) fn json_param(&self, map: &Map) -> SqlParam {
        if self.flavor.is_json_flavor() {
            SqlParam::Json(serde_json::to_value(Value::Map(map.clone())).unwrap_or_default())
        } else {
            SqlParam::Text(Value::Map(map.clone()).json_dumps())
        }
    }

    /// `register_pipe(pipe)`: insert the pipe's keys and parameters into `mrsm_pipes`.
    pub async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let _ = self.ensure_instance_tables(!pipe.temporary).await;
        match self.get_pipe_id(pipe).await {
            Ok(Some(_)) => return SuccessTuple::fail(format!("{pipe} is already registered.")),
            Ok(None) => {}
            Err(e) => return SuccessTuple::fail(e.to_string()),
        }
        let table = self.instance_table_name("mrsm_pipes");
        let query = format!(
            "INSERT INTO {table} (connector_keys, metric_key, location_key, parameters) VALUES ({})",
            placeholders(self.flavor, 1, 4)
        );
        let params = vec![
            SqlParam::Text(pipe.connector_keys().to_string()),
            SqlParam::Text(pipe.metric_key().to_string()),
            SqlParam::opt_text(pipe.location_key()),
            // Persist the raw definition (templates unresolved), as v4 stores `pipe._attributes['parameters']`.
            self.json_param(pipe.raw_parameters()),
        ];
        bump_metadata_epoch(match self.exec_params(&query, &params).await {
            Some(_) => SuccessTuple::ok(format!("Successfully registered {pipe}.")),
            None => SuccessTuple::fail(format!("Failed to register {pipe}.")),
        })
    }

    /// `get_pipe_id(pipe)`: the pipe's `pipe_id`, or `None` (always `None` for temporary pipes).
    pub async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<i64>> {
        if pipe.temporary {
            return Ok(None);
        }
        if !self.ensure_instance_tables(true).await {
            return Ok(None);
        }
        let table = self.instance_table_name("mrsm_pipes");
        let f = self.flavor;
        let mut params = vec![
            SqlParam::Text(pipe.connector_keys().to_string()),
            SqlParam::Text(pipe.metric_key().to_string()),
        ];
        let loc_clause = match pipe.location_key() {
            Some(lk) => {
                params.push(SqlParam::Text(lk.to_string()));
                format!("{table}.location_key = {}", placeholder(f, 3))
            }
            None => format!("{table}.location_key IS NULL"),
        };
        let query = format!(
            "SELECT {table}.pipe_id \nFROM {table} \nWHERE {table}.connector_keys = {} AND {table}.metric_key = {} AND {loc_clause}",
            placeholder(f, 1),
            placeholder(f, 2)
        );
        let v = self.value_params_catalog(&query, &params).await?;
        Ok(v.and_then(|v| v.as_i64()))
    }

    /// `get_pipe_attributes(pipe)`: the pipe's row from `mrsm_pipes` (`{}` if unregistered).
    pub async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        let Some(id) = self.get_pipe_id(pipe).await? else {
            return Ok(Map::new());
        };
        let table = self.instance_table_name("mrsm_pipes");
        let query = format!(
            "SELECT {table}.pipe_id, {table}.connector_keys, {table}.metric_key, {table}.location_key, {table}.parameters \nFROM {table} \nWHERE {table}.pipe_id = {}",
            placeholder(self.flavor, 1)
        );
        let df = self
            .read_params_catalog(&query, &[SqlParam::Int(id)])
            .await
            .map_err(|e| {
                be(format!(
                    "Failed to read metadata for {pipe} from {self}.\n{e}"
                ))
            })?;
        let docs = frame_docs(&df);
        let Some(doc) = docs.first() else {
            return Ok(Map::new());
        };
        let mut out = Map::new();
        out.insert(
            "pipe_id".into(),
            kv_int(doc.get("pipe_id"))
                .map(Value::Int)
                .unwrap_or(Value::Null),
        );
        for key in ["connector_keys", "metric_key", "location_key"] {
            out.insert(key.into(), kv_str(doc.get(key)).into());
        }
        out.insert(
            "parameters".into(),
            Value::Map(parse_parameters(doc.get("parameters"))),
        );
        Ok(out)
    }

    /// The registered parameters of a pipe (`None` if unregistered).
    pub async fn get_pipe_parameters(&self, pipe: &Pipe) -> ConnectorResult<Option<Map>> {
        let attrs = self.get_pipe_attributes(pipe).await?;
        if attrs.is_empty() {
            return Ok(None);
        }
        Ok(attrs.get("parameters").map(|v| v.to_map()))
    }

    /// `edit_pipe(pipe, patch)`: persist the pipe's in-memory parameters.
    pub async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        let id = match self.get_pipe_id(pipe).await {
            Ok(Some(id)) => id,
            Ok(None) => {
                return SuccessTuple::fail(format!(
                    "{pipe} is not registered and cannot be edited."
                ));
            }
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        // Raw parameters: resolved templates must never be written back (see `register_pipe`).
        let parameters = if !patch {
            pipe.raw_parameters().clone()
        } else {
            let original = match self.get_pipe_parameters(pipe).await {
                Ok(p) => p.unwrap_or_default(),
                Err(e) => return SuccessTuple::fail(e.to_string()),
            };
            apply_patch_maps(&original, pipe.raw_parameters())
        };
        let table = self.instance_table_name("mrsm_pipes");
        let f = self.flavor;
        let query = format!(
            "UPDATE {table} SET parameters={} WHERE {table}.pipe_id = {}",
            placeholder(f, 1),
            placeholder(f, 2)
        );
        let params = vec![self.json_param(&parameters), SqlParam::Int(id)];
        bump_metadata_epoch(match self.exec_params(&query, &params).await {
            Some(_) => SuccessTuple::ok(format!("Successfully edited {pipe}.")),
            None => SuccessTuple::fail(format!("Failed to edit {pipe}.")),
        })
    }

    /// `delete_pipe(pipe)`: delete the pipe's registration (the table is dropped by
    /// `Pipe.delete()` beforehand).
    pub async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let id = match self.get_pipe_id(pipe).await {
            Ok(Some(id)) if id != 0 => id,
            Ok(_) => return SuccessTuple::fail(format!("{pipe} is not registered.")),
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let table = self.instance_table_name("mrsm_pipes");
        let query = format!(
            "DELETE FROM {table} WHERE {table}.pipe_id = {}",
            placeholder(self.flavor, 1)
        );
        bump_metadata_epoch(match self.exec_params(&query, &[SqlParam::Int(id)]).await {
            Some(_) => SuccessTuple::ok("Success"),
            None => SuccessTuple::fail(format!("Failed to delete registration for {pipe}.")),
        })
    }

    /// `fetch_pipes_keys(connector_keys, metric_keys, location_keys, tags, params)`: a mapping
    /// of pipe IDs to `(connector_keys, metric_key, location_key, parameters)`.
    ///
    /// `_` negates values; tags are `,`-ANDed within a group and ORed across groups.
    pub async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        let f = self.flavor;
        let table = self.instance_table_name("mrsm_pipes");
        if !self
            .table_exists("mrsm_pipes", self.instance_schema())
            .await
        {
            return Ok(PipesKeys::Dict(IndexMap::new()));
        }
        let location_keys: Vec<String> = query
            .location_keys
            .iter()
            .map(|lk| match lk.as_deref() {
                None | Some("[None]") | Some("None") | Some("null") => "None".to_string(),
                Some(other) => other.to_string(),
            })
            .collect();
        let cols: [(&str, Vec<String>); 3] = [
            ("connector_keys", query.connector_keys.clone()),
            ("metric_key", query.metric_keys.clone()),
            ("location_key", location_keys),
        ];
        let mut parameters: IndexMap<String, Value> = query
            .params
            .clone()
            .unwrap_or_default()
            .into_iter()
            .collect();
        for (col, vals) in &cols {
            if !(vals.is_empty() || (vals.len() == 1 && vals[0] == "*")) {
                parameters.insert(
                    col.to_string(),
                    Value::List(vals.iter().cloned().map(Value::Str).collect()),
                );
            }
        }
        let table_cols = [
            "pipe_id",
            "connector_keys",
            "metric_key",
            "location_key",
            "parameters",
        ];
        let mut binds: Vec<SqlParam> = Vec::new();
        let next = |binds: &mut Vec<SqlParam>, p: SqlParam| -> String {
            binds.push(p);
            placeholder(f, binds.len())
        };
        let mut wheres: Vec<String> = Vec::new();
        let prefix = negation_prefix();
        for (key, val) in &parameters {
            if !table_cols.contains(&key.as_str()) || matches!(val, Value::List(_)) {
                continue;
            }
            let val = match val {
                Value::Map(_) => Value::Str(val.json_dumps()),
                other => other.clone(),
            };
            if val.py_str().starts_with(prefix) {
                // PARITY: the negated parameter compares the column with the *key name*
                // (`pipes_tbl.c[key] != key`, `_pipes.py:256`).
                let p = next(&mut binds, SqlParam::Text(key.clone()));
                wheres.push(format!("{table}.{key} != {p}"));
            } else {
                let none = next(&mut binds, SqlParam::Text("None".into()));
                let p = next(
                    &mut binds,
                    match &val {
                        Value::Int(i) => SqlParam::Int(*i),
                        Value::Bool(b) => SqlParam::Bool(*b),
                        Value::Float(x) => SqlParam::Float(*x),
                        Value::Null => SqlParam::Null,
                        other => SqlParam::Text(other.py_str()),
                    },
                );
                wheres.push(format!("coalesce({table}.{key}, {none}) = {p}"));
            }
        }
        for (col, vals) in &cols {
            if vals.is_empty() {
                continue;
            }
            let (inc, exc) = separate_negation_values(vals);
            for (vals, op) in [(inc, "IN"), (exc, "NOT IN")] {
                if vals.is_empty() {
                    continue;
                }
                let none = next(&mut binds, SqlParam::Text("None".into()));
                let ps: Vec<String> = vals
                    .into_iter()
                    .map(|v| next(&mut binds, SqlParam::Text(v)))
                    .collect();
                wheres.push(format!(
                    "coalesce({table}.{col}, {none}) {op} ({})",
                    ps.join(", ")
                ));
            }
        }

        // Finally, parse tags.
        let mut ors: Vec<String> = Vec::new();
        let mut nands: Vec<String> = Vec::new();
        for tag in &query.tags {
            let group: Vec<String> = tag.split(',').map(str::to_string).collect();
            let (inc, exc) = separate_negation_values(&group);
            if f.is_json_flavor() {
                let tags_jsonb =
                    format!("CAST(CAST({table}.parameters AS JSONB) -> 'tags' AS JSONB)");
                if !inc.is_empty() {
                    let p = next(
                        &mut binds,
                        SqlParam::Json(serde_json::Value::Array(
                            inc.iter().cloned().map(serde_json::Value::String).collect(),
                        )),
                    );
                    ors.push(format!("({tags_jsonb} @> {p})"));
                }
                for xt in exc {
                    let p = next(
                        &mut binds,
                        SqlParam::Json(serde_json::Value::Array(vec![serde_json::Value::String(
                            xt,
                        )])),
                    );
                    nands.push(format!("NOT ({tags_jsonb} @> {p})"));
                }
            } else {
                let cast = match f {
                    Flavor::Mysql | Flavor::Mariadb => {
                        format!("CAST({table}.parameters AS CHAR)")
                    }
                    // Oracle has no unsized `VARCHAR` (`ORA-00906`), and `LIKE` reads the `CLOB` directly.
                    Flavor::Oracle => format!("{table}.parameters"),
                    // PARITY: SQLAlchemy renders `cast(col, String)` as `CAST(col AS VARCHAR(max))` on
                    // MSSQL (`MSTypeCompiler.visit_VARCHAR`); a bare `VARCHAR` is `VARCHAR(30)` there
                    // and truncates the parameters before the tags.
                    Flavor::Mssql => format!("CAST({table}.parameters AS VARCHAR(max))"),
                    _ => format!("CAST({table}.parameters AS VARCHAR)"),
                };
                let sub_ands: Vec<String> = inc
                    .iter()
                    .map(|nt| {
                        let p = next(&mut binds, SqlParam::Text(format!("%\"tags\":%\"{nt}\"%")));
                        format!("{cast} LIKE {p}")
                    })
                    .collect();
                if !sub_ands.is_empty() {
                    ors.push(format!("({})", sub_ands.join(" AND ")));
                }
                for xt in exc {
                    let p = next(&mut binds, SqlParam::Text(format!("%\"tags\":%\"{xt}\"%")));
                    nands.push(format!("{cast} NOT LIKE {p}"));
                }
            }
        }
        if !nands.is_empty() {
            wheres.push(format!("({})", nands.join(" AND ")));
        }
        if !ors.is_empty() {
            wheres.push(format!("({})", ors.join(" OR ")));
        }
        let true_lit = match f {
            // SQLAlchemy renders `true` as `1 = 1` without native booleans.
            Flavor::Mysql
            | Flavor::Mariadb
            | Flavor::Sqlite
            | Flavor::Geopackage
            | Flavor::Mssql
            | Flavor::Oracle => "1 = 1",
            _ => "true",
        };
        let mut where_str = true_lit.to_string();
        for w in &wheres {
            where_str.push_str(" AND ");
            where_str.push_str(w);
        }
        let loc_order = if f.omit_nullsfirst() {
            format!("{table}.location_key ASC")
        } else {
            format!("{table}.location_key ASC NULLS FIRST")
        };
        let sql = format!(
            "SELECT {table}.pipe_id, {table}.connector_keys, {table}.metric_key, {table}.location_key, {table}.parameters \nFROM {table} \nWHERE {where_str} ORDER BY {table}.connector_keys ASC, {table}.metric_key ASC, {loc_order}"
        );
        let df = self.read_params(&sql, &binds).await.map_err(be)?;
        let mut out: IndexMap<PipeId, PipeKeysEntry> = IndexMap::new();
        for doc in frame_docs(&df) {
            let Some(id) = kv_int(doc.get("pipe_id")) else {
                continue;
            };
            out.insert(
                PipeId::Int(id),
                PipeKeysEntry {
                    connector_keys: kv_str(doc.get("connector_keys")).unwrap_or_default(),
                    metric_key: kv_str(doc.get("metric_key")).unwrap_or_default(),
                    location_key: kv_str(doc.get("location_key")),
                    extra: Some(Value::Map(parse_parameters(doc.get("parameters")))),
                },
            );
        }
        Ok(PipesKeys::Dict(out))
    }
}
