//! An in-memory `InstanceConnector` for the server tests.
//!
//! It keeps pipes, rows, users, plugins and tokens in memory and answers with the same messages
//! as the v4 `SQLConnector` on sqlite (`connectors/sql/_pipes.py`, `_users.py`, `_plugins.py`,
//! and the generic `instance/_tokens.py`), so the contract replay can compare the server's
//! responses with the recorded Python server's. It reports itself as `sql:fixture`.

#![allow(dead_code)]

use std::collections::HashMap;
use std::sync::Mutex;

use chrono::Utc;
use indexmap::IndexMap;
use meerschaum_connector::pipe_ops::{self, Doc, compare_values, frame_to_docs};
use meerschaum_connector::{
    Connector, ConnectorError, ConnectorResult, DataQuery, DateBound, InstanceConnector, PipeId,
    PipeKeysEntry, PipesKeys, PipesKeysQuery, PluginRecord, SyncOptions, TokenModel, async_trait,
};
use meerschaum_core::users::{Token, User, valid_username};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{PipeInput, pipe_enforce_dtypes};
use meerschaum_kernel::value::Value as KValue;
use uuid::Uuid;

/// PBKDF2 rounds for hashes the test double creates (fast; verification reads the rounds).
pub const TEST_ROUNDS: u32 = 1000;

#[derive(Debug, Clone)]
struct UserRow {
    id: i64,
    username: String,
    password_hash: String,
    email: Option<String>,
    user_type: Option<String>,
    attributes: Map,
}

#[derive(Debug, Clone)]
struct PluginRow {
    name: String,
    version: Option<String>,
    user_id: Option<Value>,
    attributes: Map,
}

#[derive(Debug, Default)]
struct Data {
    pipes: IndexMap<(String, String, Option<String>), (i64, Map)>,
    next_pipe_id: i64,
    tables: HashMap<String, Vec<Doc>>,
    users: Vec<UserRow>,
    next_user_id: i64,
    plugins: Vec<PluginRow>,
    tokens: Vec<TokenModel>,
}

/// The in-memory instance.
#[derive(Debug)]
pub struct MemoryInstance {
    label: String,
    data: Mutex<Data>,
}

fn keys_of(pipe: &Pipe) -> (String, String, Option<String>) {
    (
        pipe.connector_keys().to_string(),
        pipe.metric_key().to_string(),
        pipe.location_key().map(String::from),
    )
}

fn target_of(pipe: &Pipe) -> String {
    pipe.target(None).unwrap_or_else(|_| pipe.target_legacy())
}

fn index_columns(pipe: &Pipe) -> Vec<String> {
    let mut cols = Vec::new();
    for v in pipe.columns().values() {
        let c = v.py_str();
        if !v.is_null() && !cols.contains(&c) {
            cols.push(c);
        }
    }
    cols
}

fn same(a: &KValue, b: &KValue) -> bool {
    match (a.is_na(), b.is_na()) {
        (true, true) => true,
        (false, false) => compare_values(a, b) == std::cmp::Ordering::Equal,
        _ => false,
    }
}

fn param_matches(cell: &KValue, param: &Value) -> bool {
    let items: Vec<&Value> = match param {
        Value::List(l) => l.iter().collect(),
        other => vec![other],
    };
    let mut includes = Vec::new();
    let mut excludes = Vec::new();
    for v in items {
        match v {
            Value::Str(s) if s.starts_with('_') => excludes.push(Value::Str(s[1..].to_string())),
            other => includes.push(other.clone()),
        }
    }
    let eq = |v: &Value| match v {
        Value::Null => cell.is_na(),
        Value::Str(s) if s == "None" => cell.is_na(),
        other => !cell.is_na() && same(cell, &KValue::from(other)),
    };
    (includes.is_empty() || includes.iter().any(eq)) && !excludes.iter().any(eq)
}

fn bound_cmp(cell: &KValue, bound: &DateBound) -> Option<std::cmp::Ordering> {
    if cell.is_na() {
        return None;
    }
    Some(compare_values(cell, &bound.to_value()))
}

fn row_matches(doc: &Doc, query: &DataQuery, dt_col: Option<&str>) -> bool {
    if let Some(dt) = dt_col {
        let cell = doc.get(dt).cloned().unwrap_or(KValue::Null);
        if let Some(b) = &query.begin {
            if bound_cmp(&cell, b) != Some(std::cmp::Ordering::Greater)
                && bound_cmp(&cell, b) != Some(std::cmp::Ordering::Equal)
            {
                return false;
            }
        }
        if let Some(e) = &query.end {
            if bound_cmp(&cell, e) != Some(std::cmp::Ordering::Less) {
                return false;
            }
        }
    }
    if let Some(params) = &query.params {
        for (col, p) in params {
            let cell = doc.get(col).cloned().unwrap_or(KValue::Null);
            if !param_matches(&cell, p) {
                return false;
            }
        }
    }
    true
}

fn infer_type(values: &[&KValue]) -> String {
    let Some(v) = values.iter().find(|v| !v.is_na()) else {
        return "object".into();
    };
    match v {
        KValue::Bool(_) => "bool".into(),
        KValue::Int(_) => "int64".into(),
        KValue::Float(_) => "float64".into(),
        KValue::Str(_) => "string".into(),
        KValue::Datetime(d) if d.tz.is_some() => "datetime64[ns, UTC]".into(),
        KValue::Datetime(_) => "datetime64[ns]".into(),
        KValue::Json(_) => "json".into(),
        KValue::Uuid(_) => "uuid".into(),
        KValue::Decimal(_) => "numeric".into(),
        _ => "object".into(),
    }
}

impl MemoryInstance {
    /// A new instance labeled `label` (`sql:{label}`).
    pub fn new(label: &str) -> MemoryInstance {
        MemoryInstance {
            label: label.to_string(),
            data: Mutex::new(Data {
                next_pipe_id: 1,
                next_user_id: 1,
                ..Default::default()
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Data> {
        self.data.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Seed a user directly (like `instance.register_user(User(...))` in the generator).
    pub fn seed_user(&self, username: &str, password: &str, user_type: Option<&str>) -> i64 {
        let mut d = self.lock();
        let id = d.next_user_id;
        d.next_user_id += 1;
        d.users.push(UserRow {
            id,
            username: username.to_string(),
            password_hash: meerschaum_core::users::hash_password(password, None, Some(TEST_ROUNDS))
                .expect("hash"),
            email: None,
            user_type: user_type.map(String::from),
            attributes: Map::new(),
        });
        id
    }

    fn user_row(&self, username: &str) -> Option<UserRow> {
        self.lock()
            .users
            .iter()
            .find(|u| u.username == username)
            .cloned()
    }

    fn rows(&self, pipe: &Pipe) -> Option<Vec<Doc>> {
        self.lock().tables.get(&target_of(pipe)).cloned()
    }

    fn column_types(rows: &[Doc]) -> IndexMap<String, String> {
        let mut cols: Vec<String> = Vec::new();
        for r in rows {
            for k in r.keys() {
                if !cols.contains(k) {
                    cols.push(k.clone());
                }
            }
        }
        cols.into_iter()
            .map(|c| {
                let vals: Vec<&KValue> = rows.iter().filter_map(|r| r.get(&c)).collect();
                let t = infer_type(&vals);
                (c, t)
            })
            .collect()
    }

    async fn frame_of(&self, pipe: &Pipe, rows: Vec<Doc>) -> ConnectorResult<Frame> {
        let ctx = pipe_ops::dtypes_context(self, pipe, pipe.parameters()).await?;
        let mut cols: Vec<String> = Vec::new();
        for r in &rows {
            for k in r.keys() {
                if !cols.contains(k) {
                    cols.push(k.clone());
                }
            }
        }
        let rows: Vec<Doc> = rows
            .into_iter()
            .map(|r| {
                cols.iter()
                    .map(|c| (c.clone(), r.get(c).cloned().unwrap_or(KValue::Null)))
                    .collect()
            })
            .collect();
        pipe_enforce_dtypes(PipeInput::Docs(rows), &ctx)
            .map_err(|e| ConnectorError::Backend(e.to_string()))
    }
}

#[async_trait]
impl Connector for MemoryInstance {
    fn connector_type(&self) -> &str {
        "sql"
    }

    fn label(&self) -> &str {
        &self.label
    }

    fn is_thread_safe(&self) -> bool {
        true
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }
}

#[async_trait]
impl InstanceConnector for MemoryInstance {
    async fn register_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let mut d = self.lock();
        let k = keys_of(pipe);
        if d.pipes.contains_key(&k) {
            return SuccessTuple::fail(format!("{pipe} is already registered."));
        }
        let id = d.next_pipe_id;
        d.next_pipe_id += 1;
        d.pipes.insert(k, (id, pipe.parameters().clone()));
        SuccessTuple::ok(format!("Successfully registered {pipe}."))
    }

    async fn get_pipe_attributes(&self, pipe: &Pipe) -> ConnectorResult<Map> {
        let d = self.lock();
        let Some((id, params)) = d.pipes.get(&keys_of(pipe)) else {
            return Ok(Map::new());
        };
        let mut m = Map::new();
        m.insert("pipe_id".into(), Value::Int(*id));
        m.insert("connector_keys".into(), pipe.connector_keys().into());
        m.insert("metric_key".into(), pipe.metric_key().into());
        m.insert(
            "location_key".into(),
            pipe.location_key().map(String::from).into(),
        );
        m.insert("parameters".into(), Value::Map(params.clone()));
        Ok(m)
    }

    async fn get_pipe_id(&self, pipe: &Pipe) -> ConnectorResult<Option<PipeId>> {
        Ok(self
            .lock()
            .pipes
            .get(&keys_of(pipe))
            .map(|(id, _)| PipeId::Int(*id)))
    }

    async fn edit_pipe(&self, pipe: &Pipe, patch: bool) -> SuccessTuple {
        let mut d = self.lock();
        let Some((_, params)) = d.pipes.get_mut(&keys_of(pipe)) else {
            return SuccessTuple::fail(format!("{pipe} is not registered."));
        };
        *params = if patch {
            meerschaum_core::value::apply_patch_maps(params, pipe.parameters())
        } else {
            pipe.parameters().clone()
        };
        SuccessTuple::ok(format!("Successfully edited {pipe}."))
    }

    async fn delete_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let mut d = self.lock();
        d.tables.remove(&target_of(pipe));
        d.pipes.shift_remove(&keys_of(pipe));
        SuccessTuple::success()
    }

    async fn fetch_pipes_keys(&self, query: &PipesKeysQuery) -> ConnectorResult<PipesKeys> {
        let d = self.lock();
        let mut entries: Vec<(&(String, String, Option<String>), &(i64, Map))> =
            d.pipes.iter().collect();
        entries.sort_by(|a, b| a.0.cmp(b.0));
        let matches = |filters: &[String], v: &str| {
            let (neg, pos): (Vec<&String>, Vec<&String>) =
                filters.iter().partition(|f| f.starts_with('_'));
            (pos.is_empty() || pos.iter().any(|f| *f == v)) && !neg.iter().any(|f| &f[1..] == v)
        };
        let mut out = IndexMap::new();
        for ((ck, mk, lk), (id, params)) in entries {
            if !matches(&query.connector_keys, ck) || !matches(&query.metric_keys, mk) {
                continue;
            }
            if !query.location_keys.is_empty() && !query.location_keys.contains(lk) {
                continue;
            }
            if !query.tags.is_empty() {
                let tags: Vec<String> = match params.get("tags") {
                    Some(Value::List(l)) => l.iter().map(Value::py_str).collect(),
                    _ => Vec::new(),
                };
                let any = query
                    .tags
                    .iter()
                    .any(|group| group.split(',').all(|t| tags.iter().any(|x| x == t.trim())));
                if !any {
                    continue;
                }
            }
            if let Some(p) = &query.params {
                let field = |k: &str| -> Value {
                    match k {
                        "connector_keys" => Value::Str(ck.clone()),
                        "metric_key" => Value::Str(mk.clone()),
                        "location_key" => lk.clone().into(),
                        _ => params.get(k).cloned().unwrap_or(Value::Null),
                    }
                };
                if !p.iter().all(|(k, v)| &field(k) == v) {
                    continue;
                }
            }
            out.insert(
                PipeId::Int(*id),
                PipeKeysEntry {
                    connector_keys: ck.clone(),
                    metric_key: mk.clone(),
                    location_key: lk.clone(),
                    // The sqlite instance returns the parameters as a JSON string.
                    extra: Some(Value::Str(Value::Map(params.clone()).json_dumps())),
                },
            );
        }
        Ok(PipesKeys::Dict(out))
    }

    async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        Ok(self.lock().tables.contains_key(&target_of(pipe)))
    }

    async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        self.lock().tables.remove(&target_of(pipe));
        SuccessTuple::success()
    }

    async fn sync_pipe(&self, pipe: &Pipe, df: Frame, opts: &SyncOptions) -> SuccessTuple {
        let docs = frame_to_docs(&df);
        let index = index_columns(pipe);
        let mut d = self.lock();
        let table = d.tables.entry(target_of(pipe)).or_default();
        let (mut inserted, mut updated) = (0usize, 0usize);
        for doc in docs {
            let existing = if opts.check_existing && !index.is_empty() {
                table.iter().position(|row| {
                    index.iter().all(|c| {
                        same(
                            row.get(c).unwrap_or(&KValue::Null),
                            doc.get(c).unwrap_or(&KValue::Null),
                        )
                    })
                })
            } else {
                None
            };
            match existing {
                Some(i) => {
                    let row = &mut table[i];
                    let changed = doc
                        .iter()
                        .any(|(k, v)| !same(row.get(k).unwrap_or(&KValue::Null), v));
                    if changed {
                        for (k, v) in doc {
                            row.insert(k, v);
                        }
                        updated += 1;
                    }
                }
                None => {
                    table.push(doc);
                    inserted += 1;
                }
            }
        }
        SuccessTuple::ok(format!("Inserted {inserted}, updated {updated} rows."))
    }

    async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        let dt = pipe.column("datetime");
        let mut d = self.lock();
        if let Some(rows) = d.tables.get_mut(&target_of(pipe)) {
            rows.retain(|r| !row_matches(r, query, dt.as_deref()));
        }
        SuccessTuple::success()
    }

    async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        let Some(rows) = self.rows(pipe) else {
            return Ok(None);
        };
        let dt = pipe.column("datetime");
        let rows: Vec<Doc> = rows
            .into_iter()
            .filter(|r| row_matches(r, query, dt.as_deref()))
            .collect();
        let mut df = self.frame_of(pipe, rows).await?;
        if let Some(select) = query.select_columns.as_ref().filter(|s| !s.is_empty()) {
            let keep: Vec<String> = select.iter().filter(|c| df.contains(c)).cloned().collect();
            df = df
                .select(&keep)
                .map_err(|e| ConnectorError::Backend(e.to_string()))?;
        }
        if let Some(omit) = &query.omit_columns {
            for c in omit {
                df.drop(c);
            }
        }
        Ok(Some(pipe_ops::pipe_postprocess(pipe, df, query)))
    }

    async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        let Some(dt) = pipe.column("datetime") else {
            return Ok(None);
        };
        let Some(rows) = self.rows(pipe) else {
            return Ok(None);
        };
        let q = DataQuery {
            params: params.cloned(),
            ..Default::default()
        };
        let mut best: Option<KValue> = None;
        for r in rows.iter().filter(|r| row_matches(r, &q, Some(&dt))) {
            let Some(v) = r.get(&dt).filter(|v| !v.is_na()) else {
                continue;
            };
            let better = match &best {
                None => true,
                Some(b) => {
                    let o = compare_values(v, b);
                    if newest {
                        o == std::cmp::Ordering::Greater
                    } else {
                        o == std::cmp::Ordering::Less
                    }
                }
            };
            if better {
                best = Some(v.clone());
            }
        }
        Ok(best.as_ref().and_then(DateBound::from_value))
    }

    async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        let dt = pipe.column("datetime");
        Ok(Some(
            self.rows(pipe)
                .unwrap_or_default()
                .iter()
                .filter(|r| row_matches(r, query, dt.as_deref()))
                .count() as i64,
        ))
    }

    async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        Ok(Self::column_types(&self.rows(pipe).unwrap_or_default()))
    }

    async fn get_pipe_size(&self, pipe: &Pipe) -> ConnectorResult<Option<i64>> {
        Ok(self.rows(pipe).map(|_| 4096))
    }

    async fn compress_pipe(&self, _pipe: &Pipe) -> SuccessTuple {
        SuccessTuple::fail("Compression is not supported for flavor 'sqlite'.")
    }

    async fn decompress_pipe(&self, _pipe: &Pipe, _no_policy: bool) -> SuccessTuple {
        SuccessTuple::fail("Decompression is not supported for flavor 'sqlite'.")
    }

    async fn vacuum_pipe(&self, _pipe: &Pipe, _full: bool) -> SuccessTuple {
        SuccessTuple::ok("Size unchanged (4.1 kB).")
    }

    async fn analyze_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        SuccessTuple::ok(format!("Analyzed {pipe}."))
    }

    async fn partition_pipe(&self, _pipe: &Pipe, _chunk_minutes: Option<i64>) -> SuccessTuple {
        SuccessTuple::fail("Repartitioning is not supported for flavor 'sqlite'.")
    }

    // --- Users (`connectors/sql/_users.py`) -----------------------------------------------

    async fn register_user(&self, user: &mut User) -> SuccessTuple {
        let valid = valid_username(&user.username);
        if !valid.success {
            return valid;
        }
        if self.user_row(&user.username).is_some() {
            return SuccessTuple::fail(format!("User '{}' already exists.", user.username));
        }
        let hash = match &user.password_hash {
            Some(h) => h.clone(),
            None => meerschaum_core::users::hash_password(&user.password, None, Some(TEST_ROUNDS))
                .expect("hash"),
        };
        let mut d = self.lock();
        let id = d.next_user_id;
        d.next_user_id += 1;
        d.users.push(UserRow {
            id,
            username: user.username.clone(),
            password_hash: hash,
            email: user.email.clone(),
            user_type: user.user_type.clone(),
            attributes: user.attributes.clone().unwrap_or_default(),
        });
        user.user_id = Some(Value::Int(id));
        SuccessTuple::ok(format!("Successfully registered user '{}'.", user.username))
    }

    async fn get_user_id(&self, user: &User) -> ConnectorResult<Option<Value>> {
        Ok(self.user_row(&user.username).map(|u| Value::Int(u.id)))
    }

    async fn get_username(&self, user_id: &Value) -> ConnectorResult<Option<Value>> {
        Ok(self
            .lock()
            .users
            .iter()
            .find(|u| Some(u.id) == user_id.as_i64())
            .map(|u| Value::Str(u.username.clone())))
    }

    async fn get_users(&self) -> ConnectorResult<Vec<String>> {
        Ok(self
            .lock()
            .users
            .iter()
            .map(|u| u.username.clone())
            .collect())
    }

    async fn edit_user(&self, user: &User) -> SuccessTuple {
        let Some(row) = self.user_row(&user.username) else {
            return SuccessTuple::fail(format!(
                "User '{0}' does not exist. Register user '{0}' before editing.",
                user.username
            ));
        };
        let mut d = self.lock();
        let Some(r) = d.users.iter_mut().find(|u| u.id == row.id) else {
            return SuccessTuple::fail("missing");
        };
        if !user.password.is_empty() {
            r.password_hash =
                meerschaum_core::users::hash_password(&user.password, None, Some(TEST_ROUNDS))
                    .expect("hash");
        }
        // PARITY (sql/_users.py:159): `None != ''`, so unset fields are written as NULL.
        if user.email.as_deref() != Some("") {
            r.email = user.email.clone();
        }
        if let Some(a) = user.attributes.as_ref().filter(|a| !a.is_empty()) {
            r.attributes = a.clone();
        }
        if user.user_type.as_deref() != Some("") {
            r.user_type = user.user_type.clone();
        }
        SuccessTuple::ok(format!("Successfully edited user '{}'.", user.username))
    }

    async fn delete_user(&self, user: &User) -> SuccessTuple {
        let Some(row) = self.user_row(&user.username) else {
            return SuccessTuple::fail(format!(
                "User '{}' is not registered and cannot be deleted.",
                user.username
            ));
        };
        let mut d = self.lock();
        d.users.retain(|u| u.id != row.id);
        d.plugins
            .retain(|p| p.user_id.as_ref().and_then(Value::as_i64) != Some(row.id));
        SuccessTuple::ok(format!("Successfully deleted user '{}'", user.username))
    }

    async fn get_user_password_hash(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self.user_row(&user.username).map(|u| u.password_hash))
    }

    async fn get_user_type(&self, user: &User) -> ConnectorResult<Option<String>> {
        Ok(self.user_row(&user.username).and_then(|u| u.user_type))
    }

    async fn get_user_attributes(&self, user: &User) -> ConnectorResult<Option<Map>> {
        Ok(self.user_row(&user.username).map(|u| u.attributes))
    }

    // --- Plugins (`connectors/sql/_plugins.py`) -------------------------------------------

    async fn register_plugin(&self, plugin: &PluginRecord, force: bool) -> SuccessTuple {
        let mut d = self.lock();
        let row = PluginRow {
            name: plugin.name.clone(),
            version: plugin.version.clone(),
            user_id: plugin.user_id.clone(),
            attributes: plugin.attributes.clone(),
        };
        match d.plugins.iter_mut().find(|p| p.name == plugin.name) {
            Some(existing) => {
                let old = existing.version.clone().unwrap_or_default();
                let new = plugin.version.clone().unwrap_or_default();
                if !force && !old.is_empty() && !new.is_empty() && old >= new {
                    return SuccessTuple::fail(format!(
                        "Version '{new}' of plugin '{}' must be greater than existing version '{old}'.",
                        plugin.name
                    ));
                }
                *existing = row;
            }
            None => d.plugins.push(row),
        }
        SuccessTuple::ok(format!("Successfully registered plugin '{}'.", plugin.name))
    }

    async fn get_plugin_id(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        Ok(self
            .lock()
            .plugins
            .iter()
            .find(|p| p.name == plugin.name)
            .map(|p| p.name.clone()))
    }

    async fn get_plugin_version(&self, plugin: &PluginRecord) -> ConnectorResult<Option<String>> {
        Ok(self
            .lock()
            .plugins
            .iter()
            .find(|p| p.name == plugin.name)
            .and_then(|p| p.version.clone()))
    }

    async fn get_plugin_user_id(&self, plugin: &PluginRecord) -> ConnectorResult<Option<Value>> {
        Ok(self
            .lock()
            .plugins
            .iter()
            .find(|p| p.name == plugin.name)
            .and_then(|p| p.user_id.clone()))
    }

    async fn get_plugin_attributes(&self, plugin: &PluginRecord) -> ConnectorResult<Map> {
        Ok(self
            .lock()
            .plugins
            .iter()
            .find(|p| p.name == plugin.name)
            .map(|p| p.attributes.clone())
            .unwrap_or_default())
    }

    async fn get_plugins(
        &self,
        user_id: Option<&Value>,
        search_term: Option<&str>,
    ) -> ConnectorResult<Vec<String>> {
        Ok(self
            .lock()
            .plugins
            .iter()
            .filter(|p| user_id.is_none_or(|u| p.user_id.as_ref() == Some(u)))
            .filter(|p| search_term.is_none_or(|s| p.name.starts_with(s)))
            .map(|p| p.name.clone())
            .collect())
    }

    async fn delete_plugin(&self, plugin: &PluginRecord) -> SuccessTuple {
        let mut d = self.lock();
        if !d.plugins.iter().any(|p| p.name == plugin.name) {
            return SuccessTuple::ok(format!("Plugin '{}' was not registered.", plugin.name));
        }
        d.plugins.retain(|p| p.name != plugin.name);
        SuccessTuple::ok(format!("Successfully deleted plugin '{}'.", plugin.name))
    }

    // --- Tokens (`connectors/instance/_tokens.py`) ----------------------------------------

    async fn register_token(&self, token: &mut Token) -> SuccessTuple {
        let user_id = match (&token.user_id, &token.user) {
            (Some(id), _) if !id.is_null() => Some(id.clone()),
            (_, Some(u)) => self.user_row(&u.username).map(|r| Value::Int(r.id)),
            _ => None,
        };
        if user_id.is_none() {
            return SuccessTuple::fail("Cannot register a token without a user.");
        }
        let (id, secret) = match token.generate_credentials() {
            Ok(c) => c,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let hash =
            meerschaum_core::users::hash_password(&secret, None, Some(TEST_ROUNDS)).expect("hash");
        token.id = Some(id);
        token.creation = Some(Utc::now());
        token.secret_hash = Some(hash.clone());
        self.lock().tokens.push(TokenModel {
            id: Some(id),
            creation: token.creation,
            expiration: token.expiration,
            label: token.label.clone(),
            user_id,
            secret_hash: Some(hash),
            scopes: token.scopes.clone(),
            is_valid: token.is_valid,
        });
        SuccessTuple::success()
    }

    async fn edit_token(&self, token: &Token) -> SuccessTuple {
        let Some(id) = token.id else {
            return SuccessTuple::fail("Token ID is not set.");
        };
        let mut d = self.lock();
        let Some(t) = d.tokens.iter_mut().find(|t| t.id == Some(id)) else {
            return SuccessTuple::fail(format!("Token {id} does not exist."));
        };
        t.label = token.label.clone();
        t.expiration = token.expiration;
        t.scopes = token.scopes.clone();
        t.is_valid = token.is_valid;
        SuccessTuple::success()
    }

    async fn invalidate_token(&self, token: &Token) -> SuccessTuple {
        let Some(id) = token.id else {
            return SuccessTuple::fail("Token ID is not set.");
        };
        let mut d = self.lock();
        let Some(t) = d.tokens.iter_mut().find(|t| t.id == Some(id)) else {
            return SuccessTuple::fail(format!("Token {id} does not exist."));
        };
        t.is_valid = false;
        SuccessTuple::success()
    }

    async fn delete_token(&self, token: &Token) -> SuccessTuple {
        let Some(id) = token.id else {
            return SuccessTuple::fail("Token ID is not set.");
        };
        let mut d = self.lock();
        if !d.tokens.iter().any(|t| t.id == Some(id)) {
            return SuccessTuple::fail(format!("Token {id} does not exist."));
        }
        d.tokens.retain(|t| t.id != Some(id));
        SuccessTuple::success()
    }

    async fn get_tokens(
        &self,
        user: Option<&User>,
        labels: Option<&[String]>,
        ids: Option<&[Uuid]>,
    ) -> ConnectorResult<Vec<TokenModel>> {
        let user_id = user
            .and_then(|u| self.user_row(&u.username))
            .map(|r| Value::Int(r.id));
        let mut out: Vec<TokenModel> = self
            .lock()
            .tokens
            .iter()
            .filter(|t| user.is_none() || t.user_id == user_id)
            .filter(|t| labels.is_none_or(|l| t.label.as_ref().is_some_and(|x| l.contains(x))))
            .filter(|t| ids.is_none_or(|i| t.id.is_some_and(|x| i.contains(&x))))
            .cloned()
            .collect();
        out.sort_by_key(|t| std::cmp::Reverse(t.creation));
        Ok(out)
    }

    async fn get_token_model(&self, token_id: &Uuid) -> ConnectorResult<Option<TokenModel>> {
        Ok(self
            .lock()
            .tokens
            .iter()
            .find(|t| t.id == Some(*token_id))
            .cloned())
    }

    async fn get_token_secret_hash(&self, token_id: &Uuid) -> ConnectorResult<Option<String>> {
        Ok(self
            .get_token_model(token_id)
            .await?
            .and_then(|t| t.secret_hash))
    }

    async fn get_token_user_id(&self, token_id: &Uuid) -> ConnectorResult<Option<Value>> {
        Ok(self
            .get_token_model(token_id)
            .await?
            .and_then(|t| t.user_id))
    }

    async fn get_token_scopes(&self, token_id: &Uuid) -> ConnectorResult<Vec<String>> {
        Ok(self
            .get_token_model(token_id)
            .await?
            .map(|t| t.scopes)
            .unwrap_or_default())
    }

    async fn token_exists(&self, token_id: &Uuid) -> ConnectorResult<bool> {
        Ok(self.get_token_model(token_id).await?.is_some())
    }
}
