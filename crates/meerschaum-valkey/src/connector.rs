//! `ValkeyConnector` (`meerschaum/connectors/valkey/_ValkeyConnector.py`): attributes and URI,
//! the `get` / `set` helpers, `push_df` / `push_docs`, `read` / `read_docs` and `drop_table`.

use std::sync::Arc;
use std::time::Duration;

use indexmap::IndexMap;
use meerschaum_api::pipe_ops::{self, Doc};
use meerschaum_api::traits::{ConnectorError, ConnectorResult};
use meerschaum_api::wire::{ToJsonOptions, quote_plus, to_json};
use meerschaum_core::{Map, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::query::{ParamValue, QueryOptions, query_df};
use meerschaum_kernel::value::Value as KValue;

use crate::backend::{BackendError, ScoreBound, ValkeyBackend};
use crate::fred_backend::FredBackend;
use crate::keys::{bound_score, get_datetime_column_key, push_score, quote_table};
use crate::memory::MemoryBackend;
use crate::pyjson::{dumps_doc, loads_doc};

/// A function generating new user IDs (`generate_password(12)` by default).
pub type IdFactory = Arc<dyn Fn() -> String + Send + Sync>;

/// A Valkey instance connector.
///
/// Build it from connection attributes ([`ValkeyConnector::connect`]), a URI
/// ([`ValkeyConnector::from_uri`]) or an explicit backend ([`ValkeyConnector::with_backend`]).
#[derive(Clone)]
pub struct ValkeyConnector {
    pub(crate) label: String,
    pub(crate) attributes: Map,
    pub(crate) backend: Arc<dyn ValkeyBackend>,
    pub(crate) id_factory: IdFactory,
}

impl std::fmt::Debug for ValkeyConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ValkeyConnector")
            .field("label", &self.label)
            .finish_non_exhaustive()
    }
}

impl std::fmt::Display for ValkeyConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "valkey:{}", self.label)
    }
}

/// Convert a backend error into a connector error.
pub(crate) fn be(e: BackendError) -> ConnectorError {
    ConnectorError::Backend(e.0)
}

fn attr_str(v: &Value) -> String {
    meerschaum_core::pyfmt::py_str(v)
}

fn default_id_factory() -> IdFactory {
    Arc::new(|| {
        meerschaum_core::users::generate_password(12)
            .unwrap_or_else(|_| uuid::Uuid::new_v4().simple().to_string()[..12].to_string())
    })
}

impl ValkeyConnector {
    /// `REQUIRED_ATTRIBUTES`.
    pub const REQUIRED_ATTRIBUTES: &'static [&'static str] = &["host"];
    /// `OPTIONAL_ATTRIBUTES`.
    pub const OPTIONAL_ATTRIBUTES: &'static [&'static str] =
        &["port", "username", "password", "db", "socket_timeout"];

    /// `DEFAULT_ATTRIBUTES`: `username='default'`, `port=6379`, `db=0`, `socket_timeout=300`.
    pub fn default_attributes() -> Map {
        let mut m = Map::new();
        m.insert("username".into(), Value::Str("default".into()));
        m.insert("port".into(), Value::Int(6379));
        m.insert("db".into(), Value::Int(0));
        m.insert("socket_timeout".into(), Value::Int(300));
        m
    }

    /// A connector over an explicit backend (no connection is made).
    pub fn with_backend(
        label: &str,
        attributes: Map,
        backend: Arc<dyn ValkeyBackend>,
    ) -> ValkeyConnector {
        ValkeyConnector {
            label: label.to_string(),
            attributes,
            backend,
            id_factory: default_id_factory(),
        }
    }

    /// A connector over a fresh in-memory backend.
    pub fn in_memory(label: &str) -> ValkeyConnector {
        Self::with_backend(label, Map::new(), Arc::new(MemoryBackend::new()))
    }

    /// Connect with the given attributes (`host` is required unless `uri` is given).
    pub async fn connect(label: &str, attributes: Map) -> ConnectorResult<ValkeyConnector> {
        if !attributes.contains_key("uri") && !attributes.contains_key("host") {
            return Err(ConnectorError::Invalid(format!(
                "Missing required attribute 'host' for connector 'valkey:{label}'."
            )));
        }
        let probe =
            ValkeyConnector::with_backend(label, attributes, Arc::new(MemoryBackend::new()));
        let timeout = probe
            .attributes
            .get("socket_timeout")
            .and_then(|v| v.as_i64().or_else(|| attr_str(v).parse().ok()))
            .map(|s: i64| Duration::from_secs(s.max(1) as u64));
        let backend = FredBackend::connect(&probe.connection_url(), timeout)
            .await
            .map_err(be)?;
        Ok(ValkeyConnector {
            backend: Arc::new(backend),
            ..probe
        })
    }

    /// Connect from a URI (`valkey://[user:password@]host[:port][/db][?timeout=Ns]`).
    pub async fn from_uri(label: &str, uri: &str) -> ConnectorResult<ValkeyConnector> {
        let mut attributes = Map::new();
        attributes.insert("uri".into(), Value::Str(uri.to_string()));
        Self::connect(label, attributes).await
    }

    /// The attributes for `valkey:{label}` from the config registry: the `default` section
    /// patched with the label's section (`Connector.__init__`).
    pub fn attributes_from_config(config: &meerschaum_core::Config, label: &str) -> Map {
        let base = config
            .get(&["meerschaum", "connectors", "valkey", "default"])
            .map(|v| v.to_map())
            .unwrap_or_default();
        let specific = config
            .get(&["meerschaum", "connectors", "valkey", label])
            .map(|v| v.to_map())
            .unwrap_or_default();
        meerschaum_core::value::apply_patch_maps(&base, &specific)
    }

    /// Replace the user ID generator (tests use deterministic IDs).
    pub fn set_id_factory(&mut self, factory: IdFactory) {
        self.id_factory = factory;
    }

    /// The label.
    pub fn label(&self) -> &str {
        &self.label
    }

    /// The configured attributes.
    pub fn attributes(&self) -> &Map {
        &self.attributes
    }

    /// The backend.
    pub fn backend(&self) -> &Arc<dyn ValkeyBackend> {
        &self.backend
    }

    /// The `URI` property: the given `uri`, else
    /// `valkey://[username:][password@]host[:port][/db][?timeout={socket_timeout}s]` built from
    /// the attributes that are set.
    pub fn uri(&self) -> String {
        if let Some(uri) = self.attributes.get("uri") {
            return attr_str(uri);
        }
        let mut uri = String::from("valkey://");
        if let Some(u) = self.attributes.get("username") {
            uri += &(quote_plus(&attr_str(u)) + ":");
        }
        if let Some(p) = self.attributes.get("password") {
            uri += &(quote_plus(&attr_str(p)) + "@");
        }
        if let Some(h) = self.attributes.get("host") {
            uri += &attr_str(h);
        }
        if let Some(p) = self.attributes.get("port") {
            uri += &format!(":{}", attr_str(p));
        }
        if let Some(db) = self.attributes.get("db") {
            uri += &format!("/{}", attr_str(db));
        }
        if let Some(t) = self.attributes.get("socket_timeout") {
            uri += &format!("?timeout={}s", attr_str(t));
        }
        uri
    }

    /// The URL handed to the client library (`redis://` scheme, query string dropped).
    pub fn connection_url(&self) -> String {
        if let Some(uri) = self.attributes.get("uri") {
            let uri = attr_str(uri);
            let uri = uri.split('?').next().unwrap_or(&uri).to_string();
            return uri;
        }
        let get = |k: &str| self.attributes.get(k).map(attr_str);
        let host = get("host").unwrap_or_else(|| "localhost".into());
        let port = get("port").unwrap_or_else(|| "6379".into());
        let db = get("db").unwrap_or_else(|| "0".into());
        let creds = match (get("username"), get("password")) {
            (u, Some(p)) => format!(
                "{}:{}@",
                quote_plus(&u.unwrap_or_else(|| "default".into())),
                quote_plus(&p)
            ),
            _ => String::new(),
        };
        format!("redis://{creds}{host}:{port}/{db}")
    }

    /// `set(key, value)`.
    pub async fn set(&self, key: &str, value: &str) -> ConnectorResult<()> {
        self.backend.set(key, value).await.map_err(be)
    }

    /// `set(key, value, ex=seconds)`.
    pub async fn set_ex(&self, key: &str, value: &str, seconds: i64) -> ConnectorResult<()> {
        self.backend.set_ex(key, value, seconds).await.map_err(be)
    }

    /// `get(key)`: the decoded value, or `None`.
    pub async fn get(&self, key: &str) -> ConnectorResult<Option<String>> {
        self.backend.get(key).await.map_err(be)
    }

    /// `test_connection()`: `PING`.
    pub async fn ping(&self) -> ConnectorResult<bool> {
        self.backend.ping().await.map_err(be)
    }

    /// `push_df(df, table, datetime_column)`: serialize with `to_json` (records, ISO dates,
    /// 15-digit doubles) and push the documents. Returns the number of new documents.
    pub async fn push_df(
        &self,
        df: &Frame,
        table: &str,
        datetime_column: Option<&str>,
    ) -> ConnectorResult<i64> {
        let text = to_json(df, None, &ToJsonOptions::default());
        let parsed: serde_json::Value = serde_json::from_str(&text)?;
        let docs: Vec<Doc> = match parsed {
            serde_json::Value::Array(items) => items
                .into_iter()
                .map(|item| match item {
                    serde_json::Value::Object(o) => o
                        .into_iter()
                        .map(|(k, v)| (k, crate::pyjson::json_scalar_to_kernel(v)))
                        .collect(),
                    _ => Doc::new(),
                })
                .collect(),
            _ => Vec::new(),
        };
        self.push_docs(&docs, table, datetime_column).await
    }

    /// `push_docs(docs, table, datetime_column)`: add documents to a sorted set scored by the
    /// datetime column (epoch seconds or ints), or to a plain set without one. The datetime
    /// column falls back to the one stored for the table and is stored afterwards. Returns the
    /// number of new documents.
    pub async fn push_docs(
        &self,
        docs: &[Doc],
        table: &str,
        datetime_column: Option<&str>,
    ) -> ConnectorResult<i64> {
        let table_name = quote_table(table);
        let dt_key = get_datetime_column_key(table);
        let remote_dt = self.get(&dt_key).await?;
        let datetime_column = datetime_column
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty())
            .or(remote_dt.filter(|s| !s.is_empty()));
        let count = |b: &Arc<dyn ValkeyBackend>, dt: bool| {
            let b = b.clone();
            let t = table_name.clone();
            async move {
                if dt {
                    b.zcard(&t).await
                } else {
                    b.scard(&t).await
                }
            }
        };
        let old_len = count(&self.backend, datetime_column.is_some())
            .await
            .map_err(be)?;
        for doc in docs {
            let doc_str = dumps_doc(doc, true);
            match &datetime_column {
                Some(dt) => {
                    let ts = push_score(doc.get(dt)).map_err(ConnectorError::Invalid)?;
                    self.backend
                        .zadd(&table_name, &doc_str, ts as f64)
                        .await
                        .map_err(be)?;
                }
                None => self.backend.sadd(&table_name, &doc_str).await.map_err(be)?,
            }
        }
        if let Some(dt) = &datetime_column {
            self.set(&dt_key, dt).await?;
        }
        let new_len = count(&self.backend, datetime_column.is_some())
            .await
            .map_err(be)?;
        Ok(new_len - old_len)
    }

    /// `_push_hash_docs_to_list(docs, table)`: store each document as a hash and append its key
    /// to a list. Returns the next index.
    pub async fn push_hash_docs_to_list(&self, docs: &[Doc], table: &str) -> ConnectorResult<i64> {
        let table_name = quote_table(table);
        let next_ix = self.backend.llen(&table_name).await.map_err(be)?.max(1);
        for (i, doc) in docs.iter().enumerate() {
            let doc_key = format!("{table_name}:{}", next_ix + i as i64);
            let fields: Vec<(String, String)> =
                doc.iter().map(|(k, v)| (k.clone(), v.py_str())).collect();
            self.backend.hset(&doc_key, &fields).await.map_err(be)?;
            self.backend
                .rpush(&table_name, &doc_key)
                .await
                .map_err(be)?;
        }
        Ok(next_ix + docs.len() as i64)
    }

    /// `_read_docs_from_list(table, begin_ix, end_ix)`: the hashes referenced by a list.
    pub async fn read_docs_from_list(
        &self,
        table: &str,
        begin_ix: Option<i64>,
        end_ix: Option<i64>,
    ) -> ConnectorResult<Vec<IndexMap<String, String>>> {
        let begin_ix = begin_ix.unwrap_or(0);
        if end_ix == Some(0) {
            return Ok(Vec::new());
        }
        let end_ix = end_ix.map_or(-1, |e| e - 1);
        let table_name = quote_table(table);
        let keys = self
            .backend
            .lrange(&table_name, begin_ix, end_ix)
            .await
            .map_err(be)?;
        let mut out = Vec::new();
        for key in keys {
            out.push(
                self.backend
                    .hgetall(&key)
                    .await
                    .map_err(be)?
                    .into_iter()
                    .collect(),
            );
        }
        Ok(out)
    }

    /// `get_datetime_column_key(table)`.
    pub fn get_datetime_column_key(&self, table: &str) -> String {
        get_datetime_column_key(table)
    }

    /// The score bounds `read_docs` uses for `begin` / `end`.
    pub(crate) fn score_bounds(
        begin: Option<&KValue>,
        end: Option<&KValue>,
    ) -> ConnectorResult<(ScoreBound, ScoreBound)> {
        let conv = |v: Option<&KValue>, inf: ScoreBound| -> ConnectorResult<ScoreBound> {
            match v {
                None => Ok(inf),
                Some(v) if v.is_na() => Ok(inf),
                Some(v) => Ok(ScoreBound::Value(
                    bound_score(v).map_err(ConnectorError::Invalid)? as f64,
                )),
            }
        };
        Ok((
            conv(begin, ScoreBound::NegInf)?,
            conv(end, ScoreBound::PosInf)?,
        ))
    }

    /// `read_docs(table, begin, end)`: the stored documents, filtered by score when the table
    /// has a datetime column (both bounds inclusive, as `ZRANGEBYSCORE` is).
    pub async fn read_docs(
        &self,
        table: &str,
        begin: Option<&KValue>,
        end: Option<&KValue>,
    ) -> ConnectorResult<Vec<Doc>> {
        let table_name = quote_table(table);
        let datetime_column = self.get(&get_datetime_column_key(table)).await?;
        let members = match datetime_column.filter(|d| !d.is_empty()) {
            None => self.backend.smembers(&table_name).await.map_err(be)?,
            Some(_) => {
                let (min, max) = Self::score_bounds(begin, end)?;
                self.backend
                    .zrangebyscore(&table_name, min, max)
                    .await
                    .map_err(be)?
            }
        };
        members
            .iter()
            .map(|m| loads_doc(m).map_err(ConnectorError::from))
            .collect()
    }

    /// `read(table, begin, end, params, datetime_column, select_columns, omit_columns)`:
    /// `read_docs` as a frame, filtered with `query_df`.
    #[allow(clippy::too_many_arguments)]
    pub async fn read(
        &self,
        table: &str,
        begin: Option<&KValue>,
        end: Option<&KValue>,
        params: Option<&Map>,
        datetime_column: Option<&str>,
        select_columns: Option<Vec<String>>,
        omit_columns: Option<Vec<String>>,
    ) -> ConnectorResult<Option<Frame>> {
        let docs = self.read_docs(table, begin, end).await?;
        let df = docs_frame(&docs, &[], false)?;
        let datetime_column = match datetime_column {
            Some(d) => Some(d.to_string()),
            None => self.get(&get_datetime_column_key(table)).await?,
        };
        let has_dt = datetime_column.is_some();
        let qparams: IndexMap<String, ParamValue> = params
            .map(pipe_ops::params_to_query)
            .map(|p| pipe_ops::coerce_params_to_frame(p, &df))
            .unwrap_or_default();
        let result = query_df(
            &df,
            &QueryOptions {
                params: qparams,
                begin: if has_dt { begin.cloned() } else { None },
                end: if has_dt { end.cloned() } else { None },
                datetime_column,
                select_columns,
                omit_columns,
                reset_index: true,
                coerce_types: false,
            },
        )
        .map_err(|e| ConnectorError::Backend(e.to_string()))?;
        Ok(Some(result.frame))
    }

    /// `drop_table(table)`: delete the table key and its datetime-column key.
    pub async fn drop_table(&self, table: &str) -> ConnectorResult<()> {
        self.backend.del(&[quote_table(table)]).await.map_err(be)?;
        self.backend
            .del(&[get_datetime_column_key(table)])
            .await
            .map_err(be)?;
        Ok(())
    }
}

/// `parse_df_datetimes(docs, ignore_cols, strip_timezone)` on documents.
pub(crate) fn docs_frame(
    docs: &[Doc],
    ignore_cols: &[String],
    strip_timezone: bool,
) -> ConnectorResult<Frame> {
    let df = meerschaum_kernel::docs::frame_from_docs(docs)
        .map_err(|e| ConnectorError::Backend(e.to_string()))?;
    Ok(meerschaum_kernel::docs::parse_df_datetimes(
        &df,
        ignore_cols,
        strip_timezone,
        false,
    ))
}
