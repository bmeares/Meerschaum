//! Logical instance identities transported over an authenticated main callback.

use meerschaum_core::team_scope::api_execution::Alias;
use meerschaum_core::{Map, Pipe};

use crate::source_fetch;
use crate::{ConnectorError, ConnectorResult, DataQuery};

use super::{APIConnector, Body, RequestOptions};

impl APIConnector {
    /// Resolve an exact alias section before selecting a native database driver. An invalid
    /// descriptor is an error, never `None` (which could permit native-driver fallback).
    pub fn execution_alias_from_config(
        config: &meerschaum_core::Config,
        keys: &str,
    ) -> ConnectorResult<Option<Self>> {
        let (typ, label) = keys.split_once(':').ok_or_else(|| {
            ConnectorError::Invalid("An execution alias needs type:label keys.".into())
        })?;
        let attrs = config
            .get(&["meerschaum", "connectors", typ, label])
            .map(|v| v.to_map())
            .unwrap_or_default();
        Alias::from_attributes(&attrs, keys)
            .map_err(ConnectorError::Invalid)?
            .map(|alias| Self::from_execution_alias(&alias))
            .transpose()
    }

    /// Make an API transport without changing `sql:label` / `valkey:label` object identities.
    /// Source fetch is supported only through the canonical native SQL broker.
    pub fn from_execution_alias(alias: &Alias) -> ConnectorResult<Self> {
        alias
            .validate(&alias.logical_keys)
            .map_err(ConnectorError::Invalid)?;
        let origin = reqwest::Url::parse(&alias.callback_url)
            .map_err(|_| ConnectorError::Invalid("Invalid API callback origin.".into()))?;
        if !matches!(origin.scheme(), "http" | "https")
            || origin.host().is_none()
            || !origin.username().is_empty()
            || origin.password().is_some()
            || origin.query().is_some()
            || origin.fragment().is_some()
            || !matches!(origin.path(), "" | "/")
            || origin.port() == Some(0)
        {
            return Err(ConnectorError::Invalid(
                "Invalid API callback origin.".into(),
            ));
        }
        let mut connector = Self::new(
            Some("execution_callback"),
            Map::from_iter([
                ("uri".into(), alias.callback_url.clone().into()),
                (
                    "credential_file".into(),
                    alias.credential_file.to_string_lossy().to_string().into(),
                ),
                ("instance_keys".into(), alias.logical_keys.clone().into()),
            ]),
        )?;
        let (typ, label) = alias.logical_keys.split_once(':').expect("validated alias");
        connector.logical_identity = Some((typ.into(), label.into()));
        Ok(connector)
    }

    /// Read only the canonical registered source. Local SQL/parameters are never sent.
    pub async fn fetch_registered_source(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<meerschaum_kernel::Frame>> {
        let Some((typ, label)) = &self.logical_identity else {
            return Err(ConnectorError::Invalid(
                "A source broker requires an execution alias.".into(),
            ));
        };
        if typ != "sql" {
            return Err(ConnectorError::NotImplemented(
                "This logical source has no native SQL broker.".into(),
            ));
        }
        let request = source_fetch::Request::for_pipe(&format!("{typ}:{label}"), pipe, query)
            .map_err(ConnectorError::Invalid)?;
        let mut bounded = self.clone();
        bounded.response_limit = Some(
            self.response_limit
                .unwrap_or(source_fetch::MAX_RESPONSE_BYTES)
                .min(source_fetch::MAX_RESPONSE_BYTES),
        );
        let response = bounded
            .post(
                &source_fetch::path(&request.pipe),
                RequestOptions {
                    body: Body::Json(serde_json::to_string(&request).map_err(|_| {
                        ConnectorError::Invalid("Cannot encode source fetch request.".into())
                    })?),
                    ..Default::default()
                },
            )
            .await?;
        if !response.ok() {
            return Err(ConnectorError::Http {
                status: response.status,
                body: response.text(),
            });
        }
        if response.bytes.len() > source_fetch::MAX_RESPONSE_BYTES {
            return Err(ConnectorError::Invalid(
                "Source fetch response exceeded its byte limit.".into(),
            ));
        }
        let result: source_fetch::Response =
            serde_json::from_slice(&response.bytes).map_err(|_| {
                ConnectorError::Invalid("Invalid canonical source fetch response.".into())
            })?;
        result.validate(&request).map_err(ConnectorError::Invalid)?;
        let frame = decode_frame(result)?;
        Ok(Some(frame))
    }
}

pub(crate) const REPLACE_REFUSAL: &str = "Bounded source replacement requires a registered destination on the same Main and sealed credential provider.";

/// Decode one bounded typed page without accumulating any earlier page's rows.
fn decode_frame(result: source_fetch::Response) -> ConnectorResult<meerschaum_kernel::Frame> {
    let docs = result
        .rows
        .into_iter()
        .map(|row| {
            row.into_iter()
                .map(|(name, value)| (name, meerschaum_kernel::value::Value::from(value)))
                .collect()
        })
        .collect::<Vec<meerschaum_connector::pipe_ops::Doc>>();
    let frame = meerschaum_kernel::docs::frame_from_docs(&docs)
        .map_err(|_| ConnectorError::Invalid("Invalid source fetch records.".into()))?;
    meerschaum_kernel::enforce::enforce_dtypes(
        &frame,
        &result.dtypes,
        &meerschaum_kernel::enforce::EnforceOptions::default(),
    )
    .map_err(|_| ConnectorError::Invalid("Invalid source fetch column types.".into()))
}

/// A complete main-owned source snapshot, consumed one bounded native frame at a time.
/// SQL and credentials are never included in the structured source request.
pub(crate) struct PagedSource {
    conn: APIConnector,
    request: source_fetch::PageRequest,
    complete: bool,
    failed: bool,
    rows: usize,
}

impl PagedSource {
    pub(crate) fn new(conn: APIConnector, pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Self> {
        let Some((typ, label)) = &conn.logical_identity else {
            return Err(ConnectorError::Invalid(
                "A source snapshot requires an execution alias.".into(),
            ));
        };
        if typ != "sql" {
            return Err(ConnectorError::NotImplemented(
                "This logical source has no native SQL broker.".into(),
            ));
        }
        let request = source_fetch::Request::for_pipe(&format!("{typ}:{label}"), pipe, query)
            .map_err(ConnectorError::Invalid)?;
        let mut conn = conn;
        conn.response_limit = Some(
            conn.response_limit
                .unwrap_or(source_fetch::MAX_PAGE_BYTES)
                .min(source_fetch::MAX_PAGE_BYTES),
        );
        Ok(Self {
            conn,
            request: source_fetch::PageRequest {
                version: source_fetch::PAGE_VERSION,
                request,
                cursor: None,
                close: false,
            },
            complete: false,
            failed: false,
            rows: 0,
        })
    }

    async fn page(&mut self) -> ConnectorResult<Option<meerschaum_kernel::enforce::PipeInput>> {
        if self.failed {
            return Err(ConnectorError::Invalid(
                "The source snapshot stream has failed.".into(),
            ));
        }
        if self.complete {
            return Ok(None);
        }
        // Request authentication rereads the launch's credential file on every HTTP request.
        // The main binds the cursor to the stable grant/launch, so rotation is supported.
        let response = self
            .conn
            .post(
                &source_fetch::page_path(&self.request.request.pipe),
                RequestOptions {
                    body: Body::Json(serde_json::to_string(&self.request).map_err(|_| {
                        ConnectorError::Invalid("Cannot encode source snapshot request.".into())
                    })?),
                    ..Default::default()
                },
            )
            .await?;
        if !response.ok() {
            return Err(ConnectorError::Http {
                status: response.status,
                body: response.text(),
            });
        }
        let result: source_fetch::PageResponse = serde_json::from_slice(&response.bytes)
            .map_err(|_| ConnectorError::Invalid("Invalid source snapshot response.".into()))?;
        result
            .validate(&self.request)
            .map_err(ConnectorError::Invalid)?;
        self.rows = self
            .rows
            .checked_add(result.data.rows.len())
            .ok_or_else(|| {
                ConnectorError::Invalid("The source snapshot row count overflowed.".into())
            })?;
        if self
            .request
            .request
            .limit
            .is_some_and(|limit| self.rows > limit)
        {
            return Err(ConnectorError::Invalid(
                "The source snapshot exceeded its explicit query limit.".into(),
            ));
        }
        self.complete = result.complete;
        self.request.cursor = result.cursor;
        if result.data.rows.is_empty() && self.complete {
            return Ok(None);
        }
        Ok(Some(meerschaum_kernel::enforce::PipeInput::Frame(
            decode_frame(result.data)?,
        )))
    }
}

#[meerschaum_connector::async_trait]
impl meerschaum_connector::ChunkStream for PagedSource {
    fn collection_refusal(&self) -> Option<&'static str> {
        Some(REPLACE_REFUSAL)
    }

    async fn next_chunk(
        &mut self,
    ) -> ConnectorResult<Option<meerschaum_kernel::enforce::PipeInput>> {
        let result = self.page().await;
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    async fn finish(&mut self) -> ConnectorResult<meerschaum_connector::FetchedKind> {
        if self.failed || !self.complete {
            return Err(ConnectorError::Invalid(
                "The source snapshot did not reach its final page.".into(),
            ));
        }
        Ok(meerschaum_connector::FetchedKind::Data { generator: true })
    }
}

impl Drop for PagedSource {
    fn drop(&mut self) {
        if self.complete || self.request.cursor.is_none() {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let conn = self.conn.clone();
        let mut request = self.request.clone();
        request.close = true;
        // An abandoned stream releases its disk quota; bounded server TTL is the fallback
        // when the process/runtime disappears before this best-effort close can run.
        handle.spawn(async move {
            let Ok(body) = serde_json::to_string(&request) else {
                return;
            };
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                conn.post(
                    &source_fetch::page_path(&request.request.pipe),
                    RequestOptions {
                        body: Body::Json(body),
                        ..Default::default()
                    },
                ),
            )
            .await;
        });
    }
}
