//! Two-phase bounded replacement through the same Main and sealed credential provider.

use meerschaum_connector::source_replace::{
    PreparedSourceReplace, SourceReplaceSummary, SourceReplaceTransport,
};
use meerschaum_connector::{
    Connector, ConnectorError, ConnectorResult, DataQuery, SyncOptions, async_trait,
};
use meerschaum_core::{Pipe, Value};

use super::batch_sender::{BatchSender, Operation};
use super::{APIConnector, Body, RequestOptions};
use crate::{source_fetch, source_replace};

impl APIConnector {
    pub(crate) fn replacement_transport(&self) -> Option<SourceReplaceTransport> {
        if !self
            .logical_identity
            .as_ref()
            .is_some_and(|(typ, _)| typ == "sql")
        {
            return None;
        }
        let provider = self
            .attributes
            .get("credential_file")
            .and_then(Value::as_str)?;
        Some(SourceReplaceTransport {
            origin: self.url.clone(),
            provider: provider.into(),
        })
    }

    pub(crate) async fn prepare_source_replacement(
        &self,
        pipe: &Pipe,
        source: &SourceReplaceTransport,
        query: &DataQuery,
        options: &SyncOptions,
    ) -> ConnectorResult<Option<Box<dyn PreparedSourceReplace>>> {
        if self.replacement_transport().as_ref() != Some(source)
            || self.connector_keys() != pipe.instance_keys()
        {
            return Ok(None);
        }
        if !options.replace
            || query.begin != options.begin
            || query.end != options.end
            || query.params != options.params
        {
            return Err(ConnectorError::Invalid(
                "The prepared source and replacement windows must match.".into(),
            ));
        }
        let request = source_replace::Request {
            version: source_replace::VERSION,
            source: source_fetch::Request::for_pipe(pipe.connector_keys(), pipe, query)
                .map_err(ConnectorError::Invalid)?,
            phase: source_replace::Phase::Prepare,
            id: None,
            enforce_dtypes: options
                .extra
                .get("enforce_dtypes")
                .is_none_or(Value::truthy),
        };
        let mut conn = self.clone();
        conn.response_limit = Some(
            conn.response_limit
                .unwrap_or(source_replace::MAX_RESPONSE_BYTES)
                .min(source_replace::MAX_RESPONSE_BYTES),
        );
        let response = send(&conn, &request, None).await?;
        let source_replace::Response::Prepared { id, summary } = response else {
            return Err(ConnectorError::Invalid(
                "Invalid prepared replacement response.".into(),
            ));
        };
        let mut request = request;
        request.phase = source_replace::Phase::Commit;
        request.id = Some(id);
        request.validate().map_err(ConnectorError::Invalid)?;
        let bound = |value: Option<source_fetch::Bound>| {
            value
                .map(|value| value.to_bound())
                .transpose()
                .map_err(ConnectorError::Invalid)
        };
        let summary = SourceReplaceSummary {
            begin: bound(summary.begin)?,
            end: bound(summary.end)?,
            first: bound(summary.first)?,
            last: bound(summary.last)?,
            rows: summary.rows,
            skipped: summary.skipped,
        };
        Ok(Some(Box::new(Prepared {
            conn,
            request,
            summary,
            finished: false,
            batch_attempted: false,
            pipe: pipe.clone(),
        })))
    }
}

async fn send(
    conn: &APIConnector,
    request: &source_replace::Request,
    batch: Option<&mut BatchSender<'_>>,
) -> ConnectorResult<source_replace::Response> {
    request.validate().map_err(ConnectorError::Invalid)?;
    let path = source_replace::path(&request.source.pipe);
    let options = RequestOptions {
        body: Body::Json(serde_json::to_string(request)?),
        ..Default::default()
    };
    let response = match batch {
        Some(sender) if request.phase == source_replace::Phase::Commit => {
            sender
                .post(&path, options, Operation::SourceReplace)
                .await?
        }
        None => conn.post(&path, options).await?,
        _ => {
            return Err(ConnectorError::Invalid(
                "Invalid batched replacement phase.".into(),
            ));
        }
    };
    if !response.ok() {
        return Err(ConnectorError::Http {
            status: response.status,
            body: response.text(),
        });
    }
    serde_json::from_slice(&response.bytes)
        .map_err(|_| ConnectorError::Invalid("Invalid prepared replacement response.".into()))
}

struct Prepared {
    conn: APIConnector,
    request: source_replace::Request,
    summary: SourceReplaceSummary,
    finished: bool,
    batch_attempted: bool,
    pipe: Pipe,
}

#[async_trait]
impl PreparedSourceReplace for Prepared {
    fn summary(&self) -> &SourceReplaceSummary {
        &self.summary
    }

    async fn commit(&mut self) -> ConnectorResult<meerschaum_core::success::SyncResult> {
        if self.finished {
            return Err(ConnectorError::Invalid(
                "The replacement lease is already closed.".into(),
            ));
        }
        if self.batch_attempted {
            return Err(ConnectorError::Invalid(
                "The exact replacement batch was already attempted. Inspect the original operation before another write.".into(),
            ));
        }
        // This distinct request rereads the sealed credential provider after materialization
        // and observer work; the commit never borrows an expired prepare credential.
        let mut sender = BatchSender::new(&self.conn, &self.pipe).await?;
        self.batch_attempted = sender.is_batched();
        let source_replace::Response::Committed { result } =
            send(&self.conn, &self.request, Some(&mut sender)).await?
        else {
            return Err(ConnectorError::Invalid(
                "Invalid replacement commit response.".into(),
            ));
        };
        self.finished = true;
        if result.success {
            self.conn.invalidate_pipe_cache(&self.pipe);
        }
        Ok(result)
    }

    async fn abandon(&mut self) -> ConnectorResult<()> {
        if self.batch_attempted {
            return Ok(());
        }
        self.close().await
    }

    async fn close(&mut self) -> ConnectorResult<()> {
        if self.finished {
            return Ok(());
        }
        let mut request = self.request.clone();
        request.phase = source_replace::Phase::Close;
        let source_replace::Response::Closed = send(&self.conn, &request, None).await? else {
            return Err(ConnectorError::Invalid(
                "Invalid replacement close response.".into(),
            ));
        };
        self.finished = true;
        Ok(())
    }
}

impl Drop for Prepared {
    fn drop(&mut self) {
        // Closing an unresolved lease can cancel the writer or remove its retained
        // receipt. Main bounds the lifetime of abandoned preparations and receipts.
        if self.finished || self.batch_attempted {
            return;
        }
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };
        let conn = self.conn.clone();
        let mut request = self.request.clone();
        request.phase = source_replace::Phase::Close;
        handle.spawn(async move {
            let _ = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                send(&conn, &request, None),
            )
            .await;
        });
    }
}
