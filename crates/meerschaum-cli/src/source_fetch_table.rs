//! Source broker adapter for the connector's typed registered table read.

use std::io::Write;
use std::sync::{Arc, atomic::AtomicBool};
use std::time::{Duration, Instant};

use meerschaum_connector_api::source_fetch::{self, Response};
use meerschaum_connector_sql::{
    SqlConnector,
    instance::registered_source::{RegisteredSource, SourceReadBudget, SourceReadError},
};
use meerschaum_server::{backends::SourceFetchError, routes::source_fetch::Plan};

use super::{EncodedBudget, SnapshotIdentity, response};
use crate::source_fetch_snapshot::{Snapshot, Store};

fn source(plan: &Plan) -> Result<RegisteredSource<'_>, SourceFetchError> {
    let table = plan
        .registered_table()
        .ok_or(SourceFetchError::Unsupported)?;
    Ok(RegisteredSource {
        pipe: plan.pipe(),
        registration: table.registration,
        schema: &table.schema,
        table: &table.table,
    })
}

fn error(error: SourceReadError) -> SourceFetchError {
    match error {
        SourceReadError::Unsupported => SourceFetchError::Unsupported,
        SourceReadError::Changed => SourceFetchError::Stale,
        SourceReadError::TooLarge => SourceFetchError::TooLarge,
        SourceReadError::Unavailable => SourceFetchError::Unavailable,
    }
}

fn empty() -> Response {
    Response {
        version: source_fetch::VERSION,
        dtypes: Default::default(),
        rows: Vec::new(),
        complete: true,
    }
}

fn budget<'a>(plan: &Plan, timeout: Duration, cancelled: &'a AtomicBool) -> SourceReadBudget<'a> {
    SourceReadBudget {
        deadline: Instant::now() + timeout,
        cancelled,
        batch_rows: source_fetch::PAGE_ROWS.min(plan.request().row_limit),
        batch_bytes: source_fetch::MAX_PAGE_BYTES - 1024,
    }
}

pub(super) async fn read(
    sql: &SqlConnector,
    plan: &Plan,
    timeout: Duration,
    cancelled: &AtomicBool,
) -> Result<Response, SourceFetchError> {
    let request = plan.request();
    let mut query = request.data_query().map_err(|_| SourceFetchError::Failed)?;
    let count = request.row_limit + 1;
    query.limit = Some(request.limit.map_or(count, |limit| limit.min(count)) as i64);
    let mut result = empty();
    let mut encoded = EncodedBudget {
        bytes: 128,
        limit: source_fetch::MAX_RESPONSE_BYTES,
    };
    let mut failure = None;
    let outcome = sql
        .read_registered_source(
            source(plan)?,
            Some(&query),
            budget(plan, timeout, cancelled),
            |frame| {
                let record = (|| {
                    let available = request.row_limit.saturating_sub(result.rows.len());
                    let keep = available.min(frame.height());
                    result.complete &= keep == frame.height();
                    let frame = if keep == frame.height() {
                        frame
                    } else {
                        frame.take(&(0..keep).collect::<Vec<_>>())
                    };
                    let page = response(&frame, true)?;
                    if result.dtypes.is_empty() {
                        serde_json::to_writer(&mut encoded, &page.dtypes)
                            .map_err(|_| SourceFetchError::TooLarge)?;
                        result.dtypes = page.dtypes;
                    } else if result.dtypes != page.dtypes {
                        return Err(SourceFetchError::Failed);
                    }
                    for row in page.rows {
                        serde_json::to_writer(&mut encoded, &row)
                            .map_err(|_| SourceFetchError::TooLarge)?;
                        encoded
                            .write_all(b",")
                            .map_err(|_| SourceFetchError::TooLarge)?;
                        result.rows.push(row);
                    }
                    Ok(())
                })();
                record.map_err(|err| {
                    failure = Some(err);
                    SourceReadError::Unavailable
                })
            },
        )
        .await;
    outcome.map_err(|err| failure.unwrap_or_else(|| error(err)))?;
    Ok(result)
}

pub(super) async fn capture(
    sql: &SqlConnector,
    plan: &Plan,
    store: &Arc<Store>,
    identity: SnapshotIdentity<'_>,
    timeout: Duration,
    cancelled: &AtomicBool,
) -> Result<Snapshot, SourceFetchError> {
    let query = plan
        .request()
        .data_query()
        .map_err(|_| SourceFetchError::Failed)?;
    let mut writer = store.writer()?;
    let mut failure = None;
    let schema = sql
        .read_registered_source(
            source(plan)?,
            Some(&query),
            budget(plan, timeout, cancelled),
            |frame| {
                response(&frame, true)
                    .and_then(|page| writer.push(&page))
                    .map_err(|err| {
                        failure = Some(err);
                        SourceReadError::Unavailable
                    })
            },
        )
        .await
        .map_err(|err| failure.unwrap_or_else(|| error(err)))?;
    if writer.is_empty() {
        writer.push(&empty())?;
    }
    Ok(writer.finish(
        identity.binding,
        schema,
        identity.configuration.into(),
        snapshot_key(plan)?,
    ))
}

pub(super) async fn proof(
    sql: &SqlConnector,
    plan: &Plan,
    timeout: Duration,
    cancelled: &AtomicBool,
) -> Result<String, SourceFetchError> {
    sql.read_registered_source(
        source(plan)?,
        None,
        budget(plan, timeout, cancelled),
        |_| Ok(()),
    )
    .await
    .map_err(error)
}

// Snapshot binding already carries the exact request fingerprint. This distinct source
// key keeps the type of read and its approved physical identity in the store proof too.
pub(super) fn snapshot_key(plan: &Plan) -> Result<String, SourceFetchError> {
    let table = source(plan)?;
    Ok(format!(
        "postgresql:{}:{}:{}",
        table.registration, table.schema, table.table
    ))
}
