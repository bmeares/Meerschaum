//! `fetch` from an `api:` source, read in bounded windows (v4: the source pipe's
//! `get_data(as_iterator=True)`). The server answers at most its `max_response_row_limit` rows
//! per read and drops the rest silently, so each read asks for the connector's
//! `max_response_row_limit` attribute (else [`READ_ROW_LIMIT`]) rows explicitly (a server
//! allowing fewer refuses with a `413` naming its own limit, which is then used, a few times at
//! most), and a read that comes back full is split in half and read again: never taken for the
//! whole window. Only an unregistered source or one without a table reads as empty; any other
//! refusal (a lost permission, a rate limit) is an error. Windows are read one at a time, as the
//! sync asks for them.

use std::collections::VecDeque;

use meerschaum_connector::traits::{ChunkStream, FetchedKind};
use meerschaum_connector::windows::{
    ROW_LIMIT_RETRIES, Windows, advance, just_past, refused_row_limit, split_point,
};
use meerschaum_core::Pipe;
use meerschaum_kernel::enforce::PipeInput;

use crate::traits::{ConnectorError, ConnectorResult, DataQuery, async_trait};

use super::APIConnector;

/// The rows one read asks for: the server's default `api:data:max_response_row_limit`.
pub const READ_ROW_LIMIT: i64 = 100_000;

/// The windows of `source` to read for `query`, made as they are asked for: the source's own
/// chunk interval (its `verify` parameters, read from this API) from `query.begin` (else the
/// source's oldest value) to `query.end` (else just past its newest), filtered by
/// `query.params`. One window, the query itself, when the source has no rows to bound, no
/// datetime axis or no end past its newest value; a full read of any window is split anyway.
enum Plan {
    Windows(Windows, DataQuery),
    Once(Option<DataQuery>),
}

impl Plan {
    fn next(&mut self) -> Option<DataQuery> {
        match self {
            Plan::Windows(windows, query) => windows.next().map(|(begin, end)| DataQuery {
                begin: Some(begin),
                end: Some(end),
                ..query.clone()
            }),
            Plan::Once(query) => query.take(),
        }
    }
}

async fn plan(conn: &APIConnector, source: &Pipe, query: &DataQuery) -> ConnectorResult<Plan> {
    let params = query.params.as_ref();
    let begin = match &query.begin {
        Some(begin) => Some(begin.clone()),
        None => conn.get_sync_time(source, params, false).await?,
    };
    let end = match &query.end {
        Some(end) => Some(end.clone()),
        None => conn
            .get_sync_time(source, params, true)
            .await?
            .and_then(just_past),
    };
    let (Some(begin), Some(end)) = (begin, end) else {
        return Ok(Plan::Once(Some(query.clone())));
    };
    let mut sized = source.clone();
    sized.set_parameters(conn.pipe_resolved_parameters(source).await);
    let interval = sized
        .get_chunk_interval(None, &meerschaum_core::pipe::PipeDefaults::default())
        .ok()
        .filter(|interval| advance(&begin, interval).is_some());
    Ok(match interval {
        Some(interval) => Plan::Windows(Windows::new(begin, end, interval), query.clone()),
        // A chunk interval of another kind than the axis (or none): one window, split as needed.
        None => Plan::Once(Some(DataQuery {
            begin: Some(begin),
            end: Some(end),
            ..query.clone()
        })),
    })
}

/// The chunks of an `api:` source, read lazily: one window per [`ChunkStream::next_chunk`].
pub(crate) struct SourceChunks {
    conn: APIConnector,
    source: Pipe,
    query: DataQuery,
    limit: i64,
    /// How many more times the limit may be lowered to a server's smaller one.
    retries: u32,
    /// The windows still to make (`None` until the first chunk is asked for).
    plan: Option<Plan>,
    /// The halves of split windows, read before the next new window.
    pending: VecDeque<DataQuery>,
}

impl APIConnector {
    /// The rows one read of a source asks for: the connector's positive `max_response_row_limit`
    /// attribute, else [`READ_ROW_LIMIT`].
    pub(crate) fn read_row_limit(&self) -> i64 {
        match self.attributes.get("max_response_row_limit") {
            Some(meerschaum_core::Value::Int(n)) if *n > 0 => *n,
            Some(meerschaum_core::Value::Str(s)) => s
                .trim()
                .parse()
                .ok()
                .filter(|n: &i64| *n > 0)
                .unwrap_or(READ_ROW_LIMIT),
            _ => READ_ROW_LIMIT,
        }
    }
}

impl SourceChunks {
    pub(crate) fn new(conn: APIConnector, source: Pipe, query: DataQuery) -> SourceChunks {
        let limit = conn.read_row_limit();
        SourceChunks {
            conn,
            source,
            query,
            limit,
            retries: ROW_LIMIT_RETRIES,
            plan: None,
            pending: VecDeque::new(),
        }
    }
}

/// Read `window` of `source` (strictly) with at most `*limit` rows, lowering `*limit` to the
/// smaller limit a server names in its `413` refusal (at most `*retries` times).
async fn read_limited(
    conn: &APIConnector,
    source: &Pipe,
    window: &DataQuery,
    limit: &mut i64,
    retries: &mut u32,
) -> ConnectorResult<meerschaum_kernel::Frame> {
    loop {
        let limited = DataQuery {
            limit: Some(*limit),
            ..window.clone()
        };
        match conn.read_pipe_data(source, &limited, true).await {
            Ok(Some(df)) => return Ok(df),
            // An unregistered source (`409`), or one without a table yet (`400`): no rows.
            Ok(None) => return Ok(meerschaum_kernel::Frame::empty()),
            Err(e) => match refused_row_limit(&e) {
                Some(allowed) if allowed < *limit && *retries > 0 => {
                    *limit = allowed;
                    *retries -= 1;
                }
                _ => return Err(e),
            },
        }
    }
}

/// Read `window` of `source` with at most `*limit` rows, or `Ok(None)` when the window must be
/// split (the read came back full). An unsplittable full read is an error.
async fn read_window(
    conn: &APIConnector,
    source: &Pipe,
    window: &DataQuery,
    limit: &mut i64,
    retries: &mut u32,
) -> ConnectorResult<Option<meerschaum_kernel::Frame>> {
    let df = read_limited(conn, source, window, limit, retries).await?;
    if i64::try_from(df.height()).unwrap_or(i64::MAX) < *limit {
        return Ok(Some(df));
    }
    if let (Some(begin), Some(end)) = (&window.begin, &window.end)
        && split_point(begin, end).is_some()
    {
        return Ok(None);
    }
    Err(ConnectorError::Backend(format!(
        "{source} answered {limit} rows between {} and {}, the most its server sends in one \
         read (`api:data:max_response_row_limit`), and these rows cannot be read in smaller \
         parts: the rest would be lost. Raise the server's limit.",
        window
            .begin
            .as_ref()
            .map_or("None".into(), crate::traits::DateBound::py_str),
        window
            .end
            .as_ref()
            .map_or("None".into(), crate::traits::DateBound::py_str),
    )))
}

#[async_trait]
impl ChunkStream for SourceChunks {
    async fn next_chunk(&mut self) -> ConnectorResult<Option<PipeInput>> {
        if self.plan.is_none() {
            self.plan = Some(plan(&self.conn, &self.source, &self.query).await?);
        }
        loop {
            let window = match self.pending.pop_front() {
                Some(window) => window,
                None => match self.plan.as_mut().and_then(Plan::next) {
                    Some(window) => window,
                    None => return Ok(None),
                },
            };
            match read_window(
                &self.conn,
                &self.source,
                &window,
                &mut self.limit,
                &mut self.retries,
            )
            .await?
            {
                Some(df) if df.height() == 0 => continue,
                Some(df) => return Ok(Some(PipeInput::Frame(df))),
                None => {
                    let (Some(begin), Some(end)) = (&window.begin, &window.end) else {
                        unreachable!("only a bounded window is split");
                    };
                    let middle = split_point(begin, end).expect("checked by read_window");
                    self.pending.push_front(DataQuery {
                        begin: Some(middle.clone()),
                        ..window.clone()
                    });
                    self.pending.push_front(DataQuery {
                        end: Some(middle),
                        ..window
                    });
                }
            }
        }
    }

    async fn finish(&mut self) -> ConnectorResult<FetchedKind> {
        // v4's `fetch` returned the source's chunk generator.
        Ok(FetchedKind::Data { generator: true })
    }
}

/// [`Connector::fetch`](crate::traits::Connector::fetch) as one frame: one read of the whole
/// query, an error when it comes back as full as the server allows (use the chunked fetch).
pub(crate) async fn fetch_whole(
    conn: &APIConnector,
    source: &Pipe,
    query: &DataQuery,
) -> ConnectorResult<Option<meerschaum_kernel::Frame>> {
    let (mut limit, mut retries) = (conn.read_row_limit(), ROW_LIMIT_RETRIES);
    let df = read_limited(conn, source, query, &mut limit, &mut retries).await?;
    if i64::try_from(df.height()).unwrap_or(i64::MAX) >= limit {
        return Err(ConnectorError::Backend(format!(
            "{source} answered {limit} rows, the most its server sends in one read \
             (`api:data:max_response_row_limit`): the rest would be lost. Fetch it in chunks \
             (sync the pipe) or with a narrower window."
        )));
    }
    Ok(Some(df))
}
