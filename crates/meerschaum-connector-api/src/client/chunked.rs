//! `fetch` from an `api:` source, read in bounded windows (v4: the source pipe's
//! `get_data(as_iterator=True)`). The server answers at most its `max_response_row_limit` rows
//! per read and drops the rest silently, so each read asks for [`READ_ROW_LIMIT`] rows
//! explicitly (a server allowing fewer refuses with its own limit, which is then used), and a
//! read that comes back full is split in half and read again: never taken for the whole window.
//! Windows are read one at a time, as the sync asks for them.

use std::collections::VecDeque;

use meerschaum_connector::traits::{ChunkStream, FetchedKind};
use meerschaum_connector::windows::{just_past, refused_row_limit, split_point};
use meerschaum_core::Pipe;
use meerschaum_kernel::enforce::PipeInput;

use crate::traits::{ConnectorError, ConnectorResult, DataQuery, async_trait};

use super::APIConnector;

/// The rows one read asks for: the server's default `api:data:max_response_row_limit`.
pub const READ_ROW_LIMIT: i64 = 100_000;

/// The windows of `source` to read for `query`: the server's own chunk bounds of the source
/// (its `verify` chunk interval) from `query.begin` (else the source's oldest value) to
/// `query.end` (else just past its newest), filtered by `query.params`; one window, the query
/// itself, when the source has no rows to bound (or no datetime axis).
async fn source_windows(
    conn: &APIConnector,
    source: &Pipe,
    query: &DataQuery,
) -> ConnectorResult<VecDeque<DataQuery>> {
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
            .map(just_past),
    };
    let (Some(begin), Some(end)) = (begin, end) else {
        return Ok(VecDeque::from([query.clone()]));
    };
    let bounds = conn
        .get_pipe_chunk_bounds(source, Some(&begin), Some(&end), true, None)
        .await?;
    let windows: VecDeque<DataQuery> = bounds
        .into_iter()
        .filter(|(b, e)| b.is_some() || e.is_some())
        .map(|(begin, end)| DataQuery {
            begin,
            end,
            ..query.clone()
        })
        .collect();
    Ok(if windows.is_empty() {
        VecDeque::from([DataQuery {
            begin: Some(begin),
            end: Some(end),
            ..query.clone()
        }])
    } else {
        windows
    })
}

/// The chunks of an `api:` source, read lazily: one window per [`ChunkStream::next_chunk`].
pub(crate) struct SourceChunks {
    conn: APIConnector,
    source: Pipe,
    query: DataQuery,
    limit: i64,
    /// `None` until the first chunk is asked for.
    windows: Option<VecDeque<DataQuery>>,
}

impl SourceChunks {
    pub(crate) fn new(conn: APIConnector, source: Pipe, query: DataQuery) -> SourceChunks {
        SourceChunks {
            conn,
            source,
            query,
            limit: READ_ROW_LIMIT,
            windows: None,
        }
    }
}

/// Read `window` of `source` with at most `*limit` rows (lowered to a server's own smaller limit
/// when it refuses the larger one), or `Ok(None)` when the window must be split (the read came
/// back full). An unsplittable full read is an error.
async fn read_window(
    conn: &APIConnector,
    source: &Pipe,
    window: &DataQuery,
    limit: &mut i64,
) -> ConnectorResult<Option<meerschaum_kernel::Frame>> {
    loop {
        let limited = DataQuery {
            limit: Some(*limit),
            ..window.clone()
        };
        let df = match conn.get_pipe_data(source, &limited).await {
            Ok(Some(df)) => df,
            // An unregistered source, or one without a table yet: no rows.
            Ok(None) => return Ok(Some(meerschaum_kernel::Frame::empty())),
            Err(e) => match refused_row_limit(&e.to_string()) {
                Some(allowed) if allowed < *limit => {
                    *limit = allowed;
                    continue;
                }
                _ => return Err(e),
            },
        };
        if i64::try_from(df.height()).unwrap_or(i64::MAX) < *limit {
            return Ok(Some(df));
        }
        let splittable = match (&window.begin, &window.end) {
            (Some(begin), Some(end)) => split_point(begin, end).is_some(),
            _ => false,
        };
        if splittable {
            return Ok(None);
        }
        return Err(ConnectorError::Backend(format!(
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
        )));
    }
}

#[async_trait]
impl ChunkStream for SourceChunks {
    async fn next_chunk(&mut self) -> ConnectorResult<Option<PipeInput>> {
        if self.windows.is_none() {
            self.windows = Some(source_windows(&self.conn, &self.source, &self.query).await?);
        }
        let Some(windows) = self.windows.as_mut() else {
            return Ok(None);
        };
        while let Some(window) = windows.pop_front() {
            match read_window(&self.conn, &self.source, &window, &mut self.limit).await? {
                Some(df) if df.height() == 0 => continue,
                Some(df) => return Ok(Some(PipeInput::Frame(df))),
                None => {
                    let (Some(begin), Some(end)) = (&window.begin, &window.end) else {
                        unreachable!("only a bounded window is split");
                    };
                    let middle = split_point(begin, end).expect("checked by read_window");
                    windows.push_front(DataQuery {
                        begin: Some(middle.clone()),
                        ..window.clone()
                    });
                    windows.push_front(DataQuery {
                        end: Some(middle),
                        ..window
                    });
                }
            }
        }
        Ok(None)
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
    let mut limit = READ_ROW_LIMIT;
    loop {
        let limited = DataQuery {
            limit: Some(limit),
            ..query.clone()
        };
        match conn.get_pipe_data(source, &limited).await {
            Ok(Some(df)) if i64::try_from(df.height()).unwrap_or(i64::MAX) >= limit => {
                return Err(ConnectorError::Backend(format!(
                    "{source} answered {limit} rows, the most its server sends in one read \
                     (`api:data:max_response_row_limit`): the rest would be lost. Fetch it in \
                     chunks (sync the pipe) or with a narrower window."
                )));
            }
            Err(e) => match refused_row_limit(&e.to_string()) {
                Some(allowed) if allowed < limit => limit = allowed,
                _ => return Err(e),
            },
            other => return other,
        }
    }
}
