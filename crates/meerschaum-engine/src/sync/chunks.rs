//! Syncing a fetch delivered in chunks ([`ChunkedFetch`]): collecting the chunks for a replace,
//! syncing each chunk of a chunked source or a generator, and summarizing the chunks' results.

use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use meerschaum_connector::pipe_ops::{self};
use meerschaum_connector::{
    ChunkStream, ChunkedFetch, Connector, ConnectorError, DateBound, FetchedKind,
    InstanceConnector, async_trait,
};
use meerschaum_core::Pipe;
use meerschaum_core::success::SyncResult;
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::Value as KValue;

use super::{
    DeltaObserver, FetchChunk, SyncArgs, SyncEnv, TRIM_INTERVAL, date_bound_to_bound, fail,
    fetch_failed, sync_inner,
};

/// Every chunk of a chunked fetch as one input, for a replacing sync (one window, one
/// transaction). A fetch that returned hook results or synced in parallel has nothing to
/// replace with: its result is returned as `Err`, as the chunked flow reports it.
pub(super) async fn collect_chunks(
    pipe: &Pipe,
    chunked: ChunkedFetch,
) -> Result<PipeInput, Box<SyncResult>> {
    let mut chunks = chunked.chunks;
    let failed = |e: ConnectorError| Box::new(fetch_failed(pipe, pipe.connector_keys(), &e));
    if let Some(reason) = chunks.collection_refusal() {
        return Err(failed(ConnectorError::NotImplemented(reason.into())));
    }
    let mut inputs: Vec<PipeInput> = Vec::new();
    loop {
        match chunks.next_chunk().await {
            Ok(Some(chunk)) => inputs.push(chunk),
            Ok(None) => break,
            Err(e) => return Err(failed(e)),
        }
    }
    match chunks.finish().await {
        Err(e) => return Err(failed(e)),
        Ok(FetchedKind::HookResults(results)) if inputs.is_empty() => {
            return Err(Box::new(hook_results_summary(&results)));
        }
        Ok(FetchedKind::InParallel) if inputs.is_empty() => {
            return Err(Box::new(SyncResult::new(
                true,
                format!("{pipe} is being synced in parallel."),
            )));
        }
        Ok(FetchedKind::Nothing) if inputs.is_empty() => {
            return Err(Box::new(fail(format!("No data were fetched for {pipe}."))));
        }
        Ok(_) => {}
    }
    if inputs.len() == 1 {
        return Ok(inputs.remove(0));
    }
    // Frames (SQL fetch chunks) stay columnar: one frame of every chunk's rows.
    if inputs.iter().all(|i| matches!(i, PipeInput::Frame(_))) {
        let frames: Vec<Frame> = inputs
            .into_iter()
            .filter_map(|i| match i {
                PipeInput::Frame(f) => Some(f),
                _ => None,
            })
            .collect();
        return Frame::concat(&frames).map(PipeInput::Frame).map_err(|e| {
            Box::new(fail(format!(
                "Failed to combine the chunks of {pipe}:\n{e}"
            )))
        });
    }
    let mut docs = Vec::new();
    for input in inputs {
        match input {
            PipeInput::Docs(d) => docs.extend(d),
            PipeInput::Frame(f) => docs.extend(pipe_ops::frame_to_docs(&f)),
            PipeInput::Columns(c) => {
                let n = c.values().map(Vec::len).max().unwrap_or(0);
                for i in 0..n {
                    docs.push(
                        c.iter()
                            .map(|(k, v)| (k.clone(), v.get(i).cloned().unwrap_or(KValue::Null)))
                            .collect(),
                    );
                }
            }
        }
    }
    Ok(PipeInput::Docs(docs))
}

/// Whether a fetch from `source` would hold a read open on the storage the sync writes to
/// ([`Connector::exclusive_storage`]): such a fetch is read whole instead of streamed.
pub(super) fn shares_exclusive_storage(
    source: &dyn Connector,
    instance: &dyn InstanceConnector,
) -> bool {
    source
        .exclusive_storage()
        .is_some_and(|storage| instance.exclusive_storage().as_ref() == Some(&storage))
}

/// The rows of one fetched frame in chunks of `size` rows, for a source that cannot stream: an
/// empty frame is one empty chunk.
pub(super) struct FrameChunks {
    df: Frame,
    size: usize,
    start: usize,
    done: bool,
}

impl FrameChunks {
    pub(super) fn new(df: Frame, size: usize) -> Self {
        FrameChunks {
            df,
            size: size.max(1),
            start: 0,
            done: false,
        }
    }
}

#[async_trait]
impl ChunkStream for FrameChunks {
    async fn next_chunk(&mut self) -> Result<Option<PipeInput>, ConnectorError> {
        if self.done {
            return Ok(None);
        }
        let height = self.df.height();
        let end = (self.start + self.size).min(height);
        let chunk = self.df.take(&(self.start..end).collect::<Vec<_>>());
        self.start = end;
        self.done = self.start >= height;
        Ok(Some(PipeInput::Frame(chunk)))
    }

    async fn finish(&mut self) -> Result<FetchedKind, ConnectorError> {
        Ok(FetchedKind::Data { generator: true })
    }
}

/// SQL `fetch` yields chunks in v4, including one empty chunk for an empty result. Each chunk
/// is synced as it arrives ([`Connector::fetch_stream`], or the chunks of a frame read whole),
/// so a streamed fetch holds a few chunks in memory, never the whole result. A read that fails
/// after the first chunk keeps the chunks synced so far and fails the sync with the error.
pub(super) async fn sync_fetched_chunks(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    mut chunks: Box<dyn ChunkStream>,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
) -> SyncResult {
    let mut results = Vec::new();
    let mut aggregate = SyncResult::new(true, String::new());
    loop {
        let input = match chunks.next_chunk().await {
            Ok(Some(input)) => input,
            Ok(None) => break,
            Err(e) if results.is_empty() => {
                return fetch_failed(pipe, pipe.connector_keys(), &e);
            }
            Err(e) => {
                let detail = format!(
                    "Failed to fetch data from {}:\n    {e}",
                    pipe.connector_keys()
                );
                tracing::warn!("{detail}");
                aggregate.success = false;
                results.push((false, detail));
                break;
            }
        };
        let chunk = match input {
            PipeInput::Frame(frame) => frame,
            other => match input_frame(&other) {
                Some(frame) => frame,
                None => return fail(format!("Failed to read a fetched chunk of {pipe}.")),
            },
        };
        let label = fetched_chunk_label(&chunk, pipe.column("datetime").as_deref());
        let rows = chunk.height();
        meerschaum_core::dprint!(
            "Fetched chunk {} of {pipe}: {rows} rows ({}).",
            results.len(),
            label.trim()
        );
        let info = {
            let (begin, end) = fetched_chunk_bounds(&chunk, pipe.column("datetime").as_deref());
            FetchChunk {
                index: results.len(),
                rows: rows as u64,
                begin,
                end,
            }
        };
        if let Some(o) = observer {
            o.chunk_started(&info);
        }
        let chunk_start = Instant::now();
        let result = Box::pin(sync_inner(
            env,
            pipe,
            Some(PipeInput::Frame(chunk)),
            args,
            observer,
        ))
        .await;
        crate::memory::release_free_memory_throttled(TRIM_INTERVAL);
        if let Some(o) = observer {
            o.chunk_finished(&info, &result, chunk_start.elapsed());
        }
        let message = if results.is_empty() {
            let msg = format!("\n{label}\n{}", result.message);
            if !result.success {
                if env.issued_batch() {
                    let mut partial = result;
                    partial.message = format!("Unable to sync initial chunk for {pipe}:\n{msg}");
                    return partial;
                }
                return fail(format!("Unable to sync initial chunk for {pipe}:\n{msg}"));
            }
            msg
        } else {
            format!(
                "{} a chunk ({} row{}) to {pipe}:\n{label}\n{}",
                if result.success {
                    "Synced"
                } else {
                    "Failed to sync"
                },
                comma_count(rows),
                if rows == 1 { "" } else { "s" },
                result.message
            )
        };
        aggregate.success &= result.success;
        aggregate.inserted += result.inserted;
        aggregate.updated += result.updated;
        aggregate.upserted += result.upserted;
        aggregate.deleted += result.deleted;
        aggregate.begin = aggregate.begin.or(result.begin);
        aggregate.end = result.end;
        aggregate.changed_intervals.extend(result.changed_intervals);
        results.push((result.success, message));
        if !result.success && env.delivery.is_some() {
            break;
        }
    }
    // PARITY: v4 sorts the `(success, message)` tuples, putting failed chunks first.
    summarize_chunk_results(pipe, results, aggregate)
}

/// A raw chunk as a frame, for its label and progress bounds only (never synced from this).
fn input_frame(input: &PipeInput) -> Option<Frame> {
    match input {
        PipeInput::Frame(f) => Some(f.clone()),
        PipeInput::Docs(d) => meerschaum_kernel::docs::frame_from_docs(d).ok(),
        PipeInput::Columns(c) => meerschaum_kernel::docs::frame_from_columns(c).ok(),
    }
}

fn input_rows(input: &PipeInput) -> usize {
    match input {
        PipeInput::Frame(f) => f.height(),
        PipeInput::Docs(d) => d.len(),
        PipeInput::Columns(c) => c.values().map(Vec::len).max().unwrap_or(0),
    }
}

/// v4's `_sync` for a `fetch()` result that is not one DataFrame (`core/Pipe/_sync.py`, "If
/// connector is a plugin with a `sync()` method ... Fetch the dataframe from the connector's
/// `fetch()` method"): `None`, `True`, the results of the `chunk_hook` calls, a list that came
/// back empty, a generator (every chunk synced on its own), or a single frame.
pub(super) async fn sync_chunked_source(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    chunked: ChunkedFetch,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
) -> SyncResult {
    let mut chunks = chunked.chunks;
    let fetch_failed = |e: ConnectorError| fetch_failed(pipe, pipe.connector_keys(), &e);
    let first = match chunks.next_chunk().await {
        Ok(first) => first,
        Err(e) => return fetch_failed(e),
    };
    let Some(first) = first else {
        return match chunks.finish().await {
            Err(e) => fetch_failed(e),
            Ok(FetchedKind::HookResults(results)) => hook_results_summary(&results),
            Ok(FetchedKind::InParallel) => {
                SyncResult::new(true, format!("{pipe} is being synced in parallel."))
            }
            Ok(FetchedKind::Data { generator: true }) => {
                SyncResult::new(true, "Received an empty generator; nothing to do.")
            }
            Ok(_) => fail(format!("No data were fetched for {pipe}.")),
        };
    };
    let second = match chunks.next_chunk().await {
        Ok(second) => second,
        Err(e) => return fetch_failed(e),
    };
    if second.is_none() {
        match chunks.finish().await {
            Err(e) => return fetch_failed(e),
            Ok(FetchedKind::Data { generator: false }) => {
                // One DataFrame (or one list of documents): the ordinary flow.
                meerschaum_core::dprint!("Fetched {} rows for {pipe}.", input_rows(&first));
                if matches!(&first, PipeInput::Docs(docs) if docs.is_empty()) {
                    return SyncResult::new(true, format!("No new rows were returned for {pipe}."));
                }
                return Box::pin(sync_inner(env, pipe, Some(first), args, observer)).await;
            }
            Ok(_) => {}
        }
    }
    sync_generator_chunks(env, pipe, first, second, chunks, args, observer).await
}

/// The `(success, message)` results a plugin's `chunk_hook` calls returned: all must have
/// succeeded, the messages are joined (`'\n'.join(...)`).
fn hook_results_summary(results: &[meerschaum_core::SuccessTuple]) -> SyncResult {
    if results.is_empty() {
        return SyncResult::new(true, "No new rows were returned.");
    }
    SyncResult::new(
        results.iter().all(|r| r.success),
        results
            .iter()
            .map(|r| r.message.as_str())
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// v4's chunk-generator branch of `_sync`: the first chunk is synced alone (a failure ends the
/// sync), every other chunk gets three attempts with `attempt ** 2` seconds of sleep after each
/// failure, and the messages are sorted and summarized.
async fn sync_generator_chunks(
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    first: PipeInput,
    second: Option<PipeInput>,
    mut chunks: Box<dyn meerschaum_connector::ChunkStream>,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
) -> SyncResult {
    const MAX_CHUNK_ATTEMPTS: u32 = 3;
    let dt = pipe.column("datetime");
    let mut results: Vec<(bool, String)> = Vec::new();
    let mut aggregate = SyncResult::new(true, String::new());
    let mut upcoming = second;
    let mut current = Some(first);
    while let Some(input) = current.take() {
        let index = results.len();
        let rows = input_rows(&input);
        let (label, info) = match input_frame(&input) {
            Some(frame) => {
                let (begin, end) = fetched_chunk_bounds(&frame, dt.as_deref());
                (
                    fetched_chunk_label(&frame, dt.as_deref()),
                    FetchChunk {
                        index,
                        rows: rows as u64,
                        begin,
                        end,
                    },
                )
            }
            None => (
                String::new(),
                FetchChunk {
                    index,
                    rows: rows as u64,
                    begin: None,
                    end: None,
                },
            ),
        };
        meerschaum_core::dprint!(
            "Fetched chunk {index} of {pipe}: {rows} rows ({}).",
            label.trim()
        );
        if let Some(o) = observer {
            o.chunk_started(&info);
        }
        let started = Instant::now();
        let result = if index == 0 {
            Box::pin(sync_inner(env, pipe, Some(input), args, observer)).await
        } else {
            let mut attempts = 0u32;
            loop {
                let result =
                    Box::pin(sync_inner(env, pipe, Some(input.clone()), args, observer)).await;
                if result.success || env.issued_batch() {
                    break result;
                }
                attempts += 1;
                let sleep_seconds = u64::from(attempts * attempts);
                tracing::warn!(
                    "Failed to sync chunk to {pipe} (attempt {attempts} / {MAX_CHUNK_ATTEMPTS}).\n\
                     Sleeping for {sleep_seconds} second{}:\n{}",
                    if sleep_seconds == 1 { "" } else { "s" },
                    result.message
                );
                tokio::time::sleep(Duration::from_secs(sleep_seconds)).await;
                if attempts >= MAX_CHUNK_ATTEMPTS {
                    break result;
                }
            }
        };
        crate::memory::release_free_memory_throttled(TRIM_INTERVAL);
        if let Some(o) = observer {
            o.chunk_finished(&info, &result, started.elapsed());
        }
        let message = if index == 0 {
            let msg = format!("\n{label}\n{}", result.message);
            if !result.success {
                if env.issued_batch() {
                    let mut partial = result;
                    partial.message = format!("Unable to sync initial chunk for {pipe}:\n{msg}");
                    return partial;
                }
                return fail(format!("Unable to sync initial chunk for {pipe}:\n{msg}"));
            }
            msg
        } else {
            format!(
                "{} a chunk ({} row{}) to {pipe}:\n{label}\n{}",
                if result.success {
                    "Synced"
                } else {
                    "Failed to sync"
                },
                comma_count(rows),
                if rows == 1 { "" } else { "s" },
                result.message
            )
        };
        aggregate.success &= result.success;
        aggregate.inserted += result.inserted;
        aggregate.updated += result.updated;
        aggregate.upserted += result.upserted;
        aggregate.deleted += result.deleted;
        aggregate.begin = aggregate.begin.or(result.begin);
        aggregate.end = result.end;
        aggregate.changed_intervals.extend(result.changed_intervals);
        results.push((result.success, message));
        if !result.success && env.delivery.is_some() {
            break;
        }
        current = match upcoming.take() {
            Some(next) => Some(next),
            None => match chunks.next_chunk().await {
                Ok(next) => next,
                Err(e) => {
                    tracing::warn!(
                        "Failed to fetch data from {}:\n    {e}",
                        pipe.connector_keys()
                    );
                    aggregate.success = false;
                    results.push((false, format!("Failed to fetch the next chunk:\n{e}")));
                    None
                }
            },
        };
    }
    summarize_chunk_results(pipe, results, aggregate)
}

/// `Synced N chunks to <pipe> ...`: v4 sorts the `(success, message)` tuples, which puts failed
/// chunks first.
fn summarize_chunk_results(
    pipe: &Pipe,
    mut results: Vec<(bool, String)>,
    mut aggregate: SyncResult,
) -> SyncResult {
    results.sort();
    let succeeded = results.iter().filter(|r| r.0).count();
    let failed = results.len() - succeeded;
    let messages = results
        .iter()
        .map(|r| r.1.as_str())
        .collect::<Vec<_>>()
        .join("\n\n");
    aggregate.message = format!(
        "Synced {} chunk{} to {pipe}\n({succeeded} succeeded, {failed} failed):\n\n{}",
        comma_count(results.len()),
        if results.len() == 1 { "" } else { "s" },
        messages.trim()
    );
    aggregate
}

/// The smallest and largest datetime-axis values of a chunk, as engine bounds.
fn fetched_chunk_bounds(
    df: &Frame,
    dt: Option<&str>,
) -> (
    Option<meerschaum_core::success::Bound>,
    Option<meerschaum_core::success::Bound>,
) {
    use meerschaum_core::success::Bound;
    let Some(column) = dt.and_then(|c| df.column(c)) else {
        return (None, None);
    };
    let values = column.data.values();
    let mut present = values.iter().filter(|v| !v.is_na()).collect::<Vec<_>>();
    present.sort_by(|a, b| pipe_ops::compare_values(a, b));
    let to_bound = |v: &KValue| match v {
        KValue::Datetime(d) => Some(Bound::Datetime(DateTime::from_naive_utc_and_offset(
            d.utc, Utc,
        ))),
        KValue::Date(d) => d
            .and_hms_opt(0, 0, 0)
            .map(|n| Bound::Datetime(DateTime::from_naive_utc_and_offset(n, Utc))),
        KValue::Int(i) => Some(Bound::Int(*i)),
        // A fetch result before dtype enforcement (SQLite text, ISO strings from a plugin).
        KValue::Str(text) => DateBound::parse(text).and_then(|b| date_bound_to_bound(&b)),
        _ => None,
    };
    (
        present.first().and_then(|v| to_bound(v)),
        present.last().and_then(|v| to_bound(v)),
    )
}

fn fetched_chunk_label(df: &Frame, dt: Option<&str>) -> String {
    let Some(column) = dt.and_then(|c| df.column(c)) else {
        return String::new();
    };
    let values = column.data.values();
    let mut present = values.iter().filter(|v| !v.is_na()).collect::<Vec<_>>();
    present.sort_by(|a, b| pipe_ops::compare_values(a, b));
    match (present.first(), present.last()) {
        (Some(a), Some(b)) => format!("{} - {}", a.py_str(), b.py_str()),
        _ => String::new(),
    }
}

fn comma_count(count: usize) -> String {
    let digits = count.to_string();
    let mut out = String::new();
    for (index, digit) in digits.chars().enumerate() {
        if index > 0 && (digits.len() - index).is_multiple_of(3) {
            out.push(',');
        }
        out.push(digit);
    }
    out
}
