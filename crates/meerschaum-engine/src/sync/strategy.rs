//! Source query planning for immutable time-series pipes.

use std::sync::Arc;

use meerschaum_connector::{ConnectorResult, CpiPolynomials};

use chrono::Datelike;

use super::*;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) enum Strategy {
    #[default]
    Backtrack,
    Simple,
    Naive,
    SlowId,
    Append,
    Join,
    IterativeSimple {
        unbounded: bool,
    },
    DailyRowCount {
        unbounded: bool,
    },
    BinarySearch {
        unbounded: bool,
    },
    IterativeCpi {
        unbounded: bool,
    },
    Monthly(Correction),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Correction {
    Naive,
    IterativeSimple,
    DailyRowCount,
    BinarySearch,
    IterativeCpi,
}

impl Correction {
    fn unbounded(self) -> Strategy {
        match self {
            Self::Naive => Strategy::Naive,
            Self::IterativeSimple => Strategy::IterativeSimple { unbounded: true },
            Self::DailyRowCount => Strategy::DailyRowCount { unbounded: true },
            Self::BinarySearch => Strategy::BinarySearch { unbounded: true },
            Self::IterativeCpi => Strategy::IterativeCpi { unbounded: true },
        }
    }
}

impl Strategy {
    pub(super) fn read(pipe: &Pipe) -> ConnectorResult<Self> {
        let Some(sync) = pipe.param("sync") else {
            return Ok(Self::Backtrack);
        };
        let name = match sync.get("strategy") {
            None => return Ok(Self::Backtrack),
            Some(Value::Str(name)) => name.as_str(),
            Some(_) => return Err(invalid("sync.strategy must be a string.")),
        };
        let strategy = match name {
            "backtrack" => Self::Backtrack,
            "simple" => Self::Simple,
            "naive" => Self::Naive,
            "slow-id" => Self::SlowId,
            "append" => Self::Append,
            "join" => Self::Join,
            "iterative-simple" => Self::IterativeSimple { unbounded: false },
            "unbounded-simple" | "unbounded-iterative-simple" => {
                Self::IterativeSimple { unbounded: true }
            }
            "daily-row-count" => Self::DailyRowCount { unbounded: false },
            "unbounded-daily-row-count" => Self::DailyRowCount { unbounded: true },
            "binary-search" => Self::BinarySearch { unbounded: false },
            "unbounded-binary-search" => Self::BinarySearch { unbounded: true },
            "iterative-cpisync" => Self::IterativeCpi { unbounded: false },
            "unbounded-iterative-cpisync" => Self::IterativeCpi { unbounded: true },
            "simple-monthly-naive" => Self::Monthly(Correction::Naive),
            "simple-monthly-iterative-simple" => Self::Monthly(Correction::IterativeSimple),
            "simple-monthly-daily-row-count" => Self::Monthly(Correction::DailyRowCount),
            "simple-monthly-binary-search" => Self::Monthly(Correction::BinarySearch),
            "simple-monthly-iterative-cpisync" => Self::Monthly(Correction::IterativeCpi),
            _ => return Err(invalid(format!("Unknown sync.strategy '{name}'."))),
        };
        if strategy != Self::Backtrack && !matches!(sync.get("immutable"), Some(Value::Bool(true)))
        {
            return Err(invalid(format!(
                "sync.strategy '{name}' requires sync.immutable: true."
            )));
        }
        Ok(strategy)
    }

    pub(super) fn is_planned(self) -> bool {
        !matches!(self, Self::Backtrack | Self::Simple | Self::Naive)
    }
}

fn invalid(message: impl Into<String>) -> ConnectorError {
    ConnectorError::Invalid(message.into())
}

/// Explicit fetch windows always keep their original semantics. An advanced strategy gets to
/// select windows only for an ordinary automatic sync.
pub(super) fn automatic(args: &SyncArgs) -> bool {
    args.begin == SyncBegin::Default && args.end.is_none()
}

const DAY_SECONDS: i64 = 86_400;
const MAX_PROBES: usize = 4_096;

fn interval_setting(pipe: &Pipe, key: &str, default: i64) -> ConnectorResult<i64> {
    match pipe.param("sync").and_then(|sync| sync.get(key)) {
        None => Ok(default),
        Some(Value::Int(n)) if *n > 0 => Ok(*n),
        _ => Err(invalid(format!("sync.{key} must be a positive integer."))),
    }
}

fn day(bound: &DateBound) -> ConnectorResult<i64> {
    match bound {
        DateBound::Datetime(dt) => Ok(dt.utc.and_utc().timestamp().div_euclid(DAY_SECONDS)),
        DateBound::Int(_) => Err(invalid(
            "Corrective sync strategies require a timestamp axis.",
        )),
    }
}

fn midnight(day: i64) -> ConnectorResult<DateBound> {
    day.checked_mul(DAY_SECONDS)
        .and_then(|seconds| DateTime::from_timestamp(seconds, 0))
        .map(DateBound::utc)
        .ok_or_else(|| invalid("The corrective sync window exceeds the timestamp range."))
}

pub(super) async fn run(
    strategy: Strategy,
    env: &SyncEnv<'_>,
    pipe: &Pipe,
    source: Arc<dyn Connector>,
    instance: &dyn InstanceConnector,
    args: &SyncArgs,
    observer: Option<&dyn DeltaObserver>,
) -> SyncResult {
    let query = DataQuery {
        params: args.params.clone(),
        ..Default::default()
    };
    let mut checkpoint = None;
    let planned = match strategy {
        Strategy::Monthly(correction) => monthly_plan(
            pipe,
            source.as_ref(),
            instance,
            query.clone(),
            args,
            correction,
        )
        .await
        .map(|(plan, pending)| {
            checkpoint = pending;
            plan
        }),
        _ => plan(strategy, pipe, source.as_ref(), instance, query.clone()).await,
    };
    let plan = match planned {
        Ok(plan) => plan,
        Err(error) => return fetch_failed(pipe, &source.connector_keys(), &error),
    };
    let may_stream = !shares_exclusive_storage(source.as_ref(), instance);
    let chunksize = args.chunksize.map(|size| {
        if size > 0 {
            size as usize
        } else {
            env.registry.fetch_chunksize(pipe.connector_keys()).max(1)
        }
    });
    let chunks: Box<dyn ChunkStream> = match plan {
        FetchPlan::Windows(windows) => Box::new(WindowStream {
            pipe: pipe.clone(),
            source,
            windows: windows.into_iter(),
            chunksize,
            may_stream,
            stream: None,
        }),
        FetchPlan::Watermarks { watermarks, joined } => {
            let frame = match source
                .immutable_sync_fetch_by_id(pipe, &query, &watermarks, joined)
                .await
            {
                Ok(frame) => frame.unwrap_or_default(),
                Err(error) => return fetch_failed(pipe, &source.connector_keys(), &error),
            };
            let size = chunksize.unwrap_or_else(|| frame.height().max(1));
            Box::new(FrameChunks::new(frame, size))
        }
    };
    let mut result = sync_fetched_chunks(env, pipe, chunks, args, observer).await;
    if result.success
        && let Some(checkpoint) = checkpoint
        && let Err(error) = checkpoint.save(instance).await
    {
        result.success = false;
        result.message.push_str(&format!(
            "\nThe monthly correction completed, but its checkpoint could not be saved: {error}"
        ));
    }
    result
}

async fn plan(
    strategy: Strategy,
    pipe: &Pipe,
    source: &dyn Connector,
    instance: &dyn InstanceConnector,
    query: DataQuery,
) -> ConnectorResult<FetchPlan> {
    if pipe.column("datetime").is_none() {
        return Err(invalid("This sync strategy requires a datetime column."));
    }
    if matches!(
        strategy,
        Strategy::SlowId | Strategy::Append | Strategy::Join
    ) && pipe.column("id").is_none()
    {
        return Err(invalid("This sync strategy requires an id column."));
    }
    if !instance.pipe_exists_checked(pipe).await? {
        return Ok(FetchPlan::Windows(vec![query.into()]));
    }
    match strategy {
        Strategy::SlowId | Strategy::Append | Strategy::Join => {
            plan_ids(strategy, pipe, instance, query).await
        }
        Strategy::IterativeSimple { unbounded }
        | Strategy::DailyRowCount { unbounded }
        | Strategy::BinarySearch { unbounded }
        | Strategy::IterativeCpi { unbounded } => {
            let windows =
                plan_corrective(strategy, unbounded, pipe, source, instance, query).await?;
            Ok(FetchPlan::Windows(windows))
        }
        _ => unreachable!("strategies with a single ordinary fetch do not need a plan"),
    }
}

struct FetchWindow {
    query: DataQuery,
    cpi: Option<(CpiPolynomials, usize)>,
}

impl From<DataQuery> for FetchWindow {
    fn from(query: DataQuery) -> Self {
        Self { query, cpi: None }
    }
}

enum FetchPlan {
    Windows(Vec<FetchWindow>),
    Watermarks {
        watermarks: Vec<(String, DateBound)>,
        joined: bool,
    },
}

async fn plan_ids(
    strategy: Strategy,
    pipe: &Pipe,
    instance: &dyn InstanceConnector,
    query: DataQuery,
) -> ConnectorResult<FetchPlan> {
    let watermarks = instance.immutable_sync_watermarks(pipe, &query).await?;
    if watermarks.is_empty() {
        return Ok(FetchPlan::Windows(vec![query.into()]));
    }
    if strategy == Strategy::SlowId {
        // Include newly seen IDs, even when their earliest rows are older than the slowest
        // existing ID. The source applies the same lower bound to all known IDs.
        let mut watermarks = watermarks;
        let oldest = watermarks
            .iter()
            .map(|(_, bound)| bound)
            .min_by_key(|bound| match bound {
                DateBound::Int(n) => i128::from(*n),
                DateBound::Datetime(dt) => {
                    let dt = dt.utc.and_utc();
                    i128::from(dt.timestamp()) * 1_000_000_000
                        + i128::from(dt.timestamp_subsec_nanos())
                }
            })
            .cloned()
            .expect("nonempty watermarks");
        for (_, bound) in &mut watermarks {
            *bound = oldest.clone();
        }
        return Ok(FetchPlan::Watermarks {
            watermarks,
            joined: true,
        });
    }
    Ok(FetchPlan::Watermarks {
        watermarks,
        joined: strategy == Strategy::Join,
    })
}

async fn plan_corrective(
    strategy: Strategy,
    unbounded: bool,
    pipe: &Pipe,
    source: &dyn Connector,
    instance: &dyn InstanceConnector,
    query: DataQuery,
) -> ConnectorResult<Vec<FetchWindow>> {
    let lookback = interval_setting(pipe, "lookback_days", 30)?;
    let Some(newest) = source
        .get_sync_time_remote(pipe, query.params.as_ref(), true)
        .await?
    else {
        return Ok(Vec::new());
    };
    let end = day(&newest)? + 1;
    let Some(oldest) = source
        .get_sync_time_remote(pipe, query.params.as_ref(), false)
        .await?
    else {
        return Err(invalid(
            "The source's oldest timestamp could not be determined.",
        ));
    };
    let oldest = day(&oldest)?;
    let begin = if unbounded {
        oldest
    } else {
        oldest.max(end.saturating_sub(lookback))
    };
    if begin >= end {
        return Ok(Vec::new());
    }
    if matches!(strategy, Strategy::DailyRowCount { .. }) {
        let mut scope = query;
        scope.begin = Some(midnight(begin)?);
        scope.end = Some(midnight(end)?);
        let source_counts = source
            .immutable_sync_remote_daily_counts(pipe, &scope)
            .await?;
        let target_counts = instance.immutable_sync_daily_counts(pipe, &scope).await?;
        if target_counts
            .iter()
            .any(|(day, count)| source_counts.get(day).unwrap_or(&0) < count)
        {
            return Err(invalid(
                "The target has more rows than the source on a corrective day. Check the immutable source contract.",
            ));
        }
        return source_counts
            .into_iter()
            .filter(|(day, count)| target_counts.get(day) != Some(count))
            .map(|(day, _)| window(&scope, day, day + 1).map(FetchWindow::from))
            .collect();
    }

    let linear =
        matches!(strategy, Strategy::IterativeSimple { .. }) && end - begin <= MAX_PROBES as i64;
    // When isolated timestamps give iterative-simple a very large extent, prune equal
    // ranges first. This avoids making a query for every empty day without dropping old rows.
    let mut pending = if linear {
        (begin..end).rev().map(|day| (day, day + 1)).collect()
    } else {
        vec![(begin, end)]
    };
    let mut windows = Vec::new();
    let mut probes = 0;
    while let Some((lo, hi)) = pending.pop() {
        let candidate = window(&query, lo, hi)?;
        if probes == MAX_PROBES {
            // Fetch the remaining spans without further probes. They are disjoint from
            // the windows that were previously accepted.
            windows.push(candidate.into());
            continue;
        }
        probes += 1;
        let source_count = source
            .get_rowcount_remote(pipe, &candidate)
            .await?
            .ok_or_else(|| invalid("The source did not return an exact row count."))?;
        let target_count = instance
            .get_pipe_rowcount(pipe, &candidate)
            .await?
            .ok_or_else(|| invalid("The target did not return an exact row count."))?;
        if source_count == target_count {
            continue;
        }
        if source_count < target_count {
            return Err(invalid(
                "The target has more rows than the source in a corrective window. Check the immutable source contract.",
            ));
        }
        if hi - lo == 1 || target_count == 0 {
            let mut fetch = FetchWindow::from(candidate);
            let missing = source_count - target_count;
            if matches!(strategy, Strategy::IterativeCpi { .. })
                && target_count > 0
                && source_count <= 1024
                && (1..=16).contains(&missing)
            {
                let points = missing + 1;
                let src = source
                    .immutable_sync_remote_cpi(pipe, &fetch.query, points)
                    .await?;
                let dst = instance
                    .immutable_sync_target_cpi(pipe, &fetch.query, points)
                    .await?;
                fetch.cpi =
                    super::cpi::recover(&src, &dst, source_count, target_count, points as usize)
                        .map(|polynomials| (polynomials, missing as usize));
            }
            windows.push(fetch);
        } else {
            let mid = lo + (hi - lo) / 2;
            pending.push((mid, hi));
            pending.push((lo, mid));
        }
    }
    Ok(windows)
}

fn window(query: &DataQuery, lo: i64, hi: i64) -> ConnectorResult<DataQuery> {
    Ok(DataQuery {
        begin: Some(midnight(lo)?),
        end: Some(midnight(hi)?),
        ..query.clone()
    })
}

struct WindowStream {
    pipe: Pipe,
    source: Arc<dyn Connector>,
    windows: std::vec::IntoIter<FetchWindow>,
    chunksize: Option<usize>,
    may_stream: bool,
    stream: Option<Box<dyn ChunkStream>>,
}

#[async_trait]
impl ChunkStream for WindowStream {
    async fn next_chunk(&mut self) -> ConnectorResult<Option<PipeInput>> {
        loop {
            if let Some(stream) = &mut self.stream {
                if let Some(chunk) = stream.next_chunk().await? {
                    return Ok(Some(chunk));
                }
                self.stream = None;
            }
            let Some(FetchWindow { query, cpi }) = self.windows.next() else {
                return Ok(None);
            };
            if let Some((polynomials, expected)) = cpi {
                let frame = self
                    .source
                    .immutable_sync_fetch_cpi(&self.pipe, &query, &polynomials)
                    .await?;
                if frame.height() == expected {
                    self.stream = Some(Box::new(FrameChunks::new(
                        frame,
                        self.chunksize.unwrap_or(expected),
                    )));
                    continue;
                }
                // Source changes between the metadata read and the fetch can invalidate
                // the polynomial. Re-read the window with the ordinary keyed merge.
            }
            if self.may_stream
                && let Some(size) = self.chunksize
                && let Some(stream) = self.source.fetch_stream(&self.pipe, &query, size).await?
            {
                self.stream = Some(stream);
                continue;
            }
            if let Some(frame) = self.source.fetch(&self.pipe, &query).await?
                && !frame.is_empty()
            {
                let size = self.chunksize.unwrap_or_else(|| frame.height());
                self.stream = Some(Box::new(FrameChunks::new(frame, size)));
            }
        }
    }
}

const CORRECTION_METRIC: &str = "engine_sync_corrections";

struct Checkpoint {
    state: Pipe,
    key: String,
    pipe_ref: String,
}

impl Checkpoint {
    async fn saved(&self, instance: &dyn InstanceConnector) -> ConnectorResult<bool> {
        if !instance.pipe_exists_checked(&self.state).await? {
            return Ok(false);
        }
        let mut params = Map::new();
        params.insert("checkpoint".into(), Value::Str(self.key.clone()));
        instance
            .pipe_has_rows_checked(&self.state, Some(&params))
            .await
    }

    async fn save(self, instance: &dyn InstanceConnector) -> ConnectorResult<()> {
        let row = IndexMap::from([
            ("checkpoint".into(), KValue::Str(self.key)),
            ("pipe_ref".into(), KValue::Str(self.pipe_ref)),
        ]);
        let frame =
            meerschaum_kernel::docs::frame_from_docs(&[row]).map_err(|e| invalid(e.to_string()))?;
        let result = instance
            .sync_pipe(
                &self.state,
                frame,
                &SyncOptions {
                    check_existing: false,
                    ..Default::default()
                },
            )
            .await;
        if !result.success {
            return Err(ConnectorError::Backend(result.message));
        }
        Ok(())
    }
}

async fn monthly_plan(
    pipe: &Pipe,
    source: &dyn Connector,
    instance: &dyn InstanceConnector,
    query: DataQuery,
    args: &SyncArgs,
    correction: Correction,
) -> ConnectorResult<(FetchPlan, Option<Checkpoint>)> {
    if instance.connector_type() != "sql"
        || pipe.temporary
        || query.params.as_ref().is_some_and(|p| !p.is_empty())
    {
        return Err(invalid(
            "Monthly corrective sync requires an unfiltered, registered pipe on a direct SQL instance.",
        ));
    }
    if pipe.column("datetime").is_none() {
        return Err(invalid(
            "Monthly corrective sync requires a datetime column.",
        ));
    }
    let state = instance
        .engine_state_pipe(pipe.instance_keys(), CORRECTION_METRIC)
        .await?
        .ok_or_else(|| invalid("The instance cannot store monthly sync checkpoints."))?;
    let pipe_ref = pipe.pipe_ref();
    let at = args.sync_timestamp;
    let checkpoint = Checkpoint {
        state,
        key: format!("{pipe_ref}:{}:{:02}:{correction:?}", at.year(), at.month()),
        pipe_ref,
    };
    let newest = instance.get_sync_time(pipe, None, true).await?;
    if !checkpoint.saved(instance).await? {
        let windows = if correction == Correction::Naive || newest.is_none() {
            vec![query.into()]
        } else {
            plan_corrective(correction.unbounded(), true, pipe, source, instance, query).await?
        };
        return Ok((FetchPlan::Windows(windows), Some(checkpoint)));
    }
    let simple = DataQuery {
        begin: newest,
        ..query
    };
    Ok((FetchPlan::Windows(vec![simple.into()]), None))
}
