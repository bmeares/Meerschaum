//! The source side of `sql:` pipes (`meerschaum/connectors/sql/_fetch.py`): `fetch` executes the
//! metadefinition built by [`crate::queries::metadef::get_pipe_metadef`] on this connector.

use arrow::array::RecordBatch;
use indexmap::IndexMap;
use meerschaum_connector::{
    ChunkStream, ConnectorResult, DateBound, FetchedKind, InstanceConnector,
};
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeInput;

use crate::flavor::Flavor;

use crate::queries::metadef::{
    Bound, MetadefArgs, ParentSpec, get_pipe_metadef_for_execution as get_pipe_metadef,
    get_pipe_metadef_for_sync,
};

use super::values::py_datetime;
use super::{SqlConnector, be};

/// The arguments of `fetch(pipe, begin, end, check_existing, params)`.
#[derive(Debug, Clone)]
pub struct FetchArgs {
    /// `begin` (`Bound::Default` is Python's `''`: the sync time minus the backtrack window).
    pub begin: Bound,
    /// `end`.
    pub end: Bound,
    /// `check_existing` (`False` disables the backtrack window).
    pub check_existing: bool,
    /// `params`.
    pub params: Option<IndexMap<String, Value>>,
    /// `pipe.get_sync_time()` on the pipe's instance (used when `begin` is the default).
    pub sync_time: Option<DateBound>,
    /// `pipe.parent`, for predicate pushdown.
    pub parent: Option<ParentSpec>,
}

impl Default for FetchArgs {
    fn default() -> Self {
        FetchArgs {
            begin: Bound::Default,
            end: Bound::None,
            check_existing: true,
            params: None,
            sync_time: None,
            parent: None,
        }
    }
}

/// A connector bound as a metadefinition bound.
pub fn metadef_bound(b: Option<&DateBound>) -> Bound {
    match b {
        None => Bound::None,
        Some(DateBound::Int(i)) => Bound::Int(*i),
        Some(DateBound::Datetime(dt)) => Bound::DateTime(py_datetime(dt)),
    }
}

impl SqlConnector {
    /// Resolve the same first parent the Python facade supplies to the query builder.
    pub(crate) async fn resolve_fetch_parent(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<Option<ParentSpec>> {
        let Some(mut parent) = pipe.parent(&self.settings.pipe_defaults).map_err(be)? else {
            return Ok(None);
        };
        let provider = self.instance_provider();
        let external = if parent.instance_keys() == self.keys() {
            None
        } else {
            provider.instance(parent.instance_keys())
        };
        let instance: &dyn InstanceConnector = if parent.instance_keys() == self.keys() {
            self
        } else if let Some(instance) = external.as_deref() {
            instance
        } else {
            return Ok(None);
        };
        let attributes = instance.get_pipe_attributes(&parent).await?;
        let registered = attributes
            .get("parameters")
            .map(Value::to_map)
            .unwrap_or_default();
        parent.set_parameters(meerschaum_core::value::apply_patch_maps(
            &registered,
            parent.raw_parameters(),
        ));
        if meerschaum_connector::template::needs_instance_resolution(&parent) {
            let (resolved, warnings) = meerschaum_connector::resolve_pipe(
                &parent,
                instance,
                Some(provider.as_ref()),
                &self.settings.pipe_defaults,
            )
            .await;
            for warning in warnings {
                tracing::warn!("{warning}");
            }
            parent = resolved;
        }
        let dt_col = parent.column("datetime");
        let dt_typ = dt_col
            .as_ref()
            .and_then(|column| parent.dtypes().get(column).map(Value::py_str));
        Ok(Some(ParentSpec {
            target: parent.target(None).map_err(be)?,
            schema: instance.pipe_table_schema(&parent),
            dt_col,
            dt_typ,
            precision_unit: parent
                .precision(&self.settings.pipe_defaults)
                .map_err(be)?
                .unit
                .name()
                .into(),
        }))
    }

    /// `get_pipe_metadef(pipe, params, begin, end, check_existing)`. Uses the pipe's parameters as
    /// resolved so far; [`SqlConnector::prepare_pipe`] first for cross-pipe templates.
    pub fn get_pipe_metadef(
        &self,
        pipe: &Pipe,
        args: &FetchArgs,
    ) -> ConnectorResult<Option<String>> {
        let sp = self.sql_pipe(pipe)?;
        let mut m = MetadefArgs::new(&sp);
        m.params = args.params.as_ref();
        m.begin = args.begin.clone();
        m.end = args.end.clone();
        m.check_existing = args.check_existing;
        m.sync_time = metadef_bound(args.sync_time.as_ref());
        m.parent = args.parent.clone();
        get_pipe_metadef(self.flavor, &m).map_err(be)
    }

    /// The query a sync runs for [`get_pipe_metadef`](SqlConnector::get_pipe_metadef): the same
    /// definition and bounds, with SQLite datetimes compared in their canonical text form and
    /// null-like `params` strings matched as text on declared text / index columns (rc15).
    pub fn get_pipe_metadef_for_sync(
        &self,
        pipe: &Pipe,
        args: &FetchArgs,
    ) -> ConnectorResult<Option<String>> {
        let sp = self.sql_pipe(pipe)?;
        let mut m = MetadefArgs::new(&sp);
        m.params = args.params.as_ref();
        m.begin = args.begin.clone();
        m.end = args.end.clone();
        m.check_existing = args.check_existing;
        m.sync_time = metadef_bound(args.sync_time.as_ref());
        m.parent = args.parent.clone();
        get_pipe_metadef_for_sync(self.flavor, &m).map_err(be)
    }

    /// The query a fetch runs: cross-pipe templates resolved, the parent found, the sync
    /// metadefinition built.
    async fn fetch_query(&self, pipe: &Pipe, args: &FetchArgs) -> ConnectorResult<String> {
        // Resolve cross-pipe templates (`{{ Pipe() }}` in the SQL definition) first.
        let pipe = self.prepare_pipe(pipe).await;
        let mut args = args.clone();
        if args.parent.is_none() {
            args.parent = self.resolve_fetch_parent(&pipe).await?;
        }
        self.get_pipe_metadef_for_sync(&pipe, &args)?.ok_or_else(|| {
            be(format!(
                "Could not determine a SQL definition for {pipe}.\n    Set the key `query` in `pipe.parameters` to a valid SQL query."
            ))
        })
    }

    /// `fetch(pipe, begin, end, check_existing)`: execute the pipe's SQL definition on this
    /// (source) connector and return the rows.
    pub async fn fetch(&self, pipe: &Pipe, args: &FetchArgs) -> ConnectorResult<Option<Frame>> {
        let meta_def = self.fetch_query(pipe, args).await?;
        Ok(Some(self.read(&meta_def).await.map_err(be)?))
    }

    /// Whether [`fetch_stream`](SqlConnector::fetch_stream) streams on this flavor. Oracle reads
    /// whole: an unconstrained `NUMBER` is typed from every value of a collected read, which a
    /// stream (one schema, fixed by its first batch) cannot do.
    pub fn streams_fetch(&self) -> bool {
        self.flavor != Flavor::Oracle
    }

    /// [`fetch`](SqlConnector::fetch) as a stream of frames of `chunksize` rows: the rows of the
    /// collected read, in the same order and with the same types, read while the caller syncs
    /// the earlier chunks. `None` where the flavor cannot stream
    /// ([`streams_fetch`](SqlConnector::streams_fetch)).
    pub async fn fetch_stream(
        &self,
        pipe: &Pipe,
        args: &FetchArgs,
        chunksize: usize,
    ) -> ConnectorResult<Option<FetchStream>> {
        if !self.streams_fetch() {
            return Ok(None);
        }
        let meta_def = self.fetch_query(pipe, args).await?;
        self.log(&meta_def);
        let batches = self
            .driver
            .read_stream_batches(&meta_def)
            .await
            .map_err(be)?;
        Ok(Some(FetchStream::new(batches, chunksize)))
    }

    /// The storage a reader held open on this connector would lock against writers
    /// ([`Connector::exclusive_storage`](meerschaum_connector::Connector::exclusive_storage)):
    /// the file of an SQLite, GeoPackage or DuckDB database (an in-memory database is this
    /// connector's own), the database of an MSSQL server (locking reads by default), or this
    /// connector's own pool when it has a single connection.
    pub fn exclusive_storage(&self) -> Option<String> {
        let spec = &self.spec;
        match self.flavor {
            Flavor::Sqlite | Flavor::Geopackage | Flavor::Duckdb => {
                if spec.is_memory() {
                    return Some(format!("memory:{}", self.label));
                }
                let database = spec.database.clone()?;
                let path = std::fs::canonicalize(&database)
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or(database);
                Some(format!("file:{path}"))
            }
            Flavor::Mssql => Some(format!(
                "mssql://{}:{}/{}",
                spec.host.as_deref().unwrap_or("localhost").to_lowercase(),
                spec.port.unwrap_or(1433),
                spec.database.as_deref().unwrap_or_default()
            )),
            // One pooled connection: a read held open leaves none for this connector's writes.
            _ if spec.max_connections() <= 1 => Some(format!("pool:{}", self.keys())),
            _ => None,
        }
    }
}

/// The fetched rows of [`SqlConnector::fetch_stream`]: the driver's batches re-cut into frames
/// of exactly `chunksize` rows (the last one shorter), so the chunks are the row ranges a
/// collected read split by `chunksize` gives. An empty result is one empty frame with the
/// result's columns.
pub struct FetchStream {
    batches: crate::driver::RecordBatchStream,
    chunksize: usize,
    pending: std::collections::VecDeque<RecordBatch>,
    pending_rows: usize,
    emitted: bool,
    done: bool,
}

impl FetchStream {
    /// Re-cut `batches` into chunks of `chunksize` rows.
    pub fn new(batches: crate::driver::RecordBatchStream, chunksize: usize) -> Self {
        FetchStream {
            batches,
            chunksize: chunksize.max(1),
            pending: Default::default(),
            pending_rows: 0,
            emitted: false,
            done: false,
        }
    }

    /// The next chunk, or `None` after the last one. A read that fails mid-stream is `Err`.
    pub async fn next_frame(&mut self) -> crate::error::Result<Option<Frame>> {
        use futures_util::StreamExt;
        loop {
            if self.pending_rows >= self.chunksize {
                return self.emit(self.chunksize).map(Some);
            }
            if self.done {
                if self.pending_rows > 0 {
                    return self.emit(self.pending_rows).map(Some);
                }
                if !self.emitted {
                    self.emitted = true;
                    let empty = RecordBatch::new_empty(self.batches.schema());
                    return crate::driver::batches_to_frame(&[empty]).map(Some);
                }
                return Ok(None);
            }
            match self.batches.next().await {
                Some(Ok(batch)) => {
                    if batch.num_rows() > 0 {
                        self.pending_rows += batch.num_rows();
                        self.pending.push_back(batch);
                    }
                }
                Some(Err(e)) => {
                    self.done = true;
                    self.pending.clear();
                    self.pending_rows = 0;
                    self.emitted = true;
                    return Err(e);
                }
                None => self.done = true,
            }
        }
    }

    /// Take the first `n` pending rows as one frame.
    fn emit(&mut self, n: usize) -> crate::error::Result<Frame> {
        let mut parts = Vec::new();
        let mut needed = n;
        while needed > 0 {
            let batch = self
                .pending
                .pop_front()
                .expect("pending rows cover the chunk");
            let rows = batch.num_rows();
            if rows <= needed {
                needed -= rows;
                parts.push(batch);
            } else {
                parts.push(batch.slice(0, needed));
                self.pending.push_front(batch.slice(needed, rows - needed));
                needed = 0;
            }
        }
        self.pending_rows -= n;
        self.emitted = true;
        crate::driver::batches_to_frame(&parts)
    }
}

#[meerschaum_connector::async_trait]
impl ChunkStream for FetchStream {
    async fn next_chunk(&mut self) -> ConnectorResult<Option<PipeInput>> {
        Ok(self.next_frame().await.map_err(be)?.map(PipeInput::Frame))
    }

    async fn finish(&mut self) -> ConnectorResult<FetchedKind> {
        Ok(FetchedKind::Data { generator: true })
    }
}
