//! The source side of `sql:` pipes (`meerschaum/connectors/sql/_fetch.py`): `fetch` executes the
//! metadefinition built by [`crate::queries::metadef::get_pipe_metadef`] on this connector.

use indexmap::IndexMap;
use meerschaum_connector::{ConnectorResult, DateBound, InstanceConnector};
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::Frame;

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

    /// `fetch(pipe, begin, end, check_existing)`: execute the pipe's SQL definition on this
    /// (source) connector and return the rows.
    pub async fn fetch(&self, pipe: &Pipe, args: &FetchArgs) -> ConnectorResult<Option<Frame>> {
        // Resolve cross-pipe templates (`{{ Pipe() }}` in the SQL definition) first.
        let pipe = self.prepare_pipe(pipe).await;
        let mut args = args.clone();
        if args.parent.is_none() {
            args.parent = self.resolve_fetch_parent(&pipe).await?;
        }
        let Some(meta_def) = self.get_pipe_metadef_for_sync(&pipe, &args)? else {
            return Err(be(format!(
                "Could not determine a SQL definition for {pipe}.\n    Set the key `query` in `pipe.parameters` to a valid SQL query."
            )));
        };
        Ok(Some(self.read(&meta_def).await.map_err(be)?))
    }
}
