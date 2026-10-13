//! The source side of `sql:` pipes (`meerschaum/connectors/sql/_fetch.py`): `fetch` executes the
//! metadefinition built by [`crate::queries::metadef::get_pipe_metadef`] on this connector.

use indexmap::IndexMap;
use meerschaum_connector::{ConnectorResult, DateBound};
use meerschaum_core::{Pipe, Value};
use meerschaum_kernel::Frame;

use crate::queries::metadef::{
    Bound, MetadefArgs, ParentSpec, get_pipe_metadef_for_execution as get_pipe_metadef,
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
    /// `get_pipe_metadef(pipe, params, begin, end, check_existing)`.
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

    /// `fetch(pipe, begin, end, check_existing)`: execute the pipe's SQL definition on this
    /// (source) connector and return the rows.
    pub async fn fetch(&self, pipe: &Pipe, args: &FetchArgs) -> ConnectorResult<Option<Frame>> {
        let Some(meta_def) = self.get_pipe_metadef(pipe, args)? else {
            return Err(be(format!(
                "Could not determine a SQL definition for {pipe}.\n    Set the key `query` in `pipe.parameters` to a valid SQL query."
            )));
        };
        Ok(Some(self.read(&meta_def).await.map_err(be)?))
    }
}
