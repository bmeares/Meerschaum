//! Reading and removing pipe data (`_pipes.py`: `get_pipe_data`, `get_pipe_data_query`,
//! `get_sync_time`, `pipe_exists`, `get_pipe_rowcount`, `drop_pipe`, `clear_pipe`,
//! `deduplicate_pipe`, `get_pipe_columns_types`, `get_pipe_columns_indices`), plus the
//! `Pipe`-level wrappers the façade needs (`get_data(as_iterator=True)` chunking by
//! `get_chunk_bounds`, `get_backtrack_data`, `parse_date_bounds`).

use chrono::Duration;
use indexmap::IndexMap;
use meerschaum_connector::{ConnectorResult, DataQuery, DateBound};
use meerschaum_core::dtypes::{are_dtypes_equal, get_pd_type_from_db_type};
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::{
    PipeDtypesContext, PipeInput, pipe_enforce_dtypes, pipe_get_dtypes, pipe_is_tz_aware,
};
use meerschaum_kernel::value::DateTimeValue;

use crate::dateadd::{DateArg, Number, dateadd_for_execution as dateadd_str};
use crate::dtypes::get_db_type_from_pd_type;
use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use crate::pipe::{ChunkInterval, SqlPipe, str_map};
use crate::queries::cte::{ColsToSelect, format_cte_subquery, wrap_query_with_cte};
use crate::queries::metadef::get_pipe_query;
use crate::queries::table::{IndexDoc, get_create_table_query, get_rename_table_queries};
use crate::where_clause::build_where;

use super::values::{coerce_bound, date_arg, sync_time_from_sql};
use super::{SqlConnector, be, reflect};

/// What `Pipe` reads from the instance while building queries: the table's columns and the
/// pipe's dtypes with and without inference.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PipeState {
    /// `pipe.exists()`.
    pub exists: bool,
    /// `pipe.get_columns_types()`.
    pub cols_types: IndexMap<String, String>,
    /// `pipe.get_dtypes(infer=True)` (`pipe.dtypes`).
    pub dtypes: IndexMap<String, String>,
    /// `pipe.get_dtypes(infer=False)`.
    pub explicit_dtypes: IndexMap<String, String>,
}

/// `Pipe.infer_dtypes()`: database types as pandas dtypes.
pub fn infer_dtypes(cols_types: &IndexMap<String, String>) -> IndexMap<String, String> {
    cols_types
        .iter()
        .map(|(c, t)| {
            let pd = if meerschaum_core::pyfmt::py_isupper(t) {
                get_pd_type_from_db_type(t, true)
            } else {
                meerschaum_kernel::dtypes::to_pandas_dtype(t)
            };
            (c.clone(), pd)
        })
        .collect()
}

/// `pipe.columns` as a string map (falsy values dropped).
pub fn columns_map(pipe: &Pipe) -> IndexMap<String, String> {
    str_map(&pipe.columns())
        .into_iter()
        .filter(|(_, c)| !c.is_empty() && c != "None")
        .collect()
}

/// The configured dtypes (`parameters['dtypes']`, role keys resolved).
pub fn configured_dtypes(pipe: &Pipe) -> IndexMap<String, String> {
    pipe.dtypes()
        .iter()
        .filter(|(_, v)| !v.is_null())
        .map(|(k, v)| (k.clone(), v.py_str()))
        .collect()
}

/// `Pipe.get_dtypes(infer)` given the table's columns types.
pub fn pipe_dtypes(
    pipe: &Pipe,
    cols_types: &IndexMap<String, String>,
    infer: bool,
) -> IndexMap<String, String> {
    let remote = if infer {
        infer_dtypes(cols_types)
    } else {
        IndexMap::new()
    };
    pipe_get_dtypes(
        &configured_dtypes(pipe),
        &remote,
        &columns_map(pipe),
        pipe.autoincrement(),
    )
}

/// `pipe.guess_datetime()` over `pipe.dtypes`.
pub fn guess_datetime(pipe: &Pipe, dtypes: &IndexMap<String, String>) -> Option<String> {
    if let Some(Value::Null) = pipe.dtypes().get("datetime") {
        return None;
    }
    dtypes
        .iter()
        .find(|(_, t)| are_dtypes_equal(t, "datetime"))
        .map(|(c, _)| c.clone())
}

/// A bound's position in microseconds (datetimes) or raw units (integers).
fn bound_pos(b: &DateBound) -> i64 {
    match b {
        DateBound::Int(i) => *i,
        DateBound::Datetime(dt) => dt.utc.and_utc().timestamp_micros(),
    }
}

fn bound_from_pos(pos: i64, template: &DateBound) -> DateBound {
    match template {
        DateBound::Int(_) => DateBound::Int(pos),
        DateBound::Datetime(dt) => DateBound::Datetime(DateTimeValue {
            utc: chrono::DateTime::from_timestamp_micros(pos)
                .map(|d| d.naive_utc())
                .unwrap_or(dt.utc),
            tz: dt.tz.clone(),
        }),
    }
}

/// One chunk from [`get_chunk_bounds`]: `(begin, end)` with `None` unbounded.
pub type ChunkBounds = (Option<DateBound>, Option<DateBound>);

/// `Pipe.get_chunk_bounds(begin, end, bounded, chunk_interval, align)` with the sync times
/// already resolved (`oldest` / `newest` stand in for `get_sync_time(newest=False/True)`).
///
/// The same port as `meerschaum-engine`'s `axis::get_chunk_bounds`, over bounds instead of axis
/// positions (the engine is above this crate).
#[allow(clippy::too_many_arguments)]
pub fn get_chunk_bounds(
    begin: Option<DateBound>,
    end: Option<DateBound>,
    bounded: bool,
    chunk_interval: ChunkInterval,
    align: bool,
    oldest: Option<DateBound>,
    newest: Option<DateBound>,
) -> Result<Vec<ChunkBounds>, String> {
    let include_less_than_begin = !bounded && begin.is_none();
    let include_greater_than_end = !bounded && end.is_none();
    let begin = begin.or(oldest);
    let mut consolidate_end_chunk = false;
    let end = match end {
        Some(e) => Some(e),
        None => newest.map(|n| {
            consolidate_end_chunk = true;
            match n {
                DateBound::Int(i) => DateBound::Int(i + 1),
                DateBound::Datetime(dt) => DateBound::Datetime(DateTimeValue {
                    utc: dt.utc + Duration::minutes(1),
                    tz: dt.tz,
                }),
            }
        }),
    };
    let (begin, end) = match (begin, end) {
        (None, None) => return Ok(vec![(None, None)]),
        (Some(b), Some(e)) => (b, e),
        _ => return Err("cannot compute chunk bounds with only one bound".into()),
    };
    let is_datetime = matches!(begin, DateBound::Datetime(_));
    let chunk: i64 = match chunk_interval {
        ChunkInterval::Int(i) => i.max(1),
        ChunkInterval::Delta(d) => d.num_microseconds().unwrap_or(i64::MAX).max(1),
    };
    let (b, e) = (bound_pos(&begin), bound_pos(&end));
    // PARITY: `if begin and end:` — an integer bound of 0 is falsy (`_data.py:856`).
    let truthy = |v: i64| is_datetime || v != 0;
    if truthy(b) && truthy(e) && b >= e {
        return Ok(if bounded {
            vec![(Some(begin.clone()), Some(begin))]
        } else {
            vec![(Some(begin), None)]
        });
    }
    let mut cursor = if align {
        b.div_euclid(chunk) * chunk
    } else {
        b
    };
    let mut bounds: Vec<(i64, i64)> = Vec::new();
    while cursor < e {
        let next = cursor.saturating_add(chunk);
        bounds.push((cursor, next));
        cursor = next;
        if bounds.len() >= 1_000_000 {
            return Err(format!(
                "Too many chunks of size '{chunk}' between '{}' and '{}'.",
                begin.py_str(),
                end.py_str()
            ));
        }
    }
    if bounds.len() > 1 && consolidate_end_chunk {
        let last = bounds.pop().expect("len > 1");
        let second_last = bounds.pop().expect("len > 1");
        bounds.push((second_last.0, last.1));
    }
    if bounds.is_empty() && e >= b {
        bounds.push((b, e));
    }
    let Some(last) = bounds.last_mut() else {
        return Err("no chunk bounds (end precedes begin)".into());
    };
    if last.1 > e {
        last.1 = e;
    }
    if last.0 == last.1 {
        bounds.pop();
    }
    if align {
        if let Some(first) = bounds.first_mut() {
            if first.0 < b {
                first.0 = b;
            }
        }
    }
    let mut out: Vec<ChunkBounds> = bounds
        .into_iter()
        .map(|(x, y)| {
            (
                Some(bound_from_pos(x, &begin)),
                Some(bound_from_pos(y, &begin)),
            )
        })
        .collect();
    if include_less_than_begin {
        out.insert(0, (None, Some(begin)));
    }
    if include_greater_than_end {
        out.push((Some(end), None));
    }
    Ok(out)
}

/// The arguments of `get_pipe_data_query` beyond [`DataQuery`].
#[derive(Debug, Clone, Default)]
pub struct DataQueryOptions {
    /// `begin=''`: use the sync time minus the backtrack interval.
    pub begin_default: bool,
    /// `begin_add_minutes`.
    pub begin_add_minutes: i64,
    /// `end_add_minutes`.
    pub end_add_minutes: i64,
    /// `replace_nulls`.
    pub replace_nulls: Option<String>,
    /// `skip_existing_cols_check`.
    pub skip_existing_cols_check: bool,
    /// `order=None` (omit the `ORDER BY`); `DataQuery.order = None` means Python's default `'asc'`.
    pub no_order: bool,
}

impl SqlConnector {
    /// `pipe.get_columns_types()` / `get_pipe_columns_types(pipe)`: `{column: DB TYPE}`.
    pub async fn get_pipe_columns_types(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, String>> {
        let sp = self.sql_pipe(pipe)?;
        Ok(self.columns_types(&sp).await)
    }

    pub(crate) async fn columns_types(&self, sp: &SqlPipe) -> IndexMap<String, String> {
        let schema = self.get_pipe_schema(sp);
        if !self.table_exists(&sp.target, schema.as_deref()).await {
            return IndexMap::new();
        }
        if reflect::uses_reflection(self.flavor) {
            self.get_reflected_columns_types(&sp.target, schema.as_deref())
                .await
        } else {
            self.get_table_cols_types(&sp.target, schema.as_deref())
                .await
        }
    }

    /// The table state a pipe operation reads.
    pub async fn pipe_state(&self, sp: &SqlPipe) -> PipeState {
        let cols_types = self.columns_types(sp).await;
        let exists = !cols_types.is_empty()
            || self
                .table_exists(&sp.target, self.get_pipe_schema(sp).as_deref())
                .await;
        PipeState {
            exists,
            dtypes: pipe_dtypes(sp, &cols_types, true),
            explicit_dtypes: pipe_dtypes(sp, &cols_types, false),
            cols_types,
        }
    }

    /// The `Pipe.enforce_dtypes()` context for a pipe.
    pub fn dtypes_ctx(&self, sp: &SqlPipe, state: &PipeState) -> PipeDtypesContext {
        PipeDtypesContext {
            dtypes: state.dtypes.clone(),
            explicit_dtypes: state.explicit_dtypes.clone(),
            enforce: sp.enforce(),
            tz_aware: pipe_is_tz_aware(
                &columns_map(sp),
                &state.dtypes,
                sp.autotime(),
                &self.settings.pipe_defaults.autotime_column,
            ),
            mixed_numerics: sp.mixed_numerics(),
        }
    }

    /// `get_pipe_columns_indices(pipe)`: `{column: [{name, type}]}`.
    pub async fn get_pipe_columns_indices(
        &self,
        pipe: &Pipe,
    ) -> ConnectorResult<IndexMap<String, Vec<IndexDoc>>> {
        let sp = self.sql_pipe(pipe)?;
        Ok(self
            .get_table_cols_indices(&sp.target, self.get_pipe_schema(&sp).as_deref())
            .await)
    }

    /// `pipe_exists(pipe)`: whether the target table exists.
    pub async fn pipe_exists(&self, pipe: &Pipe) -> ConnectorResult<bool> {
        let sp = self.sql_pipe(pipe)?;
        Ok(self
            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
            .await)
    }

    /// `Pipe.parse_date_bounds(bound)`: coerce a datetime to the axis' timezone (and an
    /// integer axis with `precision` to an epoch integer).
    pub fn parse_date_bound(
        &self,
        sp: &SqlPipe,
        dtypes: &IndexMap<String, String>,
        bound: Option<DateBound>,
    ) -> Result<Option<DateBound>, String> {
        let Some(bound) = bound else {
            return Ok(None);
        };
        let DateBound::Datetime(dt) = &bound else {
            return Ok(Some(bound));
        };
        let dt_col = sp.column("datetime");
        let dt_typ = dt_col
            .as_ref()
            .and_then(|c| dtypes.get(c).cloned())
            .unwrap_or_else(|| "datetime".into());
        if are_dtypes_equal(&dt_typ, "int") {
            if sp.param("precision").is_some_and(|p| p.truthy()) {
                let unit = sp.precision_unit().map_err(|e| e.to_string())?;
                let v = meerschaum_kernel::cast::datetime_to_int(
                    &meerschaum_kernel::Value::Datetime(dt.clone()),
                    unit.name(),
                )
                .map_err(|e| e.to_string())?;
                return Ok(Some(DateBound::Int(v)));
            }
            return Err(format!(
                "Cannot use datetime bound '{}' on the non-epoch integer axis of {}.\n    Pass an integer instead, or set the `precision` parameter.",
                bound.py_str(),
                sp.pipe()
            ));
        }
        let dt_typ = if dt_typ == "datetime" {
            "datetime64[ns, UTC]".to_string()
        } else {
            dt_typ
        };
        Ok(Some(coerce_bound(
            bound,
            !dt_typ.to_lowercase().contains("utc"),
        )))
    }

    /// `get_pipe_data_query(pipe, select_columns, omit_columns, begin, end, params, order,
    /// limit, ...)`: the `SELECT` for a pipe's data (`None` never happens on SQL).
    pub async fn get_pipe_data_query(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        opts: &DataQueryOptions,
    ) -> ConnectorResult<String> {
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state(&sp).await;
        self.build_data_query(&sp, &state, query, opts).await
    }

    pub async fn build_data_query(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &DataQuery,
        opts: &DataQueryOptions,
    ) -> ConnectorResult<String> {
        let flavor = self.flavor;
        let q = |c: &str| sql_item_name(c, flavor, None);
        let enforce = sp.enforce();
        let dt_col = sp.column("datetime");
        let empty = IndexMap::new();
        let existing_cols = if enforce { &state.cols_types } else { &empty };
        let skip_check = opts.skip_existing_cols_check || !enforce;
        let dt_typ = dt_col
            .as_ref()
            .and_then(|c| existing_cols.get(c))
            .map(|t| get_pd_type_from_db_type(t, true));
        let dt_db_type = dt_typ
            .as_ref()
            .map(|t| get_db_type_from_pd_type(t, Some(flavor)));
        let mut select_columns: Vec<String> = match &query.select_columns {
            Some(cols) if !cols.is_empty() => cols
                .iter()
                .filter(|c| skip_check || existing_cols.contains_key(*c))
                .cloned()
                .collect(),
            _ => existing_cols.keys().cloned().collect(),
        };
        if let Some(omit) = query.omit_columns.as_ref().filter(|o| !o.is_empty()) {
            select_columns.retain(|c| !omit.contains(c));
        }

        let mut begin = query.begin.clone();
        let mut end = query.end.clone();
        if opts.begin_default {
            begin = self.sync_time(sp, state, None, true).await;
            if let Some(b) = begin.take() {
                let bt = sp.backtrack_interval(true).map_err(be)?;
                begin = Some(subtract_interval(b, bt));
            }
        }
        begin = self
            .parse_date_bound(sp, &state.dtypes, begin)
            .map_err(be)?;
        end = self.parse_date_bound(sp, &state.dtypes, end).map_err(be)?;
        if let Some(t) = &dt_typ {
            let strip = !t.to_lowercase().contains("utc");
            begin = begin.map(|b| coerce_bound(b, strip));
            end = end.map(|e| coerce_bound(e, strip));
        }

        let cols_names: Vec<String> = select_columns.iter().map(|c| q(c)).collect();
        let select_cols_str = if cols_names.is_empty() {
            "SELECT *".to_string()
        } else {
            let items: Vec<String> = cols_names
                .iter()
                .map(|n| match &opts.replace_nulls {
                    // PARITY: Python's f-string is missing braces around `col_name`
                    // (`_pipes.py:1513`), so the literal text `col_name` is emitted.
                    Some(r) => format!("COALESCE(col_name, '{r}') AS {n}"),
                    None => n.clone(),
                })
                .collect();
            format!("SELECT\n    {}", items.join(",\n    "))
        };
        let pipe_table_name = self.pipe_table_name(sp);
        let mut sql = format!("{select_cols_str}\nFROM {pipe_table_name}");
        let mut where_ = String::new();

        let mariadb_workaround = flavor == Flavor::Mariadb
            && query.limit.is_some()
            && crate::queries::partition::should_partition(flavor, sp);

        let order = if opts.no_order {
            None
        } else {
            let o = query.order.clone().unwrap_or_else(|| "asc".into());
            Some(if o == "asc" || o == "desc" {
                o.to_uppercase()
            } else {
                tracing::warn!("Ignoring unsupported order '{o}'. Falling back to 'asc'.");
                "ASC".to_string()
            })
        };

        let (dt_name_col, is_guess) = match &dt_col {
            None => (guess_datetime(sp, &state.dtypes), true),
            Some(c) => (Some(c.clone()), false),
        };
        let dt = dt_name_col.as_deref().map(q);

        let columns = str_map(&sp.columns());
        let quoted_indices: Vec<String> = columns
            .iter()
            .filter(|(_, v)| !v.is_empty() && (skip_check || existing_cols.contains_key(*v)))
            .map(|(_, v)| q(v))
            .collect();

        if (begin.is_some() || end.is_some()) && is_guess {
            match &dt_name_col {
                None => {
                    tracing::warn!(
                        "No datetime could be determined for {}.\n    Ignoring begin and end...",
                        sp.pipe()
                    );
                    begin = None;
                    end = None;
                }
                Some(c) => tracing::warn!(
                    "A datetime wasn't specified for {}.\n    Using column \"{c}\" for datetime bounds...",
                    sp.pipe()
                ),
            }
        }

        let dt_ok = dt_name_col
            .as_ref()
            .is_some_and(|c| skip_check || existing_cols.contains_key(c));
        let mut is_dt_bound = false;
        if let (Some(b), true) = (&begin, dt_ok) {
            let begin_da = dateadd_str(
                Some(flavor),
                Some("minute"),
                Number::Int(opts.begin_add_minutes),
                &date_arg(Some(b)),
                dt_db_type.as_deref(),
            )
            .map_err(be)?;
            where_.push_str(&format!(
                "\n    {} >= {begin_da}",
                dt.clone().unwrap_or_default()
            ));
            if end.is_some() {
                where_.push_str("\n    AND\n    ");
            }
            is_dt_bound = true;
        }
        if let (Some(e), true) = (&end, dt_ok) {
            let mut e = e.clone();
            if let (DateBound::Int(ei), Some(DateBound::Int(bi))) = (&e, &begin) {
                if ei == bi {
                    e = DateBound::Int(ei + 1);
                }
            }
            let end_da = dateadd_str(
                Some(flavor),
                Some("minute"),
                Number::Int(opts.end_add_minutes),
                &date_arg(Some(&e)),
                dt_db_type.as_deref(),
            )
            .map_err(be)?;
            where_.push_str(&format!("{} <  {end_da}", dt.clone().unwrap_or_default()));
            is_dt_bound = true;
        }

        if let Some(params) = &query.params {
            let valid: IndexMap<String, Value> = params
                .iter()
                .filter(|(k, _)| skip_check || existing_cols.contains_key(*k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if !valid.is_empty() {
                let w = build_where(&valid, flavor, true);
                let w = meerschaum_core::pyfmt::py_lstrip(&w)
                    .replace("WHERE", if is_dt_bound { "    AND" } else { "    " });
                where_.push_str("    ");
                where_.push_str(&w);
            }
        }
        if !where_.is_empty() {
            sql.push_str("\nWHERE ");
            sql.push_str(&where_);
        }

        if let Some(order) = &order {
            let mut order_by = String::new();
            if !quoted_indices.is_empty() {
                order_by.push_str("\nORDER BY ");
                if let (Some(d), true) = (&dt, dt_ok) {
                    let expr = if mariadb_workaround {
                        format!("COALESCE({d}, {d})")
                    } else {
                        d.clone()
                    };
                    order_by.push_str(&format!("{expr} {order},"));
                }
                for col in &quoted_indices {
                    if Some(col) == dt.as_ref() {
                        continue;
                    }
                    order_by.push_str(&format!(" {col} {order},"));
                }
                order_by.pop();
            }
            sql.push_str(&order_by);
        }

        if let Some(limit) = query.limit {
            match flavor {
                Flavor::Mssql => {
                    sql = format!("SELECT TOP {limit}\n{}", &sql["SELECT ".len()..]);
                }
                Flavor::Oracle => {
                    let rows: Vec<String> = (1..=limit).map(|i| i.to_string()).collect();
                    sql = format!(
                        "SELECT * FROM (\n  {sql}\n)\nWHERE ROWNUM IN ({})",
                        rows.join(", ")
                    );
                }
                _ => sql.push_str(&format!("\nLIMIT {limit}")),
            }
        }
        Ok(sql)
    }

    /// `get_pipe_data(pipe, select_columns, omit_columns, begin, end, params, order, limit)`
    /// followed by the `Pipe.get_data()` post-processing (column selection, dtype enforcement,
    /// sorting). `None` if the table does not exist.
    pub async fn get_pipe_data(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state(&sp).await;
        if !state.exists {
            return Ok(None);
        }
        self.get_pipe_data_with_state(&sp, &state, query, &DataQueryOptions::default())
            .await
    }

    /// Raw database documents, matching v4's result.mappings() (no pipe dtype enforcement).
    pub async fn get_pipe_docs(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Vec<IndexMap<String, meerschaum_kernel::Value>>> {
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state(&sp).await;
        if !state.exists {
            return Ok(Vec::new());
        }
        let sql = self
            .build_data_query(&sp, &state, query, &DataQueryOptions::default())
            .await?;
        self.log(&sql);
        let df = self.driver.read_docs_frame(&sql).await.map_err(be)?;
        Ok(super::frame_docs(&df))
    }

    pub(crate) async fn get_pipe_data_with_state(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &DataQuery,
        options: &DataQueryOptions,
    ) -> ConnectorResult<Option<Frame>> {
        let sql = self.build_data_query(sp, state, query, options).await?;
        let df = self.read(&sql).await.map_err(be)?;

        // `Pipe.get_data()`: keep the requested columns, add missing ones as nulls.
        let user_select: Vec<String> = query
            .select_columns
            .clone()
            .filter(|c| !c.is_empty())
            .unwrap_or_else(|| df.names());
        let omit = query.omit_columns.clone().unwrap_or_default();
        let keep: Vec<String> = df
            .names()
            .into_iter()
            .filter(|c| !omit.contains(c) && user_select.contains(c))
            .collect();
        let mut df = if keep.len() != df.width() {
            df.select(&keep).map_err(be)?
        } else {
            df
        };
        let cols_to_add: IndexMap<String, String> = user_select
            .iter()
            .filter(|c| !df.contains(c))
            .map(|c| {
                (
                    c.clone(),
                    state
                        .dtypes
                        .get(c)
                        .cloned()
                        .unwrap_or_else(|| "string".into()),
                )
            })
            .collect();
        if !cols_to_add.is_empty() {
            df = meerschaum_kernel::diff::add_missing_cols(&df, &{
                let mut all: IndexMap<String, String> = df.dtypes();
                all.extend(cols_to_add);
                all
            });
        }
        let ctx = self.dtypes_ctx(sp, state);
        let mut df = pipe_enforce_dtypes(PipeInput::Frame(df), &ctx).map_err(be)?;
        if self.flavor == Flavor::Geopackage {
            // GeoPackage geometries are `BLOB`s, which the driver tags as `bytes`; once enforced
            // as geometries the tag must not make the frame's bytes columns claim them (the
            // `filter_existing` diff would base64-decode the new geometries).
            for (col, typ) in &state.dtypes {
                if !are_dtypes_equal(typ, "geometry") && !typ.starts_with("geography") {
                    continue;
                }
                if let Some(c) = df.column_mut(col) {
                    if c.tags.dtype.as_deref() == Some("bytes") {
                        c.tags.dtype = Some("geometry".into());
                    }
                }
            }
        }
        if options.no_order {
            if let Some(limit) = query.limit {
                if df.height() > limit.max(0) as usize {
                    df = df.take(&(0..limit.max(0) as usize).collect::<Vec<_>>());
                }
            }
            Ok(Some(df))
        } else {
            Ok(Some(meerschaum_connector::pipe_ops::pipe_postprocess(
                sp, df, query,
            )))
        }
    }

    /// `Pipe.get_data(as_iterator=True)`: the data in chunks of `get_chunk_bounds`
    /// (`chunk_interval` defaults to the pipe's `verify.chunk_minutes`).
    pub async fn get_pipe_data_chunks(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
        chunk_interval: Option<crate::pipe::ChunkIntervalArg>,
    ) -> ConnectorResult<Vec<Frame>> {
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state(&sp).await;
        if !state.exists {
            return Ok(Vec::new());
        }
        let dt_col = sp.column("datetime");
        let begin = self
            .parse_date_bound(&sp, &state.dtypes, query.begin.clone())
            .map_err(be)?;
        let end = self
            .parse_date_bound(&sp, &state.dtypes, query.end.clone())
            .map_err(be)?;
        let params = query.params.as_ref();
        let min_dt = match (&dt_col, &begin) {
            (None, _) => None,
            (_, Some(b)) => Some(b.clone()),
            _ => self
                .sync_time(&sp, &state, params, false)
                .await
                .and_then(|b| {
                    self.parse_date_bound(&sp, &state.dtypes, Some(b))
                        .ok()
                        .flatten()
                }),
        };
        let mut max_dt = match (&dt_col, &end) {
            (None, _) => None,
            (_, Some(e)) => Some(e.clone()),
            _ => self
                .sync_time(&sp, &state, params, true)
                .await
                .and_then(|b| {
                    self.parse_date_bound(&sp, &state.dtypes, Some(b))
                        .ok()
                        .flatten()
                }),
        };
        // We want to search just past the maximum value.
        if end.is_none() {
            max_dt = max_dt.map(|m| match m {
                DateBound::Int(i) => DateBound::Int(i + 1),
                DateBound::Datetime(dt) => {
                    let rounded = meerschaum_kernel::cast::round_time(
                        &DateTimeValue {
                            utc: dt.utc + Duration::minutes(1),
                            tz: dt.tz,
                        },
                        60_000_000,
                        meerschaum_kernel::cast::RoundTo::Down,
                    );
                    DateBound::Datetime(rounded)
                }
            });
        }
        let interval = match chunk_interval {
            Some(ci) => sp.chunk_interval_with(ci).map_err(be)?,
            None => sp.chunk_interval().map_err(be)?,
        };
        let single = match (&min_dt, &max_dt) {
            (None, None) => true,
            (Some(mn), Some(mx)) => {
                let width = match interval {
                    ChunkInterval::Int(i) => i,
                    ChunkInterval::Delta(d) => d.num_microseconds().unwrap_or(i64::MAX),
                };
                bound_pos(mn).saturating_add(width) > bound_pos(mx)
            }
            _ => true,
        };
        if single {
            let q = DataQuery {
                begin,
                end,
                ..query.clone()
            };
            return Ok(self
                .get_pipe_data_with_state(&sp, &state, &q, &DataQueryOptions::default())
                .await?
                .into_iter()
                .collect());
        }
        let bounds =
            get_chunk_bounds(min_dt, max_dt, false, interval, false, None, None).map_err(be)?;
        let mut out = Vec::new();
        for (b, e) in bounds {
            let q = DataQuery {
                begin: b,
                end: e,
                ..query.clone()
            };
            if let Some(chunk) = self
                .get_pipe_data_with_state(&sp, &state, &q, &DataQueryOptions::default())
                .await?
            {
                if chunk.height() > 0 {
                    out.push(chunk);
                }
            }
        }
        Ok(out)
    }

    /// `Pipe.get_backtrack_data(backtrack_minutes, begin, params, limit)`: rows newer than the
    /// sync time (or `begin`) minus the backtrack window, newest first.
    pub async fn get_backtrack_data(
        &self,
        pipe: &Pipe,
        backtrack_minutes: Option<i64>,
        query: &DataQuery,
    ) -> ConnectorResult<Option<Frame>> {
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state(&sp).await;
        if !state.exists {
            return Ok(None);
        }
        let mut begin = self
            .parse_date_bound(&sp, &state.dtypes, query.begin.clone())
            .map_err(be)?;
        if begin.is_none() {
            begin = self
                .sync_time(&sp, &state, query.params.as_ref(), true)
                .await
                .and_then(|b| {
                    self.parse_date_bound(&sp, &state.dtypes, Some(b))
                        .ok()
                        .flatten()
                });
        }
        let backtrack = match backtrack_minutes {
            Some(m) => match &begin {
                Some(DateBound::Datetime(_)) => ChunkInterval::minutes(m),
                _ => ChunkInterval::Int(m),
            },
            None => sp.backtrack_interval(true).map_err(be)?,
        };
        let begin = begin.map(|b| subtract_interval(b, backtrack));
        let q = DataQuery {
            begin,
            order: Some(query.order.clone().unwrap_or_else(|| "desc".into())),
            ..query.clone()
        };
        self.get_pipe_data_with_state(&sp, &state, &q, &DataQueryOptions::default())
            .await
    }

    /// `get_sync_time(pipe, params, newest, remote)`: the newest (or oldest) datetime-axis
    /// value, `None` if the table is empty or missing.
    pub async fn get_sync_time(
        &self,
        pipe: &Pipe,
        params: Option<&Map>,
        newest: bool,
        remote: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state(&sp).await;
        if remote {
            return self.remote_sync_time(&sp, &state, params, newest).await;
        }
        Ok(self.sync_time(&sp, &state, params, newest).await)
    }

    pub(crate) async fn sync_time(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        params: Option<&Map>,
        newest: bool,
    ) -> Option<DateBound> {
        let query = self.get_sync_time_query(sp, state, params, newest)?;
        let v = self.value(&query).await?;
        let bound = sync_time_from_sql(&v)?;
        // PARITY: v4 restores the pipe timezone after parsing SQLite's raw timestamp text.
        self.parse_date_bound(sp, &state.dtypes, Some(bound))
            .ok()
            .flatten()
    }

    /// The query `get_sync_time(pipe, params, newest)` executes (`None` when the pipe has no
    /// datetime axis or no table).
    pub fn get_sync_time_query(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        params: Option<&Map>,
        newest: bool,
    ) -> Option<String> {
        let flavor = self.flavor;
        let dt_col = sp.column("datetime")?;
        let existing_cols = &state.cols_types;
        if existing_cols.is_empty() {
            return None;
        }
        let mut valid: IndexMap<String, Value> = params
            .map(|p| {
                p.iter()
                    .filter(|(k, _)| existing_cols.contains_key(*k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        // If no bounds are provided for the datetime column, add `IS NOT NULL`.
        if !valid.contains_key(&dt_col) {
            valid.insert(dt_col.clone(), Value::Str("_None".into()));
        }
        let where_ = build_where(&valid, flavor, true);
        let table_name = self.pipe_table_name(sp);
        let src_query = format!(
            "SELECT {}\nFROM {table_name}{where_}",
            sql_item_name(&dt_col, flavor, None)
        );
        self.sync_time_query(sp, &dt_col, &src_query, newest, flavor, false)
            .ok()
    }

    fn sync_time_query(
        &self,
        sp: &SqlPipe,
        dt_col: &str,
        src_query: &str,
        newest: bool,
        flavor: Flavor,
        remote: bool,
    ) -> crate::Result<String> {
        let src_name = sql_item_name("src", self.flavor, None);
        let dt_col_name = sql_item_name(dt_col, self.flavor, None);
        let asc_or_desc = if newest { "DESC" } else { "ASC" };
        let mut base_query = match self.flavor {
            Flavor::Mssql => format!(
                "SELECT TOP 1 {dt_col_name}\nFROM {src_name}\nORDER BY {dt_col_name} {asc_or_desc}"
            ),
            Flavor::Oracle => format!(
                "SELECT * FROM (\n    SELECT {dt_col_name}\n    FROM {src_name}\n    ORDER BY {dt_col_name} {asc_or_desc}\n) WHERE ROWNUM = 1"
            ),
            _ => format!(
                "SELECT {dt_col_name}\nFROM {src_name}\nORDER BY {dt_col_name} {asc_or_desc}\nLIMIT 1"
            ),
        };
        // NOTE: MariaDB's optimizer stops an ordered `LIMIT 1` partition index scan early on a
        // `RANGE COLUMNS` table with a `WHERE` clause (MariaDB 12.x); `MIN`/`MAX` scans the
        // pruned partitions correctly.
        if self.flavor == Flavor::Mariadb
            && !remote
            && crate::queries::partition::should_partition(self.flavor, sp)
        {
            let agg = if newest { "MAX" } else { "MIN" };
            base_query = format!("SELECT {agg}({dt_col_name}) AS {dt_col_name}\nFROM {src_name}");
        }
        wrap_query_with_cte(src_query, &base_query, flavor, "src")
    }

    async fn remote_sync_time(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        params: Option<&Map>,
        newest: bool,
    ) -> ConnectorResult<Option<DateBound>> {
        let Some(dt_col) = sp.column("datetime") else {
            return Ok(None);
        };
        if !sp.connector_keys().starts_with("sql:") {
            tracing::warn!("Cannot get the remote sync time for {}.", sp.pipe());
            return Ok(None);
        }
        let params_map: Option<IndexMap<String, Value>> =
            params.map(|p| p.iter().map(|(k, v)| (k.clone(), v.clone())).collect());
        let mut args = crate::queries::metadef::MetadefArgs::new(sp);
        args.params = params_map.as_ref();
        args.begin = crate::queries::metadef::Bound::None;
        args.end = crate::queries::metadef::Bound::None;
        let Some(src_query) =
            crate::queries::metadef::get_pipe_metadef(self.flavor, &args).map_err(be)?
        else {
            return Ok(None);
        };
        // PARITY: the remote query is built with the instance's metadef and executed on the
        // instance (`_pipes.py:2979`, `self.value(query)`), with the source's flavor for the CTE.
        let query = self
            .sync_time_query(sp, &dt_col, &src_query, newest, self.flavor, true)
            .map_err(be)?;
        let bound = self
            .value(&query)
            .await
            .and_then(|v| sync_time_from_sql(&v));
        Ok(self
            .parse_date_bound(sp, &state.dtypes, bound)
            .ok()
            .flatten())
    }

    /// `get_pipe_rowcount(pipe, begin, end, params, remote)`.
    pub async fn get_pipe_rowcount(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<Option<i64>> {
        let sp = self.sql_pipe(pipe)?;
        if query.remote {
            let has_def = sp
                .param("fetch")
                .and_then(|f| f.get("definition"))
                .is_some();
            if !has_def {
                return Err(be(format!(
                    "'fetch:definition' must be an attribute of {pipe} to get a remote rowcount."
                )));
            }
        }
        let state = self.pipe_state(&sp).await;
        let Some(sql) = self.get_pipe_rowcount_query(&sp, &state, query)? else {
            return Ok(None);
        };
        Ok(self.value(&sql).await.and_then(|v| v.as_i64()))
    }

    /// The `COUNT(*)` query of `get_pipe_rowcount` (`None` when the table does not exist).
    pub fn get_pipe_rowcount_query(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &DataQuery,
    ) -> ConnectorResult<Option<String>> {
        let pipe = sp.pipe();
        let flavor = self.flavor;
        let remote = query.remote;
        if !remote && !state.exists {
            return Ok(None);
        }
        let pipe_name = self.pipe_table_name(sp);
        let dt_col_cfg = sp.column("datetime");
        let dt_typ = dt_col_cfg.as_ref().map(|c| {
            state
                .dtypes
                .get(c)
                .cloned()
                .unwrap_or_else(|| "datetime".into())
        });
        let dt_db_type = dt_typ
            .as_ref()
            .map(|t| get_db_type_from_pd_type(t, Some(flavor)));
        let (dt_col, is_guess) = match &dt_col_cfg {
            None => (guess_datetime(sp, &state.dtypes), true),
            Some(c) => (Some(c.clone()), false),
        };
        let dt_name = dt_col.as_deref().map(|c| sql_item_name(c, flavor, None));
        // `Pipe.get_rowcount()` parses the bounds first (`_data.py:592`).
        let mut begin = self
            .parse_date_bound(sp, &state.dtypes, query.begin.clone())
            .map_err(be)?;
        let mut end = self
            .parse_date_bound(sp, &state.dtypes, query.end.clone())
            .map_err(be)?;
        if (begin.is_some() || end.is_some()) && is_guess {
            match &dt_col {
                None => {
                    tracing::warn!(
                        "No datetime could be determined for {pipe}.\n    Ignoring begin and end..."
                    );
                    begin = None;
                    end = None;
                }
                Some(c) => tracing::warn!(
                    "A datetime wasn't specified for {pipe}.\n    Using column \"{c}\" for datetime bounds..."
                ),
            }
        }
        // PARITY: the selected columns come from a Python `set`; their order is
        // `PYTHONHASHSEED`-dependent there and first-seen here.
        let mut col_names: Vec<String> = Vec::new();
        if let Some(c) = &dt_col {
            col_names.push(c.clone());
        }
        if let Some(p) = &query.params {
            for k in p.keys() {
                if !col_names.contains(k) {
                    col_names.push(k.clone());
                }
            }
        }
        let cols: Vec<String> = if col_names.is_empty() {
            vec!["*".into()]
        } else {
            col_names
                .iter()
                .map(|c| sql_item_name(c, flavor, None))
                .collect()
        };
        let src = if !remote {
            format!("SELECT {}\nFROM {pipe_name}", cols.join(", "))
        } else {
            get_pipe_query(sp).unwrap_or_default()
        };
        let parent_query = format!(
            "SELECT COUNT(*)\nFROM {}",
            sql_item_name("src", flavor, None)
        );
        let mut sql = wrap_query_with_cte(&src, &parent_query, flavor, "src").map_err(be)?;
        if begin.is_some() || end.is_some() {
            sql.push_str("\nWHERE");
        }
        let dn = dt_name.clone().unwrap_or_default();
        if let Some(b) = &begin {
            sql.push_str(&format!(
                "\n    {dn} >= {}",
                dateadd_str(
                    Some(flavor),
                    Some("minute"),
                    Number::Int(0),
                    &date_arg(Some(b)),
                    dt_db_type.as_deref()
                )
                .map_err(be)?
            ));
        }
        if end.is_some() && begin.is_some() {
            sql.push_str("\n    AND");
        }
        if let Some(e) = &end {
            sql.push_str(&format!(
                "\n    {dn} <  {}",
                dateadd_str(
                    Some(flavor),
                    Some("minute"),
                    Number::Int(0),
                    &date_arg(Some(e)),
                    dt_db_type.as_deref()
                )
                .map_err(be)?
            ));
        }
        if let Some(params) = &query.params {
            let valid: IndexMap<String, Value> = params
                .iter()
                .filter(|(k, _)| state.cols_types.contains_key(*k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            if !valid.is_empty() {
                let w = build_where(&valid, flavor, true).replace(
                    "WHERE",
                    if begin.is_some() || end.is_some() {
                        "AND"
                    } else {
                        "WHERE"
                    },
                );
                sql.push_str(&w);
            }
        }
        Ok(Some(sql))
    }

    /// `drop_pipe(pipe)`: drop the target table (keep the registration).
    pub async fn drop_pipe(&self, pipe: &Pipe) -> SuccessTuple {
        let sp = match self.sql_pipe(pipe) {
            Ok(sp) => sp,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let schema = self.get_pipe_schema(&sp);
        let target_name = self.pipe_table_name(&sp);
        self.state
            .lock()
            .expect("state")
            .internal_pipe_states
            .remove(&target_name);
        let mut success = true;
        if self.table_exists(&sp.target, schema.as_deref()).await {
            let if_exists = if self.flavor.drop_if_exists() {
                "IF EXISTS"
            } else {
                ""
            };
            success = self
                .exec(&format!("DROP TABLE {if_exists} {target_name}"))
                .await
                .is_some();
        }
        // Remove the table's GeoPackage metadata and spatial index (no-op otherwise).
        if success && self.flavor == crate::flavor::Flavor::Geopackage {
            let _ = self.drop_geopackage_metadata(&sp.target).await;
        }
        // Drop any MSSQL partition scheme + function the table referenced (no-op otherwise).
        if success {
            let cleanup =
                crate::queries::partition::get_partition_cleanup_queries(self.flavor, &sp);
            if !cleanup.is_empty() {
                let _ = self.exec_queries(&cleanup, false, false).await;
            }
        }
        if success {
            SuccessTuple::ok("Success")
        } else {
            SuccessTuple::fail(format!("Failed to drop {pipe}."))
        }
    }

    /// `clear_pipe(pipe, begin, end, params)`: delete rows (`>= begin`, `< end`, matching
    /// `params`) without dropping the table.
    pub async fn clear_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        match self.clear_pipe_inner(pipe, query).await {
            Ok(t) => t,
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    async fn clear_pipe_inner(
        &self,
        pipe: &Pipe,
        query: &DataQuery,
    ) -> ConnectorResult<SuccessTuple> {
        let sp = self.sql_pipe(pipe)?;
        let state = self.pipe_state(&sp).await;
        if !state.exists {
            return Ok(SuccessTuple::ok(format!(
                "{pipe} does not exist, so nothing was cleared."
            )));
        }
        let sql = self.get_clear_pipe_query(&sp, &state, query)?;
        Ok(if self.exec(&sql).await.is_some() {
            SuccessTuple::ok("Success")
        } else {
            SuccessTuple::fail(format!("Failed to clear {pipe}."))
        })
    }

    /// The `DELETE` query of `clear_pipe(pipe, begin, end, params)`.
    pub fn get_clear_pipe_query(
        &self,
        sp: &SqlPipe,
        state: &PipeState,
        query: &DataQuery,
    ) -> ConnectorResult<String> {
        let pipe = sp.pipe();
        let flavor = self.flavor;
        let pipe_name = self.pipe_table_name(sp);
        let dt_cfg = sp.column("datetime");
        let dt_typ = dt_cfg.as_ref().map(|c| {
            state
                .dtypes
                .get(c)
                .cloned()
                .unwrap_or_else(|| "datetime".into())
        });
        let dt_db_type = dt_typ
            .as_ref()
            .map(|t| get_db_type_from_pd_type(t, Some(flavor)));
        let (dt_col, is_guess) = match &dt_cfg {
            None => (guess_datetime(sp, &state.dtypes), true),
            Some(c) => (Some(c.clone()), false),
        };
        let dt_name = dt_col
            .as_deref()
            .map(|c| sql_item_name(c, flavor, None))
            .unwrap_or_default();
        // `Pipe.clear()` parses the bounds first (`_clear.py:61`).
        let mut begin = self
            .parse_date_bound(sp, &state.dtypes, query.begin.clone())
            .map_err(be)?;
        let mut end = self
            .parse_date_bound(sp, &state.dtypes, query.end.clone())
            .map_err(be)?;
        if (begin.is_some() || end.is_some()) && is_guess {
            match &dt_col {
                None => {
                    tracing::warn!(
                        "No datetime could be determined for {pipe}.\n    Ignoring datetime bounds..."
                    );
                    begin = None;
                    end = None;
                }
                Some(c) => tracing::warn!(
                    "A datetime wasn't specified for {pipe}.\n    Using column \"{c}\" for datetime bounds..."
                ),
            }
        }
        let valid: IndexMap<String, Value> = query
            .params
            .as_ref()
            .map(|p| {
                p.iter()
                    .filter(|(k, _)| state.cols_types.contains_key(*k))
                    .map(|(k, v)| (k.clone(), v.clone()))
                    .collect()
            })
            .unwrap_or_default();
        let mut sql = format!("DELETE FROM {pipe_name}\nWHERE 1 = 1\n");
        if !valid.is_empty() {
            sql.push_str("\n    AND ");
            sql.push_str(&build_where(&valid, flavor, false));
        }
        if let Some(b) = &begin {
            sql.push_str(&format!(
                "\n    AND {dt_name} >= {}",
                dateadd_str(
                    Some(flavor),
                    Some("day"),
                    Number::Int(0),
                    &date_arg(Some(b)),
                    dt_db_type.as_deref()
                )
                .map_err(be)?
            ));
        }
        if let Some(e) = &end {
            sql.push_str(&format!(
                "\n    AND {dt_name} <  {}",
                dateadd_str(
                    Some(flavor),
                    Some("day"),
                    Number::Int(0),
                    &date_arg(Some(e)),
                    dt_db_type.as_deref()
                )
                .map_err(be)?
            ));
        }
        Ok(sql)
    }

    /// `deduplicate_pipe(pipe, begin, end, params)`: rebuild the table keeping one row per
    /// index (the newest by the datetime axis).
    ///
    /// PARITY: like v4, `begin`, `end` and `params` are accepted but the whole table is
    /// deduplicated (`_pipes.py:3995`).
    pub async fn deduplicate_pipe(&self, pipe: &Pipe, query: &DataQuery) -> SuccessTuple {
        let _ = query;
        match self.deduplicate_pipe_inner(pipe).await {
            Ok(t) => t,
            Err(e) => SuccessTuple::fail(e.to_string()),
        }
    }

    async fn deduplicate_pipe_inner(&self, pipe: &Pipe) -> ConnectorResult<SuccessTuple> {
        let sp = self.sql_pipe(pipe)?;
        let flavor = self.flavor;
        let q = |c: &str| sql_item_name(c, flavor, None);
        let pipe_table_name = self.pipe_table_name(&sp);
        let state = self.pipe_state(&sp).await;
        if !state.exists {
            return Ok(SuccessTuple::fail(format!(
                "Table {pipe_table_name} does not exist."
            )));
        }
        let dt_col = sp.column("datetime");
        let cols_types = &state.cols_types;
        let get_rowcount_query = format!("SELECT COUNT(*) FROM {pipe_table_name}");
        let Some(old_rowcount) = self
            .value(&get_rowcount_query)
            .await
            .and_then(|v| v.as_i64())
        else {
            return Ok(SuccessTuple::fail(format!(
                "Failed to get rowcount for table {pipe_table_name}."
            )));
        };
        // Non-datetime indices that in fact exist.
        let indices: Vec<String> = str_map(&sp.columns())
            .into_iter()
            .map(|(_, c)| c)
            .filter(|c| !c.is_empty() && Some(c) != dt_col.as_ref() && cols_types.contains_key(c))
            .collect();
        let indices_names: Vec<String> = indices.iter().map(|c| q(c)).collect();
        let existing_cols_names: Vec<String> = cols_types.keys().map(|c| q(c)).collect();
        let duplicate_row_number_name = q("dup_row_num");
        let previous_row_number_name = q("prev_row_num");
        let mut index_list_str = dt_col.as_deref().map(q).unwrap_or_default();
        let mut index_list_str_ordered = dt_col
            .as_deref()
            .map(|c| format!("{} DESC", q(c)))
            .unwrap_or_default();
        if !indices.is_empty() {
            index_list_str.push_str(&format!(", {}", indices_names.join(", ")));
            index_list_str_ordered.push_str(&format!(", {}", indices_names.join(", ")));
        }
        if index_list_str.starts_with(',') {
            index_list_str = index_list_str
                .trim_start_matches(',')
                .trim_start()
                .to_string();
        }
        if index_list_str_ordered.starts_with(',') {
            index_list_str_ordered = index_list_str_ordered
                .trim_start_matches(',')
                .trim_start()
                .to_string();
        }
        let cols_list_str = existing_cols_names.join(", ");
        let is_old_mysql = if flavor.mysql_partition() {
            self.db_version()
                .await
                .and_then(|v| v.split('.').next().and_then(|m| m.parse::<i64>().ok()))
                .is_some_and(|major| major < 8)
        } else {
            false
        };
        let src_query = format!(
            "\n        SELECT\n            {cols_list_str},\n            ROW_NUMBER() OVER (\n                PARTITION BY\n                {index_list_str}\n                ORDER BY {index_list_str_ordered}\n            ) AS {duplicate_row_number_name}\n        FROM {pipe_table_name}\n    "
        );
        let mut duplicates_cte_subquery = format_cte_subquery(
            &src_query,
            flavor,
            "src",
            &ColsToSelect::Raw(cols_list_str.clone()),
        )
        .map_err(be)?
            + &format!("\n        WHERE {duplicate_row_number_name} = 1\n        ");
        if is_old_mysql {
            let plus = index_list_str.replace(", ", " + ");
            let null_str = crate::queries::update::get_null_replacement("str", Some(Flavor::Mysql));
            duplicates_cte_subquery = format!(
                "\n        SELECT\n            {index_list_str}\n        FROM (\n          SELECT\n            {index_list_str},\n            IF(\n                @{previous_row_number_name} <> {plus},\n                @{duplicate_row_number_name} := 0,\n                @{duplicate_row_number_name}\n            ),\n            @{previous_row_number_name} := {plus},\n            @{duplicate_row_number_name} := @{duplicate_row_number_name} + 1 AS {duplicate_row_number_name}\n          FROM\n            {pipe_table_name},\n            (\n                SELECT @{duplicate_row_number_name} := 0\n            ) AS {duplicate_row_number_name},\n            (\n                SELECT @{previous_row_number_name} := '{null_str}'\n            ) AS {previous_row_number_name}\n          ORDER BY {index_list_str_ordered}\n        ) AS t\n        WHERE {duplicate_row_number_name} = 1\n        "
            );
        }
        let session_id =
            meerschaum_core::users::generate_password(3).unwrap_or_else(|_| "000".into());
        let dedup_table = self.get_temporary_target(&sp.target, Some(&session_id), Some("dedup"));
        let temp_old_table = self.get_temporary_target(&sp.target, Some(&session_id), Some("old"));
        let schema = self.get_pipe_schema(&sp);
        let temp_old_table_name = sql_item_name(&temp_old_table, flavor, schema.as_deref());
        let create_temporary_table_query = get_create_table_query(
            &crate::queries::table::QueryOrDtypes::Query(duplicates_cte_subquery),
            &dedup_table,
            flavor,
            None,
        )
        .map_err(be)?
            + &format!("\n    ORDER BY {index_list_str_ordered}\n    ");
        let if_exists = if flavor.drop_if_exists() {
            "IF EXISTS"
        } else {
            ""
        };
        let mut alter_queries: Vec<String> = Vec::new();
        alter_queries.extend(
            get_rename_table_queries(&sp.target, &temp_old_table, flavor, schema.as_deref(), None)
                .map_err(be)?,
        );
        alter_queries.extend(
            get_rename_table_queries(&dedup_table, &sp.target, flavor, None, schema.as_deref())
                .map_err(be)?,
        );
        alter_queries.push(format!("DROP TABLE {if_exists} {temp_old_table_name}"));

        let _ = self
            .log_temporary_tables_creation(
                std::slice::from_ref(&temp_old_table),
                false,
                !sp.temporary,
            )
            .await;
        if self.exec(&create_temporary_table_query).await.is_none() {
            return Ok(SuccessTuple::fail(format!(
                "Failed to deduplicate table {pipe_table_name}."
            )));
        }
        let results = self.exec_queries(&alter_queries, true, true).await;
        let mut fail_query = None;
        for (i, query) in alter_queries.iter().enumerate() {
            if results.get(i).is_none_or(Option::is_none) {
                fail_query = Some(query.clone());
                break;
            }
        }
        if let Some(fq) = fail_query {
            return Ok(SuccessTuple::fail(format!(
                "Failed to execute query:\n{fq}"
            )));
        }
        let new_rowcount = self
            .value(&get_rowcount_query)
            .await
            .and_then(|v| v.as_i64());
        let mut msg = format!("Successfully deduplicated table {pipe_table_name}");
        if Some(old_rowcount) != new_rowcount {
            msg.push_str(&format!(
                "\nfrom {} to {} rows",
                fmt_thousands(old_rowcount),
                new_rowcount
                    .map(fmt_thousands)
                    .unwrap_or_else(|| "None".into())
            ));
        }
        msg.push('.');
        Ok(SuccessTuple::ok(msg))
    }

    /// The `DateArg` for a bound (exposed for tests).
    pub fn date_arg(bound: Option<&DateBound>) -> DateArg {
        date_arg(bound)
    }
}

/// `bound - interval` (datetimes by a duration, integers by an integer).
pub fn subtract_interval(bound: DateBound, interval: ChunkInterval) -> DateBound {
    match (bound, interval) {
        (DateBound::Int(i), ChunkInterval::Int(n)) => DateBound::Int(i - n),
        (DateBound::Int(i), ChunkInterval::Delta(d)) => DateBound::Int(i - d.num_minutes()),
        (DateBound::Datetime(dt), ChunkInterval::Delta(d)) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc - d,
            tz: dt.tz,
        }),
        (DateBound::Datetime(dt), ChunkInterval::Int(n)) => DateBound::Datetime(DateTimeValue {
            utc: dt.utc - Duration::minutes(n),
            tz: dt.tz,
        }),
    }
}

/// Python's `f"{n:,}"`.
pub fn fmt_thousands(n: i64) -> String {
    let s = n.unsigned_abs().to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 { format!("-{out}") } else { out }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn thousands() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(1234567), "1,234,567");
        assert_eq!(fmt_thousands(-1000), "-1,000");
    }

    #[test]
    fn chunk_bounds_ints() {
        let b = get_chunk_bounds(
            Some(DateBound::Int(0)),
            Some(DateBound::Int(10)),
            true,
            ChunkInterval::Int(4),
            false,
            None,
            None,
        )
        .unwrap();
        assert_eq!(
            b,
            vec![
                (Some(DateBound::Int(0)), Some(DateBound::Int(4))),
                (Some(DateBound::Int(4)), Some(DateBound::Int(8))),
                (Some(DateBound::Int(8)), Some(DateBound::Int(10))),
            ]
        );
    }
}
