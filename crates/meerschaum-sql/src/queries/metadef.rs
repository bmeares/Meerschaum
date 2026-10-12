//! The SQL fetch metadefinition: `get_pipe_query` and `get_pipe_metadef`
//! (`meerschaum/connectors/sql/_fetch.py:88-410`).

use chrono::{DateTime, TimeDelta, Utc};
use indexmap::IndexMap;

use crate::dateadd::{DateArg, Number, dateadd_str};
use crate::dtypes::{get_db_type_from_pd_type, precision_unit_alias, precision_unit_scalar};
use crate::error::Result;
use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use crate::pipe::{Interval, PipeSpec, total_seconds};
use crate::pytext::{PyDateTime, dedent, py_strip};
use crate::queries::cte::{ColsToSelect, format_cte_subquery, wrap_query_with_cte};
use crate::value::PyValue;
use crate::where_clause::build_where;

/// `get_pipe_query(pipe)`: the first of `fetch.definition`, `definition`, `query`, `sql`,
/// dedented and stripped. `{{ Pipe(...) }}` substitution happens before this (core templating).
pub fn get_pipe_query(pipe: &PipeSpec) -> Option<String> {
    let truthy_str = |v: Option<&PyValue>| -> Option<String> {
        v.filter(|v| v.is_truthy()).map(PyValue::py_str)
    };
    let definition = truthy_str(pipe.param("fetch").and_then(|f| f.get("definition")))
        .or_else(|| truthy_str(pipe.param("definition")))
        .or_else(|| truthy_str(pipe.param("query")))
        .or_else(|| truthy_str(pipe.param("sql")))?;
    Some(dedent(py_strip(&definition)))
}

/// A `begin`/`end` bound for the metadefinition.
#[derive(Debug, Clone, PartialEq)]
pub enum Bound {
    /// `''`: use the sync time minus the backtrack interval (the `begin` default).
    Default,
    /// `None`: unbounded.
    None,
    /// A `datetime`.
    DateTime(PyDateTime),
    /// An integer-axis value.
    Int(i64),
    /// A raw string (`'now'` or a timestamp / SQL expression).
    Str(String),
}

impl Bound {
    fn is_unset(&self) -> bool {
        matches!(self, Bound::Default | Bound::None)
    }

    fn to_date_arg(&self) -> DateArg {
        match self {
            Bound::Default | Bound::None => DateArg::None,
            Bound::DateTime(d) => DateArg::DateTime(*d),
            Bound::Int(i) => DateArg::Int(*i),
            Bound::Str(s) => DateArg::Str(s.clone()),
        }
    }

    fn ge(&self, other: &Bound) -> Option<bool> {
        match (self, other) {
            (Bound::Int(a), Bound::Int(b)) => Some(a >= b),
            (Bound::DateTime(a), Bound::DateTime(b)) => Some(match (a, b) {
                (PyDateTime::Aware(x), PyDateTime::Aware(y)) => x >= y,
                _ => a.naive_local() >= b.naive_local(),
            }),
            (Bound::Str(a), Bound::Str(b)) => Some(a >= b),
            _ => None,
        }
    }
}

/// The parent pipe's attributes used for predicate pushdown (`pipe.parent`).
#[derive(Debug, Clone, PartialEq)]
pub struct ParentSpec {
    /// `parent.target`.
    pub target: String,
    /// `parent.instance_connector.get_pipe_schema(parent)` (`None` for non-SQL instances).
    pub schema: Option<String>,
    /// `parent.columns.get('datetime')`.
    pub dt_col: Option<String>,
    /// `parent.dtypes.get(dt_col, 'datetime')`.
    pub dt_typ: Option<String>,
    /// `parent.precision['unit']`.
    pub precision_unit: String,
}

/// Arguments of `get_pipe_metadef(pipe, params, begin, end, check_existing)`.
#[derive(Debug, Clone)]
pub struct MetadefArgs<'a> {
    /// The pipe (its definition comes from [`get_pipe_query`]).
    pub pipe: &'a PipeSpec,
    /// `params`.
    pub params: Option<&'a IndexMap<String, PyValue>>,
    /// `begin` (default [`Bound::Default`]).
    pub begin: Bound,
    /// `end` (default [`Bound::None`]).
    pub end: Bound,
    /// `check_existing`.
    pub check_existing: bool,
    /// `pipe.get_sync_time()`, used when `begin` is [`Bound::Default`].
    pub sync_time: Bound,
    /// `pipe.parent`, if any.
    pub parent: Option<ParentSpec>,
}

impl<'a> MetadefArgs<'a> {
    /// Python's defaults: `begin=''`, `end=None`, `check_existing=True`, no sync time, no parent.
    pub fn new(pipe: &'a PipeSpec) -> Self {
        MetadefArgs {
            pipe,
            params: None,
            begin: Bound::Default,
            end: Bound::None,
            check_existing: true,
            sync_time: Bound::None,
            parent: None,
        }
    }
}

/// `get_current_timestamp(unit, _now=dt, as_int=True)`: a datetime as an integer epoch.
///
/// PARITY: for nanosecond precision Python ignores a `datetime` `_now` and returns
/// `time.time_ns()` (`utils/dtypes/__init__.py:1226`); reproduced here.
pub fn datetime_to_epoch_int(dt: &PyDateTime, precision_unit: &str) -> i64 {
    let unit = precision_unit_alias(precision_unit)
        .map(str::to_string)
        .unwrap_or_else(|| precision_unit.to_string());
    if unit == "nanosecond" {
        return Utc::now().timestamp_nanos_opt().unwrap_or(0);
    }
    let Some(scalar) = precision_unit_scalar(&unit) else {
        return 0;
    };
    let delta_us: i64 = match unit.as_str() {
        "microsecond" => 1,
        "millisecond" => 1_000,
        "second" => 1_000_000,
        "minute" => 60_000_000,
        "hour" => 3_600_000_000,
        "day" => 86_400_000_000,
        _ => 1,
    };
    // `round_time(dt, delta, 'down')` on wall-clock time, then `coerce_timezone` (naive = UTC).
    let local = dt.naive_local();
    let min = chrono::NaiveDate::from_ymd_opt(1, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .expect("valid");
    let total_us = (local - min).num_microseconds().unwrap_or(0);
    let rounded_us = total_us.div_euclid(delta_us) * delta_us;
    let rounded_local = local + TimeDelta::microseconds(rounded_us - total_us);
    let utc: DateTime<Utc> = match dt {
        PyDateTime::Naive(_) => rounded_local.and_utc(),
        PyDateTime::Aware(a) => rounded_local
            .and_local_timezone(*a.offset())
            .single()
            .expect("fixed offset")
            .with_timezone(&Utc),
    };
    let epoch_us = utc.timestamp_micros();
    let ts = epoch_us as f64 / 1e6;
    (ts * scalar.as_f64()) as i64
}

fn bound_to_int(b: &Bound, unit: &str) -> Bound {
    match b {
        Bound::DateTime(dt) => Bound::Int(datetime_to_epoch_int(dt, unit)),
        other => other.clone(),
    }
}

/// `get_pipe_metadef(...)`: the fetch query wrapping the pipe's definition in
/// `WITH "definition" AS (...)` with begin/end/backtrack bounds and `params`.
///
/// Returns `Ok(None)` when the pipe has no definition (Python raises `ValueError`).
pub fn get_pipe_metadef(flavor: Flavor, args: &MetadefArgs<'_>) -> Result<Option<String>> {
    let pipe = args.pipe;
    let parent = args.parent.as_ref();
    let parent_dt_col = parent.and_then(|p| p.dt_col.clone());
    let parent_dt_typ = parent_dt_col.as_ref().map(|_| {
        parent
            .and_then(|p| p.dt_typ.clone())
            .unwrap_or_else(|| "datetime".to_string())
    });
    let mut dt_col = parent_dt_col.clone().or_else(|| pipe.column("datetime"));
    let dt_typ = parent_dt_typ.clone().unwrap_or_else(|| {
        dt_col
            .as_ref()
            .and_then(|c| pipe.dtypes().get(c).cloned())
            .unwrap_or_else(|| "datetime".to_string())
    });
    let db_dt_typ = if dt_typ.is_empty() {
        None
    } else {
        Some(get_db_type_from_pd_type(&dt_typ, Some(flavor)))
    };
    let precision_unit = if parent_dt_typ.is_some() {
        parent.map(|p| p.precision_unit.clone()).unwrap_or_default()
    } else {
        pipe.precision_unit()
    };
    let is_guess = dt_col.is_none();
    if is_guess {
        dt_col = pipe.guess_datetime();
    }
    let dt_name = dt_col.as_deref().map(|c| sql_item_name(c, flavor, None));

    let mut begin = args.begin.clone();
    let mut end = args.end.clone();
    if (!begin.is_unset() || end != Bound::None) && is_guess {
        if dt_col.is_none() {
            tracing::warn!("Unable to determine a datetime column. Ignoring begin and end...");
            begin = Bound::Default;
            end = Bound::None;
        } else {
            tracing::warn!(
                "A datetime wasn't specified. Using column \"{}\" for datetime bounds...",
                dt_col.clone().unwrap_or_default()
            );
        }
    }

    let apply_backtrack = begin == Bound::Default && args.check_existing;
    let backtrack_interval = pipe.get_backtrack_interval(args.check_existing);
    let mut btm: i64 = match backtrack_interval {
        Interval::Duration(d) => (total_seconds(d) / 60.0) as i64,
        Interval::Int(i) => i,
    };
    if begin == Bound::Default {
        begin = args.sync_time.clone();
    }

    let is_int_typ = dt_typ.to_lowercase().contains("int");
    if is_int_typ {
        begin = bound_to_int(&begin, &precision_unit);
        end = bound_to_int(&end, &precision_unit);
        if let Interval::Duration(d) = backtrack_interval {
            let true_unit = precision_unit_alias(&precision_unit)
                .map(str::to_string)
                .unwrap_or_else(|| precision_unit.clone());
            if let Some(scalar) = precision_unit_scalar(&true_unit) {
                btm = (total_seconds(d) * scalar.as_f64()) as i64;
            }
        }
    }

    if !begin.is_unset() && end != Bound::None && begin.ge(&end) == Some(true) {
        begin = Bound::None;
    }

    let mut begin_da: Option<String> = None;
    let mut end_da: Option<String> = None;
    if dt_name.is_some() {
        if !begin.is_unset() {
            let datepart = if matches!(begin, Bound::Int(_)) {
                None
            } else {
                Some("minute")
            };
            let number = if apply_backtrack { -btm } else { 0 };
            begin_da = Some(dateadd_str(
                Some(flavor),
                datepart,
                Number::Int(number),
                &begin.to_date_arg(),
                db_dt_typ.as_deref(),
            )?);
        }
        if end != Bound::None {
            let datepart = if matches!(end, Bound::Int(_)) {
                None
            } else {
                Some("minute")
            };
            end_da = Some(dateadd_str(
                Some(flavor),
                datepart,
                Number::Int(0),
                &end.to_date_arg(),
                db_dt_typ.as_deref(),
            )?);
        }
    }

    let definition_name = sql_item_name("definition", flavor, None);
    let Some(mut definition) = get_pipe_query(pipe) else {
        return Ok(None);
    };

    // Attempt to push down the predicate onto the parent's table.
    let mut handled_bounding = false;
    if let (Some(parent), Some(parent_dt_col), Some(parent_dt_typ)) =
        (parent, parent_dt_col.as_ref(), parent_dt_typ.as_ref())
    {
        if !begin.is_unset() || end != Bound::None {
            let parent_dt_name = sql_item_name(parent_dt_col, flavor, None);
            let parent_db_dt_typ = get_db_type_from_pd_type(parent_dt_typ, Some(flavor));
            let (mut p_begin, mut p_end, mut p_btm) = (begin.clone(), end.clone(), btm);
            if parent_dt_typ.to_lowercase().contains("int") {
                let p_precision_unit = parent.precision_unit.clone();
                let scalar_for = |unit: &str| {
                    precision_unit_scalar(precision_unit_alias(unit).unwrap_or(unit))
                        .map(|s| s.as_f64())
                        .unwrap_or(1.0)
                };
                let to_dt = |b: &Bound| -> Option<PyDateTime> {
                    match b {
                        Bound::DateTime(d) => Some(*d),
                        Bound::Int(i) => {
                            let secs = *i as f64 / scalar_for(&precision_unit);
                            let us = (secs * 1e6).round() as i64;
                            DateTime::<Utc>::from_timestamp_micros(us).map(PyDateTime::utc)
                        }
                        _ => None,
                    }
                };
                if let Some(d) = to_dt(&begin) {
                    p_begin = Bound::Int(datetime_to_epoch_int(&d, &p_precision_unit));
                }
                if let Some(d) = to_dt(&end) {
                    p_end = Bound::Int(datetime_to_epoch_int(&d, &p_precision_unit));
                }
                if let Interval::Duration(d) = backtrack_interval {
                    p_btm = (total_seconds(d) * scalar_for(&p_precision_unit)) as i64;
                }
            }

            // PARITY: Python always passes `datepart='minute'` here, even for integer bounds.
            let parent_begin_da = if p_begin.is_unset() {
                None
            } else {
                Some(dateadd_str(
                    Some(flavor),
                    Some("minute"),
                    Number::Int(if apply_backtrack { -p_btm } else { 0 }),
                    &p_begin.to_date_arg(),
                    Some(&parent_db_dt_typ),
                )?)
            };
            let parent_end_da = if p_end == Bound::None {
                None
            } else {
                Some(dateadd_str(
                    Some(flavor),
                    Some("minute"),
                    Number::Int(0),
                    &p_end.to_date_arg(),
                    Some(&parent_db_dt_typ),
                )?)
            };

            let parent_target = parent.target.as_str();
            let parent_item_name = sql_item_name(parent_target, flavor, None);
            let parent_item_name_full =
                sql_item_name(parent_target, flavor, parent.schema.as_deref());
            if definition.contains(&parent_item_name)
                || definition.contains(&parent_item_name_full)
                || definition.contains(parent_target)
            {
                let pushdown_cte_name = sql_item_name("_mrsm_pushdown", flavor, None);
                let mut pushdown_where = String::new();
                if let Some(b) = &parent_begin_da {
                    pushdown_where.push_str(&format!("\n    {parent_dt_name} >= {b}"));
                }
                if parent_begin_da.is_some() && parent_end_da.is_some() {
                    pushdown_where.push_str("\n    AND");
                }
                if let Some(e) = &parent_end_da {
                    pushdown_where.push_str(&format!("\n    {parent_dt_name} < {e}"));
                }
                let pushdown_query =
                    format!("SELECT *\nFROM {parent_item_name_full}\nWHERE {pushdown_where}");
                let mut new_body = definition.clone();
                let mut parent_found = false;
                for pattern in [
                    parent_item_name_full.as_str(),
                    parent_item_name.as_str(),
                    parent_target,
                ] {
                    if new_body.contains(pattern) {
                        new_body = new_body.replace(pattern, &pushdown_cte_name);
                        parent_found = true;
                    }
                }
                if parent_found {
                    definition =
                        wrap_query_with_cte(&pushdown_query, &new_body, flavor, "_mrsm_pushdown")?;
                    handled_bounding = true;
                }
            }
        }
    }

    // TODO(core-merge): the experimental `join_fetch` path (`_join_fetch_query`) is not ported;
    // `system.experimental.join_fetch` defaults to `False`.
    let mut meta_def = format_cte_subquery(
        &definition,
        flavor,
        "definition",
        &ColsToSelect::Raw("*".to_string()),
    )?;

    let lower = meta_def.to_lowercase();
    // `meta_def.lower()[meta_def.lower().rfind('definition'):]` (`-1` slices the last char).
    let tail_start = lower
        .rfind("definition")
        .or_else(|| lower.char_indices().last().map(|(i, _)| i))
        .unwrap_or(0);
    let mut has_where = lower[tail_start..].contains("where");
    if dt_name.is_some() && (begin_da.is_some() || end_da.is_some()) && !handled_bounding {
        let definition_dt_name =
            format!("{definition_name}.{}", dt_name.clone().unwrap_or_default());
        meta_def.push('\n');
        meta_def.push_str(if has_where { "AND" } else { "WHERE" });
        meta_def.push(' ');
        has_where = true;
        if let Some(b) = &begin_da {
            meta_def.push_str(&format!("\n    {definition_dt_name}\n    >=\n    {b}\n"));
        }
        if begin_da.is_some() && end_da.is_some() {
            meta_def.push_str("    AND");
        }
        if let Some(e) = &end_da {
            meta_def.push_str(&format!("\n    {definition_dt_name}\n    <\n    {e}\n"));
        }
    }

    if let Some(params) = args.params {
        let params_where = build_where(params, flavor, false);
        meta_def.push_str("\n    ");
        meta_def.push_str(if has_where { "AND" } else { "WHERE" });
        meta_def.push_str("    ");
        meta_def.push_str(&params_where);
    }

    Ok(Some(crate::pytext::py_rstrip(&meta_def).to_string()))
}
