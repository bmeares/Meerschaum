//! Native range-partition management (`meerschaum/connectors/sql/_partition.py`).

use chrono::{Datelike, NaiveDate, NaiveDateTime, TimeDelta};

use crate::dtypes::get_db_type_from_pd_type;
use crate::flavor::Flavor;
use crate::ident::{sql_item_name, truncate_item_name};
use crate::pipe::{ChunkInterval, ChunkIntervalArg, SqlPipe};
use crate::queries::ConnectorCtx;
use meerschaum_core::pyfmt::PyDateTime;

/// `_MAX_PARTITIONS_PER_SYNC` (`_partition.py:43`); overridable via
/// `system.connectors.sql.instance.max_partitions_per_sync` (see [`max_partitions_per_sync`]).
pub const MAX_PARTITIONS_PER_SYNC: usize = 10_000;

/// `_max_partitions_per_sync()` (`_partition.py:45`): the configured per-sync cap, else
/// [`MAX_PARTITIONS_PER_SYNC`] when the key is missing or not an integer.
///
/// A cap of zero or below behaves like one partition per sync, as in Python.
pub fn max_partitions_per_sync(config: &meerschaum_core::Config) -> usize {
    let opts = meerschaum_core::config::GetOptions {
        warn: false,
        ..Default::default()
    };
    let (valid, value) = config.get_with(
        &[
            "system",
            "connectors",
            "sql",
            "instance",
            "max_partitions_per_sync",
        ],
        opts,
    );
    if !valid || value.is_null() {
        return MAX_PARTITIONS_PER_SYNC;
    }
    match meerschaum_core::pyfmt::py_int(&value) {
        Some(n) => usize::try_from(n).unwrap_or(0),
        None => MAX_PARTITIONS_PER_SYNC,
    }
}

/// A partition boundary: a datetime or an integer-axis value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionValue {
    /// A `datetime`.
    DateTime(PyDateTime),
    /// An `int`.
    Int(i64),
}

impl PartitionValue {
    fn le(&self, other: &PartitionValue) -> bool {
        match (self, other) {
            (PartitionValue::Int(a), PartitionValue::Int(b)) => a <= b,
            (PartitionValue::DateTime(a), PartitionValue::DateTime(b)) => match (a, b) {
                (PyDateTime::Aware(x), PyDateTime::Aware(y)) => x <= y,
                _ => a.naive_local() <= b.naive_local(),
            },
            _ => false,
        }
    }
}

fn py_datetime_min() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(1, 1, 1)
        .expect("valid")
        .and_hms_opt(0, 0, 0)
        .expect("valid")
}

fn py_datetime_max() -> NaiveDateTime {
    NaiveDate::from_ymd_opt(9999, 12, 31)
        .expect("valid")
        .and_hms_micro_opt(23, 59, 59, 999_999)
        .expect("valid")
}

/// `epoch + delta` with Python's `OverflowError` outside years 1..=9999 reported as `None`.
fn py_add(base: NaiveDateTime, delta: TimeDelta) -> Option<NaiveDateTime> {
    let out = base.checked_add_signed(delta)?;
    if out.year() < 1 || out.year() > 9999 {
        return None;
    }
    Some(out)
}

fn with_tz_of(template: &PyDateTime, naive: NaiveDateTime) -> PyDateTime {
    match template {
        PyDateTime::Naive(_) => PyDateTime::Naive(naive),
        PyDateTime::Aware(a) => PyDateTime::Aware(
            naive
                .and_local_timezone(*a.offset())
                .single()
                .expect("fixed offsets are unambiguous"),
        ),
    }
}

/// `_partition_bounds(value, interval)`: the epoch-aligned `[lo, hi)` containing `value`.
pub fn partition_bounds(
    value: PartitionValue,
    interval: ChunkInterval,
) -> (PartitionValue, PartitionValue) {
    match (value, interval) {
        (PartitionValue::Int(v), ChunkInterval::Int(i)) => {
            let n = v.div_euclid(i);
            let lo = n * i;
            (PartitionValue::Int(lo), PartitionValue::Int(lo + i))
        }
        (PartitionValue::DateTime(dt), ChunkInterval::Delta(delta)) => {
            // The epoch shares `value`'s tzinfo, so the arithmetic happens on wall-clock time.
            let epoch = NaiveDate::from_ymd_opt(1970, 1, 1)
                .expect("valid")
                .and_hms_opt(0, 0, 0)
                .expect("valid");
            let local = dt.naive_local();
            let value_us = (local - epoch).num_microseconds().unwrap_or(i64::MAX);
            let interval_us = delta.num_microseconds().unwrap_or(i64::MAX).max(1);
            let n = value_us.div_euclid(interval_us);
            let lo = n
                .checked_mul(interval_us)
                .and_then(|us| py_add(epoch, TimeDelta::microseconds(us)))
                .unwrap_or_else(py_datetime_min);
            let hi = py_add(lo, delta).unwrap_or_else(py_datetime_max);
            (
                PartitionValue::DateTime(with_tz_of(&dt, lo)),
                PartitionValue::DateTime(with_tz_of(&dt, hi)),
            )
        }
        (PartitionValue::Int(v), ChunkInterval::Delta(delta)) => {
            // An int value with a duration: Python would raise; treat the duration as minutes.
            partition_bounds(
                PartitionValue::Int(v),
                ChunkInterval::Int(delta.num_minutes().max(1)),
            )
        }
        (PartitionValue::DateTime(dt), ChunkInterval::Int(i)) => {
            partition_bounds(PartitionValue::DateTime(dt), ChunkInterval::minutes(i))
        }
    }
}

/// `_partition_literal(value)`: the SQL literal for a boundary.
pub fn partition_literal(flavor: Flavor, value: PartitionValue) -> String {
    match value {
        PartitionValue::DateTime(dt) => {
            // MySQL/MariaDB store datetimes naive (as UTC); PostgreSQL keeps the offset.
            if flavor.mysql_partition() {
                let naive = dt.to_utc().strip_tz();
                format!("'{}'", naive.strftime_full())
            } else {
                format!("'{}'", dt.isoformat())
            }
        }
        PartitionValue::Int(i) => i.to_string(),
    }
}

/// `_partition_name(pipe, lo)`: the (truncated) child-partition name.
pub fn partition_name(flavor: Flavor, target: &str, lo: PartitionValue) -> String {
    let suffix = match lo {
        PartitionValue::DateTime(dt) => dt.strftime_compact(),
        PartitionValue::Int(i) => i.to_string().replace('-', "n"),
    };
    truncate_item_name(&format!("{target}_p_{suffix}"), flavor)
}

/// `_get_partition_ranges_for_df` given the frame's min/max datetime values.
///
/// Returns the ranges and whether the `max_parts` cap was hit (Python warns).
pub fn partition_ranges(
    min_val: PartitionValue,
    max_val: PartitionValue,
    interval: ChunkInterval,
    max_parts: usize,
) -> (Vec<(PartitionValue, PartitionValue)>, bool) {
    let mut ranges = Vec::new();
    let (lo, _) = partition_bounds(min_val, interval);
    let mut cursor = lo;
    while cursor.le(&max_val) {
        let (lo_c, hi_c) = partition_bounds(cursor, interval);
        ranges.push((lo_c, hi_c));
        if hi_c == cursor {
            break;
        }
        cursor = hi_c;
        if ranges.len() >= max_parts {
            tracing::warn!(
                "Reached the {max_parts}-partition limit; consider a larger `chunk_minutes`."
            );
            return (ranges, true);
        }
    }
    (ranges, false)
}

/// `_should_partition(pipe)`.
pub fn should_partition(flavor: Flavor, pipe: &SqlPipe) -> bool {
    flavor.partitionable() && pipe.column("datetime").is_some() && pipe.hypertable()
}

/// `_get_partition_column(pipe)`.
pub fn get_partition_column(flavor: Flavor, pipe: &SqlPipe) -> Option<String> {
    if !should_partition(flavor, pipe) {
        return None;
    }
    pipe.column("datetime")
}

/// `_get_initial_partition_bounds`: `(partition_name, upper_bound_literal)` for MySQL inline
/// partitions.
pub fn get_initial_partition_bounds(
    flavor: Flavor,
    target: &str,
    ranges: &[(PartitionValue, PartitionValue)],
) -> Vec<(String, String)> {
    ranges
        .iter()
        .map(|(lo, hi)| {
            (
                partition_name(flavor, target, *lo),
                partition_literal(flavor, *hi),
            )
        })
        .collect()
}

/// `_partition_function_name(pipe)` (MSSQL).
pub fn partition_function_name(flavor: Flavor, target: &str) -> String {
    truncate_item_name(&format!("pf_{target}"), flavor)
}

/// `_partition_scheme_name(pipe)` (MSSQL).
pub fn partition_scheme_name(flavor: Flavor, target: &str) -> String {
    truncate_item_name(&format!("ps_{target}"), flavor)
}

/// The partition-count query of `_get_partition_count` (`_partition.py:126`).
///
/// `database` is the MySQL database name (`schema or self.database`).
pub fn get_partition_count_query(
    ctx: &ConnectorCtx,
    pipe: &SqlPipe,
    database: Option<&str>,
) -> Option<String> {
    let flavor = ctx.flavor;
    let schema = ctx.get_pipe_schema(pipe);
    if flavor.pg_partition() {
        let full = sql_item_name(&pipe.target, flavor, schema.as_deref()).replace('\'', "''");
        return Some(format!(
            "SELECT COUNT(*) FROM pg_inherits WHERE inhparent = '{full}'::regclass"
        ));
    }
    if flavor.mysql_partition() {
        let db_name = schema.or_else(|| database.map(str::to_string))?;
        let clean_db = db_name.replace('\'', "''");
        let clean_target = pipe.target.replace('\'', "''");
        return Some(format!(
            "SELECT COUNT(*) FROM information_schema.PARTITIONS\nWHERE TABLE_SCHEMA = '{clean_db}' AND TABLE_NAME = '{clean_target}'\n  AND PARTITION_NAME IS NOT NULL"
        ));
    }
    if flavor.mssql_partition() {
        let func_name = partition_function_name(flavor, &pipe.target).replace('\'', "''");
        return Some(format!(
            "SELECT COUNT(*) FROM sys.partition_functions pf\nJOIN sys.partition_range_values prv ON pf.function_id = prv.function_id\nWHERE pf.name = '{func_name}'"
        ));
    }
    None
}

/// `_get_chunk_count_timescaledb`'s query (`_partition.py:171`).
pub fn get_timescaledb_chunk_count_query(ctx: &ConnectorCtx, pipe: &SqlPipe) -> String {
    let schema = ctx.get_pipe_schema(pipe);
    let clean_target = pipe.target.replace('\'', "''");
    let schema_clause = match schema.filter(|s| !s.is_empty()) {
        Some(s) => format!(" AND hypertable_schema = '{}'", s.replace('\'', "''")),
        None => String::new(),
    };
    format!(
        "SELECT COUNT(*) FROM timescaledb_information.chunks\nWHERE hypertable_name = '{clean_target}'{schema_clause}"
    )
}

/// The `pg_partitioned_table` guard of `_create_missing_partitions_pg` (`_partition.py:412`).
pub fn get_pg_is_partitioned_query(ctx: &ConnectorCtx, pipe: &SqlPipe) -> String {
    let schema = ctx.get_pipe_schema(pipe);
    let parent_name = sql_item_name(&pipe.target, ctx.flavor, schema.as_deref());
    format!(
        "SELECT 1 FROM pg_partitioned_table WHERE partrelid = '{}'::regclass",
        parent_name.replace('\'', "''")
    )
}

/// The `CREATE TABLE IF NOT EXISTS ... PARTITION OF` queries of `_create_missing_partitions_pg`.
pub fn get_pg_partition_creation_queries(
    ctx: &ConnectorCtx,
    pipe: &SqlPipe,
    ranges: &[(PartitionValue, PartitionValue)],
) -> Vec<String> {
    let flavor = ctx.flavor;
    let schema = ctx.get_pipe_schema(pipe);
    let parent_name = sql_item_name(&pipe.target, flavor, schema.as_deref());
    ranges
        .iter()
        .map(|(lo, hi)| {
            let part_name = sql_item_name(
                &partition_name(flavor, &pipe.target, *lo),
                flavor,
                schema.as_deref(),
            );
            format!(
                "CREATE TABLE IF NOT EXISTS {part_name} PARTITION OF {parent_name}\nFOR VALUES FROM ({}) TO ({})",
                partition_literal(flavor, *lo),
                partition_literal(flavor, *hi)
            )
        })
        .collect()
}

/// `_get_mysql_max_partition_bound`'s query; `None` when no database name is known.
pub fn get_mysql_max_partition_bound_query(
    ctx: &ConnectorCtx,
    pipe: &SqlPipe,
    database: Option<&str>,
) -> Option<String> {
    let db_name = ctx
        .get_pipe_schema(pipe)
        .filter(|s| !s.is_empty())
        .or_else(|| database.map(str::to_string))?;
    let clean_db = db_name.replace('\'', "''");
    let clean_target = pipe.target.replace('\'', "''");
    Some(format!(
        "SELECT PARTITION_DESCRIPTION\nFROM information_schema.PARTITIONS\nWHERE TABLE_SCHEMA = '{clean_db}' AND TABLE_NAME = '{clean_target}'\n  AND PARTITION_DESCRIPTION IS NOT NULL AND PARTITION_DESCRIPTION != 'MAXVALUE'"
    ))
}

/// Parse `PARTITION_DESCRIPTION` values into the highest boundary (`_partition.py:479-496`).
pub fn parse_mysql_max_partition_bound(
    descriptions: &[String],
    is_int_axis: bool,
) -> Option<PartitionValue> {
    let mut max_bound: Option<PartitionValue> = None;
    for raw in descriptions {
        let text = meerschaum_core::pyfmt::py_strip(raw)
            .trim_matches('\'')
            .trim_matches('"')
            .to_string();
        let bound = if is_int_axis {
            match meerschaum_core::pyfmt::parse_py_int(&text) {
                Some(i) => PartitionValue::Int(i),
                None => continue,
            }
        } else {
            match PyDateTime::parse_isoformat(&text) {
                Some(dt) => PartitionValue::DateTime(dt),
                None => continue,
            }
        };
        match &max_bound {
            Some(m) if !m.le(&bound) || *m == bound => {}
            _ => max_bound = Some(bound),
        }
    }
    max_bound
}

fn add_interval(value: PartitionValue, interval: ChunkInterval) -> PartitionValue {
    match (value, interval) {
        (PartitionValue::Int(v), ChunkInterval::Int(i)) => PartitionValue::Int(v + i),
        (PartitionValue::DateTime(dt), ChunkInterval::Delta(d)) => {
            let naive = py_add(dt.naive_local(), d).unwrap_or_else(py_datetime_max);
            PartitionValue::DateTime(with_tz_of(&dt, naive))
        }
        (PartitionValue::Int(v), ChunkInterval::Delta(d)) => {
            PartitionValue::Int(v + d.num_minutes())
        }
        (PartitionValue::DateTime(dt), ChunkInterval::Int(i)) => {
            add_interval(PartitionValue::DateTime(dt), ChunkInterval::minutes(i))
        }
    }
}

/// The `ALTER TABLE ... ADD PARTITION` queries of `_create_missing_partitions_mysql`.
///
/// `max_val` is the frame's maximum datetime; it is normalized to naive UTC like Python.
pub fn get_mysql_add_partition_queries(
    ctx: &ConnectorCtx,
    pipe: &SqlPipe,
    max_bound: PartitionValue,
    max_val: PartitionValue,
    interval: ChunkInterval,
    max_parts: usize,
) -> Vec<String> {
    let flavor = ctx.flavor;
    let max_val = match (max_bound, max_val) {
        (PartitionValue::DateTime(_), PartitionValue::DateTime(dt)) => {
            PartitionValue::DateTime(if dt.is_aware() {
                dt.to_utc().strip_tz()
            } else {
                dt
            })
        }
        (PartitionValue::DateTime(_), PartitionValue::Int(_)) => return Vec::new(),
        (PartitionValue::Int(_), v) => v,
    };
    let parent_name = sql_item_name(&pipe.target, flavor, ctx.get_pipe_schema(pipe).as_deref());
    let mut queries = Vec::new();
    let mut cursor = max_bound;
    while cursor.le(&max_val) {
        let lo = cursor;
        let hi = add_interval(lo, interval);
        let part_name = sql_item_name(&partition_name(flavor, &pipe.target, lo), flavor, None);
        queries.push(format!(
            "ALTER TABLE {parent_name} ADD PARTITION (PARTITION {part_name} VALUES LESS THAN ({}))",
            partition_literal(flavor, hi)
        ));
        if hi == cursor {
            break;
        }
        cursor = hi;
        if queries.len() >= max_parts {
            break;
        }
    }
    queries
}

/// `_get_mssql_partition_creation_queries`: the partition function and scheme.
///
/// `boundaries` are the `lo` grid points (`_get_partition_boundary_values`).
pub fn get_mssql_partition_creation_queries(
    pipe: &SqlPipe,
    boundaries: &[PartitionValue],
) -> Vec<String> {
    let flavor = Flavor::Mssql;
    let Some(dt_col) = pipe.column("datetime") else {
        return Vec::new();
    };
    let func_name = partition_function_name(flavor, &pipe.target);
    let scheme_name = partition_scheme_name(flavor, &pipe.target);
    let func_item = sql_item_name(&func_name, flavor, None);
    let scheme_item = sql_item_name(&scheme_name, flavor, None);
    let clean_func = func_name.replace('\'', "''");
    let clean_scheme = scheme_name.replace('\'', "''");
    let dt_typ = pipe
        .dtype(&dt_col)
        .unwrap_or_else(|| "datetime".to_string());
    let dt_db_type = get_db_type_from_pd_type(&dt_typ, Some(flavor));
    let values_clause = boundaries
        .iter()
        .map(|b| partition_literal(flavor, *b))
        .collect::<Vec<_>>()
        .join(", ");
    vec![
        format!(
            "IF NOT EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{clean_func}')\nCREATE PARTITION FUNCTION {func_item} ({dt_db_type})\nAS RANGE RIGHT FOR VALUES ({values_clause})"
        ),
        format!(
            "IF NOT EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{clean_scheme}')\nCREATE PARTITION SCHEME {scheme_item}\nAS PARTITION {func_item} ALL TO ([PRIMARY])"
        ),
    ]
}

/// `_get_mssql_max_partition_boundary`'s query.
pub fn get_mssql_max_partition_boundary_query(pipe: &SqlPipe) -> String {
    let func_name = partition_function_name(Flavor::Mssql, &pipe.target).replace('\'', "''");
    let cast_type = if pipe.is_int_axis() {
        "BIGINT"
    } else {
        "DATETIMEOFFSET"
    };
    format!(
        "SELECT CONVERT(NVARCHAR(64), MAX(CAST(prv.value AS {cast_type})), 127) AS b\nFROM sys.partition_functions pf\nJOIN sys.partition_range_values prv ON pf.function_id = prv.function_id\nWHERE pf.name = '{func_name}'"
    )
}

/// Parse `_get_mssql_max_partition_boundary`'s result (`_partition.py:665-678`).
pub fn parse_mssql_max_partition_boundary(text: &str, is_int_axis: bool) -> Option<PartitionValue> {
    let text = meerschaum_core::pyfmt::py_strip(text);
    if is_int_axis {
        return meerschaum_core::pyfmt::parse_py_int(text).map(PartitionValue::Int);
    }
    let head: String = text.chars().take(19).collect();
    let naive = NaiveDateTime::parse_from_str(&head, "%Y-%m-%dT%H:%M:%S")
        .or_else(|_| NaiveDateTime::parse_from_str(&head, "%Y-%m-%d %H:%M:%S"))
        .ok()?;
    Some(PartitionValue::DateTime(PyDateTime::utc(naive.and_utc())))
}

/// The `SPLIT RANGE` queries of `_create_missing_partitions_mssql`.
pub fn get_mssql_split_partition_queries(
    pipe: &SqlPipe,
    max_bound: PartitionValue,
    max_val: PartitionValue,
    interval: ChunkInterval,
    max_parts: usize,
) -> Vec<String> {
    let flavor = Flavor::Mssql;
    let max_val = match (max_bound, max_val) {
        (PartitionValue::DateTime(_), PartitionValue::DateTime(dt)) => {
            PartitionValue::DateTime(if dt.is_aware() {
                dt.to_utc()
            } else {
                PyDateTime::utc(dt.naive_local().and_utc())
            })
        }
        (PartitionValue::DateTime(_), PartitionValue::Int(_)) => return Vec::new(),
        (PartitionValue::Int(_), v) => v,
    };
    let func_item = sql_item_name(&partition_function_name(flavor, &pipe.target), flavor, None);
    let scheme_item = sql_item_name(&partition_scheme_name(flavor, &pipe.target), flavor, None);
    let mut queries = Vec::new();
    let mut cursor = add_interval(max_bound, interval);
    let mut count = 0;
    while cursor.le(&max_val) {
        queries.push(format!(
            "ALTER PARTITION SCHEME {scheme_item} NEXT USED [PRIMARY]"
        ));
        queries.push(format!(
            "ALTER PARTITION FUNCTION {func_item}() SPLIT RANGE ({})",
            partition_literal(flavor, cursor)
        ));
        let next = add_interval(cursor, interval);
        if next == cursor {
            break;
        }
        cursor = next;
        count += 1;
        if count >= max_parts {
            break;
        }
    }
    queries
}

/// `_get_partition_cleanup_queries(pipe)`: drop the MSSQL partition scheme then function.
pub fn get_partition_cleanup_queries(flavor: Flavor, pipe: &SqlPipe) -> Vec<String> {
    if !flavor.mssql_partition() || !should_partition(flavor, pipe) {
        return Vec::new();
    }
    let func_name = partition_function_name(flavor, &pipe.target);
    let scheme_name = partition_scheme_name(flavor, &pipe.target);
    let func_item = sql_item_name(&func_name, flavor, None);
    let scheme_item = sql_item_name(&scheme_name, flavor, None);
    let clean_func = func_name.replace('\'', "''");
    let clean_scheme = scheme_name.replace('\'', "''");
    vec![
        format!(
            "IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '{clean_scheme}')\nDROP PARTITION SCHEME {scheme_item}"
        ),
        format!(
            "IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '{clean_func}')\nDROP PARTITION FUNCTION {func_item}"
        ),
    ]
}

/// The TimescaleDB branch of `partition_pipe`: `SELECT set_chunk_time_interval(...)`.
///
/// Fails where Python's `pipe.get_chunk_interval(...)` raises (an invalid `precision`).
pub fn get_set_chunk_time_interval_query(
    ctx: &ConnectorCtx,
    pipe: &SqlPipe,
    new_minutes: i64,
) -> crate::error::Result<String> {
    let pipe_name = sql_item_name(
        &pipe.target,
        ctx.flavor,
        ctx.get_pipe_schema(pipe).as_deref(),
    );
    let regclass_literal = format!("'{}'", pipe_name.replace('\'', "''"));
    let interval =
        pipe.chunk_interval_with(ChunkIntervalArg::Delta(TimeDelta::minutes(new_minutes)))?;
    let chunk_time_interval = match interval {
        ChunkInterval::Int(i) => i.to_string(),
        ChunkInterval::Delta(_) => format!("INTERVAL '{} MINUTES'", interval.whole_minutes()),
    };
    Ok(format!(
        "SELECT set_chunk_time_interval({regclass_literal}, {chunk_time_interval})"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> PartitionValue {
        PartitionValue::DateTime(PyDateTime::parse_isoformat(s).unwrap())
    }

    #[test]
    fn partition_cap_from_config() {
        use meerschaum_core::config::{Config, ConfigOptions};
        let root = std::env::temp_dir().join(format!("_mrsm_rs_cap_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let config_for = |patch: Option<&str>| {
            let mut env = vec![("MRSM_ROOT_DIR".to_string(), root.display().to_string())];
            if let Some(p) = patch {
                env.push(("MRSM_CONFIG".to_string(), p.to_string()));
            }
            let mut opts = ConfigOptions::with_env(env);
            opts.allow_writes = false;
            Config::new(opts).unwrap()
        };
        assert_eq!(
            max_partitions_per_sync(&config_for(None)),
            MAX_PARTITIONS_PER_SYNC
        );
        let patch = |v: &str| {
            format!(
                r#"{{"system": {{"connectors": {{"sql": {{"instance": {{"max_partitions_per_sync": {v}}}}}}}}}}}"#
            )
        };
        assert_eq!(max_partitions_per_sync(&config_for(Some(&patch("7")))), 7);
        assert_eq!(
            max_partitions_per_sync(&config_for(Some(&patch("\"x\"")))),
            MAX_PARTITIONS_PER_SYNC
        );
        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn bounds() {
        let (lo, hi) = partition_bounds(
            dt("2024-01-15T12:00:00+00:00"),
            ChunkInterval::minutes(43200),
        );
        assert_eq!(
            partition_literal(Flavor::Postgresql, lo),
            "'2023-12-19T00:00:00+00:00'"
        );
        assert_eq!(
            partition_literal(Flavor::Mysql, hi),
            "'2024-01-18 00:00:00.000000'"
        );
        assert_eq!(
            partition_bounds(PartitionValue::Int(-5), ChunkInterval::Int(10)),
            (PartitionValue::Int(-10), PartitionValue::Int(0))
        );
        assert_eq!(
            partition_name(Flavor::Postgresql, "t", PartitionValue::Int(-10)),
            "t_p_n10"
        );
    }
}
