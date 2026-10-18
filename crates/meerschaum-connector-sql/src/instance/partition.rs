//! Runtime native range partitioning (`meerschaum/connectors/sql/_partition.py`): creating the
//! partitions a frame needs before insert, partition introspection and `partition_pipe`.

use chrono::TimeDelta;
use meerschaum_connector::DataQuery;
use meerschaum_core::{Map, Pipe, SuccessTuple, Value};
use meerschaum_kernel::Frame;
use meerschaum_kernel::value::Value as KValue;

use crate::flavor::Flavor;
use crate::pipe::{ChunkInterval, ChunkIntervalArg, SqlPipe};
use crate::queries::partition::{
    PartitionValue, get_mssql_max_partition_boundary_query, get_mssql_partition_creation_queries,
    get_mssql_split_partition_queries, get_mysql_add_partition_queries,
    get_mysql_max_partition_bound_query, get_partition_count_query, get_pg_is_partitioned_query,
    get_pg_partition_creation_queries, get_set_chunk_time_interval_query,
    get_timescaledb_chunk_count_query, parse_mssql_max_partition_boundary,
    parse_mysql_max_partition_bound, partition_ranges, partition_scheme_name, should_partition,
};

use super::values::py_datetime;
use super::{SqlConnector, frame_docs, kv_str};
use crate::driver::SqlValue;

/// `_normalize_boundary(val)`: datetimes and integers as partition values; dates become
/// midnight datetimes.
fn partition_value_of(v: &KValue) -> Option<PartitionValue> {
    match v {
        KValue::Datetime(dt) => Some(PartitionValue::DateTime(py_datetime(dt))),
        KValue::Int(i) => Some(PartitionValue::Int(*i)),
        KValue::Float(f) if !f.is_nan() => Some(PartitionValue::Int(*f as i64)),
        KValue::Date(d) => Some(PartitionValue::DateTime(
            meerschaum_core::pyfmt::PyDateTime::Naive(d.and_hms_opt(0, 0, 0).expect("midnight")),
        )),
        _ => None,
    }
}

fn pv_lt(a: &PartitionValue, b: &PartitionValue) -> bool {
    match (a, b) {
        (PartitionValue::Int(x), PartitionValue::Int(y)) => x < y,
        (PartitionValue::DateTime(x), PartitionValue::DateTime(y)) => {
            match (x.is_aware(), y.is_aware()) {
                (true, true) => x.to_utc().naive_local() < y.to_utc().naive_local(),
                _ => x.naive_local() < y.naive_local(),
            }
        }
        _ => false,
    }
}

/// The min and max datetime-axis values of a frame (`series.dropna().min()` / `.max()`).
pub fn frame_axis_bounds(df: &Frame, dt_col: &str) -> Option<(PartitionValue, PartitionValue)> {
    let data = df.data(dt_col).ok()?;
    let mut min: Option<PartitionValue> = None;
    let mut max: Option<PartitionValue> = None;
    for v in data.values() {
        if v.is_na() {
            continue;
        }
        let Some(pv) = partition_value_of(&v) else {
            continue;
        };
        if min.as_ref().is_none_or(|m| pv_lt(&pv, m)) {
            min = Some(pv);
        }
        if max.as_ref().is_none_or(|m| pv_lt(m, &pv)) {
            max = Some(pv);
        }
    }
    Some((min?, max?))
}

impl SqlConnector {
    /// `_should_partition(pipe)`.
    pub fn should_partition(&self, sp: &SqlPipe) -> bool {
        should_partition(self.flavor, sp)
    }

    /// `_get_partition_ranges_for_df(pipe, df)`: the `(lo, hi)` partitions spanning a frame.
    pub fn partition_ranges_for_frame(
        &self,
        sp: &SqlPipe,
        df: &Frame,
    ) -> Vec<(PartitionValue, PartitionValue)> {
        let Some(dt_col) = sp.column("datetime") else {
            return Vec::new();
        };
        if !df.contains(&dt_col) {
            return Vec::new();
        }
        let Some((min_val, max_val)) = frame_axis_bounds(df, &dt_col) else {
            return Vec::new();
        };
        let Ok(interval) = sp.chunk_interval() else {
            return Vec::new();
        };
        let (ranges, capped) = partition_ranges(
            min_val,
            max_val,
            interval,
            self.settings.max_partitions_per_sync,
        );
        if capped {
            tracing::warn!(
                "Reached the {}-partition limit for {}; consider a larger `chunk_minutes`.",
                self.settings.max_partitions_per_sync,
                sp.pipe()
            );
        }
        ranges
    }

    /// `_create_missing_partitions(pipe, df)`: create any range partitions a frame needs
    /// before its rows are inserted (a no-op success for non-partitioned pipes).
    pub async fn create_missing_partitions(&self, sp: &SqlPipe, df: &Frame) -> SuccessTuple {
        if !self.should_partition(sp) {
            return SuccessTuple::ok("Pipe is not partitioned.");
        }
        if df.height() == 0 {
            return SuccessTuple::ok("No rows to partition.");
        }
        if self.flavor.pg_partition() {
            return self.create_missing_partitions_pg(sp, df).await;
        }
        if self.flavor.mysql_partition() {
            return self.create_missing_partitions_mysql(sp, df).await;
        }
        if self.flavor.mssql_partition() {
            return self.create_missing_partitions_mssql(sp, df).await;
        }
        SuccessTuple::ok("Pipe is not partitioned.")
    }

    /// `_get_mssql_partition_creation_queries(pipe, df)`: the partition scheme name and the
    /// queries creating the partition function and scheme, run before `CREATE TABLE`.
    pub(crate) fn mssql_partition_creation(
        &self,
        sp: &SqlPipe,
        df: &Frame,
    ) -> (String, Vec<String>) {
        let boundaries: Vec<PartitionValue> = self
            .partition_ranges_for_frame(sp, df)
            .into_iter()
            .map(|(lo, _)| lo)
            .collect();
        (
            partition_scheme_name(self.flavor, &sp.target),
            get_mssql_partition_creation_queries(sp, &boundaries),
        )
    }

    /// `_get_mssql_max_partition_boundary(pipe)`.
    pub async fn get_mssql_max_partition_boundary(&self, sp: &SqlPipe) -> Option<PartitionValue> {
        let query = get_mssql_max_partition_boundary_query(sp);
        let text = match self.value(&query).await? {
            SqlValue::Null => return None,
            v => v.to_string(),
        };
        parse_mssql_max_partition_boundary(&text, sp.is_int_axis())
    }

    /// `_create_missing_partitions_mssql(pipe, df)`: extend the partition function upward with
    /// `SPLIT RANGE` (each after `NEXT USED`).
    async fn create_missing_partitions_mssql(&self, sp: &SqlPipe, df: &Frame) -> SuccessTuple {
        let Some(dt_col) = sp.column("datetime").filter(|c| df.contains(c)) else {
            return SuccessTuple::ok("No partitions to create.");
        };
        let Some((_, max_val)) = frame_axis_bounds(df, &dt_col) else {
            return SuccessTuple::ok("No partitions to create.");
        };
        let Some(max_bound) = self.get_mssql_max_partition_boundary(sp).await else {
            return SuccessTuple::ok("No partitions to create.");
        };
        let Ok(interval) = sp.chunk_interval() else {
            return SuccessTuple::ok("No partitions to create.");
        };
        let max_parts = self.settings.max_partitions_per_sync;
        let queries =
            get_mssql_split_partition_queries(sp, max_bound, max_val, interval, max_parts);
        if queries.is_empty() {
            return SuccessTuple::ok("No partitions to create.");
        }
        let count = queries.len() / 2;
        if count >= max_parts {
            tracing::warn!(
                "Reached the {max_parts}-partition limit for {}; consider a larger `chunk_minutes`.",
                sp.pipe()
            );
        }
        if !self.exec_queries_all(&queries, true, true).await {
            return SuccessTuple::fail(format!("Failed to split partitions for {}.", sp.pipe()));
        }
        SuccessTuple::ok(format!("Split {count} partition(s) for {}.", sp.pipe()))
    }

    async fn create_missing_partitions_pg(&self, sp: &SqlPipe, df: &Frame) -> SuccessTuple {
        let ranges = self.partition_ranges_for_frame(sp, df);
        if ranges.is_empty() {
            return SuccessTuple::ok("No partitions to create.");
        }
        // Guard against a pre-existing PLAIN table.
        let is_partitioned = self
            .value(&get_pg_is_partitioned_query(&self.ctx(), sp))
            .await
            .is_some();
        if !is_partitioned {
            return SuccessTuple::ok(format!(
                "{} target table is not partitioned; skipping partition creation.",
                sp.pipe()
            ));
        }
        let queries = get_pg_partition_creation_queries(&self.ctx(), sp, &ranges);
        if !self.exec_queries_all(&queries, true, true).await {
            return SuccessTuple::fail(format!("Failed to create partitions for {}.", sp.pipe()));
        }
        SuccessTuple::ok(format!(
            "Ensured {} partition(s) for {}.",
            queries.len(),
            sp.pipe()
        ))
    }

    /// `_get_mysql_max_partition_bound(pipe)`.
    pub async fn get_mysql_max_partition_bound(&self, sp: &SqlPipe) -> Option<PartitionValue> {
        let query = get_mysql_max_partition_bound_query(&self.ctx(), sp, self.database())?;
        let df = self.read(&query).await.ok()?;
        let descriptions: Vec<String> = frame_docs(&df)
            .iter()
            .filter_map(|d| {
                d.iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case("PARTITION_DESCRIPTION"))
                    .and_then(|(_, v)| kv_str(Some(v)))
            })
            .collect();
        if descriptions.is_empty() {
            return None;
        }
        let dt_col = sp.column("datetime");
        let dt_dtype = dt_col
            .as_ref()
            .and_then(|c| sp.dtype(c))
            .unwrap_or_else(|| "datetime".into());
        parse_mysql_max_partition_bound(&descriptions, dt_dtype.to_lowercase().contains("int"))
    }

    async fn create_missing_partitions_mysql(&self, sp: &SqlPipe, df: &Frame) -> SuccessTuple {
        let Some(dt_col) = sp.column("datetime").filter(|c| df.contains(c)) else {
            return SuccessTuple::ok("No partitions to create.");
        };
        let Some((_, max_val)) = frame_axis_bounds(df, &dt_col) else {
            return SuccessTuple::ok("No partitions to create.");
        };
        let Some(max_bound) = self.get_mysql_max_partition_bound(sp).await else {
            // No partitions exist (table created without them); nothing we can extend.
            return SuccessTuple::ok("No partitions to create.");
        };
        let Ok(interval) = sp.chunk_interval() else {
            return SuccessTuple::ok("No partitions to create.");
        };
        let queries = get_mysql_add_partition_queries(
            &self.ctx(),
            sp,
            max_bound,
            max_val,
            interval,
            self.settings.max_partitions_per_sync,
        );
        if queries.is_empty() {
            return SuccessTuple::ok("No partitions to create.");
        }
        if queries.len() >= self.settings.max_partitions_per_sync {
            tracing::warn!(
                "Reached the {}-partition limit for {}; consider a larger `chunk_minutes`.",
                self.settings.max_partitions_per_sync,
                sp.pipe()
            );
        }
        if !self.exec_queries_all(&queries, true, true).await {
            return SuccessTuple::fail(format!("Failed to add partitions for {}.", sp.pipe()));
        }
        SuccessTuple::ok(format!(
            "Added {} partition(s) to {}.",
            queries.len(),
            sp.pipe()
        ))
    }

    /// `_get_partition_count(pipe)`.
    pub async fn get_partition_count(&self, sp: &SqlPipe) -> Option<i64> {
        let query = get_partition_count_query(&self.ctx(), sp, self.database())?;
        let v = self.value(&query).await?.as_i64()?;
        Some(if self.flavor.mssql_partition() {
            v + 1
        } else {
            v
        })
    }

    /// `_get_chunk_count_timescaledb(pipe)`.
    pub async fn get_chunk_count_timescaledb(&self, sp: &SqlPipe) -> Option<i64> {
        let query = get_timescaledb_chunk_count_query(&self.ctx(), sp);
        self.value(&query).await?.as_i64()
    }

    /// `get_partition_info(pipe)`: `{flavor, partitioned, count, interval}` for
    /// `show partitions` (`interval` in minutes for datetime axes, axis units otherwise).
    pub async fn get_partition_info(&self, pipe: &Pipe) -> Map {
        self.partition_info(pipe, false).await
    }

    /// Native display metadata with datetime intervals represented as `{seconds: value}`.
    /// The public compatibility method keeps integer minutes; both paths use one query.
    pub async fn get_partition_info_typed(&self, pipe: &Pipe) -> Map {
        self.partition_info(pipe, true).await
    }

    async fn partition_info(&self, pipe: &Pipe, typed_interval: bool) -> Map {
        let mut info = Map::new();
        info.insert("flavor".into(), Value::Str(self.flavor.as_str().into()));
        info.insert("partitioned".into(), Value::Bool(false));
        info.insert("count".into(), Value::Null);
        info.insert("interval".into(), Value::Null);
        let Ok(sp) = self.sql_pipe(pipe) else {
            return info;
        };
        if !self
            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
            .await
        {
            return info;
        }
        let interval_value = |sp: &SqlPipe| match sp.chunk_interval() {
            Ok(interval) => partition_interval_value(interval, typed_interval),
            Err(_) => Value::Null,
        };
        if self.flavor.is_timescaledb() {
            if !self.is_hypertable(&sp).await {
                return info;
            }
            info.insert("partitioned".into(), Value::Bool(true));
            info.insert(
                "count".into(),
                self.get_chunk_count_timescaledb(&sp)
                    .await
                    .map(Value::Int)
                    .unwrap_or(Value::Null),
            );
            info.insert("interval".into(), interval_value(&sp));
            return info;
        }
        if !self.should_partition(&sp) {
            return info;
        }
        let Some(count) = self.get_partition_count(&sp).await.filter(|c| *c != 0) else {
            return info;
        };
        info.insert("partitioned".into(), Value::Bool(true));
        info.insert("count".into(), Value::Int(count));
        info.insert("interval".into(), interval_value(&sp));
        info
    }

    /// `partition_pipe(pipe, chunk_minutes)`: rebuild the target table at a new partition width
    /// (TimescaleDB: `set_chunk_time_interval` for future chunks).
    pub async fn partition_pipe(&self, pipe: &Pipe, chunk_minutes: Option<i64>) -> SuccessTuple {
        self.partition_pipe_with_parameters(pipe, chunk_minutes)
            .await
            .0
    }

    /// Return the parameter patch applied during repartitioning, including on a later failure.
    pub async fn partition_pipe_with_parameters(
        &self,
        pipe: &Pipe,
        chunk_minutes: Option<i64>,
    ) -> (SuccessTuple, Map) {
        let mut changed = Map::new();
        let result = self
            .partition_pipe_inner(pipe, chunk_minutes, &mut changed)
            .await;
        (result, changed)
    }

    async fn partition_pipe_inner(
        &self,
        pipe: &Pipe,
        chunk_minutes: Option<i64>,
        changed: &mut Map,
    ) -> SuccessTuple {
        let flavor = self.flavor;
        if !(flavor.partitionable() || flavor.is_timescaledb()) {
            return SuccessTuple::fail(format!(
                "Repartitioning is not supported for flavor '{}'.",
                flavor.as_str()
            ));
        }
        let sp = match self.sql_pipe(pipe) {
            Ok(sp) => sp,
            Err(e) => return SuccessTuple::fail(e.to_string()),
        };
        let is_timescaledb = flavor.is_timescaledb();
        if !is_timescaledb && !self.should_partition(&sp) {
            return SuccessTuple::fail(format!(
                "{pipe} is not partitioned. Set `hypertable` to `True` (and define a `datetime` column) to enable native range partitioning."
            ));
        }
        if sp.column("datetime").is_none() {
            return SuccessTuple::fail(format!("{pipe} has no `datetime` column to partition by."));
        }
        if !self
            .table_exists(&sp.target, self.get_pipe_schema(&sp).as_deref())
            .await
        {
            return SuccessTuple::fail(format!("{pipe} does not exist; nothing to repartition."));
        }
        let configured = sp
            .param("verify")
            .and_then(|v| v.get("chunk_minutes"))
            .filter(|v| v.truthy())
            .cloned();
        let new_minutes = match chunk_minutes {
            Some(m) => Value::Int(m),
            None => configured.unwrap_or(Value::Int(self.settings.pipe_defaults.chunk_minutes)),
        };
        let new_minutes = match new_minutes {
            Value::Int(m) if m > 0 => m,
            other => {
                return SuccessTuple::fail(format!(
                    "Invalid chunk interval '{}'; must be a positive integer of minutes.",
                    other.py_str()
                ));
            }
        };
        let mut patch = Map::new();
        let mut verify = Map::new();
        verify.insert("chunk_minutes".into(), Value::Int(new_minutes));
        patch.insert("verify".into(), Value::Map(verify));

        if is_timescaledb {
            let query = match get_set_chunk_time_interval_query(&self.ctx(), &sp, new_minutes) {
                Ok(q) => q,
                Err(e) => return SuccessTuple::fail(e.to_string()),
            };
            if self.exec(&query).await.is_none() {
                return SuccessTuple::fail(format!("Failed to set chunk interval for {pipe}."));
            }
            let interval =
                sp.chunk_interval_with(ChunkIntervalArg::Delta(TimeDelta::minutes(new_minutes)));
            let mut updated = pipe.clone();
            *changed = patch.clone();
            updated.update_parameters(patch);
            let _ = self.edit_pipe(&updated, false).await;
            let desc = match interval {
                Ok(ChunkInterval::Int(i)) => {
                    format!("{} epoch units", super::data::fmt_thousands(i))
                }
                _ => format!("{new_minutes} minutes"),
            };
            return SuccessTuple::ok(format!(
                "Set chunk interval for {pipe} to {desc} (applies to future chunks; existing chunks are unchanged)."
            ));
        }

        // Non-TimescaleDB: rebuild via a drop + re-sync round-trip.
        let current = sp.chunk_interval();
        let new = sp.chunk_interval_with(ChunkIntervalArg::Delta(TimeDelta::minutes(new_minutes)));
        if let (Ok(c), Ok(n)) = (&current, &new)
            && c == n
        {
            return SuccessTuple::ok(format!(
                "{pipe} is already partitioned at {new_minutes} minutes."
            ));
        }
        let rowcount_before = self
            .get_pipe_rowcount(pipe, &DataQuery::default())
            .await
            .ok()
            .flatten();
        let df = match self.get_pipe_data(pipe, &DataQuery::default()).await {
            Ok(Some(df)) => df,
            _ => {
                return SuccessTuple::fail(format!(
                    "Could not read data for {pipe}; aborting repartition."
                ));
            }
        };
        let mut updated = pipe.clone();
        *changed = patch.clone();
        updated.update_parameters(patch);
        if !updated.temporary {
            let res = self.edit_pipe(&updated, false).await;
            if !res.success {
                return SuccessTuple::fail(format!(
                    "Failed to persist new partition width for {pipe}:\n{}",
                    res.message
                ));
            }
        }
        let res = self.drop_pipe(&updated).await;
        if !res.success {
            return SuccessTuple::fail(format!(
                "Failed to drop {pipe} during repartition:\n{}",
                res.message
            ));
        }
        let res = self
            .sync_pipe(&updated, df, &meerschaum_connector::SyncOptions::default())
            .await;
        if !res.success {
            return SuccessTuple::fail(format!(
                "Repartition of {pipe} failed during re-sync; the table was dropped and must be resynced from its source:\n{}",
                res.message
            ));
        }
        let rowcount_after = self
            .get_pipe_rowcount(&updated, &DataQuery::default())
            .await
            .ok()
            .flatten();
        if let (Some(b), Some(a)) = (rowcount_before, rowcount_after)
            && a != b
        {
            tracing::warn!("Row count changed during repartition of {pipe} ({b} -> {a}).");
        }
        SuccessTuple::ok(format!("Repartitioned {pipe} to {new_minutes} minutes."))
    }

    /// The initial MySQL partitions for a new table (`_get_initial_partition_bounds`).
    pub(crate) fn initial_partition_bounds(
        &self,
        sp: &SqlPipe,
        df: &Frame,
    ) -> Vec<(String, String)> {
        let ranges = self.partition_ranges_for_frame(sp, df);
        crate::queries::partition::get_initial_partition_bounds(self.flavor, &sp.target, &ranges)
    }

    /// Whether this flavor uses inline partitions at `CREATE TABLE` (MySQL / MariaDB).
    pub(crate) fn inline_partitions(&self) -> bool {
        matches!(self.flavor, Flavor::Mysql | Flavor::Mariadb)
    }
}

fn partition_interval_value(interval: ChunkInterval, typed: bool) -> Value {
    match interval {
        ChunkInterval::Int(i) => Value::Int(i),
        ChunkInterval::Delta(d) if typed => Value::Map(Map::from_iter([(
            "seconds".into(),
            Value::Float(d.num_milliseconds() as f64 / 1000.0),
        )])),
        ChunkInterval::Delta(d) => Value::Int(d.num_minutes()),
    }
}

#[cfg(test)]
mod interval_tests {
    use super::*;

    #[test]
    fn datetime_metadata_keeps_kind_without_changing_compatibility_minutes() {
        let day = ChunkInterval::Delta(TimeDelta::days(1));
        assert_eq!(partition_interval_value(day, false), Value::Int(1440));
        assert_eq!(
            partition_interval_value(day, true),
            Value::Map(Map::from_iter([("seconds".into(), Value::Float(86400.0)),]))
        );
        assert_eq!(
            partition_interval_value(ChunkInterval::Int(1440), true),
            Value::Int(1440)
        );
    }
}
