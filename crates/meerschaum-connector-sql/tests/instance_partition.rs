//! `tests/test_partition.py`: native range partitioning and epoch-aligned chunk bounds.
mod common;
mod fork_helpers;
use chrono::{Duration, TimeDelta};
use common::*;
use fork_helpers::{partition_data, row_dt};
use indexmap::IndexMap;
use meerschaum_connector::{DataQuery, DateBound};
use meerschaum_connector_sql::Flavor;
use meerschaum_connector_sql::instance::data::get_chunk_bounds;
use meerschaum_connector_sql::instance::values::{partition_value, py_datetime};
use meerschaum_connector_sql::pipe::{ChunkInterval, ChunkIntervalArg, SqlPipe};
use meerschaum_connector_sql::queries::partition::{PartitionValue, partition_bounds};
use meerschaum_connector_sql::queries::table::{
    CreateTableOptions, get_create_table_query_from_dtypes,
};
use meerschaum_core::{Pipe, Value};

const CHUNK_MINUTES: i64 = 1440;
const PARTITIONABLE: &[&str] = &["postgresql", "postgis", "mysql", "mariadb", "mssql"];

fn partitioned_params() -> String {
    format!(
        r#"{{"columns": {{"datetime": "datetime", "id": "id"}}, "hypertable": true, "verify": {{"chunk_minutes": {CHUNK_MINUTES}}}}}"#
    )
}

async fn count_partitions(c: &Ctx, pipe: &Pipe) -> i64 {
    let sp = c.conn.sql_pipe(pipe).unwrap();
    c.conn.get_partition_count(&sp).await.unwrap_or(-1)
}

#[test]
fn test_partition_ddl_quotes_mixed_case_column() {
    for (flavor, quote) in [
        (Flavor::Postgresql, "\"startTime\""),
        (Flavor::Postgis, "\"startTime\""),
        (Flavor::Mysql, "`startTime`"),
        (Flavor::Mariadb, "`startTime`"),
        (Flavor::Mssql, "[startTime]"),
    ] {
        let dtypes: IndexMap<String, String> =
            [("startTime", "datetime"), ("id", "int"), ("val", "int")]
                .into_iter()
                .map(|(a, b)| (a.to_string(), b.to_string()))
                .collect();
        let bounds = flavor
            .mysql_partition()
            .then(|| vec![("p20230101".to_string(), "'2023-01-02 00:00:00'".to_string())]);
        let queries = get_create_table_query_from_dtypes(
            &dtypes,
            "tbl",
            flavor,
            &CreateTableOptions {
                datetime_column: Some("startTime".into()),
                primary_key: Some("id".into()),
                partition_by_column: Some("startTime".into()),
                partition_bounds: bounds,
                partition_scheme_name: Some("ps_tbl".into()),
                ..Default::default()
            },
        )
        .unwrap();
        let ddl = queries.join("\n");
        assert!(ddl.contains(quote), "{flavor:?}: {ddl}");
        assert!(!ddl.contains("RANGE (startTime") && !ddl.contains("COLUMNS (startTime"));
    }
}

flavor_tests!(test_partitioned_sync_roundtrip, |c: Ctx| async move {
    if !PARTITIONABLE.contains(&c.flavor) {
        return;
    }
    let data = partition_data();
    let part = c.pipe(
        "test",
        "partition",
        Some("partitioned"),
        &partitioned_params(),
    );
    assert!(c.conn.should_partition(&c.conn.sql_pipe(&part).unwrap()));
    let plain = c.pipe(
        "test",
        "partition",
        Some("unpartitioned"),
        r#"{"columns": {"datetime": "datetime", "id": "id"}, "hypertable": false}"#,
    );
    assert!(!c.conn.should_partition(&c.conn.sql_pipe(&plain).unwrap()));
    for pipe in [&part, &plain] {
        ok(c.sync(pipe, data.clone()).await);
    }
    assert_eq!(
        c.rowcount(&part, DataQuery::default()).await,
        c.rowcount(&plain, DataQuery::default()).await
    );
    let pd = c.docs(&part).await;
    let qd = c.docs(&plain).await;
    assert_eq!(pd.len(), qd.len());
    assert_eq!(pd.len(), 96);
    assert_eq!(row_dt(&pd[0], "datetime"), row_dt(&qd[0], "datetime"));
    assert_eq!(
        row_dt(pd.last().unwrap(), "datetime"),
        row_dt(qd.last().unwrap(), "datetime")
    );
    let n = count_partitions(&c, &part).await;
    assert!(n > 1, "expected multiple partitions, got {n}");
    // PARITY: `_get_partition_count` counts a `RANGE RIGHT` function's boundaries + 1 on MSSQL,
    // so a table without a partition function reports one partition (`_partition.py:164`).
    let plain_count = if c.is_mssql() { 1 } else { 0 };
    assert_eq!(count_partitions(&c, &plain).await, plain_count);
    c.delete(&part).await;
    c.delete(&plain).await;
});

flavor_tests!(test_partitioned_resync_idempotent, |c: Ctx| async move {
    if !PARTITIONABLE.contains(&c.flavor) {
        return;
    }
    let data = partition_data();
    let pipe = c.pipe(
        "test",
        "partition",
        Some("partition_resync"),
        &partitioned_params(),
    );
    ok(c.sync(&pipe, data.clone()).await);
    let rowcount = c.rowcount(&pipe, DataQuery::default()).await;
    ok(c.sync(&pipe, data).await);
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, rowcount);
    c.delete(&pipe).await;
});

#[test]
fn test_partition_bounds_deterministic() {
    let interval = ChunkInterval::Delta(TimeDelta::days(1));
    let a = partition_value(&ubound(2023, 1, 1, 13, 30));
    let b = partition_value(&ubound(2023, 1, 1, 2, 5));
    let (lo_a, hi_a) = partition_bounds(a, interval);
    assert_eq!((lo_a, hi_a), partition_bounds(b, interval));
    assert_eq!(lo_a, partition_value(&ubound(2023, 1, 1, 0, 0)));
    assert_eq!(hi_a, partition_value(&ubound(2023, 1, 2, 0, 0)));
    assert_eq!(
        partition_bounds(PartitionValue::Int(1437), ChunkInterval::Int(1000)),
        (PartitionValue::Int(1000), PartitionValue::Int(2000))
    );
}

fn sql_pipe(json: &str) -> SqlPipe {
    let pipe = Pipe::builder("test", "partition")
        .parameters(params(json))
        .build()
        .unwrap();
    SqlPipe::new(pipe, Flavor::Sqlite).unwrap()
}

#[test]
fn test_chunk_interval_aliases_and_range() {
    let p = sql_pipe(r#"{"columns": {"datetime": "ts"}, "verify": {"chunk_days": 7}}"#);
    assert_eq!(
        p.chunk_interval().unwrap(),
        ChunkInterval::Delta(TimeDelta::days(7))
    );
    let p = sql_pipe(
        r#"{"columns": {"datetime": "ts"}, "verify": {"chunk_minutes": 60, "chunk_days": 7}}"#,
    );
    assert_eq!(p.chunk_interval().unwrap(), ChunkInterval::minutes(60));
    let p = sql_pipe(
        r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}, "verify": {"chunk_range": 1000, "chunk_minutes": 5}}"#,
    );
    assert_eq!(p.chunk_interval().unwrap(), ChunkInterval::Int(1000));
    let p = sql_pipe(
        r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}, "verify": {"chunk_minutes": 500}}"#,
    );
    assert_eq!(p.chunk_interval().unwrap(), ChunkInterval::Int(500));
}

flavor_tests!(test_repartition_roundtrip, |c: Ctx| async move {
    if !PARTITIONABLE.contains(&c.flavor) {
        return;
    }
    let pipe = c.pipe(
        "test",
        "partition",
        Some("repartition"),
        &partitioned_params(),
    );
    ok(c.sync(&pipe, partition_data()).await);
    let sp = c.conn.sql_pipe(&pipe).unwrap();
    assert_eq!(
        sp.chunk_interval().unwrap(),
        ChunkInterval::minutes(CHUNK_MINUTES)
    );
    let before = c.rowcount(&pipe, DataQuery::default()).await;
    let n_before = count_partitions(&c, &pipe).await;

    let new_minutes = CHUNK_MINUTES * 2;
    ok(c.conn.partition_pipe(&pipe, Some(new_minutes)).await);
    let registered = c.registered(&pipe).await;
    assert_eq!(
        registered
            .get("verify")
            .and_then(|v| v.get("chunk_minutes")),
        Some(&Value::Int(new_minutes))
    );
    // `pipe.parameters` reflects the persisted width.
    let mut pipe = pipe.clone();
    pipe.set_parameters(registered);
    assert_eq!(c.rowcount(&pipe, DataQuery::default()).await, before);
    let n_after = count_partitions(&c, &pipe).await;
    assert!(
        n_before > 1 && n_after < n_before,
        "{n_before} -> {n_after}"
    );
    c.delete(&pipe).await;
});

flavor_tests!(test_get_partition_info, |c: Ctx| async move {
    if !PARTITIONABLE.contains(&c.flavor) {
        return;
    }
    let pipe = c.pipe("test", "partition", Some("info"), &partitioned_params());
    ok(c.sync(&pipe, partition_data()).await);
    let info = c.conn.get_partition_info(&pipe).await;
    assert_eq!(
        info.get("partitioned"),
        Some(&Value::Bool(true)),
        "{info:?}"
    );
    // `interval` is reported in minutes for datetime axes.
    assert_eq!(info.get("interval"), Some(&Value::Int(CHUNK_MINUTES)));
    if let Some(Value::Int(n)) = info.get("count") {
        assert!(*n > 1);
    }
    let plain = c.pipe(
        "test",
        "partition",
        Some("info_plain"),
        r#"{"columns": {"datetime": "datetime", "id": "id"}, "hypertable": false}"#,
    );
    ok(c.sync(&plain, partition_data()).await);
    assert_eq!(
        c.conn.get_partition_info(&plain).await.get("partitioned"),
        Some(&Value::Bool(false))
    );
    c.delete(&pipe).await;
    c.delete(&plain).await;
});

flavor_tests!(test_repartition_unpartitioned_errors, |c: Ctx| async move {
    if PARTITIONABLE.contains(&c.flavor) || c.flavor == "timescaledb" {
        return;
    }
    let pipe = c.pipe(
        "test",
        "partition",
        Some("noop"),
        r#"{"columns": {"datetime": "datetime", "id": "id"}}"#,
    );
    ok(c.sync(&pipe, partition_data()).await);
    let res = c.conn.partition_pipe(&pipe, None).await;
    assert!(!res.success);
    assert_eq!(
        res.message,
        format!(
            "Repartitioning is not supported for flavor '{}'.",
            c.conn.flavor().as_str()
        )
    );
    c.delete(&pipe).await;
});

#[test]
fn test_aligned_chunk_bounds_coincide_with_partitions() {
    let interval = ChunkInterval::Delta(TimeDelta::minutes(CHUNK_MINUTES));
    let begin = ubound(2023, 1, 1, 12, 0);
    let end = ubound(2023, 1, 5, 0, 0);
    let bounds = get_chunk_bounds(
        Some(begin.clone()),
        Some(end.clone()),
        true,
        interval,
        true,
        None,
        None,
    )
    .unwrap();
    assert!(!bounds.is_empty());
    assert_eq!(bounds[0].0, Some(begin));
    for (_, hi) in &bounds[..bounds.len() - 1] {
        let edge = partition_value(hi.as_ref().unwrap());
        assert_eq!(
            partition_bounds(edge, interval).0,
            edge,
            "{hi:?} not aligned"
        );
    }
    for w in bounds.windows(2) {
        assert_eq!(w[0].1, w[1].0);
    }
    assert_eq!(bounds.last().unwrap().1, Some(end));
    let _ = (py_datetime, Duration::zero(), DateBound::Int(0));
}

#[test]
fn test_explicit_chunk_interval_scales_to_int_axis() {
    let ms = sql_pipe(
        r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}, "precision": {"unit": "millisecond"}}"#,
    );
    assert_eq!(
        ms.chunk_interval_with(ChunkIntervalArg::Delta(TimeDelta::minutes(43200)))
            .unwrap(),
        ChunkInterval::Int(2_592_000_000)
    );
    assert_eq!(
        ms.chunk_interval_with(ChunkIntervalArg::Int(1000)).unwrap(),
        ChunkInterval::Int(1000)
    );
    let dt = sql_pipe(r#"{"columns": {"datetime": "ts"}}"#);
    assert_eq!(
        dt.chunk_interval_with(ChunkIntervalArg::Delta(TimeDelta::minutes(43200)))
            .unwrap(),
        ChunkInterval::Delta(TimeDelta::days(30))
    );
    assert_eq!(
        dt.chunk_interval_with(ChunkIntervalArg::Int(43200))
            .unwrap(),
        ChunkInterval::Delta(TimeDelta::days(30))
    );
}

// TimescaleDB: `partition_pipe` sets the chunk interval for future chunks.
flavor_tests!(
    test_repartition_timescaledb_chunk_interval,
    |c: Ctx| async move {
        if c.flavor != "timescaledb" {
            return;
        }
        let pipe = c.pipe(
            "test",
            "partition",
            Some("ts_repartition"),
            &partitioned_params(),
        );
        ok(c.sync(&pipe, partition_data()).await);
        let res = ok(c.conn.partition_pipe(&pipe, Some(CHUNK_MINUTES * 2)).await);
        assert!(res.message.contains("2880 minutes"), "{}", res.message);
        assert_eq!(
            c.registered(&pipe)
                .await
                .get("verify")
                .and_then(|v| v.get("chunk_minutes")),
            Some(&Value::Int(CHUNK_MINUTES * 2))
        );
        let info = c.conn.get_partition_info(&pipe).await;
        assert_eq!(
            info.get("partitioned"),
            Some(&Value::Bool(true)),
            "{info:?}"
        );
        c.delete(&pipe).await;
    }
);
