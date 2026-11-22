//! Tests of `show data`'s limited reads: the head and tail of a long window and the backtrack.

use super::*;
use meerschaum_connector_sql::uri::{ConnectionSpec, ConnectorAttributes};
use meerschaum_connector_sql::{SqlConnector, flavor::Flavor};
use std::sync::Arc;

async fn connector(dir: &std::path::Path, rows: usize) -> Arc<SqlConnector> {
    let attrs = ConnectorAttributes {
        flavor: Some(Flavor::Sqlite),
        database: Some(dir.join("show.db").to_string_lossy().into_owned()),
        ..Default::default()
    };
    let sql = Arc::new(
        SqlConnector::connect("show", ConnectionSpec::from_attributes(&attrs).unwrap())
            .await
            .unwrap(),
    );
    sql.exec("CREATE TABLE show_t (id INTEGER PRIMARY KEY, dt TEXT, v REAL)")
        .await
        .unwrap();
    for i in 0..rows {
        sql.exec(&format!(
            "INSERT INTO show_t VALUES ({i}, datetime('2024-01-01', '+{i} hours'), {i}.5)"
        ))
        .await
        .unwrap();
    }
    sql
}

fn pipe(datetime: bool) -> meerschaum_core::Pipe {
    let mut columns = Map::new();
    columns.insert("primary".into(), Value::Str("id".into()));
    if datetime {
        columns.insert("datetime".into(), Value::Str("dt".into()));
    }
    let mut params = Map::new();
    params.insert("columns".into(), Value::Map(columns));
    params.insert("target".into(), Value::Str("show_t".into()));
    meerschaum_core::Pipe::builder("sql:show", "t")
        .parameters(params)
        .build()
        .unwrap()
}

fn plan(limits: Option<DisplayOptions>) -> DataPlan {
    DataPlan {
        backtrack: None,
        begin: None,
        end: None,
        params: None,
        limits,
    }
}

fn options() -> DisplayOptions {
    DisplayOptions {
        max_rows: 20,
        min_rows: 6,
        max_columns: None,
        max_colwidth: 50,
        width: None,
    }
}

fn first_column(read: &DataRead) -> Vec<String> {
    let cells = read.frame.cells();
    cells.columns[0]
        .1
        .iter()
        .map(|c| match c {
            Cell::Int(i) => i.to_string(),
            other => format!("{other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn a_long_window_reads_only_the_head_and_the_tail() {
    let dir = tempfile::tempdir().unwrap();
    let sql = connector(dir.path(), 100).await;
    sql.enable_query_log();
    let conn: &dyn InstanceConnector = sql.as_ref();
    let read = read_pipe_data(conn, &pipe(true), &plan(Some(options())))
        .await
        .expect("rows");
    assert_eq!(read.total, 100);
    assert_eq!(read.fetched, Fetched::HeadTail { head: 3 });
    assert_eq!(first_column(&read), ["0", "1", "2", "97", "98", "99"]);
    // No statement read the whole table: every `SELECT *`-style read carries a LIMIT.
    let reads: Vec<String> = sql
        .take_query_log()
        .into_iter()
        .filter(|q| q.contains("show_t") && !q.contains("COUNT") && !q.contains("count("))
        .filter(|q| q.trim_start().to_uppercase().starts_with("SELECT"))
        .filter(|q| !q.contains("pragma") && !q.contains("sqlite_master"))
        .collect();
    assert!(!reads.is_empty());
    assert!(
        reads.iter().all(|q| q.to_uppercase().contains("LIMIT")),
        "{reads:#?}"
    );
}

#[tokio::test]
async fn a_backtrack_window_keeps_the_newest_first_orientation() {
    let dir = tempfile::tempdir().unwrap();
    let sql = connector(dir.path(), 100).await;
    let conn: &dyn InstanceConnector = sql.as_ref();
    let mut backtrack = plan(Some(options()));
    // 60 hours back from the newest row: 61 rows (the bound is inclusive).
    backtrack.backtrack = Some(3600.0);
    let read = read_pipe_data(conn, &pipe(true), &backtrack)
        .await
        .expect("rows");
    assert_eq!(read.total, 61);
    assert_eq!(first_column(&read), ["99", "98", "97", "41", "40", "39"]);
    // Short enough to read whole: the plain backtrack read, newest first as in v4.
    backtrack.backtrack = Some(600.0);
    let short = read_pipe_data(conn, &pipe(true), &backtrack)
        .await
        .expect("rows");
    assert_eq!((short.total, short.fetched), (11, Fetched::All));
    assert_eq!(first_column(&short)[..2], ["99", "98"]);
}

#[tokio::test]
async fn a_pipe_without_a_datetime_reads_the_first_rows_only() {
    let dir = tempfile::tempdir().unwrap();
    let sql = connector(dir.path(), 100).await;
    let conn: &dyn InstanceConnector = sql.as_ref();
    // The table has a TEXT column named `dt`, not a datetime dtype, so it has no axis.
    let read = read_pipe_data(conn, &pipe(false), &plan(Some(options())))
        .await
        .expect("rows");
    assert_eq!(read.total, 100);
    assert_eq!(read.fetched, Fetched::HeadOnly);
    assert_eq!(first_column(&read), ["0", "1", "2", "3", "4", "5"]);
}

#[test]
fn the_axis_is_columns_datetime_or_v4s_guess_and_needs_an_order_by() {
    let dtypes = |pairs: &[(&str, &str)]| -> indexmap::IndexMap<String, String> {
        pairs
            .iter()
            .map(|(c, t)| (c.to_string(), t.to_string()))
            .collect()
    };
    let cols = |pairs: &[(&str, &str)]| -> Map {
        pairs
            .iter()
            .map(|(r, c)| (r.to_string(), Value::Str(c.to_string())))
            .collect()
    };
    let table = dtypes(&[("id", "int"), ("ts", "datetime64[ns, UTC]"), ("v", "float")]);
    // `columns:datetime`, or the guessed datetime dtype next to another index column.
    assert!(orders_by_datetime(
        &cols(&[("datetime", "ts")]),
        false,
        &table
    ));
    assert!(orders_by_datetime(&cols(&[("id", "id")]), false, &table));
    // A datetime dtype alone: v4 emits no ORDER BY, so a "tail" would be arbitrary.
    assert!(!orders_by_datetime(&cols(&[]), false, &table));
    // `dtypes:datetime: null` switches the guess off; a missing column is no axis.
    assert!(!orders_by_datetime(&cols(&[("id", "id")]), true, &table));
    assert!(!orders_by_datetime(
        &cols(&[("datetime", "gone")]),
        false,
        &table
    ));
    assert!(!orders_by_datetime(
        &cols(&[("id", "id")]),
        false,
        &dtypes(&[("id", "int"), ("v", "float")])
    ));
}

#[tokio::test]
async fn a_datetime_dtype_without_columns_prints_the_head_only() {
    let dir = tempfile::tempdir().unwrap();
    let sql = connector(dir.path(), 0).await;
    sql.exec("CREATE TABLE show_d (ts DATETIME, v REAL)")
        .await
        .unwrap();
    for i in 0..100 {
        // Out of time order: an unordered "tail" would be the last physical rows.
        sql.exec(&format!(
            "INSERT INTO show_d VALUES (datetime('2024-01-01', '+{} hours'), {i}.5)",
            (i * 37) % 100
        ))
        .await
        .unwrap();
    }
    let mut params = Map::new();
    params.insert("target".into(), Value::Str("show_d".into()));
    params.insert(
        "dtypes".into(),
        Value::Map(Map::from_iter([(
            "ts".into(),
            Value::Str("datetime".into()),
        )])),
    );
    let pipe = meerschaum_core::Pipe::builder("sql:show", "d")
        .parameters(params)
        .build()
        .unwrap();
    let conn: &dyn InstanceConnector = sql.as_ref();
    let read = read_pipe_data(conn, &pipe, &plan(Some(options())))
        .await
        .expect("rows");
    assert_eq!(read.total, 100);
    assert_eq!(read.fetched, Fetched::HeadOnly);
}

#[tokio::test]
async fn short_windows_unlimited_reads_and_missing_tables_read_whole() {
    let dir = tempfile::tempdir().unwrap();
    let sql = connector(dir.path(), 15).await;
    let conn: &dyn InstanceConnector = sql.as_ref();
    let short = read_pipe_data(conn, &pipe(true), &plan(Some(options())))
        .await
        .expect("rows");
    assert_eq!((short.total, short.fetched), (15, Fetched::All));
    let all = read_pipe_data(conn, &pipe(true), &plan(None))
        .await
        .expect("rows");
    assert_eq!((all.total, all.fetched), (15, Fetched::All));
    let long_dir = tempfile::tempdir().unwrap();
    let long = connector(long_dir.path(), 40).await;
    let unlimited = read_pipe_data(long.as_ref(), &pipe(true), &plan(None))
        .await
        .expect("rows");
    assert_eq!((unlimited.total, unlimited.fetched), (40, Fetched::All));
    let missing = meerschaum_core::Pipe::builder("sql:show", "nothing")
        .build()
        .unwrap();
    assert!(
        read_pipe_data(conn, &missing, &plan(Some(options())))
            .await
            .is_none()
    );
}

#[test]
fn the_backtrack_window_starts_before_the_newest_value() {
    assert_eq!(
        backtrack_begin(DateBound::Int(100), 30).py_str(),
        "70".to_string()
    );
    let day = chrono::NaiveDate::from_ymd_opt(2024, 1, 2)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let begin = backtrack_begin(DateBound::Datetime(DateTimeValue::naive(day)), 90);
    assert_eq!(begin.py_str(), "2024-01-01 22:30:00");
}
