//! The generic port of the v4 bodies (a [`Reader`] with no SQL fast path) against the SQL
//! driver's own `Pipe.get_data()` pipeline, on a SQLite file.

use super::*;
use indexmap::IndexMap;
use meerschaum_engine::registry::StaticRegistry;
use meerschaum_kernel::value::DateTimeValue;

const INSTANCE: &str = "sql:w15l2_unit";

fn run<F: std::future::Future>(fut: F) -> F::Output {
    crate::runtime::runtime().block_on(fut)
}

fn start() -> chrono::NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
}

fn doc(i: i64) -> IndexMap<String, Cell> {
    let mut d = IndexMap::new();
    d.insert(
        "dt".to_string(),
        Cell::Datetime(DateTimeValue::utc(start() + Duration::hours(i))),
    );
    d.insert("id".to_string(), Cell::Int(i % 3));
    d.insert("val".to_string(), Cell::Float(i as f64 / 2.0));
    d.insert("name".to_string(), Cell::Str(format!("n{i}")));
    d
}

fn pipe(metric: &str) -> Pipe {
    let json = serde_json::json!({
        "columns": {"datetime": "dt", "id": "id"},
        "dtypes": {"val": "float", "name": "str"},
    });
    let Value::Map(parameters) = pipe_ops::json_to_core(&json) else {
        unreachable!()
    };
    Pipe::builder("w15l2", metric)
        .instance(INSTANCE)
        .parameters(parameters)
        .build()
        .unwrap()
}

struct Fixture {
    generic: Reader,
    sql: Reader,
    path: std::path::PathBuf,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A SQLite file with `n` rows synced, read through the SQL fast path and the generic path.
fn fixture(tag: &str, n: i64) -> (Fixture, Pipe) {
    let path = std::env::temp_dir().join(format!(
        "w15l2-{tag}-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let conn = run(SqlConnector::from_uri(
        "w15l2_unit",
        &format!("sqlite:///{}", path.display()),
    ))
    .unwrap();
    let conn = Arc::new(conn);
    let registry: Arc<dyn ConnectorRegistry> = Arc::new(
        StaticRegistry::new().with_instance(INSTANCE, conn.clone() as Arc<dyn InstanceConnector>),
    );
    let reader = |sql: Option<Arc<SqlConnector>>| Reader {
        instance: conn.clone(),
        sql,
        sources: HashMap::new(),
        registry: registry.clone(),
        defaults: PipeDefaults::default(),
        backtrack_minutes: 1440,
        filter_limit: 250,
        columns_types: None,
    };
    let generic = reader(None);
    let sql = reader(Some(conn.clone()));
    let pipe = pipe(tag);
    let docs: Vec<_> = (0..n).map(doc).collect();
    let env = sql.env();
    let result = run(meerschaum_engine::sync::sync_pipe(
        &env,
        &pipe,
        Some(PipeInput::Docs(docs)),
        &SyncArgs::default(),
        None,
    ));
    assert!(result.success, "{}", result.message);
    (Fixture { generic, sql, path }, pipe)
}

fn rows(data: &Data) -> Vec<Vec<String>> {
    match data {
        Data::Missing => vec![],
        Data::Docs(docs) | Data::TypedDocs(docs, _) => docs
            .iter()
            .map(|d| d.values().map(Cell::py_str).collect())
            .collect(),
        Data::Frame(frame, _) => (0..frame.height())
            .map(|i| frame.row(i).iter().map(Cell::py_str).collect())
            .collect(),
    }
}

fn names(data: &Data) -> Vec<String> {
    match data {
        Data::Frame(frame, _) => frame.names(),
        _ => vec![],
    }
}

fn at(hours: i64) -> Option<DateBound> {
    Some(DateBound::Datetime(DateTimeValue::utc(
        start() + Duration::hours(hours),
    )))
}

fn params(json: serde_json::Value) -> Option<Map> {
    match pipe_ops::json_to_core(&json) {
        Value::Map(m) => Some(m),
        _ => None,
    }
}

#[test]
fn the_generic_get_data_matches_the_sql_pipeline() {
    let (fx, pipe) = fixture("getdata", 12);
    let cols = |c: &[&str]| Some(c.iter().map(|s| s.to_string()).collect::<Vec<_>>());
    let order = |o: &str| Some(o.to_string());
    let cases = [
        GetDataArgs {
            order: order("asc"),
            ..Default::default()
        },
        GetDataArgs {
            select_columns: cols(&["dt", "val"]),
            order: order("asc"),
            ..Default::default()
        },
        GetDataArgs {
            omit_columns: cols(&["name"]),
            order: order("asc"),
            ..Default::default()
        },
        GetDataArgs {
            select_columns: cols(&["dt", "val"]),
            omit_columns: cols(&["val"]),
            order: order("asc"),
            ..Default::default()
        },
        GetDataArgs {
            select_columns: cols(&["dt", "nope"]),
            omit_columns: cols(&["nope"]),
            order: order("asc"),
            ..Default::default()
        },
        GetDataArgs {
            begin: at(2),
            end: at(9),
            order: order("asc"),
            ..Default::default()
        },
        GetDataArgs {
            params: params(serde_json::json!({"id": 1})),
            order: order("desc"),
            limit: Some(3),
            ..Default::default()
        },
        GetDataArgs {
            select_columns: cols(&["dt", "nope"]),
            order: order("asc"),
            ..Default::default()
        },
        GetDataArgs {
            select_columns: cols(&["dt", "id"]),
            add_missing_columns: true,
            order: order("asc"),
            ..Default::default()
        },
    ];
    for (i, args) in cases.iter().enumerate() {
        let want = run(fx.sql.get_data(&pipe, args)).unwrap();
        let got = run(fx.generic.get_data(&pipe, args)).unwrap();
        assert_eq!(names(&got), names(&want), "case {i} columns");
        assert_eq!(rows(&got), rows(&want), "case {i} rows");
    }
}

#[test]
fn a_missing_pipe_has_no_data_and_no_dtypes() {
    let (fx, _) = fixture("missing", 3);
    let ghost = pipe("never_synced");
    for reader in [&fx.sql, &fx.generic] {
        let data = run(reader.get_data(&ghost, &GetDataArgs::default())).unwrap();
        assert!(matches!(data, Data::Missing));
        let docs = run(reader.get_data(
            &ghost,
            &GetDataArgs {
                as_docs: true,
                ..Default::default()
            },
        ))
        .unwrap();
        assert!(matches!(docs, Data::Docs(d) if d.is_empty()));
        assert!(run(reader.infer_dtypes(&ghost, false)).unwrap().is_empty());
        assert_eq!(
            run(reader.get_rowcount(&ghost, None, None, None, false)).unwrap(),
            0
        );
        assert!(
            run(reader.get_sync_time(&ghost, None, true, false, false, false))
                .unwrap()
                .is_none()
        );
    }
}

#[test]
fn sync_times_and_rowcounts() {
    let (fx, pipe) = fixture("times", 12);
    for reader in [&fx.sql, &fx.generic] {
        let newest = run(reader.get_sync_time(&pipe, None, true, false, false, false)).unwrap();
        assert_eq!(newest, at(11));
        let oldest = run(reader.get_sync_time(&pipe, None, false, false, false, false)).unwrap();
        assert_eq!(oldest, at(0));
        // The default backtrack is a day: the sync time minus 1440 minutes.
        let back = run(reader.get_sync_time(&pipe, None, true, false, true, false)).unwrap();
        assert_eq!(
            back,
            Some(DateBound::Datetime(DateTimeValue::utc(
                start() + Duration::hours(11) - Duration::minutes(1440)
            )))
        );
        let by_id = run(reader.get_sync_time(
            &pipe,
            params(serde_json::json!({"id": 1})).as_ref(),
            true,
            false,
            false,
            false,
        ))
        .unwrap();
        assert_eq!(by_id, at(10));
        assert_eq!(
            run(reader.get_rowcount(&pipe, None, None, None, false)).unwrap(),
            12
        );
        assert_eq!(
            run(reader.get_rowcount(&pipe, at(3), at(8), None, false)).unwrap(),
            5
        );
        assert_eq!(
            run(reader.get_rowcount(
                &pipe,
                None,
                None,
                params(serde_json::json!({"id": 0})),
                false
            ))
            .unwrap(),
            4
        );
    }
}

#[test]
fn chunk_plans() {
    let (fx, pipe) = fixture("chunks", 12);
    for reader in [&fx.sql, &fx.generic] {
        let plan = |interval| {
            run(reader.plan_chunks(
                &pipe,
                None,
                None,
                None,
                Some(ChunkIntervalArg::Delta(TimeDelta::hours(interval))),
            ))
            .unwrap()
        };
        // Twelve hourly rows span 12 hours: chunks of 5 hours make three, 30 days one.
        assert!(matches!(plan(5), ChunkPlan::Bounds(b) if b.len() == 3));
        assert!(matches!(plan(24 * 30), ChunkPlan::Single));
        let ghost = pipe_without_table();
        assert!(matches!(
            run(reader.plan_chunks(&ghost, None, None, None, None)).unwrap(),
            ChunkPlan::Empty
        ));
    }
}

fn pipe_without_table() -> Pipe {
    pipe("no_table_yet")
}

#[test]
fn backtrack_data_is_newest_first() {
    let (fx, pipe) = fixture("backtrack", 12);
    for reader in [&fx.sql, &fx.generic] {
        let data =
            run(reader.get_backtrack_data(&pipe, Some(180.0), GetDataArgs::default())).unwrap();
        let got = rows(&data);
        // The sync time (hour 11) minus 3 hours is hour 8: hours 8..=11, newest first.
        assert_eq!(got.len(), 4);
        assert!(got[0][0].contains("2026-01-01 11:00:00"), "{:?}", got[0]);
        assert!(got[3][0].contains("2026-01-01 08:00:00"), "{:?}", got[3]);
    }
}

#[test]
fn filter_existing_splits_new_and_changed_rows() {
    let (fx, pipe) = fixture("filter", 12);
    let mut incoming: Vec<_> = (3..9).map(doc).collect();
    for d in incoming.iter_mut().skip(3) {
        d.insert("val".to_string(), Cell::Float(100.0));
    }
    incoming.extend((12..16).map(doc));
    for reader in [&fx.sql, &fx.generic] {
        let (unseen, update, delta) = run(reader.filter_existing(
            &pipe,
            PipeInput::Docs(incoming.clone()),
            &FilterOpts::default(),
        ))
        .unwrap()
        .frames;
        assert_eq!(unseen.height(), 4);
        assert_eq!(update.height(), 3);
        assert_eq!(delta.height(), 7);
    }
}

#[test]
fn the_generic_deduplicate_removes_duplicates() {
    pyo3::Python::initialize();
    let (fx, pipe) = fixture("dedup", 6);
    // Duplicates around the sync's diff: write the rows again straight to the table.
    let conn = fx.sql.sql.clone().unwrap();
    let table = conn
        .sql_pipe(&pipe)
        .map(|sp| conn.pipe_table_name(&sp))
        .unwrap();
    run(conn.exec(&format!("INSERT INTO {table} SELECT * FROM {table}")));
    // A generic pass must see the duplicates and leave one of each.
    let before = run(fx.generic.get_rowcount(&pipe, None, None, None, false)).unwrap();
    let result = run(fx.generic.deduplicate(
        &pipe,
        &DedupArgs {
            use_instance_method: false,
            ..Default::default()
        },
    ));
    assert!(result.success, "{}", result.message);
    let after = run(fx.generic.get_rowcount(&pipe, None, None, None, false)).unwrap();
    assert!(after <= before);
    assert_eq!(after, 6, "{}", result.message);
}

#[test]
fn helpers() {
    assert_eq!(
        distinct_len(&[vec!["a".into()], vec!["a".into()], vec!["b".into()]]),
        2
    );
    let frame = meerschaum_kernel::docs::frame_from_docs(&[doc(1), doc(1), doc(2)]).unwrap();
    let out = drop_duplicates_last(&frame, &["dt".to_string(), "id".to_string()]);
    assert_eq!(out.height(), 2);
    let a = DateBound::Int(5);
    assert_eq!(
        sub_interval(&a, &ChunkInterval::Int(3)),
        Some(DateBound::Int(2))
    );
}
