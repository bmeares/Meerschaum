//! `parse_date_bounds` on the fetch path (`core/Pipe/_fetch.py:78`, `_sync.py:624`): the
//! `begin` / `end` a connector's `fetch()` receives are coerced to the timezone of the pipe's
//! datetime dtype. SQLite stores timestamps without an offset, so the raw sync time is naive;
//! v4 hands a UTC-aware `begin` to `fetch()` for a `datetime` (UTC) column.

mod common;

use std::sync::{Arc, Mutex};

use common::*;
use meerschaum_connector::{Connector, ConnectorResult, DataQuery, DateBound, async_trait};
use meerschaum_core::Pipe;
use meerschaum_engine::{StaticRegistry, SyncArgs, SyncBegin};
use meerschaum_kernel::Frame;
use meerschaum_kernel::enforce::PipeInput;
use meerschaum_kernel::value::{DateTimeValue, TzInfo, Value as KValue};

/// A source that records the bounds `fetch()` receives and returns nothing new.
#[derive(Default)]
struct Recorder {
    queries: Mutex<Vec<DataQuery>>,
}

#[async_trait]
impl Connector for Recorder {
    fn connector_type(&self) -> &str {
        "rec"
    }

    fn label(&self) -> &str {
        "test"
    }

    async fn test_connection(&self) -> ConnectorResult<bool> {
        Ok(true)
    }

    async fn fetch(&self, _pipe: &Pipe, query: &DataQuery) -> ConnectorResult<Option<Frame>> {
        self.queries.lock().unwrap().push(query.clone());
        Ok(Some(Frame::default()))
    }
}

fn ts(hour: u32) -> chrono::NaiveDateTime {
    chrono::NaiveDate::from_ymd_opt(2026, 1, 2)
        .unwrap()
        .and_hms_opt(hour, 0, 0)
        .unwrap()
}

fn doc(hour: u32) -> indexmap::IndexMap<String, KValue> {
    let mut d = indexmap::IndexMap::new();
    d.insert("ts".into(), KValue::Datetime(DateTimeValue::utc(ts(hour))));
    d.insert("id".into(), KValue::Int(1));
    d
}

async fn setup(tag: &str, ts_dtype: &str) -> (Env, Arc<Recorder>, meerschaum_engine::Engine, Pipe) {
    let env = env(tag).await;
    let recorder = Arc::new(Recorder::default());
    let source: Arc<dyn Connector> = recorder.clone();
    let registry = Arc::new(StaticRegistry::clone(&env.registry).with_source("rec:test", source));
    let engine = meerschaum_engine::Engine::new(registry, env.config());
    let pipe = Pipe::builder("rec:test", "readings")
        .instance(INSTANCE)
        .parameters(json_map(&serde_json::json!({
            "columns": {"datetime": "ts", "id": "id"},
            "dtypes": {"ts": ts_dtype, "id": "int"},
        })))
        .build()
        .unwrap();
    let r = engine
        .sync_pipe(
            &pipe,
            Some(PipeInput::Docs(vec![doc(10)])),
            &SyncArgs::default(),
        )
        .await;
    assert!(r.success, "{}", r.message);
    (env, recorder, engine, pipe)
}

fn datetime(b: &Option<DateBound>) -> DateTimeValue {
    match b {
        Some(DateBound::Datetime(dt)) => dt.clone(),
        other => panic!("expected a datetime bound, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_begin_is_utc_aware_for_a_utc_datetime_column() {
    let (_env, recorder, engine, pipe) = setup("bounds_utc", "datetime").await;

    // `begin=''`: the sync time (naive from SQLite) made UTC-aware, minus the backtrack.
    let r = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(r.success, "{}", r.message);
    // An explicit naive `begin` and `end` are made UTC-aware too.
    let args = SyncArgs {
        begin: SyncBegin::At(DateBound::Datetime(DateTimeValue::naive(ts(1)))),
        end: Some(DateBound::Datetime(DateTimeValue::naive(ts(5)))),
        ..Default::default()
    };
    let r = engine.sync_pipe(&pipe, None, &args).await;
    assert!(r.success, "{}", r.message);

    let queries = recorder.queries.lock().unwrap().clone();
    assert_eq!(queries.len(), 2);
    let begin = datetime(&queries[0].begin);
    assert_eq!(begin.tz, Some(TzInfo::Utc), "{begin:?}");
    assert_eq!(begin.utc, ts(10) - chrono::TimeDelta::minutes(1440));
    assert_eq!(queries[0].end, None);
    let (begin, end) = (datetime(&queries[1].begin), datetime(&queries[1].end));
    assert_eq!((begin.tz, end.tz), (Some(TzInfo::Utc), Some(TzInfo::Utc)));
    assert_eq!((begin.utc, end.utc), (ts(1), ts(5)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fetch_begin_is_naive_for_a_naive_datetime_column() {
    let (env, recorder, engine, pipe) = setup("bounds_naive", "datetime64[ns]").await;
    assert!(
        env.conn
            .get_sync_time(&pipe, None, true, false)
            .await
            .unwrap()
            .is_some()
    );

    let args = SyncArgs {
        begin: SyncBegin::At(DateBound::Datetime(DateTimeValue::utc(ts(1)))),
        ..Default::default()
    };
    let r = engine.sync_pipe(&pipe, None, &SyncArgs::default()).await;
    assert!(r.success, "{}", r.message);
    let r = engine.sync_pipe(&pipe, None, &args).await;
    assert!(r.success, "{}", r.message);

    let queries = recorder.queries.lock().unwrap().clone();
    assert_eq!(datetime(&queries[0].begin).tz, None);
    let begin = datetime(&queries[1].begin);
    assert_eq!((begin.tz, begin.utc), (None, ts(1)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn datetime_bounds_on_an_integer_axis_need_precision() {
    let env = env("bounds_int").await;
    let recorder = Arc::new(Recorder::default());
    let source: Arc<dyn Connector> = recorder.clone();
    let registry = Arc::new(StaticRegistry::clone(&env.registry).with_source("rec:test", source));
    let engine = meerschaum_engine::Engine::new(registry, env.config());
    let make = |params: serde_json::Value| {
        Pipe::builder("rec:test", "epochs")
            .instance(INSTANCE)
            .parameters(json_map(&params))
            .build()
            .unwrap()
    };
    let args = SyncArgs {
        begin: SyncBegin::At(DateBound::Datetime(DateTimeValue::utc(ts(0)))),
        ..Default::default()
    };
    let bare = make(serde_json::json!({"columns": {"datetime": "t"}, "dtypes": {"t": "int"}}));
    let r = engine.sync_pipe(&bare, None, &args).await;
    assert!(!r.success);
    assert!(
        r.message.contains("non-epoch integer axis"),
        "{}",
        r.message
    );

    let epoch = make(serde_json::json!({
        "columns": {"datetime": "t"}, "dtypes": {"t": "int"}, "precision": "second",
    }));
    let r = engine.sync_pipe(&epoch, None, &args).await;
    assert!(r.success, "{}", r.message);
    let q = recorder.queries.lock().unwrap().last().cloned().unwrap();
    assert_eq!(q.begin, Some(DateBound::Int(ts(0).and_utc().timestamp())));
}
