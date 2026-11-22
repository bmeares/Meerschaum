//! Tests of `copy pipes`: the incremental window, the backtrack and the chunked copy.

use super::copy::{
    CopyData, axis_pipe, backtrack_from, backtrack_minutes, backtrack_windows, bound_before,
    copy_chunks, copy_summary, copy_windows, incremental_query, incremental_window,
};
use chrono::{TimeZone, Utc};
use meerschaum_connector::windows::{just_past, split_point};
use meerschaum_connector::{DataQuery, DateBound};
use meerschaum_core::pipe::PipeDefaults;
use meerschaum_core::{Pipe, Value};

fn pipe(params: &str) -> Pipe {
    Pipe::builder("sql:src", "temp")
        .parameters(Value::from_json_str(params).unwrap().to_map())
        .build()
        .unwrap()
}

fn at(hour: u32) -> DateBound {
    DateBound::utc(Utc.with_ymd_and_hms(2024, 1, 2, hour, 0, 0).unwrap())
}

#[test]
fn the_modes_are_v4_choices() {
    assert_eq!(CopyData::parse("incremental"), Some(CopyData::Incremental));
    assert_eq!(CopyData::parse("all"), Some(CopyData::All));
    assert_eq!(CopyData::parse("everything"), None);
}

#[test]
fn the_watermark_is_the_sync_time_minus_the_pipes_backtrack() {
    let d = PipeDefaults::default();
    let dt = pipe(r#"{"columns": {"datetime": "ts"}}"#);
    assert_eq!(
        backtrack_from(&dt, at(12), 1440, &d).py_str(),
        "2024-01-01 12:00:00+00:00"
    );
    let own = pipe(r#"{"columns": {"datetime": "ts"}, "fetch": {"backtrack_minutes": 120}}"#);
    assert_eq!(
        backtrack_from(&own, at(12), 1440, &d).py_str(),
        "2024-01-02 10:00:00+00:00"
    );
    // An integer axis in milliseconds: one minute is 60,000 units.
    let ms = pipe(
        r#"{"columns": {"datetime": "t"}, "dtypes": {"t": "int"}, "precision": "ms",
                "fetch": {"backtrack_minutes": 1}}"#,
    );
    let big = 1_700_000_000_123_456_789_i64;
    assert!(matches!(
        backtrack_from(&ms, DateBound::Int(big), 1440, &d),
        DateBound::Int(v) if v == big - 60_000
    ));
    // Integer values without a dtype: the axis is an integer one, in raw minutes.
    assert!(matches!(
        backtrack_from(&dt, DateBound::Int(10_000), 1440, &d),
        DateBound::Int(8_560)
    ));
    assert!(axis_pipe(&dt, &DateBound::Int(1)).is_int_axis());
    assert!(!axis_pipe(&dt, &at(1)).is_int_axis());
    // PARITY: a datetime sync time on a pipe whose dtype says int keeps the sync time.
    let int_dtype = pipe(r#"{"columns": {"datetime": "ts"}, "dtypes": {"ts": "int"}}"#);
    assert_eq!(
        backtrack_from(&int_dtype, at(12), 1440, &d).py_str(),
        at(12).py_str()
    );
}

#[test]
fn backtrack_reads_the_source_pipes_own_window() {
    let d = PipeDefaults::default();
    let own = pipe(r#"{"columns": {"datetime": "ts"}, "fetch": {"backtrack_minutes": 120}}"#);
    assert_eq!(backtrack_minutes(&own, 1440, &d), 120);
    assert_eq!(
        backtrack_minutes(&pipe(r#"{"columns": {"datetime": "ts"}}"#), 1440, &d),
        1440
    );
    let ms = pipe(
        r#"{"columns": {"datetime": "t"}, "dtypes": {"t": "int"}, "precision": "ms",
                "fetch": {"backtrack_minutes": 2}}"#,
    );
    assert_eq!(backtrack_minutes(&ms, 1440, &d), 120_000);
}

#[test]
fn begin_only_raises_the_watermark_and_an_empty_window_reads_nothing() {
    let p = pipe(r#"{"columns": {"datetime": "ts"}}"#);
    let q = |begin: Option<DateBound>, end: Option<DateBound>| DataQuery {
        begin,
        end,
        ..Default::default()
    };
    let begin_of = |(query, _): (Option<DataQuery>, String)| query.unwrap().begin.unwrap();
    let (query, note) = incremental_query(&p, Some(at(10)), &q(Some(at(12)), None)).unwrap();
    assert_eq!(query.unwrap().begin.unwrap().py_str(), at(12).py_str());
    assert!(note.contains("--begin"), "{note}");
    let raised = incremental_query(&p, Some(at(12)), &q(Some(at(10)), None)).unwrap();
    assert_eq!(begin_of(raised).py_str(), at(12).py_str());
    // `--end` at or before the window's start: no query, so the source is never read.
    assert!(
        incremental_query(&p, Some(at(12)), &q(None, Some(at(12))))
            .unwrap()
            .0
            .is_none()
    );
    assert!(
        incremental_query(&p, None, &q(Some(at(5)), Some(at(4))))
            .unwrap()
            .0
            .is_none()
    );
    // No rows yet: everything (or from `--begin`).
    let (all, note) = incremental_query(&p, None, &q(None, None)).unwrap();
    assert!(all.unwrap().begin.is_none());
    assert!(
        note.ends_with("has no rows yet: copying all rows."),
        "{note}"
    );
    // Integer watermark, datetime `--begin`: refused, never guessed.
    assert!(incremental_query(&p, Some(DateBound::Int(5)), &q(Some(at(1)), None)).is_err());
}

/// A destination (or source) whose row count and sync times are whatever the test says.
struct Destination {
    rows: Result<Option<i64>, String>,
    /// The oldest and the newest value of the datetime axis.
    sync_times: Result<(Option<DateBound>, Option<DateBound>), String>,
}

#[async_trait::async_trait]
impl meerschaum_connector::Connector for Destination {
    fn connector_type(&self) -> &str {
        "fake"
    }

    fn label(&self) -> &str {
        "destination"
    }

    async fn test_connection(&self) -> meerschaum_connector::ConnectorResult<bool> {
        Ok(true)
    }
}

#[async_trait::async_trait]
impl meerschaum_connector::InstanceConnector for Destination {
    async fn register_pipe(&self, _: &Pipe) -> meerschaum_core::SuccessTuple {
        meerschaum_core::SuccessTuple::success()
    }

    async fn get_pipe_attributes(
        &self,
        _: &Pipe,
    ) -> meerschaum_connector::ConnectorResult<meerschaum_core::Map> {
        Ok(meerschaum_core::Map::new())
    }

    async fn get_pipe_id(
        &self,
        _: &Pipe,
    ) -> meerschaum_connector::ConnectorResult<Option<meerschaum_connector::PipeId>> {
        Ok(None)
    }

    async fn fetch_pipes_keys(
        &self,
        _: &meerschaum_connector::PipesKeysQuery,
    ) -> meerschaum_connector::ConnectorResult<meerschaum_connector::PipesKeys> {
        Ok(meerschaum_connector::PipesKeys::List(Vec::new()))
    }

    async fn pipe_exists(&self, _: &Pipe) -> meerschaum_connector::ConnectorResult<bool> {
        Ok(true)
    }

    async fn drop_pipe(&self, _: &Pipe) -> meerschaum_core::SuccessTuple {
        meerschaum_core::SuccessTuple::success()
    }

    async fn sync_pipe(
        &self,
        _: &Pipe,
        _: meerschaum_kernel::Frame,
        _: &meerschaum_connector::SyncOptions,
    ) -> meerschaum_core::SuccessTuple {
        panic!("an incremental window never writes")
    }

    async fn get_pipe_data(
        &self,
        _: &Pipe,
        _: &DataQuery,
    ) -> meerschaum_connector::ConnectorResult<Option<meerschaum_kernel::Frame>> {
        panic!("an incremental window never reads rows")
    }

    async fn get_sync_time(
        &self,
        _: &Pipe,
        _: Option<&meerschaum_core::Map>,
        newest: bool,
    ) -> meerschaum_connector::ConnectorResult<Option<DateBound>> {
        match &self.sync_times {
            Ok((oldest, newest_value)) => Ok(if newest {
                newest_value.clone()
            } else {
                oldest.clone()
            }),
            Err(e) => Err(meerschaum_connector::ConnectorError::Backend(e.clone())),
        }
    }

    async fn get_pipe_rowcount(
        &self,
        _: &Pipe,
        _: &DataQuery,
    ) -> meerschaum_connector::ConnectorResult<Option<i64>> {
        self.rows
            .clone()
            .map_err(meerschaum_connector::ConnectorError::Backend)
    }

    async fn get_pipe_columns_types(
        &self,
        _: &Pipe,
    ) -> meerschaum_connector::ConnectorResult<indexmap::IndexMap<String, String>> {
        Ok(indexmap::IndexMap::new())
    }
}

fn window(rows: Result<Option<i64>, String>) -> Result<(Option<DataQuery>, String), String> {
    let p = pipe(r#"{"columns": {"datetime": "ts"}}"#);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(incremental_window(
        &Destination {
            rows,
            sync_times: Ok((None, None)),
        },
        &p,
        &DataQuery::default(),
        1440,
        &PipeDefaults::default(),
    ))
}

#[test]
fn only_a_destination_proven_empty_is_filled() {
    // A count that cannot be determined is not an empty destination: refused with no
    // query, so the source is never read.
    let unknown = window(Ok(None)).unwrap_err();
    assert!(unknown.contains("cannot be determined"), "{unknown}");
    let failed = window(Err("connection reset".into())).unwrap_err();
    assert!(failed.contains("connection reset"), "{failed}");
    let negative = window(Ok(Some(-1))).unwrap_err();
    assert!(negative.contains("Invalid row count"), "{negative}");
    let rows = window(Ok(Some(3))).unwrap_err();
    assert!(rows.contains("has rows but no sync time"), "{rows}");
    // Counted and empty: the first fill.
    let (query, note) = window(Ok(Some(0))).unwrap();
    assert!(query.unwrap().begin.is_none());
    assert!(
        note.ends_with("has no rows yet: copying all rows."),
        "{note}"
    );
}

#[test]
fn bounds_compare_only_on_the_same_axis() {
    let p = pipe(r#"{"columns": {"datetime": "ts"}}"#);
    assert_eq!(bound_before(&at(1), &at(2), &p), Ok(true));
    assert_eq!(bound_before(&at(2), &at(2), &p), Ok(false));
    assert_eq!(
        bound_before(&DateBound::Int(1), &DateBound::Int(2), &p),
        Ok(true)
    );
    assert!(bound_before(&at(1), &DateBound::Int(2), &p).is_err());
}

#[test]
fn any_failed_pipe_fails_the_copy() {
    let s = |v: &[&str]| v.iter().map(|x| x.to_string()).collect::<Vec<_>>();
    let ok = copy_summary("sql:dest", &s(&["a", "b"]), &[], &[]);
    assert!(ok.success);
    assert_eq!(ok.message, "Copied 2 pipes.");
    let partial = copy_summary(
        "sql:dest",
        &s(&["a"]),
        &s(&["c"]),
        &[("b".to_string(), "table is locked\nmore".to_string())],
    );
    assert!(!partial.success);
    assert_eq!(
        partial.message,
        "Copied 1 of 3 pipes to 'sql:dest' (1 failed, 1 skipped).\n  - b: failed: table is locked\n  - c: skipped (already on 'sql:dest'; pass --force to overwrite)"
    );
    // Pipes already on the destination are not a failure.
    let skipped = copy_summary("sql:dest", &[], &s(&["a"]), &[]);
    assert!(skipped.success);
    assert_eq!(
        skipped.message,
        "Copied 0 of 1 pipe to 'sql:dest' (1 skipped).\n  - a: skipped (already on 'sql:dest'; pass --force to overwrite)"
    );
    assert!(!copy_summary("sql:dest", &[], &[], &[]).success);
}

/// A source on an integer axis (`axis[i]` is row `i`'s value) behind a server that answers at
/// most `limit` rows per read, silently, as `/data` does.
fn capped_reader(
    axis: Vec<i64>,
) -> impl FnMut(&DataQuery) -> meerschaum_connector::ConnectorResult<Option<meerschaum_kernel::Frame>>
{
    use meerschaum_kernel::value::Value as KValue;
    move |window: &DataQuery| {
        let inside = |v: i64| {
            window
                .begin
                .as_ref()
                .is_none_or(|b| matches!(b, DateBound::Int(b) if v >= *b))
                && window
                    .end
                    .as_ref()
                    .is_none_or(|e| matches!(e, DateBound::Int(e) if v < *e))
        };
        let docs: Vec<indexmap::IndexMap<String, KValue>> = axis
            .iter()
            .copied()
            .filter(|v| inside(*v))
            .take(window.limit.map_or(usize::MAX, |l| l as usize))
            .map(|v| [("n".to_string(), KValue::Int(v))].into_iter().collect())
            .collect();
        Ok(Some(
            meerschaum_kernel::docs::frame_from_docs(&docs).unwrap(),
        ))
    }
}

fn int_window(begin: Option<i64>, end: Option<i64>) -> DataQuery {
    DataQuery {
        begin: begin.map(DateBound::Int),
        end: end.map(DateBound::Int),
        ..Default::default()
    }
}

#[test]
fn a_full_read_is_split_until_nothing_is_cut() {
    let pipe = pipe(r#"{"columns": {"datetime": "n"}, "dtypes": {"n": "int"}}"#);
    let mut synced = 0usize;
    let res = copy_chunks(
        &pipe,
        &pipe,
        vec![
            int_window(Some(0), Some(10)),
            int_window(Some(10), Some(20)),
        ],
        Some(3),
        capped_reader((0..20).collect()),
        |df| {
            synced += df.height();
            meerschaum_core::SuccessTuple::success()
        },
    );
    assert!(res.success, "{}", res.message);
    assert_eq!(synced, 20, "rows past the server's limit were lost");
    // Without the limit the same windows read everything in two reads.
    let mut whole = 0usize;
    let res = copy_chunks(
        &pipe,
        &pipe,
        vec![
            int_window(Some(0), Some(10)),
            int_window(Some(10), Some(20)),
        ],
        None,
        capped_reader((0..20).collect()),
        |df| {
            whole += df.height();
            meerschaum_core::SuccessTuple::success()
        },
    );
    assert!(res.success && whole == 20 && res.message.contains("Synced 2 chunks"));
}

#[test]
fn a_full_read_that_cannot_be_split_fails_the_copy() {
    let pipe = pipe(r#"{"columns": {"datetime": "n"}, "dtypes": {"n": "int"}}"#);
    // Five rows at one axis value: no smaller window holds fewer.
    for window in [int_window(Some(4), Some(5)), int_window(None, None)] {
        let res = copy_chunks(
            &pipe,
            &pipe,
            vec![window],
            Some(3),
            capped_reader(vec![4; 5]),
            |_| meerschaum_core::SuccessTuple::success(),
        );
        assert!(!res.success);
        assert!(
            res.message.contains("the rest would be lost"),
            "{}",
            res.message
        );
    }
    // A read the server refused (nothing) is never an empty chunk.
    let res = copy_chunks(
        &pipe,
        &pipe,
        vec![int_window(Some(0), Some(10))],
        Some(3),
        |_: &DataQuery| Ok(None),
        |_| meerschaum_core::SuccessTuple::success(),
    );
    assert!(!res.success, "{}", res.message);
}

#[test]
fn split_points_and_the_end_past_the_newest_value() {
    assert_eq!(
        split_point(&DateBound::Int(0), &DateBound::Int(10)),
        Some(DateBound::Int(5))
    );
    assert_eq!(split_point(&DateBound::Int(4), &DateBound::Int(5)), None);
    let at = |m: u32, s: u32| {
        DateBound::Datetime(meerschaum_kernel::value::DateTimeValue {
            utc: Utc
                .with_ymd_and_hms(2024, 1, 1, 0, m, s)
                .unwrap()
                .naive_utc(),
            tz: None,
        })
    };
    assert_eq!(just_past(at(5, 30)), Some(at(6, 0)));
    assert_eq!(just_past(DateBound::Int(7)), Some(DateBound::Int(8)));
    assert_eq!(split_point(&at(0, 0), &at(2, 0)), Some(at(1, 0)));
}

fn source(sync_times: Result<(Option<DateBound>, Option<DateBound>), String>) -> Destination {
    Destination {
        rows: Ok(None),
        sync_times,
    }
}

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(future)
}

#[test]
fn windows_carry_resolved_bounds_and_an_unreadable_bound_is_an_error() {
    let dated = pipe(r#"{"columns": {"datetime": "n"}, "dtypes": {"n": "int"}}"#);
    let defaults = PipeDefaults::default();
    let all = DataQuery::default();
    // One chunk: its resolved bounds (so a limited read can split it), not an open query.
    let windows = block_on(copy_windows(
        &source(Ok((Some(DateBound::Int(0)), Some(DateBound::Int(19))))),
        &dated,
        &all,
        &defaults,
    ))
    .unwrap();
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].begin, Some(DateBound::Int(0)));
    assert_eq!(windows[0].end, Some(DateBound::Int(20)));
    // A sync time that cannot be read: an error, never the whole history.
    let unreadable = block_on(copy_windows(
        &source(Err("the source is down".into())),
        &dated,
        &all,
        &defaults,
    ));
    assert!(unreadable.unwrap_err().contains("the source is down"));
    // Explicit bounds need no sync time; a pipe without an axis is one read, as given.
    let bounded = DataQuery {
        begin: Some(DateBound::Int(3)),
        end: Some(DateBound::Int(7)),
        ..Default::default()
    };
    let windows = block_on(copy_windows(
        &source(Err("unused".into())),
        &dated,
        &bounded,
        &defaults,
    ))
    .unwrap();
    assert_eq!(
        (windows[0].begin.clone(), windows[0].end.clone()),
        (Some(DateBound::Int(3)), Some(DateBound::Int(7)))
    );
    // The newest value is the axis's largest: the window stays open at its end (no end past
    // it exists), never an end that cuts off that row.
    let windows = block_on(copy_windows(
        &source(Ok((
            Some(DateBound::Int(0)),
            Some(DateBound::Int(i64::MAX)),
        ))),
        &dated,
        &all,
        &defaults,
    ))
    .unwrap();
    assert_eq!(windows.len(), 1);
    assert!(windows[0].end.is_none(), "{windows:?}");
    let flat = pipe(r#"{"columns": {"id": "station"}}"#);
    let windows = block_on(copy_windows(
        &source(Err("unused".into())),
        &flat,
        &all,
        &defaults,
    ))
    .unwrap();
    assert_eq!(windows.len(), 1);
    assert!(windows[0].begin.is_none() && windows[0].end.is_none());
}

#[test]
fn a_limited_backtrack_copy_is_split_never_cut() {
    let p = pipe(
        r#"{"columns": {"datetime": "n"}, "dtypes": {"n": "int"}, "fetch": {"backtrack_minutes": 10}}"#,
    );
    let defaults = PipeDefaults::default();
    let src = source(Ok((Some(DateBound::Int(0)), Some(DateBound::Int(39)))));
    let windows = block_on(backtrack_windows(&src, &p, 1440, &defaults)).unwrap();
    let Some(DateBound::Int(begin)) = windows[0].begin.clone() else {
        panic!("{windows:?}");
    };
    assert!(
        begin > 0 && begin < 39,
        "the backtrack window starts at {begin}"
    );
    assert_eq!(windows.last().unwrap().end, Some(DateBound::Int(40)));
    let mut synced = 0usize;
    let res = copy_chunks(
        &p,
        &p,
        windows,
        Some(3),
        capped_reader((0..40).collect()),
        |df| {
            synced += df.height();
            meerschaum_core::SuccessTuple::success()
        },
    );
    assert!(res.success, "{}", res.message);
    assert_eq!(
        synced,
        (40 - begin) as usize,
        "rows past the limit were lost"
    );
    // No sync time to read: an error, not the server's first rows.
    assert!(
        block_on(backtrack_windows(
            &source(Err("down".into())),
            &p,
            1440,
            &defaults
        ))
        .is_err()
    );
}
