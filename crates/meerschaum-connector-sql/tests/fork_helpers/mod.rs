//! Data generators shared by the maintenance / partition / verify / dedup tests (synthetic
//! stand-ins for the Python suite's `plugin:stress` fetch).
#![allow(dead_code)]

use chrono::{Duration, NaiveDateTime};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

use crate::common::{Doc, doc, i, naive};

/// `plugin:stress` with `rows` and `id`: `rows` evenly spaced rows in `[begin, end)`, ids
/// `1..=num_ids`, deterministic values.
pub fn stress_docs(begin: NaiveDateTime, end: NaiveDateTime, rows: i64, num_ids: i64) -> Vec<Doc> {
    let span = (end - begin).num_seconds();
    (0..rows)
        .map(|k| {
            let dt = begin + Duration::seconds(span * k / rows);
            // Truncate to the minute like the stress plugin's rounded timestamps.
            let dt = dt - Duration::seconds(dt.and_utc().timestamp() % 60);
            doc(vec![
                ("datetime", KValue::Datetime(DateTimeValue::naive(dt))),
                ("id", i(k % num_ids + 1)),
                ("val", KValue::Float((k * 7 % 100) as f64 + 0.5)),
            ])
        })
        .collect()
}

/// `_deterministic_data()` of `tests/test_partition.py`: hourly rows over four days.
pub fn partition_data() -> Vec<Doc> {
    let mut rows = Vec::new();
    let mut cursor = naive(2023, 1, 1, 0, 0, 0);
    let end = naive(2023, 1, 5, 0, 0, 0);
    while cursor < end {
        use chrono::{Datelike, Timelike};
        rows.push(doc(vec![
            ("datetime", KValue::Datetime(DateTimeValue::naive(cursor))),
            ("id", i((cursor.day() % 3) as i64 + 1)),
            ("val", i(cursor.hour() as i64)),
        ]));
        cursor += Duration::hours(1);
    }
    rows
}

/// The naive (UTC) datetime of a row's `datetime` column.
pub fn row_dt(d: &Doc, col: &str) -> NaiveDateTime {
    match d.get(col) {
        Some(KValue::Datetime(dt)) => dt.utc,
        other => panic!("not a datetime: {other:?}"),
    }
}
