//! `enforce_dtypes(coerce_timezone=False)` (PARITY H9): a Polars-native cast still puts the
//! datetime axis in UTC, because `_enforce_dtypes_with_polars` ignores `coerce_timezone`.

use indexmap::IndexMap;
use meerschaum_kernel::enforce::{EnforceOptions, enforce_dtypes};
use meerschaum_kernel::frame::{Column, Frame};
use meerschaum_kernel::value::{DateTimeValue, Value};
use polars::prelude::*;

fn dtypes(pairs: &[(&str, &str)]) -> IndexMap<String, String> {
    pairs
        .iter()
        .map(|(c, t)| (c.to_string(), t.to_string()))
        .collect()
}

fn naive_micros(micros: i64) -> Series {
    Int64Chunked::from_iter_values("dt".into(), [micros].into_iter())
        .into_datetime(TimeUnit::Microseconds, None)
        .into_series()
}

#[test]
fn native_datetime_cast_without_timezone_coercion_is_utc() {
    let frame = Frame::new(vec![
        Column::native(naive_micros(1_767_225_600_000_000)),
        Column::object("id", vec![Value::Int(1)]),
    ])
    .unwrap();
    let opts = EnforceOptions {
        coerce_timezone: false,
        ..Default::default()
    };
    let out = enforce_dtypes(&frame, &dtypes(&[("dt", "datetime"), ("id", "int")]), &opts).unwrap();
    assert_eq!(out.dtypes()["dt"], "datetime64[us, UTC]");
}

#[test]
fn untyped_columns_still_cast_the_native_datetime_subset_to_utc() {
    let naive = DateTimeValue::naive(
        chrono::NaiveDate::from_ymd_opt(2026, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap(),
    );
    let frame = Frame::new(vec![
        Column::object("dt", vec![Value::Datetime(naive)]),
        Column::object("note", vec![Value::Str("x".into())]),
    ])
    .unwrap();
    let opts = EnforceOptions {
        coerce_timezone: false,
        ..Default::default()
    };
    // `note` is untyped, so the object path runs (and skips the timezone step), but the typed
    // `dt` column goes through the native subset first, which always ends up in UTC.
    let out = enforce_dtypes(&frame, &dtypes(&[("dt", "datetime")]), &opts).unwrap();
    assert_eq!(out.dtypes()["dt"], "datetime64[us, UTC]");
    assert_eq!(out.dtypes()["note"], "object");
}
