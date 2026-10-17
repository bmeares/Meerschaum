//! Response serialization of pipe data: `to_json(df, date_format, date_unit, double_precision,
//! geometry_format)` (`GET .../data`), `df.to_csv(index=False)` (`GET .../csv`), documents
//! (`GET .../docs`) and `json_serialize_value` bounds.
//!
//! Cells go through `meerschaum_connector_api::wire::to_json_value` (the pandas `to_json` port) except
//! native datetime columns, which pandas renders itself: ISO 8601 with the `date_unit`'s
//! fractional digits and a `Z` suffix for tz-aware columns, or epoch integers in `date_unit`.

use meerschaum_connector::DateBound;
use meerschaum_connector_api::wire::{ToJsonOptions, to_json_value, ujson_string};
use meerschaum_kernel::Frame;
use meerschaum_kernel::frame::ColumnData;
use meerschaum_kernel::serialize::GeometryFormat;
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

/// `date_unit` (`s`, `ms`, `us`, `ns`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DateUnit {
    /// Seconds.
    S,
    /// Milliseconds.
    Ms,
    /// Microseconds (the default).
    Us,
    /// Nanoseconds.
    Ns,
}

impl DateUnit {
    /// Parse `date_unit` (pandas raises `ValueError` otherwise).
    pub fn parse(s: &str) -> Option<DateUnit> {
        match s {
            "s" => Some(DateUnit::S),
            "ms" => Some(DateUnit::Ms),
            "us" => Some(DateUnit::Us),
            "ns" => Some(DateUnit::Ns),
            _ => None,
        }
    }

    fn digits(self) -> usize {
        match self {
            DateUnit::S => 0,
            DateUnit::Ms => 3,
            DateUnit::Us => 6,
            DateUnit::Ns => 9,
        }
    }
}

/// The options of `GET .../data`.
#[derive(Debug, Clone, Copy)]
pub struct DataJsonOptions {
    /// `date_format='epoch'` instead of `'iso'`.
    pub epoch: bool,
    /// `date_unit`.
    pub date_unit: DateUnit,
    /// `double_precision` (at most 15).
    pub double_precision: u32,
    /// `geometry_format`.
    pub geometry_format: GeometryFormat,
}

fn native_datetime(dt: &DateTimeValue, aware: bool, opts: &DataJsonOptions) -> String {
    let micros = dt.utc.and_utc().timestamp_micros();
    if opts.epoch {
        let v = match opts.date_unit {
            DateUnit::S => micros.div_euclid(1_000_000),
            DateUnit::Ms => micros.div_euclid(1_000),
            DateUnit::Us => micros,
            DateUnit::Ns => micros.saturating_mul(1_000),
        };
        return v.to_string();
    }
    let base = dt.utc.format("%Y-%m-%dT%H:%M:%S").to_string();
    let frac_micros = micros.rem_euclid(1_000_000);
    let frac = match opts.date_unit {
        DateUnit::S => String::new(),
        DateUnit::Ms => format!(".{:03}", frac_micros / 1000),
        DateUnit::Us => format!(".{frac_micros:06}"),
        DateUnit::Ns => format!(".{:09}", frac_micros * 1000),
    };
    let _ = opts.date_unit.digits();
    format!("\"{base}{frac}{}\"", if aware { "Z" } else { "" })
}

/// `meerschaum.utils.dataframe.to_json(df, orient='records', ...)` for `GET .../data`.
pub fn data_to_json(df: &Frame, opts: &DataJsonOptions) -> String {
    let wire_opts = ToJsonOptions {
        columns_orient: false,
        double_precision: opts.double_precision.min(15),
        geometry_format: opts.geometry_format,
        epoch_dates: opts.epoch,
    };
    let columns: Vec<(String, bool, Option<bool>, Vec<KValue>)> = df
        .columns()
        .iter()
        .map(|c| {
            let dtype = c.data.dtype_str();
            let native_dt =
                matches!(c.data, ColumnData::Native(_)) && dtype.starts_with("datetime64");
            let aware = native_dt.then(|| dtype.contains(','));
            (c.name.clone(), native_dt, aware, c.data.values())
        })
        .collect();
    let rows: Vec<String> = (0..df.height())
        .map(|i| {
            let cells: Vec<String> = columns
                .iter()
                .map(|(name, native_dt, aware, values)| {
                    let v = &values[i];
                    let cell = match v {
                        KValue::Datetime(dt) if *native_dt => {
                            native_datetime(dt, aware.unwrap_or(false), opts)
                        }
                        _ => to_json_value(v, &wire_opts),
                    };
                    format!("{}:{cell}", ujson_string(name))
                })
                .collect();
            format!("{{{}}}", cells.join(","))
        })
        .collect();
    format!("[{}]", rows.join(","))
}

/// One CSV cell as pandas' `to_csv` writes it (`str()` of the value, nulls empty, timestamps as
/// `str(pd.Timestamp)`).
fn csv_cell(v: &KValue) -> String {
    if v.is_na() {
        return String::new();
    }
    match v {
        KValue::Float(f) => meerschaum_core::pyfmt::float_repr(*f),
        other => other.py_str(),
    }
}

/// `csv.QUOTE_MINIMAL` quoting.
fn csv_quote(s: &str) -> String {
    if s.contains([',', '"', '\n', '\r']) {
        format!("\"{}\"", s.replace('"', "\"\""))
    } else {
        s.to_string()
    }
}

/// `df.to_csv(index=False)`.
pub fn to_csv(df: &Frame) -> String {
    let mut out = String::new();
    let names = df.names();
    out.push_str(
        &names
            .iter()
            .map(|n| csv_quote(n))
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push('\n');
    let columns: Vec<Vec<KValue>> = df.columns().iter().map(|c| c.data.values()).collect();
    for i in 0..df.height() {
        let row: Vec<String> = columns
            .iter()
            .map(|vals| csv_quote(&csv_cell(&vals[i])))
            .collect();
        out.push_str(&row.join(","));
        out.push('\n');
    }
    out
}

/// `json_serialize_value(bound)`: `isoformat()` for timestamps, the integer otherwise.
pub fn bound_json(b: &Option<DateBound>) -> meerschaum_core::Value {
    match b {
        None => meerschaum_core::Value::Null,
        Some(DateBound::Int(i)) => meerschaum_core::Value::Int(*i),
        Some(DateBound::Datetime(dt)) => meerschaum_core::Value::Str(dt.isoformat()),
    }
}

/// Python's `str(bound)` (`2024-01-01 00:00:00+00:00`).
pub fn bound_str(b: &DateBound) -> String {
    b.py_str()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn pandas_native_datetimes() {
        let dt = DateTimeValue::utc(
            NaiveDate::from_ymd_opt(2024, 1, 2)
                .unwrap()
                .and_hms_milli_opt(12, 30, 0, 250)
                .unwrap(),
        );
        let mut o = DataJsonOptions {
            epoch: false,
            date_unit: DateUnit::Us,
            double_precision: 15,
            geometry_format: GeometryFormat::WkbHex,
        };
        assert_eq!(
            native_datetime(&dt, true, &o),
            "\"2024-01-02T12:30:00.250000Z\""
        );
        o.date_unit = DateUnit::Ms;
        assert_eq!(
            native_datetime(&dt, false, &o),
            "\"2024-01-02T12:30:00.250\""
        );
        o.date_unit = DateUnit::S;
        assert_eq!(native_datetime(&dt, true, &o), "\"2024-01-02T12:30:00Z\"");
        o.epoch = true;
        assert_eq!(native_datetime(&dt, true, &o), "1704198600");
        o.date_unit = DateUnit::Ms;
        assert_eq!(native_datetime(&dt, true, &o), "1704198600250");
    }

    #[test]
    fn csv_quoting() {
        assert_eq!(csv_quote("a,b"), "\"a,b\"");
        assert_eq!(csv_quote("plain"), "plain");
        assert_eq!(csv_cell(&KValue::Float(1e20)), "1e+20");
        assert_eq!(csv_cell(&KValue::Null), "");
    }
}
