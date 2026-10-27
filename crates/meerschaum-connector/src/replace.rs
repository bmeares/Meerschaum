//! Replacing syncs ([`SyncOptions::replace`], rc15): the frame's rows replace the pipe's rows of
//! a window `[begin, end)` (matching `params`). The instances share the window rules here; each
//! deletes the window and writes the rows its own way (SQL in one transaction).

use meerschaum_core::success::SyncResult;
use meerschaum_core::{Map, Pipe};
use meerschaum_kernel::Frame;
use meerschaum_kernel::value::Value as KValue;

use crate::traits::{DateBound, SyncOptions};

/// The rows of `df` inside a replacing sync's window (`[opts.begin, opts.end)` on the pipe's
/// datetime column `dt_col`, matching `opts.params`) and how many rows were left out.
///
/// Without bounds every row is in the window; with a bound, a row whose datetime is null is
/// not. `literal_null_markers` are the columns whose null-like strings are text (see
/// `meerschaum_core::dtypes::null_marker_text_columns`), as the instance's `params` filters
/// read them.
pub fn rows_in_window(
    dt_col: Option<&str>,
    df: &Frame,
    opts: &SyncOptions,
    literal_null_markers: &[String],
) -> Result<(Frame, usize), String> {
    use std::cmp::Ordering;
    let mut keep: Vec<usize> = (0..df.height()).collect();
    if let Some(dt) = dt_col.filter(|c| df.contains(c))
        && (opts.begin.is_some() || opts.end.is_some())
    {
        let values = df.data(dt).map_err(|e| e.to_string())?.values();
        let cmp = |v: &KValue, b: &DateBound| -> Option<Ordering> {
            match (v, b) {
                (KValue::Datetime(d), DateBound::Datetime(b)) => Some(d.utc.cmp(&b.utc)),
                (KValue::Date(d), DateBound::Datetime(b)) => {
                    d.and_hms_opt(0, 0, 0).map(|n| n.cmp(&b.utc))
                }
                (KValue::Int(i), DateBound::Int(b)) => Some(i.cmp(b)),
                (KValue::Float(f), DateBound::Int(b)) => f.partial_cmp(&(*b as f64)),
                (KValue::Str(s), DateBound::Datetime(b)) => {
                    meerschaum_kernel::value::parse_datetime_value(s).map(|d| d.utc.cmp(&b.utc))
                }
                (KValue::Str(s), DateBound::Int(b)) => {
                    s.trim().parse::<i64>().ok().map(|i| i.cmp(b))
                }
                _ => None,
            }
        };
        keep.retain(|&i| {
            let v = &values[i];
            if v.is_na() {
                return false;
            }
            let after_begin = opts
                .begin
                .as_ref()
                .is_none_or(|b| cmp(v, b).is_some_and(|o| o != Ordering::Less));
            let before_end = opts
                .end
                .as_ref()
                .is_none_or(|e| cmp(v, e).is_some_and(|o| o == Ordering::Less));
            after_begin && before_end
        });
    }
    let mut inside = if keep.len() == df.height() {
        df.clone()
    } else {
        df.take(&keep)
    };
    if let Some(params) = opts.params.as_ref().filter(|p| !p.is_empty()) {
        let params: Map = params
            .iter()
            .filter(|(k, _)| inside.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        if !params.is_empty() {
            let result = meerschaum_kernel::query::query_df(
                &inside,
                &meerschaum_kernel::query::QueryOptions {
                    params: meerschaum_kernel::query::params_from_core(&params),
                    reset_index: true,
                    literal_null_markers: literal_null_markers.to_vec(),
                    ..Default::default()
                },
            )
            .map_err(|e| e.to_string())?;
            inside = result.frame;
        }
    }
    let skipped = df.height() - inside.height();
    Ok((inside, skipped))
}

/// How one side of a replacing sync's window was given.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum WindowBound {
    /// Not given: the rows' own datetime range decides that side (see [`resolve_window`]).
    #[default]
    FromRows,
    /// Given as `None`: no bound on that side.
    Unbounded,
    /// Given.
    At(DateBound),
}

impl WindowBound {
    /// `Some(bound)` → [`WindowBound::At`], `None` → [`WindowBound::FromRows`] (a bound that
    /// was not given, as `SQLConnector.sync_pipe(begin=None)`).
    pub fn given_or_from_rows(bound: Option<DateBound>) -> WindowBound {
        match bound {
            Some(b) => WindowBound::At(b),
            None => WindowBound::FromRows,
        }
    }
}

/// The message when a window cannot be derived because there are no rows.
pub fn no_rows_window(pipe: &Pipe) -> String {
    format!(
        "Cannot replace a window of {pipe} without rows: pass `begin` and `end` to clear [begin, end)."
    )
}

/// The window `[begin, end)` a replacing sync of the rows `df` replaces (the rule
/// `pipe.sync(df, replace=True)`, `SQLConnector.sync_pipe(..., replace=True)` and the API's
/// `POST .../data?replace=true` share).
///
/// A given bound is used as given ([`WindowBound::Unbounded`] is no bound on that side). A
/// `begin` that was not given is the rows' earliest datetime, an `end` that was not given is
/// just after their latest (`+1` on an integer axis, `+1 µs` on a datetime axis), so every row
/// passed is inside. Without rows, an `end` that was not given is unbounded when `begin` was
/// given, and a window with neither bound given is refused: nothing says what to clear (an
/// empty frame never clears a whole table by accident).
///
/// A pipe without a datetime column has one window, the whole table: bounds are refused.
pub fn resolve_window(
    pipe: &Pipe,
    df: &Frame,
    begin: WindowBound,
    end: WindowBound,
) -> Result<(Option<DateBound>, Option<DateBound>), String> {
    let given_begin = begin != WindowBound::FromRows;
    let Some(dt_col) = pipe.column("datetime") else {
        if given_begin || end != WindowBound::FromRows {
            return Err(no_datetime_window(pipe));
        }
        return Ok((None, None));
    };
    let (rows_min, rows_max) = rows_datetime_range(df, &dt_col);
    let begin = match begin {
        WindowBound::At(b) => Some(b),
        WindowBound::Unbounded => None,
        WindowBound::FromRows => Some(rows_min.ok_or_else(|| no_rows_window(pipe))?),
    };
    let end = match end {
        WindowBound::At(e) => Some(e),
        WindowBound::Unbounded => None,
        WindowBound::FromRows => match rows_max {
            Some(e) => Some(e),
            None if given_begin => None,
            None => return Err(no_rows_window(pipe)),
        },
    };
    Ok((begin, end))
}

/// The earliest datetime of a frame's datetime column and the bound just after its latest
/// (`+1` on an integer axis, `+1 µs` on a datetime axis): `None` for no non-null values.
/// Datetime strings (a frame that was not dtype-enforced) are parsed.
pub fn rows_datetime_range(df: &Frame, dt_col: &str) -> (Option<DateBound>, Option<DateBound>) {
    let Some(column) = df.column(dt_col) else {
        return (None, None);
    };
    let as_bound = |v: &KValue| -> Option<DateBound> {
        match v {
            KValue::Datetime(d) => Some(DateBound::Datetime(d.clone())),
            KValue::Date(d) => Some(DateBound::Datetime(
                meerschaum_kernel::value::DateTimeValue::naive(d.and_hms_opt(0, 0, 0)?),
            )),
            KValue::Int(i) => Some(DateBound::Int(*i)),
            KValue::Float(f) if f.is_finite() && f.fract() == 0.0 => {
                Some(DateBound::Int(*f as i64))
            }
            KValue::Str(s) => DateBound::parse(s),
            _ => None,
        }
    };
    let mut bounds: Vec<DateBound> = column
        .data
        .values()
        .iter()
        .filter(|v| !v.is_na())
        .filter_map(as_bound)
        .collect();
    bounds.sort_by(|a, b| crate::pipe_ops::compare_values(&a.to_value(), &b.to_value()));
    let after = |b: &DateBound| -> DateBound {
        match b {
            DateBound::Datetime(d) => {
                let mut d = d.clone();
                d.utc += chrono::TimeDelta::microseconds(1);
                DateBound::Datetime(d)
            }
            DateBound::Int(i) => DateBound::Int(i.saturating_add(1)),
        }
    };
    (bounds.first().cloned(), bounds.last().map(after))
}

/// `1,234`.
fn thousands(n: u64) -> String {
    let digits = n.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

/// The result of a replacing sync: `Replaced {deleted} rows with {inserted} rows.` (the engine
/// reads the counts back from this message), plus the rows of the frame that were outside the
/// window and so not written.
pub fn replace_result(deleted: u64, inserted: u64, skipped: usize, success: bool) -> SyncResult {
    let rows = |n: u64| format!("{} row{}", thousands(n), if n == 1 { "" } else { "s" });
    let mut message = format!("Replaced {} with {}.", rows(deleted), rows(inserted));
    if skipped > 0 {
        message.push_str(&format!(
            "\nSkipped {} outside the window.",
            rows(skipped as u64)
        ));
    }
    let mut result = SyncResult::new(success, message);
    result.inserted = inserted;
    result.deleted = deleted;
    result
}

/// The refusal when a window is asked of a pipe without a datetime column (its only window is
/// the whole table).
pub fn no_datetime_window(pipe: &Pipe) -> String {
    format!(
        "Cannot replace a window of {pipe}: it has no datetime column (`columns: {{datetime: ...}}`). Without `begin` and `end`, `replace=True` replaces the whole table."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_kernel::frame::Column;
    use meerschaum_kernel::value::DateTimeValue;

    fn dt(d: u32) -> KValue {
        KValue::Datetime(DateTimeValue::naive(
            chrono::NaiveDate::from_ymd_opt(2024, 1, d)
                .unwrap()
                .and_hms_opt(0, 0, 0)
                .unwrap(),
        ))
    }

    fn bound(d: u32) -> DateBound {
        match dt(d) {
            KValue::Datetime(v) => DateBound::Datetime(v),
            _ => unreachable!(),
        }
    }

    #[test]
    fn rows_outside_the_window_are_left_out() {
        let df = Frame::new(vec![
            Column::object("ts", vec![dt(1), dt(2), dt(3), KValue::Null]),
            Column::object(
                "id",
                vec![
                    KValue::Str("a".into()),
                    KValue::Str("NA".into()),
                    KValue::Str("a".into()),
                    KValue::Str("a".into()),
                ],
            ),
        ])
        .unwrap();
        let opts = SyncOptions {
            begin: Some(bound(2)),
            end: Some(bound(3)),
            replace: true,
            ..Default::default()
        };
        let (inside, skipped) = rows_in_window(Some("ts"), &df, &opts, &[]).unwrap();
        assert_eq!((inside.height(), skipped), (1, 3));
        // No bounds: every row (NULL datetimes included).
        let (inside, skipped) =
            rows_in_window(Some("ts"), &df, &SyncOptions::default(), &[]).unwrap();
        assert_eq!((inside.height(), skipped), (4, 0));
        // `params` with a text index column: 'NA' is the value NA.
        let mut params = Map::new();
        params.insert("id".into(), meerschaum_core::Value::Str("NA".into()));
        let opts = SyncOptions {
            params: Some(params),
            ..Default::default()
        };
        let (inside, _) = rows_in_window(Some("ts"), &df, &opts, &["id".to_string()]).unwrap();
        assert_eq!(inside.height(), 1);
    }

    fn pipe(columns: &str) -> Pipe {
        let params = meerschaum_core::Value::from_json_str(&format!(r#"{{"columns": {columns}}}"#))
            .unwrap()
            .to_map();
        Pipe::builder("a", "b")
            .instance("sql:x")
            .parameters(params)
            .build()
            .unwrap()
    }

    #[test]
    fn windows_not_given_come_from_the_rows() {
        let p = pipe(r#"{"datetime": "ts"}"#);
        let df = Frame::new(vec![Column::object(
            "ts",
            vec![
                dt(3),
                KValue::Null,
                dt(1),
                KValue::Str("2024-01-02 00:00:00".into()),
            ],
        )])
        .unwrap();
        let after = |d: u32| match bound(d) {
            DateBound::Datetime(mut v) => {
                v.utc += chrono::TimeDelta::microseconds(1);
                DateBound::Datetime(v)
            }
            other => other,
        };
        // Neither bound given: [min, max + 1 µs).
        assert_eq!(
            resolve_window(&p, &df, WindowBound::FromRows, WindowBound::FromRows).unwrap(),
            (Some(bound(1)), Some(after(3)))
        );
        // Given bounds are kept; `Unbounded` is no bound.
        assert_eq!(
            resolve_window(&p, &df, WindowBound::At(bound(2)), WindowBound::Unbounded).unwrap(),
            (Some(bound(2)), None)
        );
        // No rows: a missing `end` is unbounded after a given `begin`; nothing given is refused.
        let empty = df.clear();
        assert_eq!(
            resolve_window(&p, &empty, WindowBound::At(bound(2)), WindowBound::FromRows).unwrap(),
            (Some(bound(2)), None)
        );
        let err =
            resolve_window(&p, &empty, WindowBound::FromRows, WindowBound::FromRows).unwrap_err();
        assert!(err.contains("without rows"), "{err}");
        // An integer axis ends one past the largest value.
        let df = Frame::new(vec![Column::object(
            "ts",
            vec![KValue::Int(5), KValue::Int(2)],
        )])
        .unwrap();
        assert_eq!(
            resolve_window(&p, &df, WindowBound::FromRows, WindowBound::FromRows).unwrap(),
            (Some(DateBound::Int(2)), Some(DateBound::Int(6)))
        );
        // Without a datetime column the window is the whole table, and bounds are refused.
        let p = pipe(r#"{"id": "id"}"#);
        assert_eq!(
            resolve_window(&p, &empty, WindowBound::FromRows, WindowBound::FromRows).unwrap(),
            (None, None)
        );
        assert!(
            resolve_window(&p, &empty, WindowBound::At(bound(1)), WindowBound::FromRows).is_err()
        );
    }

    #[test]
    fn replace_messages_carry_the_counts() {
        let r = replace_result(1234, 1, 2, true);
        assert_eq!(
            r.message,
            "Replaced 1,234 rows with 1 row.\nSkipped 2 rows outside the window."
        );
        assert_eq!((r.deleted, r.inserted), (1234, 1));
    }
}
