//! `Pipe.get_chunk_interval` and `_scale_delta_to_int_axis` (`meerschaum/core/Pipe/_data.py`).

use chrono::TimeDelta;

use super::{Pipe, PipeDefaults, PipeError};
use crate::dtypes::PrecisionScalar;
use crate::pyfmt::py_int;
use crate::value::Value;

/// A chunk interval: a duration for datetime axes, or a width in epoch units for integer axes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkInterval {
    /// A `timedelta`.
    Delta(TimeDelta),
    /// An `int` (integer datetime axis).
    Int(i64),
}

impl ChunkInterval {
    /// `timedelta(minutes=m)`.
    pub fn minutes(m: i64) -> ChunkInterval {
        ChunkInterval::Delta(TimeDelta::minutes(m))
    }

    /// `int(interval.total_seconds() / 60)` for durations, the value itself for integers.
    pub fn whole_minutes(&self) -> i64 {
        match self {
            ChunkInterval::Delta(d) => (total_seconds(*d) / 60.0) as i64,
            ChunkInterval::Int(i) => *i,
        }
    }
}

/// `pipes:parameters:fetch:backtrack_minutes` default (`config/_default.py`).
pub const DEFAULT_BACKTRACK_MINUTES: i64 = 1440;

/// The `chunk_interval` argument of `get_chunk_interval`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkIntervalArg {
    /// A `timedelta`.
    Delta(TimeDelta),
    /// An `int`: epoch units on an integer axis, minutes otherwise.
    Int(i64),
}

/// `timedelta.total_seconds()`.
pub fn total_seconds(delta: TimeDelta) -> f64 {
    let micros = delta.num_microseconds().unwrap_or(i64::MAX);
    micros as f64 / 1e6
}

/// Round half to even, as `timedelta` does for fractional microseconds.
fn round_half_even(x: f64) -> f64 {
    let r = x.round();
    if (x - x.trunc()).abs() == 0.5 {
        2.0 * (x / 2.0).round()
    } else {
        r
    }
}

/// `timedelta(**{unit: value})` for an `int` or `float` value. `unit_micros` is the number of
/// microseconds in one unit.
pub fn timedelta_from_value(value: &Value, unit_micros: i64) -> Result<TimeDelta, PipeError> {
    let micros = match value {
        Value::Int(i) => i.checked_mul(unit_micros),
        Value::Bool(b) => Some(i64::from(*b) * unit_micros),
        Value::Float(f) => {
            let m = round_half_even(*f * unit_micros as f64);
            (m.is_finite() && m.abs() < 9.2e18).then_some(m as i64)
        }
        other => {
            return Err(PipeError::TypeError(format!(
                "unsupported type for timedelta component: {}",
                other.type_name()
            )));
        }
    };
    let micros = micros.ok_or_else(|| PipeError::ValueError("timedelta overflow".into()))?;
    Ok(TimeDelta::microseconds(micros))
}

const MICROS_PER_SECOND: i64 = 1_000_000;
const MICROS_PER_MINUTE: i64 = 60 * MICROS_PER_SECOND;
const MICROS_PER_HOUR: i64 = 60 * MICROS_PER_MINUTE;
const MICROS_PER_DAY: i64 = 24 * MICROS_PER_HOUR;
const MICROS_PER_WEEK: i64 = 7 * MICROS_PER_DAY;

impl Pipe {
    fn int_axis_info(&self) -> (Option<String>, bool) {
        let dt_col = self.column("datetime");
        let dtype = match &dt_col {
            Some(c) => self.dtype(c).unwrap_or_else(|| "datetime".to_string()),
            None => "datetime".to_string(),
        };
        (dt_col, dtype.to_lowercase().contains("int"))
    }

    /// The datetime axis dtype (`pipe.dtypes.get(dt_col, 'datetime')`), or `None` without a
    /// datetime column.
    pub fn dt_dtype(&self) -> Option<String> {
        let dt_col = self.column("datetime")?;
        Some(
            self.dtype(&dt_col)
                .unwrap_or_else(|| "datetime".to_string()),
        )
    }

    /// Whether the pipe has an integer datetime axis (`'int' in str(dt_dtype).lower()`).
    pub fn is_int_axis(&self) -> bool {
        let (dt_col, is_int) = self.int_axis_info();
        dt_col.is_some() && is_int
    }

    /// Port of `get_backtrack_interval(check_existing)` (`core/Pipe/_fetch.py:100`): the
    /// configured `fetch.backtrack_minutes` (else `default_backtrack_minutes`, the config value
    /// `pipes:parameters:fetch:backtrack_minutes`) as a duration, or in epoch units on an
    /// integer axis (minutes verbatim when no `precision` is configured). Zero when
    /// `check_existing` is false.
    pub fn get_backtrack_interval(
        &self,
        check_existing: bool,
        default_backtrack_minutes: i64,
        defaults: &PipeDefaults,
    ) -> Result<ChunkInterval, PipeError> {
        let minutes = if check_existing {
            self.parameters()
                .get("fetch")
                .and_then(|f| f.get("backtrack_minutes"))
                .filter(|v| !v.is_null())
                .cloned()
                .unwrap_or(Value::Int(default_backtrack_minutes))
        } else {
            Value::Int(0)
        };
        let as_delta =
            || timedelta_from_value(&minutes, MICROS_PER_MINUTE).map(ChunkInterval::Delta);
        if self.column("datetime").is_none() || !self.is_int_axis() {
            return as_delta();
        }
        let int_minutes = || {
            py_int(&minutes).ok_or_else(|| {
                PipeError::TypeError(format!(
                    "invalid backtrack_minutes: {}",
                    crate::pyfmt::py_repr(&minutes)
                ))
            })
        };
        if !self
            .parameters()
            .get("precision")
            .is_some_and(Value::truthy)
        {
            return int_minutes().map(ChunkInterval::Int);
        }
        let precision = self.precision(defaults)?;
        // `int(backtrack_minutes * 60 * scalar)`: exact for integer scalars, float otherwise.
        Ok(ChunkInterval::Int(
            match (precision.unit.py_scalar(), &minutes) {
                (PrecisionScalar::Int(scalar), Value::Int(m)) => m.saturating_mul(60 * scalar),
                (scalar, _) => {
                    let m = minutes.as_f64().ok_or_else(|| {
                        PipeError::TypeError(format!(
                            "invalid backtrack_minutes: {}",
                            crate::pyfmt::py_repr(&minutes)
                        ))
                    })?;
                    (m * 60.0 * scalar.as_f64()) as i64
                }
            },
        ))
    }

    /// Port of `get_chunk_interval(chunk_interval)`.
    ///
    /// The size comes from the first present of `verify.chunk_minutes`, `chunk_hours`,
    /// `chunk_days`, `chunk_weeks`, `chunk_years` (365 days each) and `chunk_seconds`, else the
    /// configured default. An integer axis uses `verify.chunk_range` verbatim if set, otherwise
    /// scales the duration by the pipe's `precision` (legacy: minutes when no precision is set).
    pub fn get_chunk_interval(
        &self,
        chunk_interval: Option<ChunkIntervalArg>,
        defaults: &PipeDefaults,
    ) -> Result<ChunkInterval, PipeError> {
        let (dt_col, is_int_axis) = self.int_axis_info();
        let verify = self
            .parameters()
            .get("verify")
            .cloned()
            .unwrap_or_else(Value::new_map);

        if let Some(arg) = chunk_interval {
            if dt_col.is_some() && is_int_axis {
                return match arg {
                    ChunkIntervalArg::Int(i) => Ok(ChunkInterval::Int(i)),
                    ChunkIntervalArg::Delta(d) => Ok(ChunkInterval::Int(
                        self.scale_delta_to_int_axis(d, defaults)?,
                    )),
                };
            }
            let minutes = match arg {
                ChunkIntervalArg::Int(i) => i,
                ChunkIntervalArg::Delta(d) => (total_seconds(d) / 60.0) as i64,
            };
            return Ok(ChunkInterval::Delta(TimeDelta::minutes(minutes)));
        }

        if dt_col.is_some() && is_int_axis {
            if let Some(range) = verify.get("chunk_range").filter(|v| !v.is_null()) {
                return py_int(range).map(ChunkInterval::Int).ok_or_else(|| {
                    PipeError::ValueError(format!(
                        "invalid literal for int(): {}",
                        crate::pyfmt::py_repr(range)
                    ))
                });
            }
        }

        let mut chunk_delta = None;
        for (suffix, unit_micros) in [
            ("minutes", MICROS_PER_MINUTE),
            ("hours", MICROS_PER_HOUR),
            ("days", MICROS_PER_DAY),
            ("weeks", MICROS_PER_WEEK),
            ("years", 365 * MICROS_PER_DAY),
            ("seconds", MICROS_PER_SECOND),
        ] {
            let Some(val) = verify
                .get(&format!("chunk_{suffix}"))
                .filter(|v| !v.is_null())
            else {
                continue;
            };
            chunk_delta = Some(timedelta_from_value(val, unit_micros)?);
            break;
        }
        let chunk_delta = match chunk_delta {
            Some(d) => d,
            None => TimeDelta::minutes(defaults.chunk_minutes),
        };

        if dt_col.is_none() {
            return Ok(ChunkInterval::Delta(chunk_delta));
        }
        if is_int_axis {
            return Ok(ChunkInterval::Int(
                self.scale_delta_to_int_axis(chunk_delta, defaults)?,
            ));
        }
        Ok(ChunkInterval::Delta(chunk_delta))
    }

    /// Port of `_scale_delta_to_int_axis`: convert a duration into epoch units via `precision`
    /// (minutes verbatim when no `precision` is configured), at least 1.
    pub fn scale_delta_to_int_axis(
        &self,
        chunk_delta: TimeDelta,
        defaults: &PipeDefaults,
    ) -> Result<i64, PipeError> {
        let minutes = std::cmp::max((total_seconds(chunk_delta) / 60.0) as i64, 1);
        if !self
            .parameters()
            .get("precision")
            .is_some_and(Value::truthy)
        {
            return Ok(minutes);
        }
        let precision = self.precision(defaults)?;
        let scaled = (total_seconds(chunk_delta) * precision.unit.scalar()) as i64;
        Ok(std::cmp::max(scaled, 1))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipe(params: &str) -> Pipe {
        Pipe::builder("a", "b")
            .parameters(
                Value::from_json_str(params)
                    .unwrap()
                    .as_map()
                    .unwrap()
                    .clone(),
            )
            .build()
            .unwrap()
    }

    #[test]
    fn chunk_intervals() {
        let d = PipeDefaults::default();
        assert_eq!(
            pipe("{}").get_chunk_interval(None, &d).unwrap(),
            ChunkInterval::Delta(TimeDelta::minutes(43200))
        );
        assert_eq!(
            pipe(r#"{"verify": {"chunk_days": 1.5, "chunk_hours": 2}}"#)
                .get_chunk_interval(None, &d)
                .unwrap(),
            ChunkInterval::Delta(TimeDelta::hours(2))
        );
        let int_axis = r#"{"columns": {"datetime": "id"}, "dtypes": {"id": "int"}, "verify": {"chunk_days": 1}}"#;
        assert_eq!(
            pipe(int_axis).get_chunk_interval(None, &d).unwrap(),
            ChunkInterval::Int(1440)
        );
        let int_prec = r#"{"columns": {"datetime": "id"}, "dtypes": {"id": "int"}, "precision": "ms", "verify": {"chunk_days": 1}}"#;
        assert_eq!(
            pipe(int_prec).get_chunk_interval(None, &d).unwrap(),
            ChunkInterval::Int(86_400_000)
        );
    }

    #[test]
    fn backtrack_intervals() {
        let d = PipeDefaults::default();
        let bt = |p: &str, check| {
            pipe(p)
                .get_backtrack_interval(check, DEFAULT_BACKTRACK_MINUTES, &d)
                .unwrap()
        };
        assert_eq!(bt("{}", true), ChunkInterval::minutes(1440));
        assert_eq!(bt("{}", false), ChunkInterval::minutes(0));
        assert_eq!(
            bt(r#"{"fetch": {"backtrack_minutes": 5}}"#, true),
            ChunkInterval::minutes(5)
        );
        let int_axis = r#"{"columns": {"datetime": "id"}, "dtypes": {"id": "int"}, "fetch": {"backtrack_minutes": 2}}"#;
        assert_eq!(bt(int_axis, true), ChunkInterval::Int(2));
        let int_prec = r#"{"columns": {"datetime": "id"}, "dtypes": {"id": "int"}, "precision": "s", "fetch": {"backtrack_minutes": 2}}"#;
        assert_eq!(bt(int_prec, true), ChunkInterval::Int(120));
        assert_eq!(ChunkInterval::minutes(90).whole_minutes(), 90);
        assert!(pipe(int_axis).is_int_axis());
        assert_eq!(pipe(int_axis).dt_dtype().as_deref(), Some("int"));
    }
}
