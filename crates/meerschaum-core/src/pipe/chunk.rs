//! `Pipe.get_chunk_interval` and `_scale_delta_to_int_axis` (`meerschaum/core/Pipe/_data.py`).

use chrono::TimeDelta;

use super::{Pipe, PipeDefaults, PipeError};
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
    fn is_int_axis(&self) -> (Option<String>, bool) {
        let dt_col = self.column("datetime");
        let dtype = match &dt_col {
            Some(c) => self.dtype(c).unwrap_or_else(|| "datetime".to_string()),
            None => "datetime".to_string(),
        };
        (dt_col, dtype.to_lowercase().contains("int"))
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
        let (dt_col, is_int_axis) = self.is_int_axis();
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
}
