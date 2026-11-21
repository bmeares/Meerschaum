//! Date-bound helpers shared by every crate that plans chunks or compares sync times:
//! ordering, timezone coercion and adding a chunk interval.

use meerschaum_core::pipe::ChunkInterval;
use meerschaum_kernel::value::DateTimeValue;

use crate::traits::DateBound;

/// How [`DateBound::checked_add_interval`] treats an integer bound with a duration, or a
/// timestamp with an integer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MixedInterval {
    /// No sum (`None`).
    Refuse,
    /// The interval counts whole minutes.
    Minutes,
}

impl DateBound {
    /// `str(bound)` of an optional bound (`None` without one).
    pub fn optional_py_str(bound: &Option<DateBound>) -> String {
        bound
            .as_ref()
            .map(DateBound::py_str)
            .unwrap_or_else(|| "None".into())
    }

    /// `self < other` for bounds of the same kind (instants compared in UTC); bounds of
    /// different kinds are never ordered.
    pub fn precedes(&self, other: &DateBound) -> bool {
        match (self, other) {
            (DateBound::Int(x), DateBound::Int(y)) => x < y,
            (DateBound::Datetime(x), DateBound::Datetime(y)) => x.utc < y.utc,
            _ => false,
        }
    }

    /// `coerce_timezone(dt, strip_utc=not aware)`: a timestamp as aware UTC (`aware`) or as
    /// naive UTC wall time; integers are unchanged.
    pub fn coerce_tz(self, aware: bool) -> DateBound {
        match self {
            DateBound::Datetime(dt) => DateBound::Datetime(if aware {
                DateTimeValue::utc(dt.utc)
            } else {
                DateTimeValue::naive(dt.utc)
            }),
            other => other,
        }
    }

    /// `bound + chunk_interval`, or `None` when the sum leaves the representable range.
    ///
    /// A timestamp takes a duration and an integer an integer. For the other two pairings,
    /// `mixed` decides: [`MixedInterval::Refuse`] answers `None` (Python raises `TypeError`),
    /// [`MixedInterval::Minutes`] reads the interval as whole minutes, as a chunk interval
    /// resolved without the axis's dtype is (`int(chunk_interval.total_seconds() / 60)`).
    pub fn checked_add_interval(
        &self,
        interval: &ChunkInterval,
        mixed: MixedInterval,
    ) -> Option<DateBound> {
        let add_delta = |dt: &DateTimeValue, delta: chrono::TimeDelta| {
            Some(DateBound::Datetime(DateTimeValue {
                utc: dt.utc.checked_add_signed(delta)?,
                tz: dt.tz.clone(),
            }))
        };
        match (self, interval, mixed) {
            (DateBound::Datetime(dt), ChunkInterval::Delta(d), _) => add_delta(dt, *d),
            (DateBound::Int(i), ChunkInterval::Int(n), _) => {
                Some(DateBound::Int(i.checked_add(*n)?))
            }
            (DateBound::Int(i), ChunkInterval::Delta(d), MixedInterval::Minutes) => {
                Some(DateBound::Int(i.checked_add(d.num_minutes())?))
            }
            (DateBound::Datetime(dt), ChunkInterval::Int(n), MixedInterval::Minutes) => {
                add_delta(dt, chrono::TimeDelta::try_minutes(*n)?)
            }
            (_, _, MixedInterval::Refuse) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    fn naive(h: u32) -> DateBound {
        DateBound::Datetime(DateTimeValue::naive(
            NaiveDate::from_ymd_opt(2024, 1, 1)
                .unwrap()
                .and_hms_opt(h, 0, 0)
                .unwrap(),
        ))
    }

    #[test]
    fn precedes_orders_bounds_of_one_kind_only() {
        assert!(DateBound::Int(1).precedes(&DateBound::Int(2)));
        assert!(!DateBound::Int(2).precedes(&DateBound::Int(2)));
        assert!(naive(1).precedes(&naive(2)));
        assert!(!DateBound::Int(1).precedes(&naive(2)));
        assert!(!naive(1).precedes(&DateBound::Int(2)));
    }

    #[test]
    fn optional_py_str_spells_a_missing_bound_none() {
        assert_eq!(DateBound::optional_py_str(&None), "None");
        assert_eq!(DateBound::optional_py_str(&Some(DateBound::Int(3))), "3");
        assert_eq!(
            DateBound::optional_py_str(&Some(naive(1))),
            "2024-01-01 01:00:00"
        );
    }

    #[test]
    fn coerce_tz_makes_timestamps_aware_or_naive_utc() {
        let aware = naive(1).coerce_tz(true);
        assert!(matches!(&aware, DateBound::Datetime(dt) if dt.tz.is_some()));
        assert_eq!(aware.coerce_tz(false), naive(1));
        assert_eq!(DateBound::Int(4).coerce_tz(true), DateBound::Int(4));
    }

    #[test]
    fn checked_add_interval_by_kind_and_policy() {
        use MixedInterval::{Minutes, Refuse};
        for mixed in [Refuse, Minutes] {
            assert_eq!(
                DateBound::Int(5).checked_add_interval(&ChunkInterval::Int(3), mixed),
                Some(DateBound::Int(8))
            );
            assert_eq!(
                naive(1).checked_add_interval(&ChunkInterval::minutes(60), mixed),
                Some(naive(2))
            );
            // Past the representable range: no sum, never a panic or a wrapped value.
            assert_eq!(
                DateBound::Int(i64::MAX).checked_add_interval(&ChunkInterval::Int(1), mixed),
                None
            );
            let max = DateBound::Datetime(DateTimeValue::naive(chrono::NaiveDateTime::MAX));
            assert_eq!(
                max.checked_add_interval(&ChunkInterval::minutes(1), mixed),
                None
            );
        }
        assert_eq!(
            DateBound::Int(5).checked_add_interval(&ChunkInterval::minutes(3), Refuse),
            None
        );
        assert_eq!(
            naive(1).checked_add_interval(&ChunkInterval::Int(60), Refuse),
            None
        );
        assert_eq!(
            DateBound::Int(5).checked_add_interval(&ChunkInterval::minutes(3), Minutes),
            Some(DateBound::Int(8))
        );
        assert_eq!(
            naive(1).checked_add_interval(&ChunkInterval::Int(60), Minutes),
            Some(naive(2))
        );
    }
}
