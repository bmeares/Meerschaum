//! Bounded read windows on a pipe's datetime axis: `copy pipes` and the `api:` source's chunked
//! fetch read `[begin, end)` windows and split one whose read came back as full as the server
//! allows, so no row is lost to a response limit.

use meerschaum_kernel::value::DateTimeValue;

use meerschaum_core::pipe::ChunkInterval;

use crate::traits::{ConnectorError, DateBound};

/// Just past `newest` (v4's `_get_data_as_iterator`: an integer plus one; a timestamp plus a
/// minute, rounded down to the minute), or `None` when that is past the axis's range (the
/// window then stays open at its end, so the newest row is never cut off).
pub fn just_past(newest: DateBound) -> Option<DateBound> {
    use chrono::Timelike;
    match newest {
        DateBound::Int(i) => i.checked_add(1).map(DateBound::Int),
        DateBound::Datetime(dt) => {
            let next = dt.utc.checked_add_signed(chrono::Duration::minutes(1))?;
            let floored = next
                .with_second(0)
                .and_then(|t| t.with_nanosecond(0))
                .unwrap_or(next);
            Some(DateBound::Datetime(DateTimeValue {
                utc: floored,
                tz: dt.tz,
            }))
        }
    }
}

/// A point strictly inside `[begin, end)`, or `None` when the window cannot be split (rows at
/// one axis value). The halves `[begin, mid)` and `[mid, end)` hold every row of the window once.
pub fn split_point(begin: &DateBound, end: &DateBound) -> Option<DateBound> {
    match (begin, end) {
        (DateBound::Int(b), DateBound::Int(e)) if e.checked_sub(*b)? >= 2 => {
            Some(DateBound::Int(b + (e - b) / 2))
        }
        (DateBound::Datetime(b), DateBound::Datetime(e)) => {
            let half = (e.utc - b.utc) / 2;
            (half >= chrono::Duration::microseconds(1)).then(|| {
                DateBound::Datetime(DateTimeValue {
                    utc: b.utc + half,
                    tz: b.tz.clone(),
                })
            })
        }
        _ => None,
    }
}

/// `bound + interval`, or `None` past the axis's range or for an axis and an interval of
/// different kinds.
pub fn advance(bound: &DateBound, interval: &ChunkInterval) -> Option<DateBound> {
    match (bound, interval) {
        (DateBound::Int(i), ChunkInterval::Int(width)) => i.checked_add(*width).map(DateBound::Int),
        (DateBound::Datetime(dt), ChunkInterval::Delta(delta)) => {
            dt.utc.checked_add_signed(*delta).map(|utc| {
                DateBound::Datetime(DateTimeValue {
                    utc,
                    tz: dt.tz.clone(),
                })
            })
        }
        _ => None,
    }
}

/// `a < b` on one axis (`None` for a timestamp and an integer).
pub fn before(a: &DateBound, b: &DateBound) -> Option<bool> {
    match (a, b) {
        (DateBound::Int(x), DateBound::Int(y)) => Some(x < y),
        (DateBound::Datetime(x), DateBound::Datetime(y)) => Some(x.utc < y.utc),
        _ => None,
    }
}

/// The `[begin, end)` windows of `interval` from `begin` to `end`, made one at a time (never a
/// list, however many there are). The last one ends at `end`; an interval that does not advance
/// on this axis gives one window, `[begin, end)`.
pub struct Windows {
    next: Option<DateBound>,
    end: DateBound,
    interval: ChunkInterval,
}

impl Windows {
    /// The windows from `begin` to `end`; none when `begin` is not before `end` (or they are on
    /// different axes).
    pub fn new(begin: DateBound, end: DateBound, interval: ChunkInterval) -> Windows {
        let next = (before(&begin, &end) == Some(true)).then_some(begin);
        Windows {
            next,
            end,
            interval,
        }
    }
}

impl Iterator for Windows {
    type Item = (DateBound, DateBound);

    fn next(&mut self) -> Option<Self::Item> {
        let begin = self.next.take()?;
        let end = match advance(&begin, &self.interval) {
            Some(end)
                if before(&begin, &end) == Some(true) && before(&end, &self.end) == Some(true) =>
            {
                self.next = Some(end.clone());
                end
            }
            _ => self.end.clone(),
        };
        Some((begin, end))
    }
}

/// The row limit an `api:` server named when it refused a larger one: only from its `413`
/// answer ([`ConnectorError::Http`]; v4's and v5's text "... exceeds the maximum response size
/// of N rows."), never from another error that happens to hold those words.
pub fn refused_row_limit(error: &ConnectorError) -> Option<i64> {
    let ConnectorError::Http { status: 413, body } = error else {
        return None;
    };
    let rest = &body[body.find("maximum response size of ")? + 25..];
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok().filter(|n| *n > 0)
}

/// How many times one read may lower its row limit to a server's smaller one.
pub const ROW_LIMIT_RETRIES: u32 = 4;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_split_and_end_past_the_newest_value() {
        assert_eq!(
            split_point(&DateBound::Int(0), &DateBound::Int(10)),
            Some(DateBound::Int(5))
        );
        assert_eq!(split_point(&DateBound::Int(4), &DateBound::Int(5)), None);
        assert_eq!(
            split_point(&DateBound::Int(i64::MIN), &DateBound::Int(i64::MAX)),
            None
        );
        assert_eq!(just_past(DateBound::Int(7)), Some(DateBound::Int(8)));
        // Past the axis: no end, never an end that cuts off the newest row.
        assert_eq!(just_past(DateBound::Int(i64::MAX)), None);
        let latest = DateBound::Datetime(DateTimeValue {
            utc: chrono::NaiveDateTime::MAX,
            tz: None,
        });
        assert_eq!(just_past(latest), None);
    }

    #[test]
    fn windows_are_made_one_at_a_time_up_to_the_end() {
        let all: Vec<_> = Windows::new(
            DateBound::Int(0),
            DateBound::Int(25),
            ChunkInterval::Int(10),
        )
        .collect();
        assert_eq!(
            all,
            vec![
                (DateBound::Int(0), DateBound::Int(10)),
                (DateBound::Int(10), DateBound::Int(20)),
                (DateBound::Int(20), DateBound::Int(25)),
            ]
        );
        // Near the axis's end, and an interval that does not advance: no overflow, one window.
        let last: Vec<_> = Windows::new(
            DateBound::Int(i64::MAX - 5),
            DateBound::Int(i64::MAX),
            ChunkInterval::Int(10),
        )
        .collect();
        assert_eq!(
            last,
            vec![(DateBound::Int(i64::MAX - 5), DateBound::Int(i64::MAX))]
        );
        assert_eq!(
            Windows::new(DateBound::Int(0), DateBound::Int(5), ChunkInterval::Int(0)).count(),
            1
        );
        // Billions of windows cost nothing until they are read.
        let mut many = Windows::new(
            DateBound::Int(0),
            DateBound::Int(i64::MAX),
            ChunkInterval::Int(1),
        );
        assert_eq!(many.nth(2), Some((DateBound::Int(2), DateBound::Int(3))));
        assert_eq!(
            Windows::new(DateBound::Int(5), DateBound::Int(5), ChunkInterval::Int(1)).count(),
            0
        );
    }

    #[test]
    fn only_a_413_names_a_row_limit() {
        let refusal = |status: u16| ConnectorError::Http {
            status,
            body: r#"{"detail":"Requested limit 9 exceeds the maximum response size of 3 rows."}"#
                .into(),
        };
        assert_eq!(refused_row_limit(&refusal(413)), Some(3));
        assert_eq!(refused_row_limit(&refusal(500)), None);
        assert_eq!(
            refused_row_limit(&ConnectorError::Backend(
                "the maximum response size of 3 rows".into()
            )),
            None
        );
    }
}
