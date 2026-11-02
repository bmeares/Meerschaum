//! Bounded read windows on a pipe's datetime axis: `copy pipes` and the `api:` source's chunked
//! fetch read `[begin, end)` windows and split one whose read came back as full as the server
//! allows, so no row is lost to a response limit.

use meerschaum_kernel::value::DateTimeValue;

use crate::traits::DateBound;

/// Just past `newest` (v4's `_get_data_as_iterator`: an integer plus one; a timestamp plus a
/// minute, rounded down to the minute).
pub fn just_past(newest: DateBound) -> DateBound {
    use chrono::Timelike;
    match newest {
        DateBound::Int(i) => DateBound::Int(i.saturating_add(1)),
        DateBound::Datetime(dt) => {
            let next = dt.utc + chrono::Duration::minutes(1);
            let floored = next
                .with_second(0)
                .and_then(|t| t.with_nanosecond(0))
                .unwrap_or(next);
            DateBound::Datetime(DateTimeValue {
                utc: floored,
                tz: dt.tz,
            })
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

/// The row limit a server named in its refusal of a larger one (v4's and v5's text: "...
/// exceeds the maximum response size of N rows."), if `message` holds one.
pub fn refused_row_limit(message: &str) -> Option<i64> {
    let rest = &message[message.find("maximum response size of ")? + 25..];
    let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
    digits.parse().ok().filter(|n| *n > 0)
}

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
        assert_eq!(just_past(DateBound::Int(7)), DateBound::Int(8));
        assert_eq!(
            refused_row_limit(
                r#"{"detail":"Requested limit 9 exceeds the maximum response size of 3 rows."}"#
            ),
            Some(3)
        );
        assert_eq!(refused_row_limit("Not found"), None);
    }
}
