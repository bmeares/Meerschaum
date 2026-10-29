//! `dateadd_str(...)` (`meerschaum/utils/sql.py:688`) and the `dateutil`-style string parsing it
//! relies on.

use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use crate::dtypes::{get_db_type_from_pd_type, get_pd_type_from_db_type};
use crate::error::{Result, SqlError};
use crate::flavor::Flavor;
use crate::ident::clean;
use meerschaum_core::pyfmt::{PyDateTime, float_repr, py_strip};

/// `_VALID_DATEPARTS` (`utils/sql.py:685`).
pub const VALID_DATEPARTS: [&str; 6] = ["year", "month", "day", "hour", "minute", "second"];

/// The `begin` argument of `dateadd_str`.
#[derive(Debug, Clone, PartialEq)]
pub enum DateArg {
    /// `None` (or the empty string): `dateadd_str` returns `''`.
    None,
    /// An `int` axis value.
    Int(i64),
    /// A `datetime`.
    DateTime(PyDateTime),
    /// A string: `'now'`, a parseable timestamp, or a raw SQL expression (checked by `clean`).
    Str(String),
}

/// The `number` argument (`Union[int, float]`).
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Number {
    /// An `int`.
    Int(i64),
    /// A `float`.
    Float(f64),
}

impl Number {
    fn is_zero(self) -> bool {
        match self {
            Number::Int(i) => i == 0,
            Number::Float(f) => f == 0.0,
        }
    }

    fn is_positive(self) -> bool {
        match self {
            Number::Int(i) => i > 0,
            Number::Float(f) => f > 0.0,
        }
    }

    fn negated(self) -> Number {
        match self {
            Number::Int(i) => Number::Int(-i),
            Number::Float(f) => Number::Float(-f),
        }
    }

    /// Python `str(number)`.
    pub fn py_str(self) -> String {
        match self {
            Number::Int(i) => i.to_string(),
            Number::Float(f) => float_repr(f),
        }
    }
}

impl From<i64> for Number {
    fn from(i: i64) -> Self {
        Number::Int(i)
    }
}

/// Parse a timestamp string the way `dateutil.parser.parse` does for the formats Meerschaum
/// produces and users commonly pass (ISO 8601 with `T` or space, optional fraction, `Z`/`UTC`/
/// `±HH:MM`/`±HHMM`/`±HH` offsets, bare dates, `/` separators).
///
/// PARITY: `dateutil` accepts many more free-form inputs (`'Jan 1 2022'`, a bare day `'1'`).
/// Those fall through to `clean()` here instead of being quoted; see the crate report.
pub fn parse_datetime_str(s: &str) -> Option<PyDateTime> {
    let t = py_strip(s);
    if t.is_empty() {
        return None;
    }
    let normalized = t.replacen('T', " ", 1).replace('/', "-");
    let (body, tz) = split_tz_suffix(&normalized);
    let body = body.trim_end();
    let naive = parse_naive(body)?;
    Some(match tz {
        None => PyDateTime::Naive(naive),
        Some(offset) => {
            PyDateTime::Aware(DateTime::from_naive_utc_and_offset(naive - offset, offset))
        }
    })
}

fn parse_naive(body: &str) -> Option<NaiveDateTime> {
    if let Ok(d) = NaiveDate::parse_from_str(body, "%Y-%m-%d") {
        return Some(d.and_time(NaiveTime::MIN));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S%.f", "%Y-%m-%d %H:%M"] {
        if let Ok(n) = NaiveDateTime::parse_from_str(body, fmt) {
            return Some(n);
        }
    }
    None
}

/// Split a trailing timezone designator off a timestamp string.
fn split_tz_suffix(s: &str) -> (&str, Option<FixedOffset>) {
    let utc = FixedOffset::east_opt(0).expect("zero offset");
    for suffix in [" UTC", "UTC", "Z", " Z"] {
        if let Some(body) = s.strip_suffix(suffix) {
            return (body, Some(utc));
        }
    }
    // Look for `+HH:MM`, `+HHMM` or `+HH` after the time part.
    let Some(time_start) = s.find(':') else {
        return (s, None);
    };
    let tail = &s[time_start..];
    if let Some(pos) = tail.rfind(['+', '-']) {
        let off_str = &tail[pos + 1..];
        let sign = if tail.as_bytes()[pos] == b'-' { -1 } else { 1 };
        let digits: String = off_str.chars().filter(|c| c.is_ascii_digit()).collect();
        let ok_shape = off_str.chars().all(|c| c.is_ascii_digit() || c == ':')
            && matches!(digits.len(), 2 | 4);
        if ok_shape {
            let hours: i32 = digits[..2].parse().ok().unwrap_or(0);
            let minutes: i32 = if digits.len() == 4 {
                digits[2..].parse().ok().unwrap_or(0)
            } else {
                0
            };
            if let Some(off) = FixedOffset::east_opt(sign * (hours * 3600 + minutes * 60)) {
                return (&s[..time_start + pos], Some(off));
            }
        }
    }
    (s, None)
}

/// Execution equivalent for bundled DuckDB, whose TIMESTAMPTZ interval overload needs ICU.
/// Arithmetic on UTC wall time keeps the same instant without loading extensions. The public
/// v4 query renderer stays byte-for-byte compatible with the reference fixtures.
pub(crate) fn dateadd_for_execution(
    flavor: Option<Flavor>,
    datepart: Option<&str>,
    number: Number,
    begin: &DateArg,
    db_type: Option<&str>,
) -> Result<String> {
    let sql = dateadd_str(flavor, datepart, number, begin, db_type)?;
    #[cfg(feature = "duckdb")]
    if flavor == Some(Flavor::Duckdb)
        && let Some((base, interval)) = sql.rsplit_once(" + INTERVAL ")
        && (base.contains("TIMESTAMPTZ")
            || base.contains("TIMESTAMP WITH TIME ZONE")
            || base == "NOW()")
    {
        return Ok(format!(
            "CAST(CAST({base} AS TIMESTAMP) + INTERVAL {interval} AS TIMESTAMPTZ)"
        ));
    }
    Ok(sql)
}

/// `dateadd_str(flavor, datepart, number, begin, db_type)`.
///
/// `flavor = None` reproduces Python being handed a key that is not a flavor (e.g. the
/// `'sqlite_delete_insert'` pseudo-flavor): the result is `''` for non-integer `begin`.
pub fn dateadd_str(
    flavor: Option<Flavor>,
    datepart: Option<&str>,
    number: Number,
    begin: &DateArg,
    db_type: Option<&str>,
) -> Result<String> {
    if let DateArg::Int(i) = begin {
        let mut num_str = i.to_string();
        if !number.is_zero() {
            if number.is_positive() {
                num_str.push_str(&format!(" + {}", number.py_str()));
            } else {
                num_str.push_str(&format!(" - {}", number.negated().py_str()));
            }
        }
        return Ok(num_str);
    }
    let datepart = match datepart {
        Some(d) if VALID_DATEPARTS.contains(&d) => d,
        other => {
            return Err(SqlError::InvalidDatepart(
                other.unwrap_or("None").to_string(),
            ));
        }
    };

    let (raw_begin, begin_time): (String, Option<PyDateTime>) = match begin {
        DateArg::None => return Ok(String::new()),
        DateArg::Str(s) if s.is_empty() => return Ok(String::new()),
        DateArg::Str(s) => (s.clone(), parse_datetime_str(s)),
        DateArg::DateTime(dt) => (String::new(), Some(*dt)),
        DateArg::Int(_) => unreachable!("handled above"),
    };

    // `begin` is the (possibly quoted) SQL fragment from here on.
    let begin_sql: String = match (&begin_time, begin) {
        // Unable to parse into a datetime: throw if banned symbols are included.
        (None, _) => {
            clean(&raw_begin)?;
            raw_begin.clone()
        }
        (Some(_), DateArg::DateTime(dt)) => {
            let dt = if dt.is_aware() { dt.to_utc() } else { *dt };
            if flavor.is_some_and(Flavor::timezone_naive) {
                format!("'{}'", dt.strip_tz().py_str())
            } else {
                format!("'{}'", dt.py_str())
            }
        }
        (Some(_), _) => format!("'{raw_begin}'"),
    };

    let mut dt_is_utc = match &begin_time {
        Some(bt) => bt.is_aware(),
        None => {
            let after_colon = raw_begin
                .split_once(':')
                .map(|(_, r)| r)
                .unwrap_or(&raw_begin);
            raw_begin.contains('+') || after_colon.contains('-')
        }
    };
    let db_type: String = match db_type.filter(|d| !d.is_empty()) {
        Some(d) => {
            let db_type_is_utc = get_pd_type_from_db_type(d, true)
                .to_lowercase()
                .contains("utc");
            dt_is_utc = dt_is_utc || db_type_is_utc;
            d.to_string()
        }
        None => get_db_type_from_pd_type(
            if dt_is_utc {
                "datetime64[ns, UTC]"
            } else {
                "datetime64[ns]"
            },
            flavor,
        ),
    };

    let number_str = number.py_str();
    let is_now = begin_sql == "now";
    let da = match flavor {
        Some(
            Flavor::Postgresql
            | Flavor::Postgis
            | Flavor::Timescaledb
            | Flavor::TimescaledbHa
            | Flavor::Cockroachdb
            | Flavor::Citus,
        ) => {
            let b = if !is_now {
                format!("CAST({begin_sql} AS {db_type})")
            } else {
                format!("CAST(NOW() AT TIME ZONE 'utc' AS {db_type})")
            };
            if number.is_zero() {
                b
            } else {
                format!("{b} + INTERVAL '{number_str} {datepart}'")
            }
        }
        Some(Flavor::Duckdb) => {
            let b = if !is_now {
                format!("CAST({begin_sql} AS {db_type})")
            } else {
                "NOW()".to_string()
            };
            if number.is_zero() {
                b
            } else {
                format!("{b} + INTERVAL '{number_str} {datepart}'")
            }
        }
        Some(Flavor::Mssql) => {
            let mut b = begin_sql.clone();
            if let Some(bt) = &begin_time
                && bt.microsecond() != 0
                && !dt_is_utc
            {
                // PARITY: `begin[:-4] + "'"` trims to milliseconds (`utils/sql.py:837`).
                let chars: Vec<char> = b.chars().collect();
                let keep = chars.len().saturating_sub(4);
                b = chars[..keep].iter().collect::<String>() + "'";
            }
            let mut b = if b != "now" {
                format!("CAST({b} AS {db_type})")
            } else {
                "GETUTCDATE()".to_string()
            };
            if dt_is_utc {
                b.push_str(" AT TIME ZONE 'UTC'");
            }
            if number.is_zero() {
                b
            } else {
                format!("DATEADD({datepart}, {number_str}, {b})")
            }
        }
        Some(Flavor::Mysql | Flavor::Mariadb) => {
            let b = if !is_now {
                format!("CAST({begin_sql} AS DATETIME(6))")
            } else {
                "UTC_TIMESTAMP(6)".to_string()
            };
            if number.is_zero() {
                b
            } else {
                format!("DATE_ADD({b}, INTERVAL {number_str} {datepart})")
            }
        }
        Some(Flavor::Sqlite | Flavor::Geopackage) => {
            format!("datetime({begin_sql}, '{number_str} {datepart}')")
        }
        Some(Flavor::Oracle) => {
            let b = if is_now {
                // PARITY: the format string `'%Y:%m:%d %M:%S.%f'` (colons, no hour) and the
                // unquoted result are reproduced from `utils/sql.py:857`.
                let now = PyDateTime::utc(Utc::now()).naive_local();
                now.format("%Y:%m:%d %M:%S.%6f").to_string()
            } else if let Some(bt) = &begin_time {
                // PARITY: uses the *original* datetime (not converted to UTC) (`utils/sql.py:860`).
                bt.strftime_full()
            } else {
                begin_sql.clone()
            };
            let dt_format = "YYYY-MM-DD HH24:MI:SS.FF";
            let quoted = if begin_time.is_some() {
                format!("'{b}'")
            } else {
                b
            };
            let mut da = if begin_time.is_some() {
                format!("TO_TIMESTAMP({quoted}, '{dt_format}')")
            } else {
                quoted
            };
            if !number.is_zero() {
                da.push_str(&format!(" + INTERVAL '{number_str}' {datepart}"));
            }
            da
        }
        None => String::new(),
    };
    Ok(da)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dt(s: &str) -> DateArg {
        DateArg::DateTime(PyDateTime::parse_isoformat(s).unwrap())
    }

    #[test]
    fn docstring_examples() {
        assert_eq!(
            dateadd_str(
                Some(Flavor::Mssql),
                Some("day"),
                Number::Int(1),
                &dt("2022-01-01T00:00:00"),
                None
            )
            .unwrap(),
            "DATEADD(day, 1, CAST('2022-01-01 00:00:00' AS DATETIME2(7)))"
        );
        assert_eq!(
            dateadd_str(
                Some(Flavor::Postgresql),
                Some("day"),
                Number::Int(1),
                &dt("2022-01-01T00:00:00"),
                None
            )
            .unwrap(),
            "CAST('2022-01-01 00:00:00' AS TIMESTAMP(6)) + INTERVAL '1 day'"
        );
    }

    #[test]
    fn integer_axis() {
        assert_eq!(
            dateadd_str(
                Some(Flavor::Sqlite),
                None,
                Number::Int(-5),
                &DateArg::Int(10),
                None
            )
            .unwrap(),
            "10 - 5"
        );
    }

    #[test]
    fn invalid_datepart() {
        assert!(
            dateadd_str(
                Some(Flavor::Sqlite),
                Some("week"),
                Number::Int(1),
                &DateArg::Str("now".into()),
                None
            )
            .is_err()
        );
    }

    #[test]
    fn parse_strings() {
        let p = parse_datetime_str("2022-01-01 00:00:00+00:00").unwrap();
        assert!(p.is_aware());
        let p = parse_datetime_str("2022-01-01").unwrap();
        assert!(!p.is_aware());
        assert!(parse_datetime_str("now").is_none());
    }
}
