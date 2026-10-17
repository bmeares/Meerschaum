//! Python `datetime` formatting used by the job layout: `isoformat()`, `strftime()` and a port of
//! the `strptime()` matcher `strip_timestamp_from_line` relies on.

use chrono::format::{Item, StrftimeItems};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use meerschaum_core::pyfmt::PyDateTime;
use regex::Regex;

/// `datetime.now(timezone.utc)`.
pub fn utcnow() -> DateTime<Utc> {
    Utc::now()
}

/// `dt.replace(tzinfo=None).isoformat()` (the `process.*` timestamps):
/// `YYYY-MM-DDTHH:MM:SS[.ffffff]`.
pub fn naive_isoformat(dt: DateTime<Utc>) -> String {
    PyDateTime::Naive(dt.naive_utc()).isoformat()
}

/// `dt.isoformat()` of an aware UTC datetime (the stop file's `stop_time`):
/// `YYYY-MM-DDTHH:MM:SS[.ffffff]+00:00`.
pub fn aware_isoformat(dt: DateTime<Utc>) -> String {
    PyDateTime::utc(dt).isoformat()
}

/// `datetime.fromisoformat(s)`.
pub fn fromisoformat(s: &str) -> Option<PyDateTime> {
    PyDateTime::parse_isoformat(s)
}

/// `dt.strftime(fmt)`. Unsupported directives are emitted verbatim.
pub fn strftime(dt: NaiveDateTime, fmt: &str) -> String {
    let items: Vec<Item<'_>> = StrftimeItems::new(fmt).collect();
    if items.iter().any(|i| matches!(i, Item::Error)) {
        return fmt.to_string();
    }
    dt.format_with_items(items.into_iter()).to_string()
}

/// The prefix `RotatingFile` and the interceptor inject: `strftime(fmt) + ' | '`.
pub fn timestamp_prefix(dt: DateTime<Utc>, fmt: &str) -> String {
    format!("{} | ", strftime(dt.naive_utc(), fmt))
}

/// Whether `datetime.strptime(s, fmt)` succeeds (Python's `_strptime` regex rules for the
/// directives jobs use: `%Y %m %d %H %M %S %f %y %%`).
pub fn strptime_matches(s: &str, fmt: &str) -> bool {
    let mut pattern = String::from("^(?i)");
    let mut fields: Vec<char> = Vec::new();
    let mut chars = fmt.chars().peekable();
    let mut pending_space = false;
    while let Some(c) = chars.next() {
        if c.is_whitespace() {
            // PARITY: `_strptime` replaces whitespace runs in the format with `\s+`.
            if !pending_space {
                pattern.push_str(r"\s+");
                pending_space = true;
            }
            continue;
        }
        pending_space = false;
        if c != '%' {
            pattern.push_str(&regex::escape(&c.to_string()));
            continue;
        }
        let Some(d) = chars.next() else {
            return false;
        };
        let group = match d {
            'Y' => r"(\d\d\d\d)",
            'm' => r"(1[0-2]|0[1-9]|[1-9])",
            'd' => r"(3[01]|[12]\d|0[1-9]|[1-9]| [1-9])",
            'H' => r"(2[0-3]|[0-1]\d|\d)",
            'M' => r"([0-5]\d|\d)",
            'S' => r"(6[0-1]|[0-5]\d|\d)",
            'f' => r"(\d{1,6})",
            'y' => r"(\d\d)",
            '%' => {
                pattern.push('%');
                continue;
            }
            _ => {
                // Fall back to chrono for directives outside the subset.
                return NaiveDateTime::parse_from_str(s, fmt).is_ok();
            }
        };
        fields.push(d);
        pattern.push_str(group);
    }
    pattern.push('$');
    let Ok(re) = Regex::new(&pattern) else {
        return false;
    };
    let Some(caps) = re.captures(s) else {
        return false;
    };
    let mut year = 1900;
    let mut month = 1;
    let mut day = 1;
    for (i, d) in fields.iter().enumerate() {
        let v = caps.get(i + 1).map(|m| m.as_str().trim()).unwrap_or("");
        let n: i32 = match d {
            'f' => 0,
            _ => v.parse().unwrap_or(0),
        };
        match d {
            'Y' => year = n,
            'y' => year = if n <= 68 { 2000 + n } else { 1900 + n },
            'm' => month = n as u32,
            'd' => day = n as u32,
            _ => {}
        }
    }
    // PARITY: `strptime` rejects impossible dates (`ValueError: day is out of range`).
    NaiveDate::from_ymd_opt(year, month, day).is_some()
}

/// Port of `meerschaum.utils.formatting._jobs.strip_timestamp_from_line`.
pub fn strip_timestamp_from_line(line: &str, timestamp_format: &str) -> String {
    let now_str = strftime(utcnow().naive_utc(), timestamp_format);
    let n = now_str.chars().count();
    let prefix: String = line.chars().take(n).collect();
    if strptime_matches(&prefix, timestamp_format) {
        // PARITY: `line[(len(now_str) + 3):]` assumes the ` | ` separator.
        return line.chars().skip(n + 3).collect();
    }
    line.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn isoformats() {
        let dt = Utc.with_ymd_and_hms(2026, 10, 12, 18, 50, 42).unwrap();
        assert_eq!(naive_isoformat(dt), "2026-10-12T18:50:42");
        assert_eq!(aware_isoformat(dt), "2026-10-12T18:50:42+00:00");
        let dt = dt + chrono::Duration::microseconds(147653);
        assert_eq!(naive_isoformat(dt), "2026-10-12T18:50:42.147653");
        assert_eq!(
            timestamp_prefix(dt, "%Y-%m-%d %H:%M"),
            "2026-10-12 18:50 | "
        );
    }

    #[test]
    fn strptime_subset() {
        let fmt = "%Y-%m-%d %H:%M";
        assert!(strptime_matches("2024-12-31 23:59", fmt));
        assert!(!strptime_matches("2024-13-31 23:59", fmt));
        assert!(!strptime_matches("2024-02-30 23:59", fmt));
        assert!(!strptime_matches("   2024-01-01 0", fmt));
    }
}
