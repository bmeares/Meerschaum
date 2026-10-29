//! The `type=` callables of the v4 parser and the Python builtins they rely on.
//!
//! Ports of `parse_datetime`, `parse_executor_keys` and `parse_name`
//! (`meerschaum/_internal/arguments/_parser.py`), `string_to_dict` (`meerschaum/utils/misc.py`),
//! and the builtins `int()`, `float()` and `pathlib.Path()` as `argparse` calls them.

use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime, TimeZone, Utc};
use regex::Regex;
use std::sync::LazyLock;

use meerschaum_core::pyfmt::py_strip;

use crate::dateutil;
use crate::pyjson;
use crate::pyliteral;
use crate::value::{PyException, PyValue};

/// Clock inputs for relative and partial datetimes, so results are reproducible in tests.
#[derive(Debug, Clone)]
pub struct DateContext {
    /// `datetime.now(timezone.utc)` as a naive UTC datetime (`parse_datetime`'s `now`).
    pub now_utc: NaiveDateTime,
    /// `dateutil`'s `default`: today's local date at midnight.
    pub dateutil_default: NaiveDateTime,
    /// The local year (`dateutil`'s two-digit-year pivot).
    pub local_year: i32,
}

impl DateContext {
    /// The current clock.
    pub fn system() -> Self {
        let local = Local::now();
        let today = local.date_naive();
        Self {
            now_utc: Utc::now().naive_utc(),
            dateutil_default: today.and_hms_opt(0, 0, 0).unwrap_or_default(),
            local_year: local.year(),
        }
    }
}

/// How a type function failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConvError {
    /// `TypeError` / `ValueError`: `argparse` reports `invalid <type> value: '<arg>'`.
    Invalid,
    /// Any other exception, which escapes `argparse` unchanged (e.g. `error()` raising `Exception`).
    Raise(PyException),
}

/// Build an `int` from ASCII decimal digits with an optional sign (no underscores).
pub fn int_from_decimal_digits(text: &str) -> PyValue {
    let (neg, digits) = match text.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, text.strip_prefix('+').unwrap_or(text)),
    };
    let trimmed = digits.trim_start_matches('0');
    let trimmed = if trimmed.is_empty() { "0" } else { trimmed };
    match trimmed.parse::<i128>() {
        Ok(v) => PyValue::Int(if neg { -v } else { v }),
        Err(_) => PyValue::BigInt(if neg {
            format!("-{trimmed}")
        } else {
            trimmed.to_string()
        }),
    }
}

/// Validate digits with PEP 515 underscores (only single underscores between digits).
fn strip_underscores(s: &str) -> Option<String> {
    if s.is_empty() || s.starts_with('_') || s.ends_with('_') || s.contains("__") {
        return None;
    }
    Some(s.replace('_', ""))
}

/// Python `int(s)` (base 10). `None` means `ValueError`.
pub fn py_int(s: &str) -> Option<PyValue> {
    let s = py_strip(s);
    let (sign, body) = match s.chars().next() {
        Some('-') => ("-", &s[1..]),
        Some('+') => ("", &s[1..]),
        _ => ("", s),
    };
    let digits = strip_underscores(body)?;
    if !digits.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(int_from_decimal_digits(&format!("{sign}{digits}")))
}

/// Python `float(s)`. `None` means `ValueError`.
pub fn py_float(s: &str) -> Option<f64> {
    let s = py_strip(s);
    let (neg, body) = match s.chars().next() {
        Some('-') => (true, &s[1..]),
        Some('+') => (false, &s[1..]),
        _ => (false, s),
    };
    let lower = body.to_ascii_lowercase();
    let value = match lower.as_str() {
        "inf" | "infinity" => f64::INFINITY,
        "nan" => f64::NAN,
        _ => {
            let (mantissa, exponent) = match lower.find('e') {
                Some(ix) => (&lower[..ix], Some(&lower[ix + 1..])),
                None => (lower.as_str(), None),
            };
            let (int_part, frac_part) = match mantissa.split_once('.') {
                Some((i, f)) => (i, Some(f)),
                None => (mantissa, None),
            };
            let int_digits = if int_part.is_empty() {
                String::new()
            } else {
                strip_underscores(int_part)?
            };
            let frac_digits = match frac_part {
                Some("") | None => String::new(),
                Some(f) => strip_underscores(f)?,
            };
            if int_digits.is_empty() && frac_digits.is_empty() {
                return None;
            }
            if !int_digits
                .chars()
                .chain(frac_digits.chars())
                .all(|c| c.is_ascii_digit())
            {
                return None;
            }
            let mut text = format!(
                "{}.{}",
                if int_digits.is_empty() {
                    "0"
                } else {
                    &int_digits
                },
                if frac_digits.is_empty() {
                    "0"
                } else {
                    &frac_digits
                }
            );
            if let Some(exp) = exponent {
                let (esign, edigits) = match exp.chars().next() {
                    Some('-') => ("-", &exp[1..]),
                    Some('+') => ("", &exp[1..]),
                    _ => ("", exp),
                };
                let edigits = strip_underscores(edigits)?;
                if !edigits.chars().all(|c| c.is_ascii_digit()) {
                    return None;
                }
                text.push_str(&format!("e{esign}{edigits}"));
            }
            text.parse::<f64>().ok()?
        }
    };
    Some(if neg { -value } else { value })
}

/// `meerschaum.utils.misc.is_int`: `float(s).is_integer()`.
pub fn is_int(s: &str) -> bool {
    match py_float(s) {
        Some(f) => f.is_finite() && f.fract() == 0.0,
        None => false,
    }
}

/// `pathlib.Path(s)` → `str(path)` on POSIX.
pub fn py_path(s: &str) -> String {
    let root = if s.starts_with("//") && !s.starts_with("///") {
        "//"
    } else if s.starts_with('/') {
        "/"
    } else {
        ""
    };
    let parts: Vec<&str> = s
        .split('/')
        .filter(|p| !p.is_empty() && *p != ".")
        .collect();
    let joined = format!("{root}{}", parts.join("/"));
    if joined.is_empty() {
        ".".into()
    } else {
        joined
    }
}

/// `parse_name`: an empty `--name` becomes `None`.
pub fn parse_name(name: &str) -> Result<PyValue, ConvError> {
    if name.is_empty() {
        return Ok(PyValue::None);
    }
    Ok(PyValue::str(name))
}

/// `parse_executor_keys`.
pub fn parse_executor_keys(keys: &str) -> Result<PyValue, ConvError> {
    if keys == "local" || keys == "systemd" {
        return Ok(PyValue::str(keys));
    }
    if keys.to_lowercase() == "none" {
        return Ok(PyValue::str("local"));
    }
    // `<type>:<label>` of an executor type a native plugin declares (Meerschaum 5).
    if let Some((kind, label)) = keys.split_once(':')
        && !kind.is_empty()
        && !label.is_empty()
        && kind != "api"
        && crate::native_executors::is_native_executor_type(kind)
    {
        return Ok(PyValue::str(keys));
    }
    if !keys.starts_with("api:") {
        // PARITY: the typo "exectutor" is in the v4 message (`_parser.py` `parse_executor_keys`).
        return Err(ConvError::Raise(PyException::new(
            "Exception",
            format!("Invalid exectutor keys '{keys}'."),
        )));
    }
    Ok(PyValue::str(keys))
}

/// `string_to_dict`: JSON when the string starts with `{`, else `key:value,key:value`.
pub fn string_to_dict(params: &str) -> Result<PyValue, ConvError> {
    if params.is_empty() {
        return Ok(PyValue::Dict(vec![]));
    }
    let chars: Vec<char> = params.chars().collect();
    let n = chars.len();
    // PARITY: "Kind of a weird edge case" in `misc.py` `string_to_dict`: a string whose second and
    // second-to-last characters are braces is parsed as JSON without its first and last characters.
    if n > 4 && chars[1] == '{' && chars[n - 2] == '}' {
        let inner: String = chars[1..n - 1].iter().collect();
        return pyjson::loads(&inner).map_err(|_| ConvError::Invalid);
    }
    if params.starts_with('{') {
        return pyjson::loads(params).map_err(|_| ConvError::Invalid);
    }

    let mut items: Vec<String> = Vec::new();
    let mut bracket_level = 0i32;
    let mut brace_level = 0i32;
    let mut current = String::new();
    let mut in_quotes = false;
    let mut quote_char = '\0';
    for i in 0..n {
        let c = chars[i];
        if in_quotes {
            if c == quote_char && (i == 0 || chars[i - 1] != '\\') {
                in_quotes = false;
            }
        } else if c == '"' || c == '\'' {
            in_quotes = true;
            quote_char = c;
        } else if c == '[' {
            bracket_level += 1;
        } else if c == ']' {
            bracket_level -= 1;
        } else if c == '{' {
            brace_level += 1;
        } else if c == '}' {
            brace_level -= 1;
        } else if c == ',' && bracket_level == 0 && brace_level == 0 {
            items.push(std::mem::take(&mut current));
            continue;
        }
        current.push(c);
    }
    if !current.is_empty() {
        items.push(current);
    }

    let mut out: Vec<(PyValue, PyValue)> = Vec::new();
    for param in items {
        let param = py_strip(&param);
        if param.is_empty() {
            continue;
        }
        let Some((key, raw_value)) = param.split_once(':') else {
            continue;
        };
        // PARITY: `keys = _keys[:-1]` always has one element, so the nested-key loop in
        // `string_to_dict` never runs and keys are never `literal_eval`ed.
        let value = pyliteral::literal_eval(raw_value).unwrap_or_else(|_| PyValue::str(raw_value));
        if let Some(slot) = out.iter_mut().find(|(k, _)| k.as_str() == Some(key)) {
            slot.1 = value;
        } else {
            out.push((PyValue::str(key), value));
        }
    }
    Ok(PyValue::Dict(out))
}

static AGO_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(\d+)\s+(days?|minutes?|seconds?|hours?|weeks?|months?|years?)\s+ago")
        .expect("valid regex")
});
static ROUND_RE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(\d+)\s+(days?|minutes?|seconds?|hours?|weeks?|years?)").expect("valid regex")
});

fn digits_to_i128(s: &str) -> Option<i128> {
    let mut v: i128 = 0;
    for c in s.chars() {
        let d = c.to_digit(10)? as i128;
        v = v.checked_mul(10)?.checked_add(d)?;
    }
    Some(v)
}

/// Microseconds since `datetime.min` (0001-01-01).
fn micros_since_min(dt: &NaiveDateTime) -> i128 {
    let min = NaiveDate::from_ymd_opt(1, 1, 1)
        .and_then(|d| d.and_hms_opt(0, 0, 0))
        .unwrap_or_default();
    let delta = *dt - min;
    delta.num_microseconds().map(i128::from).unwrap_or(0)
}

/// `dt + timedelta(microseconds=delta)` with Python's datetime range (years 1 through 9999).
pub fn add_micros(dt: &NaiveDateTime, delta: i128) -> Option<NaiveDateTime> {
    let delta = i64::try_from(delta).ok()?;
    let out = dt.checked_add_signed(Duration::microseconds(delta))?;
    if !(1..=9999).contains(&out.year()) {
        return None;
    }
    Some(out)
}

/// `meerschaum.utils.dtypes.round_time(dt, date_delta, to='down')`.
fn round_time_down(dt: &NaiveDateTime, delta_micros: i128) -> Option<NaiveDateTime> {
    if delta_micros == 0 {
        return Some(*dt);
    }
    let total = micros_since_min(dt);
    // `Decimal.to_integral_value(ROUND_DOWN)` truncates toward zero.
    let intervals = total / delta_micros;
    let adjustment = intervals * delta_micros - total;
    add_micros(dt, adjustment)
}

/// `calendar.monthrange(year, month)[1]`.
pub fn days_in_month(year: i64, month: i64) -> Option<i64> {
    if !(1..=12).contains(&month) {
        return None;
    }
    Some(match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        _ => {
            let leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
            if leap { 29 } else { 28 }
        }
    })
}

/// `now - relativedelta(months=months)`, keeping the time and clamping the day.
fn sub_months(dt: &NaiveDateTime, months: i128) -> Option<NaiveDateTime> {
    let total = dt.year() as i128 * 12 + (dt.month0() as i128) - months;
    let year = total.div_euclid(12);
    let month = total.rem_euclid(12) + 1;
    if !(1..=9999).contains(&year) {
        return None;
    }
    let dim = days_in_month(year as i64, month as i64)?;
    let day = (dt.day() as i64).min(dim);
    NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32).map(|d| d.and_time(dt.time()))
}

fn invalid_datetime(dt_str: &str) -> ConvError {
    ConvError::Raise(PyException::new(
        "Exception",
        format!("'{dt_str}' is not a valid datetime format."),
    ))
}

/// `parse_datetime`: an `int`, `'None'`, `now`, `<n> <unit> ago [<n> <unit>]`, or anything
/// `dateutil.parser.parse` accepts (coerced to UTC).
pub fn parse_datetime(dt_str: &str, ctx: &DateContext) -> Result<PyValue, ConvError> {
    if is_int(dt_str) {
        // PARITY: `is_int('1.0')` is true but `int('1.0')` raises, which `argparse` reports as
        // `invalid parse_datetime value` (`_parser.py` `parse_datetime`).
        return py_int(dt_str).ok_or(ConvError::Invalid);
    }
    if dt_str == "None" || dt_str == "[None]" {
        return Ok(PyValue::str("None"));
    }

    let lower = dt_str.to_lowercase();
    let now = ctx.now_utc;
    let dt: Option<NaiveDateTime> = if lower == "now" {
        Some(now)
    } else if let Some(caps) = AGO_RE.captures(&lower) {
        relative_ago(&lower, &caps, &now)
    } else {
        match dateutil::parse(dt_str, ctx) {
            Ok((naive, offset)) => match offset {
                Some(seconds) => add_micros(&naive, -(seconds as i128) * 1_000_000),
                None => Some(naive),
            },
            Err(_) => None,
        }
    };
    let Some(dt) = dt else {
        return Err(invalid_datetime(dt_str));
    };
    Ok(PyValue::DateTime(Utc.from_utc_datetime(&dt)))
}

fn unit_micros(unit: &str) -> Option<i128> {
    Some(match unit {
        "seconds" => 1_000_000,
        "minutes" => 60_000_000,
        "hours" => 3_600_000_000,
        "days" => 86_400_000_000,
        "weeks" => 7 * 86_400_000_000,
        _ => return None,
    })
}

fn relative_ago(
    lower: &str,
    caps: &regex::Captures<'_>,
    now: &NaiveDateTime,
) -> Option<NaiveDateTime> {
    let val = digits_to_i128(&caps[1])?;
    let mut unit = caps[2].to_string();
    if !unit.ends_with('s') {
        unit.push('s');
    }
    // PARITY: `lower.split('ago ')[-1]` is the whole string when it ends in "ago", so
    // "10 days ago" rounds down to a multiple of 10 days (`_parser.py` `parse_datetime`).
    let round_part = match lower.rfind("ago ") {
        Some(ix) => &lower[ix + 4..],
        None => lower,
    };
    let mut round_micros: Option<i128> = None;
    if !round_part.is_empty()
        && let Some(rc) = ROUND_RE.captures(round_part)
    {
        let round_val = digits_to_i128(&rc[1])?;
        let mut round_unit = rc[2].to_string();
        if !round_unit.ends_with('s') {
            round_unit.push('s');
        }
        // PARITY: `timedelta(years=...)` raises `TypeError`, so any "<n> years" rounding (which
        // includes every "<n> years ago") is reported as an invalid datetime.
        let per = unit_micros(&round_unit)?;
        let micros = round_val.checked_mul(per)?;
        if micros / 86_400_000_000 > crate::value::TIMEDELTA_MAX_DAYS {
            return None;
        }
        round_micros = Some(micros);
    }

    let dt = match unit.as_str() {
        "months" => sub_months(now, val)?,
        "years" => sub_months(now, val.checked_mul(12)?)?,
        other => {
            let micros = val.checked_mul(unit_micros(other)?)?;
            if micros / 86_400_000_000 > crate::value::TIMEDELTA_MAX_DAYS {
                return None;
            }
            add_micros(now, -micros)?
        }
    };
    match round_micros {
        Some(micros) => round_time_down(&dt, micros),
        None => Some(dt),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_int_and_float() {
        assert_eq!(py_int(" 7 "), Some(PyValue::Int(7)));
        assert_eq!(py_int("1_000"), Some(PyValue::Int(1000)));
        assert_eq!(py_int("3.0"), None);
        assert_eq!(py_int("0x10"), None);
        assert_eq!(py_float("5."), Some(5.0));
        assert_eq!(py_float(".5"), Some(0.5));
        assert_eq!(py_float("1_0.5"), Some(10.5));
        assert!(py_float("abc").is_none());
        assert!(is_int("1e3"));
        assert!(!is_int("nan"));
    }

    #[test]
    fn pathlib_normalization() {
        assert_eq!(py_path(""), ".");
        assert_eq!(py_path("./x//y/"), "x/y");
        assert_eq!(py_path("..//a/./b"), "../a/b");
        assert_eq!(py_path("//net/share"), "//net/share");
        assert_eq!(py_path("///a"), "/a");
    }
}
