//! Python-compatible wire encoding helpers (`urllib.parse.quote_plus` / `urlencode`, and the way
//! `requests` turns a `params=` dict into a query string).

/// `urllib.parse.quote_plus(s)`: unreserved characters (`A-Za-z0-9_.-~`) stay, spaces become `+`,
/// everything else is percent-encoded from UTF-8 with upper-case hex.
pub fn quote_plus(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

/// `urllib.parse.unquote_plus(s)`.
pub fn unquote_plus(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let hex = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                match u8::from_str_radix(hex, 16) {
                    Ok(v) => {
                        out.push(v);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            other => {
                out.push(other);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// A query parameter value as `requests` encodes it: `None` drops the key, a list repeats it,
/// anything else is `str(value)`.
#[derive(Debug, Clone, PartialEq)]
pub enum QueryValue {
    /// `None`: the key is omitted.
    None,
    /// `str(value)`.
    One(String),
    /// A list: one `key=value` pair per item.
    Many(Vec<String>),
}

impl QueryValue {
    /// `str(True)` / `str(False)`.
    pub fn bool(b: bool) -> QueryValue {
        QueryValue::One(py_bool(b).to_string())
    }

    /// An optional string (`None` drops the key).
    pub fn opt(s: Option<impl Into<String>>) -> QueryValue {
        match s {
            Some(s) => QueryValue::One(s.into()),
            None => QueryValue::None,
        }
    }
}

impl From<&str> for QueryValue {
    fn from(s: &str) -> Self {
        QueryValue::One(s.to_string())
    }
}

impl From<String> for QueryValue {
    fn from(s: String) -> Self {
        QueryValue::One(s)
    }
}

impl From<i64> for QueryValue {
    fn from(i: i64) -> Self {
        QueryValue::One(i.to_string())
    }
}

/// Python's `str(bool)`.
pub fn py_bool(b: bool) -> &'static str {
    if b { "True" } else { "False" }
}

/// `requests`' encoding of `params=` (an ordered dict): `urlencode(..., doseq=True)` with `None`
/// values dropped. Returns the query string without the leading `?` (empty when nothing remains).
pub fn encode_query(params: &[(&str, QueryValue)]) -> String {
    let mut parts = Vec::new();
    for (k, v) in params {
        match v {
            QueryValue::None => {}
            QueryValue::One(s) => parts.push(format!("{}={}", quote_plus(k), quote_plus(s))),
            QueryValue::Many(items) => {
                for s in items {
                    parts.push(format!("{}={}", quote_plus(k), quote_plus(s)));
                }
            }
        }
    }
    parts.join("&")
}

/// `urlencode(dict)` for a form body (`data=` a dict).
pub fn encode_form(fields: &[(&str, &str)]) -> String {
    fields
        .iter()
        .map(|(k, v)| format!("{}={}", quote_plus(k), quote_plus(v)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Parse a query string into ordered pairs (`parse_qsl(..., keep_blank_values=True)`).
pub fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|p| !p.is_empty())
        .map(|p| match p.split_once('=') {
            Some((k, v)) => (unquote_plus(k), unquote_plus(v)),
            None => (unquote_plus(p), String::new()),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn python_quoting() {
        assert_eq!(quote_plus("[\"a b\", 1]"), "%5B%22a+b%22%2C+1%5D");
        assert_eq!(quote_plus("sql:main/x~y"), "sql%3Amain%2Fx~y");
        assert_eq!(quote_plus("é"), "%C3%A9");
        assert_eq!(unquote_plus("%5B%22a+b%22%2C+1%5D"), "[\"a b\", 1]");
        assert_eq!(
            encode_query(&[
                ("a", "1".into()),
                ("b", QueryValue::None),
                ("c", QueryValue::Many(vec!["x".into(), "y z".into()])),
                ("d", QueryValue::bool(true)),
            ]),
            "a=1&c=x&c=y+z&d=True"
        );
        assert_eq!(
            parse_query("a=1&c=x&c=y+z"),
            vec![
                ("a".to_string(), "1".to_string()),
                ("c".to_string(), "x".to_string()),
                ("c".to_string(), "y z".to_string())
            ]
        );
    }
}

// ------------------------------------------------------------------------------------------------
// `meerschaum.utils.dataframe.to_json()` (pandas' vendored ujson encoder)
// ------------------------------------------------------------------------------------------------

use meerschaum_kernel::Frame;
use meerschaum_kernel::serialize::{GeometryFormat, GeometryOutput, serialize_geometry};
use meerschaum_kernel::value::Value as KValue;

/// C's `printf("%.{p}g", v)` for a finite, positive `v`.
fn c_format_g(v: f64, p: usize) -> String {
    let p = p.max(1);
    let sci = format!("{:.*e}", p - 1, v);
    let (mantissa, exp) = sci.split_once('e').unwrap_or((&sci, "0"));
    let exp: i32 = exp.parse().unwrap_or(0);
    let strip = |s: String| -> String {
        if s.contains('.') {
            s.trim_end_matches('0').trim_end_matches('.').to_string()
        } else {
            s
        }
    };
    if exp < -4 || exp >= p as i32 {
        let m = strip(mantissa.to_string());
        let sign = if exp < 0 { '-' } else { '+' };
        format!("{m}e{sign}{:02}", exp.abs())
    } else {
        let decimals = (p as i32 - 1 - exp).max(0) as usize;
        strip(format!("{v:.decimals$}"))
    }
}

/// pandas' ujson `Buffer_AppendDoubleUnchecked(value)` with `double_precision`: fixed notation
/// with at most `precision` decimals (trailing zeros dropped, at least `.0`), `%.{precision}g`
/// outside `[1e-15, 1e16)`. `NaN` / infinities are `null` (`to_json` fills them as `NA`).
pub fn ujson_double(value: f64, precision: u32) -> String {
    if !value.is_finite() {
        return "null".into();
    }
    let precision = precision.min(15);
    let thres_max = 1e16 - 1.0;
    let thres_min = 1e-15;
    let neg = value < 0.0;
    let v = value.abs();
    let sign = if neg { "-" } else { "" };
    if v > thres_max || (v != 0.0 && v < thres_min) {
        return format!("{sign}{}", c_format_g(v, precision as usize));
    }
    let pow10 = 10f64.powi(precision as i32);
    let mut whole = v as u64;
    let tmp = (v - whole as f64) * pow10;
    let mut frac = tmp as u64;
    let diff = tmp - frac as f64;
    if diff > 0.5 || (diff == 0.5 && (frac == 0 || frac & 1 == 1)) {
        frac += 1;
    }
    if frac as f64 >= pow10 {
        frac = 0;
        whole += 1;
    }
    if precision == 0 {
        let diff = v - whole as f64;
        if diff > 0.5 || (diff == 0.5 && whole & 1 == 1) {
            whole += 1;
        }
        return format!("{sign}{whole}");
    }
    if frac == 0 {
        return format!("{sign}{whole}.0");
    }
    let mut count = precision as usize;
    while frac.is_multiple_of(10) {
        count -= 1;
        frac /= 10;
    }
    format!("{sign}{whole}.{frac:0count$}")
}

/// pandas' ujson string escaping: `ensure_ascii=True` with lower-case hex, and forward slashes
/// escaped (`a/b` → `a\/b`).
pub fn ujson_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '/' => out.push_str("\\/"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c if c.is_ascii() => out.push(c),
            c => {
                let mut buf = [0u16; 2];
                for unit in c.encode_utf16(&mut buf) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    out.push('"');
    out
}

fn ujson_json(j: &serde_json::Value, precision: u32) -> String {
    match j {
        serde_json::Value::Null => "null".into(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => i.to_string(),
            None => ujson_double(n.as_f64().unwrap_or(f64::NAN), precision),
        },
        serde_json::Value::String(s) => ujson_string(s),
        serde_json::Value::Array(a) => format!(
            "[{}]",
            a.iter()
                .map(|v| ujson_json(v, precision))
                .collect::<Vec<_>>()
                .join(",")
        ),
        serde_json::Value::Object(o) => format!(
            "{{{}}}",
            o.iter()
                .map(|(k, v)| format!("{}:{}", ujson_string(k), ujson_json(v, precision)))
                .collect::<Vec<_>>()
                .join(",")
        ),
    }
}

/// Options for [`to_json`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ToJsonOptions {
    /// `orient='records'` (`false`) or `orient='columns'` (`true`).
    pub columns_orient: bool,
    /// `double_precision` (15 in `to_json`).
    pub double_precision: u32,
    /// `geometry_format` (`geojson` by default, `wkb_hex` for syncs).
    pub geometry_format: GeometryFormat,
    /// `date_format='epoch'` (milliseconds) instead of ISO.
    pub epoch_dates: bool,
}

impl Default for ToJsonOptions {
    fn default() -> Self {
        ToJsonOptions {
            columns_orient: false,
            double_precision: 15,
            geometry_format: GeometryFormat::GeoJson,
            epoch_dates: false,
        }
    }
}

/// One cell as pandas' `to_json` writes it after `meerschaum.utils.dataframe.to_json`'s
/// conversions (UUID → `str`, bytes → base64, numeric → `serialize_decimal`, dates → ISO,
/// geometry → `geometry_format`).
pub fn to_json_value(v: &KValue, opts: &ToJsonOptions) -> String {
    match v {
        v if v.is_na() => "null".into(),
        KValue::Bool(b) => b.to_string(),
        KValue::Int(i) => i.to_string(),
        KValue::Float(f) => ujson_double(*f, opts.double_precision),
        KValue::Str(s) => ujson_string(s),
        KValue::Datetime(dt) => {
            // `date_unit='us'`: microsecond precision.
            let micros = dt.utc.and_utc().timestamp_micros();
            if opts.epoch_dates {
                return (micros.div_euclid(1000)).to_string();
            }
            let truncated = meerschaum_kernel::value::DateTimeValue {
                utc: chrono::DateTime::from_timestamp_micros(micros)
                    .map(|d| d.naive_utc())
                    .unwrap_or(dt.utc),
                tz: dt.tz.clone(),
            };
            ujson_string(&truncated.isoformat())
        }
        KValue::Date(d) => ujson_string(&meerschaum_kernel::serialize::serialize_date(d)),
        KValue::Uuid(u) => ujson_string(&u.hyphenated().to_string()),
        KValue::Decimal(d) => {
            match meerschaum_kernel::serialize::serialize_decimal(d, false, None, None) {
                Ok(Some(s)) => ujson_string(&s),
                _ => "null".into(),
            }
        }
        KValue::Bytes(_) => match meerschaum_kernel::serialize::serialize_bytes(v) {
            Ok(Some(s)) => ujson_string(&s),
            _ => "null".into(),
        },
        KValue::Geometry(_) => match serialize_geometry(v, opts.geometry_format, None) {
            Ok(Some(GeometryOutput::Text(s))) => ujson_string(&s),
            Ok(Some(GeometryOutput::GeoJson(j))) => ujson_json(&j, opts.double_precision),
            Ok(Some(GeometryOutput::Bytes(b))) => ujson_string(&String::from_utf8_lossy(&b)),
            _ => "null".into(),
        },
        KValue::Json(j) => ujson_json(j, opts.double_precision),
        other => ujson_string(&other.py_str()),
    }
}

/// `meerschaum.utils.dataframe.to_json(df, orient=..., double_precision=15, ...)`.
///
/// `index` holds the row labels for `orient='columns'` (`df.index` after the caller's slicing;
/// `None` means `0..n`).
pub fn to_json(df: &Frame, index: Option<&[usize]>, opts: &ToJsonOptions) -> String {
    let n = df.height();
    let default_index: Vec<usize> = (0..n).collect();
    let index = index.unwrap_or(&default_index);
    let columns: Vec<(String, Vec<KValue>)> = df
        .columns()
        .iter()
        .map(|c| (c.name.clone(), c.data.values()))
        .collect();
    if opts.columns_orient {
        let parts: Vec<String> = columns
            .iter()
            .map(|(name, values)| {
                let cells: Vec<String> = values
                    .iter()
                    .enumerate()
                    .map(|(i, v)| {
                        format!(
                            "\"{}\":{}",
                            index.get(i).copied().unwrap_or(i),
                            to_json_value(v, opts)
                        )
                    })
                    .collect();
                format!("{}:{{{}}}", ujson_string(name), cells.join(","))
            })
            .collect();
        return format!("{{{}}}", parts.join(","));
    }
    let rows: Vec<String> = (0..n)
        .map(|i| {
            let cells: Vec<String> = columns
                .iter()
                .map(|(name, values)| {
                    format!("{}:{}", ujson_string(name), to_json_value(&values[i], opts))
                })
                .collect();
            format!("{{{}}}", cells.join(","))
        })
        .collect();
    format!("[{}]", rows.join(","))
}

#[cfg(test)]
mod ujson_tests {
    use super::*;

    #[test]
    fn doubles() {
        assert_eq!(ujson_double(0.1, 15), "0.1");
        assert_eq!(ujson_double(1e20, 15), "1e+20");
        assert_eq!(ujson_double(1e-7, 10), "0.0000001");
        assert_eq!(ujson_double(3.0, 10), "3.0");
        assert_eq!(ujson_double(-4.0, 15), "-4.0");
        assert_eq!(ujson_double(1.123_456_789_012_345_7, 10), "1.123456789");
        assert_eq!(ujson_double(123456789.123, 10), "123456789.1229999959");
        assert_eq!(ujson_double(9007199254740992.0, 10), "9007199254740992.0");
        assert_eq!(ujson_double(f64::NAN, 15), "null");
        assert_eq!(ujson_string("a/b é\n"), "\"a\\/b \\u00e9\\n\"");
    }
}
