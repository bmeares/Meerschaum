//! Value conversions for the instance methods: datetime bounds between the connector types and
//! the query builders, cell encoding for inserts (`to_sql` / `psql_insert_copy`), and small
//! Python text helpers (`items_str`, `format_bytes`).

use chrono::{FixedOffset, NaiveDateTime, TimeZone};
use meerschaum_connector::DateBound;
use meerschaum_core::pyfmt::{JsonStyle, PyDateTime, float_repr, json_dumps};
use meerschaum_kernel::serialize::{serialize_datetime, serialize_decimal};
use meerschaum_kernel::value::{DateTimeValue, Value as KValue};

use crate::dateadd::DateArg;
use crate::driver::{SqlParam, SqlValue};
use crate::flavor::{DriverFamily, Flavor};
use crate::queries::partition::PartitionValue;

/// `meerschaum.utils.misc.items_str(items)` with the default arguments.
pub fn items_str(items: &[String]) -> String {
    let q = "'";
    match items.len() {
        0 => String::new(),
        1 => format!("{q}{}{q}", items[0]),
        2 => format!("{q}{}{q} and {q}{}{q}", items[0], items[1]),
        n => {
            let sep = format!("{q}, {q}");
            format!(
                "{q}{}{q}, and {q}{}{q}",
                items[..n - 1].join(&sep),
                items[n - 1]
            )
        }
    }
}

/// `meerschaum.utils.formatting.format_bytes(num_bytes)`.
pub fn format_bytes(num_bytes: Option<i64>) -> String {
    let Some(n) = num_bytes else {
        return "?".into();
    };
    let sign = if n < 0 { "-" } else { "" };
    let mut value = (n as f64).abs();
    let units = ["B", "kB", "MB", "GB", "TB", "PB", "EB"];
    let mut ix = 0;
    while value >= 1000.0 && ix < units.len() - 1 {
        value /= 1000.0;
        ix += 1;
    }
    if ix == 0 {
        return format!("{sign}{} {}", value as i64, units[ix]);
    }
    format!("{sign}{value:.1} {}", units[ix])
}

/// A kernel datetime as a Python `datetime` (aware with its UTC offset, or naive).
pub fn py_datetime(dt: &DateTimeValue) -> PyDateTime {
    match &dt.tz {
        None => PyDateTime::Naive(dt.utc),
        Some(tz) => {
            let offset = tz.offset_at(&dt.utc);
            let fixed = FixedOffset::east_opt(offset.local_minus_utc())
                .unwrap_or(FixedOffset::east_opt(0).expect("utc"));
            PyDateTime::Aware(fixed.from_utc_datetime(&dt.utc))
        }
    }
}

/// A Python `datetime` as a kernel datetime.
pub fn kernel_datetime(dt: &PyDateTime) -> DateTimeValue {
    match dt {
        PyDateTime::Naive(n) => DateTimeValue::naive(*n),
        PyDateTime::Aware(a) => DateTimeValue::utc(a.naive_utc()),
    }
}

/// A bound as `dateadd_str`'s `begin`.
pub fn date_arg(bound: Option<&DateBound>) -> DateArg {
    match bound {
        None => DateArg::None,
        Some(DateBound::Int(i)) => DateArg::Int(*i),
        Some(DateBound::Datetime(dt)) => DateArg::DateTime(py_datetime(dt)),
    }
}

/// A bound as a partition value.
pub fn partition_value(bound: &DateBound) -> PartitionValue {
    match bound {
        DateBound::Int(i) => PartitionValue::Int(*i),
        DateBound::Datetime(dt) => PartitionValue::DateTime(py_datetime(dt)),
    }
}

/// A partition value as a bound.
pub fn bound_from_partition(value: &PartitionValue) -> DateBound {
    match value {
        PartitionValue::Int(i) => DateBound::Int(*i),
        PartitionValue::DateTime(dt) => DateBound::Datetime(kernel_datetime(dt)),
    }
}

/// `coerce_timezone(dt, strip_utc)`: convert aware datetimes to UTC, optionally dropping the
/// timezone.
pub fn coerce_bound(bound: DateBound, strip_utc: bool) -> DateBound {
    match bound {
        DateBound::Datetime(dt) => {
            let out = match dt.tz {
                None if strip_utc => dt,
                // `dt.replace(tzinfo=timezone.utc)`.
                None => DateTimeValue::utc(dt.utc),
                Some(_) if strip_utc => DateTimeValue::naive(dt.utc),
                Some(_) => DateTimeValue::utc(dt.utc),
            };
            DateBound::Datetime(out)
        }
        other => other,
    }
}

/// A database scalar as a sync time (`get_sync_time`'s conversions).
pub fn sync_time_from_sql(value: &SqlValue) -> Option<DateBound> {
    match value {
        SqlValue::Null => None,
        SqlValue::Int(i) => Some(DateBound::Int(*i)),
        SqlValue::UInt(u) => i64::try_from(*u).ok().map(DateBound::Int),
        SqlValue::Decimal(d) if d.fract().is_zero() => {
            d.to_string().parse::<i64>().ok().map(DateBound::Int)
        }
        SqlValue::Float(f) if f.fract() == 0.0 => Some(DateBound::Int(*f as i64)),
        SqlValue::Timestamp(t) => Some(DateBound::Datetime(DateTimeValue::naive(*t))),
        SqlValue::TimestampTz(t) => Some(DateBound::Datetime(DateTimeValue::utc(t.naive_utc()))),
        // Sometimes the datetime is actually a date.
        SqlValue::Date(d) => Some(DateBound::Datetime(DateTimeValue::naive(
            d.and_hms_opt(0, 0, 0).expect("midnight"),
        ))),
        // SQLite returns `str` (`dateutil.parser.parse`).
        SqlValue::Text(s) => {
            let dt = crate::dateadd::parse_datetime_str(s)?;
            Some(DateBound::Datetime(kernel_datetime(&dt)))
        }
        _ => None,
    }
}

/// `json.dumps(x, default=json_serialize_value, sort_keys=sort_keys)` with the default
/// separators (`', '`, `': '`).
pub fn json_dumps_default(j: &serde_json::Value, sort_keys: bool) -> String {
    fn sort(j: &serde_json::Value) -> serde_json::Value {
        match j {
            serde_json::Value::Object(o) => {
                let mut keys: Vec<&String> = o.keys().collect();
                keys.sort();
                let mut m = serde_json::Map::new();
                for k in keys {
                    m.insert(k.clone(), sort(&o[k]));
                }
                serde_json::Value::Object(m)
            }
            serde_json::Value::Array(a) => serde_json::Value::Array(a.iter().map(sort).collect()),
            other => other.clone(),
        }
    }
    let j = if sort_keys { sort(j) } else { j.clone() };
    let core = meerschaum_connector::pipe_ops::json_to_core(&j);
    json_dumps(&core, JsonStyle::Default, true)
}

/// The decimal `(precision, scale)` of a database numeric type (`NUMERIC(10,2)`, `DECIMAL(38, 20)`).
pub fn numeric_precision_scale(db_type: &str) -> Option<(i64, i64)> {
    let upper = db_type.to_uppercase();
    if !(upper.starts_with("NUMERIC") || upper.starts_with("DECIMAL")) {
        return None;
    }
    let inner = upper.split_once('(')?.1.split_once(')')?.0;
    let (p, s) = inner.split_once(',')?;
    Some((p.trim().parse().ok()?, s.trim().parse().ok()?))
}

/// The per-column encoding plan for inserts.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnEncoding {
    /// The column name.
    pub name: String,
    /// `(precision, scale)` to quantize decimals to (numeric columns only).
    pub numeric: Option<(Option<i64>, Option<i64>)>,
}

/// A datetime's wall-clock text in SQLAlchemy's storage format (`%Y-%m-%d %H:%M:%S.%f`).
fn sa_datetime_text(dt: &NaiveDateTime) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

fn decimal_text(d: &meerschaum_kernel::Decimal, enc: &ColumnEncoding) -> Option<String> {
    let (p, s) = enc.numeric.unwrap_or((None, None));
    serialize_decimal(d, true, p, s).ok().flatten()
}

/// Encode one cell for a bound `INSERT` (the `method='multi'` path of `to_sql`).
pub fn encode_param(v: &KValue, flavor: Flavor, enc: &ColumnEncoding) -> SqlParam {
    if v.is_na() {
        return SqlParam::Null;
    }
    let family = flavor.driver_family();
    match v {
        KValue::Bool(b) => match family {
            DriverFamily::Sqlite => SqlParam::Int(*b as i64),
            _ => SqlParam::Bool(*b),
        },
        KValue::Int(i) => {
            if enc.numeric.is_some() {
                SqlParam::Text(i.to_string())
            } else {
                SqlParam::Int(*i)
            }
        }
        KValue::Float(f) => SqlParam::Float(*f),
        KValue::Str(s) => SqlParam::Text(s.clone()),
        KValue::Bytes(b) => SqlParam::Bytes(b.clone()),
        KValue::Decimal(d) => match decimal_text(d, enc) {
            Some(s) => SqlParam::Text(s),
            None => SqlParam::Null,
        },
        KValue::Uuid(u) => SqlParam::Text(u.hyphenated().to_string()),
        KValue::Datetime(dt) => {
            // TIMEZONE_NAIVE flavors strip to UTC; SQLite stores the wall-clock components.
            let wall = if flavor.timezone_naive() {
                dt.utc
            } else {
                dt.local()
            };
            SqlParam::Text(sa_datetime_text(&wall))
        }
        KValue::Date(d) => SqlParam::Text(d.format("%Y-%m-%d").to_string()),
        KValue::Json(j) => match j {
            serde_json::Value::Object(_) | serde_json::Value::Array(_) => {
                if flavor.is_json_flavor() {
                    SqlParam::Json(j.clone())
                } else {
                    SqlParam::Text(json_dumps_default(j, true))
                }
            }
            other => SqlParam::Text(other.to_string()),
        },
        KValue::Geometry(g) => SqlParam::Text(g.to_wkb_hex(true)),
        KValue::Null | KValue::NA | KValue::NaT => SqlParam::Null,
    }
}

/// Encode one cell for PostgreSQL `COPY ... CSV NULL '\N'` (`psql_insert_copy`): `None` is
/// `\N`.
pub fn encode_copy_field(v: &KValue, enc: &ColumnEncoding) -> Option<String> {
    if v.is_na() {
        return None;
    }
    // NOTE: PostgreSQL doesn't support NUL chars in text, so they're removed from strings.
    let strip_nul = |s: String| s.replace('\0', "").replace("\\u0000", "");
    Some(match v {
        KValue::Str(s) => strip_nul(s.clone()),
        KValue::Json(j) => strip_nul(json_dumps_default(j, false)),
        KValue::Bool(b) => if *b { "True" } else { "False" }.to_string(),
        KValue::Int(i) => i.to_string(),
        KValue::Float(f) => float_repr(*f),
        KValue::Decimal(d) => decimal_text(d, enc)?,
        // `encode_bytes_for_bytea(with_prefix=True)`.
        KValue::Bytes(b) => {
            let mut s = String::from("\\x");
            for byte in b {
                s.push_str(&format!("{byte:02x}"));
            }
            s
        }
        KValue::Uuid(u) => u.hyphenated().to_string(),
        KValue::Datetime(dt) => serialize_datetime(dt),
        KValue::Date(d) => d.format("%Y-%m-%d").to_string(),
        KValue::Geometry(g) => g.to_wkb_hex(true),
        KValue::Null | KValue::NA | KValue::NaT => return None,
    })
}

/// Python's `csv.writer` (`QUOTE_MINIMAL`, `\r\n`) for one row; `None` fields are written as
/// the bare `\N` marker.
pub fn csv_row(fields: &[Option<String>], out: &mut String) {
    for (i, field) in fields.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match field {
            None => out.push_str("\\N"),
            Some(f) => {
                let needs_quotes = f.contains(',')
                    || f.contains('"')
                    || f.contains('\r')
                    || f.contains('\n')
                    || (fields.len() == 1 && f.is_empty());
                if needs_quotes {
                    out.push('"');
                    out.push_str(&f.replace('"', "\"\""));
                    out.push('"');
                } else {
                    out.push_str(f);
                }
            }
        }
    }
    out.push_str("\r\n");
}

/// A database scalar as a kernel cell.
pub fn kernel_from_sql(v: &SqlValue) -> KValue {
    match v {
        SqlValue::Null => KValue::Null,
        SqlValue::Bool(b) => KValue::Bool(*b),
        SqlValue::Int(i) => KValue::Int(*i),
        SqlValue::UInt(u) => i64::try_from(*u)
            .map(KValue::Int)
            .unwrap_or(KValue::Float(*u as f64)),
        SqlValue::Float(f) => KValue::Float(*f),
        SqlValue::Decimal(d) => meerschaum_kernel::Decimal::parse(&d.to_string())
            .map(KValue::Decimal)
            .unwrap_or(KValue::Str(d.to_string())),
        SqlValue::Text(s) => KValue::Str(s.clone()),
        SqlValue::Json(s) => serde_json::from_str(s)
            .map(meerschaum_kernel::frame::json_to_value)
            .unwrap_or(KValue::Str(s.clone())),
        SqlValue::Bytes(b) => KValue::Bytes(b.clone()),
        SqlValue::Uuid(u) => KValue::Uuid(*u),
        SqlValue::Date(d) => KValue::Date(*d),
        SqlValue::Time(t) => KValue::Str(t.to_string()),
        SqlValue::Timestamp(t) => KValue::Datetime(DateTimeValue::naive(*t)),
        SqlValue::TimestampTz(t) => KValue::Datetime(DateTimeValue::utc(t.naive_utc())),
    }
}

/// A core (parameters) value as a kernel `params` cell for `build_where`.
pub fn core_params(
    params: &meerschaum_core::Map,
) -> indexmap::IndexMap<String, meerschaum_core::Value> {
    params.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytes_format() {
        assert_eq!(format_bytes(Some(0)), "0 B");
        assert_eq!(format_bytes(Some(1536)), "1.5 kB");
        assert_eq!(format_bytes(None), "?");
        assert_eq!(format_bytes(Some(-2_500_000)), "-2.5 MB");
    }

    #[test]
    fn csv() {
        let mut s = String::new();
        csv_row(
            &[
                Some("a,b".into()),
                None,
                Some("x\"y".into()),
                Some(String::new()),
            ],
            &mut s,
        );
        assert_eq!(s, "\"a,b\",\\N,\"x\"\"y\",\r\n");
    }

    #[test]
    fn items() {
        assert_eq!(
            items_str(&["a".into(), "b".into(), "c".into()]),
            "'a', 'b', and 'c'"
        );
    }

    #[test]
    fn numeric_types() {
        assert_eq!(numeric_precision_scale("NUMERIC(10,2)"), Some((10, 2)));
        assert_eq!(numeric_precision_scale("DECIMAL(38, 20)"), Some((38, 20)));
        assert_eq!(numeric_precision_scale("NUMERIC"), None);
    }
}
