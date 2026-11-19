//! Named `:param` SQL parameter rewriting and JSON-to-[`SqlParam`] conversion.
//!
//! Native plugins (and other callers with JSON parameter maps) pass queries with SQLAlchemy-style
//! `:name` placeholders and a JSON object of parameter values. This module scans the SQL text
//! safely (skipping quoted strings, quoted identifiers, dollar-quoted strings, comments, and
//! PostgreSQL `::type` casts), rewrites each `:name` to the target flavor's positional placeholder
//! (`$1`, `?`, `@P1`, `:1`), and converts each JSON value into a typed [`SqlParam`].
//!
//! Typed values that JSON cannot distinguish from plain strings (datetimes, dates, bytes,
//! explicitly typed empty arrays, or raw JSON documents) travel as tagged two-key objects:
//! `{"$mrsm_sql": "<type>", "value": <v>}`.

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde_json::Value;

use crate::{
    Result, SqlError,
    driver::params::{SqlParam, placeholder},
    flavor::{DriverFamily, Flavor},
};

/// Convert a JSON value from a parameter map into a typed [`SqlParam`].
pub fn json_to_sql_param(value: &Value) -> Result<SqlParam> {
    match value {
        Value::Null => Ok(SqlParam::Null),
        Value::Bool(b) => Ok(SqlParam::Bool(*b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(SqlParam::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(SqlParam::Float(f))
            } else {
                Err(SqlError::InvalidArgument(format!(
                    "SQL parameter number '{n}' is out of range."
                )))
            }
        }
        Value::String(s) => Ok(SqlParam::Text(s.clone())),
        Value::Object(map) => {
            if let Some(tag) = map.get("$mrsm_sql").and_then(Value::as_str)
                && map.contains_key("value")
                && map.len() == 2
            {
                return decode_tagged(tag, &map["value"]);
            }
            Ok(SqlParam::Json(value.clone()))
        }
        Value::Array(items) => decode_array(items),
    }
}

fn parse_timestamptz(value: &Value) -> Result<DateTime<Utc>> {
    let text = value.as_str().ok_or_else(|| {
        SqlError::InvalidArgument(
            "SQL `timestamptz` parameter value must be an ISO-8601 string.".into(),
        )
    })?;
    let parsed = meerschaum_kernel::value::parse_datetime_value(text).ok_or_else(|| {
        SqlError::InvalidArgument(format!("'{text}' is not a valid ISO-8601 timestamp."))
    })?;
    Ok(DateTime::from_naive_utc_and_offset(parsed.utc, Utc))
}

fn parse_timestamp(value: &Value) -> Result<NaiveDateTime> {
    let text = value.as_str().ok_or_else(|| {
        SqlError::InvalidArgument(
            "SQL `timestamp` parameter value must be an ISO-8601 string.".into(),
        )
    })?;
    let parsed = meerschaum_kernel::value::parse_datetime_value(text).ok_or_else(|| {
        SqlError::InvalidArgument(format!("'{text}' is not a valid ISO-8601 timestamp."))
    })?;
    Ok(parsed.utc)
}

fn parse_date(value: &Value) -> Result<NaiveDate> {
    let text = value.as_str().ok_or_else(|| {
        SqlError::InvalidArgument("SQL `date` parameter value must be a YYYY-MM-DD string.".into())
    })?;
    NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d").map_err(|e| {
        SqlError::InvalidArgument(format!("'{text}' is not a valid YYYY-MM-DD date: {e}"))
    })
}

fn decode_tagged(tag: &str, value: &Value) -> Result<SqlParam> {
    if value.is_null() {
        return Ok(SqlParam::Null);
    }
    match tag {
        "timestamptz" => Ok(SqlParam::TimestampTz(parse_timestamptz(value)?)),
        "timestamp" => Ok(SqlParam::Timestamp(parse_timestamp(value)?)),
        "date" => Ok(SqlParam::Date(parse_date(value)?)),
        "bytes" => {
            let text = value.as_str().ok_or_else(|| {
                SqlError::InvalidArgument(
                    "SQL `bytes` parameter value must be a base64 string.".into(),
                )
            })?;
            Ok(SqlParam::Bytes(decode_base64(text)?))
        }
        "json" => Ok(SqlParam::Json(value.clone())),
        "text[]" => {
            let items = expect_array(tag, value)?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Null => out.push(None),
                    Value::String(s) => out.push(Some(s.clone())),
                    _ => {
                        return Err(SqlError::InvalidArgument(
                            "SQL `text[]` elements must be strings or null.".into(),
                        ));
                    }
                }
            }
            Ok(SqlParam::TextArray(out))
        }
        "int[]" => {
            let items = expect_array(tag, value)?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Null => out.push(None),
                    Value::Number(n) if n.as_i64().is_some() => out.push(n.as_i64()),
                    _ => {
                        return Err(SqlError::InvalidArgument(
                            "SQL `int[]` elements must be 64-bit integers or null.".into(),
                        ));
                    }
                }
            }
            Ok(SqlParam::IntArray(out))
        }
        "float[]" => {
            let items = expect_array(tag, value)?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Null => out.push(None),
                    Value::Number(n) if n.as_f64().is_some() => out.push(n.as_f64()),
                    _ => {
                        return Err(SqlError::InvalidArgument(
                            "SQL `float[]` elements must be numbers or null.".into(),
                        ));
                    }
                }
            }
            Ok(SqlParam::FloatArray(out))
        }
        "bool[]" => {
            let items = expect_array(tag, value)?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                match item {
                    Value::Null => out.push(None),
                    Value::Bool(b) => out.push(Some(*b)),
                    _ => {
                        return Err(SqlError::InvalidArgument(
                            "SQL `bool[]` elements must be booleans or null.".into(),
                        ));
                    }
                }
            }
            Ok(SqlParam::BoolArray(out))
        }
        "timestamptz[]" => {
            let items = expect_array(tag, value)?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                if item.is_null() {
                    out.push(None);
                } else {
                    out.push(Some(parse_timestamptz(unwrap_tagged_or_raw(
                        item,
                        "timestamptz",
                    ))?));
                }
            }
            Ok(SqlParam::TimestampTzArray(out))
        }
        "timestamp[]" => {
            let items = expect_array(tag, value)?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                if item.is_null() {
                    out.push(None);
                } else {
                    out.push(Some(parse_timestamp(unwrap_tagged_or_raw(
                        item,
                        "timestamp",
                    ))?));
                }
            }
            Ok(SqlParam::TimestampArray(out))
        }
        "date[]" => {
            let items = expect_array(tag, value)?;
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                if item.is_null() {
                    out.push(None);
                } else {
                    out.push(Some(parse_date(unwrap_tagged_or_raw(item, "date"))?));
                }
            }
            Ok(SqlParam::DateArray(out))
        }
        other => Err(SqlError::InvalidArgument(format!(
            "Unknown SQL parameter type tag '{other}'."
        ))),
    }
}

fn unwrap_tagged_or_raw<'a>(value: &'a Value, expected_tag: &str) -> &'a Value {
    if let Value::Object(map) = value
        && map.len() == 2
        && map.get("$mrsm_sql").and_then(Value::as_str) == Some(expected_tag)
        && let Some(inner) = map.get("value")
    {
        return inner;
    }
    value
}

fn expect_array<'a>(tag: &str, value: &'a Value) -> Result<&'a [Value]> {
    value.as_array().map(Vec::as_slice).ok_or_else(|| {
        SqlError::InvalidArgument(format!("SQL `{tag}` parameter value must be a JSON array."))
    })
}

fn tagged_kind(value: &Value) -> Option<(&str, &Value)> {
    let map = value.as_object()?;
    if map.len() != 2 {
        return None;
    }
    let tag = map.get("$mrsm_sql")?.as_str()?;
    let inner = map.get("value")?;
    Some((tag, inner))
}

fn decode_array(items: &[Value]) -> Result<SqlParam> {
    if items.is_empty() {
        return Ok(SqlParam::EmptyArray);
    }
    let non_null: Vec<&Value> = items.iter().filter(|v| !v.is_null()).collect();
    if non_null.is_empty() {
        return Ok(SqlParam::TextArray(vec![None; items.len()]));
    }
    if non_null.iter().all(|v| v.is_string()) {
        return Ok(SqlParam::TextArray(
            items
                .iter()
                .map(|v| v.as_str().map(str::to_owned))
                .collect(),
        ));
    }
    if non_null.iter().all(|v| v.as_i64().is_some()) {
        return Ok(SqlParam::IntArray(
            items.iter().map(|v| v.as_i64()).collect(),
        ));
    }
    if non_null.iter().all(|v| v.as_f64().is_some()) {
        return Ok(SqlParam::FloatArray(
            items.iter().map(|v| v.as_f64()).collect(),
        ));
    }
    if non_null.iter().all(|v| v.is_boolean()) {
        return Ok(SqlParam::BoolArray(
            items.iter().map(|v| v.as_bool()).collect(),
        ));
    }
    if non_null
        .iter()
        .all(|v| matches!(tagged_kind(v), Some(("timestamptz", _))))
    {
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            if item.is_null() {
                out.push(None);
            } else {
                let (_, inner) = tagged_kind(item).expect("checked");
                out.push(Some(parse_timestamptz(inner)?));
            }
        }
        return Ok(SqlParam::TimestampTzArray(out));
    }
    if non_null
        .iter()
        .all(|v| matches!(tagged_kind(v), Some(("timestamp", _))))
    {
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            if item.is_null() {
                out.push(None);
            } else {
                let (_, inner) = tagged_kind(item).expect("checked");
                out.push(Some(parse_timestamp(inner)?));
            }
        }
        return Ok(SqlParam::TimestampArray(out));
    }
    if non_null
        .iter()
        .all(|v| matches!(tagged_kind(v), Some(("date", _))))
    {
        let mut out = Vec::with_capacity(items.len());
        for item in items {
            if item.is_null() {
                out.push(None);
            } else {
                let (_, inner) = tagged_kind(item).expect("checked");
                out.push(Some(parse_date(inner)?));
            }
        }
        return Ok(SqlParam::DateArray(out));
    }
    Ok(SqlParam::Json(Value::Array(items.to_vec())))
}

/// Rewrite `:name` bind parameters in `sql` to `flavor`'s positional placeholders and resolve
/// their values from `params` (a JSON object, or `null` for an empty map).
///
/// On PostgreSQL, an untyped empty JSON array (`[]`) rewrites its placeholder to the untyped
/// empty array literal `'{}'`, so `WHERE id = ANY(:ids)` works for any column element type
/// (`text`, `bigint`, `integer`, `uuid`, `timestamptz`, ...).
pub fn bind_named_sql(
    flavor: Flavor,
    sql: &str,
    params: &Value,
) -> Result<(String, Vec<SqlParam>)> {
    let map = match params {
        Value::Null => None,
        Value::Object(map) => Some(map),
        _ => {
            return Err(SqlError::InvalidArgument(
                "SQL parameters must be a JSON object.".into(),
            ));
        }
    };

    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut bound: Vec<SqlParam> = Vec::new();
    let mut i = 0;

    while i < len {
        let b = bytes[i];

        // Single-quoted string literal (handle '' and E'...' backslash escapes).
        if b == b'\'' {
            let escaped_str = i > 0
                && (bytes[i - 1] == b'E' || bytes[i - 1] == b'e')
                && (i == 1 || !is_ident_char(bytes[i - 2]));
            let start = i;
            i += 1;
            while i < len {
                if escaped_str && bytes[i] == b'\\' && i + 1 < len {
                    i += 2;
                    continue;
                }
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < len && bytes[i] == b'\'' {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // Double-quoted identifier ("..." with "" escape).
        if b == b'"' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'"' {
                    i += 1;
                    if i < len && bytes[i] == b'"' {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // Backtick-quoted identifier (`...` with `` escape).
        if b == b'`' {
            let start = i;
            i += 1;
            while i < len {
                if bytes[i] == b'`' {
                    i += 1;
                    if i < len && bytes[i] == b'`' {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // Line comment (-- ...\n).
        if b == b'-' && i + 1 < len && bytes[i + 1] == b'-' {
            let start = i;
            i += 2;
            while i < len && bytes[i] != b'\n' {
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // Block comment (/* ... */, supporting PostgreSQL nesting).
        if b == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
            let start = i;
            i += 2;
            let mut depth = 1usize;
            while i < len && depth > 0 {
                if bytes[i] == b'/' && i + 1 < len && bytes[i + 1] == b'*' {
                    depth += 1;
                    i += 2;
                } else if bytes[i] == b'*' && i + 1 < len && bytes[i + 1] == b'/' {
                    depth -= 1;
                    i += 2;
                } else {
                    i += 1;
                }
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // PostgreSQL dollar-quoted string ($$...$$ or $tag$...$tag$).
        if b == b'$'
            && let Some(tag_end) = match_dollar_tag(bytes, i)
        {
            let tag = &sql[i..tag_end];
            let start = i;
            i = tag_end;
            if let Some(rel) = sql[i..].find(tag) {
                i += rel + tag.len();
            } else {
                i = len;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        // Escaped colon (\:) -> literal ':'.
        if b == b'\\' && i + 1 < len && bytes[i + 1] == b':' {
            out.push(':');
            i += 2;
            continue;
        }

        // Named bind parameter :name (not ::cast, not preceded by identifier char or ':').
        if b == b':' {
            let prev_blocks = i > 0 && (is_ident_char(bytes[i - 1]) || bytes[i - 1] == b':');
            let next_is_colon = i + 1 < len && bytes[i + 1] == b':';
            if next_is_colon {
                // PostgreSQL type cast `::` — emit both colons verbatim.
                out.push_str("::");
                i += 2;
                continue;
            }
            if !prev_blocks && i + 1 < len && is_ident_start(bytes[i + 1]) {
                let name_start = i + 1;
                let mut name_end = name_start + 1;
                while name_end < len && is_ident_char(bytes[name_end]) {
                    name_end += 1;
                }
                // Single trailing ':' (e.g. `:foo:`) is not a bind parameter, but PostgreSQL
                // type casts (`:val::numeric`) ARE bind parameters followed by `::`.
                if name_end < len
                    && bytes[name_end] == b':'
                    && bytes.get(name_end + 1) != Some(&b':')
                {
                    out.push_str(&sql[i..name_end]);
                    i = name_end;
                    continue;
                }
                let name = &sql[name_start..name_end];
                let raw = map.and_then(|m| m.get(name)).ok_or_else(|| {
                    SqlError::InvalidArgument(format!("Missing SQL bind parameter ':{name}'."))
                })?;
                let param = json_to_sql_param(raw)?;
                if matches!(param, SqlParam::EmptyArray)
                    && flavor.driver_family() == DriverFamily::Postgres
                {
                    out.push_str("'{}'");
                } else {
                    bound.push(param);
                    out.push_str(&placeholder(flavor, bound.len()));
                }
                i = name_end;
                continue;
            }
        }

        // Advance one UTF-8 char.
        let ch = sql[i..].chars().next().expect("valid utf8");
        out.push(ch);
        i += ch.len_utf8();
    }

    Ok((out, bound))
}

fn is_ident_start(b: u8) -> bool {
    b.is_ascii_alphabetic() || b == b'_'
}

fn is_ident_char(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

/// Match a PostgreSQL dollar-quote opening tag (`$$` or `$ident$`) starting at `pos`.
/// Returns the byte index immediately after the closing `$`.
fn match_dollar_tag(bytes: &[u8], pos: usize) -> Option<usize> {
    if bytes.get(pos) != Some(&b'$') {
        return None;
    }
    let next = *bytes.get(pos + 1)?;
    if next == b'$' {
        return Some(pos + 2);
    }
    if !is_ident_start(next) {
        return None;
    }
    let mut j = pos + 2;
    while j < bytes.len() && is_ident_char(bytes[j]) {
        j += 1;
    }
    if bytes.get(j) == Some(&b'$') {
        Some(j + 1)
    } else {
        None
    }
}

fn decode_base64(input: &str) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(input.len() * 3 / 4);
    let mut buf: u32 = 0;
    let mut bits: u8 = 0;
    for b in input.bytes() {
        if b.is_ascii_whitespace() || b == b'=' {
            continue;
        }
        let val = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' | b'-' => 62,
            b'/' | b'_' => 63,
            _ => {
                return Err(SqlError::InvalidArgument(
                    "Invalid base64 character in SQL `bytes` parameter.".into(),
                ));
            }
        };
        buf = (buf << 6) | u32::from(val);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rewrites_named_params_and_preserves_pg_casts_quotes_and_comments() {
        let sql = "\
            SELECT id, ':not_a_param', \"col:also_not\", $$:dollar_not$$, $tag$:tagged$tag$ \
            -- :comment_not\n\
            /* /* nested :block_not */ */\n\
            FROM readings \
            WHERE time >= :begin AND id = ANY(:ids) AND val = :val::numeric AND label = 'a\\:b' AND tag = :val";
        let params = json!({
            "begin": {"$mrsm_sql": "timestamptz", "value": "2026-09-25T17:00:00+00:00"},
            "ids": ["c1", "c2"],
            "val": 42
        });
        let (rewritten, bound) = bind_named_sql(Flavor::Postgresql, sql, &params).unwrap();
        assert!(rewritten.contains("':not_a_param'"));
        assert!(rewritten.contains("\"col:also_not\""));
        assert!(rewritten.contains("$$:dollar_not$$"));
        assert!(rewritten.contains("$tag$:tagged$tag$"));
        assert!(rewritten.contains("-- :comment_not"));
        assert!(rewritten.contains("/* /* nested :block_not */ */"));
        assert!(rewritten.contains("WHERE time >= $1 AND id = ANY($2) AND val = $3::numeric"));
        assert!(rewritten.ends_with("AND tag = $4"));
        assert_eq!(bound.len(), 4);
        assert!(matches!(bound[0], SqlParam::TimestampTz(_)));
        assert_eq!(
            bound[1],
            SqlParam::TextArray(vec![Some("c1".into()), Some("c2".into())])
        );
        assert_eq!(bound[2], SqlParam::Int(42));
        assert_eq!(bound[3], SqlParam::Int(42));
    }

    #[test]
    fn empty_array_becomes_untyped_empty_array_literal_on_postgres() {
        let (rewritten, bound) = bind_named_sql(
            Flavor::Timescaledb,
            "SELECT * FROM heartbeats WHERE id = ANY(:ids) AND age > :min_age",
            &json!({"ids": [], "min_age": 5}),
        )
        .unwrap();
        assert_eq!(
            rewritten,
            "SELECT * FROM heartbeats WHERE id = ANY('{}') AND age > $1"
        );
        assert_eq!(bound, vec![SqlParam::Int(5)]);
    }

    #[test]
    fn sqlite_uses_question_marks_and_rejects_missing_params() {
        let (rewritten, bound) = bind_named_sql(
            Flavor::Sqlite,
            "SELECT * FROM t WHERE a = :a AND b = :b",
            &json!({"a": "hello", "b": null}),
        )
        .unwrap();
        assert_eq!(rewritten, "SELECT * FROM t WHERE a = ? AND b = ?");
        assert_eq!(bound, vec![SqlParam::Text("hello".into()), SqlParam::Null]);

        let err = bind_named_sql(Flavor::Sqlite, "SELECT :missing", &json!({})).unwrap_err();
        assert!(err.to_string().contains(":missing"));
    }

    #[test]
    fn tagged_bytes_date_and_int_arrays_decode_cleanly() {
        assert_eq!(
            json_to_sql_param(&json!({"$mrsm_sql": "bytes", "value": "aGVsbG8="})).unwrap(),
            SqlParam::Bytes(b"hello".to_vec())
        );
        assert_eq!(
            json_to_sql_param(&json!({"$mrsm_sql": "date", "value": "2026-09-25"})).unwrap(),
            SqlParam::Date(NaiveDate::from_ymd_opt(2026, 9, 25).unwrap())
        );
        assert_eq!(
            json_to_sql_param(&json!([1, 2, null, 4])).unwrap(),
            SqlParam::IntArray(vec![Some(1), Some(2), None, Some(4)])
        );
    }
}
