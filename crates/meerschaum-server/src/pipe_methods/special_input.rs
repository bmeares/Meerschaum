//! Typed JSON transport hints affect input decoding, never accepted pipe parameters.
use std::collections::BTreeSet;

use meerschaum_core::dtypes::{MrsmDtype, parse_numeric_precision_scale, resolve_alias};
use meerschaum_core::{Map, Value};
use meerschaum_kernel::enforce::{PipeDtypesContext, PipeInput};
use meerschaum_kernel::value::TzInfo;

use crate::error::ApiError;

const MAX_BYTES: usize = 64 * 1024;
const MAX_COLUMNS: usize = 1024;

// Older SDK clients use str(dict) for keyword query parameters. Permit only one flat
// mapping of quoted strings before using the native constant parser. This lexical bound
// excludes nesting, unary recursion and names/calls; it never evaluates Python.
fn flat_string_mapping(encoded: &str) -> bool {
    let mut quote = None;
    let mut escaped = false;
    let mut opened = false;
    let mut closed = false;
    for character in encoded.chars() {
        if let Some(delimiter) = quote {
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == delimiter {
                quote = None;
            }
            continue;
        }
        if character.is_whitespace() {
            continue;
        }
        if closed {
            return false;
        }
        match character {
            '{' if !opened => opened = true,
            '}' if opened => closed = true,
            '\'' | '"' if opened => quote = Some(character),
            ':' | ',' if opened => {}
            _ => return false,
        }
    }
    opened && closed && quote.is_none()
}

fn known(dtype: &str) -> bool {
    if dtype.len() > 128 || dtype.chars().any(char::is_control) {
        return false;
    }
    let dtype = resolve_alias(dtype);
    if matches!(
        dtype,
        "json" | "bytes" | "uuid" | "bool" | "date" | "datetime" | "numeric"
    ) {
        return true;
    }
    if dtype.starts_with("numeric[") && dtype.ends_with(']') {
        if !dtype[8..dtype.len() - 1]
            .chars()
            .all(|c| c.is_ascii_digit() || c == ',' || c == ' ')
        {
            return false;
        }
        return parse_numeric_precision_scale(dtype).is_some_and(|(precision, scale)| {
            (1..=1000).contains(&precision) && (0..=precision).contains(&scale)
        });
    }
    if matches!(MrsmDtype::parse(dtype), Ok(MrsmDtype::Spatial(_))) {
        return true;
    }
    if !dtype.starts_with("datetime64[") || !dtype.ends_with(']') {
        return false;
    }
    let parts: Vec<_> = dtype[11..dtype.len() - 1]
        .split(',')
        .map(str::trim)
        .collect();
    (1..=2).contains(&parts.len())
        && matches!(parts[0], "ns" | "us" | "ms" | "s")
        && parts.get(1).is_none_or(|tz| TzInfo::parse(tz).is_some())
}

pub(super) fn parse(encoded: Option<&str>) -> Result<Map, ApiError> {
    let Some(encoded) = encoded else {
        return Ok(Map::new());
    };
    if encoded.len() > MAX_BYTES {
        return Err(ApiError::new(
            413,
            "Special input dtype hints exceed their byte bound.",
        ));
    }
    let value = Value::from_json_str(encoded)
        .or_else(|_| {
            if flat_string_mapping(encoded) {
                meerschaum_core::pyfmt::literal_eval(encoded)
            } else {
                Err("Invalid flat string mapping".into())
            }
        })
        .map_err(|_| ApiError::new(400, "Invalid special input dtype hints."))?;
    let hints = value
        .as_map()
        .ok_or_else(|| ApiError::new(400, "Special input dtype hints must be a mapping."))?;
    if hints.len() > MAX_COLUMNS
        || hints.iter().any(|(column, dtype)| {
            column.is_empty()
                || column.len() > 1024
                || column.chars().any(char::is_control)
                || !dtype.as_str().is_some_and(known)
        })
    {
        return Err(ApiError::new(
            400,
            "Special input dtype hints contain an unsupported column or type.",
        ));
    }
    Ok(hints.clone())
}

/// A copy of the original dtype context is used only to decode this payload. The caller
/// retains the original context when detecting new native inference after conversion.
pub(super) fn context(
    original: &PipeDtypesContext,
    hints: &Map,
    input: &PipeInput,
) -> Result<PipeDtypesContext, ApiError> {
    // Also validate trusted Rust callers; only the route's parser accepts encoded JSON.
    let hints = parse(Some(&Value::Map(hints.clone()).to_json_string()))?;
    let columns: BTreeSet<&str> = match input {
        PipeInput::Frame(frame) => frame
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .collect(),
        PipeInput::Docs(docs) => docs
            .iter()
            .flat_map(|doc| doc.keys().map(String::as_str))
            .collect(),
        PipeInput::Columns(columns) => columns.keys().map(String::as_str).collect(),
    };
    let mut context = original.clone();
    for (column, dtype) in hints {
        if !columns.contains(column.as_str()) {
            return Err(ApiError::new(
                400,
                "A special dtype hint names a column absent from this payload.",
            ));
        }
        let dtype = dtype
            .as_str()
            .expect("the hint parser requires string dtypes");
        if let Some(explicit) = original.explicit_dtypes.get(&column) {
            if !meerschaum_core::dtypes::are_dtypes_equal(explicit, dtype) {
                return Err(ApiError::new(
                    409,
                    "A transport dtype hint cannot override an explicit registered dtype.",
                ));
            }
            continue;
        }
        context.dtypes.insert(column.clone(), dtype.to_string());
        context.explicit_dtypes.insert(column, dtype.to_string());
    }
    Ok(context)
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_kernel::detect::get_special_cols;
    use meerschaum_kernel::enforce::pipe_enforce_dtypes;
    use meerschaum_kernel::value::Value as KValue;

    fn original() -> PipeDtypesContext {
        PipeDtypesContext {
            dtypes: Default::default(),
            explicit_dtypes: Default::default(),
            enforce: true,
            tz_aware: false,
            mixed_numerics: false,
        }
    }

    #[test]
    fn special_payload_hints_decode_values_without_mutating_the_registered_context() {
        let original = original();
        let input = PipeInput::Docs(vec![indexmap::IndexMap::from_iter([(
            "payload".into(),
            KValue::Str("AQID".into()),
        )])]);
        let hints = parse(Some(r#"{"payload":"bytes"}"#)).unwrap();
        assert_eq!(
            hints,
            parse(Some("{'payload': 'bytes'}")).unwrap(),
            "The existing SDK's Python-style mapping stopped decoding"
        );
        let transient = context(&original, &hints, &input).unwrap();
        let frame = pipe_enforce_dtypes(input, &transient).unwrap();
        assert_eq!(frame.row(0)[0], KValue::Bytes(vec![1, 2, 3]));
        assert_eq!(
            get_special_cols(&frame).get("payload").map(String::as_str),
            Some("bytes")
        );
        assert!(original.dtypes.is_empty() && original.explicit_dtypes.is_empty());
    }

    #[test]
    fn special_payload_hints_refuse_missing_columns_explicit_overrides_and_unknown_types() {
        let input = PipeInput::Docs(vec![indexmap::IndexMap::from_iter([(
            "payload".into(),
            KValue::Str("AQID".into()),
        )])]);
        let mut explicit = original();
        explicit.dtypes.insert("payload".into(), "str".into());
        explicit.explicit_dtypes = explicit.dtypes.clone();
        let hints = parse(Some(r#"{"payload":"bytes"}"#)).unwrap();
        assert!(context(&explicit, &hints, &input).is_err());
        assert!(
            context(
                &original(),
                &parse(Some(r#"{"absent":"bytes"}"#)).unwrap(),
                &input
            )
            .is_err()
        );
        for invalid in [
            r#"{"payload":"sql:secret"}"#,
            r#"{"payload":{"target":"elsewhere"}}"#,
            r#"["bytes"]"#,
            r#"{"payload":"numeric[not-a-number]"}"#,
            "{'payload': ['bytes']}",
            "{'payload': __import__('outside')}",
            "{'payload': ----------------1}",
        ] {
            assert!(parse(Some(invalid)).is_err(), "Accepted {invalid}");
        }
        assert!(parse(Some(&" ".repeat(MAX_BYTES + 1))).is_err());
    }
}
