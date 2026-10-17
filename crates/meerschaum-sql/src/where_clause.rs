//! `build_where(params, ...)` (`meerschaum/utils/sql.py:1085`).
//!
//! The `params` grammar:
//! - `{'c': 'red'}` → `"c" = 'red'`
//! - `{'c': ['red', 'blue']}` → `("c" IN ('red', 'blue'))`
//! - `{'c': '_red'}` → `"c" != 'red'`; `{'c': ['_red']}` → `("c" NOT IN ('red'))`
//! - `{'c': None}` / `'None'` → `"c" IS NULL`; `'_None'` → `"c" IS NOT NULL`
//! - `{'c': {'k': 'v'}}` → `CAST("c" AS TEXT) = '{"k": "v"}'`

use indexmap::IndexMap;

use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use meerschaum_core::Value;
use meerschaum_core::dtypes::value_is_null;

/// `STATIC_CONFIG['system']['fetch_pipes_keys']['negation_prefix']` (from `static.toml`).
pub fn negation_prefix() -> &'static str {
    &meerschaum_core::static_config()
        .system
        .fetch_pipes_keys
        .negation_prefix
}

/// The words that make `build_where` give up (`utils/sql.py:1134`).
const BAD_WORDS: [&str; 3] = ["drop ", "--", ";"];

/// Build the `WHERE` clause for `params` (`with_where` adds the leading `WHERE`).
///
/// Returns an empty string (and logs a warning, as Python does) when the serialized params
/// contain a banned word.
pub fn build_where(params: &IndexMap<String, Value>, flavor: Flavor, with_where: bool) -> String {
    build_where_with(params, flavor, with_where, WhereStyle::default())
}

/// Knobs of [`build_where_with`] that change the SQL text without changing its meaning.
#[derive(Debug, Clone, Copy, Default)]
pub struct WhereStyle {
    /// Write every `IN` / `NOT IN` list as `("col" IN (...)) = 1`, which SQLite's planner cannot
    /// drive an index with (a unary `+` on the column would too, but it drops the column's
    /// affinity, so `+"id" IN ('1')` never matches an integer). Set for datetime-bounded
    /// queries, where an `IN` list over (nearly) every id otherwise outranks the datetime range
    /// (SQLite has no statistics on a fresh table) and the "narrow" backtrack query scans a
    /// whole id's worth of the table.
    pub sqlite_no_index_on_lists: bool,
}

/// [`build_where`] with a [`WhereStyle`].
pub fn build_where_with(
    params: &IndexMap<String, Value>,
    flavor: Flavor,
    with_where: bool,
    style: WhereStyle,
) -> String {
    let params_json = Value::Map(params.clone()).json_dumps().to_lowercase();
    let negation_prefix = negation_prefix();
    for word in BAD_WORDS {
        if params_json.contains(word) {
            tracing::warn!("Aborting build_where() due to possible SQL injection.");
            return String::new();
        }
    }

    let none_if_null = |v: &Value| -> Option<Value> {
        if value_is_null(&v.py_str()) {
            None
        } else {
            Some(v.clone())
        }
    };

    let leading_and = "\n    AND ";
    let mut where_ = String::new();
    for (key, value) in params {
        let key_name = sql_item_name(key, flavor, None);
        match value {
            // Search across a list (`IN` syntax).
            Value::List(items) => {
                let hide_from_planner = style.sqlite_no_index_on_lists && flavor == Flavor::Sqlite;
                let (open, close) = if hide_from_planner {
                    ("(", ") = 1")
                } else {
                    ("", "")
                };
                let includes: Vec<Option<Value>> = items
                    .iter()
                    .filter(|item| !item.py_str().starts_with(negation_prefix))
                    .map(none_if_null)
                    .collect();
                let has_null_includes = includes.iter().any(Option::is_none);
                let not_null_includes: Vec<&Value> = includes.iter().flatten().collect();
                let excludes: Vec<Option<Value>> = items
                    .iter()
                    .filter(|item| item.py_str().starts_with(negation_prefix))
                    .map(|item| {
                        let s = item.py_str();
                        none_if_null(&Value::Str(s[negation_prefix.len()..].to_string()))
                    })
                    .collect();
                let has_null_excludes = excludes.iter().any(Option::is_none);
                let not_null_excludes: Vec<&Value> = excludes.iter().flatten().collect();

                if !includes.is_empty() {
                    where_.push_str(leading_and);
                    where_.push('(');
                }
                if !not_null_includes.is_empty() {
                    where_.push_str(&format!("{open}{key_name} IN ("));
                    let quoted: Vec<String> = not_null_includes
                        .iter()
                        .map(|item| format!("'{}'", item.py_str().replace('\'', "''")))
                        .collect();
                    where_.push_str(&quoted.join(", "));
                    where_.push(')');
                    where_.push_str(close);
                }
                if has_null_includes {
                    if !not_null_includes.is_empty() {
                        where_.push_str("\n    OR ");
                    }
                    where_.push_str(&format!("{key_name} IS NULL"));
                }
                if !includes.is_empty() {
                    where_.push(')');
                }

                if !excludes.is_empty() {
                    where_.push_str(leading_and);
                    where_.push('(');
                }
                if !not_null_excludes.is_empty() {
                    where_.push_str(&format!("{open}{key_name} NOT IN ("));
                    let quoted: Vec<String> = not_null_excludes
                        .iter()
                        .map(|item| format!("'{}'", item.py_str().replace('\'', "''")))
                        .collect();
                    where_.push_str(&quoted.join(", "));
                    where_.push(')');
                    where_.push_str(close);
                }
                if has_null_excludes {
                    if !not_null_excludes.is_empty() {
                        where_.push_str("\n    AND ");
                    }
                    where_.push_str(&format!("{key_name} IS NOT NULL"));
                }
                if !excludes.is_empty() {
                    where_.push(')');
                }
            }
            // Search a dictionary.
            Value::Map(_) => {
                where_.push_str(&format!(
                    "{leading_and}CAST({key_name} AS TEXT) = '{}'",
                    value.json_dumps()
                ));
            }
            _ => {
                let mut eq_sign = "=";
                let mut is_null = "IS NULL";
                let mut value: Option<String> = match value {
                    Value::Null => None,
                    other => Some(other.py_str()),
                };
                let as_str = value.clone().unwrap_or_else(|| "None".to_string());
                // PARITY: `str(value).lstrip(negation_prefix)` strips *every* leading underscore
                // (`utils/sql.py:1198`), so `'__None'` is still treated as `IS NOT NULL`.
                if value_is_null(as_str.trim_start_matches(negation_prefix)) {
                    value = if as_str.starts_with(negation_prefix) {
                        Some(format!("{negation_prefix}None"))
                    } else {
                        None
                    };
                }
                let current = value.clone().unwrap_or_else(|| "None".to_string());
                if let Some(stripped) = current.strip_prefix(negation_prefix) {
                    let stripped = stripped.to_string();
                    eq_sign = "!=";
                    if value_is_null(&stripped) {
                        value = None;
                        is_null = "IS NOT NULL";
                    } else {
                        value = Some(stripped);
                    }
                }
                match value {
                    None => where_.push_str(&format!("{leading_and}{key_name} {is_null}")),
                    Some(v) => where_.push_str(&format!(
                        "{leading_and}{key_name} {eq_sign} '{}'",
                        v.replace('\'', "''")
                    )),
                }
            }
        }
    }

    if where_.len() > 1 {
        let body = &where_[leading_and.len()..];
        where_ = format!("{}{}", if with_where { "\nWHERE\n    " } else { "" }, body);
    }
    where_
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(json: &str) -> IndexMap<String, Value> {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn list_in() {
        assert_eq!(
            build_where(&params(r#"{"foo": [1, 2, 3]}"#), Flavor::Postgresql, true),
            "\nWHERE\n    (\"foo\" IN ('1', '2', '3'))"
        );
    }

    #[test]
    fn sqlite_list_without_index() {
        let style = WhereStyle {
            sqlite_no_index_on_lists: true,
        };
        assert_eq!(
            build_where_with(
                &params(r#"{"foo": [1, 2], "bar": "x"}"#),
                Flavor::Sqlite,
                true,
                style
            ),
            "\nWHERE\n    ((\"foo\" IN ('1', '2')) = 1)\n    AND \"bar\" = 'x'"
        );
        // Other flavors ignore the style.
        assert_eq!(
            build_where_with(&params(r#"{"foo": [1]}"#), Flavor::Postgresql, true, style),
            "\nWHERE\n    (\"foo\" IN ('1'))"
        );
    }

    #[test]
    fn negations() {
        assert_eq!(
            build_where(
                &params(r#"{"a": "_x", "b": "_None", "c": null}"#),
                Flavor::Sqlite,
                false
            ),
            "\"a\" != 'x'\n    AND \"b\" IS NOT NULL\n    AND \"c\" IS NULL"
        );
    }

    #[test]
    fn injection() {
        assert_eq!(
            build_where(&params(r#"{"a": "x; drop table y"}"#), Flavor::Sqlite, true),
            ""
        );
    }
}
