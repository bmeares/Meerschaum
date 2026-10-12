//! CTE helpers: `find_top_level_select_index`, `get_leading_with_index`, `wrap_query_with_cte`,
//! `format_cte_subquery` (`meerschaum/utils/sql.py:2687-2940`).
//!
//! Indices are code-point indices into the query, like Python string indices.

use crate::error::{Result, SqlError};
use crate::flavor::Flavor;
use crate::ident::sql_item_name;
use crate::pytext::{indent, py_lstrip, py_rstrip, py_strip};

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

fn find_from(chars: &[char], needle: &[char], start: usize) -> Option<usize> {
    if needle.is_empty() || start > chars.len() {
        return None;
    }
    (start..=chars.len().saturating_sub(needle.len())).find(|&i| chars[i..].starts_with(needle))
}

/// Lowercase the query per character so indices line up with the original.
///
/// PARITY: Python lowercases the whole string (`query.lower()`), which can change the length of
/// a handful of non-ASCII characters (e.g. `'İ'`); this keeps one character per character.
fn lower_chars(query: &str) -> Vec<char> {
    query
        .chars()
        .map(|c| c.to_lowercase().next().unwrap_or(c))
        .collect()
}

/// `find_top_level_select_index(query, last)`: the code-point index of the first (or last)
/// top-level `SELECT`, skipping comments, string literals, quoted identifiers and parentheses.
pub fn find_top_level_select_index(query: &str, last: bool) -> Option<usize> {
    let q = lower_chars(query);
    let n = q.len();
    let mut depth: i64 = 0;
    let mut found = None;
    let mut i = 0;
    let select: Vec<char> = "select".chars().collect();
    while i < n {
        let ch = q[i];
        let nxt = if i + 1 < n { Some(q[i + 1]) } else { None };
        if ch == '-' && nxt == Some('-') {
            i = match find_from(&q, &['\n'], i) {
                None => n,
                Some(eol) => eol + 1,
            };
            continue;
        }
        if ch == '/' && nxt == Some('*') {
            i = match find_from(&q, &['*', '/'], i + 2) {
                None => n,
                Some(end) => end + 2,
            };
            continue;
        }
        if ch == '\'' {
            i += 1;
            while i < n {
                if q[i] == '\'' {
                    if i + 1 < n && q[i + 1] == '\'' {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
            continue;
        }
        if ch == '"' || ch == '`' {
            i = match find_from(&q, &[ch], i + 1) {
                None => n,
                Some(end) => end + 1,
            };
            continue;
        }
        if ch == '[' {
            i = match find_from(&q, &[']'], i + 1) {
                None => n,
                Some(end) => end + 1,
            };
            continue;
        }
        if ch == '(' {
            depth += 1;
            i += 1;
            continue;
        }
        if ch == ')' {
            depth -= 1;
            i += 1;
            continue;
        }
        if depth == 0 && ch == 's' && q[i..].starts_with(&select) {
            let before_ok = i == 0 || !is_word_char(q[i - 1]);
            let after_ok = i + 6 == n || !is_word_char(q[i + 6]);
            if before_ok && after_ok {
                if !last {
                    return Some(i);
                }
                found = Some(i);
                i += 6;
                continue;
            }
        }
        i += 1;
    }
    found
}

/// `get_leading_with_index(query)`: the index of a leading `WITH` (after whitespace and
/// comments), or `None`.
pub fn get_leading_with_index(query: &str) -> Option<usize> {
    let q = lower_chars(query);
    let n = q.len();
    let mut i = 0;
    while i < n {
        let ch = q[i];
        let nxt = if i + 1 < n { Some(q[i + 1]) } else { None };
        if matches!(ch, ' ' | '\t' | '\r' | '\n') {
            i += 1;
            continue;
        }
        if ch == '-' && nxt == Some('-') {
            i = match find_from(&q, &['\n'], i) {
                None => n,
                Some(eol) => eol + 1,
            };
            continue;
        }
        if ch == '/' && nxt == Some('*') {
            i = match find_from(&q, &['*', '/'], i + 2) {
                None => n,
                Some(end) => end + 2,
            };
            continue;
        }
        break;
    }
    let with: Vec<char> = "with".chars().collect();
    if i <= n && q[i..].starts_with(&with) && (i + 4 == n || !is_word_char(q[i + 4])) {
        return Some(i);
    }
    None
}

/// Slice a string by code-point indices.
pub(crate) fn char_slice(s: &str, start: usize, end: Option<usize>) -> String {
    let it = s.chars().skip(start);
    match end {
        Some(e) => it.take(e.saturating_sub(start)).collect(),
        None => it.collect(),
    }
}

/// `wrap_query_with_cte(sub_query, parent_query, flavor, cte_name)`.
pub fn wrap_query_with_cte(
    sub_query: &str,
    parent_query: &str,
    flavor: Flavor,
    cte_name: &str,
) -> Result<String> {
    let sub_query = py_lstrip(sub_query);
    let parent_query = py_lstrip(parent_query);
    let cte_name_quoted = sql_item_name(cte_name, flavor, None);

    if flavor.no_cte() {
        // PARITY: plain substring replacement of the CTE name (`utils/sql.py:2850`), so other
        // identifiers containing it are rewritten too.
        let marker = "--MRSM_SUBQUERY--";
        return Ok(parent_query
            .replace(&cte_name_quoted, marker)
            .replace(cte_name, marker)
            .replace(marker, &format!("(\n{sub_query}\n) AS {cte_name_quoted}")));
    }

    let lead = if get_leading_with_index(sub_query).is_some() {
        let select_index = find_top_level_select_index(sub_query, false)
            .ok_or_else(|| SqlError::NoTopLevelSelect(sub_query.to_string()))?;
        let definitions_raw = char_slice(sub_query, 0, Some(select_index));
        let mut definitions = py_rstrip(&definitions_raw).to_string();
        if definitions.ends_with(',') {
            definitions.pop();
            definitions = py_rstrip(&definitions).to_string();
        }
        let body_raw = char_slice(sub_query, select_index, None);
        let body = py_strip(&body_raw);
        format!(
            "{definitions}\n, {cte_name_quoted} AS (\n{}\n)",
            indent(body, "    ")
        )
    } else {
        format!(
            "WITH {cte_name_quoted} AS (\n{}\n)",
            indent(sub_query, "    ")
        )
    };

    let Some(parent_with_index) = get_leading_with_index(parent_query) else {
        return Ok(format!("{lead}\n{parent_query}"));
    };
    // The parent query declares CTEs of its own: hoist them after the new definition.
    let prefix_raw = char_slice(parent_query, 0, Some(parent_with_index));
    let parent_prefix = py_rstrip(&prefix_raw);
    let rest_raw = char_slice(parent_query, parent_with_index + 4, None);
    let parent_rest = py_lstrip(&rest_raw);
    let prefix = if parent_prefix.is_empty() {
        String::new()
    } else {
        format!("{parent_prefix}\n")
    };
    Ok(format!("{prefix}{lead}\n, {parent_rest}"))
}

/// The `cols_to_select` argument of [`format_cte_subquery`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColsToSelect {
    /// A pre-quoted string inserted verbatim (default `'*'`).
    Raw(String),
    /// Column names to quote and join with commas.
    Columns(Vec<String>),
}

/// `format_cte_subquery(sub_query, flavor, sub_name, cols_to_select)`.
pub fn format_cte_subquery(
    sub_query: &str,
    flavor: Flavor,
    sub_name: &str,
    cols_to_select: &ColsToSelect,
) -> Result<String> {
    let quoted_sub_name = sql_item_name(sub_name, flavor, None);
    let cols_str = match cols_to_select {
        ColsToSelect::Raw(s) => s.clone(),
        ColsToSelect::Columns(cols) => cols
            .iter()
            .map(|c| sql_item_name(c, flavor, None))
            .collect::<Vec<_>>()
            .join(", "),
    };
    let parent_query = format!("SELECT {cols_str}\nFROM {quoted_sub_name}");
    wrap_query_with_cte(sub_query, &parent_query, flavor, sub_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docstring_example() {
        let sub = "WITH foo AS (SELECT 1 AS val) SELECT (val * 2) AS newval FROM foo";
        let parent = "SELECT newval * 3 FROM src";
        assert_eq!(
            wrap_query_with_cte(sub, parent, Flavor::Mssql, "src").unwrap(),
            "WITH foo AS (SELECT 1 AS val)\n, [src] AS (\n    SELECT (val * 2) AS newval FROM foo\n)\nSELECT newval * 3 FROM src"
        );
    }

    #[test]
    fn select_index() {
        assert_eq!(
            find_top_level_select_index("-- select\nSELECT 1", false),
            Some(10)
        );
        assert_eq!(
            find_top_level_select_index("WITH a AS (SELECT 1) SELECT * FROM a", true),
            Some(21)
        );
        assert_eq!(find_top_level_select_index("(SELECT 1)", false), None);
        assert_eq!(get_leading_with_index("  /* c */ with x"), Some(10));
        assert_eq!(get_leading_with_index("withx"), None);
    }
}
