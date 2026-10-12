//! Target table naming: `truncate_item_name` (`meerschaum/utils/sql.py`) and
//! `truncate_string_sections` (`meerschaum/utils/misc.py`).

/// `max_name_lens` from `meerschaum/utils/sql.py`: the identifier length limit per SQL flavor.
pub const MAX_NAME_LENS: &[(&str, usize)] = &[
    ("default", 64),
    ("mssql", 128),
    ("oracle", 30),
    ("postgresql", 64),
    ("postgis", 64),
    ("timescaledb", 64),
    ("timescaledb-ha", 64),
    ("citus", 64),
    ("cockroachdb", 64),
    ("sqlite", 1024),
    ("geopackage", 1024),
    ("mysql", 64),
    ("mariadb", 64),
];

/// The identifier length limit for a flavor (`max_name_lens.get(flavor, max_name_lens['default'])`).
pub fn max_name_len(flavor: &str) -> usize {
    MAX_NAME_LENS
        .iter()
        .find(|(f, _)| *f == flavor)
        .map_or(64, |(_, n)| *n)
}

/// A name that cannot be shortened enough.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("String could not be truncated: '{0}'")]
pub struct TruncateError(pub String);

/// Port of `truncate_string_sections`: shorten every `delimiter`-separated section (longest
/// first, one character per round) until the name fits.
///
/// PARITY: names of exactly `max_len` characters are still processed (`len(item) < max_len`
/// is the early return), so they lose characters (`utils/misc.py:1323`).
pub fn truncate_string_sections(
    item: &str,
    delimiter: &str,
    max_len: usize,
) -> Result<String, TruncateError> {
    if item.chars().count() < max_len {
        return Ok(item.to_string());
    }
    let sections: Vec<(usize, String)> = item
        .split(delimiter)
        .map(String::from)
        .enumerate()
        .collect();
    let mut sorted = sections.clone();
    // Python's `sorted` is stable: equal lengths keep their original order.
    sorted.sort_by_key(|(_, s)| std::cmp::Reverse(s.chars().count()));
    let available = max_len as i64 - sections.len() as i64;
    let total =
        |v: &[(usize, String)]| -> i64 { v.iter().map(|(_, s)| s.chars().count() as i64).sum() };
    let mut current = sorted;
    let mut len = total(&current);
    while len > available {
        current = current
            .into_iter()
            .map(|(i, s)| {
                let n = s.chars().count();
                if n > 1 {
                    (i, s.chars().take(n - 1).collect())
                } else {
                    (i, s)
                }
            })
            .collect();
        let old = len;
        len = total(&current);
        if old == len {
            return Err(TruncateError(item.to_string()));
        }
    }
    current.sort_by_key(|(i, _)| *i);
    Ok(current
        .into_iter()
        .map(|(_, s)| s)
        .collect::<Vec<_>>()
        .join(delimiter))
}

/// Port of `truncate_item_name(item, flavor)`.
pub fn truncate_item_name(item: &str, flavor: &str) -> Result<String, TruncateError> {
    truncate_string_sections(item, "_", max_name_len(flavor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncation() {
        assert_eq!(
            truncate_string_sections("abc_def_ghi", "_", 10).unwrap(),
            "ab_de_gh"
        );
        let name = "a".repeat(64);
        // PARITY: exactly max_len is truncated.
        assert_eq!(truncate_item_name(&name, "postgresql").unwrap().len(), 63);
        assert_eq!(truncate_item_name("short", "oracle").unwrap(), "short");
    }
}
