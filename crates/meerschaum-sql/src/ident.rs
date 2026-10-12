//! Identifier quoting and sanitizing: `clean`, `sql_item_name`, `pg_capital`, `oracle_capital`,
//! `truncate_item_name` (`meerschaum/utils/sql.py:668-1082`).

use crate::error::{Result, SqlError};
use crate::flavor::{Flavor, max_name_len_for, table_wrappers_for};
use crate::pytext::truncate_string_sections;

/// The substrings `clean()` refuses (`utils/sql.py:674`).
pub const BANNED_SYMBOLS: [&str; 14] = [
    ";",
    "--",
    "drop ",
    "/*",
    "*/",
    "union ",
    "exec ",
    "execute ",
    "insert ",
    "update ",
    "delete ",
    "create ",
    "alter ",
    "truncate ",
];

/// `clean(substring)`: raise if `substring` contains a banned keyword or symbol.
///
/// This is a keyword blocklist only (quote-only payloads pass). Use it for internal names.
pub fn clean(substring: &str) -> Result<()> {
    let lower = substring.to_lowercase();
    for symbol in BANNED_SYMBOLS {
        if lower.contains(symbol) {
            return Err(SqlError::InvalidString(substring.to_string()));
        }
    }
    Ok(())
}

/// Reserved words that must stay quoted on Oracle (`utils/sql.py:986`).
const ORACLE_RESERVED: [&str; 10] = [
    "float", "varchar", "nvarchar", "clob", "boolean", "integer", "table", "row", "date", "time",
];

/// `sql_item_name(item, flavor, schema)`: quote (and truncate) a table, column or index name.
pub fn sql_item_name(item: &str, flavor: Flavor, schema: Option<&str>) -> String {
    sql_item_name_opt(item, Some(flavor), schema)
}

/// `sql_item_name` for a flavor key that may not be a real flavor (`None` → `'default'`).
pub fn sql_item_name_opt(item: &str, flavor: Option<Flavor>, schema: Option<&str>) -> String {
    let mut truncated_item = truncate_item_name_opt(item, flavor);
    let wrappers = if flavor == Some(Flavor::Oracle) {
        truncated_item = pg_capital(&truncated_item, true);
        // NOTE: System-reserved words must be quoted.
        if ORACLE_RESERVED.contains(&truncated_item.to_lowercase().as_str()) {
            ("\"", "\"")
        } else {
            ("", "")
        }
    } else {
        table_wrappers_for(flavor)
    };

    // NOTE: SQLite does not support schemas.
    let schema = match flavor {
        Some(Flavor::Sqlite | Flavor::Geopackage) => None,
        Some(Flavor::Mssql) if item.starts_with('#') => None,
        _ => schema,
    };

    let schema_prefix = match schema {
        // PARITY: on Oracle the wrappers are empty unless the *item* is reserved, so the schema
        // is emitted bare (or with the item's quotes) (`utils/sql.py:1002`).
        Some(s) => format!("{}{}{}.", wrappers.0, s, wrappers.1),
        None => String::new(),
    };
    format!(
        "{}{}{}{}",
        schema_prefix, wrappers.0, truncated_item, wrappers.1
    )
}

/// `pg_capital(s, quote_capitals)`: wrap in double quotes only if needed.
pub fn pg_capital(s: &str, quote_capitals: bool) -> String {
    if s.starts_with('"') && s.ends_with('"') {
        return s.to_string();
    }
    let s = s.replace('"', "");
    let mut needs_quotes = s.starts_with('_');
    if !needs_quotes {
        for c in s.chars() {
            if c == '_' {
                continue;
            }
            if !c.is_alphanumeric() || (quote_capitals && c.is_uppercase()) {
                needs_quotes = true;
                break;
            }
        }
    }
    if needs_quotes { format!("\"{s}\"") } else { s }
}

/// `oracle_capital(s)`: a no-op kept for API parity (`utils/sql.py:1056`).
pub fn oracle_capital(s: &str) -> String {
    s.to_string()
}

/// `truncate_item_name(item, flavor)`: shorten to the flavor's identifier length limit.
pub fn truncate_item_name(item: &str, flavor: Flavor) -> String {
    truncate_item_name_opt(item, Some(flavor))
}

/// `truncate_item_name` for an optional flavor key (`None` → `'default'`).
pub fn truncate_item_name_opt(item: &str, flavor: Option<Flavor>) -> String {
    truncate_string_sections(item, "_", max_name_len_for(flavor))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting() {
        assert_eq!(sql_item_name("table", Flavor::Sqlite, None), "\"table\"");
        assert_eq!(sql_item_name("table", Flavor::Mssql, None), "[table]");
        assert_eq!(
            sql_item_name("table", Flavor::Postgresql, Some("abc")),
            "\"abc\".\"table\""
        );
        assert_eq!(sql_item_name("abc", Flavor::Oracle, None), "abc");
        assert_eq!(sql_item_name("table", Flavor::Oracle, None), "\"table\"");
        assert_eq!(sql_item_name("MyCol", Flavor::Oracle, None), "\"MyCol\"");
        assert_eq!(sql_item_name("#tmp", Flavor::Mssql, Some("dbo")), "[#tmp]");
    }

    #[test]
    fn cleaning() {
        assert!(clean("abc").is_ok());
        assert!(clean("a; DROP TABLE x").is_err());
        assert!(clean("' OR '1'='1").is_ok());
    }
}
