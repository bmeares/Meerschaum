//! Identifier quoting and sanitizing: `clean`, `sql_item_name`, `pg_capital`, `oracle_capital`,
//! `truncate_item_name` (`meerschaum/utils/sql.py:668-1082`).

use crate::error::{Result, SqlError};
use crate::flavor::{Flavor, max_name_len_for, table_wrappers_for};
use meerschaum_core::pipe::truncate_string_sections_best_effort;

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

/// Words that must stay quoted on Oracle: v4's list (`utils/sql.py:986`: `float`, `varchar`,
/// `nvarchar`, `clob`, `boolean`, `integer`, `table`, `row`, `date`, `time`) plus Oracle's true
/// reserved words (every word that cannot be an unquoted column name).
///
/// PARITY (W17O-003 in `crates/meerschaum-py/PARITY.md`): v4 quotes only its ten words, so a column named `uid`, `user`, `level`, `comment`,
/// `size`, `start`, ... fails to create (`ORA-00904`); v5 quotes them on purpose. This cannot
/// change a name v4 created, because v4 could not create a table with such a name. The extra
/// words are the Oracle SQL Language Reference's "Oracle SQL Reserved Words" table, verified on
/// Oracle XE 21c by `CREATE TABLE t (x NUMBER, <word> NUMBER)` for every keyword in
/// `V$RESERVED_WORDS` (109 failures: the documented words except `COLUMN_VALUE` and
/// `NESTED_TABLE_ID`, which are creatable, plus `EXCEPT`); [`ORACLE_RESERVED_EXTRA`] is those 109
/// minus the six that are also on v4's list. The golden test strips exactly these quotes from the Rust output on the Oracle
/// flavor (`tests/golden.rs`).
pub const ORACLE_RESERVED_V4: [&str; 10] = [
    "float", "varchar", "nvarchar", "clob", "boolean", "integer", "table", "row", "date", "time",
];

/// Oracle's true reserved words beyond [`ORACLE_RESERVED_V4`] (see there): quoted by v5 only.
pub const ORACLE_RESERVED_EXTRA: &[&str] = &[
    "access",
    "add",
    "all",
    "alter",
    "and",
    "any",
    "as",
    "asc",
    "audit",
    "between",
    "by",
    "char",
    "check",
    "cluster",
    "column",
    "comment",
    "compress",
    "connect",
    "create",
    "current",
    "decimal",
    "default",
    "delete",
    "desc",
    "distinct",
    "drop",
    "else",
    "except",
    "exclusive",
    "exists",
    "file",
    "for",
    "from",
    "grant",
    "group",
    "having",
    "identified",
    "immediate",
    "in",
    "increment",
    "index",
    "initial",
    "insert",
    "intersect",
    "into",
    "is",
    "level",
    "like",
    "lock",
    "long",
    "maxextents",
    "minus",
    "mlslabel",
    "mode",
    "modify",
    "noaudit",
    "nocompress",
    "not",
    "nowait",
    "null",
    "number",
    "of",
    "offline",
    "on",
    "online",
    "option",
    "or",
    "order",
    "pctfree",
    "prior",
    "public",
    "raw",
    "rename",
    "resource",
    "revoke",
    "rowid",
    "rownum",
    "rows",
    "select",
    "session",
    "set",
    "share",
    "size",
    "smallint",
    "start",
    "successful",
    "synonym",
    "sysdate",
    "then",
    "to",
    "trigger",
    "uid",
    "union",
    "unique",
    "update",
    "user",
    "validate",
    "values",
    "varchar2",
    "view",
    "whenever",
    "where",
    "with",
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
        let lower_item = truncated_item.to_lowercase();
        if ORACLE_RESERVED_V4.contains(&lower_item.as_str())
            || ORACLE_RESERVED_EXTRA.contains(&lower_item.as_str())
        {
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
///
/// PARITY: Python raises `String could not be truncated` when a name has too many sections to
/// fit (`utils/misc.py:1336`); the quoting helpers cannot fail, so the shortest name is used.
pub fn truncate_item_name_opt(item: &str, flavor: Option<Flavor>) -> String {
    truncate_string_sections_best_effort(item, "_", max_name_len_for(flavor))
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
