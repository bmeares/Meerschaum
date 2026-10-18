//! Table introspection at runtime: `get_table_cols_types`, `get_table_cols_indices`, and the
//! SQLAlchemy reflection path `get_pipe_columns_types` takes on SQLite and MySQL / MariaDB
//! (`str(column.type)` of a reflected `sqlalchemy.Table`).

use indexmap::IndexMap;
use meerschaum_kernel::value::Value as KValue;

use crate::flavor::{DriverFamily, Flavor};
use crate::queries::table::{
    ColumnIndexRow, ColumnTypeRow, IndexDoc, cols_indices_from_rows, cols_types_from_rows,
    get_postgis_geo_columns_types_query, get_table_cols_indices_query, get_table_cols_types_query,
    introspection_schema, needs_postgis_lookup, postgis_geo_columns_types_from_rows,
};

use super::{SqlConnector, frame_docs, kv_int, kv_str};

/// SQLAlchemy's SQLite type reflection (`SQLiteDialect._resolve_type_affinity`) followed by
/// `str(type)`.
///
/// PARITY: positional arguments are applied to the type's constructor, so `TEXT(10,2)` becomes
/// `TEXT(length=10, collation=2)` and renders as `TEXT(10) COLLATE "2"`.
pub fn sqlite_reflected_type(declared: &str) -> String {
    let upper = declared.to_uppercase();
    let (coltype, args): (String, Option<Vec<i64>>) = {
        let name_end = upper
            .char_indices()
            .find(|(_, c)| !(c.is_alphanumeric() || *c == '_' || *c == ' '))
            .map(|(i, _)| i)
            .unwrap_or(upper.len());
        let name = upper[..name_end].to_string();
        let rest = &upper[name_end..];
        let args = if rest.starts_with('(') {
            rest.find(')').map(|close| {
                let inner = &rest[1..close];
                let mut nums = Vec::new();
                let mut cur = String::new();
                for c in inner.chars() {
                    if c.is_ascii_digit() {
                        cur.push(c);
                    } else if !cur.is_empty() {
                        nums.push(cur.parse().unwrap_or(0));
                        cur.clear();
                    }
                }
                if !cur.is_empty() {
                    nums.push(cur.parse().unwrap_or(0));
                }
                nums
            })
        } else {
            None
        };
        (name, args)
    };
    let args = args.unwrap_or_default();
    #[derive(PartialEq)]
    enum T {
        Plain(&'static str),
        Str(&'static str),
        Numeric(&'static str),
        Float(&'static str),
        Null,
    }
    let ty = match coltype.as_str() {
        "BIGINT" => T::Plain("BIGINT"),
        "BLOB" => T::Plain("BLOB"),
        "BOOL" | "BOOLEAN" => T::Plain("BOOLEAN"),
        "CHAR" => T::Str("CHAR"),
        "DATE" | "DATE_CHAR" => T::Plain("DATE"),
        "DATETIME" | "DATETIME_CHAR" => T::Plain("DATETIME"),
        "DOUBLE" => T::Plain("DOUBLE"),
        "DECIMAL" => T::Numeric("DECIMAL"),
        "FLOAT" => T::Float("FLOAT"),
        "INT" | "INTEGER" => T::Plain("INTEGER"),
        "JSON" => T::Plain("JSON"),
        "NUMERIC" => T::Numeric("NUMERIC"),
        "REAL" => T::Plain("REAL"),
        "SMALLINT" => T::Plain("SMALLINT"),
        "TEXT" => T::Str("TEXT"),
        "TIME" | "TIME_CHAR" => T::Plain("TIME"),
        "TIMESTAMP" => T::Plain("TIMESTAMP"),
        "VARCHAR" => T::Str("VARCHAR"),
        "NVARCHAR" => T::Str("NVARCHAR"),
        "NCHAR" => T::Str("NCHAR"),
        c if c.contains("INT") => T::Plain("INTEGER"),
        c if c.contains("CHAR") || c.contains("CLOB") || c.contains("TEXT") => T::Str("TEXT"),
        c if c.contains("BLOB") || c.is_empty() => T::Null,
        c if c.contains("REAL") || c.contains("FLOA") || c.contains("DOUB") => T::Plain("REAL"),
        _ => T::Numeric("NUMERIC"),
    };
    match ty {
        T::Null => "NULL".to_string(),
        T::Plain(name) => {
            if name == "BLOB"
                && let Some(len) = args.first().filter(|l| **l != 0)
            {
                return format!("BLOB({len})");
            }
            name.to_string()
        }
        T::Str(name) => {
            let mut out = name.to_string();
            if args.len() > 2 {
                return name.to_string();
            }
            if let Some(len) = args.first().filter(|l| **l != 0) {
                out.push_str(&format!("({len})"));
            }
            if let Some(coll) = args.get(1) {
                out.push_str(&format!(" COLLATE \"{coll}\""));
            }
            out
        }
        T::Numeric(name) => match args.as_slice() {
            [] => name.to_string(),
            [p] => format!("{name}({p})"),
            [p, s] | [p, s, _] => format!("{name}({p}, {s})"),
            _ => name.to_string(),
        },
        T::Float(name) => match args.as_slice() {
            [] => name.to_string(),
            [p] | [p, _] | [p, _, _] => format!("{name}({p})"),
            _ => name.to_string(),
        },
    }
}

/// SQLAlchemy's MySQL / MariaDB type reflection (`SHOW CREATE TABLE`) followed by `str(type)`.
pub fn mysql_reflected_type(column_type: &str) -> String {
    let lower = column_type.trim().to_lowercase();
    let name_end = lower.find(['(', ' ']).unwrap_or(lower.len());
    let name = &lower[..name_end];
    let args: Vec<String> = lower[name_end..]
        .strip_prefix('(')
        .and_then(|r| r.split_once(')'))
        .map(|(inner, _)| inner.split(',').map(|a| a.trim().to_string()).collect())
        .unwrap_or_default();
    let upper = name.to_uppercase();
    match name {
        "int" | "integer" => "INTEGER".into(),
        "tinyint" | "smallint" | "mediumint" | "bigint" | "bit" | "year" => upper,
        "decimal" | "numeric" | "dec" | "fixed" => match args.as_slice() {
            [] => "DECIMAL".into(),
            [p] => format!("DECIMAL({p})"),
            [p, s, ..] => format!("DECIMAL({p}, {s})"),
        },
        "float" => match args.as_slice() {
            [p] => format!("FLOAT({p})"),
            _ => "FLOAT".into(),
        },
        "double" | "real" => "DOUBLE".into(),
        "char" | "varchar" | "binary" | "varbinary" => match args.first() {
            Some(len) => format!("{upper}({len})"),
            None => upper,
        },
        "tinytext" | "text" | "mediumtext" | "longtext" | "tinyblob" | "blob" | "mediumblob"
        | "longblob" | "datetime" | "timestamp" | "date" | "time" | "json" => upper,
        "boolean" | "bool" => "BOOL".into(),
        // Unrecognized types (e.g. spatial types) reflect as `NullType`.
        _ => "NULL".into(),
    }
}

/// The words SQLAlchemy's Oracle dialect quotes (`sqlalchemy/dialects/oracle/base.py`
/// `RESERVED_WORDS`); `normalize_name` / `denormalize_name` use them to decide whether a name
/// needs quotes.
const SQLALCHEMY_ORACLE_RESERVED: &[&str] = &[
    "share",
    "raw",
    "drop",
    "between",
    "from",
    "desc",
    "option",
    "prior",
    "long",
    "then",
    "default",
    "alter",
    "is",
    "into",
    "minus",
    "integer",
    "number",
    "grant",
    "identified",
    "all",
    "to",
    "order",
    "on",
    "float",
    "date",
    "having",
    "cluster",
    "nowait",
    "resource",
    "any",
    "table",
    "index",
    "for",
    "update",
    "where",
    "check",
    "smallint",
    "with",
    "delete",
    "by",
    "asc",
    "revoke",
    "like",
    "size",
    "rename",
    "nocompress",
    "null",
    "group",
    "values",
    "as",
    "in",
    "view",
    "exclusive",
    "compress",
    "synonym",
    "select",
    "insert",
    "exists",
    "not",
    "trigger",
    "else",
    "create",
    "intersect",
    "pctfree",
    "distinct",
    "user",
    "connect",
    "set",
    "mode",
    "of",
    "unique",
    "varchar2",
    "varchar",
    "lock",
    "or",
    "char",
    "decimal",
    "union",
    "public",
    "and",
    "start",
    "uid",
    "comment",
    "current",
    "level",
];

/// Whether SQLAlchemy's Oracle identifier preparer would quote the lowercase name `lower`
/// (`IdentifierPreparer._requires_quotes` for a name that already is lowercase).
fn oracle_requires_quotes(lower: &str) -> bool {
    SQLALCHEMY_ORACLE_RESERVED.contains(&lower)
        || lower
            .chars()
            .next()
            .is_none_or(|c| c.is_ascii_digit() || c == '_' || c == '$')
        || !lower
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '$')
}

/// `OracleDialect.normalize_name`: an all-uppercase name that needs no quotes is lowercased.
fn oracle_normalize_name(name: &str) -> String {
    let lower = name.to_lowercase();
    if name.to_uppercase() == name && !oracle_requires_quotes(&lower) {
        lower
    } else {
        name.to_string()
    }
}

/// `OracleDialect.denormalize_name`: a lowercase name that needs no quotes is uppercased.
fn oracle_denormalize_name(name: &str) -> String {
    if name.to_lowercase() == name && !oracle_requires_quotes(name) {
        name.to_uppercase()
    } else {
        name.to_string()
    }
}

/// SQLAlchemy's Oracle type reflection (`OracleDialect.get_multi_columns`) followed by
/// `str(column.type)`, the strings v4's `get_pipe_columns_types` returns on Oracle.
///
/// PARITY: `sqlalchemy/dialects/oracle/base.py` `get_multi_columns`: `NUMBER` with no precision and
/// scale 0 is `INTEGER`, every other `NUMBER` renders as a bare `NUMBER` (no precision), `FLOAT` with
/// binary precision 126 / 63 is `DOUBLE PRECISION` / `REAL`, character types render `VARCHAR(n)` /
/// `NVARCHAR(n)` / `CHAR(n)` / `NCHAR(n)` with the character length, `RAW` drops its length, every
/// `TIMESTAMP` variant is `TIMESTAMP`, `INTERVAL DAY TO SECOND` is `INTERVAL`, and a type SQLAlchemy
/// does not know reflects as `NullType` (`NULL`).
pub fn oracle_reflected_type(
    data_type: &str,
    char_length: Option<i64>,
    precision: Option<i64>,
    scale: Option<i64>,
) -> String {
    match data_type {
        "NUMBER" => {
            if precision.is_none() && scale == Some(0) {
                "INTEGER".into()
            } else {
                "NUMBER".into()
            }
        }
        "FLOAT" => match precision {
            Some(126) => "DOUBLE PRECISION".into(),
            Some(63) => "REAL".into(),
            _ => "FLOAT".into(),
        },
        "VARCHAR2" | "NVARCHAR2" | "CHAR" | "NCHAR" => {
            let name = match data_type {
                "VARCHAR2" => "VARCHAR",
                "NVARCHAR2" => "NVARCHAR",
                other => other,
            };
            match char_length {
                Some(n) => format!("{name}({n})"),
                None => name.to_string(),
            }
        }
        "RAW" => "RAW".into(),
        t if t.contains("WITH TIME ZONE") || t.contains("WITH LOCAL TIME ZONE") => {
            "TIMESTAMP".into()
        }
        other => {
            // `re.sub(r"\(\d+\)", "", coltype)`.
            let mut stripped = String::new();
            let mut rest = other;
            while let Some(open) = rest.find('(') {
                match rest[open + 1..].find(')') {
                    Some(close)
                        if close > 0
                            && rest[open + 1..open + 1 + close]
                                .chars()
                                .all(|c| c.is_ascii_digit()) =>
                    {
                        stripped.push_str(&rest[..open]);
                        rest = &rest[open + close + 2..];
                    }
                    _ => {
                        stripped.push_str(&rest[..=open]);
                        rest = &rest[open + 1..];
                    }
                }
            }
            stripped.push_str(rest);
            match stripped.as_str() {
                "VARCHAR2" | "NVARCHAR2" | "CHAR" | "NCHAR" | "DATE" | "BLOB" | "BFILE"
                | "CLOB" | "NCLOB" | "LONG" | "BINARY_DOUBLE" | "BINARY_FLOAT" | "ROWID"
                | "VECTOR" | "TIMESTAMP" | "FLOAT" | "REAL" | "RAW" => stripped,
                "NUMBER" => "NUMBER".into(),
                "DOUBLE PRECISION" => "DOUBLE PRECISION".into(),
                "INTERVAL DAY TO SECOND" => "INTERVAL".into(),
                _ => "NULL".into(),
            }
        }
    }
}

fn type_rows(docs: &[IndexMap<String, KValue>]) -> Vec<ColumnTypeRow> {
    docs.iter()
        .filter_map(|d| {
            Some(ColumnTypeRow {
                database: kv_str(d.get("database")),
                schema: kv_str(d.get("schema")),
                table: kv_str(d.get("table")),
                column: kv_str(d.get("column"))?,
                type_: kv_str(d.get("type")).unwrap_or_default(),
                numeric_precision: kv_int(d.get("numeric_precision")),
                numeric_scale: kv_int(d.get("numeric_scale")),
            })
        })
        .collect()
}

impl SqlConnector {
    /// `get_table_cols_types(table, self, schema=schema)`: `{column: DB TYPE}`.
    pub async fn get_table_cols_types(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> IndexMap<String, String> {
        let query = get_table_cols_types_query(table, self.flavor);
        // Restrict the information_schema view before its privilege/type joins. Keep v4's
        // cross-schema fallback when the requested schema has no matching table.
        let scoped = if self.flavor.driver_family() == DriverFamily::Postgres {
            introspection_schema(self.flavor, schema)
                .map(|s| format!("{query} AND table_schema = '{}'", s.replace("'", "''")))
        } else {
            None
        };
        let df = match scoped {
            Some(q) => match self.read_catalog(&q).await {
                Ok(df) if df.height() > 0 => Ok(df),
                _ => self.read_catalog(&query).await,
            },
            None => self.read_catalog(&query).await,
        };
        let Ok(df) = df else {
            return IndexMap::new();
        };
        let rows = type_rows(&frame_docs(&df));
        let mut geometry = IndexMap::new();
        let schema_resolved = introspection_schema(self.flavor, schema);
        let filtered: Vec<ColumnTypeRow> = rows
            .iter()
            .filter(|r| {
                schema_resolved.is_none() || r.schema.as_deref() == schema_resolved.as_deref()
            })
            .cloned()
            .collect();
        let check = if filtered.is_empty() {
            &rows
        } else {
            &filtered
        };
        if self.flavor.driver_family() == DriverFamily::Postgres
            && needs_postgis_lookup(check)
            && let Ok(q) = get_postgis_geo_columns_types_query(table, schema_resolved.as_deref())
            && let Ok(gdf) = self.read(&q).await
        {
            let geo_rows: Vec<(String, String, String, Option<i64>)> = frame_docs(&gdf)
                .iter()
                .filter_map(|d| {
                    Some((
                        kv_str(d.get("column"))?,
                        kv_str(d.get("func"))?,
                        kv_str(d.get("type")).unwrap_or_default(),
                        kv_int(d.get("srid")),
                    ))
                })
                .collect();
            geometry = postgis_geo_columns_types_from_rows(&geo_rows);
        }
        // `database` is not passed by the pipe methods, so it does not filter.
        cols_types_from_rows(&rows, self.flavor, schema, None, &geometry)
    }

    /// `get_table_cols_indices(table, self, schema=schema)`: `{column: [{name, type}]}`.
    pub async fn get_table_cols_indices(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> IndexMap<String, Vec<IndexDoc>> {
        let ischema = introspection_schema(self.flavor, schema);
        let query = get_table_cols_indices_query(table, self.flavor, ischema.as_deref());
        let Ok(df) = self.read_catalog(&query).await else {
            return IndexMap::new();
        };
        let rows: Vec<ColumnIndexRow> = frame_docs(&df)
            .iter()
            .filter_map(|d| {
                Some(ColumnIndexRow {
                    database: kv_str(d.get("database")),
                    schema: kv_str(d.get("schema")),
                    table: kv_str(d.get("table")),
                    column: kv_str(d.get("column"))?,
                    index: kv_str(d.get("index")),
                    index_type: kv_str(d.get("index_type")),
                    clustered: d.get("clustered").and_then(|v| match v {
                        KValue::Bool(b) => Some(*b),
                        KValue::Int(i) => Some(*i != 0),
                        _ => None,
                    }),
                })
            })
            .collect();
        cols_indices_from_rows(&rows, self.flavor, schema, None)
    }

    /// The SQLAlchemy reflection path of `get_pipe_columns_types` (Oracle, MySQL, MariaDB,
    /// SQLite, GeoPackage): `{column: str(column.type)}` in ordinal order.
    pub async fn get_reflected_columns_types(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> IndexMap<String, String> {
        let esc = |s: &str| s.replace('\'', "''");
        let table_trunc = crate::ident::truncate_item_name(table, self.flavor);
        match self.flavor.driver_family() {
            DriverFamily::Sqlite => {
                let query = format!(
                    "SELECT name AS \"column\", type AS \"type\" FROM pragma_table_info('{}') ORDER BY cid",
                    esc(&table_trunc)
                );
                let Ok(df) = self.read(&query).await else {
                    return IndexMap::new();
                };
                frame_docs(&df)
                    .iter()
                    .filter_map(|d| {
                        Some((
                            kv_str(d.get("column"))?,
                            sqlite_reflected_type(&kv_str(d.get("type")).unwrap_or_default()),
                        ))
                    })
                    .collect()
            }
            DriverFamily::Mysql => {
                let schema_clause = match schema.filter(|s| !s.is_empty()) {
                    Some(s) => format!("'{}'", esc(s)),
                    None => "DATABASE()".to_string(),
                };
                let query = format!(
                    "SELECT COLUMN_NAME AS `column`, CAST(COLUMN_TYPE AS CHAR) AS `type`\nFROM information_schema.COLUMNS\nWHERE TABLE_SCHEMA = {schema_clause} AND TABLE_NAME = '{}'\nORDER BY ORDINAL_POSITION",
                    esc(&table_trunc)
                );
                let Ok(df) = self.read(&query).await else {
                    return IndexMap::new();
                };
                frame_docs(&df)
                    .iter()
                    .filter_map(|d| {
                        Some((
                            kv_str(d.get("column"))?,
                            mysql_reflected_type(&kv_str(d.get("type")).unwrap_or_default()),
                        ))
                    })
                    .collect()
            }
            DriverFamily::Oracle => self.get_oracle_reflected_columns_types(table, schema).await,
            _ => self.get_table_cols_types(table, schema).await,
        }
    }

    /// The Oracle branch of [`get_reflected_columns_types`](Self::get_reflected_columns_types):
    /// SQLAlchemy's `Table(name, autoload_with=engine)` reads `all_tab_cols` for the current schema
    /// (or the given one) and the denormalized table name, in column order.
    async fn get_oracle_reflected_columns_types(
        &self,
        table: &str,
        schema: Option<&str>,
    ) -> IndexMap<String, String> {
        let esc = |s: &str| s.replace('\'', "''");
        let table_trunc = crate::ident::truncate_item_name(table, self.flavor);
        let owner = match schema.filter(|s| !s.is_empty()) {
            Some(s) => format!("'{}'", esc(&oracle_denormalize_name(s))),
            None => "sys_context('USERENV', 'CURRENT_SCHEMA')".to_string(),
        };
        // The denormalized name first; the name as given when the table was created quoted.
        let mut names = vec![oracle_denormalize_name(&table_trunc)];
        if !names.contains(&table_trunc) {
            names.push(table_trunc.clone());
        }
        for name in names {
            let query = format!(
                "SELECT column_name AS \"column\", data_type AS \"type\", char_length AS \"char_length\", data_precision AS \"numeric_precision\", data_scale AS \"numeric_scale\"\nFROM all_tab_cols\nWHERE owner = {owner} AND table_name = '{}' AND hidden_column = 'NO'\nORDER BY column_id",
                esc(&name)
            );
            let Ok(df) = self.read(&query).await else {
                continue;
            };
            let out: IndexMap<String, String> = frame_docs(&df)
                .iter()
                .filter_map(|d| {
                    Some((
                        oracle_normalize_name(&kv_str(d.get("column"))?),
                        oracle_reflected_type(
                            &kv_str(d.get("type")).unwrap_or_default(),
                            kv_int(d.get("char_length")),
                            kv_int(d.get("numeric_precision")),
                            kv_int(d.get("numeric_scale")),
                        ),
                    ))
                })
                .collect();
            if !out.is_empty() {
                return out;
            }
        }
        IndexMap::new()
    }
}

/// Whether `get_pipe_columns_types` reflects through SQLAlchemy for this flavor.
pub fn uses_reflection(flavor: Flavor) -> bool {
    matches!(
        flavor,
        Flavor::Oracle | Flavor::Mysql | Flavor::Mariadb | Flavor::Sqlite | Flavor::Geopackage
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sqlite_types() {
        assert_eq!(
            sqlite_reflected_type("TEXT(10,2)"),
            "TEXT(10) COLLATE \"2\""
        );
        assert_eq!(sqlite_reflected_type("INT"), "INTEGER");
        assert_eq!(sqlite_reflected_type("TIMESTAMP"), "TIMESTAMP");
        assert_eq!(sqlite_reflected_type("DOUBLE PRECISION"), "REAL");
        assert_eq!(sqlite_reflected_type("NUMERIC(28,10)"), "NUMERIC(28, 10)");
        assert_eq!(sqlite_reflected_type("VARCHAR(256)"), "VARCHAR(256)");
        assert_eq!(sqlite_reflected_type(""), "NULL");
        assert_eq!(sqlite_reflected_type("FLOAT"), "FLOAT");
    }

    #[test]
    fn oracle_types() {
        let t = oracle_reflected_type;
        // INT / INTEGER / SMALLINT (precision NULL, scale 0) are INTEGER; other NUMBERs render bare.
        assert_eq!(t("NUMBER", None, None, Some(0)), "INTEGER");
        assert_eq!(t("NUMBER", None, None, None), "NUMBER");
        assert_eq!(t("NUMBER", None, Some(38), Some(0)), "NUMBER");
        assert_eq!(t("NUMBER", None, Some(10), Some(2)), "NUMBER");
        assert_eq!(t("FLOAT", None, Some(126), None), "DOUBLE PRECISION");
        assert_eq!(t("FLOAT", None, Some(63), None), "REAL");
        assert_eq!(t("FLOAT", None, Some(10), None), "FLOAT");
        assert_eq!(t("NVARCHAR2", Some(2000), None, None), "NVARCHAR(2000)");
        assert_eq!(t("VARCHAR2", Some(10), None, None), "VARCHAR(10)");
        assert_eq!(t("CHAR", Some(36), None, None), "CHAR(36)");
        assert_eq!(t("NCHAR", Some(5), None, None), "NCHAR(5)");
        assert_eq!(t("RAW", None, None, None), "RAW");
        assert_eq!(t("TIMESTAMP(6)", None, None, None), "TIMESTAMP");
        assert_eq!(
            t("TIMESTAMP(9) WITH TIME ZONE", None, None, None),
            "TIMESTAMP"
        );
        assert_eq!(
            t("TIMESTAMP(6) WITH LOCAL TIME ZONE", None, None, None),
            "TIMESTAMP"
        );
        assert_eq!(t("DATE", None, None, None), "DATE");
        assert_eq!(t("BLOB", None, None, None), "BLOB");
        assert_eq!(t("NCLOB", None, None, None), "NCLOB");
        assert_eq!(t("BINARY_DOUBLE", None, None, None), "BINARY_DOUBLE");
        assert_eq!(
            t("INTERVAL DAY(2) TO SECOND(6)", None, None, None),
            "INTERVAL"
        );
        assert_eq!(t("INTERVAL YEAR(2) TO MONTH", None, None, None), "NULL");
        assert_eq!(t("LONG RAW", None, None, None), "NULL");
    }

    #[test]
    fn oracle_names() {
        assert_eq!(oracle_normalize_name("DT"), "dt");
        assert_eq!(oracle_normalize_name("COL1"), "col1");
        assert_eq!(oracle_normalize_name("MyCol"), "MyCol");
        assert_eq!(oracle_normalize_name("DATE"), "DATE");
        assert_eq!(oracle_denormalize_name("w5sql_table"), "W5SQL_TABLE");
        assert_eq!(oracle_denormalize_name("Mixed"), "Mixed");
        assert_eq!(oracle_denormalize_name("date"), "date");
    }

    #[test]
    fn mysql_types() {
        assert_eq!(mysql_reflected_type("decimal(38,20)"), "DECIMAL(38, 20)");
        assert_eq!(mysql_reflected_type("char(36)"), "CHAR(36)");
        assert_eq!(mysql_reflected_type("int(11)"), "INTEGER");
        assert_eq!(mysql_reflected_type("bigint(20)"), "BIGINT");
        assert_eq!(mysql_reflected_type("tinyint(1)"), "TINYINT");
        assert_eq!(mysql_reflected_type("datetime(6)"), "DATETIME");
        assert_eq!(mysql_reflected_type("double"), "DOUBLE");
    }
}
