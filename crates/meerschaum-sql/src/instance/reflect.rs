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
            if name == "BLOB" {
                if let Some(len) = args.first().filter(|l| **l != 0) {
                    return format!("BLOB({len})");
                }
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
            Some(q) => match self.read(&q).await {
                Ok(df) if df.height() > 0 => Ok(df),
                _ => self.read(&query).await,
            },
            None => self.read(&query).await,
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
        if self.flavor.driver_family() == DriverFamily::Postgres && needs_postgis_lookup(check) {
            if let Ok(q) = get_postgis_geo_columns_types_query(table, schema_resolved.as_deref()) {
                if let Ok(gdf) = self.read(&q).await {
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
            }
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
        let Ok(df) = self.read(&query).await else {
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
            _ => self.get_table_cols_types(table, schema).await,
        }
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
