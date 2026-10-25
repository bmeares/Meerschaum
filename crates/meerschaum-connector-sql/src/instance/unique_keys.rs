//! Unique keys on the instance tables (new in 5.0): a pipe's keys in `mrsm_pipes` and a
//! username in `mrsm_users`.
//!
//! v4 creates these tables without them, so two processes registering the same pipe or user at
//! once can both pass the existence check (`get_pipe_id`, `get_user_id`) and insert two rows.
//! Every instance gets the unique indexes of [`UNIQUE_KEYS`] the first time a connector ensures
//! the instance tables (registering, syncing, editing or looking up a pipe or user; listing pipes
//! does not), whether the tables were created by v4 or by 5.0. The `CREATE TABLE` text stays
//! v4's (see `tables.rs`), and v4 can still read and write tables that have the indexes.
//!
//! - A nullable key column ([`UniqueKey::nullable`], `location_key`) is `NULL` for most rows, and
//!   most databases treat `NULL`s as distinct in a unique index, so the PostgreSQL family, SQLite
//!   and DuckDB index `COALESCE(location_key, '[None]')` (`[None]` is how a missing location key
//!   is spelled on the command line, so no pipe has it as its location key). MSSQL and Oracle
//!   compare `NULL`s as equal in a composite unique index, so they index the columns. MySQL
//!   8.0.13 and later index the expression; MySQL 5.7 and MariaDB have no expression indexes and
//!   index the columns, which covers pipes with a location key only.
//! - An existing table with duplicate rows is never changed: no row is deleted, the index is not
//!   added, and `mrsm doctor` names the duplicates. The index is added by a later connector once
//!   the extra rows are gone.
//! - The check is one catalog read per key, connector and process; an index is built only when
//!   it is missing (no lock is taken on an instance that already has it).

use std::collections::HashMap;

use meerschaum_connector::ConnectorResult;

use crate::driver::SqlParam;
use crate::driver::params::placeholder;
use crate::flavor::{DriverFamily, Flavor};

use super::{SqlConnector, be, frame_docs, kv_int, kv_str};

/// What a missing value of a nullable key column is indexed as.
const NULL_VALUE: &str = "[None]";

/// A unique key on an instance table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UniqueKey {
    /// The instance table.
    pub table: &'static str,
    /// The name of its unique index.
    pub index: &'static str,
    /// The table's integer id column (names the duplicate rows).
    pub id_column: &'static str,
    /// The `NOT NULL` key columns.
    pub columns: &'static [&'static str],
    /// A nullable key column whose `NULL` counts as one value.
    pub nullable: Option<&'static str>,
}

/// A pipe's keys in `mrsm_pipes`.
pub const PIPES_KEY: UniqueKey = UniqueKey {
    table: "mrsm_pipes",
    index: "uq_mrsm_pipes_keys",
    id_column: "pipe_id",
    columns: &["connector_keys", "metric_key"],
    nullable: Some("location_key"),
};

/// A username in `mrsm_users`.
pub const USERS_KEY: UniqueKey = UniqueKey {
    table: "mrsm_users",
    index: "uq_mrsm_users_username",
    id_column: "user_id",
    columns: &["username"],
    nullable: None,
};

/// Every unique key 5.0 adds to the instance tables.
pub const UNIQUE_KEYS: [UniqueKey; 2] = [PIPES_KEY, USERS_KEY];

/// A key value held by more than one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Duplicate {
    /// The key's values, in the order of [`UniqueKey::key_columns`] (`None`: `NULL`).
    pub values: Vec<Option<String>>,
    /// The id of every row holding it, in ascending order.
    pub ids: Vec<i64>,
}

/// Whether an instance table has its unique key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyStatus {
    /// The index exists.
    Present,
    /// The table does not exist yet (it gets the index when it is created).
    NoTable,
    /// The index is missing. `duplicates` lists the key values held by more than one row, which
    /// must be removed before the index can be added (empty: the next connector adds it).
    Missing {
        /// The key values held by more than one row.
        duplicates: Vec<Duplicate>,
    },
}

impl UniqueKey {
    /// The key columns, the nullable one last.
    pub fn key_columns(&self) -> Vec<&'static str> {
        self.columns.iter().copied().chain(self.nullable).collect()
    }

    /// The `CREATE UNIQUE INDEX` statements to try, in order, for `table` (the quoted table
    /// name). With a nullable column, MySQL tries the expression index first, which MySQL 5.7
    /// rejects.
    pub fn ddl(&self, flavor: Flavor, table: &str) -> Vec<String> {
        let index = self.index;
        let create = |cols: &[String]| {
            format!(
                "CREATE UNIQUE INDEX {index} ON {table} ({})",
                cols.join(", ")
            )
        };
        let mut cols: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        let Some(nullable) = self.nullable else {
            // Oracle refuses a second index on the column list of v4's `ix_<table>_<column>`
            // (ORA-01408): there the key is a constraint that the existing index enforces.
            if flavor == Flavor::Oracle && cols.len() == 1 {
                return vec![
                    create(&cols),
                    format!(
                        "ALTER TABLE {table} ADD CONSTRAINT {index} UNIQUE ({}) USING INDEX ix_{}_{}",
                        cols[0], self.table, cols[0]
                    ),
                ];
            }
            return vec![create(&cols)];
        };
        let coalesce = format!("COALESCE({nullable}, '{NULL_VALUE}')");
        let plain = {
            let mut c = cols.clone();
            c.push(nullable.to_string());
            create(&c)
        };
        match (flavor.driver_family(), flavor) {
            (DriverFamily::Postgres | DriverFamily::Sqlite | DriverFamily::Duckdb, _) => {
                cols.push(format!("({coalesce})"));
                vec![create(&cols)]
            }
            // A functional key part needs its own parentheses.
            (_, Flavor::Mysql) => {
                cols.push(format!("(({coalesce}))"));
                vec![create(&cols), plain]
            }
            _ => vec![plain],
        }
    }

    /// The catalog query counting the index on the table, with its parameters. `schema` is the
    /// instance schema; `table` the quoted table name (MSSQL resolves it).
    fn exists_query(
        &self,
        flavor: Flavor,
        schema: Option<&str>,
        table: &str,
    ) -> (String, Vec<SqlParam>) {
        let p = |n| placeholder(flavor, n);
        let name = SqlParam::Text(self.index.to_string());
        let raw_table = self.table;
        match (flavor.driver_family(), flavor) {
            (DriverFamily::Postgres, _) => (
                format!(
                    "SELECT COUNT(*) FROM pg_indexes WHERE indexname = {} AND tablename = '{raw_table}' AND schemaname = COALESCE({}, current_schema())",
                    p(1),
                    p(2)
                ),
                vec![name, SqlParam::opt_text(schema)],
            ),
            (DriverFamily::Duckdb, _) => (
                format!(
                    "SELECT COUNT(*) FROM duckdb_indexes() WHERE index_name = {} AND table_name = '{raw_table}' AND schema_name = COALESCE({}, current_schema())",
                    p(1),
                    p(2)
                ),
                vec![name, SqlParam::opt_text(schema)],
            ),
            (DriverFamily::Sqlite, _) => (
                format!(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = {} AND tbl_name = '{raw_table}'",
                    p(1)
                ),
                vec![name],
            ),
            (DriverFamily::Mysql, _) => (
                format!(
                    "SELECT COUNT(*) FROM information_schema.statistics WHERE index_name = {} AND table_name = '{raw_table}' AND table_schema = DATABASE()",
                    p(1)
                ),
                vec![name],
            ),
            (_, Flavor::Mssql) => (
                format!(
                    "SELECT COUNT(*) FROM sys.indexes WHERE name = {} AND object_id = OBJECT_ID({})",
                    p(1),
                    p(2)
                ),
                vec![name, SqlParam::Text(table.to_string())],
            ),
            _ => (
                // Oracle stores unquoted names in upper case; the key is an index or a
                // constraint (see `ddl`).
                format!(
                    "SELECT (SELECT COUNT(*) FROM all_indexes WHERE index_name = UPPER({}) AND table_name = '{upper}' AND table_owner = COALESCE(UPPER({}), USER)) + (SELECT COUNT(*) FROM all_constraints WHERE constraint_name = UPPER({}) AND table_name = '{upper}' AND owner = COALESCE(UPPER({}), USER)) FROM dual",
                    p(1),
                    p(2),
                    p(3),
                    p(4),
                    upper = raw_table.to_uppercase(),
                ),
                vec![
                    name.clone(),
                    SqlParam::opt_text(schema),
                    name,
                    SqlParam::opt_text(schema),
                ],
            ),
        }
    }
}

/// Group `(id, key values)` rows (ordered by id) into the key values held by more than one row,
/// in the order of their first row. `NULL`s compare equal.
pub fn find_duplicates(rows: &[(i64, Vec<Option<String>>)]) -> Vec<Duplicate> {
    let mut order: Vec<Vec<Option<String>>> = Vec::new();
    let mut ids: HashMap<Vec<Option<String>>, Vec<i64>> = HashMap::new();
    for (id, values) in rows {
        let entry = ids.entry(values.clone()).or_default();
        if entry.is_empty() {
            order.push(values.clone());
        }
        entry.push(*id);
    }
    order
        .into_iter()
        .filter_map(|values| {
            let ids = ids.remove(&values)?;
            (ids.len() > 1).then_some(Duplicate { values, ids })
        })
        .collect()
}

impl SqlConnector {
    fn unique_key_schema(&self) -> Option<&str> {
        if self.flavor.no_schema() {
            None
        } else {
            self.instance_schema()
        }
    }

    /// Whether the table of `key` has its unique index.
    pub async fn unique_key_exists(&self, key: &UniqueKey) -> ConnectorResult<bool> {
        let table = self.instance_table_name(key.table);
        let (sql, params) = key.exists_query(self.flavor, self.unique_key_schema(), &table);
        let count = self.value_params_catalog(&sql, &params).await?;
        Ok(count.and_then(|v| v.as_i64()).unwrap_or(0) > 0)
    }

    /// The values of `key` held by more than one row of its table.
    pub async fn unique_key_duplicates(&self, key: &UniqueKey) -> ConnectorResult<Vec<Duplicate>> {
        let table = self.instance_table_name(key.table);
        let columns = key.key_columns();
        let sql = format!(
            "SELECT {}, {} FROM {table} ORDER BY {}",
            key.id_column,
            columns.join(", "),
            key.id_column
        );
        let df = self.read(&sql).await.map_err(be)?;
        let rows: Vec<(i64, Vec<Option<String>>)> = frame_docs(&df)
            .iter()
            .filter_map(|doc| {
                let id = kv_int(doc.get(key.id_column))?;
                let values = columns.iter().map(|c| kv_str(doc.get(*c))).collect();
                Some((id, values))
            })
            .collect();
        Ok(find_duplicates(&rows))
    }

    /// Whether the table of `key` has its unique index (read-only; `mrsm doctor`).
    pub async fn unique_key_status(&self, key: &UniqueKey) -> ConnectorResult<KeyStatus> {
        if !self.table_exists(key.table, self.instance_schema()).await {
            return Ok(KeyStatus::NoTable);
        }
        if self.unique_key_exists(key).await? {
            return Ok(KeyStatus::Present);
        }
        Ok(KeyStatus::Missing {
            duplicates: self.unique_key_duplicates(key).await?,
        })
    }

    /// Add the index of `key` when it is missing and no value is held twice. Never fails the
    /// caller: what went wrong is logged, and the next connector tries again.
    pub async fn ensure_unique_key(&self, key: &UniqueKey) {
        let what = format!("{} on {self}", key.table);
        match self.unique_key_exists(key).await {
            Ok(true) => return,
            Ok(false) => {}
            Err(e) => {
                tracing::warn!("Could not look for the unique key of {what}: {e}");
                return;
            }
        }
        match self.unique_key_duplicates(key).await {
            Ok(duplicates) if !duplicates.is_empty() => {
                tracing::warn!(
                    "Not adding a unique key to {what}: {} value(s) are held by more than one row. Run `mrsm doctor` for the list.",
                    duplicates.len()
                );
                return;
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("Could not read {what}: {e}");
                return;
            }
        }
        let table = self.instance_table_name(key.table);
        let mut last_error = String::new();
        for ddl in key.ddl(self.flavor, &table) {
            self.log(&ddl);
            match self.driver.exec(&ddl).await {
                Ok(_) => return,
                Err(e) => last_error = e.to_string(),
            }
        }
        // Another process may have added it first.
        if !matches!(self.unique_key_exists(key).await, Ok(true)) {
            tracing::warn!("Could not add a unique key to {what}: {last_error}");
        }
    }

    /// [`ensure_unique_key`](SqlConnector::ensure_unique_key) for every key of [`UNIQUE_KEYS`].
    pub async fn ensure_unique_keys(&self) {
        for key in &UNIQUE_KEYS {
            self.ensure_unique_key(key).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(id: i64, values: &[Option<&str>]) -> (i64, Vec<Option<String>>) {
        (id, values.iter().map(|v| v.map(str::to_string)).collect())
    }

    #[test]
    fn duplicates_group_a_null_as_one_value() {
        let rows = [
            row(1, &[Some("sql:a"), Some("m"), None]),
            row(2, &[Some("sql:a"), Some("m"), Some("x")]),
            row(3, &[Some("sql:b"), Some("m"), None]),
            row(4, &[Some("sql:a"), Some("m"), None]),
            row(5, &[Some("sql:a"), Some("m"), Some("x")]),
            row(6, &[Some("sql:a"), Some("m"), None]),
        ];
        let dups = find_duplicates(&rows);
        assert_eq!(dups.len(), 2);
        assert_eq!(dups[0].values[2], None);
        assert_eq!(dups[0].ids, vec![1, 4, 6]);
        assert_eq!(dups[1].values[2].as_deref(), Some("x"));
        assert_eq!(dups[1].ids, vec![2, 5]);
        assert!(find_duplicates(&rows[..3]).is_empty());
    }

    #[test]
    fn the_pipes_key_covers_a_missing_location_where_the_database_can() {
        let ddl = |f| PIPES_KEY.ddl(f, "mrsm_pipes");
        for f in [
            Flavor::Postgresql,
            Flavor::Timescaledb,
            Flavor::Cockroachdb,
            Flavor::Sqlite,
            Flavor::Geopackage,
            Flavor::Duckdb,
        ] {
            assert_eq!(
                ddl(f),
                vec![
                    "CREATE UNIQUE INDEX uq_mrsm_pipes_keys ON mrsm_pipes (connector_keys, metric_key, (COALESCE(location_key, '[None]')))"
                ],
                "{f:?}"
            );
        }
        let mysql = ddl(Flavor::Mysql);
        assert_eq!(mysql.len(), 2);
        assert!(mysql[0].ends_with("((COALESCE(location_key, '[None]'))))"));
        for f in [Flavor::Mariadb, Flavor::Mssql, Flavor::Oracle] {
            assert_eq!(
                ddl(f),
                vec![
                    "CREATE UNIQUE INDEX uq_mrsm_pipes_keys ON mrsm_pipes (connector_keys, metric_key, location_key)"
                ],
                "{f:?}"
            );
        }
    }

    #[test]
    fn the_users_key_is_the_username_everywhere() {
        let oracle = USERS_KEY.ddl(Flavor::Oracle, "mrsm_users");
        assert_eq!(
            oracle[1],
            "ALTER TABLE mrsm_users ADD CONSTRAINT uq_mrsm_users_username UNIQUE (username) USING INDEX ix_mrsm_users_username"
        );
        for f in [
            Flavor::Postgresql,
            Flavor::Sqlite,
            Flavor::Duckdb,
            Flavor::Mysql,
            Flavor::Mariadb,
            Flavor::Mssql,
        ] {
            assert_eq!(
                USERS_KEY.ddl(f, "mrsm_users"),
                vec!["CREATE UNIQUE INDEX uq_mrsm_users_username ON mrsm_users (username)"],
                "{f:?}"
            );
        }
    }
}
