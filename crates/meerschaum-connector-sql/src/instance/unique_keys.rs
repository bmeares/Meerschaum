//! Unique keys on the instance tables (new in 5.0): a pipe's keys in `mrsm_pipes`, a username in
//! `mrsm_users` and a plugin name in `mrsm_plugins`.
//!
//! v4 creates these tables without them, so two processes registering the same pipe, user or
//! plugin at once can both pass the existence check (`get_pipe_id`, `get_user_id`,
//! `get_plugin_id`) and insert two rows.
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

/// A plugin name in `mrsm_plugins`.
pub const PLUGINS_KEY: UniqueKey = UniqueKey {
    table: "mrsm_plugins",
    index: "uq_mrsm_plugins_plugin_name",
    id_column: "plugin_id",
    columns: &["plugin_name"],
    nullable: None,
};

/// Every unique key 5.0 adds to the instance tables.
pub const UNIQUE_KEYS: [UniqueKey; 3] = [PIPES_KEY, USERS_KEY, PLUGINS_KEY];

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

impl UniqueKey {
    /// The query listing the rows whose key value is held by more than one row, compared by the
    /// database (its collation decides, as it does for the index): `dup_first` (the lowest id of
    /// the group), `dup_id` (the row) and the key columns (the group's values; a missing value
    /// of the nullable column reads as `'[None]'`), ordered by group and id. `table` is the
    /// quoted table name.
    pub fn duplicates_query(&self, table: &str) -> String {
        let id = self.id_column;
        let null_key = |col: &str, alias: &str| format!("COALESCE({alias}{col}, '{NULL_VALUE}')");
        let mut group_by: Vec<String> = self.columns.iter().map(|c| c.to_string()).collect();
        let mut select: Vec<String> = group_by.clone();
        let mut join: Vec<String> = self
            .columns
            .iter()
            .map(|c| format!("t.{c} = g.{c}"))
            .collect();
        if let Some(n) = self.nullable {
            group_by.push(null_key(n, ""));
            select.push(format!("{} AS {n}", null_key(n, "")));
            join.push(format!("{} = g.{n}", null_key(n, "t.")));
        }
        let values: Vec<String> = self
            .key_columns()
            .iter()
            .map(|c| format!("g.{c}"))
            .collect();
        format!(
            "SELECT g.dup_first, t.{id} AS dup_id, {values} FROM {table} t INNER JOIN (SELECT {select}, MIN({id}) AS dup_first FROM {table} GROUP BY {group_by} HAVING COUNT(*) > 1) g ON {join} ORDER BY g.dup_first, t.{id}",
            values = values.join(", "),
            select = select.join(", "),
            group_by = group_by.join(", "),
            join = join.join(" AND "),
        )
    }
}

/// Group the rows of [`UniqueKey::duplicates_query`] (`(group, id, values)`, ordered by group
/// and id) into duplicates. The nullable column's [`NULL_VALUE`] reads as `None`.
fn group_duplicates(key: &UniqueKey, rows: Vec<(i64, i64, Vec<Option<String>>)>) -> Vec<Duplicate> {
    let nullable_at = key.nullable.map(|_| key.columns.len());
    let mut out: Vec<(i64, Duplicate)> = Vec::new();
    for (group, id, mut values) in rows {
        match out.last_mut() {
            Some((g, dup)) if *g == group => dup.ids.push(id),
            _ => {
                if let Some(i) = nullable_at
                    && values.get(i).and_then(|v| v.as_deref()) == Some(NULL_VALUE)
                {
                    values[i] = None;
                }
                out.push((
                    group,
                    Duplicate {
                        values,
                        ids: vec![id],
                    },
                ));
            }
        }
    }
    out.into_iter()
        .map(|(_, dup)| dup)
        .filter(|dup| dup.ids.len() > 1)
        .collect()
}

/// Whether a failed `CREATE UNIQUE INDEX` from [`UniqueKey::ddl`] may fall back to the next
/// statement: only when the database cannot build that form of the key, never because of the
/// table's rows (a duplicate entry), a lock or a lost connection. `number` is the MySQL error
/// number when the driver reports one; `message` the error text.
///
/// - MySQL before 8.0.13 (and a MariaDB server behind the `mysql` flavor) refuses the functional
///   key part as a syntax error (1064, `ER_PARSE_ERROR`); MySQL 8 refuses some expressions with
///   a functional index error (3753 to 3762, `ER_FUNCTIONAL_INDEX_*`).
/// - Oracle refuses a second index on the columns of v4's index (ORA-01408).
pub fn ddl_fallback_allowed(flavor: Flavor, number: Option<u16>, message: &str) -> bool {
    match flavor.driver_family() {
        DriverFamily::Mysql => {
            let number = number.or_else(|| {
                // "1064 (42000): You have an error ..." when only the text is left.
                let digits: String = message
                    .trim_start()
                    .chars()
                    .take_while(char::is_ascii_digit)
                    .collect();
                digits.parse().ok()
            });
            match number {
                Some(n) => n == 1064 || (3753..=3762).contains(&n),
                None => {
                    let lower = message.to_lowercase();
                    lower.contains("error in your sql syntax") || lower.contains("functional index")
                }
            }
        }
        _ if flavor == Flavor::Oracle => message.contains("ORA-01408"),
        _ => false,
    }
}

/// The MySQL error number of a driver error, if it is one.
fn mysql_error_number(error: &crate::error::SqlError) -> Option<u16> {
    match error {
        crate::error::SqlError::Driver(sqlx::Error::Database(db)) => db
            .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
            .map(|e| e.number()),
        _ => None,
    }
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

    /// The values of `key` held by more than one row of its table, as the database compares
    /// them (a case-insensitive collation makes `Alice` and `alice` one value, as it does for the
    /// index).
    pub async fn unique_key_duplicates(&self, key: &UniqueKey) -> ConnectorResult<Vec<Duplicate>> {
        let table = self.instance_table_name(key.table);
        let columns = key.key_columns();
        let df = self.read(&key.duplicates_query(&table)).await.map_err(be)?;
        let rows = frame_docs(&df)
            .iter()
            .filter_map(|doc| {
                let group = kv_int(doc.get("dup_first"))?;
                let id = kv_int(doc.get("dup_id"))?;
                let values = columns.iter().map(|c| kv_str(doc.get(*c))).collect();
                Some((group, id, values))
            })
            .collect();
        Ok(group_duplicates(key, rows))
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
                Err(e) => {
                    last_error = e.to_string();
                    // A weaker form of the key must not stand in for one refused over the rows
                    // (e.g. a duplicate inserted after the check above).
                    if !ddl_fallback_allowed(self.flavor, mysql_error_number(&e), &last_error) {
                        break;
                    }
                }
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

    fn row(group: i64, id: i64, values: &[Option<&str>]) -> (i64, i64, Vec<Option<String>>) {
        (
            group,
            id,
            values.iter().map(|v| v.map(str::to_string)).collect(),
        )
    }

    #[test]
    fn duplicates_group_by_the_lowest_id_and_read_a_missing_location_as_null() {
        let rows = vec![
            row(1, 1, &[Some("sql:a"), Some("m"), Some("[None]")]),
            row(1, 4, &[Some("sql:a"), Some("m"), Some("[None]")]),
            row(1, 6, &[Some("sql:a"), Some("m"), Some("[None]")]),
            row(2, 2, &[Some("sql:A"), Some("m"), Some("x")]),
            row(2, 5, &[Some("sql:A"), Some("m"), Some("x")]),
            row(3, 3, &[Some("sql:b"), Some("m"), Some("y")]),
        ];
        let dups = group_duplicates(&PIPES_KEY, rows);
        assert_eq!(dups.len(), 2);
        assert_eq!(
            dups[0].values,
            vec![Some("sql:a".into()), Some("m".into()), None]
        );
        assert_eq!(dups[0].ids, vec![1, 4, 6]);
        assert_eq!(dups[1].values[2].as_deref(), Some("x"));
        assert_eq!(dups[1].ids, vec![2, 5]);
        // Only the nullable column's spelling of a missing value reads as `None`.
        let users = group_duplicates(
            &USERS_KEY,
            vec![row(7, 7, &[Some("[None]")]), row(7, 9, &[Some("[None]")])],
        );
        assert_eq!(users[0].values, vec![Some("[None]".into())]);
    }

    #[test]
    fn duplicates_are_compared_by_the_database() {
        assert_eq!(
            PIPES_KEY.duplicates_query("mrsm_pipes"),
            "SELECT g.dup_first, t.pipe_id AS dup_id, g.connector_keys, g.metric_key, g.location_key FROM mrsm_pipes t INNER JOIN (SELECT connector_keys, metric_key, COALESCE(location_key, '[None]') AS location_key, MIN(pipe_id) AS dup_first FROM mrsm_pipes GROUP BY connector_keys, metric_key, COALESCE(location_key, '[None]') HAVING COUNT(*) > 1) g ON t.connector_keys = g.connector_keys AND t.metric_key = g.metric_key AND COALESCE(t.location_key, '[None]') = g.location_key ORDER BY g.dup_first, t.pipe_id"
        );
    }

    #[test]
    fn only_an_unsupported_key_form_falls_back() {
        let mysql = |n, m| ddl_fallback_allowed(Flavor::Mysql, n, m);
        // MySQL 5.7 / MariaDB: no functional key parts.
        assert!(mysql(Some(1064), "You have an error in your SQL syntax"));
        assert!(mysql(
            None,
            "1064 (42000): You have an error in your SQL syntax; check the manual"
        ));
        assert!(mysql(None, "You have an error in your SQL syntax"));
        assert!(mysql(
            Some(3757),
            "Cannot create a functional index on an expression"
        ));
        // A duplicate inserted after the check, a lock wait, a lost connection: stop.
        assert!(!mysql(
            Some(1062),
            "Duplicate entry 'sql:a-m-[None]' for key 'uq_mrsm_pipes_keys'"
        ));
        assert!(!mysql(
            None,
            "1062 (23000): Duplicate entry 'sql:a-m-[None]' for key"
        ));
        assert!(!mysql(Some(1205), "Lock wait timeout exceeded"));
        assert!(!mysql(
            None,
            "error communicating with database: Connection reset by peer"
        ));
        assert!(!ddl_fallback_allowed(
            Flavor::Mariadb,
            Some(1062),
            "Duplicate entry"
        ));
        // Oracle: v4's index already covers the column.
        assert!(ddl_fallback_allowed(
            Flavor::Oracle,
            None,
            "ORA-01408: such column list already indexed"
        ));
        assert!(!ddl_fallback_allowed(
            Flavor::Oracle,
            None,
            "ORA-01452: cannot CREATE UNIQUE INDEX; duplicate keys found"
        ));
        assert!(!ddl_fallback_allowed(
            Flavor::Postgresql,
            None,
            "syntax error"
        ));
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
    fn the_plugins_key_is_the_plugin_name() {
        assert_eq!(
            PLUGINS_KEY.ddl(Flavor::Postgresql, "mrsm_plugins"),
            vec!["CREATE UNIQUE INDEX uq_mrsm_plugins_plugin_name ON mrsm_plugins (plugin_name)"]
        );
        assert_eq!(
            PLUGINS_KEY.ddl(Flavor::Oracle, "mrsm_plugins")[1],
            "ALTER TABLE mrsm_plugins ADD CONSTRAINT uq_mrsm_plugins_plugin_name UNIQUE (plugin_name) USING INDEX ix_mrsm_plugins_plugin_name"
        );
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
