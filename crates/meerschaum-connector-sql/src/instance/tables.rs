//! The instance tables (`meerschaum/connectors/sql/tables/__init__.py`) and the temporary-table
//! bookkeeping of `_instance.py`.
//!
//! v4 builds these tables with SQLAlchemy's `MetaData.create_all`; the DDL below is the exact
//! text SQLAlchemy compiles for each dialect (see `tests/fixtures/instance_tables.json`), so a
//! Rust-created instance is indistinguishable from a v4 one and a v4 instance is read as is.

use chrono::{NaiveDateTime, Utc};

use crate::driver::SqlParam;
use crate::driver::params::placeholder;
use crate::flavor::{DriverFamily, Flavor};
use crate::ident::sql_item_name;

use super::SqlConnector;

/// The instance tables in `MetaData.sorted_tables` order.
pub const INSTANCE_TABLES: [&str; 4] = [
    "mrsm_pipes",
    "mrsm_temp_tables",
    "mrsm_users",
    "mrsm_plugins",
];

/// SQLAlchemy's identifier quoting (`IdentifierPreparer.quote`): quote names that are not
/// lower-case legal identifiers or are reserved words.
fn sa_quote(name: &str, flavor: Flavor) -> String {
    let legal = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_lowercase() || c == '_')
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '$');
    let reserved = matches!(
        name,
        "table" | "user" | "order" | "group" | "select" | "where" | "from" | "index" | "key"
    );
    if legal && !reserved {
        return name.to_string();
    }
    match flavor.driver_family() {
        DriverFamily::Mysql => format!("`{}`", name.replace('`', "``")),
        DriverFamily::Mssql => format!("[{}]", name.replace(']', "]]")),
        _ => format!("\"{}\"", name.replace('"', "\"\"")),
    }
}

fn sa_table(table: &str, schema: Option<&str>, flavor: Flavor) -> String {
    match schema {
        Some(s) => format!("{}.{}", sa_quote(s, flavor), sa_quote(table, flavor)),
        None => sa_quote(table, flavor),
    }
}

/// `ix_<schema>_<table>_<column>` (SQLAlchemy's default `ix_%(column_0_label)s`).
fn sa_index_name(table: &str, column: &str, schema: Option<&str>) -> String {
    match schema {
        Some(s) => format!("ix_{s}_{table}_{column}"),
        None => format!("ix_{table}_{column}"),
    }
}

/// The `CREATE TABLE` / `CREATE INDEX` statements `get_tables(create=True)` emits for one
/// instance table, in order. `schema` is the instance schema (`self.schema`).
///
/// DuckDB and Oracle key the ids on sequences (`_sequence_flavors`): the `CREATE SEQUENCE` that
/// `create_all` issues (with `checkfirst`) precedes the table, made idempotent
/// (`IF NOT EXISTS` on DuckDB, an `ORA-00955` guard on Oracle).
pub fn instance_table_ddl(table: &str, flavor: Flavor, schema: Option<&str>) -> Vec<String> {
    let schema = if flavor.no_schema() { None } else { schema };
    let family = flavor.driver_family();
    let json = flavor.is_json_flavor();
    let params_type = if json {
        "JSONB"
    } else {
        match flavor {
            Flavor::Mssql => "VARCHAR(max)",
            Flavor::Oracle => "CLOB",
            _ => "TEXT",
        }
    };
    let id_col = |name: &str| -> String {
        match (family, flavor) {
            (DriverFamily::Postgres, _) => format!("{name} SERIAL NOT NULL"),
            (DriverFamily::Mysql, _) => format!("{name} INTEGER NOT NULL AUTO_INCREMENT"),
            (_, Flavor::Mssql) => format!("{name} INTEGER NOT NULL IDENTITY"),
            (_, Flavor::Duckdb) => {
                format!("{name} INTEGER DEFAULT nextval('{name}_seq') NOT NULL")
            }
            (_, Flavor::Oracle) => format!("{name} INTEGER DEFAULT {name}_seq.nextval NOT NULL"),
            _ => format!("{name} INTEGER NOT NULL"),
        }
    };
    let sequence = |name: &str| -> Option<String> {
        match flavor {
            Flavor::Duckdb => Some(format!("CREATE SEQUENCE IF NOT EXISTS {name}_seq")),
            Flavor::Oracle => Some(format!(
                "BEGIN EXECUTE IMMEDIATE 'CREATE SEQUENCE {name}_seq'; EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;"
            )),
            _ => None,
        }
    };
    let varchar = |n: u32| -> String {
        if flavor == Flavor::Oracle {
            format!("VARCHAR2({n} CHAR)")
        } else {
            format!("VARCHAR({n})")
        }
    };
    let dt_type = match (family, flavor) {
        (DriverFamily::Postgres, _) | (_, Flavor::Duckdb) => "TIMESTAMP WITHOUT TIME ZONE",
        (_, Flavor::Oracle) => "DATE",
        _ => "DATETIME",
    };
    // MSSQL spells out nullable columns.
    let null = if flavor == Flavor::Mssql { " NULL" } else { "" };
    let t = |name: &str| sa_table(name, schema, flavor);
    let q = |name: &str| sa_quote(name, flavor);
    // MSSQL skips index names (`_skip_index_names_flavors`).
    let index_names = flavor != Flavor::Mssql;
    let ix = |col: &str| {
        format!(
            "CREATE INDEX {} ON {} ({})",
            sa_index_name(table, col, schema),
            t(table),
            q(col)
        )
    };
    let v256 = varchar(256);
    match table {
        "mrsm_pipes" => {
            let mut out: Vec<String> = sequence("pipe_id").into_iter().collect();
            out.push(format!(
                "\nCREATE TABLE {} (\n\t{}, \n\tconnector_keys {v256} NOT NULL, \n\tmetric_key {v256} NOT NULL, \n\tlocation_key {v256}{null}, \n\tparameters {params_type}{null}, \n\tPRIMARY KEY (pipe_id)\n)\n\n",
                t("mrsm_pipes"),
                id_col("pipe_id"),
            ));
            if json {
                out.push(format!(
                    "CREATE INDEX ix_mrsm_pipes_parameters_tags ON {} USING gin ((parameters['tags']))",
                    t("mrsm_pipes")
                ));
            }
            if index_names {
                out.push(ix("connector_keys"));
                out.push(ix("location_key"));
                out.push(ix("metric_key"));
            }
            out
        }
        "mrsm_temp_tables" => {
            let mut out = vec![format!(
                "\nCREATE TABLE {} (\n\tdate_created {dt_type} NOT NULL, \n\t{} {v256} NOT NULL, \n\tready_to_drop {dt_type}{null}\n)\n\n",
                t("mrsm_temp_tables"),
                q("table")
            )];
            out.push(ix("date_created"));
            if index_names {
                out.push(ix("table"));
            }
            out
        }
        "mrsm_users" => {
            let mut out: Vec<String> = sequence("user_id").into_iter().collect();
            out.push(format!(
                "\nCREATE TABLE {} (\n\t{}, \n\tusername {v256} NOT NULL, \n\tpassword_hash {}{null}, \n\temail {v256}{null}, \n\tuser_type {v256}{null}, \n\tattributes {params_type}{null}, \n\tPRIMARY KEY (user_id)\n)\n\n",
                t("mrsm_users"),
                id_col("user_id"),
                varchar(1024),
            ));
            if index_names {
                out.push(ix("username"));
            }
            out
        }
        "mrsm_plugins" => {
            let fk = if flavor == Flavor::Duckdb {
                String::new()
            } else {
                format!(
                    ", \n\tFOREIGN KEY(user_id) REFERENCES {} (user_id)",
                    t("mrsm_users")
                )
            };
            let mut out: Vec<String> = sequence("plugin_id").into_iter().collect();
            out.push(format!(
                "\nCREATE TABLE {} (\n\t{}, \n\tplugin_name {v256} NOT NULL, \n\tuser_id INTEGER NOT NULL, \n\tversion {v256}{null}, \n\tattributes {params_type}{null}, \n\tPRIMARY KEY (plugin_id){fk}\n)\n\n",
                t("mrsm_plugins"),
                id_col("plugin_id"),
            ));
            if index_names {
                out.push(ix("plugin_name"));
            }
            out
        }
        _ => Vec::new(),
    }
}

/// `datetime.now(timezone.utc).replace(tzinfo=None)`.
fn utc_now_naive() -> NaiveDateTime {
    Utc::now().naive_utc()
}

impl SqlConnector {
    /// `sql_item_name(table, self.flavor, self.instance_schema)` for an instance table, as
    /// SQLAlchemy renders it.
    pub fn instance_table_name(&self, table: &str) -> String {
        let schema = if self.flavor.no_schema() {
            None
        } else {
            self.instance_schema()
        };
        sa_table(table, schema, self.flavor)
    }

    /// A naive UTC timestamp parameter in the form SQLAlchemy stores it for this flavor
    /// (text on SQLite).
    pub(crate) fn timestamp_param(&self, dt: NaiveDateTime) -> SqlParam {
        match self.flavor.driver_family() {
            DriverFamily::Sqlite => SqlParam::Text(dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()),
            _ => SqlParam::Timestamp(dt),
        }
    }

    /// `get_tables(mrsm_instance=self, create=create)`: ensure the instance tables (and the
    /// internal schema) exist. Returns whether they are available.
    pub async fn ensure_instance_tables(&self, create: bool) -> bool {
        if self.tables_ready() {
            return true;
        }
        if !create {
            return self
                .table_exists("mrsm_pipes", self.instance_schema())
                .await;
        }
        // `create_schemas(conn, [conn.internal_schema])`.
        if !self.flavor.no_schema()
            && let Some(schema) = self.internal_schema()
        {
            let query = crate::queries::table::get_create_schema_if_not_exists_queries(
                Some(schema),
                self.flavor,
            );
            if let Ok(queries) = query {
                let _ = self.exec_queries(&queries, false, false).await;
            }
        }
        let mut ok = true;
        for table in INSTANCE_TABLES {
            if self.table_exists(table, self.instance_schema()).await {
                continue;
            }
            let ddl = instance_table_ddl(table, self.flavor, self.instance_schema());
            // `create_all` issues each statement on its own (checkfirst per table).
            let results = self.exec_queries(&ddl, true, true).await;
            if !results.iter().all(Option::is_some) {
                // Another process may have created it concurrently.
                ok = ok && self.table_exists(table, self.instance_schema()).await;
            }
        }
        if ok {
            // 5.0: the unique keys on pipe keys and usernames, for new and existing (v4) tables.
            self.ensure_unique_keys().await;
            self.mark_tables_ready();
        }
        ok
    }

    /// `_log_temporary_tables_creation(tables, ready_to_drop, create)`.
    pub async fn log_temporary_tables_creation(
        &self,
        tables: &[String],
        ready_to_drop: bool,
        create: bool,
    ) -> (bool, String) {
        let _ = self.ensure_instance_tables(create).await;
        let now = utc_now_naive();
        let table_name = self.instance_table_name("mrsm_temp_tables");
        let f = self.flavor;
        let query = format!(
            "INSERT INTO {table_name} (date_created, {}, ready_to_drop) VALUES ({}, {}, {})",
            sa_quote("table", f),
            placeholder(f, 1),
            placeholder(f, 2),
            placeholder(f, 3)
        );
        let mut success = true;
        for table in tables {
            let params = vec![
                self.timestamp_param(now),
                SqlParam::Text(table.clone()),
                if ready_to_drop {
                    self.timestamp_param(now)
                } else {
                    SqlParam::Null
                },
            ];
            // NOTE: We may be running in a temporary context without instance tables.
            if self.driver().exec_with(&query, &params).await.is_err() {
                success = false;
            }
        }
        self.record_temp_tables(tables, ready_to_drop);
        let msg = if success {
            "Success".to_string()
        } else {
            format!(
                "Failed to log temp tables {}.",
                crate::instance::values::items_str(tables)
            )
        };
        (success, msg)
    }

    /// `_drop_temporary_table(table)`.
    pub async fn drop_temporary_table(&self, table: &str) -> (bool, String) {
        let if_exists = if self.flavor.drop_if_exists() {
            "IF EXISTS"
        } else {
            ""
        };
        if if_exists.is_empty() && !self.table_exists(table, self.internal_schema()).await {
            return (true, "Success".into());
        }
        let drop_query = format!(
            "DROP TABLE {if_exists} {}",
            sql_item_name(table, self.flavor, self.internal_schema())
        );
        let mut drop_success = self.exec(&drop_query).await.is_some();
        if !drop_success && !self.table_exists(table, self.internal_schema()).await {
            drop_success = true;
        }
        let msg = if drop_success {
            "Success".to_string()
        } else {
            format!("Failed to drop temporary table '{table}'.")
        };
        (drop_success, msg)
    }

    /// `_drop_temporary_tables()`.
    pub async fn drop_temporary_tables(&self) -> (bool, String) {
        let table_name = self.instance_table_name("mrsm_temp_tables");
        let col = sa_quote("table", self.flavor);
        let mut tables_to_drop = self.ready_temp_tables();
        if tables_to_drop.is_empty() {
            let query = format!(
                "SELECT DISTINCT {table_name}.{col} \nFROM {table_name} \nWHERE {table_name}.ready_to_drop IS NOT NULL"
            );
            if let Ok(df) = self.read(&query).await {
                for doc in super::frame_docs(&df) {
                    if let Some(t) = super::kv_str(doc.get("table"))
                        && !tables_to_drop.contains(&t)
                    {
                        tables_to_drop.push(t);
                    }
                }
            }
        }
        if tables_to_drop.is_empty() {
            return (true, "No tables to drop.".into());
        }
        let mut dropped = Vec::new();
        let mut failed = Vec::new();
        for table in &tables_to_drop {
            let (ok, _) = self.drop_temporary_table(table).await;
            if !ok {
                failed.push(table.clone());
                continue;
            }
            dropped.push(table.clone());
            self.forget_temp_table(table);
        }
        if !dropped.is_empty() {
            let f = self.flavor;
            let query = format!(
                "DELETE FROM {table_name} WHERE {table_name}.{col} IN ({})",
                crate::driver::params::placeholders(f, 1, dropped.len())
            );
            let params: Vec<SqlParam> = dropped.iter().cloned().map(SqlParam::Text).collect();
            let _ = self.driver().exec_with(&query, &params).await;
        }
        let n = tables_to_drop.len();
        if failed.is_empty() {
            (
                true,
                format!("Dropped {n} table{}.", if n != 1 { "s" } else { "" }),
            )
        } else {
            (
                false,
                format!(
                    "Failed to drop temporary tables {}.",
                    crate::instance::values::items_str(&failed)
                ),
            )
        }
    }

    /// `_drop_old_temporary_tables(refresh)`.
    pub async fn drop_old_temporary_tables(&self, refresh: bool) -> (bool, String) {
        let now_ts = std::time::Instant::now();
        let last_check = self.stale_check();
        if refresh || last_check.is_some_and(|t| now_ts.duration_since(t).as_secs_f64() < 60.0) {
            return self.drop_temporary_tables().await;
        }
        self.set_stale_check(now_ts);
        let now = utc_now_naive();
        let end = now - chrono::Duration::minutes(self.settings.stale_temporary_tables_minutes);
        let table_name = self.instance_table_name("mrsm_temp_tables");
        let col = sa_quote("table", self.flavor);
        let f = self.flavor;
        let query = format!(
            "SELECT DISTINCT {table_name}.{col} \nFROM {table_name} \nWHERE {table_name}.date_created < {}",
            placeholder(f, 1)
        );
        let Ok(df) = self.read_params(&query, &[self.timestamp_param(end)]).await else {
            return (true, "Success".into());
        };
        let stale: Vec<String> = super::frame_docs(&df)
            .iter()
            .filter_map(|d| super::kv_str(d.get("table")))
            .collect();
        if !stale.is_empty() {
            let update = format!(
                "UPDATE {table_name} SET ready_to_drop={} WHERE {table_name}.date_created < {} AND {table_name}.ready_to_drop IS NULL",
                placeholder(f, 1),
                placeholder(f, 2)
            );
            let _ = self
                .driver()
                .exec_with(
                    &update,
                    &[self.timestamp_param(now), self.timestamp_param(end)],
                )
                .await;
            self.record_temp_tables(&stale, true);
        }
        self.drop_temporary_tables().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quoting() {
        assert_eq!(sa_quote("table", Flavor::Mysql), "`table`");
        assert_eq!(sa_quote("mrsm_pipes", Flavor::Postgresql), "mrsm_pipes");
        assert_eq!(sa_quote("MySchema", Flavor::Postgresql), "\"MySchema\"");
    }

    /// The DDL SQLAlchemy compiles for the MSSQL, DuckDB and Oracle dialects (v4.1.4,
    /// `CreateTable(table).compile(dialect=conn.engine.dialect)`), whitespace-normalized.
    #[test]
    fn native_flavor_ddl() {
        let norm = |s: &str| s.split_whitespace().collect::<Vec<_>>().join(" ");
        let ddl = |table: &str, flavor: Flavor, schema: Option<&str>| {
            instance_table_ddl(table, flavor, schema)
                .iter()
                .map(|s| norm(s))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ddl("mrsm_pipes", Flavor::Mssql, Some("dbo")),
            vec![norm(
                "CREATE TABLE dbo.mrsm_pipes ( pipe_id INTEGER NOT NULL IDENTITY, connector_keys VARCHAR(256) NOT NULL, metric_key VARCHAR(256) NOT NULL, location_key VARCHAR(256) NULL, parameters VARCHAR(max) NULL, PRIMARY KEY (pipe_id) )"
            )]
        );
        assert_eq!(
            ddl("mrsm_temp_tables", Flavor::Mssql, Some("dbo")),
            vec![
                norm(
                    "CREATE TABLE dbo.mrsm_temp_tables ( date_created DATETIME NOT NULL, [table] VARCHAR(256) NOT NULL, ready_to_drop DATETIME NULL )"
                ),
                "CREATE INDEX ix_dbo_mrsm_temp_tables_date_created ON dbo.mrsm_temp_tables (date_created)".to_string(),
            ]
        );
        assert_eq!(
            ddl("mrsm_users", Flavor::Duckdb, None),
            vec![
                "CREATE SEQUENCE IF NOT EXISTS user_id_seq".to_string(),
                norm(
                    "CREATE TABLE mrsm_users ( user_id INTEGER DEFAULT nextval('user_id_seq') NOT NULL, username VARCHAR(256) NOT NULL, password_hash VARCHAR(1024), email VARCHAR(256), user_type VARCHAR(256), attributes TEXT, PRIMARY KEY (user_id) )"
                ),
                "CREATE INDEX ix_mrsm_users_username ON mrsm_users (username)".to_string(),
            ]
        );
        assert_eq!(
            ddl("mrsm_temp_tables", Flavor::Duckdb, None)[0],
            norm(
                "CREATE TABLE mrsm_temp_tables ( date_created TIMESTAMP WITHOUT TIME ZONE NOT NULL, \"table\" VARCHAR(256) NOT NULL, ready_to_drop TIMESTAMP WITHOUT TIME ZONE )"
            )
        );
        let oracle = ddl("mrsm_plugins", Flavor::Oracle, None);
        assert!(oracle[0].contains("CREATE SEQUENCE plugin_id_seq"));
        assert_eq!(
            oracle[1],
            norm(
                "CREATE TABLE mrsm_plugins ( plugin_id INTEGER DEFAULT plugin_id_seq.nextval NOT NULL, plugin_name VARCHAR2(256 CHAR) NOT NULL, user_id INTEGER NOT NULL, version VARCHAR2(256 CHAR), attributes CLOB, PRIMARY KEY (plugin_id), FOREIGN KEY(user_id) REFERENCES mrsm_users (user_id) )"
            )
        );
        assert_eq!(
            ddl("mrsm_temp_tables", Flavor::Oracle, None)[0],
            norm(
                "CREATE TABLE mrsm_temp_tables ( date_created DATE NOT NULL, \"table\" VARCHAR2(256 CHAR) NOT NULL, ready_to_drop DATE )"
            )
        );
    }

    /// The stale scan is throttled to one per minute: with the throttle window open it flags
    /// (and drops) stale bookkeeping rows, inside it it leaves them alone, and it scans again
    /// once the window has lapsed.
    #[tokio::test]
    async fn stale_scan_throttle_window() {
        let path = std::env::temp_dir().join(format!(
            "_mrsm_rs_stale_scan_{}.db",
            uuid::Uuid::new_v4().simple()
        ));
        let conn =
            SqlConnector::from_uri("rs_stale_scan", &format!("sqlite:///{}", path.display()))
                .await
                .expect("sqlite connects");
        assert!(conn.ensure_instance_tables(true).await);
        let table = conn.instance_table_name("mrsm_temp_tables");
        let stale = utc_now_naive() - chrono::Duration::days(30);
        let count = |name: &'static str| {
            let conn = &conn;
            let table = table.clone();
            async move {
                conn.value(&format!(
                    "SELECT COUNT(*) FROM {table} WHERE \"table\" = '{name}' AND ready_to_drop IS NULL"
                ))
                .await
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
            }
        };
        let seed = |name: &'static str| {
            let conn = &conn;
            let table = table.clone();
            let created = conn.timestamp_param(stale);
            async move {
                conn.driver()
                    .exec_with(
                        &format!(
                            "INSERT INTO {table} (date_created, \"table\", ready_to_drop) VALUES (?, ?, NULL)"
                        ),
                        &[created, SqlParam::Text(name.to_string())],
                    )
                    .await
                    .expect("seed");
            }
        };

        seed("_stale_a").await;
        assert!(conn.stale_check().is_none());
        assert!(conn.drop_old_temporary_tables(false).await.0);
        let scanned_at = conn.stale_check().expect("the first call records its scan");
        assert_eq!(count("_stale_a").await, 0, "the first call scans");

        seed("_stale_b").await;
        for _ in 0..4 {
            assert!(conn.drop_old_temporary_tables(false).await.0);
        }
        assert_eq!(count("_stale_b").await, 1, "throttled calls never scan");
        assert_eq!(conn.stale_check(), Some(scanned_at));

        let lapsed = std::time::Instant::now()
            .checked_sub(std::time::Duration::from_secs(61))
            .expect("a monotonic clock older than a minute");
        conn.set_stale_check(lapsed);
        assert!(conn.drop_old_temporary_tables(false).await.0);
        assert_eq!(count("_stale_b").await, 0, "the window lapsed: scan again");
        drop(conn);
        let _ = std::fs::remove_file(&path);
    }
}
