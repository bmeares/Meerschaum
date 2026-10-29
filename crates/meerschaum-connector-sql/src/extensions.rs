//! PostgreSQL extensions: what is installed, what the server ships, and the update of each
//! (v5, `mrsm upgrade extensions`, the version hints and `mrsm doctor`).
//!
//! Docker runs `/docker-entrypoint-initdb.d` only on an empty volume, so nothing updates a
//! database's extensions after an image pull: TimescaleDB keeps running the version that was
//! created (the HA image ships the older libraries too), PostGIS keeps its old SQL objects.
//! [`status`] reads one database's extensions (`pg_extension` x `pg_available_extensions`) and the
//! server facts the update needs, with one query on its own short-lived connection, never
//! mutating. [`plan`] turns that into the statements, and [`apply`] runs one of them, each on a
//! NEW dedicated connection whose first (and only) statement is the update: TimescaleDB refuses
//! `ALTER EXTENSION timescaledb UPDATE` once the session has loaded the old library.
//!
//! Only the PostgreSQL family has extensions (`postgresql`, `postgis`, `timescaledb`,
//! `timescaledb-ha`, `citus`); every other flavor (and CockroachDB) reports none.

use std::time::Duration;

use serde::Deserialize;
use sqlx::postgres::PgConnection;
use sqlx::{AssertSqlSafe, Connection};

use crate::error::{Result, SqlError};
use crate::flavor::Flavor;
use crate::uri::ConnectionSpec;

/// The PostGIS extensions `postgis_extensions_upgrade()` updates together.
pub const POSTGIS_FAMILY: &[&str] = &[
    "postgis",
    "postgis_raster",
    "postgis_sfcgal",
    "postgis_topology",
    "postgis_tiger_geocoder",
];

/// The default timeout of [`status`] (connect plus the query).
pub const DEFAULT_STATUS_TIMEOUT: Duration = Duration::from_secs(5);

/// One installed extension.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ExtensionStatus {
    /// `pg_extension.extname`.
    pub name: String,
    /// `pg_extension.extversion`.
    pub installed: String,
    /// `pg_available_extensions.default_version`: what `CREATE EXTENSION` / `ALTER EXTENSION
    /// ... UPDATE` installs. `None` when the server no longer ships the extension.
    #[serde(default)]
    pub available: Option<String>,
    /// The schema the extension lives in.
    #[serde(default)]
    pub schema: Option<String>,
    /// The extension's owner.
    #[serde(default)]
    pub owner: Option<String>,
    /// Whether the current user is (a member of) the owner.
    #[serde(default)]
    pub owned: bool,
    /// For a lagging extension: whether the server ships an update path from the installed
    /// version to the default one (`pg_extension_update_paths`). `None` when it does not lag.
    #[serde(default)]
    pub update_path: Option<bool>,
}

impl ExtensionStatus {
    /// Whether the installed version differs from the version the server ships by default.
    pub fn lags(&self) -> bool {
        self.available
            .as_deref()
            .is_some_and(|available| available != self.installed)
    }

    /// Whether it lags and the server can update it (an update path exists).
    pub fn updatable(&self) -> bool {
        self.lags() && self.update_path != Some(false)
    }
}

/// One database's extensions and the server facts the update needs.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct DatabaseStatus {
    /// `current_database()`.
    pub database: String,
    /// `server_version_num` (e.g. `180006`).
    pub server_version_num: i64,
    /// `server_version` (e.g. `18.6 (Ubuntu 18.6-1.pgdg22.04+2)`).
    pub server_version: String,
    /// `current_user`.
    pub current_user: String,
    /// Whether `current_user` is a superuser.
    pub superuser: bool,
    /// `shared_preload_libraries` (`None` when the role may not read it).
    #[serde(default)]
    pub shared_preload_libraries: Option<String>,
    /// `timescaledb.telemetry_level` (`None` without TimescaleDB).
    #[serde(default)]
    pub telemetry_level: Option<String>,
    /// The installed extensions, by name.
    pub extensions: Vec<ExtensionStatus>,
}

impl DatabaseStatus {
    /// The installed extension `name`.
    pub fn extension(&self, name: &str) -> Option<&ExtensionStatus> {
        self.extensions.iter().find(|e| e.name == name)
    }

    /// The extensions whose installed version lags the server's default.
    pub fn lagging(&self) -> Vec<&ExtensionStatus> {
        self.extensions.iter().filter(|e| e.lags()).collect()
    }

    /// The entries of `shared_preload_libraries` (`None` when it could not be read).
    pub fn preload_libraries(&self) -> Option<Vec<String>> {
        self.shared_preload_libraries
            .as_deref()
            .map(split_library_list)
    }

    /// Whether `library` is preloaded (`None` when `shared_preload_libraries` could not be
    /// read).
    pub fn preloaded(&self, library: &str) -> Option<bool> {
        self.preload_libraries()
            .map(|libs| libs.iter().any(|l| l == library))
    }

    /// `pg_stat_statements` is installed but not in `shared_preload_libraries`, so every read
    /// of the view fails ("must be loaded via shared_preload_libraries").
    pub fn pg_stat_statements_unloaded(&self) -> bool {
        self.extension("pg_stat_statements").is_some()
            && self.preloaded("pg_stat_statements") == Some(false)
    }
}

/// Split a `shared_preload_libraries` value (`timescaledb,pg_textsearch`, possibly quoted).
pub fn split_library_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(|s| s.trim().trim_matches('"').trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// Whether `flavor` has PostgreSQL extensions.
pub fn supports_extensions(flavor: Flavor) -> bool {
    matches!(
        flavor,
        Flavor::Postgresql
            | Flavor::Postgis
            | Flavor::Timescaledb
            | Flavor::TimescaledbHa
            | Flavor::Citus
    )
}

/// `"name"` with embedded quotes doubled.
pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `'text'` with embedded quotes doubled.
pub fn quote_literal(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

/// The status query: one row, one JSON text column. `pg_settings` hides superuser-only
/// settings from other roles instead of raising, and the update paths are computed only for
/// lagging extensions.
const STATUS_QUERY: &str = r#"SELECT json_build_object(
    'database', current_database(),
    'server_version_num', current_setting('server_version_num')::bigint,
    'server_version', current_setting('server_version'),
    'current_user', current_user,
    'superuser', COALESCE((SELECT rolsuper FROM pg_roles WHERE rolname = current_user), false),
    'shared_preload_libraries', (SELECT setting FROM pg_settings WHERE name = 'shared_preload_libraries'),
    'telemetry_level', (SELECT setting FROM pg_settings WHERE name = 'timescaledb.telemetry_level'),
    'extensions', COALESCE((
        SELECT json_agg(json_build_object(
            'name', e.extname,
            'installed', e.extversion,
            'available', a.default_version,
            'schema', n.nspname,
            'owner', r.rolname,
            'owned', pg_has_role(current_user, e.extowner, 'MEMBER'),
            'update_path', CASE
                WHEN a.default_version IS NOT NULL AND a.default_version <> e.extversion
                THEN EXISTS (
                    SELECT 1 FROM pg_extension_update_paths(e.extname) p
                    WHERE p.source = e.extversion
                      AND p.target = a.default_version
                      AND p.path IS NOT NULL
                )
            END
        ) ORDER BY e.extname)
        FROM pg_extension e
        LEFT JOIN pg_available_extensions a ON a.name = e.extname
        LEFT JOIN pg_namespace n ON n.oid = e.extnamespace
        LEFT JOIN pg_roles r ON r.oid = e.extowner
    ), '[]'::json)
)::text"#;

/// `spec` pointed at `database` (the instance's own database when `None`).
pub fn spec_for_database(spec: &ConnectionSpec, database: Option<&str>) -> ConnectionSpec {
    let mut spec = spec.clone();
    if let Some(database) = database {
        spec.database = Some(database.to_string());
    }
    spec
}

/// Open a dedicated connection (never from a pool). `sqlx` sends only the startup message (the
/// configured runtime parameters): no statement runs before the caller's first one.
async fn open(spec: &ConnectionSpec) -> Result<PgConnection> {
    use crate::driver::connect::{self, ConnectErrorKind};
    let opts = crate::driver::pg_connect_options(spec)?;
    // One attempt (no pool retry), bounded by `connect_timeout` and classified like the pool's
    // first connection: a refused connection or rejected credentials fail with a clear message.
    let limit = connect::connect_timeout(spec);
    match tokio::time::timeout(limit, PgConnection::connect_with(&opts)).await {
        Ok(Ok(connection)) => Ok(connection),
        Ok(Err(e)) => Err(connect::map_sqlx(spec, e, limit)),
        Err(_) => Err(SqlError::Connect(connect::make_error(
            spec,
            ConnectErrorKind::Timeout,
            "",
            limit,
        ))),
    }
}

fn timeout_error(what: &str, timeout: Duration) -> SqlError {
    SqlError::Database(format!(
        "{what} did not answer within {:.1} s.",
        timeout.as_secs_f64()
    ))
}

/// Read one database's extensions and server facts (`database`: another database on the same
/// server; `None`: the connector's own). `Ok(None)` for a flavor without extensions. Connecting
/// and the query together are bounded by `timeout`; nothing is changed.
pub async fn status(
    spec: &ConnectionSpec,
    database: Option<&str>,
    timeout: Duration,
) -> Result<Option<DatabaseStatus>> {
    if !supports_extensions(spec.flavor) {
        return Ok(None);
    }
    let spec = spec_for_database(spec, database);
    let fut = async {
        let mut conn = open(&spec).await?;
        // Unnamed statement (`persistent(false)`): works behind PgBouncer in transaction mode.
        let row = sqlx::query(STATUS_QUERY)
            .persistent(false)
            .fetch_one(&mut conn)
            .await?;
        let text: String = sqlx::Row::try_get(&row, 0)?;
        let _ = conn.close().await;
        serde_json::from_str::<DatabaseStatus>(&text)
            .map_err(|e| SqlError::Database(format!("Unexpected extension status: {e}")))
    };
    match tokio::time::timeout(timeout, fut).await {
        Ok(result) => result.map(Some),
        Err(_) => Err(timeout_error("The database", timeout)),
    }
}

/// How an update runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateKind {
    /// `ALTER EXTENSION timescaledb UPDATE` as the first statement of a new session.
    Timescaledb,
    /// `SELECT postgis_extensions_upgrade();` (PostGIS and its raster, topology, SFCGAL and
    /// tiger geocoder extensions together).
    Postgis,
    /// `ALTER EXTENSION <name> UPDATE`.
    Alter,
}

/// One update of one database: the extensions it brings to their default versions and its
/// statement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedUpdate {
    /// The database it runs in.
    pub database: String,
    /// The extensions it updates (several for the PostGIS family).
    pub extensions: Vec<ExtensionStatus>,
    /// How it runs.
    pub kind: UpdateKind,
    /// The statement (exactly what runs, and what a superuser can run by hand).
    pub statement: String,
}

impl PlannedUpdate {
    /// The first extension's name (`postgis` for the PostGIS family).
    pub fn name(&self) -> &str {
        self.extensions
            .first()
            .map(|e| e.name.as_str())
            .unwrap_or_default()
    }

    /// Whether the current user may run it (superuser, or a member of every extension's owner).
    pub fn permitted(&self, status: &DatabaseStatus) -> bool {
        status.superuser || self.extensions.iter().all(|e| e.owned)
    }
}

/// What lags in `status` and cannot be updated (no update path from the installed version):
/// reported, never attempted.
pub fn not_updatable(status: &DatabaseStatus) -> Vec<&ExtensionStatus> {
    status
        .extensions
        .iter()
        .filter(|e| e.lags() && !e.updatable())
        .collect()
}

/// The updates `status` needs: TimescaleDB first, then the PostGIS family (one call), then
/// every other lagging extension by name. Extensions without an update path are left out (see
/// [`not_updatable`]).
pub fn plan(status: &DatabaseStatus) -> Vec<PlannedUpdate> {
    let mut out = Vec::new();
    let lagging: Vec<&ExtensionStatus> =
        status.extensions.iter().filter(|e| e.updatable()).collect();
    if let Some(ts) = lagging.iter().find(|e| e.name == "timescaledb") {
        out.push(PlannedUpdate {
            database: status.database.clone(),
            extensions: vec![(*ts).clone()],
            kind: UpdateKind::Timescaledb,
            statement: "ALTER EXTENSION timescaledb UPDATE;".into(),
        });
    }
    let postgis: Vec<ExtensionStatus> = lagging
        .iter()
        .filter(|e| POSTGIS_FAMILY.contains(&e.name.as_str()))
        .map(|e| (*e).clone())
        .collect();
    // `postgis_extensions_upgrade()` is a function of `postgis` itself; without `postgis`
    // installed (impossible for its dependents) fall back to `ALTER EXTENSION`.
    let has_postgis = status.extension("postgis").is_some();
    if !postgis.is_empty() && has_postgis {
        out.push(PlannedUpdate {
            database: status.database.clone(),
            extensions: postgis,
            kind: UpdateKind::Postgis,
            statement: "SELECT postgis_extensions_upgrade();".into(),
        });
    }
    for ext in lagging {
        if ext.name == "timescaledb" || (has_postgis && POSTGIS_FAMILY.contains(&ext.name.as_str()))
        {
            continue;
        }
        out.push(PlannedUpdate {
            database: status.database.clone(),
            extensions: vec![ext.clone()],
            kind: UpdateKind::Alter,
            statement: format!("ALTER EXTENSION {} UPDATE;", quote_ident(&ext.name)),
        });
    }
    out
}

/// Run one planned update on a NEW connection to its database, as that session's first
/// statement (simple query protocol: no prepare, no type introspection).
pub async fn apply(spec: &ConnectionSpec, update: &PlannedUpdate) -> Result<()> {
    run_first_statement(spec, Some(&update.database), &update.statement).await
}

/// Run `sql` as the first statement of a new dedicated session on `database` (simple query
/// protocol), then close the session.
pub async fn run_first_statement(
    spec: &ConnectionSpec,
    database: Option<&str>,
    sql: &str,
) -> Result<()> {
    let spec = spec_for_database(spec, database);
    let mut conn = open(&spec).await?;
    let result = sqlx::raw_sql(AssertSqlSafe(sql)).execute(&mut conn).await;
    let _ = conn.close().await;
    result.map(|_| ()).map_err(SqlError::from)
}

/// Run `sql` on a new session after another statement (`SELECT 1`) has loaded whatever the
/// session loads: what [`apply`] avoids. Exposed for the tests that prove TimescaleDB's
/// first-statement rule is real.
pub async fn run_after_first_query(
    spec: &ConnectionSpec,
    database: Option<&str>,
    sql: &str,
) -> Result<()> {
    let spec = spec_for_database(spec, database);
    let mut conn = open(&spec).await?;
    let result = async {
        sqlx::raw_sql("SELECT 1").execute(&mut conn).await?;
        sqlx::raw_sql(AssertSqlSafe(sql)).execute(&mut conn).await
    }
    .await;
    let _ = conn.close().await;
    result.map(|_| ()).map_err(SqlError::from)
}

/// The server-level statements that turn TimescaleDB's telemetry off the way the image's
/// `000_install_timescaledb.sh` does for `TIMESCALEDB_TELEMETRY=off` (it writes the setting to
/// `postgresql.conf`; `ALTER SYSTEM` writes `postgresql.auto.conf`, which also wins over it).
pub const TELEMETRY_OFF_STATEMENTS: &[&str] = &[
    "ALTER SYSTEM SET timescaledb.telemetry_level = 'off';",
    "SELECT pg_reload_conf();",
];

/// Unschedule the telemetry job in one database with TimescaleDB (the image runs
/// `SELECT alter_job(1, scheduled := false)`; the job is found by its procedure instead of its
/// id). Idempotent.
pub const UNSCHEDULE_TELEMETRY_JOB: &str = "SELECT alter_job(job_id, scheduled => false) \
FROM timescaledb_information.jobs WHERE proc_name = 'policy_telemetry' AND scheduled;";

/// `ALTER SYSTEM SET shared_preload_libraries = ...` keeping `current` and appending
/// `library` (`None` when it is already listed).
pub fn preload_statement(current: &[String], library: &str) -> Option<String> {
    if current.iter().any(|l| l == library) {
        return None;
    }
    let libs: Vec<String> = current
        .iter()
        .map(String::as_str)
        .chain(std::iter::once(library))
        .map(quote_literal)
        .collect();
    Some(format!(
        "ALTER SYSTEM SET shared_preload_libraries = {};",
        libs.join(", ")
    ))
}

/// Whether TimescaleDB's telemetry is still on (`timescaledb.telemetry_level` is set and not
/// `off`).
pub fn telemetry_on(status: &DatabaseStatus) -> bool {
    status.extension("timescaledb").is_some()
        && status
            .telemetry_level
            .as_deref()
            .is_some_and(|level| level != "off")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ext(name: &str, installed: &str, available: Option<&str>) -> ExtensionStatus {
        let lags = available.is_some_and(|a| a != installed);
        ExtensionStatus {
            name: name.into(),
            installed: installed.into(),
            available: available.map(str::to_string),
            schema: Some("public".into()),
            owner: Some("postgres".into()),
            owned: false,
            update_path: lags.then_some(true),
        }
    }

    fn status(extensions: Vec<ExtensionStatus>) -> DatabaseStatus {
        DatabaseStatus {
            database: "db".into(),
            server_version_num: 180006,
            server_version: "18.6".into(),
            current_user: "u".into(),
            superuser: true,
            shared_preload_libraries: Some("timescaledb,pg_textsearch".into()),
            telemetry_level: Some("basic".into()),
            extensions,
        }
    }

    #[test]
    fn plans_timescaledb_first_postgis_together_rest_by_name() {
        let s = status(vec![
            ext("pg_stat_statements", "1.11", Some("1.12")),
            ext("plpgsql", "1.0", Some("1.0")),
            ext("postgis", "3.5.2", Some("3.6.4")),
            ext("postgis_raster", "3.5.2", Some("3.6.4")),
            ext("timescaledb", "2.29.2", Some("2.30.2")),
            ext("timescaledb_toolkit", "1.22.0", Some("1.26.0")),
        ]);
        let p = plan(&s);
        let names: Vec<&str> = p.iter().map(|u| u.name()).collect();
        assert_eq!(
            names,
            vec![
                "timescaledb",
                "postgis",
                "pg_stat_statements",
                "timescaledb_toolkit"
            ]
        );
        assert_eq!(p[0].statement, "ALTER EXTENSION timescaledb UPDATE;");
        assert_eq!(p[1].statement, "SELECT postgis_extensions_upgrade();");
        assert_eq!(p[1].extensions.len(), 2);
        assert_eq!(
            p[3].statement,
            "ALTER EXTENSION \"timescaledb_toolkit\" UPDATE;"
        );
        assert!(p.iter().all(|u| u.permitted(&s)));
    }

    #[test]
    fn nothing_lags_nothing_planned() {
        let s = status(vec![ext("timescaledb", "2.30.2", Some("2.30.2"))]);
        assert!(plan(&s).is_empty());
        assert!(s.lagging().is_empty());
        // An extension the server no longer ships does not lag (nothing to update to).
        let s = status(vec![ext("gone", "1.0", None)]);
        assert!(plan(&s).is_empty());
    }

    #[test]
    fn no_update_path_is_reported_not_planned() {
        let mut e = ext("timescaledb", "2.31.0", Some("2.30.2"));
        e.update_path = Some(false);
        let s = status(vec![e]);
        assert!(plan(&s).is_empty());
        assert_eq!(not_updatable(&s).len(), 1);
    }

    #[test]
    fn permissions_need_superuser_or_owner() {
        let mut s = status(vec![ext("timescaledb_toolkit", "1.22.0", Some("1.26.0"))]);
        s.superuser = false;
        let p = plan(&s);
        assert!(!p[0].permitted(&s));
        s.extensions[0].owned = true;
        assert!(plan(&s)[0].permitted(&s));
    }

    #[test]
    fn preload_and_telemetry() {
        let mut s = status(vec![
            ext("timescaledb", "2.30.2", Some("2.30.2")),
            ext("pg_stat_statements", "1.12", Some("1.12")),
        ]);
        assert!(s.pg_stat_statements_unloaded());
        assert!(telemetry_on(&s));
        assert_eq!(
            preload_statement(&s.preload_libraries().unwrap(), "pg_stat_statements").unwrap(),
            "ALTER SYSTEM SET shared_preload_libraries = 'timescaledb', 'pg_textsearch', \
             'pg_stat_statements';"
        );
        s.shared_preload_libraries = Some("timescaledb, \"pg_stat_statements\"".into());
        assert!(!s.pg_stat_statements_unloaded());
        assert_eq!(
            preload_statement(&s.preload_libraries().unwrap(), "pg_stat_statements"),
            None
        );
        // Unreadable (not a superuser): unknown, not "unloaded".
        s.shared_preload_libraries = None;
        assert!(!s.pg_stat_statements_unloaded());
        s.telemetry_level = Some("off".into());
        assert!(!telemetry_on(&s));
        assert_eq!(
            preload_statement(&[], "pg_stat_statements").unwrap(),
            "ALTER SYSTEM SET shared_preload_libraries = 'pg_stat_statements';"
        );
    }

    #[test]
    fn flavors_with_extensions() {
        assert!(supports_extensions(Flavor::TimescaledbHa));
        assert!(supports_extensions(Flavor::Postgis));
        assert!(!supports_extensions(Flavor::Cockroachdb));
        assert!(!supports_extensions(Flavor::Sqlite));
        assert!(!supports_extensions(Flavor::Mysql));
    }

    #[test]
    fn parses_status_json() {
        let text = r#"{"database":"d","server_version_num":180006,"server_version":"18.6",
            "current_user":"u","superuser":false,"shared_preload_libraries":null,
            "telemetry_level":null,"extensions":[{"name":"plpgsql","installed":"1.0",
            "available":"1.0","schema":"pg_catalog","owner":"postgres","owned":false,
            "update_path":null}]}"#;
        let s: DatabaseStatus = serde_json::from_str(text).unwrap();
        assert_eq!(s.extensions.len(), 1);
        assert!(s.lagging().is_empty());
        assert_eq!(s.preloaded("x"), None);
    }
}
