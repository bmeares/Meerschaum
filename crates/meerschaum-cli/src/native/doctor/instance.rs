//! SQL, API, Valkey, extension, and unique-key instance checks for `mrsm doctor`.

use super::*;

/// Run async primitives with `ctx.session.block_on(..)`, keep the call short (it runs inside the
/// doctor's time budget) and never write to the database.
pub(super) fn extension_checks(ctx: &InstanceContext) -> Vec<Check> {
    let Some(sql) = &ctx.sql else {
        return vec![Check::skip(
            "extensions",
            "postgresql",
            "the instance is not a reachable SQL instance",
        )];
    };
    // One read-only query on its own short-lived connection, bounded.
    let timeout = Duration::from_secs(3);
    let status = ctx
        .session
        .block_on(async {
            tokio::time::timeout(
                timeout + Duration::from_millis(500),
                meerschaum_connector_sql::extensions::status(sql.spec(), None, timeout),
            )
            .await
        })
        .map_err(|_| "the database did not answer in time".to_string())
        .and_then(|r| r.map_err(|e| e.to_string()));
    crate::native::extensions::doctor_checks(
        &status,
        &ctx.keys,
        ctx.stack_db,
        ctx.stack_telemetry_off,
    )
}

/// The unique keys of the instance tables (5.0: pipe keys, usernames, plugin names), read-only.
pub(super) fn unique_key_checks(ctx: &InstanceContext) -> Vec<Check> {
    let Some(sql) = &ctx.sql else {
        return vec![Check::skip(
            "instance",
            "unique keys",
            "the instance is not a reachable SQL instance",
        )];
    };
    UNIQUE_KEYS
        .iter()
        .map(|key| {
            let status = ctx.session.block_on(async {
                tokio::time::timeout(PROBE_TIMEOUT, sql.unique_key_status(key))
                    .await
                    .map_err(|_| "the database did not answer in time".to_string())
                    .and_then(|r| r.map_err(|e| e.to_string()))
            });
            let table = sql.instance_table_name(key.table);
            Check::from_verdict(
                "instance",
                unique_key_check_name(key),
                judge_unique_key(key, &status, &table),
            )
        })
        .collect()
}

/// Whether DuckDB's library can be loaded (`show drivers` logic), and whether a missing one
/// would be downloaded on first use.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct DuckdbGate {
    /// Why the library cannot be loaded (`None`: it is found).
    pub problem: Option<String>,
    /// `MRSM_NO_AUTO_INSTALL` is not set.
    pub auto_install: bool,
    /// Side-effect-free embedding-host facts; no host is present in standalone processes.
    pub host: Option<DuckdbHostDiagnostic>,
}

/// The instance is a DuckDB database but its library is missing: connecting would load it, and
/// loading downloads and installs it on first use, so the doctor does not connect. `None`: go on.
pub fn judge_duckdb_gate(g: &DuckdbGate) -> Option<Verdict> {
    let problem = g.problem.as_ref()?;
    let fix = "`mrsm install driver duckdb` (or set MRSM_DUCKDB_LIB to a libduckdb you provide)";
    Some(if g.auto_install && !problem.starts_with("not loadable") {
        Verdict::warn(
            format!(
                "not probed: DuckDB's library is {problem}; it is downloaded on first use and the doctor never installs"
            ),
            fix,
        )
    } else {
        Verdict::fail(
            format!("the instance is a DuckDB database but its library is {problem}"),
            fix,
        )
    })
}

/// The instance group. Returns the checks and, for a reachable `sql:` instance, its connector.
pub(super) fn gather_instance(
    session: &crate::native::Session,
    config: &Config,
    keys: &str,
    duckdb: &DuckdbGate,
) -> (Vec<Check>, Option<Arc<SqlConnector>>) {
    use crate::native::session::split_keys;
    let (typ, label) = split_keys(keys);
    let (valid, section) =
        config.get_with(&["meerschaum", "connectors", &typ, &label], config_opts());
    let defined = valid && section.as_map().is_some_and(|m| !m.is_empty());
    if !defined && matches!(typ.as_str(), "sql" | "api" | "valkey") {
        let var = format!("MRSM_{}_{}", typ.to_uppercase(), label.to_uppercase());
        return (
            vec![Check::fail(
                "instance",
                "connection",
                format!(
                    "the instance '{typ}:{label}' is not defined: no connector section and no {var} variable"
                ),
                format!(
                    "define it (`mrsm bootstrap connector {typ}:{label}`, or set {var}='<uri>'), or choose an existing instance with `-i` or `mrsm edit config meerschaum`"
                ),
            )],
            None,
        );
    }
    match typ.as_str() {
        "sql" => gather_sql(session, config, &label, keys, duckdb),
        "api" => (gather_api(session, config, &label), None),
        "valkey" => (gather_valkey(session, config, &label), None),
        _ => (
            vec![Check::skip(
                "instance",
                "connection",
                format!(
                    "'{keys}' is a plugin's instance connector: the doctor never imports plugin code"
                ),
            )],
            None,
        ),
    }
}

/// Whether a TCP connection to `host:port` opens within `timeout` (the OS error otherwise).
pub(super) fn tcp_reachable(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    use std::net::ToSocketAddrs;
    let addrs: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|e| format!("cannot resolve '{host}': {e}"))?
        .collect();
    let mut last = format!("no address for '{host}'");
    for addr in addrs {
        match std::net::TcpStream::connect_timeout(&addr, timeout) {
            Ok(_) => return Ok(()),
            Err(e) => last = format!("{e}"),
        }
    }
    Err(last)
}

pub(super) fn gather_sql(
    session: &crate::native::Session,
    config: &Config,
    label: &str,
    keys: &str,
    duckdb: &DuckdbGate,
) -> (Vec<Check>, Option<Arc<SqlConnector>>) {
    use crate::native::session::{connector_attributes, sql_spec};
    let attrs = connector_attributes(config, "sql", label);
    let flavor = flavor_of(&attrs);
    if flavor == "duckdb"
        && let Some(host) = &duckdb.host
    {
        match host {
            DuckdbHostDiagnostic::Python { .. } => {
                return (
                    vec![Check::skip(
                        "instance",
                        "connection",
                        "Python DuckDB transport: database connection not probed to preserve read-only diagnostics (no database or shared anchor was opened)",
                    )],
                    None,
                );
            }
            DuckdbHostDiagnostic::Refused(reason) => {
                return (
                    vec![Check::fail(
                        "instance",
                        "connection",
                        reason.clone(),
                        "restart with a fresh interpreter (spawn or exec)",
                    )],
                    None,
                );
            }
            DuckdbHostDiagnostic::Native => {}
        }
    }
    if matches!(flavor.as_str(), "sqlite" | "geopackage" | "duckdb") {
        let root_db = config
            .paths()
            .path("SQLITE_DB_PATH")
            .to_string_lossy()
            .into_owned();
        let raw = config
            .get(&["meerschaum", "connectors", "sql", label])
            .map(|v| v.to_map())
            .unwrap_or_default();
        if let Some(path) =
            sqlite_path(&raw, label, &root_db).or_else(|| sqlite_path(&attrs, label, &root_db))
            && path != ":memory:"
            && !Path::new(&path).exists()
        {
            return (
                vec![Check::warn(
                    "instance",
                    "connection",
                    format!(
                        "the {flavor} database {path} does not exist yet (nothing was created)"
                    ),
                    "any command that writes to the instance creates it (e.g. `mrsm bootstrap pipe`); check the path if you expected data there",
                )],
                None,
            );
        }
    }
    if flavor == "duckdb"
        && let Some(verdict) = judge_duckdb_gate(duckdb)
    {
        return (
            vec![Check::from_verdict("instance", "connection", verdict)],
            None,
        );
    }
    let spec = match sql_spec(label, config) {
        Ok(spec) => spec,
        Err(e) => {
            return (
                vec![Check::fail(
                    "instance",
                    "connection",
                    format!(
                        "the connector '{keys}' is invalid: {}",
                        first_line(&e.message)
                    ),
                    "fix its attributes (`mrsm edit config meerschaum`): a flavor or a uri, plus host, port, database and credentials",
                )],
                None,
            );
        }
    };
    let target = match (&spec.host, spec.port, &spec.database) {
        (Some(h), Some(p), Some(d)) => format!("{h}:{p}/{d}"),
        (Some(h), None, Some(d)) => format!("{h}/{d}"),
        (_, _, Some(d)) => d.clone(),
        _ => keys.to_string(),
    };
    let flavor = spec.flavor;
    // A closed port answers at once; without this the connection pool retries until its timeout.
    if let (Some(host), Some(port)) = (spec.host.as_deref(), spec.port.or(flavor.default_port()))
        && !host.starts_with('/')
        && let Err(e) = tcp_reachable(host, port, Duration::from_secs(3))
    {
        return (
            vec![Check::fail(
                "instance",
                "connection",
                format!("{keys} ({flavor}) at {target}: {e}"),
                "check the server is running and that the connector's host, port and credentials are right (`mrsm edit config meerschaum`), or choose another instance with `-i`",
            )],
            None,
        );
    }
    let settings =
        meerschaum_connector_sql::instance::InstanceSettings::from_config(flavor, config);
    let conn = {
        let _guard = session.handle().enter();
        let connection = if matches!(flavor, Flavor::Sqlite | Flavor::Geopackage) {
            SqlConnector::connect_lazy_sqlite_read_only(label, spec, settings)
        } else {
            SqlConnector::connect_lazy(label, spec, settings)
        };
        match connection {
            Ok(c) => Arc::new(c),
            Err(e) => {
                return (
                    vec![Check::fail(
                        "instance",
                        "connection",
                        format!("cannot create the connector: {e}"),
                        "check the connector's attributes (`mrsm edit config meerschaum`)",
                    )],
                    None,
                );
            }
        }
    };
    let query = meerschaum_connector_sql::queries::table::get_db_version_query(flavor);
    let reached =
        session.block_on(async { tokio::time::timeout(PROBE_TIMEOUT, conn.read(&query)).await });
    let fix = "check the server is running and that the connector's host, port and credentials are right (`mrsm edit config meerschaum`), or choose another instance with `-i`";
    match reached {
        Err(_) => {
            return (
                vec![Check::fail(
                    "instance",
                    "connection",
                    format!(
                        "{keys} ({flavor}) at {target} did not answer within {} seconds",
                        PROBE_TIMEOUT.as_secs()
                    ),
                    fix,
                )],
                None,
            );
        }
        Ok(Err(e)) => {
            return (
                vec![Check::fail(
                    "instance",
                    "connection",
                    format!(
                        "{keys} ({flavor}) at {target}: {}",
                        first_line(&e.to_string())
                    ),
                    fix,
                )],
                None,
            );
        }
        Ok(Ok(_)) => {}
    }
    let version = session
        .block_on(async { tokio::time::timeout(PROBE_TIMEOUT, conn.db_version()).await })
        .ok()
        .flatten()
        .map(|v| first_line(&v))
        .unwrap_or_else(|| "unknown".into());
    let mut out = vec![
        Check::pass(
            "instance",
            "connection",
            format!("{keys} ({flavor}) at {target}"),
        ),
        Check::pass("instance", "server version", version),
    ];
    let schema = session
        .block_on(async { tokio::time::timeout(PROBE_TIMEOUT, conn.require_schema()).await })
        .ok()
        .and_then(Result::ok)
        .map(|_| conn.instance_schema().map(str::to_string))
        .unwrap_or(None);
    let names = ["mrsm_pipes", "mrsm_users", "mrsm_plugins"];
    let present: Vec<bool> = names
        .iter()
        .map(|t| {
            session
                .block_on(async {
                    tokio::time::timeout(PROBE_TIMEOUT, conn.table_exists(t, schema.as_deref()))
                        .await
                })
                .unwrap_or(false)
        })
        .collect();
    let missing: Vec<&str> = names
        .iter()
        .zip(&present)
        .filter(|(_, p)| !**p)
        .map(|(t, _)| *t)
        .collect();
    out.push(if missing.is_empty() {
        Check::pass("instance", "tables", format!("{} exist", names.join(", ")))
    } else if missing.len() == names.len() {
        Check::warn(
            "instance",
            "tables",
            "no Meerschaum tables yet: this is a new instance (nothing was created)",
            "they are created by the first command that writes (`mrsm bootstrap pipe`, `mrsm register pipes`, `mrsm sync pipes`)",
        )
    } else {
        Check::warn(
            "instance",
            "tables",
            format!("missing: {}", missing.join(", ")),
            "`mrsm register pipes` or any write command creates the missing tables; if they were dropped on purpose, ignore this",
        )
    });
    if let Some(pipe) = conn.engine_state_pipe(keys, "engine_runs") {
        let exists = session
            .block_on(async { tokio::time::timeout(PROBE_TIMEOUT, conn.pipe_exists(&pipe)).await })
            .ok()
            .and_then(Result::ok)
            .unwrap_or(false);
        out.push(Check::pass(
            "instance",
            "engine state",
            if exists {
                "the Syncing Engine's run history exists"
            } else {
                "no engine runs recorded yet (the first graph run creates the state tables)"
            },
        ));
    }
    (out, Some(conn))
}

pub(super) fn gather_api(
    session: &crate::native::Session,
    config: &Config,
    label: &str,
) -> Vec<Check> {
    let mut connector = match meerschaum_connector_api::APIConnector::from_config(config, label) {
        Ok(c) => c,
        Err(e) => {
            return vec![Check::fail(
                "instance",
                "connection",
                format!(
                    "the connector 'api:{label}' is invalid: {}",
                    first_line(&e.to_string())
                ),
                "set its host, port and protocol (`mrsm edit config meerschaum`)",
            )];
        }
    };
    connector.set_timeout(PROBE_TIMEOUT);
    let uri = connector.uri();
    let fix = "start the API (`mrsm start api`) or fix the connector's host, port and protocol (`mrsm edit config meerschaum`)";
    let health = session.block_on(async {
        tokio::time::timeout(
            PROBE_TIMEOUT + Duration::from_secs(1),
            connector.healthcheck(),
        )
        .await
    });
    let mut out = Vec::new();
    // The URI carries credentials only as a password the connector keeps separately; print host only.
    let shown = uri.split('@').next_back().unwrap_or(&uri).to_string();
    match health {
        Err(_) => {
            out.push(Check::fail(
                "instance",
                "healthcheck",
                format!(
                    "{shown} did not answer within {} seconds",
                    PROBE_TIMEOUT.as_secs()
                ),
                fix,
            ));
            return out;
        }
        Ok(Err(e)) => {
            out.push(Check::fail(
                "instance",
                "healthcheck",
                format!("{shown}: {}", first_line(&e.to_string())),
                fix,
            ));
            return out;
        }
        Ok(Ok(h)) if h.success => {
            out.push(Check::pass(
                "instance",
                "healthcheck",
                format!("{shown}: {}", h.message),
            ));
        }
        Ok(Ok(h)) => {
            out.push(Check::fail(
                "instance",
                "healthcheck",
                format!(
                    "{shown} answers, but its own instance does not: {}",
                    first_line(&h.message)
                ),
                "check the database behind the API server (run `mrsm doctor` on that host)",
            ));
        }
    }
    let version = session
        .block_on(async { tokio::time::timeout(PROBE_TIMEOUT, connector.get_mrsm_version()).await })
        .ok()
        .flatten();
    out.push(Check::from_verdict(
        "instance",
        "version",
        judge_api_version(version.as_deref(), crate::VERSION),
    ));
    out
}

pub(super) fn gather_valkey(
    session: &crate::native::Session,
    config: &Config,
    label: &str,
) -> Vec<Check> {
    let attrs = meerschaum_connector_valkey::ValkeyConnector::attributes_from_config(config, label);
    let result = session.block_on(async {
        tokio::time::timeout(
            PROBE_TIMEOUT,
            meerschaum_connector_valkey::ValkeyConnector::connect(label, attrs),
        )
        .await
    });
    match result {
        Ok(Ok(_)) => vec![Check::pass(
            "instance",
            "connection",
            format!("valkey:{label} connected"),
        )],
        Ok(Err(e)) => vec![Check::fail(
            "instance",
            "connection",
            format!("valkey:{label}: {}", first_line(&e.to_string())),
            "check the server is running and the connector's host, port and password (`mrsm edit config meerschaum`)",
        )],
        Err(_) => vec![Check::fail(
            "instance",
            "connection",
            format!(
                "valkey:{label} did not answer within {} seconds",
                PROBE_TIMEOUT.as_secs()
            ),
            "check the server is running and the connector's host and port",
        )],
    }
}
