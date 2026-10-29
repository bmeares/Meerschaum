//! `upgrade extensions` (v5): bring a PostgreSQL instance's extensions to the versions its server
//! ships, and the one-line hints `stack up`, `upgrade meerschaum` and `start api` print when an
//! extension lags.
//!
//! Docker runs `/docker-entrypoint-initdb.d` only on an empty volume, so after an image pull a
//! database keeps the extension versions it was created with (TimescaleDB keeps loading its old
//! library; the HA image still ships it). The status and the updates are
//! `meerschaum_connector_sql::extensions`: every update runs as the FIRST statement of its own new
//! session (TimescaleDB refuses `ALTER EXTENSION timescaledb UPDATE` in a session that loaded the
//! old library). The target is the instance (`-i`); further databases on the same server are
//! named as action words (`upgrade extensions template1 postgres`), never all of them implicitly.
//! On the stack's own database (`sql:main` with a stack in this root) and as a superuser, the
//! stack's `TIMESCALEDB_TELEMETRY: 'off'` is applied (the image's setup script never ran under
//! the stack's directory mount), and `pg_stat_statements` may be added to
//! `shared_preload_libraries` (a separate question, default no; `-y` / `--force` apply it,
//! `--noask` keeps the default; it takes effect after a restart of the database, which is never
//! done here). After a TimescaleDB update
//! there, the stack's running `api` and `grafana` are restarted (their pooled sessions keep the
//! old library).

use std::net::IpAddr;
use std::path::Path;
use std::time::Duration;

use serde_json::Value as Json;

use meerschaum_connector_sql::extensions::{
    self, DatabaseStatus, PlannedUpdate, TELEMETRY_OFF_STATEMENTS, UNSCHEDULE_TELEMETRY_JOB,
    UpdateKind,
};
use meerschaum_connector_sql::uri::ConnectionSpec;
use meerschaum_core::SuccessTuple;

use crate::builtin::ActionResult;
use crate::dispatch::{CommandSpec, Runtime};
use crate::format::rich::console::{Overflow, Renderable};
use crate::format::rich::table::{Column, HEAVY_HEAD, Table};
use crate::value::{ArgsMap, PyValue};

use super::session::{Session, split_keys, sql_spec};
use super::{flag, info, instance_keys, plural, session, warn, words, yes_no};

/// How long reading one database's extensions may take in the action.
const ACTION_TIMEOUT: Duration = Duration::from_secs(15);

/// How long a hint may wait for the database (a hint never slows its host action down more).
pub const HINT_TIMEOUT: Duration = Duration::from_secs(2);

/// The config path of the stack database's telemetry switch.
const TELEMETRY_KEYS: &[&str] = &[
    "stack",
    "docker-compose.yaml",
    "services",
    "db",
    "environment",
    "TIMESCALEDB_TELEMETRY",
];

/// `type:label` with the default label.
fn full_keys(keys: &str) -> String {
    let (typ, label) = split_keys(keys);
    format!("{typ}:{label}")
}

/// The connection spec of `keys` when it is a `sql:` connector of a flavor with extensions.
fn postgres_spec(rt: &mut Runtime<'_>, keys: &str) -> Result<Option<ConnectionSpec>, String> {
    let (typ, label) = split_keys(keys);
    if typ != "sql" {
        return Ok(None);
    }
    let spec = sql_spec(&label, rt.config()).map_err(|e| e.message.trim().to_string())?;
    Ok(extensions::supports_extensions(spec.flavor).then_some(spec))
}

/// Classify only this root's owned, running database and its exact connection endpoint. The
/// container's Compose origin, published/internal port and configured database must match;
/// read-only SQL identity/address checks prove that a localhost override did not reach another
/// PostgreSQL server (including when Docker uses a remote daemon). Failure is conservative:
/// ordinary explicit extension updates still work, without stack tuning or service restarts.
pub fn is_stack_instance(rt: &mut Runtime<'_>, keys: &str) -> bool {
    if full_keys(keys) != "sql:main" {
        return false;
    }
    let compose_path = rt.config().paths().path("STACK_COMPOSE_PATH");
    let Ok(spec) = sql_spec("main", rt.config()) else {
        return false;
    };
    let db_hostname = rt
        .config()
        .get(&["stack", "docker-compose.yaml", "services", "db", "hostname"])
        .map(|v| v.py_str());
    if !stack_candidate(
        keys,
        compose_path.exists(),
        spec.host.as_deref(),
        db_hostname.as_deref(),
    ) {
        return false;
    }
    let Some(container) = owned_service(rt, "db", true) else {
        return false;
    };
    let Some(database) = stack_database(&container) else {
        return false;
    };
    if spec.database.as_deref() != Some(database.as_str()) {
        return false;
    }
    let Some(id) = container.get("Id").and_then(Json::as_str) else {
        return false;
    };
    let Some(expected_id) = container_query(
        rt,
        id,
        &database,
        "SELECT system_identifier::text FROM pg_catalog.pg_control_system()",
    ) else {
        return false;
    };
    if expected_id.is_empty() {
        return false;
    }
    // A cloned cluster on a remote Docker host can share its system ID and private IPs with
    // a local server. Hold a uniquely named live probe connection and require the owned
    // container to see that exact backend, rather than inferring identity from static fields.
    let challenge = format!("mrsm-stack-{}", uuid::Uuid::new_v4().simple());
    let mut probe_spec = spec.clone();
    probe_spec
        .params
        .insert("application_name".into(), challenge.clone());
    let sql = "SELECT json_build_object('system_identifier', system_identifier::text, 'address', host(inet_server_addr()), 'port', inet_server_port(), 'database', current_database(), 'backend_pid', pg_backend_pid())::text FROM pg_catalog.pg_control_system()";
    let actual = session(rt).block_on(async {
        tokio::time::timeout(HINT_TIMEOUT, async {
            let connector =
                meerschaum_connector_sql::SqlConnector::connect("stack_identity", probe_spec)
                    .await
                    .ok()?;
            let actual = match connector.value(sql).await {
                Some(meerschaum_connector_sql::driver::SqlValue::Text(text)) => {
                    serde_json::from_str::<Json>(&text).ok()?
                }
                _ => return None,
            };
            Some((connector, actual))
        })
        .await
        .ok()
        .flatten()
    });
    let Some((_connection, actual)) = actual else {
        return false;
    };
    if !stack_endpoint_matches(&spec, &container, &expected_id, &actual) {
        return false;
    }
    let Some(pid) = actual.get("backend_pid").and_then(Json::as_u64) else {
        return false;
    };
    // Both interpolated fields originate here / from the server, never arbitrary user text.
    let proof_sql = format!(
        "SELECT coalesce(json_agg(json_build_object('backend_pid', pid, 'application_name', application_name)), '[]') FROM pg_catalog.pg_stat_activity WHERE pid = {pid} AND application_name = '{challenge}'"
    );
    let proof = container_query(rt, id, &database, &proof_sql)
        .and_then(|text| serde_json::from_str::<Json>(&text).ok());
    proof.is_some_and(|proof| live_backend_matches(&actual, &challenge, &proof))
}

fn container_query(rt: &mut Runtime<'_>, id: &str, database: &str, sql: &str) -> Option<String> {
    // Never inherit PGHOST: the owned container's loopback server is the one being checked.
    let probe = CommandSpec {
        program: "docker".into(),
        args: ["exec", id, "sh", "-c", "export PGHOST=127.0.0.1 PGUSER=\"${POSTGRES_USER:-postgres}\"; export PGPASSWORD=\"${POSTGRES_PASSWORD:-}\"; exec psql -X -q -At -v ON_ERROR_STOP=1 -d \"$1\" -c \"$2\"", "mrsm-stack-identity", database, sql].into_iter().map(str::to_string).collect(),
        quiet: true, ..CommandSpec::default()
    };
    let out = rt.host.output(&probe, Some(HINT_TIMEOUT)).ok()?;
    (out.code == 0 && !out.timed_out)
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn live_backend_matches(actual: &Json, challenge: &str, proof: &Json) -> bool {
    let Some(pid) = actual.get("backend_pid").and_then(Json::as_u64) else {
        return false;
    };
    proof.as_array().is_some_and(|rows| {
        rows.len() == 1
            && rows[0].get("backend_pid").and_then(Json::as_u64) == Some(pid)
            && rows[0].get("application_name").and_then(Json::as_str) == Some(challenge)
    })
}

fn stack_database(container: &Json) -> Option<String> {
    let env = container.pointer("/Config/Env")?.as_array()?;
    let value = |key: &str| {
        env.iter()
            .filter_map(Json::as_str)
            .filter_map(|s| s.split_once('='))
            .find_map(|(k, v)| (k == key && !v.is_empty()).then_some(v.to_string()))
    };
    Some(
        value("POSTGRES_DB")
            .or_else(|| value("POSTGRES_USER"))
            .unwrap_or_else(|| "postgres".into()),
    )
}

fn ip(value: &str) -> Option<IpAddr> {
    value
        .trim()
        .trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<IpAddr>()
        .ok()
        .map(|ip| ip.to_canonical())
}

/// Pure endpoint proof shared by classification regressions. Never infer server identity from
/// a connector label, a localhost hostname, or a Compose project name alone.
fn stack_endpoint_matches(
    spec: &ConnectionSpec,
    container: &Json,
    expected_id: &str,
    actual: &Json,
) -> bool {
    if actual.get("system_identifier").and_then(Json::as_str) != Some(expected_id)
        || actual.get("database").and_then(Json::as_str) != spec.database.as_deref()
        || stack_database(container).as_deref() != spec.database.as_deref()
    {
        return false;
    }
    let Some(address) = actual.get("address").and_then(Json::as_str).and_then(ip) else {
        return false;
    };
    let Some(server_port) = actual
        .get("port")
        .and_then(Json::as_u64)
        .and_then(|p| u16::try_from(p).ok())
    else {
        return false;
    };
    let networks = container
        .pointer("/NetworkSettings/Networks")
        .and_then(Json::as_object);
    if !networks.is_some_and(|networks| {
        networks.values().any(|network| {
            ["IPAddress", "GlobalIPv6Address"]
                .iter()
                .any(|key| network.get(key).and_then(Json::as_str).and_then(ip) == Some(address))
        })
    }) {
        return false;
    }
    let Some(host) = spec.host.as_deref() else {
        return false;
    };
    let port = spec.port.unwrap_or(5432);
    let local = host.eq_ignore_ascii_case("localhost")
        || ip(host).is_some_and(|address| address.is_loopback());
    if !local {
        return port == server_port;
    }
    let binding_key = format!("{server_port}/tcp");
    let bindings = container
        .pointer("/NetworkSettings/Ports")
        .and_then(|p| p.get(&binding_key))
        .and_then(Json::as_array);
    bindings.is_some_and(|bindings| {
        bindings.iter().any(|binding| {
            let published = binding
                .get("HostPort")
                .and_then(Json::as_str)
                .and_then(|p| p.parse::<u16>().ok());
            let bind = binding.get("HostIp").and_then(Json::as_str).and_then(ip);
            let host_ip = ip(host);
            published == Some(port)
                && bind.is_some_and(|bind| {
                    if host.eq_ignore_ascii_case("localhost") {
                        bind.is_unspecified() || bind.is_loopback()
                    } else {
                        Some(bind) == host_ip
                            || (bind.is_unspecified()
                                && host_ip.is_some_and(|host| host.is_ipv4() == bind.is_ipv4()))
                    }
                })
        })
    })
}

/// The config half of [`is_stack_instance`] (unit-tested): `sql:main`, a written compose file and
/// a local host.
pub fn stack_candidate(
    keys: &str,
    compose_exists: bool,
    host: Option<&str>,
    db_hostname: Option<&str>,
) -> bool {
    if full_keys(keys) != "sql:main" || !compose_exists {
        return false;
    }
    let Some(host) = host.map(|h| h.trim().trim_start_matches('[').trim_end_matches(']')) else {
        return false;
    };
    let host = host.to_ascii_lowercase();
    matches!(host.as_str(), "localhost" | "127.0.0.1" | "::1" | "db")
        || db_hostname.is_some_and(|d| !d.is_empty() && d.eq_ignore_ascii_case(&host))
}

/// The stack's Compose project name.
fn stack_project(rt: &mut Runtime<'_>) -> String {
    rt.config()
        .get(&["stack", "project_name"])
        .map(|v| v.py_str())
        .unwrap_or_else(|| "mrsm".into())
}

/// Read a single service container, accepting only a matching Compose origin from this root.
fn owned_service(rt: &mut Runtime<'_>, service: &str, running: bool) -> Option<Json> {
    let project = stack_project(rt);
    let compose_path = rt.config().paths().path("STACK_COMPOSE_PATH");
    let mut args: Vec<String> = vec![
        "ps".into(),
        "-q".into(),
        "--filter".into(),
        format!("label=com.docker.compose.project={project}"),
        "--filter".into(),
        format!("label=com.docker.compose.service={service}"),
    ];
    if running {
        args.extend(["--filter".into(), "status=running".into()]);
    } else {
        args.insert(1, "-a".into());
    }
    let spec = CommandSpec {
        program: "docker".into(),
        args,
        quiet: true,
        ..CommandSpec::default()
    };
    let out = rt.host.output(&spec, Some(HINT_TIMEOUT)).ok()?;
    if out.code != 0 || out.timed_out {
        return None;
    }
    let ids = String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .map(str::to_string)
        .collect::<Vec<_>>();
    if ids.len() != 1 {
        return None;
    }
    let spec = CommandSpec {
        program: "docker".into(),
        args: vec![
            "container".into(),
            "inspect".into(),
            "--format".into(),
            "{{json .}}".into(),
            ids[0].clone(),
        ],
        quiet: true,
        ..CommandSpec::default()
    };
    let out = rt.host.output(&spec, Some(HINT_TIMEOUT)).ok()?;
    if out.code != 0 || out.timed_out {
        return None;
    }
    let container: Json = serde_json::from_slice(&out.stdout).ok()?;
    container_owned(&container, &project, service, &compose_path, running).then_some(container)
}

fn container_owned(
    container: &Json,
    project: &str,
    service: &str,
    compose_path: &Path,
    running: bool,
) -> bool {
    let Some(labels) = container
        .pointer("/Config/Labels")
        .and_then(Json::as_object)
    else {
        return false;
    };
    let labels = labels
        .iter()
        .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string())))
        .collect::<std::collections::BTreeMap<_, _>>();
    labels.get("com.docker.compose.project").map(String::as_str) == Some(project)
        && labels.get("com.docker.compose.service").map(String::as_str) == Some(service)
        && (!running || container.pointer("/State/Running").and_then(Json::as_bool) == Some(true))
        && super::upgrade_postgres::created_from(&labels, compose_path)
}

/// Whether this root owns a container for `service`; foreign containers sharing the project
/// never qualify for automatic extension-related restarts.
pub fn stack_service_container(rt: &mut Runtime<'_>, service: &str, running: bool) -> bool {
    owned_service(rt, service, running).is_some()
}

/// What to do about a planned `pg_stat_statements` preload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreloadDecision {
    /// `-y` / `--force`: apply it without asking.
    Apply,
    /// Interactive: ask the separate question (default no).
    Ask,
    /// `--noask` / `MRSM_NOASK`: the default answer (no); print the SQL and the restart command.
    Skip,
}

/// The preload rule (user decision, 2026-10-18): `-y` / `--force` apply it, `--noask` keeps the
/// default (no), an interactive run asks (default no). `yes` includes `--force`; `noask`
/// includes `MRSM_NOASK`. `-y` wins over `--noask`, as in `yes_no`.
pub fn preload_decision(yes: bool, noask: bool) -> PreloadDecision {
    if yes {
        PreloadDecision::Apply
    } else if noask {
        PreloadDecision::Skip
    } else {
        PreloadDecision::Ask
    }
}

thread_local! {
    /// Set while `upgrade mrsm` brings the stack up and makes its own offer: `stack up` then
    /// leaves the hint to it (one line, not two).
    static SUPPRESS_STACK_HINT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Run `f` with the `stack up` hint suppressed.
pub fn without_stack_hint<T>(f: impl FnOnce() -> T) -> T {
    SUPPRESS_STACK_HINT.with(|c| c.set(true));
    let out = f();
    SUPPRESS_STACK_HINT.with(|c| c.set(false));
    out
}

/// Whether `stack up` should print its hint (not inside [`without_stack_hint`]).
pub fn stack_hint_enabled() -> bool {
    !SUPPRESS_STACK_HINT.with(|c| c.get())
}

/// Whether the stack config turns TimescaleDB's telemetry off (`TIMESCALEDB_TELEMETRY: 'off'`).
pub fn stack_telemetry_off(rt: &mut Runtime<'_>) -> bool {
    rt.config()
        .get(TELEMETRY_KEYS)
        .is_some_and(|v| v.py_str().trim().eq_ignore_ascii_case("off"))
}

/// The hint for one database's status: `None` when nothing can be updated.
pub fn hint_text(status: &DatabaseStatus, keys: &str) -> Option<String> {
    let lagging: Vec<String> = status
        .extensions
        .iter()
        .filter(|e| e.updatable())
        .map(|e| {
            format!(
                "{} {} -> {}",
                e.name,
                e.installed,
                e.available.as_deref().unwrap_or_default()
            )
        })
        .collect();
    if lagging.is_empty() {
        return None;
    }
    Some(format!(
        "Extensions in database '{}' are older than the server's ({}). \
         Run `mrsm upgrade extensions -i {}` to update them.",
        status.database,
        lagging.join(", "),
        full_keys(keys)
    ))
}

/// The hint for a connection spec, read within `timeout` (errors are swallowed: `None`).
pub fn spec_hint(
    session: &Session,
    spec: &ConnectionSpec,
    keys: &str,
    timeout: Duration,
) -> Option<String> {
    if !extensions::supports_extensions(spec.flavor) {
        return None;
    }
    // The bound also covers a runtime that is slow to schedule the probe.
    let status = session
        .block_on(async {
            tokio::time::timeout(
                timeout + Duration::from_millis(200),
                extensions::status(spec, None, timeout),
            )
            .await
        })
        .ok()?
        .ok()??;
    hint_text(&status, keys)
}

/// The hint for the instance `keys` (`None` for anything but a reachable PostgreSQL instance
/// with a lagging extension). Never fails and never waits longer than about `timeout`.
pub fn lag_hint(rt: &mut Runtime<'_>, keys: &str, timeout: Duration) -> Option<String> {
    let spec = postgres_spec(rt, keys).ok()??;
    let session = super::session(rt);
    spec_hint(&session, &spec, keys, timeout)
}

/// One row of the plan table.
struct PlanRow {
    database: String,
    extension: String,
    installed: String,
    available: String,
    statement: String,
}

fn render_plan(rt: &mut Runtime<'_>, rows: &[PlanRow], nopretty: bool) {
    if nopretty {
        let mut out = String::new();
        for r in rows {
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\n",
                r.database, r.extension, r.installed, r.available, r.statement
            ));
        }
        rt.print(&out);
        return;
    }
    let text =
        rt.with_formatter(|f| {
            let mut table = Table::new();
            table.box_chars = Some(HEAVY_HEAD);
            table.show_lines = true;
            table.header_style = crate::format::rich::style::StyleRef::Name("bold".into());
            for header in [
                "Database",
                "Extension",
                "Installed",
                "Available",
                "Statement",
            ] {
                table.add_column(Column {
                    header: crate::format::rich::text::Text::plain_text(header),
                    overflow: Overflow::Fold,
                    ..Column::default()
                });
            }
            for r in rows {
                let cell = |s: &str| {
                    Some(Box::new(crate::format::rich::text::Text::plain_text(s))
                        as Box<dyn Renderable>)
                };
                table.add_row(vec![
                    cell(&r.database),
                    cell(&r.extension),
                    cell(&r.installed),
                    cell(&r.available),
                    cell(&r.statement),
                ]);
            }
            f.console().print(&table)
        });
    rt.print(&text);
}

fn update_rows(update: &PlannedUpdate) -> Vec<PlanRow> {
    update
        .extensions
        .iter()
        .enumerate()
        .map(|(i, e)| PlanRow {
            database: update.database.clone(),
            extension: e.name.clone(),
            installed: e.installed.clone(),
            available: e.available.clone().unwrap_or_default(),
            // The PostGIS family shares one call: show it once.
            statement: if i == 0 {
                update.statement.clone()
            } else {
                "(same call)".into()
            },
        })
        .collect()
}

/// The server-level changes for the stack's database.
#[derive(Debug, Default)]
struct ServerPlan {
    /// The current `timescaledb.telemetry_level` when it must be turned off.
    telemetry_from: Option<String>,
    /// The databases (with TimescaleDB) whose telemetry job is unscheduled.
    telemetry_databases: Vec<String>,
    /// `ALTER SYSTEM SET shared_preload_libraries = ...` adding `pg_stat_statements`.
    preload: Option<String>,
}

/// What the stack's database needs at the server level (`stack_db`: the target is the stack's
/// database; `telemetry_off`: the stack config asks for it). Only for a superuser.
fn server_plan(statuses: &[DatabaseStatus], stack_db: bool, telemetry_off: bool) -> ServerPlan {
    let mut plan = ServerPlan::default();
    let Some(first) = statuses.first() else {
        return plan;
    };
    if !stack_db || !first.superuser {
        return plan;
    }
    if telemetry_off && let Some(on) = statuses.iter().find(|s| extensions::telemetry_on(s)) {
        plan.telemetry_from = on.telemetry_level.clone();
        plan.telemetry_databases = statuses
            .iter()
            .filter(|s| s.extension("timescaledb").is_some())
            .map(|s| s.database.clone())
            .collect();
    }
    if statuses
        .iter()
        .any(DatabaseStatus::pg_stat_statements_unloaded)
        && let Some(libs) = first.preload_libraries()
    {
        plan.preload = extensions::preload_statement(&libs, "pg_stat_statements");
    }
    plan
}

/// The databases to visit: the instance's own, then each named one (deduplicated).
fn target_databases(spec: &ConnectionSpec, named: &[String]) -> Vec<Option<String>> {
    let mut out: Vec<Option<String>> = vec![None];
    let own = spec.database.clone();
    let mut seen: Vec<String> = own.iter().cloned().collect();
    for name in named.iter().map(|n| n.trim()).filter(|n| !n.is_empty()) {
        if seen.iter().any(|s| s == name) {
            continue;
        }
        seen.push(name.to_string());
        out.push(Some(name.to_string()));
    }
    out
}

/// `upgrade extensions [databases ...] [-i <instance>]`.
pub fn upgrade_extensions(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let keys = full_keys(&instance_keys(args, rt));
    let yes = flag(args, "yes") || flag(args, "force");
    let noask = flag(args, "noask");
    let nopretty = flag(args, "nopretty");
    let spec = match postgres_spec(rt, &keys) {
        Ok(Some(spec)) => spec,
        Ok(None) => {
            return Ok(SuccessTuple::ok(format!(
                "Nothing to update: '{keys}' is not a PostgreSQL instance, so it has no \
                 extensions."
            )));
        }
        Err(e) => return Ok(SuccessTuple::fail(e)),
    };
    let session = super::session(rt);
    let mut failures: Vec<String> = Vec::new();
    let mut statuses: Vec<DatabaseStatus> = Vec::new();
    for database in target_databases(&spec, &words(args, "action")) {
        let label = database
            .clone()
            .or_else(|| spec.database.clone())
            .unwrap_or_default();
        match session.block_on(extensions::status(
            &spec,
            database.as_deref(),
            ACTION_TIMEOUT,
        )) {
            Ok(Some(status)) => statuses.push(status),
            Ok(None) => {}
            Err(e) => failures.push(format!("Could not read the extensions of '{label}': {e}")),
        }
    }
    if statuses.is_empty() {
        return Ok(SuccessTuple::fail(failures.join("\n")));
    }

    let stack_db = is_stack_instance(rt, &keys);
    let telemetry_off = stack_telemetry_off(rt);
    let server = server_plan(&statuses, stack_db, telemetry_off);
    let updates: Vec<PlannedUpdate> = statuses.iter().flat_map(extensions::plan).collect();
    let mut skipped: Vec<String> = Vec::new();
    for status in &statuses {
        for e in extensions::not_updatable(status) {
            skipped.push(format!(
                "'{}' in '{}': no update path from {} to {} on this server.",
                e.name,
                status.database,
                e.installed,
                e.available.as_deref().unwrap_or_default()
            ));
        }
    }

    let mut rows: Vec<PlanRow> = updates.iter().flat_map(update_rows).collect();
    if let Some(from) = &server.telemetry_from {
        rows.push(PlanRow {
            database: "(server)".into(),
            extension: "timescaledb.telemetry_level".into(),
            installed: from.clone(),
            available: "off".into(),
            statement: TELEMETRY_OFF_STATEMENTS.join("\n"),
        });
        for database in &server.telemetry_databases {
            rows.push(PlanRow {
                database: database.clone(),
                extension: "timescaledb (telemetry job)".into(),
                installed: "scheduled".into(),
                available: "unscheduled".into(),
                statement: UNSCHEDULE_TELEMETRY_JOB.into(),
            });
        }
    }
    for line in &skipped {
        warn(rt, &format!("Skipping {line}"));
    }
    let databases: Vec<String> = statuses
        .iter()
        .map(|s| format!("'{}'", s.database))
        .collect();
    if rows.is_empty() && server.preload.is_none() {
        let mut message = format!(
            "Every extension is up to date in {} on '{keys}'.",
            databases.join(", ")
        );
        if !skipped.is_empty() {
            message = format!("Nothing to update on '{keys}' ({} skipped).", skipped.len());
        }
        if !failures.is_empty() {
            return Ok(SuccessTuple::fail(format!(
                "{message}\n{}",
                failures.join("\n")
            )));
        }
        return Ok(SuccessTuple::ok(message));
    }

    let mut run_main = false;
    if !rows.is_empty() {
        render_plan(rt, &rows, nopretty);
        run_main = yes_no(
            rt,
            &format!("Update the extensions above on '{keys}'?"),
            "y",
            yes,
            noask,
        )?;
    }
    let mut run_preload = false;
    if let Some(statement) = &server.preload {
        let decision = preload_decision(yes, super::check_noask(rt, noask));
        if decision == PreloadDecision::Apply {
            run_preload = true;
        } else if decision == PreloadDecision::Ask {
            info(
                rt,
                &format!(
                    "`pg_stat_statements` is installed but not in `shared_preload_libraries`, \
                     so its view cannot be read. Adding it takes effect after \
                     `mrsm stack restart db`:\n{statement}"
                ),
            );
            run_preload = yes_no(
                rt,
                "Add `pg_stat_statements` to `shared_preload_libraries`?",
                "n",
                false,
                false,
            )?;
        } else {
            info(
                rt,
                &format!(
                    "`pg_stat_statements` is installed but not in `shared_preload_libraries`, \
                     so its view cannot be read. Not changing it (`--noask` keeps the default \
                     answer, no): to preload it, run as a superuser:\n{statement}\n\
                     then restart the database: `mrsm stack restart db`."
                ),
            );
        }
    }
    if !run_main && !run_preload {
        if rows.is_empty() {
            return Ok(SuccessTuple::ok(format!(
                "Every extension is up to date in {} on '{keys}'; \
                 `shared_preload_libraries` was not changed.",
                databases.join(", ")
            )));
        }
        return Ok(SuccessTuple::fail("Nothing was updated."));
    }

    let mut updated = 0usize;
    let mut timescaledb_updated = false;
    let mut failed_updates: Vec<(String, String)> = Vec::new();
    if run_main {
        for update in &updates {
            match session.block_on(extensions::apply(&spec, update)) {
                Ok(()) => {
                    if update.kind == UpdateKind::Timescaledb {
                        timescaledb_updated = true;
                    }
                }
                Err(e) => {
                    failed_updates.push((update.database.clone(), update.name().to_string()));
                    failures.push(format!(
                        "Failed to update '{}' in '{}': {}\n    Run as a superuser or the \
                     extension's owner, connected to database '{}':\n    {}",
                        update.name(),
                        update.database,
                        e.to_string().trim(),
                        update.database,
                        update.statement
                    ))
                }
            }
        }
        if server.telemetry_from.is_some() {
            for statement in TELEMETRY_OFF_STATEMENTS {
                if let Err(e) =
                    session.block_on(extensions::run_first_statement(&spec, None, statement))
                {
                    failures.push(format!(
                        "Failed to run `{statement}`: {}",
                        e.to_string().trim()
                    ));
                }
            }
            for database in &server.telemetry_databases {
                if let Err(e) = session.block_on(extensions::run_first_statement(
                    &spec,
                    Some(database),
                    UNSCHEDULE_TELEMETRY_JOB,
                )) {
                    failures.push(format!(
                        "Failed to unschedule the telemetry job in '{database}': {}",
                        e.to_string().trim()
                    ));
                }
            }
        }
    }
    let mut preload_done = false;
    if run_preload && let Some(statement) = &server.preload {
        match session.block_on(extensions::run_first_statement(&spec, None, statement)) {
            Ok(()) => {
                preload_done = true;
                info(
                    rt,
                    "Added `pg_stat_statements` to `shared_preload_libraries`. It takes effect \
                     after the database restarts: run `mrsm stack restart db`.",
                );
            }
            Err(e) => failures.push(format!(
                "Failed to add `pg_stat_statements` to `shared_preload_libraries`: {}",
                e.to_string().trim()
            )),
        }
    }

    // Count what the databases report now (an update that ran but left a version behind is a
    // failure too).
    if run_main {
        for update in &updates {
            let after = session.block_on(extensions::status(
                &spec,
                Some(&update.database),
                ACTION_TIMEOUT,
            ));
            let Ok(Some(after)) = after else {
                continue;
            };
            if failed_updates.contains(&(update.database.clone(), update.name().to_string())) {
                continue;
            }
            for e in &update.extensions {
                match after.extension(&e.name) {
                    Some(now) if !now.lags() => updated += 1,
                    Some(now) => failures.push(format!(
                        "'{}' in '{}' is still {} (the server ships {}).",
                        e.name,
                        update.database,
                        now.installed,
                        now.available.as_deref().unwrap_or_default()
                    )),
                    None => {}
                }
            }
        }
    }

    if timescaledb_updated {
        // The stack's services that hold pooled sessions (which keep the old library loaded).
        let restart_stack = stack_db && is_stack_instance(rt, &keys);
        for (service, label) in [("api", "API"), ("grafana", "Grafana")] {
            if restart_stack && stack_service_container(rt, service, true) {
                info(
                    rt,
                    &format!(
                        "Restarting the stack's {label} so its sessions load the new \
                         TimescaleDB version."
                    ),
                );
                super::packages::run_stack(rt, &["restart", service], flag(args, "debug"));
            }
        }
        info(
            rt,
            "TimescaleDB was updated: restart long-running jobs and API servers that use this \
             database elsewhere, so their sessions load the new version.",
        );
    }

    let failed = failures.len();
    let mut summary = format!(
        "Updated {updated} extension{}, failed {failed}, skipped {} on '{keys}'.",
        plural(updated),
        skipped.len()
    );
    if server.telemetry_from.is_some() && run_main {
        summary.push_str("\nTimescaleDB telemetry is off.");
    }
    if preload_done {
        summary.push_str("\n`pg_stat_statements` is preloaded after `mrsm stack restart db`.");
    }
    if failures.is_empty() {
        Ok(SuccessTuple::ok(summary))
    } else {
        Ok(SuccessTuple::fail(format!(
            "{summary}\n{}",
            failures.join("\n")
        )))
    }
}

/// The `extensions` checks of `mrsm doctor` for one database's status (`Ok(None)`: not a
/// PostgreSQL instance). `stack_db`: the instance is the stack's database; `telemetry_off`: the
/// stack config sets `TIMESCALEDB_TELEMETRY: 'off'`.
pub fn doctor_checks(
    status: &Result<Option<DatabaseStatus>, String>,
    keys: &str,
    stack_db: bool,
    telemetry_off: bool,
) -> Vec<super::doctor::Check> {
    use super::doctor::Check;
    const G: &str = "extensions";
    let keys = full_keys(keys);
    let command = format!("mrsm upgrade extensions -i {keys}");
    let status = match status {
        Ok(Some(status)) => status,
        Ok(None) => {
            return vec![Check::skip(
                G,
                "postgresql",
                format!("'{keys}' is not a PostgreSQL instance"),
            )];
        }
        Err(e) => {
            return vec![Check::warn(
                G,
                "postgresql",
                format!("could not read the extensions of '{keys}': {e}"),
                "check that the database is reachable, then run `mrsm doctor` again",
            )];
        }
    };
    let mut checks = Vec::new();
    let lagging: Vec<String> = status
        .extensions
        .iter()
        .filter(|e| e.updatable())
        .map(|e| {
            format!(
                "{} {} -> {}",
                e.name,
                e.installed,
                e.available.as_deref().unwrap_or_default()
            )
        })
        .collect();
    if lagging.is_empty() {
        let names: Vec<&str> = status.extensions.iter().map(|e| e.name.as_str()).collect();
        checks.push(Check::pass(
            G,
            "versions",
            format!(
                "every extension in '{}' is at the server's version ({})",
                status.database,
                names.join(", ")
            ),
        ));
    } else {
        checks.push(Check::warn(
            G,
            "versions",
            format!(
                "older than the server's in '{}': {}",
                status.database,
                lagging.join(", ")
            ),
            command.clone(),
        ));
    }
    for e in extensions::not_updatable(status) {
        checks.push(Check::warn(
            G,
            e.name.clone(),
            format!(
                "installed {} but the server ships {} with no update path",
                e.installed,
                e.available.as_deref().unwrap_or_default()
            ),
            "install an image that ships an update path for this version",
        ));
    }
    if stack_db && telemetry_off && status.extension("timescaledb").is_some() {
        if extensions::telemetry_on(status) {
            checks.push(Check::warn(
                G,
                "telemetry",
                format!(
                    "timescaledb.telemetry_level is '{}' although the stack config sets \
                     TIMESCALEDB_TELEMETRY: 'off'",
                    status.telemetry_level.as_deref().unwrap_or_default()
                ),
                command.clone(),
            ));
        } else {
            checks.push(Check::pass(G, "telemetry", "TimescaleDB telemetry is off"));
        }
    }
    if status.pg_stat_statements_unloaded() {
        let fix = if stack_db {
            format!("{command} (then `mrsm stack restart db`)")
        } else {
            "add pg_stat_statements to shared_preload_libraries and restart the server".into()
        };
        checks.push(Check::warn(
            G,
            "pg_stat_statements",
            "installed but not in shared_preload_libraries, so its view cannot be read",
            fix,
        ));
    }
    checks
}

/// Arguments for running `upgrade extensions` from another action.
pub fn upgrade_args(keys: &str, yes: bool, noask: bool, debug: bool) -> ArgsMap {
    let mut args = ArgsMap::new();
    args.insert("action".into(), PyValue::str_list(&[] as &[String]));
    args.insert("mrsm_instance".into(), PyValue::Str(keys.to_string()));
    args.insert("yes".into(), PyValue::Bool(yes));
    args.insert("noask".into(), PyValue::Bool(noask));
    args.insert("debug".into(), PyValue::Bool(debug));
    args
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_connector_sql::extensions::ExtensionStatus;

    fn ext(name: &str, installed: &str, available: &str) -> ExtensionStatus {
        ExtensionStatus {
            name: name.into(),
            installed: installed.into(),
            available: Some(available.into()),
            schema: None,
            owner: None,
            owned: true,
            update_path: (installed != available).then_some(true),
        }
    }

    fn status(db: &str, extensions: Vec<ExtensionStatus>) -> DatabaseStatus {
        DatabaseStatus {
            database: db.into(),
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
    fn hint_names_the_lagging_extensions_and_the_command() {
        let s = status(
            "meerschaum",
            vec![
                ext("timescaledb", "2.29.2", "2.30.2"),
                ext("postgis", "3.6.4", "3.6.4"),
            ],
        );
        let hint = hint_text(&s, "sql").unwrap();
        assert!(hint.contains("timescaledb 2.29.2 -> 2.30.2"), "{hint}");
        assert!(!hint.contains("postgis"), "{hint}");
        assert!(
            hint.contains("`mrsm upgrade extensions -i sql:main`"),
            "{hint}"
        );
    }

    #[test]
    fn no_hint_when_nothing_can_be_updated() {
        let s = status("db", vec![ext("timescaledb", "2.30.2", "2.30.2")]);
        assert_eq!(hint_text(&s, "sql:main"), None);
        let mut e = ext("timescaledb", "2.31.0", "2.30.2");
        e.update_path = Some(false);
        assert_eq!(hint_text(&status("db", vec![e]), "sql:main"), None);
    }

    #[test]
    fn server_plan_only_for_the_stack_db_and_a_superuser() {
        let s = vec![
            status(
                "meerschaum",
                vec![
                    ext("timescaledb", "2.30.2", "2.30.2"),
                    ext("pg_stat_statements", "1.12", "1.12"),
                ],
            ),
            status("template1", vec![]),
        ];
        let p = server_plan(&s, true, true);
        assert_eq!(p.telemetry_from.as_deref(), Some("basic"));
        assert_eq!(p.telemetry_databases, vec!["meerschaum".to_string()]);
        assert_eq!(
            p.preload.as_deref(),
            Some(
                "ALTER SYSTEM SET shared_preload_libraries = 'timescaledb', 'pg_textsearch', \
                 'pg_stat_statements';"
            )
        );
        // Not the stack's database: nothing at the server level.
        let p = server_plan(&s, false, true);
        assert!(p.telemetry_from.is_none() && p.preload.is_none());
        // The stack config does not ask for telemetry off.
        assert!(server_plan(&s, true, false).telemetry_from.is_none());
        // Not a superuser.
        let mut s2 = s.clone();
        s2[0].superuser = false;
        let p = server_plan(&s2, true, true);
        assert!(p.telemetry_from.is_none() && p.preload.is_none());
        // Already off.
        let mut s3 = s.clone();
        s3[0].telemetry_level = Some("off".into());
        assert!(server_plan(&s3, true, true).telemetry_from.is_none());
    }

    #[test]
    fn doctor_warns_on_lag_telemetry_and_preload() {
        use crate::native::doctor::Status;
        let s = status(
            "meerschaum",
            vec![
                ext("timescaledb", "2.29.2", "2.30.2"),
                ext("pg_stat_statements", "1.12", "1.12"),
            ],
        );
        let checks = doctor_checks(&Ok(Some(s.clone())), "sql:main", true, true);
        let by = |name: &str| checks.iter().find(|c| c.name == name).unwrap().clone();
        assert_eq!(by("versions").status, Status::Warn);
        assert_eq!(
            by("versions").fix.as_deref(),
            Some("mrsm upgrade extensions -i sql:main")
        );
        assert_eq!(by("telemetry").status, Status::Warn);
        assert_eq!(by("pg_stat_statements").status, Status::Warn);
        // Not the stack's database: no telemetry check.
        let checks = doctor_checks(&Ok(Some(s.clone())), "sql:other", false, true);
        assert!(checks.iter().all(|c| c.name != "telemetry"));
        // Up to date, telemetry off, preloaded: all pass.
        let mut ok = status("meerschaum", vec![ext("timescaledb", "2.30.2", "2.30.2")]);
        ok.telemetry_level = Some("off".into());
        let checks = doctor_checks(&Ok(Some(ok)), "sql:main", true, true);
        assert!(
            checks.iter().all(|c| c.status == Status::Pass),
            "{checks:?}"
        );
        // Not PostgreSQL / unreachable.
        assert_eq!(
            doctor_checks(&Ok(None), "sql:local", false, false)[0].status,
            Status::Skip
        );
        assert_eq!(
            doctor_checks(&Err("timeout".into()), "sql:main", true, true)[0].status,
            Status::Warn
        );
    }

    fn owned_db() -> Json {
        serde_json::json!({
            "Id": "scratch-db-1", "State": {"Running": true},
            "Config": {"Env": ["POSTGRES_USER=mrsm", "POSTGRES_DB=meerschaum"], "Labels": {
                "com.docker.compose.project": "scratch",
                "com.docker.compose.service": "db",
                "com.docker.compose.project.config_files": "/scratch/stack/docker-compose.yaml"
            }},
            "NetworkSettings": {
                "Ports": {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "55432"}]},
                "Networks": {"scratch_backend": {"IPAddress": "192.0.2.2", "GlobalIPv6Address": "2001:db8::2"}}
            }
        })
    }

    fn endpoint() -> Json {
        serde_json::json!({"system_identifier": "123456", "address": "192.0.2.2", "port": 5432, "database": "meerschaum"})
    }

    #[test]
    fn stack_origin_must_identify_this_root_and_each_restarted_service() {
        let path = Path::new("/scratch/stack/docker-compose.yaml");
        let owned = owned_db();
        assert!(container_owned(&owned, "scratch", "db", path, true));
        let mut foreign = owned.clone();
        foreign["Config"]["Labels"]["com.docker.compose.project.config_files"] =
            Json::from("/foreign/stack/docker-compose.yaml");
        assert!(!container_owned(&foreign, "scratch", "db", path, true));
        assert!(!container_owned(
            &owned,
            "another-project",
            "db",
            path,
            true
        ));
        assert!(!container_owned(&owned, "scratch", "api", path, true));
        // A foreign API or Grafana container is refused even when the db itself is owned.
        for service in ["api", "grafana"] {
            foreign["Config"]["Labels"]["com.docker.compose.service"] = Json::from(service);
            assert!(!container_owned(&foreign, "scratch", service, path, true));
        }
    }

    #[test]
    fn stack_endpoint_requires_the_published_port_database_identity_and_container_address() {
        let container = owned_db();
        let spec =
            ConnectionSpec::from_uri("postgresql://u:p@127.0.0.1:55432/meerschaum", None, None)
                .unwrap();
        let actual = endpoint();
        assert!(stack_endpoint_matches(&spec, &container, "123456", &actual));
        let mut bad = spec.clone();
        bad.port = Some(55433);
        assert!(!stack_endpoint_matches(&bad, &container, "123456", &actual));
        bad = spec.clone();
        bad.database = Some("another_db".into());
        assert!(!stack_endpoint_matches(&bad, &container, "123456", &actual));
        for (key, value) in [
            ("system_identifier", Json::from("foreign-server")),
            ("address", Json::from("192.0.2.3")),
            ("database", Json::from("another_db")),
            ("port", Json::from(5433)),
        ] {
            let mut foreign = actual.clone();
            foreign[key] = value;
            let classified = stack_endpoint_matches(&spec, &container, "123456", &foreign);
            assert!(!classified, "{key}");
            let plan = server_plan(
                &[status(
                    "meerschaum",
                    vec![ext("timescaledb", "2.29.2", "2.30.2")],
                )],
                classified,
                true,
            );
            assert!(plan.telemetry_from.is_none() && plan.preload.is_none());
        }
        // Same identifier is insufficient for a cloned cluster at another endpoint.
        let mut clone = actual.clone();
        clone["address"] = Json::from("192.0.2.99");
        assert!(!stack_endpoint_matches(&spec, &container, "123456", &clone));
    }

    #[test]
    fn a_remote_clone_with_the_same_static_identity_must_observe_the_live_nonce() {
        let container = owned_db();
        let spec =
            ConnectionSpec::from_uri("postgresql://u:p@127.0.0.1:55432/meerschaum", None, None)
                .unwrap();
        let mut actual = endpoint();
        actual["backend_pid"] = Json::from(1234);
        // Two hosts can have identical private IPs, DB name, port and physical-cluster ID.
        assert!(stack_endpoint_matches(&spec, &container, "123456", &actual));
        assert!(!live_backend_matches(
            &actual,
            "unique-session-proof",
            &serde_json::json!([])
        ));
        assert!(!live_backend_matches(
            &actual,
            "unique-session-proof",
            &serde_json::json!([{"backend_pid": 1234, "application_name": "another-session"}])
        ));
        assert!(!live_backend_matches(
            &actual,
            "unique-session-proof",
            &serde_json::json!([{"backend_pid": 9999, "application_name": "unique-session-proof"}])
        ));
        assert!(live_backend_matches(
            &actual,
            "unique-session-proof",
            &serde_json::json!([{"backend_pid": 1234, "application_name": "unique-session-proof"}])
        ));
    }

    #[test]
    fn in_network_endpoints_use_the_internal_port_and_ipv6_bindings_match_address_family() {
        let mut container = owned_db();
        let mut spec =
            ConnectionSpec::from_uri("postgresql://u:p@db:5432/meerschaum", None, None).unwrap();
        assert!(stack_endpoint_matches(
            &spec,
            &container,
            "123456",
            &endpoint()
        ));
        spec.port = Some(55432);
        assert!(!stack_endpoint_matches(
            &spec,
            &container,
            "123456",
            &endpoint()
        ));
        spec.host = Some("::1".into());
        container["NetworkSettings"]["Ports"]["5432/tcp"] =
            serde_json::json!([{"HostIp": "::", "HostPort": "55432"}]);
        let mut actual = endpoint();
        actual["address"] = Json::from("2001:db8::2");
        assert!(stack_endpoint_matches(&spec, &container, "123456", &actual));
        spec.host = Some("127.0.0.1".into());
        assert!(!stack_endpoint_matches(
            &spec, &container, "123456", &actual
        ));
    }

    #[test]
    fn the_stack_database_is_local_and_has_a_compose_file() {
        assert!(stack_candidate(
            "sql:main",
            true,
            Some("localhost"),
            Some("db")
        ));
        assert!(stack_candidate("sql", true, Some("127.0.0.1"), Some("db")));
        assert!(stack_candidate("sql:main", true, Some("[::1]"), None));
        // Inside the stack's network (the `api` service): the db hostname.
        assert!(stack_candidate("sql:main", true, Some("db"), Some("db")));
        assert!(stack_candidate("sql:main", true, Some("pg"), Some("pg")));
        // A remote server is never the stack, even with a compose file written long ago.
        assert!(!stack_candidate(
            "sql:main",
            true,
            Some("db.example.com"),
            Some("db")
        ));
        assert!(!stack_candidate(
            "sql:main",
            true,
            Some("10.0.0.5"),
            Some("db")
        ));
        assert!(!stack_candidate("sql:main", true, None, Some("db")));
        // No compose file, or another instance.
        assert!(!stack_candidate(
            "sql:main",
            false,
            Some("localhost"),
            Some("db")
        ));
        assert!(!stack_candidate(
            "sql:other",
            true,
            Some("localhost"),
            Some("db")
        ));
    }

    #[test]
    fn yes_applies_the_preload_noask_skips_it_interactive_asks() {
        // `-y` (and `--force`, which the action folds into `yes`).
        assert_eq!(preload_decision(true, false), PreloadDecision::Apply);
        // `--noask` / `MRSM_NOASK`: the default answer, no.
        assert_eq!(preload_decision(false, true), PreloadDecision::Skip);
        // Interactive: the separate question (default no).
        assert_eq!(preload_decision(false, false), PreloadDecision::Ask);
        // `-y` wins over `--noask`, as in `yes_no`.
        assert_eq!(preload_decision(true, true), PreloadDecision::Apply);
    }

    #[test]
    fn the_stack_hint_can_be_suppressed() {
        assert!(stack_hint_enabled());
        let inside = without_stack_hint(stack_hint_enabled);
        assert!(!inside);
        assert!(stack_hint_enabled());
    }

    #[test]
    fn targets_the_own_database_then_named_ones_once() {
        let spec =
            ConnectionSpec::from_uri("postgresql://u:p@localhost:5432/meerschaum", None, None)
                .unwrap();
        let named = vec![
            "template1".to_string(),
            "meerschaum".to_string(),
            "template1".to_string(),
            "postgres".to_string(),
        ];
        assert_eq!(
            target_databases(&spec, &named),
            vec![None, Some("template1".into()), Some("postgres".into())]
        );
        assert_eq!(target_databases(&spec, &[]), vec![None]);
    }
}
