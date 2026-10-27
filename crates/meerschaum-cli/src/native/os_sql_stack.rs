//! The process-launching actions `os`, `sql` and `stack` (`meerschaum/actions/{os,sql,stack}.py`).
//!
//! - `os <command ...>` runs a subprocess and returns its captured output.
//! - `sql` reads / executes queries on a `sql:` connector, or opens the flavor's interactive
//!   client (Rainfrog first, configurable per connector/flavor, then the Python and plain
//!   clients; `--nopretty` forces the plain client; nothing is installed implicitly).
//! - `stack <docker compose args>` writes the Docker Compose stack project from the `stack`
//!   config and runs `docker compose` against it.

use std::time::Duration;

use meerschaum_core::config::paths::Paths;
use meerschaum_core::value::{Map, Value};
use meerschaum_core::{SuccessTuple, pyfmt};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::{CommandSpec, Runtime};
use crate::format::rich::pretty::PyObj;
use crate::format::{TupleArgs, format_dataframe};
use crate::value::ArgsMap;

use super::{Instance, flag, pprint, print_tuple, session, warn, words, yes_no};

/// The exception text Python raises when `Popen` cannot start `program`.
fn spawn_error(program: &str, error: &std::io::Error) -> ActionError {
    let quoted = pyfmt::str_repr(program);
    match error.kind() {
        std::io::ErrorKind::NotFound => ActionError::new(
            "FileNotFoundError",
            format!("[Errno 2] No such file or directory: {quoted}"),
        ),
        std::io::ErrorKind::PermissionDenied => ActionError::new(
            "PermissionError",
            format!("[Errno 13] Permission denied: {quoted}"),
        ),
        _ => ActionError::new("OSError", error.to_string()),
    }
}

/// `os <command ...>`: run the command and print its output (`shell=False`).
pub fn os(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail(
            crate::action_docs::docstring("os").unwrap_or_default(),
        ));
    }
    let mut command = action;
    command.extend(words(args, "sub_args"));
    let timeout = rt
        .config()
        .get(&["shell", "timeout"])
        .and_then(|v| pyfmt::py_int(&v).map(|n| (n, v.py_str())));
    let spec = CommandSpec {
        program: command[0].clone(),
        args: command[1..].to_vec(),
        ..CommandSpec::default()
    };
    let out = rt
        .host
        .output(
            &spec,
            timeout
                .as_ref()
                .map(|(n, _)| Duration::from_secs((*n).max(0) as u64)),
        )
        .map_err(|e| spawn_error(&command[0], &e))?;
    if out.timed_out {
        let shown = PyObj::List(command.iter().map(|c| PyObj::Str(c.clone())).collect()).repr();
        let seconds = timeout.map(|(_, s)| s).unwrap_or_default();
        rt.print(&format!(
            "Command '{shown}' timed out after {seconds} seconds\n"
        ));
    }
    let output = String::from_utf8_lossy(&out.stdout).into_owned();
    let error_output = String::from_utf8_lossy(&out.stderr).into_owned();
    rt.print(&output);
    if out.code != 0 {
        return Ok(SuccessTuple::fail(error_output));
    }
    Ok(SuccessTuple::ok(output))
}

/// The interactive client each flavor opens (`flavor_clis`).
fn flavor_cli(flavor: &str) -> Option<&'static str> {
    Some(match flavor {
        "postgresql" | "postgis" | "timescaledb" | "timescaledb-ha" | "cockroachdb" | "citus" => {
            "pgcli"
        }
        "mysql" | "mariadb" | "percona" => "mycli",
        "sqlite" | "geopackage" => "litecli",
        "mssql" => "mssqlcli",
        "duckdb" => "gadwall",
        _ => return None,
    })
}

/// The plain client used when the `cli-tools` program is not on `PATH`.
fn fallback_cli(flavor: &str) -> Option<&'static str> {
    Some(match flavor {
        "postgresql" | "postgis" | "timescaledb" | "timescaledb-ha" | "cockroachdb" | "citus" => {
            "psql"
        }
        "mysql" | "mariadb" | "percona" => "mysql",
        "sqlite" | "geopackage" => "sqlite3",
        "mssql" => "sqlcmd",
        "duckdb" => "duckdb",
        _ => return None,
    })
}

/// `SQLConnector.cli()`: launch the flavor's interactive client.
pub fn open_cli(rt: &mut Runtime<'_>, label: &str) -> ActionResult {
    open_cli_with_options(rt, label, false)
}

fn rainfrog_driver(flavor: &str) -> Option<&'static str> {
    match flavor {
        "postgresql" | "postgis" | "timescaledb" | "timescaledb-ha" | "cockroachdb" | "citus" => {
            Some("postgres")
        }
        "mysql" | "mariadb" | "percona" => Some("mysql"),
        "sqlite" | "geopackage" => Some("sqlite"),
        "duckdb" => Some("duckdb"),
        "oracle" => Some("oracle"),
        _ => None,
    }
}

fn open_cli_with_options(rt: &mut Runtime<'_>, label: &str, nopretty: bool) -> ActionResult {
    let keys = format!("sql:{label}");
    let failed = |message: String| {
        Ok(SuccessTuple::fail(format!(
            "[{keys}] Failed to start CLI:\n{message}"
        )))
    };
    // The connection attributes only: the client connects on its own, so an unreachable
    // database is the client's message, not ours.
    let attrs = super::session::connector_attributes(rt.config(), "sql", label);
    if attrs.is_empty() {
        return failed(format!("'{keys}' is not a SQL connector."));
    }
    let spec = match super::connectors::sql_spec(label, &attrs) {
        Ok(spec) => spec,
        Err(message) => return failed(message),
    };
    let flavor = spec.flavor.to_string();
    let configured = attrs
        .get("cli")
        .and_then(Value::as_str)
        .map(str::to_string)
        .or_else(|| {
            rt.config()
                .get(&["system", "connectors", "sql", "cli", &flavor])
                .and_then(|v| v.as_str().map(str::to_string))
        });
    let mut candidates = Vec::new();
    if !nopretty {
        if let Some(preferred) = configured.filter(|s| !s.is_empty()) {
            candidates.push(preferred);
        } else if rainfrog_driver(&flavor).is_some() {
            candidates.push("rainfrog".into());
        }
        if let Some(python) = flavor_cli(&flavor) {
            candidates.push(python.into());
        }
    }
    if let Some(plain) = fallback_cli(&flavor) {
        candidates.push(plain.into());
    }
    let path = rt.config().paths();
    let selected = candidates
        .iter()
        .find_map(|name| find_program(rt, name, &path).map(|program| (name.clone(), program)));
    let Some((cli, program)) = selected else {
        return failed(format!(
            "Cannot find a SQL client for '{flavor}' (tried {}). Install Rainfrog with \
             `cargo install rainfrog --locked`, install `meerschaum[cli-tools]`, or add the \
             plain flavor client to PATH.",
            candidates.join(", ")
        ));
    };
    let is_file = matches!(flavor.as_str(), "sqlite" | "duckdb" | "geopackage");
    let database = spec
        .resolved_database()
        .ok()
        .flatten()
        .or(spec.database.clone());
    let mut password: Option<String> = spec.password.clone();
    let target = if is_file {
        match database {
            Some(d) if !d.is_empty() => d,
            _ => {
                return failed(format!(
                    "Cannot determine database from connector '{keys}'."
                ));
            }
        }
    } else {
        let url = spec
            .engine_str()
            .map(|u| strip_driver(&u))
            .unwrap_or_default();
        // The password never goes on the command line (`ps` shows it): the client reads it
        // from its environment variable.
        let (url, from_url) = split_password(&url);
        password = password.take().or(from_url);
        url
    };
    let args = match cli.as_str() {
        "rainfrog" => {
            let Some(driver) = rainfrog_driver(&flavor) else {
                return failed(format!("Rainfrog does not support flavor '{flavor}'."));
            };
            vec!["--driver".into(), driver.into()]
        }
        "mysql" => mysql_args(&spec),
        "sqlcmd" => mssql_args(&spec),
        _ => vec![target.clone()],
    };
    let mut command = CommandSpec {
        program,
        args,
        ..CommandSpec::default()
    };
    if cli == "rainfrog" {
        // Rainfrog reads DATABASE_URL itself. Keep the full URL (including TLS/query options
        // and credentials) out of argv; do not inherit an unrelated DATABASE_URL.
        let url = if is_file {
            format!("{}://{target}", rainfrog_driver(&flavor).unwrap())
        } else {
            strip_driver(
                &spec
                    .engine_str()
                    .map_err(|e| ActionError::new("ValueError", e.to_string()))?,
            )
        };
        command.env.push(("DATABASE_URL".into(), url));
    }
    if let Some(password) = password.filter(|p| !p.is_empty()) {
        for name in password_variables(&flavor) {
            command.env.push((name.to_string(), password.clone()));
        }
    }
    if flavor == "mssql" && (rt.env)("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT").is_none() {
        command
            .env
            .push(("DOTNET_SYSTEM_GLOBALIZATION_INVARIANT".into(), "1".into()));
    }
    match rt.host.run(&command) {
        Ok(0) => {}
        Ok(code) => return failed(format!("SQL client exited with status {code}.")),
        Err(e) => return failed(e.to_string()),
    }
    let text = rt.with_formatter(|f| {
        f.pprint(
            &PyObj::Tuple(vec![PyObj::Bool(true), PyObj::Str("Success".into())]),
            false,
        )
    });
    rt.print(&text);
    Ok(SuccessTuple::ok("Success"))
}

/// The environment variables the flavor's clients read the password from (`pgcli`/`psql`,
/// `mycli`/`mysql`, `mssqlcli`/`sqlcmd`).
fn password_variables(flavor: &str) -> &'static [&'static str] {
    match flavor {
        "mysql" | "mariadb" | "percona" => &["MYSQL_PWD"],
        "mssql" => &["MSSQL_CLI_PASSWORD", "SQLCMDPASSWORD"],
        "sqlite" | "geopackage" | "duckdb" => &[],
        _ => &["PGPASSWORD"],
    }
}

/// `scheme://user:password@host/db` without the password, and the (percent-decoded) password.
fn split_password(url: &str) -> (String, Option<String>) {
    let Some((scheme, rest)) = url.split_once("://") else {
        return (url.to_string(), None);
    };
    let authority_end = rest.find(['/', '?']).unwrap_or(rest.len());
    let Some(at) = rest[..authority_end].rfind('@') else {
        return (url.to_string(), None);
    };
    let (credentials, tail) = rest.split_at(at);
    let Some((user, password)) = credentials.split_once(':') else {
        return (url.to_string(), None);
    };
    (
        format!("{scheme}://{user}{tail}"),
        Some(percent_decode(password)),
    )
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if bytes.len() >= i + 3
                && text.is_char_boundary(i + 1)
                && text.is_char_boundary(i + 3) =>
            {
                match u8::from_str_radix(&text[i + 1..i + 3], 16) {
                    Ok(byte) => {
                        out.push(byte);
                        i += 3;
                    }
                    Err(_) => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            byte => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn strip_driver(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => {
            let scheme = scheme.split('+').next().unwrap_or(scheme);
            format!("{scheme}://{rest}")
        }
        None => url.to_string(),
    }
}

fn mysql_args(spec: &meerschaum_connector_sql::uri::ConnectionSpec) -> Vec<String> {
    let mut args = Vec::new();
    if let Some(h) = &spec.host {
        args.push(format!("--host={h}"));
    }
    if let Some(p) = spec.port {
        args.push(format!("--port={p}"));
    }
    if let Some(u) = &spec.username {
        args.push(format!("--user={u}"));
    }
    if let Some(d) = &spec.database {
        args.push(d.clone());
    }
    args
}

fn mssql_args(spec: &meerschaum_connector_sql::uri::ConnectionSpec) -> Vec<String> {
    let mut args = Vec::new();
    if let (Some(h), Some(p)) = (&spec.host, spec.port) {
        args.extend(["-S".into(), format!("tcp:{h},{p}")]);
    }
    if let Some(d) = &spec.database {
        args.extend(["-d".into(), d.clone()]);
    }
    if let Some(u) = &spec.username {
        args.extend(["-U".into(), u.clone()]);
    }
    args
}

/// `which(program)` over `PATH` and the root's virtual environments.
fn find_program(rt: &mut Runtime<'_>, program: &str, paths: &Paths) -> Option<String> {
    let path_var = (rt.env)("PATH");
    if let Some(found) = super::pip::which(program, path_var.as_deref()) {
        return Some(found.display().to_string());
    }
    let venvs = paths.venvs_dir.clone()?;
    for entry in std::fs::read_dir(venvs).ok()?.flatten() {
        let candidate = entry.path().join("bin").join(program);
        if candidate.is_file() {
            return Some(candidate.display().to_string());
        }
    }
    None
}

/// `sql [label] [read|exec] [query]`.
pub fn sql(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let nopretty = flag(args, "nopretty");
    if action.len() == 1 && (action[0] == "read" || action[0] == "exec") {
        rt.print(&format!(
            "{}\n",
            crate::action_docs::docstring("sql").unwrap_or_default()
        ));
        return Ok(SuccessTuple::fail("Query must not be empty."));
    }
    if action.is_empty() {
        return open_cli_with_options(rt, "main", nopretty);
    }
    let sql_section = rt
        .config()
        .get(&["meerschaum", "connectors", "sql"])
        .map(|v| v.to_map())
        .unwrap_or_default();
    let mut label: Option<String> = None;
    let query = action[action.len() - 1].clone();
    for a in &action {
        if a == "read" || a == "exec" {
            continue;
        }
        if sql_section.contains_key(a) {
            label = Some(a.clone());
        }
    }
    let label = label.unwrap_or_else(|| "main".into());
    let keys = format!("sql:{label}");
    // `sql <label>` only opens the client, which connects on its own (the database may be down).
    if query == label {
        return open_cli_with_options(rt, &label, nopretty);
    }
    let sess = session(rt);
    let conn = match sess.instance(&keys, rt.config()) {
        Ok(Instance::Sql(c)) => c,
        _ => {
            return Ok(SuccessTuple::fail(format!(
                "Could not create SQL connector '{label}'.\nVerify meerschaum:connectors:sql:{label} is defined correctly with `show connectors`, and update or add connectors with `edit config`."
            )));
        }
    };
    // The method is guessed from the structure of the query (an explicit `read` / `exec` word
    // only matters to that guess's absence in v4, and is always overridden).
    let method = if query.to_lowercase().contains("select") || !query.contains(' ') {
        "read"
    } else {
        "exec"
    };
    if method == "read" {
        let text = if query.contains(' ') && !(query.starts_with('"') && query.ends_with('"')) {
            query.clone()
        } else {
            format!(
                "SELECT * FROM {}",
                meerschaum_connector_sql::ident::sql_item_name(
                    &query,
                    conn.flavor(),
                    conn.schema()
                )
            )
        };
        // A protected connector runs only a read statement, read-only (`read_raw`).
        match sess.block_on(conn.read_raw(&text)) {
            Err(meerschaum_connector_sql::SqlError::Protected(refusal)) => {
                return Ok(SuccessTuple::fail(refusal));
            }
            // PARITY: a statement that returns no result set (`CREATE VIEW ... AS SELECT ...`, which
            // contains `select` and so is routed to `read`) has already run; v4's `read()` then
            // fails with SQLAlchemy's "This result object does not return rows", logs it and
            // returns `None`, which the action prints (`actions/sql.py`).
            Ok(frame) if frame.names().is_empty() => {
                rt.eprint(
                    "This result object does not return rows. It has been closed automatically.\n",
                );
                pprint(rt, &PyObj::None, nopretty);
            }
            Ok(frame) => {
                if nopretty {
                    rt.print(&format!("{}\n", split_json(&frame)));
                } else {
                    let table = format_dataframe(&super::show::tab_frame(&frame), None);
                    rt.print(&format!("{table}\n"));
                }
            }
            Err(e) => {
                // PARITY: `read()` returns `None` after logging the error, and the action then
                // prints it (`pprint(None)`) and succeeds (`actions/sql.py`).
                rt.eprint(&format!("{e}\n"));
                pprint(rt, &PyObj::None, nopretty);
            }
        }
    } else {
        if let Err(refusal) = conn.check_writable("execute SQL") {
            return Ok(SuccessTuple::fail(refusal));
        }
        match sess.block_on(conn.driver().exec(&query)) {
            Ok(_) => {
                if !nopretty {
                    print_tuple(
                        rt,
                        &SuccessTuple::ok(format!("Successfully executed query:\n\n{query}")),
                        TupleArgs::default(),
                    );
                }
            }
            Err(e) => {
                warn(rt, &e.to_string());
                pprint(rt, &PyObj::None, nopretty);
            }
        }
    }
    Ok(SuccessTuple::ok("Success"))
}

/// `to_json(df, orient='split', index=False)`.
fn split_json(frame: &meerschaum_kernel::Frame) -> String {
    let opts = meerschaum_connector_api::wire::ToJsonOptions::default();
    let columns: Vec<String> = frame
        .columns()
        .iter()
        .map(|c| {
            meerschaum_connector_api::wire::to_json_value(
                &meerschaum_kernel::value::Value::Str(c.name.clone()),
                &opts,
            )
        })
        .collect();
    let values: Vec<Vec<meerschaum_kernel::value::Value>> =
        frame.columns().iter().map(|c| c.data.values()).collect();
    let rows: Vec<String> = (0..frame.height())
        .map(|i| {
            let cells: Vec<String> = values
                .iter()
                .map(|col| meerschaum_connector_api::wire::to_json_value(&col[i], &opts))
                .collect();
            format!("[{}]", cells.join(","))
        })
        .collect();
    format!(
        "{{\"columns\":[{}],\"data\":[{}]}}",
        columns.join(","),
        rows.join(",")
    )
}

// --- stack ----------------------------------------------------------------------------------

const COMPOSE_HEADER: &str = "
##############################################################
#                                                            #
#                   DO NOT EDIT THIS FILE!                   #
#                                                            #
#          Any changes you make will be overwritten.         #
#                                                            #
# Instead, you can change this file's configuration with     #
# `edit config stack` under the docker-compose.yaml section. #
#                                                            #
##############################################################
";

/// `meerschaum.config.stack.env_dict` (values may hold `MRSM{}` symlinks).
pub(crate) fn stack_env_dict() -> Map {
    let mut env = Map::new();
    let s = |v: &str| Value::Str(v.to_string());
    env.insert("COMPOSE_PROJECT_NAME".into(), s("mrsm"));
    env.insert("TIMESCALEDB_VERSION".into(), s("pg18"));
    env.insert(
        "POSTGRES_USER".into(),
        s("MRSM{meerschaum:connectors:sql:main:username}"),
    );
    env.insert(
        "POSTGRES_PASSWORD".into(),
        s("MRSM{meerschaum:connectors:sql:main:password}"),
    );
    env.insert(
        "POSTGRES_DB".into(),
        s("MRSM{meerschaum:connectors:sql:main:database}"),
    );
    env.insert(
        "VALKEY_USERNAME".into(),
        s("MRSM{meerschaum:connectors:valkey:main:username}"),
    );
    env.insert(
        "VALKEY_PASSWORD".into(),
        s("MRSM{meerschaum:connectors:valkey:main:password}"),
    );
    env.insert("MEERSCHAUM_API_HOSTNAME".into(), s("api"));
    env.insert("ALLOW_IP_RANGE".into(), s("0.0.0.0/0"));
    env.insert("MEERSCHAUM_API_CONFIG_RESOURCES".into(), s("/meerschaum"));
    env.insert(
        "MEERSCHAUM_API_CONFIG".into(),
        s("{\n    \"meerschaum\": MRSM{!meerschaum},\n    \"system\": MRSM{!system},\n    \"api\": MRSM{!api}\n}"),
    );
    env.insert(
        "MEERSCHAUM_API_PATCH".into(),
        s("{\n    \"meerschaum\": {\n        \"connectors\": {\n            \"sql\": {\n                \"main\": {\n                    \"host\": \"MRSM{stack:docker-compose.yaml:services:db:hostname}\",\n                    \"port\": 5432\n                },\n                \"local\": {\n                    \"database\": \"/meerschaum/sqlite/mrsm_local.db\"\n                }\n            },\n            \"valkey\": {\n                \"main\": {\n                    \"host\": \"MRSM{stack:docker-compose.yaml:services:valkey:hostname}\",\n                    \"port\": 6379\n                }\n            }\n        }\n    }\n}"),
    );
    env
}

/// `sync_yaml_configs(keys, sub_path, substitute=True, replace_tuples=...)`.
fn sync_yaml_config(
    rt: &mut Runtime<'_>,
    keys: &[&str],
    sub_path: &std::path::Path,
    replace: &[(&str, &str)],
) -> Result<(), String> {
    use meerschaum_core::config::GetOptions;
    let (_, value) = rt.config().get_with(
        keys,
        GetOptions {
            patch: true,
            substitute: true,
            write_missing: false,
            warn: false,
        },
    );
    let mut text = meerschaum_core::yaml::dump(&value).map_err(|e| e.0)?;
    for (old, new) in replace {
        text = text.replace(old, new);
    }
    // The header is the existing file's leading comment / blank lines.
    let header = std::fs::read_to_string(sub_path)
        .map(|existing| {
            let mut header = String::new();
            for line in existing.split_inclusive('\n') {
                if !line.starts_with('#') && line != "\n" {
                    break;
                }
                header.push_str(line);
            }
            header
        })
        .unwrap_or_default();
    if let Some(parent) = sub_path.parent() {
        std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    std::fs::write(sub_path, format!("{header}{text}")).map_err(|e| e.to_string())
}

/// `sync_files(['stack'])` (`config/_sync.py`): rewrite the compose, Grafana and init files from
/// the `stack` config. `stack()` runs it before every `docker compose` command, so the setup script
/// the compose file mounts as a FILE (`create-extensions.sql:/docker-entrypoint-initdb.d/
/// 100_meerschaum.sql`) always exists: a missing bind-mount source would become a directory.
fn sync_stack_files(rt: &mut Runtime<'_>) -> Result<(), String> {
    let paths = rt.config().paths();
    sync_yaml_config(
        rt,
        &["stack", "docker-compose.yaml"],
        &paths.path("STACK_COMPOSE_PATH"),
        &[("$", "$$"), ("<DOLLAR>", "$")],
    )?;
    sync_yaml_config(
        rt,
        &["stack", "grafana", "datasource"],
        &paths.path("GRAFANA_DATASOURCE_PATH"),
        &[],
    )?;
    sync_yaml_config(
        rt,
        &["stack", "grafana", "dashboard"],
        &paths.path("GRAFANA_DASHBOARD_PATH"),
        &[],
    )?;
    let init = paths.path("DB_CREATE_EXTENSIONS_PATH");
    if !init.exists() {
        if let Some(parent) = init.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        // The one copy of the setup script (`meerschaum-core`, `config/create-extensions.sql`).
        std::fs::write(&init, meerschaum_core::config::sync::CREATE_EXTENSIONS_TEXT)
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// `write_stack()`: write the files from the `stack` config (the compose file with its header),
/// then `sync_files(['stack'])`.
pub fn write_stack(rt: &mut Runtime<'_>) -> Result<(), String> {
    use meerschaum_core::config::GetOptions;
    let paths = rt.config().paths();
    let opts = GetOptions {
        patch: true,
        substitute: true,
        write_missing: false,
        warn: false,
    };
    let files: [(std::path::PathBuf, Vec<&str>, Option<&str>); 3] = [
        (
            paths.path("STACK_COMPOSE_PATH"),
            vec!["stack", "docker-compose.yaml"],
            Some(COMPOSE_HEADER),
        ),
        (
            paths.path("GRAFANA_DATASOURCE_PATH"),
            vec!["stack", "grafana", "datasource"],
            None,
        ),
        (
            paths.path("GRAFANA_DASHBOARD_PATH"),
            vec!["stack", "grafana", "dashboard"],
            None,
        ),
    ];
    for (path, keys, header) in files {
        let (_, value) = rt.config().get_with(&keys, opts);
        let mut text = header.unwrap_or_default().to_string();
        text.push_str(&meerschaum_core::yaml::dump(&value).map_err(|e| e.0)?);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        std::fs::write(&path, text).map_err(|e| e.to_string())?;
    }
    sync_stack_files(rt)
}

/// `stack <command ...>`: control the Meerschaum stack with Docker Compose.
pub fn stack(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let paths = rt.config().paths();
    // `os.environ` patched by the stack's variables (strings only).
    let stack_env = {
        let substituted = rt
            .config()
            .search_and_substitute(&stack_env_dict())
            .unwrap_or_default();
        let mut vars: Vec<(String, String)> = Vec::new();
        for (k, v) in substituted {
            if let Value::Str(s) = v {
                vars.push((k, s));
            }
        }
        vars
    };
    let action = words(args, "action");
    let mut sysargs = words(args, "sysargs");
    // `stack()` called without sysargs: use the action words.
    if !action.is_empty() && sysargs.is_empty() {
        sysargs = action;
        if sysargs[0] != "stack" {
            sysargs.insert(0, "stack".into());
        }
    }
    let necessary = [
        paths.path("STACK_COMPOSE_PATH"),
        paths.path("GRAFANA_DATASOURCE_PATH"),
        paths.path("GRAFANA_DASHBOARD_PATH"),
    ];
    let bootstrap = necessary.iter().any(|p| !p.exists());
    let written = if bootstrap {
        write_stack(rt)
    } else {
        sync_stack_files(rt)
    };
    if let Err(e) = written {
        return Err(ActionError::new("OSError", e));
    }
    let project_name = rt
        .config()
        .get(&["stack", "project_name"])
        .map(|v| v.py_str())
        .unwrap_or_else(|| "mrsm".into());
    let docker = |args: &[&str]| CommandSpec {
        program: "docker".into(),
        args: args.iter().map(|a| a.to_string()).collect(),
        quiet: true,
        ..CommandSpec::default()
    };
    if !matches!(rt.host.run(&docker(&["ps"])), Ok(0)) {
        warn(
            rt,
            "Could not connect to Docker. Is the Docker service running?",
        );
        rt.eprint(
            "To start the Docker service, run `sudo systemctl start docker` or `sudo dockerd`.\nOn Windows or MacOS, make sure Docker Desktop is running.\n",
        );
        return Ok(SuccessTuple::fail(
            "Failed to connect to the Docker engine.",
        ));
    }
    if !matches!(rt.host.run(&docker(&["compose"])), Ok(0)) {
        return Ok(SuccessTuple::fail("Docker Compose is not available."));
    }
    if sysargs.iter().any(|a| a == "down")
        && sysargs.iter().any(|a| a == "-v")
        && !yes_no(
            rt,
            "Are you sure you want to drop volumes?\n    This cannot be undone!",
            "n",
            false,
            false,
        )?
    {
        return Ok(SuccessTuple::fail("Nothing was dropped."));
    }
    // `up` is checked completely; `start` and `restart` (which also start a stopped `db`) only
    // for an `upgrade postgres` / `restore postgres` / `rollback` moving the database right now.
    let guard = sysargs
        .iter()
        .skip(1)
        .find_map(|a| super::upgrade_postgres::StackCheck::of(a));
    let starting = guard.is_some();
    if let Some(check) = guard
        && !flag(args, "force")
        && !sysargs.iter().any(|a| a == "--force")
        && let Err(e) = super::upgrade_postgres::stack_up_preflight(
            rt,
            &project_name,
            &paths.path("STACK_COMPOSE_PATH"),
            &stack_env,
            check,
        )
    {
        return Ok(SuccessTuple::fail(e));
    }
    let mut cmd_list: Vec<String> = vec!["--project-name".into(), project_name];
    cmd_list.extend(sysargs.iter().skip(1).cloned());
    cmd_list.retain(|a| a != "--debug" && !(starting && a == "--force"));
    let mut compose_args = vec!["compose".to_string()];
    compose_args.extend(cmd_list.iter().cloned());
    let spec = CommandSpec {
        program: "docker".into(),
        args: compose_args,
        env: stack_env,
        cwd: paths
            .path("STACK_COMPOSE_PATH")
            .parent()
            .map(|p| p.to_path_buf()),
        ..CommandSpec::default()
    };
    let rc = rt.host.run(&spec).unwrap_or(1);
    let success = rc == 0;
    // v5: after `stack up`, one line when the stack's database answers quickly and an extension
    // lags the image's version (never fails or noticeably slows `stack up`).
    if success
        && sysargs.iter().skip(1).any(|a| a == "up")
        && super::extensions::stack_hint_enabled()
        && let Some(hint) =
            super::extensions::lag_hint(rt, "sql:main", super::extensions::HINT_TIMEOUT)
    {
        super::info(rt, &hint);
    }
    let message = if success {
        "Success".to_string()
    } else {
        format!(
            "Failed to execute commands:\n{}",
            PyObj::List(cmd_list.iter().map(|c| PyObj::Str(c.clone())).collect()).repr()
        )
    };
    Ok(SuccessTuple { success, message })
}
