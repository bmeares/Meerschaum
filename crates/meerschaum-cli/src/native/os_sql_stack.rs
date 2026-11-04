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
    open_cli_with_options(rt, label, false, false)
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

/// The Python client of a flavor and its install name (the `cli-tools` extra), which `sql` offers
/// to install into the `mrsm` virtual environment when no client is found.
fn installable_cli(flavor: &str) -> Option<(&'static str, &'static str)> {
    let cli = flavor_cli(flavor)?;
    Some(match cli {
        "pgcli" => (cli, "pgcli>=3.1.0"),
        "mycli" => (cli, "mycli>=1.23.2"),
        "litecli" => (cli, "litecli>=1.5.0"),
        "gadwall" => (cli, "gadwall>=0.2.0"),
        _ => return None,
    })
}

fn open_cli_with_options(
    rt: &mut Runtime<'_>,
    label: &str,
    nopretty: bool,
    noask: bool,
) -> ActionResult {
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
    let find = |rt: &mut Runtime<'_>| {
        candidates
            .iter()
            .find_map(|name| find_client(rt, name, &path).map(|client| (name.clone(), client)))
    };
    let mut selected = find(rt);
    // Nothing on this host: offer the flavor's Python client (installed into the `mrsm` virtual
    // environment, which is searched next time too) instead of only failing. Without a terminal
    // or with `--noask` / `MRSM_NOASK`, the message says how to install one.
    let installable = installable_cli(&flavor).filter(|_| !nopretty);
    if selected.is_none()
        && let Some((name, requirement)) = installable
        && !super::check_noask(rt, noask)
        && std::io::IsTerminal::is_terminal(&std::io::stdin())
        && yes_no(
            rt,
            &format!(
                "No SQL client for '{flavor}' was found. Install {name} into the Meerschaum \
                 virtual environment now?"
            ),
            "y",
            false,
            noask,
        )?
    {
        let env = super::pip::PipEnv::new(rt);
        let installed = super::pip::pip_install(
            &env,
            rt,
            &super::pip::PipInstall {
                names: vec![requirement.to_string()],
                venv: "mrsm".into(),
                ..super::pip::PipInstall::default()
            },
        );
        if !installed {
            return failed(format!(
                "Failed to install {name}. Install it with `mrsm install packages {name}`."
            ));
        }
        selected = find(rt);
    }
    let Some((cli, client)) = selected else {
        let install = match installable {
            Some((name, _)) => {
                format!("`mrsm install packages {name}` (or `meerschaum[cli-tools]`)")
            }
            None => "`meerschaum[cli-tools]`".to_string(),
        };
        return failed(format!(
            "Cannot find a SQL client for '{flavor}' (tried {}) on PATH ({}), in ~/.cargo/bin, \
             ~/.local/bin or Meerschaum's virtual environments. Install {install}, install \
             Rainfrog with `cargo install rainfrog --locked`, or add the plain flavor client to \
             PATH.",
            candidates.join(", "),
            (rt.env)("PATH").unwrap_or_default(),
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
        program: client.program,
        args: client.prefix.into_iter().chain(args).collect(),
        env: client.env,
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

/// How a SQL client starts: its program, the arguments before the client's own, and variables.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Client {
    program: String,
    prefix: Vec<String>,
    env: Vec<(String, String)>,
}

impl Client {
    fn program(path: &std::path::Path) -> Client {
        Client {
            program: path.display().to_string(),
            prefix: Vec::new(),
            env: Vec::new(),
        }
    }
}

/// The directories searched after `PATH`: a service's `PATH` is often reduced
/// (`/usr/local/bin:/usr/bin:/bin`), which hides the user's own tools. `~/.cargo/bin` (or
/// `$CARGO_HOME/bin`: `cargo install rainfrog`), `~/.local/bin` (`pip install --user`), and the
/// directory of the configured interpreter (`MRSM_PYTHON`: a virtual environment's `bin`).
fn extra_client_dirs(rt: &Runtime<'_>) -> Vec<std::path::PathBuf> {
    use std::path::PathBuf;
    let mut dirs = Vec::new();
    if let Some(cargo) = (rt.env)("CARGO_HOME").filter(|v| !v.is_empty()) {
        dirs.push(PathBuf::from(cargo).join("bin"));
    }
    if let Some(home) = (rt.env)("HOME").filter(|v| !v.is_empty()) {
        let home = PathBuf::from(home);
        dirs.push(home.join(".cargo").join("bin"));
        dirs.push(home.join(".local").join("bin"));
    }
    if let Some(python) = (rt.env)("MRSM_PYTHON").filter(|v| !v.trim().is_empty()) {
        let python = PathBuf::from(python.trim());
        if python.is_absolute()
            && let Some(dir) = python.parent()
        {
            dirs.push(dir.to_path_buf());
        }
    }
    dirs.dedup();
    dirs
}

/// The interpreter (with its arguments) and the top-level module of a Python console script:
/// `#!/usr/bin/python3` (or `#!/usr/bin/env python3`) and the first `from pkg... import` /
/// `import pkg` of a script `pip` writes. `None` for anything that is not a Python script.
fn python_script(path: &std::path::Path) -> Option<(Vec<String>, String)> {
    use std::io::{BufRead, BufReader, Read};
    let mut file = std::fs::File::open(path).ok()?;
    let mut head = [0u8; 2];
    file.read_exact(&mut head).ok()?;
    if &head != b"#!" {
        return None;
    }
    let mut lines = BufReader::new(file).lines().map_while(Result::ok).take(40);
    let shebang: Vec<String> = lines
        .next()?
        .split_whitespace()
        .map(str::to_string)
        .collect();
    let mut interpreter = shebang;
    if interpreter.first().is_some_and(|p| {
        std::path::Path::new(p)
            .file_name()
            .is_some_and(|n| n == "env")
    }) {
        interpreter.remove(0);
        if interpreter.first().is_some_and(|a| a == "-S") {
            interpreter.remove(0);
        }
    }
    let program = std::path::Path::new(interpreter.first()?)
        .file_name()?
        .to_string_lossy()
        .into_owned();
    if !program.starts_with("python") {
        return None;
    }
    let module = lines.find_map(|line| {
        let line = line.trim();
        let rest = line
            .strip_prefix("from ")
            .or_else(|| line.strip_prefix("import "))?;
        let name: String = rest
            .chars()
            .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
            .collect();
        (!name.is_empty() && !matches!(name.as_str(), "re" | "sys" | "os" | "__future__"))
            .then_some(name)
    })?;
    Some((interpreter, module))
}

/// Why a found client cannot run, if it cannot: a Python console script whose interpreter is gone
/// or cannot import the package it runs (a script left by an older Python, or a package that was
/// removed). A client that is not a Python script is taken as it is.
fn broken_client(client: &Client, script: &std::path::Path) -> Option<String> {
    let (mut interpreter, module) = python_script(script)?;
    // A script run by a virtual environment's interpreter (`site-packages/bin`) uses that one.
    if !client.prefix.is_empty() {
        interpreter = vec![client.program.clone()];
    }
    let mut command = std::process::Command::new(&interpreter[0]);
    command
        .args(&interpreter[1..])
        .args(["-c", &format!("import {module}")])
        .envs(client.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());
    let python = interpreter.join(" ");
    match command.status() {
        Ok(status) if status.success() => None,
        Ok(_) => Some(format!("its Python ({python}) cannot import `{module}`")),
        Err(_) => Some(format!("its Python ({python}) is not installed")),
    }
}

/// `which(program)` over `PATH`, [`extra_client_dirs`], and the root's virtual environments: a
/// venv's own `bin`, and the scripts `mrsm install packages` writes beside its packages
/// (`site-packages/bin`, run with the venv's interpreter and those packages on its path). A Python
/// client that cannot run ([`broken_client`]: e.g. a `pgcli` script in `~/.local/bin` left by an
/// older Python) is skipped with a warning, and the search goes on.
fn find_client(rt: &mut Runtime<'_>, program: &str, paths: &Paths) -> Option<Client> {
    let mut dirs: Vec<std::path::PathBuf> = (rt.env)("PATH")
        .map(|path| std::env::split_paths(&path).collect())
        .unwrap_or_default();
    dirs.extend(extra_client_dirs(rt));
    let mut seen = std::collections::HashSet::new();
    let mut usable = |rt: &mut Runtime<'_>, client: Client, script: &std::path::Path| {
        if !seen.insert(script.to_path_buf()) {
            return None;
        }
        match broken_client(&client, script) {
            None => Some(client),
            Some(reason) => {
                warn(
                    rt,
                    &format!("Skipping {program} at {}: {reason}.", script.display()),
                );
                None
            }
        }
    };
    for dir in &dirs {
        if let Some(found) = super::pip::which(program, dir.to_str())
            && let Some(client) = usable(rt, Client::program(&found), &found)
        {
            return Some(client);
        }
    }
    // `MRSM_VENVS_DIR`, else `<root>/venvs` (`VIRTENV_RESOURCES_PATH`).
    let venvs = paths
        .venvs_dir
        .clone()
        .unwrap_or_else(|| paths.root_dir.join("venvs"));
    let mut entries: Vec<_> = std::fs::read_dir(venvs).ok()?.flatten().collect();
    // `mrsm` (where `sql` installs its clients) first, then the others by name.
    entries.sort_by_key(|e| (e.file_name() != "mrsm", e.file_name()));
    for entry in entries {
        let venv = entry.path();
        let bin = venv.join(if cfg!(windows) { "Scripts" } else { "bin" });
        if let Some(found) = super::pip::which(program, bin.to_str())
            && let Some(client) = usable(rt, Client::program(&found), &found)
        {
            return Some(client);
        }
        let python = bin.join(if cfg!(windows) {
            "python.exe"
        } else {
            "python"
        });
        for site in super::venvs::site_packages(&venv) {
            let script = site.join("bin").join(program);
            if !script.is_file() || !python.is_file() {
                continue;
            }
            let mut pythonpath = vec![site.clone()];
            if let Some(existing) = (rt.env)("PYTHONPATH").filter(|v| !v.is_empty()) {
                pythonpath.extend(std::env::split_paths(&existing));
            }
            let pythonpath = std::env::join_paths(pythonpath)
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|_| site.display().to_string());
            let client = Client {
                program: python.display().to_string(),
                prefix: vec![script.display().to_string()],
                env: vec![("PYTHONPATH".into(), pythonpath)],
            };
            if let Some(client) = usable(rt, client, &script) {
                return Some(client);
            }
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
        return open_cli_with_options(rt, "main", nopretty, flag(args, "noask"));
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
        return open_cli_with_options(rt, &label, nopretty, flag(args, "noask"));
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
