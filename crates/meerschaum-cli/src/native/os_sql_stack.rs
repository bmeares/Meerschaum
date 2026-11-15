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
    // The user's own command: it keeps the whole environment, like `sh` (5.1).
    let spec = CommandSpec {
        scrub_secrets: false,
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
        scrub_secrets: true,
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
    let mut command =
        meerschaum_core::child_env::scrubbed(std::process::Command::new(&interpreter[0]));
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

/// The network between the stack's API server and its Grafana (`api:grafana`, 5.1): Grafana
/// trusts the auth-proxy header from the API server's fixed address on it only.
pub const GRAFANA_PROXY_NETWORK: &str = "grafana_auth";

/// The addresses on `api:grafana:proxy_subnet` (an IPv4 network of at least eight addresses,
/// [`meerschaum_web::grafana::DEFAULT_PROXY_SUBNET`] by default): `(subnet, API server, Grafana)`,
/// the first two hosts after Docker's gateway.
pub fn grafana_proxy_addresses(
    grafana: &Map,
) -> Result<(String, std::net::Ipv4Addr, std::net::Ipv4Addr), String> {
    let subnet = meerschaum_web::grafana::setting_text(grafana, "proxy_subnet")
        .unwrap_or_else(|| meerschaum_web::grafana::DEFAULT_PROXY_SUBNET.to_string());
    let invalid = || {
        format!(
            "`api:grafana:proxy_subnet` ('{subnet}') must be an IPv4 network such as \
             '{}' (a prefix of /29 or shorter).",
            meerschaum_web::grafana::DEFAULT_PROXY_SUBNET
        )
    };
    let (address, prefix) = subnet.split_once('/').ok_or_else(invalid)?;
    let address: std::net::Ipv4Addr = address.trim().parse().map_err(|_| invalid())?;
    let prefix: u32 = prefix.trim().parse().map_err(|_| invalid())?;
    if !(8..=29).contains(&prefix) {
        return Err(invalid());
    }
    let mask = u32::MAX << (32 - prefix);
    let network = u32::from(address) & mask;
    Ok((
        format!("{}/{prefix}", std::net::Ipv4Addr::from(network)),
        std::net::Ipv4Addr::from(network + 2),
        std::net::Ipv4Addr::from(network + 3),
    ))
}

/// What the stack's Grafana is given for the API server to sign its users in (`api:grafana`,
/// 5.1). `forced`: set over whatever the stack config has (the auth proxy and the server
/// administrator); `defaults`: set only when missing (served under `/grafana`). Anonymous access
/// is left as the stack config has it.
///
/// The auth-proxy header is trusted from `proxy_whitelist` (a string or a list), else from
/// `api_address` (the stack's API server on [`GRAFANA_PROXY_NETWORK`]); with neither, the auth
/// proxy is turned off (`warning` says what to set). Never a network range by default.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GrafanaSignIn {
    pub forced: Vec<(&'static str, String)>,
    pub defaults: Vec<(&'static str, String)>,
    pub warning: Option<String>,
}

pub fn grafana_sign_in_environment(
    grafana: &Map,
    admin_password: &str,
    api_address: Option<std::net::Ipv4Addr>,
) -> GrafanaSignIn {
    use meerschaum_web::grafana::setting_text;
    let whitelist = setting_text(grafana, "proxy_whitelist")
        .or_else(|| api_address.map(|address| address.to_string()));
    let mut forced = vec![
        (
            "GF_SECURITY_ADMIN_USER",
            setting_text(grafana, "admin_user").unwrap_or_else(|| "admin".into()),
        ),
        ("GF_SECURITY_ADMIN_PASSWORD", admin_password.to_string()),
        ("GF_AUTH_PROXY_HEADER_NAME", "X-WEBAUTH-USER".into()),
        ("GF_AUTH_PROXY_HEADER_PROPERTY", "username".into()),
        ("GF_AUTH_PROXY_AUTO_SIGN_UP", "true".into()),
    ];
    let mut warning = None;
    match whitelist {
        Some(whitelist) => {
            forced.push(("GF_AUTH_PROXY_ENABLED", "true".into()));
            forced.push(("GF_AUTH_PROXY_WHITELIST", whitelist));
        }
        None => {
            forced.push(("GF_AUTH_PROXY_ENABLED", "false".into()));
            forced.push(("GF_AUTH_PROXY_WHITELIST", String::new()));
            warning = Some(
                "Grafana's sign-in through Meerschaum is off: the stack has no `api` service, so \
                 the API server's address is not known. Set `api:grafana:proxy_whitelist` to the \
                 address Grafana sees the API server connect from (and `api:grafana:url` to how \
                 the API server reaches Grafana)."
                    .to_string(),
            );
        }
    }
    GrafanaSignIn {
        forced,
        defaults: vec![
            (
                "GF_SERVER_ROOT_URL",
                setting_text(grafana, "root_url")
                    .unwrap_or_else(|| "%(protocol)s://%(domain)s:%(http_port)s/grafana/".into()),
            ),
            ("GF_SERVER_SERVE_FROM_SUB_PATH", "true".into()),
        ],
        warning,
    }
}

/// Set `name` in a compose `environment` (a map or a `KEY=value` list); `force` replaces an
/// existing value.
pub(super) fn set_environment(environment: &mut Value, name: &str, value: &str, force: bool) {
    match environment {
        Value::Map(map) => {
            if force || !map.contains_key(name) {
                map.insert(name.to_string(), Value::Str(value.to_string()));
            }
        }
        Value::List(items) => {
            let prefix = format!("{name}=");
            let exists = items
                .iter()
                .any(|item| matches!(item, Value::Str(s) if s.starts_with(&prefix) || s == name));
            if exists && !force {
                return;
            }
            items.retain(
                |item| !matches!(item, Value::Str(s) if s.starts_with(&prefix) || s == name),
            );
            items.push(Value::Str(format!("{name}={value}")));
        }
        other => {
            let mut map = Map::new();
            map.insert(name.to_string(), Value::Str(value.to_string()));
            *other = Value::Map(map);
        }
    }
}

/// A service's `networks` as a map (compose takes a list or a map), with `network` attached at
/// `address`.
fn attach_network(service: &mut Map, network: &str, address: std::net::Ipv4Addr) {
    let mut networks = match service.get("networks") {
        Some(Value::Map(map)) => map.clone(),
        Some(Value::List(items)) => items
            .iter()
            .map(|item| (item.py_str(), Value::Null))
            .collect(),
        _ => Map::new(),
    };
    let mut attachment = Map::new();
    attachment.insert("ipv4_address".into(), Value::Str(address.to_string()));
    networks.insert(network.to_string(), Value::Map(attachment));
    service.insert("networks".into(), Value::Map(networks));
}

/// The api service's `MRSM_PATCH` with `api:grafana:admin_password` (the stack's API server has
/// a root of its own, so it is given the password Grafana is started with). `None` when the
/// variable is not JSON.
fn patched_api_patch(existing: Option<&str>, admin_password: &str) -> Option<String> {
    let mut patch: serde_json::Value = match existing.map(str::trim).filter(|s| !s.is_empty()) {
        Some(text) => serde_json::from_str(text).ok()?,
        None => serde_json::json!({}),
    };
    let object = patch.as_object_mut()?;
    let api = object
        .entry("api")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()?;
    let grafana = api
        .entry("grafana")
        .or_insert_with(|| serde_json::json!({}))
        .as_object_mut()?;
    grafana.insert("admin_password".into(), admin_password.into());
    serde_json::to_string_pretty(&patch).ok()
}

/// The compose file's config with what `api:grafana:enabled` needs (5.1): the `grafana` service
/// gets [`grafana_sign_in_environment`] (security settings forced, the others only when
/// missing); when the stack has an `api` service, both join [`GRAFANA_PROXY_NETWORK`] at fixed
/// addresses, the API server reaches `grafana` there (`extra_hosts`), and it is given the
/// administrator password. Other files and a stack without a `grafana` service are returned as
/// they are.
fn with_grafana_sign_in(rt: &mut Runtime<'_>, keys: &[&str], mut value: Value) -> Value {
    if keys != ["stack", "docker-compose.yaml"] {
        return value;
    }
    let grafana = rt
        .config()
        .get_with(
            &["api", "grafana"],
            meerschaum_core::config::GetOptions {
                patch: true,
                substitute: true,
                write_missing: false,
                warn: false,
            },
        )
        .1
        .to_map();
    if !grafana.get("enabled").is_some_and(Value::truthy) {
        return value;
    }
    let has_grafana = value
        .get("services")
        .and_then(|services| services.get("grafana"))
        .is_some_and(|service| service.as_map().is_some());
    if !has_grafana {
        return value;
    }
    let admin_password = match meerschaum_web::grafana::setting_text(&grafana, "admin_password") {
        Some(password) => password,
        None => {
            let path = meerschaum_web::grafana::admin_password_path(
                &rt.config().paths().path("API_CONFIG_RESOURCES_PATH"),
            );
            match meerschaum_web::grafana::read_or_generate_admin_password(&path) {
                Ok(password) => password,
                Err(e) => {
                    warn(rt, &format!("{e}\nGrafana's sign-in was not configured."));
                    return value;
                }
            }
        }
    };
    if admin_password == meerschaum_web::grafana::DEFAULT_ADMIN_PASSWORD {
        warn(
            rt,
            "`api:grafana:admin_password` is Grafana's default ('admin'): set another one. \
             Grafana's sign-in was not configured.",
        );
        return value;
    }
    let has_api = value
        .get("services")
        .and_then(|services| services.get("api"))
        .is_some_and(|service| service.as_map().is_some());
    let addresses = if has_api {
        match grafana_proxy_addresses(&grafana) {
            Ok(addresses) => Some(addresses),
            Err(e) => {
                warn(rt, &e);
                None
            }
        }
    } else {
        None
    };
    let sign_in = grafana_sign_in_environment(
        &grafana,
        &admin_password,
        addresses.as_ref().map(|(_, api, _)| *api),
    );
    if let Some(warning) = &sign_in.warning {
        warn(rt, warning);
    }
    let Some(compose) = value.as_map_mut() else {
        return value;
    };
    if let Some((subnet, api_address, grafana_address)) = &addresses {
        let networks = compose
            .entry("networks".to_string())
            .or_insert_with(|| Value::Map(Map::new()));
        if !matches!(networks, Value::Map(_)) {
            *networks = Value::Map(Map::new());
        }
        if let Value::Map(networks) = networks {
            let mut pool = Map::new();
            pool.insert("subnet".into(), Value::Str(subnet.clone()));
            let mut ipam = Map::new();
            ipam.insert("config".into(), Value::List(vec![Value::Map(pool)]));
            let mut network = Map::new();
            network.insert("ipam".into(), Value::Map(ipam));
            networks.insert(GRAFANA_PROXY_NETWORK.into(), Value::Map(network));
        }
        if let Some(Value::Map(services)) = compose.get_mut("services") {
            if let Some(Value::Map(api)) = services.get_mut("api") {
                attach_network(api, GRAFANA_PROXY_NETWORK, *api_address);
                // `grafana` from the API server is Grafana's address on this network.
                let host = format!("grafana:{grafana_address}");
                let mut hosts = match api.get("extra_hosts") {
                    Some(Value::List(items)) => items.clone(),
                    Some(Value::Map(map)) => map
                        .iter()
                        .map(|(k, v)| Value::Str(format!("{k}:{}", v.py_str())))
                        .collect(),
                    _ => Vec::new(),
                };
                hosts.retain(|item| {
                    !matches!(item, Value::Str(s) if s.starts_with("grafana:") || s.starts_with("grafana="))
                });
                hosts.push(Value::Str(host));
                api.insert("extra_hosts".into(), Value::List(hosts));
            }
            if let Some(Value::Map(service)) = services.get_mut("grafana") {
                attach_network(service, GRAFANA_PROXY_NETWORK, *grafana_address);
            }
        }
    }
    if let Some(Value::Map(services)) = compose.get_mut("services") {
        if has_api && let Some(Value::Map(api)) = services.get_mut("api") {
            let environment = api
                .entry("environment".to_string())
                .or_insert_with(|| Value::Map(Map::new()));
            let existing = match environment {
                Value::Map(map) => map.get("MRSM_PATCH").map(Value::py_str),
                Value::List(items) => items.iter().find_map(|item| match item {
                    Value::Str(s) => s.strip_prefix("MRSM_PATCH=").map(str::to_string),
                    _ => None,
                }),
                _ => None,
            };
            match patched_api_patch(existing.as_deref(), &admin_password) {
                Some(patch) => set_environment(environment, "MRSM_PATCH", &patch, true),
                None => warn(
                    rt,
                    "The stack's `api` service has an `MRSM_PATCH` that is not JSON: set \
                     `api:grafana:admin_password` for it yourself.",
                ),
            }
        }
        if let Some(Value::Map(service)) = services.get_mut("grafana") {
            let environment = service
                .entry("environment".to_string())
                .or_insert_with(|| Value::Map(Map::new()));
            for (name, setting) in &sign_in.forced {
                set_environment(environment, name, setting, true);
            }
            for (name, setting) in &sign_in.defaults {
                set_environment(environment, name, setting, false);
            }
        }
    }
    value
}

/// The stack's provisioned "Meerschaum Main" data source reads through the main read-only role
/// (5.1, [`super::stack_readonly`]): the data source file gets the role's credentials, the
/// compose file the one-shot service that sets the role up before Grafana starts. `warn`: tell
/// the user what could not be done (once per command).
fn with_main_readonly(rt: &mut Runtime<'_>, keys: &[&str], mut value: Value, warn_: bool) -> Value {
    use super::stack_readonly as ro;
    let is_datasource = keys == ["stack", "grafana", "datasource"];
    let is_compose = keys == ["stack", "docker-compose.yaml"];
    if !is_datasource && !is_compose {
        return value;
    }
    let opts = meerschaum_core::config::GetOptions {
        patch: true,
        substitute: true,
        write_missing: false,
        warn: false,
    };
    let main = rt
        .config()
        .get_with(&["meerschaum", "connectors", "sql", "main"], opts)
        .1
        .to_map();
    let setting = |key: &str| {
        main.get(key)
            .filter(|v| !v.is_null())
            .map(Value::py_str)
            .filter(|s| !s.is_empty())
    };
    let owner_user = setting("username");
    let owner_password = setting("password").unwrap_or_default();
    let mut datasource = if is_datasource {
        value.clone()
    } else {
        rt.config()
            .get_with(&["stack", "grafana", "datasource"], opts)
            .1
    };
    // The same switch every writer of the data source file applies (fails closed: no password
    // rather than the owner's).
    let (source, error) = rt
        .config()
        .with_state(|s| s.main_readonly_datasource(&mut datasource));
    if let Some(e) = error
        && warn_
        && is_datasource
    {
        warn(
            rt,
            &format!(
                "{e}\nGrafana's '{}' data source is written without a password.",
                ro::MAIN_DATASOURCE
            ),
        );
    }
    let password = datasource
        .get("datasources")
        .and_then(Value::as_list)
        .and_then(|sources| {
            sources
                .iter()
                .find(|s| s.get("name").map(Value::py_str).as_deref() == Some(ro::MAIN_DATASOURCE))
        })
        .and_then(|s| s.get("secureJsonData"))
        .and_then(|s| s.get("password"))
        .map(Value::py_str)
        .unwrap_or_default();
    let stack_grafana = rt.config().get_with(&["stack", "grafana"], opts).1.to_map();
    let sql_connectors = rt
        .config()
        .get_with(&["meerschaum", "connectors", "sql"], opts)
        .1
        .to_map();
    let (schemas, refused) = ro::readonly_schemas(&stack_grafana, &sql_connectors);
    if is_datasource {
        if warn_ {
            if !refused.is_empty() {
                warn(
                    rt,
                    &format!(
                        "`stack:grafana:readonly_schemas` names the schema of a team ({}): it is \
                         never granted to Grafana's read-only main data source.",
                        refused.join(", ")
                    ),
                );
            }
            let compose = rt
                .config()
                .get_with(&["stack", "docker-compose.yaml"], opts)
                .1;
            if ro::anonymous_enabled(&compose)
                && let Some(warning) = ro::anonymous_warning(&datasource, &source)
            {
                warn(rt, &warning);
            }
        }
        return datasource;
    }
    let setup = ro::RoleSetup {
        owner_user: owner_user.as_deref().unwrap_or(""),
        owner_password: &owner_password,
        password: &password,
        schemas: &schemas,
        revision: &ro::revision(&datasource, &schemas),
    };
    if let Err(e) = ro::add_role_setup(&mut value, &source, &setup)
        && warn_
    {
        warn(
            rt,
            &format!(
                "Grafana's '{}' data source connects as the read-only role '{}', which `mrsm \
                 stack` cannot create: {e}. It cannot connect until the role exists on its \
                 database.",
                ro::MAIN_DATASOURCE,
                meerschaum_connector_sql::team_database::MAIN_READONLY_ROLE
            ),
        );
    }
    value
}

/// The compose file's config with the services' credentials in files (5.1,
/// [`super::stack_secrets`]): the `api` service's `MRSM_CONFIG` / `MRSM_PATCH` (when its image
/// reads `MRSM_CONFIG_FILE`, see [`super::stack_secrets::api_form`]), Grafana's administrator
/// password and the `grafana_readonly` service's passwords. The files are written here; when
/// they cannot be, the config is returned as it was (a container never starts without its
/// configuration). `warn_`: tell the user what stays in the environment (once per command).
fn with_secret_files(rt: &mut Runtime<'_>, keys: &[&str], value: Value, warn_: bool) -> Value {
    use super::stack_secrets as secrets;
    if keys != ["stack", "docker-compose.yaml"] {
        return value;
    }
    let image = value
        .get("services")
        .and_then(|services| services.get("api"))
        .and_then(|api| api.get("image"))
        .map(Value::py_str);
    let setting = rt
        .config()
        .get_with(
            &["stack", secrets::API_SETTING],
            meerschaum_core::config::GetOptions {
                patch: true,
                substitute: true,
                write_missing: false,
                warn: false,
            },
        )
        .1;
    let setting = (!setting.is_null()).then_some(setting);
    let form = secrets::api_form(image.as_deref(), setting.as_ref());
    let dir = secrets::secrets_dir(&rt.config().paths().path("STACK_RESOURCES_PATH"));
    let mut externalized = value.clone();
    let written = secrets::prepare_dir(&dir).and_then(|salt| {
        let (valkey_password, extra_warnings) = stack_valkey_password(rt, &mut externalized, &dir);
        let mut done = secrets::externalize(&mut externalized, &dir, &form, &salt);
        let (valkey, warnings) =
            secrets::externalize_valkey(&mut externalized, &dir, valkey_password.as_deref(), &salt);
        done.services.push(valkey);
        done.warnings.extend(extra_warnings);
        done.warnings.extend(warnings);
        secrets::write_files(&dir, &done.services).map(|()| done)
    });
    match written {
        Ok(done) => {
            if warn_ {
                for warning in &done.warnings {
                    warn(rt, warning);
                }
            }
            externalized
        }
        Err(e) => {
            if warn_ {
                warn(
                    rt,
                    &format!(
                        "{e}\nThe stack's services get their credentials in their environment \
                         (in `stack/docker-compose.yaml` and `docker inspect`)."
                    ),
                );
            }
            value
        }
    }
}

/// The password the stack's Valkey requires (5.1, [`super::stack_secrets::externalize_valkey`])
/// and what to tell the user: `valkey:main`'s password. When it has none, one is generated once
/// (owner-only, in the secrets directory, as Grafana's administrator password is when unset) and
/// given to the `api` service through its `MRSM_PATCH`, with a warning saying how to point
/// `valkey:main` on this host at it. `None` when the stack has no service of Valkey's images.
fn stack_valkey_password(
    rt: &mut Runtime<'_>,
    compose: &mut Value,
    dir: &std::path::Path,
) -> (Option<String>, Vec<String>) {
    use super::stack_secrets as secrets;
    let image = compose
        .get("services")
        .and_then(|services| services.get("valkey"))
        .and_then(|valkey| valkey.get("image"))
        .map(Value::py_str);
    if !secrets::is_valkey_image(image.as_deref()) {
        return (None, Vec::new());
    }
    let configured = rt
        .config()
        .get_with(
            &["meerschaum", "connectors", "valkey", "main", "password"],
            meerschaum_core::config::GetOptions {
                patch: true,
                substitute: true,
                write_missing: false,
                warn: false,
            },
        )
        .1;
    if !configured.is_null() && !configured.py_str().is_empty() {
        return (Some(configured.py_str()), Vec::new());
    }
    let path = dir.join(secrets::VALKEY_GENERATED_FILENAME);
    let password = match meerschaum_core::config::grafana_readonly::read_or_generate_secret(
        &path,
        "the stack's Valkey password",
    ) {
        Ok(password) => password,
        Err(e) => {
            return (
                None,
                vec![format!("{e}\nThe stack's Valkey runs without a password.")],
            );
        }
    };
    if let Some(Value::Map(api)) = compose
        .as_map_mut()
        .and_then(|c| c.get_mut("services"))
        .and_then(Value::as_map_mut)
        .and_then(|services| services.get_mut("api"))
    {
        let environment = api
            .entry("environment".to_string())
            .or_insert_with(|| Value::Map(Map::new()));
        let existing = secrets::env_get(Some(environment), "MRSM_PATCH");
        match patched_valkey_patch(existing.as_deref(), &password) {
            Some(patch) => set_environment(environment, "MRSM_PATCH", &patch, true),
            None => {
                return (
                    None,
                    vec![
                        "`valkey:main` has no password and the stack's `api` service's \
                         `MRSM_PATCH` is not JSON: the stack's Valkey runs without a password. \
                         Set `meerschaum:connectors:valkey:main:password`."
                            .into(),
                    ],
                );
            }
        }
    }
    let warning = format!(
        "`valkey:main` has no password, so the stack's Valkey requires a generated one, stored \
         in {path}. Set `meerschaum:connectors:valkey:main:password` to `MRSM{{file:{path}}}` so \
         `valkey:main` on this host can connect.",
        path = path.display()
    );
    (Some(password), vec![warning])
}

/// The api service's `MRSM_PATCH` with `valkey:main`'s generated password (see
/// [`stack_valkey_password`]). `None` when the variable is not JSON.
fn patched_valkey_patch(existing: Option<&str>, password: &str) -> Option<String> {
    let mut patch: serde_json::Value = match existing.map(str::trim).filter(|s| !s.is_empty()) {
        Some(text) => serde_json::from_str(text).ok()?,
        None => serde_json::json!({}),
    };
    let mut node = patch.as_object_mut()?;
    for key in ["meerschaum", "connectors", "valkey", "main"] {
        node = node
            .entry(key)
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()?;
    }
    node.insert("password".into(), password.into());
    serde_json::to_string_pretty(&patch).ok()
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
    let value = with_grafana_sign_in(rt, keys, value);
    let value = with_main_readonly(rt, keys, value, true);
    let value = with_secret_files(rt, keys, value, true);
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
        let value = with_grafana_sign_in(rt, &keys, value);
        // `sync_stack_files` below writes them again, with the warnings.
        let value = with_main_readonly(rt, &keys, value, false);
        let value = with_secret_files(rt, &keys, value, false);
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
        scrub_secrets: true,
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

#[cfg(test)]
mod grafana_tests {
    use super::*;
    use std::collections::HashMap;

    fn map(json: &str) -> Map {
        Value::from_json_str(json).unwrap().to_map()
    }

    fn env(list: &[(&'static str, String)]) -> HashMap<&'static str, String> {
        list.iter().cloned().collect()
    }

    #[test]
    fn the_sign_in_environment_trusts_the_api_servers_address_only() {
        let api: std::net::Ipv4Addr = "10.213.71.2".parse().unwrap();
        let sign_in = grafana_sign_in_environment(&Map::new(), "generated", Some(api));
        let forced = env(&sign_in.forced);
        assert_eq!(forced["GF_AUTH_PROXY_ENABLED"], "true");
        assert_eq!(forced["GF_AUTH_PROXY_HEADER_NAME"], "X-WEBAUTH-USER");
        // Exactly the API server's address, never a network range.
        assert_eq!(forced["GF_AUTH_PROXY_WHITELIST"], "10.213.71.2");
        assert_eq!(forced["GF_SECURITY_ADMIN_USER"], "admin");
        assert_eq!(forced["GF_SECURITY_ADMIN_PASSWORD"], "generated");
        // Anonymous access is not Meerschaum's to change.
        assert!(!forced.keys().any(|k| k.starts_with("GF_AUTH_ANONYMOUS")));
        assert!(sign_in.warning.is_none());
        let defaults = env(&sign_in.defaults);
        assert_eq!(defaults["GF_SERVER_SERVE_FROM_SUB_PATH"], "true");
        assert!(defaults["GF_SERVER_ROOT_URL"].ends_with("/grafana/"));

        // Lists are joined with commas (Grafana's syntax), not a Python repr.
        let section = map(r#"{"root_url": "https://data.example.com/grafana/",
                "proxy_whitelist": ["10.1.2.3", "10.1.2.4"], "admin_user": "root"}"#);
        let sign_in = grafana_sign_in_environment(&section, "s3cret", Some(api));
        let forced = env(&sign_in.forced);
        assert_eq!(forced["GF_AUTH_PROXY_WHITELIST"], "10.1.2.3,10.1.2.4");
        assert_eq!(forced["GF_SECURITY_ADMIN_USER"], "root");
        assert_eq!(
            env(&sign_in.defaults)["GF_SERVER_ROOT_URL"],
            "https://data.example.com/grafana/"
        );
    }

    #[test]
    fn without_an_address_the_auth_proxy_is_off() {
        let sign_in = grafana_sign_in_environment(&Map::new(), "generated", None);
        let forced = env(&sign_in.forced);
        assert_eq!(forced["GF_AUTH_PROXY_ENABLED"], "false");
        assert_eq!(forced["GF_AUTH_PROXY_WHITELIST"], "");
        assert!(sign_in.warning.unwrap().contains("proxy_whitelist"));
        // A configured whitelist is enough.
        let sign_in =
            grafana_sign_in_environment(&map(r#"{"proxy_whitelist": "192.0.2.7"}"#), "g", None);
        assert_eq!(env(&sign_in.forced)["GF_AUTH_PROXY_ENABLED"], "true");
    }

    #[test]
    fn the_proxy_subnet_gives_fixed_addresses() {
        let (subnet, api, grafana) = grafana_proxy_addresses(&Map::new()).unwrap();
        assert_eq!(subnet, "10.213.71.0/29");
        assert_eq!(api.to_string(), "10.213.71.2");
        assert_eq!(grafana.to_string(), "10.213.71.3");
        let (subnet, api, _) =
            grafana_proxy_addresses(&map(r#"{"proxy_subnet": "10.9.8.77/28"}"#)).unwrap();
        assert_eq!(subnet, "10.9.8.64/28");
        assert_eq!(api.to_string(), "10.9.8.66");
        for bad in ["10.0.0.0/30", "10.0.0.0", "nonsense/24", "10.0.0.0/7"] {
            assert!(
                grafana_proxy_addresses(&map(&format!(r#"{{"proxy_subnet": "{bad}"}}"#))).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn security_settings_override_the_stack_and_the_rest_is_kept() {
        let mut environment = Value::Map(map(
            r#"{"GF_AUTH_PROXY_WHITELIST": "172.16.0.0/12", "GF_SERVER_ROOT_URL": "mine",
                "GF_AUTH_ANONYMOUS_ENABLED": "true"}"#,
        ));
        set_environment(
            &mut environment,
            "GF_AUTH_PROXY_WHITELIST",
            "10.213.71.2",
            true,
        );
        set_environment(&mut environment, "GF_SERVER_ROOT_URL", "theirs", false);
        let m = environment.to_map();
        assert_eq!(m["GF_AUTH_PROXY_WHITELIST"].py_str(), "10.213.71.2");
        assert_eq!(m["GF_SERVER_ROOT_URL"].py_str(), "mine");
        assert_eq!(m["GF_AUTH_ANONYMOUS_ENABLED"].py_str(), "true");
        let mut list = Value::List(vec![Value::Str("GF_SECURITY_ADMIN_PASSWORD=admin".into())]);
        set_environment(&mut list, "GF_SECURITY_ADMIN_PASSWORD", "generated", true);
        assert_eq!(
            list,
            Value::List(vec![Value::Str(
                "GF_SECURITY_ADMIN_PASSWORD=generated".into()
            )])
        );
    }

    #[test]
    fn the_api_server_is_given_the_password() {
        let patch = patched_api_patch(Some(r#"{"meerschaum": {"x": 1}}"#), "pw").unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&patch).unwrap();
        assert_eq!(parsed["api"]["grafana"]["admin_password"], "pw");
        assert_eq!(parsed["meerschaum"]["x"], 1);
        assert!(patched_api_patch(Some("not json"), "pw").is_none());
        assert!(patched_api_patch(None, "pw").is_some());
    }
}

#[cfg(test)]
mod compose_writer_tests {
    use super::*;
    use meerschaum_core::config::{Config, ConfigOptions};

    fn compose_after(
        root: &std::path::Path,
        env: &[(&str, &str)],
        stack_first: bool,
    ) -> (String, String) {
        let mut vars: Vec<(String, String)> =
            vec![("MRSM_ROOT_DIR".into(), root.to_string_lossy().into_owned())];
        vars.extend(env.iter().map(|(k, v)| (k.to_string(), v.to_string())));
        let mut opts = ConfigOptions::with_env(vars);
        opts.allow_writes = true;
        opts.home = root.to_path_buf();
        let mut out = Vec::new();
        let mut rt = Runtime::new(
            crate::types::DateContext::system(),
            80,
            &mut out,
            |_: &str| None,
        );
        rt.err = Box::new(std::io::sink());
        rt.config = Some(Config::new(opts).unwrap());
        let path = rt.config().paths().path("STACK_COMPOSE_PATH");
        let read = |path: &std::path::Path| std::fs::read_to_string(path).unwrap_or_default();
        let core = |rt: &mut Runtime<'_>| {
            rt.config()
                .with_state(|s| s.sync_files(&["stack"]))
                .unwrap();
        };
        if stack_first {
            write_stack(&mut rt).unwrap();
            let by_stack = read(&path);
            core(&mut rt);
            (by_stack, read(&path))
        } else {
            core(&mut rt);
            let by_core = read(&path);
            write_stack(&mut rt).unwrap();
            (read(&path), by_core)
        }
    }

    /// `mrsm stack` writes the hardened compose file, and the core `sync_files` (Python's first
    /// read of the `stack` config) never downgrades it: one result on disk.
    #[test]
    fn the_core_sync_never_downgrades_the_stacks_compose_file() {
        for env in [
            vec![],
            vec![("MRSM_CONFIG", r#"{"api": {"grafana": {"enabled": true}}}"#)],
        ] {
            let dir = tempfile::tempdir().unwrap();
            let (by_stack, after_core) = compose_after(dir.path(), &env, true);
            assert!(by_stack.contains("grafana_readonly"), "{by_stack}");
            if !env.is_empty() {
                assert!(by_stack.contains(GRAFANA_PROXY_NETWORK), "{by_stack}");
                assert!(by_stack.contains("GF_AUTH_PROXY_ENABLED"), "{by_stack}");
            }
            assert_eq!(
                after_core, by_stack,
                "the core sync rewrote the compose file"
            );

            // The core sync first: it leaves the compose file to `mrsm stack`.
            let dir = tempfile::tempdir().unwrap();
            let (by_stack, by_core) = compose_after(dir.path(), &env, false);
            assert!(
                by_core.is_empty() || by_core == by_stack,
                "the core sync wrote an unhardened compose file:\n{by_core}"
            );
        }
    }

    /// A stack without a `grafana` service and without the `api` service's configuration is
    /// still written by the core sync (nothing to harden). 5.1: with the `api` service's
    /// `MRSM_CONFIG`, the core sync leaves it to `mrsm stack`, which moves it to a file.
    #[test]
    fn the_core_sync_writes_a_compose_file_without_grafana() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("config")).unwrap();
        // 5.1: nor a `valkey` service (`mrsm stack` gives it its password in a file).
        let patch = r#"{"stack": {"docker-compose.yaml": {"services":
            {"grafana": null, "api": null, "valkey": null}}}}"#;
        let (by_stack, by_core) = compose_after(dir.path(), &[("MRSM_CONFIG", patch)], false);
        assert!(!by_core.is_empty());
        assert!(!by_core.contains("grafana_readonly"), "{by_core}");
        assert!(by_stack.contains("services:"), "{by_stack}");

        let dir = tempfile::tempdir().unwrap();
        let patch = r#"{"stack": {"docker-compose.yaml": {"services": {"grafana": null}}}}"#;
        let (by_stack, by_core) = compose_after(dir.path(), &[("MRSM_CONFIG", patch)], false);
        assert!(by_core.is_empty() || by_core == by_stack, "{by_core}");
        assert!(by_stack.contains("MRSM_CONFIG_FILE"), "{by_stack}");
    }

    /// A root whose files `write_stack` (and then the core sync) wrote, with `env` and an
    /// optional `stack.yaml`: `(compose text, secrets directory)`.
    fn stack_files(
        root: &std::path::Path,
        env: &[(&str, &str)],
        stack_yaml: Option<&str>,
    ) -> (String, std::path::PathBuf) {
        if let Some(text) = stack_yaml {
            std::fs::create_dir_all(root.join("config")).unwrap();
            std::fs::write(root.join("config").join("stack.yaml"), text).unwrap();
        }
        let (by_stack, after_core) = compose_after(root, env, true);
        assert_eq!(
            by_stack, after_core,
            "the core sync rewrote the compose file"
        );
        (by_stack, root.join("stack").join("secrets"))
    }

    const SECRET_CONFIG: &str = r#"{"meerschaum": {"connectors": {
        "sql": {"main": {"username": "mrsm", "password": "pg-s3cret-xyz"}},
        "api": {"main": {"password": "api-s3cret-xyz"}}}},
        "api": {"grafana": {"enabled": true, "admin_password": "gf-s3cret-xyz"}}}"#;

    fn assert_no_secret(text: &str) {
        for secret in ["pg-s3cret-xyz", "api-s3cret-xyz", "gf-s3cret-xyz"] {
            assert!(!text.contains(secret), "{secret} in:\n{text}");
        }
    }

    /// 5.1: the default stack's compose file holds paths, not credentials; the `api` service
    /// reads them from files only its owner's directory reaches.
    #[test]
    fn the_stack_passes_the_api_its_configuration_by_file() {
        let dir = tempfile::tempdir().unwrap();
        let (compose, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", SECRET_CONFIG)], None);
        assert_no_secret(&compose);
        assert!(
            compose.contains("MRSM_CONFIG_FILE: /run/secrets/meerschaum/mrsm_config.json"),
            "{compose}"
        );
        assert!(compose.contains("MRSM_PATCH_FILE:"), "{compose}");
        assert!(
            compose.contains("GF_SECURITY_ADMIN_PASSWORD__FILE:"),
            "{compose}"
        );
        assert!(
            compose.contains(super::super::stack_secrets::REVISION_LABEL),
            "{compose}"
        );
        let config = std::fs::read_to_string(secrets.join("api").join("mrsm_config.json")).unwrap();
        let config: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(
            config["meerschaum"]["connectors"]["sql"]["main"]["password"],
            "pg-s3cret-xyz"
        );
        assert!(config["system"].is_object());
        let patch = std::fs::read_to_string(secrets.join("api").join("mrsm_patch.json")).unwrap();
        let patch: serde_json::Value = serde_json::from_str(&patch).unwrap();
        assert_eq!(
            patch["meerschaum"]["connectors"]["sql"]["main"]["host"],
            "db"
        );
        assert_eq!(patch["api"]["grafana"]["admin_password"], "gf-s3cret-xyz");
        let owner =
            std::fs::read_to_string(secrets.join("grafana_readonly").join("owner_password"))
                .unwrap();
        assert_eq!(owner, "pg-s3cret-xyz");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&secrets), 0o700);
            assert_eq!(mode(&secrets.join("api")), 0o755);
            assert_eq!(mode(&secrets.join("api").join("mrsm_config.json")), 0o644);
        }
    }

    /// Upgrades: a `stack.yaml` holding the old default entries (as `MRSM{...}` references or
    /// materialized) is moved to files; a customized entry's own keys reach the container.
    #[test]
    fn an_upgraded_stack_yaml_moves_to_files_and_keeps_customized_keys() {
        let references = r#"
docker-compose.yaml:
  services:
    api:
      environment:
        MRSM_CONFIG: '{"meerschaum": MRSM{!meerschaum}, "system": MRSM{!system}, "api": MRSM{!api}}'
        MRSM_PATCH: '{"meerschaum": {"connectors": {"sql": {"main": {"host": "db", "port": 5432}}}}}'
"#;
        let materialized = r#"
docker-compose.yaml:
  services:
    api:
      environment:
        MRSM_CONFIG: '{"meerschaum": {"connectors": {"sql": {"main": {"password": "pg-s3cret-xyz"}}}}, "custom": {"kept": "yes"}}'
"#;
        for (stack_yaml, custom) in [(references, false), (materialized, true)] {
            let dir = tempfile::tempdir().unwrap();
            let (compose, secrets) = stack_files(
                dir.path(),
                &[("MRSM_CONFIG", SECRET_CONFIG)],
                Some(stack_yaml),
            );
            assert_no_secret(&compose);
            let config =
                std::fs::read_to_string(secrets.join("api").join("mrsm_config.json")).unwrap();
            assert!(config.contains("pg-s3cret-xyz"), "{config}");
            assert_eq!(config.contains("\"kept\": \"yes\""), custom, "{config}");
            // The user's config file is never rewritten.
            assert_eq!(
                std::fs::read_to_string(dir.path().join("config").join("stack.yaml")).unwrap(),
                stack_yaml
            );
        }
    }

    /// 5.1: the stack's Valkey requires `valkey:main`'s password (its images ignore
    /// `VALKEY_PASSWORD`): a configuration file in its own secrets subdirectory, mounted
    /// read-only, and no password in the compose file.
    #[test]
    fn the_stacks_valkey_requires_the_configured_password() {
        let dir = tempfile::tempdir().unwrap();
        let config = r#"{"meerschaum": {"connectors": {"valkey": {"main":
            {"password": "vk-s3cret-\"xyz"}}}}}"#;
        let (compose, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", config)], None);
        assert!(!compose.contains("vk-s3cret"), "{compose}");
        assert!(!compose.contains("VALKEY_PASSWORD"), "{compose}");
        assert!(
            compose.contains("- /run/secrets/meerschaum/valkey.conf"),
            "{compose}"
        );
        let conf = std::fs::read_to_string(secrets.join("valkey").join("valkey.conf")).unwrap();
        assert!(conf.contains(r#"requirepass "vk-s3cret-\"xyz""#), "{conf}");
        // The default password (an upgraded root that never set one).
        let dir = tempfile::tempdir().unwrap();
        let (_, secrets) = stack_files(dir.path(), &[], None);
        let conf = std::fs::read_to_string(secrets.join("valkey").join("valkey.conf")).unwrap();
        assert!(conf.contains(r#"requirepass "mrsm""#), "{conf}");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&secrets.join("valkey")), 0o755);
            assert_eq!(mode(&secrets.join("valkey").join("valkey.conf")), 0o644);
        }
    }

    /// 5.1: `valkey:main` without a password: one is generated once (owner-only), the stack's
    /// Valkey requires it and the `api` service is given it; the next write keeps it.
    #[test]
    fn a_stack_without_a_valkey_password_gets_a_generated_one() {
        let dir = tempfile::tempdir().unwrap();
        let config = r#"{"meerschaum": {"connectors": {"valkey": {"main": {"password": null}}}}}"#;
        let (compose, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", config)], None);
        let generated = std::fs::read_to_string(secrets.join(".valkey_password")).unwrap();
        assert_eq!(generated.len(), 64);
        assert!(!compose.contains(&generated), "{compose}");
        let conf = std::fs::read_to_string(secrets.join("valkey").join("valkey.conf")).unwrap();
        assert!(
            conf.contains(&format!("requirepass \"{generated}\"")),
            "{conf}"
        );
        let patch = std::fs::read_to_string(secrets.join("api").join("mrsm_patch.json")).unwrap();
        let patch: serde_json::Value = serde_json::from_str(&patch).unwrap();
        assert_eq!(
            patch["meerschaum"]["connectors"]["valkey"]["main"]["password"],
            generated.as_str()
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(secrets.join(".valkey_password"))
                .unwrap()
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600);
        }
        let (_, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", config)], None);
        assert_eq!(
            std::fs::read_to_string(secrets.join(".valkey_password")).unwrap(),
            generated
        );
    }

    /// An image older than 5.1.0 keeps the environment form (its API server would start without
    /// its configuration otherwise), and the files of a previous write are removed.
    #[test]
    fn an_old_image_keeps_the_environment_form() {
        let dir = tempfile::tempdir().unwrap();
        let (_, secrets) = stack_files(dir.path(), &[("MRSM_CONFIG", SECRET_CONFIG)], None);
        assert!(secrets.join("api").join("mrsm_config.json").exists());
        let old = r#"
docker-compose.yaml:
  services:
    api:
      image: bmeares/meerschaum:5.0.2-full
"#;
        let (compose, secrets) =
            stack_files(dir.path(), &[("MRSM_CONFIG", SECRET_CONFIG)], Some(old));
        assert!(compose.contains("MRSM_CONFIG: "), "{compose}");
        assert!(!compose.contains("MRSM_CONFIG_FILE"), "{compose}");
        assert!(!secrets.join("api").join("mrsm_config.json").exists());
        // `stack:api_config_files: true` is the way for an image the rule cannot vouch for.
        let custom = r#"
api_config_files: true
docker-compose.yaml:
  services:
    api:
      image: example/meerschaum-custom:1
"#;
        let (compose, _) = stack_files(dir.path(), &[("MRSM_CONFIG", SECRET_CONFIG)], Some(custom));
        assert!(compose.contains("MRSM_CONFIG_FILE"), "{compose}");
        assert_no_secret(&compose);
    }
}
