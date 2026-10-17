//! The connector-configuration actions of `meerschaum/actions/{register,copy,delete,start}.py`:
//! `register connectors`, `copy connectors`, `delete connectors`, `delete config` and
//! `start connectors`, plus `parse_connector_keys` over the config registry (a connector is
//! validated from its attributes; nothing connects until `start connectors`).

use meerschaum_core::SuccessTuple;
use meerschaum_core::value::{Map, Value};
use meerschaum_sql::uri::{ConnectionOptions, ConnectionSpec, ConnectorAttributes};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::rich::console::Renderable;
use crate::format::rich::pretty::PyObj;
use crate::format::rich::table::{Column, MINIMAL, Table};
use crate::format::rich::text::Text;
use crate::value::ArgsMap;

use super::ask::{Ask, ask};
use super::session::{connector_attributes, split_keys};
use super::{flag, info, pprint, session, warn, words, yes_no};

/// A connector resolved from its keys (`parse_connector_keys`).
#[derive(Debug, Clone)]
pub struct Parsed {
    /// `connector.type`.
    pub typ: String,
    /// `connector.label`.
    pub label: String,
    /// The attributes (`type:default` patched with the label's section).
    pub attrs: Map,
    /// `connector.flavor` (`sql` connectors).
    pub flavor: Option<String>,
    /// `connector.database` (`sql` connectors).
    pub database: Option<String>,
}

impl Parsed {
    /// `str(connector)`.
    pub fn keys(&self) -> String {
        format!("{}:{}", self.typ, self.label)
    }
}

/// `error(msg, stack=False)` printed without raising (the exception text v4 logs before it
/// reports a failure).
pub fn print_error(rt: &mut Runtime<'_>, message: &str) {
    let text = rt.with_formatter(|f| {
        let line = format!(" \u{1f6d1} {message}");
        if f.settings.ansi {
            format!("\n{}\n", f.fill_ansi(&line, "bold red"))
        } else {
            format!("\n{line}\n")
        }
    });
    rt.eprint(&text);
}

fn attr_str(attrs: &Map, key: &str) -> Option<String> {
    match attrs.get(key) {
        None | Some(Value::Null) => None,
        Some(v) => Some(v.py_str()),
    }
}

/// `SQLConnector.__init__` + `create_engine()`'s attribute resolution, without connecting.
pub(super) fn sql_spec(label: &str, attrs: &Map) -> Result<ConnectionSpec, String> {
    let mut ca = ConnectorAttributes {
        label: Some(label.to_string()),
        ..Default::default()
    };
    for (k, v) in attrs {
        if v.is_null() {
            continue;
        }
        let s = v.py_str();
        match k.as_str() {
            "flavor" => {
                ca.flavor = Some(
                    s.parse::<meerschaum_sql::flavor::Flavor>()
                        .map_err(|e| e.to_string())?,
                )
            }
            "uri" => ca.uri = Some(s),
            "host" => ca.host = Some(s),
            "port" => ca.port = s.trim().parse::<u16>().ok(),
            "username" => ca.username = Some(s),
            "password" => ca.password = Some(s),
            "database" => ca.database = Some(s),
            "schema" => ca.schema = Some(s),
            "options" => {
                ca.options = Some(match v {
                    Value::Map(m) => ConnectionOptions::Pairs(
                        m.iter().map(|(k, v)| (k.clone(), v.py_str())).collect(),
                    ),
                    _ => ConnectionOptions::Str(s),
                })
            }
            "pool_size" => ca.pool_size = s.trim().parse().ok(),
            "max_overflow" => ca.max_overflow = s.trim().parse().ok(),
            other => ca.extra.push((other.to_string(), s)),
        }
    }
    ConnectionSpec::from_attributes(&ca).map_err(|e| e.to_string())
}

/// `parse_connector_keys(keys)`: the connector the keys name, or `None` after the warnings v4
/// logs when it cannot be built.
pub fn parse_connector_keys(rt: &mut Runtime<'_>, keys: &str) -> Option<Parsed> {
    let parsed = parse_connector_keys_inner(rt, keys);
    // `get_connector()` reads the config with `warn=False`.
    let _ = rt.config().take_warnings();
    parsed
}

fn parse_connector_keys_inner(rt: &mut Runtime<'_>, keys: &str) -> Option<Parsed> {
    let (typ, label) = split_keys(keys);
    let full = format!("{typ}:{label}");
    match typ.as_str() {
        "sql" => {
            let mut attrs = connector_attributes(rt.config(), "sql", &label);
            let flavor = attrs.get("flavor").map(Value::py_str).unwrap_or_default();
            if matches!(flavor.as_str(), "sqlite" | "geopackage") {
                // PARITY: SQLite / GeoPackage connectors do not inherit `sql:default` and
                // `{SQLITE_DB_PATH}` is the root's SQLite file (`SQLConnector.create_engine`).
                attrs = rt
                    .config()
                    .get(&["meerschaum", "connectors", "sql", &label])
                    .map(|v| v.to_map())
                    .unwrap_or_default();
                let db = attr_str(&attrs, "database");
                if db.as_deref() == Some("{SQLITE_DB_PATH}") || (label == "local" && db.is_none()) {
                    let path = rt.config().paths().path("SQLITE_DB_PATH");
                    attrs.insert(
                        "database".into(),
                        Value::Str(path.to_string_lossy().into_owned()),
                    );
                }
            }
            let failure = if attrs.is_empty() {
                Some(format!(
                    "    Missing flavor. Provide flavor as a key for '{full}'."
                ))
            } else {
                sql_spec(&label, &attrs).err()
            };
            if let Some(message) = failure {
                warn(
                    rt,
                    &format!("Exception when creating connector '{full}'.\n{message}"),
                );
                return None;
            }
            let spec = sql_spec(&label, &attrs).ok()?;
            Some(Parsed {
                typ,
                label,
                flavor: Some(spec.flavor.to_string()),
                database: spec.database.clone(),
                attrs,
            })
        }
        "api" | "valkey" => {
            let attrs = connector_attributes(rt.config(), &typ, &label);
            if attr_str(&attrs, "host").is_none() && attr_str(&attrs, "uri").is_none() {
                warn(rt, &format!("Incorrect attributes for connector '{full}'."));
                print_error(rt, &format!("Missing 'host' for connector '{full}'."));
                return None;
            }
            Some(Parsed {
                typ,
                label,
                attrs,
                flavor: None,
                database: None,
            })
        }
        "plugin" => {
            let installed = crate::builtin::plugin_names(&rt.config().paths().plugins_dirs);
            if !installed.contains(&label) {
                print_error(
                    rt,
                    &format!("Plugin '{label}' cannot be found. Is it installed?"),
                );
                warn(rt, &format!("Exception when creating connector '{full}'."));
                return None;
            }
            Some(Parsed {
                typ,
                label,
                attrs: Map::new(),
                flavor: None,
                database: None,
            })
        }
        _ => {
            let custom = crate::python::manifest(rt)
                .ok()
                .is_some_and(|m| m.connector_type(&typ).is_some());
            if !custom {
                warn(rt, &format!("Cannot create Connector of type '{typ}'."));
                return None;
            }
            Some(Parsed {
                attrs: connector_attributes(rt.config(), &typ, &label),
                typ,
                label,
                flavor: None,
                database: None,
            })
        }
    }
}

/// `get_connector(keys)` for an instance: the warnings v4 logs when the keys cannot be built,
/// then the session's connector (`None` when it cannot be built).
pub fn connect_instance(rt: &mut Runtime<'_>, keys: &str) -> Option<super::Instance> {
    // Custom connector types have no attribute validation here: the session builds them.
    parse_connector_keys(rt, keys)?;
    let sess = session(rt);
    sess.instance(keys, rt.config()).ok()
}

/// The `meerschaum` config section (`get_config('meerschaum')`).
fn meerschaum_section(rt: &mut Runtime<'_>) -> Map {
    rt.config()
        .get(&["meerschaum"])
        .map(|v| v.to_map())
        .unwrap_or_default()
}

/// `register connectors <type:label> --params ...`.
pub fn register_connectors(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut all_keys = words(args, "action");
    all_keys.extend(words(args, "connector_keys"));
    let params = super::params(args);
    register_connector_keys(rt, args, &all_keys, params)
}

/// The body of `_register_connectors(action, connector_keys, params, **kwargs)`.
pub fn register_connector_keys(
    rt: &mut Runtime<'_>,
    args: &ArgsMap,
    all_keys: &[String],
    params: Option<Map>,
) -> ActionResult {
    if all_keys.len() != 1 {
        return Ok(SuccessTuple::fail(
            "Provide one pair of keys for the connector to be registered.",
        ));
    }
    let keys = &all_keys[0];
    if keys.matches(':').count() != 1 {
        return Ok(SuccessTuple::fail(
            "Connector keys must be in the format `type:label`.",
        ));
    }
    let (type_, label) = keys.split_once(':').expect("one colon");
    let mut mrsm_config = meerschaum_section(rt);
    let connectors = mrsm_config
        .entry("connectors".to_string())
        .or_insert_with(|| Value::Map(Map::new()));
    // PARITY: `if type_ not in mrsm_config['connectors']: mrsm_config['connectors'] = {}`
    // (`actions/register.py`) empties the section, and the next line raises `KeyError`.
    let Some(existing_type) = connectors.get(type_).cloned() else {
        return Err(ActionError::new("KeyError", format!("'{type_}'")));
    };
    let mut is_new = true;
    if let Some(existing) = existing_type.get(label) {
        let new_params = params.clone().map(Value::Map).unwrap_or(Value::Null);
        if *existing == new_params {
            return Ok(SuccessTuple::ok("Connector exists, nothing to do."));
        }
        let existing = existing.clone();
        let shown_new = match &params {
            Some(p) => Value::Map(p.clone()),
            None => Value::Map(Map::new()),
        };
        let text = rt.with_formatter(|f| {
            let mut table = Table::new();
            table.box_chars = Some(MINIMAL);
            for header in ["Existing Parameters", "New Parameters"] {
                table.add_column(Column {
                    header: Text::plain_text(header),
                    ..Column::default()
                });
            }
            table.add_row(vec![
                Some(Box::new(super::rich_json::json_text(&existing)) as Box<dyn Renderable>),
                Some(Box::new(super::rich_json::json_text(&shown_new))),
            ]);
            f.console().print(&table)
        });
        rt.print(&text);
        warn(rt, &format!("Connector '{keys}' already exists."));
        if !yes_no(
            rt,
            &format!("Do you want to overwrite connector '{keys}'?"),
            "n",
            flag(args, "yes"),
            flag(args, "noask"),
        )? {
            return Ok(SuccessTuple::fail("Nothing was changed."));
        }
        is_new = false;
    }
    let value = params.map(Value::Map).unwrap_or(Value::Null);
    if let Some(Value::Map(connectors)) = mrsm_config.get_mut("connectors") {
        match connectors.get_mut(type_) {
            Some(Value::Map(by_label)) => {
                by_label.insert(label.to_string(), value);
            }
            _ => {
                let mut by_label = Map::new();
                by_label.insert(label.to_string(), value);
                connectors.insert(type_.to_string(), Value::Map(by_label));
            }
        }
    }
    if !write_meerschaum(rt, mrsm_config) {
        return Ok(SuccessTuple::fail("Failed to update configuration."));
    }
    Ok(SuccessTuple::ok(format!(
        "Successfully {} connector '{keys}'.",
        if is_new { "registered" } else { "updated" }
    )))
}

/// `write_config({'meerschaum': mrsm_config})` (and refresh the in-memory section).
fn write_meerschaum(rt: &mut Runtime<'_>, section: Map) -> bool {
    let mut config = Map::new();
    config.insert("meerschaum".into(), Value::Map(section));
    let ok = rt.config().write_config(Some(&config), None);
    flush_config_warnings(rt);
    ok
}

/// Print the warnings the config registry recorded (`warn(..., stack=False)`).
pub fn flush_config_warnings(rt: &mut Runtime<'_>) {
    for message in rt.config().take_warnings() {
        warn(rt, &message);
    }
}

/// `copy connectors <type:label> [<new keys>]`.
pub fn copy_connectors(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut keys = words(args, "action");
    keys.extend(words(args, "connector_keys"));
    if keys.is_empty() {
        return Ok(SuccessTuple::fail("No connectors to copy."));
    }
    if keys.len() > 2 {
        return Ok(SuccessTuple::fail("Provide one set of connector keys."));
    }
    let ck = keys[0].clone();
    let Some(conn) = parse_connector_keys(rt, &ck) else {
        return Ok(SuccessTuple::fail(format!(
            "Unable to parse connector '{ck}'."
        )));
    };
    let (mut new_ck, mut new_label) = (None, None);
    if keys.len() == 2 {
        new_ck = keys[1].contains(':').then(|| keys[1].clone());
        new_label = keys[1].rsplit(':').next().map(str::to_string);
    }
    let new_label = match new_label {
        Some(l) => l,
        None => ask(
            rt,
            &format!("Enter a label for the new '{}' connector:", conn.typ),
            &Ask::new(),
        )?,
    };
    let new_ck = new_ck.unwrap_or_else(|| format!("{}:{new_label}", conn.typ));
    info(
        rt,
        &format!("Registering connector '{new_ck}' from '{ck}'..."),
    );
    let attrs = rt
        .config()
        .get(&["meerschaum", "connectors", &conn.typ, &conn.label])
        .unwrap_or(Value::Null);
    pprint(rt, &PyObj::from_value(&attrs), flag(args, "nopretty"));
    if !flag(args, "force")
        && !yes_no(
            rt,
            &format!("Register connector '{new_ck}' with the above attributes?"),
            "n",
            flag(args, "yes"),
            flag(args, "noask"),
        )?
    {
        return Ok(SuccessTuple::fail("Nothing was copied."));
    }
    let params = match attrs {
        Value::Map(m) => Some(m),
        _ => None,
    };
    register_connector_keys(rt, args, &[new_ck], params)
}

/// `delete connectors <type:label> ...`.
pub fn delete_connectors(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut keys: Vec<String> = Vec::new();
    for k in words(args, "action")
        .into_iter()
        .chain(words(args, "connector_keys"))
    {
        if !keys.contains(&k) {
            keys.push(k);
        }
    }
    if keys.is_empty() {
        return Ok(SuccessTuple::fail("No connectors to delete."));
    }
    let (yes, force, noask) = (flag(args, "yes"), flag(args, "force"), flag(args, "noask"));
    let mut to_delete: Vec<Parsed> = Vec::new();
    for ck in &keys {
        let Some(conn) = parse_connector_keys(rt, ck) else {
            warn(
                rt,
                &format!("Could not parse connector '{ck}'. Skipping..."),
            );
            continue;
        };
        if !force
            && !yes_no(
                rt,
                &format!(
                    "Are you sure you want to delete connector '{}' from the configuration file?",
                    conn.keys()
                ),
                "n",
                yes,
                noask,
            )?
        {
            info(rt, &format!("Skipping connector '{}'...", conn.keys()));
            continue;
        }
        to_delete.push(conn);
    }
    if to_delete.is_empty() {
        return Ok(SuccessTuple::fail(
            "No changes made to the configuration file.",
        ));
    }
    // `cf = _config()` holds only the sections v4 had loaded and rewrites all of them; the
    // `meerschaum` section is the only one this action changes, so only it is written.
    let mut config = Map::new();
    config.insert("meerschaum".into(), Value::Map(meerschaum_section(rt)));
    for c in &to_delete {
        if let (Some(flavor), Some(database)) = (&c.flavor, &c.database)
            && matches!(flavor.as_str(), "sqlite" | "duckdb" | "geopackage")
            && !database.contains(":memory:")
            && std::path::Path::new(database).exists()
            && (force
                // PARITY: the whole block runs under `except Exception: pass`, so a failed
                // prompt (`EOFError`) counts as "keep the file" and the connector is still
                // removed from the configuration (`actions/delete.py`).
                || yes_no(
                    rt,
                    &format!("Detected '{flavor}' database '{database}'. Delete this file?"),
                    "n",
                    yes,
                    noask,
                )
                .unwrap_or(false))
            && std::fs::remove_file(database).is_err()
        {
            warn(
                rt,
                &format!(
                    "Failed to delete database file for connector '{}'. Ignoring...",
                    c.keys()
                ),
            );
        }
        let removed = match config.get_mut("meerschaum") {
            Some(Value::Map(section)) => match section.get_mut("connectors") {
                Some(Value::Map(connectors)) => match connectors.get_mut(&c.typ) {
                    Some(Value::Map(by_label)) => by_label.shift_remove(&c.label).is_some(),
                    _ => false,
                },
                _ => false,
            },
            _ => false,
        };
        if !removed {
            warn(
                rt,
                &format!(
                    "Failed to delete connector '{}' from configuration. Skipping...",
                    c.keys()
                ),
            );
        }
    }
    rt.config().write_config(Some(&config), None);
    flush_config_warnings(rt);
    Ok(SuccessTuple::ok("Success"))
}

/// `delete config [keys...]`.
pub fn delete_config(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let paths = rt.config().paths();
    let mut file_paths: Vec<std::path::PathBuf> = ["STACK_COMPOSE_PATH", "DEFAULT_CONFIG_DIR_PATH"]
        .iter()
        .map(|n| paths.path(n))
        .filter(|p| p.exists())
        .collect();
    let action = words(args, "action");
    let keys = if action.is_empty() {
        crate::builtin::possible_config_keys(rt)
    } else {
        action
    };
    for k in &keys {
        let path = rt
            .config()
            .with_state(|s| s.get_keyfile_path(k, false, None));
        if let Some(p) = path {
            file_paths.push(p);
        }
    }
    let force = flag(args, "force");
    let answer = if force || file_paths.is_empty() {
        true
    } else {
        let sep = "\n  - ";
        let listing: Vec<String> = file_paths.iter().map(|p| p.display().to_string()).collect();
        yes_no(
            rt,
            &format!(
                "Are you sure you want to delete the following configuration files?{}{}\n",
                sep,
                listing.join(sep)
            ),
            "n",
            flag(args, "yes"),
            flag(args, "noask"),
        )?
    };
    if !answer {
        return Ok(SuccessTuple::fail("Nothing deleted."));
    }
    for path in &file_paths {
        if path.is_file() {
            let _ = std::fs::remove_file(path);
        } else if path.is_dir() {
            let _ = std::fs::remove_dir_all(path);
        }
    }
    Ok(SuccessTuple::ok("Success"))
}

/// `start connectors <keys> ...`: poll the connectors until a connection can be made
/// (`retry_connect`).
pub fn start_connectors(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut keys: Vec<String> = Vec::new();
    for k in words(args, "action")
        .into_iter()
        .chain(words(args, "connector_keys"))
    {
        if !keys.contains(&k) {
            keys.push(k);
        }
    }
    // The keys are validated without connecting (`parse_instance_keys` only builds the object);
    // connection failures are retried below.
    let mut valid: Vec<String> = Vec::new();
    let mut endpoints: Vec<Option<(String, u16)>> = Vec::new();
    let sess = session(rt);
    for k in &keys {
        match parse_connector_keys(rt, k) {
            Some(parsed) => {
                let normalized = format!("{}:{}", parsed.typ, parsed.label);
                if !valid.contains(&normalized) {
                    endpoints.push(tcp_endpoint(&parsed));
                    valid.push(normalized);
                }
            }
            None => warn(rt, &format!("Invalid connector keys: '{k}'. Skipping...")),
        }
    }
    if valid.is_empty() {
        return Ok(SuccessTuple::fail("No valid connector keys were provided."));
    }
    let retry_wait = super::int(args, "min_seconds").unwrap_or(3).max(0) as u64;
    let max_retries = 50usize;
    let mut connected: Vec<Option<bool>> = vec![None; valid.len()];
    let mut attempts = 0usize;
    while attempts < max_retries && connected.iter().any(Option::is_none) {
        let mut quit = false;
        for (i, keys) in valid.iter().enumerate() {
            if connected[i].is_some() {
                continue;
            }
            // A refused connection fails at once, as the Python drivers do (a connection pool
            // would keep retrying it until its own acquire timeout).
            let refused = endpoints[i]
                .as_ref()
                .and_then(|(host, port)| tcp_refused(host, *port));
            // Building the connector may itself connect (SQL pools, Valkey): a failure there is
            // a failed attempt, like `_connect` raising in v4.
            let result = match refused {
                Some(message) => Err(meerschaum_connector::ConnectorError::Backend(message)),
                None => match sess.instance(keys, rt.config()) {
                    Ok(instance) => sess.block_on(instance.conn().test_connection()),
                    Err(e) => Err(meerschaum_connector::ConnectorError::Backend(e.message)),
                },
            };
            match result {
                Ok(true) => {
                    info(rt, &format!("Connected to '{keys}'."));
                    connected[i] = Some(true);
                }
                other => {
                    if let Err(e) = other {
                        rt.print(&format!("{e}\n"));
                    }
                    warn(
                        rt,
                        &format!(
                            "Connection to '{keys}' failed.\n    Press [Enter] to retry or wait {retry_wait} seconds."
                        ),
                    );
                    info(
                        rt,
                        &format!(
                            "Failed connection attempt ({} / {max_retries})",
                            attempts + 1
                        ),
                    );
                    if retry_wait > 0 {
                        let text = rt
                            .host
                            .read_line_timeout(std::time::Duration::from_secs(retry_wait));
                        if text.is_some_and(|t| {
                            matches!(t.as_str(), "q" | "quit" | "pass" | "exit" | "stop")
                        }) {
                            connected[i] = Some(false);
                            quit = true;
                        }
                    }
                }
            }
        }
        attempts += 1;
        if quit {
            break;
        }
    }
    let successes: Vec<String> = valid
        .iter()
        .zip(&connected)
        .filter(|(_, c)| **c == Some(true))
        .map(|(keys, _)| keys.clone())
        .collect();
    let fails: Vec<String> = valid
        .iter()
        .zip(&connected)
        .filter(|(_, c)| **c != Some(true))
        .map(|(keys, _)| keys.clone())
        .collect();
    let success = fails.is_empty();
    let mut msg = if success {
        format!(
            "Successfully started connector{} {}.",
            super::plural(successes.len()),
            super::items_str(&successes)
        )
    } else {
        format!("Failed to start {} connectors.", fails.len())
    };
    if !fails.is_empty() {
        msg.push_str(&format!(
            "\n    Failed to start connector{} {}.",
            super::plural(fails.len()),
            super::items_str(&fails)
        ));
    }
    Ok(SuccessTuple {
        success,
        message: msg,
    })
}

/// The TCP endpoint of a network database connector (`sql` and `valkey` with an explicit port).
fn tcp_endpoint(parsed: &Parsed) -> Option<(String, u16)> {
    if !matches!(parsed.typ.as_str(), "sql" | "valkey") {
        return None;
    }
    let (host, port) = if parsed.typ == "sql" {
        let spec = sql_spec(&parsed.label, &parsed.attrs).ok()?;
        (spec.host.clone()?, spec.port?)
    } else {
        (
            attr_str(&parsed.attrs, "host")?,
            attr_str(&parsed.attrs, "port")?.parse().ok()?,
        )
    };
    Some((host, port))
}

/// The driver-style message when nothing accepts connections at `host:port` (`None` when the
/// connection is accepted, or when it only times out: the connector's own timeouts decide).
fn tcp_refused(host: &str, port: u16) -> Option<String> {
    use std::net::ToSocketAddrs;
    let addrs: Vec<_> = (host, port).to_socket_addrs().ok()?.collect();
    let mut refused = false;
    for addr in addrs {
        match std::net::TcpStream::connect_timeout(&addr, std::time::Duration::from_secs(2)) {
            Ok(_) => return None,
            Err(e) if e.kind() == std::io::ErrorKind::ConnectionRefused => refused = true,
            Err(_) => return None,
        }
    }
    refused.then(|| {
        format!("connection to server at \"{host}\", port {port} failed: Connection refused")
    })
}

/// Whether `attrs` can build a `typ:label` connector (`get_connector(typ, label, **attrs)`).
pub fn validate_attributes(typ: &str, label: &str, attrs: &Map) -> Result<(), String> {
    match typ {
        "sql" => {
            if !attrs.contains_key("flavor") && !attrs.contains_key("uri") {
                return Err(format!(
                    "    Missing flavor. Provide flavor as a key for 'sql:{label}'."
                ));
            }
            sql_spec(label, attrs).map(|_| ())
        }
        "api" | "valkey" => {
            if attr_str(attrs, "host").is_none() && attr_str(attrs, "uri").is_none() {
                return Err(format!("Missing 'host' for connector '{typ}:{label}'."));
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
