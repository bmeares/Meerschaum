//! `login` (`meerschaum/actions/login.py`): log into API connectors, prompting for credentials
//! and optionally saving them to the connector's configuration.

use meerschaum_connector_api::APIConnector;
use meerschaum_core::SuccessTuple;
use meerschaum_core::value::{Map, Value};

use crate::builtin::ActionResult;
use crate::dispatch::Runtime;
use crate::format::TupleArgs;
use crate::value::ArgsMap;

use super::ask::{Ask, ask, get_password, is_interrupt};
use super::session::{connector_attributes, split_keys};
use super::{flag, print_tuple, session, warn, words, yes_no};

/// `login [api:label ...]`.
pub fn login(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let (yes, noask) = (flag(args, "yes"), flag(args, "noask"));
    let mut possible = words(args, "action");
    possible.extend(words(args, "connector_keys"));
    let mut keys: Vec<String> = Vec::new();
    for k in possible {
        if !k.starts_with("api:") {
            warn(
                rt,
                &format!("Connector '{k}' is not an API connector. Skipping..."),
            );
            continue;
        }
        keys.push(k);
    }
    let mut connectors: Vec<(String, String, Map)> = Vec::new();
    for k in &keys {
        let (typ, label) = split_keys(k);
        let attrs = connector_attributes(rt.config(), &typ, &label);
        let undefined = attrs.is_empty();
        // The connector's attributes include what a `uri` carries (`c.__dict__` in v4).
        let connector = match APIConnector::new(Some(&label), attrs) {
            Ok(connector) => connector,
            Err(e) => {
                // `get_connector` warns about the attributes and prints the error (without a
                // traceback) before `parse_instance_keys` raises. A connector defined nowhere
                // says where to define it (rc15).
                if undefined {
                    warn(
                        rt,
                        &super::connectors::undefined_connector_message(&typ, &label),
                    );
                } else if e.to_string().contains("Missing required attribute 'host'") {
                    warn(rt, &format!("Incorrect attributes for connector '{k}'."));
                    let line = rt.with_formatter(|f| {
                        let icon = f.formatting_str(&["errors", f.charset(), "icon"]);
                        let mut line = format!(" {icon} Missing 'host' for connector '{k}'.");
                        if f.settings.ansi {
                            let style = f.formatting_str(&["errors", "ansi", "rich", "style"]);
                            line = f.fill_ansi(&line, &style);
                        }
                        format!("\n{line}\n")
                    });
                    rt.eprint(&line);
                }
                warn(
                    rt,
                    &format!("Unable to build connector '{k}'. Is it registered?"),
                );
                continue;
            }
        };
        connectors.push((typ, label, connector.attributes().clone()));
    }
    let sess = session(rt);
    let mut successes = 0usize;
    for (typ, label, mut attrs) in connectors {
        let shown = format!("{typ}:{label}");
        loop {
            let mut login_change = false;
            if !attrs.contains_key("username") || !attrs.contains_key("password") {
                login_change = true;
                crate::shell::clear_screen(rt, flag(args, "debug"));
                let creds = ask(
                    rt,
                    &format!("Username for connector '{shown}':"),
                    &Ask::new(),
                )
                .and_then(|username| {
                    get_password(rt, Some(&username), None, false).map(|p| (username, p))
                });
                match creds {
                    Ok((username, password)) => {
                        attrs.insert("username".into(), Value::Str(username));
                        attrs.insert("password".into(), Value::Str(password));
                    }
                    Err(e) if is_interrupt(&e) => {
                        return Ok(SuccessTuple::fail("Cancelled login."));
                    }
                    Err(e) => return Err(e),
                }
            }
            let connector = match APIConnector::new(Some(&label), attrs.clone()) {
                Ok(c) => c,
                Err(e) => {
                    warn(
                        rt,
                        &format!(
                            "Failed to log in to connector '{shown}':\n\n{e}\n\nSkipping '{shown}'..."
                        ),
                    );
                    break;
                }
            };
            let login_tuple = sess.block_on(connector.login());
            if !login_tuple.success {
                if yes_no(
                    rt,
                    &format!(
                        "Would you like to try different login credentials for connector '{shown}'?"
                    ),
                    "y",
                    yes,
                    noask,
                )? {
                    attrs.shift_remove("username");
                    attrs.shift_remove("password");
                    continue;
                } else if login_change {
                    attrs.shift_remove("username");
                    attrs.shift_remove("password");
                }
            }
            print_tuple(rt, &login_tuple, TupleArgs::default());
            if login_tuple.success {
                successes += 1;
            }
            if login_tuple.success && login_change {
                if yes_no(
                    rt,
                    &format!("Would you like to save the new credentials for connector '{shown}'?"),
                    "y",
                    yes,
                    noask,
                )? {
                    save_credentials(rt, &typ, &label, &attrs);
                } else {
                    attrs.shift_remove("username");
                    attrs.shift_remove("password");
                }
            }
            break;
        }
    }
    Ok(SuccessTuple {
        success: successes > 0,
        message: format!(
            "Logged into {successes} connector{}.",
            super::plural(successes)
        ),
    })
}

/// `meerschaum_config['connectors'][type][label].update({username, password})` and
/// `write_config({'meerschaum': meerschaum_config})`.
fn save_credentials(rt: &mut Runtime<'_>, typ: &str, label: &str, attrs: &Map) {
    let mut section = rt
        .config()
        .get(&["meerschaum"])
        .map(|v| v.to_map())
        .unwrap_or_default();
    if let Some(Value::Map(connectors)) = section.get_mut("connectors")
        && let Some(Value::Map(by_label)) = connectors.get_mut(typ)
    {
        let entry = by_label
            .entry(label.to_string())
            .or_insert_with(|| Value::Map(Map::new()));
        if let Value::Map(entry) = entry {
            for key in ["username", "password"] {
                if let Some(v) = attrs.get(key) {
                    entry.insert(key.to_string(), v.clone());
                }
            }
        }
    }
    let mut config = Map::new();
    config.insert("meerschaum".into(), Value::Map(section));
    rt.config().write_config(Some(&config), None);
    super::connectors::flush_config_warnings(rt);
}
