//! `bootstrap connectors|pipes|jobs` (`meerschaum/actions/bootstrap.py`, `Pipe.bootstrap()`): the
//! interactive wizards. Every prompt goes through the CLI's own prompt helpers.

use meerschaum_core::value::{Map, Value, apply_patch_maps};
use meerschaum_core::{Pipe, SuccessTuple, static_config};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::rich::pretty::PyObj;
use crate::format::{OptionsArgs, TupleArgs};
use crate::value::{ArgsMap, PyValue};

use super::ask::{Ask, ask, choose, eof, interrupted, is_interrupt, is_secret_attribute};
use super::pipes::{Selection, explicit_pipes};
use super::{flag, info, items_str, pprint, print_tuple, session, string, warn, words, yes_no};

/// `config.get('formatting', 'emoji', name)`.
fn emoji(rt: &mut Runtime<'_>, name: &str) -> String {
    rt.config()
        .get(&["formatting", "emoji", name])
        .map(|v| v.py_str())
        .unwrap_or_default()
}

fn clear_screen_if_enabled(rt: &mut Runtime<'_>, debug: bool) {
    crate::shell::clear_screen(rt, debug);
}

/// `get_connector_labels()`: every configured connector's keys and the data plugins.
fn connector_labels(rt: &mut Runtime<'_>) -> Vec<String> {
    let connectors = rt
        .config()
        .get(&["meerschaum", "connectors"])
        .map(|v| v.to_map())
        .unwrap_or_default();
    let mut types: Vec<String> = connectors.keys().cloned().collect();
    types.push("plugin".into());
    let mut out: Vec<String> = Vec::new();
    for t in &types {
        if t == "plugin" {
            if let Ok(manifest) = crate::python::manifest(rt) {
                out.extend(
                    manifest
                        .plugins
                        .values()
                        .filter(|p| !p.data_functions.is_empty())
                        .map(|p| format!("plugin:{}", p.name)),
                );
            }
            continue;
        }
        if let Some(Value::Map(labels)) = connectors.get(t) {
            out.extend(
                labels
                    .keys()
                    .filter(|l| *l != "default")
                    .map(|l| format!("{t}:{l}")),
            );
        }
    }
    out.sort();
    out
}

/// The connector keys the `bootstrap pipes` dropdown suggests: those in the configuration,
/// `MRSM_<TYPE>_<LABEL>` environment connectors and `plugin:<name>` connectors.
fn connector_suggestions(rt: &mut Runtime<'_>) -> Vec<String> {
    let mut out = connector_labels(rt);
    let mut types: Vec<String> = vec!["sql".into(), "api".into(), "valkey".into()];
    if let Some(Value::Map(connectors)) = rt.config().get(&["meerschaum", "connectors"]) {
        types.extend(connectors.keys().cloned());
    }
    if let Ok(manifest) = crate::python::manifest(rt) {
        types.extend(manifest.connector_types.keys().cloned());
    }
    types.sort();
    types.dedup();
    out.extend(env_connector_keys(
        std::env::vars().map(|(name, _)| name),
        &types,
    ));
    out.sort();
    out.dedup();
    out
}

/// `type:label` of each `MRSM_<TYPE>_<LABEL>` variable whose type is one of `types`.
fn env_connector_keys(names: impl Iterator<Item = String>, types: &[String]) -> Vec<String> {
    let mut keys = Vec::new();
    for name in names {
        let Some(rest) = name.strip_prefix("MRSM_") else {
            continue;
        };
        for typ in types {
            let prefix = format!("{}_", typ.to_uppercase());
            if let Some(label) = rest.strip_prefix(&prefix)
                && !label.is_empty()
            {
                keys.push(format!("{typ}:{}", label.to_lowercase()));
            }
        }
    }
    keys
}

/// The pipes registered on the instance (no suggestions when it cannot be read).
fn registered_keys(
    rt: &mut Runtime<'_>,
    inst: &super::Instance,
) -> Vec<meerschaum_connector::PipeKeysEntry> {
    let sess = session(rt);
    match sess.block_on(
        inst.conn()
            .fetch_pipes_keys(&meerschaum_connector::PipesKeysQuery::default()),
    ) {
        Ok(meerschaum_connector::PipesKeys::List(entries)) => entries,
        Ok(meerschaum_connector::PipesKeys::Dict(entries)) => entries.into_values().collect(),
        Err(_) => Vec::new(),
    }
}

/// The metric keys on the instance, those of the chosen connector first.
fn metric_suggestions(
    known: &[meerschaum_connector::PipeKeysEntry],
    connector_keys: &str,
) -> Vec<String> {
    let mut ours: Vec<String> = known
        .iter()
        .filter(|e| e.connector_keys == connector_keys)
        .map(|e| e.metric_key.clone())
        .collect();
    ours.sort();
    let mut others: Vec<String> = known
        .iter()
        .filter(|e| e.connector_keys != connector_keys)
        .map(|e| e.metric_key.clone())
        .collect();
    others.sort();
    ours.extend(others);
    ours
}

/// The location keys on the instance: those of the connector and metric, then those of the
/// connector, then the rest.
fn location_suggestions(
    known: &[meerschaum_connector::PipeKeysEntry],
    connector_keys: &str,
    metric_key: &str,
) -> Vec<String> {
    let location = |e: &meerschaum_connector::PipeKeysEntry| {
        e.location_key
            .clone()
            .filter(|l| !l.is_empty() && l != "None")
    };
    let mut out: Vec<String> = Vec::new();
    for tier in 0..3 {
        let mut tier_keys: Vec<String> = known
            .iter()
            .filter(|e| match tier {
                0 => e.connector_keys == connector_keys && e.metric_key == metric_key,
                1 => e.connector_keys == connector_keys && e.metric_key != metric_key,
                _ => e.connector_keys != connector_keys,
            })
            .filter_map(location)
            .collect();
        tier_keys.sort();
        out.extend(tier_keys);
    }
    out
}

/// What the connector wizard produced.
pub enum Wizard {
    /// `return_keys=True`: the new connector's `(type, label)`.
    Keys(String, String),
    /// A final tuple (aborted, or success without `return_keys`).
    Done(SuccessTuple),
}

/// The attribute names the wizard asks about for a connector type (`CONNECTOR_ATTRIBUTES`, else
/// the class's `REQUIRED_ATTRIBUTES` / `OPTIONAL_ATTRIBUTES` / `DEFAULT_ATTRIBUTES`).
struct TypeAttributes {
    required: Vec<String>,
    optional: Vec<String>,
    default: Vec<(String, Value)>,
}

fn strings(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

fn type_attributes(rt: &mut Runtime<'_>, typ: &str) -> TypeAttributes {
    match typ {
        "api" => TypeAttributes {
            required: strings(&["host"]),
            optional: strings(&[
                "port",
                "username",
                "password",
                "client_id",
                "client_secret",
                "api_key",
            ]),
            default: vec![("protocol".into(), Value::Str("http".into()))],
        },
        "valkey" => TypeAttributes {
            required: strings(&["host"]),
            optional: strings(&["port", "username", "password", "db", "socket_timeout"]),
            default: vec![
                ("username".into(), Value::Str("default".into())),
                ("port".into(), Value::Int(6379)),
                ("db".into(), Value::Int(0)),
                ("socket_timeout".into(), Value::Int(300)),
            ],
        },
        _ => {
            let custom = crate::python::manifest(rt)
                .ok()
                .and_then(|m| m.connector_type(typ).cloned());
            match custom {
                Some(c) => TypeAttributes {
                    required: c.required_attributes,
                    optional: c.optional_attributes,
                    default: Vec::new(),
                },
                None => TypeAttributes {
                    required: Vec::new(),
                    optional: Vec::new(),
                    default: Vec::new(),
                },
            }
        }
    }
}

/// `is_int(val)` then `int(val)`.
fn int_or_str(val: String) -> Value {
    match val.trim().parse::<i64>() {
        Ok(i) => Value::Int(i),
        Err(_) => Value::Str(val),
    }
}

/// The connector types the wizard offers (`sorted(k for k in connectors if k != 'plugin')`).
fn connector_types(rt: &mut Runtime<'_>) -> Vec<String> {
    let mut types: Vec<String> = vec![
        "api".into(),
        "sql".into(),
        "systemd".into(),
        "valkey".into(),
    ];
    if let Ok(manifest) = crate::python::manifest(rt) {
        for t in manifest.connector_types.keys() {
            if t != "plugin" && !types.contains(t) {
                types.push(t.clone());
            }
        }
    }
    types.sort();
    types
}

/// `_bootstrap_connectors(action, ..., return_keys=False)`.
pub fn bootstrap_connectors_wizard(
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    return_keys: bool,
) -> Result<Wizard, ActionError> {
    let (yes, force, noask) = (flag(args, "yes"), flag(args, "force"), flag(args, "noask"));
    let debug = flag(args, "debug");
    let abort = Wizard::Done(SuccessTuple::fail("No connectors bootstrapped."));
    let types = connector_types(rt);
    let Some(Some(typ)) = interrupted(choose(
        rt,
        "Please choose a connector type.\n    See https://meerschaum.io/reference/connectors for documentation on connectors.\n",
        &types,
        Some("sql"),
        true,
    ))?
    else {
        return Ok(abort);
    };
    clear_screen_if_enabled(rt, debug);
    let existing: Map = rt
        .config()
        .get(&["meerschaum", "connectors", &typ])
        .map(|v| v.to_map())
        .unwrap_or_default();
    let label = loop {
        let Some(label) = interrupted(ask(
            rt,
            &format!("New label for '{typ}' connector:"),
            &Ask::new(),
        ))?
        else {
            return Ok(abort);
        };
        if existing.contains_key(&label) {
            warn(rt, &format!("Connector '{typ}:{label}' already exists."));
            let overwrite = yes_no(
                rt,
                &format!("Do you want to overwrite connector '{typ}:{label}'?"),
                "n",
                yes,
                noask,
            )?;
            if !overwrite && !force {
                return Ok(Wizard::Done(SuccessTuple::fail(
                    "No changes made to connector configuration.",
                )));
            }
            break label;
        } else if label.is_empty() {
            warn(rt, "Please enter a label.");
        } else {
            break label;
        }
    };

    let mut new_attributes = Map::new();
    let (required, optional, defaults): (Vec<String>, Vec<String>, Vec<(String, Value)>) =
        if typ == "sql" {
            let flavors: Vec<String> = {
                let mut f: Vec<String> = static_config()
                    .sql
                    .create_engine_flavors
                    .keys()
                    .cloned()
                    .collect();
                f.sort();
                f
            };
            let default_flavor = flavors
                .iter()
                .any(|f| f == "postgresql")
                .then_some("postgresql");
            let Some(Some(flavor)) = interrupted(choose(
                rt,
                &format!("Flavor for connector '{typ}:{label}':"),
                &flavors,
                default_flavor,
                true,
            ))?
            else {
                return Ok(abort);
            };
            new_attributes.insert("flavor".into(), Value::Str(flavor.clone()));
            let spec = &static_config().sql.create_engine_flavors[&flavor];
            let mut required: Vec<String> = match &spec.requirements {
                Value::List(items) => items.iter().map(Value::py_str).collect(),
                _ => Vec::new(),
            };
            required.sort();
            let defaults: Vec<(String, Value)> = spec
                .defaults
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            (required, Vec::new(), defaults)
        } else {
            let attrs = type_attributes(rt, &typ);
            let mut required = attrs.required;
            let mut optional = attrs.optional;
            required.sort();
            optional.sort();
            (required, optional, attrs.default)
        };
    info(
        rt,
        &format!(
            "Please answer the following questions to configure the new connector '{typ}:{label}'.\nPress [Ctrl + C] to skip."
        ),
    );
    for r in &required {
        let default = defaults
            .iter()
            .find(|(k, _)| k == r)
            .map(|(_, v)| v.py_str());
        let mut options = Ask::new();
        if let Some(d) = default {
            options = options.default(d);
        }
        if is_secret_attribute(r) {
            options = options.password();
        }
        let val = match ask(rt, &format!("Value for {r}:"), &options) {
            Ok(v) => v,
            Err(e) if is_interrupt(&e) => continue,
            Err(e) => return Err(e),
        };
        new_attributes.insert(r.clone(), int_or_str(val));
    }
    for o in &optional {
        let mut options = Ask::new();
        if is_secret_attribute(o) {
            options = options.password();
        }
        let val = match ask(
            rt,
            &format!("Value for {o} (optional; empty to omit):"),
            &options,
        ) {
            Ok(v) => v,
            Err(e) if is_interrupt(&e) => continue,
            Err(e) => return Err(e),
        };
        if !val.is_empty() {
            new_attributes.insert(o.clone(), Value::Str(val));
        }
    }
    for (k, v) in &defaults {
        if new_attributes.contains_key(k) {
            continue;
        }
        let mut options = Ask::new().default(v.py_str());
        if is_secret_attribute(k) {
            options = options.password();
        }
        let val = match ask(rt, &format!("Value for {k}:"), &options) {
            Ok(v) => v,
            Err(e) if is_interrupt(&e) => continue,
            Err(e) => return Err(e),
        };
        new_attributes.insert(k.clone(), int_or_str(val));
    }
    clear_screen_if_enabled(rt, debug);
    if let Err(message) = super::connectors::validate_attributes(&typ, &label, &new_attributes) {
        return Ok(Wizard::Done(SuccessTuple::fail(format!(
            "Failed to bootstrap connector '{typ}:{label}' with exception:\n{message}"
        ))));
    }
    // The secrets were typed masked: they are not printed back (v4 printed them).
    let shown: Map = new_attributes
        .iter()
        .map(|(k, v)| {
            if is_secret_attribute(k) && !matches!(v, Value::Null) {
                (k.clone(), Value::Str("********".into()))
            } else {
                (k.clone(), v.clone())
            }
        })
        .collect();
    pprint(rt, &PyObj::from_value(&Value::Map(shown)), false);
    let ok = if yes {
        true
    } else {
        yes_no(
            rt,
            &format!("Are you ok with these new attributes for connector '{typ}:{label}'?"),
            "y",
            yes,
            noask,
        )?
    };
    if !ok {
        return Ok(Wizard::Done(SuccessTuple::fail(
            "No changes made to connectors configuration.",
        )));
    }
    let mut section = rt
        .config()
        .get(&["meerschaum"])
        .map(|v| v.to_map())
        .unwrap_or_default();
    let connectors = section
        .entry("connectors".to_string())
        .or_insert_with(|| Value::Map(Map::new()));
    if let Value::Map(connectors) = connectors {
        let by_type = connectors
            .entry(typ.clone())
            .or_insert_with(|| Value::Map(Map::new()));
        if let Value::Map(by_type) = by_type {
            by_type.insert(label.clone(), Value::Map(new_attributes));
        }
    }
    let mut config = Map::new();
    config.insert("meerschaum".into(), Value::Map(section));
    rt.config().write_config(Some(&config), None);
    super::connectors::flush_config_warnings(rt);
    if return_keys {
        return Ok(Wizard::Keys(typ, label));
    }
    Ok(Wizard::Done(SuccessTuple::ok("Success")))
}

/// `bootstrap connectors`.
pub fn bootstrap_connectors(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    match bootstrap_connectors_wizard(args, rt, false)? {
        Wizard::Done(tup) => Ok(tup),
        Wizard::Keys(..) => Ok(SuccessTuple::ok("Success")),
    }
}

/// `repr(list)` of strings.
fn list_repr(items: &[String]) -> String {
    PyObj::List(items.iter().map(|s| PyObj::Str(s.clone())).collect()).repr()
}

/// `bootstrap pipes`.
pub fn bootstrap_pipes(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let (yes, force, noask) = (flag(args, "yes"), flag(args, "force"), flag(args, "noask"));
    let debug = flag(args, "debug");
    let inst = super::get_instance(args, rt)?;
    let mut connector_keys = words(args, "connector_keys");
    let mut metric_keys = words(args, "metric_keys");
    let mut location_keys: Vec<Option<String>> = Selection::from_args(args).location_keys;
    let abort = SuccessTuple::fail("No pipes were bootstrapped.");

    if !connector_keys.is_empty() && !metric_keys.is_empty() {
        let locations: Vec<String> = location_keys
            .iter()
            .map(|l| l.clone().unwrap_or_else(|| "None".into()))
            .collect();
        let locations_repr = PyObj::List(
            location_keys
                .iter()
                .map(|l| match l {
                    Some(s) => PyObj::Str(s.clone()),
                    None => PyObj::None,
                })
                .collect(),
        )
        .repr();
        let _ = locations;
        info(
            rt,
            &format!(
                "You've provided the following keys:\n  - Connector Keys: {}\n  - Metric Keys: {}\n{}",
                list_repr(&connector_keys),
                list_repr(&metric_keys),
                if location_keys.is_empty() {
                    String::new()
                } else {
                    format!("  - Location Keys: {locations_repr}\n")
                }
            ),
        );
        if !force
            && !yes_no(
                rt,
                "Would you like to bootstrap pipes with these keys?\nExisting pipes will be deleted!",
                "n",
                yes,
                noask,
            )?
        {
            return Ok(abort);
        }
    } else {
        let new_label = "New";
        // The keys already registered on the instance (for the metric and location dropdowns).
        let known = registered_keys(rt, &inst);
        info(
            rt,
            "To create a pipe without explicitly using a connector, use the `register pipes` command.\n",
        );
        let mut choices = connector_suggestions(rt);
        choices.push(new_label.to_string());
        let connector_emoji = emoji(rt, "connector");
        let question = format!(
            "Where are the data coming from?\n\n    Please type the keys of a connector or enter '{new_label}'\n    to register a new connector.\n\n {connector_emoji} Connector:"
        );
        // The warning is shown on the metric question's screen: the screen is cleared between
        // questions, which would wipe it here.
        let mut unknown_connector: Option<String> = None;
        let mut ck = loop {
            let Some(typed) =
                interrupted(ask(rt, &question, &Ask::new().suggestions(choices.clone())))?
            else {
                return Ok(abort);
            };
            let typed = typed.trim().to_string();
            if typed.is_empty() {
                warn(rt, "Please pick a connector.");
                continue;
            }
            if typed != new_label && !choices.contains(&typed) {
                // Any keys are accepted: the connector may be defined where this host cannot see
                // it (a plugin installed later, a remote instance).
                unknown_connector = Some(format!(
                    "Connector '{typed}' is not configured on this host (it is not in the configuration, an `MRSM_<TYPE>_<LABEL>` environment variable or a plugin)."
                ));
            }
            break typed;
        };
        if ck == new_label {
            clear_screen_if_enabled(rt, debug);
            loop {
                match bootstrap_connectors_wizard(args, rt, true)? {
                    Wizard::Keys(t, l) => {
                        clear_screen_if_enabled(rt, debug);
                        ck = format!("{t}:{l}");
                        break;
                    }
                    Wizard::Done(tup) if !tup.success => return Ok(abort),
                    Wizard::Done(_) => {}
                }
                warn(
                    rt,
                    "Please register a new connector or press CTRL+C to cancel.",
                );
            }
        }
        connector_keys = vec![ck.clone()];

        let mk = loop {
            clear_screen_if_enabled(rt, debug);
            if let Some(message) = unknown_connector.take() {
                warn(rt, &message);
            }
            let metric_emoji = emoji(rt, "metric");
            let Some(mk) = interrupted(ask(
                rt,
                &format!(
                    "What kind of data is this?\n\n    The metric is the label for the contents of the pipe.\n    For example, 'weather' might be a metric for weather station data.\n\n {metric_emoji} Metric:"
                ),
                &Ask::new().suggestions(metric_suggestions(&known, &ck)),
            ))?
            else {
                return Ok(abort);
            };
            if !mk.is_empty() {
                break mk;
            }
            warn(rt, "Please enter a metric.");
        };
        metric_keys = vec![mk.clone()];

        clear_screen_if_enabled(rt, debug);
        let location_emoji = emoji(rt, "location");
        let Some(lk) = interrupted(ask(
            rt,
            &format!(
                "Where are the data located?\n\n    You can build pipes that share a connector and metric\n    but are in different locations.\n\n    For example, you could create Pipe('{ck}', '{mk}', 'home') and\n    Pipe('{ck}', '{mk}', 'work'), which would share a connector\n    and metric, but may come from different tables.\n\n    In most cases, you can omit the location.\n\n {location_emoji} Location (Empty to omit):"
            ),
            &Ask::new().suggestions(location_suggestions(&known, &ck, &mk)),
        ))?
        else {
            return Ok(abort);
        };
        location_keys = vec![(!lk.is_empty()).then_some(lk)];
    }

    clear_screen_if_enabled(rt, debug);
    let sel = Selection {
        connector_keys: connector_keys.clone(),
        metric_keys: metric_keys.clone(),
        location_keys: location_keys.clone(),
        tags: words(args, "tags"),
        params: super::params(args),
    };
    let selected = explicit_pipes(rt, &inst, &sel)?;
    let sess = session(rt);
    let mut pipes: Vec<Pipe> = Vec::new();
    for sp in selected {
        let exists = sess
            .block_on(inst.conn().get_pipe_id(&sp.pipe))
            .ok()
            .flatten()
            .is_some();
        if exists && !force {
            let q = format!(
                "{} already exists.\n\n    Delete {}?\n    Data will be lost!",
                sp.pipe, sp.pipe
            );
            if !yes_no(rt, &q, "n", false, false)? {
                info(rt, &format!("Skipping bootstrapping {}...", sp.pipe));
                continue;
            }
        }
        pipes.push(sp.pipe);
    }
    if pipes.is_empty() {
        return Ok(abort);
    }
    let (mut successes, mut failures) = (0usize, 0usize);
    for mut pipe in pipes.clone() {
        let tup = match bootstrap_pipe(rt, &inst, &mut pipe, args) {
            Ok(t) => t,
            Err(e) => SuccessTuple::fail(format!(
                "Failed to bootstrap {pipe} with exception:\n{}",
                e.message
            )),
        };
        if tup.success {
            successes += 1;
        } else {
            failures += 1;
        }
    }
    Ok(SuccessTuple {
        success: successes > 0,
        message: format!(
            "Finished bootstrapping {} pipe{}\n    ({successes} succeeded, {failures} failed).",
            pipes.len(),
            super::plural(pipes.len())
        ),
    })
}

/// `Pipe.bootstrap(interactive=True, ...)`.
fn bootstrap_pipe(
    rt: &mut Runtime<'_>,
    inst: &super::Instance,
    pipe: &mut Pipe,
    args: &ArgsMap,
) -> Result<SuccessTuple, ActionError> {
    let (yes, noask) = (flag(args, "yes"), flag(args, "noask"));
    let debug = flag(args, "debug");
    let sess = session(rt);
    let exists = sess
        .block_on(inst.conn().get_pipe_id(pipe))
        .ok()
        .flatten()
        .is_some();
    if exists {
        // `Pipe.delete(drop=True)`.
        let dropped = sess.block_on(inst.conn().drop_pipe(pipe));
        if !dropped.success {
            warn(rt, &format!("Failed to drop {pipe}:\n{}", dropped.message));
        }
        let deleted = sess.block_on(inst.conn().delete_pipe(pipe));
        if !deleted.success {
            return Ok(deleted);
        }
    }
    clear_screen_if_enabled(rt, debug);
    let parameters = ask_for_parameters(rt, inst, pipe, debug)?;
    pipe.set_parameters(parameters);
    pprint(
        rt,
        &PyObj::from_value(&Value::Map(pipe.raw_parameters().clone())),
        false,
    );
    if ask(
        rt,
        &format!("\n    Press [Enter] to register {pipe} with the above configuration:"),
        &Ask::new().no_icon(),
    )
    .is_err()
    {
        return Ok(SuccessTuple::fail(format!("Aborted bootstrapping {pipe}.")));
    }
    let registered = super::manage::register_pipe(rt, inst, pipe);
    if !registered.success {
        return Ok(registered);
    }
    clear_screen_if_enabled(rt, debug);
    let attempt = (|| -> Result<Option<SuccessTuple>, ActionError> {
        if yes_no(
            rt,
            &format!("Would you like to edit the definition for {pipe}?"),
            "n",
            yes,
            noask,
        )? {
            let mut edit_args = args.clone();
            edit_args.insert(
                "connector_keys".into(),
                PyValue::str_list(&[pipe.connector_keys().to_string()]),
            );
            edit_args.insert(
                "metric_keys".into(),
                PyValue::str_list(&[pipe.metric_key().to_string()]),
            );
            edit_args.insert(
                "location_keys".into(),
                PyValue::List(vec![match pipe.location_key() {
                    Some(l) => PyValue::Str(l.to_string()),
                    None => PyValue::None,
                }]),
            );
            edit_args.insert(
                "action".into(),
                PyValue::str_list(&["definition".to_string()]),
            );
            edit_args.insert("params".into(), PyValue::None);
            edit_args.insert("yes".into(), PyValue::Bool(true));
            let edited = super::definition::edit_pipes(&edit_args, rt, true)?;
            if !edited.success {
                return Ok(Some(edited));
            }
        }
        if yes_no(
            rt,
            &format!("Would you like to try syncing {pipe} now?"),
            "n",
            yes,
            noask,
        )? {
            let mut sysargs: Vec<String> = vec![
                "sync".into(),
                "pipes".into(),
                "-c".into(),
                pipe.connector_keys().to_string(),
                "-m".into(),
                pipe.metric_key().to_string(),
                "-i".into(),
                inst.keys(),
            ];
            if let Some(l) = pipe.location_key() {
                sysargs.extend(["-l".into(), l.to_string()]);
            }
            let dates = rt.dates.clone();
            let mut sync_args = crate::parse::parse_arguments(&sysargs, &dates)
                .map_err(|e| ActionError::new(&e.class, e.message))?;
            sync_args.insert("action".into(), PyValue::List(Vec::new()));
            if pipe.location_key().is_none() {
                sync_args.insert("location_keys".into(), PyValue::List(vec![PyValue::None]));
            }
            if debug {
                sync_args.insert("debug".into(), PyValue::Bool(true));
            }
            let synced = super::sync::sync_pipes(&sync_args, rt)?;
            if !synced.success {
                return Ok(Some(synced));
            }
        }
        Ok(None)
    })();
    match attempt {
        Ok(Some(tup)) => return Ok(tup),
        Ok(None) => {}
        Err(e) => {
            return Ok(SuccessTuple::fail(format!(
                "Failed to bootstrap {pipe}:\n{}",
                e.message
            )));
        }
    }
    print_tuple(
        rt,
        &SuccessTuple::ok(format!("Finished bootstrapping {pipe}!")),
        TupleArgs::default(),
    );
    info(
        rt,
        "You can edit this pipe later with `edit pipes` or set the definition with `edit pipes definition`.\n    To sync data into your pipe, run `sync pipes`.",
    );
    Ok(SuccessTuple::ok("Success"))
}

/// `_get_parameters(pipe)`.
fn ask_for_parameters(
    rt: &mut Runtime<'_>,
    inst: &super::Instance,
    pipe: &Pipe,
    debug: bool,
) -> Result<Map, ActionError> {
    let get = |rt: &mut Runtime<'_>, keys: &[&str]| rt.config().get(keys).unwrap_or(Value::Null);
    let source_type = pipe
        .connector_keys()
        .split(':')
        .next()
        .unwrap_or_default()
        .to_string();
    let mut parameters = Map::new();
    match source_type.as_str() {
        "sql" => {
            let mut fetch = Map::new();
            fetch.insert(
                "backtrack_minutes".into(),
                get(rt, &["pipes", "parameters", "fetch", "backtrack_minutes"]),
            );
            fetch.insert("definition".into(), Value::Null);
            parameters.insert("fetch".into(), Value::Map(fetch));
            let mut verify = Map::new();
            verify.insert(
                "chunk_minutes".into(),
                get(rt, &["pipes", "parameters", "verify", "chunk_minutes"]),
            );
            parameters.insert("verify".into(), Value::Map(verify));
        }
        "api" => {
            parameters.insert("fetch".into(), Value::Map(Map::new()));
        }
        "plugin" => {
            let sess = session(rt);
            if let Some(Ok(outcome)) =
                super::manage::ask_connector_register(&sess, rt.config(), pipe)
                && outcome.defined
            {
                if outcome.valid {
                    parameters = apply_patch_maps(&parameters, &outcome.parameters);
                } else {
                    warn(
                        rt,
                        &format!(
                            "Plugin '{}' did not return a dictionary of attributes.",
                            pipe.connector_keys().trim_start_matches("plugin:")
                        ),
                    );
                }
            }
        }
        _ => {}
    }
    let ask_for_cols = parameters
        .get("columns")
        .and_then(|c| c.get("datetime"))
        .is_none_or(Value::is_null);
    if ask_for_cols {
        parameters.insert(
            "columns".into(),
            Value::Map(ask_for_columns(rt, pipe, debug)?),
        );
    }
    clear_screen_if_enabled(rt, debug);
    info(
        rt,
        &format!(
            "You have the option to change the target table name for {pipe}.\n    To keep the default, press [Enter].\n"
        ),
    );
    let flavor = inst.sql().map(|s| s.flavor().to_string());
    let default_target = pipe.target(flavor.as_deref()).unwrap_or_default();
    let target = ask(
        rt,
        &format!("Target table name for {pipe}:"),
        &Ask::new().default(default_target),
    )?;
    parameters.insert("target".into(), Value::Str(target));
    clear_screen_if_enabled(rt, debug);
    info(
        rt,
        "In addition to the key flags (-c, -m, -l), you can select pipes with `--tags` (-t).\n    Separate tags with commas (e.g. tag1,tag2).\n",
    );
    let tags = ask(
        rt,
        &format!("Tags for {pipe} (empty to omit):"),
        &Ask::new(),
    )
    .map(|text| {
        text.split(',')
            .filter(|t| !t.is_empty())
            .map(|t| Value::Str(t.trim().to_string()))
            .collect::<Vec<_>>()
    })
    .unwrap_or_default();
    parameters.insert("tags".into(), Value::List(tags));
    Ok(parameters)
}

/// `to_snake_case(text)`.
fn to_snake_case(text: &str) -> String {
    let mut out = String::new();
    let mut prev_lower = false;
    for c in text.chars() {
        if c.is_alphanumeric() {
            if c.is_uppercase() && prev_lower {
                out.push('_');
            }
            out.extend(c.to_lowercase());
            prev_lower = c.is_lowercase() || c.is_ascii_digit();
        } else if !out.ends_with('_') && !out.is_empty() {
            out.push('_');
            prev_lower = false;
        }
    }
    out.trim_matches('_').to_string()
}

/// `_ask_for_columns(pipe)`.
fn ask_for_columns(rt: &mut Runtime<'_>, pipe: &Pipe, debug: bool) -> Result<Map, ActionError> {
    let mut cols = Map::new();
    info(rt, &format!("Please enter index columns for {pipe}:"));
    let datetime_name = ask(
        rt,
        "Datetime column (empty to omit):",
        &Ask::new().no_icon(),
    )
    .unwrap_or_default();
    if !datetime_name.is_empty() {
        cols.insert("datetime".into(), Value::Str(datetime_name));
    }
    let id_name = ask(rt, "ID column (empty to omit):", &Ask::new().no_icon()).unwrap_or_default();
    if !id_name.is_empty() {
        cols.insert("id".into(), Value::Str(id_name));
    }
    if yes_no(rt, "Add more columns?", "y", false, false)? {
        loop {
            clear_screen_if_enabled(rt, debug);
            info(rt, "Current index columns:");
            let text =
                rt.with_formatter(|f| super::rich_json::print_json(f, &Value::Map(cols.clone())));
            rt.print(&text);
            let col_name = ask(rt, "Enter index column (empty to stop):", &Ask::new())?;
            if col_name.is_empty() {
                break;
            }
            if cols.values().any(|v| v.py_str() == col_name) {
                continue;
            }
            let mut col_ix = to_snake_case(&col_name);
            if cols.contains_key(&col_ix) {
                col_ix.push('_');
            }
            cols.insert(col_ix, Value::Str(col_name));
        }
    }
    Ok(cols)
}

/// `bootstrap jobs [names ...]`.
pub fn bootstrap_jobs(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut action = words(args, "action");
    if action.is_empty() {
        action = vec![ask(
            rt,
            "What is the name of the job you'd like to create?",
            &Ask::new(),
        )?];
    }
    let exec_keys = string(args, "executor_keys");
    let ctx = super::jobs::jobs_context(rt);
    let debug = flag(args, "debug");
    for name in &action {
        clear_screen_if_enabled(rt, debug);
        let mut existing = meerschaum_jobs::Job::new(
            name,
            None,
            meerschaum_jobs::job::JobOptions {
                executor_keys: exec_keys.clone(),
                ..Default::default()
            },
            &ctx,
        )
        .map_err(|e| ActionError::new("ValueError", e.to_string()))?;
        if existing.exists() {
            let mut edit_args = args.clone();
            edit_args.insert(
                "action".into(),
                PyValue::str_list(std::slice::from_ref(name)),
            );
            let Some(result) = super::run("_edit_jobs", &edit_args, rt) else {
                return Ok(SuccessTuple::fail(
                    "Editing existing jobs is not available in this build.",
                ));
            };
            let edited = result?;
            if !edited.success {
                return Ok(edited);
            }
            continue;
        }
        info(
            rt,
            &format!(
                "Editing arguments for job '{name}'.\n    Press [Esc + Enter] to submit, [CTRL + C] to exit.\n\n    Tip: join multiple actions with `+`, add pipeline arguments with `:`.\n    https://meerschaum.io/reference/actions/#chaining-actions\n"
            ),
        );
        let text = match rt.host.read_multiline("") {
            Some(t) => t,
            None => return Err(eof()),
        };
        let words_in = crate::shlex::split(&text).map_err(|e| ActionError::new("ValueError", e))?;
        let (new_sysargs, pipeline_args) = crate::sysargs::split_pipeline_sysargs(&words_in);
        let chained = crate::sysargs::split_chained_sysargs(&new_sysargs)
            .map_err(|e| ActionError::new(&e.class, e.message))?;
        clear_screen_if_enabled(rt, debug);
        if chained.len() > 1 {
            let steps: Vec<String> = chained.iter().map(|s| crate::shlex::join(s)).collect();
            let opts = OptionsArgs {
                header: Some(format!("Steps in Job '{name}':")),
                number_options: true,
                nopretty: flag(args, "nopretty"),
                ..OptionsArgs::default()
            };
            let printed = rt.with_formatter(|f| f.print_options(&steps, &opts));
            rt.print(&printed);
        } else {
            let header = rt.with_formatter(|f| f.make_header(&format!("Action for Job '{name}':")));
            rt.print(&format!(
                "\n{header}\n{}\n",
                crate::shlex::join(&new_sysargs)
            ));
        }
        if !pipeline_args.is_empty() {
            let header = rt.with_formatter(|f| f.make_header("Pipeline Arguments:"));
            rt.print(&format!(
                "\n{header}\n{}\n\n",
                crate::shlex::join(&pipeline_args)
            ));
        }
        if !yes_no(
            rt,
            &format!(
                "Are you sure you want to create job '{name}' with the above arguments?\n    The job will be started if you continue."
            ),
            "n",
            flag(args, "yes"),
            flag(args, "noask"),
        )? {
            return Ok(SuccessTuple::ok("Nothing was changed."));
        }
        let mut job = meerschaum_jobs::Job::from_sysargs_str(
            name,
            &text,
            meerschaum_jobs::job::JobOptions {
                executor_keys: exec_keys.clone(),
                ..Default::default()
            },
            &ctx,
        )
        .map_err(|e| ActionError::new("ValueError", e.to_string()))?;
        for w in std::mem::take(&mut job.warnings) {
            warn(rt, &w);
        }
        let started = job.start();
        // What creating the job warned about (a systemd unit without a pinned interpreter).
        super::jobs::emit_job_warnings(rt, &mut job);
        if !started.success {
            return Ok(started);
        }
    }
    Ok(SuccessTuple::ok(format!(
        "Successfully bootstrapped job{} {}.",
        super::plural(action.len()),
        items_str(&action)
    )))
}
