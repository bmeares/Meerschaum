//! Python-only actions and the shared plugin manifest.
use std::sync::Arc;

use meerschaum_core::SuccessTuple;
use meerschaum_worker::manifest::{ArgumentManifest, Manifest, PluginManifest};
use meerschaum_worker::protocol::Msg;
use meerschaum_worker::worker::CallOptions;
use rmpv::Value as Wire;

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::native::{self, session};
use crate::value::ArgsMap;

/// Fingerprinted plugin metadata for this session, from memory: the plugin trees are validated
/// once per prompt or command (see `Session::manifest`), not once per call.
pub fn manifest(rt: &mut Runtime<'_>) -> Result<Arc<Manifest>, ActionError> {
    let session = session(rt);
    session.manifest(rt.config())
}

/// Validate the plugin trees now (after `install`/`uninstall`/`setup`/`upgrade plugins`).
pub fn refresh_manifest(rt: &mut Runtime<'_>) -> Result<Arc<Manifest>, ActionError> {
    let session = session(rt);
    session.refresh_manifest(rt.config())
}

/// The plugin action `words` name, resolved the way v4's `get_action(action)` does (the
/// action list is the positional words before the first flag):
///
/// - one word: the action with exactly that name, underscores included (`foo_bar`);
/// - several words: they are joined with `_` and every action whose name contains `_` and is
///   a prefix of the joined string is a candidate (the prefix need not end at a word); the
///   candidate leaving the fewest characters unmatched wins, before any subaction;
/// - otherwise the action named by the first word, whose subaction the next word may name.
///
/// `names` are the candidate action names. Core actions have no `_` in their names, so they
/// never compete in the second rule. The worker repeats the resolution with v4's own code;
/// this only decides whether a plugin owns the words.
pub fn resolve_action<'a>(
    words: &[String],
    names: impl IntoIterator<Item = &'a str>,
) -> Option<&'a str> {
    if words.iter().all(String::is_empty) {
        return None;
    }
    let names: Vec<&str> = names.into_iter().collect();
    if words.len() == 1 {
        return names.iter().copied().find(|name| *name == words[0]);
    }
    let joined = words.join("_");
    // PARITY: v4 sorts `(len(leftovers), function)` tuples, which raises `TypeError` on a
    // length tie; the first minimal candidate is used here.
    let mut best: Option<(usize, &str)> = None;
    for name in names.iter().copied().filter(|name| name.contains('_')) {
        if joined.starts_with(name) {
            let leftovers = joined.replace(name, "").chars().count();
            if best.is_none_or(|(len, _)| leftovers < len) {
                best = Some((leftovers, name));
            }
        }
    }
    if let Some((_, name)) = best {
        return Some(name);
    }
    names.iter().copied().find(|name| *name == words[0])
}

/// Resolve plugin actions as v4's `get_action` does ([`resolve_action`]).
pub fn action_name(words: &[String], manifest: &Manifest) -> Option<String> {
    resolve_action(
        words,
        manifest
            .plugins
            .values()
            .flat_map(|p| &p.actions)
            .map(|a| a.name.as_str()),
    )
    .map(str::to_owned)
}

/// Plugin sysargs are parsed by the worker so `add_plugin_argument` retains Python semantics.
pub fn plugin_action(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let sysargs = native::words(args, "sysargs");
    let kwargs = Wire::Map(
        args.iter()
            .map(|(key, value)| (Wire::from(key.as_str()), argument_value(value)))
            .collect(),
    );
    let message = if sysargs.is_empty() {
        Msg::new().set("kwargs", kwargs)
    } else {
        Msg::new()
            .set(
                "sysargs",
                Wire::Array(sysargs.into_iter().map(Wire::from).collect()),
            )
            .set("patch", kwargs)
    };
    let handler = native::sync::callbacks(rt, args)?;
    call("host_action", message, Some(handler), rt)
}

fn argument_value(value: &crate::value::PyValue) -> Wire {
    use crate::value::PyValue as P;
    use meerschaum_worker::protocol::ext;
    match value {
        P::DateTime(dt) => Wire::Ext(ext::DATETIME, dt.to_rfc3339().into_bytes()),
        P::TimeDelta(delta) => Wire::Ext(
            ext::TIMEDELTA,
            (delta.total_microseconds() as f64 / 1_000_000.0)
                .to_string()
                .into_bytes(),
        ),
        P::List(values) | P::Tuple(values) | P::Set(values) => {
            Wire::Array(values.iter().map(argument_value).collect())
        }
        P::Dict(entries) => Wire::Map(
            entries
                .iter()
                .map(|(k, v)| (argument_value(k), argument_value(v)))
                .collect(),
        ),
        P::Bytes(bytes) => Wire::Binary(bytes.clone()),
        _ => meerschaum_worker::protocol::core_to_wire(&native::py_to_value(value)),
    }
}

fn call(
    op: &str,
    mut message: Msg,
    handler: Option<Arc<dyn meerschaum_worker::callback::CallbackHandler>>,
    rt: &mut Runtime<'_>,
) -> ActionResult {
    let session = session(rt);
    let pool = session.pool(rt.config());
    message.insert("stream_output", true);
    let (sender, mut output) = tokio::sync::mpsc::channel(64);
    let reply = session
        .block_on(async {
            let call = pool.call(
                op,
                message,
                Vec::new(),
                CallOptions {
                    chunks: Some(sender),
                    ..Default::default()
                },
                handler,
            );
            tokio::pin!(call);
            let result = loop {
                tokio::select! {
                    result = &mut call => break result,
                    Some(chunk) = output.recv() => print_chunk(rt, chunk),
                }
            };
            while let Ok(chunk) = output.try_recv() {
                print_chunk(rt, chunk);
            }
            result
        })
        .map_err(|e| ActionError::new("WorkerError", e.to_string()))?;
    let result = match reply.value {
        Wire::Map(fields) => Msg(fields),
        _ => return Err(ActionError::new("WorkerError", "Invalid action reply")),
    };
    rt.print(result.str("stdout").unwrap_or_default());
    rt.eprint(result.str("stderr").unwrap_or_default());
    let tuple = result
        .get("result")
        .and_then(Wire::as_array)
        .ok_or_else(|| ActionError::new("WorkerError", "Invalid action result"))?;
    Ok(SuccessTuple {
        success: tuple.first().and_then(Wire::as_bool).unwrap_or(false),
        message: tuple
            .get(1)
            .and_then(Wire::as_str)
            .unwrap_or_default()
            .to_owned(),
    })
}

fn print_chunk(rt: &mut Runtime<'_>, chunk: meerschaum_worker::FetchChunk) {
    if let meerschaum_worker::FetchChunk::Text(text) = chunk
        && let Ok(value) = serde_json::from_str::<serde_json::Value>(&text)
    {
        let text = value["text"].as_str().unwrap_or_default();
        if value["stream"] == "stderr" {
            rt.eprint(text);
        } else {
            rt.print(text);
        }
    }
}

/// Append plugin help only when plugins contributed actions or flags.
pub fn print_help(rt: &mut Runtime<'_>) {
    let Ok(manifest) = manifest(rt) else {
        return;
    };
    let mut text = String::new();
    for plugin in manifest.plugins.values() {
        for action in &plugin.actions {
            text.push_str(&format!(
                "\n{}\n{}\n",
                action.name.replace('_', " "),
                action.doc.as_deref().unwrap_or_default()
            ));
        }
        for arg in &plugin.arguments {
            text.push_str(&format!(
                "  {}  {}\n",
                arg.option_strings.join(", "),
                arg.help.as_deref().unwrap_or_default()
            ));
        }
    }
    rt.print(&text);
}

/// Invoke plugin completers on the process's existing worker pool.
pub struct Completer {
    session: native::Session,
    pool: meerschaum_worker::WorkerPool,
    manifest: Arc<Manifest>,
}

impl Completer {
    /// A completer sharing the CLI's runtime and workers.
    pub fn new(
        session: native::Session,
        pool: meerschaum_worker::WorkerPool,
        manifest: Arc<Manifest>,
    ) -> Self {
        Self {
            session,
            pool,
            manifest,
        }
    }
}

impl crate::shell::completer::PluginCompleter for Completer {
    fn complete(&self, main: &str, _action: &[String], line: &str) -> Option<Vec<String>> {
        if !self
            .manifest
            .plugins
            .values()
            .flat_map(|p| &p.actions)
            .any(|a| a.name == main && a.has_completer)
        {
            return None;
        }
        let reply = self
            .session
            .block_on(self.pool.call(
                "complete",
                Msg::new().set("action", main).set("line", line),
                Vec::new(),
                CallOptions {
                    timeout: Some(std::time::Duration::from_secs(5)),
                    ..Default::default()
                },
                None,
            ))
            .ok()?;
        let Wire::Map(fields) = reply.value else {
            return None;
        };
        let message = Msg(fields);
        Some(
            message
                .get("result")?
                .as_array()?
                .iter()
                .filter_map(|v| v.as_str().map(str::to_owned))
                .collect(),
        )
    }
}

/// Populate plugin flags for native actions using the plugin's own argparse declarations.
pub fn arguments(
    args: &mut ArgsMap,
    manifest: &Manifest,
    rt: &mut Runtime<'_>,
) -> Result<(), ActionError> {
    let session = session(rt);
    // Native plugins declare their flags in the manifest; the Python worker never sees them.
    let natives = session.native_plugins(rt.config())?;
    let native_plugins: Vec<&PluginManifest> = manifest
        .plugins
        .values()
        .filter(|p| natives.contains(&p.name))
        .collect();
    if !native_plugins.is_empty() {
        let sysargs = native::words(args, "sysargs");
        crate::native_plugins::arguments(args, &native_plugins, &sysargs)
            .map_err(|e| ActionError::new("InvalidArgumentException", e))?;
    }
    let python_arguments: Vec<&ArgumentManifest> = manifest
        .plugins
        .values()
        .filter(|p| !natives.contains(&p.name))
        .flat_map(|p| &p.arguments)
        .filter(|a| a.dest.is_some())
        .collect();
    if python_arguments.is_empty() {
        return Ok(());
    }
    let sysargs = native::words(args, "sysargs");
    if let Some(defaults) = unused_defaults(&python_arguments, &sysargs) {
        // PARITY: `parse_arguments` fills every declared argument with its `argparse` default;
        // none was given, so the defaults the manifest records are the worker's answer and
        // no interpreter (and no plugin import) is needed.
        for (dest, value) in defaults {
            args.insert(dest, value);
        }
        return Ok(());
    }
    let destinations: Vec<_> = python_arguments
        .iter()
        .filter_map(|a| a.dest.as_ref())
        .collect();
    let pool = session.pool(rt.config());
    let reply = session
        .block_on(
            pool.call(
                "parse_plugin_args",
                Msg::new()
                    .set(
                        "sysargs",
                        Wire::Array(sysargs.into_iter().map(Wire::from).collect()),
                    )
                    .set(
                        "destinations",
                        Wire::Array(
                            destinations
                                .into_iter()
                                .map(|s| Wire::from(s.as_str()))
                                .collect(),
                        ),
                    ),
                Vec::new(),
                CallOptions::default(),
                None,
            ),
        )
        .map_err(|e| ActionError::new("InvalidArgumentException", e.to_string()))?;
    if let Wire::Map(fields) = reply.value {
        for (key, value) in fields {
            if let Some(key) = key.as_str() {
                args.insert(key.to_owned(), wire_value(&value));
            }
        }
    }
    Ok(())
}

/// The `argparse` defaults of `arguments` when `sysargs` cannot mention any of them, else `None`
/// (the worker parses). Conservative: a word that equals, abbreviates, extends (`--flag=v`,
/// `-fv`) or is prefixed by any option string counts as a mention, and defaults `argparse`
/// transforms (strings run through `type`, `SUPPRESS`) are left to the worker.
fn unused_defaults(
    arguments: &[&ArgumentManifest],
    sysargs: &[String],
) -> Option<Vec<(String, crate::value::PyValue)>> {
    let mut defaults = Vec::new();
    for argument in arguments {
        // PARITY: `parse_arguments` pops every value that `is False` and every `None`, so an
        // argument that was not given is absent from the keyword arguments (a plugin sees
        // `kwargs.get('flag')` as `None`, not `False`), and the string `'None'` becomes `None`
        // (kept, `none_args_keep`).
        let default = &argument.default;
        let converted = default.as_str().is_some_and(|text| {
            text == "==SUPPRESS==" || argument.type_name.as_deref().is_some_and(|t| t != "str")
        });
        if converted {
            return None;
        }
        let mentioned = sysargs
            .iter()
            .filter(|word| word.starts_with('-'))
            .any(|word| {
                let head = word.split('=').next().unwrap_or(word);
                argument
                    .option_strings
                    .iter()
                    .any(|option| word.starts_with(option.as_str()) || option.starts_with(head))
            });
        if mentioned {
            return None;
        }
        let dest = argument.dest.clone()?;
        if let Some(value) =
            crate::native_plugins::v4_argument_value(crate::native_plugins::json_to_py(default))
        {
            defaults.push((dest, value));
        }
    }
    Some(defaults)
}

fn wire_value(value: &Wire) -> crate::value::PyValue {
    use crate::value::PyValue as P;
    match value {
        Wire::Nil => P::None,
        Wire::Boolean(b) => P::Bool(*b),
        Wire::Integer(i) => P::Int(
            i.as_i64()
                .map(i128::from)
                .unwrap_or_else(|| i.as_u64().unwrap_or_default() as i128),
        ),
        Wire::F32(f) => P::Float(*f as f64),
        Wire::F64(f) => P::Float(*f),
        Wire::String(s) => P::Str(s.as_str().unwrap_or_default().to_owned()),
        Wire::Array(a) => P::List(a.iter().map(wire_value).collect()),
        Wire::Map(m) => P::Dict(
            m.iter()
                .map(|(k, v)| (wire_value(k), wire_value(v)))
                .collect(),
        ),
        Wire::Binary(b) => P::Bytes(b.clone()),
        _ => P::Str(value.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::PyValue;

    fn argument(option: &str, action: &str, default: serde_json::Value) -> ArgumentManifest {
        ArgumentManifest {
            option_strings: vec![option.into()],
            dest: Some(option.trim_start_matches('-').replace('-', "_")),
            action: action.into(),
            default,
            ..Default::default()
        }
    }

    fn words(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| w.to_string()).collect()
    }

    #[test]
    fn defaults_are_used_when_no_flag_is_mentioned() {
        let level = argument("--zz-level", "_StoreAction", serde_json::json!(3));
        let loud = argument("--zz-loud", "_StoreTrueAction", serde_json::json!(false));
        let none = argument("--zz-name", "_StoreAction", serde_json::Value::Null);
        let arguments = [&level, &loud, &none];
        let defaults = unused_defaults(&arguments, &words(&["show", "pipes", "-c", "sql:main"]))
            .expect("fast path");
        // v4 pops `False` and `None`: a flag that was not given is absent.
        assert_eq!(defaults, vec![("zz_level".to_string(), PyValue::Int(3))]);
    }

    #[test]
    fn defaults_follow_v4s_parse_arguments() {
        let on = argument("--zz-on", "_StoreTrueAction", serde_json::json!(true));
        let string_none = argument("--zz-none", "_StoreAction", serde_json::json!("None"));
        let empty = argument("--zz-empty", "_StoreAction", serde_json::json!(""));
        let zero = argument("--zz-zero", "_StoreAction", serde_json::json!(0));
        let list = argument("--zz-list", "_StoreAction", serde_json::json!([]));
        let off = argument("--zz-off", "_StoreFalseAction", serde_json::json!(false));
        let arguments = [&on, &string_none, &empty, &zero, &list, &off];
        let defaults = unused_defaults(&arguments, &words(&["show"])).expect("fast path");
        assert_eq!(
            defaults,
            vec![
                ("zz_on".to_string(), PyValue::Bool(true)),
                ("zz_none".to_string(), PyValue::None),
                ("zz_empty".to_string(), PyValue::Str(String::new())),
                ("zz_zero".to_string(), PyValue::Int(0)),
                ("zz_list".to_string(), PyValue::List(vec![])),
            ]
        );
    }

    fn names<'a>(list: &[&'a str]) -> Vec<&'a str> {
        list.to_vec()
    }

    #[test]
    fn actions_resolve_like_v4s_get_action() {
        let all = names(&["w17bx_hello", "w17bx", "show_w17b", "plain"]);
        let resolve = |input: &[&str]| resolve_action(&words(input), all.iter().copied());
        // One word: the exact name, underscores included.
        assert_eq!(resolve(&["w17bx_hello"]), Some("w17bx_hello"));
        assert_eq!(resolve(&["w17bx"]), Some("w17bx"));
        assert_eq!(resolve(&["w17bx_hellofoo"]), None);
        assert_eq!(resolve(&["hello"]), None);
        // Several words: joined, prefix match, before any subaction.
        assert_eq!(resolve(&["w17bx", "hello"]), Some("w17bx_hello"));
        assert_eq!(resolve(&["w17bx_hello", "extra"]), Some("w17bx_hello"));
        assert_eq!(resolve(&["w17bx", "hellofoo"]), Some("w17bx_hello"));
        assert_eq!(resolve(&["w17bx_hello_more", "x"]), Some("w17bx_hello"));
        assert_eq!(resolve(&["show", "w17b"]), Some("show_w17b"));
        // The first word names an action: it owns the rest (subactions, arguments).
        assert_eq!(resolve(&["w17bx", "foo"]), Some("w17bx"));
        assert_eq!(resolve(&["plain", "anything", "else"]), Some("plain"));
        // Nobody claims it: core actions and shell commands.
        assert_eq!(resolve(&["show", "pipes"]), None);
        assert_eq!(resolve(&["ls", "-l"]), None);
        assert_eq!(resolve(&[]), None);
        assert_eq!(resolve(&[""]), None);
        // The candidate leaving the fewest characters unmatched wins.
        let nested = names(&["a_b", "a_b_c"]);
        assert_eq!(
            resolve_action(&words(&["a", "b", "c"]), nested.iter().copied()),
            Some("a_b_c")
        );
        assert_eq!(
            resolve_action(&words(&["a_b", "d"]), nested.iter().copied()),
            Some("a_b")
        );
    }

    #[test]
    fn any_possible_mention_goes_to_the_worker() {
        let level = argument("--zz-level", "_StoreAction", serde_json::json!(3));
        let arguments = [&level];
        for sysargs in [
            &["--zz-level", "5"][..],
            &["--zz-level=5"],
            &["--zz-lev", "5"],
            &["--zz"],
            &["--zz-level-extra"],
        ] {
            assert!(
                unused_defaults(&arguments, &words(sysargs)).is_none(),
                "{sysargs:?}"
            );
        }
        let short = argument("-z", "_StoreAction", serde_json::Value::Null);
        assert!(unused_defaults(&[&short], &words(&["-z5"])).is_none());
        assert!(unused_defaults(&[&short], &words(&["-c", "x"])).is_some());
    }

    #[test]
    fn defaults_argparse_transforms_go_to_the_worker() {
        let mut typed = argument("--zz-n", "_StoreAction", serde_json::json!("5"));
        typed.type_name = Some("int".into());
        assert!(unused_defaults(&[&typed], &words(&["show"])).is_none());
        let suppressed = argument("--zz-s", "_StoreAction", serde_json::json!("==SUPPRESS=="));
        assert!(unused_defaults(&[&suppressed], &words(&["show"])).is_none());
    }
}
