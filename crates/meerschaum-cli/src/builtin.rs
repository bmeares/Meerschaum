//! The core actions that need no instance connector, ported from `meerschaum/actions/`:
//! `show version|config|arguments|environment|help|actions|plugins`, `edit config`, `reload`,
//! `compose explain|init|--dry up`, `python` and `sh` (the shell's `!` escape).
//!
//! Each action returns `Ok(SuccessTuple)` or an [`ActionError`] for what v4 raises; the
//! dispatcher turns an error into v4's `Failed to execute ...` tuple (`_do_action_wrapper`).

use std::path::PathBuf;

use meerschaum_core::config::GetOptions;
use meerschaum_core::pyfmt::{JsonStyle, json_dumps};
use meerschaum_core::value::{Map, Value};
use meerschaum_core::{SuccessTuple, static_config};

use crate::actions::CORE_ACTIONS;
use crate::dispatch::{CommandSpec, Runtime};
use crate::format::rich::pretty::PyObj;
use crate::format::{Formatter, OptionsArgs};
use crate::help;
use crate::value::{ArgsMap, PyValue};

/// An exception an action raised (`type(e).__name__`, `str(e)`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionError {
    /// The exception class name.
    pub class: String,
    /// `str(e)`.
    pub message: String,
}

impl ActionError {
    /// An exception.
    pub fn new(class: &str, message: impl Into<String>) -> ActionError {
        ActionError {
            class: class.into(),
            message: message.into(),
        }
    }
}

/// What an action returns.
pub type ActionResult = Result<SuccessTuple, ActionError>;

/// The v4 `hidden_commands` of the shell (not listed by `show actions`).
pub const HIDDEN_COMMANDS: &[&str] = &[
    "os",
    "sh",
    "pass",
    "quit",
    "eof",
    "exit",
    "_relative_run_script",
    "ipy",
];

fn flag(args: &ArgsMap, key: &str) -> bool {
    args.get(key).is_some_and(PyValue::is_truthy)
}

fn string(args: &ArgsMap, key: &str) -> Option<String> {
    match args.get(key) {
        None | Some(PyValue::None) => None,
        Some(v) => Some(v.py_str()),
    }
}

fn words(args: &ArgsMap, key: &str) -> Vec<String> {
    args.get(key)
        .and_then(PyValue::as_list)
        .map(|l| l.iter().map(PyValue::py_str).collect())
        .unwrap_or_default()
}

/// A parsed argument value as a Python object.
pub fn pyobj(v: &PyValue) -> PyObj {
    match v {
        PyValue::None => PyObj::None,
        PyValue::Bool(b) => PyObj::Bool(*b),
        PyValue::Int(i) => PyObj::Int(*i),
        PyValue::Float(f) => PyObj::Float(*f),
        PyValue::Str(s) => PyObj::Str(s.clone()),
        PyValue::List(l) => PyObj::List(l.iter().map(pyobj).collect()),
        PyValue::Tuple(l) => PyObj::Tuple(l.iter().map(pyobj).collect()),
        PyValue::Set(l) => PyObj::Set(l.iter().map(pyobj).collect()),
        PyValue::Dict(d) => PyObj::Dict(d.iter().map(|(k, v)| (pyobj(k), pyobj(v))).collect()),
        other => PyObj::Other(other.py_repr()),
    }
}

fn args_obj(args: &ArgsMap) -> PyObj {
    PyObj::Dict(
        args.iter()
            .map(|(k, v)| (PyObj::Str(k.clone()), pyobj(v)))
            .collect(),
    )
}

fn options_args(args: &ArgsMap) -> OptionsArgs {
    OptionsArgs {
        nopretty: flag(args, "nopretty"),
        ..OptionsArgs::default()
    }
}

/// `show version`.
pub fn show_version(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let version = crate::VERSION;
    if flag(args, "nopretty") {
        rt.print(&format!("{version}\n"));
    } else {
        let text = rt.with_formatter(|f| f.info(&format!("Meerschaum v{version}"), true));
        rt.print(&text);
    }
    Ok(SuccessTuple::success())
}

/// `show help`: `print(parser.format_help())`.
pub fn show_help(_args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let text = format!("{}\n", help::format_help(rt.columns));
    rt.print(&text);
    crate::python::print_help(rt);
    Ok(SuccessTuple::success())
}

/// The actions `show actions` lists.
pub fn listed_actions() -> Vec<String> {
    CORE_ACTIONS
        .iter()
        .map(|a| a.name.to_string())
        .filter(|a| !HIDDEN_COMMANDS.contains(&a.as_str()))
        .filter(|a| !crate::actions::V5_ONLY_ACTIONS.contains(&a.as_str()))
        .collect()
}

/// `show actions`.
pub fn show_actions(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let opts = OptionsArgs {
        name: "actions".into(),
        sort_options: true,
        ..options_args(args)
    };
    let mut actions = listed_actions();
    if let Ok(manifest) = crate::python::manifest(rt) {
        actions.extend(
            manifest
                .plugins
                .values()
                .flat_map(|p| &p.actions)
                .map(|a| a.name.clone()),
        );
    }
    if rt.in_shell {
        // PARITY: the shell adds its commands to v4's global `actions` dict
        // (`shell_attrs['_actions']` is that dict), so `show actions` lists them there.
        actions.extend(crate::shell::SHELL_COMMANDS.iter().map(|s| s.to_string()));
    }
    let text = rt.with_formatter(|f| f.print_options(&actions, &opts));
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// `show arguments`: `pprint(kw)`.
pub fn show_arguments(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let obj = args_obj(args);
    let text = rt.with_formatter(|f| f.pprint(&obj, false));
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// `show environment`: the `MRSM_*` variables.
pub fn show_environment(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let prefix = &static_config().environment.prefix;
    let vars: Vec<(PyObj, PyObj)> = (rt.vars)()
        .into_iter()
        .filter(|(k, _)| k.starts_with(prefix.as_str()))
        .map(|(k, v)| (PyObj::Str(k), PyObj::Str(v)))
        .collect();
    let nopretty = flag(args, "nopretty");
    let text = rt.with_formatter(|f| f.pprint(&PyObj::Dict(vars), nopretty));
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// The whole config for `show config` without keys.
fn full_config(f: &Formatter<'_>) -> Map {
    let config = f.config();
    let mut keys: Vec<String> = config.defaults().keys().cloned().collect();
    let config_dir = config.paths().config_dir;
    if let Ok(entries) = std::fs::read_dir(&config_dir) {
        let mut names: Vec<String> = entries
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        for name in names {
            let key = match name.rsplit_once('.') {
                Some((k, _)) => k.to_string(),
                None => continue,
            };
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
    }
    let symlinks_key = &static_config().config.symlinks_key;
    let mut out = Map::new();
    for key in keys {
        if &key == symlinks_key {
            continue;
        }
        if let Some(v) = config.get(&[key.as_str()]) {
            out.insert(key, v);
        }
    }
    out
}

/// `show config [keys...]`.
pub fn show_config(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let debug = flag(args, "debug");
    let nopretty = flag(args, "nopretty");
    let result = rt.with_formatter(|f| -> Result<Option<String>, ActionError> {
        let config = if action.is_empty() {
            // PARITY: v4 prints `_config()`, the keys this process has loaded so far (which
            // varies from run to run); the Rust CLI prints every key.
            Value::Map(full_config(f))
        } else {
            let keys: Vec<&str> = action.iter().map(String::as_str).collect();
            let (valid, value) = f.config().get_with(
                &keys,
                GetOptions {
                    warn: false,
                    ..GetOptions::default()
                },
            );
            let _ = f.config().take_warnings();
            if !valid {
                return Ok(None);
            }
            value
        };
        let Value::Map(map) = config else {
            // PARITY: `config.items()` on a scalar raises AttributeError (show.py:146).
            let type_name = match &config {
                Value::Str(_) => "str",
                Value::Int(_) => "int",
                Value::Float(_) => "float",
                Value::Bool(_) => "bool",
                Value::List(_) => "list",
                Value::Null => "NoneType",
                Value::Map(_) => "dict",
            };
            return Err(ActionError::new(
                "AttributeError",
                format!("'{type_name}' object has no attribute 'items'"),
            ));
        };
        let symlinks_key = &static_config().config.symlinks_key;
        let to_print: Map = if debug {
            map
        } else {
            map.into_iter().filter(|(k, _)| k != symlinks_key).collect()
        };
        if nopretty {
            return Ok(Some(format!(
                "{}\n",
                json_dumps(&Value::Map(to_print), JsonStyle::Default, true)
            )));
        }
        Ok(Some(
            f.pprint(&PyObj::from_value(&Value::Map(to_print)), false),
        ))
    })?;
    match result {
        None => {
            let list = PyObj::List(action.iter().map(|a| PyObj::Str(a.clone())).collect());
            Ok(SuccessTuple::fail(format!(
                "Invalid configuration keys '{}'.",
                list.repr()
            )))
        }
        Some(text) => {
            rt.print(&text);
            Ok(SuccessTuple::success())
        }
    }
}

/// The installed plugins, by scanning the plugins directories (`get_plugins_names()`): a
/// `<name>.py` file or a `<name>/` package with an `__init__.py`.
pub fn plugin_names(dirs: &[PathBuf]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.filter_map(|e| e.ok()) {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.starts_with('.') || name == "__pycache__" || name == "__init__.py" {
                continue;
            }
            let path = entry.path();
            let plugin = if path.is_dir() {
                path.join("__init__.py").exists().then(|| name.clone())
            } else {
                name.strip_suffix(".py").map(str::to_string)
            };
            if let Some(p) = plugin
                && !names.contains(&p)
            {
                names.push(p);
            }
        }
    }
    // PARITY: v4 lists plugins in `os.listdir` order of its symlink directory; sorted here.
    names.sort();
    names
}

/// The installed plugins as `show plugins` lists them: the Python plugins merged with the
/// native (C-ABI) ones, sorted. In pretty output a native plugin carries a ` (native)` marker;
/// `--nopretty` keeps bare names so scripts and completers see v4's format.
pub fn installed_plugin_listing(
    mut names: Vec<String>,
    natives: &[String],
    nopretty: bool,
) -> Vec<String> {
    if natives.is_empty() {
        return names;
    }
    names.retain(|n| !natives.contains(n));
    let mut all: Vec<(String, bool)> = names.into_iter().map(|n| (n, false)).collect();
    all.extend(natives.iter().map(|n| (n.clone(), true)));
    all.sort();
    all.dedup();
    all.into_iter()
        .map(|(name, native)| {
            if native && !nopretty {
                format!("{name} (native)")
            } else {
                name
            }
        })
        .collect()
}

/// `show plugins` (installed plugins only; `all` and `<username>` need a repository).
pub fn show_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    // `parse_repo_keys(repository)` raises for non-API repositories.
    crate::native::show::repo_keys(args, rt)?;
    let action = words(args, "action");
    if !(action.is_empty() || action == [""]) {
        return crate::native::show::show_plugins_repo(args, rt);
    }
    let nopretty = flag(args, "nopretty");
    let repository = string(args, "repository");
    // v5: native (C-ABI) plugins live in the same directories. A native plugin shadows a
    // Python plugin of the same name, so it is listed once, with a marker.
    let natives: Vec<String> = crate::native::session(rt)
        .native_plugins(rt.config())
        .map(|n| n.names())
        .unwrap_or_default();
    let text = rt.with_formatter(|f| {
        let repo = repository.clone().unwrap_or_else(|| {
            f.config()
                .get(&["meerschaum", "repository"])
                .map(|v| v.py_str())
                .unwrap_or_else(|| "api:mrsm".into())
        });
        let repo = if repo.contains(':') {
            repo
        } else {
            format!("api:{repo}")
        };
        let names = installed_plugin_listing(
            plugin_names(&f.config().paths().plugins_dirs),
            &natives,
            nopretty,
        );
        let mut out = String::new();
        if !nopretty {
            out.push_str(&f.info(
                &format!(
                    "To see all installable plugins from repository '{repo}', run `show plugins all`"
                ),
                true,
            ));
            out.push_str(&f.info(
                "To see plugins created by a certain user, run `show plugins [username]`",
                true,
            ));
        }
        let opts = OptionsArgs {
            header: Some("Installed plugins:".into()),
            ..options_args(args)
        };
        out.push_str(&f.print_options(&names, &opts));
        out
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// `reload`: the configuration is read fresh by every command, so this rebuilds the plugin
/// manifest unconditionally (a fresh worker imports every plugin again), re-opens the native
/// plugins and recycles the Python workers so the next action imports the plugins anew.
pub fn reload(_args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let session = crate::native::session(rt);
    let manifest = match session.reload_plugins(rt.config()) {
        Ok(manifest) => manifest,
        Err(error) => {
            return Ok(SuccessTuple::fail(format!(
                "Unable to reload plugins: {}",
                error.message
            )));
        }
    };
    let count = manifest.plugins.len();
    let message = format!(
        "Reloaded {count} plugin{}; the configuration is read fresh on every command.",
        if count == 1 { "" } else { "s" }
    );
    let text = rt.with_formatter(|f| f.info(&message, true));
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// What `edit config` reports after the editor exits (v4 ran `reload`).
fn reload_config(rt: &mut Runtime<'_>) -> ActionResult {
    let text = rt.with_formatter(|f| {
        f.info(
            "Nothing to reload: the Rust CLI reads the configuration on every command.",
            true,
        )
    });
    rt.print(&text);
    Ok(SuccessTuple::success())
}

/// `get_possible_keys()`: the default keys and the files in the config directory, sorted.
pub fn possible_config_keys(rt: &mut Runtime<'_>) -> Vec<String> {
    let config = rt.config();
    let mut keys: Vec<String> = config.defaults().keys().cloned().collect();
    if let Ok(entries) = std::fs::read_dir(config.paths().config_dir) {
        for e in entries.filter_map(|e| e.ok()) {
            let name = e.file_name().to_string_lossy().into_owned();
            let key = name
                .rsplit_once('.')
                .map(|(k, _)| k.to_string())
                .unwrap_or_default();
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
    }
    keys.sort();
    keys
}

/// `edit config [keys...]`: open each key's file in `$EDITOR`, re-read it, then `reload`.
pub fn edit_config(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    if flag(args, "rollback") {
        return crate::native::residual::rollback_config(args, rt);
    }
    let mut action = words(args, "action");
    if action.is_empty() {
        let noask = flag(args, "noask");
        let choice = if noask {
            "meerschaum".to_string()
        } else {
            let keys = possible_config_keys(rt);
            let menu = rt.with_formatter(|f| {
                let opts = OptionsArgs {
                    header: Some("Choose a configuration file to edit:".into()),
                    number_options: true,
                    ..OptionsArgs::default()
                };
                f.print_options_plain(&keys, &opts)
            });
            rt.print(&menu);
            let answer = rt
                .host
                .read_line(" ❓ [default: meerschaum] ")
                .unwrap_or_default();
            let answer = answer.trim().to_string();
            if answer.is_empty() {
                "meerschaum".into()
            } else if let Ok(i) = answer.parse::<usize>() {
                keys.get(i.wrapping_sub(1)).cloned().unwrap_or(answer)
            } else {
                answer
            }
        };
        action = vec![choice];
    }
    for key in &action {
        // Snapshot the pristine file before any edits so `edit config --rollback` can restore it.
        let backup_path = rt.config().with_state(|s| s.backup_config_key(key));
        // `get_config(key, write_missing=True)` creates the file from the defaults. v4 pops the key
        // from the in-memory config first (`config.pop(key, {})`): a key an `MRSM_CONFIG` patch
        // or an earlier read already loaded would otherwise never reach the lookup that writes
        // the missing file.
        let popped = rt.config().with_state(|s| {
            let mut config = s.config_lossless().clone();
            let popped = config.shift_remove(key);
            s.set_config_lossless(config);
            popped
        });
        let path = {
            let config = rt.config();
            let _ = config.get_with(
                &[key.as_str()],
                GetOptions {
                    warn: false,
                    ..GetOptions::default()
                },
            );
            let _ = config.take_warnings();
            config.with_state(|s| s.get_keyfile_path(key, true, None))
        };
        let Some(path) = path else {
            continue;
        };
        loop {
            if let Err(message) = crate::native::editor::edit_file(rt, &path) {
                return Ok(SuccessTuple::fail(message));
            }
            let text = std::fs::read_to_string(&path).unwrap_or_default();
            let parsed = if path.extension().is_some_and(|e| e == "json") {
                Value::from_json_str(&text).map_err(|e| e.to_string())
            } else {
                meerschaum_core::yaml::load(&text).map_err(|e| e.to_string())
            };
            match parsed {
                Ok(v) if v.truthy() => break,
                Ok(_) => break,
                Err(_) => {
                    // v4 puts the popped config back (`_config()[key] = key_config`).
                    if let Some(previous) = popped.clone().filter(|v| v.truthy()) {
                        rt.config().with_state(|s| {
                            let mut config = s.config_lossless().clone();
                            config.insert(key.clone(), previous);
                            s.set_config_lossless(config);
                        });
                    }
                    let warning =
                        rt.with_formatter(|f| f.warn(&format!("Could not parse key '{key}'.")));
                    rt.eprint(&warning);
                    if let Some(backup) = &backup_path
                        && crate::native::yes_no(
                            rt,
                            &format!("Restore '{key}' from the backup taken before editing?"),
                            "n",
                            false,
                            false,
                        )
                        .unwrap_or(false)
                    {
                        let (ok, message) = rt
                            .config()
                            .with_state(|s| s.restore_config_backup(key, Some(backup), 0));
                        if ok {
                            crate::native::info(rt, &message);
                            break;
                        }
                        crate::native::warn(rt, &message);
                    }
                    if rt
                        .host
                        .read_line(&format!(
                            " ❓ Press [Enter] to edit '{}', [CTRL+C] to exit.",
                            path.display()
                        ))
                        .is_none()
                    {
                        return Ok(SuccessTuple::fail(""));
                    }
                }
            }
        }
    }
    reload_config(rt)
}

/// `clear` without a subaction: clear the screen (`clear pipes` needs an instance).
pub fn clear(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    if !words(args, "action").is_empty() {
        return crate::native::residual::choose_subaction("clear", args, rt);
    }
    crate::shell::clear_screen(rt, flag(args, "debug"));
    Ok(SuccessTuple::ok(""))
}

/// `sh [commands]`: run through `bash -c`.
pub fn sh(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let mut action = words(args, "action");
    let sub_args = words(args, "sub_args");
    if let Some(first) = action.first().cloned() {
        if let Some(stripped) = first.strip_prefix('!') {
            action[0] = stripped.to_string();
        } else if first == "sh" {
            action.remove(0);
        }
    }
    let mut cmd_list = action.clone();
    cmd_list.extend(sub_args);
    let mut command = vec!["bash".to_string()];
    if !action.is_empty() {
        command.push("-c".into());
        command.push(crate::shlex::join(&cmd_list));
    }
    let spec = CommandSpec {
        program: command[0].clone(),
        args: command[1..].to_vec(),
        ..CommandSpec::default()
    };
    match rt.host.run(&spec) {
        Err(_) => Ok(SuccessTuple::fail(format!(
            "Invalid commands: '{}'",
            PyObj::List(command.iter().map(|c| PyObj::Str(c.clone())).collect()).repr()
        ))),
        Ok(0) => Ok(SuccessTuple::success()),
        Ok(code) => Ok(SuccessTuple::fail(format!("Returned exit code: {code}"))),
    }
}

/// The Python interpreter `mrsm python` starts: `$MRSM_PYTHON`, else `python3` (`python` on
/// Windows); a `--venv` uses that virtual environment's interpreter.
pub fn python_executable(rt: &mut Runtime<'_>, venv: Option<&str>) -> String {
    if let Some(venv) = venv {
        let paths = rt.config().paths();
        let venvs = paths
            .get("VIRTENV_RESOURCES_PATH")
            .map(|p| PathBuf::from(p.as_posix()))
            .unwrap_or_else(|| paths.root_dir.join("venvs"));
        let bin = if cfg!(windows) {
            "Scripts/python.exe"
        } else {
            "bin/python"
        };
        return venvs.join(venv).join(bin).to_string_lossy().into_owned();
    }
    (rt.env)("MRSM_PYTHON")
        .unwrap_or_else(|| if cfg!(windows) { "python" } else { "python3" }.into())
}

fn random_name(n: usize) -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        ^ (std::process::id() as u64).rotate_left(32);
    (0..n)
        .map(|_| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            ALPHABET[(seed % ALPHABET.len() as u64) as usize] as char
        })
        .collect()
}

/// The code `mrsm python` writes to its init script, and whether to open `ptpython`.
pub fn python_init_script(
    action: &[String],
    venv: Option<&str>,
    sub_args: &[String],
    nopretty: bool,
) -> String {
    let mut joined: Vec<String> = if venv.is_none() && sub_args.is_empty() {
        vec!["import meerschaum as mrsm".into()]
    } else {
        Vec::new()
    };
    let mut line = String::new();
    for (i, a) in action.iter().enumerate() {
        if a.is_empty() {
            continue;
        }
        line.push_str(a);
        line.push(' ');
        if a.ends_with(';') || i + 1 == action.len() {
            line.pop();
            joined.push(std::mem::take(&mut line));
        }
    }
    let mut print_command = String::from(
        "from meerschaum.utils.packages import attempt_import; \
         ptft = attempt_import(\"prompt_toolkit.formatted_text\", lazy=False); \
         pts = attempt_import(\"prompt_toolkit.shortcuts\"); ansi = ptft.ANSI(\"\"\"",
    );
    let ps1 = "\\033[1m>>> \\033[0m";
    for (i, a) in joined.iter().enumerate() {
        let mut l = format!("{ps1}{}", a.replace(';', "\n"));
        if !l.contains('\n') && i + 1 != joined.len() {
            l.push('\n');
        }
        print_command.push_str(&l);
    }
    print_command.push_str("\"\"\"); pts.print_formatted_text(ansi); ");
    if nopretty || venv.is_some() || !sub_args.is_empty() {
        print_command.clear();
    }
    let mut command = print_command;
    for a in &joined {
        command.push_str(a);
        if !a.ends_with(';') {
            command.push(';');
        }
        command.push(' ');
    }
    command
}

/// `python [code]`: start an interpreter with `meerschaum` imported.
pub fn python(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let sub_args = words(args, "sub_args");
    let venv = string(args, "venv");
    let noask = flag(args, "noask");
    let mut nopretty = flag(args, "nopretty") || noask;
    if string(args, "executor_keys").is_some_and(|e| e.starts_with("api:")) {
        let w = rt.with_formatter(|f| {
            f.warn("Cannot open a Python REPL remotely, falling back to local...")
        });
        rt.eprint(&w);
    }
    let command = python_init_script(&action, venv.as_deref(), &sub_args, nopretty);
    if !sub_args.is_empty() {
        nopretty = true;
    }
    let dir = {
        let config = rt.config();
        config.paths().path("PYTHON_RESOURCES_PATH")
    };
    let _ = std::fs::create_dir_all(&dir);
    let script = dir.join(format!("{}.py", random_name(8)));
    std::fs::write(&script, &command)
        .map_err(|e| ActionError::new("OSError", format!("{e}: '{}'", script.display())))?;
    let script_posix = script.to_string_lossy().replace('\\', "/");
    let python = python_executable(rt, venv.as_deref());
    let spec = if !nopretty {
        // `run_python_package('ptpython', ['--dark-bg', '-i', script])`, else a plain REPL.
        let has_ptpython = rt
            .host
            .run(&CommandSpec {
                program: python.clone(),
                args: vec!["-c".into(), "import ptpython".into()],
                quiet: true,
                ..CommandSpec::default()
            })
            .is_ok_and(|rc| rc == 0);
        if has_ptpython {
            CommandSpec {
                program: python.clone(),
                args: vec![
                    "-m".into(),
                    "ptpython".into(),
                    "--dark-bg".into(),
                    "-i".into(),
                    script_posix.clone(),
                ],
                ..CommandSpec::default()
            }
        } else {
            CommandSpec {
                program: python.clone(),
                args: vec!["-i".into(), script_posix.clone()],
                ..CommandSpec::default()
            }
        }
    } else if !sub_args.is_empty() {
        CommandSpec {
            program: python.clone(),
            args: sub_args.clone(),
            ..CommandSpec::default()
        }
    } else {
        let mut a: Vec<String> = if noask { Vec::new() } else { vec!["-i".into()] };
        a.push(script_posix.clone());
        CommandSpec {
            program: python.clone(),
            args: a,
            ..CommandSpec::default()
        }
    };
    let rc = rt.host.run(&spec).unwrap_or(1);
    if script.exists() && std::fs::remove_file(&script).is_err() {
        let w = rt.with_formatter(|f| {
            f.warn(&format!(
                "Failed to clean up tempory file '{}'.",
                script.display()
            ))
        });
        rt.eprint(&w);
    }
    Ok(if rc == 0 {
        SuccessTuple::success()
    } else {
        SuccessTuple::fail(format!("Python interpreter returned {rc}."))
    })
}
