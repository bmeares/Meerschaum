//! The plugin administration actions: `uninstall plugins`, `delete plugins`, `edit plugins`,
//! `verify plugins`, `upgrade plugins` and the upload half of `register plugins`, ported from
//! `meerschaum/actions/{uninstall,delete,edit,verify,upgrade,register}.py` and the `Plugin` /
//! `APIConnector` methods they call (`uninstall`, `remove_venv`, `make_tar`,
//! `register_plugin`, `delete_plugin`, `get_plugin_attributes`).
//!
//! # Differences from v4
//!
//! - A plugin's module docstring (the default description in `register plugins`) is read from the
//!   source text, not from the imported module, so a plugin that fails to import still offers it.
//! - `register plugins` reads a changed description as one line (v4's `prompt_toolkit` multiline
//!   box needs a terminal; `Esc+Enter` has no counterpart on a pipe).
//! - `edit plugins` opens `$VISUAL` / `$EDITOR` (default `vi`, `native/editor.rs`); v4 falls back to installing `pyvim`.
//! - `verify plugins` imports each plugin in a fresh worker (`reload`), reports native
//!   plugins as imported when the SDK opened them, and reads a failing plugin's error from the
//!   manifest. v4 imports in the CLI process.
//! - A native (C-ABI) plugin is installed, uninstalled and verified like a Python one; its source
//!   cannot be edited (`edit plugins` says so).

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use meerschaum_connector_api::traits::PluginRecord;
use meerschaum_core::static_config::static_config;
use meerschaum_core::value::{Map, Value};
use meerschaum_core::{SuccessTuple, pyfmt};

use crate::builtin::{ActionError, ActionResult};
use crate::dispatch::Runtime;
use crate::format::{OptionsArgs, TupleArgs};
use crate::value::{ArgsMap, PyValue};

use super::ask::{Ask, ask};
use super::lifecycle::{
    all_plugin_names, exception, plugin_installed, plugin_is_native, refresh, repo_connector,
};
use super::plugin_files::{
    declared_version, is_native_plugin_dir, plugin_file, remove_origin, split_repo,
};
use super::{flag, info, items_str, plural, print_tuple, session, warn, words, yes_no};

/// `Plugin(name)`: the name without its `@repository`.
fn plugin_name(name: &str) -> Result<String, ActionError> {
    split_repo(name, &static_config().plugins.repo_separator)
        .map(|(name, _)| name)
        .map_err(exception)
}

fn plugins_dirs(rt: &mut Runtime<'_>) -> Vec<PathBuf> {
    rt.config().paths().plugins_dirs.clone()
}

// ---------------------------------------------------------------------------------------------
// uninstall plugins
// ---------------------------------------------------------------------------------------------

/// `Plugin.uninstall()`: the source, the virtual environment and the origin entry.
fn uninstall_plugin(rt: &mut Runtime<'_>, args: &ArgsMap, name: &str) -> SuccessTuple {
    let dirs = plugins_dirs(rt);
    let mut warnings = 0;
    if !plugin_installed(rt, args, name) {
        info(
            rt,
            &format!(
                "Plugin '{name}' doesn't seem to be installed.\n    Checking for artifacts..."
            ),
        );
    } else {
        let mut failure: Option<String> = None;
        let mut removed = false;
        if let Some(file) = plugin_file(&dirs, name) {
            removed = true;
            let real = std::fs::canonicalize(&file).unwrap_or(file);
            let result = if real.file_name().is_some_and(|n| n == "__init__.py") {
                real.parent().map_or(Ok(()), std::fs::remove_dir_all)
            } else {
                std::fs::remove_file(&real)
            };
            if let Err(e) = result {
                failure = Some(e.to_string());
            }
        }
        for dir in &dirs {
            let source = dir.join(format!("{name}.rs"));
            if source.is_file() {
                removed = true;
                if let Err(e) = std::fs::remove_file(&source) {
                    failure.get_or_insert(e.to_string());
                }
            }
            let native = dir.join(name);
            if is_native_plugin_dir(&native) {
                removed = true;
                if let Err(e) = std::fs::remove_dir_all(&native) {
                    failure.get_or_insert(e.to_string());
                }
            }
        }
        match failure {
            Some(e) => {
                warn(
                    rt,
                    &format!("Could not remove source files for plugin '{name}':\n{e}"),
                );
                warnings += 1;
            }
            None if removed => info(rt, &format!("Removed source files for plugin '{name}'.")),
            None => {}
        }
    }

    let venv_path = {
        let paths = rt.config().paths();
        paths
            .get("VIRTENV_RESOURCES_PATH")
            .map(|p| PathBuf::from(p.as_posix()))
            .unwrap_or_else(|| paths.root_dir.join("venvs"))
            .join(name)
    };
    if venv_path.exists() {
        match std::fs::remove_dir_all(&venv_path) {
            Err(e) => {
                warn(
                    rt,
                    &format!("Failed to remove virtual environment for plugin '{name}':\n{e}"),
                );
                warnings += 1;
            }
            Ok(()) => info(
                rt,
                &format!("Removed virtual environment from plugin '{name}'."),
            ),
        }
    }

    let success = warnings < 3;
    let origins = static_config().plugins.origins_filename.clone();
    for dir in &dirs {
        remove_origin(dir, &origins, name);
    }
    if success {
        SuccessTuple::ok(format!("Successfully uninstalled plugin '{name}'."))
    } else {
        SuccessTuple::fail(format!("Failed to uninstall plugin '{name}'."))
    }
}

/// `uninstall plugins [names] [--force]`: remove plugins; repository registrations stay.
pub fn uninstall_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    repo_connector(rt, args)?;
    let force = flag(args, "force");
    let mut to_uninstall: Vec<String> = Vec::new();
    for requested in words(args, "action") {
        let name = plugin_name(&requested)?;
        if force || plugin_installed(rt, args, &name) {
            to_uninstall.push(name);
        } else {
            warn(
                rt,
                &format!("Plugin '{name}' is not installed. Add `--force` to try anyway."),
            );
        }
    }
    if to_uninstall.is_empty() {
        return Ok(SuccessTuple::fail("No plugins to uninstall."));
    }

    let mut question = "Are you sure you want to remove these plugins?\n".to_string();
    for name in &to_uninstall {
        question.push_str(&format!(" - {name}\n"));
    }
    let answer = force || yes_no(rt, &question, "n", flag(args, "yes"), flag(args, "noask"))?;
    if !answer {
        return Ok(SuccessTuple::fail("No plugins were uninstalled."));
    }

    let (mut succeeded, mut failed) = (0usize, 0usize);
    for name in &to_uninstall {
        let result = uninstall_plugin(rt, args, name);
        if result.success {
            succeeded += 1;
        } else {
            warn(rt, &result.message);
            failed += 1;
        }
    }
    refresh(rt);
    Ok(SuccessTuple {
        success: succeeded > 0,
        message: format!(
            "Done uninstalling {} plugins\n    ({succeeded} succeeded, {failed} failed).",
            succeeded + failed
        ),
    })
}

// ---------------------------------------------------------------------------------------------
// delete plugins
// ---------------------------------------------------------------------------------------------

/// `delete plugins <names>`: delete plugins from a repository.
pub fn delete_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let (keys, connector) = repo_connector(rt, args)?;
    let mut action = Vec::new();
    for requested in words(args, "action") {
        action.push(plugin_name(&requested)?);
    }
    let force = flag(args, "force");
    let answer = force
        || {
            let list = format!("\n  - {}", action.join("\n  - "));
            let question = format!(
                "Are you sure you want to delete the following plugins from the repository '{keys}'?\nTHIS CANNOT BE UNDONE!\n{list}\n"
            );
            yes_no(rt, &question, "n", flag(args, "yes"), flag(args, "noask"))?
        };
    if !answer {
        return Ok(SuccessTuple::fail("No plugins deleted."));
    }
    let session = session(rt);
    for name in &action {
        info(
            rt,
            &format!("Deleting plugin '{name}' from Meerschaum repository '{keys}'..."),
        );
        let record = PluginRecord {
            name: name.clone(),
            ..PluginRecord::default()
        };
        let result = session.block_on(connector.delete_plugin(&record));
        print_tuple(rt, &result, TupleArgs::default());
    }
    refresh(rt);
    Ok(SuccessTuple::success())
}

// ---------------------------------------------------------------------------------------------
// edit plugins
// ---------------------------------------------------------------------------------------------

/// `edit plugins <names>`: open a plugin's source in `$EDITOR`; an unknown name offers to
/// bootstrap a new plugin.
pub fn edit_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    if action.is_empty() {
        return Ok(SuccessTuple::fail("Specify which plugin to edit."));
    }
    let dirs = plugins_dirs(rt);
    for requested in action {
        let name = plugin_name(&requested)?;
        let Some(file) = plugin_file(&dirs, &name) else {
            if plugin_is_native(rt, &name) {
                return Ok(SuccessTuple::fail(format!(
                    "Plugin '{name}' is a native plugin; edit its source and rebuild it."
                )));
            }
            warn(rt, &format!("Plugin '{name}' is not installed."));
            let create = yes_no(
                rt,
                &format!("Would you like to create a new plugin '{name}'?"),
                "y",
                flag(args, "yes"),
                flag(args, "noask"),
            )?;
            if !create {
                return Ok(SuccessTuple::fail(format!(
                    "Plugin '{name}' does not exist."
                )));
            }
            let mut bootstrap_args = args.clone();
            bootstrap_args.insert("action".into(), PyValue::str_list(&[name.as_str()]));
            // PARITY: v4 ignores the wizard's result; a failure (an editor that cannot be started,
            // an invalid name) is the action's result here.
            let created = super::bootstrap_plugins::bootstrap_plugins(&bootstrap_args, rt)?;
            if !created.success {
                return Ok(created);
            }
            continue;
        };
        let path = std::fs::canonicalize(&file).unwrap_or(file);
        let opened = ask(
            rt,
            &format!("Press [Enter] to open '{}':", path.display()),
            &Ask::new().no_icon(),
        );
        if opened.is_err() {
            continue;
        }
        edit_file(rt, &path)?;
        crate::builtin::reload(args, rt)?;
    }
    Ok(SuccessTuple::success())
}

/// `edit_file(path)` for the plugin actions: a start failure is an error, not a silent success.
pub fn edit_file(rt: &mut Runtime<'_>, path: &Path) -> Result<bool, ActionError> {
    super::editor::edit_file(rt, path).map_err(exception)
}

// ---------------------------------------------------------------------------------------------
// verify plugins
// ---------------------------------------------------------------------------------------------

/// `verify plugins [names]`: check that each plugin imports.
pub fn verify_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let names = if action.is_empty() {
        all_plugin_names(rt, args)
    } else {
        action.clone()
    };
    if names.is_empty() {
        return Ok(if action.is_empty() {
            SuccessTuple::ok("There are no installed plugins; nothing to do.")
        } else {
            SuccessTuple::fail(format!("Unable to verify plugins {}.", items_str(&action)))
        });
    }
    let opts = OptionsArgs {
        header: Some(format!("Verifying {} plugins", names.len())),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&names, &opts));
    rt.print(&text);

    // A fresh worker imports every plugin again, so a package installed since is seen.
    let session = session(rt);
    let manifest = session
        .reload_plugins(rt.config())
        .map_err(|e| ActionError::new("WorkerError", e.message))?;
    let mut failed: Vec<String> = Vec::new();
    for requested in &names {
        let name = plugin_name(requested)?;
        let imported = manifest
            .plugins
            .get(&name)
            .is_some_and(|p| p.import_error.is_none())
            || (plugin_is_native(rt, &name) && !manifest.plugins.contains_key(&name));
        if !imported {
            failed.push(name.clone());
        }
        let message = if imported {
            format!("Imported plugin '{name}'.")
        } else {
            format!("Failed to import plugin '{name}'.")
        };
        print_tuple(
            rt,
            &SuccessTuple {
                success: imported,
                message,
            },
            TupleArgs {
                calm: true,
                ..TupleArgs::default()
            },
        );
    }
    let success = failed.is_empty();
    let message = if success {
        format!("Successfully imported {} plugins.", names.len())
    } else {
        format!(
            "Failed to import plugin{} {}.",
            plural(failed.len()),
            items_str(&failed)
        )
    };
    Ok(SuccessTuple { success, message })
}

// ---------------------------------------------------------------------------------------------
// upgrade plugins
// ---------------------------------------------------------------------------------------------

/// `upgrade plugins [names]`: install every (or the named) plugin again from its repository.
pub fn upgrade_plugins(args: &ArgsMap, rt: &mut Runtime<'_>) -> ActionResult {
    let action = words(args, "action");
    let to_install = if action.is_empty() {
        all_plugin_names(rt, args)
    } else {
        action
    };
    if to_install.is_empty() {
        return Ok(SuccessTuple::fail("No plugins to upgrade."));
    }
    let opts = OptionsArgs {
        header: Some("Plugins to Upgrade:".into()),
        ..OptionsArgs::default()
    };
    let text = rt.with_formatter(|f| f.print_options(&to_install, &opts));
    rt.print(&text);
    let question = format!("Upgrade {} plugins?", to_install.len());
    if flag(args, "force") || yes_no(rt, &question, "y", flag(args, "yes"), flag(args, "noask"))? {
        let mut install_args = args.clone();
        install_args.insert("action".into(), PyValue::str_list(&to_install));
        return super::lifecycle::install_plugins(&install_args, rt);
    }
    Ok(SuccessTuple::fail("No plugins upgraded."))
}

// ---------------------------------------------------------------------------------------------
// register plugins
// ---------------------------------------------------------------------------------------------

/// The module docstring of Python source text (`module.__doc__`): the first statement, when it
/// is a plain string literal.
pub fn module_docstring(source: &str) -> Option<String> {
    let mut rest = source.trim_start_matches('\u{feff}');
    // Blank lines, comments (shebang, encoding cookie) and form feeds come before the statement.
    loop {
        rest = rest.trim_start_matches([' ', '\t', '\r', '\n', '\u{c}']);
        if rest.starts_with('#') {
            rest = rest.split_once('\n').map_or("", |(_, r)| r);
        } else {
            break;
        }
    }
    let prefix_len = rest
        .chars()
        .take_while(|c| matches!(c, 'r' | 'R' | 'u' | 'U'))
        .count();
    let (prefix, body) = rest.split_at(prefix_len);
    let raw = prefix.contains(['r', 'R']);
    let quote = ["\"\"\"", "'''", "\"", "'"]
        .into_iter()
        .find(|q| body.starts_with(q))?;
    let content = &body[quote.len()..];
    let mut out = String::new();
    let mut chars = content.char_indices().peekable();
    while let Some((i, c)) = chars.next() {
        if content[i..].starts_with(quote) {
            return Some(out);
        }
        if c == '\\' {
            let (_, next) = chars.next()?;
            if raw {
                out.push('\\');
                out.push(next);
                continue;
            }
            match next {
                '\n' => {}
                'n' => out.push('\n'),
                't' => out.push('\t'),
                'r' => out.push('\r'),
                '\\' => out.push('\\'),
                '\'' => out.push('\''),
                '"' => out.push('"'),
                other => {
                    out.push('\\');
                    out.push(other);
                }
            }
            continue;
        }
        if c == '\n' && quote.len() == 1 {
            return None;
        }
        out.push(c);
    }
    None
}

/// `plugin.module.__doc__` stripped, from the plugin's source file.
fn plugin_docstring(file: &Path) -> String {
    std::fs::read_to_string(file)
        .ok()
        .and_then(|text| module_docstring(&text))
        .map(|doc| doc.trim().to_string())
        .unwrap_or_default()
}

/// `register plugins`: upload each plugin's archive, asking whether to set its description.
pub fn register_uploads(
    args: &ArgsMap,
    rt: &mut Runtime<'_>,
    to_register: &[String],
) -> ActionResult {
    let (keys, connector) = repo_connector(rt, args)?;
    let session = session(rt);
    let dirs = plugins_dirs(rt);
    let (noask, force, yes) = (flag(args, "noask"), flag(args, "force"), flag(args, "yes"));
    let debug = flag(args, "debug");
    let mut succeeded = 0usize;
    let mut failed = 0usize;
    for name in to_register {
        let mut record = PluginRecord {
            name: name.clone(),
            ..PluginRecord::default()
        };
        record.attributes = session
            .block_on(connector.get_plugin_attributes(&record))
            .map_err(|e| ActionError::new("Exception", e.to_string()))?;
        let file = plugin_file(&dirs, name);
        let source = file
            .as_ref()
            .and_then(|f| std::fs::read_to_string(f).ok())
            .unwrap_or_default();
        record.version = declared_version(&source).ok().flatten();
        let existing = record
            .attributes
            .get("description")
            .map(pyfmt::py_str)
            .unwrap_or_default();
        let doc = file.as_deref().map(plugin_docstring).unwrap_or_default();
        let mut desc: Option<String> = Some(if existing.is_empty() { doc } else { existing });

        let current = desc.clone().unwrap_or_default();
        let mut question = format!("Would you like to add a description to plugin '{name}'?");
        if !current.is_empty() {
            info(
                rt,
                &format!("Found existing description for plugin '{name}':"),
            );
            rt.print(&format!("{current}\n"));
            question = "Would you like to overwrite this description?\nTo edit the existing text, visit /dash/plugins for this repository.".into();
        }
        if !noask && !force && yes_no(rt, &question, "n", yes, false)? {
            info(rt, "Press (Esc + Enter) to submit, (CTRL + C) to cancel.");
            desc = ask(
                rt,
                "",
                &Ask::new().no_icon().editable(current.trim().to_string()),
            )
            .ok();
            if desc.as_deref() == Some("") {
                desc = None;
            }
        }
        if let Some(description) = desc {
            record.attributes =
                Map::from_iter([("description".to_string(), Value::Str(description))]);
        }
        info(
            rt,
            &format!("Registering plugin '{name}' to Meerschaum API '{keys}'...\n"),
        );
        let archive = make_tar(rt, name, debug)?;
        let bytes = std::fs::read(&archive)
            .map_err(|e| ActionError::new("FileNotFoundError", e.to_string()))?;
        let archive_name = format!("{name}.tar.gz");
        let result = session.block_on(connector.register_plugin(&record, bytes, &archive_name));
        print_tuple(
            rt,
            &SuccessTuple {
                success: result.success,
                message: format!("{}\n", result.message),
            },
            TupleArgs::default(),
        );
        if result.success {
            succeeded += 1;
        } else {
            failed += 1;
        }
    }
    let message = format!(
        "Finished registering {} plugins\n    ({succeeded} succeeded, {failed} failed).",
        to_register.len()
    );
    refresh(rt);
    Ok(SuccessTuple {
        success: succeeded > 0,
        message,
    })
}

/// `Plugin.make_tar()`: compress the plugin's sources into `<archives>/<name>.tar.gz`.
fn make_tar(rt: &mut Runtime<'_>, name: &str, debug: bool) -> Result<PathBuf, ActionError> {
    let dirs = plugins_dirs(rt);
    let file = plugin_file(&dirs, name)
        .ok_or_else(|| exception(format!("Could not find file for plugin '{name}'.")))?;
    let archive_path = {
        let paths = rt.config().paths();
        paths
            .get("PLUGINS_ARCHIVES_RESOURCES_PATH")
            .map(|p| PathBuf::from(p.as_posix()))
            .unwrap_or_else(|| {
                paths
                    .root_dir
                    .join(".internal")
                    .join("plugins")
                    .join(".archives")
            })
            .join(format!("{name}.tar.gz"))
    };
    let is_dir = file.file_name().is_some_and(|n| n == "__init__.py");
    let real = std::fs::canonicalize(&file).unwrap_or(file.clone());
    let write = || -> std::io::Result<()> {
        if let Some(parent) = archive_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let encoder = flate2::write::GzEncoder::new(
            std::fs::File::create(&archive_path)?,
            flate2::Compression::default(),
        );
        let mut builder = tar::Builder::new(encoder);
        builder.follow_symlinks(false);
        if !is_dir {
            let plain = real.clone();
            builder.append_path_with_name(&plain, format!("{name}.py"))?;
        } else {
            let root = real.parent().unwrap_or(&real).to_path_buf();
            let ignored = patterns_to_ignore(&root);
            if debug {
                let mut shown: Vec<&String> = ignored.iter().collect();
                shown.sort();
                eprintln!("Patterns to ignore:\n{shown:?}");
            }
            let mut files = Vec::new();
            walk_files(&root, "", &mut files);
            files.sort();
            for relative in files {
                let member = format!("{name}/{relative}");
                let base = relative.rsplit('/').next().unwrap_or_default();
                // PARITY: `if pattern in str(fp) or f.startswith('.')` runs inside the loop over
                // the patterns, so a plugin whose patterns match nothing keeps its dotfiles.
                let skip = ignored
                    .iter()
                    .any(|pattern| member.contains(pattern.as_str()) || base.starts_with('.'));
                if skip {
                    continue;
                }
                builder.append_path_with_name(root.join(&relative), &member)?;
            }
        }
        builder.into_inner()?.finish()?;
        Ok(())
    };
    write().map_err(|e| ActionError::new("OSError", e.to_string()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&archive_path, std::fs::Permissions::from_mode(0o775));
    }
    Ok(archive_path)
}

/// Files below `dir` as `/`-joined relative paths (`os.walk` with `followlinks=False`: a
/// symlink to a directory is listed but not entered, and is not a file).
fn walk_files(dir: &Path, prefix: &str, out: &mut Vec<String>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        let relative = if prefix.is_empty() {
            name.clone()
        } else {
            format!("{prefix}/{name}")
        };
        let path = entry.path();
        let is_link = entry.file_type().is_ok_and(|t| t.is_symlink());
        if path.is_dir() {
            if !is_link {
                walk_files(&path, &relative, out);
            }
        } else {
            out.push(relative);
        }
    }
}

/// `parse_gitignore()`: the default patterns themselves without a `.gitignore`, else the set of
/// files below `root` that `default_patterns + .gitignore` match (`PathSpec.match_tree`).
fn patterns_to_ignore(root: &Path) -> BTreeSet<String> {
    const DEFAULTS: [&str; 5] = [".pyc", "__pycache__/", "eggs/", "__pypackages__/", ".git"];
    let gitignore = root.join(".gitignore");
    let Ok(text) = std::fs::read_to_string(&gitignore) else {
        return DEFAULTS.iter().map(|s| s.to_string()).collect();
    };
    let patterns: Vec<GitPattern> = DEFAULTS
        .iter()
        .copied()
        .chain(text.lines())
        .filter_map(GitPattern::new)
        .collect();
    let mut files = Vec::new();
    walk_files(root, "", &mut files);
    files
        .into_iter()
        .filter(|file| {
            let mut included = false;
            for pattern in &patterns {
                if pattern.regex.is_match(file) {
                    included = pattern.include;
                }
            }
            included
        })
        .collect()
}

/// One `gitwildmatch` pattern: a regex over `/`-joined relative paths.
struct GitPattern {
    regex: regex::Regex,
    include: bool,
}

impl GitPattern {
    fn new(line: &str) -> Option<GitPattern> {
        let mut pattern = line.trim();
        if pattern.is_empty() || pattern.starts_with('#') || pattern == "/" {
            return None;
        }
        let include = !pattern.starts_with('!');
        if !include {
            pattern = &pattern[1..];
        }
        let directory = pattern.ends_with('/');
        let pattern = pattern.trim_end_matches('/');
        let anchored = pattern.contains('/');
        let pattern = pattern.trim_start_matches('/');
        let segments: Vec<&str> = pattern.split('/').collect();
        let last = segments.len() - 1;
        let mut body = String::new();
        for (i, segment) in segments.iter().enumerate() {
            if *segment == "**" {
                if last == 0 {
                    body.push_str(".*");
                } else if i == 0 {
                    body.push_str("(?:.*/)?");
                } else if i == last {
                    body.push_str(".*");
                } else {
                    body.push_str("(?:[^/]+/)*");
                }
                continue;
            }
            body.push_str(&glob_segment(segment));
            if i != last {
                body.push('/');
            }
        }
        let head = if anchored { "^" } else { "^(?:.*/)?" };
        let tail = if directory { "/.*$" } else { "(?:/.*)?$" };
        regex::Regex::new(&format!("{head}{body}{tail}"))
            .ok()
            .map(|regex| GitPattern { regex, include })
    }
}

/// One path segment of a glob as a regex fragment.
fn glob_segment(segment: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = segment.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            '*' => out.push_str("[^/]*"),
            '?' => out.push_str("[^/]"),
            '\\' if i + 1 < chars.len() => {
                i += 1;
                out.push_str(&regex::escape(&chars[i].to_string()));
            }
            '[' => {
                if let Some(end) = chars[i + 1..].iter().position(|c| *c == ']') {
                    let class: String = chars[i + 1..i + 1 + end].iter().collect();
                    let class = match class.strip_prefix(['!', '^']) {
                        Some(rest) => format!("^{rest}"),
                        None => class,
                    };
                    out.push('[');
                    out.push_str(&class.replace('\\', "\\\\"));
                    out.push(']');
                    i += end + 1;
                } else {
                    out.push_str("\\[");
                }
            }
            c => out.push_str(&regex::escape(&c.to_string())),
        }
        i += 1;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docstrings_come_from_the_first_statement() {
        assert_eq!(
            module_docstring("#! /usr/bin/env python\n\"\"\"\nHello.\n\"\"\"\nx = 1\n").as_deref(),
            Some("\nHello.\n")
        );
        assert_eq!(module_docstring("'one' ; x").as_deref(), Some("one"));
        assert_eq!(module_docstring("x = 1\n\"\"\"late\"\"\"\n"), None);
        assert_eq!(module_docstring("r'a\\nb'").as_deref(), Some("a\\nb"));
        assert_eq!(module_docstring("\"a\\nb\"").as_deref(), Some("a\nb"));
        assert_eq!(module_docstring("\"unterminated\nx"), None);
    }

    /// `PathSpec.from_lines('gitwildmatch', [pattern]).match_file(path)` (pathspec 1.1.1).
    #[test]
    fn gitignore_patterns_match_like_pathspec() {
        let cases: &[(&str, &str, bool)] = &[
            ("*.log", "a/b/x.log", true),
            ("*.log", "x.log", true),
            ("*.log", "x.logx", false),
            ("__pycache__/", "__pycache__/x.pyc", true),
            ("__pycache__/", "pkg/__pycache__/x.pyc", true),
            ("__pycache__/", "__pycache__", false),
            ("/build", "pkg/build/x", false),
            ("/build", "build/x", true),
            ("/build", "build", true),
            ("docs/*.md", "docs/a.md", true),
            ("docs/*.md", "x/docs/a.md", false),
            ("docs/*.md", "docs/a/b.md", false),
            ("a/**/b", "a/x/y/b", true),
            ("a/**/b", "a/b", true),
            ("a/**/b", "a/b/c", true),
            ("**/foo", "x/foo", true),
            ("**/foo", "foo", true),
            ("foo/**", "foo/bar", true),
            ("foo/**", "foo", false),
            ("*.py[co]", "x.pyc", true),
            ("*.py[co]", "x.pyd", false),
            ("?.txt", "a.txt", true),
            ("?.txt", "ab.txt", false),
            (".pyc", "a.pyc", false),
            (".pyc", ".pyc", true),
            (".git", "a/.git/config", true),
            (".git", ".gitignore", false),
            ("eggs/", "x/eggs/y", true),
            ("data/", "data", false),
            ("!x", "x", false),
            ("foo", "foo/bar/baz", true),
            ("foo", "a/foo", true),
            ("a/b", "a/b", true),
            ("a/b", "c/a/b", false),
            ("a/b/", "a/b/c", true),
            ("*", "x", true),
            ("*", "a/b", true),
            ("**", "a/b", true),
            ("[!a]x", "bx", true),
            ("[!a]x", "ax", false),
            ("\\#x", "#x", true),
            ("x*y", "xaby", true),
            ("x*y", "x/y", false),
        ];
        for (pattern, path, expected) in cases {
            let matched =
                GitPattern::new(pattern).is_some_and(|p| p.include && p.regex.is_match(path));
            assert_eq!(matched, *expected, "{pattern:?} on {path:?}");
        }
        assert!(GitPattern::new("# comment").is_none());
    }
}
