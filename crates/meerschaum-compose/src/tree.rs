//! Compose trees (v5, design decision D22): `include:` and `parent:`.
//!
//! A root compose file lists other compose files under `include:` (a file, a directory or a
//! glob, resolved relative to the including file, transitively). Each included project keeps its
//! own identity (project name, plugins directory, relative paths and `.env`), runs under the
//! root's `root_dir` unless it sets one, and has the including project's values layered on top
//! of its own (the topmost project wins):
//!
//! - `config` is deep-merged (the project's own `stack.project_name` stays);
//! - `environment:` and the `.env` variables of the including project win;
//! - `pipes:` matched by `(connector, metric, location, instance)`: the including project's
//!   entry replaces the included one;
//! - `plugins:` matched by name: the including project's spec wins;
//! - `jobs:` stay per project; the including project overrides one with the key
//!   `<project>/<job>`;
//! - a project searches its own `plugins_dir` first, then its ancestors' (never a sibling's).
//!
//! `parent:` (an explicit path, directory or compose file, relative to the file that has it)
//! makes every subaction run in that file's tree, limited to the project.
//!
//! [`load_tree`] reads the files and applies the overrides; the findings it records (each
//! override, include cycles, files that fail to parse) are what `compose explain` lists.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use meerschaum_core::config::paths::resolve_path;
use meerschaum_core::pyfmt::py_str;
use meerschaum_core::value::{Map, Value, apply_patch};

use crate::error::{ComposeError, ErrorKind, Messages};
use crate::lint::LintWarning;
use crate::model::PluginSpec;
use crate::pipes::{ProjectConfig, build_pipes, get_defined_pipes_meta};
use crate::project::{
    DEFAULT_COMPOSE_FILE_CANDIDATES, Host, LoadOptions, PipeKey, Project, load_project_in, posix,
    resolve_against,
};

/// One `include:` entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IncludeEntry {
    /// The path, directory or glob as written.
    pub path: String,
    /// A missing required path is an error; a missing optional one is a warning.
    pub required: bool,
}

/// Parse the `include:` key of a document. Entries that are not a string or `{path, required}`
/// come back as the second element, as messages.
pub fn include_entries(doc: &Map) -> (Vec<IncludeEntry>, Vec<String>) {
    let mut entries = Vec::new();
    let mut problems = Vec::new();
    let items: Vec<&Value> = match doc.get("include") {
        None | Some(Value::Null) => return (entries, problems),
        Some(Value::List(l)) => l.iter().collect(),
        Some(other) => vec![other],
    };
    for (i, item) in items.into_iter().enumerate() {
        match item {
            Value::Str(s) if !s.is_empty() => entries.push(IncludeEntry {
                path: s.clone(),
                required: true,
            }),
            Value::Map(m) => match m.get("path") {
                Some(Value::Str(p)) if !p.is_empty() => entries.push(IncludeEntry {
                    path: p.clone(),
                    required: m.get("required").is_none_or(Value::truthy),
                }),
                _ => problems.push(format!(
                    "include[{i}] needs a `path` (a string): {}",
                    py_str(item)
                )),
            },
            other => problems.push(format!(
                "include[{i}] must be a path or `{{path, required}}`, not {}",
                py_str(other)
            )),
        }
    }
    (entries, problems)
}

fn has_glob_chars(s: &str) -> bool {
    s.contains(['*', '?', '['])
}

fn is_compose_yaml(name: &str) -> bool {
    name.ends_with(".yaml") || name.ends_with(".yml")
}

/// The compose files in a directory: every `*.yaml` / `*.yml` file in it plus
/// `<dir>/*/mrsm-compose.yaml` (or `.yml`), each group sorted by name.
pub fn directory_files(dir: &Path) -> Vec<PathBuf> {
    let Ok(read) = std::fs::read_dir(dir) else {
        return Vec::new();
    };
    let mut files = Vec::new();
    let mut subdirs = Vec::new();
    let mut entries: Vec<_> = read.filter_map(Result::ok).collect();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') {
            continue;
        }
        let path = entry.path();
        if path.is_file() && is_compose_yaml(&name) {
            files.push(path);
        } else if path.is_dir() {
            for candidate in DEFAULT_COMPOSE_FILE_CANDIDATES {
                let nested = path.join(candidate);
                if nested.is_file() {
                    subdirs.push(nested);
                    break;
                }
            }
        }
    }
    files.extend(subdirs);
    files
}

/// What an include entry expanded to.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Expansion {
    /// The compose files, in order.
    pub files: Vec<PathBuf>,
    /// The path matched nothing (a missing path or an empty directory is reported by the
    /// caller; an empty glob is silent).
    pub missing: bool,
}

/// Expand an entry relative to the including file's directory.
pub fn expand_entry(base: &Path, entry: &IncludeEntry) -> Result<Expansion, ComposeError> {
    // A path that exists as written is a path, whatever characters it has (`clients/[acme]`).
    let literal = resolve_against(base, Path::new(&entry.path));
    let is_literal = has_glob_chars(&entry.path) && (literal.is_dir() || literal.is_file());
    if has_glob_chars(&entry.path) && !is_literal {
        // The directory the pattern is relative to is not a pattern: `proj[1]/` must not be read
        // as a character class.
        let pattern = if Path::new(&entry.path).is_absolute() {
            PathBuf::from(&entry.path)
        } else {
            Path::new(&glob::Pattern::escape(&base.to_string_lossy())).join(&entry.path)
        };
        let paths = glob::glob(&pattern.to_string_lossy()).map_err(|e| {
            ComposeError::value(format!("Invalid include pattern '{}': {e}", entry.path))
        })?;
        let mut files = Vec::new();
        for path in paths.filter_map(Result::ok) {
            if path.is_dir() {
                files.extend(directory_files(&path));
            } else if path.is_file() {
                files.push(path);
            }
        }
        return Ok(Expansion {
            files,
            missing: false,
        });
    }
    let path = resolve_against(base, Path::new(&entry.path));
    if path.is_dir() {
        return Ok(Expansion {
            files: directory_files(&path),
            missing: false,
        });
    }
    if path.is_file() {
        return Ok(Expansion {
            files: vec![path],
            missing: false,
        });
    }
    Ok(Expansion {
        files: Vec::new(),
        missing: true,
    })
}

fn canonical(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| resolve_path(path))
}

/// The compose file a `parent:` value names, relative to `from`'s directory: a compose file, or
/// a directory holding `mrsm-compose.yaml` / `.yml`.
pub fn parent_file(from: &Path, doc: &Map) -> Option<Result<PathBuf, String>> {
    let value = match doc.get("parent") {
        None | Some(Value::Null) => return None,
        Some(Value::Str(s)) if !s.is_empty() => s.clone(),
        Some(other) => {
            return Some(Err(format!(
                "`parent` must be a path to a compose file or directory, not {}",
                py_str(other)
            )));
        }
    };
    let base = from.parent().unwrap_or_else(|| Path::new("."));
    let path = resolve_against(base, Path::new(&value));
    if path.is_file() {
        return Some(Ok(path));
    }
    if path.is_dir() {
        for candidate in DEFAULT_COMPOSE_FILE_CANDIDATES {
            let file = path.join(candidate);
            if file.is_file() {
                return Some(Ok(file));
            }
        }
        return Some(Err(format!(
            "The parent directory '{}' has no mrsm-compose.yaml.",
            path.display()
        )));
    }
    Some(Err(format!(
        "The parent '{}' does not exist.",
        path.display()
    )))
}

/// The `parent:` of a compose file read straight from its text (a cheap peek before the file
/// is fully loaded). A file that cannot be read or parsed has no parent here: loading it
/// reports the error.
fn peek_document(file: &Path) -> Map {
    std::fs::read_to_string(file)
        .ok()
        .and_then(|text| meerschaum_core::yaml::load(&text).ok())
        .and_then(|v| match v {
            Value::Map(m) => Some(m),
            _ => None,
        })
        .unwrap_or_default()
}

/// The `parent:` chain of a compose file: `[topmost, ..., file]`. A cycle (or a parent that
/// cannot be found) warns and stops at the last valid level.
pub fn parent_chain(file: &Path, msgs: &mut Messages) -> Vec<PathBuf> {
    let mut chain = vec![canonical(file)];
    loop {
        let current = chain[0].clone();
        let doc = peek_document(&current);
        match parent_file(&current, &doc) {
            None => break,
            Some(Err(e)) => {
                msgs.warn(format!(
                    "{}: {e} Running the project on its own.",
                    current.display()
                ));
                break;
            }
            Some(Ok(parent)) => {
                let parent = canonical(&parent);
                if chain.contains(&parent) {
                    msgs.warn(format!(
                        "`parent:` cycle: '{}' names '{}', which is already above it. Stopping at '{}'.",
                        current.display(),
                        parent.display(),
                        current.display()
                    ));
                    break;
                }
                chain.insert(0, parent);
            }
        }
    }
    chain
}

/// A project in a [`Tree`].
#[derive(Debug, Clone)]
pub struct Node {
    /// The project, with the including projects' overrides applied.
    pub project: Project,
    /// The including project.
    pub parent: Option<usize>,
    /// The included projects, in the order listed.
    pub children: Vec<usize>,
}

/// A root compose file and the projects it includes.
#[derive(Debug, Clone, Default)]
pub struct Tree {
    /// The projects: the root first, then the included ones in load order.
    pub nodes: Vec<Node>,
    /// Findings recorded while applying overrides (what `explain` lists).
    pub findings: Vec<LintWarning>,
    /// Problems found while loading, already shown as warnings: an include that is missing,
    /// invalid or a cycle, and a file that fails to parse (skipped).
    pub warned: Vec<LintWarning>,
}

impl Tree {
    /// What makes the tree unsafe to run any job operation on, as one message: two projects with
    /// the same name (jobs and tags are owned by name, so each would treat the other's jobs as
    /// its own stale ones) and an included file that could not be loaded (its jobs would be
    /// left untouched while the run reports success). `explain` lists both as findings instead.
    pub fn blocking_problems(&self) -> Option<String> {
        let mut lines: Vec<String> = Vec::new();
        for f in &self.findings {
            if f.code == "duplicate-project-name" {
                lines.push(f.message.clone());
            }
        }
        for f in &self.warned {
            if f.code == "include-load-failed" {
                lines.push(format!(
                    "{} Its jobs were not touched; fix the file and run the command again.",
                    f.message
                ));
            }
        }
        if lines.is_empty() {
            return None;
        }
        Some(lines.join("\n"))
    }

    /// The root project.
    pub fn root(&self) -> &Node {
        &self.nodes[0]
    }

    /// Whether the tree has projects besides the root.
    pub fn is_multi(&self) -> bool {
        self.nodes.len() > 1
    }

    /// The projects in the order operations run: depth-first, the projects a project includes
    /// (in the order listed) before it, so the root is last.
    pub fn order(&self) -> Vec<usize> {
        fn visit(tree: &Tree, ix: usize, out: &mut Vec<usize>) {
            for child in &tree.nodes[ix].children {
                visit(tree, *child, out);
            }
            out.push(ix);
        }
        let mut out = Vec::new();
        visit(self, 0, &mut out);
        out
    }

    /// The projects above `ix`, nearest first.
    pub fn ancestors(&self, ix: usize) -> Vec<usize> {
        let mut out = Vec::new();
        let mut cur = self.nodes[ix].parent;
        while let Some(p) = cur {
            out.push(p);
            cur = self.nodes[p].parent;
        }
        out
    }

    /// The projects below `ix`.
    pub fn descendants(&self, ix: usize) -> Vec<usize> {
        let mut out = Vec::new();
        let mut stack: Vec<usize> = self.nodes[ix].children.iter().rev().copied().collect();
        while let Some(n) = stack.pop() {
            out.push(n);
            stack.extend(self.nodes[n].children.iter().rev().copied());
        }
        out
    }

    /// The index of the project with this name.
    pub fn by_name(&self, name: &str) -> Option<usize> {
        self.nodes
            .iter()
            .position(|n| n.project.project_name() == name)
    }

    /// The index of the project loaded from this file.
    pub fn by_file(&self, file: &Path) -> Option<usize> {
        let file = canonical(file);
        self.nodes
            .iter()
            .position(|n| canonical(&n.project.file) == file)
    }
}

/// How to load a tree.
#[derive(Debug, Clone, Default)]
pub struct TreeOptions<'a> {
    /// How the root file is located and read.
    pub load: LoadOptions<'a>,
    /// Compose files to attach under a given including file even when it does not list them
    /// (`parent:`): `(parent file, child file)`.
    pub attach: Vec<(PathBuf, PathBuf)>,
}

struct Loader<'a, 'b> {
    host: &'a Host,
    isolated: bool,
    nodes: Vec<Node>,
    loaded: HashMap<PathBuf, usize>,
    warned: Vec<LintWarning>,
    attach: &'b [(PathBuf, PathBuf)],
    msgs: &'b mut Messages,
}

impl Loader<'_, '_> {
    /// Show a load problem as a warning and keep it.
    fn finding(&mut self, finding: LintWarning, _warn: bool) {
        self.msgs.warn(finding.to_string());
        self.warned.push(finding);
    }

    /// Load the projects `parent_ix` includes (and theirs, depth-first).
    fn visit(&mut self, parent_ix: usize, stack: &mut Vec<PathBuf>) -> Result<(), ComposeError> {
        let parent_file = self.nodes[parent_ix].project.file.clone();
        let base = parent_file
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        let (entries, problems) = include_entries(&self.nodes[parent_ix].project.document);
        for problem in problems {
            self.finding(
                LintWarning::warning("include-invalid", "include", problem),
                true,
            );
        }
        let mut files: Vec<(PathBuf, bool)> = Vec::new();
        for entry in &entries {
            let expansion = expand_entry(&base, entry)?;
            if expansion.missing {
                let message = format!(
                    "The included path '{}' (in {}) does not exist.",
                    entry.path,
                    parent_file.display()
                );
                if entry.required {
                    return Err(ComposeError::new(ErrorKind::FileNotFound, message));
                }
                self.finding(
                    LintWarning::warning("include-missing", "include", message),
                    true,
                );
                continue;
            }
            files.extend(expansion.files.into_iter().map(|f| (f, false)));
        }
        let parent_key = canonical(&parent_file);
        for (from, child) in self.attach {
            if canonical(from) == parent_key {
                files.push((child.clone(), true));
            }
        }

        for (file, attached) in files {
            let key = canonical(&file);
            if stack.contains(&key) {
                self.finding(
                    LintWarning::warning(
                        "include-cycle",
                        "include",
                        format!(
                            "'{}' includes '{}', which is already being loaded above it; skipping that include.",
                            parent_file.display(),
                            key.display()
                        ),
                    ),
                    true,
                );
                continue;
            }
            if let Some(existing) = self.loaded.get(&key).copied() {
                // The same file reached twice loads once.
                if attached && !self.nodes[parent_ix].children.contains(&existing) {
                    self.nodes[parent_ix].children.push(existing);
                }
                continue;
            }
            if attached {
                self.finding(
                    LintWarning::warning(
                        "parent-not-including",
                        "parent",
                        format!(
                            "'{}' names '{}' as its parent, which does not include it; treating it as included.",
                            key.display(),
                            parent_file.display()
                        ),
                    ),
                    true,
                );
            }
            let (cwd, env, inherit_root) = {
                let parent = &self.nodes[parent_ix].project;
                (
                    key.parent().map(Path::to_path_buf).unwrap_or_default(),
                    parent.process_env.clone(),
                    parent.root_dir(),
                )
            };
            let loaded = load_project_in(
                self.host,
                &LoadOptions {
                    file: Some(&key),
                    env_file: None,
                    isolated: self.isolated,
                    cwd: Some(&cwd),
                    env: Some(&env),
                    inherit_root: Some(&inherit_root),
                },
                self.msgs,
            );
            let project = match loaded {
                Ok(p) => p,
                Err(e) => {
                    self.finding(
                        LintWarning::warning(
                            "include-load-failed",
                            posix(&key),
                            format!("Could not load the included file '{}': {e}", key.display()),
                        ),
                        true,
                    );
                    continue;
                }
            };
            let ix = self.nodes.len();
            self.nodes.push(Node {
                project,
                parent: Some(parent_ix),
                children: Vec::new(),
            });
            self.nodes[parent_ix].children.push(ix);
            self.loaded.insert(key.clone(), ix);
            stack.push(key);
            let result = self.visit(ix, stack);
            stack.pop();
            result?;
        }
        Ok(())
    }
}

/// Load the root compose file and every project it includes, then apply the overrides.
pub fn load_tree(
    host: &Host,
    opts: &TreeOptions<'_>,
    msgs: &mut Messages,
) -> Result<Tree, ComposeError> {
    let root = load_project_in(host, &opts.load, msgs)?;
    let root_key = canonical(&root.file);
    let mut loader = Loader {
        host,
        isolated: opts.load.isolated,
        nodes: vec![Node {
            project: root,
            parent: None,
            children: Vec::new(),
        }],
        loaded: HashMap::from([(root_key.clone(), 0)]),
        warned: Vec::new(),
        attach: &opts.attach,
        msgs,
    };
    loader.visit(0, &mut vec![root_key])?;
    let Loader { nodes, warned, .. } = loader;
    let mut tree = Tree {
        nodes,
        findings: Vec::new(),
        warned,
    };
    if tree.is_multi() {
        finalize(&mut tree, host);
    }
    Ok(tree)
}

fn plugin_name(spec: &str) -> String {
    PluginSpec::parse(spec).name
}

fn map_of<'a>(config: &'a Map, key: &str) -> Option<&'a Map> {
    config.get(key).and_then(Value::as_map)
}

/// The dotted paths both values set to different non-map values.
fn config_conflicts(sub: &Value, parent: &Value, path: &str, out: &mut Vec<String>) {
    match (sub, parent) {
        (Value::Map(a), Value::Map(b)) => {
            for (k, bv) in b {
                if path == "config" && k == "stack" {
                    continue;
                }
                if let Some(av) = a.get(k) {
                    config_conflicts(av, bv, &format!("{path}.{k}"), out);
                }
            }
        }
        (a, b) if a != b => out.push(path.to_string()),
        _ => {}
    }
}

/// The including project's `environment:` as it passes down to an included project: without
/// the root and plugins directories (the project inherits those through `root_dir` and
/// `plugins_dir`), the other path variables made absolute against the including project's
/// directory.
fn passed_down_environment(env: &Map, parent_file: &Path) -> Map {
    let statics = meerschaum_core::static_config::static_config();
    let name = |key: &str| -> Option<String> {
        statics
            .get(&["environment", key])
            .and_then(Value::as_str)
            .map(String::from)
    };
    let structural: Vec<String> = ["root", "plugins"].iter().filter_map(|k| name(k)).collect();
    let paths: Vec<String> = ["config_dir", "venvs", "work_dir"]
        .iter()
        .filter_map(|k| name(k))
        .collect();
    let base = parent_file.parent().unwrap_or_else(|| Path::new("."));
    let mut out = Map::new();
    for (key, value) in env {
        if structural.contains(key) {
            continue;
        }
        let value = match value {
            Value::Str(s) if paths.contains(key) && !s.is_empty() => {
                if s.trim_start().starts_with('[') {
                    match Value::from_json_str(s) {
                        Ok(Value::List(items)) => {
                            let abs: Vec<Value> = items
                                .iter()
                                .map(|i| {
                                    Value::Str(posix(&resolve_against(base, Path::new(&py_str(i)))))
                                })
                                .collect();
                            Value::Str(crate::project::json_compact(&Value::List(abs)))
                        }
                        _ => value.clone(),
                    }
                } else {
                    Value::Str(posix(&resolve_against(base, Path::new(s))))
                }
            }
            other => other.clone(),
        };
        out.insert(key.clone(), value);
    }
    out
}

/// Layer `parent` (already effective) over `sub`. Returns the notes.
fn overlay(sub: &mut Project, parent: &Project, notes: &mut Vec<LintWarning>) {
    let name = sub.project_name();
    let parent_name = parent.project_name();
    let location = |what: &str| format!("{name}: {what}");

    // `config`: deep-merged, the including project wins; the project keeps its own name.
    let sub_block = Value::Map(sub.config_block());
    let parent_block = Value::Map(parent.config_block());
    let merged = apply_patch(&sub_block, &parent_block);
    let mut merged = match merged {
        Value::Map(m) => m,
        _ => Map::new(),
    };
    let stack = merged
        .entry("stack".to_string())
        .or_insert_with(Value::new_map);
    if let Value::Map(stack) = stack {
        stack.insert("project_name".into(), Value::Str(name.clone()));
    }
    let mut conflicts = Vec::new();
    config_conflicts(&sub_block, &parent_block, "config", &mut conflicts);
    for path in conflicts.into_iter().take(20) {
        notes.push(LintWarning::info(
            "override-config",
            location(&path),
            format!("`{path}` is set by '{parent_name}', whose value wins."),
        ));
    }
    let connectors = merged
        .get("meerschaum")
        .and_then(Value::as_map)
        .and_then(|m| m.get("connectors"))
        .filter(|v| v.truthy())
        .cloned();
    sub.config.insert("config".into(), Value::Map(merged));
    if let Some(connectors) = connectors {
        sub.config.insert("connectors".into(), connectors);
    }

    // `instance` (copied to `config.instance`).
    if let Some(instance) = parent.config.get("instance").filter(|v| !v.is_null()) {
        sub.config.insert("instance".into(), instance.clone());
        if let Some(Value::Map(config)) = sub.config.get_mut("config") {
            config.insert("instance".into(), instance.clone());
        }
    }

    // `environment`: the including project's variables win. The directories it sets are the
    // including project's: the root and plugins directories pass down as the `root_dir` and
    // `plugins_dir` merges above, the other paths as absolute paths (a relative one would
    // resolve against this project's directory).
    if let Some(parent_env) = map_of(&parent.config, "environment")
        .map(|env| passed_down_environment(env, &parent.file))
        .filter(|m| !m.is_empty())
    {
        let parent_env = &parent_env;
        let sub_env = map_of(&sub.config, "environment")
            .cloned()
            .unwrap_or_default();
        let both: Vec<String> = parent_env
            .keys()
            .filter(|k| sub_env.contains_key(*k))
            .cloned()
            .collect();
        let merged_env = meerschaum_core::value::apply_patch_maps(&sub_env, parent_env);
        sub.config
            .insert("environment".into(), Value::Map(merged_env));
        if !both.is_empty() {
            notes.push(LintWarning::info(
                "override-environment",
                location("environment"),
                format!(
                    "The environment variable{} {} come{} from '{parent_name}'.",
                    if both.len() == 1 { "" } else { "s" },
                    both.join(", "),
                    if both.len() == 1 { "s" } else { "" },
                ),
            ));
        }
    }

    // `plugins`: matched by name, the including project's spec wins.
    if let (Some(Value::List(sub_plugins)), Some(Value::List(parent_plugins))) = (
        sub.config.get("plugins").cloned(),
        parent.config.get("plugins"),
    ) {
        let parent_by_name: HashMap<String, String> = parent_plugins
            .iter()
            .map(|p| {
                let s = py_str(p);
                (plugin_name(&s), s)
            })
            .collect();
        let mut out = Vec::new();
        for plugin in sub_plugins {
            let spec = py_str(&plugin);
            match parent_by_name.get(&plugin_name(&spec)) {
                Some(winner) if *winner != spec => {
                    notes.push(LintWarning::info(
                        "override-plugin",
                        location("plugins"),
                        format!(
                            "The plugin '{spec}' is replaced by '{winner}' from '{parent_name}'."
                        ),
                    ));
                    out.push(Value::Str(winner.clone()));
                }
                _ => out.push(plugin),
            }
        }
        sub.config.insert("plugins".into(), Value::List(out));
    }

    // `plugins_dir`: its own directories first, then the including project's.
    let mut dirs: Vec<Value> = match sub.config.get("plugins_dir") {
        Some(Value::List(l)) => l.clone(),
        _ => Vec::new(),
    };
    if let Some(Value::List(parent_dirs)) = parent.config.get("plugins_dir") {
        for dir in parent_dirs {
            if !dirs.contains(dir) {
                dirs.push(dir.clone());
            }
        }
    }
    sub.config.insert("plugins_dir".into(), Value::List(dirs));
}

/// Every project's jobs named `<descendant project>/<job>` are overrides of that descendant's
/// job: take them out of the project's own jobs.
fn take_job_overrides(tree: &mut Tree) -> HashMap<usize, Vec<(usize, String, Value)>> {
    let mut overrides: HashMap<usize, Vec<(usize, String, Value)>> = HashMap::new();
    for ix in 0..tree.nodes.len() {
        let descendants: Vec<(usize, String)> = tree
            .descendants(ix)
            .into_iter()
            .map(|d| (d, tree.nodes[d].project.project_name()))
            .collect();
        if descendants.is_empty() {
            continue;
        }
        let Some(Value::Map(jobs)) = tree.nodes[ix].project.config.get("jobs").cloned() else {
            continue;
        };
        let mut own = Map::new();
        for (key, command) in jobs {
            let target = key.split_once('/').and_then(|(project, job)| {
                descendants
                    .iter()
                    .find(|(_, n)| n == project)
                    .map(|(d, _)| (*d, job.to_string()))
            });
            match target {
                Some((d, job)) => overrides.entry(d).or_default().push((ix, job, command)),
                None => {
                    own.insert(key, command);
                }
            }
        }
        tree.nodes[ix]
            .project
            .config
            .insert("jobs".into(), Value::Map(own));
    }
    overrides
}

/// Apply the overrides top-down and compute the pipes each project must leave to its ancestors.
fn finalize(tree: &mut Tree, host: &Host) {
    let root_file = tree.nodes[0].project.file.clone();
    let root_name = tree.nodes[0].project.project_name();
    let overrides = take_job_overrides(tree);

    // Parents are loaded before their children, so index order is top-down.
    let mut findings = Vec::new();
    for ix in 0..tree.nodes.len() {
        tree.nodes[ix].project.compose_root = Some(root_file.clone());
        if ix > 0 {
            tree.nodes[ix].project.cache_suffix = Some(tree.nodes[ix].project.project_name());
            let parent_ix = tree.nodes[ix]
                .parent
                .expect("an included project has a parent");
            let parent = tree.nodes[parent_ix].project.clone();
            let mut sub = tree.nodes[ix].project.clone();
            overlay(&mut sub, &parent, &mut findings);
            // Job overrides: nearest including project first, the topmost last (it wins).
            if let Some(list) = overrides.get(&ix) {
                let mut list = list.clone();
                let ancestors = tree.ancestors(ix);
                list.sort_by_key(|(from, _, _)| ancestors.iter().position(|a| a == from));
                for (from, job, command) in list {
                    let from_name = tree.nodes[from].project.project_name();
                    let jobs = sub
                        .config
                        .entry("jobs".to_string())
                        .or_insert_with(Value::new_map);
                    if !jobs.is_map() {
                        *jobs = Value::new_map();
                    }
                    if let Value::Map(jobs) = jobs {
                        let existed = jobs.contains_key(&job);
                        jobs.insert(job.clone(), command);
                        findings.push(LintWarning::info(
                            "override-job",
                            format!("{}: jobs.{job}", sub.project_name()),
                            format!(
                                "The job '{job}' is {} by '{from_name}' (jobs.{}/{job}).",
                                if existed { "replaced" } else { "added" },
                                sub.project_name()
                            ),
                        ));
                    }
                }
            }
            tree.nodes[ix].project = sub;
        }
    }

    // Project names must be unique: jobs and tags are owned by name.
    let mut seen: HashMap<String, usize> = HashMap::new();
    for (ix, node) in tree.nodes.iter().enumerate() {
        let name = node.project.project_name();
        if let Some(first) = seen.get(&name) {
            findings.push(LintWarning::warning(
                "duplicate-project-name",
                name.clone(),
                format!(
                    "'{}' and '{}' are both named '{name}'; their pipes and jobs cannot be told apart. Set `project_name` in one of them.",
                    tree.nodes[*first].project.file.display(),
                    node.project.file.display()
                ),
            ));
        } else {
            seen.insert(name, ix);
        }
    }

    // Pipes: an ancestor's definition replaces the included one.
    let mut defined: HashMap<usize, Vec<(PipeKey, String)>> = HashMap::new();
    let mut scratch = Messages::default();
    for ix in 0..tree.nodes.len() {
        let parent_keys: Vec<(PipeKey, String)> = tree.nodes[ix]
            .parent
            .map(|p| {
                let mut keys: Vec<(PipeKey, String)> = tree.nodes[p]
                    .project
                    .excluded_pipes
                    .iter()
                    .map(|k| (k.clone(), String::new()))
                    .collect();
                keys.extend(defined.get(&p).cloned().unwrap_or_default());
                keys
            })
            .unwrap_or_default();
        let excluded: Vec<PipeKey> = parent_keys.iter().map(|(k, _)| k.clone()).collect();
        tree.nodes[ix].project.excluded_pipes = excluded;
        let project = tree.nodes[ix].project.clone();
        let name = project.project_name();
        let keys = defined_keys(host, &project, &mut scratch);
        for (key, label) in &keys {
            if let Some((_, owner)) = parent_keys.iter().find(|(k, _)| k == key) {
                let owner = if owner.is_empty() {
                    root_name.clone()
                } else {
                    owner.clone()
                };
                findings.push(LintWarning::info(
                    "override-pipe",
                    format!("{name}: {label}"),
                    format!(
                        "{label} is defined by '{owner}', which replaces this project's definition."
                    ),
                ));
            }
        }
        let own: Vec<(PipeKey, String)> = keys
            .into_iter()
            .filter(|(k, _)| !parent_keys.iter().any(|(pk, _)| pk == k))
            .map(|(k, _)| (k, name.clone()))
            .collect();
        let mut all = parent_keys;
        all.extend(own);
        defined.insert(ix, all);
    }
    tree.findings.extend(findings);
}

impl Tree {
    /// The keys of every pipe any project of the tree defines.
    pub fn defined_pipe_keys(&self, host: &Host) -> Vec<PipeKey> {
        let mut scratch = Messages::default();
        let mut keys = Vec::new();
        for node in &self.nodes {
            for (key, _) in defined_keys(host, &node.project, &mut scratch) {
                if !keys.contains(&key) {
                    keys.push(key);
                }
            }
        }
        keys
    }
}

/// The keys of the pipes a project defines (errors in the pipe definitions are reported when
/// the project runs).
fn defined_keys(host: &Host, project: &Project, msgs: &mut Messages) -> Vec<(PipeKey, String)> {
    let Ok(pc) = ProjectConfig::new(host, project) else {
        return Vec::new();
    };
    let Ok(metas) = get_defined_pipes_meta(&project.config, &pc, true, msgs) else {
        return Vec::new();
    };
    let Ok(pipes) = build_pipes(&metas, &pc.pipe_defaults()) else {
        return Vec::new();
    };
    pipes.iter().map(|p| (pipe_key(p), p.to_string())).collect()
}

/// The identifying keys of a pipe.
pub fn pipe_key(pipe: &meerschaum_core::pipe::Pipe) -> PipeKey {
    (
        pipe.connector_keys().to_string(),
        pipe.metric_key().to_string(),
        pipe.location_key().map(String::from),
        pipe.instance_keys().to_string(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(yaml: &str) -> Map {
        match meerschaum_core::yaml::load(yaml).unwrap() {
            Value::Map(m) => m,
            _ => Map::new(),
        }
    }

    #[test]
    fn include_entries_accept_strings_and_maps() {
        let (entries, problems) = include_entries(&doc(
            "include:\n  - a.yaml\n  - path: b/\n    required: false\n  - 3\n  - {required: true}\n",
        ));
        assert_eq!(
            entries,
            vec![
                IncludeEntry {
                    path: "a.yaml".into(),
                    required: true
                },
                IncludeEntry {
                    path: "b/".into(),
                    required: false
                }
            ]
        );
        assert_eq!(problems.len(), 2);
        let (entries, _) = include_entries(&doc("include: one.yaml\n"));
        assert_eq!(entries.len(), 1);
        assert!(include_entries(&doc("project_name: x\n")).0.is_empty());
    }

    #[test]
    fn directories_list_yaml_files_then_nested_compose_files() {
        let tmp = tempfile::tempdir().unwrap();
        let dir = tmp.path();
        std::fs::write(dir.join("b.yaml"), "").unwrap();
        std::fs::write(dir.join("a.yml"), "").unwrap();
        std::fs::write(dir.join("notes.txt"), "").unwrap();
        std::fs::create_dir(dir.join("sub")).unwrap();
        std::fs::write(dir.join("sub/mrsm-compose.yaml"), "").unwrap();
        std::fs::create_dir(dir.join("empty")).unwrap();
        let names: Vec<String> = directory_files(dir)
            .iter()
            .map(|p| {
                p.strip_prefix(dir)
                    .unwrap()
                    .to_string_lossy()
                    .replace('\\', "/")
            })
            .collect();
        assert_eq!(names, vec!["a.yml", "b.yaml", "sub/mrsm-compose.yaml"]);
    }

    #[test]
    fn missing_paths_are_flagged_and_empty_globs_are_not() {
        let tmp = tempfile::tempdir().unwrap();
        let entry = |p: &str| IncludeEntry {
            path: p.into(),
            required: true,
        };
        assert!(
            expand_entry(tmp.path(), &entry("nope.yaml"))
                .unwrap()
                .missing
        );
        let globbed = expand_entry(tmp.path(), &entry("x/*.yaml")).unwrap();
        assert!(!globbed.missing && globbed.files.is_empty());
    }
}
