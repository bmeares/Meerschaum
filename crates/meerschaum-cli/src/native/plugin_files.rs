//! Plugin sources on disk: discovery by v4's rule, `required` read from source text, archive
//! extraction, `.mrsm_origins.json`, and the rollback copies `Plugin.install` keeps.
//!
//! # Discovery (`get_plugins_names`)
//!
//! v4 lists the entries of the symlink farm `<root>/.internal/plugins` (every non-hidden entry of
//! every plugins directory except `__pycache__` and `__init__.py`) and keeps the ones
//! `Plugin.is_installed()` accepts, which is a file-system test: a directory with an
//! `__init__.py`, or a `<name>.py` file. A plugin is **never imported** to decide: a plugin
//! whose import fails is listed (released v4.1.4: `show plugins` prints it, `setup plugins`
//! counts it, and the import error is only warned about when something imports it), and a
//! directory without `__init__.py` is not. The first plugins directory providing a name wins.

use std::collections::BTreeMap;
use std::io::Read;
use std::path::{Path, PathBuf};

use serde_json::Value as Json;

/// The source of an installed plugin: `dir/name/__init__.py` or `dir/name.py`.
pub fn plugin_file(dirs: &[PathBuf], name: &str) -> Option<PathBuf> {
    for dir in dirs {
        let package = dir.join(name).join("__init__.py");
        if package.is_file() {
            return Some(package);
        }
        let module = dir.join(format!("{name}.py"));
        if module.is_file() {
            return Some(module);
        }
    }
    None
}

/// Whether `dir/entry` is a native (C-ABI) plugin: a directory with a `plugin.toml`.
pub fn is_native_plugin_dir(path: &Path) -> bool {
    path.is_dir() && path.join("plugin.toml").is_file()
}

/// Native source or library directory, without compiling or loading it.
pub fn native_plugin_path(dirs: &[PathBuf], name: &str) -> Option<PathBuf> {
    dirs.iter().find_map(|dir| {
        let package = dir.join(name);
        if is_native_plugin_dir(&package) {
            return Some(package);
        }
        let file = dir.join(format!("{name}.rs"));
        file.is_file().then_some(file)
    })
}

/// Editable native entry point. A binary-only package has no source to edit.
pub fn native_source(path: &Path) -> Option<PathBuf> {
    if path.is_file() {
        return Some(path.to_path_buf());
    }
    let manifest = std::fs::read_to_string(path.join("Cargo.toml")).ok()?;
    let cargo: toml::Value = toml::from_str(&manifest).ok()?;
    let relative = cargo
        .get("lib")
        .and_then(|lib| lib.get("path"))
        .and_then(toml::Value::as_str)
        .unwrap_or("src/lib.rs");
    let source = path.join(relative);
    source.is_file().then_some(source)
}

/// Native descriptors carry versions without importing the plugin; single files are unversioned.
pub fn native_version(path: &Path) -> Option<String> {
    let text = std::fs::read_to_string(path.join("plugin.toml")).ok()?;
    let descriptor: toml::Value = toml::from_str(&text).ok()?;
    descriptor.get("version")?.as_str().map(str::to_owned)
}

/// The installed Python plugin names (v4's rule, see the module docs), in directory order with
/// each directory's entries sorted by name (v4 lists the farm in file-system order).
pub fn python_plugin_names(dirs: &[PathBuf]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        let mut items: Vec<String> = entries
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        items.sort();
        for item in items {
            if item.starts_with('.') || item == "__pycache__" || item == "__init__.py" {
                continue;
            }
            let path = dir.join(&item);
            let name = if path.is_dir() {
                item.clone()
            } else if let Some(stem) = item.strip_suffix(".py") {
                stem.to_string()
            } else {
                continue;
            };
            if !names.contains(&name) && plugin_file(dirs, &name).is_some() {
                names.push(name);
            }
        }
    }
    names
}

/// Python plugin names plus native plugin directories, native names shadowing Python ones
/// (a name appears once, at the position its first directory gives it).
pub fn installed_plugin_names(dirs: &[PathBuf]) -> Vec<String> {
    let mut names: Vec<String> = Vec::new();
    for dir in dirs {
        let Ok(entries) = std::fs::read_dir(dir) else {
            continue;
        };
        let mut items: Vec<String> = entries
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        items.sort();
        for item in items {
            if item.starts_with('.') || item == "__pycache__" || item == "__init__.py" {
                continue;
            }
            let path = dir.join(&item);
            // A native directory and a Python package directory both name the plugin after it.
            let name = if path.is_dir() {
                item.clone()
            } else if let Some(stem) = item
                .strip_suffix(".py")
                .or_else(|| item.strip_suffix(".rs"))
            {
                stem.to_string()
            } else {
                continue;
            };
            if names.contains(&name) {
                continue;
            }
            if native_plugin_path(dirs, &name).is_some() || plugin_file(dirs, &name).is_some() {
                names.push(name);
            }
        }
    }
    names
}

/// A plugin name every action may turn into a path (`<plugins>/<name>.py`, `venvs/<name>`,
/// `.archives/<name>.tar.gz`): not empty, no path separators or NUL, no `..`, no leading `.`,
/// and not `mrsm` (the shared core virtual environment). One check for every action; v4 trusts
/// the name and `uninstall plugins ../.. --force` would remove a directory outside `venvs/`.
pub fn validate_plugin_name(name: &str) -> Result<(), String> {
    let invalid = name.is_empty()
        || name.contains(['/', '\\', '\0'])
        || name.contains("..")
        || name.starts_with('.')
        || name == "mrsm";
    if invalid {
        return Err(format!("Invalid plugin name: '{name}'"));
    }
    Ok(())
}

/// `Plugin.__init__`'s split of `name@repo` on the repository separator: `(name, repo)`.
/// More than one separator is v4's `error(f"Invalid plugin name: '{name}'")`; so is a name
/// [`validate_plugin_name`] refuses.
pub fn split_repo(name: &str, separator: &str) -> Result<(String, Option<String>), String> {
    let (plugin, repository) = if !name.contains(separator) {
        (name.to_string(), None)
    } else {
        let parts: Vec<&str> = name.split(separator).collect();
        if parts.len() != 2 {
            return Err(format!("Invalid plugin name: '{name}'"));
        }
        (parts[0].to_string(), Some(parts[1].to_string()))
    };
    validate_plugin_name(&plugin).map_err(|_| format!("Invalid plugin name: '{name}'"))?;
    Ok((plugin, repository))
}

/// The `required` list declared in a plugin's source text, for plugins whose import fails (the
/// manifest has nothing for them).
///
/// v4's `Plugin.get_dependencies()` reads the source text this way for every action that does
/// not import the plugin. It finds `\nrequired[: annotation] = [` and reads the bracketed list
/// with `ast.literal_eval`. PARITY (deliberate repair): v4 also deletes the first `=` inside
/// the list text (`'pandas>=2.0'` becomes `'pandas>2.0'`; `Plugin.get_dependencies`
/// `.replace('=', '', 1)`) and ends the list at the first `]` even inside a string or comment.
/// Here the list is delimited by a string- and comment-aware bracket match and evaluated as
/// written.
pub fn static_required(text: &str) -> Result<Vec<String>, String> {
    if !text.contains("required") {
        return Ok(Vec::new());
    }
    let re = regex::Regex::new(r"\nrequired(:\s*)?.*=").expect("required pattern");
    let Some(found) = re.find(text) else {
        return Ok(Vec::new());
    };
    // v4 reads from the first `=` after the match start (the pattern's own `.*=` is greedy).
    let Some(equals) = text[found.start()..].find('=') else {
        return Ok(Vec::new());
    };
    let after = &text[found.start() + equals + 1..];
    let Some(open) = after.find('[') else {
        return Ok(Vec::new());
    };
    let list = &after[open..];
    let Some(end) = matching_bracket(list) else {
        return Err("'[' was never closed".to_string());
    };
    match crate::pyliteral::literal_eval(&list[..=end]) {
        Ok(crate::value::PyValue::List(items)) => {
            Ok(items.iter().map(crate::value::PyValue::py_str).collect())
        }
        Ok(_) | Err(_) => Err("malformed node or string".to_string()),
    }
}

/// The index of the `]` closing the `[` that starts `text`, skipping strings and comments.
fn matching_bracket(text: &str) -> Option<usize> {
    let bytes = text.as_bytes();
    let mut depth = 0usize;
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'[' => depth += 1,
            b']' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    return Some(i);
                }
            }
            b'#' => {
                while i < bytes.len() && bytes[i] != b'\n' {
                    i += 1;
                }
                continue;
            }
            quote @ (b'\'' | b'"') => {
                let triple = bytes[i..].starts_with(&[quote, quote, quote]);
                i += if triple { 3 } else { 1 };
                while i < bytes.len() {
                    if bytes[i] == b'\\' {
                        i += 2;
                        continue;
                    }
                    if triple {
                        if bytes[i..].starts_with(&[quote, quote, quote]) {
                            i += 3;
                            break;
                        }
                    } else if bytes[i] == quote {
                        i += 1;
                        break;
                    } else if bytes[i] == b'\n' {
                        break;
                    }
                    i += 1;
                }
                continue;
            }
            _ => {}
        }
        i += 1;
    }
    None
}

/// `__version__ = '...'` from a plugin's source (`Plugin._install`'s scan): the first line
/// containing `__version__` followed by `=` (anywhere in the line) gives
/// `literal_eval(line.split('=')[1])`. `Err` is the exception `literal_eval` would raise.
pub fn declared_version(text: &str) -> Result<Option<String>, String> {
    let re = regex::Regex::new(r"__version__(\s?)=").expect("version pattern");
    for line in text.lines() {
        if !line.contains("__version__") {
            continue;
        }
        if !re.is_match(line.trim()) {
            continue;
        }
        let value = line.split('=').nth(1).unwrap_or_default().trim();
        return match crate::pyliteral::literal_eval(value) {
            Ok(crate::value::PyValue::Str(s)) => Ok(Some(s)),
            Ok(crate::value::PyValue::None) => Ok(None),
            Ok(other) => Ok(Some(other.py_str())),
            Err(_) => Err("malformed node or string".to_string()),
        };
    }
    Ok(None)
}

/// The largest regular member `extract_archive` writes (a plugin is source code).
pub const MAX_MEMBER_BYTES: u64 = 256 * 1024 * 1024;

/// `safely_extract_tar(tarf, output_dir)` for a `.tar.gz`, streaming members to disk.
///
/// Every member must stay inside `output_dir` (v4 compares the absolute paths and raises
/// `Attempted Path Traversal in Tar File`; here by path components, see below). Deviations from v4,
/// which unpacks links: symbolic and hard links are refused outright (a link can point a later
/// member outside the directory, and a lexical check cannot see that), the canonical parent of
/// every destination is checked against the canonical `output_dir` on disk before each write, and
/// a regular member larger than [`MAX_MEMBER_BYTES`] is refused. `output_dir` should be empty.
pub fn extract_archive(archive: &[u8], output_dir: &Path) -> Result<(), String> {
    const TRAVERSAL: &str = "Attempted Path Traversal in Tar File";
    let decoder = flate2::read::GzDecoder::new(archive);
    let mut tar = tar::Archive::new(decoder);
    std::fs::create_dir_all(output_dir).map_err(|e| e.to_string())?;
    let canonical_root = output_dir.canonicalize().map_err(|e| e.to_string())?;
    let abs_directory = absolute_lexical(output_dir);
    let entries = tar.entries().map_err(|e| e.to_string())?;
    for entry in entries {
        let mut entry = entry.map_err(|e| e.to_string())?;
        let name = entry.path().map_err(|e| e.to_string())?.into_owned();
        let destination = output_dir.join(&name);
        // Deviation from v4: `Path::starts_with` compares components. v4 compares the strings, so
        // a member such as `../<name>.txt` next to the directory `<name>` passes its check
        // (`safely_extract_tar`: `abs_target.startswith(abs_directory)`).
        if !absolute_lexical(&destination).starts_with(&abs_directory) {
            return Err(TRAVERSAL.into());
        }
        let entry_type = entry.header().entry_type();
        if entry_type.is_symlink() || entry_type.is_hard_link() {
            return Err(format!(
                "Links are not allowed in plugin archives: '{}'",
                name.display()
            ));
        }
        if !(entry_type.is_dir() || entry_type.is_file()) {
            // Pax headers, devices and other special members carry nothing a plugin needs.
            continue;
        }
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
            let canonical_parent = parent.canonicalize().map_err(|e| e.to_string())?;
            if !canonical_parent.starts_with(&canonical_root) {
                return Err(TRAVERSAL.into());
            }
        }
        if entry_type.is_dir() {
            std::fs::create_dir_all(&destination).map_err(|e| e.to_string())?;
            continue;
        }
        let mode = entry.header().mode().unwrap_or(0o644);
        let mtime = entry.header().mtime().ok();
        let mut file = std::fs::File::create(&destination).map_err(|e| e.to_string())?;
        let copied = std::io::copy(&mut (&mut entry).take(MAX_MEMBER_BYTES + 1), &mut file)
            .map_err(|e| e.to_string())?;
        if copied > MAX_MEMBER_BYTES {
            drop(file);
            let _ = std::fs::remove_file(&destination);
            return Err(format!(
                "Archive member '{}' is larger than {} MiB",
                name.display(),
                MAX_MEMBER_BYTES / (1024 * 1024)
            ));
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = file.set_permissions(std::fs::Permissions::from_mode(mode & 0o755 | 0o600));
        }
        #[cfg(not(unix))]
        let _ = mode;
        if let Some(mtime) = mtime {
            let _ =
                file.set_modified(std::time::UNIX_EPOCH + std::time::Duration::from_secs(mtime));
        }
    }
    Ok(())
}

/// `os.path.abspath`: make absolute and collapse `.` and `..` lexically.
fn absolute_lexical(path: &Path) -> PathBuf {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("/"))
            .join(path)
    };
    let mut out = PathBuf::new();
    for component in absolute.components() {
        match component {
            std::path::Component::ParentDir => {
                out.pop();
            }
            std::path::Component::CurDir => {}
            other => out.push(other.as_os_str()),
        }
    }
    out
}

/// `plugins/_origins.py`: the `.mrsm_origins.json` of one plugins directory.
pub fn read_origins(dir: &Path, filename: &str) -> BTreeMap<String, String> {
    let Ok(text) = std::fs::read_to_string(dir.join(filename)) else {
        return BTreeMap::new();
    };
    let Ok(Json::Object(map)) = serde_json::from_str::<Json>(&text) else {
        return BTreeMap::new();
    };
    map.into_iter()
        .filter_map(|(name, keys)| {
            keys.as_str()
                .filter(|k| !k.is_empty())
                .map(|k| (name, k.to_string()))
        })
        .collect()
}

/// `write_plugin_origin(name, repo_keys, dir)`: `json.dump(origins, indent=4, sort_keys=True)`.
pub fn write_origin(dir: &Path, filename: &str, name: &str, repo_keys: &str) -> bool {
    if repo_keys.is_empty() {
        return false;
    }
    let mut origins = read_origins(dir, filename);
    if origins.get(name).is_some_and(|k| k == repo_keys) {
        return true;
    }
    origins.insert(name.to_string(), repo_keys.to_string());
    let value: Json = origins
        .into_iter()
        .map(|(k, v)| (k, Json::String(v)))
        .collect::<serde_json::Map<String, Json>>()
        .into();
    let text = super::pip::sorted_pretty(&value, 4);
    std::fs::create_dir_all(dir)
        .and_then(|()| std::fs::write(dir.join(filename), text))
        .is_ok()
}

/// `remove_plugin_origin(name, dir)`: drop a plugin's entry from one plugins directory's
/// `.mrsm_origins.json` (a directory that does not name it is left alone).
pub fn remove_origin(dir: &Path, filename: &str, name: &str) -> bool {
    let mut origins = read_origins(dir, filename);
    if origins.remove(name).is_none() {
        return true;
    }
    let value: Json = origins
        .into_iter()
        .map(|(k, v)| (k, Json::String(v)))
        .collect::<serde_json::Map<String, Json>>()
        .into();
    let text = super::pip::sorted_pretty(&value, 4);
    std::fs::write(dir.join(filename), text).is_ok()
}

/// `_copy_path`: copy a file, directory or symlink without following symlinks.
pub fn copy_path(source: &Path, destination: &Path) -> std::io::Result<()> {
    let meta = std::fs::symlink_metadata(source)?;
    if meta.file_type().is_symlink() {
        #[cfg(unix)]
        {
            let target = std::fs::read_link(source)?;
            return std::os::unix::fs::symlink(target, destination);
        }
        #[cfg(not(unix))]
        {
            return std::fs::copy(source, destination).map(|_| ());
        }
    }
    if meta.is_dir() {
        std::fs::create_dir_all(destination)?;
        for entry in std::fs::read_dir(source)? {
            let entry = entry?;
            copy_path(&entry.path(), &destination.join(entry.file_name()))?;
        }
        return Ok(());
    }
    std::fs::copy(source, destination).map(|_| ())
}

/// `_remove_path`: remove a file, directory or symlink if it exists.
pub fn remove_path(path: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() || meta.is_file() => std::fs::remove_file(path),
        Ok(meta) if meta.is_dir() => std::fs::remove_dir_all(path),
        _ => Ok(()),
    }
}

/// The copies `Plugin._install_with_rollback` makes of the plugin's sources and venv before it
/// replaces them, and puts them back when the installation fails.
pub struct Rollback {
    tracked: Vec<PathBuf>,
    directory: PathBuf,
    backups: Vec<(PathBuf, PathBuf)>,
    prepared: bool,
}

impl Rollback {
    /// Track `tracked` paths, keeping copies under a fresh directory in `temp_root`.
    pub fn new(name: &str, temp_root: &Path, tracked: Vec<PathBuf>) -> std::io::Result<Rollback> {
        std::fs::create_dir_all(temp_root)?;
        let directory = temp_root.join(format!(
            ".{name}-rollback-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&directory)?;
        Ok(Rollback {
            tracked,
            directory,
            backups: Vec::new(),
            prepared: false,
        })
    }

    /// `prepare_rollback`: copy every tracked path that exists.
    pub fn prepare(&mut self) -> std::io::Result<()> {
        for (index, path) in self.tracked.iter().enumerate() {
            if std::fs::symlink_metadata(path).is_err() {
                continue;
            }
            let backup = self.directory.join(index.to_string());
            copy_path(path, &backup)?;
            self.backups.push((path.clone(), backup));
        }
        self.prepared = true;
        Ok(())
    }

    /// `rollback`: remove the tracked paths and restore the copies (a no-op before `prepare`).
    pub fn restore(&mut self) -> std::io::Result<()> {
        if !self.prepared {
            return Ok(());
        }
        for path in &self.tracked {
            remove_path(path)?;
        }
        for (path, backup) in std::mem::take(&mut self.backups) {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            if std::fs::rename(&backup, &path).is_err() {
                copy_path(&backup, &path)?;
                remove_path(&backup)?;
            }
        }
        Ok(())
    }
}

impl Drop for Rollback {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.directory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_edit_source_respects_cargo_library_path() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            &root.join("plugin.toml"),
            "name = 'demo'\nversion = '1.0.0'\n",
        );
        assert_eq!(native_source(root), None);
        write(&root.join("Cargo.toml"), "[package]\nname = 'demo'\n");
        write(&root.join("src/lib.rs"), "");
        assert_eq!(native_source(root), Some(root.join("src/lib.rs")));
        write(&root.join("Cargo.toml"), "[lib]\npath = 'custom.rs'\n");
        assert_eq!(native_source(root), None);
        write(&root.join("custom.rs"), "");
        assert_eq!(native_source(root), Some(root.join("custom.rs")));
        assert_eq!(
            native_source(&root.join("custom.rs")),
            Some(root.join("custom.rs"))
        );
    }

    fn write(path: &Path, text: &str) {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    #[test]
    fn discovery_follows_v4s_file_system_rule() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(&root.join("ok.py"), "");
        write(&root.join("pkg/__init__.py"), "");
        write(&root.join("no_init/mod.py"), "");
        write(&root.join("_private.py"), "");
        write(&root.join(".hidden.py"), "");
        write(&root.join("README.md"), "");
        write(&root.join("__init__.py"), "");
        write(&root.join("__pycache__/x.pyc"), "");
        write(
            &root.join("broken.py"),
            "import definitely_not_a_module_xyz\n",
        );
        let dirs = vec![root.to_path_buf()];
        // A failing import does not hide a plugin; a package without `__init__.py` is not one.
        assert_eq!(
            python_plugin_names(&dirs),
            vec!["_private", "broken", "ok", "pkg"]
        );
    }

    #[test]
    fn the_first_directory_wins_and_native_directories_count() {
        let a = tempfile::tempdir().unwrap();
        let b = tempfile::tempdir().unwrap();
        write(&a.path().join("same.py"), "a = 1\n");
        write(&b.path().join("same.py"), "b = 1\n");
        write(&b.path().join("only_b.py"), "");
        write(
            &b.path().join("native_one/plugin.toml"),
            "name = 'native_one'\n",
        );
        let dirs = vec![a.path().to_path_buf(), b.path().to_path_buf()];
        assert_eq!(plugin_file(&dirs, "same"), Some(a.path().join("same.py")));
        assert_eq!(python_plugin_names(&dirs), vec!["same", "only_b"]);
        assert_eq!(
            installed_plugin_names(&dirs),
            vec!["same", "native_one", "only_b"]
        );
    }

    #[test]
    fn repository_separator() {
        assert_eq!(split_repo("noaa", "@").unwrap(), ("noaa".into(), None));
        assert_eq!(
            split_repo("noaa@api:x", "@").unwrap(),
            ("noaa".into(), Some("api:x".into()))
        );
        assert_eq!(
            split_repo("a@b@c", "@").unwrap_err(),
            "Invalid plugin name: 'a@b@c'"
        );
    }

    #[test]
    fn plugin_names_cannot_name_a_path() {
        for bad in [
            "",
            "..",
            "../x",
            "a/b",
            "a\\b",
            ".hidden",
            "a..b",
            "mrsm",
            "a\0b",
            "/abs",
            "../..@api:x",
        ] {
            assert!(split_repo(bad, "@").is_err(), "{bad:?}");
        }
        for good in ["noaa", "my-plugin", "w7_pyfig", "a.b", "plugin@api:mrsm"] {
            assert!(split_repo(good, "@").is_ok(), "{good:?}");
        }
    }

    #[test]
    fn static_required_keeps_specifiers_intact() {
        let text =
            "__version__ = '1'\n\nrequired = ['pandas>=2.0', 'x==1', 'plugin:foo@api:bar']\n";
        assert_eq!(
            static_required(text).unwrap(),
            vec!["pandas>=2.0", "x==1", "plugin:foo@api:bar"]
        );
        let annotated = "import os\nrequired: list = [\n    'a==1',   # a comment ]\n    'b[extra]',\n]\ndef setup(): pass\n";
        assert_eq!(
            static_required(annotated).unwrap(),
            vec!["a==1", "b[extra]"]
        );
        assert!(static_required("x = 1\n").unwrap().is_empty());
        assert!(static_required("\nrequired = []\n").unwrap().is_empty());
        assert!(static_required("\nrequired = ['a'\n").is_err());
    }

    #[test]
    fn declared_versions() {
        assert_eq!(
            declared_version("import os\n__version__ = '1.2.3'\n").unwrap(),
            Some("1.2.3".into())
        );
        assert_eq!(
            declared_version("__version__='2.0'\n").unwrap(),
            Some("2.0".into())
        );
        // A comment line mentioning the name does not count (no `=`).
        assert_eq!(
            declared_version("# __version__ is below\nx = 1\n").unwrap(),
            None
        );
        assert_eq!(declared_version("x = 1\n").unwrap(), None);
        assert!(declared_version("__version__ = oops\n").is_err());
    }

    fn gzip_bytes(bytes: &[u8]) -> Vec<u8> {
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut gz, bytes).unwrap();
        gz.finish().unwrap()
    }

    fn tarball(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut builder = tar::Builder::new(Vec::new());
        for (name, data) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(data.len() as u64);
            header.set_mode(0o644);
            header.set_mtime(1_700_000_000);
            header.set_cksum();
            builder.append_data(&mut header, name, *data).unwrap();
        }
        let tar_bytes = builder.into_inner().unwrap();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut gz, &tar_bytes).unwrap();
        gz.finish().unwrap()
    }

    #[test]
    fn archives_extract_inside_the_directory_only() {
        let dir = tempfile::tempdir().unwrap();
        let good = tarball(&[
            ("plug/__init__.py", b"x = 1\n"),
            ("plug/sub/data.txt", b"d"),
        ]);
        extract_archive(&good, dir.path()).unwrap();
        assert_eq!(
            std::fs::read_to_string(dir.path().join("plug/__init__.py")).unwrap(),
            "x = 1\n"
        );
        assert!(dir.path().join("plug/sub/data.txt").is_file());
        // The archive's own modification time is kept.
        let modified = std::fs::metadata(dir.path().join("plug/__init__.py"))
            .unwrap()
            .modified()
            .unwrap();
        assert_eq!(
            modified
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            1_700_000_000
        );
        let evil = {
            // `append_data` refuses `..`, so write the name into the header directly.
            let mut header = tar::Header::new_gnu();
            header.as_old_mut().name[..11].copy_from_slice(b"../evil.txt");
            header.set_size(1);
            header.set_mode(0o644);
            header.set_cksum();
            let mut builder = tar::Builder::new(Vec::new());
            builder.append(&header, &b"x"[..]).unwrap();
            let tar_bytes = builder.into_inner().unwrap();
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            std::io::Write::write_all(&mut gz, &tar_bytes).unwrap();
            gz.finish().unwrap()
        };
        let target = tempfile::tempdir().unwrap();
        let error = extract_archive(&evil, target.path()).unwrap_err();
        assert_eq!(error, "Attempted Path Traversal in Tar File");
        assert!(!target.path().parent().unwrap().join("evil.txt").exists());
        assert!(extract_archive(b"not a tarball", target.path()).is_err());
        // A sibling whose name starts with the directory's name is outside it too (v4's string
        // comparison lets `../plug.txt` out of `plug`).
        let sibling = {
            let mut header = tar::Header::new_gnu();
            header.as_old_mut().name[..11].copy_from_slice(b"../plug.txt");
            header.set_size(1);
            header.set_mode(0o644);
            header.set_cksum();
            let mut builder = tar::Builder::new(Vec::new());
            builder.append(&header, &b"x"[..]).unwrap();
            gzip_bytes(&builder.into_inner().unwrap())
        };
        let parent = tempfile::tempdir().unwrap();
        std::fs::create_dir(parent.path().join("plug")).unwrap();
        assert_eq!(
            extract_archive(&sibling, &parent.path().join("plug")).unwrap_err(),
            "Attempted Path Traversal in Tar File"
        );
        assert!(!parent.path().join("plug.txt").exists());
    }

    fn raw_member(name: &str, kind: tar::EntryType, link: Option<&str>, data: &[u8]) -> Vec<u8> {
        let mut header = tar::Header::new_gnu();
        header.set_entry_type(kind);
        header.set_path(name).unwrap();
        if let Some(link) = link {
            header.set_link_name(link).unwrap();
        }
        header.set_size(data.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        let mut out = header.as_bytes().to_vec();
        out.extend_from_slice(data);
        out.resize(out.len().div_ceil(512) * 512, 0);
        out
    }

    #[test]
    fn links_are_refused_and_nothing_is_written_outside() {
        // `x -> .`, `t -> x/..` (the parent of the output directory on disk), then `t/escaped.txt`.
        let mut tar_bytes = Vec::new();
        tar_bytes.extend(raw_member("x", tar::EntryType::Symlink, Some("."), b""));
        tar_bytes.extend(raw_member("t", tar::EntryType::Symlink, Some("x/.."), b""));
        tar_bytes.extend(raw_member(
            "t/escaped.txt",
            tar::EntryType::Regular,
            None,
            b"owned",
        ));
        tar_bytes.extend([0u8; 1024]);
        let archive = gzip_bytes(&tar_bytes);
        let parent = tempfile::tempdir().unwrap();
        let out = parent.path().join("out");
        let error = extract_archive(&archive, &out).unwrap_err();
        assert!(error.starts_with("Links are not allowed"), "{error}");
        assert!(!parent.path().join("escaped.txt").exists());
        assert!(!out.join("t").exists());
        // Hard links are refused too.
        let mut tar_bytes = raw_member("h", tar::EntryType::Link, Some("/etc/passwd"), b"");
        tar_bytes.extend([0u8; 1024]);
        let error = extract_archive(&gzip_bytes(&tar_bytes), &out).unwrap_err();
        assert!(error.starts_with("Links are not allowed"), "{error}");
    }

    #[test]
    fn origins_are_written_like_python_json() {
        let dir = tempfile::tempdir().unwrap();
        assert!(write_origin(
            dir.path(),
            ".mrsm_origins.json",
            "zeta",
            "api:a"
        ));
        assert!(write_origin(
            dir.path(),
            ".mrsm_origins.json",
            "alpha",
            "api:b"
        ));
        assert_eq!(
            std::fs::read_to_string(dir.path().join(".mrsm_origins.json")).unwrap(),
            "{\n    \"alpha\": \"api:b\",\n    \"zeta\": \"api:a\"\n}"
        );
        assert!(!write_origin(dir.path(), ".mrsm_origins.json", "x", ""));
        assert_eq!(read_origins(dir.path(), ".mrsm_origins.json").len(), 2);
    }

    #[test]
    fn rollback_restores_what_was_replaced() {
        let dir = tempfile::tempdir().unwrap();
        let plugins = dir.path().join("plugins");
        let venv = dir.path().join("venvs/plug");
        write(&plugins.join("plug.py"), "old\n");
        write(&venv.join("lib/marker"), "m");
        let mut rollback = Rollback::new(
            "plug",
            &dir.path().join("tmp"),
            vec![plugins.join("plug"), plugins.join("plug.py"), venv.clone()],
        )
        .unwrap();
        // Nothing prepared: a restore is a no-op.
        std::fs::write(plugins.join("plug.py"), "changed\n").unwrap();
        rollback.restore().unwrap();
        assert_eq!(
            std::fs::read_to_string(plugins.join("plug.py")).unwrap(),
            "changed\n"
        );
        std::fs::write(plugins.join("plug.py"), "old\n").unwrap();
        rollback.prepare().unwrap();
        std::fs::write(plugins.join("plug.py"), "new\n").unwrap();
        write(&plugins.join("plug/__init__.py"), "package\n");
        std::fs::remove_dir_all(&venv).unwrap();
        rollback.restore().unwrap();
        assert_eq!(
            std::fs::read_to_string(plugins.join("plug.py")).unwrap(),
            "old\n"
        );
        assert!(!plugins.join("plug").exists());
        assert!(venv.join("lib/marker").is_file());
    }
}
