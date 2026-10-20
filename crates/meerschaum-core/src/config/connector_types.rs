//! Which `MRSM_<TYPE>_<LABEL>` environment variables are connectors.
//!
//! v4 read every variable that matched the URI regex, so `MRSM_DUCKDB_LIB` became a `duckdb:lib`
//! connector. v5 only reads the variables of a connector type that exists: the built-in types, plus
//! the types plugins register with `@make_connector` (read from the cached plugin manifests, or
//! from the plugin sources when no manifest knows the plugin yet). Everything else is ignored.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use regex::Regex;

/// The connector types that need no plugin (`connectors.connectors` plus the `systemd` executor).
pub const BUILTIN_CONNECTOR_TYPES: [&str; 5] = ["api", "plugin", "sql", "systemd", "valkey"];

/// Whether `typ` (lowercase) is a built-in connector type.
pub fn is_builtin_connector_type(typ: &str) -> bool {
    BUILTIN_CONNECTOR_TYPES.contains(&typ)
}

/// The most files and bytes of plugin source read when looking for `@make_connector`.
const MAX_SOURCE_FILES: usize = 4000;
const MAX_SOURCE_BYTES: u64 = 2 * 1024 * 1024;
const MAX_SOURCE_DEPTH: usize = 4;

/// `@make_connector` (optionally with other decorators between it and the class), then the class.
static DECORATED_CLASS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)^[ \t]*@(?:[\w.]*\.)?make_connector\b[^\n]*\n(?:[ \t]*(?:@[^\n]*|#[^\n]*|)\n)*[ \t]*class[ \t]+(\w+)")
        .expect("valid make_connector regex")
});

/// `make_connector(FooConnector)` called as a function.
static CALLED_CLASS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?m)\bmake_connector\(\s*(\w+)\s*[,)]").expect("valid make_connector call regex")
});

/// The type `make_connector` registers a class under (`FooConnector` -> `foo`).
fn type_of_class(name: &str) -> String {
    let lower = name.to_lowercase();
    lower
        .strip_suffix("connector")
        .or_else(|| lower.strip_suffix("executor"))
        .unwrap_or(&lower)
        .to_string()
}

/// The connector types a manifest file lists (`connector_types` keys), or none if unreadable.
fn manifest_types(path: &Path) -> Vec<String> {
    let Ok(text) = std::fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(json) = serde_json::from_str::<serde_json::Value>(&text) else {
        return Vec::new();
    };
    json.get("connector_types")
        .and_then(serde_json::Value::as_object)
        .map(|m| m.keys().map(|k| k.to_lowercase()).collect())
        .unwrap_or_default()
}

/// The types registered by `@make_connector` in the Python files under `dir`.
fn source_types(dir: &Path, out: &mut BTreeSet<String>) {
    let mut budget = MAX_SOURCE_FILES;
    let mut stack: Vec<(PathBuf, usize)> = vec![(dir.to_path_buf(), 0)];
    while let Some((path, depth)) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&path) else {
            continue;
        };
        for entry in entries.flatten() {
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            let name = entry.file_name().to_string_lossy().into_owned();
            if kind.is_dir() {
                if depth + 1 < MAX_SOURCE_DEPTH
                    && name != "__pycache__"
                    && name != "node_modules"
                    && !name.starts_with('.')
                {
                    stack.push((entry.path(), depth + 1));
                }
                continue;
            }
            if !kind.is_file() || !name.ends_with(".py") {
                continue;
            }
            if budget == 0 {
                return;
            }
            budget -= 1;
            let Ok(meta) = entry.metadata() else { continue };
            if meta.len() > MAX_SOURCE_BYTES {
                continue;
            }
            let Ok(text) = std::fs::read_to_string(entry.path()) else {
                continue;
            };
            if !text.contains("make_connector") {
                continue;
            }
            for caps in DECORATED_CLASS
                .captures_iter(&text)
                .chain(CALLED_CLASS.captures_iter(&text))
            {
                out.insert(type_of_class(&caps[1]));
            }
        }
    }
}

/// The connector types registered by plugins: the cached manifests under `root_dir/cache`, and
/// the `@make_connector` classes found in `plugins_dirs` (a plugin not yet in a manifest).
pub fn plugin_connector_types(root_dir: &Path, plugins_dirs: &[PathBuf]) -> BTreeSet<String> {
    let mut types = BTreeSet::new();
    let cache = root_dir.join("cache");
    for file in ["plugins_manifest.json", "native_plugins_manifest.json"] {
        types.extend(manifest_types(&cache.join(file)));
    }
    for dir in plugins_dirs {
        source_types(dir, &mut types);
    }
    types
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn class_names_map_to_types() {
        assert_eq!(type_of_class("MQTTConnector"), "mqtt");
        assert_eq!(type_of_class("FooExecutor"), "foo");
        assert_eq!(type_of_class("Bar"), "bar");
    }

    #[test]
    fn sources_and_manifests_are_read() {
        let root = tempfile::tempdir().unwrap();
        let plugins = root.path().join("plugins");
        std::fs::create_dir_all(plugins.join("demo")).unwrap();
        std::fs::write(
            plugins.join("demo/__init__.py"),
            "import meerschaum as mrsm\nfrom meerschaum.connectors import make_connector, Connector\n\n@make_connector\n# comment\n@other\nclass DemoConnector(Connector):\n    pass\n",
        )
        .unwrap();
        std::fs::write(plugins.join("plain.py"), "class NotAConnector: pass\n").unwrap();
        std::fs::create_dir_all(root.path().join("cache")).unwrap();
        std::fs::write(
            root.path().join("cache/plugins_manifest.json"),
            r#"{"connector_types":{"Fromjson":{"plugin":"x"}}}"#,
        )
        .unwrap();
        let types = plugin_connector_types(root.path(), &[plugins]);
        assert_eq!(
            types.into_iter().collect::<Vec<_>>(),
            vec!["demo".to_string(), "fromjson".to_string()]
        );
    }
}
