//! Ordered multi-directory discovery; native entries take precedence over Python entries.
use crate::{LoadedPlugin, Result, error, host::validate_manifest};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::HashSet,
    path::{Path, PathBuf},
    time::UNIX_EPOCH,
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Candidate {
    pub name: String,
    pub version: String,
    pub directory: PathBuf,
    pub library: PathBuf,
    pub fingerprint: String,
}
#[derive(Deserialize)]
struct Descriptor {
    name: String,
    version: String,
}
/// Spec naming includes `lib` on Windows too (`lib<name>.dll`).
pub fn library_name(name: &str) -> String {
    format!("lib{name}.{}", std::env::consts::DLL_EXTENSION)
}
/// No implicit home/config fallback. Callers supply configured directories in priority order.
pub fn discover(directories: &[PathBuf]) -> Result<Vec<Candidate>> {
    let mut seen = HashSet::new();
    let mut found = Vec::new();
    for root in directories {
        if !root.exists() {
            continue;
        }
        let mut entries = std::fs::read_dir(root)
            .map_err(error)?
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(error)?;
        entries.sort_by_key(|e| e.file_name());
        for entry in entries {
            let directory = entry.path();
            let descriptor = directory.join("plugin.toml");
            if !descriptor.is_file() {
                continue;
            }
            let desc: Descriptor =
                toml::from_str(&std::fs::read_to_string(&descriptor).map_err(error)?)
                    .map_err(error)?;
            if desc.name.is_empty()
                || !desc
                    .name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
                || desc.version.is_empty()
            {
                return Err(error("invalid native plugin descriptor"));
            }
            if !seen.insert(desc.name.clone()) {
                continue;
            }
            let library = directory.join(library_name(&desc.name));
            if !library.is_file() {
                return Err(error(format!("missing native library for {}", desc.name)));
            }
            let fingerprint = fingerprint(&directory)?;
            found.push(Candidate {
                name: desc.name,
                version: desc.version,
                directory,
                library,
                fingerprint,
            });
        }
    }
    Ok(found)
}
/// Parse the OS path-list form of MRSM_PLUGINS_DIR, without resolving defaults.
pub fn environment_directories() -> Vec<PathBuf> {
    std::env::var_os("MRSM_PLUGINS_DIR")
        .map(|v| std::env::split_paths(&v).collect())
        .unwrap_or_default()
}

/// SHA-256 of sorted relative paths, sizes and nanosecond mtimes, ignoring Python bytecode.
/// Symlinks are rejected to avoid cycles and untracked external dependencies.
pub fn fingerprint(root: &Path) -> Result<String> {
    fn walk(root: &Path, dir: &Path, hash: &mut Sha256) -> Result<()> {
        let mut entries = std::fs::read_dir(dir)
            .map_err(error)?
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(error)?;
        entries.sort_by_key(|e| e.path());
        for entry in entries {
            let path = entry.path();
            let kind = entry.file_type().map_err(error)?;
            if entry.file_name() == "__pycache__"
                || path.extension().is_some_and(|e| e == "pyc")
                || (kind.is_dir() && entry.file_name().to_string_lossy().starts_with('.'))
            {
                continue;
            }
            if kind.is_symlink() {
                return Err(error("symlinks in native plugin trees are unsupported"));
            }
            let meta = entry.metadata().map_err(error)?;
            if kind.is_dir() {
                walk(root, &path, hash)?;
            } else if kind.is_file() {
                let rel = path
                    .strip_prefix(root)
                    .map_err(error)?
                    .to_string_lossy()
                    .replace('\\', "/");
                hash.update(rel.as_bytes());
                hash.update([0]);
                hash.update(meta.len().to_le_bytes());
                hash.update(
                    meta.modified()
                        .map_err(error)?
                        .duration_since(UNIX_EPOCH)
                        .map_err(error)?
                        .as_nanos()
                        .to_le_bytes(),
                );
            }
        }
        Ok(())
    }
    let mut hash = Sha256::new();
    let descriptor: Descriptor =
        toml::from_str(&std::fs::read_to_string(root.join("plugin.toml")).map_err(error)?)
            .map_err(error)?;
    hash.update(descriptor.name.as_bytes());
    hash.update([0]);
    walk(root, root, &mut hash)?;
    Ok(format!("{:x}", hash.finalize()))
}

/// Read cached manifests without loading code on cache hits. Cache destination must be
/// outside plugin directories. Atomic replacement prevents partially written JSON.
/// # Safety
/// Every discovered library must satisfy `LoadedPlugin::load`'s trust requirements.
pub unsafe fn cached_manifest(directories: &[PathBuf], cache: &Path) -> Result<Value> {
    let candidates = discover(directories)?;
    // Same byte grammar as the Python-worker host manifest fingerprint.
    let mut hash = Sha256::new();
    hash.update(crate::MANIFEST_SCHEMA_VERSION.to_le_bytes());
    for candidate in &candidates {
        hash.update(candidate.name.as_bytes());
        hash.update([0]);
        hash.update(candidate.fingerprint.as_bytes());
    }
    let key = format!("{:x}", hash.finalize());
    if let Ok(bytes) = std::fs::read(cache) {
        if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
            if v["fingerprint"] == key && validate_manifest(&v).is_ok() {
                return Ok(v);
            }
        }
    }
    let mut result = json!({"schema_version":crate::MANIFEST_SCHEMA_VERSION,"meerschaum_version":env!("CARGO_PKG_VERSION"),"python_version":"","python_executable":null,"fingerprint":key,"plugins":{},"connector_types":{},"pre_sync_hooks":[],"post_sync_hooks":[],"load_errors":{}});
    for candidate in candidates {
        // SAFETY: forwarded trust requirement from this function's caller.
        let plugin = unsafe { LoadedPlugin::load(&candidate.library) }?;
        if plugin.name != candidate.name || plugin.version != candidate.version {
            return Err(error("descriptor and exported metadata differ"));
        }
        let mut entry = plugin.manifest["plugins"][&plugin.name].clone();
        entry["fingerprint"] = json!(candidate.fingerprint);
        entry["path"] = json!(candidate.directory);
        result["plugins"][&plugin.name] = entry;
        if let Some(map) = plugin.manifest["connector_types"].as_object() {
            for (key, value) in map {
                result["connector_types"][key] = value.clone();
            }
        }
        for field in ["pre_sync_hooks", "post_sync_hooks"] {
            if let Some(items) = plugin.manifest[field].as_array() {
                result[field].as_array_mut().unwrap().extend(items.clone());
            }
        }
    }
    validate_manifest(&result)?;
    if let Some(parent) = cache.parent() {
        std::fs::create_dir_all(parent).map_err(error)?;
    }
    // A unique temporary sibling also allows concurrent independent manifest builders.
    let temp = cache.with_extension(format!(
        "{}.{}.tmp",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(error)?
            .as_nanos()
    ));
    std::fs::write(&temp, serde_json::to_vec_pretty(&result).map_err(error)?).map_err(error)?;
    let renamed = std::fs::rename(&temp, cache).map_err(error);
    if renamed.is_err() {
        let _ = std::fs::remove_file(&temp);
    }
    renamed?;
    Ok(result)
}
