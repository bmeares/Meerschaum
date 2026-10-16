//! Ordered multi-directory discovery; native entries take precedence over Python entries.
use crate::{
    LoadedPlugin, Result, error,
    host::validate_manifest,
    source::{self, SourceBuild},
};
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
    /// The library to load. For a source plugin this is the file Cargo produces under the
    /// build directory, which does not exist yet while `build` is `Some`.
    pub library: PathBuf,
    /// Names, sizes and mtimes of the plugin tree; for a source plugin the content hash of
    /// the sources (see [`crate::source`]).
    pub fingerprint: String,
    /// The plugin is built from a Cargo crate by Meerschaum.
    #[serde(default)]
    pub source_crate: bool,
    /// Set while the library still has to be built (see [`Discovery::build_pending`]).
    #[serde(default)]
    pub build: Option<SourceBuild>,
    /// A one-line warning for a plugin that still loads (a stale shipped library that could
    /// not be rebuilt); printed by `discover`, `cached_manifest` and `NativePlugins::open`.
    #[serde(default)]
    pub warning: Option<String>,
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
/// A plugin directory that was skipped, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    pub directory: PathBuf,
    pub reason: String,
    /// The plugin is a valid source crate that could not be built (no toolchain, failed
    /// build) rather than an invalid plugin.
    pub source_crate: bool,
}

impl Skipped {
    /// The warning text printed for this plugin.
    pub fn message(&self) -> String {
        format!(
            "Skipping {}native plugin at '{}': {}",
            if self.source_crate { "" } else { "invalid " },
            self.directory.display(),
            self.reason
        )
    }
}

/// The result of scanning the plugin directories: the usable plugins and the ones skipped.
#[derive(Debug, Clone, Default)]
pub struct Discovery {
    pub candidates: Vec<Candidate>,
    pub skipped: Vec<Skipped>,
}

fn warn_candidates(candidates: &[Candidate]) {
    for warning in candidates.iter().filter_map(|c| c.warning.as_deref()) {
        eprintln!("Warning: {warning}");
    }
}

fn warn_skipped(skipped: &[Skipped]) {
    for item in skipped {
        eprintln!("Warning: {}", item.message());
    }
}

struct Skip {
    reason: String,
    source_crate: bool,
}

impl Skip {
    fn invalid(e: impl std::fmt::Display) -> Skip {
        Skip {
            reason: e.to_string(),
            source_crate: false,
        }
    }
    fn source(reason: String) -> Skip {
        Skip {
            reason,
            source_crate: true,
        }
    }
}

/// Where source plugins are built: explicit, or `<root>/cache/native_build` of the process
/// environment's Meerschaum root (resolved by `meerschaum-core`, only when a source plugin
/// needs it).
struct BuildRoot<'a>(Option<&'a Path>);

impl BuildRoot<'_> {
    fn resolve(&self) -> Result<PathBuf> {
        if let Some(path) = self.0 {
            return Ok(path.to_path_buf());
        }
        let lookup = |key: &str| std::env::var(key).ok();
        let home = ["HOME", "USERPROFILE"]
            .iter()
            .find_map(std::env::var_os)
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/"));
        let paths = meerschaum_core::config::Paths::from_env(&lookup, &home, cfg!(windows), None)
            .map_err(error)?;
        Ok(paths.root_dir.join("cache").join(source::BUILD_DIR_NAME))
    }
}

fn read_candidate(
    directory: &Path,
    seen: &HashSet<String>,
    build_root: &BuildRoot<'_>,
) -> std::result::Result<Option<Candidate>, Skip> {
    let descriptor = directory.join("plugin.toml");
    if !descriptor.is_file() {
        return Ok(None);
    }
    let desc: Descriptor = toml::from_str(
        &std::fs::read_to_string(&descriptor)
            .map_err(error)
            .map_err(Skip::invalid)?,
    )
    .map_err(error)
    .map_err(Skip::invalid)?;
    if desc.name.is_empty()
        || !desc
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        || desc.version.is_empty()
    {
        return Err(Skip::invalid("invalid native plugin descriptor"));
    }
    if seen.contains(&desc.name) {
        return Ok(None);
    }
    let library = directory.join(library_name(&desc.name));
    let is_source = source::is_source_crate(directory);
    // A library shipped next to the sources wins unless a build input is newer than it.
    let prebuilt =
        library.is_file() && !(is_source && source::sources_newer_than(directory, &library));
    if prebuilt || !is_source {
        if !library.is_file() {
            return Err(Skip::invalid(format!(
                "missing native library for {}",
                desc.name
            )));
        }
        return Ok(Some(Candidate {
            fingerprint: fingerprint(directory).map_err(Skip::invalid)?,
            name: desc.name,
            version: desc.version,
            directory: directory.to_path_buf(),
            library,
            source_crate: false,
            build: None,
            warning: None,
        }));
    }
    source_candidate(directory, desc, library, build_root)
}

/// A source plugin: validate the crate and decide whether its library needs building.
fn source_candidate(
    directory: &Path,
    desc: Descriptor,
    shipped_library: PathBuf,
    build_root: &BuildRoot<'_>,
) -> std::result::Result<Option<Candidate>, Skip> {
    let info = source::read_crate(directory).map_err(Skip::invalid)?;
    if info.package_name != desc.name {
        return Err(Skip::invalid(format!(
            "the crate's package name '{}' does not match the plugin.toml name '{}'",
            info.package_name, desc.name
        )));
    }
    if !info.cdylib {
        return Err(Skip::invalid(
            "the crate must build a cdylib (add `[lib] crate-type = [\"cdylib\"]` to Cargo.toml)",
        ));
    }
    let source_fp = source::source_fingerprint(directory).map_err(Skip::invalid)?;
    let target_dir = build_root
        .resolve()
        .map_err(Skip::invalid)?
        .join(&desc.name);
    let library = source::built_library(&target_dir, &info.library_name);
    let mut candidate = Candidate {
        name: desc.name.clone(),
        version: desc.version,
        directory: directory.to_path_buf(),
        library,
        fingerprint: source_fp,
        source_crate: true,
        build: None,
        warning: None,
    };
    if source::build_is_current(&target_dir, &candidate.library, &candidate.fingerprint) {
        return Ok(Some(candidate));
    }
    if source::build_failed_before(&target_dir, &candidate.fingerprint) {
        return Err(Skip::source(format!(
            "the last build failed; edit the sources or delete '{}' to retry",
            target_dir.display()
        )));
    }
    match source::find_cargo() {
        Some(cargo) => {
            candidate.build = Some(SourceBuild {
                cargo,
                manifest_path: directory.join("Cargo.toml"),
                target_dir,
                fingerprint: candidate.fingerprint.clone(),
            });
            Ok(Some(candidate))
        }
        // No toolchain: a stale shipped library is still better than none.
        None if shipped_library.is_file() => Ok(Some(Candidate {
            fingerprint: fingerprint(directory).map_err(Skip::invalid)?,
            library: shipped_library,
            source_crate: false,
            warning: Some(format!(
                "Using native plugin at '{}': its library is older than the sources and was not \
                 rebuilt (no `cargo` was found on PATH)",
                directory.display()
            )),
            ..candidate
        })),
        None => Err(Skip::source(format!(
            "it is a source crate and no `cargo` was found on PATH (install a Rust toolchain or ship {})",
            library_name(&desc.name)
        ))),
    }
}

/// Scan the directories without building anything. An invalid plugin is recorded in
/// `skipped` and the scan continues (PARITY: v4 warns about a broken Python plugin and loads
/// the rest). No implicit home/config fallback. Callers supply configured directories in
/// priority order.
///
/// A source plugin whose library is missing or out of date is returned with
/// [`Candidate::build`] set; [`Discovery::build_pending`] builds it. The build directory is
/// `<root>/cache/native_build` of the Meerschaum root from the process environment; use
/// [`discover_report_in`] to name it.
pub fn discover_report(directories: &[PathBuf]) -> Discovery {
    scan(directories, &BuildRoot(None))
}

/// [`discover_report`] with an explicit build directory (the parent of the per-plugin target
/// directories, normally `<root>/cache/native_build`).
pub fn discover_report_in(directories: &[PathBuf], build_dir: &Path) -> Discovery {
    scan(directories, &BuildRoot(Some(build_dir)))
}

fn scan(directories: &[PathBuf], build_root: &BuildRoot<'_>) -> Discovery {
    let mut seen = HashSet::new();
    let mut found = Discovery::default();
    for root in directories {
        if !root.exists() {
            continue;
        }
        let entries =
            match std::fs::read_dir(root).and_then(|rd| rd.collect::<std::io::Result<Vec<_>>>()) {
                Ok(mut entries) => {
                    entries.sort_by_key(|e| e.file_name());
                    entries
                }
                Err(e) => {
                    found.skipped.push(Skipped {
                        directory: root.clone(),
                        reason: e.to_string(),
                        source_crate: false,
                    });
                    continue;
                }
            };
        for entry in entries {
            let directory = entry.path();
            match read_candidate(&directory, &seen, build_root) {
                Ok(Some(candidate)) => {
                    seen.insert(candidate.name.clone());
                    found.candidates.push(candidate);
                }
                Ok(None) => {}
                Err(skip) => found.skipped.push(Skipped {
                    directory,
                    reason: skip.reason,
                    source_crate: skip.source_crate,
                }),
            }
        }
    }
    found
}

impl Discovery {
    /// Build the source plugins whose library is missing or out of date with
    /// `cargo build --release`. A build failure moves the plugin to `skipped` with the last
    /// lines of cargo's stderr; the other plugins are unaffected. Plugins whose build is
    /// current are not touched.
    pub fn build_pending(&mut self) {
        let mut ready = Vec::with_capacity(self.candidates.len());
        for mut candidate in std::mem::take(&mut self.candidates) {
            if let Some(spec) = candidate.build.take() {
                match source::build(&candidate.name, &spec, &candidate.library) {
                    Ok(fingerprint) => candidate.fingerprint = fingerprint,
                    Err(reason) => {
                        self.skipped.push(Skipped {
                            directory: candidate.directory,
                            reason,
                            source_crate: true,
                        });
                        continue;
                    }
                }
            }
            ready.push(candidate);
        }
        self.candidates = ready;
    }
}

/// Like [`discover_report`] but building source plugins, and printing a warning for every
/// skipped plugin.
pub fn discover(directories: &[PathBuf]) -> Result<Vec<Candidate>> {
    let mut found = discover_report(directories);
    found.build_pending();
    warn_candidates(&found.candidates);
    warn_skipped(&found.skipped);
    Ok(found.candidates)
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
    Ok(hex::encode(hash.finalize()))
}

/// Read cached manifests without loading code on cache hits. Cache destination must be
/// outside plugin directories. Atomic replacement prevents partially written JSON. Source
/// plugins are built first when their library is missing or out of date, into
/// `native_build` next to the cache file.
/// # Safety
/// Every discovered library must satisfy `LoadedPlugin::load`'s trust requirements.
pub unsafe fn cached_manifest(directories: &[PathBuf], cache: &Path) -> Result<Value> {
    let mut discovery = discover_report_in(directories, &build_dir_for_cache(cache));
    discovery.build_pending();
    warn_candidates(&discovery.candidates);
    // SAFETY: forwarded trust requirement from this function's caller.
    let (manifest, skipped) = unsafe { manifest_for(discovery, cache) }?;
    warn_skipped(&skipped);
    Ok(manifest)
}

/// The directory source plugins are built in for a manifest cache file
/// (`<root>/cache/native_plugins_manifest.json` -> `<root>/cache/native_build`).
pub fn build_dir_for_cache(cache: &Path) -> PathBuf {
    cache
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(source::BUILD_DIR_NAME)
}

/// Like [`cached_manifest`], returning the skipped plugins (invalid descriptor, missing or
/// unloadable library, source plugin that could not be built) instead of printing them.
/// Skipped plugins are absent from the manifest. A `Candidate::warning` (stale shipped
/// library) is not printed here.
/// # Safety
/// Every discovered library must satisfy `LoadedPlugin::load`'s trust requirements.
pub unsafe fn cached_manifest_report(
    directories: &[PathBuf],
    cache: &Path,
) -> Result<(Value, Vec<Skipped>)> {
    let mut discovery = discover_report_in(directories, &build_dir_for_cache(cache));
    discovery.build_pending();
    // SAFETY: forwarded trust requirement from this function's caller.
    unsafe { manifest_for(discovery, cache) }
}

/// The manifest of an already scanned (and built) [`Discovery`]; the returned skipped list is
/// the discovery's plus the plugins that failed to load.
/// # Safety
/// Every candidate's library must satisfy `LoadedPlugin::load`'s trust requirements.
pub(crate) unsafe fn manifest_for(
    discovery: Discovery,
    cache: &Path,
) -> Result<(Value, Vec<Skipped>)> {
    let Discovery {
        candidates,
        mut skipped,
    } = discovery;
    // Same byte grammar as the Python-worker host manifest fingerprint.
    let mut hash = Sha256::new();
    hash.update(crate::MANIFEST_SCHEMA_VERSION.to_le_bytes());
    for candidate in &candidates {
        hash.update(candidate.name.as_bytes());
        hash.update([0]);
        hash.update(candidate.fingerprint.as_bytes());
    }
    let key = hex::encode(hash.finalize());
    if let Ok(bytes) = std::fs::read(cache) {
        if let Ok(v) = serde_json::from_slice::<Value>(&bytes) {
            if v["fingerprint"] == key && validate_manifest(&v).is_ok() {
                return Ok((v, skipped));
            }
        }
    }
    let mut result = json!({"schema_version":crate::MANIFEST_SCHEMA_VERSION,"meerschaum_version":env!("CARGO_PKG_VERSION"),"python_version":"","python_executable":null,"fingerprint":key,"plugins":{},"connector_types":{},"pre_sync_hooks":[],"post_sync_hooks":[],"load_errors":{}});
    for candidate in candidates {
        // SAFETY: forwarded trust requirement from this function's caller.
        let plugin = match unsafe { LoadedPlugin::load(&candidate.library) } {
            Ok(plugin) => plugin,
            Err(e) => {
                skipped.push(Skipped {
                    directory: candidate.directory,
                    reason: e.to_string(),
                    source_crate: false,
                });
                continue;
            }
        };
        if plugin.name != candidate.name || plugin.version != candidate.version {
            skipped.push(Skipped {
                directory: candidate.directory,
                reason: "descriptor and exported metadata differ".to_owned(),
                source_crate: false,
            });
            continue;
        }
        let mut entry = plugin.manifest["plugins"][&plugin.name].clone();
        entry["fingerprint"] = json!(candidate.fingerprint);
        entry["path"] = json!(candidate.directory);
        if candidate.source_crate {
            // Built by Meerschaum under <root>/cache/native_build, not next to plugin.toml.
            entry["library"] = json!(candidate.library);
        }
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
    Ok((result, skipped))
}
