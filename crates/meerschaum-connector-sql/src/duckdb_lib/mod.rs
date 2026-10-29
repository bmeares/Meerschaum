//! DuckDB's shared library, found or installed at run time.
//!
//! Meerschaum does not compile DuckDB in. The `duckdb` flavor loads DuckDB's own release build
//! (`libduckdb`, [`VERSION`]) through its stable C API (see `driver/duckdb/ffi.rs`), the way v4
//! installed a package on first use. The library is looked up in this order:
//!
//! 1. `MRSM_DUCKDB_LIB`: an explicit file path (air-gapped hosts; a failure to load it is an error);
//! 2. the cache `<root>/drivers/duckdb/<version>/libduckdb.{so,dylib}` / `duckdb.dll`;
//! 3. the system loader path (`libduckdb.so`, ...), if it is `ffi::MIN_VERSION` or newer;
//! 4. a download of the official release asset, checked against a SHA-256 pinned in this crate,
//!    unless `MRSM_NO_AUTO_INSTALL` is set.
//!
//! The library is loaded once per process. `mrsm install driver duckdb` and
//! `mrsm uninstall driver duckdb` manage the cache; `mrsm show drivers` reports where it comes from.

mod install;

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use crate::driver::duckdb::ffi;
use crate::error::SqlError;
use crate::flavor::Flavor;

/// The DuckDB release the installer downloads (its C API is the contract).
pub const VERSION: &str = "1.5.6";

/// Where a loaded or locatable library comes from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Source {
    /// `MRSM_DUCKDB_LIB`.
    Env,
    /// `<root>/drivers/duckdb/<version>/`.
    Cache,
    /// The system loader path.
    System,
}

impl Source {
    /// The word `show drivers` prints.
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Env => "env",
            Source::Cache => "cache",
            Source::System => "system",
        }
    }
}

/// The settings that decide where the library lives (process environment by default; the CLI
/// passes its own lookup).
#[derive(Debug, Clone, Default)]
pub struct Settings {
    /// The Meerschaum root (`MRSM_ROOT_DIR`); the cache lives in `<root>/drivers`.
    pub root: Option<PathBuf>,
    /// `MRSM_DUCKDB_LIB`.
    pub lib: Option<PathBuf>,
    /// `MRSM_NO_AUTO_INSTALL`.
    pub no_auto_install: bool,
}

impl Settings {
    /// Read the settings through an environment lookup.
    ///
    /// Without `MRSM_ROOT_DIR`, use the normal default root in every build.
    pub fn from_env(env: &dyn Fn(&str) -> Option<String>) -> Settings {
        let set = |name: &str| env(name).filter(|v| !v.is_empty());
        let root = match env("MRSM_ROOT_DIR") {
            Some(r) => Some(meerschaum_core::config::paths::resolve_path(Path::new(&r))),
            None => {
                let base = meerschaum_core::config::Paths::process_dot_config_dir();
                Some(base.join(if cfg!(windows) {
                    "Meerschaum"
                } else {
                    "meerschaum"
                }))
            }
        };
        Settings {
            root,
            lib: set("MRSM_DUCKDB_LIB").map(|p| absolutize(Path::new(&p))),
            no_auto_install: set("MRSM_NO_AUTO_INSTALL")
                .is_some_and(|v| matches!(v.to_lowercase().as_str(), "1" | "true" | "yes")),
        }
    }

    /// The settings of this process: the environment, with the root replaced by
    /// [`set_root_override`] when one is set (the root the Meerschaum config in effect uses).
    pub fn from_process_env() -> Settings {
        let mut settings = Settings::from_env(&|name| std::env::var(name).ok());
        if let Some(root) = root_override() {
            settings.root = Some(root);
        }
        settings
    }

    fn root(&self) -> Result<&Path, SqlError> {
        self.root.as_deref().ok_or_else(|| {
            unavailable(
                "the Meerschaum root is not known here; \
                 set MRSM_ROOT_DIR, or set MRSM_DUCKDB_LIB to the path of the library"
                    .into(),
            )
        })
    }
}

static ROOT_OVERRIDE: RwLock<Option<PathBuf>> = RwLock::new(None);

/// Make `root` the Meerschaum root the implicit (first-use) install and lookup use in this
/// process, instead of `MRSM_ROOT_DIR`. `mrsm install driver` / `show drivers` take the root
/// from the config in effect; this makes a connection opened later agree with them, also after
/// the root changed at run time (Python's `set_root()` / `replace_root_dir()`). `None` goes back
/// to the environment.
pub fn set_root_override(root: Option<PathBuf>) {
    if let Ok(mut r) = ROOT_OVERRIDE.write() {
        *r = root;
    }
}

/// The root set by [`set_root_override`].
pub fn root_override() -> Option<PathBuf> {
    ROOT_OVERRIDE.read().ok().and_then(|r| r.clone())
}

/// An absolute, canonical form of a user-given library path, so the existence check and the load
/// name the same file whatever the working directory does later (a relative `MRSM_DUCKDB_LIB`
/// would otherwise be looked up twice).
fn absolutize(path: &Path) -> PathBuf {
    std::fs::canonicalize(path)
        .or_else(|_| std::path::absolute(path))
        .unwrap_or_else(|_| path.to_path_buf())
}

/// The file name of the library on this platform.
pub fn lib_file() -> &'static str {
    if cfg!(windows) {
        "duckdb.dll"
    } else if cfg!(target_os = "macos") {
        "libduckdb.dylib"
    } else {
        "libduckdb.so"
    }
}

/// `<root>/drivers/duckdb/<version>/`.
pub fn cache_dir(root: &Path) -> PathBuf {
    drivers_dir(root).join(VERSION)
}

fn drivers_dir(root: &Path) -> PathBuf {
    root.join("drivers").join("duckdb")
}

fn unavailable(reason: String) -> SqlError {
    SqlError::DriverUnavailable {
        flavor: Flavor::Duckdb,
        reason,
    }
}

/// Reports the progress of an install (the CLI draws its progress line; the default prints two
/// lines to stderr).
pub trait Reporter: Send + Sync {
    /// The install is starting: the release version and the download size in bytes.
    fn start(&self, version: &str, bytes: u64);
    /// `done` of `total` bytes downloaded.
    fn progress(&self, done: u64, total: u64);
    /// The install ended: the library path, or the reason it failed.
    fn finish(&self, result: Result<&Path, &str>);
}

/// Reports nothing.
pub struct NullReporter;

impl Reporter for NullReporter {
    fn start(&self, _: &str, _: u64) {}
    fn progress(&self, _: u64, _: u64) {}
    fn finish(&self, _: Result<&Path, &str>) {}
}

/// The default reporter: v4-style lines on stderr.
struct StderrReporter;

impl Reporter for StderrReporter {
    fn start(&self, version: &str, bytes: u64) {
        eprintln!(
            "Installing the DuckDB driver (v{version}, {} MB)...",
            (bytes + 500_000) / 1_000_000
        );
    }
    fn progress(&self, _: u64, _: u64) {}
    fn finish(&self, result: Result<&Path, &str>) {
        if let Ok(path) = result {
            eprintln!("Installed the DuckDB driver to '{}'.", path.display());
        }
    }
}

static REPORTER: RwLock<Option<Arc<dyn Reporter>>> = RwLock::new(None);

/// Replace the reporter an implicit (first-use) install uses; `None` restores the default.
pub fn set_reporter(reporter: Option<Arc<dyn Reporter>>) {
    if let Ok(mut r) = REPORTER.write() {
        *r = reporter;
    }
}

fn current_reporter() -> Arc<dyn Reporter> {
    REPORTER
        .read()
        .ok()
        .and_then(|r| r.clone())
        .unwrap_or_else(|| Arc::new(StderrReporter))
}

/// What [`status`] found.
#[derive(Debug, Clone)]
pub struct Status {
    /// Where the library comes from; `None` when it is missing.
    pub source: Option<Source>,
    /// The library path (`libduckdb.so` for the system loader).
    pub path: Option<String>,
    /// `duckdb_library_version()` of the loaded library.
    pub version: Option<String>,
    /// Why a located library could not be loaded (or why none was found).
    pub error: Option<String>,
}

/// A library loaded in this process.
#[derive(Debug, Clone)]
pub struct Loaded {
    pub source: Source,
    pub path: String,
    pub version: String,
}

fn loaded_from(api: &ffi::Api, source: Source) -> Loaded {
    Loaded {
        source,
        path: api.path.display().to_string(),
        version: api.version.clone(),
    }
}

/// Which source the already loaded library came from (it is whatever the first load chose).
static LOADED_SOURCE: std::sync::OnceLock<Source> = std::sync::OnceLock::new();

/// The first place to load the library from: `MRSM_DUCKDB_LIB` (even a missing file: an explicit
/// setting is never skipped), else the cache when the library is installed there. The system
/// loader path is the caller's last resort.
fn candidate(settings: &Settings) -> Option<(Source, PathBuf)> {
    if let Some(path) = &settings.lib {
        return Some((Source::Env, absolutize(path)));
    }
    let cached = cache_dir(settings.root.as_deref()?).join(lib_file());
    cached.is_file().then_some((Source::Cache, cached))
}

/// Load the library if it can be found without installing: the process's library, `MRSM_DUCKDB_LIB`,
/// the cache, then the system loader path. `Ok(None)` means it is missing.
pub fn load_existing(settings: &Settings) -> Result<Option<Loaded>, SqlError> {
    if let Some(api) = ffi::loaded() {
        let source = LOADED_SOURCE.get().copied().unwrap_or(Source::System);
        return Ok(Some(loaded_from(api, source)));
    }
    let attempt = |path: &Path, source: Source| -> Result<Loaded, String> {
        let api = ffi::load(path)?;
        let _ = LOADED_SOURCE.set(source);
        Ok(loaded_from(api, source))
    };
    match candidate(settings) {
        Some((Source::Env, path)) => {
            if !path.is_file() {
                return Err(unavailable(format!(
                    "MRSM_DUCKDB_LIB is set to '{}', which is not a file",
                    path.display()
                )));
            }
            return attempt(&path, Source::Env)
                .map(Some)
                .map_err(|e| unavailable(format!("{e} (from MRSM_DUCKDB_LIB)")));
        }
        Some((source, path)) => {
            return attempt(&path, source).map(Some).map_err(|e| {
                unavailable(format!(
                    "{e}. Reinstall it with `mrsm install driver duckdb --force`"
                ))
            });
        }
        None => {}
    }
    // The system loader path. A library that is too old or broken is skipped, not fatal: the
    // download below installs a good one.
    Ok(system_candidate().and_then(|path| attempt(&path, Source::System).ok()))
}

/// What to load from the system library path, if anything: a file found in an absolute system
/// library directory, loaded by its full path, never by bare name. A bare name is not safe on any
/// platform: a Windows or macOS loader may search the working directory, and a Linux
/// `LD_LIBRARY_PATH` with an empty entry (`/x:`, common after `export
/// LD_LIBRARY_PATH=/x:$LD_LIBRARY_PATH` with the variable unset) means "the current directory" to
/// `dlopen`. The directories are the absolute ones of `client_libs::system_library_dirs`
/// (`LD_LIBRARY_PATH`, `ld.so.conf` and the standard directories on Linux; Homebrew and
/// `/usr/local/lib` on macOS; the absolute `PATH` entries on Windows). On Windows `ffi::load`
/// also restricts the DLL search directories.
fn system_candidate() -> Option<PathBuf> {
    crate::client_libs::find_system_library(lib_file())
}

/// The message of a missing library when nothing may be installed.
fn missing_message() -> String {
    format!(
        "the DuckDB library ({}) was not found and MRSM_NO_AUTO_INSTALL is set. Run \
         `mrsm install driver duckdb`, or set MRSM_DUCKDB_LIB to the path of a DuckDB library \
         (v{}.{} or newer)",
        lib_file(),
        ffi::MIN_VERSION.0,
        ffi::MIN_VERSION.1
    )
}

/// Make sure the library is loaded: find it, or download it on first use (unless
/// `MRSM_NO_AUTO_INSTALL`).
pub async fn ensure_loaded(settings: &Settings) -> Result<Loaded, SqlError> {
    if let Some(loaded) = load_existing(settings)? {
        return Ok(loaded);
    }
    if settings.no_auto_install {
        return Err(unavailable(missing_message()));
    }
    // An offline host would otherwise wait out the connection timeout again on every connection:
    // the first failed download is remembered for the life of the process.
    if let Some(reason) = install_failure() {
        return Err(unavailable(format!(
            "{reason} (not retried in this process; run `mrsm install driver duckdb` once the network is back)"
        )));
    }
    let reporter = current_reporter();
    if let Err(e) = install(settings, false, reporter.as_ref()).await {
        remember_install_failure(&e);
        return Err(e);
    }
    load_existing(settings)?.ok_or_else(|| unavailable(missing_message()))
}

/// The reason the implicit install failed earlier in this process.
static INSTALL_FAILURE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

fn install_failure() -> Option<String> {
    INSTALL_FAILURE
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

fn remember_install_failure(error: &SqlError) {
    let reason = match error {
        SqlError::DriverUnavailable { reason, .. } => reason.clone(),
        other => other.to_string(),
    };
    *INSTALL_FAILURE.lock().unwrap_or_else(|e| e.into_inner()) = Some(reason);
}

/// Download, verify and install the pinned library into `<root>/drivers`. An installed library
/// is kept unless `force`. Returns the path of the cached library.
pub async fn install(
    settings: &Settings,
    force: bool,
    reporter: &dyn Reporter,
) -> Result<PathBuf, SqlError> {
    let root = settings.root()?;
    let Some(asset) = install::current_asset() else {
        return Err(unavailable(format!(
            "DuckDB publishes no library for this platform; set MRSM_DUCKDB_LIB to the path of one (v{}.{} or newer)",
            ffi::MIN_VERSION.0,
            ffi::MIN_VERSION.1
        )));
    };
    let target = cache_dir(root).join(lib_file());
    if !force && target.is_file() {
        return Ok(target);
    }
    reporter.start(VERSION, asset.size);
    let result = install::install_from(
        root,
        &install::release_url(asset),
        asset.sha256,
        asset.size,
        lib_file(),
        force,
        reporter,
    )
    .await;
    match &result {
        Ok(path) => {
            // An explicit install repairs an earlier failure.
            *INSTALL_FAILURE.lock().unwrap_or_else(|e| e.into_inner()) = None;
            reporter.finish(Ok(path))
        }
        Err(e) => reporter.finish(Err(e)),
    }
    result.map_err(|e| unavailable(format!("could not install the DuckDB library: {e}")))
}

/// The size of the pinned download, in megabytes (for prompts and messages).
pub fn download_megabytes() -> Option<u64> {
    install::current_asset().map(install::size_mb)
}

/// Remove the cached libraries (`<root>/drivers/duckdb`). Returns whether anything was removed.
pub fn uninstall(settings: &Settings) -> Result<bool, SqlError> {
    let dir = drivers_dir(settings.root()?);
    if !dir.exists() {
        return Ok(false);
    }
    std::fs::remove_dir_all(&dir)
        .map_err(|e| unavailable(format!("could not remove '{}': {e}", dir.display())))?;
    Ok(true)
}

/// Where the library would come from and, when it loads, its version. A library named by
/// `MRSM_DUCKDB_LIB` or installed in the cache is loaded to read its version (nothing is ever
/// installed). One found only on the system library path is reported by its path and **not
/// loaded**: `show drivers` never runs the initializers of a library nobody asked for.
pub fn status(settings: &Settings) -> Status {
    if ffi::loaded().is_none() && candidate(settings).is_none() {
        return match crate::client_libs::find_system_library(lib_file()) {
            Some(path) => Status {
                source: Some(Source::System),
                path: Some(path.display().to_string()),
                version: None,
                error: None,
            },
            None => Status {
                source: None,
                path: None,
                version: None,
                error: None,
            },
        };
    }
    match load_existing(settings) {
        Ok(Some(l)) => Status {
            source: Some(l.source),
            path: Some(l.path),
            version: Some(l.version),
            error: None,
        },
        Ok(None) => Status {
            source: None,
            path: None,
            version: None,
            error: None,
        },
        Err(e) => Status {
            source: None,
            path: None,
            version: None,
            error: Some(match e {
                SqlError::DriverUnavailable { reason, .. } => reason,
                other => other.to_string(),
            }),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> + use<> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }
    }

    #[test]
    fn settings_read_the_environment() {
        // Absolute on every platform (`/x` is not absolute on Windows), and nonexistent so the
        // library path is not canonicalized into another spelling.
        let tmp = tempfile::tempdir().unwrap();
        let tmp_path = std::fs::canonicalize(tmp.path()).unwrap();
        let (root, lib) = (tmp_path.join("no-root"), tmp_path.join("libduckdb.so"));
        let s = Settings::from_env(&env_of(&[
            ("MRSM_ROOT_DIR", root.to_str().unwrap()),
            ("MRSM_DUCKDB_LIB", lib.to_str().unwrap()),
            ("MRSM_NO_AUTO_INSTALL", "True"),
        ]));
        assert_eq!(s.root.as_deref(), Some(root.as_path()));
        assert_eq!(s.lib.as_deref(), Some(lib.as_path()));
        assert!(s.no_auto_install);
        let s = Settings::from_env(&env_of(&[
            ("MRSM_ROOT_DIR", "/r"),
            ("MRSM_NO_AUTO_INSTALL", ""),
            ("MRSM_DUCKDB_LIB", ""),
        ]));
        assert!(!s.no_auto_install);
        assert!(s.lib.is_none());
    }

    #[test]
    fn settings_use_normal_roots_without_a_development_opt_in() {
        let settings = Settings::from_env(&|_| None);
        let expected =
            meerschaum_core::config::Paths::process_dot_config_dir().join(if cfg!(windows) {
                "Meerschaum"
            } else {
                "meerschaum"
            });
        assert_eq!(settings.root, Some(expected));
        let settings = Settings::from_env(&env_of(&[("MRSM_ROOT_DIR", "")]));
        assert_eq!(
            settings.root,
            Some(meerschaum_core::config::paths::resolve_path(Path::new("")))
        );
    }

    #[test]
    fn the_cache_path_is_versioned() {
        let p = cache_dir(Path::new("r"));
        assert!(
            p.ends_with(Path::new("drivers").join("duckdb").join(VERSION)),
            "{p:?}"
        );
    }

    #[test]
    fn lookup_order_is_env_then_cache() {
        let root = tempfile::tempdir().unwrap();
        let mut s = Settings {
            root: Some(root.path().to_path_buf()),
            lib: None,
            no_auto_install: false,
        };
        assert!(candidate(&s).is_none());
        let dir = cache_dir(root.path());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(lib_file()), b"x").unwrap();
        let (source, path) = candidate(&s).unwrap();
        assert_eq!(source, Source::Cache);
        assert_eq!(path, dir.join(lib_file()));
        // The explicit setting wins over an installed cache, even when its file is missing.
        s.lib = Some(root.path().join("absent").join("libduckdb.so"));
        assert_eq!(candidate(&s).unwrap().0, Source::Env);
        // A root that is not known (debug build, no MRSM_ROOT_DIR) has no cache.
        s.lib = None;
        s.root = None;
        assert!(candidate(&s).is_none());
    }

    #[test]
    fn a_relative_library_path_is_made_absolute_before_the_check_and_the_load() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("libduckdb-rel.so"), b"x").unwrap();
        let cwd = std::env::current_dir().unwrap();
        // `absolutize` resolves against the working directory at the time it is called.
        std::env::set_current_dir(dir.path()).unwrap();
        let abs = absolutize(Path::new("libduckdb-rel.so"));
        std::env::set_current_dir(cwd).unwrap();
        assert!(abs.is_absolute(), "{abs:?}");
        assert!(abs.is_file(), "{abs:?}");
        // A path that does not exist still becomes absolute (so the error names the real place).
        assert!(absolutize(Path::new("no/such/lib.so")).is_absolute());
    }

    #[tokio::test]
    async fn a_failed_install_is_remembered_for_the_life_of_the_process() {
        let root = tempfile::tempdir().unwrap();
        let settings = Settings {
            root: Some(root.path().to_path_buf()),
            lib: None,
            no_auto_install: false,
        };
        let _serial = FAILURE_TESTS.lock().await;
        if ffi::loaded().is_some() || probe_system() {
            return;
        }
        remember_install_failure(&unavailable(
            "could not install the DuckDB library: offline".into(),
        ));
        let err = ensure_loaded(&settings).await.unwrap_err().to_string();
        *INSTALL_FAILURE.lock().unwrap() = None;
        assert!(err.contains("offline"), "{err}");
        assert!(err.contains("not retried"), "{err}");
        // No download was attempted: nothing was created under the root.
        assert!(!cache_dir(root.path()).exists());
    }

    #[test]
    fn a_missing_explicit_library_is_an_error_not_a_fallback() {
        let tmp = tempfile::tempdir().unwrap();
        let s = Settings {
            root: None,
            lib: Some(tmp.path().join("not-here").join("libduckdb.so")),
            no_auto_install: false,
        };
        let err = load_existing(&s).unwrap_err().to_string();
        assert!(err.contains("MRSM_DUCKDB_LIB"), "{err}");
        assert!(err.contains("not a file"), "{err}");
    }

    #[test]
    fn the_no_auto_install_message_names_both_fixes() {
        let m = missing_message();
        assert!(m.contains("mrsm install driver duckdb"), "{m}");
        assert!(m.contains("MRSM_DUCKDB_LIB"), "{m}");
        assert!(m.contains("MRSM_NO_AUTO_INSTALL"), "{m}");
    }

    #[test]
    fn uninstall_removes_only_the_driver_cache() {
        let root = tempfile::tempdir().unwrap();
        let s = Settings {
            root: Some(root.path().to_path_buf()),
            ..Settings::default()
        };
        assert!(!uninstall(&s).unwrap());
        std::fs::create_dir_all(cache_dir(root.path())).unwrap();
        std::fs::write(cache_dir(root.path()).join(lib_file()), b"x").unwrap();
        std::fs::create_dir_all(root.path().join("drivers").join("other")).unwrap();
        assert!(uninstall(&s).unwrap());
        assert!(!drivers_dir(root.path()).exists());
        assert!(root.path().join("drivers").join("other").exists());
    }

    #[tokio::test]
    async fn without_a_library_and_with_no_auto_install_the_error_is_clear() {
        // Only meaningful when no library can be found (not set, not cached, not on the system
        // path); otherwise the lookup succeeds, which is also correct.
        let root = tempfile::tempdir().unwrap();
        let s = Settings {
            root: Some(root.path().to_path_buf()),
            lib: None,
            no_auto_install: true,
        };
        let _serial = FAILURE_TESTS.lock().await;
        if ffi::loaded().is_some() || probe_system() {
            return;
        }
        *INSTALL_FAILURE.lock().unwrap() = None;
        let err = ensure_loaded(&s).await.unwrap_err().to_string();
        assert!(err.contains("MRSM_NO_AUTO_INSTALL"), "{err}");
        assert!(!cache_dir(root.path()).exists());
    }

    /// The process root override decides where the cache is looked up and installed, ahead of
    /// `MRSM_ROOT_DIR` (the connector and `install driver` must agree on one root).
    #[test]
    fn the_root_override_replaces_the_environment_root() {
        static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
        let first = tempfile::tempdir().unwrap();
        let second = tempfile::tempdir().unwrap();
        let env = env_of(&[("MRSM_ROOT_DIR", first.path().to_str().unwrap())]);
        // `from_env` is the environment alone; `from_process_env` adds the override.
        assert_eq!(
            Settings::from_env(&env).root.as_deref(),
            Some(std::fs::canonicalize(first.path()).unwrap().as_path())
        );
        set_root_override(Some(second.path().to_path_buf()));
        let s = Settings::from_process_env();
        assert_eq!(s.root.as_deref(), Some(second.path()));
        // The cache the lookup consults is under the override: a library there is a candidate.
        let dir = cache_dir(second.path());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(lib_file()), b"x").unwrap();
        let mut s = s;
        s.lib = None;
        assert_eq!(candidate(&s), Some((Source::Cache, dir.join(lib_file()))));
        // A later change is followed, and `None` goes back to the environment.
        set_root_override(Some(first.path().to_path_buf()));
        assert_eq!(
            Settings::from_process_env().root.as_deref(),
            Some(first.path())
        );
        set_root_override(None);
        assert_eq!(root_override(), None);
    }

    fn probe_system() -> bool {
        crate::client_libs::find_system_library(lib_file()).is_some()
    }

    /// The two tests that use the process-wide install failure must not interleave.
    static FAILURE_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
}
