//! Source plugins: a plugin directory that is a Cargo crate (`plugin.toml` + `Cargo.toml` +
//! `src/`) and ships no up-to-date `lib<name>.<ext>`. Meerschaum builds it with
//! `cargo build --release` into `<root>/cache/native_build/<name>` (the plugins directory stays
//! clean and may be read-only) and loads the library from there.
//!
//! A build is skipped while the stamp written after the last successful build equals the
//! fingerprint of the sources (the contents of every file in the crate directory except
//! `target/`, hidden entries other than `.cargo/`, and shared libraries) and the library still
//! exists.
use crate::{Result, error};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::SystemTime,
};

/// A source plugin whose library still has to be built.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceBuild {
    /// The `cargo` executable to run.
    pub cargo: PathBuf,
    /// The crate's `Cargo.toml`.
    pub manifest_path: PathBuf,
    /// The Cargo target directory (`<root>/cache/native_build/<name>`).
    pub target_dir: PathBuf,
    /// Shared dependency cache for generated single-file plugins.
    #[serde(default)]
    pub shared_target: Option<PathBuf>,
    /// The source fingerprint to stamp after a successful build.
    pub fingerprint: String,
}

/// The directory below the Meerschaum root's `cache` directory that holds the build output.
pub const BUILD_DIR_NAME: &str = "native_build";

const STAMP_NAME: &str = "mrsm-source-fingerprint";
const FAILED_NAME: &str = "mrsm-build-failed";
const FAILURE_TAIL_LINES: usize = 80;

/// Whether `directory` is a Cargo crate (it has a `Cargo.toml`).
pub fn is_source_crate(directory: &Path) -> bool {
    directory.join("Cargo.toml").is_file()
}

/// What `Cargo.toml` says about the library: the name Cargo gives the produced file, and
/// whether `cdylib` is one of the crate types.
#[derive(Debug)]
pub struct CrateInfo {
    pub package_name: String,
    pub library_name: String,
    pub cdylib: bool,
}

pub fn read_crate(directory: &Path) -> Result<CrateInfo> {
    let text = std::fs::read_to_string(directory.join("Cargo.toml")).map_err(error)?;
    let manifest: toml::Value =
        toml::from_str(&text).map_err(|e| error(format!("invalid Cargo.toml: {e}")))?;
    let package_name = manifest
        .get("package")
        .and_then(|p| p.get("name"))
        .and_then(|n| n.as_str())
        .ok_or_else(|| error("Cargo.toml has no [package] name"))?
        .to_owned();
    let lib = manifest.get("lib");
    let library_name = lib
        .and_then(|l| l.get("name"))
        .and_then(|n| n.as_str())
        .unwrap_or(&package_name)
        .replace('-', "_");
    let cdylib = lib
        .and_then(|l| l.get("crate-type").or_else(|| l.get("crate_type")))
        .and_then(|t| t.as_array())
        .is_some_and(|types| types.iter().any(|t| t.as_str() == Some("cdylib")));
    Ok(CrateInfo {
        package_name,
        library_name,
        cdylib,
    })
}

/// The library file Cargo produces in the release profile of `target_dir`.
pub fn built_library(target_dir: &Path, library_name: &str) -> PathBuf {
    target_dir.join("release").join(format!(
        "{}{library_name}.{}",
        std::env::consts::DLL_PREFIX,
        std::env::consts::DLL_EXTENSION
    ))
}

/// The files that make up the source fingerprint, sorted by relative path: the whole crate
/// directory except `target/`, hidden entries (but `.cargo/`, which holds Cargo's config),
/// and shared libraries next to the sources. That covers `include_str!` assets and files a
/// `build.rs` reads. With `include_lock` false the top-level `Cargo.lock` is left out.
fn source_files(directory: &Path, include_lock: bool) -> Result<Vec<PathBuf>> {
    fn walk(root: &Path, dir: &Path, include_lock: bool, out: &mut Vec<PathBuf>) -> Result<()> {
        let mut entries = std::fs::read_dir(dir)
            .map_err(error)?
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(error)?;
        entries.sort_by_key(|e| e.path());
        let top = dir == root;
        for entry in entries {
            let name = entry.file_name().to_string_lossy().into_owned();
            let kind = entry.file_type().map_err(error)?;
            if top
                && ((kind.is_dir() && name == "target") || (!include_lock && name == "Cargo.lock"))
            {
                continue;
            }
            if name.starts_with('.') && !(top && name == ".cargo") {
                continue;
            }
            if top
                && entry
                    .path()
                    .extension()
                    .is_some_and(|e| e == std::env::consts::DLL_EXTENSION)
            {
                continue;
            }
            if kind.is_symlink() {
                return Err(error("symlinks in native plugin trees are unsupported"));
            }
            if kind.is_dir() {
                walk(root, &entry.path(), include_lock, out)?;
            } else if kind.is_file() {
                out.push(entry.path());
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(directory, directory, include_lock, &mut files)?;
    Ok(files)
}

/// SHA-256 of the relative paths and contents of the source files. Content based, so it does
/// not change when a checkout rewrites mtimes.
pub fn source_fingerprint(directory: &Path) -> Result<String> {
    hash_sources(directory, true)
}

fn hash_sources(directory: &Path, include_lock: bool) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(b"mrsm-source-plugin-v1\0");
    for file in source_files(directory, include_lock)? {
        let rel = file
            .strip_prefix(directory)
            .map_err(error)?
            .to_string_lossy()
            .replace('\\', "/");
        let bytes = std::fs::read(&file).map_err(error)?;
        hash.update(rel.as_bytes());
        hash.update([0]);
        hash.update((bytes.len() as u64).to_le_bytes());
        hash.update(&bytes);
    }
    Ok(hex::encode(hash.finalize()))
}

/// Whether a build input (not `plugin.toml`) was modified after `library`. Used only to
/// decide whether a library shipped next to the sources is stale.
pub fn sources_newer_than(directory: &Path, library: &Path) -> bool {
    let Ok(built) = std::fs::metadata(library).and_then(|m| m.modified()) else {
        return true;
    };
    let Ok(files) = source_files(directory, true) else {
        return false;
    };
    files
        .iter()
        .filter(|f| f.file_name().is_none_or(|n| n != "plugin.toml"))
        .filter_map(|f| std::fs::metadata(f).and_then(|m| m.modified()).ok())
        .any(|modified: SystemTime| modified > built)
}

/// Whether the build in `target_dir` was made from the sources with `fingerprint`.
pub fn build_is_current(target_dir: &Path, library: &Path, fingerprint: &str) -> bool {
    library.is_file()
        && std::fs::read_to_string(target_dir.join(STAMP_NAME))
            .is_ok_and(|stamp| stamp.trim() == fingerprint)
}

/// The `cargo` to run: `$CARGO` when set (and present), otherwise `cargo` found on `PATH`.
pub fn find_cargo() -> Option<PathBuf> {
    find_cargo_in(
        std::env::var_os("CARGO"),
        std::env::var_os("PATH").as_deref(),
    )
}

fn find_cargo_in(cargo_env: Option<OsString>, path: Option<&std::ffi::OsStr>) -> Option<PathBuf> {
    if let Some(value) = cargo_env.filter(|v| !v.is_empty()) {
        let candidate = PathBuf::from(&value);
        if candidate.components().count() > 1 {
            return candidate.is_file().then_some(candidate);
        }
        return search_path(&value, path);
    }
    search_path(std::ffi::OsStr::new("cargo"), path)
}

fn search_path(program: &std::ffi::OsStr, path: Option<&std::ffi::OsStr>) -> Option<PathBuf> {
    let suffix = std::env::consts::EXE_SUFFIX;
    for dir in std::env::split_paths(path?) {
        let mut name = program.to_os_string();
        if !suffix.is_empty() && !name.to_string_lossy().ends_with(suffix) {
            name.push(suffix);
        }
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

/// Build the library. Prints a one-line notice on stderr; on failure the error carries the
/// last lines of cargo's stderr. Returns the fingerprint of the sources after the build:
/// cargo may create or update `Cargo.lock`, which changes the fingerprint, so when nothing but
/// the lock changed during the build the new fingerprint is the one stamped (otherwise the next
/// run would rebuild for nothing). A source edited during the build keeps the old stamp and is
/// rebuilt next time.
pub fn build(
    name: &str,
    spec: &SourceBuild,
    library: &Path,
) -> std::result::Result<String, String> {
    let directory = spec.manifest_path.parent().unwrap_or(Path::new("."));
    let before = hash_sources(directory, false).map_err(|e| e.to_string())?;
    let result = run_cargo(name, spec, library);
    let fingerprint = match hash_sources(directory, false) {
        Ok(after) if after == before => {
            source_fingerprint(directory).unwrap_or_else(|_| spec.fingerprint.clone())
        }
        _ => spec.fingerprint.clone(),
    };
    match result {
        Ok(()) => {
            write_stamp(&spec.target_dir, STAMP_NAME, &fingerprint)
                .map_err(|e| format!("could not write the build stamp: {e}"))?;
            let _ = std::fs::remove_file(spec.target_dir.join(FAILED_NAME));
            let _ = std::fs::remove_file(spec.target_dir.join("mrsm-build-error.txt"));
            Ok(fingerprint)
        }
        Err(reason) => {
            // Remember the failure so the next runs skip the plugin instead of rebuilding.
            let _ = write_stamp(&spec.target_dir, "mrsm-build-error.txt", &reason);
            let _ = write_stamp(&spec.target_dir, FAILED_NAME, &fingerprint);
            Err(reason)
        }
    }
}

fn run_cargo(name: &str, spec: &SourceBuild, library: &Path) -> std::result::Result<(), String> {
    eprintln!("Building native plugin '{name}' (cargo build --release)...");
    let output = Command::new(&spec.cargo)
        .args(["build", "--release", "--manifest-path"])
        .arg(&spec.manifest_path)
        .arg("--target-dir")
        .arg(spec.shared_target.as_ref().unwrap_or(&spec.target_dir))
        .env("CARGO_INCREMENTAL", "0")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .map_err(|e| format!("could not run '{}': {e}", spec.cargo.display()))?;
    if !output.status.success() {
        let directory = spec.manifest_path.parent().unwrap_or(Path::new("."));
        let stderr = String::from_utf8_lossy(&output.stderr);
        // The generated user.rs retains the original source line numbers.
        let stderr =
            if let Ok(original) = std::fs::read_to_string(directory.join("mrsm-source-path.txt")) {
                stderr
                    .replace(
                        &format!("{}:", directory.join("user.rs").display()),
                        "user.rs:",
                    )
                    .replace("user.rs:", &format!("{original}:"))
            } else {
                stderr.into_owned()
            };
        let lines: Vec<&str> = stderr.lines().collect();
        let tail = lines[lines.len().saturating_sub(FAILURE_TAIL_LINES)..].join("\n");
        return Err(format!(
            "`cargo build --release` failed ({}):\n{tail}",
            output.status
        ));
    }
    if let Some(shared) = &spec.shared_target {
        let built = shared
            .join("release")
            .join(library.file_name().ok_or("missing library name")?);
        std::fs::create_dir_all(&spec.target_dir).map_err(|e| e.to_string())?;
        std::fs::create_dir_all(library.parent().ok_or("missing library parent")?)
            .map_err(|e| e.to_string())?;
        let temp = library.with_extension(format!("{}.tmp", std::process::id()));
        std::fs::copy(&built, &temp)
            .and_then(|_| std::fs::rename(&temp, library))
            .map_err(|e| format!("could not install built plugin: {e}"))?;
    }
    if !library.is_file() {
        return Err(format!(
            "`cargo build --release` succeeded but produced no '{}' (the crate must build a cdylib)",
            library.display()
        ));
    }
    Ok(())
}

/// Write `<target_dir>/<file>` atomically (a unique temporary sibling, then rename).
fn write_stamp(target_dir: &Path, file: &str, content: &str) -> std::io::Result<()> {
    std::fs::create_dir_all(target_dir)?;
    let temp = target_dir.join(format!("{file}.{}.tmp", std::process::id()));
    std::fs::write(&temp, content)
        .and_then(|()| std::fs::rename(&temp, target_dir.join(file)))
        .inspect_err(|_| {
            let _ = std::fs::remove_file(&temp);
        })
}

/// Whether the last build of exactly these sources failed (deleting the build directory or
/// editing the sources clears it).
pub fn build_failed_before(target_dir: &Path, fingerprint: &str) -> bool {
    std::fs::read_to_string(target_dir.join(FAILED_NAME))
        .is_ok_and(|stamp| stamp.trim() == fingerprint)
}

/// Generate a single-file plugin crate in the cache, leaving the user's source untouched.
/// Embed the host-matched row runtime; do not compile the host SDK into simple plugins.
pub fn prepare_single_file(file: &Path, name: &str, directory: &Path) -> Result<()> {
    crate::single_file::prepare(file, name, directory)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cargo_lookup_honors_the_cargo_variable_then_path() {
        let dir = tempfile::tempdir().unwrap();
        let exe = dir
            .path()
            .join(format!("cargo{}", std::env::consts::EXE_SUFFIX));
        std::fs::write(&exe, "").unwrap();
        let path = std::env::join_paths([dir.path()]).unwrap();
        assert_eq!(find_cargo_in(None, Some(&path)), Some(exe.clone()));
        assert_eq!(
            find_cargo_in(Some(exe.clone().into_os_string()), None),
            Some(exe)
        );
        let empty = tempfile::tempdir().unwrap();
        let empty_path = std::env::join_paths([empty.path()]).unwrap();
        assert_eq!(find_cargo_in(None, Some(&empty_path)), None);
        assert_eq!(find_cargo_in(None, None), None);
        assert_eq!(
            find_cargo_in(
                Some(empty.path().join("nope").into_os_string()),
                Some(&path)
            ),
            None
        );
    }

    #[test]
    fn crate_info_reads_names_and_crate_types() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("Cargo.toml"),
            "[package]\nname = \"my-plugin\"\n[lib]\ncrate-type = [\"cdylib\"]\n",
        )
        .unwrap();
        let info = read_crate(dir.path()).unwrap();
        assert_eq!(info.package_name, "my-plugin");
        assert_eq!(info.library_name, "my_plugin");
        assert!(info.cdylib);
        std::fs::write(dir.path().join("Cargo.toml"), "[package]\nname = \"x\"\n").unwrap();
        assert!(!read_crate(dir.path()).unwrap().cdylib);
    }

    #[test]
    fn fingerprint_follows_contents_not_mtimes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("src")).unwrap();
        std::fs::write(dir.path().join("Cargo.toml"), "a").unwrap();
        std::fs::write(dir.path().join("plugin.toml"), "b").unwrap();
        std::fs::write(dir.path().join("src/lib.rs"), "c").unwrap();
        let first = source_fingerprint(dir.path()).unwrap();
        std::fs::write(dir.path().join("src/lib.rs"), "c").unwrap();
        assert_eq!(first, source_fingerprint(dir.path()).unwrap());
        std::fs::create_dir(dir.path().join("target")).unwrap();
        std::fs::write(dir.path().join("target/junk"), "x").unwrap();
        std::fs::create_dir(dir.path().join(".git")).unwrap();
        std::fs::write(dir.path().join(".git/HEAD"), "x").unwrap();
        assert_eq!(first, source_fingerprint(dir.path()).unwrap());
        // Files outside src/ (assets, build.rs inputs, Cargo config) are part of it.
        std::fs::create_dir(dir.path().join("assets")).unwrap();
        std::fs::write(dir.path().join("assets/data.txt"), "1").unwrap();
        let with_asset = source_fingerprint(dir.path()).unwrap();
        assert_ne!(first, with_asset);
        std::fs::write(dir.path().join("assets/data.txt"), "2").unwrap();
        assert_ne!(with_asset, source_fingerprint(dir.path()).unwrap());
        std::fs::create_dir(dir.path().join(".cargo")).unwrap();
        std::fs::write(dir.path().join(".cargo/config.toml"), "").unwrap();
        assert_ne!(with_asset, source_fingerprint(dir.path()).unwrap());
        std::fs::write(dir.path().join("src/lib.rs"), "d").unwrap();
        assert_ne!(first, source_fingerprint(dir.path()).unwrap());
    }
}
