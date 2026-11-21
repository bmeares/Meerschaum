//! Host-owned compiler preparation for a confined single-file native source.
//!
//! Only a native host constructor may select this snapshot. Public
//! executor updates, job properties and inherited environment cannot select a compiler,
//! wrapper, vendor directory, lock file, Cargo home or target directory.
//!
//! - [`provisioning`]: the administrator's fixed installation input that selects the compiler.
//! - `runner`: the confined runner process that compiles inside the sandbox.

pub(crate) mod provisioning;
mod runner;
pub(crate) use runner::Runner;

use meerschaum_core::team_scope::{create_private_dir, write_private};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

const MAX_MANIFEST: usize = 2 * 1024 * 1024;
const MAX_FILES: usize = 16_384;
const MAX_SNAPSHOT: u64 = 1024 * 1024 * 1024;
const MAX_LOCK: usize = 1024 * 1024;
const LOCK_PACKAGE: &str = "mrsm_native_source";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema: u8,
    files: BTreeMap<String, String>,
}

/// The paths become trusted only after the host verifies the entire immutable snapshot.
/// Fields are private so no deserialized child/config/HTTP value can construct this proof.
pub(crate) struct Compiler {
    root: PathBuf,
    identity: String,
    lock: toml::Table,
    anchor: PathBuf,
}

/// Written outside the generation's writable state by its trusted native preparer. The
/// eventual runtime must require the sealed native Api/Metadata provenance before using it.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Prepared {
    pub identity: String,
    pub owner_deadline: i64,
    pub state: PathBuf,
    pub cargo: PathBuf,
    pub rustc: PathBuf,
    pub vendor: PathBuf,
    pub cargo_home: PathBuf,
    pub cwd: PathBuf,
    pub lock_template: String,
    /// Existing lock files are writable individually; their parent/config stay read-only.
    pub lock_files: Vec<PathBuf>,
}

fn failure() -> String {
    "The owned native compiler snapshot is incomplete or invalid.".into()
}

fn digest(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[cfg(unix)]
fn relative(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 1024
        && !path.contains('\\')
        && !path.chars().any(char::is_control)
        && path.split('/').all(|part| !matches!(part, "" | "." | ".."))
        && !Path::new(path).is_absolute()
}

#[cfg(not(unix))]
fn regular_file(_: &Path, _: &str, _: u64) -> Result<std::fs::File, String> {
    Err("The confined native compiler requires a supported Unix host.".into())
}

#[cfg(unix)]
fn regular_file(root: &Path, path: &str, limit: u64) -> Result<std::fs::File, String> {
    if !relative(path) {
        return Err(failure());
    }
    let mut parent = root.to_path_buf();
    let mut components = path.split('/').peekable();
    while let Some(component) = components.next() {
        parent.push(component);
        if components.peek().is_some()
            && !std::fs::symlink_metadata(&parent)
                .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
        {
            return Err(failure());
        }
    }
    let fd = rustix::fs::open(
        parent,
        rustix::fs::OFlags::RDONLY
            | rustix::fs::OFlags::NOFOLLOW
            | rustix::fs::OFlags::NONBLOCK
            | rustix::fs::OFlags::CLOEXEC,
        rustix::fs::Mode::empty(),
    )
    .map_err(|_| failure())?;
    let file = std::fs::File::from(fd);
    let metadata = file.metadata().map_err(|_| failure())?;
    if !metadata.is_file() || metadata.len() > limit {
        return Err(failure());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o222 != 0 {
            return Err("The native compiler snapshot must be read-only.".into());
        }
    }
    Ok(file)
}

fn read(root: &Path, path: &str, limit: usize) -> Result<Vec<u8>, String> {
    use std::io::Read;
    let mut bytes = Vec::new();
    regular_file(root, path, limit as u64)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| failure())?;
    if bytes.len() > limit {
        return Err(failure());
    }
    Ok(bytes)
}

impl Compiler {
    pub(crate) fn identity(&self) -> &str {
        &self.identity
    }

    pub(crate) fn anchor(&self) -> &Path {
        &self.anchor
    }

    /// `identity` is selected by the administrator/native constructor, never a request.
    /// The snapshot contains toolchain/{bin,lib,...}, vendor/* and a root Cargo.lock
    /// template whose sole local package is mrsm_native_source version 0.0.0.
    pub(crate) fn open(root: &Path, identity: &str, anchor: &Path) -> Result<Self, String> {
        let anchor = compiler_anchor(anchor)?;
        if !meerschaum_server::executor::plugins::valid_digest(identity)
            || !std::fs::symlink_metadata(root)
                .is_ok_and(|metadata| metadata.is_dir() && !metadata.file_type().is_symlink())
        {
            return Err(failure());
        }
        let root = root.canonicalize().map_err(|_| failure())?;
        let manifest_bytes = read(&root, "compiler.json", MAX_MANIFEST)?;
        if digest(&manifest_bytes) != identity {
            return Err(failure());
        }
        let manifest: Manifest = serde_json::from_slice(&manifest_bytes).map_err(|_| failure())?;
        if manifest.schema != 1
            || manifest.files.is_empty()
            || manifest.files.len() > MAX_FILES
            || !["toolchain/bin/cargo", "toolchain/bin/rustc", "Cargo.lock"]
                .iter()
                .all(|path| manifest.files.contains_key(*path))
            || !manifest
                .files
                .keys()
                .any(|path| path.starts_with("vendor/"))
            || manifest.files.get("toolchain/bin/cargo")
                == manifest.files.get("toolchain/bin/rustc")
        {
            return Err(failure());
        }
        // A directory grant must not make an unlisted file (including Cargo config or a
        // replacement library) readable. The operator freezes every directory before this
        // constructor; later administrator changes require a new snapshot/identity.
        let mut directories = vec![(root.clone(), 0usize)];
        let mut entries = 0usize;
        while let Some((directory, depth)) = directories.pop() {
            if depth > 32 {
                return Err(failure());
            }
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if std::fs::symlink_metadata(&directory)
                    .map_err(|_| failure())?
                    .permissions()
                    .mode()
                    & 0o222
                    != 0
                {
                    return Err(
                        "The native compiler snapshot directories must be read-only.".into(),
                    );
                }
            }
            for entry in std::fs::read_dir(&directory).map_err(|_| failure())? {
                let entry = entry.map_err(|_| failure())?;
                entries += 1;
                if entries > MAX_FILES * 2 {
                    return Err(failure());
                }
                let kind = entry.file_type().map_err(|_| failure())?;
                if kind.is_dir() {
                    directories.push((entry.path(), depth + 1));
                } else if kind.is_file() {
                    let path = entry.path();
                    let relative = path
                        .strip_prefix(&root)
                        .ok()
                        .and_then(Path::to_str)
                        .ok_or_else(failure)?;
                    if relative != "compiler.json" && !manifest.files.contains_key(relative) {
                        return Err(failure());
                    }
                } else {
                    return Err(failure());
                }
            }
        }
        let mut total = 0u64;
        for (path, expected) in &manifest.files {
            if !(path.starts_with("toolchain/")
                || path.starts_with("vendor/")
                || path == "Cargo.lock")
                || !meerschaum_server::executor::plugins::valid_digest(expected)
            {
                return Err(failure());
            }
            use std::io::Read;
            let mut file = regular_file(&root, path, MAX_SNAPSHOT.saturating_sub(total))?;
            let mut hasher = Sha256::new();
            let mut buffer = [0u8; 64 * 1024];
            loop {
                let count = file.read(&mut buffer).map_err(|_| failure())?;
                if count == 0 {
                    break;
                }
                total = total
                    .checked_add(count as u64)
                    .filter(|total| *total <= MAX_SNAPSHOT)
                    .ok_or_else(failure)?;
                hasher.update(&buffer[..count]);
            }
            let actual: String = hasher
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect();
            if &actual != expected {
                return Err(failure());
            }
        }
        // Require native executables. The distinct pinned Cargo/rustc files above also
        // reject rustup's same-binary multicall shims; the administrator selects the
        // actual audited toolchain, rather than an arbitrary executable wrapper.
        for executable in ["toolchain/bin/cargo", "toolchain/bin/rustc"] {
            use std::io::Read;
            let mut magic = [0u8; 4];
            regular_file(&root, executable, MAX_SNAPSHOT)?
                .read_exact(&mut magic)
                .map_err(|_| failure())?;
            if magic != *b"\x7fELF" {
                return Err(failure());
            }
        }
        let lock: toml::Table = toml::from_str(
            std::str::from_utf8(&read(&root, "Cargo.lock", MAX_LOCK)?).map_err(|_| failure())?,
        )
        .map_err(|_| failure())?;
        let packages = lock
            .get("package")
            .and_then(toml::Value::as_array)
            .ok_or_else(failure)?;
        let local: Vec<_> = packages
            .iter()
            .filter(|package| package.get("source").is_none())
            .collect();
        if local.len() != 1
            || local[0].get("name").and_then(toml::Value::as_str) != Some(LOCK_PACKAGE)
            || local[0].get("version").and_then(toml::Value::as_str) != Some("0.0.0")
        {
            return Err(failure());
        }
        Ok(Self {
            root,
            identity: identity.into(),
            lock,
            anchor,
        })
    }

    pub(crate) fn toolchain(&self) -> PathBuf {
        self.root.join("toolchain")
    }
    pub(crate) fn vendor(&self) -> PathBuf {
        self.root.join("vendor")
    }

    /// No target environment variable is created. SourceBuild keeps its explicit private
    /// --target-dir, and the per-generation Cargo home is never shared with another job.
    pub(crate) fn prepare(
        &self,
        generation: &Path,
        state: &Path,
        owner_deadline: i64,
    ) -> Result<Prepared, String> {
        if !(1..=60).contains(&(owner_deadline - chrono::Utc::now().timestamp())) {
            return Err("The compiler requires its sealed bounded native owner deadline.".into());
        }
        if state.parent() != Some(generation)
            || !matches!(
                state.file_name().and_then(|name| name.to_str()),
                Some("state" | ".mrsm_team_work")
            )
        {
            return Err(failure());
        }
        // The private state has a recursive write grant. Cargo configuration must live
        // outside it, where a selected Python source cannot rewrite a wrapper/settings
        // before the native source builder runs. The generation itself is read-only.
        let cargo_home = generation.join("native-cargo-home");
        create_private_dir(&cargo_home).map_err(|_| failure())?;
        // Cargo discovers project configuration from CWD/ancestors, not --manifest-path.
        // The constructor-selected system anchor and every ancestor are config-free and
        // unwritable to this worker; a private directory below /tmp or HOME is insufficient.
        let cwd = compiler_anchor(&self.anchor)?;
        let rustc = self.toolchain().join("bin/rustc");
        let vendor = self.vendor();
        let config = toml::to_string(&serde_json::json!({
            "net":{"offline":true},
            "source":{"crates-io":{"replace-with":"mrsm-owned-vendor"},
                "mrsm-owned-vendor":{"directory":vendor}},
            "build":{"rustc":rustc,"jobs":2},
            "cache":{"auto-clean-frequency":"never"}
        }))
        .map_err(|_| failure())?;
        let config_file = cargo_home.join("config.toml");
        write_private(&config_file, &config).map_err(|_| failure())?;
        // These are the two package-cache locks used by the pinned Cargo snapshot. The
        // actual confined fixture must prove it needs no broader Cargo-home writes;
        // another Cargo version requires its own snapshot and acceptance proof.
        let lock_files = [".package-cache", ".package-cache-mutate"]
            .iter()
            .map(|name| cargo_home.join(name))
            .collect::<Vec<_>>();
        for lock in &lock_files {
            write_private(lock, "").map_err(|_| failure())?;
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&config_file, std::fs::Permissions::from_mode(0o400))
                .map_err(|_| failure())?;
            // Keep the directory removable by its trusted host lifecycle. Its immutable
            // child view comes from the generation's mandatory Read grant, not chmod;
            // only the two existing lock files receive separate Write grants.
        }
        Ok(Prepared {
            identity: self.identity.clone(),
            owner_deadline,
            state: state.to_path_buf(),
            cargo: self.toolchain().join("bin/cargo"),
            rustc,
            vendor,
            cargo_home,
            cwd,
            lock_template: toml::to_string(&self.lock).map_err(|_| failure())?,
            lock_files,
        })
    }
}

fn compiler_anchor(anchor: &Path) -> Result<PathBuf, String> {
    let canonical = anchor.canonicalize().map_err(|_| failure())?;
    if canonical != anchor || !canonical.is_absolute() {
        return Err(failure());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let uid = rustix::process::geteuid().as_raw();
        for path in canonical.ancestors() {
            let metadata = std::fs::symlink_metadata(path).map_err(|_| failure())?;
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.mode() & 0o022 != 0
                || (metadata.uid() == uid && metadata.mode() & 0o200 != 0)
                || std::fs::symlink_metadata(path.join(".cargo")).is_ok()
            {
                return Err("The compiler CWD and its ancestors must be config-free and unwritable to the worker.".into());
            }
        }
        Ok(canonical)
    }
    #[cfg(not(unix))]
    {
        Err("The confined compiler anchor requires a supported Unix host.".into())
    }
}

impl Prepared {
    /// Add only these existing-file grants to the mandatory native Api/Metadata spec.
    /// Never grant the Cargo home directory writable: that would admit mutable config.
    pub(crate) fn lock_rules(&self) -> Vec<meerschaum_core::team_scope::sandbox::Rule> {
        use meerschaum_core::team_scope::sandbox::{Access, Rule};
        self.lock_files
            .iter()
            .map(|path| {
                Rule::new(
                    path,
                    Access::ReadWrite,
                    "this generation's pinned compiler package-cache lock",
                )
            })
            .collect()
    }

    /// Rename only the lock template's generated root package; all dependency versions and
    /// checksums stay fixed. The source builder must use --frozen with this private file.
    pub(crate) fn source_lock(&self, name: &str) -> Result<String, String> {
        if !meerschaum_server::executor::plugins::valid_name(name) {
            return Err(failure());
        }
        let mut lock: toml::Table = toml::from_str(&self.lock_template).map_err(|_| failure())?;
        let packages = lock
            .get_mut("package")
            .and_then(toml::Value::as_array_mut)
            .ok_or_else(failure)?;
        let mut found = false;
        for package in packages {
            if package.get("source").is_some() {
                continue;
            }
            if found || package.get("name").and_then(toml::Value::as_str) != Some(LOCK_PACKAGE) {
                return Err(failure());
            }
            package
                .as_table_mut()
                .ok_or_else(failure)?
                .insert("name".into(), name.into());
            found = true;
        }
        if !found {
            return Err(failure());
        }
        toml::to_string(&lock).map_err(|_| failure())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::fs::PermissionsExt;

    #[test]
    fn a_read_only_cargo_leaf_does_not_hide_a_writable_ancestor() {
        let root = tempfile::tempdir().unwrap();
        let leaf = root.path().join("compiler-cwd");
        std::fs::create_dir(&leaf).unwrap();
        std::fs::set_permissions(&leaf, std::fs::Permissions::from_mode(0o500)).unwrap();
        assert!(compiler_anchor(&leaf.canonicalize().unwrap()).is_err());
    }
}
