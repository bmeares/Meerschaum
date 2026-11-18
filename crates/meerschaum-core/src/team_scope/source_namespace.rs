//! Native birth-captured executable-source namespace for a credentialed Team launch.
//! It changes no Team root/config/venv/job authority and is not an Api sandbox proof.

use std::collections::BTreeSet;
#[cfg(unix)]
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use serde::{Deserialize, Serialize};

/// Host-selected descriptor path inherited by this signed Team launch.
pub const ENV: &str = "MRSM_TEAM_SOURCE_CONTEXT";
/// Fixed descriptor name inside its private source namespace.
pub const FILE: &str = "team-source-runtime.json";
/// Native interpreter entry point, after its source namespace has been captured.
pub const PYTHON_CHILD: &str = "--_mrsm-team-source-python";
/// Fixed native Factory storage, mounted separately from the Team's durable work root.
pub const FACTORY_CONTROL_ROOT: &str = "/var/lib/meerschaum/factory";
#[cfg(unix)]
const MAX_BYTES: usize = 2 * 1024 * 1024;

/// Discovery paths fixed for one launch, without changing its durable Team root.
/// This is not an API credential, source grant, or confinement proof.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Namespace {
    /// Existing durable Team root; ordinary state, workdir and resources stay here.
    pub root: PathBuf,
    /// Sealed Factory storage, outside the recursively writable durable Team root.
    pub control_root: PathBuf,
    /// Exact selected entry points used by native discovery.
    pub view: PathBuf,
    /// Host-created inert Python package containing the same exact entry points.
    pub package: PathBuf,
    /// Launch-private native and Python discovery cache.
    pub cache: PathBuf,
    /// Main-selected source catalog digest; absence permits only an empty source set.
    pub catalog_sha256: Option<String>,
    /// Exact selected plugin names, also limiting installed entry-point discovery.
    pub names: BTreeSet<String>,
}

/// Host-selected only. This descriptor is never accepted by an API DTO/job property.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Launch {
    /// Descriptor format version, currently one.
    pub schema: u8,
    /// Exact native release shared by the launcher and SDK.
    pub release: String,
    /// Nonsecret fixed discovery paths and immutable catalog selection.
    pub namespace: Namespace,
    /// Administrator-selected executable/import mapping, prepared without running Python.
    pub runtime: super::sandbox::runtime::Runtime,
    /// Exact mandatory command boundary, also installed in the SDK before site hooks.
    pub sandbox: super::sandbox::Spec,
}

static CAPTURED: OnceLock<Result<Option<Launch>, String>> = OnceLock::new();

fn unavailable() -> String {
    "This Team launch has no exact native source namespace. Recreate it through its owning executor rather than selecting plugin paths from a later environment or configuration.".into()
}

fn canonical(path: &Path) -> Result<(), String> {
    if !path.is_absolute()
        || path
            .components()
            .any(|part| matches!(part, std::path::Component::ParentDir))
        || std::fs::canonicalize(path).ok().as_deref() != Some(path)
    {
        return Err(unavailable());
    }
    Ok(())
}

impl Launch {
    /// Digest the complete fixed descriptor transferred to the SDK before site hooks.
    pub fn digest(&self) -> Result<String, String> {
        use sha2::{Digest, Sha256};
        let bytes = serde_json::to_vec(self).map_err(|_| unavailable())?;
        Ok(Sha256::digest(&bytes)
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect())
    }

    /// Read a bounded private regular descriptor and validate its canonical fixed layout.
    pub fn read(path: &Path) -> Result<Self, String> {
        canonical(path)?;
        if path.file_name() != Some(std::ffi::OsStr::new(FILE)) {
            return Err(unavailable());
        }
        #[cfg(unix)]
        let file = {
            std::fs::File::from(
                rustix::fs::open(
                    path,
                    rustix::fs::OFlags::RDONLY
                        | rustix::fs::OFlags::NOFOLLOW
                        | rustix::fs::OFlags::NONBLOCK
                        | rustix::fs::OFlags::CLOEXEC,
                    rustix::fs::Mode::empty(),
                )
                .map_err(|_| unavailable())?,
            )
        };
        #[cfg(not(unix))]
        return Err(
            "A signed Team source namespace requires supported native Unix launchers.".into(),
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = file.metadata().map_err(|_| unavailable())?;
            if !metadata.is_file()
                || metadata.len() > MAX_BYTES as u64
                || metadata.permissions().mode() & 0o077 != 0
            {
                return Err(unavailable());
            }
            let mut bytes = Vec::new();
            file.take(MAX_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| unavailable())?;
            if bytes.len() > MAX_BYTES {
                return Err(unavailable());
            }
            let launch: Self = serde_json::from_slice(&bytes).map_err(|_| unavailable())?;
            launch.validate(path.parent().ok_or_else(unavailable)?)?;
            Ok(launch)
        }
    }

    #[cfg(unix)]
    fn validate(&self, directory: &Path) -> Result<(), String> {
        let selected = &self.namespace;
        canonical(&selected.root)?;
        canonical(&selected.control_root)?;
        canonical(directory)?;
        for path in [&selected.view, &selected.package, &selected.cache] {
            canonical(path)?;
        }
        let generation = directory
            .file_name()
            .and_then(|part| part.to_str())
            .and_then(|part| part.strip_prefix("team-sources-"))
            .and_then(|part| uuid::Uuid::parse_str(part).ok())
            .filter(|id| !id.is_nil());
        let context = directory.parent().ok_or_else(unavailable)?;
        let id = context
            .file_name()
            .and_then(|part| part.to_str())
            .and_then(|part| uuid::Uuid::parse_str(part).ok())
            .filter(|id| !id.is_nil());
        if self.schema != 1
            || self.release != env!("CARGO_PKG_VERSION")
            || self.runtime.api_only
            || self.runtime.native_compiler.is_some()
            || generation.is_none()
            || id.is_none()
            || selected.root.starts_with(&selected.control_root)
            || selected.control_root.starts_with(&selected.root)
            || context.parent() != Some(selected.control_root.join("execution-contexts").as_path())
            || selected.view != directory.join("plugins")
            || selected.package != directory.join("package/plugins")
            || selected.cache != directory.join("cache")
            || selected.names.len() > 256
            || selected.names.iter().any(|name| {
                name.is_empty()
                    || name.len() > 128
                    || !name
                        .bytes()
                        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
            })
            || selected.catalog_sha256.as_ref().is_some_and(|digest| {
                digest.len() != 64
                    || !digest
                        .bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
            || selected.catalog_sha256.is_none() && !selected.names.is_empty()
            || self.sandbox.mode != super::sandbox::Mode::RequireSockets
            || self.sandbox.runtime.as_ref() != Some(&self.runtime)
            || self.sandbox.canary != super::sandbox::canary_path(&selected.control_root)
            || !self.sandbox.protected.contains(&selected.control_root)
            || !self
                .sandbox
                .allowed(self.sandbox.rules.clone())
                .1
                .is_empty()
        {
            return Err(unavailable());
        }
        canonical(&self.runtime.launcher)?;
        if self.runtime.launcher.starts_with(&selected.root) || !self.runtime.python.is_absolute() {
            return Err(unavailable());
        }
        Ok(())
    }
}

/// Called at native command birth before discovery. Captures absence too, so a plugin cannot
/// later select a source namespace by changing its process environment.
pub fn capture_native() -> Result<(), String> {
    let result = CAPTURED.get_or_init(|| {
        std::env::var_os(ENV)
            .filter(|path| !path.is_empty())
            .map(|path| {
                let launch = Launch::read(Path::new(&path))?;
                let report = super::sandbox::current().ok_or_else(unavailable)?;
                report
                    .require_sockets_verdict()
                    .map_err(|_| unavailable())?;
                let spec = std::env::var_os(super::sandbox::SPEC_VAR).ok_or_else(unavailable)?;
                if super::sandbox::Spec::read(Path::new(&spec)).ok().as_ref()
                    != Some(&launch.sandbox)
                {
                    return Err(unavailable());
                }
                Ok(launch)
            })
            .transpose()
    });
    result.as_ref().map(|_| ()).map_err(Clone::clone)
}

/// Called by the same-release native trampoline before site hooks, using its already-captured
/// fixed descriptor path. Later calls cannot replace the first namespace, including absence.
pub fn capture_sdk(path: &Path, expected_sha256: &str) -> Result<(), String> {
    let launch = Launch::read(path)?;
    if launch.digest()? != expected_sha256 {
        return Err(unavailable());
    }
    if CAPTURED
        .get()
        .is_some_and(|captured| !matches!(captured, Ok(Some(current)) if current == &launch))
    {
        return Err(unavailable());
    }
    // The SDK's Rust state is distinct from the CLI's. Reapply the same immutable
    // boundary before importing site hooks; a later environment cannot choose its rules.
    super::sandbox::enforce(&launch.sandbox, &[])
        .require_sockets_verdict()
        .map_err(|_| unavailable())?;
    let captured = CAPTURED.get_or_init(|| Ok(Some(launch.clone())));
    match captured {
        Ok(Some(current)) if current == &launch => Ok(()),
        _ => Err(unavailable()),
    }
}

/// Return the Team launch captured before plugin imports.
pub fn launch() -> Result<Option<&'static Launch>, String> {
    match CAPTURED.get() {
        None | Some(Ok(None)) => Ok(None),
        Some(Ok(Some(launch))) => Ok(Some(launch)),
        Some(Err(error)) => Err(error.clone()),
    }
}

/// Whether native startup has already fixed a selection, explicit absence, or refusal.
pub fn is_captured() -> bool {
    CAPTURED.get().is_some()
}

/// The descriptor is a scoped source namespace, never a mechanism for changing root.
pub fn for_root(root: &Path) -> Option<&'static Namespace> {
    launch()
        .ok()
        .flatten()
        .map(|launch| &launch.namespace)
        .filter(|namespace| namespace.root == root)
}

impl std::fmt::Debug for Launch {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TeamSourceLaunch")
            .finish_non_exhaustive()
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use crate::team_scope::{create_private_dir, write_private};

    fn fixture(root: &Path) -> (PathBuf, Launch) {
        let work_root = root.join("work");
        let control_root = root.join("control");
        create_private_dir(&work_root).unwrap();
        let directory = control_root
            .join("execution-contexts")
            .join(uuid::Uuid::new_v4().simple().to_string())
            .join(format!("team-sources-{}", uuid::Uuid::new_v4().simple()));
        let namespace = Namespace {
            root: work_root,
            control_root: control_root.clone(),
            view: directory.join("plugins"),
            package: directory.join("package/plugins"),
            cache: directory.join("cache"),
            catalog_sha256: None,
            names: BTreeSet::new(),
        };
        for path in [&namespace.view, &namespace.package, &namespace.cache] {
            create_private_dir(path).unwrap();
        }
        let executable = std::env::current_exe().unwrap().canonicalize().unwrap();
        let runtime = super::super::sandbox::runtime::Runtime {
            api_only: false,
            sdk_pycache_prefix: None,
            native_compiler: None,
            launcher: executable.clone(),
            python: executable,
            environment: Vec::new(),
            packages: Default::default(),
            site_paths: Vec::new(),
            prefix: None,
        };
        let launch = Launch {
            schema: 1,
            release: env!("CARGO_PKG_VERSION").into(),
            namespace,
            sandbox: super::super::sandbox::Spec {
                mode: super::super::sandbox::Mode::RequireSockets,
                canary: super::super::sandbox::canary_path(&control_root),
                protected: vec![control_root],
                runtime: Some(runtime.clone()),
                ..super::super::sandbox::Spec::default()
            },
            runtime,
        };
        let path = directory.join(FILE);
        write_private(&path, &serde_json::to_string(&launch).unwrap()).unwrap();
        (path, launch)
    }

    #[test]
    fn team_namespaces_preserve_root_and_refuse_aliases_or_cross_profile_descriptors() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (path, launch) = fixture(&root);
        assert!(Launch::read(&path).unwrap() == launch);
        let alias = path.parent().unwrap().join("source-alias.json");
        std::os::unix::fs::symlink(&path, &alias).unwrap();
        assert!(Launch::read(&alias).is_err());
        for change in 0..4 {
            let mut changed = launch.clone();
            match change {
                0 => changed.runtime.api_only = true,
                1 => changed.namespace.package = root.join(".internal/plugins"),
                2 => {
                    changed.namespace.names.insert("uncaptured".into());
                }
                _ => changed.release.push_str("-other"),
            }
            write_private(&path, &serde_json::to_string(&changed).unwrap()).unwrap();
            assert!(Launch::read(&path).is_err());
        }
    }

    #[test]
    #[allow(unsafe_code)]
    fn native_birth_capture_survives_later_environment_and_sdk_reselection() {
        const CHILD: &str = "MRSM_TEST_SOURCE_NAMESPACE_CHILD";
        if let Ok(mode) = std::env::var(CHILD) {
            let original =
                PathBuf::from(std::env::var("MRSM_TEST_SOURCE_NAMESPACE_ORIGINAL").unwrap());
            let other = PathBuf::from(std::env::var("MRSM_TEST_SOURCE_NAMESPACE_OTHER").unwrap());
            let original_launch = Launch::read(&original).unwrap();
            let other_launch = Launch::read(&other).unwrap();
            if mode == "present" {
                assert!(
                    capture_native().is_err(),
                    "an unconfined source namespace must fail before discovery"
                );
                assert!(launch().is_err());
                return;
            }
            capture_native().unwrap();
            // SAFETY: dedicated subprocess with one selected test; no worker/runtime is started.
            unsafe {
                std::env::set_var(ENV, &other);
            }
            capture_native().unwrap();
            if mode == "present" {
                assert!(launch().unwrap().unwrap() == &original_launch);
                assert!(capture_sdk(&other, &other_launch.digest().unwrap()).is_err());
                let root = original_launch.namespace.root.clone();
                let paths = crate::config::Paths::from_env(
                    &|name| match name {
                        "MRSM_ROOT_DIR" => Some(root.display().to_string()),
                        "MRSM_PLUGINS_DIR" => {
                            Some(other_launch.namespace.view.display().to_string())
                        }
                        _ => None,
                    },
                    &root,
                    false,
                    None,
                )
                .unwrap();
                assert_eq!(paths.root_dir, root);
                assert_eq!(paths.plugins_dirs, vec![original_launch.namespace.view]);
            } else {
                assert!(launch().unwrap().is_none());
                assert!(capture_sdk(&other, &other_launch.digest().unwrap()).is_err());
            }
            return;
        }
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().canonicalize().unwrap();
        let (original, _) = fixture(&root);
        let (other, _) = fixture(&root);
        for mode in ["present", "absent"] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command.args(["--exact","team_scope::source_namespace::tests::native_birth_capture_survives_later_environment_and_sdk_reselection","--nocapture","--test-threads=1"])
                .env(CHILD,mode).env("MRSM_TEST_SOURCE_NAMESPACE_ORIGINAL",&original)
                .env("MRSM_TEST_SOURCE_NAMESPACE_OTHER",&other)
                .env_remove("TMUX").env_remove("TMUX_PANE").env_remove(ENV);
            if mode == "present" {
                command.env(ENV, &original);
            }
            let output = command.output().unwrap();
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
}
