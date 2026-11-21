//! Fixed administrator installation input; never a public/runtime configuration selector.
//!
//! This loader opens no worker/compiler process. A native server calls it once before its
//! first private context/cache read. A missing policy deliberately selects no compiler;
//! a present invalid policy fails startup, without an ambient toolchain fallback.

use crate::executor::compiler::Compiler;
#[cfg(target_os = "linux")]
use serde::Deserialize;
#[cfg(target_os = "linux")]
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;

#[cfg(target_os = "linux")]
const DIRECTORY: &[&str] = &["etc", "meerschaum"];
#[cfg(target_os = "linux")]
const POLICY: &str = "native-runtime.json";
#[cfg(target_os = "linux")]
const MAX_POLICY: usize = 16 * 1024;
#[cfg(target_os = "linux")]
const SNAPSHOT_PREFIX: &str = "/usr/local/lib/meerschaum/compiler";

#[cfg(target_os = "linux")]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Descriptor {
    schema: u8,
    release: String,
    architecture: String,
    compiler: Selection,
}

#[cfg(target_os = "linux")]
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Selection {
    snapshot: PathBuf,
    manifest_sha256: String,
    anchor: PathBuf,
}

fn refused() -> String {
    "The administrator's native runtime policy is invalid; repair its protected installation before starting this server."
        .into()
}

#[cfg(target_os = "linux")]
fn absolute(path: &Path) -> bool {
    path.is_absolute()
        && path
            .to_str()
            .is_some_and(|text| text.len() <= 4096 && !text.chars().any(char::is_control))
        && path
            .components()
            .all(|part| !matches!(part, Component::CurDir | Component::ParentDir))
}

#[cfg(target_os = "linux")]
impl Descriptor {
    fn validate(&self) -> Result<(), String> {
        if self.schema != 1
            || self.release != env!("CARGO_PKG_VERSION")
            || self.architecture != std::env::consts::ARCH
            || !absolute(&self.compiler.snapshot)
            || !absolute(&self.compiler.anchor)
            || !meerschaum_server::executor::plugins::valid_digest(&self.compiler.manifest_sha256)
        {
            return Err(refused());
        }
        Ok(())
    }
    fn open(self) -> Result<Arc<Compiler>, String> {
        self.validate()?;
        // Compiler::open verifies exact compiler.json, every listed toolchain/vendor/lock
        // byte, absence of unlisted files/links, frozen modes, real native binaries, exact
        // generated source lock shape and the config-free system anchor. No tool is run.
        let compiler = Compiler::open(
            &self.compiler.snapshot,
            &self.compiler.manifest_sha256,
            &self.compiler.anchor,
        )
        .map_err(|_| refused())?;
        native_architecture(&compiler)?;
        Ok(Arc::new(compiler))
    }
}

#[cfg(target_os = "linux")]
fn native_architecture(compiler: &Compiler) -> Result<(), String> {
    use rustix::fs::{Mode, OFlags};
    use std::io::Read;
    let expected = match std::env::consts::ARCH {
        "x86_64" => 62u16,
        "aarch64" => 183u16,
        _ => return Err(refused()),
    };
    for name in ["cargo", "rustc"] {
        let mut file = std::fs::File::from(
            rustix::fs::open(
                compiler.toolchain().join("bin").join(name),
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| refused())?,
        );
        let mut header = [0u8; 20];
        file.read_exact(&mut header).map_err(|_| refused())?;
        if &header[..6] != b"\x7fELF\x02\x01"
            || u16::from_le_bytes([header[18], header[19]]) != expected
        {
            return Err(refused());
        }
    }
    Ok(())
}

/// A native host may only choose the fixed, protected installation. Paths from config,
/// environment, signed HTTP updates, source declarations and job metadata never reach here.
pub(crate) fn load() -> Result<Option<Arc<Compiler>>, String> {
    // Ineligible children/embedders do not inspect the host installation at all. This
    // preserves a CodeTeam's nested server and ordinary Python entry compatibility.
    // These checks only remove compiler selection; no marker can enable or choose it.
    // Private Api sessions retain their independent sticky ConfinedNone/kernel ceiling.
    if meerschaum_core::self_exe::native_executable().is_none()
        || meerschaum_core::team_scope::sandbox::current().is_some()
        || std::env::var_os(meerschaum_core::team_scope::sandbox::SPEC_VAR).is_some()
        || std::env::var_os(meerschaum_core::team_scope::api_execution::PROFILE_VAR).is_some()
    {
        return Ok(None);
    }
    #[cfg(not(target_os = "linux"))]
    {
        // Confined compilation is currently a Linux capability. A configured policy on
        // another platform must not look like a working native compiler installation.
        match std::fs::symlink_metadata("/etc/meerschaum/native-runtime.json") {
            Ok(_) => Err(refused()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(_) => Err(refused()),
        }
    }
    #[cfg(target_os = "linux")]
    {
        use rustix::fs::{Mode, OFlags};
        let mut directory = std::fs::File::from(
            rustix::fs::open(
                "/",
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| refused())?,
        );
        trusted_directory(&directory)?;
        for name in DIRECTORY {
            let Some(child) = child_directory(&directory, name)? else {
                return Ok(None);
            };
            directory = child;
        }
        let Some(bytes) = descriptor_bytes(&directory)? else {
            return Ok(None);
        };
        // The runtime can select only a snapshot installed under the fixed protected
        // prefix. A policy cannot point at Team sources, former mutable launch state,
        // an owner home or an arbitrary filesystem tree.
        let prefix = Path::new(SNAPSHOT_PREFIX);
        let mut installed = std::fs::File::from(
            rustix::fs::open(
                "/",
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| refused())?,
        );
        immutable_directory(&installed)?;
        for component in prefix.components().filter_map(|part| match part {
            Component::Normal(name) => name.to_str(),
            _ => None,
        }) {
            installed = child_directory(&installed, component)?.ok_or_else(refused)?;
            immutable_directory(&installed)?;
        }
        select(&bytes, prefix, &installed).map(Some)
    }
}

#[cfg(target_os = "linux")]
fn select(
    bytes: &[u8],
    installed_prefix: &Path,
    installed: &std::fs::File,
) -> Result<Arc<Compiler>, String> {
    if bytes.len() > MAX_POLICY {
        return Err(refused());
    }
    let descriptor: Descriptor = serde_json::from_slice(bytes).map_err(|_| refused())?;
    descriptor.validate()?;
    immutable_directory(installed)?;
    let relative = descriptor
        .compiler
        .snapshot
        .strip_prefix(installed_prefix)
        .map_err(|_| refused())?;
    if relative.as_os_str().is_empty() {
        return Err(refused());
    }
    let mut directory = installed.try_clone().map_err(|_| refused())?;
    for component in relative.components() {
        let Component::Normal(name) = component else {
            return Err(refused());
        };
        let name = name.to_str().ok_or_else(refused)?;
        directory = child_directory(&directory, name)?.ok_or_else(refused)?;
        immutable_directory(&directory)?;
    }
    descriptor.open()
}

#[cfg(target_os = "linux")]
fn trusted_directory(directory: &std::fs::File) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let metadata = directory.metadata().map_err(|_| refused())?;
    let uid = rustix::process::geteuid().as_raw();
    if !metadata.is_dir() || metadata.mode() & 0o022 != 0 || ![0, uid].contains(&metadata.uid()) {
        return Err(refused());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn immutable_directory(directory: &std::fs::File) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    trusted_directory(directory)?;
    let metadata = directory.metadata().map_err(|_| refused())?;
    if metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o200 != 0 {
        return Err(refused());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn child_directory(parent: &std::fs::File, name: &str) -> Result<Option<std::fs::File>, String> {
    use rustix::fs::{Mode, OFlags};
    let descriptor = match rustix::fs::openat(
        parent,
        name,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(descriptor) => descriptor,
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(_) => return Err(refused()),
    };
    let directory = std::fs::File::from(descriptor);
    trusted_directory(&directory)?;
    Ok(Some(directory))
}

#[cfg(target_os = "linux")]
fn descriptor_bytes(parent: &std::fs::File) -> Result<Option<Vec<u8>>, String> {
    use rustix::fs::{Mode, OFlags};
    use std::io::Read;
    use std::os::unix::fs::MetadataExt;
    let descriptor = match rustix::fs::openat(
        parent,
        POLICY,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(descriptor) => descriptor,
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(_) => return Err(refused()),
    };
    let file = std::fs::File::from(descriptor);
    let metadata = file.metadata().map_err(|_| refused())?;
    let uid = rustix::process::geteuid().as_raw();
    if !metadata.is_file()
        || metadata.len() > MAX_POLICY as u64
        || metadata.mode() & 0o222 != 0
        || ![0, uid].contains(&metadata.uid())
    {
        return Err(refused());
    }
    let mut bytes = Vec::new();
    file.take(MAX_POLICY as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| refused())?;
    if bytes.len() > MAX_POLICY {
        return Err(refused());
    }
    Ok(Some(bytes))
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeMap;
    use std::os::unix::fs::PermissionsExt;

    struct Fixture {
        root: tempfile::TempDir,
        directory: std::fs::File,
        snapshot: PathBuf,
        identity: String,
    }
    impl Fixture {
        fn new() -> Self {
            Self::with_machine(match std::env::consts::ARCH {
                "aarch64" => 183,
                _ => 62,
            })
        }
        fn with_machine(machine: u16) -> Self {
            let root = tempfile::tempdir().unwrap();
            let snapshot = root.path().join("snapshot");
            std::fs::create_dir(&snapshot).unwrap();
            let mut cargo = vec![0; 20];
            cargo[..6].copy_from_slice(b"\x7fELF\x02\x01");
            cargo[18..20].copy_from_slice(&machine.to_le_bytes());
            let mut rustc = cargo.clone();
            cargo.extend_from_slice(b"fixture-cargo");
            rustc.extend_from_slice(b"fixture-rustc");
            let files = BTreeMap::from([
                // No binary is executed. ELF-marked distinct fixtures test installation
                // selection/pin validation; actual native compilation has its own fixture.
                ("toolchain/bin/cargo", cargo.as_slice()),
                ("toolchain/bin/rustc", rustc.as_slice()),
                ("vendor/fixture/src/lib.rs", b"pub fn fixture() {}".as_slice()),
                ("Cargo.lock", b"version = 4\n[[package]]\nname = \"mrsm_native_source\"\nversion = \"0.0.0\"\n".as_slice()),
            ]);
            let mut hashes = BTreeMap::new();
            for (name, bytes) in files {
                let path = snapshot.join(name);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                std::fs::write(&path, bytes).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();
                hashes.insert(name, meerschaum_server::executor::plugins::sha256(bytes));
            }
            let manifest = serde_json::to_vec(&json!({"schema":1,"files":hashes})).unwrap();
            std::fs::write(snapshot.join("compiler.json"), &manifest).unwrap();
            std::fs::set_permissions(
                snapshot.join("compiler.json"),
                std::fs::Permissions::from_mode(0o444),
            )
            .unwrap();
            let identity = meerschaum_server::executor::plugins::sha256(&manifest);
            for path in [
                "toolchain/bin",
                "toolchain",
                "vendor/fixture/src",
                "vendor/fixture",
                "vendor",
                "",
            ] {
                std::fs::set_permissions(
                    snapshot.join(path),
                    std::fs::Permissions::from_mode(0o555),
                )
                .unwrap();
            }
            let directory = std::fs::File::open(root.path()).unwrap();
            Self {
                root,
                directory,
                snapshot,
                identity,
            }
        }
        fn value(&self) -> serde_json::Value {
            json!({"schema":1,"release":env!("CARGO_PKG_VERSION"),"architecture":std::env::consts::ARCH,
                "compiler":{"snapshot":self.snapshot,"manifest_sha256":self.identity,"anchor":"/usr/lib"}})
        }
        fn policy(&self, value: serde_json::Value) {
            std::fs::set_permissions(self.root.path(), std::fs::Permissions::from_mode(0o700))
                .unwrap();
            let path = self.root.path().join(POLICY);
            if path.exists() {
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
            }
            std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o444)).unwrap();
            std::fs::set_permissions(self.root.path(), std::fs::Permissions::from_mode(0o555))
                .unwrap();
        }
        fn load(&self) -> Result<Option<Arc<Compiler>>, String> {
            descriptor_bytes(&self.directory)?
                .map(|bytes| select(&bytes, self.root.path(), &self.directory))
                .transpose()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            // Only this fixture's own frozen directories are changed for TempDir cleanup.
            let _ =
                std::fs::set_permissions(self.root.path(), std::fs::Permissions::from_mode(0o755));
            for path in [
                "",
                "toolchain",
                "toolchain/bin",
                "vendor",
                "vendor/fixture",
                "vendor/fixture/src",
            ] {
                let _ = std::fs::set_permissions(
                    self.snapshot.join(path),
                    std::fs::Permissions::from_mode(0o755),
                );
            }
        }
    }

    #[test]
    fn missing_policy_selects_none_and_a_valid_file_checks_the_entire_snapshot() {
        let fixture = Fixture::new();
        assert!(fixture.load().unwrap().is_none());
        fixture.policy(fixture.value());
        if rustix::process::geteuid().is_root() {
            // The current compiler anchor deliberately refuses root-writable ancestors.
            assert!(fixture.load().is_err());
            return;
        }
        let compiler = fixture.load().unwrap().unwrap();
        assert_eq!(compiler.identity(), fixture.identity);
        assert_eq!(compiler.anchor(), Path::new("/usr/lib"));
    }

    #[test]
    fn configured_bad_release_architecture_paths_or_pin_never_become_none() {
        let fixture = Fixture::new();
        for mutate in [0, 1, 2, 3, 4, 5, 6] {
            let mut value = fixture.value();
            match mutate {
                0 => value["release"] = json!("0.0.0"),
                1 => value["architecture"] = json!("not-this-architecture"),
                2 => value["compiler"]["manifest_sha256"] = json!("0".repeat(64)),
                3 => value["compiler"]["snapshot"] = json!("relative/snapshot"),
                4 => value["compiler"]["anchor"] = json!("/usr/../usr/lib"),
                5 => value["compiler"]["cargo"] = json!("/untrusted/wrapper"),
                _ => value["compiler"]["snapshot"] = json!("/usr/lib"),
            }
            fixture.policy(value);
            assert!(
                fixture.load().is_err(),
                "policy mutation {mutate} was accepted"
            );
        }
        // Matching descriptor architecture cannot disguise different, correctly
        // manifest-pinned compiler ELF inputs. No executable is started here.
        let wrong = Fixture::with_machine(if std::env::consts::ARCH == "aarch64" {
            62
        } else {
            183
        });
        wrong.policy(wrong.value());
        assert!(wrong.load().is_err());
    }

    #[test]
    fn changed_vendor_bytes_and_unlisted_cargo_config_refuse_the_snapshot() {
        let fixture = Fixture::new();
        fixture.policy(fixture.value());
        if rustix::process::geteuid().is_root() {
            return;
        }
        assert!(fixture.load().is_ok());
        let vendor = fixture.snapshot.join("vendor/fixture/src/lib.rs");
        std::fs::set_permissions(&vendor, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::write(&vendor, "changed source").unwrap();
        std::fs::set_permissions(&vendor, std::fs::Permissions::from_mode(0o444)).unwrap();
        assert!(fixture.load().is_err());
        std::fs::set_permissions(&vendor, std::fs::Permissions::from_mode(0o644)).unwrap();
        std::fs::write(&vendor, "pub fn fixture() {}").unwrap();
        std::fs::set_permissions(&vendor, std::fs::Permissions::from_mode(0o444)).unwrap();
        std::fs::set_permissions(&fixture.snapshot, std::fs::Permissions::from_mode(0o755))
            .unwrap();
        std::fs::write(
            fixture.snapshot.join("config.toml"),
            "[build]\nrustc='wrapper'",
        )
        .unwrap();
        std::fs::set_permissions(
            fixture.snapshot.join("config.toml"),
            std::fs::Permissions::from_mode(0o444),
        )
        .unwrap();
        std::fs::set_permissions(&fixture.snapshot, std::fs::Permissions::from_mode(0o555))
            .unwrap();
        assert!(fixture.load().is_err());
    }

    #[test]
    fn policy_links_fifo_and_writable_installation_are_refused_without_waiting() {
        let fixture = Fixture::new();
        fixture.policy(fixture.value());
        let policy = fixture.root.path().join(POLICY);
        std::fs::set_permissions(&policy, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(fixture.load().is_err());
        std::fs::set_permissions(fixture.root.path(), std::fs::Permissions::from_mode(0o700))
            .unwrap();
        std::fs::remove_file(&policy).unwrap();
        std::os::unix::fs::symlink(fixture.snapshot.join("compiler.json"), &policy).unwrap();
        assert!(fixture.load().is_err());
        std::fs::remove_file(&policy).unwrap();
        rustix::fs::mknodat(
            &fixture.directory,
            POLICY,
            rustix::fs::FileType::Fifo,
            rustix::fs::Mode::RUSR,
            0,
        )
        .unwrap();
        let start = std::time::Instant::now();
        assert!(fixture.load().is_err());
        assert!(start.elapsed() < std::time::Duration::from_secs(1));
        let child = fixture.root.path().join("mutable");
        std::fs::create_dir(&child).unwrap();
        std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o777)).unwrap();
        assert!(child_directory(&fixture.directory, "mutable").is_err());
        std::fs::set_permissions(&child, std::fs::Permissions::from_mode(0o755)).unwrap();
        let child_file = std::fs::File::open(&child).unwrap();
        trusted_directory(&child_file).unwrap();
        assert!(immutable_directory(&child_file).is_err());
        std::os::unix::fs::symlink(&fixture.snapshot, fixture.root.path().join("alias")).unwrap();
        assert!(child_directory(&fixture.directory, "alias").is_err());
    }
}
