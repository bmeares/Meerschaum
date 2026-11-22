//! Native Factory constructor-only installation checks. No worker can select these paths.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use meerschaum_server::executor::control::Verifier;
use meerschaum_server::executor::factory_artifacts::{self as artifacts, Manifest};
use meerschaum_server::executor::factory_attestation::{
    Authority, MAX_KEY_FILE, PRIVATE_PATH, Pin,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::Read;
use std::path::PathBuf;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Private {
    pin: Pin,
    pkcs8: String,
}

/// Native constructor proof after fixed-path manifest/material checks, never Deserialize.
pub(crate) struct Installed {
    pub(crate) authority: Authority,
    pub(crate) manifest: Manifest,
}

fn refused() -> String {
    "The protected native Factory response installation is invalid; repair its administrator-provisioned artifact/key binding.".into()
}

#[cfg(target_os = "linux")]
fn metadata(file: &std::fs::File, directory: bool, private: bool) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let metadata = file.metadata().map_err(|_| refused())?;
    let uid = rustix::process::geteuid().as_raw();
    if !(if directory {
        metadata.is_dir()
    } else {
        metadata.is_file()
    }) || ![0, uid].contains(&metadata.uid())
        || metadata.mode() & 0o022 != 0
        || !directory && metadata.nlink() != 1
        || private && (metadata.mode() & 0o077 != 0 || metadata.uid() != uid)
        || !private
            && metadata.uid() == uid
            && metadata.mode() & 0o200 != 0
            && !rustix::fs::fstatvfs(file)
                .map_err(|_| refused())?
                .f_flag
                .contains(rustix::fs::StatVfsMountFlags::RDONLY)
    {
        return Err(refused());
    }
    Ok(())
}

/// Main reads only its administrator-mounted public pin, never desired/ or health.
#[cfg(target_os = "linux")]
pub(crate) fn public_pin(control: &Verifier) -> Result<Option<Pin>, String> {
    let path = format!(
        "{}/{}.json",
        meerschaum_server::executor::factory_attestation::PUBLIC_DIRECTORY,
        control.generation.simple()
    );
    let Some(raw) = bounded(&path, MAX_KEY_FILE, false)? else {
        return Ok(None);
    };
    let pin: Pin = serde_json::from_slice(&raw).map_err(|_| refused())?;
    pin.matches_control(control).map_err(|_| refused())?;
    Ok(Some(pin))
}
#[cfg(not(target_os = "linux"))]
pub(crate) fn public_pin(_: &Verifier) -> Result<Option<Pin>, String> {
    Ok(None)
}

#[cfg(target_os = "linux")]
fn open(path: &str, private: bool) -> Result<Option<std::fs::File>, String> {
    use rustix::fs::{Mode, OFlags};
    if !artifacts::canonical_path(path, true) {
        return Err(refused());
    }
    let mut directory = std::fs::File::from(
        rustix::fs::open(
            "/",
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        )
        .map_err(|_| refused())?,
    );
    metadata(&directory, true, false)?;
    let parts = path[1..].split('/').collect::<Vec<_>>();
    for part in &parts[..parts.len() - 1] {
        directory = match rustix::fs::openat(
            &directory,
            *part,
            OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            Mode::empty(),
        ) {
            Ok(descriptor) => std::fs::File::from(descriptor),
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(_) => return Err(refused()),
        };
        metadata(&directory, true, false)?;
    }
    let file = match rustix::fs::openat(
        &directory,
        *parts.last().ok_or_else(refused)?,
        OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
        Mode::empty(),
    ) {
        Ok(descriptor) => std::fs::File::from(descriptor),
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(_) => return Err(refused()),
    };
    metadata(&file, false, private)?;
    Ok(Some(file))
}

#[cfg(target_os = "linux")]
fn bounded(path: &str, maximum: usize, private: bool) -> Result<Option<Vec<u8>>, String> {
    let Some(file) = open(path, private)? else {
        return Ok(None);
    };
    if file.metadata().map_err(|_| refused())?.len() > maximum as u64 {
        return Err(refused());
    }
    let mut bytes = Vec::new();
    file.take(maximum as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| refused())?;
    if bytes.len() > maximum {
        return Err(refused());
    }
    Ok(Some(bytes))
}

#[cfg(target_os = "linux")]
fn measure(mut file: std::fs::File, expected: &artifacts::File) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let before = file.metadata().map_err(|_| refused())?;
    if before.len() != expected.bytes || expected.bytes > artifacts::MAX_FILE_BYTES {
        return Err(refused());
    }
    let mut bytes = [0u8; 128 * 1024];
    let mut count = 0u64;
    let mut digest = Sha256::new();
    loop {
        let read = file.read(&mut bytes).map_err(|_| refused())?;
        if read == 0 {
            break;
        }
        count = count.checked_add(read as u64).ok_or_else(refused)?;
        if count > expected.bytes {
            return Err(refused());
        }
        digest.update(&bytes[..read]);
    }
    let after = file.metadata().map_err(|_| refused())?;
    let digest = digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if count != expected.bytes
        || digest != expected.sha256
        || (
            before.dev(),
            before.ino(),
            before.len(),
            before.mtime(),
            before.mtime_nsec(),
            before.ctime(),
            before.ctime_nsec(),
        ) != (
            after.dev(),
            after.ino(),
            after.len(),
            after.mtime(),
            after.mtime_nsec(),
            after.ctime(),
            after.ctime_nsec(),
        )
    {
        return Err(refused());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
fn exact_tree(root: &str, expected: &BTreeSet<String>, skip_pycache: bool) -> Result<(), String> {
    use std::os::unix::fs::MetadataExt;
    let mut pending = vec![(PathBuf::from(root), 0usize)];
    let mut found = BTreeSet::new();
    let mut nodes = 0usize;
    while let Some((directory, depth)) = pending.pop() {
        if depth > 64 {
            return Err(refused());
        }
        let metadata = std::fs::symlink_metadata(&directory).map_err(|_| refused())?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.mode() & 0o022 != 0
            || metadata.uid() == rustix::process::geteuid().as_raw() && metadata.mode() & 0o200 != 0
        {
            return Err(refused());
        }
        for child in std::fs::read_dir(&directory).map_err(|_| refused())? {
            let child = child.map_err(|_| refused())?;
            nodes += 1;
            if nodes > artifacts::MAX_FILES * 2 {
                return Err(refused());
            }
            if skip_pycache && child.file_name() == "__pycache__" {
                continue;
            }
            let path = child.path();
            let metadata = std::fs::symlink_metadata(&path).map_err(|_| refused())?;
            if metadata.is_dir() && !metadata.file_type().is_symlink() {
                pending.push((path, depth + 1));
            } else if metadata.is_file() && !metadata.file_type().is_symlink() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|_| refused())?
                    .to_str()
                    .ok_or_else(refused)?;
                if !artifacts::canonical_path(relative, false)
                    || !found.insert(relative.to_string())
                {
                    return Err(refused());
                }
            } else {
                return Err(refused());
            }
        }
    }
    if &found != expected {
        return Err(refused());
    }
    Ok(())
}

/// The signer trusts persisted state below this control directory. Check every directory
/// before Factory initialization creates or chmods anything there.
#[cfg(target_os = "linux")]
fn validate_control_root(path: &std::path::Path) -> Result<(), String> {
    use rustix::fs::{Mode, OFlags};
    use std::os::unix::fs::MetadataExt;
    use std::path::Component;

    let invalid = || {
        "The Factory signer needs a pre-provisioned private control directory under administrator-controlled parents.".to_string()
    };
    let mut components = path.components();
    if components.next() != Some(Component::RootDir) || path.parent().is_none() {
        return Err(invalid());
    }
    let flags = OFlags::PATH | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC;
    let mut directory =
        std::fs::File::from(rustix::fs::open("/", flags, Mode::empty()).map_err(|_| invalid())?);
    let uid = rustix::process::geteuid().as_raw();
    for component in components {
        let Component::Normal(name) = component else {
            return Err(invalid());
        };
        let metadata = directory.metadata().map_err(|_| invalid())?;
        if ![0, uid].contains(&metadata.uid()) || metadata.mode() & 0o022 != 0 {
            return Err(invalid());
        }
        directory = std::fs::File::from(
            rustix::fs::openat(&directory, name, flags, Mode::empty()).map_err(|_| invalid())?,
        );
    }
    let metadata = directory.metadata().map_err(|_| invalid())?;
    if metadata.uid() != uid || metadata.mode() & 0o777 != 0o700 {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(target_os = "linux")]
pub(crate) fn load(
    control: &Verifier,
    compiler: Option<&crate::executor::compiler::Compiler>,
    control_root: &std::path::Path,
) -> Result<Option<Installed>, String> {
    if meerschaum_core::self_exe::native_executable().is_none()
        || meerschaum_core::team_scope::sandbox::current().is_some()
        || std::env::var_os(meerschaum_core::team_scope::sandbox::SPEC_VAR).is_some()
        || std::env::var_os(meerschaum_core::team_scope::api_execution::PROFILE_VAR).is_some()
    {
        return Ok(None);
    }
    let Some(secret) = bounded(PRIVATE_PATH, MAX_KEY_FILE, true)? else {
        return Ok(None);
    };
    validate_control_root(control_root)?;
    // A generation key is not an owner-writable runtime config. The administrator's
    // readonly secret mount (or equivalent protected installation) must enforce this.
    use std::os::unix::fs::OpenOptionsExt;
    match std::fs::OpenOptions::new()
        .write(true)
        .custom_flags(
            rustix::fs::OFlags::NOFOLLOW.bits() as i32
                | rustix::fs::OFlags::NONBLOCK.bits() as i32
                | rustix::fs::OFlags::CLOEXEC.bits() as i32,
        )
        .open(PRIVATE_PATH)
    {
        Ok(_) => return Err(refused()),
        Err(error)
            if error.kind() == std::io::ErrorKind::PermissionDenied
                || error.raw_os_error() == Some(rustix::io::Errno::ROFS.raw_os_error()) => {}
        Err(_) => return Err(refused()),
    }
    let material: Private = serde_json::from_slice(&secret).map_err(|_| refused())?;
    material
        .pin
        .matches_control(control)
        .map_err(|_| refused())?;
    let raw = bounded(
        artifacts::MANIFEST_PATH,
        artifacts::MAX_MANIFEST_BYTES,
        false,
    )?
    .ok_or_else(refused)?;
    let manifest = Manifest::decode(&raw).map_err(|_| refused())?;
    if manifest.identity(&raw).map_err(|_| refused())? != material.pin.artifacts
        || manifest.architecture != std::env::consts::ARCH
        || manifest
            .compiler
            .as_ref()
            .map(|compiler| compiler.manifest_sha256.as_str())
            != compiler.map(|compiler| compiler.identity())
    {
        return Err(refused());
    }
    for (path, expected) in manifest.files().map_err(|_| refused())? {
        measure(open(&path, false)?.ok_or_else(refused)?, expected)?;
    }
    // Hash the actual running native executable, not merely a pathname which a
    // differently built process could quote. /proc/self/exe is the kernel-owned link.
    measure(
        std::fs::File::open("/proc/self/exe").map_err(|_| refused())?,
        &manifest.cli.file,
    )?;
    if let Some(sdk) = &manifest.sdk {
        let expected = sdk
            .files
            .keys()
            .filter_map(|path| path.strip_prefix("meerschaum/").map(str::to_string))
            .collect();
        exact_tree(&format!("{}/meerschaum", sdk.root), &expected, true)?;
    }
    if let Some(compiler) = &manifest.compiler {
        exact_tree(
            &compiler.snapshot,
            &compiler.files.keys().cloned().collect(),
            false,
        )?;
    }
    let key = STANDARD.decode(&material.pkcs8).map_err(|_| refused())?;
    let authority = Authority::from_pkcs8(control, material.pin, &key).map_err(|_| refused())?;
    Ok(Some(Installed {
        authority,
        manifest,
    }))
}

#[cfg(not(target_os = "linux"))]
pub(crate) fn load(
    _: &Verifier,
    _: Option<&crate::executor::compiler::Compiler>,
    _: &std::path::Path,
) -> Result<Option<Installed>, String> {
    match std::fs::symlink_metadata(PRIVATE_PATH) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        _ => Err(refused()),
    }
}
