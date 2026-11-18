//! Host administrator provisioning, separate from Main's public desired deployment.
//! Only independently verified immutable image material can select an installation.

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use meerschaum_server::executor::factory_attestation::{Authority, Pin};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::executor_factory_image::VerifiedImage;
use crate::executor_registry::Registration;

const MAX_INSTALL: usize = 16 * 1024;

/// File-sourced Compose secrets preserve host ownership. The generated Factory service
/// deliberately runs as 1000; an unprivileged provisioner cannot remap a different 0600 owner.
pub(crate) fn provisioner_uid(uid: u32) -> Result<(), String> {
    if uid != 1000 {
        return Err("Container Factory provisioning requires administrator uid 1000 to match the generated 1000:1000 service and private file-sourced secrets. Use a matching administrator installation; no image or key material was created.".into());
    }
    Ok(())
}

pub(crate) fn check_provisioner() -> Result<(), String> {
    #[cfg(unix)]
    {
        provisioner_uid(rustix::process::geteuid().as_raw())
    }
    #[cfg(not(unix))]
    {
        Err("Native Factory provisioning requires protected Unix ownership.".into())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Private {
    pin: Pin,
    pkcs8: String,
}

fn refused() -> String {
    "The protected native Factory installation is unavailable or changed; repair the administrator installation without replacing a live generation's identity.".into()
}

pub(crate) fn directory(root: &Path) -> PathBuf {
    root.join(crate::executor_registry::DIRECTORY)
        .join("attestation")
}

#[cfg(unix)]
pub(crate) fn checked_directory(path: &Path, create: bool) -> Result<(), String> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    if create {
        match std::fs::DirBuilder::new().mode(0o700).create(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return Err(refused()),
        }
    }
    let metadata = std::fs::symlink_metadata(path).map_err(|_| refused())?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
    {
        return Err(refused());
    }
    Ok(())
}

#[cfg(unix)]
pub(crate) fn read_private(path: &Path, maximum: usize) -> Result<Option<Vec<u8>>, String> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(
            rustix::fs::OFlags::NOFOLLOW.bits() as i32
                | rustix::fs::OFlags::NONBLOCK.bits() as i32
                | rustix::fs::OFlags::CLOEXEC.bits() as i32,
        )
        .open(path)
    {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(refused()),
    };
    let metadata = file.metadata().map_err(|_| refused())?;
    if !metadata.is_file()
        || metadata.nlink() != 1
        || metadata.uid() != rustix::process::geteuid().as_raw()
        || metadata.mode() & 0o077 != 0
        || metadata.len() > maximum as u64
    {
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

#[cfg(unix)]
pub(crate) fn write_new(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(
            rustix::fs::OFlags::NOFOLLOW.bits() as i32 | rustix::fs::OFlags::CLOEXEC.bits() as i32,
        )
        .open(path)
        .map_err(|_| refused())?;
    file.write_all(bytes)
        .and_then(|_| file.sync_all())
        .map_err(|_| refused())
}

/// Return the administrator-selected secret file once the installation is verified.
#[cfg(unix)]
pub(crate) fn existing(
    root: &Path,
    record: &Registration,
    image: &str,
    identity: &meerschaum_server::executor::factory_artifacts::Identity,
) -> Result<Option<PathBuf>, String> {
    let base = directory(root);
    checked_directory(base.parent().ok_or_else(refused)?, false)?;
    checked_directory(&base, false)?;
    checked_directory(&base.join("private"), false)?;
    checked_directory(&base.join("public"), false)?;
    let name = record.verifier.generation.simple().to_string();
    let generation = base.join("private").join(&name);
    let public_file = base.join("public").join(format!("{name}.json"));
    match std::fs::symlink_metadata(&generation) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if read_private(&public_file, MAX_INSTALL)?.is_some() {
                return Err(refused());
            }
            return Ok(None);
        }
        Err(_) => return Err(refused()),
        Ok(_) => {}
    }
    checked_directory(&generation, false)?;
    let private_file = generation.join("factory.json");
    let pin = read_private(&public_file, MAX_INSTALL)?.ok_or_else(refused)?;
    let pin: Pin = serde_json::from_slice(&pin).map_err(|_| refused())?;
    let private = read_private(&private_file, MAX_INSTALL)?.ok_or_else(refused)?;
    let private: Private = serde_json::from_slice(&private).map_err(|_| refused())?;
    let key = STANDARD.decode(&private.pkcs8).map_err(|_| refused())?;
    Authority::from_pkcs8(&record.verifier, pin.clone(), &key).map_err(|_| refused())?;
    if private.pin != pin
        || pin.image_sha256.as_deref() != Some(image)
        || &pin.artifacts != identity
        || record.retired
        || record.team_id != record.verifier.team_id
    {
        return Err(refused());
    }
    Ok(Some(private_file))
}

#[cfg(not(unix))]
pub(crate) fn existing(
    _: &Path,
    _: &Registration,
    _: &str,
    _: &meerschaum_server::executor::factory_artifacts::Identity,
) -> Result<Option<PathBuf>, String> {
    Err(refused())
}
#[cfg(not(unix))]
pub(crate) fn checked_directory(_: &Path, _: bool) -> Result<(), String> {
    Err(refused())
}
#[cfg(not(unix))]
pub(crate) fn read_private(_: &Path, _: usize) -> Result<Option<Vec<u8>>, String> {
    Err(refused())
}
#[cfg(not(unix))]
pub(crate) fn write_new(_: &Path, _: &[u8]) -> Result<(), String> {
    Err(refused())
}

#[cfg(unix)]
pub(crate) fn provision(
    root: &Path,
    record: &Registration,
    image: &VerifiedImage,
) -> Result<PathBuf, String> {
    use std::os::unix::fs::OpenOptionsExt;
    if record.retired || record.team_id != record.verifier.team_id {
        return Err(refused());
    }
    record.verifier.validate().map_err(|_| refused())?;
    // The administrator's root is already selected by its native CLI. Refuse links
    // in this installation subtree, including a substituted public/private directory.
    let base = directory(root);
    checked_directory(base.parent().ok_or_else(refused)?, false)?;
    checked_directory(&base, true)?;
    let private = base.join("private");
    let public = base.join("public");
    checked_directory(&private, true)?;
    checked_directory(&public, true)?;
    let lock_path = base.join("lock");
    let lock = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(
            rustix::fs::OFlags::NOFOLLOW.bits() as i32 | rustix::fs::OFlags::CLOEXEC.bits() as i32,
        )
        .open(lock_path)
        .map_err(|_| refused())?;
    {
        use std::os::unix::fs::MetadataExt;
        let metadata = lock.metadata().map_err(|_| refused())?;
        if !metadata.is_file()
            || metadata.nlink() != 1
            || metadata.mode() & 0o077 != 0
            || metadata.uid() != rustix::process::geteuid().as_raw()
        {
            return Err(refused());
        }
    }
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match lock.try_lock() {
            Ok(()) => break,
            Err(std::fs::TryLockError::WouldBlock) if std::time::Instant::now() < until => {
                std::thread::sleep(std::time::Duration::from_millis(20))
            }
            _ => return Err(refused()),
        }
    }
    let name = record.verifier.generation.simple().to_string();
    let generation = private.join(&name);
    let private_file = generation.join("factory.json");
    let public_file = public.join(format!("{name}.json"));
    let existing_pin = read_private(&public_file, MAX_INSTALL)?;
    let generation_exists = match std::fs::symlink_metadata(&generation) {
        Ok(_) => true,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => return Err(refused()),
    };
    let material = if generation_exists {
        checked_directory(&generation, false)?;
        let bytes = read_private(&private_file, MAX_INSTALL)?.ok_or_else(refused)?;
        let material: Private = serde_json::from_slice(&bytes).map_err(|_| refused())?;
        let secret = STANDARD.decode(&material.pkcs8).map_err(|_| refused())?;
        Authority::from_pkcs8(&record.verifier, material.pin.clone(), &secret)
            .map_err(|_| refused())?;
        material
    } else {
        if existing_pin.is_some() {
            return Err(refused());
        }
        let (authority, document) = Authority::generate(
            &record.verifier,
            Some(image.image().into()),
            image.identity().clone(),
        )
        .map_err(|_| refused())?;
        let material = Private {
            pin: authority.pin().clone(),
            pkcs8: STANDARD.encode(document.as_ref()),
        };
        let stage = private.join(format!(".prepare-{}", uuid::Uuid::new_v4().simple()));
        checked_directory(&stage, true)?;
        let result = (|| {
            let bytes = serde_json::to_vec(&material).map_err(|_| refused())?;
            if bytes.len() > MAX_INSTALL {
                return Err(refused());
            }
            write_new(&stage.join("factory.json"), &bytes)?;
            std::fs::rename(&stage, &generation).map_err(|_| refused())
        })();
        if result.is_err() {
            let _ = std::fs::remove_file(stage.join("factory.json"));
            let _ = std::fs::remove_dir(&stage);
        }
        result?;
        material
    };
    if material.pin.artifacts != *image.identity()
        || material.pin.image_sha256.as_deref() != Some(image.image())
    {
        // An administrator image update needs a new control generation, not silent
        // rebinding of a live response key to another executable/compiler installation.
        return Err(refused());
    }
    material
        .pin
        .matches_control(&record.verifier)
        .map_err(|_| refused())?;
    let pin_bytes = serde_json::to_vec(&material.pin).map_err(|_| refused())?;
    match existing_pin {
        Some(bytes)
            if serde_json::from_slice::<Pin>(&bytes).ok().as_ref() == Some(&material.pin) => {}
        Some(_) => return Err(refused()),
        // A crash after the private generation's atomic publication may leave this
        // public copy absent. Reconstruct only from that exact validated original key;
        // missing private material is always a refusal, never key regeneration.
        None => write_new(&public_file, &pin_bytes)?,
    }
    Ok(private_file)
}

#[cfg(not(unix))]
pub(crate) fn provision(_: &Path, _: &Registration, _: &VerifiedImage) -> Result<PathBuf, String> {
    Err("Native Factory installation requires the supported protected Unix file boundary.".into())
}

#[cfg(test)]
mod provisioner_tests {
    #[test]
    fn fixed_compose_identity_requires_matching_administrator_before_any_material() {
        assert!(super::provisioner_uid(1000).is_ok());
        for uid in [0, 1, 1001, u32::MAX] {
            let refused = super::provisioner_uid(uid).unwrap_err();
            assert!(refused.contains("uid 1000"));
            assert!(refused.contains("no image or key material"));
        }
    }
}
