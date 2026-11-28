//! Optional target archives are stamped with the owner, version and digest of the canonical
//! source archive. Replacing a source archive even at the same version invalidates old binaries.
use std::{
    fs,
    io::{self, Read, Write},
    path::{Component, Path, PathBuf},
};

use bytes::Bytes;
use meerschaum_core::Value;
use sha2::{Digest, Sha256};

use crate::error::ApiError;
use meerschaum_connector_api::plugin_artifacts::{MAX_ARCHIVE_BYTES, valid_source_digest};

const MAX_TARGETS: usize = 64;
const MAX_EXPANDED: u64 = 256 * 1024 * 1024;

/// Names on the legacy repository route may include dots. They must fit one filename.
pub fn validate_name(name: &str) -> Result<(), ApiError> {
    if name.is_empty()
        || name.len() > 200
        || name == "."
        || name == ".."
        || name.contains(['/', '\\', ':'])
        || name.chars().any(char::is_control)
    {
        return Err(ApiError::new(422, "Invalid plugin name."));
    }
    Ok(())
}

fn io_error(err: io::Error) -> ApiError {
    ApiError::exception("Could not access the plugin repository", err)
}

/// Serialize publishers and deletes across server processes sharing the same archive directory.
/// A small fixed set of lock files bounds storage regardless of how many names are requested.
pub async fn lock(directory: &Path, name: &str, write: bool) -> Result<Option<fs::File>, ApiError> {
    let bucket = Sha256::digest(name.as_bytes())[0];
    let locks = directory.join(".registry-locks");
    let file = tokio::task::spawn_blocking(move || {
        let path = locks.join(format!("{bucket:02x}"));
        if write {
            fs::create_dir_all(locks)?;
            fs::File::options()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)
                .map(Some)
        } else {
            match fs::File::open(path) {
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                other => other.map(Some),
            }
        }
    })
    .await
    .map_err(|e| ApiError::exception("Plugin registry lock failed", e))?
    .map_err(io_error)?;
    let Some(file) = file else {
        return Ok(None);
    };
    // Waiting with a blocking OS lock would occupy a Tokio blocking thread. Other publishers
    // need that same pool to finish their archive writes and release the lock.
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        let result = if write {
            file.try_lock()
        } else {
            file.try_lock_shared()
        };
        match result {
            Ok(()) => return Ok(Some(file)),
            Err(fs::TryLockError::WouldBlock) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(std::time::Duration::from_millis(25)).await
            }
            Err(fs::TryLockError::WouldBlock) => {
                return Err(ApiError::unavailable(
                    "The plugin repository is busy. Retry this request.",
                ));
            }
            Err(fs::TryLockError::Error(error)) => return Err(io_error(error)),
        }
    }
}

pub fn directory(source: &Path) -> PathBuf {
    source
        .parent()
        .expect("plugin archive directory")
        .join(".targets")
        .join(source.file_name().expect("plugin archive name"))
}

/// Hold the previous source and targets until the accompanying registry deletion commits.
/// A failed or cancelled deletion restores both before releasing the publisher lock.
pub struct Removal {
    temporary: Option<tempfile::TempDir>,
    moved: Vec<(PathBuf, PathBuf)>,
}

impl Removal {
    pub fn stage(source: &Path) -> io::Result<Self> {
        let temporary = tempfile::tempdir_in(source.parent().expect("plugin archive directory"))?;
        let mut removal = Self {
            temporary: Some(temporary),
            moved: Vec::new(),
        };
        for (label, original) in [
            ("source", source.to_path_buf()),
            ("targets", directory(source)),
        ] {
            let saved = removal
                .temporary
                .as_ref()
                .expect("staged removal")
                .path()
                .join(label);
            match fs::rename(&original, &saved) {
                Ok(()) => removal.moved.push((original, saved)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        Ok(removal)
    }

    pub fn commit(mut self) {
        self.moved.clear();
    }
}

impl Drop for Removal {
    fn drop(&mut self) {
        for (original, saved) in self.moved.drain(..).rev() {
            if let Err(e) = fs::rename(&saved, &original) {
                // Preserve any file we could not restore for administrator recovery.
                if let Some(directory) = self.temporary.take() {
                    let _ = directory.keep();
                }
                tracing::error!("Could not restore a staged plugin archive: {e}");
            }
        }
    }
}

pub async fn stage(
    path: PathBuf,
    prefix: Vec<u8>,
    bytes: Bytes,
) -> Result<tempfile::NamedTempFile, ApiError> {
    tokio::task::spawn_blocking(move || {
        let parent = path
            .parent()
            .ok_or_else(|| io::Error::other("Missing archive directory"))?;
        fs::create_dir_all(parent)?;
        let mut staged = tempfile::NamedTempFile::new_in(parent)?;
        staged.write_all(&prefix)?;
        staged.write_all(&bytes)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            staged
                .as_file()
                .set_permissions(fs::Permissions::from_mode(0o775))?;
        }
        Ok(staged)
    })
    .await
    .map_err(|e| ApiError::exception("Could not stage the archive", e))?
    .map_err(io_error)
}

pub fn commit(staged: tempfile::NamedTempFile, path: &Path) -> Result<(), ApiError> {
    staged
        .persist(path)
        .map(|_| ())
        .map_err(|e| io_error(e.error))
}

pub fn release_key(owner: &Value, version: &str, source_sha256: &str) -> Vec<u8> {
    Sha256::digest(
        serde_json::to_vec(&(owner, version, source_sha256)).expect("serializable plugin identity"),
    )
    .to_vec()
}

pub fn validate_binary(
    name: &str,
    target: &str,
    version: &str,
    bytes: Bytes,
) -> Result<(), String> {
    let extension = if target.split('-').any(|s| s == "windows") {
        "dll"
    } else if target.ends_with("-darwin") {
        "dylib"
    } else {
        "so"
    };
    let library = format!("lib{name}.{extension}");
    let mut seen = std::collections::HashSet::new();
    let mut descriptor_found = false;
    let mut library_found = false;
    let input = flate2::read::GzDecoder::new(&bytes[..]).take(MAX_EXPANDED + 1);
    let mut archive = tar::Archive::new(input);
    let invalid = |_| "Invalid native plugin archive.".to_string();
    for (i, entry) in archive.entries().map_err(invalid)?.enumerate() {
        let mut entry = entry.map_err(invalid)?;
        if i >= 4096 || entry.size() > MAX_EXPANDED {
            return Err("The native plugin archive exceeds its expanded limit.".into());
        }
        let path = entry.path().map_err(invalid)?.into_owned();
        let mut components = path.components();
        let mut first = components.next();
        if first == Some(Component::CurDir) {
            first = components.next();
            if first.is_none() && entry.header().entry_type().is_dir() {
                continue;
            }
        }
        if first != Some(Component::Normal(name.as_ref()))
            || !components
                .clone()
                .all(|c| matches!(c, Component::Normal(_)))
        {
            return Err(
                "The archive must contain only files in the named plugin directory.".into(),
            );
        }
        let path = components.as_path();
        // The receiver may use a case-insensitive filesystem with different separators.
        // Portable names make the descriptor and library we validate unambiguous there too.
        let relative = path
            .to_str()
            .ok_or("Non-UTF-8 native plugin archive path.")?;
        if relative.len() > 1024
            || relative.contains(['\\', ':'])
            || relative.split('/').any(|part| part.ends_with(['.', ' ']))
            || !seen.insert(relative.to_ascii_lowercase())
        {
            return Err("Invalid or duplicate native plugin archive path.".into());
        }
        let kind = entry.header().entry_type();
        if kind.is_dir() {
            continue;
        }
        if !kind.is_file()
            || path.as_os_str().is_empty()
            || relative.eq_ignore_ascii_case("Cargo.toml")
        {
            return Err("Upload a binary-only native package with regular files.".into());
        }
        if path == Path::new("plugin.toml") {
            if entry.size() > 64 * 1024 {
                return Err("Invalid native plugin descriptor.".into());
            }
            let mut text = String::new();
            entry.read_to_string(&mut text).map_err(invalid)?;
            let descriptor: toml::Value =
                toml::from_str(&text).map_err(|_| "Invalid plugin.toml.")?;
            if descriptor.get("name").and_then(|v| v.as_str()) != Some(name)
                || descriptor.get("version").and_then(|v| v.as_str()) != Some(version)
            {
                return Err(
                    "The artifact descriptor must match the registered plugin name and version."
                        .into(),
                );
            }
            descriptor_found = true;
        } else if path == Path::new(&library) {
            if entry.size() == 0 {
                return Err("Invalid native plugin library.".into());
            }
            library_found = true;
        }
    }
    if archive.into_inner().limit() == 0 || !descriptor_found || !library_found {
        return Err(format!(
            "The archive must contain {name}/plugin.toml and {name}/{library} within its expanded limit."
        ));
    }
    Ok(())
}

pub async fn publish(path: PathBuf, key: Vec<u8>, bytes: Bytes) -> Result<(), ApiError> {
    let parent = path.parent().expect("target directory");
    if !path.exists()
        && parent.exists()
        && fs::read_dir(parent)
            .map_err(io_error)?
            .take(MAX_TARGETS)
            .count()
            >= MAX_TARGETS
    {
        return Err(ApiError::new(
            409,
            "This plugin has reached its target archive limit.",
        ));
    }
    let staged = stage(path.clone(), key, bytes).await?;
    commit(staged, &path)
}

/// A missing or superseded target is an ordinary source fallback. A corrupt file is an error.
pub async fn read(path: PathBuf, key: Vec<u8>) -> Result<Option<Vec<u8>>, ApiError> {
    tokio::task::spawn_blocking(move || {
        let mut file = match fs::File::open(path) {
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            other => other?,
        };
        let mut stored = [0u8; 32];
        file.read_exact(&mut stored)?;
        if stored[..] != key {
            return Ok(None);
        }
        let mut archive = Vec::new();
        file.take(MAX_ARCHIVE_BYTES as u64 + 1)
            .read_to_end(&mut archive)?;
        if archive.len() > MAX_ARCHIVE_BYTES {
            return Err(io::Error::other("Oversized native plugin archive"));
        }
        Ok(Some(archive))
    })
    .await
    .map_err(|e| ApiError::exception("Could not read native plugin archive", e))?
    .map_err(io_error)
}

/// Attach an archive after confirming that this publisher owns the canonical source release.
/// The caller holds the plugin's exclusive archive lock throughout the operation.
pub(super) async fn register(
    ctx: &super::Ctx,
    plugin: &meerschaum_connector::PluginRecord,
    owner: &Value,
    target: String,
    archive: Bytes,
) -> Result<(), ApiError> {
    let version = plugin
        .version
        .as_deref()
        .filter(|v| !v.is_empty() && v.len() <= 128)
        .ok_or_else(|| ApiError::new(422, "A target archive needs its registered version."))?;
    let digest = ctx
        .query
        .opt_str("source_sha256")
        .filter(|v| valid_source_digest(v))
        .ok_or_else(|| {
            ApiError::new(
                422,
                "Supply the lowercase SHA-256 of the registered source archive.",
            )
        })?;
    let registered = ctx
        .state
        .instance()
        .get_plugin_version(plugin)
        .await
        .map_err(|e| ApiError::exception("get_plugin_version", e))?;
    if registered.as_deref() != Some(version) {
        return Err(ApiError::new(
            409,
            "The registered plugin version has changed.",
        ));
    }
    let path = super::archive_path(&ctx.state, &plugin.name);
    let source = tokio::fs::read(&path)
        .await
        .map_err(|_| ApiError::new(409, "The registered source archive is unavailable."))?;
    if crate::executor::plugins::sha256(&source) != digest {
        return Err(ApiError::new(
            409,
            "The source archive SHA-256 does not match the registered release.",
        ));
    }
    let key = release_key(owner, version, &digest);
    let target_path = directory(&path).join(format!("native-{target}"));
    let inspected = archive.clone();
    let check_name = plugin.name.clone();
    let check_version = version.to_owned();
    tokio::task::spawn_blocking(move || {
        validate_binary(&check_name, &target, &check_version, inspected)
    })
    .await
    .map_err(|e| ApiError::exception("Native plugin archive validation failed", e))?
    .map_err(|e| ApiError::new(422, e))?;
    publish(target_path, key, archive).await?;
    Ok(())
}

/// Return the registered target only if it still belongs to this exact release.
pub(super) async fn select(
    state: &crate::state::AppState,
    name: &str,
    target: &str,
    source: Vec<u8>,
) -> Result<Vec<u8>, ApiError> {
    let path = directory(&super::archive_path(state, name)).join(format!("native-{target}"));
    if !tokio::fs::try_exists(&path).await.map_err(io_error)? {
        return Ok(source);
    }
    let conn = state.instance();
    let record = super::record(name);
    let owner = conn
        .get_plugin_user_id(&record)
        .await
        .map_err(|e| ApiError::exception("get_plugin_user_id", e))?;
    let version = conn
        .get_plugin_version(&record)
        .await
        .map_err(|e| ApiError::exception("get_plugin_version", e))?;
    if let (Some(owner), Some(version)) = (owner, version) {
        let key = release_key(&owner, &version, &crate::executor::plugins::sha256(&source));
        if let Some(binary) = read(path, key).await? {
            return Ok(binary);
        }
    }
    Ok(source)
}
