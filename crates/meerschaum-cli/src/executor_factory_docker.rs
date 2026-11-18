//! Administrator-owned Docker reads. No image process is started and no archive is extracted.

use crate::dispatch::{CommandSpec, Host};
use crate::executor_factory_image::VerifiedImage;
use meerschaum_server::executor::factory_artifacts::{self as artifacts, Identity, Manifest};
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::time::Duration;

const MAX_MARKER: usize = 16 * 1024;

fn refused() -> String {
    "Could not independently verify the administrator-selected native executor image; no Factory response identity was provisioned.".into()
}

struct Docker<'a> {
    host: &'a mut dyn Host,
    environment: &'a [(String, String)],
}
impl Docker<'_> {
    fn stream<T, F>(
        &mut self,
        arguments: Vec<String>,
        maximum: u64,
        seconds: u64,
        mut consume: F,
    ) -> Result<T, String>
    where
        T: Send,
        F: FnMut(&mut dyn Read) -> Result<T, String> + Send,
    {
        let mut result = None;
        let code = self
            .host
            .output_stream(
                &CommandSpec {
                    program: "docker".into(),
                    args: arguments,
                    env: self.environment.to_vec(),
                    scrub_secrets: true,
                    quiet: true,
                    ..Default::default()
                },
                Duration::from_secs(seconds),
                maximum,
                &mut |stream| {
                    if result.is_some() {
                        return Err(io::Error::other("Repeated native image stream."));
                    }
                    let value = consume(stream);
                    let failed = value.is_err();
                    result = Some(value);
                    if failed {
                        Err(io::Error::other("Native image verification refused."))
                    } else {
                        Ok(())
                    }
                },
            )
            .map_err(|_| refused())?;
        if code != 0 {
            return Err(refused());
        }
        result.ok_or_else(refused)?
    }
    fn bytes(
        &mut self,
        arguments: Vec<String>,
        maximum: usize,
        seconds: u64,
    ) -> Result<Vec<u8>, String> {
        self.stream(arguments, maximum as u64, seconds, |stream| {
            let mut bytes = Vec::new();
            stream
                .take(maximum as u64 + 1)
                .read_to_end(&mut bytes)
                .map_err(|_| refused())?;
            if bytes.len() > maximum {
                return Err(refused());
            }
            Ok(bytes)
        })
    }
    fn remove_owned(&mut self, name: &str, nonce: &str, image: &str) -> Result<(), String> {
        let body = self.bytes(
            vec![
                "container".into(),
                "inspect".into(),
                "--format".into(),
                "{{json .}}".into(),
                name.into(),
            ],
            128 * 1024,
            10,
        )?;
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| refused())?;
        let id = value["Id"]
            .as_str()
            .filter(|value| artifacts::valid_digest(value))
            .ok_or_else(refused)?;
        if value["Name"] != format!("/{name}")
            || value["Image"] != image
            || value["State"]["Running"] != false
            || value["Config"]["Labels"]["mrsm.factory.inspection"] != nonce
        {
            return Err(refused());
        }
        self.bytes(
            vec![
                "container".into(),
                "rm".into(),
                "--volumes".into(),
                id.into(),
            ],
            1024,
            15,
        )?;
        Ok(())
    }
}

/// No fields can be chosen by deserialization or a runtime endpoint. Only this module's
/// protected administrator cache loader can construct a verified-cache input.
pub(crate) struct CacheProof {
    image: String,
    manifest: Vec<u8>,
    identity: Identity,
}
impl CacheProof {
    pub(crate) fn into_parts(self) -> (String, Vec<u8>, Identity) {
        (self.image, self.manifest, self.identity)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Marker {
    schema: u8,
    image: String,
    manifest_sha256: String,
    verified_files: usize,
    verified_bytes: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Selection {
    pub reference: String,
    pub image: String,
    pub identity: Identity,
}

pub(crate) fn selected(root: &Path, reference: &str) -> Result<Option<Selection>, String> {
    let base = crate::executor_factory_install::directory(root);
    match std::fs::symlink_metadata(&base) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(refused()),
        Ok(_) => crate::executor_factory_install::checked_directory(&base, false)?,
    }
    crate::executor_factory_install::checked_directory(base.parent().ok_or_else(refused)?, false)?;
    let path = base.join("selected.json");
    let Some(bytes) = crate::executor_factory_install::read_private(&path, MAX_MARKER)? else {
        return Ok(None);
    };
    let selection: Selection = serde_json::from_slice(&bytes).map_err(|_| refused())?;
    selection.identity.validate().map_err(|_| refused())?;
    if !selection
        .image
        .strip_prefix("sha256:")
        .is_some_and(artifacts::valid_digest)
    {
        return Err(refused());
    }
    let proof = cached(root, &selection.image)?.ok_or_else(refused)?;
    if proof.identity != selection.identity {
        return Err(refused());
    }
    // Rendering a different administrator configuration does not use an old tag's
    // pin or create another key. Its next explicit stack up must inspect that image.
    Ok((selection.reference == reference).then_some(selection))
}

fn image_directory(root: &Path, image: &str) -> Result<PathBuf, String> {
    let digest = image
        .strip_prefix("sha256:")
        .filter(|value| artifacts::valid_digest(value))
        .ok_or_else(refused)?;
    Ok(crate::executor_factory_install::directory(root)
        .join("images")
        .join(digest))
}

fn cached(root: &Path, image: &str) -> Result<Option<CacheProof>, String> {
    let directory = image_directory(root, image)?;
    let base = crate::executor_factory_install::directory(root);
    for path in [base.clone(), base.join("images")] {
        match std::fs::symlink_metadata(&path) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(_) => return Err(refused()),
            Ok(_) => crate::executor_factory_install::checked_directory(&path, false)?,
        }
    }
    crate::executor_factory_install::checked_directory(base.parent().ok_or_else(refused)?, false)?;
    match std::fs::symlink_metadata(&directory) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(refused()),
        Ok(_) => crate::executor_factory_install::checked_directory(&directory, false)?,
    }
    let marker = crate::executor_factory_install::read_private(
        &directory.join("verified.json"),
        MAX_MARKER,
    )?
    .ok_or_else(refused)?;
    let marker: Marker = serde_json::from_slice(&marker).map_err(|_| refused())?;
    let manifest = crate::executor_factory_install::read_private(
        &directory.join("manifest.json"),
        artifacts::MAX_MANIFEST_BYTES,
    )?
    .ok_or_else(refused)?;
    let decoded = Manifest::decode(&manifest).map_err(|_| refused())?;
    let identity = decoded.identity(&manifest).map_err(|_| refused())?;
    let files = decoded.files().map_err(|_| refused())?;
    let total = files
        .values()
        .try_fold(0u64, |total, file| total.checked_add(file.bytes))
        .ok_or_else(refused)?;
    if marker.schema != 1
        || marker.image != image
        || marker.manifest_sha256 != identity.manifest_sha256
        || marker.verified_files != files.len()
        || marker.verified_bytes != total
    {
        return Err(refused());
    }
    let entries = std::fs::read_dir(&directory)
        .map_err(|_| refused())?
        .take(3)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| refused())?;
    if entries.len() != 2
        || entries.iter().any(|entry| {
            !matches!(
                entry.file_name().to_str(),
                Some("verified.json" | "manifest.json")
            )
        })
    {
        return Err(refused());
    }
    Ok(Some(CacheProof {
        image: image.into(),
        manifest,
        identity,
    }))
}

fn save(root: &Path, image: &VerifiedImage) -> Result<(), String> {
    let base = crate::executor_factory_install::directory(root);
    crate::executor_factory_install::checked_directory(base.parent().ok_or_else(refused)?, false)?;
    crate::executor_factory_install::checked_directory(&base, true)?;
    let images = base.join("images");
    crate::executor_factory_install::checked_directory(&images, true)?;
    let mut staging = tempfile::Builder::new();
    staging.prefix(".verify-");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // tempfile directories otherwise use the process umask's ordinary directory
        // mode. The published cache retains this directory, so create it private.
        staging.permissions(std::fs::Permissions::from_mode(0o700));
    }
    let stage = staging.tempdir_in(&images).map_err(|_| refused())?;
    crate::executor_factory_install::checked_directory(stage.path(), false)?;
    let decoded = Manifest::decode(image.manifest()).map_err(|_| refused())?;
    let files = decoded.files().map_err(|_| refused())?;
    let marker = Marker {
        schema: 1,
        image: image.image().into(),
        manifest_sha256: image.identity().manifest_sha256.clone(),
        verified_files: files.len(),
        verified_bytes: files
            .values()
            .try_fold(0u64, |total, file| total.checked_add(file.bytes))
            .ok_or_else(refused)?,
    };
    crate::executor_factory_install::write_new(
        &stage.path().join("manifest.json"),
        image.manifest(),
    )?;
    crate::executor_factory_install::write_new(
        &stage.path().join("verified.json"),
        &serde_json::to_vec(&marker).map_err(|_| refused())?,
    )?;
    let destination = image_directory(root, image.image())?;
    match std::fs::rename(stage.path(), &destination) {
        Ok(()) => {
            let _ = stage.keep();
            Ok(())
        }
        Err(_) => match cached(root, image.image())? {
            Some(existing) if existing.manifest == image.manifest() => Ok(()),
            _ => Err(refused()),
        },
    }
}

pub(crate) fn select(root: &Path, reference: &str, image: &VerifiedImage) -> Result<(), String> {
    let base = crate::executor_factory_install::directory(root);
    crate::executor_factory_install::checked_directory(&base, false)?;
    let path = base.join("selected.json");
    // Present links, malformed state and insecure modes are never repaired by following
    // or overwriting them. New values publish atomically after all generation keys exist.
    let _ = selected(root, reference)?;
    let selection = Selection {
        reference: reference.into(),
        image: image.image().into(),
        identity: image.identity().clone(),
    };
    let mut temporary = tempfile::NamedTempFile::new_in(&base).map_err(|_| refused())?;
    use std::io::Write;
    temporary
        .write_all(&serde_json::to_vec(&selection).map_err(|_| refused())?)
        .map_err(|_| refused())?;
    temporary.as_file().sync_all().map_err(|_| refused())?;
    temporary.persist(path).map_err(|_| refused())?;
    Ok(())
}

pub(crate) fn inspect(
    root: &Path,
    reference: &str,
    environment: &[(String, String)],
    host: &mut dyn Host,
) -> Result<VerifiedImage, String> {
    if meerschaum_core::team_scope::sandbox::current().is_some()
        || std::env::var_os(meerschaum_core::team_scope::sandbox::SPEC_VAR).is_some()
        || std::env::var_os(meerschaum_core::team_scope::api_execution::PROFILE_VAR).is_some()
    {
        return Err(refused());
    }
    let mut docker = Docker { host, environment };
    let raw = docker.bytes(
        vec![
            "image".into(),
            "inspect".into(),
            "--format".into(),
            "{{.Id}}".into(),
            reference.into(),
        ],
        128,
        10,
    )?;
    let image = std::str::from_utf8(&raw)
        .map_err(|_| refused())?
        .trim()
        .to_string();
    if !image
        .strip_prefix("sha256:")
        .is_some_and(artifacts::valid_digest)
    {
        return Err(refused());
    }
    if let Some(proof) = cached(root, &image)? {
        return VerifiedImage::from_cache(proof);
    }
    let nonce = uuid::Uuid::new_v4().simple().to_string();
    let name = format!("mrsm-factory-inspect-{nonce}");
    let result = (|| {
        let created = docker.bytes(
            vec![
                "container".into(),
                "create".into(),
                "--name".into(),
                name.clone(),
                "--label".into(),
                format!("mrsm.factory.inspection={nonce}"),
                "--network".into(),
                "none".into(),
                "--read-only".into(),
                "--user".into(),
                "1000:1000".into(),
                "--entrypoint".into(),
                artifacts::CLI_PATH.into(),
                image.clone(),
                "--version".into(),
            ],
            128,
            15,
        )?;
        let id = std::str::from_utf8(&created).map_err(|_| refused())?.trim();
        if !artifacts::valid_digest(id) {
            return Err(refused());
        }
        let copied = docker.bytes(
            vec![
                "container".into(),
                "cp".into(),
                format!("{id}:{}", artifacts::MANIFEST_PATH),
                "-".into(),
            ],
            artifacts::MAX_MANIFEST_BYTES + 64 * 1024,
            20,
        )?;
        let mut archive = tar::Archive::new(copied.as_slice());
        let mut entries = archive.entries().map_err(|_| refused())?;
        let mut entry = entries.next().ok_or_else(refused)?.map_err(|_| refused())?;
        if !entry.header().entry_type().is_file()
            || entry.size() > artifacts::MAX_MANIFEST_BYTES as u64
        {
            return Err(refused());
        }
        let mut manifest = Vec::new();
        entry.read_to_end(&mut manifest).map_err(|_| refused())?;
        drop(entry);
        if entries.next().is_some() {
            return Err(refused());
        }
        Manifest::decode(&manifest).map_err(|_| refused())?;
        let mut material = Some((image.clone(), manifest));
        docker.stream(
            vec!["container".into(), "export".into(), id.into()],
            16 * 1024 * 1024 * 1024,
            240,
            |reader| {
                let (image, manifest) = material.take().ok_or_else(refused)?;
                VerifiedImage::verify(image, manifest, reader)
            },
        )
    })();
    let cleanup = docker.remove_owned(&name, &nonce, &image);
    match (result, cleanup) {
        (Ok(verified), Ok(())) => {
            save(root, &verified)?;
            Ok(verified)
        }
        // A creation failure may have left a daemon-side object; exact inspection and
        // cleanup are still attempted. Uncertain cleanup is never reported successful.
        _ => Err(refused()),
    }
}
