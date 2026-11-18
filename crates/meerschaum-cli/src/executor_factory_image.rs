//! Administrator-only, streaming verification of a never-started image's installed bytes.
//! No archive member is extracted and no image executable is run.

use meerschaum_server::executor::factory_artifacts::{self as artifacts, Identity, Manifest};
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::io::Read;

const MAX_ARCHIVE_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAX_ARCHIVE_NODES: usize = 500_000;

pub(crate) struct VerifiedImage {
    image: String,
    manifest: Vec<u8>,
    identity: Identity,
}

fn refused() -> String {
    "The administrator-selected image has incomplete, mutable or mismatched native Factory material."
        .into()
}

impl VerifiedImage {
    /// The cache proof is constructible only by the protected administrator cache
    /// loader after checking exact immutable image identity and complete proof marker.
    pub(crate) fn from_cache(
        proof: crate::executor_factory_docker::CacheProof,
    ) -> Result<Self, String> {
        let (image, manifest, identity) = proof.into_parts();
        let decoded = Manifest::decode(&manifest).map_err(|_| refused())?;
        if decoded.identity(&manifest).map_err(|_| refused())? != identity
            || !image
                .strip_prefix("sha256:")
                .is_some_and(artifacts::valid_digest)
        {
            return Err(refused());
        }
        Ok(Self {
            image,
            manifest,
            identity,
        })
    }
    pub(crate) fn image(&self) -> &str {
        &self.image
    }
    pub(crate) fn manifest(&self) -> &[u8] {
        &self.manifest
    }
    pub(crate) fn identity(&self) -> &Identity {
        &self.identity
    }

    /// Only the administrator's image inspector may call this constructor. The image
    /// ID must be Docker's inspected content ID, never a runtime response or mutable tag.
    pub(super) fn verify(
        image: String,
        manifest_bytes: Vec<u8>,
        reader: &mut dyn Read,
    ) -> Result<Self, String> {
        if !image
            .strip_prefix("sha256:")
            .is_some_and(artifacts::valid_digest)
        {
            return Err(refused());
        }
        let manifest = Manifest::decode(&manifest_bytes).map_err(|_| refused())?;
        let expected = manifest.files().map_err(|_| refused())?;
        let identity = manifest.identity(&manifest_bytes).map_err(|_| refused())?;
        let mut directories = BTreeSet::new();
        for path in expected
            .keys()
            .map(String::as_str)
            .chain([artifacts::MANIFEST_PATH])
        {
            let mut parent = path;
            while let Some((next, _)) = parent.rsplit_once('/') {
                if next.is_empty() {
                    break;
                }
                directories.insert(next.to_string());
                parent = next;
            }
        }
        let mut seen = BTreeSet::new();
        let mut seen_directories = BTreeSet::new();
        let mut manifest_seen = false;
        let mut nodes = 0usize;
        let mut archive = tar::Archive::new(Bounded {
            reader,
            remaining: MAX_ARCHIVE_BYTES,
        });
        for item in archive.entries().map_err(|_| refused())? {
            let mut entry = item.map_err(|_| refused())?;
            nodes += 1;
            if nodes > MAX_ARCHIVE_NODES {
                return Err(refused());
            }
            let raw = entry.path_bytes();
            let name = std::str::from_utf8(&raw).map_err(|_| refused())?;
            let relative = name.strip_suffix('/').unwrap_or(name);
            if matches!(relative, "." | "") && entry.header().entry_type().is_dir() {
                continue;
            }
            if !artifacts::canonical_path(relative, false) {
                return Err(refused());
            }
            let path = format!("/{relative}");
            let is_manifest = path == artifacts::MANIFEST_PATH;
            let file = expected.get(&path).copied();
            let in_sdk = manifest.sdk.as_ref().is_some_and(|sdk| {
                path.starts_with(&format!("{}/meerschaum/", sdk.root))
                    && !relative.split('/').any(|part| part == "__pycache__")
            });
            let in_compiler = manifest
                .compiler
                .as_ref()
                .is_some_and(|compiler| path.starts_with(&format!("{}/", compiler.snapshot)));
            if directories.contains(&path) || in_sdk || in_compiler || file.is_some() || is_manifest
            {
                let header = entry.header();
                let mode = header.mode().map_err(|_| refused())?;
                let uid = header.uid().map_err(|_| refused())?;
                // Generated Compose's Factory user is exactly 1000. Manual protected
                // installs perform the equivalent check against their actual native uid.
                if mode & 0o022 != 0 || uid == 1000 && mode & 0o200 != 0 {
                    return Err(refused());
                }
                if header.entry_type().is_dir() {
                    if file.is_some() || is_manifest {
                        return Err(refused());
                    }
                    if directories.contains(&path) && !seen_directories.insert(path.clone()) {
                        return Err(refused());
                    }
                    continue;
                }
                if !header.entry_type().is_file() || file.is_none() && !is_manifest {
                    return Err(refused());
                }
            }
            if let Some(file) = file {
                if !seen.insert(path.clone()) || entry.size() != file.bytes {
                    return Err(refused());
                }
                let mut digest = Sha256::new();
                let mut size = 0u64;
                let mut header = Vec::new();
                let mut bytes = [0u8; 128 * 1024];
                loop {
                    let count = entry.read(&mut bytes).map_err(|_| refused())?;
                    if count == 0 {
                        break;
                    }
                    size = size.checked_add(count as u64).ok_or_else(refused)?;
                    if size > file.bytes {
                        return Err(refused());
                    }
                    digest.update(&bytes[..count]);
                    if header.len() < 20 {
                        header.extend(&bytes[..count.min(20 - header.len())]);
                    }
                }
                let digest = digest
                    .finalize()
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>();
                if size != file.bytes || digest != file.sha256 {
                    return Err(refused());
                }
                let native = path == artifacts::CLI_PATH
                    || path.contains("/meerschaum/_core/_native") && path.ends_with(".so")
                    || manifest.compiler.as_ref().is_some_and(|compiler| {
                        path == format!("{}/toolchain/bin/cargo", compiler.snapshot)
                            || path == format!("{}/toolchain/bin/rustc", compiler.snapshot)
                    });
                if native {
                    let machine = match manifest.architecture.as_str() {
                        "x86_64" => 62u16,
                        "aarch64" => 183u16,
                        _ => return Err(refused()),
                    };
                    if header.len() != 20
                        || &header[..6] != b"\x7fELF\x02\x01"
                        || u16::from_le_bytes([header[18], header[19]]) != machine
                    {
                        return Err(refused());
                    }
                }
            } else if is_manifest {
                if manifest_seen || entry.size() != manifest_bytes.len() as u64 {
                    return Err(refused());
                }
                let mut bytes = Vec::new();
                entry
                    .take(artifacts::MAX_MANIFEST_BYTES as u64 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(|_| refused())?;
                if bytes != manifest_bytes {
                    return Err(refused());
                }
                manifest_seen = true;
            }
        }
        // tar stops at its terminator; drain the remaining stream under the same cap.
        // A second archive or hidden trailing authority entries are never accepted.
        let mut rest = archive.into_inner();
        let mut bytes = [0u8; 16 * 1024];
        loop {
            let count = rest.read(&mut bytes).map_err(|_| refused())?;
            if count == 0 {
                break;
            }
            if bytes[..count].iter().any(|byte| *byte != 0) {
                return Err(refused());
            }
        }
        if !manifest_seen || seen.len() != expected.len() || seen_directories != directories {
            return Err(refused());
        }
        Ok(Self {
            image,
            manifest: manifest_bytes,
            identity,
        })
    }
}

struct Bounded<'a> {
    reader: &'a mut dyn Read,
    remaining: u64,
}
impl Read for Bounded<'_> {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.remaining == 0 {
            let mut extra = [0];
            return match self.reader.read(&mut extra)? {
                0 => Ok(0),
                _ => Err(std::io::Error::other(
                    "The static image archive exceeds its bound.",
                )),
            };
        }
        let maximum = output
            .len()
            .min(self.remaining.min(usize::MAX as u64) as usize);
        let count = self.reader.read(&mut output[..maximum])?;
        self.remaining -= count as u64;
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn elf() -> Vec<u8> {
        let mut value = vec![0; 20];
        value[..6].copy_from_slice(b"\x7fELF\x02\x01");
        value[18..].copy_from_slice(&62u16.to_le_bytes());
        value
    }
    fn manifest() -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({"schema":1,"release":crate::VERSION,
            "architecture":"x86_64","cli":{"path":artifacts::CLI_PATH,
            "bytes":20,"sha256":artifacts::digest(&elf())},"sdk":null,"compiler":null}))
        .unwrap()
    }
    fn archive(cli: &[u8], writable: bool, duplicate: bool) -> Vec<u8> {
        let mut archive = tar::Builder::new(Vec::new());
        for directory in [
            "usr",
            "usr/local",
            "usr/local/bin",
            "usr/local/share",
            "usr/local/share/meerschaum",
        ] {
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Directory);
            header.set_mode(if writable && directory == "usr/local" {
                0o777
            } else {
                0o555
            });
            header.set_uid(0);
            header.set_gid(0);
            header.set_size(0);
            header.set_cksum();
            archive
                .append_data(&mut header, directory, std::io::empty())
                .unwrap();
        }
        let manifest = manifest();
        let mut entries = vec![
            (artifacts::CLI_PATH, cli),
            (artifacts::MANIFEST_PATH, manifest.as_slice()),
        ];
        if duplicate {
            entries.push((artifacts::CLI_PATH, cli));
        }
        for (path, bytes) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_mode(0o444);
            header.set_uid(0);
            header.set_gid(0);
            header.set_size(bytes.len() as u64);
            header.set_cksum();
            archive.append_data(&mut header, &path[1..], bytes).unwrap();
        }
        archive.into_inner().unwrap()
    }
    #[test]
    fn static_archive_proves_exact_native_bytes_without_extraction_or_execution() {
        let bytes = archive(&elf(), false, false);
        let image = VerifiedImage::verify(
            format!("sha256:{}", "a".repeat(64)),
            manifest(),
            &mut Cursor::new(bytes),
        )
        .unwrap();
        assert_eq!(image.identity().cli_sha256, artifacts::digest(&elf()));
        assert!(image.identity().sdk_sha256.is_none());
    }
    #[test]
    fn static_archive_rejects_tamper_duplicate_mutable_ancestry_and_truncation() {
        let mut changed = elf();
        changed[10] = 1;
        let good = archive(&elf(), false, false);
        for bytes in [
            archive(&changed, false, false),
            archive(&elf(), false, true),
            archive(&elf(), true, false),
            good[..600].to_vec(),
        ] {
            assert!(
                VerifiedImage::verify(
                    format!("sha256:{}", "a".repeat(64)),
                    manifest(),
                    &mut Cursor::new(bytes)
                )
                .is_err()
            );
        }
    }
}
