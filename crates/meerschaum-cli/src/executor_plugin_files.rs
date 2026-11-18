//! Immutable Host source objects. Caller-specific views never import mutable Main paths.

use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
#[cfg(unix)]
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use meerschaum_server::executor::{
    plugins,
    source_ceiling::{Ceiling, Fingerprint},
};
#[cfg(unix)]
use sha2::{Digest, Sha256};

/// Actual copied bytes and their immutable import entry points, selected only by Main.
pub struct Snapshot {
    /// Relative file names and SHA-256 digests of the bytes actually copied.
    pub hashes: BTreeMap<String, String>,
    /// Import entry points inside sealed, content-addressed source objects.
    pub sources: Vec<(OsString, PathBuf)>,
    /// Opaque binding to this exact original canonical source view and copied file set.
    pub view_sha256: String,
}

/// Shared immutable content, separate from launch-private writable state and grant views.
pub struct Store {
    root: PathBuf,
}
impl Store {
    /// Main chooses a private root outside all launch-writable directories.
    pub fn new(root: &Path) -> Result<Self, String> {
        let root = std::fs::canonicalize(root)
            .map_err(|_| "The Host source root is unavailable.")?
            .join("executor-plugin-files");
        for path in [&root, &root.join("objects"), &root.join("staging")] {
            meerschaum_core::team_scope::create_private_dir(path)
                .map_err(|_| "Could not prepare private Host source storage.")?;
            let metadata = std::fs::symlink_metadata(path)
                .map_err(|_| "Host source storage is unavailable.")?;
            if !metadata.is_dir() || metadata.is_symlink() {
                return Err("Unsafe Host source storage.".into());
            }
        }
        Ok(Self { root })
    }

    /// Copy one selected view, checking its actual copied content against captured authority.
    pub fn snapshot(
        &self,
        sources: &[(OsString, PathBuf)],
        expected: Option<&Ceiling>,
    ) -> Result<Snapshot, String> {
        let stage = self
            .root
            .join("staging")
            .join(uuid::Uuid::new_v4().simple().to_string());
        let mut builder = std::fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            builder.mode(0o700);
        }
        builder
            .create(&stage)
            .map_err(|_| "Could not stage a private Host source snapshot.")?;
        let result = self.snapshot_in(&stage, sources, expected);
        // This fresh staging directory has never been granted to a child. No old state or
        // uncertain process generation is traversed or removed here.
        let _ = std::fs::remove_dir_all(&stage);
        result
    }

    fn snapshot_in(
        &self,
        stage: &Path,
        sources: &[(OsString, PathBuf)],
        expected: Option<&Ceiling>,
    ) -> Result<Snapshot, String> {
        let mut hashes = BTreeMap::new();
        let mut names = BTreeSet::new();
        let mut paths = Vec::new();
        let mut remaining = 128 * 1024 * 1024usize;
        let mut entries = 4096usize;
        for (filename, path) in sources {
            let name = filename.to_str().ok_or("Invalid Host source filename.")?;
            if name.contains(['/', '\\'])
                || matches!(name, "." | ".." | "")
                || !names.insert(name.to_string())
            {
                return Err("Ambiguous Host source entry points.".into());
            }
            let plugin = plugin_name(name)?;
            let main = self
                .root
                .parent()
                .ok_or("Invalid Host source storage root.")?;
            let source = validate_source(main, path)?;
            let destination = stage.join(&plugin).join("source").join(name);
            std::fs::create_dir_all(destination.parent().ok_or("Invalid Host source target.")?)
                .map_err(|_| "Could not prepare a fresh Host source object.")?;
            copy_entry(
                path,
                Some(&destination),
                name,
                &mut hashes,
                &mut remaining,
                &mut entries,
            )?;
            paths.push((name.to_string(), source));
        }
        let ceiling = Ceiling::from_files(&hashes)?;
        if expected.is_some_and(|expected| expected != &ceiling) {
            return Err("The Host source bytes changed from their captured authority. Recreate this execution explicitly.".into());
        }
        paths.sort();
        let mut binding = b"mrsm-host-source-view-v1\0".to_vec();
        binding.extend(
            serde_json::to_vec(&(&paths, &hashes)).map_err(|_| "Invalid Host source view.")?,
        );
        let view_sha256 = plugins::sha256(&binding);
        let mut published = BTreeMap::new();
        for (name, fingerprint) in &ceiling.sources {
            let Fingerprint::Files { tree_sha256 } = fingerprint else {
                return Err("A Host source must use a Files fingerprint.".into());
            };
            let object = self.root.join("objects").join(tree_sha256);
            let staged = stage.join(name);
            match std::fs::symlink_metadata(&object) {
                Ok(_) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    match std::fs::rename(&staged, &object) {
                        Ok(()) => {}
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::AlreadyExists
                                    | std::io::ErrorKind::DirectoryNotEmpty
                            ) => {}
                        Err(_) => {
                            return Err(
                                "Could not publish the immutable Host source object.".into()
                            );
                        }
                    }
                }
                Err(_) => return Err("The immutable Host source object is unavailable.".into()),
            }
            for path in [&object, &object.join("source")] {
                let metadata = std::fs::symlink_metadata(path)
                    .map_err(|_| "The sealed Host source object is unavailable.")?;
                if !metadata.is_dir() || metadata.is_symlink() {
                    return Err("Unsafe sealed Host source object.".into());
                }
            }
            let mut entries = Vec::new();
            for filename in &names {
                if plugin_name(filename)? == *name {
                    let source = object.join("source").join(filename);
                    let metadata = std::fs::symlink_metadata(&source)
                        .map_err(|_| "A sealed Host source entry is missing.")?;
                    if metadata.is_symlink() || !(metadata.is_dir() || metadata.is_file()) {
                        return Err("Unsafe sealed Host source entry.".into());
                    }
                    entries.push((OsString::from(filename), source));
                }
            }
            let actual = crate::executor_registration::source_hashes(&entries)?;
            let actual = Ceiling::from_files(&actual)?;
            if actual.sources.len() != 1 || actual.sources.get(name) != Some(fingerprint) {
                return Err(
                    "The sealed Host source bytes do not match their content identity.".into(),
                );
            }
            published.extend(entries);
        }
        Ok(Snapshot {
            hashes,
            sources: published.into_iter().collect(),
            view_sha256,
        })
    }
}

/// Pure inspection never creates the Store, staging, a Host epoch or writable runtime state.
/// It reads the same held NOFOLLOW descriptors and hashes the same actual bytes as copies.
pub(crate) struct Fingerprints {
    pub hashes: BTreeMap<String, String>,
    pub view_sha256: String,
}
pub(crate) fn inspect(
    root: &Path,
    sources: &[(OsString, PathBuf)],
) -> Result<Fingerprints, String> {
    let root = std::fs::canonicalize(root).map_err(|_| "The Host source root is unavailable.")?;
    let mut hashes = BTreeMap::new();
    let mut paths = Vec::new();
    let mut names = BTreeSet::new();
    let mut remaining = 128 * 1024 * 1024;
    let mut entries = 4096;
    for (filename, path) in sources {
        let name = filename.to_str().ok_or("Invalid Host source filename.")?;
        if name.contains(['/', '\\'])
            || matches!(name, "." | ".." | "")
            || !names.insert(name.to_string())
        {
            return Err("Ambiguous Host source entry points.".into());
        }
        let _ = plugin_name(name)?;
        let canonical = validate_source(&root, path)?;
        copy_entry(path, None, name, &mut hashes, &mut remaining, &mut entries)?;
        paths.push((name.to_string(), canonical));
    }
    let _ = Ceiling::from_files(&hashes)?;
    paths.sort();
    let mut binding = b"mrsm-host-source-view-v1\0".to_vec();
    binding
        .extend(serde_json::to_vec(&(&paths, &hashes)).map_err(|_| "Invalid Host source view.")?);
    Ok(Fingerprints {
        hashes,
        view_sha256: plugins::sha256(&binding),
    })
}
fn validate_source(main: &Path, path: &Path) -> Result<PathBuf, String> {
    let source =
        std::fs::canonicalize(path).map_err(|_| "A selected Host source is unavailable.")?;
    let store = main.join("executor-plugin-files");
    if store.starts_with(&source)
        || [
            "config",
            "jobs",
            "team_scopes",
            "venvs",
            "execution-contexts",
            "executors",
            ".internal",
        ]
        .iter()
        .any(|name| source.starts_with(main.join(name)))
    {
        return Err("A Host plugin source cannot select Main credentials, shared state or its own snapshot storage.".into());
    }
    Ok(source)
}

fn plugin_name(filename: &str) -> Result<String, String> {
    let name = filename
        .strip_suffix(".py")
        .or_else(|| filename.strip_suffix(".rs"))
        .unwrap_or(filename);
    if !plugins::valid_name(name) || name == "__init__" {
        return Err("Invalid Host source name.".into());
    }
    Ok(name.into())
}

#[cfg(not(unix))]
fn copy_entry(
    _: &Path,
    _: Option<&Path>,
    _: &str,
    _: &mut BTreeMap<String, String>,
    _: &mut usize,
    _: &mut usize,
) -> Result<(), String> {
    Err("Immutable Host source preparation requires the supported native platform.".into())
}

#[cfg(unix)]
fn copy_entry(
    source: &Path,
    destination: Option<&Path>,
    name: &str,
    hashes: &mut BTreeMap<String, String>,
    remaining: &mut usize,
    entries: &mut usize,
) -> Result<(), String> {
    use rustix::fs::{Mode, OFlags, open, openat};
    let mut parent = open(
        "/",
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|_| "Could not anchor the Host source descriptor.")?;
    if source.components().any(|part| {
        matches!(
            part,
            std::path::Component::ParentDir | std::path::Component::CurDir
        )
    }) {
        return Err("A selected Host source path must not contain relative components.".into());
    }
    let parts = source
        .components()
        .filter_map(|part| match part {
            std::path::Component::Normal(name) => Some(name),
            _ => None,
        })
        .collect::<Vec<_>>();
    if !source.is_absolute() || parts.is_empty() {
        return Err("Invalid canonical Host source path.".into());
    }
    for (index, part) in parts.iter().enumerate() {
        let mut flags = OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC;
        if index + 1 < parts.len() {
            flags |= OFlags::DIRECTORY;
        }
        parent = openat(&parent, *part, flags, Mode::empty())
            .map_err(|_| "A Host source path changed or contains an unsafe link.")?;
    }
    copy_descriptor(
        std::fs::File::from(parent),
        destination,
        name,
        0,
        hashes,
        remaining,
        entries,
    )
}

#[cfg(unix)]
fn copy_descriptor(
    mut file: std::fs::File,
    destination: Option<&Path>,
    name: &str,
    depth: usize,
    hashes: &mut BTreeMap<String, String>,
    remaining: &mut usize,
    entries: &mut usize,
) -> Result<(), String> {
    use rustix::fs::{Mode, OFlags, openat};
    *entries = entries
        .checked_sub(1)
        .ok_or("The Host source snapshot exceeds its entry bound.")?;
    if depth > 32 || name.len() > 1024 {
        return Err("The Host source snapshot exceeds its path bound.".into());
    }
    let metadata = file
        .metadata()
        .map_err(|_| "The held Host source is unavailable.")?;
    if metadata.is_dir() {
        if let Some(destination) = destination {
            let mut builder = std::fs::DirBuilder::new();
            use std::os::unix::fs::DirBuilderExt;
            builder
                .mode(0o700)
                .create(destination)
                .map_err(|_| "Could not prepare a fresh source directory.")?;
        }
        let mut directory = rustix::fs::Dir::read_from(&file)
            .map_err(|_| "The held Host source directory is unavailable.")?;
        let mut names = Vec::new();
        while let Some(entry) = directory.read() {
            let entry = entry.map_err(|_| "The held Host source directory changed.")?;
            let leaf = entry
                .file_name()
                .to_str()
                .map_err(|_| "Invalid Host package filename.")?;
            if matches!(leaf, "." | "..") {
                continue;
            }
            if names.len() >= *entries {
                return Err("Too many Host source entries.".into());
            }
            names.push(leaf.to_string());
        }
        names.sort();
        for leaf in names {
            let child = openat(
                &file,
                leaf.as_str(),
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(|_| "A Host package member changed or is an unsafe link.")?;
            let destination = destination.map(|path| path.join(&leaf));
            copy_descriptor(
                std::fs::File::from(child),
                destination.as_deref(),
                &format!("{name}/{leaf}"),
                depth + 1,
                hashes,
                remaining,
                entries,
            )?;
        }
        return Ok(());
    }
    if !metadata.is_file()
        || metadata.len() > 32 * 1024 * 1024
        || metadata.len() > *remaining as u64
    {
        return Err("A Host source is not a bounded regular file.".into());
    }
    use std::os::unix::fs::OpenOptionsExt;
    let mut output = destination
        .map(|destination| {
            std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(0o400)
                .open(destination)
                .map_err(|_| "Could not create a fresh immutable source file.")
        })
        .transpose()?;
    let mut digest = Sha256::new();
    let limit = (*remaining).min(32 * 1024 * 1024);
    let mut size = 0usize;
    let mut buffer = vec![0u8; 64 * 1024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|_| "Could not read the held Host source.")?;
        if count == 0 {
            break;
        }
        size = size
            .checked_add(count)
            .filter(|size| *size <= limit)
            .ok_or("The Host source grew beyond its byte bound.")?;
        digest.update(&buffer[..count]);
        if let Some(output) = output.as_mut() {
            output
                .write_all(&buffer[..count])
                .map_err(|_| "Could not copy the immutable Host source bytes.")?;
        }
    }
    if let Some(output) = output.as_ref() {
        output
            .sync_all()
            .map_err(|_| "Could not publish complete Host source bytes.")?;
    }
    *remaining -= size;
    if hashes
        .insert(
            name.into(),
            digest
                .finalize()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect(),
        )
        .is_some()
    {
        return Err("Duplicate Host source snapshot paths.".into());
    }
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;

    #[test]
    fn inert_inspection_hashes_the_same_held_bytes_without_creating_execution_state() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("plugins/probe");
        std::fs::create_dir_all(package.join("resources")).unwrap();
        let code = b"raise AssertionError('Inert reader inspection must never import source')\n";
        std::fs::write(package.join("__init__.py"), code).unwrap();
        std::fs::write(package.join("resources/data.json"), b"{\"fixture\":true}").unwrap();
        let sources = vec![(OsString::from("probe"), package.clone())];
        let observed = inspect(root.path(), &sources).unwrap();
        assert_eq!(std::fs::read(package.join("__init__.py")).unwrap(), code);
        for name in [
            "executor-plugin-files",
            "executor-host-identity",
            "executor-plugin-metadata",
            "execution-contexts",
            "jobs",
        ] {
            assert!(
                !root.path().join(name).exists(),
                "Inspection created {name}"
            );
        }
        let copied = Store::new(root.path())
            .unwrap()
            .snapshot(&sources, None)
            .unwrap();
        assert_eq!(observed.hashes, copied.hashes);
        assert_eq!(observed.view_sha256, copied.view_sha256);
        let link = package.join("resources/unsafe");
        std::os::unix::fs::symlink(package.join("__init__.py"), &link).unwrap();
        assert!(inspect(root.path(), &sources).is_err());
        std::fs::remove_file(&link).unwrap();
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            &link,
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        let began = std::time::Instant::now();
        assert!(inspect(root.path(), &sources).is_err());
        assert!(began.elapsed() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn copied_bytes_stay_immutable_and_content_objects_are_separate_from_grant_views() {
        let root = tempfile::tempdir().unwrap();
        let selected = root.path().join("plugins");
        std::fs::create_dir_all(selected.join("a/resources")).unwrap();
        std::fs::write(
            selected.join("a/__init__.py"),
            "raise AssertionError('Snapshot must never import source')\n",
        )
        .unwrap();
        std::fs::write(selected.join("a/resources/data.json"), "{\"before\":true}").unwrap();
        std::fs::write(
            selected.join("b.py"),
            "raise AssertionError('No imports')\n",
        )
        .unwrap();
        let sources = vec![
            ("a".into(), selected.join("a")),
            ("b.py".into(), selected.join("b.py")),
        ];
        let hashes = crate::executor_registration::source_hashes(&sources).unwrap();
        let ceiling = Ceiling::from_files(&hashes).unwrap();
        let store = Store::new(root.path()).unwrap();
        let first = store.snapshot(&sources, Some(&ceiling)).unwrap();
        assert_eq!(first.hashes, hashes);
        assert!(
            first
                .sources
                .iter()
                .all(|(_, path)| path.starts_with(store.root.join("objects")))
        );
        let a = first
            .sources
            .iter()
            .find(|(name, _)| name == "a")
            .unwrap()
            .1
            .clone();
        let b = first
            .sources
            .iter()
            .find(|(name, _)| name == "b.py")
            .unwrap()
            .1
            .clone();
        std::fs::write(selected.join("a/resources/data.json"), "{\"after\":true}").unwrap();
        assert_eq!(
            std::fs::read_to_string(a.join("resources/data.json")).unwrap(),
            "{\"before\":true}"
        );
        assert!(store.snapshot(&sources, Some(&ceiling)).is_err());
        assert_eq!(
            std::fs::read_dir(store.root.join("staging"))
                .unwrap()
                .count(),
            0
        );
        let next = store.snapshot(&sources, None).unwrap();
        assert_ne!(first.view_sha256, next.view_sha256);
        assert_ne!(
            next.sources.iter().find(|(name, _)| name == "a").unwrap().1,
            a
        );
        assert_eq!(
            next.sources
                .iter()
                .find(|(name, _)| name == "b.py")
                .unwrap()
                .1,
            b
        );
        let narrow = store.snapshot(&sources[1..], None).unwrap();
        assert_eq!(narrow.sources, vec![(OsString::from("b.py"), b)]);
        assert_ne!(narrow.view_sha256, next.view_sha256);
    }

    #[test]
    fn unsafe_package_links_and_fifos_fail_without_publishing_or_blocking() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("plugins/probe");
        std::fs::create_dir_all(&package).unwrap();
        std::fs::write(
            package.join("__init__.py"),
            "raise AssertionError('No imports')\n",
        )
        .unwrap();
        let outside = root.path().join("ungranted-file");
        std::fs::write(&outside, "ungranted fixture data").unwrap();
        std::os::unix::fs::symlink(&outside, package.join("outside.py")).unwrap();
        let sources = vec![(OsString::from("probe"), package.clone())];
        let store = Store::new(root.path()).unwrap();
        assert!(store.snapshot(&sources, None).is_err());
        assert_eq!(
            std::fs::read_dir(store.root.join("objects"))
                .unwrap()
                .count(),
            0
        );
        std::fs::remove_file(package.join("outside.py")).unwrap();
        rustix::fs::mkfifoat(
            rustix::fs::CWD,
            package.join("blocked.py"),
            rustix::fs::Mode::RUSR | rustix::fs::Mode::WUSR,
        )
        .unwrap();
        let began = std::time::Instant::now();
        assert!(store.snapshot(&sources, None).is_err());
        assert!(began.elapsed() < std::time::Duration::from_secs(1));
        assert_eq!(
            std::fs::read_dir(store.root.join("objects"))
                .unwrap()
                .count(),
            0
        );
        assert_eq!(
            std::fs::read_dir(store.root.join("staging"))
                .unwrap()
                .count(),
            0
        );
        assert!(
            store
                .snapshot(&[(OsString::from("main"), root.path().into())], None)
                .is_err()
        );
    }
}
