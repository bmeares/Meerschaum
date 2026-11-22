//! Fixed confined connector-register operation and bounded source fingerprints.

use meerschaum_server::executor::registration::{Outcome, Registration, Sources};
use meerschaum_server::executor::{plugins, registration};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};

/// Content identities of only the host-selected import entry points, never a shared root.
pub(crate) fn source_hashes(
    sources: &[(OsString, PathBuf)],
) -> Result<BTreeMap<String, String>, String> {
    let mut hashes = BTreeMap::new();
    let mut remaining = 128 * 1024 * 1024usize;
    let mut entries_remaining = 4096usize;
    for (filename, source) in sources {
        let filename = filename
            .to_str()
            .ok_or("Invalid registration source entry point.")?;
        if filename.contains(['/', '\\']) || matches!(filename, "." | "..") {
            return Err("Invalid registration source entry point.".into());
        }
        let source = std::fs::canonicalize(source)
            .map_err(|_| "The selected registration source is unavailable.")?;
        hash_tree(
            &source,
            filename,
            0,
            &mut hashes,
            &mut remaining,
            &mut entries_remaining,
        )?;
    }
    Ok(hashes)
}

fn hash_tree(
    path: &Path,
    name: &str,
    depth: usize,
    hashes: &mut BTreeMap<String, String>,
    remaining: &mut usize,
    entries_remaining: &mut usize,
) -> Result<(), String> {
    *entries_remaining = entries_remaining
        .checked_sub(1)
        .ok_or("Registration source snapshot exceeds its entry limit.")?;
    if hashes.len() >= 4096 || depth > 32 || name.len() > 1024 {
        return Err("Registration source snapshot exceeds its limit.".into());
    }
    let metadata =
        std::fs::symlink_metadata(path).map_err(|_| "A registration source is unavailable.")?;
    if metadata.is_dir() {
        let mut entries = Vec::new();
        for entry in
            std::fs::read_dir(path).map_err(|_| "A registration package is unavailable.")?
        {
            if entries.len() >= 4096 {
                return Err("Registration package exceeds its entry limit.".into());
            }
            entries.push(entry.map_err(|_| "A registration package is unavailable.")?);
        }
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let filename = entry.file_name();
            let filename = filename
                .to_str()
                .ok_or("Invalid registration package entry.")?;
            hash_tree(
                &entry.path(),
                &format!("{name}/{filename}"),
                depth + 1,
                hashes,
                remaining,
                entries_remaining,
            )?;
        }
        return Ok(());
    }
    if !metadata.is_file()
        || metadata.len() > 32 * 1024 * 1024
        || metadata.len() > *remaining as u64
    {
        return Err("A registration source is not a bounded regular file.".into());
    }
    #[cfg(unix)]
    let file = std::fs::File::from(
        rustix::fs::open(
            path,
            rustix::fs::OFlags::RDONLY
                | rustix::fs::OFlags::NOFOLLOW
                | rustix::fs::OFlags::NONBLOCK
                | rustix::fs::OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(|_| "The registration source is unavailable.")?,
    );
    #[cfg(not(unix))]
    return Err("Confined registration requires a supported native platform.".into());
    #[cfg(unix)]
    {
        if !file
            .metadata()
            .map_err(|_| "The registration source is unavailable.")?
            .is_file()
        {
            return Err("The registration source is not a regular file.".into());
        }
        let limit = (*remaining).min(32 * 1024 * 1024);
        let mut reader = file.take(limit as u64 + 1);
        // Recursion keeps directory frames alive while hashing a leaf. Allocate the read
        // buffer on the heap rather than reserving 64KiB in every recursive stack frame.
        let mut buffer = vec![0u8; 64 * 1024];
        let mut bytes = 0usize;
        let mut digest = Sha256::new();
        loop {
            let count = reader
                .read(&mut buffer)
                .map_err(|_| "The registration source is unreadable.")?;
            if count == 0 {
                break;
            }
            bytes = bytes.saturating_add(count);
            if bytes > limit {
                return Err("Registration source exceeds its byte limit.".into());
            }
            digest.update(&buffer[..count]);
        }
        *remaining -= bytes;
        if hashes
            .insert(name.into(), hex::encode(digest.finalize()))
            .is_some()
        {
            return Err("Registration source entry points are ambiguous.".into());
        }
        Ok(())
    }
}

fn names(registration: &Registration) -> Result<BTreeSet<String>, String> {
    match &registration.sources {
        Sources::Files { hashes } => hashes
            .keys()
            .map(|path| {
                let first = path.split('/').next().ok_or("Invalid source snapshot.")?;
                let name = first
                    .strip_suffix(".py")
                    .or_else(|| first.strip_suffix(".rs"))
                    .unwrap_or(first);
                if !plugins::valid_name(name) {
                    return Err("Invalid registration plugin name.".into());
                }
                Ok(name.to_string())
            })
            .collect(),
        Sources::Archives { catalog } => Ok(catalog.plugins.keys().cloned().collect()),
    }
}

#[cfg(all(test, unix))]
mod source_tests {
    use super::*;

    #[test]
    fn bounded_deep_packages_hash_on_a_normal_worker_stack_without_importing() {
        let root = tempfile::tempdir().unwrap();
        let package = root.path().join("deep_package");
        std::fs::create_dir(&package).unwrap();
        std::fs::write(
            package.join("__init__.py"),
            "raise RuntimeError('never import')\n",
        )
        .unwrap();
        let mut leaf = package.clone();
        let mut relative = "deep_package".to_string();
        for depth in 0..28 {
            let name = format!("level_{depth}");
            leaf.push(&name);
            relative.push('/');
            relative.push_str(&name);
        }
        std::fs::create_dir_all(&leaf).unwrap();
        let bytes = vec![b'x'; 128 * 1024];
        std::fs::write(leaf.join("data.bin"), &bytes).unwrap();
        let actual = std::thread::Builder::new()
            .stack_size(1024 * 1024)
            .spawn(move || source_hashes(&[(OsString::from("deep_package"), package)]))
            .unwrap()
            .join()
            .unwrap()
            .unwrap();
        assert_eq!(actual.len(), 2);
        assert_eq!(
            actual[&format!("{relative}/data.bin")],
            hex::encode(Sha256::digest(bytes))
        );
    }
}

/// Runs after the native mandatory boundary, with the descriptor selected by signed Main
/// context delivery. Public arguments alone cannot turn this into an arbitrary worker RPC.
pub(crate) fn run(runtime: &mut crate::dispatch::Runtime<'_>) -> Result<Vec<u8>, String> {
    if std::env::var(meerschaum_core::team_scope::api_execution::PROFILE_VAR).as_deref() != Ok("1")
        || std::env::var_os(meerschaum_server::executor::environment::METADATA_VAR).is_some()
    {
        return Err("Connector registration requires its private API execution profile.".into());
    }
    meerschaum_core::team_scope::sandbox::current()
        .ok_or("Connector registration requires a confined native runtime.")?
        .require_sockets_verdict()
        .map_err(
            |_| "Connector registration requires mandatory filesystem/socket/signal isolation.",
        )?;
    let value = runtime
        .config()
        .get(&[registration::CONFIG_KEY])
        .ok_or("No Main-selected connector registration descriptor is available.")?;
    let registration: Registration = serde_json::from_str(&value.to_json_string())
        .map_err(|_| "Invalid connector registration descriptor.")?;
    registration
        .validate()
        .map_err(|error| error.detail_text())?;
    if registration.deadline <= chrono::Utc::now().timestamp() {
        return Err("The connector registration operation expired before invocation.".into());
    }
    let grants: BTreeSet<String> = serde_json::from_str(
        &std::env::var("MRSM_EXECUTOR_PLUGINS").map_err(|_| "No host-selected source grants.")?,
    )
    .map_err(|_| "Invalid host-selected source grants.")?;
    if grants != names(&registration)? {
        return Err("The registration source ceiling changed.".into());
    }
    match &registration.sources {
        Sources::Files { hashes } => {
            let directories = runtime.config().paths().plugins_dirs.clone();
            let mut selected = Vec::new();
            for name in &grants {
                let mut found = None;
                for directory in &directories {
                    for filename in [format!("{name}.py"), format!("{name}.rs"), name.clone()] {
                        let path = directory.join(&filename);
                        if path.exists() {
                            if found.is_some() {
                                return Err("The registration source view is ambiguous.".into());
                            }
                            found = Some((OsString::from(filename), path));
                        }
                    }
                }
                selected.push(found.ok_or("The approved registration source is missing.")?);
            }
            if source_hashes(&selected)? != *hashes {
                return Err("Registration source content changed before invocation.".into());
            }
        }
        Sources::Archives { catalog } => {
            let actual = runtime
                .config()
                .get(&[plugins::CONFIG_KEY])
                .ok_or("The exact registration source catalog is unavailable.")?;
            let actual: plugins::Catalog = serde_json::from_str(&actual.to_json_string())
                .map_err(|_| "Invalid registration source catalog.")?;
            if actual != *catalog {
                return Err("The registration archive source view changed.".into());
            }
        }
    }
    let pipe = registration.pipe().map_err(|error| error.detail_text())?;
    let session = crate::native::session(runtime);
    let outcome =
        match crate::native::manage::ask_connector_register(&session, runtime.config(), &pipe) {
            None => Outcome::Undefined,
            Some(Ok(value)) if !value.defined => Outcome::Undefined,
            Some(Ok(value)) if value.valid => Outcome::Parameters {
                parameters: value.parameters,
            },
            Some(Ok(value)) => Outcome::Invalid {
                representation: value.repr,
            },
            Some(Err(_)) => return Err("The confined connector register hook failed.".into()),
        };
    let bytes = serde_json::to_vec(&outcome)
        .map_err(|_| "Could not encode the confined registration result.")?;
    if bytes.len() > 1024 * 1024 {
        return Err("The confined registration result exceeds its byte limit.".into());
    }
    Ok(bytes)
}
