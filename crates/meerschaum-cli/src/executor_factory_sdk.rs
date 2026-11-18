//! Exact SDK import targets from the native constructor's measured fixed manifest.
//! Only its fresh empty bytecode prefix receives a Read rule; no existing runtime gains access.
//! Neither the SDK targets nor prefix are selected by a request or environment.

use meerschaum_server::executor::factory_artifacts::Sdk;
use std::path::{Path, PathBuf};

pub(crate) const PYCACHE: &str = "sdk-pycache";

pub(crate) fn bind(
    prepared: &mut crate::executor_profiles::Prepared,
    sdk: &Sdk,
) -> Result<(), String> {
    let refused =
        || "The private runtime SDK differs from the native Factory installation.".to_string();
    let runtime = prepared.spec.runtime.as_mut().ok_or_else(refused)?;
    if !runtime.api_only {
        return Err(refused());
    }
    for (module, relative) in [
        ("meerschaum", "meerschaum/__init__.py"),
        ("_meerschaum_mrsm", "_meerschaum_mrsm.py"),
    ] {
        if !sdk.files.contains_key(relative) {
            return Err(refused());
        }
        let target = PathBuf::from(&sdk.root).join(relative);
        if std::fs::canonicalize(&target).ok().as_deref() != Some(target.as_path())
            || runtime
                .packages
                .get(module)
                .is_some_and(|existing| existing != &target)
        {
            return Err(refused());
        }
        // TrustedPackages is installed before Python sites and the SDK's native provenance
        // capture searches this exact meerschaum target first. Contradictory mappings refuse.
        runtime.packages.insert(module.into(), target);
    }
    let prefix = prepared.directory.join(PYCACHE);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        // Every generation is fresh. An existing directory is a refusal, never a reused
        // worker cache or an administrator/owner Python cache to clear.
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&prefix)
            .map_err(|_| refused())?;
    }
    #[cfg(not(unix))]
    return Err(refused());
    validate_prefix(&prefix, &prepared.directory)?;
    runtime.sdk_pycache_prefix = Some(prefix.clone());
    let rule = meerschaum_core::team_scope::sandbox::Rule::new(
        &prefix,
        meerschaum_core::team_scope::sandbox::Access::Read,
        "host-selected empty native SDK bytecode cache",
    );
    let (rules, dropped) = prepared.spec.allowed(vec![rule]);
    if !dropped.is_empty() {
        return Err(refused());
    }
    prepared.spec.rules.extend(rules);
    let spec = prepared
        .environment
        .iter()
        .find(|(name, _)| name == meerschaum_core::team_scope::sandbox::SPEC_VAR)
        .map(|(_, value)| PathBuf::from(value))
        .ok_or_else(refused)?;
    if spec.parent() != Some(prepared.directory.as_path()) {
        return Err(refused());
    }
    meerschaum_core::team_scope::write_private(
        &spec,
        &serde_json::to_string(&prepared.spec).map_err(|_| refused())?,
    )
    .map_err(|_| refused())
}

/// The attested cache is empty and kernel-read-only to the worker. It never uses source
/// tree __pycache__ files or the ordinary container/owner PYTHONPYCACHEPREFIX setting.
pub(crate) fn validate_prefix(prefix: &Path, generation: &Path) -> Result<(), String> {
    let refused =
        || "The protected SDK bytecode cache is not this empty native generation.".to_string();
    if prefix != generation.join(PYCACHE)
        || std::fs::canonicalize(prefix).ok().as_deref() != Some(prefix)
    {
        return Err(refused());
    }
    let metadata = std::fs::symlink_metadata(prefix).map_err(|_| refused())?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() {
        return Err(refused());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.uid() != rustix::process::geteuid().as_raw() || metadata.mode() & 0o077 != 0 {
            return Err(refused());
        }
    }
    #[cfg(not(unix))]
    return Err(refused());
    if std::fs::read_dir(prefix)
        .map_err(|_| refused())?
        .next()
        .is_some()
    {
        return Err(refused());
    }
    Ok(())
}
