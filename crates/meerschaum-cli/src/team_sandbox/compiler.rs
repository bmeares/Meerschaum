//! Capture one protected native compiler policy before any source discovery or Python startup.
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use meerschaum_core::team_scope::sandbox::{self, Spec};
use meerschaum_plugin::source::compiler::{CompilerPolicy, SourceCompiler};

static POLICY: OnceLock<Result<CompilerPolicy, String>> = OnceLock::new();

pub(super) struct Captured {
    spec_path: PathBuf,
    spec: Spec,
    prepared: Option<crate::executor::compiler::Prepared>,
}

/// Read exactly the fixed native sibling while startup still owns the environment. No later
/// Session or imported source can select another file, descriptor or compiler policy.
pub(super) fn read(spec_path: &Path, spec: &Spec) -> Result<Captured, String> {
    let binding = spec
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.native_compiler.as_ref());
    let prepared = if let Some(binding) = binding {
        let directory = spec_path.parent().ok_or("Missing compiler generation.")?;
        let bytes = crate::executor_profiles::read_compiler_descriptor(
            &directory.join(crate::executor_profiles::COMPILER_FILE),
        )?;
        if meerschaum_server::executor::plugins::sha256(&bytes) != binding.prepared_sha256 {
            return Err("The protected native compiler material changed before startup.".into());
        }
        let prepared = serde_json::from_slice(&bytes)
            .map_err(|_| "Invalid protected native compiler material.")?;
        crate::executor_profiles::validate_compiler_material(spec, directory, &prepared, binding)?;
        Some(prepared)
    } else {
        None
    };
    Ok(Captured {
        spec_path: spec_path.into(),
        spec: spec.clone(),
        prepared,
    })
}

/// Enforce first. Read-only path shape alone is not host provenance: active kernel grants,
/// exact captured native material and Main's independent stored-generation attestation are
/// all required. Forged descendant data cannot add reads or execution to its inherited wall.
pub(super) fn install(captured: Result<Captured, String>) {
    let result = captured.and_then(|captured| {
        sandbox::current()
            .ok_or("Native compiler startup has no active boundary.")?
            .require_sockets_verdict()?;
        let Some(prepared) = captured.prepared else {
            return Ok(CompilerPolicy::Confined(None));
        };
        let directory = captured
            .spec_path
            .parent()
            .ok_or("Missing compiler generation.")?;
        let context = directory
            .parent()
            .ok_or("Missing native compiler context.")?;
        let canonical = std::fs::canonicalize(&captured.spec_path)
            .map_err(|_| "The compiler spec is unavailable.")?;
        let generation_name = directory
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("generation-"))
            .and_then(|name| uuid::Uuid::parse_str(name).ok());
        let context_id = context
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| uuid::Uuid::parse_str(name).ok());
        let runtime = captured
            .spec
            .runtime
            .as_ref()
            .ok_or("Missing native compiler runtime.")?;
        if canonical != captured.spec_path
            || generation_name.is_none_or(|id| id.is_nil())
            || context_id.is_none_or(|id| id.is_nil())
            || context
                .parent()
                .and_then(Path::file_name)
                .and_then(|name| name.to_str())
                != Some("execution-contexts")
            || !matches!(
                captured
                    .spec_path
                    .file_name()
                    .and_then(|name| name.to_str()),
                Some(sandbox::SPEC_FILE | sandbox::JOB_SPEC_FILE)
            )
            || std::env::current_exe()
                .ok()
                .and_then(|path| std::fs::canonicalize(path).ok())
                .as_ref()
                != Some(&runtime.launcher)
            || std::env::var_os("MRSM_CONFIG_FILE").map(PathBuf::from)
                != Some(directory.join("config.json"))
            || std::env::var_os("MRSM_ROOT_DIR").map(PathBuf::from) != Some(prepared.state.clone())
        {
            return Err("The compiler lost its protected native launch binding.".into());
        }
        // A private source cannot replace a protected descriptor/config by chmod or by
        // creating its own Cargo settings. Test the installed grants, not mode bits alone.
        deny_write_directory(directory)?;
        deny_write_directory(&prepared.cargo_home)?;
        deny_write_file(&captured.spec_path)?;
        deny_write_file(&directory.join(crate::executor_profiles::COMPILER_FILE))?;
        deny_write_file(&prepared.cargo_home.join("config.toml"))?;
        // An old API terminal may keep running ordinary commands after compilation's fixed
        // initial budget. It gains no fresh compiler on reconnect or credential renewal.
        if prepared.owner_deadline <= chrono::Utc::now().timestamp() {
            return Ok(CompilerPolicy::Confined(None));
        }
        let runner = crate::executor::compiler::Runner::admitted(prepared)?;
        let compiler: Arc<dyn SourceCompiler> = Arc::new(runner);
        Ok(CompilerPolicy::Confined(Some(compiler)))
    });
    let _ = POLICY.set(result);
}

fn deny_write_directory(directory: &Path) -> Result<(), String> {
    let probe = directory.join(format!(
        ".compiler-native-probe-{}",
        uuid::Uuid::new_v4().simple()
    ));
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&probe)
    {
        Ok(file) => {
            drop(file);
            let _ = std::fs::remove_file(&probe);
            Err("The native compiler generation remains writable.".into())
        }
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => Ok(()),
        Err(_) => Err("The native compiler directory boundary could not be verified.".into()),
    }
}
fn deny_write_file(path: &Path) -> Result<(), String> {
    match std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(_) => Err("A protected native compiler file remains writable.".into()),
        Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => Ok(()),
        Err(_) => Err("The native compiler file boundary could not be verified.".into()),
    }
}

/// An embedded interpreter may already have enforced its boundary. It cannot reconstruct
/// host compiler selection after site/import startup, so retain explicit refusal.
pub(super) fn inherit_without_compiler() {
    let _ = POLICY.set(Ok(CompilerPolicy::Confined(None)));
}

pub(super) fn verdict() -> Result<(), String> {
    match POLICY.get() {
        Some(Ok(_)) => Ok(()),
        Some(Err(error)) => Err(error.clone()),
        None => {
            Err("The API process did not capture its native compiler policy before startup.".into())
        }
    }
}
pub(super) fn policy() -> Option<CompilerPolicy> {
    if let Some(policy) = POLICY.get() {
        return Some(policy.clone().unwrap_or(CompilerPolicy::Confined(None)));
    }
    (sandbox::is_native_api()
        || std::env::var_os(meerschaum_core::team_scope::api_execution::PROFILE_VAR).is_some())
    .then_some(CompilerPolicy::Confined(None))
}
