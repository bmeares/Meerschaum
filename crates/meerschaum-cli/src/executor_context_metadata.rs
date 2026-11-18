//! Main-host cache eligibility from sealed host state, never a worker environment or reply.

use super::*;

pub(super) fn attest(
    contexts: &Contexts,
    id: Uuid,
    revision: u64,
    name: &str,
    catalog_sha256: &str,
    deadline: i64,
) -> Result<(), String> {
    let refused = || {
        "The canonical Host metadata job has no exact completed sealed Metadata generation."
            .to_string()
    };
    let expected_compiler = contexts.source_compiler_identity();
    let (identity, generation, compiler) = {
        let inner = contexts
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let entry = inner.entries.get(&id).ok_or_else(refused)?;
        if entry.revision != revision
            || entry.identity.profile != Profile::Metadata
            || entry.identity.target != (Target::Job { name: name.into() })
            || entry.identity.discovery_catalog_sha256.as_deref() != Some(catalog_sha256)
            || entry.identity.metadata_deadline != Some(deadline)
            || entry.identity.source_compiler_sha256 != expected_compiler
            || entry
                .compiler
                .as_ref()
                .map(|binding| binding.manifest_sha256.as_str())
                != expected_compiler.as_deref()
            || deadline <= chrono::Utc::now().timestamp()
        {
            return Err(refused());
        }
        let proof = ContextProof::read(
            &contexts
                .root
                .join(id.simple().to_string())
                .join("identity.json"),
        )?;
        if proof.id != id
            || proof.revision != revision
            || proof.identity != entry.identity
            || !proof.attempted
            || proof.generation != entry.generation
            || proof.compiler != entry.compiler
        {
            return Err(refused());
        }
        let generation = entry.generation.clone().ok_or_else(refused)?;
        generation.validate(name).map_err(|_| refused())?;
        if entry.compiler.is_some() {
            let spec = generation
                .directory
                .join(meerschaum_core::team_scope::sandbox::JOB_SPEC_FILE);
            let raw = crate::executor_profiles::read_compiler_descriptor(&spec)?;
            let spec_model: meerschaum_core::team_scope::sandbox::Spec =
                serde_json::from_slice(&raw).map_err(|_| refused())?;
            if spec_model
                .runtime
                .as_ref()
                .and_then(|runtime| runtime.native_compiler.as_ref())
                != entry.compiler.as_ref()
            {
                return Err(refused());
            }
            let bytes = crate::executor_profiles::read_compiler_descriptor(
                &generation
                    .directory
                    .join(crate::executor_profiles::COMPILER_FILE),
            )?;
            let prepared: crate::executor_compiler::Prepared =
                serde_json::from_slice(&bytes).map_err(|_| refused())?;
            if prepared.owner_deadline != deadline {
                return Err(refused());
            }
            let binding = entry.compiler.as_ref().ok_or_else(refused)?;
            if meerschaum_server::executor::plugins::sha256(&bytes) != binding.prepared_sha256 {
                return Err(refused());
            }
            crate::executor_profiles::validate_compiler_material(
                &spec_model,
                &generation.directory,
                &prepared,
                binding,
            )?;
        }
        (entry.identity.clone(), generation, entry.compiler.clone())
    };
    // Supervisor cleanup precedes final-result publication, but the supervisor can still be
    // returning from that publication. Wait only for this exact birth-bound generation; an
    // unreadable, missing or malformed lifecycle record is never a transient success.
    let until = std::time::Instant::now() + std::time::Duration::from_secs(2);
    loop {
        match generation.inactive() {
            Ok(true) => break,
            Ok(false) if std::time::Instant::now() < until => {
                std::thread::sleep(std::time::Duration::from_millis(20));
            }
            _ => return Err(refused()),
        }
    }
    let _completed = generation
        .metadata_result(deadline)
        .map_err(|_| refused())?;
    let inner = contexts
        .inner
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let entry = inner.entries.get(&id).ok_or_else(refused)?;
    if entry.revision != revision
        || entry.identity != identity
        || entry.generation.as_ref() != Some(&generation)
        || entry.compiler != compiler
        || deadline <= chrono::Utc::now().timestamp()
    {
        return Err(refused());
    }
    Ok(())
}

/// A response authority signs only this exact native completed generation. Catalog/config
/// input alone never constructs a completion; the sealed Runtime is checked against the
/// constructor's independently measured fixed installation before its receipt is returned.
pub(super) fn quote(
    contexts: &Contexts,
    actor: &meerschaum_server::executor::control::Actor,
    request: &meerschaum_server::executor::factory_attestation::quote::Request,
    manifest: &meerschaum_server::executor::factory_artifacts::Manifest,
    authority: &meerschaum_server::executor::factory_attestation::Authority,
    configuration_sha256: &str,
) -> Result<meerschaum_server::executor::factory_attestation::quote::Response, String> {
    let refused = || {
        "The native Metadata result does not match the protected Factory installation.".to_string()
    };
    request.validate().map_err(|_| refused())?;
    let name = format!("mrsm-{}", request.context.simple());
    attest(
        contexts,
        request.context,
        request.revision,
        &name,
        &request.catalog_sha256,
        request.deadline,
    )?;
    let (identity, generation, compiler) = {
        let inner = contexts
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let entry = inner.entries.get(&request.context).ok_or_else(refused)?;
        if entry.identity.owner != actor.owner
            || entry.revision != request.revision
            || entry.identity.profile != Profile::Metadata
            || entry.identity.discovery_catalog_sha256.as_deref()
                != Some(request.catalog_sha256.as_str())
            || entry.identity.metadata_deadline != Some(request.deadline)
        {
            return Err(refused());
        }
        (
            entry.identity.clone(),
            entry.generation.clone().ok_or_else(refused)?,
            entry.compiler.clone(),
        )
    };
    let spec_path = generation
        .directory
        .join(meerschaum_core::team_scope::sandbox::JOB_SPEC_FILE);
    // Bounded, NOFOLLOW/CLOEXEC descriptor read; Spec::read is intentionally not used.
    let raw = crate::executor_profiles::read_compiler_descriptor(&spec_path)?;
    let spec: meerschaum_core::team_scope::sandbox::Spec =
        serde_json::from_slice(&raw).map_err(|_| refused())?;
    let runtime = spec.runtime.as_ref().ok_or_else(refused)?;
    // The current runtime requires Python even for native-source metadata. SDK-free image
    // provisioning is valid, but its execution eligibility needs a separate native-only path.
    let sdk = manifest.sdk.as_ref().ok_or_else(refused)?;
    let package = PathBuf::from(&sdk.root).join("meerschaum/__init__.py");
    let shim = PathBuf::from(&sdk.root).join("_meerschaum_mrsm.py");
    if spec.mode != meerschaum_core::team_scope::sandbox::Mode::RequireSockets
        || !runtime.api_only
        || runtime.launcher != std::path::Path::new(&manifest.cli.path)
        || runtime.packages.get("meerschaum") != Some(&package)
        || runtime.packages.get("_meerschaum_mrsm") != Some(&shim)
        || runtime.sdk_pycache_prefix.as_deref()
            != Some(
                generation
                    .directory
                    .join(crate::executor_factory_sdk::PYCACHE)
                    .as_path(),
            )
        || runtime.native_compiler != compiler
        || compiler
            .as_ref()
            .map(|compiler| compiler.manifest_sha256.as_str())
            != manifest
                .compiler
                .as_ref()
                .map(|compiler| compiler.manifest_sha256.as_str())
    {
        return Err(refused());
    }
    crate::executor_factory_sdk::validate_prefix(
        runtime.sdk_pycache_prefix.as_deref().ok_or_else(refused)?,
        &generation.directory,
    )?;
    if let Some(binding) = &compiler {
        let bytes = crate::executor_profiles::read_compiler_descriptor(
            &generation
                .directory
                .join(crate::executor_profiles::COMPILER_FILE),
        )?;
        if meerschaum_server::executor::plugins::sha256(&bytes) != binding.prepared_sha256 {
            return Err(refused());
        }
        let prepared: crate::executor_compiler::Prepared =
            serde_json::from_slice(&bytes).map_err(|_| refused())?;
        if prepared.owner_deadline != request.deadline {
            return Err(refused());
        }
        crate::executor_profiles::validate_compiler_material(
            &spec,
            &generation.directory,
            &prepared,
            binding,
        )?;
    }
    let completed = generation
        .metadata_result(request.deadline)
        .map_err(|_| refused())?;
    // No await or re-capture after this final Context check. Retiring, rotating or preparing
    // a replacement entry while the receipt was read invalidates the signing operation.
    let inner = contexts
        .inner
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let entry = inner.entries.get(&request.context).ok_or_else(refused)?;
    if entry.revision != request.revision
        || entry.identity != identity
        || entry.generation.as_ref() != Some(&generation)
        || entry.compiler != compiler
        || request.deadline <= chrono::Utc::now().timestamp()
    {
        return Err(refused());
    }
    // Sign synchronously while retaining the same Context lock; a concurrent retirement
    // cannot slip between the final state proof and creation of its native response.
    authority
        .metadata_quote(
            request,
            configuration_sha256,
            actor,
            &completed,
            chrono::Utc::now().timestamp(),
        )
        .map_err(|_| refused())
}
