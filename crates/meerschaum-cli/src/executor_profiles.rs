//! Preparation for API-only launches. Admission remains with the fresh original source.
//!
//! This module never reads a Python manifest or starts an interpreter. Every executable and
//! import path is supplied by the trusted host. The standalone CLI installs the generated
//! mandatory sandbox before any discovery; an embedded Python launcher is not sufficient,
//! since Python may have executed site hooks before entering Rust.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use meerschaum_core::team_scope::api_execution::{self, PROFILE_VAR};
use meerschaum_core::team_scope::sandbox::{self, Access, Mode, Rule, Spec};
use meerschaum_core::team_scope::{create_private_dir, write_private};
use meerschaum_core::{Map, Value};

/// Paths selected by the host, never supplied by an execution-context HTTP request.
pub struct Inputs<'a> {
    /// Main-created credential-free discovery, with no job-metadata or network access.
    pub metadata: bool,
    /// Private executor/server root, containing credentials that must be denied.
    pub server_root: &'a Path,
    /// Generated, launch-specific directory containing no other launch's files.
    pub directory: &'a Path,
    /// Already filtered configuration; only its known connector label names are retained.
    pub base: &'a Map,
    /// Supported logical instances. Source/plugin connectors need separate explicit support.
    pub logical_instances: &'a BTreeSet<String>,
    /// The default logical instance (must be admitted above).
    pub default_instance: &'a str,
    /// Validated main callback origin.
    pub callback_url: &'a str,
    /// Trusted standalone mrsm, Python prefixes/package paths, and granted plugin sources.
    /// Never grant a shared Team state/venv directory here.
    pub read_exec: &'a [PathBuf],
    /// The exact job's read-only metadata directory, if needed by its supervisor protocol.
    pub job_dir: Option<&'a Path>,
    /// Trusted runtime environment. A narrow allowlist is applied without passthrough.
    pub environment: &'a [(String, String)],
}

/// Prepared material contains no bearer bytes. Credential rotation is owned by Contexts.
#[derive(Debug)]
pub struct Prepared {
    /// Fresh sealed generation. A prior child cannot read or modify this generation.
    pub directory: PathBuf,
    /// Complete environment: spawn with env_clear, never merge persisted user environment.
    pub environment: Vec<(String, String)>,
    /// The credential path the supervisor rotates atomically.
    pub credential_file: PathBuf,
    /// Mandatory spec, also useful to the host's standalone process acceptance probe.
    pub spec: Spec,
}

/// Create an isolated launch root and mandatory spec. Preparation itself is not sandbox proof;
/// the launched process must produce an active successful RequireSockets report.
pub fn prepare(inputs: &Inputs<'_>) -> Result<Prepared, String> {
    let root = absolute_directory(inputs.server_root)?;
    let directory = absolute_directory(inputs.directory)?;
    if directory == root || !directory.starts_with(&root) {
        return Err("An API execution context must be a private child of the runtime root.".into());
    }
    // A host may not accidentally select the server root or a sibling context as writable state.
    private_directory(inputs.directory)?;
    let directory =
        std::fs::canonicalize(&directory).map_err(|_| "Invalid API execution directory.")?;
    if !directory.starts_with(&root) {
        return Err("The API execution directory cannot escape the runtime root.".into());
    }
    let directory = fresh_generation(&directory)?;
    let result = prepare_generation(inputs, root, directory.clone());
    if result.is_err() {
        // No child has received this fresh path. Never traverse an older mutable state here.
        let _ = std::fs::remove_dir_all(directory);
    }
    result
}

fn prepare_generation(
    inputs: &Inputs<'_>,
    root: PathBuf,
    directory: PathBuf,
) -> Result<Prepared, String> {
    let credential_file = directory.join("credential.json");
    let config_file = directory.join("config.json");
    let config_dir = directory.join("config");
    // A job uses the supervisor's existing private result/heartbeat protocol. Its writable
    // root is beside this context's spec, never the trusted jobs/<name> metadata directory.
    let state = directory.join(if inputs.job_dir.is_some() {
        sandbox::JOB_WORK_DIR
    } else {
        "state"
    });
    for path in [&config_dir, &state] {
        private_directory(path)?;
    }
    let paths: Vec<PathBuf> = ["tmp", "cache", "venvs", "plugins", "home", "shell-state"]
        .iter()
        .map(|name| state.join(name))
        .collect();
    for path in &paths {
        private_directory(path)?;
    }
    // Discovery has no caller data-plane transport, logical connector labels, or settings.
    // The sentinel file supplies only a lifecycle deadline; it is never an API credential.
    let empty = Map::new();
    let metadata_instances = BTreeSet::from(["api:metadata".to_string()]);
    let mut config = if inputs.metadata {
        api_execution::configuration(
            &empty,
            &metadata_instances,
            "api:metadata",
            meerschaum_server::executor::environment::METADATA_ORIGIN,
            &credential_file,
        )?
    } else {
        api_execution::configuration(
            inputs.base,
            inputs.logical_instances,
            inputs.default_instance,
            inputs.callback_url,
            &credential_file,
        )?
    };
    if let Some(value) = inputs
        .base
        .get(meerschaum_server::executor::plugins::CONFIG_KEY)
    {
        // Only the typed, credential-free source catalog may cross this boundary. Never
        // copy arbitrary Main/Team settings or repository capability bytes into B.
        let bytes = value.to_json_string().into_bytes();
        let catalog: meerschaum_server::executor::plugins::Catalog =
            serde_json::from_slice(&bytes).map_err(|_| "Invalid host-selected plugin catalog.")?;
        let grants: BTreeSet<String> = inputs
            .environment
            .iter()
            .rev()
            .find(|(key, _)| key == "MRSM_EXECUTOR_PLUGINS")
            .map(|(_, value)| serde_json::from_str(value))
            .transpose()
            .map_err(|_| "Invalid host-selected plugin grants.")?
            .unwrap_or_default();
        let reserved = crate::actions::CORE_ACTIONS
            .iter()
            .map(|action| action.name.to_string())
            .collect();
        let catalog = match &catalog.placement {
            meerschaum_server::executor::plugin_host::Placement::Remote => {
                meerschaum_server::executor::plugins::Validated::decode(
                    &bytes,
                    meerschaum_server::executor::plugins::Expected {
                        team_id: catalog.team_id,
                        generation: catalog.generation,
                        grants: &grants,
                        reserved_actions: &reserved,
                    },
                )
            }
            meerschaum_server::executor::plugin_host::Placement::Host(host) => {
                meerschaum_server::executor::plugins::Validated::decode_host(
                    &bytes, host, &grants, &reserved,
                )
            }
        }
        .map_err(|_| "The plugin catalog does not match this API source view.")?;
        config.insert(
            meerschaum_server::executor::plugins::CONFIG_KEY.into(),
            Value::from_json_str(
                &String::from_utf8(
                    catalog
                        .encode()
                        .map_err(|_| "Invalid plugin catalog encoding.")?,
                )
                .map_err(|_| "Invalid plugin catalog encoding.")?,
            )
            .map_err(|_| "Invalid plugin catalog encoding.")?,
        );
    }
    if let Some(value) = inputs
        .base
        .get(meerschaum_server::executor::registration::CONFIG_KEY)
    {
        if inputs.metadata {
            return Err(
                "Credential-free discovery cannot receive caller registration parameters.".into(),
            );
        }
        let registration: meerschaum_server::executor::registration::Registration =
            serde_json::from_str(&value.to_json_string())
                .map_err(|_| "Invalid Main-selected registration descriptor.")?;
        registration
            .validate()
            .map_err(|error| error.detail_text())?;
        config.insert(
            meerschaum_server::executor::registration::CONFIG_KEY.into(),
            value.clone(),
        );
    }
    if let Some(value) = inputs
        .base
        .get(meerschaum_server::executor::extensions::CONFIG_KEY)
    {
        if inputs.metadata {
            return Err("Metadata discovery cannot receive extension invocation input.".into());
        }
        let extension: meerschaum_server::executor::extensions::Operation =
            serde_json::from_str(&value.to_json_string())
                .map_err(|_| "Invalid Main-selected extension descriptor.")?;
        extension.validated().map_err(|error| error.detail_text())?;
        config.insert(
            meerschaum_server::executor::extensions::CONFIG_KEY.into(),
            value.clone(),
        );
    }
    write_private(&config_file, &Value::Map(config).to_json_string())
        .map_err(|_| "Could not write the API execution configuration.")?;
    let canary =
        sandbox::ensure_canary(&root).map_err(|_| "Could not prepare the sandbox canary.")?;
    let mut protected = vec![
        PathBuf::from("/"),
        root.clone(),
        root.join("config"),
        root.join("jobs"),
        root.join("execution-contexts"),
        root.join("team_scopes"),
        root.join("venvs"),
        root.join("plugins"),
    ];
    if let Some(home) = std::env::var_os("HOME").filter(|h| !h.is_empty()) {
        protected.push(home.into());
    }
    let mut spec = Spec {
        mode: Mode::RequireSockets,
        team: "API execution".into(),
        protected,
        canary,
        ..Spec::default()
    };
    let mut rules: Vec<Rule> = sandbox::SYSTEM_RULES
        .iter()
        // B never needs to connect to a privileged system Unix service, even with ABI 9.
        .filter(|(_, access, _)| *access != Access::Connect)
        .map(|(path, access, why)| Rule::new(*path, *access, why))
        .collect();
    rules.push(Rule::new(
        &directory,
        Access::Read,
        "this launch's configuration and rotating credential",
    ));
    rules.push(Rule::new(
        &state,
        Access::ReadWrite,
        "this launch's private writable state",
    ));
    for path in inputs.read_exec {
        let canonical =
            std::fs::canonicalize(path).map_err(|_| "A trusted runtime path is unavailable.")?;
        // Reading these trees would recover Team/main credentials or another launch's bearer.
        if canonical.starts_with(root.join("execution-contexts"))
            || canonical.starts_with(root.join("team_scopes"))
            || canonical.starts_with(root.join("config"))
            || canonical.starts_with(root.join("jobs"))
            || canonical.starts_with(root.join("venvs"))
        {
            return Err(
                "A shared credential or writable runtime path cannot be granted to API execution."
                    .into(),
            );
        }
        rules.push(Rule::new(
            canonical,
            Access::ReadExec,
            "host-selected runtime or granted source",
        ));
    }
    if let Some(job) = inputs.job_dir.filter(|_| !inputs.metadata) {
        let job = absolute_directory(job)?;
        if !job.starts_with(root.join("jobs")) || job == root.join("jobs") {
            return Err("The API launch job directory must name its exact runtime job.".into());
        }
        rules.push(Rule::new(job, Access::Read, "this launch's job metadata"));
    }
    let (allowed, dropped) = spec.allowed(rules);
    if dropped
        .iter()
        .any(|r| r.why == "host-selected runtime or granted source")
    {
        return Err("A trusted runtime path covers protected API data.".into());
    }
    spec.rules = allowed;
    let spec_file = directory.join(if inputs.job_dir.is_some() {
        sandbox::JOB_SPEC_FILE
    } else {
        sandbox::SPEC_FILE
    });
    write_private(
        &spec_file,
        &serde_json::to_string(&spec).map_err(|_| "Could not encode the API sandbox.")?,
    )
    .map_err(|_| "Could not write the API sandbox.")?;
    let mut environment: Vec<(String, String)> = inputs
        .environment
        .iter()
        .filter(|(key, _)| {
            if inputs.metadata && matches!(key.as_str(), "SSL_CERT_FILE" | "SSL_CERT_DIR") {
                return false;
            }
            matches!(
                key.as_str(),
                "PATH"
                    | "LANG"
                    | "LANGUAGE"
                    | "TZ"
                    | "TERM"
                    | "COLORTERM"
                    | "NO_COLOR"
                    | "MRSM_BIN"
                    | "MRSM_PYTHON"
                    | "PYTHONPATH"
                    | "SSL_CERT_FILE"
                    | "SSL_CERT_DIR"
            ) || key.starts_with("LC_")
        })
        .cloned()
        .collect();
    for (key, value) in [
        ("MRSM_ROOT_DIR", state.as_path()),
        ("MRSM_CONFIG_FILE", config_file.as_path()),
        ("MRSM_CONFIG_DIR", config_dir.as_path()),
        ("MRSM_TEAM_SANDBOX", spec_file.as_path()),
        ("MRSM_PLUGINS_DIR", paths[3].as_path()),
        ("MRSM_VENVS_DIR", paths[2].as_path()),
        ("HOME", paths[4].as_path()),
        ("TMPDIR", paths[0].as_path()),
        ("XDG_CACHE_HOME", paths[1].as_path()),
    ] {
        environment.retain(|(name, _)| name != key);
        environment.push((key.into(), value.to_string_lossy().into_owned()));
    }
    if inputs.metadata {
        environment.push((
            meerschaum_server::executor::environment::METADATA_VAR.into(),
            "1".into(),
        ));
        environment.push(("MRSM_NONINTERACTIVE".into(), "1".into()));
    }
    for (key, value) in [
        (PROFILE_VAR, "1"),
        ("PYTHONNOUSERSITE", "1"),
        ("PYTHONSAFEPATH", "1"),
        ("MRSM_NO_UPDATE_CHECK", "1"),
        ("MRSM_TEAM_SCOPE", "API execution"),
    ] {
        environment.retain(|(name, _)| name != key);
        environment.push((key.into(), value.into()));
    }
    Ok(Prepared {
        directory,
        environment,
        credential_file,
        spec,
    })
}

/// Prepare a production launch that may start Python workers. Runtime paths come from the
/// registered native host without executing an interpreter. The native trampoline installs
/// the mandatory boundary before restoring import paths or running any Python site hook.
pub fn prepare_with_runtime(inputs: &Inputs<'_>) -> Result<Prepared, String> {
    let mut prepared = prepare(inputs)?;
    let directory = prepared.directory.clone();
    let result = complete_runtime(inputs, &mut prepared);
    if let Err(error) = result {
        let _ = std::fs::remove_dir_all(directory);
        return Err(error);
    }
    Ok(prepared)
}

fn complete_runtime(inputs: &Inputs<'_>, prepared: &mut Prepared) -> Result<(), String> {
    let base_rules = prepared.spec.rules.len();
    let mut runtime = meerschaum_core::team_scope::sandbox::runtime::Runtime::prepare(
        &mut prepared.spec,
        inputs.environment,
    )
    .map_err(|error| error.to_string())?;
    // A Team runtime may legitimately use its shared plugin environments; B's runtime may
    // read only host packages and this launch's state. Apply the stricter profile after the
    // common runtime resolver has canonicalized every executable and import/library grant.
    let root = absolute_directory(inputs.server_root)?;
    let directory = &prepared.directory;
    let forbidden = [
        "config",
        "jobs",
        "team_scopes",
        "venvs",
        "execution-contexts",
    ]
    .map(|name| root.join(name));
    if prepared.spec.rules.iter().skip(base_rules).any(|rule| {
        !rule.path.starts_with(directory)
            && forbidden.iter().any(|path| rule.path.starts_with(path))
    }) {
        return Err(
            "The API interpreter cannot read another launch or shared Team runtime.".into(),
        );
    }
    prepared
        .environment
        .retain(|(name, _)| !meerschaum_core::team_scope::sandbox::runtime::injection_name(name));
    for (name, value) in [
        ("MRSM_BIN", runtime.launcher.to_string_lossy().into_owned()),
        ("MRSM_PYTHON", runtime.python.to_string_lossy().into_owned()),
    ] {
        prepared.environment.retain(|(key, _)| key != name);
        prepared.environment.push((name.into(), value));
    }
    runtime.api_only = true;
    prepared.spec.runtime = Some(runtime);
    write_private(
        &directory.join(if inputs.job_dir.is_some() {
            sandbox::JOB_SPEC_FILE
        } else {
            sandbox::SPEC_FILE
        }),
        &serde_json::to_string(&prepared.spec).map_err(|_| "Could not encode the API runtime.")?,
    )
    .map_err(|_| "Could not write the API runtime.")?;
    Ok(())
}

/// Fixed generation sibling; neither environment nor public requests choose this path.
pub(crate) const COMPILER_FILE: &str = "native-compiler.json";
pub(crate) const COMPILER_TOOLCHAIN: &str = "host-owned immutable native compiler toolchain";
pub(crate) const COMPILER_VENDOR: &str = "host-owned immutable native compiler vendor";
pub(crate) const COMPILER_ANCHOR: &str = "host-owned config-free native compiler anchor";

/// Attach only to this fresh, never-launched generation. Renewal must reuse its material.
pub(crate) fn prepare_compiler(
    profile: &mut Prepared,
    compiler: &crate::executor_compiler::Compiler,
    deadline: i64,
) -> Result<sandbox::runtime::NativeCompilerBinding, String> {
    let state = profile
        .environment
        .iter()
        .find(|(key, _)| key == "MRSM_ROOT_DIR")
        .map(|(_, value)| PathBuf::from(value))
        .ok_or("Missing private compiler state.")?;
    let prepared = compiler.prepare(&profile.directory, &state, deadline)?;
    let bytes =
        serde_json::to_vec(&prepared).map_err(|_| "Could not encode the private compiler.")?;
    let binding = sandbox::runtime::NativeCompilerBinding {
        manifest_sha256: compiler.identity().to_string(),
        prepared_sha256: meerschaum_server::executor::plugins::sha256(&bytes),
    };
    let mut rules = vec![
        Rule::new(compiler.toolchain(), Access::ReadExec, COMPILER_TOOLCHAIN),
        Rule::new(compiler.vendor(), Access::ReadExec, COMPILER_VENDOR),
        Rule::new(compiler.anchor(), Access::Read, COMPILER_ANCHOR),
        Rule::new(
            compiler
                .toolchain()
                .parent()
                .ok_or("Missing compiler snapshot.")?
                .join("compiler.json"),
            Access::Read,
            "host-owned immutable native compiler manifest",
        ),
    ];
    rules.extend(prepared.lock_rules());
    let (rules, dropped) = profile.spec.allowed(rules);
    if !dropped.is_empty() {
        return Err("The owned compiler covers protected runtime data.".into());
    }
    profile.spec.rules.extend(rules);
    profile
        .spec
        .runtime
        .as_mut()
        .ok_or("The private compiler requires a native runtime.")?
        .native_compiler = Some(binding.clone());
    validate_compiler_material(&profile.spec, &profile.directory, &prepared, &binding)?;
    write_private(
        &profile.directory.join(COMPILER_FILE),
        std::str::from_utf8(&bytes).map_err(|_| "Invalid compiler material.")?,
    )
    .map_err(|_| "Could not persist the private compiler material.")?;
    let spec = profile
        .environment
        .iter()
        .find(|(key, _)| key == sandbox::SPEC_VAR)
        .map(|(_, value)| PathBuf::from(value))
        .ok_or("Missing private compiler spec.")?;
    write_private(
        &spec,
        &serde_json::to_string(&profile.spec).map_err(|_| "Invalid compiler runtime.")?,
    )
    .map_err(|_| "Could not persist the private compiler runtime.")?;
    Ok(binding)
}

/// Verify the fixed descriptor against this exact native spec. This validates bounded data;
/// host provenance additionally comes from protected Context state / native startup capture.
pub(crate) fn validate_compiler_material(
    spec: &Spec,
    directory: &Path,
    prepared: &crate::executor_compiler::Prepared,
    binding: &sandbox::runtime::NativeCompilerBinding,
) -> Result<(), String> {
    let refuse =
        || "The compiler does not match its exact protected native generation.".to_string();
    let runtime = spec.runtime.as_ref().ok_or_else(refuse)?;
    if spec.mode != Mode::RequireSockets
        || !runtime.api_only
        || runtime.native_compiler.as_ref() != Some(binding)
        || prepared.identity != binding.manifest_sha256
        || !meerschaum_server::executor::plugins::valid_digest(&binding.manifest_sha256)
        || !meerschaum_server::executor::plugins::valid_digest(&binding.prepared_sha256)
        || prepared.state.parent() != Some(directory)
        || !matches!(
            prepared.state.file_name().and_then(|name| name.to_str()),
            Some("state" | ".mrsm_team_work")
        )
        || prepared.cargo_home != directory.join("native-cargo-home")
        || prepared.lock_files
            != [".package-cache", ".package-cache-mutate"]
                .iter()
                .map(|name| prepared.cargo_home.join(name))
                .collect::<Vec<_>>()
    {
        return Err(refuse());
    }
    let toolchain = prepared
        .cargo
        .parent()
        .and_then(Path::parent)
        .ok_or_else(refuse)?;
    let snapshot = toolchain.parent().ok_or_else(refuse)?;
    if toolchain.file_name().and_then(|name| name.to_str()) != Some("toolchain")
        || prepared.cargo != toolchain.join("bin/cargo")
        || prepared.rustc != toolchain.join("bin/rustc")
        || prepared.vendor != snapshot.join("vendor")
    {
        return Err(refuse());
    }
    for path in [
        directory,
        prepared.state.as_path(),
        toolchain,
        prepared.cargo.as_path(),
        prepared.rustc.as_path(),
        prepared.vendor.as_path(),
        prepared.cwd.as_path(),
        prepared.cargo_home.as_path(),
    ] {
        if !path.is_absolute() || std::fs::canonicalize(path).ok().as_deref() != Some(path) {
            return Err(refuse());
        }
    }
    for (path, access, why) in [
        (toolchain, Access::ReadExec, COMPILER_TOOLCHAIN),
        (prepared.vendor.as_path(), Access::ReadExec, COMPILER_VENDOR),
        (prepared.cwd.as_path(), Access::Read, COMPILER_ANCHOR),
    ] {
        if !spec
            .rules
            .iter()
            .any(|rule| rule.path == path && rule.access == access && rule.why == why)
        {
            return Err(refuse());
        }
    }
    let manifest = snapshot.join("compiler.json");
    if !spec.rules.iter().any(|rule| {
        rule.path == manifest
            && rule.access == Access::Read
            && rule.why == "host-owned immutable native compiler manifest"
    }) || meerschaum_server::executor::plugins::sha256(&read_compiler_descriptor(&manifest)?)
        != binding.manifest_sha256
        || [toolchain, prepared.vendor.as_path(), prepared.cwd.as_path()]
            .iter()
            .any(|path| path.starts_with(&prepared.state) || prepared.state.starts_with(path))
    {
        return Err(refuse());
    }
    for path in &prepared.lock_files {
        if !spec
            .rules
            .iter()
            .any(|rule| rule.path == *path && rule.access == Access::ReadWrite)
            || !std::fs::symlink_metadata(path)
                .is_ok_and(|metadata| metadata.is_file() && !metadata.is_symlink())
        {
            return Err(refuse());
        }
    }
    // An exact toolchain may not alias selected source/runtime-write grants. An inherited
    // kernel boundary must remain an upper bound even if a descendant invents new JSON.
    let protected = [
        directory.to_path_buf(),
        toolchain.to_path_buf(),
        prepared.vendor.clone(),
        prepared.cwd.clone(),
    ];
    for rule in &spec.rules {
        if matches!(
            rule.access,
            Access::ReadWrite
                | Access::DataFile
                | Access::Device
                | Access::SharedMemory
                | Access::Lock
        ) && protected
            .iter()
            .any(|path| path.starts_with(&rule.path) || rule.path.starts_with(path))
            && !prepared.lock_files.contains(&rule.path)
            && rule.path != prepared.state
        {
            return Err(refuse());
        }
        if rule.why == "host-selected runtime or granted source"
            && [toolchain, prepared.vendor.as_path(), prepared.cwd.as_path()]
                .iter()
                .any(|path| path.starts_with(&rule.path) || rule.path.starts_with(path))
        {
            return Err(refuse());
        }
    }
    Ok(())
}

/// A host attests only the retained exact generation, never a worker's claimed compiler.
pub(crate) fn read_compiler_binding(
    spec_path: &Path,
) -> Result<Option<sandbox::runtime::NativeCompilerBinding>, String> {
    let spec = Spec::read(spec_path).map_err(|_| "The compiler spec is unavailable.")?;
    let binding = spec
        .runtime
        .as_ref()
        .and_then(|runtime| runtime.native_compiler.clone());
    let directory = spec_path.parent().ok_or("Missing compiler generation.")?;
    if let Some(binding) = &binding {
        let bytes = read_compiler_descriptor(&directory.join(COMPILER_FILE))?;
        if meerschaum_server::executor::plugins::sha256(&bytes) != binding.prepared_sha256 {
            return Err("The protected compiler descriptor changed.".into());
        }
        let prepared =
            serde_json::from_slice(&bytes).map_err(|_| "Invalid compiler descriptor.")?;
        validate_compiler_material(&spec, directory, &prepared, binding)?;
    }
    Ok(binding)
}

pub(crate) fn read_compiler_descriptor(path: &Path) -> Result<Vec<u8>, String> {
    use std::io::Read;
    #[cfg(not(unix))]
    {
        let _ = path;
        return Err("Confined compilation requires supported Unix.".into());
    }
    #[cfg(unix)]
    {
        let file = std::fs::File::from(
            rustix::fs::open(
                path,
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| "The protected compiler descriptor is unavailable.")?,
        );
        let metadata = file
            .metadata()
            .map_err(|_| "The compiler descriptor is unavailable.")?;
        if !metadata.is_file() || metadata.len() > 2 * 1024 * 1024 {
            return Err("Invalid bounded compiler descriptor.".into());
        }
        let mut bytes = Vec::new();
        file.take(2 * 1024 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "Could not read the compiler descriptor.")?;
        if bytes.len() > 2 * 1024 * 1024 {
            return Err("Compiler descriptor too large.".into());
        }
        Ok(bytes)
    }
}

const MAX_OUTSTANDING_GENERATIONS: usize = 64;

fn fresh_generation(directory: &Path) -> Result<PathBuf, String> {
    // Same-revision credential renewals reuse their retained environment and never enter
    // preparation. Bound abandoned/old staging entries until the owning lifecycle retires
    // the context; this is a backstop, not permission to reuse child-writable state.
    let count = std::fs::read_dir(directory)
        .map_err(|_| "The API launch directory is unavailable.")?
        .try_fold(0usize, |count, entry| {
            let entry = entry.map_err(|_| "The API launch directory is unavailable.")?;
            Ok::<_, String>(
                count
                    + usize::from(
                        entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("generation-"),
                    ),
            )
        })?;
    if count >= MAX_OUTSTANDING_GENERATIONS {
        return Err(
            "Too many unretired API runtime generations exist. An administrator must verify their lifecycle proofs and retire uncertain staging before another runtime can be prepared."
                .into(),
        );
    }
    let path = directory.join(format!("generation-{}", uuid::Uuid::new_v4().simple()));
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder
        .create(&path)
        .map_err(|_| "Could not create a fresh API launch generation.")?;
    Ok(path)
}

fn absolute_directory(path: &Path) -> Result<PathBuf, String> {
    if !path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        return Err("API execution paths must be absolute and generated by the host.".into());
    }
    // Resolve existing ancestors too, so a symlinked runtime root has one identity.
    if path.exists() {
        std::fs::canonicalize(path).map_err(|_| "Invalid API execution path.".into())
    } else {
        let parent = path.parent().ok_or("Invalid API execution path.")?;
        let parent = absolute_directory(parent)?;
        Ok(parent.join(path.file_name().ok_or("Invalid API execution path.")?))
    }
}

pub(crate) fn private_directory(path: &Path) -> Result<(), String> {
    // Call only below sealed host-owned ancestors or a fresh, never-launched generation.
    // These checks reject malformed inputs; they cannot make a writable ancestor race-safe.
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
                return Err("An API execution state path is not a plain directory.".into());
            }
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err("An API execution state path is unavailable.".into()),
        }
    }
    create_private_dir(path).map_err(|_| "Could not prepare the API execution state.".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn fresh_preparation_never_revisits_old_mutable_ancestors_and_failure_discards_only_staging() {
        use std::os::unix::fs::PermissionsExt;
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let directory = root.join("execution-contexts/one");
        let instances = BTreeSet::from(["sql:main".into()]);
        let mut inputs = Inputs {
            metadata: false,
            server_root: &root,
            directory: &directory,
            base: &Map::new(),
            logical_instances: &instances,
            default_instance: "sql:main",
            callback_url: "https://main.example",
            read_exec: &[],
            job_dir: None,
            environment: &[],
        };
        let first = prepare(&inputs).unwrap();
        let outside = root.join("outside-state");
        std::fs::create_dir(&outside).unwrap();
        std::fs::set_permissions(&outside, std::fs::Permissions::from_mode(0o750)).unwrap();
        let tmp = first.directory.join("state/tmp");
        std::fs::remove_dir(&tmp).unwrap();
        std::os::unix::fs::symlink(&outside, &tmp).unwrap();
        let second = prepare(&inputs).unwrap();
        assert_ne!(first.directory, second.directory);
        for (spec, other) in [
            (&first.spec, &second.directory),
            (&second.spec, &first.directory),
        ] {
            assert!(!spec.rules.iter().any(|rule| other.starts_with(&rule.path)));
        }
        assert!(std::fs::symlink_metadata(&tmp).unwrap().is_symlink());
        assert_eq!(
            std::fs::metadata(&outside).unwrap().permissions().mode() & 0o777,
            0o750
        );
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);
        inputs.default_instance = "sql:ungranted";
        assert!(prepare(&inputs).is_err());
        assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 2);
        assert!(first.directory.is_dir());
        assert!(second.directory.is_dir());
        assert!(std::fs::symlink_metadata(&tmp).unwrap().is_symlink());
        assert_eq!(std::fs::read_dir(&outside).unwrap().count(), 0);
    }

    #[test]
    fn metadata_preparation_has_no_source_identity_or_job_metadata_grants() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let directory = root.join("execution-contexts/metadata");
        let job = root.join("jobs/private-canonical-name");
        let base = Value::from_json_str(r#"{"meerschaum":{"instance":"sql:private_label","connectors":{"sql":{"private_label":{"password":"private-password","database":"/private-db"}}}},"plugins":{"private":{"token":"private-token"}}}"#).unwrap().to_map();
        let instances = BTreeSet::from(["sql:private_label".into()]);
        let prepared = prepare(&Inputs {
            metadata: true,
            server_root: &root,
            directory: &directory,
            base: &base,
            logical_instances: &instances,
            default_instance: "sql:private_label",
            callback_url: "https://private-main.example",
            read_exec: &[],
            job_dir: Some(&job),
            environment: &[
                ("AUTHORIZATION".into(), "private-bearer".into()),
                (
                    "HTTP_PROXY".into(),
                    "http://proxy-user:proxy-secret@main.invalid".into(),
                ),
                (
                    "HTTPS_PROXY".into(),
                    "https://proxy-user:proxy-secret@main.invalid".into(),
                ),
                (
                    "ALL_PROXY".into(),
                    "socks5://proxy-user:proxy-secret@main.invalid".into(),
                ),
                ("AWS_SECRET_ACCESS_KEY".into(), "cloud-secret".into()),
                ("SSL_CERT_FILE".into(), "/private/certificate-secret".into()),
            ],
        })
        .unwrap();
        let config = std::fs::read_to_string(prepared.directory.join("config.json")).unwrap();
        for value in [
            "private_label",
            "private-password",
            "/private-db",
            "private-token",
            "private-main",
            "private-bearer",
            "proxy-secret",
            "cloud-secret",
            "certificate-secret",
        ] {
            assert!(!config.contains(value), "{value}");
            assert!(
                !format!("{:?}", prepared.environment).contains(value),
                "{value}"
            );
        }
        assert!(config.contains(meerschaum_server::executor::environment::METADATA_ORIGIN));
        assert!(
            !prepared
                .spec
                .rules
                .iter()
                .any(|rule| job.starts_with(&rule.path))
        );
        assert!(prepared.environment.iter().any(|(name, value)| name
            == meerschaum_server::executor::environment::METADATA_VAR
            && value == "1"));
        assert_eq!(prepared.spec.mode, Mode::RequireSockets);
    }

    #[test]
    fn preparation_never_grants_team_databases_siblings_or_ambient_credentials() {
        let root = tempfile::tempdir().unwrap();
        let root = root.path().canonicalize().unwrap();
        let directory = root.join("execution-contexts/one");
        let base = Value::from_json_str(r#"{"meerschaum":{"connectors":{"sql":{"main":{"password":"synthetic-db-secret","database":"/private.db"}}}}}"#).unwrap().to_map();
        let instances = BTreeSet::from(["sql:main".into()]);
        let environment = vec![
            ("MRSM_SQL_MAIN".into(), "secret".into()),
            ("AWS_SECRET_ACCESS_KEY".into(), "secret".into()),
            ("MRSM_CONFIG".into(), "secret".into()),
            ("TMUX".into(), "default".into()),
            ("PYTHONHOME".into(), "secret".into()),
            ("PATH".into(), "/usr/bin".into()),
        ];
        let prepared = prepare(&Inputs {
            metadata: false,
            server_root: &root,
            directory: &directory,
            base: &base,
            logical_instances: &instances,
            default_instance: "sql:main",
            callback_url: "https://main.example",
            read_exec: &[],
            job_dir: None,
            environment: &environment,
        })
        .unwrap();
        assert_eq!(prepared.spec.mode, Mode::RequireSockets);
        assert!(!prepared.environment.iter().any(|(k, _)| matches!(
            k.as_str(),
            "MRSM_SQL_MAIN" | "AWS_SECRET_ACCESS_KEY" | "MRSM_CONFIG" | "TMUX" | "PYTHONHOME"
        )));
        for denied in [
            root.join("config/meerschaum.yaml"),
            root.join("private.db"),
            root.join("execution-contexts/two/credential.json"),
            root.join("team_scopes/team/config.json"),
        ] {
            assert!(
                !prepared
                    .spec
                    .rules
                    .iter()
                    .any(|rule| denied.starts_with(&rule.path)),
                "{}",
                denied.display()
            );
        }
        assert!(
            !std::fs::read_to_string(prepared.directory.join("config.json"))
                .unwrap()
                .contains("synthetic-db-secret")
        );
        assert!(
            !prepared.credential_file.exists(),
            "preparation must never mint a credential"
        );
    }
}
