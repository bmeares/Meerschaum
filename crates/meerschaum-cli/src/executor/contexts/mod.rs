//! Executor-local paths and configuration for signed, main-owned launches.
//!
//! The children prepare one launch each: `api_job` the API-profile job launch, `launch` the
//! `LaunchEnvironment` and terminal `SessionLookup` implementations, `metadata` the main-host
//! cache eligibility, and `team` the per-launch source namespaces of credentialed Team execution.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, OnceLock};

use meerschaum_core::team_scope::{create_private_dir, write_private};
use meerschaum_server::executor::environment::{Profile, Target, Update};
use meerschaum_server::executor::terminal::TerminalSessions;
use meerschaum_server::webterm::{SessionLookup, ShellCommand};
use serde::{Deserialize, Serialize};
use sha2::Digest;
use uuid::Uuid;

mod api_job;
mod launch;
mod metadata;
mod team;
use api_job::{ApiJobLaunch, LaunchDirectories, prepare_api_job};
pub(crate) use launch::Sessions;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    source_compiler_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    metadata_deadline: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    extension_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    registration_sha256: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    discovery_catalog_sha256: Option<String>,
    owner: String,
    owner_id: Option<String>,
    profile: Profile,
    target: Target,
    callback_url: String,
}
struct Entry {
    compiler: Option<meerschaum_core::team_scope::sandbox::runtime::NativeCompilerBinding>,
    identity: Identity,
    expires_at: i64,
    revision: u64,
    environment: Option<Vec<(String, String)>>,
    generation: Option<meerschaum_jobs::supervisor::JobGeneration>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ContextProof {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compiler: Option<meerschaum_core::team_scope::sandbox::runtime::NativeCompilerBinding>,
    id: Uuid,
    revision: u64,
    identity: Identity,
    generation: Option<meerschaum_jobs::supervisor::JobGeneration>,
    #[serde(default)]
    attempted: bool,
    #[serde(default)]
    cleared_for_rebuild: bool,
    #[serde(default)]
    terminal_tracking: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    terminal_spec: Option<PathBuf>,
}

impl ContextProof {
    fn terminal_spec(&self, root: &std::path::Path) -> Result<&std::path::Path, String> {
        if self.identity.profile != Profile::Api
            || self.identity.target != Target::Terminal
            || self.terminal_tracking != 1
        {
            return Err("The private terminal has no trusted cleanup tracking protocol; operator runtime retirement is required.".into());
        }
        let spec = self
            .terminal_spec
            .as_deref()
            .ok_or("The private terminal lost its sealed launch specification.")?;
        let directory = std::fs::canonicalize(root.join(self.id.simple().to_string()))
            .map_err(|_| "The private terminal identity directory is unavailable.")?;
        let generation = spec
            .parent()
            .ok_or("Invalid private terminal specification.")?;
        let name = generation
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(|name| name.strip_prefix("generation-"))
            .and_then(|name| Uuid::parse_str(name).ok())
            .filter(|id| !id.is_nil());
        if generation.parent() != Some(directory.as_path())
            || name.is_none()
            || spec.file_name()
                != Some(std::ffi::OsStr::new(
                    meerschaum_core::team_scope::sandbox::SPEC_FILE,
                ))
            || std::fs::canonicalize(spec).ok().as_deref() != Some(spec)
        {
            return Err("The private terminal specification escaped its sealed context.".into());
        }
        Ok(spec)
    }

    fn read(path: &std::path::Path) -> Result<Self, String> {
        use std::io::Read;
        let metadata = std::fs::symlink_metadata(path).map_err(|_| "The previous runtime identity proof is unavailable. Ask an administrator to recover this job.")?;
        if !metadata.is_file() || metadata.len() > 16 * 1024 {
            return Err("Invalid previous runtime identity proof.".into());
        }
        let mut bytes = Vec::new();
        std::fs::File::open(path)
            .map_err(|_| "The runtime proof is unreadable.")?
            .take(16 * 1024 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| "The runtime proof is unreadable.")?;
        if bytes.len() > 16 * 1024 {
            return Err("The runtime proof exceeds its bound.".into());
        }
        serde_json::from_slice(&bytes)
            .map_err(|_| "Invalid previous runtime identity proof.".into())
    }

    fn validate_job(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        id: Uuid,
        revision: u64,
    ) -> Result<String, String> {
        let name = format!("mrsm-{}", id.simple());
        if id.is_nil()
            || self.id != id
            || self.revision != revision
            || self.identity.target != (Target::Job { name: name.clone() })
            || (!actor.administrator && self.identity.owner != actor.owner)
        {
            return Err(
                "This retirement does not match the previous canonical job identity and revision."
                    .into(),
            );
        }
        if let Some(generation) = &self.generation {
            generation
                .validate(&name)
                .map_err(|_| "Invalid previous job generation binding.")?;
            if generation.context != id.simple().to_string()
                || generation.revision != revision
                || generation.owner != self.identity.owner
                || generation.owner_id != self.identity.owner_id
                || generation.identity
                    != identity_digest(id, revision, &self.identity, self.compiler.as_ref())?
            {
                return Err("The previous job generation has another source identity.".into());
            }
        } else if self.identity.profile.private_runtime() {
            return Err("The previous API generation has no sealed lifecycle binding.".into());
        }
        Ok(name)
    }
}

fn identity_digest(
    id: Uuid,
    revision: u64,
    identity: &Identity,
    compiler: Option<&meerschaum_core::team_scope::sandbox::runtime::NativeCompilerBinding>,
) -> Result<String, String> {
    if compiler.map(|binding| binding.manifest_sha256.as_str())
        != identity.source_compiler_sha256.as_deref()
    {
        return Err("The canonical runtime lost its exact compiler identity.".into());
    }
    // Preserve old no-compiler hashes exactly; attached material also binds its descriptor.
    let bytes = if let Some(compiler) = compiler {
        serde_json::to_vec(&(id, revision, identity, compiler))
    } else {
        serde_json::to_vec(&(id, revision, identity))
    }
    .map_err(|_| "Could not identify the canonical runtime.")?;
    Ok(sha2::Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
}

fn stop_generation(
    proof: &ContextProof,
    name: &str,
    jobs: &meerschaum_jobs::JobsContext,
) -> Result<(), String> {
    let mut job = meerschaum_jobs::Job::open(name, jobs)
        .map_err(|_| "Could not open this launch's job definition.")?;
    if job.status() != meerschaum_jobs::Status::Stopped
        && (!job.stop(None).success || job.status() != meerschaum_jobs::Status::Stopped)
    {
        return Err("Could not stop this launch before retiring its context.".into());
    }
    if let Some(generation) = &proof.generation {
        if !proof.attempted {
            return Ok(());
        }
        let directory = jobs
            .paths
            .root
            .join("execution-contexts")
            .join(proof.id.simple().to_string());
        if generation.directory.parent() != Some(directory.as_path()) {
            return Err("The previous job generation escaped its canonical context.".into());
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            match generation.inactive() {
                Ok(true) => break,
                Ok(false) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                Ok(false) => return Err("The previous native supervisor is still finishing. Retry this start.".into()),
                Err(_) => return Err("Cannot prove the previous API process tree completed cleanup. An administrator must retire the owned execution runtime and confirm its old processes are gone before recovering this job; removing runtime metadata does not establish that proof.".into()),
            }
        }
    }
    Ok(())
}
fn stop_terminal_generation(proof: &ContextProof, root: &std::path::Path) -> Result<(), String> {
    let spec = proof.terminal_spec(root)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        match meerschaum_jobs::supervisor::stop_terminal_launches(spec) {
            Ok(true)=>return Ok(()),
            Ok(false) if std::time::Instant::now()<deadline=>std::thread::sleep(std::time::Duration::from_millis(20)),
            Ok(false)=>return Err("The private terminal process tree has not completed cleanup. Retry after its guardian stops; operator runtime retirement is required for unknown pending launches.".into()),
            Err(_)=>return Err("The private terminal cleanup proof is unavailable. An administrator must retire the owned terminal runtime before recovery; removing metadata is insufficient.".into()),
        }
    }
}

struct Retired {
    identity: Identity,
    revision: u64,
    until: i64,
}
#[derive(Default)]
struct Inner {
    base: Option<meerschaum_core::Map>,
    runtime_environment: Option<Vec<(String, String)>>,
    entries: BTreeMap<Uuid, Entry>,
    retired: BTreeMap<Uuid, Retired>,
}

/// Credentials and overlays are never selected by a path from an HTTP request.
pub(crate) struct Contexts {
    root: PathBuf,
    /// Durable command state. A remote Factory keeps its control files elsewhere.
    work_root: PathBuf,
    inner: Mutex<Inner>,
    source_compiler: OnceLock<Option<Arc<crate::executor::compiler::Compiler>>>,
    /// Fixed SDK roots from the constructor's measured native Factory installation.
    factory_sdk: OnceLock<Option<meerschaum_server::executor::factory_artifacts::Sdk>>,
}

/// Selected by the Main host after original-source admission. This type is never decoded
/// from a request or persisted job environment, and carries no ambient configuration lookup.
pub(crate) struct HostInputs {
    pub base: meerschaum_core::Map,
    pub environment: Vec<(String, String)>,
    pub sources: Vec<(std::ffi::OsString, PathBuf)>,
}

impl Contexts {
    fn stopped_path(&self, id: Uuid) -> PathBuf {
        self.root
            .parent()
            .unwrap_or(&self.root)
            .join("executor-retired-jobs")
            .join(format!("{}.json", id.simple()))
    }
    fn factory(&self) -> bool {
        self.root.parent() != Some(self.work_root.as_path())
    }

    fn recorded(&self, id: Uuid) -> bool {
        let present = |path: &std::path::Path| match std::fs::symlink_metadata(path) {
            Ok(_) => true,
            Err(error) => error.kind() != std::io::ErrorKind::NotFound,
        };
        present(&self.root.join(id.simple().to_string())) || present(&self.stopped_path(id))
    }

    pub(crate) fn new(root: PathBuf) -> Self {
        Self::with_roots(root.clone(), root)
    }

    /// Constructor-owned separation; neither updates nor job environments select this path.
    pub(crate) fn for_factory(work_root: PathBuf, control_root: PathBuf) -> Result<Self, String> {
        if !control_root.is_absolute()
            || control_root
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err("The Factory control root must be an absolute administrator path.".into());
        }
        std::fs::create_dir_all(&work_root)
            .map_err(|_| "Could not prepare the durable Team root.")?;
        let work_root = std::fs::canonicalize(work_root)
            .map_err(|_| "The durable Team root is unavailable.")?;
        if work_root.starts_with(&control_root) || control_root.starts_with(&work_root) {
            return Err("The Factory control root must be outside the durable Team root.".into());
        }
        // Validate the ancestry before creating or chmodding the control directory.
        crate::executor_profiles::private_directory(&control_root)?;
        if std::fs::canonicalize(&control_root).ok().as_ref() != Some(&control_root) {
            return Err("The Factory control root cannot use a symbolic alias.".into());
        }
        #[cfg(unix)]
        if control_root
            == std::path::Path::new(
                meerschaum_core::team_scope::source_namespace::FACTORY_CONTROL_ROOT,
            )
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(&control_root)
                .map_err(|_| "The generated Factory control volume is unavailable.")?;
            if metadata.uid() != 1000 || metadata.gid() != 1000 || metadata.mode() & 0o777 != 0o700
            {
                return Err(
                    "The generated Factory control volume requires owner 1000:1000 and mode 0700."
                        .into(),
                );
            }
            for ancestor in control_root.ancestors().skip(1) {
                let metadata = std::fs::symlink_metadata(ancestor)
                    .map_err(|_| "Invalid Factory control volume ancestry.")?;
                if !metadata.is_dir() || metadata.uid() != 0 || metadata.mode() & 0o022 != 0 {
                    return Err("The generated Factory control volume requires root-owned parent directories.".into());
                }
            }
        }
        Ok(Self::with_roots(work_root, control_root))
    }

    fn with_roots(work_root: PathBuf, control_root: PathBuf) -> Self {
        Self {
            root: control_root.join("execution-contexts"),
            work_root,
            inner: Mutex::new(Inner::default()),
            source_compiler: OnceLock::new(),
            factory_sdk: OnceLock::new(),
        }
    }

    /// Native host construction only; no update, property, environment or config selector.
    pub(crate) fn set_source_compiler(
        &self,
        compiler: Option<Arc<crate::executor::compiler::Compiler>>,
    ) -> Result<(), String> {
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !inner.entries.is_empty() {
            return Err("The compiler must be selected before private runtime preparation.".into());
        }
        self.source_compiler
            .set(compiler)
            .map_err(|_| "The private source compiler selection is already fixed.".into())
    }
    /// Constructor-only SDK mapping. Neither a request nor a job environment chooses this.
    pub(crate) fn set_factory_sdk(
        &self,
        sdk: Option<meerschaum_server::executor::factory_artifacts::Sdk>,
    ) -> Result<(), String> {
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        if !inner.entries.is_empty() {
            return Err(
                "The native Factory SDK must be selected before runtime preparation.".into(),
            );
        }
        self.factory_sdk
            .set(sdk)
            .map_err(|_| "The native Factory SDK is already fixed.".into())
    }

    pub(crate) fn source_compiler_identity(&self) -> Option<String> {
        if let Some(compiler) = self.source_compiler.get() {
            return compiler
                .as_ref()
                .map(|compiler| compiler.identity().to_string());
        }
        let _inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        self.source_compiler
            .get_or_init(|| None)
            .as_ref()
            .map(|compiler| compiler.identity().to_string())
    }

    pub(crate) fn configure(&self, base: meerschaum_core::Map) -> Result<(), String> {
        create_private_dir(&self.root).map_err(|_| "Could not prepare execution contexts.")?;
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.base = Some(base);
        inner.runtime_environment = None;
        Ok(())
    }

    pub(crate) fn configure_runtime(
        &self,
        base: meerschaum_core::Map,
        environment: Vec<(String, String)>,
    ) -> Result<(), String> {
        self.configure(base)?;
        self.inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .runtime_environment = Some(environment);
        Ok(())
    }

    pub(crate) fn update(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        update: Update,
    ) -> Result<(), String> {
        if matches!(update.profile, Profile::Api | Profile::Team) {
            if update.discovery_catalog_sha256.is_some()
                || update.registration.is_some()
                || update.extension.is_some()
            {
                return Err(
                    "This API launch requires its exact host-selected source catalog.".into(),
                );
            }
            let (mut base, mut environment) = {
                let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
                (
                    inner
                        .base
                        .clone()
                        .ok_or("The executor is not configured.")?,
                    inner
                        .runtime_environment
                        .clone()
                        .ok_or("Execution needs a trusted native runtime.")?,
                )
            };
            base.shift_remove(meerschaum_server::executor::plugins::CONFIG_KEY);
            environment.retain(|(name, _)| {
                !matches!(name.as_str(), "MRSM_EXECUTOR_PLUGINS" | "MRSM_PLUGINS_DIR")
            });
            environment.push(("MRSM_EXECUTOR_PLUGINS".into(), "[]".into()));
            return self.update_with(
                actor,
                update,
                Some(&HostInputs {
                    base,
                    environment,
                    sources: Vec::new(),
                }),
            );
        }
        self.update_with(actor, update, None)
    }

    /// A missing catalog may request a pull only after existing launch ownership and the
    /// immutable selector have been checked. The later update repeats these checks under lock.
    pub(crate) fn check_update_identity(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        update: &Update,
    ) -> Result<(), String> {
        update.validate(actor)?;
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let identity = Identity {
            source_compiler_sha256: if update.profile.private_runtime() {
                self.source_compiler
                    .get_or_init(|| None)
                    .as_ref()
                    .map(|compiler| compiler.identity().to_string())
            } else {
                None
            },
            metadata_deadline: update.metadata_deadline,
            extension_sha256: update
                .extension
                .as_ref()
                .map(|value| value.digest())
                .transpose()
                .map_err(|error| error.detail_text())?,
            registration_sha256: update
                .registration
                .as_ref()
                .map(|value| value.digest())
                .transpose()
                .map_err(|error| error.detail_text())?,
            discovery_catalog_sha256: update.discovery_catalog_sha256.clone(),
            owner: update.owner.clone(),
            owner_id: update.owner_id.clone(),
            profile: update.profile,
            target: update.target.clone(),
            callback_url: update.callback_url.clone(),
        };
        if inner.retired.get(&update.id).is_some_and(|retired| {
            retired.identity != identity || update.revision <= retired.revision
        }) {
            return Err("This execution context was retired or replaced.".into());
        }
        if let Some(entry) = inner.entries.get(&update.id) {
            if entry.identity != identity
                || entry.revision > update.revision
                || entry.expires_at > update.credential.expires_at
            {
                return Err("This context no longer matches its immutable launch identity.".into());
            }
        } else {
            for path in [
                self.stopped_path(update.id),
                self.root
                    .join(update.id.simple().to_string())
                    .join("identity.json"),
            ] {
                match std::fs::symlink_metadata(&path) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(_) => return Err("The previous runtime identity is unavailable.".into()),
                    Ok(_) => {}
                }
                let proof = ContextProof::read(&path)?;
                if proof.identity != identity
                    || proof.revision > update.revision
                    || (path == self.stopped_path(update.id)
                        && proof.revision == update.revision
                        && !proof.cleared_for_rebuild)
                {
                    return Err("The persisted launch was retired or replaced.".into());
                }
            }
        }
        Ok(())
    }

    /// Main checks the completed canonical Metadata generation before accepting shared data.
    /// Call on the blocking executor: the native lifecycle may be finishing its bounded drain.
    pub(crate) fn attest_metadata(
        &self,
        id: Uuid,
        revision: u64,
        name: &str,
        catalog_sha256: &str,
        deadline: i64,
    ) -> Result<(), String> {
        metadata::attest(self, id, revision, name, catalog_sha256, deadline)
    }

    /// Signed Gateway only; the native completion and signing final check share one Context lock.
    pub(crate) fn metadata_quote(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        request: &meerschaum_server::executor::factory_attestation::quote::Request,
        installed: &crate::executor::factory::local::Installed,
        configuration_sha256: &str,
    ) -> Result<meerschaum_server::executor::factory_attestation::quote::Response, String> {
        metadata::quote(
            self,
            actor,
            request,
            &installed.manifest,
            &installed.authority,
            configuration_sha256,
        )
    }

    pub(crate) fn update_host(&self, update: Update, inputs: HostInputs) -> Result<(), String> {
        if !update.profile.private_runtime() {
            return Err("Host execution contexts require a private execution profile.".into());
        }
        create_private_dir(&self.root).map_err(|_| "Could not prepare execution contexts.")?;
        let actor = meerschaum_server::executor::control::Actor {
            owner: update.owner.clone(),
            username: None,
            permissions: Default::default(),
            administrator: true,
        };
        self.update_with(&actor, update, Some(&inputs))
    }

    /// The signed Main-only Metadata operation supplies the exact immutable source subset.
    /// This never reconfigures the shared Team runtime or adopts caller connector attributes.
    pub(crate) fn update_discovery(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        update: Update,
        catalog: meerschaum_server::executor::plugins::Validated,
        sources: Vec<(std::ffi::OsString, PathBuf)>,
    ) -> Result<(), String> {
        if update.profile != Profile::Metadata
            || update.discovery_catalog_sha256.as_deref()
                != Some(
                    catalog
                        .digest()
                        .map_err(|error| error.to_string())?
                        .as_str(),
                )
        {
            return Err("The discovery context does not match its exact source catalog.".into());
        }
        let mut environment = self
            .inner
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .runtime_environment
            .clone()
            .ok_or("Discovery needs a trusted native runtime.")?;
        environment.retain(|(name, _)| name != "MRSM_EXECUTOR_PLUGINS");
        environment.push((
            "MRSM_EXECUTOR_PLUGINS".into(),
            serde_json::to_string(&catalog.catalog().plugins.keys().collect::<Vec<_>>())
                .map_err(|_| "Could not bind the discovery source ceiling.")?,
        ));
        let base = meerschaum_core::Map::from_iter([(
            meerschaum_server::executor::plugins::CONFIG_KEY.into(),
            meerschaum_core::Value::from_json_str(
                &String::from_utf8(catalog.encode().map_err(|error| error.to_string())?)
                    .map_err(|_| "Invalid discovery catalog encoding.")?,
            )
            .map_err(|_| "Invalid discovery catalog encoding.")?,
        )]);
        self.update_with(
            actor,
            update,
            Some(&HostInputs {
                base,
                environment,
                sources,
            }),
        )
    }

    /// A signed API hook uses an exact source subset without reconfiguring its Team runtime.
    pub(crate) fn update_registration(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        update: Update,
        catalog: meerschaum_server::executor::plugins::Validated,
        sources: Vec<(std::ffi::OsString, PathBuf)>,
    ) -> Result<(), String> {
        if update.profile != Profile::Api || !matches!(update.target, Target::Job { .. }) {
            return Err("A connector registration requires a private API job context.".into());
        }
        let Some(meerschaum_server::executor::registration::Registration {
            sources:
                meerschaum_server::executor::registration::Sources::Archives { catalog: expected },
            ..
        }) = &update.registration
        else {
            return Err(
                "A remote registration requires its exact archive source selection.".into(),
            );
        };
        if expected != catalog.catalog() {
            return Err("The registration archive selection changed.".into());
        }
        self.update_api_sources(actor, update, catalog, sources)
    }

    /// Ordinary API execution receives exactly its signed source selection, independently
    /// of the Team's control configuration or another caller's live source view.
    pub(crate) fn update_api_sources(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        update: Update,
        catalog: meerschaum_server::executor::plugins::Validated,
        sources: Vec<(std::ffi::OsString, PathBuf)>,
    ) -> Result<(), String> {
        let digest = catalog.digest().map_err(|error| error.to_string())?;
        if let Some(extension) = &update.extension
            && extension.sources()
                != (meerschaum_server::executor::registration::Sources::Archives {
                    catalog: catalog.catalog().clone(),
                })
        {
            return Err("The extension source selection changed.".into());
        }
        let selected = update
            .discovery_catalog_sha256
            .as_deref()
            .map(str::to_string)
            .or_else(|| {
                update.registration.as_ref().and_then(|registration| {
                    let meerschaum_server::executor::registration::Sources::Archives { catalog } =
                        &registration.sources
                    else {
                        return None;
                    };
                    serde_json::to_vec(catalog)
                        .ok()
                        .map(|bytes| meerschaum_server::executor::plugins::sha256(&bytes))
                })
            });
        if update.profile != Profile::Api || selected.as_deref() != Some(digest.as_str()) {
            return Err("The API context does not match its immutable source catalog.".into());
        }
        let (mut base, mut environment) = {
            let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            (
                inner
                    .base
                    .clone()
                    .ok_or("The executor is not configured.")?,
                inner
                    .runtime_environment
                    .clone()
                    .ok_or("The API operation requires a trusted native runtime.")?,
            )
        };
        environment.retain(|(key, _)| key != "MRSM_EXECUTOR_PLUGINS");
        environment.push((
            "MRSM_EXECUTOR_PLUGINS".into(),
            serde_json::to_string(&catalog.catalog().plugins.keys().collect::<Vec<_>>())
                .map_err(|_| "Invalid registration source ceiling.")?,
        ));
        base.insert(
            meerschaum_server::executor::plugins::CONFIG_KEY.into(),
            meerschaum_core::Value::from_json_str(
                &String::from_utf8(catalog.encode().map_err(|error| error.to_string())?)
                    .map_err(|_| "Invalid registration catalog.")?,
            )
            .map_err(|_| "Invalid registration catalog.")?,
        );
        self.update_with(
            actor,
            update,
            Some(&HostInputs {
                base,
                environment,
                sources,
            }),
        )
    }

    fn update_with(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        update: Update,
        host: Option<&HostInputs>,
    ) -> Result<(), String> {
        update.validate(actor)?;
        let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let compiler = if update.profile.private_runtime() {
            self.source_compiler.get_or_init(|| None).clone()
        } else {
            None
        };
        let identity = Identity {
            source_compiler_sha256: compiler
                .as_ref()
                .map(|compiler| compiler.identity().to_string()),
            metadata_deadline: update.metadata_deadline,
            extension_sha256: update
                .extension
                .as_ref()
                .map(|value| value.digest())
                .transpose()
                .map_err(|error| error.detail_text())?,
            registration_sha256: update
                .registration
                .as_ref()
                .map(|registration| registration.digest())
                .transpose()
                .map_err(|error| error.detail_text())?,
            discovery_catalog_sha256: update.discovery_catalog_sha256,
            owner: update.owner,
            owner_id: update.owner_id,
            profile: update.profile,
            target: update.target,
            callback_url: update.callback_url,
        };
        let now = chrono::Utc::now().timestamp();
        if update.credential.expires_at <= now {
            return Err("The callback credential expired while waiting for its context.".into());
        }
        inner.retired.retain(|_, entry| entry.until > now);
        if let Some(retired) = inner.retired.get(&update.id)
            && (retired.identity != identity || update.revision <= retired.revision)
        {
            return Err("This launch revision has been retired.".into());
        }
        if inner.retired.len() >= 4096 {
            return Err("Execution context retirement capacity is exhausted.".into());
        }
        if let Some(current) = inner.entries.get(&update.id) {
            if current.identity != identity {
                return Err("This execution context belongs to another launch.".into());
            }
            if current.expires_at > update.credential.expires_at {
                return Err("This credential rotation is older than the current context.".into());
            }
            if current.revision > update.revision {
                return Err("This launch revision is older than the current context.".into());
            }
        } else if inner.entries.len() >= 1024 {
            return Err("Execution context capacity is exhausted.".into());
        }
        let base = host
            .map(|inputs| &inputs.base)
            .or(inner.base.as_ref())
            .ok_or("The executor is not configured.")?;
        let directory = self.root.join(update.id.simple().to_string());
        create_private_dir(&directory).map_err(|_| "Could not prepare the launch context.")?;
        if !inner.entries.contains_key(&update.id) {
            let stopped = self.stopped_path(update.id);
            if stopped.exists() {
                let previous = ContextProof::read(&stopped)?;
                if previous.identity != identity
                    || previous.revision > update.revision
                    || (previous.revision == update.revision && !previous.cleared_for_rebuild)
                {
                    return Err("This job generation was already retired or belongs to another original source.".into());
                }
            }
            if directory.join("identity.json").exists() {
                let previous = ContextProof::read(&directory.join("identity.json"))?;
                if previous.identity != identity || previous.revision > update.revision {
                    return Err(
                        "The persisted runtime has a newer revision or another original source."
                            .into(),
                    );
                }
            }
        }
        let mut environment = inner
            .entries
            .get(&update.id)
            .and_then(|entry| entry.environment.clone());
        let fresh = inner
            .entries
            .get(&update.id)
            .is_none_or(|entry| entry.revision < update.revision);
        if fresh
            && identity.profile == Profile::Api
            && identity.target == Target::Terminal
            && directory.join("identity.json").exists()
        {
            let previous = ContextProof::read(&directory.join("identity.json"))?;
            let spec = previous.terminal_spec(&self.root)?;
            if !meerschaum_jobs::supervisor::terminal_launches_inactive(spec)
                .map_err(|_| "The previous terminal cleanup proof is unavailable.")?
            {
                return Err(
                    "Close the previous private terminal before preparing a new generation.".into(),
                );
            }
        }
        if fresh {
            if identity.profile.private_runtime() {
                let trusted = host
                    .map(|inputs| &inputs.environment)
                    .or(inner.runtime_environment.as_ref())
                    .ok_or("API-only execution needs a trusted native runtime.")?;
                let name = match &identity.target {
                    Target::Job { name } => Some(name.as_str()),
                    Target::Terminal => None,
                };
                environment = Some(prepare_api_job(ApiJobLaunch {
                    directories: LaunchDirectories {
                        control: self.root.parent().ok_or("Invalid executor context root.")?,
                        work: &self.work_root,
                        launch: &directory,
                    },
                    base,
                    trusted,
                    name,
                    callback_url: &identity.callback_url,
                    metadata: identity.profile == Profile::Metadata,
                    host_sources: host.map(|inputs| inputs.sources.as_slice()),
                    registration: update.registration.as_ref(),
                    extension: update.extension.as_ref(),
                    compiler: compiler.as_deref(),
                    factory_sdk: self.factory_sdk.get().and_then(Option::as_ref),
                    compiler_deadline: identity
                        .metadata_deadline
                        .unwrap_or_else(|| update.credential.expires_at.min(now + 60)),
                })?);
            } else {
                environment = Some(team::prepare(
                    LaunchDirectories {
                        work: &self.work_root,
                        control: self.root.parent().ok_or("Invalid Team control root.")?,
                        launch: &directory,
                    },
                    base,
                    Some(&identity.callback_url),
                    host.map(|inputs| inputs.environment.as_slice())
                        .or(inner.runtime_environment.as_deref())
                        .ok_or("Team execution needs a trusted native runtime.")?,
                    host.map(|inputs| inputs.sources.as_slice()).unwrap_or(&[]),
                    identity.discovery_catalog_sha256.as_deref(),
                    match &identity.target {
                        Target::Job { name } => Some(name.as_str()),
                        Target::Terminal => None,
                    },
                )?);
            }
        }
        // A rotation updates only the sealed generation selected by preparation.
        let config = environment
            .as_ref()
            .and_then(|env| env.iter().rev().find(|(key, _)| key == "MRSM_CONFIG_FILE"))
            .map(|(_, value)| std::path::Path::new(value))
            .ok_or("The launch has no prepared configuration.")?;
        let launch_directory = std::fs::canonicalize(&directory)
            .map_err(|_| "The launch directory is unavailable.")?;
        let launch_generation = config
            .parent()
            .filter(|path| path.parent() == Some(launch_directory.as_path()))
            .ok_or("The configuration is outside its sealed launch generation.")?;
        let credential_path = launch_generation.join("credential.json");
        let compiler_binding = if identity.profile.private_runtime() {
            let spec = environment
                .as_ref()
                .and_then(|env| env.iter().find(|(key, _)| key == "MRSM_TEAM_SANDBOX"))
                .map(|(_, path)| PathBuf::from(path))
                .ok_or("The private runtime lost its native specification.")?;
            let binding = crate::executor_profiles::read_compiler_binding(&spec)?;
            if binding
                .as_ref()
                .map(|binding| binding.manifest_sha256.as_str())
                != identity.source_compiler_sha256.as_deref()
            {
                return Err(
                    "The prepared private compiler has another native host identity.".into(),
                );
            }
            if !fresh
                && inner
                    .entries
                    .get(&update.id)
                    .is_none_or(|entry| entry.compiler != binding)
            {
                return Err("Credential renewal cannot replace private compiler material.".into());
            }
            binding
        } else {
            None
        };
        let rotate = (|| {
            if update.credential.expires_at <= chrono::Utc::now().timestamp() {
                return Err("The callback credential expired while preparing its context.".into());
            }
            write_private(
                &credential_path,
                &serde_json::to_string(&update.credential)
                    .map_err(|_| "Could not encode callback credential.")?,
            )
            .map_err(|_| "Could not rotate the callback credential.".to_string())
        })();
        if let Err(error) = rotate {
            if fresh {
                // This new generation was never published to an entry or a child.
                let _ = std::fs::remove_dir_all(launch_generation);
            }
            return Err(error);
        }
        let generation = if matches!(identity.target, Target::Job { .. }) {
            Some(meerschaum_jobs::supervisor::JobGeneration {
                context: update.id.simple().to_string(),
                revision: update.revision,
                owner: identity.owner.clone(),
                owner_id: identity.owner_id.clone(),
                identity: identity_digest(
                    update.id,
                    update.revision,
                    &identity,
                    compiler_binding.as_ref(),
                )?,
                directory: launch_generation.to_path_buf(),
            })
        } else {
            None
        };
        let terminal = identity.profile == Profile::Api && identity.target == Target::Terminal;
        let terminal_spec = if terminal {
            Some(
                environment
                    .as_ref()
                    .and_then(|values| values.iter().find(|(key, _)| key == "MRSM_TEAM_SANDBOX"))
                    .map(|(_, path)| PathBuf::from(path))
                    .ok_or("The private terminal has no sealed launch specification.")?,
            )
        } else {
            None
        };
        let proof = ContextProof {
            compiler: compiler_binding.clone(),
            id: update.id,
            revision: update.revision,
            identity: identity.clone(),
            generation: generation.clone(),
            attempted: if fresh {
                false
            } else {
                ContextProof::read(&directory.join("identity.json"))?.attempted
            },
            cleared_for_rebuild: false,
            terminal_tracking: if terminal { 1 } else { 0 },
            terminal_spec,
        };
        write_private(
            &directory.join("identity.json"),
            &serde_json::to_string(&proof)
                .map_err(|_| "Could not encode the runtime identity proof.")?,
        )
        .map_err(|_| "Could not persist the sealed runtime identity proof.")?;
        inner.entries.insert(
            update.id,
            Entry {
                compiler: compiler_binding,
                identity,
                expires_at: update.credential.expires_at,
                revision: update.revision,
                environment,
                generation,
            },
        );
        inner.retired.remove(&update.id);
        Ok(())
    }

    pub(crate) fn revoke(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        id: Uuid,
        revision: Option<u64>,
    ) -> Result<Option<Target>, String> {
        {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            if !inner.entries.contains_key(&id) {
                let directory = self.root.join(id.simple().to_string());
                match std::fs::symlink_metadata(&directory) {
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
                    Ok(metadata) if metadata.is_dir() && !metadata.is_symlink() => {}
                    _ => {
                        return Err(
                            "The previous terminal identity is unavailable; refusing to erase it."
                                .into(),
                        );
                    }
                }
                let proof = ContextProof::read(&directory.join("identity.json"))?;
                if proof.id != id
                    || proof.identity.target != Target::Terminal
                    || revision.is_some_and(|revision| revision != proof.revision)
                    || (!actor.administrator && actor.owner != proof.identity.owner)
                {
                    return Err(
                        "This retirement differs from the persisted terminal identity.".into(),
                    );
                }
                if proof.identity.profile == Profile::Api {
                    stop_terminal_generation(&proof, &self.root)?;
                }
                self.archive_proof(id, &proof)?;
                std::fs::remove_dir_all(&directory)
                    .map_err(|_| "Could not retire the previous terminal context.")?;
                inner.retired.insert(
                    id,
                    Retired {
                        identity: proof.identity,
                        revision: proof.revision,
                        until: chrono::Utc::now().timestamp() + 90,
                    },
                );
                return Ok(Some(Target::Terminal));
            }
        }
        self.revoke_with(actor, id, revision, |target| {
            if matches!(target, Target::Job { .. }) {
                return Err("A job context must be retired through its owning runtime.".into());
            }
            let proof = ContextProof::read(
                &self
                    .root
                    .join(id.simple().to_string())
                    .join("identity.json"),
            )?;
            if proof.identity.profile == Profile::Api {
                stop_terminal_generation(&proof, &self.root)?;
            }
            self.archive_proof(id, &proof)?;
            Ok(())
        })
    }

    fn archive_proof(&self, id: Uuid, proof: &ContextProof) -> Result<(), String> {
        let stopped = self.stopped_path(id);
        create_private_dir(
            stopped
                .parent()
                .ok_or("Invalid retired runtime identity path.")?,
        )
        .map_err(|_| "Could not prepare retired runtime identities.")?;
        write_private(
            &stopped,
            &serde_json::to_string(proof).map_err(|_| "Invalid retired runtime identity.")?,
        )
        .map_err(|_| "Could not persist retired runtime identity.".to_string())
    }

    /// Keep the context lock through exact-revision validation, process stop and retirement.
    /// A caller timeout cannot let a late stop kill a newer launch after it obtains this lock.
    /// This is blocking lifecycle work; async hosts must call it on their blocking executor.
    pub(crate) fn revoke_job(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        id: Uuid,
        revision: u64,
        jobs: &meerschaum_jobs::JobsContext,
    ) -> Result<Option<Target>, String> {
        {
            let mut inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
            if !inner.entries.contains_key(&id) {
                let directory = self.root.join(id.simple().to_string());
                let stopped = self.stopped_path(id);
                if !directory.exists() {
                    if let Some(retired) = inner.retired.get(&id)
                        && retired.revision == revision
                        && (actor.administrator || retired.identity.owner == actor.owner)
                    {
                        return Ok(None);
                    }
                    let proof = ContextProof::read(&stopped)?;
                    proof.validate_job(actor, id, revision)?;
                    return Ok(None);
                }
                let proof = ContextProof::read(&directory.join("identity.json"))?;
                let name = proof.validate_job(actor, id, revision)?;
                stop_generation(&proof, &name, jobs)?;
                create_private_dir(
                    stopped
                        .parent()
                        .ok_or("Invalid stopped runtime proof path.")?,
                )
                .map_err(|_| "Could not prepare stopped runtime identities.")?;
                write_private(
                    &stopped,
                    &serde_json::to_string(&proof)
                        .map_err(|_| "Invalid stopped runtime identity.")?,
                )
                .map_err(|_| "Could not record the stopped runtime identity.")?;
                std::fs::remove_dir_all(&directory)
                    .map_err(|_| "Could not retire the previous stopped runtime.")?;
                inner.retired.insert(
                    id,
                    Retired {
                        identity: proof.identity,
                        revision,
                        until: chrono::Utc::now().timestamp() + 90,
                    },
                );
                return Ok(Some(Target::Job { name }));
            }
        }
        self.revoke_with(actor, id, Some(revision), |target| {
            if let Target::Job { name } = target {
                let proof = ContextProof::read(
                    &self
                        .root
                        .join(id.simple().to_string())
                        .join("identity.json"),
                )?;
                if proof.validate_job(actor, id, revision)? != *name {
                    return Err("This runtime names another canonical job.".into());
                }
                stop_generation(&proof, name, jobs)?;
                let stopped = self.stopped_path(id);
                create_private_dir(stopped.parent().ok_or("Invalid stopped proof directory.")?)
                    .map_err(|_| "Could not prepare stopped runtime identities.")?;
                write_private(
                    &stopped,
                    &serde_json::to_string(&proof)
                        .map_err(|_| "Invalid stopped runtime identity.")?,
                )
                .map_err(|_| "Could not record the stopped runtime identity.")?;
            }
            Ok(())
        })
    }

    fn revoke_with(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        id: Uuid,
        revision: Option<u64>,
        stop: impl FnOnce(&Target) -> Result<(), String>,
    ) -> Result<Option<Target>, String> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if !inner.entries.contains_key(&id) {
            return Ok(None);
        }
        if revision.is_some_and(|revision| {
            inner
                .entries
                .get(&id)
                .is_none_or(|e| e.revision != revision)
        }) {
            return Err("This retirement names another launch revision.".into());
        }
        if let Some(entry) = inner.entries.get(&id)
            && !actor.administrator
            && actor.owner != entry.identity.owner
        {
            return Err("This execution context belongs to another caller.".into());
        }
        let target = inner
            .entries
            .get(&id)
            .expect("entry checked under the same lock")
            .identity
            .target
            .clone();
        // A failed stop leaves the current context tracked, so a later exact retirement or
        // expiry sweep can retry. Never forget a still-running or paused owned process.
        stop(&target)?;
        // Remove the bearer before forgetting the entry. Only this generated UUID path is touched.
        let directory = self.root.join(id.simple().to_string());
        match std::fs::remove_dir_all(directory) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => return Err("Could not retire the launch context.".into()),
        }
        let entry = inner
            .entries
            .remove(&id)
            .expect("entry checked under the same lock");
        inner.retired.insert(
            id,
            Retired {
                identity: entry.identity,
                revision: entry.revision,
                until: chrono::Utc::now().timestamp() + 90,
            },
        );
        Ok(Some(target))
    }

    pub(crate) fn clear(&self, jobs: &meerschaum_jobs::JobsContext) -> Result<(), String> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let actor = meerschaum_server::executor::control::Actor {
            owner: "executor".into(),
            username: None,
            permissions: Default::default(),
            administrator: true,
        };
        // A fresh API process has an empty memory map. Its persisted attempted proof must
        // survive until the same complete cleanup check succeeds; absence from memory is
        // never evidence that a previous process tree stopped.
        let mut persisted = BTreeMap::new();
        let entries = match std::fs::read_dir(&self.root) {
            Ok(entries) => Some(entries),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(_) => return Err("Could not inspect persisted execution identities.".into()),
        };
        if let Some(entries) = entries {
            for entry in entries {
                if persisted.len() >= 4096 {
                    return Err("Persisted execution identity capacity is exhausted; operator recovery is required.".into());
                }
                let entry = entry.map_err(|_| "A persisted execution identity is unavailable.")?;
                let filename = entry.file_name();
                let text = filename
                    .to_str()
                    .ok_or("Invalid persisted execution identity.")?;
                let id = Uuid::parse_str(text)
                    .ok()
                    .filter(|id| !id.is_nil() && text == id.simple().to_string())
                    .ok_or(
                        "Unknown data in the execution identity directory; refusing to remove it.",
                    )?;
                let metadata = std::fs::symlink_metadata(entry.path())
                    .map_err(|_| "A persisted execution identity is unavailable.")?;
                if !metadata.is_dir() || metadata.is_symlink() {
                    return Err("A persisted execution identity is not a sealed directory.".into());
                }
                let proof = ContextProof::read(&entry.path().join("identity.json"))?;
                if proof.id != id || proof.revision == 0 {
                    return Err("A persisted execution identity names another launch.".into());
                }
                if let Some(current) = inner.entries.get(&id)
                    && (current.identity != proof.identity || current.revision != proof.revision)
                {
                    return Err(
                        "The persisted execution identity differs from the current runtime.".into(),
                    );
                }
                persisted.insert(id, proof);
            }
        }
        if inner.entries.keys().any(|id| !persisted.contains_key(id)) {
            return Err("A tracked execution lost its sealed identity proof; refusing to clear its runtime.".into());
        }
        let mut archived = Vec::new();
        for (id, mut proof) in persisted {
            if !proof.identity.profile.private_runtime() && proof.generation.is_none() {
                continue;
            }
            let name = match &proof.identity.target {
                Target::Job { name } => name.clone(),
                Target::Terminal => {
                    stop_terminal_generation(&proof, &self.root)?;
                    proof.cleared_for_rebuild = true;
                    archived.push((id, proof));
                    continue;
                }
            };
            proof.validate_job(&actor, id, proof.revision)?;
            stop_generation(&proof, &name, jobs)?;
            proof.cleared_for_rebuild = true;
            archived.push((id, proof));
        }
        // No context directory is removed before every prior private job has proved cleanup.
        for (id, proof) in archived {
            let stopped = self.stopped_path(id);
            create_private_dir(stopped.parent().ok_or("Invalid stopped proof directory.")?)
                .map_err(|_| "Could not prepare stopped runtime identities.")?;
            write_private(
                &stopped,
                &serde_json::to_string(&proof).map_err(|_| "Invalid stopped runtime identity.")?,
            )
            .map_err(|_| "Could not persist the controlled rebuild stop proof.")?;
        }
        inner.base = None;
        inner.runtime_environment = None;
        inner.entries.clear();
        inner.retired.clear();
        match std::fs::remove_dir_all(&self.root) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(_) => Err("Could not retire execution contexts.".into()),
        }
    }

    /// Capture provenance only while the observed active job has its exact, current signed
    /// context. A marker alone, stale credential or guessed canonical name is never authority
    /// for controlled rebuild recovery.
    pub(crate) fn job_revision(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> Option<u64> {
        use meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY;
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|value| Uuid::parse_str(value).ok())?;
        if id.is_nil()
            || name != format!("mrsm-{}", id.simple())
            || properties
                .get(JOB_CONTEXT_PROPERTY)
                .and_then(meerschaum_core::Value::as_str)
                .and_then(|value| Uuid::parse_str(value).ok())
                != Some(id)
        {
            return None;
        }
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        inner
            .entries
            .get(&id)
            .filter(|entry| {
                entry.identity.target == (Target::Job { name: name.into() })
                    && entry.expires_at > chrono::Utc::now().timestamp()
                    && (entry.identity.profile == Profile::Team || entry.environment.is_some())
            })
            .map(|entry| entry.revision)
    }

    pub(crate) fn retire_terminal(&self, name: &str) {
        let Some(id) = name
            .strip_prefix("executor-terminal-")
            .and_then(|s| Uuid::parse_str(s).ok())
        else {
            return;
        };
        let administrator = meerschaum_server::executor::control::Actor {
            owner: "server".into(),
            username: None,
            permissions: Default::default(),
            administrator: true,
        };
        let _ = self.revoke(&administrator, id, None);
    }

    /// Hold the context lock until the old process is stopped: a new revision can be
    /// delivered afterward, and cannot be killed by an expiry sweep of the earlier launch.
    pub(crate) fn expire_jobs(&self, jobs: &meerschaum_jobs::JobsContext) {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let now = chrono::Utc::now().timestamp();
        inner.retired.retain(|_, retired| retired.until > now);
        let expired: Vec<_> = inner
            .entries
            .iter()
            .filter_map(|(id, entry)| match &entry.identity.target {
                Target::Job { name }
                    if (if entry.identity.profile == Profile::Metadata {
                        entry.expires_at
                    } else {
                        entry.expires_at.saturating_add(60)
                    }) <= now =>
                {
                    Some((*id, name.clone()))
                }
                _ => None,
            })
            .collect();
        for (id, name) in expired {
            let directory = self.root.join(id.simple().to_string());
            let Ok(proof) = ContextProof::read(&directory.join("identity.json")) else {
                continue;
            };
            if inner.entries.get(&id).is_none_or(|entry| {
                proof.id != id
                    || proof.revision != entry.revision
                    || proof.identity != entry.identity
                    || proof.generation != entry.generation
            }) || stop_generation(&proof, &name, jobs).is_err()
                || self.archive_proof(id, &proof).is_err()
                || std::fs::remove_dir_all(directory).is_err()
            {
                continue;
            }
            if let Some(entry) = inner.entries.remove(&id) {
                inner.retired.insert(
                    id,
                    Retired {
                        identity: entry.identity,
                        revision: entry.revision,
                        until: now + 90,
                    },
                );
            }
        }
    }

    pub(crate) fn terminal_shell(
        &self,
        id: Uuid,
        grant: &meerschaum_server::executor::terminal::TerminalGrant,
        configured: &ShellCommand,
    ) -> Result<ShellCommand, String> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = inner
            .entries
            .get(&id)
            .filter(|e| {
                e.identity.target == Target::Terminal
                    && e.identity.owner == grant.owner
                    && e.identity.owner_id.as_deref() == Some(&grant.owner_id)
                    && e.expires_at > chrono::Utc::now().timestamp()
            })
            .ok_or("The terminal has no current execution context.")?;
        if entry.identity.profile == Profile::Metadata {
            return Err("Metadata discovery cannot become a terminal.".into());
        }
        if entry.identity.profile == Profile::Api {
            let mut env = entry
                .environment
                .clone()
                .ok_or("The API terminal has no prepared runtime.")?;
            let value = |key: &str| {
                env.iter()
                    .rev()
                    .find(|(name, _)| name == key)
                    .map(|(_, value)| value.clone())
            };
            let program = value("MRSM_BIN").ok_or("The API terminal has no native launcher.")?;
            let cwd = value("MRSM_ROOT_DIR")
                .map(PathBuf::from)
                .ok_or("The API terminal has no private state.")?;
            env.retain(|(key, _)| !matches!(key.as_str(), "TMUX" | "TMUX_PANE"));
            env.push((
                meerschaum_server::webterm::RESTRICTED_ENV.into(),
                "1".into(),
            ));
            env.push((
                "MRSM_WEBTERM_STATE_DIR".into(),
                cwd.join("shell-state").to_string_lossy().into_owned(),
            ));
            return crate::executor::terminal_guard::wrap(ShellCommand {
                program,
                args: Vec::new(),
                env,
                clear_env: true,
                cwd: Some(cwd),
            });
        }
        let mut shell = configured.clone();
        shell.clear_env = true;
        shell.env = entry
            .environment
            .clone()
            .ok_or("The Team terminal has no exact prepared source environment.")?;
        let native = shell
            .env
            .iter()
            .rev()
            .find(|(name, _)| name == "MRSM_BIN")
            .map(|(_, value)| value.clone())
            .ok_or("The Team terminal has no native launcher.")?;
        shell.program = native;
        shell
            .args
            .insert(0, crate::executor::terminal_guard::TEAM_EXEC_ARG.into());
        shell.cwd = Some(self.work_root.clone());
        let descriptor = shell
            .env
            .iter()
            .rev()
            .find(|(name, _)| name == meerschaum_core::team_scope::source_namespace::ENV)
            .map(|(_, value)| PathBuf::from(value))
            .ok_or("The Team terminal has no sealed source namespace.")?;
        let launch = meerschaum_core::team_scope::source_namespace::Launch::read(&descriptor)?;
        // As for signed jobs, dynamic loader/import settings belong to the sealed
        // runtime and are restored only after the native launch installs confinement.
        shell.env.retain(|(name, _)| {
            name != "MRSM_WEBTERM_STATE_DIR"
                && !meerschaum_core::team_scope::sandbox::runtime::injection_name(name)
        });
        shell.env.push((
            "MRSM_WEBTERM_STATE_DIR".into(),
            launch
                .namespace
                .cache
                .join("shell-state")
                .display()
                .to_string(),
        ));
        Ok(shell)
    }

    fn shell(
        &self,
        id: Uuid,
        grant: &meerschaum_server::executor::terminal::TerminalGrant,
        configured: &ShellCommand,
    ) -> Result<ShellCommand, String> {
        self.terminal_shell(id, grant, configured)
    }

    fn terminal_restricted(
        &self,
        id: Uuid,
        grant: &meerschaum_server::executor::terminal::TerminalGrant,
    ) -> Result<bool, String> {
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let entry = inner
            .entries
            .get(&id)
            .filter(|entry| {
                entry.identity.target == Target::Terminal
                    && entry.identity.owner == grant.owner
                    && entry.identity.owner_id.as_deref() == Some(&grant.owner_id)
                    && entry.expires_at > chrono::Utc::now().timestamp()
            })
            .ok_or("The terminal has no current execution context.")?;
        Ok(entry.identity.profile == Profile::Api)
    }
}

#[cfg(test)]
pub(crate) fn native_model_host() -> Result<(), String> {
    static BOOTSTRAP: OnceLock<Result<(), String>> = OnceLock::new();
    let expected = std::env::current_exe()
        .and_then(std::fs::canonicalize)
        .map_err(|_| "The test model's native executable is unavailable.".to_string())?;
    BOOTSTRAP
        .get_or_init(|| {
            if meerschaum_core::self_exe::native_executable().is_none() {
                // A different test may register this same real host between these two calls.
                // An error is acceptable only when the immutable result proves that exact host.
                let _ = meerschaum_core::self_exe::register_native_current_executable();
            }
            if meerschaum_core::self_exe::native_executable().as_ref() == Some(&expected) {
                Ok(())
            } else {
                Err("The test model needs its actual current native host.".into())
            }
        })
        .clone()?;
    if meerschaum_core::self_exe::native_executable().as_ref() != Some(&expected) {
        return Err("The registered test host differs from its current executable.".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
