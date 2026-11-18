//! Executor-local paths and configuration for signed, main-owned launches.

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

#[path = "executor_context_metadata.rs"]
mod metadata;
#[path = "executor_context_team.rs"]
mod team;

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
    source_compiler: OnceLock<Option<Arc<crate::executor_compiler::Compiler>>>,
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
        compiler: Option<Arc<crate::executor_compiler::Compiler>>,
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
        installed: &crate::executor_factory_local::Installed,
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
            return crate::executor_terminal_guard::wrap(ShellCommand {
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
            .insert(0, crate::executor_terminal_guard::TEAM_EXEC_ARG.into());
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

struct LaunchDirectories<'a> {
    control: &'a std::path::Path,
    work: &'a std::path::Path,
    launch: &'a std::path::Path,
}

struct ApiJobLaunch<'a> {
    directories: LaunchDirectories<'a>,
    base: &'a meerschaum_core::Map,
    trusted: &'a [(String, String)],
    name: Option<&'a str>,
    callback_url: &'a str,
    metadata: bool,
    host_sources: Option<&'a [(std::ffi::OsString, PathBuf)]>,
    registration: Option<&'a meerschaum_server::executor::registration::Registration>,
    extension: Option<&'a meerschaum_server::executor::extensions::Operation>,
    compiler: Option<&'a crate::executor_compiler::Compiler>,
    factory_sdk: Option<&'a meerschaum_server::executor::factory_artifacts::Sdk>,
    compiler_deadline: i64,
}

fn prepare_api_job(job: ApiJobLaunch<'_>) -> Result<Vec<(String, String)>, String> {
    let ApiJobLaunch {
        directories:
            LaunchDirectories {
                control: root,
                work: work_root,
                launch: directory,
            },
        base,
        trusted,
        name,
        callback_url,
        metadata,
        host_sources,
        registration,
        extension,
        compiler,
        factory_sdk,
        compiler_deadline,
    } = job;
    let mut selected_base = base.clone();
    selected_base.shift_remove(meerschaum_server::executor::extensions::CONFIG_KEY);
    if let Some(extension) = extension {
        if metadata || registration.is_some() {
            return Err("Extension admission conflicts with this private profile.".into());
        }
        extension.validated().map_err(|error| error.detail_text())?;
        selected_base.insert(
            meerschaum_server::executor::extensions::CONFIG_KEY.into(),
            meerschaum_core::Value::from_json_str(
                &serde_json::to_string(extension).map_err(|_| "Invalid extension descriptor.")?,
            )
            .map_err(|_| "Invalid extension descriptor.")?,
        );
    }
    selected_base.shift_remove(meerschaum_server::executor::registration::CONFIG_KEY);
    if let Some(registration) = registration {
        if metadata {
            return Err("Credential-free discovery cannot receive registration parameters.".into());
        }
        registration
            .validate()
            .map_err(|error| error.detail_text())?;
        selected_base.insert(
            meerschaum_server::executor::registration::CONFIG_KEY.into(),
            meerschaum_core::Value::from_json_str(
                &serde_json::to_string(registration)
                    .map_err(|_| "Invalid registration descriptor.")?,
            )
            .map_err(|_| "Invalid registration descriptor.")?,
        );
    }
    let base = &selected_base;
    let connectors = base
        .get("meerschaum")
        .and_then(|value| value.get("connectors"))
        .map(|value| value.to_map())
        .unwrap_or_default();
    let mut logical = std::collections::BTreeSet::new();
    for kind in ["sql", "api", "valkey"] {
        for (label, attributes) in connectors
            .get(kind)
            .map(|value| value.to_map())
            .unwrap_or_default()
        {
            if label != "default" && attributes.as_map().is_some() {
                let keys = format!("{kind}:{label}");
                meerschaum_core::team_scope::api_execution::split_keys(&keys)?;
                logical.insert(keys);
            }
        }
    }
    let default = if metadata {
        "api:metadata"
    } else {
        base.get("meerschaum")
            .and_then(|value| value.get("instance"))
            .and_then(meerschaum_core::Value::as_str)
            .ok_or("The API launch has no logical default instance.")?
    };
    let lookup = |key: &str| {
        trusted
            .iter()
            .rev()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str())
    };
    let grants: std::collections::BTreeSet<String> =
        serde_json::from_str(lookup("MRSM_EXECUTOR_PLUGINS").unwrap_or("[]"))
            .map_err(|_| "Invalid trusted plugin grants.")?;
    if grants
        .iter()
        .any(|name| !meerschaum_server::executor::plugins::valid_name(name))
    {
        return Err("Invalid host-selected plugin source name.".into());
    }
    let mut sources = Vec::new();
    if let Some(selected) = host_sources {
        for (filename, source) in selected {
            let text = filename
                .to_str()
                .ok_or("Invalid granted plugin filename.")?;
            let plugin = text
                .strip_suffix(".py")
                .or_else(|| text.strip_suffix(".rs"))
                .unwrap_or(text);
            if text.contains(['/', '\\']) || text == "." || text == ".." || !grants.contains(plugin)
            {
                return Err("The host selected an ungranted plugin source.".into());
            }
            let source = std::fs::canonicalize(source)
                .map_err(|_| "The granted plugin source is unavailable.")?;
            sources.push((filename.clone(), source));
        }
    } else if let Some(view) = lookup("MRSM_PLUGINS_DIR") {
        let view = std::path::Path::new(view);
        if !view.starts_with(root.join("executor-plugin-views")) {
            return Err("API source discovery requires the host's granted plugin view.".into());
        }
        for entry in
            std::fs::read_dir(view).map_err(|_| "The granted plugin view is unavailable.")?
        {
            let entry = entry.map_err(|_| "The granted plugin source is unavailable.")?;
            let filename = entry.file_name();
            let text = filename
                .to_str()
                .ok_or("Invalid granted plugin filename.")?;
            let plugin = text
                .strip_suffix(".py")
                .or_else(|| text.strip_suffix(".rs"))
                .unwrap_or(text);
            if !grants.contains(plugin) {
                return Err("The plugin view contains an ungranted source.".into());
            }
            let source = std::fs::canonicalize(entry.path())
                .map_err(|_| "The granted plugin source is unavailable.")?;
            if source == root.join("plugins") || !source.starts_with(root.join("plugins")) {
                return Err("A granted source cannot escape the private plugin store.".into());
            }
            sources.push((filename, source));
        }
    }
    let read_exec = sources
        .iter()
        .map(|(_, source)| source.clone())
        .collect::<Vec<_>>();
    let job = name.map(|name| root.join("jobs").join(name));
    let mut prepared =
        crate::executor_profiles::prepare_with_runtime(&crate::executor_profiles::Inputs {
            metadata,
            server_root: root,
            directory,
            base,
            logical_instances: &logical,
            default_instance: default,
            callback_url,
            read_exec: &read_exec,
            job_dir: job.as_deref(),
            environment: trusted,
        })?;
    if work_root != root {
        // The remote command root includes Team credentials and mutable dependencies.
        // Its absence from this API profile is independent of the sealed control layout.
        let work_root = std::fs::canonicalize(work_root)
            .map_err(|_| "The durable Team root is unavailable.")?;
        if prepared
            .spec
            .rules
            .iter()
            .any(|rule| rule.path.starts_with(&work_root) || work_root.starts_with(&rule.path))
        {
            return Err("An API runtime cannot read the durable Team root.".into());
        }
        prepared.spec.protected.push(work_root);
        prepared.spec.protected.push(PathBuf::from("/run/secrets"));
        let spec_path = prepared
            .environment
            .iter()
            .find(|(key, _)| key == "MRSM_TEAM_SANDBOX")
            .map(|(_, value)| PathBuf::from(value))
            .ok_or("The API runtime has no native specification.")?;
        write_private(
            &spec_path,
            &serde_json::to_string(&prepared.spec)
                .map_err(|_| "Invalid API runtime specification.")?,
        )
        .map_err(|_| "Could not protect the durable Team root.")?;
    }
    let generation = prepared.directory.clone();
    let result = (|| {
        if let Some(sdk) = factory_sdk {
            crate::executor_factory_sdk::bind(&mut prepared, sdk)?;
        }
        let view = prepared
            .environment
            .iter()
            .find(|(key, _)| key == "MRSM_PLUGINS_DIR")
            .map(|(_, value)| PathBuf::from(value))
            .ok_or("The API source view was not prepared.")?;
        // Python's confined loader deliberately never writes the shared plugins package. Give
        // this launch its own inert package, with only the same exact granted source links.
        let state = prepared
            .environment
            .iter()
            .find(|(key, _)| key == "MRSM_ROOT_DIR")
            .map(|(_, value)| PathBuf::from(value))
            .ok_or("The API state was not prepared.")?;
        let venvs = prepared
            .environment
            .iter()
            .find(|(key, _)| key == "MRSM_VENVS_DIR")
            .map(|(_, value)| PathBuf::from(value))
            .ok_or("The private plugin environments were not prepared.")?;
        // The SDK activates named source environments without creating them during imports.
        // Empty private sites support dependency-free sources; no shared Team packages or
        // interpreter are probed/copied. Only this never-launched generation is touched.
        for plugin in &grants {
            crate::executor_profiles::private_directory(
                &venvs.join(plugin).join("lib/site-packages"),
            )?;
        }
        let internal = state.join(".internal");
        let package = internal.join("plugins");
        crate::executor_profiles::private_directory(&internal)?;
        crate::executor_profiles::private_directory(&package)?;
        write_private(&package.join("__init__.py"), "")
            .map_err(|_| "Could not prepare the inert plugin package.")?;
        for (filename, source) in sources {
            for destination in [&view, &package] {
                let target = destination.join(&filename);
                match std::fs::symlink_metadata(&target) {
                    Ok(metadata)
                        if metadata.file_type().is_symlink()
                            && std::fs::read_link(&target).ok().as_ref() == Some(&source) =>
                    {
                        continue;
                    }
                    Ok(_) => {
                        return Err("An API source view may not replace an existing entry.".into());
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(_) => return Err("The API source view is unavailable.".into()),
                }
                #[cfg(unix)]
                std::os::unix::fs::symlink(&source, target)
                    .map_err(|_| "Could not prepare an API source view.")?;
                #[cfg(not(unix))]
                return Err("API-only execution requires a supported native platform.".into());
            }
        }
        if let Some(compiler) = compiler {
            crate::executor_profiles::prepare_compiler(&mut prepared, compiler, compiler_deadline)?;
        }
        if work_root != root {
            let secrets = std::path::Path::new("/run/secrets");
            if prepared
                .spec
                .rules
                .iter()
                .any(|rule| rule.path.starts_with(secrets) || secrets.starts_with(&rule.path))
            {
                return Err("A Factory API runtime cannot access administrator secrets.".into());
            }
        }
        // This cap also limits installed package entry points; it is not taken from job properties.
        prepared.environment.push((
            "MRSM_EXECUTOR_PLUGINS".into(),
            serde_json::to_string(&grants).map_err(|_| "Could not encode plugin grants.")?,
        ));
        Ok(prepared.environment)
    })();
    if result.is_err() {
        // No launcher has received this fresh generation yet. Older generations may still
        // be running and must never be revisited while preparing this one.
        let _ = std::fs::remove_dir_all(generation);
    }
    result
}

impl std::fmt::Debug for Contexts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExecutionContexts").finish_non_exhaustive()
    }
}

impl meerschaum_jobs::daemon::LaunchEnvironment for Contexts {
    fn compute_deadline(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<meerschaum_jobs::supervisor::ComputeDeadline>> {
        let Some(revision) = self.job_revision(name, properties) else {
            if self.authoritative(name, properties) {
                return Err(std::io::Error::other(
                    "The job has no current compute authority.",
                ));
            }
            return Ok(None);
        };
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|id| Uuid::parse_str(id).ok())
            .ok_or_else(|| std::io::Error::other("Invalid canonical compute identity."))?;
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let entry = inner
            .entries
            .get(&id)
            .filter(|entry| entry.revision == revision)
            .ok_or_else(|| {
                std::io::Error::other("The compute generation changed before launch.")
            })?;
        match (entry.identity.profile, entry.identity.metadata_deadline) {
            (Profile::Metadata, Some(expires_at)) => {
                Ok(Some(meerschaum_jobs::supervisor::ComputeDeadline {
                    expires_at,
                }))
            }
            (Profile::Metadata, _) | (_, Some(_)) => Err(std::io::Error::other(
                "The compute deadline does not match its private profile.",
            )),
            _ => Ok(None),
        }
    }
    fn result_protocol(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<meerschaum_jobs::supervisor::ResultProtocol> {
        use meerschaum_jobs::supervisor::ResultProtocol;
        if !self.authoritative(name, properties) {
            return Ok(ResultProtocol::Ordinary);
        }
        let revision = self.job_revision(name, properties).ok_or_else(|| {
            std::io::Error::other("The result protocol has no current signed context.")
        })?;
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|id| Uuid::parse_str(id).ok())
            .ok_or_else(|| std::io::Error::other("Invalid canonical result target."))?;
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let entry = inner
            .entries
            .get(&id)
            .filter(|entry| entry.revision == revision)
            .ok_or_else(|| std::io::Error::other("The result context changed before launch."))?;
        Ok(if entry.identity.profile == Profile::Metadata {
            ResultProtocol::Metadata
        } else if entry.identity.profile == Profile::Api
            && entry.identity.extension_sha256.is_some()
        {
            ResultProtocol::Extension
        } else if entry.identity.profile == Profile::Api
            && entry.identity.registration_sha256.is_some()
        {
            ResultProtocol::Registration
        } else {
            ResultProtocol::Ordinary
        })
    }
    fn retained_generation(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<meerschaum_jobs::supervisor::JobGeneration>> {
        let revision = self.job_revision(name, properties).ok_or_else(|| {
            std::io::Error::other("The result has no current unexpired canonical source.")
        })?;
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(|| std::io::Error::other("Invalid canonical result identity."))?;
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let entry = inner
            .entries
            .get(&id)
            .filter(|entry| {
                entry.revision == revision
                    && entry.expires_at > chrono::Utc::now().timestamp()
                    && entry.identity.target == (Target::Job { name: name.into() })
            })
            .ok_or_else(|| std::io::Error::other("The retained result source is unavailable."))?;
        let path = self
            .root
            .join(id.simple().to_string())
            .join("identity.json");
        let proof = ContextProof::read(&path).map_err(std::io::Error::other)?;
        if proof.id != id
            || proof.revision != revision
            || proof.identity != entry.identity
            || !proof.attempted
            || proof.cleared_for_rebuild
            || proof.generation != entry.generation
            || proof.compiler != entry.compiler
            || entry.expires_at <= chrono::Utc::now().timestamp()
        {
            return Err(std::io::Error::other(
                "The retained native result generation changed.",
            ));
        }
        if let Some(generation) = &entry.generation {
            generation.validate(name)?;
        }
        Ok(entry.generation.clone())
    }

    fn job_generation(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<meerschaum_jobs::supervisor::JobGeneration>> {
        let revision = self
            .job_revision(name, properties)
            .ok_or_else(|| std::io::Error::other("The job has no current canonical generation."))?;
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|value| Uuid::parse_str(value).ok())
            .ok_or_else(|| std::io::Error::other("Invalid canonical job."))?;
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        let generation = inner
            .entries
            .get(&id)
            .filter(|entry| entry.revision == revision)
            .map(|entry| entry.generation.clone())
            .ok_or_else(|| std::io::Error::other("The job generation changed before launch."))?;
        if let Some(generation) = &generation {
            let path = self
                .root
                .join(id.simple().to_string())
                .join("identity.json");
            let mut proof = ContextProof::read(&path).map_err(std::io::Error::other)?;
            if proof.revision != revision || proof.generation.as_ref() != Some(generation) {
                return Err(std::io::Error::other(
                    "The sealed job lifecycle binding changed.",
                ));
            }
            proof.attempted = true;
            write_private(
                &path,
                &serde_json::to_string(&proof).map_err(std::io::Error::other)?,
            )?;
            generation.pending()?;
        }
        Ok(generation)
    }
    fn authoritative(&self, name: &str, properties: &meerschaum_core::Map) -> bool {
        if properties.contains_key(meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY) {
            return true;
        }
        name.strip_prefix("mrsm-")
            .and_then(|value| Uuid::parse_str(value).ok())
            .is_some_and(|id| {
                self.inner
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .entries
                    .contains_key(&id)
                    || self.factory() && self.recorded(id)
            })
    }

    fn tracks_unattributed_publication(&self) -> bool {
        self.factory()
    }

    fn unattributed_team_environment(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<Vec<(String, String)>>> {
        if !self.factory() || self.authoritative(name, properties) {
            return Ok(None);
        }
        self.unattributed_team_job(name)
            .map(Some)
            .map_err(std::io::Error::other)
    }

    fn owner_guard(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
    ) -> std::io::Result<Option<meerschaum_jobs::supervisor::LifecycleOwner>> {
        if properties.contains_key(meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY) {
            self.job_revision(name, properties).ok_or_else(|| {
                std::io::Error::other("The job's current signed owner context is unavailable.")
            })?;
        } else if !self.factory() {
            return Ok(None);
        } else if self.authoritative(name, properties) {
            return Err(std::io::Error::other(
                "The signed Factory job lost its launch identity.",
            ));
        }
        // This host process owns the context. Neither persisted job properties nor its old
        // environment can nominate another process to keep an orphaned worker alive.
        meerschaum_jobs::supervisor::LifecycleOwner::current().map(Some)
    }

    fn environment(
        &self,
        name: &str,
        properties: &meerschaum_core::Map,
        base: &[(String, String)],
    ) -> std::io::Result<Vec<(String, String)>> {
        use meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY;
        let marked = properties.get(JOB_CONTEXT_PROPERTY);
        let id = name
            .strip_prefix("mrsm-")
            .and_then(|s| Uuid::parse_str(s).ok());
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let entry = id.and_then(|id| inner.entries.get(&id).map(|e| (id, e)));
        if marked.is_none() && entry.is_none() {
            if self.factory() && id.is_some_and(|id| self.recorded(id)) {
                return Err(std::io::Error::other(
                    "The signed Factory job lost its launch identity.",
                ));
            }
            return Ok(base.to_vec());
        }
        let (id, entry) = entry.ok_or_else(|| {
            std::io::Error::other("The job's signed launch context is unavailable.")
        })?;
        if marked
            .and_then(meerschaum_core::Value::as_str)
            .and_then(|s| Uuid::parse_str(s).ok())
            != Some(id)
            || entry.identity.target != (Target::Job { name: name.into() })
            || entry.expires_at <= chrono::Utc::now().timestamp()
        {
            return Err(std::io::Error::other(
                "The job's signed launch context is invalid or stale.",
            ));
        }
        entry.environment.clone().ok_or_else(|| {
            std::io::Error::other("The signed launch has no exact prepared runtime.")
        })
    }
}

/// Both the lease and its rotating context must be current before creating a shell/window.
pub(crate) struct Sessions {
    pub terminals: Arc<TerminalSessions>,
    pub contexts: Arc<Contexts>,
}

#[async_trait::async_trait]
impl SessionLookup for Sessions {
    async fn username(&self, id: &str) -> Result<Option<String>, String> {
        self.terminals.username(id).await
    }
    async fn restricted(&self, id: &str) -> Result<bool, String> {
        let caller = meerschaum_server::executor::gateway::current()
            .ok_or("An executor terminal requires signed authority.")?;
        let id = Uuid::parse_str(id).map_err(|_| "Invalid terminal identity.")?;
        let grant = self
            .terminals
            .grant(id, &caller.claims.actor)
            .ok_or("This terminal lease is unavailable.")?;
        self.contexts.terminal_restricted(id, &grant)
    }
    async fn shell(&self, id: &str, configured: &ShellCommand) -> Result<ShellCommand, String> {
        let caller = meerschaum_server::executor::gateway::current()
            .ok_or("An executor terminal requires signed authority.")?;
        let id = Uuid::parse_str(id).map_err(|_| "Invalid terminal identity.")?;
        let grant = self
            .terminals
            .grant(id, &caller.claims.actor)
            .ok_or("This terminal lease is unavailable.")?;
        let contexts = self.contexts.clone();
        let configured = configured.clone();
        tokio::task::spawn_blocking(move || contexts.shell(id, &grant, &configured))
            .await
            .map_err(|_| "The terminal context was interrupted.".to_string())?
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
mod tests {
    use super::*;
    use meerschaum_server::executor::control::Actor;

    trait NativeModelConfiguration {
        fn configure_test(&self, base: meerschaum_core::Map) -> Result<(), String>;
    }
    impl NativeModelConfiguration for Contexts {
        fn configure_test(&self, base: meerschaum_core::Map) -> Result<(), String> {
            native_model_host()?;
            self.configure_runtime(
                base,
                meerschaum_core::process_env::vars()
                    .into_iter()
                    .filter(|(name, _)| meerschaum_core::team_scope::is_allowed(name))
                    .collect(),
            )
        }
    }

    fn factory_contexts(root: &std::path::Path) -> Contexts {
        Contexts::for_factory(root.join("work"), root.join("control")).unwrap()
    }

    fn team_generation(contexts: &Contexts, id: Uuid) -> PathBuf {
        let inner = contexts.inner.lock().unwrap();
        let environment = inner.entries[&id].environment.as_ref().unwrap();
        let (_, path) = environment
            .iter()
            .find(|(key, _)| key == "MRSM_CONFIG_FILE")
            .unwrap();
        std::path::Path::new(path).parent().unwrap().to_path_buf()
    }

    #[test]
    fn rebuild_revision_requires_exact_current_job_provenance() {
        use meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY;
        let root = tempfile::tempdir().unwrap();
        let contexts = factory_contexts(root.path());
        contexts
            .configure_test(
                meerschaum_core::Value::from_json_str(
                    r#"{"meerschaum":{"connectors":{"api":{}}}}"#,
                )
                .unwrap()
                .to_map(),
            )
            .unwrap();
        let id = Uuid::new_v4();
        let name = format!("mrsm-{}", id.simple());
        let properties =
            meerschaum_core::Map::from_iter([(JOB_CONTEXT_PROPERTY.into(), id.to_string().into())]);
        assert_eq!(contexts.job_revision(&name, &properties), None);
        let mut current = update(id, 30);
        current.revision = 7;
        current.target = Target::Job { name: name.clone() };
        contexts.update(&actor("alice"), current).unwrap();
        assert_eq!(contexts.job_revision(&name, &properties), Some(7));
        assert_eq!(contexts.job_revision(&name, &Default::default()), None);
        assert_eq!(
            contexts.job_revision(&format!("{name}/child"), &properties),
            None
        );
        let other = meerschaum_core::Map::from_iter([(
            JOB_CONTEXT_PROPERTY.into(),
            Uuid::new_v4().to_string().into(),
        )]);
        assert_eq!(contexts.job_revision(&name, &other), None);
        contexts
            .inner
            .lock()
            .unwrap()
            .entries
            .get_mut(&id)
            .unwrap()
            .expires_at = chrono::Utc::now().timestamp();
        assert_eq!(contexts.job_revision(&name, &properties), None);
        contexts
            .clear(&meerschaum_jobs::JobsContext::for_root(
                root.path().join("control"),
            ))
            .unwrap();
        assert_eq!(contexts.job_revision(&name, &properties), None);
    }

    #[test]
    fn late_renewal_and_delete_cannot_revive_or_stop_a_new_launch_revision() {
        let root = tempfile::tempdir().unwrap();
        let contexts = factory_contexts(root.path());
        contexts
            .configure_test(
                meerschaum_core::Value::from_json_str(
                    r#"{"meerschaum":{"connectors":{"api":{}}}}"#,
                )
                .unwrap()
                .to_map(),
            )
            .unwrap();
        let id = Uuid::new_v4();
        contexts.update(&actor("alice"), update(id, 20)).unwrap();
        contexts.revoke(&actor("alice"), id, Some(1)).unwrap();
        assert!(contexts.update(&actor("alice"), update(id, 30)).is_err());
        let mut next = update(id, 30);
        next.revision = 2;
        contexts.update(&actor("alice"), next).unwrap();
        assert!(contexts.revoke(&actor("alice"), id, Some(1)).is_err());
        assert!(
            team_generation(&contexts, id)
                .join("credential.json")
                .exists()
        );
    }

    #[test]
    fn delayed_job_retirement_keeps_a_new_revision_behind_its_stop_and_never_stops_it_late() {
        use std::sync::mpsc;
        use std::time::Duration;
        let root = tempfile::tempdir().unwrap();
        let contexts = Arc::new(factory_contexts(root.path()));
        contexts
            .configure_test(
                meerschaum_core::Value::from_json_str(
                    r#"{"meerschaum":{"connectors":{"api":{}}}}"#,
                )
                .unwrap()
                .to_map(),
            )
            .unwrap();
        let id = Uuid::new_v4();
        let name = format!("mrsm-{}", id.simple());
        let mut current = update(id, 25);
        current.target = Target::Job { name: name.clone() };
        contexts.update(&actor("alice"), current).unwrap();
        // Without the owning job runtime even a valid actor cannot forget an active target.
        assert!(contexts.revoke(&actor("alice"), id, Some(1)).is_err());
        let (entered_tx, entered_rx) = mpsc::channel();
        let (finish_tx, finish_rx) = mpsc::channel();
        let retiring = contexts.clone();
        let expected = name.clone();
        let retirement = std::thread::spawn(move || {
            retiring.revoke_with(&actor("alice"), id, Some(1), |target| {
                assert_eq!(target, &Target::Job { name: expected });
                entered_tx.send(()).unwrap();
                finish_rx
                    .recv_timeout(Duration::from_secs(5))
                    .map_err(|_| "The owned stop model was not released.".to_string())?;
                Ok(())
            })
        });
        entered_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            contexts.inner.try_lock().is_err(),
            "The stop must hold the context lock."
        );
        let (attempt_tx, attempt_rx) = mpsc::channel();
        let (delivered_tx, delivered_rx) = mpsc::channel();
        let delivering = contexts.clone();
        let mut next = update(id, 26);
        next.target = Target::Job { name: name.clone() };
        next.revision = 2;
        let delivery = std::thread::spawn(move || {
            attempt_tx.send(()).unwrap();
            let result = delivering.update(&actor("alice"), next);
            delivered_tx.send(result).unwrap();
        });
        attempt_rx.recv_timeout(Duration::from_secs(5)).unwrap();
        assert!(
            delivered_rx
                .recv_timeout(Duration::from_millis(30))
                .is_err()
        );
        finish_tx.send(()).unwrap();
        assert_eq!(
            retirement.join().unwrap().unwrap(),
            Some(Target::Job { name })
        );
        delivered_rx
            .recv_timeout(Duration::from_secs(5))
            .unwrap()
            .unwrap();
        delivery.join().unwrap();
        let mut stopped_newer = false;
        assert!(
            contexts
                .revoke_with(&actor("alice"), id, Some(1), |_| {
                    stopped_newer = true;
                    Ok(())
                })
                .is_err()
        );
        assert!(
            !stopped_newer,
            "A delayed old retirement must not invoke the stop callback."
        );
        assert_eq!(contexts.inner.lock().unwrap().entries[&id].revision, 2);
        assert!(
            team_generation(&contexts, id)
                .join("credential.json")
                .is_file()
        );
    }

    #[test]
    fn marked_jobs_require_a_current_exact_context_on_every_restart_and_after_expiry() {
        use meerschaum_server::executor::environment::JOB_CONTEXT_PROPERTY;
        let root = tempfile::tempdir().unwrap();
        let contexts = Arc::new(factory_contexts(root.path()));
        contexts
            .configure_test(
                meerschaum_core::Value::from_json_str(
                    r#"{"meerschaum":{"connectors":{"api":{}}}}"#,
                )
                .unwrap()
                .to_map(),
            )
            .unwrap();
        let id = Uuid::new_v4();
        let name = format!("mrsm-{}", id.simple());
        let props =
            meerschaum_core::Map::from_iter([(JOB_CONTEXT_PROPERTY.into(), id.to_string().into())]);
        let jobs = meerschaum_jobs::JobsContext::for_root(root.path().join("control"))
            .with_launch_environment(vec![("MRSM_CONFIG".into(), "ambient".into())])
            .with_launch_resolver(contexts.clone());
        assert!(jobs.environment_for(&name, &props).is_err());
        let mut current = update(id, 30);
        current.target = Target::Job { name: name.clone() };
        contexts.update(&actor("alice"), current).unwrap();
        let environment = jobs.environment_for(&name, &props).unwrap();
        assert!(!environment.iter().any(|(key, _)| key == "MRSM_CONFIG"));
        assert!(environment.iter().any(|(key, _)| key == "MRSM_CONFIG_FILE"));
        assert!(
            jobs.environment_for(&format!("mrsm-{}", Uuid::new_v4().simple()), &props)
                .is_err()
        );
        contexts
            .inner
            .lock()
            .unwrap()
            .entries
            .get_mut(&id)
            .unwrap()
            .expires_at = chrono::Utc::now().timestamp() - 61;
        contexts.expire_jobs(&jobs);
        assert!(jobs.environment_for(&name, &props).is_err());
        let mut late = update(id, 30);
        late.target = Target::Job { name };
        assert!(contexts.update(&actor("alice"), late).is_err());
    }

    fn actor(owner: &str) -> Actor {
        Actor {
            owner: owner.into(),
            username: Some(owner.into()),
            permissions: Default::default(),
            administrator: false,
        }
    }
    fn update(id: Uuid, seconds: i64) -> Update {
        Update {
            metadata_deadline: None,
            discovery_catalog_sha256: None,
            extension: None,
            registration: None,
            id,
            revision: 1,
            owner: "alice".into(),
            owner_id: Some("registered-alice".into()),
            profile: Profile::Team,
            target: Target::Terminal,
            callback_url: "https://main.example:8989".into(),
            credential: meerschaum_connector_api::models::ExecutionCredential {
                token: format!("synthetic-{seconds}"),
                expires_at: chrono::Utc::now().timestamp() + seconds,
            },
        }
    }
    #[test]
    fn metadata_renewal_cannot_install_a_callback_or_change_its_profile() {
        use meerschaum_server::executor::environment::{METADATA_ORIGIN, METADATA_TOKEN};
        let root = tempfile::tempdir().unwrap();
        let contexts = factory_contexts(root.path());
        contexts.configure_test(Default::default()).unwrap();
        let id = Uuid::new_v4();
        let directory = contexts.root.join(id.simple().to_string());
        let generation = directory.join("generation-test");
        create_private_dir(&generation).unwrap();
        let deadline = chrono::Utc::now().timestamp() + 40;
        let identity = Identity {
            source_compiler_sha256: None,
            metadata_deadline: Some(deadline),
            extension_sha256: None,
            registration_sha256: None,
            discovery_catalog_sha256: Some("a".repeat(64)),
            owner: "alice".into(),
            owner_id: Some("registered-alice".into()),
            profile: Profile::Metadata,
            target: Target::Job {
                name: format!("mrsm-{}", id.simple()),
            },
            callback_url: METADATA_ORIGIN.into(),
        };
        let proof = ContextProof {
            compiler: None,
            id,
            revision: 1,
            identity: identity.clone(),
            generation: None,
            attempted: false,
            cleared_for_rebuild: false,
            terminal_tracking: 0,
            terminal_spec: None,
        };
        write_private(
            &directory.join("identity.json"),
            &serde_json::to_string(&proof).unwrap(),
        )
        .unwrap();
        let spec_path = generation.join(meerschaum_core::team_scope::sandbox::JOB_SPEC_FILE);
        write_private(
            &spec_path,
            &serde_json::to_string(&meerschaum_core::team_scope::sandbox::Spec {
                mode: meerschaum_core::team_scope::sandbox::Mode::RequireSockets,
                ..Default::default()
            })
            .unwrap(),
        )
        .unwrap();
        contexts.inner.lock().unwrap().entries.insert(
            id,
            Entry {
                compiler: None,
                identity,
                revision: 1,
                expires_at: chrono::Utc::now().timestamp() + 10,
                environment: Some(vec![
                    (
                        "MRSM_CONFIG_FILE".into(),
                        generation.join("config.json").display().to_string(),
                    ),
                    ("MRSM_TEAM_SANDBOX".into(), spec_path.display().to_string()),
                ]),
                generation: None,
            },
        );
        let make_update = || {
            let mut value = update(id, 20);
            value.profile = Profile::Metadata;
            value.metadata_deadline = Some(deadline);
            value.discovery_catalog_sha256 = Some("a".repeat(64));
            value.target = Target::Job {
                name: format!("mrsm-{}", id.simple()),
            };
            value.callback_url = METADATA_ORIGIN.into();
            value.credential.token = METADATA_TOKEN.into();
            value
        };
        contexts.update(&actor("alice"), make_update()).unwrap();
        let token_path = generation.join("credential.json");
        let sentinel = std::fs::read_to_string(&token_path).unwrap();
        assert!(sentinel.contains(METADATA_TOKEN));
        for offset in [-1, 1] {
            let mut changed = make_update();
            changed.metadata_deadline = Some(deadline + offset);
            assert!(contexts.update(&actor("alice"), changed).is_err());
            assert_eq!(std::fs::read_to_string(&token_path).unwrap(), sentinel);
        }
        for revision in [1, 2] {
            let mut changed = make_update();
            changed.revision = revision;
            changed.credential.token = "actual-callback-must-never-be-written".into();
            assert!(contexts.update(&actor("alice"), changed).is_err());
            assert_eq!(std::fs::read_to_string(&token_path).unwrap(), sentinel);
        }
        let mut promoted = make_update();
        promoted.profile = Profile::Api;
        promoted.callback_url = "https://main.example".into();
        promoted.credential.token = "actual-callback-must-never-be-written".into();
        assert!(contexts.update(&actor("alice"), promoted).is_err());
        assert_eq!(std::fs::read_to_string(&token_path).unwrap(), sentinel);
    }

    #[test]
    fn team_configuration_rotates_only_its_bearer_and_keeps_the_launch_identity() {
        let root = tempfile::tempdir().unwrap();
        let contexts = factory_contexts(root.path());
        let base = meerschaum_core::Value::from_json_str(r#"{"meerschaum":{"instance":"sql:team", "connectors":{"sql":{"team":{"password":"synthetic-team-only"},"main":null},"api":{}}}}"#).unwrap().to_map();
        contexts.configure_test(base).unwrap();
        let id = Uuid::new_v4();
        assert!(contexts.update(&actor("bob"), update(id, 20)).is_err());
        contexts.update(&actor("alice"), update(id, 20)).unwrap();
        let directory = team_generation(&contexts, id);
        let config = std::fs::read_to_string(directory.join("config.json")).unwrap();
        let before_environment = contexts.inner.lock().unwrap().entries[&id]
            .environment
            .clone()
            .unwrap();
        assert!(
            before_environment
                .iter()
                .any(|(name, value)| name == "MRSM_ROOT_DIR"
                    && std::path::Path::new(value) == root.path().join("work"))
        );
        let descriptor = before_environment
            .iter()
            .find(|(name, _)| name == meerschaum_core::team_scope::source_namespace::ENV)
            .unwrap();
        let selected = meerschaum_core::team_scope::source_namespace::Launch::read(
            std::path::Path::new(&descriptor.1),
        )
        .unwrap();
        assert!(selected.namespace.names.is_empty());
        assert!(selected.namespace.package.starts_with(&directory));
        let json: serde_json::Value = serde_json::from_str(&config).unwrap();
        assert_eq!(json["meerschaum"]["instance"], "sql:team");
        assert!(json["meerschaum"]["connectors"]["sql"]["main"].is_null());
        assert_eq!(json["meerschaum"]["executor"], "api:main_callback");
        assert!(!config.contains("synthetic-20"));
        let mut moved = update(id, 25);
        moved.callback_url = "https://other.example".into();
        assert!(contexts.update(&actor("alice"), moved).is_err());
        let mut replacement = update(id, 25);
        replacement.owner_id = Some("replacement-alice".into());
        assert!(contexts.update(&actor("alice"), replacement).is_err());
        let mut api = update(id, 25);
        api.profile = Profile::Api;
        assert!(contexts.update(&actor("alice"), api).is_err());
        contexts.update(&actor("alice"), update(id, 30)).unwrap();
        assert_eq!(
            contexts.inner.lock().unwrap().entries[&id]
                .environment
                .as_ref(),
            Some(&before_environment)
        );
        assert!(contexts.update(&actor("alice"), update(id, 10)).is_err());
        assert_eq!(
            std::fs::read_to_string(directory.join("config.json")).unwrap(),
            config
        );
        let token = std::fs::read_to_string(directory.join("credential.json")).unwrap();
        assert!(token.contains("synthetic-30"));
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(directory.join("credential.json"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o600
            );
        }
        assert!(contexts.revoke(&actor("bob"), id, Some(1)).is_err());
        contexts.revoke(&actor("alice"), id, Some(1)).unwrap();
        assert!(!directory.exists());
        assert!(!root.path().join("unrelated").exists());
    }

    #[test]
    fn terminal_shell_requires_the_registered_identity_and_removes_ambient_config_patches() {
        let root = tempfile::tempdir().unwrap();
        let contexts = factory_contexts(root.path());
        contexts
            .configure_test(
                meerschaum_core::Value::from_json_str(
                    r#"{"meerschaum":{"connectors":{"api":{}}}}"#,
                )
                .unwrap()
                .to_map(),
            )
            .unwrap();
        let id = Uuid::new_v4();
        let mut grant = meerschaum_server::executor::terminal::TerminalGrant {
            id,
            owner: "alice".into(),
            owner_id: "registered-alice".into(),
        };
        let mut shell = ShellCommand::new("mrsm");
        shell.env = [
            "MRSM_CONFIG",
            "MRSM_CONFIG_FILE",
            "MRSM_PATCH",
            "MRSM_PATCH_FILE",
            "MRSM_PLUGINS_DIR",
        ]
        .map(|key| (key.into(), "ambient".into()))
        .into();
        assert!(contexts.shell(id, &grant, &shell).is_err());
        contexts.update(&actor("alice"), update(id, 30)).unwrap();
        grant.owner_id = "replacement-alice".into();
        assert!(contexts.shell(id, &grant, &shell).is_err());
        grant.owner_id = "registered-alice".into();
        let shell = contexts.shell(id, &grant, &shell).unwrap();
        assert!(shell.clear_env);
        assert_eq!(
            shell.args.first().map(String::as_str),
            Some(crate::executor_terminal_guard::TEAM_EXEC_ARG),
        );
        assert!(!shell.env.iter().any(|(key, value)| value == "ambient"
            || matches!(
                key.as_str(),
                "MRSM_CONFIG" | "MRSM_PATCH" | "MRSM_PATCH_FILE" | "TMUX" | "TMUX_PANE"
            )));
        assert!(
            shell
                .env
                .iter()
                .any(|(key, _)| key == meerschaum_core::team_scope::source_namespace::ENV)
        );
        assert!(
            shell
                .env
                .iter()
                .any(|(key, value)| key == "MRSM_CONFIG_FILE" && value.ends_with("/config.json"))
        );
        contexts
            .clear(&meerschaum_jobs::JobsContext::for_root(
                root.path().join("control"),
            ))
            .unwrap();
        assert!(contexts.shell(id, &grant, &shell).is_err());
    }
}
