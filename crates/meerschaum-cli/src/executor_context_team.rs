//! Per-launch source namespaces for credentialed Team execution. Durable Team paths stay put.

use super::*;
use meerschaum_core::team_scope::{sandbox, source_namespace};
use meerschaum_server::executor::plugins;

impl Contexts {
    pub(crate) fn update_team_sources(
        &self,
        actor: &meerschaum_server::executor::control::Actor,
        update: Update,
        catalog: plugins::Validated,
        sources: Vec<(std::ffi::OsString, PathBuf)>,
    ) -> Result<(), String> {
        let digest = catalog
            .digest()
            .map_err(|_| "Invalid Team source catalog.")?;
        if update.profile != Profile::Team
            || update.discovery_catalog_sha256.as_deref() != Some(digest.as_str())
            || update.registration.is_some()
            || update.extension.is_some()
            || !catalog.catalog().placement.is_remote()
        {
            return Err("The Team context does not match its exact signed source catalog.".into());
        }
        let (mut base, mut environment) = self.team_inputs()?;
        base.insert(
            plugins::CONFIG_KEY.into(),
            meerschaum_core::Value::from_json_str(
                &String::from_utf8(
                    catalog
                        .encode()
                        .map_err(|_| "Invalid Team source catalog.")?,
                )
                .map_err(|_| "Invalid Team source catalog.")?,
            )
            .map_err(|_| "Invalid Team source catalog.")?,
        );
        environment.retain(|(name, _)| name != "MRSM_EXECUTOR_PLUGINS");
        environment.push((
            "MRSM_EXECUTOR_PLUGINS".into(),
            serde_json::to_string(&catalog.catalog().plugins.keys().collect::<Vec<_>>())
                .map_err(|_| "Invalid Team source ceiling.")?,
        ));
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

    /// Build a fresh boundary for a Factory job with no Main callback. Only its
    /// owning supervisor starts this path, after the durable job definition exists.
    pub(super) fn unattributed_team_job(
        &self,
        name: &str,
    ) -> Result<Vec<(String, String)>, String> {
        if meerschaum_jobs::names::invalid_job_name_reason(name).is_some() {
            return Err("Invalid Factory job name.".into());
        }
        let control = self
            .root
            .parent()
            .ok_or("Invalid Factory control storage.")?;
        let (base, trusted) = self.team_inputs()?;
        let mut selected = None;
        let mut sources = Vec::new();
        if let Some(value) = base.get(plugins::CONFIG_KEY) {
            let catalog: plugins::Catalog = serde_json::from_str(&value.to_json_string())
                .map_err(|_| "Invalid Factory source catalog.")?;
            let directory = trusted
                .iter()
                .find(|(key, _)| key == "MRSM_PLUGINS_DIR")
                .map(|(_, path)| std::path::Path::new(path))
                .ok_or("The Factory's selected plugin view is unavailable.")?;
            for (name, plugin) in &catalog.plugins {
                let filename = plugin.kind.entry_name(name);
                sources.push((filename.clone().into(), directory.join(filename)));
            }
            selected = Some(plugins::sha256(
                &serde_json::to_vec(&catalog).map_err(|_| "Invalid Factory source catalog.")?,
            ));
        }
        let job = control.join("jobs").join(name);
        if !meerschaum_jobs::names::is_direct_child(&control.join("jobs"), &job, name)
            || !std::fs::symlink_metadata(&job).is_ok_and(|metadata| metadata.is_dir())
        {
            return Err("The Factory job definition has no sealed directory.".into());
        }
        // Job deletion also removes its callback-free generations. A restart always
        // selects the currently configured catalog and never opens an older generation.
        let directory = job
            .join("factory")
            .join("execution-contexts")
            .join(Uuid::new_v4().simple().to_string());
        crate::executor_profiles::private_directory(&directory)?;
        let result = prepare(
            LaunchDirectories {
                work: &self.work_root,
                control,
                launch: &directory,
            },
            &base,
            None,
            &trusted,
            &sources,
            selected.as_deref(),
            Some(name),
        );
        if result.is_err() {
            let _ = std::fs::remove_dir_all(directory);
        }
        result
    }

    pub(super) fn team_inputs(
        &self,
    ) -> Result<(meerschaum_core::Map, Vec<(String, String)>), String> {
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        Ok((
            inner
                .base
                .clone()
                .ok_or("The executor is not configured.")?,
            inner
                .runtime_environment
                .clone()
                .ok_or("Team execution needs a trusted native runtime.")?,
        ))
    }
}

pub(super) fn prepare(
    directories: LaunchDirectories<'_>,
    base: &meerschaum_core::Map,
    callback_url: Option<&str>,
    trusted: &[(String, String)],
    sources: &[(std::ffi::OsString, PathBuf)],
    selected: Option<&str>,
    job_name: Option<&str>,
) -> Result<Vec<(String, String)>, String> {
    let LaunchDirectories {
        work: server_root,
        control: control_root,
        launch: directory,
    } = directories;
    let root =
        std::fs::canonicalize(server_root).map_err(|_| "The Team runtime root is unavailable.")?;
    let control_root = std::fs::canonicalize(control_root)
        .map_err(|_| "The Factory control root is unavailable.")?;
    if root.starts_with(&control_root) || control_root.starts_with(&root) {
        return Err("Team command state must be separate from Factory control storage.".into());
    }
    let directory =
        std::fs::canonicalize(directory).map_err(|_| "The Team context is unavailable.")?;
    let namespace_control = directory
        .parent()
        .and_then(std::path::Path::parent)
        .ok_or("The Team launch has no sealed context parent.")?;
    if !namespace_control.starts_with(&control_root) {
        return Err("The Team launch is outside Factory control storage.".into());
    }
    let catalog: Option<plugins::Catalog> = base
        .get(plugins::CONFIG_KEY)
        .map(|value| {
            serde_json::from_str(&value.to_json_string())
                .map_err(|_| "Invalid Team source catalog.")
        })
        .transpose()?;
    let mut expected = BTreeMap::new();
    if let Some(catalog) = &catalog {
        if !catalog.placement.is_remote()
            || selected
                != Some(
                    plugins::sha256(
                        &serde_json::to_vec(catalog).map_err(|_| "Invalid Team source catalog.")?,
                    )
                    .as_str(),
                )
        {
            return Err(
                "The Team source catalog differs from its immutable context identity.".into(),
            );
        }
        for (name, source) in &catalog.plugins {
            expected.insert(source.kind.entry_name(name), name.clone());
        }
    } else if selected.is_some() {
        return Err("The Team source catalog is missing.".into());
    }
    let mut entries = BTreeMap::new();
    for (filename, source) in sources {
        let filename = filename.to_str().ok_or("Invalid Team source filename.")?;
        let source =
            std::fs::canonicalize(source).map_err(|_| "A selected Team source is unavailable.")?;
        if !expected.contains_key(filename)
            || !source.starts_with(control_root.join("plugins/.executor-objects"))
            || entries.insert(filename.to_string(), source).is_some()
        {
            return Err("The Team source view differs from its signed archive selection.".into());
        }
    }
    if entries.keys().ne(expected.keys()) {
        return Err("The Team source view is incomplete.".into());
    }

    let namespace_root = directory.join(format!("team-sources-{}", Uuid::new_v4().simple()));
    create_private_dir(&namespace_root)
        .map_err(|_| "Could not prepare the Team source namespace.")?;
    let result = (|| {
        // These names are Team-owned. Create a missing directory without changing
        // permissions through an existing Team-selected link.
        for name in ["tmp", "config"] {
            let path = root.join(name);
            match std::fs::create_dir(&path) {
                Ok(()) => {}
                Err(error)
                    if error.kind() == std::io::ErrorKind::AlreadyExists
                        && std::fs::symlink_metadata(&path).is_ok_and(|m| m.is_dir()) => {}
                Err(_) => return Err("The durable Team directory is unavailable.".into()),
            }
        }
        let view = namespace_root.join("plugins");
        let package = namespace_root.join("package/plugins");
        let cache = namespace_root.join("cache");
        for path in [&view, &package, &cache] {
            create_private_dir(path).map_err(|_| "Could not prepare the Team source namespace.")?;
        }
        create_private_dir(&cache.join("shell-state"))
            .map_err(|_| "Could not prepare the Team terminal state.")?;
        write_private(&package.join("__init__.py"), "")
            .map_err(|_| "Could not prepare the Team source package.")?;
        for (filename, source) in &entries {
            for path in [&view, &package] {
                #[cfg(unix)]
                std::os::unix::fs::symlink(source, path.join(filename))
                    .map_err(|_| "Could not bind a selected Team source.")?;
                #[cfg(not(unix))]
                return Err("Team source namespaces need a supported native Unix launcher.".into());
            }
        }
        // The Team retains its durable state, while every parent-trusted object and
        // sibling private launch is outside that recursively writable subtree.
        let canary = sandbox::ensure_canary(namespace_control)
            .map_err(|_| "Could not prepare the Factory confinement canary.")?;
        let mut model = sandbox::Spec {
            mode: sandbox::Mode::RequireSockets,
            team: "Team execution".into(),
            protected: vec![
                PathBuf::from("/"),
                PathBuf::from("/run/secrets"),
                control_root.clone(),
            ],
            canary,
            ..sandbox::Spec::default()
        };
        if namespace_control != control_root {
            model.protected.push(namespace_control.into());
        }
        model.rules = sandbox::SYSTEM_RULES
            .iter()
            .map(|(path, access, why)| sandbox::Rule::new(*path, *access, why))
            .chain([
                sandbox::Rule::new(
                    &root,
                    sandbox::Access::ReadWrite,
                    "durable Team command state",
                ),
                sandbox::Rule::new(
                    &namespace_root,
                    sandbox::Access::Read,
                    "this Team generation's sealed configuration",
                ),
                sandbox::Rule::new(
                    &cache,
                    sandbox::Access::ReadWrite,
                    "this Team launch's discovery cache",
                ),
            ])
            .collect();
        for source in entries.values() {
            model.rules.push(sandbox::Rule::new(
                source,
                sandbox::Access::ReadExec,
                "exact signed Team source",
            ));
        }
        if let Some(name) = job_name {
            if callback_url.is_some()
                && !name.strip_prefix("mrsm-").is_some_and(|id| {
                    directory.file_name().and_then(|part| part.to_str()) == Some(id)
                })
            {
                return Err("The Team job belongs to another context.".into());
            }
            let work = namespace_root.join(sandbox::JOB_WORK_DIR);
            create_private_dir(&work)
                .map_err(|_| "Could not prepare the Team job work directory.")?;
            model.rules.push(sandbox::Rule::new(
                work,
                sandbox::Access::ReadWrite,
                "this Team job's result and heartbeat",
            ));
        }
        // Preparation reads host-selected runtime files; it never starts Python.
        let prepared_rule_count = model.rules.len();
        let runtime = sandbox::runtime::Runtime::prepare(&mut model, trusted)
            .map_err(|_| "The Team source launch needs a trusted native interpreter/runtime.")?;
        if runtime.launcher.starts_with(&root) {
            return Err(
                "The native Team launcher must be installed outside writable Team state.".into(),
            );
        }
        let runtime_rules = model.rules.split_off(prepared_rule_count);
        for rule in runtime_rules {
            if rule.path.starts_with(&control_root) || rule.path.starts_with("/run/secrets") {
                return Err("A Team interpreter grant selects private Factory storage.".into());
            }
            // The root grant already includes Team dependencies. A name inside writable
            // state must not become an independent grant on a later pathname lookup.
            if !rule.path.starts_with(&root) {
                model.rules.push(rule);
            }
        }
        let (rules, dropped) = model.allowed(model.rules.clone());
        if !dropped.is_empty() {
            return Err("A Team runtime grant covers sealed Factory control storage.".into());
        }
        model.rules = rules;
        model.runtime = Some(runtime.clone());
        let launch = source_namespace::Launch {
            schema: 1,
            release: env!("CARGO_PKG_VERSION").into(),
            namespace: source_namespace::Namespace {
                root: root.clone(),
                control_root: namespace_control.into(),
                view: view.clone(),
                package,
                cache,
                catalog_sha256: selected.map(str::to_string),
                names: expected.values().cloned().collect(),
            },
            runtime,
            sandbox: model.clone(),
        };
        let descriptor = namespace_root.join(source_namespace::FILE);
        write_private(
            &descriptor,
            &serde_json::to_string(&launch).map_err(|_| "Invalid Team runtime.")?,
        )
        .map_err(|_| "Could not write the Team source descriptor.")?;
        let spec_file = namespace_root.join(if job_name.is_some() {
            sandbox::JOB_SPEC_FILE
        } else {
            sandbox::SPEC_FILE
        });
        write_private(
            &spec_file,
            &serde_json::to_string(&model).map_err(|_| "Invalid Team command confinement.")?,
        )
        .map_err(|_| "Could not write the Team command confinement.")?;
        source_namespace::Launch::read(&descriptor)?;
        let config_file = namespace_root.join("config.json");
        let mut config = serde_json::to_value(base)
            .map_err(|_| "Could not encode the Team launch configuration.")?;
        if let Some(url) = callback_url {
            config["meerschaum"]["connectors"]["api"]["main_callback"] = serde_json::json!({
                "uri": url,
                "credential_file": namespace_root.join("credential.json"),
            });
            config["meerschaum"]["executor"] = "api:main_callback".into();
        }
        write_private(&config_file, &config.to_string())
            .map_err(|_| "Could not write the Team launch configuration.")?;
        let mut environment: Vec<_> = trusted
            .iter()
            .filter(|(name, _)| {
                !matches!(
                    name.as_str(),
                    "MRSM_CONFIG"
                        | "MRSM_CONFIG_FILE"
                        | "MRSM_CONFIG_DIR"
                        | "MRSM_PATCH"
                        | "MRSM_PATCH_FILE"
                        | "MRSM_PLUGINS_DIR"
                        | "MRSM_EXECUTOR_PLUGINS"
                        | source_namespace::ENV
                        | sandbox::SPEC_VAR
                        | "MRSM_TEAM_SANDBOX_MODE"
                        | "XDG_CACHE_HOME"
                        | "TMPDIR"
                        | "MRSM_BIN"
                        | "MRSM_PYTHON"
                        | "TMUX"
                        | "TMUX_PANE"
                )
            })
            .cloned()
            .collect();
        // Root, cwd, dependencies and ordinary installed resources are preserved verbatim.
        environment.extend([
            ("MRSM_ROOT_DIR".into(), root.display().to_string()),
            (
                "MRSM_BIN".into(),
                launch.runtime.launcher.display().to_string(),
            ),
            (
                "MRSM_PYTHON".into(),
                launch.runtime.python.display().to_string(),
            ),
            ("MRSM_CONFIG_FILE".into(), config_file.display().to_string()),
            (
                "MRSM_CONFIG_DIR".into(),
                root.join("config").display().to_string(),
            ),
            ("MRSM_PLUGINS_DIR".into(), view.display().to_string()),
            (sandbox::SPEC_VAR.into(), spec_file.display().to_string()),
            ("MRSM_TEAM_SANDBOX_MODE".into(), "require_sockets".into()),
            (
                "XDG_CACHE_HOME".into(),
                launch.namespace.cache.display().to_string(),
            ),
            ("TMPDIR".into(), root.join("tmp").display().to_string()),
            (
                source_namespace::ENV.into(),
                descriptor.display().to_string(),
            ),
            (
                "MRSM_EXECUTOR_PLUGINS".into(),
                serde_json::to_string(&launch.namespace.names)
                    .map_err(|_| "Invalid Team source names.")?,
            ),
        ]);
        Ok(environment)
    })();
    if result.is_err() {
        let _ = std::fs::remove_dir_all(&namespace_root);
    }
    result
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use meerschaum_server::executor::{control::Actor, plugin_host::Placement};
    use std::collections::BTreeSet;

    fn selected(
        root: &std::path::Path,
        team: Uuid,
        generation: Uuid,
        name: &str,
        digest: char,
    ) -> (plugins::Validated, Vec<(std::ffi::OsString, PathBuf)>) {
        let catalog = plugins::Catalog {
            schema: plugins::SCHEMA,
            release: env!("CARGO_PKG_VERSION").into(),
            team_id: team,
            generation,
            placement: Placement::Remote,
            plugins: BTreeMap::from([(
                name.into(),
                plugins::Plugin {
                    archive_sha256: digest.to_string().repeat(64),
                    kind: plugins::SourceKind::PythonModule,
                    metadata: None,
                },
            )]),
        };
        let grants = BTreeSet::from([name.into()]);
        let validated = plugins::Validated::decode(
            &serde_json::to_vec(&catalog).unwrap(),
            plugins::Expected {
                team_id: team,
                generation,
                grants: &grants,
                reserved_actions: &BTreeSet::new(),
            },
        )
        .unwrap();
        let object = root
            .join("plugins/.executor-objects")
            .join(name)
            .join("source");
        create_private_dir(&object).unwrap();
        let source = object.join(format!("{name}.py"));
        std::fs::write(&source, "selected = True\n").unwrap();
        (validated, vec![(format!("{name}.py").into(), source)])
    }
    fn update(id: Uuid, digest: String, expires: i64) -> Update {
        Update {
            metadata_deadline: None,
            extension: None,
            registration: None,
            discovery_catalog_sha256: Some(digest),
            id,
            revision: 1,
            owner: "alice".into(),
            owner_id: Some("registered-alice".into()),
            profile: Profile::Team,
            target: Target::Job {
                name: format!("mrsm-{}", id.simple()),
            },
            callback_url: "https://main.example".into(),
            credential: meerschaum_connector_api::models::ExecutionCredential {
                token: "synthetic".into(),
                expires_at: expires,
            },
        }
    }
    fn descriptor(
        contexts: &Contexts,
        id: Uuid,
    ) -> (Vec<(String, String)>, source_namespace::Launch) {
        let env = contexts.inner.lock().unwrap().entries[&id]
            .environment
            .clone()
            .unwrap();
        let file = env
            .iter()
            .find(|(name, _)| name == source_namespace::ENV)
            .unwrap();
        let launch = source_namespace::Launch::read(std::path::Path::new(&file.1)).unwrap();
        (env, launch)
    }

    #[test]
    fn different_team_launches_keep_independent_sources_and_renew_without_rewriting_durable_state()
    {
        super::super::native_model_host().unwrap();
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("work");
        create_private_dir(&root).unwrap();
        let root = root.canonicalize().unwrap();
        let control = temp.path().join("control");
        create_private_dir(&control).unwrap();
        let durable = root.join("custom-durable-state");
        std::fs::write(&durable, "keep across jobs/reloads").unwrap();
        let venvs = root.join("installed-dependencies");
        create_private_dir(&venvs).unwrap();
        let mut trusted: Vec<_> = meerschaum_core::process_env::vars()
            .into_iter()
            .filter(|(name, _)| meerschaum_core::team_scope::is_allowed(name))
            .collect();
        trusted.push(("MRSM_VENVS_DIR".into(), venvs.display().to_string()));
        let contexts = Contexts::for_factory(root.clone(), control.clone()).unwrap();
        contexts
            .configure_runtime(meerschaum_core::Map::new(), trusted)
            .unwrap();
        let actor = Actor {
            owner: "alice".into(),
            username: None,
            permissions: Default::default(),
            administrator: false,
        };
        let team = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let (alpha, alpha_sources) = selected(&control, team, generation, "alpha", 'a');
        let (beta, beta_sources) = selected(&control, team, generation, "beta", 'b');
        let now = chrono::Utc::now().timestamp();
        let request = update(a, alpha.digest().unwrap(), now + 20);
        contexts
            .update_team_sources(
                &actor,
                request.clone(),
                alpha.clone(),
                alpha_sources.clone(),
            )
            .unwrap();
        contexts
            .update_team_sources(
                &actor,
                update(b, beta.digest().unwrap(), now + 20),
                beta.clone(),
                beta_sources.clone(),
            )
            .unwrap();
        let (original, view_a) = descriptor(&contexts, a);
        let (_, view_b) = descriptor(&contexts, b);
        let generation = view_a.namespace.view.parent().unwrap();
        let context = generation.parent().unwrap();
        assert!(generation.join("credential.json").is_file());
        assert!(!context.join("credential.json").exists());
        let binding = contexts.inner.lock().unwrap().entries[&a]
            .generation
            .clone()
            .unwrap();
        assert_eq!(binding.directory, generation);
        binding.validate(&format!("mrsm-{}", a.simple())).unwrap();
        assert!(
            view_a
                .sandbox
                .rules
                .iter()
                .all(|rule| !context.starts_with(&rule.path))
        );
        assert_ne!(view_a.namespace.package, view_b.namespace.package);
        assert!(view_a.namespace.package.join("alpha.py").exists());
        assert!(!view_a.namespace.package.join("beta.py").exists());
        assert!(view_b.namespace.package.join("beta.py").exists());
        assert!(!root.join(".internal/plugins/alpha.py").exists());
        assert!(original.contains(&("MRSM_ROOT_DIR".into(), root.display().to_string())));
        assert!(original.contains(&("MRSM_VENVS_DIR".into(), venvs.display().to_string())));
        let mut renewal = request.clone();
        renewal.credential.expires_at = now + 30;
        contexts
            .update_team_sources(&actor, renewal, alpha, alpha_sources)
            .unwrap();
        assert_eq!(descriptor(&contexts, a).0, original);
        let mut changed = request;
        changed.discovery_catalog_sha256 = Some(beta.digest().unwrap());
        changed.credential.expires_at = now + 35;
        assert!(
            contexts
                .update_team_sources(&actor, changed, beta, beta_sources)
                .is_err()
        );
        assert_eq!(descriptor(&contexts, a).0, original);
        assert_eq!(
            std::fs::read_to_string(&durable).unwrap(),
            "keep across jobs/reloads"
        );
        assert!(venvs.is_dir());
    }
}
