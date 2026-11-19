//! API-profile job launch preparation for executor contexts.

use super::*;

pub(super) struct LaunchDirectories<'a> {
    pub(super) control: &'a std::path::Path,
    pub(super) work: &'a std::path::Path,
    pub(super) launch: &'a std::path::Path,
}

pub(super) struct ApiJobLaunch<'a> {
    pub(super) directories: LaunchDirectories<'a>,
    pub(super) base: &'a meerschaum_core::Map,
    pub(super) trusted: &'a [(String, String)],
    pub(super) name: Option<&'a str>,
    pub(super) callback_url: &'a str,
    pub(super) metadata: bool,
    pub(super) host_sources: Option<&'a [(std::ffi::OsString, PathBuf)]>,
    pub(super) registration: Option<&'a meerschaum_server::executor::registration::Registration>,
    pub(super) extension: Option<&'a meerschaum_server::executor::extensions::Operation>,
    pub(super) compiler: Option<&'a crate::executor_compiler::Compiler>,
    pub(super) factory_sdk: Option<&'a meerschaum_server::executor::factory_artifacts::Sdk>,
    pub(super) compiler_deadline: i64,
}

pub(super) fn prepare_api_job(job: ApiJobLaunch<'_>) -> Result<Vec<(String, String)>, String> {
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
