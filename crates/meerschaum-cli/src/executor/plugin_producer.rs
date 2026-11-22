//! Projection of a confined runtime's discovery output into bounded control metadata.
//!
//! This module is not a discovery fallback for Main or the executor control process.

use meerschaum_server::executor::{plugin_capabilities, plugin_handlers, plugin_host::Placement};
use std::collections::{BTreeMap, BTreeSet};

use meerschaum_server::executor::plugins::{
    Action, Catalog, Expected, McpEntry, Metadata, Page, PageRenderer, Route, Validated,
};
use meerschaum_worker::manifest::Manifest;
use serde_json::Value;

pub fn from_runtime(runtime: &mut crate::dispatch::Runtime<'_>) -> Result<Vec<u8>, String> {
    if std::env::var(meerschaum_server::executor::environment::METADATA_VAR).as_deref() != Ok("1") {
        return Err("Shared plugin metadata requires credential-free discovery.".into());
    }
    let report = meerschaum_core::team_scope::sandbox::current()
        .ok_or("Plugin discovery requires a confined native execution context.")?;
    report.require_sockets_verdict().map_err(
        |_| "Plugin discovery requires mandatory filesystem, socket and signal isolation.",
    )?;
    let value = runtime
        .config()
        .get(&[meerschaum_server::executor::plugins::CONFIG_KEY])
        .ok_or("This execution has no host-selected source catalog.")?;
    let bytes = value.to_json_string().into_bytes();
    let catalog: Catalog =
        serde_json::from_slice(&bytes).map_err(|_| "Invalid host-selected source catalog.")?;
    let grants: BTreeSet<String> = serde_json::from_str(
        &std::env::var("MRSM_EXECUTOR_PLUGINS")
            .map_err(|_| "The producer has no source grants.")?,
    )
    .map_err(|_| "Invalid host-selected source grants.")?;
    let reserved = crate::actions::CORE_ACTIONS
        .iter()
        .map(|action| action.name.to_string())
        .collect();
    let seed = match &catalog.placement {
        Placement::Remote => Validated::decode(
            &bytes,
            Expected {
                team_id: catalog.team_id,
                generation: catalog.generation,
                grants: &grants,
                reserved_actions: &reserved,
            },
        ),
        Placement::Host(host) => Validated::decode_host(&bytes, host, &grants, &reserved),
    }
    .map_err(|error| error.to_string())?;
    produce(&seed, runtime)
}

/// Discover only inside a real native launch which has already proved the mandatory
/// filesystem/socket/signal boundary. Calling this from Main or control refuses before
/// obtaining a Session manifest or loading a Python/native plugin.
pub fn produce(
    seed: &Validated,
    runtime: &mut crate::dispatch::Runtime<'_>,
) -> Result<Vec<u8>, String> {
    if std::env::var(meerschaum_server::executor::environment::METADATA_VAR).as_deref() != Ok("1") {
        return Err("Shared plugin metadata requires credential-free discovery.".into());
    }
    let report = meerschaum_core::team_scope::sandbox::current()
        .ok_or("Plugin discovery requires a confined native execution context.")?;
    report.require_sockets_verdict().map_err(
        |_| "Plugin discovery requires mandatory filesystem, socket and signal isolation.",
    )?;
    let grants: BTreeSet<String> = serde_json::from_str(
        &std::env::var("MRSM_EXECUTOR_PLUGINS")
            .map_err(|_| "The producer has no host-selected source grants.")?,
    )
    .map_err(|_| "Invalid host-selected producer grants.")?;
    if grants != seed.catalog().plugins.keys().cloned().collect() {
        return Err("The producer source grants do not match its catalog.".into());
    }
    meerschaum_core::team_scope::sandbox::deny_network()?;
    let session = crate::native::session(runtime);
    let manifest = session
        .reload_plugins(runtime.config())
        .map_err(|_| "Confined plugin discovery failed.")?;
    let natives = session
        .native_plugins(runtime.config())
        .map_err(|_| "Confined native plugin discovery failed.")?;
    let mut sources = seed.catalog().clone();
    for source in sources.plugins.values_mut() {
        source.metadata = None;
    }
    let pool = session.pool(runtime.config());
    let args = meerschaum_worker::protocol::Msg::new()
        .set(
            "catalog_sha256",
            seed.source_digest().map_err(|error| error.to_string())?,
        )
        .set(
            "catalog_json",
            serde_json::to_string(&sources).map_err(|_| "Invalid source catalog.")?,
        )
        .set(
            "fingerprints_json",
            serde_json::to_string(
                &seed
                    .plugin_fingerprints()
                    .map_err(|error| error.to_string())?,
            )
            .map_err(|_| "Invalid native-computed source fingerprints.")?,
        );
    let reply = session
        .block_on(pool.call(
            "inspect_selected_sources",
            args,
            Vec::new(),
            meerschaum_worker::CallOptions {
                timeout: Some(std::time::Duration::from_secs(20)),
                ..Default::default()
            },
            None,
        ))
        .map_err(|_| "Confined extension observation failed.")?;
    if !reply.frames.is_empty() || !reply.chunks.is_empty() {
        return Err("Extension discovery returned unexpected data frames.".into());
    }
    let observed: Observed =
        serde_json::from_value(meerschaum_worker::protocol::wire_to_json(&reply.value))
            .map_err(|_| "Invalid confined extension observation.")?;
    project_observed(seed, &manifest, natives.manifest(), Some(observed))?
        .encode()
        .map_err(|error| error.to_string())
}

pub(crate) fn mcp(entry: &Value) -> Result<McpEntry, String> {
    let name = entry
        .get("name")
        .and_then(Value::as_str)
        .ok_or("A confined MCP declaration has no name.")?;
    Ok(McpEntry {
        name: name.into(),
        title: entry
            .get("title")
            .and_then(Value::as_str)
            .map(str::to_string),
        mime_type: entry
            .get("mime_type")
            .and_then(Value::as_str)
            .map(str::to_string),
        annotations: entry.get("annotations").cloned().unwrap_or_else(|| {
            let mut result = serde_json::Map::new();
            for (field, key) in [
                ("read_only", "readOnlyHint"),
                ("destructive", "destructiveHint"),
                ("idempotent", "idempotentHint"),
                ("open_world", "openWorldHint"),
            ] {
                if let Some(value) = entry.get(field).and_then(Value::as_bool) {
                    result.insert(key.into(), value.into());
                }
            }
            if result.is_empty() {
                Value::Null
            } else {
                Value::Object(result)
            }
        }),
        arguments: entry
            .get("arguments")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
        description: entry
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .into(),
        scopes: entry
            .get("scopes")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()
            .map_err(|_| "Invalid confined MCP scopes.")?
            .unwrap_or_default(),
        input_schema: entry.get("input_schema").cloned().unwrap_or(Value::Null),
        output_schema: entry.get("output_schema").cloned().unwrap_or(Value::Null),
        uri: entry.get("uri").and_then(Value::as_str).map(str::to_string),
        uri_template: entry
            .get("uri_template")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

/// Pure data projection. The caller must obtain `manifest` and `natives` from the already
/// confined native producer; Main must never call Session discovery to obtain these inputs.
pub fn project(
    seed: &Validated,
    manifest: &Manifest,
    natives: &Value,
) -> Result<Validated, String> {
    project_observed(seed, manifest, natives, None)
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Observed {
    cli_arguments: BTreeMap<String, meerschaum_server::executor::plugins::ArgumentRegistration>,
    registries: BTreeMap<String, plugin_handlers::Registry>,
    capabilities: plugin_capabilities::Capabilities,
}

fn project_observed(
    seed: &Validated,
    manifest: &Manifest,
    natives: &Value,
    observed: Option<Observed>,
) -> Result<Validated, String> {
    let mut catalog: Catalog = seed.catalog().clone();
    let grants: BTreeSet<String> = catalog.plugins.keys().cloned().collect();
    let source_digest = seed.source_digest().map_err(|error| error.to_string())?;
    let fingerprints = seed
        .plugin_fingerprints()
        .map_err(|error| error.to_string())?;
    let source_digests = seed.plugin_digests().map_err(|error| error.to_string())?;
    if let Some(observed) = &observed {
        if observed.registries.keys().cloned().collect::<BTreeSet<_>>() != grants
            || observed
                .cli_arguments
                .keys()
                .cloned()
                .collect::<BTreeSet<_>>()
                != grants
        {
            return Err("Extension observation has another selected source set.".into());
        }
        observed.capabilities.validate(seed)?;
    }
    if manifest.plugins.keys().any(|name| !grants.contains(name)) {
        return Err("Confined discovery reported an ungranted plugin.".into());
    }
    for (name, plugin) in &mut catalog.plugins {
        let loaded = manifest
            .plugins
            .get(name)
            .ok_or("A granted plugin has no confined manifest.")?;
        if loaded.name != *name {
            return Err("A granted plugin manifest reported another source identity.".into());
        }
        if let Some(error) = &loaded.import_error {
            let error: String = error
                .chars()
                .filter(|character| !character.is_control() || matches!(character, '\n' | '\t'))
                .take(2048)
                .collect();
            return Err(format!(
                "Confined metadata import failed for '{name}': {}",
                meerschaum_server::redact::mask_uri_passwords(&error)
            ));
        }
        let native = natives.get("plugins").and_then(|plugins| plugins.get(name));
        // Python route functions and Dash apps must be inspected in a confined extension
        // runtime before advertising their handlers. Do not report an empty successful set.
        if observed.is_none()
            && native.is_none()
            && (!loaded.api_plugins.is_empty() || !loaded.dash_plugins.is_empty())
        {
            return Err("Confined extension route discovery is required for this plugin.".into());
        }
        let mut metadata = Metadata::default();
        use meerschaum_server::executor::plugins::ArgumentRegistration;
        metadata.cli_arguments = match plugin.kind {
            meerschaum_server::executor::plugins::SourceKind::NativeSource
            | meerschaum_server::executor::plugins::SourceKind::NativePackage => Some(match native
                .and_then(|value| value.get("arguments"))
                .and_then(Value::as_array)
            {
                Some(declarations) if declarations.is_empty() => ArgumentRegistration::Absent,
                Some(_) => ArgumentRegistration::Present,
                None => ArgumentRegistration::Unsupported,
            }),
            _ => observed
                .as_ref()
                .and_then(|result| result.cli_arguments.get(name).cloned()),
        };
        metadata.actions = loaded
            .actions
            .iter()
            .map(|action| Action {
                name: action.name.clone(),
                words: action.action_words(),
                description: action.doc.clone().unwrap_or_default(),
            })
            .collect();
        metadata.mcp_tools = loaded
            .mcp_tools
            .iter()
            .map(|entry| McpEntry {
                name: entry.name.clone(),
                title: entry.title.clone(),
                mime_type: None,
                annotations: entry.annotations.clone(),
                arguments: Vec::new(),
                description: entry.description.clone(),
                scopes: entry.scopes.clone(),
                input_schema: entry.input_schema.clone(),
                output_schema: entry.output_schema.clone(),
                uri: None,
                uri_template: None,
            })
            .collect();
        metadata.mcp_resources = loaded
            .mcp_resources
            .iter()
            .map(|entry| McpEntry {
                name: entry.name.clone(),
                title: entry.title.clone(),
                mime_type: entry.mime_type.clone(),
                annotations: Value::Null,
                arguments: Vec::new(),
                description: entry.description.clone(),
                scopes: entry.scopes.clone(),
                input_schema: Value::Null,
                output_schema: Value::Null,
                uri: entry.uri.clone(),
                uri_template: entry.uri_template.clone(),
            })
            .collect();
        metadata.mcp_prompts = loaded
            .mcp_prompts
            .iter()
            .map(|entry| McpEntry {
                name: entry.name.clone(),
                title: entry.title.clone(),
                mime_type: None,
                annotations: Value::Null,
                arguments: entry.arguments.clone(),
                description: entry.description.clone().unwrap_or_default(),
                scopes: entry.scopes.clone(),
                input_schema: Value::Null,
                output_schema: Value::Null,
                uri: None,
                uri_template: None,
            })
            .collect();
        for page in &loaded.web_pages {
            let path = page.path.trim_matches('/');
            let path = path.strip_prefix("dash/").unwrap_or(path);
            let renderer = match page.mode.as_str() {
                "html" => PageRenderer::Html,
                "dash" => PageRenderer::Dash,
                _ => return Err("Unsupported confined plugin page renderer.".into()),
            };
            metadata.pages.push(Page {
                path: path.into(),
                title: page.page_key.clone().unwrap_or_else(|| name.clone()),
                renderer,
            });
        }
        if let Some(native) = native {
            for route in native
                .get("api_routes")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let text = |key| route.get(key).and_then(Value::as_str).map(str::to_string);
                metadata.api_routes.push(Route {
                    name: text("name").ok_or("Invalid confined route name.")?,
                    method: text("method")
                        .unwrap_or_else(|| "GET".into())
                        .to_ascii_uppercase(),
                    path: text("path").ok_or("Invalid confined route path.")?,
                    scopes: route
                        .get("scopes")
                        .map(|value| serde_json::from_value(value.clone()))
                        .transpose()
                        .map_err(|_| "Invalid confined route scopes.")?
                        .unwrap_or_default(),
                });
            }
            // The raw native registry retains API/MCP fields the shared worker subset does
            // not necessarily expose. Project only data, never a callable, path or host scope.
            for (key, target) in [
                ("mcp_tools", &mut metadata.mcp_tools),
                ("mcp_resources", &mut metadata.mcp_resources),
                ("mcp_prompts", &mut metadata.mcp_prompts),
            ] {
                if let Some(entries) = native.get(key).and_then(Value::as_array) {
                    *target = entries.iter().map(mcp).collect::<Result<_, _>>()?;
                }
            }
        }
        if let Some(observed) = &observed {
            let mut registry = observed.registries[name].clone();
            if registry.plugin != *name {
                return Err("Observed registry has another plugin identity.".into());
            }
            registry.validate_source(&source_digest, &fingerprints[name])?;
            if native.is_none() {
                metadata.api_routes = match &registry.api {
                    plugin_handlers::Discovery::Complete { entries } => entries
                        .iter()
                        .map(|entry| {
                            let plugin_handlers::Selector::Api { method, path } = &entry.selector
                            else {
                                return Err("Invalid observed API handler.");
                            };
                            Ok(Route {
                                name: format!("handler_{}", &entry.id[..16]),
                                method: method.clone(),
                                path: path.clone(),
                                scopes: entry.scopes.iter().cloned().collect(),
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => Vec::new(),
                };
                metadata.pages = match &registry.pages {
                    plugin_handlers::Discovery::Complete { entries } => entries
                        .iter()
                        .map(|entry| {
                            let plugin_handlers::Selector::Page { path, renderer } =
                                &entry.selector
                            else {
                                return Err("Invalid observed page handler.");
                            };
                            Ok(Page {
                                path: path.clone(),
                                title: name.clone(),
                                renderer: *renderer,
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    _ => Vec::new(),
                };
            } else {
                // Native declarations are already obtained from the confined native loader.
                // Python cannot invent a successful native registry.
                let entries = metadata
                    .api_routes
                    .iter()
                    .map(|route| {
                        let selector = plugin_handlers::Selector::Api {
                            method: route.method.clone(),
                            path: route.path.clone(),
                        };
                        Ok(plugin_handlers::Handler {
                            dash: None,
                            id: plugin_handlers::handler_id(
                                &source_digest,
                                name,
                                &source_digests[name],
                                &selector,
                            )?,
                            selector,
                            scopes: route.scopes.iter().cloned().collect(),
                        })
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                registry.api = plugin_handlers::Discovery::Complete { entries };
                let mut entries = Vec::new();
                for (group, kind) in [
                    (&metadata.mcp_tools, "tool"),
                    (&metadata.mcp_resources, "resource"),
                    (&metadata.mcp_prompts, "prompt"),
                ] {
                    for entry in group {
                        let selector = match kind {
                            "tool" => plugin_handlers::Selector::McpTool {
                                name: entry.name.clone(),
                            },
                            "resource" => plugin_handlers::Selector::McpResource {
                                name: entry.name.clone(),
                            },
                            _ => plugin_handlers::Selector::McpPrompt {
                                name: entry.name.clone(),
                            },
                        };
                        entries.push(plugin_handlers::Handler {
                            dash: None,
                            id: plugin_handlers::handler_id(
                                &source_digest,
                                name,
                                &source_digests[name],
                                &selector,
                            )?,
                            selector,
                            scopes: entry.scopes.iter().cloned().collect(),
                        });
                    }
                }
                registry.mcp = plugin_handlers::Discovery::Complete { entries };
                registry.pages = plugin_handlers::Discovery::Complete {
                    entries: metadata
                        .pages
                        .iter()
                        .map(|page| {
                            let selector = plugin_handlers::Selector::Page {
                                path: page.path.clone(),
                                renderer: page.renderer,
                            };
                            Ok(plugin_handlers::Handler {
                                dash: None,
                                id: plugin_handlers::handler_id(
                                    &source_digest,
                                    name,
                                    &source_digests[name],
                                    &selector,
                                )?,
                                selector,
                                scopes: BTreeSet::new(),
                            })
                        })
                        .collect::<Result<_, String>>()?,
                };
            }
            metadata.registry = Some(registry);
            metadata.capabilities = observed.capabilities.plugins.get(name).cloned();
        }
        plugin.metadata = Some(metadata);
    }
    let reserved = crate::actions::CORE_ACTIONS
        .iter()
        .map(|action| action.name.to_string())
        .collect();
    let bytes =
        serde_json::to_vec(&catalog).map_err(|_| "Could not encode confined plugin metadata.")?;
    match &catalog.placement {
        Placement::Remote => Validated::decode(
            &bytes,
            Expected {
                team_id: catalog.team_id,
                generation: catalog.generation,
                grants: &grants,
                reserved_actions: &reserved,
            },
        ),
        Placement::Host(host) => Validated::decode_host(&bytes, host, &grants, &reserved),
    }
    .map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_worker::manifest::{ActionManifest, PluginManifest};
    use serde_json::json;

    fn seed() -> Validated {
        let team = uuid::Uuid::new_v4();
        let generation = uuid::Uuid::new_v4();
        let grants = BTreeSet::from(["probe".into()]);
        Validated::decode(&serde_json::to_vec(&json!({
            "schema":1,"release":env!("CARGO_PKG_VERSION"),"team_id":team,"generation":generation,
            "plugins":{"probe":{"archive_sha256":"0".repeat(64),"kind":"python_module","metadata":{}}}
        })).unwrap(),Expected {team_id:team,generation,grants:&grants,reserved_actions:&BTreeSet::new()}).unwrap()
    }

    #[test]
    fn confined_projection_strips_runtime_paths_and_refuses_undiscovered_extensions_and_extra_sources()
     {
        let seed = seed();
        let mut manifest = Manifest::default();
        let mut plugin = PluginManifest {
            name: "probe".into(),
            path: Some("/private/runtime/source.py".into()),
            ..Default::default()
        };
        plugin.actions.push(ActionManifest {
            name: "profile_probe".into(),
            words: vec!["profile".into(), "probe".into()],
            doc: Some("A bounded action.".into()),
            ..Default::default()
        });
        manifest.plugins.insert("probe".into(), plugin);
        let projected = project(&seed, &manifest, &json!({})).unwrap();
        assert_eq!(projected.action_names().unwrap(), ["profile_probe"]);
        assert!(
            !String::from_utf8(projected.encode().unwrap())
                .unwrap()
                .contains("/private")
        );
        manifest
            .plugins
            .get_mut("probe")
            .unwrap()
            .api_plugins
            .push("register_routes".into());
        assert!(
            project(&seed, &manifest, &json!({})).is_err(),
            "Unknown Python route metadata became empty success."
        );
        manifest
            .plugins
            .get_mut("probe")
            .unwrap()
            .api_plugins
            .clear();
        manifest.plugins.insert(
            "other".into(),
            PluginManifest {
                name: "other".into(),
                ..Default::default()
            },
        );
        assert!(project(&seed, &manifest, &json!({})).is_err());
    }

    #[test]
    fn host_projection_keeps_files_identity_and_refuses_forged_registry_content() {
        use meerschaum_server::executor::{plugin_host::HostIdentity, source_ceiling::Fingerprint};
        let host = HostIdentity {
            team_id: None,
            generation: uuid::Uuid::new_v4(),
            files: BTreeMap::from([("probe.py".into(), "a".repeat(64))]),
            view_sha256: "b".repeat(64),
        };
        let grants = BTreeSet::from(["probe".into()]);
        let mut catalog = seed().catalog().clone();
        catalog.team_id = uuid::Uuid::nil();
        catalog.generation = host.generation;
        catalog.placement = Placement::Host(host.clone());
        catalog
            .plugins
            .get_mut("probe")
            .unwrap()
            .archive_sha256
            .clear();
        let seed = Validated::decode_host(
            &serde_json::to_vec(&catalog).unwrap(),
            &host,
            &grants,
            &BTreeSet::new(),
        )
        .unwrap();
        let mut manifest = Manifest::default();
        manifest.plugins.insert(
            "probe".into(),
            PluginManifest {
                name: "probe".into(),
                ..Default::default()
            },
        );
        let digest = seed.source_digest().unwrap();
        let file_digest = seed.plugin_digest("probe").unwrap();
        let selector = plugin_handlers::Selector::Api {
            method: "GET".into(),
            path: "/host-projection-probe".into(),
        };
        let registry: plugin_handlers::Registry = serde_json::from_value(json!({
            "source_catalog_sha256":digest, "plugin":"probe", "files_sha256":file_digest,
            "api":{"state":"complete","entries":[{
                "id":plugin_handlers::handler_id(&digest,"probe",&file_digest,&selector).unwrap(),
                "selector":selector,"scopes":[]
            }]},
            "mcp":{"state":"complete","entries":[]},
            "pages":{"state":"complete","entries":[]},
            "dash_callbacks":{"state":"complete","entries":[]}
        }))
        .unwrap();
        let observed = Observed {
            cli_arguments: BTreeMap::from([(
                "probe".into(),
                meerschaum_server::executor::plugins::ArgumentRegistration::Absent,
            )]),
            registries: BTreeMap::from([("probe".into(), registry.clone())]),
            capabilities: plugin_capabilities::Capabilities {
                source_catalog_sha256: digest,
                plugins: BTreeMap::from([(
                    "probe".into(),
                    plugin_capabilities::Classification::Known {
                        python: true,
                        module_functions: BTreeSet::new(),
                        required: BTreeSet::new(),
                        connector_types: BTreeMap::new(),
                    },
                )]),
            },
        };
        let projected = project_observed(&seed, &manifest, &json!({}), Some(observed)).unwrap();
        assert_eq!(
            projected.source_digest().unwrap(),
            seed.source_digest().unwrap()
        );
        let expected = Fingerprint::Files {
            tree_sha256: file_digest,
        };
        let actual = projected.catalog().plugins["probe"]
            .metadata
            .as_ref()
            .unwrap()
            .registry
            .as_ref()
            .unwrap();
        actual
            .validate_source(&seed.source_digest().unwrap(), &expected)
            .unwrap();
        assert!(actual.archive_sha256.is_empty());
        let mut forged = projected.catalog().clone();
        forged
            .plugins
            .get_mut("probe")
            .unwrap()
            .metadata
            .as_mut()
            .unwrap()
            .registry
            .as_mut()
            .unwrap()
            .files_sha256 = Some("f".repeat(64));
        assert!(
            Validated::decode_host(
                &serde_json::to_vec(&forged).unwrap(),
                &host,
                &grants,
                &BTreeSet::new()
            )
            .is_err()
        );
        assert!(
            Validated::decode(
                &projected.encode().unwrap(),
                Expected {
                    team_id: uuid::Uuid::nil(),
                    generation: host.generation,
                    grants: &grants,
                    reserved_actions: &BTreeSet::new(),
                }
            )
            .is_err()
        );
    }

    #[test]
    fn producer_refuses_an_unconfined_control_process_before_plugin_discovery() {
        let mut output = Vec::new();
        let mut runtime = crate::dispatch::Runtime::system(&mut output);
        assert!(produce(&seed(), &mut runtime).is_err());
        drop(runtime);
        assert!(output.is_empty());
    }
}
