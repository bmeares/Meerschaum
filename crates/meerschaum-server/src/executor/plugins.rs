//! Bounded plugin declarations, never executable discovery in the control process.
//!
//! A catalog is data from a confined producer. Its source digests and the signed current
//! grant set bind its declarations; declarations never replace request authorization.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

pub const SCHEMA: u16 = 1;
/// Framework discovery format, separate from the immutable source catalog schema. Bump when
/// producer observations change so an older incomplete registry cannot satisfy a new cache.
pub const PRODUCER_SCHEMA: u16 = 6;
pub const MAX_CATALOG_BYTES: usize = meerschaum_core::team_scope::api_execution::MAX_METADATA_BYTES;
pub const MAX_PLUGINS: usize = 256;
pub const MAX_ENTRIES: usize = 2048;
/// Host-selected pure metadata carried into a launch, never repository credentials.
pub const CONFIG_KEY: &str = "_executor_plugin_catalog";

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Catalog {
    pub schema: u16,
    pub release: String,
    pub team_id: Uuid,
    pub generation: Uuid,
    /// Host file snapshots have an explicit identity. Remote catalogs keep their wire shape.
    #[serde(
        default,
        skip_serializing_if = "super::plugin_host::Placement::is_remote"
    )]
    pub placement: super::plugin_host::Placement,
    pub plugins: BTreeMap<String, Plugin>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Plugin {
    /// Only remote catalogs claim archive content; Host identities bind exact file trees.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub archive_sha256: String,
    pub kind: SourceKind,
    /// None is a source-only bootstrap descriptor. It must not advertise empty metadata.
    #[serde(default)]
    pub metadata: Option<Metadata>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    PythonModule,
    PythonPackage,
    NativeSource,
    NativePackage,
}

impl SourceKind {
    pub fn entry_name(self, plugin: &str) -> String {
        match self {
            Self::PythonModule => format!("{plugin}.py"),
            Self::NativeSource => format!("{plugin}.rs"),
            Self::PythonPackage | Self::NativePackage => plugin.into(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Metadata {
    /// Confined observation of plugin CLI declarations. Older catalogs supply no evidence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cli_arguments: Option<ArgumentRegistration>,
    /// Missing observations stay unknown; callers cannot infer an empty extension set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry: Option<super::plugin_handlers::Registry>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<super::plugin_capabilities::Classification>,
    #[serde(default)]
    pub actions: Vec<Action>,
    #[serde(default)]
    pub api_routes: Vec<Route>,
    #[serde(default)]
    pub mcp_tools: Vec<McpEntry>,
    #[serde(default)]
    pub mcp_resources: Vec<McpEntry>,
    #[serde(default)]
    pub mcp_prompts: Vec<McpEntry>,
    #[serde(default)]
    pub pages: Vec<Page>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArgumentRegistration {
    Absent,
    Present,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Action {
    pub name: String,
    pub words: Vec<String>,
    #[serde(default)]
    pub description: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Route {
    pub name: String,
    pub method: String,
    pub path: String,
    #[serde(default)]
    pub scopes: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct McpEntry {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub annotations: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub arguments: Vec<serde_json::Value>,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub scopes: Vec<String>,
    #[serde(default)]
    pub input_schema: serde_json::Value,
    #[serde(default)]
    pub output_schema: serde_json::Value,
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(default)]
    pub uri_template: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Page {
    /// A relative plugin page name, never a Main console path or an origin.
    pub path: String,
    pub title: String,
    pub renderer: PageRenderer,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PageRenderer {
    Html,
    Dash,
}

pub struct Expected<'a> {
    pub team_id: Uuid,
    pub generation: Uuid,
    pub grants: &'a BTreeSet<String>,
    /// Core and trusted internal action names cannot be claimed by delivered metadata.
    pub reserved_actions: &'a BTreeSet<String>,
}

/// Constructible only after bounded parsing and exact identity/grant checks.
#[derive(Clone, Debug)]
pub struct Validated(Catalog);

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("Plugin metadata exceeds its limits.")]
    TooLarge,
    #[error("Plugin metadata is invalid or unsupported.")]
    Invalid,
    #[error("Plugin metadata does not match current execution authority.")]
    Stale,
    #[error("Confined plugin metadata discovery has not completed.")]
    Incomplete,
}

pub fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && name
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-'))
        && !name.starts_with(['_', '-'])
}

pub fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
}

pub fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn scopes(values: &[String]) -> Result<(), Error> {
    if values.len() > 64
        || values
            .iter()
            .any(|value| !meerschaum_core::access::ALL_PERMISSIONS.contains(&value.as_str()))
        || values.iter().collect::<BTreeSet<_>>().len() != values.len()
    {
        return Err(Error::Invalid);
    }
    Ok(())
}

fn data_limits(value: &serde_json::Value, depth: usize, nodes: &mut usize) -> Result<(), Error> {
    *nodes += 1;
    if depth > 24 || *nodes > 50_000 {
        return Err(Error::TooLarge);
    }
    let text = |value: &str| {
        if value.len() > 16 * 1024
            || value
                .chars()
                .any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t'))
        {
            Err(Error::Invalid)
        } else {
            Ok(())
        }
    };
    match value {
        serde_json::Value::String(value) => text(value)?,
        serde_json::Value::Array(values) => {
            for value in values {
                data_limits(value, depth + 1, nodes)?;
            }
        }
        serde_json::Value::Object(values) => {
            for (key, value) in values {
                text(key)?;
                data_limits(value, depth + 1, nodes)?;
            }
        }
        _ => {}
    }
    Ok(())
}

impl Validated {
    pub fn decode(bytes: &[u8], expected: Expected<'_>) -> Result<Self, Error> {
        Self::decode_for(bytes, expected, None)
    }

    /// Parse a Main-created Host file catalog without fabricating remote registration.
    pub fn decode_host(
        bytes: &[u8],
        host: &super::plugin_host::HostIdentity,
        grants: &BTreeSet<String>,
        reserved_actions: &BTreeSet<String>,
    ) -> Result<Self, Error> {
        Self::decode_for(
            bytes,
            Expected {
                team_id: host.team_id.unwrap_or_else(Uuid::nil),
                generation: host.generation,
                grants,
                reserved_actions,
            },
            Some(host),
        )
    }

    fn decode_for(
        bytes: &[u8],
        expected: Expected<'_>,
        host: Option<&super::plugin_host::HostIdentity>,
    ) -> Result<Self, Error> {
        if bytes.len() > MAX_CATALOG_BYTES {
            return Err(Error::TooLarge);
        }
        let value: serde_json::Value = serde_json::from_slice(bytes).map_err(|_| Error::Invalid)?;
        data_limits(&value, 0, &mut 0)?;
        let catalog: Catalog = serde_json::from_value(value).map_err(|_| Error::Invalid)?;
        if catalog.schema != SCHEMA || catalog.release != env!("CARGO_PKG_VERSION") {
            return Err(Error::Invalid);
        }
        match (host, &catalog.placement) {
            (None, super::plugin_host::Placement::Remote) if !expected.team_id.is_nil() => {}
            (Some(expected), super::plugin_host::Placement::Host(actual)) if expected == actual => {
                actual
                    .validate_catalog(catalog.team_id, catalog.generation, &catalog.plugins)
                    .map_err(|_| Error::Stale)?;
            }
            _ => return Err(Error::Stale),
        }
        if expected.generation.is_nil()
            || catalog.team_id != expected.team_id
            || catalog.generation != expected.generation
            || catalog.plugins.keys().cloned().collect::<BTreeSet<_>>() != *expected.grants
        {
            return Err(Error::Stale);
        }
        if catalog.plugins.len() > MAX_PLUGINS {
            return Err(Error::TooLarge);
        }
        let mut entries = 0;
        let mut actions = BTreeSet::new();
        let source_digest = Self(catalog.clone()).source_digest()?;
        let fingerprints = super::source_ceiling::Ceiling::from_catalog(&catalog)
            .map_err(|_| Error::Invalid)?
            .sources;
        for (name, plugin) in &catalog.plugins {
            if !valid_name(name) || (host.is_none() && !valid_digest(&plugin.archive_sha256)) {
                return Err(Error::Invalid);
            }
            let Some(metadata) = &plugin.metadata else {
                continue;
            };
            if let Some(registry) = &metadata.registry {
                if registry.plugin != *name {
                    return Err(Error::Invalid);
                }
                registry
                    .validate_source(
                        &source_digest,
                        fingerprints.get(name).ok_or(Error::Invalid)?,
                    )
                    .map_err(|_| Error::Invalid)?;
                for handler in registry.handlers() {
                    use super::plugin_handlers::Selector;
                    let declared_scopes = match &handler.selector {
                        Selector::Api { method, path } => metadata
                            .api_routes
                            .iter()
                            .find(|entry| &entry.method == method && &entry.path == path)
                            .map(|entry| entry.scopes.iter().cloned().collect::<BTreeSet<_>>()),
                        Selector::McpTool { name } => metadata
                            .mcp_tools
                            .iter()
                            .find(|entry| &entry.name == name)
                            .map(|entry| entry.scopes.iter().cloned().collect()),
                        Selector::McpResource { name } => metadata
                            .mcp_resources
                            .iter()
                            .find(|entry| &entry.name == name)
                            .map(|entry| entry.scopes.iter().cloned().collect()),
                        Selector::McpPrompt { name } => metadata
                            .mcp_prompts
                            .iter()
                            .find(|entry| &entry.name == name)
                            .map(|entry| entry.scopes.iter().cloned().collect()),
                        Selector::Page { path, renderer } => metadata
                            .pages
                            .iter()
                            .find(|entry| &entry.path == path && &entry.renderer == renderer)
                            .map(|_| BTreeSet::new()),
                        Selector::DashCallback { .. } => Some(handler.scopes.clone()),
                    };
                    if declared_scopes.as_ref() != Some(&handler.scopes) {
                        return Err(Error::Invalid);
                    }
                }
                entries += registry.handlers().count();
            }
            entries += metadata.actions.len()
                + metadata.api_routes.len()
                + metadata.mcp_tools.len()
                + metadata.mcp_resources.len()
                + metadata.mcp_prompts.len()
                + metadata.pages.len();
            if entries > MAX_ENTRIES {
                return Err(Error::TooLarge);
            }
            for action in &metadata.actions {
                if !valid_name(&action.name)
                    || action.words.is_empty()
                    || action.words.len() > 8
                    || action.words.iter().any(|word| !valid_name(word))
                    || action.words.join("_") != action.name
                    || expected.reserved_actions.contains(&action.words[0])
                    || !actions.insert(action.words.clone())
                {
                    return Err(Error::Invalid);
                }
            }
            let mut routes = BTreeSet::new();
            for route in &metadata.api_routes {
                if !valid_name(&route.name)
                    || !matches!(
                        route.method.as_str(),
                        "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE" | "OPTIONS"
                    )
                    || crate::native::check_route_path(&route.path).is_err()
                    || !routes.insert((&route.method, &route.path))
                {
                    return Err(Error::Invalid);
                }
                scopes(&route.scopes)?;
            }
            for group in [
                &metadata.mcp_tools,
                &metadata.mcp_resources,
                &metadata.mcp_prompts,
            ] {
                let mut names = BTreeSet::new();
                for entry in group {
                    if !valid_name(&entry.name) || !names.insert(&entry.name) {
                        return Err(Error::Invalid);
                    }
                    scopes(&entry.scopes)?;
                    if entry.arguments.len() > 64
                        || entry.arguments.iter().any(|argument| {
                            let Some(fields) = argument.as_object() else {
                                return true;
                            };
                            fields.keys().any(|key| {
                                !matches!(key.as_str(), "name" | "description" | "required")
                            }) || !fields
                                .get("name")
                                .and_then(serde_json::Value::as_str)
                                .is_some_and(valid_name)
                                || fields
                                    .get("description")
                                    .is_some_and(|value| !value.is_null() && !value.is_string())
                                || fields
                                    .get("required")
                                    .is_some_and(|value| !value.is_boolean())
                        })
                    {
                        return Err(Error::Invalid);
                    }
                }
            }
            let mut pages = BTreeSet::new();
            for page in &metadata.pages {
                if page.path.len() > 512
                    || page.path.split('/').any(|part| !valid_name(part))
                    || !pages.insert(&page.path)
                    || page.title.is_empty()
                {
                    return Err(Error::Invalid);
                }
            }
        }
        let validated = Self(catalog);
        let classifications = validated
            .0
            .plugins
            .iter()
            .filter_map(|(name, plugin)| {
                Some((
                    name.clone(),
                    plugin.metadata.as_ref()?.capabilities.clone()?,
                ))
            })
            .collect::<std::collections::BTreeMap<_, _>>();
        if !classifications.is_empty() {
            super::plugin_capabilities::Capabilities {
                source_catalog_sha256: source_digest,
                plugins: classifications,
            }
            .validate(&validated)
            .map_err(|_| Error::Invalid)?;
        }
        Ok(validated)
    }

    pub fn catalog(&self) -> &Catalog {
        &self.0
    }

    /// Main-computed source identities for fixed confined worker operations.
    pub fn plugin_fingerprints(
        &self,
    ) -> Result<BTreeMap<String, super::source_ceiling::Fingerprint>, Error> {
        Ok(super::source_ceiling::Ceiling::from_catalog(&self.0)
            .map_err(|_| Error::Invalid)?
            .sources)
    }

    /// Remote archive or domain-separated Host file-tree content identities, never cache keys.
    pub fn plugin_digests(&self) -> Result<BTreeMap<String, String>, Error> {
        self.plugin_fingerprints().map(|sources| {
            sources
                .into_iter()
                .map(|(name, source)| {
                    let digest = match source {
                        super::source_ceiling::Fingerprint::Archive { archive_sha256, .. } => {
                            archive_sha256
                        }
                        super::source_ceiling::Fingerprint::Files { tree_sha256 } => tree_sha256,
                    };
                    (name, digest)
                })
                .collect()
        })
    }

    /// The exact selected plugin's generic content digest; an absent source fails closed.
    pub fn plugin_digest(&self, name: &str) -> Result<String, Error> {
        self.plugin_digests()?.remove(name).ok_or(Error::Stale)
    }

    pub fn encode(&self) -> Result<Vec<u8>, Error> {
        let bytes = serde_json::to_vec(&self.0).map_err(|_| Error::Invalid)?;
        if bytes.len() > MAX_CATALOG_BYTES {
            return Err(Error::TooLarge);
        }
        Ok(bytes)
    }

    pub fn digest(&self) -> Result<String, Error> {
        Ok(sha256(&self.encode()?))
    }

    /// Stable whole-source identity. Discovered data never participates in handler IDs.
    pub fn source_digest(&self) -> Result<String, Error> {
        let mut sources = self.0.clone();
        for plugin in sources.plugins.values_mut() {
            plugin.metadata = None;
        }
        let bytes = serde_json::to_vec(&sources).map_err(|_| Error::Invalid)?;
        if bytes.len() > MAX_CATALOG_BYTES {
            return Err(Error::TooLarge);
        }
        Ok(sha256(&bytes))
    }

    pub fn complete(&self) -> bool {
        self.0
            .plugins
            .values()
            .all(|plugin| plugin.metadata.is_some())
    }

    pub fn action_names(&self) -> Result<Vec<String>, Error> {
        if !self.complete() {
            return Err(Error::Incomplete);
        }
        Ok(self
            .0
            .plugins
            .values()
            .filter_map(|plugin| plugin.metadata.as_ref())
            .flat_map(|metadata| metadata.actions.iter().map(|action| action.name.clone()))
            .collect())
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn missing_plugin_argument_evidence_remains_unknown() {
        let legacy: super::Metadata = serde_json::from_str("{}").unwrap();
        assert!(legacy.cli_arguments.is_none());
        let present: super::Metadata =
            serde_json::from_str(r#"{"cli_arguments":"present"}"#).unwrap();
        assert_eq!(
            present.cli_arguments,
            Some(super::ArgumentRegistration::Present)
        );
        let absent: super::Metadata =
            serde_json::from_str(r#"{"cli_arguments":"absent"}"#).unwrap();
        assert_eq!(
            absent.cli_arguments,
            Some(super::ArgumentRegistration::Absent)
        );
    }

    use super::*;
    use serde_json::json;

    #[test]
    fn explicit_host_support_does_not_change_remote_catalog_bytes_or_hashes() {
        #[derive(Serialize)]
        struct RemoteCatalog<'a> {
            schema: u16,
            release: &'a str,
            team_id: Uuid,
            generation: Uuid,
            plugins: &'a BTreeMap<String, Plugin>,
        }
        let team_id = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let mut value = fixture(team_id, generation);
        value["plugins"]["probe"]["metadata"] = serde_json::Value::Null;
        let grants = BTreeSet::from(["probe".into()]);
        let validated = Validated::decode(
            &serde_json::to_vec(&value).unwrap(),
            Expected {
                team_id,
                generation,
                grants: &grants,
                reserved_actions: &BTreeSet::new(),
            },
        )
        .unwrap();
        let legacy_bytes = serde_json::to_vec(&RemoteCatalog {
            schema: SCHEMA,
            release: env!("CARGO_PKG_VERSION"),
            team_id,
            generation,
            plugins: &validated.catalog().plugins,
        })
        .unwrap();
        assert_eq!(validated.encode().unwrap(), legacy_bytes);
        assert_eq!(validated.digest().unwrap(), sha256(&legacy_bytes));
        assert_eq!(validated.source_digest().unwrap(), sha256(&legacy_bytes));
        assert_eq!(validated.plugin_digest("probe").unwrap(), "0".repeat(64));
        let mut no_archive = value;
        no_archive["plugins"]["probe"]
            .as_object_mut()
            .unwrap()
            .remove("archive_sha256");
        assert!(
            Validated::decode(
                &serde_json::to_vec(&no_archive).unwrap(),
                Expected {
                    team_id,
                    generation,
                    grants: &grants,
                    reserved_actions: &BTreeSet::new(),
                }
            )
            .is_err()
        );
    }

    fn fixture(team_id: Uuid, generation: Uuid) -> serde_json::Value {
        json!({
            "schema": SCHEMA, "release": env!("CARGO_PKG_VERSION"), "team_id":team_id,
            "generation":generation, "plugins": {
                "probe": {"archive_sha256":"0".repeat(64),"kind":"python_module","metadata": {
                    "actions":[{"name":"profile_probe","words":["profile","probe"],"description":"A confined action."}],
                    "api_routes":[{"name":"status","method":"GET","path":"/probe/status","scopes":["pipes:read"]}],
                    "mcp_tools":[{"name":"probe","description":"Current result.","scopes":["pipes:read"],"input_schema":{"type":"object"}}],
                    "pages":[{"path":"overview","title":"Probe","renderer":"html"}]
                }}
            }
        })
    }

    #[test]
    fn inert_catalog_binds_exact_release_team_generation_grants_and_preserves_metadata() {
        let team = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let grants = BTreeSet::from(["probe".into()]);
        let reserved = BTreeSet::from(["show".into(), "start".into()]);
        let expected = || Expected {
            team_id: team,
            generation,
            grants: &grants,
            reserved_actions: &reserved,
        };
        let bytes = serde_json::to_vec(&fixture(team, generation)).unwrap();
        let catalog = Validated::decode(&bytes, expected()).unwrap();
        assert_eq!(catalog.action_names().unwrap(), ["profile_probe"]);
        assert_eq!(
            catalog.catalog().plugins["probe"]
                .metadata
                .as_ref()
                .unwrap()
                .pages[0]
                .title,
            "Probe"
        );
        assert!(valid_digest(&catalog.digest().unwrap()));
        let mut source_only = fixture(team, generation);
        source_only["plugins"]["probe"]["metadata"] = serde_json::Value::Null;
        let source_only =
            Validated::decode(&serde_json::to_vec(&source_only).unwrap(), expected()).unwrap();
        assert!(!source_only.complete());
        assert!(matches!(source_only.action_names(), Err(Error::Incomplete)));
        assert_ne!(source_only.digest().unwrap(), catalog.digest().unwrap());
        assert_eq!(
            catalog.digest().unwrap(),
            Validated::decode(&catalog.encode().unwrap(), expected())
                .unwrap()
                .digest()
                .unwrap()
        );
        let wrong = Expected {
            team_id: Uuid::new_v4(),
            ..expected()
        };
        assert!(matches!(
            Validated::decode(&bytes, wrong),
            Err(Error::Stale)
        ));
        let none = BTreeSet::new();
        assert!(matches!(
            Validated::decode(
                &bytes,
                Expected {
                    grants: &none,
                    ..expected()
                }
            ),
            Err(Error::Stale)
        ));
        let wrong = Expected {
            generation: Uuid::new_v4(),
            ..expected()
        };
        assert!(matches!(
            Validated::decode(&bytes, wrong),
            Err(Error::Stale)
        ));
    }

    #[test]
    fn delivered_metadata_cannot_select_core_routes_paths_extra_grants_or_unbounded_data() {
        let team = Uuid::new_v4();
        let generation = Uuid::new_v4();
        let grants = BTreeSet::from(["probe".into()]);
        let reserved = BTreeSet::from(["show".into(), "start".into()]);
        let decode = |value: &serde_json::Value| {
            Validated::decode(
                &serde_json::to_vec(value).unwrap(),
                Expected {
                    team_id: team,
                    generation,
                    grants: &grants,
                    reserved_actions: &reserved,
                },
            )
        };
        for (pointer, replacement) in [
            ("/release", json!("other-release")),
            ("/plugins/probe/archive_sha256", json!("../outside")),
            (
                "/plugins/probe/metadata/actions/0/name",
                json!("show_version"),
            ),
            (
                "/plugins/probe/metadata/actions/0/words",
                json!(["show", "version"]),
            ),
            ("/plugins/probe/metadata/api_routes/0/path", json!("/login")),
            ("/plugins/probe/metadata/api_routes/0/method", json!("*")),
            ("/plugins/probe/metadata/api_routes/0/scopes", json!(["*"])),
            (
                "/plugins/probe/metadata/pages/0/path",
                json!("https://main.invalid/dash"),
            ),
            (
                "/plugins/probe/metadata/actions/0/description",
                json!("\u{1b}]52;clipboard"),
            ),
        ] {
            let mut value = fixture(team, generation);
            *value.pointer_mut(pointer).unwrap() = replacement;
            assert!(decode(&value).is_err(), "Accepted {pointer}");
        }
        let mut value = fixture(team, generation);
        value["plugins"]["probe"]["path"] = json!("/main/credentials");
        assert!(decode(&value).is_err());
        let mut value = fixture(team, generation);
        value["plugins"]["probe"]["metadata"]["actions"][0]["description"] =
            json!("x".repeat(16 * 1024 + 1));
        assert!(decode(&value).is_err());
        assert!(matches!(
            Validated::decode(
                &vec![b' '; MAX_CATALOG_BYTES + 1],
                Expected {
                    team_id: team,
                    generation,
                    grants: &grants,
                    reserved_actions: &reserved,
                }
            ),
            Err(Error::TooLarge)
        ));
    }
}
