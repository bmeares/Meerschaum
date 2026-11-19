//! Typed extension declarations. No module or attribute lookup is accepted from a caller.
//! A declaration is data, never route authority.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::plugins;

const MAX_INPUT_BYTES: usize = 1024 * 1024;
const MAX_RESULT_BYTES: usize = 2 * 1024 * 1024;
const MAX_HEADERS: usize = 8;

/// Missing and unsupported discovery never advertise an empty successful registry.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum Discovery<T> {
    #[default]
    Missing,
    Unsupported {
        feature: String,
    },
    Complete {
        entries: T,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Selector {
    Api {
        method: String,
        path: String,
    },
    McpTool {
        name: String,
    },
    McpResource {
        name: String,
    },
    McpPrompt {
        name: String,
    },
    Page {
        path: String,
        renderer: plugins::PageRenderer,
    },
    DashCallback {
        output: String,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Handler {
    pub id: String,
    pub selector: Selector,
    pub scopes: BTreeSet<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dash: Option<DashBindings>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Binding {
    pub id: String,
    pub property: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DashBindings {
    pub inputs: Vec<Binding>,
    pub state: Vec<Binding>,
    pub outputs: Vec<Binding>,
}

impl DashBindings {
    fn validate(&self) -> Result<(), String> {
        if self.outputs.is_empty()
            || self.outputs.len() > 64
            || self.inputs.len() > 128
            || self.state.len() > 128
        {
            return Err("Dash bindings exceed their limits.".into());
        }
        for group in [&self.inputs, &self.state, &self.outputs] {
            let mut unique = BTreeSet::new();
            for binding in group {
                if binding.id.is_empty()
                    || binding.id.len() > 256
                    || binding.id.chars().any(|c| c.is_control())
                    || !plugins::valid_name(&binding.property)
                    || !unique.insert((&binding.id, &binding.property))
                {
                    return Err(
                        "Pattern, duplicate or invalid Dash bindings are unsupported.".into(),
                    );
                }
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registry {
    pub source_catalog_sha256: String,
    pub plugin: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub archive_sha256: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub files_sha256: Option<String>,
    pub api: Discovery<Vec<Handler>>,
    #[serde(default)]
    pub mcp: Discovery<Vec<Handler>>,
    pub pages: Discovery<Vec<Handler>>,
    pub dash_callbacks: Discovery<Vec<Handler>>,
}

/// IDs bind the entire admitted derivation set as well as the declared source and selector.
/// Reusing a narrower set cannot retain another plugin's registry mutations.
pub fn handler_id(
    catalog_sha256: &str,
    plugin: &str,
    source_sha256: &str,
    selector: &Selector,
) -> Result<String, String> {
    if !plugins::valid_digest(catalog_sha256)
        || !plugins::valid_name(plugin)
        || !plugins::valid_digest(source_sha256)
    {
        return Err("Invalid extension source binding.".into());
    }
    let mut bytes = b"mrsm-extension-handler-v1\0".to_vec();
    bytes.extend(
        serde_json::to_vec(&(catalog_sha256, plugin, source_sha256, selector))
            .map_err(|_| "Invalid typed extension selector.")?,
    );
    Ok(plugins::sha256(&bytes))
}

impl Registry {
    /// A handler has exactly one explicit content identity. Files are never fake archives.
    pub fn content_digest(&self) -> Result<&str, String> {
        match (self.archive_sha256.as_str(), self.files_sha256.as_deref()) {
            (archive, None) if plugins::valid_digest(archive) => Ok(archive),
            ("", Some(files)) if plugins::valid_digest(files) => Ok(files),
            _ => Err("The extension registry requires one exact source identity.".into()),
        }
    }

    /// Compatibility entry point for an ordinary registered executor archive.
    pub fn validate(&self, catalog_sha256: &str, source: &plugins::Plugin) -> Result<(), String> {
        self.validate_source(
            catalog_sha256,
            &super::source_ceiling::Fingerprint::Archive {
                source_kind: source.kind,
                archive_sha256: source.archive_sha256.clone(),
            },
        )
    }

    /// The expected kind and digest must come from Main's validated whole-source catalog.
    pub fn validate_source(
        &self,
        catalog_sha256: &str,
        source: &super::source_ceiling::Fingerprint,
    ) -> Result<(), String> {
        let content_digest = self.content_digest()?;
        let bound = match source {
            super::source_ceiling::Fingerprint::Archive { archive_sha256, .. } => {
                self.files_sha256.is_none() && &self.archive_sha256 == archive_sha256
            }
            super::source_ceiling::Fingerprint::Files { tree_sha256 } => {
                self.archive_sha256.is_empty() && self.files_sha256.as_ref() == Some(tree_sha256)
            }
        };
        if self.source_catalog_sha256 != catalog_sha256
            || !plugins::valid_name(&self.plugin)
            || !bound
        {
            return Err("The extension registry has another source.".into());
        }
        let mut ids = BTreeSet::new();
        for (kind, state) in [
            ("api", &self.api),
            ("mcp", &self.mcp),
            ("page", &self.pages),
            ("dash_callback", &self.dash_callbacks),
        ] {
            let entries = match state {
                Discovery::Complete { entries } => entries,
                Discovery::Missing => continue,
                Discovery::Unsupported { feature } => {
                    if feature.is_empty()
                        || feature.len() > 256
                        || feature.chars().any(char::is_control)
                    {
                        return Err("Invalid unsupported extension marker.".into());
                    }
                    continue;
                }
            };
            if entries.len() > plugins::MAX_ENTRIES {
                return Err("Too many extension handlers.".into());
            }
            for handler in entries {
                match (&handler.selector, &handler.dash) {
                    (Selector::DashCallback { .. }, Some(bindings)) => bindings.validate()?,
                    (Selector::DashCallback { .. }, None) => {
                        return Err("Dash callbacks require observed exact bindings.".into());
                    }
                    (_, Some(_)) => {
                        return Err("Another handler kind cannot carry Dash bindings.".into());
                    }
                    _ => {}
                }
                let valid_kind = matches!(
                    (kind, &handler.selector),
                    ("api", Selector::Api { .. })
                        | (
                            "mcp",
                            Selector::McpTool { .. }
                                | Selector::McpResource { .. }
                                | Selector::McpPrompt { .. }
                        )
                        | ("page", Selector::Page { .. })
                        | ("dash_callback", Selector::DashCallback { .. })
                );
                if !valid_kind
                    || handler.id
                        != handler_id(
                            catalog_sha256,
                            &self.plugin,
                            content_digest,
                            &handler.selector,
                        )?
                    || !ids.insert(&handler.id)
                    || handler.scopes.iter().any(|scope| {
                        !meerschaum_core::access::ALL_PERMISSIONS.contains(&scope.as_str())
                    })
                {
                    return Err("Invalid or duplicate declared extension handler.".into());
                }
                match &handler.selector {
                    Selector::Api { method, path }
                        if matches!(
                            method.as_str(),
                            "GET" | "HEAD" | "POST" | "PUT" | "PATCH" | "DELETE" | "OPTIONS"
                        ) && crate::native::check_route_path(path).is_ok() => {}
                    Selector::Page { path, .. }
                        if path.len() <= 512 && path.split('/').all(plugins::valid_name) => {}
                    Selector::McpTool { name }
                    | Selector::McpResource { name }
                    | Selector::McpPrompt { name }
                        if plugins::valid_name(name) => {}
                    Selector::DashCallback { output }
                        if !output.is_empty()
                            && output.len() <= 4096
                            && !output.chars().any(char::is_control) => {}
                    _ => return Err("Unsupported declared extension selector.".into()),
                }
            }
        }
        Ok(())
    }

    /// Only positively discovered declarations can be selected for invocation.
    pub fn handlers(&self) -> impl Iterator<Item = &Handler> {
        [&self.api, &self.mcp, &self.pages, &self.dash_callbacks]
            .into_iter()
            .flat_map(|state| match state {
                Discovery::Complete { entries } => entries.as_slice(),
                _ => &[],
            })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Api {
        method: String,
        path: String,
        query: Vec<(String, String)>,
        headers: BTreeMap<String, String>,
        body: String,
    },
    Mcp {
        arguments: Value,
    },
    Page {
        query: Vec<(String, String)>,
    },
    DashCallback {
        inputs: Value,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Invocation {
    pub catalog_sha256: String,
    pub plugin: String,
    pub handler_id: String,
    pub request: Request,
}

impl Invocation {
    pub fn validate(
        &self,
        expected_catalog_sha256: &str,
        registry: &Registry,
        handler: &Handler,
    ) -> Result<(), String> {
        let bytes = serde_json::to_vec(self).map_err(|_| "Invalid extension request.")?;
        if bytes.len() > MAX_INPUT_BYTES
            || !plugins::valid_digest(&self.catalog_sha256)
            || !plugins::valid_name(&self.plugin)
            || self.catalog_sha256 != expected_catalog_sha256
            || self.plugin != registry.plugin
            || self.handler_id != handler.id
            || self.handler_id
                != handler_id(
                    expected_catalog_sha256,
                    &registry.plugin,
                    registry.content_digest()?,
                    &handler.selector,
                )?
        {
            return Err("Invalid extension invocation binding or size.".into());
        }
        match (&handler.selector, &self.request) {
            (
                Selector::Api {
                    method,
                    path: template,
                },
                Request::Api {
                    method: actual,
                    path,
                    query,
                    headers,
                    ..
                },
            ) => {
                let parameter_count = template
                    .chars()
                    .filter(|character| *character == '{')
                    .count();
                let matches = if parameter_count == 0 {
                    path == template
                } else {
                    crate::native::path_params(template, path).len() == parameter_count
                };
                if method != actual
                    || !matches
                    || path.len() > 4096
                    || path.contains(['#', '\\'])
                    || query.len() > 256
                    || headers.len() > MAX_HEADERS
                {
                    return Err("Invalid declared API request.".into());
                }
                if headers.iter().any(|(name, value)| {
                    !matches!(name.as_str(), "accept" | "content-type")
                        || value.len() > 1024
                        || value.chars().any(char::is_control)
                }) {
                    return Err("Caller credentials and routing headers cannot cross the extension boundary.".into());
                }
            }
            (
                Selector::McpTool { .. }
                | Selector::McpResource { .. }
                | Selector::McpPrompt { .. },
                Request::Mcp { .. },
            ) => {}
            (Selector::Page { .. }, Request::Page { query }) if query.len() <= 256 => {}
            (Selector::DashCallback { output }, Request::DashCallback { inputs }) => {
                let bindings = handler
                    .dash
                    .as_ref()
                    .ok_or("Dash bindings are unavailable.")?;
                let value = inputs.as_object().ok_or("Invalid Dash callback inputs.")?;
                if value.keys().any(|key| {
                    !matches!(
                        key.as_str(),
                        "output" | "outputs" | "changedPropIds" | "inputs" | "state"
                    )
                }) || value.get("output").and_then(Value::as_str) != Some(output.as_str())
                {
                    return Err("A Dash callback cannot select another output.".into());
                }
                for (field, expected) in [("inputs", &bindings.inputs), ("state", &bindings.state)]
                {
                    let observed = value
                        .get(field)
                        .and_then(Value::as_array)
                        .ok_or("Missing Dash input bindings.")?;
                    if observed.len() != expected.len()
                        || observed.iter().zip(expected).any(|(value, expected)| {
                            value.as_object().is_none_or(|fields| {
                                fields.len() != 3 || !fields.contains_key("value")
                            }) || value.get("id").and_then(Value::as_str)
                                != Some(expected.id.as_str())
                                || value.get("property").and_then(Value::as_str)
                                    != Some(expected.property.as_str())
                        })
                    {
                        return Err("The Dash request has another declared input/state set.".into());
                    }
                }
                let outputs = serde_json::to_value(&bindings.outputs)
                    .map_err(|_| "Invalid Dash output binding.")?;
                let expected = if bindings.outputs.len() == 1 {
                    &outputs[0]
                } else {
                    &outputs
                };
                if value.get("outputs") != Some(expected) {
                    return Err("The Dash request has another output binding.".into());
                }
                let changed = value
                    .get("changedPropIds")
                    .and_then(Value::as_array)
                    .ok_or("Invalid changed Dash properties.")?;
                if changed.len() > bindings.inputs.len()
                    || changed.iter().any(|value| {
                        value.as_str().is_none_or(|changed| {
                            !bindings.inputs.iter().any(|binding| {
                                changed == format!("{}.{}", binding.id, binding.property)
                            })
                        })
                    })
                {
                    return Err("A Dash request changed an undeclared property.".into());
                }
            }
            _ => return Err("The request does not match its declared handler kind.".into()),
        }
        Ok(())
    }
}

/// No Set-Cookie, Location, authorization header or executor-selected origin is representable.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reply {
    Json {
        value: Value,
    },
    Api {
        status: u16,
        body: String,
    },
    Page {
        fragment: String,
        #[serde(default = "page_success")]
        status: u16,
    },
    DashCallback {
        updates: Value,
    },
}
fn page_success() -> u16 {
    200
}
impl Reply {
    pub fn validate(&self, selector: &Selector) -> Result<(), String> {
        if serde_json::to_vec(self)
            .map_err(|_| "Invalid extension response.")?
            .len()
            > MAX_RESULT_BYTES
        {
            return Err("The extension response exceeds its bound.".into());
        }
        match (selector, self) {
            (Selector::Api { .. }, Self::Api { status, .. }) if (200..=599).contains(status) => {
                Ok(())
            }
            (
                Selector::McpTool { .. }
                | Selector::McpResource { .. }
                | Selector::McpPrompt { .. },
                Self::Json { .. },
            ) => Ok(()),
            (Selector::Page { .. }, Self::Page { status, .. }) if (200..=599).contains(status) => {
                Ok(())
            }
            (Selector::DashCallback { .. }, Self::DashCallback { .. }) => Ok(()),
            _ => Err("The response does not match its declared handler kind.".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn registry(catalog: &str) -> Registry {
        let selector = Selector::Api {
            method: "POST".into(),
            path: "/extension-probe/{item}".into(),
        };
        Registry {
            source_catalog_sha256: catalog.into(),
            plugin: "probe".into(),
            archive_sha256: "b".repeat(64),
            files_sha256: None,
            api: Discovery::Complete {
                entries: vec![Handler {
                    id: handler_id(catalog, "probe", &"b".repeat(64), &selector).unwrap(),
                    selector,
                    scopes: BTreeSet::from(["pipes:read".into()]),
                    dash: None,
                }],
            },
            mcp: Discovery::Complete {
                entries: Vec::new(),
            },
            pages: Discovery::Complete {
                entries: Vec::new(),
            },
            dash_callbacks: Discovery::Complete {
                entries: Vec::new(),
            },
        }
    }
    #[test]
    fn source_set_and_registry_completeness_remain_bound_to_handler_ids() {
        let catalog = "a".repeat(64);
        let mut registry = registry(&catalog);
        let source = plugins::Plugin {
            archive_sha256: "b".repeat(64),
            kind: plugins::SourceKind::PythonModule,
            metadata: None,
        };
        registry.validate(&catalog, &source).unwrap();
        assert!(registry.validate(&"c".repeat(64), &source).is_err());
        registry.pages = Discovery::Missing;
        registry.validate(&catalog, &source).unwrap();
        assert_eq!(registry.handlers().count(), 1);
        registry.pages = Discovery::Unsupported {
            feature: "page_credential_free_render_failed".into(),
        };
        registry.validate(&catalog, &source).unwrap();
        assert_eq!(registry.handlers().count(), 1);
    }
    #[test]
    fn host_file_registry_cannot_be_relabelled_as_an_executor_archive_even_with_equal_digests() {
        let catalog = "a".repeat(64);
        let mut registry = registry(&catalog);
        let archive = super::super::source_ceiling::Fingerprint::Archive {
            source_kind: plugins::SourceKind::PythonModule,
            archive_sha256: "b".repeat(64),
        };
        let files = super::super::source_ceiling::Fingerprint::Files {
            tree_sha256: "b".repeat(64),
        };
        registry.validate_source(&catalog, &archive).unwrap();
        assert!(registry.validate_source(&catalog, &files).is_err());
        let remote = serde_json::to_value(&registry).unwrap();
        assert!(remote.get("files_sha256").is_none());
        registry.archive_sha256.clear();
        registry.files_sha256 = Some("b".repeat(64));
        registry.validate_source(&catalog, &files).unwrap();
        assert!(registry.validate_source(&catalog, &archive).is_err());
        let encoded = serde_json::to_value(&registry).unwrap();
        assert!(encoded.get("archive_sha256").is_none());
        let handler = registry.handlers().next().unwrap();
        Invocation {
            catalog_sha256: catalog.clone(),
            plugin: "probe".into(),
            handler_id: handler.id.clone(),
            request: Request::Api {
                method: "POST".into(),
                path: "/extension-probe/one".into(),
                query: Vec::new(),
                headers: BTreeMap::new(),
                body: "{}".into(),
            },
        }
        .validate(&catalog, &registry, handler)
        .unwrap();
        registry.archive_sha256 = "b".repeat(64);
        assert!(registry.content_digest().is_err());
        assert!(registry.validate_source(&catalog, &files).is_err());
        registry.archive_sha256.clear();
        registry.files_sha256 = Some("c".repeat(64));
        assert!(registry.validate_source(&catalog, &files).is_err());
    }

    #[test]
    fn typed_invocation_never_forwards_main_credentials_or_selects_another_handler_kind() {
        let catalog = "a".repeat(64);
        let registry = registry(&catalog);
        let Discovery::Complete { entries } = &registry.api else {
            panic!()
        };
        let handler = &entries[0];
        let mut invocation = Invocation {
            catalog_sha256: catalog.clone(),
            plugin: "probe".into(),
            handler_id: handler.id.clone(),
            request: Request::Api {
                method: "POST".into(),
                path: "/extension-probe/one".into(),
                query: Vec::new(),
                headers: BTreeMap::new(),
                body: "{}".into(),
            },
        };
        invocation.validate(&catalog, &registry, handler).unwrap();
        for name in [
            "authorization",
            "cookie",
            "proxy-authorization",
            "x-mrsm-team",
            "x-mrsm-executor-actor",
        ] {
            if let Request::Api { headers, .. } = &mut invocation.request {
                headers.insert(name.into(), "secret".into());
            }
            assert!(invocation.validate(&catalog, &registry, handler).is_err());
            if let Request::Api { headers, .. } = &mut invocation.request {
                headers.clear();
            }
        }
        invocation.request = Request::Mcp {
            arguments: Value::Null,
        };
        assert!(invocation.validate(&catalog, &registry, handler).is_err());
        assert!(
            Reply::Page {
                fragment: "<script>evil()</script>".into(),
                status: 200,
            }
            .validate(&handler.selector)
            .is_err()
        );
    }
}
