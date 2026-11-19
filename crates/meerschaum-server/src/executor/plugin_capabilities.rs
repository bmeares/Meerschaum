//! Pure source classifications, derived only by credential-free confined discovery.
//! Missing classification must never be interpreted as `streaming = false` / `python = false`.

use super::plugins;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConnectorCapabilities {
    pub python: bool,
    pub streaming: bool,
    pub instance: bool,
    pub thread_safe: bool,
    pub methods: BTreeSet<String>,
    pub required_attributes: BTreeSet<String>,
    pub optional_attributes: BTreeSet<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum Classification {
    Unsupported {
        feature: String,
    },
    Known {
        python: bool,
        /// Actual imported module's callable data functions, including subscribe.
        module_functions: BTreeSet<String>,
        /// Declared dependencies, never resolved or installed by the Main reader.
        required: BTreeSet<String>,
        /// A missing class/source key remains unclassified.
        connector_types: BTreeMap<String, ConnectorCapabilities>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Capabilities {
    /// Entire exact immutable selected source set, never only this plugin's source hash.
    pub source_catalog_sha256: String,
    pub plugins: BTreeMap<String, Classification>,
}

impl Capabilities {
    pub fn validate(&self, source_catalog: &plugins::Validated) -> Result<(), String> {
        if self.source_catalog_sha256
            != source_catalog
                .source_digest()
                .map_err(|_| "Invalid source catalog.")?
            || self.plugins.keys().collect::<BTreeSet<_>>()
                != source_catalog
                    .catalog()
                    .plugins
                    .keys()
                    .collect::<BTreeSet<_>>()
        {
            return Err("Connector capabilities have another exact source set.".into());
        }
        let bytes = serde_json::to_vec(self).map_err(|_| "Invalid connector capability data.")?;
        if bytes.len() > plugins::MAX_CATALOG_BYTES {
            return Err("Connector capability data exceeds its bound.".into());
        }
        let mut types = BTreeSet::new();
        for (name, classification) in &self.plugins {
            match classification {
                Classification::Unsupported { feature }
                    if feature.is_empty()
                        || feature.len() > 256
                        || feature.chars().any(char::is_control) =>
                {
                    return Err("Invalid unsupported capability marker.".into());
                }
                Classification::Unsupported { .. } => {}
                Classification::Known {
                    python: observed_python,
                    module_functions,
                    required,
                    connector_types,
                } => {
                    if module_functions.iter().any(|name| {
                        !matches!(
                            name.as_str(),
                            "fetch" | "sync" | "register" | "setup" | "subscribe"
                        )
                    }) || required.len() > 256
                        || connector_types.len() > 256
                    {
                        return Err("Invalid observed module capabilities.".into());
                    }
                    let python = matches!(
                        source_catalog.catalog().plugins[name].kind,
                        plugins::SourceKind::PythonModule | plugins::SourceKind::PythonPackage
                    );
                    if *observed_python != python {
                        return Err("Module classification has another implementation kind.".into());
                    }
                    for (typ, capability) in connector_types {
                        if !plugins::valid_name(typ)
                            || !types.insert(typ)
                            || capability.python != python
                            || capability.methods.len() > 1024
                            || capability.required_attributes.len() > 256
                            || capability.optional_attributes.len() > 256
                            || capability
                                .methods
                                .iter()
                                .chain(&capability.required_attributes)
                                .chain(&capability.optional_attributes)
                                .any(|name| {
                                    name.is_empty()
                                        || name.len() > 256
                                        || name.chars().any(char::is_control)
                                })
                        {
                            return Err("Invalid observed connector capabilities.".into());
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// This pure lookup never imports, guesses or starts discovery. The caller must verify
    /// fresh original ACLs and current exact source hashes before exposing a cached value.
    pub fn lookup(&self, source_keys: &str) -> Option<ConnectorCapabilities> {
        let (typ, label) = source_keys.split_once(':')?;
        if typ == "plugin" {
            let Classification::Known {
                python,
                module_functions,
                ..
            } = self.plugins.get(label)?
            else {
                return None;
            };
            return Some(ConnectorCapabilities {
                python: *python,
                streaming: module_functions.contains("subscribe"),
                instance: false,
                thread_safe: false,
                methods: module_functions.clone(),
                required_attributes: BTreeSet::new(),
                optional_attributes: BTreeSet::new(),
            });
        }
        self.plugins
            .values()
            .find_map(|classification| match classification {
                Classification::Known {
                    connector_types, ..
                } => connector_types.get(typ).cloned(),
                _ => None,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_observed_flags_never_deserialize_as_nonstreaming_native_capabilities() {
        let value = serde_json::json!({"instance":false,"thread_safe":false,"methods":[],"required_attributes":[],"optional_attributes":[]});
        assert!(serde_json::from_value::<ConnectorCapabilities>(value.clone()).is_err());
        let mut missing_python = value.clone();
        missing_python["streaming"] = false.into();
        assert!(serde_json::from_value::<ConnectorCapabilities>(missing_python).is_err());
        let mut explicit = value;
        explicit["python"] = true.into();
        explicit["streaming"] = false.into();
        let observed = serde_json::from_value::<ConnectorCapabilities>(explicit).unwrap();
        assert!(observed.python);
        assert!(!observed.streaming);
    }
    #[test]
    fn reader_lookup_keeps_unknown_distinct_from_observed_module_subscribe() {
        let mut capabilities = Capabilities {
            source_catalog_sha256: "a".repeat(64),
            plugins: BTreeMap::from([(
                "probe".into(),
                Classification::Unsupported {
                    feature: "not_discovered".into(),
                },
            )]),
        };
        assert!(capabilities.lookup("plugin:probe").is_none());
        assert!(capabilities.lookup("plugin:missing").is_none());
        capabilities.plugins.insert(
            "probe".into(),
            Classification::Known {
                python: true,
                module_functions: BTreeSet::from(["subscribe".into()]),
                required: BTreeSet::new(),
                connector_types: BTreeMap::new(),
            },
        );
        let module = capabilities.lookup("plugin:probe").unwrap();
        assert!(module.python && module.streaming);
        assert!(capabilities.lookup("custom:label").is_none());
    }
}
