//! Immutable Main-selected executable sources. Never decoded from a job property or env.

use super::plugins;
use crate::access::Access;
use crate::auth::Principal;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Fingerprint {
    Archive {
        source_kind: plugins::SourceKind,
        archive_sha256: String,
    },
    Files {
        tree_sha256: String,
    },
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Ceiling {
    pub sources: BTreeMap<String, Fingerprint>,
}
impl Ceiling {
    pub fn from_catalog(catalog: &plugins::Catalog) -> Result<Self, String> {
        if let super::plugin_host::Placement::Host(host) = &catalog.placement {
            host.validate_catalog(catalog.team_id, catalog.generation, &catalog.plugins)?;
            return Self::from_files(&host.files);
        }
        let ceiling = Self {
            sources: catalog
                .plugins
                .iter()
                .map(|(name, source)| {
                    (
                        name.clone(),
                        Fingerprint::Archive {
                            source_kind: source.kind,
                            archive_sha256: source.archive_sha256.clone(),
                        },
                    )
                })
                .collect(),
        };
        ceiling.validate()?;
        Ok(ceiling)
    }
    /// Registration's exact host-selected source file snapshot is grouped by plugin entry.
    pub fn from_files(files: &BTreeMap<String, String>) -> Result<Self, String> {
        if files.len() > 4096 {
            return Err("Too many source snapshot files.".into());
        }
        let mut groups: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
        for (path, digest) in files {
            if path.len() > 1024
                || path.starts_with('/')
                || path.contains('\\')
                || path.chars().any(char::is_control)
                || path.split('/').any(|part| matches!(part, "" | "." | ".."))
                || !plugins::valid_digest(digest)
            {
                return Err("Invalid captured source file snapshot.".into());
            }
            let first = path
                .split('/')
                .next()
                .ok_or("Missing source entry point.")?;
            let name = first
                .strip_suffix(".py")
                .or_else(|| first.strip_suffix(".rs"))
                .unwrap_or(first);
            if !plugins::valid_name(name) {
                return Err("Invalid captured source name.".into());
            }
            groups
                .entry(name.into())
                .or_default()
                .insert(path.clone(), digest.clone());
        }
        let sources = groups
            .into_iter()
            .map(|(name, files)| {
                let mut bytes = b"mrsm-source-files-v1\0".to_vec();
                bytes.extend(
                    serde_json::to_vec(&files).map_err(|_| "Invalid source snapshot encoding.")?,
                );
                Ok((
                    name,
                    Fingerprint::Files {
                        tree_sha256: plugins::sha256(&bytes),
                    },
                ))
            })
            .collect::<Result<_, String>>()?;
        let ceiling = Self { sources };
        ceiling.validate()?;
        Ok(ceiling)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.sources.len() > plugins::MAX_PLUGINS
            || self.sources.iter().any(|(name, source)| {
                !plugins::valid_name(name)
                    || !plugins::valid_digest(match source {
                        Fingerprint::Archive { archive_sha256, .. } => archive_sha256,
                        Fingerprint::Files { tree_sha256 } => tree_sha256,
                    })
            })
        {
            return Err("Invalid immutable source ceiling.".into());
        }
        Ok(())
    }
    pub fn names(&self) -> BTreeSet<String> {
        self.sources.keys().cloned().collect()
    }
    pub fn contains(&self, requested: &Self) -> bool {
        requested
            .sources
            .iter()
            .all(|(name, source)| self.sources.get(name) == Some(source))
    }
    pub fn permits_archive(&self, name: &str, digest: &str) -> bool {
        matches!(self.sources.get(name),Some(Fingerprint::Archive {archive_sha256,..}) if archive_sha256==digest)
    }
    pub fn permits_name(&self, name: &str) -> bool {
        self.sources.contains_key(name)
    }
    /// A callback-derived operation cannot recover sources removed from its parent, even when
    /// the original user/Team gained those sources after the parent launch.
    pub fn within_parent(&self, caller: &Access) -> bool {
        match &caller.auth {
            Some(Principal::Execution(identity)) => identity
                .grant
                .source_ceiling
                .as_ref()
                .map_or(self.sources.is_empty(), |parent| parent.contains(self)),
            _ => true,
        }
    }
    pub fn current(&self, caller: &Access, team: Option<&meerschaum_core::access::Team>) -> bool {
        let explicit = team
            .filter(|team| {
                team.is_isolated()
                    || team
                        .execution()
                        .is_ok_and(|settings| settings.mode.is_remote())
            })
            .map(|team| team.granted_plugins().into_iter().collect::<BTreeSet<_>>());
        self.sources.keys().all(|name| {
            meerschaum_core::access::can_use_plugin(&caller.principal, name)
                && explicit.as_ref().is_none_or(|grants| grants.contains(name))
        })
    }
}

/// Name checks for Main's source guard also apply to non-isolated callback principals, whose
/// ordinary user visibility would otherwise allow every Main-installed plugin.
pub fn permits_name(caller: &Access, name: &str) -> bool {
    match &caller.auth {
        Some(Principal::Execution(identity)) => identity
            .grant
            .source_ceiling
            .as_ref()
            .is_some_and(|ceiling| ceiling.permits_name(name)),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn source_type_content_and_exact_subset_are_immutable() {
        let a = Ceiling {
            sources: BTreeMap::from([(
                "a".into(),
                Fingerprint::Archive {
                    source_kind: plugins::SourceKind::PythonModule,
                    archive_sha256: "a".repeat(64),
                },
            )]),
        };
        a.validate().unwrap();
        assert!(a.contains(&Ceiling::default()));
        let mut changed = a.clone();
        changed.sources.insert(
            "b".into(),
            Fingerprint::Files {
                tree_sha256: "b".repeat(64),
            },
        );
        assert!(!a.contains(&changed));
        changed = a.clone();
        changed.sources.insert(
            "a".into(),
            Fingerprint::Files {
                tree_sha256: "a".repeat(64),
            },
        );
        assert!(!a.contains(&changed));
        let files = BTreeMap::from([
            ("a/__init__.py".into(), "a".repeat(64)),
            ("a/extra.py".into(), "b".repeat(64)),
        ]);
        let first = Ceiling::from_files(&files).unwrap();
        let mut changed = files;
        changed.insert("a/extra.py".into(), "c".repeat(64));
        assert!(!first.contains(&Ceiling::from_files(&changed).unwrap()));
    }
}
