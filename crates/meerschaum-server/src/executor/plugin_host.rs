//! Explicit Main-host source identity. Host files are never fabricated executor archives.

use super::{
    plugins::{self, Plugin, SourceKind},
    source_ceiling::{Ceiling, Fingerprint},
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

/// Remote catalogs retain their existing wire shape; a Host identity is explicit.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Placement {
    /// Registered sibling executor archives, the original catalog representation.
    #[default]
    Remote,
    /// Main-selected immutable file snapshots, including Everyone without a Team identity.
    Host(HostIdentity),
}
impl Placement {
    /// Preserve the remote source-only serialization and digest exactly.
    pub fn is_remote(&self) -> bool {
        matches!(self, Self::Remote)
    }
}

/// Main owns the generation and verifies original live files before deriving or invoking.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostIdentity {
    /// Real registered Team, or Everyone. No synthetic Team is registered for Host metadata.
    pub team_id: Option<Uuid>,
    /// Persistent private Main-host authority generation, independent of remote registries.
    pub generation: Uuid,
    /// Exact relative file identities; names never select an arbitrary Main filesystem path.
    pub files: BTreeMap<String, String>,
    /// Opaque binding to the exact original canonical source view and copied bytes.
    pub view_sha256: String,
}
impl HostIdentity {
    /// Bound shape and file identities before using any metadata or runtime paths.
    pub fn validate(&self) -> Result<(), String> {
        if self.generation.is_nil()
            || self.team_id.is_some_and(|id| id.is_nil())
            || !plugins::valid_digest(&self.view_sha256)
        {
            return Err("Invalid Main-host source identity.".into());
        }
        let ceiling = Ceiling::from_files(&self.files)?;
        if ceiling.names().iter().any(|name| name == "__init__") {
            return Err("Reserved Host source name.".into());
        }
        for name in ceiling.names() {
            let _ = self.source_kind(&name)?;
        }
        Ok(())
    }

    /// Only entry-point markers inside the captured Files snapshot determine source kind.
    pub fn source_kind(&self, name: &str) -> Result<SourceKind, String> {
        if !plugins::valid_name(name) {
            return Err("Invalid Host source name.".into());
        }
        let candidates = [
            (format!("{name}.py"), SourceKind::PythonModule),
            (format!("{name}/__init__.py"), SourceKind::PythonPackage),
            (format!("{name}.rs"), SourceKind::NativeSource),
            (format!("{name}/plugin.toml"), SourceKind::NativePackage),
        ]
        .into_iter()
        .filter(|(entry, _)| self.files.contains_key(entry))
        .map(|(_, kind)| kind)
        .collect::<Vec<_>>();
        if candidates.len() != 1 {
            return Err("The Host source has no unambiguous supported import entry point.".into());
        }
        Ok(candidates[0])
    }

    /// Domain-separated Files tree digests bind handler IDs; cache keys still bind the whole set.
    pub fn plugin_digests(&self) -> Result<BTreeMap<String, String>, String> {
        let ceiling = Ceiling::from_files(&self.files)?;
        ceiling
            .sources
            .into_iter()
            .map(|(name, fingerprint)| match fingerprint {
                Fingerprint::Files { tree_sha256 } => Ok((name, tree_sha256)),
                _ => Err("A Host source cannot claim archive authority.".into()),
            })
            .collect()
    }

    /// Validate legacy scope slots against the explicit Host identity. Everyone uses nil only
    /// as that wire slot; the Host/None variant, not a fabricated registered Team, is authority.
    pub fn validate_catalog(
        &self,
        team_id: Uuid,
        generation: Uuid,
        entries: &BTreeMap<String, Plugin>,
    ) -> Result<(), String> {
        self.validate()?;
        let digests = self.plugin_digests()?;
        if team_id != self.team_id.unwrap_or_else(Uuid::nil)
            || generation != self.generation
            || entries.keys().cloned().collect::<BTreeSet<_>>() != digests.keys().cloned().collect()
        {
            return Err(
                "The catalog does not match its exact Host scope and file snapshot.".into(),
            );
        }
        for (name, entry) in entries {
            if !entry.archive_sha256.is_empty() || entry.kind != self.source_kind(name)? {
                return Err("A Host catalog cannot claim executor archive content.".into());
            }
        }
        Ok(())
    }
}

/// Read the existing Main-owned Host generation without creating authority or a lock file.
pub fn read_generation(root: &std::path::Path) -> Result<Uuid, String> {
    use std::io::Read;
    #[cfg(not(unix))]
    {
        let _ = root;
        return Err("The Host generation requires supported Unix.".into());
    }
    #[cfg(unix)]
    {
        let file = std::fs::File::from(
            rustix::fs::open(
                root.join("executor-host-identity"),
                rustix::fs::OFlags::RDONLY
                    | rustix::fs::OFlags::NOFOLLOW
                    | rustix::fs::OFlags::NONBLOCK
                    | rustix::fs::OFlags::CLOEXEC,
                rustix::fs::Mode::empty(),
            )
            .map_err(|_| "The Host generation is unavailable.")?,
        );
        let metadata = file
            .metadata()
            .map_err(|_| "The Host generation is unavailable.")?;
        if !metadata.is_file() || metadata.len() > 64 {
            return Err("Invalid Host generation.".into());
        }
        let mut bytes = Vec::new();
        file.take(65)
            .read_to_end(&mut bytes)
            .map_err(|_| "Unreadable Host generation.")?;
        if bytes.len() > 64 {
            return Err("Oversized Host generation.".into());
        }
        std::str::from_utf8(&bytes)
            .ok()
            .and_then(|text| Uuid::parse_str(text).ok())
            .filter(|id| !id.is_nil())
            .ok_or_else(|| "Invalid Host generation.".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn identity() -> HostIdentity {
        HostIdentity {
            team_id: None,
            generation: Uuid::new_v4(),
            files: BTreeMap::from([("a.py".into(), "a".repeat(64))]),
            view_sha256: "b".repeat(64),
        }
    }
    #[test]
    fn everyone_has_an_explicit_host_identity_and_cannot_claim_archives_or_another_scope() {
        let identity = identity();
        let entries = BTreeMap::from([(
            "a".into(),
            Plugin {
                archive_sha256: String::new(),
                kind: SourceKind::PythonModule,
                metadata: None,
            },
        )]);
        identity
            .validate_catalog(Uuid::nil(), identity.generation, &entries)
            .unwrap();
        assert!(
            identity
                .validate_catalog(Uuid::new_v4(), identity.generation, &entries)
                .is_err()
        );
        let mut archives = entries;
        archives.get_mut("a").unwrap().archive_sha256 = "a".repeat(64);
        assert!(
            identity
                .validate_catalog(Uuid::nil(), identity.generation, &archives)
                .is_err()
        );
        let encoded = serde_json::to_value(Placement::Host(identity)).unwrap();
        assert_eq!(encoded["kind"], "host");
        assert!(encoded["team_id"].is_null());
    }
    #[test]
    fn files_snapshot_paths_entry_kinds_and_content_are_exact() {
        let mut identity = identity();
        identity.validate().unwrap();
        let original = identity.plugin_digests().unwrap();
        identity.files.insert("a.py".into(), "c".repeat(64));
        assert_ne!(identity.plugin_digests().unwrap(), original);
        identity
            .files
            .insert("a/__init__.py".into(), "a".repeat(64));
        assert!(identity.validate().is_err());
        identity.files.remove("a/__init__.py");
        identity.files.insert("../secret.py".into(), "a".repeat(64));
        assert!(identity.validate().is_err());
    }

    #[test]
    fn host_decode_requires_the_exact_files_identity_and_remote_decode_refuses_it() {
        let identity = identity();
        let catalog = plugins::Catalog {
            schema: plugins::SCHEMA,
            release: env!("CARGO_PKG_VERSION").into(),
            team_id: Uuid::nil(),
            generation: identity.generation,
            placement: Placement::Host(identity.clone()),
            plugins: BTreeMap::from([(
                "a".into(),
                Plugin {
                    archive_sha256: String::new(),
                    kind: SourceKind::PythonModule,
                    metadata: None,
                },
            )]),
        };
        let bytes = serde_json::to_vec(&catalog).unwrap();
        assert!(
            !String::from_utf8(bytes.clone())
                .unwrap()
                .contains("archive_sha256")
        );
        let grants = BTreeSet::from(["a".into()]);
        let reserved = BTreeSet::new();
        let validated =
            plugins::Validated::decode_host(&bytes, &identity, &grants, &reserved).unwrap();
        assert!(!validated.complete());
        assert_eq!(
            validated.plugin_digests().unwrap(),
            identity.plugin_digests().unwrap()
        );
        assert_eq!(
            validated.plugin_fingerprints().unwrap(),
            Ceiling::from_files(&identity.files).unwrap().sources
        );
        assert!(validated.plugin_digest("absent").is_err());
        assert!(
            plugins::Validated::decode(
                &bytes,
                plugins::Expected {
                    team_id: Uuid::new_v4(),
                    generation: identity.generation,
                    grants: &grants,
                    reserved_actions: &reserved,
                }
            )
            .is_err()
        );
        let mut changed = identity.clone();
        changed.files.insert("a.py".into(), "c".repeat(64));
        assert!(plugins::Validated::decode_host(&bytes, &changed, &grants, &reserved).is_err());
        assert!(
            plugins::Validated::decode_host(&bytes, &identity, &BTreeSet::new(), &reserved)
                .is_err()
        );
        let mut claimed = catalog;
        claimed.plugins.get_mut("a").unwrap().archive_sha256 = "a".repeat(64);
        assert!(
            plugins::Validated::decode_host(
                &serde_json::to_vec(&claimed).unwrap(),
                &identity,
                &grants,
                &reserved
            )
            .is_err()
        );
    }
}
