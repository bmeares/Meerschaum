//! Main-API executor identities and the public desired deployment shared with `stack up`.
//!
//! Only `desired/` is shared with the administrator's stack directory. Private signing keys
//! remain in the main API's root; sibling executors receive a single public verifier file.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use meerschaum_core::access::Team;
use meerschaum_core::execution::{Mode, Settings, team_key};
use meerschaum_core::team_scope::{create_private_dir, write_private};
use meerschaum_server::executor::control::{Authority, Verifier};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Relative main-API storage, with `desired/` containing no private control material.
pub const DIRECTORY: &str = "executors";
/// Public directory mounted into the main API and read by `mrsm stack up` on the host.
pub const DESIRED: &str = "desired";
/// Optional environment override for the public desired directory inside container stacks so
/// mounting it does not cause Docker to pre-create `/meerschaum/executors` as `root:root`.
pub const DESIRED_DIR_ENV: &str = "MRSM_EXECUTOR_DESIRED_DIR";
/// Mount target for the public desired directory inside the `api` service container.
pub const CONTAINER_DESIRED_DIR: &str = "/run/meerschaum/executors/desired";

/// One desired or retired executor, with stable UUID identity through team renames.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Registration {
    /// Stable UUID team identity.
    pub team_id: Uuid,
    /// Current display name.
    pub name: String,
    /// Stable service/network/volume suffix.
    pub key: String,
    /// Public control identity. The private key is in a different directory.
    pub verifier: Verifier,
    /// Administrator-selected deployment and resource settings.
    pub settings: Settings,
    /// Retired generations cannot issue further requests and are excluded from deployment.
    pub retired: bool,
}

/// Public desired deployment. It deliberately contains no private signing keys or team config.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Desired {
    /// The main API's stable identity.
    pub issuer: Option<Uuid>,
    /// Active and retired records, keyed by stable UUID. Retired data volumes remain retained.
    pub teams: BTreeMap<Uuid, Registration>,
}

/// Read and update identities under a process-shared, owner-only file lock.
pub struct Registry {
    root: PathBuf,
}

impl Registry {
    /// Storage rooted in the main API's own private root.
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.join(DIRECTORY),
        }
    }

    /// Public desired files; mount only this directory into a stack's API service.
    pub fn desired_dir(&self) -> PathBuf {
        if let Some(override_dir) = std::env::var_os(DESIRED_DIR_ENV).filter(|v| !v.is_empty()) {
            let configured_root = std::env::var_os("MRSM_ROOT_DIR")
                .filter(|v| !v.is_empty())
                .map(PathBuf::from);
            if configured_root.is_none_or(|r| self.root == r.join(DIRECTORY)) {
                return PathBuf::from(override_dir);
            }
        }
        self.root.join(DESIRED)
    }

    fn lock(&self) -> Result<std::fs::File, String> {
        create_private_dir(&self.root)
            .map_err(|_| "Could not create executor identity storage.")?;
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.root.join("lock"))
            .map_err(|_| "Could not open the executor identity lock.")?;
        fs4::FileExt::lock(&file).map_err(|_| "Could not lock executor identity storage.")?;
        Ok(file)
    }

    fn read(&self) -> Result<Desired, String> {
        match std::fs::read(self.root.join("registry.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|_| "Invalid executor identity registry.".into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                let initialized = self
                    .root
                    .join("initialized")
                    .try_exists()
                    .map_err(|_| "Could not inspect executor initialization state.")?;
                let populated = |name: &str| -> Result<bool, String> {
                    match std::fs::read_dir(self.root.join(name)) {
                        Ok(mut entries) => entries
                            .next()
                            .transpose()
                            .map(|entry| entry.is_some())
                            .map_err(|_| "Could not inspect existing executor identities.".into()),
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
                        Err(_) => Err("Could not inspect existing executor identities.".into()),
                    }
                };
                if initialized || populated("keys")? || populated(DESIRED)? {
                    return Err("The private executor registry is missing from an initialized deployment. Restore its private backup before reconciling executors.".into());
                }
                Ok(Desired::default())
            }
            Err(_) => Err("Could not read executor identity storage.".into()),
        }
    }

    /// Reconcile after reading current teams. Deletion or host mode retires the generation;
    /// selecting a remote executor again creates a fresh generation. Renames preserve it.
    pub fn reconcile(&self, teams: &[Team]) -> Result<Desired, String> {
        let _lock = self.lock()?;
        let mut desired = self.read()?;
        let issuer = *desired.issuer.get_or_insert_with(Uuid::new_v4);
        let mut selected = BTreeMap::new();
        // Validate all desired selections before changing persisted state.
        for team in teams {
            let settings = team.execution().map_err(|e| e.to_string())?;
            if settings.mode == Mode::Host {
                continue;
            }
            let key = team_key(team).map_err(|e| e.to_string())?;
            let id = team
                .id
                .as_deref()
                .and_then(|v| Uuid::parse_str(v).ok())
                .ok_or("An executor requires a registered UUID team.")?;
            selected.insert(id, (team, settings, key));
        }
        // Written before generating identities: a failed or interrupted first write must not
        // be mistaken for a fresh deployment and mint a replacement issuer on the next call.
        write_private(&self.root.join("initialized"), "1\n")
            .map_err(|_| "Could not persist executor initialization state.")?;
        for (id, record) in desired.teams.iter_mut() {
            if !selected.contains_key(id) {
                record.retired = true;
            }
        }
        for (id, (team, settings, key)) in selected {
            if let Some(record) = desired.teams.get_mut(&id).filter(|r| {
                !r.retired
                    && r.settings == settings
                    && r.verifier.version == env!("CARGO_PKG_VERSION")
                    && r.verifier.protocol == meerschaum_server::executor::control::PROTOCOL
            }) {
                record.name = team.name.clone();
                record.settings = settings;
                continue;
            }
            let generation = Uuid::new_v4();
            let (authority, secret) = Authority::generate(issuer, id, generation)
                .map_err(|_| "Could not generate an executor control identity.")?;
            let keys = self.root.join("keys");
            create_private_dir(&keys).map_err(|_| "Could not create executor key storage.")?;
            use base64::Engine;
            let encoded = base64::engine::general_purpose::STANDARD.encode(secret.as_ref());
            write_private(&keys.join(format!("{generation}.pk8")), &encoded)
                .map_err(|_| "Could not persist executor control identity.")?;
            desired.teams.insert(
                id,
                Registration {
                    team_id: id,
                    name: team.name.clone(),
                    key,
                    verifier: authority.verifier(),
                    settings,
                    retired: false,
                },
            );
        }
        // Retire callback authority before exposing a replacement deployment. Even an old
        // bearer with a live source user cannot adopt a reselected team generation.
        for record in desired.teams.values() {
            meerschaum_server::executor::callbacks::set_generation(
                self.root
                    .parent()
                    .ok_or("Invalid executor authority root.")?,
                record.team_id,
                (!record.retired && record.settings.mode.is_remote())
                    .then_some(record.verifier.generation),
            )
            .map_err(|_| "Could not reconcile executor callback generations.")?;
        }
        let text = serde_json::to_string_pretty(&desired)
            .map_err(|_| "Could not encode desired executors.")?;
        write_private(&self.root.join("registry.json"), &text)
            .map_err(|_| "Could not persist desired executors.")?;
        self.write_desired(&desired, &text)?;
        Ok(desired)
    }

    fn write_desired(&self, desired: &Desired, text: &str) -> Result<(), String> {
        let directory = self.desired_dir();
        create_private_dir(&directory)
            .map_err(|_| "Could not create public executor deployment storage.")?;
        for record in desired.teams.values().filter(|r| !r.retired) {
            let text = serde_json::to_string_pretty(&record.verifier)
                .map_err(|_| "Could not encode executor verifier.")?;
            write_private(&directory.join(format!("{}.json", record.key)), &text)
                .map_err(|_| "Could not persist public executor verifier.")?;
        }
        write_private(&directory.join("index.json"), text)
            .map_err(|_| "Could not persist public executor deployment.".into())
    }

    /// Load an active record and its private signing authority. Callers must first verify the
    /// team still selects this executor against the current authoritative team store.
    pub fn authority(&self, id: Uuid) -> Result<(Registration, Authority), String> {
        let _lock = self.lock()?;
        let desired = self.read()?;
        let record = desired
            .teams
            .get(&id)
            .filter(|r| !r.retired)
            .cloned()
            .ok_or("The selected executor has not been provisioned or has been retired.")?;
        let encoded = std::fs::read_to_string(
            self.root
                .join("keys")
                .join(format!("{}.pk8", record.verifier.generation)),
        )
        .map_err(|_| "Could not read executor control identity.")?;
        use base64::Engine;
        let secret = base64::engine::general_purpose::STANDARD
            .decode(encoded.trim())
            .map_err(|_| "Invalid executor control identity.")?;
        let authority = Authority::from_pkcs8(
            record.verifier.issuer,
            id,
            record.verifier.generation,
            &secret,
        )
        .map_err(|_| "Invalid executor control identity.")?;
        if authority.verifier() != record.verifier {
            return Err("Executor control identity does not match its registry.".into());
        }
        Ok((record, authority))
    }

    /// Independent reverse identity comes only from the administrator's protected
    /// Main mount, never this public desired deployment or an HTTP response.
    pub fn factory_pin(
        &self,
        id: Uuid,
    ) -> Result<Option<meerschaum_server::executor::factory_attestation::Pin>, String> {
        // Reader-only planning must not create a registry or load its signing key.
        // read() sees one atomic snapshot of Main's private registry.
        let registered = self.read()?;
        let record = registered
            .teams
            .get(&id)
            .filter(|r| !r.retired && r.team_id == id && r.verifier.team_id == id)
            .ok_or("The selected executor has not been provisioned or has been retired.")?;
        crate::executor_factory_local::public_pin(&record.verifier)
    }

    /// Read only public desired deployment; stack provisioning never needs private authority.
    pub fn desired(&self) -> Result<Desired, String> {
        match std::fs::read(self.desired_dir().join("index.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map_err(|_| "Invalid public executor deployment.".into()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Desired::default()),
            Err(_) => Err("Could not read public executor deployment.".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meerschaum_core::{Map, Value};

    #[test]
    fn missing_private_registry_never_regenerates_an_initialized_deployment() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new(root.path());
        registry.reconcile(&[team()]).unwrap();
        let public = std::fs::read(registry.desired_dir().join("index.json")).unwrap();
        let keys = || {
            let mut keys = std::fs::read_dir(root.path().join(DIRECTORY).join("keys"))
                .unwrap()
                .map(|e| e.unwrap().file_name())
                .collect::<Vec<_>>();
            keys.sort();
            keys
        };
        let before = keys();
        std::fs::remove_file(root.path().join(DIRECTORY).join("registry.json")).unwrap();
        assert!(registry.reconcile(&[team()]).is_err());
        assert_eq!(
            public,
            std::fs::read(registry.desired_dir().join("index.json")).unwrap()
        );
        assert_eq!(before, keys());
        // Older initialized deployments without the marker are protected by their artifacts.
        std::fs::remove_file(root.path().join(DIRECTORY).join("initialized")).unwrap();
        assert!(registry.reconcile(&[team()]).is_err());
        std::fs::remove_dir_all(root.path().join(DIRECTORY).join("keys")).unwrap();
        assert!(registry.reconcile(&[team()]).is_err());
    }

    fn team() -> Team {
        let mut team = Team::new("Wetlands");
        team.id = Some(Uuid::new_v4().to_string());
        team.attributes.insert("isolated".into(), Value::Bool(true));
        team.attributes.insert(
            "execution".into(),
            Value::Map(Map::from_iter([("mode".into(), "container".into())])),
        );
        team
    }

    #[test]
    fn rename_is_stable_and_reselection_after_retirement_rotates_control_identity() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new(root.path());
        let mut team = team();
        let first = registry.reconcile(&[team.clone()]).unwrap();
        let id = Uuid::parse_str(team.id.as_deref().unwrap()).unwrap();
        let initial = &first.teams[&id];
        assert_eq!(
            registry.authority(id).unwrap().1.verifier(),
            initial.verifier
        );
        team.name = "Estuaries".into();
        let renamed = registry.reconcile(&[team.clone()]).unwrap();
        assert_eq!(renamed.teams[&id].key, initial.key);
        assert_eq!(renamed.teams[&id].verifier, initial.verifier);
        let retired = registry.reconcile(&[]).unwrap();
        assert!(retired.teams[&id].retired);
        assert!(registry.authority(id).is_err());
        let recreated = registry.reconcile(&[team]).unwrap();
        assert_ne!(
            recreated.teams[&id].verifier.generation,
            initial.verifier.generation
        );
        assert_eq!(recreated.teams[&id].key, initial.key);
        let text = std::fs::read_to_string(registry.desired_dir().join("index.json")).unwrap();
        assert!(!text.contains("PRIVATE"));
        assert!(!text.contains("pk8"));
    }

    #[test]
    fn corruption_or_missing_private_keys_never_regenerates_a_live_identity() {
        let root = tempfile::tempdir().unwrap();
        let registry = Registry::new(root.path());
        let desired = registry.reconcile(&[team()]).unwrap();
        let record = desired.teams.values().next().unwrap();
        let key = registry
            .root
            .join("keys")
            .join(format!("{}.pk8", record.verifier.generation));
        std::fs::write(&key, "invalid").unwrap();
        assert!(registry.authority(record.team_id).is_err());
        assert_eq!(
            registry.desired().unwrap().teams[&record.team_id].verifier,
            record.verifier
        );
    }
}
