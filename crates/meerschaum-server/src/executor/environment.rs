//! Signed delivery of a launch's rotating callback credentials and runtime profile.
//! Only opaque main-owned identities cross this interface; never filesystem paths.

use meerschaum_connector_api::models::ExecutionCredential;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Server-owned marker: the job must resolve this canonical context on every launch.
pub const JOB_CONTEXT_PROPERTY: &str = "mrsm_launch_context";
/// A metadata worker carries no reusable API credential, even in its private context files.
pub const METADATA_TOKEN: &str = "mrsm-metadata-no-authority";
pub const METADATA_ORIGIN: &str = "http://metadata.invalid";
pub const METADATA_VAR: &str = "MRSM_METADATA_EXECUTION";

/// Retire only the revision main observed; a delayed delete cannot stop a later restart.
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Retire {
    pub revision: u64,
}

/// A retains its assigned team environment; B receives API-only configuration and confinement.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Profile {
    #[default]
    Team,
    Api,
    /// Main-created source discovery only: no database, callback or network authority.
    Metadata,
}

impl Profile {
    pub fn private_runtime(self) -> bool {
        matches!(self, Self::Api | Self::Metadata)
    }
}

/// The one resource allowed to use a delivered context.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Target {
    Terminal,
    Job { name: String },
}

/// Main signs this complete body. The executor chooses every local path itself.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Update {
    /// Main's immutable compute deadline. Only credential-free Metadata jobs may carry it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_deadline: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extension: Option<super::extensions::Operation>,
    /// Main-selected fixed connector hook; no public argument selects this descriptor.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registration: Option<super::registration::Registration>,
    /// Exact immutable source catalog selected by Main for this private launch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub discovery_catalog_sha256: Option<String>,
    pub id: Uuid,
    /// Persisted by main; retirement of this revision cannot be undone by a late renewal.
    pub revision: u64,
    pub owner: String,
    pub owner_id: Option<String>,
    pub profile: Profile,
    pub target: Target,
    pub callback_url: String,
    pub credential: ExecutionCredential,
}

impl Update {
    /// Structural checks precede any filesystem or process operation.
    pub fn validate(&self, actor: &super::control::Actor) -> Result<(), String> {
        let now = chrono::Utc::now().timestamp();
        match (self.profile, self.metadata_deadline) {
            (Profile::Metadata, Some(deadline))
                if deadline > now
                    && deadline <= now.saturating_add(60)
                    && self.credential.expires_at <= deadline => {}
            (Profile::Metadata, _) | (_, Some(_)) => {
                return Err(
                    "Only Metadata jobs may carry their immutable bounded compute deadline.".into(),
                );
            }
            _ => {}
        }
        if let Some(extension) = &self.extension {
            if self.profile != Profile::Api
                || !matches!(self.target, Target::Job { .. })
                || self.registration.is_some()
            {
                return Err("An extension requires its exact private API job.".into());
            }
            let catalog = extension.validated().map_err(|error| error.detail_text())?;
            // The complete operation binds a whole source-only catalog, including a Host
            // epoch and view. Files are not converted into invented executor archives.
            let digest = catalog
                .source_digest()
                .map_err(|_| "Invalid extension source selection.")?;
            if self.discovery_catalog_sha256.as_deref() != Some(digest.as_str()) {
                return Err("The extension and launch source catalogs differ.".into());
            }
        }
        if let Some(registration) = &self.registration {
            if self.profile != Profile::Api || !matches!(self.target, Target::Job { .. }) {
                return Err("A typed connector registration requires its private API job.".into());
            }
            registration
                .validate()
                .map_err(|error| error.detail_text())?;
        }
        if self.profile == Profile::Metadata
            && (!matches!(self.target, Target::Job { .. })
                || self.credential.token != METADATA_TOKEN
                || self.callback_url != METADATA_ORIGIN
                || self
                    .discovery_catalog_sha256
                    .as_deref()
                    .is_none_or(|digest| !super::plugins::valid_digest(digest)))
        {
            return Err("Metadata discovery requires its credential-free job context.".into());
        }
        if self
            .discovery_catalog_sha256
            .as_ref()
            .is_some_and(|digest| !super::plugins::valid_digest(digest))
        {
            return Err("A launch may select only a valid immutable source catalog.".into());
        }
        if let Some(registration) = &self.registration
            && let Some(digest) = &self.discovery_catalog_sha256
        {
            let super::registration::Sources::Archives { catalog } = &registration.sources else {
                return Err("A Host file snapshot cannot select an archive catalog.".into());
            };
            if super::plugins::sha256(
                &serde_json::to_vec(catalog).map_err(|_| "Invalid source catalog.")?,
            ) != *digest
            {
                return Err("The registration and launch source catalogs differ.".into());
            }
        }
        let text = |s: &str| !s.is_empty() && s.len() <= 256 && !s.chars().any(char::is_control);
        if self.id.is_nil()
            || self.revision == 0
            || !text(&self.owner)
            || self.owner_id.as_ref().is_some_and(|s| !text(s))
            || (!actor.administrator && actor.owner != self.owner)
        {
            return Err("The execution context has an invalid owner or identity.".into());
        }
        if let Target::Job { name } = &self.target {
            let Some(id) = name.strip_prefix("mrsm-") else {
                return Err("The execution context does not name a canonical job.".into());
            };
            if id.len() != 32 || Uuid::parse_str(id).is_err() {
                return Err("The execution context does not name a canonical job.".into());
            }
            if Uuid::parse_str(id).ok() != Some(self.id) {
                return Err(
                    "The execution context does not match its canonical job identity.".into(),
                );
            }
        }
        validate_callback_url(&self.callback_url)?;
        let now = chrono::Utc::now().timestamp();
        if self.credential.expires_at <= now
            || self.credential.expires_at > now + super::callbacks::TOKEN_SECONDS + 5
            || self.credential.token.is_empty()
            || self.credential.token.len() > 16 * 1024
            || !self.credential.token.is_ascii()
            || self
                .credential
                .token
                .bytes()
                .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
        {
            return Err("The execution context's callback credential is stale or invalid.".into());
        }
        Ok(())
    }
}

/// An API origin only: no reusable login, path/query override, fragment or routing credentials.
pub fn validate_callback_url(value: &str) -> Result<(), String> {
    let invalid = || {
        "The execution callback URL must be an HTTP or HTTPS API origin without credentials, query or path.".to_string()
    };
    if value.len() > 2048 || value.contains(['#', '\\']) {
        return Err(invalid());
    }
    let uri: hyper::Uri = value.parse().map_err(|_| invalid())?;
    if !matches!(uri.scheme_str(), Some("http" | "https"))
        || uri.authority().is_none_or(|a| {
            a.host().is_empty() || a.as_str().contains('@') || a.port_u16() == Some(0)
        })
        || uri
            .path_and_query()
            .is_some_and(|p| p.as_str() != "/" && !p.as_str().is_empty())
    {
        return Err(invalid());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compute_deadline_is_required_for_metadata_and_never_added_to_other_profiles() {
        let now = chrono::Utc::now().timestamp();
        let id = Uuid::new_v4();
        let actor = super::super::control::Actor {
            owner: "alice".into(),
            username: None,
            permissions: Default::default(),
            administrator: false,
        };
        let valid = Update {
            metadata_deadline: Some(now + 50),
            extension: None,
            registration: None,
            discovery_catalog_sha256: Some("a".repeat(64)),
            id,
            revision: 1,
            owner: "alice".into(),
            owner_id: None,
            profile: Profile::Metadata,
            target: Target::Job {
                name: format!("mrsm-{}", id.simple()),
            },
            callback_url: METADATA_ORIGIN.into(),
            credential: ExecutionCredential {
                token: METADATA_TOKEN.into(),
                expires_at: now + 20,
            },
        };
        valid.validate(&actor).unwrap();
        for deadline in [None, Some(now), Some(now + 61), Some(now + 10)] {
            let mut changed = valid.clone();
            changed.metadata_deadline = deadline;
            assert!(changed.validate(&actor).is_err(), "{deadline:?}");
        }
        for profile in [Profile::Api, Profile::Team] {
            let mut changed = valid.clone();
            changed.profile = profile;
            assert!(changed.validate(&actor).is_err());
            changed.metadata_deadline = None;
            changed.discovery_catalog_sha256 = None;
            changed.validate(&actor).unwrap();
        }
    }
    #[test]
    fn callback_origins_never_accept_login_or_routing_overrides() {
        for url in [
            "http://api:8989",
            "https://api.example/",
            "http://[::1]:8989",
        ] {
            validate_callback_url(url).unwrap();
        }
        for url in [
            "http://user:password@api",
            "http://api?token=x",
            "http://api/path",
            "file:///secret",
            "http://api:0",
            "//api",
            "http://api/#fragment",
        ] {
            assert!(validate_callback_url(url).is_err(), "{url}");
        }
    }

    #[test]
    fn credentialed_team_may_bind_an_exact_catalog_without_gaining_private_operations() {
        let now = chrono::Utc::now().timestamp();
        let actor = super::super::control::Actor {
            owner: "alice".into(),
            username: None,
            permissions: Default::default(),
            administrator: false,
        };
        let id = Uuid::new_v4();
        let mut update = Update {
            metadata_deadline: None,
            extension: None,
            registration: None,
            discovery_catalog_sha256: Some("a".repeat(64)),
            id,
            revision: 1,
            owner: "alice".into(),
            owner_id: Some("registered-alice".into()),
            profile: Profile::Team,
            target: Target::Job {
                name: format!("mrsm-{}", id.simple()),
            },
            callback_url: "https://main.example".into(),
            credential: ExecutionCredential {
                token: "synthetic-limited-callback".into(),
                expires_at: now + 20,
            },
        };
        update.validate(&actor).unwrap();
        assert!(!update.profile.private_runtime());
        update.discovery_catalog_sha256 = Some("invalid".into());
        assert!(update.validate(&actor).is_err());
        update.discovery_catalog_sha256 = Some("a".repeat(64));
        update.metadata_deadline = Some(now + 20);
        assert!(update.validate(&actor).is_err());
    }
}
