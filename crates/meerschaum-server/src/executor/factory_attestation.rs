//! Administrator-provisioned reverse identity for the native executor Factory.
//!
//! This is separate from Main's request signing key. The public pin is installed
//! out of band from verified image bytes. Neither a response body, a caller's bearer,
//! job properties nor a worker environment can select or establish this identity.

use aws_lc_rs::pkcs8::Document;
use aws_lc_rs::rand::SystemRandom;
use aws_lc_rs::signature::{Ed25519KeyPair, KeyPair};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::control::Verifier;
use super::factory_artifacts::{Identity, digest, valid_digest};

pub const PRIVATE_PATH: &str = "/run/secrets/mrsm-factory-attestation.json";
pub const PUBLIC_DIRECTORY: &str = "/etc/meerschaum/executor-factory-pins";
pub const MAX_KEY_FILE: usize = 16 * 1024;

#[path = "factory_quote.rs"]
pub mod quote;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Pin {
    pub schema: u8,
    pub issuer: Uuid,
    pub team_id: Uuid,
    pub generation: Uuid,
    /// Exact public Main request key, independently bound during administrator provisioning.
    pub control_public_key_sha256: String,
    /// Docker's immutable image content ID. Manual protected installations may use None.
    pub image_sha256: Option<String>,
    pub artifacts: Identity,
    pub public_key: String,
}

fn refused() -> &'static str {
    "The administrator-provisioned native Factory identity is invalid."
}

fn control_key_digest(control: &Verifier) -> Result<String, &'static str> {
    control.validate().map_err(|_| refused())?;
    let bytes = URL_SAFE_NO_PAD
        .decode(&control.public_key)
        .map_err(|_| refused())?;
    if bytes.len() != 32 {
        return Err(refused());
    }
    Ok(digest(&bytes))
}

impl Pin {
    pub fn validate(&self) -> Result<(), &'static str> {
        self.artifacts.validate()?;
        if self.schema != 1
            || self.issuer.is_nil()
            || self.team_id.is_nil()
            || self.generation.is_nil()
            || !valid_digest(&self.control_public_key_sha256)
            || self
                .image_sha256
                .as_deref()
                .is_some_and(|image| !image.strip_prefix("sha256:").is_some_and(valid_digest))
            || URL_SAFE_NO_PAD
                .decode(&self.public_key)
                .map_err(|_| refused())?
                .len()
                != 32
        {
            return Err(refused());
        }
        Ok(())
    }

    pub fn matches_control(&self, control: &Verifier) -> Result<(), &'static str> {
        self.validate()?;
        control.validate().map_err(|_| refused())?;
        if self.issuer != control.issuer
            || self.team_id != control.team_id
            || self.generation != control.generation
            || self.control_public_key_sha256 != control_key_digest(control)?
            || self.public_key == control.public_key
        {
            return Err(refused());
        }
        Ok(())
    }
}

/// Constructor-owned signing material. It is never serialized or available to a child.
/// Loading this key alone does not prove installed artifact bytes: the native Factory
/// constructor must separately verify its fixed installation against the exact Pin.
pub struct Authority {
    key: Ed25519KeyPair,
    pin: Pin,
}

impl std::fmt::Debug for Authority {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NativeFactoryAuthority")
            .field("pin", &self.pin)
            .finish_non_exhaustive()
    }
}

impl Authority {
    /// Administrator provisioning only; the caller persists the private document separately
    /// from the public pin. This never reuses Main's request key or a request credential.
    pub fn generate(
        control: &Verifier,
        image_sha256: Option<String>,
        artifacts: Identity,
    ) -> Result<(Self, Document), &'static str> {
        let document =
            Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).map_err(|_| refused())?;
        let key = Ed25519KeyPair::from_pkcs8(document.as_ref()).map_err(|_| refused())?;
        let pin = Pin {
            schema: 1,
            issuer: control.issuer,
            team_id: control.team_id,
            generation: control.generation,
            control_public_key_sha256: control_key_digest(control)?,
            image_sha256,
            artifacts,
            public_key: URL_SAFE_NO_PAD.encode(key.public_key().as_ref()),
        };
        pin.matches_control(control)?;
        Ok((Self { key, pin }, document))
    }

    pub fn from_pkcs8(control: &Verifier, pin: Pin, secret: &[u8]) -> Result<Self, &'static str> {
        pin.matches_control(control)?;
        let key = Ed25519KeyPair::from_pkcs8(secret).map_err(|_| refused())?;
        if URL_SAFE_NO_PAD.encode(key.public_key().as_ref()) != pin.public_key {
            return Err(refused());
        }
        Ok(Self { key, pin })
    }

    pub fn pin(&self) -> &Pin {
        &self.pin
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn artifacts() -> Identity {
        Identity {
            release: env!("CARGO_PKG_VERSION").into(),
            architecture: "x86_64".into(),
            manifest_sha256: "a".repeat(64),
            cli_sha256: "b".repeat(64),
            sdk_sha256: None,
            compiler_sha256: Some("c".repeat(64)),
        }
    }
    #[test]
    fn factory_key_is_independent_and_binds_only_one_control_generation() {
        let (main, _) = super::super::control::Authority::generate(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        )
        .unwrap();
        let control = main.verifier();
        let (factory, secret) = Authority::generate(
            &control,
            Some(format!("sha256:{}", "d".repeat(64))),
            artifacts(),
        )
        .unwrap();
        assert_ne!(factory.pin().public_key, control.public_key);
        Authority::from_pkcs8(&control, factory.pin().clone(), secret.as_ref()).unwrap();
        let (another_main, _) = super::super::control::Authority::generate(
            control.issuer,
            control.team_id,
            control.generation,
        )
        .unwrap();
        assert!(
            Authority::from_pkcs8(
                &another_main.verifier(),
                factory.pin().clone(),
                secret.as_ref(),
            )
            .is_err(),
            "Matching IDs cannot substitute another Main request key."
        );
        Authority::from_pkcs8(&control, factory.pin().clone(), secret.as_ref()).unwrap();
        let mut other = control.clone();
        other.generation = Uuid::new_v4();
        assert!(Authority::from_pkcs8(&other, factory.pin().clone(), secret.as_ref()).is_err());
        let mut wrong = factory.pin().clone();
        wrong.team_id = Uuid::new_v4();
        assert!(Authority::from_pkcs8(&control, wrong, secret.as_ref()).is_err());
    }
    #[test]
    fn another_response_key_and_reusing_main_request_key_are_both_refused() {
        let (main, main_secret) = super::super::control::Authority::generate(
            Uuid::new_v4(),
            Uuid::new_v4(),
            Uuid::new_v4(),
        )
        .unwrap();
        let control = main.verifier();
        let (factory, _) = Authority::generate(&control, None, artifacts()).unwrap();
        let (_, another) = Authority::generate(&control, None, artifacts()).unwrap();
        assert!(Authority::from_pkcs8(&control, factory.pin().clone(), another.as_ref()).is_err());
        let mut wrong = factory.pin().clone();
        wrong.public_key = control.public_key.clone();
        assert!(Authority::from_pkcs8(&control, wrong, main_secret.as_ref()).is_err());
    }
}
