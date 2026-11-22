//! Single-use Main challenge and independent Factory response signature.
//! Native MetadataResult cannot be decoded from an HTTP response or job properties.

use super::{Authority, Pin};
use crate::executor::control::Actor;
use crate::executor::factory_artifacts::{digest, valid_digest};
use aws_lc_rs::signature::{ED25519, UnparsedPublicKey};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use meerschaum_jobs::supervisor::MetadataResult;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

const DOMAIN: &[u8] = b"mrsm-native-factory-metadata-result-v1\0";
pub const PATH: &str = "/_mrsm/executor/metadata-proof";
pub const MAX_RESPONSE: usize = 16 * 1024;
pub const MAX_REQUEST: usize = 2048;
const MAX_PAYLOAD: usize = 8 * 1024;
const LIFETIME: i64 = 30;
const MAX_DEADLINE: i64 = 60;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub challenge: Uuid,
    pub context: Uuid,
    pub revision: u64,
    pub catalog_sha256: String,
    pub deadline: i64,
}
impl Request {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.challenge.is_nil()
            || self.context.is_nil()
            || self.revision == 0
            || !valid_digest(&self.catalog_sha256)
            || self.deadline <= 0
        {
            return Err(refused());
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Response {
    payload: String,
    signature: String,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    schema: u8,
    pin_sha256: String,
    request_sha256: String,
    configuration_sha256: String,
    actor_sha256: String,
    context_identity_sha256: String,
    native_receipt_sha256: String,
    native_launch_id: String,
    message_sha256: String,
    message_bytes: usize,
    completed_at: i64,
    issued_at: i64,
    expires_at: i64,
}

fn refused() -> &'static str {
    "The native Factory metadata proof is unavailable, mismatched or already consumed."
}
pub fn pin_digest(pin: &Pin) -> Result<String, &'static str> {
    pin.validate()?;
    Ok(digest(&serde_json::to_vec(pin).map_err(|_| refused())?))
}
fn actor_digest(actor: &Actor) -> Result<String, &'static str> {
    Ok(digest(&serde_json::to_vec(actor).map_err(|_| refused())?))
}

impl Authority {
    /// The native Factory must first confirm the exact sealed Metadata Context,
    /// source catalog, original actor, SDK/compiler bindings and configuration.
    /// The result itself is always read from the native supervisor's receipt.
    pub fn metadata_quote(
        &self,
        request: &Request,
        configuration: &str,
        actor: &Actor,
        completed: &MetadataResult,
        now: i64,
    ) -> Result<Response, &'static str> {
        request.validate()?;
        let generation = completed.generation();
        if !valid_digest(configuration)
            || request
                .deadline
                .checked_sub(now)
                .is_none_or(|remaining| remaining <= 0 || remaining > MAX_DEADLINE)
            || generation.context != request.context.simple().to_string()
            || generation.revision != request.revision
            || generation.owner != actor.owner
            || completed.deadline() != request.deadline
            || completed.completed_at() > now
            || !valid_digest(&generation.identity)
        {
            return Err(refused());
        }
        let payload = Payload {
            schema: 1,
            pin_sha256: pin_digest(&self.pin)?,
            request_sha256: digest(&serde_json::to_vec(request).map_err(|_| refused())?),
            configuration_sha256: configuration.into(),
            actor_sha256: actor_digest(actor)?,
            context_identity_sha256: generation.identity.clone(),
            native_receipt_sha256: completed.receipt_sha256().into(),
            native_launch_id: completed.launch_id().into(),
            message_sha256: completed.message_sha256().into(),
            message_bytes: completed.message_bytes(),
            completed_at: completed.completed_at(),
            issued_at: now,
            expires_at: now
                .checked_add(LIFETIME)
                .ok_or_else(refused)?
                .min(request.deadline),
        };
        let bytes = serde_json::to_vec(&payload).map_err(|_| refused())?;
        if bytes.len() > MAX_PAYLOAD {
            return Err(refused());
        }
        let mut signed = DOMAIN.to_vec();
        signed.extend(&bytes);
        Ok(Response {
            payload: URL_SAFE_NO_PAD.encode(bytes),
            signature: URL_SAFE_NO_PAD.encode(self.key.sign(&signed).as_ref()),
        })
    }
}

/// Main-owned operation state. Neither Clone nor Deserialize; its nonce cannot be
/// supplied by a response. A bad response does not consume the pending challenge.
pub struct Pending {
    request: Request,
    pin: Pin,
    configuration: String,
    actor_sha256: String,
    created_at: i64,
    consumed: AtomicBool,
}
pub struct Verified {
    pin_sha256: String,
    receipt_sha256: String,
}
impl Verified {
    pub fn pin_sha256(&self) -> &str {
        &self.pin_sha256
    }
    pub fn receipt_sha256(&self) -> &str {
        &self.receipt_sha256
    }
}

impl Pending {
    pub fn new(
        pin: Pin,
        configuration: String,
        actor: &Actor,
        generation: (Uuid, u64),
        catalog_sha256: String,
        deadline: i64,
        now: i64,
    ) -> Result<Self, &'static str> {
        pin.validate()?;
        let request = Request {
            challenge: Uuid::new_v4(),
            context: generation.0,
            revision: generation.1,
            catalog_sha256,
            deadline,
        };
        request.validate()?;
        if !valid_digest(&configuration)
            || now <= 0
            || deadline
                .checked_sub(now)
                .is_none_or(|remaining| remaining <= 0 || remaining > MAX_DEADLINE)
        {
            return Err(refused());
        }
        Ok(Self {
            request,
            pin,
            configuration,
            actor_sha256: actor_digest(actor)?,
            created_at: now,
            consumed: AtomicBool::new(false),
        })
    }
    pub fn request(&self) -> &Request {
        &self.request
    }
    pub fn verify(
        &self,
        encoded: &[u8],
        native_message: &[u8],
        now: i64,
    ) -> Result<Verified, &'static str> {
        if encoded.len() > MAX_RESPONSE
            || native_message.len() > crate::executor::plugins::MAX_CATALOG_BYTES
            || self.request.deadline <= now
            || self.consumed.load(Ordering::Acquire)
        {
            return Err(refused());
        }
        let response: Response = serde_json::from_slice(encoded).map_err(|_| refused())?;
        if response.payload.len() > MAX_PAYLOAD * 2 || response.signature.len() > 128 {
            return Err(refused());
        }
        let bytes = URL_SAFE_NO_PAD
            .decode(&response.payload)
            .map_err(|_| refused())?;
        if bytes.len() > MAX_PAYLOAD {
            return Err(refused());
        }
        let signature = URL_SAFE_NO_PAD
            .decode(&response.signature)
            .map_err(|_| refused())?;
        if signature.len() != 64 {
            return Err(refused());
        }
        let public = URL_SAFE_NO_PAD
            .decode(&self.pin.public_key)
            .map_err(|_| refused())?;
        let mut signed = DOMAIN.to_vec();
        signed.extend(&bytes);
        UnparsedPublicKey::new(&ED25519, public)
            .verify(&signed, &signature)
            .map_err(|_| refused())?;
        let payload: Payload = serde_json::from_slice(&bytes).map_err(|_| refused())?;
        if payload.schema != 1
            || payload.pin_sha256 != pin_digest(&self.pin)?
            || payload.request_sha256
                != digest(&serde_json::to_vec(&self.request).map_err(|_| refused())?)
            || payload.configuration_sha256 != self.configuration
            || payload.actor_sha256 != self.actor_sha256
            || payload.message_sha256 != digest(native_message)
            || payload.message_bytes != native_message.len()
            || !valid_digest(&payload.context_identity_sha256)
            || !valid_digest(&payload.native_receipt_sha256)
            || payload.native_launch_id.is_empty()
            || payload.native_launch_id.len() > 128
            || payload.native_launch_id.chars().any(char::is_control)
            || payload.completed_at > payload.issued_at
            || payload.completed_at <= 0
            || payload.issued_at
                < self
                    .created_at
                    .saturating_sub(super::super::control::CLOCK_SKEW_SECONDS)
            || payload.issued_at > now.saturating_add(super::super::control::CLOCK_SKEW_SECONDS)
            || payload.expires_at <= now
            || payload.expires_at > self.request.deadline
            || payload.expires_at <= payload.issued_at
            || payload.expires_at - payload.issued_at > LIFETIME
        {
            return Err(refused());
        }
        self.consumed
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .map_err(|_| refused())?;
        Ok(Verified {
            pin_sha256: payload.pin_sha256,
            receipt_sha256: payload.native_receipt_sha256,
        })
    }
}

#[cfg(test)]
mod tests;
