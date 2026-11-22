//! Main's receipt for metadata produced by a provisioned native compiler.

use super::PreparedExecutor;
use meerschaum_connector_api::client::{Body, Method, RequestOptions};
use meerschaum_server::executor::factory_attestation::{Pin, quote};
use meerschaum_server::executor::plugins::{Catalog, SourceKind};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Receipt {
    pin_sha256: String,
    native_receipt_sha256: String,
}

pub(super) fn pin(root: &Path, catalog: &Catalog) -> Result<Option<Pin>, String> {
    if !catalog.placement.is_remote()
        || !catalog
            .plugins
            .values()
            .any(|source| source.kind == SourceKind::NativeSource)
    {
        return Ok(None);
    }
    let pin = crate::executor::registry::Registry::new(root)
        .factory_pin(catalog.team_id)?
        .filter(|pin| {
            pin.team_id == catalog.team_id
                && pin.generation == catalog.generation
                && pin.artifacts.sdk_sha256.is_some()
                && pin.artifacts.compiler_sha256.is_some()
        })
        .ok_or("Remote Rust discovery requires this executor's administrator-provisioned SDK and compiler.")?;
    Ok(Some(pin))
}

pub(super) fn matches(receipt: Option<&Receipt>, pin_digest: Option<&str>) -> bool {
    match (receipt, pin_digest) {
        (None, None) => true,
        (Some(receipt), Some(digest)) => {
            receipt.pin_sha256 == digest
                && meerschaum_server::executor::factory_artifacts::valid_digest(
                    &receipt.native_receipt_sha256,
                )
        }
        _ => false,
    }
}

impl PreparedExecutor {
    /// Check the completed job under the same actor and configuration used to start discovery.
    pub(super) async fn verify_metadata(
        &self,
        pin: &Pin,
        context: Uuid,
        revision: u64,
        catalog_sha256: &str,
        deadline: i64,
        bytes: &[u8],
    ) -> Result<Receipt, String> {
        pin.matches_control(&self.control)?;
        let challenge = quote::Pending::new(
            pin.clone(),
            self.configuration_sha256.clone(),
            &self.actor,
            (context, revision),
            catalog_sha256.into(),
            deadline,
            chrono::Utc::now().timestamp(),
        )?;
        let reply = self
            .api
            .with_response_byte_limit(quote::MAX_RESPONSE)
            .make_request(
                Method::Post,
                quote::PATH,
                RequestOptions {
                    body: Body::Json(
                        serde_json::to_string(challenge.request()).map_err(|e| e.to_string())?,
                    ),
                    timeout: Some(Duration::from_secs(4)),
                    ..Default::default()
                },
            )
            .await
            .map_err(|_| "The executor's native metadata receipt is unavailable.")?;
        if reply.status != 200 {
            return Err("The executor could not attest its native metadata result.".into());
        }
        let verified = challenge.verify(&reply.bytes, bytes, chrono::Utc::now().timestamp())?;
        Ok(Receipt {
            pin_sha256: verified.pin_sha256().into(),
            native_receipt_sha256: verified.receipt_sha256().into(),
        })
    }
}
