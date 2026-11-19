//! Native-only description of an actual configured API transport.
//!
//! No Serialize/Deserialize and no public request adapter. A restricted logical callback
//! connector must return None; its caller bearer is never a configured service credential.

use meerschaum_core::Map;
use sha2::{Digest, Sha256};
use std::fmt::{Debug, Formatter};

#[derive(Clone)]
pub enum ServiceCredential {
    /// Only usable if the native Main itself is currently configured without authentication.
    None,
    Password {
        username: String,
        password: String,
    },
    ApiKey {
        value: String,
    },
    Client {
        id: String,
        secret: String,
    },
}
impl Debug for ServiceCredential {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("ConfiguredApiServiceCredential([private])")
    }
}

/// Returned only by the actual configured native APIConnector implementation. This is input
/// to a separate trusted AppState native-self binding; URL equality alone is not authority.
#[derive(Clone)]
pub struct NativeApiTarget {
    pub logical: String,
    pub origin: String,
    pub instance: Option<String>,
    pub credential: ServiceCredential,
    pub definition_sha256: String,
}
impl Debug for NativeApiTarget {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ConfiguredNativeApiTarget")
            .field("logical", &self.logical)
            .finish_non_exhaustive()
    }
}

impl NativeApiTarget {
    /// Host-side description of the actual normalized configured transport. Constructing
    /// this description does not create a self-API binding or authorize a native operation.
    pub fn configured(
        logical: String,
        origin: String,
        instance: Option<String>,
        credential: ServiceCredential,
        resolved_attributes: &Map,
    ) -> Option<Self> {
        if !logical.starts_with("api:")
            || logical.len() <= 4
            || logical.len() > 256
            || logical.chars().any(char::is_control)
            || logical.contains('|')
        {
            return None;
        }
        let bytes = serde_json::to_vec(resolved_attributes).ok()?;
        if bytes.len() > 64 * 1024 {
            return None;
        }
        let mut digest = Sha256::new();
        digest.update(b"mrsm-configured-api-target-v1\0");
        digest.update(&bytes);
        let definition_sha256 = digest
            .finalize()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        Some(Self {
            logical,
            origin,
            instance,
            credential,
            definition_sha256,
        })
    }
}
