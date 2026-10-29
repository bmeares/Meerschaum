//! Models shared by instance connectors (`meerschaum/models/tokens.py`).

use chrono::{DateTime, Utc};
use meerschaum_core::Value;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Every scope name, in `STATIC_CONFIG['tokens']['scopes']` order.
pub fn all_scopes() -> Vec<String> {
    meerschaum_core::static_config().scope_names()
}

fn default_scopes() -> Vec<String> {
    all_scopes()
}

fn default_true() -> bool {
    true
}

/// `TokenModel` (`meerschaum/models/tokens.py`): a token as stored on an instance.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TokenModel {
    /// The token (client) ID.
    #[serde(default)]
    pub id: Option<Uuid>,
    /// The creation timestamp.
    #[serde(default)]
    pub creation: Option<DateTime<Utc>>,
    /// The expiration timestamp (`None` never expires).
    #[serde(default)]
    pub expiration: Option<DateTime<Utc>>,
    /// The label.
    #[serde(default)]
    pub label: Option<String>,
    /// The owner's user ID (an int, a UUID or a string).
    #[serde(default)]
    pub user_id: Option<Value>,
    /// The secret hash (never sent over the API).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret_hash: Option<String>,
    /// The scopes (all scopes when unset).
    #[serde(default = "default_scopes")]
    pub scopes: Vec<String>,
    /// Whether the token is valid.
    #[serde(default = "default_true")]
    pub is_valid: bool,
}
