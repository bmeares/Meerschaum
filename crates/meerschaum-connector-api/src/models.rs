//! Shared route and auth models: the request and response bodies of `meerschaum/api/routes`
//! (`meerschaum/api/models`, `meerschaum/models`) as serde structs, for the client here and for
//! `meerschaum-server`.
//!
//! Wire shapes are the v4.1 shapes. `SuccessTuple` serializes as `[bool, "message"]`
//! (`SuccessTupleResponseModel`), see [`meerschaum_core::SuccessTuple`].

use chrono::{DateTime, NaiveDateTime, Utc};
use indexmap::IndexMap;
use meerschaum_core::{Map, Value};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The scope every route group requires (`ScopedAuth([...])` in `meerschaum/api/routes`).
pub mod scopes {
    /// Read pipes' parameters and the contents of target tables.
    pub const PIPES_READ: &str = "pipes:read";
    /// Update pipes' parameters and sync to target tables.
    pub const PIPES_WRITE: &str = "pipes:write";
    /// Drop target tables.
    pub const PIPES_DROP: &str = "pipes:drop";
    /// Delete pipes' parameters and drop target tables.
    pub const PIPES_DELETE: &str = "pipes:delete";
    /// Execute arbitrary actions.
    pub const ACTIONS_EXECUTE: &str = "actions:execute";
    /// Read the available connectors.
    pub const CONNECTORS_READ: &str = "connectors:read";
    /// Execute read-only SQL queries against SQL connectors.
    pub const SQL_READ: &str = "sql:read";
    /// Read jobs' properties.
    pub const JOBS_READ: &str = "jobs:read";
    /// Write jobs' properties.
    pub const JOBS_WRITE: &str = "jobs:write";
    /// Run jobs.
    pub const JOBS_EXECUTE: &str = "jobs:execute";
    /// Delete jobs.
    pub const JOBS_DELETE: &str = "jobs:delete";
    /// Read jobs' logs.
    pub const LOGS_READ: &str = "logs:read";
    /// Stop running jobs.
    pub const JOBS_STOP: &str = "jobs:stop";
    /// Pause running jobs.
    pub const JOBS_PAUSE: &str = "jobs:pause";
    /// Read an instance's system-level metadata.
    pub const INSTANCE_READ: &str = "instance:read";
    /// Allow chaining API instances using the associated credentials.
    pub const INSTANCE_CHAIN: &str = "instance:chain";
    /// Register and update plugins' metadata.
    pub const PLUGINS_WRITE: &str = "plugins:write";
    /// Read attributes of registered plugins.
    pub const PLUGINS_READ: &str = "plugins:read";
    /// Delete plugins (owned by user) from the repository.
    pub const PLUGINS_DELETE: &str = "plugins:delete";
    /// Read metadata about the associated account.
    pub const USERS_READ: &str = "users:read";
    /// Write metadata for the associated account.
    pub const USERS_WRITE: &str = "users:write";
    /// Register new user accounts.
    pub const USERS_REGISTER: &str = "users:register";
    /// Delete the associated user account (or other users for admins).
    pub const USERS_DELETE: &str = "users:delete";
    /// The wildcard scope (admins and `--no-auth`).
    pub const ALL: &str = "*";

    /// Every scope name, in `STATIC_CONFIG['tokens']['scopes']` order.
    pub fn all_scopes() -> Vec<String> {
        meerschaum_core::static_config().scope_names()
    }
}

/// `ScopedAuth(scopes)`: the scopes a route requires, with the v4 grant check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScopedAuth {
    /// The required scopes (all must be granted).
    pub scopes: Vec<String>,
}

impl ScopedAuth {
    /// `ScopedAuth([...])`.
    pub fn new(scopes: &[&str]) -> ScopedAuth {
        ScopedAuth {
            scopes: scopes.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// `_authenticate()`'s check: `'*'` grants everything; otherwise every required scope must
    /// be granted. Returns the first missing scope as the 403 detail
    /// (`"Missing required scope: '{scope}'"`).
    pub fn check(&self, granted: &[String]) -> Result<(), String> {
        if granted.iter().any(|s| s == scopes::ALL) {
            return Ok(());
        }
        for scope in &self.scopes {
            if !granted.contains(scope) {
                return Err(format!("Missing required scope: '{scope}'"));
            }
        }
        Ok(())
    }
}

/// The JWT claims issued by `/login` (`manager.create_access_token(data={'sub', 'scopes',
/// 'type'}, expires=...)`, HS256 with the root directory's secret key).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JwtClaims {
    /// The username (password grant) or the token ID (client-credentials grant).
    pub sub: String,
    /// The granted scopes.
    #[serde(default)]
    pub scopes: Vec<String>,
    /// The user type (`None` for client credentials).
    #[serde(rename = "type", default)]
    pub user_type: Option<String>,
    /// The expiration as epoch seconds (added by `fastapi_login`).
    pub exp: i64,
}

/// The `/login` form (`CustomOAuth2PasswordRequestForm`), sent as
/// `application/x-www-form-urlencoded`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LoginForm {
    /// `password` or `client_credentials` (inferred when omitted).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grant_type: Option<String>,
    /// The username (password grant).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    /// The password (password grant).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    /// Space-separated requested scopes (omitted: the principal's allowed scopes).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    /// The token ID (client-credentials grant).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    /// The token secret (client-credentials grant).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_secret: Option<String>,
}

/// The `/login` response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoginResponse {
    /// The JWT.
    pub access_token: String,
    /// Always `"bearer"`.
    pub token_type: String,
    /// The naive UTC expiration (`'%Y-%m-%dT%H:%M:%S.%f'`).
    pub expires: NaiveDateTime,
}

pub use meerschaum_connector::models::TokenModel;

fn default_scopes() -> Vec<String> {
    scopes::all_scopes()
}

fn default_true() -> bool {
    true
}

/// `RegisterTokenRequestModel` (`POST /tokens/register`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterTokenRequest {
    /// The label.
    pub label: Option<String>,
    /// The expiration (`null` never expires).
    pub expiration: Option<DateTime<Utc>>,
    /// The scopes.
    pub scopes: Vec<String>,
}

/// `RegisterTokenResponseModel`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RegisterTokenResponse {
    /// The label.
    pub label: String,
    /// The secret (shown once).
    pub secret: String,
    /// The token ID.
    pub id: Uuid,
    /// The API key (`mrsm-key:` + base64 of `id:secret`).
    pub api_key: String,
    /// The expiration.
    pub expiration: Option<DateTime<Utc>>,
}

/// `GetTokenResponseModel` (`GET /tokens`, `GET /tokens/{id}`, body of `POST
/// /tokens/{id}/edit`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GetTokenResponse {
    /// The token ID.
    #[serde(default)]
    pub id: Option<Uuid>,
    /// The creation timestamp.
    pub creation: Option<DateTime<Utc>>,
    /// The expiration.
    pub expiration: Option<DateTime<Utc>>,
    /// The label.
    pub label: Option<String>,
    /// The owner's user ID.
    #[serde(default)]
    pub user_id: Option<Value>,
    /// The scopes.
    #[serde(default = "default_scopes")]
    pub scopes: Vec<String>,
    /// Whether the token is valid.
    #[serde(default = "default_true")]
    pub is_valid: bool,
}

impl From<GetTokenResponse> for TokenModel {
    fn from(r: GetTokenResponse) -> Self {
        TokenModel {
            id: r.id,
            creation: r.creation,
            expiration: r.expiration,
            label: r.label,
            user_id: r.user_id,
            secret_hash: None,
            scopes: r.scopes,
            is_valid: r.is_valid,
        }
    }
}

/// `UserModel` (`meerschaum/models/users.py`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserModel {
    /// The user ID.
    #[serde(default)]
    pub user_id: Option<Value>,
    /// The username.
    pub username: String,
    /// The email.
    #[serde(default)]
    pub email: Option<String>,
    /// The user type.
    #[serde(rename = "type", default)]
    pub user_type: Option<String>,
    /// The attributes.
    #[serde(default)]
    pub attributes: Option<Map>,
}

/// `GET /users/me`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CurrentUserResponse {
    /// The username (`'no_auth'` without authentication).
    pub username: String,
    /// The user ID (UUIDs as strings).
    pub user_id: Option<Value>,
    /// The user type (`'admin'` without authentication).
    pub user_type: Option<String>,
    /// The attributes.
    pub attributes: Option<Map>,
}

/// The `/users/register` and `/users/edit` forms (`application/x-www-form-urlencoded`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UserForm {
    /// The username.
    pub username: String,
    /// The plaintext password.
    pub password: String,
    /// The user type.
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub user_type: Option<String>,
    /// The email.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    /// `json.dumps(attributes)`.
    pub attributes: String,
}

/// A job's metadata (`GET /jobs`, `GET /jobs/{name}`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobMetadata {
    /// The job's arguments.
    pub sysargs: Vec<String>,
    /// The last result, if the job has stopped.
    #[serde(default)]
    pub result: Option<Value>,
    /// Whether the job restarts.
    #[serde(default)]
    pub restart: bool,
    /// `running`, `paused` or `stopped`.
    #[serde(default)]
    pub status: String,
    /// The daemon's status, PID and properties.
    #[serde(default)]
    pub daemon: JobDaemon,
}

/// The `daemon` part of [`JobMetadata`].
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct JobDaemon {
    /// The daemon status.
    #[serde(default)]
    pub status: Option<String>,
    /// The process ID.
    #[serde(default)]
    pub pid: Option<i64>,
    /// The daemon properties (`daemon.began`, `daemon.ended`, `logs`, ...).
    #[serde(default)]
    pub properties: Map,
}

/// The body of `POST /jobs/{name}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CreateJobRequest {
    /// The job's arguments.
    pub sysargs: Vec<String>,
    /// Extra daemon properties.
    pub properties: Option<Map>,
}

/// `GET /info`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InstanceInfo {
    /// The Meerschaum version.
    pub version: String,
    /// The number of registered plugins.
    pub num_plugins: i64,
    /// The number of registered users.
    pub num_users: i64,
    /// The number of registered pipes.
    pub num_pipes: i64,
}

/// `GET /healthcheck` (status 200 or 503).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Healthcheck {
    /// Whether the instance connector is reachable.
    pub success: bool,
    /// `"Success"` or the failure message.
    pub message: String,
}

/// `GET /id` (debug servers only).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerIds {
    /// The server ID (`MRSM_SERVER_ID`).
    pub server: String,
    /// The worker process ID.
    pub process: i64,
}

/// A FastAPI error body (`{"detail": ...}`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorDetail {
    /// The detail (a string, or a list of validation errors).
    pub detail: Value,
}

// --------------------------------------------------------------------------------------------
// Engine state (§5.8): the `/engine/*` route family. New in v5: there is no v4 counterpart, so
// these shapes are the contract between this client and `meerschaum-server`. Every route
// requires the `pipes:write` scope.
// --------------------------------------------------------------------------------------------

/// A dirty-range ledger entry (`POST /engine/ledger`, `GET /engine/ledger`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerEntry {
    /// The parent pipe (`"{instance}|{ck}|{mk}|{lk}"`).
    pub pipe_ref: String,
    /// The per-pipe sequence number.
    pub seq: i64,
    /// When the entry was written.
    pub created: DateTime<Utc>,
    /// The changed interval's lower bound (an ISO timestamp, an int, or `null`).
    #[serde(default)]
    pub begin: Value,
    /// The changed interval's upper bound.
    #[serde(default)]
    pub end: Value,
}

/// Query of `GET /engine/ledger`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LedgerQuery {
    /// The child whose parents' entries to read.
    pub child_ref: String,
    /// Only entries with `seq > after_seq`.
    #[serde(default)]
    pub after_seq: Option<i64>,
}

/// An edge cursor (`PUT /engine/cursors`): primary key `(parent_ref, child_ref)`, upsert.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EdgeCursor {
    /// The parent pipe.
    pub parent_ref: String,
    /// The child pipe.
    pub child_ref: String,
    /// The last consumed ledger sequence number.
    pub seq: i64,
    /// The parent's watermark when the cursor advanced.
    #[serde(default)]
    pub parent_watermark_at_cursor: Value,
}

/// A pipe's persisted engine state (`GET`/`PUT /engine/state`): primary key `pipe_ref`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EngineState {
    /// The pipe.
    pub pipe_ref: String,
    /// The definition hash.
    #[serde(default)]
    pub definition_hash: Option<String>,
    /// Other state fields.
    #[serde(flatten)]
    pub extra: IndexMap<String, Value>,
}

/// A run or per-node result (`POST`/`GET /engine/runs`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    /// The run ID.
    pub run_id: String,
    /// The node (`None` for the run itself).
    #[serde(default)]
    pub pipe_ref: Option<String>,
    /// When the run (or node) started.
    pub started: DateTime<Utc>,
    /// When it finished.
    #[serde(default)]
    pub finished: Option<DateTime<Utc>>,
    /// The status (`success`, `failed`, `skipped`, `upstream_failed`, ...).
    pub status: String,
    /// Other fields (`SyncResult` counts, message, ...).
    #[serde(flatten)]
    pub extra: IndexMap<String, Value>,
}

/// A run event (`POST /engine/events`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EngineEvent {
    /// The run ID.
    pub run_id: String,
    /// When the event happened.
    pub created: DateTime<Utc>,
    /// The event type (`run_started`, `node_finished`, ...).
    pub event: String,
    /// The node, if any.
    #[serde(default)]
    pub pipe_ref: Option<String>,
    /// The event payload.
    #[serde(default)]
    pub payload: Value,
}

/// One event of `GET /engine/runs/{run_id}/events/stream` (the SSE `data:`; the SSE `event:` is
/// [`event`](Self::event) and the SSE `id:` is [`seq`](Self::seq)).
///
/// The shape is the engine's typed event (`meerschaum_engine::Event`): the envelope (`run_id`,
/// `seq`, `ts`, `pipe_ref`, `event`) with the kind's fields flattened in, so
/// `serde_json::from_value::<meerschaum_engine::Event>` reads it back.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EngineRunEvent {
    /// The run ID.
    pub run_id: String,
    /// The per-run sequence number (the SSE `id`, and what `Last-Event-ID` resumes after).
    pub seq: i64,
    /// When the event happened.
    pub ts: DateTime<Utc>,
    /// The node, for node-level events.
    #[serde(default)]
    pub pipe_ref: Option<String>,
    /// The event name (`run_started`, `chunk_finished`, ..., `run_finished`).
    pub event: String,
    /// The kind's own fields (`action`, `chunk`, `rows`, `result`, ...).
    #[serde(flatten)]
    pub fields: IndexMap<String, serde_json::Value>,
}

impl EngineRunEvent {
    /// An event from a persisted row's columns; `payload` is the kind's JSON (an object whose
    /// `event` tag, if any, is dropped in favor of `event`).
    pub fn from_row(
        run_id: &str,
        seq: i64,
        ts: DateTime<Utc>,
        pipe_ref: Option<String>,
        event: &str,
        payload: serde_json::Value,
    ) -> EngineRunEvent {
        let payload = match payload {
            serde_json::Value::String(s) => serde_json::from_str(&s).unwrap_or_default(),
            other => other,
        };
        let mut fields = IndexMap::new();
        if let serde_json::Value::Object(map) = payload {
            for (k, v) in map {
                if !["event", "run_id", "seq", "ts", "pipe_ref"].contains(&k.as_str()) {
                    fields.insert(k, v);
                }
            }
        }
        EngineRunEvent {
            run_id: run_id.to_string(),
            seq,
            ts,
            pipe_ref,
            event: event.to_string(),
            fields,
        }
    }

    /// Whether this is the run's last event (`run_finished`).
    pub fn is_run_finished(&self) -> bool {
        self.event == "run_finished"
    }

    /// Whether this is the server's synthetic `run_closed`: the stream ends because the run
    /// will not send `run_finished` (`reason`: `interrupted`, `holder_gone` or `idle`).
    pub fn is_run_closed(&self) -> bool {
        self.event == "run_closed"
    }

    /// Whether a stream ends after this event (`run_finished` or `run_closed`).
    pub fn is_terminal(&self) -> bool {
        self.is_run_finished() || self.is_run_closed()
    }
}

/// Body of `POST /engine/leases` (acquire or extend) and `DELETE /engine/leases` (release;
/// `ttl_ms` ignored).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaseRequest {
    /// The pipe.
    pub pipe_ref: String,
    /// The holder (engine instance ID).
    pub holder: String,
    /// The time to live in milliseconds.
    #[serde(default)]
    pub ttl_ms: u64,
}

/// Response of the `/engine/leases` routes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LeaseResponse {
    /// Whether the holder now holds (acquire) or released (release) the lease.
    pub success: bool,
}

/// Parse a JSON body as a `SuccessTuple` the way the Python client does: a list
/// `[success, message]`, or `{"detail": ...}` (with the response's truthiness), or the raw text.
pub fn parse_success_tuple(ok: bool, text: &str) -> meerschaum_core::SuccessTuple {
    match serde_json::from_str::<serde_json::Value>(text) {
        Ok(serde_json::Value::Array(items)) if items.len() >= 2 => {
            let success = items[0].as_bool().unwrap_or(false);
            let message = match &items[1] {
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            meerschaum_core::SuccessTuple { success, message }
        }
        Ok(serde_json::Value::Object(o)) if o.contains_key("detail") => {
            let message = match &o["detail"] {
                serde_json::Value::String(s) => s.clone(),
                other => detail_repr(other),
            };
            meerschaum_core::SuccessTuple {
                success: ok,
                message,
            }
        }
        _ => meerschaum_core::SuccessTuple {
            success: ok,
            message: text.to_string(),
        },
    }
}

/// Python's `str()` of a non-string `detail` value (validation-error lists).
pub fn detail_repr(v: &serde_json::Value) -> String {
    match Value::from_json_str(&v.to_string()) {
        Ok(cv) => meerschaum_core::pyfmt::py_repr(&cv),
        Err(_) => v.to_string(),
    }
}
