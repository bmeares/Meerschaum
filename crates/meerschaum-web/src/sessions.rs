//! Shared session wire format. The instance fallback follows the internal users-pipe pattern.
use async_trait::async_trait;
use chrono::Utc;
use meerschaum_connector::{DataQuery, InstanceConnector, SyncOptions};
use meerschaum_core::{Map, Pipe, Value};
use meerschaum_kernel::{Frame, value::Value as KValue};
use serde_json::{Value as Json, json};
use std::sync::Arc;

// PARITY: api/dash/sessions.py uses a root-scoped UUID cookie and a 30-day TTL.
pub const COOKIE: &str = "mrsm-session-id";
pub const EXPIRES_SECONDS: i64 = 43200 * 60;
pub const KEY_PREFIX: &str = "mrsm_session_id:";

/// Cache adapters must use `mrsm_session_id:{id}`, compact JSON, and the supplied TTL.
#[async_trait]
pub trait SessionStore: Send + Sync {
    async fn get(&self, id: &str) -> Result<Option<Json>, String>;
    async fn set(&self, id: &str, payload: Json, ttl: i64) -> Result<(), String>;
    async fn delete(&self, id: &str) -> Result<(), String>;
}

/// Rows: session_id (primary key), data (compact JSON string), expires (Unix seconds).
/// This store never uses a process-local session cache.
pub struct InstanceSessions {
    instance: Arc<dyn InstanceConnector>,
    pipe: Pipe,
    writes: tokio::sync::Mutex<()>,
}
impl InstanceSessions {
    pub fn new(instance: Arc<dyn InstanceConnector>) -> Self {
        let parameters: Map = serde_json::from_value(json!({
            "target":"mrsm_sessions", "columns":{"primary":"session_id"},
            "dtypes":{"session_id":"str", "data":"str", "expires":"int"},
            "upsert":true, "hypertable":false
        }))
        .expect("static parameters");
        let pipe = Pipe::builder("mrsm", "sessions")
            .instance(&instance.connector_keys())
            .kwarg("temporary", true)
            .parameters(parameters)
            .build()
            .expect("session pipe");
        Self {
            instance,
            pipe,
            writes: tokio::sync::Mutex::new(()),
        }
    }
    fn query(id: &str) -> DataQuery {
        DataQuery {
            params: Some(Map::from_iter([(
                "session_id".into(),
                Value::Str(id.into()),
            )])),
            ..Default::default()
        }
    }
}
#[async_trait]
impl SessionStore for InstanceSessions {
    async fn get(&self, id: &str) -> Result<Option<Json>, String> {
        let rows = self
            .instance
            .get_pipe_docs(&self.pipe, &Self::query(id))
            .await
            .map_err(|e| e.to_string())?;
        let Some(row) = rows.first() else {
            return Ok(None);
        };
        let expires = row
            .get("expires")
            .and_then(|v| v.py_str().parse::<i64>().ok())
            .unwrap_or(0);
        if expires <= Utc::now().timestamp() {
            return Ok(None);
        }
        Ok(row
            .get("data")
            .and_then(|v| serde_json::from_str(&v.py_str()).ok()))
    }
    async fn set(&self, id: &str, payload: Json, ttl: i64) -> Result<(), String> {
        let _lock = self.writes.lock().await;
        let df = Frame::from_object_columns(vec![
            ("session_id".into(), vec![KValue::Str(id.into())]),
            ("data".into(), vec![KValue::Str(payload.to_string())]),
            (
                "expires".into(),
                vec![KValue::Int(Utc::now().timestamp() + ttl)],
            ),
        ])
        .map_err(|e| e.to_string())?;
        let result = self
            .instance
            .sync_pipe(&self.pipe, df, &SyncOptions::default())
            .await;
        if result.success {
            Ok(())
        } else {
            Err(result.message)
        }
    }
    async fn delete(&self, id: &str) -> Result<(), String> {
        let result = self.instance.clear_pipe(&self.pipe, &Self::query(id)).await;
        if result.success {
            Ok(())
        } else {
            Err(result.message)
        }
    }
}

/// Exact v4 cache protocol, for deployments configuring a Valkey cache connector.
pub struct ValkeySessions(pub Arc<dyn meerschaum_connector_valkey::backend::ValkeyBackend>);
#[async_trait]
impl SessionStore for ValkeySessions {
    async fn get(&self, id: &str) -> Result<Option<Json>, String> {
        self.0
            .get(&format!("{KEY_PREFIX}{id}"))
            .await
            .map_err(|e| e.to_string())?
            .map(|raw| serde_json::from_str(&raw).map_err(|e| e.to_string()))
            .transpose()
    }
    async fn set(&self, id: &str, payload: Json, ttl: i64) -> Result<(), String> {
        self.0
            .set_ex(&format!("{KEY_PREFIX}{id}"), &payload.to_string(), ttl)
            .await
            .map_err(|e| e.to_string())
    }
    async fn delete(&self, id: &str) -> Result<(), String> {
        self.0
            .del(&[format!("{KEY_PREFIX}{id}")])
            .await
            .map(|_| ())
            .map_err(|e| e.to_string())
    }
}
