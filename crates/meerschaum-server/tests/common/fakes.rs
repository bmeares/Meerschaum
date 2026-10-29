//! Backends that record or fail, for the tests of the v5 routes and MCP tools.

#![allow(dead_code)]

use std::sync::Mutex;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use indexmap::IndexMap;
use meerschaum_connector_api::models::{
    EdgeCursor, EngineEvent, EngineState, LedgerEntry, RunRecord,
};
use meerschaum_core::{Map, SuccessTuple, Value};
use meerschaum_server::backends::{
    CreateJob, EngineBackend, EngineStoreError, JobsBackend, LogMonitor, PipelinesBackend,
    PlanError, PlanOptions, RunsFilter,
};
use serde_json::{Value as Json, json};

/// A job created through [`RecordingJobs`]: `(name, sysargs, properties)`.
pub type CreatedJob = (String, Vec<String>, Option<Map>);

/// Jobs that remember what they were asked to start.
#[derive(Default)]
pub struct RecordingJobs {
    /// Every job created.
    pub created: Mutex<Vec<CreatedJob>>,
}

#[async_trait]
impl JobsBackend for RecordingJobs {
    async fn list(&self) -> IndexMap<String, Json> {
        self.created
            .lock()
            .unwrap()
            .iter()
            .map(|(name, sysargs, properties)| {
                (
                    name.clone(),
                    json!({"sysargs": sysargs, "status": "running", "result": [true, "ok"],
                        "restart": false,
                        "daemon": {"properties": properties.clone().map(Value::Map)}}),
                )
            })
            .collect()
    }
    async fn get(&self, name: &str) -> Option<Json> {
        self.list().await.get(name).cloned()
    }
    async fn exists(&self, name: &str) -> bool {
        self.get(name).await.is_some()
    }
    async fn create(&self, name: &str, sysargs: Vec<String>, properties: Option<Map>) -> CreateJob {
        self.created
            .lock()
            .unwrap()
            .push((name.into(), sysargs, properties));
        CreateJob::Started(SuccessTuple::ok("Started."))
    }
    async fn delete(&self, _: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn start(&self, _: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn stop(&self, _: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn pause(&self, _: &str) -> SuccessTuple {
        SuccessTuple::success()
    }
    async fn logs(&self, _: &str) -> Option<String> {
        Some("one\ntwo\nthree\n".into())
    }
    async fn stop_time(&self, _: &str) -> Option<DateTime<Utc>> {
        None
    }
    async fn is_blocking_on_stdin(&self, _: &str) -> bool {
        false
    }
    async fn prompt_kwargs(&self, _: &str) -> Map {
        Map::new()
    }
    async fn monitor_logs(&self, _: &str) -> Option<LogMonitor> {
        None
    }
}

/// A planner that plans `daily` and refuses `broken`.
pub struct FakePlans;

#[async_trait]
impl PipelinesBackend for FakePlans {
    async fn plan(
        &self,
        name: &str,
        registry: &str,
        options: &PlanOptions,
    ) -> Result<Json, PlanError> {
        if name == "broken" {
            return Err(PlanError::Invalid(
                "Missing registered Pipeline member: sql:a|sql:b|raw|None".into(),
            ));
        }
        Ok(json!({
            "plan": {"nodes": [{"pipe_ref": "sql:fixture|sql:b|raw|None", "action": "sync"}]},
            "text": format!("plan of {name} on {registry} from {:?}", options.begin),
            "warnings": [],
        }))
    }
}

/// An engine store that cannot be read.
pub struct BrokenStore;

#[async_trait]
impl EngineBackend for BrokenStore {
    async fn append_ledger(&self, _: Vec<LedgerEntry>) -> SuccessTuple {
        SuccessTuple::fail("down")
    }
    async fn ledger_after(&self, _: &str, _: Option<i64>) -> Vec<LedgerEntry> {
        Vec::new()
    }
    async fn put_cursors(&self, _: Vec<EdgeCursor>) -> SuccessTuple {
        SuccessTuple::fail("down")
    }
    async fn get_state(&self, _: &str) -> Option<EngineState> {
        None
    }
    async fn put_states(&self, _: Vec<EngineState>) -> SuccessTuple {
        SuccessTuple::fail("down")
    }
    async fn record_runs(&self, _: Vec<RunRecord>) -> SuccessTuple {
        SuccessTuple::fail("the instance refused the write: relation mrsm_engine_runs is locked")
    }
    async fn get_runs(&self, _: &RunsFilter) -> Result<Vec<RunRecord>, EngineStoreError> {
        Err(EngineStoreError::Failed(
            "connection to sql:secret-host refused".into(),
        ))
    }
    async fn record_events(&self, _: Vec<EngineEvent>) -> SuccessTuple {
        SuccessTuple::fail("down")
    }
    async fn acquire_lease(&self, _: &str, _: &str, _: std::time::Duration) -> bool {
        false
    }
    async fn release_lease(&self, _: &str, _: &str) -> bool {
        false
    }
}
