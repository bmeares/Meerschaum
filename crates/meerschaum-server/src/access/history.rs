//! Private registration evidence for retained history; never part of public run JSON.

use std::collections::HashMap;

use meerschaum_connector::{DataQuery, pipe_ops::Doc, sync_metrics};
use meerschaum_connector_api::models::RunRecord;
use meerschaum_core::{Map, Value};
use meerschaum_kernel::value::Value as KValue;

use super::{RefFilter, guard};

struct NodeIdentity {
    reference: String,
    captured: Option<String>,
}

fn record_identity(record: &RunRecord) -> Option<NodeIdentity> {
    Some(NodeIdentity {
        reference: record
            .extra
            .get("registration_ref")
            .and_then(Value::as_str)
            .or(record.pipe_ref.as_deref())?
            .to_string(),
        captured: record
            .extra
            .get("registration_id")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
}

fn string(doc: &Doc, key: &str) -> Option<String> {
    match doc.get(key)? {
        KValue::Str(value) => Some(value.clone()),
        _ => None,
    }
}

impl RefFilter {
    /// A remote host needs generation authority even when its stored API credential is admin.
    pub fn requiring_generations(mut self) -> Self {
        self.strict_generations = true;
        self
    }

    async fn identity_current(&mut self, identity: &NodeIdentity) -> bool {
        let Some(instance) = identity.reference.split('|').next() else {
            return false;
        };
        if !self.registrations.contains_key(instance) {
            let current = match self.state.get_api_connector(Some(instance)) {
                Ok(conn)
                    if guard::check_connector_teams(
                        &self.state,
                        &self.access,
                        &conn.connector_keys(),
                    )
                    .is_ok() =>
                {
                    sync_metrics::registrations(conn.as_ref()).await.ok()
                }
                _ => None,
            };
            self.registrations.insert(instance.to_string(), current);
        }
        self.registrations
            .get(instance)
            .and_then(Option::as_ref)
            .is_some_and(|current| {
                sync_metrics::registration_matches(
                    &identity.reference,
                    identity.captured.as_deref(),
                    instance,
                    current,
                )
            })
    }

    /// Check the stored immutable node segments before authorizing any whole-run metadata.
    /// Registrations are fresh once per exact instance and node reads use requested run IDs.
    pub async fn prime_run_generations(&mut self, records: &[RunRecord]) {
        if self.access.is_admin() && !self.strict_generations {
            return;
        }
        let mut nodes: HashMap<String, Vec<NodeIdentity>> = HashMap::new();
        let mut ids = Vec::new();
        for record in records {
            if self.run_generations.contains_key(&record.run_id) {
                continue;
            }
            ids.push(record.run_id.clone());
            if record.pipe_ref.is_some()
                && let Some(identity) = record_identity(record)
            {
                nodes
                    .entry(record.run_id.clone())
                    .or_default()
                    .push(identity);
            }
        }
        ids.sort();
        ids.dedup();
        let missing: Vec<_> = ids
            .iter()
            .filter(|id| !nodes.contains_key(*id))
            .cloned()
            .collect();
        if !missing.is_empty() {
            let conn = self.state.get_api_connector(Some(&self.default_instance));
            if let Ok(conn) = conn {
                for chunk in missing.chunks(128) {
                    let params: Map = [(
                        "run_id".into(),
                        Value::List(chunk.iter().cloned().map(Value::Str).collect()),
                    )]
                    .into_iter()
                    .collect();
                    let rows = match conn
                        .engine_state_pipe(&conn.connector_keys(), "engine_nodes")
                        .await
                    {
                        Ok(Some(pipe)) => conn
                            .get_pipe_docs(
                                &pipe,
                                &DataQuery {
                                    params: Some(params),
                                    ..Default::default()
                                },
                            )
                            .await
                            .ok(),
                        _ => None,
                    };
                    if let Some(rows) = rows {
                        for doc in rows {
                            if let (Some(id), Some(reference)) = (
                                string(&doc, "run_id"),
                                string(&doc, "registration_ref")
                                    .or_else(|| string(&doc, "pipe_ref")),
                            ) {
                                nodes.entry(id).or_default().push(NodeIdentity {
                                    reference,
                                    captured: string(&doc, "registration_id"),
                                });
                            }
                        }
                    }
                }
            }
            // Custom engine stores may keep their nodes outside the selected instance.
            // Their explicit stored evidence is subject to the same fresh registration check.
            for id in missing
                .iter()
                .filter(|id| !nodes.contains_key(*id))
                .collect::<Vec<_>>()
            {
                if let Ok(rows) = self
                    .state
                    .engine
                    .get_runs(&crate::backends::RunsFilter {
                        run_id: Some(id.clone()),
                        ..Default::default()
                    })
                    .await
                {
                    for row in rows.iter().filter(|row| row.pipe_ref.is_some()) {
                        if let Some(identity) = record_identity(row) {
                            nodes.entry(id.clone()).or_default().push(identity);
                        }
                    }
                }
            }
        }
        for id in ids {
            let identities = nodes.remove(&id).unwrap_or_default();
            let header = records
                .iter()
                .find(|row| row.run_id == id && row.pipe_ref.is_none());
            let expected = header
                .and_then(|row| row.extra.get("num_nodes"))
                .and_then(Value::as_i64)
                .filter(|count| *count > 0);
            let complete = header.is_none()
                || expected.is_some_and(|count| identities.len() >= count as usize);
            let mut current = !identities.is_empty() && complete;
            for identity in &identities {
                current &= self.identity_current(identity).await;
            }
            self.run_generations.insert(id, current);
        }
    }

    /// Only a proven current segment may grant restricted access to retained history.
    pub fn run_generation_current(&self, run_id: &str) -> bool {
        (self.access.is_admin() && !self.strict_generations)
            || self.run_generations.get(run_id).copied().unwrap_or(false)
    }
}
