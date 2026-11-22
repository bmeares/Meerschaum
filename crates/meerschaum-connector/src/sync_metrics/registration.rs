//! Registration segments: the stable key of a pipe on its instance, the stamp of its persisted ID
//! and access, and the checks that history belongs to the pipe's current registration.

use std::collections::HashMap;

use meerschaum_core::{Map, Pipe, Value};
use sha2::{Digest, Sha256};

use crate::{ConnectorResult, InstanceConnector};

/// Stable identity within an instance; aliases of the instance share the same key.
pub fn pipe_key(connector: &str, metric: &str, location: Option<&str>) -> String {
    let identity = serde_json::to_vec(&(connector, metric, location)).unwrap_or_default();
    hex::encode(Sha256::digest(identity))
}

/// Persisted ID plus normalized ACL. Access changes start a new visible metrics segment.
pub fn registration_stamp(identity: &str, parameters: &Map) -> String {
    let access = meerschaum_core::access::PipeAccess::from_parameters(parameters).to_value();
    hex::encode(Sha256::digest(
        format!("{identity}|{}", access.json_dumps()).as_bytes(),
    ))
}

/// Registration segment observed during an existing metadata read, without another query.
pub fn observed_registration(pipe: &Pipe) -> Option<String> {
    pipe.registration_id()
        .map(|identity| registration_stamp(&identity, pipe.raw_parameters()))
}

/// Current registration segments, in one fresh bulk metadata query.
pub async fn registrations<C: InstanceConnector + ?Sized>(
    conn: &C,
) -> ConnectorResult<HashMap<String, String>> {
    Ok(registration_snapshot(conn).await?.stamps)
}

pub(super) struct RegistrationSnapshot {
    pub(super) stamps: HashMap<String, String>,
    pub(super) keys: HashMap<String, (String, String, Option<String>)>,
    pub(super) pipelines: HashMap<String, (String, String)>,
}

pub(super) async fn registration_snapshot<C: InstanceConnector + ?Sized>(
    conn: &C,
) -> ConnectorResult<RegistrationSnapshot> {
    let keys = conn
        .registered_pipes(&crate::PipesKeysQuery::default())
        .await?;
    let mut current = HashMap::new();
    let mut pipe_keys = HashMap::new();
    let mut pipelines = HashMap::new();
    for (identity, entry) in keys {
        if let Some(identity) = identity {
            let parameters = match entry.extra {
                Some(Value::Map(parameters)) => parameters,
                Some(Value::Str(text)) => Value::from_json_str(&text)
                    .ok()
                    .map(|value| value.to_map())
                    .unwrap_or_default(),
                _ => Map::new(),
            };
            if entry.connector_keys == "mrsm" {
                if entry.metric_key == "pipeline"
                    && let (Some(name), Some(identity)) = (
                        entry.location_key,
                        parameters
                            .get("pipeline")
                            .and_then(|definition| definition.get("id"))
                            .and_then(Value::as_str),
                    )
                {
                    pipelines.insert(name, (identity.to_string(), conn.connector_keys()));
                }
                continue;
            }
            let key = pipe_key(
                &entry.connector_keys,
                &entry.metric_key,
                entry.location_key.as_deref(),
            );
            current.insert(
                key.clone(),
                registration_stamp(&identity.to_string(), &parameters),
            );
            pipe_keys.insert(
                key,
                (entry.connector_keys, entry.metric_key, entry.location_key),
            );
        }
    }
    let saved = conn
        .registered_pipes(&crate::PipesKeysQuery {
            connector_keys: vec!["mrsm".into()],
            metric_keys: vec!["pipeline".into()],
            ..Default::default()
        })
        .await?;
    for (_, entry) in saved {
        let parameters = match &entry.extra {
            Some(Value::Map(parameters)) => parameters.clone(),
            Some(Value::Str(text)) => Value::from_json_str(text)
                .ok()
                .map(|value| value.to_map())
                .unwrap_or_default(),
            _ => Map::new(),
        };
        if let (Some(name), Some(identity)) = (
            &entry.location_key,
            parameters
                .get("pipeline")
                .and_then(|definition| definition.get("id"))
                .and_then(Value::as_str),
        ) {
            pipelines.insert(name.clone(), (identity.to_string(), conn.connector_keys()));
        }
    }
    Ok(RegistrationSnapshot {
        stamps: current,
        keys: pipe_keys,
        pipelines,
    })
}

/// A pipe reference with or without its instance prefix.
pub fn reference_keys(reference: &str) -> Option<(&str, &str, Option<&str>)> {
    let fields: Vec<&str> = reference.split('|').collect();
    let (connector, metric, location) = match fields.as_slice() {
        [_, connector, metric, location] | [connector, metric, location] => {
            (*connector, *metric, Some(*location))
        }
        [connector, metric] => (*connector, *metric, None),
        _ => return None,
    };
    if connector.is_empty() || metric.is_empty() || connector == "mrsm" {
        return None;
    }
    Some((
        connector,
        metric,
        location.filter(|value| !matches!(*value, "None" | "")),
    ))
}

/// Whether immutable history belongs to a current registration on this exact instance.
/// Missing legacy evidence and foreign API aliases never establish continuity.
pub fn registration_matches(
    reference: &str,
    captured: Option<&str>,
    instance: &str,
    current: &HashMap<String, String>,
) -> bool {
    if reference.split('|').count() != 4 || reference.split('|').next() != Some(instance) {
        return false;
    }
    let Some(captured) = captured.filter(|value| !value.is_empty()) else {
        return false;
    };
    reference_keys(reference).is_some_and(|(connector, metric, location)| {
        current
            .get(&pipe_key(connector, metric, location))
            .map(String::as_str)
            == Some(captured)
    })
}

/// The original registry, or an exact API-local registry proven by the receiving server.
/// Network writers must strip and independently validate `registration_registry` before storing it.
pub fn snapshot_registry(definition: &serde_json::Value) -> Option<&str> {
    definition
        .get("registration_registry")
        .or_else(|| definition.get("registry"))
        .and_then(serde_json::Value::as_str)
        .filter(|registry| !registry.is_empty())
}

/// Remove transport-only registration evidence after authorization, preserving original registries.
pub fn strip_snapshot_proofs(selection: &mut serde_json::Value) -> bool {
    let mut changed = false;
    if let Some(snapshots) = selection
        .get_mut("pipeline_definitions")
        .and_then(serde_json::Value::as_array_mut)
    {
        for snapshot in snapshots {
            if let Some(snapshot) = snapshot.as_object_mut() {
                changed |= snapshot.remove("registration_registry").is_some();
            }
        }
    }
    changed
}
