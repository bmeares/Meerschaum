//! Exact native registry for a constructor-bound self-API engine.
//!
//! This never resolves the original actor's logical label through a second HTTP hop and
//! never changes that actor's connector grants. Fresh loads run on the exact captured native
//! connector; the caller must perform final fresh actor/service checks after these awaits.

use std::collections::HashMap;
use std::sync::Arc;

use meerschaum_connector::{InstanceConnector, PipesKeysQuery};
use meerschaum_core::{Map, Pipe, Value};

use crate::{
    AppState,
    access::{self, Access, PipeIndex},
    error::ApiError,
};

const MAX_REGISTERED_PIPES: usize = 4096;
const MAX_REGISTERED_PARAMETERS: usize = 4 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct RegisteredObject {
    pub registration: Value,
    pub logical: Pipe,
    pub physical: Pipe,
}

pub(crate) struct NativeRegistry {
    logical: String,
    native: Arc<dyn InstanceConnector>,
    objects: HashMap<String, RegisteredObject>,
    index: PipeIndex,
}

fn unavailable() -> ApiError {
    ApiError::unavailable("The selected native Pipe registry is unavailable or exceeds its bound.")
}

fn missing() -> ApiError {
    ApiError::new(404, "The selected Pipe was not found.")
}

fn parameters(value: Option<Value>) -> Result<Map, ApiError> {
    match value {
        Some(Value::Map(parameters)) => Ok(parameters),
        Some(Value::Str(text)) => match Value::from_json_str(&text) {
            Ok(Value::Map(parameters)) => Ok(parameters),
            _ => Err(unavailable()),
        },
        _ => Err(unavailable()),
    }
}

fn selected_keys<'a>(
    logical: &str,
    reference: &'a str,
) -> Result<(&'a str, &'a str, Option<&'a str>), ApiError> {
    if reference.is_empty() || reference.len() > 2048 || reference.chars().any(char::is_control) {
        return Err(missing());
    }
    let parts: Vec<_> = reference.split('|').collect();
    let (connector, metric, location) = match parts.as_slice() {
        [instance, connector, metric, location] if *instance == logical => {
            (*connector, *metric, Some(*location))
        }
        [connector, metric, location] => (*connector, *metric, Some(*location)),
        [connector, metric] => (*connector, *metric, None),
        // A foreign explicit registry is not an API-local alias for this native target.
        _ => return Err(missing()),
    };
    if connector.is_empty() || metric.is_empty() {
        return Err(missing());
    }
    let location = location.filter(|value| !matches!(*value, "" | "None" | "null" | "[None]"));
    Ok((connector, metric, location))
}

impl NativeRegistry {
    /// Use after the exact native writer lock for mutation authority. This bypasses both
    /// AppState pipe/index caches and configured API-client metadata caches.
    pub(crate) async fn load(
        state: &AppState,
        logical: &str,
        native: Arc<dyn InstanceConnector>,
    ) -> Result<Self, ApiError> {
        let rows = native
            .registered_pipes(&PipesKeysQuery::default())
            .await
            .map_err(|_| unavailable())?;
        if rows.len() > MAX_REGISTERED_PIPES {
            return Err(unavailable());
        }
        let mut objects = HashMap::new();
        let mut dict = crate::state::PipesDict::new();
        let mut bytes = 0usize;
        for (identity, entry) in rows {
            let identity = identity.ok_or_else(unavailable)?;
            let registration = identity.to_value();
            let identity = identity.to_string();
            let parameters = parameters(entry.extra)?;
            bytes = bytes
                .checked_add(Value::Map(parameters.clone()).to_json_string().len())
                .and_then(|total| total.checked_add(entry.connector_keys.len()))
                .and_then(|total| total.checked_add(entry.metric_key.len()))
                .and_then(|total| {
                    total.checked_add(entry.location_key.as_ref().map_or(0, String::len))
                })
                .ok_or_else(unavailable)?;
            if bytes > MAX_REGISTERED_PARAMETERS {
                return Err(unavailable());
            }
            let location = crate::state::normalize_location(entry.location_key.as_deref());
            let make = |instance: &str| {
                let pipe = Pipe::builder(&entry.connector_keys, &entry.metric_key)
                    .location(location.as_deref())
                    .instance(instance)
                    .defaults(state.config.pipe_defaults.clone())
                    .parameters(parameters.clone())
                    .build()
                    .map_err(|_| unavailable())?;
                pipe.set_registration_id(Some(identity.clone()));
                Ok::<_, ApiError>(pipe)
            };
            let physical = make(&native.connector_keys())?;
            let logical_pipe = make(logical)?;
            let key = meerschaum_connector::sync_metrics::pipe_key(
                &entry.connector_keys,
                &entry.metric_key,
                location.as_deref(),
            );
            if objects
                .insert(
                    key,
                    RegisteredObject {
                        registration,
                        logical: logical_pipe,
                        physical: physical.clone(),
                    },
                )
                .is_some()
            {
                return Err(unavailable());
            }
            dict.entry(entry.connector_keys)
                .or_default()
                .entry(entry.metric_key)
                .or_default()
                .insert(location, Arc::new(crate::state::PipeHandle::new(physical)));
        }
        let index = access::index_from_dict(logical, &dict, native.as_ref());
        Ok(Self {
            logical: logical.into(),
            native,
            objects,
            index,
        })
    }

    pub(crate) fn object(&self, reference: &str) -> Result<&RegisteredObject, ApiError> {
        let (connector, metric, location) = selected_keys(&self.logical, reference)?;
        self.objects
            .get(&meerschaum_connector::sync_metrics::pipe_key(
                connector, metric, location,
            ))
            .ok_or_else(missing)
    }

    /// A logical alias has two independent principals. Original administrator status cannot
    /// bypass a restricted configured service, and service ownership cannot grant the caller
    /// another Team or raw SQL authority.
    pub(crate) fn check(
        &self,
        state: &AppState,
        original: &Access,
        service: &Access,
        reference: &str,
        write: bool,
    ) -> Result<&RegisteredObject, ApiError> {
        let object = self.object(reference)?;
        access::guard::check_connector_teams(state, original, &self.logical)?;
        access::guard::check_connector_teams(state, service, &self.native.connector_keys())?;
        for (principal, pipe) in [(original, &object.logical), (service, &object.physical)] {
            access::guard::check_source_connector(state, principal, pipe.connector_keys())?;
            let access = principal.for_instance(state, pipe.instance_keys());
            if !access.can_see_pipe(pipe) {
                return Err(missing());
            }
            if write && !access.can_edit(pipe.raw_parameters()) {
                return Err(access::not_editable(pipe));
            }
            access::guard::check_pipe_target(
                self.native.as_ref(),
                pipe,
                &self.index.hidden_targets_for(&access),
            )?;
        }
        Ok(object)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_exact_logical_registry_does_not_adopt_foreign_main_or_client_aliases() {
        let logical = "api:selected";
        assert_eq!(
            selected_keys(logical, "api:selected|manual:posted|data|None").unwrap(),
            ("manual:posted", "data", None),
        );
        assert_eq!(
            selected_keys(logical, "manual:posted|data|None").unwrap(),
            ("manual:posted", "data", None),
        );
        for reference in [
            "sql:main|manual:posted|data|None",
            "api:foreign|manual:posted|data|None",
            "api:selected|manual:posted|data|None|extra",
            "api:selected||data|None",
        ] {
            assert!(selected_keys(logical, reference).is_err(), "{reference}");
        }
    }
}
