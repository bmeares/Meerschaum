//! A Team job uses a configured service account for data and a separate Jobs-only callback.
//! Its primitive data requests follow the ordinary server routes and their resource checks.

use super::{Controller, PreparedExecutor};
use crate::executor_jobs::{JobPlacement, Record};
use meerschaum_connector_api::APIConnector;
use meerschaum_core::access::{Team, same_team};
use meerschaum_core::{Map, Value};
use meerschaum_server::access::Access;
use meerschaum_server::auth::Principal;
use meerschaum_server::executor::callbacks::{Identity, Purpose};
use meerschaum_server::executor::environment::Profile;
use meerschaum_server::executor::gateway::Configuration;
use meerschaum_server::executor::pipe_operation::StoredIntent;
use meerschaum_server::executor::plugins::{self, SourceKind};
use meerschaum_server::executor::registration::Sources;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

const REFUSED: &str = "This Team sync requires one approved Python plugin and its explicit credentialed Team data service.";

/// Derived from the exact filtered configuration signed for the executor connection. Retain
/// only a fingerprint of the endpoint, upstream instance and account. Passwords may rotate.
pub(super) fn authorities(team: &Team, configuration: &Configuration) -> BTreeMap<String, String> {
    let Some(connectors) = configuration
        .config
        .get("meerschaum")
        .and_then(|section| section.get("connectors"))
        .and_then(|section| section.get("api"))
    else {
        return BTreeMap::new();
    };
    let Some(executor) = team
        .execution()
        .ok()
        .and_then(|settings| settings.connector_keys(team).ok().flatten())
    else {
        return BTreeMap::new();
    };
    team.granted_connectors()
        .into_iter()
        .filter_map(|instance| {
            let label = instance.strip_prefix("api:")?;
            if !plugins::valid_name(label)
                || matches!(label, "main_callback" | "default")
                || executor == instance
            {
                return None;
            }
            let attrs = connectors.get(label)?.as_map()?;
            let digest = service_authority(&team.name, attrs)?;
            Some((instance, digest))
        })
        .collect()
}

fn service_authority(team: &str, attrs: &Map) -> Option<String> {
    let field = |name| {
        attrs
            .get(name)
            .and_then(Value::as_str)
            .filter(|v| !v.is_empty())
    };
    let reserved = match attrs.get("teams")? {
        Value::List(names) if names.len() == 1 => names[0].as_str()?,
        Value::Str(name) => name.trim(),
        _ => return None,
    };
    if !same_team(Some(team), Some(reserved))
        || [
            "credential_file",
            "uri",
            meerschaum_core::team_scope::api_execution::ALIAS_ATTRIBUTE,
        ]
        .iter()
        .any(|name| attrs.get(*name).is_some_and(|v| !v.is_null()))
    {
        return None;
    }
    field("password")?;
    let username = field("username")?;
    let host = field("host")?;
    let target = field("instance_keys")?;
    let protocol = field("protocol").unwrap_or("http");
    if !matches!(protocol, "http" | "https")
        || protocol == "https" && attrs.get("verify") == Some(&Value::Bool(false))
        || !target.strip_prefix("sql:").is_some_and(plugins::valid_name)
    {
        return None;
    }
    let port = match attrs.get("port") {
        None | Some(Value::Null) => String::new(),
        Some(port) => format!(":{}", port.py_str().parse::<std::num::NonZeroU16>().ok()?),
    };
    let origin = reqwest::Url::parse(&format!("{protocol}://{host}{port}")).ok()?;
    if origin.host().is_none()
        || !origin.username().is_empty()
        || origin.password().is_some()
        || origin.path() != "/"
        || origin.query().is_some()
        || origin.fragment().is_some()
    {
        return None;
    }
    let public = (origin.origin().ascii_serialization(), target, username);
    Some(plugins::sha256(&serde_json::to_vec(&public).ok()?))
}

struct Target {
    instance: String,
    plugin: String,
    generation: Uuid,
}

fn target(record: &Record) -> Result<Target, String> {
    if record.placement != JobPlacement::Remote
        || record.team_id.is_nil()
        || record.profile != Profile::Team
        || record.action_request
        || record.operation.is_some()
        || record.discovery.is_some()
        || record.extension.is_some()
        || record.connector_registration.is_some()
        || record.schedule.is_some()
    {
        return Err(REFUSED.into());
    }
    let (instance, plugin) = StoredIntent::capture(&record.sysargs)
        .and_then(|intent| intent.team_sync_target())
        .map_err(|_| REFUSED)?;
    let Some(Sources::Archives { catalog }) = &record.source_selection else {
        return Err(REFUSED.into());
    };
    if !catalog.placement.is_remote()
        || catalog.team_id != record.team_id
        || !catalog.plugins.get(&plugin).is_some_and(|source| {
            matches!(
                source.kind,
                SourceKind::PythonModule | SourceKind::PythonPackage
            )
        })
    {
        return Err(REFUSED.into());
    }
    Ok(Target {
        instance,
        plugin,
        generation: catalog.generation,
    })
}

fn prepared_authority<'a>(
    record: &Record,
    executor: &'a PreparedExecutor,
) -> Result<&'a String, String> {
    let target = target(record)?;
    if executor.control.team_id != record.team_id
        || executor.control.generation != target.generation
    {
        return Err("The source archive belongs to another executor generation.".into());
    }
    executor
        .team_pipe_authorities
        .get(&target.instance)
        .ok_or_else(|| REFUSED.into())
}

pub(super) fn capture(
    record: &Record,
    caller: &Access,
    executor: &PreparedExecutor,
) -> Result<String, String> {
    if !matches!(caller.auth, Some(Principal::User(_) | Principal::Token(_))) || caller.no_auth {
        return Err("This Team sync requires its original authenticated job request.".into());
    }
    prepared_authority(record, executor).cloned()
}

pub(super) fn admit(record: &Record, identity: &Identity) -> Result<(), String> {
    let grant = &identity.grant;
    if !record
        .team_pipe_authority
        .as_deref()
        .is_some_and(plugins::valid_digest)
        || grant.team_id != Some(record.team_id)
        || grant.purpose != Purpose::Jobs
        || grant.depth != 0
        || grant.graph_tracking != 1
        || grant.graph_pending
        || grant.graph_origin.is_some()
    {
        return Err("This Team sync lost its original Jobs-only callback or data service.".into());
    }
    let target = target(record)?;
    if grant.generation != Some(target.generation) {
        return Err("The original Jobs-only callback belongs to another source generation.".into());
    }
    let caller = identity.caller();
    let team = caller
        .team_records
        .iter()
        .find(|team| {
            team.id.as_deref().and_then(|id| Uuid::parse_str(id).ok()) == Some(record.team_id)
        })
        .ok_or(REFUSED)?;
    if !team.granted_connectors().contains(&target.instance)
        || !team.granted_plugins().contains(&target.plugin)
    {
        return Err(REFUSED.into());
    }
    Ok(())
}

impl Record {
    pub(crate) fn valid_team_pipe_authority(&self) -> bool {
        self.team_pipe_authority.as_ref().is_none_or(|digest| {
            plugins::valid_digest(digest)
                && self.callback.is_some_and(|id| !id.is_nil())
                && self.owner_binding.is_some()
                && target(self).is_ok()
        })
    }
}

impl Controller {
    /// Use the signer that authenticated this exact configuration for delivery and execution.
    pub(crate) async fn prepare_job_executor(
        &self,
        record: &Record,
    ) -> Result<Arc<APIConnector>, String> {
        let executor = self.prepare_id_bound(record.team_id).await?;
        if let Some(expected) = &record.team_pipe_authority
            && prepared_authority(record, &executor)? != expected
        {
            return Err(
                "The Team data service changed. Recreate this job for its new authority.".into(),
            );
        }
        Ok(executor.api)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn team_plugin_sync_retains_its_explicit_instance_and_source() {
        let words: Vec<_> = [
            "sync",
            "pipes",
            "-i",
            "api:team_data",
            "-c",
            "plugin:fixture",
            "-m",
            "stations",
        ]
        .into_iter()
        .map(String::from)
        .collect();
        assert_eq!(
            StoredIntent::capture(&words)
                .unwrap()
                .team_sync_target()
                .unwrap(),
            ("api:team_data".into(), "fixture".into())
        );
        let mut scheduled = words;
        scheduled.push("--loop".into());
        assert!(
            StoredIntent::capture(&scheduled)
                .unwrap()
                .team_sync_target()
                .is_err()
        );
    }

    #[test]
    fn team_service_account_password_rotation_preserves_its_authority() {
        let mut attrs: Map = serde_json::from_value(serde_json::json!({
            "teams":["Fixture"],"host":"fixture-api","port":8989,
            "username":"service-user","password":"fixture-password","instance_keys":"sql:fixture"
        }))
        .unwrap();
        let original = service_authority("Fixture", &attrs).unwrap();
        attrs.insert("password".into(), "rotated-fixture-password".into());
        assert_eq!(
            service_authority("Fixture", &attrs).as_ref(),
            Some(&original)
        );
        attrs.insert("username".into(), "other-user".into());
        assert_ne!(
            service_authority("Fixture", &attrs).as_ref(),
            Some(&original)
        );
        assert_eq!(service_authority("Other", &attrs), None);
    }
}
