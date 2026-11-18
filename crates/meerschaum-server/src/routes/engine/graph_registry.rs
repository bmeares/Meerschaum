//! Validate captured self-API registrations on the native store held by Main.
use super::{
    registry::{NativeRegistry, RegisteredObject},
    selection::SelfApiBinding,
    service::ServiceAuthority,
};
use crate::{AppState, access::Access, error::ApiError, executor::pipe_operation::Snapshot};
use meerschaum_connector::PipesKeysQuery;
use std::{
    collections::{BTreeSet, HashMap},
    future::Future,
    sync::Arc,
};

struct View {
    binding: SelfApiBinding,
    registry: NativeRegistry,
    authority: ServiceAuthority,
    service: Access,
}

/// Only configured same-Main bindings from a captured graph can supply these registration reads.
pub struct NativeGraphRegistries(Arc<HashMap<String, View>>);
tokio::task_local! { static ACTIVE: Arc<HashMap<String, View>>; }

impl NativeGraphRegistries {
    pub async fn load(state: &AppState, snapshot: Option<&Snapshot>) -> Result<Self, ApiError> {
        let mut views = HashMap::new();
        let aliases: BTreeSet<_> = snapshot
            .into_iter()
            .flat_map(|graph| graph.members().keys())
            .filter_map(|reference| reference.split_once('|').map(|(instance, _)| instance))
            .filter(|instance| instance.starts_with("api:"))
            .collect();
        for alias in aliases {
            let Some(binding) = state.self_api_bindings.binding(alias) else {
                continue;
            };
            binding.current(state)?;
            let authority = ServiceAuthority::capture(state, binding).await?;
            let registry = NativeRegistry::load(state, alias, state.instance()).await?;
            let service = authority.fresh(state, &["pipes:read"]).await?;
            authority.finish(state, &service)?;
            binding.current(state)?;
            views.insert(
                alias.to_owned(),
                View {
                    binding: binding.clone(),
                    registry,
                    authority,
                    service,
                },
            );
        }
        Ok(Self(Arc::new(views)))
    }

    pub async fn scope<T>(
        &self,
        state: &AppState,
        check: impl Future<Output = Result<T, String>>,
    ) -> Result<T, String> {
        if self.0.is_empty() {
            return check.await;
        }
        let result = ACTIVE.scope(self.0.clone(), check).await?;
        for view in self.0.values() {
            let current = view
                .authority
                .fresh(state, &["pipes:read"])
                .await
                .map_err(|error| error.detail_text())?;
            if current.principal != view.service.principal
                || current.primary_team != view.service.primary_team
            {
                return Err("The self-API graph service changed during admission.".into());
            }
            view.authority
                .finish(state, &current)
                .map_err(|error| error.detail_text())?;
            view.binding
                .current(state)
                .map_err(|error| error.detail_text())?;
        }
        Ok(result)
    }
}

pub(crate) fn registered(
    state: &AppState,
    caller: &Access,
    reference: &str,
    write: bool,
) -> Result<Option<RegisteredObject>, ApiError> {
    let Some((instance, _)) = reference.split_once('|') else {
        return Ok(None);
    };
    ACTIVE
        .try_with(|views| {
            let Some(view) = views.get(instance) else {
                return Ok(None);
            };
            view.binding.current(state)?;
            view.service.require(
                if write {
                    &["pipes:read", "pipes:write"]
                } else {
                    &["pipes:read"]
                },
                &[],
            )?;
            let object = view
                .registry
                .check(state, caller, &view.service, reference, write)?;
            Ok(Some(object.clone()))
        })
        .unwrap_or(Ok(None))
}

pub(crate) async fn check_selection(
    state: &AppState,
    caller: &Access,
    instance: &str,
    query: &PipesKeysQuery,
) -> Result<bool, ApiError> {
    let Ok(views) = ACTIVE.try_with(Arc::clone) else {
        return Ok(false);
    };
    let Some(view) = views.get(instance) else {
        return Ok(false);
    };
    view.binding.current(state)?;
    view.service.require(&["pipes:read", "pipes:write"], &[])?;
    let keys = state
        .instance()
        .fetch_pipes_keys(query)
        .await
        .map_err(|_| {
            ApiError::unavailable("Could not resolve the captured native registry selection.")
        })?;
    for entry in keys.entries() {
        if entry.connector_keys == "mrsm" {
            if query.connector_keys.iter().any(|key| key == "mrsm") {
                return Err(ApiError::not_found());
            }
            continue;
        }
        let reference = format!(
            "{instance}|{}|{}|{}",
            entry.connector_keys,
            entry.metric_key,
            entry.location_key.as_deref().unwrap_or("None")
        );
        view.registry
            .check(state, caller, &view.service, &reference, true)?;
    }
    view.binding.current(state)?;
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ServerConfig,
        backends::InstanceResolver,
        executor::pipe_operation::{PipeOperation, SnapshotMember},
    };
    use meerschaum_connector::InstanceConnector;
    use meerschaum_core::{Pipe, Value};
    use serde_json::json;

    struct Alias(Arc<dyn InstanceConnector>);
    impl InstanceResolver for Alias {
        fn resolve(&self, keys: &str) -> Option<Arc<dyn InstanceConnector>> {
            (keys == "api:writer").then(|| self.0.clone())
        }
    }

    #[tokio::test]
    async fn captured_self_api_registration_and_selection_use_the_native_sqlite_registry() {
        let dir = tempfile::tempdir().unwrap();
        let local = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let addr = local.local_addr().unwrap();
        let mut config = ServerConfig::for_root("sql:main", dir.path());
        config.no_auth = false;
        config.allow_multiple_instances = true;
        config.allowed_instance_keys = vec!["sql:main".into(), "api:writer".into()];
        config.execution_callback_url = Some(format!("http://{addr}"));
        let settings = json!({"host":"127.0.0.1","port":addr.port(),"instance_keys":"sql:main", "username":"fixture_service", "password":"fixture-service-password"});
        config.connectors = serde_json::from_value::<Value>(json!({"api":{"writer":settings}}))
            .unwrap()
            .to_map();
        let api = meerschaum_connector_api::APIConnector::new(
            Some("writer"),
            serde_json::from_value::<Value>(settings).unwrap().to_map(),
        )
        .unwrap();
        let db = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", dir.path().join("test.db").display()),
            )
            .await
            .unwrap(),
        );
        let pipe = Pipe::builder("sql:main", "station")
            .instance("sql:main")
            .build()
            .unwrap();
        assert!(db.register_pipe(&pipe).await.success);
        let mut service =
            meerschaum_core::users::User::new("fixture_service", Some("fixture-service-password"));
        service.user_type = Some("admin".into());
        assert!(db.register_user(&mut service).await.success);
        let registration = Value::Int(db.get_pipe_id(&pipe).await.unwrap().unwrap());
        let parameters = db.get_pipe_attributes(&pipe).await.unwrap()["parameters"].to_map();
        let reference = "api:writer|sql:main|station|None";
        let snapshot = Snapshot::new(
            [(
                reference.to_string(),
                SnapshotMember {
                    registration: registration.clone(),
                    parameters,
                    writable: true,
                    node_window: None,
                },
            )]
            .into(),
        )
        .unwrap();
        let state = AppState::builder(config, db.clone())
            .resolver(Arc::new(Alias(Arc::new(api))))
            .native_self_api_engines()
            .build()
            .unwrap();
        NativeGraphRegistries::load(&state, Some(&snapshot))
            .await
            .unwrap();
        let binding = state.self_api_bindings.binding("api:writer").unwrap();
        let verified = binding
            .verified_password
            .lock()
            .await
            .clone()
            .expect("verified configured service password");
        NativeGraphRegistries::load(&state, Some(&snapshot))
            .await
            .unwrap();
        assert_eq!(
            binding.verified_password.lock().await.as_ref(),
            Some(&verified)
        );
        service.password_hash = None;
        assert!(db.edit_user(&service).await.success);
        let registries = NativeGraphRegistries::load(&state, Some(&snapshot))
            .await
            .unwrap();
        assert_ne!(
            binding.verified_password.lock().await.as_ref(),
            Some(&verified)
        );
        let actor = Access::everything();
        let pipe_operation = PipeOperation {
            pipe_ref: reference.into(),
            registration,
            verify: false,
            begin: None,
            end: None,
            parents: None,
            children: None,
            row_params: None,
            metadata: Some(snapshot),
        };
        let validation = registries.scope(&state, async {
            let object = registered(&state, &actor, reference, true)
                .map_err(|error| error.detail_text())?
                .expect("bound registered Pipe");
            assert_eq!(object.physical.pipe_ref(), pipe.pipe_ref());
            assert_eq!(object.logical.pipe_ref(), reference);
            let query = PipesKeysQuery {
                connector_keys: vec!["sql:main".into()],
                metric_keys: vec!["station".into()],
                location_keys: vec![None],
                ..Default::default()
            };
            assert!(
                check_selection(&state, &actor, "api:writer", &query)
                    .await
                    .map_err(|error| error.detail_text())?
            );
            pipe_operation
                .authorize(&state, &actor, &pipe_operation.sysargs().unwrap())
                .await
                .map_err(|error| error.detail_text())
        });
        tokio::select! { result=validation=>result.unwrap(), _=local.accept()=>panic!("The captured registration unnecessarily contacted the API service.") }
    }
}
