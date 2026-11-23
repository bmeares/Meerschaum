//! Read-only native Pipeline planning for an authenticated API source.
//!
//! This path never constructs a plugin registry, dispatches a CLI action, writes engine
//! state or issues a callback bearer. A short-lived Main-only proof revalidates the original
//! registered identity and exact object snapshot before every state read.

use super::*;
use meerschaum_core::Pipe;
use meerschaum_engine::{
    Bound, Depth, EdgeCursor, Graph, GraphOptions, InstanceState, LedgerEntry, PipeRef, Pipeline,
    Selection, StateError, StateSource, StaticRegistry,
};
use meerschaum_server::access::{Access, guard};
use meerschaum_server::backends::{PlanError, PlanOptions};
use meerschaum_server::executor::{callbacks, context};
use std::collections::{BTreeSet, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};

const MAX_RECORDS: usize = 100_000;
const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;
const CLASSIFICATION_UNAVAILABLE: &str = super::reader_capabilities::UNAVAILABLE;

fn unavailable() -> String {
    "The Pipeline's original source, registered members or native metadata are unavailable. Refresh the plan after checking its permissions.".into()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderMap;
    use meerschaum_core::users::User;

    fn map(value: serde_json::Value) -> Map {
        Value::from_json_str(&value.to_string()).unwrap().to_map()
    }

    #[tokio::test]
    async fn reader_plan_never_imports_plugins_or_creates_state_and_fresh_revocation_refuses() {
        let root = tempfile::tempdir().unwrap();
        let plugin = root.path().join("plugins/unimported.py");
        std::fs::create_dir_all(plugin.parent().unwrap()).unwrap();
        let marker = root.path().join("plugin-imported");
        std::fs::write(
            &plugin,
            format!(
                "from pathlib import Path\nPath({:?}).write_text('unsafe')\n",
                marker.display().to_string()
            ),
        )
        .unwrap();
        let database = root.path().join("planner.db");
        let instance = Arc::new(
            meerschaum_connector_sql::SqlConnector::from_uri(
                "main",
                &format!("sqlite:///{}", database.display()),
            )
            .await
            .unwrap(),
        );
        let mut user = User::new("reader", Some("synthetic-plan-pass"));
        user.user_type = Some("regular".into());
        user.attributes = Some(map(
            serde_json::json!({"role":"custom", "scopes":["pipes:read"]}),
        ));
        assert!(instance.register_user(&mut user).await.success);
        let first = Pipe::builder("sql:main", "parent")
            .instance("sql:main")
            .parameters(map(
                serde_json::json!({"columns":{"datetime":"id"},"dtypes":{"id":"int64"}}),
            ))
            .build()
            .unwrap();
        let second = Pipe::builder("sql:main", "child")
            .instance("sql:main")
            .parameters(map(
                serde_json::json!({"columns":{"datetime":"id"},"dtypes":{"id":"int64"},
                "parents":[{"connector":"sql:main", "metric":"parent"}]}),
            ))
            .build()
            .unwrap();
        assert!(instance.register_pipe(&first).await.success);
        assert!(instance.register_pipe(&second).await.success);
        let saved = Pipe::builder("mrsm", "pipeline")
            .location(Some("reader-plan"))
            .instance("sql:main")
            .parameters(map(
                serde_json::json!({"pipeline":{"id":Uuid::new_v4().to_string(),"name":"reader-plan",
                "targets":[first.pipe_ref(),second.pipe_ref()],"graph":true}}),
            ))
            .build()
            .unwrap();
        assert!(instance.register_pipe(&saved).await.success);
        let unknown = Pipe::builder("plugin:unimported", "unclassified")
            .instance("sql:main")
            .build()
            .unwrap();
        assert!(instance.register_pipe(&unknown).await.success);
        let unknown_selection=Pipe::builder("mrsm","pipeline").location(Some("unclassified")).instance("sql:main")
            .parameters(map(serde_json::json!({"pipeline":{"id":Uuid::new_v4().to_string(),"targets":[unknown.pipe_ref()]}}))).build().unwrap();
        assert!(instance.register_pipe(&unknown_selection).await.success);
        let mut options = ConfigOptions::with_env(vec![
            ("MRSM_ROOT_DIR".into(), root.path().display().to_string()),
            (
                "MRSM_PLUGINS_DIR".into(),
                plugin.parent().unwrap().display().to_string(),
            ),
        ]);
        options.allow_writes = false;
        options.home = root.path().into();
        let snapshot = map(
            serde_json::json!({"meerschaum":{"instance":"sql:main","connectors":{"sql":{"main":{"flavor":"sqlite","database":database}}}}}),
        );
        let controller = Arc::new(Controller::new(
            root.path().into(),
            instance.clone(),
            options,
            snapshot,
        ));
        let state = meerschaum_server::AppState::builder(
            meerschaum_server::ServerConfig::for_root("sql:main", root.path()),
            instance.clone(),
        )
        .secret(b"synthetic-reader-plan-secret-01234567890123456789".to_vec())
        .build()
        .unwrap();
        controller.bind_state(&state).unwrap();
        let fingerprint = meerschaum_server::auth::user_credential(&state, &user)
            .await
            .unwrap()
            .unwrap();
        let credential = meerschaum_server::auth::jwt_encode(
            &map(serde_json::json!({
                "sub": user.username.clone(),
                "mrsm_principal": "user",
                "mrsm_credential": fingerprint.clone(),
                "scopes": ["pipes:read"],
                "exp": chrono::Utc::now().timestamp() + 120,
            })),
            state.secret(),
        );
        let mut headers = HeaderMap::new();
        headers.insert(
            "authorization",
            format!("Bearer {credential}").parse().unwrap(),
        );
        let caller = Arc::new(
            meerschaum_server::access::resolve(&state, &headers)
                .await
                .unwrap(),
        );
        assert!(caller.has("pipes:read"));
        assert!(!caller.has("actions:execute") && !caller.has("jobs:execute"));
        let tables_before = meerschaum_connector::pipe_ops::frame_to_docs(
            &instance
                .read("SELECT name, sql FROM sqlite_master ORDER BY name")
                .await
                .unwrap(),
        );
        let registrations_before = instance
            .registered_pipes(&Default::default())
            .await
            .unwrap();
        let planned = context::scope(
            Some(caller.clone()),
            controller.plan_metadata("reader-plan", "sql:main", &PlanOptions::default()),
        )
        .await
        .unwrap();
        assert_eq!(
            planned["plan"]["nodes"].as_array().unwrap().len(),
            2,
            "{planned}"
        );
        assert!(
            !marker.exists(),
            "Reader planning imported executable plugin source"
        );
        assert_eq!(
            meerschaum_connector::pipe_ops::frame_to_docs(
                &instance
                    .read("SELECT name, sql FROM sqlite_master ORDER BY name")
                    .await
                    .unwrap()
            ),
            tables_before,
            "Reader planning created an engine/cache table"
        );
        assert_eq!(
            instance
                .registered_pipes(&Default::default())
                .await
                .unwrap(),
            registrations_before,
            "Reader planning registered extra internal pipes"
        );
        let unknown = context::scope(
            Some(caller.clone()),
            controller.plan_metadata("unclassified", "sql:main", &PlanOptions::default()),
        )
        .await;
        assert!(
            matches!(unknown, Err(PlanError::MetadataUnavailable(_))),
            "An unclassified extension source was silently treated as a batch: {unknown:?}"
        );
        assert!(
            !marker.exists(),
            "Missing classification imported plugin source in the reader"
        );
        // The original authenticated request cannot adopt permissions changed during its work.
        user.attributes = Some(map(
            serde_json::json!({"role":"custom","scopes":["logs:read"]}),
        ));
        user.password.clear();
        assert!(instance.edit_user(&user).await.success);
        assert_eq!(
            meerschaum_server::auth::user_credential(&state, &user)
                .await
                .unwrap(),
            Some(fingerprint.clone())
        );
        assert!(
            context::scope(
                Some(caller.clone()),
                controller.plan_metadata("reader-plan", "sql:main", &PlanOptions::default())
            )
            .await
            .is_err()
        );
        user.attributes = Some(map(
            serde_json::json!({"role":"custom","scopes":["pipes:read"]}),
        ));
        assert!(instance.edit_user(&user).await.success);
        context::scope(
            Some(caller.clone()),
            controller.plan_metadata("reader-plan", "sql:main", &PlanOptions::default()),
        )
        .await
        .unwrap();
        user.password = "synthetic-replacement-plan-pass".into();
        user.password_hash = None;
        assert!(instance.edit_user(&user).await.success);
        assert_ne!(
            meerschaum_server::auth::user_credential(&state, &user)
                .await
                .unwrap(),
            Some(fingerprint)
        );
        assert!(
            context::scope(
                Some(caller),
                controller.plan_metadata("reader-plan", "sql:main", &PlanOptions::default())
            )
            .await
            .is_err()
        );
        assert!(!marker.exists());
    }
}

/// Own proof cleanup even if a client drops a request halfway through a database read.
struct Proof {
    state: meerschaum_server::AppState,
    grant: callbacks::Grant,
    /// The captured first-party or execution principal; never the ambient restart caller.
    request: Arc<Access>,
}
impl Drop for Proof {
    fn drop(&mut self) {
        let state = self.state.clone();
        let id = self.grant.id;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = callbacks::revoke(&state, id).await;
            });
        }
    }
}

impl Proof {
    async fn caller(&self) -> Result<Access, String> {
        let identity = callbacks::load_identity(&self.state, self.grant.id, self.grant.launch_id)
            .await
            .map_err(|_| unavailable())?;
        let caller = identity.caller();
        caller
            .require(&["pipes:read"], &[])
            .map_err(|_| unavailable())?;
        Ok(caller)
    }

    async fn instance(&self, keys: &str) -> Result<Arc<dyn InstanceConnector>, String> {
        let caller = self.caller().await?;
        if !keys
            .split_once(':')
            .is_some_and(|(kind, _)| matches!(kind, "sql" | "api" | "valkey"))
            || !guard::may_use_connector(&self.state, &caller, keys)
        {
            return Err(unavailable());
        }
        self.state
            .get_api_connector(Some(keys))
            .map_err(|_| unavailable())
    }
}

#[derive(Clone)]
struct Snapshot {
    pipe: Pipe,
    registration: Value,
}

impl Snapshot {
    async fn check(&self, proof: &Proof, pipeline: bool) -> Result<(), String> {
        let conn = proof.instance(self.pipe.instance_keys()).await?;
        let attributes = conn
            .get_pipe_attributes(&self.pipe)
            .await
            .map_err(|_| unavailable())?;
        let id = conn
            .get_pipe_id(&self.pipe)
            .await
            .map_err(|_| unavailable())?
            .map(|id| id.to_value());
        let caller = proof.caller().await?;
        let access = caller.for_instance(&proof.state, self.pipe.instance_keys());
        if id.as_ref() != Some(&self.registration)
            || attributes.get("parameters").and_then(Value::as_map)
                != Some(self.pipe.raw_parameters())
            || !(if pipeline {
                access.can_see(self.pipe.raw_parameters())
            } else {
                access.can_see_pipe(&self.pipe)
            })
        {
            return Err(unavailable());
        }
        Ok(())
    }
}

struct ScopedState<'a> {
    proof: &'a Proof,
    inner: InstanceState,
    snapshots: HashMap<PipeRef, Snapshot>,
    failed: AtomicBool,
}

impl ScopedState<'_> {
    async fn check(&self, nodes: &[&PipeRef], instance: Option<&str>) -> Result<(), StateError> {
        let result = async {
            if let Some(instance) = instance {
                let _ = self.proof.instance(instance).await?;
            }
            for node in nodes {
                self.snapshots
                    .get(*node)
                    .ok_or_else(unavailable)?
                    .check(self.proof, false)
                    .await?;
            }
            Ok::<_, String>(())
        }
        .await;
        result.map_err(|_| {
            self.failed.store(true, Ordering::Release);
            StateError::Other(unavailable())
        })
    }

    fn bounded<T>(&self, result: Result<T, StateError>) -> Result<T, StateError> {
        result.map_err(|_| {
            self.failed.store(true, Ordering::Release);
            StateError::Other(unavailable())
        })
    }
}

impl StateSource for ScopedState<'_> {
    async fn watermark(&self, node: &PipeRef) -> Result<Option<Bound>, StateError> {
        self.check(&[node], None).await?;
        let result = self.bounded(self.inner.watermark(node).await);
        self.check(&[node], None).await?;
        result
    }
    async fn oldest_watermark(&self, node: &PipeRef) -> Result<Option<Bound>, StateError> {
        self.check(&[node], None).await?;
        let result = self.bounded(self.inner.oldest_watermark(node).await);
        self.check(&[node], None).await?;
        result
    }
    async fn definition_hash(&self, node: &PipeRef) -> Result<Option<String>, StateError> {
        self.check(&[node], None).await?;
        let result = self.bounded(self.inner.definition_hash(node).await);
        self.check(&[node], None).await?;
        result
    }
    async fn cursor(
        &self,
        instance: &str,
        parent: &PipeRef,
        child: &PipeRef,
    ) -> Result<Option<EdgeCursor>, StateError> {
        if instance != child.instance_keys() {
            return Err(StateError::Other(unavailable()));
        }
        self.check(&[parent, child], Some(instance)).await?;
        let result = self.bounded(self.inner.cursor(instance, parent, child).await);
        self.check(&[parent, child], Some(instance)).await?;
        result
    }
    async fn ledger_after(
        &self,
        instance: &str,
        parent: &PipeRef,
        after: i64,
    ) -> Result<Vec<LedgerEntry>, StateError> {
        self.check(&[parent], Some(instance)).await?;
        let result = self.bounded(self.inner.ledger_after(instance, parent, after).await);
        self.check(&[parent], Some(instance)).await?;
        result
    }
    async fn latest_seq(
        &self,
        instance: &str,
        parent: &PipeRef,
    ) -> Result<Option<i64>, StateError> {
        self.check(&[parent], Some(instance)).await?;
        let result = self.bounded(self.inner.latest_seq(instance, parent).await);
        self.check(&[parent], Some(instance)).await?;
        result
    }
}

fn depth(value: &serde_json::Value) -> Result<Option<Depth>, String> {
    if !(value.is_null() || value.is_boolean() || value.as_u64().is_some()) {
        return Err("Invalid registered Pipeline traversal depth.".into());
    }
    let value = Value::from_json_str(&value.to_string()).map_err(|_| unavailable())?;
    Ok(Depth::from_value(&value))
}

fn validate_reference(reference: &str) -> Result<PipeRef, String> {
    let parts: Vec<_> = reference.split('|').collect();
    if reference.len() > 2048
        || parts.len() != 4
        || parts
            .iter()
            .any(|part| part.is_empty() || part.chars().any(char::is_control))
    {
        return Err("Invalid registered Pipeline member identity.".into());
    }
    Ok(PipeRef::from_string(reference))
}

impl Controller {
    /// A reader plans with native metadata only; no job, action scope or plugin import.
    pub async fn plan_metadata(
        self: &Arc<Self>,
        name: &str,
        registry: &str,
        options: &PlanOptions,
    ) -> Result<serde_json::Value, PlanError> {
        meerschaum_core::pipeline::validate_name(name).map_err(PlanError::Invalid)?;
        let source = context::current()
            .filter(|caller| caller.auth.is_some())
            .ok_or_else(|| {
                PlanError::Failed("The Pipeline plan lost its original source.".into())
            })?;
        source
            .require(&["pipes:read"], &[])
            .map_err(|_| PlanError::Invalid(unavailable()))?;
        let state = self.state().map_err(PlanError::Failed)?;
        let team_id = if meerschaum_core::access::is_everyone(Some(&source.primary_team)) {
            None
        } else {
            Some(
                self.team_id(&source.primary_team)
                    .await
                    .map_err(PlanError::Failed)?
                    .ok_or_else(|| PlanError::Invalid(unavailable()))?,
            )
        };
        let source_ceiling = match &source.auth {
            Some(meerschaum_server::auth::Principal::Execution(parent)) => {
                parent.grant.source_ceiling.clone().unwrap_or_default()
            }
            _ => Default::default(),
        };
        let grant = Box::pin(callbacks::register_with_sources(
            &state,
            &source,
            Uuid::new_v4(),
            team_id,
            &["pipes:read"],
            callbacks::Purpose::Command,
            source_ceiling,
        ))
        .await
        .map_err(|_| PlanError::Invalid(unavailable()))?;
        let proof = Proof {
            state,
            grant,
            request: source,
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(30),
            Box::pin(self.plan_with_proof(&proof, name, registry, options)),
        )
        .await;
        // Revoke before returning as well as through cancellation-safe Drop.
        let _ = callbacks::revoke(&proof.state, proof.grant.id).await;
        result
            .map_err(|_| PlanError::Failed("The bounded metadata plan timed out.".into()))?
            .map_err(|message| {
                if message == CLASSIFICATION_UNAVAILABLE {
                    PlanError::MetadataUnavailable(message)
                } else {
                    PlanError::Invalid(message)
                }
            })
    }

    async fn plan_with_proof(
        &self,
        proof: &Proof,
        name: &str,
        registry: &str,
        options: &PlanOptions,
    ) -> Result<serde_json::Value, String> {
        let config = self.config()?;
        let settings = meerschaum_engine::EngineConfig::from_config(&config, Some(&self.root));
        let conn = proof.instance(registry).await?;
        let mut metadata = Pipe::builder("mrsm", "pipeline")
            .location(Some(name))
            .instance(registry)
            .defaults(settings.defaults.clone())
            .build()
            .map_err(|_| unavailable())?;
        let attributes = conn
            .get_pipe_attributes(&metadata)
            .await
            .map_err(|_| unavailable())?;
        let parameters = attributes
            .get("parameters")
            .and_then(Value::as_map)
            .ok_or_else(unavailable)?
            .clone();
        if Value::Map(parameters.clone()).to_json_string().len() > MAX_METADATA_BYTES {
            return Err(unavailable());
        }
        metadata.set_parameters(parameters.clone());
        let registration = conn
            .get_pipe_id(&metadata)
            .await
            .map_err(|_| unavailable())?
            .map(|id| id.to_value())
            .ok_or_else(unavailable)?;
        let metadata = Snapshot {
            pipe: metadata,
            registration,
        };
        metadata.check(proof, true).await?;
        let definition: crate::native::pipelines::Definition = serde_json::from_str(
            &parameters
                .get("pipeline")
                .ok_or_else(unavailable)?
                .to_json_string(),
        )
        .map_err(|_| "Invalid registered Pipeline definition.".to_string())?;
        if definition.targets.len() > MAX_RECORDS
            || definition
                .defaults
                .keys()
                .any(|key| !meerschaum_core::pipeline::EXECUTION_DEFAULTS.contains(&key.as_str()))
        {
            return Err("Invalid registered Pipeline execution defaults.".into());
        }
        let targets = definition
            .targets
            .iter()
            .map(|reference| validate_reference(reference))
            .collect::<Result<Vec<_>, _>>()?;
        let selection = Selection::refs(targets)
            .with_parents(depth(&definition.parents)?)
            .with_children(depth(&definition.children)?)
            .with_graph(definition.graph.unwrap_or(true));
        let mut argv = vec!["plan".into(), "pipelines".into(), name.into()];
        for (flag, explicit, key) in [
            ("--begin", &options.begin, "begin"),
            ("--end", &options.end, "end"),
        ] {
            let value = explicit.clone().or_else(|| {
                definition
                    .defaults
                    .get(key)
                    .filter(|value| !value.is_null())
                    .map(|value| {
                        value
                            .as_str()
                            .map(str::to_string)
                            .unwrap_or_else(|| value.to_string())
                    })
            });
            if let Some(value) = value {
                if value.len() > 256 || value.chars().any(char::is_control) {
                    return Err("Invalid Pipeline bound.".into());
                }
                argv.push(format!("{flag}={value}"));
            }
        }
        let args = crate::parse::parse_arguments(&argv, &crate::types::DateContext::system())
            .map_err(|_| "Invalid Pipeline bound.".to_string())?;
        let bounds = crate::native::sync::SyncArgs::from_args(&args);
        let plan_options = meerschaum_engine::PlanOptions {
            begin: bounds
                .begin
                .as_ref()
                .and_then(meerschaum_engine::sync::date_bound_to_bound),
            end: bounds
                .end
                .as_ref()
                .and_then(meerschaum_engine::sync::date_bound_to_bound),
            interactive: false,
            yes: ["yes", "force"].iter().any(|key| {
                definition
                    .defaults
                    .get(*key)
                    .and_then(|value| Value::from_json_str(&value.to_string()).ok())
                    .is_some_and(|value| value.truthy())
            }),
            auto_rebuild: settings.auto_rebuild,
            inclusive_cap: settings.inclusive_cap,
            ..Default::default()
        };
        let mut native = StaticRegistry::new();
        let mut records = Vec::new();
        let mut loaded = BTreeSet::new();
        let mut pending = selection.target_instances();
        let mut bytes = 0usize;
        let mut graph = loop {
            if loaded.len().saturating_add(pending.len()) > 64 {
                return Err("The Pipeline spans too many logical instances.".into());
            }
            for keys in std::mem::take(&mut pending) {
                if !loaded.insert(keys.clone()) {
                    continue;
                }
                let conn = proof.instance(&keys).await?;
                native = native.with_instance(&keys, conn);
                let next = meerschaum_engine::engine::fetch_records(
                    &native,
                    &settings.defaults,
                    &keys,
                    &Default::default(),
                )
                .await
                .map_err(|_| unavailable())?;
                if records.len().saturating_add(next.len()) > MAX_RECORDS {
                    return Err("The Pipeline metadata exceeds its record limit.".into());
                }
                for record in &next {
                    bytes = bytes.saturating_add(
                        Value::Map(record.parameters.clone()).to_json_string().len(),
                    );
                    if bytes > MAX_METADATA_BYTES {
                        return Err("The Pipeline metadata exceeds its payload limit.".into());
                    }
                }
                let _ = proof.instance(&keys).await?;
                records.extend(next);
            }
            let graph = Graph::build(
                records.clone(),
                &GraphOptions {
                    defaults: settings.defaults.clone(),
                    implicit_edges: settings.implicit_edges,
                    default_backtrack_minutes: settings.default_backtrack_minutes as f64,
                },
            );
            pending = selection
                .referenced_instances(&graph)
                .difference(&loaded)
                .cloned()
                .collect();
            if pending.is_empty() {
                break graph;
            }
        };
        let mut subgraph = Pipeline::new(selection.clone())
            .resolve(&graph)
            .map_err(|_| "The registered Pipeline graph cannot be planned.".to_string())?;
        let mut snapshots = HashMap::new();
        let mut source_keys = BTreeSet::new();
        for reference in &subgraph.order {
            let node = graph.node(reference).ok_or_else(unavailable)?;
            let registration = node
                .record
                .pipe_id
                .clone()
                .filter(|value| !value.is_null())
                .ok_or_else(unavailable)?;
            let snapshot = Snapshot {
                pipe: node.pipe.clone(),
                registration,
            };
            // Hidden dependencies never become permission to inspect their data or state.
            snapshot.check(proof, false).await?;
            if node
                .record
                .connector_keys
                .split_once(':')
                .is_some_and(|(kind, _)| !matches!(kind, "sql" | "api" | "valkey"))
            {
                source_keys.insert(node.record.connector_keys.clone());
            }
            snapshots.insert(reference.clone(), snapshot);
        }
        let classified = if source_keys.is_empty() {
            None
        } else {
            let identity =
                callbacks::load_identity(&proof.state, proof.grant.id, proof.grant.launch_id)
                    .await
                    .map_err(|_| unavailable())?;
            let observed = self
                .cached_reader_capabilities(&identity, &proof.request, &source_keys)
                .await
                .map_err(|_| CLASSIFICATION_UNAVAILABLE.to_string())?;
            for record in &mut records {
                if source_keys.contains(&record.connector_keys) {
                    let flags = observed
                        .entries
                        .get(&record.connector_keys)
                        .and_then(|entry| *entry)
                        .ok_or(CLASSIFICATION_UNAVAILABLE)?;
                    record.python = flags.python;
                    record.streaming = flags.streaming;
                }
            }
            let classified_graph = Graph::build(
                records,
                &GraphOptions {
                    defaults: settings.defaults.clone(),
                    implicit_edges: settings.implicit_edges,
                    default_backtrack_minutes: settings.default_backtrack_minutes as f64,
                },
            );
            let classified_subgraph = Pipeline::new(selection)
                .resolve(&classified_graph)
                .map_err(|_| unavailable())?;
            if classified_subgraph.order != subgraph.order {
                return Err(unavailable());
            }
            graph = classified_graph;
            subgraph = classified_subgraph;
            Some(observed)
        };
        let inner = InstanceState::new(Arc::new(native), registry)
            .with_read_limit(MAX_RECORDS)
            .with_read_byte_limit(MAX_METADATA_BYTES);
        inner.register_pipes(
            snapshots
                .iter()
                .map(|(reference, snapshot)| (reference.clone(), snapshot.pipe.clone())),
        );
        let scoped = ScopedState {
            proof,
            inner,
            snapshots,
            failed: AtomicBool::new(false),
        };
        let mut plan = meerschaum_engine::plan(&graph, &subgraph, &scoped, &plan_options).await;
        if scoped.failed.load(Ordering::Acquire) {
            return Err(unavailable());
        }
        metadata.check(proof, true).await?;
        for snapshot in scoped.snapshots.values() {
            snapshot.check(proof, false).await?;
        }
        if let Some(classified) = classified {
            let identity =
                callbacks::load_identity(&proof.state, proof.grant.id, proof.grant.launch_id)
                    .await
                    .map_err(|_| unavailable())?;
            let fresh = self
                .cached_reader_capabilities(&identity, &proof.request, &source_keys)
                .await
                .map_err(|_| CLASSIFICATION_UNAVAILABLE.to_string())?;
            if classified != fresh {
                return Err(CLASSIFICATION_UNAVAILABLE.into());
            }
        }
        // A graph warning can refer to an unselected private registration. The plan's node
        // diagnostics remain useful; global registration warnings stay on privileged tools.
        plan.warnings.clear();
        Ok(serde_json::json!({ "plan": plan, "text": plan.to_string(), "warnings": plan.warnings }))
    }
}
