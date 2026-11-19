//! Worker consumption of Main instructions through the first enforced host transport.

use meerschaum_connector_api::APIConnector;
use meerschaum_connector_api::client::{
    GraphBinding, GraphInstruction, GraphPlanReply, GraphPlanRequest,
};
use meerschaum_engine::{PipeRef, Plan, PlanAuthority, PlannedNode};
use std::sync::Arc;

pub(super) fn callback() -> Result<Option<APIConnector>, String> {
    meerschaum_core::team_scope::sandbox::with_api_transport(|alias| {
        APIConnector::from_execution_alias(alias).map_err(|error| error.to_string())
    })
}

/// The native dispatcher must use its built-in action and arguments for a captured graph.
/// Only the transport installed by the enclosing sandbox can select this behavior.
pub(crate) fn captured_dispatch(
    rt: &mut crate::dispatch::Runtime<'_>,
    action: &[String],
    words: &[String],
) -> Result<Option<MainGraph>, String> {
    if !matches!(action, [command, subject] if matches!(command.as_str(), "sync" | "verify") && subject == "pipes")
        && !matches!(action, [command, subject, _] if command == "sync" && subject == "pipelines")
    {
        return Ok(None);
    }
    let Some(callback) = callback()? else {
        return Ok(None);
    };
    super::session(rt).block_on(async {
        let Some(reply) = callback
            .graph_plan(&GraphPlanRequest::Dispatch)
            .await
            .map_err(|error| error.to_string())?
        else {
            return Ok(None);
        };
        let GraphInstruction::Dispatch {
            verify,
            argv_sha256,
        } = reply.instruction
        else {
            return Err("Main did not identify the canonical action.".into());
        };
        let actual = meerschaum_server::executor::pipe_operation::dispatch_argv_sha256(words)
            .map_err(|error| error.detail_text())?;
        if verify != (action[0] == "verify") || argv_sha256 != actual {
            return Err("The worker command differs from the action captured by Main.".into());
        }
        Ok(Some(MainGraph {
            callback,
            consumer: reply.consumer,
            issuer: reply.issuer,
            pipeline_request: None,
        }))
    })
}

pub(super) async fn verification(
    callback: &APIConnector,
    pipe: &meerschaum_core::Pipe,
) -> Result<
    Option<(
        GraphBinding,
        GraphBinding,
        meerschaum_server::executor::pipe_operation::Verification,
    )>,
    String,
> {
    let Some(reply) = callback
        .graph_plan(&GraphPlanRequest::Verify {
            reference: pipe.pipe_ref(),
        })
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(None);
    };
    let GraphInstruction::Verify(value) = reply.instruction else {
        return Err("Main did not issue verification instructions.".into());
    };
    let plan: meerschaum_server::executor::pipe_operation::Verification =
        serde_json::from_value(value).map_err(|_| "Invalid Main verification instructions.")?;
    if plan.reference != pipe.pipe_ref()
        || plan.calls.len() > 1024
        || plan.definition_hash != meerschaum_engine::definition_hash(pipe.raw_parameters())
    {
        return Err("The exact Main verification definition changed.".into());
    }
    Ok(Some((reply.consumer, reply.issuer, plan)))
}

/// Attach Main's per-launch instructions to an API worker's graph engine. Ordinary
/// native hosts retain local planning. An API worker never selects this callback
/// from mutable Config, Python dictionaries, command arguments or environment.
pub async fn attach(
    engine: meerschaum_engine::Engine,
) -> Result<meerschaum_engine::Engine, String> {
    let callback = callback()?;
    let Some(callback) = callback else {
        return Ok(engine);
    };
    match MainGraph::connect(callback).await? {
        Some(authority) => Ok(engine.with_plan_authority(authority)),
        None => Ok(engine),
    }
}

pub(crate) struct MainGraph {
    callback: APIConnector,
    consumer: GraphBinding,
    issuer: GraphBinding,
    pipeline_request: Option<GraphPlanRequest>,
}
impl std::fmt::Debug for MainGraph {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("MainGraph").finish_non_exhaustive()
    }
}
impl MainGraph {
    pub(super) async fn pipeline(
        &mut self,
        registry: &str,
        name: &str,
        run_id: &str,
    ) -> Result<(serde_json::Value, Vec<String>), String> {
        let request = GraphPlanRequest::Pipeline {
            registry: registry.into(),
            name: name.into(),
            run_id: run_id.into(),
        };
        match self.request(request.clone()).await?.instruction {
            GraphInstruction::Pipeline { definition, roots } => {
                self.pipeline_request = Some(request);
                Ok((definition, roots))
            }
            _ => Err("Main did not return its captured Pipeline selection.".into()),
        }
    }
    /// None is legitimate only when freshly authenticated Main says this exact
    /// callback has no captured graph. A present graph's missing permit is an error.
    pub(super) async fn connect(callback: APIConnector) -> Result<Option<Arc<Self>>, String> {
        let reply = callback
            .graph_plan(&GraphPlanRequest::Initial)
            .await
            .map_err(|error| error.to_string())?;
        let Some(reply) = reply else { return Ok(None) };
        let GraphInstruction::Initial(_) = reply.instruction else {
            return Err("Invalid Main graph instruction phase.".into());
        };
        Ok(Some(Arc::new(Self {
            callback,
            consumer: reply.consumer,
            issuer: reply.issuer,
            pipeline_request: None,
        })))
    }
    async fn request(&self, request: GraphPlanRequest) -> Result<GraphPlanReply, String> {
        let reply = self
            .callback
            .graph_plan(&request)
            .await
            .map_err(|error| error.to_string())?
            .ok_or("Main retired this captured graph; local fallback is unavailable.")?;
        if reply.consumer != self.consumer || reply.issuer != self.issuer {
            return Err("The graph callback/context revision changed. Start a fresh original-source operation.".into());
        }
        Ok(reply)
    }
}
#[meerschaum_connector::async_trait]
impl PlanAuthority for MainGraph {
    fn new_runtime_run_id(&self, registry: &str) -> Option<String> {
        registry.starts_with("api:").then(|| {
            format!(
                "mrsm-graph-{}-{}",
                self.consumer.launch,
                uuid::Uuid::new_v4()
            )
        })
    }

    async fn confirms_unused_run_id(&self, registry: &str, run_id: &str) -> Result<bool, String> {
        let Some(request) = &self.pipeline_request else {
            return Ok(false);
        };
        if !matches!(request, GraphPlanRequest::Pipeline { registry: saved_registry, run_id: saved_id, .. } if registry == saved_registry && run_id == saved_id)
        {
            return Err("The run ID or registry differs from Main's captured Pipeline.".into());
        }
        match self.request(request.clone()).await?.instruction {
            GraphInstruction::Pipeline { .. } => Ok(true),
            _ => Err("Main did not confirm the captured Pipeline run ID.".into()),
        }
    }
    fn node_delivery(
        &self,
        node: &PipeRef,
    ) -> Option<meerschaum_connector::batch_delivery::NodeDelivery> {
        Some(meerschaum_connector::batch_delivery::NodeDelivery::new(
            self.consumer.clone(),
            self.issuer.clone(),
            node.as_str().into(),
        ))
    }
    async fn initial(&self, _local: &Plan) -> Result<Plan, String> {
        let reply = self.request(GraphPlanRequest::Initial).await?;
        let GraphInstruction::Initial(plan) = reply.instruction else {
            return Err("Invalid Main initial plan.".into());
        };
        serde_json::from_value(plan).map_err(|_| "Main's exact graph plan is unavailable.".into())
    }
    async fn ready(&self, node: &PipeRef) -> Result<PlannedNode, String> {
        // No local begin/end/watermark/parent-success values are sent as authority.
        let reply = self
            .request(GraphPlanRequest::Ready {
                reference: node.as_str().into(),
            })
            .await?;
        let GraphInstruction::Ready(node) = reply.instruction else {
            return Err("Invalid Main ready-node instruction.".into());
        };
        serde_json::from_value(node)
            .map_err(|_| "Main's exact ready-node plan is unavailable.".into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{Json, Router, extract::State, routing::post};
    use meerschaum_connector_api::client::authentication::{Request, RequestAuthentication};
    use serde_json::{Value, json};
    use std::sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    };

    struct Authentication;
    impl RequestAuthentication for Authentication {
        fn authorization(&self, _: Request<'_>) -> meerschaum_connector::ConnectorResult<String> {
            Ok("Bearer synthetic-instruction-consumer".into())
        }
    }
    struct Main {
        consumer: GraphBinding,
        issuer: GraphBinding,
        requests: Mutex<Vec<Value>>,
        changed: AtomicBool,
        absent: AtomicBool,
    }
    fn node() -> Value {
        json!({"pipe_ref":"sql:main|sql:source|fixture|None","action":"root","bounds":{"begin":5,"end":10},"reason":"Exact Main window","definition_hash":"exact-native-definition"})
    }
    fn plan() -> Value {
        json!({"nodes":[node()],"edges":[],"flat":true,"warnings":[]})
    }
    async fn instruction(State(main): State<Arc<Main>>, Json(request): Json<Value>) -> Json<Value> {
        main.requests.lock().unwrap().push(request.clone());
        if main.absent.load(Ordering::SeqCst) {
            return Json(
                json!({"success":true,"message":"Main verified no graph","instruction":null}),
            );
        }
        let mut consumer = main.consumer.clone();
        if main.changed.load(Ordering::SeqCst) {
            consumer.revision += 1;
        }
        let instruction = if request["phase"] == "initial" {
            GraphInstruction::Initial(plan())
        } else {
            GraphInstruction::Ready(node())
        };
        Json(
            json!({"success":true,"message":"Main-issued only","instruction":GraphPlanReply {version:1,consumer,issuer:main.issuer.clone(),instruction}}),
        )
    }

    #[tokio::test]
    async fn native_graph_consumer_uses_only_main_bounds_and_refuses_changed_or_retired_lineage() {
        let binding = || GraphBinding {
            grant: uuid::Uuid::new_v4(),
            launch: uuid::Uuid::new_v4(),
            revision: 1,
        };
        let main = Arc::new(Main {
            consumer: binding(),
            issuer: binding(),
            requests: Mutex::new(Vec::new()),
            changed: AtomicBool::new(false),
            absent: AtomicBool::new(false),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let app = Router::new()
            .route(
                meerschaum_connector_api::client::GRAPH_PLAN_PATH,
                post(instruction),
            )
            .with_state(main.clone());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        // Dropping the fixture future on failure also stops its one owned listener.
        struct Abort(tokio::task::JoinHandle<()>);
        impl Drop for Abort {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _server = Abort(server);
        let callback = APIConnector::new(
            Some("instruction-consumer"),
            meerschaum_core::Value::from_json_str(
                &json!({"host":"127.0.0.1","port":port}).to_string(),
            )
            .unwrap()
            .to_map(),
        )
        .unwrap()
        .with_request_authentication(Arc::new(Authentication), 8192)
        .unwrap();
        let authority = MainGraph::connect(callback).await.unwrap().unwrap();
        let mut local: Plan = serde_json::from_value(plan()).unwrap();
        local.nodes[0].bounds = Some(meerschaum_engine::Bounds {
            begin: meerschaum_engine::Begin::Unbounded,
            end: None,
        });
        let issued = authority.initial(&local).await.unwrap();
        assert_eq!(issued, serde_json::from_value::<Plan>(plan()).unwrap());
        let reference = local.nodes[0].pipe_ref.clone();
        let ready = authority.ready(&reference).await.unwrap();
        assert_eq!(
            ready.bounds,
            serde_json::from_value::<PlannedNode>(node())
                .unwrap()
                .bounds
        );
        let requests = main.requests.lock().unwrap().clone();
        assert_eq!(
            requests,
            vec![
                json!({"phase":"initial"}),
                json!({"phase":"initial"}),
                json!({"phase":"ready","reference":reference.as_str()})
            ]
        );
        // A renewed context is not permission to substitute a different canonical record.
        main.changed.store(true, Ordering::SeqCst);
        assert!(
            authority
                .ready(&reference)
                .await
                .unwrap_err()
                .contains("revision changed")
        );
        main.changed.store(false, Ordering::SeqCst);
        main.absent.store(true, Ordering::SeqCst);
        assert!(
            authority
                .initial(&local)
                .await
                .unwrap_err()
                .contains("local fallback")
        );
    }
}
